"""
Credit / debit vertical spreads and iron condor calculations.

All heavy lifting is done via PySpark self-joins so the work can
distribute across cores (and eventually across a cluster).

Public API
----------
- ``calculate_spreads(df, ...)``  → DataFrame of vertical spreads
- ``calculate_iron_condors(df, ...)`` → DataFrame of iron condors

Both accept the raw option-chain DataFrame produced by
``IBKRClient.get_option_chains`` (schema: ``OPTION_CHAIN_SCHEMA``).
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def calculate_spreads(
    chain_df: DataFrame,
) -> DataFrame:
    """
    Generate all vertical spread combinations (credit AND debit) and rank them.

    Logic
    -----
    1. **Leg selection** — all contracts with a valid ``mid`` price.
       Short legs must be OTM or ATM.
    2. **Self-join** — pair each short leg with every other contract
       of the same symbol / expiration / right (both strike orderings).
    3. **Metric calculation** — net premium, width, max profit, max loss,
       spread ratio, ROC, breakeven, % to strike, % to breakeven,
       and net Greeks for the spread.
    4. **Strategy labelling** — e.g. "Bull Put Credit Spread",
       "Bull Call Debit Spread", "Bear Call Credit Spread", etc.
    5. **Ranking** — ordered by ``spread_ratio DESC``.

    Parameters
    ----------
    chain_df:
        Raw option-chain DataFrame (``OPTION_CHAIN_SCHEMA``).

    Returns
    -------
    DataFrame
        One row per spread pair with all computed metrics.
    """
    valid = chain_df.filter(F.col("mid").isNotNull())

    # ── short legs (must be OTM or ATM) ──────────────────────────────
    short_legs = (
        valid
        .filter(
            F.when(
                F.col("right") == "P",
                F.col("strike") <= F.col("underlying_price"),
            ).otherwise(
                F.col("strike") >= F.col("underlying_price"),
            )
        )
        .alias("s")
    )

    # ── long legs (further OTM → cheaper, filtering in join) ─────────
    long_legs = valid.alias("l")

    # ── self-join ────────────────────────────────────────────────────
    # Pair every short leg with every other contract of the same
    # symbol / expiration / right.  Both strike orderings are kept
    # so we produce credit AND debit spreads.

    join_cond = [
        F.col("s.symbol") == F.col("l.symbol"),
        F.col("s.expiration") == F.col("l.expiration"),
        F.col("s.right") == F.col("l.right"),
        F.col("s.strike") != F.col("l.strike"),
    ]

    spreads = short_legs.join(long_legs, on=join_cond, how="inner")

    # ── metrics ──────────────────────────────────────────────────────

    # net_premium > 0 → credit received;  < 0 → debit paid
    net_premium = F.col("s.mid") - F.col("l.mid")
    width = F.abs(F.col("s.strike") - F.col("l.strike"))
    is_credit = net_premium >= 0

    # Per-share max profit / max loss (always positive values)
    raw_max_profit = F.when(is_credit, net_premium).otherwise(width + net_premium)
    raw_max_loss = F.when(is_credit, width - net_premium).otherwise(F.abs(net_premium))

    max_profit = raw_max_profit * 100   # ×100 shares per contract
    max_loss = raw_max_loss * 100       # ×100 shares per contract

    spread_ratio = (F.abs(net_premium) / width) * 100
    credit_yield = F.when(raw_max_loss > 0, (raw_max_profit / raw_max_loss) * 100)
    roc = (F.abs(net_premium) / F.col("s.strike")) * 100

    # Breakeven:
    #   Credit spreads → based on short strike
    #     Bull Put Credit:  short_strike − premium
    #     Bear Call Credit: short_strike + premium
    #   Debit spreads → based on long strike
    #     Bull Call Debit:  long_strike + |premium|
    #     Bear Put Debit:   long_strike − |premium|
    breakeven = F.when(
        is_credit,
        F.when(F.col("s.right") == "P",
               F.col("s.strike") - net_premium
        ).otherwise(
               F.col("s.strike") + net_premium
        ),
    ).otherwise(
        F.when(F.col("s.right") == "P",
               F.col("l.strike") + net_premium      # net_premium < 0
        ).otherwise(
               F.col("l.strike") - net_premium      # net_premium < 0
        ),
    )

    pct_to_short_strike = (F.abs(
        F.col("s.underlying_price") - F.col("s.strike")
    ) / F.col("s.underlying_price")) * 100
    pct_to_long_strike = (F.abs(
        F.col("s.underlying_price") - F.col("l.strike")
    ) / F.col("s.underlying_price")) * 100

    pct_to_breakeven = (F.abs(
        F.col("s.underlying_price") - breakeven
    ) / F.col("s.underlying_price")) * 100

    # ── strategy label ───────────────────────────────────────────────
    # Direction: Bull = profit when stock rises, Bear = profit when falls
    #   Puts:  s.strike > l.strike → Bull;  s.strike < l.strike → Bear
    #   Calls: s.strike > l.strike → Bull;  s.strike < l.strike → Bear
    #   (selling higher-strike put or buying lower-strike call = bullish)
    direction = F.when(
        F.col("s.right") == "P",
        F.when(F.col("s.strike") > F.col("l.strike"), F.lit("Bull"))
         .otherwise(F.lit("Bear")),
    ).otherwise(
        F.when(F.col("s.strike") < F.col("l.strike"), F.lit("Bear"))
         .otherwise(F.lit("Bull")),
    )
    right_label = F.when(F.col("s.right") == "P", F.lit("Put")).otherwise(F.lit("Call"))
    spread_type = F.when(is_credit, F.lit("Credit")).otherwise(F.lit("Debit"))
    strategy_label = F.concat(
        direction, F.lit(" "), right_label, F.lit(" "), spread_type, F.lit(" Spread")
    )

    result = (
        spreads
        .select(
            F.col("s.symbol").alias("symbol"),
            F.col("s.expiration").alias("expiration"),
            F.col("s.right").alias("right"),
            strategy_label.alias("strategy"),
            F.col("s.strike").alias("short_strike"),
            pct_to_short_strike.alias("pct_to_short_strike"),
            F.col("s.mid").alias("short_mid"),
            F.col("s.delta").alias("short_delta"),
            F.col("l.strike").alias("long_strike"),
            pct_to_long_strike.alias("pct_to_long_strike"),
            F.col("l.mid").alias("long_mid"),
            F.col("l.delta").alias("long_delta"),
            (net_premium * 100).alias("credit"),
            width.alias("width"),
            max_profit.alias("max_profit"),
            max_loss.alias("max_loss"),
            spread_ratio.alias("spread_ratio"),
            credit_yield.alias("credit_yield"),
            roc.alias("roc"),
            breakeven.alias("breakeven"),
            pct_to_breakeven.alias("pct_to_breakeven"),
            F.col("s.underlying_price").alias("underlying_price"),
            F.col("s.dte").alias("dte"),
            # Net Greeks (short − long because we *sell* the short leg)
            (F.col("s.delta") - F.col("l.delta")).alias("net_delta"),
            (F.col("s.gamma") - F.col("l.gamma")).alias("net_gamma"),
            (F.col("s.theta") - F.col("l.theta")).alias("net_theta"),
            (F.col("s.vega") - F.col("l.vega")).alias("net_vega"),
        )
        .filter(F.col("max_profit") > 0)   # valid spreads only
        .filter(F.col("max_loss") > 0)
    )

    # ── deduplicate mirror pairs ─────────────────────────────────────
    # When both legs are OTM, the pair (A, B) appears twice with roles
    # swapped — once as credit and once as debit.  Keep only one row
    # per unique pair of contracts, preferring the credit version.
    dedup_window = Window.partitionBy(
        "symbol", "expiration", "right",
        F.least("short_strike", "long_strike"),
        F.greatest("short_strike", "long_strike"),
    ).orderBy(F.col("credit").desc())   # credit version wins

    result = (
        result
        .withColumn("_dedup_rank", F.row_number().over(dedup_window))
        .filter(F.col("_dedup_rank") == 1)
        .drop("_dedup_rank")
        .orderBy(F.col("spread_ratio").desc())
    )

    logger.info("Generated %d spread combinations", result.count())
    return result


def calculate_iron_condors(
    spreads_df: DataFrame,
) -> DataFrame:
    """
    Build iron condors by combining the best bull-put and best bear-call
    spread for each symbol / expiration.

    An iron condor = *sell* an OTM put spread + *sell* an OTM call spread
    on the same underlying for the same expiration.

    Parameters
    ----------
    chain_df:
        Raw option-chain DataFrame.

    Returns
    -------
    DataFrame
        One row per iron condor with combined metrics.
    """

    # Only credit spreads make sense for iron condors
    credit_spreads = spreads_df.filter(F.col("strategy").contains("Credit"))

    # ── pick the top credit spread per (symbol, expiration, right) ───
    window = Window.partitionBy("symbol", "expiration", "right").orderBy(
        F.col("spread_ratio").desc()
    )

    top_spreads = (
        credit_spreads
        .withColumn("_rank", F.row_number().over(window))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )

    put_side = top_spreads.filter(F.col("right") == "P").alias("p")
    call_side = top_spreads.filter(F.col("right") == "C").alias("c")

    condor_join = [
        F.col("p.symbol") == F.col("c.symbol"),
        F.col("p.expiration") == F.col("c.expiration"),
    ]

    condors = put_side.join(call_side, on=condor_join, how="inner")

    # p.credit and c.credit are already ×100 (from spreads output)
    total_credit = F.col("p.credit") + F.col("c.credit")

    # width is per-share; multiply by 100 for contract-level
    wider_width_contract = F.greatest(F.col("p.width"), F.col("c.width")) * 100
    ic_max_loss = wider_width_contract - total_credit
    ic_ratio = (total_credit / wider_width_contract) * 100
    ic_credit_yield = F.when(ic_max_loss > 0, (total_credit / ic_max_loss) * 100)
    # ROC: per-share total credit / average of the two short strikes
    ic_per_share_credit = total_credit / 100
    avg_short_strike = (F.col("p.short_strike") + F.col("c.short_strike")) / 2
    ic_roc = (ic_per_share_credit / avg_short_strike) * 100

    # Breakevens use per-share credit (÷100) against per-share strikes
    per_share_put_credit = F.col("p.credit") / 100
    per_share_call_credit = F.col("c.credit") / 100
    lower_be = F.col("p.short_strike") - per_share_put_credit
    upper_be = F.col("c.short_strike") + per_share_call_credit

    result = condors.select(
        F.col("p.symbol").alias("symbol"),
        F.col("p.expiration").alias("expiration"),
        F.col("p.short_strike").alias("put_short_strike"),
        F.col("p.long_strike").alias("put_long_strike"),
        F.col("c.short_strike").alias("call_short_strike"),
        F.col("c.long_strike").alias("call_long_strike"),
        F.col("p.credit").alias("put_credit"),
        F.col("c.credit").alias("call_credit"),
        total_credit.alias("total_credit"),
        F.col("p.width").alias("put_width"),
        F.col("c.width").alias("call_width"),
        total_credit.alias("max_profit"),
        ic_max_loss.alias("max_loss"),
        ic_ratio.alias("spread_ratio"),
        ic_credit_yield.alias("credit_yield"),
        ic_roc.alias("ROC"),
        lower_be.alias("lower_breakeven"),
        upper_be.alias("upper_breakeven"),
        F.col("p.underlying_price").alias("underlying_price"),
        (F.abs(F.col("p.underlying_price") - lower_be) / F.col("p.underlying_price") * 100).alias(
            "pct_to_lower_breakeven"
        ),
        (F.abs(F.col("p.underlying_price") - upper_be) / F.col("p.underlying_price") * 100).alias(
            "pct_to_upper_breakeven"
        ),
        F.col("p.dte").alias("dte"),
        # Net Greeks: sum of both legs
        (F.col("p.net_delta") + F.col("c.net_delta")).alias("net_delta"),
        (F.col("p.net_gamma") + F.col("c.net_gamma")).alias("net_gamma"),
        (F.col("p.net_theta") + F.col("c.net_theta")).alias("net_theta"),
        (F.col("p.net_vega") + F.col("c.net_vega")).alias("net_vega"),
    )

    logger.info("Generated %d iron condor combinations", result.count())
    return result

def calculate_iron_butterflies(
    chain_df: DataFrame,
) -> DataFrame:
    """
    Build iron butterflies for each symbol / expiration.

    An iron butterfly = sell ATM straddle + buy OTM strangle wings,
    where both short legs share the same ATM strike.  Only symmetric
    wing widths (put_width == call_width) are kept.

    Parameters
    ----------
    chain_df:
        Raw option-chain DataFrame (``OPTION_CHAIN_SCHEMA``).

    Returns
    -------
    DataFrame
        One row per (symbol, expiration, ATM strike, wing width).
    """
    valid = chain_df.filter(F.col("mid").isNotNull())

    # ── ATM strike per (symbol, expiration) ───────────────────────────
    atm_window = Window.partitionBy("symbol", "expiration").orderBy(
        F.abs(F.col("strike") - F.col("underlying_price"))
    )
    atm_strikes = (
        valid.select("symbol", "expiration", "strike", "underlying_price")
        .distinct()
        .withColumn("_r", F.row_number().over(atm_window))
        .filter(F.col("_r") == 1).drop("_r")
        .select(
            F.col("symbol").alias("atm_sym"),
            F.col("expiration").alias("atm_exp"),
            F.col("strike").alias("atm_strike"),
        )
    )

    def _join_atm(df, right):
        return (
            df.filter(F.col("right") == right)
            .join(
                atm_strikes,
                on=[
                    F.col("symbol") == F.col("atm_sym"),
                    F.col("expiration") == F.col("atm_exp"),
                    F.col("strike") == F.col("atm_strike"),
                ],
                how="inner",
            )
            .drop("atm_sym", "atm_exp", "atm_strike")
        )

    short_puts  = _join_atm(valid, "P").alias("sp")
    short_calls = _join_atm(valid, "C").alias("sc")
    long_puts   = valid.filter(F.col("right") == "P").alias("lp")
    long_calls  = valid.filter(F.col("right") == "C").alias("lc")

    put_spread = (
        short_puts.join(long_puts, on=[
            F.col("sp.symbol") == F.col("lp.symbol"),
            F.col("sp.expiration") == F.col("lp.expiration"),
            F.col("lp.strike") < F.col("sp.strike"),
        ], how="inner")
        .select(
            F.col("sp.symbol").alias("symbol"),
            F.col("sp.expiration").alias("expiration"),
            F.col("sp.strike").alias("atm_strike"),
            F.col("lp.strike").alias("put_wing_strike"),
            F.col("sp.mid").alias("short_put_mid"),
            F.col("lp.mid").alias("long_put_mid"),
            (F.col("sp.mid") - F.col("lp.mid")).alias("put_credit_per_share"),
            (F.col("sp.strike") - F.col("lp.strike")).alias("put_wing_width"),
            F.col("sp.underlying_price").alias("underlying_price"),
            F.col("sp.dte").alias("dte"),
            F.col("sp.delta").alias("short_put_delta"),
            F.col("lp.delta").alias("long_put_delta"),
            F.col("sp.implied_vol").alias("atm_put_iv"),
        ).alias("ps")
    )

    call_spread = (
        short_calls.join(long_calls, on=[
            F.col("sc.symbol") == F.col("lc.symbol"),
            F.col("sc.expiration") == F.col("lc.expiration"),
            F.col("lc.strike") > F.col("sc.strike"),
        ], how="inner")
        .select(
            F.col("sc.symbol").alias("symbol"),
            F.col("sc.expiration").alias("expiration"),
            F.col("sc.strike").alias("atm_strike"),
            F.col("lc.strike").alias("call_wing_strike"),
            F.col("sc.mid").alias("short_call_mid"),
            F.col("lc.mid").alias("long_call_mid"),
            (F.col("sc.mid") - F.col("lc.mid")).alias("call_credit_per_share"),
            (F.col("lc.strike") - F.col("sc.strike")).alias("call_wing_width"),
            F.col("sc.delta").alias("short_call_delta"),
            F.col("lc.delta").alias("long_call_delta"),
            F.col("sc.implied_vol").alias("atm_call_iv"),
        ).alias("cs")
    )

    butterflies = put_spread.join(call_spread, on=[
        F.col("ps.symbol")         == F.col("cs.symbol"),
        F.col("ps.expiration")     == F.col("cs.expiration"),
        F.col("ps.atm_strike")     == F.col("cs.atm_strike"),
        F.col("ps.put_wing_width") == F.col("cs.call_wing_width"),  # symmetric
    ], how="inner")

    total_credit_ps = F.col("ps.put_credit_per_share") + F.col("cs.call_credit_per_share")
    wing_width      = F.col("ps.put_wing_width")
    total_credit_c  = total_credit_ps * 100
    max_loss_c      = (wing_width - total_credit_ps) * 100
    spread_ratio    = (total_credit_ps / wing_width) * 100
    credit_yield    = F.when(max_loss_c > 0, total_credit_c / max_loss_c * 100)
    roc             = (total_credit_ps / F.col("ps.atm_strike")) * 100
    lower_be        = F.col("ps.atm_strike") - total_credit_ps
    upper_be        = F.col("ps.atm_strike") + total_credit_ps

    result = (
        butterflies
        .filter(total_credit_ps > 0)
        .filter(max_loss_c > 0)
        .select(
            F.col("ps.symbol").alias("symbol"),
            F.col("ps.expiration").alias("expiration"),
            F.col("ps.dte").alias("dte"),
            F.col("ps.atm_strike").alias("atm_strike"),
            F.col("ps.put_wing_strike").alias("put_wing_strike"),
            F.col("cs.call_wing_strike").alias("call_wing_strike"),
            wing_width.alias("wing_width"),
            F.col("ps.short_put_mid").alias("short_put_mid"),
            F.col("cs.short_call_mid").alias("short_call_mid"),
            F.col("ps.long_put_mid").alias("long_put_mid"),
            F.col("cs.long_call_mid").alias("long_call_mid"),
            total_credit_c.alias("total_credit"),
            total_credit_c.alias("max_profit"),
            max_loss_c.alias("max_loss"),
            spread_ratio.alias("spread_ratio"),
            credit_yield.alias("credit_yield"),
            roc.alias("roc"),
            lower_be.alias("lower_breakeven"),
            upper_be.alias("upper_breakeven"),
            F.col("ps.underlying_price").alias("underlying_price"),
            (F.abs(F.col("ps.underlying_price") - F.col("ps.atm_strike"))
             / F.col("ps.underlying_price") * 100).alias("pct_to_atm_strike"),
            (F.abs(F.col("ps.underlying_price") - lower_be)
             / F.col("ps.underlying_price") * 100).alias("pct_to_lower_breakeven"),
            (F.abs(F.col("ps.underlying_price") - upper_be)
             / F.col("ps.underlying_price") * 100).alias("pct_to_upper_breakeven"),
            F.col("ps.short_put_delta").alias("short_put_delta"),
            F.col("ps.long_put_delta").alias("long_put_delta"),
            F.col("cs.short_call_delta").alias("short_call_delta"),
            F.col("cs.long_call_delta").alias("long_call_delta"),
            F.col("ps.atm_put_iv").alias("atm_put_iv"),
            F.col("cs.atm_call_iv").alias("atm_call_iv"),
        )
        .orderBy(F.col("spread_ratio").desc())
    )

    logger.info("Generated %d iron butterfly combinations", result.count())
    return result

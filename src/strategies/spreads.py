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

    Exactly four canonical strategies are produced:

    **Credit spreads** (net premium received up-front):

    - *Put Credit Spread* — sell ATM/OTM put, buy lower-strike OTM put.
      Max profit = net credit.  Max loss = width − net credit.
    - *Call Credit Spread* — sell ATM/OTM call, buy higher-strike OTM call.
      Max profit = net credit.  Max loss = width − net credit.

    **Debit spreads** (net premium paid up-front):

    - *Put Debit Spread* — buy ATM/ITM put, sell lower-strike OTM put.
      Max profit = width − net debit.  Max loss = net debit.
    - *Call Debit Spread* — buy ATM/ITM call, sell higher-strike OTM call.
      Max profit = width − net debit.  Max loss = net debit.

    Logic
    -----
    1. **Leg selection** — all contracts with a valid ``mid`` price.
       Short legs must be OTM or ATM.
    2. **Self-join** — pair each short leg with every other contract
       of the same symbol / expiration / right.
    3. **Metric calculation** — net premium, width, max profit, max loss,
       spread ratio, ROC, breakeven, % to strike, % to breakeven,
       and net Greeks for the spread.
    4. **Strategy labelling** — derived from strike ordering:
       ``Put Credit``, ``Call Credit``, ``Put Debit``, ``Call Debit``.
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

    short_right = F.col("s.right")
    short_strike = F.col("s.strike")
    long_strike = F.col("l.strike")
    underlying_price = F.col("s.underlying_price")
    short_mid = F.col("s.mid")
    long_mid = F.col("l.mid")
    short_delta = F.col("s.delta")
    long_delta = F.col("l.delta")
    short_gamma = F.col("s.gamma")
    long_gamma = F.col("l.gamma")
    short_theta = F.col("s.theta")
    long_theta = F.col("l.theta")
    short_vega = F.col("s.vega")
    long_vega = F.col("l.vega")

    # net_premium > 0 → credit received;  < 0 → debit paid
    net_premium = short_mid - long_mid
    width = F.abs(short_strike - long_strike)
    is_credit = net_premium >= 0

    # Per-share max profit / max loss (always positive values)
    raw_max_profit = F.when(is_credit, net_premium).otherwise(width + net_premium)
    raw_max_loss = F.when(is_credit, width - net_premium).otherwise(F.abs(net_premium))

    max_profit = raw_max_profit * 100   # ×100 shares per contract
    max_loss = raw_max_loss * 100       # ×100 shares per contract
    credit = net_premium * 100

    spread_ratio = (F.abs(net_premium) / width) * 100
    credit_yield = F.when(raw_max_loss > 0, (raw_max_profit / raw_max_loss) * 100)
    roc = (F.abs(net_premium) / short_strike) * 100

    # Breakeven:
    #   Credit spreads → based on short strike
    #     Put Credit:   short_strike − premium
    #     Call Credit:  short_strike + premium
    #   Debit spreads → based on long strike
    #     Call Debit:   long_strike + |premium|
    #     Put Debit:    long_strike − |premium|
    breakeven = F.when(
        is_credit,
        F.when(short_right == "P",
               short_strike - net_premium
        ).otherwise(
               short_strike + net_premium
        ),
    ).otherwise(
        F.when(short_right == "P",
               long_strike + net_premium      # net_premium < 0
        ).otherwise(
               long_strike - net_premium      # net_premium < 0
        ),
    )

    pct_to_short_strike = (F.abs(underlying_price - short_strike) / underlying_price) * 100
    pct_to_long_strike = (F.abs(underlying_price - long_strike) / underlying_price) * 100
    pct_to_breakeven = (F.abs(underlying_price - breakeven) / underlying_price) * 100
    net_delta = short_delta - long_delta
    net_gamma = short_gamma - long_gamma
    net_theta = short_theta - long_theta
    net_vega = short_vega - long_vega

    # ── strategy label ───────────────────────────────────────────────
    # Strike ordering is the canonical classifier (4 strategies):
    #   PUT  short > long  →  Put Credit Spread   (sell higher put, buy lower put)
    #   PUT  short < long  →  Put Debit Spread    (sell lower put, buy higher put)
    #   CALL short < long  →  Call Credit Spread  (sell lower call, buy higher call)
    #   CALL short > long  →  Call Debit Spread   (sell higher call, buy lower call)
    strategy_label = (
        F.when(
            (short_right == "P") & (short_strike > long_strike),
            F.lit("Put Credit Spread"),
        )
        .when(
            (short_right == "P") & (short_strike < long_strike),
            F.lit("Put Debit Spread"),
        )
        .when(
            (short_right == "C") & (short_strike < long_strike),
            F.lit("Call Credit Spread"),
        )
        .when(
            (short_right == "C") & (short_strike > long_strike),
            F.lit("Call Debit Spread"),
        )
        .otherwise(F.lit("Unknown"))
    )

    result = (
        spreads
        .select(
            F.col("s.symbol").alias("symbol"),
            F.col("s.expiration").alias("expiration"),
            short_right.alias("right"),
            strategy_label.alias("strategy"),
            underlying_price.alias("underlying_price"),
            spread_ratio.alias("spread_ratio"),
            short_strike.alias("short_strike"),
            pct_to_short_strike.alias("pct_to_short_strike"),
            short_mid.alias("short_mid"),
            short_delta.alias("short_delta"),
            long_strike.alias("long_strike"),
            pct_to_long_strike.alias("pct_to_long_strike"),
            long_mid.alias("long_mid"),
            long_delta.alias("long_delta"),
            credit.alias("credit"),
            width.alias("width"),
            max_profit.alias("max_profit"),
            max_loss.alias("max_loss"),
            credit_yield.alias("credit_yield"),
            roc.alias("ROC"),
            breakeven.alias("breakeven"),
            pct_to_breakeven.alias("pct_to_breakeven"),
            F.col("s.dte").alias("dte"),
            net_delta.alias("net_delta"),
            net_gamma.alias("net_gamma"),
            net_theta.alias("net_theta"),
            net_vega.alias("net_vega"),
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
    Build iron condors by combining the best put-credit and best call-credit
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

    put_credit = F.col("p.credit")
    call_credit = F.col("c.credit")
    put_width = F.col("p.width")
    call_width = F.col("c.width")
    put_short_strike = F.col("p.short_strike")
    call_short_strike = F.col("c.short_strike")
    underlying_price = F.col("p.underlying_price")

    # p.credit and c.credit are already ×100 (from spreads output)
    total_credit = put_credit + call_credit

    # width is per-share; multiply by 100 for contract-level
    wider_width_contract = F.greatest(put_width, call_width) * 100
    ic_max_loss = wider_width_contract - total_credit
    ic_ratio = (total_credit / wider_width_contract) * 100
    ic_credit_yield = F.when(ic_max_loss > 0, (total_credit / ic_max_loss) * 100)
    # ROC: per-share total credit / average of the two short strikes
    ic_per_share_credit = total_credit / 100
    avg_short_strike = (put_short_strike + call_short_strike) / 2
    ic_roc = (ic_per_share_credit / avg_short_strike) * 100

    # Breakevens use per-share credit (÷100) against per-share strikes
    per_share_put_credit = put_credit / 100
    per_share_call_credit = call_credit / 100
    lower_be = put_short_strike - per_share_put_credit
    upper_be = call_short_strike + per_share_call_credit

    result = condors.select(
        F.col("p.symbol").alias("symbol"),
        F.col("p.expiration").alias("expiration"),
        put_short_strike.alias("put_short_strike"),
        F.col("p.long_strike").alias("put_long_strike"),
        call_short_strike.alias("call_short_strike"),
        F.col("c.long_strike").alias("call_long_strike"),
        put_credit.alias("put_credit"),
        call_credit.alias("call_credit"),
        total_credit.alias("total_credit"),
        put_width.alias("put_width"),
        call_width.alias("call_width"),
        total_credit.alias("max_profit"),
        ic_max_loss.alias("max_loss"),
        ic_ratio.alias("spread_ratio"),
        ic_credit_yield.alias("credit_yield"),
        ic_roc.alias("ROC"),
        lower_be.alias("lower_breakeven"),
        upper_be.alias("upper_breakeven"),
        underlying_price.alias("underlying_price"),
        (F.abs(underlying_price - lower_be) / underlying_price * 100).alias(
            "pct_to_lower_breakeven"
        ),
        (F.abs(underlying_price - upper_be) / underlying_price * 100).alias(
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
            roc.alias("ROC"),
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

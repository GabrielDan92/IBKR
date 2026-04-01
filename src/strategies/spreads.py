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
    Generate all vertical credit-spread combinations and rank them.

    Logic
    -----
    1. **Leg selection** — all contracts with a valid ``mid`` price.
    2. **Self-join** — pair each potential short leg with every
       further-OTM contract of the same symbol / expiration / right
       (the long leg).
    3. **Metric calculation** — credit, width, max profit, max loss,
       spread ratio, ROC, breakeven, % to strike, % to breakeven,
       and net Greeks for the spread.
    4. **Ranking** — ordered by ``spread_ratio DESC`` (largest credit
       yield first).

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
    #
    # Bull put spread:  short put at higher strike, long put at lower
    #   → credit = short mid − long mid
    #   → long put strike < short put strike
    #
    # Bear call spread: short call at lower strike, long call at higher
    #   → credit = short mid − long mid
    #   → long call strike > short call strike

    join_cond = [
        F.col("s.symbol") == F.col("l.symbol"),
        F.col("s.expiration") == F.col("l.expiration"),
        F.col("s.right") == F.col("l.right"),
        F.col("s.strike") != F.col("l.strike"),
        # Long leg is further OTM than the short leg
        F.when(
            F.col("s.right") == "P",
            F.col("l.strike") < F.col("s.strike"),
        ).otherwise(
            F.col("l.strike") > F.col("s.strike"),
        ),
    ]

    spreads = short_legs.join(long_legs, on=join_cond, how="inner")

    # ── metrics ──────────────────────────────────────────────────────

    credit = F.col("s.mid") - F.col("l.mid")
    width = F.abs(F.col("s.strike") - F.col("l.strike"))
    raw_max_loss = width - credit     # per-share max loss
    max_profit = credit * 100         # ×100 shares per contract
    max_loss = raw_max_loss * 100     # ×100 shares per contract
    spread_ratio = (credit / width) * 100
    roc = F.when(raw_max_loss > 0, (credit / raw_max_loss) * 100)

    # Breakeven:
    #   Bull put → short_strike − credit  (want price to stay above)
    #   Bear call → short_strike + credit  (want price to stay below)
    breakeven = F.when(
        F.col("s.right") == "P",
        F.col("s.strike") - credit,
    ).otherwise(
        F.col("s.strike") + credit,
    )

    pct_to_short_strike = (F.abs(
        F.col("s.underlying_price") - F.col("s.strike")
    ) / F.col("s.underlying_price")) * 100

    pct_to_breakeven = (F.abs(
        F.col("s.underlying_price") - breakeven
    ) / F.col("s.underlying_price")) * 100

    strategy_label = F.when(
        F.col("s.right") == "P", F.lit("Bull Put")
    ).otherwise(F.lit("Bear Call"))

    result = (
        spreads
        .select(
            F.col("s.symbol").alias("symbol"),
            F.col("s.expiration").alias("expiration"),
            strategy_label.alias("strategy"),
            F.col("s.strike").alias("short_strike"),
            F.col("l.strike").alias("long_strike"),
            F.col("s.mid").alias("short_mid"),
            F.col("l.mid").alias("long_mid"),
            F.col("s.delta").alias("short_delta"),
            F.col("l.delta").alias("long_delta"),
            (credit * 100).alias("credit"),
            width.alias("width"),
            max_profit.alias("max_profit"),
            max_loss.alias("max_loss"),
            spread_ratio.alias("spread_ratio"),
            roc.alias("roc"),
            breakeven.alias("breakeven"),
            F.col("s.underlying_price").alias("underlying_price"),
            pct_to_short_strike.alias("pct_to_short_strike"),
            pct_to_breakeven.alias("pct_to_breakeven"),
            F.col("s.dte").alias("dte"),
            # Net Greeks (short − long because we *sell* the short leg)
            (F.col("s.delta") - F.col("l.delta")).alias("net_delta"),
            (F.col("s.gamma") - F.col("l.gamma")).alias("net_gamma"),
            (F.col("s.theta") - F.col("l.theta")).alias("net_theta"),
            (F.col("s.vega") - F.col("l.vega")).alias("net_vega"),
        )
        .filter(F.col("credit") > 0)  # only valid credit spreads
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

    # ── pick the top spread per (symbol, expiration, strategy) ───────
    window = Window.partitionBy("symbol", "expiration", "strategy").orderBy(
        F.col("spread_ratio").desc()
    )

    top_spreads = (
        spreads_df
        .withColumn("_rank", F.row_number().over(window))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )

    put_side = top_spreads.filter(F.col("strategy") == "Bull Put").alias("p")
    call_side = top_spreads.filter(F.col("strategy") == "Bear Call").alias("c")

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
    ic_roc = F.when(ic_max_loss > 0, (total_credit / ic_max_loss) * 100)

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
        ic_roc.alias("roc"),
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

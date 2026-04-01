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

from config.constants import DELTA_TOLERANCE, SHORT_DELTA

logger = logging.getLogger(__name__)


# ── public ───────────────────────────────────────────────────────────


def calculate_spreads(
    chain_df: DataFrame,
    *,
    short_delta: float = SHORT_DELTA,
    delta_tolerance: float = DELTA_TOLERANCE,
) -> DataFrame:
    """
    Generate all vertical credit-spread combinations and rank them.

    Logic
    -----
    1. **Short-leg selection** — contracts whose ``|delta|`` falls within
       ``[short_delta − tolerance, short_delta + tolerance]``.
    2. **Self-join** — pair each short leg with every further-OTM contract
       of the same symbol / expiration / right (the long leg).
    3. **Metric calculation** — credit, width, max profit, max loss,
       spread ratio, ROC, breakeven, % to strike, % to breakeven,
       and net Greeks for the spread.
    4. **Ranking** — ordered by ``spread_ratio DESC`` (largest credit
       yield first).

    Parameters
    ----------
    chain_df:
        Raw option-chain DataFrame (``OPTION_CHAIN_SCHEMA``).
    short_delta:
        Target absolute delta for the short leg.
    delta_tolerance:
        ± window around ``short_delta``.

    Returns
    -------
    DataFrame
        One row per spread pair with all computed metrics.
    """
    # Ensure delta is available
    chain_with_delta = chain_df.filter(F.col("delta").isNotNull())

    # ── short legs ───────────────────────────────────────────────────
    short_legs = (
        chain_with_delta
        .filter(
            F.abs(F.col("delta")).between(
                short_delta - delta_tolerance,
                short_delta + delta_tolerance,
            )
        )
        .alias("s")
    )

    # ── long legs (all contracts — filtering happens in join) ────────
    long_legs = chain_with_delta.alias("l")

    # ── self-join ────────────────────────────────────────────────────
    #
    # Bull put spread:  short put at higher strike, long put at lower
    #   → collect credit = short bid − long ask
    #   → short put delta ≈ −0.30  (negative for puts)
    #   → long put strike < short put strike
    #
    # Bear call spread: short call at lower strike, long call at higher
    #   → collect credit = short bid − long ask
    #   → short call delta ≈ +0.30
    #   → long call strike > short call strike

    join_cond = [
        F.col("s.symbol") == F.col("l.symbol"),
        F.col("s.expiration") == F.col("l.expiration"),
        F.col("s.right") == F.col("l.right"),
        # Long leg is further OTM than the short leg
        F.when(
            F.col("s.right") == "P",
            F.col("l.strike") < F.col("s.strike"),
        ).otherwise(
            F.col("l.strike") > F.col("s.strike"),
        ),
        # Long leg must have *lower* |delta| (further OTM)
        F.abs(F.col("l.delta")) < F.abs(F.col("s.delta")),
    ]

    spreads = short_legs.join(long_legs, on=join_cond, how="inner")

    # ── metrics ──────────────────────────────────────────────────────

    credit = F.col("s.bid") - F.col("l.ask")
    width = F.abs(F.col("s.strike") - F.col("l.strike"))
    max_profit = credit
    max_loss = width - credit
    spread_ratio = credit / width
    roc = F.when(max_loss > 0, credit / max_loss)

    # Breakeven:
    #   Bull put → short_strike − credit  (want price to stay above)
    #   Bear call → short_strike + credit  (want price to stay below)
    breakeven = F.when(
        F.col("s.right") == "P",
        F.col("s.strike") - credit,
    ).otherwise(
        F.col("s.strike") + credit,
    )

    pct_to_short_strike = F.abs(
        F.col("s.underlying_price") - F.col("s.strike")
    ) / F.col("s.underlying_price")

    pct_to_breakeven = F.abs(
        F.col("s.underlying_price") - breakeven
    ) / F.col("s.underlying_price")

    strategy_label = F.when(
        F.col("s.right") == "P", F.lit("bull_put")
    ).otherwise(F.lit("bear_call"))

    result = (
        spreads
        .select(
            F.col("s.symbol").alias("symbol"),
            F.col("s.expiration").alias("expiration"),
            strategy_label.alias("strategy"),
            F.col("s.strike").alias("short_strike"),
            F.col("l.strike").alias("long_strike"),
            F.col("s.bid").alias("short_bid"),
            F.col("l.ask").alias("long_ask"),
            F.col("s.delta").alias("short_delta"),
            F.col("l.delta").alias("long_delta"),
            credit.alias("credit"),
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
    chain_df: DataFrame,
    *,
    short_delta: float = SHORT_DELTA,
    delta_tolerance: float = DELTA_TOLERANCE,
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
    short_delta / delta_tolerance:
        Passed through to ``calculate_spreads``.

    Returns
    -------
    DataFrame
        One row per iron condor with combined metrics.
    """
    spreads = calculate_spreads(
        chain_df,
        short_delta=short_delta,
        delta_tolerance=delta_tolerance,
    )

    # ── pick the top spread per (symbol, expiration, strategy) ───────
    window = Window.partitionBy("symbol", "expiration", "strategy").orderBy(
        F.col("spread_ratio").desc()
    )

    top_spreads = (
        spreads
        .withColumn("_rank", F.row_number().over(window))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )

    put_side = top_spreads.filter(F.col("strategy") == "bull_put").alias("p")
    call_side = top_spreads.filter(F.col("strategy") == "bear_call").alias("c")

    condor_join = [
        F.col("p.symbol") == F.col("c.symbol"),
        F.col("p.expiration") == F.col("c.expiration"),
    ]

    condors = put_side.join(call_side, on=condor_join, how="inner")

    total_credit = F.col("p.credit") + F.col("c.credit")
    wider_width = F.greatest(F.col("p.width"), F.col("c.width"))
    ic_max_loss = wider_width - total_credit
    ic_ratio = total_credit / wider_width
    ic_roc = F.when(ic_max_loss > 0, total_credit / ic_max_loss)

    lower_be = F.col("p.short_strike") - F.col("p.credit")
    upper_be = F.col("c.short_strike") + F.col("c.credit")

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
        (F.abs(F.col("p.underlying_price") - lower_be) / F.col("p.underlying_price")).alias(
            "pct_to_lower_breakeven"
        ),
        (F.abs(F.col("p.underlying_price") - upper_be) / F.col("p.underlying_price")).alias(
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

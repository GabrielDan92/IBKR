"""
Short strangle strategy calculations.

A short strangle = sell an OTM put + sell an OTM call on the same
underlying for the same expiration.  Premium is collected up-front;
risk is theoretically unlimited on the call side and down to zero
on the put side (in practice limited by margin requirements).

Public API
----------
- ``calculate_strangles(df, ...)`` → DataFrame of strangle combinations

Accepts the raw option-chain DataFrame (``OPTION_CHAIN_SCHEMA``).
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def calculate_strangles(
    chain_df: DataFrame,
) -> DataFrame:
    """
    Pair OTM puts with OTM calls to form short strangles.

    Logic
    -----
    1. **Leg selection** — all OTM puts and all OTM calls with a valid bid.
    2. **Self-join** — pair every put with every call on the same symbol
       and expiration.
    3. **Metrics** — total premium, upper / lower breakevens, %
       distance to strikes and breakevens, aggregate Greeks.
    4. **Ranking** — ordered by ``total_premium DESC``.

    Parameters
    ----------
    chain_df:
        Raw option-chain DataFrame.

    Returns
    -------
    DataFrame
        One row per strangle combination with computed metrics.
    """
    valid = chain_df.filter(F.col("mid").isNotNull())

    # ── put legs (OTM puts) ─────────────────────────────────────
    put_legs = (
        valid
        .filter(F.col("right") == "P")
        .filter(F.col("strike") < F.col("underlying_price"))
        .alias("p")
    )

    # ── call legs (OTM calls) ───────────────────────────────────
    call_legs = (
        valid
        .filter(F.col("right") == "C")
        .filter(F.col("strike") > F.col("underlying_price"))
        .alias("c")
    )

    # ── pair puts with calls (same symbol, same expiration) ─────────
    join_cond = [
        F.col("p.symbol") == F.col("c.symbol"),
        F.col("p.expiration") == F.col("c.expiration"),
    ]

    strangles = put_legs.join(call_legs, on=join_cond, how="inner")

    # ── metrics ──────────────────────────────────────────────────────
    premium_per_share = F.col("p.mid") + F.col("c.mid")
    total_premium_contract = premium_per_share * 100
    lower_be = F.col("p.strike") - premium_per_share
    upper_be = F.col("c.strike") + premium_per_share
    breakeven_width = upper_be - lower_be

    pct_to_put_strike = (F.abs(
        F.col("p.underlying_price") - F.col("p.strike")
    ) / F.col("p.underlying_price")) * 100

    pct_to_call_strike = (F.abs(
        F.col("c.underlying_price") - F.col("c.strike")
    ) / F.col("c.underlying_price")) * 100

    pct_to_lower_be = (F.abs(
        F.col("p.underlying_price") - lower_be
    ) / F.col("p.underlying_price")) * 100

    pct_to_upper_be = (F.abs(
        F.col("c.underlying_price") - upper_be
    ) / F.col("c.underlying_price")) * 100

    # ROC: premium received / average of the two strikes
    avg_strike = (F.col("p.strike") + F.col("c.strike")) / 2
    roc = (premium_per_share / avg_strike) * 100

    result = (
        strangles
        .select(
            F.col("p.symbol").alias("symbol"),
            F.col("p.expiration").alias("expiration"),
            F.col("p.underlying_price").alias("underlying_price"),
            F.col("p.strike").alias("put_strike"),
            F.col("c.strike").alias("call_strike"),
            F.col("p.mid").alias("put_mid"),
            F.col("c.mid").alias("call_mid"),
            total_premium_contract.alias("total_premium"),
            lower_be.alias("lower_breakeven"),
            upper_be.alias("upper_breakeven"),
            breakeven_width.alias("breakeven_width"),
            pct_to_put_strike.alias("pct_to_put_strike"),
            pct_to_call_strike.alias("pct_to_call_strike"),
            pct_to_lower_be.alias("pct_to_lower_breakeven"),
            pct_to_upper_be.alias("pct_to_upper_breakeven"),
            roc.alias("ROC"),
            F.col("p.dte").alias("dte"),
            # Net Greeks (both legs are short → add deltas directly)
            (F.col("p.delta") + F.col("c.delta")).alias("net_delta"),
            (F.col("p.gamma") + F.col("c.gamma")).alias("net_gamma"),
            (F.col("p.theta") + F.col("c.theta")).alias("net_theta"),
            (F.col("p.vega") + F.col("c.vega")).alias("net_vega"),
            F.col("p.delta").alias("put_delta"),
            F.col("c.delta").alias("call_delta"),
            F.col("p.implied_vol").alias("put_implied_vol"),
            F.col("c.implied_vol").alias("call_implied_vol"),
        )
        .filter(premium_per_share > 0)
        .orderBy(F.col("total_premium").desc())
    )

    logger.info("Generated %d strangle combinations", result.count())
    return result

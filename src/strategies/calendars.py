"""
Calendar (horizontal) spread calculations.

A calendar spread = sell a near-term option + buy a same-strike farther-term
option on the same underlying.  Profit comes from the near-term leg decaying
faster (higher theta) than the long leg.

Public API
----------
- ``calculate_calendars(chain_df)`` → DataFrame of calendar spread candidates
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def calculate_calendars(chain_df: DataFrame) -> DataFrame:
    """
    Pair near-term (short) legs with farther-term (long) legs at the same
    strike and right to form calendar spreads.

    Logic
    -----
    1. Cross-join the chain with itself on (symbol, right, strike),
       requiring near_expiration < far_expiration.
    2. Net debit = far_mid − near_mid  (positive = net debit paid, which is
       the typical case since farther-term options are more expensive).
    3. Max profit is theoretically the value of the long leg at near expiry
       minus the debit paid — approximated here as near_mid (the value
       "captured" from the short leg decaying to zero).
    4. Theta differential = near_theta − far_theta  (should be positive;
       a larger value means faster near-term decay).
    5. Only keep combinations where the near DTE ≤ far DTE − 7 (at least
       one week apart) to ensure meaningful theta differential.

    Parameters
    ----------
    chain_df:
        Raw option-chain DataFrame (``OPTION_CHAIN_SCHEMA``).

    Returns
    -------
    DataFrame
        One row per calendar spread candidate, ordered by theta_differential
        descending (highest near-term decay first).
    """
    valid = chain_df.filter(F.col("mid").isNotNull())

    near_legs = valid.alias("n")
    far_legs  = valid.alias("f")

    join_cond = [
        F.col("n.symbol")     == F.col("f.symbol"),
        F.col("n.right")      == F.col("f.right"),
        F.col("n.strike")     == F.col("f.strike"),
        F.col("n.expiration") <  F.col("f.expiration"),   # near < far
        F.col("f.dte")        >= F.col("n.dte") + 7,      # at least 7-day gap
    ]

    calendars = near_legs.join(far_legs, on=join_cond, how="inner")

    net_debit = F.col("f.mid") - F.col("n.mid")          # almost always positive
    # Approximate max profit: near premium received if short leg expires worthless
    approx_max_profit = F.col("n.mid") * 100              # ×100 shares/contract
    net_debit_contract = net_debit * 100
    # Theta differential: how much faster near leg decays per day
    theta_differential = F.col("n.theta") - F.col("f.theta")
    iv_differential    = F.col("n.implied_vol") - F.col("f.implied_vol")
    dte_gap = F.col("f.dte") - F.col("n.dte")

    result = (
        calendars
        .filter(net_debit > 0)   # far leg more expensive than near (expected)
        .select(
            F.col("n.symbol").alias("symbol"),
            F.col("n.right").alias("right"),
            F.col("n.strike").alias("strike"),
            F.col("n.expiration").alias("near_expiration"),
            F.col("f.expiration").alias("far_expiration"),
            F.col("n.dte").alias("near_dte"),
            F.col("f.dte").alias("far_dte"),
            dte_gap.alias("dte_gap"),
            F.col("n.mid").alias("near_mid"),
            F.col("f.mid").alias("far_mid"),
            net_debit_contract.alias("net_debit"),           # cost to open, ×100
            approx_max_profit.alias("approx_max_profit"),    # if short expires 0
            (approx_max_profit / net_debit_contract * 100).alias("approx_roc"),
            theta_differential.alias("theta_differential"),  # near − far (should be >0)
            iv_differential.alias("iv_differential"),        # near IV − far IV
            F.col("n.underlying_price").alias("underlying_price"),
            (
                F.abs(F.col("n.underlying_price") - F.col("n.strike"))
                / F.col("n.underlying_price") * 100
            ).alias("pct_to_strike"),
            F.col("n.delta").alias("near_delta"),
            F.col("f.delta").alias("far_delta"),
            F.col("n.implied_vol").alias("near_iv"),
            F.col("f.implied_vol").alias("far_iv"),
            F.col("n.theta").alias("near_theta"),
            F.col("f.theta").alias("far_theta"),
        )
        .orderBy(F.col("theta_differential").desc())
    )

    logger.info("Generated %d calendar spread candidates", result.count())
    return result

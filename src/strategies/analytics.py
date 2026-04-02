"""
Market analytics derived from the raw option chain — no extra API calls needed.

Public API
----------
- ``calculate_expected_move(chain_df)``  → one row per symbol/expiration
- ``calculate_max_pain(chain_df)``       → one row per symbol/expiration
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def calculate_expected_move(chain_df: DataFrame) -> DataFrame:
    """
    ATM straddle price = market-implied one-standard-deviation expected move.

    expected_move         = ATM_call_mid + ATM_put_mid    (per share)
    expected_move_contract = expected_move × 100
    move_pct              = expected_move / underlying × 100
    upper_bound / lower_bound  = underlying ± expected_move
    """
    valid = chain_df.filter(F.col("mid").isNotNull())

    atm_window = Window.partitionBy("symbol", "expiration", "right").orderBy(
        F.abs(F.col("strike") - F.col("underlying_price"))
    )

    atm_contracts = (
        valid
        .withColumn("_atm_rank", F.row_number().over(atm_window))
        .filter(F.col("_atm_rank") == 1)
        .drop("_atm_rank")
    )

    atm_calls = (
        atm_contracts.filter(F.col("right") == "C")
        .select(
            "symbol", "expiration",
            F.col("strike").alias("atm_call_strike"),
            F.col("mid").alias("atm_call_mid"),
            F.col("underlying_price"),
            F.col("dte"),
            F.col("implied_vol").alias("atm_call_iv"),
        )
    )

    atm_puts = (
        atm_contracts.filter(F.col("right") == "P")
        .select(
            "symbol", "expiration",
            F.col("strike").alias("atm_put_strike"),
            F.col("mid").alias("atm_put_mid"),
            F.col("implied_vol").alias("atm_put_iv"),
        )
    )

    joined = atm_calls.join(atm_puts, on=["symbol", "expiration"], how="inner")

    expected_move = F.col("atm_call_mid") + F.col("atm_put_mid")
    move_pct = (expected_move / F.col("underlying_price")) * 100
    avg_iv = (F.col("atm_call_iv") + F.col("atm_put_iv")) / 2

    result = (
        joined.select(
            "symbol", "expiration", "dte", "underlying_price",
            "atm_call_strike", "atm_put_strike",
            "atm_call_mid", "atm_put_mid",
            expected_move.alias("expected_move"),
            (expected_move * 100).alias("expected_move_contract"),
            move_pct.alias("move_pct"),
            (F.col("underlying_price") + expected_move).alias("upper_bound"),
            (F.col("underlying_price") - expected_move).alias("lower_bound"),
            avg_iv.alias("avg_atm_iv"),
        )
        .orderBy("symbol", "expiration")
    )

    logger.info("Computed expected move for %d symbol/expiration pairs", result.count())
    return result


def calculate_max_pain(chain_df: DataFrame) -> DataFrame:
    """
    Max-pain strike = the strike at which total ITM option pain is minimised.

    For each candidate strike K:
        call_pain(K) = Σ  (K − C_strike) × C_OI   for all calls with strike < K
        put_pain(K)  = Σ  (P_strike − K) × P_OI   for all puts  with strike > K
        total_pain   = call_pain + put_pain

    The strike minimising total_pain is the max-pain point.
    """
    valid = chain_df.filter(
        F.col("open_interest").isNotNull() & (F.col("open_interest") > 0)
    )

    calls = valid.filter(F.col("right") == "C").alias("c")
    puts  = valid.filter(F.col("right") == "P").alias("p")

    candidates = (
        valid.select("symbol", "expiration", "strike", "underlying_price", "dte")
        .distinct()
        .alias("k")
    )

    call_pain = (
        calls.join(
            candidates,
            on=[
                F.col("c.symbol") == F.col("k.symbol"),
                F.col("c.expiration") == F.col("k.expiration"),
                F.col("c.strike") < F.col("k.strike"),
            ],
            how="inner",
        )
        .groupBy(
            F.col("k.symbol"),
            F.col("k.expiration"),
            F.col("k.strike").alias("candidate_strike"),
            F.col("k.underlying_price"),
            F.col("k.dte"),
        )
        .agg(
            F.sum(
                (F.col("k.strike") - F.col("c.strike")) * F.col("c.open_interest")
            ).alias("call_pain")
        )
    )

    put_pain = (
        puts.join(
            candidates,
            on=[
                F.col("p.symbol") == F.col("k.symbol"),
                F.col("p.expiration") == F.col("k.expiration"),
                F.col("p.strike") > F.col("k.strike"),
            ],
            how="inner",
        )
        .groupBy(
            F.col("k.symbol"),
            F.col("k.expiration"),
            F.col("k.strike").alias("candidate_strike"),
        )
        .agg(
            F.sum(
                (F.col("p.strike") - F.col("k.strike")) * F.col("p.open_interest")
            ).alias("put_pain")
        )
    )

    total_pain = (
        call_pain
        .join(put_pain, on=["symbol", "expiration", "candidate_strike"], how="left")
        .withColumn("put_pain", F.coalesce(F.col("put_pain"), F.lit(0.0)))
        .withColumn("total_pain", F.col("call_pain") + F.col("put_pain"))
    )

    pain_window = Window.partitionBy("symbol", "expiration").orderBy("total_pain")

    result = (
        total_pain
        .withColumn("_rank", F.row_number().over(pain_window))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
        .withColumnRenamed("candidate_strike", "max_pain_strike")
        .select(
            "symbol", "expiration", "dte", "underlying_price",
            "max_pain_strike",
            "call_pain", "put_pain", "total_pain",
            (
                (F.col("max_pain_strike") - F.col("underlying_price"))
                / F.col("underlying_price") * 100
            ).alias("pct_from_underlying"),
        )
        .orderBy("symbol", "expiration")
    )

    logger.info("Computed max pain for %d symbol/expiration pairs", result.count())
    return result

"""
PySpark ``StructType`` schemas for DataFrames used across the application.

Defining schemas explicitly (rather than relying on inference) ensures
type safety, catches upstream changes early, and makes Spark skip the
costly schema-inference pass when creating DataFrames.
"""

from __future__ import annotations

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# ── Option Chain ─────────────────────────────────────────────────────

OPTION_CHAIN_SCHEMA = StructType(
    [
        StructField("symbol", StringType(), nullable=False),
        StructField("expiration", StringType(), nullable=False),   # YYYYMMDD
        StructField("strike", DoubleType(), nullable=False),
        StructField("right", StringType(), nullable=False),         # C or P
        StructField("bid", DoubleType(), nullable=True),
        StructField("ask", DoubleType(), nullable=True),
        StructField("last", DoubleType(), nullable=True),
        StructField("mid", DoubleType(), nullable=True),
        StructField("delta", DoubleType(), nullable=True),
        StructField("gamma", DoubleType(), nullable=True),
        StructField("theta", DoubleType(), nullable=True),
        StructField("vega", DoubleType(), nullable=True),
        StructField("implied_vol", DoubleType(), nullable=True),
        StructField("open_interest", DoubleType(), nullable=True),
        StructField("volume", DoubleType(), nullable=True),
        StructField("underlying_price", DoubleType(), nullable=False),
        StructField("dte", IntegerType(), nullable=False),
    ]
)

# ── Spread (result of strategies.spreads) ────────────────────────────

SPREAD_SCHEMA = StructType(
    [
        StructField("symbol", StringType(), nullable=False),
        StructField("expiration", StringType(), nullable=False),
        StructField("strategy", StringType(), nullable=False),          # bull_put, bear_call
        StructField("short_strike", DoubleType(), nullable=False),
        StructField("long_strike", DoubleType(), nullable=False),
        StructField("short_mid", DoubleType(), nullable=True),
        StructField("long_mid", DoubleType(), nullable=True),
        StructField("short_delta", DoubleType(), nullable=True),
        StructField("long_delta", DoubleType(), nullable=True),
        StructField("credit", DoubleType(), nullable=True),
        StructField("width", DoubleType(), nullable=False),
        StructField("max_profit", DoubleType(), nullable=True),
        StructField("max_loss", DoubleType(), nullable=True),
        StructField("spread_ratio", DoubleType(), nullable=True),      # credit / width
        StructField("roc", DoubleType(), nullable=True),               # return on capital
        StructField("breakeven", DoubleType(), nullable=True),
        StructField("underlying_price", DoubleType(), nullable=False),
        StructField("pct_to_short_strike", DoubleType(), nullable=True),
        StructField("pct_to_breakeven", DoubleType(), nullable=True),
        StructField("dte", IntegerType(), nullable=False),
        StructField("net_delta", DoubleType(), nullable=True),
        StructField("net_gamma", DoubleType(), nullable=True),
        StructField("net_theta", DoubleType(), nullable=True),
        StructField("net_vega", DoubleType(), nullable=True),
    ]
)

# ── Iron Condor (result of strategies.spreads) ───────────────────────

IRON_CONDOR_SCHEMA = StructType(
    [
        StructField("symbol", StringType(), nullable=False),
        StructField("expiration", StringType(), nullable=False),
        StructField("put_short_strike", DoubleType(), nullable=False),
        StructField("put_long_strike", DoubleType(), nullable=False),
        StructField("call_short_strike", DoubleType(), nullable=False),
        StructField("call_long_strike", DoubleType(), nullable=False),
        StructField("put_credit", DoubleType(), nullable=True),
        StructField("call_credit", DoubleType(), nullable=True),
        StructField("total_credit", DoubleType(), nullable=True),
        StructField("put_width", DoubleType(), nullable=False),
        StructField("call_width", DoubleType(), nullable=False),
        StructField("max_profit", DoubleType(), nullable=True),
        StructField("max_loss", DoubleType(), nullable=True),
        StructField("spread_ratio", DoubleType(), nullable=True),
        StructField("roc", DoubleType(), nullable=True),
        StructField("lower_breakeven", DoubleType(), nullable=True),
        StructField("upper_breakeven", DoubleType(), nullable=True),
        StructField("underlying_price", DoubleType(), nullable=False),
        StructField("pct_to_lower_breakeven", DoubleType(), nullable=True),
        StructField("pct_to_upper_breakeven", DoubleType(), nullable=True),
        StructField("dte", IntegerType(), nullable=False),
        StructField("net_delta", DoubleType(), nullable=True),
        StructField("net_gamma", DoubleType(), nullable=True),
        StructField("net_theta", DoubleType(), nullable=True),
        StructField("net_vega", DoubleType(), nullable=True),
    ]
)

# ── Strangle (result of strategies.strangles) ────────────────────────

STRANGLE_SCHEMA = StructType(
    [
        StructField("symbol", StringType(), nullable=False),
        StructField("expiration", StringType(), nullable=False),
        StructField("put_strike", DoubleType(), nullable=False),
        StructField("call_strike", DoubleType(), nullable=False),
        StructField("put_mid", DoubleType(), nullable=True),
        StructField("call_mid", DoubleType(), nullable=True),
        StructField("total_premium", DoubleType(), nullable=True),
        StructField("lower_breakeven", DoubleType(), nullable=True),
        StructField("upper_breakeven", DoubleType(), nullable=True),
        StructField("breakeven_width", DoubleType(), nullable=True),
        StructField("underlying_price", DoubleType(), nullable=False),
        StructField("pct_to_put_strike", DoubleType(), nullable=True),
        StructField("pct_to_call_strike", DoubleType(), nullable=True),
        StructField("pct_to_lower_breakeven", DoubleType(), nullable=True),
        StructField("pct_to_upper_breakeven", DoubleType(), nullable=True),
        StructField("dte", IntegerType(), nullable=False),
        StructField("net_delta", DoubleType(), nullable=True),
        StructField("net_gamma", DoubleType(), nullable=True),
        StructField("net_theta", DoubleType(), nullable=True),
        StructField("net_vega", DoubleType(), nullable=True),
        StructField("put_delta", DoubleType(), nullable=True),
        StructField("call_delta", DoubleType(), nullable=True),
        StructField("put_implied_vol", DoubleType(), nullable=True),
        StructField("call_implied_vol", DoubleType(), nullable=True),
    ]
)

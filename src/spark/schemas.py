"""PySpark schema definitions used by the application.

Only the raw option-chain schema is currently enforced explicitly when
converting live IBKR rows into a Spark DataFrame.
"""

from __future__ import annotations

from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

OPTION_CHAIN_SCHEMA = StructType(
    [
        StructField("symbol", StringType(), nullable=False),
        StructField("expiration", DateType(), nullable=False),
        StructField("strike", DoubleType(), nullable=False),
        StructField("right", StringType(), nullable=False),         # C or P
        StructField("bid", DoubleType(), nullable=True),
        StructField("ask", DoubleType(), nullable=True),
        StructField("last", DoubleType(), nullable=True),
        StructField("close", DoubleType(), nullable=True),
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


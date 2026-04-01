"""
Tests for strangle strategy calculations.

Uses synthetic option-chain data so tests run without an IB Gateway
connection.
"""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from src.spark.schemas import OPTION_CHAIN_SCHEMA
from src.strategies.strangles import calculate_strangles


# ── synthetic data ───────────────────────────────────────────────────

_CHAIN_DATA = [
    # OTM puts  (underlying @ 100)
    {"symbol": "TEST", "expiration": "20260515", "strike": 95.0, "right": "P",
     "bid": 1.50, "ask": 1.70, "last": 1.60, "mid": 1.60,
     "delta": -0.30, "gamma": 0.03, "theta": -0.05, "vega": 0.15,
     "implied_vol": 0.25, "open_interest": 500.0, "volume": 100.0,
     "underlying_price": 100.0, "dte": 44},
    {"symbol": "TEST", "expiration": "20260515", "strike": 92.0, "right": "P",
     "bid": 0.90, "ask": 1.10, "last": 1.00, "mid": 1.00,
     "delta": -0.20, "gamma": 0.02, "theta": -0.03, "vega": 0.10,
     "implied_vol": 0.23, "open_interest": 300.0, "volume": 50.0,
     "underlying_price": 100.0, "dte": 44},
    # OTM calls  (underlying @ 100)
    {"symbol": "TEST", "expiration": "20260515", "strike": 105.0, "right": "C",
     "bid": 1.40, "ask": 1.60, "last": 1.50, "mid": 1.50,
     "delta": 0.30, "gamma": 0.03, "theta": -0.05, "vega": 0.15,
     "implied_vol": 0.25, "open_interest": 600.0, "volume": 120.0,
     "underlying_price": 100.0, "dte": 44},
    {"symbol": "TEST", "expiration": "20260515", "strike": 108.0, "right": "C",
     "bid": 0.80, "ask": 1.00, "last": 0.90, "mid": 0.90,
     "delta": 0.20, "gamma": 0.02, "theta": -0.03, "vega": 0.10,
     "implied_vol": 0.23, "open_interest": 350.0, "volume": 60.0,
     "underlying_price": 100.0, "dte": 44},
]


@pytest.fixture()
def chain_df(spark: SparkSession):
    return spark.createDataFrame(_CHAIN_DATA, schema=OPTION_CHAIN_SCHEMA)


# ── strangle tests ───────────────────────────────────────────────────


class TestCalculateStrangles:
    def test_returns_strangles(self, chain_df):
        result = calculate_strangles(chain_df)
        assert result.count() > 0

    def test_total_premium_is_sum_of_bids(self, chain_df):
        result = calculate_strangles(chain_df)
        for row in result.collect():
            expected = row.put_bid + row.call_bid
            assert abs(row.total_premium - expected) < 1e-6

    def test_put_strike_below_underlying(self, chain_df):
        result = calculate_strangles(chain_df)
        for row in result.collect():
            assert row.put_strike < row.underlying_price

    def test_call_strike_above_underlying(self, chain_df):
        result = calculate_strangles(chain_df)
        for row in result.collect():
            assert row.call_strike > row.underlying_price

    def test_lower_breakeven_below_put_strike(self, chain_df):
        result = calculate_strangles(chain_df)
        for row in result.collect():
            assert row.lower_breakeven < row.put_strike

    def test_upper_breakeven_above_call_strike(self, chain_df):
        result = calculate_strangles(chain_df)
        for row in result.collect():
            assert row.upper_breakeven > row.call_strike

    def test_ordered_by_total_premium_desc(self, chain_df):
        result = calculate_strangles(chain_df)
        rows = result.collect()
        premiums = [r.total_premium for r in rows]
        assert premiums == sorted(premiums, reverse=True)

    def test_net_delta_is_sum(self, chain_df):
        result = calculate_strangles(chain_df)
        for row in result.collect():
            expected = row.put_delta + row.call_delta
            assert abs(row.net_delta - expected) < 1e-6

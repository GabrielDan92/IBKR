"""Tests for iron butterfly calculations."""

from __future__ import annotations
import datetime
import pytest
from pyspark.sql import SparkSession
from src.spark.schemas import OPTION_CHAIN_SCHEMA
from src.strategies.spreads import calculate_iron_butterflies

_EXPIRY = datetime.date(2026, 5, 15)
_CHAIN_DATA = [
    {"symbol": "TEST", "expiration": _EXPIRY, "strike": 95.0,  "right": "P",
     "bid": 1.50, "ask": 1.70, "mid": 1.60, "last": 1.60, "delta": -0.30,
     "gamma": 0.03, "theta": -0.05, "vega": 0.15, "implied_vol": 0.25,
     "open_interest": 500.0, "volume": 100.0, "underlying_price": 100.0, "dte": 44},
    {"symbol": "TEST", "expiration": _EXPIRY, "strike": 100.0, "right": "P",
     "bid": 3.00, "ask": 3.20, "mid": 3.10, "last": 3.10, "delta": -0.50,
     "gamma": 0.05, "theta": -0.08, "vega": 0.20, "implied_vol": 0.28,
     "open_interest": 800.0, "volume": 200.0, "underlying_price": 100.0, "dte": 44},
    {"symbol": "TEST", "expiration": _EXPIRY, "strike": 100.0, "right": "C",
     "bid": 2.80, "ask": 3.00, "mid": 2.90, "last": 2.90, "delta":  0.50,
     "gamma": 0.05, "theta": -0.08, "vega": 0.20, "implied_vol": 0.26,
     "open_interest": 750.0, "volume": 180.0, "underlying_price": 100.0, "dte": 44},
    {"symbol": "TEST", "expiration": _EXPIRY, "strike": 105.0, "right": "C",
     "bid": 1.40, "ask": 1.60, "mid": 1.50, "last": 1.50, "delta":  0.30,
     "gamma": 0.03, "theta": -0.05, "vega": 0.15, "implied_vol": 0.25,
     "open_interest": 600.0, "volume": 120.0, "underlying_price": 100.0, "dte": 44},
]

@pytest.fixture()
def chain_df(spark: SparkSession):
    return spark.createDataFrame(_CHAIN_DATA, schema=OPTION_CHAIN_SCHEMA)

@pytest.fixture()
def butterflies_df(chain_df):
    return calculate_iron_butterflies(chain_df)

class TestIronButterflies:
    def test_returns_rows(self, butterflies_df):
        assert butterflies_df.count() > 0

    def test_atm_strike_is_nearest_to_underlying(self, butterflies_df):
        for row in butterflies_df.collect():
            assert row.atm_strike == 100.0

    def test_put_wing_below_atm(self, butterflies_df):
        for row in butterflies_df.collect():
            assert row.put_wing_strike < row.atm_strike

    def test_call_wing_above_atm(self, butterflies_df):
        for row in butterflies_df.collect():
            assert row.call_wing_strike > row.atm_strike

    def test_symmetric_wings(self, butterflies_df):
        for row in butterflies_df.collect():
            put_width  = row.atm_strike - row.put_wing_strike
            call_width = row.call_wing_strike - row.atm_strike
            assert abs(put_width - call_width) < 1e-6

    def test_total_credit_positive(self, butterflies_df):
        for row in butterflies_df.collect():
            assert row.total_credit > 0

    def test_max_profit_equals_total_credit(self, butterflies_df):
        for row in butterflies_df.collect():
            assert abs(row.max_profit - row.total_credit) < 1e-4

    def test_max_loss_formula(self, butterflies_df):
        """max_profit + max_loss = wing_width × 100."""
        for row in butterflies_df.collect():
            assert abs(row.max_profit + row.max_loss - row.wing_width * 100) < 1e-4

    def test_breakeven_formulas(self, butterflies_df):
        for row in butterflies_df.collect():
            per_share = row.total_credit / 100
            assert abs(row.lower_breakeven - (row.atm_strike - per_share)) < 1e-4
            assert abs(row.upper_breakeven - (row.atm_strike + per_share)) < 1e-4

    def test_spread_ratio_formula(self, butterflies_df):
        for row in butterflies_df.collect():
            expected = (row.total_credit / 100) / row.wing_width * 100
            assert abs(row.spread_ratio - expected) < 1e-4

"""Tests for expected move and max pain analytics."""

from __future__ import annotations
import datetime
import pytest
from pyspark.sql import SparkSession
from src.spark.schemas import OPTION_CHAIN_SCHEMA
from src.strategies.analytics import calculate_expected_move, calculate_max_pain

_EXPIRY = datetime.date(2026, 5, 15)
_CHAIN_DATA = [
    {"symbol": "TEST", "expiration": _EXPIRY, "strike": 95.0,  "right": "P",
     "bid": 1.50, "ask": 1.70, "last": 1.60, "mid": 1.60, "delta": -0.30,
     "gamma": 0.03, "theta": -0.05, "vega": 0.15, "implied_vol": 0.25,
     "open_interest": 500.0, "volume": 100.0, "underlying_price": 100.0, "dte": 44},
    {"symbol": "TEST", "expiration": _EXPIRY, "strike": 100.0, "right": "P",
     "bid": 3.00, "ask": 3.20, "last": 3.10, "mid": 3.10, "delta": -0.50,
     "gamma": 0.05, "theta": -0.08, "vega": 0.20, "implied_vol": 0.28,
     "open_interest": 800.0, "volume": 200.0, "underlying_price": 100.0, "dte": 44},
    {"symbol": "TEST", "expiration": _EXPIRY, "strike": 105.0, "right": "P",
     "bid": 5.50, "ask": 5.90, "last": 5.70, "mid": 5.70, "delta": -0.70,
     "gamma": 0.03, "theta": -0.05, "vega": 0.14, "implied_vol": 0.27,
     "open_interest": 300.0, "volume": 50.0,  "underlying_price": 100.0, "dte": 44},
    {"symbol": "TEST", "expiration": _EXPIRY, "strike": 100.0, "right": "C",
     "bid": 2.80, "ask": 3.00, "last": 2.90, "mid": 2.90, "delta":  0.50,
     "gamma": 0.05, "theta": -0.08, "vega": 0.20, "implied_vol": 0.26,
     "open_interest": 750.0, "volume": 180.0, "underlying_price": 100.0, "dte": 44},
    {"symbol": "TEST", "expiration": _EXPIRY, "strike": 105.0, "right": "C",
     "bid": 1.40, "ask": 1.60, "last": 1.50, "mid": 1.50, "delta":  0.30,
     "gamma": 0.03, "theta": -0.05, "vega": 0.15, "implied_vol": 0.25,
     "open_interest": 600.0, "volume": 120.0, "underlying_price": 100.0, "dte": 44},
    {"symbol": "TEST", "expiration": _EXPIRY, "strike": 95.0,  "right": "C",
     "bid": 5.20, "ask": 5.60, "last": 5.40, "mid": 5.40, "delta":  0.70,
     "gamma": 0.03, "theta": -0.05, "vega": 0.14, "implied_vol": 0.27,
     "open_interest": 280.0, "volume": 40.0,  "underlying_price": 100.0, "dte": 44},
]

@pytest.fixture()
def chain_df(spark: SparkSession):
    return spark.createDataFrame(_CHAIN_DATA, schema=OPTION_CHAIN_SCHEMA)

class TestExpectedMove:
    def test_returns_rows(self, chain_df):
        result = calculate_expected_move(chain_df)
        assert result.count() > 0

    def test_atm_strike_is_closest_to_underlying(self, chain_df):
        result = calculate_expected_move(chain_df)
        for row in result.collect():
            assert row.atm_call_strike == 100.0
            assert row.atm_put_strike  == 100.0

    def test_expected_move_equals_sum_of_atm_mids(self, chain_df):
        result = calculate_expected_move(chain_df)
        for row in result.collect():
            expected = row.atm_call_mid + row.atm_put_mid
            assert abs(row.expected_move - expected) < 1e-6

    def test_expected_move_contract_is_100x(self, chain_df):
        result = calculate_expected_move(chain_df)
        for row in result.collect():
            assert abs(row.expected_move_contract - row.expected_move * 100) < 1e-6

    def test_bounds_are_symmetric(self, chain_df):
        result = calculate_expected_move(chain_df)
        for row in result.collect():
            assert abs(row.upper_bound - (row.underlying_price + row.expected_move)) < 1e-6
            assert abs(row.lower_bound - (row.underlying_price - row.expected_move)) < 1e-6

    def test_move_pct_formula(self, chain_df):
        result = calculate_expected_move(chain_df)
        for row in result.collect():
            expected = row.expected_move / row.underlying_price * 100
            assert abs(row.move_pct - expected) < 1e-6

class TestMaxPain:
    def test_returns_rows(self, chain_df):
        result = calculate_max_pain(chain_df)
        assert result.count() > 0

    def test_one_row_per_symbol_expiration(self, chain_df):
        result = calculate_max_pain(chain_df)
        total = result.count()
        distinct = result.select("symbol", "expiration").distinct().count()
        assert total == distinct

    def test_max_pain_strike_is_valid_strike(self, chain_df):
        result = calculate_max_pain(chain_df)
        valid_strikes = {row.strike for row in chain_df.collect()}
        for row in result.collect():
            assert row.max_pain_strike in valid_strikes

    def test_pct_from_underlying_formula(self, chain_df):
        result = calculate_max_pain(chain_df)
        for row in result.collect():
            expected = (row.max_pain_strike - row.underlying_price) / row.underlying_price * 100
            assert abs(row.pct_from_underlying - expected) < 1e-6

"""
Tests for vertical spread and iron condor calculations.

Uses synthetic option-chain data so tests run without an IB Gateway
connection.
"""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from src.spark.schemas import OPTION_CHAIN_SCHEMA
from src.strategies.spreads import calculate_iron_condors, calculate_spreads


# ── synthetic data ───────────────────────────────────────────────────

_CHAIN_DATA = [
    # OTM puts  (underlying @ 100)
    {"symbol": "TEST", "expiration": "20260515", "strike": 95.0, "right": "P",
     "bid": 1.50, "ask": 1.70, "last": 1.60, "mid": 1.60,
     "delta": -0.30, "gamma": 0.03, "theta": -0.05, "vega": 0.15,
     "implied_vol": 0.25, "open_interest": 500.0, "volume": 100.0,
     "underlying_price": 100.0, "dte": 44},
    {"symbol": "TEST", "expiration": "20260515", "strike": 90.0, "right": "P",
     "bid": 0.80, "ask": 1.00, "last": 0.90, "mid": 0.90,
     "delta": -0.18, "gamma": 0.02, "theta": -0.03, "vega": 0.10,
     "implied_vol": 0.23, "open_interest": 300.0, "volume": 50.0,
     "underlying_price": 100.0, "dte": 44},
    {"symbol": "TEST", "expiration": "20260515", "strike": 85.0, "right": "P",
     "bid": 0.30, "ask": 0.50, "last": 0.40, "mid": 0.40,
     "delta": -0.08, "gamma": 0.01, "theta": -0.01, "vega": 0.05,
     "implied_vol": 0.22, "open_interest": 200.0, "volume": 30.0,
     "underlying_price": 100.0, "dte": 44},
    # OTM calls  (underlying @ 100)
    {"symbol": "TEST", "expiration": "20260515", "strike": 105.0, "right": "C",
     "bid": 1.40, "ask": 1.60, "last": 1.50, "mid": 1.50,
     "delta": 0.30, "gamma": 0.03, "theta": -0.05, "vega": 0.15,
     "implied_vol": 0.25, "open_interest": 600.0, "volume": 120.0,
     "underlying_price": 100.0, "dte": 44},
    {"symbol": "TEST", "expiration": "20260515", "strike": 110.0, "right": "C",
     "bid": 0.70, "ask": 0.90, "last": 0.80, "mid": 0.80,
     "delta": 0.18, "gamma": 0.02, "theta": -0.03, "vega": 0.10,
     "implied_vol": 0.23, "open_interest": 350.0, "volume": 60.0,
     "underlying_price": 100.0, "dte": 44},
    {"symbol": "TEST", "expiration": "20260515", "strike": 115.0, "right": "C",
     "bid": 0.25, "ask": 0.45, "last": 0.35, "mid": 0.35,
     "delta": 0.08, "gamma": 0.01, "theta": -0.01, "vega": 0.05,
     "implied_vol": 0.22, "open_interest": 180.0, "volume": 25.0,
     "underlying_price": 100.0, "dte": 44},
]


@pytest.fixture()
def chain_df(spark: SparkSession):
    return spark.createDataFrame(_CHAIN_DATA, schema=OPTION_CHAIN_SCHEMA)


# ── spread tests ─────────────────────────────────────────────────────


class TestCalculateSpreads:
    def test_returns_only_credit_spreads(self, chain_df):
        result = calculate_spreads(chain_df)
        assert result.count() > 0
        assert result.filter("credit <= 0").count() == 0

    def test_put_spread_short_above_long(self, chain_df):
        result = calculate_spreads(chain_df)
        puts = result.filter("strategy = 'bull_put'").collect()
        for row in puts:
            assert row.short_strike > row.long_strike

    def test_call_spread_long_above_short(self, chain_df):
        result = calculate_spreads(chain_df)
        calls = result.filter("strategy = 'bear_call'").collect()
        for row in calls:
            assert row.long_strike > row.short_strike

    def test_spread_ratio_is_credit_over_width(self, chain_df):
        result = calculate_spreads(chain_df)
        for row in result.collect():
            expected_ratio = row.credit / row.width
            assert abs(row.spread_ratio - expected_ratio) < 1e-6

    def test_max_loss_equals_width_minus_credit(self, chain_df):
        result = calculate_spreads(chain_df)
        for row in result.collect():
            expected_loss = row.width - row.credit
            assert abs(row.max_loss - expected_loss) < 1e-6

    def test_ordered_by_spread_ratio_desc(self, chain_df):
        result = calculate_spreads(chain_df)
        rows = result.collect()
        ratios = [r.spread_ratio for r in rows]
        assert ratios == sorted(ratios, reverse=True)


# ── iron condor tests ────────────────────────────────────────────────


class TestCalculateIronCondors:
    def test_returns_condors(self, chain_df):
        result = calculate_iron_condors(chain_df)
        assert result.count() > 0

    def test_total_credit_is_sum_of_sides(self, chain_df):
        result = calculate_iron_condors(chain_df)
        for row in result.collect():
            expected = row.put_credit + row.call_credit
            assert abs(row.total_credit - expected) < 1e-6

    def test_put_strikes_below_call_strikes(self, chain_df):
        result = calculate_iron_condors(chain_df)
        for row in result.collect():
            assert row.put_short_strike < row.call_short_strike

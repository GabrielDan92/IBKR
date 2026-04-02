"""Tests for calendar spread calculations."""

from __future__ import annotations
import datetime
import pytest
from pyspark.sql import SparkSession
from src.spark.schemas import OPTION_CHAIN_SCHEMA
from src.strategies.calendars import calculate_calendars

_NEAR = datetime.date(2026, 5, 16)   # ~45 DTE
_FAR  = datetime.date(2026, 6, 19)   # ~79 DTE (34-day gap)

_CHAIN_DATA = [
    {"symbol": "TEST", "expiration": _NEAR, "strike": 105.0, "right": "C",
     "bid": 1.40, "ask": 1.60, "mid": 1.50, "last": 1.50, "delta": 0.30,
     "gamma": 0.03, "theta": -0.06, "vega": 0.15, "implied_vol": 0.27,
     "open_interest": 400.0, "volume": 80.0, "underlying_price": 100.0, "dte": 45},
    {"symbol": "TEST", "expiration": _FAR,  "strike": 105.0, "right": "C",
     "bid": 2.20, "ask": 2.60, "mid": 2.40, "last": 2.40, "delta": 0.32,
     "gamma": 0.02, "theta": -0.04, "vega": 0.20, "implied_vol": 0.25,
     "open_interest": 300.0, "volume": 60.0, "underlying_price": 100.0, "dte": 79},
    {"symbol": "TEST", "expiration": _NEAR, "strike": 95.0,  "right": "P",
     "bid": 1.30, "ask": 1.50, "mid": 1.40, "last": 1.40, "delta": -0.28,
     "gamma": 0.03, "theta": -0.05, "vega": 0.14, "implied_vol": 0.26,
     "open_interest": 350.0, "volume": 70.0, "underlying_price": 100.0, "dte": 45},
    {"symbol": "TEST", "expiration": _FAR,  "strike": 95.0,  "right": "P",
     "bid": 2.00, "ask": 2.40, "mid": 2.20, "last": 2.20, "delta": -0.30,
     "gamma": 0.02, "theta": -0.03, "vega": 0.18, "implied_vol": 0.24,
     "open_interest": 260.0, "volume": 50.0, "underlying_price": 100.0, "dte": 79},
]

@pytest.fixture()
def chain_df(spark: SparkSession):
    return spark.createDataFrame(_CHAIN_DATA, schema=OPTION_CHAIN_SCHEMA)

@pytest.fixture()
def calendars_df(chain_df):
    return calculate_calendars(chain_df)

class TestCalendarSpreads:
    def test_returns_rows(self, calendars_df):
        assert calendars_df.count() > 0

    def test_near_expiration_before_far(self, calendars_df):
        for row in calendars_df.collect():
            assert row.near_expiration < row.far_expiration

    def test_dte_gap_at_least_7(self, calendars_df):
        for row in calendars_df.collect():
            assert row.dte_gap >= 7

    def test_net_debit_is_far_minus_near_x100(self, calendars_df):
        for row in calendars_df.collect():
            expected = (row.far_mid - row.near_mid) * 100
            assert abs(row.net_debit - expected) < 1e-4

    def test_net_debit_positive(self, calendars_df):
        for row in calendars_df.collect():
            assert row.net_debit > 0

    def test_approx_max_profit_is_near_mid_x100(self, calendars_df):
        for row in calendars_df.collect():
            assert abs(row.approx_max_profit - row.near_mid * 100) < 1e-4

    def test_approx_roc_formula(self, calendars_df):
        for row in calendars_df.collect():
            expected = row.approx_max_profit / row.net_debit * 100
            assert abs(row.approx_roc - expected) < 1e-4

    def test_theta_differential_formula(self, calendars_df):
        for row in calendars_df.collect():
            if row.near_theta is not None and row.far_theta is not None:
                assert abs(row.theta_differential - (row.near_theta - row.far_theta)) < 1e-6

    def test_both_rights_present(self, calendars_df):
        rights = {row.right for row in calendars_df.collect()}
        assert "C" in rights
        assert "P" in rights

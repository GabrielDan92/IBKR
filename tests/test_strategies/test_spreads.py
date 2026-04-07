"""
Tests for vertical spread and iron condor calculations.

Uses synthetic option-chain data so tests run without an IB Gateway
connection.
"""

from __future__ import annotations

import datetime

import pytest
from pyspark.sql import SparkSession

from src.spark.schemas import OPTION_CHAIN_SCHEMA
from src.strategies.spreads import calculate_iron_condors, calculate_spreads


# ── synthetic data ───────────────────────────────────────────────────

_EXPIRY = datetime.date(2026, 5, 15)

_CHAIN_DATA = [
    # OTM puts  (underlying @ 100)
    {"symbol": "TEST", "expiration": _EXPIRY, "strike": 95.0, "right": "P",
     "bid": 1.50, "ask": 1.70, "last": 1.60, "mid": 1.60,
     "delta": -0.30, "gamma": 0.03, "theta": -0.05, "vega": 0.15,
     "implied_vol": 0.25, "open_interest": 500.0, "volume": 100.0,
     "underlying_price": 100.0, "dte": 44},
    {"symbol": "TEST", "expiration": _EXPIRY, "strike": 90.0, "right": "P",
     "bid": 0.80, "ask": 1.00, "last": 0.90, "mid": 0.90,
     "delta": -0.18, "gamma": 0.02, "theta": -0.03, "vega": 0.10,
     "implied_vol": 0.23, "open_interest": 300.0, "volume": 50.0,
     "underlying_price": 100.0, "dte": 44},
    {"symbol": "TEST", "expiration": _EXPIRY, "strike": 85.0, "right": "P",
     "bid": 0.30, "ask": 0.50, "last": 0.40, "mid": 0.40,
     "delta": -0.08, "gamma": 0.01, "theta": -0.01, "vega": 0.05,
     "implied_vol": 0.22, "open_interest": 200.0, "volume": 30.0,
     "underlying_price": 100.0, "dte": 44},
    # OTM calls  (underlying @ 100)
    {"symbol": "TEST", "expiration": _EXPIRY, "strike": 105.0, "right": "C",
     "bid": 1.40, "ask": 1.60, "last": 1.50, "mid": 1.50,
     "delta": 0.30, "gamma": 0.03, "theta": -0.05, "vega": 0.15,
     "implied_vol": 0.25, "open_interest": 600.0, "volume": 120.0,
     "underlying_price": 100.0, "dte": 44},
    {"symbol": "TEST", "expiration": _EXPIRY, "strike": 110.0, "right": "C",
     "bid": 0.70, "ask": 0.90, "last": 0.80, "mid": 0.80,
     "delta": 0.18, "gamma": 0.02, "theta": -0.03, "vega": 0.10,
     "implied_vol": 0.23, "open_interest": 350.0, "volume": 60.0,
     "underlying_price": 100.0, "dte": 44},
    {"symbol": "TEST", "expiration": _EXPIRY, "strike": 115.0, "right": "C",
     "bid": 0.25, "ask": 0.45, "last": 0.35, "mid": 0.35,
     "delta": 0.08, "gamma": 0.01, "theta": -0.01, "vega": 0.05,
     "implied_vol": 0.22, "open_interest": 180.0, "volume": 25.0,
     "underlying_price": 100.0, "dte": 44},
]


@pytest.fixture()
def chain_df(spark: SparkSession):
    return spark.createDataFrame(_CHAIN_DATA, schema=OPTION_CHAIN_SCHEMA)


@pytest.fixture()
def spreads_df(chain_df):
    return calculate_spreads(chain_df)


# ── spread tests ─────────────────────────────────────────────────────


class TestCalculateSpreads:
    def test_returns_spreads(self, spreads_df):
        """Both credit and debit spreads are generated."""
        assert spreads_df.count() > 0

    def test_credit_spreads_have_positive_credit(self, spreads_df):
        """Rows labelled 'Credit Spread' must have credit > 0."""
        credit_rows = spreads_df.filter("strategy LIKE '%Credit%'").collect()
        assert len(credit_rows) > 0
        for row in credit_rows:
            assert row.credit > 0

    def test_debit_spreads_have_negative_credit(self, spreads_df):
        """If any debit rows survive dedup, their credit must be < 0.

        Note: with all-OTM synthetic data every debit spread is a mirror
        of a credit spread, so dedup may eliminate all debit rows.
        """
        debit_rows = spreads_df.filter("strategy LIKE '%Debit%'").collect()
        for row in debit_rows:
            assert row.credit < 0

    def test_put_credit_short_above_long(self, spreads_df):
        puts = spreads_df.filter("strategy LIKE 'Put Credit%'").collect()
        assert len(puts) > 0
        for row in puts:
            assert row.short_strike > row.long_strike

    def test_call_credit_long_above_short(self, spreads_df):
        calls = spreads_df.filter("strategy LIKE 'Call Credit%'").collect()
        assert len(calls) > 0
        for row in calls:
            assert row.long_strike > row.short_strike

    def test_spread_ratio_formula(self, spreads_df):
        """spread_ratio = |credit/100| / width * 100 = |credit| / width."""
        for row in spreads_df.collect():
            expected = abs(row.credit) / row.width
            assert abs(row.spread_ratio - expected) < 1e-4

    def test_max_profit_plus_max_loss_equals_width_x100(self, spreads_df):
        """max_profit + max_loss = width × 100 for every spread."""
        for row in spreads_df.collect():
            expected = row.width * 100
            assert abs(row.max_profit + row.max_loss - expected) < 1e-4

    def test_ordered_by_spread_ratio_desc(self, spreads_df):
        rows = spreads_df.collect()
        ratios = [r.spread_ratio for r in rows]
        assert ratios == sorted(ratios, reverse=True)


# ── iron condor tests ────────────────────────────────────────────────


class TestCalculateIronCondors:
    def test_returns_condors(self, spreads_df):
        result = calculate_iron_condors(spreads_df)
        assert result.count() > 0

    def test_total_credit_is_sum_of_sides(self, spreads_df):
        result = calculate_iron_condors(spreads_df)
        for row in result.collect():
            expected = row.put_credit + row.call_credit
            assert abs(row.total_credit - expected) < 1e-4

    def test_put_strikes_below_call_strikes(self, spreads_df):
        result = calculate_iron_condors(spreads_df)
        for row in result.collect():
            assert row.put_short_strike < row.call_short_strike

    def test_max_profit_equals_total_credit(self, spreads_df):
        result = calculate_iron_condors(spreads_df)
        for row in result.collect():
            assert abs(row.max_profit - row.total_credit) < 1e-4

    def test_max_loss_formula(self, spreads_df):
        """max_profit + max_loss = max(put_width, call_width) × 100."""
        result = calculate_iron_condors(spreads_df)
        for row in result.collect():
            wider_x100 = max(row.put_width, row.call_width) * 100
            assert abs(row.max_profit + row.max_loss - wider_x100) < 1e-4

    def test_lower_breakeven_below_put_short_strike(self, spreads_df):
        result = calculate_iron_condors(spreads_df)
        for row in result.collect():
            assert row.lower_breakeven < row.put_short_strike

    def test_upper_breakeven_above_call_short_strike(self, spreads_df):
        result = calculate_iron_condors(spreads_df)
        for row in result.collect():
            assert row.upper_breakeven > row.call_short_strike

    def test_breakeven_formulas(self, spreads_df):
        """lower_be = put_short − put_credit/100;  upper_be = call_short + call_credit/100."""
        result = calculate_iron_condors(spreads_df)
        for row in result.collect():
            assert abs(row.lower_breakeven - (row.put_short_strike - row.put_credit / 100)) < 1e-4
            assert abs(row.upper_breakeven - (row.call_short_strike + row.call_credit / 100)) < 1e-4


# ── additional coverage: debit spreads require ITM options ────────────
# When an ITM option is added as the *long* leg, the mirror credit spread
# (which would require the ITM strike as the short leg) is excluded by the
# OTM constraint → the debit spread survives dedup.

_CHAIN_DATA_WITH_ITM = _CHAIN_DATA + [
    # ITM put (strike=105 > underlying=100): enables Put Debit Spread
    {"symbol": "TEST", "expiration": _EXPIRY, "strike": 105.0, "right": "P",
     "bid": 5.30, "ask": 5.70, "last": 5.50, "mid": 5.50,
     "delta": -0.82, "gamma": 0.02, "theta": -0.04, "vega": 0.12,
     "implied_vol": 0.25, "open_interest": 150.0, "volume": 20.0,
     "underlying_price": 100.0, "dte": 44},
    # ITM call (strike=95 < underlying=100): enables Call Debit Spread
    {"symbol": "TEST", "expiration": _EXPIRY, "strike": 95.0, "right": "C",
     "bid": 5.30, "ask": 5.70, "last": 5.50, "mid": 5.50,
     "delta": 0.82, "gamma": 0.02, "theta": -0.04, "vega": 0.12,
     "implied_vol": 0.25, "open_interest": 150.0, "volume": 20.0,
     "underlying_price": 100.0, "dte": 44},
]


@pytest.fixture()
def chain_df_with_itm(spark: SparkSession):
    return spark.createDataFrame(_CHAIN_DATA_WITH_ITM, schema=OPTION_CHAIN_SCHEMA)


@pytest.fixture()
def all_spreads_df(chain_df_with_itm):
    return calculate_spreads(chain_df_with_itm)


class TestSpreadsCoverage:
    """Covers features added after the initial test suite: debit spreads,
    OTM constraint, breakeven formulas, roc, pct columns, credit_yield,
    net Greek sign, and dedup correctness."""

    def test_short_legs_are_otm_or_atm(self, all_spreads_df):
        """Short put strike ≤ underlying; short call strike ≥ underlying."""
        for row in all_spreads_df.collect():
            if "Put" in row.strategy:
                assert row.short_strike <= row.underlying_price, (
                    f"ITM put short leg: {row.short_strike} > {row.underlying_price}"
                )
            else:
                assert row.short_strike >= row.underlying_price, (
                    f"ITM call short leg: {row.short_strike} < {row.underlying_price}"
                )

    def test_all_four_strategy_types(self, all_spreads_df):
        """Put Credit, Call Credit, Put Debit, Call Debit."""
        strategies = {row.strategy for row in all_spreads_df.collect()}
        for expected in (
            "Put Credit Spread",
            "Call Credit Spread",
            "Put Debit Spread",
            "Call Debit Spread",
        ):
            assert expected in strategies, f"Missing strategy: {expected!r}"

    def test_credit_put_breakeven(self, all_spreads_df):
        """Put Credit: breakeven = short_strike − credit/100."""
        rows = all_spreads_df.filter("strategy = 'Put Credit Spread'").collect()
        assert rows
        for row in rows:
            assert abs(row.breakeven - (row.short_strike - row.credit / 100)) < 1e-4

    def test_credit_call_breakeven(self, all_spreads_df):
        """Call Credit: breakeven = short_strike + credit/100."""
        rows = all_spreads_df.filter("strategy = 'Call Credit Spread'").collect()
        assert rows
        for row in rows:
            assert abs(row.breakeven - (row.short_strike + row.credit / 100)) < 1e-4

    def test_debit_put_breakeven(self, all_spreads_df):
        """Put Debit: breakeven = long_strike − |credit|/100."""
        rows = all_spreads_df.filter("strategy = 'Put Debit Spread'").collect()
        assert rows, "No Put Debit rows found — check ITM put fixture"
        for row in rows:
            assert abs(row.breakeven - (row.long_strike - abs(row.credit) / 100)) < 1e-4

    def test_debit_call_breakeven(self, all_spreads_df):
        """Call Debit: breakeven = long_strike + |credit|/100."""
        rows = all_spreads_df.filter("strategy = 'Call Debit Spread'").collect()
        assert rows, "No Call Debit rows found — check ITM call fixture"
        for row in rows:
            assert abs(row.breakeven - (row.long_strike + abs(row.credit) / 100)) < 1e-4

    def test_roc_formula(self, all_spreads_df):
        """roc = |net_premium_per_share| / short_strike × 100 = |credit| / short_strike."""
        for row in all_spreads_df.collect():
            expected = abs(row.credit) / row.short_strike
            assert abs(row.roc - expected) < 1e-4, (
                f"roc mismatch for {row.strategy}: {row.roc} != {expected}"
            )

    def test_pct_to_short_strike(self, all_spreads_df):
        for row in all_spreads_df.collect():
            expected = abs(row.underlying_price - row.short_strike) / row.underlying_price * 100
            assert abs(row.pct_to_short_strike - expected) < 1e-4

    def test_pct_to_long_strike(self, all_spreads_df):
        for row in all_spreads_df.collect():
            expected = abs(row.underlying_price - row.long_strike) / row.underlying_price * 100
            assert abs(row.pct_to_long_strike - expected) < 1e-4

    def test_pct_to_breakeven(self, all_spreads_df):
        for row in all_spreads_df.collect():
            expected = abs(row.underlying_price - row.breakeven) / row.underlying_price * 100
            assert abs(row.pct_to_breakeven - expected) < 1e-4

    def test_credit_yield_formula(self, all_spreads_df):
        """credit_yield = max_profit / max_loss × 100."""
        for row in all_spreads_df.collect():
            if row.max_loss and row.max_loss > 0:
                expected = row.max_profit / row.max_loss * 100
                assert abs(row.credit_yield - expected) < 1e-4

    def test_net_delta_is_short_minus_long(self, all_spreads_df):
        """net_delta = short_delta − long_delta (spread position is short − long)."""
        for row in all_spreads_df.collect():
            assert abs(row.net_delta - (row.short_delta - row.long_delta)) < 1e-6

    def test_no_duplicate_contract_pairs(self, all_spreads_df):
        "After dedup each (symbol, expiration, right, lo_strike, hi_strike) is unique."
        from pyspark.sql import functions as _F
        dups = (
            all_spreads_df
            .groupBy(
                "symbol", "expiration", "right",
                _F.least("short_strike", "long_strike").alias("lo"),
                _F.greatest("short_strike", "long_strike").alias("hi"),
            )
            .count()
            .filter("count > 1")
            .count()
        )
        assert dups == 0

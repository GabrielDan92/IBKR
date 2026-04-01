"""
IBKR Options Strategy Dashboard — Streamlit app.

Reads pre-computed Parquet files (written by the spark-app service)
from the shared ``/opt/app/data`` volume and displays interactive
tables for spreads, iron condors, and strangles.
"""

from __future__ import annotations

import os

import pandas as pd
import streamlit as st

from config.constants import OUTPUT_DIR

DATA_DIR = OUTPUT_DIR

st.set_page_config(
    page_title="IBKR Options Strategies",
    page_icon="📈",
    layout="wide",
)


# ── helpers ──────────────────────────────────────────────────────────


def _pct_cols(*names: str) -> dict:
    """Return a column_config dict that formats columns as '12.34%'."""
    return {
        name: st.column_config.NumberColumn(format="%.2f%%")
        for name in names
    }


def _dollar_cols(*names: str) -> dict:
    """Return a column_config dict that formats columns as '$1,234.56'."""
    return {
        name: st.column_config.NumberColumn(format="$%.2f")
        for name in names
    }


@st.cache_data(ttl=60)
def load_parquet(path: str) -> pd.DataFrame | None:
    """Load a Parquet directory into a Pandas DataFrame, or None if missing."""
    if not os.path.exists(path):
        return None
    try:
        return pd.read_parquet(path)
    except Exception as exc:
        st.error(f"Failed to read {path}: {exc}")
        return None


# ── sidebar filters ──────────────────────────────────────────────────

st.sidebar.title("Filters")

# Load chain to get unique symbols and expirations
chain_df = load_parquet(os.path.join(DATA_DIR, "option_chain"))

if chain_df is not None and not chain_df.empty:
    symbols = sorted(chain_df["symbol"].unique())
    selected_symbols = st.sidebar.multiselect(
        "Symbols", symbols, default=symbols
    )

    expirations = sorted(chain_df["expiration"].unique())
    selected_expirations = st.sidebar.multiselect(
        "Expirations", expirations, default=expirations
    )

    min_spread_ratio = st.sidebar.slider(
        "Min spread ratio (%)", 0.0, 100.0, 25.0, 0.5
    )
else:
    selected_symbols = []
    selected_expirations = []
    min_spread_ratio = 0.0


def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    """Apply sidebar filters to a DataFrame."""
    if "symbol" in df.columns and selected_symbols:
        df = df[df["symbol"].isin(selected_symbols)]
    if "expiration" in df.columns and selected_expirations:
        df = df[df["expiration"].isin(selected_expirations)]
    if "spread_ratio" in df.columns:
        df = df[df["spread_ratio"] >= min_spread_ratio]
    return df


# ── main content ─────────────────────────────────────────────────────

st.title("IBKR Options Scanner")

if chain_df is None:
    st.warning(
        "No data found.  Run `docker compose up spark-app` first to "
        "fetch option chains and compute strategies."
    )
    st.stop()

# ── tab layout ───────────────────────────────────────────────────────
tab_spreads, tab_condors, tab_strangles, tab_chain = st.tabs(
    ["Vertical Spreads", "Iron Condors", "Strangles", "Raw Chain"]
)

# ── Spreads ──────────────────────────────────────────────────────────
with tab_spreads:
    st.subheader("Credit / Debit Spreads")
    spreads_df = load_parquet(os.path.join(DATA_DIR, "spreads"))
    if spreads_df is not None and not spreads_df.empty:
        # Strategy filter (inside the tab so it only shows when data exists)
        strategies = sorted(spreads_df["strategy"].unique())
        selected_strategies = st.multiselect(
            "Strategy", strategies, default=strategies, key="spread_strategy"
        )
        filtered = apply_filters(spreads_df)
        if selected_strategies:
            filtered = filtered[filtered["strategy"].isin(selected_strategies)]
        st.dataframe(
            filtered.sort_values("spread_ratio", ascending=False),
            use_container_width=True,
            hide_index=True,
            column_config={
                "right": None,  # hide — info is already in strategy label
                **_pct_cols(
                    "spread_ratio", "credit_yield", "roc",
                    "pct_to_short_strike", "pct_to_breakeven",
                ),
                **_dollar_cols(
                    "short_strike", "long_strike",
                    "short_mid", "long_mid", "credit",
                    "width", "max_profit", "max_loss",
                    "breakeven", "underlying_price",
                ),
            },
        )
        st.caption(f"{len(filtered)} spreads shown")
    else:
        st.info("No spread data available yet.")

# ── Iron Condors ─────────────────────────────────────────────────────
with tab_condors:
    st.subheader("Iron Condors")
    condors_df = load_parquet(os.path.join(DATA_DIR, "iron_condors"))
    if condors_df is not None and not condors_df.empty:
        filtered = apply_filters(condors_df)
        st.dataframe(
            filtered.sort_values("spread_ratio", ascending=False),
            use_container_width=True,
            hide_index=True,
            column_config={
                **_pct_cols(
                    "spread_ratio", "credit_yield", "roc",
                    "pct_to_lower_breakeven", "pct_to_upper_breakeven",
                ),
                **_dollar_cols(
                    "put_short_strike", "put_long_strike",
                    "call_short_strike", "call_long_strike",
                    "put_credit", "call_credit", "total_credit",
                    "put_width", "call_width",
                    "max_profit", "max_loss",
                    "lower_breakeven", "upper_breakeven",
                    "underlying_price",
                ),
            },
        )
        st.caption(f"{len(filtered)} iron condors shown")
    else:
        st.info("No iron condor data available yet.")

# ── Strangles ────────────────────────────────────────────────────────
with tab_strangles:
    st.subheader("Short Strangles")
    strangles_df = load_parquet(os.path.join(DATA_DIR, "strangles"))
    if strangles_df is not None and not strangles_df.empty:
        filtered = apply_filters(strangles_df)
        st.dataframe(
            filtered.sort_values("total_premium", ascending=False),
            use_container_width=True,
            hide_index=True,
            column_config={
                **_pct_cols(
                    "roc",
                    "pct_to_put_strike", "pct_to_call_strike",
                    "pct_to_lower_breakeven", "pct_to_upper_breakeven",
                ),
                **_dollar_cols(
                    "put_strike", "call_strike",
                    "put_mid", "call_mid", "total_premium",
                    "lower_breakeven", "upper_breakeven",
                    "breakeven_width", "underlying_price",
                ),
            },
        )
        st.caption(f"{len(filtered)} strangles shown")
    else:
        st.info("No strangle data available yet.")

# ── Raw Chain ────────────────────────────────────────────────────────
with tab_chain:
    st.subheader("Raw Option Chain")
    if chain_df is not None and not chain_df.empty:
        filtered = apply_filters(chain_df)
        right_filter = st.radio("Right", ["All", "Calls (C)", "Puts (P)"], horizontal=True)
        if right_filter == "Calls (C)":
            filtered = filtered[filtered["right"] == "C"]
        elif right_filter == "Puts (P)":
            filtered = filtered[filtered["right"] == "P"]
        st.dataframe(
            filtered.sort_values(["symbol", "expiration", "strike"]),
            use_container_width=True,
            hide_index=True,
        )
        st.caption(f"{len(filtered)} contracts shown")
    else:
        st.info("No chain data available yet.")

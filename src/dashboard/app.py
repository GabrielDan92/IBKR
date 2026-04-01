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


def _pct_cols(*names: str, **labeled: str) -> dict:
    """Return column_config that formats columns as '12.34%'.

    Positional args use the column name as-is.
    Keyword args map column_name="Display Label".
    """
    cfg: dict = {
        name: st.column_config.NumberColumn(format="%.2f%%")
        for name in names
    }
    for col_name, label in labeled.items():
        cfg[col_name] = st.column_config.NumberColumn(label=label, format="%.2f%%")
    return cfg


def _dollar_cols(*names: str, **labeled: str) -> dict:
    """Return column_config that formats columns as '$1,234.56'.

    Positional args use the column name as-is.
    Keyword args map column_name="Display Label".
    """
    cfg: dict = {
        name: st.column_config.NumberColumn(format="$%.2f")
        for name in names
    }
    for col_name, label in labeled.items():
        cfg[col_name] = st.column_config.NumberColumn(label=label, format="$%.2f")
    return cfg


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
                "right": None,
                "symbol": "Symbol",
                "expiration": "Expiration",
                "strategy": "Strategy",
                "dte": "DTE",
                **_pct_cols(
                    spread_ratio="Spread Ratio",
                    credit_yield="Credit Yield",
                    roc="ROC",
                    pct_to_short_strike="% to Short Strike",
                    pct_to_long_strike="% to Long Strike",
                    pct_to_breakeven="% to Breakeven",
                ),
                **_dollar_cols(
                    short_strike="Short Strike",
                    long_strike="Long Strike",
                    short_mid="Short Price (Mid)",
                    long_mid="Long Price (Mid)",
                    credit="Credit",
                    width="Width",
                    max_profit="Max Profit",
                    max_loss="Max Loss",
                    breakeven="Breakeven",
                    underlying_price="Underlying Price",
                ),
                "short_delta": st.column_config.NumberColumn(label="Short Delta", format="%.4f"),
                "long_delta": st.column_config.NumberColumn(label="Long Delta", format="%.4f"),
                "net_delta": st.column_config.NumberColumn(label="Net Delta", format="%.4f"),
                "net_gamma": st.column_config.NumberColumn(label="Net Gamma", format="%.6f"),
                "net_theta": st.column_config.NumberColumn(label="Net Theta", format="%.4f"),
                "net_vega": st.column_config.NumberColumn(label="Net Vega", format="%.4f"),
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
                "symbol": "Symbol",
                "expiration": "Expiration",
                "dte": "DTE",
                **_pct_cols(
                    spread_ratio="Spread Ratio",
                    credit_yield="Credit Yield",
                    roc="ROC",
                    pct_to_lower_breakeven="% to Lower BE",
                    pct_to_upper_breakeven="% to Upper BE",
                ),
                **_dollar_cols(
                    put_short_strike="Put Short Strike",
                    put_long_strike="Put Long Strike",
                    call_short_strike="Call Short Strike",
                    call_long_strike="Call Long Strike",
                    put_credit="Put Credit",
                    call_credit="Call Credit",
                    total_credit="Total Credit",
                    put_width="Put Width",
                    call_width="Call Width",
                    max_profit="Max Profit",
                    max_loss="Max Loss",
                    lower_breakeven="Lower Breakeven",
                    upper_breakeven="Upper Breakeven",
                    underlying_price="Underlying Price",
                ),
                "net_delta": st.column_config.NumberColumn(label="Net Delta", format="%.4f"),
                "net_gamma": st.column_config.NumberColumn(label="Net Gamma", format="%.6f"),
                "net_theta": st.column_config.NumberColumn(label="Net Theta", format="%.4f"),
                "net_vega": st.column_config.NumberColumn(label="Net Vega", format="%.4f"),
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
                "symbol": "Symbol",
                "expiration": "Expiration",
                "dte": "DTE",
                **_pct_cols(
                    roc="ROC",
                    pct_to_put_strike="% to Put Strike",
                    pct_to_call_strike="% to Call Strike",
                    pct_to_lower_breakeven="% to Lower BE",
                    pct_to_upper_breakeven="% to Upper BE",
                ),
                **_dollar_cols(
                    put_strike="Put Strike",
                    call_strike="Call Strike",
                    put_mid="Put Price (Mid)",
                    call_mid="Call Price (Mid)",
                    total_premium="Total Premium",
                    lower_breakeven="Lower Breakeven",
                    upper_breakeven="Upper Breakeven",
                    breakeven_width="Breakeven Width",
                    underlying_price="Underlying Price",
                ),
                "put_delta": st.column_config.NumberColumn(label="Put Delta", format="%.4f"),
                "call_delta": st.column_config.NumberColumn(label="Call Delta", format="%.4f"),
                "net_delta": st.column_config.NumberColumn(label="Net Delta", format="%.4f"),
                "net_gamma": st.column_config.NumberColumn(label="Net Gamma", format="%.6f"),
                "net_theta": st.column_config.NumberColumn(label="Net Theta", format="%.4f"),
                "net_vega": st.column_config.NumberColumn(label="Net Vega", format="%.4f"),
                "put_implied_vol": st.column_config.NumberColumn(label="Put IV", format="%.4f"),
                "call_implied_vol": st.column_config.NumberColumn(label="Call IV", format="%.4f"),
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

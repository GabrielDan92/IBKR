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
    min_spread_ratio = st.sidebar.slider(
        "Min spread ratio (%)", 0.0, 100.0, 25.0, 0.5
    )
    min_pct_to_short_strike = st.sidebar.slider(
        "Min % to Short Strike (%)", 0.0, 100.0, 5.0, 0.5
    )
    min_pct_to_long_strike = st.sidebar.slider(
        "Min % to Long Strike (%)", 0.0, 100.0, 5.0, 0.5
    )
    min_credit_yield = st.sidebar.slider(
        "Min Credit Yield (%)", 0.0, 100.0, 30.0, 0.5
    )
else:
    min_spread_ratio = 0.0
    min_pct_to_short_strike = 0.0
    min_pct_to_long_strike = 0.0
    min_credit_yield = 0.0


def _header_filters(df: pd.DataFrame, key_prefix: str, extra_cols: list[str] | None = None) -> pd.DataFrame:
    """Render Excel-style dropdown filters above the table for symbol, expiration,
    and any extra categorical columns.  Returns the filtered DataFrame."""
    filter_cols = []
    if "symbol" in df.columns:
        filter_cols.append("symbol")
    if "expiration" in df.columns:
        filter_cols.append("expiration")
    if extra_cols:
        filter_cols += [c for c in extra_cols if c in df.columns]

    if not filter_cols:
        return df

    cols = st.columns(len(filter_cols))
    for col_widget, col_name in zip(cols, filter_cols):
        options = sorted(df[col_name].dropna().unique().tolist())
        chosen = col_widget.multiselect(
            col_name.replace("_", " ").title(),
            options,
            default=options,
            key=f"{key_prefix}_{col_name}",
        )
        if chosen:
            df = df[df[col_name].isin(chosen)]

    return df


def apply_spread_ratio_filter(df: pd.DataFrame) -> pd.DataFrame:
    """Apply sidebar spread filters (spread ratio, % to strikes, credit yield)."""
    if "spread_ratio" in df.columns:
        df = df[df["spread_ratio"] >= min_spread_ratio]
    if "pct_to_short_strike" in df.columns:
        df = df[df["pct_to_short_strike"] >= min_pct_to_short_strike]
    if "pct_to_long_strike" in df.columns:
        df = df[df["pct_to_long_strike"] >= min_pct_to_long_strike]
    if "credit_yield" in df.columns:
        df = df[df["credit_yield"] >= min_credit_yield]
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
tab_spreads, tab_condors, tab_butterflies, tab_calendars, tab_strangles, tab_analytics, tab_chain = st.tabs(
    ["Vertical Spreads", "Iron Condors", "Iron Butterflies", "Calendars", "Strangles", "Analytics", "Raw Chain"]
)

# ── Spreads ──────────────────────────────────────────────────────────
with tab_spreads:
    st.subheader("Credit / Debit Spreads")
    spreads_df = load_parquet(os.path.join(DATA_DIR, "spreads"))
    if spreads_df is not None and not spreads_df.empty:
        filtered = apply_spread_ratio_filter(
            _header_filters(spreads_df, "spreads", extra_cols=["strategy"])
        )
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
        filtered = apply_spread_ratio_filter(
            _header_filters(condors_df, "condors")
        )
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
        filtered = _header_filters(strangles_df, "strangles")
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

        # ── Iron Butterflies ─────────────────────────────────────────────────
with tab_butterflies:
    st.subheader("Iron Butterflies")
    butterflies_df = load_parquet(os.path.join(DATA_DIR, "iron_butterflies"))
    if butterflies_df is not None and not butterflies_df.empty:
        filtered = apply_spread_ratio_filter(
            _header_filters(butterflies_df, "butterflies")
        )
        st.dataframe(
            filtered.sort_values("spread_ratio", ascending=False),
            use_container_width=True,
            hide_index=True,
            column_config={
                "symbol": "Symbol", "expiration": "Expiration", "dte": "DTE",
                **_pct_cols(
                    spread_ratio="Spread Ratio", credit_yield="Credit Yield", roc="ROC",
                    pct_to_atm_strike="% to ATM Strike",
                    pct_to_lower_breakeven="% to Lower BE",
                    pct_to_upper_breakeven="% to Upper BE",
                ),
                **_dollar_cols(
                    atm_strike="ATM Strike",
                    put_wing_strike="Put Wing", call_wing_strike="Call Wing",
                    wing_width="Wing Width",
                    short_put_mid="Short Put Mid", short_call_mid="Short Call Mid",
                    long_put_mid="Long Put Mid", long_call_mid="Long Call Mid",
                    total_credit="Total Credit", max_profit="Max Profit", max_loss="Max Loss",
                    lower_breakeven="Lower BE", upper_breakeven="Upper BE",
                    underlying_price="Underlying",
                ),
                "short_put_delta":  st.column_config.NumberColumn(label="Short Put Δ",  format="%.4f"),
                "short_call_delta": st.column_config.NumberColumn(label="Short Call Δ", format="%.4f"),
                "atm_put_iv":  st.column_config.NumberColumn(label="ATM Put IV",  format="%.4f"),
                "atm_call_iv": st.column_config.NumberColumn(label="ATM Call IV", format="%.4f"),
            },
        )
        st.caption(f"{len(filtered)} iron butterflies shown")
    else:
        st.info("No iron butterfly data available yet.")

# ── Calendar Spreads ─────────────────────────────────────────────────
with tab_calendars:
    st.subheader("Calendar Spreads")
    calendars_df = load_parquet(os.path.join(DATA_DIR, "calendars"))
    if calendars_df is not None and not calendars_df.empty:
        filtered = _header_filters(calendars_df, "calendars", extra_cols=["right"])
        st.dataframe(
            filtered.sort_values("theta_differential", ascending=False),
            use_container_width=True,
            hide_index=True,
            column_config={
                "symbol": "Symbol", "right": "Right",
                "near_expiration": "Near Expiry", "far_expiration": "Far Expiry",
                "near_dte": "Near DTE", "far_dte": "Far DTE", "dte_gap": "DTE Gap",
                **_pct_cols(
                    approx_roc="Approx ROC",
                    pct_to_strike="% to Strike",
                ),
                **_dollar_cols(
                    strike="Strike",
                    near_mid="Near Mid", far_mid="Far Mid",
                    net_debit="Net Debit", approx_max_profit="Approx Max Profit",
                    underlying_price="Underlying",
                ),
                "theta_differential": st.column_config.NumberColumn(label="Theta Diff", format="%.4f"),
                "iv_differential":    st.column_config.NumberColumn(label="IV Diff",    format="%.4f"),
                "near_delta": st.column_config.NumberColumn(label="Near Δ", format="%.4f"),
                "far_delta":  st.column_config.NumberColumn(label="Far Δ",  format="%.4f"),
                "near_iv": st.column_config.NumberColumn(label="Near IV", format="%.4f"),
                "far_iv":  st.column_config.NumberColumn(label="Far IV",  format="%.4f"),
                "near_theta": st.column_config.NumberColumn(label="Near Θ", format="%.4f"),
                "far_theta":  st.column_config.NumberColumn(label="Far Θ",  format="%.4f"),
            },
        )
        st.caption(f"{len(filtered)} calendar spreads shown")
    else:
        st.info("No calendar spread data available yet.")

# ── Analytics: Expected Move + Max Pain ──────────────────────────────
with tab_analytics:
    st.subheader("Market Analytics")

    col_em, col_mp = st.columns(2)

    with col_em:
        st.markdown("#### Expected Move (ATM Straddle)")
        em_df = load_parquet(os.path.join(DATA_DIR, "expected_move"))
        if em_df is not None and not em_df.empty:
            filtered_em = _header_filters(em_df, "em")
            st.dataframe(
                filtered_em,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "symbol": "Symbol", "expiration": "Expiration", "dte": "DTE",
                    **_pct_cols(move_pct="Move %", avg_atm_iv="Avg ATM IV"),
                    **_dollar_cols(
                        underlying_price="Underlying",
                        atm_call_strike="ATM Call Strike", atm_put_strike="ATM Put Strike",
                        atm_call_mid="ATM Call Mid", atm_put_mid="ATM Put Mid",
                        expected_move="Expected Move ($/share)",
                        expected_move_contract="Expected Move ($/contract)",
                        upper_bound="Upper Bound", lower_bound="Lower Bound",
                    ),
                },
            )
        else:
            st.info("No expected move data yet.")

    with col_mp:
        st.markdown("#### Max Pain")
        mp_df = load_parquet(os.path.join(DATA_DIR, "max_pain"))
        if mp_df is not None and not mp_df.empty:
            filtered_mp = _header_filters(mp_df, "mp")
            st.dataframe(
                filtered_mp,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "symbol": "Symbol", "expiration": "Expiration", "dte": "DTE",
                    **_pct_cols(pct_from_underlying="% from Underlying"),
                    **_dollar_cols(
                        underlying_price="Underlying",
                        max_pain_strike="Max Pain Strike",
                    ),
                    "call_pain": st.column_config.NumberColumn(label="Call Pain", format="%,.0f"),
                    "put_pain":  st.column_config.NumberColumn(label="Put Pain",  format="%,.0f"),
                    "total_pain": st.column_config.NumberColumn(label="Total Pain", format="%,.0f"),
                },
            )
        else:
            st.info("No max pain data yet.")

# ── Raw Chain ────────────────────────────────────────────────────────
with tab_chain:
    st.subheader("Raw Option Chain")
    if chain_df is not None and not chain_df.empty:
        filtered = _header_filters(chain_df, "chain", extra_cols=["right"])
        st.dataframe(
            filtered.sort_values(["symbol", "expiration", "strike"]),
            use_container_width=True,
            hide_index=True,
        )
        st.caption(f"{len(filtered)} contracts shown")
    else:
        st.info("No chain data available yet.")

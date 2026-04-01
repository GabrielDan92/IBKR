"""
Centralised application configuration.

Every tunable value lives here.  Values are read from environment
variables at import time (injected by Docker Compose from ``.env``),
falling back to sensible defaults.  This is the **single source of
truth** — no other module should hard-code configuration values.
"""

from __future__ import annotations

import os


def _env(key: str, default: str) -> str:
    """Read an env var, returning *default* if unset or empty."""
    return os.environ.get(key, "") or default


def _env_int(key: str, default: int) -> int:
    return int(_env(key, str(default)))


def _env_float(key: str, default: float) -> float:
    return float(_env(key, str(default)))


def _env_list(key: str, default: list[str]) -> list[str]:
    """Parse a comma-separated env var into a list of upper-cased strings."""
    raw = os.environ.get(key, "")
    if raw.strip():
        return [s.strip().upper() for s in raw.split(",") if s.strip()]
    return [s.upper() for s in default]


# ═════════════════════════════════════════════════════════════════════
# IB Gateway connection
# ═════════════════════════════════════════════════════════════════════
IB_GATEWAY_HOST = _env("IB_GATEWAY_HOST", "ib-gateway")
IB_GATEWAY_PORT = _env_int("IB_GATEWAY_PORT", 4004)
IB_CLIENT_ID = _env_int("IB_CLIENT_ID", 1)

# ═════════════════════════════════════════════════════════════════════
# Market data
# ═════════════════════════════════════════════════════════════════════
DEFAULT_EXCHANGE = "SMART"
DEFAULT_CURRENCY = "USD"
MARKET_DATA_TYPE = _env_int("MARKET_DATA_TYPE", 1)  # 1=live, 3=delayed, 4=delayed-frozen

# Generic tick types requested with reqMktData:
#   100 = option volume,  101 = open interest,  106 = implied volatility
GENERIC_TICKS = "100,101,106"

# ═════════════════════════════════════════════════════════════════════
# IBKR rate-limiting / pacing
# ═════════════════════════════════════════════════════════════════════
BATCH_SIZE = 45          # concurrent reqMktData calls (stay under 50 req/s)
BATCH_PAUSE_S = 1.0      # seconds to sleep between batches
TICK_SETTLE_S = 2.0      # seconds to wait for tick data to arrive
QUALIFY_BATCH_SIZE = 100  # contracts per qualifyContractsAsync batch
QUALIFY_PAUSE_S = 0.5    # seconds between qualify batches

# ═════════════════════════════════════════════════════════════════════
# Strategy defaults
# ═════════════════════════════════════════════════════════════════════
SYMBOLS = _env_list("SYMBOLS", ["AAPL"])
TARGET_DTE = _env_int("TARGET_DTE", 45)
DTE_RANGE_DAYS = _env_int("DTE_RANGE_DAYS", 7)
SHORT_DELTA = _env_float("SHORT_DELTA", 0.30)
DELTA_TOLERANCE = _env_float("DELTA_TOLERANCE", 0.05)
STRANGLE_PUT_DELTA = _env_float("STRANGLE_PUT_DELTA", 0.30)
STRANGLE_CALL_DELTA = _env_float("STRANGLE_CALL_DELTA", 0.30)
STRIKE_RANGE_PCT = _env_float("STRIKE_RANGE_PCT", 0.15)  # keep strikes within ±15% of spot

# ═════════════════════════════════════════════════════════════════════
# Spark
# ═════════════════════════════════════════════════════════════════════
SPARK_APP_NAME = _env("SPARK_APP_NAME", "ibkr-options")
SPARK_MASTER = _env("SPARK_MASTER", "local[*]")
SPARK_SHUFFLE_PARTITIONS = _env_int("SPARK_SHUFFLE_PARTITIONS", 8)
SPARK_DRIVER_MEMORY = _env("SPARK_DRIVER_MEMORY", "16g")
SPARK_TIMEZONE = _env("SPARK_TIMEZONE", "Europe/Bucharest")
SPARK_LOG_LEVEL = _env("SPARK_LOG_LEVEL", "WARN")

# ═════════════════════════════════════════════════════════════════════
# Output
# ═════════════════════════════════════════════════════════════════════
OUTPUT_DIR = _env("OUTPUT_DIR", "/opt/app/data")

# ═════════════════════════════════════════════════════════════════════
# Dashboard
# ═════════════════════════════════════════════════════════════════════
DASHBOARD_PORT = _env_int("DASHBOARD_PORT", 8501)

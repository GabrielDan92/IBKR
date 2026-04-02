"""
Application configuration loader.

All constants are defined in .env (source of truth).
This module loads them with type conversion and sensible defaults.

Usage:
    from config.constants import IB_GATEWAY_HOST, TARGET_DTE
"""

from __future__ import annotations

import os


def _env(key: str, default: str) -> str:
    """Load env var, returning default if unset or empty."""
    return os.environ.get(key, "") or default


def _env_int(key: str, default: int) -> int:
    """Load env var as int with default."""
    return int(_env(key, str(default)))


def _env_float(key: str, default: float) -> float:
    """Load env var as float with default."""
    return float(_env(key, str(default)))


def _env_list(key: str, default: list[str]) -> list[str]:
    """Load comma-separated env var as upper-cased list."""
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
DEFAULT_EXCHANGE = _env("DEFAULT_EXCHANGE", "SMART")
DEFAULT_CURRENCY = _env("DEFAULT_CURRENCY", "USD")
MARKET_DATA_TYPE = _env_int("MARKET_DATA_TYPE", 3)  # 1=live, 3=delayed, 4=delayed-frozen
GENERIC_TICKS = _env("GENERIC_TICKS", "100,101,106")  # volume, open interest, IV

# ═════════════════════════════════════════════════════════════════════
# IBKR rate-limiting / pacing
# ═════════════════════════════════════════════════════════════════════
BATCH_SIZE = _env_int("BATCH_SIZE", 45)
BATCH_PAUSE_S = _env_float("BATCH_PAUSE_S", 1.0)
TICK_SETTLE_S = _env_float("TICK_SETTLE_S", 3.0)
QUALIFY_BATCH_SIZE = _env_int("QUALIFY_BATCH_SIZE", 100)
QUALIFY_PAUSE_S = _env_float("QUALIFY_PAUSE_S", 0.5)

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
STRIKE_RANGE_PCT = _env_float("STRIKE_RANGE_PCT", 0.20)

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
# Output & Dashboard
# ═════════════════════════════════════════════════════════════════════
OUTPUT_DIR = _env("OUTPUT_DIR", "/opt/app/data")
DASHBOARD_PORT = _env_int("DASHBOARD_PORT", 8501)

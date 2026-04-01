"""
Tests for configuration / constants loading from environment variables.
"""

from __future__ import annotations


class TestConstantsEnvOverride:
    """Verify that constants.py reads env vars at import time."""

    def test_ib_gateway_host_from_env(self, monkeypatch):
        monkeypatch.setenv("IB_GATEWAY_HOST", "custom-host")
        # Re-import to pick up the env var
        import importlib
        import config.constants as mod
        importlib.reload(mod)
        assert mod.IB_GATEWAY_HOST == "custom-host"

    def test_ib_gateway_port_from_env(self, monkeypatch):
        monkeypatch.setenv("IB_GATEWAY_PORT", "9999")
        import importlib
        import config.constants as mod
        importlib.reload(mod)
        assert mod.IB_GATEWAY_PORT == 9999

    def test_symbols_from_csv_env(self, monkeypatch):
        monkeypatch.setenv("SYMBOLS", "AAPL, MSFT , TSLA")
        import importlib
        import config.constants as mod
        importlib.reload(mod)
        assert mod.SYMBOLS == ["AAPL", "MSFT", "TSLA"]

    def test_symbols_uppercased(self, monkeypatch):
        monkeypatch.setenv("SYMBOLS", "aapl,msft")
        import importlib
        import config.constants as mod
        importlib.reload(mod)
        assert mod.SYMBOLS == ["AAPL", "MSFT"]

    def test_float_from_env(self, monkeypatch):
        monkeypatch.setenv("SHORT_DELTA", "0.25")
        import importlib
        import config.constants as mod
        importlib.reload(mod)
        assert mod.SHORT_DELTA == 0.25

    def test_defaults_when_env_unset(self, monkeypatch):
        # Clear all relevant env vars
        for key in ("IB_GATEWAY_HOST", "IB_GATEWAY_PORT", "SYMBOLS",
                     "SHORT_DELTA", "TARGET_DTE"):
            monkeypatch.delenv(key, raising=False)
        import importlib
        import config.constants as mod
        importlib.reload(mod)
        assert mod.IB_GATEWAY_HOST == "ib-gateway"
        assert mod.IB_GATEWAY_PORT == 4003
        assert mod.SYMBOLS == ["AAPL"]
        assert mod.SHORT_DELTA == 0.30
        assert mod.TARGET_DTE == 45

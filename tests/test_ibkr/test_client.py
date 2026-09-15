"""Tests for the standalone IBKR client wrapper."""

from __future__ import annotations

import asyncio

from src.ibkr.client import IBKRClient


class TestIBKRClient:
    def test_exposes_expected_public_methods(self):
        client = IBKRClient()
        for name in ("connect", "disconnect", "get_option_chain", "get_option_chains"):
            assert hasattr(client, name)

    def test_async_context_manager_calls_connect_and_disconnect(self, monkeypatch):
        client = IBKRClient()
        calls: list[str] = []

        async def fake_connect(self) -> None:
            calls.append("connect")

        async def fake_disconnect(self) -> None:
            calls.append("disconnect")

        monkeypatch.setattr(IBKRClient, "connect", fake_connect)
        monkeypatch.setattr(IBKRClient, "disconnect", fake_disconnect)

        async def run_context() -> None:
            async with client as active_client:
                assert active_client is client
                calls.append("inside")

        asyncio.run(run_context())

        assert calls == ["connect", "inside", "disconnect"]


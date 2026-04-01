"""
Abstract base class for market data clients.

Any data source (IBKR live, CSV replay, mock) must implement this interface.
This allows the strategy and Spark layers to remain completely decoupled from
the data-fetching mechanism.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from config.constants import (
    DEFAULT_CURRENCY,
    DEFAULT_EXCHANGE,
    DTE_RANGE_DAYS,
    TARGET_DTE,
)


class BaseMarketDataClient(ABC):
    """Contract that all market-data providers must satisfy."""

    # ── lifecycle ────────────────────────────────────────────────────

    @abstractmethod
    async def connect(self) -> None:
        """Establish a connection to the data source."""

    @abstractmethod
    async def disconnect(self) -> None:
        """Cleanly tear down the connection."""

    # ── option chain retrieval ───────────────────────────────────────

    @abstractmethod
    async def get_option_chain(
        self,
        symbol: str,
        *,
        exchange: str = DEFAULT_EXCHANGE,
        currency: str = DEFAULT_CURRENCY,
        target_dte: int = TARGET_DTE,
        dte_range_days: int = DTE_RANGE_DAYS,
    ) -> list[dict[str, Any]]:
        """
        Return the full option chain for *symbol* within the DTE window.

        Each dict in the returned list represents a single option contract
        and must contain at least the keys defined in
        ``src.spark.schemas.OPTION_CHAIN_SCHEMA``.
        """

    @abstractmethod
    async def get_option_chains(
        self,
        symbols: list[str],
        *,
        exchange: str = DEFAULT_EXCHANGE,
        currency: str = DEFAULT_CURRENCY,
        target_dte: int = TARGET_DTE,
        dte_range_days: int = DTE_RANGE_DAYS,
    ) -> list[dict[str, Any]]:
        """
        Return the combined option chain for every symbol in *symbols*.

        Default implementation: iterate ``get_option_chain`` per symbol.
        Subclasses may override for concurrent fetching.
        """

    # ── context-manager support ──────────────────────────────────────

    async def __aenter__(self) -> BaseMarketDataClient:
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:  # noqa: ANN001
        await self.disconnect()

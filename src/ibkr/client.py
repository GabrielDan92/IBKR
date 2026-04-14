"""
IBKR market-data client built on ``ib_async``.

Connects to IB Gateway (running via IBC inside a Docker container) and pulls
live option-chain data including Greeks, bid/ask, open interest, and volume.

Usage::

    async with IBKRClient(settings.gateway) as client:
        chain = await client.get_option_chains(["AAPL", "MSFT"])
"""

from __future__ import annotations

import asyncio
import logging
import math
from datetime import datetime, timedelta, timezone
from typing import Any

from ib_async import IB, Contract, Option, Stock, Ticker

from config.constants import (
    BATCH_PAUSE_S,
    BATCH_SIZE,
    DEFAULT_CURRENCY,
    DEFAULT_EXCHANGE,
    DTE_RANGE_DAYS,
    GENERIC_TICKS,
    IB_CLIENT_ID,
    IB_GATEWAY_HOST,
    IB_GATEWAY_PORT,
    MARKET_DATA_TYPE,
    QUALIFY_BATCH_SIZE,
    QUALIFY_PAUSE_S,
    TARGET_DTE,
    TICK_SETTLE_S,
)
logger = logging.getLogger(__name__)


class IBKRClient:
    """
    Concrete market-data provider backed by IB Gateway / TWS.

    Parameters
    ----------
    host : str
        IB Gateway hostname (default from ``constants.IB_GATEWAY_HOST``).
    port : int
        IB Gateway API port (default from ``constants.IB_GATEWAY_PORT``).
    client_id : int
        Unique client identifier (default from ``constants.IB_CLIENT_ID``).
    """

    def __init__(
        self,
        host: str = IB_GATEWAY_HOST,
        port: int = IB_GATEWAY_PORT,
        client_id: int = IB_CLIENT_ID,
    ) -> None:
        self._host = host
        self._port = port
        self._client_id = client_id
        self._ib = IB()

    async def __aenter__(self) -> "IBKRClient":
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:  # noqa: ANN001
        await self.disconnect()

    # ── lifecycle ────────────────────────────────────────────────────

    async def connect(self) -> None:
        """Connect to IB Gateway."""
        logger.info("Connecting to IB Gateway at %s:%s (clientId=%s)", self._host, self._port, self._client_id)
        try:
            await self._ib.connectAsync(
                host=self._host,
                port=self._port,
                clientId=self._client_id,
            )
        except asyncio.TimeoutError:
            raise ConnectionError(
                f"Could not connect to IB Gateway at {self._host}:{self._port}. "
                "Possible causes:\n"
                "  • Wrong TWS_USERID or TWS_PASSWORD in .env\n"
                "  • IB Gateway has not finished starting (try again in 30s)\n"
                "  • Another client is already using clientId {self._client_id}\n"
                "  • 2FA was not approved on your phone\n"
                "Check ib-gateway logs: docker compose logs ib-gateway --tail 30"
            ) from None
        except ConnectionRefusedError:
            raise ConnectionError(
                f"Connection refused by IB Gateway at {self._host}:{self._port}. "
                "The gateway is likely not running or failed to authenticate.\n"
                "Check ib-gateway logs: docker compose logs ib-gateway --tail 30"
            ) from None
        # Set market-data type (1=live, 3=delayed, 4=delayed-frozen)
        self._ib.reqMarketDataType(MARKET_DATA_TYPE)
        logger.info(
            "Connected — server version %s, marketDataType=%s",
            self._ib.client.serverVersion(),
            MARKET_DATA_TYPE,
        )

    async def disconnect(self) -> None:
        """Disconnect from IB Gateway."""
        if self._ib.isConnected():
            self._ib.disconnect()
            logger.info("Disconnected from IB Gateway")

    # ── option chain retrieval ───────────────────────────────────────

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
        Fetch the live option chain for a single equity symbol.

        1. Qualify the underlying stock contract.
        2. ``reqSecDefOptParams`` → available expirations and strikes.
        3. Filter expirations to the target DTE window.
        4. Build ``Option`` contracts for each (expiry, strike, right).
        5. ``reqMktData`` in batches to collect quotes + Greeks.
        """
        # Step 1 — qualify underlying
        underlying = Stock(symbol, exchange, currency)
        qualified = await self._ib.qualifyContractsAsync(underlying)
        if not qualified:
            logger.warning("Could not qualify underlying for %s", symbol)
            return []
        underlying = qualified[0]

        # Snapshot of the underlying price (used for % calculations later).
        # Delayed / frozen data may take a few seconds to arrive, so we
        # poll in a loop instead of sleeping once.
        underlying_ticker = self._ib.reqMktData(underlying, "", False, False)

        underlying_price: float | None = None
        max_attempts = 10
        for attempt in range(1, max_attempts + 1):
            await asyncio.sleep(1.0)
            underlying_price = _mid_price(underlying_ticker)
            if underlying_price is not None:
                break
            logger.debug(
                "%s: waiting for underlying price (attempt %d/%d) — "
                "bid=%s ask=%s last=%s close=%s marketDataType=%s",
                symbol, attempt, max_attempts,
                underlying_ticker.bid, underlying_ticker.ask,
                underlying_ticker.last, underlying_ticker.close,
                underlying_ticker.marketDataType,
            )

        self._ib.cancelMktData(underlying)

        if underlying_price is None:
            logger.warning(
                "Could not obtain underlying price for %s — "
                "bid=%s ask=%s last=%s close=%s marketDataType=%s",
                symbol,
                underlying_ticker.bid, underlying_ticker.ask,
                underlying_ticker.last, underlying_ticker.close,
                underlying_ticker.marketDataType,
            )
            return []

        logger.info("%s underlying price: %.2f", symbol, underlying_price)

        # Step 2 — option parameters
        chains = await self._ib.reqSecDefOptParamsAsync(
            underlying.symbol,
            "",
            underlying.secType,
            underlying.conId,
        )
        if not chains:
            logger.warning("No option chain parameters for %s", symbol)
            return []

        # Pick the preferred exchange chain (or first available)
        chain = next((c for c in chains if c.exchange == DEFAULT_EXCHANGE), chains[0])

        # Step 3 — filter expirations
        today = datetime.now(timezone.utc).date()
        min_expiry = today + timedelta(days=target_dte - dte_range_days)
        max_expiry = today + timedelta(days=target_dte + dte_range_days)

        valid_expiries = sorted(
            exp
            for exp in chain.expirations
            if min_expiry <= datetime.strptime(exp, "%Y%m%d").date() <= max_expiry
        )
        if not valid_expiries:
            logger.warning(
                "%s: no expirations in DTE window [%s, %s]",
                symbol,
                min_expiry,
                max_expiry,
            )
            return []

        logger.info("%s: found %d expirations in DTE window", symbol, len(valid_expiries))

        # Step 4 — build Option contracts (only strikes within range of spot)
        from config.constants import STRIKE_RANGE_PCT

        lo = underlying_price * (1 - STRIKE_RANGE_PCT)
        hi = underlying_price * (1 + STRIKE_RANGE_PCT)
        viable_strikes = sorted(s for s in chain.strikes if lo <= s <= hi)
        logger.info(
            "%s: %d strikes in [%.1f, %.1f] (of %d total)",
            symbol, len(viable_strikes), lo, hi, len(chain.strikes),
        )

        contracts: list[Option] = []
        for expiry in valid_expiries:
            for strike in viable_strikes:
                for right in ("C", "P"):
                    contracts.append(
                        Option(
                            symbol,
                            expiry,
                            strike,
                            right,
                            exchange,
                            currency=currency,
                        )
                    )

        # Qualify contracts in bulk (filters out non-existent strikes)
        qualified_contracts = await self._qualify_in_batches(contracts)
        logger.info(
            "%s: %d qualified option contracts (out of %d generated)",
            symbol,
            len(qualified_contracts),
            len(contracts),
        )

        # Step 5 — request market data in batches
        rows = await self._fetch_market_data(
            qualified_contracts, symbol, underlying_price
        )
        return rows

    async def get_option_chains(
        self,
        symbols: list[str],
        *,
        exchange: str = DEFAULT_EXCHANGE,
        currency: str = DEFAULT_CURRENCY,
        target_dte: int = TARGET_DTE,
        dte_range_days: int = DTE_RANGE_DAYS,
    ) -> list[dict[str, Any]]:
        """Fetch option chains for multiple symbols sequentially."""
        all_rows: list[dict[str, Any]] = []
        for symbol in symbols:
            rows = await self.get_option_chain(
                symbol,
                exchange=exchange,
                currency=currency,
                target_dte=target_dte,
                dte_range_days=dte_range_days,
            )
            all_rows.extend(rows)
            logger.info("%s: collected %d option rows", symbol, len(rows))
        return all_rows

    # ── internal helpers ─────────────────────────────────────────────

    async def _qualify_in_batches(
        self, contracts: list[Contract], batch_size: int = QUALIFY_BATCH_SIZE
    ) -> list[Contract]:
        """Qualify contracts in batches to avoid overloading the gateway."""
        qualified: list[Contract] = []
        for i in range(0, len(contracts), batch_size):
            batch = contracts[i : i + batch_size]
            result = await self._ib.qualifyContractsAsync(*batch)
            qualified.extend(
                c for c in result
                if c is not None and getattr(c, "conId", 0) > 0
            )
            await asyncio.sleep(QUALIFY_PAUSE_S)
        return qualified

    async def _fetch_market_data(
        self,
        contracts: list[Contract],
        symbol: str,
        underlying_price: float,
    ) -> list[dict[str, Any]]:
        """
        Request market data for *contracts* in rate-limited batches.

        Returns a list of flat dicts, one per contract, suitable for
        conversion into a Spark DataFrame.
        """
        rows: list[dict[str, Any]] = []

        for batch_idx in range(0, len(contracts), BATCH_SIZE):
            batch = contracts[batch_idx : batch_idx + BATCH_SIZE]
            tickers: list[Ticker] = []

            for contract in batch:
                ticker = self._ib.reqMktData(
                    contract, GENERIC_TICKS, snapshot=False, regulatorySnapshot=False
                )
                tickers.append(ticker)

            # Allow tick data to arrive
            await asyncio.sleep(TICK_SETTLE_S)

            for ticker in tickers:
                row = _ticker_to_row(ticker, symbol, underlying_price)
                if row is not None:
                    rows.append(row)
                self._ib.cancelMktData(ticker.contract)

            # Pace between batches
            if batch_idx + BATCH_SIZE < len(contracts):
                await asyncio.sleep(BATCH_PAUSE_S)

            logger.debug(
                "%s: processed batch %d – %d of %d contracts",
                symbol,
                batch_idx,
                min(batch_idx + BATCH_SIZE, len(contracts)),
                len(contracts),
            )

        return rows


# ── free-standing helpers ────────────────────────────────────────────


def _mid_price(ticker: Ticker) -> float | None:
    """Return best available price: mid(bid,ask) → last → close → marketPrice.

    IBKR returns -1 as a sentinel when no data is available (market closed,
    no subscription, etc.).  All branches guard against non-positive values.
    """
    bid = _safe_float(ticker.bid)
    ask = _safe_float(ticker.ask)
    if bid is not None and ask is not None and bid > 0 and ask > 0:
        return (bid + ask) / 2.0
    last = _safe_float(ticker.last)
    if last is not None and last > 0:
        return last
    close = _safe_float(ticker.close)
    if close is not None and close > 0:
        return close
    # ib_async built-in fallback (last → mid → close)
    try:
        mp = ticker.marketPrice()
        if mp is not None and math.isfinite(mp) and mp > 0:
            return mp
    except (AttributeError, TypeError):
        pass
    return None


def _safe_float(value: Any) -> float | None:
    """Return *value* as a float, or ``None`` if it is NaN / missing."""
    if value is None:
        return None
    try:
        f = float(value)
        return None if math.isnan(f) else f
    except (TypeError, ValueError):
        return None


def _ticker_to_row(
    ticker: Ticker, symbol: str, underlying_price: float
) -> dict[str, Any] | None:
    """
    Convert a ``Ticker`` snapshot into a flat dict matching the Spark schema.

    Returns ``None`` if critical fields (bid or ask) are unavailable, which
    signals a contract with no market.
    """
    contract = ticker.contract

    bid = _safe_float(ticker.bid)
    ask = _safe_float(ticker.ask)
    last = _safe_float(ticker.last)
    close = _safe_float(ticker.close)

    # Mid price: best available
    mid = _mid_price(ticker)

    # Skip contracts with no usable price at all
    if mid is None:
        return None

    # Greeks from model computation (tick type 13)
    greeks = ticker.modelGreeks
    delta = _safe_float(greeks.delta) if greeks else None
    gamma = _safe_float(greeks.gamma) if greeks else None
    theta = _safe_float(greeks.theta) if greeks else None
    vega = _safe_float(greeks.vega) if greeks else None
    implied_vol = _safe_float(greeks.impliedVol) if greeks else None

    # DTE
    expiry_date = datetime.strptime(contract.lastTradeDateOrContractMonth, "%Y%m%d").date()
    dte = (expiry_date - datetime.now(timezone.utc).date()).days

    # Open interest & volume
    open_interest = None
    volume = None
    if contract.right == "C":
        open_interest = _safe_float(getattr(ticker, "callOpenInterest", None))
        volume = _safe_float(getattr(ticker, "callVolume", None))
    else:
        open_interest = _safe_float(getattr(ticker, "putOpenInterest", None))
        volume = _safe_float(getattr(ticker, "putVolume", None))

    # Fallback: generic volume field
    if volume is None:
        volume = _safe_float(ticker.volume)

    return {
        "symbol": symbol,
        "expiration": expiry_date,
        "strike": float(contract.strike),
        "right": contract.right,
        "bid": bid,
        "ask": ask,
        "last": last,
        "close": close,
        "mid": mid,
        "delta": delta,
        "gamma": gamma,
        "theta": theta,
        "vega": vega,
        "implied_vol": implied_vol,
        "open_interest": open_interest,
        "volume": volume,
        "underlying_price": underlying_price,
        "dte": dte,
    }

"""Unit tests for LiveMarketData.

All hummingbot and strategy-framework imports are mocked so tests run in the
market-data pixi environment without requiring those packages to be installed.
"""

from __future__ import annotations

import sys
import time
from decimal import Decimal
from types import ModuleType
from typing import Any
from unittest.mock import MagicMock

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Build lightweight stub modules so that ``live_market_data`` can be imported
# without hummingbot or hb-strategy-framework being installed.
# ---------------------------------------------------------------------------


def _make_primitives_stubs() -> None:
    """Inject minimal strategy_framework primitive stubs into sys.modules."""
    if "strategy_framework" in sys.modules:
        return

    # Top-level package
    sf = ModuleType("strategy_framework")
    sys.modules["strategy_framework"] = sf

    primitives = ModuleType("strategy_framework.primitives")
    sys.modules["strategy_framework.primitives"] = primitives
    sf.primitives = primitives  # type: ignore[attr-defined]

    # --- CandleData ---
    class CandleData:  # noqa: D101
        def __init__(
            self,
            *,
            timestamp: int,
            open: Decimal,
            high: Decimal,
            low: Decimal,
            close: Decimal,
            volume: Decimal,
        ) -> None:
            self.timestamp = timestamp
            self.open = open
            self.high = high
            self.low = low
            self.close = close
            self.volume = volume

        def __eq__(self, other: object) -> bool:
            if not isinstance(other, CandleData):
                return NotImplemented
            return (
                self.timestamp == other.timestamp
                and self.open == other.open
                and self.high == other.high
                and self.low == other.low
                and self.close == other.close
                and self.volume == other.volume
            )

    candle_mod = ModuleType("strategy_framework.primitives.candle")
    candle_mod.CandleData = CandleData  # type: ignore[attr-defined]
    sys.modules["strategy_framework.primitives.candle"] = candle_mod

    # --- OrderBookEntry / OrderBookSnapshot ---
    class OrderBookEntry:  # noqa: D101
        def __init__(self, *, price: Decimal, quantity: Decimal) -> None:
            self.price = price
            self.quantity = quantity

        def __eq__(self, other: object) -> bool:
            if not isinstance(other, OrderBookEntry):
                return NotImplemented
            return self.price == other.price and self.quantity == other.quantity

    class OrderBookSnapshot:  # noqa: D101
        def __init__(
            self,
            *,
            timestamp: int,
            bids: list[OrderBookEntry],
            asks: list[OrderBookEntry],
        ) -> None:
            self.timestamp = timestamp
            self.bids = bids
            self.asks = asks

        @property
        def best_bid(self) -> Decimal:
            if not self.bids:
                raise ValueError("Empty bids")
            return self.bids[0].price

        @property
        def best_ask(self) -> Decimal:
            if not self.asks:
                raise ValueError("Empty asks")
            return self.asks[0].price

        @property
        def spread(self) -> Decimal:
            return self.best_ask - self.best_bid

    ob_mod = ModuleType("strategy_framework.primitives.order_book")
    ob_mod.OrderBookEntry = OrderBookEntry  # type: ignore[attr-defined]
    ob_mod.OrderBookSnapshot = OrderBookSnapshot  # type: ignore[attr-defined]
    sys.modules["strategy_framework.primitives.order_book"] = ob_mod


# Install stubs before importing the module under test.
_make_primitives_stubs()


def _make_hummingbot_stubs() -> None:
    """Inject a minimal hummingbot.core.data_type.common stub."""
    if "hummingbot" in sys.modules:
        return

    hb = ModuleType("hummingbot")
    sys.modules["hummingbot"] = hb

    core = ModuleType("hummingbot.core")
    sys.modules["hummingbot.core"] = core

    dt = ModuleType("hummingbot.core.data_type")
    sys.modules["hummingbot.core.data_type"] = dt

    class PriceType:  # noqa: D101
        MidPrice = "MidPrice"

    common = ModuleType("hummingbot.core.data_type.common")
    common.PriceType = PriceType  # type: ignore[attr-defined]
    sys.modules["hummingbot.core.data_type.common"] = common


_make_hummingbot_stubs()


# Now import the module under test.
from market_data.live_market_data import LiveMarketData  # noqa: E402

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_provider(
    mid_price: Decimal | None = None,
    order_book: Any = None,
    candles_df: pd.DataFrame | None = None,
) -> MagicMock:
    provider = MagicMock()
    if mid_price is not None:
        provider.get_price_by_type.return_value = mid_price
    if order_book is not None:
        provider.get_order_book.return_value = order_book
    if candles_df is not None:
        provider.get_candles_df.return_value = candles_df
    return provider


def _make_order_book(bids: list[tuple[float, float]], asks: list[tuple[float, float]]) -> MagicMock:
    """Build a mock order book whose ``.snapshot`` returns two DataFrames."""
    bids_df = pd.DataFrame(bids, columns=["price", "quantity"])
    asks_df = pd.DataFrame(asks, columns=["price", "quantity"])
    ob = MagicMock()
    ob.snapshot = (bids_df, asks_df)
    return ob


def _make_candles_df(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=["timestamp", "open", "high", "low", "close", "volume"])


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestLiveMarketDataGetMidPrice:
    def test_delegates_to_provider_with_connector_name(self) -> None:
        provider = _make_provider(mid_price=Decimal("50000.00"))
        lmd = LiveMarketData(provider, "binance")

        result = lmd.get_mid_price("BTC-USDT")

        provider.get_price_by_type.assert_called_once_with("binance", "BTC-USDT", "MidPrice")
        assert result == Decimal("50000.00")

    def test_returns_decimal(self) -> None:
        provider = _make_provider(mid_price=Decimal("1234.56"))
        lmd = LiveMarketData(provider, "kraken")

        result = lmd.get_mid_price("ETH-USD")

        assert isinstance(result, Decimal)

    def test_uses_bound_connector_name(self) -> None:
        provider = _make_provider(mid_price=Decimal("1.0"))
        lmd = LiveMarketData(provider, "coinbase")

        lmd.get_mid_price("BTC-USD")

        call_args = provider.get_price_by_type.call_args
        assert call_args[0][0] == "coinbase"


class TestLiveMarketDataGetOrderBookSnapshot:
    def test_returns_order_book_snapshot(self) -> None:
        ob = _make_order_book([(50000.0, 1.5), (49999.0, 2.0)], [(50001.0, 0.5)])
        provider = _make_provider(order_book=ob)
        lmd = LiveMarketData(provider, "binance")

        snapshot = lmd.get_order_book_snapshot("BTC-USDT")

        assert len(snapshot.bids) == 2
        assert len(snapshot.asks) == 1

    def test_bids_converted_to_order_book_entries(self) -> None:
        ob = _make_order_book([(100.0, 5.0), (99.0, 3.0)], [(101.0, 2.0)])
        provider = _make_provider(order_book=ob)
        lmd = LiveMarketData(provider, "binance")

        snapshot = lmd.get_order_book_snapshot("ETH-USDT")

        assert snapshot.bids[0].price == Decimal("100.0")
        assert snapshot.bids[0].quantity == Decimal("5.0")
        assert snapshot.bids[1].price == Decimal("99.0")
        assert snapshot.bids[1].quantity == Decimal("3.0")

    def test_asks_converted_to_order_book_entries(self) -> None:
        ob = _make_order_book([(100.0, 1.0)], [(101.0, 2.0), (102.0, 0.5)])
        provider = _make_provider(order_book=ob)
        lmd = LiveMarketData(provider, "binance")

        snapshot = lmd.get_order_book_snapshot("ETH-USDT")

        assert snapshot.asks[0].price == Decimal("101.0")
        assert snapshot.asks[0].quantity == Decimal("2.0")

    def test_timestamp_is_unix_milliseconds(self) -> None:
        ob = _make_order_book([(100.0, 1.0)], [(101.0, 1.0)])
        provider = _make_provider(order_book=ob)
        lmd = LiveMarketData(provider, "binance")

        before_ms = int(time.time() * 1_000)
        snapshot = lmd.get_order_book_snapshot("BTC-USDT")
        after_ms = int(time.time() * 1_000)

        assert before_ms <= snapshot.timestamp <= after_ms

    def test_delegates_to_provider_with_connector_name(self) -> None:
        ob = _make_order_book([(1.0, 1.0)], [(2.0, 1.0)])
        provider = _make_provider(order_book=ob)
        lmd = LiveMarketData(provider, "kraken")

        lmd.get_order_book_snapshot("ETH-USD")

        provider.get_order_book.assert_called_once_with("kraken", "ETH-USD")

    def test_empty_bids_and_asks(self) -> None:
        ob = _make_order_book([], [])
        provider = _make_provider(order_book=ob)
        lmd = LiveMarketData(provider, "binance")

        snapshot = lmd.get_order_book_snapshot("BTC-USDT")

        assert snapshot.bids == []
        assert snapshot.asks == []


class TestLiveMarketDataGetCandles:
    @pytest.mark.asyncio
    async def test_returns_list_of_candle_data(self) -> None:
        df = _make_candles_df(
            [
                {
                    "timestamp": 1700000000,
                    "open": 100.0,
                    "high": 110.0,
                    "low": 90.0,
                    "close": 105.0,
                    "volume": 500.0,
                },
            ]
        )
        provider = _make_provider(candles_df=df)
        lmd = LiveMarketData(provider, "binance")

        result = await lmd.get_candles("BTC-USDT", "1m", 10)

        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_candle_fields_converted_correctly(self) -> None:
        df = _make_candles_df(
            [
                {
                    "timestamp": 1700000000,
                    "open": 100.5,
                    "high": 110.25,
                    "low": 99.75,
                    "close": 105.0,
                    "volume": 123.456,
                },
            ]
        )
        provider = _make_provider(candles_df=df)
        lmd = LiveMarketData(provider, "binance")

        result = await lmd.get_candles("BTC-USDT", "1m", 1)
        candle = result[0]

        assert candle.timestamp == 1700000000 * 1_000
        assert candle.open == Decimal("100.5")
        assert candle.high == Decimal("110.25")
        assert candle.low == Decimal("99.75")
        assert candle.close == Decimal("105.0")
        assert candle.volume == Decimal("123.456")

    @pytest.mark.asyncio
    async def test_timestamp_converted_to_milliseconds(self) -> None:
        df = _make_candles_df(
            [
                {
                    "timestamp": 1700000060,
                    "open": 1.0,
                    "high": 2.0,
                    "low": 0.5,
                    "close": 1.5,
                    "volume": 10.0,
                },
            ]
        )
        provider = _make_provider(candles_df=df)
        lmd = LiveMarketData(provider, "binance")

        result = await lmd.get_candles("ETH-USDT", "1m", 5)

        assert result[0].timestamp == 1700000060 * 1_000

    @pytest.mark.asyncio
    async def test_delegates_limit_to_provider(self) -> None:
        df = _make_candles_df([])
        provider = _make_provider(candles_df=df)
        lmd = LiveMarketData(provider, "binance")

        await lmd.get_candles("BTC-USDT", "5m", 50)

        provider.get_candles_df.assert_called_once_with("binance", "BTC-USDT", "5m", 50)

    @pytest.mark.asyncio
    async def test_delegates_interval_to_provider(self) -> None:
        df = _make_candles_df([])
        provider = _make_provider(candles_df=df)
        lmd = LiveMarketData(provider, "binance")

        await lmd.get_candles("ETH-USDT", "1h", 100)

        call_args = provider.get_candles_df.call_args[0]
        assert call_args[2] == "1h"

    @pytest.mark.asyncio
    async def test_multiple_rows_all_converted(self) -> None:
        rows = [
            {
                "timestamp": 1700000000 + i * 60,
                "open": float(i),
                "high": float(i + 1),
                "low": float(i - 1),
                "close": float(i) + 0.5,
                "volume": float(i * 10),
            }
            for i in range(1, 6)
        ]
        df = _make_candles_df(rows)
        provider = _make_provider(candles_df=df)
        lmd = LiveMarketData(provider, "binance")

        result = await lmd.get_candles("BTC-USDT", "1m", 5)

        assert len(result) == 5
        for idx, candle in enumerate(result, start=1):
            assert candle.open == Decimal(str(float(idx)))

    @pytest.mark.asyncio
    async def test_empty_dataframe_returns_empty_list(self) -> None:
        df = _make_candles_df([])
        provider = _make_provider(candles_df=df)
        lmd = LiveMarketData(provider, "binance")

        result = await lmd.get_candles("BTC-USDT", "1m", 10)

        assert result == []

    @pytest.mark.asyncio
    async def test_uses_bound_connector_name(self) -> None:
        df = _make_candles_df([])
        provider = _make_provider(candles_df=df)
        lmd = LiveMarketData(provider, "kucoin")

        await lmd.get_candles("SOL-USDT", "15m", 20)

        call_args = provider.get_candles_df.call_args[0]
        assert call_args[0] == "kucoin"


class TestLiveMarketDataProtocolSatisfaction:
    """Structural checks that LiveMarketData has the required interface."""

    def test_has_get_mid_price(self) -> None:
        lmd = LiveMarketData(MagicMock(), "binance")
        assert callable(getattr(lmd, "get_mid_price", None))

    def test_has_get_order_book_snapshot(self) -> None:
        lmd = LiveMarketData(MagicMock(), "binance")
        assert callable(getattr(lmd, "get_order_book_snapshot", None))

    def test_has_get_candles(self) -> None:
        lmd = LiveMarketData(MagicMock(), "binance")
        assert callable(getattr(lmd, "get_candles", None))

    def test_isinstance_check_with_protocol(self) -> None:
        """isinstance check passes when strategy_framework protocol is available."""
        sf = pytest.importorskip(
            "strategy_framework.protocols.market_data",
            reason="strategy_framework not installed",
        )
        protocol_cls = sf.MarketDataProtocol
        lmd = LiveMarketData(MagicMock(), "binance")
        assert isinstance(lmd, protocol_cls)

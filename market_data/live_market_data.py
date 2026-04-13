"""LiveMarketData: wraps Hummingbot's MarketDataProvider to satisfy MarketDataProtocol."""

from __future__ import annotations

import time
from decimal import Decimal
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from strategy_framework.primitives.candle import CandleData
    from strategy_framework.primitives.order_book import OrderBookSnapshot


class LiveMarketData:
    """Adapts Hummingbot's MarketDataProvider to the MarketDataProtocol interface.

    Constructor binds a connector name so all protocol methods take only
    ``trading_pair`` — the extra ``connector_name`` argument required by
    ``MarketDataProvider`` is supplied internally.

    Args:
        market_data_provider: A Hummingbot ``MarketDataProvider`` instance.
            Typed as ``Any`` to avoid importing from the hummingbot package.
        connector_name: Exchange connector identifier (e.g. ``"binance"``).
    """

    def __init__(self, market_data_provider: Any, connector_name: str) -> None:
        self._provider = market_data_provider
        self._connector_name = connector_name

    # ------------------------------------------------------------------
    # MarketDataProtocol implementation
    # ------------------------------------------------------------------

    def get_mid_price(self, trading_pair: str) -> Decimal:
        """Return the mid-price for *trading_pair* from the bound connector.

        Delegates to ``MarketDataProvider.get_price_by_type`` with
        ``PriceType.MidPrice``.
        """
        from hummingbot.core.data_type.common import PriceType  # type: ignore[import]

        return self._provider.get_price_by_type(  # type: ignore[no-any-return]
            self._connector_name, trading_pair, PriceType.MidPrice
        )

    def get_order_book_snapshot(self, trading_pair: str) -> OrderBookSnapshot:
        """Return an ``OrderBookSnapshot`` for *trading_pair*.

        Retrieves the live order book from the bound connector and converts
        the top-of-book entries into ``OrderBookEntry`` / ``OrderBookSnapshot``
        primitives defined by ``hb-strategy-framework``.
        """
        from strategy_framework.primitives.order_book import OrderBookEntry, OrderBookSnapshot

        order_book = self._provider.get_order_book(self._connector_name, trading_pair)
        bids_df, asks_df = order_book.snapshot

        bids: list[OrderBookEntry] = [
            OrderBookEntry(price=Decimal(str(row[0])), quantity=Decimal(str(row[1])))
            for row in bids_df.values.tolist()
        ]
        asks: list[OrderBookEntry] = [
            OrderBookEntry(price=Decimal(str(row[0])), quantity=Decimal(str(row[1])))
            for row in asks_df.values.tolist()
        ]

        timestamp_ms = int(time.time() * 1_000)
        return OrderBookSnapshot(timestamp=timestamp_ms, bids=bids, asks=asks)

    async def get_candles(self, trading_pair: str, interval: str, limit: int) -> list[CandleData]:
        """Return up to *limit* ``CandleData`` records for *trading_pair*.

        Calls the synchronous ``MarketDataProvider.get_candles_df`` (the feed
        is pre-warmed by the provider; no async fetch is needed at call time)
        and converts each DataFrame row to a ``CandleData`` primitive.

        The DataFrame returned by ``get_candles_df`` is expected to have
        columns: ``timestamp``, ``open``, ``high``, ``low``, ``close``,
        ``volume``.  ``timestamp`` is in seconds (float/int); ``CandleData``
        stores Unix milliseconds.
        """
        from strategy_framework.primitives.candle import CandleData

        df = self._provider.get_candles_df(self._connector_name, trading_pair, interval, limit)

        candles: list[CandleData] = []
        for row in df.itertuples(index=False):
            candles.append(
                CandleData(
                    timestamp=int(float(row.timestamp) * 1_000),
                    open=Decimal(str(row.open)),
                    high=Decimal(str(row.high)),
                    low=Decimal(str(row.low)),
                    close=Decimal(str(row.close)),
                    volume=Decimal(str(row.volume)),
                )
            )
        return candles

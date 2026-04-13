# hb-market-data

Live market data adapter for Hummingbot — implements `MarketDataProtocol` from [hb-strategy-framework](https://github.com/MementoRC/hb-strategy-framework).

## Overview

This package provides `LiveMarketData`, a concrete implementation that wraps Hummingbot's `MarketDataProvider` to satisfy the strategy framework's market data protocol.

## Installation

```bash
pip install hb-market-data
```

## Development

```bash
pixi install
pixi run check
```

## License

Apache-2.0

"""Shared pytest fixtures for hb-market-data tests."""

import pytest


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"

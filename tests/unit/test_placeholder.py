"""Placeholder test to verify test infrastructure works."""

import re


def test_import():
    """Verify the package can be imported."""
    import market_data

    assert re.fullmatch(r"\d+\.\d+\.\d+", market_data.__version__)

from __future__ import annotations

import pytest

from scripts.listing_keys import build_listing_key, row_listing_key, split_listing_key


def test_build_and_split_listing_key_round_trip():
    listing_key = build_listing_key("AAPL", "NASDAQ")

    assert listing_key == "NASDAQ::AAPL"
    assert split_listing_key(listing_key) == ("NASDAQ", "AAPL")


def test_row_listing_key_uses_ticker_and_exchange():
    assert row_listing_key({"ticker": "1306", "exchange": "TSE"}) == "TSE::1306"


def test_build_listing_key_requires_both_components():
    with pytest.raises(ValueError):
        build_listing_key("", "NASDAQ")

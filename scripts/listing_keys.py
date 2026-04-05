from __future__ import annotations

from collections.abc import Mapping


LISTING_KEY_SEPARATOR = "::"


def build_listing_key(ticker: str, exchange: str) -> str:
    normalized_ticker = ticker.strip()
    normalized_exchange = exchange.strip()
    if not normalized_ticker or not normalized_exchange:
        raise ValueError("ticker and exchange are required to build a listing key")
    return f"{normalized_exchange}{LISTING_KEY_SEPARATOR}{normalized_ticker}"


def split_listing_key(listing_key: str) -> tuple[str, str]:
    exchange, separator, ticker = listing_key.partition(LISTING_KEY_SEPARATOR)
    if not separator or not exchange or not ticker:
        raise ValueError(f"invalid listing key: {listing_key!r}")
    return exchange, ticker


def row_listing_key(row: Mapping[str, str]) -> str:
    return build_listing_key(row.get("ticker", ""), row.get("exchange", ""))

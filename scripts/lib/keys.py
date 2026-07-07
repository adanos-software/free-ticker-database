from __future__ import annotations


def listing_key(exchange: str, ticker: str) -> str:
    return f"{exchange.strip()}::{ticker.strip()}"


def split_listing_key(value: str) -> tuple[str, str]:
    if "::" not in value:
        return "", value.strip()
    exchange, ticker = value.split("::", 1)
    return exchange.strip(), ticker.strip()


def row_listing_key(row: dict[str, str]) -> str:
    return row.get("listing_key") or listing_key(row.get("exchange", ""), row.get("ticker", ""))


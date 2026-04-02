from __future__ import annotations

import requests

from scripts.fetch_exchange_masterfiles import (
    MasterfileSource,
    fetch_all_sources,
    parse_nasdaq_listed,
    parse_other_listed,
    parse_sec_company_tickers_exchange,
)


SOURCE = MasterfileSource(
    key="test",
    provider="test",
    description="test",
    source_url="https://example.com",
    format="test",
)


def test_parse_nasdaq_listed_maps_etf_and_status():
    text = "\n".join(
        [
            "Symbol|Security Name|Market Category|Test Issue|Financial Status|Round Lot Size|ETF|NextShares",
            "AAPL|Apple Inc.|Q|N|N|100|N|N",
            "QQQ|Invesco QQQ Trust, Series 1|Q|N|N|100|Y|N",
            "TEST|Test Issue Corp|Q|Y|N|100|N|N",
            "File Creation Time: 04022026",
        ]
    )

    rows = parse_nasdaq_listed(text, SOURCE)

    assert rows[0]["ticker"] == "AAPL"
    assert rows[0]["exchange"] == "NASDAQ"
    assert rows[0]["asset_type"] == "Stock"
    assert rows[1]["asset_type"] == "ETF"
    assert rows[2]["listing_status"] == "test"


def test_parse_other_listed_maps_exchange_codes():
    text = "\n".join(
        [
            "ACT Symbol|Security Name|Exchange|CQS Symbol|ETF|Round Lot Size|Test Issue|NASDAQ Symbol",
            "IBM|International Business Machines|N|IBM|N|100|N|IBM",
            "SPY|SPDR S&P 500 ETF TRUST|P|SPY|Y|100|N|SPY",
            "File Creation Time: 04022026",
        ]
    )

    rows = parse_other_listed(text, SOURCE)

    assert rows == [
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "IBM",
            "name": "International Business Machines",
            "exchange": "NYSE",
            "asset_type": "Stock",
            "listing_status": "active",
            "official": "true",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "SPY",
            "name": "SPDR S&P 500 ETF TRUST",
            "exchange": "NYSE ARCA",
            "asset_type": "ETF",
            "listing_status": "active",
            "official": "true",
        },
    ]


def test_parse_sec_company_tickers_exchange_normalizes_exchange_names():
    payload = {
        "fields": ["cik", "name", "ticker", "exchange"],
        "data": [
            [320193, "Apple Inc.", "AAPL", "Nasdaq"],
            [732717, "AT&T Inc.", "T", "NYSE"],
            [884394, "SPDR S&P 500 ETF TRUST", "SPY", "NYSE"],
            [111, "Ignored", "BAD", None],
        ],
    }

    rows = parse_sec_company_tickers_exchange(payload, SOURCE)

    assert rows[0]["exchange"] == "NASDAQ"
    assert rows[0]["asset_type"] == "Stock"
    assert rows[2]["asset_type"] == "ETF"
    assert [row["ticker"] for row in rows] == ["AAPL", "T", "SPY"]


def test_fetch_all_sources_collects_source_errors(monkeypatch):
    def fake_fetch_source_rows(source, session=None):
        if source.key == "nasdaq_listed":
            return [{"source_key": source.key, "provider": source.provider, "source_url": source.source_url, "ticker": "AAPL", "name": "Apple Inc.", "exchange": "NASDAQ", "asset_type": "Stock", "listing_status": "active", "official": "true"}]
        raise requests.RequestException("boom")

    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.fetch_source_rows", fake_fetch_source_rows)

    rows, summary = fetch_all_sources(include_manual=False)

    assert rows == [
        {
            "source_key": "nasdaq_listed",
            "provider": "Nasdaq Trader",
            "source_url": "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt",
            "ticker": "AAPL",
            "name": "Apple Inc.",
            "exchange": "NASDAQ",
            "asset_type": "Stock",
            "listing_status": "active",
            "official": "true",
        }
    ]
    assert summary["errors"]

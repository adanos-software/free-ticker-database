from __future__ import annotations

import json

from scripts.enrich_global_identifiers import (
    apply_figi,
    apply_sec_cik,
    build_sec_cik_index,
    load_sec_payload,
    select_lei_candidates,
)


def test_build_sec_cik_index_normalizes_rows():
    payload = {
        "fields": ["cik", "name", "ticker", "exchange"],
        "data": [
            [320193, "Apple Inc.", "AAPL", "Nasdaq"],
            [732717, "AT&T Inc.", "T", "NYSE"],
            [111, "Ignored", "MISS", None],
        ],
    }

    index = build_sec_cik_index(payload)

    assert index == {
        ("AAPL", "NASDAQ"): "0000320193",
        ("T", "NYSE"): "0000732717",
    }


def test_apply_sec_cik_uses_exact_or_ticker_fallback():
    rows = [
        {"ticker": "AAPL", "exchange": "NASDAQ", "cik": "", "cik_source": ""},
        {"ticker": "T", "exchange": "XETRA", "cik": "", "cik_source": ""},
    ]
    updated = apply_sec_cik(
        rows,
        {
            ("AAPL", "NASDAQ"): "0000320193",
            ("T", "NYSE"): "0000732717",
        },
    )

    assert updated == 2
    assert rows[0]["cik"] == "0000320193"
    assert rows[1]["cik"] == "0000732717"
    assert rows[1]["cik_source"] == "SEC company_tickers_exchange.json"


def test_apply_figi_maps_by_isin():
    rows = [
        {"ticker": "AAPL", "exchange": "NASDAQ", "isin": "US0378331005", "figi": "", "figi_source": ""},
        {"ticker": "MSFT", "exchange": "NASDAQ", "isin": "", "figi": "", "figi_source": ""},
    ]

    updated = apply_figi(rows, {"US0378331005": "BBG000B9XRY4"})

    assert updated == 1
    assert rows[0]["figi"] == "BBG000B9XRY4"
    assert rows[1]["figi"] == ""


def test_load_sec_payload_prefers_local_cache(tmp_path, monkeypatch):
    cache = tmp_path / "sec_company_tickers_exchange.json"
    cache.write_text(json.dumps({"fields": ["ticker"], "data": [["AAPL"]]}), encoding="utf-8")
    monkeypatch.setattr("scripts.enrich_global_identifiers.SEC_COMPANY_TICKERS_CACHE", cache)

    payload, mode = load_sec_payload()

    assert mode == "cache"
    assert payload == {"fields": ["ticker"], "data": [["AAPL"]]}


def test_select_lei_candidates_prioritizes_better_matches():
    rows = [
        {"ticker": "0ABC", "exchange": "LSE", "isin": "GB0000000001", "asset_type": "Stock"},
        {"ticker": "AAPL", "exchange": "NASDAQ", "isin": "US0378331005", "asset_type": "Stock"},
        {"ticker": "QQQ", "exchange": "NASDAQ", "isin": "US0000000002", "asset_type": "ETF"},
        {"ticker": "SHOP", "exchange": "TSX", "isin": "", "asset_type": "Stock"},
    ]

    ordered = select_lei_candidates(rows)

    assert [row["ticker"] for row in ordered] == ["AAPL", "0ABC", "QQQ", "SHOP"]

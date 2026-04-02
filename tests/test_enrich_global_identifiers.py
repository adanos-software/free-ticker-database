from __future__ import annotations

import json
import requests

from scripts.enrich_global_identifiers import (
    apply_figi,
    apply_lei,
    apply_sec_cik,
    build_figi_matches,
    build_base_identifier_rows,
    build_sec_cik_index,
    filter_lei_candidates,
    fetch_openfigi_by_isin,
    load_sec_payload,
    normalize_company_name,
    select_figi_rows,
    select_openfigi_candidate,
    select_lei_candidates,
    with_retries,
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


def test_apply_figi_maps_by_listing():
    rows = [
        {"ticker": "AAPL", "exchange": "NASDAQ", "isin": "US0378331005", "figi": "", "figi_source": ""},
        {"ticker": "MSFT", "exchange": "NASDAQ", "isin": "", "figi": "", "figi_source": ""},
    ]

    updated = apply_figi(rows, {("AAPL", "NASDAQ"): "BBG000B9XRY4"})

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


def test_select_figi_rows_filters_existing_and_exchange():
    rows = [
        {"ticker": "AAPL", "exchange": "NASDAQ", "isin": "US0378331005", "figi": ""},
        {"ticker": "IBM", "exchange": "NYSE", "isin": "US4592001014", "figi": "BBG000BLNNH6"},
        {"ticker": "BHP", "exchange": "ASX", "isin": "AU000000BHP4", "figi": ""},
    ]

    selected = select_figi_rows(rows, exchanges={"ASX"})

    assert selected == [{"ticker": "BHP", "exchange": "ASX", "isin": "AU000000BHP4", "figi": ""}]


def test_select_openfigi_candidate_prefers_exchange_hint_and_ticker():
    row = {"ticker": "SHOP", "exchange": "TSX"}
    candidates = [
        {"ticker": "SHOP", "exchCode": "US", "figi": "USFIGI"},
        {"ticker": "SHOP", "exchCode": "CN", "figi": "CNFIGI"},
    ]

    assert select_openfigi_candidate(row, candidates) == "CNFIGI"


def test_build_figi_matches_uses_row_level_selection():
    rows = [
        {"ticker": "SHOP", "exchange": "TSX", "isin": "CA82509L1076"},
        {"ticker": "SHOP", "exchange": "NYSE", "isin": "CA82509L1076"},
    ]
    candidates_by_isin = {
        "CA82509L1076": [
            {"ticker": "SHOP", "exchCode": "CN", "figi": "CNFIGI"},
            {"ticker": "SHOP", "exchCode": "US", "figi": "USFIGI"},
        ]
    }

    matches = build_figi_matches(rows, candidates_by_isin)

    assert matches == {
        ("SHOP", "TSX"): "CNFIGI",
        ("SHOP", "NYSE"): "USFIGI",
    }


def test_filter_lei_candidates_requires_stock_isin_and_missing_lei():
    rows = [
        {"ticker": "AAPL", "exchange": "NASDAQ", "isin": "US0378331005", "asset_type": "Stock", "lei": ""},
        {"ticker": "QQQ", "exchange": "NASDAQ", "isin": "US0000000002", "asset_type": "ETF", "lei": ""},
        {"ticker": "SHOP", "exchange": "TSX", "isin": "", "asset_type": "Stock", "lei": ""},
        {"ticker": "BHP", "exchange": "ASX", "isin": "AU000000BHP4", "asset_type": "Stock", "lei": "123"},
    ]

    candidates = filter_lei_candidates(rows, exchanges={"NASDAQ", "ASX"})

    assert [row["ticker"] for row in candidates] == ["AAPL"]


def test_normalize_company_name_strips_punctuation():
    assert normalize_company_name("AT&T Inc.") == "attinc"


def test_build_base_identifier_rows_preserves_existing_extended_values(tmp_path, monkeypatch):
    tickers = tmp_path / "tickers.csv"
    tickers.write_text(
        "ticker,name,exchange,country,asset_type\nAAPL,Apple Inc.,NASDAQ,United States,Stock\n",
        encoding="utf-8",
    )
    identifiers = tmp_path / "identifiers.csv"
    identifiers.write_text("ticker,isin,wkn\nAAPL,US0378331005,\n", encoding="utf-8")
    identifiers_extended = tmp_path / "identifiers_extended.csv"
    identifiers_extended.write_text(
        "ticker,exchange,isin,wkn,figi,cik,lei,figi_source,cik_source,lei_source\n"
        "AAPL,NASDAQ,US0378331005,,BBG000B9XRY4,0000320193,HWUPKR0MPOU8FGXBT394,OpenFIGI,SEC,GLEIF\n",
        encoding="utf-8",
    )
    monkeypatch.setattr("scripts.enrich_global_identifiers.TICKERS_CSV", tickers)
    monkeypatch.setattr("scripts.enrich_global_identifiers.IDENTIFIERS_CSV", identifiers)
    monkeypatch.setattr("scripts.enrich_global_identifiers.IDENTIFIERS_EXTENDED_CSV", identifiers_extended)

    rows = build_base_identifier_rows()

    assert rows == [
        {
            "ticker": "AAPL",
            "exchange": "NASDAQ",
            "isin": "US0378331005",
            "wkn": "",
            "figi": "BBG000B9XRY4",
            "cik": "0000320193",
            "lei": "HWUPKR0MPOU8FGXBT394",
            "figi_source": "OpenFIGI",
            "cik_source": "SEC",
            "lei_source": "GLEIF",
            "name": "Apple Inc.",
            "country": "United States",
            "asset_type": "Stock",
        }
    ]


def test_with_retries_retries_request_exceptions():
    attempts = {"count": 0}

    def flaky():
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise requests.RequestException("temporary")
        return "ok"

    assert with_retries(flaky, attempts=3, delay_seconds=0.0) == "ok"


def test_fetch_openfigi_by_isin_keeps_partial_progress(monkeypatch):
    calls = {"count": 0}

    def fake_post_json(url, payload, session=None, headers=None):
        calls["count"] += 1
        if calls["count"] == 2:
            raise requests.RequestException("timeout")
        return [{"data": [{"figi": f"FIGI-{job['idValue']}"}]} for job in payload]

    monkeypatch.setattr("scripts.enrich_global_identifiers.post_json", fake_post_json)

    result, errors = fetch_openfigi_by_isin(
        ["AA", "BB", "CC", "DD", "EE", "FF", "GG", "HH", "II", "JJ", "KK"],
        delay_seconds=0.0,
        retry_attempts=1,
        retry_delay_seconds=0.0,
    )

    assert result["AA"][0]["figi"] == "FIGI-AA"
    assert "KK" not in result
    assert len(errors) == 1


def test_apply_lei_continues_after_lookup_error(monkeypatch):
    rows = [
        {"ticker": "BAD", "exchange": "AMS", "isin": "NL0000000001", "asset_type": "Stock", "lei": "", "name": "Bad Co", "lei_source": ""},
        {"ticker": "GOOD", "exchange": "AMS", "isin": "NL0000000002", "asset_type": "Stock", "lei": "", "name": "Good Co", "lei_source": ""},
    ]

    def fake_fetch_gleif_lei(name, session=None, retry_attempts=3, retry_delay_seconds=2.0):
        if name == "Bad Co":
            raise requests.RequestException("dns")
        return "LEI-123"

    monkeypatch.setattr("scripts.enrich_global_identifiers.fetch_gleif_lei", fake_fetch_gleif_lei)

    updated, errors = apply_lei(rows, delay_seconds=0.0, exchanges={"AMS"})

    assert updated == 1
    assert rows[1]["lei"] == "LEI-123"
    assert len(errors) == 1

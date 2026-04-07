from __future__ import annotations

import json
import requests

from scripts.enrich_global_identifiers import (
    apply_figi,
    apply_lei,
    apply_sec_cik,
    build_summary,
    build_figi_matches,
    build_base_identifier_rows,
    build_sec_cik_index,
    filter_lei_candidates,
    fetch_openfigi_by_isin,
    fetch_gleif_lei_by_isin,
    load_sec_payload,
    normalize_company_name,
    retry_delay_for,
    sec_request_headers,
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


def test_apply_sec_cik_requires_exact_listing_match():
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

    assert updated == 1
    assert rows[0]["cik"] == "0000320193"
    assert rows[1]["cik"] == ""
    assert rows[1]["cik_source"] == ""


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


def test_sec_request_headers_include_contactable_user_agent(monkeypatch):
    monkeypatch.setattr("scripts.enrich_global_identifiers.SEC_CONTACT_EMAIL", "sec@example.com")

    headers = sec_request_headers()

    assert headers["User-Agent"] == "free-ticker-database/2.0 (sec@example.com)"
    assert headers["Referer"] == "https://www.sec.gov/"


def test_select_lei_candidates_prioritizes_better_matches():
    rows = [
        {"ticker": "0ABC", "exchange": "LSE", "isin": "GB0000000001", "asset_type": "Stock"},
        {"ticker": "AAPL", "exchange": "NASDAQ", "isin": "US0378331005", "asset_type": "Stock"},
        {"ticker": "QQQ", "exchange": "NASDAQ", "isin": "US0000000002", "asset_type": "ETF"},
        {"ticker": "SHOP", "exchange": "TSX", "isin": "", "asset_type": "Stock"},
    ]

    ordered = select_lei_candidates(rows)

    assert [row["ticker"] for row in ordered] == ["AAPL", "0ABC", "QQQ", "SHOP"]


def test_select_lei_candidates_prioritizes_identifier_backed_rows():
    rows = [
        {"ticker": "ZZZ", "exchange": "NASDAQ", "isin": "US0000000001", "asset_type": "Stock", "cik": "", "figi": ""},
        {"ticker": "AAA", "exchange": "NASDAQ", "isin": "US0000000002", "asset_type": "Stock", "cik": "0000320193", "figi": ""},
        {"ticker": "BBB", "exchange": "NASDAQ", "isin": "US0000000003", "asset_type": "Stock", "cik": "", "figi": "BBG000B9XRY4"},
    ]

    ordered = select_lei_candidates(rows)

    assert [row["ticker"] for row in ordered] == ["AAA", "BBB", "ZZZ"]


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


def test_fetch_gleif_lei_by_isin_uses_exact_isin_filter(monkeypatch):
    def fake_fetch_json(url, session=None, headers=None):
        assert "filter[isin]=US0378331005" in url
        return {"data": [{"id": "HWUPKR0MPOU8FGXBT394"}]}

    monkeypatch.setattr("scripts.enrich_global_identifiers.fetch_json", fake_fetch_json)

    lei = fetch_gleif_lei_by_isin("US0378331005")

    assert lei == "HWUPKR0MPOU8FGXBT394"


def test_normalize_company_name_strips_punctuation():
    assert normalize_company_name("AT&T Inc.") == "attinc"


def test_build_base_identifier_rows_preserves_existing_extended_values(tmp_path, monkeypatch):
    listings = tmp_path / "listings.csv"
    listings.write_text(
        "listing_key,ticker,exchange,name,asset_type,sector,country,country_code,isin,aliases\n"
        "NASDAQ::AAPL,AAPL,NASDAQ,Apple Inc.,Stock,,United States,US,US0378331005,\n",
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
    monkeypatch.setattr("scripts.enrich_global_identifiers.LISTINGS_CSV", listings)
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
            "country_code": "US",
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


def test_retry_delay_for_uses_retry_after_header():
    response = requests.Response()
    response.status_code = 429
    response.headers["Retry-After"] = "7"
    error = requests.HTTPError(response=response)

    assert retry_delay_for(error, attempt=2, base_delay_seconds=1.0) == 7.0


def test_retry_delay_for_uses_rate_limit_floor_without_header():
    response = requests.Response()
    response.status_code = 429
    error = requests.HTTPError(response=response)

    assert retry_delay_for(error, attempt=2, base_delay_seconds=1.0) == 15.0


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
        batch_size=10,
        retry_attempts=1,
        retry_delay_seconds=0.0,
    )

    assert result["AA"][0]["figi"] == "FIGI-AA"
    assert "KK" not in result
    assert len(errors) == 1


def test_fetch_openfigi_by_isin_respects_batch_size(monkeypatch):
    sizes = []

    def fake_post_json(url, payload, session=None, headers=None):
        sizes.append(len(payload))
        return [{"data": []} for _ in payload]

    monkeypatch.setattr("scripts.enrich_global_identifiers.post_json", fake_post_json)

    fetch_openfigi_by_isin(
        ["AA", "BB", "CC", "DD", "EE"],
        delay_seconds=0.0,
        batch_size=2,
        retry_attempts=1,
        retry_delay_seconds=0.0,
    )

    assert sizes == [2, 2, 1]


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


def test_apply_lei_prefers_isin_lookup_before_name(monkeypatch):
    rows = [
        {"ticker": "AAPL", "exchange": "NASDAQ", "isin": "US0378331005", "asset_type": "Stock", "lei": "", "name": "Apple Inc.", "lei_source": ""},
    ]
    calls = {"isin": 0, "name": 0}

    def fake_fetch_gleif_lei_by_isin(isin, session=None, retry_attempts=3, retry_delay_seconds=2.0):
        calls["isin"] += 1
        return "HWUPKR0MPOU8FGXBT394"

    def fake_fetch_gleif_lei(name, session=None, retry_attempts=3, retry_delay_seconds=2.0):
        calls["name"] += 1
        return ""

    monkeypatch.setattr("scripts.enrich_global_identifiers.fetch_gleif_lei_by_isin", fake_fetch_gleif_lei_by_isin)
    monkeypatch.setattr("scripts.enrich_global_identifiers.fetch_gleif_lei", fake_fetch_gleif_lei)

    updated, errors = apply_lei(rows, delay_seconds=0.0, exchanges={"NASDAQ"})

    assert updated == 1
    assert errors == []
    assert rows[0]["lei"] == "HWUPKR0MPOU8FGXBT394"
    assert calls == {"isin": 1, "name": 0}


def test_build_summary_includes_generated_at_and_source_counts():
    rows = [
        {
            "cik": "0000320193",
            "figi": "BBG000B9XRY4",
            "lei": "",
            "cik_source": "SEC",
            "figi_source": "OpenFIGI",
            "lei_source": "",
        },
        {
            "cik": "",
            "figi": "",
            "lei": "HWUPKR0MPOU8FGXBT394",
            "cik_source": "",
            "figi_source": "",
            "lei_source": "GLEIF",
        },
    ]

    summary = build_summary(rows)

    assert summary["generated_at"].endswith("Z")
    assert summary["rows"] == 2
    assert summary["cik_coverage"] == 1
    assert summary["figi_coverage"] == 1
    assert summary["lei_coverage"] == 1
    assert summary["listings_with_any_identifier"] == 2
    assert summary["cik_source_counts"] == {"SEC": 1}
    assert summary["figi_source_counts"] == {"OpenFIGI": 1}
    assert summary["lei_source_counts"] == {"GLEIF": 1}

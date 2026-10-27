from __future__ import annotations

from unittest.mock import patch

import pytest
import requests

from scripts.lib.us_exchange_directory import (
    SEC_COMPANY_TICKERS_URL,
    active_directory_keys,
    parse_nasdaq_listed_rows,
    parse_other_listed_rows,
    refresh_sec_exchange_cache,
    sec_cik_mismatch,
    sec_request_headers,
    shared_sec_cik,
)


def test_parse_nasdaq_listed_rows_extracts_active_symbols() -> None:
    text = (
        "Symbol|Security Name|Market Category|Test Issue|Financial Status|Round Lot Size|ETF|NextShares\n"
        "NEW|Renamed Inc. - Common Stock|Q|N|N|100|N|N\n"
        "File Creation Time: 0101010101\n"
    )
    rows = parse_nasdaq_listed_rows(text)
    assert len(rows) == 1
    assert rows[0]["ticker"] == "NEW"
    assert rows[0]["exchange"] == "NASDAQ"
    assert active_directory_keys(rows) == {("NASDAQ", "NEW")}


def test_parse_other_listed_rows_maps_exchange_codes() -> None:
    text = (
        "ACT Symbol|Security Name|Exchange|CQS Symbol|ETF|Round Lot Size|Test Issue|NASDAQ Symbol\n"
        "DCM|DeFi Capital Markets, Inc. Common Stock|A|DCM|N|100|N|DCM\n"
    )
    rows = parse_other_listed_rows(text)
    assert rows[0]["exchange"] == "NYSE MKT"
    assert rows[0]["ticker"] == "DCM"


def test_sec_request_headers_include_contact_email(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SEC_CONTACT_EMAIL", "contact@adanos.org")
    headers = sec_request_headers()
    assert headers["User-Agent"] == "free-ticker-database/2.0 (contact@adanos.org)"
    assert headers["Referer"] == "https://www.sec.gov/"


def test_refresh_sec_exchange_cache_sends_sec_user_agent(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    cache_path = tmp_path / "sec_company_tickers_exchange.json"
    payload = {
        "fields": ["cik", "name", "ticker", "exchange"],
        "data": [[1991261, "Rantizo", "SKK", "Nasdaq"], [1991261, "Rantizo", "RNTZ", "Nasdaq"]],
    }

    class FakeResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return payload

    with patch("scripts.lib.us_exchange_directory.requests.get", return_value=FakeResponse()) as mock_get:
        result = refresh_sec_exchange_cache(cache_path=cache_path)

    assert result == payload
    assert cache_path.exists()
    mock_get.assert_called_once_with(
        SEC_COMPANY_TICKERS_URL,
        headers=sec_request_headers(),
        timeout=60,
    )


def test_refresh_sec_exchange_cache_returns_none_after_retries(tmp_path) -> None:
    with patch(
        "scripts.lib.us_exchange_directory.requests.get",
        side_effect=requests.RequestException("blocked"),
    ):
        assert refresh_sec_exchange_cache(cache_path=tmp_path / "sec.json") is None


def test_shared_sec_cik_and_mismatch() -> None:
    payload = {
        "fields": ["cik", "name", "ticker", "exchange"],
        "data": [
            [1991261, "Rantizo", "SKK", "Nasdaq"],
            [1991261, "Rantizo", "RNTZ", "Nasdaq"],
            [2028516, "Archimedes", "ATII", "Nasdaq"],
            [2101833, "Forge Nano", "NANO", "Nasdaq"],
        ],
    }
    assert shared_sec_cik("SKK", "RNTZ", payload) == "0001991261"
    assert not sec_cik_mismatch("SKK", "RNTZ", payload)
    assert sec_cik_mismatch("ATII", "NANO", payload)

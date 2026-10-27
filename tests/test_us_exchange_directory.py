from __future__ import annotations

from scripts.lib.us_exchange_directory import (
    active_directory_keys,
    build_sec_exchange_payload_from_committed_sources,
    parse_nasdaq_listed_rows,
    parse_other_listed_rows,
    sec_cik_mismatch,
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


def test_build_sec_payload_from_committed_sources_covers_same_cik_rename() -> None:
    identifiers = [
        {
            "ticker": "RNTZ",
            "exchange": "NASDAQ",
            "cik": "0001991261",
            "cik_source": "SEC company_tickers_exchange.json",
            "name": "Rantizo",
        }
    ]
    transitions = [
        {
            "old_listing_key": "NASDAQ::SKK",
            "new_listing_key": "NASDAQ::RNTZ",
            "identity_type": "same_cik",
            "identity_value": "0001991261",
        }
    ]
    payload = build_sec_exchange_payload_from_committed_sources(identifiers, transitions)
    assert payload is not None
    assert shared_sec_cik("SKK", "RNTZ", payload) == "0001991261"


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

from __future__ import annotations

import csv

from scripts.backfill_sec_sic_sectors import (
    SecTicker,
    build_metadata_updates,
    evaluate_sec_sic_row,
    find_sec_candidates,
    index_sec_tickers,
    load_missing_sector_rows,
    map_sec_sic_to_sector,
    normalized_ticker_key,
)


def test_map_sec_sic_to_sector_handles_exact_and_range_mappings():
    assert map_sec_sic_to_sector("3674") == "Information Technology"
    assert map_sec_sic_to_sector("2834") == "Health Care"
    assert map_sec_sic_to_sector("6770") == "Financials"
    assert map_sec_sic_to_sector("6513") == "Real Estate"
    assert map_sec_sic_to_sector("9999") == ""


def test_load_missing_sector_rows_filters_us_stock_rows(tmp_path):
    path = tmp_path / "tickers.csv"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["ticker", "exchange", "asset_type", "name", "sector"])
        writer.writeheader()
        writer.writerows(
            [
                {"ticker": "AAPL", "exchange": "NASDAQ", "asset_type": "Stock", "name": "Apple Inc", "sector": ""},
                {"ticker": "QQQ", "exchange": "NASDAQ", "asset_type": "ETF", "name": "Invesco QQQ", "sector": ""},
                {"ticker": "IBM", "exchange": "NYSE", "asset_type": "Stock", "name": "IBM", "sector": "Information Technology"},
                {"ticker": "SHOP", "exchange": "TSX", "asset_type": "Stock", "name": "Shopify", "sector": ""},
            ]
        )

    rows = load_missing_sector_rows(exchanges={"NASDAQ", "NYSE"}, tickers_csv=path)

    assert [row["ticker"] for row in rows] == ["AAPL"]


def test_find_sec_candidates_uses_ticker_and_exchange_mapping():
    indexed = index_sec_tickers(
        [
            SecTicker(cik=1, name="Apple Inc.", ticker="AAPL", exchange="NASDAQ"),
            SecTicker(cik=2, name="Apple Hospitality REIT", ticker="APLE", exchange="NYSE"),
        ]
    )

    assert find_sec_candidates({"ticker": "AAPL", "exchange": "NASDAQ"}, indexed) == [
        SecTicker(cik=1, name="Apple Inc.", ticker="AAPL", exchange="NASDAQ")
    ]
    assert find_sec_candidates({"ticker": "AAPL", "exchange": "NYSE"}, indexed) == []


def test_evaluate_sec_sic_row_accepts_exact_name_and_sic_match():
    result = evaluate_sec_sic_row(
        {"ticker": "NVDA", "exchange": "NASDAQ", "asset_type": "Stock", "name": "NVIDIA CORP", "sector": ""},
        [SecTicker(cik=1045810, name="NVIDIA CORP", ticker="NVDA", exchange="NASDAQ")],
        {"sic": "3674", "sicDescription": "Semiconductors & Related Devices"},
    )

    assert result["decision"] == "accept"
    assert result["sector_update"] == "Information Technology"


def test_evaluate_sec_sic_row_rejects_bad_name_and_missing_sic():
    row = {"ticker": "NVDA", "exchange": "NASDAQ", "asset_type": "Stock", "name": "NVIDIA CORP", "sector": ""}
    candidate = SecTicker(cik=1, name="Different Corp", ticker="NVDA", exchange="NASDAQ")

    assert evaluate_sec_sic_row(row, [], None)["decision"] == "no_sec_match"
    assert evaluate_sec_sic_row(row, [candidate], {"sic": "3674", "sicDescription": "Semiconductors"})["decision"] == "name_mismatch"
    assert evaluate_sec_sic_row(row, [SecTicker(cik=1, name="NVIDIA CORP", ticker="NVDA", exchange="NASDAQ")], {})["decision"] == "missing_sic"


def test_build_metadata_updates_emits_sector_override():
    updates = build_metadata_updates(
        [
            {
                "decision": "accept",
                "ticker": "NVDA",
                "exchange": "NASDAQ",
                "sector_update": "Information Technology",
                "sec_sic": "3674",
                "sec_sic_description": "Semiconductors & Related Devices",
            },
            {"decision": "missing_sic", "ticker": "BAD", "exchange": "NASDAQ", "sector_update": ""},
        ]
    )

    assert updates == [
        {
            "ticker": "NVDA",
            "exchange": "NASDAQ",
            "field": "stock_sector",
            "decision": "update",
            "proposed_value": "Information Technology",
            "confidence": "0.72",
            "reason": "SEC submissions data lists SIC 3674 (Semiconductors & Related Devices) for the exact ticker/exchange CIK match; SIC was conservatively mapped to a canonical stock sector.",
        }
    ]


def test_ticker_normalization_matches_class_separator_style():
    assert normalized_ticker_key("BRK-B") == "BRK.B"

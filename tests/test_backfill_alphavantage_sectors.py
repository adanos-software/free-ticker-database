from __future__ import annotations

import csv

from scripts.backfill_alphavantage_sectors import (
    build_metadata_updates,
    evaluate_alphavantage_row,
    load_missing_sector_rows,
    normalize_alphavantage_sector,
    normalized_ticker_key,
)


def test_normalization_maps_alphavantage_sector_to_canonical_sector():
    assert normalize_alphavantage_sector("BASIC MATERIALS") == "Materials"
    assert normalize_alphavantage_sector("CONSUMER CYCLICAL") == "Consumer Discretionary"
    assert normalize_alphavantage_sector("FINANCIAL SERVICES") == "Financials"
    assert normalize_alphavantage_sector("TRADE & SERVICES") == ""


def test_load_missing_sector_rows_filters_us_stocks(tmp_path):
    path = tmp_path / "tickers.csv"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["ticker", "exchange", "asset_type", "name", "sector"])
        writer.writeheader()
        writer.writerows(
            [
                {"ticker": "AAUC", "exchange": "NYSE", "asset_type": "Stock", "name": "Allied Gold Corporation", "sector": ""},
                {"ticker": "AAPL", "exchange": "NASDAQ", "asset_type": "Stock", "name": "Apple Inc", "sector": "Information Technology"},
                {"ticker": "QQQ", "exchange": "NASDAQ", "asset_type": "ETF", "name": "Invesco QQQ Trust", "sector": ""},
                {"ticker": "SHOP", "exchange": "TSX", "asset_type": "Stock", "name": "Shopify Inc", "sector": ""},
            ]
        )

    rows = load_missing_sector_rows(exchanges={"NASDAQ", "NYSE"}, tickers_csv=path)

    assert [row["ticker"] for row in rows] == ["AAUC"]


def test_evaluate_alphavantage_row_accepts_strict_match():
    result = evaluate_alphavantage_row(
        {
            "ticker": "ABLV",
            "exchange": "NASDAQ",
            "asset_type": "Stock",
            "name": "Able View Global Inc. Class B Ordinary Shares",
            "sector": "",
        },
        {
            "Symbol": "ABLV",
            "Name": "Able View Global Inc. Class B Ordinary Shares",
            "Exchange": "NASDAQ",
            "AssetType": "Common Stock",
            "Sector": "CONSUMER CYCLICAL",
            "Industry": "SPECIALTY RETAIL",
        },
    )

    assert result["decision"] == "accept"
    assert result["sector_update"] == "Consumer Discretionary"


def test_evaluate_alphavantage_row_rejects_mismatches_and_empty_sector():
    row = {
        "ticker": "AAUC",
        "exchange": "NYSE",
        "asset_type": "Stock",
        "name": "Allied Gold Corporation",
        "sector": "",
    }

    assert evaluate_alphavantage_row(row, {"Information": "rate limit"})["decision"] == "api_message"
    assert evaluate_alphavantage_row(
        row,
        {"Symbol": "AAUC", "Name": "Allied Gold Corporation", "Exchange": "NASDAQ", "AssetType": "Common Stock", "Sector": "BASIC MATERIALS"},
    )["decision"] == "exchange_mismatch"
    assert evaluate_alphavantage_row(
        row,
        {"Symbol": "AAUC", "Name": "Different Corp", "Exchange": "NYSE", "AssetType": "Common Stock", "Sector": "BASIC MATERIALS"},
    )["decision"] == "name_mismatch"
    assert evaluate_alphavantage_row(
        row,
        {"Symbol": "AAUC", "Name": "Allied Gold Corporation", "Exchange": "NYSE", "AssetType": "Common Stock", "Sector": ""},
    )["decision"] == "missing_sector"


def test_build_metadata_updates_emits_sector_override():
    updates = build_metadata_updates(
        [
            {"decision": "accept", "ticker": "AAUC", "exchange": "NYSE", "sector_update": "Materials"},
            {"decision": "missing_sector", "ticker": "BAD", "exchange": "NYSE", "sector_update": ""},
        ]
    )

    assert updates == [
        {
            "ticker": "AAUC",
            "exchange": "NYSE",
            "field": "stock_sector",
            "decision": "update",
            "proposed_value": "Materials",
            "confidence": "0.76",
            "reason": "Alpha Vantage OVERVIEW supplied a normalized stock sector for a US listing without sector; accepted only after ticker, exchange, Common Stock asset type, issuer-name, numeric-token, and canonical-sector gates matched.",
        }
    ]


def test_ticker_normalization_matches_us_class_separator_style():
    assert normalized_ticker_key("BRK-B") == "BRK.B"

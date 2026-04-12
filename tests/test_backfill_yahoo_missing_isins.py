from __future__ import annotations

import csv

from scripts.backfill_yahoo_missing_isins import (
    build_metadata_updates,
    evaluate_missing_isin_row,
    load_missing_isin_rows,
    strict_names_match,
)


def test_load_missing_isin_rows_filters_exchange_asset_and_isin(tmp_path):
    path = tmp_path / "tickers.csv"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["ticker", "exchange", "asset_type", "name", "isin"])
        writer.writeheader()
        writer.writerows(
            [
                {"ticker": "AAAA", "exchange": "BATS", "asset_type": "ETF", "name": "Amplius Aggressive Asset Allocation ETF", "isin": ""},
                {"ticker": "AAPL", "exchange": "NASDAQ", "asset_type": "Stock", "name": "Apple Inc.", "isin": ""},
                {"ticker": "ASX", "exchange": "ASX", "asset_type": "Stock", "name": "ASX Limited", "isin": ""},
                {"ticker": "IVV", "exchange": "NYSE ARCA", "asset_type": "ETF", "name": "iShares Core S&P 500 ETF", "isin": "US4642872000"},
            ]
        )

    rows = load_missing_isin_rows(exchanges={"BATS", "NASDAQ"}, asset_types={"ETF"}, tickers_csv=path)

    assert [row["ticker"] for row in rows] == ["AAAA"]


def test_strict_names_match_rejects_rollover_number_mismatch():
    assert strict_names_match("Amplius Aggressive Asset Allocation ETF", "Amplius Aggressive Asset Allocation ETF")
    assert not strict_names_match("Innovator Equity Defined Protection ETF - 2 Yr To April 2028", "Innovator Equity Defined Protection ETF - 2 Yr To April 2026")


def test_evaluate_missing_isin_row_accepts_valid_yahoo_match():
    result = evaluate_missing_isin_row(
        {"ticker": "AAAA", "exchange": "BATS", "asset_type": "ETF", "name": "Amplius Aggressive Asset Allocation ETF"},
        {
            "exists": True,
            "symbol": "AAAA",
            "longName": "Amplius Aggressive Asset Allocation ETF",
            "quoteType": "ETF",
            "exchange": "BTS",
            "fullExchangeName": "Cboe US",
            "isin": "US02072Q6897",
            "history_rows": 5,
        },
    )

    assert result["decision"] == "accept"
    assert result["yahoo_isin"] == "US02072Q6897"


def test_evaluate_missing_isin_row_rejects_number_token_mismatch():
    result = evaluate_missing_isin_row(
        {"ticker": "AAPR", "exchange": "BATS", "asset_type": "ETF", "name": "Innovator Equity Defined Protection ETF - 2 Yr To April 2026"},
        {
            "exists": True,
            "symbol": "AAPR",
            "longName": "Innovator Equity Defined Protection ETF - 2 Yr To April 2028",
            "quoteType": "ETF",
            "exchange": "BTS",
            "fullExchangeName": "Cboe US",
            "isin": "US45783Y3356",
            "history_rows": 5,
        },
    )

    assert result["decision"] == "number_token_mismatch"


def test_evaluate_missing_isin_row_rejects_unexpected_isin_country():
    result = evaluate_missing_isin_row(
        {"ticker": "AAAA", "exchange": "BATS", "asset_type": "ETF", "name": "Amplius Aggressive Asset Allocation ETF"},
        {
            "exists": True,
            "symbol": "AAAA",
            "longName": "Amplius Aggressive Asset Allocation ETF",
            "quoteType": "ETF",
            "exchange": "BTS",
            "fullExchangeName": "Cboe US",
            "isin": "CA02072Q6893",
            "history_rows": 5,
        },
    )

    assert result["decision"] == "isin_country_mismatch"


def test_build_metadata_updates_emits_isin_override():
    updates = build_metadata_updates(
        [
            {"decision": "accept", "ticker": "AAAA", "exchange": "BATS", "yahoo_isin": "US02072Q6897"},
            {"decision": "name_mismatch", "ticker": "BAD", "exchange": "BATS", "yahoo_isin": "US0000000000"},
        ]
    )

    assert updates == [
        {
            "ticker": "AAAA",
            "exchange": "BATS",
            "field": "isin",
            "decision": "update",
            "proposed_value": "US02072Q6897",
            "confidence": "0.86",
            "reason": "Yahoo Finance returned a valid ISIN for a row without ISIN, accepted only after exact Yahoo venue, quote type, expected ISIN country prefix, strict issuer/product-name, numeric-token, and ISIN-checksum gates matched.",
        }
    ]

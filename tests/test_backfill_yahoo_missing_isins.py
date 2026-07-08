from __future__ import annotations

import csv

from scripts.backfill_yahoo_missing_isins import (
    apply_reviewed_isin_overrides,
    build_metadata_updates,
    evaluate_missing_isin_row,
    load_reviewed_isin_overrides,
    load_missing_isin_rows,
    strict_names_match,
    write_report_csv,
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


def test_evaluate_missing_isin_row_accepts_valid_tsxv_yahoo_match():
    result = evaluate_missing_isin_row(
        {"ticker": "AF.P", "exchange": "TSXV", "asset_type": "Stock", "name": "AF2 Capital Corp."},
        {
            "exists": True,
            "symbol": "AF.P.V",
            "longName": "AF2 Capital Corp",
            "quoteType": "EQUITY",
            "exchange": "VAN",
            "fullExchangeName": "TSXV",
            "isin": "CA0010941015",
            "history_rows": 5,
        },
    )

    assert result["decision"] == "accept"
    assert result["yahoo_isin"] == "CA0010941015"


def test_evaluate_missing_isin_row_accepts_valid_neo_yahoo_match():
    result = evaluate_missing_isin_row(
        {"ticker": "HNDA", "exchange": "NEO", "asset_type": "Stock", "name": "HONDA MOTOR CO LTD CDR (CAD Hedged)"},
        {
            "exists": True,
            "symbol": "HNDA.NE",
            "longName": "Honda Motor Co., Ltd. CDR (CAD Hedged)",
            "quoteType": "EQUITY",
            "exchange": "NEO",
            "fullExchangeName": "Cboe CA",
            "isin": "CA05277B2093",
            "history_rows": 5,
        },
    )

    assert result["decision"] == "accept"
    assert result["yahoo_isin"] == "CA05277B2093"


def test_evaluate_missing_isin_row_accepts_foreign_incorporated_nasdaq_match():
    result = evaluate_missing_isin_row(
        {"ticker": "AACI", "exchange": "NASDAQ", "asset_type": "Stock", "name": "Armada Acquisition Corp. III"},
        {
            "exists": True,
            "symbol": "AACI",
            "longName": "Armada Acquisition Corp. III",
            "quoteType": "EQUITY",
            "exchange": "NMS",
            "fullExchangeName": "NasdaqGS",
            "isin": "KYG0R38M1018",
            "history_rows": 5,
        },
    )

    assert result["decision"] == "accept"
    assert result["yahoo_isin"] == "KYG0R38M1018"


def test_evaluate_missing_isin_row_accepts_foreign_incorporated_nyse_match():
    result = evaluate_missing_isin_row(
        {"ticker": "AIIA", "exchange": "NYSE", "asset_type": "Stock", "name": "AI Infrastructure Acquisition Corp. Class A Ordinary Shares"},
        {
            "exists": True,
            "symbol": "AIIA",
            "longName": "AI Infrastructure Acquisition Corp.",
            "quoteType": "EQUITY",
            "exchange": "NYQ",
            "fullExchangeName": "NYSE",
            "isin": "KYG013361095",
            "history_rows": 5,
        },
    )

    assert result["decision"] == "accept"
    assert result["yahoo_isin"] == "KYG013361095"


def test_evaluate_missing_isin_row_rejects_foreign_nyse_name_mismatch_after_prefix_gate():
    result = evaluate_missing_isin_row(
        {"ticker": "SLB", "exchange": "NYSE", "asset_type": "Stock", "name": "SLB Limited Common Shares"},
        {
            "exists": True,
            "symbol": "SLB",
            "longName": "SLB N.V.",
            "quoteType": "EQUITY",
            "exchange": "NYQ",
            "fullExchangeName": "NYSE",
            "isin": "AN8068571086",
            "history_rows": 5,
        },
    )

    assert result["decision"] == "name_mismatch"


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


def test_reviewed_isin_override_suppresses_future_yahoo_accept(tmp_path):
    metadata_updates = tmp_path / "metadata_updates.csv"
    metadata_updates.write_text(
        "ticker,exchange,field,decision,proposed_value,confidence,reason\n"
        "NMAD,NASDAQ,isin,clear,,0.95,OpenFIGI mismatch\n"
        "TTE,NYSE,isin,update,FR0000120271,0.95,Official issuer source\n",
        encoding="utf-8",
    )

    results = apply_reviewed_isin_overrides(
        [
            {
                "decision": "accept",
                "ticker": "NMAD",
                "exchange": "NASDAQ",
                "yahoo_isin": "JO3123411014",
            },
            {
                "decision": "accept",
                "ticker": "AACI",
                "exchange": "NASDAQ",
                "yahoo_isin": "KYG0R38M1018",
            },
            {
                "decision": "accept",
                "ticker": "TTE",
                "exchange": "NYSE",
                "yahoo_isin": "CA89158C1068",
            },
        ],
        load_reviewed_isin_overrides(metadata_updates),
    )

    assert results[0]["decision"] == "reviewed_isin_override"
    assert results[1]["decision"] == "accept"
    assert results[2]["decision"] == "reviewed_isin_override"
    assert build_metadata_updates(results) == [
        {
            "ticker": "AACI",
            "exchange": "NASDAQ",
            "field": "isin",
            "decision": "update",
            "proposed_value": "KYG0R38M1018",
            "confidence": "0.86",
            "reason": "Yahoo Finance returned a valid ISIN for a row without ISIN, accepted only after exact Yahoo venue, quote type, expected ISIN country prefix, strict issuer/product-name, numeric-token, and ISIN-checksum gates matched.",
        }
    ]


def test_write_report_csv_uses_lf_line_endings(tmp_path):
    path = tmp_path / "missing_isin_backfill.csv"

    write_report_csv(
        path,
        [
            {
                "ticker": "AAAA",
                "exchange": "BATS",
                "asset_type": "ETF",
                "name": "Amplius Aggressive Asset Allocation ETF",
                "yahoo_symbol": "AAAA",
                "yahoo_name": "Amplius Aggressive Asset Allocation ETF",
                "yahoo_quote_type": "ETF",
                "yahoo_exchange": "BTS",
                "yahoo_full_exchange": "Cboe US",
                "yahoo_isin": "US02072Q6897",
                "history_rows": 5,
                "exchange_match": True,
                "quote_type_match": True,
                "name_match": True,
                "number_tokens_match": True,
                "decision": "accept",
                "error": "",
            }
        ],
    )

    content = path.read_bytes()
    assert b"\r\n" not in content
    assert content.endswith(b"\n")

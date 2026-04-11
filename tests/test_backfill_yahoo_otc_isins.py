from __future__ import annotations

import csv

from scripts.backfill_yahoo_otc_isins import (
    build_metadata_updates,
    evaluate_otc_missing_isin_row,
    load_otc_missing_isin_rows,
    parse_args,
    write_report_csv,
)


def test_load_otc_missing_isin_rows_filters_only_otc_rows_without_isin(tmp_path):
    path = tmp_path / "tickers.csv"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["ticker", "exchange", "asset_type", "name", "isin"])
        writer.writeheader()
        writer.writerows(
            [
                {"ticker": "AAHIF", "exchange": "OTC", "asset_type": "Stock", "name": "Asahi Co. Ltd", "isin": ""},
                {"ticker": "AAPL", "exchange": "NASDAQ", "asset_type": "Stock", "name": "Apple Inc.", "isin": ""},
                {"ticker": "ABOIF", "exchange": "OTC", "asset_type": "Stock", "name": "Aboitiz Equity Ventures Inc", "isin": "PHY0001Z1040"},
            ]
        )

    rows = load_otc_missing_isin_rows(path)

    assert [row["ticker"] for row in rows] == ["AAHIF"]


def test_evaluate_otc_missing_isin_accepts_valid_yahoo_match():
    row = {
        "ticker": "AAHIF",
        "exchange": "OTC",
        "asset_type": "Stock",
        "name": "Asahi Co. Ltd",
        "isin": "",
    }
    yahoo_result = {
        "exists": True,
        "symbol": "AAHIF",
        "quoteType": "EQUITY",
        "longName": "Asahi Co., Ltd.",
        "shortName": "",
        "exchange": "PNK",
        "fullExchangeName": "OTC Markets OTCPK",
        "isin": "JP3110500000",
        "history_rows": 5,
    }

    result = evaluate_otc_missing_isin_row(row, yahoo_result)

    assert result["decision"] == "accept"
    assert result["yahoo_isin"] == "JP3110500000"
    assert result["exchange_match"] is True
    assert result["quote_type_match"] is True
    assert result["name_match"] is True


def test_evaluate_otc_missing_isin_rejects_blank_invalid_and_mismatch_cases():
    row = {
        "ticker": "ACOPF",
        "exchange": "OTC",
        "asset_type": "Stock",
        "name": "The a2 Milk Company Limited",
        "isin": "",
    }
    base = {
        "exists": True,
        "symbol": "ACOPF",
        "quoteType": "EQUITY",
        "longName": "The a2 Milk Company Limited",
        "exchange": "PNK",
        "fullExchangeName": "OTC Markets OTCPK",
        "history_rows": 5,
    }

    assert evaluate_otc_missing_isin_row(row, {**base, "isin": "-"})["decision"] == "missing_isin"
    assert evaluate_otc_missing_isin_row(row, {**base, "isin": "US0000000000"})["decision"] == "invalid_isin"
    assert evaluate_otc_missing_isin_row(row, {**base, "isin": "NZATME0002S8", "exchange": "NMS", "fullExchangeName": "NasdaqGS"})["decision"] == "exchange_mismatch"
    assert evaluate_otc_missing_isin_row(row, {**base, "isin": "NZATME0002S8", "quoteType": "ETF"})["decision"] == "quote_type_mismatch"
    assert evaluate_otc_missing_isin_row(row, {**base, "isin": "NZATME0002S8", "longName": "Different Company Inc."})["decision"] == "name_mismatch"


def test_build_metadata_updates_emits_only_accepted_isin_updates():
    updates = build_metadata_updates(
        [
            {
                "ticker": "AAHIF",
                "exchange": "OTC",
                "decision": "accept",
                "yahoo_isin": "JP3110500000",
            },
            {
                "ticker": "ACOPF",
                "exchange": "OTC",
                "decision": "missing_isin",
                "yahoo_isin": "",
            },
        ]
    )

    assert updates == [
        {
            "ticker": "AAHIF",
            "exchange": "OTC",
            "field": "isin",
            "decision": "update",
            "proposed_value": "JP3110500000",
            "confidence": "0.85",
            "reason": "Yahoo Finance returned a valid ISIN for an OTC row without ISIN, with matching OTC venue, quote type, and issuer name; used as a gated review override rather than a hard source import.",
        }
    ]


def test_write_report_csv_handles_empty_rows(tmp_path):
    path = tmp_path / "report.csv"
    write_report_csv(path, [])

    assert path.read_text(encoding="utf-8").startswith("ticker,exchange,asset_type")


def test_parse_args_supports_ticker_filter_and_apply():
    args = parse_args(["--ticker", "AAHIF", "--ticker", "ABOIF", "--apply", "--limit", "20"])

    assert args.ticker == ["AAHIF", "ABOIF"]
    assert args.apply is True
    assert args.limit == 20

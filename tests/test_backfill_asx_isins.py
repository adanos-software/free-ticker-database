from __future__ import annotations

import csv
from io import BytesIO

import pandas as pd

from scripts.backfill_asx_isins import (
    AsxIsinRow,
    build_metadata_updates,
    evaluate_asx_row,
    load_asx_missing_isin_rows,
    parse_asx_isin_xls,
    strict_names_match,
)


def make_asx_xls() -> bytes:
    buffer = BytesIO()
    dataframe = pd.DataFrame(
        [
            {"ASX code": "", "Company name": "", "Security type": "", "ISIN code": ""},
            {
                "ASX code": "A2M",
                "Company name": "THE A2 MILK COMPANY LIMITED",
                "Security type": "ORDINARY FULLY PAID",
                "ISIN code": "NZATME0002S8",
            },
            {
                "ASX code": "BAD",
                "Company name": "Bad Isin Ltd",
                "Security type": "ORDINARY FULLY PAID",
                "ISIN code": "NOTANISIN",
            },
        ]
    )
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        dataframe.to_excel(writer, sheet_name="ISIN", index=False)
    return buffer.getvalue()


def test_parse_asx_isin_xls_filters_blank_and_invalid_rows():
    assert parse_asx_isin_xls(make_asx_xls()) == [
        AsxIsinRow(
            ticker="A2M",
            name="THE A2 MILK COMPANY LIMITED",
            security_type="ORDINARY FULLY PAID",
            isin="NZATME0002S8",
        )
    ]


def test_load_asx_missing_isin_rows_filters_exchange_and_empty_isin(tmp_path):
    path = tmp_path / "tickers.csv"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["ticker", "exchange", "asset_type", "name", "isin"])
        writer.writeheader()
        writer.writerows(
            [
                {"ticker": "A2M", "exchange": "ASX", "asset_type": "Stock", "name": "THE A2 MILK COMPANY LIMITED", "isin": ""},
                {"ticker": "AAPL", "exchange": "NASDAQ", "asset_type": "Stock", "name": "Apple Inc.", "isin": ""},
                {"ticker": "ASX", "exchange": "ASX", "asset_type": "Stock", "name": "ASX LIMITED", "isin": "AU000000ASX7"},
            ]
        )

    assert [row["ticker"] for row in load_asx_missing_isin_rows(path)] == ["A2M"]


def test_strict_names_match_rejects_numeric_token_drift():
    assert strict_names_match("Innovator Equity Defined Protection ETF - 2 Yr To April 2026", "Innovator Equity Defined Protection ETF - 2 Yr To April 2026")
    assert not strict_names_match("Innovator Equity Defined Protection ETF - 2 Yr To April 2028", "Innovator Equity Defined Protection ETF - 2 Yr To April 2026")


def test_evaluate_asx_row_accepts_official_code_name_and_isin():
    result = evaluate_asx_row(
        {"ticker": "A2M", "exchange": "ASX", "asset_type": "Stock", "name": "THE A2 MILK COMPANY LIMITED", "isin": ""},
        {"A2M": AsxIsinRow("A2M", "THE A2 MILK COMPANY LIMITED", "ORDINARY FULLY PAID", "NZATME0002S8")},
    )

    assert result["decision"] == "accept"
    assert result["asx_isin"] == "NZATME0002S8"


def test_build_metadata_updates_emits_isin_override():
    updates = build_metadata_updates(
        [
            {"decision": "accept", "ticker": "A2M", "exchange": "ASX", "asx_isin": "NZATME0002S8"},
            {"decision": "name_mismatch", "ticker": "BAD", "exchange": "ASX", "asx_isin": "AU0000000000"},
        ]
    )

    assert updates == [
        {
            "ticker": "A2M",
            "exchange": "ASX",
            "field": "isin",
            "decision": "update",
            "proposed_value": "NZATME0002S8",
            "confidence": "0.96",
            "reason": "Official ASX ISIN.xls lists this ASX code with a valid ISIN; accepted only after exact ASX code, issuer-name, numeric-token, and ISIN-checksum gates matched a current ASX row without ISIN.",
        }
    ]

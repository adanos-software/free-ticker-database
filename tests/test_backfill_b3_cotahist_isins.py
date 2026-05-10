from __future__ import annotations

import csv
from io import BytesIO
from zipfile import ZipFile

from scripts.backfill_b3_cotahist_isins import (
    CotahistRow,
    build_metadata_updates,
    evaluate_b3_row,
    latest_rows_by_ticker,
    load_b3_missing_isin_rows,
    parse_cotahist_line,
    parse_cotahist_text,
    read_cotahist_zip,
)


def make_line(
    *,
    ticker: str = "PETR4",
    date: str = "20260508",
    bdi_code: str = "02",
    market_type: str = "010",
    name: str = "PETROBRAS",
    isin: str = "BRPETRACNPR6",
) -> str:
    line = list(" " * 245)
    line[0:2] = "01"
    line[2:10] = date
    line[10:12] = bdi_code
    line[12:24] = f"{ticker:<12}"[:12]
    line[24:27] = market_type
    line[27:39] = f"{name:<12}"[:12]
    line[230:242] = isin
    return "".join(line)


def test_parse_cotahist_line_reads_official_fixed_width_fields():
    assert parse_cotahist_line(make_line()) == CotahistRow(
        ticker="PETR4",
        name="PETROBRAS",
        bdi_code="02",
        market_type="010",
        date="20260508",
        isin="BRPETRACNPR6",
    )


def test_parse_cotahist_line_rejects_non_data_non_spot_and_invalid_isin():
    assert parse_cotahist_line("00" + " " * 243) is None
    assert parse_cotahist_line(make_line(market_type="070")) is None
    assert parse_cotahist_line(make_line(isin="US0378331005")) is None
    assert parse_cotahist_line(make_line(isin="BRPETRACNPR0")) is None


def test_parse_cotahist_text_and_zip():
    buffer = BytesIO()
    with ZipFile(buffer, "w") as archive:
        archive.writestr("COTAHIST_A2026.TXT", make_line() + "\n")

    assert parse_cotahist_text(read_cotahist_zip(buffer.getvalue())) == [
        CotahistRow("PETR4", "PETROBRAS", "02", "010", "20260508", "BRPETRACNPR6")
    ]


def test_latest_rows_by_ticker_keeps_only_latest_distinct_records():
    rows = [
        CotahistRow("PETR4", "PETROBRAS", "02", "010", "20250508", "BRPETRACNPR6"),
        CotahistRow("PETR4", "PETROBRAS", "02", "010", "20260508", "BRPETRACNPR6"),
        CotahistRow("PETR4", "PETROBRAS", "96", "020", "20260508", "BRPETRACNPR6"),
    ]

    assert latest_rows_by_ticker(rows)["PETR4"] == rows[1:]


def test_load_b3_missing_isin_rows_filters_exchange_and_empty_isin(tmp_path):
    path = tmp_path / "tickers.csv"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["ticker", "exchange", "asset_type", "name", "isin"])
        writer.writeheader()
        writer.writerows(
            [
                {"ticker": "PETR4", "exchange": "B3", "asset_type": "Stock", "name": "Petrobras", "isin": ""},
                {"ticker": "AAPL", "exchange": "NASDAQ", "asset_type": "Stock", "name": "Apple Inc.", "isin": ""},
                {"ticker": "VALE3", "exchange": "B3", "asset_type": "Stock", "name": "Vale", "isin": "BRVALEACNOR0"},
            ]
        )

    assert [row["ticker"] for row in load_b3_missing_isin_rows(path)] == ["PETR4"]


def test_evaluate_b3_row_accepts_exact_ticker_unique_isin():
    result = evaluate_b3_row(
        {"ticker": "PETR4", "exchange": "B3", "asset_type": "Stock", "name": "Petrobras", "isin": ""},
        [CotahistRow("PETR4", "PETROBRAS", "02", "010", "20260508", "BRPETRACNPR6")],
    )

    assert result["decision"] == "accept"
    assert result["cotahist_isin"] == "BRPETRACNPR6"


def test_evaluate_b3_row_rejects_missing_and_ambiguous_isin():
    target = {"ticker": "PETR4", "exchange": "B3", "asset_type": "Stock", "name": "Petrobras", "isin": ""}
    assert evaluate_b3_row(target, [])["decision"] == "no_cotahist_match"
    assert (
        evaluate_b3_row(
            target,
            [
                CotahistRow("PETR4", "PETROBRAS", "02", "010", "20260508", "BRPETRACNPR6"),
                CotahistRow("PETR4", "PETROBRAS", "02", "010", "20260508", "BRPETRACNOR9"),
            ],
        )["decision"]
        == "ambiguous_cotahist_isin"
    )


def test_build_metadata_updates_emits_review_override():
    updates = build_metadata_updates(
        [
            {
                "decision": "accept",
                "ticker": "PETR4",
                "exchange": "B3",
                "cotahist_isin": "BRPETRACNPR6",
            },
            {"decision": "no_cotahist_match", "ticker": "BAD", "exchange": "B3", "cotahist_isin": ""},
        ],
        [2026],
    )

    assert updates == [
        {
            "ticker": "PETR4",
            "exchange": "B3",
            "field": "isin",
            "decision": "update",
            "proposed_value": "BRPETRACNPR6",
            "confidence": "0.95",
            "reason": "Official B3 COTAHIST annual files supplied a valid BR ISIN for an exact B3 trading code; accepted only for spot/fractional market records after ticker, BR-prefix, and ISIN-checksum gates. Years checked: 2026.",
        }
    ]

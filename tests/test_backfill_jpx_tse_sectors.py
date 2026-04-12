from __future__ import annotations

import csv

from scripts.backfill_jpx_tse_sectors import (
    JpxListedIssue,
    build_metadata_updates,
    evaluate_jpx_sector_row,
    load_missing_tse_sector_rows,
    normalize_jpx_code,
    normalize_jpx_sector,
    verify_jpx_tse_sectors,
)


def test_normalize_jpx_code_handles_excel_numeric_codes():
    assert normalize_jpx_code("1301.0") == "1301"
    assert normalize_jpx_code(1301) == "1301"


def test_normalize_jpx_sector_maps_33_industries_to_canonical_sector():
    assert normalize_jpx_sector("電気機器") == "Information Technology"
    assert normalize_jpx_sector("銀行業") == "Financials"
    assert normalize_jpx_sector("不動産業") == "Real Estate"
    assert normalize_jpx_sector("-") == ""


def test_load_missing_tse_sector_rows_filters_stock_rows(tmp_path):
    path = tmp_path / "tickers.csv"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["ticker", "exchange", "asset_type", "name", "sector"])
        writer.writeheader()
        writer.writerows(
            [
                {"ticker": "1301", "exchange": "TSE", "asset_type": "Stock", "name": "Kyokuyo Co Ltd", "sector": ""},
                {"ticker": "1305", "exchange": "TSE", "asset_type": "ETF", "name": "iFreeETF TOPIX", "sector": ""},
                {"ticker": "AAPL", "exchange": "NASDAQ", "asset_type": "Stock", "name": "Apple Inc", "sector": ""},
                {"ticker": "7203", "exchange": "TSE", "asset_type": "Stock", "name": "Toyota Motor Corp", "sector": "Consumer Discretionary"},
            ]
        )

    rows = load_missing_tse_sector_rows(tickers_csv=path)

    assert [row["ticker"] for row in rows] == ["1301"]


def test_evaluate_jpx_sector_row_accepts_code_match():
    result = evaluate_jpx_sector_row(
        {"ticker": "1301", "exchange": "TSE", "asset_type": "Stock", "name": "Kyokuyo Co Ltd", "sector": ""},
        [
            JpxListedIssue(
                code="1301",
                name="極洋",
                market_segment="プライム（内国株式）",
                industry_code_33="50",
                industry_33="水産・農林業",
            )
        ],
    )

    assert result["decision"] == "accept"
    assert result["sector_update"] == "Consumer Staples"


def test_evaluate_jpx_sector_row_rejects_missing_and_ambiguous_candidates():
    row = {"ticker": "1301", "exchange": "TSE", "asset_type": "Stock", "name": "Kyokuyo Co Ltd", "sector": ""}
    issue = JpxListedIssue(code="1301", name="極洋", market_segment="ETF・ETN", industry_code_33="-", industry_33="-")

    assert evaluate_jpx_sector_row(row, [])["decision"] == "no_jpx_match"
    assert evaluate_jpx_sector_row(row, [issue, issue])["decision"] == "ambiguous_jpx_match"
    assert evaluate_jpx_sector_row(row, [issue])["decision"] == "missing_jpx_industry"


def test_verify_jpx_tse_sectors_indexes_by_code():
    results = verify_jpx_tse_sectors(
        [{"ticker": "1301", "exchange": "TSE", "asset_type": "Stock", "name": "Kyokuyo Co Ltd", "sector": ""}],
        [JpxListedIssue(code="1301", name="極洋", market_segment="プライム（内国株式）", industry_code_33="50", industry_33="水産・農林業")],
    )

    assert results[0]["decision"] == "accept"


def test_build_metadata_updates_emits_sector_override():
    updates = build_metadata_updates(
        [
            {"decision": "accept", "ticker": "1301", "exchange": "TSE", "sector_update": "Consumer Staples", "jpx_33_industry": "水産・農林業"},
            {"decision": "missing_jpx_industry", "ticker": "1305", "exchange": "TSE", "sector_update": "", "jpx_33_industry": "-"},
        ]
    )

    assert updates == [
        {
            "ticker": "1301",
            "exchange": "TSE",
            "field": "stock_sector",
            "decision": "update",
            "proposed_value": "Consumer Staples",
            "confidence": "0.74",
            "reason": "Official JPX listed-issues file maps TSE code 1301 to JPX 33-industry '水産・農林業', which was normalized to a canonical stock sector.",
        }
    ]

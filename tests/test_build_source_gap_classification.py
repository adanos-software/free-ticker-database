from __future__ import annotations

from scripts.build_completion_backlog import (
    FIELD_MISSING_ETF_CATEGORY,
    FIELD_MISSING_ISIN,
    FIELD_MISSING_STOCK_SECTOR,
)
from scripts.build_source_gap_classification import build_source_gap_classifications, summarize


def test_build_source_gap_classifications_covers_all_gap_types() -> None:
    rows = build_source_gap_classifications(
        core_listings=[
            {
                "listing_key": "ASX::AF2",
                "ticker": "AF2",
                "exchange": "ASX",
                "asset_type": "ETF",
                "name": "ANGLE ASSET FINANCE - RADIAN TRUST 2025-1",
                "scope_reason": "primary_listing_missing_isin",
            }
        ],
        tickers=[
            {
                "ticker": "OTC1",
                "exchange": "OTC",
                "asset_type": "Stock",
                "name": "Example OTC Inc.",
                "stock_sector": "",
                "etf_category": "",
            },
            {
                "ticker": "BTCX",
                "exchange": "TSX",
                "asset_type": "ETF",
                "name": "Example Bitcoin ETF",
                "stock_sector": "",
                "etf_category": "",
            },
        ],
    )

    by_field = {row.field: row for row in rows}
    assert by_field[FIELD_MISSING_ISIN].gap_class == "debt_or_securitized_identifier_gap"
    assert by_field[FIELD_MISSING_STOCK_SECTOR].gap_class == "otc_sector_source_gap"
    assert by_field[FIELD_MISSING_ETF_CATEGORY].gap_class == "digital_asset_etf_category_gap"
    assert all(row.review_needed for row in rows)
    assert all(row.source_gate for row in rows)


def test_summarize_reports_field_and_class_totals() -> None:
    rows = build_source_gap_classifications(
        core_listings=[
            {
                "listing_key": "TSXV::ABC-H",
                "ticker": "ABC-H",
                "exchange": "TSXV",
                "asset_type": "Stock",
                "name": "ABC Holdings",
                "scope_reason": "primary_listing_missing_isin",
            }
        ],
        tickers=[],
    )

    summary = summarize(rows, "2026-05-11T00:00:00Z")

    assert summary["rows"] == 1
    assert summary["field_totals"] == {FIELD_MISSING_ISIN: 1}
    assert summary["class_totals"] == {"capital_pool_or_halted_identifier_gap": 1}
    assert summary["policy"]["release_gate"]


def test_tmx_cpc_sector_evidence_classifies_as_core_exclusion_candidate() -> None:
    rows = build_source_gap_classifications(
        core_listings=[],
        tickers=[
            {
                "ticker": "AAA.P",
                "exchange": "TSXV",
                "asset_type": "Stock",
                "name": "First Tidal Acquisition Corp.",
                "stock_sector": "",
                "etf_category": "",
            }
        ],
        tmx_sector_results=[
            {
                "ticker": "AAA.P",
                "exchange": "TSXV",
                "tmx_sector": "CPC",
                "decision": "unsupported_or_ambiguous_tmx_sector",
            }
        ],
    )

    assert len(rows) == 1
    assert rows[0].gap_class == "shell_or_cpc_sector_gap"
    assert "TMX" in rows[0].recommended_next_source


def test_depositary_stock_sector_gap_does_not_require_underlying_sector_fill() -> None:
    rows = build_source_gap_classifications(
        core_listings=[],
        tickers=[
            {
                "ticker": "BENZ",
                "exchange": "TSX",
                "asset_type": "Stock",
                "name": "Mercedes-Benz CDR (CAD Hedged)",
                "stock_sector": "",
                "etf_category": "",
            }
        ],
    )

    assert len(rows) == 1
    assert rows[0].gap_class == "adr_cdr_or_depositary_sector_gap"

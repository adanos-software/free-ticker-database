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


def test_implemented_official_source_marks_residual_sector_as_source_gap() -> None:
    rows = build_source_gap_classifications(
        core_listings=[],
        tickers=[
            {
                "ticker": "ABC",
                "exchange": "TESTX",
                "asset_type": "Stock",
                "name": "ABC PLC",
                "stock_sector": "",
                "etf_category": "",
            }
        ],
        source_inventory_rows=[
            {
                "exchange": "TESTX",
                "current_status": "official_full",
                "implementation_status": "implemented",
            }
        ],
    )

    assert len(rows) == 1
    assert rows[0].gap_class == "official_industry_taxonomy_unavailable_gap"


def test_implemented_source_without_isin_marks_identifier_as_source_gap() -> None:
    rows = build_source_gap_classifications(
        core_listings=[
            {
                "listing_key": "MSX::BKMB",
                "ticker": "BKMB",
                "exchange": "MSX",
                "asset_type": "Stock",
                "name": "BANK MUSCAT",
                "scope_reason": "primary_listing_missing_isin",
            }
        ],
        tickers=[],
        source_inventory_rows=[
            {
                "exchange": "MSX",
                "current_status": "official_full",
                "implementation_status": "implemented",
                "notes": "The official MSX companies API exposes symbol and sector but does not expose ISIN.",
                "blocker": "",
            }
        ],
    )

    assert len(rows) == 1
    assert rows[0].gap_class == "official_identifier_not_exposed_source_gap"


def test_exact_official_reference_without_isin_marks_identifier_as_source_gap() -> None:
    rows = build_source_gap_classifications(
        core_listings=[
            {
                "listing_key": "PSX::TEST",
                "ticker": "TEST",
                "exchange": "PSX",
                "asset_type": "Stock",
                "name": "Test Limited",
                "scope_reason": "primary_listing_missing_isin",
            }
        ],
        tickers=[],
        masterfile_reference_rows=[
            {
                "ticker": "TEST",
                "exchange": "PSX",
                "official": "true",
                "isin": "",
            }
        ],
    )

    assert len(rows) == 1
    assert rows[0].gap_class == "official_identifier_not_exposed_source_gap"


def test_current_official_directory_absence_marks_identifier_as_source_gap() -> None:
    rows = build_source_gap_classifications(
        core_listings=[
            {
                "listing_key": "B3::TEST3",
                "ticker": "TEST3",
                "exchange": "B3",
                "asset_type": "Stock",
                "name": "Test SA",
                "scope_reason": "primary_listing_missing_isin",
            }
        ],
        tickers=[],
        b3_cotahist_isin_probe_rows=[
            {
                "ticker": "TEST3",
                "exchange": "B3",
                "decision": "no_cotahist_match",
            }
        ],
    )

    assert len(rows) == 1
    assert rows[0].gap_class == "official_current_directory_absent_identifier_gap"


def test_official_etf_reference_without_taxonomy_marks_product_taxonomy_source_gap() -> None:
    rows = build_source_gap_classifications(
        core_listings=[],
        tickers=[
            {
                "ticker": "ETF1",
                "exchange": "TESTX",
                "asset_type": "ETF",
                "name": "Test ETF",
                "stock_sector": "",
                "etf_category": "",
            }
        ],
        masterfile_reference_rows=[
            {
                "ticker": "ETF1",
                "exchange": "TESTX",
                "official": "true",
                "sector": "",
            }
        ],
    )

    assert len(rows) == 1
    assert rows[0].gap_class == "official_product_taxonomy_unavailable_gap"


def test_rhodium_etcs_are_classified_as_commodity_category_gaps() -> None:
    rows = build_source_gap_classifications(
        core_listings=[],
        tickers=[
            {
                "ticker": "XFRD",
                "exchange": "XETRA",
                "asset_type": "ETF",
                "name": "db Physical Rhodium ETC (EUR)",
                "stock_sector": "",
                "etf_category": "",
            }
        ],
    )

    assert len(rows) == 1
    assert rows[0].gap_class == "commodity_etf_category_gap"

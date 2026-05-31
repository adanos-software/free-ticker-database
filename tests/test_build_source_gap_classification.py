from __future__ import annotations

from scripts.build_completion_backlog import (
    FIELD_MISSING_ETF_CATEGORY,
    FIELD_MISSING_ISIN,
    FIELD_MISSING_STOCK_SECTOR,
)
from scripts.build_source_gap_classification import (
    build_source_gap_classifications,
    classification_context_for,
    evidence_gate_context_for,
    source_gap_context_for,
    summarize,
    write_markdown,
)


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
    assert all(row.source_gap_context for row in rows)
    assert all(row.classification_context for row in rows)
    assert all(row.evidence_gate_context for row in rows)


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
    assert summary["top_source_gap_review_batches"] == [
        {
            "field": FIELD_MISSING_ISIN,
            "gap_class": "capital_pool_or_halted_identifier_gap",
            "exchange": "TSXV",
            "rows": 1,
            "recommended_next_source": "Current exchange issuer/status file or CPC/shell prospectus.",
            "source_gate": "Exact halted/CPC symbol and direct current identifier evidence.",
        }
    ]
    assert summary["policy"]["release_gate"]


def test_write_markdown_surfaces_top_review_batches(tmp_path) -> None:
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
    path = tmp_path / "source_gap_classification.md"

    write_markdown(path, rows, summary)

    markdown = path.read_text(encoding="utf-8")
    assert "## Top Review Batches" in markdown
    assert "| missing_isin_primary | capital_pool_or_halted_identifier_gap | TSXV | 1 |" in markdown
    assert "Exact halted/CPC symbol and direct current identifier evidence." in markdown


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


def test_implemented_official_source_without_exact_reference_marks_identifier_source_gap() -> None:
    rows = build_source_gap_classifications(
        core_listings=[
            {
                "listing_key": "TESTX::ALIAS",
                "ticker": "ALIAS",
                "exchange": "TESTX",
                "asset_type": "Stock",
                "name": "Alias Plc",
                "scope_reason": "primary_listing_missing_isin",
            }
        ],
        tickers=[],
        source_inventory_rows=[
            {
                "exchange": "TESTX",
                "current_status": "official_full",
                "implementation_status": "implemented",
            }
        ],
        masterfile_reference_rows=[
            {
                "ticker": "CANON",
                "exchange": "TESTX",
                "official": "true",
                "isin": "GB0000000000",
            }
        ],
    )

    assert len(rows) == 1
    assert rows[0].gap_class == "official_identifier_reference_unmatched_gap"


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


def test_implemented_official_source_without_exact_product_reference_marks_category_source_gap() -> None:
    rows = build_source_gap_classifications(
        core_listings=[],
        tickers=[
            {
                "ticker": "ETFALIAS",
                "exchange": "TESTX",
                "asset_type": "ETF",
                "name": "Alias ETF",
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
        masterfile_reference_rows=[
            {
                "ticker": "ETF",
                "exchange": "TESTX",
                "official": "true",
                "sector": "Equity",
            }
        ],
    )

    assert len(rows) == 1
    assert rows[0].gap_class == "official_product_reference_unmatched_category_gap"


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


def test_source_gap_contexts_are_derived_from_row_fields() -> None:
    row = {
        "listing_key": "TSX::ABC",
        "exchange": "TSX",
        "ticker": "ABC",
        "asset_type": "Stock",
        "field": "missing_isin_primary",
        "target_field": "isin",
        "name": "ABC Inc.",
        "gap_class": "official_identifier_source_gap",
        "review_needed": "true",
        "confidence_policy": "Require official source.",
        "recommended_next_source": "Official exchange detail feed.",
        "source_gate": "Exact exchange/symbol/name and checksum.",
    }

    assert (
        source_gap_context_for(row)
        == "listing_key=TSX::ABC;exchange=TSX;ticker=ABC;asset_type=Stock;"
        "field=missing_isin_primary;target_field=isin"
    )
    assert (
        classification_context_for(row)
        == "gap_class=official_identifier_source_gap;review_needed=true;"
        "confidence_policy_present=true;name_present=true"
    )
    assert (
        evidence_gate_context_for(row)
        == "recommended_next_source_present=true;source_gate_present=true;"
        "target_field=isin;gap_class=official_identifier_source_gap"
    )

from __future__ import annotations

import csv

from scripts.build_otc_name_mismatch_review import (
    build_review_rows,
    classify_name_mismatch,
    summarize,
    write_csv,
    write_markdown,
)


def entry_row(
    ticker: str,
    name: str,
    isin: str,
    *,
    asset_type: str = "Stock",
) -> dict[str, str]:
    return {
        "listing_key": f"OTC::{ticker}",
        "ticker": ticker,
        "exchange": "OTC",
        "asset_type": asset_type,
        "name": name,
        "country": "United States",
        "country_code": "US",
        "isin": isin,
        "instrument_scope": "extended",
        "scope_reason": "otc_listing",
        "primary_listing_key": f"OTC::{ticker}",
        "venue_status": "official_full",
        "evidence_level": "official_reference",
        "evidence_sources": "official:OTC Markets:otc_markets_stock_screener",
        "quality_status": "warn",
        "quality_score": "65",
        "issue_count": "1",
        "issue_types": "official_name_mismatch",
        "issues": "[]",
        "recommended_action": "review_or_backfill",
    }


def official_ref(ticker: str, name: str) -> dict[str, str]:
    return {
        "source_key": "otc_markets_stock_screener",
        "provider": "OTC Markets",
        "source_url": "https://example.test",
        "ticker": ticker,
        "name": name,
        "exchange": "OTC",
        "asset_type": "Stock",
        "listing_status": "active",
        "reference_scope": "exchange_directory",
        "official": "true",
        "isin": "",
        "sector": "",
    }


def test_classify_name_mismatch_separates_rename_and_stale_without_isin():
    assert classify_name_mismatch("CurrentC Power Corporation", "ACADIA ENERGY CORP", "US92855W2017") == (
        "probable_otc_rename_or_symbol_reuse",
        "high",
        "verify_current_issuer_with_isin_source_before_name_update",
    )
    assert classify_name_mismatch("Old Issuer Inc", "NEW ISSUER CORP", "") == (
        "stale_or_symbol_reuse_without_isin",
        "critical",
        "verify_or_quarantine_before_trusting_listing",
    )


def test_classify_name_mismatch_flags_abbreviation_review():
    assert classify_name_mismatch("AirIQ Inc", "AIRLQ INC NEW", "CA0091204036") == (
        "weak_abbreviation_or_truncation_review",
        "medium",
        "review_before_metadata_update",
    )


def test_build_review_rows_filters_to_otc_official_name_warns():
    rows = build_review_rows(
        [
            entry_row("AECX", "CurrentC Power Corporation", "US92855W2017"),
            {**entry_row("MSFT", "Microsoft Corporation", "US5949181045"), "exchange": "NASDAQ", "listing_key": "NASDAQ::MSFT"},
            {**entry_row("PASS", "Pass Corp", ""), "quality_status": "pass"},
        ],
        [
            official_ref("AECX", "ACADIA ENERGY CORP"),
            official_ref("PASS", "PASS CORP"),
        ],
    )

    assert len(rows) == 1
    assert rows[0].listing_key == "OTC::AECX"
    assert rows[0].official_name == "ACADIA ENERGY CORP"
    assert rows[0].review_class == "probable_otc_rename_or_symbol_reuse"


def test_otc_review_writes_csv_and_markdown(tmp_path):
    rows = build_review_rows(
        [entry_row("AECX", "CurrentC Power Corporation", "US92855W2017")],
        [official_ref("AECX", "ACADIA ENERGY CORP")],
    )
    csv_path = tmp_path / "otc.csv"
    md_path = tmp_path / "otc.md"

    write_csv(csv_path, rows)
    payload = summarize(rows, "2026-04-17T00:00:00Z", csv_path)
    write_markdown(md_path, payload)

    csv_rows = list(csv.DictReader(csv_path.open()))
    assert csv_rows[0]["review_class"] == "probable_otc_rename_or_symbol_reuse"
    assert "OTC Name Mismatch Review" in md_path.read_text()

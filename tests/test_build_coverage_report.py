from __future__ import annotations

import os
from datetime import datetime, timezone

from scripts.build_coverage_report import (
    build_b3_gap_breakdown,
    build_b3_masterfile_diagnostics,
    build_country_report,
    build_exchange_reference_catalog,
    build_exchange_report,
    build_freshness_report,
    build_gap_report,
    build_global_summary,
    build_masterfile_collision_report,
    build_source_freshness_summary,
    build_source_report,
    freshness_review_context_for,
    freshness_review_signal_rows,
    load_verification_report,
    render_markdown,
    refresh_gate_context_for,
    source_artifact_context_for,
)


def test_build_exchange_reference_catalog_classifies_venue_statuses():
    masterfiles = [
        {"exchange": "NASDAQ", "source_key": "nasdaq_listed", "official": "true", "reference_scope": "exchange_directory"},
        {"exchange": "TSX", "source_key": "tmx_interlisted", "official": "true", "reference_scope": "interlisted_subset"},
        {"exchange": "OTC", "source_key": "manual:otc", "official": "false", "reference_scope": "manual"},
    ]

    catalog = build_exchange_reference_catalog(masterfiles)

    assert catalog == {
        "NASDAQ": {
            "exchange": "NASDAQ",
            "venue_status": "official_full",
            "official_source_count": 1,
            "manual_source_count": 0,
            "reference_scopes": ["exchange_directory"],
        },
        "OTC": {
            "exchange": "OTC",
            "venue_status": "manual_only",
            "official_source_count": 0,
            "manual_source_count": 1,
            "reference_scopes": [],
        },
        "TSX": {
            "exchange": "TSX",
            "venue_status": "official_partial",
            "official_source_count": 1,
            "manual_source_count": 0,
            "reference_scopes": ["interlisted_subset"],
        },
    }


def test_build_source_report_adds_age_and_freshness_status(monkeypatch):
    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 4, 8, 12, 0, tzinfo=timezone.utc)

    monkeypatch.setattr("scripts.build_coverage_report.datetime", FixedDateTime)
    rows = build_source_report(
        [
            {
                "key": "nasdaq_listed",
                "provider": "Nasdaq Trader",
                "reference_scope": "exchange_directory",
                "official": True,
            },
            {
                "key": "old_source",
                "provider": "Old Exchange",
                "reference_scope": "exchange_directory",
                "official": True,
            },
            {
                "key": "stale_subset",
                "provider": "Subset Exchange",
                "reference_scope": "listed_companies_subset",
                "official": True,
            },
            {
                "key": "unavailable_full",
                "provider": "Unavailable Exchange",
                "reference_scope": "exchange_directory",
                "official": True,
            },
        ],
        {
            "generated_at": "2026-04-08T00:00:00Z",
            "source_details": {
                "nasdaq_listed": {
                    "mode": "network",
                    "rows": 10,
                    "generated_at": "2026-04-08T00:00:00Z",
                },
                "old_source": {
                    "mode": "cache",
                    "rows": 5,
                    "generated_at": "2026-03-30T00:00:00Z",
                },
                "stale_subset": {
                    "mode": "network",
                    "rows": 7,
                    "generated_at": "2026-04-03T12:00:00Z",
                },
                "unavailable_full": {
                    "mode": "unavailable",
                    "rows": 0,
                    "generated_at": "2026-04-08T00:00:00Z",
                    "last_error": "official endpoint returned 403; partial caches are ignored",
                },
            },
        },
    )

    assert rows[0]["age_hours"] == 12.0
    assert rows[0]["age_bucket"] == "age_0_48h"
    assert rows[0]["freshness_status"] == "fresh"
    assert rows[0]["refresh_priority"] == "P4"
    assert rows[0]["refresh_queue"] == "fresh_no_refresh_needed"
    assert rows[0]["review_strategy"] == "no_refresh_required"
    assert rows[0]["evidence_required"] == "fresh_source_generated_at_with_age_under_48h"
    assert rows[0]["recommended_refresh_action"] == "no_refresh_needed"
    assert rows[0]["recommended_next_source"] == (
        "No refresh needed; retain current fresh source evidence for scope exchange_directory."
    )
    assert rows[0]["source_gate"] == "Freshness evidence is present; no data change is authorized by freshness alone."
    assert rows[0]["source_artifact_context"] == source_artifact_context_for(rows[0])
    assert rows[0]["freshness_review_context"] == freshness_review_context_for(rows[0])
    assert rows[0]["refresh_gate_context"] == refresh_gate_context_for(rows[0])
    assert rows[1]["freshness_status"] == "old"
    assert rows[1]["age_bucket"] == "age_168_336h"
    assert rows[1]["refresh_priority"] == "P1"
    assert rows[1]["refresh_queue"] == "refresh_official_exchange_directory_before_identity_or_collision_work"
    assert rows[1]["review_strategy"] == "refresh_official_exchange_directory_before_identity_or_collision_work"
    assert rows[1]["evidence_required"] == "official_exchange_directory_refresh_artifact_with_generated_at_and_row_count"
    assert rows[1]["recommended_refresh_action"] == "refresh_official_exchange_directory_before_identity_or_collision_work"
    assert rows[1]["recommended_next_source"] == (
        "Refresh the official exchange-directory source for scope exchange_directory using mode cache."
    )
    assert rows[1]["source_gate"] == (
        "Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated."
    )
    assert rows[2]["freshness_status"] == "stale"
    assert rows[2]["age_bucket"] == "age_48_168h"
    assert rows[2]["refresh_priority"] == "P2"
    assert rows[2]["refresh_queue"] == "refresh_official_subset_before_gap_enrichment"
    assert rows[2]["review_strategy"] == "refresh_official_subset_before_gap_enrichment"
    assert rows[2]["evidence_required"] == "official_subset_refresh_artifact_with_generated_at_scope_and_row_count"
    assert rows[2]["recommended_refresh_action"] == "refresh_official_subset_before_gap_enrichment"
    assert rows[2]["recommended_next_source"] == (
        "Refresh the official subset source for scope listed_companies_subset before identifier or metadata gap work."
    )
    assert rows[2]["source_gate"] == (
        "Do not fill identifiers, sectors, or categories from stale subset data until a fresh scoped artifact exists."
    )
    assert rows[3]["freshness_status"] == "fresh"
    assert rows[3]["last_error"] == "official endpoint returned 403; partial caches are ignored"
    assert rows[3]["refresh_priority"] == "P1"
    assert rows[3]["refresh_queue"] == "restore_or_replace_unavailable_source_before_data_fill"
    assert rows[3]["review_strategy"] == "restore_or_replace_unavailable_source_before_data_fill"
    assert rows[3]["evidence_required"] == "source_restored_or_replaced_with_official_or_documented_unavailable_decision"
    assert rows[3]["recommended_refresh_action"] == "restore_or_replace_unavailable_source_before_data_fill"
    assert rows[3]["recommended_next_source"] == (
        "Restore the unavailable official source for scope exchange_directory, or document an official replacement/unavailable decision."
    )
    assert rows[3]["source_gate"] == (
        "Keep fields blank until the official source is restored or a documented official replacement/unavailable decision exists."
    )


def test_source_freshness_contexts_are_derived_from_row_fields():
    row = {
        "key": "nasdaq_listed",
        "provider": "Nasdaq Trader",
        "reference_scope": "exchange_directory",
        "official": True,
        "mode": "network",
        "rows": 5471,
        "generated_at": "2026-05-24T15:53:44Z",
        "age_bucket": "age_0_48h",
        "freshness_status": "fresh",
        "refresh_priority": "P4",
        "refresh_queue": "fresh_no_refresh_needed",
        "recommended_refresh_action": "no_refresh_needed",
        "review_strategy": "no_refresh_required",
        "evidence_required": "fresh_source_generated_at_with_age_under_48h",
    }

    assert (
        source_artifact_context_for(row)
        == "key=nasdaq_listed;provider=Nasdaq Trader;reference_scope=exchange_directory;"
        "official=true;mode=network;rows=5471;last_error=none"
    )
    assert (
        freshness_review_context_for(row)
        == "generated_at=2026-05-24T15:53:44Z;age_bucket=age_0_48h;"
        "freshness_status=fresh;refresh_priority=P4"
    )
    assert (
        refresh_gate_context_for(row)
        == "refresh_queue=fresh_no_refresh_needed;recommended_refresh_action=no_refresh_needed;"
        "review_strategy=no_refresh_required;evidence_required=fresh_source_generated_at_with_age_under_48h"
    )


def test_freshness_review_signal_rows_surface_symbol_change_age_without_apply_authority():
    rows = freshness_review_signal_rows(
        {
            "tickers_built_at": "2026-04-06T00:00:00Z",
            "tickers_age_hours": 12.0,
            "symbol_changes_generated_at": "2026-04-06T10:00:00Z",
            "symbol_changes_age_hours": 2.0,
            "symbol_changes_review_rows": 7,
        }
    )

    assert rows == [
        {
            "signal": "Dataset build",
            "generated_at": "2026-04-06T00:00:00Z",
            "age_hours": 12.0,
            "rows": "",
            "source_gate": "dataset_age_visibility_no_data_change_authorized",
        },
        {
            "signal": "Symbol changes",
            "generated_at": "2026-04-06T10:00:00Z",
            "age_hours": 2.0,
            "rows": 7,
            "source_gate": "symbol_change_age_visibility_no_symbol_change_authorized",
        },
    ]


def test_build_source_freshness_summary_prioritizes_old_official_exchange_directories():
    summary = build_source_freshness_summary(
        [
            {
                "key": "old_full",
                "provider": "Old Full",
                "reference_scope": "exchange_directory",
                "official": True,
                "mode": "cache",
                "rows": 50,
                "age_hours": 200.0,
                "age_bucket": "age_168_336h",
                "freshness_status": "old",
                "refresh_priority": "P1",
                "refresh_queue": "refresh_official_exchange_directory_before_identity_or_collision_work",
                "recommended_refresh_action": "refresh_official_exchange_directory_before_identity_or_collision_work",
            },
            {
                "key": "fresh_full",
                "provider": "Fresh Full",
                "reference_scope": "exchange_directory",
                "official": True,
                "mode": "network",
                "rows": 10,
                "age_hours": 2.0,
                "age_bucket": "age_0_48h",
                "freshness_status": "fresh",
                "refresh_priority": "P4",
                "refresh_queue": "fresh_no_refresh_needed",
                "recommended_refresh_action": "no_refresh_needed",
            },
        ]
    )

    assert summary["source_count"] == 2
    assert summary["freshness_status_totals"] == {"fresh": 1, "old": 1}
    assert summary["source_age_bucket_totals"] == {"age_0_48h": 1, "age_168_336h": 1}
    assert summary["refresh_priority_totals"] == {"P1": 1, "P4": 1}
    assert summary["refresh_queue_totals"] == {
        "fresh_no_refresh_needed": 1,
        "refresh_official_exchange_directory_before_identity_or_collision_work": 1,
    }
    assert summary["refresh_queue_scope_totals"] == {
        "fresh_no_refresh_needed": {"exchange_directory": 1},
        "refresh_official_exchange_directory_before_identity_or_collision_work": {"exchange_directory": 1},
    }
    assert summary["refresh_queue_mode_totals"] == {
        "fresh_no_refresh_needed": {"network": 1},
        "refresh_official_exchange_directory_before_identity_or_collision_work": {"cache": 1},
    }
    assert summary["refresh_queue_priority_totals"] == {
        "fresh_no_refresh_needed": {"P4": 1},
        "refresh_official_exchange_directory_before_identity_or_collision_work": {"P1": 1},
    }
    assert summary["refresh_queue_age_bucket_totals"] == {
        "fresh_no_refresh_needed": {"age_0_48h": 1},
        "refresh_official_exchange_directory_before_identity_or_collision_work": {"age_168_336h": 1},
    }
    assert summary["recommended_refresh_action_totals"] == {
        "no_refresh_needed": 1,
        "refresh_official_exchange_directory_before_identity_or_collision_work": 1,
    }
    assert summary["refresh_queue_review_strategy_totals"] == {
        "fresh_no_refresh_needed": {"no_refresh_required": 1},
        "refresh_official_exchange_directory_before_identity_or_collision_work": {
            "refresh_official_exchange_directory_before_identity_or_collision_work": 1,
        },
    }
    assert summary["refresh_queue_evidence_required_totals"] == {
        "fresh_no_refresh_needed": {"fresh_source_generated_at_with_age_under_48h": 1},
        "refresh_official_exchange_directory_before_identity_or_collision_work": {
            "official_exchange_directory_refresh_artifact_with_generated_at_and_row_count": 1,
        },
    }
    assert summary["top_source_refresh_batches"] == [
        {
            "refresh_queue": "refresh_official_exchange_directory_before_identity_or_collision_work",
            "reference_scope": "exchange_directory",
            "mode": "cache",
            "refresh_priority": "P1",
            "source_count": 1,
            "total_rows": 50,
            "max_age_hours": 200.0,
            "review_strategy": "refresh_official_exchange_directory_before_identity_or_collision_work",
            "evidence_required": "official_exchange_directory_refresh_artifact_with_generated_at_and_row_count",
            "recommended_next_source": (
                "Refresh the official exchange-directory source for scope exchange_directory using mode cache."
            ),
            "source_gate": (
                "Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated."
            ),
        },
        {
            "refresh_queue": "fresh_no_refresh_needed",
            "reference_scope": "exchange_directory",
            "mode": "network",
            "refresh_priority": "P4",
            "source_count": 1,
            "total_rows": 10,
            "max_age_hours": 2.0,
            "review_strategy": "no_refresh_required",
            "evidence_required": "fresh_source_generated_at_with_age_under_48h",
            "recommended_next_source": (
                "No refresh needed; retain current fresh source evidence for scope exchange_directory."
            ),
            "source_gate": "Freshness evidence is present; no data change is authorized by freshness alone.",
        },
    ]
    assert summary["old_official_exchange_directory_count"] == 1
    assert summary["top_old_official_exchange_directories"] == [
        {
            "key": "old_full",
            "provider": "Old Full",
            "mode": "cache",
            "rows": 50,
            "age_hours": 200.0,
        }
    ]


def test_build_exchange_report_includes_masterfile_and_verification_rates():
    tickers = [
        {"ticker": "AAPL", "exchange": "NASDAQ", "isin": "US0378331005", "sector": "Information Technology"},
        {"ticker": "IBM", "exchange": "NYSE", "isin": "", "sector": "Information Technology"},
    ]
    identifiers = [
        {"ticker": "AAPL", "exchange": "NASDAQ", "cik": "0000320193", "figi": "", "lei": ""},
        {"ticker": "IBM", "exchange": "NYSE", "cik": "0000051143", "figi": "BBG000BLNNH6", "lei": ""},
    ]
    masterfiles = [
        {"ticker": "AAPL", "exchange": "NASDAQ", "listing_status": "active", "reference_scope": "exchange_directory", "official": "true", "source_key": "nasdaq_listed"},
        {"ticker": "QQQ", "exchange": "NASDAQ", "listing_status": "active", "reference_scope": "exchange_directory", "official": "true", "source_key": "nasdaq_listed"},
        {"ticker": "IBM", "exchange": "NYSE", "listing_status": "active", "reference_scope": "exchange_directory", "official": "true", "source_key": "other"},
        {"ticker": "1301", "exchange": "TSE", "listing_status": "active", "reference_scope": "exchange_directory", "official": "true", "source_key": "jpx"},
    ]
    verification_rows = [
        {
            "exchange": "NASDAQ",
            "items": 10,
            "verified": 8,
            "reference_gap": 2,
            "missing_from_official": 0,
            "name_mismatch": 0,
            "cross_exchange_collision": 0,
            "officially_covered_items": 8,
        }
    ]
    etf_verification_rows = [
        {
            "exchange": "NASDAQ",
            "items": 3,
            "verified": 2,
            "reference_gap": 1,
            "missing_from_official": 0,
            "name_mismatch": 0,
            "cross_exchange_collision": 0,
            "officially_covered_items": 2,
        }
    ]

    rows = build_exchange_report(
        tickers,
        identifiers,
        masterfiles,
        stock_verification_exchange_rows=verification_rows,
        etf_verification_exchange_rows=etf_verification_rows,
    )

    assert rows == [
        {
            "exchange": "NASDAQ",
            "venue_status": "official_full",
            "official_source_count": 1,
            "manual_source_count": 0,
            "reference_scopes": ["exchange_directory"],
            "tickers": 1,
            "isin_coverage": 1,
            "sector_coverage": 1,
            "cik_coverage": 1,
            "figi_coverage": 0,
            "lei_coverage": 0,
            "masterfile_symbols": 2,
            "masterfile_matches": 1,
            "masterfile_collisions": 0,
            "masterfile_missing": 1,
            "masterfile_match_rate": 50.0,
            "masterfile_collision_rate": 0.0,
            "verification_items": 10,
            "verification_verified": 8,
            "verification_reference_gap": 2,
            "verification_missing_from_official": 0,
            "verification_name_mismatch": 0,
            "verification_cross_exchange_collision": 0,
            "verification_verified_rate_on_covered": 100.0,
            "stock_verification_items": 10,
            "stock_verification_verified": 8,
            "stock_verification_reference_gap": 2,
            "stock_verification_missing_from_official": 0,
            "stock_verification_name_mismatch": 0,
            "stock_verification_cross_exchange_collision": 0,
            "stock_verification_verified_rate_on_covered": 100.0,
            "etf_verification_items": 3,
            "etf_verification_verified": 2,
            "etf_verification_reference_gap": 1,
            "etf_verification_missing_from_official": 0,
            "etf_verification_name_mismatch": 0,
            "etf_verification_cross_exchange_collision": 0,
            "etf_verification_verified_rate_on_covered": 100.0,
            "unresolved_count": 3,
        },
        {
            "exchange": "NYSE",
            "venue_status": "official_full",
            "official_source_count": 1,
            "manual_source_count": 0,
            "reference_scopes": ["exchange_directory"],
            "tickers": 1,
            "isin_coverage": 0,
            "sector_coverage": 1,
            "cik_coverage": 1,
            "figi_coverage": 1,
            "lei_coverage": 0,
            "masterfile_symbols": 1,
            "masterfile_matches": 1,
            "masterfile_collisions": 0,
            "masterfile_missing": 0,
            "masterfile_match_rate": 100.0,
            "masterfile_collision_rate": 0.0,
            "verification_items": 0,
            "verification_verified": 0,
            "verification_reference_gap": 0,
            "verification_missing_from_official": 0,
            "verification_name_mismatch": 0,
            "verification_cross_exchange_collision": 0,
            "verification_verified_rate_on_covered": None,
            "stock_verification_items": 0,
            "stock_verification_verified": 0,
            "stock_verification_reference_gap": 0,
            "stock_verification_missing_from_official": 0,
            "stock_verification_name_mismatch": 0,
            "stock_verification_cross_exchange_collision": 0,
            "stock_verification_verified_rate_on_covered": None,
            "etf_verification_items": 0,
            "etf_verification_verified": 0,
            "etf_verification_reference_gap": 0,
            "etf_verification_missing_from_official": 0,
            "etf_verification_name_mismatch": 0,
            "etf_verification_cross_exchange_collision": 0,
            "etf_verification_verified_rate_on_covered": None,
            "unresolved_count": 0,
        },
        {
            "exchange": "TSE",
            "venue_status": "official_full",
            "official_source_count": 1,
            "manual_source_count": 0,
            "reference_scopes": ["exchange_directory"],
            "tickers": 0,
            "isin_coverage": 0,
            "sector_coverage": 0,
            "cik_coverage": 0,
            "figi_coverage": 0,
            "lei_coverage": 0,
            "masterfile_symbols": 1,
            "masterfile_matches": 0,
            "masterfile_collisions": 0,
            "masterfile_missing": 1,
            "masterfile_match_rate": 0.0,
            "masterfile_collision_rate": 0.0,
            "verification_items": 0,
            "verification_verified": 0,
            "verification_reference_gap": 0,
            "verification_missing_from_official": 0,
            "verification_name_mismatch": 0,
            "verification_cross_exchange_collision": 0,
            "verification_verified_rate_on_covered": None,
            "stock_verification_items": 0,
            "stock_verification_verified": 0,
            "stock_verification_reference_gap": 0,
            "stock_verification_missing_from_official": 0,
            "stock_verification_name_mismatch": 0,
            "stock_verification_cross_exchange_collision": 0,
            "stock_verification_verified_rate_on_covered": None,
            "etf_verification_items": 0,
            "etf_verification_verified": 0,
            "etf_verification_reference_gap": 0,
            "etf_verification_missing_from_official": 0,
            "etf_verification_name_mismatch": 0,
            "etf_verification_cross_exchange_collision": 0,
            "etf_verification_verified_rate_on_covered": None,
            "unresolved_count": 0,
        },
    ]


def test_build_country_report_summarizes_identifier_coverage():
    tickers = [
        {"ticker": "AAPL", "exchange": "NASDAQ", "country": "United States", "isin": "US0378331005", "sector": "Information Technology"},
        {"ticker": "SHOP", "exchange": "TSX", "country": "Canada", "isin": "", "sector": ""},
    ]
    identifiers = [
        {"ticker": "AAPL", "exchange": "NASDAQ", "cik": "0000320193", "figi": "BBG000B9XRY4", "lei": ""},
        {"ticker": "SHOP", "exchange": "TSX", "cik": "", "figi": "", "lei": ""},
    ]

    rows = build_country_report(tickers, identifiers)

    assert rows == [
        {
            "country": "Canada",
            "tickers": 1,
            "isin_coverage": 0,
            "sector_coverage": 0,
            "cik_coverage": 0,
            "figi_coverage": 0,
            "lei_coverage": 0,
        },
        {
            "country": "United States",
            "tickers": 1,
            "isin_coverage": 1,
            "sector_coverage": 1,
            "cik_coverage": 1,
            "figi_coverage": 1,
            "lei_coverage": 0,
        },
    ]


def test_global_summary_markdown_and_gaps_include_new_sections():
    exchange_coverage = [
        {
            "exchange": "NYSE",
            "venue_status": "official_full",
            "masterfile_symbols": 5,
            "masterfile_matches": 4,
            "masterfile_collisions": 1,
            "masterfile_missing": 0,
        }
    ]
    report = {
        "_meta": {
            "generated_at": "2026-04-06T01:00:00Z",
            "source_files": {
                "tickers_csv": "data/tickers.csv",
                "listings_csv": "data/listings.csv",
            },
            "policy": "Coverage and freshness report only. It does not authorize inferred identifiers, sectors, categories, names, or symbol changes.",
        },
        "global": build_global_summary(
            tickers=[{"ticker": "AAA", "exchange": "NYSE", "asset_type": "Stock", "isin": "X", "sector": "Y"}],
            listings=[{"ticker": "AAA", "exchange": "NYSE", "asset_type": "Stock", "isin": "X", "sector": "Y"}],
            aliases=[{"ticker": "AAA", "alias": "alpha"}],
            instrument_scopes=[
                {"listing_key": "NYSE::AAA", "instrument_scope": "core", "scope_reason": "primary_listing"},
                {"listing_key": "NYSE::BBB", "instrument_scope": "core", "scope_reason": "primary_listing_missing_isin"},
            ],
            identifiers_extended=[{"ticker": "AAA", "exchange": "NYSE", "cik": "1", "figi": "", "lei": ""}],
            listing_status_history=[{"ticker": "AAA"}],
            listing_events=[{"ticker": "AAA"}],
            exchange_coverage=exchange_coverage,
            stock_verification_summary={"items": 1, "status_counts": {"verified": 1}},
            etf_verification_summary={"items": 2, "status_counts": {"verified": 1, "reference_gap": 1}},
        ),
        "freshness": {
            "tickers_built_at": "2026-04-06T00:00:00Z",
            "tickers_age_hours": 1.0,
            "symbol_changes_generated_at": "2026-04-06T00:30:00Z",
            "symbol_changes_age_hours": 0.5,
            "symbol_changes_review_rows": 3,
        },
        "source_coverage": [
            {
                "key": "nasdaq_listed",
                "provider": "Nasdaq Trader",
                "reference_scope": "exchange_directory",
                "mode": "network",
                "rows": 10,
                "generated_at": "2026-04-06T00:00:00Z",
                "age_hours": 1.0,
                "freshness_status": "fresh",
                "refresh_priority": "P4",
                "recommended_refresh_action": "no_refresh_needed",
            }
        ],
        "source_freshness_summary": {"refresh_priority_totals": {"P4": 1}},
        "exchange_coverage": [
            {
                "exchange": "NYSE",
                "venue_status": "official_full",
                "tickers": 1,
                "isin_coverage": 1,
                "sector_coverage": 1,
                "cik_coverage": 1,
                "figi_coverage": 0,
                "lei_coverage": 0,
                "masterfile_symbols": 5,
                "masterfile_matches": 4,
                "masterfile_collisions": 1,
                "masterfile_missing": 0,
                "masterfile_match_rate": 80.0,
                "masterfile_collision_rate": 20.0,
                "verification_verified_rate_on_covered": 100.0,
            }
        ],
        "country_coverage": [],
        "gap_report": [
            {
                "exchange": "NYSE",
                "venue_status": "official_full",
                "unresolved_findings": 2,
                "reference_gap": 0,
                "missing_from_official": 1,
                "name_mismatch": 1,
                "cross_exchange_collision": 0,
            }
        ],
    }

    markdown = render_markdown(report)

    assert report["global"]["tickers"] == 1
    assert report["global"]["listing_keys"] == 1
    assert report["_meta"]["generated_at"]
    assert report["_meta"]["source_files"]["tickers_csv"] == "data/tickers.csv"
    assert report["_meta"]["policy"].startswith("Coverage and freshness report only.")
    assert report["global"]["instrument_scope_core"] == 2
    assert report["global"]["instrument_scope_primary_listing_missing_isin"] == 1
    assert report["global"]["official_full_exchanges"] == 1
    assert report["global"]["etf_verification_items"] == 2
    assert "# Coverage Report" in markdown
    assert "## Freshness" in markdown
    assert "## Freshness Review Summary" in markdown
    assert "Symbol changes | 2026-04-06T00:30:00Z | 0.5 | 3" in markdown
    assert "symbol_change_age_visibility_no_symbol_change_authorized" in markdown
    assert "### Source Freshness Totals" in markdown
    assert "## Source Coverage" in markdown
    assert "## Unresolved Gaps" in markdown


def test_build_masterfile_collision_report_exposes_cross_exchange_conflicts():
    tickers = [
        {"ticker": "1301", "exchange": "TWSE"},
        {"ticker": "AAPL", "exchange": "NASDAQ"},
    ]
    masterfiles = [
        {"ticker": "1301", "name": "Kyokuyo", "exchange": "TSE", "listing_status": "active", "reference_scope": "exchange_directory"},
        {"ticker": "1306", "name": "ETF", "exchange": "TSE", "listing_status": "active", "reference_scope": "exchange_directory"},
        {"ticker": "AAPL", "name": "Apple Inc.", "exchange": "NASDAQ", "listing_status": "active", "reference_scope": "exchange_directory"},
    ]

    report = build_masterfile_collision_report(tickers, masterfiles)

    assert report["global"] == {
        "official_symbols": 3,
        "matched": 1,
        "collisions": 1,
        "missing": 1,
    }
    assert report["exchanges"][1]["collision_examples"][0]["ticker"] == "1301"
    assert report["exchanges"][1]["missing_examples"][0]["ticker"] == "1306"


def test_build_freshness_report_calculates_ages(tmp_path, monkeypatch):
    identifiers_csv = tmp_path / "identifiers.csv"
    identifiers_extended_csv = tmp_path / "identifiers_extended.csv"
    identifier_summary_json = tmp_path / "identifier_summary.json"
    symbol_changes_review_json = tmp_path / "symbol_changes_review.json"
    for path in (identifiers_csv, identifiers_extended_csv, identifier_summary_json, symbol_changes_review_json):
        path.write_text("", encoding="utf-8")

    older = datetime(2026, 4, 6, 10, 30, tzinfo=timezone.utc).timestamp()
    for path in (identifiers_csv, identifiers_extended_csv, identifier_summary_json, symbol_changes_review_json):
        os.utime(path, (older, older))
    newer_symbol_review_mtime = datetime(2026, 4, 7, 10, 30, tzinfo=timezone.utc).timestamp()
    os.utime(symbol_changes_review_json, (newer_symbol_review_mtime, newer_symbol_review_mtime))

    monkeypatch.setattr("scripts.build_coverage_report.IDENTIFIERS_CSV", identifiers_csv)
    monkeypatch.setattr("scripts.build_coverage_report.IDENTIFIERS_EXTENDED_CSV", identifiers_extended_csv)
    monkeypatch.setattr("scripts.build_coverage_report.IDENTIFIER_SUMMARY_JSON", identifier_summary_json)
    monkeypatch.setattr("scripts.build_coverage_report.SYMBOL_CHANGES_REVIEW_JSON", symbol_changes_review_json)

    freshness = build_freshness_report(
        {"generated_at": "2026-04-06T10:00:00Z"},
        {"generated_at": "2026-04-06T11:00:00Z"},
        {"observed_at": "2026-04-06T12:00:00Z"},
        {"run_dir": "data/stock_verification/run-a", "generated_at": "2026-04-06T13:00:00Z"},
        {"run_dir": "data/etf_verification/run-b", "generated_at": "2026-04-06T14:00:00Z"},
        {
            "_meta": {"generated_at": "2026-04-06T15:00:00Z"},
            "summary": {"review_rows": 12},
        },
        {
            "source_gap_classification": (
                {"summary": {"generated_at": "2026-04-06T16:00:00Z", "rows": 34}},
                tmp_path / "source_gap_classification.json",
            ),
            "ohlcv_plausibility": (
                {"_meta": {"generated_at": "2026-04-06T17:00:00Z", "rows": 56}},
                tmp_path / "ohlcv_plausibility.json",
            ),
        },
    )

    assert freshness["masterfiles_generated_at"] == "2026-04-06T10:00:00Z"
    assert freshness["identifiers_generated_at"] == "2026-04-06T11:00:00Z"
    assert freshness["listing_history_observed_at"] == "2026-04-06T12:00:00Z"
    assert freshness["latest_verification_run"] == "data/stock_verification/run-a"
    assert freshness["latest_etf_verification_run"] == "data/etf_verification/run-b"
    assert freshness["symbol_changes_generated_at"] == "2026-04-06T15:00:00Z"
    assert freshness["symbol_changes_review_rows"] == 12
    assert freshness["source_gap_classification_generated_at"] == "2026-04-06T16:00:00Z"
    assert freshness["source_gap_classification_rows"] == 34
    assert freshness["ohlcv_plausibility_generated_at"] == "2026-04-06T17:00:00Z"
    assert freshness["ohlcv_plausibility_rows"] == 56


def test_build_freshness_report_uses_newer_identifier_artifact_timestamp(tmp_path, monkeypatch):
    identifiers_csv = tmp_path / "identifiers.csv"
    identifiers_extended_csv = tmp_path / "identifiers_extended.csv"
    identifier_summary_json = tmp_path / "identifier_summary.json"
    symbol_changes_review_json = tmp_path / "symbol_changes_review.json"
    for path in (identifiers_csv, identifiers_extended_csv, identifier_summary_json, symbol_changes_review_json):
        path.write_text("", encoding="utf-8")

    newer = datetime(2026, 4, 6, 12, 30, tzinfo=timezone.utc).timestamp()
    os.utime(identifiers_csv, (newer, newer))

    older = datetime(2026, 4, 6, 10, 15, tzinfo=timezone.utc).timestamp()
    for path in (identifiers_extended_csv, identifier_summary_json, symbol_changes_review_json):
        os.utime(path, (older, older))

    monkeypatch.setattr("scripts.build_coverage_report.IDENTIFIERS_CSV", identifiers_csv)
    monkeypatch.setattr("scripts.build_coverage_report.IDENTIFIERS_EXTENDED_CSV", identifiers_extended_csv)
    monkeypatch.setattr("scripts.build_coverage_report.IDENTIFIER_SUMMARY_JSON", identifier_summary_json)
    monkeypatch.setattr("scripts.build_coverage_report.SYMBOL_CHANGES_REVIEW_JSON", symbol_changes_review_json)

    freshness = build_freshness_report(
        {"generated_at": "2026-04-06T10:00:00Z"},
        {"generated_at": "2026-04-06T11:00:00Z"},
        {"observed_at": "2026-04-06T12:00:00Z"},
        {"run_dir": "data/stock_verification/run-a", "generated_at": "2026-04-06T13:00:00Z"},
        {"run_dir": "data/etf_verification/run-b", "generated_at": "2026-04-06T14:00:00Z"},
    )

    assert freshness["identifiers_generated_at"] == "2026-04-06T12:30:00Z"


def test_build_gap_report_and_b3_breakdown():
    exchange_coverage = [
        {
            "exchange": "B3",
            "venue_status": "official_full",
            "masterfile_missing": 10,
            "masterfile_collisions": 0,
        }
    ]
    stock_verification_exchange_rows = [
        {
            "exchange": "B3",
            "reference_gap": 0,
            "missing_from_official": 3,
            "name_mismatch": 1,
            "cross_exchange_collision": 0,
        }
    ]
    etf_verification_exchange_rows = [
        {
            "exchange": "B3",
            "reference_gap": 2,
            "missing_from_official": 0,
            "name_mismatch": 0,
            "cross_exchange_collision": 1,
        }
    ]
    verification_rows = [
        {"exchange": "B3", "ticker": "ABBV34", "name": "AbbVie", "status": "missing_from_official"},
        {"exchange": "B3", "ticker": "ASAI3F", "name": "Sendas", "status": "missing_from_official"},
        {"exchange": "B3", "ticker": "PETR4", "name": "Petrobras", "status": "missing_from_official"},
    ]

    gap_report = build_gap_report(exchange_coverage, stock_verification_exchange_rows, etf_verification_exchange_rows)
    b3_breakdown = build_b3_gap_breakdown(verification_rows)

    assert gap_report == [
        {
            "exchange": "B3",
            "venue_status": "official_full",
            "unresolved_findings": 7,
            "reference_gap": 2,
            "missing_from_official": 3,
            "name_mismatch": 1,
            "cross_exchange_collision": 1,
            "stock_reference_gap": 0,
            "stock_missing_from_official": 3,
            "stock_name_mismatch": 1,
            "stock_cross_exchange_collision": 0,
            "etf_reference_gap": 2,
            "etf_missing_from_official": 0,
            "etf_name_mismatch": 0,
            "etf_cross_exchange_collision": 1,
            "masterfile_missing": 10,
            "masterfile_collisions": 0,
        }
    ]
    assert b3_breakdown["categories"] == {
        "bdr_or_foreign_receipt": 1,
        "fractional_line": 1,
        "local_share_line": 1,
    }


def test_build_b3_masterfile_diagnostics_compares_dataset_against_active_official_directory():
    diagnostics = build_b3_masterfile_diagnostics(
        [
            {"listing_key": "B3::PETR4", "ticker": "PETR4", "exchange": "B3", "asset_type": "Stock", "name": "Petrobras"},
            {"listing_key": "B3::AFOF11", "ticker": "AFOF11", "exchange": "B3", "asset_type": "ETF", "name": "Alianza FOFII"},
            {"listing_key": "B3::ABBV34", "ticker": "ABBV34", "exchange": "B3", "asset_type": "Stock", "name": "AbbVie BDR"},
            {"listing_key": "NYSE::PETR", "ticker": "PETR", "exchange": "NYSE", "asset_type": "Stock", "name": "Petrobras ADR"},
        ],
        [
            {
                "source_key": "b3_instruments_equities",
                "ticker": "PETR4",
                "exchange": "B3",
                "asset_type": "Stock",
                "listing_status": "active",
                "reference_scope": "exchange_directory",
                "official": "true",
            },
            {
                "source_key": "b3_listed_etfs",
                "ticker": "AFOF11",
                "exchange": "B3",
                "asset_type": "ETF",
                "listing_status": "active",
                "reference_scope": "listed_companies_subset",
                "official": "true",
            },
            {
                "source_key": "b3_instruments_equities",
                "ticker": "VALE3",
                "exchange": "B3",
                "asset_type": "Stock",
                "listing_status": "active",
                "reference_scope": "exchange_directory",
                "official": "true",
            },
        ],
    )

    assert diagnostics["dataset_rows"] == 3
    assert diagnostics["active_exchange_directory_rows"] == 2
    assert diagnostics["matched_dataset_rows"] == 1
    assert diagnostics["missing_dataset_rows"] == 2
    assert diagnostics["official_any_source_matched_dataset_rows"] == 2
    assert diagnostics["official_any_source_missing_dataset_rows"] == 1
    assert diagnostics["official_any_source_match_rate"] == 66.67
    assert diagnostics["official_active_symbols_not_in_dataset"] == 1
    assert diagnostics["dataset_match_rate"] == 33.33
    assert diagnostics["missing_category_totals"] == {
        "bdr_or_foreign_receipt": 1,
        "unit_or_fund_line": 1,
    }
    assert diagnostics["missing_asset_type_totals"] == {"ETF": 1, "Stock": 1}
    assert diagnostics["missing_source_presence_totals"] == {
        "absent_from_all_b3_masterfile_sources": 1,
        "present_only_in_non_exchange_directory_source": 1,
    }
    assert diagnostics["missing_examples"]["unit_or_fund_line"] == [
        {
            "listing_key": "B3::AFOF11",
            "ticker": "AFOF11",
            "asset_type": "ETF",
            "name": "Alianza FOFII",
            "source_presence": "present_only_in_non_exchange_directory_source",
            "candidate_sources": "b3_listed_etfs",
        }
    ]


def test_load_verification_report_reads_latest_chunk_rows(tmp_path):
    run_dir = tmp_path / "run-1"
    run_dir.mkdir()
    (run_dir / "summary.json").write_text(
        '{"items": 2, "status_counts": {"verified": 1, "reference_gap": 1}}',
        encoding="utf-8",
    )
    (run_dir / "chunk-01-of-01.json").write_text(
        '[{"exchange":"NASDAQ","status":"verified"},{"exchange":"OTC","status":"reference_gap"}]',
        encoding="utf-8",
    )

    report = load_verification_report(run_dir)

    assert report["summary"]["items"] == 2
    assert report["exchange_rows"] == [
        {
            "exchange": "NASDAQ",
            "items": 1,
            "verified": 1,
            "reference_gap": 0,
            "missing_from_official": 0,
            "name_mismatch": 0,
            "cross_exchange_collision": 0,
            "asset_type_mismatch": 0,
            "non_active_official": 0,
            "officially_covered_items": 1,
            "verified_rate_on_covered": 100.0,
        },
        {
            "exchange": "OTC",
            "items": 1,
            "verified": 0,
            "reference_gap": 1,
            "missing_from_official": 0,
            "name_mismatch": 0,
            "cross_exchange_collision": 0,
            "asset_type_mismatch": 0,
            "non_active_official": 0,
            "officially_covered_items": 0,
            "verified_rate_on_covered": None,
        },
    ]


def test_load_verification_report_synthesizes_summary_without_top_level_file(tmp_path):
    run_dir = tmp_path / "run-2"
    run_dir.mkdir()
    (run_dir / "chunk-01-of-02.summary.json").write_text(
        '{"items": 1, "status_counts": {"verified": 1}}',
        encoding="utf-8",
    )
    (run_dir / "chunk-02-of-02.summary.json").write_text(
        '{"items": 1, "status_counts": {"reference_gap": 1}}',
        encoding="utf-8",
    )
    (run_dir / "chunk-01-of-02.json").write_text(
        '[{"exchange":"NASDAQ","status":"verified"}]',
        encoding="utf-8",
    )
    (run_dir / "chunk-02-of-02.json").write_text(
        '[{"exchange":"OTC","status":"reference_gap"}]',
        encoding="utf-8",
    )

    report = load_verification_report(run_dir)

    assert report["summary"] == {
        "items": 2,
        "status_counts": {"reference_gap": 1, "verified": 1},
        "finding_examples": [],
    }
    assert report["generated_at"]

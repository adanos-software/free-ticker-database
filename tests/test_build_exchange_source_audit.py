from __future__ import annotations

from scripts.build_exchange_source_audit import build_exchange_source_audit, summarize


def test_audit_emits_one_row_per_coverage_venue_and_keeps_missing_denominator_explicit():
    coverage = {
        "by_exchange": [
            {
                "exchange": "FULL",
                "venue_status": "official_full",
                "masterfile_symbols": 2,
                "masterfile_matches": 2,
                "masterfile_collisions": 0,
                "masterfile_missing": 0,
                "official_recall_pct": 100.0,
                "collision_adjusted_recall_pct": 100.0,
                "reference_scopes": ["exchange_directory"],
            },
            {
                "exchange": "PARTIAL",
                "venue_status": "official_partial",
                "masterfile_symbols": 0,
                "masterfile_matches": 0,
                "masterfile_collisions": 0,
                "masterfile_missing": 0,
                "official_recall_pct": None,
                "collision_adjusted_recall_pct": None,
                "reference_scopes": ["listed_companies_subset"],
            },
        ],
        "source_coverage": [
            {"key": "full_directory", "mode": "network", "freshness_status": "fresh"},
            {
                "key": "partial_subset",
                "mode": "unavailable",
                "freshness_status": "old",
                "last_error": "Empty refresh result; preserved 10 existing rows",
            },
        ],
    }
    listings = [
        {"exchange": "FULL", "asset_type": "Stock"},
        {"exchange": "FULL", "asset_type": "ETF"},
        {"exchange": "PARTIAL", "asset_type": "Stock"},
        {"exchange": "PARTIAL", "asset_type": "ETF"},
    ]
    references = [
        {
            "exchange": "FULL",
            "source_key": "full_directory",
            "official": "true",
            "listing_status": "active",
            "asset_type": "Stock",
        },
        {
            "exchange": "FULL",
            "source_key": "full_directory",
            "official": "true",
            "listing_status": "active",
            "asset_type": "ETF",
        },
        {
            "exchange": "PARTIAL",
            "source_key": "partial_subset",
            "official": "true",
            "listing_status": "active",
            "asset_type": "Stock",
        },
    ]

    rows = build_exchange_source_audit(coverage, listings, references)

    assert [row["exchange"] for row in rows] == ["FULL", "PARTIAL"]
    assert rows[0]["audit_outcome"] == "maintain"
    assert rows[1]["denominator_status"] == "denominator_missing"
    assert rows[1]["missing_product_classes"] == "ETF"
    assert rows[1]["unavailable_source_keys"] == "partial_subset"
    assert rows[1]["source_blocker_classes"] == "empty_refresh"
    assert rows[1]["audit_outcome"] == "refresh_unavailable"
    assert rows[1]["promotion_readiness"] == "blocked_source_unavailable"
    assert summarize(rows)["venue_status_counts"] == {
        "official_full": 1,
        "official_partial": 1,
    }
    assert summarize(rows)["promotion_ready_venues"] == 0

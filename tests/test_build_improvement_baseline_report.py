from scripts.build_improvement_baseline_report import (
    BASELINE_SOURCE_FILES,
    baseline_contexts,
    campaign_baseline_context,
    build_campaign_baseline,
    exchange_baseline,
    exchange_baseline_context,
    global_baseline_context,
    render_markdown,
    row_count,
    source_freshness_totals,
)


def test_row_count_reads_report_shapes() -> None:
    assert row_count({"summary": {"rows": 3}}) == 3
    assert row_count({"_meta": {"selected_rows": 4}}) == 4
    assert row_count({"rows": [{"listing_key": "A"}, {"listing_key": "B"}]}) == 2
    assert row_count({"review_items": [{"listing_key": "A"}]}) == 1
    assert row_count({}) == 0


def test_source_freshness_totals_counts_missing_as_unknown() -> None:
    totals = source_freshness_totals(
        {
            "source_coverage": [
                {"freshness_status": "fresh"},
                {"freshness_status": "old"},
                {"freshness_status": ""},
                {},
            ]
        }
    )

    assert totals == {"fresh": 1, "old": 1, "unknown": 2}


def test_exchange_baseline_combines_coverage_quality_and_source_gaps() -> None:
    rows = exchange_baseline(
        {
            "exchange_coverage": [
                {
                    "exchange": "B3",
                    "tickers": 10,
                    "isin_coverage": 9,
                    "sector_coverage": 8,
                    "masterfile_matches": 7,
                    "masterfile_collisions": 1,
                    "masterfile_missing": 2,
                }
            ]
        },
        [
            {"exchange": "B3", "quality_status": "warn"},
            {"exchange": "B3", "quality_status": "source_gap"},
            {"exchange": "ASX", "quality_status": "source_gap"},
        ],
        [
            {"exchange": "B3"},
            {"exchange": "B3"},
            {"exchange": "ASX"},
        ],
    )

    assert rows["B3"] == {
        "tickers": 10,
        "isin_coverage": 9,
        "sector_coverage": 8,
        "masterfile_matches": 7,
        "masterfile_collisions": 1,
        "masterfile_missing": 2,
        "source_gap_rows": 2,
        "entry_quality_warn_rows": 1,
        "entry_quality_source_gap_rows": 1,
        "entry_quality_quarantine_rows": 0,
    }
    assert rows["ASX"]["source_gap_rows"] == 1


def test_build_campaign_baseline_tracks_all_campaigns_and_freshness_gates() -> None:
    baseline = build_campaign_baseline(
        {
            "coverage": {
                "source_coverage": [{"freshness_status": "old"}, {"freshness_status": "fresh"}],
                "source_freshness_summary": {
                    "source_count": 2,
                    "freshness_status_totals": {"fresh": 1, "old": 1},
                    "refresh_priority_totals": {"P1": 1},
                    "recommended_refresh_action_totals": {"refresh_official_exchange_directory": 1},
                    "old_official_exchange_directory_count": 1,
                },
                "freshness": {"symbol_changes_review_rows": 3, "ohlcv_plausibility_rows": 4},
            },
            "source_gap": {
                "summary": {
                    "rows": 5,
                    "class_totals": {"official_gap": 5},
                    "top_source_gap_review_batches": [
                        {
                            "field": "missing_sector_stock",
                            "gap_class": "official_gap",
                            "exchange": "B3",
                            "rows": 5,
                            "recommended_next_source": "Official taxonomy source.",
                            "source_gate": "Exact listing-keyed taxonomy evidence.",
                        }
                    ],
                }
            },
            "financialdata": {
                "summary": {
                    "supplement_rows": 6,
                    "apply_eligibility_counts": {"keep_absent_until_name_gated_official_isin_match": 6},
                    "verification_evidence_required_counts": {
                        "official_active_masterfile_or_registry_row_matching_financialdata_name_and_listing": 6
                    },
                }
            },
        }
    )

    assert list(baseline) == [
        "b3",
        "otc",
        "canada",
        "asx",
        "weak_sector",
        "masterfile_collisions",
        "symbol_changes",
        "ohlcv",
        "freshness",
        "baseline",
    ]
    assert baseline["freshness"]["source_count"] == 2
    assert baseline["freshness"]["source_gap_rows"] == 5
    assert baseline["freshness"]["top_source_gap_review_batches"] == [
        {
            "field": "missing_sector_stock",
            "gap_class": "official_gap",
            "exchange": "B3",
            "rows": 5,
            "recommended_next_source": "Official taxonomy source.",
            "source_gate": "Exact listing-keyed taxonomy evidence.",
        }
    ]
    assert baseline["freshness"]["financialdata_supplement_rows"] == 6
    assert baseline["baseline"]["tracked_campaigns"] == 10


def test_baseline_contexts_are_exact_field_summaries() -> None:
    global_values = {
        "tickers": 10,
        "listing_keys": 9,
        "source_gap_rows": 2,
        "entry_quality_warn_rows": 1,
        "entry_quality_quarantine_rows": 0,
        "validation_failed_error_gates": 0,
    }
    campaign_values = {
        "b3": {"missing_isin_residual_rows": 2, "field_totals": {"missing_isin_primary": 2}},
    }
    exchange_values = {
        "B3": {
            "tickers": 10,
            "isin_coverage": 9,
            "sector_coverage": 8,
            "source_gap_rows": 2,
            "entry_quality_warn_rows": 1,
            "entry_quality_source_gap_rows": 0,
            "entry_quality_quarantine_rows": 0,
        }
    }

    assert global_baseline_context(global_values) == (
        "metric_count=6;tickers=10;listing_keys=9;source_gap_rows=2;"
        "warn_rows=1;quarantine_rows=0;validation_failed_error_gates=0"
    )
    assert campaign_baseline_context("b3", campaign_values["b3"]) == (
        "campaign_key=b3;metric_count=2;nested_metric_count=1;numeric_row_total=2"
    )
    assert exchange_baseline_context("B3", exchange_values["B3"]) == (
        "exchange=B3;tickers=10;isin_coverage=9;sector_coverage=8;source_gap_rows=2;"
        "warn_rows=1;quality_source_gap_rows=0;quarantine_rows=0"
    )
    assert baseline_contexts(global_values, campaign_values, exchange_values) == {
        "global": global_baseline_context(global_values),
        "campaigns": {"b3": campaign_baseline_context("b3", campaign_values["b3"])},
        "exchanges": {"B3": exchange_baseline_context("B3", exchange_values["B3"])},
    }


def test_render_markdown_includes_source_file_traceability() -> None:
    markdown = render_markdown(
        {
            "_meta": {"generated_at": "2026-05-24T00:00:00Z", "source_files": BASELINE_SOURCE_FILES},
            "summary": {
                "global_metric_count": 1,
                "campaign_count": 1,
                "exchange_count": 1,
                "source_file_count": len(BASELINE_SOURCE_FILES),
                "baseline_context": "metric_count=1;tickers=1;listing_keys=0;source_gap_rows=0;warn_rows=0;quarantine_rows=0;validation_failed_error_gates=0",
            },
            "global_baseline": {"tickers": 1},
            "campaign_baseline": {
                "freshness": {
                    "tracked_campaigns": 1,
                    "top_source_gap_review_batches": [
                        {
                            "field": "missing_sector_stock",
                            "gap_class": "official_industry_taxonomy_unavailable_gap",
                            "exchange": "B3",
                            "rows": 2,
                            "recommended_next_source": "Official taxonomy source.",
                            "source_gate": "Exact listing-keyed taxonomy evidence.",
                        }
                    ],
                }
            },
            "exchange_baseline": {
                "B3": {
                    "tickers": 1,
                    "isin_coverage": 1,
                    "sector_coverage": 1,
                    "source_gap_rows": 0,
                    "entry_quality_warn_rows": 0,
                    "entry_quality_source_gap_rows": 0,
                    "entry_quality_quarantine_rows": 0,
                }
            },
            "baseline_contexts": {
                "global": "metric_count=1;tickers=1;listing_keys=0;source_gap_rows=0;warn_rows=0;quarantine_rows=0;validation_failed_error_gates=0",
                "campaigns": {
                    "freshness": "campaign_key=freshness;metric_count=2;nested_metric_count=1;numeric_row_total=0"
                },
                "exchanges": {
                    "B3": "exchange=B3;tickers=1;isin_coverage=1;sector_coverage=1;source_gap_rows=0;warn_rows=0;quality_source_gap_rows=0;quarantine_rows=0"
                },
            },
        }
    )

    assert "## Summary" in markdown
    assert "Global context: `metric_count=1;tickers=1" in markdown
    assert "| baseline_context | `campaign_key=freshness" in markdown
    assert "exchange=B3;tickers=1;isin_coverage=1" in markdown
    assert "## Source Files" in markdown
    assert "| `coverage_report` | `data/reports/coverage_report.json` |" in markdown
    assert "| `entry_quality_csv` | `data/reports/entry_quality.csv` |" in markdown
    assert "| top_source_gap_review_batches | `1` ranked batches |" in markdown
    assert "| `missing_sector_stock` | `official_industry_taxonomy_unavailable_gap` | `B3` | 2 | Official taxonomy source. | Exact listing-keyed taxonomy evidence. |" in markdown

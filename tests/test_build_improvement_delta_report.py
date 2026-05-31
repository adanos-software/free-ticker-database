from scripts.build_improvement_delta_report import (
    build_acceptance_delta_matrix,
    build_delta_payload,
    compare_mapping,
    delta_context,
    exchange_acceptance_deltas,
    flatten_numeric_deltas,
    render_markdown,
)


BASELINE_SOURCE_FILES = {
    "coverage_report": "data/reports/coverage_report.json",
    "source_gap_classification_json": "data/reports/source_gap_classification.json",
}


def test_compare_mapping_recurses_and_computes_numeric_delta() -> None:
    compared = compare_mapping(
        {"global": {"isin_coverage": 10, "status": "old"}},
        {"global": {"isin_coverage": 12, "status": "fresh"}},
    )

    assert compared["global"]["children"]["isin_coverage"] == {
        "baseline": 10,
        "current": 12,
        "delta": 2,
    }
    assert compared["global"]["children"]["status"] == {
        "baseline": "old",
        "current": "fresh",
        "delta": None,
    }


def test_flatten_numeric_deltas_returns_nested_numeric_rows() -> None:
    rows = flatten_numeric_deltas(
        {
            "global": {
                "children": {
                    "isin_coverage": {"baseline": 10, "current": 12, "delta": 2},
                    "status": {"baseline": "old", "current": "fresh", "delta": None},
                }
            }
        }
    )

    assert rows == [{"path": "global.isin_coverage", "baseline": 10, "current": 12, "delta": 2}]


def test_build_delta_payload_counts_changed_numeric_rows() -> None:
    payload = build_delta_payload(
        {
            "_meta": {"generated_at": "2026-05-24T00:00:00Z"},
            "global_baseline": {"isin_coverage": 10, "source_gap_rows": 5},
            "campaign_baseline": {"b3": {"missing_isin_residual_rows": 2}},
            "exchange_baseline": {"B3": {"isin_coverage": 5}},
        },
        {
            "_meta": {"generated_at": "2026-05-24T01:00:00Z", "source_files": BASELINE_SOURCE_FILES},
            "global_baseline": {"isin_coverage": 12, "source_gap_rows": 5},
            "campaign_baseline": {"b3": {"missing_isin_residual_rows": 1}},
            "exchange_baseline": {"B3": {"isin_coverage": 6}},
        },
    )

    assert payload["summary"]["numeric_delta_rows"] == 4
    assert payload["summary"]["changed_numeric_delta_rows"] == 3
    assert payload["summary"]["global_changed_numeric_delta_rows"] == 1
    assert payload["summary"]["campaign_changed_numeric_delta_rows"] == 1
    assert payload["summary"]["exchange_changed_numeric_delta_rows"] == 1
    assert payload["summary"]["changed_exchange_rows"] == 1
    assert payload["_meta"]["source_files"] == {
        "baseline_snapshot": "data/reports/improvement_baseline.json",
        "current_snapshot_coverage_report": "data/reports/coverage_report.json",
        "current_snapshot_source_gap_classification_json": "data/reports/source_gap_classification.json",
    }
    assert payload["changed_numeric_deltas"] == [
        {"path": "global_baseline.isin_coverage", "baseline": 10, "current": 12, "delta": 2},
        {"path": "campaign_baseline.b3.missing_isin_residual_rows", "baseline": 2, "current": 1, "delta": -1},
        {"path": "exchange_baseline.B3.isin_coverage", "baseline": 5, "current": 6, "delta": 1},
    ]


def test_build_acceptance_delta_matrix_maps_required_release_metrics() -> None:
    matrix = build_acceptance_delta_matrix(
        {
            "global_baseline": {
                "isin_coverage": 10,
                "stock_sector_coverage": 20,
                "etf_category_coverage": 30,
                "source_gap_rows": 40,
                "entry_quality_warn_rows": 5,
                "entry_quality_quarantine_rows": 1,
            }
        },
        {
            "global_baseline": {
                "isin_coverage": 12,
                "stock_sector_coverage": 21,
                "etf_category_coverage": 30,
                "source_gap_rows": 38,
                "entry_quality_warn_rows": 7,
                "entry_quality_quarantine_rows": 0,
            }
        },
    )

    assert matrix == {
        "isin_delta": {
            "baseline": 10,
            "current": 12,
            "delta": 2,
            "delta_context": "scope=global;metric=isin_delta;baseline=10;current=12;delta=2;direction=improved;review_policy=source_level_review_required_before_claiming_completion",
        },
        "sector_delta": {
            "baseline": 20,
            "current": 21,
            "delta": 1,
            "delta_context": "scope=global;metric=sector_delta;baseline=20;current=21;delta=1;direction=improved;review_policy=source_level_review_required_before_claiming_completion",
        },
        "category_delta": {
            "baseline": 30,
            "current": 30,
            "delta": 0,
            "delta_context": "scope=global;metric=category_delta;baseline=30;current=30;delta=0;direction=unchanged;review_policy=no_data_change_inferred",
        },
        "source_gap_delta": {
            "baseline": 40,
            "current": 38,
            "delta": -2,
            "delta_context": "scope=global;metric=source_gap_delta;baseline=40;current=38;delta=-2;direction=improved;review_policy=source_level_review_required_before_claiming_completion",
        },
        "warn_delta": {
            "baseline": 5,
            "current": 7,
            "delta": 2,
            "delta_context": "scope=global;metric=warn_delta;baseline=5;current=7;delta=2;direction=regressed;review_policy=regression_review_required_before_release",
        },
        "quarantine_delta": {
            "baseline": 1,
            "current": 0,
            "delta": -1,
            "delta_context": "scope=global;metric=quarantine_delta;baseline=1;current=0;delta=-1;direction=improved;review_policy=source_level_review_required_before_claiming_completion",
        },
    }


def test_exchange_acceptance_deltas_maps_required_exchange_metrics() -> None:
    matrix = exchange_acceptance_deltas(
        {
            "exchange_baseline": {
                "B3": {
                    "isin_coverage": 10,
                    "sector_coverage": 20,
                    "source_gap_rows": 3,
                    "entry_quality_warn_rows": 1,
                    "entry_quality_quarantine_rows": 0,
                    "entry_quality_source_gap_rows": 4,
                    "masterfile_collisions": 2,
                    "masterfile_missing": 5,
                }
            }
        },
        {
            "exchange_baseline": {
                "B3": {
                    "isin_coverage": 11,
                    "sector_coverage": 19,
                    "source_gap_rows": 2,
                    "entry_quality_warn_rows": 1,
                    "entry_quality_quarantine_rows": 1,
                    "entry_quality_source_gap_rows": 3,
                    "masterfile_collisions": 0,
                    "masterfile_missing": 4,
                }
            }
        },
    )

    assert matrix["B3"]["isin_delta"] == {
        "baseline": 10,
        "current": 11,
        "delta": 1,
        "delta_context": delta_context("isin_delta", {"baseline": 10, "current": 11, "delta": 1}, "B3"),
    }
    assert matrix["B3"]["sector_delta"] == {
        "baseline": 20,
        "current": 19,
        "delta": -1,
        "delta_context": delta_context("sector_delta", {"baseline": 20, "current": 19, "delta": -1}, "B3"),
    }
    assert matrix["B3"]["source_gap_delta"] == {
        "baseline": 3,
        "current": 2,
        "delta": -1,
        "delta_context": delta_context("source_gap_delta", {"baseline": 3, "current": 2, "delta": -1}, "B3"),
    }
    assert matrix["B3"]["quarantine_delta"] == {
        "baseline": 0,
        "current": 1,
        "delta": 1,
        "delta_context": delta_context("quarantine_delta", {"baseline": 0, "current": 1, "delta": 1}, "B3"),
    }
    assert matrix["B3"]["masterfile_collision_delta"] == {
        "baseline": 2,
        "current": 0,
        "delta": -2,
        "delta_context": delta_context(
            "masterfile_collision_delta",
            {"baseline": 2, "current": 0, "delta": -2},
            "B3",
        ),
    }


def test_render_markdown_includes_source_file_traceability() -> None:
    markdown = render_markdown(
        {
            "_meta": {
                "generated_at": "2026-05-24T02:00:00Z",
                "baseline_generated_at": "2026-05-24T00:00:00Z",
                "source_files": {
                    "baseline_snapshot": "data/reports/improvement_baseline.json",
                    "current_snapshot_coverage_report": "data/reports/coverage_report.json",
                },
            },
            "summary": {"numeric_delta_rows": 1, "changed_numeric_delta_rows": 0},
            "acceptance_delta_matrix": {
                "isin_delta": {
                    "baseline": 1,
                    "current": 1,
                    "delta": 0,
                    "delta_context": "scope=global;metric=isin_delta;baseline=1;current=1;delta=0;direction=unchanged;review_policy=no_data_change_inferred",
                },
            },
            "changed_exchange_acceptance_deltas": {},
            "changed_numeric_deltas": [],
        }
    )

    assert "## Source Files" in markdown
    assert "scope=global;metric=isin_delta" in markdown
    assert "| `baseline_snapshot` | `data/reports/improvement_baseline.json` |" in markdown
    assert "| `current_snapshot_coverage_report` | `data/reports/coverage_report.json` |" in markdown

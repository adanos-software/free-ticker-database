import json
import os

from scripts.build_release_acceptance_report import (
    EXPECTED_BASELINE_SOURCE_FILES,
    EXPECTED_CAMPAIGN_SOURCE_FILES,
    EXPECTED_DELTA_SOURCE_FILES,
    REQUIRED_DELTA_KEYS,
    b3_etf_apply_category_review_context,
    b3_etf_apply_gate_context,
    b3_etf_apply_official_source_context,
    b3_masterfile_gap_listing_context,
    b3_masterfile_gap_official_candidate_context,
    b3_masterfile_gap_review_gate_context,
    b3_isin_official_source_context,
    b3_residual_review_context,
    b3_residual_scope_review_context,
    b3_residual_source_gap_context,
    b3_sector_official_source_context,
    evaluate_before_after_delta_matrix,
    delta_context,
    evaluate_campaign_baseline_alignment,
    baseline_campaign_context,
    baseline_context_integrity_gaps,
    baseline_exchange_context,
    baseline_global_context,
    evaluate_improvement_baseline_integrity,
    evaluate_progress_markdown_traceability,
    campaign_artifact_context,
    blocker_context,
    campaign_before_after_context,
    campaign_closure_context,
    campaign_context,
    campaign_delta_review_context,
    campaign_summary_context,
    evaluate_campaign_artifact_integrity,
    evaluate_campaign_acceptance_matrices,
    evaluate_canada_figi_gate,
    canada_identifier_review_context,
    canada_figi_apply_existing_identifier_context,
    canada_figi_apply_gate_context,
    canada_figi_apply_openfigi_probe_context,
    canada_official_source_context,
    canada_source_gap_context,
    collision_existing_dataset_context,
    collision_exchange_context,
    collision_identity_resolution_context,
    collision_official_source_context,
    collision_recommended_next_source,
    collision_source_gate,
    source_artifact_context,
    source_freshness_review_context,
    source_refresh_gate_context,
    evaluate_adanos_detection_simulation,
    evaluate_campaign_review_policies,
    evaluate_campaign_reviewability,
    campaign_status_rows,
    evaluate_next_review_batch_visibility,
    evaluate_next_review_command_safety_gate,
    command_mutation_risk,
    command_readiness_context,
    command_safety_context,
    command_safety_summary_context,
    command_script_paths,
    review_required_command_context,
    review_required_preflight_checks,
    review_required_preflight_context,
    risky_command_scripts,
    next_review_batch_closure_context,
    ngx_apply_gate_context,
    ngx_apply_mapping_review_context,
    ngx_apply_official_source_context,
    evaluate_campaign_closure_readiness_visibility,
    evaluate_campaign_closure_blocker_visibility,
    evaluate_coverage_freshness_visibility,
    evaluate_apply_artifact_traceability,
    evaluate_asx_residual_gate,
    evaluate_b3_residual_gate,
    evaluate_entry_quality_command_report,
    evaluate_supplement_artifact_traceability,
    financialdata_discovery_context,
    financialdata_official_identity_context,
    financialdata_supplement_review_context,
    evaluate_gate_group,
    evaluate_masterfile_collision_gate,
    evaluate_ohlcv_plausibility_gate,
    evaluate_otc_scope_gate,
    otc_scope_review_context,
    otc_scope_review_decision_context,
    otc_scope_recommended_next_source,
    otc_scope_review_strategy,
    otc_scope_source_gap_context,
    otc_scope_source_gate,
    otc_scope_verification_evidence_required,
    otc_name_mismatch_decision_review_context,
    otc_name_mismatch_identity_review_context,
    otc_name_mismatch_official_source_context,
    ohlcv_plausibility_review_context,
    ohlcv_sample_context,
    ohlcv_selection_context,
    evaluate_review_artifact_gates,
    evaluate_review_artifact_policy,
    evaluate_review_artifact_siblings,
    evaluate_review_row_evidence,
    evaluate_review_row_traceability,
    evaluate_release_source_report_integrity,
    evaluate_source_gap_traceability,
    evaluate_symbol_change_review_gate,
    evaluate_weak_sector_residual_gate,
    gate_lookup,
    is_valid_iso_utc_timestamp,
    parse_iso_utc_timestamp,
    symbol_change_scope_match_context,
    symbol_change_source_review_context,
    symbol_change_workflow_review_context,
    weak_sector_recommended_next_source_for,
    weak_sector_review_strategy_for,
    weak_sector_source_gate_for,
    campaign_review_policy_missing_marker_groups,
    review_policy_missing_marker_groups,
    row_has_review_evidence,
    row_has_apply_traceability,
    row_has_review_identity,
    render_markdown,
    release_summary_context,
    execution_plan_context,
    execution_ranking_context,
    execution_ranking_reason,
    execution_summary_context,
    next_review_workload_context,
    source_gap_classification_context,
    source_gap_context,
    source_gap_evidence_gate_context,
)


CAMPAIGN_META_FIXTURE = {
    "generated_at": "2026-05-24T00:00:00Z",
    "rows": 1,
    "source_files": EXPECTED_CAMPAIGN_SOURCE_FILES,
    "policy": "Progress report only. It summarizes review artifacts and does not authorize data fills or symbol changes.",
}

DELTA_META_FIXTURE = {
    "generated_at": "2026-05-24T02:00:00Z",
    "baseline_generated_at": "2026-05-24T00:00:00Z",
    "current_generated_at": "2026-05-24T01:00:00Z",
    "baseline_path": "data/reports/improvement_baseline.json",
    "source_files": EXPECTED_DELTA_SOURCE_FILES,
    "policy": "Delta report only. Positive or negative deltas require source-level review before any data change is inferred.",
}

BASELINE_META_FIXTURE = {
    "generated_at": "2026-05-24T00:00:00Z",
    "purpose": "Baseline snapshot for future campaign before/after deltas. It is not a data-fill source.",
    "source_files": EXPECTED_BASELINE_SOURCE_FILES,
    "source_reports": EXPECTED_BASELINE_SOURCE_FILES,
}


def zero_delta_matrix() -> dict[str, dict[str, object]]:
    matrix = {}
    for key in REQUIRED_DELTA_KEYS:
        row = {"baseline": 1, "current": 1, "delta": 0}
        row["delta_context"] = delta_context(key, row)
        matrix[key] = row
    return matrix


def test_gate_lookup_indexes_validation_gates() -> None:
    gates = gate_lookup(
        {
            "gates": [
                {"name": "invalid_isin_rows", "passed": True, "actual": 0},
                {"name": "", "passed": False},
            ]
        }
    )

    assert gates == {"invalid_isin_rows": {"name": "invalid_isin_rows", "passed": True, "actual": 0}}


def test_evaluate_gate_group_fails_when_gate_is_missing() -> None:
    result = evaluate_gate_group(
        {"invalid_isin_rows": {"name": "invalid_isin_rows", "passed": True, "actual": 0}},
        ["invalid_isin_rows", "duplicate_listing_key_count"],
    )

    assert result["passed"] is False
    assert result["gates"][1]["missing"] is True


def test_evaluate_gate_group_passes_when_all_gates_pass() -> None:
    result = evaluate_gate_group(
        {
            "invalid_isin_rows": {"name": "invalid_isin_rows", "passed": True, "actual": 0},
            "duplicate_listing_key_count": {"name": "duplicate_listing_key_count", "passed": True, "actual": 0},
        },
        ["invalid_isin_rows", "duplicate_listing_key_count"],
    )

    assert result["passed"] is True


def test_evaluate_release_source_report_integrity_requires_files_and_generated_at(tmp_path) -> None:
    reports_dir = tmp_path / "data" / "reports"
    reports_dir.mkdir(parents=True)
    (reports_dir / "validation_report.json").write_text(
        '{"_meta":{"generated_at":"2026-05-24T00:00:00Z"}}',
        encoding="utf-8",
    )
    (reports_dir / "entry_quality_gate.json").write_text(
        '{"_meta":{"generated_at":"2026-05-24T00:00:01Z"}}',
        encoding="utf-8",
    )

    result = evaluate_release_source_report_integrity(
        {
            "validation_report": "data/reports/validation_report.json",
            "entry_quality_gate": "data/reports/entry_quality_gate.json",
        },
        root=tmp_path,
        release_generated_at="2026-05-24T00:00:02Z",
    )

    assert result["passed"] is True
    assert result["release_generated_at"] == "2026-05-24T00:00:02Z"
    assert result["checked"]["validation_report"]["generated_at"] == "2026-05-24T00:00:00Z"
    assert result["invalid_generated_at"] == {}
    assert result["generated_after_release"] == {}


def test_evaluate_release_source_report_integrity_fails_for_missing_file_or_timestamp(tmp_path) -> None:
    reports_dir = tmp_path / "data" / "reports"
    reports_dir.mkdir(parents=True)
    (reports_dir / "validation_report.json").write_text('{"summary":{}}', encoding="utf-8")

    result = evaluate_release_source_report_integrity(
        {
            "validation_report": "data/reports/validation_report.json",
            "entry_quality_gate": "data/reports/entry_quality_gate.json",
        },
        root=tmp_path,
    )

    assert result["passed"] is False
    assert result["missing_generated_at"] == ["validation_report"]
    assert result["missing_files"] == ["entry_quality_gate"]


def test_evaluate_release_source_report_integrity_fails_for_invalid_timestamp(tmp_path) -> None:
    reports_dir = tmp_path / "data" / "reports"
    reports_dir.mkdir(parents=True)
    (reports_dir / "validation_report.json").write_text(
        '{"_meta":{"generated_at":"not-a-timestamp"}}',
        encoding="utf-8",
    )

    result = evaluate_release_source_report_integrity(
        {"validation_report": "data/reports/validation_report.json"},
        root=tmp_path,
    )

    assert result["passed"] is False
    assert result["invalid_generated_at"] == {"validation_report": "not-a-timestamp"}


def test_evaluate_release_source_report_integrity_fails_for_future_source_report(tmp_path) -> None:
    reports_dir = tmp_path / "data" / "reports"
    reports_dir.mkdir(parents=True)
    (reports_dir / "validation_report.json").write_text(
        '{"_meta":{"generated_at":"2026-05-24T00:00:03Z"}}',
        encoding="utf-8",
    )

    result = evaluate_release_source_report_integrity(
        {"validation_report": "data/reports/validation_report.json"},
        root=tmp_path,
        release_generated_at="2026-05-24T00:00:02Z",
    )

    assert result["passed"] is False
    assert result["generated_after_release"] == {
        "validation_report": {
            "source_generated_at": "2026-05-24T00:00:03Z",
            "release_generated_at": "2026-05-24T00:00:02Z",
        }
    }


def test_evaluate_release_source_report_integrity_fails_for_invalid_release_timestamp(tmp_path) -> None:
    reports_dir = tmp_path / "data" / "reports"
    reports_dir.mkdir(parents=True)
    (reports_dir / "validation_report.json").write_text(
        '{"_meta":{"generated_at":"2026-05-24T00:00:00Z"}}',
        encoding="utf-8",
    )

    result = evaluate_release_source_report_integrity(
        {"validation_report": "data/reports/validation_report.json"},
        root=tmp_path,
        release_generated_at="not-a-timestamp",
    )

    assert result["passed"] is False
    assert result["release_generated_at_invalid"] is True


def test_evaluate_progress_markdown_traceability_requires_generated_at_and_sources(tmp_path) -> None:
    reports_dir = tmp_path / "data" / "reports"
    reports_dir.mkdir(parents=True)
    (reports_dir / "improvement_baseline.json").write_text(
        '{"_meta":{"generated_at":"2026-05-24T00:00:00Z"}}',
        encoding="utf-8",
    )
    (reports_dir / "improvement_baseline.md").write_text(
        "# Improvement Baseline\n\nGenerated: `2026-05-24T00:00:00Z`\n\n## Source Files\n\n"
        "| Key | Path |\n|---|---|\n"
        "| `coverage_report` | `data/reports/coverage_report.json` |\n",
        encoding="utf-8",
    )

    result = evaluate_progress_markdown_traceability(
        {
            "improvement_baseline": {
                "json": "data/reports/improvement_baseline.json",
                "markdown": "data/reports/improvement_baseline.md",
                "source_files": {"coverage_report": "data/reports/coverage_report.json"},
            }
        },
        root=tmp_path,
    )

    assert result["passed"] is True
    assert result["checked_reports"] == ["improvement_baseline"]


def test_evaluate_progress_markdown_traceability_fails_for_stale_or_missing_sources(tmp_path) -> None:
    reports_dir = tmp_path / "data" / "reports"
    reports_dir.mkdir(parents=True)
    (reports_dir / "improvement_baseline.json").write_text(
        '{"_meta":{"generated_at":"2026-05-24T00:00:00Z"}}',
        encoding="utf-8",
    )
    (reports_dir / "improvement_baseline.md").write_text(
        "# Improvement Baseline\n\nGenerated: `2026-05-23T00:00:00Z`\n\n",
        encoding="utf-8",
    )

    result = evaluate_progress_markdown_traceability(
        {
            "improvement_baseline": {
                "json": "data/reports/improvement_baseline.json",
                "markdown": "data/reports/improvement_baseline.md",
                "source_files": {"coverage_report": "data/reports/coverage_report.json"},
            },
            "improvement_deltas": {
                "json": "data/reports/improvement_deltas.json",
                "markdown": "data/reports/improvement_deltas.md",
                "source_files": {},
            },
        },
        root=tmp_path,
    )

    assert result["passed"] is False
    assert result["missing_files"] == {
        "improvement_deltas": [
            "data/reports/improvement_deltas.json",
            "data/reports/improvement_deltas.md",
        ]
    }
    assert result["missing_source_sections"] == ["improvement_baseline"]
    assert result["generated_at_mismatches"] == {
        "improvement_baseline": {
            "expected": "2026-05-24T00:00:00Z",
            "markdown": "data/reports/improvement_baseline.md",
        }
    }
    assert result["missing_source_paths"] == {
        "improvement_baseline": ["data/reports/coverage_report.json"]
    }
    assert result["missing_source_keys"] == {
        "improvement_baseline": ["coverage_report"]
    }


def test_evaluate_adanos_detection_simulation_requires_clean_probe_results() -> None:
    result = evaluate_adanos_detection_simulation(
        {
            "_meta": {
                "generated_at": "2026-05-24T00:00:00Z",
                "source_files": {"adanos_reference_csv": "data/adanos/ticker_reference.csv"},
            },
            "summary": {
                "reference_rows": 10,
                "alias_entries": 8,
                "tickers_with_aliases": 6,
                "positive_probes": 5,
                "positive_misses": 0,
                "negative_probes": 5,
                "negative_hits": 0,
            },
            "positive_results": [
                {"id": f"positive_{index}", "text": "Microsoft rallied.", "expected_ticker": "MSFT"}
                for index in range(5)
            ],
            "negative_results": [
                {"id": f"negative_{index}", "text": "The market closed higher."}
                for index in range(5)
            ],
            "positive_misses": [],
            "negative_hits": [],
        }
    )

    assert result["passed"] is True


def test_evaluate_adanos_detection_simulation_fails_for_false_positive_or_weak_metadata() -> None:
    result = evaluate_adanos_detection_simulation(
        {
            "_meta": {
                "generated_at": "not-a-timestamp",
                "source_files": {"adanos_reference_csv": "data/other.csv"},
            },
            "summary": {
                "reference_rows": 10,
                "alias_entries": 8,
                "tickers_with_aliases": 6,
                "positive_probes": 4,
                "positive_misses": 1,
                "negative_probes": 5,
                "negative_hits": 1,
            },
            "positive_results": [{"id": "miss", "text": "No known issuer here.", "expected_ticker": "TSLA"}],
            "negative_results": [{"id": "hit", "text": "The bank held rates."}],
            "positive_misses": [{"id": "miss", "text": "No known issuer here.", "expected_ticker": "TSLA"}],
            "negative_hits": [{"id": "hit", "text": "The bank held rates."}],
        }
    )

    assert result["passed"] is False
    assert result["invalid_generated_at"] == "not-a-timestamp"
    assert result["source_file_mismatch"] is True
    assert result["nonzero_detection_failures"] == {"positive_misses": 1, "negative_hits": 1}
    assert result["minimum_probe_gaps"] == {"positive_probes": 4}


def test_is_valid_iso_utc_timestamp_requires_z_suffix_and_parseable_value() -> None:
    assert is_valid_iso_utc_timestamp("2026-05-24T00:00:00Z") is True
    assert is_valid_iso_utc_timestamp("2026-05-24T00:00:00+00:00") is False
    assert is_valid_iso_utc_timestamp("not-a-timestamp") is False
    assert parse_iso_utc_timestamp("2026-05-24T00:00:00Z") is not None
    assert parse_iso_utc_timestamp("not-a-timestamp") is None


def test_evaluate_entry_quality_command_report_requires_passed_report_matching_validation() -> None:
    result = evaluate_entry_quality_command_report(
        {
            "_meta": {
                "generated_at": "2026-05-24T00:00:00Z",
                "entry_quality_csv": "data/reports/entry_quality.csv",
                "warn_allowlist_csv": "data/reports/entry_quality_warn_allowlist.csv",
                "policy": "Command evidence",
            },
            "passed": True,
            "quarantine_count": 0,
            "unexpected_warn_count": 0,
            "warn_count": 2,
            "allowed_warn_count": 3,
        },
        {
            "entry_quality_quarantine_count": {"actual": 0},
            "entry_quality_unexpected_warn_count": {"actual": 0},
        },
    )

    assert result["passed"] is True
    assert result["generated_at"] == "2026-05-24T00:00:00Z"


def test_evaluate_entry_quality_command_report_fails_for_missing_or_mismatched_report() -> None:
    result = evaluate_entry_quality_command_report(
        {
            "passed": True,
            "quarantine_count": 1,
            "warn_count": 2,
            "allowed_warn_count": 3,
        },
        {
            "entry_quality_quarantine_count": {"actual": 0},
            "entry_quality_unexpected_warn_count": {"actual": 0},
        },
    )

    assert result["passed"] is False
    assert result["missing_meta_keys"] == ["generated_at", "entry_quality_csv", "warn_allowlist_csv", "policy"]
    assert result["missing_keys"] == ["unexpected_warn_count"]
    assert result["mismatched_counts"] == {
        "quarantine_count": {"command_report": 1, "validation_gate": 0}
    }


def test_evaluate_coverage_freshness_visibility_requires_dataset_source_and_symbol_change_ages() -> None:
    result = evaluate_coverage_freshness_visibility(
        {
            "_meta": {
                "generated_at": "2026-05-24T01:05:00Z",
                "source_files": {
                    "tickers_csv": "data/tickers.csv",
                    "listings_csv": "data/listings.csv",
                    "core_listings_csv": "data/core_listings.csv",
                    "aliases_csv": "data/aliases.csv",
                    "instrument_scopes_csv": "data/instrument_scopes.csv",
                    "identifiers_extended_csv": "data/identifiers_extended.csv",
                    "identifier_summary_json": "data/identifier_summary.json",
                    "masterfile_reference_csv": "data/masterfiles/reference.csv",
                    "masterfile_sources_json": "data/masterfiles/sources.json",
                    "masterfile_summary_json": "data/masterfiles/summary.json",
                    "listing_status_history_csv": "data/history/listing_status_history.csv",
                    "listing_events_csv": "data/history/listing_events.csv",
                    "daily_listing_summary_json": "data/history/daily_listing_summary.json",
                    "symbol_changes_review_json": "data/reports/symbol_changes_review.json",
                    "entry_quality_json": "data/reports/entry_quality.json",
                    "source_gap_classification_json": "data/reports/source_gap_classification.json",
                    "masterfile_collision_review_json": "data/reports/masterfile_collision_review.json",
                    "ohlcv_plausibility_json": "data/reports/ohlcv_plausibility.json",
                },
                "policy": "Coverage and freshness report only. It does not authorize inferred identifiers, sectors, categories, names, or symbol changes.",
            },
            "freshness": {
                "tickers_built_at": "2026-05-24T00:00:00Z",
                "tickers_age_hours": 2.0,
                "masterfiles_generated_at": "2026-05-24T00:30:00Z",
                "masterfiles_age_hours": 1.5,
                "identifiers_generated_at": "2026-05-24T00:15:00Z",
                "identifiers_age_hours": 1.75,
                "listing_history_observed_at": "2026-05-24T00:10:00Z",
                "listing_history_age_hours": 1.83,
                "latest_verification_run": "reports/verification/2026-05-24",
                "latest_verification_generated_at": "2026-05-24T00:20:00Z",
                "latest_verification_age_hours": 1.67,
                "latest_stock_verification_run": "reports/verification/stock/2026-05-24",
                "latest_stock_verification_generated_at": "2026-05-24T00:20:00Z",
                "latest_stock_verification_age_hours": 1.67,
                "latest_etf_verification_run": "reports/verification/etf/2026-05-24",
                "latest_etf_verification_generated_at": "2026-05-24T00:25:00Z",
                "latest_etf_verification_age_hours": 1.58,
                "symbol_changes_generated_at": "2026-05-24T01:00:00Z",
                "symbol_changes_age_hours": 1.0,
                "symbol_changes_review_rows": 3,
                "entry_quality_generated_at": "2026-05-24T00:35:00Z",
                "entry_quality_age_hours": 1.42,
                "entry_quality_rows": 2,
                "masterfile_collision_review_generated_at": "2026-05-24T00:40:00Z",
                "masterfile_collision_review_age_hours": 1.33,
                "masterfile_collision_review_rows": 1,
                "ohlcv_plausibility_generated_at": "2026-05-24T00:45:00Z",
                "ohlcv_plausibility_age_hours": 1.25,
                "ohlcv_plausibility_rows": 4,
                "source_gap_classification_generated_at": "2026-05-24T00:50:00Z",
                "source_gap_classification_age_hours": 1.17,
                "source_gap_classification_rows": 5,
            },
            "source_coverage": [
                {
                    "key": "nasdaq_listed",
                    "provider": "Nasdaq Trader",
                    "reference_scope": "exchange_directory",
                    "official": True,
                    "mode": "network",
                    "rows": 100,
                    "generated_at": "2026-05-24T00:00:00Z",
                    "age_hours": 2.0,
                    "age_bucket": "age_0_48h",
                    "freshness_status": "fresh",
                    "refresh_priority": "P4",
                    "refresh_queue": "fresh_no_refresh_needed",
                    "review_strategy": "no_refresh_required",
                    "evidence_required": "fresh_source_generated_at_with_age_under_48h",
                    "recommended_next_source": (
                        "No refresh needed; retain current fresh source evidence for scope exchange_directory."
                    ),
                    "source_gate": (
                        "Freshness evidence is present; no data change is authorized by freshness alone."
                    ),
                    "recommended_refresh_action": "no_refresh_needed",
                        "source_artifact_context": (
                            "key=nasdaq_listed;provider=Nasdaq Trader;reference_scope=exchange_directory;"
                            "official=true;mode=network;rows=100;last_error=none"
                        ),
                    "freshness_review_context": (
                        "generated_at=2026-05-24T00:00:00Z;age_bucket=age_0_48h;"
                        "freshness_status=fresh;refresh_priority=P4"
                    ),
                    "refresh_gate_context": (
                        "refresh_queue=fresh_no_refresh_needed;recommended_refresh_action=no_refresh_needed;"
                        "review_strategy=no_refresh_required;evidence_required=fresh_source_generated_at_with_age_under_48h"
                    ),
                },
                {
                    "key": "old_exchange",
                    "provider": "Old Exchange",
                    "reference_scope": "exchange_directory",
                    "official": True,
                    "mode": "cache",
                    "rows": 50,
                    "generated_at": "2026-05-20T00:00:00Z",
                    "age_hours": 200.0,
                    "age_bucket": "age_168_336h",
                    "freshness_status": "old",
                    "refresh_priority": "P1",
                    "refresh_queue": "refresh_official_exchange_directory_before_identity_or_collision_work",
                    "review_strategy": "refresh_official_exchange_directory_before_identity_or_collision_work",
                    "evidence_required": "official_exchange_directory_refresh_artifact_with_generated_at_and_row_count",
                    "recommended_next_source": (
                        "Refresh the official exchange-directory source for scope exchange_directory using mode cache."
                    ),
                    "source_gate": (
                        "Do not perform identity, collision, or listing-add work until the official exchange "
                        "directory is freshly regenerated."
                    ),
                    "recommended_refresh_action": "refresh_official_exchange_directory_before_identity_or_collision_work",
                        "source_artifact_context": (
                            "key=old_exchange;provider=Old Exchange;reference_scope=exchange_directory;"
                            "official=true;mode=cache;rows=50;last_error=none"
                        ),
                    "freshness_review_context": (
                        "generated_at=2026-05-20T00:00:00Z;age_bucket=age_168_336h;"
                        "freshness_status=old;refresh_priority=P1"
                    ),
                    "refresh_gate_context": (
                        "refresh_queue=refresh_official_exchange_directory_before_identity_or_collision_work;"
                        "recommended_refresh_action=refresh_official_exchange_directory_before_identity_or_collision_work;"
                        "review_strategy=refresh_official_exchange_directory_before_identity_or_collision_work;"
                        "evidence_required=official_exchange_directory_refresh_artifact_with_generated_at_and_row_count"
                    ),
                },
            ],
            "source_freshness_summary": {
                "source_count": 2,
                "freshness_status_totals": {"fresh": 1, "old": 1},
                "source_age_bucket_totals": {"age_0_48h": 1, "age_168_336h": 1},
                "refresh_priority_totals": {"P1": 1, "P4": 1},
                "refresh_queue_totals": {
                    "fresh_no_refresh_needed": 1,
                    "refresh_official_exchange_directory_before_identity_or_collision_work": 1,
                },
                "refresh_queue_scope_totals": {
                    "fresh_no_refresh_needed": {"exchange_directory": 1},
                    "refresh_official_exchange_directory_before_identity_or_collision_work": {"exchange_directory": 1},
                },
                "refresh_queue_mode_totals": {
                    "fresh_no_refresh_needed": {"network": 1},
                    "refresh_official_exchange_directory_before_identity_or_collision_work": {"cache": 1},
                },
                "refresh_queue_priority_totals": {
                    "fresh_no_refresh_needed": {"P4": 1},
                    "refresh_official_exchange_directory_before_identity_or_collision_work": {"P1": 1},
                },
                "refresh_queue_age_bucket_totals": {
                    "fresh_no_refresh_needed": {"age_0_48h": 1},
                    "refresh_official_exchange_directory_before_identity_or_collision_work": {"age_168_336h": 1},
                },
                "recommended_refresh_action_totals": {
                    "no_refresh_needed": 1,
                    "refresh_official_exchange_directory_before_identity_or_collision_work": 1,
                },
                "refresh_queue_review_strategy_totals": {
                    "fresh_no_refresh_needed": {"no_refresh_required": 1},
                    "refresh_official_exchange_directory_before_identity_or_collision_work": {
                        "refresh_official_exchange_directory_before_identity_or_collision_work": 1
                    },
                },
                "refresh_queue_evidence_required_totals": {
                    "fresh_no_refresh_needed": {"fresh_source_generated_at_with_age_under_48h": 1},
                    "refresh_official_exchange_directory_before_identity_or_collision_work": {
                        "official_exchange_directory_refresh_artifact_with_generated_at_and_row_count": 1
                    },
                },
                "top_source_refresh_batches": [
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
                            "Do not perform identity, collision, or listing-add work until the official exchange "
                            "directory is freshly regenerated."
                        ),
                    }
                ],
                "old_official_exchange_directory_count": 1,
                "top_old_official_exchange_directories": [{"key": "old_exchange"}],
            },
            "b3_masterfile_diagnostics": {
                "policy": "B3 diagnostic only. Missing rows are review targets and do not authorize inferred identifiers.",
                "dataset_rows": 3,
                "active_exchange_directory_rows": 4,
                "all_b3_masterfile_rows": 5,
                "matched_dataset_rows": 2,
                "missing_dataset_rows": 1,
                "official_any_source_matched_dataset_rows": 3,
                "official_any_source_missing_dataset_rows": 0,
                "official_any_source_match_rate": 100.0,
                "official_active_symbols_not_in_dataset": 2,
                "dataset_match_rate": 66.67,
                "active_source_key_totals": {"b3_instruments_equities": 4},
                "all_source_key_totals": {"b3_instruments_equities": 4, "b3_listed_etfs": 1},
                "missing_category_totals": {"unit_or_fund_line": 1},
                "missing_asset_type_totals": {"ETF": 1},
                "missing_source_presence_totals": {"present_only_in_non_exchange_directory_source": 1},
                "missing_examples": {
                    "unit_or_fund_line": [
                        {
                            "listing_key": "B3::AFOF11",
                            "ticker": "AFOF11",
                            "asset_type": "ETF",
                            "name": "Alianza FOFII",
                            "source_presence": "present_only_in_non_exchange_directory_source",
                            "candidate_sources": "b3_listed_etfs",
                        }
                    ]
                },
            },
        }
    )

    assert result["passed"] is True
    assert result["source_rows"] == 2
    assert result["source_freshness_status_totals"] == {"fresh": 1, "old": 1}
    assert result["source_age_bucket_totals"] == {"age_0_48h": 1, "age_168_336h": 1}
    assert result["source_refresh_priority_totals"] == {"P4": 1, "P1": 1}
    assert result["source_refresh_queue_totals"] == {
        "fresh_no_refresh_needed": 1,
        "refresh_official_exchange_directory_before_identity_or_collision_work": 1,
    }
    assert result["source_refresh_queue_scope_totals"] == {
        "fresh_no_refresh_needed": {"exchange_directory": 1},
        "refresh_official_exchange_directory_before_identity_or_collision_work": {"exchange_directory": 1},
    }
    assert result["source_refresh_queue_mode_totals"] == {
        "fresh_no_refresh_needed": {"network": 1},
        "refresh_official_exchange_directory_before_identity_or_collision_work": {"cache": 1},
    }
    assert result["source_refresh_queue_priority_totals"] == {
        "fresh_no_refresh_needed": {"P4": 1},
        "refresh_official_exchange_directory_before_identity_or_collision_work": {"P1": 1},
    }
    assert result["source_refresh_queue_age_bucket_totals"] == {
        "fresh_no_refresh_needed": {"age_0_48h": 1},
        "refresh_official_exchange_directory_before_identity_or_collision_work": {"age_168_336h": 1},
    }
    assert result["source_refresh_queue_review_strategy_totals"] == {
        "fresh_no_refresh_needed": {"no_refresh_required": 1},
        "refresh_official_exchange_directory_before_identity_or_collision_work": {
            "refresh_official_exchange_directory_before_identity_or_collision_work": 1
        },
    }
    assert result["source_refresh_queue_evidence_required_totals"] == {
        "fresh_no_refresh_needed": {"fresh_source_generated_at_with_age_under_48h": 1},
        "refresh_official_exchange_directory_before_identity_or_collision_work": {
            "official_exchange_directory_refresh_artifact_with_generated_at_and_row_count": 1
        },
    }
    assert result["b3_masterfile_diagnostics"]["missing_dataset_rows"] == 1


def test_evaluate_coverage_freshness_visibility_fails_for_missing_or_inconsistent_freshness() -> None:
    result = evaluate_coverage_freshness_visibility(
        {
            "_meta": {
                "generated_at": "not-a-timestamp",
                "source_files": {
                    "tickers_csv": "data/other.csv",
                    "unexpected_json": "data/unexpected.json",
                },
                "policy": "Coverage report.",
            },
            "freshness": {
                "tickers_built_at": "not-a-timestamp",
                "tickers_age_hours": -1,
                "masterfiles_generated_at": "2026-05-24T00:30:00Z",
                "masterfiles_age_hours": 1.5,
                "identifiers_generated_at": "2026-05-24T00:15:00Z",
                "identifiers_age_hours": 1.75,
                "symbol_changes_generated_at": "2026-05-24T01:00:00Z",
                "symbol_changes_age_hours": 1.0,
                "symbol_changes_review_rows": -1,
            },
            "source_coverage": [
                {
                    "key": "broken_source",
                    "provider": "Broken",
                    "reference_scope": "exchange_directory",
                    "official": True,
                    "mode": "cache",
                    "rows": 1,
                    "generated_at": "2026-05-24T00:00:00+00:00",
                    "age_hours": "",
                    "freshness_status": "expired",
                    "refresh_priority": "P9",
                    "recommended_refresh_action": "",
                }
            ],
            "source_freshness_summary": {
                "source_count": 2,
                "freshness_status_totals": {"fresh": 1},
                "refresh_priority_totals": {"P4": 1},
                "recommended_refresh_action_totals": {"no_refresh_needed": 1},
                "old_official_exchange_directory_count": 0,
                "top_old_official_exchange_directories": [],
            },
        }
    )

    assert result["passed"] is False
    assert result["invalid_meta_generated_at"] == "not-a-timestamp"
    assert result["source_file_mismatches"]["tickers_csv"] == {
        "expected": "data/tickers.csv",
        "actual": "data/other.csv",
    }
    assert result["source_file_mismatches"]["listings_csv"] == {
        "expected": "data/listings.csv",
        "actual": None,
    }
    assert result["unexpected_source_files"] == ["unexpected_json"]
    assert result["policy_missing_markers"] == [
        "Coverage and freshness report only",
        "does not authorize",
        "identifiers",
        "symbol changes",
    ]
    assert result["invalid_timestamps"] == ["tickers_built_at"]
    assert result["invalid_age_fields"] == [
        "tickers_age_hours",
        "listing_history_age_hours",
        "latest_verification_age_hours",
        "latest_stock_verification_age_hours",
        "latest_etf_verification_age_hours",
        "entry_quality_age_hours",
        "masterfile_collision_review_age_hours",
        "ohlcv_plausibility_age_hours",
        "source_gap_classification_age_hours",
    ]
    assert result["invalid_count_fields"] == [
        "symbol_changes_review_rows",
        "entry_quality_rows",
        "masterfile_collision_review_rows",
        "ohlcv_plausibility_rows",
        "source_gap_classification_rows",
    ]
    assert result["source_count_mismatch"] == {"reported": 2, "actual": 1}
    assert result["source_row_gaps"] == [
        {
                "row_index": 0,
                "key": "broken_source",
                "missing_keys": [
                    "age_bucket",
                    "refresh_queue",
                    "review_strategy",
                    "evidence_required",
                    "recommended_next_source",
                    "source_gate",
                    "source_artifact_context",
                    "freshness_review_context",
                    "refresh_gate_context",
                ],
                "invalid_fields": [
                    "generated_at",
                    "age_hours",
                    "freshness_status",
                    "age_bucket",
                    "refresh_priority",
                    "refresh_queue",
                    "recommended_refresh_action",
                    "recommended_next_source",
                    "source_gate",
                    "review_strategy",
                    "evidence_required",
                    "source_artifact_context",
                    "freshness_review_context",
                    "refresh_gate_context",
                ],
            }
    ]
    assert result["summary_mismatches"]["freshness_status_totals"] == {
        "expired": {"reported": 0, "actual": 1},
        "fresh": {"reported": 1, "actual": 0},
    }
    assert "dataset_rows" in result["missing_b3_diagnostic_keys"]


def test_source_freshness_contexts_are_exact_field_summaries() -> None:
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
        source_artifact_context(row)
        == "key=nasdaq_listed;provider=Nasdaq Trader;reference_scope=exchange_directory;"
        "official=true;mode=network;rows=5471;last_error=none"
    )
    assert (
        source_freshness_review_context(row)
        == "generated_at=2026-05-24T15:53:44Z;age_bucket=age_0_48h;"
        "freshness_status=fresh;refresh_priority=P4"
    )
    assert (
        source_refresh_gate_context(row)
        == "refresh_queue=fresh_no_refresh_needed;recommended_refresh_action=no_refresh_needed;"
        "review_strategy=no_refresh_required;evidence_required=fresh_source_generated_at_with_age_under_48h"
    )


def test_evaluate_source_gap_traceability_requires_listing_key_source_gate_and_matching_summaries() -> None:
    result = evaluate_source_gap_traceability(
        {
            "summary": {
                "rows": 1,
                "field_totals": {"missing_isin_primary": 1},
                "class_totals": {"official_identifier_not_exposed_source_gap": 1},
                "class_by_field": {"missing_isin_primary:official_identifier_not_exposed_source_gap": 1},
                "top_exchanges_by_field": {"missing_isin_primary": [{"exchange": "TSX", "rows": 1}]},
                "top_source_gap_review_batches": [
                    {
                        "field": "missing_isin_primary",
                        "gap_class": "official_identifier_not_exposed_source_gap",
                        "exchange": "TSX",
                        "rows": 1,
                        "recommended_next_source": "Official CSD/security registry.",
                        "source_gate": "Exact symbol/name and direct ISIN evidence.",
                    }
                ],
                "policy": {
                    "no_unreviewed_heuristics": "classify only",
                    "release_gate": "deterministic class and source gate",
                },
            },
            "rows": [
                {
                    "listing_key": "TSX::ABC",
                    "ticker": "ABC",
                    "exchange": "TSX",
                    "field": "missing_isin_primary",
                    "target_field": "isin",
                    "gap_class": "official_identifier_not_exposed_source_gap",
                    "review_needed": "true",
                    "confidence_policy": "Keep blank until official source exposes ISIN.",
                    "recommended_next_source": "Official CSD/security registry.",
                    "source_gate": "Exact symbol/name and direct ISIN evidence.",
                    "source_gap_context": (
                        "listing_key=TSX::ABC;exchange=TSX;ticker=ABC;asset_type=none;"
                        "field=missing_isin_primary;target_field=isin"
                    ),
                    "classification_context": (
                        "gap_class=official_identifier_not_exposed_source_gap;review_needed=true;"
                        "confidence_policy_present=true;name_present=false"
                    ),
                    "evidence_gate_context": (
                        "recommended_next_source_present=true;source_gate_present=true;"
                        "target_field=isin;gap_class=official_identifier_not_exposed_source_gap"
                    ),
                }
            ],
        }
    )

    assert result["passed"] is True
    assert result["rows"] == 1


def test_b3_masterfile_gap_contexts_are_exact_field_summaries() -> None:
    row = {
        "listing_key": "B3::AFOF11",
        "ticker": "AFOF11",
        "asset_type": "ETF",
        "current_etf_category": "Equity",
        "b3_gap_category": "unit_or_fund_line",
        "source_presence": "present_only_in_non_exchange_directory_source",
        "candidate_sources": "b3_listed_etfs",
        "candidate_isins": "BRAFOFCTF000",
        "candidate_sectors": "Fixed Income",
        "active_exchange_directory_match": "false",
        "any_official_b3_source_match": "true",
        "b3_resolution_queue": "official_subset_category_requires_review",
        "residual_decision": "official_b3_non_directory_source_requires_scope_or_parser_review",
        "review_bucket": "official_b3_non_directory_source_review",
        "official_subset_review_decision": "official_subset_category_mismatch_requires_apply_gate",
        "official_subset_closure_eligibility": "blocked_until_category_apply_gate",
        "apply_eligibility": "review_scope_or_parser_before_any_data_change",
        "verification_evidence_required": (
            "official_b3_source_row_plus_scope_decision_or_parser_fix_before_reclassifying_gap"
        ),
    }

    assert (
        b3_masterfile_gap_listing_context(row)
        == "listing_key=B3::AFOF11;ticker=AFOF11;asset_type=ETF;"
        "b3_gap_category=unit_or_fund_line;current_etf_category=Equity"
    )
    assert (
        b3_masterfile_gap_official_candidate_context(row)
        == "source_presence=present_only_in_non_exchange_directory_source;candidate_sources=b3_listed_etfs;"
        "candidate_isins_present=true;candidate_sectors_present=true;"
        "active_exchange_directory_match=false;any_official_b3_source_match=true"
    )
    assert (
        b3_masterfile_gap_review_gate_context(row)
        == "b3_resolution_queue=official_subset_category_requires_review;"
        "residual_decision=official_b3_non_directory_source_requires_scope_or_parser_review;"
        "review_bucket=official_b3_non_directory_source_review;"
        "official_subset_review_decision=official_subset_category_mismatch_requires_apply_gate;"
        "official_subset_closure_eligibility=blocked_until_category_apply_gate;"
        "apply_eligibility=review_scope_or_parser_before_any_data_change;"
        "verification_evidence_required=official_b3_source_row_plus_scope_decision_or_parser_fix_before_reclassifying_gap"
    )


def test_source_gap_contexts_are_exact_field_summaries() -> None:
    row = {
        "listing_key": "TSX::ABC",
        "exchange": "TSX",
        "ticker": "ABC",
        "asset_type": "Stock",
        "field": "missing_isin_primary",
        "target_field": "isin",
        "name": "ABC Inc.",
        "gap_class": "official_identifier_not_exposed_source_gap",
        "review_needed": "true",
        "confidence_policy": "Keep blank until official source exposes ISIN.",
        "recommended_next_source": "Official CSD/security registry.",
        "source_gate": "Exact symbol/name and direct ISIN evidence.",
    }

    assert (
        source_gap_context(row)
        == "listing_key=TSX::ABC;exchange=TSX;ticker=ABC;asset_type=Stock;"
        "field=missing_isin_primary;target_field=isin"
    )
    assert (
        source_gap_classification_context(row)
        == "gap_class=official_identifier_not_exposed_source_gap;review_needed=true;"
        "confidence_policy_present=true;name_present=true"
    )
    assert (
        source_gap_evidence_gate_context(row)
        == "recommended_next_source_present=true;source_gate_present=true;"
        "target_field=isin;gap_class=official_identifier_not_exposed_source_gap"
    )


def test_evaluate_source_gap_traceability_fails_for_bad_rows_or_stale_summaries() -> None:
    result = evaluate_source_gap_traceability(
        {
            "summary": {
                "rows": 2,
                "field_totals": {"missing_isin_primary": 2},
                "class_totals": {"official_identifier_not_exposed_source_gap": 2},
                "class_by_field": {"missing_isin_primary:official_identifier_not_exposed_source_gap": 2},
                "top_exchanges_by_field": {"missing_isin_primary": [{"exchange": "TSX", "rows": 2}]},
                "top_source_gap_review_batches": [
                    {
                        "field": "unknown_field",
                        "gap_class": "official_identifier_not_exposed_source_gap",
                        "exchange": "TSX",
                        "rows": 0,
                        "recommended_next_source": "",
                        "source_gate": "",
                    }
                ],
                "policy": {"no_unreviewed_heuristics": "classify only"},
            },
            "rows": [
                {
                    "listing_key": "",
                    "ticker": "ABC",
                    "exchange": "TSX",
                    "field": "unknown_field",
                    "target_field": "isin",
                    "gap_class": "official_identifier_not_exposed_source_gap",
                    "review_needed": "false",
                    "confidence_policy": "",
                    "recommended_next_source": "Official CSD/security registry.",
                    "source_gate": "",
                }
            ],
        }
    )

    assert result["passed"] is False
    assert result["row_count_mismatch"] == {"reported": 2, "actual": 1}
    assert result["missing_policy_keys"] == ["release_gate"]
    assert result["summary_mismatches"]["field_totals"] == {
        "missing_isin_primary": {"reported": 2, "actual": 0},
        "unknown_field": {"reported": 0, "actual": 1},
    }
    assert result["top_exchange_gaps"] == [
        {
            "field": "top_exchanges_by_field.unknown_field",
            "reported": None,
            "expected": [{"exchange": "TSX", "rows": 1}],
        },
        {
            "field": "top_exchanges_by_field",
            "unknown_fields": ["missing_isin_primary"],
        },
    ]
    assert result["top_review_batch_gaps"] == [
        {
            "row_index": 0,
            "missing_keys": ["recommended_next_source", "source_gate"],
            "invalid_fields": ["field", "rows"],
        }
    ]
    assert result["row_gaps"] == [
        {
            "row_index": 0,
            "listing_key": "",
            "missing_keys": [
                "listing_key",
                "confidence_policy",
                "source_gate",
                "source_gap_context",
                "classification_context",
                "evidence_gate_context",
            ],
            "invalid_fields": [
                "field",
                "review_needed",
                "confidence_policy",
                "source_gate",
                "source_gap_context",
                "classification_context",
                "evidence_gate_context",
            ],
            "available_keys": [
                "confidence_policy",
                "exchange",
                "field",
                "gap_class",
                "listing_key",
                "recommended_next_source",
                "review_needed",
                "source_gate",
                "target_field",
                "ticker",
            ],
        }
    ]


def test_evaluate_symbol_change_review_gate_requires_taxonomy_and_matching_summaries() -> None:
    rows = [
        {
            "change_id": "rename",
            "effective_date": "2026-05-01",
            "old_symbol": "OLD",
            "new_symbol": "NEW",
            "source": "stockanalysis_symbol_changes",
            "source_confidence": "secondary_review",
            "review_needed": "true",
            "match_status": "old_symbol_present_new_symbol_missing",
            "symbol_change_workflow_queue": "review_verified_rename_or_delisting",
            "review_bucket": "action_required_possible_rename_or_delisting",
            "review_priority": "P1",
            "recency_bucket": "recent_30d",
            "apply_eligibility": "requires_official_venue_confirmation",
            "verification_evidence_required": "official_exchange_notice_or_current_directory_showing_old_symbol_inactive_new_symbol_active_same_issuer",
            "recommended_action": "review_possible_rename_or_delisting_in_source_scope",
            "exchange_scope_status": "matches_within_source_scope",
        },
        {
            "change_id": "duplicate",
            "effective_date": "2026-05-02",
            "old_symbol": "AAA",
            "new_symbol": "BBB",
            "source": "stockanalysis_symbol_changes",
            "source_confidence": "secondary_review",
            "review_needed": "true",
            "match_status": "old_and_new_symbols_present",
            "symbol_change_workflow_queue": "review_duplicate_or_cross_listing",
            "review_bucket": "action_required_duplicate_or_cross_listing",
            "review_priority": "P1",
            "recency_bucket": "recent_30d",
            "apply_eligibility": "requires_official_venue_confirmation",
            "verification_evidence_required": "official_exchange_directory_plus_listing_key_review_to_distinguish_duplicate_cross_listing_or_transition",
            "recommended_action": "review_duplicate_or_cross_listing_state_in_source_scope",
            "exchange_scope_status": "matches_within_source_scope",
        },
        {
            "change_id": "reflected",
            "effective_date": "2026-04-01",
            "old_symbol": "CCC",
            "new_symbol": "DDD",
            "source": "stockanalysis_symbol_changes",
            "source_confidence": "secondary_review",
            "review_needed": "true",
            "match_status": "new_symbol_present_old_symbol_missing",
            "symbol_change_workflow_queue": "audit_already_reflected",
            "review_bucket": "already_reflected_in_source_scope",
            "review_priority": "P4",
            "recency_bucket": "recent_90d",
            "apply_eligibility": "audit_only_no_apply",
            "verification_evidence_required": "audit_only_confirm_no_canonical_change_needed",
            "recommended_action": "already_reflected_or_new_symbol_added_in_source_scope",
            "exchange_scope_status": "matches_within_source_scope",
        },
        {
            "change_id": "no_match",
            "effective_date": "2025-01-01",
            "old_symbol": "EEE",
            "new_symbol": "FFF",
            "source": "stockanalysis_symbol_changes",
            "source_confidence": "secondary_review",
            "review_needed": "true",
            "match_status": "no_matching_listing",
            "symbol_change_workflow_queue": "document_no_dataset_match",
            "review_bucket": "no_dataset_match_for_source_scope",
            "review_priority": "P3",
            "recency_bucket": "older_than_90d",
            "apply_eligibility": "no_dataset_action_without_scope_mapping",
            "verification_evidence_required": "official_exchange_scope_mapping_or_ignore_as_external_non_dataset_event",
            "recommended_action": "ignore_or_map_exchange_scope_before_applying",
            "exchange_scope_status": "matches_within_source_scope",
        },
    ]
    strategy_by_queue = {
        "audit_already_reflected": "audit_already_reflected_no_canonical_change",
        "document_no_dataset_match": "document_no_dataset_match_without_canonical_action",
        "review_duplicate_or_cross_listing": "resolve_duplicate_cross_listing_or_transition_before_any_symbol_change",
        "review_verified_rename_or_delisting": "verify_rename_or_delisting_with_official_venue_or_issuer_evidence",
    }
    next_source_by_queue = {
        "audit_already_reflected": "Audit-only confirmation from scoped listing records; no canonical change.",
        "document_no_dataset_match": "Official exchange scope mapping, or document the event as outside the dataset.",
        "review_duplicate_or_cross_listing": "Official exchange directory records plus listing-key review for both symbols.",
        "review_verified_rename_or_delisting": (
            "Official exchange notice, issuer notice, or current exchange directory proving old/new symbols for the same issuer."
        ),
    }
    source_gate_by_queue = {
        "audit_already_reflected": "Audit only; no ticker, listing, or name change is authorized.",
        "document_no_dataset_match": "No dataset action without scoped official mapping to an existing or intended listing.",
        "review_duplicate_or_cross_listing": (
            "Do not change symbols until duplicate, cross-listing, or transition state is resolved listing-key by listing-key."
        ),
        "review_verified_rename_or_delisting": (
            "Do not rename until official listing-keyed evidence proves old inactive and new active for the same issuer."
        ),
    }
    for row in rows:
        queue = row["symbol_change_workflow_queue"]
        if queue == "review_verified_rename_or_delisting":
            row["old_scoped_match_count"] = 1
            row["new_scoped_match_count"] = 0
            row["old_scoped_listing_keys"] = "NYSE::OLD"
            row["new_scoped_listing_keys"] = ""
            row["listing_key_review_status"] = "old_scoped_listing_key_only"
        elif queue == "review_duplicate_or_cross_listing":
            row["old_scoped_match_count"] = 1
            row["new_scoped_match_count"] = 1
            row["old_scoped_listing_keys"] = "NYSE::AAA"
            row["new_scoped_listing_keys"] = "NYSE::BBB"
            row["listing_key_review_status"] = "old_and_new_scoped_listing_keys_present"
        elif queue == "audit_already_reflected":
            row["old_scoped_match_count"] = 0
            row["new_scoped_match_count"] = 1
            row["old_scoped_listing_keys"] = ""
            row["new_scoped_listing_keys"] = "NYSE::DDD"
            row["listing_key_review_status"] = "new_scoped_listing_key_only"
        else:
            row["old_scoped_match_count"] = 0
            row["new_scoped_match_count"] = 0
            row["old_scoped_listing_keys"] = ""
            row["new_scoped_listing_keys"] = ""
            row["listing_key_review_status"] = "no_scoped_listing_key_match"
        row["old_match_count"] = row["old_scoped_match_count"]
        row["new_match_count"] = row["new_scoped_match_count"]
        row["review_strategy"] = strategy_by_queue[queue]
        row["recommended_next_source"] = next_source_by_queue[queue]
        row["source_gate"] = source_gate_by_queue[queue]
        row["issuer_name_review_status"] = "no_scoped_listing_name_available"
        row["source_review_context"] = symbol_change_source_review_context(row)
        row["scope_match_context"] = symbol_change_scope_match_context(row)
        row["workflow_review_context"] = symbol_change_workflow_review_context(row)
    result = evaluate_symbol_change_review_gate(
        {
            "_meta": {
                "generated_at": "2026-05-24T00:00:00Z",
                "source_url": "https://stockanalysis.com/actions/changes/",
                "source_files": {
                    "listings_csv": "data/listings.csv",
                    "changes_csv": "data/corporate_actions/symbol_changes.csv",
                },
                "source_policy": "secondary_review_only",
                "policy": "Secondary symbol-change feed for review only. No ticker, name, listing, or corporate-action data is applied without listing-keyed official exchange or issuer evidence.",
            },
            "summary": {
                "review_rows": 4,
                "match_status_counts": {
                    "new_symbol_present_old_symbol_missing": 1,
                    "no_matching_listing": 1,
                    "old_and_new_symbols_present": 1,
                    "old_symbol_present_new_symbol_missing": 1,
                },
                "symbol_change_workflow_queue_counts": {
                    "audit_already_reflected": 1,
                    "document_no_dataset_match": 1,
                    "review_duplicate_or_cross_listing": 1,
                    "review_verified_rename_or_delisting": 1,
                },
                "symbol_change_backlog": {
                    "status": "listing_keyed_symbol_change_review_queue_open",
                    "rows": 4,
                    "verified_rename_or_delisting_review_rows": 1,
                    "duplicate_or_cross_listing_review_rows": 1,
                    "already_reflected_audit_rows": 1,
                    "out_of_scope_collision_blocked_rows": 0,
                    "missing_source_scope_mapping_rows": 0,
                    "no_dataset_match_documentation_rows": 1,
                    "time_sensitive_review_rows": 2,
                    "direct_symbol_change_apply_allowed_rows": 0,
                    "secondary_feed_apply_authorized": False,
                    "source_gate": (
                        "Symbol-change feed rows are review signals only; ticker, name, listing, or alias changes "
                        "require listing-keyed official venue or issuer evidence for old/new symbols and issuer identity."
                    ),
                },
                "review_bucket_counts": {
                    "action_required_duplicate_or_cross_listing": 1,
                    "action_required_possible_rename_or_delisting": 1,
                    "already_reflected_in_source_scope": 1,
                    "no_dataset_match_for_source_scope": 1,
                },
                "review_priority_counts": {"P1": 2, "P3": 1, "P4": 1},
                "review_bucket_priorities": {
                    "action_required_duplicate_or_cross_listing": "P1",
                    "action_required_possible_rename_or_delisting": "P1",
                    "already_reflected_in_source_scope": "P4",
                    "no_dataset_match_for_source_scope": "P3",
                },
                "recency_bucket_counts": {"older_than_90d": 1, "recent_30d": 2, "recent_90d": 1},
                "review_priority_recency_counts": {
                    "P1:recent_30d": 2,
                    "P3:older_than_90d": 1,
                    "P4:recent_90d": 1,
                },
                "workflow_queue_recency_counts": {
                    "audit_already_reflected:recent_90d": 1,
                    "document_no_dataset_match:older_than_90d": 1,
                    "review_duplicate_or_cross_listing:recent_30d": 1,
                    "review_verified_rename_or_delisting:recent_30d": 1,
                },
                "workflow_queue_priority_counts": {
                    "audit_already_reflected:P4": 1,
                    "document_no_dataset_match:P3": 1,
                    "review_duplicate_or_cross_listing:P1": 1,
                    "review_verified_rename_or_delisting:P1": 1,
                },
                "workflow_queue_scope_counts": {
                    "audit_already_reflected:matches_within_source_scope": 1,
                    "document_no_dataset_match:matches_within_source_scope": 1,
                    "review_duplicate_or_cross_listing:matches_within_source_scope": 1,
                    "review_verified_rename_or_delisting:matches_within_source_scope": 1,
                },
                "workflow_queue_match_status_counts": {
                    "audit_already_reflected:new_symbol_present_old_symbol_missing": 1,
                    "document_no_dataset_match:no_matching_listing": 1,
                    "review_duplicate_or_cross_listing:old_and_new_symbols_present": 1,
                    "review_verified_rename_or_delisting:old_symbol_present_new_symbol_missing": 1,
                },
                "workflow_queue_source_hint_counts": {
                    "audit_already_reflected": {"missing": 1},
                    "document_no_dataset_match": {"missing": 1},
                    "review_duplicate_or_cross_listing": {"missing": 1},
                    "review_verified_rename_or_delisting": {"missing": 1},
                },
                "workflow_queue_source_confidence_counts": {
                    "audit_already_reflected": {"secondary_review": 1},
                    "document_no_dataset_match": {"secondary_review": 1},
                    "review_duplicate_or_cross_listing": {"secondary_review": 1},
                    "review_verified_rename_or_delisting": {"secondary_review": 1},
                },
                "workflow_queue_issuer_name_review_counts": {
                    "audit_already_reflected": {"no_scoped_listing_name_available": 1},
                    "document_no_dataset_match": {"no_scoped_listing_name_available": 1},
                    "review_duplicate_or_cross_listing": {"no_scoped_listing_name_available": 1},
                    "review_verified_rename_or_delisting": {"no_scoped_listing_name_available": 1},
                },
                "workflow_queue_listing_key_review_counts": {
                    "audit_already_reflected": {"new_scoped_listing_key_only": 1},
                    "document_no_dataset_match": {"no_scoped_listing_key_match": 1},
                    "review_duplicate_or_cross_listing": {"old_and_new_scoped_listing_keys_present": 1},
                    "review_verified_rename_or_delisting": {"old_scoped_listing_key_only": 1},
                },
                "workflow_queue_review_strategy_counts": {
                    "audit_already_reflected": {"audit_already_reflected_no_canonical_change": 1},
                    "document_no_dataset_match": {"document_no_dataset_match_without_canonical_action": 1},
                    "review_duplicate_or_cross_listing": {
                        "resolve_duplicate_cross_listing_or_transition_before_any_symbol_change": 1
                    },
                    "review_verified_rename_or_delisting": {
                        "verify_rename_or_delisting_with_official_venue_or_issuer_evidence": 1
                    },
                },
                "top_symbol_change_workflow_batches": [
                    {
                        "symbol_change_workflow_queue": "review_duplicate_or_cross_listing",
                        "review_priority": "P1",
                        "recency_bucket": "recent_30d",
                        "exchange_scope_status": "matches_within_source_scope",
                        "rows": 1,
                        "review_strategy": "resolve_duplicate_cross_listing_or_transition_before_any_symbol_change",
                        "evidence_required": "official_exchange_directory_plus_listing_key_review_to_distinguish_duplicate_cross_listing_or_transition",
                        "recommended_next_source": "Official exchange directory records plus listing-key review for both symbols.",
                        "source_gate": (
                            "Do not change symbols until duplicate, cross-listing, or transition state is resolved "
                            "listing-key by listing-key."
                        ),
                    },
                    {
                        "symbol_change_workflow_queue": "review_verified_rename_or_delisting",
                        "review_priority": "P1",
                        "recency_bucket": "recent_30d",
                        "exchange_scope_status": "matches_within_source_scope",
                        "rows": 1,
                        "review_strategy": "verify_rename_or_delisting_with_official_venue_or_issuer_evidence",
                        "evidence_required": "official_exchange_notice_or_current_directory_showing_old_symbol_inactive_new_symbol_active_same_issuer",
                        "recommended_next_source": (
                            "Official exchange notice, issuer notice, or current exchange directory proving old/new "
                            "symbols for the same issuer."
                        ),
                        "source_gate": (
                            "Do not rename until official listing-keyed evidence proves old inactive and new active "
                            "for the same issuer."
                        ),
                    },
                    {
                        "symbol_change_workflow_queue": "document_no_dataset_match",
                        "review_priority": "P3",
                        "recency_bucket": "older_than_90d",
                        "exchange_scope_status": "matches_within_source_scope",
                        "rows": 1,
                        "review_strategy": "document_no_dataset_match_without_canonical_action",
                        "evidence_required": "official_exchange_scope_mapping_or_ignore_as_external_non_dataset_event",
                        "recommended_next_source": (
                            "Official exchange scope mapping, or document the event as outside the dataset."
                        ),
                        "source_gate": (
                            "No dataset action without scoped official mapping to an existing or intended listing."
                        ),
                    },
                    {
                        "symbol_change_workflow_queue": "audit_already_reflected",
                        "review_priority": "P4",
                        "recency_bucket": "recent_90d",
                        "exchange_scope_status": "matches_within_source_scope",
                        "rows": 1,
                        "review_strategy": "audit_already_reflected_no_canonical_change",
                        "evidence_required": "audit_only_confirm_no_canonical_change_needed",
                        "recommended_next_source": (
                            "Audit-only confirmation from scoped listing records; no canonical change."
                        ),
                        "source_gate": "Audit only; no ticker, listing, or name change is authorized.",
                    },
                ],
                "apply_eligibility_counts": {
                    "audit_only_no_apply": 1,
                    "no_dataset_action_without_scope_mapping": 1,
                    "requires_official_venue_confirmation": 2,
                },
                "symbol_change_apply_readiness_counts": {
                    "audit_only_no_canonical_change": 1,
                    "blocked_until_listing_keyed_official_symbol_change_evidence": 2,
                    "document_or_ignore_until_scoped_official_dataset_match": 1,
                },
                "verification_evidence_required_counts": {
                    "audit_only_confirm_no_canonical_change_needed": 1,
                    "official_exchange_directory_plus_listing_key_review_to_distinguish_duplicate_cross_listing_or_transition": 1,
                    "official_exchange_notice_or_current_directory_showing_old_symbol_inactive_new_symbol_active_same_issuer": 1,
                    "official_exchange_scope_mapping_or_ignore_as_external_non_dataset_event": 1,
                },
                "time_sensitive_workflow_queue_counts": {
                    "review_duplicate_or_cross_listing": 1,
                    "review_verified_rename_or_delisting": 1,
                },
                "time_sensitive_recency_counts": {"recent_30d": 2},
                "time_sensitive_apply_readiness_counts": {
                    "blocked_until_listing_keyed_official_symbol_change_evidence": 2,
                },
                "top_time_sensitive_symbol_change_batches": [
                    {
                        "symbol_change_workflow_queue": "review_duplicate_or_cross_listing",
                        "recency_bucket": "recent_30d",
                        "exchange_scope_status": "matches_within_source_scope",
                        "match_status": "old_and_new_symbols_present",
                        "listing_key_review_status": "old_and_new_scoped_listing_keys_present",
                        "rows": 1,
                        "review_strategy": "resolve_duplicate_cross_listing_or_transition_before_any_symbol_change",
                        "evidence_required": "official_exchange_directory_plus_listing_key_review_to_distinguish_duplicate_cross_listing_or_transition",
                        "recommended_next_source": "Official exchange directory records plus listing-key review for both symbols.",
                        "source_gate": (
                            "Do not change symbols until duplicate, cross-listing, or transition state is resolved "
                            "listing-key by listing-key."
                        ),
                    },
                    {
                        "symbol_change_workflow_queue": "review_verified_rename_or_delisting",
                        "recency_bucket": "recent_30d",
                        "exchange_scope_status": "matches_within_source_scope",
                        "match_status": "old_symbol_present_new_symbol_missing",
                        "listing_key_review_status": "old_scoped_listing_key_only",
                        "rows": 1,
                        "review_strategy": "verify_rename_or_delisting_with_official_venue_or_issuer_evidence",
                        "evidence_required": "official_exchange_notice_or_current_directory_showing_old_symbol_inactive_new_symbol_active_same_issuer",
                        "recommended_next_source": (
                            "Official exchange notice, issuer notice, or current exchange directory proving old/new "
                            "symbols for the same issuer."
                        ),
                        "source_gate": (
                            "Do not rename until official listing-keyed evidence proves old inactive and new active "
                            "for the same issuer."
                        ),
                    },
                ],
                "recommended_action_counts": {
                    "already_reflected_or_new_symbol_added_in_source_scope": 1,
                    "ignore_or_map_exchange_scope_before_applying": 1,
                    "review_duplicate_or_cross_listing_state_in_source_scope": 1,
                    "review_possible_rename_or_delisting_in_source_scope": 1,
                },
                "exchange_scope_status_counts": {"matches_within_source_scope": 4},
            },
            "review_items": rows,
        }
    )

    assert result["passed"] is True
    assert result["rows"] == 4


def test_evaluate_symbol_change_review_gate_fails_for_unsafe_apply_or_stale_summary() -> None:
    result = evaluate_symbol_change_review_gate(
        {
            "_meta": {
                "generated_at": "not-a-timestamp",
                "source_url": "https://example.com/changes/",
                "source_files": {
                    "listings_csv": "data/other.csv",
                    "unexpected_csv": "data/unexpected.csv",
                },
                "source_policy": "auto_apply",
                "policy": "apply symbol changes",
            },
            "summary": {
                "review_rows": 2,
                "match_status_counts": {"old_symbol_present_new_symbol_missing": 2},
                "symbol_change_workflow_queue_counts": {"audit_already_reflected": 2},
                "review_bucket_counts": {"action_required_possible_rename_or_delisting": 2},
                "review_priority_counts": {"P1": 2},
                "review_bucket_priorities": {"action_required_possible_rename_or_delisting": "P1"},
                "recency_bucket_counts": {"recent_7d": 2},
                "review_priority_recency_counts": {"P1:recent_7d": 2},
                "workflow_queue_source_hint_counts": {"audit_already_reflected": {"missing": 1}},
                "workflow_queue_source_confidence_counts": {"audit_already_reflected": {"unreviewed": 1}},
                "apply_eligibility_counts": {"audit_only_no_apply": 2},
                "verification_evidence_required_counts": {"": 2},
                "recommended_action_counts": {"review_possible_rename_or_delisting_in_source_scope": 2},
                "exchange_scope_status_counts": {"matches_within_source_scope": 2},
            },
            "review_items": [
                {
                    "change_id": "bad",
                    "effective_date": "2026-05-01",
                    "old_symbol": "OLD",
                    "new_symbol": "NEW",
                    "source": "stockanalysis_symbol_changes",
                    "source_confidence": "unreviewed",
                    "review_needed": "false",
                    "match_status": "old_symbol_present_new_symbol_missing",
                    "symbol_change_workflow_queue": "audit_already_reflected",
                    "review_bucket": "action_required_possible_rename_or_delisting",
                    "review_priority": "P1",
                    "review_strategy": "audit_already_reflected_no_canonical_change",
                    "recency_bucket": "recent_7d",
                    "apply_eligibility": "audit_only_no_apply",
                    "verification_evidence_required": "",
                    "recommended_action": "review_possible_rename_or_delisting_in_source_scope",
                    "exchange_scope_status": "matches_within_source_scope",
                    "issuer_name_review_status": "no_scoped_listing_name_available",
                }
            ],
        }
    )

    assert result["passed"] is False
    assert result["invalid_generated_at"] == "not-a-timestamp"
    assert result["source_url_mismatch"] == {
        "expected": "https://stockanalysis.com/actions/changes/",
        "actual": "https://example.com/changes/",
    }
    assert result["source_policy_mismatch"] == {
        "expected": "secondary_review_only",
        "actual": "auto_apply",
    }
    assert result["source_file_mismatches"] == {
        "listings_csv": {"expected": "data/listings.csv", "actual": "data/other.csv"},
        "changes_csv": {
            "expected": "data/corporate_actions/symbol_changes.csv",
            "actual": None,
        },
    }
    assert result["unexpected_source_files"] == ["unexpected_csv"]
    assert result["policy_missing_markers"] == [
        "review only",
        "No ticker",
        "listing-keyed official exchange or issuer evidence",
    ]
    assert result["row_count_mismatch"] == {"reported": 2, "actual": 1}
    assert result["summary_mismatches"]["apply_eligibility_counts"] == {
        "audit_only_no_apply": {"reported": 2, "actual": 1},
    }
    assert set(result["row_gaps"][0]["invalid_fields"]) == {
        "source_confidence",
            "review_needed",
            "symbol_change_workflow_queue",
            "p1_apply_eligibility",
            "listing_key_review_status",
            "verification_evidence_required",
        "recommended_next_source",
        "source_gate",
        "source_review_context",
        "scope_match_context",
        "workflow_review_context",
    }
    assert result["missing_distinctions"] == [
        "already_reflected",
        "duplicate_or_cross_listing",
        "no_match_or_scope_gap",
    ]


def test_evaluate_ohlcv_plausibility_gate_requires_sampling_policy_and_matching_summaries() -> None:
    rows = [
        {
            "listing_key": "NASDAQ::WARN",
            "ticker": "WARN",
            "exchange": "NASDAQ",
            "name": "Warn Corp",
            "isin": "US0000000001",
            "entry_quality_status": "warn",
            "selection_bucket": "entry_quality_warn",
            "plausibility_status": "not_checked",
            "plausibility_score": 0,
            "review_bucket": "not_checked_entry_quality_warn_sample",
            "review_priority": "P2",
            "sampling_strategy": "collect_ohlcv_sample_then_existing_entry_quality_review",
            "plausibility_use": "sampling_queue_for_existing_entry_quality_warn",
            "verification_evidence_required": "local_or_bounded_network_ohlcv_sample_then_existing_entry_quality_review",
            "recommended_next_source": (
                "Collect a local or bounded-network OHLCV sample for NASDAQ, then review the existing entry-quality warning."
            ),
            "source_gate": (
                "Sampling can prioritize review, but entry-quality changes still require the existing official evidence gates."
            ),
            "ohlcv_symbol": "WARN",
            "recommended_action": "provide_local_ohlcv_or_run_fetch_yahoo_sample",
            "issues": [{"issue_type": "no_ohlcv_sample"}],
        },
        {
            "listing_key": "NYSE::GAP",
            "ticker": "GAP",
            "exchange": "NYSE",
            "name": "Gap Corp",
            "isin": "",
            "entry_quality_status": "source_gap",
            "source_gap_class": "official_identifier_not_exposed_source_gap",
            "selection_bucket": "source_gap:official_identifier_not_exposed_source_gap",
            "plausibility_status": "not_checked",
            "plausibility_score": 0,
            "review_bucket": "not_checked_source_gap_cluster_sample",
            "review_priority": "P3",
            "sampling_strategy": "collect_ohlcv_sample_then_source_gap_review",
            "plausibility_use": "sampling_queue_for_existing_source_gap",
            "verification_evidence_required": "local_or_bounded_network_ohlcv_sample_then_source_gap_review",
            "recommended_next_source": (
                "Collect a local or bounded-network OHLCV sample for NYSE, then use it only as source-gap review context."
            ),
            "source_gate": (
                "Sampling can prioritize source-gap review, but cannot fill identifiers, sectors, categories, names, or symbols."
            ),
            "ohlcv_symbol": "GAP",
            "recommended_action": "provide_local_ohlcv_or_run_fetch_yahoo_sample",
            "issues": [{"issue_type": "no_ohlcv_sample"}],
        },
        {
            "listing_key": "LSE::BASE",
            "ticker": "BASE",
            "exchange": "LSE",
            "name": "Baseline plc",
            "isin": "GB0000000002",
            "entry_quality_status": "pass",
            "source_gap_class": "",
            "selection_bucket": "large_exchange:LSE",
            "plausibility_status": "pass",
            "plausibility_score": 100,
            "review_bucket": "checked_plausible_sample",
            "review_priority": "P4",
            "sampling_strategy": "retain_as_plausibility_baseline_no_data_change",
            "plausibility_use": "market_data_plausibility_evidence_only",
            "verification_evidence_required": "none_no_database_change_authorized",
            "recommended_next_source": "Retain sampled OHLCV evidence for LSE as a plausibility baseline only.",
            "source_gate": "Plausible OHLCV sample is baseline evidence only; no database change is authorized.",
            "ohlcv_symbol": "BASE.L",
            "recommended_action": "none",
            "issues": [],
        },
    ]
    for row in rows:
        row["bar_count"] = row.get("bar_count", 0)
        row["first_bar_date"] = row.get("first_bar_date", "")
        row["last_bar_date"] = row.get("last_bar_date", "")
        row["ohlcv_source"] = row.get("ohlcv_source", "")
        row["source_gap_field"] = row.get("source_gap_field", "")
        row["source_gap_class"] = row.get("source_gap_class", "")
        row["selection_context"] = ohlcv_selection_context(row)
        row["ohlcv_sample_context"] = ohlcv_sample_context(row)
        row["plausibility_review_context"] = ohlcv_plausibility_review_context(row)
    result = evaluate_ohlcv_plausibility_gate(
        {
            "_meta": {
                "generated_at": "2026-05-24T00:00:00Z",
                "rows": 3,
                "selected_rows": 3,
                "skipped_not_checked_rows": 0,
                "source_files": {
                    "listings_csv": "data/listings.csv",
                    "entry_quality_csv": "data/reports/entry_quality.csv",
                    "source_gap_classification_csv": "data/reports/source_gap_classification.csv",
                },
                "policy": "Plausibility sampling only. OHLCV signals never authorize identifier, sector, category, name, listing, or symbol-change updates without official listing-keyed review evidence.",
                "parameters": {"fetch_yahoo": False, "include_not_checked": True},
            },
            "summary": {
                "status_counts": {"not_checked": 2, "pass": 1},
                "issue_counts": {"no_ohlcv_sample": 2},
                "selection_bucket_counts": {
                    "entry_quality_warn": 1,
                    "large_exchange:LSE": 1,
                    "source_gap:official_identifier_not_exposed_source_gap": 1,
                },
                "selection_bucket_exchange_counts": {
                    "entry_quality_warn": {"NASDAQ": 1},
                    "large_exchange:LSE": {"LSE": 1},
                    "source_gap:official_identifier_not_exposed_source_gap": {"NYSE": 1},
                },
                "selection_bucket_status_counts": {
                    "entry_quality_warn": {"not_checked": 1},
                    "large_exchange:LSE": {"pass": 1},
                    "source_gap:official_identifier_not_exposed_source_gap": {"not_checked": 1},
                },
                "review_bucket_counts": {
                    "checked_plausible_sample": 1,
                    "not_checked_entry_quality_warn_sample": 1,
                    "not_checked_source_gap_cluster_sample": 1,
                },
                "review_bucket_selection_bucket_counts": {
                    "checked_plausible_sample": {"large_exchange:LSE": 1},
                    "not_checked_entry_quality_warn_sample": {"entry_quality_warn": 1},
                    "not_checked_source_gap_cluster_sample": {
                        "source_gap:official_identifier_not_exposed_source_gap": 1
                    },
                },
                "review_bucket_exchange_counts": {
                    "checked_plausible_sample": {"LSE": 1},
                    "not_checked_entry_quality_warn_sample": {"NASDAQ": 1},
                    "not_checked_source_gap_cluster_sample": {"NYSE": 1},
                },
                "review_bucket_sampling_strategy_counts": {
                    "checked_plausible_sample": {"retain_as_plausibility_baseline_no_data_change": 1},
                    "not_checked_entry_quality_warn_sample": {
                        "collect_ohlcv_sample_then_existing_entry_quality_review": 1
                    },
                    "not_checked_source_gap_cluster_sample": {
                        "collect_ohlcv_sample_then_source_gap_review": 1
                    },
                },
                "review_bucket_sampling_readiness_counts": {
                    "checked_plausible_sample": {"checked_without_source": 1},
                    "not_checked_entry_quality_warn_sample": {"needs_ohlcv_sample": 1},
                    "not_checked_source_gap_cluster_sample": {"needs_ohlcv_sample": 1},
                },
                "top_ohlcv_sampling_batches": [
                    {
                        "review_bucket": "not_checked_entry_quality_warn_sample",
                        "selection_bucket": "entry_quality_warn",
                        "exchange": "NASDAQ",
                        "plausibility_status": "not_checked",
                        "rows": 1,
                        "review_priority": "P2",
                        "sampling_strategy": "collect_ohlcv_sample_then_existing_entry_quality_review",
                        "evidence_required": "local_or_bounded_network_ohlcv_sample_then_existing_entry_quality_review",
                        "recommended_next_source": (
                            "Collect a local or bounded-network OHLCV sample for NASDAQ, then review the existing "
                            "entry-quality warning."
                        ),
                        "source_gate": (
                            "Sampling can prioritize review, but entry-quality changes still require the existing "
                            "official evidence gates."
                        ),
                    }
                ],
                "review_priority_counts": {"P2": 1, "P3": 1, "P4": 1},
                "plausibility_use_counts": {
                    "market_data_plausibility_evidence_only": 1,
                    "sampling_queue_for_existing_entry_quality_warn": 1,
                    "sampling_queue_for_existing_source_gap": 1,
                },
                "canonical_data_change_authorization_counts": {
                    "no_canonical_data_change_authorized": 3,
                },
                "verification_evidence_required_counts": {
                    "local_or_bounded_network_ohlcv_sample_then_existing_entry_quality_review": 1,
                    "local_or_bounded_network_ohlcv_sample_then_source_gap_review": 1,
                    "none_no_database_change_authorized": 1,
                },
                "source_gap_class_counts": {"official_identifier_not_exposed_source_gap": 1},
                "sampling_coverage": {
                    "selected_rows": 3,
                    "report_rows": 3,
                    "checked_rows": 1,
                    "not_checked_rows": 2,
                    "skipped_not_checked_rows": 0,
                    "local_sample_rows": 0,
                    "yahoo_sample_rows": 1,
                    "warn_or_source_gap_signal_rows": 0,
                },
                "ohlcv_sampling_backlog": {
                    "status": "sampling_queue_enabled_plausibility_only",
                    "selected_rows": 3,
                    "report_rows": 3,
                    "checked_rows": 1,
                    "not_checked_rows": 2,
                    "source_gap_cluster_sample_rows": 1,
                    "entry_quality_warn_sample_rows": 1,
                    "large_exchange_baseline_sample_rows": 1,
                    "warn_or_source_gap_signal_rows": 0,
                    "direct_canonical_data_change_allowed_rows": 0,
                    "plausibility_signal_only": True,
                    "source_gate": (
                        "OHLCV sampling is plausibility evidence only; identifiers, sectors, categories, names, "
                        "listings, and symbols remain blocked until official listing-keyed review evidence is available."
                    ),
                },
                "top_flagged_exchanges": [{"exchange": "NYSE", "not_checked": 1}],
            },
            "review_items": rows,
            "flagged_items": [],
        }
    )

    assert result["passed"] is True
    assert result["rows"] == 3


def test_evaluate_ohlcv_plausibility_gate_fails_for_auto_change_action_or_stale_summary() -> None:
    result = evaluate_ohlcv_plausibility_gate(
        {
            "_meta": {
                "generated_at": "not-a-timestamp",
                "rows": 2,
                "selected_rows": 1,
                "skipped_not_checked_rows": 0,
                "source_files": {
                    "listings_csv": "data/other.csv",
                    "unexpected_csv": "data/unexpected.csv",
                },
                "policy": "Market data report.",
                "parameters": {"fetch_yahoo": False},
            },
            "summary": {
                "status_counts": {"not_checked": 2},
                "issue_counts": {"no_ohlcv_sample": 2},
                "selection_bucket_counts": {"entry_quality_warn": 2},
                "review_bucket_counts": {"not_checked_entry_quality_warn_sample": 2},
                "review_priority_counts": {"P2": 2},
                "plausibility_use_counts": {"sampling_queue_for_existing_entry_quality_warn": 2},
                "canonical_data_change_authorization_counts": {"no_canonical_data_change_authorized": 2},
                "verification_evidence_required_counts": {"none_no_database_change_authorized": 2},
                "source_gap_class_counts": {},
                "top_flagged_exchanges": [{"exchange": "", "not_checked": -1}],
            },
            "review_items": [
                {
                    "listing_key": "NASDAQ::BAD",
                    "ticker": "BAD",
                    "exchange": "NASDAQ",
                    "entry_quality_status": "warn",
                    "selection_bucket": "entry_quality_warn",
                    "plausibility_status": "not_checked",
                    "plausibility_score": 101,
                    "review_bucket": "not_checked_entry_quality_warn_sample",
                    "review_priority": "P2",
                    "sampling_strategy": "fill_isin_from_ohlcv",
                    "plausibility_use": "sampling_queue_for_existing_entry_quality_warn",
                    "verification_evidence_required": "none_no_database_change_authorized",
                    "ohlcv_symbol": "BAD",
                    "recommended_action": "fill_isin_from_ohlcv_sample",
                    "issues": [],
                }
            ],
        }
    )

    assert result["passed"] is False
    assert result["invalid_generated_at"] == "not-a-timestamp"
    assert result["source_file_mismatches"] == {
        "listings_csv": {"expected": "data/listings.csv", "actual": "data/other.csv"},
        "entry_quality_csv": {"expected": "data/reports/entry_quality.csv", "actual": None},
        "source_gap_classification_csv": {
            "expected": "data/reports/source_gap_classification.csv",
            "actual": None,
        },
    }
    assert result["unexpected_source_files"] == ["unexpected_csv"]
    assert result["row_count_mismatch"] == {"reported": 2, "actual": 1}
    assert result["row_gaps"][0]["invalid_fields"] == [
        "plausibility_score",
        "sampling_strategy",
        "verification_evidence_required",
        "recommended_next_source",
        "source_gate",
        "selection_context",
        "ohlcv_sample_context",
        "plausibility_review_context",
        "issues",
    ]
    assert result["forbidden_action_rows"][0]["forbidden_markers"] == ["fill_isin"]
    assert result["missing_policy_markers"] == ["plausibility", "never authorize", "official", "listing-keyed"]
    assert result["missing_sampling_buckets"] == ["source_gap:", "large_exchange:"]
    assert result["invalid_top_flagged_exchanges"] is True


def test_evaluate_masterfile_collision_gate_requires_non_symbol_identity_review() -> None:
    rows = [
        {
            "target_listing_key": "AMS::2AAP",
            "ticker": "2AAP",
            "target_exchange": "AMS",
            "official_name": "Leverage Shares 2x Apple ETP Securities",
            "official_asset_type": "ETF",
            "official_isin": "XS2337099563",
            "official_source_key": "euronext_etfs",
            "existing_listing_keys": "LSE::2AAP",
            "existing_exchanges": "LSE",
            "identity_evidence": "official_isin|same_isin_existing_listing|exact_name_match|asset_type_consistent",
            "collision_risk_flags": "ticker_reused_on_other_exchange|same_isin_existing_listing|exact_name_match",
            "identity_resolution_queue": "review_cross_listing_same_isin_exact_name",
            "review_bucket": "same_isin_exact_name_cross_listing_candidate",
            "review_priority": "P1",
            "review_decision": "same_isin_cross_listing_candidate_requires_exchange_scope_review",
            "clearance_evidence_required": "official_target_exchange_listing_status_mic_name_instrument_type",
            "recommended_next_action": "verify_target_exchange_listing_status_mic_name_and_instrument_type_before_cross_listing_add",
            "asset_type_mismatch": "false",
        },
        {
            "target_listing_key": "BATS::FOO",
            "ticker": "FOO",
            "target_exchange": "BATS",
            "official_name": "Foo Europe PLC",
            "official_asset_type": "Stock",
            "official_isin": "IE0000000002",
            "official_source_key": "cboe_bats",
            "existing_listing_keys": "NASDAQ::FOO",
            "existing_exchanges": "NASDAQ",
            "identity_evidence": "official_isin|asset_type_consistent",
            "collision_risk_flags": "ticker_reused_on_other_exchange|existing_isin_conflict|no_exact_name_match",
            "identity_resolution_queue": "review_distinct_official_isin_new_listing",
            "review_bucket": "distinct_official_isin_new_listing_candidate",
            "review_priority": "P2",
            "review_decision": "new_listing_candidate_requires_official_listing_add_review",
            "clearance_evidence_required": "official_target_exchange_listing_key_isin_name_instrument_type_listing_status",
            "recommended_next_action": "add_only_after_official_listing_key_isin_name_exchange_and_scope_review",
            "asset_type_mismatch": "false",
        },
        {
            "target_listing_key": "ADX::ADIB",
            "ticker": "ADIB",
            "target_exchange": "ADX",
            "official_name": "Abu Dhabi Islamic Bank PJSC",
            "official_asset_type": "Stock",
            "official_isin": "",
            "official_source_key": "adx_market_watch",
            "existing_listing_keys": "EGX::ADIB",
            "existing_exchanges": "EGX",
            "identity_evidence": "asset_type_consistent",
            "collision_risk_flags": "ticker_reused_on_other_exchange|missing_official_isin|no_exact_name_match",
            "identity_resolution_queue": "blocked_symbol_only_missing_non_symbol_identity",
            "review_bucket": "hold_symbol_only_collision_needs_non_symbol_identity",
            "review_priority": "P3",
            "review_decision": "symbol_collision_requires_non_symbol_identity_source",
            "clearance_evidence_required": "official_non_symbol_identifier_or_keep_absent",
            "recommended_next_action": "keep_absent_until_official_isin_or_other_non_symbol_identity_source_distinguishes_listing",
            "asset_type_mismatch": "false",
        },
        {
            "target_listing_key": "PAR::BAR",
            "ticker": "BAR",
            "target_exchange": "PAR",
            "official_name": "Bar SA",
            "official_asset_type": "Stock",
            "official_isin": "FR0000000001",
            "official_source_key": "euronext_equities",
            "existing_listing_keys": "MIL::BAR",
            "existing_exchanges": "MIL",
            "identity_evidence": "official_isin|same_isin_existing_listing|asset_type_consistent",
            "collision_risk_flags": "ticker_reused_on_other_exchange|same_isin_existing_listing|no_exact_name_match",
            "identity_resolution_queue": "review_cross_listing_same_isin_name_or_scope_gap",
            "review_bucket": "same_isin_cross_listing_needs_name_or_scope_review",
            "review_priority": "P2",
            "review_decision": "same_isin_cross_listing_candidate_requires_exchange_scope_review",
            "clearance_evidence_required": "official_target_exchange_listing_status_plus_name_or_scope_reconciliation",
            "recommended_next_action": "verify_target_exchange_listing_status_mic_name_and_instrument_type_before_cross_listing_add",
            "asset_type_mismatch": "false",
        },
        {
            "target_listing_key": "BATS::AV",
            "ticker": "AV",
            "target_exchange": "BATS",
            "official_name": "Aviva ETF",
            "official_asset_type": "ETF",
            "official_isin": "",
            "official_source_key": "nasdaq_other_listed",
            "existing_listing_keys": "LSE::AV",
            "existing_exchanges": "LSE",
            "identity_evidence": "asset_type_conflict",
            "collision_risk_flags": "ticker_reused_on_other_exchange|missing_official_isin|no_exact_name_match|asset_type_mismatch",
            "identity_resolution_queue": "blocked_asset_type_conflict",
            "review_bucket": "resolve_asset_type_conflict_before_identity_review",
            "review_priority": "P2",
            "review_decision": "symbol_collision_requires_non_symbol_identity_source",
            "clearance_evidence_required": "official_instrument_type_resolution_before_listing_identity_review",
            "recommended_next_action": "resolve_instrument_type_before_any_listing_addition",
            "asset_type_mismatch": "true",
        },
    ]
    strategy_by_queue = {
        "blocked_asset_type_conflict": "batch_block_instrument_type_conflict_until_official_resolution",
        "blocked_symbol_only_missing_non_symbol_identity": "batch_hold_symbol_reuse_until_non_symbol_identity_source",
        "review_cross_listing_same_isin_exact_name": "batch_review_same_isin_exact_name_cross_listing_scope",
        "review_cross_listing_same_isin_name_or_scope_gap": "batch_review_same_isin_name_or_scope_reconciliation",
        "review_distinct_official_isin_new_listing": "batch_review_distinct_isin_new_listing_candidates",
    }
    evidence_by_queue = {
        "blocked_asset_type_conflict": "official_instrument_type_resolution_before_listing_identity_review",
        "blocked_symbol_only_missing_non_symbol_identity": "official_non_symbol_identifier_or_keep_absent",
        "review_cross_listing_same_isin_exact_name": "official_target_and_existing_exchange_directories_confirm_active_same_instrument",
        "review_cross_listing_same_isin_name_or_scope_gap": "official_target_exchange_status_plus_issuer_or_name_scope_reconciliation",
        "review_distinct_official_isin_new_listing": "official_target_exchange_listing_key_isin_name_instrument_type_listing_status",
    }
    for row in rows:
        queue = row["identity_resolution_queue"]
        row["review_strategy"] = strategy_by_queue[queue]
        row["verification_evidence_required"] = evidence_by_queue[queue]
        exchange_context = collision_exchange_context(row["target_exchange"], row["existing_exchanges"])
        row["recommended_next_source"] = collision_recommended_next_source(queue, exchange_context)
        row["source_gate"] = collision_source_gate(queue)
        row["official_source_context"] = collision_official_source_context(row)
        row["existing_dataset_context"] = collision_existing_dataset_context(row)
        row["identity_resolution_context"] = collision_identity_resolution_context(row)
    result = evaluate_masterfile_collision_gate(
        {
            "_meta": {
                "generated_at": "2026-05-24T00:00:00Z",
                "source_files": {
                    "listings_csv": "data/listings.csv",
                    "masterfile_reference_csv": "data/masterfiles/reference.csv",
                },
                "policy": {
                    "no_symbol_only_additions": "symbol equality is not identity evidence",
                    "truth_required": "official target-exchange evidence required",
                    "blank_until_verified": "keep absent until verified",
                },
            },
            "summary": {
                "rows": 5,
                "decision_totals": {
                    "new_listing_candidate_requires_official_listing_add_review": 1,
                    "same_isin_cross_listing_candidate_requires_exchange_scope_review": 2,
                    "symbol_collision_requires_non_symbol_identity_source": 2,
                },
                "review_bucket_totals": {
                    "distinct_official_isin_new_listing_candidate": 1,
                    "hold_symbol_only_collision_needs_non_symbol_identity": 1,
                    "resolve_asset_type_conflict_before_identity_review": 1,
                    "same_isin_cross_listing_needs_name_or_scope_review": 1,
                    "same_isin_exact_name_cross_listing_candidate": 1,
                },
                "review_priority_totals": {"P1": 1, "P2": 3, "P3": 1},
                "collision_risk_flag_totals": {
                    "asset_type_mismatch": 1,
                    "exact_name_match": 1,
                    "existing_isin_conflict": 1,
                    "missing_official_isin": 2,
                    "no_exact_name_match": 4,
                    "same_isin_existing_listing": 2,
                    "ticker_reused_on_other_exchange": 5,
                },
                "identity_resolution_queue_totals": {
                    "blocked_asset_type_conflict": 1,
                    "blocked_symbol_only_missing_non_symbol_identity": 1,
                    "review_cross_listing_same_isin_exact_name": 1,
                    "review_cross_listing_same_isin_name_or_scope_gap": 1,
                    "review_distinct_official_isin_new_listing": 1,
                },
                "identity_resolution_risk_flag_totals": {
                    "blocked_asset_type_conflict": {
                        "asset_type_mismatch": 1,
                        "missing_official_isin": 1,
                        "no_exact_name_match": 1,
                        "ticker_reused_on_other_exchange": 1,
                    },
                    "blocked_symbol_only_missing_non_symbol_identity": {
                        "missing_official_isin": 1,
                        "no_exact_name_match": 1,
                        "ticker_reused_on_other_exchange": 1,
                    },
                    "review_cross_listing_same_isin_exact_name": {
                        "exact_name_match": 1,
                        "same_isin_existing_listing": 1,
                        "ticker_reused_on_other_exchange": 1,
                    },
                    "review_cross_listing_same_isin_name_or_scope_gap": {
                        "no_exact_name_match": 1,
                        "same_isin_existing_listing": 1,
                        "ticker_reused_on_other_exchange": 1,
                    },
                    "review_distinct_official_isin_new_listing": {
                        "existing_isin_conflict": 1,
                        "no_exact_name_match": 1,
                        "ticker_reused_on_other_exchange": 1,
                    },
                },
                "identity_resolution_exchange_totals": {
                    "blocked_asset_type_conflict": {"BATS": 1},
                    "blocked_symbol_only_missing_non_symbol_identity": {"ADX": 1},
                    "review_cross_listing_same_isin_exact_name": {"AMS": 1},
                    "review_cross_listing_same_isin_name_or_scope_gap": {"PAR": 1},
                    "review_distinct_official_isin_new_listing": {"BATS": 1},
                },
                "identity_resolution_asset_type_totals": {
                    "blocked_asset_type_conflict": {"ETF": 1},
                    "blocked_symbol_only_missing_non_symbol_identity": {"Stock": 1},
                    "review_cross_listing_same_isin_exact_name": {"ETF": 1},
                    "review_cross_listing_same_isin_name_or_scope_gap": {"Stock": 1},
                    "review_distinct_official_isin_new_listing": {"Stock": 1},
                },
                "identity_resolution_official_source_totals": {
                    "blocked_asset_type_conflict": {"nasdaq_other_listed": 1},
                    "blocked_symbol_only_missing_non_symbol_identity": {"adx_market_watch": 1},
                    "review_cross_listing_same_isin_exact_name": {"euronext_etfs": 1},
                    "review_cross_listing_same_isin_name_or_scope_gap": {"euronext_equities": 1},
                    "review_distinct_official_isin_new_listing": {"cboe_bats": 1},
                },
                "identity_resolution_existing_exchange_pair_totals": {
                    "blocked_asset_type_conflict": {"BATS::LSE": 1},
                    "blocked_symbol_only_missing_non_symbol_identity": {"ADX::EGX": 1},
                    "review_cross_listing_same_isin_exact_name": {"AMS::LSE": 1},
                    "review_cross_listing_same_isin_name_or_scope_gap": {"PAR::MIL": 1},
                    "review_distinct_official_isin_new_listing": {"BATS::NASDAQ": 1},
                },
                "identity_resolution_pair_review_strategy_totals": {
                    "blocked_asset_type_conflict": {
                        "batch_block_instrument_type_conflict_until_official_resolution": 1
                    },
                    "blocked_symbol_only_missing_non_symbol_identity": {
                        "batch_hold_symbol_reuse_until_non_symbol_identity_source": 1
                    },
                    "review_cross_listing_same_isin_exact_name": {
                        "batch_review_same_isin_exact_name_cross_listing_scope": 1
                    },
                    "review_cross_listing_same_isin_name_or_scope_gap": {
                        "batch_review_same_isin_name_or_scope_reconciliation": 1
                    },
                    "review_distinct_official_isin_new_listing": {
                        "batch_review_distinct_isin_new_listing_candidates": 1
                    },
                },
                "identity_resolution_review_strategy_totals": {
                    "blocked_asset_type_conflict": {
                        "batch_block_instrument_type_conflict_until_official_resolution": 1
                    },
                    "blocked_symbol_only_missing_non_symbol_identity": {
                        "batch_hold_symbol_reuse_until_non_symbol_identity_source": 1
                    },
                    "review_cross_listing_same_isin_exact_name": {
                        "batch_review_same_isin_exact_name_cross_listing_scope": 1
                    },
                    "review_cross_listing_same_isin_name_or_scope_gap": {
                        "batch_review_same_isin_name_or_scope_reconciliation": 1
                    },
                    "review_distinct_official_isin_new_listing": {
                        "batch_review_distinct_isin_new_listing_candidates": 1
                    },
                },
                "identity_resolution_evidence_required_totals": {
                    "blocked_asset_type_conflict": {
                        "official_instrument_type_resolution_before_listing_identity_review": 1
                    },
                    "blocked_symbol_only_missing_non_symbol_identity": {
                        "official_non_symbol_identifier_or_keep_absent": 1
                    },
                    "review_cross_listing_same_isin_exact_name": {
                        "official_target_and_existing_exchange_directories_confirm_active_same_instrument": 1
                    },
                    "review_cross_listing_same_isin_name_or_scope_gap": {
                        "official_target_exchange_status_plus_issuer_or_name_scope_reconciliation": 1
                    },
                    "review_distinct_official_isin_new_listing": {
                        "official_target_exchange_listing_key_isin_name_instrument_type_listing_status": 1
                    },
                },
                "identity_resolution_identity_evidence_totals": {
                    "blocked_asset_type_conflict": {"asset_type_conflict": 1},
                    "blocked_symbol_only_missing_non_symbol_identity": {"asset_type_consistent": 1},
                    "review_cross_listing_same_isin_exact_name": {
                        "asset_type_consistent": 1,
                        "exact_name_match": 1,
                        "official_isin": 1,
                        "same_isin_existing_listing": 1,
                    },
                    "review_cross_listing_same_isin_name_or_scope_gap": {
                        "asset_type_consistent": 1,
                        "official_isin": 1,
                        "same_isin_existing_listing": 1,
                    },
                    "review_distinct_official_isin_new_listing": {
                        "asset_type_consistent": 1,
                        "official_isin": 1,
                    },
                },
                "identity_resolution_clearance_readiness_totals": {
                    "blocked_symbol_only_non_symbol_identity_required": 1,
                    "blocked_until_asset_type_conflict_resolved": 1,
                    "needs_official_listing_add_review": 1,
                    "needs_official_name_or_scope_reconciliation": 1,
                    "review_ready_same_isin_exact_name_scope_check": 1,
                },
                "identity_resolution_queue_clearance_readiness_totals": {
                    "blocked_asset_type_conflict": {"blocked_until_asset_type_conflict_resolved": 1},
                    "blocked_symbol_only_missing_non_symbol_identity": {
                        "blocked_symbol_only_non_symbol_identity_required": 1
                    },
                    "review_cross_listing_same_isin_exact_name": {
                        "review_ready_same_isin_exact_name_scope_check": 1
                    },
                    "review_cross_listing_same_isin_name_or_scope_gap": {
                        "needs_official_name_or_scope_reconciliation": 1
                    },
                    "review_distinct_official_isin_new_listing": {"needs_official_listing_add_review": 1},
                },
                "top_identity_resolution_clearance_batches": [
                    {
                        "identity_resolution_queue": "review_cross_listing_same_isin_exact_name",
                        "clearance_readiness": "review_ready_same_isin_exact_name_scope_check",
                        "rows": 1,
                        "review_strategy": "batch_review_same_isin_exact_name_cross_listing_scope",
                        "evidence_required": "official_target_and_existing_exchange_directories_confirm_active_same_instrument",
                        "recommended_next_source": "Official active-listing directories for both exchanges in target::existing.",
                        "source_gate": (
                            "Do not add or merge until both official exchange directories confirm the same active instrument."
                        ),
                    }
                ],
                "top_identity_resolution_pair_review_batches": [
                    {
                        "identity_resolution_queue": "review_cross_listing_same_isin_exact_name",
                        "exchange_pair": "AMS::LSE",
                        "rows": 1,
                        "review_strategy": "batch_review_same_isin_exact_name_cross_listing_scope",
                        "evidence_required": "official_target_and_existing_exchange_directories_confirm_active_same_instrument",
                        "recommended_next_source": "Official active-listing directories for both exchanges in AMS::LSE.",
                        "source_gate": (
                            "Do not add or merge until both official exchange directories confirm the same active instrument."
                        ),
                    }
                ],
                "same_isin_exact_name_scope_review_rows": 1,
                "top_same_isin_exact_name_scope_review_batches": [
                    {
                        "exchange_pair": "AMS::LSE",
                        "official_source_key": "euronext_etfs",
                        "official_asset_type": "ETF",
                        "clearance_evidence_required": "official_target_exchange_listing_status_mic_name_instrument_type",
                        "rows": 1,
                        "review_strategy": "batch_review_same_isin_exact_name_cross_listing_scope",
                        "evidence_required": "official_target_and_existing_exchange_directories_confirm_active_same_instrument",
                        "recommended_next_source": "Official active-listing directories for both exchanges in AMS::LSE.",
                        "source_gate": (
                            "Do not add or merge until both official exchange directories confirm the same active instrument."
                        ),
                    }
                ],
                "clearance_evidence_totals": {
                    "official_instrument_type_resolution_before_listing_identity_review": 1,
                    "official_non_symbol_identifier_or_keep_absent": 1,
                    "official_target_exchange_listing_key_isin_name_instrument_type_listing_status": 1,
                    "official_target_exchange_listing_status_mic_name_instrument_type": 1,
                    "official_target_exchange_listing_status_plus_name_or_scope_reconciliation": 1,
                },
                "exchange_totals": {"ADX": 1, "AMS": 1, "BATS": 2, "PAR": 1},
                "official_asset_type_totals": {"ETF": 2, "Stock": 3},
                "asset_type_mismatch_totals": {"false": 4, "true": 1},
                "official_source_totals": {
                    "adx_market_watch": 1,
                    "cboe_bats": 1,
                    "euronext_equities": 1,
                    "euronext_etfs": 1,
                    "nasdaq_other_listed": 1,
                },
                "policy": {
                    "no_symbol_only_additions": "symbol equality is not identity evidence",
                    "truth_required": "official target-exchange evidence required",
                    "blank_until_verified": "keep absent until verified",
                },
            },
            "rows": rows,
        }
    )

    assert result["passed"] is True
    assert result["rows"] == 5


def test_evaluate_masterfile_collision_gate_fails_for_symbol_only_addition_or_stale_summary() -> None:
    result = evaluate_masterfile_collision_gate(
        {
            "_meta": {
                "generated_at": "not-a-timestamp",
                "source_files": {
                    "listings_csv": "data/other.csv",
                    "unexpected_csv": "data/unexpected.csv",
                },
                "policy": {"truth_required": "different policy"},
            },
            "summary": {
                "rows": 2,
                "decision_totals": {"new_listing_candidate_requires_official_listing_add_review": 2},
                "review_bucket_totals": {"distinct_official_isin_new_listing_candidate": 2},
                "review_priority_totals": {"P2": 2},
                "collision_risk_flag_totals": {"ticker_reused_on_other_exchange": 2},
                "identity_resolution_queue_totals": {"review_distinct_official_isin_new_listing": 2},
                "identity_resolution_risk_flag_totals": {
                    "review_distinct_official_isin_new_listing": {"ticker_reused_on_other_exchange": 2}
                },
                "identity_resolution_exchange_totals": {"review_distinct_official_isin_new_listing": {"ADX": 2}},
                "identity_resolution_asset_type_totals": {"review_distinct_official_isin_new_listing": {"Stock": 2}},
                "identity_resolution_official_source_totals": {
                    "review_distinct_official_isin_new_listing": {"adx_market_watch": 2}
                },
                "clearance_evidence_totals": {"none": 2},
                "exchange_totals": {"ADX": 2},
                "official_asset_type_totals": {"Stock": 2},
                "asset_type_mismatch_totals": {"false": 2},
                "official_source_totals": {"adx_market_watch": 2},
                "policy": {"truth_required": "official evidence required"},
            },
            "rows": [
                {
                    "target_listing_key": "ADX::ADIB",
                    "ticker": "ADIB",
                    "target_exchange": "ADX",
                    "official_name": "Abu Dhabi Islamic Bank PJSC",
                    "official_asset_type": "Stock",
                    "official_isin": "",
                    "official_source_key": "adx_market_watch",
                    "existing_listing_keys": "EGX::ADIB",
                    "existing_exchanges": "EGX",
                    "identity_evidence": "asset_type_consistent",
                    "collision_risk_flags": "ticker_reused_on_other_exchange|missing_official_isin",
                    "review_bucket": "hold_symbol_only_collision_needs_non_symbol_identity",
                    "review_priority": "P3",
                    "review_decision": "symbol_collision_requires_non_symbol_identity_source",
                    "clearance_evidence_required": "official_non_symbol_identifier_or_keep_absent",
                    "recommended_next_action": "add_listing_from_symbol_match",
                    "asset_type_mismatch": "false",
                }
            ],
        }
    )

    assert result["passed"] is False
    assert result["invalid_generated_at"] == "not-a-timestamp"
    assert result["source_file_mismatches"] == {
        "listings_csv": {"expected": "data/listings.csv", "actual": "data/other.csv"},
        "masterfile_reference_csv": {
            "expected": "data/masterfiles/reference.csv",
            "actual": None,
        },
    }
    assert result["unexpected_source_files"] == ["unexpected_csv"]
    assert result["meta_policy_mismatch"] is True
    assert result["row_count_mismatch"] == {"reported": 2, "actual": 1}
    assert result["unsafe_symbol_only_rows"] == [
        {
            "row_index": 0,
            "target_listing_key": "ADX::ADIB",
            "recommended_next_action": "add_listing_from_symbol_match",
        }
    ]
    assert result["missing_policy_keys"] == ["no_symbol_only_additions", "blank_until_verified"]
    assert result["missing_required_buckets"] == [
        "same_isin_cross_listing_needs_name_or_scope_review",
        "distinct_official_isin_new_listing_candidate",
    ]
    assert result["summary_mismatches"]["review_bucket_totals"] == {
        "distinct_official_isin_new_listing_candidate": {"reported": 2, "actual": 0},
        "hold_symbol_only_collision_needs_non_symbol_identity": {"reported": 0, "actual": 1},
    }


def test_evaluate_otc_scope_gate_requires_scope_decision_before_metadata_enrichment() -> None:
    rows = [
        {
            "listing_key": "OTC::CLEAN",
            "ticker": "CLEAN",
            "exchange": "OTC",
            "asset_type": "Stock",
            "instrument_scope": "extended",
            "scope_reason": "otc_listing",
            "quality_status": "pass",
            "issue_types": "",
            "source_gap_field": "",
            "source_gap_class": "",
            "source_of_truth_outcome": "",
            "scope_decision": "already_extended_otc_listing",
            "otc_review_decision_status": "not_applicable",
            "review_bucket": "clean_extended_otc_listing",
            "review_priority": "P4",
            "scope_apply_eligibility": "already_extended_no_scope_change_required",
            "metadata_enrichment_gate": "no_metadata_enrichment_needed",
            "recommended_action": "keep_extended_scope",
        },
        {
            "listing_key": "OTC::NAME",
            "ticker": "NAME",
            "exchange": "OTC",
            "asset_type": "Stock",
            "instrument_scope": "extended",
            "scope_reason": "otc_listing",
            "quality_status": "warn",
            "issue_types": "official_name_mismatch",
            "source_gap_field": "",
            "source_gap_class": "",
            "source_of_truth_outcome": "",
            "scope_decision": "already_extended_otc_listing",
            "otc_review_decision_status": "pending_otc_name_mismatch_review",
            "review_bucket": "official_name_mismatch_review_first",
            "review_priority": "P2",
            "scope_apply_eligibility": "already_extended_no_scope_change_required",
            "metadata_enrichment_gate": "otc_name_mismatch_review_required_before_name_or_metadata_changes",
            "recommended_action": "use_otc_name_mismatch_review_before_name_changes",
        },
        {
            "listing_key": "OTC::GAP",
            "ticker": "GAP",
            "exchange": "OTC",
            "asset_type": "Stock",
            "instrument_scope": "extended",
            "scope_reason": "otc_listing",
            "quality_status": "source_gap",
            "issue_types": "official_reference_gap",
            "source_gap_field": "missing_sector_stock",
            "source_gap_class": "otc_sector_source_gap",
            "source_of_truth_outcome": "accepted_source_gap",
            "scope_decision": "already_extended_otc_listing",
            "otc_review_decision_status": "not_applicable",
            "review_bucket": "documented_otc_sector_source_gap",
            "review_priority": "P3",
            "scope_apply_eligibility": "already_extended_no_scope_change_required",
            "metadata_enrichment_gate": "reviewed_issuer_sector_source_required_keep_blank",
            "recommended_action": "leave_blank_as_documented_source_gap_until_reviewed_source",
        },
        {
            "listing_key": "OTC::CORE",
            "ticker": "CORE",
            "exchange": "OTC",
            "asset_type": "Stock",
            "instrument_scope": "core",
            "scope_reason": "primary_listing",
            "quality_status": "pass",
            "issue_types": "",
            "source_gap_field": "",
            "source_gap_class": "",
            "source_of_truth_outcome": "core_exclusion_candidate",
            "scope_decision": "core_exclusion_candidate_requires_review",
            "otc_review_decision_status": "not_applicable",
            "review_bucket": "core_exclusion_candidate_scope_review",
            "review_priority": "P1",
            "scope_apply_eligibility": "blocked_until_core_exclusion_candidate_scope_decision",
            "metadata_enrichment_gate": "scope_decision_required_before_any_metadata_enrichment",
            "recommended_action": "review_official_instrument_type_before_fill_or_drop",
        },
        {
            "listing_key": "OTC::QUALITY",
            "ticker": "QUALITY",
            "exchange": "OTC",
            "asset_type": "Stock",
            "instrument_scope": "extended",
            "scope_reason": "otc_listing",
            "quality_status": "source_gap",
            "issue_types": "official_reference_gap",
            "source_gap_field": "",
            "source_gap_class": "",
            "source_of_truth_outcome": "",
            "scope_decision": "already_extended_otc_listing",
            "otc_review_decision_status": "not_applicable",
            "review_bucket": "otc_quality_source_gap_review",
            "review_priority": "P3",
            "scope_apply_eligibility": "already_extended_no_scope_change_required",
            "metadata_enrichment_gate": "source_gap_review_required_before_enrichment",
            "recommended_action": "keep_extended_scope",
        },
        {
            "listing_key": "OTC::WARN",
            "ticker": "WARN",
            "exchange": "OTC",
            "asset_type": "Stock",
            "instrument_scope": "extended",
            "scope_reason": "otc_listing",
            "quality_status": "warn",
            "issue_types": "noncritical_warn",
            "source_gap_field": "",
            "source_gap_class": "",
            "source_of_truth_outcome": "",
            "scope_decision": "already_extended_otc_listing",
            "otc_review_decision_status": "not_applicable",
            "review_bucket": "otc_quality_warn_review",
            "review_priority": "P2",
            "scope_apply_eligibility": "already_extended_no_scope_change_required",
            "metadata_enrichment_gate": "entry_quality_warn_review_required_before_enrichment",
            "recommended_action": "keep_extended_scope",
        },
    ]
    for row in rows:
        row["review_strategy"] = otc_scope_review_strategy(row["review_bucket"])
        row["verification_evidence_required"] = otc_scope_verification_evidence_required(row["review_bucket"])
        row["recommended_next_source"] = otc_scope_recommended_next_source(row["review_bucket"])
        row["source_gate"] = otc_scope_source_gate(row["review_bucket"])
        row["source_gap_context"] = otc_scope_source_gap_context(row)
        row["otc_review_decision_context"] = otc_scope_review_decision_context(row)
        row["scope_review_context"] = otc_scope_review_context(row)
    result = evaluate_otc_scope_gate(
        {
            "summary": {
                "rows": 6,
                "scope_decision_totals": {
                    "already_extended_otc_listing": 5,
                    "core_exclusion_candidate_requires_review": 1,
                },
                "instrument_scope_totals": {"core": 1, "extended": 5},
                "scope_reason_totals": {"otc_listing": 5, "primary_listing": 1},
                "quality_status_totals": {"pass": 2, "source_gap": 2, "warn": 2},
                "review_bucket_totals": {
                    "clean_extended_otc_listing": 1,
                    "core_exclusion_candidate_scope_review": 1,
                    "documented_otc_sector_source_gap": 1,
                    "official_name_mismatch_review_first": 1,
                    "otc_quality_source_gap_review": 1,
                    "otc_quality_warn_review": 1,
                },
                "review_bucket_asset_type_totals": {
                    "clean_extended_otc_listing": {"Stock": 1},
                    "core_exclusion_candidate_scope_review": {"Stock": 1},
                    "documented_otc_sector_source_gap": {"Stock": 1},
                    "official_name_mismatch_review_first": {"Stock": 1},
                    "otc_quality_source_gap_review": {"Stock": 1},
                    "otc_quality_warn_review": {"Stock": 1},
                },
                "review_bucket_metadata_gate_totals": {
                    "clean_extended_otc_listing": {"no_metadata_enrichment_needed": 1},
                    "core_exclusion_candidate_scope_review": {
                        "scope_decision_required_before_any_metadata_enrichment": 1
                    },
                    "documented_otc_sector_source_gap": {
                        "reviewed_issuer_sector_source_required_keep_blank": 1
                    },
                    "official_name_mismatch_review_first": {
                        "otc_name_mismatch_review_required_before_name_or_metadata_changes": 1
                    },
                    "otc_quality_source_gap_review": {"source_gap_review_required_before_enrichment": 1},
                    "otc_quality_warn_review": {"entry_quality_warn_review_required_before_enrichment": 1},
                },
                "review_priority_totals": {"P1": 1, "P2": 2, "P3": 2, "P4": 1},
                "review_strategy_totals": {
                    "decide_core_extended_or_exclude_before_otc_metadata_work": 1,
                    "keep_sector_blank_until_reviewed_issuer_sector_source": 1,
                    "no_scope_or_metadata_action_required": 1,
                    "resolve_listing_keyed_name_mismatch_before_metadata_work": 1,
                    "review_entry_quality_warning_before_metadata_work": 1,
                    "review_quality_source_gap_before_metadata_work": 1,
                },
                "verification_evidence_required_totals": {
                    "current_pass_status_and_extended_scope_policy_no_metadata_action": 1,
                    "entry_quality_warning_review_before_metadata_change": 1,
                    "official_otc_listing_status_or_scope_policy_decision_before_metadata_work": 1,
                    "reviewed_issuer_sector_source_with_exact_listing_or_keep_blank_decision": 1,
                    "reviewed_otc_name_mismatch_decision_before_name_or_metadata_change": 1,
                    "source_gap_review_or_reviewed_source_before_metadata_change": 1,
                },
                "top_otc_scope_review_batches": [
                    {
                        "review_priority": "P1",
                        "review_bucket": "core_exclusion_candidate_scope_review",
                        "asset_type": "Stock",
                        "quality_status": "pass",
                        "metadata_enrichment_gate": "scope_decision_required_before_any_metadata_enrichment",
                        "rows": 1,
                        "review_strategy": "decide_core_extended_or_exclude_before_otc_metadata_work",
                        "verification_evidence_required": (
                            "official_otc_listing_status_or_scope_policy_decision_before_metadata_work"
                        ),
                        "recommended_next_source": (
                            "OTC Markets security profile, exchange tier/status evidence, SEC/issuer filing, or reviewed scope policy decision."
                        ),
                        "source_gate": (
                            "Decide core, extended, or exclude before any identifier, name, sector, or category enrichment."
                        ),
                    }
                ],
                "scope_apply_eligibility_totals": {
                    "already_extended_no_scope_change_required": 5,
                    "blocked_until_core_exclusion_candidate_scope_decision": 1,
                },
                "otc_scope_completion": {
                    "status": "scope_review_open",
                    "rows": 6,
                    "extended_otc_rows": 5,
                    "otc_listing_scope_reason_rows": 5,
                    "already_extended_scope_decision_rows": 5,
                    "core_exclusion_candidate_rows": 1,
                    "unexpected_core_scope_rows": 0,
                    "blocked_scope_decision_rows": 1,
                    "scope_apply_allowed_rows": 0,
                    "metadata_enrichment_authorized": False,
                    "source_gate": (
                        "OTC scope is complete only when every current OTC row is extended/otc_listing and "
                        "no core or core-exclusion scope decision remains open; metadata still requires "
                        "listing-keyed evidence."
                    ),
                },
                "post_scope_metadata_backlog": {
                    "status": "metadata_review_backlog_open",
                    "rows": 4,
                    "metadata_enrichment_authorized": False,
                    "scope_blocked_rows": 1,
                    "source_gate": (
                        "Post-scope OTC metadata work remains blocked unless each row has listing-keyed OTC Markets, "
                        "issuer, SEC, registry, or reviewed fallback evidence; no ticker-only enrichment is allowed."
                    ),
                },
                "post_scope_metadata_backlog_bucket_totals": {
                    "documented_otc_sector_source_gap": 1,
                    "official_name_mismatch_review_first": 1,
                    "otc_quality_source_gap_review": 1,
                    "otc_quality_warn_review": 1,
                },
                "post_scope_metadata_backlog_gate_totals": {
                    "entry_quality_warn_review_required_before_enrichment": 1,
                    "otc_name_mismatch_review_required_before_name_or_metadata_changes": 1,
                    "reviewed_issuer_sector_source_required_keep_blank": 1,
                    "source_gap_review_required_before_enrichment": 1,
                },
                "post_scope_metadata_backlog_examples": [
                    {
                        "listing_key": "OTC::SECTOR",
                        "ticker": "SECTOR",
                        "asset_type": "Stock",
                        "name": "Sector Source Gap Corp",
                        "review_bucket": "documented_otc_sector_source_gap",
                        "quality_status": "source_gap",
                        "metadata_enrichment_gate": "reviewed_issuer_sector_source_required_keep_blank",
                        "review_strategy": "keep_sector_blank_until_reviewed_issuer_sector_source",
                        "verification_evidence_required": (
                            "reviewed_issuer_sector_source_with_exact_listing_or_keep_blank_decision"
                        ),
                        "recommended_next_source": (
                            "SEC SIC, issuer filings, OTC Markets profile, or reviewed secondary company profile."
                        ),
                        "source_gate": "Canonical stock sector only after exchange/name gate; no ticker/name-only inference.",
                    }
                ],
                "metadata_enrichment_gate_totals": {
                    "entry_quality_warn_review_required_before_enrichment": 1,
                    "no_metadata_enrichment_needed": 1,
                    "otc_name_mismatch_review_required_before_name_or_metadata_changes": 1,
                    "reviewed_issuer_sector_source_required_keep_blank": 1,
                    "source_gap_review_required_before_enrichment": 1,
                    "scope_decision_required_before_any_metadata_enrichment": 1,
                },
                "source_gap_field_totals": {"missing_sector_stock": 1},
                "source_gap_class_totals": {"otc_sector_source_gap": 1},
                "source_of_truth_outcome_totals": {
                    "accepted_source_gap": 1,
                    "core_exclusion_candidate": 1,
                },
                "otc_core_exclusion_candidate_rows": 1,
                "otc_core_exclusion_candidate_asset_type_totals": {"Stock": 1},
                "otc_core_exclusion_candidate_quality_status_totals": {"pass": 1},
                "otc_core_exclusion_candidate_metadata_gate_totals": {
                    "scope_decision_required_before_any_metadata_enrichment": 1
                },
                "otc_core_exclusion_candidate_review_examples": [
                    {
                        "listing_key": "OTC::CORE",
                        "ticker": "CORE",
                        "asset_type": "Stock",
                        "quality_status": "pass",
                        "scope_decision": "core_exclusion_candidate_requires_review",
                        "review_bucket": "core_exclusion_candidate_scope_review",
                        "metadata_enrichment_gate": "scope_decision_required_before_any_metadata_enrichment",
                        "recommended_action": "review_official_instrument_type_before_fill_or_drop",
                        "source_gate": (
                            "Decide core, extended, or exclude before any identifier, name, sector, or category enrichment."
                        ),
                    }
                ],
                "drop_override_rows": 1,
                "drop_override_rows_still_present": 0,
                "otc_review_decision_rows": 1,
                "otc_review_decision_active_name_mismatch_rows": 0,
                "otc_name_mismatch_unreviewed_active_rows": 1,
                "otc_review_decision_resolution_totals": {
                    "pending_active_name_mismatch_review": 1,
                    "reviewed_decision_not_in_current_otc_scope": 1,
                },
                "otc_review_decision_current_listing_suppressed_rows": 0,
                "otc_review_decision_current_listing_suppressed_examples": [],
                "otc_review_decision_not_current_scope_rows": 1,
                "otc_review_decision_not_current_scope_examples": ["OTC::STALE"],
                "otc_review_decision_stale_rows": 1,
                "otc_review_decision_stale_examples": ["OTC::STALE"],
                "policy": {
                    "otc_scope": "OTC listings are extended unless reviewed otherwise.",
                    "no_blind_sector_enrichment": "Sector/category blanks require reviewed evidence.",
                },
            },
            "rows": rows,
        }
    )

    assert result["passed"] is True
    assert result["rows"] == 6


def test_evaluate_otc_scope_gate_fails_for_blind_source_gap_enrichment_or_stale_summary() -> None:
    result = evaluate_otc_scope_gate(
        {
            "summary": {
                "rows": 2,
                "scope_decision_totals": {"already_extended_otc_listing": 2},
                "instrument_scope_totals": {"extended": 2},
                "scope_reason_totals": {"otc_listing": 2},
                "quality_status_totals": {"source_gap": 2},
                "review_bucket_totals": {"documented_otc_sector_source_gap": 2},
                "review_priority_totals": {"P3": 2},
                "scope_apply_eligibility_totals": {"already_extended_no_scope_change_required": 2},
                "metadata_enrichment_gate_totals": {"no_metadata_enrichment_needed": 2},
                "source_gap_field_totals": {"missing_sector_stock": 2},
                "source_gap_class_totals": {"otc_sector_source_gap": 2},
                "source_of_truth_outcome_totals": {"accepted_source_gap": 2},
                "drop_override_rows": 1,
                "drop_override_rows_still_present": 1,
                "otc_review_decision_rows": 0,
                "otc_review_decision_active_name_mismatch_rows": 0,
                "otc_name_mismatch_unreviewed_active_rows": 0,
                "otc_review_decision_resolution_totals": {},
                "otc_review_decision_current_listing_suppressed_rows": 0,
                "otc_review_decision_current_listing_suppressed_examples": [],
                "otc_review_decision_not_current_scope_rows": 0,
                "otc_review_decision_not_current_scope_examples": [],
                "otc_review_decision_stale_rows": 0,
                "otc_review_decision_stale_examples": [],
                "policy": {"otc_scope": "OTC listings are extended."},
            },
            "rows": [
                {
                    "listing_key": "OTC::GAP",
                    "ticker": "GAP",
                    "exchange": "OTC",
                    "asset_type": "Stock",
                    "instrument_scope": "extended",
                    "scope_reason": "otc_listing",
                    "quality_status": "source_gap",
                    "issue_types": "official_reference_gap",
                    "source_gap_field": "missing_sector_stock",
                    "source_gap_class": "otc_sector_source_gap",
                    "source_of_truth_outcome": "accepted_source_gap",
                    "scope_decision": "already_extended_otc_listing",
                    "otc_review_decision_status": "not_applicable",
                    "review_bucket": "documented_otc_sector_source_gap",
                    "review_priority": "P3",
                    "scope_apply_eligibility": "already_extended_no_scope_change_required",
                    "metadata_enrichment_gate": "no_metadata_enrichment_needed",
                    "review_strategy": "keep_sector_blank_until_reviewed_issuer_sector_source",
                    "verification_evidence_required": (
                        "reviewed_issuer_sector_source_with_exact_listing_or_keep_blank_decision"
                    ),
                    "recommended_action": "fill_sector_from_symbol_shape",
                }
            ],
        }
    )

    assert result["passed"] is False
    assert result["row_count_mismatch"] == {"reported": 2, "actual": 1}
    assert result["unsafe_enrichment_rows"] == [
        {
            "row_index": 0,
            "listing_key": "OTC::GAP",
            "recommended_action": "fill_sector_from_symbol_shape",
            "metadata_enrichment_gate": "no_metadata_enrichment_needed",
        }
    ]
    assert result["missing_policy_keys"] == ["no_blind_sector_enrichment"]
    assert result["drop_override_rows_still_present"] == 1
    assert result["summary_mismatches"]["metadata_enrichment_gate_totals"] == {
        "no_metadata_enrichment_needed": {"reported": 2, "actual": 1},
    }


def test_evaluate_otc_scope_gate_fails_for_quality_source_gap_without_review_gate() -> None:
    result = evaluate_otc_scope_gate(
        {
            "summary": {
                "rows": 1,
                "scope_decision_totals": {"already_extended_otc_listing": 1},
                "instrument_scope_totals": {"extended": 1},
                "scope_reason_totals": {"otc_listing": 1},
                "quality_status_totals": {"source_gap": 1},
                "review_bucket_totals": {"otc_quality_source_gap_review": 1},
                "review_priority_totals": {"P3": 1},
                "scope_apply_eligibility_totals": {"already_extended_no_scope_change_required": 1},
                "metadata_enrichment_gate_totals": {"no_metadata_enrichment_needed": 1},
                "source_gap_field_totals": {},
                "source_gap_class_totals": {},
                "source_of_truth_outcome_totals": {},
                "drop_override_rows": 0,
                "drop_override_rows_still_present": 0,
                "otc_review_decision_rows": 0,
                "otc_review_decision_active_name_mismatch_rows": 0,
                "otc_name_mismatch_unreviewed_active_rows": 0,
                "otc_review_decision_resolution_totals": {},
                "otc_review_decision_current_listing_suppressed_rows": 0,
                "otc_review_decision_current_listing_suppressed_examples": [],
                "otc_review_decision_not_current_scope_rows": 0,
                "otc_review_decision_not_current_scope_examples": [],
                "otc_review_decision_stale_rows": 0,
                "otc_review_decision_stale_examples": [],
                "policy": {
                    "otc_scope": "OTC listings are extended.",
                    "no_blind_sector_enrichment": "No blind enrichment.",
                },
            },
            "rows": [
                {
                    "listing_key": "OTC::SRC",
                    "ticker": "SRC",
                    "exchange": "OTC",
                    "asset_type": "Stock",
                    "instrument_scope": "extended",
                    "scope_reason": "otc_listing",
                    "quality_status": "source_gap",
                    "issue_types": "official_reference_gap",
                    "source_gap_field": "",
                    "source_gap_class": "",
                    "source_of_truth_outcome": "",
                    "scope_decision": "already_extended_otc_listing",
                    "otc_review_decision_status": "not_applicable",
                    "review_bucket": "otc_quality_source_gap_review",
                    "review_priority": "P3",
                    "scope_apply_eligibility": "already_extended_no_scope_change_required",
                    "metadata_enrichment_gate": "no_metadata_enrichment_needed",
                        "review_strategy": "review_quality_source_gap_before_metadata_work",
                        "verification_evidence_required": "source_gap_review_or_reviewed_source_before_metadata_change",
                        "recommended_next_source": (
                            "Entry-quality source-gap artifact and stronger OTC Markets, issuer, SEC, or registry evidence."
                        ),
                        "source_gate": "Resolve or document the source gap before any metadata enrichment.",
                        "recommended_action": "fill_sector_from_name_shape",
                    }
            ],
        }
    )

    assert result["passed"] is False
    assert result["row_gaps"][0]["invalid_fields"] == [
        "source_gap_context",
        "otc_review_decision_context",
        "scope_review_context",
        "metadata_enrichment_gate",
    ]
    assert result["unsafe_enrichment_rows"] == [
        {
            "row_index": 0,
            "listing_key": "OTC::SRC",
            "recommended_action": "fill_sector_from_name_shape",
            "metadata_enrichment_gate": "no_metadata_enrichment_needed",
        }
    ]


def test_evaluate_canada_figi_gate_requires_queue_and_apply_traceability() -> None:
    residual_rows = [
        {
            "listing_key": "TSX::APPLY",
            "ticker": "APPLY",
            "exchange": "TSX",
            "asset_type": "Stock",
            "isin": "CA0000000001",
            "figi": "BBG000000001",
            "missing_isin": "false",
            "missing_figi": "false",
            "isin_decision": "isin_present",
            "figi_decision": "figi_present",
            "isin_apply_eligibility": "no_isin_action_required",
            "figi_apply_eligibility": "no_figi_action_required",
            "verification_evidence_required": "none_no_identifier_change_authorized",
            "canada_resolution_queue": "residual_no_identifier_action",
            "review_strategy": "keep_metadata_blank_until_stronger_official_source",
            "queue_evidence_required": "none_no_identifier_change_authorized",
            "recommended_next_source": "No identifier source required unless a future data change is proposed.",
            "source_gate": "No data change is authorized by this residual row.",
            "recommended_next_action": "none",
            "openfigi_review_status": "",
            "openfigi_review_decision": "",
            "source_gap_fields": "",
            "source_gap_classes": "",
            "source_of_truth_outcomes": "",
            "official_masterfile_sources": "tmx_listed_issuers",
        },
        {
            "listing_key": "NEO::GAP",
            "ticker": "GAP",
            "exchange": "NEO",
            "asset_type": "ETF",
            "isin": "CA0000000002",
            "figi": "",
            "missing_isin": "false",
            "missing_figi": "true",
            "isin_decision": "isin_present",
            "figi_decision": "missing_figi_reviewed_source_gap_no_openfigi_match",
            "isin_apply_eligibility": "no_isin_action_required",
            "figi_apply_eligibility": "keep_blank_as_reviewed_openfigi_source_gap",
            "verification_evidence_required": "stronger_figi_source_required_openfigi_no_match_reviewed",
            "canada_resolution_queue": "reviewed_openfigi_no_match_source_gap",
            "review_strategy": "keep_figi_blank_after_reviewed_openfigi_no_match",
            "queue_evidence_required": "stronger_figi_source_required_openfigi_no_match_reviewed",
            "recommended_next_source": (
                "Stronger FIGI source or reviewed OpenFIGI re-check evidence; existing no-match remains a source gap."
            ),
            "source_gate": (
                "Do not re-probe or fill FIGI from symbol/name; keep blank until stronger reviewed FIGI evidence exists."
            ),
            "recommended_next_action": "keep_blank_as_documented_openfigi_source_gap_until_stronger_source",
            "openfigi_review_status": "accepted_source_gap_no_openfigi_match",
            "openfigi_review_decision": "no_openfigi_match",
            "source_gap_fields": "missing_isin_primary",
            "source_gap_classes": "official_identifier_not_exposed_source_gap",
            "source_of_truth_outcomes": "accepted_source_gap",
            "official_masterfile_sources": "cboe_canada_listing_directory",
        },
    ]
    for row in residual_rows:
        row["source_gap_context"] = canada_source_gap_context(row)
        row["official_source_context"] = canada_official_source_context(row)
        row["identifier_review_context"] = canada_identifier_review_context(row)
    result = evaluate_canada_figi_gate(
        {
            "_meta": {
                "generated_at": "2026-05-24T00:00:00Z",
                "policy": {"tmx_first": "TMX/Cboe official sources first."},
                "source_files": {
                    "tickers_csv": "data/tickers.csv",
                    "identifiers_extended_csv": "data/identifiers_extended.csv",
                    "masterfile_reference_csv": "data/masterfiles/reference.csv",
                    "source_gap_classification_csv": "data/reports/source_gap_classification.csv",
                    "source_of_truth_decisions_csv": "data/reports/source_of_truth_decisions.csv",
                    "openfigi_gap_csv": "data/review_overrides/canada_figi_openfigi_gaps.csv",
                },
            },
            "summary": {
                "rows": 2,
                "exchange_totals": {"NEO": 1, "TSX": 1},
                "asset_type_totals": {"ETF": 1, "Stock": 1},
                "core_exclusion_candidate_rows": 0,
                "core_exclusion_candidate_exchange_totals": {},
                "core_exclusion_candidate_asset_type_totals": {},
                "core_exclusion_candidate_resolution_queue_totals": {},
                "core_exclusion_candidate_official_source_totals": {},
                "core_exclusion_candidate_source_gap_class_totals": {},
                "core_exclusion_candidate_examples": [],
                "missing_isin_rows": 0,
                "missing_figi_rows": 1,
                "canada_resolution_queue_totals": {
                    "residual_no_identifier_action": 1,
                    "reviewed_openfigi_no_match_source_gap": 1,
                },
                "canada_resolution_queue_exchange_totals": {
                    "residual_no_identifier_action": {"TSX": 1},
                    "reviewed_openfigi_no_match_source_gap": {"NEO": 1},
                },
                "canada_resolution_queue_asset_type_totals": {
                    "residual_no_identifier_action": {"Stock": 1},
                    "reviewed_openfigi_no_match_source_gap": {"ETF": 1},
                },
                "canada_resolution_queue_source_gap_class_totals": {
                    "residual_no_identifier_action": {"none": 1},
                    "reviewed_openfigi_no_match_source_gap": {
                        "official_identifier_not_exposed_source_gap": 1
                    },
                },
                "canada_resolution_queue_official_source_totals": {
                    "residual_no_identifier_action": {"tmx_listed_issuers": 1},
                    "reviewed_openfigi_no_match_source_gap": {"cboe_canada_listing_directory": 1},
                },
                "canada_resolution_queue_review_strategy_totals": {
                    "residual_no_identifier_action": {
                        "keep_metadata_blank_until_stronger_official_source": 1
                    },
                    "reviewed_openfigi_no_match_source_gap": {
                        "keep_figi_blank_after_reviewed_openfigi_no_match": 1
                    },
                },
                "canada_resolution_queue_evidence_required_totals": {
                    "residual_no_identifier_action": {
                        "none_no_identifier_change_authorized": 1
                    },
                    "reviewed_openfigi_no_match_source_gap": {
                        "stronger_figi_source_required_openfigi_no_match_reviewed": 1
                    },
                },
                "top_canada_resolution_review_batches": [
                    {
                        "canada_resolution_queue": "reviewed_openfigi_no_match_source_gap",
                        "exchange": "NEO",
                        "official_source": "cboe_canada_listing_directory",
                        "rows": 1,
                        "review_strategy": "keep_figi_blank_after_reviewed_openfigi_no_match",
                        "evidence_required": "stronger_figi_source_required_openfigi_no_match_reviewed",
                        "recommended_next_source": (
                            "Stronger FIGI source or reviewed OpenFIGI re-check evidence; existing no-match remains a source gap."
                        ),
                        "source_gate": (
                            "Do not re-probe or fill FIGI from symbol/name; keep blank until stronger reviewed FIGI evidence exists."
                        ),
                    }
                ],
                "isin_decision_totals": {"isin_present": 2},
                "figi_decision_totals": {
                    "figi_present": 1,
                    "missing_figi_reviewed_source_gap_no_openfigi_match": 1,
                },
                "isin_apply_eligibility_totals": {"no_isin_action_required": 2},
                "figi_apply_eligibility_totals": {
                    "keep_blank_as_reviewed_openfigi_source_gap": 1,
                    "no_figi_action_required": 1,
                },
                "verification_evidence_required_totals": {
                    "none_no_identifier_change_authorized": 1,
                    "stronger_figi_source_required_openfigi_no_match_reviewed": 1,
                },
                "openfigi_review_status_totals": {"accepted_source_gap_no_openfigi_match": 1},
                "openfigi_review_decision_totals": {"no_openfigi_match": 1},
                "source_gap_field_totals": {"missing_isin_primary": 1},
                "source_gap_class_totals": {"official_identifier_not_exposed_source_gap": 1},
                "source_of_truth_outcome_totals": {"accepted_source_gap": 1},
                "official_masterfile_source_totals": {
                    "cboe_canada_listing_directory": 1,
                    "tmx_listed_issuers": 1,
                },
                "policy": {
                    "tmx_first": "TMX/Cboe official sources first.",
                    "figi_gate": "FIGI enrichment is OpenFIGI-by-ISIN only.",
                    "openfigi_review_gate": "OpenFIGI no-match rows are retained as source gaps.",
                    "no_guessing": "No identifier is inferred.",
                },
            },
            "rows": residual_rows,
        },
        {
            "summary": {
                "rows": 0,
                "batch_size": 100,
                "batches": 0,
                "excluded_openfigi_gap_rows": 1,
                "exchange_totals": {},
                "asset_type_totals": {},
                "openfigi_exchange_hint_totals": {},
                "apply_eligibility_totals": {
                    "keep_figi_blank_as_reviewed_openfigi_source_gap_until_stronger_source": 1,
                    "no_active_openfigi_probe_rows": 1,
                },
                "verification_evidence_required_totals": {
                    "none_queue_drained_or_candidates_excluded_as_reviewed_source_gaps": 1,
                    "stronger_figi_source_required_for_reviewed_openfigi_no_match_or_cross_isin_collision": 1,
                },
                "policy": {
                    "input_gate": "valid ISIN and missing FIGI only",
                    "reviewed_gap_gate": "reviewed gaps excluded",
                    "no_symbol_guessing": "no symbol guessing",
                },
            },
            "rows": [],
        },
        {
            "summary": {
                "applied": True,
                "rows": 1,
                "gap_rows_added": 0,
                "decision_totals": {"apply": 1},
                "reason_totals": {"accepted_probe_match": 1},
                "applied_rows": 1,
                "written_rows": 1,
                "policy": {
                    "source": "accepted probe rows only",
                    "gates": "listing_key, ISIN, exchange hint and collision gates",
                    "openfigi_no_match": "no-match rows persisted as source gaps",
                },
            },
            "rows": [
                {
                    "listing_key": "TSX::APPLY",
                    "ticker": "APPLY",
                    "exchange": "TSX",
                    "isin": "CA0000000001",
                    "figi": "BBG000000001",
                    "decision": "apply",
                    "reason": "accepted_probe_match",
                }
            ],
        },
    )

    assert result["passed"] is True
    assert result["residual_rows"] == 2
    assert result["queue_rows"] == 0
    assert result["apply_rows"] == 1


def test_evaluate_canada_figi_gate_fails_for_unreviewed_candidate_or_bad_apply() -> None:
    result = evaluate_canada_figi_gate(
        {
            "_meta": {
                "generated_at": "not-a-timestamp",
                "policy": {"tmx_first": "official first"},
                "source_files": {
                    "tickers_csv": "data/tickers.csv",
                    "identifiers_extended_csv": "data/identifiers_extended.csv",
                    "masterfile_reference_csv": "data/masterfiles/reference.csv",
                    "source_gap_classification_csv": "data/reports/source_gap_classification.csv",
                    "source_of_truth_decisions_csv": "data/reports/source_of_truth_decisions.csv",
                    "openfigi_gap_csv": "data/wrong.csv",
                    "unexpected_csv": "data/unexpected.csv",
                },
            },
            "summary": {
                "rows": 1,
                "exchange_totals": {"TSX": 1},
                "asset_type_totals": {"Stock": 1},
                "missing_isin_rows": 0,
                "missing_figi_rows": 1,
                "canada_resolution_queue_totals": {"openfigi_candidate_after_isin_gate": 1},
                "canada_resolution_queue_exchange_totals": {"openfigi_candidate_after_isin_gate": {"TSX": 1}},
                "canada_resolution_queue_asset_type_totals": {"openfigi_candidate_after_isin_gate": {"Stock": 1}},
                "isin_decision_totals": {"isin_present": 1},
                "figi_decision_totals": {"missing_figi_openfigi_candidate": 1},
                "isin_apply_eligibility_totals": {"no_isin_action_required": 1},
                "figi_apply_eligibility_totals": {"probe": 1},
                "verification_evidence_required_totals": {"weak": 1},
                "openfigi_review_status_totals": {},
                "openfigi_review_decision_totals": {},
                "source_gap_field_totals": {},
                "source_gap_class_totals": {},
                "source_of_truth_outcome_totals": {},
                "official_masterfile_source_totals": {"tmx_listed_issuers": 1},
                "policy": {"tmx_first": "official first"},
            },
            "rows": [
                {
                    "listing_key": "TSX::BAD",
                    "ticker": "BAD",
                    "exchange": "TSX",
                    "asset_type": "Stock",
                    "isin": "CA0000000003",
                    "figi": "",
                    "missing_isin": "false",
                    "missing_figi": "true",
                    "isin_decision": "isin_present",
                    "figi_decision": "missing_figi_openfigi_candidate",
                    "isin_apply_eligibility": "no_isin_action_required",
                    "figi_apply_eligibility": "probe",
                    "verification_evidence_required": "weak",
                    "canada_resolution_queue": "openfigi_candidate_after_isin_gate",
                    "review_strategy": "queue_openfigi_by_isin_with_canada_exchange_hint",
                    "queue_evidence_required": "openfigi_id_isin_query_with_canada_exchange_hint_then_collision_gate",
                    "recommended_next_action": "probe",
                    "openfigi_review_status": "",
                    "openfigi_review_decision": "",
                    "source_gap_fields": "",
                    "source_gap_classes": "",
                    "source_of_truth_outcomes": "",
                    "official_masterfile_sources": "tmx_listed_issuers",
                }
            ],
        },
        {
            "summary": {
                "rows": 0,
                "batch_size": 100,
                "batches": 0,
                "excluded_openfigi_gap_rows": 1,
                "exchange_totals": {},
                "asset_type_totals": {},
                "openfigi_exchange_hint_totals": {},
                "apply_eligibility_totals": {"no_active_openfigi_probe_rows": 1},
                "verification_evidence_required_totals": {
                    "none_queue_drained_or_candidates_excluded_as_reviewed_source_gaps": 1,
                },
                "policy": {},
            },
            "rows": [],
        },
        {
            "summary": {
                "applied": True,
                "rows": 1,
                "gap_rows_added": 0,
                "decision_totals": {"apply": 1},
                "reason_totals": {"accepted_probe_match": 1},
                "applied_rows": 0,
                "written_rows": 0,
                "policy": {},
            },
            "rows": [
                {
                    "listing_key": "TSX::BAD",
                    "ticker": "BAD",
                    "exchange": "TSX",
                    "isin": "",
                    "figi": "",
                    "decision": "apply",
                    "reason": "accepted_probe_match",
                }
            ],
        },
    )

    assert result["passed"] is False
    assert result["residual_invalid_generated_at"] == "not-a-timestamp"
    assert result["residual_source_file_mismatches"] == {
        "openfigi_gap_csv": {
            "expected": "data/review_overrides/canada_figi_openfigi_gaps.csv",
            "actual": "data/wrong.csv",
        }
    }
    assert result["residual_unexpected_source_files"] == ["unexpected_csv"]
    assert result["queue_exclusion_mismatch"] == {
        "reported": 1,
        "residual_reviewed_openfigi_gap_rows": 0,
    }
    assert result["queue_candidate_mismatch"] == {
        "queue_rows": 0,
        "residual_openfigi_candidate_rows": 1,
    }
    assert result["missing_policy_keys"]["residual"] == ["figi_gate", "openfigi_review_gate", "no_guessing"]
    assert result["missing_policy_keys"]["queue"] == ["input_gate", "reviewed_gap_gate", "no_symbol_guessing"]
    assert result["missing_policy_keys"]["apply"] == ["source", "gates", "openfigi_no_match"]
    assert result["apply_row_gaps"][0]["invalid_fields"] == ["figi"]
    assert result["applied_rows_mismatch"] == {"reported": 0, "actual": 1}


def test_evaluate_b3_residual_gate_requires_official_first_residual_review() -> None:
    result = evaluate_b3_residual_gate(
        {
            "_meta": {
                "generated_at": "2026-05-24T00:00:00Z",
                "policy": {"no_guessing": "No B3 ISIN is guessed."},
                "source_files": {
                    "tickers_csv": "data/tickers.csv",
                    "instrument_scopes_csv": "data/instrument_scopes.csv",
                    "source_gap_classification_csv": "data/reports/source_gap_classification.csv",
                    "source_of_truth_decisions_csv": "data/reports/source_of_truth_decisions.csv",
                    "masterfile_reference_csv": "data/masterfiles/reference.csv",
                    "cotahist_probe_csv": "data/b3_verification/cotahist_isin_probe_current.csv",
                },
            },
            "summary": {
                "rows": 2,
                "gap_class_totals": {
                    "fund_or_trust_identifier_gap": 1,
                    "inactive_or_legacy_identifier_gap": 1,
                },
                "residual_decision_totals": {
                    "accepted_source_gap_requires_fund_or_registry_source": 1,
                    "core_exclusion_candidate_requires_scope_review": 1,
                },
                "current_instrument_scope_totals": {"core": 2},
                "current_scope_reason_totals": {"primary_listing_missing_isin": 2},
                "review_bucket_totals": {
                    "fund_or_registry_identifier_source_gap": 1,
                    "scope_review_before_identifier_fill": 1,
                },
                "review_priority_totals": {"P1": 1, "P2": 1},
                "apply_eligibility_totals": {
                    "blocked_until_core_or_extended_scope_decision": 1,
                    "source_gap_keep_blank_until_official_identifier_evidence": 1,
                },
                "verification_evidence_required_totals": {
                    "official_fund_trust_registry_or_prospectus_with_exact_symbol_product_name_and_valid_isin": 1,
                    "scope_decision_for_core_extended_or_exclude_before_identifier_fill": 1,
                },
                "cotahist_probe_decision_totals": {"missing_probe_row": 1, "no_cotahist_match": 1},
                "review_strategy_totals": {
                    "decide_b3_core_extended_or_exclude_before_identifier_work": 1,
                    "seek_official_fund_trust_registry_or_prospectus_isin": 1,
                },
                "top_b3_isin_review_batches": [
                    {
                        "review_priority": "P1",
                        "review_bucket": "scope_review_before_identifier_fill",
                        "gap_class": "inactive_or_legacy_identifier_gap",
                        "asset_type": "Stock",
                        "rows": 1,
                        "review_strategy": "decide_b3_core_extended_or_exclude_before_identifier_work",
                        "verification_evidence_required": "scope_decision_for_core_extended_or_exclude_before_identifier_fill",
                        "recommended_next_source": (
                            "Current B3 source plus reviewed core, extended, or exclude scope decision before identifier work."
                        ),
                        "source_gate": (
                            "No ISIN fill until the B3 listing is reviewed as core, extended, or excluded."
                        ),
                    },
                    {
                        "review_priority": "P2",
                        "review_bucket": "fund_or_registry_identifier_source_gap",
                        "gap_class": "fund_or_trust_identifier_gap",
                        "asset_type": "ETF",
                        "rows": 1,
                        "review_strategy": "seek_official_fund_trust_registry_or_prospectus_isin",
                        "verification_evidence_required": "official_fund_trust_registry_or_prospectus_with_exact_symbol_product_name_and_valid_isin",
                        "recommended_next_source": (
                            "Official fund/trust registry, prospectus, CSD, or reviewed identifier feed with exact product match."
                        ),
                        "source_gate": (
                            "Keep ISIN blank until official fund/trust product evidence exposes a valid checksum ISIN."
                        ),
                    },
                ],
                "policy": {
                    "no_guessing": "No B3 ISIN is guessed.",
                    "listing_keyed_review": "Each B3 ISIN residual is listing-keyed.",
                },
            },
            "rows": [
                {
                    "listing_key": "B3::AFOF11",
                    "ticker": "AFOF11",
                    "exchange": "B3",
                    "asset_type": "ETF",
                    "gap_class": "fund_or_trust_identifier_gap",
                    "source_of_truth_outcome": "accepted_source_gap",
                    "source_gap_context": (
                        "gap_class=fund_or_trust_identifier_gap;"
                        "source_of_truth_outcome=accepted_source_gap;review_needed=true"
                    ),
                    "current_instrument_scope": "core",
                    "current_scope_reason": "primary_listing_missing_isin",
                    "scope_review_context": (
                        "current_instrument_scope=core;current_scope_reason=primary_listing_missing_isin;"
                        "source_of_truth_outcome=accepted_source_gap"
                    ),
                    "review_needed": "true",
                    "recommended_next_source": (
                        "Official fund/trust registry, prospectus, CSD, or reviewed identifier feed with exact product match."
                    ),
                    "source_gate": "Keep ISIN blank until official fund/trust product evidence exposes a valid checksum ISIN.",
                    "b3_instruments_equities_match": "false",
                    "b3_instruments_equities_isin": "",
                    "cotahist_probe_decision": "no_cotahist_match",
                    "cotahist_probe_isin": "",
                    "official_source_context": (
                        "b3_instruments_equities_match=false;b3_instruments_equities_isin=none;"
                        "cotahist_probe_decision=no_cotahist_match;cotahist_probe_isin=none"
                    ),
                    "residual_decision": "accepted_source_gap_requires_fund_or_registry_source",
                    "review_bucket": "fund_or_registry_identifier_source_gap",
                    "review_priority": "P2",
                    "review_strategy": "seek_official_fund_trust_registry_or_prospectus_isin",
                    "apply_eligibility": "source_gap_keep_blank_until_official_identifier_evidence",
                    "verification_evidence_required": "official_fund_trust_registry_or_prospectus_with_exact_symbol_product_name_and_valid_isin",
                    "residual_review_context": (
                        "residual_decision=accepted_source_gap_requires_fund_or_registry_source;"
                        "review_bucket=fund_or_registry_identifier_source_gap;"
                        "apply_eligibility=source_gap_keep_blank_until_official_identifier_evidence;"
                        "verification_evidence_required=official_fund_trust_registry_or_prospectus_with_exact_symbol_product_name_and_valid_isin"
                    ),
                },
                {
                    "listing_key": "B3::C3RP3",
                    "ticker": "C3RP3",
                    "exchange": "B3",
                    "asset_type": "Stock",
                    "gap_class": "inactive_or_legacy_identifier_gap",
                    "source_of_truth_outcome": "core_exclusion_candidate",
                    "source_gap_context": (
                        "gap_class=inactive_or_legacy_identifier_gap;"
                        "source_of_truth_outcome=core_exclusion_candidate;review_needed=true"
                    ),
                    "current_instrument_scope": "core",
                    "current_scope_reason": "primary_listing_missing_isin",
                    "scope_review_context": (
                        "current_instrument_scope=core;current_scope_reason=primary_listing_missing_isin;"
                        "source_of_truth_outcome=core_exclusion_candidate"
                    ),
                    "review_needed": "true",
                    "recommended_next_source": (
                        "Current B3 source plus reviewed core, extended, or exclude scope decision before identifier work."
                    ),
                    "source_gate": "No ISIN fill until the B3 listing is reviewed as core, extended, or excluded.",
                    "b3_instruments_equities_match": "false",
                    "b3_instruments_equities_isin": "",
                    "cotahist_probe_decision": "",
                    "cotahist_probe_isin": "",
                    "official_source_context": (
                        "b3_instruments_equities_match=false;b3_instruments_equities_isin=none;"
                        "cotahist_probe_decision=none;cotahist_probe_isin=none"
                    ),
                    "residual_decision": "core_exclusion_candidate_requires_scope_review",
                    "review_bucket": "scope_review_before_identifier_fill",
                    "review_priority": "P1",
                    "review_strategy": "decide_b3_core_extended_or_exclude_before_identifier_work",
                    "apply_eligibility": "blocked_until_core_or_extended_scope_decision",
                    "verification_evidence_required": "scope_decision_for_core_extended_or_exclude_before_identifier_fill",
                    "residual_review_context": (
                        "residual_decision=core_exclusion_candidate_requires_scope_review;"
                        "review_bucket=scope_review_before_identifier_fill;"
                        "apply_eligibility=blocked_until_core_or_extended_scope_decision;"
                        "verification_evidence_required=scope_decision_for_core_extended_or_exclude_before_identifier_fill"
                    ),
                },
            ],
        },
        {
            "_meta": {
                "generated_at": "2026-05-24T00:00:00Z",
                "policy": {"no_guessing": "No B3 sector is guessed."},
                "source_files": {
                    "tickers_csv": "data/tickers.csv",
                    "source_gap_classification_csv": "data/reports/source_gap_classification.csv",
                    "source_of_truth_decisions_csv": "data/reports/source_of_truth_decisions.csv",
                    "b3_sector_probe_csv": "data/b3_verification/sector_classification_backfill.csv",
                },
            },
            "summary": {
                "rows": 2,
                "gap_class_totals": {"official_industry_taxonomy_unavailable_gap": 2},
                "residual_decision_totals": {
                    "accepted_source_gap_no_b3_classification_code_match": 1,
                    "core_exclusion_candidate_requires_scope_review": 1,
                },
                "review_bucket_totals": {
                    "no_b3_classification_code_match_source_gap": 1,
                    "scope_review_before_sector_fill": 1,
                },
                "review_priority_totals": {"P1": 1, "P3": 1},
                "apply_eligibility_totals": {
                    "blocked_until_core_or_extended_scope_decision": 1,
                    "source_gap_keep_blank_until_official_taxonomy_evidence": 1,
                },
                "verification_evidence_required_totals": {
                    "scope_decision_for_core_extended_or_exclude_before_sector_fill": 1,
                    "stronger_official_b3_or_issuer_taxonomy_source_with_exact_listing_match": 1,
                },
                "b3_probe_decision_totals": {"missing_probe_row": 1, "no_b3_code_match": 1},
                "review_strategy_totals": {
                    "decide_b3_core_extended_or_exclude_before_sector_work": 1,
                    "keep_blank_until_stronger_official_b3_or_issuer_taxonomy": 1,
                },
                "top_b3_sector_review_batches": [
                    {
                        "review_priority": "P1",
                        "review_bucket": "scope_review_before_sector_fill",
                        "gap_class": "official_industry_taxonomy_unavailable_gap",
                        "b3_code_shape": "missing_b3_code",
                        "asset_type": "Stock",
                        "rows": 1,
                        "review_strategy": "decide_b3_core_extended_or_exclude_before_sector_work",
                        "verification_evidence_required": "scope_decision_for_core_extended_or_exclude_before_sector_fill",
                        "recommended_next_source": (
                            "Current B3 source plus reviewed core, extended, or exclude scope decision before sector work."
                        ),
                        "source_gate": (
                            "No sector fill until the B3 listing is reviewed as core, extended, or excluded."
                        ),
                    },
                    {
                        "review_priority": "P3",
                        "review_bucket": "no_b3_classification_code_match_source_gap",
                        "gap_class": "official_industry_taxonomy_unavailable_gap",
                        "b3_code_shape": "alpha_b3_code",
                        "asset_type": "Stock",
                        "rows": 1,
                        "review_strategy": "keep_blank_until_stronger_official_b3_or_issuer_taxonomy",
                        "verification_evidence_required": "stronger_official_b3_or_issuer_taxonomy_source_with_exact_listing_match",
                        "recommended_next_source": (
                            "Stronger official B3 or issuer taxonomy source exposing sector for the exact listing."
                        ),
                        "source_gate": (
                            "Keep stock_sector blank until official B3 or issuer taxonomy evidence matches the exact listing."
                        ),
                    },
                ],
                "b3_code_shape_totals": {"alpha_b3_code": 1, "missing_b3_code": 1},
                "alphanumeric_b3_code_rows": 0,
                "alphanumeric_b3_code_examples": [],
                "policy": {
                    "no_guessing": "No B3 sector is guessed.",
                    "listing_keyed_review": "Each B3 sector residual is listing-keyed.",
                    "b3_code_shape_review": "Alphanumeric B3 issuer-code roots are preserved.",
                },
            },
            "rows": [
                {
                    "listing_key": "B3::MISS3",
                    "ticker": "MISS3",
                    "exchange": "B3",
                    "asset_type": "Stock",
                    "gap_class": "official_industry_taxonomy_unavailable_gap",
                    "source_of_truth_outcome": "accepted_source_gap",
                    "source_gap_context": (
                        "gap_class=official_industry_taxonomy_unavailable_gap;"
                        "source_of_truth_outcome=accepted_source_gap;review_needed=true"
                    ),
                    "review_needed": "true",
                    "recommended_next_source": (
                        "Stronger official B3 or issuer taxonomy source exposing sector for the exact listing."
                    ),
                    "source_gate": (
                        "Keep stock_sector blank until official B3 or issuer taxonomy evidence matches the exact listing."
                    ),
                    "b3_probe_decision": "no_b3_code_match",
                    "b3_code": "MISS",
                    "b3_sector_update": "",
                    "official_source_context": (
                        "b3_probe_decision=no_b3_code_match;b3_code=MISS;"
                        "b3_sector_update=none;b3_source_url=none"
                    ),
                    "residual_decision": "accepted_source_gap_no_b3_classification_code_match",
                    "review_bucket": "no_b3_classification_code_match_source_gap",
                    "review_priority": "P3",
                    "review_strategy": "keep_blank_until_stronger_official_b3_or_issuer_taxonomy",
                    "apply_eligibility": "source_gap_keep_blank_until_official_taxonomy_evidence",
                    "verification_evidence_required": "stronger_official_b3_or_issuer_taxonomy_source_with_exact_listing_match",
                    "residual_review_context": (
                        "residual_decision=accepted_source_gap_no_b3_classification_code_match;"
                        "review_bucket=no_b3_classification_code_match_source_gap;"
                        "apply_eligibility=source_gap_keep_blank_until_official_taxonomy_evidence;"
                        "verification_evidence_required=stronger_official_b3_or_issuer_taxonomy_source_with_exact_listing_match"
                    ),
                },
                {
                    "listing_key": "B3::OLD3",
                    "ticker": "OLD3",
                    "exchange": "B3",
                    "asset_type": "Stock",
                    "gap_class": "official_industry_taxonomy_unavailable_gap",
                    "source_of_truth_outcome": "core_exclusion_candidate",
                    "source_gap_context": (
                        "gap_class=official_industry_taxonomy_unavailable_gap;"
                        "source_of_truth_outcome=core_exclusion_candidate;review_needed=true"
                    ),
                    "review_needed": "true",
                    "recommended_next_source": (
                        "Current B3 source plus reviewed core, extended, or exclude scope decision before sector work."
                    ),
                    "source_gate": "No sector fill until the B3 listing is reviewed as core, extended, or excluded.",
                    "b3_probe_decision": "",
                    "b3_code": "",
                    "b3_sector_update": "",
                    "official_source_context": (
                        "b3_probe_decision=none;b3_code=none;b3_sector_update=none;b3_source_url=none"
                    ),
                    "residual_decision": "core_exclusion_candidate_requires_scope_review",
                    "review_bucket": "scope_review_before_sector_fill",
                    "review_priority": "P1",
                    "review_strategy": "decide_b3_core_extended_or_exclude_before_sector_work",
                    "apply_eligibility": "blocked_until_core_or_extended_scope_decision",
                    "verification_evidence_required": "scope_decision_for_core_extended_or_exclude_before_sector_fill",
                    "residual_review_context": (
                        "residual_decision=core_exclusion_candidate_requires_scope_review;"
                        "review_bucket=scope_review_before_sector_fill;"
                        "apply_eligibility=blocked_until_core_or_extended_scope_decision;"
                        "verification_evidence_required=scope_decision_for_core_extended_or_exclude_before_sector_fill"
                    ),
                },
            ],
        },
    )

    assert result["passed"] is True
    assert result["rows"] == {"isin": 2, "sector": 2, "total": 4}


def test_evaluate_b3_residual_gate_fails_for_unsafe_apply_or_stale_summary() -> None:
    result = evaluate_b3_residual_gate(
        {
            "_meta": {
                "generated_at": "not-a-timestamp",
                "policy": {"no_guessing": "No B3 ISIN is guessed."},
                "source_files": {
                    "tickers_csv": "data/tickers.csv",
                    "instrument_scopes_csv": "data/instrument_scopes.csv",
                    "source_gap_classification_csv": "data/reports/source_gap_classification.csv",
                    "source_of_truth_decisions_csv": "data/reports/source_of_truth_decisions.csv",
                    "masterfile_reference_csv": "data/masterfiles/reference.csv",
                    "cotahist_probe_csv": "data/wrong.csv",
                },
            },
            "summary": {
                "rows": 2,
                "gap_class_totals": {"fund_or_trust_identifier_gap": 2},
                "residual_decision_totals": {"official_cotahist_isin_available_review_apply": 2},
                "review_bucket_totals": {"official_isin_candidate_requires_apply_gate": 2},
                "review_priority_totals": {"P1": 2},
                "apply_eligibility_totals": {"apply_now": 2},
                "verification_evidence_required_totals": {"weak": 2},
                "cotahist_probe_decision_totals": {"accept": 2},
                "policy": {"no_guessing": "No guessing."},
            },
            "rows": [
                {
                    "listing_key": "B3::BAD11",
                    "ticker": "BAD11",
                    "exchange": "B3",
                    "asset_type": "ETF",
                    "gap_class": "fund_or_trust_identifier_gap",
                    "source_of_truth_outcome": "accepted_source_gap",
                    "current_instrument_scope": "core",
                    "current_scope_reason": "primary_listing_missing_isin",
                    "scope_review_context": "wrong",
                    "review_needed": "true",
                    "recommended_next_source": "COTAHIST",
                    "source_gate": "weak",
                    "b3_instruments_equities_match": "false",
                    "b3_instruments_equities_isin": "",
                    "cotahist_probe_decision": "accept",
                    "cotahist_probe_isin": "",
                    "residual_decision": "official_cotahist_isin_available_review_apply",
                    "review_bucket": "official_isin_candidate_requires_apply_gate",
                    "review_priority": "P1",
                    "review_strategy": "validate_official_b3_or_cotahist_isin_before_apply",
                    "apply_eligibility": "apply_now",
                    "verification_evidence_required": "weak",
                }
            ],
        },
        {
            "_meta": {
                "generated_at": "2026-05-24T00:00:00Z",
                "policy": {"listing_keyed_review": "listing keyed"},
                "source_files": {
                    "tickers_csv": "data/tickers.csv",
                    "source_gap_classification_csv": "data/reports/source_gap_classification.csv",
                    "source_of_truth_decisions_csv": "data/reports/source_of_truth_decisions.csv",
                    "b3_sector_probe_csv": "data/wrong.csv",
                    "unexpected_csv": "data/unexpected.csv",
                },
            },
            "summary": {
                "rows": 1,
                "gap_class_totals": {"official_industry_taxonomy_unavailable_gap": 1},
                "residual_decision_totals": {"official_b3_sector_available_rebuild_required": 1},
                "review_bucket_totals": {"official_sector_candidate_requires_apply_gate": 1},
                "review_priority_totals": {"P1": 1},
                "apply_eligibility_totals": {"apply_now": 1},
                "verification_evidence_required_totals": {"weak": 1},
                "b3_probe_decision_totals": {"accept": 1},
                "b3_code_shape_totals": {"alpha_b3_code": 1},
                "alphanumeric_b3_code_rows": 1,
                "alphanumeric_b3_code_examples": [{"listing_key": "B3::BAD3", "b3_code": "BAD", "b3_probe_decision": "accept"}],
                "policy": {"listing_keyed_review": "listing keyed"},
            },
            "rows": [
                {
                    "listing_key": "B3::BAD3",
                    "ticker": "BAD3",
                    "exchange": "B3",
                    "asset_type": "Stock",
                    "gap_class": "official_industry_taxonomy_unavailable_gap",
                    "source_of_truth_outcome": "accepted_source_gap",
                    "review_needed": "true",
                    "recommended_next_source": "B3 taxonomy",
                    "source_gate": "weak",
                    "b3_probe_decision": "accept",
                    "b3_code": "BAD",
                    "b3_sector_update": "",
                    "residual_decision": "official_b3_sector_available_rebuild_required",
                    "review_bucket": "official_sector_candidate_requires_apply_gate",
                    "review_priority": "P1",
                    "review_strategy": "validate_official_b3_taxonomy_candidate_before_sector_apply",
                    "apply_eligibility": "apply_now",
                    "verification_evidence_required": "weak",
                }
            ],
        },
    )

    assert result["passed"] is False
    assert result["isin"]["invalid_generated_at"] == "not-a-timestamp"
    assert result["isin"]["source_file_mismatches"] == {
        "cotahist_probe_csv": {
            "expected": "data/b3_verification/cotahist_isin_probe_current.csv",
            "actual": "data/wrong.csv",
        }
    }
    assert result["isin"]["row_count_mismatch"] == {"reported": 2, "actual": 1}
    assert result["isin"]["row_gaps"][0]["invalid_fields"] == [
        "official_isin",
        "apply_eligibility",
        "verification_evidence_required",
        "recommended_next_source",
        "source_gate",
        "source_gap_context",
        "scope_review_context",
        "official_source_context",
        "residual_review_context",
    ]
    assert result["isin"]["unsafe_apply_rows"] == [
        {"row_index": 0, "listing_key": "B3::BAD11", "apply_eligibility": "apply_now"}
    ]
    assert result["isin"]["missing_policy_keys"] == ["listing_keyed_review"]
    assert result["sector"]["row_gaps"][0]["invalid_fields"] == [
        "b3_sector_update",
        "apply_eligibility",
        "verification_evidence_required",
        "recommended_next_source",
        "source_gate",
        "source_gap_context",
        "official_source_context",
        "residual_review_context",
    ]
    assert result["sector"]["unsafe_apply_rows"] == [
        {"row_index": 0, "listing_key": "B3::BAD3", "apply_eligibility": "apply_now"}
    ]
    assert result["sector"]["source_file_mismatches"] == {
        "b3_sector_probe_csv": {
            "expected": "data/b3_verification/sector_classification_backfill.csv",
            "actual": "data/wrong.csv",
        }
    }
    assert result["sector"]["unexpected_source_files"] == ["unexpected_csv"]
    assert result["sector"]["missing_policy_keys"] == ["no_guessing"]


def test_evaluate_asx_residual_gate_requires_official_first_source_gap_handling() -> None:
    rows = [
        {
            "listing_key": "ASX::RM1",
            "ticker": "RM1",
            "exchange": "ASX",
            "asset_type": "ETF",
            "field": "missing_isin_primary",
            "target_field": "isin",
            "gap_class": "debt_or_securitized_identifier_gap",
            "source_of_truth_outcome": "core_exclusion_candidate",
            "review_needed": "true",
            "recommended_next_source": (
                "asx_listed_companies plus reviewed scope decision for core, extended, or exclude before identifier work."
            ),
            "source_gate": "No ISIN or category enrichment until the listing is reviewed as core, extended, or excluded.",
            "official_source_context": "official_masterfile_sources=asx_listed_companies;asx_isin_probe_decision=no_asx_match",
            "official_capability": "masterfile_match=true;masterfile_exposes_isin=false;masterfile_exposes_sector=false",
            "official_masterfile_match": "true",
            "official_masterfile_exposes_isin": "false",
            "official_masterfile_exposes_sector": "false",
            "official_masterfile_sources": "asx_listed_companies",
            "asx_isin_probe_decision": "no_asx_match",
            "asx_isin_probe_isin": "",
            "asx_resolution_queue": "core_exclusion_candidate_identifier_scope_review",
            "residual_decision": "core_exclusion_candidate_requires_scope_review",
            "review_bucket": "scope_review_before_any_data_fill",
            "review_priority": "P1",
            "review_strategy": "scope_review_before_asx_identifier_enrichment",
            "apply_eligibility": "blocked_until_core_or_extended_scope_decision",
            "verification_evidence_required": "scope_decision_for_core_extended_or_exclude_before_identifier_or_category_fill",
            "recommended_next_action": "review_scope_before_identifier_or_category_enrichment",
        },
        {
            "listing_key": "ASX::VCD",
            "ticker": "VCD",
            "exchange": "ASX",
            "asset_type": "ETF",
            "field": "missing_etf_category",
            "target_field": "etf_category",
            "gap_class": "official_product_taxonomy_unavailable_gap",
            "source_of_truth_outcome": "accepted_source_gap",
            "review_needed": "true",
            "recommended_next_source": (
                "Official ASX product taxonomy, issuer/sponsor product page, PDS, or reviewed product taxonomy source."
            ),
            "source_gate": "Keep ETF category blank until exact product taxonomy evidence exists.",
            "official_source_context": "official_masterfile_sources=asx_investment_products;asx_isin_probe_decision=missing_probe_row",
            "official_capability": "masterfile_match=true;masterfile_exposes_isin=false;masterfile_exposes_sector=false",
            "official_masterfile_match": "true",
            "official_masterfile_exposes_isin": "false",
            "official_masterfile_exposes_sector": "false",
            "official_masterfile_sources": "asx_investment_products",
            "asx_isin_probe_decision": "",
            "asx_isin_probe_isin": "",
            "asx_resolution_queue": "missing_etf_category_requires_official_product_taxonomy",
            "residual_decision": "accepted_source_gap_requires_official_product_taxonomy",
            "review_bucket": "product_taxonomy_source_gap",
            "review_priority": "P2",
            "review_strategy": "seek_official_or_reviewed_asx_product_taxonomy",
            "apply_eligibility": "keep_category_blank_until_official_product_taxonomy_source",
            "verification_evidence_required": "official_or_reviewed_product_taxonomy_with_exact_listing_match",
            "recommended_next_action": "keep_category_blank_until_official_or_reviewed_product_taxonomy_exists",
        },
        {
            "listing_key": "ASX::ABC",
            "ticker": "ABC",
            "exchange": "ASX",
            "asset_type": "Stock",
            "field": "missing_isin_primary",
            "target_field": "isin",
            "gap_class": "official_identifier_reference_unmatched_gap",
            "source_of_truth_outcome": "accepted_source_gap",
            "review_needed": "true",
            "recommended_next_source": (
                "Current ASX ISIN workbook, registry, issuer, trustee, or prospectus source with exact listing match."
            ),
            "source_gate": (
                "Keep ISIN blank until current ASX, registry, issuer, or trustee evidence proves the identifier."
            ),
            "official_source_context": "official_masterfile_sources=asx_listed_companies;asx_isin_probe_decision=no_asx_match",
            "official_capability": "masterfile_match=true;masterfile_exposes_isin=false;masterfile_exposes_sector=false",
            "official_masterfile_match": "true",
            "official_masterfile_exposes_isin": "false",
            "official_masterfile_exposes_sector": "false",
            "official_masterfile_sources": "asx_listed_companies",
            "asx_isin_probe_decision": "no_asx_match",
            "asx_isin_probe_isin": "",
            "asx_resolution_queue": "missing_isin_not_in_current_asx_isin_workbook",
            "residual_decision": "accepted_source_gap_not_in_current_asx_isin_workbook",
            "review_bucket": "identifier_source_gap",
            "review_priority": "P3",
            "review_strategy": "keep_isin_blank_until_current_asx_or_registry_source",
            "apply_eligibility": "keep_identifier_blank_until_direct_registry_issuer_or_official_asx_evidence",
            "verification_evidence_required": "direct_registry_issuer_or_official_asx_identifier_source_with_exact_listing_match",
            "recommended_next_action": "keep_identifier_blank_until_direct_registry_issuer_or_official_asx_evidence_exists",
        },
    ]
    result = evaluate_asx_residual_gate(
        {
            "_meta": {
                "generated_at": "2026-05-24T00:00:00Z",
                "policy": {"official_first": "ASX official workbooks first."},
                "source_files": {
                    "tickers_csv": "data/tickers.csv",
                    "masterfile_reference_csv": "data/masterfiles/reference.csv",
                    "source_gap_classification_csv": "data/reports/source_gap_classification.csv",
                    "source_of_truth_decisions_csv": "data/reports/source_of_truth_decisions.csv",
                    "asx_isin_probe_csv": "data/asx_verification/missing_isin_backfill.csv",
                },
            },
            "summary": {
                "rows": 3,
                "field_totals": {"missing_etf_category": 1, "missing_isin_primary": 2},
                "asset_type_totals": {"ETF": 2, "Stock": 1},
                    "core_exclusion_candidate_rows": 1,
                    "core_exclusion_candidate_field_totals": {"missing_isin_primary": 1},
                    "core_exclusion_candidate_asset_type_totals": {"ETF": 1},
                    "core_exclusion_candidate_gap_class_totals": {"debt_or_securitized_identifier_gap": 1},
                    "core_exclusion_candidate_resolution_queue_totals": {
                        "core_exclusion_candidate_identifier_scope_review": 1
                    },
                    "core_exclusion_candidate_official_source_totals": {"asx_listed_companies": 1},
                    "core_exclusion_candidate_official_capability_totals": {
                        "masterfile_exposes_isin=false": 1,
                        "masterfile_exposes_sector=false": 1,
                        "masterfile_match=true": 1,
                    },
                    "gap_class_totals": {
                    "debt_or_securitized_identifier_gap": 1,
                    "official_identifier_reference_unmatched_gap": 1,
                    "official_product_taxonomy_unavailable_gap": 1,
                    },
                    "source_of_truth_outcome_totals": {"accepted_source_gap": 2, "core_exclusion_candidate": 1},
                    "asx_residual_backlog": {
                        "status": "review_only_scope_identifier_or_product_taxonomy_source_gaps",
                        "rows": 3,
                        "scope_decision_required_rows": 1,
                        "identity_review_required_rows": 0,
                        "official_identifier_source_required_rows": 1,
                        "official_product_taxonomy_required_rows": 1,
                        "official_isin_apply_candidate_rows": 0,
                        "direct_data_apply_allowed_rows": 0,
                        "metadata_enrichment_authorized": False,
                        "source_gate": (
                            "ASX residual work remains blocked unless exact ASX workbook, registry, issuer, trustee, prospectus, "
                            "or reviewed product-taxonomy evidence proves the value; no ticker/name or peer-instrument inference."
                        ),
                    },
                    "asx_residual_backlog_queue_totals": {
                        "core_exclusion_candidate_identifier_scope_review": 1,
                        "missing_etf_category_requires_official_product_taxonomy": 1,
                        "missing_isin_not_in_current_asx_isin_workbook": 1,
                    },
                    "asx_residual_backlog_evidence_required_totals": {
                        "direct_registry_issuer_or_official_asx_identifier_source_with_exact_listing_match": 1,
                        "official_or_reviewed_product_taxonomy_with_exact_listing_match": 1,
                        "scope_decision_for_core_extended_or_exclude_before_identifier_or_category_fill": 1,
                    },
                    "asx_resolution_queue_totals": {
                        "core_exclusion_candidate_identifier_scope_review": 1,
                        "missing_etf_category_requires_official_product_taxonomy": 1,
                    "missing_isin_not_in_current_asx_isin_workbook": 1,
                },
                "asx_resolution_queue_field_totals": {
                    "core_exclusion_candidate_identifier_scope_review": {"missing_isin_primary": 1},
                    "missing_etf_category_requires_official_product_taxonomy": {"missing_etf_category": 1},
                    "missing_isin_not_in_current_asx_isin_workbook": {"missing_isin_primary": 1},
                },
                "asx_resolution_queue_gap_class_totals": {
                    "core_exclusion_candidate_identifier_scope_review": {"debt_or_securitized_identifier_gap": 1},
                    "missing_etf_category_requires_official_product_taxonomy": {
                        "official_product_taxonomy_unavailable_gap": 1
                    },
                    "missing_isin_not_in_current_asx_isin_workbook": {
                        "official_identifier_reference_unmatched_gap": 1
                    },
                },
                "asx_resolution_queue_official_source_totals": {
                    "core_exclusion_candidate_identifier_scope_review": {"asx_listed_companies": 1},
                    "missing_etf_category_requires_official_product_taxonomy": {"asx_investment_products": 1},
                    "missing_isin_not_in_current_asx_isin_workbook": {"asx_listed_companies": 1},
                },
                "asx_resolution_queue_review_strategy_totals": {
                    "core_exclusion_candidate_identifier_scope_review": {
                        "scope_review_before_asx_identifier_enrichment": 1
                    },
                    "missing_etf_category_requires_official_product_taxonomy": {
                        "seek_official_or_reviewed_asx_product_taxonomy": 1
                    },
                    "missing_isin_not_in_current_asx_isin_workbook": {
                        "keep_isin_blank_until_current_asx_or_registry_source": 1
                    },
                },
                "asx_resolution_queue_evidence_required_totals": {
                    "core_exclusion_candidate_identifier_scope_review": {
                        "scope_decision_for_core_extended_or_exclude_before_identifier_or_category_fill": 1
                    },
                    "missing_etf_category_requires_official_product_taxonomy": {
                        "official_or_reviewed_product_taxonomy_with_exact_listing_match": 1
                    },
                    "missing_isin_not_in_current_asx_isin_workbook": {
                        "direct_registry_issuer_or_official_asx_identifier_source_with_exact_listing_match": 1
                    },
                },
                "asx_resolution_queue_official_capability_totals": {
                    "core_exclusion_candidate_identifier_scope_review": {
                        "masterfile_exposes_isin=false": 1,
                        "masterfile_exposes_sector=false": 1,
                        "masterfile_match=true": 1,
                    },
                    "missing_etf_category_requires_official_product_taxonomy": {
                        "masterfile_exposes_isin=false": 1,
                        "masterfile_exposes_sector=false": 1,
                        "masterfile_match=true": 1,
                    },
                    "missing_isin_not_in_current_asx_isin_workbook": {
                        "masterfile_exposes_isin=false": 1,
                        "masterfile_exposes_sector=false": 1,
                        "masterfile_match=true": 1,
                    },
                },
                "top_asx_resolution_review_batches": [
                    {
                        "asx_resolution_queue": "core_exclusion_candidate_identifier_scope_review",
                        "field": "missing_isin_primary",
                        "official_source": "asx_listed_companies",
                        "rows": 1,
                        "review_strategy": "scope_review_before_asx_identifier_enrichment",
                        "evidence_required": "scope_decision_for_core_extended_or_exclude_before_identifier_or_category_fill",
                        "recommended_next_source": (
                            "asx_listed_companies plus reviewed scope decision for core, extended, or exclude before identifier work."
                        ),
                        "source_gate": "No ISIN or category enrichment until the listing is reviewed as core, extended, or excluded.",
                    },
                    {
                        "asx_resolution_queue": "missing_etf_category_requires_official_product_taxonomy",
                        "field": "missing_etf_category",
                        "official_source": "asx_investment_products",
                        "rows": 1,
                        "review_strategy": "seek_official_or_reviewed_asx_product_taxonomy",
                        "evidence_required": "official_or_reviewed_product_taxonomy_with_exact_listing_match",
                        "recommended_next_source": (
                            "Official ASX product taxonomy, issuer/sponsor product page, PDS, or reviewed product taxonomy source."
                        ),
                        "source_gate": "Keep ETF category blank until exact product taxonomy evidence exists.",
                    },
                    {
                        "asx_resolution_queue": "missing_isin_not_in_current_asx_isin_workbook",
                        "field": "missing_isin_primary",
                        "official_source": "asx_listed_companies",
                        "rows": 1,
                        "review_strategy": "keep_isin_blank_until_current_asx_or_registry_source",
                        "evidence_required": "direct_registry_issuer_or_official_asx_identifier_source_with_exact_listing_match",
                        "recommended_next_source": (
                            "Current ASX ISIN workbook, registry, issuer, trustee, or prospectus source with exact listing match."
                        ),
                        "source_gate": (
                            "Keep ISIN blank until current ASX, registry, issuer, or trustee evidence proves the identifier."
                        ),
                    },
                ],
                "residual_decision_totals": {
                    "accepted_source_gap_not_in_current_asx_isin_workbook": 1,
                    "accepted_source_gap_requires_official_product_taxonomy": 1,
                    "core_exclusion_candidate_requires_scope_review": 1,
                },
                "review_bucket_totals": {
                    "identifier_source_gap": 1,
                    "product_taxonomy_source_gap": 1,
                    "scope_review_before_any_data_fill": 1,
                },
                "review_priority_totals": {"P1": 1, "P2": 1, "P3": 1},
                "apply_eligibility_totals": {
                    "blocked_until_core_or_extended_scope_decision": 1,
                    "keep_category_blank_until_official_product_taxonomy_source": 1,
                    "keep_identifier_blank_until_direct_registry_issuer_or_official_asx_evidence": 1,
                },
                "verification_evidence_required_totals": {
                    "direct_registry_issuer_or_official_asx_identifier_source_with_exact_listing_match": 1,
                    "official_or_reviewed_product_taxonomy_with_exact_listing_match": 1,
                    "scope_decision_for_core_extended_or_exclude_before_identifier_or_category_fill": 1,
                },
                "asx_isin_probe_decision_totals": {"no_asx_match": 2},
                "official_masterfile_match_totals": {"true": 3},
                "official_masterfile_exposes_isin_totals": {"false": 3},
                "official_masterfile_exposes_sector_totals": {"false": 3},
                "official_masterfile_source_totals": {"asx_investment_products": 1, "asx_listed_companies": 2},
                "policy": {
                    "official_first": "ASX official workbooks first.",
                    "no_guessing": "No ASX ISIN or category is inferred.",
                    "scope_first": "Scope first before fill.",
                },
            },
            "rows": rows,
        }
    )

    assert result["passed"] is True
    assert result["rows"] == 3


def test_evaluate_asx_residual_gate_fails_for_unsafe_apply_or_stale_summary() -> None:
    result = evaluate_asx_residual_gate(
        {
            "_meta": {
                "generated_at": "not-a-timestamp",
                "policy": {"official_first": "ASX first"},
                "source_files": {
                    "tickers_csv": "data/tickers.csv",
                    "masterfile_reference_csv": "data/masterfiles/reference.csv",
                    "source_gap_classification_csv": "data/reports/source_gap_classification.csv",
                    "source_of_truth_decisions_csv": "data/reports/source_of_truth_decisions.csv",
                    "asx_isin_probe_csv": "data/wrong.csv",
                    "unexpected_csv": "data/unexpected.csv",
                },
            },
            "summary": {
                "rows": 2,
                "field_totals": {"missing_isin_primary": 2},
                "asset_type_totals": {"Stock": 2},
                "gap_class_totals": {"official_identifier_reference_unmatched_gap": 2},
                "source_of_truth_outcome_totals": {"accepted_source_gap": 2},
                "asx_resolution_queue_totals": {"official_asx_isin_candidate_apply_gate": 2},
                "asx_resolution_queue_field_totals": {
                    "official_asx_isin_candidate_apply_gate": {"missing_isin_primary": 2}
                },
                "residual_decision_totals": {"official_asx_isin_available_review_apply": 2},
                "review_bucket_totals": {"official_isin_candidate_requires_apply_gate": 2},
                "review_priority_totals": {"P1": 2},
                "apply_eligibility_totals": {"apply_now": 2},
                "verification_evidence_required_totals": {"weak": 2},
                "asx_isin_probe_decision_totals": {"accept": 2},
                "official_masterfile_source_totals": {},
                "policy": {"official_first": "ASX first"},
            },
            "rows": [
                {
                    "listing_key": "ASX::BAD",
                    "ticker": "BAD",
                    "exchange": "ASX",
                    "asset_type": "Stock",
                    "field": "missing_isin_primary",
                    "target_field": "isin",
                    "gap_class": "official_identifier_reference_unmatched_gap",
                    "source_of_truth_outcome": "accepted_source_gap",
                    "review_needed": "true",
                    "recommended_next_source": "ASX workbook",
                    "source_gate": "weak",
                    "official_masterfile_sources": "",
                    "asx_isin_probe_decision": "accept",
                    "asx_isin_probe_isin": "",
                    "asx_resolution_queue": "official_asx_isin_candidate_apply_gate",
                    "residual_decision": "official_asx_isin_available_review_apply",
                    "review_bucket": "official_isin_candidate_requires_apply_gate",
                    "review_priority": "P1",
                    "review_strategy": "weak_strategy",
                    "apply_eligibility": "apply_now",
                    "verification_evidence_required": "weak",
                    "recommended_next_action": "apply",
                }
            ],
        }
    )

    assert result["passed"] is False
    assert result["invalid_generated_at"] == "not-a-timestamp"
    assert result["source_file_mismatches"] == {
        "asx_isin_probe_csv": {
            "expected": "data/asx_verification/missing_isin_backfill.csv",
            "actual": "data/wrong.csv",
        }
    }
    assert result["unexpected_source_files"] == ["unexpected_csv"]
    assert result["row_count_mismatch"] == {"reported": 2, "actual": 1}
    assert set(result["row_gaps"][0]["invalid_fields"]) == {
        "asx_isin_probe_isin",
        "official_capability",
        "official_source_context",
        "recommended_next_source",
        "review_strategy",
        "source_gate",
        "verification_evidence_required",
    }
    assert result["unsafe_apply_rows"] == [
        {
            "row_index": 0,
            "listing_key": "ASX::BAD",
            "apply_eligibility": "apply_now",
        }
    ]
    assert result["missing_policy_keys"] == ["no_guessing", "scope_first"]
    assert result["summary_mismatches"]["apply_eligibility_totals"] == {
        "apply_now": {"reported": 2, "actual": 1},
    }


def test_evaluate_weak_sector_residual_gate_requires_blocked_official_sector_candidates() -> None:
    rows = [
        {
            "listing_key": "NGX::ABC",
            "ticker": "ABC",
            "exchange": "NGX",
            "asset_type": "Stock",
            "gap_class": "official_industry_taxonomy_unavailable_gap",
            "source_of_truth_outcome": "accepted_source_gap",
            "review_needed": "true",
            "recommended_next_source": "Official venue profile.",
            "source_gate": "Normalize official sector after exact listing-key match.",
            "official_masterfile_match": "true",
            "official_masterfile_sources": "ngx_company_profile_directory|ngx_equities_price_list",
            "official_masterfile_exposes_sector": "true",
            "official_masterfile_sector_values": "SERVICES",
            "weak_sector_resolution_queue": "official_sector_candidate_normalization_review",
            "residual_decision": "official_sector_available_review_apply",
            "review_bucket": "official_sector_candidate_requires_normalization_gate",
            "review_priority": "P1",
            "apply_eligibility": "blocked_until_canonical_sector_normalization_and_listing_key_gate",
            "verification_evidence_required": "official_venue_sector_value_with_canonical_mapping_and_exact_listing_key_match",
            "recommended_next_action": "apply_only_after_official_sector_normalization_and_listing_key_gate",
        },
        {
            "listing_key": "PSE::CPX",
            "ticker": "CPX",
            "exchange": "PSE",
            "asset_type": "Stock",
            "gap_class": "shell_or_cpc_sector_gap",
            "source_of_truth_outcome": "core_exclusion_candidate",
            "review_needed": "true",
            "recommended_next_source": "Scope review.",
            "source_gate": "Decide core or extended scope before enrichment.",
            "official_masterfile_match": "true",
            "official_masterfile_sources": "pse_listed_company_directory",
            "official_masterfile_exposes_sector": "false",
            "official_masterfile_sector_values": "",
            "weak_sector_resolution_queue": "core_exclusion_candidate_scope_review_before_sector_fill",
            "residual_decision": "core_exclusion_candidate_requires_scope_review",
            "review_bucket": "scope_review_before_sector_fill",
            "review_priority": "P1",
            "apply_eligibility": "blocked_until_core_or_extended_scope_decision",
            "verification_evidence_required": "scope_decision_for_core_extended_or_exclude_before_sector_enrichment",
            "recommended_next_action": "review_scope_before_sector_enrichment",
        },
        {
            "listing_key": "BK::KFIN",
            "ticker": "KFIN",
            "exchange": "BK",
            "asset_type": "Stock",
            "gap_class": "official_industry_taxonomy_unavailable_gap",
            "source_of_truth_outcome": "accepted_source_gap",
            "review_needed": "true",
            "recommended_next_source": "Official masterfile with exposed sector.",
            "source_gate": "Keep blank until official sector exists.",
            "official_masterfile_match": "true",
            "official_masterfile_sources": "boursa_kuwait_stocks",
            "official_masterfile_exposes_sector": "false",
            "official_masterfile_sector_values": "",
            "weak_sector_resolution_queue": "official_masterfile_without_sector_source_gap",
            "residual_decision": "accepted_source_gap_official_masterfile_without_sector",
            "review_bucket": "official_masterfile_without_sector_source_gap",
            "review_priority": "P3",
            "apply_eligibility": "keep_sector_blank_until_official_masterfile_exposes_sector_or_reviewed_taxonomy",
            "verification_evidence_required": "official_masterfile_or_issuer_taxonomy_update_exposing_sector_for_exact_listing",
            "recommended_next_action": "keep_sector_blank_until_official_masterfile_exposes_sector_or_reviewed_taxonomy",
        },
        {
            "listing_key": "OSL::OSE",
            "ticker": "OSE",
            "exchange": "OSL",
            "asset_type": "Stock",
            "gap_class": "official_industry_taxonomy_unavailable_gap",
            "source_of_truth_outcome": "accepted_source_gap",
            "review_needed": "true",
            "recommended_next_source": "Official venue taxonomy.",
            "source_gate": "Keep blank until venue taxonomy source exists.",
            "official_masterfile_match": "true",
            "official_masterfile_sources": "euronext_equities",
            "official_masterfile_exposes_sector": "false",
            "official_masterfile_sector_values": "",
            "weak_sector_resolution_queue": "venue_official_taxonomy_unavailable_source_gap",
            "residual_decision": "accepted_source_gap_no_official_sector_taxonomy",
            "review_bucket": "venue_official_taxonomy_unavailable_source_gap",
            "review_priority": "P2",
            "apply_eligibility": "keep_sector_blank_until_venue_official_taxonomy_source_exists",
            "verification_evidence_required": "new_or_restored_official_venue_industry_taxonomy_source",
            "recommended_next_action": "keep_sector_blank_until_venue_official_taxonomy_source_exists",
        },
        {
            "listing_key": "CSE_LK::CARG",
            "ticker": "CARG",
            "exchange": "CSE_LK",
            "asset_type": "Stock",
            "gap_class": "official_industry_taxonomy_unavailable_gap",
            "source_of_truth_outcome": "accepted_source_gap",
            "review_needed": "true",
            "recommended_next_source": "Official masterfile with exposed sector.",
            "source_gate": "Keep blank until official sector exists.",
            "official_masterfile_match": "true",
            "official_masterfile_sources": "cse_lk_all_security_code",
            "official_masterfile_exposes_sector": "false",
            "official_masterfile_sector_values": "",
            "weak_sector_resolution_queue": "official_masterfile_without_sector_source_gap",
            "residual_decision": "accepted_source_gap_official_masterfile_without_sector",
            "review_bucket": "official_masterfile_without_sector_source_gap",
            "review_priority": "P3",
            "apply_eligibility": "keep_sector_blank_until_official_masterfile_exposes_sector_or_reviewed_taxonomy",
            "verification_evidence_required": "official_masterfile_or_issuer_taxonomy_update_exposing_sector_for_exact_listing",
            "recommended_next_action": "keep_sector_blank_until_official_masterfile_exposes_sector_or_reviewed_taxonomy",
        },
        {
            "listing_key": "CSE_MA::AFM",
            "ticker": "AFM",
            "exchange": "CSE_MA",
            "asset_type": "Stock",
            "gap_class": "official_industry_taxonomy_unavailable_gap",
            "source_of_truth_outcome": "accepted_source_gap",
            "review_needed": "true",
            "recommended_next_source": "Official venue taxonomy.",
            "source_gate": "Keep blank until venue taxonomy source exists.",
            "official_masterfile_match": "true",
            "official_masterfile_sources": "euronext_equities",
            "official_masterfile_exposes_sector": "false",
            "official_masterfile_sector_values": "",
            "weak_sector_resolution_queue": "venue_official_taxonomy_unavailable_source_gap",
            "residual_decision": "accepted_source_gap_no_official_sector_taxonomy",
            "review_bucket": "venue_official_taxonomy_unavailable_source_gap",
            "review_priority": "P2",
            "apply_eligibility": "keep_sector_blank_until_venue_official_taxonomy_source_exists",
            "verification_evidence_required": "new_or_restored_official_venue_industry_taxonomy_source",
            "recommended_next_action": "keep_sector_blank_until_venue_official_taxonomy_source_exists",
        },
        {
            "listing_key": "Euronext::ABI",
            "ticker": "ABI",
            "exchange": "Euronext",
            "asset_type": "Stock",
            "gap_class": "official_industry_taxonomy_unavailable_gap",
            "source_of_truth_outcome": "accepted_source_gap",
            "review_needed": "true",
            "recommended_next_source": "Official venue taxonomy.",
            "source_gate": "Keep blank until venue taxonomy source exists.",
            "official_masterfile_match": "true",
            "official_masterfile_sources": "euronext_equities",
            "official_masterfile_exposes_sector": "false",
            "official_masterfile_sector_values": "",
            "weak_sector_resolution_queue": "venue_official_taxonomy_unavailable_source_gap",
            "residual_decision": "accepted_source_gap_no_official_sector_taxonomy",
            "review_bucket": "venue_official_taxonomy_unavailable_source_gap",
            "review_priority": "P2",
            "apply_eligibility": "keep_sector_blank_until_venue_official_taxonomy_source_exists",
            "verification_evidence_required": "new_or_restored_official_venue_industry_taxonomy_source",
            "recommended_next_action": "keep_sector_blank_until_venue_official_taxonomy_source_exists",
        },
        {
            "listing_key": "SEM::MCBG",
            "ticker": "MCBG",
            "exchange": "SEM",
            "asset_type": "Stock",
            "gap_class": "official_industry_taxonomy_unavailable_gap",
            "source_of_truth_outcome": "accepted_source_gap",
            "review_needed": "true",
            "recommended_next_source": "Official masterfile with exposed sector.",
            "source_gate": "Keep blank until official sector exists.",
            "official_masterfile_match": "true",
            "official_masterfile_sources": "sem_isin",
            "official_masterfile_exposes_sector": "false",
            "official_masterfile_sector_values": "",
            "weak_sector_resolution_queue": "official_masterfile_without_sector_source_gap",
            "residual_decision": "accepted_source_gap_official_masterfile_without_sector",
            "review_bucket": "official_masterfile_without_sector_source_gap",
            "review_priority": "P3",
            "apply_eligibility": "keep_sector_blank_until_official_masterfile_exposes_sector_or_reviewed_taxonomy",
            "verification_evidence_required": "official_masterfile_or_issuer_taxonomy_update_exposing_sector_for_exact_listing",
            "recommended_next_action": "keep_sector_blank_until_official_masterfile_exposes_sector_or_reviewed_taxonomy",
        },
    ]
    for row in rows:
        row["review_strategy"] = weak_sector_review_strategy_for(row["weak_sector_resolution_queue"])[0]
        official_source = row["official_masterfile_sources"] or "none"
        sector_values = row["official_masterfile_sector_values"] or "none"
        row["recommended_next_source"] = weak_sector_recommended_next_source_for(
            row["weak_sector_resolution_queue"],
            official_source,
        )
        row["source_gate"] = weak_sector_source_gate_for(row["weak_sector_resolution_queue"])
        row["official_source_context"] = (
            f"official_masterfile_sources={official_source};official_sector_values={sector_values}"
        )
        row["official_capability"] = (
            f"masterfile_match={row['official_masterfile_match']};"
            f"masterfile_exposes_sector={row['official_masterfile_exposes_sector']}"
        )
    result = evaluate_weak_sector_residual_gate(
        {
            "_meta": {
                "generated_at": "2026-05-24T00:00:00Z",
                "policy": {"venue_specific": "Configured weak-sector venues only."},
                "exchanges": ["BK", "CSE_LK", "CSE_MA", "Euronext", "NGX", "OSL", "PSE", "SEM"],
                "source_files": {
                    "tickers_csv": "data/tickers.csv",
                    "masterfile_reference_csv": "data/masterfiles/reference.csv",
                    "source_gap_classification_csv": "data/reports/source_gap_classification.csv",
                    "source_of_truth_decisions_csv": "data/reports/source_of_truth_decisions.csv",
                },
            },
            "summary": {
                "exchanges": ["BK", "CSE_LK", "CSE_MA", "Euronext", "NGX", "OSL", "PSE", "SEM"],
                "rows": 8,
                "exchange_totals": {
                    "BK": 1,
                    "CSE_LK": 1,
                    "CSE_MA": 1,
                    "Euronext": 1,
                    "NGX": 1,
                    "OSL": 1,
                    "PSE": 1,
                    "SEM": 1,
                },
                "gap_class_totals": {
                    "official_industry_taxonomy_unavailable_gap": 7,
                    "shell_or_cpc_sector_gap": 1,
                },
                "source_of_truth_outcome_totals": {"accepted_source_gap": 7, "core_exclusion_candidate": 1},
                "weak_sector_resolution_queue_totals": {
                    "core_exclusion_candidate_scope_review_before_sector_fill": 1,
                    "official_masterfile_without_sector_source_gap": 3,
                    "official_sector_candidate_normalization_review": 1,
                    "venue_official_taxonomy_unavailable_source_gap": 3,
                },
                "weak_sector_resolution_queue_exchange_totals": {
                    "core_exclusion_candidate_scope_review_before_sector_fill": {"PSE": 1},
                    "official_masterfile_without_sector_source_gap": {"BK": 1, "CSE_LK": 1, "SEM": 1},
                    "official_sector_candidate_normalization_review": {"NGX": 1},
                    "venue_official_taxonomy_unavailable_source_gap": {
                        "CSE_MA": 1,
                        "Euronext": 1,
                        "OSL": 1,
                    },
                },
                "weak_sector_resolution_queue_gap_class_totals": {
                    "core_exclusion_candidate_scope_review_before_sector_fill": {"shell_or_cpc_sector_gap": 1},
                    "official_masterfile_without_sector_source_gap": {
                        "official_industry_taxonomy_unavailable_gap": 3
                    },
                    "official_sector_candidate_normalization_review": {
                        "official_industry_taxonomy_unavailable_gap": 1
                    },
                    "venue_official_taxonomy_unavailable_source_gap": {
                        "official_industry_taxonomy_unavailable_gap": 3
                    },
                },
                "weak_sector_resolution_queue_official_source_totals": {
                    "core_exclusion_candidate_scope_review_before_sector_fill": {
                        "pse_listed_company_directory": 1
                    },
                    "official_masterfile_without_sector_source_gap": {
                        "boursa_kuwait_stocks": 1,
                        "cse_lk_all_security_code": 1,
                        "sem_isin": 1,
                    },
                    "official_sector_candidate_normalization_review": {
                        "ngx_company_profile_directory": 1,
                        "ngx_equities_price_list": 1,
                    },
                    "venue_official_taxonomy_unavailable_source_gap": {"euronext_equities": 3},
                },
                "weak_sector_resolution_queue_review_strategy_totals": {
                    "core_exclusion_candidate_scope_review_before_sector_fill": {
                        "scope_review_before_weak_sector_enrichment": 1
                    },
                    "official_masterfile_without_sector_source_gap": {
                        "keep_blank_until_official_masterfile_or_issuer_sector_source": 3
                    },
                    "official_sector_candidate_normalization_review": {
                        "normalize_official_sector_candidate_before_apply": 1
                    },
                    "venue_official_taxonomy_unavailable_source_gap": {
                        "restore_or_add_venue_official_taxonomy_parser": 3
                    },
                },
                "weak_sector_resolution_queue_official_capability_totals": {
                    "core_exclusion_candidate_scope_review_before_sector_fill": {
                        "masterfile_exposes_sector=false": 1,
                        "masterfile_match=true": 1,
                    },
                    "official_masterfile_without_sector_source_gap": {
                        "masterfile_exposes_sector=false": 3,
                        "masterfile_match=true": 3,
                    },
                    "official_sector_candidate_normalization_review": {
                        "masterfile_exposes_sector=true": 1,
                        "masterfile_match=true": 1,
                    },
                    "venue_official_taxonomy_unavailable_source_gap": {
                        "masterfile_exposes_sector=false": 3,
                        "masterfile_match=true": 3,
                    },
                },
                "venue_backlog_exchange_queue_totals": {
                    "BK": {"official_masterfile_without_sector_source_gap": 1},
                    "CSE_LK": {"official_masterfile_without_sector_source_gap": 1},
                    "CSE_MA": {"venue_official_taxonomy_unavailable_source_gap": 1},
                    "Euronext": {"venue_official_taxonomy_unavailable_source_gap": 1},
                    "NGX": {"official_sector_candidate_normalization_review": 1},
                    "OSL": {"venue_official_taxonomy_unavailable_source_gap": 1},
                    "PSE": {"core_exclusion_candidate_scope_review_before_sector_fill": 1},
                    "SEM": {"official_masterfile_without_sector_source_gap": 1},
                },
                "venue_backlog_exchange_official_capability_totals": {
                    "BK": {"masterfile_exposes_sector=false": 1, "masterfile_match=true": 1},
                    "CSE_LK": {"masterfile_exposes_sector=false": 1, "masterfile_match=true": 1},
                    "CSE_MA": {"masterfile_exposes_sector=false": 1, "masterfile_match=true": 1},
                    "Euronext": {"masterfile_exposes_sector=false": 1, "masterfile_match=true": 1},
                    "NGX": {"masterfile_exposes_sector=true": 1, "masterfile_match=true": 1},
                    "OSL": {"masterfile_exposes_sector=false": 1, "masterfile_match=true": 1},
                    "PSE": {"masterfile_exposes_sector=false": 1, "masterfile_match=true": 1},
                    "SEM": {"masterfile_exposes_sector=false": 1, "masterfile_match=true": 1},
                },
                "top_venue_backlog_batches": [
                    {
                        "weak_sector_resolution_queue": "venue_official_taxonomy_unavailable_source_gap",
                        "exchange": "Euronext",
                        "official_source": "euronext_equities",
                        "rows": 1,
                        "review_strategy": "restore_or_add_venue_official_taxonomy_parser",
                        "evidence_required": "new_or_restored_official_venue_industry_taxonomy_source",
                        "recommended_next_source": "Official venue industry or sector taxonomy source for the exchange.",
                        "source_gate": "Keep sector blank until a venue-official taxonomy parser or source exists.",
                    }
                ],
                "top_weak_sector_resolution_review_batches": [
                    {
                        "weak_sector_resolution_queue": "venue_official_taxonomy_unavailable_source_gap",
                        "exchange": "Euronext",
                        "official_source": "euronext_equities",
                        "rows": 1,
                        "review_strategy": "restore_or_add_venue_official_taxonomy_parser",
                        "evidence_required": "new_or_restored_official_venue_industry_taxonomy_source",
                        "recommended_next_source": "Official venue industry or sector taxonomy source for the exchange.",
                        "source_gate": "Keep sector blank until a venue-official taxonomy parser or source exists.",
                    }
                ],
                "residual_decision_totals": {
                    "accepted_source_gap_no_official_sector_taxonomy": 3,
                    "accepted_source_gap_official_masterfile_without_sector": 3,
                    "core_exclusion_candidate_requires_scope_review": 1,
                    "official_sector_available_review_apply": 1,
                },
                "review_bucket_totals": {
                    "official_masterfile_without_sector_source_gap": 3,
                    "official_sector_candidate_requires_normalization_gate": 1,
                    "scope_review_before_sector_fill": 1,
                    "venue_official_taxonomy_unavailable_source_gap": 3,
                },
                "review_priority_totals": {"P1": 2, "P2": 3, "P3": 3},
                "apply_eligibility_totals": {
                    "blocked_until_canonical_sector_normalization_and_listing_key_gate": 1,
                    "blocked_until_core_or_extended_scope_decision": 1,
                    "keep_sector_blank_until_official_masterfile_exposes_sector_or_reviewed_taxonomy": 3,
                    "keep_sector_blank_until_venue_official_taxonomy_source_exists": 3,
                },
                "verification_evidence_required_totals": {
                    "new_or_restored_official_venue_industry_taxonomy_source": 3,
                    "official_masterfile_or_issuer_taxonomy_update_exposing_sector_for_exact_listing": 3,
                    "official_venue_sector_value_with_canonical_mapping_and_exact_listing_key_match": 1,
                    "scope_decision_for_core_extended_or_exclude_before_sector_enrichment": 1,
                },
                "weak_sector_backlog": {
                    "status": "venue_specific_review_queue_open",
                    "rows": 8,
                    "official_sector_candidate_rows": 1,
                    "scope_decision_required_rows": 1,
                    "masterfile_without_sector_rows": 3,
                    "venue_taxonomy_source_required_rows": 3,
                    "direct_sector_apply_allowed_rows": 0,
                    "metadata_enrichment_authorized": False,
                    "source_gate": (
                        "Weak-sector enrichment remains blocked unless venue-official masterfile, issuer, "
                        "or reviewed taxonomy evidence maps the exact listing to a canonical sector; "
                        "no global symbol/name/peer inference is allowed."
                    ),
                },
                "weak_sector_backlog_queue_totals": {
                    "core_exclusion_candidate_scope_review_before_sector_fill": 1,
                    "official_masterfile_without_sector_source_gap": 3,
                    "official_sector_candidate_normalization_review": 1,
                    "venue_official_taxonomy_unavailable_source_gap": 3,
                },
                "weak_sector_backlog_evidence_required_totals": {
                    "new_or_restored_official_venue_industry_taxonomy_source": 3,
                    "official_masterfile_or_issuer_taxonomy_update_exposing_sector_for_exact_listing": 3,
                    "official_venue_sector_value_with_canonical_mapping_and_exact_listing_key_match": 1,
                    "scope_decision_for_core_extended_or_exclude_before_sector_enrichment": 1,
                },
                "official_masterfile_match_totals": {"true": 8},
                "official_masterfile_exposes_sector_totals": {"false": 7, "true": 1},
                "official_masterfile_source_totals": {
                    "boursa_kuwait_stocks": 1,
                    "cse_lk_all_security_code": 1,
                    "euronext_equities": 3,
                    "ngx_company_profile_directory": 1,
                    "ngx_equities_price_list": 1,
                    "pse_listed_company_directory": 1,
                    "sem_isin": 1,
                },
                "official_sector_value_totals": {"SERVICES": 1},
                "official_sector_candidate_rows": 1,
                "official_sector_candidate_exchange_totals": {"NGX": 1},
                "official_sector_candidate_value_totals": {"SERVICES": 1},
                "scope_review_rows": 1,
                "scope_review_exchange_totals": {"PSE": 1},
                "scope_review_gap_class_totals": {"shell_or_cpc_sector_gap": 1},
                "masterfile_without_sector_rows": 3,
                "masterfile_without_sector_exchange_totals": {"BK": 1, "CSE_LK": 1, "SEM": 1},
                "policy": {
                    "venue_specific": "Each weak-sector exchange is reviewed against venue-specific evidence.",
                    "official_first": "Official exchange or issuer taxonomy comes first.",
                    "no_guessing": "No sector is inferred from names or global fuzzy mappings.",
                },
            },
            "rows": rows,
        }
    )

    assert result["passed"] is True
    assert result["rows"] == 8


def test_evaluate_weak_sector_residual_gate_fails_for_unsafe_sector_apply_or_stale_summary() -> None:
    result = evaluate_weak_sector_residual_gate(
        {
            "_meta": {
                "generated_at": "not-a-timestamp",
                "policy": {"official_first": "Official first."},
                "exchanges": ["NGX"],
                "source_files": {
                    "tickers_csv": "data/tickers.csv",
                    "masterfile_reference_csv": "data/wrong.csv",
                    "source_gap_classification_csv": "data/reports/source_gap_classification.csv",
                    "source_of_truth_decisions_csv": "data/reports/source_of_truth_decisions.csv",
                    "unexpected_csv": "data/unexpected.csv",
                },
            },
            "summary": {
                "exchanges": ["NGX"],
                "rows": 2,
                "exchange_totals": {"NGX": 2},
                "gap_class_totals": {"official_industry_taxonomy_unavailable_gap": 2},
                "source_of_truth_outcome_totals": {"accepted_source_gap": 2},
                "weak_sector_resolution_queue_totals": {"official_sector_candidate_normalization_review": 2},
                "weak_sector_resolution_queue_exchange_totals": {
                    "official_sector_candidate_normalization_review": {"NGX": 2}
                },
                "weak_sector_resolution_queue_gap_class_totals": {
                    "official_sector_candidate_normalization_review": {
                        "official_industry_taxonomy_unavailable_gap": 2
                    }
                },
                "weak_sector_resolution_queue_official_source_totals": {
                    "official_sector_candidate_normalization_review": {"ngx_company_profile_directory": 2}
                },
                "residual_decision_totals": {"official_sector_available_review_apply": 2},
                "review_bucket_totals": {"official_sector_candidate_requires_normalization_gate": 2},
                "review_priority_totals": {"P1": 2},
                "apply_eligibility_totals": {"apply_now": 2},
                "verification_evidence_required_totals": {"weak": 2},
                "official_masterfile_source_totals": {"ngx_company_profile_directory": 2},
                "official_sector_value_totals": {"SERVICES": 2},
                "policy": {"official_first": "Official first."},
            },
            "rows": [
                {
                    "listing_key": "NGX::BAD",
                    "ticker": "BAD",
                    "exchange": "NGX",
                    "asset_type": "Stock",
                    "gap_class": "official_industry_taxonomy_unavailable_gap",
                    "source_of_truth_outcome": "accepted_source_gap",
                    "review_needed": "true",
                    "recommended_next_source": "NGX profile",
                    "source_gate": "weak",
                    "official_masterfile_match": "true",
                    "official_masterfile_sources": "ngx_company_profile_directory",
                    "official_masterfile_exposes_sector": "true",
                    "official_masterfile_sector_values": "SERVICES",
                    "weak_sector_resolution_queue": "official_sector_candidate_normalization_review",
                    "residual_decision": "official_sector_available_review_apply",
                    "review_bucket": "official_sector_candidate_requires_normalization_gate",
                    "review_priority": "P1",
                    "apply_eligibility": "apply_now",
                    "verification_evidence_required": "weak",
                    "review_strategy": "normalize_official_sector_candidate_before_apply",
                    "recommended_next_action": "apply",
                }
            ],
        }
    )

    assert result["passed"] is False
    assert result["invalid_generated_at"] == "not-a-timestamp"
    assert result["source_file_mismatches"] == {
        "masterfile_reference_csv": {
            "expected": "data/masterfiles/reference.csv",
            "actual": "data/wrong.csv",
        }
    }
    assert result["unexpected_source_files"] == ["unexpected_csv"]
    assert result["meta_exchange_mismatch"] == {
        "reported": ["NGX"],
        "expected": ["BK", "CSE_LK", "CSE_MA", "Euronext", "NGX", "OSL", "PSE", "SEM"],
    }
    assert result["row_count_mismatch"] == {"reported": 2, "actual": 1}
    assert result["row_gaps"][0]["invalid_fields"] == [
        "apply_eligibility",
        "verification_evidence_required",
        "recommended_next_action",
        "recommended_next_source",
        "source_gate",
        "official_source_context",
        "official_capability",
    ]
    assert result["unsafe_sector_apply_rows"] == [
        {
            "row_index": 0,
            "listing_key": "NGX::BAD",
            "apply_eligibility": "apply_now",
        }
    ]
    assert result["missing_policy_keys"] == ["venue_specific", "no_guessing"]
    assert result["reported_exchange_mismatch"] == {
        "reported": ["NGX"],
        "observed": ["NGX"],
        "expected": ["BK", "CSE_LK", "CSE_MA", "Euronext", "NGX", "OSL", "PSE", "SEM"],
    }
    assert result["summary_mismatches"]["apply_eligibility_totals"] == {
        "apply_now": {"reported": 2, "actual": 1},
    }


def test_evaluate_before_after_delta_matrix_requires_complete_zero_delta_matrix() -> None:
    result = evaluate_before_after_delta_matrix(
        {
            "_meta": DELTA_META_FIXTURE,
            "summary": {"changed_exchange_rows": 0, "changed_numeric_delta_rows": 0},
            "acceptance_delta_matrix": zero_delta_matrix(),
        }
    )

    assert result["passed"] is True
    assert result["source_file_mismatches"] == {}
    assert result["policy_missing_markers"] == []
    assert result["missing_delta_contexts"] == []
    assert result["invalid_delta_contexts"] == {}


def test_evaluate_before_after_delta_matrix_allows_documented_noncritical_deltas() -> None:
    result = evaluate_before_after_delta_matrix(
        {
            "_meta": DELTA_META_FIXTURE,
            "summary": {"changed_exchange_rows": 1, "changed_numeric_delta_rows": 1},
            "acceptance_delta_matrix": zero_delta_matrix(),
            "exchange_acceptance_delta_matrix": {
                "B3": {
                    "isin_delta": {
                        "baseline": 1,
                        "current": 1,
                        "delta": 0,
                        "delta_context": delta_context(
                            "isin_delta",
                            {"baseline": 1, "current": 1, "delta": 0},
                            "B3",
                        ),
                    }
                }
            },
        }
    )

    assert result["passed"] is True
    assert result["changed_exchange_rows"] == 1
    assert result["changed_numeric_delta_rows"] == 1
    assert result["missing_exchange_delta_contexts"] == {}
    assert result["invalid_exchange_delta_contexts"] == {}


def test_evaluate_before_after_delta_matrix_requires_exchange_delta_contexts() -> None:
    result = evaluate_before_after_delta_matrix(
        {
            "_meta": DELTA_META_FIXTURE,
            "summary": {"changed_exchange_rows": 1, "changed_numeric_delta_rows": 1},
            "acceptance_delta_matrix": zero_delta_matrix(),
            "exchange_acceptance_delta_matrix": {
                "B3": {
                    "isin_delta": {"baseline": 1, "current": 1, "delta": 0},
                    "sector_delta": {
                        "baseline": 1,
                        "current": 2,
                        "delta": 1,
                        "delta_context": "wrong",
                    },
                }
            },
        }
    )

    assert result["passed"] is False
    assert result["missing_exchange_delta_contexts"] == {"B3": ["isin_delta"]}
    assert result["invalid_exchange_delta_contexts"]["B3"]["sector_delta"]["actual"] == "wrong"


def test_evaluate_before_after_delta_matrix_fails_for_missing_or_changed_critical_metrics() -> None:
    result = evaluate_before_after_delta_matrix(
        {
            "_meta": DELTA_META_FIXTURE,
            "summary": {"changed_exchange_rows": 0, "changed_numeric_delta_rows": 0},
            "acceptance_delta_matrix": {
                "isin_delta": {
                    "baseline": 1,
                    "current": 2,
                    "delta": 1,
                    "delta_context": "wrong",
                },
            },
        }
    )

    assert result["passed"] is False
    assert result["non_zero_deltas"] == ["isin_delta"]
    assert result["invalid_delta_contexts"]["isin_delta"]["actual"] == "wrong"
    assert "sector_delta" in result["missing_keys"]


def test_evaluate_before_after_delta_matrix_fails_for_stale_delta_metadata() -> None:
    result = evaluate_before_after_delta_matrix(
        {
            "_meta": {
                "generated_at": "not-a-timestamp",
                "baseline_generated_at": "2026-05-24T00:00:00Z",
                "current_generated_at": "also-bad",
                "baseline_path": "data/reports/other.json",
                "source_files": {
                    "baseline_snapshot": "data/reports/other.json",
                    "unexpected_report": "data/reports/unexpected.json",
                },
                "policy": "Delta report.",
            },
            "summary": {"changed_exchange_rows": 0, "changed_numeric_delta_rows": 0},
            "acceptance_delta_matrix": zero_delta_matrix(),
        }
    )

    assert result["passed"] is False
    assert result["invalid_generated_at"] == "not-a-timestamp"
    assert result["invalid_current_generated_at"] == "also-bad"
    assert result["baseline_path_mismatch"] is True
    assert result["source_file_mismatches"]["baseline_snapshot"] == {
        "expected": "data/reports/improvement_baseline.json",
        "actual": "data/reports/other.json",
    }
    assert result["unexpected_source_files"] == ["unexpected_report"]
    assert set(result["policy_missing_markers"]) == {
        "Delta report only",
        "source-level review",
        "before any data change is inferred",
    }


def test_evaluate_campaign_acceptance_matrices_requires_all_campaigns_to_have_matrix() -> None:
    result = evaluate_campaign_acceptance_matrices(
        {
            "summary": {"campaigns": 10},
            "campaigns": [
                {
                    "campaign_key": f"campaign_{index}",
                    "acceptance_matrix": {
                        "exchange_scope": "all",
                        "affected_artifact_rows": index,
                        "campaign_metric_deltas": {},
                        "global_acceptance_deltas": {
                            key: {"baseline": 1, "current": 1, "delta": 0}
                            for key in REQUIRED_DELTA_KEYS
                        },
                        "exchange_scope_deltas": {
                            "exchange_count": 1,
                            "changed_exchange_rows": 0,
                            "delta_totals": {key: 0 for key in REQUIRED_DELTA_KEYS},
                        },
                        "required_metrics": REQUIRED_DELTA_KEYS,
                    },
                }
                for index in range(10)
            ],
        }
    )

    assert result["passed"] is True
    assert result["campaigns"] == 10


def test_evaluate_campaign_acceptance_matrices_requires_before_after_for_report_campaigns() -> None:
    matrix = {
        "exchange_scope": ["B3"],
        "affected_artifact_rows": 1,
        "campaign_metric_deltas": {},
        "global_acceptance_deltas": {
            key: {"baseline": 1, "current": 1, "delta": 0}
            for key in REQUIRED_DELTA_KEYS
        },
        "exchange_scope_deltas": {
            "exchange_count": 1,
            "changed_exchange_rows": 0,
            "delta_totals": {key: 0 for key in REQUIRED_DELTA_KEYS},
        },
        "required_metrics": REQUIRED_DELTA_KEYS,
    }

    result = evaluate_campaign_acceptance_matrices(
        {
            "summary": {"campaigns": 10},
            "campaigns": [
                {
                    "campaign_key": "b3",
                    "status": "partial",
                    "artifacts": [{"path": "data/reports/b3.json", "rows": 1}],
                    "acceptance_matrix": matrix,
                },
                *[
                    {
                        "campaign_key": f"campaign_{index}",
                        "acceptance_matrix": matrix,
                    }
                    for index in range(1, 10)
                ],
            ],
        }
    )

    assert result["passed"] is False
    assert result["missing_before_after_summaries"] == ["b3"]


def test_evaluate_campaign_acceptance_matrices_requires_exact_before_after_context() -> None:
    matrix = {
        "exchange_scope": ["B3"],
        "affected_artifact_rows": 1,
        "campaign_metric_deltas": {},
        "global_acceptance_deltas": {
            key: {"baseline": 1, "current": 1, "delta": 0}
            for key in REQUIRED_DELTA_KEYS
        },
        "exchange_scope_deltas": {
            "exchange_count": 1,
            "changed_exchange_rows": 0,
            "delta_totals": {key: 0 for key in REQUIRED_DELTA_KEYS},
        },
        "required_metrics": REQUIRED_DELTA_KEYS,
    }
    before_after = {
        "exchange_scope": ["B3"],
        "affected_artifact_rows": 1,
        "global_before_after": {
            key: {"before": 1, "after": 1, "delta": 0}
            for key in REQUIRED_DELTA_KEYS
        },
        "exchange_scope_delta_totals": {key: 0 for key in REQUIRED_DELTA_KEYS},
        "warn_quarantine_delta": {
            "warn_delta": {"before": 1, "after": 1, "delta": 0},
            "quarantine_delta": {"before": 1, "after": 1, "delta": 0},
        },
        "policy": "Before/after values are release evidence only.",
    }
    before_after["before_after_context"] = campaign_before_after_context(before_after)

    result = evaluate_campaign_acceptance_matrices(
        {
            "summary": {"campaigns": 10},
            "campaigns": [
                {
                    "campaign_key": "campaign_0",
                    "status": "partial",
                    "artifacts": [{"path": "data/reports/campaign_0.json", "rows": 1}],
                    "acceptance_matrix": matrix,
                    "before_after_summary": before_after,
                },
                *[
                    {"campaign_key": f"campaign_{index}", "acceptance_matrix": matrix}
                    for index in range(1, 10)
                ],
            ],
        }
    )

    assert result["passed"] is True

    before_after["before_after_context"] = "wrong"
    stale = evaluate_campaign_acceptance_matrices(
        {
            "summary": {"campaigns": 10},
            "campaigns": [
                {
                    "campaign_key": "campaign_0",
                    "status": "partial",
                    "artifacts": [{"path": "data/reports/campaign_0.json", "rows": 1}],
                    "acceptance_matrix": matrix,
                    "before_after_summary": before_after,
                },
                *[
                    {"campaign_key": f"campaign_{index}", "acceptance_matrix": matrix}
                    for index in range(1, 10)
                ],
            ],
        }
    )

    assert stale["passed"] is False
    assert stale["before_after_mismatches"][0]["field"] == "before_after_context"
    assert stale["before_after_mismatches"][0]["before_after"] == "wrong"


def test_evaluate_campaign_acceptance_matrices_checks_exchange_delta_consistency() -> None:
    result = evaluate_campaign_acceptance_matrices(
        {
            "summary": {"campaigns": 10},
            "campaigns": [
                {
                    "campaign_key": f"campaign_{index}",
                    "acceptance_matrix": {
                        "exchange_scope": "all",
                        "affected_artifact_rows": index,
                        "campaign_metric_deltas": {},
                        "global_acceptance_deltas": {
                            **{
                                key: {"baseline": 1, "current": 1, "delta": 0}
                                for key in REQUIRED_DELTA_KEYS
                            },
                            "isin_delta": {"baseline": 1, "current": 2, "delta": 1},
                        },
                        "exchange_scope_deltas": {
                            "exchange_count": 2,
                            "changed_exchange_rows": 0,
                            "delta_totals": {
                                **{key: 0 for key in REQUIRED_DELTA_KEYS},
                                "isin_delta": 2,
                            },
                        },
                        "required_metrics": REQUIRED_DELTA_KEYS,
                    },
                }
                for index in range(10)
            ],
        }
    )

    assert result["passed"] is False
    assert result["exchange_scope_delta_consistency_gaps"][0] == {
        "campaign_key": "campaign_0",
        "field": "exchange_scope_deltas.changed_exchange_rows",
        "changed_exchange_rows": 0,
        "non_zero_delta_totals": ["isin_delta"],
    }
    assert result["exchange_scope_delta_consistency_gaps"][1] == {
        "campaign_key": "campaign_0",
        "field": "exchange_scope_deltas.delta_totals",
        "reason": "all_scope_totals_must_match_global_acceptance_deltas",
        "mismatches": {"isin_delta": {"scope_delta_total": 2, "global_delta": 1}},
    }


def test_evaluate_campaign_acceptance_matrices_fails_for_missing_matrix_fields() -> None:
    result = evaluate_campaign_acceptance_matrices(
        {
            "summary": {"campaigns": 2},
            "campaigns": [
                {"campaign_key": "b3"},
                {
                    "campaign_key": "otc",
                    "acceptance_matrix": {
                        "required_metrics": ["isin_delta"],
                        "affected_artifact_rows": -1,
                        "global_acceptance_deltas": {"isin_delta": {"delta": 0}},
                        "exchange_scope_deltas": {"delta_totals": {"isin_delta": 0}},
                    },
                },
            ],
        }
    )

    assert result["passed"] is False
    assert result["missing_matrices"] == ["b3"]
    assert result["missing_required_metrics"]["otc"] == [
        "sector_delta",
        "category_delta",
        "source_gap_delta",
        "warn_delta",
        "quarantine_delta",
    ]
    assert result["missing_exchange_scopes"] == ["otc"]
    assert result["missing_affected_rows"] == ["otc"]
    assert result["missing_campaign_metric_deltas"] == ["otc"]
    assert result["missing_global_acceptance_deltas"]["otc"] == [
        "sector_delta",
        "category_delta",
        "source_gap_delta",
        "warn_delta",
        "quarantine_delta",
    ]
    assert result["missing_exchange_scope_delta_fields"]["otc"] == ["exchange_count", "changed_exchange_rows"]
    assert result["missing_exchange_scope_delta_totals"]["otc"] == [
        "sector_delta",
        "category_delta",
        "source_gap_delta",
        "warn_delta",
        "quarantine_delta",
    ]


def test_evaluate_campaign_acceptance_matrices_requires_b3_masterfile_diagnostic_evidence() -> None:
    def matrix() -> dict[str, object]:
        return {
            "exchange_scope": ["B3"],
            "affected_artifact_rows": 1,
            "campaign_metric_deltas": {
                "residual_rows": {"baseline": 6, "current": 6, "delta": 0},
            },
            "global_acceptance_deltas": {
                key: {"baseline": 1, "current": 1, "delta": 0}
                for key in REQUIRED_DELTA_KEYS
            },
            "exchange_scope_deltas": {
                "exchange_count": 1,
                "changed_exchange_rows": 0,
                "delta_totals": {key: 0 for key in REQUIRED_DELTA_KEYS},
            },
            "required_metrics": REQUIRED_DELTA_KEYS,
        }

    campaigns = [
        {
            "campaign_key": "b3",
            "acceptance_matrix": matrix(),
            "evidence": {
                "b3_dataset_rows": 3,
                "b3_active_exchange_directory_rows": 4,
                "b3_all_masterfile_rows": 5,
                "b3_masterfile_matched_dataset_rows": 2,
                "b3_masterfile_missing_dataset_rows": 1,
                "b3_masterfile_dataset_match_rate": 66.67,
                "b3_official_any_source_matched_dataset_rows": 3,
                "b3_official_any_source_missing_dataset_rows": 0,
                "b3_official_any_source_match_rate": 100.0,
                "b3_missing_category_totals": {"unit_or_fund_line": 1},
                "b3_missing_asset_type_totals": {"ETF": 1},
                "b3_missing_source_presence_totals": {"present_only_in_non_exchange_directory_source": 1},
                "b3_missing_examples": {
                    "unit_or_fund_line": [{"listing_key": "B3::AFOF11"}],
                },
                "b3_masterfile_gap_review_rows": 1,
                "b3_masterfile_gap_review_open_rows": 1,
                "b3_masterfile_gap_review_closed_no_data_change_rows": 0,
                "b3_masterfile_gap_review_source_presence_totals": {"present_only_in_non_exchange_directory_source": 1},
                "b3_masterfile_gap_review_open_source_presence_totals": {
                    "present_only_in_non_exchange_directory_source": 1
                },
                "b3_masterfile_gap_review_bucket_totals": {"official_b3_non_directory_source_review": 1},
                "b3_masterfile_gap_review_resolution_queue_totals": {
                    "official_subset_category_requires_review": 1
                },
                "b3_masterfile_gap_review_open_resolution_queue_totals": {
                    "official_subset_category_requires_review": 1
                },
                "b3_masterfile_gap_review_open_next_source_totals": {
                    "Official B3 subset source plus category taxonomy evidence with exact listing-key match.": 1
                },
                "b3_masterfile_gap_review_open_evidence_path_totals": {
                    "official_b3_subset_category_apply_evidence": 1,
                },
                "b3_masterfile_gap_review_source_gap_resolution_gate_totals": {
                    "apply_only_after_listing_keyed_category_review": 1,
                },
                "b3_masterfile_gap_review_strategy_totals": {
                    "review_official_subset_category_and_scope_before_apply_gate": 1,
                },
                "b3_masterfile_gap_review_resolution_queue_asset_type_totals": {
                    "official_subset_category_requires_review": {"ETF": 1}
                },
                "b3_masterfile_gap_review_resolution_queue_gap_category_totals": {
                    "official_subset_category_requires_review": {"unit_or_fund_line": 1}
                },
                "b3_masterfile_gap_review_candidate_source_totals": {"b3_listed_etfs": 1},
                "b3_masterfile_gap_review_candidate_sector_present_rows": 1,
                "b3_masterfile_gap_review_candidate_isin_present_rows": 0,
                "b3_masterfile_gap_review_candidate_category_review_decision_totals": {
                    "official_candidate_category_differs_from_current_requires_review": 1
                },
                "b3_masterfile_gap_review_official_subset_review_decision_totals": {
                    "official_subset_category_mismatch_requires_apply_gate": 1
                },
                "b3_masterfile_gap_review_official_subset_closure_eligibility_totals": {
                    "blocked_until_category_apply_gate": 1
                },
                "b3_masterfile_gap_review_official_subset_closure_ready_rows": 0,
                "b3_masterfile_gap_review_candidate_category_mismatch_rows": 1,
                "b3_masterfile_gap_review_candidate_category_mismatch_examples": [
                    {
                        "listing_key": "B3::AFOF11",
                        "ticker": "AFOF11",
                        "current_etf_category": "Equity",
                        "candidate_sectors": "Fixed Income",
                        "candidate_sources": "b3_listed_etfs",
                    }
                ],
                "b3_masterfile_gap_review_coverage_diagnosis": {
                    "status": "active_directory_coverage_has_official_subset_parser_or_scope_gap",
                    "dataset_rows": 3,
                    "active_directory_match_rate": 66.67,
                    "active_directory_missing_dataset_rows": 1,
                    "open_review_rows": 1,
                    "closed_no_data_change_rows": 0,
                    "official_non_directory_gap_rows": 1,
                    "absent_from_all_b3_source_gap_rows": 0,
                    "official_subset_candidate_isin_rows": 0,
                    "official_subset_candidate_sector_rows": 1,
                    "rows_requiring_parser_or_scope_review": 1,
                    "rows_requiring_external_active_evidence": 0,
                    "data_change_authorized": False,
                    "root_cause": "official subset parser/scope review",
                    "source_gate": "No B3 ISIN, sector, category, name, symbol, or scope change is authorized.",
                },
                "top_b3_masterfile_gap_review_batches": [
                    {
                        "review_priority": "P2",
                        "b3_resolution_queue": "official_subset_category_requires_review",
                        "asset_type": "ETF",
                        "b3_gap_category": "unit_or_fund_line",
                        "source_presence": "present_only_in_non_exchange_directory_source",
                        "rows": 1,
                        "review_strategy": "review_official_subset_category_and_scope_before_apply_gate",
                        "verification_evidence_required": (
                            "official_b3_source_row_plus_scope_decision_or_parser_fix_before_reclassifying_gap"
                        ),
                        "b3_source_gap_evidence_path": "official_b3_subset_category_apply_evidence",
                        "source_gap_resolution_gate": "apply_only_after_listing_keyed_category_review",
                        "recommended_next_source": (
                            "Official B3 subset source plus category taxonomy evidence with exact listing-key match."
                        ),
                        "source_gate": (
                            "Apply category only after official subset category, listing key, and current dataset category are reviewed."
                        ),
                    }
                ],
                "top_open_b3_masterfile_review_batches": [
                    {
                        "review_priority": "P2",
                        "b3_resolution_queue": "official_subset_category_requires_review",
                        "asset_type": "ETF",
                        "b3_gap_category": "unit_or_fund_line",
                        "source_presence": "present_only_in_non_exchange_directory_source",
                        "rows": 1,
                        "review_strategy": "review_official_subset_category_and_scope_before_apply_gate",
                        "verification_evidence_required": (
                            "official_b3_source_row_plus_scope_decision_or_parser_fix_before_reclassifying_gap"
                        ),
                        "b3_source_gap_evidence_path": "official_b3_subset_category_apply_evidence",
                        "source_gap_resolution_gate": "apply_only_after_listing_keyed_category_review",
                        "recommended_next_source": (
                            "Official B3 subset source plus category taxonomy evidence with exact listing-key match."
                        ),
                        "source_gate": (
                            "Apply category only after official subset category, listing key, and current dataset category are reviewed."
                        ),
                    }
                ],
                "top_open_b3_masterfile_review_rows": [
                    {
                        "listing_key": "B3::AFOF11",
                        "ticker": "AFOF11",
                        "asset_type": "ETF",
                        "name": "Alianza Fofii Fundo De Investimento Imobiliario",
                        "b3_gap_category": "unit_or_fund_line",
                        "b3_resolution_queue": "official_subset_category_requires_review",
                        "review_priority": "P2",
                        "review_strategy": "review_official_subset_category_and_scope_before_apply_gate",
                        "verification_evidence_required": (
                            "official_b3_source_row_plus_scope_decision_or_parser_fix_before_reclassifying_gap"
                        ),
                        "b3_source_gap_evidence_path": "official_b3_subset_category_apply_evidence",
                        "source_gap_resolution_gate": "apply_only_after_listing_keyed_category_review",
                        "recommended_next_source": (
                            "Official B3 subset source plus category taxonomy evidence with exact listing-key match."
                        ),
                        "source_gate": (
                            "Apply category only after official subset category, listing key, and current dataset category are reviewed."
                        ),
                    }
                ],
                "b3_etf_category_apply_rows": 2,
                "b3_etf_category_written_updates": 1,
                "b3_etf_category_apply_decision_totals": {"apply": 1, "skip": 1},
                "b3_etf_category_update_totals": {"Fixed Income": 1},
                "b3_missing_isin_residual_rows": 1,
                "b3_missing_sector_residual_rows": 2,
                "isin_review_strategy_totals": {
                    "decide_b3_core_extended_or_exclude_before_identifier_work": 1,
                },
                "top_b3_isin_review_batches": [
                    {
                        "review_priority": "P1",
                        "review_bucket": "scope_review_before_identifier_fill",
                        "gap_class": "fund_or_trust_identifier_gap",
                        "asset_type": "ETF",
                        "rows": 1,
                        "review_strategy": "decide_b3_core_extended_or_exclude_before_identifier_work",
                        "verification_evidence_required": "scope_decision_for_core_extended_or_exclude_before_identifier_fill",
                        "recommended_next_source": (
                            "Current B3 source plus reviewed core, extended, or exclude scope decision before identifier work."
                        ),
                        "source_gate": (
                            "No ISIN fill until the B3 listing is reviewed as core, extended, or excluded."
                        ),
                    }
                ],
                "b3_isin_official_source_identifier_exposure": {
                    "b3_instruments_equities": {
                        "rows": 1,
                        "isin_present_rows": 1,
                        "isin_missing_rows": 0,
                    },
                    "b3_listed_etfs": {
                        "rows": 1,
                        "isin_present_rows": 0,
                        "isin_missing_rows": 1,
                    },
                },
                "sector_review_strategy_totals": {
                    "keep_blank_until_stronger_official_b3_or_issuer_taxonomy": 2,
                },
                "top_b3_sector_review_batches": [
                    {
                        "review_priority": "P3",
                        "review_bucket": "no_b3_classification_code_match_source_gap",
                        "gap_class": "official_industry_taxonomy_unavailable_gap",
                        "b3_code_shape": "alpha_b3_code",
                        "asset_type": "Stock",
                        "rows": 2,
                        "review_strategy": "keep_blank_until_stronger_official_b3_or_issuer_taxonomy",
                        "verification_evidence_required": "stronger_official_b3_or_issuer_taxonomy_source_with_exact_listing_match",
                        "recommended_next_source": (
                            "Stronger official B3 or issuer taxonomy source exposing sector for the exact listing."
                        ),
                        "source_gate": (
                            "Keep stock_sector blank until official B3 or issuer taxonomy evidence matches the exact listing."
                        ),
                    }
                ],
                "sector_b3_code_shape_totals": {"alpha_b3_code": 1, "alphanumeric_b3_code": 1},
                "sector_alphanumeric_b3_code_rows": 1,
                "sector_alphanumeric_b3_code_examples": [
                    {"listing_key": "B3::A6OP3", "ticker": "A6OP3", "b3_code": "A6OP"}
                ],
                "b3_residual_workstream_rows": {
                    "masterfile_active_directory_gap": 1,
                    "missing_isin_residual": 1,
                    "missing_sector_residual": 2,
                },
                "b3_residual_workstream_priority_totals": {
                    "masterfile_active_directory_gap": {"P2": 1},
                    "missing_isin_residual": {"P1": 1},
                    "missing_sector_residual": {"P3": 2},
                },
                "b3_residual_workstream_readiness_totals": {
                    "masterfile_active_directory_gap": {
                        "review_scope_or_parser_before_any_data_change": 1
                    },
                    "missing_isin_residual": {
                        "blocked_until_core_or_extended_scope_decision": 1
                    },
                    "missing_sector_residual": {
                        "source_gap_keep_blank_until_official_taxonomy_evidence": 2
                    },
                },
            },
            "delta_evidence": {
                "known_deltas": {
                    "current_b3_masterfile_missing_dataset_rows": 1,
                    "current_b3_masterfile_dataset_match_rate": 66.67,
                    "current_b3_official_any_source_missing_dataset_rows": 0,
                    "current_b3_official_any_source_match_rate": 100.0,
                    "current_b3_masterfile_gap_review_rows": 1,
                    "current_b3_masterfile_gap_review_open_rows": 1,
                    "current_b3_etf_category_written_updates": 1,
                    "current_b3_sector_alphanumeric_code_rows": 1,
                    "campaign_start_coverage_snapshot": {
                        "residual_rows": {"baseline": 6, "current": 6, "delta": 0},
                    },
                    "source_gap_delta": {"baseline": 1, "current": 1, "delta": 0},
                    "warn_quarantine_delta": {
                        "warn_delta": 0,
                        "quarantine_delta": 0,
                    },
                },
            },
        },
        *[
            {"campaign_key": f"campaign_{index}", "acceptance_matrix": matrix()}
            for index in range(1, 10)
        ],
    ]

    result = evaluate_campaign_acceptance_matrices(
        {"summary": {"campaigns": 10}, "campaigns": campaigns}
    )

    assert result["passed"] is True
    assert result["b3_campaign_evidence_gaps"] == []


def test_evaluate_campaign_acceptance_matrices_fails_for_stale_b3_masterfile_evidence() -> None:
    base_matrix = {
        "exchange_scope": ["B3"],
        "affected_artifact_rows": 1,
        "campaign_metric_deltas": {
            "residual_rows": {"baseline": 8, "current": 8, "delta": 0},
        },
        "global_acceptance_deltas": {
            key: {"baseline": 1, "current": 1, "delta": 0}
            for key in REQUIRED_DELTA_KEYS
        },
        "exchange_scope_deltas": {
            "exchange_count": 1,
            "changed_exchange_rows": 0,
            "delta_totals": {key: 0 for key in REQUIRED_DELTA_KEYS},
        },
        "required_metrics": REQUIRED_DELTA_KEYS,
    }
    result = evaluate_campaign_acceptance_matrices(
        {
            "summary": {"campaigns": 10},
            "campaigns": [
                {
                    "campaign_key": "b3",
                    "acceptance_matrix": base_matrix,
                    "evidence": {
                        "b3_dataset_rows": 3,
                        "b3_active_exchange_directory_rows": 4,
                        "b3_all_masterfile_rows": 5,
                        "b3_masterfile_matched_dataset_rows": 1,
                        "b3_masterfile_missing_dataset_rows": 1,
                        "b3_masterfile_dataset_match_rate": 90.0,
                        "b3_official_any_source_matched_dataset_rows": 1,
                        "b3_official_any_source_missing_dataset_rows": 1,
                        "b3_official_any_source_match_rate": 90.0,
                        "b3_missing_category_totals": {"unit_or_fund_line": 2},
                        "b3_missing_asset_type_totals": {"ETF": 1},
                        "b3_missing_source_presence_totals": {"present_only_in_non_exchange_directory_source": 1},
                        "b3_missing_examples": {
                            "unit_or_fund_line": [{"listing_key": "NYSE::BAD"}],
                        },
                        "b3_masterfile_gap_review_rows": 0,
                        "b3_masterfile_gap_review_open_rows": 0,
                        "b3_masterfile_gap_review_closed_no_data_change_rows": 1,
                        "b3_masterfile_gap_review_source_presence_totals": {},
                        "b3_masterfile_gap_review_open_source_presence_totals": {},
                        "b3_masterfile_gap_review_bucket_totals": {},
                        "b3_masterfile_gap_review_resolution_queue_totals": {},
                        "b3_masterfile_gap_review_open_resolution_queue_totals": {},
                        "b3_masterfile_gap_review_strategy_totals": {},
                        "b3_masterfile_gap_review_resolution_queue_asset_type_totals": {},
                        "b3_masterfile_gap_review_resolution_queue_gap_category_totals": {},
                        "b3_masterfile_gap_review_candidate_source_totals": {},
                        "b3_masterfile_gap_review_candidate_sector_present_rows": 0,
                        "b3_masterfile_gap_review_candidate_isin_present_rows": 0,
                        "b3_masterfile_gap_review_candidate_category_review_decision_totals": {},
                        "b3_masterfile_gap_review_official_subset_review_decision_totals": {},
                        "b3_masterfile_gap_review_official_subset_closure_eligibility_totals": {},
                        "b3_masterfile_gap_review_official_subset_closure_ready_rows": 1,
                        "b3_masterfile_gap_review_candidate_category_mismatch_rows": 0,
                        "b3_masterfile_gap_review_candidate_category_mismatch_examples": [],
                        "b3_masterfile_gap_review_coverage_diagnosis": {
                            "status": "stale",
                            "dataset_rows": 3,
                            "active_directory_match_rate": 90.0,
                            "active_directory_missing_dataset_rows": 1,
                            "open_review_rows": 0,
                            "closed_no_data_change_rows": 1,
                            "official_non_directory_gap_rows": 1,
                            "absent_from_all_b3_source_gap_rows": 0,
                            "official_subset_candidate_isin_rows": 0,
                            "official_subset_candidate_sector_rows": 0,
                            "rows_requiring_parser_or_scope_review": 1,
                            "rows_requiring_external_active_evidence": 0,
                            "data_change_authorized": True,
                            "root_cause": "stale",
                            "source_gate": "stale",
                        },
                        "top_b3_masterfile_gap_review_batches": [],
                        "top_open_b3_masterfile_review_batches": [],
                        "b3_etf_category_apply_rows": 0,
                        "b3_etf_category_written_updates": 0,
                        "b3_etf_category_apply_decision_totals": {},
                        "b3_etf_category_update_totals": {},
                        "b3_missing_isin_residual_rows": 0,
                        "b3_missing_sector_residual_rows": 0,
                        "b3_isin_official_source_identifier_exposure": {
                            "b3_instruments_equities": {
                                "rows": 0,
                                "isin_present_rows": 0,
                                "isin_missing_rows": 0,
                            },
                        },
                        "sector_b3_code_shape_totals": {"alpha_b3_code": 1, "alphanumeric_b3_code": 1},
                        "sector_alphanumeric_b3_code_rows": 0,
                        "sector_alphanumeric_b3_code_examples": [
                            {"listing_key": "B3::A6OP3", "ticker": "A6OP3", "b3_code": "A6OP"}
                        ],
                        "b3_residual_workstream_rows": {
                            "masterfile_active_directory_gap": 0,
                            "missing_isin_residual": 0,
                            "missing_sector_residual": 0,
                        },
                        "b3_residual_workstream_priority_totals": {
                            "masterfile_active_directory_gap": {},
                            "missing_isin_residual": {},
                            "missing_sector_residual": {},
                        },
                        "b3_residual_workstream_readiness_totals": {
                            "masterfile_active_directory_gap": {},
                            "missing_isin_residual": {},
                            "missing_sector_residual": {},
                        },
                    },
                    "delta_evidence": {"known_deltas": {}},
                },
                *[
                    {"campaign_key": f"campaign_{index}", "acceptance_matrix": base_matrix}
                    for index in range(1, 10)
                ],
            ],
        }
    )

    assert result["passed"] is False
    assert {"field": "b3_dataset_row_balance", "matched": 1, "missing": 1, "dataset": 3} in result[
        "b3_campaign_evidence_gaps"
    ]
    assert {
        "field": "b3_masterfile_dataset_match_rate",
        "reported": 90.0,
        "expected": 33.33,
    } in result["b3_campaign_evidence_gaps"]
    assert {
        "field": "top_b3_masterfile_gap_review_batches",
        "reason": "expected_ranked_review_batches",
    } in result["b3_campaign_evidence_gaps"]
    assert {
        "field": "b3_masterfile_gap_review_official_subset_closure_ready_rows",
        "reported": 1,
        "expected": 0,
    } in result["b3_campaign_evidence_gaps"]


def test_evaluate_campaign_acceptance_matrices_requires_otc_scope_evidence() -> None:
    def matrix() -> dict[str, object]:
        return {
            "exchange_scope": ["OTC"],
            "affected_artifact_rows": 4,
            "campaign_metric_deltas": {
                "residual_rows": {"baseline": 6, "current": 6, "delta": 0},
            },
            "global_acceptance_deltas": {
                key: {"baseline": 1, "current": 1, "delta": 0}
                for key in REQUIRED_DELTA_KEYS
            },
            "exchange_scope_deltas": {
                "exchange_count": 1,
                "changed_exchange_rows": 0,
                "delta_totals": {key: 0 for key in REQUIRED_DELTA_KEYS},
            },
            "required_metrics": REQUIRED_DELTA_KEYS,
        }

    result = evaluate_campaign_acceptance_matrices(
        {
            "summary": {"campaigns": 10},
            "campaigns": [
                {
                    "campaign_key": "otc",
                    "acceptance_matrix": matrix(),
                    "evidence": {
                        "otc_review_rows": 4,
                        "source_gap_rows": 2,
                        "drop_override_rows_still_present": 0,
                        "review_bucket_totals": {
                            "documented_otc_sector_source_gap": 1,
                            "documented_otc_category_source_gap": 1,
                            "official_name_mismatch_review_first": 2,
                        },
                        "review_bucket_asset_type_totals": {
                            "documented_otc_category_source_gap": {"ETF": 1},
                            "documented_otc_sector_source_gap": {"Stock": 1},
                        "official_name_mismatch_review_first": {"Stock": 2},
                    },
                        "review_bucket_metadata_gate_totals": {
                            "documented_otc_category_source_gap": {
                                "reviewed_product_category_source_required_keep_blank": 1
                            },
                            "documented_otc_sector_source_gap": {
                                "reviewed_issuer_sector_source_required_keep_blank": 1
                            },
                        "official_name_mismatch_review_first": {
                            "otc_name_mismatch_review_required_before_name_or_metadata_changes": 2
                        },
                    },
                    "review_priority_totals": {"P2": 2, "P3": 2},
                    "scope_review_strategy_totals": {
                        "keep_category_blank_until_reviewed_product_taxonomy_source": 1,
                        "keep_sector_blank_until_reviewed_issuer_sector_source": 1,
                        "resolve_listing_keyed_name_mismatch_before_metadata_work": 2,
                    },
                    "scope_verification_evidence_required_totals": {
                        "reviewed_issuer_sector_source_with_exact_listing_or_keep_blank_decision": 1,
                        "reviewed_otc_name_mismatch_decision_before_name_or_metadata_change": 2,
                        "reviewed_product_taxonomy_source_with_exact_listing_or_keep_blank_decision": 1,
                    },
                    "top_otc_scope_review_batches": [
                        {
                            "review_priority": "P2",
                            "review_bucket": "official_name_mismatch_review_first",
                            "asset_type": "Stock",
                            "quality_status": "warn",
                            "metadata_enrichment_gate": "otc_name_mismatch_review_required_before_name_or_metadata_changes",
                            "rows": 2,
                            "review_strategy": "resolve_listing_keyed_name_mismatch_before_metadata_work",
                            "verification_evidence_required": (
                                "reviewed_otc_name_mismatch_decision_before_name_or_metadata_change"
                            ),
                            "recommended_next_source": (
                                "OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history."
                            ),
                            "source_gate": (
                                "No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision."
                            ),
                        }
                    ],
                    "otc_core_exclusion_candidate_rows": 0,
                    "otc_core_exclusion_candidate_asset_type_totals": {},
                    "otc_core_exclusion_candidate_metadata_gate_totals": {},
                    "otc_core_exclusion_candidate_review_examples": [],
                    "scope_apply_eligibility_totals": {"already_extended_no_scope_change_required": 4},
                    "otc_scope_completion": {
                        "status": "complete_extended_scope_no_core_candidates",
                        "rows": 4,
                        "extended_otc_rows": 4,
                        "otc_listing_scope_reason_rows": 4,
                        "already_extended_scope_decision_rows": 4,
                        "core_exclusion_candidate_rows": 0,
                        "unexpected_core_scope_rows": 0,
                        "blocked_scope_decision_rows": 0,
                        "scope_apply_allowed_rows": 0,
                        "metadata_enrichment_authorized": False,
                        "source_gate": (
                            "OTC scope is complete only when every current OTC row is extended/otc_listing and "
                            "no core or core-exclusion scope decision remains open; metadata still requires "
                            "listing-keyed evidence."
                        ),
                    },
                    "post_scope_metadata_backlog": {
                        "status": "metadata_review_backlog_open",
                        "rows": 4,
                        "metadata_enrichment_authorized": False,
                        "scope_blocked_rows": 0,
                        "source_gate": (
                            "Post-scope OTC metadata work remains blocked unless each row has listing-keyed OTC Markets, "
                            "issuer, SEC, registry, or reviewed fallback evidence; no ticker-only enrichment is allowed."
                        ),
                    },
                    "post_scope_metadata_backlog_bucket_totals": {
                        "documented_otc_category_source_gap": 1,
                        "documented_otc_sector_source_gap": 1,
                        "official_name_mismatch_review_first": 1,
                        "otc_quality_source_gap_review": 1,
                    },
                    "post_scope_metadata_backlog_gate_totals": {
                        "otc_name_mismatch_review_required_before_name_or_metadata_changes": 1,
                        "reviewed_issuer_sector_source_required_keep_blank": 1,
                        "reviewed_product_category_source_required_keep_blank": 1,
                        "source_gap_review_required_before_enrichment": 1,
                    },
                    "post_scope_metadata_backlog_examples": [
                        {
                            "listing_key": "OTC::NAME",
                            "ticker": "NAME",
                            "asset_type": "Stock",
                            "name": "Name Mismatch Corp",
                            "review_bucket": "official_name_mismatch_review_first",
                            "quality_status": "warn",
                            "metadata_enrichment_gate": "otc_name_mismatch_review_required_before_name_or_metadata_changes",
                            "review_strategy": "resolve_listing_keyed_name_mismatch_before_metadata_work",
                            "verification_evidence_required": (
                                "reviewed_otc_name_mismatch_decision_before_name_or_metadata_change"
                            ),
                            "recommended_next_source": (
                                "OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history."
                            ),
                            "source_gate": (
                                "No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision."
                            ),
                        }
                    ],
                    "metadata_enrichment_gate_totals": {
                        "otc_name_mismatch_review_required_before_name_or_metadata_changes": 1,
                        "reviewed_issuer_sector_source_required_keep_blank": 1,
                        "reviewed_product_category_source_required_keep_blank": 1,
                        "source_gap_review_required_before_enrichment": 1,
                    },
                    "source_gap_field_totals": {"missing_sector_stock": 1, "missing_etf_category": 1},
                    "source_gap_class_totals": {"otc_sector_source_gap": 1, "otc_category_source_gap": 1},
                    "source_of_truth_outcome_totals": {"accepted_source_gap": 2},
                    "name_mismatch_review_rows": 2,
                    "otc_review_decision_active_name_mismatch_rows": 1,
                    "otc_name_mismatch_unreviewed_active_rows": 1,
                    "otc_review_decision_resolution_totals": {
                        "pending_active_name_mismatch_review": 1,
                        "reviewed_decision_covers_active_name_mismatch": 1,
                    },
                    "otc_review_decision_current_listing_suppressed_rows": 0,
                    "otc_review_decision_not_current_scope_rows": 0,
                    "otc_review_decision_stale_rows": 0,
                    "name_mismatch_class_counts": {"probable_otc_rename_or_symbol_reuse": 1, "hold_unresolved": 1},
                    "name_mismatch_priority_counts": {"high": 1, "held": 1},
                    "name_mismatch_review_strategy_counts": {
                        "keep_current_until_stronger_issuer_history_source": 1,
                        "verify_isin_anchored_issuer_history_before_name_change": 1,
                    },
                    "top_otc_name_mismatch_review_batches": [
                        {
                            "review_priority": "high",
                            "review_class": "probable_otc_rename_or_symbol_reuse",
                            "isin_presence": "with_isin",
                            "official_sources": "otc_markets_stock_screener",
                            "rows": 1,
                            "review_strategy": "verify_isin_anchored_issuer_history_before_name_change",
                            "verification_evidence_required": "official_or_reviewed_isin_bearing_source_matching_current_issuer_listing_key_and_name",
                            "recommended_next_source": (
                                "Official or reviewed ISIN-bearing issuer-history source matching current issuer, listing key, and name."
                            ),
                            "source_gate": (
                                "Do not change the name until ISIN-anchored evidence proves the same current issuer."
                            ),
                        },
                        {
                            "review_priority": "held",
                            "review_class": "hold_unresolved",
                            "isin_presence": "with_isin",
                            "official_sources": "otc_markets_stock_screener",
                            "rows": 1,
                            "review_strategy": "keep_current_until_stronger_issuer_history_source",
                            "verification_evidence_required": "stronger_official_or_reviewed_issuer_history_source_before_any_name_change",
                            "recommended_next_source": (
                                "Stronger official or reviewed issuer-history source matching the OTC listing key."
                            ),
                            "source_gate": (
                                "Keep current name until stronger issuer-history evidence resolves the ambiguity."
                            ),
                        },
                    ],
                    "name_mismatch_apply_eligibility_counts": {
                            "blocked_until_isin_anchored_issuer_history_review": 1,
                            "keep_current_until_stronger_issuer_history_source": 1,
                        },
                        "name_mismatch_verification_evidence_required_counts": {
                            "official_or_reviewed_isin_bearing_source_matching_current_issuer_listing_key_and_name": 1,
                            "stronger_official_or_reviewed_issuer_history_source_before_any_name_change": 1,
                        },
                    },
                    "delta_evidence": {
                        "known_deltas": {
                            "drop_override_rows_still_present": 0,
                            "accepted_source_gap_rows": 2,
                            "otc_core_exclusion_candidate_rows": 0,
                            "active_name_mismatch_review_rows": 2,
                            "otc_name_mismatch_unreviewed_active_rows": 1,
                            "otc_review_decision_current_listing_suppressed_rows": 0,
                            "otc_review_decision_not_current_scope_rows": 0,
                            "otc_review_decision_stale_rows": 0,
                            "campaign_start_scope_snapshot": {
                                "residual_rows": {"baseline": 6, "current": 6, "delta": 0},
                            },
                            "warn_quarantine_delta": {
                                "warn_delta": 0,
                                "quarantine_delta": 0,
                            },
                        }
                    },
                },
                *[
                    {"campaign_key": f"campaign_{index}", "acceptance_matrix": matrix()}
                    for index in range(1, 10)
                ],
            ],
        }
    )

    assert result["passed"] is True
    assert result["otc_campaign_evidence_gaps"] == []


def test_evaluate_campaign_acceptance_matrices_fails_for_stale_otc_scope_evidence() -> None:
    base_matrix = {
        "exchange_scope": ["OTC"],
        "affected_artifact_rows": 4,
        "campaign_metric_deltas": {},
        "global_acceptance_deltas": {
            key: {"baseline": 1, "current": 1, "delta": 0}
            for key in REQUIRED_DELTA_KEYS
        },
        "exchange_scope_deltas": {
            "exchange_count": 1,
            "changed_exchange_rows": 0,
            "delta_totals": {key: 0 for key in REQUIRED_DELTA_KEYS},
        },
        "required_metrics": REQUIRED_DELTA_KEYS,
    }

    result = evaluate_campaign_acceptance_matrices(
        {
            "summary": {"campaigns": 10},
            "campaigns": [
                {
                    "campaign_key": "otc",
                    "acceptance_matrix": base_matrix,
                    "evidence": {
                        "otc_review_rows": 4,
                        "source_gap_rows": 2,
                        "drop_override_rows_still_present": 1,
                        "review_bucket_totals": {"documented_otc_sector_source_gap": 3},
                        "review_bucket_asset_type_totals": {"documented_otc_sector_source_gap": {"Stock": 3}},
                        "review_bucket_metadata_gate_totals": {
                            "documented_otc_sector_source_gap": {"reviewed_issuer_sector_source_required_keep_blank": 3}
                        },
                        "review_priority_totals": {"P3": 4},
                        "scope_review_strategy_totals": {"keep_sector_blank_until_reviewed_issuer_sector_source": 3},
                        "scope_verification_evidence_required_totals": {
                            "reviewed_issuer_sector_source_with_exact_listing_or_keep_blank_decision": 3
                        },
                        "top_otc_scope_review_batches": [
                            {
                                "review_priority": "P3",
                                "review_bucket": "documented_otc_sector_source_gap",
                                "asset_type": "Stock",
                                "quality_status": "source_gap",
                                "metadata_enrichment_gate": "reviewed_issuer_sector_source_required_keep_blank",
                                "rows": 3,
                                "review_strategy": "keep_sector_blank_until_reviewed_issuer_sector_source",
                                "verification_evidence_required": (
                                    "reviewed_issuer_sector_source_with_exact_listing_or_keep_blank_decision"
                                ),
                                "recommended_next_source": (
                                    "SEC SIC, issuer filings, OTC Markets profile, or reviewed secondary company profile."
                                ),
                                "source_gate": (
                                    "Canonical stock sector only after exchange/name gate; no ticker/name-only inference."
                                ),
                            }
                        ],
                        "otc_core_exclusion_candidate_rows": 0,
                        "otc_core_exclusion_candidate_asset_type_totals": {},
                        "otc_core_exclusion_candidate_metadata_gate_totals": {},
                        "otc_core_exclusion_candidate_review_examples": [],
                        "scope_apply_eligibility_totals": {"already_extended_no_scope_change_required": 4},
                        "otc_scope_completion": {
                            "status": "scope_review_open",
                            "rows": 3,
                            "extended_otc_rows": 3,
                            "otc_listing_scope_reason_rows": 3,
                            "already_extended_scope_decision_rows": 3,
                            "core_exclusion_candidate_rows": 0,
                            "unexpected_core_scope_rows": 0,
                            "blocked_scope_decision_rows": 0,
                            "scope_apply_allowed_rows": 1,
                            "metadata_enrichment_authorized": True,
                            "source_gate": "metadata still requires listing-keyed evidence",
                        },
                        "post_scope_metadata_backlog": {
                            "status": "metadata_review_backlog_open",
                            "rows": 4,
                            "metadata_enrichment_authorized": True,
                            "scope_blocked_rows": 0,
                            "source_gate": "",
                        },
                        "post_scope_metadata_backlog_bucket_totals": {
                            "documented_otc_sector_source_gap": 3,
                        },
                        "post_scope_metadata_backlog_gate_totals": {
                            "reviewed_issuer_sector_source_required_keep_blank": 4,
                        },
                        "post_scope_metadata_backlog_examples": [],
                        "metadata_enrichment_gate_totals": {
                            "reviewed_issuer_sector_source_required_keep_blank": 4,
                            "fill_from_symbol_shape": 1,
                        },
                        "source_gap_field_totals": {"missing_sector_stock": 1},
                        "source_gap_class_totals": {"otc_sector_source_gap": 2},
                        "source_of_truth_outcome_totals": {"accepted_source_gap": 1},
                        "name_mismatch_review_rows": 2,
                        "otc_review_decision_active_name_mismatch_rows": 0,
                        "otc_name_mismatch_unreviewed_active_rows": 1,
                        "otc_review_decision_resolution_totals": {
                            "pending_active_name_mismatch_review": 1,
                            "reviewed_decision_suppresses_current_listing_warning": 1,
                            "reviewed_decision_not_in_current_otc_scope": 1,
                        },
                        "otc_review_decision_current_listing_suppressed_rows": 1,
                        "otc_review_decision_not_current_scope_rows": 1,
                        "otc_review_decision_stale_rows": 2,
                        "name_mismatch_class_counts": {"probable_otc_rename_or_symbol_reuse": 1},
                        "name_mismatch_priority_counts": {"high": 2},
                        "name_mismatch_review_strategy_counts": {
                            "verify_isin_anchored_issuer_history_before_name_change": 2
                        },
                        "top_otc_name_mismatch_review_batches": [
                            {
                                "review_priority": "high",
                                "review_class": "probable_otc_rename_or_symbol_reuse",
                                "isin_presence": "with_isin",
                                "official_sources": "otc_markets_stock_screener",
                                "rows": 2,
                                "review_strategy": "verify_isin_anchored_issuer_history_before_name_change",
                                "verification_evidence_required": "official_or_reviewed_isin_bearing_source_matching_current_issuer_listing_key_and_name",
                                "recommended_next_source": (
                                    "Official or reviewed ISIN-bearing issuer-history source matching current issuer, listing key, and name."
                                ),
                                "source_gate": (
                                    "Do not change the name until ISIN-anchored evidence proves the same current issuer."
                                ),
                            }
                        ],
                        "name_mismatch_apply_eligibility_counts": {"blocked_until_isin_anchored_issuer_history_review": 2},
                        "name_mismatch_verification_evidence_required_counts": {
                            "official_or_reviewed_isin_bearing_source_matching_current_issuer_listing_key_and_name": 2,
                        },
                    },
                    "delta_evidence": {
                        "known_deltas": {
                            "drop_override_rows_still_present": 1,
                            "accepted_source_gap_rows": 1,
                            "otc_core_exclusion_candidate_rows": 0,
                            "active_name_mismatch_review_rows": 3,
                            "otc_name_mismatch_unreviewed_active_rows": 0,
                            "otc_review_decision_current_listing_suppressed_rows": 0,
                            "otc_review_decision_not_current_scope_rows": 1,
                            "otc_review_decision_stale_rows": 1,
                        }
                    },
                },
                *[
                    {"campaign_key": f"campaign_{index}", "acceptance_matrix": base_matrix}
                    for index in range(1, 10)
                ],
            ],
        }
    )

    assert result["passed"] is False
    gaps = result["otc_campaign_evidence_gaps"]
    assert {
        "field": "drop_override_rows_still_present",
        "reported": 1,
        "expected": 0,
    } in gaps
    assert {
        "field": "source_gap_field_totals",
        "reported_total": 1,
        "source_gap_rows": 2,
    } in gaps
    assert {
        "field": "source_of_truth_outcome_totals.accepted_source_gap",
        "reported": 1,
        "source_gap_rows": 2,
    } in gaps
    assert {
        "field": "metadata_enrichment_gate_totals",
        "forbidden_keys": ["fill_from_symbol_shape"],
    } in gaps
    assert {
        "field": "otc_scope_completion.metadata_enrichment_authorized",
        "reported": True,
        "expected": False,
    } in gaps
    assert {
        "field": "otc_scope_completion.scope_apply_allowed_rows",
        "reported": 1,
        "expected": 0,
    } in gaps
    assert {
        "field": "otc_name_mismatch_review_balance",
        "reviewed_active": 0,
        "unreviewed_active": 1,
        "name_mismatch_review_rows": 2,
    } in gaps
    assert {
        "field": "otc_scope_completion.status",
        "reported": "scope_review_open",
        "expected": "complete_extended_scope_no_core_candidates",
    } in gaps


def test_evaluate_campaign_acceptance_matrices_requires_canada_figi_campaign_evidence() -> None:
    def matrix() -> dict[str, object]:
        return {
            "exchange_scope": ["TSX", "TSXV", "NEO"],
            "affected_artifact_rows": 5,
            "campaign_metric_deltas": {
                "residual_rows": {"baseline": 6, "current": 6, "delta": 0},
            },
            "global_acceptance_deltas": {
                key: {"baseline": 1, "current": 1, "delta": 0}
                for key in REQUIRED_DELTA_KEYS
            },
            "exchange_scope_deltas": {
                "exchange_count": 3,
                "changed_exchange_rows": 0,
                "delta_totals": {key: 0 for key in REQUIRED_DELTA_KEYS},
            },
            "required_metrics": REQUIRED_DELTA_KEYS,
        }

    result = evaluate_campaign_acceptance_matrices(
        {
            "summary": {"campaigns": 10},
            "campaigns": [
                {
                    "campaign_key": "canada",
                    "acceptance_matrix": matrix(),
                    "evidence": {
                        "canada_residual_rows": 5,
                        "active_figi_queue_rows": 0,
                        "missing_isin_rows": 2,
                        "missing_figi_rows": 3,
                        "canada_identifier_backlog": {
                            "status": "figi_queue_drained_remaining_isin_scope_or_reviewed_source_gaps",
                            "rows": 5,
                            "scope_decision_required_rows": 1,
                            "official_isin_source_required_rows": 2,
                            "figi_blocked_until_isin_rows": 2,
                            "reviewed_openfigi_source_gap_rows": 2,
                            "openfigi_candidate_rows": 0,
                            "direct_identifier_apply_allowed_rows": 0,
                            "metadata_enrichment_authorized": False,
                            "source_gate": (
                                "Canadian identifier work remains blocked unless a listing-keyed official CSD, issuer, prospectus, "
                                "transfer-agent, OpenFIGI-by-ISIN, or reviewed stronger source proves the value; no symbol/name inference."
                            ),
                        },
                        "canada_identifier_backlog_queue_totals": {
                            "core_exclusion_candidate_identifier_scope_review": 1,
                            "missing_isin_official_canada_masterfiles_do_not_expose_isin": 2,
                            "reviewed_openfigi_no_match_source_gap": 2,
                        },
                        "canada_identifier_backlog_evidence_required_totals": {
                            "official_csd_issuer_or_reviewed_identifier_source_with_exact_listing_match_and_valid_isin": 2,
                            "scope_decision_for_core_extended_or_exclude_before_identifier_enrichment": 1,
                            "stronger_figi_source_required_openfigi_no_match_reviewed": 2,
                        },
                        "canada_core_exclusion_candidate_rows": 1,
                        "canada_core_exclusion_candidate_exchange_totals": {"TSXV": 1},
                        "canada_core_exclusion_candidate_asset_type_totals": {"Stock": 1},
                        "canada_core_exclusion_candidate_resolution_queue_totals": {
                            "core_exclusion_candidate_identifier_scope_review": 1
                        },
                        "canada_core_exclusion_candidate_official_source_totals": {
                            "tmx_listed_issuers": 1
                        },
                        "canada_core_exclusion_candidate_source_gap_class_totals": {
                            "capital_pool_or_halted_identifier_gap": 1
                        },
                        "exchange_totals": {"TSX": 2, "TSXV": 2, "NEO": 1},
                        "official_masterfile_source_totals": {"tmx_listed_issuers": 4, "cboe_canada_listing_directory": 1},
                        "canada_resolution_queue_totals": {
                            "core_exclusion_candidate_identifier_scope_review": 1,
                            "missing_isin_official_canada_masterfiles_do_not_expose_isin": 2,
                            "reviewed_openfigi_no_match_source_gap": 2,
                        },
                        "canada_resolution_queue_exchange_totals": {
                            "core_exclusion_candidate_identifier_scope_review": {"TSXV": 1},
                            "missing_isin_official_canada_masterfiles_do_not_expose_isin": {"TSX": 2},
                            "reviewed_openfigi_no_match_source_gap": {"NEO": 1, "TSXV": 1},
                        },
                        "canada_resolution_queue_asset_type_totals": {
                            "core_exclusion_candidate_identifier_scope_review": {"Stock": 1},
                            "missing_isin_official_canada_masterfiles_do_not_expose_isin": {"Stock": 2},
                            "reviewed_openfigi_no_match_source_gap": {"ETF": 1, "Stock": 1},
                        },
                        "canada_resolution_queue_source_gap_class_totals": {
                            "core_exclusion_candidate_identifier_scope_review": {
                                "capital_pool_or_halted_identifier_gap": 1
                            },
                            "missing_isin_official_canada_masterfiles_do_not_expose_isin": {
                                "official_identifier_not_exposed_source_gap": 2
                            },
                            "reviewed_openfigi_no_match_source_gap": {"none": 2},
                        },
                        "canada_resolution_queue_official_source_totals": {
                            "core_exclusion_candidate_identifier_scope_review": {"tmx_listed_issuers": 1},
                            "missing_isin_official_canada_masterfiles_do_not_expose_isin": {
                                "tmx_listed_issuers": 2
                            },
                            "reviewed_openfigi_no_match_source_gap": {"tmx_listed_issuers": 2},
                        },
                        "canada_resolution_queue_review_strategy_totals": {
                            "core_exclusion_candidate_identifier_scope_review": {
                                "scope_review_before_canada_identifier_enrichment": 1
                            },
                            "missing_isin_official_canada_masterfiles_do_not_expose_isin": {
                                "seek_official_canada_isin_source": 2
                            },
                            "reviewed_openfigi_no_match_source_gap": {
                                "keep_figi_blank_after_reviewed_openfigi_no_match": 2
                            },
                        },
                        "canada_resolution_queue_evidence_required_totals": {
                            "core_exclusion_candidate_identifier_scope_review": {
                                "official_listing_scope_decision_for_core_extended_or_exclude": 1
                            },
                            "missing_isin_official_canada_masterfiles_do_not_expose_isin": {
                                "official_csd_issuer_or_reviewed_identifier_source_with_exact_listing_match_and_valid_isin": 2
                            },
                            "reviewed_openfigi_no_match_source_gap": {
                                "stronger_figi_source_required_openfigi_no_match_reviewed": 2
                            },
                        },
                        "top_canada_resolution_review_batches": [
                            {
                                "canada_resolution_queue": "missing_isin_official_canada_masterfiles_do_not_expose_isin",
                                "exchange": "TSX",
                                "official_source": "tmx_listed_issuers",
                                "rows": 2,
                                "review_strategy": "seek_official_canada_isin_source",
                                "evidence_required": "official_csd_issuer_or_reviewed_identifier_source_with_exact_listing_match_and_valid_isin",
                                "recommended_next_source": (
                                    "Official CSD, issuer, prospectus, transfer-agent, or reviewed identifier source exposing a valid ISIN."
                                ),
                                "source_gate": (
                                    "Keep ISIN blank until a direct official or reviewed identifier source exposes a valid checksum ISIN."
                                ),
                            }
                        ],
                        "source_gap_field_totals": {"missing_isin_primary": 2},
                        "source_gap_class_totals": {"official_identifier_not_exposed_source_gap": 2},
                        "source_of_truth_outcome_totals": {"accepted_source_gap": 2},
                        "reviewed_openfigi_source_gap_rows": 2,
                        "openfigi_review_status_totals": {"accepted_source_gap_no_openfigi_match": 2},
                        "openfigi_review_decision_totals": {"no_openfigi_match": 2},
                        "isin_apply_eligibility_totals": {
                            "keep_blank_until_official_isin_source": 2,
                            "no_isin_action_required": 3,
                        },
                        "figi_apply_eligibility_totals": {
                            "blocked_until_isin_resolved": 2,
                            "keep_blank_as_reviewed_openfigi_source_gap": 2,
                            "no_figi_action_required": 1,
                        },
                        "verification_evidence_required_totals": {
                            "official_csd_issuer_or_reviewed_identifier_source_with_exact_listing_match_and_valid_isin": 2,
                            "stronger_figi_source_required_openfigi_no_match_reviewed": 2,
                            "none_no_identifier_change_authorized": 1,
                        },
                        "figi_queue_apply_eligibility_totals": {
                            "no_active_openfigi_probe_rows": 1,
                            "keep_figi_blank_as_reviewed_openfigi_source_gap_until_stronger_source": 2,
                        },
                        "figi_queue_verification_evidence_required_totals": {
                            "none_queue_drained_or_candidates_excluded_as_reviewed_source_gaps": 1,
                            "stronger_figi_source_required_for_reviewed_openfigi_no_match_or_cross_isin_collision": 2,
                        },
                        "figi_queue_review_strategy_totals": {
                            "no_active_openfigi_probe_rows_after_gates": 1,
                            "keep_reviewed_openfigi_source_gaps_excluded_until_stronger_source": 2,
                        },
                        "top_canada_figi_queue_review_batches": [
                            {
                                "exchange": "reviewed_openfigi_source_gap",
                                "asset_type": "all",
                                "openfigi_exchange_hint": "CN",
                                "rows": 2,
                                "review_strategy": "keep_reviewed_openfigi_source_gaps_excluded_until_stronger_source",
                                "apply_eligibility": "keep_figi_blank_as_reviewed_openfigi_source_gap_until_stronger_source",
                                "verification_evidence_required": "stronger_figi_source_required_for_reviewed_openfigi_no_match_or_cross_isin_collision",
                                "recommended_next_source": (
                                    "Stronger FIGI source or reviewed OpenFIGI re-check evidence for the existing reviewed source gap."
                                ),
                                "source_gate": (
                                    "Do not re-probe or fill FIGI from symbol/name; keep blank until stronger reviewed FIGI evidence exists."
                                ),
                            }
                        ],
                        "applied_figi_rows": 4,
                        "openfigi_gap_rows_added": 1,
                    },
                    "delta_evidence": {
                        "known_deltas": {
                            "applied_figi_rows": 4,
                            "openfigi_gap_rows_added": 1,
                            "active_figi_queue_rows": 0,
                            "canada_core_exclusion_candidate_rows": 1,
                            "reviewed_openfigi_source_gap_rows": 2,
                            "isin_delta": {"baseline": 1, "current": 1, "delta": 0},
                            "sector_category_delta": {
                                "sector_delta": {"baseline": 1, "current": 1, "delta": 0},
                                "category_delta": {"baseline": 1, "current": 1, "delta": 0},
                            },
                            "warn_quarantine_delta": {
                                "warn_delta": 0,
                                "quarantine_delta": 0,
                            },
                        }
                    },
                },
                *[
                    {"campaign_key": f"campaign_{index}", "acceptance_matrix": matrix()}
                    for index in range(1, 10)
                ],
            ],
        }
    )

    assert result["passed"] is True
    assert result["canada_campaign_evidence_gaps"] == []


def test_evaluate_campaign_acceptance_matrices_fails_for_stale_canada_figi_campaign_evidence() -> None:
    base_matrix = {
        "exchange_scope": ["TSX", "TSXV", "NEO"],
        "affected_artifact_rows": 5,
        "campaign_metric_deltas": {},
        "global_acceptance_deltas": {
            key: {"baseline": 1, "current": 1, "delta": 0}
            for key in REQUIRED_DELTA_KEYS
        },
        "exchange_scope_deltas": {
            "exchange_count": 3,
            "changed_exchange_rows": 0,
            "delta_totals": {key: 0 for key in REQUIRED_DELTA_KEYS},
        },
        "required_metrics": REQUIRED_DELTA_KEYS,
    }

    result = evaluate_campaign_acceptance_matrices(
        {
            "summary": {"campaigns": 10},
            "campaigns": [
                {
                    "campaign_key": "canada",
                    "acceptance_matrix": base_matrix,
                    "evidence": {
                        "canada_residual_rows": 5,
                        "active_figi_queue_rows": 0,
                        "missing_isin_rows": 2,
                        "missing_figi_rows": 3,
                        "canada_identifier_backlog": {
                            "status": "apply_without_sources",
                            "rows": 3,
                            "scope_decision_required_rows": 3,
                            "official_isin_source_required_rows": 4,
                            "figi_blocked_until_isin_rows": 5,
                            "reviewed_openfigi_source_gap_rows": 2,
                            "openfigi_candidate_rows": 0,
                            "direct_identifier_apply_allowed_rows": 1,
                            "metadata_enrichment_authorized": True,
                            "source_gate": "",
                        },
                        "canada_identifier_backlog_queue_totals": {
                            "missing_isin_official_canada_masterfiles_do_not_expose_isin": 4,
                        },
                        "canada_identifier_backlog_evidence_required_totals": {
                            "ticker_match_only": 4,
                        },
                        "canada_core_exclusion_candidate_rows": 2,
                        "canada_core_exclusion_candidate_exchange_totals": {"TSXV": 1, "NYSE": 1},
                        "canada_core_exclusion_candidate_asset_type_totals": {"Stock": 1},
                        "canada_core_exclusion_candidate_resolution_queue_totals": {
                            "core_exclusion_candidate_identifier_scope_review": 2
                        },
                        "canada_core_exclusion_candidate_official_source_totals": {
                            "tmx_listed_issuers": 1
                        },
                        "canada_core_exclusion_candidate_source_gap_class_totals": {
                            "capital_pool_or_halted_identifier_gap": 1
                        },
                        "exchange_totals": {"TSX": 4, "NYSE": 1},
                        "official_masterfile_source_totals": {"tmx_listed_issuers": 5},
                        "canada_resolution_queue_totals": {
                            "missing_isin_official_canada_masterfiles_do_not_expose_isin": 4,
                        },
                        "canada_resolution_queue_exchange_totals": {
                            "missing_isin_official_canada_masterfiles_do_not_expose_isin": {"TSX": 3},
                        },
                        "canada_resolution_queue_asset_type_totals": {
                            "missing_isin_official_canada_masterfiles_do_not_expose_isin": {"Stock": 3},
                        },
                        "canada_resolution_queue_source_gap_class_totals": {
                            "missing_isin_official_canada_masterfiles_do_not_expose_isin": {
                                "official_identifier_not_exposed_source_gap": 4
                            }
                        },
                        "canada_resolution_queue_official_source_totals": {
                            "missing_isin_official_canada_masterfiles_do_not_expose_isin": {
                                "tmx_listed_issuers": 4
                            }
                        },
                        "canada_resolution_queue_review_strategy_totals": {
                            "missing_isin_official_canada_masterfiles_do_not_expose_isin": {
                                "apply_from_symbol": 4
                            }
                        },
                        "canada_resolution_queue_evidence_required_totals": {
                            "missing_isin_official_canada_masterfiles_do_not_expose_isin": {
                                "ticker_match_only": 4
                            }
                        },
                        "top_canada_resolution_review_batches": [
                            {
                                "canada_resolution_queue": "missing_isin_official_canada_masterfiles_do_not_expose_isin",
                                "exchange": "TSX",
                                "official_source": "tmx_listed_issuers",
                                "rows": 4,
                                "review_strategy": "apply_from_symbol",
                                "evidence_required": "ticker_match_only",
                                "recommended_next_source": "",
                                "source_gate": "",
                            }
                        ],
                        "source_gap_field_totals": {"missing_isin_primary": 1},
                        "source_gap_class_totals": {"official_identifier_not_exposed_source_gap": 2},
                        "source_of_truth_outcome_totals": {"accepted_source_gap": 2},
                        "reviewed_openfigi_source_gap_rows": 2,
                        "openfigi_review_status_totals": {"accepted_source_gap_no_openfigi_match": 1},
                        "openfigi_review_decision_totals": {"no_openfigi_match": 2},
                        "isin_apply_eligibility_totals": {"keep_blank_until_official_isin_source": 4},
                        "figi_apply_eligibility_totals": {"blocked_until_isin_resolved": 5},
                        "verification_evidence_required_totals": {"stronger_figi_source_required_openfigi_no_match_reviewed": 5},
                            "figi_queue_apply_eligibility_totals": {"apply_figi_without_review": 1},
                            "figi_queue_verification_evidence_required_totals": {
                                "none_queue_drained_or_candidates_excluded_as_reviewed_source_gaps": 1,
                            },
                            "figi_queue_review_strategy_totals": {
                                "no_active_openfigi_probe_rows_after_gates": 1,
                            },
                            "top_canada_figi_queue_review_batches": [
                                {
                                    "exchange": "TSX",
                                    "asset_type": "Stock",
                                    "openfigi_exchange_hint": "CN",
                                    "rows": 1,
                                    "review_strategy": "apply_from_symbol",
                                    "apply_eligibility": "apply_figi_without_review",
                                    "verification_evidence_required": "ticker_match_only",
                                    "recommended_next_source": "Bad symbol-only source.",
                                    "source_gate": "Bad symbol-only gate.",
                                }
                            ],
                            "applied_figi_rows": 4,
                        "openfigi_gap_rows_added": 1,
                    },
                    "delta_evidence": {
                        "known_deltas": {
                            "applied_figi_rows": 3,
                            "openfigi_gap_rows_added": 1,
                            "active_figi_queue_rows": 0,
                            "canada_core_exclusion_candidate_rows": 1,
                            "reviewed_openfigi_source_gap_rows": 2,
                        }
                    },
                },
                *[
                    {"campaign_key": f"campaign_{index}", "acceptance_matrix": base_matrix}
                    for index in range(1, 10)
                ],
            ],
        }
    )

    assert result["passed"] is False
    gaps = result["canada_campaign_evidence_gaps"]
    assert {"field": "exchange_totals", "unexpected_exchanges": ["NYSE"]} in gaps
    assert {
        "field": "source_gap_field_totals.missing_isin_primary",
        "reported": 1,
        "missing_isin_rows": 2,
    } in gaps
    assert {
        "field": "openfigi_review_status_totals",
        "reported_total": 1,
        "reviewed_openfigi_source_gap_rows": 2,
    } in gaps
    assert {
        "field": "figi_queue_apply_eligibility_totals",
        "missing_keys": ["no_active_openfigi_probe_rows"],
    } in gaps
    assert {
        "field": "figi_queue_apply_eligibility_totals",
        "forbidden_keys": ["apply_figi_without_review"],
    } in gaps
    assert {
        "field": "canada_core_exclusion_candidate_exchange_totals",
        "reported_total": 2,
        "canada_core_exclusion_candidate_rows": 2,
    } not in gaps
    assert {
        "field": "canada_core_exclusion_candidate_asset_type_totals",
        "reported_total": 1,
        "canada_core_exclusion_candidate_rows": 2,
    } in gaps
    assert {
        "field": "canada_core_exclusion_candidate_exchange_totals",
        "unexpected_exchanges": ["NYSE"],
    } in gaps
    assert {
        "field": "canada_resolution_queue_totals",
        "reported_total": 4,
        "canada_residual_rows": 5,
    } in gaps
    assert {
        "field": "canada_resolution_queue_exchange_totals",
        "queue": "missing_isin_official_canada_masterfiles_do_not_expose_isin",
        "reported_total": 3,
        "expected": 4,
    } in gaps
    assert {"field": "top_canada_figi_queue_review_batches.review_strategy", "row_index": 0, "reported": "apply_from_symbol"} in gaps


def test_evaluate_campaign_acceptance_matrices_requires_asx_residual_campaign_evidence() -> None:
    def matrix() -> dict[str, object]:
        return {
            "exchange_scope": ["ASX"],
            "affected_artifact_rows": 6,
            "campaign_metric_deltas": {
                "residual_rows": {"baseline": 6, "current": 6, "delta": 0},
            },
            "global_acceptance_deltas": {
                key: {"baseline": 1, "current": 1, "delta": 0}
                for key in REQUIRED_DELTA_KEYS
            },
            "exchange_scope_deltas": {
                "exchange_count": 1,
                "changed_exchange_rows": 0,
                "delta_totals": {key: 0 for key in REQUIRED_DELTA_KEYS},
            },
            "required_metrics": REQUIRED_DELTA_KEYS,
        }

    result = evaluate_campaign_acceptance_matrices(
        {
            "summary": {"campaigns": 10},
            "campaigns": [
                {
                    "campaign_key": "asx",
                    "acceptance_matrix": matrix(),
                    "evidence": {
                        "asx_residual_rows": 6,
                        "field_totals": {"missing_isin_primary": 4, "missing_etf_category": 2},
                        "asset_type_totals": {"Stock": 4, "ETF": 2},
                        "asx_core_exclusion_candidate_rows": 2,
                        "asx_core_exclusion_candidate_field_totals": {"missing_isin_primary": 2},
                        "asx_core_exclusion_candidate_asset_type_totals": {"Stock": 2},
                        "asx_core_exclusion_candidate_gap_class_totals": {
                            "official_current_directory_absent_identifier_gap": 2
                        },
                        "asx_core_exclusion_candidate_resolution_queue_totals": {
                            "core_exclusion_candidate_identifier_scope_review": 2
                        },
                        "asx_core_exclusion_candidate_official_source_totals": {
                            "asx_listed_companies": 2
                        },
                        "asx_core_exclusion_candidate_official_capability_totals": {
                            "masterfile_exposes_isin=false": 2,
                            "masterfile_exposes_sector=false": 2,
                            "masterfile_match=true": 2,
                        },
                        "gap_class_totals": {
                            "official_current_directory_absent_identifier_gap": 4,
                            "official_product_taxonomy_unavailable_gap": 2,
                        },
                        "source_of_truth_outcome_totals": {"accepted_source_gap": 2, "core_exclusion_candidate": 4},
                        "asx_residual_backlog": {
                            "status": "review_only_scope_identifier_or_product_taxonomy_source_gaps",
                            "rows": 6,
                            "scope_decision_required_rows": 2,
                            "identity_review_required_rows": 0,
                            "official_identifier_source_required_rows": 2,
                            "official_product_taxonomy_required_rows": 2,
                            "official_isin_apply_candidate_rows": 0,
                            "direct_data_apply_allowed_rows": 0,
                            "metadata_enrichment_authorized": False,
                            "source_gate": (
                                "ASX residual work remains blocked unless exact ASX workbook, registry, issuer, trustee, prospectus, "
                                "or reviewed product-taxonomy evidence proves the value; no ticker/name or peer-instrument inference."
                            ),
                        },
                        "asx_residual_backlog_queue_totals": {
                            "core_exclusion_candidate_identifier_scope_review": 2,
                            "missing_etf_category_requires_official_product_taxonomy": 2,
                            "missing_isin_not_in_current_asx_isin_workbook": 2,
                        },
                        "asx_residual_backlog_evidence_required_totals": {
                            "direct_registry_issuer_or_official_asx_identifier_source_with_exact_listing_match": 2,
                            "official_or_reviewed_product_taxonomy_with_exact_listing_match": 2,
                            "scope_decision_for_core_extended_or_exclude_before_identifier_or_category_fill": 2,
                        },
                        "asx_resolution_queue_totals": {
                            "core_exclusion_candidate_identifier_scope_review": 2,
                            "missing_etf_category_requires_official_product_taxonomy": 2,
                            "missing_isin_not_in_current_asx_isin_workbook": 2,
                        },
                        "asx_resolution_queue_field_totals": {
                            "core_exclusion_candidate_identifier_scope_review": {"missing_isin_primary": 2},
                            "missing_etf_category_requires_official_product_taxonomy": {"missing_etf_category": 2},
                            "missing_isin_not_in_current_asx_isin_workbook": {"missing_isin_primary": 2},
                        },
                        "asx_resolution_queue_gap_class_totals": {
                            "core_exclusion_candidate_identifier_scope_review": {
                                "official_current_directory_absent_identifier_gap": 2
                            },
                            "missing_etf_category_requires_official_product_taxonomy": {
                                "official_product_taxonomy_unavailable_gap": 2
                            },
                            "missing_isin_not_in_current_asx_isin_workbook": {
                                "official_current_directory_absent_identifier_gap": 2
                            },
                        },
                        "asx_resolution_queue_official_source_totals": {
                            "core_exclusion_candidate_identifier_scope_review": {"asx_listed_companies": 2},
                            "missing_etf_category_requires_official_product_taxonomy": {"asx_listed_companies": 2},
                            "missing_isin_not_in_current_asx_isin_workbook": {"asx_listed_companies": 2},
                        },
                        "asx_resolution_queue_review_strategy_totals": {
                            "core_exclusion_candidate_identifier_scope_review": {
                                "scope_review_before_asx_identifier_enrichment": 2
                            },
                            "missing_etf_category_requires_official_product_taxonomy": {
                                "seek_official_or_reviewed_asx_product_taxonomy": 2
                            },
                            "missing_isin_not_in_current_asx_isin_workbook": {
                                "keep_isin_blank_until_current_asx_or_registry_source": 2
                            },
                        },
                        "asx_resolution_queue_evidence_required_totals": {
                            "core_exclusion_candidate_identifier_scope_review": {
                                "scope_decision_for_core_extended_or_exclude_before_identifier_or_category_fill": 2
                            },
                            "missing_etf_category_requires_official_product_taxonomy": {
                                "official_or_reviewed_product_taxonomy_with_exact_listing_match": 2
                            },
                            "missing_isin_not_in_current_asx_isin_workbook": {
                                "direct_registry_issuer_or_official_asx_identifier_source_with_exact_listing_match": 2
                            },
                        },
                        "asx_resolution_queue_official_capability_totals": {
                            "core_exclusion_candidate_identifier_scope_review": {
                                "masterfile_exposes_isin=false": 2,
                                "masterfile_exposes_sector=false": 2,
                                "masterfile_match=true": 2,
                            },
                            "missing_etf_category_requires_official_product_taxonomy": {
                                "masterfile_exposes_isin=false": 2,
                                "masterfile_exposes_sector=false": 2,
                                "masterfile_match=true": 2,
                            },
                            "missing_isin_not_in_current_asx_isin_workbook": {
                                "masterfile_exposes_isin=false": 2,
                                "masterfile_exposes_sector=false": 2,
                                "masterfile_match=true": 2,
                            },
                        },
                        "top_asx_resolution_review_batches": [
                            {
                                "asx_resolution_queue": "core_exclusion_candidate_identifier_scope_review",
                                "field": "missing_isin_primary",
                                "official_source": "asx_listed_companies",
                                "rows": 2,
                                "review_strategy": "scope_review_before_asx_identifier_enrichment",
                                "evidence_required": "scope_decision_for_core_extended_or_exclude_before_identifier_or_category_fill",
                                "recommended_next_source": (
                                    "asx_listed_companies plus reviewed scope decision for core, extended, or exclude before identifier work."
                                ),
                                "source_gate": (
                                    "No ISIN or category enrichment until the listing is reviewed as core, extended, or excluded."
                                ),
                            }
                        ],
                        "residual_decision_totals": {
                            "accepted_source_gap_not_in_current_asx_isin_workbook": 2,
                            "core_exclusion_candidate_requires_scope_review": 4,
                        },
                        "review_bucket_totals": {
                            "identifier_source_gap": 2,
                            "product_taxonomy_source_gap": 2,
                            "scope_review_before_any_data_fill": 2,
                        },
                        "review_priority_totals": {"P1": 2, "P2": 2, "P3": 2},
                        "apply_eligibility_totals": {
                            "blocked_until_core_or_extended_scope_decision": 2,
                            "keep_category_blank_until_official_product_taxonomy_source": 2,
                            "keep_identifier_blank_until_direct_registry_issuer_or_official_asx_evidence": 2,
                        },
                        "verification_evidence_required_totals": {
                            "scope_decision_for_core_extended_or_exclude_before_identifier_or_category_fill": 2,
                            "official_or_reviewed_product_taxonomy_with_exact_listing_match": 2,
                            "direct_registry_issuer_or_official_asx_identifier_source_with_exact_listing_match": 2,
                        },
                        "asx_isin_probe_decision_totals": {"no_asx_match": 4},
                        "official_masterfile_match_totals": {"true": 6},
                        "official_masterfile_exposes_isin_totals": {"false": 6},
                        "official_masterfile_exposes_sector_totals": {"false": 6},
                        "official_masterfile_source_totals": {"asx_listed_companies": 6},
                    },
                    "delta_evidence": {
                        "known_deltas": {
                            "current_asx_residual_rows": 6,
                            "asx_core_exclusion_candidate_rows": 2,
                            "campaign_start_residual_snapshot": {"baseline": 6, "current": 6, "delta": 0},
                            "source_gap_delta": {"baseline": 1, "current": 1, "delta": 0},
                            "warn_quarantine_delta": {"warn_delta": 0, "quarantine_delta": 0},
                        }
                    },
                },
                *[
                    {"campaign_key": f"campaign_{index}", "acceptance_matrix": matrix()}
                    for index in range(1, 10)
                ],
            ],
        }
    )

    assert result["passed"] is True
    assert result["asx_campaign_evidence_gaps"] == []


def test_evaluate_campaign_acceptance_matrices_fails_for_stale_asx_residual_campaign_evidence() -> None:
    base_matrix = {
        "exchange_scope": ["ASX"],
        "affected_artifact_rows": 6,
        "campaign_metric_deltas": {},
        "global_acceptance_deltas": {
            key: {"baseline": 1, "current": 1, "delta": 0}
            for key in REQUIRED_DELTA_KEYS
        },
        "exchange_scope_deltas": {
            "exchange_count": 1,
            "changed_exchange_rows": 0,
            "delta_totals": {key: 0 for key in REQUIRED_DELTA_KEYS},
        },
        "required_metrics": REQUIRED_DELTA_KEYS,
    }

    result = evaluate_campaign_acceptance_matrices(
        {
            "summary": {"campaigns": 10},
            "campaigns": [
                {
                    "campaign_key": "asx",
                    "acceptance_matrix": base_matrix,
                    "evidence": {
                        "asx_residual_rows": 6,
                        "field_totals": {"missing_isin_primary": 4, "bad_field": 2},
                        "asset_type_totals": {"Stock": 6},
                        "asx_core_exclusion_candidate_rows": 2,
                        "asx_core_exclusion_candidate_field_totals": {"missing_isin_primary": 1},
                        "asx_core_exclusion_candidate_asset_type_totals": {"Stock": 2},
                        "asx_core_exclusion_candidate_gap_class_totals": {
                            "official_current_directory_absent_identifier_gap": 2
                        },
                        "asx_core_exclusion_candidate_resolution_queue_totals": {
                            "core_exclusion_candidate_identifier_scope_review": 1
                        },
                        "asx_core_exclusion_candidate_official_source_totals": {},
                        "asx_core_exclusion_candidate_official_capability_totals": {},
                        "gap_class_totals": {"official_current_directory_absent_identifier_gap": 5},
                        "source_of_truth_outcome_totals": {"accepted_source_gap": 6},
                        "asx_residual_backlog": {
                            "status": "review_only_scope_identifier_or_product_taxonomy_source_gaps",
                            "rows": 6,
                            "scope_decision_required_rows": 0,
                            "identity_review_required_rows": 0,
                            "official_identifier_source_required_rows": 6,
                            "official_product_taxonomy_required_rows": 0,
                            "official_isin_apply_candidate_rows": 0,
                            "direct_data_apply_allowed_rows": 0,
                            "metadata_enrichment_authorized": False,
                            "source_gate": (
                                "ASX residual work remains blocked unless exact ASX workbook, registry, issuer, trustee, prospectus, "
                                "or reviewed product-taxonomy evidence proves the value; no ticker/name or peer-instrument inference."
                            ),
                        },
                        "asx_residual_backlog_queue_totals": {
                            "missing_isin_not_in_current_asx_isin_workbook": 6,
                        },
                        "asx_residual_backlog_evidence_required_totals": {
                            "direct_registry_issuer_or_official_asx_identifier_source_with_exact_listing_match": 6,
                        },
                        "asx_resolution_queue_totals": {
                            "missing_isin_not_in_current_asx_isin_workbook": 5,
                        },
                        "asx_resolution_queue_field_totals": {
                            "missing_isin_not_in_current_asx_isin_workbook": {"missing_isin_primary": 4},
                        },
                        "asx_resolution_queue_gap_class_totals": {
                            "missing_isin_not_in_current_asx_isin_workbook": {
                                "official_current_directory_absent_identifier_gap": 5
                            },
                        },
                        "asx_resolution_queue_official_source_totals": {
                            "missing_isin_not_in_current_asx_isin_workbook": {"asx_listed_companies": 5},
                        },
                        "asx_resolution_queue_review_strategy_totals": {
                            "missing_isin_not_in_current_asx_isin_workbook": {
                                "keep_isin_blank_until_current_asx_or_registry_source": 5,
                            },
                        },
                            "asx_resolution_queue_evidence_required_totals": {
                                "missing_isin_not_in_current_asx_isin_workbook": {
                                    "wrong_evidence": 5,
                                },
                            },
                            "asx_resolution_queue_official_capability_totals": {
                                "missing_isin_not_in_current_asx_isin_workbook": {
                                    "masterfile_exposes_isin=false": 5,
                                    "masterfile_exposes_sector=false": 5,
                                    "masterfile_match=true": 5,
                                },
                            },
                        "top_asx_resolution_review_batches": [
                            {
                                "asx_resolution_queue": "missing_isin_not_in_current_asx_isin_workbook",
                                "field": "missing_isin_primary",
                                "official_source": "asx_listed_companies",
                                "rows": 5,
                                "review_strategy": "keep_isin_blank_until_current_asx_or_registry_source",
                                "evidence_required": "wrong_evidence",
                                "recommended_next_source": "",
                                "source_gate": "",
                            }
                        ],
                        "residual_decision_totals": {"accepted_source_gap_not_in_current_asx_isin_workbook": 6},
                        "review_bucket_totals": {"identifier_source_gap": 6},
                        "review_priority_totals": {"P3": 6},
                        "apply_eligibility_totals": {"apply_without_exact_asx_gate": 1, "keep_identifier_blank_until_direct_registry_issuer_or_official_asx_evidence": 5},
                        "verification_evidence_required_totals": {
                            "direct_registry_issuer_or_official_asx_identifier_source_with_exact_listing_match": 6,
                        },
                            "asx_isin_probe_decision_totals": {"no_asx_match": 3},
                            "official_masterfile_match_totals": {"true": 6},
                            "official_masterfile_exposes_isin_totals": {"false": 6},
                            "official_masterfile_exposes_sector_totals": {"false": 6},
                            "official_masterfile_source_totals": {"asx_listed_companies": 6},
                    },
                    "delta_evidence": {
                        "known_deltas": {
                            "current_asx_residual_rows": 5,
                            "asx_core_exclusion_candidate_rows": 1,
                        }
                    },
                },
                *[
                    {"campaign_key": f"campaign_{index}", "acceptance_matrix": base_matrix}
                    for index in range(1, 10)
                ],
            ],
        }
    )

    assert result["passed"] is False
    gaps = result["asx_campaign_evidence_gaps"]
    assert {
        "field": "gap_class_totals",
        "reported_total": 5,
        "asx_residual_rows": 6,
    } in gaps
    assert {"field": "field_totals", "unexpected_fields": ["bad_field"]} in gaps
    assert {
        "field": "asx_isin_probe_decision_totals",
        "reported_total": 3,
        "missing_isin_primary": 4,
    } in gaps
    assert {
        "field": "asx_resolution_queue_totals",
        "reported_total": 5,
        "asx_residual_rows": 6,
    } in gaps
    assert {
        "field": "asx_resolution_queue_field_totals",
        "queue": "missing_isin_not_in_current_asx_isin_workbook",
        "reported_total": 4,
        "expected": 5,
    } in gaps
    assert {
        "field": "asx_core_exclusion_candidate_field_totals",
        "reported_total": 1,
        "asx_core_exclusion_candidate_rows": 2,
    } in gaps
    assert {
        "field": "review_bucket_totals",
        "missing_keys": ["scope_review_before_any_data_fill", "product_taxonomy_source_gap"],
    } in gaps
    assert {
        "field": "apply_eligibility_totals",
        "forbidden_keys": ["apply_without_exact_asx_gate"],
    } in gaps
    assert {
        "field": "delta_evidence.current_asx_residual_rows",
        "reported": 5,
        "expected": 6,
    } in gaps
    assert {
        "field": "delta_evidence.asx_core_exclusion_candidate_rows",
        "reported": 1,
        "expected": 2,
    } in gaps
    assert {
        "field": "delta_evidence.known_deltas",
        "missing_keys": ["campaign_start_residual_snapshot", "source_gap_delta", "warn_quarantine_delta"],
    } in gaps


def test_evaluate_campaign_acceptance_matrices_requires_weak_sector_campaign_evidence() -> None:
    def matrix() -> dict[str, object]:
        return {
            "exchange_scope": sorted(["BK", "CSE_LK", "CSE_MA", "Euronext", "NGX", "OSL", "PSE", "SEM"]),
            "affected_artifact_rows": 8,
            "campaign_metric_deltas": {
                "residual_rows": {"baseline": 8, "current": 8, "delta": 0},
            },
            "global_acceptance_deltas": {
                key: {"baseline": 1, "current": 1, "delta": 0}
                for key in REQUIRED_DELTA_KEYS
            },
            "exchange_scope_deltas": {
                "exchange_count": 8,
                "changed_exchange_rows": 0,
                "delta_totals": {key: 0 for key in REQUIRED_DELTA_KEYS},
            },
            "required_metrics": REQUIRED_DELTA_KEYS,
        }

    exchanges = {"BK": 1, "CSE_LK": 1, "CSE_MA": 1, "Euronext": 1, "NGX": 1, "OSL": 1, "PSE": 1, "SEM": 1}
    result = evaluate_campaign_acceptance_matrices(
        {
            "summary": {"campaigns": 10},
            "campaigns": [
                {
                    "campaign_key": "weak_sector",
                    "acceptance_matrix": matrix(),
                    "evidence": {
                        "weak_sector_rows": 8,
                        "exchanges": sorted(exchanges),
                        "exchange_totals": exchanges,
                        "top_exchange_residuals": dict(list(exchanges.items())[:5]),
                        "official_sector_candidate_rows": 2,
                        "official_sector_candidate_exchange_totals": {"NGX": 2},
                        "official_sector_candidate_value_totals": {"SERVICES": 2},
                        "scope_review_rows": 2,
                        "scope_review_exchange_totals": {"PSE": 2},
                        "scope_review_gap_class_totals": {"official_industry_taxonomy_unavailable_gap": 2},
                        "masterfile_without_sector_rows": 2,
                        "masterfile_without_sector_exchange_totals": {"BK": 2},
                        "gap_class_totals": {"official_industry_taxonomy_unavailable_gap": 8},
                        "source_of_truth_outcome_totals": {"accepted_source_gap": 6, "core_exclusion_candidate": 2},
                        "weak_sector_backlog": {
                            "status": "venue_specific_review_queue_open",
                            "rows": 8,
                            "official_sector_candidate_rows": 2,
                            "scope_decision_required_rows": 2,
                            "masterfile_without_sector_rows": 2,
                            "venue_taxonomy_source_required_rows": 2,
                            "direct_sector_apply_allowed_rows": 0,
                            "metadata_enrichment_authorized": False,
                            "source_gate": (
                                "Weak-sector enrichment remains blocked unless venue-official masterfile, issuer, or reviewed taxonomy "
                                "evidence maps the exact listing to a canonical sector; no global symbol/name/peer inference is allowed."
                            ),
                        },
                        "weak_sector_backlog_queue_totals": {
                            "core_exclusion_candidate_scope_review_before_sector_fill": 2,
                            "official_masterfile_without_sector_source_gap": 2,
                            "official_sector_candidate_normalization_review": 2,
                            "venue_official_taxonomy_unavailable_source_gap": 2,
                        },
                        "weak_sector_backlog_evidence_required_totals": {
                            "new_or_restored_official_venue_industry_taxonomy_source": 2,
                            "official_masterfile_or_issuer_taxonomy_update_exposing_sector_for_exact_listing": 2,
                            "official_venue_sector_value_with_canonical_mapping_and_exact_listing_key_match": 2,
                            "scope_decision_for_core_extended_or_exclude_before_sector_enrichment": 2,
                        },
                        "weak_sector_resolution_queue_totals": {
                            "core_exclusion_candidate_scope_review_before_sector_fill": 2,
                            "official_masterfile_without_sector_source_gap": 2,
                            "official_sector_candidate_normalization_review": 2,
                            "venue_official_taxonomy_unavailable_source_gap": 2,
                        },
                        "weak_sector_resolution_queue_exchange_totals": {
                            "core_exclusion_candidate_scope_review_before_sector_fill": {"PSE": 2},
                            "official_masterfile_without_sector_source_gap": {"BK": 2},
                            "official_sector_candidate_normalization_review": {"NGX": 2},
                            "venue_official_taxonomy_unavailable_source_gap": {"CSE_MA": 1, "Euronext": 1},
                        },
                        "weak_sector_resolution_queue_gap_class_totals": {
                            "core_exclusion_candidate_scope_review_before_sector_fill": {
                                "official_industry_taxonomy_unavailable_gap": 2
                            },
                            "official_masterfile_without_sector_source_gap": {
                                "official_industry_taxonomy_unavailable_gap": 2
                            },
                            "official_sector_candidate_normalization_review": {
                                "official_industry_taxonomy_unavailable_gap": 2
                            },
                            "venue_official_taxonomy_unavailable_source_gap": {
                                "official_industry_taxonomy_unavailable_gap": 2
                            },
                        },
                        "weak_sector_resolution_queue_official_source_totals": {
                            "core_exclusion_candidate_scope_review_before_sector_fill": {
                                "pse_listed_company_directory": 2
                            },
                            "official_masterfile_without_sector_source_gap": {
                                "pse_listed_company_directory": 2
                            },
                            "official_sector_candidate_normalization_review": {
                                "pse_listed_company_directory": 2
                            },
                            "venue_official_taxonomy_unavailable_source_gap": {
                                "pse_listed_company_directory": 2
                            },
                        },
                        "weak_sector_resolution_queue_review_strategy_totals": {
                            "core_exclusion_candidate_scope_review_before_sector_fill": {
                                "scope_review_before_weak_sector_enrichment": 2
                            },
                            "official_masterfile_without_sector_source_gap": {
                                "keep_blank_until_official_masterfile_or_issuer_sector_source": 2
                            },
                            "official_sector_candidate_normalization_review": {
                                "normalize_official_sector_candidate_before_apply": 2
                            },
                            "venue_official_taxonomy_unavailable_source_gap": {
                                "restore_or_add_venue_official_taxonomy_parser": 2
                            },
                        },
                        "weak_sector_resolution_queue_official_capability_totals": {
                            "core_exclusion_candidate_scope_review_before_sector_fill": {
                                "masterfile_exposes_sector=false": 2,
                                "masterfile_match=true": 2,
                            },
                            "official_masterfile_without_sector_source_gap": {
                                "masterfile_exposes_sector=false": 2,
                                "masterfile_match=true": 2,
                            },
                            "official_sector_candidate_normalization_review": {
                                "masterfile_exposes_sector=true": 2,
                                "masterfile_match=true": 2,
                            },
                            "venue_official_taxonomy_unavailable_source_gap": {
                                "masterfile_exposes_sector=false": 2,
                                "masterfile_match=true": 2,
                            },
                        },
                        "venue_backlog_exchange_queue_totals": {
                            "BK": {"official_masterfile_without_sector_source_gap": 1},
                            "CSE_LK": {"core_exclusion_candidate_scope_review_before_sector_fill": 1},
                            "CSE_MA": {"venue_official_taxonomy_unavailable_source_gap": 1},
                            "Euronext": {"venue_official_taxonomy_unavailable_source_gap": 1},
                            "NGX": {"official_sector_candidate_normalization_review": 1},
                            "OSL": {"official_sector_candidate_normalization_review": 1},
                            "PSE": {"core_exclusion_candidate_scope_review_before_sector_fill": 1},
                            "SEM": {"official_masterfile_without_sector_source_gap": 1},
                        },
                        "venue_backlog_exchange_official_capability_totals": {
                            "BK": {"masterfile_exposes_sector=false": 1, "masterfile_match=true": 1},
                            "CSE_LK": {"masterfile_exposes_sector=false": 1, "masterfile_match=true": 1},
                            "CSE_MA": {"masterfile_exposes_sector=false": 1, "masterfile_match=true": 1},
                            "Euronext": {"masterfile_exposes_sector=false": 1, "masterfile_match=true": 1},
                            "NGX": {"masterfile_exposes_sector=true": 1, "masterfile_match=true": 1},
                            "OSL": {"masterfile_exposes_sector=true": 1, "masterfile_match=true": 1},
                            "PSE": {"masterfile_exposes_sector=false": 1, "masterfile_match=true": 1},
                            "SEM": {"masterfile_exposes_sector=false": 1, "masterfile_match=true": 1},
                        },
                        "top_venue_backlog_batches": [
                            {
                                "weak_sector_resolution_queue": "core_exclusion_candidate_scope_review_before_sector_fill",
                                "exchange": "PSE",
                                "official_source": "pse_listed_company_directory",
                                "rows": 2,
                                "review_strategy": "scope_review_before_weak_sector_enrichment",
                                "evidence_required": "scope_decision_for_core_extended_or_exclude_before_sector_enrichment",
                                "recommended_next_source": "Official listing scope evidence before any sector enrichment.",
                                "source_gate": (
                                    "No sector fill until the listing is confirmed as core, extended, or excluded."
                                ),
                            }
                        ],
                        "top_weak_sector_resolution_review_batches": [
                            {
                                "weak_sector_resolution_queue": "core_exclusion_candidate_scope_review_before_sector_fill",
                                "exchange": "PSE",
                                "official_source": "pse_listed_company_directory",
                                "rows": 2,
                                "review_strategy": "scope_review_before_weak_sector_enrichment",
                                "evidence_required": "scope_decision_for_core_extended_or_exclude_before_sector_enrichment",
                                "recommended_next_source": "Official listing scope evidence before any sector enrichment.",
                                "source_gate": (
                                    "No sector fill until the listing is confirmed as core, extended, or excluded."
                                ),
                            }
                        ],
                        "residual_decision_totals": {
                            "official_sector_available_review_apply": 2,
                            "accepted_source_gap_no_official_sector_taxonomy": 4,
                            "core_exclusion_candidate_requires_scope_review": 2,
                        },
                        "review_bucket_totals": {
                            "official_sector_candidate_requires_normalization_gate": 2,
                            "scope_review_before_sector_fill": 2,
                            "official_masterfile_without_sector_source_gap": 2,
                            "venue_official_taxonomy_unavailable_source_gap": 2,
                        },
                        "review_priority_totals": {"P1": 4, "P2": 2, "P3": 2},
                        "apply_eligibility_totals": {
                            "blocked_until_canonical_sector_normalization_and_listing_key_gate": 2,
                            "blocked_until_core_or_extended_scope_decision": 2,
                            "keep_sector_blank_until_official_masterfile_exposes_sector_or_reviewed_taxonomy": 2,
                            "keep_sector_blank_until_venue_official_taxonomy_source_exists": 2,
                        },
                        "verification_evidence_required_totals": {
                            "official_venue_sector_value_with_canonical_mapping_and_exact_listing_key_match": 2,
                            "scope_decision_for_core_extended_or_exclude_before_sector_enrichment": 2,
                            "official_masterfile_or_issuer_taxonomy_update_exposing_sector_for_exact_listing": 2,
                            "new_or_restored_official_venue_industry_taxonomy_source": 2,
                        },
                        "official_masterfile_match_totals": {"true": 8},
                        "official_masterfile_exposes_sector_totals": {"false": 6, "true": 2},
                        "official_masterfile_source_totals": {"pse_listed_company_directory": 8},
                        "official_sector_value_totals": {"SERVICES": 2},
                        "ngx_applied_rows": 2,
                        "ngx_written_updates": 0,
                    },
                    "delta_evidence": {
                        "known_deltas": {
                            "ngx_review_rows": 2,
                            "ngx_written_updates": 0,
                            "current_weak_sector_rows": 8,
                            "official_sector_candidate_rows": 2,
                            "scope_review_rows": 2,
                            "masterfile_without_sector_rows": 2,
                            "campaign_start_sector_coverage_snapshot": {"baseline": 8, "current": 8, "delta": 0},
                            "source_gap_delta": {"baseline": 1, "current": 1, "delta": 0},
                            "warn_quarantine_delta": {"warn_delta": 0, "quarantine_delta": 0},
                        }
                    },
                },
                *[
                    {"campaign_key": f"campaign_{index}", "acceptance_matrix": matrix()}
                    for index in range(1, 10)
                ],
            ],
        }
    )

    assert result["passed"] is True
    assert result["weak_sector_campaign_evidence_gaps"] == []


def test_evaluate_campaign_acceptance_matrices_fails_for_stale_weak_sector_campaign_evidence() -> None:
    base_matrix = {
        "exchange_scope": sorted(["BK", "CSE_LK", "CSE_MA", "Euronext", "NGX", "OSL", "PSE", "SEM"]),
        "affected_artifact_rows": 8,
        "campaign_metric_deltas": {},
        "global_acceptance_deltas": {
            key: {"baseline": 1, "current": 1, "delta": 0}
            for key in REQUIRED_DELTA_KEYS
        },
        "exchange_scope_deltas": {
            "exchange_count": 8,
            "changed_exchange_rows": 0,
            "delta_totals": {key: 0 for key in REQUIRED_DELTA_KEYS},
        },
        "required_metrics": REQUIRED_DELTA_KEYS,
    }

    result = evaluate_campaign_acceptance_matrices(
        {
            "summary": {"campaigns": 10},
            "campaigns": [
                {
                    "campaign_key": "weak_sector",
                    "acceptance_matrix": base_matrix,
                    "evidence": {
                        "weak_sector_rows": 8,
                        "exchanges": ["BK", "PSE"],
                        "exchange_totals": {"BK": 4, "PSE": 3, "NYSE": 1},
                        "top_exchange_residuals": {"BK": 4},
                        "official_sector_candidate_rows": 8,
                        "official_sector_candidate_exchange_totals": {"PSE": 7},
                        "official_sector_candidate_value_totals": {"SERVICES": 7},
                        "scope_review_rows": 2,
                        "scope_review_exchange_totals": {"PSE": 1},
                        "scope_review_gap_class_totals": {"official_industry_taxonomy_unavailable_gap": 2},
                        "masterfile_without_sector_rows": 1,
                        "masterfile_without_sector_exchange_totals": {},
                        "gap_class_totals": {"official_industry_taxonomy_unavailable_gap": 7},
                        "source_of_truth_outcome_totals": {"accepted_source_gap": 8},
                        "weak_sector_backlog": {
                            "status": "unsafe_apply",
                            "rows": 7,
                            "official_sector_candidate_rows": 8,
                            "scope_decision_required_rows": 2,
                            "masterfile_without_sector_rows": 1,
                            "venue_taxonomy_source_required_rows": 0,
                            "direct_sector_apply_allowed_rows": 1,
                            "metadata_enrichment_authorized": True,
                            "source_gate": "",
                        },
                        "weak_sector_backlog_queue_totals": {
                            "official_sector_candidate_normalization_review": 7,
                        },
                        "weak_sector_backlog_evidence_required_totals": {
                            "official_venue_sector_value_with_canonical_mapping_and_exact_listing_key_match": 7,
                        },
                        "weak_sector_resolution_queue_totals": {
                            "official_sector_candidate_normalization_review": 7,
                        },
                        "weak_sector_resolution_queue_exchange_totals": {
                            "official_sector_candidate_normalization_review": {"PSE": 6},
                        },
                        "weak_sector_resolution_queue_gap_class_totals": {
                            "official_sector_candidate_normalization_review": {
                                "official_industry_taxonomy_unavailable_gap": 6
                            },
                        },
                        "weak_sector_resolution_queue_official_source_totals": {
                            "official_sector_candidate_normalization_review": {
                                "pse_listed_company_directory": 6
                            },
                        },
                        "weak_sector_resolution_queue_review_strategy_totals": {
                            "official_sector_candidate_normalization_review": {
                                "normalize_official_sector_candidate_before_apply": 6
                            },
                        },
                        "weak_sector_resolution_queue_official_capability_totals": {
                            "official_sector_candidate_normalization_review": {
                                "masterfile_exposes_sector=true": 6,
                                "masterfile_match=true": 6,
                            },
                        },
                        "venue_backlog_exchange_queue_totals": {
                            "BK": {"official_sector_candidate_normalization_review": 4},
                            "PSE": {"official_sector_candidate_normalization_review": 3},
                            "NYSE": {"official_sector_candidate_normalization_review": 1},
                        },
                        "venue_backlog_exchange_official_capability_totals": {
                            "BK": {"masterfile_exposes_sector=true": 4, "masterfile_match=true": 4},
                            "PSE": {"masterfile_exposes_sector=true": 3, "masterfile_match=true": 3},
                            "NYSE": {"masterfile_exposes_sector=true": 1, "masterfile_match=true": 1},
                        },
                        "top_venue_backlog_batches": [
                            {
                                "weak_sector_resolution_queue": "official_sector_candidate_normalization_review",
                                "exchange": "PSE",
                                "official_source": "pse_listed_company_directory",
                                "rows": 6,
                                "review_strategy": "normalize_official_sector_candidate_before_apply",
                                "evidence_required": "official_venue_sector_value_with_canonical_mapping_and_exact_listing_key_match",
                                "recommended_next_source": (
                                    "Official sector value from pse_listed_company_directory plus canonical sector mapping."
                                ),
                                "source_gate": (
                                    "Apply only after exact listing-key match and canonical sector normalization."
                                ),
                            }
                        ],
                        "top_weak_sector_resolution_review_batches": [
                            {
                                "weak_sector_resolution_queue": "official_sector_candidate_normalization_review",
                                "exchange": "PSE",
                                "official_source": "pse_listed_company_directory",
                                "rows": 6,
                                "review_strategy": "normalize_official_sector_candidate_before_apply",
                                "evidence_required": "official_venue_sector_value_with_canonical_mapping_and_exact_listing_key_match",
                                "recommended_next_source": (
                                    "Official sector value from pse_listed_company_directory plus canonical sector mapping."
                                ),
                                "source_gate": (
                                    "Apply only after exact listing-key match and canonical sector normalization."
                                ),
                            }
                        ],
                        "residual_decision_totals": {"official_sector_available_review_apply": 8},
                        "review_bucket_totals": {"official_sector_candidate_requires_normalization_gate": 8},
                        "review_priority_totals": {"P1": 8},
                        "apply_eligibility_totals": {"apply_from_global_name_mapping": 1, "blocked_until_canonical_sector_normalization_and_listing_key_gate": 7},
                        "verification_evidence_required_totals": {
                            "official_venue_sector_value_with_canonical_mapping_and_exact_listing_key_match": 8,
                        },
                        "official_masterfile_match_totals": {"true": 8},
                        "official_masterfile_exposes_sector_totals": {"true": 8},
                        "official_masterfile_source_totals": {"pse_listed_company_directory": 8},
                        "official_sector_value_totals": {"SERVICES": 7},
                        "ngx_applied_rows": 2,
                        "ngx_written_updates": 1,
                    },
                    "delta_evidence": {
                        "known_deltas": {
                            "ngx_review_rows": 2,
                            "ngx_written_updates": 1,
                            "current_weak_sector_rows": 7,
                            "official_sector_candidate_rows": 7,
                            "scope_review_rows": 1,
                            "masterfile_without_sector_rows": 1,
                        }
                    },
                },
                *[
                    {"campaign_key": f"campaign_{index}", "acceptance_matrix": base_matrix}
                    for index in range(1, 10)
                ],
            ],
        }
    )

    assert result["passed"] is False
    gaps = result["weak_sector_campaign_evidence_gaps"]
    assert {
        "field": "exchanges",
        "reported": ["BK", "PSE"],
        "expected": sorted(["BK", "CSE_LK", "CSE_MA", "Euronext", "NGX", "OSL", "PSE", "SEM"]),
    } in gaps
    assert {
        "field": "exchange_totals",
        "unexpected_exchanges": ["NYSE"],
        "missing_exchanges": ["CSE_LK", "CSE_MA", "Euronext", "NGX", "OSL", "SEM"],
    } in gaps
    assert {
        "field": "gap_class_totals",
        "reported_total": 7,
        "weak_sector_rows": 8,
    } in gaps
    assert {
        "field": "weak_sector_resolution_queue_totals",
        "reported_total": 7,
        "weak_sector_rows": 8,
    } in gaps
    assert {
        "field": "weak_sector_resolution_queue_exchange_totals",
        "queue": "official_sector_candidate_normalization_review",
        "reported_total": 6,
        "expected": 7,
    } in gaps
    assert {
        "field": "weak_sector_resolution_queue_gap_class_totals",
        "queue": "official_sector_candidate_normalization_review",
        "reported_total": 6,
        "expected": 7,
    } in gaps
    assert {
        "field": "weak_sector_resolution_queue_official_capability_totals",
        "queue": "official_sector_candidate_normalization_review",
        "capability": "masterfile_match",
        "reported_total": 6,
        "expected": 7,
    } in gaps
    assert {
        "field": "weak_sector_resolution_queue_official_capability_totals",
        "queue": "official_sector_candidate_normalization_review",
        "capability": "masterfile_exposes_sector",
        "reported_total": 6,
        "expected": 7,
    } in gaps
    assert {
        "field": "review_bucket_totals",
        "missing_keys": [
            "scope_review_before_sector_fill",
            "official_masterfile_without_sector_source_gap",
            "venue_official_taxonomy_unavailable_source_gap",
        ],
    } in gaps
    assert {
        "field": "apply_eligibility_totals",
        "forbidden_keys": ["apply_from_global_name_mapping"],
    } in gaps
    assert {
        "field": "official_sector_candidate_exchange_totals",
        "reported_total": 7,
        "official_sector_candidate_rows": 8,
    } in gaps
    assert {
        "field": "scope_review_exchange_totals",
        "reported_total": 1,
        "scope_review_rows": 2,
    } in gaps
    assert {
        "field": "masterfile_without_sector_exchange_totals",
        "reported_total": 0,
        "masterfile_without_sector_rows": 1,
    } in gaps
    assert {"field": "ngx_written_updates", "reported": 1, "expected": 0} in gaps
    assert {
        "field": "delta_evidence.current_weak_sector_rows",
        "reported": 7,
        "expected": 8,
    } in gaps
    assert {
        "field": "delta_evidence.known_deltas",
        "missing_keys": [
            "campaign_start_sector_coverage_snapshot",
            "source_gap_delta",
            "warn_quarantine_delta",
        ],
    } in gaps


def test_evaluate_campaign_acceptance_matrices_requires_masterfile_collision_campaign_evidence() -> None:
    def matrix() -> dict[str, object]:
        return {
            "exchange_scope": "all",
            "affected_artifact_rows": 6,
            "campaign_metric_deltas": {},
            "global_acceptance_deltas": {
                key: {"baseline": 1, "current": 1, "delta": 0}
                for key in REQUIRED_DELTA_KEYS
            },
            "exchange_scope_deltas": {
                "exchange_count": 2,
                "changed_exchange_rows": 0,
                "delta_totals": {key: 0 for key in REQUIRED_DELTA_KEYS},
            },
            "required_metrics": REQUIRED_DELTA_KEYS,
        }

    result = evaluate_campaign_acceptance_matrices(
        {
            "summary": {"campaigns": 10},
            "campaigns": [
                {
                    "campaign_key": "masterfile_collisions",
                    "acceptance_matrix": matrix(),
                    "evidence": {
                        "collision_review_rows": 6,
                        "decision_totals": {
                            "new_listing_candidate_requires_official_listing_add_review": 2,
                            "same_isin_cross_listing_candidate_requires_exchange_scope_review": 2,
                            "symbol_collision_requires_non_symbol_identity_source": 2,
                        },
                        "review_bucket_totals": {
                            "distinct_official_isin_new_listing_candidate": 2,
                            "same_isin_cross_listing_needs_name_or_scope_review": 2,
                            "hold_symbol_only_collision_needs_non_symbol_identity": 2,
                        },
                        "review_priority_totals": {"P1": 2, "P2": 2, "P3": 2},
                        "collision_risk_flag_totals": {
                            "ticker_reused_on_other_exchange": 6,
                            "same_isin_existing_listing": 2,
                            "missing_official_isin": 2,
                        },
                        "identity_resolution_queue_totals": {
                            "blocked_symbol_only_missing_non_symbol_identity": 2,
                            "review_cross_listing_same_isin_name_or_scope_gap": 2,
                            "review_distinct_official_isin_new_listing": 2,
                        },
                        "identity_resolution_risk_flag_totals": {
                            "blocked_symbol_only_missing_non_symbol_identity": {
                                "missing_official_isin": 2,
                                "ticker_reused_on_other_exchange": 2,
                            },
                            "review_cross_listing_same_isin_name_or_scope_gap": {
                                "same_isin_existing_listing": 2,
                                "ticker_reused_on_other_exchange": 2,
                            },
                            "review_distinct_official_isin_new_listing": {
                                "existing_isin_conflict": 2,
                                "ticker_reused_on_other_exchange": 2,
                            },
                        },
                        "identity_resolution_exchange_totals": {
                            "blocked_symbol_only_missing_non_symbol_identity": {"NYSE": 1, "TSX": 1},
                            "review_cross_listing_same_isin_name_or_scope_gap": {"NYSE": 1, "TSX": 1},
                            "review_distinct_official_isin_new_listing": {"NYSE": 1, "TSX": 1},
                        },
                        "identity_resolution_asset_type_totals": {
                            "blocked_symbol_only_missing_non_symbol_identity": {"Stock": 2},
                            "review_cross_listing_same_isin_name_or_scope_gap": {"Stock": 2},
                            "review_distinct_official_isin_new_listing": {"ETF": 1, "Stock": 1},
                        },
                        "identity_resolution_official_source_totals": {
                            "blocked_symbol_only_missing_non_symbol_identity": {
                                "sec_company_tickers_exchange": 1,
                                "tmx_listed_issuers": 1,
                            },
                            "review_cross_listing_same_isin_name_or_scope_gap": {
                                "sec_company_tickers_exchange": 1,
                                "tmx_listed_issuers": 1,
                            },
                            "review_distinct_official_isin_new_listing": {
                                "sec_company_tickers_exchange": 1,
                                "tmx_listed_issuers": 1,
                            },
                        },
                        "identity_resolution_existing_exchange_pair_totals": {
                            "blocked_symbol_only_missing_non_symbol_identity": {
                                "NYSE::NASDAQ": 1,
                                "TSX::TSXV": 1,
                            },
                            "review_cross_listing_same_isin_name_or_scope_gap": {
                                "NYSE::NASDAQ": 1,
                                "TSX::TSXV": 1,
                            },
                            "review_distinct_official_isin_new_listing": {
                                "NYSE::NASDAQ": 1,
                                "TSX::TSXV": 1,
                            },
                        },
                        "identity_resolution_pair_review_strategy_totals": {
                            "blocked_symbol_only_missing_non_symbol_identity": {
                                "batch_hold_symbol_reuse_until_non_symbol_identity_source": 2
                            },
                            "review_cross_listing_same_isin_name_or_scope_gap": {
                                "batch_review_same_isin_name_or_scope_reconciliation": 2
                            },
                            "review_distinct_official_isin_new_listing": {
                                "batch_review_distinct_isin_new_listing_candidates": 2
                            },
                        },
                        "identity_resolution_review_strategy_totals": {
                            "blocked_symbol_only_missing_non_symbol_identity": {
                                "batch_hold_symbol_reuse_until_non_symbol_identity_source": 2
                            },
                            "review_cross_listing_same_isin_name_or_scope_gap": {
                                "batch_review_same_isin_name_or_scope_reconciliation": 2
                            },
                            "review_distinct_official_isin_new_listing": {
                                "batch_review_distinct_isin_new_listing_candidates": 2
                            },
                        },
                        "identity_resolution_evidence_required_totals": {
                            "blocked_symbol_only_missing_non_symbol_identity": {
                                "official_non_symbol_identifier_or_keep_absent": 2
                            },
                            "review_cross_listing_same_isin_name_or_scope_gap": {
                                "official_target_exchange_status_plus_issuer_or_name_scope_reconciliation": 2
                            },
                            "review_distinct_official_isin_new_listing": {
                                "official_target_exchange_listing_key_isin_name_instrument_type_listing_status": 2
                            },
                        },
                        "identity_resolution_identity_evidence_totals": {
                            "blocked_symbol_only_missing_non_symbol_identity": {"asset_type_consistent": 2},
                            "review_cross_listing_same_isin_name_or_scope_gap": {
                                "asset_type_consistent": 2,
                                "official_isin": 2,
                                "same_isin_existing_listing": 2,
                            },
                            "review_distinct_official_isin_new_listing": {
                                "asset_type_consistent": 2,
                                "official_isin": 2,
                            },
                        },
                        "identity_resolution_clearance_readiness_totals": {
                            "blocked_symbol_only_non_symbol_identity_required": 2,
                            "needs_official_listing_add_review": 2,
                            "needs_official_name_or_scope_reconciliation": 2,
                        },
                        "identity_resolution_queue_clearance_readiness_totals": {
                            "blocked_symbol_only_missing_non_symbol_identity": {
                                "blocked_symbol_only_non_symbol_identity_required": 2
                            },
                            "review_cross_listing_same_isin_name_or_scope_gap": {
                                "needs_official_name_or_scope_reconciliation": 2
                            },
                            "review_distinct_official_isin_new_listing": {
                                "needs_official_listing_add_review": 2
                            },
                        },
                        "top_identity_resolution_clearance_batches": [
                            {
                                "identity_resolution_queue": "blocked_symbol_only_missing_non_symbol_identity",
                                "clearance_readiness": "blocked_symbol_only_non_symbol_identity_required",
                                "rows": 2,
                                "review_strategy": "batch_hold_symbol_reuse_until_non_symbol_identity_source",
                                "evidence_required": "official_non_symbol_identifier_or_keep_absent",
                                "recommended_next_source": (
                                    "Official non-symbol identifier evidence for target::existing, or keep the target listing absent."
                                ),
                                "source_gate": "Keep absent; ticker equality alone is not identity evidence.",
                            }
                        ],
                        "top_identity_resolution_pair_review_batches": [
                            {
                                "identity_resolution_queue": "review_cross_listing_same_isin_name_or_scope_gap",
                                "exchange_pair": "NYSE::NASDAQ",
                                "rows": 1,
                                "review_strategy": "batch_review_same_isin_name_or_scope_reconciliation",
                                "evidence_required": "official_target_exchange_status_plus_issuer_or_name_scope_reconciliation",
                                "recommended_next_source": (
                                    "Official target-exchange status plus issuer/name or scope reconciliation for NYSE::NASDAQ."
                                ),
                                "source_gate": (
                                    "Do not resolve identity until official listing status and issuer/name or scope "
                                    "differences are reconciled."
                                ),
                            }
                        ],
                        "same_isin_exact_name_scope_review_rows": 0,
                        "top_same_isin_exact_name_scope_review_batches": [],
                        "clearance_evidence_totals": {
                            "official_target_exchange_listing_key_isin_name_instrument_type_listing_status": 2,
                            "official_target_exchange_listing_status_plus_name_or_scope_reconciliation": 2,
                            "official_non_symbol_identifier_or_keep_absent": 2,
                        },
                        "exchange_totals": {"NYSE": 3, "TSX": 3},
                        "official_asset_type_totals": {"Stock": 5, "ETF": 1},
                        "asset_type_mismatch_totals": {"false": 5, "true": 1},
                        "official_source_totals": {"sec_company_tickers_exchange": 3, "tmx_listed_issuers": 3},
                        "asset_type_mismatches": 1,
                    },
                    "delta_evidence": {
                        "known_deltas": {
                            "current_collision_review_rows": 6,
                            "collision_resolution_delta": 0,
                            "listing_addition_delta": 0,
                            "warn_quarantine_delta": {"warn_delta": 0, "quarantine_delta": 0},
                        }
                    },
                },
                *[
                    {"campaign_key": f"campaign_{index}", "acceptance_matrix": matrix()}
                    for index in range(1, 10)
                ],
            ],
        }
    )

    assert result["passed"] is True
    assert result["masterfile_collision_campaign_evidence_gaps"] == []


def test_evaluate_campaign_acceptance_matrices_fails_for_stale_masterfile_collision_campaign_evidence() -> None:
    base_matrix = {
        "exchange_scope": "all",
        "affected_artifact_rows": 6,
        "campaign_metric_deltas": {},
        "global_acceptance_deltas": {
            key: {"baseline": 1, "current": 1, "delta": 0}
            for key in REQUIRED_DELTA_KEYS
        },
        "exchange_scope_deltas": {
            "exchange_count": 2,
            "changed_exchange_rows": 0,
            "delta_totals": {key: 0 for key in REQUIRED_DELTA_KEYS},
        },
        "required_metrics": REQUIRED_DELTA_KEYS,
    }

    result = evaluate_campaign_acceptance_matrices(
        {
            "summary": {"campaigns": 10},
            "campaigns": [
                {
                    "campaign_key": "masterfile_collisions",
                    "acceptance_matrix": base_matrix,
                    "evidence": {
                        "collision_review_rows": 6,
                        "decision_totals": {"symbol_only_add": 6},
                        "review_bucket_totals": {"same_isin_exact_name_cross_listing_candidate": 6},
                        "review_priority_totals": {"P1": 6},
                        "collision_risk_flag_totals": {"ticker_reused_on_other_exchange": 5},
                        "clearance_evidence_totals": {"ticker_match_only": 6},
                        "identity_resolution_queue_totals": {
                            "review_cross_listing_same_isin_exact_name": 6,
                        },
                        "identity_resolution_risk_flag_totals": {
                            "review_cross_listing_same_isin_exact_name": {
                                "ticker_reused_on_other_exchange": 5,
                            },
                        },
                        "identity_resolution_exchange_totals": {
                            "review_cross_listing_same_isin_exact_name": {"NYSE": 5},
                        },
                        "identity_resolution_asset_type_totals": {
                            "review_cross_listing_same_isin_exact_name": {"Stock": 6},
                        },
                        "identity_resolution_official_source_totals": {
                            "review_cross_listing_same_isin_exact_name": {"sec_company_tickers_exchange": 5},
                        },
                        "identity_resolution_existing_exchange_pair_totals": {
                            "review_cross_listing_same_isin_exact_name": {"NYSE::NASDAQ": 5},
                        },
                        "identity_resolution_pair_review_strategy_totals": {
                            "review_cross_listing_same_isin_exact_name": {
                                "batch_hold_symbol_reuse_until_non_symbol_identity_source": 4
                            },
                        },
                        "identity_resolution_review_strategy_totals": {
                            "review_cross_listing_same_isin_exact_name": {
                                "batch_hold_symbol_reuse_until_non_symbol_identity_source": 4
                            },
                        },
                        "identity_resolution_evidence_required_totals": {
                            "review_cross_listing_same_isin_exact_name": {
                                "ticker_match_only": 4,
                            },
                        },
                        "identity_resolution_identity_evidence_totals": {
                            "review_cross_listing_same_isin_exact_name": {
                                "official_isin": 5,
                                "same_isin_existing_listing": 5,
                            },
                        },
                        "top_identity_resolution_pair_review_batches": [
                            {
                                "identity_resolution_queue": "review_cross_listing_same_isin_exact_name",
                                "exchange_pair": "NYSE::NASDAQ",
                                "rows": 5,
                                "review_strategy": "ticker_only_strategy",
                                "evidence_required": "ticker_match_only",
                            }
                        ],
                        "exchange_totals": {"NYSE": 5},
                        "official_asset_type_totals": {"Stock": 6},
                        "asset_type_mismatch_totals": {"false": 5, "true": 1},
                        "official_source_totals": {"sec_company_tickers_exchange": 6},
                        "asset_type_mismatches": 2,
                    },
                    "delta_evidence": {"known_deltas": {"current_collision_review_rows": 5}},
                },
                *[
                    {"campaign_key": f"campaign_{index}", "acceptance_matrix": base_matrix}
                    for index in range(1, 10)
                ],
            ],
        }
    )

    assert result["passed"] is False
    gaps = result["masterfile_collision_campaign_evidence_gaps"]
    assert {
        "field": "decision_totals",
        "unexpected_decisions": ["symbol_only_add"],
    } in gaps
    assert {
        "field": "review_bucket_totals",
        "missing_keys": [
            "hold_symbol_only_collision_needs_non_symbol_identity",
            "same_isin_cross_listing_needs_name_or_scope_review",
            "distinct_official_isin_new_listing_candidate",
        ],
    } in gaps
    assert {
        "field": "clearance_evidence_totals",
        "forbidden_keys": ["ticker_match_only"],
    } in gaps
    assert {
        "field": "exchange_totals",
        "reported_total": 5,
        "collision_review_rows": 6,
    } in gaps
    assert {
        "field": "collision_risk_flag_totals.ticker_reused_on_other_exchange",
        "reported": 5,
        "collision_review_rows": 6,
    } in gaps
    assert {
        "field": "asset_type_mismatch_totals.true",
        "reported": 1,
        "asset_type_mismatches": 2,
    } in gaps
    assert {
        "field": "identity_resolution_pair_review_strategy_totals.review_cross_listing_same_isin_exact_name",
        "reported_total": 4,
        "expected_pair_total": 5,
    } in gaps
    assert {
        "field": "identity_resolution_pair_review_strategy_totals.review_cross_listing_same_isin_exact_name",
        "unexpected_strategies": ["batch_hold_symbol_reuse_until_non_symbol_identity_source"],
    } in gaps
    assert {
        "field": "identity_resolution_identity_evidence_totals.review_cross_listing_same_isin_exact_name.official_isin",
        "reported": 5,
        "expected": 6,
    } in gaps
    assert {
        "field": "identity_resolution_identity_evidence_totals.review_cross_listing_same_isin_exact_name.exact_name_match",
        "reported": None,
        "expected": 6,
    } in gaps
    assert {
        "field": "top_identity_resolution_pair_review_batches",
        "row_index": 0,
        "queue": "review_cross_listing_same_isin_exact_name",
        "review_strategy": "ticker_only_strategy",
        "expected": "batch_review_same_isin_exact_name_cross_listing_scope",
    } in gaps
    assert {
        "field": "delta_evidence.current_collision_review_rows",
        "reported": 5,
        "expected": 6,
    } in gaps
    assert {
        "field": "delta_evidence.known_deltas",
        "missing_keys": ["collision_resolution_delta", "listing_addition_delta", "warn_quarantine_delta"],
    } in gaps


def test_evaluate_campaign_acceptance_matrices_requires_symbol_change_campaign_evidence() -> None:
    def matrix() -> dict[str, object]:
        return {
            "exchange_scope": "all",
            "affected_artifact_rows": 8,
            "campaign_metric_deltas": {},
            "global_acceptance_deltas": {
                key: {"baseline": 1, "current": 1, "delta": 0}
                for key in REQUIRED_DELTA_KEYS
            },
            "exchange_scope_deltas": {
                "exchange_count": 2,
                "changed_exchange_rows": 0,
                "delta_totals": {key: 0 for key in REQUIRED_DELTA_KEYS},
            },
            "required_metrics": REQUIRED_DELTA_KEYS,
        }

    result = evaluate_campaign_acceptance_matrices(
        {
            "summary": {"campaigns": 10},
            "campaigns": [
                {
                    "campaign_key": "symbol_changes",
                    "acceptance_matrix": matrix(),
                    "evidence": {
                        "symbol_change_review_rows": 8,
                        "match_status_counts": {
                            "new_symbol_present_old_symbol_missing": 2,
                            "old_symbol_present_new_symbol_missing": 2,
                            "old_and_new_symbols_present": 2,
                            "no_matching_listing": 2,
                        },
                        "symbol_change_workflow_queue_counts": {
                            "audit_already_reflected": 2,
                            "document_no_dataset_match": 2,
                            "review_duplicate_or_cross_listing": 2,
                            "review_verified_rename_or_delisting": 2,
                        },
                        "symbol_change_backlog": {
                            "status": "listing_keyed_symbol_change_review_queue_open",
                            "rows": 8,
                            "verified_rename_or_delisting_review_rows": 2,
                            "duplicate_or_cross_listing_review_rows": 2,
                            "already_reflected_audit_rows": 2,
                            "out_of_scope_collision_blocked_rows": 0,
                            "missing_source_scope_mapping_rows": 0,
                            "no_dataset_match_documentation_rows": 2,
                            "time_sensitive_review_rows": 2,
                            "direct_symbol_change_apply_allowed_rows": 0,
                            "secondary_feed_apply_authorized": False,
                            "source_gate": (
                                "Symbol-change feed rows are review signals only; ticker, name, listing, or alias changes "
                                "require listing-keyed official venue or issuer evidence for old/new symbols and issuer identity."
                            ),
                        },
                        "review_bucket_counts": {
                            "already_reflected_in_source_scope": 2,
                            "action_required_possible_rename_or_delisting": 2,
                            "action_required_duplicate_or_cross_listing": 2,
                            "no_dataset_match_for_source_scope": 2,
                        },
                        "review_priority_counts": {"P1": 4, "P3": 2, "P4": 2},
                        "review_bucket_priorities": {
                            "already_reflected_in_source_scope": "P4",
                            "action_required_possible_rename_or_delisting": "P1",
                            "action_required_duplicate_or_cross_listing": "P1",
                            "no_dataset_match_for_source_scope": "P3",
                        },
                        "recency_bucket_counts": {"recent_7d": 1, "recent_30d": 1, "recent_90d": 2, "older_than_90d": 4},
                        "review_priority_recency_counts": {
                            "P1:recent_7d": 1,
                            "P1:recent_30d": 1,
                            "P1:recent_90d": 2,
                            "P3:older_than_90d": 2,
                            "P4:older_than_90d": 2,
                        },
                        "workflow_queue_recency_counts": {
                            "audit_already_reflected:older_than_90d": 2,
                            "document_no_dataset_match:older_than_90d": 2,
                            "review_duplicate_or_cross_listing:recent_90d": 2,
                            "review_verified_rename_or_delisting:recent_7d": 1,
                            "review_verified_rename_or_delisting:recent_30d": 1,
                        },
                        "workflow_queue_priority_counts": {
                            "audit_already_reflected:P4": 2,
                            "document_no_dataset_match:P3": 2,
                            "review_duplicate_or_cross_listing:P1": 2,
                            "review_verified_rename_or_delisting:P1": 2,
                        },
                        "workflow_queue_scope_counts": {
                            "audit_already_reflected:global_symbol_collision_outside_source_scope": 2,
                            "document_no_dataset_match:matches_within_source_scope": 2,
                            "review_duplicate_or_cross_listing:matches_within_source_scope": 2,
                            "review_verified_rename_or_delisting:matches_within_source_scope": 2,
                        },
                        "workflow_queue_match_status_counts": {
                            "audit_already_reflected:new_symbol_present_old_symbol_missing": 2,
                            "document_no_dataset_match:no_matching_listing": 2,
                            "review_duplicate_or_cross_listing:old_and_new_symbols_present": 2,
                            "review_verified_rename_or_delisting:old_symbol_present_new_symbol_missing": 2,
                        },
                        "workflow_queue_source_hint_counts": {
                            "audit_already_reflected": {"US_LISTED": 2},
                            "document_no_dataset_match": {"US_LISTED": 2},
                            "review_duplicate_or_cross_listing": {"US_LISTED": 2},
                            "review_verified_rename_or_delisting": {"US_LISTED": 2},
                        },
                        "workflow_queue_source_confidence_counts": {
                            "audit_already_reflected": {"secondary_review": 2},
                            "document_no_dataset_match": {"secondary_review": 2},
                            "review_duplicate_or_cross_listing": {"secondary_review": 2},
                            "review_verified_rename_or_delisting": {"secondary_review": 2},
                        },
                        "workflow_queue_issuer_name_review_counts": {
                            "audit_already_reflected": {"no_scoped_listing_name_available": 2},
                            "document_no_dataset_match": {"no_scoped_listing_name_available": 2},
                            "review_duplicate_or_cross_listing": {"no_scoped_listing_name_available": 2},
                            "review_verified_rename_or_delisting": {"no_scoped_listing_name_available": 2},
                        },
                        "workflow_queue_listing_key_review_counts": {
                            "audit_already_reflected": {"new_scoped_listing_key_only": 2},
                            "document_no_dataset_match": {"no_scoped_listing_key_match": 2},
                            "review_duplicate_or_cross_listing": {"old_and_new_scoped_listing_keys_present": 2},
                            "review_verified_rename_or_delisting": {"old_scoped_listing_key_only": 2},
                        },
                        "workflow_queue_review_strategy_counts": {
                            "audit_already_reflected": {"audit_already_reflected_no_canonical_change": 2},
                            "document_no_dataset_match": {"document_no_dataset_match_without_canonical_action": 2},
                            "review_duplicate_or_cross_listing": {
                                "resolve_duplicate_cross_listing_or_transition_before_any_symbol_change": 2
                            },
                            "review_verified_rename_or_delisting": {
                                "verify_rename_or_delisting_with_official_venue_or_issuer_evidence": 2
                            },
                        },
                        "top_symbol_change_workflow_batches": [
                            {
                                "symbol_change_workflow_queue": "review_verified_rename_or_delisting",
                                "review_priority": "P1",
                                "recency_bucket": "recent_7d",
                                "exchange_scope_status": "matches_within_source_scope",
                                "rows": 1,
                                "review_strategy": "verify_rename_or_delisting_with_official_venue_or_issuer_evidence",
                                "evidence_required": "official_exchange_notice_or_current_directory_showing_old_symbol_inactive_new_symbol_active_same_issuer",
                                "recommended_next_source": (
                                    "Official exchange notice, issuer notice, or current exchange directory proving "
                                    "old/new symbols for the same issuer."
                                ),
                                "source_gate": (
                                    "Do not rename until official listing-keyed evidence proves old inactive and new "
                                    "active for the same issuer."
                                ),
                            },
                            {
                                "symbol_change_workflow_queue": "review_duplicate_or_cross_listing",
                                "review_priority": "P1",
                                "recency_bucket": "recent_90d",
                                "exchange_scope_status": "matches_within_source_scope",
                                "rows": 2,
                                "review_strategy": "resolve_duplicate_cross_listing_or_transition_before_any_symbol_change",
                                "evidence_required": "official_exchange_directory_plus_listing_key_review_to_distinguish_duplicate_cross_listing_or_transition",
                                "recommended_next_source": (
                                    "Official exchange directory records plus listing-key review for both symbols."
                                ),
                                "source_gate": (
                                    "Do not change symbols until duplicate, cross-listing, or transition state is "
                                    "resolved listing-key by listing-key."
                                ),
                            },
                            {
                                "symbol_change_workflow_queue": "document_no_dataset_match",
                                "review_priority": "P3",
                                "recency_bucket": "older_than_90d",
                                "exchange_scope_status": "matches_within_source_scope",
                                "rows": 2,
                                "review_strategy": "document_no_dataset_match_without_canonical_action",
                                "evidence_required": "official_exchange_scope_mapping_or_ignore_as_external_non_dataset_event",
                                "recommended_next_source": (
                                    "Official exchange scope mapping, or document the event as outside the dataset."
                                ),
                                "source_gate": (
                                    "No dataset action without scoped official mapping to an existing or intended listing."
                                ),
                            },
                        ],
                        "apply_eligibility_counts": {
                            "requires_official_venue_confirmation": 4,
                            "no_dataset_action_without_scope_mapping": 2,
                            "audit_only_no_apply": 2,
                        },
                        "symbol_change_apply_readiness_counts": {
                            "blocked_until_listing_keyed_official_symbol_change_evidence": 4,
                            "document_or_ignore_until_scoped_official_dataset_match": 2,
                            "audit_only_no_canonical_change": 2,
                        },
                        "verification_evidence_required_counts": {
                            "official_exchange_notice_or_current_directory_showing_old_symbol_inactive_new_symbol_active_same_issuer": 2,
                            "official_exchange_directory_plus_listing_key_review_to_distinguish_duplicate_cross_listing_or_transition": 2,
                            "official_exchange_scope_mapping_or_ignore_as_external_non_dataset_event": 2,
                            "audit_only_confirm_no_canonical_change_needed": 2,
                        },
                        "recommended_action_counts": {
                            "review_possible_rename_or_delisting_in_source_scope": 2,
                            "review_duplicate_or_cross_listing_state_in_source_scope": 2,
                            "ignore_or_map_exchange_scope_before_applying": 2,
                            "already_reflected_or_new_symbol_added_in_source_scope": 2,
                        },
                        "time_sensitive_review_rows": 2,
                        "time_sensitive_workflow_queue_counts": {"review_verified_rename_or_delisting": 2},
                        "time_sensitive_recency_counts": {"recent_7d": 1, "recent_30d": 1},
                        "time_sensitive_apply_readiness_counts": {
                            "blocked_until_listing_keyed_official_symbol_change_evidence": 2,
                        },
                        "top_time_sensitive_symbol_change_batches": [
                            {
                                "symbol_change_workflow_queue": "review_verified_rename_or_delisting",
                                "recency_bucket": "recent_7d",
                                "exchange_scope_status": "matches_within_source_scope",
                                "match_status": "old_symbol_present_new_symbol_missing",
                                "listing_key_review_status": "old_scoped_listing_key_only",
                                "rows": 1,
                                "review_strategy": "verify_rename_or_delisting_with_official_venue_or_issuer_evidence",
                                "evidence_required": "official_exchange_notice_or_current_directory_showing_old_symbol_inactive_new_symbol_active_same_issuer",
                                "recommended_next_source": (
                                    "Official exchange notice, issuer notice, or current exchange directory proving "
                                    "old/new symbols for the same issuer."
                                ),
                                "source_gate": (
                                    "Do not rename until official listing-keyed evidence proves old inactive and new "
                                    "active for the same issuer."
                                ),
                            },
                            {
                                "symbol_change_workflow_queue": "review_verified_rename_or_delisting",
                                "recency_bucket": "recent_30d",
                                "exchange_scope_status": "matches_within_source_scope",
                                "match_status": "old_symbol_present_new_symbol_missing",
                                "listing_key_review_status": "old_scoped_listing_key_only",
                                "rows": 1,
                                "review_strategy": "verify_rename_or_delisting_with_official_venue_or_issuer_evidence",
                                "evidence_required": "official_exchange_notice_or_current_directory_showing_old_symbol_inactive_new_symbol_active_same_issuer",
                                "recommended_next_source": (
                                    "Official exchange notice, issuer notice, or current exchange directory proving "
                                    "old/new symbols for the same issuer."
                                ),
                                "source_gate": (
                                    "Do not rename until official listing-keyed evidence proves old inactive and new "
                                    "active for the same issuer."
                                ),
                            },
                        ],
                        "exchange_scope_status_counts": {"matches_within_source_scope": 6, "global_symbol_collision_outside_source_scope": 2},
                    },
                    "delta_evidence": {
                        "known_deltas": {
                            "source_scope_outside_collision_rows": 2,
                            "verified_rename_delta": 0,
                            "duplicate_resolution_delta": 0,
                            "warn_quarantine_delta": {"warn_delta": 0, "quarantine_delta": 0},
                        }
                    },
                },
                *[
                    {"campaign_key": f"campaign_{index}", "acceptance_matrix": matrix()}
                    for index in range(1, 10)
                ],
            ],
        }
    )

    assert result["passed"] is True
    assert result["symbol_change_campaign_evidence_gaps"] == []


def test_evaluate_campaign_acceptance_matrices_fails_for_stale_symbol_change_campaign_evidence() -> None:
    base_matrix = {
        "exchange_scope": "all",
        "affected_artifact_rows": 8,
        "campaign_metric_deltas": {},
        "global_acceptance_deltas": {
            key: {"baseline": 1, "current": 1, "delta": 0}
            for key in REQUIRED_DELTA_KEYS
        },
        "exchange_scope_deltas": {
            "exchange_count": 2,
            "changed_exchange_rows": 0,
            "delta_totals": {key: 0 for key in REQUIRED_DELTA_KEYS},
        },
        "required_metrics": REQUIRED_DELTA_KEYS,
    }

    result = evaluate_campaign_acceptance_matrices(
        {
            "summary": {"campaigns": 10},
            "campaigns": [
                {
                    "campaign_key": "symbol_changes",
                    "acceptance_matrix": base_matrix,
                    "evidence": {
                        "symbol_change_review_rows": 8,
                        "match_status_counts": {"old_symbol_present_new_symbol_missing": 8},
                        "symbol_change_workflow_queue_counts": {"review_verified_rename_or_delisting": 7},
                        "review_bucket_counts": {"action_required_possible_rename_or_delisting": 8},
                        "review_priority_counts": {"P1": 8},
                        "review_bucket_priorities": {"action_required_possible_rename_or_delisting": "P1"},
                        "recency_bucket_counts": {"recent_7d": 4, "older_than_90d": 4},
                        "review_priority_recency_counts": {"P1:recent_7d": 3, "P4:older_than_90d": 4},
                        "workflow_queue_source_hint_counts": {
                            "review_verified_rename_or_delisting": {"US_LISTED": 7}
                        },
                        "workflow_queue_source_confidence_counts": {
                            "review_verified_rename_or_delisting": {"secondary_review": 7}
                        },
                        "apply_eligibility_counts": {"auto_rename": 1, "requires_official_venue_confirmation": 6},
                        "verification_evidence_required_counts": {"official_exchange_notice": 8},
                        "recommended_action_counts": {"review_possible_rename_or_delisting_in_source_scope": 7},
                        "time_sensitive_review_rows": 1,
                        "time_sensitive_workflow_queue_counts": {"review_verified_rename_or_delisting": 2},
                        "time_sensitive_recency_counts": {"older_than_90d": 1},
                        "exchange_scope_status_counts": {"global_symbol_collision_outside_source_scope": 3, "matches_within_source_scope": 5},
                    },
                    "delta_evidence": {"known_deltas": {"source_scope_outside_collision_rows": 2}},
                },
                *[
                    {"campaign_key": f"campaign_{index}", "acceptance_matrix": base_matrix}
                    for index in range(1, 10)
                ],
            ],
        }
    )

    assert result["passed"] is False
    gaps = result["symbol_change_campaign_evidence_gaps"]
    assert {
        "field": "recommended_action_counts",
        "reported_total": 7,
        "symbol_change_review_rows": 8,
    } in gaps
    assert {
        "field": "review_priority_recency_counts",
        "priority_totals": {"P1": 3, "P4": 4},
        "review_priority_counts": {"P1": 8},
    } in gaps
    assert {
        "field": "review_bucket_counts",
        "missing_distinctions": ["already_reflected", "duplicate_or_cross_listing", "no_match_or_scope_gap"],
    } in gaps
    assert {
        "field": "apply_eligibility_counts",
        "forbidden_keys": ["auto_rename"],
    } in gaps
    assert {
        "field": "apply_eligibility_counts.requires_official_venue_confirmation",
        "reported": 6,
        "p1_rows": 8,
    } in gaps
    assert {
        "field": "time_sensitive_workflow_queue_counts",
        "reported_total": 2,
        "time_sensitive_review_rows": 1,
    } in gaps
    assert {
        "field": "time_sensitive_recency_counts",
        "invalid_recency_buckets": ["older_than_90d"],
    } in gaps
    assert {
        "field": "delta_evidence.source_scope_outside_collision_rows",
        "reported": 2,
        "expected": 3,
    } in gaps
    assert {
        "field": "delta_evidence.known_deltas",
        "missing_keys": ["verified_rename_delta", "duplicate_resolution_delta", "warn_quarantine_delta"],
    } in gaps


def test_evaluate_campaign_acceptance_matrices_requires_ohlcv_campaign_evidence() -> None:
    def matrix() -> dict[str, object]:
        return {
            "exchange_scope": "all",
            "affected_artifact_rows": 6,
            "campaign_metric_deltas": {},
            "global_acceptance_deltas": {
                key: {"baseline": 1, "current": 1, "delta": 0}
                for key in REQUIRED_DELTA_KEYS
            },
            "exchange_scope_deltas": {
                "exchange_count": 3,
                "changed_exchange_rows": 0,
                "delta_totals": {key: 0 for key in REQUIRED_DELTA_KEYS},
            },
            "required_metrics": REQUIRED_DELTA_KEYS,
        }

    result = evaluate_campaign_acceptance_matrices(
        {
            "summary": {"campaigns": 10},
            "campaigns": [
                {
                    "campaign_key": "ohlcv",
                    "acceptance_matrix": matrix(),
                    "evidence": {
                        "ohlcv_rows": 6,
                        "selected_sample_rows": 6,
                        "checked_sample_rows": 0,
                        "not_checked_sample_rows": 6,
                        "sampling_coverage": {
                            "selected_rows": 6,
                            "report_rows": 6,
                            "checked_rows": 0,
                            "not_checked_rows": 6,
                            "skipped_not_checked_rows": 0,
                            "local_sample_rows": 0,
                            "yahoo_sample_rows": 0,
                            "warn_or_source_gap_signal_rows": 0,
                        },
                        "ohlcv_sampling_backlog": {
                            "status": "sampling_queue_enabled_plausibility_only",
                            "selected_rows": 6,
                            "report_rows": 6,
                            "checked_rows": 0,
                            "not_checked_rows": 6,
                            "source_gap_cluster_sample_rows": 2,
                            "entry_quality_warn_sample_rows": 2,
                            "large_exchange_baseline_sample_rows": 2,
                            "warn_or_source_gap_signal_rows": 0,
                            "direct_canonical_data_change_allowed_rows": 0,
                            "plausibility_signal_only": True,
                            "source_gate": (
                                "OHLCV sampling is plausibility evidence only; identifiers, sectors, categories, names, "
                                "listings, and symbols remain blocked until official listing-keyed review evidence is available."
                            ),
                        },
                        "status_counts": {"not_checked": 6},
                        "issue_counts": {"no_ohlcv_sample": 6},
                        "selection_bucket_counts": {
                            "entry_quality_warn": 2,
                            "source_gap:official_identifier_not_exposed_source_gap": 2,
                            "large_exchange:NYSE": 2,
                        },
                        "selection_bucket_exchange_counts": {
                            "entry_quality_warn": {"NYSE": 2},
                            "large_exchange:NYSE": {"NYSE": 2},
                            "source_gap:official_identifier_not_exposed_source_gap": {"NYSE": 2},
                        },
                        "selection_bucket_status_counts": {
                            "entry_quality_warn": {"not_checked": 2},
                            "large_exchange:NYSE": {"not_checked": 2},
                            "source_gap:official_identifier_not_exposed_source_gap": {"not_checked": 2},
                        },
                        "review_bucket_counts": {
                            "not_checked_entry_quality_warn_sample": 2,
                            "not_checked_source_gap_cluster_sample": 2,
                            "not_checked_large_exchange_baseline_sample": 2,
                        },
                        "review_bucket_selection_bucket_counts": {
                            "not_checked_entry_quality_warn_sample": {"entry_quality_warn": 2},
                            "not_checked_source_gap_cluster_sample": {
                                "source_gap:official_identifier_not_exposed_source_gap": 2
                            },
                            "not_checked_large_exchange_baseline_sample": {"large_exchange:NYSE": 2},
                        },
                        "review_bucket_exchange_counts": {
                            "not_checked_entry_quality_warn_sample": {"NYSE": 2},
                            "not_checked_source_gap_cluster_sample": {"NYSE": 2},
                            "not_checked_large_exchange_baseline_sample": {"NYSE": 2},
                        },
                        "review_bucket_sampling_strategy_counts": {
                            "not_checked_entry_quality_warn_sample": {
                                "collect_ohlcv_sample_then_existing_entry_quality_review": 2
                            },
                            "not_checked_source_gap_cluster_sample": {
                                "collect_ohlcv_sample_then_source_gap_review": 2
                            },
                            "not_checked_large_exchange_baseline_sample": {
                                "collect_ohlcv_sample_for_large_exchange_baseline": 2
                            },
                        },
                        "review_bucket_sampling_readiness_counts": {
                            "not_checked_entry_quality_warn_sample": {"needs_ohlcv_sample": 2},
                            "not_checked_source_gap_cluster_sample": {"needs_ohlcv_sample": 2},
                            "not_checked_large_exchange_baseline_sample": {"needs_ohlcv_sample": 2},
                        },
                        "top_ohlcv_sampling_batches": [
                            {
                                "review_bucket": "not_checked_entry_quality_warn_sample",
                                "selection_bucket": "entry_quality_warn",
                                "exchange": "NYSE",
                                "plausibility_status": "not_checked",
                                "rows": 2,
                                "review_priority": "P2",
                                "sampling_strategy": "collect_ohlcv_sample_then_existing_entry_quality_review",
                                "evidence_required": "local_or_bounded_network_ohlcv_sample_then_existing_entry_quality_review",
                                "recommended_next_source": (
                                    "Collect a local or bounded-network OHLCV sample for NYSE, then review the existing "
                                    "entry-quality warning."
                                ),
                                "source_gate": (
                                    "Sampling can prioritize review, but entry-quality changes still require the existing "
                                    "official evidence gates."
                                ),
                            }
                        ],
                        "review_priority_counts": {"P2": 2, "P3": 2, "P4": 2},
                        "plausibility_use_counts": {
                            "sampling_queue_for_existing_entry_quality_warn": 2,
                            "sampling_queue_for_existing_source_gap": 2,
                            "baseline_market_data_sampling_queue": 2,
                        },
                        "canonical_data_change_authorization_counts": {
                            "no_canonical_data_change_authorized": 6,
                        },
                        "verification_evidence_required_counts": {
                            "local_or_bounded_network_ohlcv_sample_then_existing_entry_quality_review": 2,
                            "local_or_bounded_network_ohlcv_sample_then_source_gap_review": 2,
                            "local_or_bounded_network_ohlcv_sample_for_baseline_only": 2,
                        },
                        "source_gap_class_counts": {"official_identifier_not_exposed_source_gap": 2},
                        "top_flagged_exchanges": [{"exchange": "NYSE", "not_checked": 2}],
                    },
                    "delta_evidence": {
                        "known_deltas": {
                            "selected_sample_rows": 6,
                            "checked_sample_rows": 0,
                            "warn_or_source_gap_signal_rows": 0,
                        }
                    },
                },
                *[
                    {"campaign_key": f"campaign_{index}", "acceptance_matrix": matrix()}
                    for index in range(1, 10)
                ],
            ],
        }
    )

    assert result["passed"] is True
    assert result["ohlcv_campaign_evidence_gaps"] == []


def test_evaluate_campaign_acceptance_matrices_fails_for_stale_ohlcv_campaign_evidence() -> None:
    base_matrix = {
        "exchange_scope": "all",
        "affected_artifact_rows": 6,
        "campaign_metric_deltas": {},
        "global_acceptance_deltas": {
            key: {"baseline": 1, "current": 1, "delta": 0}
            for key in REQUIRED_DELTA_KEYS
        },
        "exchange_scope_deltas": {
            "exchange_count": 3,
            "changed_exchange_rows": 0,
            "delta_totals": {key: 0 for key in REQUIRED_DELTA_KEYS},
        },
        "required_metrics": REQUIRED_DELTA_KEYS,
    }

    result = evaluate_campaign_acceptance_matrices(
        {
            "summary": {"campaigns": 10},
            "campaigns": [
                {
                    "campaign_key": "ohlcv",
                    "acceptance_matrix": base_matrix,
                    "evidence": {
                        "ohlcv_rows": 6,
                        "selected_sample_rows": 6,
                        "checked_sample_rows": 2,
                        "not_checked_sample_rows": 5,
                        "sampling_coverage": {
                            "selected_rows": 5,
                            "report_rows": 6,
                            "checked_rows": 2,
                            "not_checked_rows": 5,
                        },
                        "status_counts": {"not_checked": 6},
                        "issue_counts": {"no_ohlcv_sample": 5},
                        "selection_bucket_counts": {"entry_quality_warn": 3, "source_gap:official_identifier_not_exposed_source_gap": 3},
                        "selection_bucket_status_counts": {
                            "entry_quality_warn": {"not_checked": 2},
                            "source_gap:official_identifier_not_exposed_source_gap": {"not_checked": 3},
                        },
                        "review_bucket_counts": {"not_checked_entry_quality_warn_sample": 6},
                        "review_priority_counts": {"P2": 6},
                        "plausibility_use_counts": {"fill_isin_from_ohlcv": 1, "sampling_queue_for_existing_entry_quality_warn": 5},
                        "canonical_data_change_authorization_counts": {"apply_from_ohlcv": 1, "no_canonical_data_change_authorized": 5},
                        "verification_evidence_required_counts": {"none_no_database_change_authorized": 1, "local_or_bounded_network_ohlcv_sample_then_existing_entry_quality_review": 5},
                        "source_gap_class_counts": {"official_identifier_not_exposed_source_gap": 2},
                        "top_flagged_exchanges": [{"exchange": "", "not_checked": -1}],
                    },
                    "delta_evidence": {"known_deltas": {"selected_sample_rows": 5}},
                },
                *[
                    {"campaign_key": f"campaign_{index}", "acceptance_matrix": base_matrix}
                    for index in range(1, 10)
                ],
            ],
        }
    )

    assert result["passed"] is False
    gaps = result["ohlcv_campaign_evidence_gaps"]
    assert {
        "field": "selection_bucket_counts",
        "missing_sampling_buckets": ["large_exchange:"],
    } in gaps
    assert {
        "field": "source_gap_class_counts",
        "reported_total": 2,
        "source_gap_selection_rows": 3,
    } in gaps
    assert {
        "field": "plausibility_use_counts",
        "forbidden_keys": ["fill_isin_from_ohlcv"],
    } in gaps
    assert {
        "field": "verification_evidence_required_counts",
        "forbidden_keys": ["none_no_database_change_authorized"],
    } in gaps
    assert {"field": "top_flagged_exchanges", "reason": "expected_ranked_exchange_rows"} in gaps
    assert {
        "field": "delta_evidence.selected_sample_rows",
        "reported": 5,
        "expected": 6,
    } in gaps
    assert {
        "field": "delta_evidence.known_deltas",
        "missing_keys": ["checked_sample_rows", "warn_or_source_gap_signal_rows"],
    } in gaps


def test_evaluate_campaign_acceptance_matrices_requires_freshness_campaign_evidence() -> None:
    def matrix() -> dict[str, object]:
        return {
            "exchange_scope": "all",
            "affected_artifact_rows": 4,
            "campaign_metric_deltas": {},
            "global_acceptance_deltas": {
                key: {"baseline": 1, "current": 1, "delta": 0}
                for key in REQUIRED_DELTA_KEYS
            },
            "exchange_scope_deltas": {
                "exchange_count": 2,
                "changed_exchange_rows": 0,
                "delta_totals": {key: 0 for key in REQUIRED_DELTA_KEYS},
            },
            "required_metrics": REQUIRED_DELTA_KEYS,
        }

    freshness_snapshot = [
        {
            "key": key,
            "source_type": source_type,
            "generated_at": "2026-05-24T00:00:00Z",
            "age_hours": 24.0,
            "age_bucket": "age_0_48h",
            "rows": 1,
            "recommended_next_source": f"Fresh generated_at and row-count evidence for {key}.",
            "source_gate": (
                "Freshness is visibility evidence only; it does not authorize canonical data changes without listing-keyed source review."
            ),
        }
        for key, source_type in (
            ("entry_quality", "quality_gate"),
            ("identifiers", "dataset"),
            ("masterfiles", "source_inventory"),
            ("ohlcv_plausibility", "sampling_report"),
            ("source_gap_classification", "review_report"),
            ("symbol_changes", "workflow_report"),
            ("tickers", "dataset"),
        )
    ]

    evidence = {
        "source_gap_rows": 4,
        "coverage_freshness_keys": 9,
        "freshness_snapshot": freshness_snapshot,
        "freshness_snapshot_age_bucket_totals": {"age_0_48h": 7},
        "top_freshness_snapshot_ages": freshness_snapshot[:3],
        "source_freshness_status_totals": {"fresh": 1, "old": 3},
        "source_age_bucket_totals": {"age_0_48h": 1, "age_168_336h": 3},
        "source_refresh_priority_totals": {"P1": 2, "P2": 1, "P4": 1},
        "source_refresh_queue_totals": {
            "fresh_no_refresh_needed": 1,
            "refresh_official_exchange_directory_before_identity_or_collision_work": 2,
            "refresh_official_subset_before_gap_enrichment": 1,
        },
        "source_refresh_queue_scope_totals": {
            "fresh_no_refresh_needed": {"exchange_directory": 1},
            "refresh_official_exchange_directory_before_identity_or_collision_work": {"exchange_directory": 2},
            "refresh_official_subset_before_gap_enrichment": {"listed_companies_subset": 1},
        },
        "source_refresh_queue_mode_totals": {
            "fresh_no_refresh_needed": {"network": 1},
            "refresh_official_exchange_directory_before_identity_or_collision_work": {"cache": 2},
            "refresh_official_subset_before_gap_enrichment": {"cache": 1},
        },
        "source_refresh_queue_priority_totals": {
            "fresh_no_refresh_needed": {"P4": 1},
            "refresh_official_exchange_directory_before_identity_or_collision_work": {"P1": 2},
            "refresh_official_subset_before_gap_enrichment": {"P2": 1},
        },
        "source_refresh_queue_age_bucket_totals": {
            "fresh_no_refresh_needed": {"age_0_48h": 1},
            "refresh_official_exchange_directory_before_identity_or_collision_work": {"age_168_336h": 2},
            "refresh_official_subset_before_gap_enrichment": {"age_168_336h": 1},
        },
        "source_refresh_action_totals": {
            "no_refresh_needed": 1,
            "refresh_official_exchange_directory_before_identity_or_collision_work": 2,
            "refresh_official_subset_before_gap_enrichment": 1,
        },
        "source_refresh_queue_review_strategy_totals": {
            "fresh_no_refresh_needed": {"no_refresh_required": 1},
            "refresh_official_exchange_directory_before_identity_or_collision_work": {
                "refresh_official_exchange_directory_before_identity_or_collision_work": 2
            },
            "refresh_official_subset_before_gap_enrichment": {
                "refresh_official_subset_before_gap_enrichment": 1
            },
        },
        "source_refresh_queue_evidence_required_totals": {
            "fresh_no_refresh_needed": {"fresh_source_generated_at_with_age_under_48h": 1},
            "refresh_official_exchange_directory_before_identity_or_collision_work": {
                "official_exchange_directory_refresh_artifact_with_generated_at_and_row_count": 2
            },
            "refresh_official_subset_before_gap_enrichment": {
                "official_subset_refresh_artifact_with_generated_at_scope_and_row_count": 1
            },
        },
        "top_source_refresh_batches": [
            {
                "refresh_queue": "refresh_official_exchange_directory_before_identity_or_collision_work",
                "reference_scope": "exchange_directory",
                "mode": "cache",
                "refresh_priority": "P1",
                "source_count": 2,
                "total_rows": 100,
                "max_age_hours": 200.0,
                "review_strategy": "refresh_official_exchange_directory_before_identity_or_collision_work",
                "evidence_required": "official_exchange_directory_refresh_artifact_with_generated_at_and_row_count",
                "recommended_next_source": (
                    "Refresh the official exchange-directory source for scope exchange_directory using mode cache."
                ),
                "source_gate": (
                    "Do not perform identity, collision, or listing-add work until the official exchange directory is freshly regenerated."
                ),
            }
        ],
        "old_official_exchange_directory_count": 2,
        "top_old_official_exchange_directories": [{"key": "old", "provider": "Official", "age_hours": 200.0}],
        "source_gap_class_totals": {"official_identifier_not_exposed_source_gap": 2, "official_industry_taxonomy_unavailable_gap": 2},
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
        "financialdata_supplement_rows": 4,
        "financialdata_apply_eligibility_counts": {
            "blocked_until_unique_official_isin_candidate_resolved": 2,
            "keep_absent_until_name_gated_official_isin_match": 2,
        },
        "financialdata_verification_evidence_required_counts": {
            "single_official_active_listing_with_valid_isin_and_name_gate": 2,
            "official_active_masterfile_or_registry_row_matching_financialdata_name_and_listing": 2,
        },
        "top_financialdata_supplement_review_batches": [
            {
                "review_priority": "P2",
                "financialdata_review_queue": "resolve_ambiguous_official_isin_candidates",
                "decision": "reject",
                "reason": "ambiguous_official_isin_candidates",
                "financialdata_exchange": "NSE_IN",
                "financialdata_review_scope": "global_expansion_candidate",
                "official_source_key": "nse_india_securities_available",
                "review_strategy": "resolve_to_single_official_active_isin_candidate_before_apply",
                "apply_eligibility": "blocked_until_unique_official_isin_candidate_resolved",
                "verification_evidence_required": "single_official_active_listing_with_valid_isin_and_name_gate",
                "rows": 2,
                "recommended_next_source": (
                    "Single official active listing or registry row resolving the ambiguous candidates with exact name and ISIN."
                ),
                "source_gate": (
                    "Do not write supplement until exactly one official valid-ISIN candidate remains after name/listing review."
                ),
            }
        ],
    }

    result = evaluate_campaign_acceptance_matrices(
        {
            "summary": {"campaigns": 10},
            "campaigns": [
                {
                    "campaign_key": "freshness",
                    "acceptance_matrix": matrix(),
                    "evidence": evidence,
                    "delta_evidence": {
                        "known_deltas": {
                            "coverage_freshness_keys": 9,
                            "freshness_snapshot": freshness_snapshot,
                            "freshness_snapshot_age_bucket_totals": {"age_0_48h": 7},
                            "top_freshness_snapshot_ages": freshness_snapshot[:3],
                            "source_freshness_status_totals": {"fresh": 1, "old": 3},
                            "source_age_bucket_totals": {"age_0_48h": 1, "age_168_336h": 3},
                            "source_refresh_priority_totals": {"P1": 2, "P2": 1, "P4": 1},
                            "source_refresh_queue_totals": {
                                "fresh_no_refresh_needed": 1,
                                "refresh_official_exchange_directory_before_identity_or_collision_work": 2,
                                "refresh_official_subset_before_gap_enrichment": 1,
                            },
                            "source_refresh_queue_scope_totals": {
                                "fresh_no_refresh_needed": {"exchange_directory": 1},
                                "refresh_official_exchange_directory_before_identity_or_collision_work": {
                                    "exchange_directory": 2
                                },
                                "refresh_official_subset_before_gap_enrichment": {"listed_companies_subset": 1},
                            },
                            "source_refresh_queue_mode_totals": {
                                "fresh_no_refresh_needed": {"network": 1},
                                "refresh_official_exchange_directory_before_identity_or_collision_work": {"cache": 2},
                                "refresh_official_subset_before_gap_enrichment": {"cache": 1},
                            },
                            "source_refresh_queue_priority_totals": {
                                "fresh_no_refresh_needed": {"P4": 1},
                                "refresh_official_exchange_directory_before_identity_or_collision_work": {"P1": 2},
                                "refresh_official_subset_before_gap_enrichment": {"P2": 1},
                            },
                            "source_refresh_queue_age_bucket_totals": {
                                "fresh_no_refresh_needed": {"age_0_48h": 1},
                                "refresh_official_exchange_directory_before_identity_or_collision_work": {
                                    "age_168_336h": 2
                                },
                                "refresh_official_subset_before_gap_enrichment": {"age_168_336h": 1},
                            },
                            "source_refresh_queue_review_strategy_totals": {
                                "fresh_no_refresh_needed": {"no_refresh_required": 1},
                                "refresh_official_exchange_directory_before_identity_or_collision_work": {
                                    "refresh_official_exchange_directory_before_identity_or_collision_work": 2
                                },
                                "refresh_official_subset_before_gap_enrichment": {
                                    "refresh_official_subset_before_gap_enrichment": 1
                                },
                            },
                            "source_refresh_queue_evidence_required_totals": {
                                "fresh_no_refresh_needed": {"fresh_source_generated_at_with_age_under_48h": 1},
                                "refresh_official_exchange_directory_before_identity_or_collision_work": {
                                    "official_exchange_directory_refresh_artifact_with_generated_at_and_row_count": 2
                                },
                                "refresh_official_subset_before_gap_enrichment": {
                                    "official_subset_refresh_artifact_with_generated_at_scope_and_row_count": 1
                                },
                            },
                            "source_refresh_action_totals": {
                                "no_refresh_needed": 1,
                                "refresh_official_exchange_directory_before_identity_or_collision_work": 2,
                                "refresh_official_subset_before_gap_enrichment": 1,
                            },
                            "old_official_exchange_directory_count": 2,
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
                            "financialdata_supplement_rows": 4,
                            "top_financialdata_supplement_review_batches": [
                                {
                                    "review_priority": "P2",
                                    "financialdata_review_queue": "resolve_ambiguous_official_isin_candidates",
                                    "decision": "reject",
                                    "reason": "ambiguous_official_isin_candidates",
                                    "financialdata_exchange": "NSE_IN",
                                    "financialdata_review_scope": "global_expansion_candidate",
                                    "official_source_key": "nse_india_securities_available",
                                    "review_strategy": "resolve_to_single_official_active_isin_candidate_before_apply",
                                    "apply_eligibility": "blocked_until_unique_official_isin_candidate_resolved",
                                    "verification_evidence_required": (
                                        "single_official_active_listing_with_valid_isin_and_name_gate"
                                    ),
                                    "rows": 2,
                                    "recommended_next_source": (
                                        "Single official active listing or registry row resolving the ambiguous candidates with exact name and ISIN."
                                    ),
                                    "source_gate": (
                                        "Do not write supplement until exactly one official valid-ISIN candidate remains after name/listing review."
                                    ),
                                }
                            ],
                        }
                    },
                },
                *[
                    {"campaign_key": f"campaign_{index}", "acceptance_matrix": matrix()}
                    for index in range(1, 10)
                ],
            ],
        }
    )

    assert result["passed"] is True
    assert result["freshness_campaign_evidence_gaps"] == []


def test_evaluate_campaign_acceptance_matrices_fails_for_stale_freshness_campaign_evidence() -> None:
    base_matrix = {
        "exchange_scope": "all",
        "affected_artifact_rows": 4,
        "campaign_metric_deltas": {},
        "global_acceptance_deltas": {
            key: {"baseline": 1, "current": 1, "delta": 0}
            for key in REQUIRED_DELTA_KEYS
        },
        "exchange_scope_deltas": {
            "exchange_count": 2,
            "changed_exchange_rows": 0,
            "delta_totals": {key: 0 for key in REQUIRED_DELTA_KEYS},
        },
        "required_metrics": REQUIRED_DELTA_KEYS,
    }

    result = evaluate_campaign_acceptance_matrices(
        {
            "summary": {"campaigns": 10},
            "campaigns": [
                {
                    "campaign_key": "freshness",
                    "acceptance_matrix": base_matrix,
                    "evidence": {
                        "source_gap_rows": 4,
                        "coverage_freshness_keys": 9,
                        "source_freshness_status_totals": {"fresh": 1, "old": 2},
                        "source_age_bucket_totals": {"age_0_48h": 1},
                        "source_refresh_priority_totals": {"P1": 2},
                        "source_refresh_queue_totals": {"fresh_no_refresh_needed": 1},
                        "source_refresh_queue_age_bucket_totals": {"fresh_no_refresh_needed": {"age_0_48h": 1}},
                        "source_refresh_action_totals": {"no_refresh_needed": 2},
                        "source_refresh_queue_review_strategy_totals": {"fresh_no_refresh_needed": {"bad_strategy": 1}},
                        "source_refresh_queue_evidence_required_totals": {"fresh_no_refresh_needed": {"bad_evidence": 1}},
                        "top_source_refresh_batches": [
                            {
                                "refresh_queue": "",
                                "reference_scope": "",
                                "mode": "",
                                "refresh_priority": "P5",
                                "source_count": 0,
                                "total_rows": -1,
                                "max_age_hours": -1,
                                "review_strategy": "bad_strategy",
                                "evidence_required": "",
                            }
                        ],
                        "old_official_exchange_directory_count": 3,
                        "top_old_official_exchange_directories": [{"key": "", "provider": "", "age_hours": -1}],
                        "source_gap_class_totals": {"official_identifier_not_exposed_source_gap": 3},
                        "top_source_gap_review_batches": [
                            {
                                "field": "bad_field",
                                "gap_class": "",
                                "exchange": "",
                                "rows": 0,
                                "recommended_next_source": "",
                                "source_gate": "",
                            }
                        ],
                        "financialdata_supplement_rows": 4,
                        "financialdata_apply_eligibility_counts": {"apply_from_global_ticker_reuse": 1, "blocked_until_unique_official_isin_candidate_resolved": 2},
                        "financialdata_verification_evidence_required_counts": {
                            "single_official_active_listing_with_valid_isin_and_name_gate": 3,
                        },
                        "top_financialdata_supplement_review_batches": [
                            {
                                "review_priority": "P5",
                                "financialdata_review_queue": "",
                                "decision": "reject",
                                "reason": "",
                                "financialdata_exchange": "",
                                "financialdata_review_scope": "",
                                "official_source_key": "",
                                "review_strategy": "",
                                "apply_eligibility": "apply_from_global_ticker_reuse",
                                "verification_evidence_required": "ticker_match_only",
                                "rows": 0,
                                "recommended_next_source": "",
                                "source_gate": "",
                            }
                        ],
                    },
                    "delta_evidence": {
                        "known_deltas": {
                            "coverage_freshness_keys": 8,
                            "source_freshness_status_totals": {"fresh": 1, "old": 2},
                            "source_age_bucket_totals": {"age_0_48h": 1},
                            "source_refresh_priority_totals": {"P1": 2},
                            "source_refresh_queue_totals": {"fresh_no_refresh_needed": 1},
                            "source_refresh_queue_age_bucket_totals": {"fresh_no_refresh_needed": {"age_0_48h": 1}},
                            "source_refresh_queue_review_strategy_totals": {"fresh_no_refresh_needed": {"bad_strategy": 1}},
                            "source_refresh_queue_evidence_required_totals": {"fresh_no_refresh_needed": {"bad_evidence": 1}},
                            "old_official_exchange_directory_count": 3,
                            "top_source_gap_review_batches": [],
                            "financialdata_supplement_rows": 3,
                            "top_financialdata_supplement_review_batches": [],
                        }
                    },
                },
                *[
                    {"campaign_key": f"campaign_{index}", "acceptance_matrix": base_matrix}
                    for index in range(1, 10)
                ],
            ],
        }
    )

    assert result["passed"] is False
    gaps = result["freshness_campaign_evidence_gaps"]
    assert {
        "field": "source_refresh_priority_totals",
        "reported_total": 2,
        "source_freshness_rows": 3,
    } in gaps
    assert {
        "field": "old_official_exchange_directory_count",
        "reported": 3,
        "old_or_stale_sources": 2,
    } in gaps
    assert {"field": "top_old_official_exchange_directories", "reason": "expected_ranked_source_rows"} in gaps
    assert {"field": "top_source_refresh_batches", "reason": "expected_ranked_refresh_strategy_rows"} in gaps
    assert {
        "field": "source_gap_class_totals",
        "reported_total": 3,
        "source_gap_rows": 4,
    } in gaps
    assert {"field": "top_source_gap_review_batches", "reason": "expected_ranked_source_gap_review_batches"} in gaps
    assert {
        "field": "financialdata_apply_eligibility_counts",
        "forbidden_keys": ["apply_from_global_ticker_reuse"],
    } in gaps
    assert {
        "field": "top_financialdata_supplement_review_batches",
        "reason": "expected_ranked_source_gated_financialdata_review_batches",
    } in gaps
    assert {
        "field": "evidence",
            "missing_keys": [
                "freshness_snapshot",
                "freshness_snapshot_age_bucket_totals",
                "top_freshness_snapshot_ages",
                "source_refresh_queue_scope_totals",
                "source_refresh_queue_mode_totals",
                "source_refresh_queue_priority_totals",
        ],
    } in gaps


def test_evaluate_campaign_acceptance_matrices_requires_baseline_campaign_evidence() -> None:
    def matrix() -> dict[str, object]:
        return {
            "exchange_scope": "all",
            "affected_artifact_rows": 2,
            "campaign_metric_deltas": {},
            "global_acceptance_deltas": {
                key: {"baseline": 1, "current": 1, "delta": 0}
                for key in REQUIRED_DELTA_KEYS
            },
            "exchange_scope_deltas": {
                "exchange_count": 80,
                "changed_exchange_rows": 0,
                "delta_totals": {key: 0 for key in REQUIRED_DELTA_KEYS},
            },
            "required_metrics": REQUIRED_DELTA_KEYS,
        }

    result = evaluate_campaign_acceptance_matrices(
        {
            "summary": {"campaigns": 10},
            "campaigns": [
                {
                    "campaign_key": "baseline",
                    "acceptance_matrix": matrix(),
                    "evidence": {
                        "baseline_global_metrics": 16,
                        "baseline_campaigns": 10,
                        "changed_numeric_delta_rows": 0,
                    },
                    "delta_evidence": {
                        "known_deltas": {
                            "baseline_generated_at": "2026-05-24T18:51:10Z",
                            "future_delta_reference": "data/reports/improvement_baseline.json",
                            "current_delta_report": "data/reports/improvement_deltas.json",
                        }
                    },
                },
                *[
                    {"campaign_key": f"campaign_{index}", "acceptance_matrix": matrix()}
                    for index in range(1, 10)
                ],
            ],
        }
    )

    assert result["passed"] is True
    assert result["baseline_campaign_evidence_gaps"] == []


def test_evaluate_campaign_acceptance_matrices_fails_for_stale_baseline_campaign_evidence() -> None:
    base_matrix = {
        "exchange_scope": "all",
        "affected_artifact_rows": 2,
        "campaign_metric_deltas": {},
        "global_acceptance_deltas": {
            key: {"baseline": 1, "current": 1, "delta": 0}
            for key in REQUIRED_DELTA_KEYS
        },
        "exchange_scope_deltas": {
            "exchange_count": 80,
            "changed_exchange_rows": 0,
            "delta_totals": {key: 0 for key in REQUIRED_DELTA_KEYS},
        },
        "required_metrics": REQUIRED_DELTA_KEYS,
    }

    result = evaluate_campaign_acceptance_matrices(
        {
            "summary": {"campaigns": 10},
            "campaigns": [
                {
                    "campaign_key": "baseline",
                    "acceptance_matrix": base_matrix,
                    "evidence": {
                        "baseline_global_metrics": 15,
                        "baseline_campaigns": 9,
                        "changed_numeric_delta_rows": 1,
                    },
                    "delta_evidence": {
                        "known_deltas": {
                            "baseline_generated_at": "not-a-date",
                            "future_delta_reference": "wrong.json",
                            "current_delta_report": "data/reports/improvement_deltas.json",
                        }
                    },
                },
                *[
                    {"campaign_key": f"campaign_{index}", "acceptance_matrix": base_matrix}
                    for index in range(1, 10)
                ],
            ],
        }
    )

    assert result["passed"] is False
    gaps = result["baseline_campaign_evidence_gaps"]
    assert {"field": "baseline_global_metrics", "reported": 15, "expected": 16} in gaps
    assert {"field": "baseline_campaigns", "reported": 9, "expected": 10} in gaps
    assert {"field": "changed_numeric_delta_rows", "reported": 1, "expected": 0} in gaps
    assert {"field": "delta_evidence.baseline_generated_at", "reported": "not-a-date"} in gaps
    assert {
        "field": "delta_evidence.future_delta_reference",
        "reported": "wrong.json",
        "expected": "data/reports/improvement_baseline.json",
    } in gaps


def test_evaluate_campaign_reviewability_requires_expected_priority_keys() -> None:
    keys = [
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
    campaign_rows = []
    for index, key in enumerate(keys, start=1):
        campaign = {
            "priority": index,
            "campaign_key": key,
            "status": "partial",
            "next_action": "review",
            "artifacts": [{"path": f"data/reports/{key}.json", "rows": index}],
            "acceptance_matrix": {"exchange_scope": "all", "affected_artifact_rows": index},
            "delta_evidence": {"status": "partial", "missing_deltas": []},
            "before_after_summary": {"exchange_scope": "all"},
        }
        campaign["campaign_context"] = campaign_context(campaign)
        campaign["artifact_context"] = campaign_artifact_context(campaign)
        campaign["delta_review_context"] = campaign_delta_review_context(campaign)
        campaign["closure_context"] = campaign_closure_context(campaign)
        campaign_rows.append(campaign)
    summary = {
        "campaigns": 10,
        "complete_campaigns": 0,
        "next_review_batches": 10,
        "next_review_batch_rows": 55,
        "closure_ready_campaigns": 9,
        "closure_blocked_campaigns": 1,
        "closure_blockers": 1,
        "validation_failed_error_gates": 0,
    }

    result = evaluate_campaign_reviewability(
        {
            "summary": summary,
            "summary_context": campaign_summary_context(summary),
            "campaigns": campaign_rows,
        }
    )

    assert result["passed"] is True
    assert result["summary_context_missing"] is False
    assert result["summary_context_mismatch"] == {}
    assert result["observed_campaign_keys_by_priority"][1] == "b3"
    assert result["observed_campaign_keys_by_priority"][10] == "baseline"


def test_campaign_contexts_are_exact_field_summaries() -> None:
    campaign = {
        "priority": 1,
        "campaign_key": "b3",
        "status": "partial",
        "next_action": "review B3",
        "artifacts": [
            {"path": "data/reports/a.json", "rows": 2},
            {"path": "data/reports/b.json", "rows": 5},
        ],
        "acceptance_matrix": {"exchange_scope": ["B3"], "affected_artifact_rows": 7},
        "delta_evidence": {"status": "partial", "missing_deltas": ["source_gap_delta"]},
        "before_after_summary": {"exchange_scope": ["B3"]},
    }

    assert campaign_context(campaign) == "priority=1;campaign_key=b3;status=partial;exchange_scope=B3"
    assert (
        campaign_artifact_context(campaign)
        == "artifact_count=2;affected_artifact_rows=7;primary_artifact=data/reports/b.json;primary_artifact_rows=5"
    )
    assert (
        campaign_delta_review_context(campaign)
        == "delta_status=partial;missing_delta_count=1;before_after_present=true;next_action_present=true"
    )
    assert (
        campaign_closure_context(campaign)
        == "closure_gate=missing_delta_evidence_must_be_resolved_or_documented_before_campaign_closure;artifact_rows=7;missing_delta_count=1;closure_status=blocked_until_closure_gate_resolved"
    )


def test_evaluate_campaign_reviewability_fails_for_missing_or_misordered_campaigns() -> None:
    result = evaluate_campaign_reviewability(
        {
            "summary": {"campaigns": 2},
            "summary_context": "wrong",
            "campaigns": [
                {"priority": 1, "campaign_key": "otc", "status": "partial", "next_action": "review"},
                {"priority": 1, "campaign_key": "b3", "status": "", "next_action": ""},
                {"priority": 2, "campaign_key": "b3", "status": "partial", "next_action": "review"},
            ],
        }
    )

    assert result["passed"] is False
    assert result["summary_context_mismatch"] == {
        "expected": (
            "campaigns=2;complete_campaigns=0;next_review_batches=0;next_review_batch_rows=0;"
            "closure_ready_campaigns=0;closure_blocked_campaigns=0;closure_blockers=0;"
            "validation_failed_error_gates=0"
        ),
        "actual": "wrong",
    }
    assert result["duplicate_priorities"] == [1]
    assert result["key_mismatches"] == {2: {"expected": "otc", "actual": "b3"}}
    assert result["missing_priorities"] == [3, 4, 5, 6, 7, 8, 9, 10]
    assert result["missing_next_actions"] == ["b3"]
    assert result["missing_statuses"] == ["b3"]
    assert result["missing_context_fields"] == {
        "b3": ["campaign_context", "artifact_context", "delta_review_context", "closure_context"],
        "otc": ["campaign_context", "artifact_context", "delta_review_context", "closure_context"],
    }
    assert result["invalid_context_fields"] == {
        "b3": ["campaign_context", "artifact_context", "delta_review_context", "closure_context"],
        "otc": ["campaign_context", "artifact_context", "delta_review_context", "closure_context"],
    }


def test_campaign_status_rows_extracts_priority_status_and_next_action() -> None:
    rows = campaign_status_rows(
        {
            "campaigns": [
                {
                    "priority": 2,
                    "campaign_key": "otc",
                    "status": "scoped",
                    "delta_evidence": {"status": "partial"},
                    "next_action": "review source gaps",
                },
                {
                    "priority": 1,
                    "campaign_key": "b3",
                    "status": "partial",
                    "delta_evidence": {"status": "review_only"},
                    "next_action": "refresh official source",
                },
            ],
        }
    )

    assert rows == [
        {
            "priority": 1,
            "campaign_key": "b3",
            "status": "partial",
            "delta_status": "review_only",
            "next_action": "refresh official source",
        },
        {
            "priority": 2,
            "campaign_key": "otc",
            "status": "scoped",
            "delta_status": "partial",
            "next_action": "review source gaps",
        },
    ]


def test_evaluate_next_review_batch_visibility_requires_all_campaign_batches() -> None:
    keys = [
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
    batches = [
        {
            "priority": index,
            "campaign_key": key,
            "status": "partial",
            "delta_status": "partial",
            "artifact_rows": 0 if key == "baseline" else index,
            "primary_artifact": "" if key == "baseline" else f"data/reports/{key}.json",
            "closure_gate": "review_required",
            "next_action": "review",
            "recommended_next_source": "Official listing-keyed review source.",
            "source_gate": "No data change before review gate.",
        }
        for index, key in enumerate(keys, start=1)
    ]
    for batch in batches:
        batch["closure_context"] = next_review_batch_closure_context(batch)
    workload = {
        "total_batches": len(batches),
        "total_rows": sum(row["artifact_rows"] for row in batches),
        "blocked_batches": len(batches),
        "rows_by_campaign_key": {row["campaign_key"]: row["artifact_rows"] for row in batches},
        "rows_by_closure_gate": {
            "review_required": sum(row["artifact_rows"] for row in batches)
        },
        "largest_batch": {
            "priority": 9,
            "campaign_key": "freshness",
            "artifact_rows": 9,
            "primary_artifact": "data/reports/freshness.json",
            "closure_gate": "review_required",
        },
        "policy": "Workload rows are review workload only. They do not authorize data changes or closure without processing the listing-keyed artifacts.",
    }
    workload["workload_context"] = next_review_workload_context(workload)
    execution_plan = []
    for batch in batches:
        row = {
            "execution_order": batch["priority"],
            "priority": batch["priority"],
            "campaign_key": batch["campaign_key"],
            "artifact_rows": batch["artifact_rows"],
            "primary_artifact": batch["primary_artifact"],
            "closure_gate": batch["closure_gate"],
            "evidence_command": "python scripts/build_improvement_delta_report.py",
            "command_mode": "local_report_rebuild",
            "network_required": False,
            "data_change_authorized": False,
            "recommended_next_source": batch["recommended_next_source"],
            "source_gate": batch["source_gate"],
            "next_action": batch["next_action"],
        }
        row["ranking_reason"] = execution_ranking_reason(row)
        row["ranking_context"] = execution_ranking_context(row)
        row["command_scripts"] = command_script_paths(row["evidence_command"])
        row["missing_command_scripts"] = []
        row["command_readiness_context"] = command_readiness_context(row)
        row["command_mutation_risk"] = command_mutation_risk(row["command_scripts"])
        row["risky_command_scripts"] = risky_command_scripts(row["command_scripts"])
        row["manual_review_required_before_run"] = row["command_mutation_risk"] == "review_required"
        row["command_safety_context"] = command_safety_context(row)
        row["review_required_command_context"] = review_required_command_context(row)
        row["review_required_preflight_checks"] = review_required_preflight_checks(row)
        row["review_required_preflight_context"] = review_required_preflight_context(row)
        row["execution_context"] = execution_plan_context(row)
        execution_plan.append(row)
    execution_summary = {
        "total_actions": len(execution_plan),
        "local_report_rebuild_actions": len(execution_plan),
        "network_evidence_refresh_actions": 0,
        "network_required_rows": 0,
        "local_report_rebuild_rows": sum(row["artifact_rows"] for row in execution_plan),
        "rows_by_command_mode": {
            "local_report_rebuild": sum(row["artifact_rows"] for row in execution_plan)
        },
        "network_campaign_keys": [],
        "local_campaign_keys": [row["campaign_key"] for row in execution_plan],
        "data_change_authorized_actions": 0,
        "policy": "Execution summary is planning evidence only. Network refreshes collect source evidence, and no row authorizes data changes.",
    }
    execution_summary["execution_summary_context"] = execution_summary_context(execution_summary)
    command_safety_summary = {
        "total_actions": len(execution_plan),
        "risk_counts": {"report_or_fetch_only": len(execution_plan)},
        "review_required_actions": 0,
        "report_or_fetch_only_actions": len(execution_plan),
        "manual_review_required_actions": 0,
        "review_required_campaign_keys": [],
        "review_required_command_rows": [],
        "preflight_complete_actions": 0,
        "preflight_gap_campaign_keys": [],
        "data_change_authorized_actions": 0,
        "execution_ready_without_manual_review": True,
        "execution_blocking_gate": "no_manual_command_blockers_detected",
        "execution_blocking_campaign_keys": [],
        "policy": "Command safety summary is a planning guard. Review-required commands must be inspected before execution and still do not authorize data changes.",
    }
    command_safety_summary["command_safety_summary_context"] = command_safety_summary_context(command_safety_summary)
    result = evaluate_next_review_batch_visibility(
        {
            "summary": {
                "next_review_batches": 10,
                "next_review_batch_rows": sum(row["artifact_rows"] for row in batches),
            },
            "next_review_batches": batches,
            "next_review_workload": workload,
            "next_review_execution_plan": execution_plan,
            "next_review_execution_summary": execution_summary,
            "next_review_command_safety_summary": command_safety_summary,
        }
    )

    assert result["passed"] is True
    assert result["observed_campaign_keys_by_priority"][1] == "b3"
    assert result["workload_mismatches"] == {}
    assert result["execution_plan_gaps"] == []
    assert result["execution_plan_ranking_mismatches"] == {}
    assert result["execution_plan_command_gaps"] == {}
    assert result["execution_summary_mismatches"] == {}
    assert result["command_safety_summary_mismatches"] == {}


def test_evaluate_next_review_batch_visibility_fails_for_missing_or_weak_rows() -> None:
    result = evaluate_next_review_batch_visibility(
        {
            "summary": {"next_review_batches": 2, "next_review_batch_rows": 999},
            "next_review_batches": [
                {
                    "priority": 1,
                    "campaign_key": "otc",
                    "status": "",
                    "delta_status": "partial",
                    "artifact_rows": -1,
                    "primary_artifact": "",
                    "closure_gate": "",
                    "next_action": "",
                    "recommended_next_source": "",
                    "source_gate": "",
                    "closure_context": "",
                },
                {
                    "priority": 1,
                    "campaign_key": "b3",
                    "status": "partial",
                    "delta_status": "partial",
                    "artifact_rows": 1,
                    "primary_artifact": "data/reports/b3.json",
                    "closure_gate": "review_required",
                    "next_action": "review",
                    "recommended_next_source": "Official source.",
                    "source_gate": "Exact source gate.",
                    "closure_context": "wrong",
                },
            ],
        }
    )

    assert result["passed"] is False
    assert result["duplicate_priorities"] == [1]
    assert result["key_mismatches"] == {}
    assert result["missing_priorities"] == [2, 3, 4, 5, 6, 7, 8, 9, 10]
    assert result["missing_fields"]["otc"] == [
        "status",
        "closure_gate",
        "next_action",
        "recommended_next_source",
        "source_gate",
        "closure_context",
    ]
    assert result["invalid_context_fields"] == {
        "b3": ["closure_context"],
        "otc": ["closure_context"],
    }
    assert result["invalid_artifact_rows"] == ["otc"]
    assert result["invalid_primary_artifacts"] == ["otc"]
    assert result["summary_next_review_batch_rows"] == 999
    assert result["computed_next_review_batch_rows"] == 0


def test_evaluate_next_review_command_safety_gate_blocks_manual_review_commands() -> None:
    result = evaluate_next_review_command_safety_gate(
        {
            "next_review_execution_plan": [
                {
                    "campaign_key": "b3",
                    "manual_review_required_before_run": True,
                    "review_required_preflight_checks": [
                        "inspect_risky_scripts_before_execution",
                        "confirm_listing_keyed_source_review_for_any_write",
                        "rerun_quality_validation_and_release_acceptance_after_execution",
                    ],
                    "data_change_authorized": False,
                },
                {
                    "campaign_key": "otc",
                    "manual_review_required_before_run": False,
                    "review_required_preflight_checks": [],
                    "data_change_authorized": False,
                },
            ],
            "next_review_command_safety_summary": {
                "manual_review_required_actions": 1,
                "preflight_gap_campaign_keys": [],
                "data_change_authorized_actions": 0,
                "execution_ready_without_manual_review": False,
                "execution_blocking_gate": "manual_review_required_before_execution",
                "execution_blocking_campaign_keys": ["b3"],
                "policy": "Command safety summary is a planning guard. Review-required commands must be inspected before execution and still do not authorize data changes.",
            },
        }
    )

    assert result["passed"] is True
    assert result["manual_review_campaign_keys"] == ["b3"]
    assert result["execution_ready_without_manual_review"] is False
    assert result["execution_blocking_gate"] == "manual_review_required_before_execution"
    assert result["execution_blocking_campaign_keys"] == ["b3"]
    assert result["data_change_authorized_campaign_keys"] == []


def test_evaluate_next_review_command_safety_gate_fails_for_stale_or_authorized_summary() -> None:
    result = evaluate_next_review_command_safety_gate(
        {
            "next_review_execution_plan": [
                {
                    "campaign_key": "b3",
                    "manual_review_required_before_run": True,
                    "review_required_preflight_checks": [],
                    "data_change_authorized": True,
                }
            ],
            "next_review_command_safety_summary": {
                "manual_review_required_actions": 0,
                "preflight_gap_campaign_keys": [],
                "data_change_authorized_actions": 0,
                "execution_ready_without_manual_review": True,
                "execution_blocking_gate": "no_manual_command_blockers_detected",
                "execution_blocking_campaign_keys": [],
                "policy": "Command safety summary.",
            },
        }
    )

    assert result["passed"] is False
    assert result["data_change_authorized_campaign_keys"] == ["b3"]
    assert result["mismatches"]["manual_review_required_actions"] == {"reported": 0, "expected": 1}
    assert result["mismatches"]["preflight_gap_campaign_keys"] == {"reported": [], "expected": ["b3"]}
    assert result["mismatches"]["data_change_authorized_actions"] == {"reported": 0, "expected": 1}
    assert result["mismatches"]["execution_ready_without_manual_review"] == {
        "reported": True,
        "expected": False,
    }
    assert result["mismatches"]["execution_blocking_gate"] == {
        "reported": "no_manual_command_blockers_detected",
        "expected": "manual_review_required_before_execution",
    }
    assert result["mismatches"]["execution_blocking_campaign_keys"] == {
        "reported": [],
        "expected": ["b3"],
    }
    assert "planning guard" in result["missing_policy_markers"]


def test_evaluate_campaign_closure_readiness_visibility_matches_batches() -> None:
    result = evaluate_campaign_closure_readiness_visibility(
        {
            "next_review_batches": [
                {
                    "campaign_key": "b3",
                    "closure_gate": "missing_delta_evidence_must_be_resolved_or_documented_before_campaign_closure",
                },
                {
                    "campaign_key": "otc",
                    "closure_gate": "review_artifact_rows_must_be_processed_before_campaign_closure",
                },
            ],
            "closure_readiness": {
                "ready_campaigns": 1,
                "blocked_campaigns": 1,
                "closure_gate_counts": {
                    "missing_delta_evidence_must_be_resolved_or_documented_before_campaign_closure": 1,
                    "review_artifact_rows_must_be_processed_before_campaign_closure": 1,
                },
                "ready_campaign_keys": ["otc"],
                "blocked_campaign_keys": ["b3"],
                "policy": "Campaigns are not closure-ready while missing delta evidence or review artifacts remain.",
            },
        }
    )

    assert result["passed"] is True


def test_evaluate_campaign_closure_readiness_visibility_fails_for_stale_summary() -> None:
    result = evaluate_campaign_closure_readiness_visibility(
        {
            "next_review_batches": [
                {
                    "campaign_key": "b3",
                    "closure_gate": "missing_delta_evidence_must_be_resolved_or_documented_before_campaign_closure",
                }
            ],
            "closure_readiness": {
                "ready_campaigns": 1,
                "blocked_campaigns": 0,
                "closure_gate_counts": {},
                "ready_campaign_keys": ["b3"],
                "blocked_campaign_keys": [],
                "policy": "Campaigns need work.",
            },
        }
    )

    assert result["passed"] is False
    assert result["mismatches"]["ready_campaigns"] == {"reported": 1, "expected": 0}
    assert result["mismatches"]["blocked_campaigns"] == {"reported": 0, "expected": 1}
    assert "not closure-ready" in result["missing_policy_markers"]
    assert "missing delta evidence" in result["missing_policy_markers"]
    assert "review artifacts" in result["missing_policy_markers"]


def test_evaluate_campaign_closure_blocker_visibility_matches_blocked_batches() -> None:
    blocker = {
        "priority": 1,
        "campaign_key": "b3",
        "blocker_type": "missing_delta_evidence",
        "closure_gate": "missing_delta_evidence_must_be_resolved_or_documented_before_campaign_closure",
        "artifact_rows": 2,
        "primary_artifact": "data/reports/b3.json",
        "first_missing_delta": "source_gap_delta",
        "evidence_command": "python scripts/build_b3_residual_isin_review.py",
        "command_mode": "local_report_rebuild",
        "network_required": False,
        "data_change_authorized": False,
        "next_action": "review",
        "recommended_next_source": "Official source.",
        "source_gate": "Exact source gate.",
    }
    blocker["blocker_context"] = blocker_context(blocker)
    result = evaluate_campaign_closure_blocker_visibility(
        {
            "summary": {"closure_blockers": 1},
            "next_review_batches": [
                {
                    "campaign_key": "b3",
                    "closure_gate": "missing_delta_evidence_must_be_resolved_or_documented_before_campaign_closure",
                },
                {
                    "campaign_key": "otc",
                    "closure_gate": "review_artifact_rows_must_be_processed_before_campaign_closure",
                },
            ],
            "closure_blockers": [blocker],
        }
    )

    assert result["passed"] is True
    assert result["observed_blocker_keys"] == ["b3"]


def test_evaluate_campaign_closure_blocker_visibility_fails_for_stale_or_weak_blockers() -> None:
    result = evaluate_campaign_closure_blocker_visibility(
        {
            "summary": {"closure_blockers": 2},
            "next_review_batches": [
                {
                    "campaign_key": "b3",
                    "closure_gate": "missing_delta_evidence_must_be_resolved_or_documented_before_campaign_closure",
                }
            ],
            "closure_blockers": [
                {
                    "priority": None,
                    "campaign_key": "otc",
                    "blocker_type": "unexpected",
                    "closure_gate": "",
                    "artifact_rows": "2",
                    "first_missing_delta": "",
                    "evidence_command": "make report",
                    "command_mode": "unsafe",
                    "network_required": "no",
                    "data_change_authorized": True,
                    "next_action": "",
                    "recommended_next_source": "",
                    "source_gate": "",
                    "blocker_context": "wrong",
                }
            ],
        }
    )

    assert result["passed"] is False
    assert result["mismatches"]["blocker_keys"] == {"reported": ["otc"], "expected": ["b3"]}
    assert result["mismatches"]["summary.closure_blockers"] == {"reported": 2, "expected": 1}
    assert result["missing_fields"]["otc"] == [
        "priority",
        "closure_gate",
        "first_missing_delta",
        "next_action",
        "recommended_next_source",
        "source_gate",
    ]
    assert result["invalid_context_fields"] == {"otc": ["blocker_context"]}
    assert result["invalid_rows"] == ["otc", "otc", "otc", "otc", "otc", "otc"]


def test_evaluate_campaign_review_policies_requires_no_guessing_source_authority_and_traceability() -> None:
    policy = {
        "campaign_scope": "b3 campaign decisions must preserve source-authority gates",
        "no_guessing": "No inferred identifiers, sectors, categories, names, or symbol changes may be applied.",
        "source_authority": "Official exchange, registry, issuer, or source evidence is required first.",
        "uncertain_gap_handling": "Unverified fields remain blank and tracked as source gaps.",
        "traceability": "Any future data change must be listing-keyed and tied to source evidence.",
    }

    result = evaluate_campaign_review_policies(
        {
            "campaigns": [
                {
                    "campaign_key": "b3",
                    "review_policy": policy,
                }
            ]
        }
    )

    assert result["passed"] is True
    assert result["checked_campaigns"] == ["b3"]
    assert campaign_review_policy_missing_marker_groups(policy) == []


def test_evaluate_campaign_review_policies_fails_for_missing_or_weak_policy() -> None:
    result = evaluate_campaign_review_policies(
        {
            "campaigns": [
                {"campaign_key": "b3"},
                {
                    "campaign_key": "otc",
                    "review_policy": {
                        "campaign_scope": "otc",
                        "no_guessing": "careful",
                        "source_authority": "",
                        "uncertain_gap_handling": "review later",
                        "traceability": "document",
                    },
                },
            ]
        }
    )

    assert result["passed"] is False
    assert result["missing_policy"] == ["b3"]
    assert result["missing_policy_keys"] == {"otc": ["source_authority"]}
    assert result["weak_policy"] == {
        "otc": ["no_guessing", "source_authority", "uncertain_gap_handling", "traceability"]
    }


def test_evaluate_campaign_baseline_alignment_requires_all_expected_campaign_keys() -> None:
    expected_keys = [
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

    result = evaluate_campaign_baseline_alignment(
        {"campaign_baseline": {key: {} for key in expected_keys}},
        {"campaign_deltas": {key: {} for key in reversed(expected_keys)}},
    )

    assert result["passed"] is True
    assert result["baseline_campaign_keys"] == expected_keys
    assert sorted(result["delta_campaign_keys"]) == sorted(expected_keys)


def test_evaluate_campaign_baseline_alignment_fails_for_missing_unexpected_or_misordered_keys() -> None:
    result = evaluate_campaign_baseline_alignment(
        {
            "campaign_baseline": {
                "otc": {},
                "b3": {},
                "canada": {},
                "asx": {},
                "weak_sector": {},
                "masterfile_collisions": {},
                "symbol_changes": {},
                "ohlcv": {},
                "unexpected": {},
            }
        },
        {"campaign_deltas": {"b3": {}, "otc": {}, "unexpected": {}}},
    )

    assert result["passed"] is False
    assert result["baseline_order_matches"] is False
    assert result["missing_baseline_keys"] == ["freshness", "baseline"]
    assert result["missing_delta_keys"] == [
        "canada",
        "asx",
        "weak_sector",
        "masterfile_collisions",
        "symbol_changes",
        "ohlcv",
        "freshness",
        "baseline",
    ]
    assert result["unexpected_baseline_keys"] == ["unexpected"]
    assert result["unexpected_delta_keys"] == ["unexpected"]


def test_evaluate_improvement_baseline_integrity_requires_traceable_snapshot() -> None:
    global_baseline = {
        "tickers": 10,
        "listing_keys": 10,
        "isin_coverage": 9,
        "stock_sector_coverage": 8,
        "etf_category_coverage": 7,
        "source_gap_rows": 2,
        "entry_quality_warn_rows": 1,
        "entry_quality_quarantine_rows": 0,
        "validation_failed_error_gates": 0,
        "source_freshness_status_totals": {"fresh": 1},
    }
    campaign_baseline = {
        "b3": {},
        "baseline": {
            "tracked_campaigns": 2,
            "global_metric_count": 10,
            "exchange_count": 1,
            "baseline_snapshot_rows": 1,
        },
    }
    exchange_baseline = {
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
    result = evaluate_improvement_baseline_integrity(
        {
            "_meta": BASELINE_META_FIXTURE,
            "summary": {
                "global_metric_count": 10,
                "campaign_count": 2,
                "exchange_count": 1,
                "source_file_count": len(EXPECTED_BASELINE_SOURCE_FILES),
                "baseline_context": baseline_global_context(global_baseline),
            },
            "global_baseline": global_baseline,
            "campaign_baseline": campaign_baseline,
            "exchange_baseline": exchange_baseline,
            "baseline_contexts": {
                "global": baseline_global_context(global_baseline),
                "campaigns": {
                    key: baseline_campaign_context(key, value)
                    for key, value in campaign_baseline.items()
                },
                "exchanges": {
                    key: baseline_exchange_context(key, value)
                    for key, value in exchange_baseline.items()
                },
            },
        }
    )

    assert result["passed"] is True
    assert result["source_file_mismatches"] == {}
    assert result["source_report_mismatches"] == {}
    assert result["baseline_summary_mismatches"] == {}
    assert result["top_summary_mismatches"] == {}
    assert result["baseline_context_gaps"] == {}


def test_baseline_context_integrity_gaps_require_exact_contexts() -> None:
    gaps = baseline_context_integrity_gaps(
        {
            "global": "wrong",
            "campaigns": {"b3": "wrong"},
            "exchanges": {"B3": "wrong"},
        },
        {"tickers": 10},
        {"b3": {"missing_isin_residual_rows": 2}},
        {
            "B3": {
                "tickers": 10,
                "isin_coverage": 9,
                "sector_coverage": 8,
                "source_gap_rows": 2,
                "entry_quality_warn_rows": 1,
                "entry_quality_source_gap_rows": 0,
                "entry_quality_quarantine_rows": 0,
            }
        },
    )

    assert gaps["global"]["actual"] == "wrong"
    assert gaps["campaigns"]["b3"]["actual"] == "wrong"
    assert gaps["exchanges"]["B3"]["actual"] == "wrong"


def test_evaluate_improvement_baseline_integrity_fails_for_stale_or_weak_snapshot() -> None:
    result = evaluate_improvement_baseline_integrity(
        {
            "_meta": {
                "generated_at": "not-a-timestamp",
                "purpose": "Snapshot.",
                "source_files": {
                    "coverage_report": "data/reports/other.json",
                    "unexpected_report": "data/reports/unexpected.json",
                },
                "source_reports": {
                    "coverage_report": "data/reports/other.json",
                    "unexpected_report": "data/reports/unexpected.json",
                },
            },
            "summary": {
                "global_metric_count": 99,
                "campaign_count": 99,
                "exchange_count": 99,
                "source_file_count": 0,
                "baseline_context": "wrong",
            },
            "global_baseline": {
                "tickers": "10",
                "listing_keys": 10,
            },
            "campaign_baseline": {
                "baseline": {
                    "tracked_campaigns": 99,
                    "global_metric_count": 99,
                    "exchange_count": 99,
                    "baseline_snapshot_rows": 0,
                },
            },
            "exchange_baseline": {"B3": {"tickers": 10}, "BROKEN": "not-a-row"},
            "baseline_contexts": {"global": "wrong", "campaigns": {}, "exchanges": {}},
        }
    )

    assert result["passed"] is False
    assert result["invalid_generated_at"] == "not-a-timestamp"
    assert result["source_file_mismatches"]["coverage_report"] == {
        "expected": "data/reports/coverage_report.json",
        "actual": "data/reports/other.json",
    }
    assert result["source_report_mismatches"]["coverage_report"] == {
        "expected": "data/reports/coverage_report.json",
        "actual": "data/reports/other.json",
    }
    assert result["unexpected_source_files"] == ["unexpected_report"]
    assert result["unexpected_source_reports"] == ["unexpected_report"]
    assert set(result["purpose_missing_markers"]) == {
        "Baseline snapshot",
        "future campaign before/after deltas",
        "not a data-fill source",
    }
    assert "source_gap_rows" in result["missing_global_keys"]
    assert result["nonnumeric_global_keys"] == ["tickers"]
    assert result["freshness_totals_missing"] is True
    assert result["exchange_key_gaps"]["B3"] == [
        "isin_coverage",
        "sector_coverage",
        "source_gap_rows",
        "entry_quality_warn_rows",
        "entry_quality_source_gap_rows",
        "entry_quality_quarantine_rows",
    ]
    assert result["malformed_exchange_rows"] == ["BROKEN"]
    assert result["baseline_summary_mismatches"]["baseline_snapshot_rows"] == {
        "expected": 1,
        "actual": 0,
    }
    assert result["top_summary_mismatches"]["global_metric_count"] == {
        "expected": 2,
        "actual": 99,
    }
    assert result["baseline_context_gaps"]["global"]["actual"] == "wrong"


def test_evaluate_review_artifact_gates_requires_gate_keys_for_review_reports(tmp_path) -> None:
    reports_dir = tmp_path / "data" / "reports"
    reports_dir.mkdir(parents=True)
    (reports_dir / "b3_residual_isin_review.json").write_text(
        '{"summary":{"rows":1,"apply_eligibility_totals":{"blocked":1}},"rows":[]}',
        encoding="utf-8",
    )

    result = evaluate_review_artifact_gates(
        {
            "campaigns": [
                {
                    "campaign_key": "b3",
                    "artifacts": [{"path": "data/reports/b3_residual_isin_review.json"}],
                }
            ]
        },
        reports_dir=reports_dir,
    )

    assert result["passed"] is True
    assert result["checked_count"] == 1


def test_evaluate_campaign_artifact_integrity_fails_for_stale_source_files(tmp_path) -> None:
    reports_dir = tmp_path / "data" / "reports"
    reports_dir.mkdir(parents=True)
    (reports_dir / "coverage_report.json").write_text(
        '{"_meta":{"generated_at":"2026-05-24T00:00:00Z"},"global":{}}',
        encoding="utf-8",
    )

    result = evaluate_campaign_artifact_integrity(
        {
            "_meta": {
                "generated_at": "not-a-timestamp",
                "rows": 1,
                "source_files": {
                    "coverage_report": "data/reports/other.json",
                    "unexpected_report": "data/reports/unexpected.json",
                },
                "policy": "Progress report.",
            },
            "campaigns": [
                {
                    "campaign_key": "freshness",
                    "artifacts": [
                        {
                            "path": "data/reports/coverage_report.json",
                            "rows": 0,
                            "generated_at": "2026-05-24T00:00:00Z",
                        }
                    ],
                }
            ],
        },
        reports_dir=reports_dir,
    )

    assert result["passed"] is False
    assert result["invalid_generated_at"] == "not-a-timestamp"
    assert result["unexpected_source_files"] == ["unexpected_report"]
    assert set(result["policy_missing_markers"]) == {
        "Progress report only",
        "does not authorize",
        "data fills",
        "symbol changes",
    }
    assert result["source_file_mismatches"]["coverage_report"] == {
        "expected": "data/reports/coverage_report.json",
        "actual": "data/reports/other.json",
    }
    assert result["source_file_mismatches"]["source_gap_classification"] == {
        "expected": "data/reports/source_gap_classification.json",
        "actual": None,
    }


def test_evaluate_review_artifact_siblings_requires_csv_and_markdown_review_outputs(tmp_path) -> None:
    reports_dir = tmp_path / "data" / "reports"
    reports_dir.mkdir(parents=True)
    (reports_dir / "b3_residual_isin_review.json").write_text(
        '{"summary":{"rows":1},"rows":[{"listing_key":"B3::ABC"}]}',
        encoding="utf-8",
    )
    (reports_dir / "b3_residual_isin_review.csv").write_text("listing_key\nB3::ABC\n", encoding="utf-8")
    (reports_dir / "b3_residual_isin_review.md").write_text("# B3 review\n", encoding="utf-8")

    result = evaluate_review_artifact_siblings(
        {
            "campaigns": [
                {
                    "campaign_key": "b3",
                    "artifacts": [{"path": "data/reports/b3_residual_isin_review.json"}],
                }
            ]
        },
        reports_dir=reports_dir,
    )

    assert result["passed"] is True
    assert result["checked_count"] == 1
    assert result["sibling_paths"]["data/reports/b3_residual_isin_review.json"] == {
        "csv": "data/reports/b3_residual_isin_review.csv",
        "md": "data/reports/b3_residual_isin_review.md",
    }


def test_evaluate_review_artifact_siblings_fails_for_missing_or_stale_siblings(tmp_path) -> None:
    reports_dir = tmp_path / "data" / "reports"
    reports_dir.mkdir(parents=True)
    (reports_dir / "symbol_changes_review.json").write_text(
        '{"summary":{"rows":2},"rows":[{"change_id":"one"},{"change_id":"two"}]}',
        encoding="utf-8",
    )
    (reports_dir / "symbol_changes_review.csv").write_text("change_id\none\n", encoding="utf-8")
    (reports_dir / "symbol_changes_review.md").write_text("", encoding="utf-8")
    (reports_dir / "canada_figi_queue.json").write_text(
        '{"summary":{"rows":0},"rows":[]}',
        encoding="utf-8",
    )

    result = evaluate_review_artifact_siblings(
        {
            "campaigns": [
                {
                    "campaign_key": "symbol_changes",
                    "artifacts": [{"path": "data/reports/symbol_changes_review.json"}],
                },
                {
                    "campaign_key": "canada",
                    "artifacts": [{"path": "data/reports/canada_figi_queue.json"}],
                },
            ]
        },
        reports_dir=reports_dir,
    )

    assert result["passed"] is False
    assert result["csv_row_count_mismatches"] == [
        {
            "path": "data/reports/symbol_changes_review.json",
            "json_rows": 2,
            "csv_rows": 1,
        }
    ]
    assert result["empty_markdown_summaries"] == ["data/reports/symbol_changes_review.json"]
    assert result["missing_siblings"] == [
        {"path": "data/reports/canada_figi_queue.json", "missing": ["csv", "md"]}
    ]


def test_evaluate_review_artifact_siblings_fails_for_old_sibling_files(tmp_path) -> None:
    reports_dir = tmp_path / "data" / "reports"
    reports_dir.mkdir(parents=True)
    json_path = reports_dir / "b3_residual_isin_review.json"
    csv_path = reports_dir / "b3_residual_isin_review.csv"
    md_path = reports_dir / "b3_residual_isin_review.md"
    json_path.write_text('{"summary":{"rows":1},"rows":[{"listing_key":"B3::ABC"}]}', encoding="utf-8")
    csv_path.write_text("listing_key\nB3::ABC\n", encoding="utf-8")
    md_path.write_text("# B3 review\n", encoding="utf-8")
    os.utime(json_path, (1_700_000_000, 1_700_000_000))
    os.utime(csv_path, (1_699_999_900, 1_699_999_900))
    os.utime(md_path, (1_699_999_939, 1_699_999_939))

    result = evaluate_review_artifact_siblings(
        {
            "campaigns": [
                {
                    "campaign_key": "b3",
                    "artifacts": [{"path": "data/reports/b3_residual_isin_review.json"}],
                }
            ]
        },
        reports_dir=reports_dir,
    )

    assert result["passed"] is False
    assert result["stale_siblings"] == [
        {"path": "data/reports/b3_residual_isin_review.json", "sibling": "csv", "age_seconds": 100},
        {"path": "data/reports/b3_residual_isin_review.json", "sibling": "md", "age_seconds": 61},
    ]


def test_evaluate_review_artifact_gates_fails_for_missing_gate_summary(tmp_path) -> None:
    reports_dir = tmp_path / "data" / "reports"
    reports_dir.mkdir(parents=True)
    (reports_dir / "symbol_changes_review.json").write_text(
        '{"summary":{"rows":1},"rows":[{"listing_key":"X::Y"}]}',
        encoding="utf-8",
    )

    result = evaluate_review_artifact_gates(
        {
            "campaigns": [
                {
                    "campaign_key": "symbol_changes",
                    "artifacts": [{"path": "data/reports/symbol_changes_review.json"}],
                }
            ]
        },
        reports_dir=reports_dir,
    )

    assert result["passed"] is False
    assert result["missing_gate_summaries"] == ["data/reports/symbol_changes_review.json"]


def test_evaluate_review_artifact_policy_requires_review_policy(tmp_path) -> None:
    reports_dir = tmp_path / "data" / "reports"
    reports_dir.mkdir(parents=True)
    (reports_dir / "b3_residual_isin_review.json").write_text(
        '{"_meta":{"policy":"Review queue only. No guessing; official source evidence is required before any apply."},"summary":{"rows":1},"rows":[]}',
        encoding="utf-8",
    )

    result = evaluate_review_artifact_policy(
        {
            "campaigns": [
                {
                    "campaign_key": "b3",
                    "artifacts": [{"path": "data/reports/b3_residual_isin_review.json"}],
                }
            ]
        },
        reports_dir=reports_dir,
    )

    assert result["passed"] is True
    assert result["policies"]["data/reports/b3_residual_isin_review.json"] == (
        "Review queue only. No guessing; official source evidence is required before any apply."
    )


def test_evaluate_review_artifact_policy_accepts_source_policy_fallback(tmp_path) -> None:
    reports_dir = tmp_path / "data" / "reports"
    reports_dir.mkdir(parents=True)
    (reports_dir / "symbol_changes_review.json").write_text(
        '{"_meta":{"source_policy":"secondary review only; official source evidence is required before apply"},"summary":{"rows":1},"rows":[]}',
        encoding="utf-8",
    )

    result = evaluate_review_artifact_policy(
        {
            "campaigns": [
                {
                    "campaign_key": "symbol_changes",
                    "artifacts": [{"path": "data/reports/symbol_changes_review.json"}],
                }
            ]
        },
        reports_dir=reports_dir,
    )

    assert result["passed"] is True
    assert result["policies"]["data/reports/symbol_changes_review.json"] == (
        "secondary review only; official source evidence is required before apply"
    )


def test_evaluate_review_artifact_policy_fails_for_missing_policy(tmp_path) -> None:
    reports_dir = tmp_path / "data" / "reports"
    reports_dir.mkdir(parents=True)
    (reports_dir / "ohlcv_plausibility.json").write_text(
        '{"summary":{"rows":1},"review_items":[]}',
        encoding="utf-8",
    )

    result = evaluate_review_artifact_policy(
        {
            "campaigns": [
                {
                    "campaign_key": "ohlcv",
                    "artifacts": [{"path": "data/reports/ohlcv_plausibility.json"}],
                }
            ]
        },
        reports_dir=reports_dir,
    )

    assert result["passed"] is False
    assert result["missing_policy"] == ["data/reports/ohlcv_plausibility.json"]


def test_evaluate_review_artifact_policy_fails_for_weak_policy(tmp_path) -> None:
    reports_dir = tmp_path / "data" / "reports"
    reports_dir.mkdir(parents=True)
    (reports_dir / "symbol_changes_review.json").write_text(
        '{"_meta":{"policy":"secondary_review_only"},"summary":{"rows":1},"rows":[]}',
        encoding="utf-8",
    )

    result = evaluate_review_artifact_policy(
        {
            "campaigns": [
                {
                    "campaign_key": "symbol_changes",
                    "artifacts": [{"path": "data/reports/symbol_changes_review.json"}],
                }
            ]
        },
        reports_dir=reports_dir,
    )

    assert result["passed"] is False
    assert result["weak_policy"] == {
        "data/reports/symbol_changes_review.json": ["authority_or_evidence_gate"]
    }


def test_review_policy_missing_marker_groups_checks_review_and_authority_semantics() -> None:
    assert review_policy_missing_marker_groups(
        "Review queue only. No guessing; official source evidence is required before apply."
    ) == []
    assert review_policy_missing_marker_groups("official source required") == ["review_or_no_guessing_gate"]
    assert review_policy_missing_marker_groups("review only") == ["authority_or_evidence_gate"]


def test_row_has_review_identity_accepts_listing_keys_and_review_candidates() -> None:
    assert row_has_review_identity({"listing_key": "B3::PETR4"}) is True
    assert row_has_review_identity({"old_listing_keys": ["NYSE::ABC"]}) is True
    assert row_has_review_identity({"change_id": "NYSE:ABC:2026-05-24"}) is True
    assert row_has_review_identity({"official_source_key": "source-a", "official_ticker": "ABC"}) is True
    assert row_has_review_identity({"financialdata_exchange": "NYSE", "financialdata_ticker": "ABC"}) is True
    assert row_has_review_identity({"ticker": "ABC"}) is False


def test_row_has_review_evidence_accepts_source_review_decision_or_gap_fields() -> None:
    assert row_has_review_evidence({"source_of_truth_outcome": "accepted_source_gap"}) is True
    assert row_has_review_evidence({"review_decision": "hold"}) is True
    assert row_has_review_evidence({"apply_eligibility": "blocked"}) is True
    assert row_has_review_evidence({"verification_evidence_required": "official_source"}) is True
    assert row_has_review_evidence({"ticker": "ABC", "name": "ABC Inc"}) is False


def test_row_has_apply_traceability_requires_listing_decision_reason_and_source_evidence() -> None:
    assert row_has_apply_traceability(
        {
            "listing_key": "TSXV::SUM",
            "decision": "apply",
            "reason": "accepted_probe_match",
            "isin": "CA8662511010",
            "figi": "BBG01GRB8TG0",
            "verification_evidence_required": "listing_key_isin_exchange_hint_openfigi_figi_and_cross_isin_collision_gates",
        }
    ) is True
    assert row_has_apply_traceability(
        {
            "listing_key": "NGX::ABC",
            "decision": "skip",
            "reason": "unsupported_broad_ngx_sector_label",
            "official_sector": "SERVICES",
        }
    ) is True
    assert row_has_apply_traceability({"listing_key": "TSXV::SUM", "decision": "apply", "reason": "accepted"}) is False


def test_evaluate_review_row_traceability_requires_row_level_identity(tmp_path) -> None:
    reports_dir = tmp_path / "data" / "reports"
    reports_dir.mkdir(parents=True)
    (reports_dir / "symbol_changes_review.json").write_text(
        """
        {
          "summary": {"rows": 2, "apply_eligibility_counts": {"review": 2}},
          "rows": [
            {"change_id": "NYSE:ABC:2026-05-24", "old_listing_keys": ["NYSE::ABC"]},
            {"old_symbol": "DEF", "new_symbol": "XYZ"}
          ]
        }
        """,
        encoding="utf-8",
    )

    result = evaluate_review_row_traceability(
        {
            "campaigns": [
                {
                    "campaign_key": "symbol_changes",
                    "artifacts": [{"path": "data/reports/symbol_changes_review.json"}],
                }
            ]
        },
        reports_dir=reports_dir,
    )

    assert result["passed"] is False
    assert result["identity_gap_rows"]["data/reports/symbol_changes_review.json"] == [
        {
            "row_index": 1,
            "reason": "missing_listing_key_or_review_identity",
            "available_keys": ["new_symbol", "old_symbol"],
        }
    ]


def test_evaluate_review_row_traceability_fails_when_report_counts_rows_without_container(tmp_path) -> None:
    reports_dir = tmp_path / "data" / "reports"
    reports_dir.mkdir(parents=True)
    (reports_dir / "ohlcv_plausibility.json").write_text(
        '{"_meta":{"rows":2},"summary":{"review_bucket_counts":{"sample":2}},"flagged_items":[]}',
        encoding="utf-8",
    )

    result = evaluate_review_row_traceability(
        {
            "campaigns": [
                {
                    "campaign_key": "ohlcv",
                    "artifacts": [{"path": "data/reports/ohlcv_plausibility.json"}],
                }
            ]
        },
        reports_dir=reports_dir,
    )

    assert result["passed"] is False
    assert result["identity_gap_rows"]["data/reports/ohlcv_plausibility.json"] == [
        {
            "row_index": None,
            "reason": "review_row_container_count_mismatch",
            "reported_rows": 2,
            "row_container_rows": 0,
            "accepted_row_container_keys": ["rows", "review_items", "items"],
        }
    ]


def test_evaluate_review_row_traceability_passes_for_empty_queue_and_identity_rows(tmp_path) -> None:
    reports_dir = tmp_path / "data" / "reports"
    reports_dir.mkdir(parents=True)
    (reports_dir / "canada_figi_queue.json").write_text(
        '{"summary":{"rows":0,"apply_eligibility_totals":{"no_active_openfigi_probe_rows":1}},"rows":[]}',
        encoding="utf-8",
    )
    (reports_dir / "financialdata_isin_supplements_review.json").write_text(
        """
        {
          "summary": {"rows": 1, "apply_eligibility_counts": {"review": 1}},
          "review_items": [
            {"financialdata_exchange": "NYSE", "financialdata_ticker": "ABC"}
          ]
        }
        """,
        encoding="utf-8",
    )

    result = evaluate_review_row_traceability(
        {
            "campaigns": [
                {
                    "campaign_key": "canada",
                    "artifacts": [{"path": "data/reports/canada_figi_queue.json"}],
                },
                {
                    "campaign_key": "freshness",
                    "artifacts": [{"path": "data/reports/financialdata_isin_supplements_review.json"}],
                },
            ]
        },
        reports_dir=reports_dir,
    )

    assert result["passed"] is True
    assert result["checked_count"] == 2


def test_evaluate_review_row_evidence_requires_source_review_decision_or_gap_fields(tmp_path) -> None:
    reports_dir = tmp_path / "data" / "reports"
    reports_dir.mkdir(parents=True)
    (reports_dir / "b3_residual_isin_review.json").write_text(
        """
        {
          "summary": {"rows": 2, "apply_eligibility_totals": {"blocked": 1}},
          "rows": [
            {"listing_key": "B3::OK", "source_of_truth_outcome": "accepted_source_gap"},
            {"listing_key": "B3::BAD", "ticker": "BAD"}
          ]
        }
        """,
        encoding="utf-8",
    )

    result = evaluate_review_row_evidence(
        {
            "campaigns": [
                {
                    "campaign_key": "b3",
                    "artifacts": [{"path": "data/reports/b3_residual_isin_review.json"}],
                }
            ]
        },
        reports_dir=reports_dir,
    )

    assert result["passed"] is False
    assert result["evidence_gap_rows"]["data/reports/b3_residual_isin_review.json"] == [
        {
            "row_index": 1,
            "reason": "missing_source_review_decision_or_gap_evidence",
            "available_keys": ["listing_key", "ticker"],
        }
    ]


def test_evaluate_review_row_evidence_validates_otc_name_mismatch_contexts(tmp_path) -> None:
    reports_dir = tmp_path / "data" / "reports"
    reports_dir.mkdir(parents=True)
    row = {
        "listing_key": "OTC::AECX",
        "ticker": "AECX",
        "exchange": "OTC",
        "current_name": "CurrentC Power Corporation",
        "official_name": "ACADIA ENERGY CORP",
        "isin": "US92855W2017",
        "country": "United States",
        "official_sources": "otc_markets_stock_screener",
        "token_overlap": 0,
        "current_token_count": 3,
        "official_token_count": 3,
        "review_class": "probable_otc_rename_or_symbol_reuse",
        "review_strategy": "verify_isin_anchored_issuer_history_before_name_change",
        "apply_eligibility": "blocked_until_isin_anchored_issuer_history_review",
        "verification_evidence_required": (
            "official_or_reviewed_isin_bearing_source_matching_current_issuer_listing_key_and_name"
        ),
        "recommended_next_source": (
            "Official or reviewed ISIN-bearing issuer-history source matching current issuer, listing key, and name."
        ),
        "source_gate": "Do not change the name until ISIN-anchored evidence proves the same current issuer.",
        "review_decision": "",
    }
    row["official_source_context"] = otc_name_mismatch_official_source_context(row)
    row["identity_review_context"] = otc_name_mismatch_identity_review_context(row)
    row["decision_review_context"] = "stale"
    (reports_dir / "otc_name_mismatch_review.json").write_text(
        json.dumps(
            {
                "_meta": {"rows": 1},
                "summary": {"rows": 1, "review_class_counts": {"probable_otc_rename_or_symbol_reuse": 1}},
                "items": [row],
            }
        ),
        encoding="utf-8",
    )

    result = evaluate_review_row_evidence(
        {
            "campaigns": [
                {
                    "campaign_key": "otc",
                    "artifacts": [{"path": "data/reports/otc_name_mismatch_review.json"}],
                }
            ]
        },
        reports_dir=reports_dir,
    )

    assert result["passed"] is False
    assert result["evidence_gap_rows"]["data/reports/otc_name_mismatch_review.json"][0]["invalid_fields"] == [
        "decision_review_context"
    ]


def test_evaluate_review_row_evidence_fails_when_report_counts_rows_without_container(tmp_path) -> None:
    reports_dir = tmp_path / "data" / "reports"
    reports_dir.mkdir(parents=True)
    (reports_dir / "ohlcv_plausibility.json").write_text(
        '{"_meta":{"selected_rows":2},"summary":{"review_bucket_counts":{"sample":2}},"flagged_items":[]}',
        encoding="utf-8",
    )

    result = evaluate_review_row_evidence(
        {
            "campaigns": [
                {
                    "campaign_key": "ohlcv",
                    "artifacts": [{"path": "data/reports/ohlcv_plausibility.json"}],
                }
            ]
        },
        reports_dir=reports_dir,
    )

    assert result["passed"] is False
    assert result["evidence_gap_rows"]["data/reports/ohlcv_plausibility.json"] == [
        {
            "row_index": None,
            "reason": "review_row_container_count_mismatch",
            "reported_rows": 2,
            "row_container_rows": 0,
            "accepted_row_container_keys": ["rows", "review_items", "items"],
        }
    ]


def test_evaluate_review_row_evidence_passes_for_empty_queue_and_evidence_rows(tmp_path) -> None:
    reports_dir = tmp_path / "data" / "reports"
    reports_dir.mkdir(parents=True)
    (reports_dir / "canada_figi_queue.json").write_text(
        '{"summary":{"rows":0,"apply_eligibility_totals":{"no_active_openfigi_probe_rows":1}},"rows":[]}',
        encoding="utf-8",
    )
    (reports_dir / "symbol_changes_review.json").write_text(
        """
        {
          "summary": {"rows": 1, "apply_eligibility_counts": {"review": 1}},
          "rows": [
            {"change_id": "NYSE:ABC:2026-05-24", "source": "NASDAQ Trader", "review_bucket": "already_reflected"}
          ]
        }
        """,
        encoding="utf-8",
    )

    result = evaluate_review_row_evidence(
        {
            "campaigns": [
                {
                    "campaign_key": "canada",
                    "artifacts": [{"path": "data/reports/canada_figi_queue.json"}],
                },
                {
                    "campaign_key": "symbol_changes",
                    "artifacts": [{"path": "data/reports/symbol_changes_review.json"}],
                },
            ]
        },
        reports_dir=reports_dir,
    )

    assert result["passed"] is True
    assert result["checked_count"] == 2


def test_evaluate_apply_artifact_traceability_requires_policy_and_row_traceability(tmp_path) -> None:
    reports_dir = tmp_path / "data" / "reports"
    reports_dir.mkdir(parents=True)
    (reports_dir / "canada_figi_apply_report.json").write_text(
        """
        {
          "summary": {
            "generated_at": "2026-05-24T00:00:00Z",
            "rows": 1,
            "policy": {"gates": "listing_key, ISIN, exchange hint, non-empty FIGI"}
          },
          "rows": [
            {
              "listing_key": "TSXV::SUM",
              "decision": "apply",
              "reason": "accepted_probe_match",
              "isin": "CA8662511010",
              "figi": "BBG01GRB8TG0",
              "requested_exchange_hint": "CN",
              "openfigi_exch_code": "CN",
              "openfigi_candidate_count": "1",
              "identifier_isin": "CA8662511010",
              "listing_index_isin": "CA8662511010",
              "existing_identifier_figi": "",
              "existing_listing_index_figi": "",
              "verification_evidence_required": "listing_key_isin_exchange_hint_openfigi_figi_and_cross_isin_collision_gates",
              "openfigi_probe_context": "requested_exchange_hint=CN;openfigi_exch_code=CN;openfigi_figi_present=true;candidate_count=1",
              "existing_identifier_context": "identifier_isin=CA8662511010;listing_index_isin=CA8662511010;existing_identifier_figi=none;existing_listing_index_figi=none",
              "apply_gate_context": "decision=apply;reason=accepted_probe_match;verification_evidence_required=listing_key_isin_exchange_hint_openfigi_figi_and_cross_isin_collision_gates"
            }
          ]
        }
        """,
        encoding="utf-8",
    )

    result = evaluate_apply_artifact_traceability(
        {
            "campaigns": [
                {
                    "campaign_key": "canada",
                    "artifacts": [{"path": "data/reports/canada_figi_apply_report.json"}],
                }
            ]
        },
        reports_dir=reports_dir,
    )

    assert result["passed"] is True
    assert result["checked_count"] == 1


def test_evaluate_apply_artifact_traceability_validates_b3_etf_apply_contexts(tmp_path) -> None:
    reports_dir = tmp_path / "data" / "reports"
    reports_dir.mkdir(parents=True)
    row = {
        "listing_key": "B3::B5MB11",
        "ticker": "B5MB11",
        "exchange": "B3",
        "current_etf_category": "Equity",
        "official_sector": "Fixed Income",
        "category_update": "Fixed Income",
        "candidate_sources": "b3_listed_etfs",
        "candidate_source_urls": "https://sistemaswebb3-listados.b3.com.br/fundsListedProxy/Search/",
        "decision": "apply",
        "reason": "official_b3_listed_etf_category_differs_from_current",
        "verification_evidence_required": (
            "official_b3_listed_etfs_row_with_exact_listing_key_and_single_canonical_etf_category"
        ),
    }
    row["official_source_context"] = b3_etf_apply_official_source_context(row)
    row["category_review_context"] = b3_etf_apply_category_review_context(row)
    row["apply_gate_context"] = "stale"
    (reports_dir / "b3_etf_category_apply_report.json").write_text(
        json.dumps(
            {
                "summary": {
                    "rows": 1,
                    "policy": {"source": "official B3", "no_guessing": "No category inference"},
                },
                "rows": [row],
            }
        ),
        encoding="utf-8",
    )

    result = evaluate_apply_artifact_traceability(
        {
            "campaigns": [
                {
                    "campaign_key": "b3",
                    "artifacts": [{"path": "data/reports/b3_etf_category_apply_report.json"}],
                }
            ]
        },
        reports_dir=reports_dir,
    )

    assert result["passed"] is False
    assert result["traceability_gap_rows"]["data/reports/b3_etf_category_apply_report.json"][0]["invalid_fields"] == [
        "apply_gate_context"
    ]


def test_evaluate_apply_artifact_traceability_validates_ngx_apply_contexts(tmp_path) -> None:
    reports_dir = tmp_path / "data" / "reports"
    reports_dir.mkdir(parents=True)
    row = {
        "listing_key": "NGX::ABCTRANS",
        "ticker": "ABCTRANS",
        "exchange": "NGX",
        "official_sector": "SERVICES",
        "sector_update": "",
        "decision": "skip",
        "reason": "unsupported_broad_ngx_sector_label",
    }
    row["official_source_context"] = ngx_apply_official_source_context(row)
    row["mapping_review_context"] = "stale"
    row["apply_gate_context"] = ngx_apply_gate_context(row)
    (reports_dir / "ngx_official_sector_apply_report.json").write_text(
        json.dumps(
            {
                "summary": {
                    "rows": 1,
                    "policy": {"source": "official NGX", "no_guessing": "No sector inference"},
                },
                "rows": [row],
            }
        ),
        encoding="utf-8",
    )

    result = evaluate_apply_artifact_traceability(
        {
            "campaigns": [
                {
                    "campaign_key": "weak_sector",
                    "artifacts": [{"path": "data/reports/ngx_official_sector_apply_report.json"}],
                }
            ]
        },
        reports_dir=reports_dir,
    )

    assert result["passed"] is False
    assert result["traceability_gap_rows"]["data/reports/ngx_official_sector_apply_report.json"][0]["invalid_fields"] == [
        "mapping_review_context"
    ]


def test_evaluate_apply_artifact_traceability_fails_for_missing_policy_or_row_evidence(tmp_path) -> None:
    reports_dir = tmp_path / "data" / "reports"
    reports_dir.mkdir(parents=True)
    (reports_dir / "canada_figi_apply_report.json").write_text(
        """
        {
          "summary": {"rows": 2},
          "rows": [
            {"listing_key": "TSXV::SUM", "decision": "apply", "reason": "accepted_probe_match", "isin": "CA8662511010"},
            {"decision": "apply", "reason": "accepted_probe_match", "figi": "BBG01GRB8TG0"}
          ]
        }
        """,
        encoding="utf-8",
    )

    result = evaluate_apply_artifact_traceability(
        {
            "campaigns": [
                {
                    "campaign_key": "canada",
                    "artifacts": [{"path": "data/reports/canada_figi_apply_report.json"}],
                }
            ]
        },
        reports_dir=reports_dir,
    )

    assert result["passed"] is False
    assert result["missing_policy"] == ["data/reports/canada_figi_apply_report.json"]
    assert result["traceability_gap_rows"]["data/reports/canada_figi_apply_report.json"] == [
        {
            "row_index": 0,
            "reason": "missing_listing_key_decision_reason_or_source_evidence",
            "available_keys": ["decision", "isin", "listing_key", "reason"],
            "invalid_fields": [
                "openfigi_probe_context",
                "existing_identifier_context",
                "apply_gate_context",
            ],
        },
        {
            "row_index": 1,
            "reason": "missing_listing_key_decision_reason_or_source_evidence",
            "available_keys": ["decision", "figi", "reason"],
            "invalid_fields": [
                "openfigi_probe_context",
                "existing_identifier_context",
                "apply_gate_context",
            ],
        },
    ]


def test_evaluate_supplement_artifact_traceability_requires_policy_identity_and_official_evidence(tmp_path) -> None:
    reports_dir = tmp_path / "data" / "reports"
    reports_dir.mkdir(parents=True)
    (reports_dir / "financialdata_isin_supplements_review.json").write_text(
        """
        {
          "summary": {
            "rows": 2,
            "generated_at": "2026-05-24T00:00:00Z",
            "policy": {
              "financialdata_role": "FinancialData rows are discovery signals only.",
              "identifier_gate": "A supplement requires official evidence."
            }
          },
          "review_items": [
            {
              "financialdata_ticker": "RELIANCE",
              "financialdata_exchange": "NSE_IN",
              "financialdata_name": "Reliance Industries Limited",
              "financialdata_review_scope": "current_exchange_gap",
              "decision": "preserve",
              "reason": "already_in_financialdata_supplement",
              "financialdata_review_queue": "preserve_existing_reviewed_supplement",
              "review_priority": "P4",
              "review_strategy": "preserve_existing_reviewed_supplement_no_new_apply",
              "apply_eligibility": "preserve_existing_reviewed_supplement_no_new_apply",
              "verification_evidence_required": "existing_reviewed_supplement_retained_with_original_official_source",
              "recommended_next_source": "Existing reviewed FinancialData supplement source.",
              "source_gate": "Preserve existing reviewed supplement; do not create a new row from FinancialData alone.",
              "official_ticker": "RELIANCE",
              "official_exchange": "NSE_IN",
              "official_name": "Reliance Industries Limited",
              "official_isin": "INE002A01018",
              "official_source_key": "nse_india_securities_available",
              "official_reference_scope": "exchange_directory",
              "financialdata_discovery_context": "financialdata_exchange=NSE_IN;financialdata_ticker=RELIANCE;financialdata_review_scope=current_exchange_gap;financialdata_name_present=true",
              "official_identity_context": "official_exchange=NSE_IN;official_ticker=RELIANCE;official_source_key=nse_india_securities_available;official_reference_scope=exchange_directory;official_isin_present=true;official_name_present=true",
              "supplement_review_context": "decision=preserve;reason=already_in_financialdata_supplement;financialdata_review_queue=preserve_existing_reviewed_supplement;apply_eligibility=preserve_existing_reviewed_supplement_no_new_apply;verification_evidence_required=existing_reviewed_supplement_retained_with_original_official_source"
            },
            {
              "financialdata_ticker": "MISS",
              "financialdata_exchange": "KRX",
              "financialdata_review_scope": "global_expansion_candidate",
              "decision": "reject",
              "reason": "no_name_gated_official_isin_match",
              "financialdata_review_queue": "keep_absent_until_official_name_gated_match",
              "review_priority": "P2",
              "review_strategy": "keep_absent_until_official_name_gated_identifier_evidence_exists",
              "apply_eligibility": "keep_absent_until_name_gated_official_isin_match",
              "verification_evidence_required": "official_active_masterfile_or_registry_row_matching_financialdata_name_and_listing",
              "recommended_next_source": "Official active masterfile, registry, or issuer source matching FinancialData name and listing identity.",
              "source_gate": "Keep absent until an official active source satisfies exact name/listing identity and ISIN gates.",
              "financialdata_discovery_context": "financialdata_exchange=KRX;financialdata_ticker=MISS;financialdata_review_scope=global_expansion_candidate;financialdata_name_present=false",
              "official_identity_context": "official_exchange=none;official_ticker=none;official_source_key=none;official_reference_scope=none;official_isin_present=false;official_name_present=false",
              "supplement_review_context": "decision=reject;reason=no_name_gated_official_isin_match;financialdata_review_queue=keep_absent_until_official_name_gated_match;apply_eligibility=keep_absent_until_name_gated_official_isin_match;verification_evidence_required=official_active_masterfile_or_registry_row_matching_financialdata_name_and_listing"
            }
          ]
        }
        """,
        encoding="utf-8",
    )

    result = evaluate_supplement_artifact_traceability(
        {
            "campaigns": [
                {
                    "campaign_key": "freshness",
                    "artifacts": [{"path": "data/reports/financialdata_isin_supplements_review.json"}],
                }
            ]
        },
        reports_dir=reports_dir,
    )

    assert result["passed"] is True
    assert result["checked_count"] == 1


def test_financialdata_supplement_contexts_are_exact_field_summaries() -> None:
    row = {
        "financialdata_exchange": "NSE_IN",
        "financialdata_ticker": "RELIANCE",
        "financialdata_review_scope": "global_expansion_candidate",
        "financialdata_name": "Reliance Industries Limited",
        "official_exchange": "NSE_IN",
        "official_ticker": "RELIANCE",
        "official_source_key": "nse_india_securities_available",
        "official_reference_scope": "exchange_directory",
        "official_isin": "INE002A01018",
        "official_name": "Reliance Industries Limited",
        "decision": "accept",
        "reason": "official_isin_name_gated_unique_primary",
        "financialdata_review_queue": "official_name_gated_supplement_candidate",
        "apply_eligibility": "eligible_for_supplement_only_after_official_active_isin_name_gate_and_duplicate_checks",
        "verification_evidence_required": "official_active_masterfile_valid_isin_name_gate_no_existing_ticker_or_isin_collision",
    }

    assert (
        financialdata_discovery_context(row)
        == "financialdata_exchange=NSE_IN;financialdata_ticker=RELIANCE;"
        "financialdata_review_scope=global_expansion_candidate;financialdata_name_present=true"
    )
    assert (
        financialdata_official_identity_context(row)
        == "official_exchange=NSE_IN;official_ticker=RELIANCE;"
        "official_source_key=nse_india_securities_available;official_reference_scope=exchange_directory;"
        "official_isin_present=true;official_name_present=true"
    )
    assert (
        financialdata_supplement_review_context(row)
        == "decision=accept;reason=official_isin_name_gated_unique_primary;"
        "financialdata_review_queue=official_name_gated_supplement_candidate;"
        "apply_eligibility=eligible_for_supplement_only_after_official_active_isin_name_gate_and_duplicate_checks;"
        "verification_evidence_required=official_active_masterfile_valid_isin_name_gate_no_existing_ticker_or_isin_collision"
    )


def test_evaluate_supplement_artifact_traceability_fails_for_missing_policy_or_accept_evidence(tmp_path) -> None:
    reports_dir = tmp_path / "data" / "reports"
    reports_dir.mkdir(parents=True)
    (reports_dir / "financialdata_isin_supplements_review.json").write_text(
        """
        {
          "summary": {"rows": 1, "policy": {"financialdata_role": "discovery only"}},
          "review_items": [
            {
              "financialdata_ticker": "RELIANCE",
              "financialdata_exchange": "NSE_IN",
              "decision": "accept",
              "reason": "accepted",
              "apply_eligibility": "eligible_for_supplement",
              "verification_evidence_required": "official_source",
              "official_ticker": "RELIANCE"
            }
          ]
        }
        """,
        encoding="utf-8",
    )

    result = evaluate_supplement_artifact_traceability(
        {
            "campaigns": [
                {
                    "campaign_key": "freshness",
                    "artifacts": [{"path": "data/reports/financialdata_isin_supplements_review.json"}],
                }
            ]
        },
        reports_dir=reports_dir,
    )

    assert result["passed"] is False
    assert result["missing_policy"] == ["data/reports/financialdata_isin_supplements_review.json"]
    assert result["traceability_gap_rows"]["data/reports/financialdata_isin_supplements_review.json"] == [
        {
            "row_index": 0,
            "reason": "missing_financialdata_identity_review_or_official_evidence",
            "missing_keys": [
                "financialdata_review_queue",
                "review_priority",
                "review_strategy",
                "recommended_next_source",
                "source_gate",
                "financialdata_discovery_context",
                "official_identity_context",
                "supplement_review_context",
            ],
            "missing_official_evidence": ["official_exchange", "official_isin", "official_source_key"],
            "invalid_fields": [
                "review_priority",
                "financialdata_discovery_context",
                "official_identity_context",
                "supplement_review_context",
            ],
            "available_keys": [
                "apply_eligibility",
                "decision",
                "financialdata_exchange",
                "financialdata_ticker",
                "official_ticker",
                "reason",
                "verification_evidence_required",
            ],
        }
    ]


def test_evaluate_campaign_artifact_integrity_checks_files_and_row_counts(tmp_path) -> None:
    reports_dir = tmp_path / "data" / "reports"
    reports_dir.mkdir(parents=True)
    (reports_dir / "financialdata_isin_supplements_review.json").write_text(
        '{"summary":{"generated_at":"2026-05-24T00:00:00Z"},"review_items":[{},{}]}',
        encoding="utf-8",
    )

    result = evaluate_campaign_artifact_integrity(
        {
            "_meta": CAMPAIGN_META_FIXTURE,
            "campaigns": [
                {
                    "campaign_key": "freshness",
                    "artifacts": [
                        {
                            "path": "data/reports/financialdata_isin_supplements_review.json",
                            "rows": 2,
                            "generated_at": "2026-05-24T00:00:00Z",
                        }
                    ],
                }
            ]
        },
        reports_dir=reports_dir,
    )

    assert result["passed"] is True
    assert result["checked_count"] == 1


def test_evaluate_campaign_artifact_integrity_fails_for_stale_row_counts(tmp_path) -> None:
    reports_dir = tmp_path / "data" / "reports"
    reports_dir.mkdir(parents=True)
    (reports_dir / "financialdata_isin_supplements_review.json").write_text(
        '{"summary":{"generated_at":"2026-05-24T00:00:00Z"},"review_items":[{},{}]}',
        encoding="utf-8",
    )

    result = evaluate_campaign_artifact_integrity(
        {
            "_meta": CAMPAIGN_META_FIXTURE,
            "campaigns": [
                {
                    "campaign_key": "freshness",
                    "artifacts": [
                        {
                            "path": "data/reports/financialdata_isin_supplements_review.json",
                            "rows": 0,
                            "generated_at": "2026-05-24T00:00:00Z",
                        }
                    ],
                }
            ]
        },
        reports_dir=reports_dir,
    )

    assert result["passed"] is False
    assert result["row_count_mismatches"] == [
        {
            "path": "data/reports/financialdata_isin_supplements_review.json",
            "campaign_rows": 0,
            "actual_rows": 2,
        }
    ]


def test_evaluate_campaign_artifact_integrity_fails_for_stale_generated_at(tmp_path) -> None:
    reports_dir = tmp_path / "data" / "reports"
    reports_dir.mkdir(parents=True)
    (reports_dir / "coverage_report.json").write_text(
        '{"_meta":{"generated_at":"2026-05-24T01:00:00Z"},"global":{}}',
        encoding="utf-8",
    )

    result = evaluate_campaign_artifact_integrity(
        {
            "_meta": CAMPAIGN_META_FIXTURE,
            "campaigns": [
                {
                    "campaign_key": "freshness",
                    "artifacts": [
                        {
                            "path": "data/reports/coverage_report.json",
                            "rows": 0,
                            "generated_at": "2026-05-24T00:00:00Z",
                        }
                    ],
                }
            ]
        },
        reports_dir=reports_dir,
    )

    assert result["passed"] is False
    assert result["generated_at_mismatches"] == [
        {
            "path": "data/reports/coverage_report.json",
            "campaign_generated_at": "2026-05-24T00:00:00Z",
            "actual_generated_at": "2026-05-24T01:00:00Z",
        }
    ]


def test_evaluate_campaign_artifact_integrity_fails_for_missing_generated_at(tmp_path) -> None:
    reports_dir = tmp_path / "data" / "reports"
    reports_dir.mkdir(parents=True)
    (reports_dir / "coverage_report.json").write_text('{"global":{}}', encoding="utf-8")

    result = evaluate_campaign_artifact_integrity(
        {
            "_meta": CAMPAIGN_META_FIXTURE,
            "campaigns": [
                {
                    "campaign_key": "freshness",
                    "artifacts": [{"path": "data/reports/coverage_report.json", "rows": 0, "generated_at": ""}],
                }
            ]
        },
        reports_dir=reports_dir,
    )

    assert result["passed"] is False
    assert result["missing_generated_at"] == ["data/reports/coverage_report.json"]


def test_render_markdown_surfaces_campaign_evidence_gap_counts() -> None:
    payload = {
        "_meta": {"generated_at": "2026-05-24T00:00:00Z"},
        "passed": False,
        "summary": {
            "criteria": 2,
            "passed_criteria": 1,
            "failed_criteria": 1,
            "validation_failed_error_gates": 0,
        },
        "summary_context": (
            "passed=false;criteria=2;passed_criteria=1;"
            "failed_criteria=1;validation_failed_error_gates=0"
        ),
        "campaign_status_rows": [
            {
                "priority": 1,
                "campaign_key": "b3",
                "status": "partial",
                "delta_status": "review_only",
                "next_action": "refresh official source",
            }
        ],
        "criteria": {
            "before_after_delta_matrix": {
                "passed": True,
                "acceptance_delta_matrix": {
                    key: {"baseline": 1, "current": 1, "delta": 0}
                    for key in REQUIRED_DELTA_KEYS
                },
            },
            "campaign_acceptance_matrices": {
                "passed": False,
                "b3_campaign_evidence_gaps": [],
                "otc_campaign_evidence_gaps": [{"field": "source_gap_rows"}],
            },
        },
    }

    markdown = render_markdown(payload)

    assert "## Campaign Status" in markdown
    assert "## Summary" in markdown
    assert (
        "Summary context: `passed=false;criteria=2;passed_criteria=1;"
        "failed_criteria=1;validation_failed_error_gates=0`"
    ) in markdown
    assert "| `criteria` | `2` |" in markdown
    assert "| `failed_criteria` | `1` |" in markdown
    assert "## Failed Criteria" in markdown
    assert "| `campaign_acceptance_matrices` |" in markdown
    assert "| 1 | `b3` | `partial` | `review_only` | refresh official source |" in markdown
    assert "## Campaign Evidence Gates" in markdown
    assert "| `b3_campaign_evidence_gaps` | 0 |" in markdown
    assert "| `otc_campaign_evidence_gaps` | 1 |" in markdown
    assert "`[{\"field\": \"source_gap_rows\"}]`" in markdown


def test_release_summary_context_is_stable_and_machine_readable() -> None:
    summary = {
        "criteria": 41,
        "passed_criteria": 41,
        "failed_criteria": 0,
        "validation_failed_error_gates": 0,
    }

    assert release_summary_context(True, summary) == (
        "passed=true;criteria=41;passed_criteria=41;"
        "failed_criteria=0;validation_failed_error_gates=0"
    )

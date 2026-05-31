from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = ROOT / "data" / "reports"

DEFAULT_JSON_OUT = REPORTS_DIR / "release_acceptance.json"
DEFAULT_MD_OUT = REPORTS_DIR / "release_acceptance.md"
REVIEW_ARTIFACT_SIBLING_STALE_GRACE_SECONDS = 60
COMMAND_SCRIPT_PATTERN = re.compile(r"\bpython\s+(scripts/[\w./-]+\.py)")

RELEASE_SOURCE_REPORTS = {
    "validation_report": "data/reports/validation_report.json",
    "entry_quality_gate": "data/reports/entry_quality_gate.json",
    "improvement_baseline": "data/reports/improvement_baseline.json",
    "improvement_deltas": "data/reports/improvement_deltas.json",
    "improvement_campaigns": "data/reports/improvement_campaigns.json",
    "symbol_changes_review": "data/reports/symbol_changes_review.json",
    "ohlcv_plausibility": "data/reports/ohlcv_plausibility.json",
    "ohlcv_warning_review": "data/reports/ohlcv_warning_review.json",
    "masterfile_collision_review": "data/reports/masterfile_collision_review.json",
    "otc_scope_review": "data/reports/otc_scope_review.json",
    "canada_residual_review": "data/reports/canada_residual_review.json",
    "canada_figi_queue": "data/reports/canada_figi_queue.json",
    "canada_figi_apply_report": "data/reports/canada_figi_apply_report.json",
    "b3_residual_isin_review": "data/reports/b3_residual_isin_review.json",
    "b3_residual_sector_review": "data/reports/b3_residual_sector_review.json",
    "asx_residual_review": "data/reports/asx_residual_review.json",
    "weak_sector_residual_review": "data/reports/weak_sector_residual_review.json",
    "adanos_alias_audit": "data/reports/adanos_alias_audit.json",
    "adanos_detection_simulation": "data/reports/adanos_detection_simulation.json",
}

REQUIRED_GATE_GROUPS = {
    "no_invalid_isins": ["invalid_isin_rows"],
    "no_duplicate_primary_tickers": ["duplicate_primary_ticker_count"],
    "no_duplicate_listing_keys": ["duplicate_listing_key_count"],
    "no_duplicate_public_aliases": ["duplicate_public_alias_count"],
    "no_mojibake_names": ["rows_with_mojibake_names"],
    "no_unreviewed_country_isin_conflicts": ["country_isin_prefix_mismatch_without_review"],
    "adanos_alias_safety": [
        "adanos_alias_findings",
        "adanos_alias_parse_errors",
        "adanos_alias_common_word_count",
        "review_alias_removals_open_count",
    ],
    "source_gap_review_integrity": [
        "source_gap_classification_invalid_rows",
        "source_gap_classification_current_gap_mismatch",
        "source_of_truth_decision_invalid_rows",
        "source_of_truth_decision_duplicate_keys",
        "source_of_truth_decision_gap_mismatch",
        "source_of_truth_decision_class_mismatch",
    ],
    "entry_quality_release_gate": [
        "entry_quality_quarantine_count",
        "entry_quality_unexpected_warn_count",
    ],
}

REQUIRED_DELTA_KEYS = [
    "isin_delta",
    "sector_delta",
    "category_delta",
    "source_gap_delta",
    "warn_delta",
    "quarantine_delta",
]
EXPECTED_BASELINE_SOURCE_FILES = {
    "coverage_report": "data/reports/coverage_report.json",
    "source_gap_classification_json": "data/reports/source_gap_classification.json",
    "source_gap_classification_csv": "data/reports/source_gap_classification.csv",
    "entry_quality_json": "data/reports/entry_quality.json",
    "entry_quality_csv": "data/reports/entry_quality.csv",
    "validation_report": "data/reports/validation_report.json",
    "b3_residual_isin_review": "data/reports/b3_residual_isin_review.json",
    "b3_residual_sector_review": "data/reports/b3_residual_sector_review.json",
    "otc_scope_review": "data/reports/otc_scope_review.json",
    "canada_residual_review": "data/reports/canada_residual_review.json",
    "canada_figi_queue": "data/reports/canada_figi_queue.json",
    "asx_residual_review": "data/reports/asx_residual_review.json",
    "weak_sector_residual_review": "data/reports/weak_sector_residual_review.json",
    "masterfile_collision_review": "data/reports/masterfile_collision_review.json",
    "symbol_changes_review": "data/reports/symbol_changes_review.json",
    "ohlcv_plausibility": "data/reports/ohlcv_plausibility.json",
    "ohlcv_warning_review": "data/reports/ohlcv_warning_review.json",
    "financialdata_isin_supplements_review": "data/reports/financialdata_isin_supplements_review.json",
}
EXPECTED_DELTA_SOURCE_FILES = {
    "baseline_snapshot": "data/reports/improvement_baseline.json",
    "current_snapshot_coverage_report": "data/reports/coverage_report.json",
    "current_snapshot_source_gap_classification_json": "data/reports/source_gap_classification.json",
    "current_snapshot_source_gap_classification_csv": "data/reports/source_gap_classification.csv",
    "current_snapshot_entry_quality_json": "data/reports/entry_quality.json",
    "current_snapshot_entry_quality_csv": "data/reports/entry_quality.csv",
    "current_snapshot_validation_report": "data/reports/validation_report.json",
    "current_snapshot_b3_residual_isin_review": "data/reports/b3_residual_isin_review.json",
    "current_snapshot_b3_residual_sector_review": "data/reports/b3_residual_sector_review.json",
    "current_snapshot_otc_scope_review": "data/reports/otc_scope_review.json",
    "current_snapshot_canada_residual_review": "data/reports/canada_residual_review.json",
    "current_snapshot_canada_figi_queue": "data/reports/canada_figi_queue.json",
    "current_snapshot_asx_residual_review": "data/reports/asx_residual_review.json",
    "current_snapshot_weak_sector_residual_review": "data/reports/weak_sector_residual_review.json",
    "current_snapshot_masterfile_collision_review": "data/reports/masterfile_collision_review.json",
    "current_snapshot_symbol_changes_review": "data/reports/symbol_changes_review.json",
    "current_snapshot_ohlcv_plausibility": "data/reports/ohlcv_plausibility.json",
    "current_snapshot_ohlcv_warning_review": "data/reports/ohlcv_warning_review.json",
    "current_snapshot_financialdata_isin_supplements_review": "data/reports/financialdata_isin_supplements_review.json",
}
REQUIRED_COVERAGE_FRESHNESS_KEYS = (
    "tickers_built_at",
    "tickers_age_hours",
    "masterfiles_generated_at",
    "masterfiles_age_hours",
    "identifiers_generated_at",
    "identifiers_age_hours",
    "listing_history_observed_at",
    "listing_history_age_hours",
    "latest_verification_run",
    "latest_verification_generated_at",
    "latest_verification_age_hours",
    "latest_stock_verification_run",
    "latest_stock_verification_generated_at",
    "latest_stock_verification_age_hours",
    "latest_etf_verification_run",
    "latest_etf_verification_generated_at",
    "latest_etf_verification_age_hours",
    "symbol_changes_generated_at",
    "symbol_changes_age_hours",
    "symbol_changes_review_rows",
    "entry_quality_generated_at",
    "entry_quality_age_hours",
    "entry_quality_rows",
    "masterfile_collision_review_generated_at",
    "masterfile_collision_review_age_hours",
    "masterfile_collision_review_rows",
    "ohlcv_plausibility_generated_at",
    "ohlcv_plausibility_age_hours",
    "ohlcv_plausibility_rows",
    "source_gap_classification_generated_at",
    "source_gap_classification_age_hours",
    "source_gap_classification_rows",
)
EXPECTED_COVERAGE_SOURCE_FILES = {
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
}
REQUIRED_SOURCE_FRESHNESS_ROW_KEYS = (
    "key",
    "provider",
    "reference_scope",
    "official",
    "mode",
    "rows",
    "generated_at",
    "age_hours",
    "age_bucket",
    "freshness_status",
    "refresh_priority",
    "refresh_queue",
    "review_strategy",
    "evidence_required",
    "recommended_next_source",
    "source_gate",
    "recommended_refresh_action",
    "source_artifact_context",
    "freshness_review_context",
    "refresh_gate_context",
)
REQUIRED_SOURCE_FRESHNESS_SUMMARY_KEYS = (
    "source_count",
    "freshness_status_totals",
    "source_age_bucket_totals",
    "refresh_priority_totals",
    "refresh_queue_totals",
    "refresh_queue_scope_totals",
    "refresh_queue_mode_totals",
    "refresh_queue_priority_totals",
    "refresh_queue_age_bucket_totals",
    "recommended_refresh_action_totals",
    "refresh_queue_review_strategy_totals",
    "refresh_queue_evidence_required_totals",
    "top_source_refresh_batches",
    "old_official_exchange_directory_count",
    "top_old_official_exchange_directories",
)
REQUIRED_B3_MASTERFILE_DIAGNOSTIC_KEYS = (
    "policy",
    "dataset_rows",
    "active_exchange_directory_rows",
    "all_b3_masterfile_rows",
    "matched_dataset_rows",
    "missing_dataset_rows",
    "official_any_source_matched_dataset_rows",
    "official_any_source_missing_dataset_rows",
    "official_any_source_match_rate",
    "official_active_symbols_not_in_dataset",
    "dataset_match_rate",
    "active_source_key_totals",
    "all_source_key_totals",
    "missing_category_totals",
    "missing_asset_type_totals",
    "missing_source_presence_totals",
    "missing_examples",
)
FRESHNESS_CAMPAIGN_EVIDENCE_KEYS = (
    "source_gap_rows",
    "coverage_freshness_keys",
    "freshness_snapshot",
    "freshness_snapshot_age_bucket_totals",
    "top_freshness_snapshot_ages",
    "source_freshness_status_totals",
    "source_age_bucket_totals",
    "source_refresh_priority_totals",
    "source_refresh_queue_totals",
    "source_refresh_queue_scope_totals",
    "source_refresh_queue_mode_totals",
    "source_refresh_queue_priority_totals",
    "source_refresh_queue_age_bucket_totals",
    "source_refresh_action_totals",
    "source_refresh_queue_review_strategy_totals",
    "source_refresh_queue_evidence_required_totals",
    "top_source_refresh_batches",
    "old_official_exchange_directory_count",
    "top_old_official_exchange_directories",
    "source_gap_class_totals",
    "top_source_gap_review_batches",
    "financialdata_supplement_rows",
    "financialdata_apply_eligibility_counts",
    "financialdata_verification_evidence_required_counts",
    "top_financialdata_supplement_review_batches",
)
FRESHNESS_CAMPAIGN_DELTA_EVIDENCE_KEYS = (
    "coverage_freshness_keys",
    "freshness_snapshot",
    "freshness_snapshot_age_bucket_totals",
    "top_freshness_snapshot_ages",
    "source_freshness_status_totals",
    "source_age_bucket_totals",
    "source_refresh_priority_totals",
    "source_refresh_queue_totals",
    "source_refresh_queue_scope_totals",
    "source_refresh_queue_mode_totals",
    "source_refresh_queue_priority_totals",
    "source_refresh_queue_age_bucket_totals",
    "source_refresh_action_totals",
    "source_refresh_queue_review_strategy_totals",
    "source_refresh_queue_evidence_required_totals",
    "old_official_exchange_directory_count",
    "top_source_gap_review_batches",
    "financialdata_supplement_rows",
    "top_financialdata_supplement_review_batches",
)

SOURCE_REFRESH_REVIEW_STRATEGIES = {
    "capture_source_generated_at_before_refresh_decision",
    "manual_source_refresh_review_required",
    "no_refresh_required",
    "refresh_official_exchange_directory_before_identity_or_collision_work",
    "refresh_official_subset_before_gap_enrichment",
    "restore_or_replace_unavailable_source_before_data_fill",
    "review_secondary_source_freshness_or_replace",
}


def source_refresh_strategy_for(queue: str) -> tuple[str, str]:
    if queue == "fresh_no_refresh_needed":
        return ("no_refresh_required", "fresh_source_generated_at_with_age_under_48h")
    if queue == "refresh_official_exchange_directory_before_identity_or_collision_work":
        return (
            "refresh_official_exchange_directory_before_identity_or_collision_work",
            "official_exchange_directory_refresh_artifact_with_generated_at_and_row_count",
        )
    if queue == "refresh_official_subset_before_gap_enrichment":
        return (
            "refresh_official_subset_before_gap_enrichment",
            "official_subset_refresh_artifact_with_generated_at_scope_and_row_count",
        )
    if queue == "restore_or_replace_unavailable_source_before_data_fill":
        return (
            "restore_or_replace_unavailable_source_before_data_fill",
            "source_restored_or_replaced_with_official_or_documented_unavailable_decision",
        )
    if queue == "capture_source_generated_at_before_refresh_decision":
        return (
            "capture_source_generated_at_before_refresh_decision",
            "source_generated_at_or_fetch_timestamp_required",
        )
    if queue == "review_secondary_source_freshness_or_replace":
        return (
            "review_secondary_source_freshness_or_replace",
            "reviewed_secondary_source_freshness_or_replacement_decision",
        )
    return ("manual_source_refresh_review_required", "manual_review_required")


def source_artifact_context(row: dict[str, Any]) -> str:
    return (
        f"key={row.get('key', '') or 'none'};"
        f"provider={row.get('provider', '') or 'none'};"
        f"reference_scope={row.get('reference_scope', '') or 'none'};"
        f"official={str(row.get('official', '')).lower() or 'none'};"
        f"mode={row.get('mode', '') or 'none'};"
        f"rows={row.get('rows', 0)};"
        f"last_error={row.get('last_error', '') or 'none'}"
    )


def source_freshness_review_context(row: dict[str, Any]) -> str:
    return (
        f"generated_at={row.get('generated_at', '') or 'none'};"
        f"age_bucket={row.get('age_bucket', '') or 'none'};"
        f"freshness_status={row.get('freshness_status', '') or 'none'};"
        f"refresh_priority={row.get('refresh_priority', '') or 'none'}"
    )


def source_refresh_gate_context(row: dict[str, Any]) -> str:
    return (
        f"refresh_queue={row.get('refresh_queue', '') or 'none'};"
        f"recommended_refresh_action={row.get('recommended_refresh_action', '') or 'none'};"
        f"review_strategy={row.get('review_strategy', '') or 'none'};"
        f"evidence_required={row.get('evidence_required', '') or 'none'}"
    )


BASELINE_CAMPAIGN_EVIDENCE_KEYS = (
    "baseline_global_metrics",
    "baseline_campaigns",
    "changed_numeric_delta_rows",
)
BASELINE_CAMPAIGN_DELTA_EVIDENCE_KEYS = (
    "baseline_generated_at",
    "future_delta_reference",
    "current_delta_report",
)
REQUIRED_B3_MASTERFILE_EXAMPLE_KEYS = (
    "listing_key",
    "ticker",
    "asset_type",
    "name",
    "source_presence",
    "candidate_sources",
)
REQUIRED_SOURCE_GAP_ROW_KEYS = (
    "listing_key",
    "ticker",
    "exchange",
    "field",
    "target_field",
    "gap_class",
    "review_needed",
    "confidence_policy",
    "recommended_next_source",
    "source_gate",
    "source_gap_context",
    "classification_context",
    "evidence_gate_context",
)
REQUIRED_SOURCE_GAP_SUMMARY_KEYS = (
    "rows",
    "field_totals",
    "class_totals",
    "class_by_field",
    "top_exchanges_by_field",
    "top_source_gap_review_batches",
    "policy",
)
REQUIRED_SOURCE_GAP_REVIEW_BATCH_KEYS = (
    "field",
    "gap_class",
    "exchange",
    "rows",
    "recommended_next_source",
    "source_gate",
)
SOURCE_GAP_FIELD_VALUES = {"missing_etf_category", "missing_isin_primary", "missing_sector_stock"}
REQUIRED_SYMBOL_CHANGE_ROW_KEYS = (
    "change_id",
    "effective_date",
    "old_symbol",
    "new_symbol",
    "source",
    "source_confidence",
    "review_needed",
    "match_status",
    "symbol_change_workflow_queue",
    "review_bucket",
    "review_priority",
    "review_strategy",
    "recency_bucket",
    "apply_eligibility",
    "verification_evidence_required",
    "recommended_next_source",
    "source_gate",
    "recommended_action",
    "exchange_scope_status",
    "listing_key_review_status",
    "issuer_name_review_status",
    "source_review_context",
    "scope_match_context",
    "workflow_review_context",
)
REQUIRED_SYMBOL_CHANGE_SUMMARY_KEYS = (
    "review_rows",
    "match_status_counts",
    "symbol_change_workflow_queue_counts",
    "symbol_change_backlog",
    "review_bucket_counts",
    "review_priority_counts",
    "review_bucket_priorities",
    "recency_bucket_counts",
    "review_priority_recency_counts",
    "workflow_queue_recency_counts",
    "workflow_queue_priority_counts",
    "workflow_queue_scope_counts",
    "workflow_queue_match_status_counts",
    "workflow_queue_source_hint_counts",
    "workflow_queue_source_confidence_counts",
    "workflow_queue_issuer_name_review_counts",
    "workflow_queue_listing_key_review_counts",
    "workflow_queue_review_strategy_counts",
    "top_symbol_change_workflow_batches",
    "apply_eligibility_counts",
    "symbol_change_apply_readiness_counts",
    "verification_evidence_required_counts",
    "time_sensitive_workflow_queue_counts",
    "time_sensitive_recency_counts",
    "time_sensitive_apply_readiness_counts",
    "top_time_sensitive_symbol_change_batches",
    "recommended_action_counts",
    "exchange_scope_status_counts",
)
SYMBOL_CHANGE_MATCH_STATUSES = {
    "informational_no_old_symbol",
    "new_symbol_present_old_symbol_missing",
    "no_matching_listing",
    "old_and_new_symbols_present",
    "old_symbol_present_new_symbol_missing",
    "symbol_present_only_outside_source_scope",
}
SYMBOL_CHANGE_REVIEW_BUCKETS = {
    "action_required_duplicate_or_cross_listing",
    "action_required_possible_rename_or_delisting",
    "already_reflected_in_scope_with_global_symbol_collision",
    "already_reflected_in_source_scope",
    "hold_out_of_scope_symbol_collision",
    "manual_review_due_to_out_of_scope_collision",
    "manual_review_required",
    "manual_scope_mapping_required",
    "no_dataset_match_for_source_scope",
}
SYMBOL_CHANGE_WORKFLOW_QUEUES = {
    "audit_already_reflected",
    "blocked_missing_source_scope_mapping",
    "blocked_out_of_scope_symbol_collision",
    "document_no_dataset_match",
    "manual_review_required",
    "review_duplicate_or_cross_listing",
    "review_verified_rename_or_delisting",
}
SYMBOL_CHANGE_BUCKET_TO_WORKFLOW_QUEUE = {
    "action_required_duplicate_or_cross_listing": "review_duplicate_or_cross_listing",
    "action_required_possible_rename_or_delisting": "review_verified_rename_or_delisting",
    "already_reflected_in_scope_with_global_symbol_collision": "audit_already_reflected",
    "already_reflected_in_source_scope": "audit_already_reflected",
    "hold_out_of_scope_symbol_collision": "blocked_out_of_scope_symbol_collision",
    "manual_review_due_to_out_of_scope_collision": "blocked_out_of_scope_symbol_collision",
    "manual_review_required": "manual_review_required",
    "manual_scope_mapping_required": "blocked_missing_source_scope_mapping",
    "no_dataset_match_for_source_scope": "document_no_dataset_match",
}
SYMBOL_CHANGE_PRIORITIES = {"P1", "P2", "P3", "P4"}
SYMBOL_CHANGE_RECENCY_BUCKETS = {
    "older_than_90d",
    "recent_30d",
    "recent_7d",
    "recent_90d",
    "unknown_or_future_effective_date",
}
SYMBOL_CHANGE_APPLY_ELIGIBILITIES = {
    "audit_only_no_apply",
    "blocked_until_exchange_scope_resolved",
    "manual_review_required",
    "no_dataset_action_without_scope_mapping",
    "requires_official_venue_confirmation",
}
SYMBOL_CHANGE_APPLY_READINESS = {
    "audit_only_no_canonical_change",
    "blocked_until_listing_keyed_official_symbol_change_evidence",
    "blocked_until_source_exchange_scope_and_non_symbol_identity_evidence",
    "document_or_ignore_until_scoped_official_dataset_match",
    "manual_review_required_no_apply",
}
SYMBOL_CHANGE_EXCHANGE_SCOPE_STATUSES = {
    "global_symbol_collision_outside_source_scope",
    "matches_within_source_scope",
    "unscoped_source_hint",
}
SYMBOL_CHANGE_LISTING_KEY_REVIEW_STATUSES = {
    "new_scoped_listing_key_only",
    "no_scoped_listing_key_match",
    "old_and_new_scoped_listing_keys_present",
    "old_scoped_listing_key_only",
}
SYMBOL_CHANGE_REVIEW_STRATEGIES = {
    "audit_already_reflected_no_canonical_change",
    "block_until_source_scope_and_non_symbol_identity_resolved",
    "document_no_dataset_match_without_canonical_action",
    "manual_symbol_change_review_required",
    "map_source_exchange_scope_before_symbol_review",
    "resolve_duplicate_cross_listing_or_transition_before_any_symbol_change",
    "verify_rename_or_delisting_with_official_venue_or_issuer_evidence",
}
EXPECTED_SYMBOL_CHANGE_SOURCE_FILES = {
    "listings_csv": "data/listings.csv",
    "changes_csv": "data/corporate_actions/symbol_changes.csv",
}
SYMBOL_CHANGE_CAMPAIGN_EVIDENCE_KEYS = (
    "symbol_change_review_rows",
    "match_status_counts",
    "symbol_change_workflow_queue_counts",
    "symbol_change_backlog",
    "review_bucket_counts",
    "review_priority_counts",
    "review_bucket_priorities",
    "recency_bucket_counts",
    "review_priority_recency_counts",
    "workflow_queue_recency_counts",
    "workflow_queue_priority_counts",
    "workflow_queue_scope_counts",
    "workflow_queue_match_status_counts",
    "workflow_queue_source_hint_counts",
    "workflow_queue_source_confidence_counts",
    "workflow_queue_issuer_name_review_counts",
    "workflow_queue_listing_key_review_counts",
    "workflow_queue_review_strategy_counts",
    "top_symbol_change_workflow_batches",
    "apply_eligibility_counts",
    "verification_evidence_required_counts",
    "recommended_action_counts",
    "time_sensitive_review_rows",
    "time_sensitive_workflow_queue_counts",
    "time_sensitive_recency_counts",
    "top_time_sensitive_symbol_change_batches",
    "exchange_scope_status_counts",
)
SYMBOL_CHANGE_CAMPAIGN_DELTA_EVIDENCE_KEYS = (
    "source_scope_outside_collision_rows",
    "verified_rename_delta",
    "duplicate_resolution_delta",
    "warn_quarantine_delta",
)


def symbol_change_workflow_strategy_for(queue: str) -> tuple[str, str]:
    if queue == "review_verified_rename_or_delisting":
        return (
            "verify_rename_or_delisting_with_official_venue_or_issuer_evidence",
            "official_exchange_notice_or_current_directory_showing_old_symbol_inactive_new_symbol_active_same_issuer",
        )
    if queue == "review_duplicate_or_cross_listing":
        return (
            "resolve_duplicate_cross_listing_or_transition_before_any_symbol_change",
            "official_exchange_directory_plus_listing_key_review_to_distinguish_duplicate_cross_listing_or_transition",
        )
    if queue == "audit_already_reflected":
        return (
            "audit_already_reflected_no_canonical_change",
            "audit_only_confirm_no_canonical_change_needed",
        )
    if queue == "blocked_out_of_scope_symbol_collision":
        return (
            "block_until_source_scope_and_non_symbol_identity_resolved",
            "official_exchange_scope_and_non_symbol_identity_evidence_before_apply",
        )
    if queue == "blocked_missing_source_scope_mapping":
        return (
            "map_source_exchange_scope_before_symbol_review",
            "source_exchange_mapping_before_any_symbol_change_review",
        )
    if queue == "document_no_dataset_match":
        return (
            "document_no_dataset_match_without_canonical_action",
            "official_exchange_scope_mapping_or_ignore_as_external_non_dataset_event",
        )
    return ("manual_symbol_change_review_required", "manual_review_required")


def symbol_change_workflow_recommended_next_source_for(queue: str, exchange_scope_status: str) -> str:
    if queue == "review_verified_rename_or_delisting":
        return "Official exchange notice, issuer notice, or current exchange directory proving old/new symbols for the same issuer."
    if queue == "review_duplicate_or_cross_listing":
        return "Official exchange directory records plus listing-key review for both symbols."
    if queue == "blocked_out_of_scope_symbol_collision":
        return "Official source exchange scope mapping plus non-symbol identity evidence before any symbol action."
    if queue == "blocked_missing_source_scope_mapping":
        return "Documented source-to-exchange scope mapping before symbol-change review."
    if queue == "document_no_dataset_match":
        return "Official exchange scope mapping, or document the event as outside the dataset."
    if queue == "audit_already_reflected":
        if exchange_scope_status == "global_symbol_collision_outside_source_scope":
            return "Audit-only comparison against official scoped exchange evidence; no canonical change."
        return "Audit-only confirmation from scoped listing records; no canonical change."
    return "Manual official symbol-change evidence."


def symbol_change_workflow_source_gate_for(queue: str) -> str:
    if queue == "review_verified_rename_or_delisting":
        return "Do not rename until official listing-keyed evidence proves old inactive and new active for the same issuer."
    if queue == "review_duplicate_or_cross_listing":
        return "Do not change symbols until duplicate, cross-listing, or transition state is resolved listing-key by listing-key."
    if queue == "blocked_out_of_scope_symbol_collision":
        return "Block apply; global symbol collision outside source scope is not symbol-change evidence."
    if queue == "blocked_missing_source_scope_mapping":
        return "Block review until the secondary feed event is mapped to an exchange scope."
    if queue == "document_no_dataset_match":
        return "No dataset action without scoped official mapping to an existing or intended listing."
    if queue == "audit_already_reflected":
        return "Audit only; no ticker, listing, or name change is authorized."
    return "Manual review required; secondary-feed evidence alone is insufficient."


def symbol_change_apply_readiness_for(apply_eligibility: str) -> str:
    if apply_eligibility == "requires_official_venue_confirmation":
        return "blocked_until_listing_keyed_official_symbol_change_evidence"
    if apply_eligibility == "blocked_until_exchange_scope_resolved":
        return "blocked_until_source_exchange_scope_and_non_symbol_identity_evidence"
    if apply_eligibility == "no_dataset_action_without_scope_mapping":
        return "document_or_ignore_until_scoped_official_dataset_match"
    if apply_eligibility == "audit_only_no_apply":
        return "audit_only_no_canonical_change"
    return "manual_review_required_no_apply"


def symbol_change_source_review_context(row: dict[str, Any]) -> str:
    return (
        f"source={row.get('source', '') or 'none'};"
        f"source_exchange_hint={row.get('source_exchange_hint', '') or 'none'};"
        f"source_confidence={row.get('source_confidence', '') or 'none'};"
        f"source_url={row.get('source_url', '') or 'none'}"
    )


def symbol_change_scope_match_context(row: dict[str, Any]) -> str:
    return (
        f"exchange_scope_status={row.get('exchange_scope_status', '') or 'none'};"
        f"old_match_count={row.get('old_match_count', 0)};"
        f"new_match_count={row.get('new_match_count', 0)};"
        f"old_scoped_match_count={row.get('old_scoped_match_count', 0)};"
        f"new_scoped_match_count={row.get('new_scoped_match_count', 0)};"
        f"old_scoped_listing_keys={row.get('old_scoped_listing_keys', '') or 'none'};"
        f"new_scoped_listing_keys={row.get('new_scoped_listing_keys', '') or 'none'}"
    )


def symbol_change_workflow_review_context(row: dict[str, Any]) -> str:
    return (
        f"workflow_queue={row.get('symbol_change_workflow_queue', '') or 'none'};"
        f"review_bucket={row.get('review_bucket', '') or 'none'};"
        f"apply_eligibility={row.get('apply_eligibility', '') or 'none'};"
        f"verification_evidence_required={row.get('verification_evidence_required', '') or 'none'}"
    )


REQUIRED_OHLCV_ROW_KEYS = (
    "listing_key",
    "ticker",
    "exchange",
    "name",
    "isin",
    "entry_quality_status",
    "selection_bucket",
    "plausibility_status",
    "plausibility_score",
    "review_bucket",
    "review_priority",
    "sampling_strategy",
    "plausibility_use",
    "verification_evidence_required",
    "recommended_next_source",
    "source_gate",
    "selection_context",
    "ohlcv_sample_context",
    "plausibility_review_context",
    "ohlcv_symbol",
    "recommended_action",
    "issues",
)
REQUIRED_OHLCV_SUMMARY_KEYS = (
    "status_counts",
    "issue_counts",
    "selection_bucket_counts",
    "selection_bucket_exchange_counts",
    "selection_bucket_status_counts",
    "review_bucket_counts",
    "review_bucket_selection_bucket_counts",
    "review_bucket_exchange_counts",
    "review_bucket_sampling_strategy_counts",
    "review_bucket_sampling_readiness_counts",
    "top_ohlcv_sampling_batches",
    "ohlcv_sampling_backlog",
    "review_priority_counts",
    "plausibility_use_counts",
    "canonical_data_change_authorization_counts",
    "verification_evidence_required_counts",
    "source_gap_class_counts",
    "sampling_coverage",
    "top_flagged_exchanges",
)
OHLCV_PLAUSIBILITY_STATUSES = {"pass", "warn", "fail", "not_checked", "notice", "source_gap"}
OHLCV_REVIEW_PRIORITIES = {"P1", "P2", "P3", "P4"}
OHLCV_SAMPLING_STRATEGIES = {
    "collect_ohlcv_sample_for_large_exchange_baseline",
    "collect_ohlcv_sample_then_existing_entry_quality_review",
    "collect_ohlcv_sample_then_source_gap_review",
    "manual_ohlcv_sampling_review_required",
    "resolve_market_data_source_gap_before_interpreting_listing",
    "retain_as_plausibility_baseline_no_data_change",
    "review_checked_ohlcv_anomaly_against_listing_status_and_corporate_actions",
    "review_low_severity_market_data_notice_only_if_prioritized",
}
OHLCV_SAMPLING_READINESS_STATUSES = {
    "checked_local_sample",
    "checked_without_source",
    "checked_yahoo_sample",
    "needs_ohlcv_sample",
}
OHLCV_CANONICAL_AUTHORIZATION_STATUSES = {
    "no_canonical_data_change_authorized",
    "official_listing_review_required_before_any_canonical_change",
}
OHLCV_ALLOWED_AUTO_CHANGE_EVIDENCE = {"none_no_database_change_authorized"}
OHLCV_FORBIDDEN_AUTOMATIC_ACTION_MARKERS = (
    "apply",
    "backfill",
    "change_ticker",
    "fill_isin",
    "fill_sector",
    "fill_category",
    "rename",
    "update_name",
)
EXPECTED_OHLCV_SOURCE_FILES = {
    "listings_csv": "data/listings.csv",
    "entry_quality_csv": "data/reports/entry_quality.csv",
    "source_gap_classification_csv": "data/reports/source_gap_classification.csv",
}
OHLCV_CAMPAIGN_EVIDENCE_KEYS = (
    "ohlcv_rows",
    "selected_sample_rows",
    "checked_sample_rows",
    "not_checked_sample_rows",
    "sampling_coverage",
    "status_counts",
    "issue_counts",
    "selection_bucket_counts",
    "selection_bucket_exchange_counts",
    "selection_bucket_status_counts",
    "review_bucket_counts",
    "review_bucket_selection_bucket_counts",
    "review_bucket_exchange_counts",
    "review_bucket_sampling_strategy_counts",
    "review_bucket_sampling_readiness_counts",
    "top_ohlcv_sampling_batches",
    "ohlcv_sampling_backlog",
    "review_priority_counts",
    "plausibility_use_counts",
    "canonical_data_change_authorization_counts",
    "verification_evidence_required_counts",
    "source_gap_class_counts",
    "top_flagged_exchanges",
)
OHLCV_CAMPAIGN_DELTA_EVIDENCE_KEYS = (
    "selected_sample_rows",
    "checked_sample_rows",
    "warn_or_source_gap_signal_rows",
)


def ohlcv_review_priority_for(review_bucket: str) -> str:
    if review_bucket == "checked_ohlcv_anomaly_requires_listing_review":
        return "P1"
    if review_bucket in {
        "checked_market_data_source_gap",
        "not_checked_entry_quality_warn_sample",
    }:
        return "P2"
    if review_bucket in {
        "checked_low_severity_market_data_notice",
        "not_checked_source_gap_cluster_sample",
    }:
        return "P3"
    return "P4"


def ohlcv_sampling_strategy_for(review_bucket: str) -> tuple[str, str]:
    if review_bucket == "checked_ohlcv_anomaly_requires_listing_review":
        return (
            "review_checked_ohlcv_anomaly_against_listing_status_and_corporate_actions",
            "official_listing_status_corporate_action_and_market_data_source_review_before_any_listing_change",
        )
    if review_bucket == "checked_market_data_source_gap":
        return (
            "resolve_market_data_source_gap_before_interpreting_listing",
            "alternate_market_data_source_or_official_listing_status_before_interpreting_gap",
        )
    if review_bucket == "checked_low_severity_market_data_notice":
        return (
            "review_low_severity_market_data_notice_only_if_prioritized",
            "market_data_provider_review_if_prioritizing_quality_cleanup",
        )
    if review_bucket == "checked_plausible_sample":
        return ("retain_as_plausibility_baseline_no_data_change", "none_no_database_change_authorized")
    if review_bucket == "not_checked_entry_quality_warn_sample":
        return (
            "collect_ohlcv_sample_then_existing_entry_quality_review",
            "local_or_bounded_network_ohlcv_sample_then_existing_entry_quality_review",
        )
    if review_bucket == "not_checked_source_gap_cluster_sample":
        return (
            "collect_ohlcv_sample_then_source_gap_review",
            "local_or_bounded_network_ohlcv_sample_then_source_gap_review",
        )
    if review_bucket == "not_checked_large_exchange_baseline_sample":
        return (
            "collect_ohlcv_sample_for_large_exchange_baseline",
            "local_or_bounded_network_ohlcv_sample_for_baseline_only",
        )
    return ("manual_ohlcv_sampling_review_required", "manual_review_required")


def ohlcv_recommended_next_source_for(review_bucket: str, exchange: str) -> str:
    if review_bucket == "checked_ohlcv_anomaly_requires_listing_review":
        return f"Official listing status, corporate-action evidence, and independent market-data sample for {exchange}."
    if review_bucket == "checked_market_data_source_gap":
        return f"Alternate market-data source or official listing-status evidence for {exchange}."
    if review_bucket == "checked_low_severity_market_data_notice":
        return f"Reviewed market-data provider sample for {exchange} if this quality issue is prioritized."
    if review_bucket == "checked_plausible_sample":
        return f"Retain sampled OHLCV evidence for {exchange} as a plausibility baseline only."
    if review_bucket == "not_checked_entry_quality_warn_sample":
        return f"Collect a local or bounded-network OHLCV sample for {exchange}, then review the existing entry-quality warning."
    if review_bucket == "not_checked_source_gap_cluster_sample":
        return f"Collect a local or bounded-network OHLCV sample for {exchange}, then use it only as source-gap review context."
    if review_bucket == "not_checked_large_exchange_baseline_sample":
        return f"Collect a local or bounded-network OHLCV baseline sample for {exchange}."
    return f"Manual OHLCV sampling review for {exchange}."


def ohlcv_source_gate_for(review_bucket: str) -> str:
    if review_bucket == "checked_ohlcv_anomaly_requires_listing_review":
        return "Do not change listing data until official listing status and corporate-action evidence explain the anomaly."
    if review_bucket == "checked_market_data_source_gap":
        return "Do not interpret a market-data gap as a listing problem without an alternate source or official status check."
    if review_bucket == "checked_low_severity_market_data_notice":
        return "Treat as market-data quality context only; no canonical data change is authorized."
    if review_bucket == "checked_plausible_sample":
        return "Plausible OHLCV sample is baseline evidence only; no database change is authorized."
    if review_bucket == "not_checked_entry_quality_warn_sample":
        return "Sampling can prioritize review, but entry-quality changes still require the existing official evidence gates."
    if review_bucket == "not_checked_source_gap_cluster_sample":
        return "Sampling can prioritize source-gap review, but cannot fill identifiers, sectors, categories, names, or symbols."
    if review_bucket == "not_checked_large_exchange_baseline_sample":
        return "Baseline sampling is monitoring evidence only; it does not authorize canonical data changes."
    return "Manual review required; OHLCV plausibility alone is never sufficient for canonical data changes."


def ohlcv_selection_context(row: dict[str, Any]) -> str:
    return (
        f"selection_bucket={row.get('selection_bucket', '') or 'none'};"
        f"entry_quality_status={row.get('entry_quality_status', '') or 'none'};"
        f"source_gap_field={row.get('source_gap_field', '') or 'none'};"
        f"source_gap_class={row.get('source_gap_class', '') or 'none'}"
    )


def ohlcv_sample_context(row: dict[str, Any]) -> str:
    issues = row.get("issues", [])
    issue_count = len(issues) if isinstance(issues, list) else 0
    return (
        f"ohlcv_source={row.get('ohlcv_source', '') or 'none'};"
        f"ohlcv_symbol={row.get('ohlcv_symbol', '') or 'none'};"
        f"bar_count={row.get('bar_count', 0)};"
        f"first_bar_date={row.get('first_bar_date', '') or 'none'};"
        f"last_bar_date={row.get('last_bar_date', '') or 'none'};"
        f"issue_count={issue_count}"
    )


def ohlcv_plausibility_review_context(row: dict[str, Any]) -> str:
    return (
        f"plausibility_status={row.get('plausibility_status', '') or 'none'};"
        f"review_bucket={row.get('review_bucket', '') or 'none'};"
        f"sampling_strategy={row.get('sampling_strategy', '') or 'none'};"
        f"verification_evidence_required={row.get('verification_evidence_required', '') or 'none'}"
    )


def canada_figi_apply_openfigi_probe_context(row: dict[str, Any]) -> str:
    return (
        f"requested_exchange_hint={row.get('requested_exchange_hint', '') or 'none'};"
        f"openfigi_exch_code={row.get('openfigi_exch_code', '') or 'none'};"
        f"openfigi_figi_present={'true' if row.get('figi') else 'false'};"
        f"candidate_count={row.get('openfigi_candidate_count', '') or 'none'}"
    )


def canada_figi_apply_existing_identifier_context(row: dict[str, Any]) -> str:
    return (
        f"identifier_isin={row.get('identifier_isin', '') or 'none'};"
        f"listing_index_isin={row.get('listing_index_isin', '') or 'none'};"
        f"existing_identifier_figi={row.get('existing_identifier_figi', '') or 'none'};"
        f"existing_listing_index_figi={row.get('existing_listing_index_figi', '') or 'none'}"
    )


def canada_figi_apply_gate_context(row: dict[str, Any]) -> str:
    return (
        f"decision={row.get('decision', '') or 'none'};"
        f"reason={row.get('reason', '') or 'none'};"
        f"verification_evidence_required={row.get('verification_evidence_required', '') or 'none'}"
    )


def b3_etf_apply_official_source_context(row: dict[str, Any]) -> str:
    return (
        f"candidate_sources={row.get('candidate_sources', '') or 'none'};"
        f"candidate_source_urls={row.get('candidate_source_urls', '') or 'none'}"
    )


def b3_etf_apply_category_review_context(row: dict[str, Any]) -> str:
    return (
        f"current_etf_category={row.get('current_etf_category', '') or 'none'};"
        f"official_sector={row.get('official_sector', '') or 'none'};"
        f"category_update={row.get('category_update', '') or 'none'}"
    )


def b3_etf_apply_gate_context(row: dict[str, Any]) -> str:
    return (
        f"decision={row.get('decision', '') or 'none'};"
        f"reason={row.get('reason', '') or 'none'};"
        f"verification_evidence_required={row.get('verification_evidence_required', '') or 'none'}"
    )


def ngx_apply_official_source_context(row: dict[str, Any]) -> str:
    return (
        f"exchange={row.get('exchange', '') or 'none'};"
        f"official_sector={row.get('official_sector', '') or 'none'}"
    )


def ngx_apply_mapping_review_context(row: dict[str, Any]) -> str:
    return (
        f"official_sector={row.get('official_sector', '') or 'none'};"
        f"sector_update={row.get('sector_update', '') or 'none'};"
        f"mapping_supported={'true' if row.get('sector_update') else 'false'}"
    )


def ngx_apply_gate_context(row: dict[str, Any]) -> str:
    return (
        f"decision={row.get('decision', '') or 'none'};"
        f"reason={row.get('reason', '') or 'none'}"
    )


def financialdata_discovery_context(row: dict[str, Any]) -> str:
    return (
        f"financialdata_exchange={row.get('financialdata_exchange', '') or 'none'};"
        f"financialdata_ticker={row.get('financialdata_ticker', '') or 'none'};"
        f"financialdata_review_scope={row.get('financialdata_review_scope', '') or 'none'};"
        f"financialdata_name_present={'true' if row.get('financialdata_name') else 'false'}"
    )


def financialdata_official_identity_context(row: dict[str, Any]) -> str:
    return (
        f"official_exchange={row.get('official_exchange', '') or 'none'};"
        f"official_ticker={row.get('official_ticker', '') or 'none'};"
        f"official_source_key={row.get('official_source_key', '') or 'none'};"
        f"official_reference_scope={row.get('official_reference_scope', '') or 'none'};"
        f"official_isin_present={'true' if row.get('official_isin') else 'false'};"
        f"official_name_present={'true' if row.get('official_name') else 'false'}"
    )


def financialdata_supplement_review_context(row: dict[str, Any]) -> str:
    return (
        f"decision={row.get('decision', '') or 'none'};"
        f"reason={row.get('reason', '') or 'none'};"
        f"financialdata_review_queue={row.get('financialdata_review_queue', '') or 'none'};"
        f"apply_eligibility={row.get('apply_eligibility', '') or 'none'};"
        f"verification_evidence_required={row.get('verification_evidence_required', '') or 'none'}"
    )


def source_gap_context(row: dict[str, Any]) -> str:
    return (
        f"listing_key={row.get('listing_key', '') or 'none'};"
        f"exchange={row.get('exchange', '') or 'none'};"
        f"ticker={row.get('ticker', '') or 'none'};"
        f"asset_type={row.get('asset_type', '') or 'none'};"
        f"field={row.get('field', '') or 'none'};"
        f"target_field={row.get('target_field', '') or 'none'}"
    )


def source_gap_classification_context(row: dict[str, Any]) -> str:
    return (
        f"gap_class={row.get('gap_class', '') or 'none'};"
        f"review_needed={row.get('review_needed', '') or 'none'};"
        f"confidence_policy_present={'true' if row.get('confidence_policy') else 'false'};"
        f"name_present={'true' if row.get('name') else 'false'}"
    )


def source_gap_evidence_gate_context(row: dict[str, Any]) -> str:
    return (
        f"recommended_next_source_present={'true' if row.get('recommended_next_source') else 'false'};"
        f"source_gate_present={'true' if row.get('source_gate') else 'false'};"
        f"target_field={row.get('target_field', '') or 'none'};"
        f"gap_class={row.get('gap_class', '') or 'none'}"
    )


REQUIRED_COLLISION_ROW_KEYS = (
    "target_listing_key",
    "ticker",
    "target_exchange",
    "official_name",
    "official_asset_type",
    "official_source_key",
    "official_source_context",
    "existing_listing_keys",
    "existing_exchanges",
    "existing_dataset_context",
    "identity_evidence",
    "identity_resolution_context",
    "collision_risk_flags",
    "identity_resolution_queue",
    "review_bucket",
    "review_priority",
    "review_strategy",
    "verification_evidence_required",
    "recommended_next_source",
    "source_gate",
    "review_decision",
    "clearance_evidence_required",
    "recommended_next_action",
)
REQUIRED_COLLISION_SUMMARY_KEYS = (
    "rows",
    "decision_totals",
    "review_bucket_totals",
    "review_priority_totals",
    "collision_risk_flag_totals",
    "identity_resolution_queue_totals",
    "identity_resolution_risk_flag_totals",
    "identity_resolution_exchange_totals",
    "identity_resolution_asset_type_totals",
    "identity_resolution_official_source_totals",
    "identity_resolution_existing_exchange_pair_totals",
    "identity_resolution_pair_review_strategy_totals",
    "identity_resolution_review_strategy_totals",
    "identity_resolution_evidence_required_totals",
    "identity_resolution_identity_evidence_totals",
    "identity_resolution_clearance_readiness_totals",
    "identity_resolution_queue_clearance_readiness_totals",
    "top_identity_resolution_clearance_batches",
    "top_identity_resolution_pair_review_batches",
    "same_isin_exact_name_scope_review_rows",
    "top_same_isin_exact_name_scope_review_batches",
    "clearance_evidence_totals",
    "exchange_totals",
    "official_asset_type_totals",
    "asset_type_mismatch_totals",
    "official_source_totals",
    "policy",
)
COLLISION_REVIEW_DECISIONS = {
    "new_listing_candidate_requires_official_listing_add_review",
    "same_isin_cross_listing_candidate_requires_exchange_scope_review",
    "symbol_collision_requires_non_symbol_identity_source",
}
COLLISION_REVIEW_BUCKETS = {
    "distinct_official_isin_new_listing_candidate",
    "hold_symbol_only_collision_needs_non_symbol_identity",
    "resolve_asset_type_conflict_before_identity_review",
    "same_isin_cross_listing_needs_name_or_scope_review",
    "same_isin_exact_name_cross_listing_candidate",
}
COLLISION_REVIEW_PRIORITIES = {"P1", "P2", "P3"}
COLLISION_IDENTITY_RESOLUTION_QUEUES = {
    "blocked_asset_type_conflict",
    "blocked_symbol_only_missing_non_symbol_identity",
    "review_cross_listing_same_isin_exact_name",
    "review_cross_listing_same_isin_name_or_scope_gap",
    "review_distinct_official_isin_new_listing",
}
COLLISION_BUCKET_TO_IDENTITY_QUEUE = {
    "distinct_official_isin_new_listing_candidate": "review_distinct_official_isin_new_listing",
    "hold_symbol_only_collision_needs_non_symbol_identity": "blocked_symbol_only_missing_non_symbol_identity",
    "resolve_asset_type_conflict_before_identity_review": "blocked_asset_type_conflict",
    "same_isin_cross_listing_needs_name_or_scope_review": "review_cross_listing_same_isin_name_or_scope_gap",
    "same_isin_exact_name_cross_listing_candidate": "review_cross_listing_same_isin_exact_name",
}
COLLISION_PAIR_REVIEW_STRATEGIES = {
    "batch_block_instrument_type_conflict_until_official_resolution",
    "batch_hold_symbol_reuse_until_non_symbol_identity_source",
    "batch_review_distinct_isin_new_listing_candidates",
    "batch_review_same_isin_exact_name_cross_listing_scope",
    "batch_review_same_isin_name_or_scope_reconciliation",
}
COLLISION_QUEUE_TO_PAIR_REVIEW_STRATEGY = {
    "blocked_asset_type_conflict": "batch_block_instrument_type_conflict_until_official_resolution",
    "blocked_symbol_only_missing_non_symbol_identity": "batch_hold_symbol_reuse_until_non_symbol_identity_source",
    "review_cross_listing_same_isin_exact_name": "batch_review_same_isin_exact_name_cross_listing_scope",
    "review_cross_listing_same_isin_name_or_scope_gap": "batch_review_same_isin_name_or_scope_reconciliation",
    "review_distinct_official_isin_new_listing": "batch_review_distinct_isin_new_listing_candidates",
}
COLLISION_QUEUE_TO_EVIDENCE_REQUIRED = {
    "blocked_asset_type_conflict": "official_instrument_type_resolution_before_listing_identity_review",
    "blocked_symbol_only_missing_non_symbol_identity": "official_non_symbol_identifier_or_keep_absent",
    "review_cross_listing_same_isin_exact_name": "official_target_and_existing_exchange_directories_confirm_active_same_instrument",
    "review_cross_listing_same_isin_name_or_scope_gap": "official_target_exchange_status_plus_issuer_or_name_scope_reconciliation",
    "review_distinct_official_isin_new_listing": "official_target_exchange_listing_key_isin_name_instrument_type_listing_status",
}
COLLISION_CLEARANCE_READINESS_STATUSES = {
    "blocked_symbol_only_non_symbol_identity_required",
    "blocked_until_asset_type_conflict_resolved",
    "needs_official_listing_add_review",
    "needs_official_name_or_scope_reconciliation",
    "review_ready_same_isin_exact_name_scope_check",
}
COLLISION_QUEUE_TO_CLEARANCE_READINESS = {
    "blocked_asset_type_conflict": "blocked_until_asset_type_conflict_resolved",
    "blocked_symbol_only_missing_non_symbol_identity": "blocked_symbol_only_non_symbol_identity_required",
    "review_cross_listing_same_isin_exact_name": "review_ready_same_isin_exact_name_scope_check",
    "review_cross_listing_same_isin_name_or_scope_gap": "needs_official_name_or_scope_reconciliation",
    "review_distinct_official_isin_new_listing": "needs_official_listing_add_review",
}


def collision_exchange_context(target_exchange: str, existing_exchanges: str) -> str:
    pairs = [f"{target_exchange}::{exchange}" for exchange in existing_exchanges.split("|") if exchange]
    return "|".join(pairs) or target_exchange


def collision_recommended_next_source(queue: str, exchange_context: str) -> str:
    if queue == "review_cross_listing_same_isin_exact_name":
        return f"Official active-listing directories for both exchanges in {exchange_context}."
    if queue == "review_cross_listing_same_isin_name_or_scope_gap":
        return f"Official target-exchange status plus issuer/name or scope reconciliation for {exchange_context}."
    if queue == "review_distinct_official_isin_new_listing":
        return f"Official target-exchange listing record for {exchange_context} with listing key, ISIN, name, type, and status."
    if queue == "blocked_asset_type_conflict":
        return f"Official instrument-type evidence resolving the asset-type conflict for {exchange_context}."
    if queue == "blocked_symbol_only_missing_non_symbol_identity":
        return f"Official non-symbol identifier evidence for {exchange_context}, or keep the target listing absent."
    return f"Manual identity-resolution evidence for {exchange_context}."


def collision_official_source_context(row: dict[str, Any]) -> str:
    source_key = str(row.get("official_source_key", "") or "none")
    source_url = str(row.get("official_source_url", "") or "none")
    return f"official_source_key={source_key};official_source_url={source_url}"


def collision_existing_dataset_context(row: dict[str, Any]) -> str:
    return (
        f"existing_listing_keys={str(row.get('existing_listing_keys', '') or 'none')};"
        f"existing_exchanges={str(row.get('existing_exchanges', '') or 'none')};"
        f"existing_isins={str(row.get('existing_isins', '') or 'none')};"
        f"same_isin_listing_keys={str(row.get('same_isin_listing_keys', '') or 'none')};"
        f"name_exact_match_listing_keys={str(row.get('name_exact_match_listing_keys', '') or 'none')};"
        f"asset_type_mismatch={str(row.get('asset_type_mismatch', '') or 'none')}"
    )


def collision_identity_resolution_context(row: dict[str, Any]) -> str:
    exchange_context = collision_exchange_context(
        str(row.get("target_exchange", "")),
        str(row.get("existing_exchanges", "")),
    )
    return (
        f"exchange_context={exchange_context or 'none'};"
        f"identity_resolution_queue={str(row.get('identity_resolution_queue', '') or 'none')};"
        f"review_bucket={str(row.get('review_bucket', '') or 'none')};"
        f"identity_evidence={str(row.get('identity_evidence', '') or 'none')}"
    )


def collision_source_gate(queue: str) -> str:
    if queue == "review_cross_listing_same_isin_exact_name":
        return "Do not add or merge until both official exchange directories confirm the same active instrument."
    if queue == "review_cross_listing_same_isin_name_or_scope_gap":
        return "Do not resolve identity until official listing status and issuer/name or scope differences are reconciled."
    if queue == "review_distinct_official_isin_new_listing":
        return "Do not add the listing until official target-exchange evidence proves key, ISIN, name, type, and active status."
    if queue == "blocked_asset_type_conflict":
        return "Block identity resolution until official instrument-type evidence resolves the conflict."
    if queue == "blocked_symbol_only_missing_non_symbol_identity":
        return "Keep absent; ticker equality alone is not identity evidence."
    return "Manual review required; symbol-only evidence is insufficient."
COLLISION_REQUIRED_POLICY_KEYS = (
    "no_symbol_only_additions",
    "truth_required",
    "blank_until_verified",
)
EXPECTED_COLLISION_SOURCE_FILES = {
    "listings_csv": "data/listings.csv",
    "masterfile_reference_csv": "data/masterfiles/reference.csv",
}
MASTERFILE_COLLISION_CAMPAIGN_EVIDENCE_KEYS = (
    "collision_review_rows",
    "decision_totals",
    "review_bucket_totals",
    "review_priority_totals",
    "collision_risk_flag_totals",
    "identity_resolution_queue_totals",
    "identity_resolution_risk_flag_totals",
    "identity_resolution_exchange_totals",
    "identity_resolution_asset_type_totals",
    "identity_resolution_official_source_totals",
    "identity_resolution_existing_exchange_pair_totals",
    "identity_resolution_pair_review_strategy_totals",
    "identity_resolution_review_strategy_totals",
    "identity_resolution_evidence_required_totals",
    "identity_resolution_identity_evidence_totals",
    "identity_resolution_clearance_readiness_totals",
    "identity_resolution_queue_clearance_readiness_totals",
    "top_identity_resolution_clearance_batches",
    "top_identity_resolution_pair_review_batches",
    "same_isin_exact_name_scope_review_rows",
    "top_same_isin_exact_name_scope_review_batches",
    "clearance_evidence_totals",
    "exchange_totals",
    "official_asset_type_totals",
    "asset_type_mismatch_totals",
    "official_source_totals",
    "asset_type_mismatches",
)
MASTERFILE_COLLISION_CAMPAIGN_DELTA_EVIDENCE_KEYS = (
    "current_collision_review_rows",
    "collision_resolution_delta",
    "listing_addition_delta",
    "warn_quarantine_delta",
)
EXPECTED_CAMPAIGN_SOURCE_FILES = {
    "coverage_report": "data/reports/coverage_report.json",
    "source_gap_classification": "data/reports/source_gap_classification.json",
    "b3_masterfile_gap_review": "data/reports/b3_masterfile_gap_review.json",
    "b3_etf_category_apply_report": "data/reports/b3_etf_category_apply_report.json",
    "b3_residual_isin_review": "data/reports/b3_residual_isin_review.json",
    "b3_residual_sector_review": "data/reports/b3_residual_sector_review.json",
    "otc_scope_review": "data/reports/otc_scope_review.json",
    "otc_name_mismatch_review": "data/reports/otc_name_mismatch_review.json",
    "canada_residual_review": "data/reports/canada_residual_review.json",
    "canada_figi_queue": "data/reports/canada_figi_queue.json",
    "canada_figi_apply_report": "data/reports/canada_figi_apply_report.json",
    "asx_residual_review": "data/reports/asx_residual_review.json",
    "weak_sector_residual_review": "data/reports/weak_sector_residual_review.json",
    "ngx_official_sector_apply_report": "data/reports/ngx_official_sector_apply_report.json",
    "masterfile_collision_review": "data/reports/masterfile_collision_review.json",
    "symbol_changes_review": "data/reports/symbol_changes_review.json",
    "ohlcv_plausibility": "data/reports/ohlcv_plausibility.json",
    "ohlcv_warning_review": "data/reports/ohlcv_warning_review.json",
    "financialdata_isin_supplements_review": "data/reports/financialdata_isin_supplements_review.json",
    "validation_report": "data/reports/validation_report.json",
    "improvement_baseline": "data/reports/improvement_baseline.json",
    "improvement_deltas": "data/reports/improvement_deltas.json",
}
PROGRESS_MARKDOWN_TRACEABILITY_REPORTS = {
    "improvement_baseline": {
        "json": "data/reports/improvement_baseline.json",
        "markdown": "data/reports/improvement_baseline.md",
        "source_files": EXPECTED_BASELINE_SOURCE_FILES,
    },
    "improvement_deltas": {
        "json": "data/reports/improvement_deltas.json",
        "markdown": "data/reports/improvement_deltas.md",
        "source_files": EXPECTED_DELTA_SOURCE_FILES,
    },
    "improvement_campaigns": {
        "json": "data/reports/improvement_campaigns.json",
        "markdown": "data/reports/improvement_campaigns.md",
        "source_files": EXPECTED_CAMPAIGN_SOURCE_FILES,
    },
}
REQUIRED_OTC_SCOPE_ROW_KEYS = (
    "listing_key",
    "ticker",
    "exchange",
    "asset_type",
    "instrument_scope",
    "scope_reason",
    "quality_status",
    "source_gap_context",
    "scope_decision",
    "otc_review_decision_status",
    "otc_review_decision_context",
    "review_bucket",
    "review_priority",
    "scope_apply_eligibility",
    "metadata_enrichment_gate",
    "scope_review_context",
    "review_strategy",
    "verification_evidence_required",
    "recommended_next_source",
    "source_gate",
    "recommended_action",
)
REQUIRED_OTC_SCOPE_SUMMARY_KEYS = (
    "rows",
    "scope_decision_totals",
    "instrument_scope_totals",
    "scope_reason_totals",
    "quality_status_totals",
    "review_bucket_totals",
    "review_bucket_asset_type_totals",
    "review_bucket_metadata_gate_totals",
    "review_priority_totals",
    "review_strategy_totals",
    "verification_evidence_required_totals",
    "top_otc_scope_review_batches",
    "scope_apply_eligibility_totals",
    "otc_scope_completion",
    "post_scope_metadata_backlog",
    "post_scope_metadata_backlog_bucket_totals",
    "post_scope_metadata_backlog_gate_totals",
    "post_scope_metadata_backlog_examples",
    "metadata_enrichment_gate_totals",
    "source_gap_field_totals",
    "source_gap_class_totals",
    "source_of_truth_outcome_totals",
    "otc_core_exclusion_candidate_rows",
    "otc_core_exclusion_candidate_asset_type_totals",
    "otc_core_exclusion_candidate_quality_status_totals",
    "otc_core_exclusion_candidate_metadata_gate_totals",
    "otc_core_exclusion_candidate_review_examples",
    "drop_override_rows",
    "drop_override_rows_still_present",
    "otc_review_decision_rows",
    "otc_review_decision_active_name_mismatch_rows",
    "otc_name_mismatch_unreviewed_active_rows",
    "otc_review_decision_resolution_totals",
    "otc_review_decision_current_listing_suppressed_rows",
    "otc_review_decision_current_listing_suppressed_examples",
    "otc_review_decision_not_current_scope_rows",
    "otc_review_decision_not_current_scope_examples",
    "otc_review_decision_stale_rows",
    "otc_review_decision_stale_examples",
    "policy",
)
OTC_SCOPE_DECISIONS = {
    "already_extended_otc_listing",
    "core_exclusion_candidate_requires_review",
    "unexpected_otc_core_scope_review_required",
}
OTC_REVIEW_BUCKETS = {
    "clean_extended_otc_listing",
    "core_exclusion_candidate_scope_review",
    "documented_otc_category_source_gap",
    "documented_otc_sector_source_gap",
    "documented_otc_source_gap",
    "official_name_mismatch_review_first",
    "otc_quality_source_gap_review",
    "otc_quality_warn_review",
    "unexpected_core_scope_requires_review",
}
OTC_REVIEW_PRIORITIES = {"P1", "P2", "P3", "P4"}
OTC_REQUIRED_POLICY_KEYS = ("otc_scope", "no_blind_sector_enrichment")


def otc_scope_review_strategy(review_bucket: str) -> str:
    if review_bucket == "unexpected_core_scope_requires_review":
        return "review_unexpected_otc_core_scope_before_release"
    if review_bucket == "core_exclusion_candidate_scope_review":
        return "decide_core_extended_or_exclude_before_otc_metadata_work"
    if review_bucket == "official_name_mismatch_review_first":
        return "resolve_listing_keyed_name_mismatch_before_metadata_work"
    if review_bucket == "documented_otc_sector_source_gap":
        return "keep_sector_blank_until_reviewed_issuer_sector_source"
    if review_bucket == "documented_otc_category_source_gap":
        return "keep_category_blank_until_reviewed_product_taxonomy_source"
    if review_bucket == "documented_otc_source_gap":
        return "keep_metadata_blank_until_reviewed_otc_source"
    if review_bucket == "otc_quality_warn_review":
        return "review_entry_quality_warning_before_metadata_work"
    if review_bucket == "otc_quality_source_gap_review":
        return "review_quality_source_gap_before_metadata_work"
    if review_bucket == "clean_extended_otc_listing":
        return "no_scope_or_metadata_action_required"
    return "manual_otc_scope_review"


def otc_scope_verification_evidence_required(review_bucket: str) -> str:
    if review_bucket == "unexpected_core_scope_requires_review":
        return "reviewed_scope_override_or_official_listing_scope_policy_before_release"
    if review_bucket == "core_exclusion_candidate_scope_review":
        return "official_otc_listing_status_or_scope_policy_decision_before_metadata_work"
    if review_bucket == "official_name_mismatch_review_first":
        return "reviewed_otc_name_mismatch_decision_before_name_or_metadata_change"
    if review_bucket == "documented_otc_sector_source_gap":
        return "reviewed_issuer_sector_source_with_exact_listing_or_keep_blank_decision"
    if review_bucket == "documented_otc_category_source_gap":
        return "reviewed_product_taxonomy_source_with_exact_listing_or_keep_blank_decision"
    if review_bucket == "documented_otc_source_gap":
        return "reviewed_otc_metadata_source_with_exact_listing_or_keep_blank_decision"
    if review_bucket == "otc_quality_warn_review":
        return "entry_quality_warning_review_before_metadata_change"
    if review_bucket == "otc_quality_source_gap_review":
        return "source_gap_review_or_reviewed_source_before_metadata_change"
    if review_bucket == "clean_extended_otc_listing":
        return "current_pass_status_and_extended_scope_policy_no_metadata_action"
    return "manual_otc_scope_review_evidence"


def otc_scope_recommended_next_source(review_bucket: str) -> str:
    if review_bucket == "unexpected_core_scope_requires_review":
        return "Instrument scope override, OTC Markets security profile, SEC/issuer filing, or reviewed scope decision."
    if review_bucket == "core_exclusion_candidate_scope_review":
        return "OTC Markets security profile, exchange tier/status evidence, SEC/issuer filing, or reviewed scope policy decision."
    if review_bucket == "official_name_mismatch_review_first":
        return "OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history."
    if review_bucket == "documented_otc_sector_source_gap":
        return "SEC SIC, issuer filings, OTC Markets profile, or reviewed secondary company profile."
    if review_bucket == "documented_otc_category_source_gap":
        return "Issuer fund documents, ETF sponsor page, prospectus, OTC Markets profile, or reviewed product taxonomy source."
    if review_bucket == "documented_otc_source_gap":
        return "Exact listing-keyed OTC Markets, issuer, SEC, or reviewed registry evidence."
    if review_bucket == "otc_quality_warn_review":
        return "Entry-quality source evidence plus OTC Markets, issuer, SEC, or registry confirmation."
    if review_bucket == "otc_quality_source_gap_review":
        return "Entry-quality source-gap artifact and stronger OTC Markets, issuer, SEC, or registry evidence."
    if review_bucket == "clean_extended_otc_listing":
        return "No additional source required unless a future metadata change is proposed."
    return "Manual OTC scope review evidence."


def otc_scope_source_gate(review_bucket: str) -> str:
    if review_bucket == "unexpected_core_scope_requires_review":
        return "No scope or metadata change until the core override is reviewed and listing-keyed."
    if review_bucket == "core_exclusion_candidate_scope_review":
        return "Decide core, extended, or exclude before any identifier, name, sector, or category enrichment."
    if review_bucket == "official_name_mismatch_review_first":
        return "No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision."
    if review_bucket == "documented_otc_sector_source_gap":
        return "Canonical stock sector only after exchange/name gate; no ticker/name-only inference."
    if review_bucket == "documented_otc_category_source_gap":
        return "ETF category only from exact product evidence; no category inference from ticker or issuer family."
    if review_bucket == "documented_otc_source_gap":
        return "Keep metadata blank until exact listing-keyed source evidence or reviewed keep-blank decision exists."
    if review_bucket == "otc_quality_warn_review":
        return "Resolve the quality warning before using the row for metadata enrichment."
    if review_bucket == "otc_quality_source_gap_review":
        return "Resolve or document the source gap before any metadata enrichment."
    if review_bucket == "clean_extended_otc_listing":
        return "Current extended-scope pass row; no metadata action is authorized by this report."
    return "Manual review required before any data change."


def otc_scope_source_gap_context(row: dict[str, Any]) -> str:
    return (
        f"quality_status={str(row.get('quality_status', '') or 'none')};"
        f"issue_types={str(row.get('issue_types', '') or 'none')};"
        f"source_gap_field={str(row.get('source_gap_field', '') or 'none')};"
        f"source_gap_class={str(row.get('source_gap_class', '') or 'none')};"
        f"source_of_truth_outcome={str(row.get('source_of_truth_outcome', '') or 'none')}"
    )


def otc_scope_review_decision_context(row: dict[str, Any]) -> str:
    return (
        f"listing_key={str(row.get('listing_key', '') or 'none')};"
        f"otc_review_decision_status={str(row.get('otc_review_decision_status', '') or 'none')}"
    )


def otc_scope_review_context(row: dict[str, Any]) -> str:
    return (
        f"instrument_scope={str(row.get('instrument_scope', '') or 'none')};"
        f"scope_reason={str(row.get('scope_reason', '') or 'none')};"
        f"scope_decision={str(row.get('scope_decision', '') or 'none')};"
        f"scope_apply_eligibility={str(row.get('scope_apply_eligibility', '') or 'none')};"
        f"metadata_enrichment_gate={str(row.get('metadata_enrichment_gate', '') or 'none')}"
    )


def otc_name_mismatch_review_strategy(review_class: str) -> str:
    if review_class == "hold_unresolved":
        return "keep_current_until_stronger_issuer_history_source"
    if review_class == "stale_or_symbol_reuse_without_isin":
        return "resolve_or_quarantine_with_official_otc_profile_or_issuer_history"
    if review_class == "probable_otc_rename_or_symbol_reuse":
        return "verify_isin_anchored_issuer_history_before_name_change"
    if review_class == "weak_abbreviation_or_truncation_review":
        return "review_official_alias_or_abbreviation_before_matcher_tuning"
    if review_class == "matcher_false_positive":
        return "tighten_matcher_without_dataset_metadata_change"
    return "manual_otc_name_review"


def otc_name_mismatch_recommended_next_source(review_class: str) -> str:
    if review_class == "hold_unresolved":
        return "Stronger official or reviewed issuer-history source matching the OTC listing key."
    if review_class == "stale_or_symbol_reuse_without_isin":
        return "Official OTC profile, registry, exchange notice, or issuer-history source matching the listing key."
    if review_class == "probable_otc_rename_or_symbol_reuse":
        return "Official or reviewed ISIN-bearing issuer-history source matching current issuer, listing key, and name."
    if review_class == "weak_abbreviation_or_truncation_review":
        return "Official alias, abbreviation, issuer, OTC profile, or registry evidence matching the exact listing identity."
    if review_class == "matcher_false_positive":
        return "Entry-quality matcher regression evidence; no issuer metadata source is needed for a data change."
    return "Manual reviewed OTC identity source matching the listing key."


def otc_name_mismatch_source_gate(review_class: str) -> str:
    if review_class == "hold_unresolved":
        return "Keep current name until stronger issuer-history evidence resolves the ambiguity."
    if review_class == "stale_or_symbol_reuse_without_isin":
        return "Do not rename or trust reused OTC symbol without listing-keyed official identity or quarantine evidence."
    if review_class == "probable_otc_rename_or_symbol_reuse":
        return "Do not change the name until ISIN-anchored evidence proves the same current issuer."
    if review_class == "weak_abbreviation_or_truncation_review":
        return "Tune matcher only after official alias evidence; do not change metadata from abbreviation alone."
    if review_class == "matcher_false_positive":
        return "Matcher-only fix; no ticker, listing, name, sector, or category change is authorized."
    return "Manual review required before any OTC name or metadata change."


def otc_name_mismatch_official_source_context(row: dict[str, Any]) -> str:
    return (
        f"official_sources={row.get('official_sources', '') or 'none'};"
        f"official_name_present={'true' if row.get('official_name') else 'false'}"
    )


def otc_name_mismatch_identity_review_context(row: dict[str, Any]) -> str:
    return (
        f"token_overlap={row.get('token_overlap', 0)};"
        f"current_token_count={row.get('current_token_count', 0)};"
        f"official_token_count={row.get('official_token_count', 0)};"
        f"isin_presence={'with_isin' if row.get('isin') else 'without_isin'};"
        f"country={row.get('country', '') or 'none'}"
    )


def otc_name_mismatch_decision_review_context(row: dict[str, Any]) -> str:
    return (
        f"review_class={row.get('review_class', '') or 'none'};"
        f"apply_eligibility={row.get('apply_eligibility', '') or 'none'};"
        f"verification_evidence_required={row.get('verification_evidence_required', '') or 'none'};"
        f"review_decision={row.get('review_decision', '') or 'none'}"
    )


OTC_CAMPAIGN_EVIDENCE_KEYS = (
    "otc_review_rows",
    "source_gap_rows",
    "drop_override_rows_still_present",
    "review_bucket_totals",
    "review_bucket_asset_type_totals",
    "review_priority_totals",
    "scope_review_strategy_totals",
    "scope_verification_evidence_required_totals",
    "top_otc_scope_review_batches",
    "otc_core_exclusion_candidate_rows",
    "otc_core_exclusion_candidate_asset_type_totals",
    "otc_core_exclusion_candidate_metadata_gate_totals",
    "otc_core_exclusion_candidate_review_examples",
    "scope_apply_eligibility_totals",
    "otc_scope_completion",
    "metadata_enrichment_gate_totals",
    "source_gap_field_totals",
    "source_gap_class_totals",
    "source_of_truth_outcome_totals",
    "name_mismatch_review_rows",
    "otc_review_decision_active_name_mismatch_rows",
    "otc_name_mismatch_unreviewed_active_rows",
    "otc_review_decision_resolution_totals",
    "otc_review_decision_current_listing_suppressed_rows",
    "otc_review_decision_not_current_scope_rows",
    "otc_review_decision_stale_rows",
    "name_mismatch_class_counts",
    "name_mismatch_priority_counts",
    "name_mismatch_review_strategy_counts",
    "top_otc_name_mismatch_review_batches",
    "name_mismatch_apply_eligibility_counts",
    "name_mismatch_verification_evidence_required_counts",
)
OTC_CAMPAIGN_DELTA_EVIDENCE_KEYS = (
    "drop_override_rows_still_present",
    "accepted_source_gap_rows",
    "otc_core_exclusion_candidate_rows",
    "active_name_mismatch_review_rows",
    "otc_name_mismatch_unreviewed_active_rows",
    "otc_review_decision_current_listing_suppressed_rows",
    "otc_review_decision_not_current_scope_rows",
    "otc_review_decision_stale_rows",
    "campaign_start_scope_snapshot",
    "warn_quarantine_delta",
)
REQUIRED_CANADA_RESIDUAL_SUMMARY_KEYS = (
    "rows",
    "exchange_totals",
    "asset_type_totals",
    "missing_isin_rows",
    "missing_figi_rows",
    "core_exclusion_candidate_rows",
    "core_exclusion_candidate_exchange_totals",
    "core_exclusion_candidate_asset_type_totals",
    "core_exclusion_candidate_resolution_queue_totals",
    "core_exclusion_candidate_official_source_totals",
    "core_exclusion_candidate_source_gap_class_totals",
    "core_exclusion_candidate_examples",
    "canada_resolution_queue_totals",
    "canada_resolution_queue_exchange_totals",
    "canada_resolution_queue_asset_type_totals",
    "canada_resolution_queue_source_gap_class_totals",
    "canada_resolution_queue_official_source_totals",
    "canada_resolution_queue_review_strategy_totals",
    "canada_resolution_queue_evidence_required_totals",
    "top_canada_resolution_review_batches",
    "isin_decision_totals",
    "figi_decision_totals",
    "isin_apply_eligibility_totals",
    "figi_apply_eligibility_totals",
    "verification_evidence_required_totals",
    "openfigi_review_status_totals",
    "openfigi_review_decision_totals",
    "source_gap_field_totals",
    "source_gap_class_totals",
    "source_of_truth_outcome_totals",
    "official_masterfile_source_totals",
    "policy",
)
REQUIRED_CANADA_RESIDUAL_ROW_KEYS = (
    "listing_key",
    "ticker",
    "exchange",
    "asset_type",
    "isin",
    "figi",
    "missing_isin",
    "missing_figi",
    "source_gap_context",
    "official_source_context",
    "canada_resolution_queue",
    "review_strategy",
    "queue_evidence_required",
    "recommended_next_source",
    "source_gate",
    "isin_decision",
    "figi_decision",
    "isin_apply_eligibility",
    "figi_apply_eligibility",
    "verification_evidence_required",
    "identifier_review_context",
    "recommended_next_action",
)
REQUIRED_CANADA_QUEUE_SUMMARY_KEYS = (
    "rows",
    "batch_size",
    "batches",
    "excluded_openfigi_gap_rows",
    "exchange_totals",
    "asset_type_totals",
    "openfigi_exchange_hint_totals",
    "apply_eligibility_totals",
    "verification_evidence_required_totals",
    "policy",
)
REQUIRED_CANADA_QUEUE_ROW_KEYS = (
    "batch_id",
    "batch_position",
    "listing_key",
    "ticker",
    "exchange",
    "asset_type",
    "name",
    "isin",
    "figi_decision",
    "openfigi_exchange_hint",
    "review_gate",
    "apply_eligibility",
    "verification_evidence_required",
)
REQUIRED_CANADA_APPLY_SUMMARY_KEYS = (
    "applied",
    "rows",
    "gap_rows_added",
    "decision_totals",
    "reason_totals",
    "applied_rows",
    "policy",
    "written_rows",
)
REQUIRED_CANADA_APPLY_ROW_KEYS = (
    "listing_key",
    "ticker",
    "exchange",
    "isin",
    "figi",
    "decision",
    "reason",
)
CANADA_EXCHANGES = {"NEO", "TSX", "TSXV"}
CANADA_QUEUE_ELIGIBILITY = "eligible_for_openfigi_probe_only_no_figi_apply_until_collision_gate"
CANADA_QUEUE_EVIDENCE = "openfigi_id_isin_query_with_canada_exchange_hint_then_collision_and_cross_isin_review"
CANADA_EXCLUDED_GAP_ELIGIBILITY = "keep_figi_blank_as_reviewed_openfigi_source_gap_until_stronger_source"
CANADA_APPLY_DECISIONS = {"apply", "reject", "skip"}
CANADA_REQUIRED_RESIDUAL_POLICY_KEYS = ("tmx_first", "figi_gate", "openfigi_review_gate", "no_guessing")
CANADA_REQUIRED_QUEUE_POLICY_KEYS = ("input_gate", "reviewed_gap_gate", "no_symbol_guessing")
CANADA_REQUIRED_APPLY_POLICY_KEYS = ("source", "gates", "openfigi_no_match")
CANADA_REVIEW_STRATEGIES = {
    "block_figi_until_valid_isin",
    "keep_figi_blank_after_reviewed_openfigi_cross_isin_collision",
    "keep_figi_blank_after_reviewed_openfigi_no_match",
    "keep_isin_blank_until_stronger_official_source",
    "keep_metadata_blank_until_stronger_official_source",
    "queue_openfigi_by_isin_with_canada_exchange_hint",
    "scope_review_before_canada_identifier_enrichment",
    "scope_review_before_canada_metadata_enrichment",
    "seek_official_canada_isin_source",
}


def canada_recommended_next_source_for_queue(queue: str, official_source: str) -> str:
    source_label = official_source if official_source != "none" else "current official exchange or issuer source"
    if queue == "core_exclusion_candidate_identifier_scope_review":
        return f"{source_label} plus reviewed scope decision for core, extended, or exclude before identifier work."
    if queue == "core_exclusion_candidate_metadata_scope_review":
        return f"{source_label} plus reviewed scope decision before any sector or ETF-category work."
    if queue == "missing_isin_official_canada_masterfiles_do_not_expose_isin":
        return "Official CSD, issuer, prospectus, transfer-agent, or reviewed identifier source exposing a valid ISIN."
    if queue == "missing_isin_reviewed_source_gap":
        return "Stronger official Canada identifier source with exact listing-key, issuer/name, and valid ISIN evidence."
    if queue == "reviewed_openfigi_no_match_source_gap":
        return "Stronger FIGI source or reviewed OpenFIGI re-check evidence; existing no-match remains a source gap."
    if queue == "reviewed_openfigi_cross_isin_collision_source_gap":
        return "Stronger FIGI source resolving the cross-ISIN collision with exact listing-key evidence."
    if queue == "openfigi_candidate_after_isin_gate":
        return "OpenFIGI ID_ISIN query with Canada exchange hint, followed by collision and cross-ISIN review."
    if queue == "figi_blocked_until_isin_resolved":
        return "Official ISIN source first; FIGI lookup remains blocked until a valid ISIN is proven."
    if queue == "metadata_source_gap_keep_blank_until_stronger_source":
        return "Official issuer, product, exchange, or reviewed registry metadata source with exact listing match."
    if queue == "residual_no_identifier_action":
        return "No identifier source required unless a future data change is proposed."
    return "Manual Canada source review with exact listing-key evidence."


def canada_source_gate_for_queue(queue: str) -> str:
    if queue == "core_exclusion_candidate_identifier_scope_review":
        return "No ISIN or FIGI enrichment until the listing is reviewed as core, extended, or excluded."
    if queue == "core_exclusion_candidate_metadata_scope_review":
        return "No sector or category fill until scope is decided with official listing evidence."
    if queue == "missing_isin_official_canada_masterfiles_do_not_expose_isin":
        return "Keep ISIN blank until a direct official or reviewed identifier source exposes a valid checksum ISIN."
    if queue == "missing_isin_reviewed_source_gap":
        return "Keep ISIN blank until stronger official evidence resolves the reviewed source gap."
    if queue == "reviewed_openfigi_no_match_source_gap":
        return "Do not re-probe or fill FIGI from symbol/name; keep blank until stronger reviewed FIGI evidence exists."
    if queue == "reviewed_openfigi_cross_isin_collision_source_gap":
        return "Do not apply cross-ISIN FIGI candidates; require stronger listing-keyed collision resolution."
    if queue == "openfigi_candidate_after_isin_gate":
        return "OpenFIGI result is a candidate only; apply only after listing-keyed collision and cross-ISIN gates pass."
    if queue == "figi_blocked_until_isin_resolved":
        return "Do not query or apply FIGI before the ISIN gate is resolved."
    if queue == "metadata_source_gap_keep_blank_until_stronger_source":
        return "Keep metadata blank until exact official or reviewed metadata evidence exists."
    if queue == "residual_no_identifier_action":
        return "No data change is authorized by this residual row."
    return "Manual review required before any data change."


def canada_source_gap_context(row: dict[str, Any]) -> str:
    return (
        f"source_gap_fields={str(row.get('source_gap_fields', '') or 'none')};"
        f"source_gap_classes={str(row.get('source_gap_classes', '') or 'none')};"
        f"source_of_truth_outcomes={str(row.get('source_of_truth_outcomes', '') or 'none')}"
    )


def canada_official_source_context(row: dict[str, Any]) -> str:
    return (
        f"official_masterfile_match={str(row.get('official_masterfile_match', '') or 'none')};"
        f"official_masterfile_sources={str(row.get('official_masterfile_sources', '') or 'none')};"
        f"official_masterfile_exposes_isin={str(row.get('official_masterfile_exposes_isin', '') or 'none')};"
        f"official_masterfile_exposes_sector={str(row.get('official_masterfile_exposes_sector', '') or 'none')}"
    )


def canada_identifier_review_context(row: dict[str, Any]) -> str:
    return (
        f"missing_isin={str(row.get('missing_isin', '') or 'none')};"
        f"missing_figi={str(row.get('missing_figi', '') or 'none')};"
        f"isin_decision={str(row.get('isin_decision', '') or 'none')};"
        f"figi_decision={str(row.get('figi_decision', '') or 'none')};"
        f"isin_apply_eligibility={str(row.get('isin_apply_eligibility', '') or 'none')};"
        f"figi_apply_eligibility={str(row.get('figi_apply_eligibility', '') or 'none')};"
        f"openfigi_review_status={str(row.get('openfigi_review_status', '') or 'none')};"
        f"openfigi_review_decision={str(row.get('openfigi_review_decision', '') or 'none')}"
    )


CANADA_CAMPAIGN_EVIDENCE_KEYS = (
    "canada_residual_rows",
    "active_figi_queue_rows",
    "missing_isin_rows",
    "missing_figi_rows",
    "canada_identifier_backlog",
    "canada_identifier_backlog_queue_totals",
    "canada_identifier_backlog_evidence_required_totals",
    "canada_core_exclusion_candidate_rows",
    "canada_core_exclusion_candidate_exchange_totals",
    "canada_core_exclusion_candidate_asset_type_totals",
    "canada_core_exclusion_candidate_resolution_queue_totals",
    "canada_core_exclusion_candidate_official_source_totals",
    "canada_core_exclusion_candidate_source_gap_class_totals",
    "exchange_totals",
    "official_masterfile_source_totals",
    "canada_resolution_queue_totals",
    "canada_resolution_queue_exchange_totals",
    "canada_resolution_queue_asset_type_totals",
    "canada_resolution_queue_source_gap_class_totals",
    "canada_resolution_queue_official_source_totals",
    "canada_resolution_queue_review_strategy_totals",
    "canada_resolution_queue_evidence_required_totals",
    "top_canada_resolution_review_batches",
    "source_gap_field_totals",
    "source_gap_class_totals",
    "source_of_truth_outcome_totals",
    "reviewed_openfigi_source_gap_rows",
    "openfigi_review_status_totals",
    "openfigi_review_decision_totals",
    "isin_apply_eligibility_totals",
    "figi_apply_eligibility_totals",
    "verification_evidence_required_totals",
    "figi_queue_apply_eligibility_totals",
    "figi_queue_verification_evidence_required_totals",
    "figi_queue_review_strategy_totals",
    "top_canada_figi_queue_review_batches",
    "applied_figi_rows",
    "openfigi_gap_rows_added",
)
CANADA_CAMPAIGN_DELTA_EVIDENCE_KEYS = (
    "applied_figi_rows",
    "openfigi_gap_rows_added",
    "active_figi_queue_rows",
    "canada_core_exclusion_candidate_rows",
    "reviewed_openfigi_source_gap_rows",
    "isin_delta",
    "sector_category_delta",
    "warn_quarantine_delta",
)
REQUIRED_B3_ISIN_SUMMARY_KEYS = (
    "rows",
    "gap_class_totals",
    "residual_decision_totals",
    "current_instrument_scope_totals",
    "current_scope_reason_totals",
    "review_bucket_totals",
    "review_priority_totals",
    "apply_eligibility_totals",
    "verification_evidence_required_totals",
    "cotahist_probe_decision_totals",
    "review_strategy_totals",
    "top_b3_isin_review_batches",
    "policy",
)
REQUIRED_B3_ISIN_ROW_KEYS = (
    "listing_key",
    "ticker",
    "exchange",
    "asset_type",
    "gap_class",
    "source_of_truth_outcome",
    "source_gap_context",
    "current_instrument_scope",
    "current_scope_reason",
    "scope_review_context",
    "review_needed",
    "recommended_next_source",
    "source_gate",
    "b3_instruments_equities_match",
    "cotahist_probe_decision",
    "official_source_context",
    "residual_decision",
    "review_bucket",
    "review_priority",
    "review_strategy",
    "apply_eligibility",
    "verification_evidence_required",
    "residual_review_context",
)
REQUIRED_B3_SECTOR_SUMMARY_KEYS = (
    "rows",
    "gap_class_totals",
    "residual_decision_totals",
    "review_bucket_totals",
    "review_priority_totals",
    "apply_eligibility_totals",
    "verification_evidence_required_totals",
    "b3_probe_decision_totals",
    "review_strategy_totals",
    "top_b3_sector_review_batches",
    "b3_code_shape_totals",
    "alphanumeric_b3_code_rows",
    "alphanumeric_b3_code_examples",
    "policy",
)
REQUIRED_B3_SECTOR_ROW_KEYS = (
    "listing_key",
    "ticker",
    "exchange",
    "asset_type",
    "gap_class",
    "source_of_truth_outcome",
    "source_gap_context",
    "review_needed",
    "recommended_next_source",
    "source_gate",
    "b3_probe_decision",
    "b3_code",
    "official_source_context",
    "residual_decision",
    "review_bucket",
    "review_priority",
    "review_strategy",
    "apply_eligibility",
    "verification_evidence_required",
    "residual_review_context",
)
B3_ISIN_REVIEW_BUCKETS = {
    "active_listing_evidence_required_source_gap",
    "fund_or_registry_identifier_source_gap",
    "identifier_source_gap",
    "not_in_current_b3_directory_source_gap",
    "official_isin_candidate_requires_apply_gate",
    "scope_review_before_identifier_fill",
}
B3_SECTOR_REVIEW_BUCKETS = {
    "ambiguous_b3_code_requires_manual_review",
    "missing_probe_row_requires_parser_or_source_review",
    "no_b3_classification_code_match_source_gap",
    "official_sector_candidate_requires_apply_gate",
    "requires_stronger_official_taxonomy_source_gap",
    "scope_review_before_sector_fill",
}
B3_REVIEW_PRIORITIES = {"P1", "P2", "P3", "P4"}
B3_REQUIRED_POLICY_KEYS = ("no_guessing", "listing_keyed_review")


def b3_residual_review_strategy(review_bucket: str) -> str:
    if review_bucket == "official_isin_candidate_requires_apply_gate":
        return "validate_official_b3_or_cotahist_isin_before_apply"
    if review_bucket == "scope_review_before_identifier_fill":
        return "decide_b3_core_extended_or_exclude_before_identifier_work"
    if review_bucket == "active_listing_evidence_required_source_gap":
        return "seek_current_active_b3_listing_status_then_identifier_source"
    if review_bucket == "fund_or_registry_identifier_source_gap":
        return "seek_official_fund_trust_registry_or_prospectus_isin"
    if review_bucket == "not_in_current_b3_directory_source_gap":
        return "keep_blank_until_current_b3_directory_or_registry_evidence"
    if review_bucket == "official_sector_candidate_requires_apply_gate":
        return "validate_official_b3_taxonomy_candidate_before_sector_apply"
    if review_bucket == "scope_review_before_sector_fill":
        return "decide_b3_core_extended_or_exclude_before_sector_work"
    if review_bucket == "ambiguous_b3_code_requires_manual_review":
        return "manual_b3_code_disambiguation_before_taxonomy_mapping"
    if review_bucket == "missing_probe_row_requires_parser_or_source_review":
        return "refresh_b3_taxonomy_probe_or_parser_before_sector_work"
    if review_bucket == "no_b3_classification_code_match_source_gap":
        return "keep_blank_until_stronger_official_b3_or_issuer_taxonomy"
    if review_bucket == "requires_stronger_official_taxonomy_source_gap":
        return "seek_stronger_official_taxonomy_before_sector_mapping"
    return "manual_b3_residual_review"


def b3_residual_source_gap_context(row: dict[str, Any]) -> str:
    return (
        f"gap_class={str(row.get('gap_class', '') or 'none')};"
        f"source_of_truth_outcome={str(row.get('source_of_truth_outcome', '') or 'none')};"
        f"review_needed={str(row.get('review_needed', '') or 'none')}"
    )


def b3_isin_official_source_context(row: dict[str, Any]) -> str:
    return (
        f"b3_instruments_equities_match={str(row.get('b3_instruments_equities_match', '') or 'none')};"
        f"b3_instruments_equities_isin={str(row.get('b3_instruments_equities_isin', '') or 'none')};"
        f"cotahist_probe_decision={str(row.get('cotahist_probe_decision', '') or 'none')};"
        f"cotahist_probe_isin={str(row.get('cotahist_probe_isin', '') or 'none')}"
    )


def b3_sector_official_source_context(row: dict[str, Any]) -> str:
    return (
        f"b3_probe_decision={str(row.get('b3_probe_decision', '') or 'none')};"
        f"b3_code={str(row.get('b3_code', '') or 'none')};"
        f"b3_sector_update={str(row.get('b3_sector_update', '') or 'none')};"
        f"b3_source_url={str(row.get('b3_source_url', '') or 'none')}"
    )


def b3_residual_scope_review_context(row: dict[str, Any]) -> str:
    return (
        f"current_instrument_scope={row.get('current_instrument_scope', '') or 'none'};"
        f"current_scope_reason={row.get('current_scope_reason', '') or 'none'};"
        f"source_of_truth_outcome={row.get('source_of_truth_outcome', '') or 'none'}"
    )


def b3_residual_review_context(row: dict[str, Any]) -> str:
    return (
        f"residual_decision={str(row.get('residual_decision', '') or 'none')};"
        f"review_bucket={str(row.get('review_bucket', '') or 'none')};"
        f"apply_eligibility={str(row.get('apply_eligibility', '') or 'none')};"
        f"verification_evidence_required={str(row.get('verification_evidence_required', '') or 'none')}"
    )


def verification_evidence_for_b3_bucket(
    review_bucket: str,
    *,
    official_candidate_bucket: str,
    official_candidate_evidence: str,
    scope_bucket: str,
    scope_evidence: str,
) -> str:
    if review_bucket == official_candidate_bucket:
        return official_candidate_evidence
    if review_bucket == scope_bucket:
        return scope_evidence
    if review_bucket == "active_listing_evidence_required_source_gap":
        return "current_active_b3_listing_status_plus_direct_identifier_source"
    if review_bucket == "fund_or_registry_identifier_source_gap":
        return "official_fund_trust_registry_or_prospectus_with_exact_symbol_product_name_and_valid_isin"
    if review_bucket == "not_in_current_b3_directory_source_gap":
        return "new_current_b3_directory_or_official_delisting_inactive_evidence"
    if review_bucket == "ambiguous_b3_code_requires_manual_review":
        return "manual_b3_code_disambiguation_against_exact_listing_name_before_sector_mapping"
    if review_bucket == "missing_probe_row_requires_parser_or_source_review":
        return "parser_or_source_refresh_to_produce_b3_classification_probe_row"
    if review_bucket == "no_b3_classification_code_match_source_gap":
        return "stronger_official_b3_or_issuer_taxonomy_source_with_exact_listing_match"
    if review_bucket == "requires_stronger_official_taxonomy_source_gap":
        return "stronger_official_taxonomy_source_before_canonical_sector_mapping"
    return "manual_review_required"


def recommended_next_source_for_b3_bucket(review_bucket: str, *, is_sector: bool) -> str:
    if is_sector:
        if review_bucket == "official_sector_candidate_requires_apply_gate":
            return "Official B3 taxonomy/probe row with exact listing, B3 code, and canonical sector mapping."
        if review_bucket == "scope_review_before_sector_fill":
            return "Current B3 source plus reviewed core, extended, or exclude scope decision before sector work."
        if review_bucket == "ambiguous_b3_code_requires_manual_review":
            return "Official B3 taxonomy row plus manual exact-name disambiguation for the listing key."
        if review_bucket == "missing_probe_row_requires_parser_or_source_review":
            return "Refreshed official B3 taxonomy source or parser output that produces a listing-keyed probe row."
        if review_bucket == "no_b3_classification_code_match_source_gap":
            return "Stronger official B3 or issuer taxonomy source exposing sector for the exact listing."
        if review_bucket == "requires_stronger_official_taxonomy_source_gap":
            return "Stronger official taxonomy source before any canonical sector mapping."
        return "Manual B3 sector review source with exact listing-key evidence."
    if review_bucket == "official_isin_candidate_requires_apply_gate":
        return "Official B3 InstrumentsEquities or COTAHIST row exposing a valid ISIN for the exact listing."
    if review_bucket == "scope_review_before_identifier_fill":
        return "Current B3 source plus reviewed core, extended, or exclude scope decision before identifier work."
    if review_bucket == "active_listing_evidence_required_source_gap":
        return "Current B3 active-listing status source plus a direct official identifier source."
    if review_bucket == "fund_or_registry_identifier_source_gap":
        return "Official fund/trust registry, prospectus, CSD, or reviewed identifier feed with exact product match."
    if review_bucket == "not_in_current_b3_directory_source_gap":
        return "Current official B3 directory, CSD/security registry, or official delisting/inactive evidence."
    return "Manual B3 identifier review source with exact listing-key evidence."


def source_gate_for_b3_bucket(review_bucket: str, *, is_sector: bool) -> str:
    if is_sector:
        if review_bucket == "official_sector_candidate_requires_apply_gate":
            return "Apply only after listing-key, taxonomy row, and canonical sector mapping validation."
        if review_bucket == "scope_review_before_sector_fill":
            return "No sector fill until the B3 listing is reviewed as core, extended, or excluded."
        if review_bucket == "ambiguous_b3_code_requires_manual_review":
            return "Keep sector blank until manual disambiguation proves the exact B3 code/listing match."
        if review_bucket == "missing_probe_row_requires_parser_or_source_review":
            return "Keep sector blank until the official parser/source produces a listing-keyed taxonomy probe row."
        if review_bucket == "no_b3_classification_code_match_source_gap":
            return "Keep stock_sector blank until official B3 or issuer taxonomy evidence matches the exact listing."
        if review_bucket == "requires_stronger_official_taxonomy_source_gap":
            return "Keep sector blank until stronger official taxonomy evidence is available."
        return "Manual B3 sector review required before any sector change."
    if review_bucket == "official_isin_candidate_requires_apply_gate":
        return "Apply only after listing-key, issuer/product name, and ISIN checksum validation."
    if review_bucket == "scope_review_before_identifier_fill":
        return "No ISIN fill until the B3 listing is reviewed as core, extended, or excluded."
    if review_bucket == "active_listing_evidence_required_source_gap":
        return "Keep ISIN blank until active listing status and a direct identifier source are both present."
    if review_bucket == "fund_or_registry_identifier_source_gap":
        return "Keep ISIN blank until official fund/trust product evidence exposes a valid checksum ISIN."
    if review_bucket == "not_in_current_b3_directory_source_gap":
        return "Keep ISIN blank unless the listing reappears in a current official directory or registry evidence."
    return "Manual B3 identifier review required before any ISIN change."


B3_CAMPAIGN_DIAGNOSTIC_EVIDENCE_KEYS = (
    "b3_dataset_rows",
    "b3_active_exchange_directory_rows",
    "b3_all_masterfile_rows",
    "b3_masterfile_matched_dataset_rows",
    "b3_masterfile_missing_dataset_rows",
    "b3_masterfile_dataset_match_rate",
    "b3_official_any_source_matched_dataset_rows",
    "b3_official_any_source_missing_dataset_rows",
    "b3_official_any_source_match_rate",
    "b3_missing_category_totals",
    "b3_missing_asset_type_totals",
    "b3_missing_source_presence_totals",
    "b3_missing_examples",
    "b3_masterfile_gap_review_rows",
    "b3_masterfile_gap_review_open_rows",
    "b3_masterfile_gap_review_closed_no_data_change_rows",
    "b3_masterfile_gap_review_source_presence_totals",
    "b3_masterfile_gap_review_open_source_presence_totals",
    "b3_masterfile_gap_review_bucket_totals",
    "b3_masterfile_gap_review_resolution_queue_totals",
    "b3_masterfile_gap_review_open_resolution_queue_totals",
    "b3_masterfile_gap_review_open_next_source_totals",
    "b3_masterfile_gap_review_open_evidence_path_totals",
    "b3_masterfile_gap_review_source_gap_resolution_gate_totals",
    "top_open_b3_masterfile_review_rows",
    "b3_masterfile_gap_review_strategy_totals",
    "b3_masterfile_gap_review_resolution_queue_asset_type_totals",
    "b3_masterfile_gap_review_resolution_queue_gap_category_totals",
    "b3_masterfile_gap_review_candidate_source_totals",
    "b3_masterfile_gap_review_candidate_sector_present_rows",
    "b3_masterfile_gap_review_candidate_isin_present_rows",
    "b3_masterfile_gap_review_candidate_category_review_decision_totals",
    "b3_masterfile_gap_review_candidate_category_mismatch_rows",
    "b3_masterfile_gap_review_candidate_category_mismatch_examples",
    "b3_masterfile_gap_review_coverage_diagnosis",
    "b3_masterfile_gap_review_official_subset_review_decision_totals",
    "b3_masterfile_gap_review_official_subset_closure_eligibility_totals",
    "b3_masterfile_gap_review_official_subset_closure_ready_rows",
    "top_open_b3_masterfile_review_batches",
    "b3_etf_category_apply_rows",
    "b3_etf_category_written_updates",
    "b3_etf_category_apply_decision_totals",
    "b3_etf_category_update_totals",
    "b3_missing_isin_residual_rows",
    "b3_missing_sector_residual_rows",
    "isin_review_strategy_totals",
    "top_b3_isin_review_batches",
    "b3_isin_official_source_identifier_exposure",
    "sector_review_strategy_totals",
    "top_b3_sector_review_batches",
    "sector_b3_code_shape_totals",
    "sector_alphanumeric_b3_code_rows",
    "sector_alphanumeric_b3_code_examples",
    "b3_residual_workstream_rows",
    "b3_residual_workstream_priority_totals",
    "b3_residual_workstream_readiness_totals",
)
B3_CAMPAIGN_DELTA_EVIDENCE_KEYS = (
    "current_b3_masterfile_missing_dataset_rows",
    "current_b3_masterfile_dataset_match_rate",
    "current_b3_official_any_source_missing_dataset_rows",
    "current_b3_official_any_source_match_rate",
    "current_b3_masterfile_gap_review_rows",
    "current_b3_masterfile_gap_review_open_rows",
    "current_b3_etf_category_written_updates",
    "current_b3_sector_alphanumeric_code_rows",
    "campaign_start_coverage_snapshot",
    "source_gap_delta",
    "warn_quarantine_delta",
)


def b3_masterfile_gap_review_strategy(b3_resolution_queue: str) -> str:
    if b3_resolution_queue == "official_subset_category_already_reflected_scope_review":
        return "confirm_official_subset_scope_or_parser_gap_before_closing_directory_gap"
    if b3_resolution_queue == "official_bdr_subset_without_category_source_gap_closed":
        return "close_bdr_subset_gap_without_data_change_keep_category_source_gap"
    if b3_resolution_queue == "official_subset_without_category_scope_review":
        return "review_official_subset_scope_before_category_or_parser_change"
    if b3_resolution_queue == "official_subset_category_requires_review":
        return "review_official_subset_category_and_scope_before_apply_gate"
    if b3_resolution_queue == "absent_from_all_b3_sources_local_share_source_gap":
        return "keep_local_share_gap_until_current_official_b3_or_issuer_evidence"
    if b3_resolution_queue == "absent_from_all_b3_sources_fund_or_receipt_source_gap":
        return "keep_fund_or_receipt_gap_until_current_official_b3_or_issuer_evidence"
    if b3_resolution_queue == "absent_from_all_b3_sources_unclassified_source_gap":
        return "manual_b3_source_gap_review_before_any_data_change"
    return "manual_b3_masterfile_gap_review"


def b3_masterfile_gap_recommended_next_source(b3_resolution_queue: str) -> str:
    if b3_resolution_queue == "official_subset_category_already_reflected_scope_review":
        return "Current active B3 exchange directory or reviewed parser/scope evidence for the listed ETF/fund subset."
    if b3_resolution_queue == "official_bdr_subset_without_category_source_gap_closed":
        return "Official B3 BDR/ETF subset confirms the listing; keep category/ISIN unchanged until stronger B3 or issuer evidence exposes them."
    if b3_resolution_queue == "official_subset_without_category_scope_review":
        return "Current active B3 exchange directory, B3 ETF/BDR subset source, or issuer product page with exact listing match."
    if b3_resolution_queue == "official_subset_category_requires_review":
        return "Official B3 subset source plus category taxonomy evidence with exact listing-key match."
    if b3_resolution_queue == "absent_from_all_b3_sources_local_share_source_gap":
        return "Current B3 exchange directory, B3 issuer page, CVM filing, or issuer investor-relations listing evidence."
    if b3_resolution_queue == "absent_from_all_b3_sources_fund_or_receipt_source_gap":
        return "Current B3 fund/ETF/BDR source, issuer/sponsor page, prospectus, or official product registry."
    if b3_resolution_queue == "absent_from_all_b3_sources_unclassified_source_gap":
        return "Current B3 directory or issuer/registry evidence with exact ticker, name, and instrument type."
    return "Manual B3 source review with exact listing-key evidence."


def b3_masterfile_gap_source_gate(b3_resolution_queue: str) -> str:
    if b3_resolution_queue == "official_subset_category_already_reflected_scope_review":
        return "Close the directory gap only after confirming the subset is intentionally outside the active directory or parser-scoped."
    if b3_resolution_queue == "official_bdr_subset_without_category_source_gap_closed":
        return "No B3 category, ISIN, name, symbol, or scope change is authorized; the official BDR subset evidence only closes the active-directory gap."
    if b3_resolution_queue == "official_subset_without_category_scope_review":
        return "No category or scope change until the official subset row is reviewed against the dataset listing."
    if b3_resolution_queue == "official_subset_category_requires_review":
        return "Apply category only after official subset category, listing key, and current dataset category are reviewed."
    if b3_resolution_queue == "absent_from_all_b3_sources_local_share_source_gap":
        return "Keep row as source gap until current official B3 or issuer evidence proves the active local-share listing."
    if b3_resolution_queue == "absent_from_all_b3_sources_fund_or_receipt_source_gap":
        return "Keep row as source gap until current official product evidence proves active fund, ETF, BDR, or receipt listing."
    if b3_resolution_queue == "absent_from_all_b3_sources_unclassified_source_gap":
        return "No data change without exact current official evidence for ticker, name, and instrument type."
    return "Manual review required before any data change."


def b3_masterfile_gap_listing_context(row: dict[str, Any]) -> str:
    return (
        f"listing_key={row.get('listing_key', '') or 'none'};"
        f"ticker={row.get('ticker', '') or 'none'};"
        f"asset_type={row.get('asset_type', '') or 'none'};"
        f"b3_gap_category={row.get('b3_gap_category', '') or 'none'};"
        f"current_etf_category={row.get('current_etf_category', '') or 'none'}"
    )


def b3_masterfile_gap_official_candidate_context(row: dict[str, Any]) -> str:
    return (
        f"source_presence={row.get('source_presence', '') or 'none'};"
        f"candidate_sources={row.get('candidate_sources', '') or 'none'};"
        f"candidate_isins_present={'true' if row.get('candidate_isins') else 'false'};"
        f"candidate_sectors_present={'true' if row.get('candidate_sectors') else 'false'};"
        f"active_exchange_directory_match={row.get('active_exchange_directory_match', '') or 'none'};"
        f"any_official_b3_source_match={row.get('any_official_b3_source_match', '') or 'none'}"
    )


def b3_masterfile_gap_review_gate_context(row: dict[str, Any]) -> str:
    return (
        f"b3_resolution_queue={row.get('b3_resolution_queue', '') or 'none'};"
        f"residual_decision={row.get('residual_decision', '') or 'none'};"
        f"review_bucket={row.get('review_bucket', '') or 'none'};"
        f"official_subset_review_decision={row.get('official_subset_review_decision', '') or 'none'};"
        f"official_subset_closure_eligibility={row.get('official_subset_closure_eligibility', '') or 'none'};"
        f"apply_eligibility={row.get('apply_eligibility', '') or 'none'};"
        f"verification_evidence_required={row.get('verification_evidence_required', '') or 'none'}"
    )


REQUIRED_ASX_RESIDUAL_SUMMARY_KEYS = (
    "rows",
    "field_totals",
    "asset_type_totals",
    "core_exclusion_candidate_rows",
    "core_exclusion_candidate_field_totals",
    "core_exclusion_candidate_asset_type_totals",
    "core_exclusion_candidate_gap_class_totals",
    "core_exclusion_candidate_resolution_queue_totals",
    "core_exclusion_candidate_official_source_totals",
    "core_exclusion_candidate_official_capability_totals",
    "gap_class_totals",
    "source_of_truth_outcome_totals",
    "asx_residual_backlog",
    "asx_residual_backlog_queue_totals",
    "asx_residual_backlog_evidence_required_totals",
    "asx_resolution_queue_totals",
    "asx_resolution_queue_field_totals",
    "asx_resolution_queue_gap_class_totals",
    "asx_resolution_queue_official_source_totals",
    "asx_resolution_queue_review_strategy_totals",
    "asx_resolution_queue_evidence_required_totals",
    "asx_resolution_queue_official_capability_totals",
    "top_asx_resolution_review_batches",
    "residual_decision_totals",
    "review_bucket_totals",
    "review_priority_totals",
    "apply_eligibility_totals",
    "verification_evidence_required_totals",
    "asx_isin_probe_decision_totals",
    "official_masterfile_match_totals",
    "official_masterfile_exposes_isin_totals",
    "official_masterfile_exposes_sector_totals",
    "official_masterfile_source_totals",
    "policy",
)
REQUIRED_ASX_RESIDUAL_ROW_KEYS = (
    "listing_key",
    "ticker",
    "exchange",
    "asset_type",
    "field",
    "target_field",
    "gap_class",
    "source_of_truth_outcome",
    "review_needed",
    "recommended_next_source",
    "source_gate",
    "official_source_context",
    "official_capability",
    "asx_resolution_queue",
    "residual_decision",
    "review_bucket",
    "review_priority",
    "review_strategy",
    "apply_eligibility",
    "verification_evidence_required",
    "recommended_next_action",
)
ASX_RESIDUAL_FIELDS = {"missing_isin_primary", "missing_etf_category"}
ASX_REVIEW_BUCKETS = {
    "identifier_source_gap",
    "identity_mismatch_requires_manual_review",
    "official_isin_candidate_requires_apply_gate",
    "product_taxonomy_source_gap",
    "scope_review_before_any_data_fill",
}
ASX_REVIEW_PRIORITIES = {"P1", "P2", "P3"}
ASX_REQUIRED_POLICY_KEYS = ("official_first", "no_guessing", "scope_first")
ASX_REVIEW_STRATEGIES = {
    "apply_candidate_only_after_asx_isin_workbook_gates",
    "keep_category_blank_until_asx_product_taxonomy_source",
    "keep_isin_blank_until_current_asx_or_registry_source",
    "manual_asx_residual_review_required",
    "manual_identity_review_before_asx_isin_apply",
    "scope_review_before_asx_category_or_metadata_enrichment",
    "scope_review_before_asx_identifier_enrichment",
    "seek_official_or_reviewed_asx_product_taxonomy",
    "seek_registry_or_issuer_isin_source",
}
ASX_CAMPAIGN_EVIDENCE_KEYS = (
    "asx_residual_rows",
    "field_totals",
    "asset_type_totals",
    "asx_core_exclusion_candidate_rows",
    "asx_core_exclusion_candidate_field_totals",
    "asx_core_exclusion_candidate_asset_type_totals",
    "asx_core_exclusion_candidate_gap_class_totals",
    "asx_core_exclusion_candidate_resolution_queue_totals",
    "asx_core_exclusion_candidate_official_source_totals",
    "asx_core_exclusion_candidate_official_capability_totals",
    "gap_class_totals",
    "source_of_truth_outcome_totals",
    "asx_resolution_queue_totals",
    "asx_resolution_queue_field_totals",
    "asx_resolution_queue_gap_class_totals",
    "asx_resolution_queue_official_source_totals",
    "asx_resolution_queue_review_strategy_totals",
    "asx_resolution_queue_evidence_required_totals",
    "asx_resolution_queue_official_capability_totals",
    "top_asx_resolution_review_batches",
    "residual_decision_totals",
    "review_bucket_totals",
    "review_priority_totals",
    "apply_eligibility_totals",
    "verification_evidence_required_totals",
    "asx_isin_probe_decision_totals",
    "official_masterfile_match_totals",
    "official_masterfile_exposes_isin_totals",
    "official_masterfile_exposes_sector_totals",
    "official_masterfile_source_totals",
)
ASX_CAMPAIGN_DELTA_EVIDENCE_KEYS = (
    "current_asx_residual_rows",
    "asx_core_exclusion_candidate_rows",
    "campaign_start_residual_snapshot",
    "source_gap_delta",
    "warn_quarantine_delta",
)


def asx_review_strategy_for(queue: str) -> tuple[str, str]:
    if queue == "core_exclusion_candidate_identifier_scope_review":
        return (
            "scope_review_before_asx_identifier_enrichment",
            "scope_decision_for_core_extended_or_exclude_before_identifier_or_category_fill",
        )
    if queue in {
        "core_exclusion_candidate_category_scope_review",
        "core_exclusion_candidate_scope_review",
    }:
        return (
            "scope_review_before_asx_category_or_metadata_enrichment",
            "scope_decision_for_core_extended_or_exclude_before_identifier_or_category_fill",
        )
    if queue == "official_asx_isin_candidate_apply_gate":
        return (
            "apply_candidate_only_after_asx_isin_workbook_gates",
            "official_asx_isin_workbook_exact_code_name_numeric_token_and_valid_isin_checksum",
        )
    if queue == "asx_isin_workbook_name_mismatch_manual_review":
        return (
            "manual_identity_review_before_asx_isin_apply",
            "manual_exact_name_or_alias_resolution_before_isin_apply",
        )
    if queue == "missing_isin_not_in_current_asx_isin_workbook":
        return (
            "keep_isin_blank_until_current_asx_or_registry_source",
            "direct_registry_issuer_or_official_asx_identifier_source_with_exact_listing_match",
        )
    if queue in {
        "missing_isin_official_asx_feeds_do_not_expose_isin",
        "missing_isin_requires_registry_or_issuer_source",
    }:
        return (
            "seek_registry_or_issuer_isin_source",
            "direct_registry_issuer_or_official_asx_identifier_source_with_exact_listing_match",
        )
    if queue == "missing_etf_category_official_taxonomy_unavailable":
        return (
            "keep_category_blank_until_asx_product_taxonomy_source",
            "official_or_reviewed_product_taxonomy_with_exact_listing_match",
        )
    if queue == "missing_etf_category_requires_official_product_taxonomy":
        return (
            "seek_official_or_reviewed_asx_product_taxonomy",
            "official_or_reviewed_product_taxonomy_with_exact_listing_match",
        )
    return ("manual_asx_residual_review_required", "manual_review_required")


def asx_recommended_next_source_for(queue: str, official_source: str) -> str:
    source_label = official_source if official_source != "none" else "current official ASX, registry, or issuer source"
    if queue == "core_exclusion_candidate_identifier_scope_review":
        return f"{source_label} plus reviewed scope decision for core, extended, or exclude before identifier work."
    if queue in {
        "core_exclusion_candidate_category_scope_review",
        "core_exclusion_candidate_scope_review",
    }:
        return f"{source_label} plus reviewed scope decision before identifier, category, or metadata work."
    if queue == "official_asx_isin_candidate_apply_gate":
        return "Official ASX ISIN workbook row with exact code, name token, instrument context, and valid ISIN checksum."
    if queue == "asx_isin_workbook_name_mismatch_manual_review":
        return "Manual exact-name or reviewed-alias resolution against ASX workbook, issuer, or registry evidence."
    if queue == "missing_isin_not_in_current_asx_isin_workbook":
        return "Current ASX ISIN workbook, registry, issuer, trustee, or prospectus source with exact listing match."
    if queue in {
        "missing_isin_official_asx_feeds_do_not_expose_isin",
        "missing_isin_requires_registry_or_issuer_source",
    }:
        return "Direct registry, issuer, trustee, prospectus, or official ASX identifier source with exact listing match."
    if queue == "missing_etf_category_official_taxonomy_unavailable":
        return "Official ASX product taxonomy, issuer/sponsor product page, PDS, or reviewed product taxonomy source."
    if queue == "missing_etf_category_requires_official_product_taxonomy":
        return "Official ASX product taxonomy, issuer/sponsor product page, PDS, or reviewed product taxonomy source."
    return "Manual ASX residual review with exact listing-key evidence."


def asx_source_gate_for(queue: str) -> str:
    if queue == "core_exclusion_candidate_identifier_scope_review":
        return "No ISIN or category enrichment until the listing is reviewed as core, extended, or excluded."
    if queue in {
        "core_exclusion_candidate_category_scope_review",
        "core_exclusion_candidate_scope_review",
    }:
        return "No identifier, category, or metadata fill until scope is decided with official listing evidence."
    if queue == "official_asx_isin_candidate_apply_gate":
        return "Apply ISIN only after exact ASX code/name/instrument match and checksum validation."
    if queue == "asx_isin_workbook_name_mismatch_manual_review":
        return "Do not apply workbook ISIN until the name mismatch is manually resolved with exact listing evidence."
    if queue == "missing_isin_not_in_current_asx_isin_workbook":
        return "Keep ISIN blank until current ASX, registry, issuer, or trustee evidence proves the identifier."
    if queue in {
        "missing_isin_official_asx_feeds_do_not_expose_isin",
        "missing_isin_requires_registry_or_issuer_source",
    }:
        return "Keep ISIN blank until direct official or reviewed identifier evidence exists."
    if queue == "missing_etf_category_official_taxonomy_unavailable":
        return "Keep ETF category blank until exact product taxonomy evidence exists."
    if queue == "missing_etf_category_requires_official_product_taxonomy":
        return "Keep ETF category blank until exact product taxonomy evidence exists."
    return "Manual review required before any data change."


REQUIRED_WEAK_SECTOR_SUMMARY_KEYS = (
    "exchanges",
    "rows",
    "exchange_totals",
    "official_sector_candidate_rows",
    "official_sector_candidate_exchange_totals",
    "official_sector_candidate_value_totals",
    "scope_review_rows",
    "scope_review_exchange_totals",
    "scope_review_gap_class_totals",
    "masterfile_without_sector_rows",
    "masterfile_without_sector_exchange_totals",
    "gap_class_totals",
    "source_of_truth_outcome_totals",
    "weak_sector_backlog",
    "weak_sector_backlog_queue_totals",
    "weak_sector_backlog_evidence_required_totals",
    "weak_sector_resolution_queue_totals",
    "weak_sector_resolution_queue_exchange_totals",
    "weak_sector_resolution_queue_gap_class_totals",
    "weak_sector_resolution_queue_official_source_totals",
    "weak_sector_resolution_queue_review_strategy_totals",
    "weak_sector_resolution_queue_official_capability_totals",
    "venue_backlog_exchange_queue_totals",
    "venue_backlog_exchange_official_capability_totals",
    "top_venue_backlog_batches",
    "top_weak_sector_resolution_review_batches",
    "residual_decision_totals",
    "review_bucket_totals",
    "review_priority_totals",
    "apply_eligibility_totals",
    "verification_evidence_required_totals",
    "official_masterfile_match_totals",
    "official_masterfile_exposes_sector_totals",
    "official_masterfile_source_totals",
    "official_sector_value_totals",
    "policy",
)
REQUIRED_WEAK_SECTOR_ROW_KEYS = (
    "listing_key",
    "ticker",
    "exchange",
    "asset_type",
    "gap_class",
    "source_of_truth_outcome",
    "review_needed",
    "recommended_next_source",
    "source_gate",
    "official_source_context",
    "official_capability",
    "official_masterfile_match",
    "official_masterfile_sources",
    "official_masterfile_exposes_sector",
    "weak_sector_resolution_queue",
    "residual_decision",
    "review_bucket",
    "review_priority",
    "apply_eligibility",
    "verification_evidence_required",
    "review_strategy",
    "recommended_next_action",
)
WEAK_SECTOR_REVIEW_BUCKETS = {
    "official_masterfile_without_sector_source_gap",
    "official_sector_candidate_requires_normalization_gate",
    "scope_review_before_sector_fill",
    "venue_official_taxonomy_unavailable_source_gap",
    "venue_specific_sector_source_gap",
}
WEAK_SECTOR_EXCHANGES = {"BK", "CSE_LK", "CSE_MA", "Euronext", "NGX", "OSL", "PSE", "SEM"}
WEAK_SECTOR_REVIEW_PRIORITIES = {"P1", "P2", "P3", "P4"}
WEAK_SECTOR_REQUIRED_POLICY_KEYS = ("venue_specific", "official_first", "no_guessing")
WEAK_SECTOR_REVIEW_STRATEGIES = {
    "keep_blank_until_official_masterfile_or_issuer_sector_source",
    "manual_weak_sector_review_required",
    "normalize_official_sector_candidate_before_apply",
    "restore_or_add_venue_official_taxonomy_parser",
    "scope_review_before_weak_sector_enrichment",
    "seek_reviewed_venue_specific_taxonomy_source",
}
WEAK_SECTOR_CAMPAIGN_EVIDENCE_KEYS = (
    "weak_sector_rows",
    "exchanges",
    "exchange_totals",
    "top_exchange_residuals",
    "official_sector_candidate_rows",
    "official_sector_candidate_exchange_totals",
    "official_sector_candidate_value_totals",
    "scope_review_rows",
    "scope_review_exchange_totals",
    "scope_review_gap_class_totals",
    "masterfile_without_sector_rows",
    "masterfile_without_sector_exchange_totals",
    "gap_class_totals",
    "source_of_truth_outcome_totals",
    "weak_sector_backlog",
    "weak_sector_backlog_queue_totals",
    "weak_sector_backlog_evidence_required_totals",
    "weak_sector_resolution_queue_totals",
    "weak_sector_resolution_queue_exchange_totals",
    "weak_sector_resolution_queue_gap_class_totals",
    "weak_sector_resolution_queue_official_source_totals",
    "weak_sector_resolution_queue_review_strategy_totals",
    "weak_sector_resolution_queue_official_capability_totals",
    "venue_backlog_exchange_queue_totals",
    "venue_backlog_exchange_official_capability_totals",
    "top_venue_backlog_batches",
    "top_weak_sector_resolution_review_batches",
    "residual_decision_totals",
    "review_bucket_totals",
    "review_priority_totals",
    "apply_eligibility_totals",
    "verification_evidence_required_totals",
    "official_masterfile_match_totals",
    "official_masterfile_exposes_sector_totals",
    "official_masterfile_source_totals",
    "official_sector_value_totals",
    "ngx_applied_rows",
    "ngx_written_updates",
)
WEAK_SECTOR_CAMPAIGN_DELTA_EVIDENCE_KEYS = (
    "ngx_review_rows",
    "ngx_written_updates",
    "current_weak_sector_rows",
    "official_sector_candidate_rows",
    "scope_review_rows",
    "masterfile_without_sector_rows",
    "campaign_start_sector_coverage_snapshot",
    "source_gap_delta",
    "warn_quarantine_delta",
)


def weak_sector_review_strategy_for(queue: str) -> tuple[str, str]:
    if queue == "official_sector_candidate_normalization_review":
        return (
            "normalize_official_sector_candidate_before_apply",
            "official_venue_sector_value_with_canonical_mapping_and_exact_listing_key_match",
        )
    if queue == "core_exclusion_candidate_scope_review_before_sector_fill":
        return (
            "scope_review_before_weak_sector_enrichment",
            "scope_decision_for_core_extended_or_exclude_before_sector_enrichment",
        )
    if queue == "official_masterfile_without_sector_source_gap":
        return (
            "keep_blank_until_official_masterfile_or_issuer_sector_source",
            "official_masterfile_or_issuer_taxonomy_update_exposing_sector_for_exact_listing",
        )
    if queue == "venue_official_taxonomy_unavailable_source_gap":
        return (
            "restore_or_add_venue_official_taxonomy_parser",
            "new_or_restored_official_venue_industry_taxonomy_source",
        )
    if queue == "venue_specific_sector_source_gap":
        return (
            "seek_reviewed_venue_specific_taxonomy_source",
            "reviewed_venue_specific_taxonomy_source_with_exact_listing_match",
        )
    return ("manual_weak_sector_review_required", "manual_review_required")


def weak_sector_recommended_next_source_for(queue: str, official_source: str) -> str:
    source = official_source if official_source and official_source != "none" else "venue official source"
    if queue == "official_sector_candidate_normalization_review":
        return f"Official sector value from {source} plus canonical sector mapping."
    if queue == "core_exclusion_candidate_scope_review_before_sector_fill":
        return "Official listing scope evidence before any sector enrichment."
    if queue == "official_masterfile_without_sector_source_gap":
        return f"Updated official masterfile or issuer taxonomy exposing sector for the exact listing; current source: {source}."
    if queue == "venue_official_taxonomy_unavailable_source_gap":
        return "Official venue industry or sector taxonomy source for the exchange."
    if queue == "venue_specific_sector_source_gap":
        return "Reviewed venue-specific official taxonomy source for the exact listing."
    return "Manual weak-sector review source."


def weak_sector_source_gate_for(queue: str) -> str:
    if queue == "official_sector_candidate_normalization_review":
        return "Apply only after exact listing-key match and canonical sector normalization."
    if queue == "core_exclusion_candidate_scope_review_before_sector_fill":
        return "No sector fill until the listing is confirmed as core, extended, or excluded."
    if queue == "official_masterfile_without_sector_source_gap":
        return "Keep sector blank until an official masterfile or issuer record exposes sector for the exact listing."
    if queue == "venue_official_taxonomy_unavailable_source_gap":
        return "Keep sector blank until a venue-official taxonomy parser or source exists."
    if queue == "venue_specific_sector_source_gap":
        return "Keep sector blank until reviewed venue-specific taxonomy evidence matches the exact listing."
    return "Manual review required; ticker/name inference is not sufficient."


REQUIRED_CAMPAIGN_REVIEW_POLICY_KEYS = (
    "campaign_scope",
    "no_guessing",
    "source_authority",
    "uncertain_gap_handling",
    "traceability",
)
CAMPAIGN_REVIEW_POLICY_REQUIRED_MARKER_GROUPS = {
    "no_guessing": ("no inferred", "no guessing", "not inferred", "not guessed"),
    "source_authority": ("official", "exchange", "registry", "issuer", "source"),
    "uncertain_gap_handling": ("blank", "source gap", "source-gap", "unverified"),
    "traceability": ("listing-keyed", "listing_key", "review status", "source evidence"),
}

REVIEW_ARTIFACT_MARKERS = ("review", "queue", "plausibility")
APPLY_ARTIFACT_MARKERS = ("apply_report",)
REVIEW_GATE_KEY_MARKERS = ("eligibility", "evidence", "gate", "clearance")
REVIEW_ROW_CONTAINER_KEYS = ("rows", "review_items", "items")
REVIEW_ROW_IDENTITY_KEYS = (
    "listing_key",
    "review_key",
    "change_id",
)
REVIEW_ROW_LISTING_KEY_COLLECTIONS = (
    "existing_listing_keys",
    "old_listing_keys",
    "new_listing_keys",
    "old_scoped_listing_keys",
    "new_scoped_listing_keys",
)
REVIEW_ROW_CANDIDATE_IDENTITY_KEY_GROUPS = (
    ("official_source_key", "official_ticker"),
    ("financialdata_exchange", "financialdata_ticker"),
)
REVIEW_ROW_EVIDENCE_KEY_MARKERS = (
    "source",
    "review",
    "decision",
    "gap",
    "eligibility",
    "evidence",
    "gate",
    "official",
    "reason",
    "scope",
)
APPLY_ROW_EVIDENCE_KEYS = (
    "isin",
    "figi",
    "official_sector",
    "sector_update",
    "source",
    "official_source_key",
    "verification_evidence_required",
    "openfigi_probe_context",
    "existing_identifier_context",
    "apply_gate_context",
    "official_source_context",
    "category_review_context",
    "mapping_review_context",
)
SUPPLEMENT_REVIEW_ARTIFACT_NAME = "financialdata_isin_supplements_review.json"
SUPPLEMENT_ROW_REQUIRED_KEYS = (
    "financialdata_ticker",
    "financialdata_exchange",
    "decision",
    "reason",
    "financialdata_review_queue",
    "review_priority",
    "review_strategy",
    "apply_eligibility",
    "verification_evidence_required",
    "recommended_next_source",
    "source_gate",
    "financialdata_discovery_context",
    "official_identity_context",
    "supplement_review_context",
)
SUPPLEMENT_OFFICIAL_EVIDENCE_KEYS = (
    "official_ticker",
    "official_exchange",
    "official_isin",
    "official_source_key",
)
REVIEW_POLICY_REQUIRED_MARKER_GROUPS = {
    "review_or_no_guessing_gate": (
        "review",
        "no_guess",
        "no guessing",
        "not changed",
        "never authorize",
        "remain blank",
        "source gaps remain blank",
        "not automatically",
        "excluded until",
        "discovery signals only",
        "collision evidence",
    ),
    "authority_or_evidence_gate": (
        "official",
        "listing_key",
        "source",
        "evidence",
        "gate",
        "valid",
        "stronger",
    ),
}
EXPECTED_CAMPAIGN_KEYS_BY_PRIORITY = {
    1: "b3",
    2: "otc",
    3: "canada",
    4: "asx",
    5: "weak_sector",
    6: "masterfile_collisions",
    7: "symbol_changes",
    8: "ohlcv",
    9: "freshness",
    10: "baseline",
}


def is_review_artifact_path(artifact_path: str) -> bool:
    artifact_name = Path(artifact_path).name
    return artifact_path.endswith(".json") and any(marker in artifact_name for marker in REVIEW_ARTIFACT_MARKERS)


def is_apply_artifact_path(artifact_path: str) -> bool:
    artifact_name = Path(artifact_path).name
    return artifact_path.endswith(".json") and any(marker in artifact_name for marker in APPLY_ARTIFACT_MARKERS)


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def is_valid_iso_utc_timestamp(value: str) -> bool:
    if not value.endswith("Z"):
        return False
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return True


def parse_iso_utc_timestamp(value: str) -> datetime | None:
    if not is_valid_iso_utc_timestamp(value):
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def gate_lookup(validation_report: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {gate.get("name", ""): gate for gate in validation_report.get("gates", []) if gate.get("name")}


def evaluate_gate_group(gates: dict[str, dict[str, Any]], gate_names: list[str]) -> dict[str, Any]:
    gate_rows = [gates.get(name, {"name": name, "passed": False, "actual": None, "missing": True}) for name in gate_names]
    return {
        "passed": all(bool(row.get("passed")) for row in gate_rows),
        "gates": gate_rows,
    }


def evaluate_release_source_report_integrity(
    source_reports: dict[str, str],
    *,
    root: Path = ROOT,
    release_generated_at: str = "",
) -> dict[str, Any]:
    checked: dict[str, dict[str, Any]] = {}
    missing_files: list[str] = []
    missing_generated_at: list[str] = []
    invalid_generated_at: dict[str, str] = {}
    generated_after_release: dict[str, dict[str, str]] = {}
    release_generated_at_invalid = bool(release_generated_at) and not is_valid_iso_utc_timestamp(release_generated_at)
    release_generated_at_dt = parse_iso_utc_timestamp(release_generated_at) if release_generated_at else None
    for key, relative_path in source_reports.items():
        path = root / relative_path
        row = {"path": relative_path, "exists": path.exists(), "generated_at": ""}
        if not path.exists():
            missing_files.append(key)
            checked[key] = row
            continue
        payload = load_json(path)
        generated_at = report_generated_at(payload)
        row["generated_at"] = generated_at
        if not generated_at:
            missing_generated_at.append(key)
        elif not is_valid_iso_utc_timestamp(generated_at):
            invalid_generated_at[key] = generated_at
        elif release_generated_at_dt and parse_iso_utc_timestamp(generated_at) > release_generated_at_dt:
            generated_after_release[key] = {
                "source_generated_at": generated_at,
                "release_generated_at": release_generated_at,
            }
        checked[key] = row
    return {
        "passed": (
            bool(checked)
            and not release_generated_at_invalid
            and not missing_files
            and not missing_generated_at
            and not invalid_generated_at
            and not generated_after_release
        ),
        "release_generated_at": release_generated_at,
        "release_generated_at_invalid": release_generated_at_invalid,
        "checked": checked,
        "missing_files": missing_files,
        "missing_generated_at": missing_generated_at,
        "invalid_generated_at": invalid_generated_at,
        "generated_after_release": generated_after_release,
    }


def evaluate_progress_markdown_traceability(
    reports: dict[str, dict[str, Any]] = PROGRESS_MARKDOWN_TRACEABILITY_REPORTS,
    *,
    root: Path = ROOT,
) -> dict[str, Any]:
    checked: list[str] = []
    missing_files: dict[str, list[str]] = {}
    missing_source_sections: list[str] = []
    missing_generated_at: list[str] = []
    generated_at_mismatches: dict[str, dict[str, str]] = {}
    missing_source_paths: dict[str, list[str]] = {}
    missing_source_keys: dict[str, list[str]] = {}
    for report_key, config in reports.items():
        json_relative = str(config.get("json", ""))
        markdown_relative = str(config.get("markdown", ""))
        json_path = root / json_relative
        markdown_path = root / markdown_relative
        checked.append(report_key)
        missing_for_report = []
        if not json_path.exists():
            missing_for_report.append(json_relative)
        if not markdown_path.exists():
            missing_for_report.append(markdown_relative)
        if missing_for_report:
            missing_files[report_key] = missing_for_report
            continue
        payload = load_json(json_path)
        generated_at = report_generated_at(payload)
        markdown = markdown_path.read_text(encoding="utf-8")
        if not generated_at:
            missing_generated_at.append(report_key)
        elif f"Generated: `{generated_at}`" not in markdown:
            generated_at_mismatches[report_key] = {
                "expected": generated_at,
                "markdown": markdown_relative,
            }
        if "## Source Files" not in markdown:
            missing_source_sections.append(report_key)
        expected_source_files = config.get("source_files", {})
        if not isinstance(expected_source_files, dict):
            expected_source_files = {}
        missing_paths = [
            path
            for path in expected_source_files.values()
            if f"`{path}`" not in markdown
        ]
        if missing_paths:
            missing_source_paths[report_key] = missing_paths
        missing_keys = [
            key
            for key in expected_source_files
            if f"`{key}`" not in markdown
        ]
        if missing_keys:
            missing_source_keys[report_key] = missing_keys
    return {
        "passed": (
            bool(checked)
            and not missing_files
            and not missing_source_sections
            and not missing_generated_at
            and not generated_at_mismatches
            and not missing_source_paths
            and not missing_source_keys
        ),
        "checked_reports": checked,
        "checked_count": len(checked),
        "missing_files": missing_files,
        "missing_source_sections": missing_source_sections,
        "missing_generated_at": missing_generated_at,
        "generated_at_mismatches": generated_at_mismatches,
        "missing_source_paths": missing_source_paths,
        "missing_source_keys": missing_source_keys,
    }


def evaluate_adanos_detection_simulation(payload: dict[str, Any]) -> dict[str, Any]:
    meta = payload.get("_meta", {})
    summary = payload.get("summary", {})
    if not isinstance(meta, dict):
        meta = {}
    if not isinstance(summary, dict):
        summary = {}
    source_files = meta.get("source_files", {})
    if not isinstance(source_files, dict):
        source_files = {}

    required_summary_counts = [
        "reference_rows",
        "alias_entries",
        "tickers_with_aliases",
        "positive_probes",
        "positive_misses",
        "negative_probes",
        "negative_hits",
    ]
    missing_summary_keys = [key for key in required_summary_counts if key not in summary]
    invalid_summary_counts = [
        key
        for key in required_summary_counts
        if key in summary and (not isinstance(summary.get(key), int) or isinstance(summary.get(key), bool))
    ]
    positive_results = payload.get("positive_results", [])
    negative_results = payload.get("negative_results", [])
    positive_misses = payload.get("positive_misses", [])
    negative_hits = payload.get("negative_hits", [])
    containers = {
        "positive_results": positive_results,
        "negative_results": negative_results,
        "positive_misses": positive_misses,
        "negative_hits": negative_hits,
    }
    invalid_containers = [key for key, value in containers.items() if not isinstance(value, list)]
    positive_results_list = positive_results if isinstance(positive_results, list) else []
    negative_results_list = negative_results if isinstance(negative_results, list) else []
    positive_misses_list = positive_misses if isinstance(positive_misses, list) else []
    negative_hits_list = negative_hits if isinstance(negative_hits, list) else []

    count_mismatches: dict[str, dict[str, Any]] = {}
    for field, rows in (
        ("positive_probes", positive_results_list),
        ("negative_probes", negative_results_list),
        ("positive_misses", positive_misses_list),
        ("negative_hits", negative_hits_list),
    ):
        if isinstance(summary.get(field), int) and summary.get(field) != len(rows):
            count_mismatches[field] = {"summary": summary.get(field), "rows": len(rows)}

    invalid_positive_probes = [
        index
        for index, row in enumerate(positive_results_list, start=1)
        if not isinstance(row, dict) or not row.get("id") or not row.get("text") or not row.get("expected_ticker")
    ]
    invalid_negative_probes = [
        index
        for index, row in enumerate(negative_results_list, start=1)
        if not isinstance(row, dict) or not row.get("id") or not row.get("text")
    ]
    required_zero_fields = {
        "positive_misses": summary.get("positive_misses"),
        "negative_hits": summary.get("negative_hits"),
    }
    nonzero_detection_failures = {
        key: value
        for key, value in required_zero_fields.items()
        if isinstance(value, int) and value != 0
    }
    minimum_probe_gaps = {
        "positive_probes": summary.get("positive_probes"),
        "negative_probes": summary.get("negative_probes"),
    }
    minimum_probe_gaps = {
        key: value
        for key, value in minimum_probe_gaps.items()
        if not isinstance(value, int) or value < 5
    }
    missing_meta_keys = [
        key
        for key in ("generated_at", "source_files")
        if key not in meta or meta.get(key) in ("", None, {}, [])
    ]
    invalid_generated_at = (
        str(meta.get("generated_at", ""))
        if meta.get("generated_at") and not is_valid_iso_utc_timestamp(str(meta.get("generated_at")))
        else ""
    )
    source_file_mismatch = (
        source_files.get("adanos_reference_csv") != "data/adanos/ticker_reference.csv"
    )
    return {
        "passed": (
            not missing_meta_keys
            and not invalid_generated_at
            and not source_file_mismatch
            and not missing_summary_keys
            and not invalid_summary_counts
            and not invalid_containers
            and not count_mismatches
            and not invalid_positive_probes
            and not invalid_negative_probes
            and not nonzero_detection_failures
            and not minimum_probe_gaps
            and isinstance(summary.get("reference_rows"), int)
            and summary.get("reference_rows", 0) > 0
            and isinstance(summary.get("alias_entries"), int)
            and summary.get("alias_entries", 0) > 0
            and isinstance(summary.get("tickers_with_aliases"), int)
            and summary.get("tickers_with_aliases", 0) > 0
        ),
        "generated_at": meta.get("generated_at", ""),
        "source_files": source_files,
        "missing_meta_keys": missing_meta_keys,
        "invalid_generated_at": invalid_generated_at,
        "source_file_mismatch": source_file_mismatch,
        "missing_summary_keys": missing_summary_keys,
        "invalid_summary_counts": invalid_summary_counts,
        "invalid_containers": invalid_containers,
        "count_mismatches": count_mismatches,
        "invalid_positive_probes": invalid_positive_probes[:20],
        "invalid_negative_probes": invalid_negative_probes[:20],
        "nonzero_detection_failures": nonzero_detection_failures,
        "minimum_probe_gaps": minimum_probe_gaps,
        "summary": summary,
    }


def evaluate_entry_quality_command_report(
    entry_quality_gate: dict[str, Any],
    validation_gates: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    meta = entry_quality_gate.get("_meta", {})
    required_keys = {
        "passed",
        "quarantine_count",
        "unexpected_warn_count",
        "warn_count",
        "allowed_warn_count",
    }
    missing_meta_keys = [
        key
        for key in ("generated_at", "entry_quality_csv", "warn_allowlist_csv", "policy")
        if not isinstance(meta, dict) or not meta.get(key)
    ]
    missing_keys = sorted(key for key in required_keys if key not in entry_quality_gate)
    expected_counts = {
        "quarantine_count": validation_gates.get("entry_quality_quarantine_count", {}).get("actual"),
        "unexpected_warn_count": validation_gates.get("entry_quality_unexpected_warn_count", {}).get("actual"),
    }
    mismatched_counts = {
        key: {"command_report": entry_quality_gate.get(key), "validation_gate": value}
        for key, value in expected_counts.items()
        if key in entry_quality_gate and value is not None and entry_quality_gate.get(key) != value
    }
    return {
        "passed": (
            bool(entry_quality_gate.get("passed"))
            and not missing_keys
            and not missing_meta_keys
            and not mismatched_counts
        ),
        "generated_at": meta.get("generated_at", "") if isinstance(meta, dict) else "",
        "missing_meta_keys": missing_meta_keys,
        "missing_keys": missing_keys,
        "mismatched_counts": mismatched_counts,
        "command_report": {
            key: entry_quality_gate.get(key)
            for key in sorted(required_keys)
            if key in entry_quality_gate
        },
        "expected_counts_from_validation": expected_counts,
    }


def is_nonnegative_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and value >= 0


def delta_value(row: Any) -> int | float:
    if not isinstance(row, dict):
        return 0
    value = row.get("delta", 0)
    return value if isinstance(value, (int, float)) and not isinstance(value, bool) else 0


def delta_direction(metric: str, delta: Any) -> str:
    if not isinstance(delta, (int, float)) or isinstance(delta, bool):
        return "non_numeric_delta_review_required"
    if delta == 0:
        return "unchanged"
    lower_is_better = (
        "source_gap" in metric
        or "warn" in metric
        or "quarantine" in metric
        or "collision" in metric
        or "missing" in metric
    )
    if lower_is_better:
        return "improved" if delta < 0 else "regressed"
    return "improved" if delta > 0 else "regressed"


def delta_review_policy(metric: str, delta: Any) -> str:
    direction = delta_direction(metric, delta)
    if direction == "unchanged":
        return "no_data_change_inferred"
    if direction == "improved":
        return "source_level_review_required_before_claiming_completion"
    if direction == "regressed":
        return "regression_review_required_before_release"
    return "manual_delta_review_required"


def delta_context(metric: str, row: dict[str, Any], exchange: str = "") -> str:
    scope = exchange or "global"
    delta = row.get("delta")
    return (
        f"scope={scope};"
        f"metric={metric};"
        f"baseline={row.get('baseline', 0)};"
        f"current={row.get('current', 0)};"
        f"delta={delta};"
        f"direction={delta_direction(metric, delta)};"
        f"review_policy={delta_review_policy(metric, delta)}"
    )


def compare_counter_to_reported(counter: Counter[str], reported: Any) -> dict[str, dict[str, int]]:
    if not isinstance(reported, dict):
        return {"__summary__": {"reported": -1, "actual": sum(counter.values())}}
    mismatches: dict[str, dict[str, int]] = {}
    keys = sorted(set(counter) | {str(key) for key in reported})
    for key in keys:
        actual = int(counter.get(key, 0))
        try:
            expected = int(reported.get(key, 0))
        except (TypeError, ValueError):
            expected = -1
        if actual != expected:
            mismatches[key] = {"reported": expected, "actual": actual}
    return mismatches


def evaluate_coverage_freshness_visibility(coverage_report: dict[str, Any]) -> dict[str, Any]:
    meta = coverage_report.get("_meta", {})
    freshness = coverage_report.get("freshness", {})
    source_rows = coverage_report.get("source_coverage", [])
    source_summary = coverage_report.get("source_freshness_summary", {})
    b3_diagnostics = coverage_report.get("b3_masterfile_diagnostics", {})
    if not isinstance(meta, dict):
        meta = {}
    if not isinstance(freshness, dict):
        freshness = {}
    if not isinstance(source_rows, list):
        source_rows = []
    if not isinstance(source_summary, dict):
        source_summary = {}
    if not isinstance(b3_diagnostics, dict):
        b3_diagnostics = {}
    source_files = meta.get("source_files", {})
    if not isinstance(source_files, dict):
        source_files = {}
    missing_meta_keys = [
        key
        for key in ("generated_at", "source_files", "policy")
        if key not in meta or meta.get(key) in ("", None, {}, [])
    ]
    invalid_meta_generated_at = (
        str(meta.get("generated_at", ""))
        if meta.get("generated_at") and not is_valid_iso_utc_timestamp(str(meta.get("generated_at")))
        else ""
    )
    source_file_mismatches = {
        key: {"expected": expected, "actual": source_files.get(key)}
        for key, expected in EXPECTED_COVERAGE_SOURCE_FILES.items()
        if source_files.get(key) != expected
    }
    unexpected_source_files = sorted(key for key in source_files if key not in EXPECTED_COVERAGE_SOURCE_FILES)
    policy_missing_markers = [
        marker
        for marker in ("Coverage and freshness report only", "does not authorize", "identifiers", "symbol changes")
        if marker not in str(meta.get("policy", ""))
    ]

    missing_freshness_keys = [key for key in REQUIRED_COVERAGE_FRESHNESS_KEYS if key not in freshness]
    invalid_timestamps = [
        key
        for key in REQUIRED_COVERAGE_FRESHNESS_KEYS
        if key.endswith("_at") and freshness.get(key) and not is_valid_iso_utc_timestamp(str(freshness.get(key)))
    ]
    invalid_age_fields = [
        key
        for key in REQUIRED_COVERAGE_FRESHNESS_KEYS
        if key.endswith("_age_hours") and not is_nonnegative_number(freshness.get(key))
    ]
    invalid_count_fields = [
        key
        for key in (
            "symbol_changes_review_rows",
            "entry_quality_rows",
            "masterfile_collision_review_rows",
            "ohlcv_plausibility_rows",
            "source_gap_classification_rows",
        )
        if not isinstance(freshness.get(key), int) or freshness.get(key) < 0
    ]
    missing_source_summary_keys = [key for key in REQUIRED_SOURCE_FRESHNESS_SUMMARY_KEYS if key not in source_summary]
    source_count_mismatch = {
        "reported": source_summary.get("source_count", 0),
        "actual": len(source_rows),
    } if source_summary.get("source_count", 0) != len(source_rows) else {}

    source_row_gaps: list[dict[str, Any]] = []
    freshness_status_counter: Counter[str] = Counter()
    source_age_bucket_counter: Counter[str] = Counter()
    refresh_priority_counter: Counter[str] = Counter()
    refresh_queue_counter: Counter[str] = Counter()
    refresh_queue_scope_counter: dict[str, Counter[str]] = defaultdict(Counter)
    refresh_queue_mode_counter: dict[str, Counter[str]] = defaultdict(Counter)
    refresh_queue_priority_counter: dict[str, Counter[str]] = defaultdict(Counter)
    refresh_queue_age_bucket_counter: dict[str, Counter[str]] = defaultdict(Counter)
    refresh_queue_strategy_counter: dict[str, Counter[str]] = defaultdict(Counter)
    refresh_queue_evidence_counter: dict[str, Counter[str]] = defaultdict(Counter)
    refresh_action_counter: Counter[str] = Counter()
    for index, row in enumerate(source_rows):
        if not isinstance(row, dict):
            source_row_gaps.append({"row_index": index, "reason": "source_row_is_not_object"})
            continue
        missing_keys = [key for key in REQUIRED_SOURCE_FRESHNESS_ROW_KEYS if key not in row]
        invalid_fields: list[str] = []
        if row.get("generated_at") and not is_valid_iso_utc_timestamp(str(row.get("generated_at"))):
            invalid_fields.append("generated_at")
        if not is_nonnegative_number(row.get("age_hours")):
            invalid_fields.append("age_hours")
        if row.get("freshness_status") not in {"fresh", "old", "stale", "unknown"}:
            invalid_fields.append("freshness_status")
        if row.get("age_bucket") not in {"age_0_48h", "age_48_168h", "age_168_336h", "age_over_336h", "unknown_age"}:
            invalid_fields.append("age_bucket")
        if row.get("refresh_priority") not in {"P1", "P2", "P3", "P4"}:
            invalid_fields.append("refresh_priority")
        if not row.get("refresh_queue"):
            invalid_fields.append("refresh_queue")
        if not row.get("recommended_refresh_action"):
            invalid_fields.append("recommended_refresh_action")
        if not row.get("recommended_next_source"):
            invalid_fields.append("recommended_next_source")
        if not row.get("source_gate"):
            invalid_fields.append("source_gate")
        if row.get("refresh_queue") and row.get("refresh_queue") not in {
            "capture_source_generated_at_before_refresh_decision",
            "fresh_no_refresh_needed",
            "refresh_official_exchange_directory_before_identity_or_collision_work",
            "refresh_official_subset_before_gap_enrichment",
            "restore_or_replace_unavailable_source_before_data_fill",
            "review_secondary_source_freshness_or_replace",
        }:
            invalid_fields.append("refresh_queue")
        elif row.get("refresh_queue") == "fresh_no_refresh_needed" and row.get("recommended_refresh_action") != "no_refresh_needed":
            invalid_fields.append("refresh_queue")
        refresh_queue = str(row.get("refresh_queue") or "unknown")
        expected_strategy, expected_evidence_required = source_refresh_strategy_for(refresh_queue)
        if row.get("review_strategy") != expected_strategy:
            invalid_fields.append("review_strategy")
        if row.get("evidence_required") != expected_evidence_required:
            invalid_fields.append("evidence_required")
        if row.get("source_artifact_context") != source_artifact_context(row):
            invalid_fields.append("source_artifact_context")
        if row.get("freshness_review_context") != source_freshness_review_context(row):
            invalid_fields.append("freshness_review_context")
        if row.get("refresh_gate_context") != source_refresh_gate_context(row):
            invalid_fields.append("refresh_gate_context")
        if missing_keys or invalid_fields:
            source_row_gaps.append(
                {
                    "row_index": index,
                    "key": row.get("key", ""),
                    "missing_keys": missing_keys,
                    "invalid_fields": invalid_fields,
                }
            )
        freshness_status_counter[str(row.get("freshness_status") or "unknown")] += 1
        source_age_bucket_counter[str(row.get("age_bucket") or "unknown_age")] += 1
        refresh_priority_counter[str(row.get("refresh_priority") or "unknown")] += 1
        refresh_queue_counter[refresh_queue] += 1
        refresh_queue_scope_counter[refresh_queue][str(row.get("reference_scope") or "missing")] += 1
        refresh_queue_mode_counter[refresh_queue][str(row.get("mode") or "missing")] += 1
        refresh_queue_priority_counter[refresh_queue][str(row.get("refresh_priority") or "missing")] += 1
        refresh_queue_age_bucket_counter[refresh_queue][str(row.get("age_bucket") or "unknown_age")] += 1
        refresh_queue_strategy_counter[refresh_queue][expected_strategy] += 1
        refresh_queue_evidence_counter[refresh_queue][expected_evidence_required] += 1
        refresh_action_counter[str(row.get("recommended_refresh_action") or "unknown")] += 1

    summary_mismatches = {
        "freshness_status_totals": compare_counter_to_reported(
            freshness_status_counter,
            source_summary.get("freshness_status_totals"),
        ),
        "refresh_priority_totals": compare_counter_to_reported(
            refresh_priority_counter,
            source_summary.get("refresh_priority_totals"),
        ),
        "source_age_bucket_totals": compare_counter_to_reported(
            source_age_bucket_counter,
            source_summary.get("source_age_bucket_totals"),
        ),
        "refresh_queue_totals": compare_counter_to_reported(
            refresh_queue_counter,
            source_summary.get("refresh_queue_totals"),
        ),
        "recommended_refresh_action_totals": compare_counter_to_reported(
            refresh_action_counter,
            source_summary.get("recommended_refresh_action_totals"),
        ),
    }
    refresh_queue_scope_mismatches = {
        queue: compare_counter_to_reported(
            counter,
            source_summary.get("refresh_queue_scope_totals", {}).get(queue)
            if isinstance(source_summary.get("refresh_queue_scope_totals"), dict)
            else None,
        )
        for queue, counter in refresh_queue_scope_counter.items()
    }
    refresh_queue_scope_mismatches = {
        queue: mismatch for queue, mismatch in refresh_queue_scope_mismatches.items() if mismatch
    }
    refresh_queue_mode_mismatches = {
        queue: compare_counter_to_reported(
            counter,
            source_summary.get("refresh_queue_mode_totals", {}).get(queue)
            if isinstance(source_summary.get("refresh_queue_mode_totals"), dict)
            else None,
        )
        for queue, counter in refresh_queue_mode_counter.items()
    }
    refresh_queue_mode_mismatches = {
        queue: mismatch for queue, mismatch in refresh_queue_mode_mismatches.items() if mismatch
    }
    refresh_queue_priority_mismatches = {
        queue: compare_counter_to_reported(
            counter,
            source_summary.get("refresh_queue_priority_totals", {}).get(queue)
            if isinstance(source_summary.get("refresh_queue_priority_totals"), dict)
            else None,
        )
        for queue, counter in refresh_queue_priority_counter.items()
    }
    refresh_queue_priority_mismatches = {
        queue: mismatch for queue, mismatch in refresh_queue_priority_mismatches.items() if mismatch
    }
    refresh_queue_age_bucket_mismatches = {
        queue: compare_counter_to_reported(
            counter,
            source_summary.get("refresh_queue_age_bucket_totals", {}).get(queue)
            if isinstance(source_summary.get("refresh_queue_age_bucket_totals"), dict)
            else None,
        )
        for queue, counter in refresh_queue_age_bucket_counter.items()
    }
    refresh_queue_age_bucket_mismatches = {
        queue: mismatch for queue, mismatch in refresh_queue_age_bucket_mismatches.items() if mismatch
    }
    refresh_queue_strategy_mismatches = {
        queue: compare_counter_to_reported(
            counter,
            source_summary.get("refresh_queue_review_strategy_totals", {}).get(queue)
            if isinstance(source_summary.get("refresh_queue_review_strategy_totals"), dict)
            else None,
        )
        for queue, counter in refresh_queue_strategy_counter.items()
    }
    refresh_queue_strategy_mismatches = {
        queue: mismatch for queue, mismatch in refresh_queue_strategy_mismatches.items() if mismatch
    }
    refresh_queue_evidence_mismatches = {
        queue: compare_counter_to_reported(
            counter,
            source_summary.get("refresh_queue_evidence_required_totals", {}).get(queue)
            if isinstance(source_summary.get("refresh_queue_evidence_required_totals"), dict)
            else None,
        )
        for queue, counter in refresh_queue_evidence_counter.items()
    }
    refresh_queue_evidence_mismatches = {
        queue: mismatch for queue, mismatch in refresh_queue_evidence_mismatches.items() if mismatch
    }
    top_refresh_batch_gaps: list[dict[str, Any]] = []
    top_refresh_batches = source_summary.get("top_source_refresh_batches")
    if not isinstance(top_refresh_batches, list) or not top_refresh_batches:
        top_refresh_batch_gaps.append({"field": "top_source_refresh_batches", "reason": "expected_ranked_refresh_batches"})
    elif isinstance(top_refresh_batches, list):
        for index, batch in enumerate(top_refresh_batches):
            if not isinstance(batch, dict):
                top_refresh_batch_gaps.append({"row_index": index, "reason": "expected_object"})
                continue
            queue = str(batch.get("refresh_queue", ""))
            strategy = str(batch.get("review_strategy", ""))
            expected_strategy, _expected_evidence = source_refresh_strategy_for(queue)
            missing_keys = [
                key
                for key in (
                    "refresh_queue",
                    "reference_scope",
                    "mode",
                    "refresh_priority",
                    "source_count",
                    "total_rows",
                    "max_age_hours",
                    "review_strategy",
                    "evidence_required",
                    "recommended_next_source",
                    "source_gate",
                )
                if key not in batch
            ]
            invalid_fields = []
            if queue not in refresh_queue_counter:
                invalid_fields.append("refresh_queue")
            if batch.get("refresh_priority") not in {"P1", "P2", "P3", "P4"}:
                invalid_fields.append("refresh_priority")
            if not isinstance(batch.get("source_count"), int) or batch.get("source_count", 0) <= 0:
                invalid_fields.append("source_count")
            if not isinstance(batch.get("total_rows"), int) or batch.get("total_rows", -1) < 0:
                invalid_fields.append("total_rows")
            if batch.get("max_age_hours") is not None and not is_nonnegative_number(batch.get("max_age_hours")):
                invalid_fields.append("max_age_hours")
            if strategy not in SOURCE_REFRESH_REVIEW_STRATEGIES or strategy != expected_strategy:
                invalid_fields.append("review_strategy")
            if not batch.get("evidence_required") or batch.get("evidence_required") in {"none", "ticker_match_only"}:
                invalid_fields.append("evidence_required")
            if not batch.get("recommended_next_source"):
                invalid_fields.append("recommended_next_source")
            if not batch.get("source_gate"):
                invalid_fields.append("source_gate")
            if missing_keys or invalid_fields:
                top_refresh_batch_gaps.append(
                    {"row_index": index, "missing_keys": missing_keys, "invalid_fields": invalid_fields}
                )
    summary_mismatches = {key: value for key, value in summary_mismatches.items() if value}
    missing_b3_diagnostic_keys = [
        key for key in REQUIRED_B3_MASTERFILE_DIAGNOSTIC_KEYS if key not in b3_diagnostics
    ]
    b3_diagnostic_gaps: list[dict[str, Any]] = []
    b3_numeric_keys = (
        "dataset_rows",
        "active_exchange_directory_rows",
        "all_b3_masterfile_rows",
        "matched_dataset_rows",
        "missing_dataset_rows",
        "official_any_source_matched_dataset_rows",
        "official_any_source_missing_dataset_rows",
        "official_active_symbols_not_in_dataset",
    )
    for key in b3_numeric_keys:
        if not isinstance(b3_diagnostics.get(key), int) or b3_diagnostics.get(key, -1) < 0:
            b3_diagnostic_gaps.append({"field": key, "reason": "expected_nonnegative_integer"})
    dataset_rows = b3_diagnostics.get("dataset_rows")
    matched_dataset_rows = b3_diagnostics.get("matched_dataset_rows")
    missing_dataset_rows = b3_diagnostics.get("missing_dataset_rows")
    official_any_source_matched_rows = b3_diagnostics.get("official_any_source_matched_dataset_rows")
    official_any_source_missing_rows = b3_diagnostics.get("official_any_source_missing_dataset_rows")
    if all(isinstance(value, int) for value in (dataset_rows, matched_dataset_rows, missing_dataset_rows)):
        if matched_dataset_rows + missing_dataset_rows != dataset_rows:
            b3_diagnostic_gaps.append(
                {
                    "field": "dataset_row_balance",
                    "matched_dataset_rows": matched_dataset_rows,
                    "missing_dataset_rows": missing_dataset_rows,
                    "dataset_rows": dataset_rows,
                }
            )
    if all(
        isinstance(value, int)
        for value in (dataset_rows, official_any_source_matched_rows, official_any_source_missing_rows)
    ):
        if official_any_source_matched_rows + official_any_source_missing_rows != dataset_rows:
            b3_diagnostic_gaps.append(
                {
                    "field": "official_any_source_dataset_row_balance",
                    "official_any_source_matched_dataset_rows": official_any_source_matched_rows,
                    "official_any_source_missing_dataset_rows": official_any_source_missing_rows,
                    "dataset_rows": dataset_rows,
                }
            )
        expected_any_source_rate = (
            round(official_any_source_matched_rows / dataset_rows * 100, 2) if dataset_rows else None
        )
        if b3_diagnostics.get("official_any_source_match_rate") != expected_any_source_rate:
            b3_diagnostic_gaps.append(
                {
                    "field": "official_any_source_match_rate",
                    "reported": b3_diagnostics.get("official_any_source_match_rate"),
                    "expected": expected_any_source_rate,
                }
            )
        expected_rate = round(matched_dataset_rows / dataset_rows * 100, 2) if dataset_rows else None
        if b3_diagnostics.get("dataset_match_rate") != expected_rate:
            b3_diagnostic_gaps.append(
                {
                    "field": "dataset_match_rate",
                    "reported": b3_diagnostics.get("dataset_match_rate"),
                    "expected": expected_rate,
                }
            )
    for counter_key in (
        "active_source_key_totals",
        "all_source_key_totals",
        "missing_category_totals",
        "missing_asset_type_totals",
        "missing_source_presence_totals",
    ):
        counter = b3_diagnostics.get(counter_key)
        if not isinstance(counter, dict) or any(not isinstance(value, int) or value < 0 for value in counter.values()):
            b3_diagnostic_gaps.append({"field": counter_key, "reason": "expected_nonnegative_integer_counter"})
    if isinstance(missing_dataset_rows, int):
        for counter_key in ("missing_category_totals", "missing_asset_type_totals", "missing_source_presence_totals"):
            counter = b3_diagnostics.get(counter_key)
            if isinstance(counter, dict) and sum(counter.values()) != missing_dataset_rows:
                b3_diagnostic_gaps.append(
                    {
                        "field": counter_key,
                        "reported_total": sum(counter.values()),
                        "missing_dataset_rows": missing_dataset_rows,
                    }
                )
    policy = str(b3_diagnostics.get("policy", "")).lower()
    if not all(marker in policy for marker in ("diagnostic", "do not authorize", "inferred")):
        b3_diagnostic_gaps.append({"field": "policy", "reason": "missing_no_inference_diagnostic_policy"})
    missing_examples = b3_diagnostics.get("missing_examples", {})
    if not isinstance(missing_examples, dict):
        b3_diagnostic_gaps.append({"field": "missing_examples", "reason": "expected_category_mapping"})
    elif isinstance(missing_dataset_rows, int) and missing_dataset_rows > 0 and not missing_examples:
        b3_diagnostic_gaps.append({"field": "missing_examples", "reason": "examples_required_for_missing_rows"})
    elif isinstance(missing_examples, dict):
        for category, examples in missing_examples.items():
            if not isinstance(examples, list):
                b3_diagnostic_gaps.append({"field": f"missing_examples.{category}", "reason": "expected_list"})
                continue
            for index, example in enumerate(examples):
                if not isinstance(example, dict):
                    b3_diagnostic_gaps.append(
                        {"field": f"missing_examples.{category}[{index}]", "reason": "expected_object"}
                    )
                    continue
                missing_example_keys = [
                    key for key in REQUIRED_B3_MASTERFILE_EXAMPLE_KEYS if key not in example
                ]
                invalid_example_fields = []
                if not str(example.get("listing_key", "")).startswith("B3::"):
                    invalid_example_fields.append("listing_key")
                if example.get("source_presence") not in {
                    "present_only_in_non_exchange_directory_source",
                    "absent_from_all_b3_masterfile_sources",
                }:
                    invalid_example_fields.append("source_presence")
                if missing_example_keys or invalid_example_fields:
                    b3_diagnostic_gaps.append(
                        {
                            "field": f"missing_examples.{category}[{index}]",
                            "missing_keys": missing_example_keys,
                            "invalid_fields": invalid_example_fields,
                        }
                    )
    return {
        "passed": (
            bool(freshness)
            and bool(source_rows)
            and not missing_meta_keys
            and not invalid_meta_generated_at
            and not source_file_mismatches
            and not unexpected_source_files
            and not policy_missing_markers
            and not missing_freshness_keys
            and not invalid_timestamps
            and not invalid_age_fields
            and not invalid_count_fields
            and not missing_source_summary_keys
            and not source_count_mismatch
            and not source_row_gaps
            and not summary_mismatches
            and not refresh_queue_scope_mismatches
            and not refresh_queue_mode_mismatches
            and not refresh_queue_priority_mismatches
            and not refresh_queue_age_bucket_mismatches
            and not refresh_queue_strategy_mismatches
            and not refresh_queue_evidence_mismatches
            and not top_refresh_batch_gaps
            and not missing_b3_diagnostic_keys
            and not b3_diagnostic_gaps
        ),
        "missing_meta_keys": missing_meta_keys,
        "invalid_meta_generated_at": invalid_meta_generated_at,
        "source_file_mismatches": source_file_mismatches,
        "unexpected_source_files": unexpected_source_files,
        "policy_missing_markers": policy_missing_markers,
        "freshness_keys": sorted(freshness),
        "source_rows": len(source_rows),
        "missing_freshness_keys": missing_freshness_keys,
        "invalid_timestamps": invalid_timestamps,
        "invalid_age_fields": invalid_age_fields,
        "invalid_count_fields": invalid_count_fields,
        "missing_source_summary_keys": missing_source_summary_keys,
        "source_count_mismatch": source_count_mismatch,
        "source_row_gaps": source_row_gaps[:20],
        "summary_mismatches": summary_mismatches,
        "refresh_queue_scope_mismatches": refresh_queue_scope_mismatches,
        "refresh_queue_mode_mismatches": refresh_queue_mode_mismatches,
        "refresh_queue_priority_mismatches": refresh_queue_priority_mismatches,
        "refresh_queue_age_bucket_mismatches": refresh_queue_age_bucket_mismatches,
        "refresh_queue_strategy_mismatches": refresh_queue_strategy_mismatches,
        "refresh_queue_evidence_mismatches": refresh_queue_evidence_mismatches,
        "top_refresh_batch_gaps": top_refresh_batch_gaps,
        "missing_b3_diagnostic_keys": missing_b3_diagnostic_keys,
        "b3_diagnostic_gaps": b3_diagnostic_gaps[:20],
        "b3_masterfile_diagnostics": {
            key: b3_diagnostics.get(key)
            for key in (
                "dataset_rows",
                "active_exchange_directory_rows",
                "matched_dataset_rows",
                "missing_dataset_rows",
                "dataset_match_rate",
                "official_any_source_matched_dataset_rows",
                "official_any_source_missing_dataset_rows",
                "official_any_source_match_rate",
                "missing_category_totals",
                "missing_source_presence_totals",
            )
            if key in b3_diagnostics
        },
        "source_freshness_status_totals": dict(freshness_status_counter),
        "source_age_bucket_totals": dict(source_age_bucket_counter),
        "source_refresh_priority_totals": dict(refresh_priority_counter),
        "source_refresh_queue_totals": dict(refresh_queue_counter),
        "source_refresh_queue_scope_totals": {
            queue: dict(counter) for queue, counter in refresh_queue_scope_counter.items()
        },
        "source_refresh_queue_mode_totals": {
            queue: dict(counter) for queue, counter in refresh_queue_mode_counter.items()
        },
        "source_refresh_queue_priority_totals": {
            queue: dict(counter) for queue, counter in refresh_queue_priority_counter.items()
        },
        "source_refresh_queue_age_bucket_totals": {
            queue: dict(counter) for queue, counter in refresh_queue_age_bucket_counter.items()
        },
        "source_refresh_queue_review_strategy_totals": {
            queue: dict(counter) for queue, counter in refresh_queue_strategy_counter.items()
        },
        "source_refresh_queue_evidence_required_totals": {
            queue: dict(counter) for queue, counter in refresh_queue_evidence_counter.items()
        },
    }


def evaluate_source_gap_traceability(source_gap_report: dict[str, Any]) -> dict[str, Any]:
    summary = source_gap_report.get("summary", {})
    rows = source_gap_report.get("rows", [])
    if not isinstance(summary, dict):
        summary = {}
    if not isinstance(rows, list):
        rows = []
    missing_summary_keys = [key for key in REQUIRED_SOURCE_GAP_SUMMARY_KEYS if key not in summary]
    row_count_mismatch = {
        "reported": summary.get("rows", 0),
        "actual": len(rows),
    } if summary.get("rows", 0) != len(rows) else {}
    field_counter: Counter[str] = Counter()
    class_counter: Counter[str] = Counter()
    class_by_field_counter: Counter[str] = Counter()
    exchange_by_field_counter: Counter[tuple[str, str]] = Counter()
    row_gaps: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            row_gaps.append({"row_index": index, "reason": "source_gap_row_is_not_object"})
            continue
        missing_keys = [key for key in REQUIRED_SOURCE_GAP_ROW_KEYS if not row.get(key)]
        invalid_fields: list[str] = []
        field = str(row.get("field", ""))
        gap_class = str(row.get("gap_class", ""))
        if field not in SOURCE_GAP_FIELD_VALUES:
            invalid_fields.append("field")
        if row.get("review_needed") not in {"true", True}:
            invalid_fields.append("review_needed")
        for key in ("confidence_policy", "recommended_next_source", "source_gate"):
            if not str(row.get(key, "")).strip():
                invalid_fields.append(key)
        if row.get("source_gap_context") != source_gap_context(row):
            invalid_fields.append("source_gap_context")
        if row.get("classification_context") != source_gap_classification_context(row):
            invalid_fields.append("classification_context")
        if row.get("evidence_gate_context") != source_gap_evidence_gate_context(row):
            invalid_fields.append("evidence_gate_context")
        if missing_keys or invalid_fields:
            row_gaps.append(
                {
                    "row_index": index,
                    "listing_key": row.get("listing_key", ""),
                    "missing_keys": missing_keys,
                    "invalid_fields": invalid_fields,
                    "available_keys": sorted(row)[:20],
                }
            )
        field_counter[field] += 1
        class_counter[gap_class] += 1
        class_by_field_counter[f"{field}:{gap_class}"] += 1
        exchange_by_field_counter[(field, str(row.get("exchange", "") or "missing"))] += 1
    summary_mismatches = {
        "field_totals": compare_counter_to_reported(field_counter, summary.get("field_totals")),
        "class_totals": compare_counter_to_reported(class_counter, summary.get("class_totals")),
        "class_by_field": compare_counter_to_reported(class_by_field_counter, summary.get("class_by_field")),
    }
    summary_mismatches = {key: value for key, value in summary_mismatches.items() if value}
    policy = summary.get("policy", {})
    missing_policy_keys = [
        key
        for key in ("no_unreviewed_heuristics", "release_gate")
        if not isinstance(policy, dict) or not policy.get(key)
    ]
    expected_top_exchanges_by_field = {
        field: [
            {"exchange": exchange, "rows": count}
            for (row_field, exchange), count in exchange_by_field_counter.most_common()
            if row_field == field
        ][:10]
        for field in sorted(field_counter)
    }
    top_exchange_gaps: list[dict[str, Any]] = []
    reported_top_exchanges = summary.get("top_exchanges_by_field", {})
    if not isinstance(reported_top_exchanges, dict):
        top_exchange_gaps.append({"field": "top_exchanges_by_field", "reason": "expected_field_mapping"})
    else:
        for field, expected_rows in expected_top_exchanges_by_field.items():
            reported_rows = reported_top_exchanges.get(field)
            if reported_rows != expected_rows:
                top_exchange_gaps.append(
                    {
                        "field": f"top_exchanges_by_field.{field}",
                        "reported": reported_rows,
                        "expected": expected_rows,
                    }
                )
        unknown_fields = sorted(set(reported_top_exchanges) - set(expected_top_exchanges_by_field))
        if unknown_fields:
            top_exchange_gaps.append({"field": "top_exchanges_by_field", "unknown_fields": unknown_fields})
    top_review_batch_gaps: list[dict[str, Any]] = []
    top_review_batches = summary.get("top_source_gap_review_batches")
    if not isinstance(top_review_batches, list) or not top_review_batches:
        top_review_batch_gaps.append(
            {"field": "top_source_gap_review_batches", "reason": "expected_ranked_review_batches"}
        )
    elif isinstance(top_review_batches, list):
        for index, batch in enumerate(top_review_batches):
            if not isinstance(batch, dict):
                top_review_batch_gaps.append({"row_index": index, "reason": "expected_object"})
                continue
            missing_batch_keys = [
                key
                for key in REQUIRED_SOURCE_GAP_REVIEW_BATCH_KEYS
                if batch.get(key) in ("", None, [], {})
            ]
            invalid_batch_fields = []
            if batch.get("field") not in SOURCE_GAP_FIELD_VALUES:
                invalid_batch_fields.append("field")
            if not isinstance(batch.get("rows"), int) or isinstance(batch.get("rows"), bool) or batch.get("rows") <= 0:
                invalid_batch_fields.append("rows")
            if missing_batch_keys or invalid_batch_fields:
                top_review_batch_gaps.append(
                    {
                        "row_index": index,
                        "missing_keys": missing_batch_keys,
                        "invalid_fields": invalid_batch_fields,
                    }
                )
    return {
        "passed": (
            bool(rows)
            and not missing_summary_keys
            and not row_count_mismatch
            and not row_gaps
            and not summary_mismatches
            and not missing_policy_keys
            and not top_exchange_gaps
            and not top_review_batch_gaps
        ),
        "rows": len(rows),
        "missing_summary_keys": missing_summary_keys,
        "row_count_mismatch": row_count_mismatch,
        "row_gaps": row_gaps[:20],
        "summary_mismatches": summary_mismatches,
        "missing_policy_keys": missing_policy_keys,
        "top_exchange_gaps": top_exchange_gaps[:20],
        "top_review_batch_gaps": top_review_batch_gaps[:20],
        "required_review_batch_keys": list(REQUIRED_SOURCE_GAP_REVIEW_BATCH_KEYS),
        "required_row_keys": list(REQUIRED_SOURCE_GAP_ROW_KEYS),
        "accepted_fields": sorted(SOURCE_GAP_FIELD_VALUES),
    }


def evaluate_symbol_change_review_gate(symbol_changes_review: dict[str, Any]) -> dict[str, Any]:
    meta = symbol_changes_review.get("_meta", {})
    summary = symbol_changes_review.get("summary", {})
    rows = symbol_changes_review.get("review_items", [])
    if not isinstance(meta, dict):
        meta = {}
    if not isinstance(summary, dict):
        summary = {}
    if not isinstance(rows, list):
        rows = []
    source_files = meta.get("source_files", {})
    if not isinstance(source_files, dict):
        source_files = {}
    missing_meta_keys = [
        key
        for key in ("generated_at", "source_url", "source_files", "source_policy", "policy")
        if key not in meta or meta.get(key) in ("", None, {}, [])
    ]
    invalid_generated_at = (
        str(meta.get("generated_at", ""))
        if meta.get("generated_at") and not is_valid_iso_utc_timestamp(str(meta.get("generated_at")))
        else ""
    )
    source_url_mismatch = {
        "expected": "https://stockanalysis.com/actions/changes/",
        "actual": meta.get("source_url"),
    } if meta.get("source_url") != "https://stockanalysis.com/actions/changes/" else {}
    source_policy_mismatch = {
        "expected": "secondary_review_only",
        "actual": meta.get("source_policy"),
    } if meta.get("source_policy") != "secondary_review_only" else {}
    source_file_mismatches = {
        key: {"expected": expected, "actual": source_files.get(key)}
        for key, expected in EXPECTED_SYMBOL_CHANGE_SOURCE_FILES.items()
        if source_files.get(key) != expected
    }
    unexpected_source_files = sorted(key for key in source_files if key not in EXPECTED_SYMBOL_CHANGE_SOURCE_FILES)
    policy_missing_markers = [
        marker
        for marker in ("review only", "No ticker", "listing-keyed official exchange or issuer evidence")
        if marker not in str(meta.get("policy", ""))
    ]
    missing_summary_keys = [key for key in REQUIRED_SYMBOL_CHANGE_SUMMARY_KEYS if key not in summary]
    row_count_mismatch = {
        "reported": summary.get("review_rows", 0),
        "actual": len(rows),
    } if summary.get("review_rows", 0) != len(rows) else {}
    counters: dict[str, Counter[str]] = {
        "match_status_counts": Counter(),
        "symbol_change_workflow_queue_counts": Counter(),
        "review_bucket_counts": Counter(),
        "review_priority_counts": Counter(),
        "recency_bucket_counts": Counter(),
        "review_priority_recency_counts": Counter(),
        "workflow_queue_recency_counts": Counter(),
        "workflow_queue_priority_counts": Counter(),
        "workflow_queue_scope_counts": Counter(),
        "workflow_queue_match_status_counts": Counter(),
        "workflow_queue_source_hint_counts": Counter(),
        "workflow_queue_source_confidence_counts": Counter(),
        "workflow_queue_issuer_name_review_counts": Counter(),
        "workflow_queue_listing_key_review_counts": Counter(),
        "workflow_queue_review_strategy_counts": Counter(),
        "apply_eligibility_counts": Counter(),
        "symbol_change_apply_readiness_counts": Counter(),
        "verification_evidence_required_counts": Counter(),
        "time_sensitive_workflow_queue_counts": Counter(),
        "time_sensitive_recency_counts": Counter(),
        "time_sensitive_apply_readiness_counts": Counter(),
        "recommended_action_counts": Counter(),
        "exchange_scope_status_counts": Counter(),
    }
    row_gaps: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            row_gaps.append({"row_index": index, "reason": "symbol_change_row_is_not_object"})
            continue
        missing_keys = [key for key in REQUIRED_SYMBOL_CHANGE_ROW_KEYS if not row.get(key)]
        invalid_fields: list[str] = []
        match_status = str(row.get("match_status", ""))
        workflow_queue = str(row.get("symbol_change_workflow_queue", ""))
        review_bucket = str(row.get("review_bucket", ""))
        review_priority = str(row.get("review_priority", ""))
        review_strategy = str(row.get("review_strategy", ""))
        recency_bucket = str(row.get("recency_bucket", ""))
        apply_eligibility = str(row.get("apply_eligibility", ""))
        exchange_scope_status = str(row.get("exchange_scope_status", ""))
        listing_key_review_status = str(row.get("listing_key_review_status", ""))
        issuer_name_review_status = str(row.get("issuer_name_review_status", ""))
        if match_status not in SYMBOL_CHANGE_MATCH_STATUSES:
            invalid_fields.append("match_status")
        if review_bucket not in SYMBOL_CHANGE_REVIEW_BUCKETS:
            invalid_fields.append("review_bucket")
        if workflow_queue not in SYMBOL_CHANGE_WORKFLOW_QUEUES:
            invalid_fields.append("symbol_change_workflow_queue")
        if SYMBOL_CHANGE_BUCKET_TO_WORKFLOW_QUEUE.get(review_bucket) != workflow_queue:
            invalid_fields.append("symbol_change_workflow_queue")
        if review_priority not in SYMBOL_CHANGE_PRIORITIES:
            invalid_fields.append("review_priority")
        expected_strategy, expected_evidence = symbol_change_workflow_strategy_for(workflow_queue)
        if review_strategy != expected_strategy:
            invalid_fields.append("review_strategy")
        if recency_bucket not in SYMBOL_CHANGE_RECENCY_BUCKETS:
            invalid_fields.append("recency_bucket")
        if apply_eligibility not in SYMBOL_CHANGE_APPLY_ELIGIBILITIES:
            invalid_fields.append("apply_eligibility")
        if exchange_scope_status not in SYMBOL_CHANGE_EXCHANGE_SCOPE_STATUSES:
            invalid_fields.append("exchange_scope_status")
        if listing_key_review_status not in SYMBOL_CHANGE_LISTING_KEY_REVIEW_STATUSES:
            invalid_fields.append("listing_key_review_status")
        if row.get("source_confidence") != "secondary_review":
            invalid_fields.append("source_confidence")
        if row.get("review_needed") not in {"true", True}:
            invalid_fields.append("review_needed")
        if issuer_name_review_status not in {
            "feed_name_differs_from_scoped_listing_name",
            "feed_name_exactly_matches_scoped_listing_name",
            "feed_name_partially_overlaps_scoped_listing_name",
            "no_feed_company_name",
            "no_scoped_listing_name_available",
        }:
            invalid_fields.append("issuer_name_review_status")
        if review_priority == "P1" and apply_eligibility != "requires_official_venue_confirmation":
            invalid_fields.append("p1_apply_eligibility")
        try:
            old_scoped_count = int(row.get("old_scoped_match_count") or 0)
            new_scoped_count = int(row.get("new_scoped_match_count") or 0)
        except (TypeError, ValueError):
            old_scoped_count = 0
            new_scoped_count = 0
            invalid_fields.append("scoped_match_count")
        expected_listing_key_review_status = (
            "old_and_new_scoped_listing_keys_present"
            if old_scoped_count and new_scoped_count
            else "old_scoped_listing_key_only"
            if old_scoped_count
            else "new_scoped_listing_key_only"
            if new_scoped_count
            else "no_scoped_listing_key_match"
        )
        if listing_key_review_status != expected_listing_key_review_status:
            invalid_fields.append("listing_key_review_status")
        if workflow_queue == "review_verified_rename_or_delisting" and listing_key_review_status != "old_scoped_listing_key_only":
            invalid_fields.append("listing_key_review_status")
        if workflow_queue == "review_duplicate_or_cross_listing" and listing_key_review_status != "old_and_new_scoped_listing_keys_present":
            invalid_fields.append("listing_key_review_status")
        if workflow_queue == "audit_already_reflected" and listing_key_review_status == "old_scoped_listing_key_only":
            invalid_fields.append("listing_key_review_status")
        if workflow_queue == "document_no_dataset_match" and listing_key_review_status != "no_scoped_listing_key_match":
            invalid_fields.append("listing_key_review_status")
        if not str(row.get("verification_evidence_required", "")).strip():
            invalid_fields.append("verification_evidence_required")
        if str(row.get("verification_evidence_required", "")) != expected_evidence:
            invalid_fields.append("verification_evidence_required")
        expected_next_source = symbol_change_workflow_recommended_next_source_for(workflow_queue, exchange_scope_status)
        if row.get("recommended_next_source") != expected_next_source:
            invalid_fields.append("recommended_next_source")
        expected_source_gate = symbol_change_workflow_source_gate_for(workflow_queue)
        if row.get("source_gate") != expected_source_gate:
            invalid_fields.append("source_gate")
        if row.get("source_review_context") != symbol_change_source_review_context(row):
            invalid_fields.append("source_review_context")
        if row.get("scope_match_context") != symbol_change_scope_match_context(row):
            invalid_fields.append("scope_match_context")
        if row.get("workflow_review_context") != symbol_change_workflow_review_context(row):
            invalid_fields.append("workflow_review_context")
        if missing_keys or invalid_fields:
            row_gaps.append(
                {
                    "row_index": index,
                    "change_id": row.get("change_id", ""),
                    "missing_keys": missing_keys,
                    "invalid_fields": invalid_fields,
                    "available_keys": sorted(row)[:25],
                }
            )
        counters["match_status_counts"][match_status] += 1
        counters["symbol_change_workflow_queue_counts"][workflow_queue] += 1
        counters["review_bucket_counts"][review_bucket] += 1
        counters["review_priority_counts"][review_priority] += 1
        counters["recency_bucket_counts"][recency_bucket] += 1
        counters["review_priority_recency_counts"][f"{review_priority}:{recency_bucket}"] += 1
        counters["workflow_queue_recency_counts"][f"{workflow_queue}:{recency_bucket}"] += 1
        counters["workflow_queue_priority_counts"][f"{workflow_queue}:{review_priority}"] += 1
        counters["workflow_queue_scope_counts"][f"{workflow_queue}:{exchange_scope_status}"] += 1
        counters["workflow_queue_match_status_counts"][f"{workflow_queue}:{match_status}"] += 1
        counters["workflow_queue_source_hint_counts"][f"{workflow_queue}:{row.get('source_exchange_hint', '') or 'missing'}"] += 1
        counters["workflow_queue_source_confidence_counts"][f"{workflow_queue}:{row.get('source_confidence', '') or 'missing'}"] += 1
        counters["workflow_queue_issuer_name_review_counts"][f"{workflow_queue}:{issuer_name_review_status}"] += 1
        counters["workflow_queue_listing_key_review_counts"][f"{workflow_queue}:{listing_key_review_status}"] += 1
        counters["workflow_queue_review_strategy_counts"][f"{workflow_queue}:{review_strategy}"] += 1
        counters["apply_eligibility_counts"][apply_eligibility] += 1
        apply_readiness = symbol_change_apply_readiness_for(apply_eligibility)
        counters["symbol_change_apply_readiness_counts"][apply_readiness] += 1
        counters["verification_evidence_required_counts"][str(row.get("verification_evidence_required", ""))] += 1
        if review_priority == "P1" and recency_bucket in {"recent_7d", "recent_30d"}:
            counters["time_sensitive_workflow_queue_counts"][workflow_queue] += 1
            counters["time_sensitive_recency_counts"][recency_bucket] += 1
            counters["time_sensitive_apply_readiness_counts"][apply_readiness] += 1
        counters["recommended_action_counts"][str(row.get("recommended_action", ""))] += 1
        counters["exchange_scope_status_counts"][exchange_scope_status] += 1
    summary_mismatches = {
        key: compare_counter_to_reported(counter, summary.get(key))
        for key, counter in counters.items()
        if key
        not in {
            "workflow_queue_source_hint_counts",
            "workflow_queue_source_confidence_counts",
            "workflow_queue_issuer_name_review_counts",
            "workflow_queue_listing_key_review_counts",
        }
        and key != "workflow_queue_review_strategy_counts"
    }
    expected_queue_source_hint_counts = {
        queue: dict(
            sorted(
                Counter(
                    str(row.get("source_exchange_hint", "") or "missing")
                    for row in rows
                    if isinstance(row, dict) and str(row.get("symbol_change_workflow_queue", "")) == queue
                ).items()
            )
        )
        for queue in sorted(counters["symbol_change_workflow_queue_counts"])
    }
    if summary.get("workflow_queue_source_hint_counts") != expected_queue_source_hint_counts:
        summary_mismatches["workflow_queue_source_hint_counts"] = {
            "reported": summary.get("workflow_queue_source_hint_counts"),
            "actual": expected_queue_source_hint_counts,
        }
    expected_queue_source_confidence_counts = {
        queue: dict(
            sorted(
                Counter(
                    str(row.get("source_confidence", "") or "missing")
                    for row in rows
                    if isinstance(row, dict) and str(row.get("symbol_change_workflow_queue", "")) == queue
                ).items()
            )
        )
        for queue in sorted(counters["symbol_change_workflow_queue_counts"])
    }
    if summary.get("workflow_queue_source_confidence_counts") != expected_queue_source_confidence_counts:
        summary_mismatches["workflow_queue_source_confidence_counts"] = {
            "reported": summary.get("workflow_queue_source_confidence_counts"),
            "actual": expected_queue_source_confidence_counts,
        }
    expected_queue_issuer_name_review_counts = {
        queue: dict(
            sorted(
                Counter(
                    str(row.get("issuer_name_review_status", "") or "missing")
                    for row in rows
                    if isinstance(row, dict) and str(row.get("symbol_change_workflow_queue", "")) == queue
                ).items()
            )
        )
        for queue in sorted(counters["symbol_change_workflow_queue_counts"])
    }
    if summary.get("workflow_queue_issuer_name_review_counts") != expected_queue_issuer_name_review_counts:
        summary_mismatches["workflow_queue_issuer_name_review_counts"] = {
            "reported": summary.get("workflow_queue_issuer_name_review_counts"),
            "actual": expected_queue_issuer_name_review_counts,
        }
    expected_queue_listing_key_review_counts = {
        queue: dict(
            sorted(
                Counter(
                    str(row.get("listing_key_review_status", "") or "missing")
                    for row in rows
                    if isinstance(row, dict) and str(row.get("symbol_change_workflow_queue", "")) == queue
                ).items()
            )
        )
        for queue in sorted(counters["symbol_change_workflow_queue_counts"])
    }
    if summary.get("workflow_queue_listing_key_review_counts") != expected_queue_listing_key_review_counts:
        summary_mismatches["workflow_queue_listing_key_review_counts"] = {
            "reported": summary.get("workflow_queue_listing_key_review_counts"),
            "actual": expected_queue_listing_key_review_counts,
        }
    expected_queue_strategy_counts = {
        queue: dict(
            sorted(
                Counter(
                    str(row.get("review_strategy", ""))
                    for row in rows
                    if isinstance(row, dict) and str(row.get("symbol_change_workflow_queue", "")) == queue
                ).items()
            )
        )
        for queue in sorted(counters["symbol_change_workflow_queue_counts"])
    }
    if summary.get("workflow_queue_review_strategy_counts") != expected_queue_strategy_counts:
        summary_mismatches["workflow_queue_review_strategy_counts"] = {
            "reported": summary.get("workflow_queue_review_strategy_counts"),
            "actual": expected_queue_strategy_counts,
        }
    symbol_change_backlog = summary.get("symbol_change_backlog")
    symbol_change_backlog_gaps: list[dict[str, Any]] = []
    if not isinstance(symbol_change_backlog, dict):
        symbol_change_backlog_gaps.append({"field": "symbol_change_backlog", "reason": "expected_object"})
    else:
        expected_backlog = {
            "rows": len(rows),
            "verified_rename_or_delisting_review_rows": counters["symbol_change_workflow_queue_counts"].get(
                "review_verified_rename_or_delisting", 0
            ),
            "duplicate_or_cross_listing_review_rows": counters["symbol_change_workflow_queue_counts"].get(
                "review_duplicate_or_cross_listing", 0
            ),
            "already_reflected_audit_rows": counters["symbol_change_workflow_queue_counts"].get(
                "audit_already_reflected", 0
            ),
            "out_of_scope_collision_blocked_rows": counters["symbol_change_workflow_queue_counts"].get(
                "blocked_out_of_scope_symbol_collision", 0
            ),
            "missing_source_scope_mapping_rows": counters["symbol_change_workflow_queue_counts"].get(
                "blocked_missing_source_scope_mapping", 0
            ),
            "no_dataset_match_documentation_rows": counters["symbol_change_workflow_queue_counts"].get(
                "document_no_dataset_match", 0
            ),
            "time_sensitive_review_rows": sum(counters["time_sensitive_workflow_queue_counts"].values()),
            "direct_symbol_change_apply_allowed_rows": 0,
        }
        for key, expected in expected_backlog.items():
            if symbol_change_backlog.get(key) != expected:
                symbol_change_backlog_gaps.append(
                    {
                        "field": f"symbol_change_backlog.{key}",
                        "reported": symbol_change_backlog.get(key),
                        "expected": expected,
                    }
                )
        if symbol_change_backlog.get("secondary_feed_apply_authorized") is not False:
            symbol_change_backlog_gaps.append(
                {
                    "field": "symbol_change_backlog.secondary_feed_apply_authorized",
                    "reported": symbol_change_backlog.get("secondary_feed_apply_authorized"),
                    "expected": False,
                }
            )
        if not symbol_change_backlog.get("source_gate"):
            symbol_change_backlog_gaps.append(
                {"field": "symbol_change_backlog.source_gate", "reason": "missing_source_gate"}
            )
    top_workflow_batch_gaps: list[dict[str, Any]] = []
    top_workflow_batches = summary.get("top_symbol_change_workflow_batches")
    if not isinstance(top_workflow_batches, list) or not top_workflow_batches:
        top_workflow_batch_gaps.append(
            {"field": "top_symbol_change_workflow_batches", "reason": "expected_ranked_workflow_batches"}
        )
    elif isinstance(top_workflow_batches, list):
        for index, batch in enumerate(top_workflow_batches):
            if not isinstance(batch, dict):
                top_workflow_batch_gaps.append({"row_index": index, "reason": "expected_object"})
                continue
            queue = str(batch.get("symbol_change_workflow_queue", ""))
            strategy = str(batch.get("review_strategy", ""))
            expected_strategy, _expected_evidence = symbol_change_workflow_strategy_for(queue)
            missing_keys = [
                key
                for key in (
                    "symbol_change_workflow_queue",
                    "review_priority",
                    "recency_bucket",
                    "exchange_scope_status",
                    "rows",
                    "review_strategy",
                    "evidence_required",
                    "recommended_next_source",
                    "source_gate",
                )
                if key not in batch
            ]
            invalid_fields = []
            if queue not in SYMBOL_CHANGE_WORKFLOW_QUEUES:
                invalid_fields.append("symbol_change_workflow_queue")
            if batch.get("review_priority") not in SYMBOL_CHANGE_PRIORITIES:
                invalid_fields.append("review_priority")
            if batch.get("recency_bucket") not in SYMBOL_CHANGE_RECENCY_BUCKETS:
                invalid_fields.append("recency_bucket")
            if batch.get("exchange_scope_status") not in SYMBOL_CHANGE_EXCHANGE_SCOPE_STATUSES:
                invalid_fields.append("exchange_scope_status")
            if not isinstance(batch.get("rows"), int) or batch.get("rows", 0) <= 0:
                invalid_fields.append("rows")
            if strategy not in SYMBOL_CHANGE_REVIEW_STRATEGIES or strategy != expected_strategy:
                invalid_fields.append("review_strategy")
            if not batch.get("evidence_required") or batch.get("evidence_required") in {"none", "ticker_match_only"}:
                invalid_fields.append("evidence_required")
            if not batch.get("recommended_next_source"):
                invalid_fields.append("recommended_next_source")
            if not batch.get("source_gate"):
                invalid_fields.append("source_gate")
            if missing_keys or invalid_fields:
                top_workflow_batch_gaps.append(
                    {"row_index": index, "missing_keys": missing_keys, "invalid_fields": invalid_fields}
                )
    summary_mismatches = {key: value for key, value in summary_mismatches.items() if value}
    bucket_priorities = summary.get("review_bucket_priorities", {})
    missing_bucket_priorities: dict[str, str] = {}
    if not isinstance(bucket_priorities, dict):
        missing_bucket_priorities = dict(counters["review_bucket_counts"])
    else:
        missing_bucket_priorities = {
            bucket: str(bucket_priorities.get(bucket, ""))
            for bucket in counters["review_bucket_counts"]
            if bucket_priorities.get(bucket) not in SYMBOL_CHANGE_PRIORITIES
        }
    required_distinctions = {
        "already_reflected": {
            "already_reflected_in_source_scope",
            "already_reflected_in_scope_with_global_symbol_collision",
        },
        "rename_or_delisting": {"action_required_possible_rename_or_delisting"},
        "duplicate_or_cross_listing": {"action_required_duplicate_or_cross_listing"},
        "no_match_or_scope_gap": {"no_dataset_match_for_source_scope", "manual_scope_mapping_required"},
    }
    missing_distinctions = [
        key
        for key, accepted_buckets in required_distinctions.items()
        if not (accepted_buckets & set(counters["review_bucket_counts"]))
    ]
    required_workflow_queues = {
        "audit_already_reflected",
        "document_no_dataset_match",
        "review_duplicate_or_cross_listing",
        "review_verified_rename_or_delisting",
    }
    missing_workflow_queues = sorted(required_workflow_queues - set(counters["symbol_change_workflow_queue_counts"]))
    return {
        "passed": (
            bool(rows)
            and not missing_meta_keys
            and not invalid_generated_at
            and not source_url_mismatch
            and not source_policy_mismatch
            and not source_file_mismatches
            and not unexpected_source_files
            and not policy_missing_markers
            and not missing_summary_keys
            and not row_count_mismatch
            and not row_gaps
            and not summary_mismatches
            and not symbol_change_backlog_gaps
            and not missing_bucket_priorities
            and not missing_distinctions
            and not missing_workflow_queues
            and not top_workflow_batch_gaps
        ),
        "missing_meta_keys": missing_meta_keys,
        "invalid_generated_at": invalid_generated_at,
        "source_url_mismatch": source_url_mismatch,
        "source_policy_mismatch": source_policy_mismatch,
        "source_file_mismatches": source_file_mismatches,
        "unexpected_source_files": unexpected_source_files,
        "policy_missing_markers": policy_missing_markers,
        "rows": len(rows),
        "missing_summary_keys": missing_summary_keys,
        "row_count_mismatch": row_count_mismatch,
        "row_gaps": row_gaps[:20],
        "summary_mismatches": summary_mismatches,
        "symbol_change_backlog_gaps": symbol_change_backlog_gaps,
        "missing_bucket_priorities": missing_bucket_priorities,
        "missing_distinctions": missing_distinctions,
        "missing_workflow_queues": missing_workflow_queues,
        "top_workflow_batch_gaps": top_workflow_batch_gaps,
        "required_row_keys": list(REQUIRED_SYMBOL_CHANGE_ROW_KEYS),
        "accepted_match_statuses": sorted(SYMBOL_CHANGE_MATCH_STATUSES),
        "accepted_review_buckets": sorted(SYMBOL_CHANGE_REVIEW_BUCKETS),
        "accepted_apply_eligibilities": sorted(SYMBOL_CHANGE_APPLY_ELIGIBILITIES),
    }


def evaluate_ohlcv_plausibility_gate(ohlcv_report: dict[str, Any]) -> dict[str, Any]:
    meta = ohlcv_report.get("_meta", {})
    summary = ohlcv_report.get("summary", {})
    rows = ohlcv_report.get("review_items", [])
    flagged_rows = ohlcv_report.get("flagged_items", [])
    if not isinstance(meta, dict):
        meta = {}
    if not isinstance(summary, dict):
        summary = {}
    if not isinstance(rows, list):
        rows = []
    if not isinstance(flagged_rows, list):
        flagged_rows = []
    source_files = meta.get("source_files", {})
    if not isinstance(source_files, dict):
        source_files = {}
    missing_meta_keys = [
        key
        for key in ("generated_at", "rows", "selected_rows", "skipped_not_checked_rows", "source_files", "policy", "parameters")
        if key not in meta or meta.get(key) in ("", None, {}, [])
    ]
    invalid_generated_at = (
        str(meta.get("generated_at", ""))
        if meta.get("generated_at") and not is_valid_iso_utc_timestamp(str(meta.get("generated_at")))
        else ""
    )
    source_file_mismatches = {
        key: {"expected": expected, "actual": source_files.get(key)}
        for key, expected in EXPECTED_OHLCV_SOURCE_FILES.items()
        if source_files.get(key) != expected
    }
    unexpected_source_files = sorted(key for key in source_files if key not in EXPECTED_OHLCV_SOURCE_FILES)
    missing_summary_keys = [key for key in REQUIRED_OHLCV_SUMMARY_KEYS if key not in summary]
    expected_rows = meta.get("rows", 0)
    row_count_mismatch = {
        "reported": expected_rows,
        "actual": len(rows),
    } if expected_rows != len(rows) else {}
    counters: dict[str, Counter[str]] = {
        "status_counts": Counter(),
        "issue_counts": Counter(),
        "selection_bucket_counts": Counter(),
        "review_bucket_counts": Counter(),
        "review_priority_counts": Counter(),
        "plausibility_use_counts": Counter(),
        "canonical_data_change_authorization_counts": Counter(),
        "verification_evidence_required_counts": Counter(),
        "source_gap_class_counts": Counter(),
    }
    selection_bucket_exchange_counter: dict[str, Counter[str]] = defaultdict(Counter)
    selection_bucket_status_counter: dict[str, Counter[str]] = defaultdict(Counter)
    review_bucket_selection_bucket_counter: dict[str, Counter[str]] = defaultdict(Counter)
    review_bucket_exchange_counter: dict[str, Counter[str]] = defaultdict(Counter)
    review_bucket_sampling_strategy_counter: dict[str, Counter[str]] = defaultdict(Counter)
    review_bucket_sampling_readiness_counter: dict[str, Counter[str]] = defaultdict(Counter)
    row_gaps: list[dict[str, Any]] = []
    forbidden_action_rows: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            row_gaps.append({"row_index": index, "reason": "ohlcv_row_is_not_object"})
            continue
        missing_keys = [key for key in REQUIRED_OHLCV_ROW_KEYS if key not in row]
        invalid_fields: list[str] = []
        plausibility_status = str(row.get("plausibility_status", ""))
        review_priority = str(row.get("review_priority", ""))
        plausibility_score = row.get("plausibility_score")
        if plausibility_status not in OHLCV_PLAUSIBILITY_STATUSES:
            invalid_fields.append("plausibility_status")
        if review_priority not in OHLCV_REVIEW_PRIORITIES:
            invalid_fields.append("review_priority")
        if not isinstance(plausibility_score, int) or isinstance(plausibility_score, bool) or not 0 <= plausibility_score <= 100:
            invalid_fields.append("plausibility_score")
        if not str(row.get("selection_bucket", "")).strip():
            invalid_fields.append("selection_bucket")
        if not str(row.get("plausibility_use", "")).strip():
            invalid_fields.append("plausibility_use")
        if not str(row.get("verification_evidence_required", "")).strip():
            invalid_fields.append("verification_evidence_required")
        review_bucket = str(row.get("review_bucket", ""))
        strategy, evidence_required = ohlcv_sampling_strategy_for(review_bucket)
        if row.get("sampling_strategy") != strategy:
            invalid_fields.append("sampling_strategy")
        if (
            row.get("verification_evidence_required") != evidence_required
            and "verification_evidence_required" not in invalid_fields
        ):
            invalid_fields.append("verification_evidence_required")
        expected_next_source = ohlcv_recommended_next_source_for(
            review_bucket,
            str(row.get("exchange", "") or "missing"),
        )
        if row.get("recommended_next_source") != expected_next_source:
            invalid_fields.append("recommended_next_source")
        expected_source_gate = ohlcv_source_gate_for(review_bucket)
        if row.get("source_gate") != expected_source_gate:
            invalid_fields.append("source_gate")
        if row.get("selection_context") != ohlcv_selection_context(row):
            invalid_fields.append("selection_context")
        if row.get("ohlcv_sample_context") != ohlcv_sample_context(row):
            invalid_fields.append("ohlcv_sample_context")
        if row.get("plausibility_review_context") != ohlcv_plausibility_review_context(row):
            invalid_fields.append("plausibility_review_context")
        if (
            plausibility_status != "pass"
            and row.get("verification_evidence_required") in OHLCV_ALLOWED_AUTO_CHANGE_EVIDENCE
            and "verification_evidence_required" not in invalid_fields
        ):
            invalid_fields.append("verification_evidence_required")
        issues = row.get("issues", [])
        if not isinstance(issues, list):
            invalid_fields.append("issues")
            issues = []
        if plausibility_status != "pass" and not issues:
            invalid_fields.append("issues")
        recommended_action = str(row.get("recommended_action", ""))
        action_normalized = recommended_action.lower()
        forbidden_markers = [
            marker
            for marker in OHLCV_FORBIDDEN_AUTOMATIC_ACTION_MARKERS
            if marker in action_normalized
        ]
        if forbidden_markers:
            forbidden_action_rows.append(
                {
                    "row_index": index,
                    "listing_key": row.get("listing_key", ""),
                    "recommended_action": recommended_action,
                    "forbidden_markers": forbidden_markers,
                }
            )
        if missing_keys or invalid_fields:
            row_gaps.append(
                {
                    "row_index": index,
                    "listing_key": row.get("listing_key", ""),
                    "missing_keys": missing_keys,
                    "invalid_fields": invalid_fields,
                    "available_keys": sorted(row)[:25],
                }
            )
        counters["status_counts"][plausibility_status] += 1
        selection_bucket = str(row.get("selection_bucket", ""))
        counters["selection_bucket_counts"][selection_bucket] += 1
        selection_bucket_exchange_counter[selection_bucket][str(row.get("exchange", "") or "missing")] += 1
        selection_bucket_status_counter[selection_bucket][plausibility_status] += 1
        counters["review_bucket_counts"][review_bucket] += 1
        review_bucket_selection_bucket_counter[review_bucket][selection_bucket] += 1
        review_bucket_exchange_counter[review_bucket][str(row.get("exchange", "") or "missing")] += 1
        review_bucket_sampling_strategy_counter[review_bucket][strategy] += 1
        if plausibility_status == "not_checked":
            sampling_readiness = "needs_ohlcv_sample"
        elif row.get("ohlcv_source") == "yahoo_chart":
            sampling_readiness = "checked_yahoo_sample"
        elif row.get("ohlcv_source"):
            sampling_readiness = "checked_local_sample"
        else:
            sampling_readiness = "checked_without_source"
        review_bucket_sampling_readiness_counter[review_bucket][sampling_readiness] += 1
        counters["review_priority_counts"][review_priority] += 1
        counters["plausibility_use_counts"][str(row.get("plausibility_use", ""))] += 1
        if review_bucket == "checked_ohlcv_anomaly_requires_listing_review":
            authorization = "official_listing_review_required_before_any_canonical_change"
        else:
            authorization = "no_canonical_data_change_authorized"
        counters["canonical_data_change_authorization_counts"][authorization] += 1
        counters["verification_evidence_required_counts"][str(row.get("verification_evidence_required", ""))] += 1
        source_gap_class = str(row.get("source_gap_class", ""))
        if row.get("entry_quality_status") == "source_gap":
            counters["source_gap_class_counts"][source_gap_class or "none"] += 1
        for issue in issues:
            if isinstance(issue, dict):
                issue_type = str(issue.get("issue_type", ""))
                if issue_type:
                    counters["issue_counts"][issue_type] += 1
    summary_mismatches = {
        key: compare_counter_to_reported(counter, summary.get(key))
        for key, counter in counters.items()
    }
    summary_mismatches = {key: value for key, value in summary_mismatches.items() if value}
    selection_bucket_exchange_mismatches = {
        bucket: compare_counter_to_reported(
            counter,
            summary.get("selection_bucket_exchange_counts", {}).get(bucket)
            if isinstance(summary.get("selection_bucket_exchange_counts"), dict)
            else None,
        )
        for bucket, counter in selection_bucket_exchange_counter.items()
    }
    selection_bucket_exchange_mismatches = {
        bucket: mismatch for bucket, mismatch in selection_bucket_exchange_mismatches.items() if mismatch
    }
    selection_bucket_status_mismatches = {
        bucket: compare_counter_to_reported(
            counter,
            summary.get("selection_bucket_status_counts", {}).get(bucket)
            if isinstance(summary.get("selection_bucket_status_counts"), dict)
            else None,
        )
        for bucket, counter in selection_bucket_status_counter.items()
    }
    selection_bucket_status_mismatches = {
        bucket: mismatch for bucket, mismatch in selection_bucket_status_mismatches.items() if mismatch
    }
    review_bucket_selection_bucket_mismatches = {
        bucket: compare_counter_to_reported(
            counter,
            summary.get("review_bucket_selection_bucket_counts", {}).get(bucket)
            if isinstance(summary.get("review_bucket_selection_bucket_counts"), dict)
            else None,
        )
        for bucket, counter in review_bucket_selection_bucket_counter.items()
    }
    review_bucket_selection_bucket_mismatches = {
        bucket: mismatch for bucket, mismatch in review_bucket_selection_bucket_mismatches.items() if mismatch
    }
    review_bucket_exchange_mismatches = {
        bucket: compare_counter_to_reported(
            counter,
            summary.get("review_bucket_exchange_counts", {}).get(bucket)
            if isinstance(summary.get("review_bucket_exchange_counts"), dict)
            else None,
        )
        for bucket, counter in review_bucket_exchange_counter.items()
    }
    review_bucket_exchange_mismatches = {
        bucket: mismatch for bucket, mismatch in review_bucket_exchange_mismatches.items() if mismatch
    }
    review_bucket_sampling_strategy_mismatches = {
        bucket: compare_counter_to_reported(
            counter,
            summary.get("review_bucket_sampling_strategy_counts", {}).get(bucket)
            if isinstance(summary.get("review_bucket_sampling_strategy_counts"), dict)
            else None,
        )
        for bucket, counter in review_bucket_sampling_strategy_counter.items()
    }
    review_bucket_sampling_strategy_mismatches = {
        bucket: mismatch for bucket, mismatch in review_bucket_sampling_strategy_mismatches.items() if mismatch
    }
    review_bucket_sampling_readiness_mismatches = {
        bucket: compare_counter_to_reported(
            counter,
            summary.get("review_bucket_sampling_readiness_counts", {}).get(bucket)
            if isinstance(summary.get("review_bucket_sampling_readiness_counts"), dict)
            else None,
        )
        for bucket, counter in review_bucket_sampling_readiness_counter.items()
    }
    review_bucket_sampling_readiness_mismatches = {
        bucket: mismatch for bucket, mismatch in review_bucket_sampling_readiness_mismatches.items() if mismatch
    }
    sampling_coverage = summary.get("sampling_coverage", {})
    sampling_coverage_gaps: list[dict[str, Any]] = []
    if not isinstance(sampling_coverage, dict):
        sampling_coverage_gaps.append({"field": "sampling_coverage", "reason": "expected_object"})
        sampling_coverage = {}
    required_coverage_keys = (
        "selected_rows",
        "report_rows",
        "checked_rows",
        "not_checked_rows",
        "skipped_not_checked_rows",
        "local_sample_rows",
        "yahoo_sample_rows",
        "warn_or_source_gap_signal_rows",
    )
    for key in required_coverage_keys:
        value = sampling_coverage.get(key)
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            sampling_coverage_gaps.append({"field": f"sampling_coverage.{key}", "reason": "expected_nonnegative_integer"})
    if not sampling_coverage_gaps:
        report_rows = sampling_coverage["report_rows"]
        checked_rows = sampling_coverage["checked_rows"]
        not_checked_rows = sampling_coverage["not_checked_rows"]
        selected_rows = sampling_coverage["selected_rows"]
        skipped_rows = sampling_coverage["skipped_not_checked_rows"]
        signal_rows = sampling_coverage["warn_or_source_gap_signal_rows"]
        if report_rows != len(rows):
            sampling_coverage_gaps.append(
                {"field": "sampling_coverage.report_rows", "reported": report_rows, "actual": len(rows)}
            )
        if checked_rows + not_checked_rows != report_rows:
            sampling_coverage_gaps.append(
                {
                    "field": "sampling_coverage.checked_plus_not_checked",
                    "reported": checked_rows + not_checked_rows,
                    "expected": report_rows,
                }
            )
        if selected_rows < report_rows or selected_rows - report_rows != skipped_rows:
            sampling_coverage_gaps.append(
                {
                    "field": "sampling_coverage.selected_rows",
                    "selected_rows": selected_rows,
                    "report_rows": report_rows,
                    "skipped_not_checked_rows": skipped_rows,
                }
            )
        if not_checked_rows != counters["status_counts"].get("not_checked", 0):
            sampling_coverage_gaps.append(
                {
                    "field": "sampling_coverage.not_checked_rows",
                    "reported": not_checked_rows,
                    "expected": counters["status_counts"].get("not_checked", 0),
                }
            )
        expected_signal_rows = sum(counters["status_counts"].get(status, 0) for status in ("warn", "source_gap", "notice"))
        if signal_rows != expected_signal_rows:
            sampling_coverage_gaps.append(
                {
                    "field": "sampling_coverage.warn_or_source_gap_signal_rows",
                    "reported": signal_rows,
                    "expected": expected_signal_rows,
                }
            )
    ohlcv_sampling_backlog = summary.get("ohlcv_sampling_backlog")
    ohlcv_sampling_backlog_gaps: list[dict[str, Any]] = []
    if not isinstance(ohlcv_sampling_backlog, dict):
        ohlcv_sampling_backlog_gaps.append({"field": "ohlcv_sampling_backlog", "reason": "expected_object"})
    elif not sampling_coverage_gaps:
        expected_backlog = {
            "selected_rows": sampling_coverage["selected_rows"],
            "report_rows": len(rows),
            "checked_rows": sampling_coverage["checked_rows"],
            "not_checked_rows": sampling_coverage["not_checked_rows"],
            "entry_quality_warn_sample_rows": counters["selection_bucket_counts"].get("entry_quality_warn", 0),
            "source_gap_cluster_sample_rows": sum(
                count
                for bucket, count in counters["selection_bucket_counts"].items()
                if str(bucket).startswith("source_gap:")
            ),
            "large_exchange_baseline_sample_rows": sum(
                count
                for bucket, count in counters["selection_bucket_counts"].items()
                if str(bucket).startswith("large_exchange:")
            ),
            "warn_or_source_gap_signal_rows": sampling_coverage["warn_or_source_gap_signal_rows"],
            "direct_canonical_data_change_allowed_rows": 0,
        }
        for key, expected in expected_backlog.items():
            if ohlcv_sampling_backlog.get(key) != expected:
                ohlcv_sampling_backlog_gaps.append(
                    {
                        "field": f"ohlcv_sampling_backlog.{key}",
                        "reported": ohlcv_sampling_backlog.get(key),
                        "expected": expected,
                    }
                )
        if ohlcv_sampling_backlog.get("plausibility_signal_only") is not True:
            ohlcv_sampling_backlog_gaps.append(
                {
                    "field": "ohlcv_sampling_backlog.plausibility_signal_only",
                    "reported": ohlcv_sampling_backlog.get("plausibility_signal_only"),
                    "expected": True,
                }
            )
        if not ohlcv_sampling_backlog.get("source_gate"):
            ohlcv_sampling_backlog_gaps.append(
                {"field": "ohlcv_sampling_backlog.source_gate", "reason": "missing_source_gate"}
            )
    policy = str(meta.get("policy", "")).lower()
    missing_policy_markers = [
        marker
        for marker in ("plausibility", "never authorize", "official", "listing-keyed")
        if marker not in policy
    ]
    missing_sampling_buckets = [
        bucket
        for bucket in ("entry_quality_warn", "source_gap:", "large_exchange:")
        if not any(str(value).startswith(bucket) for value in counters["selection_bucket_counts"])
    ]
    top_flagged_exchanges = summary.get("top_flagged_exchanges", [])
    invalid_top_flagged_exchanges = (
        not isinstance(top_flagged_exchanges, list)
        or any(
            not isinstance(row, dict) or not row.get("exchange") or not is_nonnegative_number(row.get("not_checked"))
            for row in top_flagged_exchanges
        )
    )
    top_sampling_batch_gaps: list[dict[str, Any]] = []
    top_sampling_batches = summary.get("top_ohlcv_sampling_batches", [])
    if not isinstance(top_sampling_batches, list) or not top_sampling_batches:
        top_sampling_batch_gaps.append({"field": "top_ohlcv_sampling_batches", "reason": "expected_ranked_sampling_batches"})
    elif isinstance(top_sampling_batches, list):
        for index, batch in enumerate(top_sampling_batches):
            if not isinstance(batch, dict):
                top_sampling_batch_gaps.append({"row_index": index, "reason": "expected_object"})
                continue
            review_bucket = str(batch.get("review_bucket", ""))
            strategy = str(batch.get("sampling_strategy", ""))
            expected_strategy, _expected_evidence = ohlcv_sampling_strategy_for(review_bucket)
            missing_keys = [
                key
                for key in (
                    "review_bucket",
                    "selection_bucket",
                    "exchange",
                    "plausibility_status",
                    "rows",
                    "review_priority",
                    "sampling_strategy",
                    "evidence_required",
                    "recommended_next_source",
                    "source_gate",
                )
                if key not in batch
            ]
            invalid_fields = []
            if review_bucket not in review_bucket_sampling_strategy_counter:
                invalid_fields.append("review_bucket")
            if batch.get("plausibility_status") not in OHLCV_PLAUSIBILITY_STATUSES:
                invalid_fields.append("plausibility_status")
            if batch.get("review_priority") not in OHLCV_REVIEW_PRIORITIES:
                invalid_fields.append("review_priority")
            if not isinstance(batch.get("rows"), int) or batch.get("rows", 0) <= 0:
                invalid_fields.append("rows")
            if strategy not in OHLCV_SAMPLING_STRATEGIES or strategy != expected_strategy:
                invalid_fields.append("sampling_strategy")
            if not batch.get("evidence_required"):
                invalid_fields.append("evidence_required")
            if not batch.get("recommended_next_source"):
                invalid_fields.append("recommended_next_source")
            if not batch.get("source_gate"):
                invalid_fields.append("source_gate")
            expected_next_source = ohlcv_recommended_next_source_for(review_bucket, str(batch.get("exchange", "") or "missing"))
            if batch.get("recommended_next_source") != expected_next_source:
                invalid_fields.append("recommended_next_source")
            expected_source_gate = ohlcv_source_gate_for(review_bucket)
            if batch.get("source_gate") != expected_source_gate:
                invalid_fields.append("source_gate")
            if missing_keys or invalid_fields:
                top_sampling_batch_gaps.append(
                    {"row_index": index, "missing_keys": missing_keys, "invalid_fields": invalid_fields}
                )
    return {
        "passed": (
            bool(rows)
            and not missing_meta_keys
            and not invalid_generated_at
            and not source_file_mismatches
            and not unexpected_source_files
            and not missing_summary_keys
            and not row_count_mismatch
            and not row_gaps
            and not forbidden_action_rows
            and not summary_mismatches
            and not selection_bucket_exchange_mismatches
            and not selection_bucket_status_mismatches
            and not review_bucket_selection_bucket_mismatches
            and not review_bucket_exchange_mismatches
            and not review_bucket_sampling_strategy_mismatches
            and not review_bucket_sampling_readiness_mismatches
            and not sampling_coverage_gaps
            and not ohlcv_sampling_backlog_gaps
            and not missing_policy_markers
            and not missing_sampling_buckets
            and not invalid_top_flagged_exchanges
            and not top_sampling_batch_gaps
        ),
        "missing_meta_keys": missing_meta_keys,
        "invalid_generated_at": invalid_generated_at,
        "source_file_mismatches": source_file_mismatches,
        "unexpected_source_files": unexpected_source_files,
        "rows": len(rows),
        "flagged_rows": len(flagged_rows),
        "missing_summary_keys": missing_summary_keys,
        "row_count_mismatch": row_count_mismatch,
        "row_gaps": row_gaps[:20],
        "forbidden_action_rows": forbidden_action_rows[:20],
        "summary_mismatches": summary_mismatches,
        "selection_bucket_exchange_mismatches": selection_bucket_exchange_mismatches,
        "selection_bucket_status_mismatches": selection_bucket_status_mismatches,
        "review_bucket_selection_bucket_mismatches": review_bucket_selection_bucket_mismatches,
        "review_bucket_exchange_mismatches": review_bucket_exchange_mismatches,
        "review_bucket_sampling_strategy_mismatches": review_bucket_sampling_strategy_mismatches,
        "review_bucket_sampling_readiness_mismatches": review_bucket_sampling_readiness_mismatches,
        "sampling_coverage_gaps": sampling_coverage_gaps,
        "ohlcv_sampling_backlog_gaps": ohlcv_sampling_backlog_gaps,
        "missing_policy_markers": missing_policy_markers,
        "missing_sampling_buckets": missing_sampling_buckets,
        "invalid_top_flagged_exchanges": invalid_top_flagged_exchanges,
        "top_sampling_batch_gaps": top_sampling_batch_gaps,
        "required_row_keys": list(REQUIRED_OHLCV_ROW_KEYS),
        "accepted_plausibility_statuses": sorted(OHLCV_PLAUSIBILITY_STATUSES),
    }


def evaluate_masterfile_collision_gate(collision_report: dict[str, Any]) -> dict[str, Any]:
    meta = collision_report.get("_meta", {})
    summary = collision_report.get("summary", {})
    rows = collision_report.get("rows", [])
    if not isinstance(meta, dict):
        meta = {}
    if not isinstance(summary, dict):
        summary = {}
    if not isinstance(rows, list):
        rows = []
    source_files = meta.get("source_files", {})
    if not isinstance(source_files, dict):
        source_files = {}
    missing_meta_keys = [
        key
        for key in ("generated_at", "source_files", "policy")
        if key not in meta or meta.get(key) in ("", None, {}, [])
    ]
    invalid_generated_at = (
        str(meta.get("generated_at", ""))
        if meta.get("generated_at") and not is_valid_iso_utc_timestamp(str(meta.get("generated_at")))
        else ""
    )
    source_file_mismatches = {
        key: {"expected": expected, "actual": source_files.get(key)}
        for key, expected in EXPECTED_COLLISION_SOURCE_FILES.items()
        if source_files.get(key) != expected
    }
    unexpected_source_files = sorted(key for key in source_files if key not in EXPECTED_COLLISION_SOURCE_FILES)
    meta_policy_mismatch = (
        isinstance(meta.get("policy"), dict)
        and isinstance(summary.get("policy"), dict)
        and meta.get("policy") != summary.get("policy")
    )
    missing_summary_keys = [key for key in REQUIRED_COLLISION_SUMMARY_KEYS if key not in summary]
    row_count_mismatch = {
        "reported": summary.get("rows", 0),
        "actual": len(rows),
    } if summary.get("rows", 0) != len(rows) else {}
    counters: dict[str, Counter[str]] = {
        "decision_totals": Counter(),
        "review_bucket_totals": Counter(),
        "review_priority_totals": Counter(),
        "collision_risk_flag_totals": Counter(),
        "identity_resolution_queue_totals": Counter(),
        "clearance_evidence_totals": Counter(),
        "exchange_totals": Counter(),
        "official_asset_type_totals": Counter(),
        "asset_type_mismatch_totals": Counter(),
        "official_source_totals": Counter(),
    }
    row_gaps: list[dict[str, Any]] = []
    unsafe_symbol_only_rows: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            row_gaps.append({"row_index": index, "reason": "collision_row_is_not_object"})
            continue
        missing_keys = [key for key in REQUIRED_COLLISION_ROW_KEYS if not row.get(key)]
        invalid_fields: list[str] = []
        review_decision = str(row.get("review_decision", ""))
        review_bucket = str(row.get("review_bucket", ""))
        review_priority = str(row.get("review_priority", ""))
        identity_evidence = str(row.get("identity_evidence", ""))
        collision_risk_flags = str(row.get("collision_risk_flags", ""))
        identity_resolution_queue = str(row.get("identity_resolution_queue", ""))
        review_strategy = str(row.get("review_strategy", ""))
        verification_evidence_required = str(row.get("verification_evidence_required", ""))
        recommended_next_source = str(row.get("recommended_next_source", ""))
        source_gate = str(row.get("source_gate", ""))
        official_source_context = str(row.get("official_source_context", ""))
        existing_dataset_context = str(row.get("existing_dataset_context", ""))
        identity_resolution_context = str(row.get("identity_resolution_context", ""))
        clearance_evidence = str(row.get("clearance_evidence_required", ""))
        recommended_action = str(row.get("recommended_next_action", ""))
        official_isin = str(row.get("official_isin", ""))
        if review_decision not in COLLISION_REVIEW_DECISIONS:
            invalid_fields.append("review_decision")
        if review_bucket not in COLLISION_REVIEW_BUCKETS:
            invalid_fields.append("review_bucket")
        if review_priority not in COLLISION_REVIEW_PRIORITIES:
            invalid_fields.append("review_priority")
        if identity_resolution_queue not in COLLISION_IDENTITY_RESOLUTION_QUEUES:
            invalid_fields.append("identity_resolution_queue")
        if COLLISION_BUCKET_TO_IDENTITY_QUEUE.get(review_bucket) != identity_resolution_queue:
            invalid_fields.append("identity_resolution_queue")
        if COLLISION_QUEUE_TO_PAIR_REVIEW_STRATEGY.get(identity_resolution_queue) != review_strategy:
            invalid_fields.append("review_strategy")
        if COLLISION_QUEUE_TO_EVIDENCE_REQUIRED.get(identity_resolution_queue) != verification_evidence_required:
            invalid_fields.append("verification_evidence_required")
        exchange_context = collision_exchange_context(
            str(row.get("target_exchange", "")),
            str(row.get("existing_exchanges", "")),
        )
        if recommended_next_source != collision_recommended_next_source(identity_resolution_queue, exchange_context):
            invalid_fields.append("recommended_next_source")
        if source_gate != collision_source_gate(identity_resolution_queue):
            invalid_fields.append("source_gate")
        if official_source_context != collision_official_source_context(row):
            invalid_fields.append("official_source_context")
        if existing_dataset_context != collision_existing_dataset_context(row):
            invalid_fields.append("existing_dataset_context")
        if identity_resolution_context != collision_identity_resolution_context(row):
            invalid_fields.append("identity_resolution_context")
        if "ticker_reused_on_other_exchange" not in collision_risk_flags.split("|"):
            invalid_fields.append("collision_risk_flags")
        if not clearance_evidence or clearance_evidence == "none":
            invalid_fields.append("clearance_evidence_required")
        if not recommended_action:
            invalid_fields.append("recommended_next_action")
        if review_bucket == "hold_symbol_only_collision_needs_non_symbol_identity":
            if review_decision != "symbol_collision_requires_non_symbol_identity_source":
                invalid_fields.append("review_decision")
            if official_isin:
                invalid_fields.append("official_isin")
            if "missing_official_isin" not in collision_risk_flags.split("|"):
                invalid_fields.append("collision_risk_flags")
            if clearance_evidence != "official_non_symbol_identifier_or_keep_absent":
                invalid_fields.append("clearance_evidence_required")
            if "keep_absent" not in recommended_action:
                unsafe_symbol_only_rows.append(
                    {
                        "row_index": index,
                        "target_listing_key": row.get("target_listing_key", ""),
                        "recommended_next_action": recommended_action,
                    }
                )
        if review_bucket in {
            "same_isin_exact_name_cross_listing_candidate",
            "same_isin_cross_listing_needs_name_or_scope_review",
            "distinct_official_isin_new_listing_candidate",
        } and "official_isin" not in identity_evidence.split("|"):
            invalid_fields.append("identity_evidence")
        if (
            review_bucket == "resolve_asset_type_conflict_before_identity_review"
            and "asset_type_conflict" not in identity_evidence.split("|")
        ):
            invalid_fields.append("identity_evidence")
        if review_bucket == "same_isin_exact_name_cross_listing_candidate":
            required_identity = {"official_isin", "same_isin_existing_listing", "exact_name_match"}
            if not required_identity.issubset(set(identity_evidence.split("|"))):
                invalid_fields.append("identity_evidence")
        if missing_keys or invalid_fields:
            row_gaps.append(
                {
                    "row_index": index,
                    "target_listing_key": row.get("target_listing_key", ""),
                    "missing_keys": missing_keys,
                    "invalid_fields": invalid_fields,
                    "available_keys": sorted(row)[:25],
                }
            )
        counters["decision_totals"][review_decision] += 1
        counters["review_bucket_totals"][review_bucket] += 1
        counters["review_priority_totals"][review_priority] += 1
        counters["identity_resolution_queue_totals"][identity_resolution_queue] += 1
        for flag in collision_risk_flags.split("|"):
            if flag:
                counters["collision_risk_flag_totals"][flag] += 1
        counters["clearance_evidence_totals"][clearance_evidence] += 1
        counters["exchange_totals"][str(row.get("target_exchange", ""))] += 1
        counters["official_asset_type_totals"][str(row.get("official_asset_type", "") or "missing")] += 1
        counters["asset_type_mismatch_totals"][str(row.get("asset_type_mismatch", ""))] += 1
        counters["official_source_totals"][str(row.get("official_source_key", "") or "missing")] += 1
    summary_mismatches = {
        key: compare_counter_to_reported(counter, summary.get(key))
        for key, counter in counters.items()
    }
    queue_exchange_counter: dict[str, Counter[str]] = defaultdict(Counter)
    for row in rows:
        if isinstance(row, dict):
            queue_exchange_counter[str(row.get("identity_resolution_queue", ""))][str(row.get("target_exchange", ""))] += 1
    identity_resolution_exchange_mismatches = {
        queue: compare_counter_to_reported(counter, summary.get("identity_resolution_exchange_totals", {}).get(queue) if isinstance(summary.get("identity_resolution_exchange_totals"), dict) else None)
        for queue, counter in queue_exchange_counter.items()
    }
    identity_resolution_exchange_mismatches = {
        queue: mismatch for queue, mismatch in identity_resolution_exchange_mismatches.items() if mismatch
    }
    queue_asset_type_counter: dict[str, Counter[str]] = defaultdict(Counter)
    queue_risk_flag_counter: dict[str, Counter[str]] = defaultdict(Counter)
    queue_official_source_counter: dict[str, Counter[str]] = defaultdict(Counter)
    queue_existing_exchange_pair_counter: dict[str, Counter[str]] = defaultdict(Counter)
    queue_review_strategy_counter: dict[str, Counter[str]] = defaultdict(Counter)
    queue_evidence_required_counter: dict[str, Counter[str]] = defaultdict(Counter)
    queue_identity_evidence_counter: dict[str, Counter[str]] = defaultdict(Counter)
    for row in rows:
        if isinstance(row, dict):
            queue = str(row.get("identity_resolution_queue", ""))
            queue_review_strategy_counter[queue][str(row.get("review_strategy", ""))] += 1
            queue_evidence_required_counter[queue][str(row.get("verification_evidence_required", ""))] += 1
            for evidence in str(row.get("identity_evidence", "")).split("|"):
                if evidence:
                    queue_identity_evidence_counter[queue][evidence] += 1
            queue_asset_type_counter[queue][
                str(row.get("official_asset_type", "") or "missing")
            ] += 1
            queue_official_source_counter[queue][str(row.get("official_source_key", "") or "missing")] += 1
            target_exchange = str(row.get("target_exchange", ""))
            for existing_exchange in str(row.get("existing_exchanges", "")).split("|"):
                if existing_exchange:
                    queue_existing_exchange_pair_counter[queue][f"{target_exchange}::{existing_exchange}"] += 1
            for flag in str(row.get("collision_risk_flags", "")).split("|"):
                if flag:
                    queue_risk_flag_counter[queue][flag] += 1
    identity_resolution_asset_type_mismatches = {
        queue: compare_counter_to_reported(
            counter,
            summary.get("identity_resolution_asset_type_totals", {}).get(queue)
            if isinstance(summary.get("identity_resolution_asset_type_totals"), dict)
            else None,
        )
        for queue, counter in queue_asset_type_counter.items()
    }
    identity_resolution_asset_type_mismatches = {
        queue: mismatch for queue, mismatch in identity_resolution_asset_type_mismatches.items() if mismatch
    }
    identity_resolution_risk_flag_mismatches = {
        queue: compare_counter_to_reported(
            counter,
            summary.get("identity_resolution_risk_flag_totals", {}).get(queue)
            if isinstance(summary.get("identity_resolution_risk_flag_totals"), dict)
            else None,
        )
        for queue, counter in queue_risk_flag_counter.items()
    }
    identity_resolution_risk_flag_mismatches = {
        queue: mismatch for queue, mismatch in identity_resolution_risk_flag_mismatches.items() if mismatch
    }
    identity_resolution_official_source_mismatches = {
        queue: compare_counter_to_reported(
            counter,
            summary.get("identity_resolution_official_source_totals", {}).get(queue)
            if isinstance(summary.get("identity_resolution_official_source_totals"), dict)
            else None,
        )
        for queue, counter in queue_official_source_counter.items()
    }
    identity_resolution_official_source_mismatches = {
        queue: mismatch for queue, mismatch in identity_resolution_official_source_mismatches.items() if mismatch
    }
    identity_resolution_existing_exchange_pair_mismatches = {
        queue: compare_counter_to_reported(
            counter,
            summary.get("identity_resolution_existing_exchange_pair_totals", {}).get(queue)
            if isinstance(summary.get("identity_resolution_existing_exchange_pair_totals"), dict)
            else None,
        )
        for queue, counter in queue_existing_exchange_pair_counter.items()
    }
    identity_resolution_existing_exchange_pair_mismatches = {
        queue: mismatch
        for queue, mismatch in identity_resolution_existing_exchange_pair_mismatches.items()
        if mismatch
    }
    pair_strategy_counter: dict[str, Counter[str]] = defaultdict(Counter)
    for queue, pair_counter in queue_existing_exchange_pair_counter.items():
        strategy = COLLISION_QUEUE_TO_PAIR_REVIEW_STRATEGY.get(queue, "unknown_strategy")
        pair_strategy_counter[queue][strategy] = sum(pair_counter.values())
    identity_resolution_pair_review_strategy_mismatches = {
        queue: compare_counter_to_reported(
            counter,
            summary.get("identity_resolution_pair_review_strategy_totals", {}).get(queue)
            if isinstance(summary.get("identity_resolution_pair_review_strategy_totals"), dict)
            else None,
        )
        for queue, counter in pair_strategy_counter.items()
    }
    identity_resolution_review_strategy_mismatches = {
        queue: compare_counter_to_reported(
            counter,
            summary.get("identity_resolution_review_strategy_totals", {}).get(queue)
            if isinstance(summary.get("identity_resolution_review_strategy_totals"), dict)
            else None,
        )
        for queue, counter in queue_review_strategy_counter.items()
    }
    identity_resolution_review_strategy_mismatches = {
        queue: mismatch
        for queue, mismatch in identity_resolution_review_strategy_mismatches.items()
        if mismatch
    }
    identity_resolution_evidence_required_mismatches = {
        queue: compare_counter_to_reported(
            counter,
            summary.get("identity_resolution_evidence_required_totals", {}).get(queue)
            if isinstance(summary.get("identity_resolution_evidence_required_totals"), dict)
            else None,
        )
        for queue, counter in queue_evidence_required_counter.items()
    }
    identity_resolution_evidence_required_mismatches = {
        queue: mismatch
        for queue, mismatch in identity_resolution_evidence_required_mismatches.items()
        if mismatch
    }
    identity_resolution_identity_evidence_mismatches = {
        queue: compare_counter_to_reported(
            counter,
            summary.get("identity_resolution_identity_evidence_totals", {}).get(queue)
            if isinstance(summary.get("identity_resolution_identity_evidence_totals"), dict)
            else None,
        )
        for queue, counter in queue_identity_evidence_counter.items()
    }
    identity_resolution_identity_evidence_mismatches = {
        queue: mismatch
        for queue, mismatch in identity_resolution_identity_evidence_mismatches.items()
        if mismatch
    }
    identity_resolution_pair_review_strategy_mismatches = {
        queue: mismatch
        for queue, mismatch in identity_resolution_pair_review_strategy_mismatches.items()
        if mismatch
    }
    top_pair_review_batches = summary.get("top_identity_resolution_pair_review_batches")
    top_pair_review_batch_gaps: list[dict[str, Any]] = []
    if not isinstance(top_pair_review_batches, list) or not top_pair_review_batches:
        top_pair_review_batch_gaps.append(
            {"field": "top_identity_resolution_pair_review_batches", "reason": "expected_ranked_pair_batches"}
        )
    elif isinstance(top_pair_review_batches, list):
        for index, batch in enumerate(top_pair_review_batches):
            if not isinstance(batch, dict):
                top_pair_review_batch_gaps.append({"row_index": index, "reason": "expected_object"})
                continue
            queue = str(batch.get("identity_resolution_queue", ""))
            strategy = str(batch.get("review_strategy", ""))
            exchange_pair = str(batch.get("exchange_pair", ""))
            rows_value = batch.get("rows")
            missing_keys = [
                key
                for key in (
                    "identity_resolution_queue",
                    "exchange_pair",
                    "rows",
                    "review_strategy",
                    "evidence_required",
                    "recommended_next_source",
                    "source_gate",
                )
                if key not in batch
            ]
            invalid_fields = []
            if queue not in COLLISION_IDENTITY_RESOLUTION_QUEUES:
                invalid_fields.append("identity_resolution_queue")
            if "::" not in exchange_pair:
                invalid_fields.append("exchange_pair")
            if not isinstance(rows_value, int) or rows_value <= 0:
                invalid_fields.append("rows")
            if strategy not in COLLISION_PAIR_REVIEW_STRATEGIES:
                invalid_fields.append("review_strategy")
            if COLLISION_QUEUE_TO_PAIR_REVIEW_STRATEGY.get(queue) != strategy:
                invalid_fields.append("review_strategy")
            if not batch.get("evidence_required") or batch.get("evidence_required") in {"none", "ticker_match_only"}:
                invalid_fields.append("evidence_required")
            if not batch.get("recommended_next_source"):
                invalid_fields.append("recommended_next_source")
            if not batch.get("source_gate"):
                invalid_fields.append("source_gate")
            if batch.get("recommended_next_source") != collision_recommended_next_source(queue, exchange_pair):
                invalid_fields.append("recommended_next_source")
            if batch.get("source_gate") != collision_source_gate(queue):
                invalid_fields.append("source_gate")
            if missing_keys or invalid_fields:
                top_pair_review_batch_gaps.append(
                    {
                        "row_index": index,
                        "missing_keys": missing_keys,
                        "invalid_fields": invalid_fields,
                    }
                )
    same_isin_exact_name_rows = [
        row
        for row in rows
        if isinstance(row, dict)
        and row.get("identity_resolution_queue") == "review_cross_listing_same_isin_exact_name"
    ]
    same_isin_exact_name_scope_review_rows_mismatch = {}
    if summary.get("same_isin_exact_name_scope_review_rows") != len(same_isin_exact_name_rows):
        same_isin_exact_name_scope_review_rows_mismatch = {
            "reported": summary.get("same_isin_exact_name_scope_review_rows"),
            "actual": len(same_isin_exact_name_rows),
        }
    top_same_isin_exact_name_scope_batches = summary.get("top_same_isin_exact_name_scope_review_batches")
    top_same_isin_exact_name_scope_batch_gaps: list[dict[str, Any]] = []
    if same_isin_exact_name_rows and (
        not isinstance(top_same_isin_exact_name_scope_batches, list)
        or not top_same_isin_exact_name_scope_batches
    ):
        top_same_isin_exact_name_scope_batch_gaps.append(
            {"field": "top_same_isin_exact_name_scope_review_batches", "reason": "expected_ranked_scope_batches"}
        )
    elif isinstance(top_same_isin_exact_name_scope_batches, list):
        reported_total = 0
        for index, batch in enumerate(top_same_isin_exact_name_scope_batches):
            if not isinstance(batch, dict):
                top_same_isin_exact_name_scope_batch_gaps.append(
                    {"row_index": index, "reason": "expected_object"}
                )
                continue
            missing_keys = [
                key
                for key in (
                    "exchange_pair",
                    "official_source_key",
                    "official_asset_type",
                    "clearance_evidence_required",
                    "rows",
                    "review_strategy",
                    "evidence_required",
                    "recommended_next_source",
                    "source_gate",
                )
                if key not in batch
            ]
            invalid_fields = []
            exchange_pair = str(batch.get("exchange_pair", ""))
            rows_value = batch.get("rows")
            if "::" not in exchange_pair:
                invalid_fields.append("exchange_pair")
            if not isinstance(rows_value, int) or rows_value <= 0:
                invalid_fields.append("rows")
            else:
                reported_total += rows_value
            if batch.get("review_strategy") != COLLISION_QUEUE_TO_PAIR_REVIEW_STRATEGY["review_cross_listing_same_isin_exact_name"]:
                invalid_fields.append("review_strategy")
            if batch.get("evidence_required") != COLLISION_QUEUE_TO_EVIDENCE_REQUIRED["review_cross_listing_same_isin_exact_name"]:
                invalid_fields.append("evidence_required")
            if batch.get("clearance_evidence_required") != "official_target_exchange_listing_status_mic_name_instrument_type":
                invalid_fields.append("clearance_evidence_required")
            if batch.get("recommended_next_source") != collision_recommended_next_source(
                "review_cross_listing_same_isin_exact_name",
                exchange_pair,
            ):
                invalid_fields.append("recommended_next_source")
            if batch.get("source_gate") != collision_source_gate("review_cross_listing_same_isin_exact_name"):
                invalid_fields.append("source_gate")
            if missing_keys or invalid_fields:
                top_same_isin_exact_name_scope_batch_gaps.append(
                    {
                        "row_index": index,
                        "missing_keys": missing_keys,
                        "invalid_fields": invalid_fields,
                    }
                )
        if reported_total <= 0 or reported_total > len(same_isin_exact_name_rows):
            top_same_isin_exact_name_scope_batch_gaps.append(
                {
                    "field": "top_same_isin_exact_name_scope_review_batches",
                    "reported_total": reported_total,
                    "allowed_max": len(same_isin_exact_name_rows),
                }
            )
    summary_mismatches = {key: value for key, value in summary_mismatches.items() if value}
    policy = summary.get("policy", {})
    missing_policy_keys = [
        key
        for key in COLLISION_REQUIRED_POLICY_KEYS
        if not isinstance(policy, dict) or not policy.get(key)
    ]
    missing_required_buckets = [
        bucket
        for bucket in (
            "hold_symbol_only_collision_needs_non_symbol_identity",
            "same_isin_cross_listing_needs_name_or_scope_review",
            "distinct_official_isin_new_listing_candidate",
        )
        if bucket not in counters["review_bucket_totals"]
    ]
    return {
        "passed": (
            bool(rows)
            and not missing_meta_keys
            and not invalid_generated_at
            and not source_file_mismatches
            and not unexpected_source_files
            and not meta_policy_mismatch
            and not missing_summary_keys
            and not row_count_mismatch
            and not row_gaps
            and not unsafe_symbol_only_rows
            and not summary_mismatches
            and not identity_resolution_exchange_mismatches
            and not identity_resolution_asset_type_mismatches
            and not identity_resolution_risk_flag_mismatches
            and not identity_resolution_official_source_mismatches
            and not identity_resolution_existing_exchange_pair_mismatches
            and not identity_resolution_pair_review_strategy_mismatches
            and not identity_resolution_review_strategy_mismatches
            and not identity_resolution_evidence_required_mismatches
            and not identity_resolution_identity_evidence_mismatches
            and not top_pair_review_batch_gaps
            and not same_isin_exact_name_scope_review_rows_mismatch
            and not top_same_isin_exact_name_scope_batch_gaps
            and not missing_policy_keys
            and not missing_required_buckets
        ),
        "missing_meta_keys": missing_meta_keys,
        "invalid_generated_at": invalid_generated_at,
        "source_file_mismatches": source_file_mismatches,
        "unexpected_source_files": unexpected_source_files,
        "meta_policy_mismatch": meta_policy_mismatch,
        "rows": len(rows),
        "missing_summary_keys": missing_summary_keys,
        "row_count_mismatch": row_count_mismatch,
        "row_gaps": row_gaps[:20],
        "unsafe_symbol_only_rows": unsafe_symbol_only_rows[:20],
        "summary_mismatches": summary_mismatches,
        "identity_resolution_exchange_mismatches": identity_resolution_exchange_mismatches,
        "identity_resolution_asset_type_mismatches": identity_resolution_asset_type_mismatches,
        "identity_resolution_risk_flag_mismatches": identity_resolution_risk_flag_mismatches,
        "identity_resolution_official_source_mismatches": identity_resolution_official_source_mismatches,
        "identity_resolution_existing_exchange_pair_mismatches": identity_resolution_existing_exchange_pair_mismatches,
        "identity_resolution_pair_review_strategy_mismatches": identity_resolution_pair_review_strategy_mismatches,
        "identity_resolution_review_strategy_mismatches": identity_resolution_review_strategy_mismatches,
        "identity_resolution_evidence_required_mismatches": identity_resolution_evidence_required_mismatches,
        "identity_resolution_identity_evidence_mismatches": identity_resolution_identity_evidence_mismatches,
        "top_pair_review_batch_gaps": top_pair_review_batch_gaps[:20],
        "same_isin_exact_name_scope_review_rows_mismatch": same_isin_exact_name_scope_review_rows_mismatch,
        "top_same_isin_exact_name_scope_batch_gaps": top_same_isin_exact_name_scope_batch_gaps[:20],
        "missing_policy_keys": missing_policy_keys,
        "missing_required_buckets": missing_required_buckets,
        "required_row_keys": list(REQUIRED_COLLISION_ROW_KEYS),
        "accepted_review_decisions": sorted(COLLISION_REVIEW_DECISIONS),
        "accepted_review_buckets": sorted(COLLISION_REVIEW_BUCKETS),
    }


def evaluate_otc_scope_gate(otc_report: dict[str, Any]) -> dict[str, Any]:
    summary = otc_report.get("summary", {})
    rows = otc_report.get("rows", [])
    if not isinstance(summary, dict):
        summary = {}
    if not isinstance(rows, list):
        rows = []
    missing_summary_keys = [key for key in REQUIRED_OTC_SCOPE_SUMMARY_KEYS if key not in summary]
    row_count_mismatch = {
        "reported": summary.get("rows", 0),
        "actual": len(rows),
    } if summary.get("rows", 0) != len(rows) else {}
    counters: dict[str, Counter[str]] = {
        "scope_decision_totals": Counter(),
        "instrument_scope_totals": Counter(),
        "scope_reason_totals": Counter(),
        "quality_status_totals": Counter(),
        "review_bucket_totals": Counter(),
        "review_priority_totals": Counter(),
        "review_strategy_totals": Counter(),
        "verification_evidence_required_totals": Counter(),
        "scope_apply_eligibility_totals": Counter(),
        "metadata_enrichment_gate_totals": Counter(),
        "source_gap_field_totals": Counter(),
        "source_gap_class_totals": Counter(),
        "source_of_truth_outcome_totals": Counter(),
    }
    row_gaps: list[dict[str, Any]] = []
    unsafe_enrichment_rows: list[dict[str, Any]] = []
    review_bucket_asset_type_counter: dict[str, Counter[str]] = defaultdict(Counter)
    review_bucket_metadata_gate_counter: dict[str, Counter[str]] = defaultdict(Counter)
    top_scope_batch_counter: Counter[tuple[str, str, str, str, str]] = Counter()
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            row_gaps.append({"row_index": index, "reason": "otc_scope_row_is_not_object"})
            continue
        missing_keys = [key for key in REQUIRED_OTC_SCOPE_ROW_KEYS if not row.get(key)]
        invalid_fields: list[str] = []
        listing_key = str(row.get("listing_key", ""))
        exchange = str(row.get("exchange", ""))
        scope_decision = str(row.get("scope_decision", ""))
        review_bucket = str(row.get("review_bucket", ""))
        review_priority = str(row.get("review_priority", ""))
        scope_apply_eligibility = str(row.get("scope_apply_eligibility", ""))
        metadata_gate = str(row.get("metadata_enrichment_gate", ""))
        review_strategy = str(row.get("review_strategy", ""))
        verification_evidence_required = str(row.get("verification_evidence_required", ""))
        recommended_action = str(row.get("recommended_action", ""))
        source_gap_field = str(row.get("source_gap_field", ""))
        source_gap_class = str(row.get("source_gap_class", ""))
        source_of_truth_outcome = str(row.get("source_of_truth_outcome", ""))
        otc_review_decision_status = str(row.get("otc_review_decision_status", ""))
        quality_status = str(row.get("quality_status", ""))
        source_gap_context = str(row.get("source_gap_context", ""))
        otc_review_decision_context = str(row.get("otc_review_decision_context", ""))
        scope_review_context = str(row.get("scope_review_context", ""))
        if not listing_key.startswith("OTC::") or exchange != "OTC":
            invalid_fields.append("listing_key")
        if scope_decision not in OTC_SCOPE_DECISIONS:
            invalid_fields.append("scope_decision")
        if review_bucket not in OTC_REVIEW_BUCKETS:
            invalid_fields.append("review_bucket")
        if review_priority not in OTC_REVIEW_PRIORITIES:
            invalid_fields.append("review_priority")
        if review_strategy != otc_scope_review_strategy(review_bucket):
            invalid_fields.append("review_strategy")
        if verification_evidence_required != otc_scope_verification_evidence_required(review_bucket):
            invalid_fields.append("verification_evidence_required")
        if row.get("recommended_next_source") != otc_scope_recommended_next_source(review_bucket):
            invalid_fields.append("recommended_next_source")
        if row.get("source_gate") != otc_scope_source_gate(review_bucket):
            invalid_fields.append("source_gate")
        if source_gap_context != otc_scope_source_gap_context(row):
            invalid_fields.append("source_gap_context")
        if otc_review_decision_context != otc_scope_review_decision_context(row):
            invalid_fields.append("otc_review_decision_context")
        if scope_review_context != otc_scope_review_context(row):
            invalid_fields.append("scope_review_context")
        if scope_decision in {
            "core_exclusion_candidate_requires_review",
            "unexpected_otc_core_scope_review_required",
        }:
            if review_priority != "P1":
                invalid_fields.append("review_priority")
            if not scope_apply_eligibility.startswith("blocked_until_"):
                invalid_fields.append("scope_apply_eligibility")
            if metadata_gate != "scope_decision_required_before_any_metadata_enrichment":
                invalid_fields.append("metadata_enrichment_gate")
        if review_bucket in {"documented_otc_sector_source_gap", "documented_otc_category_source_gap", "documented_otc_source_gap"}:
            if not source_gap_field or not source_gap_class:
                invalid_fields.append("source_gap_field")
            if source_of_truth_outcome != "accepted_source_gap":
                invalid_fields.append("source_of_truth_outcome")
            if "keep_blank" not in metadata_gate and "required_keep_blank" not in metadata_gate:
                invalid_fields.append("metadata_enrichment_gate")
            if recommended_action != "leave_blank_as_documented_source_gap_until_reviewed_source":
                unsafe_enrichment_rows.append(
                    {
                        "row_index": index,
                        "listing_key": listing_key,
                        "recommended_action": recommended_action,
                        "metadata_enrichment_gate": metadata_gate,
                    }
                )
        if review_bucket == "official_name_mismatch_review_first":
            if metadata_gate != "otc_name_mismatch_review_required_before_name_or_metadata_changes":
                invalid_fields.append("metadata_enrichment_gate")
            if recommended_action != "use_otc_name_mismatch_review_before_name_changes":
                invalid_fields.append("recommended_action")
            if otc_review_decision_status not in {
                "pending_otc_name_mismatch_review",
                "reviewed_name_mismatch_decision_present",
            }:
                invalid_fields.append("otc_review_decision_status")
        elif otc_review_decision_status != "not_applicable":
            invalid_fields.append("otc_review_decision_status")
        if review_bucket == "otc_quality_source_gap_review":
            if quality_status != "source_gap":
                invalid_fields.append("quality_status")
            if source_gap_field or source_gap_class:
                invalid_fields.append("source_gap_field")
            if metadata_gate != "source_gap_review_required_before_enrichment":
                invalid_fields.append("metadata_enrichment_gate")
            if recommended_action != "keep_extended_scope":
                unsafe_enrichment_rows.append(
                    {
                        "row_index": index,
                        "listing_key": listing_key,
                        "recommended_action": recommended_action,
                        "metadata_enrichment_gate": metadata_gate,
                    }
                )
        if review_bucket == "otc_quality_warn_review":
            if quality_status != "warn":
                invalid_fields.append("quality_status")
            if metadata_gate != "entry_quality_warn_review_required_before_enrichment":
                invalid_fields.append("metadata_enrichment_gate")
            if recommended_action != "keep_extended_scope":
                invalid_fields.append("recommended_action")
        if review_bucket == "clean_extended_otc_listing":
            if quality_status != "pass":
                invalid_fields.append("quality_status")
            if metadata_gate != "no_metadata_enrichment_needed":
                invalid_fields.append("metadata_enrichment_gate")
            if recommended_action != "keep_extended_scope":
                invalid_fields.append("recommended_action")
        if missing_keys or invalid_fields:
            row_gaps.append(
                {
                    "row_index": index,
                    "listing_key": listing_key,
                    "missing_keys": missing_keys,
                    "invalid_fields": invalid_fields,
                    "available_keys": sorted(row)[:25],
                }
            )
        counters["scope_decision_totals"][scope_decision] += 1
        counters["instrument_scope_totals"][str(row.get("instrument_scope", ""))] += 1
        counters["scope_reason_totals"][str(row.get("scope_reason", ""))] += 1
        counters["quality_status_totals"][str(row.get("quality_status", ""))] += 1
        counters["review_bucket_totals"][review_bucket] += 1
        review_bucket_asset_type_counter[review_bucket][str(row.get("asset_type", "") or "unknown")] += 1
        review_bucket_metadata_gate_counter[review_bucket][metadata_gate] += 1
        counters["review_priority_totals"][review_priority] += 1
        counters["review_strategy_totals"][otc_scope_review_strategy(review_bucket)] += 1
        counters["verification_evidence_required_totals"][
            otc_scope_verification_evidence_required(review_bucket)
        ] += 1
        counters["scope_apply_eligibility_totals"][scope_apply_eligibility] += 1
        counters["metadata_enrichment_gate_totals"][metadata_gate] += 1
        top_scope_batch_counter[
            (
                review_priority,
                review_bucket,
                str(row.get("asset_type", "") or "unknown"),
                quality_status or "blank",
                metadata_gate,
            )
        ] += 1
        for field in source_gap_field.split("|"):
            if field:
                counters["source_gap_field_totals"][field] += 1
        for gap_class in source_gap_class.split("|"):
            if gap_class:
                counters["source_gap_class_totals"][gap_class] += 1
        for outcome in source_of_truth_outcome.split("|"):
            if outcome:
                counters["source_of_truth_outcome_totals"][outcome] += 1
    summary_mismatches = {
        key: compare_counter_to_reported(counter, summary.get(key))
        for key, counter in counters.items()
    }
    summary_mismatches = {key: value for key, value in summary_mismatches.items() if value}
    review_bucket_asset_type_mismatches = {
        bucket: compare_counter_to_reported(
            counter,
            summary.get("review_bucket_asset_type_totals", {}).get(bucket)
            if isinstance(summary.get("review_bucket_asset_type_totals"), dict)
            else None,
        )
        for bucket, counter in review_bucket_asset_type_counter.items()
    }
    review_bucket_asset_type_mismatches = {
        bucket: mismatch for bucket, mismatch in review_bucket_asset_type_mismatches.items() if mismatch
    }
    review_bucket_metadata_gate_mismatches = {
        bucket: compare_counter_to_reported(
            counter,
            summary.get("review_bucket_metadata_gate_totals", {}).get(bucket)
            if isinstance(summary.get("review_bucket_metadata_gate_totals"), dict)
            else None,
        )
        for bucket, counter in review_bucket_metadata_gate_counter.items()
    }
    review_bucket_metadata_gate_mismatches = {
        bucket: mismatch for bucket, mismatch in review_bucket_metadata_gate_mismatches.items() if mismatch
    }
    top_scope_batch_gaps: list[dict[str, Any]] = []
    top_scope_batches = summary.get("top_otc_scope_review_batches")
    if not isinstance(top_scope_batches, list) or not top_scope_batches:
        top_scope_batch_gaps.append(
            {"field": "top_otc_scope_review_batches", "reason": "expected_ranked_review_batches"}
        )
    else:
        expected_top_counts = dict(top_scope_batch_counter)
        for index, batch in enumerate(top_scope_batches):
            if not isinstance(batch, dict):
                top_scope_batch_gaps.append({"row_index": index, "reason": "expected_object"})
                continue
            bucket = str(batch.get("review_bucket", ""))
            batch_key = (
                str(batch.get("review_priority", "")),
                bucket,
                str(batch.get("asset_type", "")),
                str(batch.get("quality_status", "")),
                str(batch.get("metadata_enrichment_gate", "")),
            )
            if expected_top_counts.get(batch_key) != batch.get("rows"):
                top_scope_batch_gaps.append(
                    {
                        "row_index": index,
                        "reported": batch.get("rows"),
                        "expected": expected_top_counts.get(batch_key),
                    }
                )
            expected_strategy = otc_scope_review_strategy(bucket)
            if batch.get("review_strategy") != expected_strategy:
                top_scope_batch_gaps.append(
                    {
                        "row_index": index,
                        "field": "review_strategy",
                        "reported": batch.get("review_strategy"),
                        "expected": expected_strategy,
                    }
                )
            expected_evidence = otc_scope_verification_evidence_required(bucket)
            if batch.get("verification_evidence_required") != expected_evidence:
                top_scope_batch_gaps.append(
                    {
                        "row_index": index,
                        "field": "verification_evidence_required",
                        "reported": batch.get("verification_evidence_required"),
                        "expected": expected_evidence,
                    }
                )
            missing_instruction_fields = [
                key for key in ("recommended_next_source", "source_gate") if not batch.get(key)
            ]
            if missing_instruction_fields:
                top_scope_batch_gaps.append(
                    {
                        "row_index": index,
                        "field": "top_otc_scope_review_batches",
                        "missing_keys": missing_instruction_fields,
                    }
                )
            expected_next_source = otc_scope_recommended_next_source(bucket)
            if batch.get("recommended_next_source") != expected_next_source:
                top_scope_batch_gaps.append(
                    {
                        "row_index": index,
                        "field": "recommended_next_source",
                        "reported": batch.get("recommended_next_source"),
                        "expected": expected_next_source,
                    }
                )
            expected_source_gate = otc_scope_source_gate(bucket)
            if batch.get("source_gate") != expected_source_gate:
                top_scope_batch_gaps.append(
                    {
                        "row_index": index,
                        "field": "source_gate",
                        "reported": batch.get("source_gate"),
                        "expected": expected_source_gate,
                    }
                )
    policy = summary.get("policy", {})
    missing_policy_keys = [
        key
        for key in OTC_REQUIRED_POLICY_KEYS
        if not isinstance(policy, dict) or not policy.get(key)
    ]
    drop_override_still_present = int(summary.get("drop_override_rows_still_present", -1))
    active_name_mismatch_rows = counters["review_bucket_totals"].get("official_name_mismatch_review_first", 0)
    reviewed_active_name_mismatch_rows = int(summary.get("otc_review_decision_active_name_mismatch_rows", -1))
    unreviewed_active_name_mismatch_rows = int(summary.get("otc_name_mismatch_unreviewed_active_rows", -1))
    suppressed_current_listing_rows = int(summary.get("otc_review_decision_current_listing_suppressed_rows", -1))
    not_current_scope_rows = int(summary.get("otc_review_decision_not_current_scope_rows", -1))
    stale_review_decision_rows = int(summary.get("otc_review_decision_stale_rows", -1))
    review_decision_rows = int(summary.get("otc_review_decision_rows", -1))
    review_decision_resolution_totals = summary.get("otc_review_decision_resolution_totals", {})
    review_decision_balance = {}
    if reviewed_active_name_mismatch_rows + unreviewed_active_name_mismatch_rows != active_name_mismatch_rows:
        review_decision_balance["active_name_mismatch"] = {
            "reviewed_active": reviewed_active_name_mismatch_rows,
            "unreviewed_active": unreviewed_active_name_mismatch_rows,
            "active_name_mismatch_rows": active_name_mismatch_rows,
        }
    if reviewed_active_name_mismatch_rows + stale_review_decision_rows != review_decision_rows:
        review_decision_balance["review_decisions"] = {
            "reviewed_active": reviewed_active_name_mismatch_rows,
            "stale": stale_review_decision_rows,
            "review_decision_rows": review_decision_rows,
        }
    if suppressed_current_listing_rows + not_current_scope_rows != stale_review_decision_rows:
        review_decision_balance["stale_resolution"] = {
            "suppressed_current_listing": suppressed_current_listing_rows,
            "not_current_scope": not_current_scope_rows,
            "stale": stale_review_decision_rows,
        }
    expected_resolution_totals = {
        "pending_active_name_mismatch_review": unreviewed_active_name_mismatch_rows,
        "reviewed_decision_covers_active_name_mismatch": reviewed_active_name_mismatch_rows,
        "reviewed_decision_suppresses_current_listing_warning": suppressed_current_listing_rows,
        "reviewed_decision_not_in_current_otc_scope": not_current_scope_rows,
    }
    expected_resolution_totals = {
        key: value for key, value in expected_resolution_totals.items() if isinstance(value, int) and value > 0
    }
    if review_decision_resolution_totals != expected_resolution_totals:
        review_decision_balance["resolution_totals"] = {
            "reported": review_decision_resolution_totals,
            "expected": expected_resolution_totals,
        }
    stale_examples = summary.get("otc_review_decision_stale_examples")
    suppressed_examples = summary.get("otc_review_decision_current_listing_suppressed_examples")
    not_current_examples = summary.get("otc_review_decision_not_current_scope_examples")
    stale_example_gaps = []
    for field, examples, count in (
        ("otc_review_decision_stale_examples", stale_examples, stale_review_decision_rows),
        (
            "otc_review_decision_current_listing_suppressed_examples",
            suppressed_examples,
            suppressed_current_listing_rows,
        ),
        ("otc_review_decision_not_current_scope_examples", not_current_examples, not_current_scope_rows),
    ):
        if not isinstance(examples, list):
            stale_example_gaps.append({"field": field, "reason": "expected_list"})
        elif count > 0:
            for example in examples[:20]:
                if not isinstance(example, str) or not example.startswith("OTC::"):
                    stale_example_gaps.append({"field": field, "example": example})
    core_exclusion_rows = [
        row
        for row in rows
        if isinstance(row, dict)
        and (
            row.get("scope_decision") == "core_exclusion_candidate_requires_review"
            or row.get("review_bucket") == "core_exclusion_candidate_scope_review"
        )
    ]
    core_exclusion_summary_gaps: list[dict[str, Any]] = []
    if summary.get("otc_core_exclusion_candidate_rows") != len(core_exclusion_rows):
        core_exclusion_summary_gaps.append(
            {
                "field": "otc_core_exclusion_candidate_rows",
                "reported": summary.get("otc_core_exclusion_candidate_rows"),
                "expected": len(core_exclusion_rows),
            }
        )
    core_asset_mismatch = compare_counter_to_reported(
        Counter(str(row.get("asset_type", "") or "unknown") for row in core_exclusion_rows),
        summary.get("otc_core_exclusion_candidate_asset_type_totals"),
    )
    if core_asset_mismatch:
        core_exclusion_summary_gaps.append(
            {"field": "otc_core_exclusion_candidate_asset_type_totals", "mismatch": core_asset_mismatch}
        )
    core_quality_mismatch = compare_counter_to_reported(
        Counter(str(row.get("quality_status", "") or "blank") for row in core_exclusion_rows),
        summary.get("otc_core_exclusion_candidate_quality_status_totals"),
    )
    if core_quality_mismatch:
        core_exclusion_summary_gaps.append(
            {"field": "otc_core_exclusion_candidate_quality_status_totals", "mismatch": core_quality_mismatch}
        )
    core_gate_mismatch = compare_counter_to_reported(
        Counter(str(row.get("metadata_enrichment_gate", "")) for row in core_exclusion_rows),
        summary.get("otc_core_exclusion_candidate_metadata_gate_totals"),
    )
    if core_gate_mismatch:
        core_exclusion_summary_gaps.append(
            {"field": "otc_core_exclusion_candidate_metadata_gate_totals", "mismatch": core_gate_mismatch}
        )
    core_examples = summary.get("otc_core_exclusion_candidate_review_examples")
    if not isinstance(core_examples, list):
        core_exclusion_summary_gaps.append(
            {"field": "otc_core_exclusion_candidate_review_examples", "reason": "expected_list"}
        )
    elif len(core_exclusion_rows) > 0 and not core_examples:
        core_exclusion_summary_gaps.append(
            {"field": "otc_core_exclusion_candidate_review_examples", "reason": "expected_examples_for_active_queue"}
        )
    else:
        for index, example in enumerate(core_examples[:20]):
            if not isinstance(example, dict) or not str(example.get("listing_key", "")).startswith("OTC::"):
                core_exclusion_summary_gaps.append(
                    {"field": "otc_core_exclusion_candidate_review_examples", "row_index": index}
                )
    missing_required_buckets = [
        bucket
        for bucket in (
            "clean_extended_otc_listing",
            "documented_otc_sector_source_gap",
            "official_name_mismatch_review_first",
        )
        if bucket not in counters["review_bucket_totals"]
    ]
    scope_completion = summary.get("otc_scope_completion")
    scope_completion_gaps: list[dict[str, Any]] = []
    expected_completion = {
        "status": (
            "complete_extended_scope_no_core_candidates"
            if len(rows) > 0
            and dict(counters["instrument_scope_totals"]) == {"extended": len(rows)}
            and dict(counters["scope_reason_totals"]) == {"otc_listing": len(rows)}
            and dict(counters["scope_decision_totals"]) == {"already_extended_otc_listing": len(rows)}
            and dict(counters["scope_apply_eligibility_totals"]) == {"already_extended_no_scope_change_required": len(rows)}
            else "scope_review_open"
        ),
        "rows": len(rows),
        "extended_otc_rows": counters["instrument_scope_totals"].get("extended", 0),
        "otc_listing_scope_reason_rows": counters["scope_reason_totals"].get("otc_listing", 0),
        "already_extended_scope_decision_rows": counters["scope_decision_totals"].get("already_extended_otc_listing", 0),
        "core_exclusion_candidate_rows": len(core_exclusion_rows),
        "unexpected_core_scope_rows": counters["scope_decision_totals"].get("unexpected_otc_core_scope_review_required", 0),
        "blocked_scope_decision_rows": sum(
            count
            for eligibility, count in counters["scope_apply_eligibility_totals"].items()
            if str(eligibility).startswith("blocked_until_")
        ),
        "scope_apply_allowed_rows": 0,
        "metadata_enrichment_authorized": False,
    }
    if not isinstance(scope_completion, dict):
        scope_completion_gaps.append({"field": "otc_scope_completion", "reason": "expected_object"})
    else:
        for key, expected in expected_completion.items():
            if scope_completion.get(key) != expected:
                scope_completion_gaps.append(
                    {"field": f"otc_scope_completion.{key}", "reported": scope_completion.get(key), "expected": expected}
                )
        source_gate = str(scope_completion.get("source_gate", ""))
        if "metadata still requires listing-keyed evidence" not in source_gate:
            scope_completion_gaps.append({"field": "otc_scope_completion.source_gate", "reason": "missing_source_gate"})
    return {
        "passed": (
            bool(rows)
            and not missing_summary_keys
            and not row_count_mismatch
            and not row_gaps
            and not unsafe_enrichment_rows
            and not summary_mismatches
            and not review_bucket_asset_type_mismatches
            and not review_bucket_metadata_gate_mismatches
            and not top_scope_batch_gaps
            and not missing_policy_keys
            and drop_override_still_present == 0
            and not review_decision_balance
            and not stale_example_gaps
            and not core_exclusion_summary_gaps
            and not missing_required_buckets
            and not scope_completion_gaps
        ),
        "rows": len(rows),
        "missing_summary_keys": missing_summary_keys,
        "row_count_mismatch": row_count_mismatch,
        "row_gaps": row_gaps[:20],
        "unsafe_enrichment_rows": unsafe_enrichment_rows[:20],
        "summary_mismatches": summary_mismatches,
        "review_bucket_asset_type_mismatches": review_bucket_asset_type_mismatches,
        "review_bucket_metadata_gate_mismatches": review_bucket_metadata_gate_mismatches,
        "top_scope_batch_gaps": top_scope_batch_gaps[:20],
        "missing_policy_keys": missing_policy_keys,
        "drop_override_rows_still_present": drop_override_still_present,
        "review_decision_balance": review_decision_balance,
        "stale_review_decision_example_gaps": stale_example_gaps,
        "core_exclusion_summary_gaps": core_exclusion_summary_gaps,
        "missing_required_buckets": missing_required_buckets,
        "scope_completion_gaps": scope_completion_gaps,
        "required_row_keys": list(REQUIRED_OTC_SCOPE_ROW_KEYS),
        "accepted_scope_decisions": sorted(OTC_SCOPE_DECISIONS),
        "accepted_review_buckets": sorted(OTC_REVIEW_BUCKETS),
    }


def evaluate_canada_figi_gate(
    residual_report: dict[str, Any],
    queue_report: dict[str, Any],
    apply_report: dict[str, Any],
) -> dict[str, Any]:
    expected_residual_source_files = {
        "tickers_csv": "data/tickers.csv",
        "identifiers_extended_csv": "data/identifiers_extended.csv",
        "masterfile_reference_csv": "data/masterfiles/reference.csv",
        "source_gap_classification_csv": "data/reports/source_gap_classification.csv",
        "source_of_truth_decisions_csv": "data/reports/source_of_truth_decisions.csv",
        "openfigi_gap_csv": "data/review_overrides/canada_figi_openfigi_gaps.csv",
    }
    residual_meta = residual_report.get("_meta", {})
    residual_summary = residual_report.get("summary", {})
    residual_rows = residual_report.get("rows", [])
    queue_summary = queue_report.get("summary", {})
    queue_rows = queue_report.get("rows", [])
    apply_summary = apply_report.get("summary", {})
    apply_rows = apply_report.get("rows", [])
    if not isinstance(residual_meta, dict):
        residual_meta = {}
    if not isinstance(residual_summary, dict):
        residual_summary = {}
    if not isinstance(residual_rows, list):
        residual_rows = []
    if not isinstance(queue_summary, dict):
        queue_summary = {}
    if not isinstance(queue_rows, list):
        queue_rows = []
    if not isinstance(apply_summary, dict):
        apply_summary = {}
    if not isinstance(apply_rows, list):
        apply_rows = []
    residual_source_files = residual_meta.get("source_files", {})
    if not isinstance(residual_source_files, dict):
        residual_source_files = {}
    residual_missing_meta_keys = [
        key
        for key in ("generated_at", "source_files", "policy")
        if key not in residual_meta or residual_meta.get(key) in ("", None, {}, [])
    ]
    residual_invalid_generated_at = (
        str(residual_meta.get("generated_at", ""))
        if residual_meta.get("generated_at") and not is_valid_iso_utc_timestamp(str(residual_meta.get("generated_at")))
        else ""
    )
    residual_source_file_mismatches = {
        key: {"expected": expected, "actual": residual_source_files.get(key)}
        for key, expected in expected_residual_source_files.items()
        if residual_source_files.get(key) != expected
    }
    residual_unexpected_source_files = sorted(
        key for key in residual_source_files if key not in expected_residual_source_files
    )

    missing_summary_keys = {
        "residual": [key for key in REQUIRED_CANADA_RESIDUAL_SUMMARY_KEYS if key not in residual_summary],
        "queue": [key for key in REQUIRED_CANADA_QUEUE_SUMMARY_KEYS if key not in queue_summary],
        "apply": [key for key in REQUIRED_CANADA_APPLY_SUMMARY_KEYS if key not in apply_summary],
    }
    row_count_mismatches = {
        key: mismatch
        for key, mismatch in {
            "residual": {"reported": residual_summary.get("rows", 0), "actual": len(residual_rows)}
            if residual_summary.get("rows", 0) != len(residual_rows)
            else {},
            "queue": {"reported": queue_summary.get("rows", 0), "actual": len(queue_rows)}
            if queue_summary.get("rows", 0) != len(queue_rows)
            else {},
            "apply": {"reported": apply_summary.get("rows", 0), "actual": len(apply_rows)}
            if apply_summary.get("rows", 0) != len(apply_rows)
            else {},
        }.items()
        if mismatch
    }

    residual_counters: dict[str, Counter[str]] = {
        "exchange_totals": Counter(),
        "asset_type_totals": Counter(),
        "isin_decision_totals": Counter(),
        "figi_decision_totals": Counter(),
        "isin_apply_eligibility_totals": Counter(),
        "figi_apply_eligibility_totals": Counter(),
        "verification_evidence_required_totals": Counter(),
        "canada_resolution_queue_totals": Counter(),
        "openfigi_review_status_totals": Counter(),
        "openfigi_review_decision_totals": Counter(),
        "source_gap_field_totals": Counter(),
        "source_gap_class_totals": Counter(),
        "source_of_truth_outcome_totals": Counter(),
        "official_masterfile_source_totals": Counter(),
    }
    canada_queue_strategy_by_queue = {
        "core_exclusion_candidate_identifier_scope_review": "scope_review_before_canada_identifier_enrichment",
        "core_exclusion_candidate_metadata_scope_review": "scope_review_before_canada_metadata_enrichment",
        "missing_isin_official_canada_masterfiles_do_not_expose_isin": "seek_official_canada_isin_source",
        "missing_isin_reviewed_source_gap": "keep_isin_blank_until_stronger_official_source",
        "reviewed_openfigi_no_match_source_gap": "keep_figi_blank_after_reviewed_openfigi_no_match",
        "reviewed_openfigi_cross_isin_collision_source_gap": "keep_figi_blank_after_reviewed_openfigi_cross_isin_collision",
        "openfigi_candidate_after_isin_gate": "queue_openfigi_by_isin_with_canada_exchange_hint",
        "figi_blocked_until_isin_resolved": "block_figi_until_valid_isin",
        "metadata_source_gap_keep_blank_until_stronger_source": "keep_metadata_blank_until_stronger_official_source",
        "residual_no_identifier_action": "keep_metadata_blank_until_stronger_official_source",
    }
    canada_queue_evidence_by_queue = {
        "core_exclusion_candidate_identifier_scope_review": "official_listing_scope_decision_for_core_extended_or_exclude",
        "core_exclusion_candidate_metadata_scope_review": "official_listing_scope_decision_before_sector_or_category_fill",
        "missing_isin_official_canada_masterfiles_do_not_expose_isin": "official_csd_issuer_or_reviewed_identifier_source_with_exact_listing_match_and_valid_isin",
        "missing_isin_reviewed_source_gap": "stronger_official_identifier_source_before_isin_fill",
        "reviewed_openfigi_no_match_source_gap": "stronger_figi_source_required_openfigi_no_match_reviewed",
        "reviewed_openfigi_cross_isin_collision_source_gap": "stronger_figi_source_required_openfigi_cross_isin_collision_reviewed",
        "openfigi_candidate_after_isin_gate": "openfigi_id_isin_query_with_canada_exchange_hint_then_collision_gate",
        "figi_blocked_until_isin_resolved": "valid_isin_required_before_figi_lookup",
        "metadata_source_gap_keep_blank_until_stronger_source": "reviewed_issuer_or_product_metadata_source_with_exact_listing_match",
        "residual_no_identifier_action": "none_no_identifier_change_authorized",
    }
    canada_queue_strategy_counter: dict[str, Counter[str]] = defaultdict(Counter)
    canada_queue_evidence_counter: dict[str, Counter[str]] = defaultdict(Counter)
    canada_top_batch_counter: Counter[tuple[str, str, str]] = Counter()
    core_exclusion_counter: dict[str, Counter[str]] = {
        "core_exclusion_candidate_exchange_totals": Counter(),
        "core_exclusion_candidate_asset_type_totals": Counter(),
        "core_exclusion_candidate_resolution_queue_totals": Counter(),
        "core_exclusion_candidate_official_source_totals": Counter(),
        "core_exclusion_candidate_source_gap_class_totals": Counter(),
    }
    residual_row_gaps: list[dict[str, Any]] = []
    missing_isin_rows = 0
    missing_figi_rows = 0
    reviewed_openfigi_gap_rows = 0
    openfigi_candidate_rows = 0
    for index, row in enumerate(residual_rows):
        if not isinstance(row, dict):
            residual_row_gaps.append({"row_index": index, "reason": "canada_residual_row_is_not_object"})
            continue
        missing_keys = [key for key in REQUIRED_CANADA_RESIDUAL_ROW_KEYS if key not in row]
        invalid_fields: list[str] = []
        exchange = str(row.get("exchange", ""))
        if exchange not in CANADA_EXCHANGES:
            invalid_fields.append("exchange")
        if row.get("missing_isin") == "true":
            missing_isin_rows += 1
            if row.get("isin"):
                invalid_fields.append("missing_isin")
        if row.get("missing_figi") == "true":
            missing_figi_rows += 1
        if row.get("figi_decision") == "missing_figi_openfigi_candidate":
            openfigi_candidate_rows += 1
            if not row.get("isin"):
                invalid_fields.append("isin")
            if row.get("figi"):
                invalid_fields.append("figi")
        if str(row.get("figi_decision", "")).startswith("missing_figi_reviewed_source_gap"):
            reviewed_openfigi_gap_rows += 1
            if row.get("figi_apply_eligibility") != "keep_blank_as_reviewed_openfigi_source_gap":
                invalid_fields.append("figi_apply_eligibility")
            if "stronger_figi_source_required" not in str(row.get("verification_evidence_required", "")):
                invalid_fields.append("verification_evidence_required")
        residual_counters["exchange_totals"][exchange] += 1
        residual_counters["asset_type_totals"][str(row.get("asset_type", ""))] += 1
        residual_counters["isin_decision_totals"][str(row.get("isin_decision", ""))] += 1
        residual_counters["figi_decision_totals"][str(row.get("figi_decision", ""))] += 1
        residual_counters["isin_apply_eligibility_totals"][str(row.get("isin_apply_eligibility", ""))] += 1
        residual_counters["figi_apply_eligibility_totals"][str(row.get("figi_apply_eligibility", ""))] += 1
        residual_counters["verification_evidence_required_totals"][str(row.get("verification_evidence_required", ""))] += 1
        residual_counters["canada_resolution_queue_totals"][str(row.get("canada_resolution_queue", ""))] += 1
        queue = str(row.get("canada_resolution_queue", ""))
        strategy = canada_queue_strategy_by_queue.get(queue, "")
        evidence_required = canada_queue_evidence_by_queue.get(queue, "")
        if strategy and row.get("review_strategy") != strategy:
            invalid_fields.append("review_strategy")
        if evidence_required and row.get("queue_evidence_required") != evidence_required:
            invalid_fields.append("queue_evidence_required")
        official_source = str(row.get("official_masterfile_sources", "")) or "none"
        if "|" in official_source:
            official_source = "multiple_official_sources"
        expected_next_source = canada_recommended_next_source_for_queue(queue, official_source)
        expected_source_gate = canada_source_gate_for_queue(queue)
        if row.get("recommended_next_source") != expected_next_source:
            invalid_fields.append("recommended_next_source")
        if row.get("source_gate") != expected_source_gate:
            invalid_fields.append("source_gate")
        if row.get("source_gap_context") != canada_source_gap_context(row):
            invalid_fields.append("source_gap_context")
        if row.get("official_source_context") != canada_official_source_context(row):
            invalid_fields.append("official_source_context")
        if row.get("identifier_review_context") != canada_identifier_review_context(row):
            invalid_fields.append("identifier_review_context")
        if missing_keys or invalid_fields:
            residual_row_gaps.append(
                {
                    "row_index": index,
                    "listing_key": row.get("listing_key", ""),
                    "missing_keys": missing_keys,
                    "invalid_fields": invalid_fields,
                    "available_keys": sorted(row)[:25],
                }
            )
        if "core_exclusion_candidate" in str(row.get("source_of_truth_outcomes", "")).split("|"):
            core_exclusion_counter["core_exclusion_candidate_exchange_totals"][exchange] += 1
            core_exclusion_counter["core_exclusion_candidate_asset_type_totals"][str(row.get("asset_type", ""))] += 1
            core_exclusion_counter["core_exclusion_candidate_resolution_queue_totals"][queue] += 1
            core_sources = (
                str(row.get("official_masterfile_sources", "")).split("|")
                if str(row.get("official_masterfile_sources", ""))
                else ["none"]
            )
            for source in core_sources:
                if source:
                    core_exclusion_counter["core_exclusion_candidate_official_source_totals"][source] += 1
            core_gap_classes = (
                str(row.get("source_gap_classes", "")).split("|")
                if str(row.get("source_gap_classes", ""))
                else ["none"]
            )
            for gap_class in core_gap_classes:
                if gap_class:
                    core_exclusion_counter["core_exclusion_candidate_source_gap_class_totals"][gap_class] += 1
        if strategy:
            canada_queue_strategy_counter[queue][strategy] += 1
        if evidence_required:
            canada_queue_evidence_counter[queue][evidence_required] += 1
        sources = str(row.get("official_masterfile_sources", "")).split("|") if row.get("official_masterfile_sources") else ["none"]
        for source in sources:
            if source:
                canada_top_batch_counter[(queue, exchange, source)] += 1
        if row.get("openfigi_review_status"):
            residual_counters["openfigi_review_status_totals"][str(row.get("openfigi_review_status", ""))] += 1
        if row.get("openfigi_review_decision"):
            residual_counters["openfigi_review_decision_totals"][str(row.get("openfigi_review_decision", ""))] += 1
        for field in str(row.get("source_gap_fields", "")).split("|"):
            if field:
                residual_counters["source_gap_field_totals"][field] += 1
        for gap_class in str(row.get("source_gap_classes", "")).split("|"):
            if gap_class:
                residual_counters["source_gap_class_totals"][gap_class] += 1
        for outcome in str(row.get("source_of_truth_outcomes", "")).split("|"):
            if outcome:
                residual_counters["source_of_truth_outcome_totals"][outcome] += 1
        for source in str(row.get("official_masterfile_sources", "")).split("|"):
            if source:
                residual_counters["official_masterfile_source_totals"][source] += 1

    queue_counters: dict[str, Counter[str]] = {
        "exchange_totals": Counter(),
        "asset_type_totals": Counter(),
        "openfigi_exchange_hint_totals": Counter(),
        "apply_eligibility_totals": Counter(),
        "verification_evidence_required_totals": Counter(),
    }
    queue_row_gaps: list[dict[str, Any]] = []
    for index, row in enumerate(queue_rows):
        if not isinstance(row, dict):
            queue_row_gaps.append({"row_index": index, "reason": "canada_queue_row_is_not_object"})
            continue
        missing_keys = [key for key in REQUIRED_CANADA_QUEUE_ROW_KEYS if not row.get(key)]
        invalid_fields: list[str] = []
        if row.get("exchange") not in CANADA_EXCHANGES:
            invalid_fields.append("exchange")
        if not row.get("isin"):
            invalid_fields.append("isin")
        if row.get("openfigi_exchange_hint") != "CN":
            invalid_fields.append("openfigi_exchange_hint")
        if row.get("apply_eligibility") != CANADA_QUEUE_ELIGIBILITY:
            invalid_fields.append("apply_eligibility")
        if row.get("verification_evidence_required") != CANADA_QUEUE_EVIDENCE:
            invalid_fields.append("verification_evidence_required")
        if missing_keys or invalid_fields:
            queue_row_gaps.append(
                {
                    "row_index": index,
                    "listing_key": row.get("listing_key", ""),
                    "missing_keys": missing_keys,
                    "invalid_fields": invalid_fields,
                    "available_keys": sorted(row)[:25],
                }
            )
        queue_counters["exchange_totals"][str(row.get("exchange", ""))] += 1
        queue_counters["asset_type_totals"][str(row.get("asset_type", ""))] += 1
        queue_counters["openfigi_exchange_hint_totals"][str(row.get("openfigi_exchange_hint", ""))] += 1
        queue_counters["apply_eligibility_totals"][str(row.get("apply_eligibility", ""))] += 1
        queue_counters["verification_evidence_required_totals"][str(row.get("verification_evidence_required", ""))] += 1
    if not queue_rows:
        queue_counters["apply_eligibility_totals"]["no_active_openfigi_probe_rows"] += 1
        queue_counters["verification_evidence_required_totals"]["none_queue_drained_or_candidates_excluded_as_reviewed_source_gaps"] += 1
    excluded_openfigi_gap_rows = int(queue_summary.get("excluded_openfigi_gap_rows", 0) or 0)
    if excluded_openfigi_gap_rows:
        queue_counters["apply_eligibility_totals"][CANADA_EXCLUDED_GAP_ELIGIBILITY] += excluded_openfigi_gap_rows
        queue_counters["verification_evidence_required_totals"][
            "stronger_figi_source_required_for_reviewed_openfigi_no_match_or_cross_isin_collision"
        ] += excluded_openfigi_gap_rows

    apply_counters = {
        "decision_totals": Counter(),
        "reason_totals": Counter(),
    }
    apply_row_gaps: list[dict[str, Any]] = []
    for index, row in enumerate(apply_rows):
        if not isinstance(row, dict):
            apply_row_gaps.append({"row_index": index, "reason": "canada_apply_row_is_not_object"})
            continue
        missing_keys = [key for key in REQUIRED_CANADA_APPLY_ROW_KEYS if not row.get(key)]
        invalid_fields: list[str] = []
        if row.get("exchange") not in CANADA_EXCHANGES:
            invalid_fields.append("exchange")
        if row.get("decision") not in CANADA_APPLY_DECISIONS:
            invalid_fields.append("decision")
        if row.get("decision") == "apply":
            if not row.get("isin") or not row.get("figi"):
                invalid_fields.append("figi")
            if row.get("reason") != "accepted_probe_match":
                invalid_fields.append("reason")
        if missing_keys or invalid_fields:
            apply_row_gaps.append(
                {
                    "row_index": index,
                    "listing_key": row.get("listing_key", ""),
                    "missing_keys": missing_keys,
                    "invalid_fields": invalid_fields,
                    "available_keys": sorted(row)[:25],
                }
            )
        apply_counters["decision_totals"][str(row.get("decision", ""))] += 1
        apply_counters["reason_totals"][str(row.get("reason", ""))] += 1

    residual_summary_mismatches = {
        key: compare_counter_to_reported(counter, residual_summary.get(key))
        for key, counter in residual_counters.items()
    }
    residual_summary_mismatches = {key: value for key, value in residual_summary_mismatches.items() if value}
    for key, counter in core_exclusion_counter.items():
        mismatch = compare_counter_to_reported(counter, residual_summary.get(key))
        if mismatch:
            residual_summary_mismatches[key] = mismatch
    expected_resolution_exchange_totals = {
        queue: dict(
            sorted(
                Counter(
                    str(row.get("exchange", ""))
                    for row in residual_rows
                    if isinstance(row, dict) and str(row.get("canada_resolution_queue", "")) == queue
                ).items()
            )
        )
        for queue in sorted(residual_counters["canada_resolution_queue_totals"])
    }
    if residual_summary.get("canada_resolution_queue_exchange_totals") != expected_resolution_exchange_totals:
        residual_summary_mismatches["canada_resolution_queue_exchange_totals"] = {
            "reported": residual_summary.get("canada_resolution_queue_exchange_totals"),
            "actual": expected_resolution_exchange_totals,
        }
    expected_resolution_asset_type_totals = {
        queue: dict(
            sorted(
                Counter(
                    str(row.get("asset_type", ""))
                    for row in residual_rows
                    if isinstance(row, dict) and str(row.get("canada_resolution_queue", "")) == queue
                ).items()
            )
        )
        for queue in sorted(residual_counters["canada_resolution_queue_totals"])
    }
    if residual_summary.get("canada_resolution_queue_asset_type_totals") != expected_resolution_asset_type_totals:
        residual_summary_mismatches["canada_resolution_queue_asset_type_totals"] = {
            "reported": residual_summary.get("canada_resolution_queue_asset_type_totals"),
            "actual": expected_resolution_asset_type_totals,
        }
    expected_resolution_source_gap_class_totals = {
        queue: dict(
            sorted(
                Counter(
                    gap_class
                    for row in residual_rows
                    if isinstance(row, dict) and str(row.get("canada_resolution_queue", "")) == queue
                    for gap_class in (
                        str(row.get("source_gap_classes", "")).split("|")
                        if str(row.get("source_gap_classes", ""))
                        else ["none"]
                    )
                    if gap_class
                ).items()
            )
        )
        for queue in sorted(residual_counters["canada_resolution_queue_totals"])
    }
    if residual_summary.get("canada_resolution_queue_source_gap_class_totals") != expected_resolution_source_gap_class_totals:
        residual_summary_mismatches["canada_resolution_queue_source_gap_class_totals"] = {
            "reported": residual_summary.get("canada_resolution_queue_source_gap_class_totals"),
            "actual": expected_resolution_source_gap_class_totals,
        }
    expected_resolution_official_source_totals = {
        queue: dict(
            sorted(
                Counter(
                    source
                    for row in residual_rows
                    if isinstance(row, dict) and str(row.get("canada_resolution_queue", "")) == queue
                    for source in (
                        str(row.get("official_masterfile_sources", "")).split("|")
                        if str(row.get("official_masterfile_sources", ""))
                        else ["none"]
                    )
                    if source
                ).items()
            )
        )
        for queue in sorted(residual_counters["canada_resolution_queue_totals"])
    }
    if residual_summary.get("canada_resolution_queue_official_source_totals") != expected_resolution_official_source_totals:
        residual_summary_mismatches["canada_resolution_queue_official_source_totals"] = {
            "reported": residual_summary.get("canada_resolution_queue_official_source_totals"),
            "actual": expected_resolution_official_source_totals,
        }
    expected_resolution_strategy_totals = {
        queue: dict(sorted(counter.items()))
        for queue, counter in sorted(canada_queue_strategy_counter.items())
    }
    if residual_summary.get("canada_resolution_queue_review_strategy_totals") != expected_resolution_strategy_totals:
        residual_summary_mismatches["canada_resolution_queue_review_strategy_totals"] = {
            "reported": residual_summary.get("canada_resolution_queue_review_strategy_totals"),
            "actual": expected_resolution_strategy_totals,
        }
    expected_resolution_evidence_totals = {
        queue: dict(sorted(counter.items()))
        for queue, counter in sorted(canada_queue_evidence_counter.items())
    }
    if residual_summary.get("canada_resolution_queue_evidence_required_totals") != expected_resolution_evidence_totals:
        residual_summary_mismatches["canada_resolution_queue_evidence_required_totals"] = {
            "reported": residual_summary.get("canada_resolution_queue_evidence_required_totals"),
            "actual": expected_resolution_evidence_totals,
        }
    top_canada_batch_gaps: list[dict[str, Any]] = []
    top_canada_batches = residual_summary.get("top_canada_resolution_review_batches")
    if not isinstance(top_canada_batches, list) or not top_canada_batches:
        top_canada_batch_gaps.append(
            {"field": "top_canada_resolution_review_batches", "reason": "expected_ranked_review_batches"}
        )
    elif isinstance(top_canada_batches, list):
        for index, batch in enumerate(top_canada_batches):
            if not isinstance(batch, dict):
                top_canada_batch_gaps.append({"row_index": index, "reason": "expected_object"})
                continue
            queue = str(batch.get("canada_resolution_queue", ""))
            strategy = str(batch.get("review_strategy", ""))
            missing_keys = [
                key
                for key in (
                    "canada_resolution_queue",
                    "exchange",
                    "official_source",
                    "rows",
                    "review_strategy",
                    "evidence_required",
                    "recommended_next_source",
                    "source_gate",
                )
                if key not in batch
            ]
            invalid_fields = []
            if queue not in canada_queue_strategy_by_queue:
                invalid_fields.append("canada_resolution_queue")
            if batch.get("exchange") not in CANADA_EXCHANGES:
                invalid_fields.append("exchange")
            if not isinstance(batch.get("rows"), int) or batch.get("rows", 0) <= 0:
                invalid_fields.append("rows")
            if strategy not in CANADA_REVIEW_STRATEGIES:
                invalid_fields.append("review_strategy")
            if canada_queue_strategy_by_queue.get(queue) != strategy:
                invalid_fields.append("review_strategy")
            if not batch.get("official_source"):
                invalid_fields.append("official_source")
            if not batch.get("evidence_required") or batch.get("evidence_required") in {"none", "ticker_match_only"}:
                invalid_fields.append("evidence_required")
            if canada_queue_evidence_by_queue.get(queue) != batch.get("evidence_required"):
                invalid_fields.append("evidence_required")
            if not batch.get("recommended_next_source"):
                invalid_fields.append("recommended_next_source")
            if not batch.get("source_gate"):
                invalid_fields.append("source_gate")
            if missing_keys or invalid_fields:
                top_canada_batch_gaps.append(
                    {"row_index": index, "missing_keys": missing_keys, "invalid_fields": invalid_fields}
                )
    residual_count_mismatches = {
        key: mismatch
        for key, mismatch in {
            "missing_isin_rows": {
                "reported": int(residual_summary.get("missing_isin_rows", -1)),
                "actual": missing_isin_rows,
            }
            if int(residual_summary.get("missing_isin_rows", -1)) != missing_isin_rows
            else {},
            "missing_figi_rows": {
                "reported": int(residual_summary.get("missing_figi_rows", -1)),
                "actual": missing_figi_rows,
            }
            if int(residual_summary.get("missing_figi_rows", -1)) != missing_figi_rows
            else {},
        }.items()
        if mismatch
    }
    queue_summary_mismatches = {
        key: compare_counter_to_reported(counter, queue_summary.get(key))
        for key, counter in queue_counters.items()
    }
    queue_summary_mismatches = {key: value for key, value in queue_summary_mismatches.items() if value}
    apply_summary_mismatches = {
        key: compare_counter_to_reported(counter, apply_summary.get(key))
        for key, counter in apply_counters.items()
    }
    apply_summary_mismatches = {key: value for key, value in apply_summary_mismatches.items() if value}
    applied_rows_mismatch = {
        "reported": apply_summary.get("applied_rows", 0),
        "actual": apply_counters["decision_totals"].get("apply", 0) if apply_summary.get("applied") else 0,
    } if apply_summary.get("applied_rows", 0) != (
        apply_counters["decision_totals"].get("apply", 0) if apply_summary.get("applied") else 0
    ) else {}
    written_rows_mismatch = {
        "reported": apply_summary.get("written_rows", 0),
        "actual": apply_summary.get("applied_rows", 0) if apply_summary.get("applied") else 0,
    } if apply_summary.get("written_rows", 0) != (
        apply_summary.get("applied_rows", 0) if apply_summary.get("applied") else 0
    ) else {}
    missing_policy_keys = {
        "residual": [
            key
            for key in CANADA_REQUIRED_RESIDUAL_POLICY_KEYS
            if not isinstance(residual_summary.get("policy"), dict) or not residual_summary.get("policy", {}).get(key)
        ],
        "queue": [
            key
            for key in CANADA_REQUIRED_QUEUE_POLICY_KEYS
            if not isinstance(queue_summary.get("policy"), dict) or not queue_summary.get("policy", {}).get(key)
        ],
        "apply": [
            key
            for key in CANADA_REQUIRED_APPLY_POLICY_KEYS
            if not isinstance(apply_summary.get("policy"), dict) or not apply_summary.get("policy", {}).get(key)
        ],
    }
    queue_exclusion_mismatch = {
        "reported": excluded_openfigi_gap_rows,
        "residual_reviewed_openfigi_gap_rows": reviewed_openfigi_gap_rows,
    } if excluded_openfigi_gap_rows != reviewed_openfigi_gap_rows else {}
    queue_candidate_mismatch = {
        "queue_rows": len(queue_rows),
        "residual_openfigi_candidate_rows": openfigi_candidate_rows,
    } if len(queue_rows) != openfigi_candidate_rows else {}
    return {
        "passed": (
            bool(residual_rows)
            and not residual_missing_meta_keys
            and not residual_invalid_generated_at
            and not residual_source_file_mismatches
            and not residual_unexpected_source_files
            and not any(missing_summary_keys.values())
            and not row_count_mismatches
            and not residual_row_gaps
            and not queue_row_gaps
            and not apply_row_gaps
            and not residual_summary_mismatches
            and not top_canada_batch_gaps
            and not residual_count_mismatches
            and not queue_summary_mismatches
            and not apply_summary_mismatches
            and not applied_rows_mismatch
            and not written_rows_mismatch
            and not any(missing_policy_keys.values())
            and not queue_exclusion_mismatch
            and not queue_candidate_mismatch
        ),
        "residual_rows": len(residual_rows),
        "queue_rows": len(queue_rows),
        "apply_rows": len(apply_rows),
        "residual_missing_meta_keys": residual_missing_meta_keys,
        "residual_invalid_generated_at": residual_invalid_generated_at,
        "residual_source_file_mismatches": residual_source_file_mismatches,
        "residual_unexpected_source_files": residual_unexpected_source_files,
        "missing_summary_keys": missing_summary_keys,
        "row_count_mismatches": row_count_mismatches,
        "residual_row_gaps": residual_row_gaps[:20],
        "queue_row_gaps": queue_row_gaps[:20],
        "apply_row_gaps": apply_row_gaps[:20],
        "residual_summary_mismatches": residual_summary_mismatches,
        "top_canada_batch_gaps": top_canada_batch_gaps[:20],
        "residual_count_mismatches": residual_count_mismatches,
        "queue_summary_mismatches": queue_summary_mismatches,
        "apply_summary_mismatches": apply_summary_mismatches,
        "applied_rows_mismatch": applied_rows_mismatch,
        "written_rows_mismatch": written_rows_mismatch,
        "missing_policy_keys": missing_policy_keys,
        "queue_exclusion_mismatch": queue_exclusion_mismatch,
        "queue_candidate_mismatch": queue_candidate_mismatch,
        "reviewed_openfigi_gap_rows": reviewed_openfigi_gap_rows,
        "openfigi_candidate_rows": openfigi_candidate_rows,
    }


def evaluate_asx_residual_gate(asx_report: dict[str, Any]) -> dict[str, Any]:
    expected_source_files = {
        "tickers_csv": "data/tickers.csv",
        "masterfile_reference_csv": "data/masterfiles/reference.csv",
        "source_gap_classification_csv": "data/reports/source_gap_classification.csv",
        "source_of_truth_decisions_csv": "data/reports/source_of_truth_decisions.csv",
        "asx_isin_probe_csv": "data/asx_verification/missing_isin_backfill.csv",
    }
    meta = asx_report.get("_meta", {})
    summary = asx_report.get("summary", {})
    rows = asx_report.get("rows", [])
    if not isinstance(meta, dict):
        meta = {}
    if not isinstance(summary, dict):
        summary = {}
    if not isinstance(rows, list):
        rows = []
    source_files = meta.get("source_files", {})
    if not isinstance(source_files, dict):
        source_files = {}
    missing_meta_keys = [
        key
        for key in ("generated_at", "source_files", "policy")
        if key not in meta or meta.get(key) in ("", None, {}, [])
    ]
    invalid_generated_at = (
        str(meta.get("generated_at", ""))
        if meta.get("generated_at") and not is_valid_iso_utc_timestamp(str(meta.get("generated_at")))
        else ""
    )
    source_file_mismatches = {
        key: {"expected": expected, "actual": source_files.get(key)}
        for key, expected in expected_source_files.items()
        if source_files.get(key) != expected
    }
    unexpected_source_files = sorted(key for key in source_files if key not in expected_source_files)
    missing_summary_keys = [key for key in REQUIRED_ASX_RESIDUAL_SUMMARY_KEYS if key not in summary]
    row_count_mismatch = {
        "reported": summary.get("rows", 0),
        "actual": len(rows),
    } if summary.get("rows", 0) != len(rows) else {}
    counters: dict[str, Counter[str]] = {
        "field_totals": Counter(),
        "asset_type_totals": Counter(),
        "core_exclusion_candidate_field_totals": Counter(),
        "core_exclusion_candidate_asset_type_totals": Counter(),
        "core_exclusion_candidate_gap_class_totals": Counter(),
        "core_exclusion_candidate_resolution_queue_totals": Counter(),
        "core_exclusion_candidate_official_source_totals": Counter(),
        "core_exclusion_candidate_official_capability_totals": Counter(),
        "gap_class_totals": Counter(),
        "source_of_truth_outcome_totals": Counter(),
        "asx_resolution_queue_totals": Counter(),
        "residual_decision_totals": Counter(),
        "review_bucket_totals": Counter(),
        "review_priority_totals": Counter(),
        "apply_eligibility_totals": Counter(),
        "verification_evidence_required_totals": Counter(),
        "asx_isin_probe_decision_totals": Counter(),
        "official_masterfile_match_totals": Counter(),
        "official_masterfile_exposes_isin_totals": Counter(),
        "official_masterfile_exposes_sector_totals": Counter(),
        "official_masterfile_source_totals": Counter(),
    }
    row_gaps: list[dict[str, Any]] = []
    unsafe_apply_rows: list[dict[str, Any]] = []
    asx_queue_strategy_counter: dict[str, Counter[str]] = {}
    asx_queue_evidence_counter: dict[str, Counter[str]] = {}
    asx_queue_official_capability_counter: dict[str, Counter[str]] = {}
    asx_review_batches: Counter[tuple[str, str, str]] = Counter()
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            row_gaps.append({"row_index": index, "reason": "asx_residual_row_is_not_object"})
            continue
        missing_keys = [key for key in REQUIRED_ASX_RESIDUAL_ROW_KEYS if not row.get(key)]
        invalid_fields: list[str] = []
        listing_key = str(row.get("listing_key", ""))
        field = str(row.get("field", ""))
        queue = str(row.get("asx_resolution_queue", ""))
        review_bucket = str(row.get("review_bucket", ""))
        review_priority = str(row.get("review_priority", ""))
        apply_eligibility = str(row.get("apply_eligibility", ""))
        residual_decision = str(row.get("residual_decision", ""))
        verification_evidence = str(row.get("verification_evidence_required", ""))
        review_strategy = str(row.get("review_strategy", ""))
        recommended_action = str(row.get("recommended_next_action", ""))
        expected_strategy, expected_evidence = asx_review_strategy_for(queue)
        if not listing_key.startswith("ASX::") or row.get("exchange") != "ASX":
            invalid_fields.append("listing_key")
        if field not in ASX_RESIDUAL_FIELDS:
            invalid_fields.append("field")
        if review_bucket not in ASX_REVIEW_BUCKETS:
            invalid_fields.append("review_bucket")
        if review_priority not in ASX_REVIEW_PRIORITIES:
            invalid_fields.append("review_priority")
        if row.get("review_needed") not in {"true", True}:
            invalid_fields.append("review_needed")
        if review_strategy != expected_strategy:
            invalid_fields.append("review_strategy")
        if verification_evidence != expected_evidence:
            invalid_fields.append("verification_evidence_required")
        if field == "missing_isin_primary" and row.get("target_field") != "isin":
            invalid_fields.append("target_field")
        if field == "missing_etf_category" and row.get("target_field") != "etf_category":
            invalid_fields.append("target_field")
        if residual_decision == "official_asx_isin_available_review_apply":
            if not str(row.get("asx_isin_probe_isin", "")).startswith("AU"):
                invalid_fields.append("asx_isin_probe_isin")
            if apply_eligibility != "apply_only_after_asx_code_name_token_and_isin_checksum_validation":
                unsafe_apply_rows.append(
                    {
                        "row_index": index,
                        "listing_key": listing_key,
                        "apply_eligibility": apply_eligibility,
                    }
                )
            if verification_evidence != "official_asx_isin_workbook_exact_code_name_numeric_token_and_valid_isin_checksum":
                invalid_fields.append("verification_evidence_required")
        if review_bucket == "identity_mismatch_requires_manual_review":
            if apply_eligibility != "blocked_until_identity_mismatch_resolved":
                invalid_fields.append("apply_eligibility")
            if recommended_action != "do_not_apply_until_name_mismatch_is_manually_resolved":
                invalid_fields.append("recommended_next_action")
        if review_bucket == "scope_review_before_any_data_fill":
            if row.get("source_of_truth_outcome") != "core_exclusion_candidate":
                invalid_fields.append("source_of_truth_outcome")
            if apply_eligibility != "blocked_until_core_or_extended_scope_decision":
                invalid_fields.append("apply_eligibility")
        if review_bucket == "product_taxonomy_source_gap":
            if apply_eligibility != "keep_category_blank_until_official_product_taxonomy_source":
                invalid_fields.append("apply_eligibility")
            if "category_blank" not in recommended_action:
                invalid_fields.append("recommended_next_action")
        if review_bucket == "identifier_source_gap":
            if apply_eligibility != "keep_identifier_blank_until_direct_registry_issuer_or_official_asx_evidence":
                invalid_fields.append("apply_eligibility")
            if "identifier_blank" not in recommended_action:
                invalid_fields.append("recommended_next_action")
        official_match = str(row.get("official_masterfile_match", ""))
        official_exposes_isin = str(row.get("official_masterfile_exposes_isin", ""))
        official_exposes_sector = str(row.get("official_masterfile_exposes_sector", ""))
        official_sources = str(row.get("official_masterfile_sources", ""))
        official_source_context = str(row.get("official_source_context", ""))
        official_capability = str(row.get("official_capability", ""))
        official_source_label = official_sources or "none"
        expected_source_context = (
            f"official_masterfile_sources={official_source_label};"
            f"asx_isin_probe_decision={row.get('asx_isin_probe_decision') or 'missing_probe_row'}"
        )
        expected_capability = (
            f"masterfile_match={official_match};"
            f"masterfile_exposes_isin={official_exposes_isin};"
            f"masterfile_exposes_sector={official_exposes_sector}"
        )
        if row.get("recommended_next_source") != asx_recommended_next_source_for(queue, official_source_label):
            invalid_fields.append("recommended_next_source")
        if row.get("source_gate") != asx_source_gate_for(queue):
            invalid_fields.append("source_gate")
        if official_source_context != expected_source_context:
            invalid_fields.append("official_source_context")
        if official_capability != expected_capability:
            invalid_fields.append("official_capability")
        if missing_keys or invalid_fields:
            row_gaps.append(
                {
                    "row_index": index,
                    "listing_key": listing_key,
                    "missing_keys": missing_keys,
                    "invalid_fields": invalid_fields,
                    "available_keys": sorted(row)[:25],
                }
            )
        sources = str(row.get("official_masterfile_sources", "")).split("|") if str(row.get("official_masterfile_sources", "")) else ["none"]
        counters["field_totals"][field] += 1
        counters["asset_type_totals"][str(row.get("asset_type", ""))] += 1
        if str(row.get("source_of_truth_outcome", "")) == "core_exclusion_candidate":
            counters["core_exclusion_candidate_field_totals"][field] += 1
            counters["core_exclusion_candidate_asset_type_totals"][str(row.get("asset_type", ""))] += 1
            counters["core_exclusion_candidate_gap_class_totals"][str(row.get("gap_class", ""))] += 1
            counters["core_exclusion_candidate_resolution_queue_totals"][queue] += 1
            for source in sources:
                if source:
                    counters["core_exclusion_candidate_official_source_totals"][source] += 1
            for capability in (
                f"masterfile_match={official_match}",
                f"masterfile_exposes_isin={official_exposes_isin}",
                f"masterfile_exposes_sector={official_exposes_sector}",
            ):
                counters["core_exclusion_candidate_official_capability_totals"][capability] += 1
        counters["gap_class_totals"][str(row.get("gap_class", ""))] += 1
        counters["source_of_truth_outcome_totals"][str(row.get("source_of_truth_outcome", ""))] += 1
        counters["asx_resolution_queue_totals"][queue] += 1
        counters["residual_decision_totals"][residual_decision] += 1
        counters["review_bucket_totals"][review_bucket] += 1
        counters["review_priority_totals"][review_priority] += 1
        counters["apply_eligibility_totals"][apply_eligibility] += 1
        counters["verification_evidence_required_totals"][verification_evidence] += 1
        counters["official_masterfile_match_totals"][official_match] += 1
        counters["official_masterfile_exposes_isin_totals"][official_exposes_isin] += 1
        counters["official_masterfile_exposes_sector_totals"][official_exposes_sector] += 1
        asx_queue_strategy_counter.setdefault(queue, Counter())[expected_strategy] += 1
        asx_queue_evidence_counter.setdefault(queue, Counter())[expected_evidence] += 1
        asx_queue_official_capability_counter.setdefault(queue, Counter())[
            f"masterfile_match={official_match}"
        ] += 1
        asx_queue_official_capability_counter.setdefault(queue, Counter())[
            f"masterfile_exposes_isin={official_exposes_isin}"
        ] += 1
        asx_queue_official_capability_counter.setdefault(queue, Counter())[
            f"masterfile_exposes_sector={official_exposes_sector}"
        ] += 1
        if field == "missing_isin_primary":
            counters["asx_isin_probe_decision_totals"][str(row.get("asx_isin_probe_decision", "") or "missing_probe_row")] += 1
        for source in sources:
            if source:
                asx_review_batches[(queue, field, source)] += 1
            if source and source != "none":
                counters["official_masterfile_source_totals"][source] += 1
    summary_mismatches = {
        key: compare_counter_to_reported(counter, summary.get(key))
        for key, counter in counters.items()
    }
    summary_mismatches = {key: value for key, value in summary_mismatches.items() if value}
    expected_resolution_field_totals = {
        queue: dict(
            sorted(
                Counter(
                    str(row.get("field", ""))
                    for row in rows
                    if isinstance(row, dict) and str(row.get("asx_resolution_queue", "")) == queue
                ).items()
            )
        )
        for queue in sorted(counters["asx_resolution_queue_totals"])
    }
    if summary.get("asx_resolution_queue_field_totals") != expected_resolution_field_totals:
        summary_mismatches["asx_resolution_queue_field_totals"] = {
            "reported": summary.get("asx_resolution_queue_field_totals"),
            "actual": expected_resolution_field_totals,
        }
    expected_resolution_gap_class_totals = {
        queue: dict(
            sorted(
                Counter(
                    str(row.get("gap_class", ""))
                    for row in rows
                    if isinstance(row, dict) and str(row.get("asx_resolution_queue", "")) == queue
                ).items()
            )
        )
        for queue in sorted(counters["asx_resolution_queue_totals"])
    }
    if summary.get("asx_resolution_queue_gap_class_totals") != expected_resolution_gap_class_totals:
        summary_mismatches["asx_resolution_queue_gap_class_totals"] = {
            "reported": summary.get("asx_resolution_queue_gap_class_totals"),
            "actual": expected_resolution_gap_class_totals,
        }
    expected_resolution_official_source_totals = {
        queue: dict(
            sorted(
                Counter(
                    source
                    for row in rows
                    if isinstance(row, dict) and str(row.get("asx_resolution_queue", "")) == queue
                    for source in (
                        str(row.get("official_masterfile_sources", "")).split("|")
                        if str(row.get("official_masterfile_sources", ""))
                        else ["none"]
                    )
                    if source
                ).items()
            )
        )
        for queue in sorted(counters["asx_resolution_queue_totals"])
    }
    if summary.get("asx_resolution_queue_official_source_totals") != expected_resolution_official_source_totals:
        summary_mismatches["asx_resolution_queue_official_source_totals"] = {
            "reported": summary.get("asx_resolution_queue_official_source_totals"),
            "actual": expected_resolution_official_source_totals,
        }
    expected_resolution_strategy_totals = {
        queue: dict(sorted(counter.items()))
        for queue, counter in sorted(asx_queue_strategy_counter.items())
    }
    if summary.get("asx_resolution_queue_review_strategy_totals") != expected_resolution_strategy_totals:
        summary_mismatches["asx_resolution_queue_review_strategy_totals"] = {
            "reported": summary.get("asx_resolution_queue_review_strategy_totals"),
            "actual": expected_resolution_strategy_totals,
        }
    expected_resolution_evidence_totals = {
        queue: dict(sorted(counter.items()))
        for queue, counter in sorted(asx_queue_evidence_counter.items())
    }
    if summary.get("asx_resolution_queue_evidence_required_totals") != expected_resolution_evidence_totals:
        summary_mismatches["asx_resolution_queue_evidence_required_totals"] = {
            "reported": summary.get("asx_resolution_queue_evidence_required_totals"),
            "actual": expected_resolution_evidence_totals,
        }
    expected_resolution_capability_totals = {
        queue: dict(sorted(counter.items()))
        for queue, counter in sorted(asx_queue_official_capability_counter.items())
    }
    if summary.get("asx_resolution_queue_official_capability_totals") != expected_resolution_capability_totals:
        summary_mismatches["asx_resolution_queue_official_capability_totals"] = {
            "reported": summary.get("asx_resolution_queue_official_capability_totals"),
            "actual": expected_resolution_capability_totals,
        }
    top_asx_batch_gaps: list[dict[str, Any]] = []
    top_asx_batches = summary.get("top_asx_resolution_review_batches")
    if not isinstance(top_asx_batches, list) or not top_asx_batches:
        top_asx_batch_gaps.append(
            {"field": "top_asx_resolution_review_batches", "reason": "expected_ranked_review_batches"}
        )
    elif isinstance(top_asx_batches, list):
        for index, batch in enumerate(top_asx_batches):
            if not isinstance(batch, dict):
                top_asx_batch_gaps.append({"row_index": index, "reason": "expected_object"})
                continue
            queue = str(batch.get("asx_resolution_queue", ""))
            field = str(batch.get("field", ""))
            strategy = str(batch.get("review_strategy", ""))
            missing_keys = [
                key
                for key in (
                    "asx_resolution_queue",
                    "field",
                    "official_source",
                    "rows",
                    "review_strategy",
                    "evidence_required",
                    "recommended_next_source",
                    "source_gate",
                )
                if key not in batch
            ]
            invalid_fields = []
            expected_strategy, expected_evidence = asx_review_strategy_for(queue)
            if queue not in asx_queue_strategy_counter:
                invalid_fields.append("asx_resolution_queue")
            if field not in ASX_RESIDUAL_FIELDS:
                invalid_fields.append("field")
            if not isinstance(batch.get("rows"), int) or batch.get("rows", 0) <= 0:
                invalid_fields.append("rows")
            if strategy not in ASX_REVIEW_STRATEGIES or strategy != expected_strategy:
                invalid_fields.append("review_strategy")
            if not batch.get("official_source"):
                invalid_fields.append("official_source")
            if batch.get("evidence_required") != expected_evidence:
                invalid_fields.append("evidence_required")
            if not batch.get("recommended_next_source"):
                invalid_fields.append("recommended_next_source")
            if not batch.get("source_gate"):
                invalid_fields.append("source_gate")
            if missing_keys or invalid_fields:
                top_asx_batch_gaps.append(
                    {"row_index": index, "missing_keys": missing_keys, "invalid_fields": invalid_fields}
                )
    policy = summary.get("policy", {})
    missing_policy_keys = [
        key
        for key in ASX_REQUIRED_POLICY_KEYS
        if not isinstance(policy, dict) or not policy.get(key)
    ]
    core_exclusion_rows = counters["source_of_truth_outcome_totals"].get("core_exclusion_candidate", 0)
    core_exclusion_row_mismatch = {}
    if summary.get("core_exclusion_candidate_rows") != core_exclusion_rows:
        core_exclusion_row_mismatch = {
            "reported": summary.get("core_exclusion_candidate_rows"),
            "actual": core_exclusion_rows,
        }
    missing_required_buckets = [
        bucket
        for bucket in (
            "scope_review_before_any_data_fill",
            "product_taxonomy_source_gap",
            "identifier_source_gap",
        )
        if bucket not in counters["review_bucket_totals"]
    ]
    return {
        "passed": (
            bool(rows)
            and not missing_meta_keys
            and not invalid_generated_at
            and not source_file_mismatches
            and not unexpected_source_files
            and not missing_summary_keys
            and not row_count_mismatch
            and not row_gaps
            and not unsafe_apply_rows
            and not summary_mismatches
            and not top_asx_batch_gaps
            and not missing_policy_keys
            and not core_exclusion_row_mismatch
            and not missing_required_buckets
        ),
        "rows": len(rows),
        "missing_meta_keys": missing_meta_keys,
        "invalid_generated_at": invalid_generated_at,
        "source_file_mismatches": source_file_mismatches,
        "unexpected_source_files": unexpected_source_files,
        "missing_summary_keys": missing_summary_keys,
        "row_count_mismatch": row_count_mismatch,
        "row_gaps": row_gaps[:20],
        "unsafe_apply_rows": unsafe_apply_rows[:20],
        "summary_mismatches": summary_mismatches,
        "top_asx_batch_gaps": top_asx_batch_gaps,
        "core_exclusion_row_mismatch": core_exclusion_row_mismatch,
        "missing_policy_keys": missing_policy_keys,
        "missing_required_buckets": missing_required_buckets,
        "required_row_keys": list(REQUIRED_ASX_RESIDUAL_ROW_KEYS),
        "accepted_fields": sorted(ASX_RESIDUAL_FIELDS),
        "accepted_review_buckets": sorted(ASX_REVIEW_BUCKETS),
    }


def evaluate_b3_residual_gate(
    isin_report: dict[str, Any],
    sector_report: dict[str, Any],
) -> dict[str, Any]:
    def b3_code_shape(value: str) -> str:
        code = value.strip().upper()
        if not code:
            return "missing_b3_code"
        if any(char.isdigit() for char in code):
            return "alphanumeric_b3_code"
        return "alpha_b3_code"

    def evaluate_report(
        report: dict[str, Any],
        *,
        required_summary_keys: tuple[str, ...],
        required_row_keys: tuple[str, ...],
        accepted_buckets: set[str],
        probe_counter_key: str,
        probe_row_key: str,
        official_candidate_bucket: str,
        official_candidate_eligibility: str,
        official_candidate_evidence: str,
        source_gap_eligibility: str,
        scope_bucket: str,
        scope_evidence: str,
        expected_source_files: dict[str, str],
    ) -> dict[str, Any]:
        meta = report.get("_meta", {})
        summary = report.get("summary", {})
        rows = report.get("rows", [])
        if not isinstance(meta, dict):
            meta = {}
        if not isinstance(summary, dict):
            summary = {}
        if not isinstance(rows, list):
            rows = []
        source_files = meta.get("source_files", {})
        if not isinstance(source_files, dict):
            source_files = {}
        missing_meta_keys = [
            key
            for key in ("generated_at", "source_files", "policy")
            if key not in meta or meta.get(key) in ("", None, {}, [])
        ]
        invalid_generated_at = (
            str(meta.get("generated_at", ""))
            if meta.get("generated_at") and not is_valid_iso_utc_timestamp(str(meta.get("generated_at")))
            else ""
        )
        source_file_mismatches = {
            key: {"expected": expected, "actual": source_files.get(key)}
            for key, expected in expected_source_files.items()
            if source_files.get(key) != expected
        }
        unexpected_source_files = sorted(key for key in source_files if key not in expected_source_files)
        missing_summary_keys = [key for key in required_summary_keys if key not in summary]
        row_count_mismatch = {
            "reported": summary.get("rows", 0),
            "actual": len(rows),
        } if summary.get("rows", 0) != len(rows) else {}
        counters: dict[str, Counter[str]] = {
            "gap_class_totals": Counter(),
            "residual_decision_totals": Counter(),
            "review_bucket_totals": Counter(),
            "review_priority_totals": Counter(),
            "apply_eligibility_totals": Counter(),
            "verification_evidence_required_totals": Counter(),
            probe_counter_key: Counter(),
            "review_strategy_totals": Counter(),
        }
        top_batch_counter: Counter[tuple[str, ...]] = Counter()
        row_gaps: list[dict[str, Any]] = []
        unsafe_apply_rows: list[dict[str, Any]] = []
        for index, row in enumerate(rows):
            if not isinstance(row, dict):
                row_gaps.append({"row_index": index, "reason": "b3_residual_row_is_not_object"})
                continue
            missing_keys = [
                key
                for key in required_row_keys
                if key not in row or (key not in {probe_row_key, "b3_code"} and not row.get(key))
            ]
            invalid_fields: list[str] = []
            listing_key = str(row.get("listing_key", ""))
            review_bucket = str(row.get("review_bucket", ""))
            review_priority = str(row.get("review_priority", ""))
            review_strategy = str(row.get("review_strategy", ""))
            apply_eligibility = str(row.get("apply_eligibility", ""))
            verification_evidence = str(row.get("verification_evidence_required", ""))
            is_sector = probe_row_key == "b3_probe_decision"
            expected_official_source_context = (
                b3_sector_official_source_context(row)
                if is_sector
                else b3_isin_official_source_context(row)
            )
            if not listing_key.startswith("B3::") or row.get("exchange") != "B3":
                invalid_fields.append("listing_key")
            if row.get("review_needed") not in {"true", True}:
                invalid_fields.append("review_needed")
            if review_bucket not in accepted_buckets:
                invalid_fields.append("review_bucket")
            if review_priority not in B3_REVIEW_PRIORITIES:
                invalid_fields.append("review_priority")
            if review_strategy != b3_residual_review_strategy(review_bucket):
                invalid_fields.append("review_strategy")
            if review_bucket == official_candidate_bucket:
                if official_candidate_bucket == "official_isin_candidate_requires_apply_gate" and not (
                    row.get("b3_instruments_equities_isin") or row.get("cotahist_probe_isin")
                ):
                    invalid_fields.append("official_isin")
                if official_candidate_bucket == "official_sector_candidate_requires_apply_gate" and not row.get("b3_sector_update"):
                    invalid_fields.append("b3_sector_update")
                if apply_eligibility != official_candidate_eligibility:
                    invalid_fields.append("apply_eligibility")
                    unsafe_apply_rows.append(
                        {
                            "row_index": index,
                            "listing_key": listing_key,
                            "apply_eligibility": apply_eligibility,
                        }
                    )
                if verification_evidence != official_candidate_evidence:
                    invalid_fields.append("verification_evidence_required")
            elif apply_eligibility.startswith("apply"):
                unsafe_apply_rows.append(
                    {
                        "row_index": index,
                        "listing_key": listing_key,
                        "apply_eligibility": apply_eligibility,
                    }
                )
            if review_bucket == scope_bucket:
                if row.get("source_of_truth_outcome") != "core_exclusion_candidate":
                    invalid_fields.append("source_of_truth_outcome")
                if apply_eligibility != "blocked_until_core_or_extended_scope_decision":
                    invalid_fields.append("apply_eligibility")
                if verification_evidence != scope_evidence:
                    invalid_fields.append("verification_evidence_required")
            if review_bucket.endswith("_source_gap") or review_bucket in {
                "not_in_current_b3_directory_source_gap",
                "fund_or_registry_identifier_source_gap",
                "active_listing_evidence_required_source_gap",
                "no_b3_classification_code_match_source_gap",
                "requires_stronger_official_taxonomy_source_gap",
                "missing_probe_row_requires_parser_or_source_review",
                "ambiguous_b3_code_requires_manual_review",
            }:
                if apply_eligibility != source_gap_eligibility:
                    invalid_fields.append("apply_eligibility")
            if row.get("recommended_next_source") != recommended_next_source_for_b3_bucket(
                review_bucket, is_sector=is_sector
            ):
                invalid_fields.append("recommended_next_source")
            if row.get("source_gate") != source_gate_for_b3_bucket(review_bucket, is_sector=is_sector):
                invalid_fields.append("source_gate")
            if row.get("source_gap_context") != b3_residual_source_gap_context(row):
                invalid_fields.append("source_gap_context")
            if "scope_review_context" in required_row_keys and row.get("scope_review_context") != b3_residual_scope_review_context(row):
                invalid_fields.append("scope_review_context")
            if row.get("official_source_context") != expected_official_source_context:
                invalid_fields.append("official_source_context")
            if row.get("residual_review_context") != b3_residual_review_context(row):
                invalid_fields.append("residual_review_context")
            if missing_keys or invalid_fields:
                row_gaps.append(
                    {
                        "row_index": index,
                        "listing_key": listing_key,
                        "missing_keys": missing_keys,
                        "invalid_fields": invalid_fields,
                        "available_keys": sorted(row)[:25],
                    }
                )
            counters["gap_class_totals"][str(row.get("gap_class", ""))] += 1
            counters["residual_decision_totals"][str(row.get("residual_decision", ""))] += 1
            counters["review_bucket_totals"][review_bucket] += 1
            counters["review_priority_totals"][review_priority] += 1
            counters["apply_eligibility_totals"][apply_eligibility] += 1
            counters["verification_evidence_required_totals"][verification_evidence] += 1
            counters[probe_counter_key][str(row.get(probe_row_key, "") or "missing_probe_row")] += 1
            counters["review_strategy_totals"][b3_residual_review_strategy(review_bucket)] += 1
            if probe_row_key == "b3_probe_decision":
                top_batch_counter[
                    (
                        review_priority,
                        review_bucket,
                        str(row.get("gap_class", "")),
                        b3_code_shape(str(row.get("b3_code", ""))),
                        str(row.get("asset_type", "")),
                    )
                ] += 1
            else:
                top_batch_counter[
                    (
                        review_priority,
                        review_bucket,
                        str(row.get("gap_class", "")),
                        str(row.get("asset_type", "")),
                    )
                ] += 1
        summary_mismatches = {
            key: compare_counter_to_reported(counter, summary.get(key))
            for key, counter in counters.items()
        }
        top_batch_key = (
            "top_b3_sector_review_batches"
            if probe_row_key == "b3_probe_decision"
            else "top_b3_isin_review_batches"
        )
        top_batches = summary.get(top_batch_key)
        top_batch_gaps: list[dict[str, Any]] = []
        if not isinstance(top_batches, list) or not top_batches:
            top_batch_gaps.append({"field": top_batch_key, "reason": "expected_ranked_review_batches"})
        else:
            expected_counts = dict(top_batch_counter)
            for index, batch in enumerate(top_batches):
                if not isinstance(batch, dict):
                    top_batch_gaps.append({"row_index": index, "reason": "expected_object"})
                    continue
                review_bucket = str(batch.get("review_bucket", ""))
                batch_tuple = (
                    str(batch.get("review_priority", "")),
                    review_bucket,
                    str(batch.get("gap_class", "")),
                    str(batch.get("b3_code_shape", "")),
                    str(batch.get("asset_type", "")),
                ) if probe_row_key == "b3_probe_decision" else (
                    str(batch.get("review_priority", "")),
                    review_bucket,
                    str(batch.get("gap_class", "")),
                    str(batch.get("asset_type", "")),
                )
                if expected_counts.get(batch_tuple) != batch.get("rows"):
                    top_batch_gaps.append(
                        {
                            "row_index": index,
                            "field": top_batch_key,
                            "reported": batch.get("rows"),
                            "expected": expected_counts.get(batch_tuple),
                        }
                    )
                expected_strategy = b3_residual_review_strategy(review_bucket)
                if batch.get("review_strategy") != expected_strategy:
                    top_batch_gaps.append(
                        {
                            "row_index": index,
                            "field": "review_strategy",
                            "reported": batch.get("review_strategy"),
                            "expected": expected_strategy,
                        }
                    )
                if batch.get("verification_evidence_required") != verification_evidence_for_b3_bucket(
                    review_bucket,
                    official_candidate_bucket=official_candidate_bucket,
                    official_candidate_evidence=official_candidate_evidence,
                    scope_bucket=scope_bucket,
                    scope_evidence=scope_evidence,
                ):
                    top_batch_gaps.append(
                        {
                            "row_index": index,
                            "field": "verification_evidence_required",
                            "reported": batch.get("verification_evidence_required"),
                        }
                    )
                is_sector = probe_row_key == "b3_probe_decision"
                expected_next_source = recommended_next_source_for_b3_bucket(review_bucket, is_sector=is_sector)
                expected_source_gate = source_gate_for_b3_bucket(review_bucket, is_sector=is_sector)
                if batch.get("recommended_next_source") != expected_next_source:
                    top_batch_gaps.append(
                        {
                            "row_index": index,
                            "field": "recommended_next_source",
                            "reported": batch.get("recommended_next_source"),
                            "expected": expected_next_source,
                        }
                    )
                if batch.get("source_gate") != expected_source_gate:
                    top_batch_gaps.append(
                        {
                            "row_index": index,
                            "field": "source_gate",
                            "reported": batch.get("source_gate"),
                            "expected": expected_source_gate,
                        }
                    )
        if probe_row_key == "b3_probe_decision":
            code_shape_counter = Counter(
                b3_code_shape(str(row.get("b3_code", "")))
                for row in rows
                if isinstance(row, dict)
            )
            shape_mismatch = compare_counter_to_reported(code_shape_counter, summary.get("b3_code_shape_totals"))
            if shape_mismatch:
                summary_mismatches["b3_code_shape_totals"] = shape_mismatch
            alphanumeric_rows = [
                row
                for row in rows
                if isinstance(row, dict) and b3_code_shape(str(row.get("b3_code", ""))) == "alphanumeric_b3_code"
            ]
            if summary.get("alphanumeric_b3_code_rows") != len(alphanumeric_rows):
                summary_mismatches["alphanumeric_b3_code_rows"] = {
                    "reported": summary.get("alphanumeric_b3_code_rows"),
                    "actual": len(alphanumeric_rows),
                }
            examples = summary.get("alphanumeric_b3_code_examples")
            if not isinstance(examples, list):
                summary_mismatches["alphanumeric_b3_code_examples"] = {"reported": examples, "actual": "list_required"}
            else:
                invalid_examples = [
                    index
                    for index, example in enumerate(examples)
                    if not isinstance(example, dict)
                    or b3_code_shape(str(example.get("b3_code", ""))) != "alphanumeric_b3_code"
                    or not str(example.get("listing_key", "")).startswith("B3::")
                    or not example.get("b3_probe_decision")
                ]
                if invalid_examples:
                    summary_mismatches["alphanumeric_b3_code_examples"] = {"invalid_example_indexes": invalid_examples}
        summary_mismatches = {key: value for key, value in summary_mismatches.items() if value}
        policy = summary.get("policy", {})
        missing_policy_keys = [
            key
            for key in B3_REQUIRED_POLICY_KEYS
            if not isinstance(policy, dict) or not policy.get(key)
        ]
        return {
            "passed": (
                bool(rows)
                and not missing_meta_keys
                and not invalid_generated_at
                and not source_file_mismatches
                and not unexpected_source_files
                and not missing_summary_keys
                and not row_count_mismatch
                and not row_gaps
                and not unsafe_apply_rows
                and not summary_mismatches
                and not top_batch_gaps
                and not missing_policy_keys
            ),
            "rows": len(rows),
            "missing_meta_keys": missing_meta_keys,
            "invalid_generated_at": invalid_generated_at,
            "source_file_mismatches": source_file_mismatches,
            "unexpected_source_files": unexpected_source_files,
            "missing_summary_keys": missing_summary_keys,
            "row_count_mismatch": row_count_mismatch,
            "row_gaps": row_gaps[:20],
            "unsafe_apply_rows": unsafe_apply_rows[:20],
            "summary_mismatches": summary_mismatches,
            "top_batch_gaps": top_batch_gaps[:20],
            "missing_policy_keys": missing_policy_keys,
            "accepted_review_buckets": sorted(accepted_buckets),
        }

    isin = evaluate_report(
        isin_report,
        required_summary_keys=REQUIRED_B3_ISIN_SUMMARY_KEYS,
        required_row_keys=REQUIRED_B3_ISIN_ROW_KEYS,
        accepted_buckets=B3_ISIN_REVIEW_BUCKETS,
        probe_counter_key="cotahist_probe_decision_totals",
        probe_row_key="cotahist_probe_decision",
        official_candidate_bucket="official_isin_candidate_requires_apply_gate",
        official_candidate_eligibility="apply_only_after_listing_key_and_checksum_validation",
        official_candidate_evidence="official_b3_or_cotahist_isin_with_exact_listing_key_name_and_isin_checksum",
        source_gap_eligibility="source_gap_keep_blank_until_official_identifier_evidence",
        scope_bucket="scope_review_before_identifier_fill",
        scope_evidence="scope_decision_for_core_extended_or_exclude_before_identifier_fill",
        expected_source_files={
            "tickers_csv": "data/tickers.csv",
            "instrument_scopes_csv": "data/instrument_scopes.csv",
            "source_gap_classification_csv": "data/reports/source_gap_classification.csv",
            "source_of_truth_decisions_csv": "data/reports/source_of_truth_decisions.csv",
            "masterfile_reference_csv": "data/masterfiles/reference.csv",
            "cotahist_probe_csv": "data/b3_verification/cotahist_isin_probe_current.csv",
        },
    )
    sector = evaluate_report(
        sector_report,
        required_summary_keys=REQUIRED_B3_SECTOR_SUMMARY_KEYS,
        required_row_keys=REQUIRED_B3_SECTOR_ROW_KEYS,
        accepted_buckets=B3_SECTOR_REVIEW_BUCKETS,
        probe_counter_key="b3_probe_decision_totals",
        probe_row_key="b3_probe_decision",
        official_candidate_bucket="official_sector_candidate_requires_apply_gate",
        official_candidate_eligibility="apply_only_after_listing_key_taxonomy_and_canonical_sector_validation",
        official_candidate_evidence="official_b3_taxonomy_with_exact_listing_key_b3_code_and_canonical_sector_mapping",
        source_gap_eligibility="source_gap_keep_blank_until_official_taxonomy_evidence",
        scope_bucket="scope_review_before_sector_fill",
        scope_evidence="scope_decision_for_core_extended_or_exclude_before_sector_fill",
        expected_source_files={
            "tickers_csv": "data/tickers.csv",
            "source_gap_classification_csv": "data/reports/source_gap_classification.csv",
            "source_of_truth_decisions_csv": "data/reports/source_of_truth_decisions.csv",
            "b3_sector_probe_csv": "data/b3_verification/sector_classification_backfill.csv",
        },
    )
    return {
        "passed": isin["passed"] and sector["passed"],
        "isin": isin,
        "sector": sector,
        "rows": {
            "isin": isin["rows"],
            "sector": sector["rows"],
            "total": isin["rows"] + sector["rows"],
        },
    }


def evaluate_weak_sector_residual_gate(weak_sector_report: dict[str, Any]) -> dict[str, Any]:
    expected_source_files = {
        "tickers_csv": "data/tickers.csv",
        "masterfile_reference_csv": "data/masterfiles/reference.csv",
        "source_gap_classification_csv": "data/reports/source_gap_classification.csv",
        "source_of_truth_decisions_csv": "data/reports/source_of_truth_decisions.csv",
    }
    meta = weak_sector_report.get("_meta", {})
    summary = weak_sector_report.get("summary", {})
    rows = weak_sector_report.get("rows", [])
    if not isinstance(meta, dict):
        meta = {}
    if not isinstance(summary, dict):
        summary = {}
    if not isinstance(rows, list):
        rows = []
    source_files = meta.get("source_files", {})
    if not isinstance(source_files, dict):
        source_files = {}
    missing_meta_keys = [
        key
        for key in ("generated_at", "source_files", "exchanges", "policy")
        if key not in meta or meta.get(key) in ("", None, {}, [])
    ]
    invalid_generated_at = (
        str(meta.get("generated_at", ""))
        if meta.get("generated_at") and not is_valid_iso_utc_timestamp(str(meta.get("generated_at")))
        else ""
    )
    source_file_mismatches = {
        key: {"expected": expected, "actual": source_files.get(key)}
        for key, expected in expected_source_files.items()
        if source_files.get(key) != expected
    }
    unexpected_source_files = sorted(key for key in source_files if key not in expected_source_files)
    meta_exchange_mismatch = {}
    if meta.get("exchanges") != sorted(WEAK_SECTOR_EXCHANGES):
        meta_exchange_mismatch = {
            "reported": meta.get("exchanges"),
            "expected": sorted(WEAK_SECTOR_EXCHANGES),
        }
    missing_summary_keys = [key for key in REQUIRED_WEAK_SECTOR_SUMMARY_KEYS if key not in summary]
    row_count_mismatch = {
        "reported": summary.get("rows", 0),
        "actual": len(rows),
    } if summary.get("rows", 0) != len(rows) else {}
    counters: dict[str, Counter[str]] = {
        "exchange_totals": Counter(),
        "official_sector_candidate_exchange_totals": Counter(),
        "official_sector_candidate_value_totals": Counter(),
        "scope_review_exchange_totals": Counter(),
        "scope_review_gap_class_totals": Counter(),
        "masterfile_without_sector_exchange_totals": Counter(),
        "gap_class_totals": Counter(),
        "source_of_truth_outcome_totals": Counter(),
        "weak_sector_resolution_queue_totals": Counter(),
        "residual_decision_totals": Counter(),
        "review_bucket_totals": Counter(),
        "review_priority_totals": Counter(),
        "apply_eligibility_totals": Counter(),
        "verification_evidence_required_totals": Counter(),
        "official_masterfile_match_totals": Counter(),
        "official_masterfile_exposes_sector_totals": Counter(),
        "official_masterfile_source_totals": Counter(),
        "official_sector_value_totals": Counter(),
    }
    observed_exchanges: set[str] = set()
    row_gaps: list[dict[str, Any]] = []
    unsafe_sector_apply_rows: list[dict[str, Any]] = []
    weak_sector_queue_strategy_counter: dict[str, Counter[str]] = {}
    weak_sector_queue_official_capability_counter: dict[str, Counter[str]] = {}
    weak_sector_review_batches: Counter[tuple[str, str, str]] = Counter()
    venue_backlog_exchange_queue_counter: dict[str, Counter[str]] = {}
    venue_backlog_exchange_official_capability_counter: dict[str, Counter[str]] = {}
    venue_backlog_batches: Counter[tuple[str, str, str]] = Counter()
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            row_gaps.append({"row_index": index, "reason": "weak_sector_row_is_not_object"})
            continue
        missing_keys = [
            key
            for key in REQUIRED_WEAK_SECTOR_ROW_KEYS
            if key not in row or (key != "official_masterfile_sources" and not row.get(key))
        ]
        invalid_fields: list[str] = []
        listing_key = str(row.get("listing_key", ""))
        exchange = str(row.get("exchange", ""))
        review_bucket = str(row.get("review_bucket", ""))
        review_priority = str(row.get("review_priority", ""))
        apply_eligibility = str(row.get("apply_eligibility", ""))
        source_outcome = str(row.get("source_of_truth_outcome", ""))
        verification_evidence = str(row.get("verification_evidence_required", ""))
        recommended_action = str(row.get("recommended_next_action", ""))
        official_match = str(row.get("official_masterfile_match", ""))
        official_exposes_sector = str(row.get("official_masterfile_exposes_sector", ""))
        official_sector_values = str(row.get("official_masterfile_sector_values", ""))
        official_sources = str(row.get("official_masterfile_sources", ""))
        official_source_context = str(row.get("official_source_context", ""))
        official_capability = str(row.get("official_capability", ""))
        official_source_label = "|".join(source for source in official_sources.split("|") if source) or "none"
        queue = str(row.get("weak_sector_resolution_queue", ""))
        strategy, _evidence_required = weak_sector_review_strategy_for(queue)
        row_strategy = str(row.get("review_strategy", ""))
        expected_source_context = (
            f"official_masterfile_sources={official_source_label};"
            f"official_sector_values={official_sector_values or 'none'}"
        )
        expected_capability = f"masterfile_match={official_match};masterfile_exposes_sector={official_exposes_sector}"
        if exchange:
            observed_exchanges.add(exchange)
        if not listing_key.startswith(f"{exchange}::") or exchange not in WEAK_SECTOR_EXCHANGES:
            invalid_fields.append("listing_key")
        if review_bucket not in WEAK_SECTOR_REVIEW_BUCKETS:
            invalid_fields.append("review_bucket")
        if review_priority not in WEAK_SECTOR_REVIEW_PRIORITIES:
            invalid_fields.append("review_priority")
        if row.get("review_needed") not in {"true", True}:
            invalid_fields.append("review_needed")
        if review_bucket == "official_sector_candidate_requires_normalization_gate":
            if official_exposes_sector != "true":
                invalid_fields.append("official_masterfile_exposes_sector")
            if not official_sector_values:
                invalid_fields.append("official_masterfile_sector_values")
            if apply_eligibility != "blocked_until_canonical_sector_normalization_and_listing_key_gate":
                invalid_fields.append("apply_eligibility")
                unsafe_sector_apply_rows.append(
                    {
                        "row_index": index,
                        "listing_key": listing_key,
                        "apply_eligibility": apply_eligibility,
                    }
                )
            if verification_evidence != "official_venue_sector_value_with_canonical_mapping_and_exact_listing_key_match":
                invalid_fields.append("verification_evidence_required")
            if recommended_action != "apply_only_after_official_sector_normalization_and_listing_key_gate":
                invalid_fields.append("recommended_next_action")
        if review_bucket == "scope_review_before_sector_fill":
            if source_outcome != "core_exclusion_candidate":
                invalid_fields.append("source_of_truth_outcome")
            if apply_eligibility != "blocked_until_core_or_extended_scope_decision":
                invalid_fields.append("apply_eligibility")
            if verification_evidence != "scope_decision_for_core_extended_or_exclude_before_sector_enrichment":
                invalid_fields.append("verification_evidence_required")
        if review_bucket == "official_masterfile_without_sector_source_gap":
            if official_match != "true":
                invalid_fields.append("official_masterfile_match")
            if official_exposes_sector != "false":
                invalid_fields.append("official_masterfile_exposes_sector")
            if apply_eligibility != "keep_sector_blank_until_official_masterfile_exposes_sector_or_reviewed_taxonomy":
                invalid_fields.append("apply_eligibility")
            if "keep_sector_blank" not in recommended_action:
                invalid_fields.append("recommended_next_action")
        if review_bucket == "venue_official_taxonomy_unavailable_source_gap":
            if apply_eligibility != "keep_sector_blank_until_venue_official_taxonomy_source_exists":
                invalid_fields.append("apply_eligibility")
            if "keep_sector_blank" not in recommended_action:
                invalid_fields.append("recommended_next_action")
        if review_bucket == "venue_specific_sector_source_gap":
            if apply_eligibility != "keep_sector_blank_until_reviewed_venue_specific_source":
                invalid_fields.append("apply_eligibility")
            if "keep_sector_blank" not in recommended_action:
                invalid_fields.append("recommended_next_action")
        if apply_eligibility.startswith("apply") and review_bucket != "official_sector_candidate_requires_normalization_gate":
            unsafe_sector_apply_rows.append(
                {
                    "row_index": index,
                    "listing_key": listing_key,
                    "apply_eligibility": apply_eligibility,
                }
            )
        if row_strategy not in WEAK_SECTOR_REVIEW_STRATEGIES or row_strategy != strategy:
            invalid_fields.append("review_strategy")
        if row.get("recommended_next_source") != weak_sector_recommended_next_source_for(queue, official_source_label):
            invalid_fields.append("recommended_next_source")
        if row.get("source_gate") != weak_sector_source_gate_for(queue):
            invalid_fields.append("source_gate")
        if official_source_context != expected_source_context:
            invalid_fields.append("official_source_context")
        if official_capability != expected_capability:
            invalid_fields.append("official_capability")
        if missing_keys or invalid_fields:
            row_gaps.append(
                {
                    "row_index": index,
                    "listing_key": listing_key,
                    "missing_keys": missing_keys,
                    "invalid_fields": invalid_fields,
                    "available_keys": sorted(row)[:25],
                }
            )
        counters["exchange_totals"][exchange] += 1
        if review_bucket == "official_sector_candidate_requires_normalization_gate":
            counters["official_sector_candidate_exchange_totals"][exchange] += 1
            for value in official_sector_values.split("|"):
                if value:
                    counters["official_sector_candidate_value_totals"][value] += 1
        if review_bucket == "scope_review_before_sector_fill":
            counters["scope_review_exchange_totals"][exchange] += 1
            counters["scope_review_gap_class_totals"][str(row.get("gap_class", ""))] += 1
        if review_bucket == "official_masterfile_without_sector_source_gap":
            counters["masterfile_without_sector_exchange_totals"][exchange] += 1
        counters["gap_class_totals"][str(row.get("gap_class", ""))] += 1
        counters["source_of_truth_outcome_totals"][source_outcome] += 1
        counters["weak_sector_resolution_queue_totals"][str(row.get("weak_sector_resolution_queue", ""))] += 1
        venue_backlog_exchange_queue_counter.setdefault(exchange, Counter())[queue] += 1
        weak_sector_queue_strategy_counter.setdefault(queue, Counter())[strategy] += 1
        counters["residual_decision_totals"][str(row.get("residual_decision", ""))] += 1
        counters["review_bucket_totals"][review_bucket] += 1
        counters["review_priority_totals"][review_priority] += 1
        counters["apply_eligibility_totals"][apply_eligibility] += 1
        counters["verification_evidence_required_totals"][verification_evidence] += 1
        counters["official_masterfile_match_totals"][official_match] += 1
        counters["official_masterfile_exposes_sector_totals"][official_exposes_sector] += 1
        weak_sector_queue_official_capability_counter.setdefault(queue, Counter())[
            f"masterfile_match={official_match}"
        ] += 1
        weak_sector_queue_official_capability_counter.setdefault(queue, Counter())[
            f"masterfile_exposes_sector={official_exposes_sector}"
        ] += 1
        venue_backlog_exchange_official_capability_counter.setdefault(exchange, Counter())[
            f"masterfile_match={official_match}"
        ] += 1
        venue_backlog_exchange_official_capability_counter.setdefault(exchange, Counter())[
            f"masterfile_exposes_sector={official_exposes_sector}"
        ] += 1
        sources = (
            str(row.get("official_masterfile_sources", "")).split("|")
            if str(row.get("official_masterfile_sources", ""))
            else ["none"]
        )
        for source in sources:
            if source:
                weak_sector_review_batches[(queue, exchange, source)] += 1
                venue_backlog_batches[(exchange, queue, source)] += 1
            if source and source != "none":
                counters["official_masterfile_source_totals"][source] += 1
        for value in official_sector_values.split("|"):
            if value:
                counters["official_sector_value_totals"][value] += 1
    summary_mismatches = {
        key: compare_counter_to_reported(counter, summary.get(key))
        for key, counter in counters.items()
    }
    summary_mismatches = {key: value for key, value in summary_mismatches.items() if value}
    expected_resolution_exchange_totals = {
        queue: dict(
            sorted(
                Counter(
                    str(row.get("exchange", ""))
                    for row in rows
                    if isinstance(row, dict) and str(row.get("weak_sector_resolution_queue", "")) == queue
                ).items()
            )
        )
        for queue in sorted(counters["weak_sector_resolution_queue_totals"])
    }
    if summary.get("weak_sector_resolution_queue_exchange_totals") != expected_resolution_exchange_totals:
        summary_mismatches["weak_sector_resolution_queue_exchange_totals"] = {
            "reported": summary.get("weak_sector_resolution_queue_exchange_totals"),
            "actual": expected_resolution_exchange_totals,
        }
    expected_resolution_gap_class_totals = {
        queue: dict(
            sorted(
                Counter(
                    str(row.get("gap_class", ""))
                    for row in rows
                    if isinstance(row, dict) and str(row.get("weak_sector_resolution_queue", "")) == queue
                ).items()
            )
        )
        for queue in sorted(counters["weak_sector_resolution_queue_totals"])
    }
    if summary.get("weak_sector_resolution_queue_gap_class_totals") != expected_resolution_gap_class_totals:
        summary_mismatches["weak_sector_resolution_queue_gap_class_totals"] = {
            "reported": summary.get("weak_sector_resolution_queue_gap_class_totals"),
            "actual": expected_resolution_gap_class_totals,
        }
    expected_resolution_official_source_totals = {
        queue: dict(
            sorted(
                Counter(
                    source
                    for row in rows
                    if isinstance(row, dict) and str(row.get("weak_sector_resolution_queue", "")) == queue
                    for source in (
                        str(row.get("official_masterfile_sources", "")).split("|")
                        if str(row.get("official_masterfile_sources", ""))
                        else ["none"]
                    )
                    if source
                ).items()
            )
        )
        for queue in sorted(counters["weak_sector_resolution_queue_totals"])
    }
    if summary.get("weak_sector_resolution_queue_official_source_totals") != expected_resolution_official_source_totals:
        summary_mismatches["weak_sector_resolution_queue_official_source_totals"] = {
            "reported": summary.get("weak_sector_resolution_queue_official_source_totals"),
            "actual": expected_resolution_official_source_totals,
        }
    expected_resolution_strategy_totals = {
        queue: dict(sorted(counter.items()))
        for queue, counter in sorted(weak_sector_queue_strategy_counter.items())
    }
    if summary.get("weak_sector_resolution_queue_review_strategy_totals") != expected_resolution_strategy_totals:
        summary_mismatches["weak_sector_resolution_queue_review_strategy_totals"] = {
            "reported": summary.get("weak_sector_resolution_queue_review_strategy_totals"),
            "actual": expected_resolution_strategy_totals,
        }
    expected_resolution_capability_totals = {
        queue: dict(sorted(counter.items()))
        for queue, counter in sorted(weak_sector_queue_official_capability_counter.items())
    }
    if summary.get("weak_sector_resolution_queue_official_capability_totals") != expected_resolution_capability_totals:
        summary_mismatches["weak_sector_resolution_queue_official_capability_totals"] = {
            "reported": summary.get("weak_sector_resolution_queue_official_capability_totals"),
            "actual": expected_resolution_capability_totals,
        }
    expected_venue_backlog_exchange_queue_totals = {
        exchange: dict(sorted(counter.items()))
        for exchange, counter in sorted(venue_backlog_exchange_queue_counter.items())
    }
    if summary.get("venue_backlog_exchange_queue_totals") != expected_venue_backlog_exchange_queue_totals:
        summary_mismatches["venue_backlog_exchange_queue_totals"] = {
            "reported": summary.get("venue_backlog_exchange_queue_totals"),
            "actual": expected_venue_backlog_exchange_queue_totals,
        }
    expected_venue_backlog_exchange_capability_totals = {
        exchange: dict(sorted(counter.items()))
        for exchange, counter in sorted(venue_backlog_exchange_official_capability_counter.items())
    }
    if summary.get("venue_backlog_exchange_official_capability_totals") != expected_venue_backlog_exchange_capability_totals:
        summary_mismatches["venue_backlog_exchange_official_capability_totals"] = {
            "reported": summary.get("venue_backlog_exchange_official_capability_totals"),
            "actual": expected_venue_backlog_exchange_capability_totals,
        }
    top_weak_sector_batch_gaps: list[dict[str, Any]] = []

    def validate_top_weak_sector_batches(field: str) -> None:
        batches = summary.get(field)
        if not isinstance(batches, list) or not batches:
            top_weak_sector_batch_gaps.append({"field": field, "reason": "expected_ranked_review_batches"})
            return
        for index, batch in enumerate(batches):
            if not isinstance(batch, dict):
                top_weak_sector_batch_gaps.append({"field": field, "row_index": index, "reason": "expected_object"})
                continue
            queue = str(batch.get("weak_sector_resolution_queue", ""))
            exchange = str(batch.get("exchange", ""))
            strategy = str(batch.get("review_strategy", ""))
            missing_keys = [
                key
                for key in (
                    "weak_sector_resolution_queue",
                    "exchange",
                    "official_source",
                    "rows",
                    "review_strategy",
                    "evidence_required",
                    "recommended_next_source",
                    "source_gate",
                )
                if key not in batch
            ]
            invalid_fields = []
            expected_strategy, _expected_evidence = weak_sector_review_strategy_for(queue)
            if queue not in weak_sector_queue_strategy_counter:
                invalid_fields.append("weak_sector_resolution_queue")
            if exchange not in WEAK_SECTOR_EXCHANGES:
                invalid_fields.append("exchange")
            if not isinstance(batch.get("rows"), int) or batch.get("rows", 0) <= 0:
                invalid_fields.append("rows")
            if strategy not in WEAK_SECTOR_REVIEW_STRATEGIES or strategy != expected_strategy:
                invalid_fields.append("review_strategy")
            if not batch.get("official_source"):
                invalid_fields.append("official_source")
            if not batch.get("evidence_required") or batch.get("evidence_required") in {"none", "ticker_match_only"}:
                invalid_fields.append("evidence_required")
            if not batch.get("recommended_next_source"):
                invalid_fields.append("recommended_next_source")
            if not batch.get("source_gate"):
                invalid_fields.append("source_gate")
            if missing_keys or invalid_fields:
                top_weak_sector_batch_gaps.append(
                    {"field": field, "row_index": index, "missing_keys": missing_keys, "invalid_fields": invalid_fields}
                )

    validate_top_weak_sector_batches("top_weak_sector_resolution_review_batches")
    validate_top_weak_sector_batches("top_venue_backlog_batches")
    policy = summary.get("policy", {})
    missing_policy_keys = [
        key
        for key in WEAK_SECTOR_REQUIRED_POLICY_KEYS
        if not isinstance(policy, dict) or not policy.get(key)
    ]
    reported_exchanges = summary.get("exchanges", [])
    queue_count_mismatches = {}
    expected_queue_counts = {
        "official_sector_candidate_rows": counters["review_bucket_totals"].get(
            "official_sector_candidate_requires_normalization_gate", 0
        ),
        "scope_review_rows": counters["review_bucket_totals"].get("scope_review_before_sector_fill", 0),
        "masterfile_without_sector_rows": counters["review_bucket_totals"].get(
            "official_masterfile_without_sector_source_gap", 0
        ),
    }
    for key, expected in expected_queue_counts.items():
        if summary.get(key) != expected:
            queue_count_mismatches[key] = {"reported": summary.get(key), "actual": expected}
    reported_exchange_mismatch = {}
    if reported_exchanges != sorted(WEAK_SECTOR_EXCHANGES) or observed_exchanges != WEAK_SECTOR_EXCHANGES:
        reported_exchange_mismatch = {
            "reported": reported_exchanges,
            "observed": sorted(observed_exchanges),
            "expected": sorted(WEAK_SECTOR_EXCHANGES),
        }
    missing_required_buckets = [
        bucket
        for bucket in (
            "official_sector_candidate_requires_normalization_gate",
            "scope_review_before_sector_fill",
            "official_masterfile_without_sector_source_gap",
            "venue_official_taxonomy_unavailable_source_gap",
        )
        if bucket not in counters["review_bucket_totals"]
    ]
    return {
        "passed": (
            bool(rows)
            and not missing_meta_keys
            and not invalid_generated_at
            and not source_file_mismatches
            and not unexpected_source_files
            and not meta_exchange_mismatch
            and not missing_summary_keys
            and not row_count_mismatch
            and not row_gaps
            and not unsafe_sector_apply_rows
            and not summary_mismatches
            and not top_weak_sector_batch_gaps
            and not missing_policy_keys
            and not queue_count_mismatches
            and not reported_exchange_mismatch
            and not missing_required_buckets
        ),
        "rows": len(rows),
        "missing_meta_keys": missing_meta_keys,
        "invalid_generated_at": invalid_generated_at,
        "source_file_mismatches": source_file_mismatches,
        "unexpected_source_files": unexpected_source_files,
        "meta_exchange_mismatch": meta_exchange_mismatch,
        "missing_summary_keys": missing_summary_keys,
        "row_count_mismatch": row_count_mismatch,
        "row_gaps": row_gaps[:20],
        "unsafe_sector_apply_rows": unsafe_sector_apply_rows[:20],
        "summary_mismatches": summary_mismatches,
        "top_weak_sector_batch_gaps": top_weak_sector_batch_gaps,
        "queue_count_mismatches": queue_count_mismatches,
        "missing_policy_keys": missing_policy_keys,
        "reported_exchange_mismatch": reported_exchange_mismatch,
        "missing_required_buckets": missing_required_buckets,
        "required_row_keys": list(REQUIRED_WEAK_SECTOR_ROW_KEYS),
        "accepted_exchanges": sorted(WEAK_SECTOR_EXCHANGES),
        "accepted_review_buckets": sorted(WEAK_SECTOR_REVIEW_BUCKETS),
    }


def evaluate_before_after_delta_matrix(deltas: dict[str, Any]) -> dict[str, Any]:
    meta = deltas.get("_meta", {})
    if not isinstance(meta, dict):
        meta = {}
    source_files = meta.get("source_files", {})
    if not isinstance(source_files, dict):
        source_files = {}
    missing_meta_keys = [
        key
        for key in ("generated_at", "baseline_generated_at", "current_generated_at", "baseline_path", "source_files", "policy")
        if key not in meta or meta.get(key) in ("", None, {}, [])
    ]
    invalid_generated_at = (
        str(meta.get("generated_at", ""))
        if meta.get("generated_at") and not is_valid_iso_utc_timestamp(str(meta.get("generated_at")))
        else ""
    )
    invalid_baseline_generated_at = (
        str(meta.get("baseline_generated_at", ""))
        if meta.get("baseline_generated_at") and not is_valid_iso_utc_timestamp(str(meta.get("baseline_generated_at")))
        else ""
    )
    invalid_current_generated_at = (
        str(meta.get("current_generated_at", ""))
        if meta.get("current_generated_at") and not is_valid_iso_utc_timestamp(str(meta.get("current_generated_at")))
        else ""
    )
    baseline_path_mismatch = meta.get("baseline_path") != "data/reports/improvement_baseline.json"
    source_file_mismatches = {
        key: {"expected": expected, "actual": source_files.get(key)}
        for key, expected in EXPECTED_DELTA_SOURCE_FILES.items()
        if source_files.get(key) != expected
    }
    unexpected_source_files = sorted(key for key in source_files if key not in EXPECTED_DELTA_SOURCE_FILES)
    policy_missing_markers = [
        marker
        for marker in ("Delta report only", "source-level review", "before any data change is inferred")
        if marker not in str(meta.get("policy", ""))
    ]
    summary = deltas.get("summary", {})
    matrix = deltas.get("acceptance_delta_matrix", {})
    missing_keys = [key for key in REQUIRED_DELTA_KEYS if key not in matrix]
    missing_delta_contexts = [
        key
        for key in REQUIRED_DELTA_KEYS
        if isinstance(matrix.get(key), dict) and not matrix[key].get("delta_context")
    ]
    invalid_delta_contexts = {
        key: {"expected": delta_context(key, matrix[key]), "actual": matrix[key].get("delta_context", "")}
        for key in REQUIRED_DELTA_KEYS
        if isinstance(matrix.get(key), dict)
        and matrix[key].get("delta_context")
        and matrix[key].get("delta_context") != delta_context(key, matrix[key])
    }
    non_zero_deltas = [
        key
        for key in REQUIRED_DELTA_KEYS
        if key in matrix and matrix[key].get("delta") != 0
    ]
    changed_exchange_rows = summary.get("changed_exchange_rows")
    changed_numeric_delta_rows = summary.get("changed_numeric_delta_rows")
    exchange_matrix = deltas.get("exchange_acceptance_delta_matrix", {})
    invalid_exchange_delta_contexts: dict[str, dict[str, dict[str, str]]] = {}
    missing_exchange_delta_contexts: dict[str, list[str]] = {}
    if isinstance(exchange_matrix, dict):
        for exchange, metrics in exchange_matrix.items():
            if not isinstance(metrics, dict):
                continue
            for metric, row in metrics.items():
                if not isinstance(row, dict):
                    continue
                actual = row.get("delta_context", "")
                if not actual:
                    missing_exchange_delta_contexts.setdefault(str(exchange), []).append(str(metric))
                    continue
                expected = delta_context(str(metric), row, str(exchange))
                if actual != expected:
                    invalid_exchange_delta_contexts.setdefault(str(exchange), {})[str(metric)] = {
                        "expected": expected,
                        "actual": str(actual),
                    }
    return {
        "passed": (
            not missing_meta_keys
            and not invalid_generated_at
            and not invalid_baseline_generated_at
            and not invalid_current_generated_at
            and not baseline_path_mismatch
            and not source_file_mismatches
            and not unexpected_source_files
            and not policy_missing_markers
            and not missing_keys
            and not missing_delta_contexts
            and not invalid_delta_contexts
            and not missing_exchange_delta_contexts
            and not invalid_exchange_delta_contexts
            and not non_zero_deltas
        ),
        "missing_meta_keys": missing_meta_keys,
        "invalid_generated_at": invalid_generated_at,
        "invalid_baseline_generated_at": invalid_baseline_generated_at,
        "invalid_current_generated_at": invalid_current_generated_at,
        "baseline_path_mismatch": baseline_path_mismatch,
        "source_file_mismatches": source_file_mismatches,
        "unexpected_source_files": unexpected_source_files,
        "policy_missing_markers": policy_missing_markers,
        "acceptance_delta_matrix": matrix,
        "missing_keys": missing_keys,
        "missing_delta_contexts": missing_delta_contexts,
        "invalid_delta_contexts": invalid_delta_contexts,
        "missing_exchange_delta_contexts": missing_exchange_delta_contexts,
        "invalid_exchange_delta_contexts": invalid_exchange_delta_contexts,
        "non_zero_deltas": non_zero_deltas,
        "changed_exchange_rows": changed_exchange_rows,
        "changed_numeric_delta_rows": changed_numeric_delta_rows,
        "changed_rows_policy": "Allowed when critical acceptance deltas remain zero; review/freshness/masterfile deltas are documented in improvement_deltas.",
    }


def evaluate_campaign_acceptance_matrices(campaigns: dict[str, Any]) -> dict[str, Any]:
    rows = campaigns.get("campaigns", [])
    if not isinstance(rows, list):
        rows = []
    missing_matrices: list[str] = []
    missing_required_metrics: dict[str, list[str]] = {}
    missing_exchange_scopes: list[str] = []
    missing_affected_rows: list[str] = []
    missing_campaign_metric_deltas: list[str] = []
    missing_global_acceptance_deltas: dict[str, list[str]] = {}
    missing_exchange_scope_delta_totals: dict[str, list[str]] = {}
    missing_exchange_scope_delta_fields: dict[str, list[str]] = {}
    missing_before_after_summaries: list[str] = []
    missing_before_after_fields: dict[str, list[str]] = {}
    before_after_mismatches: list[dict[str, Any]] = []
    exchange_scope_delta_consistency_gaps: list[dict[str, Any]] = []
    b3_campaign_evidence_gaps: list[dict[str, Any]] = []
    otc_campaign_evidence_gaps: list[dict[str, Any]] = []
    canada_campaign_evidence_gaps: list[dict[str, Any]] = []
    asx_campaign_evidence_gaps: list[dict[str, Any]] = []
    weak_sector_campaign_evidence_gaps: list[dict[str, Any]] = []
    masterfile_collision_campaign_evidence_gaps: list[dict[str, Any]] = []
    symbol_change_campaign_evidence_gaps: list[dict[str, Any]] = []
    ohlcv_campaign_evidence_gaps: list[dict[str, Any]] = []
    freshness_campaign_evidence_gaps: list[dict[str, Any]] = []
    baseline_campaign_evidence_gaps: list[dict[str, Any]] = []
    for index, campaign in enumerate(rows, start=1):
        if not isinstance(campaign, dict):
            missing_matrices.append(f"row_{index}")
            continue
        campaign_key = str(campaign.get("campaign_key") or campaign.get("name") or f"row_{index}")
        matrix = campaign.get("acceptance_matrix")
        if not isinstance(matrix, dict):
            missing_matrices.append(campaign_key)
            continue
        required_metrics = matrix.get("required_metrics", [])
        missing_metrics = [key for key in REQUIRED_DELTA_KEYS if key not in required_metrics]
        if missing_metrics:
            missing_required_metrics[campaign_key] = missing_metrics
        if "exchange_scope" not in matrix:
            missing_exchange_scopes.append(campaign_key)
        affected_rows = matrix.get("affected_artifact_rows")
        if not isinstance(affected_rows, int) or isinstance(affected_rows, bool) or affected_rows < 0:
            missing_affected_rows.append(campaign_key)
        if not isinstance(matrix.get("campaign_metric_deltas"), dict):
            missing_campaign_metric_deltas.append(campaign_key)
        global_deltas = matrix.get("global_acceptance_deltas")
        if not isinstance(global_deltas, dict):
            missing_global_acceptance_deltas[campaign_key] = list(REQUIRED_DELTA_KEYS)
        else:
            missing_global_keys = [key for key in REQUIRED_DELTA_KEYS if key not in global_deltas]
            if missing_global_keys:
                missing_global_acceptance_deltas[campaign_key] = missing_global_keys
        exchange_scope_deltas = matrix.get("exchange_scope_deltas")
        if not isinstance(exchange_scope_deltas, dict):
            missing_exchange_scope_delta_fields[campaign_key] = [
                "exchange_count",
                "changed_exchange_rows",
                "delta_totals",
            ]
            missing_exchange_scope_delta_totals[campaign_key] = list(REQUIRED_DELTA_KEYS)
        else:
            missing_scope_fields = [
                key
                for key in ("exchange_count", "changed_exchange_rows", "delta_totals")
                if key not in exchange_scope_deltas
            ]
            if missing_scope_fields:
                missing_exchange_scope_delta_fields[campaign_key] = missing_scope_fields
            delta_totals = exchange_scope_deltas.get("delta_totals")
            if not isinstance(delta_totals, dict):
                missing_exchange_scope_delta_totals[campaign_key] = list(REQUIRED_DELTA_KEYS)
            else:
                missing_delta_total_keys = [key for key in REQUIRED_DELTA_KEYS if key not in delta_totals]
                if missing_delta_total_keys:
                    missing_exchange_scope_delta_totals[campaign_key] = missing_delta_total_keys
                changed_scope_rows = exchange_scope_deltas.get("changed_exchange_rows")
                non_zero_scope_delta_totals = [
                    key
                    for key in REQUIRED_DELTA_KEYS
                    if isinstance(delta_totals.get(key), (int, float))
                    and not isinstance(delta_totals.get(key), bool)
                    and delta_totals.get(key) != 0
                ]
                if changed_scope_rows == 0 and non_zero_scope_delta_totals:
                    exchange_scope_delta_consistency_gaps.append(
                        {
                            "campaign_key": campaign_key,
                            "field": "exchange_scope_deltas.changed_exchange_rows",
                            "changed_exchange_rows": changed_scope_rows,
                            "non_zero_delta_totals": non_zero_scope_delta_totals,
                        }
                    )
                if matrix.get("exchange_scope") == "all" and isinstance(global_deltas, dict):
                    mismatched_all_scope_totals = {
                        key: {
                            "scope_delta_total": delta_totals.get(key),
                            "global_delta": delta_value(global_deltas.get(key)),
                        }
                        for key in REQUIRED_DELTA_KEYS
                        if key in delta_totals and delta_totals.get(key) != delta_value(global_deltas.get(key))
                    }
                    if mismatched_all_scope_totals:
                        exchange_scope_delta_consistency_gaps.append(
                            {
                                "campaign_key": campaign_key,
                                "field": "exchange_scope_deltas.delta_totals",
                                "reason": "all_scope_totals_must_match_global_acceptance_deltas",
                                "mismatches": mismatched_all_scope_totals,
                        }
                    )
        before_after = campaign.get("before_after_summary")
        requires_before_after = "status" in campaign and "artifacts" in campaign
        if not isinstance(before_after, dict):
            if requires_before_after:
                missing_before_after_summaries.append(campaign_key)
        else:
            missing_before_after_keys = [
                key
                for key in (
                    "exchange_scope",
                    "affected_artifact_rows",
                    "global_before_after",
                    "exchange_scope_delta_totals",
                    "warn_quarantine_delta",
                    "policy",
                    "before_after_context",
                )
                if key not in before_after
            ]
            if missing_before_after_keys:
                missing_before_after_fields[campaign_key] = missing_before_after_keys
            if before_after.get("exchange_scope") != matrix.get("exchange_scope"):
                before_after_mismatches.append(
                    {
                        "campaign_key": campaign_key,
                        "field": "exchange_scope",
                        "before_after": before_after.get("exchange_scope"),
                        "acceptance_matrix": matrix.get("exchange_scope"),
                    }
                )
            if before_after.get("affected_artifact_rows") != matrix.get("affected_artifact_rows"):
                before_after_mismatches.append(
                    {
                        "campaign_key": campaign_key,
                        "field": "affected_artifact_rows",
                        "before_after": before_after.get("affected_artifact_rows"),
                        "acceptance_matrix": matrix.get("affected_artifact_rows"),
                    }
                )
            global_before_after = before_after.get("global_before_after")
            if not isinstance(global_before_after, dict):
                missing_before_after_fields.setdefault(campaign_key, []).append("global_before_after")
            else:
                missing_global_before_after = [
                    key
                    for key in REQUIRED_DELTA_KEYS
                    if not isinstance(global_before_after.get(key), dict)
                    or any(field not in global_before_after[key] for field in ("before", "after", "delta"))
                ]
                if missing_global_before_after:
                    missing_before_after_fields.setdefault(campaign_key, []).extend(
                        f"global_before_after.{key}" for key in missing_global_before_after
                    )
                if isinstance(global_deltas, dict):
                    for key in REQUIRED_DELTA_KEYS:
                        row = global_before_after.get(key)
                        if isinstance(row, dict):
                            expected = global_deltas.get(key, {})
                            if isinstance(expected, dict) and (
                                row.get("before") != expected.get("baseline")
                                or row.get("after") != expected.get("current")
                                or row.get("delta") != expected.get("delta")
                            ):
                                before_after_mismatches.append(
                                    {
                                        "campaign_key": campaign_key,
                                        "field": f"global_before_after.{key}",
                                        "before_after": row,
                                        "acceptance_matrix": expected,
                                    }
                                )
            before_after_scope_totals = before_after.get("exchange_scope_delta_totals")
            matrix_scope_totals = (
                exchange_scope_deltas.get("delta_totals") if isinstance(exchange_scope_deltas, dict) else {}
            )
            if not isinstance(before_after_scope_totals, dict):
                missing_before_after_fields.setdefault(campaign_key, []).append("exchange_scope_delta_totals")
            elif isinstance(matrix_scope_totals, dict):
                missing_scope_total_keys = [
                    key for key in REQUIRED_DELTA_KEYS if key not in before_after_scope_totals
                ]
                if missing_scope_total_keys:
                    missing_before_after_fields.setdefault(campaign_key, []).extend(
                        f"exchange_scope_delta_totals.{key}" for key in missing_scope_total_keys
                    )
                scope_mismatches = {
                    key: {
                        "before_after": before_after_scope_totals.get(key),
                        "acceptance_matrix": matrix_scope_totals.get(key),
                    }
                    for key in REQUIRED_DELTA_KEYS
                    if before_after_scope_totals.get(key) != matrix_scope_totals.get(key)
                }
                if scope_mismatches:
                    before_after_mismatches.append(
                        {
                            "campaign_key": campaign_key,
                            "field": "exchange_scope_delta_totals",
                            "mismatches": scope_mismatches,
                        }
                    )
            if before_after.get("before_after_context") != campaign_before_after_context(before_after):
                before_after_mismatches.append(
                    {
                        "campaign_key": campaign_key,
                        "field": "before_after_context",
                        "before_after": before_after.get("before_after_context"),
                        "expected": campaign_before_after_context(before_after),
                    }
                )
        if campaign_key == "b3":
            evidence = campaign.get("evidence", {})
            delta_evidence = campaign.get("delta_evidence", {})
            if not isinstance(evidence, dict):
                b3_campaign_evidence_gaps.append({"field": "evidence", "reason": "expected_object"})
                evidence = {}
            missing_evidence_keys = [
                key for key in B3_CAMPAIGN_DIAGNOSTIC_EVIDENCE_KEYS if key not in evidence
            ]
            if missing_evidence_keys:
                b3_campaign_evidence_gaps.append(
                    {"field": "evidence", "missing_keys": missing_evidence_keys}
                )
            numeric_keys = (
                "b3_dataset_rows",
                "b3_active_exchange_directory_rows",
                "b3_all_masterfile_rows",
                "b3_masterfile_matched_dataset_rows",
                "b3_masterfile_missing_dataset_rows",
                "b3_official_any_source_matched_dataset_rows",
                "b3_official_any_source_missing_dataset_rows",
                "b3_masterfile_gap_review_rows",
                "b3_masterfile_gap_review_open_rows",
                "b3_masterfile_gap_review_closed_no_data_change_rows",
                "b3_masterfile_gap_review_candidate_sector_present_rows",
                "b3_masterfile_gap_review_candidate_isin_present_rows",
                "b3_masterfile_gap_review_candidate_category_mismatch_rows",
                "b3_masterfile_gap_review_official_subset_closure_ready_rows",
                "b3_etf_category_apply_rows",
                "b3_etf_category_written_updates",
                "b3_missing_isin_residual_rows",
                "b3_missing_sector_residual_rows",
            )
            for key in numeric_keys:
                if not isinstance(evidence.get(key), int) or isinstance(evidence.get(key), bool) or evidence.get(key, -1) < 0:
                    b3_campaign_evidence_gaps.append({"field": key, "reason": "expected_nonnegative_integer"})
            dataset_rows = evidence.get("b3_dataset_rows")
            matched_rows = evidence.get("b3_masterfile_matched_dataset_rows")
            missing_rows = evidence.get("b3_masterfile_missing_dataset_rows")
            if all(isinstance(value, int) and not isinstance(value, bool) for value in (dataset_rows, matched_rows, missing_rows)):
                if matched_rows + missing_rows != dataset_rows:
                    b3_campaign_evidence_gaps.append(
                        {
                            "field": "b3_dataset_row_balance",
                            "matched": matched_rows,
                            "missing": missing_rows,
                            "dataset": dataset_rows,
                        }
                    )
                expected_rate = round(matched_rows / dataset_rows * 100, 2) if dataset_rows else None
                if evidence.get("b3_masterfile_dataset_match_rate") != expected_rate:
                    b3_campaign_evidence_gaps.append(
                        {
                            "field": "b3_masterfile_dataset_match_rate",
                            "reported": evidence.get("b3_masterfile_dataset_match_rate"),
                            "expected": expected_rate,
                        }
                    )
            any_source_matched_rows = evidence.get("b3_official_any_source_matched_dataset_rows")
            any_source_missing_rows = evidence.get("b3_official_any_source_missing_dataset_rows")
            if all(
                isinstance(value, int) and not isinstance(value, bool)
                for value in (dataset_rows, any_source_matched_rows, any_source_missing_rows)
            ):
                if any_source_matched_rows + any_source_missing_rows != dataset_rows:
                    b3_campaign_evidence_gaps.append(
                        {
                            "field": "b3_official_any_source_dataset_row_balance",
                            "matched": any_source_matched_rows,
                            "missing": any_source_missing_rows,
                            "dataset": dataset_rows,
                        }
                    )
                expected_any_source_rate = round(any_source_matched_rows / dataset_rows * 100, 2) if dataset_rows else None
                if evidence.get("b3_official_any_source_match_rate") != expected_any_source_rate:
                    b3_campaign_evidence_gaps.append(
                        {
                            "field": "b3_official_any_source_match_rate",
                            "reported": evidence.get("b3_official_any_source_match_rate"),
                            "expected": expected_any_source_rate,
                        }
                    )
            for counter_key in (
                "b3_missing_category_totals",
                "b3_missing_asset_type_totals",
                "b3_missing_source_presence_totals",
                "b3_masterfile_gap_review_source_presence_totals",
                "b3_masterfile_gap_review_open_source_presence_totals",
                "b3_masterfile_gap_review_bucket_totals",
                "b3_masterfile_gap_review_resolution_queue_totals",
                "b3_masterfile_gap_review_open_resolution_queue_totals",
                "b3_masterfile_gap_review_strategy_totals",
                "b3_masterfile_gap_review_candidate_source_totals",
                "b3_masterfile_gap_review_candidate_category_review_decision_totals",
                "b3_masterfile_gap_review_official_subset_review_decision_totals",
                "b3_masterfile_gap_review_official_subset_closure_eligibility_totals",
                "b3_etf_category_apply_decision_totals",
                "b3_etf_category_update_totals",
                "sector_b3_code_shape_totals",
            ):
                counter = evidence.get(counter_key)
                if not isinstance(counter, dict) or any(not isinstance(value, int) or value < 0 for value in counter.values()):
                    b3_campaign_evidence_gaps.append({"field": counter_key, "reason": "expected_counter"})
                elif (
                    counter_key not in {
                        "sector_b3_code_shape_totals",
                        "b3_masterfile_gap_review_candidate_source_totals",
                        "b3_masterfile_gap_review_open_source_presence_totals",
                        "b3_masterfile_gap_review_open_resolution_queue_totals",
                        "b3_etf_category_apply_decision_totals",
                        "b3_etf_category_update_totals",
                    }
                    and isinstance(missing_rows, int)
                    and sum(counter.values()) != missing_rows
                ):
                    b3_campaign_evidence_gaps.append(
                        {
                            "field": counter_key,
                            "reported_total": sum(counter.values()),
                            "missing_rows": missing_rows,
                        }
                    )
            gap_review_rows = evidence.get("b3_masterfile_gap_review_rows")
            gap_review_open_rows = evidence.get("b3_masterfile_gap_review_open_rows")
            gap_review_closed_rows = evidence.get("b3_masterfile_gap_review_closed_no_data_change_rows")
            gap_review_source_presence = evidence.get("b3_masterfile_gap_review_source_presence_totals")
            gap_review_open_source_presence = evidence.get("b3_masterfile_gap_review_open_source_presence_totals")
            gap_review_open_queues = evidence.get("b3_masterfile_gap_review_open_resolution_queue_totals")
            gap_review_open_next_sources = evidence.get("b3_masterfile_gap_review_open_next_source_totals")
            gap_review_open_evidence_paths = evidence.get("b3_masterfile_gap_review_open_evidence_path_totals")
            source_gap_resolution_gates = evidence.get("b3_masterfile_gap_review_source_gap_resolution_gate_totals")
            if all(
                isinstance(value, int) and not isinstance(value, bool)
                for value in (gap_review_rows, gap_review_open_rows, gap_review_closed_rows)
            ):
                if gap_review_open_rows + gap_review_closed_rows != gap_review_rows:
                    b3_campaign_evidence_gaps.append(
                        {
                            "field": "b3_masterfile_gap_review_open_closed_balance",
                            "open": gap_review_open_rows,
                            "closed_no_data_change": gap_review_closed_rows,
                            "review_rows": gap_review_rows,
                        }
                    )
            if isinstance(gap_review_rows, int) and isinstance(gap_review_source_presence, dict):
                if sum(gap_review_source_presence.values()) != gap_review_rows:
                    b3_campaign_evidence_gaps.append(
                        {
                            "field": "b3_masterfile_gap_review_source_presence_totals",
                            "reported_total": sum(gap_review_source_presence.values()),
                            "review_rows": gap_review_rows,
                        }
                    )
            if isinstance(gap_review_open_rows, int):
                for counter_key, counter in (
                    ("b3_masterfile_gap_review_open_source_presence_totals", gap_review_open_source_presence),
                    ("b3_masterfile_gap_review_open_resolution_queue_totals", gap_review_open_queues),
                    ("b3_masterfile_gap_review_open_next_source_totals", gap_review_open_next_sources),
                    ("b3_masterfile_gap_review_open_evidence_path_totals", gap_review_open_evidence_paths),
                ):
                    if isinstance(counter, dict) and sum(counter.values()) != gap_review_open_rows:
                        b3_campaign_evidence_gaps.append(
                            {
                                "field": counter_key,
                                "reported_total": sum(counter.values()),
                                "open_review_rows": gap_review_open_rows,
                            }
                        )
                if isinstance(missing_rows, int) and gap_review_rows != missing_rows:
                    b3_campaign_evidence_gaps.append(
                        {
                            "field": "b3_masterfile_gap_review_rows",
                            "reported": gap_review_rows,
                            "expected": missing_rows,
                        }
                    )
            if isinstance(source_gap_resolution_gates, dict) and isinstance(gap_review_rows, int):
                if sum(source_gap_resolution_gates.values()) != gap_review_rows:
                    b3_campaign_evidence_gaps.append(
                        {
                            "field": "b3_masterfile_gap_review_source_gap_resolution_gate_totals",
                            "reported_total": sum(source_gap_resolution_gates.values()),
                            "review_rows": gap_review_rows,
                        }
                    )
            candidate_source_totals = evidence.get("b3_masterfile_gap_review_candidate_source_totals")
            resolution_queue_totals = evidence.get("b3_masterfile_gap_review_resolution_queue_totals")
            strategy_totals = evidence.get("b3_masterfile_gap_review_strategy_totals")
            resolution_queue_asset_type_totals = evidence.get(
                "b3_masterfile_gap_review_resolution_queue_asset_type_totals"
            )
            resolution_queue_gap_category_totals = evidence.get(
                "b3_masterfile_gap_review_resolution_queue_gap_category_totals"
            )
            if isinstance(resolution_queue_totals, dict) and isinstance(gap_review_rows, int):
                if sum(resolution_queue_totals.values()) != gap_review_rows:
                    b3_campaign_evidence_gaps.append(
                        {
                            "field": "b3_masterfile_gap_review_resolution_queue_totals",
                            "reported_total": sum(resolution_queue_totals.values()),
                            "review_rows": gap_review_rows,
                        }
                    )
                if isinstance(strategy_totals, dict):
                    expected_strategy_totals = Counter()
                    for queue, count in resolution_queue_totals.items():
                        if isinstance(count, int) and not isinstance(count, bool):
                            expected_strategy_totals[b3_masterfile_gap_review_strategy(str(queue))] += count
                    if strategy_totals != dict(expected_strategy_totals):
                        b3_campaign_evidence_gaps.append(
                            {
                                "field": "b3_masterfile_gap_review_strategy_totals",
                                "reported": strategy_totals,
                                "expected": dict(expected_strategy_totals),
                            }
                        )
            if not isinstance(resolution_queue_asset_type_totals, dict) or any(
                not isinstance(asset_type_totals, dict)
                or any(not isinstance(value, int) or value < 0 for value in asset_type_totals.values())
                for asset_type_totals in (
                    resolution_queue_asset_type_totals.values()
                    if isinstance(resolution_queue_asset_type_totals, dict)
                    else []
                )
            ):
                b3_campaign_evidence_gaps.append(
                    {
                        "field": "b3_masterfile_gap_review_resolution_queue_asset_type_totals",
                        "reason": "expected_nested_counter",
                    }
                )
            elif isinstance(resolution_queue_totals, dict):
                for queue, count in resolution_queue_totals.items():
                    asset_type_counts = resolution_queue_asset_type_totals.get(queue)
                    if not isinstance(asset_type_counts, dict) or sum(asset_type_counts.values()) != count:
                        b3_campaign_evidence_gaps.append(
                            {
                                "field": f"b3_masterfile_gap_review_resolution_queue_asset_type_totals.{queue}",
                                "reported_total": sum(asset_type_counts.values())
                                if isinstance(asset_type_counts, dict)
                                else None,
                                "expected": count,
                            }
                        )
            if not isinstance(resolution_queue_gap_category_totals, dict) or any(
                not isinstance(gap_category_totals, dict)
                or any(not isinstance(value, int) or value < 0 for value in gap_category_totals.values())
                for gap_category_totals in (
                    resolution_queue_gap_category_totals.values()
                    if isinstance(resolution_queue_gap_category_totals, dict)
                    else []
                )
            ):
                b3_campaign_evidence_gaps.append(
                    {
                        "field": "b3_masterfile_gap_review_resolution_queue_gap_category_totals",
                        "reason": "expected_nested_counter",
                    }
                )
            elif isinstance(resolution_queue_totals, dict):
                for queue, count in resolution_queue_totals.items():
                    gap_category_counts = resolution_queue_gap_category_totals.get(queue)
                    if not isinstance(gap_category_counts, dict) or sum(gap_category_counts.values()) != count:
                        b3_campaign_evidence_gaps.append(
                            {
                                "field": f"b3_masterfile_gap_review_resolution_queue_gap_category_totals.{queue}",
                                "reported_total": sum(gap_category_counts.values())
                                if isinstance(gap_category_counts, dict)
                                else None,
                                "expected": count,
                            }
                        )
            if isinstance(candidate_source_totals, dict) and isinstance(gap_review_source_presence, dict):
                expected_candidate_source_rows = gap_review_source_presence.get(
                    "present_only_in_non_exchange_directory_source",
                    0,
                )
                if sum(candidate_source_totals.values()) != expected_candidate_source_rows:
                    b3_campaign_evidence_gaps.append(
                        {
                            "field": "b3_masterfile_gap_review_candidate_source_totals",
                            "reported_total": sum(candidate_source_totals.values()),
                            "expected": expected_candidate_source_rows,
                        }
                    )
            subset_decision_totals = evidence.get(
                "b3_masterfile_gap_review_official_subset_review_decision_totals"
            )
            subset_closure_totals = evidence.get(
                "b3_masterfile_gap_review_official_subset_closure_eligibility_totals"
            )
            subset_closure_ready_rows = evidence.get(
                "b3_masterfile_gap_review_official_subset_closure_ready_rows"
            )
            if isinstance(subset_decision_totals, dict) and isinstance(gap_review_rows, int):
                if sum(subset_decision_totals.values()) != gap_review_rows:
                    b3_campaign_evidence_gaps.append(
                        {
                            "field": "b3_masterfile_gap_review_official_subset_review_decision_totals",
                            "reported_total": sum(subset_decision_totals.values()),
                            "review_rows": gap_review_rows,
                        }
                    )
                if (
                    isinstance(gap_review_source_presence, dict)
                    and sum(
                        count
                        for decision, count in subset_decision_totals.items()
                        if str(decision).startswith("official_subset_")
                    )
                    != gap_review_source_presence.get("present_only_in_non_exchange_directory_source", 0)
                ):
                    b3_campaign_evidence_gaps.append(
                        {
                            "field": "b3_masterfile_gap_review_official_subset_review_decision_totals",
                            "reported_official_subset_total": sum(
                                count
                                for decision, count in subset_decision_totals.items()
                                if str(decision).startswith("official_subset_")
                            ),
                            "expected": gap_review_source_presence.get(
                                "present_only_in_non_exchange_directory_source",
                                0,
                            ),
                        }
                    )
            if isinstance(subset_closure_totals, dict) and isinstance(gap_review_rows, int):
                if sum(subset_closure_totals.values()) != gap_review_rows:
                    b3_campaign_evidence_gaps.append(
                        {
                            "field": "b3_masterfile_gap_review_official_subset_closure_eligibility_totals",
                            "reported_total": sum(subset_closure_totals.values()),
                            "review_rows": gap_review_rows,
                        }
                    )
                expected_ready_rows = sum(
                    count
                    for eligibility, count in subset_closure_totals.items()
                    if str(eligibility).startswith("closure_ready_")
                )
                if subset_closure_ready_rows != expected_ready_rows:
                    b3_campaign_evidence_gaps.append(
                        {
                            "field": "b3_masterfile_gap_review_official_subset_closure_ready_rows",
                            "reported": subset_closure_ready_rows,
                            "expected": expected_ready_rows,
                        }
                    )
            category_decision_totals = evidence.get(
                "b3_masterfile_gap_review_candidate_category_review_decision_totals"
            )
            category_mismatch_rows = evidence.get("b3_masterfile_gap_review_candidate_category_mismatch_rows")
            if isinstance(category_decision_totals, dict) and isinstance(category_mismatch_rows, int):
                expected_mismatch_rows = category_decision_totals.get(
                    "official_candidate_category_differs_from_current_requires_review",
                    0,
                )
                if category_mismatch_rows != expected_mismatch_rows:
                    b3_campaign_evidence_gaps.append(
                        {
                            "field": "b3_masterfile_gap_review_candidate_category_mismatch_rows",
                            "reported": category_mismatch_rows,
                            "expected": expected_mismatch_rows,
                        }
                    )
            mismatch_examples = evidence.get("b3_masterfile_gap_review_candidate_category_mismatch_examples")
            if not isinstance(mismatch_examples, list):
                b3_campaign_evidence_gaps.append(
                    {
                        "field": "b3_masterfile_gap_review_candidate_category_mismatch_examples",
                        "reason": "expected_list",
                    }
                )
            elif isinstance(category_mismatch_rows, int) and category_mismatch_rows > 0 and not mismatch_examples:
                b3_campaign_evidence_gaps.append(
                    {
                        "field": "b3_masterfile_gap_review_candidate_category_mismatch_examples",
                        "reason": "examples_required",
                    }
                )
            else:
                for example_index, example in enumerate(mismatch_examples):
                    if (
                        not isinstance(example, dict)
                        or not str(example.get("listing_key", "")).startswith("B3::")
                        or not example.get("current_etf_category")
                        or not example.get("candidate_sectors")
                        or not example.get("candidate_sources")
                    ):
                        b3_campaign_evidence_gaps.append(
                            {
                                "field": (
                                    "b3_masterfile_gap_review_candidate_category_mismatch_examples"
                                    f"[{example_index}]"
                                ),
                                "reason": "expected_b3_listing_keyed_category_mismatch_example",
                            }
                        )
            coverage_diagnosis = evidence.get("b3_masterfile_gap_review_coverage_diagnosis")
            if not isinstance(coverage_diagnosis, dict):
                b3_campaign_evidence_gaps.append(
                    {
                        "field": "b3_masterfile_gap_review_coverage_diagnosis",
                        "reason": "expected_object",
                    }
                )
            else:
                expected_diagnosis_values = {
                    "dataset_rows": dataset_rows,
                    "active_directory_match_rate": evidence.get("b3_masterfile_dataset_match_rate"),
                    "active_directory_missing_dataset_rows": missing_rows,
                    "open_review_rows": gap_review_open_rows,
                    "closed_no_data_change_rows": gap_review_closed_rows,
                    "official_non_directory_gap_rows": (
                        gap_review_source_presence.get("present_only_in_non_exchange_directory_source")
                        if isinstance(gap_review_source_presence, dict)
                        else None
                    ),
                    "absent_from_all_b3_source_gap_rows": (
                        gap_review_source_presence.get("absent_from_all_b3_masterfile_sources")
                        if isinstance(gap_review_source_presence, dict)
                        else None
                    ),
                    "official_subset_candidate_isin_rows": evidence.get(
                        "b3_masterfile_gap_review_candidate_isin_present_rows"
                    ),
                    "official_subset_candidate_sector_rows": evidence.get(
                        "b3_masterfile_gap_review_candidate_sector_present_rows"
                    ),
                    "rows_requiring_parser_or_scope_review": (
                        gap_review_source_presence.get("present_only_in_non_exchange_directory_source", 0)
                        - gap_review_closed_rows
                        if isinstance(gap_review_source_presence, dict)
                        and isinstance(gap_review_closed_rows, int)
                        else None
                    ),
                    "rows_requiring_external_active_evidence": (
                        gap_review_source_presence.get("absent_from_all_b3_masterfile_sources")
                        if isinstance(gap_review_source_presence, dict)
                        else None
                    ),
                    "data_change_authorized": False,
                }
                for key, expected in expected_diagnosis_values.items():
                    if expected is not None and coverage_diagnosis.get(key) != expected:
                        b3_campaign_evidence_gaps.append(
                            {
                                "field": f"b3_masterfile_gap_review_coverage_diagnosis.{key}",
                                "reported": coverage_diagnosis.get(key),
                                "expected": expected,
                            }
                        )
                if not coverage_diagnosis.get("root_cause") or not coverage_diagnosis.get("source_gate"):
                    b3_campaign_evidence_gaps.append(
                        {
                            "field": "b3_masterfile_gap_review_coverage_diagnosis",
                            "reason": "missing_root_cause_or_source_gate",
                        }
                    )
            top_masterfile_batches = evidence.get("top_b3_masterfile_gap_review_batches")
            top_open_masterfile_batches = evidence.get("top_open_b3_masterfile_review_batches")
            if not isinstance(top_masterfile_batches, list) or not top_masterfile_batches:
                b3_campaign_evidence_gaps.append(
                    {"field": "top_b3_masterfile_gap_review_batches", "reason": "expected_ranked_review_batches"}
                )
            else:
                required_batch_fields = {
                    "review_priority",
                    "b3_resolution_queue",
                    "asset_type",
                    "b3_gap_category",
                    "source_presence",
                    "rows",
                    "review_strategy",
                    "verification_evidence_required",
                    "b3_source_gap_evidence_path",
                    "source_gap_resolution_gate",
                    "recommended_next_source",
                    "source_gate",
                }
                valid_queues = {
                    "official_bdr_subset_without_category_source_gap_closed",
                    "official_subset_category_already_reflected_scope_review",
                    "official_subset_without_category_scope_review",
                    "official_subset_category_requires_review",
                    "absent_from_all_b3_sources_local_share_source_gap",
                    "absent_from_all_b3_sources_fund_or_receipt_source_gap",
                    "absent_from_all_b3_sources_unclassified_source_gap",
                }
                for batch_index, batch in enumerate(top_masterfile_batches):
                    if not isinstance(batch, dict) or not required_batch_fields.issubset(batch):
                        b3_campaign_evidence_gaps.append(
                            {
                                "field": "top_b3_masterfile_gap_review_batches",
                                "row_index": batch_index,
                                "reason": "expected_ranked_review_batch_fields",
                            }
                        )
                        continue
                    queue = str(batch.get("b3_resolution_queue", ""))
                    if queue not in valid_queues:
                        b3_campaign_evidence_gaps.append(
                            {
                                "field": "top_b3_masterfile_gap_review_batches",
                                "row_index": batch_index,
                                "reason": "unexpected_resolution_queue",
                            }
                        )
                    if not isinstance(batch.get("rows"), int) or batch.get("rows", 0) <= 0:
                        b3_campaign_evidence_gaps.append(
                            {
                                "field": "top_b3_masterfile_gap_review_batches",
                                "row_index": batch_index,
                                "reason": "expected_positive_rows",
                            }
                        )
                    expected_strategy = b3_masterfile_gap_review_strategy(queue)
                    if batch.get("review_strategy") != expected_strategy:
                        b3_campaign_evidence_gaps.append(
                            {
                                "field": "top_b3_masterfile_gap_review_batches",
                                "row_index": batch_index,
                                "reported": batch.get("review_strategy"),
                                "expected": expected_strategy,
                            }
                        )
                    if not batch.get("recommended_next_source") or not batch.get("source_gate"):
                        b3_campaign_evidence_gaps.append(
                            {
                                "field": "top_b3_masterfile_gap_review_batches",
                                "row_index": batch_index,
                                "reason": "expected_source_instruction_fields",
                            }
                        )
            if not isinstance(top_open_masterfile_batches, list) or not top_open_masterfile_batches:
                b3_campaign_evidence_gaps.append(
                    {"field": "top_open_b3_masterfile_review_batches", "reason": "expected_ranked_review_batches"}
                )
            elif isinstance(gap_review_open_rows, int):
                top_open_rows = sum(
                    batch.get("rows", 0)
                    for batch in top_open_masterfile_batches
                    if isinstance(batch, dict) and isinstance(batch.get("rows"), int)
                )
                if top_open_rows > gap_review_open_rows:
                    b3_campaign_evidence_gaps.append(
                        {
                            "field": "top_open_b3_masterfile_review_batches",
                            "reported_total": top_open_rows,
                            "open_review_rows": gap_review_open_rows,
                        }
                    )
            top_open_masterfile_rows = evidence.get("top_open_b3_masterfile_review_rows")
            if not isinstance(top_open_masterfile_rows, list) or not top_open_masterfile_rows:
                b3_campaign_evidence_gaps.append(
                    {"field": "top_open_b3_masterfile_review_rows", "reason": "expected_listing_keyed_open_rows"}
                )
            elif isinstance(gap_review_open_rows, int):
                if len(top_open_masterfile_rows) > min(gap_review_open_rows, 25):
                    b3_campaign_evidence_gaps.append(
                        {
                            "field": "top_open_b3_masterfile_review_rows",
                            "reported_rows": len(top_open_masterfile_rows),
                            "expected_max": min(gap_review_open_rows, 25),
                        }
                    )
                required_open_row_fields = {
                    "listing_key",
                    "ticker",
                    "asset_type",
                    "name",
                    "b3_gap_category",
                    "b3_resolution_queue",
                    "review_priority",
                    "review_strategy",
                    "verification_evidence_required",
                    "b3_source_gap_evidence_path",
                    "source_gap_resolution_gate",
                    "recommended_next_source",
                    "source_gate",
                }
                valid_open_queues = {
                    "official_subset_without_category_scope_review",
                    "official_subset_category_requires_review",
                    "absent_from_all_b3_sources_local_share_source_gap",
                    "absent_from_all_b3_sources_fund_or_receipt_source_gap",
                    "absent_from_all_b3_sources_unclassified_source_gap",
                }
                for row_index, row in enumerate(top_open_masterfile_rows):
                    if not isinstance(row, dict) or not required_open_row_fields.issubset(row):
                        b3_campaign_evidence_gaps.append(
                            {
                                "field": "top_open_b3_masterfile_review_rows",
                                "row_index": row_index,
                                "reason": "expected_listing_keyed_open_row_fields",
                            }
                        )
                        continue
                    if not str(row.get("listing_key", "")).startswith("B3::"):
                        b3_campaign_evidence_gaps.append(
                            {
                                "field": "top_open_b3_masterfile_review_rows",
                                "row_index": row_index,
                                "reason": "expected_b3_listing_key",
                            }
                        )
                    queue = str(row.get("b3_resolution_queue", ""))
                    if queue not in valid_open_queues:
                        b3_campaign_evidence_gaps.append(
                            {
                                "field": "top_open_b3_masterfile_review_rows",
                                "row_index": row_index,
                                "reason": "unexpected_open_resolution_queue",
                            }
                        )
                    expected_strategy = b3_masterfile_gap_review_strategy(queue)
                    if row.get("review_strategy") != expected_strategy:
                        b3_campaign_evidence_gaps.append(
                            {
                                "field": "top_open_b3_masterfile_review_rows",
                                "row_index": row_index,
                                "reported": row.get("review_strategy"),
                                "expected": expected_strategy,
                            }
                        )
                    if not row.get("recommended_next_source") or not row.get("source_gate"):
                        b3_campaign_evidence_gaps.append(
                            {
                                "field": "top_open_b3_masterfile_review_rows",
                                "row_index": row_index,
                                "reason": "expected_source_instruction_fields",
                            }
                        )
                    if not row.get("b3_source_gap_evidence_path") or not row.get("source_gap_resolution_gate"):
                        b3_campaign_evidence_gaps.append(
                            {
                                "field": "top_open_b3_masterfile_review_rows",
                                "row_index": row_index,
                                "reason": "expected_evidence_path_and_resolution_gate",
                            }
                        )
            etf_apply_rows = evidence.get("b3_etf_category_apply_rows")
            etf_written_updates = evidence.get("b3_etf_category_written_updates")
            etf_apply_decisions = evidence.get("b3_etf_category_apply_decision_totals")
            etf_update_totals = evidence.get("b3_etf_category_update_totals")
            if isinstance(etf_apply_rows, int) and isinstance(etf_apply_decisions, dict):
                if sum(etf_apply_decisions.values()) != etf_apply_rows:
                    b3_campaign_evidence_gaps.append(
                        {
                            "field": "b3_etf_category_apply_decision_totals",
                            "reported_total": sum(etf_apply_decisions.values()),
                            "apply_rows": etf_apply_rows,
                        }
                    )
            if isinstance(etf_written_updates, int) and isinstance(etf_apply_decisions, dict):
                expected_written_updates = etf_apply_decisions.get("apply", 0)
                if etf_written_updates not in {0, expected_written_updates}:
                    b3_campaign_evidence_gaps.append(
                        {
                            "field": "b3_etf_category_written_updates",
                            "reported": etf_written_updates,
                            "expected_dry_run_or_apply_count": expected_written_updates,
                        }
                    )
            if isinstance(etf_update_totals, dict) and isinstance(etf_apply_decisions, dict):
                expected_update_total = etf_apply_decisions.get("apply", 0)
                if sum(etf_update_totals.values()) not in {0, expected_update_total}:
                    b3_campaign_evidence_gaps.append(
                        {
                            "field": "b3_etf_category_update_totals",
                            "reported_total": sum(etf_update_totals.values()),
                            "expected_dry_run_or_apply_count": expected_update_total,
                        }
                    )
            examples = evidence.get("b3_missing_examples", {})
            if isinstance(missing_rows, int) and missing_rows > 0 and not examples:
                b3_campaign_evidence_gaps.append({"field": "b3_missing_examples", "reason": "examples_required"})
            elif not isinstance(examples, dict):
                b3_campaign_evidence_gaps.append({"field": "b3_missing_examples", "reason": "expected_object"})
            else:
                for category, rows_for_category in examples.items():
                    if not isinstance(rows_for_category, list):
                        b3_campaign_evidence_gaps.append({"field": f"b3_missing_examples.{category}", "reason": "expected_list"})
                        continue
                    for example_index, example in enumerate(rows_for_category):
                        if not isinstance(example, dict) or not str(example.get("listing_key", "")).startswith("B3::"):
                            b3_campaign_evidence_gaps.append(
                                {
                                    "field": f"b3_missing_examples.{category}[{example_index}]",
                                    "reason": "expected_b3_listing_keyed_example",
                                }
                            )
            alphanumeric_b3_code_rows = evidence.get("sector_alphanumeric_b3_code_rows")
            if (
                not isinstance(alphanumeric_b3_code_rows, int)
                or isinstance(alphanumeric_b3_code_rows, bool)
                or alphanumeric_b3_code_rows < 0
            ):
                b3_campaign_evidence_gaps.append(
                    {"field": "sector_alphanumeric_b3_code_rows", "reason": "expected_nonnegative_integer"}
                )
            code_shape_totals = evidence.get("sector_b3_code_shape_totals")
            if isinstance(code_shape_totals, dict) and isinstance(alphanumeric_b3_code_rows, int):
                if code_shape_totals.get("alphanumeric_b3_code", 0) != alphanumeric_b3_code_rows:
                    b3_campaign_evidence_gaps.append(
                        {
                            "field": "sector_alphanumeric_b3_code_rows",
                            "reported": alphanumeric_b3_code_rows,
                            "expected": code_shape_totals.get("alphanumeric_b3_code", 0),
                        }
                    )
            workstream_rows = evidence.get("b3_residual_workstream_rows")
            expected_workstream_rows = {
                "masterfile_active_directory_gap": evidence.get("b3_masterfile_gap_review_rows"),
                "missing_isin_residual": evidence.get("b3_missing_isin_residual_rows"),
                "missing_sector_residual": evidence.get("b3_missing_sector_residual_rows"),
            }
            if not isinstance(workstream_rows, dict) or any(
                not isinstance(value, int) or isinstance(value, bool) or value < 0
                for value in (workstream_rows.values() if isinstance(workstream_rows, dict) else [])
            ):
                b3_campaign_evidence_gaps.append(
                    {"field": "b3_residual_workstream_rows", "reason": "expected_counter"}
                )
            else:
                for workstream, expected in expected_workstream_rows.items():
                    if isinstance(expected, int) and workstream_rows.get(workstream) != expected:
                        b3_campaign_evidence_gaps.append(
                            {
                                "field": f"b3_residual_workstream_rows.{workstream}",
                                "reported": workstream_rows.get(workstream),
                                "expected": expected,
                            }
                        )
            workstream_priority_totals = evidence.get("b3_residual_workstream_priority_totals")
            if not isinstance(workstream_priority_totals, dict) or any(
                not isinstance(priority_totals, dict)
                or any(not isinstance(value, int) or value < 0 for value in priority_totals.values())
                for priority_totals in (
                    workstream_priority_totals.values()
                    if isinstance(workstream_priority_totals, dict)
                    else []
                )
            ):
                b3_campaign_evidence_gaps.append(
                    {"field": "b3_residual_workstream_priority_totals", "reason": "expected_nested_counter"}
                )
            elif isinstance(workstream_rows, dict):
                for workstream, count in workstream_rows.items():
                    priority_counts = workstream_priority_totals.get(workstream)
                    if not isinstance(priority_counts, dict) or sum(priority_counts.values()) != count:
                        b3_campaign_evidence_gaps.append(
                            {
                                "field": f"b3_residual_workstream_priority_totals.{workstream}",
                                "reported_total": sum(priority_counts.values())
                                if isinstance(priority_counts, dict)
                                else None,
                                "expected": count,
                            }
                        )
            workstream_readiness_totals = evidence.get("b3_residual_workstream_readiness_totals")
            if not isinstance(workstream_readiness_totals, dict) or any(
                not isinstance(readiness_totals, dict)
                or any(not isinstance(value, int) or value < 0 for value in readiness_totals.values())
                for readiness_totals in (
                    workstream_readiness_totals.values()
                    if isinstance(workstream_readiness_totals, dict)
                    else []
                )
            ):
                b3_campaign_evidence_gaps.append(
                    {"field": "b3_residual_workstream_readiness_totals", "reason": "expected_nested_counter"}
                )
            elif isinstance(workstream_rows, dict):
                for workstream, count in workstream_rows.items():
                    readiness_counts = workstream_readiness_totals.get(workstream)
                    if not isinstance(readiness_counts, dict) or sum(readiness_counts.values()) != count:
                        b3_campaign_evidence_gaps.append(
                            {
                                "field": f"b3_residual_workstream_readiness_totals.{workstream}",
                                "reported_total": sum(readiness_counts.values())
                                if isinstance(readiness_counts, dict)
                                else None,
                                "expected": count,
                            }
                        )
                    elif any(
                        "apply" in str(readiness).lower() and "already_reflected" not in str(readiness).lower()
                        for readiness in readiness_counts
                    ):
                        b3_campaign_evidence_gaps.append(
                            {
                                "field": f"b3_residual_workstream_readiness_totals.{workstream}",
                                "forbidden_apply_like_readiness": sorted(
                                    readiness
                                    for readiness in readiness_counts
                                    if "apply" in str(readiness).lower()
                                    and "already_reflected" not in str(readiness).lower()
                                ),
                            }
                        )
            source_identifier_exposure = evidence.get("b3_isin_official_source_identifier_exposure")
            if not isinstance(source_identifier_exposure, dict) or any(
                not isinstance(source_counts, dict)
                or any(
                    key not in source_counts
                    or not isinstance(source_counts.get(key), int)
                    or source_counts.get(key) < 0
                    for key in ("rows", "isin_present_rows", "isin_missing_rows")
                )
                for source_counts in (
                    source_identifier_exposure.values()
                    if isinstance(source_identifier_exposure, dict)
                    else []
                )
            ):
                b3_campaign_evidence_gaps.append(
                    {"field": "b3_isin_official_source_identifier_exposure", "reason": "expected_source_exposure"}
                )
            elif "b3_instruments_equities" not in source_identifier_exposure:
                b3_campaign_evidence_gaps.append(
                    {
                        "field": "b3_isin_official_source_identifier_exposure",
                        "reason": "missing_b3_instruments_equities_exposure",
                    }
                )
            else:
                for source_key, source_counts in source_identifier_exposure.items():
                    if source_counts["isin_present_rows"] + source_counts["isin_missing_rows"] != source_counts["rows"]:
                        b3_campaign_evidence_gaps.append(
                            {
                                "field": f"b3_isin_official_source_identifier_exposure.{source_key}",
                                "reason": "source_row_balance_mismatch",
                            }
                        )
            code_examples = evidence.get("sector_alphanumeric_b3_code_examples")
            if not isinstance(code_examples, list):
                b3_campaign_evidence_gaps.append(
                    {"field": "sector_alphanumeric_b3_code_examples", "reason": "expected_list"}
                )
            else:
                if isinstance(alphanumeric_b3_code_rows, int) and alphanumeric_b3_code_rows > 0 and not code_examples:
                    b3_campaign_evidence_gaps.append(
                        {"field": "sector_alphanumeric_b3_code_examples", "reason": "examples_required"}
                    )
                for example_index, example in enumerate(code_examples):
                    b3_code = str(example.get("b3_code", "")) if isinstance(example, dict) else ""
                    if (
                        not isinstance(example, dict)
                        or not str(example.get("listing_key", "")).startswith("B3::")
                        or not any(char.isdigit() for char in b3_code)
                    ):
                        b3_campaign_evidence_gaps.append(
                            {
                                "field": f"sector_alphanumeric_b3_code_examples[{example_index}]",
                                "reason": "expected_b3_listing_keyed_alphanumeric_code_example",
                            }
                        )
            known_deltas = delta_evidence.get("known_deltas") if isinstance(delta_evidence, dict) else {}
            if not isinstance(known_deltas, dict):
                known_deltas = {}
            missing_delta_evidence_keys = [
                key for key in B3_CAMPAIGN_DELTA_EVIDENCE_KEYS if key not in known_deltas
            ]
            if missing_delta_evidence_keys:
                b3_campaign_evidence_gaps.append(
                    {"field": "delta_evidence.known_deltas", "missing_keys": missing_delta_evidence_keys}
                )
            expected_coverage_snapshot = matrix.get("campaign_metric_deltas", {})
            if known_deltas.get("campaign_start_coverage_snapshot") != expected_coverage_snapshot:
                b3_campaign_evidence_gaps.append(
                    {
                        "field": "delta_evidence.campaign_start_coverage_snapshot",
                        "reported": known_deltas.get("campaign_start_coverage_snapshot"),
                        "expected": expected_coverage_snapshot,
                    }
                )
            global_acceptance_deltas = matrix.get("global_acceptance_deltas", {})
            if not isinstance(global_acceptance_deltas, dict):
                global_acceptance_deltas = {}
            expected_source_gap_delta = global_acceptance_deltas.get("source_gap_delta", {})
            if known_deltas.get("source_gap_delta") != expected_source_gap_delta:
                b3_campaign_evidence_gaps.append(
                    {
                        "field": "delta_evidence.source_gap_delta",
                        "reported": known_deltas.get("source_gap_delta"),
                        "expected": expected_source_gap_delta,
                    }
                )
            expected_warn_quarantine_delta = {
                "warn_delta": global_acceptance_deltas.get("warn_delta", {}).get("delta", 0),
                "quarantine_delta": global_acceptance_deltas.get("quarantine_delta", {}).get("delta", 0),
            }
            if known_deltas.get("warn_quarantine_delta") != expected_warn_quarantine_delta:
                b3_campaign_evidence_gaps.append(
                    {
                        "field": "delta_evidence.warn_quarantine_delta",
                        "reported": known_deltas.get("warn_quarantine_delta"),
                        "expected": expected_warn_quarantine_delta,
                    }
                )
        if campaign_key == "otc":
            evidence = campaign.get("evidence", {})
            delta_evidence = campaign.get("delta_evidence", {})
            if not isinstance(evidence, dict):
                otc_campaign_evidence_gaps.append({"field": "evidence", "reason": "expected_object"})
                evidence = {}
            missing_evidence_keys = [key for key in OTC_CAMPAIGN_EVIDENCE_KEYS if key not in evidence]
            if missing_evidence_keys:
                otc_campaign_evidence_gaps.append({"field": "evidence", "missing_keys": missing_evidence_keys})
            for key in (
                "otc_review_rows",
                "source_gap_rows",
                "drop_override_rows_still_present",
                "name_mismatch_review_rows",
                "otc_review_decision_active_name_mismatch_rows",
                "otc_name_mismatch_unreviewed_active_rows",
                "otc_review_decision_current_listing_suppressed_rows",
                "otc_review_decision_not_current_scope_rows",
                "otc_review_decision_stale_rows",
            ):
                if not isinstance(evidence.get(key), int) or isinstance(evidence.get(key), bool) or evidence.get(key, -1) < 0:
                    otc_campaign_evidence_gaps.append({"field": key, "reason": "expected_nonnegative_integer"})
            if evidence.get("drop_override_rows_still_present") != 0:
                otc_campaign_evidence_gaps.append(
                    {
                        "field": "drop_override_rows_still_present",
                        "reported": evidence.get("drop_override_rows_still_present"),
                        "expected": 0,
                    }
                )
            for counter_key in (
                "review_bucket_totals",
                "review_priority_totals",
                "scope_review_strategy_totals",
                "scope_verification_evidence_required_totals",
                "scope_apply_eligibility_totals",
                "metadata_enrichment_gate_totals",
                "source_gap_field_totals",
                "source_gap_class_totals",
                "source_of_truth_outcome_totals",
                "otc_review_decision_resolution_totals",
                "name_mismatch_class_counts",
                "name_mismatch_priority_counts",
                "name_mismatch_review_strategy_counts",
                "name_mismatch_apply_eligibility_counts",
                "name_mismatch_verification_evidence_required_counts",
                "post_scope_metadata_backlog_bucket_totals",
                "post_scope_metadata_backlog_gate_totals",
            ):
                counter = evidence.get(counter_key)
                if not isinstance(counter, dict) or any(not isinstance(value, int) or value < 0 for value in counter.values()):
                    otc_campaign_evidence_gaps.append({"field": counter_key, "reason": "expected_counter"})
            review_bucket_asset_type_totals = evidence.get("review_bucket_asset_type_totals")
            if not isinstance(review_bucket_asset_type_totals, dict) or any(
                not isinstance(asset_type_totals, dict)
                or any(not isinstance(value, int) or value < 0 for value in asset_type_totals.values())
                for asset_type_totals in (
                    review_bucket_asset_type_totals.values()
                    if isinstance(review_bucket_asset_type_totals, dict)
                    else []
                )
            ):
                otc_campaign_evidence_gaps.append(
                    {"field": "review_bucket_asset_type_totals", "reason": "expected_nested_counter"}
                )
            review_bucket_metadata_gate_totals = evidence.get("review_bucket_metadata_gate_totals")
            if not isinstance(review_bucket_metadata_gate_totals, dict) or any(
                not isinstance(metadata_gate_counts, dict)
                or any(not isinstance(value, int) or value < 0 for value in metadata_gate_counts.values())
                for metadata_gate_counts in (
                    review_bucket_metadata_gate_totals.values()
                    if isinstance(review_bucket_metadata_gate_totals, dict)
                    else []
                )
            ):
                otc_campaign_evidence_gaps.append(
                    {"field": "review_bucket_metadata_gate_totals", "reason": "expected_nested_counter"}
                )
            metadata_gate_totals = evidence.get("metadata_enrichment_gate_totals", {})
            otc_scope_completion = evidence.get("otc_scope_completion")
            post_scope_backlog = evidence.get("post_scope_metadata_backlog")
            if not isinstance(otc_scope_completion, dict):
                otc_campaign_evidence_gaps.append(
                    {"field": "otc_scope_completion", "reason": "expected_object"}
                )
            else:
                if otc_scope_completion.get("metadata_enrichment_authorized") is not False:
                    otc_campaign_evidence_gaps.append(
                        {
                            "field": "otc_scope_completion.metadata_enrichment_authorized",
                            "reported": otc_scope_completion.get("metadata_enrichment_authorized"),
                            "expected": False,
                        }
                    )
                if otc_scope_completion.get("scope_apply_allowed_rows") != 0:
                    otc_campaign_evidence_gaps.append(
                        {
                            "field": "otc_scope_completion.scope_apply_allowed_rows",
                            "reported": otc_scope_completion.get("scope_apply_allowed_rows"),
                            "expected": 0,
                        }
                    )
            if not isinstance(post_scope_backlog, dict):
                otc_campaign_evidence_gaps.append(
                    {"field": "post_scope_metadata_backlog", "reason": "expected_object"}
                )
            else:
                if post_scope_backlog.get("metadata_enrichment_authorized") is not False:
                    otc_campaign_evidence_gaps.append(
                        {
                            "field": "post_scope_metadata_backlog.metadata_enrichment_authorized",
                            "reported": post_scope_backlog.get("metadata_enrichment_authorized"),
                            "expected": False,
                        }
                    )
                if not post_scope_backlog.get("source_gate"):
                    otc_campaign_evidence_gaps.append(
                        {"field": "post_scope_metadata_backlog.source_gate", "reason": "missing_source_gate"}
                    )
            otc_review_rows = evidence.get("otc_review_rows")
            if isinstance(otc_review_rows, int):
                if isinstance(otc_scope_completion, dict):
                    if otc_scope_completion.get("rows") != otc_review_rows:
                        otc_campaign_evidence_gaps.append(
                            {
                                "field": "otc_scope_completion.rows",
                                "reported": otc_scope_completion.get("rows"),
                                "otc_review_rows": otc_review_rows,
                            }
                        )
                    if (
                        evidence.get("otc_core_exclusion_candidate_rows") == 0
                        and otc_scope_completion.get("status") != "complete_extended_scope_no_core_candidates"
                    ):
                        otc_campaign_evidence_gaps.append(
                            {
                                "field": "otc_scope_completion.status",
                                "reported": otc_scope_completion.get("status"),
                                "expected": "complete_extended_scope_no_core_candidates",
                            }
                        )
                for counter_key in (
                    "review_bucket_totals",
                    "review_priority_totals",
                    "scope_review_strategy_totals",
                    "scope_apply_eligibility_totals",
                    "metadata_enrichment_gate_totals",
                ):
                    counter = evidence.get(counter_key)
                    if isinstance(counter, dict) and sum(counter.values()) != otc_review_rows:
                        otc_campaign_evidence_gaps.append(
                            {
                                "field": counter_key,
                                "reported_total": sum(counter.values()),
                                "otc_review_rows": otc_review_rows,
                            }
                        )
                review_bucket_totals = evidence.get("review_bucket_totals")
                if isinstance(review_bucket_totals, dict) and isinstance(review_bucket_asset_type_totals, dict):
                    for bucket, count in review_bucket_totals.items():
                        asset_type_counts = review_bucket_asset_type_totals.get(bucket)
                        if not isinstance(asset_type_counts, dict) or sum(asset_type_counts.values()) != count:
                            otc_campaign_evidence_gaps.append(
                                {
                                    "field": f"review_bucket_asset_type_totals.{bucket}",
                                    "reported_total": sum(asset_type_counts.values())
                                    if isinstance(asset_type_counts, dict)
                                    else None,
                                    "expected": count,
                                }
                            )
                if isinstance(review_bucket_totals, dict) and isinstance(review_bucket_metadata_gate_totals, dict):
                    for bucket, count in review_bucket_totals.items():
                        metadata_gate_counts = review_bucket_metadata_gate_totals.get(bucket)
                        if not isinstance(metadata_gate_counts, dict) or sum(metadata_gate_counts.values()) != count:
                            otc_campaign_evidence_gaps.append(
                                {
                                    "field": f"review_bucket_metadata_gate_totals.{bucket}",
                                    "reported_total": sum(metadata_gate_counts.values())
                                    if isinstance(metadata_gate_counts, dict)
                                    else None,
                                    "expected": count,
                                }
                            )
                if isinstance(post_scope_backlog, dict):
                    post_scope_rows = post_scope_backlog.get("rows")
                    bucket_totals = evidence.get("post_scope_metadata_backlog_bucket_totals")
                    gate_totals = evidence.get("post_scope_metadata_backlog_gate_totals")
                    examples = evidence.get("post_scope_metadata_backlog_examples")
                    if not isinstance(post_scope_rows, int) or isinstance(post_scope_rows, bool) or post_scope_rows < 0:
                        otc_campaign_evidence_gaps.append(
                            {"field": "post_scope_metadata_backlog.rows", "reason": "expected_nonnegative_integer"}
                        )
                    else:
                        for counter_key, counter in (
                            ("post_scope_metadata_backlog_bucket_totals", bucket_totals),
                            ("post_scope_metadata_backlog_gate_totals", gate_totals),
                        ):
                            if isinstance(counter, dict) and sum(counter.values()) != post_scope_rows:
                                otc_campaign_evidence_gaps.append(
                                    {
                                        "field": counter_key,
                                        "reported_total": sum(counter.values()),
                                        "post_scope_metadata_backlog_rows": post_scope_rows,
                                    }
                                )
                        if not isinstance(examples, list):
                            otc_campaign_evidence_gaps.append(
                                {"field": "post_scope_metadata_backlog_examples", "reason": "expected_list"}
                            )
                        elif post_scope_rows > 0 and not examples:
                            otc_campaign_evidence_gaps.append(
                                {"field": "post_scope_metadata_backlog_examples", "reason": "examples_required"}
                            )
                        else:
                            required_example_fields = {
                                "listing_key",
                                "ticker",
                                "asset_type",
                                "review_bucket",
                                "metadata_enrichment_gate",
                                "verification_evidence_required",
                                "recommended_next_source",
                                "source_gate",
                            }
                            for example_index, example in enumerate(examples):
                                if not isinstance(example, dict) or not required_example_fields.issubset(example):
                                    otc_campaign_evidence_gaps.append(
                                        {
                                            "field": "post_scope_metadata_backlog_examples",
                                            "row_index": example_index,
                                            "reason": "expected_listing_keyed_metadata_backlog_example",
                                        }
                                    )
            source_gap_rows = evidence.get("source_gap_rows")
            if isinstance(source_gap_rows, int):
                for counter_key in (
                    "source_gap_field_totals",
                    "source_gap_class_totals",
                    "source_of_truth_outcome_totals",
                ):
                    counter = evidence.get(counter_key)
                    if isinstance(counter, dict) and sum(counter.values()) != source_gap_rows:
                        otc_campaign_evidence_gaps.append(
                            {
                                "field": counter_key,
                                "reported_total": sum(counter.values()),
                                "source_gap_rows": source_gap_rows,
                            }
                        )
                truth_outcomes = evidence.get("source_of_truth_outcome_totals")
                if isinstance(truth_outcomes, dict) and truth_outcomes.get("accepted_source_gap") != source_gap_rows:
                    otc_campaign_evidence_gaps.append(
                        {
                            "field": "source_of_truth_outcome_totals.accepted_source_gap",
                            "reported": truth_outcomes.get("accepted_source_gap"),
                            "source_gap_rows": source_gap_rows,
                        }
                    )
            if isinstance(metadata_gate_totals, dict):
                required_gates = {
                    "otc_name_mismatch_review_required_before_name_or_metadata_changes",
                    "reviewed_issuer_sector_source_required_keep_blank",
                    "reviewed_product_category_source_required_keep_blank",
                    "source_gap_review_required_before_enrichment",
                }
                missing_gates = [gate for gate in sorted(required_gates) if gate not in metadata_gate_totals]
                if missing_gates:
                    otc_campaign_evidence_gaps.append(
                        {"field": "metadata_enrichment_gate_totals", "missing_keys": missing_gates}
                    )
                forbidden_apply_gates = [
                    gate for gate in metadata_gate_totals if str(gate).startswith("apply") or "fill_from" in str(gate)
                ]
                if forbidden_apply_gates:
                    otc_campaign_evidence_gaps.append(
                        {"field": "metadata_enrichment_gate_totals", "forbidden_keys": forbidden_apply_gates}
                    )
            name_rows = evidence.get("name_mismatch_review_rows")
            reviewed_active_name_rows = evidence.get("otc_review_decision_active_name_mismatch_rows")
            unreviewed_active_name_rows = evidence.get("otc_name_mismatch_unreviewed_active_rows")
            suppressed_current_listing_rows = evidence.get("otc_review_decision_current_listing_suppressed_rows")
            not_current_scope_rows = evidence.get("otc_review_decision_not_current_scope_rows")
            stale_review_decision_rows = evidence.get("otc_review_decision_stale_rows")
            if all(
                isinstance(value, int) and not isinstance(value, bool)
                for value in (name_rows, reviewed_active_name_rows, unreviewed_active_name_rows)
            ):
                if reviewed_active_name_rows + unreviewed_active_name_rows != name_rows:
                    otc_campaign_evidence_gaps.append(
                        {
                            "field": "otc_name_mismatch_review_balance",
                            "reviewed_active": reviewed_active_name_rows,
                            "unreviewed_active": unreviewed_active_name_rows,
                            "name_mismatch_review_rows": name_rows,
                        }
                    )
            if all(
                isinstance(value, int) and not isinstance(value, bool)
                for value in (suppressed_current_listing_rows, not_current_scope_rows, stale_review_decision_rows)
            ):
                if suppressed_current_listing_rows + not_current_scope_rows != stale_review_decision_rows:
                    otc_campaign_evidence_gaps.append(
                        {
                            "field": "otc_review_decision_stale_resolution_balance",
                            "suppressed_current_listing": suppressed_current_listing_rows,
                            "not_current_scope": not_current_scope_rows,
                            "stale_review_decision_rows": stale_review_decision_rows,
                        }
                    )
            review_decision_resolution_totals = evidence.get("otc_review_decision_resolution_totals")
            if isinstance(review_decision_resolution_totals, dict) and all(
                isinstance(value, int) and not isinstance(value, bool)
                for value in (
                    reviewed_active_name_rows,
                    unreviewed_active_name_rows,
                    suppressed_current_listing_rows,
                    not_current_scope_rows,
                )
            ):
                expected_resolution_totals = {
                    "pending_active_name_mismatch_review": unreviewed_active_name_rows,
                    "reviewed_decision_covers_active_name_mismatch": reviewed_active_name_rows,
                    "reviewed_decision_suppresses_current_listing_warning": suppressed_current_listing_rows,
                    "reviewed_decision_not_in_current_otc_scope": not_current_scope_rows,
                }
                expected_resolution_totals = {
                    key: value for key, value in expected_resolution_totals.items() if value > 0
                }
                if review_decision_resolution_totals != expected_resolution_totals:
                    otc_campaign_evidence_gaps.append(
                        {
                            "field": "otc_review_decision_resolution_totals",
                            "reported": review_decision_resolution_totals,
                            "expected": expected_resolution_totals,
                        }
                    )
            for counter_key in (
                "name_mismatch_class_counts",
                "name_mismatch_priority_counts",
                "name_mismatch_review_strategy_counts",
                "name_mismatch_apply_eligibility_counts",
                "name_mismatch_verification_evidence_required_counts",
            ):
                counter = evidence.get(counter_key)
                if isinstance(name_rows, int) and isinstance(counter, dict) and sum(counter.values()) != name_rows:
                    otc_campaign_evidence_gaps.append(
                        {
                            "field": counter_key,
                            "reported_total": sum(counter.values()),
                            "name_mismatch_review_rows": name_rows,
                        }
                    )
            scope_strategy_totals = evidence.get("scope_review_strategy_totals")
            scope_evidence_totals = evidence.get("scope_verification_evidence_required_totals")
            review_bucket_totals = evidence.get("review_bucket_totals")
            if isinstance(scope_strategy_totals, dict) and isinstance(review_bucket_totals, dict):
                expected_scope_strategy_totals = Counter()
                for bucket, count in review_bucket_totals.items():
                    if isinstance(count, int) and not isinstance(count, bool):
                        expected_scope_strategy_totals[otc_scope_review_strategy(str(bucket))] += count
                if scope_strategy_totals != dict(expected_scope_strategy_totals):
                    otc_campaign_evidence_gaps.append(
                        {
                            "field": "scope_review_strategy_totals",
                            "reported": scope_strategy_totals,
                            "expected": dict(expected_scope_strategy_totals),
                        }
                    )
            if isinstance(scope_evidence_totals, dict) and isinstance(review_bucket_totals, dict):
                expected_scope_evidence_totals = Counter()
                for bucket, count in review_bucket_totals.items():
                    if isinstance(count, int) and not isinstance(count, bool):
                        expected_scope_evidence_totals[
                            otc_scope_verification_evidence_required(str(bucket))
                        ] += count
                if scope_evidence_totals != dict(expected_scope_evidence_totals):
                    otc_campaign_evidence_gaps.append(
                        {
                            "field": "scope_verification_evidence_required_totals",
                            "reported": scope_evidence_totals,
                            "expected": dict(expected_scope_evidence_totals),
                        }
                    )
            name_strategy_counts = evidence.get("name_mismatch_review_strategy_counts")
            name_class_counts = evidence.get("name_mismatch_class_counts")
            if isinstance(name_strategy_counts, dict) and isinstance(name_class_counts, dict):
                expected_name_strategy_counts = Counter()
                for review_class, count in name_class_counts.items():
                    if isinstance(count, int) and not isinstance(count, bool):
                        expected_name_strategy_counts[otc_name_mismatch_review_strategy(str(review_class))] += count
                if name_strategy_counts != dict(expected_name_strategy_counts):
                    otc_campaign_evidence_gaps.append(
                        {
                            "field": "name_mismatch_review_strategy_counts",
                            "reported": name_strategy_counts,
                            "expected": dict(expected_name_strategy_counts),
                        }
                    )
            for top_key, required_fields in (
                (
                    "top_otc_scope_review_batches",
                    {
                        "review_priority",
                        "review_bucket",
                        "asset_type",
                        "quality_status",
                        "metadata_enrichment_gate",
                        "rows",
                        "review_strategy",
                        "verification_evidence_required",
                        "recommended_next_source",
                        "source_gate",
                    },
                ),
                (
                    "top_otc_name_mismatch_review_batches",
                    {
                        "review_priority",
                        "review_class",
                        "isin_presence",
                        "official_sources",
                        "rows",
                        "review_strategy",
                        "verification_evidence_required",
                        "recommended_next_source",
                        "source_gate",
                    },
                ),
            ):
                top_rows = evidence.get(top_key)
                if not isinstance(top_rows, list) or not top_rows:
                    otc_campaign_evidence_gaps.append({"field": top_key, "reason": "expected_ranked_review_batches"})
                    continue
                for index, batch in enumerate(top_rows):
                    if not isinstance(batch, dict) or not required_fields.issubset(batch):
                        otc_campaign_evidence_gaps.append(
                            {"field": top_key, "row_index": index, "reason": "expected_ranked_review_batch_fields"}
                        )
                        continue
                    if not isinstance(batch.get("rows"), int) or batch.get("rows", 0) <= 0:
                        otc_campaign_evidence_gaps.append(
                            {"field": top_key, "row_index": index, "reason": "expected_positive_rows"}
                        )
                    if not batch.get("recommended_next_source") or not batch.get("source_gate"):
                        otc_campaign_evidence_gaps.append(
                            {"field": top_key, "row_index": index, "reason": "expected_source_instruction_fields"}
                        )
                    if top_key == "top_otc_scope_review_batches":
                        expected_strategy = otc_scope_review_strategy(str(batch.get("review_bucket", "")))
                        expected_evidence = otc_scope_verification_evidence_required(
                            str(batch.get("review_bucket", ""))
                        )
                    else:
                        review_class = str(batch.get("review_class", ""))
                        expected_strategy = otc_name_mismatch_review_strategy(review_class)
                        expected_evidence = str(batch.get("verification_evidence_required", ""))
                    if batch.get("review_strategy") != expected_strategy:
                        otc_campaign_evidence_gaps.append(
                            {
                                "field": top_key,
                                "row_index": index,
                                "reported": batch.get("review_strategy"),
                                "expected": expected_strategy,
                            }
                        )
                    if batch.get("verification_evidence_required") != expected_evidence:
                        otc_campaign_evidence_gaps.append(
                            {
                                "field": top_key,
                                "row_index": index,
                                "reported": batch.get("verification_evidence_required"),
                                "expected": expected_evidence,
                            }
                        )
                    if top_key == "top_otc_name_mismatch_review_batches":
                        expected_next_source = otc_name_mismatch_recommended_next_source(review_class)
                        expected_source_gate = otc_name_mismatch_source_gate(review_class)
                        if batch.get("recommended_next_source") != expected_next_source:
                            otc_campaign_evidence_gaps.append(
                                {
                                    "field": top_key,
                                    "row_index": index,
                                    "reported": batch.get("recommended_next_source"),
                                    "expected": expected_next_source,
                                }
                            )
                        if batch.get("source_gate") != expected_source_gate:
                            otc_campaign_evidence_gaps.append(
                                {
                                    "field": top_key,
                                    "row_index": index,
                                    "reported": batch.get("source_gate"),
                                    "expected": expected_source_gate,
                                }
                            )
            known_deltas = delta_evidence.get("known_deltas") if isinstance(delta_evidence, dict) else {}
            if not isinstance(known_deltas, dict):
                known_deltas = {}
            missing_delta_evidence_keys = [
                key for key in OTC_CAMPAIGN_DELTA_EVIDENCE_KEYS if key not in known_deltas
            ]
            if missing_delta_evidence_keys:
                otc_campaign_evidence_gaps.append(
                    {"field": "delta_evidence.known_deltas", "missing_keys": missing_delta_evidence_keys}
                )
            if known_deltas.get("drop_override_rows_still_present") not in {0, "0"}:
                otc_campaign_evidence_gaps.append(
                    {
                        "field": "delta_evidence.drop_override_rows_still_present",
                        "reported": known_deltas.get("drop_override_rows_still_present"),
                        "expected": 0,
                    }
                )
            if isinstance(source_gap_rows, int) and known_deltas.get("accepted_source_gap_rows") != source_gap_rows:
                otc_campaign_evidence_gaps.append(
                    {
                        "field": "delta_evidence.accepted_source_gap_rows",
                        "reported": known_deltas.get("accepted_source_gap_rows"),
                        "expected": source_gap_rows,
                    }
                )
            for key in (
                "otc_name_mismatch_unreviewed_active_rows",
                "otc_review_decision_current_listing_suppressed_rows",
                "otc_review_decision_not_current_scope_rows",
                "otc_review_decision_stale_rows",
            ):
                expected = evidence.get(key)
                if isinstance(expected, int) and known_deltas.get(key) != expected:
                    otc_campaign_evidence_gaps.append(
                        {
                            "field": f"delta_evidence.{key}",
                            "reported": known_deltas.get(key),
                            "expected": expected,
                        }
                    )
            if isinstance(name_rows, int) and known_deltas.get("active_name_mismatch_review_rows") != name_rows:
                otc_campaign_evidence_gaps.append(
                    {
                        "field": "delta_evidence.active_name_mismatch_review_rows",
                        "reported": known_deltas.get("active_name_mismatch_review_rows"),
                        "expected": name_rows,
                    }
                )
            expected_scope_snapshot = matrix.get("campaign_metric_deltas", {})
            if known_deltas.get("campaign_start_scope_snapshot") != expected_scope_snapshot:
                otc_campaign_evidence_gaps.append(
                    {
                        "field": "delta_evidence.campaign_start_scope_snapshot",
                        "reported": known_deltas.get("campaign_start_scope_snapshot"),
                        "expected": expected_scope_snapshot,
                    }
                )
            global_acceptance_deltas = matrix.get("global_acceptance_deltas", {})
            if not isinstance(global_acceptance_deltas, dict):
                global_acceptance_deltas = {}
            expected_warn_quarantine_delta = {
                "warn_delta": global_acceptance_deltas.get("warn_delta", {}).get("delta", 0),
                "quarantine_delta": global_acceptance_deltas.get("quarantine_delta", {}).get("delta", 0),
            }
            if known_deltas.get("warn_quarantine_delta") != expected_warn_quarantine_delta:
                otc_campaign_evidence_gaps.append(
                    {
                        "field": "delta_evidence.warn_quarantine_delta",
                        "reported": known_deltas.get("warn_quarantine_delta"),
                        "expected": expected_warn_quarantine_delta,
                    }
                )
        if campaign_key == "canada":
            evidence = campaign.get("evidence", {})
            delta_evidence = campaign.get("delta_evidence", {})
            if not isinstance(evidence, dict):
                canada_campaign_evidence_gaps.append({"field": "evidence", "reason": "expected_object"})
                evidence = {}
            missing_evidence_keys = [
                key for key in CANADA_CAMPAIGN_EVIDENCE_KEYS if key not in evidence
            ]
            if missing_evidence_keys:
                canada_campaign_evidence_gaps.append(
                    {"field": "evidence", "missing_keys": missing_evidence_keys}
                )
            for key in (
                "canada_residual_rows",
                "active_figi_queue_rows",
                "missing_isin_rows",
                "missing_figi_rows",
                "canada_core_exclusion_candidate_rows",
                "reviewed_openfigi_source_gap_rows",
                "applied_figi_rows",
                "openfigi_gap_rows_added",
            ):
                if not isinstance(evidence.get(key), int) or isinstance(evidence.get(key), bool) or evidence.get(key, -1) < 0:
                    canada_campaign_evidence_gaps.append({"field": key, "reason": "expected_nonnegative_integer"})
            canada_identifier_backlog = evidence.get("canada_identifier_backlog")
            if not isinstance(canada_identifier_backlog, dict):
                canada_campaign_evidence_gaps.append({"field": "canada_identifier_backlog", "reason": "expected_object"})
            else:
                for key in (
                    "rows",
                    "scope_decision_required_rows",
                    "official_isin_source_required_rows",
                    "figi_blocked_until_isin_rows",
                    "reviewed_openfigi_source_gap_rows",
                    "openfigi_candidate_rows",
                    "direct_identifier_apply_allowed_rows",
                ):
                    value = canada_identifier_backlog.get(key)
                    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                        canada_campaign_evidence_gaps.append(
                            {"field": f"canada_identifier_backlog.{key}", "reason": "expected_nonnegative_integer"}
                        )
                if canada_identifier_backlog.get("metadata_enrichment_authorized") is not False:
                    canada_campaign_evidence_gaps.append(
                        {
                            "field": "canada_identifier_backlog.metadata_enrichment_authorized",
                            "reported": canada_identifier_backlog.get("metadata_enrichment_authorized"),
                            "expected": False,
                        }
                    )
                if canada_identifier_backlog.get("direct_identifier_apply_allowed_rows") != 0:
                    canada_campaign_evidence_gaps.append(
                        {
                            "field": "canada_identifier_backlog.direct_identifier_apply_allowed_rows",
                            "reported": canada_identifier_backlog.get("direct_identifier_apply_allowed_rows"),
                            "expected": 0,
                        }
                    )
                if not canada_identifier_backlog.get("source_gate"):
                    canada_campaign_evidence_gaps.append(
                        {"field": "canada_identifier_backlog.source_gate", "reason": "missing_source_gate"}
                    )
            for counter_key in (
                "exchange_totals",
                "canada_core_exclusion_candidate_exchange_totals",
                "canada_core_exclusion_candidate_asset_type_totals",
                "official_masterfile_source_totals",
                "canada_resolution_queue_totals",
                "canada_identifier_backlog_queue_totals",
                "canada_identifier_backlog_evidence_required_totals",
                "source_gap_field_totals",
                "source_gap_class_totals",
                "source_of_truth_outcome_totals",
                "openfigi_review_status_totals",
                "openfigi_review_decision_totals",
                "isin_apply_eligibility_totals",
                "figi_apply_eligibility_totals",
                "verification_evidence_required_totals",
                "figi_queue_apply_eligibility_totals",
                "figi_queue_verification_evidence_required_totals",
            ):
                counter = evidence.get(counter_key)
                if not isinstance(counter, dict) or any(not isinstance(value, int) or value < 0 for value in counter.values()):
                    canada_campaign_evidence_gaps.append({"field": counter_key, "reason": "expected_counter"})
            canada_residual_rows = evidence.get("canada_residual_rows")
            if isinstance(canada_residual_rows, int):
                for counter_key in (
                    "exchange_totals",
                    "isin_apply_eligibility_totals",
                    "figi_apply_eligibility_totals",
                    "verification_evidence_required_totals",
                    "canada_resolution_queue_totals",
                ):
                    counter = evidence.get(counter_key)
                    if isinstance(counter, dict) and sum(counter.values()) != canada_residual_rows:
                        canada_campaign_evidence_gaps.append(
                            {
                                "field": counter_key,
                                "reported_total": sum(counter.values()),
                                "canada_residual_rows": canada_residual_rows,
                            }
                        )
            missing_isin_rows = evidence.get("missing_isin_rows")
            missing_figi_rows = evidence.get("missing_figi_rows")
            if isinstance(canada_identifier_backlog, dict):
                backlog_rows = canada_identifier_backlog.get("rows")
                if isinstance(backlog_rows, int) and isinstance(missing_isin_rows, int) and isinstance(missing_figi_rows, int):
                    if backlog_rows < max(missing_isin_rows, missing_figi_rows):
                        canada_campaign_evidence_gaps.append(
                            {
                                "field": "canada_identifier_backlog.rows",
                                "reported": backlog_rows,
                                "minimum_expected": max(missing_isin_rows, missing_figi_rows),
                            }
                        )
                scope_required_rows = canada_identifier_backlog.get("scope_decision_required_rows")
                core_exclusion_rows = evidence.get("canada_core_exclusion_candidate_rows")
                if (
                    isinstance(scope_required_rows, int)
                    and isinstance(core_exclusion_rows, int)
                    and scope_required_rows > core_exclusion_rows
                ):
                    canada_campaign_evidence_gaps.append(
                        {
                            "field": "canada_identifier_backlog.scope_decision_required_rows",
                            "reported": scope_required_rows,
                            "canada_core_exclusion_candidate_rows": core_exclusion_rows,
                        }
                    )
            exchange_totals = evidence.get("exchange_totals")
            if isinstance(exchange_totals, dict):
                unexpected_exchanges = sorted(str(exchange) for exchange in exchange_totals if exchange not in CANADA_EXCHANGES)
                if unexpected_exchanges:
                    canada_campaign_evidence_gaps.append(
                        {"field": "exchange_totals", "unexpected_exchanges": unexpected_exchanges}
                    )
            core_exclusion_rows = evidence.get("canada_core_exclusion_candidate_rows")
            core_exclusion_exchange_totals = evidence.get("canada_core_exclusion_candidate_exchange_totals")
            core_exclusion_asset_type_totals = evidence.get("canada_core_exclusion_candidate_asset_type_totals")
            core_exclusion_resolution_queue_totals = evidence.get(
                "canada_core_exclusion_candidate_resolution_queue_totals"
            )
            core_exclusion_official_source_totals = evidence.get(
                "canada_core_exclusion_candidate_official_source_totals"
            )
            core_exclusion_source_gap_class_totals = evidence.get(
                "canada_core_exclusion_candidate_source_gap_class_totals"
            )
            if isinstance(core_exclusion_rows, int):
                for counter_key, counter in (
                    ("canada_core_exclusion_candidate_exchange_totals", core_exclusion_exchange_totals),
                    ("canada_core_exclusion_candidate_asset_type_totals", core_exclusion_asset_type_totals),
                    (
                        "canada_core_exclusion_candidate_resolution_queue_totals",
                        core_exclusion_resolution_queue_totals,
                    ),
                ):
                    if isinstance(counter, dict) and sum(counter.values()) != core_exclusion_rows:
                        canada_campaign_evidence_gaps.append(
                            {
                                "field": counter_key,
                                "reported_total": sum(counter.values()),
                                "canada_core_exclusion_candidate_rows": core_exclusion_rows,
                            }
                        )
                for counter_key, counter in (
                    ("canada_core_exclusion_candidate_official_source_totals", core_exclusion_official_source_totals),
                    ("canada_core_exclusion_candidate_source_gap_class_totals", core_exclusion_source_gap_class_totals),
                ):
                    if not isinstance(counter, dict) or not counter:
                        canada_campaign_evidence_gaps.append(
                            {"field": counter_key, "reason": "expected_nonempty_counter"}
                        )
            if isinstance(core_exclusion_exchange_totals, dict):
                unexpected_core_exclusion_exchanges = sorted(
                    str(exchange) for exchange in core_exclusion_exchange_totals if exchange not in CANADA_EXCHANGES
                )
                if unexpected_core_exclusion_exchanges:
                    canada_campaign_evidence_gaps.append(
                        {
                            "field": "canada_core_exclusion_candidate_exchange_totals",
                            "unexpected_exchanges": unexpected_core_exclusion_exchanges,
                        }
                    )
            resolution_totals = evidence.get("canada_resolution_queue_totals")
            resolution_exchange_totals = evidence.get("canada_resolution_queue_exchange_totals")
            resolution_asset_type_totals = evidence.get("canada_resolution_queue_asset_type_totals")
            resolution_source_gap_class_totals = evidence.get("canada_resolution_queue_source_gap_class_totals")
            resolution_official_source_totals = evidence.get("canada_resolution_queue_official_source_totals")
            resolution_strategy_totals = evidence.get("canada_resolution_queue_review_strategy_totals")
            resolution_evidence_totals = evidence.get("canada_resolution_queue_evidence_required_totals")
            for nested_counter_key, nested_counter in (
                ("canada_resolution_queue_exchange_totals", resolution_exchange_totals),
                ("canada_resolution_queue_asset_type_totals", resolution_asset_type_totals),
                ("canada_resolution_queue_source_gap_class_totals", resolution_source_gap_class_totals),
                ("canada_resolution_queue_official_source_totals", resolution_official_source_totals),
                ("canada_resolution_queue_review_strategy_totals", resolution_strategy_totals),
                ("canada_resolution_queue_evidence_required_totals", resolution_evidence_totals),
            ):
                if not isinstance(nested_counter, dict) or any(
                    not isinstance(counter, dict)
                    or any(not isinstance(value, int) or value < 0 for value in counter.values())
                    for counter in (nested_counter.values() if isinstance(nested_counter, dict) else [])
                ):
                    canada_campaign_evidence_gaps.append({"field": nested_counter_key, "reason": "expected_nested_counter"})
            if isinstance(canada_residual_rows, int) and isinstance(resolution_totals, dict):
                if sum(resolution_totals.values()) != canada_residual_rows:
                    canada_campaign_evidence_gaps.append(
                        {
                            "field": "canada_resolution_queue_totals",
                            "reported_total": sum(resolution_totals.values()),
                            "canada_residual_rows": canada_residual_rows,
                        }
                        )
            if isinstance(resolution_totals, dict) and isinstance(resolution_exchange_totals, dict):
                for queue, total in resolution_totals.items():
                    exchange_counter = resolution_exchange_totals.get(queue)
                    if not isinstance(exchange_counter, dict) or sum(exchange_counter.values()) != total:
                        canada_campaign_evidence_gaps.append(
                            {
                                "field": "canada_resolution_queue_exchange_totals",
                                "queue": queue,
                                "reported_total": sum(exchange_counter.values()) if isinstance(exchange_counter, dict) else None,
                                "expected": total,
                            }
                        )
            if isinstance(resolution_totals, dict) and isinstance(resolution_asset_type_totals, dict):
                for queue, total in resolution_totals.items():
                    asset_type_counter = resolution_asset_type_totals.get(queue)
                    if not isinstance(asset_type_counter, dict) or sum(asset_type_counter.values()) != total:
                        canada_campaign_evidence_gaps.append(
                            {
                                "field": "canada_resolution_queue_asset_type_totals",
                                "queue": queue,
                                "reported_total": sum(asset_type_counter.values()) if isinstance(asset_type_counter, dict) else None,
                                "expected": total,
                            }
                        )
            if isinstance(resolution_totals, dict) and isinstance(resolution_source_gap_class_totals, dict):
                for queue in resolution_totals:
                    source_gap_class_counter = resolution_source_gap_class_totals.get(queue)
                    if not isinstance(source_gap_class_counter, dict) or not source_gap_class_counter:
                        canada_campaign_evidence_gaps.append(
                            {
                                "field": "canada_resolution_queue_source_gap_class_totals",
                                "queue": queue,
                                "reason": "expected_nonempty_counter",
                            }
                        )
            if isinstance(resolution_totals, dict) and isinstance(resolution_official_source_totals, dict):
                for queue in resolution_totals:
                    official_source_counter = resolution_official_source_totals.get(queue)
                    if not isinstance(official_source_counter, dict) or not official_source_counter:
                        canada_campaign_evidence_gaps.append(
                            {
                                "field": "canada_resolution_queue_official_source_totals",
                                "queue": queue,
                                "reason": "expected_nonempty_counter",
                            }
                        )
            if isinstance(resolution_totals, dict) and isinstance(resolution_strategy_totals, dict):
                for queue, total in resolution_totals.items():
                    strategy_counter = resolution_strategy_totals.get(queue)
                    if not isinstance(strategy_counter, dict) or sum(strategy_counter.values()) != total:
                        canada_campaign_evidence_gaps.append(
                            {
                                "field": "canada_resolution_queue_review_strategy_totals",
                                "queue": queue,
                                "reported_total": sum(strategy_counter.values()) if isinstance(strategy_counter, dict) else None,
                                "expected": total,
                            }
                        )
            if isinstance(resolution_totals, dict) and isinstance(resolution_evidence_totals, dict):
                for queue, total in resolution_totals.items():
                    evidence_counter = resolution_evidence_totals.get(queue)
                    if not isinstance(evidence_counter, dict) or sum(evidence_counter.values()) != total:
                        canada_campaign_evidence_gaps.append(
                            {
                                "field": "canada_resolution_queue_evidence_required_totals",
                                "queue": queue,
                                "reported_total": sum(evidence_counter.values())
                                if isinstance(evidence_counter, dict)
                                else None,
                                "expected": total,
                            }
                        )
                    elif any(strategy not in CANADA_REVIEW_STRATEGIES for strategy in strategy_counter):
                        canada_campaign_evidence_gaps.append(
                            {
                                "field": "canada_resolution_queue_review_strategy_totals",
                                "queue": queue,
                                "unexpected_strategies": sorted(
                                    strategy for strategy in strategy_counter if strategy not in CANADA_REVIEW_STRATEGIES
                                ),
                            }
                        )
            top_canada_batches = evidence.get("top_canada_resolution_review_batches")
            if (
                not isinstance(top_canada_batches, list)
                or not top_canada_batches
                or any(
                    not isinstance(batch, dict)
                    or batch.get("exchange") not in CANADA_EXCHANGES
                    or not batch.get("canada_resolution_queue")
                    or not batch.get("official_source")
                    or not isinstance(batch.get("rows"), int)
                    or batch.get("rows", 0) <= 0
                    or batch.get("review_strategy") not in CANADA_REVIEW_STRATEGIES
                    or not batch.get("evidence_required")
                    or batch.get("evidence_required") in {"none", "ticker_match_only"}
                    or not batch.get("recommended_next_source")
                    or not batch.get("source_gate")
                    for batch in top_canada_batches
                )
            ):
                canada_campaign_evidence_gaps.append(
                    {
                        "field": "top_canada_resolution_review_batches",
                        "reason": "expected_ranked_canada_strategy_rows",
                    }
                )
            source_gap_fields = evidence.get("source_gap_field_totals")
            missing_isin_rows = evidence.get("missing_isin_rows")
            if isinstance(source_gap_fields, dict) and isinstance(missing_isin_rows, int):
                if source_gap_fields.get("missing_isin_primary") != missing_isin_rows:
                    canada_campaign_evidence_gaps.append(
                        {
                            "field": "source_gap_field_totals.missing_isin_primary",
                            "reported": source_gap_fields.get("missing_isin_primary"),
                            "missing_isin_rows": missing_isin_rows,
                        }
                    )
            reviewed_openfigi_rows = evidence.get("reviewed_openfigi_source_gap_rows")
            if isinstance(reviewed_openfigi_rows, int):
                for counter_key in ("openfigi_review_status_totals", "openfigi_review_decision_totals"):
                    counter = evidence.get(counter_key)
                    if isinstance(counter, dict) and sum(counter.values()) != reviewed_openfigi_rows:
                        canada_campaign_evidence_gaps.append(
                            {
                                "field": counter_key,
                                "reported_total": sum(counter.values()),
                                "reviewed_openfigi_source_gap_rows": reviewed_openfigi_rows,
                            }
                        )
            queue_rows = evidence.get("active_figi_queue_rows")
            queue_apply = evidence.get("figi_queue_apply_eligibility_totals")
            if isinstance(queue_rows, int) and isinstance(queue_apply, dict):
                if queue_rows == 0 and "no_active_openfigi_probe_rows" not in queue_apply:
                    canada_campaign_evidence_gaps.append(
                        {"field": "figi_queue_apply_eligibility_totals", "missing_keys": ["no_active_openfigi_probe_rows"]}
                    )
                forbidden_queue_keys = [
                    key for key in queue_apply if "apply_figi" in str(key) or str(key).startswith("apply")
                ]
                if forbidden_queue_keys:
                    canada_campaign_evidence_gaps.append(
                        {"field": "figi_queue_apply_eligibility_totals", "forbidden_keys": forbidden_queue_keys}
                    )
            queue_strategy_totals = evidence.get("figi_queue_review_strategy_totals")
            if not isinstance(queue_strategy_totals, dict) or any(
                not isinstance(value, int) or isinstance(value, bool) or value < 0
                for value in (queue_strategy_totals.values() if isinstance(queue_strategy_totals, dict) else [])
            ):
                canada_campaign_evidence_gaps.append(
                    {"field": "figi_queue_review_strategy_totals", "reason": "expected_counter"}
                )
            else:
                if isinstance(queue_rows, int) and queue_rows == 0:
                    if queue_strategy_totals.get("no_active_openfigi_probe_rows_after_gates") != 1:
                        canada_campaign_evidence_gaps.append(
                            {
                                "field": "figi_queue_review_strategy_totals.no_active_openfigi_probe_rows_after_gates",
                                "reported": queue_strategy_totals.get("no_active_openfigi_probe_rows_after_gates"),
                                "expected": 1,
                            }
                        )
                reviewed_openfigi_rows = evidence.get("reviewed_openfigi_source_gap_rows")
                if isinstance(reviewed_openfigi_rows, int) and reviewed_openfigi_rows > 0:
                    if (
                        queue_strategy_totals.get("keep_reviewed_openfigi_source_gaps_excluded_until_stronger_source")
                        != reviewed_openfigi_rows
                    ):
                        canada_campaign_evidence_gaps.append(
                            {
                                "field": "figi_queue_review_strategy_totals.keep_reviewed_openfigi_source_gaps_excluded_until_stronger_source",
                                "reported": queue_strategy_totals.get(
                                    "keep_reviewed_openfigi_source_gaps_excluded_until_stronger_source"
                                ),
                                "expected": reviewed_openfigi_rows,
                            }
                        )
            top_figi_queue_batches = evidence.get("top_canada_figi_queue_review_batches")
            if not isinstance(top_figi_queue_batches, list) or not top_figi_queue_batches:
                canada_campaign_evidence_gaps.append(
                    {"field": "top_canada_figi_queue_review_batches", "reason": "expected_ranked_review_batches"}
                )
            else:
                required_batch_fields = {
                    "exchange",
                    "asset_type",
                    "openfigi_exchange_hint",
                    "rows",
                    "review_strategy",
                    "apply_eligibility",
                    "verification_evidence_required",
                    "recommended_next_source",
                    "source_gate",
                }
                for index, batch in enumerate(top_figi_queue_batches):
                    if not isinstance(batch, dict) or not required_batch_fields.issubset(batch):
                        canada_campaign_evidence_gaps.append(
                            {
                                "field": "top_canada_figi_queue_review_batches",
                                "row_index": index,
                                "reason": "expected_ranked_review_batch_fields",
                            }
                        )
                        continue
                    if not isinstance(batch.get("rows"), int) or batch.get("rows", 0) <= 0:
                        canada_campaign_evidence_gaps.append(
                            {
                                "field": "top_canada_figi_queue_review_batches",
                                "row_index": index,
                                "reason": "expected_positive_rows",
                            }
                        )
                    if not batch.get("recommended_next_source") or not batch.get("source_gate"):
                        canada_campaign_evidence_gaps.append(
                            {
                                "field": "top_canada_figi_queue_review_batches",
                                "row_index": index,
                                "reason": "expected_source_instruction_fields",
                            }
                        )
                    if batch.get("review_strategy") == "keep_reviewed_openfigi_source_gaps_excluded_until_stronger_source":
                        if batch.get("apply_eligibility") != "keep_figi_blank_as_reviewed_openfigi_source_gap_until_stronger_source":
                            canada_campaign_evidence_gaps.append(
                                {
                                    "field": "top_canada_figi_queue_review_batches.apply_eligibility",
                                    "row_index": index,
                                    "reported": batch.get("apply_eligibility"),
                                }
                            )
                    elif batch.get("review_strategy") == "probe_openfigi_by_valid_isin_with_canada_exchange_hint_then_collision_review":
                        if batch.get("apply_eligibility") != "eligible_for_openfigi_probe_only_no_figi_apply_until_collision_gate":
                            canada_campaign_evidence_gaps.append(
                                {
                                    "field": "top_canada_figi_queue_review_batches.apply_eligibility",
                                    "row_index": index,
                                    "reported": batch.get("apply_eligibility"),
                                }
                            )
                    elif batch.get("review_strategy") != "no_active_openfigi_probe_rows_after_gates":
                        canada_campaign_evidence_gaps.append(
                            {
                                "field": "top_canada_figi_queue_review_batches.review_strategy",
                                "row_index": index,
                                "reported": batch.get("review_strategy"),
                            }
                        )
            known_deltas = delta_evidence.get("known_deltas") if isinstance(delta_evidence, dict) else {}
            if not isinstance(known_deltas, dict):
                known_deltas = {}
            missing_delta_evidence_keys = [
                key for key in CANADA_CAMPAIGN_DELTA_EVIDENCE_KEYS if key not in known_deltas
            ]
            if missing_delta_evidence_keys:
                canada_campaign_evidence_gaps.append(
                    {"field": "delta_evidence.known_deltas", "missing_keys": missing_delta_evidence_keys}
                )
            for evidence_key in CANADA_CAMPAIGN_DELTA_EVIDENCE_KEYS:
                if evidence_key in evidence and known_deltas.get(evidence_key) != evidence.get(evidence_key):
                    canada_campaign_evidence_gaps.append(
                        {
                            "field": f"delta_evidence.{evidence_key}",
                            "reported": known_deltas.get(evidence_key),
                            "expected": evidence.get(evidence_key),
                        }
                    )
            global_acceptance_deltas = matrix.get("global_acceptance_deltas", {})
            if not isinstance(global_acceptance_deltas, dict):
                global_acceptance_deltas = {}
            expected_isin_delta = global_acceptance_deltas.get("isin_delta", {})
            if known_deltas.get("isin_delta") != expected_isin_delta:
                canada_campaign_evidence_gaps.append(
                    {
                        "field": "delta_evidence.isin_delta",
                        "reported": known_deltas.get("isin_delta"),
                        "expected": expected_isin_delta,
                    }
                )
            expected_sector_category_delta = {
                "sector_delta": global_acceptance_deltas.get("sector_delta", {}),
                "category_delta": global_acceptance_deltas.get("category_delta", {}),
            }
            if known_deltas.get("sector_category_delta") != expected_sector_category_delta:
                canada_campaign_evidence_gaps.append(
                    {
                        "field": "delta_evidence.sector_category_delta",
                        "reported": known_deltas.get("sector_category_delta"),
                        "expected": expected_sector_category_delta,
                    }
                )
            expected_warn_quarantine_delta = {
                "warn_delta": global_acceptance_deltas.get("warn_delta", {}).get("delta", 0),
                "quarantine_delta": global_acceptance_deltas.get("quarantine_delta", {}).get("delta", 0),
            }
            if known_deltas.get("warn_quarantine_delta") != expected_warn_quarantine_delta:
                canada_campaign_evidence_gaps.append(
                    {
                        "field": "delta_evidence.warn_quarantine_delta",
                        "reported": known_deltas.get("warn_quarantine_delta"),
                        "expected": expected_warn_quarantine_delta,
                    }
                )
        if campaign_key == "asx":
            evidence = campaign.get("evidence", {})
            delta_evidence = campaign.get("delta_evidence", {})
            if not isinstance(evidence, dict):
                asx_campaign_evidence_gaps.append({"field": "evidence", "reason": "expected_object"})
                evidence = {}
            missing_evidence_keys = [key for key in ASX_CAMPAIGN_EVIDENCE_KEYS if key not in evidence]
            if missing_evidence_keys:
                asx_campaign_evidence_gaps.append({"field": "evidence", "missing_keys": missing_evidence_keys})
            if (
                not isinstance(evidence.get("asx_residual_rows"), int)
                or isinstance(evidence.get("asx_residual_rows"), bool)
                or evidence.get("asx_residual_rows", -1) < 0
            ):
                asx_campaign_evidence_gaps.append({"field": "asx_residual_rows", "reason": "expected_nonnegative_integer"})
            for counter_key in (
                "field_totals",
                "asset_type_totals",
                "asx_core_exclusion_candidate_field_totals",
                "asx_core_exclusion_candidate_asset_type_totals",
                "asx_core_exclusion_candidate_gap_class_totals",
                "gap_class_totals",
                "source_of_truth_outcome_totals",
                "asx_residual_backlog_queue_totals",
                "asx_residual_backlog_evidence_required_totals",
                "asx_resolution_queue_totals",
                "residual_decision_totals",
                "review_bucket_totals",
                "review_priority_totals",
                "apply_eligibility_totals",
                "verification_evidence_required_totals",
                "asx_isin_probe_decision_totals",
                "official_masterfile_match_totals",
                "official_masterfile_exposes_isin_totals",
                "official_masterfile_exposes_sector_totals",
                "official_masterfile_source_totals",
                "asx_core_exclusion_candidate_official_capability_totals",
            ):
                counter = evidence.get(counter_key)
                if not isinstance(counter, dict) or any(not isinstance(value, int) or value < 0 for value in counter.values()):
                    asx_campaign_evidence_gaps.append({"field": counter_key, "reason": "expected_counter"})
            for nested_counter_key in (
                "asx_resolution_queue_field_totals",
                "asx_resolution_queue_gap_class_totals",
                "asx_resolution_queue_official_source_totals",
                "asx_resolution_queue_review_strategy_totals",
                "asx_resolution_queue_evidence_required_totals",
                "asx_resolution_queue_official_capability_totals",
            ):
                nested_counter = evidence.get(nested_counter_key)
                if not isinstance(nested_counter, dict) or any(
                    not isinstance(counter, dict)
                    or any(not isinstance(value, int) or value < 0 for value in counter.values())
                    for counter in (nested_counter.values() if isinstance(nested_counter, dict) else [])
                ):
                    asx_campaign_evidence_gaps.append({"field": nested_counter_key, "reason": "expected_nested_counter"})
            asx_rows = evidence.get("asx_residual_rows")
            asx_core_exclusion_rows = evidence.get("asx_core_exclusion_candidate_rows")
            asx_residual_backlog = evidence.get("asx_residual_backlog")
            if not isinstance(asx_residual_backlog, dict):
                asx_campaign_evidence_gaps.append({"field": "asx_residual_backlog", "reason": "expected_object"})
            else:
                for key in (
                    "rows",
                    "scope_decision_required_rows",
                    "identity_review_required_rows",
                    "official_identifier_source_required_rows",
                    "official_product_taxonomy_required_rows",
                    "official_isin_apply_candidate_rows",
                    "direct_data_apply_allowed_rows",
                ):
                    value = asx_residual_backlog.get(key)
                    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                        asx_campaign_evidence_gaps.append(
                            {"field": f"asx_residual_backlog.{key}", "reason": "expected_nonnegative_integer"}
                        )
                if asx_residual_backlog.get("metadata_enrichment_authorized") is not False:
                    asx_campaign_evidence_gaps.append(
                        {
                            "field": "asx_residual_backlog.metadata_enrichment_authorized",
                            "reported": asx_residual_backlog.get("metadata_enrichment_authorized"),
                            "expected": False,
                        }
                    )
                if asx_residual_backlog.get("direct_data_apply_allowed_rows") != 0:
                    asx_campaign_evidence_gaps.append(
                        {
                            "field": "asx_residual_backlog.direct_data_apply_allowed_rows",
                            "reported": asx_residual_backlog.get("direct_data_apply_allowed_rows"),
                            "expected": 0,
                        }
                    )
                if not asx_residual_backlog.get("source_gate"):
                    asx_campaign_evidence_gaps.append(
                        {"field": "asx_residual_backlog.source_gate", "reason": "missing_source_gate"}
                    )
            if (
                not isinstance(asx_core_exclusion_rows, int)
                or isinstance(asx_core_exclusion_rows, bool)
                or asx_core_exclusion_rows < 0
            ):
                asx_campaign_evidence_gaps.append(
                    {"field": "asx_core_exclusion_candidate_rows", "reason": "expected_nonnegative_integer"}
                )
            if isinstance(asx_rows, int):
                if isinstance(asx_residual_backlog, dict) and asx_residual_backlog.get("rows") != asx_rows:
                    asx_campaign_evidence_gaps.append(
                        {
                            "field": "asx_residual_backlog.rows",
                            "reported": asx_residual_backlog.get("rows"),
                            "asx_residual_rows": asx_rows,
                        }
                    )
                for counter_key in (
                    "field_totals",
                    "asset_type_totals",
                    "gap_class_totals",
                    "source_of_truth_outcome_totals",
                    "asx_residual_backlog_queue_totals",
                    "asx_residual_backlog_evidence_required_totals",
                    "asx_resolution_queue_totals",
                    "residual_decision_totals",
                    "review_bucket_totals",
                    "review_priority_totals",
                    "apply_eligibility_totals",
                    "verification_evidence_required_totals",
                    "official_masterfile_match_totals",
                    "official_masterfile_exposes_isin_totals",
                    "official_masterfile_exposes_sector_totals",
                ):
                    counter = evidence.get(counter_key)
                    if isinstance(counter, dict) and sum(counter.values()) != asx_rows:
                        asx_campaign_evidence_gaps.append(
                            {
                                "field": counter_key,
                                "reported_total": sum(counter.values()),
                                "asx_residual_rows": asx_rows,
                            }
                        )
                official_sources = evidence.get("official_masterfile_source_totals")
                if isinstance(official_sources, dict):
                    official_source_rows = sum(official_sources.values())
                    if official_source_rows <= 0 or official_source_rows > asx_rows:
                        asx_campaign_evidence_gaps.append(
                            {
                                "field": "official_masterfile_source_totals",
                                "reported_total": official_source_rows,
                                "asx_residual_rows": asx_rows,
                            }
                        )
            field_totals = evidence.get("field_totals")
            if isinstance(asx_core_exclusion_rows, int):
                for counter_key in (
                    "asx_core_exclusion_candidate_field_totals",
                    "asx_core_exclusion_candidate_asset_type_totals",
                    "asx_core_exclusion_candidate_gap_class_totals",
                    "asx_core_exclusion_candidate_resolution_queue_totals",
                ):
                    counter = evidence.get(counter_key)
                    if isinstance(counter, dict) and sum(counter.values()) != asx_core_exclusion_rows:
                        asx_campaign_evidence_gaps.append(
                            {
                                "field": counter_key,
                                "reported_total": sum(counter.values()),
                                "asx_core_exclusion_candidate_rows": asx_core_exclusion_rows,
                            }
                        )
                for counter_key in (
                    "asx_core_exclusion_candidate_official_source_totals",
                    "asx_core_exclusion_candidate_official_capability_totals",
                ):
                    counter = evidence.get(counter_key)
                    if not isinstance(counter, dict) or not counter:
                        asx_campaign_evidence_gaps.append(
                            {"field": counter_key, "reason": "expected_nonempty_counter"}
                        )
            if isinstance(field_totals, dict):
                unexpected_fields = sorted(str(field) for field in field_totals if field not in ASX_RESIDUAL_FIELDS)
                if unexpected_fields:
                    asx_campaign_evidence_gaps.append({"field": "field_totals", "unexpected_fields": unexpected_fields})
                missing_isin_rows = field_totals.get("missing_isin_primary", 0)
                probe_totals = evidence.get("asx_isin_probe_decision_totals")
                if isinstance(missing_isin_rows, int) and isinstance(probe_totals, dict) and sum(probe_totals.values()) != missing_isin_rows:
                    asx_campaign_evidence_gaps.append(
                        {
                            "field": "asx_isin_probe_decision_totals",
                            "reported_total": sum(probe_totals.values()),
                            "missing_isin_primary": missing_isin_rows,
                        }
                    )
            resolution_totals = evidence.get("asx_resolution_queue_totals")
            resolution_field_totals = evidence.get("asx_resolution_queue_field_totals")
            resolution_gap_class_totals = evidence.get("asx_resolution_queue_gap_class_totals")
            resolution_official_source_totals = evidence.get("asx_resolution_queue_official_source_totals")
            resolution_strategy_totals = evidence.get("asx_resolution_queue_review_strategy_totals")
            resolution_evidence_totals = evidence.get("asx_resolution_queue_evidence_required_totals")
            resolution_capability_totals = evidence.get("asx_resolution_queue_official_capability_totals")
            if isinstance(asx_rows, int) and isinstance(resolution_totals, dict):
                if sum(resolution_totals.values()) != asx_rows:
                    asx_campaign_evidence_gaps.append(
                        {
                            "field": "asx_resolution_queue_totals",
                            "reported_total": sum(resolution_totals.values()),
                            "asx_residual_rows": asx_rows,
                        }
                    )
            if isinstance(resolution_totals, dict) and isinstance(resolution_field_totals, dict):
                for queue, total in resolution_totals.items():
                    field_counter = resolution_field_totals.get(queue)
                    if not isinstance(field_counter, dict) or sum(field_counter.values()) != total:
                        asx_campaign_evidence_gaps.append(
                            {
                                "field": "asx_resolution_queue_field_totals",
                                "queue": queue,
                                "reported_total": sum(field_counter.values()) if isinstance(field_counter, dict) else None,
                                "expected": total,
                            }
                        )
            if isinstance(resolution_totals, dict) and isinstance(resolution_gap_class_totals, dict):
                for queue, total in resolution_totals.items():
                    gap_class_counter = resolution_gap_class_totals.get(queue)
                    if not isinstance(gap_class_counter, dict) or sum(gap_class_counter.values()) != total:
                        asx_campaign_evidence_gaps.append(
                            {
                                "field": "asx_resolution_queue_gap_class_totals",
                                "queue": queue,
                                "reported_total": sum(gap_class_counter.values())
                                if isinstance(gap_class_counter, dict)
                                else None,
                                "expected": total,
                            }
                        )
            if isinstance(resolution_totals, dict) and isinstance(resolution_official_source_totals, dict):
                for queue in resolution_totals:
                    official_source_counter = resolution_official_source_totals.get(queue)
                    if not isinstance(official_source_counter, dict) or not official_source_counter:
                        asx_campaign_evidence_gaps.append(
                            {
                                "field": "asx_resolution_queue_official_source_totals",
                                "queue": queue,
                                "reason": "expected_nonempty_counter",
                            }
                        )
            if isinstance(resolution_totals, dict) and isinstance(resolution_strategy_totals, dict):
                for queue, total in resolution_totals.items():
                    strategy_counter = resolution_strategy_totals.get(queue)
                    if not isinstance(strategy_counter, dict) or sum(strategy_counter.values()) != total:
                        asx_campaign_evidence_gaps.append(
                            {
                                "field": "asx_resolution_queue_review_strategy_totals",
                                "queue": queue,
                                "reported_total": sum(strategy_counter.values()) if isinstance(strategy_counter, dict) else None,
                                "expected": total,
                            }
                        )
                    elif any(strategy not in ASX_REVIEW_STRATEGIES for strategy in strategy_counter):
                        asx_campaign_evidence_gaps.append(
                            {
                                "field": "asx_resolution_queue_review_strategy_totals",
                                "queue": queue,
                                "unexpected_strategies": sorted(
                                    strategy for strategy in strategy_counter if strategy not in ASX_REVIEW_STRATEGIES
                                ),
                            }
                        )
            if isinstance(resolution_totals, dict) and isinstance(resolution_evidence_totals, dict):
                for queue, total in resolution_totals.items():
                    evidence_counter = resolution_evidence_totals.get(queue)
                    if not isinstance(evidence_counter, dict) or sum(evidence_counter.values()) != total:
                        asx_campaign_evidence_gaps.append(
                            {
                                "field": "asx_resolution_queue_evidence_required_totals",
                                "queue": queue,
                                "reported_total": sum(evidence_counter.values()) if isinstance(evidence_counter, dict) else None,
                                "expected": total,
                            }
                        )
                    else:
                        _expected_strategy, expected_evidence = asx_review_strategy_for(str(queue))
                        if set(evidence_counter) != {expected_evidence}:
                            asx_campaign_evidence_gaps.append(
                                {
                                    "field": "asx_resolution_queue_evidence_required_totals",
                                    "queue": queue,
                                    "unexpected_evidence": sorted(set(evidence_counter) - {expected_evidence}),
                                }
                            )
            if isinstance(resolution_totals, dict) and isinstance(resolution_capability_totals, dict):
                for queue, total in resolution_totals.items():
                    capability_counter = resolution_capability_totals.get(queue)
                    if not isinstance(capability_counter, dict):
                        asx_campaign_evidence_gaps.append(
                            {
                                "field": "asx_resolution_queue_official_capability_totals",
                                "queue": queue,
                                "reason": "expected_counter",
                            }
                        )
                        continue
                    for prefix in (
                        "masterfile_match=",
                        "masterfile_exposes_isin=",
                        "masterfile_exposes_sector=",
                    ):
                        subtotal = sum(
                            value
                            for capability, value in capability_counter.items()
                            if str(capability).startswith(prefix)
                        )
                        if subtotal != total:
                            asx_campaign_evidence_gaps.append(
                                {
                                    "field": "asx_resolution_queue_official_capability_totals",
                                    "queue": queue,
                                    "capability": prefix.rstrip("="),
                                    "reported_total": subtotal,
                                    "expected": total,
                                }
                            )
            top_asx_batches = evidence.get("top_asx_resolution_review_batches")
            if (
                not isinstance(top_asx_batches, list)
                or not top_asx_batches
                or any(
                    not isinstance(batch, dict)
                    or not batch.get("asx_resolution_queue")
                    or batch.get("field") not in ASX_RESIDUAL_FIELDS
                    or not batch.get("official_source")
                    or not isinstance(batch.get("rows"), int)
                    or batch.get("rows", 0) <= 0
                    or batch.get("review_strategy") not in ASX_REVIEW_STRATEGIES
                    or batch.get("evidence_required")
                    != asx_review_strategy_for(str(batch.get("asx_resolution_queue", "")))[1]
                    or not batch.get("recommended_next_source")
                    or not batch.get("source_gate")
                    for batch in top_asx_batches
                )
            ):
                asx_campaign_evidence_gaps.append(
                    {
                        "field": "top_asx_resolution_review_batches",
                        "reason": "expected_ranked_asx_strategy_rows",
                    }
                )
            review_bucket_totals = evidence.get("review_bucket_totals")
            if isinstance(review_bucket_totals, dict):
                missing_buckets = [
                    bucket
                    for bucket in ("scope_review_before_any_data_fill", "product_taxonomy_source_gap", "identifier_source_gap")
                    if bucket not in review_bucket_totals
                ]
                if missing_buckets:
                    asx_campaign_evidence_gaps.append({"field": "review_bucket_totals", "missing_keys": missing_buckets})
            apply_totals = evidence.get("apply_eligibility_totals")
            if isinstance(apply_totals, dict):
                forbidden_apply_keys = [
                    key
                    for key in apply_totals
                    if str(key).startswith("apply") and key != "apply_only_after_asx_code_name_token_and_isin_checksum_validation"
                ]
                if forbidden_apply_keys:
                    asx_campaign_evidence_gaps.append({"field": "apply_eligibility_totals", "forbidden_keys": forbidden_apply_keys})
            known_deltas = delta_evidence.get("known_deltas") if isinstance(delta_evidence, dict) else {}
            if not isinstance(known_deltas, dict):
                known_deltas = {}
            missing_delta_evidence_keys = [
                key for key in ASX_CAMPAIGN_DELTA_EVIDENCE_KEYS if key not in known_deltas
            ]
            if missing_delta_evidence_keys:
                asx_campaign_evidence_gaps.append(
                    {"field": "delta_evidence.known_deltas", "missing_keys": missing_delta_evidence_keys}
                )
            if isinstance(asx_rows, int) and known_deltas.get("current_asx_residual_rows") != asx_rows:
                asx_campaign_evidence_gaps.append(
                    {
                        "field": "delta_evidence.current_asx_residual_rows",
                        "reported": known_deltas.get("current_asx_residual_rows"),
                        "expected": asx_rows,
                    }
                )
            if (
                isinstance(asx_core_exclusion_rows, int)
                and known_deltas.get("asx_core_exclusion_candidate_rows") != asx_core_exclusion_rows
            ):
                asx_campaign_evidence_gaps.append(
                    {
                        "field": "delta_evidence.asx_core_exclusion_candidate_rows",
                        "reported": known_deltas.get("asx_core_exclusion_candidate_rows"),
                        "expected": asx_core_exclusion_rows,
                    }
                )
            expected_residual_snapshot = matrix.get("campaign_metric_deltas", {}).get("residual_rows", {})
            if known_deltas.get("campaign_start_residual_snapshot") != expected_residual_snapshot:
                asx_campaign_evidence_gaps.append(
                    {
                        "field": "delta_evidence.campaign_start_residual_snapshot",
                        "reported": known_deltas.get("campaign_start_residual_snapshot"),
                        "expected": expected_residual_snapshot,
                    }
                )
            global_acceptance_deltas = matrix.get("global_acceptance_deltas", {})
            expected_source_gap_delta = (
                global_acceptance_deltas.get("source_gap_delta", {})
                if isinstance(global_acceptance_deltas, dict)
                else {}
            )
            if known_deltas.get("source_gap_delta") != expected_source_gap_delta:
                asx_campaign_evidence_gaps.append(
                    {
                        "field": "delta_evidence.source_gap_delta",
                        "reported": known_deltas.get("source_gap_delta"),
                        "expected": expected_source_gap_delta,
                    }
                )
            expected_warn_quarantine_delta = {
                "warn_delta": global_acceptance_deltas.get("warn_delta", {}).get("delta", 0)
                if isinstance(global_acceptance_deltas, dict)
                else 0,
                "quarantine_delta": global_acceptance_deltas.get("quarantine_delta", {}).get("delta", 0)
                if isinstance(global_acceptance_deltas, dict)
                else 0,
            }
            if known_deltas.get("warn_quarantine_delta") != expected_warn_quarantine_delta:
                asx_campaign_evidence_gaps.append(
                    {
                        "field": "delta_evidence.warn_quarantine_delta",
                        "reported": known_deltas.get("warn_quarantine_delta"),
                        "expected": expected_warn_quarantine_delta,
                    }
                )
        if campaign_key == "weak_sector":
            evidence = campaign.get("evidence", {})
            delta_evidence = campaign.get("delta_evidence", {})
            if not isinstance(evidence, dict):
                weak_sector_campaign_evidence_gaps.append({"field": "evidence", "reason": "expected_object"})
                evidence = {}
            missing_evidence_keys = [
                key for key in WEAK_SECTOR_CAMPAIGN_EVIDENCE_KEYS if key not in evidence
            ]
            if missing_evidence_keys:
                weak_sector_campaign_evidence_gaps.append(
                    {"field": "evidence", "missing_keys": missing_evidence_keys}
                )
            for key in ("weak_sector_rows", "ngx_applied_rows", "ngx_written_updates"):
                if not isinstance(evidence.get(key), int) or isinstance(evidence.get(key), bool) or evidence.get(key, -1) < 0:
                    weak_sector_campaign_evidence_gaps.append({"field": key, "reason": "expected_nonnegative_integer"})
            for key in ("official_sector_candidate_rows", "scope_review_rows", "masterfile_without_sector_rows"):
                if not isinstance(evidence.get(key), int) or isinstance(evidence.get(key), bool) or evidence.get(key, -1) < 0:
                    weak_sector_campaign_evidence_gaps.append({"field": key, "reason": "expected_nonnegative_integer"})
            for counter_key in (
                "exchange_totals",
                "top_exchange_residuals",
                "official_sector_candidate_exchange_totals",
                "official_sector_candidate_value_totals",
                "scope_review_exchange_totals",
                "scope_review_gap_class_totals",
                "masterfile_without_sector_exchange_totals",
                "gap_class_totals",
                "source_of_truth_outcome_totals",
                "weak_sector_backlog_queue_totals",
                "weak_sector_backlog_evidence_required_totals",
                "weak_sector_resolution_queue_totals",
                "residual_decision_totals",
                "review_bucket_totals",
                "review_priority_totals",
                "apply_eligibility_totals",
                "verification_evidence_required_totals",
                "official_masterfile_match_totals",
                "official_masterfile_exposes_sector_totals",
                "official_masterfile_source_totals",
                "official_sector_value_totals",
            ):
                counter = evidence.get(counter_key)
                if not isinstance(counter, dict) or any(not isinstance(value, int) or value < 0 for value in counter.values()):
                    weak_sector_campaign_evidence_gaps.append({"field": counter_key, "reason": "expected_counter"})
            nested_source_counter = evidence.get("weak_sector_resolution_queue_official_source_totals")
            if (
                not isinstance(nested_source_counter, dict)
                or any(
                    not isinstance(value, dict)
                    or not value
                    or any(not isinstance(count, int) or count < 0 for count in value.values())
                    for value in nested_source_counter.values()
                )
            ):
                weak_sector_campaign_evidence_gaps.append(
                    {
                        "field": "weak_sector_resolution_queue_official_source_totals",
                        "reason": "expected_nested_source_counter",
                    }
                )
            nested_strategy_counter = evidence.get("weak_sector_resolution_queue_review_strategy_totals")
            if (
                not isinstance(nested_strategy_counter, dict)
                or any(
                    not isinstance(value, dict)
                    or not value
                    or any(not isinstance(count, int) or count < 0 for count in value.values())
                    for value in nested_strategy_counter.values()
                )
            ):
                weak_sector_campaign_evidence_gaps.append(
                    {
                        "field": "weak_sector_resolution_queue_review_strategy_totals",
                        "reason": "expected_nested_strategy_counter",
                    }
                )
            nested_capability_counter = evidence.get("weak_sector_resolution_queue_official_capability_totals")
            if (
                not isinstance(nested_capability_counter, dict)
                or any(
                    not isinstance(value, dict)
                    or not value
                    or any(not isinstance(count, int) or count < 0 for count in value.values())
                    for value in nested_capability_counter.values()
                )
            ):
                weak_sector_campaign_evidence_gaps.append(
                    {
                        "field": "weak_sector_resolution_queue_official_capability_totals",
                        "reason": "expected_nested_capability_counter",
                    }
                )
            venue_queue_counter = evidence.get("venue_backlog_exchange_queue_totals")
            if (
                not isinstance(venue_queue_counter, dict)
                or any(
                    exchange not in WEAK_SECTOR_EXCHANGES
                    or not isinstance(value, dict)
                    or not value
                    or any(not isinstance(count, int) or count < 0 for count in value.values())
                    for exchange, value in venue_queue_counter.items()
                )
            ):
                weak_sector_campaign_evidence_gaps.append(
                    {"field": "venue_backlog_exchange_queue_totals", "reason": "expected_exchange_nested_counter"}
                )
            venue_capability_counter = evidence.get("venue_backlog_exchange_official_capability_totals")
            if (
                not isinstance(venue_capability_counter, dict)
                or any(
                    exchange not in WEAK_SECTOR_EXCHANGES
                    or not isinstance(value, dict)
                    or not value
                    or any(not isinstance(count, int) or count < 0 for count in value.values())
                    for exchange, value in venue_capability_counter.items()
                )
            ):
                weak_sector_campaign_evidence_gaps.append(
                    {
                        "field": "venue_backlog_exchange_official_capability_totals",
                        "reason": "expected_exchange_nested_capability_counter",
                    }
                )
            weak_rows = evidence.get("weak_sector_rows")
            official_sector_candidate_rows = evidence.get("official_sector_candidate_rows")
            scope_review_rows = evidence.get("scope_review_rows")
            masterfile_without_sector_rows = evidence.get("masterfile_without_sector_rows")
            weak_sector_backlog = evidence.get("weak_sector_backlog")
            if not isinstance(weak_sector_backlog, dict):
                weak_sector_campaign_evidence_gaps.append({"field": "weak_sector_backlog", "reason": "expected_object"})
            else:
                for key in (
                    "rows",
                    "official_sector_candidate_rows",
                    "scope_decision_required_rows",
                    "masterfile_without_sector_rows",
                    "venue_taxonomy_source_required_rows",
                    "direct_sector_apply_allowed_rows",
                ):
                    value = weak_sector_backlog.get(key)
                    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                        weak_sector_campaign_evidence_gaps.append(
                            {"field": f"weak_sector_backlog.{key}", "reason": "expected_nonnegative_integer"}
                        )
                if weak_sector_backlog.get("metadata_enrichment_authorized") is not False:
                    weak_sector_campaign_evidence_gaps.append(
                        {
                            "field": "weak_sector_backlog.metadata_enrichment_authorized",
                            "reported": weak_sector_backlog.get("metadata_enrichment_authorized"),
                            "expected": False,
                        }
                    )
                if weak_sector_backlog.get("direct_sector_apply_allowed_rows") != 0:
                    weak_sector_campaign_evidence_gaps.append(
                        {
                            "field": "weak_sector_backlog.direct_sector_apply_allowed_rows",
                            "reported": weak_sector_backlog.get("direct_sector_apply_allowed_rows"),
                            "expected": 0,
                        }
                    )
                if not weak_sector_backlog.get("source_gate"):
                    weak_sector_campaign_evidence_gaps.append(
                        {"field": "weak_sector_backlog.source_gate", "reason": "missing_source_gate"}
                    )
            if isinstance(weak_rows, int):
                if isinstance(weak_sector_backlog, dict) and weak_sector_backlog.get("rows") != weak_rows:
                    weak_sector_campaign_evidence_gaps.append(
                        {
                            "field": "weak_sector_backlog.rows",
                            "reported": weak_sector_backlog.get("rows"),
                            "weak_sector_rows": weak_rows,
                        }
                    )
                for counter_key in (
                    "exchange_totals",
                    "gap_class_totals",
                    "source_of_truth_outcome_totals",
                    "weak_sector_backlog_queue_totals",
                    "weak_sector_backlog_evidence_required_totals",
                    "weak_sector_resolution_queue_totals",
                    "residual_decision_totals",
                    "review_bucket_totals",
                    "review_priority_totals",
                    "apply_eligibility_totals",
                    "verification_evidence_required_totals",
                    "official_masterfile_match_totals",
                    "official_masterfile_exposes_sector_totals",
                ):
                    counter = evidence.get(counter_key)
                    if isinstance(counter, dict) and sum(counter.values()) != weak_rows:
                        weak_sector_campaign_evidence_gaps.append(
                            {
                                "field": counter_key,
                                "reported_total": sum(counter.values()),
                                "weak_sector_rows": weak_rows,
                            }
                        )
            for rows_key, counter_key in (
                ("official_sector_candidate_rows", "official_sector_candidate_exchange_totals"),
                ("official_sector_candidate_rows", "official_sector_candidate_value_totals"),
                ("scope_review_rows", "scope_review_exchange_totals"),
                ("scope_review_rows", "scope_review_gap_class_totals"),
                ("masterfile_without_sector_rows", "masterfile_without_sector_exchange_totals"),
            ):
                rows_value = evidence.get(rows_key)
                counter = evidence.get(counter_key)
                if isinstance(rows_value, int) and isinstance(counter, dict) and sum(counter.values()) != rows_value:
                    weak_sector_campaign_evidence_gaps.append(
                        {
                            "field": counter_key,
                            "reported_total": sum(counter.values()),
                            rows_key: rows_value,
                        }
                    )
            reported_exchanges = evidence.get("exchanges")
            exchange_totals = evidence.get("exchange_totals")
            if reported_exchanges != sorted(WEAK_SECTOR_EXCHANGES):
                weak_sector_campaign_evidence_gaps.append(
                    {
                        "field": "exchanges",
                        "reported": reported_exchanges,
                        "expected": sorted(WEAK_SECTOR_EXCHANGES),
                    }
                )
            if isinstance(exchange_totals, dict):
                unexpected_exchanges = sorted(str(exchange) for exchange in exchange_totals if exchange not in WEAK_SECTOR_EXCHANGES)
                missing_exchanges = sorted(exchange for exchange in WEAK_SECTOR_EXCHANGES if exchange not in exchange_totals)
                if unexpected_exchanges or missing_exchanges:
                    weak_sector_campaign_evidence_gaps.append(
                        {
                            "field": "exchange_totals",
                            "unexpected_exchanges": unexpected_exchanges,
                            "missing_exchanges": missing_exchanges,
                        }
                    )
            if isinstance(exchange_totals, dict) and isinstance(venue_queue_counter, dict):
                for exchange, total in exchange_totals.items():
                    queue_counter = venue_queue_counter.get(exchange)
                    if not isinstance(queue_counter, dict) or sum(queue_counter.values()) != total:
                        weak_sector_campaign_evidence_gaps.append(
                            {
                                "field": "venue_backlog_exchange_queue_totals",
                                "exchange": exchange,
                                "reported_total": sum(queue_counter.values()) if isinstance(queue_counter, dict) else None,
                                "expected": total,
                            }
                        )
            if isinstance(exchange_totals, dict) and isinstance(venue_capability_counter, dict):
                for exchange, total in exchange_totals.items():
                    capability_counter = venue_capability_counter.get(exchange)
                    if not isinstance(capability_counter, dict):
                        weak_sector_campaign_evidence_gaps.append(
                            {
                                "field": "venue_backlog_exchange_official_capability_totals",
                                "exchange": exchange,
                                "reason": "expected_counter",
                            }
                        )
                        continue
                    for prefix in ("masterfile_match=", "masterfile_exposes_sector="):
                        subtotal = sum(
                            value
                            for capability, value in capability_counter.items()
                            if str(capability).startswith(prefix)
                        )
                        if subtotal != total:
                            weak_sector_campaign_evidence_gaps.append(
                                {
                                    "field": "venue_backlog_exchange_official_capability_totals",
                                    "exchange": exchange,
                                    "capability": prefix.rstrip("="),
                                    "reported_total": subtotal,
                                    "expected": total,
                                }
                            )
            review_bucket_totals = evidence.get("review_bucket_totals")
            resolution_totals = evidence.get("weak_sector_resolution_queue_totals")
            resolution_exchange_totals = evidence.get("weak_sector_resolution_queue_exchange_totals")
            resolution_gap_class_totals = evidence.get("weak_sector_resolution_queue_gap_class_totals")
            resolution_official_source_totals = evidence.get("weak_sector_resolution_queue_official_source_totals")
            resolution_strategy_totals = evidence.get("weak_sector_resolution_queue_review_strategy_totals")
            resolution_capability_totals = evidence.get("weak_sector_resolution_queue_official_capability_totals")
            if isinstance(weak_rows, int) and isinstance(resolution_totals, dict):
                if sum(resolution_totals.values()) != weak_rows:
                    weak_sector_campaign_evidence_gaps.append(
                        {
                            "field": "weak_sector_resolution_queue_totals",
                            "reported_total": sum(resolution_totals.values()),
                            "weak_sector_rows": weak_rows,
                        }
                        )
            if isinstance(resolution_totals, dict) and isinstance(resolution_exchange_totals, dict):
                for queue, total in resolution_totals.items():
                    exchange_counter = resolution_exchange_totals.get(queue)
                    if not isinstance(exchange_counter, dict) or sum(exchange_counter.values()) != total:
                        weak_sector_campaign_evidence_gaps.append(
                            {
                                "field": "weak_sector_resolution_queue_exchange_totals",
                                "queue": queue,
                                "reported_total": sum(exchange_counter.values()) if isinstance(exchange_counter, dict) else None,
                                "expected": total,
                            }
                        )
            if isinstance(resolution_totals, dict) and isinstance(resolution_gap_class_totals, dict):
                for queue, total in resolution_totals.items():
                    gap_class_counter = resolution_gap_class_totals.get(queue)
                    if not isinstance(gap_class_counter, dict) or sum(gap_class_counter.values()) != total:
                        weak_sector_campaign_evidence_gaps.append(
                            {
                                "field": "weak_sector_resolution_queue_gap_class_totals",
                                "queue": queue,
                                "reported_total": sum(gap_class_counter.values()) if isinstance(gap_class_counter, dict) else None,
                                "expected": total,
                            }
                        )
            if isinstance(resolution_totals, dict) and isinstance(resolution_official_source_totals, dict):
                for queue in resolution_totals:
                    source_counter = resolution_official_source_totals.get(queue)
                    if not isinstance(source_counter, dict) or not source_counter:
                        weak_sector_campaign_evidence_gaps.append(
                            {
                                "field": "weak_sector_resolution_queue_official_source_totals",
                                "queue": queue,
                                "reason": "expected_nonempty_source_counter",
                            }
                        )
            if isinstance(resolution_totals, dict) and isinstance(resolution_strategy_totals, dict):
                for queue, total in resolution_totals.items():
                    strategy_counter = resolution_strategy_totals.get(queue)
                    if not isinstance(strategy_counter, dict) or sum(strategy_counter.values()) != total:
                        weak_sector_campaign_evidence_gaps.append(
                            {
                                "field": "weak_sector_resolution_queue_review_strategy_totals",
                                "queue": queue,
                                "reported_total": sum(strategy_counter.values())
                                if isinstance(strategy_counter, dict)
                                else None,
                                "expected": total,
                            }
                        )
                    elif any(strategy not in WEAK_SECTOR_REVIEW_STRATEGIES for strategy in strategy_counter):
                        weak_sector_campaign_evidence_gaps.append(
                            {
                                "field": "weak_sector_resolution_queue_review_strategy_totals",
                                "queue": queue,
                                "unexpected_strategies": sorted(
                                    strategy for strategy in strategy_counter if strategy not in WEAK_SECTOR_REVIEW_STRATEGIES
                                ),
                            }
                        )
            if isinstance(resolution_totals, dict) and isinstance(resolution_capability_totals, dict):
                for queue, total in resolution_totals.items():
                    capability_counter = resolution_capability_totals.get(queue)
                    if not isinstance(capability_counter, dict):
                        weak_sector_campaign_evidence_gaps.append(
                            {
                                "field": "weak_sector_resolution_queue_official_capability_totals",
                                "queue": queue,
                                "reason": "expected_counter",
                            }
                        )
                        continue
                    for prefix in ("masterfile_match=", "masterfile_exposes_sector="):
                        subtotal = sum(
                            value
                            for capability, value in capability_counter.items()
                            if str(capability).startswith(prefix)
                        )
                        if subtotal != total:
                            weak_sector_campaign_evidence_gaps.append(
                                {
                                    "field": "weak_sector_resolution_queue_official_capability_totals",
                                    "queue": queue,
                                    "capability": prefix.rstrip("="),
                                    "reported_total": subtotal,
                                    "expected": total,
                                }
                            )
            top_weak_sector_batches = evidence.get("top_weak_sector_resolution_review_batches")
            if (
                not isinstance(top_weak_sector_batches, list)
                or not top_weak_sector_batches
                or any(
                    not isinstance(batch, dict)
                    or not batch.get("weak_sector_resolution_queue")
                    or batch.get("exchange") not in WEAK_SECTOR_EXCHANGES
                    or not batch.get("official_source")
                    or not isinstance(batch.get("rows"), int)
                    or batch.get("rows", 0) <= 0
                    or batch.get("review_strategy") not in WEAK_SECTOR_REVIEW_STRATEGIES
                    or not batch.get("evidence_required")
                    or batch.get("evidence_required") in {"none", "ticker_match_only"}
                    or not batch.get("recommended_next_source")
                    or not batch.get("source_gate")
                    for batch in top_weak_sector_batches
                )
            ):
                weak_sector_campaign_evidence_gaps.append(
                    {
                        "field": "top_weak_sector_resolution_review_batches",
                        "reason": "expected_ranked_weak_sector_strategy_rows",
                    }
                )
            top_venue_backlog_batches = evidence.get("top_venue_backlog_batches")
            if (
                not isinstance(top_venue_backlog_batches, list)
                or not top_venue_backlog_batches
                or any(
                    not isinstance(batch, dict)
                    or not batch.get("weak_sector_resolution_queue")
                    or batch.get("exchange") not in WEAK_SECTOR_EXCHANGES
                    or not batch.get("official_source")
                    or not isinstance(batch.get("rows"), int)
                    or batch.get("rows", 0) <= 0
                    or batch.get("review_strategy") not in WEAK_SECTOR_REVIEW_STRATEGIES
                    or not batch.get("evidence_required")
                    or batch.get("evidence_required") in {"none", "ticker_match_only"}
                    or not batch.get("recommended_next_source")
                    or not batch.get("source_gate")
                    for batch in top_venue_backlog_batches
                )
            ):
                weak_sector_campaign_evidence_gaps.append(
                    {"field": "top_venue_backlog_batches", "reason": "expected_ranked_venue_strategy_rows"}
                )
            if isinstance(review_bucket_totals, dict):
                missing_buckets = [
                    bucket
                    for bucket in (
                        "official_sector_candidate_requires_normalization_gate",
                        "scope_review_before_sector_fill",
                        "official_masterfile_without_sector_source_gap",
                        "venue_official_taxonomy_unavailable_source_gap",
                    )
                    if bucket not in review_bucket_totals
                ]
                if missing_buckets:
                    weak_sector_campaign_evidence_gaps.append(
                        {"field": "review_bucket_totals", "missing_keys": missing_buckets}
                    )
            apply_totals = evidence.get("apply_eligibility_totals")
            if isinstance(apply_totals, dict):
                forbidden_apply_keys = [
                    key
                    for key in apply_totals
                    if str(key).startswith("apply") and key != "apply_only_after_official_sector_normalization_and_listing_key_gate"
                ]
                if forbidden_apply_keys:
                    weak_sector_campaign_evidence_gaps.append(
                        {"field": "apply_eligibility_totals", "forbidden_keys": forbidden_apply_keys}
                    )
            official_sector_values = evidence.get("official_sector_value_totals")
            if isinstance(official_sector_values, dict):
                candidate_rows = (
                    official_sector_candidate_rows
                    if isinstance(official_sector_candidate_rows, int)
                    else int(review_bucket_totals.get("official_sector_candidate_requires_normalization_gate", 0))
                    if isinstance(review_bucket_totals, dict)
                    else 0
                )
                if sum(official_sector_values.values()) != candidate_rows:
                    weak_sector_campaign_evidence_gaps.append(
                        {
                            "field": "official_sector_value_totals",
                            "reported_total": sum(official_sector_values.values()),
                            "official_sector_candidate_rows": candidate_rows,
                        }
                    )
            if evidence.get("ngx_written_updates") != 0:
                weak_sector_campaign_evidence_gaps.append(
                    {
                        "field": "ngx_written_updates",
                        "reported": evidence.get("ngx_written_updates"),
                        "expected": 0,
                    }
                )
            known_deltas = delta_evidence.get("known_deltas") if isinstance(delta_evidence, dict) else {}
            if not isinstance(known_deltas, dict):
                known_deltas = {}
            missing_delta_evidence_keys = [
                key for key in WEAK_SECTOR_CAMPAIGN_DELTA_EVIDENCE_KEYS if key not in known_deltas
            ]
            if missing_delta_evidence_keys:
                weak_sector_campaign_evidence_gaps.append(
                    {"field": "delta_evidence.known_deltas", "missing_keys": missing_delta_evidence_keys}
                )
            expected_delta_values = {
                "ngx_review_rows": evidence.get("ngx_applied_rows"),
                "ngx_written_updates": evidence.get("ngx_written_updates"),
                "current_weak_sector_rows": weak_rows,
                "official_sector_candidate_rows": official_sector_candidate_rows,
                "scope_review_rows": scope_review_rows,
                "masterfile_without_sector_rows": masterfile_without_sector_rows,
            }
            for key, expected in expected_delta_values.items():
                if expected is not None and known_deltas.get(key) != expected:
                    weak_sector_campaign_evidence_gaps.append(
                        {
                            "field": f"delta_evidence.{key}",
                            "reported": known_deltas.get(key),
                            "expected": expected,
                        }
                    )
            expected_sector_snapshot = matrix.get("campaign_metric_deltas", {}).get("residual_rows", {})
            if known_deltas.get("campaign_start_sector_coverage_snapshot") != expected_sector_snapshot:
                weak_sector_campaign_evidence_gaps.append(
                    {
                        "field": "delta_evidence.campaign_start_sector_coverage_snapshot",
                        "reported": known_deltas.get("campaign_start_sector_coverage_snapshot"),
                        "expected": expected_sector_snapshot,
                    }
                )
            global_acceptance_deltas = matrix.get("global_acceptance_deltas", {})
            expected_source_gap_delta = (
                global_acceptance_deltas.get("source_gap_delta", {})
                if isinstance(global_acceptance_deltas, dict)
                else {}
            )
            if known_deltas.get("source_gap_delta") != expected_source_gap_delta:
                weak_sector_campaign_evidence_gaps.append(
                    {
                        "field": "delta_evidence.source_gap_delta",
                        "reported": known_deltas.get("source_gap_delta"),
                        "expected": expected_source_gap_delta,
                    }
                )
            expected_warn_quarantine_delta = {
                "warn_delta": global_acceptance_deltas.get("warn_delta", {}).get("delta", 0)
                if isinstance(global_acceptance_deltas, dict)
                else 0,
                "quarantine_delta": global_acceptance_deltas.get("quarantine_delta", {}).get("delta", 0)
                if isinstance(global_acceptance_deltas, dict)
                else 0,
            }
            if known_deltas.get("warn_quarantine_delta") != expected_warn_quarantine_delta:
                weak_sector_campaign_evidence_gaps.append(
                    {
                        "field": "delta_evidence.warn_quarantine_delta",
                        "reported": known_deltas.get("warn_quarantine_delta"),
                        "expected": expected_warn_quarantine_delta,
                    }
                )
        if campaign_key == "masterfile_collisions":
            evidence = campaign.get("evidence", {})
            delta_evidence = campaign.get("delta_evidence", {})
            if not isinstance(evidence, dict):
                masterfile_collision_campaign_evidence_gaps.append({"field": "evidence", "reason": "expected_object"})
                evidence = {}
            missing_evidence_keys = [
                key for key in MASTERFILE_COLLISION_CAMPAIGN_EVIDENCE_KEYS if key not in evidence
            ]
            if missing_evidence_keys:
                masterfile_collision_campaign_evidence_gaps.append(
                    {"field": "evidence", "missing_keys": missing_evidence_keys}
                )
            for key in ("collision_review_rows", "asset_type_mismatches"):
                if not isinstance(evidence.get(key), int) or isinstance(evidence.get(key), bool) or evidence.get(key, -1) < 0:
                    masterfile_collision_campaign_evidence_gaps.append(
                        {"field": key, "reason": "expected_nonnegative_integer"}
                    )
            for counter_key in (
                "decision_totals",
                "review_bucket_totals",
                "review_priority_totals",
                "identity_resolution_queue_totals",
                "identity_resolution_clearance_readiness_totals",
                "clearance_evidence_totals",
                "exchange_totals",
                "official_asset_type_totals",
                "asset_type_mismatch_totals",
                "official_source_totals",
            ):
                counter = evidence.get(counter_key)
                if not isinstance(counter, dict) or any(not isinstance(value, int) or value < 0 for value in counter.values()):
                    masterfile_collision_campaign_evidence_gaps.append({"field": counter_key, "reason": "expected_counter"})
            risk_counter = evidence.get("collision_risk_flag_totals")
            if not isinstance(risk_counter, dict) or any(not isinstance(value, int) or value < 0 for value in risk_counter.values()):
                masterfile_collision_campaign_evidence_gaps.append({"field": "collision_risk_flag_totals", "reason": "expected_counter"})
            queue_exchange_totals = evidence.get("identity_resolution_exchange_totals")
            if not isinstance(queue_exchange_totals, dict) or any(
                not isinstance(exchange_totals, dict)
                or any(not isinstance(value, int) or value < 0 for value in exchange_totals.values())
                for exchange_totals in (queue_exchange_totals.values() if isinstance(queue_exchange_totals, dict) else [])
            ):
                masterfile_collision_campaign_evidence_gaps.append(
                    {"field": "identity_resolution_exchange_totals", "reason": "expected_nested_counter"}
                )
            queue_asset_type_totals = evidence.get("identity_resolution_asset_type_totals")
            if not isinstance(queue_asset_type_totals, dict) or any(
                not isinstance(asset_type_totals, dict)
                or any(not isinstance(value, int) or value < 0 for value in asset_type_totals.values())
                for asset_type_totals in (
                    queue_asset_type_totals.values() if isinstance(queue_asset_type_totals, dict) else []
                )
            ):
                masterfile_collision_campaign_evidence_gaps.append(
                    {"field": "identity_resolution_asset_type_totals", "reason": "expected_nested_counter"}
                )
            queue_official_source_totals = evidence.get("identity_resolution_official_source_totals")
            if not isinstance(queue_official_source_totals, dict) or any(
                not isinstance(source_totals, dict)
                or any(not isinstance(value, int) or value < 0 for value in source_totals.values())
                for source_totals in (
                    queue_official_source_totals.values() if isinstance(queue_official_source_totals, dict) else []
                )
            ):
                masterfile_collision_campaign_evidence_gaps.append(
                    {"field": "identity_resolution_official_source_totals", "reason": "expected_nested_counter"}
                )
            queue_existing_exchange_pair_totals = evidence.get("identity_resolution_existing_exchange_pair_totals")
            if not isinstance(queue_existing_exchange_pair_totals, dict) or any(
                not isinstance(pair_totals, dict)
                or any(not isinstance(value, int) or value < 0 for value in pair_totals.values())
                for pair_totals in (
                    queue_existing_exchange_pair_totals.values()
                    if isinstance(queue_existing_exchange_pair_totals, dict)
                    else []
                )
            ):
                masterfile_collision_campaign_evidence_gaps.append(
                    {
                        "field": "identity_resolution_existing_exchange_pair_totals",
                        "reason": "expected_nested_counter",
                    }
                )
            queue_pair_strategy_totals = evidence.get("identity_resolution_pair_review_strategy_totals")
            if not isinstance(queue_pair_strategy_totals, dict) or any(
                not isinstance(strategy_totals, dict)
                or any(not isinstance(value, int) or value < 0 for value in strategy_totals.values())
                for strategy_totals in (
                    queue_pair_strategy_totals.values()
                    if isinstance(queue_pair_strategy_totals, dict)
                    else []
                )
            ):
                masterfile_collision_campaign_evidence_gaps.append(
                    {
                        "field": "identity_resolution_pair_review_strategy_totals",
                        "reason": "expected_nested_counter",
                    }
                )
            queue_review_strategy_totals = evidence.get("identity_resolution_review_strategy_totals")
            if not isinstance(queue_review_strategy_totals, dict) or any(
                not isinstance(strategy_totals, dict)
                or any(not isinstance(value, int) or value < 0 for value in strategy_totals.values())
                for strategy_totals in (
                    queue_review_strategy_totals.values()
                    if isinstance(queue_review_strategy_totals, dict)
                    else []
                )
            ):
                masterfile_collision_campaign_evidence_gaps.append(
                    {
                        "field": "identity_resolution_review_strategy_totals",
                        "reason": "expected_nested_counter",
                    }
                )
            queue_evidence_required_totals = evidence.get("identity_resolution_evidence_required_totals")
            if not isinstance(queue_evidence_required_totals, dict) or any(
                not isinstance(evidence_totals, dict)
                or any(not isinstance(value, int) or value < 0 for value in evidence_totals.values())
                for evidence_totals in (
                    queue_evidence_required_totals.values()
                    if isinstance(queue_evidence_required_totals, dict)
                    else []
                )
            ):
                masterfile_collision_campaign_evidence_gaps.append(
                    {
                        "field": "identity_resolution_evidence_required_totals",
                        "reason": "expected_nested_counter",
                    }
                )
            queue_identity_evidence_totals = evidence.get("identity_resolution_identity_evidence_totals")
            if not isinstance(queue_identity_evidence_totals, dict) or any(
                not isinstance(evidence_totals, dict)
                or not evidence_totals
                or any(not isinstance(value, int) or value < 0 for value in evidence_totals.values())
                for evidence_totals in (
                    queue_identity_evidence_totals.values()
                    if isinstance(queue_identity_evidence_totals, dict)
                    else []
                )
            ):
                masterfile_collision_campaign_evidence_gaps.append(
                    {
                        "field": "identity_resolution_identity_evidence_totals",
                        "reason": "expected_nested_identity_evidence_counter",
                    }
                )
            clearance_readiness_totals = evidence.get("identity_resolution_clearance_readiness_totals")
            if not isinstance(clearance_readiness_totals, dict) or any(
                not isinstance(value, int) or value < 0 for value in clearance_readiness_totals.values()
            ):
                masterfile_collision_campaign_evidence_gaps.append(
                    {"field": "identity_resolution_clearance_readiness_totals", "reason": "expected_counter"}
                )
            queue_clearance_readiness_totals = evidence.get("identity_resolution_queue_clearance_readiness_totals")
            if not isinstance(queue_clearance_readiness_totals, dict) or any(
                not isinstance(readiness_totals, dict)
                or any(not isinstance(value, int) or value < 0 for value in readiness_totals.values())
                for readiness_totals in (
                    queue_clearance_readiness_totals.values()
                    if isinstance(queue_clearance_readiness_totals, dict)
                    else []
                )
            ):
                masterfile_collision_campaign_evidence_gaps.append(
                    {
                        "field": "identity_resolution_queue_clearance_readiness_totals",
                        "reason": "expected_nested_counter",
                    }
                )
            top_clearance_batches = evidence.get("top_identity_resolution_clearance_batches")
            if (
                not isinstance(top_clearance_batches, list)
                or not top_clearance_batches
                or any(
                    not isinstance(batch, dict)
                    or batch.get("identity_resolution_queue") not in COLLISION_IDENTITY_RESOLUTION_QUEUES
                    or batch.get("clearance_readiness") not in COLLISION_CLEARANCE_READINESS_STATUSES
                    or batch.get("clearance_readiness")
                    != COLLISION_QUEUE_TO_CLEARANCE_READINESS.get(str(batch.get("identity_resolution_queue", "")))
                    or not isinstance(batch.get("rows"), int)
                    or batch.get("rows", 0) <= 0
                    or batch.get("review_strategy") not in COLLISION_PAIR_REVIEW_STRATEGIES
                    or batch.get("review_strategy")
                    != COLLISION_QUEUE_TO_PAIR_REVIEW_STRATEGY.get(str(batch.get("identity_resolution_queue", "")))
                    or not batch.get("evidence_required")
                    or batch.get("evidence_required")
                    != COLLISION_QUEUE_TO_EVIDENCE_REQUIRED.get(str(batch.get("identity_resolution_queue", "")))
                    or not batch.get("recommended_next_source")
                    or not batch.get("source_gate")
                    for batch in top_clearance_batches
                )
            ):
                masterfile_collision_campaign_evidence_gaps.append(
                    {
                        "field": "top_identity_resolution_clearance_batches",
                        "reason": "expected_ranked_clearance_readiness_rows",
                    }
                )
            top_pair_batches = evidence.get("top_identity_resolution_pair_review_batches")
            if (
                not isinstance(top_pair_batches, list)
                or not top_pair_batches
                or any(
                    not isinstance(batch, dict)
                    or batch.get("identity_resolution_queue") not in COLLISION_IDENTITY_RESOLUTION_QUEUES
                    or "::" not in str(batch.get("exchange_pair", ""))
                    or not isinstance(batch.get("rows"), int)
                    or batch.get("rows", 0) <= 0
                    or batch.get("review_strategy") not in COLLISION_PAIR_REVIEW_STRATEGIES
                    or not batch.get("evidence_required")
                    or batch.get("evidence_required") in {"none", "ticker_match_only"}
                    or not batch.get("recommended_next_source")
                    or not batch.get("source_gate")
                    for batch in top_pair_batches
                )
            ):
                masterfile_collision_campaign_evidence_gaps.append(
                    {
                        "field": "top_identity_resolution_pair_review_batches",
                        "reason": "expected_ranked_pair_strategy_rows",
                    }
                )
            queue_totals = evidence.get("identity_resolution_queue_totals")
            exact_scope_rows = evidence.get("same_isin_exact_name_scope_review_rows")
            if (
                not isinstance(exact_scope_rows, int)
                or isinstance(exact_scope_rows, bool)
                or exact_scope_rows < 0
            ):
                masterfile_collision_campaign_evidence_gaps.append(
                    {"field": "same_isin_exact_name_scope_review_rows", "reason": "expected_nonnegative_integer"}
                )
            elif isinstance(queue_totals, dict):
                expected_exact_scope_rows = queue_totals.get("review_cross_listing_same_isin_exact_name", 0)
                if exact_scope_rows != expected_exact_scope_rows:
                    masterfile_collision_campaign_evidence_gaps.append(
                        {
                            "field": "same_isin_exact_name_scope_review_rows",
                            "reported": exact_scope_rows,
                            "expected": expected_exact_scope_rows,
                        }
                    )
            exact_scope_batches = evidence.get("top_same_isin_exact_name_scope_review_batches")
            if exact_scope_rows and (
                not isinstance(exact_scope_batches, list)
                or not exact_scope_batches
                or any(
                    not isinstance(batch, dict)
                    or "::" not in str(batch.get("exchange_pair", ""))
                    or not batch.get("official_source_key")
                    or not batch.get("official_asset_type")
                    or batch.get("clearance_evidence_required")
                    != "official_target_exchange_listing_status_mic_name_instrument_type"
                    or not isinstance(batch.get("rows"), int)
                    or batch.get("rows", 0) <= 0
                    or batch.get("review_strategy")
                    != COLLISION_QUEUE_TO_PAIR_REVIEW_STRATEGY.get("review_cross_listing_same_isin_exact_name")
                    or batch.get("evidence_required")
                    != COLLISION_QUEUE_TO_EVIDENCE_REQUIRED.get("review_cross_listing_same_isin_exact_name")
                    or not batch.get("recommended_next_source")
                    or not batch.get("source_gate")
                    for batch in exact_scope_batches
                )
            ):
                masterfile_collision_campaign_evidence_gaps.append(
                    {
                        "field": "top_same_isin_exact_name_scope_review_batches",
                        "reason": "expected_ranked_same_isin_exact_name_scope_batches",
                    }
                )
            elif (
                isinstance(exact_scope_batches, list)
                and isinstance(exact_scope_rows, int)
                and exact_scope_rows > 0
            ):
                reported_exact_scope_total = sum(
                    batch.get("rows", 0)
                    for batch in exact_scope_batches
                    if isinstance(batch, dict) and isinstance(batch.get("rows"), int)
                )
                if reported_exact_scope_total <= 0 or reported_exact_scope_total > exact_scope_rows:
                    masterfile_collision_campaign_evidence_gaps.append(
                        {
                            "field": "top_same_isin_exact_name_scope_review_batches",
                            "reported_total": reported_exact_scope_total,
                            "allowed_max": exact_scope_rows,
                        }
                    )
            queue_risk_flag_totals = evidence.get("identity_resolution_risk_flag_totals")
            if not isinstance(queue_risk_flag_totals, dict) or any(
                not isinstance(risk_flag_totals, dict)
                or any(not isinstance(value, int) or value < 0 for value in risk_flag_totals.values())
                for risk_flag_totals in (
                    queue_risk_flag_totals.values() if isinstance(queue_risk_flag_totals, dict) else []
                )
            ):
                masterfile_collision_campaign_evidence_gaps.append(
                    {"field": "identity_resolution_risk_flag_totals", "reason": "expected_nested_counter"}
                )
            collision_rows = evidence.get("collision_review_rows")
            if isinstance(collision_rows, int):
                for counter_key in (
                    "decision_totals",
                    "review_bucket_totals",
                    "review_priority_totals",
                    "identity_resolution_queue_totals",
                    "identity_resolution_clearance_readiness_totals",
                    "clearance_evidence_totals",
                    "exchange_totals",
                    "official_asset_type_totals",
                    "asset_type_mismatch_totals",
                    "official_source_totals",
                ):
                    counter = evidence.get(counter_key)
                    if isinstance(counter, dict) and sum(counter.values()) != collision_rows:
                        masterfile_collision_campaign_evidence_gaps.append(
                            {
                                "field": counter_key,
                                "reported_total": sum(counter.values()),
                                "collision_review_rows": collision_rows,
                            }
                        )
            review_bucket_totals = evidence.get("review_bucket_totals")
            if isinstance(review_bucket_totals, dict):
                missing_buckets = [
                    bucket
                    for bucket in (
                        "hold_symbol_only_collision_needs_non_symbol_identity",
                        "same_isin_cross_listing_needs_name_or_scope_review",
                        "distinct_official_isin_new_listing_candidate",
                    )
                    if bucket not in review_bucket_totals
                ]
                if missing_buckets:
                    masterfile_collision_campaign_evidence_gaps.append(
                        {"field": "review_bucket_totals", "missing_keys": missing_buckets}
                    )
            if isinstance(queue_totals, dict):
                missing_queues = [
                    queue
                    for queue in (
                        "blocked_symbol_only_missing_non_symbol_identity",
                        "review_cross_listing_same_isin_name_or_scope_gap",
                        "review_distinct_official_isin_new_listing",
                    )
                    if queue not in queue_totals
                ]
                if missing_queues:
                    masterfile_collision_campaign_evidence_gaps.append(
                        {"field": "identity_resolution_queue_totals", "missing_keys": missing_queues}
                    )
            if (
                isinstance(queue_totals, dict)
                and isinstance(clearance_readiness_totals, dict)
                and isinstance(queue_clearance_readiness_totals, dict)
            ):
                expected_clearance_totals: Counter[str] = Counter()
                for queue, count in queue_totals.items():
                    expected_readiness = COLLISION_QUEUE_TO_CLEARANCE_READINESS.get(str(queue))
                    queue_counts = queue_clearance_readiness_totals.get(queue)
                    if expected_readiness is None:
                        continue
                    expected_clearance_totals[expected_readiness] += count
                    if not isinstance(queue_counts, dict) or queue_counts.get(expected_readiness) != count:
                        masterfile_collision_campaign_evidence_gaps.append(
                            {
                                "field": f"identity_resolution_queue_clearance_readiness_totals.{queue}",
                                "reported": queue_counts,
                                "expected": {expected_readiness: count},
                            }
                        )
                    elif any(readiness != expected_readiness for readiness in queue_counts):
                        masterfile_collision_campaign_evidence_gaps.append(
                            {
                                "field": f"identity_resolution_queue_clearance_readiness_totals.{queue}",
                                "unexpected_readiness": sorted(
                                    readiness for readiness in queue_counts if readiness != expected_readiness
                                ),
                            }
                        )
                if dict(sorted(expected_clearance_totals.items())) != clearance_readiness_totals:
                    masterfile_collision_campaign_evidence_gaps.append(
                        {
                            "field": "identity_resolution_clearance_readiness_totals",
                            "reported": clearance_readiness_totals,
                            "expected": dict(sorted(expected_clearance_totals.items())),
                        }
                    )
            if isinstance(queue_totals, dict) and isinstance(queue_exchange_totals, dict):
                for queue, count in queue_totals.items():
                    exchange_counts = queue_exchange_totals.get(queue)
                    if not isinstance(exchange_counts, dict) or sum(exchange_counts.values()) != count:
                        masterfile_collision_campaign_evidence_gaps.append(
                            {
                                "field": f"identity_resolution_exchange_totals.{queue}",
                                "reported_total": sum(exchange_counts.values()) if isinstance(exchange_counts, dict) else None,
                                "expected": count,
                            }
                        )
            if isinstance(queue_totals, dict) and isinstance(queue_asset_type_totals, dict):
                for queue, count in queue_totals.items():
                    asset_type_counts = queue_asset_type_totals.get(queue)
                    if not isinstance(asset_type_counts, dict) or sum(asset_type_counts.values()) != count:
                        masterfile_collision_campaign_evidence_gaps.append(
                            {
                                "field": f"identity_resolution_asset_type_totals.{queue}",
                                "reported_total": sum(asset_type_counts.values())
                                if isinstance(asset_type_counts, dict)
                                else None,
                                "expected": count,
                            }
                        )
            if isinstance(queue_totals, dict) and isinstance(queue_official_source_totals, dict):
                for queue, count in queue_totals.items():
                    source_counts = queue_official_source_totals.get(queue)
                    if not isinstance(source_counts, dict) or sum(source_counts.values()) != count:
                        masterfile_collision_campaign_evidence_gaps.append(
                            {
                                "field": f"identity_resolution_official_source_totals.{queue}",
                                "reported_total": sum(source_counts.values())
                                if isinstance(source_counts, dict)
                                else None,
                                "expected": count,
                            }
                        )
            if isinstance(queue_totals, dict) and isinstance(queue_risk_flag_totals, dict):
                for queue, count in queue_totals.items():
                    risk_flag_counts = queue_risk_flag_totals.get(queue)
                    if not isinstance(risk_flag_counts, dict):
                        masterfile_collision_campaign_evidence_gaps.append(
                            {
                                "field": f"identity_resolution_risk_flag_totals.{queue}",
                                "reason": "expected_counter",
                            }
                        )
                        continue
                    if risk_flag_counts.get("ticker_reused_on_other_exchange") != count:
                        masterfile_collision_campaign_evidence_gaps.append(
                            {
                                "field": f"identity_resolution_risk_flag_totals.{queue}.ticker_reused_on_other_exchange",
                                "reported": risk_flag_counts.get("ticker_reused_on_other_exchange"),
                                "expected": count,
                            }
                        )
                    if queue == "blocked_asset_type_conflict" and risk_flag_counts.get("asset_type_mismatch") != count:
                        masterfile_collision_campaign_evidence_gaps.append(
                            {
                                "field": "identity_resolution_risk_flag_totals.blocked_asset_type_conflict.asset_type_mismatch",
                                "reported": risk_flag_counts.get("asset_type_mismatch"),
                                "expected": count,
                            }
                        )
            if isinstance(queue_totals, dict) and isinstance(queue_review_strategy_totals, dict):
                for queue, count in queue_totals.items():
                    strategy_counts = queue_review_strategy_totals.get(queue)
                    if not isinstance(strategy_counts, dict):
                        masterfile_collision_campaign_evidence_gaps.append(
                            {
                                "field": f"identity_resolution_review_strategy_totals.{queue}",
                                "reason": "expected_counter",
                            }
                        )
                        continue
                    if sum(strategy_counts.values()) != count:
                        masterfile_collision_campaign_evidence_gaps.append(
                            {
                                "field": f"identity_resolution_review_strategy_totals.{queue}",
                                "reported_total": sum(strategy_counts.values()),
                                "expected": count,
                            }
                        )
                    unexpected_strategies = sorted(
                        strategy
                        for strategy in strategy_counts
                        if strategy not in COLLISION_PAIR_REVIEW_STRATEGIES
                        or strategy != COLLISION_QUEUE_TO_PAIR_REVIEW_STRATEGY.get(queue)
                    )
                    if unexpected_strategies:
                        masterfile_collision_campaign_evidence_gaps.append(
                            {
                                "field": f"identity_resolution_review_strategy_totals.{queue}",
                                "unexpected_strategies": unexpected_strategies,
                            }
                        )
            if isinstance(queue_totals, dict) and isinstance(queue_evidence_required_totals, dict):
                for queue, count in queue_totals.items():
                    evidence_counts = queue_evidence_required_totals.get(queue)
                    if not isinstance(evidence_counts, dict):
                        masterfile_collision_campaign_evidence_gaps.append(
                            {
                                "field": f"identity_resolution_evidence_required_totals.{queue}",
                                "reason": "expected_counter",
                            }
                        )
                        continue
                    if sum(evidence_counts.values()) != count:
                        masterfile_collision_campaign_evidence_gaps.append(
                            {
                                "field": f"identity_resolution_evidence_required_totals.{queue}",
                                "reported_total": sum(evidence_counts.values()),
                                "expected": count,
                            }
                        )
                    unexpected_evidence = sorted(
                        evidence_required
                        for evidence_required in evidence_counts
                        if evidence_required != COLLISION_QUEUE_TO_EVIDENCE_REQUIRED.get(queue)
                    )
                    if unexpected_evidence:
                        masterfile_collision_campaign_evidence_gaps.append(
                            {
                                "field": f"identity_resolution_evidence_required_totals.{queue}",
                                "unexpected_evidence_required": unexpected_evidence,
                            }
                        )
            if isinstance(queue_totals, dict) and isinstance(queue_identity_evidence_totals, dict):
                for queue, count in queue_totals.items():
                    identity_counts = queue_identity_evidence_totals.get(queue)
                    if not isinstance(identity_counts, dict):
                        masterfile_collision_campaign_evidence_gaps.append(
                            {
                                "field": f"identity_resolution_identity_evidence_totals.{queue}",
                                "reason": "expected_counter",
                            }
                        )
                        continue
                    if queue in {
                        "review_cross_listing_same_isin_exact_name",
                        "review_cross_listing_same_isin_name_or_scope_gap",
                        "review_distinct_official_isin_new_listing",
                    } and identity_counts.get("official_isin") != count:
                        masterfile_collision_campaign_evidence_gaps.append(
                            {
                                "field": f"identity_resolution_identity_evidence_totals.{queue}.official_isin",
                                "reported": identity_counts.get("official_isin"),
                                "expected": count,
                            }
                        )
                    if queue in {
                        "review_cross_listing_same_isin_exact_name",
                        "review_cross_listing_same_isin_name_or_scope_gap",
                    } and identity_counts.get("same_isin_existing_listing") != count:
                        masterfile_collision_campaign_evidence_gaps.append(
                            {
                                "field": f"identity_resolution_identity_evidence_totals.{queue}.same_isin_existing_listing",
                                "reported": identity_counts.get("same_isin_existing_listing"),
                                "expected": count,
                            }
                        )
                    if queue == "review_cross_listing_same_isin_exact_name" and identity_counts.get("exact_name_match") != count:
                        masterfile_collision_campaign_evidence_gaps.append(
                            {
                                "field": "identity_resolution_identity_evidence_totals.review_cross_listing_same_isin_exact_name.exact_name_match",
                                "reported": identity_counts.get("exact_name_match"),
                                "expected": count,
                            }
                        )
                    if queue == "blocked_asset_type_conflict" and identity_counts.get("asset_type_conflict") != count:
                        masterfile_collision_campaign_evidence_gaps.append(
                            {
                                "field": "identity_resolution_identity_evidence_totals.blocked_asset_type_conflict.asset_type_conflict",
                                "reported": identity_counts.get("asset_type_conflict"),
                                "expected": count,
                            }
                        )
                    if queue == "blocked_symbol_only_missing_non_symbol_identity" and identity_counts.get("official_isin", 0) != 0:
                        masterfile_collision_campaign_evidence_gaps.append(
                            {
                                "field": "identity_resolution_identity_evidence_totals.blocked_symbol_only_missing_non_symbol_identity.official_isin",
                                "reported": identity_counts.get("official_isin"),
                                "expected": 0,
                            }
                        )
            if (
                isinstance(queue_existing_exchange_pair_totals, dict)
                and isinstance(queue_pair_strategy_totals, dict)
            ):
                for queue, pair_counts in queue_existing_exchange_pair_totals.items():
                    if not isinstance(pair_counts, dict):
                        continue
                    expected_pair_total = sum(pair_counts.values())
                    strategy_counts = queue_pair_strategy_totals.get(queue)
                    if not isinstance(strategy_counts, dict):
                        masterfile_collision_campaign_evidence_gaps.append(
                            {
                                "field": f"identity_resolution_pair_review_strategy_totals.{queue}",
                                "reason": "expected_counter",
                            }
                        )
                        continue
                    if sum(strategy_counts.values()) != expected_pair_total:
                        masterfile_collision_campaign_evidence_gaps.append(
                            {
                                "field": f"identity_resolution_pair_review_strategy_totals.{queue}",
                                "reported_total": sum(strategy_counts.values()),
                                "expected_pair_total": expected_pair_total,
                            }
                        )
                    unexpected_strategies = sorted(
                        strategy
                        for strategy in strategy_counts
                        if strategy not in COLLISION_PAIR_REVIEW_STRATEGIES
                        or strategy != COLLISION_QUEUE_TO_PAIR_REVIEW_STRATEGY.get(queue)
                    )
                    if unexpected_strategies:
                        masterfile_collision_campaign_evidence_gaps.append(
                            {
                                "field": f"identity_resolution_pair_review_strategy_totals.{queue}",
                                "unexpected_strategies": unexpected_strategies,
                            }
                        )
            if isinstance(top_pair_batches, list):
                for index, batch in enumerate(top_pair_batches):
                    if not isinstance(batch, dict):
                        continue
                    queue = str(batch.get("identity_resolution_queue", ""))
                    strategy = str(batch.get("review_strategy", ""))
                    if (
                        queue in COLLISION_IDENTITY_RESOLUTION_QUEUES
                        and strategy != COLLISION_QUEUE_TO_PAIR_REVIEW_STRATEGY.get(queue)
                    ):
                        masterfile_collision_campaign_evidence_gaps.append(
                            {
                                "field": "top_identity_resolution_pair_review_batches",
                                "row_index": index,
                                "queue": queue,
                                "review_strategy": strategy,
                                "expected": COLLISION_QUEUE_TO_PAIR_REVIEW_STRATEGY.get(queue),
                            }
                        )
            decision_totals = evidence.get("decision_totals")
            if isinstance(decision_totals, dict):
                unexpected_decisions = sorted(
                    str(decision) for decision in decision_totals if decision not in COLLISION_REVIEW_DECISIONS
                )
                if unexpected_decisions:
                    masterfile_collision_campaign_evidence_gaps.append(
                        {"field": "decision_totals", "unexpected_decisions": unexpected_decisions}
                    )
            clearance_totals = evidence.get("clearance_evidence_totals")
            if isinstance(clearance_totals, dict):
                unsafe_clearance = [
                    key
                    for key in clearance_totals
                    if "symbol_only" in str(key) or str(key) in {"none", "ticker_match_only"}
                ]
                if unsafe_clearance:
                    masterfile_collision_campaign_evidence_gaps.append(
                        {"field": "clearance_evidence_totals", "forbidden_keys": unsafe_clearance}
                    )
            if isinstance(risk_counter, dict) and isinstance(collision_rows, int):
                ticker_reuse_rows = risk_counter.get("ticker_reused_on_other_exchange")
                if ticker_reuse_rows != collision_rows:
                    masterfile_collision_campaign_evidence_gaps.append(
                        {
                            "field": "collision_risk_flag_totals.ticker_reused_on_other_exchange",
                            "reported": ticker_reuse_rows,
                            "collision_review_rows": collision_rows,
                        }
                    )
            mismatch_totals = evidence.get("asset_type_mismatch_totals")
            if isinstance(mismatch_totals, dict) and isinstance(evidence.get("asset_type_mismatches"), int):
                if mismatch_totals.get("true", 0) != evidence.get("asset_type_mismatches"):
                    masterfile_collision_campaign_evidence_gaps.append(
                        {
                            "field": "asset_type_mismatch_totals.true",
                            "reported": mismatch_totals.get("true", 0),
                            "asset_type_mismatches": evidence.get("asset_type_mismatches"),
                        }
                    )
            known_deltas = delta_evidence.get("known_deltas") if isinstance(delta_evidence, dict) else {}
            if not isinstance(known_deltas, dict):
                known_deltas = {}
            missing_delta_evidence_keys = [
                key for key in MASTERFILE_COLLISION_CAMPAIGN_DELTA_EVIDENCE_KEYS if key not in known_deltas
            ]
            if missing_delta_evidence_keys:
                masterfile_collision_campaign_evidence_gaps.append(
                    {"field": "delta_evidence.known_deltas", "missing_keys": missing_delta_evidence_keys}
                )
            if isinstance(collision_rows, int) and known_deltas.get("current_collision_review_rows") != collision_rows:
                masterfile_collision_campaign_evidence_gaps.append(
                    {
                        "field": "delta_evidence.current_collision_review_rows",
                        "reported": known_deltas.get("current_collision_review_rows"),
                        "expected": collision_rows,
                    }
                )
            for delta_key in ("collision_resolution_delta", "listing_addition_delta"):
                if known_deltas.get(delta_key) != 0:
                    masterfile_collision_campaign_evidence_gaps.append(
                        {"field": f"delta_evidence.{delta_key}", "reported": known_deltas.get(delta_key), "expected": 0}
                    )
            warn_quarantine_delta = known_deltas.get("warn_quarantine_delta")
            global_acceptance_deltas = matrix.get("global_acceptance_deltas", {})
            expected_warn_quarantine_delta = {
                "warn_delta": global_acceptance_deltas.get("warn_delta", {}).get("delta", 0)
                if isinstance(global_acceptance_deltas, dict)
                else 0,
                "quarantine_delta": global_acceptance_deltas.get("quarantine_delta", {}).get("delta", 0)
                if isinstance(global_acceptance_deltas, dict)
                else 0,
            }
            if warn_quarantine_delta != expected_warn_quarantine_delta:
                masterfile_collision_campaign_evidence_gaps.append(
                    {
                        "field": "delta_evidence.warn_quarantine_delta",
                        "reported": warn_quarantine_delta,
                        "expected": expected_warn_quarantine_delta,
                    }
                )
        if campaign_key == "symbol_changes":
            evidence = campaign.get("evidence", {})
            delta_evidence = campaign.get("delta_evidence", {})
            if not isinstance(evidence, dict):
                symbol_change_campaign_evidence_gaps.append({"field": "evidence", "reason": "expected_object"})
                evidence = {}
            missing_evidence_keys = [
                key for key in SYMBOL_CHANGE_CAMPAIGN_EVIDENCE_KEYS if key not in evidence
            ]
            if missing_evidence_keys:
                symbol_change_campaign_evidence_gaps.append(
                    {"field": "evidence", "missing_keys": missing_evidence_keys}
                )
            for key in ("symbol_change_review_rows", "time_sensitive_review_rows"):
                if not isinstance(evidence.get(key), int) or isinstance(evidence.get(key), bool) or evidence.get(key, -1) < 0:
                    symbol_change_campaign_evidence_gaps.append(
                        {"field": key, "reason": "expected_nonnegative_integer"}
                    )
            for counter_key in (
                "match_status_counts",
                "symbol_change_workflow_queue_counts",
                "review_bucket_counts",
                "review_priority_counts",
                "recency_bucket_counts",
                "review_priority_recency_counts",
                "workflow_queue_recency_counts",
                "workflow_queue_scope_counts",
                "workflow_queue_match_status_counts",
                "apply_eligibility_counts",
                "symbol_change_apply_readiness_counts",
                "verification_evidence_required_counts",
                "time_sensitive_workflow_queue_counts",
                "time_sensitive_recency_counts",
                "time_sensitive_apply_readiness_counts",
                "recommended_action_counts",
                "exchange_scope_status_counts",
            ):
                counter = evidence.get(counter_key)
                if not isinstance(counter, dict) or any(not isinstance(value, int) or value < 0 for value in counter.values()):
                    symbol_change_campaign_evidence_gaps.append({"field": counter_key, "reason": "expected_counter"})
            symbol_rows = evidence.get("symbol_change_review_rows")
            if isinstance(symbol_rows, int):
                for counter_key in (
                    "match_status_counts",
                    "symbol_change_workflow_queue_counts",
                    "review_bucket_counts",
                    "review_priority_counts",
                "recency_bucket_counts",
                "workflow_queue_recency_counts",
                "workflow_queue_priority_counts",
                "workflow_queue_scope_counts",
                    "workflow_queue_match_status_counts",
                    "apply_eligibility_counts",
                    "symbol_change_apply_readiness_counts",
                    "verification_evidence_required_counts",
                    "recommended_action_counts",
                    "exchange_scope_status_counts",
                ):
                    counter = evidence.get(counter_key)
                    if isinstance(counter, dict) and sum(counter.values()) != symbol_rows:
                        symbol_change_campaign_evidence_gaps.append(
                            {
                                "field": counter_key,
                                "reported_total": sum(counter.values()),
                                "symbol_change_review_rows": symbol_rows,
                            }
                        )
            review_priority_counts = evidence.get("review_priority_counts")
            recency_counts = evidence.get("recency_bucket_counts")
            priority_recency_counts = evidence.get("review_priority_recency_counts")
            workflow_queue_counts = evidence.get("symbol_change_workflow_queue_counts")
            workflow_queue_recency_counts = evidence.get("workflow_queue_recency_counts")
            workflow_queue_priority_counts = evidence.get("workflow_queue_priority_counts")
            workflow_queue_scope_counts = evidence.get("workflow_queue_scope_counts")
            workflow_queue_match_status_counts = evidence.get("workflow_queue_match_status_counts")
            workflow_queue_source_hint_counts = evidence.get("workflow_queue_source_hint_counts")
            workflow_queue_source_confidence_counts = evidence.get("workflow_queue_source_confidence_counts")
            workflow_queue_issuer_name_review_counts = evidence.get("workflow_queue_issuer_name_review_counts")
            workflow_queue_listing_key_review_counts = evidence.get("workflow_queue_listing_key_review_counts")
            workflow_queue_review_strategy_counts = evidence.get("workflow_queue_review_strategy_counts")
            time_sensitive_workflow_queue_counts = evidence.get("time_sensitive_workflow_queue_counts")
            time_sensitive_recency_counts = evidence.get("time_sensitive_recency_counts")
            time_sensitive_apply_readiness_counts = evidence.get("time_sensitive_apply_readiness_counts")
            top_time_sensitive_batches = evidence.get("top_time_sensitive_symbol_change_batches")
            for field, nested_counter in (
                ("workflow_queue_source_hint_counts", workflow_queue_source_hint_counts),
                ("workflow_queue_source_confidence_counts", workflow_queue_source_confidence_counts),
                ("workflow_queue_issuer_name_review_counts", workflow_queue_issuer_name_review_counts),
                ("workflow_queue_listing_key_review_counts", workflow_queue_listing_key_review_counts),
                ("workflow_queue_review_strategy_counts", workflow_queue_review_strategy_counts),
            ):
                if not isinstance(nested_counter, dict) or any(
                    not isinstance(value, dict)
                    or any(not isinstance(count, int) or count < 0 for count in value.values())
                    for value in (nested_counter.values() if isinstance(nested_counter, dict) else [])
                ):
                    symbol_change_campaign_evidence_gaps.append(
                        {"field": field, "reason": "expected_nested_counter"}
                    )
            if isinstance(review_priority_counts, dict) and isinstance(priority_recency_counts, dict):
                priority_totals: Counter[str] = Counter()
                for key, value in priority_recency_counts.items():
                    priority = str(key).split(":", 1)[0]
                    priority_totals[priority] += value
                if dict(priority_totals) != review_priority_counts:
                    symbol_change_campaign_evidence_gaps.append(
                        {
                            "field": "review_priority_recency_counts",
                            "priority_totals": dict(priority_totals),
                            "review_priority_counts": review_priority_counts,
                        }
                    )
            if isinstance(workflow_queue_counts, dict) and isinstance(workflow_queue_recency_counts, dict):
                workflow_totals: Counter[str] = Counter()
                for key, value in workflow_queue_recency_counts.items():
                    workflow = str(key).split(":", 1)[0]
                    workflow_totals[workflow] += value
                if dict(workflow_totals) != workflow_queue_counts:
                    symbol_change_campaign_evidence_gaps.append(
                        {
                            "field": "workflow_queue_recency_counts",
                            "workflow_totals": dict(workflow_totals),
                            "symbol_change_workflow_queue_counts": workflow_queue_counts,
                        }
                    )
            if isinstance(workflow_queue_counts, dict) and isinstance(workflow_queue_priority_counts, dict):
                workflow_totals = Counter()
                for key, value in workflow_queue_priority_counts.items():
                    workflow = str(key).split(":", 1)[0]
                    workflow_totals[workflow] += value
                if dict(workflow_totals) != workflow_queue_counts:
                    symbol_change_campaign_evidence_gaps.append(
                        {
                            "field": "workflow_queue_priority_counts",
                            "workflow_totals": dict(workflow_totals),
                            "symbol_change_workflow_queue_counts": workflow_queue_counts,
                        }
                    )
            if isinstance(workflow_queue_counts, dict) and isinstance(workflow_queue_scope_counts, dict):
                workflow_totals = Counter()
                for key, value in workflow_queue_scope_counts.items():
                    workflow = str(key).split(":", 1)[0]
                    workflow_totals[workflow] += value
                if dict(workflow_totals) != workflow_queue_counts:
                    symbol_change_campaign_evidence_gaps.append(
                        {
                            "field": "workflow_queue_scope_counts",
                            "workflow_totals": dict(workflow_totals),
                            "symbol_change_workflow_queue_counts": workflow_queue_counts,
                        }
                    )
            if isinstance(workflow_queue_counts, dict) and isinstance(workflow_queue_match_status_counts, dict):
                workflow_totals = Counter()
                for key, value in workflow_queue_match_status_counts.items():
                    workflow = str(key).split(":", 1)[0]
                    workflow_totals[workflow] += value
                if dict(workflow_totals) != workflow_queue_counts:
                    symbol_change_campaign_evidence_gaps.append(
                        {
                            "field": "workflow_queue_match_status_counts",
                            "workflow_totals": dict(workflow_totals),
                            "symbol_change_workflow_queue_counts": workflow_queue_counts,
                        }
                    )
            if isinstance(workflow_queue_counts, dict):
                for field, nested_counter in (
                    ("workflow_queue_source_hint_counts", workflow_queue_source_hint_counts),
                    ("workflow_queue_source_confidence_counts", workflow_queue_source_confidence_counts),
                    ("workflow_queue_listing_key_review_counts", workflow_queue_listing_key_review_counts),
                    ("workflow_queue_review_strategy_counts", workflow_queue_review_strategy_counts),
                ):
                    if not isinstance(nested_counter, dict):
                        continue
                    for queue, count in workflow_queue_counts.items():
                        queue_counter = nested_counter.get(queue)
                        if not isinstance(queue_counter, dict) or sum(queue_counter.values()) != count:
                            symbol_change_campaign_evidence_gaps.append(
                                {
                                    "field": f"{field}.{queue}",
                                    "reported_total": sum(queue_counter.values())
                                    if isinstance(queue_counter, dict)
                                    else None,
                                    "expected": count,
                                }
                            )
                    if field == "workflow_queue_review_strategy_counts":
                        for queue, queue_counter in nested_counter.items():
                            if not isinstance(queue_counter, dict):
                                continue
                            expected_strategy, _expected_evidence = symbol_change_workflow_strategy_for(str(queue))
                            invalid_strategies = [
                                strategy
                                for strategy in queue_counter
                                if strategy not in SYMBOL_CHANGE_REVIEW_STRATEGIES or strategy != expected_strategy
                            ]
                            if invalid_strategies:
                                symbol_change_campaign_evidence_gaps.append(
                                    {
                                        "field": f"{field}.{queue}",
                                        "invalid_strategies": sorted(invalid_strategies),
                                        "expected_strategy": expected_strategy,
                                    }
                                )
                    if field == "workflow_queue_listing_key_review_counts":
                        for queue, queue_counter in nested_counter.items():
                            if not isinstance(queue_counter, dict):
                                continue
                            invalid_statuses = sorted(
                                status
                                for status in queue_counter
                                if status not in SYMBOL_CHANGE_LISTING_KEY_REVIEW_STATUSES
                            )
                            if invalid_statuses:
                                symbol_change_campaign_evidence_gaps.append(
                                    {
                                        "field": f"{field}.{queue}",
                                        "invalid_statuses": invalid_statuses,
                                    }
                                )
                        expected_status_by_queue = {
                            "review_verified_rename_or_delisting": {"old_scoped_listing_key_only"},
                            "review_duplicate_or_cross_listing": {"old_and_new_scoped_listing_keys_present"},
                            "document_no_dataset_match": {"no_scoped_listing_key_match"},
                        }
                        for queue, expected_statuses in expected_status_by_queue.items():
                            if queue in workflow_queue_counts:
                                queue_counter = nested_counter.get(queue)
                                if not isinstance(queue_counter, dict):
                                    continue
                                unexpected_statuses = sorted(
                                    status for status in queue_counter if status not in expected_statuses
                                )
                                if unexpected_statuses:
                                    symbol_change_campaign_evidence_gaps.append(
                                        {
                                            "field": f"{field}.{queue}",
                                            "unexpected_statuses": unexpected_statuses,
                                            "expected_statuses": sorted(expected_statuses),
                                        }
                                    )
            top_workflow_batches = evidence.get("top_symbol_change_workflow_batches")
            if not isinstance(top_workflow_batches, list) or not top_workflow_batches:
                symbol_change_campaign_evidence_gaps.append(
                    {"field": "top_symbol_change_workflow_batches", "reason": "expected_ranked_workflow_batches"}
                )
            else:
                for index, batch in enumerate(top_workflow_batches):
                    if not isinstance(batch, dict):
                        symbol_change_campaign_evidence_gaps.append(
                            {"field": "top_symbol_change_workflow_batches", "row_index": index, "reason": "expected_object"}
                        )
                        continue
                    queue = str(batch.get("symbol_change_workflow_queue", ""))
                    strategy = str(batch.get("review_strategy", ""))
                    expected_strategy, _expected_evidence = symbol_change_workflow_strategy_for(queue)
                    missing_keys = [
                        key
                        for key in (
                            "symbol_change_workflow_queue",
                            "review_priority",
                            "recency_bucket",
                            "exchange_scope_status",
                            "rows",
                            "review_strategy",
                            "evidence_required",
                            "recommended_next_source",
                            "source_gate",
                        )
                        if key not in batch
                    ]
                    invalid_fields = []
                    if queue not in SYMBOL_CHANGE_WORKFLOW_QUEUES:
                        invalid_fields.append("symbol_change_workflow_queue")
                    if batch.get("review_priority") not in SYMBOL_CHANGE_PRIORITIES:
                        invalid_fields.append("review_priority")
                    if batch.get("recency_bucket") not in SYMBOL_CHANGE_RECENCY_BUCKETS:
                        invalid_fields.append("recency_bucket")
                    if batch.get("exchange_scope_status") not in SYMBOL_CHANGE_EXCHANGE_SCOPE_STATUSES:
                        invalid_fields.append("exchange_scope_status")
                    if not isinstance(batch.get("rows"), int) or isinstance(batch.get("rows"), bool) or batch.get("rows", 0) <= 0:
                        invalid_fields.append("rows")
                    if strategy not in SYMBOL_CHANGE_REVIEW_STRATEGIES or strategy != expected_strategy:
                        invalid_fields.append("review_strategy")
                    if not batch.get("evidence_required") or batch.get("evidence_required") in {"none", "ticker_match_only"}:
                        invalid_fields.append("evidence_required")
                    if not batch.get("recommended_next_source"):
                        invalid_fields.append("recommended_next_source")
                    if not batch.get("source_gate"):
                        invalid_fields.append("source_gate")
                    if missing_keys or invalid_fields:
                        symbol_change_campaign_evidence_gaps.append(
                            {
                                "field": "top_symbol_change_workflow_batches",
                                "row_index": index,
                                "missing_keys": missing_keys,
                                "invalid_fields": invalid_fields,
                            }
                        )
            if isinstance(recency_counts, dict) and isinstance(priority_recency_counts, dict):
                recency_totals: Counter[str] = Counter()
                for key, value in priority_recency_counts.items():
                    parts = str(key).split(":", 1)
                    if len(parts) == 2:
                        recency_totals[parts[1]] += value
                if dict(recency_totals) != recency_counts:
                    symbol_change_campaign_evidence_gaps.append(
                        {
                            "field": "review_priority_recency_counts",
                            "recency_totals": dict(recency_totals),
                            "recency_bucket_counts": recency_counts,
                        }
                    )
            time_sensitive_rows = evidence.get("time_sensitive_review_rows")
            if isinstance(time_sensitive_rows, int):
                for field, counter in (
                    ("time_sensitive_workflow_queue_counts", time_sensitive_workflow_queue_counts),
                    ("time_sensitive_recency_counts", time_sensitive_recency_counts),
                    ("time_sensitive_apply_readiness_counts", time_sensitive_apply_readiness_counts),
                ):
                    if isinstance(counter, dict) and sum(counter.values()) != time_sensitive_rows:
                        symbol_change_campaign_evidence_gaps.append(
                            {
                                "field": field,
                                "reported_total": sum(counter.values()),
                                "time_sensitive_review_rows": time_sensitive_rows,
                            }
                        )
                if isinstance(time_sensitive_recency_counts, dict):
                    invalid_recency = sorted(
                        recency
                        for recency in time_sensitive_recency_counts
                        if recency not in {"recent_7d", "recent_30d"}
                    )
                    if invalid_recency:
                        symbol_change_campaign_evidence_gaps.append(
                            {
                                "field": "time_sensitive_recency_counts",
                                "invalid_recency_buckets": invalid_recency,
                            }
                        )
                if isinstance(time_sensitive_workflow_queue_counts, dict):
                    invalid_queues = sorted(
                        queue
                        for queue in time_sensitive_workflow_queue_counts
                        if queue not in {"review_verified_rename_or_delisting", "review_duplicate_or_cross_listing"}
                    )
                    if invalid_queues:
                        symbol_change_campaign_evidence_gaps.append(
                            {
                                "field": "time_sensitive_workflow_queue_counts",
                                "invalid_queues": invalid_queues,
                            }
                        )
                if not isinstance(top_time_sensitive_batches, list) or (
                    time_sensitive_rows > 0 and not top_time_sensitive_batches
                ):
                    symbol_change_campaign_evidence_gaps.append(
                        {
                            "field": "top_time_sensitive_symbol_change_batches",
                            "reason": "expected_ranked_time_sensitive_workflow_batches",
                        }
                    )
                elif isinstance(top_time_sensitive_batches, list):
                    top_time_sensitive_total = 0
                    for index, batch in enumerate(top_time_sensitive_batches):
                        if not isinstance(batch, dict):
                            symbol_change_campaign_evidence_gaps.append(
                                {
                                    "field": "top_time_sensitive_symbol_change_batches",
                                    "row_index": index,
                                    "reason": "expected_object",
                                }
                            )
                            continue
                        queue = str(batch.get("symbol_change_workflow_queue", ""))
                        strategy = str(batch.get("review_strategy", ""))
                        expected_strategy, _expected_evidence = symbol_change_workflow_strategy_for(queue)
                        missing_keys = [
                            key
                            for key in (
                                "symbol_change_workflow_queue",
                                "recency_bucket",
                                "exchange_scope_status",
                                "match_status",
                                "listing_key_review_status",
                                "rows",
                                "review_strategy",
                                "evidence_required",
                                "recommended_next_source",
                                "source_gate",
                            )
                            if key not in batch
                        ]
                        invalid_fields = []
                        if queue not in {"review_verified_rename_or_delisting", "review_duplicate_or_cross_listing"}:
                            invalid_fields.append("symbol_change_workflow_queue")
                        if batch.get("recency_bucket") not in {"recent_7d", "recent_30d"}:
                            invalid_fields.append("recency_bucket")
                        if batch.get("exchange_scope_status") not in SYMBOL_CHANGE_EXCHANGE_SCOPE_STATUSES:
                            invalid_fields.append("exchange_scope_status")
                        if batch.get("match_status") not in {
                            "old_symbol_present_new_symbol_missing",
                            "old_and_new_symbols_present",
                        }:
                            invalid_fields.append("match_status")
                        if batch.get("listing_key_review_status") not in {
                            "old_scoped_listing_key_only",
                            "old_and_new_scoped_listing_keys_present",
                        }:
                            invalid_fields.append("listing_key_review_status")
                        if not isinstance(batch.get("rows"), int) or isinstance(batch.get("rows"), bool) or batch.get("rows", 0) <= 0:
                            invalid_fields.append("rows")
                        else:
                            top_time_sensitive_total += batch.get("rows", 0)
                        if strategy not in SYMBOL_CHANGE_REVIEW_STRATEGIES or strategy != expected_strategy:
                            invalid_fields.append("review_strategy")
                        if not batch.get("evidence_required") or batch.get("evidence_required") in {"none", "ticker_match_only"}:
                            invalid_fields.append("evidence_required")
                        if not batch.get("recommended_next_source"):
                            invalid_fields.append("recommended_next_source")
                        if not batch.get("source_gate"):
                            invalid_fields.append("source_gate")
                        if missing_keys or invalid_fields:
                            symbol_change_campaign_evidence_gaps.append(
                                {
                                    "field": "top_time_sensitive_symbol_change_batches",
                                    "row_index": index,
                                    "missing_keys": missing_keys,
                                    "invalid_fields": invalid_fields,
                                }
                            )
                    if top_time_sensitive_total != time_sensitive_rows:
                        symbol_change_campaign_evidence_gaps.append(
                            {
                                "field": "top_time_sensitive_symbol_change_batches",
                                "reported_total": top_time_sensitive_total,
                                "time_sensitive_review_rows": time_sensitive_rows,
                            }
                        )
            review_bucket_counts = evidence.get("review_bucket_counts")
            if isinstance(review_bucket_counts, dict):
                required_distinctions = {
                    "already_reflected": {
                        "already_reflected_in_source_scope",
                        "already_reflected_in_scope_with_global_symbol_collision",
                    },
                    "rename_or_delisting": {"action_required_possible_rename_or_delisting"},
                    "duplicate_or_cross_listing": {"action_required_duplicate_or_cross_listing"},
                    "no_match_or_scope_gap": {"no_dataset_match_for_source_scope", "manual_scope_mapping_required"},
                }
                missing_distinctions = [
                    key
                    for key, buckets in required_distinctions.items()
                    if not (buckets & set(review_bucket_counts))
                ]
                if missing_distinctions:
                    symbol_change_campaign_evidence_gaps.append(
                        {"field": "review_bucket_counts", "missing_distinctions": missing_distinctions}
                    )
            if isinstance(workflow_queue_counts, dict):
                required_queues = {
                    "audit_already_reflected",
                    "document_no_dataset_match",
                    "review_duplicate_or_cross_listing",
                    "review_verified_rename_or_delisting",
                }
                missing_queues = sorted(required_queues - set(workflow_queue_counts))
                if missing_queues:
                    symbol_change_campaign_evidence_gaps.append(
                        {"field": "symbol_change_workflow_queue_counts", "missing_queues": missing_queues}
                    )
            bucket_priorities = evidence.get("review_bucket_priorities")
            if not isinstance(bucket_priorities, dict):
                symbol_change_campaign_evidence_gaps.append({"field": "review_bucket_priorities", "reason": "expected_object"})
            elif isinstance(review_bucket_counts, dict):
                invalid_priorities = {
                    bucket: bucket_priorities.get(bucket)
                    for bucket in review_bucket_counts
                    if bucket_priorities.get(bucket) not in SYMBOL_CHANGE_PRIORITIES
                }
                if invalid_priorities:
                    symbol_change_campaign_evidence_gaps.append(
                        {"field": "review_bucket_priorities", "invalid_priorities": invalid_priorities}
                    )
            apply_counts = evidence.get("apply_eligibility_counts")
            apply_readiness_counts = evidence.get("symbol_change_apply_readiness_counts")
            if isinstance(apply_counts, dict):
                forbidden_apply_keys = [
                    key
                    for key in apply_counts
                    if str(key).startswith("apply") or str(key) in {"auto_rename", "auto_symbol_change"}
                ]
                if forbidden_apply_keys:
                    symbol_change_campaign_evidence_gaps.append(
                        {"field": "apply_eligibility_counts", "forbidden_keys": forbidden_apply_keys}
                    )
                p1_rows = review_priority_counts.get("P1", 0) if isinstance(review_priority_counts, dict) else 0
                if apply_counts.get("requires_official_venue_confirmation", 0) != p1_rows:
                    symbol_change_campaign_evidence_gaps.append(
                        {
                            "field": "apply_eligibility_counts.requires_official_venue_confirmation",
                            "reported": apply_counts.get("requires_official_venue_confirmation", 0),
                            "p1_rows": p1_rows,
                        }
                    )
            if isinstance(apply_readiness_counts, dict):
                invalid_readiness = sorted(
                    readiness
                    for readiness in apply_readiness_counts
                    if readiness not in SYMBOL_CHANGE_APPLY_READINESS
                    or str(readiness).startswith("apply")
                )
                if invalid_readiness:
                    symbol_change_campaign_evidence_gaps.append(
                        {"field": "symbol_change_apply_readiness_counts", "invalid_readiness": invalid_readiness}
                    )
                if isinstance(symbol_rows, int) and sum(apply_readiness_counts.values()) != symbol_rows:
                    symbol_change_campaign_evidence_gaps.append(
                        {
                            "field": "symbol_change_apply_readiness_counts",
                            "reported_total": sum(apply_readiness_counts.values()),
                            "symbol_change_review_rows": symbol_rows,
                        }
                    )
            if isinstance(time_sensitive_apply_readiness_counts, dict):
                invalid_time_sensitive_readiness = sorted(
                    readiness
                    for readiness in time_sensitive_apply_readiness_counts
                    if readiness not in SYMBOL_CHANGE_APPLY_READINESS
                    or str(readiness).startswith("apply")
                )
                if invalid_time_sensitive_readiness:
                    symbol_change_campaign_evidence_gaps.append(
                        {
                            "field": "time_sensitive_apply_readiness_counts",
                            "invalid_readiness": invalid_time_sensitive_readiness,
                        }
                    )
            exchange_scope_counts = evidence.get("exchange_scope_status_counts")
            known_deltas = delta_evidence.get("known_deltas") if isinstance(delta_evidence, dict) else {}
            if not isinstance(known_deltas, dict):
                known_deltas = {}
            missing_delta_evidence_keys = [
                key for key in SYMBOL_CHANGE_CAMPAIGN_DELTA_EVIDENCE_KEYS if key not in known_deltas
            ]
            if missing_delta_evidence_keys:
                symbol_change_campaign_evidence_gaps.append(
                    {"field": "delta_evidence.known_deltas", "missing_keys": missing_delta_evidence_keys}
                )
            if isinstance(exchange_scope_counts, dict):
                outside_scope_rows = exchange_scope_counts.get("global_symbol_collision_outside_source_scope", 0)
                if known_deltas.get("source_scope_outside_collision_rows") != outside_scope_rows:
                    symbol_change_campaign_evidence_gaps.append(
                        {
                            "field": "delta_evidence.source_scope_outside_collision_rows",
                            "reported": known_deltas.get("source_scope_outside_collision_rows"),
                            "expected": outside_scope_rows,
                        }
                    )
            for delta_key in ("verified_rename_delta", "duplicate_resolution_delta"):
                if known_deltas.get(delta_key) != 0:
                    symbol_change_campaign_evidence_gaps.append(
                        {"field": f"delta_evidence.{delta_key}", "reported": known_deltas.get(delta_key), "expected": 0}
                    )
            warn_quarantine_delta = known_deltas.get("warn_quarantine_delta")
            global_acceptance_deltas = matrix.get("global_acceptance_deltas", {})
            expected_warn_quarantine_delta = {
                "warn_delta": global_acceptance_deltas.get("warn_delta", {}).get("delta", 0)
                if isinstance(global_acceptance_deltas, dict)
                else 0,
                "quarantine_delta": global_acceptance_deltas.get("quarantine_delta", {}).get("delta", 0)
                if isinstance(global_acceptance_deltas, dict)
                else 0,
            }
            if warn_quarantine_delta != expected_warn_quarantine_delta:
                symbol_change_campaign_evidence_gaps.append(
                    {
                        "field": "delta_evidence.warn_quarantine_delta",
                        "reported": warn_quarantine_delta,
                        "expected": expected_warn_quarantine_delta,
                    }
                )
            if isinstance(evidence.get("time_sensitive_review_rows"), int) and isinstance(recency_counts, dict):
                recent_rows = recency_counts.get("recent_7d", 0) + recency_counts.get("recent_30d", 0)
                p1_recent_rows = 0
                if isinstance(priority_recency_counts, dict):
                    p1_recent_rows = priority_recency_counts.get("P1:recent_7d", 0) + priority_recency_counts.get("P1:recent_30d", 0)
                if not (p1_recent_rows <= evidence.get("time_sensitive_review_rows") <= recent_rows):
                    symbol_change_campaign_evidence_gaps.append(
                        {
                            "field": "time_sensitive_review_rows",
                            "reported": evidence.get("time_sensitive_review_rows"),
                            "expected_min": p1_recent_rows,
                            "expected_max": recent_rows,
                        }
                    )
        if campaign_key == "ohlcv":
            evidence = campaign.get("evidence", {})
            delta_evidence = campaign.get("delta_evidence", {})
            if not isinstance(evidence, dict):
                ohlcv_campaign_evidence_gaps.append({"field": "evidence", "reason": "expected_object"})
                evidence = {}
            missing_evidence_keys = [key for key in OHLCV_CAMPAIGN_EVIDENCE_KEYS if key not in evidence]
            if missing_evidence_keys:
                ohlcv_campaign_evidence_gaps.append({"field": "evidence", "missing_keys": missing_evidence_keys})
            if (
                not isinstance(evidence.get("ohlcv_rows"), int)
                or isinstance(evidence.get("ohlcv_rows"), bool)
                or evidence.get("ohlcv_rows", -1) < 0
            ):
                ohlcv_campaign_evidence_gaps.append({"field": "ohlcv_rows", "reason": "expected_nonnegative_integer"})
            for key in ("selected_sample_rows", "checked_sample_rows", "not_checked_sample_rows"):
                if (
                    not isinstance(evidence.get(key), int)
                    or isinstance(evidence.get(key), bool)
                    or evidence.get(key, -1) < 0
                ):
                    ohlcv_campaign_evidence_gaps.append({"field": key, "reason": "expected_nonnegative_integer"})
            sampling_coverage = evidence.get("sampling_coverage")
            if not isinstance(sampling_coverage, dict):
                ohlcv_campaign_evidence_gaps.append({"field": "sampling_coverage", "reason": "expected_object"})
                sampling_coverage = {}
            for counter_key in (
                "status_counts",
                "issue_counts",
                "selection_bucket_counts",
                "review_bucket_counts",
                "review_priority_counts",
                "plausibility_use_counts",
                "canonical_data_change_authorization_counts",
                "verification_evidence_required_counts",
                "source_gap_class_counts",
            ):
                counter = evidence.get(counter_key)
                if not isinstance(counter, dict) or any(not isinstance(value, int) or value < 0 for value in counter.values()):
                    ohlcv_campaign_evidence_gaps.append({"field": counter_key, "reason": "expected_counter"})
            selection_bucket_exchange_counts = evidence.get("selection_bucket_exchange_counts")
            if not isinstance(selection_bucket_exchange_counts, dict) or any(
                not isinstance(exchange_counts, dict)
                or any(not isinstance(value, int) or value < 0 for value in exchange_counts.values())
                for exchange_counts in (
                    selection_bucket_exchange_counts.values()
                    if isinstance(selection_bucket_exchange_counts, dict)
                    else []
                )
            ):
                ohlcv_campaign_evidence_gaps.append(
                    {"field": "selection_bucket_exchange_counts", "reason": "expected_nested_counter"}
                )
            selection_bucket_status_counts = evidence.get("selection_bucket_status_counts")
            if not isinstance(selection_bucket_status_counts, dict) or any(
                not isinstance(status_counts_for_bucket, dict)
                or any(not isinstance(value, int) or value < 0 for value in status_counts_for_bucket.values())
                for status_counts_for_bucket in (
                    selection_bucket_status_counts.values()
                    if isinstance(selection_bucket_status_counts, dict)
                    else []
                )
            ):
                ohlcv_campaign_evidence_gaps.append(
                    {"field": "selection_bucket_status_counts", "reason": "expected_nested_counter"}
                )
            review_bucket_selection_bucket_counts = evidence.get("review_bucket_selection_bucket_counts")
            if not isinstance(review_bucket_selection_bucket_counts, dict) or any(
                not isinstance(selection_counts, dict)
                or any(not isinstance(value, int) or value < 0 for value in selection_counts.values())
                for selection_counts in (
                    review_bucket_selection_bucket_counts.values()
                    if isinstance(review_bucket_selection_bucket_counts, dict)
                    else []
                )
            ):
                ohlcv_campaign_evidence_gaps.append(
                    {"field": "review_bucket_selection_bucket_counts", "reason": "expected_nested_counter"}
                )
            review_bucket_exchange_counts = evidence.get("review_bucket_exchange_counts")
            if not isinstance(review_bucket_exchange_counts, dict) or any(
                not isinstance(exchange_counts, dict)
                or any(not isinstance(value, int) or value < 0 for value in exchange_counts.values())
                for exchange_counts in (
                    review_bucket_exchange_counts.values()
                    if isinstance(review_bucket_exchange_counts, dict)
                    else []
                )
            ):
                ohlcv_campaign_evidence_gaps.append(
                    {"field": "review_bucket_exchange_counts", "reason": "expected_nested_counter"}
                )
            review_bucket_sampling_strategy_counts = evidence.get("review_bucket_sampling_strategy_counts")
            review_bucket_sampling_readiness_counts = evidence.get("review_bucket_sampling_readiness_counts")
            if not isinstance(review_bucket_sampling_strategy_counts, dict) or any(
                not isinstance(strategy_counts, dict)
                or any(not isinstance(value, int) or value < 0 for value in strategy_counts.values())
                for strategy_counts in (
                    review_bucket_sampling_strategy_counts.values()
                    if isinstance(review_bucket_sampling_strategy_counts, dict)
                    else []
                )
            ):
                ohlcv_campaign_evidence_gaps.append(
                    {"field": "review_bucket_sampling_strategy_counts", "reason": "expected_nested_counter"}
                )
            if not isinstance(review_bucket_sampling_readiness_counts, dict) or any(
                not isinstance(readiness_counts, dict)
                or any(not isinstance(value, int) or value < 0 for value in readiness_counts.values())
                for readiness_counts in (
                    review_bucket_sampling_readiness_counts.values()
                    if isinstance(review_bucket_sampling_readiness_counts, dict)
                    else []
                )
            ):
                ohlcv_campaign_evidence_gaps.append(
                    {"field": "review_bucket_sampling_readiness_counts", "reason": "expected_nested_counter"}
                )
            ohlcv_rows = evidence.get("ohlcv_rows")
            if isinstance(ohlcv_rows, int):
                for counter_key in (
                    "status_counts",
                    "selection_bucket_counts",
                    "review_bucket_counts",
                    "review_priority_counts",
                    "plausibility_use_counts",
                    "canonical_data_change_authorization_counts",
                    "verification_evidence_required_counts",
                ):
                    counter = evidence.get(counter_key)
                    if isinstance(counter, dict) and sum(counter.values()) != ohlcv_rows:
                        ohlcv_campaign_evidence_gaps.append(
                            {
                                "field": counter_key,
                                "reported_total": sum(counter.values()),
                                "ohlcv_rows": ohlcv_rows,
                            }
                        )
            if isinstance(ohlcv_rows, int) and isinstance(sampling_coverage, dict):
                expected_pairs = {
                    "selected_rows": evidence.get("selected_sample_rows"),
                    "report_rows": ohlcv_rows,
                    "checked_rows": evidence.get("checked_sample_rows"),
                    "not_checked_rows": evidence.get("not_checked_sample_rows"),
                }
                for coverage_key, expected in expected_pairs.items():
                    if expected is not None and sampling_coverage.get(coverage_key) != expected:
                        ohlcv_campaign_evidence_gaps.append(
                            {
                                "field": f"sampling_coverage.{coverage_key}",
                                "reported": sampling_coverage.get(coverage_key),
                                "expected": expected,
                            }
                        )
                if (
                    isinstance(evidence.get("checked_sample_rows"), int)
                    and isinstance(evidence.get("not_checked_sample_rows"), int)
                    and evidence["checked_sample_rows"] + evidence["not_checked_sample_rows"] != ohlcv_rows
                ):
                    ohlcv_campaign_evidence_gaps.append(
                        {
                            "field": "checked_plus_not_checked_sample_rows",
                            "reported": evidence["checked_sample_rows"] + evidence["not_checked_sample_rows"],
                            "expected": ohlcv_rows,
                        }
                    )
            selection_counts = evidence.get("selection_bucket_counts")
            if isinstance(selection_counts, dict):
                missing_sampling_buckets = [
                    bucket
                    for bucket in ("entry_quality_warn", "source_gap:", "large_exchange:")
                    if not any(str(key).startswith(bucket) for key in selection_counts)
                ]
                if missing_sampling_buckets:
                    ohlcv_campaign_evidence_gaps.append(
                        {"field": "selection_bucket_counts", "missing_sampling_buckets": missing_sampling_buckets}
                    )
                if isinstance(selection_bucket_exchange_counts, dict):
                    for bucket, count in selection_counts.items():
                        exchange_counts = selection_bucket_exchange_counts.get(bucket)
                        if not isinstance(exchange_counts, dict) or sum(exchange_counts.values()) != count:
                            ohlcv_campaign_evidence_gaps.append(
                                {
                                    "field": f"selection_bucket_exchange_counts.{bucket}",
                                    "reported_total": sum(exchange_counts.values())
                                    if isinstance(exchange_counts, dict)
                                    else None,
                                    "expected": count,
                                }
                            )
                if isinstance(selection_bucket_status_counts, dict):
                    for bucket, count in selection_counts.items():
                        status_counts_for_bucket = selection_bucket_status_counts.get(bucket)
                        if not isinstance(status_counts_for_bucket, dict) or sum(status_counts_for_bucket.values()) != count:
                            ohlcv_campaign_evidence_gaps.append(
                                {
                                    "field": f"selection_bucket_status_counts.{bucket}",
                                    "reported_total": sum(status_counts_for_bucket.values())
                                    if isinstance(status_counts_for_bucket, dict)
                                    else None,
                                    "expected": count,
                                }
                            )
                        elif any(status not in OHLCV_PLAUSIBILITY_STATUSES for status in status_counts_for_bucket):
                            ohlcv_campaign_evidence_gaps.append(
                                {
                                    "field": f"selection_bucket_status_counts.{bucket}",
                                    "invalid_statuses": sorted(
                                        status
                                        for status in status_counts_for_bucket
                                        if status not in OHLCV_PLAUSIBILITY_STATUSES
                                    ),
                                }
                            )
            review_bucket_counts = evidence.get("review_bucket_counts")
            review_bucket_exchange_counts = evidence.get("review_bucket_exchange_counts")
            review_bucket_sampling_strategy_counts = evidence.get("review_bucket_sampling_strategy_counts")
            if isinstance(review_bucket_counts, dict) and isinstance(review_bucket_selection_bucket_counts, dict):
                for bucket, count in review_bucket_counts.items():
                    selection_counts_for_bucket = review_bucket_selection_bucket_counts.get(bucket)
                    if not isinstance(selection_counts_for_bucket, dict) or sum(selection_counts_for_bucket.values()) != count:
                        ohlcv_campaign_evidence_gaps.append(
                            {
                                "field": f"review_bucket_selection_bucket_counts.{bucket}",
                                "reported_total": sum(selection_counts_for_bucket.values())
                                if isinstance(selection_counts_for_bucket, dict)
                                else None,
                                "expected": count,
                            }
                        )
                known_selection_buckets = set(selection_counts) if isinstance(selection_counts, dict) else set()
                for bucket, selection_counts_for_bucket in review_bucket_selection_bucket_counts.items():
                    if not isinstance(selection_counts_for_bucket, dict):
                        continue
                    unknown_selection_buckets = sorted(set(selection_counts_for_bucket) - known_selection_buckets)
                    if unknown_selection_buckets:
                        ohlcv_campaign_evidence_gaps.append(
                            {
                                "field": f"review_bucket_selection_bucket_counts.{bucket}",
                                "unknown_selection_buckets": unknown_selection_buckets,
                            }
                        )
            if isinstance(review_bucket_counts, dict) and isinstance(review_bucket_sampling_strategy_counts, dict):
                for bucket, count in review_bucket_counts.items():
                    strategy_counts = review_bucket_sampling_strategy_counts.get(bucket)
                    if not isinstance(strategy_counts, dict) or sum(strategy_counts.values()) != count:
                        ohlcv_campaign_evidence_gaps.append(
                            {
                                "field": f"review_bucket_sampling_strategy_counts.{bucket}",
                                "reported_total": sum(strategy_counts.values())
                                if isinstance(strategy_counts, dict)
                                else None,
                                "expected": count,
                            }
                        )
            if isinstance(review_bucket_counts, dict) and isinstance(review_bucket_sampling_readiness_counts, dict):
                for bucket, count in review_bucket_counts.items():
                    readiness_counts = review_bucket_sampling_readiness_counts.get(bucket)
                    if not isinstance(readiness_counts, dict) or sum(readiness_counts.values()) != count:
                        ohlcv_campaign_evidence_gaps.append(
                            {
                                "field": f"review_bucket_sampling_readiness_counts.{bucket}",
                                "reported_total": sum(readiness_counts.values())
                                if isinstance(readiness_counts, dict)
                                else None,
                                "expected": count,
                            }
                        )
                    elif any(readiness not in OHLCV_SAMPLING_READINESS_STATUSES for readiness in readiness_counts):
                        ohlcv_campaign_evidence_gaps.append(
                            {
                                "field": f"review_bucket_sampling_readiness_counts.{bucket}",
                                "unexpected_readiness_statuses": sorted(
                                    readiness
                                    for readiness in readiness_counts
                                    if readiness not in OHLCV_SAMPLING_READINESS_STATUSES
                                ),
                            }
                        )
                    elif any(strategy not in OHLCV_SAMPLING_STRATEGIES for strategy in strategy_counts):
                        ohlcv_campaign_evidence_gaps.append(
                            {
                                "field": f"review_bucket_sampling_strategy_counts.{bucket}",
                                "unexpected_strategies": sorted(
                                    strategy for strategy in strategy_counts if strategy not in OHLCV_SAMPLING_STRATEGIES
                                ),
                            }
                        )
            if isinstance(review_bucket_counts, dict) and isinstance(review_bucket_exchange_counts, dict):
                for bucket, count in review_bucket_counts.items():
                    exchange_counts_for_bucket = review_bucket_exchange_counts.get(bucket)
                    if not isinstance(exchange_counts_for_bucket, dict) or sum(exchange_counts_for_bucket.values()) != count:
                        ohlcv_campaign_evidence_gaps.append(
                            {
                                "field": f"review_bucket_exchange_counts.{bucket}",
                                "reported_total": sum(exchange_counts_for_bucket.values())
                                if isinstance(exchange_counts_for_bucket, dict)
                                else None,
                                "expected": count,
                            }
                        )
            if isinstance(selection_counts, dict):
                source_gap_selected = sum(value for key, value in selection_counts.items() if str(key).startswith("source_gap:"))
                source_gap_classes = evidence.get("source_gap_class_counts")
                if isinstance(source_gap_classes, dict) and sum(source_gap_classes.values()) != source_gap_selected:
                    ohlcv_campaign_evidence_gaps.append(
                        {
                            "field": "source_gap_class_counts",
                            "reported_total": sum(source_gap_classes.values()),
                            "source_gap_selection_rows": source_gap_selected,
                        }
                    )
            plausibility_use_counts = evidence.get("plausibility_use_counts")
            if isinstance(plausibility_use_counts, dict):
                forbidden_uses = [
                    key
                    for key in plausibility_use_counts
                    if any(marker in str(key).lower() for marker in OHLCV_FORBIDDEN_AUTOMATIC_ACTION_MARKERS)
                ]
                if forbidden_uses:
                    ohlcv_campaign_evidence_gaps.append(
                        {"field": "plausibility_use_counts", "forbidden_keys": forbidden_uses}
                    )
            authorization_counts = evidence.get("canonical_data_change_authorization_counts")
            if isinstance(authorization_counts, dict):
                invalid_authorization = [
                    key
                    for key in authorization_counts
                    if key not in OHLCV_CANONICAL_AUTHORIZATION_STATUSES
                    or any(marker in str(key).lower() for marker in OHLCV_FORBIDDEN_AUTOMATIC_ACTION_MARKERS)
                ]
                if invalid_authorization:
                    ohlcv_campaign_evidence_gaps.append(
                        {
                            "field": "canonical_data_change_authorization_counts",
                            "invalid_authorization": invalid_authorization,
                        }
                    )
                if isinstance(ohlcv_rows, int) and sum(authorization_counts.values()) != ohlcv_rows:
                    ohlcv_campaign_evidence_gaps.append(
                        {
                            "field": "canonical_data_change_authorization_counts",
                            "reported_total": sum(authorization_counts.values()),
                            "ohlcv_rows": ohlcv_rows,
                        }
                    )
            verification_counts = evidence.get("verification_evidence_required_counts")
            if isinstance(verification_counts, dict):
                allowed_auto_change_count = sum(
                    count
                    for bucket, count in (review_bucket_counts or {}).items()
                    if bucket == "checked_plausible_sample"
                ) if isinstance(review_bucket_counts, dict) else 0
                forbidden_evidence = [
                    key
                    for key in verification_counts
                    if (
                        str(key) in OHLCV_ALLOWED_AUTO_CHANGE_EVIDENCE
                        and verification_counts.get(key, 0) > allowed_auto_change_count
                    )
                    or any(marker in str(key).lower() for marker in OHLCV_FORBIDDEN_AUTOMATIC_ACTION_MARKERS)
                ]
                if forbidden_evidence:
                    ohlcv_campaign_evidence_gaps.append(
                        {"field": "verification_evidence_required_counts", "forbidden_keys": forbidden_evidence}
                    )
            top_flagged = evidence.get("top_flagged_exchanges")
            if (
                not isinstance(top_flagged, list)
                or any(
                    not isinstance(row, dict)
                    or not row.get("exchange")
                    or not is_nonnegative_number(row.get("not_checked"))
                    for row in top_flagged
                )
            ):
                ohlcv_campaign_evidence_gaps.append({"field": "top_flagged_exchanges", "reason": "expected_ranked_exchange_rows"})
            top_sampling_batches = evidence.get("top_ohlcv_sampling_batches")
            if (
                not isinstance(top_sampling_batches, list)
                or not top_sampling_batches
                or any(
                    not isinstance(batch, dict)
                    or not batch.get("review_bucket")
                    or not batch.get("selection_bucket")
                    or not batch.get("exchange")
                    or batch.get("plausibility_status") not in OHLCV_PLAUSIBILITY_STATUSES
                    or batch.get("review_priority") not in OHLCV_REVIEW_PRIORITIES
                    or not isinstance(batch.get("rows"), int)
                    or batch.get("rows", 0) <= 0
                    or batch.get("sampling_strategy") not in OHLCV_SAMPLING_STRATEGIES
                    or not batch.get("evidence_required")
                    or not batch.get("recommended_next_source")
                    or not batch.get("source_gate")
                    for batch in top_sampling_batches
                )
            ):
                ohlcv_campaign_evidence_gaps.append(
                    {"field": "top_ohlcv_sampling_batches", "reason": "expected_ranked_sampling_strategy_rows"}
                )
            known_deltas = delta_evidence.get("known_deltas") if isinstance(delta_evidence, dict) else {}
            if not isinstance(known_deltas, dict):
                known_deltas = {}
            missing_delta_evidence_keys = [
                key for key in OHLCV_CAMPAIGN_DELTA_EVIDENCE_KEYS if key not in known_deltas
            ]
            if missing_delta_evidence_keys:
                ohlcv_campaign_evidence_gaps.append(
                    {"field": "delta_evidence.known_deltas", "missing_keys": missing_delta_evidence_keys}
                )
            selected_sample_rows = evidence.get("selected_sample_rows")
            if isinstance(selected_sample_rows, int) and known_deltas.get("selected_sample_rows") != selected_sample_rows:
                ohlcv_campaign_evidence_gaps.append(
                    {
                        "field": "delta_evidence.selected_sample_rows",
                        "reported": known_deltas.get("selected_sample_rows"),
                        "expected": selected_sample_rows,
                    }
                )
            checked_sample_rows = evidence.get("checked_sample_rows")
            if isinstance(checked_sample_rows, int) and known_deltas.get("checked_sample_rows") != checked_sample_rows:
                ohlcv_campaign_evidence_gaps.append(
                    {
                        "field": "delta_evidence.checked_sample_rows",
                        "reported": known_deltas.get("checked_sample_rows"),
                        "expected": checked_sample_rows,
                    }
                )
            if isinstance(sampling_coverage, dict):
                signal_rows = sampling_coverage.get("warn_or_source_gap_signal_rows")
                if (
                    isinstance(signal_rows, int)
                    and known_deltas.get("warn_or_source_gap_signal_rows") != signal_rows
                ):
                    ohlcv_campaign_evidence_gaps.append(
                        {
                            "field": "delta_evidence.warn_or_source_gap_signal_rows",
                            "reported": known_deltas.get("warn_or_source_gap_signal_rows"),
                            "expected": signal_rows,
                        }
                    )
        if campaign_key == "freshness":
            evidence = campaign.get("evidence", {})
            delta_evidence = campaign.get("delta_evidence", {})
            if not isinstance(evidence, dict):
                freshness_campaign_evidence_gaps.append({"field": "evidence", "reason": "expected_object"})
                evidence = {}
            missing_evidence_keys = [key for key in FRESHNESS_CAMPAIGN_EVIDENCE_KEYS if key not in evidence]
            if missing_evidence_keys:
                freshness_campaign_evidence_gaps.append({"field": "evidence", "missing_keys": missing_evidence_keys})
            for key in ("source_gap_rows", "coverage_freshness_keys", "old_official_exchange_directory_count", "financialdata_supplement_rows"):
                if not isinstance(evidence.get(key), int) or isinstance(evidence.get(key), bool) or evidence.get(key, -1) < 0:
                    freshness_campaign_evidence_gaps.append({"field": key, "reason": "expected_nonnegative_integer"})
            for counter_key in (
                "source_freshness_status_totals",
                "freshness_snapshot_age_bucket_totals",
                "source_age_bucket_totals",
                "source_refresh_priority_totals",
                "source_refresh_queue_totals",
                "source_refresh_action_totals",
                "source_gap_class_totals",
                "financialdata_apply_eligibility_counts",
                "financialdata_verification_evidence_required_counts",
            ):
                counter = evidence.get(counter_key)
                if not isinstance(counter, dict) or any(not isinstance(value, int) or value < 0 for value in counter.values()):
                    freshness_campaign_evidence_gaps.append({"field": counter_key, "reason": "expected_counter"})
            queue_scope_totals = evidence.get("source_refresh_queue_scope_totals")
            if not isinstance(queue_scope_totals, dict) or any(
                not isinstance(scope_totals, dict)
                or any(not isinstance(value, int) or value < 0 for value in scope_totals.values())
                for scope_totals in (queue_scope_totals.values() if isinstance(queue_scope_totals, dict) else [])
            ):
                freshness_campaign_evidence_gaps.append(
                    {"field": "source_refresh_queue_scope_totals", "reason": "expected_nested_counter"}
                )
            queue_mode_totals = evidence.get("source_refresh_queue_mode_totals")
            if not isinstance(queue_mode_totals, dict) or any(
                not isinstance(mode_totals, dict)
                or any(not isinstance(value, int) or value < 0 for value in mode_totals.values())
                for mode_totals in (queue_mode_totals.values() if isinstance(queue_mode_totals, dict) else [])
            ):
                freshness_campaign_evidence_gaps.append(
                    {"field": "source_refresh_queue_mode_totals", "reason": "expected_nested_counter"}
                )
            queue_priority_totals = evidence.get("source_refresh_queue_priority_totals")
            if not isinstance(queue_priority_totals, dict) or any(
                not isinstance(priority_counts, dict)
                or any(not isinstance(value, int) or value < 0 for value in priority_counts.values())
                for priority_counts in (
                    queue_priority_totals.values() if isinstance(queue_priority_totals, dict) else []
                )
            ):
                freshness_campaign_evidence_gaps.append(
                    {"field": "source_refresh_queue_priority_totals", "reason": "expected_nested_counter"}
                )
            queue_age_bucket_totals = evidence.get("source_refresh_queue_age_bucket_totals")
            if not isinstance(queue_age_bucket_totals, dict) or any(
                not isinstance(age_bucket_counts, dict)
                or any(not isinstance(value, int) or value < 0 for value in age_bucket_counts.values())
                for age_bucket_counts in (
                    queue_age_bucket_totals.values() if isinstance(queue_age_bucket_totals, dict) else []
                )
            ):
                freshness_campaign_evidence_gaps.append(
                    {"field": "source_refresh_queue_age_bucket_totals", "reason": "expected_nested_counter"}
                )
            queue_strategy_totals = evidence.get("source_refresh_queue_review_strategy_totals")
            if not isinstance(queue_strategy_totals, dict) or any(
                not isinstance(strategy_counts, dict)
                or any(not isinstance(value, int) or value < 0 for value in strategy_counts.values())
                for strategy_counts in (
                    queue_strategy_totals.values() if isinstance(queue_strategy_totals, dict) else []
                )
            ):
                freshness_campaign_evidence_gaps.append(
                    {"field": "source_refresh_queue_review_strategy_totals", "reason": "expected_nested_counter"}
                )
            queue_evidence_totals = evidence.get("source_refresh_queue_evidence_required_totals")
            if not isinstance(queue_evidence_totals, dict) or any(
                not isinstance(evidence_counts, dict)
                or any(not isinstance(value, int) or value < 0 for value in evidence_counts.values())
                for evidence_counts in (
                    queue_evidence_totals.values() if isinstance(queue_evidence_totals, dict) else []
                )
            ):
                freshness_campaign_evidence_gaps.append(
                    {"field": "source_refresh_queue_evidence_required_totals", "reason": "expected_nested_counter"}
                )
            source_status = evidence.get("source_freshness_status_totals")
            age_bucket_totals = evidence.get("source_age_bucket_totals")
            priority_totals = evidence.get("source_refresh_priority_totals")
            queue_totals = evidence.get("source_refresh_queue_totals")
            action_totals = evidence.get("source_refresh_action_totals")
            if isinstance(source_status, dict) and isinstance(age_bucket_totals, dict) and sum(source_status.values()) != sum(age_bucket_totals.values()):
                freshness_campaign_evidence_gaps.append(
                    {
                        "field": "source_age_bucket_totals",
                        "reported_total": sum(age_bucket_totals.values()),
                        "source_freshness_rows": sum(source_status.values()),
                    }
                )
            if isinstance(source_status, dict) and isinstance(priority_totals, dict) and sum(source_status.values()) != sum(priority_totals.values()):
                freshness_campaign_evidence_gaps.append(
                    {
                        "field": "source_refresh_priority_totals",
                        "reported_total": sum(priority_totals.values()),
                        "source_freshness_rows": sum(source_status.values()),
                    }
                )
            if isinstance(source_status, dict) and isinstance(action_totals, dict) and sum(source_status.values()) != sum(action_totals.values()):
                freshness_campaign_evidence_gaps.append(
                    {
                        "field": "source_refresh_action_totals",
                        "reported_total": sum(action_totals.values()),
                        "source_freshness_rows": sum(source_status.values()),
                    }
                )
            if isinstance(source_status, dict) and isinstance(queue_totals, dict) and sum(source_status.values()) != sum(queue_totals.values()):
                freshness_campaign_evidence_gaps.append(
                    {
                        "field": "source_refresh_queue_totals",
                        "reported_total": sum(queue_totals.values()),
                        "source_freshness_rows": sum(source_status.values()),
                    }
                )
            if isinstance(queue_totals, dict) and isinstance(queue_scope_totals, dict):
                for queue, count in queue_totals.items():
                    scope_counts = queue_scope_totals.get(queue)
                    if not isinstance(scope_counts, dict) or sum(scope_counts.values()) != count:
                        freshness_campaign_evidence_gaps.append(
                            {
                                "field": f"source_refresh_queue_scope_totals.{queue}",
                                "reported_total": sum(scope_counts.values())
                                if isinstance(scope_counts, dict)
                                else None,
                                "expected": count,
                            }
                        )
            if isinstance(queue_totals, dict) and isinstance(queue_mode_totals, dict):
                for queue, count in queue_totals.items():
                    mode_counts = queue_mode_totals.get(queue)
                    if not isinstance(mode_counts, dict) or sum(mode_counts.values()) != count:
                        freshness_campaign_evidence_gaps.append(
                            {
                                "field": f"source_refresh_queue_mode_totals.{queue}",
                                "reported_total": sum(mode_counts.values())
                                if isinstance(mode_counts, dict)
                                else None,
                                "expected": count,
                            }
                        )
            if isinstance(queue_totals, dict) and isinstance(queue_priority_totals, dict):
                for queue, count in queue_totals.items():
                    priority_counts = queue_priority_totals.get(queue)
                    if not isinstance(priority_counts, dict) or sum(priority_counts.values()) != count:
                        freshness_campaign_evidence_gaps.append(
                            {
                                "field": f"source_refresh_queue_priority_totals.{queue}",
                                "reported_total": sum(priority_counts.values())
                                if isinstance(priority_counts, dict)
                                else None,
                                "expected": count,
                            }
                        )
            if isinstance(queue_totals, dict) and isinstance(queue_age_bucket_totals, dict):
                for queue, count in queue_totals.items():
                    age_bucket_counts = queue_age_bucket_totals.get(queue)
                    if not isinstance(age_bucket_counts, dict) or sum(age_bucket_counts.values()) != count:
                        freshness_campaign_evidence_gaps.append(
                            {
                                "field": f"source_refresh_queue_age_bucket_totals.{queue}",
                                "reported_total": sum(age_bucket_counts.values())
                                if isinstance(age_bucket_counts, dict)
                                else None,
                                "expected": count,
                            }
                        )
            if isinstance(queue_totals, dict) and isinstance(queue_strategy_totals, dict):
                for queue, count in queue_totals.items():
                    strategy_counts = queue_strategy_totals.get(queue)
                    if not isinstance(strategy_counts, dict) or sum(strategy_counts.values()) != count:
                        freshness_campaign_evidence_gaps.append(
                            {
                                "field": f"source_refresh_queue_review_strategy_totals.{queue}",
                                "reported_total": sum(strategy_counts.values())
                                if isinstance(strategy_counts, dict)
                                else None,
                                "expected": count,
                            }
                        )
                    elif any(strategy not in SOURCE_REFRESH_REVIEW_STRATEGIES for strategy in strategy_counts):
                        freshness_campaign_evidence_gaps.append(
                            {
                                "field": f"source_refresh_queue_review_strategy_totals.{queue}",
                                "unexpected_strategies": sorted(
                                    strategy for strategy in strategy_counts if strategy not in SOURCE_REFRESH_REVIEW_STRATEGIES
                                ),
                            }
                        )
            if isinstance(queue_totals, dict) and isinstance(queue_evidence_totals, dict):
                for queue, count in queue_totals.items():
                    evidence_counts = queue_evidence_totals.get(queue)
                    if not isinstance(evidence_counts, dict) or sum(evidence_counts.values()) != count:
                        freshness_campaign_evidence_gaps.append(
                            {
                                "field": f"source_refresh_queue_evidence_required_totals.{queue}",
                                "reported_total": sum(evidence_counts.values())
                                if isinstance(evidence_counts, dict)
                                else None,
                                "expected": count,
                            }
                        )
                    else:
                        _expected_strategy, expected_evidence = source_refresh_strategy_for(str(queue))
                        if set(evidence_counts) != {expected_evidence}:
                            freshness_campaign_evidence_gaps.append(
                                {
                                    "field": f"source_refresh_queue_evidence_required_totals.{queue}",
                                    "unexpected_evidence": sorted(set(evidence_counts) - {expected_evidence}),
                                }
                            )
            if isinstance(queue_totals, dict) and isinstance(action_totals, dict):
                expected_queue_actions = {
                    "fresh_no_refresh_needed": "no_refresh_needed",
                    "refresh_official_exchange_directory_before_identity_or_collision_work": "refresh_official_exchange_directory_before_identity_or_collision_work",
                    "refresh_official_subset_before_gap_enrichment": "refresh_official_subset_before_gap_enrichment",
                    "restore_or_replace_unavailable_source_before_data_fill": "restore_or_replace_unavailable_source_before_data_fill",
                    "capture_source_generated_at_before_refresh_decision": "capture_source_generated_at_before_refresh_decision",
                    "review_secondary_source_freshness_or_replace": "review_secondary_source_freshness_or_replace",
                }
                for queue, action in expected_queue_actions.items():
                    if queue_totals.get(queue, 0) != action_totals.get(action, 0):
                        freshness_campaign_evidence_gaps.append(
                            {
                                "field": f"source_refresh_queue_totals.{queue}",
                                "reported": queue_totals.get(queue, 0),
                                "expected_from_action": action_totals.get(action, 0),
                            }
                        )
            if isinstance(source_status, dict) and evidence.get("old_official_exchange_directory_count", 0) > source_status.get("old", 0) + source_status.get("stale", 0):
                freshness_campaign_evidence_gaps.append(
                    {
                        "field": "old_official_exchange_directory_count",
                        "reported": evidence.get("old_official_exchange_directory_count"),
                        "old_or_stale_sources": source_status.get("old", 0) + source_status.get("stale", 0),
                    }
                )
            top_old = evidence.get("top_old_official_exchange_directories")
            if (
                not isinstance(top_old, list)
                or any(
                    not isinstance(row, dict)
                    or not row.get("key")
                    or not row.get("provider")
                    or not is_nonnegative_number(row.get("age_hours"))
                    for row in top_old
                )
            ):
                freshness_campaign_evidence_gaps.append(
                    {"field": "top_old_official_exchange_directories", "reason": "expected_ranked_source_rows"}
                )
            freshness_snapshot = evidence.get("freshness_snapshot")
            snapshot_age_bucket_totals = evidence.get("freshness_snapshot_age_bucket_totals")
            required_snapshot_keys = {
                "entry_quality",
                "identifiers",
                "masterfiles",
                "ohlcv_plausibility",
                "source_gap_classification",
                "symbol_changes",
                "tickers",
            }
            if not isinstance(freshness_snapshot, list) or not freshness_snapshot:
                freshness_campaign_evidence_gaps.append(
                    {"field": "freshness_snapshot", "reason": "expected_dataset_source_and_symbol_change_age_rows"}
                )
            else:
                snapshot_keys = {
                    str(row.get("key"))
                    for row in freshness_snapshot
                    if isinstance(row, dict) and row.get("key")
                }
                missing_snapshot_keys = sorted(required_snapshot_keys - snapshot_keys)
                if missing_snapshot_keys:
                    freshness_campaign_evidence_gaps.append(
                        {"field": "freshness_snapshot", "missing_keys": missing_snapshot_keys}
                    )
                for index, row in enumerate(freshness_snapshot):
                    if not isinstance(row, dict):
                        freshness_campaign_evidence_gaps.append(
                            {"field": "freshness_snapshot", "row_index": index, "reason": "expected_object"}
                        )
                        continue
                    missing_keys = [
                        key
                        for key in (
                            "key",
                            "source_type",
                            "generated_at",
                            "age_hours",
                            "age_bucket",
                            "rows",
                            "recommended_next_source",
                            "source_gate",
                        )
                        if key not in row
                    ]
                    invalid_fields: list[str] = []
                    if row.get("generated_at") and not is_valid_iso_utc_timestamp(str(row.get("generated_at"))):
                        invalid_fields.append("generated_at")
                    if not is_nonnegative_number(row.get("age_hours")):
                        invalid_fields.append("age_hours")
                    if row.get("age_bucket") not in {"age_0_48h", "age_48_168h", "age_168_336h", "age_over_336h", "unknown_age"}:
                        invalid_fields.append("age_bucket")
                    if not isinstance(row.get("rows"), int) or isinstance(row.get("rows"), bool) or row.get("rows", -1) < 0:
                        invalid_fields.append("rows")
                    if not row.get("recommended_next_source"):
                        invalid_fields.append("recommended_next_source")
                    if not row.get("source_gate"):
                        invalid_fields.append("source_gate")
                    if missing_keys or invalid_fields:
                        freshness_campaign_evidence_gaps.append(
                            {
                                "field": "freshness_snapshot",
                                "row_index": index,
                                "key": row.get("key", ""),
                                "missing_keys": missing_keys,
                                "invalid_fields": invalid_fields,
                            }
                        )
                if isinstance(snapshot_age_bucket_totals, dict):
                    expected_snapshot_age_buckets: Counter[str] = Counter()
                    for row in freshness_snapshot:
                        if isinstance(row, dict):
                            expected_snapshot_age_buckets[str(row.get("age_bucket") or "unknown_age")] += 1
                    if dict(sorted(expected_snapshot_age_buckets.items())) != snapshot_age_bucket_totals:
                        freshness_campaign_evidence_gaps.append(
                            {
                                "field": "freshness_snapshot_age_bucket_totals",
                                "reported": snapshot_age_bucket_totals,
                                "expected": dict(sorted(expected_snapshot_age_buckets.items())),
                            }
                        )
            top_freshness_ages = evidence.get("top_freshness_snapshot_ages")
            if (
                not isinstance(top_freshness_ages, list)
                or not top_freshness_ages
                or any(
                    not isinstance(row, dict)
                    or not row.get("key")
                    or not row.get("source_type")
                    or row.get("age_bucket") not in {"age_0_48h", "age_48_168h", "age_168_336h", "age_over_336h", "unknown_age"}
                    or not is_nonnegative_number(row.get("age_hours"))
                    or not isinstance(row.get("rows"), int)
                    or isinstance(row.get("rows"), bool)
                    or row.get("rows", -1) < 0
                    or not row.get("recommended_next_source")
                    or not row.get("source_gate")
                    for row in top_freshness_ages
                )
            ):
                freshness_campaign_evidence_gaps.append(
                    {"field": "top_freshness_snapshot_ages", "reason": "expected_ranked_dataset_report_workflow_age_rows"}
                )
            top_refresh_batches = evidence.get("top_source_refresh_batches")
            if (
                not isinstance(top_refresh_batches, list)
                or not top_refresh_batches
                or any(
                    not isinstance(batch, dict)
                    or not batch.get("refresh_queue")
                    or not batch.get("reference_scope")
                    or not batch.get("mode")
                    or batch.get("refresh_priority") not in {"P1", "P2", "P3", "P4"}
                    or not isinstance(batch.get("source_count"), int)
                    or batch.get("source_count", 0) <= 0
                    or not isinstance(batch.get("total_rows"), int)
                    or batch.get("total_rows", -1) < 0
                    or batch.get("max_age_hours") is not None
                    and not is_nonnegative_number(batch.get("max_age_hours"))
                    or batch.get("review_strategy") not in SOURCE_REFRESH_REVIEW_STRATEGIES
                    or not batch.get("evidence_required")
                    or batch.get("evidence_required") in {"none", "ticker_match_only"}
                    or not batch.get("recommended_next_source")
                    or not batch.get("source_gate")
                    for batch in top_refresh_batches
                )
            ):
                freshness_campaign_evidence_gaps.append(
                    {"field": "top_source_refresh_batches", "reason": "expected_ranked_refresh_strategy_rows"}
                )
            source_gap_classes = evidence.get("source_gap_class_totals")
            if isinstance(source_gap_classes, dict) and isinstance(evidence.get("source_gap_rows"), int):
                if sum(source_gap_classes.values()) != evidence.get("source_gap_rows"):
                    freshness_campaign_evidence_gaps.append(
                        {
                            "field": "source_gap_class_totals",
                            "reported_total": sum(source_gap_classes.values()),
                            "source_gap_rows": evidence.get("source_gap_rows"),
                        }
                    )
            source_gap_batches = evidence.get("top_source_gap_review_batches")
            if (
                not isinstance(source_gap_batches, list)
                or not source_gap_batches
                or any(
                    not isinstance(batch, dict)
                    or batch.get("field") not in SOURCE_GAP_FIELD_VALUES
                    or not batch.get("gap_class")
                    or not batch.get("exchange")
                    or not isinstance(batch.get("rows"), int)
                    or batch.get("rows", 0) <= 0
                    or not batch.get("recommended_next_source")
                    or not batch.get("source_gate")
                    for batch in source_gap_batches
                )
            ):
                freshness_campaign_evidence_gaps.append(
                    {"field": "top_source_gap_review_batches", "reason": "expected_ranked_source_gap_review_batches"}
                )
            financialdata_apply = evidence.get("financialdata_apply_eligibility_counts")
            if isinstance(financialdata_apply, dict) and isinstance(evidence.get("financialdata_supplement_rows"), int):
                if sum(financialdata_apply.values()) <= 0 and evidence.get("financialdata_supplement_rows") > 0:
                    freshness_campaign_evidence_gaps.append(
                        {
                            "field": "financialdata_apply_eligibility_counts",
                            "reason": "expected_review_evidence_for_supplement_rows",
                            "financialdata_supplement_rows": evidence.get("financialdata_supplement_rows"),
                        }
                    )
                forbidden_apply = [
                    key
                    for key in financialdata_apply
                    if str(key).startswith("apply") or "symbol_only" in str(key) or "global_ticker_reuse" in str(key)
                ]
                if forbidden_apply:
                    freshness_campaign_evidence_gaps.append(
                        {"field": "financialdata_apply_eligibility_counts", "forbidden_keys": forbidden_apply}
                    )
            financialdata_evidence = evidence.get("financialdata_verification_evidence_required_counts")
            if isinstance(financialdata_evidence, dict) and isinstance(evidence.get("financialdata_supplement_rows"), int):
                if sum(financialdata_evidence.values()) <= 0 and evidence.get("financialdata_supplement_rows") > 0:
                    freshness_campaign_evidence_gaps.append(
                        {
                            "field": "financialdata_verification_evidence_required_counts",
                            "reason": "expected_verification_evidence_for_supplement_rows",
                            "financialdata_supplement_rows": evidence.get("financialdata_supplement_rows"),
                        }
                    )
            financialdata_batches = evidence.get("top_financialdata_supplement_review_batches")
            required_financialdata_batch_fields = {
                "review_priority",
                "financialdata_review_queue",
                "decision",
                "reason",
                "financialdata_exchange",
                "financialdata_review_scope",
                "official_source_key",
                "review_strategy",
                "apply_eligibility",
                "verification_evidence_required",
                "recommended_next_source",
                "source_gate",
            }
            if (
                not isinstance(financialdata_batches, list)
                or not financialdata_batches
                or any(
                    not isinstance(batch, dict)
                    or any(not batch.get(field) for field in required_financialdata_batch_fields)
                    or batch.get("review_priority") not in {"P1", "P2", "P3", "P4"}
                    or not isinstance(batch.get("rows"), int)
                    or batch.get("rows", 0) <= 0
                    or str(batch.get("apply_eligibility", "")).startswith("apply")
                    or "symbol_only" in str(batch.get("apply_eligibility", ""))
                    or "global_ticker_reuse" in str(batch.get("apply_eligibility", ""))
                    or batch.get("verification_evidence_required") in {"", "none", "ticker_match_only"}
                    for batch in financialdata_batches
                )
            ):
                freshness_campaign_evidence_gaps.append(
                    {
                        "field": "top_financialdata_supplement_review_batches",
                        "reason": "expected_ranked_source_gated_financialdata_review_batches",
                    }
                )
            known_deltas = delta_evidence.get("known_deltas") if isinstance(delta_evidence, dict) else {}
            if not isinstance(known_deltas, dict):
                known_deltas = {}
            missing_delta_evidence_keys = [
                key for key in FRESHNESS_CAMPAIGN_DELTA_EVIDENCE_KEYS if key not in known_deltas
            ]
            if missing_delta_evidence_keys:
                freshness_campaign_evidence_gaps.append(
                    {"field": "delta_evidence.known_deltas", "missing_keys": missing_delta_evidence_keys}
                )
            for key in FRESHNESS_CAMPAIGN_DELTA_EVIDENCE_KEYS:
                if key in evidence and known_deltas.get(key) != evidence.get(key):
                    freshness_campaign_evidence_gaps.append(
                        {"field": f"delta_evidence.{key}", "reported": known_deltas.get(key), "expected": evidence.get(key)}
                    )
        if campaign_key == "baseline":
            evidence = campaign.get("evidence", {})
            delta_evidence = campaign.get("delta_evidence", {})
            if not isinstance(evidence, dict):
                baseline_campaign_evidence_gaps.append({"field": "evidence", "reason": "expected_object"})
                evidence = {}
            missing_evidence_keys = [key for key in BASELINE_CAMPAIGN_EVIDENCE_KEYS if key not in evidence]
            if missing_evidence_keys:
                baseline_campaign_evidence_gaps.append({"field": "evidence", "missing_keys": missing_evidence_keys})
            for key in BASELINE_CAMPAIGN_EVIDENCE_KEYS:
                if not isinstance(evidence.get(key), int) or isinstance(evidence.get(key), bool) or evidence.get(key, -1) < 0:
                    baseline_campaign_evidence_gaps.append({"field": key, "reason": "expected_nonnegative_integer"})
            expected_values = {
                "baseline_global_metrics": 16,
                "baseline_campaigns": 10,
                "changed_numeric_delta_rows": 0,
            }
            for key, expected in expected_values.items():
                if evidence.get(key) != expected:
                    baseline_campaign_evidence_gaps.append(
                        {"field": key, "reported": evidence.get(key), "expected": expected}
                    )
            known_deltas = delta_evidence.get("known_deltas") if isinstance(delta_evidence, dict) else {}
            if not isinstance(known_deltas, dict):
                known_deltas = {}
            missing_delta_evidence_keys = [
                key for key in BASELINE_CAMPAIGN_DELTA_EVIDENCE_KEYS if key not in known_deltas
            ]
            if missing_delta_evidence_keys:
                baseline_campaign_evidence_gaps.append(
                    {"field": "delta_evidence.known_deltas", "missing_keys": missing_delta_evidence_keys}
                )
            baseline_generated_at = known_deltas.get("baseline_generated_at")
            if not isinstance(baseline_generated_at, str) or not is_valid_iso_utc_timestamp(baseline_generated_at):
                baseline_campaign_evidence_gaps.append(
                    {"field": "delta_evidence.baseline_generated_at", "reported": baseline_generated_at}
                )
            expected_paths = {
                "future_delta_reference": "data/reports/improvement_baseline.json",
                "current_delta_report": "data/reports/improvement_deltas.json",
            }
            for key, expected in expected_paths.items():
                if known_deltas.get(key) != expected:
                    baseline_campaign_evidence_gaps.append(
                        {"field": f"delta_evidence.{key}", "reported": known_deltas.get(key), "expected": expected}
                    )
    expected_campaigns = campaigns.get("summary", {}).get("campaigns", len(rows))
    return {
        "passed": (
            len(rows) >= 10
            and expected_campaigns == len(rows)
            and not missing_matrices
            and not missing_required_metrics
            and not missing_exchange_scopes
            and not missing_affected_rows
            and not missing_campaign_metric_deltas
            and not missing_global_acceptance_deltas
            and not missing_exchange_scope_delta_fields
            and not missing_exchange_scope_delta_totals
            and not missing_before_after_summaries
            and not missing_before_after_fields
            and not before_after_mismatches
            and not exchange_scope_delta_consistency_gaps
            and not b3_campaign_evidence_gaps
            and not otc_campaign_evidence_gaps
            and not canada_campaign_evidence_gaps
            and not asx_campaign_evidence_gaps
            and not weak_sector_campaign_evidence_gaps
            and not masterfile_collision_campaign_evidence_gaps
            and not symbol_change_campaign_evidence_gaps
            and not ohlcv_campaign_evidence_gaps
            and not freshness_campaign_evidence_gaps
            and not baseline_campaign_evidence_gaps
        ),
        "campaigns": len(rows),
        "expected_campaigns": expected_campaigns,
        "missing_matrices": missing_matrices,
        "missing_required_metrics": missing_required_metrics,
        "missing_exchange_scopes": missing_exchange_scopes,
        "missing_affected_rows": missing_affected_rows,
        "missing_campaign_metric_deltas": missing_campaign_metric_deltas,
        "missing_global_acceptance_deltas": missing_global_acceptance_deltas,
        "missing_exchange_scope_delta_fields": missing_exchange_scope_delta_fields,
        "missing_exchange_scope_delta_totals": missing_exchange_scope_delta_totals,
        "missing_before_after_summaries": missing_before_after_summaries,
        "missing_before_after_fields": missing_before_after_fields,
        "before_after_mismatches": before_after_mismatches[:20],
        "exchange_scope_delta_consistency_gaps": exchange_scope_delta_consistency_gaps[:20],
        "b3_campaign_evidence_gaps": b3_campaign_evidence_gaps[:20],
        "otc_campaign_evidence_gaps": otc_campaign_evidence_gaps[:20],
        "canada_campaign_evidence_gaps": canada_campaign_evidence_gaps[:20],
        "asx_campaign_evidence_gaps": asx_campaign_evidence_gaps[:20],
        "weak_sector_campaign_evidence_gaps": weak_sector_campaign_evidence_gaps,
        "masterfile_collision_campaign_evidence_gaps": masterfile_collision_campaign_evidence_gaps,
        "symbol_change_campaign_evidence_gaps": symbol_change_campaign_evidence_gaps,
        "ohlcv_campaign_evidence_gaps": ohlcv_campaign_evidence_gaps[:20],
        "freshness_campaign_evidence_gaps": freshness_campaign_evidence_gaps,
        "baseline_campaign_evidence_gaps": baseline_campaign_evidence_gaps[:20],
    }


def campaign_exchange_scope_text(scope: Any) -> str:
    if isinstance(scope, list):
        return "|".join(str(item) for item in scope) or "none"
    return str(scope or "none")


def count_nonzero_before_after_deltas(rows: dict[str, Any]) -> int:
    total = 0
    for row in rows.values():
        if not isinstance(row, dict):
            continue
        value = row.get("delta")
        if isinstance(value, (int, float)) and not isinstance(value, bool) and value != 0:
            total += 1
    return total


def campaign_before_after_context(summary: dict[str, Any]) -> str:
    global_before_after = summary.get("global_before_after", {})
    if not isinstance(global_before_after, dict):
        global_before_after = {}
    exchange_scope_delta_totals = summary.get("exchange_scope_delta_totals", {})
    if not isinstance(exchange_scope_delta_totals, dict):
        exchange_scope_delta_totals = {}
    nonzero_scope_deltas = sum(
        1
        for value in exchange_scope_delta_totals.values()
        if isinstance(value, (int, float)) and not isinstance(value, bool) and value != 0
    )
    warn_quarantine = summary.get("warn_quarantine_delta", {})
    if not isinstance(warn_quarantine, dict):
        warn_quarantine = {}
    warn_delta = warn_quarantine.get("warn_delta", {})
    quarantine_delta = warn_quarantine.get("quarantine_delta", {})
    if not isinstance(warn_delta, dict):
        warn_delta = {}
    if not isinstance(quarantine_delta, dict):
        quarantine_delta = {}
    return (
        f"exchange_scope={campaign_exchange_scope_text(summary.get('exchange_scope'))};"
        f"affected_artifact_rows={summary.get('affected_artifact_rows', 0)};"
        f"global_delta_count={len(global_before_after)};"
        f"nonzero_global_delta_count={count_nonzero_before_after_deltas(global_before_after)};"
        f"nonzero_exchange_scope_delta_count={nonzero_scope_deltas};"
        f"warn_delta={warn_delta.get('delta')};"
        f"quarantine_delta={quarantine_delta.get('delta')}"
    )


def campaign_context(campaign: dict[str, Any]) -> str:
    matrix = campaign.get("acceptance_matrix", {})
    if not isinstance(matrix, dict):
        matrix = {}
    return (
        f"priority={campaign.get('priority', 'none')};"
        f"campaign_key={campaign.get('campaign_key', '') or 'none'};"
        f"status={campaign.get('status', '') or 'none'};"
        f"exchange_scope={campaign_exchange_scope_text(matrix.get('exchange_scope'))}"
    )


def campaign_artifact_context(campaign: dict[str, Any]) -> str:
    artifacts = campaign.get("artifacts", [])
    if not isinstance(artifacts, list):
        artifacts = []
    artifact_rows = [artifact for artifact in artifacts if isinstance(artifact, dict)]
    primary_artifact = max(artifact_rows, key=lambda artifact: int(artifact.get("rows") or 0), default={})
    return (
        f"artifact_count={len(artifact_rows)};"
        f"affected_artifact_rows={sum(int(artifact.get('rows') or 0) for artifact in artifact_rows)};"
        f"primary_artifact={primary_artifact.get('path', '') or 'none'};"
        f"primary_artifact_rows={int(primary_artifact.get('rows') or 0)}"
    )


def campaign_delta_review_context(campaign: dict[str, Any]) -> str:
    delta = campaign.get("delta_evidence", {})
    if not isinstance(delta, dict):
        delta = {}
    missing = delta.get("missing_deltas", [])
    if not isinstance(missing, list):
        missing = []
    return (
        f"delta_status={delta.get('status', '') or 'none'};"
        f"missing_delta_count={len(missing)};"
        f"before_after_present={'true' if isinstance(campaign.get('before_after_summary'), dict) else 'false'};"
        f"next_action_present={'true' if bool(campaign.get('next_action')) else 'false'}"
    )


def campaign_review_batch_artifact_rows(campaign: dict[str, Any]) -> int:
    artifacts = campaign.get("artifacts", [])
    if not isinstance(artifacts, list):
        return 0
    return sum(
        int(artifact.get("rows") or 0)
        for artifact in artifacts
        if isinstance(artifact, dict) and int(artifact.get("rows") or 0) > 0
    )


def campaign_closure_gate(campaign: dict[str, Any]) -> str:
    campaign_key = str(campaign.get("campaign_key") or campaign.get("name") or "")
    delta = campaign.get("delta_evidence", {})
    if not isinstance(delta, dict):
        delta = {}
    missing = delta.get("missing_deltas", [])
    if not isinstance(missing, list):
        missing = []
    if campaign_key == "baseline":
        return "future_campaign_delta_comparison_required_before_release_closure"
    if missing:
        return "missing_delta_evidence_must_be_resolved_or_documented_before_campaign_closure"
    return "review_artifact_rows_must_be_processed_before_campaign_closure"


def closure_status_for_gate(closure_gate: str) -> str:
    if closure_gate == "review_artifact_rows_must_be_processed_before_campaign_closure":
        return "ready_after_review_artifact_processing"
    return "blocked_until_closure_gate_resolved"


def campaign_closure_context(campaign: dict[str, Any]) -> str:
    delta = campaign.get("delta_evidence", {})
    if not isinstance(delta, dict):
        delta = {}
    missing = delta.get("missing_deltas", [])
    if not isinstance(missing, list):
        missing = []
    closure_gate = campaign_closure_gate(campaign)
    return (
        f"closure_gate={closure_gate};"
        f"artifact_rows={campaign_review_batch_artifact_rows(campaign)};"
        f"missing_delta_count={len(missing)};"
        f"closure_status={closure_status_for_gate(closure_gate)}"
    )


def next_review_batch_closure_context(row: dict[str, Any]) -> str:
    closure_gate = str(row.get("closure_gate") or "missing_closure_gate")
    missing = row.get("missing_delta_evidence", [])
    if not isinstance(missing, list):
        missing = []
    return (
        f"closure_gate={closure_gate};"
        f"artifact_rows={int(row.get('artifact_rows') or 0)};"
        f"missing_delta_count={len(missing)};"
        f"closure_status={closure_status_for_gate(closure_gate)}"
    )


def blocker_context(blocker: dict[str, Any]) -> str:
    return (
        f"campaign_key={blocker.get('campaign_key', '') or 'none'};"
        f"blocker_type={blocker.get('blocker_type', '') or 'none'};"
        f"closure_gate={blocker.get('closure_gate', '') or 'none'};"
        f"command_mode={blocker.get('command_mode', '') or 'none'};"
        f"network_required={str(blocker.get('network_required', '')).lower()};"
        f"data_change_authorized={str(blocker.get('data_change_authorized', '')).lower()}"
    )


def next_review_workload_context(workload: dict[str, Any]) -> str:
    top_batch = workload.get("largest_batch", {})
    if not isinstance(top_batch, dict):
        top_batch = {}
    return (
        f"batches={workload.get('total_batches', 0)};"
        f"rows={workload.get('total_rows', 0)};"
        f"blocked_batches={workload.get('blocked_batches', 0)};"
        f"top_campaign={top_batch.get('campaign_key', '') or 'none'};"
        f"top_rows={top_batch.get('artifact_rows', 0)};"
        f"top_gate={top_batch.get('closure_gate', '') or 'none'}"
    )


def execution_plan_context(row: dict[str, Any]) -> str:
    return (
        f"priority={row.get('priority', 'none')};"
        f"campaign_key={row.get('campaign_key', '') or 'none'};"
        f"artifact_rows={int(row.get('artifact_rows') or 0)};"
        f"command_mode={row.get('command_mode', '') or 'none'};"
        f"network_required={str(row.get('network_required', '')).lower()};"
        f"data_change_authorized={str(row.get('data_change_authorized', '')).lower()}"
    )


def execution_ranking_reason(row: dict[str, Any]) -> str:
    if row.get("network_required") is True:
        return "objective_priority_order_with_external_source_refresh_required"
    if int(row.get("artifact_rows") or 0) <= 0:
        return "objective_priority_order_future_delta_baseline_gate"
    return "objective_priority_order_with_local_review_artifact_processing"


def execution_ranking_context(row: dict[str, Any]) -> str:
    return (
        f"execution_order={row.get('execution_order', 'none')};"
        f"priority={row.get('priority', 'none')};"
        f"campaign_key={row.get('campaign_key', '') or 'none'};"
        f"artifact_rows={int(row.get('artifact_rows') or 0)};"
        f"network_required={str(row.get('network_required', '')).lower()};"
        f"ranking_reason={row.get('ranking_reason', '') or 'none'}"
    )


def command_script_paths(command: str) -> list[str]:
    return COMMAND_SCRIPT_PATTERN.findall(command)


def command_readiness_context(row: dict[str, Any]) -> str:
    scripts = row.get("command_scripts", [])
    if not isinstance(scripts, list):
        scripts = []
    missing = row.get("missing_command_scripts", [])
    if not isinstance(missing, list):
        missing = []
    return (
        f"campaign_key={row.get('campaign_key', '') or 'none'};"
        f"script_count={len(scripts)};"
        f"missing_script_count={len(missing)};"
        f"all_scripts_exist={str(len(missing) == 0).lower()}"
    )


def command_mutation_risk(scripts: list[str]) -> str:
    risky_markers = ("/apply_", "/backfill_", "/enrich_")
    if any(any(marker in f"/{script}" for marker in risky_markers) for script in scripts):
        return "review_required"
    return "report_or_fetch_only"


def command_safety_context(row: dict[str, Any]) -> str:
    return (
        f"campaign_key={row.get('campaign_key', '') or 'none'};"
        f"command_mutation_risk={row.get('command_mutation_risk', '') or 'none'};"
        f"manual_review_required_before_run={str(row.get('manual_review_required_before_run', '')).lower()};"
        f"data_change_authorized={str(row.get('data_change_authorized', '')).lower()}"
    )


def risky_command_scripts(scripts: list[str]) -> list[str]:
    risky_markers = ("/apply_", "/backfill_", "/enrich_")
    return [
        script for script in scripts
        if any(marker in f"/{script}" for marker in risky_markers)
    ]


def review_required_command_context(row: dict[str, Any]) -> str:
    risky_scripts = row.get("risky_command_scripts", [])
    if not isinstance(risky_scripts, list):
        risky_scripts = []
    return (
        f"campaign_key={row.get('campaign_key', '') or 'none'};"
        f"risky_script_count={len(risky_scripts)};"
        f"manual_review_required_before_run={str(row.get('manual_review_required_before_run', '')).lower()};"
        f"data_change_authorized={str(row.get('data_change_authorized', '')).lower()}"
    )


def review_required_preflight_checks(row: dict[str, Any]) -> list[str]:
    if row.get("command_mutation_risk") != "review_required":
        return []
    return [
        "inspect_risky_scripts_before_execution",
        "confirm_listing_keyed_source_review_for_any_write",
        "rerun_quality_validation_and_release_acceptance_after_execution",
    ]


def review_required_preflight_context(row: dict[str, Any]) -> str:
    checks = row.get("review_required_preflight_checks", [])
    if not isinstance(checks, list):
        checks = []
    return (
        f"campaign_key={row.get('campaign_key', '') or 'none'};"
        f"preflight_check_count={len(checks)};"
        f"manual_review_required_before_run={str(row.get('manual_review_required_before_run', '')).lower()};"
        f"data_change_authorized={str(row.get('data_change_authorized', '')).lower()}"
    )


def command_safety_summary_context(summary: dict[str, Any]) -> str:
    return (
        f"actions={summary.get('total_actions', 0)};"
        f"review_required_actions={summary.get('review_required_actions', 0)};"
        f"report_or_fetch_only_actions={summary.get('report_or_fetch_only_actions', 0)};"
        f"manual_review_required_actions={summary.get('manual_review_required_actions', 0)};"
        f"preflight_complete_actions={summary.get('preflight_complete_actions', 0)};"
        f"data_change_authorized_actions={summary.get('data_change_authorized_actions', 0)};"
        f"execution_ready_without_manual_review={str(summary.get('execution_ready_without_manual_review', '')).lower()};"
        f"execution_blocking_gate={summary.get('execution_blocking_gate', '') or 'none'}"
    )


def execution_summary_context(summary: dict[str, Any]) -> str:
    return (
        f"actions={summary.get('total_actions', 0)};"
        f"local_actions={summary.get('local_report_rebuild_actions', 0)};"
        f"network_actions={summary.get('network_evidence_refresh_actions', 0)};"
        f"network_rows={summary.get('network_required_rows', 0)};"
        f"data_change_authorized_actions={summary.get('data_change_authorized_actions', 0)}"
    )


def campaign_summary_context(summary: dict[str, Any]) -> str:
    return (
        f"campaigns={summary.get('campaigns', 0)};"
        f"complete_campaigns={summary.get('complete_campaigns', 0)};"
        f"next_review_batches={summary.get('next_review_batches', 0)};"
        f"next_review_batch_rows={summary.get('next_review_batch_rows', 0)};"
        f"closure_ready_campaigns={summary.get('closure_ready_campaigns', 0)};"
        f"closure_blocked_campaigns={summary.get('closure_blocked_campaigns', 0)};"
        f"closure_blockers={summary.get('closure_blockers', 0)};"
        f"validation_failed_error_gates={summary.get('validation_failed_error_gates', 0)}"
    )


def evaluate_campaign_reviewability(campaigns: dict[str, Any]) -> dict[str, Any]:
    rows = campaigns.get("campaigns", [])
    if not isinstance(rows, list):
        rows = []
    summary = campaigns.get("summary", {})
    if not isinstance(summary, dict):
        summary = {}
    summary_context_missing = not bool(campaigns.get("summary_context"))
    summary_context_mismatch = {}
    if campaigns.get("summary_context") and campaigns.get("summary_context") != campaign_summary_context(summary):
        summary_context_mismatch = {
            "expected": campaign_summary_context(summary),
            "actual": campaigns.get("summary_context"),
        }
    observed_by_priority: dict[int, str] = {}
    duplicate_priorities: list[int] = []
    invalid_priorities: list[Any] = []
    missing_next_actions: list[str] = []
    missing_statuses: list[str] = []
    missing_context_fields: dict[str, list[str]] = {}
    invalid_context_fields: dict[str, list[str]] = {}
    for index, campaign in enumerate(rows, start=1):
        if not isinstance(campaign, dict):
            invalid_priorities.append(f"row_{index}")
            continue
        key = str(campaign.get("campaign_key") or campaign.get("name") or f"row_{index}")
        priority = campaign.get("priority")
        if not isinstance(priority, int) or isinstance(priority, bool):
            invalid_priorities.append(priority)
            continue
        if priority in observed_by_priority:
            duplicate_priorities.append(priority)
        observed_by_priority[priority] = key
        if not campaign.get("next_action"):
            missing_next_actions.append(key)
        if not campaign.get("status"):
            missing_statuses.append(key)
        missing_context = [
            field
            for field in ("campaign_context", "artifact_context", "delta_review_context", "closure_context")
            if not campaign.get(field)
        ]
        if missing_context:
            missing_context_fields[key] = missing_context
        invalid_context = []
        if campaign.get("campaign_context") != campaign_context(campaign):
            invalid_context.append("campaign_context")
        if campaign.get("artifact_context") != campaign_artifact_context(campaign):
            invalid_context.append("artifact_context")
        if campaign.get("delta_review_context") != campaign_delta_review_context(campaign):
            invalid_context.append("delta_review_context")
        if campaign.get("closure_context") != campaign_closure_context(campaign):
            invalid_context.append("closure_context")
        if invalid_context:
            invalid_context_fields[key] = invalid_context
    missing_priorities = [priority for priority in EXPECTED_CAMPAIGN_KEYS_BY_PRIORITY if priority not in observed_by_priority]
    unexpected_priorities = [priority for priority in observed_by_priority if priority not in EXPECTED_CAMPAIGN_KEYS_BY_PRIORITY]
    key_mismatches = {
        priority: {"expected": expected_key, "actual": observed_by_priority.get(priority)}
        for priority, expected_key in EXPECTED_CAMPAIGN_KEYS_BY_PRIORITY.items()
        if priority in observed_by_priority and observed_by_priority.get(priority) != expected_key
    }
    expected_campaigns = summary.get("campaigns", len(rows))
    return {
        "passed": (
            len(rows) == len(EXPECTED_CAMPAIGN_KEYS_BY_PRIORITY)
            and expected_campaigns == len(rows)
            and not summary_context_missing
            and not summary_context_mismatch
            and not duplicate_priorities
            and not invalid_priorities
            and not missing_priorities
            and not unexpected_priorities
            and not key_mismatches
            and not missing_next_actions
            and not missing_statuses
            and not missing_context_fields
            and not invalid_context_fields
        ),
        "campaigns": len(rows),
        "expected_campaigns": expected_campaigns,
        "summary_context_missing": summary_context_missing,
        "summary_context_mismatch": summary_context_mismatch,
        "expected_campaign_keys_by_priority": EXPECTED_CAMPAIGN_KEYS_BY_PRIORITY,
        "observed_campaign_keys_by_priority": dict(sorted(observed_by_priority.items())),
        "duplicate_priorities": sorted(duplicate_priorities),
        "invalid_priorities": invalid_priorities,
        "missing_priorities": missing_priorities,
        "unexpected_priorities": sorted(unexpected_priorities),
        "key_mismatches": key_mismatches,
        "missing_next_actions": missing_next_actions,
        "missing_statuses": missing_statuses,
        "missing_context_fields": missing_context_fields,
        "invalid_context_fields": invalid_context_fields,
    }


def campaign_status_rows(campaigns: dict[str, Any]) -> list[dict[str, Any]]:
    rows = campaigns.get("campaigns", [])
    if not isinstance(rows, list):
        return []
    status_rows: list[dict[str, Any]] = []
    for campaign in sorted(
        (row for row in rows if isinstance(row, dict)),
        key=lambda row: row.get("priority") if isinstance(row.get("priority"), int) else 999,
    ):
        delta_evidence = campaign.get("delta_evidence", {})
        if not isinstance(delta_evidence, dict):
            delta_evidence = {}
        status_rows.append(
            {
                "priority": campaign.get("priority"),
                "campaign_key": campaign.get("campaign_key") or campaign.get("name"),
                "status": campaign.get("status", ""),
                "delta_status": delta_evidence.get("status", ""),
                "next_action": campaign.get("next_action", ""),
            }
        )
    return status_rows


def evaluate_next_review_batch_visibility(campaigns: dict[str, Any]) -> dict[str, Any]:
    rows = campaigns.get("next_review_batches", [])
    if not isinstance(rows, list):
        rows = []
    summary = campaigns.get("summary", {})
    if not isinstance(summary, dict):
        summary = {}
    observed_by_priority: dict[int, str] = {}
    duplicate_priorities: list[int] = []
    invalid_priorities: list[Any] = []
    missing_fields: dict[str, list[str]] = {}
    invalid_context_fields: dict[str, list[str]] = {}
    invalid_artifact_rows: list[str] = []
    invalid_primary_artifacts: list[str] = []
    rows_by_campaign_key: dict[str, int] = {}
    rows_by_closure_gate: dict[str, int] = {}
    blocked_batches = 0
    for index, row in enumerate(rows, start=1):
        if not isinstance(row, dict):
            invalid_priorities.append(f"row_{index}")
            continue
        key = str(row.get("campaign_key") or f"row_{index}")
        artifact_rows_value = int(row.get("artifact_rows") or 0)
        closure_gate = str(row.get("closure_gate") or "missing_closure_gate")
        rows_by_campaign_key[key] = rows_by_campaign_key.get(key, 0) + artifact_rows_value
        rows_by_closure_gate[closure_gate] = rows_by_closure_gate.get(closure_gate, 0) + artifact_rows_value
        if closure_gate != "review_artifact_rows_must_be_processed_before_campaign_closure":
            blocked_batches += 1
        priority = row.get("priority")
        if not isinstance(priority, int) or isinstance(priority, bool):
            invalid_priorities.append(priority)
        else:
            if priority in observed_by_priority:
                duplicate_priorities.append(priority)
            observed_by_priority[priority] = key
        missing = [
            field
            for field in (
                "campaign_key",
                "status",
                "delta_status",
                "closure_gate",
                "next_action",
                "recommended_next_source",
                "source_gate",
                "closure_context",
            )
            if not row.get(field)
        ]
        if missing:
            missing_fields[key] = missing
        if row.get("closure_context") != next_review_batch_closure_context(row):
            invalid_context_fields[key] = ["closure_context"]
        artifact_rows = row.get("artifact_rows")
        if not isinstance(artifact_rows, int) or isinstance(artifact_rows, bool) or artifact_rows < 0:
            invalid_artifact_rows.append(key)
        primary_artifact = str(row.get("primary_artifact") or "")
        if key != "baseline" and not primary_artifact.startswith("data/reports/"):
            invalid_primary_artifacts.append(key)
    missing_priorities = [priority for priority in EXPECTED_CAMPAIGN_KEYS_BY_PRIORITY if priority not in observed_by_priority]
    key_mismatches = {
        priority: {"expected": expected_key, "actual": observed_by_priority.get(priority)}
        for priority, expected_key in EXPECTED_CAMPAIGN_KEYS_BY_PRIORITY.items()
        if priority in observed_by_priority and observed_by_priority.get(priority) != expected_key
    }
    expected_batch_rows = sum(row.get("artifact_rows", 0) for row in rows if isinstance(row, dict))
    workload = campaigns.get("next_review_workload", {})
    if not isinstance(workload, dict):
        workload = {}
    largest_batch = max(rows, key=lambda row: int(row.get("artifact_rows") or 0), default={})
    expected_largest_batch = {
        "priority": largest_batch.get("priority"),
        "campaign_key": largest_batch.get("campaign_key", ""),
        "artifact_rows": int(largest_batch.get("artifact_rows") or 0),
        "primary_artifact": largest_batch.get("primary_artifact", ""),
        "closure_gate": largest_batch.get("closure_gate", ""),
    } if isinstance(largest_batch, dict) else {}
    expected_workload = {
        "total_batches": len(rows),
        "total_rows": expected_batch_rows,
        "blocked_batches": blocked_batches,
        "rows_by_campaign_key": dict(sorted(rows_by_campaign_key.items())),
        "rows_by_closure_gate": dict(sorted(rows_by_closure_gate.items())),
        "largest_batch": expected_largest_batch,
    }
    workload_mismatches = {
        key: {"reported": workload.get(key), "expected": expected_value}
        for key, expected_value in expected_workload.items()
        if workload.get(key) != expected_value
    }
    if workload.get("workload_context") != next_review_workload_context({**workload, **expected_workload}):
        workload_mismatches["workload_context"] = {
            "reported": workload.get("workload_context"),
            "expected": next_review_workload_context({**workload, **expected_workload}),
        }
    policy = str(workload.get("policy") or "")
    missing_workload_policy_markers = [
        marker
        for marker in ("review workload only", "do not authorize data changes", "listing-keyed artifacts")
        if marker not in policy
    ]
    execution_plan = campaigns.get("next_review_execution_plan", [])
    if not isinstance(execution_plan, list):
        execution_plan = []
    plan_by_priority = {
        row.get("priority"): row
        for row in execution_plan
        if isinstance(row, dict)
    }
    execution_plan_gaps: list[dict[str, Any]] = []
    execution_plan_context_mismatches: dict[str, dict[str, str]] = {}
    execution_plan_ranking_mismatches: dict[str, dict[str, Any]] = {}
    execution_plan_command_gaps: dict[str, dict[str, Any]] = {}
    for batch in rows:
        if not isinstance(batch, dict):
            continue
        priority = batch.get("priority")
        key = str(batch.get("campaign_key") or f"priority_{priority}")
        plan_row = plan_by_priority.get(priority)
        if not isinstance(plan_row, dict):
            execution_plan_gaps.append({"campaign_key": key, "reason": "missing_execution_plan_row"})
            continue
        expected_pairs = {
            "campaign_key": batch.get("campaign_key"),
            "artifact_rows": batch.get("artifact_rows"),
            "primary_artifact": batch.get("primary_artifact"),
            "closure_gate": batch.get("closure_gate"),
            "recommended_next_source": batch.get("recommended_next_source"),
            "source_gate": batch.get("source_gate"),
            "next_action": batch.get("next_action"),
        }
        mismatched = [
            field for field, expected in expected_pairs.items()
            if plan_row.get(field) != expected
        ]
        required_nonempty = [
            field
            for field in (
                "evidence_command",
                "command_mode",
                "recommended_next_source",
                "source_gate",
                "execution_context",
                "ranking_reason",
                "ranking_context",
            )
            if not plan_row.get(field)
        ]
        expected_order = priority if isinstance(priority, int) else None
        if plan_row.get("execution_order") != expected_order:
            required_nonempty.append("execution_order_matches_priority")
        if plan_row.get("data_change_authorized") is not False:
            required_nonempty.append("data_change_authorized_false")
        if not isinstance(plan_row.get("network_required"), bool):
            required_nonempty.append("network_required_bool")
        if mismatched or required_nonempty:
            execution_plan_gaps.append(
                {
                    "campaign_key": key,
                    "mismatched_fields": mismatched,
                    "missing_or_invalid_fields": required_nonempty,
                }
            )
        expected_context = execution_plan_context(plan_row)
        if plan_row.get("execution_context") != expected_context:
            execution_plan_context_mismatches[key] = {
                "reported": str(plan_row.get("execution_context") or ""),
                "expected": expected_context,
            }
        expected_ranking_reason = execution_ranking_reason(plan_row)
        expected_ranking_context = execution_ranking_context({**plan_row, "ranking_reason": expected_ranking_reason})
        ranking_mismatch: dict[str, Any] = {}
        if plan_row.get("ranking_reason") != expected_ranking_reason:
            ranking_mismatch["ranking_reason"] = {
                "reported": plan_row.get("ranking_reason"),
                "expected": expected_ranking_reason,
            }
        if plan_row.get("ranking_context") != expected_ranking_context:
            ranking_mismatch["ranking_context"] = {
                "reported": plan_row.get("ranking_context"),
                "expected": expected_ranking_context,
            }
        if ranking_mismatch:
            execution_plan_ranking_mismatches[key] = ranking_mismatch
        expected_scripts = command_script_paths(str(plan_row.get("evidence_command") or ""))
        expected_missing_scripts = [
            script for script in expected_scripts
            if not (ROOT / script).exists()
        ]
        command_gap: dict[str, Any] = {}
        if plan_row.get("command_scripts") != expected_scripts:
            command_gap["command_scripts"] = {
                "reported": plan_row.get("command_scripts"),
                "expected": expected_scripts,
            }
        if plan_row.get("missing_command_scripts") != expected_missing_scripts:
            command_gap["missing_command_scripts"] = {
                "reported": plan_row.get("missing_command_scripts"),
                "expected": expected_missing_scripts,
            }
        if expected_missing_scripts:
            command_gap["missing_local_scripts"] = expected_missing_scripts
        expected_readiness = command_readiness_context(
            {**plan_row, "command_scripts": expected_scripts, "missing_command_scripts": expected_missing_scripts}
        )
        if plan_row.get("command_readiness_context") != expected_readiness:
            command_gap["command_readiness_context"] = {
                "reported": plan_row.get("command_readiness_context"),
                "expected": expected_readiness,
            }
        expected_risk = command_mutation_risk(expected_scripts)
        expected_risky_scripts = risky_command_scripts(expected_scripts)
        expected_manual_review = expected_risk == "review_required"
        if plan_row.get("command_mutation_risk") != expected_risk:
            command_gap["command_mutation_risk"] = {
                "reported": plan_row.get("command_mutation_risk"),
                "expected": expected_risk,
            }
        if plan_row.get("manual_review_required_before_run") is not expected_manual_review:
            command_gap["manual_review_required_before_run"] = {
                "reported": plan_row.get("manual_review_required_before_run"),
                "expected": expected_manual_review,
            }
        if plan_row.get("risky_command_scripts") != expected_risky_scripts:
            command_gap["risky_command_scripts"] = {
                "reported": plan_row.get("risky_command_scripts"),
                "expected": expected_risky_scripts,
            }
        expected_safety = command_safety_context(
            {
                **plan_row,
                "command_mutation_risk": expected_risk,
                "manual_review_required_before_run": expected_manual_review,
            }
        )
        if plan_row.get("command_safety_context") != expected_safety:
            command_gap["command_safety_context"] = {
                "reported": plan_row.get("command_safety_context"),
                "expected": expected_safety,
            }
        expected_review_required_context = review_required_command_context(
            {
                **plan_row,
                "risky_command_scripts": expected_risky_scripts,
                "manual_review_required_before_run": expected_manual_review,
            }
        )
        if plan_row.get("review_required_command_context") != expected_review_required_context:
            command_gap["review_required_command_context"] = {
                "reported": plan_row.get("review_required_command_context"),
                "expected": expected_review_required_context,
            }
        expected_preflight_checks = review_required_preflight_checks(
            {**plan_row, "command_mutation_risk": expected_risk}
        )
        if plan_row.get("review_required_preflight_checks") != expected_preflight_checks:
            command_gap["review_required_preflight_checks"] = {
                "reported": plan_row.get("review_required_preflight_checks"),
                "expected": expected_preflight_checks,
            }
        expected_preflight_context = review_required_preflight_context(
            {
                **plan_row,
                "review_required_preflight_checks": expected_preflight_checks,
                "manual_review_required_before_run": expected_manual_review,
            }
        )
        if plan_row.get("review_required_preflight_context") != expected_preflight_context:
            command_gap["review_required_preflight_context"] = {
                "reported": plan_row.get("review_required_preflight_context"),
                "expected": expected_preflight_context,
            }
        if command_gap:
            execution_plan_command_gaps[key] = command_gap
    if len(execution_plan) != len(rows):
        execution_plan_gaps.append(
            {
                "reason": "execution_plan_row_count_mismatch",
                "reported": len(execution_plan),
                "expected": len(rows),
            }
        )
    execution_summary = campaigns.get("next_review_execution_summary", {})
    if not isinstance(execution_summary, dict):
        execution_summary = {}
    rows_by_command_mode: dict[str, int] = {}
    network_campaign_keys: list[str] = []
    local_campaign_keys: list[str] = []
    for row in execution_plan:
        if not isinstance(row, dict):
            continue
        command_mode = str(row.get("command_mode") or "unknown")
        artifact_rows = int(row.get("artifact_rows") or 0)
        rows_by_command_mode[command_mode] = rows_by_command_mode.get(command_mode, 0) + artifact_rows
        campaign_key = str(row.get("campaign_key") or "")
        if row.get("network_required") is True:
            network_campaign_keys.append(campaign_key)
        else:
            local_campaign_keys.append(campaign_key)
    expected_execution_summary = {
        "total_actions": len(execution_plan),
        "local_report_rebuild_actions": sum(
            1 for row in execution_plan
            if isinstance(row, dict) and row.get("command_mode") == "local_report_rebuild"
        ),
        "network_evidence_refresh_actions": sum(
            1 for row in execution_plan
            if isinstance(row, dict) and row.get("command_mode") == "network_evidence_refresh"
        ),
        "network_required_rows": sum(
            int(row.get("artifact_rows") or 0) for row in execution_plan
            if isinstance(row, dict) and row.get("network_required") is True
        ),
        "local_report_rebuild_rows": sum(
            int(row.get("artifact_rows") or 0) for row in execution_plan
            if isinstance(row, dict) and row.get("network_required") is not True
        ),
        "rows_by_command_mode": dict(sorted(rows_by_command_mode.items())),
        "network_campaign_keys": network_campaign_keys,
        "local_campaign_keys": local_campaign_keys,
        "data_change_authorized_actions": sum(
            1 for row in execution_plan
            if isinstance(row, dict) and row.get("data_change_authorized") is True
        ),
    }
    execution_summary_mismatches = {
        key: {"reported": execution_summary.get(key), "expected": expected_value}
        for key, expected_value in expected_execution_summary.items()
        if execution_summary.get(key) != expected_value
    }
    if execution_summary.get("execution_summary_context") != execution_summary_context(
        {**execution_summary, **expected_execution_summary}
    ):
        execution_summary_mismatches["execution_summary_context"] = {
            "reported": execution_summary.get("execution_summary_context"),
            "expected": execution_summary_context({**execution_summary, **expected_execution_summary}),
        }
    execution_summary_policy = str(execution_summary.get("policy") or "")
    missing_execution_summary_policy_markers = [
        marker
        for marker in ("planning evidence only", "Network refreshes collect source evidence", "no row authorizes data changes")
        if marker not in execution_summary_policy
    ]
    command_safety_summary = campaigns.get("next_review_command_safety_summary", {})
    if not isinstance(command_safety_summary, dict):
        command_safety_summary = {}
    risk_counts: dict[str, int] = {}
    review_required_campaign_keys: list[str] = []
    review_required_command_rows: list[dict[str, Any]] = []
    preflight_complete_actions = 0
    preflight_gap_campaign_keys: list[str] = []
    for row in execution_plan:
        if not isinstance(row, dict):
            continue
        risk = str(row.get("command_mutation_risk") or "unknown")
        risk_counts[risk] = risk_counts.get(risk, 0) + 1
        if risk == "review_required":
            campaign_key = str(row.get("campaign_key") or "")
            review_required_campaign_keys.append(campaign_key)
            scripts = row.get("risky_command_scripts", [])
            if not isinstance(scripts, list):
                scripts = []
            checks = row.get("review_required_preflight_checks", [])
            if not isinstance(checks, list):
                checks = []
            if len(checks) >= 3:
                preflight_complete_actions += 1
            else:
                preflight_gap_campaign_keys.append(campaign_key)
            review_required_command_rows.append(
                {
                    "campaign_key": campaign_key,
                    "risky_command_scripts": scripts,
                    "manual_review_required_before_run": row.get("manual_review_required_before_run") is True,
                    "data_change_authorized": row.get("data_change_authorized") is True,
                    "review_required_command_context": row.get("review_required_command_context", ""),
                    "review_required_preflight_checks": row.get("review_required_preflight_checks", []),
                    "review_required_preflight_context": row.get("review_required_preflight_context", ""),
                }
            )
    execution_blocking_campaign_keys = sorted(set(review_required_campaign_keys + preflight_gap_campaign_keys))
    execution_ready_without_manual_review = not execution_blocking_campaign_keys
    execution_blocking_gate = (
        "no_manual_command_blockers_detected"
        if execution_ready_without_manual_review
        else "manual_review_required_before_execution"
    )
    expected_command_safety_summary = {
        "total_actions": len(execution_plan),
        "risk_counts": dict(sorted(risk_counts.items())),
        "review_required_actions": risk_counts.get("review_required", 0),
        "report_or_fetch_only_actions": risk_counts.get("report_or_fetch_only", 0),
        "manual_review_required_actions": sum(
            1 for row in execution_plan
            if isinstance(row, dict) and row.get("manual_review_required_before_run") is True
        ),
        "review_required_campaign_keys": review_required_campaign_keys,
        "review_required_command_rows": review_required_command_rows,
        "preflight_complete_actions": preflight_complete_actions,
        "preflight_gap_campaign_keys": preflight_gap_campaign_keys,
        "data_change_authorized_actions": sum(
            1 for row in execution_plan
            if isinstance(row, dict) and row.get("data_change_authorized") is True
        ),
        "execution_ready_without_manual_review": execution_ready_without_manual_review,
        "execution_blocking_gate": execution_blocking_gate,
        "execution_blocking_campaign_keys": execution_blocking_campaign_keys,
    }
    command_safety_summary_mismatches = {
        key: {"reported": command_safety_summary.get(key), "expected": expected_value}
        for key, expected_value in expected_command_safety_summary.items()
        if command_safety_summary.get(key) != expected_value
    }
    if command_safety_summary.get("command_safety_summary_context") != command_safety_summary_context(
        {**command_safety_summary, **expected_command_safety_summary}
    ):
        command_safety_summary_mismatches["command_safety_summary_context"] = {
            "reported": command_safety_summary.get("command_safety_summary_context"),
            "expected": command_safety_summary_context({**command_safety_summary, **expected_command_safety_summary}),
        }
    command_safety_summary_policy = str(command_safety_summary.get("policy") or "")
    missing_command_safety_summary_policy_markers = [
        marker
        for marker in ("planning guard", "Review-required commands", "do not authorize data changes")
        if marker not in command_safety_summary_policy
    ]
    return {
        "passed": (
            len(rows) == len(EXPECTED_CAMPAIGN_KEYS_BY_PRIORITY)
            and summary.get("next_review_batches") == len(rows)
            and summary.get("next_review_batch_rows") == expected_batch_rows
            and not duplicate_priorities
            and not invalid_priorities
            and not missing_priorities
            and not key_mismatches
            and not missing_fields
            and not invalid_context_fields
            and not invalid_artifact_rows
            and not invalid_primary_artifacts
            and not workload_mismatches
            and not missing_workload_policy_markers
            and not execution_plan_gaps
            and not execution_plan_context_mismatches
            and not execution_plan_ranking_mismatches
            and not execution_plan_command_gaps
            and not execution_summary_mismatches
            and not missing_execution_summary_policy_markers
            and not command_safety_summary_mismatches
            and not missing_command_safety_summary_policy_markers
        ),
        "batches": len(rows),
        "summary_next_review_batches": summary.get("next_review_batches"),
        "summary_next_review_batch_rows": summary.get("next_review_batch_rows"),
        "computed_next_review_batch_rows": expected_batch_rows,
        "observed_campaign_keys_by_priority": dict(sorted(observed_by_priority.items())),
        "duplicate_priorities": sorted(duplicate_priorities),
        "invalid_priorities": invalid_priorities,
        "missing_priorities": missing_priorities,
        "key_mismatches": key_mismatches,
        "missing_fields": missing_fields,
        "invalid_context_fields": invalid_context_fields,
        "invalid_artifact_rows": invalid_artifact_rows,
        "invalid_primary_artifacts": invalid_primary_artifacts,
        "workload_mismatches": workload_mismatches,
        "missing_workload_policy_markers": missing_workload_policy_markers,
        "execution_plan_gaps": execution_plan_gaps,
        "execution_plan_context_mismatches": execution_plan_context_mismatches,
        "execution_plan_ranking_mismatches": execution_plan_ranking_mismatches,
        "execution_plan_command_gaps": execution_plan_command_gaps,
        "execution_summary_mismatches": execution_summary_mismatches,
        "missing_execution_summary_policy_markers": missing_execution_summary_policy_markers,
        "command_safety_summary_mismatches": command_safety_summary_mismatches,
        "missing_command_safety_summary_policy_markers": missing_command_safety_summary_policy_markers,
    }


def evaluate_next_review_command_safety_gate(campaigns: dict[str, Any]) -> dict[str, Any]:
    plan = campaigns.get("next_review_execution_plan", [])
    if not isinstance(plan, list):
        plan = []
    summary = campaigns.get("next_review_command_safety_summary", {})
    if not isinstance(summary, dict):
        summary = {}
    manual_review_campaign_keys: list[str] = []
    preflight_gap_campaign_keys: list[str] = []
    data_change_authorized_campaign_keys: list[str] = []
    for row in plan:
        if not isinstance(row, dict):
            continue
        campaign_key = str(row.get("campaign_key") or "")
        if row.get("manual_review_required_before_run") is True:
            manual_review_campaign_keys.append(campaign_key)
            checks = row.get("review_required_preflight_checks", [])
            if not isinstance(checks, list) or len(checks) < 3:
                preflight_gap_campaign_keys.append(campaign_key)
        if row.get("data_change_authorized") is True:
            data_change_authorized_campaign_keys.append(campaign_key)
    execution_blocking_campaign_keys = sorted(set(manual_review_campaign_keys + preflight_gap_campaign_keys))
    execution_ready_without_manual_review = not execution_blocking_campaign_keys
    execution_blocking_gate = (
        "no_manual_command_blockers_detected"
        if execution_ready_without_manual_review
        else "manual_review_required_before_execution"
    )
    expected = {
        "manual_review_required_actions": len(manual_review_campaign_keys),
        "preflight_gap_campaign_keys": preflight_gap_campaign_keys,
        "data_change_authorized_actions": len(data_change_authorized_campaign_keys),
        "execution_ready_without_manual_review": execution_ready_without_manual_review,
        "execution_blocking_gate": execution_blocking_gate,
        "execution_blocking_campaign_keys": execution_blocking_campaign_keys,
    }
    mismatches = {
        key: {"reported": summary.get(key), "expected": expected_value}
        for key, expected_value in expected.items()
        if summary.get(key) != expected_value
    }
    policy = str(summary.get("policy") or "")
    missing_policy_markers = [
        marker
        for marker in ("planning guard", "Review-required commands", "do not authorize data changes")
        if marker not in policy
    ]
    missing_summary_keys = [
        key
        for key in (
            "manual_review_required_actions",
            "preflight_gap_campaign_keys",
            "data_change_authorized_actions",
            "execution_ready_without_manual_review",
            "execution_blocking_gate",
            "execution_blocking_campaign_keys",
            "policy",
        )
        if key not in summary
    ]
    return {
        "passed": (
            not missing_summary_keys
            and not mismatches
            and not missing_policy_markers
            and not data_change_authorized_campaign_keys
        ),
        "execution_plan_rows": len(plan),
        "manual_review_campaign_keys": manual_review_campaign_keys,
        "preflight_gap_campaign_keys": preflight_gap_campaign_keys,
        "data_change_authorized_campaign_keys": data_change_authorized_campaign_keys,
        "execution_ready_without_manual_review": execution_ready_without_manual_review,
        "execution_blocking_gate": execution_blocking_gate,
        "execution_blocking_campaign_keys": execution_blocking_campaign_keys,
        "missing_summary_keys": missing_summary_keys,
        "mismatches": mismatches,
        "missing_policy_markers": missing_policy_markers,
        "policy": "Next-review execution is blocked when any command requires manual review, and no execution plan row may authorize data changes.",
    }


def evaluate_campaign_closure_readiness_visibility(campaigns: dict[str, Any]) -> dict[str, Any]:
    closure = campaigns.get("closure_readiness", {})
    if not isinstance(closure, dict):
        closure = {}
    batches = campaigns.get("next_review_batches", [])
    if not isinstance(batches, list):
        batches = []
    gate_counts: dict[str, int] = {}
    blocked_keys: list[str] = []
    ready_keys: list[str] = []
    for row in batches:
        if not isinstance(row, dict):
            continue
        gate = str(row.get("closure_gate") or "missing_closure_gate")
        gate_counts[gate] = gate_counts.get(gate, 0) + 1
        key = str(row.get("campaign_key") or "")
        if gate == "review_artifact_rows_must_be_processed_before_campaign_closure":
            ready_keys.append(key)
        else:
            blocked_keys.append(key)
    missing_keys = [
        key
        for key in ("ready_campaigns", "blocked_campaigns", "closure_gate_counts", "ready_campaign_keys", "blocked_campaign_keys", "policy")
        if key not in closure
    ]
    mismatches: dict[str, Any] = {}
    expected = {
        "ready_campaigns": len(ready_keys),
        "blocked_campaigns": len(blocked_keys),
        "closure_gate_counts": dict(sorted(gate_counts.items())),
        "ready_campaign_keys": ready_keys,
        "blocked_campaign_keys": blocked_keys,
    }
    for key, expected_value in expected.items():
        if closure.get(key) != expected_value:
            mismatches[key] = {"reported": closure.get(key), "expected": expected_value}
    policy = str(closure.get("policy") or "").lower()
    missing_policy_markers = [
        marker
        for marker in ("not closure-ready", "missing delta evidence", "review artifacts")
        if marker not in policy
    ]
    return {
        "passed": not missing_keys and not mismatches and not missing_policy_markers,
        "missing_keys": missing_keys,
        "mismatches": mismatches,
        "missing_policy_markers": missing_policy_markers,
        "computed": expected,
    }


def evaluate_campaign_closure_blocker_visibility(campaigns: dict[str, Any]) -> dict[str, Any]:
    blockers = campaigns.get("closure_blockers", [])
    if not isinstance(blockers, list):
        blockers = []
    batches = campaigns.get("next_review_batches", [])
    if not isinstance(batches, list):
        batches = []
    summary = campaigns.get("summary", {})
    if not isinstance(summary, dict):
        summary = {}
    expected_blocker_keys = [
        str(row.get("campaign_key") or "")
        for row in batches
        if isinstance(row, dict)
        and row.get("closure_gate") != "review_artifact_rows_must_be_processed_before_campaign_closure"
    ]
    observed_blocker_keys: list[str] = []
    missing_fields: dict[str, list[str]] = {}
    invalid_context_fields: dict[str, list[str]] = {}
    invalid_rows: list[str] = []
    for index, blocker in enumerate(blockers, start=1):
        if not isinstance(blocker, dict):
            invalid_rows.append(f"row_{index}")
            continue
        key = str(blocker.get("campaign_key") or f"row_{index}")
        observed_blocker_keys.append(key)
        missing = [
            field
            for field in (
                "priority",
                "campaign_key",
                "blocker_type",
                "closure_gate",
                "artifact_rows",
                "first_missing_delta",
                "evidence_command",
                "command_mode",
                "network_required",
                "data_change_authorized",
                "next_action",
                "recommended_next_source",
                "source_gate",
                "blocker_context",
            )
            if blocker.get(field) in {"", None}
        ]
        if missing:
            missing_fields[key] = missing
        if blocker.get("blocker_context") != blocker_context(blocker):
            invalid_context_fields[key] = ["blocker_context"]
        if blocker.get("blocker_type") not in {"missing_delta_evidence", "future_baseline_comparison"}:
            invalid_rows.append(key)
        if not isinstance(blocker.get("artifact_rows"), int) or isinstance(blocker.get("artifact_rows"), bool):
            invalid_rows.append(key)
        evidence_command = str(blocker.get("evidence_command") or "")
        if evidence_command and not evidence_command.startswith("python scripts/"):
            invalid_rows.append(key)
        if blocker.get("command_mode") not in {"local_report_rebuild", "network_evidence_refresh"}:
            invalid_rows.append(key)
        if not isinstance(blocker.get("network_required"), bool):
            invalid_rows.append(key)
        if blocker.get("data_change_authorized") is not False:
            invalid_rows.append(key)
    mismatches: dict[str, Any] = {}
    if observed_blocker_keys != expected_blocker_keys:
        mismatches["blocker_keys"] = {"reported": observed_blocker_keys, "expected": expected_blocker_keys}
    if summary.get("closure_blockers") != len(blockers):
        mismatches["summary.closure_blockers"] = {"reported": summary.get("closure_blockers"), "expected": len(blockers)}
    return {
        "passed": not missing_fields and not invalid_context_fields and not invalid_rows and not mismatches,
        "blockers": len(blockers),
        "expected_blocker_keys": expected_blocker_keys,
        "observed_blocker_keys": observed_blocker_keys,
        "missing_fields": missing_fields,
        "invalid_context_fields": invalid_context_fields,
        "invalid_rows": invalid_rows,
        "mismatches": mismatches,
    }


def campaign_review_policy_missing_marker_groups(policy: dict[str, Any]) -> list[str]:
    text_by_key = {key: str(policy.get(key, "")).lower() for key in REQUIRED_CAMPAIGN_REVIEW_POLICY_KEYS}
    missing: list[str] = []
    for key, markers in CAMPAIGN_REVIEW_POLICY_REQUIRED_MARKER_GROUPS.items():
        value = text_by_key.get(key, "")
        if not any(marker in value for marker in markers):
            missing.append(key)
    return missing


def evaluate_campaign_review_policies(campaigns: dict[str, Any]) -> dict[str, Any]:
    rows = campaigns.get("campaigns", [])
    if not isinstance(rows, list):
        rows = []
    checked: list[str] = []
    missing_policy: list[str] = []
    missing_policy_keys: dict[str, list[str]] = {}
    weak_policy: dict[str, list[str]] = {}
    for index, campaign in enumerate(rows, start=1):
        if not isinstance(campaign, dict):
            continue
        campaign_key = str(campaign.get("campaign_key") or campaign.get("name") or f"row_{index}")
        checked.append(campaign_key)
        policy = campaign.get("review_policy")
        if not isinstance(policy, dict):
            missing_policy.append(campaign_key)
            continue
        missing_keys = [key for key in REQUIRED_CAMPAIGN_REVIEW_POLICY_KEYS if not policy.get(key)]
        if missing_keys:
            missing_policy_keys[campaign_key] = missing_keys
        missing_marker_groups = campaign_review_policy_missing_marker_groups(policy)
        if missing_marker_groups:
            weak_policy[campaign_key] = missing_marker_groups
    return {
        "passed": bool(checked) and not missing_policy and not missing_policy_keys and not weak_policy,
        "checked_campaigns": checked,
        "checked_count": len(checked),
        "missing_policy": missing_policy,
        "missing_policy_keys": missing_policy_keys,
        "weak_policy": weak_policy,
        "required_policy_keys": list(REQUIRED_CAMPAIGN_REVIEW_POLICY_KEYS),
        "required_marker_groups": {
            key: list(values)
            for key, values in CAMPAIGN_REVIEW_POLICY_REQUIRED_MARKER_GROUPS.items()
        },
    }
def evaluate_campaign_baseline_alignment(baseline: dict[str, Any], deltas: dict[str, Any]) -> dict[str, Any]:
    expected_keys = list(EXPECTED_CAMPAIGN_KEYS_BY_PRIORITY.values())
    campaign_baseline = baseline.get("campaign_baseline", {})
    if not isinstance(campaign_baseline, dict):
        campaign_baseline = {}
    campaign_deltas = deltas.get("campaign_deltas", {})
    if not isinstance(campaign_deltas, dict):
        campaign_deltas = {}
    baseline_keys = list(campaign_baseline)
    delta_keys = list(campaign_deltas)
    missing_baseline_keys = [key for key in expected_keys if key not in campaign_baseline]
    missing_delta_keys = [key for key in expected_keys if key not in campaign_deltas]
    unexpected_baseline_keys = sorted(key for key in campaign_baseline if key not in expected_keys)
    unexpected_delta_keys = sorted(key for key in campaign_deltas if key not in expected_keys)
    baseline_order_matches = baseline_keys == expected_keys
    delta_key_set_matches = sorted(delta_keys) == sorted(expected_keys)
    return {
        "passed": (
            baseline_order_matches
            and delta_key_set_matches
            and not missing_baseline_keys
            and not missing_delta_keys
            and not unexpected_baseline_keys
            and not unexpected_delta_keys
        ),
        "expected_campaign_keys": expected_keys,
        "baseline_campaign_keys": baseline_keys,
        "delta_campaign_keys": delta_keys,
        "baseline_order_matches": baseline_order_matches,
        "delta_key_set_matches": delta_key_set_matches,
        "missing_baseline_keys": missing_baseline_keys,
        "missing_delta_keys": missing_delta_keys,
        "unexpected_baseline_keys": unexpected_baseline_keys,
        "unexpected_delta_keys": unexpected_delta_keys,
    }


def baseline_global_context(global_baseline: dict[str, Any]) -> str:
    return (
        f"metric_count={len(global_baseline)};"
        f"tickers={global_baseline.get('tickers', 0)};"
        f"listing_keys={global_baseline.get('listing_keys', 0)};"
        f"source_gap_rows={global_baseline.get('source_gap_rows', 0)};"
        f"warn_rows={global_baseline.get('entry_quality_warn_rows', 0)};"
        f"quarantine_rows={global_baseline.get('entry_quality_quarantine_rows', 0)};"
        f"validation_failed_error_gates={global_baseline.get('validation_failed_error_gates', 0)}"
    )


def baseline_campaign_context(campaign_key: str, values: dict[str, Any]) -> str:
    numeric_row_total = sum(
        int(value)
        for key, value in values.items()
        if isinstance(value, int) and not isinstance(value, bool) and ("rows" in key or key.endswith("_count"))
    )
    nested_metric_count = sum(1 for value in values.values() if isinstance(value, (dict, list)))
    return (
        f"campaign_key={campaign_key};"
        f"metric_count={len(values)};"
        f"nested_metric_count={nested_metric_count};"
        f"numeric_row_total={numeric_row_total}"
    )


def baseline_exchange_context(exchange: str, values: dict[str, Any]) -> str:
    return (
        f"exchange={exchange};"
        f"tickers={values.get('tickers', 0)};"
        f"isin_coverage={values.get('isin_coverage', 0)};"
        f"sector_coverage={values.get('sector_coverage', 0)};"
        f"source_gap_rows={values.get('source_gap_rows', 0)};"
        f"warn_rows={values.get('entry_quality_warn_rows', 0)};"
        f"quality_source_gap_rows={values.get('entry_quality_source_gap_rows', 0)};"
        f"quarantine_rows={values.get('entry_quality_quarantine_rows', 0)}"
    )


def baseline_context_integrity_gaps(
    contexts: dict[str, Any],
    global_baseline: dict[str, Any],
    campaign_baseline: dict[str, Any],
    exchange_baseline: dict[str, Any],
) -> dict[str, Any]:
    gaps: dict[str, Any] = {}
    if contexts.get("global") != baseline_global_context(global_baseline):
        gaps["global"] = {
            "expected": baseline_global_context(global_baseline),
            "actual": contexts.get("global"),
        }
    campaign_contexts = contexts.get("campaigns", {})
    if not isinstance(campaign_contexts, dict):
        gaps["campaigns"] = {"reason": "expected_object"}
        campaign_contexts = {}
    campaign_gaps = {
        key: {"expected": baseline_campaign_context(key, value), "actual": campaign_contexts.get(key)}
        for key, value in campaign_baseline.items()
        if isinstance(value, dict) and campaign_contexts.get(key) != baseline_campaign_context(key, value)
    }
    if campaign_gaps:
        gaps["campaigns"] = campaign_gaps
    exchange_contexts = contexts.get("exchanges", {})
    if not isinstance(exchange_contexts, dict):
        gaps["exchanges"] = {"reason": "expected_object"}
        exchange_contexts = {}
    exchange_gaps = {
        key: {"expected": baseline_exchange_context(key, value), "actual": exchange_contexts.get(key)}
        for key, value in exchange_baseline.items()
        if isinstance(value, dict) and exchange_contexts.get(key) != baseline_exchange_context(key, value)
    }
    if exchange_gaps:
        gaps["exchanges"] = exchange_gaps
    return gaps


def evaluate_improvement_baseline_integrity(baseline: dict[str, Any]) -> dict[str, Any]:
    meta = baseline.get("_meta", {})
    if not isinstance(meta, dict):
        meta = {}
    source_files = meta.get("source_files", {})
    if not isinstance(source_files, dict):
        source_files = {}
    source_reports = meta.get("source_reports", {})
    if not isinstance(source_reports, dict):
        source_reports = {}
    global_baseline = baseline.get("global_baseline", {})
    if not isinstance(global_baseline, dict):
        global_baseline = {}
    campaign_baseline = baseline.get("campaign_baseline", {})
    if not isinstance(campaign_baseline, dict):
        campaign_baseline = {}
    exchange_baseline = baseline.get("exchange_baseline", {})
    if not isinstance(exchange_baseline, dict):
        exchange_baseline = {}
    baseline_summary = baseline.get("summary", {})
    if not isinstance(baseline_summary, dict):
        baseline_summary = {}
    baseline_context_rows = baseline.get("baseline_contexts", {})
    if not isinstance(baseline_context_rows, dict):
        baseline_context_rows = {}

    required_global_keys = [
        "tickers",
        "listing_keys",
        "isin_coverage",
        "stock_sector_coverage",
        "etf_category_coverage",
        "source_gap_rows",
        "entry_quality_warn_rows",
        "entry_quality_quarantine_rows",
        "validation_failed_error_gates",
        "source_freshness_status_totals",
    ]
    required_exchange_keys = [
        "tickers",
        "isin_coverage",
        "sector_coverage",
        "source_gap_rows",
        "entry_quality_warn_rows",
        "entry_quality_source_gap_rows",
        "entry_quality_quarantine_rows",
    ]
    missing_meta_keys = [
        key
        for key in ("generated_at", "purpose", "source_files", "source_reports")
        if key not in meta or meta.get(key) in ("", None, {}, [])
    ]
    invalid_generated_at = (
        str(meta.get("generated_at", ""))
        if meta.get("generated_at") and not is_valid_iso_utc_timestamp(str(meta.get("generated_at")))
        else ""
    )
    source_file_mismatches = {
        key: {"expected": expected, "actual": source_files.get(key)}
        for key, expected in EXPECTED_BASELINE_SOURCE_FILES.items()
        if source_files.get(key) != expected
    }
    unexpected_source_files = sorted(key for key in source_files if key not in EXPECTED_BASELINE_SOURCE_FILES)
    source_report_mismatches = {
        key: {"expected": expected, "actual": source_reports.get(key)}
        for key, expected in EXPECTED_BASELINE_SOURCE_FILES.items()
        if source_reports.get(key) != expected
    }
    unexpected_source_reports = sorted(key for key in source_reports if key not in EXPECTED_BASELINE_SOURCE_FILES)
    purpose_missing_markers = [
        marker
        for marker in ("Baseline snapshot", "future campaign before/after deltas", "not a data-fill source")
        if marker not in str(meta.get("purpose", ""))
    ]
    missing_global_keys = [key for key in required_global_keys if key not in global_baseline]
    nonnumeric_global_keys = [
        key
        for key in required_global_keys
        if key != "source_freshness_status_totals"
        and key in global_baseline
        and not isinstance(global_baseline.get(key), int | float)
    ]
    freshness_totals_missing = not isinstance(global_baseline.get("source_freshness_status_totals"), dict)
    exchange_rows_checked = len(exchange_baseline)
    exchange_key_gaps = {
        exchange: [key for key in required_exchange_keys if key not in values]
        for exchange, values in exchange_baseline.items()
        if isinstance(values, dict)
        and any(key not in values for key in required_exchange_keys)
    }
    malformed_exchange_rows = sorted(
        exchange for exchange, values in exchange_baseline.items() if not isinstance(values, dict)
    )
    campaign_baseline_summary = campaign_baseline.get("baseline", {})
    if not isinstance(campaign_baseline_summary, dict):
        campaign_baseline_summary = {}
    baseline_summary_mismatches = {}
    expected_campaign_summary = {
        "tracked_campaigns": len(campaign_baseline),
        "global_metric_count": len(global_baseline),
        "exchange_count": len(exchange_baseline),
        "baseline_snapshot_rows": 1,
    }
    for key, expected in expected_campaign_summary.items():
        if campaign_baseline_summary.get(key) != expected:
            baseline_summary_mismatches[key] = {
                "expected": expected,
                "actual": campaign_baseline_summary.get(key),
            }
    expected_top_summary = {
        "global_metric_count": len(global_baseline),
        "campaign_count": len(campaign_baseline),
        "exchange_count": len(exchange_baseline),
        "source_file_count": len(EXPECTED_BASELINE_SOURCE_FILES),
        "baseline_context": baseline_global_context(global_baseline),
    }
    top_summary_mismatches = {
        key: {"expected": expected, "actual": baseline_summary.get(key)}
        for key, expected in expected_top_summary.items()
        if baseline_summary.get(key) != expected
    }
    context_gaps = baseline_context_integrity_gaps(
        baseline_context_rows,
        global_baseline,
        campaign_baseline,
        exchange_baseline,
    )
    return {
        "passed": (
            not missing_meta_keys
            and not invalid_generated_at
            and not source_file_mismatches
            and not unexpected_source_files
            and not source_report_mismatches
            and not unexpected_source_reports
            and not purpose_missing_markers
            and not missing_global_keys
            and not nonnumeric_global_keys
            and not freshness_totals_missing
            and exchange_rows_checked > 0
            and not exchange_key_gaps
            and not malformed_exchange_rows
            and not baseline_summary_mismatches
            and not top_summary_mismatches
            and not context_gaps
        ),
        "missing_meta_keys": missing_meta_keys,
        "invalid_generated_at": invalid_generated_at,
        "source_file_mismatches": source_file_mismatches,
        "unexpected_source_files": unexpected_source_files,
        "source_report_mismatches": source_report_mismatches,
        "unexpected_source_reports": unexpected_source_reports,
        "purpose_missing_markers": purpose_missing_markers,
        "missing_global_keys": missing_global_keys,
        "nonnumeric_global_keys": nonnumeric_global_keys,
        "freshness_totals_missing": freshness_totals_missing,
        "exchange_rows_checked": exchange_rows_checked,
        "exchange_key_gaps": exchange_key_gaps,
        "malformed_exchange_rows": malformed_exchange_rows,
        "baseline_summary_mismatches": baseline_summary_mismatches,
        "top_summary_mismatches": top_summary_mismatches,
        "baseline_context_gaps": context_gaps,
    }


def report_has_review_gate_summary(path: Path) -> bool:
    payload = load_json(path)
    summary = payload.get("summary", {})
    summary_keys = set(summary) if isinstance(summary, dict) else set()
    rows = first_review_row_container(payload)
    first_row = rows[0] if isinstance(rows, list) and rows and isinstance(rows[0], dict) else {}
    keys = summary_keys | set(first_row)
    return any(marker in key for key in keys for marker in REVIEW_GATE_KEY_MARKERS)


def report_review_policy(payload: dict[str, Any]) -> str:
    def normalize(value: Any) -> str:
        if isinstance(value, dict):
            return " ".join(normalize(item) for item in value.values())
        if isinstance(value, list):
            return " ".join(normalize(item) for item in value)
        return str(value).strip()

    summary = payload.get("summary", {})
    meta = payload.get("_meta", {})
    candidates = [
        meta.get("policy", "") if isinstance(meta, dict) else "",
        meta.get("source_policy", "") if isinstance(meta, dict) else "",
        summary.get("policy", "") if isinstance(summary, dict) else "",
        payload.get("policy", ""),
    ]
    return next((normalize(value) for value in candidates if normalize(value)), "")


def review_policy_missing_marker_groups(policy: str) -> list[str]:
    normalized = policy.lower().replace("-", "_")
    return [
        group
        for group, markers in REVIEW_POLICY_REQUIRED_MARKER_GROUPS.items()
        if not any(marker in normalized for marker in markers)
    ]


def first_review_row_container(payload: dict[str, Any]) -> list[Any]:
    for key in REVIEW_ROW_CONTAINER_KEYS:
        rows = payload.get(key)
        if isinstance(rows, list):
            return rows
    return []


def row_has_review_identity(row: dict[str, Any]) -> bool:
    for key in REVIEW_ROW_IDENTITY_KEYS:
        if row.get(key):
            return True
    for key in REVIEW_ROW_LISTING_KEY_COLLECTIONS:
        value = row.get(key)
        if isinstance(value, list) and any(str(item).strip() for item in value):
            return True
        if isinstance(value, str) and value.strip():
            return True
    return any(all(row.get(key) for key in group) for group in REVIEW_ROW_CANDIDATE_IDENTITY_KEY_GROUPS)


def row_has_review_evidence(row: dict[str, Any]) -> bool:
    return any(
        any(marker in key for marker in REVIEW_ROW_EVIDENCE_KEY_MARKERS)
        and value not in ("", None, [], {})
        for key, value in row.items()
    )


def row_has_apply_traceability(row: dict[str, Any]) -> bool:
    if str(row.get("decision", "")) == "apply" and not row.get("verification_evidence_required"):
        return False
    return (
        bool(row.get("listing_key"))
        and bool(row.get("decision"))
        and bool(row.get("reason"))
        and any(row.get(key) not in ("", None, [], {}) for key in APPLY_ROW_EVIDENCE_KEYS)
    )


def report_apply_traceability_gaps(path: Path) -> list[dict[str, Any]]:
    payload = load_json(path)
    rows = first_review_row_container(payload)
    reported_count = report_row_count(payload)
    gaps: list[dict[str, Any]] = []
    if reported_count > len(rows):
        gaps.append(
            {
                "row_index": None,
                "reason": "apply_row_container_count_mismatch",
                "reported_rows": reported_count,
                "row_container_rows": len(rows),
                "accepted_row_container_keys": list(REVIEW_ROW_CONTAINER_KEYS),
            }
        )
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            gaps.append({"row_index": index, "reason": "row_is_not_object"})
            continue
        invalid_fields: list[str] = []
        if path.name == "canada_figi_apply_report.json":
            if row.get("openfigi_probe_context") != canada_figi_apply_openfigi_probe_context(row):
                invalid_fields.append("openfigi_probe_context")
            if row.get("existing_identifier_context") != canada_figi_apply_existing_identifier_context(row):
                invalid_fields.append("existing_identifier_context")
            if row.get("apply_gate_context") != canada_figi_apply_gate_context(row):
                invalid_fields.append("apply_gate_context")
        if path.name == "b3_etf_category_apply_report.json":
            if row.get("official_source_context") != b3_etf_apply_official_source_context(row):
                invalid_fields.append("official_source_context")
            if row.get("category_review_context") != b3_etf_apply_category_review_context(row):
                invalid_fields.append("category_review_context")
            if row.get("apply_gate_context") != b3_etf_apply_gate_context(row):
                invalid_fields.append("apply_gate_context")
        if path.name == "ngx_official_sector_apply_report.json":
            if row.get("official_source_context") != ngx_apply_official_source_context(row):
                invalid_fields.append("official_source_context")
            if row.get("mapping_review_context") != ngx_apply_mapping_review_context(row):
                invalid_fields.append("mapping_review_context")
            if row.get("apply_gate_context") != ngx_apply_gate_context(row):
                invalid_fields.append("apply_gate_context")
        if not row_has_apply_traceability(row) or invalid_fields:
            gap = {
                "row_index": index,
                "reason": "missing_listing_key_decision_reason_or_source_evidence",
                "available_keys": sorted(row)[:20],
            }
            if invalid_fields:
                gap["invalid_fields"] = invalid_fields
            gaps.append(gap)
    return gaps


def report_supplement_traceability_gaps(path: Path) -> list[dict[str, Any]]:
    payload = load_json(path)
    rows = first_review_row_container(payload)
    reported_count = report_row_count(payload)
    gaps: list[dict[str, Any]] = []
    if reported_count > len(rows):
        gaps.append(
            {
                "row_index": None,
                "reason": "supplement_row_container_count_mismatch",
                "reported_rows": reported_count,
                "row_container_rows": len(rows),
                "accepted_row_container_keys": list(REVIEW_ROW_CONTAINER_KEYS),
            }
        )
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            gaps.append({"row_index": index, "reason": "row_is_not_object"})
            continue
        missing_keys = [key for key in SUPPLEMENT_ROW_REQUIRED_KEYS if not row.get(key)]
        decision = str(row.get("decision", ""))
        apply_eligibility = str(row.get("apply_eligibility", ""))
        missing_official_evidence = [
            key
            for key in SUPPLEMENT_OFFICIAL_EVIDENCE_KEYS
            if decision in {"accept", "preserve"} and not row.get(key)
        ]
        invalid_fields = []
        if row.get("review_priority") not in {"P1", "P2", "P3", "P4"}:
            invalid_fields.append("review_priority")
        if apply_eligibility.startswith("apply") or "symbol_only" in apply_eligibility or "global_ticker_reuse" in apply_eligibility:
            invalid_fields.append("apply_eligibility")
        if str(row.get("verification_evidence_required", "")) in {"", "none", "ticker_match_only"}:
            invalid_fields.append("verification_evidence_required")
        if row.get("financialdata_discovery_context") != financialdata_discovery_context(row):
            invalid_fields.append("financialdata_discovery_context")
        if row.get("official_identity_context") != financialdata_official_identity_context(row):
            invalid_fields.append("official_identity_context")
        if row.get("supplement_review_context") != financialdata_supplement_review_context(row):
            invalid_fields.append("supplement_review_context")
        if missing_keys or missing_official_evidence or invalid_fields:
            gaps.append(
                {
                    "row_index": index,
                    "reason": "missing_financialdata_identity_review_or_official_evidence",
                    "missing_keys": missing_keys,
                    "missing_official_evidence": missing_official_evidence,
                    "invalid_fields": invalid_fields,
                    "available_keys": sorted(row)[:20],
                }
            )
    return gaps


def report_review_identity_gaps(path: Path) -> list[dict[str, Any]]:
    payload = load_json(path)
    rows = first_review_row_container(payload)
    reported_count = report_row_count(payload)
    gaps: list[dict[str, Any]] = []
    if reported_count > len(rows):
        gaps.append(
            {
                "row_index": None,
                "reason": "review_row_container_count_mismatch",
                "reported_rows": reported_count,
                "row_container_rows": len(rows),
                "accepted_row_container_keys": list(REVIEW_ROW_CONTAINER_KEYS),
            }
        )
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            gaps.append({"row_index": index, "reason": "row_is_not_object"})
            continue
        if not row_has_review_identity(row):
            gaps.append(
                {
                    "row_index": index,
                    "reason": "missing_listing_key_or_review_identity",
                    "available_keys": sorted(row)[:20],
                }
            )
    return gaps


def report_review_evidence_gaps(path: Path) -> list[dict[str, Any]]:
    payload = load_json(path)
    rows = first_review_row_container(payload)
    reported_count = report_row_count(payload)
    gaps: list[dict[str, Any]] = []
    if reported_count > len(rows):
        gaps.append(
            {
                "row_index": None,
                "reason": "review_row_container_count_mismatch",
                "reported_rows": reported_count,
                "row_container_rows": len(rows),
                "accepted_row_container_keys": list(REVIEW_ROW_CONTAINER_KEYS),
            }
        )
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            gaps.append({"row_index": index, "reason": "row_is_not_object"})
            continue
        invalid_fields: list[str] = []
        if path.name == "b3_masterfile_gap_review.json":
            queue = str(row.get("b3_resolution_queue", ""))
            if row.get("recommended_next_source") != b3_masterfile_gap_recommended_next_source(queue):
                invalid_fields.append("recommended_next_source")
            if row.get("source_gate") != b3_masterfile_gap_source_gate(queue):
                invalid_fields.append("source_gate")
            if row.get("b3_listing_context") != b3_masterfile_gap_listing_context(row):
                invalid_fields.append("b3_listing_context")
            if row.get("official_candidate_context") != b3_masterfile_gap_official_candidate_context(row):
                invalid_fields.append("official_candidate_context")
            if row.get("review_gate_context") != b3_masterfile_gap_review_gate_context(row):
                invalid_fields.append("review_gate_context")
        if path.name == "otc_name_mismatch_review.json":
            review_class = str(row.get("review_class", ""))
            if row.get("review_strategy") != otc_name_mismatch_review_strategy(review_class):
                invalid_fields.append("review_strategy")
            if row.get("recommended_next_source") != otc_name_mismatch_recommended_next_source(review_class):
                invalid_fields.append("recommended_next_source")
            if row.get("source_gate") != otc_name_mismatch_source_gate(review_class):
                invalid_fields.append("source_gate")
            if row.get("official_source_context") != otc_name_mismatch_official_source_context(row):
                invalid_fields.append("official_source_context")
            if row.get("identity_review_context") != otc_name_mismatch_identity_review_context(row):
                invalid_fields.append("identity_review_context")
            if row.get("decision_review_context") != otc_name_mismatch_decision_review_context(row):
                invalid_fields.append("decision_review_context")
        if not row_has_review_evidence(row) or invalid_fields:
            gap = {
                "row_index": index,
                "reason": "missing_source_review_decision_or_gap_evidence",
                "available_keys": sorted(row)[:20],
            }
            if invalid_fields:
                gap["invalid_fields"] = invalid_fields
            gaps.append(gap)
    return gaps


def report_row_count(payload: dict[str, Any]) -> int:
    summary = payload.get("summary", {})
    containers = [summary if isinstance(summary, dict) else {}, payload.get("_meta", {}), payload]
    for container in containers:
        if not isinstance(container, dict):
            continue
        for key in ("review_rows", "rows", "selected_rows", "review_items", "items"):
            value = container.get(key)
            if isinstance(value, list):
                return len(value)
            if value in {"", None}:
                continue
            try:
                return int(value)
            except (TypeError, ValueError):
                continue
    for key in ("rows", "review_items", "items"):
        value = payload.get(key)
        if isinstance(value, list):
            return len(value)
    return 0


def report_generated_at(payload: dict[str, Any]) -> str:
    summary = payload.get("summary", {})
    return (
        payload.get("_meta", {}).get("generated_at", "")
        or (summary.get("generated_at", "") if isinstance(summary, dict) else "")
        or payload.get("generated_at", "")
    )


def csv_row_count(path: Path) -> int:
    with path.open(newline="", encoding="utf-8") as handle:
        return sum(1 for _ in csv.DictReader(handle))


def evaluate_campaign_artifact_integrity(campaigns: dict[str, Any], reports_dir: Path = REPORTS_DIR) -> dict[str, Any]:
    meta = campaigns.get("_meta", {})
    rows = campaigns.get("campaigns", [])
    if not isinstance(meta, dict):
        meta = {}
    if not isinstance(rows, list):
        rows = []
    source_files = meta.get("source_files", {})
    if not isinstance(source_files, dict):
        source_files = {}
    missing_meta_keys = [
        key
        for key in ("generated_at", "rows", "source_files", "policy")
        if key not in meta or meta.get(key) in ("", None, {}, [])
    ]
    invalid_generated_at = (
        str(meta.get("generated_at", ""))
        if meta.get("generated_at") and not is_valid_iso_utc_timestamp(str(meta.get("generated_at")))
        else ""
    )
    campaign_row_count_mismatch = {
        "reported": meta.get("rows", 0),
        "actual": len(rows),
    } if meta.get("rows", 0) != len(rows) else {}
    source_file_mismatches = {
        key: {"expected": expected, "actual": source_files.get(key)}
        for key, expected in EXPECTED_CAMPAIGN_SOURCE_FILES.items()
        if source_files.get(key) != expected
    }
    unexpected_source_files = sorted(key for key in source_files if key not in EXPECTED_CAMPAIGN_SOURCE_FILES)
    policy_missing_markers = [
        marker
        for marker in ("Progress report only", "does not authorize", "data fills", "symbol changes")
        if marker not in str(meta.get("policy", ""))
    ]
    checked: list[str] = []
    missing_files: list[str] = []
    row_count_mismatches: list[dict[str, Any]] = []
    missing_generated_at: list[str] = []
    generated_at_mismatches: list[dict[str, str]] = []
    for campaign in rows:
        if not isinstance(campaign, dict):
            continue
        artifacts = campaign.get("artifacts", [])
        if not isinstance(artifacts, list):
            continue
        for artifact in artifacts:
            if not isinstance(artifact, dict):
                continue
            artifact_path = str(artifact.get("path", ""))
            if not artifact_path.endswith(".json"):
                continue
            checked.append(artifact_path)
            path = reports_dir / Path(artifact_path).name
            if not path.exists():
                missing_files.append(artifact_path)
                continue
            payload = load_json(path)
            actual_rows = report_row_count(payload)
            expected_rows = artifact.get("rows")
            if expected_rows != actual_rows:
                row_count_mismatches.append(
                    {
                        "path": artifact_path,
                        "campaign_rows": expected_rows,
                        "actual_rows": actual_rows,
                    }
                )
            actual_generated_at = report_generated_at(payload)
            campaign_generated_at = str(artifact.get("generated_at", ""))
            if not actual_generated_at:
                missing_generated_at.append(artifact_path)
            elif campaign_generated_at != actual_generated_at:
                generated_at_mismatches.append(
                    {
                        "path": artifact_path,
                        "campaign_generated_at": campaign_generated_at,
                        "actual_generated_at": actual_generated_at,
                    }
                )
    return {
        "passed": (
            bool(checked)
            and not missing_meta_keys
            and not invalid_generated_at
            and not campaign_row_count_mismatch
            and not source_file_mismatches
            and not unexpected_source_files
            and not policy_missing_markers
            and not missing_files
            and not row_count_mismatches
            and not missing_generated_at
            and not generated_at_mismatches
        ),
        "missing_meta_keys": missing_meta_keys,
        "invalid_generated_at": invalid_generated_at,
        "campaign_row_count_mismatch": campaign_row_count_mismatch,
        "source_file_mismatches": source_file_mismatches,
        "unexpected_source_files": unexpected_source_files,
        "policy_missing_markers": policy_missing_markers,
        "checked_artifacts": checked,
        "checked_count": len(checked),
        "missing_files": missing_files,
        "row_count_mismatches": row_count_mismatches,
        "missing_generated_at": missing_generated_at,
        "generated_at_mismatches": generated_at_mismatches,
    }


def evaluate_review_artifact_siblings(campaigns: dict[str, Any], reports_dir: Path = REPORTS_DIR) -> dict[str, Any]:
    rows = campaigns.get("campaigns", [])
    if not isinstance(rows, list):
        rows = []
    checked: list[str] = []
    missing_files: list[str] = []
    missing_siblings: list[dict[str, Any]] = []
    csv_row_count_mismatches: list[dict[str, Any]] = []
    empty_markdown_summaries: list[str] = []
    stale_siblings: list[dict[str, Any]] = []
    sibling_paths: dict[str, dict[str, str]] = {}
    for campaign in rows:
        if not isinstance(campaign, dict):
            continue
        artifacts = campaign.get("artifacts", [])
        if not isinstance(artifacts, list):
            continue
        for artifact in artifacts:
            if not isinstance(artifact, dict):
                continue
            artifact_path = str(artifact.get("path", ""))
            if not is_review_artifact_path(artifact_path):
                continue
            checked.append(artifact_path)
            path = reports_dir / Path(artifact_path).name
            if not path.exists():
                missing_files.append(artifact_path)
                continue
            payload = load_json(path)
            expected_rows = report_row_count(payload)
            csv_path = path.with_suffix(".csv")
            md_path = path.with_suffix(".md")
            sibling_paths[artifact_path] = {
                "csv": str(Path("data/reports") / csv_path.name),
                "md": str(Path("data/reports") / md_path.name),
            }
            missing = [
                suffix
                for suffix, sibling_path in (("csv", csv_path), ("md", md_path))
                if not sibling_path.exists()
            ]
            if missing:
                missing_siblings.append({"path": artifact_path, "missing": missing})
                continue
            json_mtime = path.stat().st_mtime
            for suffix, sibling_path in (("csv", csv_path), ("md", md_path)):
                age_seconds = json_mtime - sibling_path.stat().st_mtime
                if age_seconds > REVIEW_ARTIFACT_SIBLING_STALE_GRACE_SECONDS:
                    stale_siblings.append(
                        {
                            "path": artifact_path,
                            "sibling": suffix,
                            "age_seconds": round(age_seconds, 3),
                        }
                    )
            actual_csv_rows = csv_row_count(csv_path)
            if actual_csv_rows != expected_rows:
                csv_row_count_mismatches.append(
                    {
                        "path": artifact_path,
                        "json_rows": expected_rows,
                        "csv_rows": actual_csv_rows,
                    }
                )
            if not md_path.read_text(encoding="utf-8").strip():
                empty_markdown_summaries.append(artifact_path)
    return {
        "passed": (
            bool(checked)
            and not missing_files
            and not missing_siblings
            and not csv_row_count_mismatches
            and not empty_markdown_summaries
            and not stale_siblings
        ),
        "checked_artifacts": checked,
        "checked_count": len(checked),
        "missing_files": missing_files,
        "missing_siblings": missing_siblings,
        "csv_row_count_mismatches": csv_row_count_mismatches,
        "empty_markdown_summaries": empty_markdown_summaries,
        "stale_siblings": stale_siblings,
        "sibling_paths": sibling_paths,
    }


def evaluate_review_artifact_gates(campaigns: dict[str, Any], reports_dir: Path = REPORTS_DIR) -> dict[str, Any]:
    rows = campaigns.get("campaigns", [])
    if not isinstance(rows, list):
        rows = []
    checked: list[str] = []
    missing_files: list[str] = []
    missing_gate_summaries: list[str] = []
    for campaign in rows:
        if not isinstance(campaign, dict):
            continue
        artifacts = campaign.get("artifacts", [])
        if not isinstance(artifacts, list):
            continue
        for artifact in artifacts:
            if not isinstance(artifact, dict):
                continue
            artifact_path = str(artifact.get("path", ""))
            if not is_review_artifact_path(artifact_path):
                continue
            checked.append(artifact_path)
            path = reports_dir / Path(artifact_path).name
            if not path.exists():
                missing_files.append(artifact_path)
                continue
            if not report_has_review_gate_summary(path):
                missing_gate_summaries.append(artifact_path)
    return {
        "passed": bool(checked) and not missing_files and not missing_gate_summaries,
        "checked_artifacts": checked,
        "checked_count": len(checked),
        "missing_files": missing_files,
        "missing_gate_summaries": missing_gate_summaries,
        "required_gate_key_markers": list(REVIEW_GATE_KEY_MARKERS),
    }


def evaluate_review_artifact_policy(campaigns: dict[str, Any], reports_dir: Path = REPORTS_DIR) -> dict[str, Any]:
    rows = campaigns.get("campaigns", [])
    if not isinstance(rows, list):
        rows = []
    checked: list[str] = []
    missing_files: list[str] = []
    missing_policy: list[str] = []
    weak_policy: dict[str, list[str]] = {}
    policies: dict[str, str] = {}
    for campaign in rows:
        if not isinstance(campaign, dict):
            continue
        artifacts = campaign.get("artifacts", [])
        if not isinstance(artifacts, list):
            continue
        for artifact in artifacts:
            if not isinstance(artifact, dict):
                continue
            artifact_path = str(artifact.get("path", ""))
            if not is_review_artifact_path(artifact_path):
                continue
            checked.append(artifact_path)
            path = reports_dir / Path(artifact_path).name
            if not path.exists():
                missing_files.append(artifact_path)
                continue
            policy = report_review_policy(load_json(path))
            if not policy:
                missing_policy.append(artifact_path)
            else:
                missing_marker_groups = review_policy_missing_marker_groups(policy)
                if missing_marker_groups:
                    weak_policy[artifact_path] = missing_marker_groups
                policies[artifact_path] = policy
    return {
        "passed": bool(checked) and not missing_files and not missing_policy and not weak_policy,
        "checked_artifacts": checked,
        "checked_count": len(checked),
        "missing_files": missing_files,
        "missing_policy": missing_policy,
        "weak_policy": weak_policy,
        "required_marker_groups": {
            key: list(values)
            for key, values in REVIEW_POLICY_REQUIRED_MARKER_GROUPS.items()
        },
        "policies": policies,
    }


def evaluate_review_row_traceability(campaigns: dict[str, Any], reports_dir: Path = REPORTS_DIR) -> dict[str, Any]:
    rows = campaigns.get("campaigns", [])
    if not isinstance(rows, list):
        rows = []
    checked: list[str] = []
    missing_files: list[str] = []
    identity_gap_rows: dict[str, list[dict[str, Any]]] = {}
    for campaign in rows:
        if not isinstance(campaign, dict):
            continue
        artifacts = campaign.get("artifacts", [])
        if not isinstance(artifacts, list):
            continue
        for artifact in artifacts:
            if not isinstance(artifact, dict):
                continue
            artifact_path = str(artifact.get("path", ""))
            if not is_review_artifact_path(artifact_path):
                continue
            checked.append(artifact_path)
            path = reports_dir / Path(artifact_path).name
            if not path.exists():
                missing_files.append(artifact_path)
                continue
            gaps = report_review_identity_gaps(path)
            if gaps:
                identity_gap_rows[artifact_path] = gaps[:20]
    return {
        "passed": bool(checked) and not missing_files and not identity_gap_rows,
        "checked_artifacts": checked,
        "checked_count": len(checked),
        "missing_files": missing_files,
        "identity_gap_rows": identity_gap_rows,
        "required_identity_keys": list(REVIEW_ROW_IDENTITY_KEYS),
        "accepted_listing_key_collections": list(REVIEW_ROW_LISTING_KEY_COLLECTIONS),
        "accepted_candidate_identity_key_groups": [list(group) for group in REVIEW_ROW_CANDIDATE_IDENTITY_KEY_GROUPS],
    }


def evaluate_review_row_evidence(campaigns: dict[str, Any], reports_dir: Path = REPORTS_DIR) -> dict[str, Any]:
    rows = campaigns.get("campaigns", [])
    if not isinstance(rows, list):
        rows = []
    checked: list[str] = []
    missing_files: list[str] = []
    evidence_gap_rows: dict[str, list[dict[str, Any]]] = {}
    for campaign in rows:
        if not isinstance(campaign, dict):
            continue
        artifacts = campaign.get("artifacts", [])
        if not isinstance(artifacts, list):
            continue
        for artifact in artifacts:
            if not isinstance(artifact, dict):
                continue
            artifact_path = str(artifact.get("path", ""))
            if not is_review_artifact_path(artifact_path):
                continue
            checked.append(artifact_path)
            path = reports_dir / Path(artifact_path).name
            if not path.exists():
                missing_files.append(artifact_path)
                continue
            gaps = report_review_evidence_gaps(path)
            if gaps:
                evidence_gap_rows[artifact_path] = gaps[:20]
    return {
        "passed": bool(checked) and not missing_files and not evidence_gap_rows,
        "checked_artifacts": checked,
        "checked_count": len(checked),
        "missing_files": missing_files,
        "evidence_gap_rows": evidence_gap_rows,
        "required_evidence_key_markers": list(REVIEW_ROW_EVIDENCE_KEY_MARKERS),
    }


def evaluate_apply_artifact_traceability(campaigns: dict[str, Any], reports_dir: Path = REPORTS_DIR) -> dict[str, Any]:
    rows = campaigns.get("campaigns", [])
    if not isinstance(rows, list):
        rows = []
    checked: list[str] = []
    missing_files: list[str] = []
    traceability_gap_rows: dict[str, list[dict[str, Any]]] = {}
    missing_policy: list[str] = []
    for campaign in rows:
        if not isinstance(campaign, dict):
            continue
        artifacts = campaign.get("artifacts", [])
        if not isinstance(artifacts, list):
            continue
        for artifact in artifacts:
            if not isinstance(artifact, dict):
                continue
            artifact_path = str(artifact.get("path", ""))
            if not is_apply_artifact_path(artifact_path):
                continue
            checked.append(artifact_path)
            path = reports_dir / Path(artifact_path).name
            if not path.exists():
                missing_files.append(artifact_path)
                continue
            payload = load_json(path)
            policy = payload.get("summary", {}).get("policy", {}) if isinstance(payload.get("summary"), dict) else {}
            if not policy:
                missing_policy.append(artifact_path)
            gaps = report_apply_traceability_gaps(path)
            if gaps:
                traceability_gap_rows[artifact_path] = gaps[:20]
    return {
        "passed": bool(checked) and not missing_files and not missing_policy and not traceability_gap_rows,
        "checked_artifacts": checked,
        "checked_count": len(checked),
        "missing_files": missing_files,
        "missing_policy": missing_policy,
        "traceability_gap_rows": traceability_gap_rows,
        "required_row_keys": ["listing_key", "decision", "reason"],
        "required_evidence_keys": list(APPLY_ROW_EVIDENCE_KEYS),
    }


def evaluate_supplement_artifact_traceability(campaigns: dict[str, Any], reports_dir: Path = REPORTS_DIR) -> dict[str, Any]:
    rows = campaigns.get("campaigns", [])
    if not isinstance(rows, list):
        rows = []
    checked: list[str] = []
    missing_files: list[str] = []
    missing_policy: list[str] = []
    traceability_gap_rows: dict[str, list[dict[str, Any]]] = {}
    missing_from_campaigns = True
    for campaign in rows:
        if not isinstance(campaign, dict):
            continue
        artifacts = campaign.get("artifacts", [])
        if not isinstance(artifacts, list):
            continue
        for artifact in artifacts:
            if not isinstance(artifact, dict):
                continue
            artifact_path = str(artifact.get("path", ""))
            if Path(artifact_path).name != SUPPLEMENT_REVIEW_ARTIFACT_NAME:
                continue
            missing_from_campaigns = False
            checked.append(artifact_path)
            path = reports_dir / Path(artifact_path).name
            if not path.exists():
                missing_files.append(artifact_path)
                continue
            payload = load_json(path)
            policy = payload.get("summary", {}).get("policy", {}) if isinstance(payload.get("summary"), dict) else {}
            if not isinstance(policy, dict) or not policy.get("financialdata_role") or not policy.get("identifier_gate"):
                missing_policy.append(artifact_path)
            gaps = report_supplement_traceability_gaps(path)
            if gaps:
                traceability_gap_rows[artifact_path] = gaps[:20]
    return {
        "passed": (
            bool(checked)
            and not missing_from_campaigns
            and not missing_files
            and not missing_policy
            and not traceability_gap_rows
        ),
        "checked_artifacts": checked,
        "checked_count": len(checked),
        "missing_from_campaigns": missing_from_campaigns,
        "missing_files": missing_files,
        "missing_policy": missing_policy,
        "traceability_gap_rows": traceability_gap_rows,
        "required_row_keys": list(SUPPLEMENT_ROW_REQUIRED_KEYS),
        "required_official_evidence_for_accept_or_preserve": list(SUPPLEMENT_OFFICIAL_EVIDENCE_KEYS),
    }


def build_payload() -> dict[str, Any]:
    generated_at = utc_now_iso()
    validation = load_json(REPORTS_DIR / "validation_report.json")
    entry_quality_gate = load_json(REPORTS_DIR / "entry_quality_gate.json")
    coverage = load_json(REPORTS_DIR / "coverage_report.json")
    source_gap = load_json(REPORTS_DIR / "source_gap_classification.json")
    symbol_changes_review = load_json(REPORTS_DIR / "symbol_changes_review.json")
    ohlcv = load_json(REPORTS_DIR / "ohlcv_plausibility.json")
    masterfile_collision = load_json(REPORTS_DIR / "masterfile_collision_review.json")
    otc_scope = load_json(REPORTS_DIR / "otc_scope_review.json")
    canada_residual = load_json(REPORTS_DIR / "canada_residual_review.json")
    canada_figi_queue = load_json(REPORTS_DIR / "canada_figi_queue.json")
    canada_figi_apply = load_json(REPORTS_DIR / "canada_figi_apply_report.json")
    b3_residual_isin = load_json(REPORTS_DIR / "b3_residual_isin_review.json")
    b3_residual_sector = load_json(REPORTS_DIR / "b3_residual_sector_review.json")
    asx_residual = load_json(REPORTS_DIR / "asx_residual_review.json")
    weak_sector = load_json(REPORTS_DIR / "weak_sector_residual_review.json")
    adanos_detection_simulation = load_json(REPORTS_DIR / "adanos_detection_simulation.json")
    baseline = load_json(REPORTS_DIR / "improvement_baseline.json")
    deltas = load_json(REPORTS_DIR / "improvement_deltas.json")
    campaigns = load_json(REPORTS_DIR / "improvement_campaigns.json")
    gates = gate_lookup(validation)
    criteria = {
        key: evaluate_gate_group(gates, names)
        for key, names in REQUIRED_GATE_GROUPS.items()
    }
    criteria["release_source_report_integrity"] = evaluate_release_source_report_integrity(
        RELEASE_SOURCE_REPORTS,
        release_generated_at=generated_at,
    )
    criteria["progress_markdown_traceability"] = evaluate_progress_markdown_traceability()
    criteria["adanos_detection_simulation"] = evaluate_adanos_detection_simulation(adanos_detection_simulation)
    criteria["entry_quality_command_report"] = evaluate_entry_quality_command_report(entry_quality_gate, gates)
    criteria["coverage_freshness_visibility"] = evaluate_coverage_freshness_visibility(coverage)
    criteria["source_gap_traceability"] = evaluate_source_gap_traceability(source_gap)
    criteria["symbol_change_review_gate"] = evaluate_symbol_change_review_gate(symbol_changes_review)
    criteria["ohlcv_plausibility_gate"] = evaluate_ohlcv_plausibility_gate(ohlcv)
    criteria["masterfile_collision_gate"] = evaluate_masterfile_collision_gate(masterfile_collision)
    criteria["otc_scope_gate"] = evaluate_otc_scope_gate(otc_scope)
    criteria["canada_figi_gate"] = evaluate_canada_figi_gate(
        canada_residual,
        canada_figi_queue,
        canada_figi_apply,
    )
    criteria["b3_residual_gate"] = evaluate_b3_residual_gate(b3_residual_isin, b3_residual_sector)
    criteria["asx_residual_gate"] = evaluate_asx_residual_gate(asx_residual)
    criteria["weak_sector_residual_gate"] = evaluate_weak_sector_residual_gate(weak_sector)
    criteria["before_after_delta_matrix"] = evaluate_before_after_delta_matrix(deltas)
    criteria["improvement_baseline_integrity"] = evaluate_improvement_baseline_integrity(baseline)
    criteria["campaign_reviewability"] = evaluate_campaign_reviewability(campaigns)
    criteria["next_review_batch_visibility"] = evaluate_next_review_batch_visibility(campaigns)
    criteria["next_review_command_safety_gate"] = evaluate_next_review_command_safety_gate(campaigns)
    criteria["campaign_closure_readiness_visibility"] = evaluate_campaign_closure_readiness_visibility(campaigns)
    criteria["campaign_closure_blocker_visibility"] = evaluate_campaign_closure_blocker_visibility(campaigns)
    criteria["campaign_review_policies"] = evaluate_campaign_review_policies(campaigns)
    criteria["campaign_baseline_alignment"] = evaluate_campaign_baseline_alignment(baseline, deltas)
    criteria["campaign_acceptance_matrices"] = evaluate_campaign_acceptance_matrices(campaigns)
    criteria["campaign_artifact_integrity"] = evaluate_campaign_artifact_integrity(campaigns)
    criteria["review_artifact_siblings"] = evaluate_review_artifact_siblings(campaigns)
    criteria["review_artifact_gates"] = evaluate_review_artifact_gates(campaigns)
    criteria["review_artifact_policy"] = evaluate_review_artifact_policy(campaigns)
    criteria["review_row_traceability"] = evaluate_review_row_traceability(campaigns)
    criteria["review_row_evidence"] = evaluate_review_row_evidence(campaigns)
    criteria["apply_artifact_traceability"] = evaluate_apply_artifact_traceability(campaigns)
    criteria["supplement_artifact_traceability"] = evaluate_supplement_artifact_traceability(campaigns)
    passed = all(bool(row.get("passed")) for row in criteria.values()) and bool(validation.get("passed"))
    summary = {
        "criteria": len(criteria),
        "passed_criteria": sum(1 for row in criteria.values() if row.get("passed")),
        "failed_criteria": sum(1 for row in criteria.values() if not row.get("passed")),
        "validation_failed_error_gates": validation.get("summary", {}).get("failed_error_gates", 0),
    }
    return {
        "_meta": {
            "generated_at": generated_at,
            "policy": "Acceptance summary only. It does not replace pytest or command execution output.",
            "source_reports": RELEASE_SOURCE_REPORTS,
        },
        "passed": passed,
        "summary": summary,
        "summary_context": release_summary_context(passed, summary),
        "criteria": criteria,
        "campaign_status_rows": campaign_status_rows(campaigns),
    }


def release_summary_context(passed: bool, summary: dict[str, Any]) -> str:
    return (
        f"passed={str(bool(passed)).lower()};"
        f"criteria={int(summary.get('criteria') or 0)};"
        f"passed_criteria={int(summary.get('passed_criteria') or 0)};"
        f"failed_criteria={int(summary.get('failed_criteria') or 0)};"
        f"validation_failed_error_gates={int(summary.get('validation_failed_error_gates') or 0)}"
    )


def markdown_cell(value: Any) -> str:
    return str(value if value is not None else "").replace("\n", " ").replace("|", "\\|")


def render_markdown(payload: dict[str, Any]) -> str:
    summary = payload.get("summary", {})
    if not isinstance(summary, dict):
        summary = {}
    lines = [
        "# Release Acceptance",
        "",
        f"Generated: `{payload['_meta']['generated_at']}`",
        "",
        f"Overall passed: `{payload['passed']}`",
        "",
        f"Summary context: `{markdown_cell(payload.get('summary_context'))}`",
        "",
        "## Summary",
        "",
        "| Metric | Value |",
        "|---|---:|",
    ]
    for key in ("criteria", "passed_criteria", "failed_criteria", "validation_failed_error_gates"):
        lines.append(f"| `{key}` | `{markdown_cell(summary.get(key))}` |")
    lines.extend([
        "",
        "| Criterion | Passed |",
        "|---|---:|",
    ])
    for key, value in payload["criteria"].items():
        lines.append(f"| `{key}` | {value.get('passed')} |")
    failed = {
        key: value
        for key, value in payload["criteria"].items()
        if not value.get("passed")
    }
    if failed:
        lines.extend(["", "## Failed Criteria", "", "| Criterion | Details |", "|---|---|"])
        for key, value in failed.items():
            details = {
                detail_key: detail_value
                for detail_key, detail_value in value.items()
                if detail_key != "passed" and detail_value not in ("", None, [], {})
            }
            lines.append(
                f"| `{markdown_cell(key)}` | `{markdown_cell(json.dumps(details, ensure_ascii=False, sort_keys=True))}` |"
            )
    lines.extend(["", "## Acceptance Delta Matrix", "", "| Metric | Baseline | Current | Delta |", "|---|---:|---:|---:|"])
    matrix = payload["criteria"]["before_after_delta_matrix"].get("acceptance_delta_matrix", {})
    for key, value in matrix.items():
        lines.append(f"| `{key}` | {value.get('baseline')} | {value.get('current')} | {value.get('delta')} |")
    campaign_status = payload.get("campaign_status_rows", [])
    if campaign_status:
        lines.extend(
            [
                "",
                "## Campaign Status",
                "",
                "| Priority | Campaign | Status | Delta Status | Next Action |",
                "|---:|---|---|---|---|",
            ]
        )
        for row in campaign_status:
            lines.append(
                "| "
                f"{markdown_cell(row.get('priority'))} | "
                f"`{markdown_cell(row.get('campaign_key'))}` | "
                f"`{markdown_cell(row.get('status'))}` | "
                f"`{markdown_cell(row.get('delta_status'))}` | "
                f"{markdown_cell(row.get('next_action'))} |"
            )
    campaign_matrices = payload["criteria"].get("campaign_acceptance_matrices", {})
    campaign_gap_keys = sorted(
        key for key in campaign_matrices if key.endswith("_campaign_evidence_gaps")
    )
    if campaign_gap_keys:
        lines.extend(
            [
                "",
                "## Campaign Evidence Gates",
                "",
                "| Guard | Gap Count |",
                "|---|---:|",
            ]
        )
        for key in campaign_gap_keys:
            gaps = campaign_matrices.get(key, [])
            gap_count = len(gaps) if isinstance(gaps, list) else "invalid"
            lines.append(f"| `{key}` | {gap_count} |")
            if isinstance(gaps, list) and gaps:
                preview = json.dumps(gaps[:3], ensure_ascii=False, sort_keys=True)
                lines.append(f"| `{key} preview` | `{preview}` |")
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build release acceptance summary from validation and improvement reports.")
    parser.add_argument("--json-out", type=Path, default=DEFAULT_JSON_OUT)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_MD_OUT)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    payload = build_payload()
    args.json_out.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    args.md_out.write_text(render_markdown(payload) + "\n", encoding="utf-8")
    print(json.dumps({"passed": payload["passed"], **payload["summary"]}, indent=2))


if __name__ == "__main__":
    main()

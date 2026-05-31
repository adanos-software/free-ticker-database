from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
REPORTS_DIR = DATA_DIR / "reports"
REVIEW_OVERRIDES_DIR = DATA_DIR / "review_overrides"

DEFAULT_TICKERS_CSV = DATA_DIR / "tickers.csv"
DEFAULT_INSTRUMENT_SCOPES_CSV = DATA_DIR / "instrument_scopes.csv"
DEFAULT_ENTRY_QUALITY_CSV = REPORTS_DIR / "entry_quality.csv"
DEFAULT_SOURCE_GAP_CLASSIFICATION_CSV = REPORTS_DIR / "source_gap_classification.csv"
DEFAULT_SOURCE_OF_TRUTH_DECISIONS_CSV = REPORTS_DIR / "source_of_truth_decisions.csv"
DEFAULT_DROP_ENTRIES_CSV = REVIEW_OVERRIDES_DIR / "drop_entries.csv"
DEFAULT_OTC_REVIEW_DECISIONS_CSV = REVIEW_OVERRIDES_DIR / "otc_review_decisions.csv"
DEFAULT_CSV_OUT = REPORTS_DIR / "otc_scope_review.csv"
DEFAULT_JSON_OUT = REPORTS_DIR / "otc_scope_review.json"
DEFAULT_MD_OUT = REPORTS_DIR / "otc_scope_review.md"

CSV_FIELDNAMES = [
    "listing_key",
    "ticker",
    "exchange",
    "asset_type",
    "name",
    "instrument_scope",
    "scope_reason",
    "quality_status",
    "issue_types",
    "source_gap_field",
    "source_gap_class",
    "source_of_truth_outcome",
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
]


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def listing_key(row: dict[str, str]) -> str:
    return row.get("listing_key") or f"{row.get('exchange', '')}::{row.get('ticker', '')}"


def build_lookup(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    return {listing_key(row): row for row in rows if listing_key(row) != "::"}


def build_multi_lookup(rows: list[dict[str, str]]) -> dict[str, list[dict[str, str]]]:
    grouped: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        key = listing_key(row)
        if key != "::":
            grouped.setdefault(key, []).append(row)
    return grouped


def otc_drop_keys(drop_rows: list[dict[str, str]]) -> set[str]:
    return {
        f"{row.get('exchange', '')}::{row.get('ticker', '')}"
        for row in drop_rows
        if row.get("exchange") == "OTC" and row.get("ticker")
    }


def otc_review_decision_keys(review_rows: list[dict[str, str]]) -> set[str]:
    return {
        f"{row.get('exchange', '')}::{row.get('ticker', '')}"
        for row in review_rows
        if row.get("exchange") == "OTC" and row.get("ticker")
    }


def scope_decision_for(scope: dict[str, str], source_of_truth_rows: list[dict[str, str]]) -> str:
    if scope.get("instrument_scope") == "core":
        return "unexpected_otc_core_scope_review_required"
    if any(row.get("source_of_truth_outcome") == "core_exclusion_candidate" for row in source_of_truth_rows):
        return "core_exclusion_candidate_requires_review"
    return "already_extended_otc_listing"


def recommended_action_for(row: dict[str, str], gap_rows: list[dict[str, str]]) -> str:
    if row["scope_decision"] == "unexpected_otc_core_scope_review_required":
        return "review_scope_override_before_metadata_enrichment"
    if row["scope_decision"] == "core_exclusion_candidate_requires_review":
        return "review_official_instrument_type_before_fill_or_drop"
    if "official_name_mismatch" in row["issue_types"].split("|"):
        return "use_otc_name_mismatch_review_before_name_changes"
    if gap_rows:
        return "leave_blank_as_documented_source_gap_until_reviewed_source"
    return "keep_extended_scope"


def review_bucket_for(row: dict[str, str], gap_rows: list[dict[str, str]]) -> str:
    if row["scope_decision"] == "unexpected_otc_core_scope_review_required":
        return "unexpected_core_scope_requires_review"
    if row["scope_decision"] == "core_exclusion_candidate_requires_review":
        return "core_exclusion_candidate_scope_review"
    if "official_name_mismatch" in row["issue_types"].split("|"):
        return "official_name_mismatch_review_first"
    if gap_rows:
        if row["source_gap_field"] == "missing_sector_stock":
            return "documented_otc_sector_source_gap"
        if row["source_gap_field"] == "missing_etf_category":
            return "documented_otc_category_source_gap"
        return "documented_otc_source_gap"
    if row["quality_status"] == "warn":
        return "otc_quality_warn_review"
    if row["quality_status"] == "source_gap":
        return "otc_quality_source_gap_review"
    return "clean_extended_otc_listing"


def review_priority_for(review_bucket: str) -> str:
    if review_bucket in {
        "unexpected_core_scope_requires_review",
        "core_exclusion_candidate_scope_review",
    }:
        return "P1"
    if review_bucket in {
        "official_name_mismatch_review_first",
        "otc_quality_warn_review",
    }:
        return "P2"
    if review_bucket in {
        "documented_otc_sector_source_gap",
        "documented_otc_category_source_gap",
        "documented_otc_source_gap",
        "otc_quality_source_gap_review",
    }:
        return "P3"
    return "P4"


def scope_apply_eligibility_for(review_bucket: str) -> str:
    if review_bucket == "unexpected_core_scope_requires_review":
        return "blocked_until_otc_core_scope_override_reviewed"
    if review_bucket == "core_exclusion_candidate_scope_review":
        return "blocked_until_core_exclusion_candidate_scope_decision"
    return "already_extended_no_scope_change_required"


def metadata_enrichment_gate_for(row: dict[str, str], gap_rows: list[dict[str, str]]) -> str:
    if row["scope_decision"] in {
        "unexpected_otc_core_scope_review_required",
        "core_exclusion_candidate_requires_review",
    }:
        return "scope_decision_required_before_any_metadata_enrichment"
    if "official_name_mismatch" in row["issue_types"].split("|"):
        return "otc_name_mismatch_review_required_before_name_or_metadata_changes"
    if gap_rows:
        if row["source_gap_field"] == "missing_sector_stock":
            return "reviewed_issuer_sector_source_required_keep_blank"
        if row["source_gap_field"] == "missing_etf_category":
            return "reviewed_product_category_source_required_keep_blank"
        return "reviewed_source_required_keep_blank"
    if row["quality_status"] == "warn":
        return "entry_quality_warn_review_required_before_enrichment"
    if row["quality_status"] == "source_gap":
        return "source_gap_review_required_before_enrichment"
    return "no_metadata_enrichment_needed"


def review_strategy_for(review_bucket: str) -> str:
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


def verification_evidence_required_for(review_bucket: str) -> str:
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


def recommended_next_source_for(review_bucket: str) -> str:
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


def source_gate_for(review_bucket: str) -> str:
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


def source_gap_context_for(row: dict[str, str]) -> str:
    return (
        f"quality_status={row.get('quality_status', '') or 'none'};"
        f"issue_types={row.get('issue_types', '') or 'none'};"
        f"source_gap_field={row.get('source_gap_field', '') or 'none'};"
        f"source_gap_class={row.get('source_gap_class', '') or 'none'};"
        f"source_of_truth_outcome={row.get('source_of_truth_outcome', '') or 'none'}"
    )


def otc_review_decision_context_for(row: dict[str, str]) -> str:
    return (
        f"listing_key={row.get('listing_key', '') or 'none'};"
        f"otc_review_decision_status={row.get('otc_review_decision_status', '') or 'none'}"
    )


def scope_review_context_for(row: dict[str, str]) -> str:
    return (
        f"instrument_scope={row.get('instrument_scope', '') or 'none'};"
        f"scope_reason={row.get('scope_reason', '') or 'none'};"
        f"scope_decision={row.get('scope_decision', '') or 'none'};"
        f"scope_apply_eligibility={row.get('scope_apply_eligibility', '') or 'none'};"
        f"metadata_enrichment_gate={row.get('metadata_enrichment_gate', '') or 'none'}"
    )


def post_scope_metadata_example_for(row: dict[str, str]) -> dict[str, str]:
    return {
        "listing_key": row["listing_key"],
        "ticker": row.get("ticker", ""),
        "asset_type": row.get("asset_type", "") or "unknown",
        "name": row.get("name", ""),
        "review_bucket": row["review_bucket"],
        "quality_status": row.get("quality_status", "") or "blank",
        "metadata_enrichment_gate": row["metadata_enrichment_gate"],
        "review_strategy": row["review_strategy"],
        "verification_evidence_required": row["verification_evidence_required"],
        "recommended_next_source": row["recommended_next_source"],
        "source_gate": row["source_gate"],
    }


def build_review_rows(
    *,
    tickers: list[dict[str, str]],
    instrument_scopes: list[dict[str, str]],
    entry_quality_rows: list[dict[str, str]],
    source_gap_rows: list[dict[str, str]],
    source_of_truth_rows: list[dict[str, str]],
    otc_review_decision_rows: list[dict[str, str]] | None = None,
) -> list[dict[str, str]]:
    tickers_lookup = build_lookup(tickers)
    entry_quality_lookup = build_lookup(entry_quality_rows)
    source_gap_lookup = build_multi_lookup(source_gap_rows)
    source_of_truth_lookup = build_multi_lookup(source_of_truth_rows)
    reviewed_keys = otc_review_decision_keys(otc_review_decision_rows or [])
    rows: list[dict[str, str]] = []
    for scope in instrument_scopes:
        if not listing_key(scope).startswith("OTC::"):
            continue
        key = listing_key(scope)
        ticker_row = tickers_lookup.get(key, {})
        quality_row = entry_quality_lookup.get(key, {})
        gap_rows = source_gap_lookup.get(key, [])
        truth_rows = source_of_truth_lookup.get(key, [])
        gap_fields = "|".join(sorted({row.get("field", "") for row in gap_rows if row.get("field")}))
        gap_classes = "|".join(sorted({row.get("gap_class", "") for row in gap_rows if row.get("gap_class")}))
        truth_outcomes = "|".join(
            sorted({row.get("source_of_truth_outcome", "") for row in truth_rows if row.get("source_of_truth_outcome")})
        )
        row = {
            "listing_key": key,
            "ticker": scope.get("ticker", "") or ticker_row.get("ticker", ""),
            "exchange": "OTC",
            "asset_type": scope.get("asset_type", "") or ticker_row.get("asset_type", ""),
            "name": scope.get("name", "") or ticker_row.get("name", ""),
            "instrument_scope": scope.get("instrument_scope", ""),
            "scope_reason": scope.get("scope_reason", ""),
            "quality_status": quality_row.get("quality_status", ""),
            "issue_types": quality_row.get("issue_types", ""),
            "source_gap_field": gap_fields,
            "source_gap_class": gap_classes,
            "source_of_truth_outcome": truth_outcomes,
            "source_gap_context": "",
            "scope_decision": scope_decision_for(scope, truth_rows),
            "otc_review_decision_status": "",
            "otc_review_decision_context": "",
            "review_bucket": "",
            "review_priority": "",
            "scope_apply_eligibility": "",
            "metadata_enrichment_gate": "",
            "scope_review_context": "",
            "review_strategy": "",
            "verification_evidence_required": "",
            "recommended_next_source": "",
            "source_gate": "",
            "recommended_action": "",
        }
        row["review_bucket"] = review_bucket_for(row, gap_rows)
        if row["review_bucket"] == "official_name_mismatch_review_first":
            row["otc_review_decision_status"] = (
                "reviewed_name_mismatch_decision_present"
                if key in reviewed_keys
                else "pending_otc_name_mismatch_review"
            )
        else:
            row["otc_review_decision_status"] = "not_applicable"
        row["review_priority"] = review_priority_for(row["review_bucket"])
        row["scope_apply_eligibility"] = scope_apply_eligibility_for(row["review_bucket"])
        row["metadata_enrichment_gate"] = metadata_enrichment_gate_for(row, gap_rows)
        row["review_strategy"] = review_strategy_for(row["review_bucket"])
        row["verification_evidence_required"] = verification_evidence_required_for(row["review_bucket"])
        row["recommended_next_source"] = recommended_next_source_for(row["review_bucket"])
        row["source_gate"] = source_gate_for(row["review_bucket"])
        row["recommended_action"] = recommended_action_for(row, gap_rows)
        row["source_gap_context"] = source_gap_context_for(row)
        row["otc_review_decision_context"] = otc_review_decision_context_for(row)
        row["scope_review_context"] = scope_review_context_for(row)
        rows.append(row)
    return sorted(rows, key=lambda row: (row["review_priority"], row["review_bucket"], row["quality_status"], row["ticker"], row["listing_key"]))


def summarize(
    rows: list[dict[str, str]],
    *,
    generated_at: str,
    drop_rows: list[dict[str, str]],
    otc_review_decision_rows: list[dict[str, str]],
) -> dict[str, Any]:
    drop_keys = otc_drop_keys(drop_rows)
    reviewed_keys = otc_review_decision_keys(otc_review_decision_rows)
    row_keys = {row["listing_key"] for row in rows}
    active_name_mismatch_keys = {
        row["listing_key"]
        for row in rows
        if row["review_bucket"] == "official_name_mismatch_review_first"
    }
    reviewed_active_name_mismatch_keys = active_name_mismatch_keys & reviewed_keys
    reviewed_current_suppressed_keys = (reviewed_keys & row_keys) - active_name_mismatch_keys
    review_decision_not_current_scope_keys = reviewed_keys - row_keys
    stale_review_decision_keys = reviewed_current_suppressed_keys | review_decision_not_current_scope_keys
    review_decision_resolution_totals = {
        "pending_active_name_mismatch_review": len(active_name_mismatch_keys - reviewed_keys),
        "reviewed_decision_covers_active_name_mismatch": len(reviewed_active_name_mismatch_keys),
        "reviewed_decision_suppresses_current_listing_warning": len(reviewed_current_suppressed_keys),
        "reviewed_decision_not_in_current_otc_scope": len(review_decision_not_current_scope_keys),
    }
    strategy_counter = Counter(review_strategy_for(row["review_bucket"]) for row in rows)
    evidence_counter = Counter(verification_evidence_required_for(row["review_bucket"]) for row in rows)
    top_batch_counter: Counter[tuple[str, str, str, str, str]] = Counter(
        (
            row["review_priority"],
            row["review_bucket"],
            row.get("asset_type", "") or "unknown",
            row["quality_status"] or "blank",
            row["metadata_enrichment_gate"],
        )
        for row in rows
    )
    core_exclusion_candidate_rows = [
        row
        for row in rows
        if row["scope_decision"] == "core_exclusion_candidate_requires_review"
        or row["review_bucket"] == "core_exclusion_candidate_scope_review"
    ]
    post_scope_metadata_rows = [
        row
        for row in rows
        if row["review_bucket"] != "clean_extended_otc_listing"
        and row["scope_apply_eligibility"] == "already_extended_no_scope_change_required"
    ]
    instrument_scope_totals = dict(sorted(Counter(row["instrument_scope"] for row in rows).items()))
    scope_reason_totals = dict(sorted(Counter(row["scope_reason"] for row in rows).items()))
    scope_decision_totals = dict(sorted(Counter(row["scope_decision"] for row in rows).items()))
    scope_apply_eligibility_totals = dict(sorted(Counter(row["scope_apply_eligibility"] for row in rows).items()))
    unexpected_core_rows = scope_decision_totals.get("unexpected_otc_core_scope_review_required", 0)
    blocked_scope_decision_rows = sum(
        count
        for eligibility, count in scope_apply_eligibility_totals.items()
        if str(eligibility).startswith("blocked_until_")
    )
    all_extended_otc = (
        len(rows) > 0
        and instrument_scope_totals == {"extended": len(rows)}
        and scope_reason_totals == {"otc_listing": len(rows)}
        and scope_decision_totals == {"already_extended_otc_listing": len(rows)}
        and scope_apply_eligibility_totals == {"already_extended_no_scope_change_required": len(rows)}
    )
    otc_scope_completion = {
        "status": "complete_extended_scope_no_core_candidates" if all_extended_otc else "scope_review_open",
        "rows": len(rows),
        "extended_otc_rows": instrument_scope_totals.get("extended", 0),
        "otc_listing_scope_reason_rows": scope_reason_totals.get("otc_listing", 0),
        "already_extended_scope_decision_rows": scope_decision_totals.get("already_extended_otc_listing", 0),
        "core_exclusion_candidate_rows": len(core_exclusion_candidate_rows),
        "unexpected_core_scope_rows": unexpected_core_rows,
        "blocked_scope_decision_rows": blocked_scope_decision_rows,
        "scope_apply_allowed_rows": 0,
        "metadata_enrichment_authorized": False,
        "source_gate": (
            "OTC scope is complete only when every current OTC row is extended/otc_listing and no core or "
            "core-exclusion scope decision remains open; metadata still requires listing-keyed evidence."
        ),
    }
    return {
        "generated_at": generated_at,
        "rows": len(rows),
        "scope_decision_totals": scope_decision_totals,
        "instrument_scope_totals": instrument_scope_totals,
        "scope_reason_totals": scope_reason_totals,
        "quality_status_totals": dict(sorted(Counter(row["quality_status"] for row in rows).items())),
        "review_bucket_totals": dict(sorted(Counter(row["review_bucket"] for row in rows).items())),
        "review_bucket_asset_type_totals": {
            bucket: dict(
                sorted(
                    Counter(
                        row.get("asset_type", "") or "unknown"
                        for row in rows
                        if row["review_bucket"] == bucket
                    ).items()
                )
            )
            for bucket in sorted({row["review_bucket"] for row in rows})
        },
        "review_bucket_metadata_gate_totals": {
            bucket: dict(
                sorted(
                    Counter(
                        row["metadata_enrichment_gate"]
                        for row in rows
                        if row["review_bucket"] == bucket
                    ).items()
                )
            )
            for bucket in sorted({row["review_bucket"] for row in rows})
        },
        "review_priority_totals": dict(sorted(Counter(row["review_priority"] for row in rows).items())),
        "scope_apply_eligibility_totals": scope_apply_eligibility_totals,
        "otc_scope_completion": otc_scope_completion,
        "post_scope_metadata_backlog": {
            "status": "metadata_review_backlog_open" if post_scope_metadata_rows else "no_post_scope_metadata_backlog",
            "rows": len(post_scope_metadata_rows),
            "metadata_enrichment_authorized": False,
            "scope_blocked_rows": blocked_scope_decision_rows,
            "source_gate": (
                "Post-scope OTC metadata work remains blocked unless each row has listing-keyed OTC Markets, "
                "issuer, SEC, registry, or reviewed fallback evidence; no ticker-only enrichment is allowed."
            ),
        },
        "post_scope_metadata_backlog_bucket_totals": dict(
            sorted(Counter(row["review_bucket"] for row in post_scope_metadata_rows).items())
        ),
        "post_scope_metadata_backlog_gate_totals": dict(
            sorted(Counter(row["metadata_enrichment_gate"] for row in post_scope_metadata_rows).items())
        ),
        "post_scope_metadata_backlog_examples": [
            post_scope_metadata_example_for(row)
            for row in sorted(
                post_scope_metadata_rows,
                key=lambda row: (
                    row["review_priority"],
                    row["review_bucket"],
                    row.get("quality_status", ""),
                    row.get("ticker", ""),
                    row["listing_key"],
                ),
            )[:25]
        ],
        "metadata_enrichment_gate_totals": dict(sorted(Counter(row["metadata_enrichment_gate"] for row in rows).items())),
        "review_strategy_totals": dict(sorted(strategy_counter.items())),
        "verification_evidence_required_totals": dict(sorted(evidence_counter.items())),
        "top_otc_scope_review_batches": [
            {
                "review_priority": priority,
                "review_bucket": bucket,
                "asset_type": asset_type,
                "quality_status": quality_status,
                "metadata_enrichment_gate": metadata_gate,
                "rows": count,
                "review_strategy": review_strategy_for(bucket),
                "verification_evidence_required": verification_evidence_required_for(bucket),
                "recommended_next_source": recommended_next_source_for(bucket),
                "source_gate": source_gate_for(bucket),
            }
            for (priority, bucket, asset_type, quality_status, metadata_gate), count in sorted(
                top_batch_counter.items(),
                key=lambda item: (-item[1], item[0][0], item[0][1], item[0][2], item[0][3], item[0][4]),
            )[:10]
        ],
        "source_gap_field_totals": dict(
            sorted(Counter(field for row in rows for field in row["source_gap_field"].split("|") if field).items())
        ),
        "source_gap_class_totals": dict(
            sorted(Counter(gap_class for row in rows for gap_class in row["source_gap_class"].split("|") if gap_class).items())
        ),
        "source_of_truth_outcome_totals": dict(
            sorted(
                Counter(
                    outcome for row in rows for outcome in row["source_of_truth_outcome"].split("|") if outcome
                ).items()
            )
        ),
        "otc_core_exclusion_candidate_rows": len(core_exclusion_candidate_rows),
        "otc_core_exclusion_candidate_asset_type_totals": dict(
            sorted(Counter(row.get("asset_type", "") or "unknown" for row in core_exclusion_candidate_rows).items())
        ),
        "otc_core_exclusion_candidate_quality_status_totals": dict(
            sorted(Counter(row.get("quality_status", "") or "blank" for row in core_exclusion_candidate_rows).items())
        ),
        "otc_core_exclusion_candidate_metadata_gate_totals": dict(
            sorted(Counter(row["metadata_enrichment_gate"] for row in core_exclusion_candidate_rows).items())
        ),
        "otc_core_exclusion_candidate_review_examples": [
            {
                "listing_key": row["listing_key"],
                "ticker": row["ticker"],
                "asset_type": row.get("asset_type", "") or "unknown",
                "quality_status": row.get("quality_status", "") or "blank",
                "scope_decision": row["scope_decision"],
                "review_bucket": row["review_bucket"],
                "metadata_enrichment_gate": row["metadata_enrichment_gate"],
                "recommended_action": row["recommended_action"],
                "source_gate": row["source_gate"],
            }
            for row in core_exclusion_candidate_rows[:20]
        ],
        "drop_override_rows": len(drop_keys),
        "drop_override_rows_still_present": len(drop_keys & row_keys),
        "otc_review_decision_rows": len(reviewed_keys),
        "otc_review_decision_active_name_mismatch_rows": len(reviewed_active_name_mismatch_keys),
        "otc_name_mismatch_unreviewed_active_rows": len(active_name_mismatch_keys - reviewed_keys),
        "otc_review_decision_resolution_totals": {
            key: value for key, value in review_decision_resolution_totals.items() if value
        },
        "otc_review_decision_current_listing_suppressed_rows": len(reviewed_current_suppressed_keys),
        "otc_review_decision_current_listing_suppressed_examples": sorted(reviewed_current_suppressed_keys)[:20],
        "otc_review_decision_not_current_scope_rows": len(review_decision_not_current_scope_keys),
        "otc_review_decision_not_current_scope_examples": sorted(review_decision_not_current_scope_keys)[:20],
        "otc_review_decision_stale_rows": len(stale_review_decision_keys),
        "otc_review_decision_stale_examples": sorted(stale_review_decision_keys)[:20],
        "policy": {
            "otc_scope": "OTC listings are extended scope unless a reviewed future scope change explicitly says otherwise.",
            "no_blind_sector_enrichment": "OTC source gaps remain blank; sector/category fills require reviewed issuer or product evidence.",
        },
    }


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDNAMES, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def write_markdown(path: Path, summary: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# OTC Scope Review",
        "",
        f"Generated at: `{summary['generated_at']}`",
        "",
        "This report verifies OTC listing scope before any OTC metadata enrichment. It does not change scope or fill fields.",
        "",
        "## Summary",
        "",
        f"- OTC listing rows: `{summary['rows']}`",
        f"- OTC drop overrides already removed from current rows: `{summary['drop_override_rows'] - summary['drop_override_rows_still_present']}`",
        f"- OTC reviewed name decisions: `{summary['otc_review_decision_rows']}`",
        f"- Active OTC name mismatches already covered by reviewed decisions: `{summary['otc_review_decision_active_name_mismatch_rows']}`",
        f"- Active OTC name mismatches still unreviewed: `{summary['otc_name_mismatch_unreviewed_active_rows']}`",
        f"- Reviewed OTC decisions suppressing current listing warnings: `{summary['otc_review_decision_current_listing_suppressed_rows']}`",
        f"- Reviewed OTC decisions outside current OTC scope: `{summary['otc_review_decision_not_current_scope_rows']}`",
        f"- OTC core-exclusion candidates requiring scope decision: `{summary['otc_core_exclusion_candidate_rows']}`",
        f"- Post-scope OTC metadata backlog rows: `{summary['post_scope_metadata_backlog']['rows']}`",
        "",
        "## OTC Review Decision Resolution",
        "",
        "| Queue | Rows |",
        "|---|---:|",
    ]
    for queue, count in summary["otc_review_decision_resolution_totals"].items():
        lines.append(f"| {queue} | {count} |")
    lines.extend([
        "",
        "## Scope Decisions",
        "",
        "| Decision | Rows |",
        "|---|---:|",
    ])
    for decision, count in summary["scope_decision_totals"].items():
        lines.append(f"| {decision} | {count} |")
    completion = summary["otc_scope_completion"]
    lines.extend(
        [
            "",
            "## Scope Completion",
            "",
            "| Metric | Value |",
            "|---|---|",
            f"| status | {completion['status']} |",
            f"| rows | {completion['rows']} |",
            f"| extended_otc_rows | {completion['extended_otc_rows']} |",
            f"| otc_listing_scope_reason_rows | {completion['otc_listing_scope_reason_rows']} |",
            f"| already_extended_scope_decision_rows | {completion['already_extended_scope_decision_rows']} |",
            f"| core_exclusion_candidate_rows | {completion['core_exclusion_candidate_rows']} |",
            f"| unexpected_core_scope_rows | {completion['unexpected_core_scope_rows']} |",
            f"| blocked_scope_decision_rows | {completion['blocked_scope_decision_rows']} |",
            f"| scope_apply_allowed_rows | {completion['scope_apply_allowed_rows']} |",
            f"| metadata_enrichment_authorized | {completion['metadata_enrichment_authorized']} |",
            f"| source_gate | {completion['source_gate']} |",
        ]
    )
    backlog = summary["post_scope_metadata_backlog"]
    lines.extend(
        [
            "",
            "## Post-Scope Metadata Backlog",
            "",
            "| Metric | Value |",
            "|---|---|",
            f"| status | {backlog['status']} |",
            f"| rows | {backlog['rows']} |",
            f"| scope_blocked_rows | {backlog['scope_blocked_rows']} |",
            f"| metadata_enrichment_authorized | {backlog['metadata_enrichment_authorized']} |",
            f"| source_gate | {backlog['source_gate']} |",
            "",
            "| Review bucket | Rows |",
            "|---|---:|",
        ]
    )
    for bucket, count in summary["post_scope_metadata_backlog_bucket_totals"].items():
        lines.append(f"| {bucket} | {count} |")
    lines.extend(["", "| Metadata gate | Rows |", "|---|---:|"])
    for gate, count in summary["post_scope_metadata_backlog_gate_totals"].items():
        lines.append(f"| {gate} | {count} |")
    lines.extend(
        [
            "",
            "| Listing key | Ticker | Asset type | Bucket | Quality | Metadata gate | Evidence required | Recommended source | Source gate |",
            "|---|---|---|---|---|---|---|---|---|",
        ]
    )
    for example in summary["post_scope_metadata_backlog_examples"]:
        lines.append(
            f"| {example['listing_key']} | {example['ticker']} | {example['asset_type']} | "
            f"{example['review_bucket']} | {example['quality_status']} | "
            f"{example['metadata_enrichment_gate']} | {example['verification_evidence_required']} | "
            f"{example['recommended_next_source']} | {example['source_gate']} |"
        )
    lines.extend(
        [
            "",
            "## Core-Exclusion Scope Gate",
            "",
            "Core-exclusion candidates are blocked from identifier, name, sector, and category enrichment until a reviewed scope decision selects `core`, `extended`, or `exclude`.",
            "",
            "| Metric | Rows |",
            "|---|---:|",
            f"| otc_core_exclusion_candidate_rows | {summary['otc_core_exclusion_candidate_rows']} |",
            "",
            "| Asset type | Rows |",
            "|---|---:|",
        ]
    )
    for asset_type, count in summary["otc_core_exclusion_candidate_asset_type_totals"].items():
        lines.append(f"| {asset_type} | {count} |")
    lines.extend(["", "| Metadata gate | Rows |", "|---|---:|"])
    for gate, count in summary["otc_core_exclusion_candidate_metadata_gate_totals"].items():
        lines.append(f"| {gate} | {count} |")
    lines.extend(
        [
            "",
            "| Listing key | Ticker | Asset type | Quality | Metadata gate | Action |",
            "|---|---|---|---|---|---|",
        ]
    )
    for example in summary["otc_core_exclusion_candidate_review_examples"]:
        lines.append(
            f"| {example['listing_key']} | {example['ticker']} | {example['asset_type']} | "
            f"{example['quality_status']} | {example['metadata_enrichment_gate']} | "
            f"{example['recommended_action']} |"
        )
    lines.extend(["", "## Quality Status", "", "| Status | Rows |", "|---|---:|"])
    for status, count in summary["quality_status_totals"].items():
        lines.append(f"| {status or 'blank'} | {count} |")
    lines.extend(["", "## Review Priorities", "", "| Priority | Rows |", "|---|---:|"])
    for priority, count in summary["review_priority_totals"].items():
        lines.append(f"| {priority} | {count} |")
    lines.extend(["", "## Review Buckets", "", "| Bucket | Rows |", "|---|---:|"])
    for bucket, count in summary["review_bucket_totals"].items():
        lines.append(f"| {bucket} | {count} |")
    lines.extend(["", "## Review Bucket By Asset Type", "", "| Bucket | Asset Type | Rows |", "|---|---|---:|"])
    for bucket, asset_type_totals in summary["review_bucket_asset_type_totals"].items():
        for asset_type, count in asset_type_totals.items():
            lines.append(f"| {bucket} | {asset_type} | {count} |")
    lines.extend(["", "## Review Bucket By Metadata Gate", "", "| Bucket | Metadata gate | Rows |", "|---|---|---:|"])
    for bucket, metadata_gate_totals in summary["review_bucket_metadata_gate_totals"].items():
        for gate, count in metadata_gate_totals.items():
            lines.append(f"| {bucket} | {gate} | {count} |")
    lines.extend(["", "## Scope Apply Eligibility", "", "| Eligibility | Rows |", "|---|---:|"])
    for eligibility, count in summary["scope_apply_eligibility_totals"].items():
        lines.append(f"| {eligibility} | {count} |")
    lines.extend(["", "## Metadata Enrichment Gates", "", "| Gate | Rows |", "|---|---:|"])
    for gate, count in summary["metadata_enrichment_gate_totals"].items():
        lines.append(f"| {gate} | {count} |")
    lines.extend(["", "## Review Strategies", "", "| Strategy | Rows |", "|---|---:|"])
    for strategy, count in summary["review_strategy_totals"].items():
        lines.append(f"| {strategy} | {count} |")
    lines.extend(["", "## Verification Evidence", "", "| Evidence required | Rows |", "|---|---:|"])
    for evidence, count in summary["verification_evidence_required_totals"].items():
        lines.append(f"| {evidence} | {count} |")
    lines.extend(
        [
            "",
            "## Top Review Batches",
            "",
            "| Priority | Bucket | Asset type | Quality status | Metadata gate | Rows | Strategy | Evidence required | Recommended next source | Source gate |",
            "|---|---|---|---|---|---:|---|---|---|---|",
        ]
    )
    for batch in summary["top_otc_scope_review_batches"]:
        lines.append(
            f"| {batch['review_priority']} | {batch['review_bucket']} | {batch['asset_type']} | "
            f"{batch['quality_status']} | {batch['metadata_enrichment_gate']} | {batch['rows']} | "
            f"{batch['review_strategy']} | {batch['verification_evidence_required']} | "
            f"{batch['recommended_next_source']} | {batch['source_gate']} |"
        )
    lines.extend(["", "## Source Gap Fields", "", "| Field | Rows |", "|---|---:|"])
    for field, count in summary["source_gap_field_totals"].items():
        lines.append(f"| {field} | {count} |")
    lines.extend(
        [
            "",
            "## Policy",
            "",
            "- OTC rows stay `extended/otc_listing`; unexpected OTC core rows require scope review before release.",
            "- OTC sector/category blanks are source gaps, not an invitation for symbol-only or name-shape enrichment.",
            "- Name warnings route through the OTC name mismatch review before any canonical name change.",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build an OTC listing-keyed scope review report.")
    parser.add_argument("--tickers-csv", type=Path, default=DEFAULT_TICKERS_CSV)
    parser.add_argument("--instrument-scopes-csv", type=Path, default=DEFAULT_INSTRUMENT_SCOPES_CSV)
    parser.add_argument("--entry-quality-csv", type=Path, default=DEFAULT_ENTRY_QUALITY_CSV)
    parser.add_argument("--source-gap-classification-csv", type=Path, default=DEFAULT_SOURCE_GAP_CLASSIFICATION_CSV)
    parser.add_argument("--source-of-truth-decisions-csv", type=Path, default=DEFAULT_SOURCE_OF_TRUTH_DECISIONS_CSV)
    parser.add_argument("--drop-entries-csv", type=Path, default=DEFAULT_DROP_ENTRIES_CSV)
    parser.add_argument("--otc-review-decisions-csv", type=Path, default=DEFAULT_OTC_REVIEW_DECISIONS_CSV)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_CSV_OUT)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_JSON_OUT)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_MD_OUT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    rows = build_review_rows(
        tickers=load_csv(args.tickers_csv),
        instrument_scopes=load_csv(args.instrument_scopes_csv),
        entry_quality_rows=load_csv(args.entry_quality_csv),
        source_gap_rows=load_csv(args.source_gap_classification_csv),
        source_of_truth_rows=load_csv(args.source_of_truth_decisions_csv),
        otc_review_decision_rows=load_csv(args.otc_review_decisions_csv),
    )
    summary = summarize(
        rows,
        generated_at=utc_now_iso(),
        drop_rows=load_csv(args.drop_entries_csv),
        otc_review_decision_rows=load_csv(args.otc_review_decisions_csv),
    )
    write_csv(args.csv_out, rows)
    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.json_out.write_text(
        json.dumps({"summary": summary, "rows": rows}, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    write_markdown(args.md_out, summary)
    print(
        json.dumps(
            {
                "rows": len(rows),
                "csv_out": display_path(args.csv_out),
                "json_out": display_path(args.json_out),
                "md_out": display_path(args.md_out),
                "scope_decision_totals": summary["scope_decision_totals"],
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()

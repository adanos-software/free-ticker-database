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
ASX_VERIFICATION_DIR = DATA_DIR / "asx_verification"

DEFAULT_TICKERS_CSV = DATA_DIR / "tickers.csv"
DEFAULT_MASTERFILE_REFERENCE_CSV = DATA_DIR / "masterfiles" / "reference.csv"
DEFAULT_SOURCE_GAP_CLASSIFICATION_CSV = REPORTS_DIR / "source_gap_classification.csv"
DEFAULT_SOURCE_OF_TRUTH_DECISIONS_CSV = REPORTS_DIR / "source_of_truth_decisions.csv"
DEFAULT_ASX_ISIN_PROBE_CSV = ASX_VERIFICATION_DIR / "missing_isin_backfill.csv"
DEFAULT_CSV_OUT = REPORTS_DIR / "asx_residual_review.csv"
DEFAULT_JSON_OUT = REPORTS_DIR / "asx_residual_review.json"
DEFAULT_MD_OUT = REPORTS_DIR / "asx_residual_review.md"

REVIEW_FIELDS = {"missing_isin_primary", "missing_etf_category"}

CSV_FIELDNAMES = [
    "listing_key",
    "ticker",
    "exchange",
    "asset_type",
    "name",
    "field",
    "target_field",
    "gap_class",
    "source_of_truth_outcome",
    "core_action",
    "fill_action",
    "review_needed",
    "recommended_next_source",
    "source_gate",
    "official_source_context",
    "official_capability",
    "official_masterfile_match",
    "official_masterfile_sources",
    "official_masterfile_exposes_isin",
    "official_masterfile_exposes_sector",
    "asx_isin_probe_decision",
    "asx_isin_probe_isin",
    "asx_isin_probe_security_type",
    "asx_resolution_queue",
    "residual_decision",
    "review_bucket",
    "review_priority",
    "review_strategy",
    "apply_eligibility",
    "verification_evidence_required",
    "recommended_next_action",
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


def build_truth_lookup(rows: list[dict[str, str]]) -> dict[tuple[str, str], dict[str, str]]:
    return {(row.get("field", ""), listing_key(row)): row for row in rows if row.get("exchange") == "ASX"}


def build_masterfile_lookup(rows: list[dict[str, str]]) -> dict[str, list[dict[str, str]]]:
    grouped: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        if row.get("exchange") != "ASX":
            continue
        if row.get("official") != "true" or row.get("listing_status") != "active":
            continue
        grouped.setdefault(f"ASX::{row.get('ticker', '')}", []).append(row)
    return grouped


def build_asx_isin_probe_lookup(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    return {f"ASX::{row.get('ticker', '')}": row for row in rows if row.get("ticker")}


def residual_decision_for(
    *,
    gap: dict[str, str],
    truth: dict[str, str],
    refs: list[dict[str, str]],
    isin_probe: dict[str, str],
) -> str:
    field = gap.get("field", "")
    if truth.get("source_of_truth_outcome") == "core_exclusion_candidate":
        return "core_exclusion_candidate_requires_scope_review"
    if field == "missing_isin_primary":
        if isin_probe.get("decision") == "accept" and isin_probe.get("asx_isin"):
            return "official_asx_isin_available_review_apply"
        if isin_probe.get("decision") == "name_mismatch":
            return "accepted_source_gap_requires_exact_name_review"
        if isin_probe.get("decision") == "no_asx_match":
            return "accepted_source_gap_not_in_current_asx_isin_workbook"
        if refs and not any(ref.get("isin") for ref in refs):
            return "accepted_source_gap_official_asx_feeds_do_not_expose_isin"
        return "accepted_source_gap_requires_registry_or_issuer_source"
    if field == "missing_etf_category":
        if any(ref.get("source_key") == "asx_investment_products" for ref in refs):
            return "accepted_source_gap_official_product_taxonomy_unavailable"
        return "accepted_source_gap_requires_official_product_taxonomy"
    return "out_of_scope"


def next_action_for(residual_decision: str, field: str) -> str:
    if residual_decision == "official_asx_isin_available_review_apply":
        return "apply_only_after_asx_code_name_numeric_token_and_isin_checksum_gates"
    if residual_decision == "core_exclusion_candidate_requires_scope_review":
        return "review_scope_before_identifier_or_category_enrichment"
    if residual_decision == "accepted_source_gap_requires_exact_name_review":
        return "do_not_apply_until_name_mismatch_is_manually_resolved"
    if field == "missing_etf_category":
        return "keep_category_blank_until_official_or_reviewed_product_taxonomy_exists"
    return "keep_identifier_blank_until_direct_registry_issuer_or_official_asx_evidence_exists"


def review_bucket_for(residual_decision: str) -> str:
    if residual_decision == "official_asx_isin_available_review_apply":
        return "official_isin_candidate_requires_apply_gate"
    if residual_decision == "accepted_source_gap_requires_exact_name_review":
        return "identity_mismatch_requires_manual_review"
    if residual_decision == "core_exclusion_candidate_requires_scope_review":
        return "scope_review_before_any_data_fill"
    if residual_decision in {
        "accepted_source_gap_official_product_taxonomy_unavailable",
        "accepted_source_gap_requires_official_product_taxonomy",
    }:
        return "product_taxonomy_source_gap"
    if residual_decision in {
        "accepted_source_gap_not_in_current_asx_isin_workbook",
        "accepted_source_gap_official_asx_feeds_do_not_expose_isin",
        "accepted_source_gap_requires_registry_or_issuer_source",
    }:
        return "identifier_source_gap"
    return "out_of_scope_or_unknown"


def review_priority_for(review_bucket: str) -> str:
    if review_bucket in {
        "official_isin_candidate_requires_apply_gate",
        "identity_mismatch_requires_manual_review",
        "scope_review_before_any_data_fill",
    }:
        return "P1"
    if review_bucket == "product_taxonomy_source_gap":
        return "P2"
    if review_bucket == "identifier_source_gap":
        return "P3"
    return "P4"


def apply_eligibility_for(review_bucket: str) -> str:
    if review_bucket == "official_isin_candidate_requires_apply_gate":
        return "apply_only_after_asx_code_name_token_and_isin_checksum_validation"
    if review_bucket == "identity_mismatch_requires_manual_review":
        return "blocked_until_identity_mismatch_resolved"
    if review_bucket == "scope_review_before_any_data_fill":
        return "blocked_until_core_or_extended_scope_decision"
    if review_bucket == "product_taxonomy_source_gap":
        return "keep_category_blank_until_official_product_taxonomy_source"
    if review_bucket == "identifier_source_gap":
        return "keep_identifier_blank_until_direct_registry_issuer_or_official_asx_evidence"
    return "manual_review_required"


def verification_evidence_for(review_bucket: str) -> str:
    if review_bucket == "official_isin_candidate_requires_apply_gate":
        return "official_asx_isin_workbook_exact_code_name_numeric_token_and_valid_isin_checksum"
    if review_bucket == "identity_mismatch_requires_manual_review":
        return "manual_exact_name_or_alias_resolution_before_isin_apply"
    if review_bucket == "scope_review_before_any_data_fill":
        return "scope_decision_for_core_extended_or_exclude_before_identifier_or_category_fill"
    if review_bucket == "product_taxonomy_source_gap":
        return "official_or_reviewed_product_taxonomy_with_exact_listing_match"
    if review_bucket == "identifier_source_gap":
        return "direct_registry_issuer_or_official_asx_identifier_source_with_exact_listing_match"
    return "manual_review_required"


def asx_resolution_queue_for(row: dict[str, str]) -> str:
    residual_decision = row["residual_decision"]
    if residual_decision == "core_exclusion_candidate_requires_scope_review":
        if row["field"] == "missing_isin_primary":
            return "core_exclusion_candidate_identifier_scope_review"
        if row["field"] == "missing_etf_category":
            return "core_exclusion_candidate_category_scope_review"
        return "core_exclusion_candidate_scope_review"
    if residual_decision == "official_asx_isin_available_review_apply":
        return "official_asx_isin_candidate_apply_gate"
    if residual_decision == "accepted_source_gap_requires_exact_name_review":
        return "asx_isin_workbook_name_mismatch_manual_review"
    if residual_decision == "accepted_source_gap_not_in_current_asx_isin_workbook":
        return "missing_isin_not_in_current_asx_isin_workbook"
    if residual_decision == "accepted_source_gap_official_asx_feeds_do_not_expose_isin":
        return "missing_isin_official_asx_feeds_do_not_expose_isin"
    if residual_decision == "accepted_source_gap_requires_registry_or_issuer_source":
        return "missing_isin_requires_registry_or_issuer_source"
    if residual_decision == "accepted_source_gap_official_product_taxonomy_unavailable":
        return "missing_etf_category_official_taxonomy_unavailable"
    if residual_decision == "accepted_source_gap_requires_official_product_taxonomy":
        return "missing_etf_category_requires_official_product_taxonomy"
    return "out_of_scope_or_unknown_review"


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


def recommended_next_source_for(queue: str, official_source: str) -> str:
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


def source_gate_for(queue: str) -> str:
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


def official_source_context_for(row: dict[str, str]) -> str:
    sources = row.get("official_masterfile_sources") or "none"
    probe_decision = row.get("asx_isin_probe_decision") or "missing_probe_row"
    return f"official_masterfile_sources={sources};asx_isin_probe_decision={probe_decision}"


def official_capability_for(row: dict[str, str]) -> str:
    return (
        f"masterfile_match={row.get('official_masterfile_match', '')};"
        f"masterfile_exposes_isin={row.get('official_masterfile_exposes_isin', '')};"
        f"masterfile_exposes_sector={row.get('official_masterfile_exposes_sector', '')}"
    )


def build_review_rows(
    *,
    tickers: list[dict[str, str]],
    masterfile_rows: list[dict[str, str]],
    source_gap_rows: list[dict[str, str]],
    source_of_truth_rows: list[dict[str, str]],
    asx_isin_probe_rows: list[dict[str, str]],
) -> list[dict[str, str]]:
    ticker_lookup = build_lookup(tickers)
    truth_lookup = build_truth_lookup(source_of_truth_rows)
    masterfile_lookup = build_masterfile_lookup(masterfile_rows)
    isin_probe_lookup = build_asx_isin_probe_lookup(asx_isin_probe_rows)
    rows: list[dict[str, str]] = []
    for gap in source_gap_rows:
        if gap.get("exchange") != "ASX" or gap.get("field") not in REVIEW_FIELDS:
            continue
        key = listing_key(gap)
        ticker = ticker_lookup.get(key, {})
        truth = truth_lookup.get((gap.get("field", ""), key), {})
        refs = masterfile_lookup.get(key, [])
        isin_probe = isin_probe_lookup.get(key, {})
        residual_decision = residual_decision_for(gap=gap, truth=truth, refs=refs, isin_probe=isin_probe)
        review_bucket = review_bucket_for(residual_decision)
        asx_resolution_queue = ""
        row = {
            "listing_key": key,
            "ticker": gap.get("ticker", ""),
            "exchange": "ASX",
            "asset_type": gap.get("asset_type", ""),
            "name": gap.get("name", "") or ticker.get("name", ""),
            "field": gap.get("field", ""),
            "target_field": gap.get("target_field", ""),
            "gap_class": gap.get("gap_class", ""),
            "source_of_truth_outcome": truth.get("source_of_truth_outcome", ""),
            "core_action": truth.get("core_action", ""),
            "fill_action": truth.get("fill_action", ""),
            "review_needed": gap.get("review_needed", ""),
            "recommended_next_source": gap.get("recommended_next_source", ""),
            "source_gate": gap.get("source_gate", ""),
            "official_source_context": "",
            "official_capability": "",
            "official_masterfile_match": "true" if refs else "false",
            "official_masterfile_sources": "|".join(
                sorted({ref.get("source_key", "") for ref in refs if ref.get("source_key")})
            ),
            "official_masterfile_exposes_isin": "true" if any(ref.get("isin") for ref in refs) else "false",
            "official_masterfile_exposes_sector": "true" if any(ref.get("sector") for ref in refs) else "false",
            "asx_isin_probe_decision": isin_probe.get("decision", ""),
            "asx_isin_probe_isin": isin_probe.get("asx_isin", ""),
            "asx_isin_probe_security_type": isin_probe.get("asx_security_type", ""),
            "asx_resolution_queue": asx_resolution_queue,
            "residual_decision": residual_decision,
            "review_bucket": review_bucket,
            "review_priority": review_priority_for(review_bucket),
            "review_strategy": "",
            "apply_eligibility": apply_eligibility_for(review_bucket),
            "verification_evidence_required": verification_evidence_for(review_bucket),
            "recommended_next_action": next_action_for(residual_decision, gap.get("field", "")),
        }
        row["asx_resolution_queue"] = asx_resolution_queue_for(row)
        row["review_strategy"], row["verification_evidence_required"] = asx_review_strategy_for(
            row["asx_resolution_queue"]
        )
        source_context = row["official_masterfile_sources"] or "none"
        row["recommended_next_source"] = recommended_next_source_for(row["asx_resolution_queue"], source_context)
        row["source_gate"] = source_gate_for(row["asx_resolution_queue"])
        row["official_source_context"] = official_source_context_for(row)
        row["official_capability"] = official_capability_for(row)
        rows.append(row)
    return sorted(rows, key=lambda row: (row["review_priority"], row["review_bucket"], row["field"], row["ticker"], row["listing_key"]))


def summarize(rows: list[dict[str, str]], generated_at: str) -> dict[str, Any]:
    core_exclusion_rows = [
        row for row in rows if row["source_of_truth_outcome"] == "core_exclusion_candidate"
    ]
    apply_eligibility_totals = Counter(row["apply_eligibility"] for row in rows)
    verification_evidence_required_totals = Counter(row["verification_evidence_required"] for row in rows)
    queue_totals = Counter(row["asx_resolution_queue"] for row in rows)
    direct_apply_candidate_rows = apply_eligibility_totals.get(
        "apply_only_after_asx_code_name_token_and_isin_checksum_validation", 0
    )
    asx_residual_backlog = {
        "status": (
            "official_isin_candidates_pending_apply_gate"
            if direct_apply_candidate_rows
            else "review_only_scope_identifier_or_product_taxonomy_source_gaps"
        ),
        "rows": len(rows),
        "scope_decision_required_rows": apply_eligibility_totals.get("blocked_until_core_or_extended_scope_decision", 0),
        "identity_review_required_rows": apply_eligibility_totals.get("blocked_until_identity_mismatch_resolved", 0),
        "official_identifier_source_required_rows": apply_eligibility_totals.get(
            "keep_identifier_blank_until_direct_registry_issuer_or_official_asx_evidence", 0
        ),
        "official_product_taxonomy_required_rows": apply_eligibility_totals.get(
            "keep_category_blank_until_official_product_taxonomy_source", 0
        ),
        "official_isin_apply_candidate_rows": direct_apply_candidate_rows,
        "direct_data_apply_allowed_rows": 0,
        "metadata_enrichment_authorized": False,
        "source_gate": (
            "ASX residual work remains blocked unless exact ASX workbook, registry, issuer, trustee, prospectus, "
            "or reviewed product-taxonomy evidence proves the value; no ticker/name or peer-instrument inference."
        ),
    }
    asx_review_strategy_totals: dict[str, Counter[str]] = {}
    asx_evidence_required_totals: dict[str, Counter[str]] = {}
    asx_official_capability_totals: dict[str, Counter[str]] = {}
    asx_review_batches: Counter[tuple[str, str, str]] = Counter()
    for row in rows:
        queue = row["asx_resolution_queue"]
        strategy, evidence_required = asx_review_strategy_for(queue)
        asx_review_strategy_totals.setdefault(queue, Counter())[strategy] += 1
        asx_evidence_required_totals.setdefault(queue, Counter())[evidence_required] += 1
        asx_official_capability_totals.setdefault(queue, Counter())[
            f"masterfile_match={row['official_masterfile_match']}"
        ] += 1
        asx_official_capability_totals.setdefault(queue, Counter())[
            f"masterfile_exposes_isin={row['official_masterfile_exposes_isin']}"
        ] += 1
        asx_official_capability_totals.setdefault(queue, Counter())[
            f"masterfile_exposes_sector={row['official_masterfile_exposes_sector']}"
        ] += 1
        sources = row["official_masterfile_sources"].split("|") if row["official_masterfile_sources"] else ["none"]
        for source in sources:
            if source:
                asx_review_batches[(queue, row["field"], source)] += 1
    top_asx_review_batches = []
    for (queue, field, official_source), count in sorted(
        asx_review_batches.items(),
        key=lambda item: (-item[1], item[0][0], item[0][1], item[0][2]),
    )[:25]:
        strategy, evidence_required = asx_review_strategy_for(queue)
        top_asx_review_batches.append(
            {
                "asx_resolution_queue": queue,
                "field": field,
                "official_source": official_source,
                "rows": count,
                "review_strategy": strategy,
                "evidence_required": evidence_required,
                "recommended_next_source": recommended_next_source_for(queue, official_source),
                "source_gate": source_gate_for(queue),
            }
        )
    return {
        "generated_at": generated_at,
        "rows": len(rows),
        "field_totals": dict(sorted(Counter(row["field"] for row in rows).items())),
        "asset_type_totals": dict(sorted(Counter(row["asset_type"] for row in rows).items())),
        "core_exclusion_candidate_rows": len(core_exclusion_rows),
        "core_exclusion_candidate_field_totals": dict(
            sorted(Counter(row["field"] for row in core_exclusion_rows).items())
        ),
        "core_exclusion_candidate_asset_type_totals": dict(
            sorted(Counter(row["asset_type"] for row in core_exclusion_rows).items())
        ),
        "core_exclusion_candidate_gap_class_totals": dict(
            sorted(Counter(row["gap_class"] for row in core_exclusion_rows).items())
        ),
        "core_exclusion_candidate_resolution_queue_totals": dict(
            sorted(Counter(row["asx_resolution_queue"] for row in core_exclusion_rows).items())
        ),
        "core_exclusion_candidate_official_source_totals": dict(
            sorted(
                Counter(
                    source
                    for row in core_exclusion_rows
                    for source in (
                        row["official_masterfile_sources"].split("|")
                        if row["official_masterfile_sources"]
                        else ["none"]
                    )
                    if source
                ).items()
            )
        ),
        "core_exclusion_candidate_official_capability_totals": dict(
            sorted(
                Counter(
                    capability
                    for row in core_exclusion_rows
                    for capability in (
                        f"masterfile_match={row['official_masterfile_match']}",
                        f"masterfile_exposes_isin={row['official_masterfile_exposes_isin']}",
                        f"masterfile_exposes_sector={row['official_masterfile_exposes_sector']}",
                    )
                ).items()
            )
        ),
        "core_exclusion_candidate_examples": [
            {
                "listing_key": row["listing_key"],
                "ticker": row["ticker"],
                "asset_type": row["asset_type"],
                "field": row["field"],
                "gap_class": row["gap_class"],
                "asx_resolution_queue": row["asx_resolution_queue"],
                "official_masterfile_sources": row["official_masterfile_sources"] or "none",
                "official_capability": row.get("official_capability") or official_capability_for(row),
                "source_gate": row.get("source_gate") or source_gate_for(row["asx_resolution_queue"]),
                "recommended_next_action": row["recommended_next_action"],
            }
            for row in core_exclusion_rows[:20]
        ],
        "gap_class_totals": dict(sorted(Counter(row["gap_class"] for row in rows).items())),
        "source_of_truth_outcome_totals": dict(sorted(Counter(row["source_of_truth_outcome"] for row in rows).items())),
        "asx_residual_backlog": asx_residual_backlog,
        "asx_residual_backlog_queue_totals": dict(sorted(queue_totals.items())),
        "asx_residual_backlog_evidence_required_totals": dict(sorted(verification_evidence_required_totals.items())),
        "asx_resolution_queue_totals": dict(sorted(queue_totals.items())),
        "asx_resolution_queue_field_totals": {
            queue: dict(sorted(Counter(row["field"] for row in rows if row["asx_resolution_queue"] == queue).items()))
            for queue in sorted({row["asx_resolution_queue"] for row in rows})
        },
        "asx_resolution_queue_gap_class_totals": {
            queue: dict(
                sorted(Counter(row["gap_class"] for row in rows if row["asx_resolution_queue"] == queue).items())
            )
            for queue in sorted({row["asx_resolution_queue"] for row in rows})
        },
        "asx_resolution_queue_official_source_totals": {
            queue: dict(
                sorted(
                    Counter(
                        source
                        for row in rows
                        if row["asx_resolution_queue"] == queue
                        for source in (
                            row["official_masterfile_sources"].split("|")
                            if row["official_masterfile_sources"]
                            else ["none"]
                        )
                        if source
                    ).items()
                )
            )
            for queue in sorted({row["asx_resolution_queue"] for row in rows})
        },
        "asx_resolution_queue_review_strategy_totals": {
            queue: dict(sorted(strategy_totals.items()))
            for queue, strategy_totals in sorted(asx_review_strategy_totals.items())
        },
        "asx_resolution_queue_evidence_required_totals": {
            queue: dict(sorted(evidence_totals.items()))
            for queue, evidence_totals in sorted(asx_evidence_required_totals.items())
        },
        "asx_resolution_queue_official_capability_totals": {
            queue: dict(sorted(capability_totals.items()))
            for queue, capability_totals in sorted(asx_official_capability_totals.items())
        },
        "top_asx_resolution_review_batches": top_asx_review_batches,
        "residual_decision_totals": dict(sorted(Counter(row["residual_decision"] for row in rows).items())),
        "review_bucket_totals": dict(sorted(Counter(row["review_bucket"] for row in rows).items())),
        "review_priority_totals": dict(sorted(Counter(row["review_priority"] for row in rows).items())),
        "apply_eligibility_totals": dict(sorted(apply_eligibility_totals.items())),
        "verification_evidence_required_totals": dict(sorted(verification_evidence_required_totals.items())),
        "asx_isin_probe_decision_totals": dict(
            sorted(Counter(row["asx_isin_probe_decision"] or "missing_probe_row" for row in rows if row["field"] == "missing_isin_primary").items())
        ),
        "official_masterfile_match_totals": dict(
            sorted(Counter(row["official_masterfile_match"] for row in rows).items())
        ),
        "official_masterfile_exposes_isin_totals": dict(
            sorted(Counter(row["official_masterfile_exposes_isin"] for row in rows).items())
        ),
        "official_masterfile_exposes_sector_totals": dict(
            sorted(Counter(row["official_masterfile_exposes_sector"] for row in rows).items())
        ),
        "official_masterfile_source_totals": dict(
            sorted(Counter(source for row in rows for source in row["official_masterfile_sources"].split("|") if source).items())
        ),
        "policy": {
            "official_first": "ASXListedCompanies.csv, ASX investment-products workbook, and ASX ISIN.xls probe context are review evidence, but only direct valid identifiers may be applied.",
            "no_guessing": "No ASX ISIN or ETF category is inferred from ticker, name shape, issuer family, or peer instruments.",
            "scope_first": "Core-exclusion candidates remain blank until scope is explicitly reviewed.",
        },
    }


def build_json_payload(
    *,
    summary: dict[str, Any],
    rows: list[dict[str, str]],
    source_files: dict[str, str],
) -> dict[str, Any]:
    return {
        "_meta": {
            "generated_at": summary["generated_at"],
            "source_files": source_files,
            "policy": summary["policy"],
        },
        "summary": summary,
        "rows": rows,
    }


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDNAMES, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def write_markdown(path: Path, rows: list[dict[str, str]], summary: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# ASX Residual Review",
        "",
        f"Generated at: `{summary['generated_at']}`",
        "",
        "This report tracks ASX residual ISIN and ETF-category gaps after current official ASX masterfile and ASX ISIN workbook checks. It does not fill values.",
        "",
        "## Summary",
        "",
        f"- Review rows: `{summary['rows']}`",
        f"- Core-exclusion candidate rows requiring scope review: `{summary['core_exclusion_candidate_rows']}`",
        f"- ASX residual backlog rows: `{summary['asx_residual_backlog']['rows']}`",
        f"- Direct data apply allowed rows: `{summary['asx_residual_backlog']['direct_data_apply_allowed_rows']}`",
        "",
        "## ASX Residual Backlog",
        "",
        f"- Status: `{summary['asx_residual_backlog']['status']}`",
        f"- Scope decision required rows: `{summary['asx_residual_backlog']['scope_decision_required_rows']}`",
        f"- Identity review required rows: `{summary['asx_residual_backlog']['identity_review_required_rows']}`",
        f"- Official identifier source required rows: `{summary['asx_residual_backlog']['official_identifier_source_required_rows']}`",
        f"- Official product taxonomy required rows: `{summary['asx_residual_backlog']['official_product_taxonomy_required_rows']}`",
        f"- Official ISIN apply candidate rows: `{summary['asx_residual_backlog']['official_isin_apply_candidate_rows']}`",
        f"- Metadata enrichment authorized: `{str(summary['asx_residual_backlog']['metadata_enrichment_authorized']).lower()}`",
        "",
        "| Queue | Rows |",
        "|---|---:|",
    ]
    for queue, count in summary["asx_residual_backlog_queue_totals"].items():
        lines.append(f"| {queue} | {count} |")
    lines.extend(["", "| Evidence required | Rows |", "|---|---:|"])
    for evidence, count in summary["asx_residual_backlog_evidence_required_totals"].items():
        lines.append(f"| {evidence} | {count} |")
    lines.extend(
        [
        "",
        "## Fields",
        "",
        "| Field | Rows |",
        "|---|---:|",
        ]
    )
    for field, count in summary["field_totals"].items():
        lines.append(f"| {field} | {count} |")
    lines.extend(["", "## Residual Decisions", "", "| Decision | Rows |", "|---|---:|"])
    for decision, count in summary["residual_decision_totals"].items():
        lines.append(f"| {decision} | {count} |")
    lines.extend(["", "## ASX Resolution Queues", "", "| Queue | Rows |", "|---|---:|"])
    for queue, count in summary["asx_resolution_queue_totals"].items():
        lines.append(f"| {queue} | {count} |")
    lines.extend(["", "## ASX Resolution Queue By Gap Class", "", "| Queue | Gap class | Rows |", "|---|---|---:|"])
    for queue, gap_class_totals in summary["asx_resolution_queue_gap_class_totals"].items():
        for gap_class, count in gap_class_totals.items():
            lines.append(f"| {queue} | {gap_class} | {count} |")
    lines.extend(["", "## ASX Resolution Queue By Official Source", "", "| Queue | Official source | Rows |", "|---|---|---:|"])
    for queue, official_source_totals in summary["asx_resolution_queue_official_source_totals"].items():
        for source, count in official_source_totals.items():
            lines.append(f"| {queue} | {source} | {count} |")
    lines.extend(["", "## ASX Official Source Capability", "", "| Queue | Capability | Rows |", "|---|---|---:|"])
    for queue, capability_totals in summary["asx_resolution_queue_official_capability_totals"].items():
        for capability, count in capability_totals.items():
            lines.append(f"| {queue} | {capability} | {count} |")
    lines.extend(["", "## ASX Review Strategies", "", "| Queue | Strategy | Rows |", "|---|---|---:|"])
    for queue, strategy_totals in summary["asx_resolution_queue_review_strategy_totals"].items():
        for strategy, count in strategy_totals.items():
            lines.append(f"| {queue} | {strategy} | {count} |")
    lines.extend(["", "## ASX Review Evidence", "", "| Queue | Evidence required | Rows |", "|---|---|---:|"])
    for queue, evidence_totals in summary["asx_resolution_queue_evidence_required_totals"].items():
        for evidence_required, count in evidence_totals.items():
            lines.append(f"| {queue} | {evidence_required} | {count} |")
    lines.extend(
        [
            "",
            "## Top ASX Review Batches",
            "",
            "| Queue | Field | Official source | Strategy | Evidence required | Recommended next source | Source gate | Rows |",
            "|---|---|---|---|---|---|---|---:|",
        ]
    )
    for batch in summary["top_asx_resolution_review_batches"]:
        lines.append(
            f"| {batch['asx_resolution_queue']} | {batch['field']} | {batch['official_source']} | "
            f"{batch['review_strategy']} | {batch['evidence_required']} | "
            f"{batch['recommended_next_source']} | {batch['source_gate']} | {batch['rows']} |"
        )
    lines.extend(["", "## Review Priorities", "", "| Priority | Rows |", "|---|---:|"])
    for priority, count in summary["review_priority_totals"].items():
        lines.append(f"| {priority} | {count} |")
    lines.extend(["", "## Review Buckets", "", "| Bucket | Rows |", "|---|---:|"])
    for bucket, count in summary["review_bucket_totals"].items():
        lines.append(f"| {bucket} | {count} |")
    lines.extend(["", "## Apply Eligibility", "", "| Eligibility | Rows |", "|---|---:|"])
    for eligibility, count in summary["apply_eligibility_totals"].items():
        lines.append(f"| {eligibility} | {count} |")
    lines.extend(["", "## Scope Blockers", "", "| Field | Rows |", "|---|---:|"])
    for field, count in summary["core_exclusion_candidate_field_totals"].items():
        lines.append(f"| {field} | {count} |")
    lines.extend(["", "| Asset type | Rows |", "|---|---:|"])
    for asset_type, count in summary["core_exclusion_candidate_asset_type_totals"].items():
        lines.append(f"| {asset_type} | {count} |")
    lines.extend(["", "| Gap class | Rows |", "|---|---:|"])
    for gap_class, count in summary["core_exclusion_candidate_gap_class_totals"].items():
        lines.append(f"| {gap_class} | {count} |")
    lines.extend(["", "| Resolution queue | Rows |", "|---|---:|"])
    for queue, count in summary["core_exclusion_candidate_resolution_queue_totals"].items():
        lines.append(f"| {queue} | {count} |")
    lines.extend(["", "| Official source | Rows |", "|---|---:|"])
    for source, count in summary["core_exclusion_candidate_official_source_totals"].items():
        lines.append(f"| {source} | {count} |")
    lines.extend(["", "| Official capability | Rows |", "|---|---:|"])
    for capability, count in summary["core_exclusion_candidate_official_capability_totals"].items():
        lines.append(f"| {capability} | {count} |")
    lines.extend(["", "## Verification Evidence", "", "| Evidence Gate | Rows |", "|---|---:|"])
    for evidence, count in summary["verification_evidence_required_totals"].items():
        lines.append(f"| {evidence} | {count} |")
    lines.extend(["", "## Rows", "", "| Listing key | Priority | Bucket | Field | Class | Outcome | Decision |", "|---|---|---|---|---|---|---|"])
    for row in rows:
        lines.append(
            f"| {row['listing_key']} | {row['review_priority']} | {row['review_bucket']} | {row['field']} | {row['gap_class']} | "
            f"{row['source_of_truth_outcome']} | {row['residual_decision']} |"
        )
    lines.extend(
        [
            "",
            "## Policy",
            "",
            "- Do not infer ISINs from issuer names, symbol shape, or same-issuer instruments.",
            "- ETF categories remain blank until official or reviewed product taxonomy evidence exists.",
            "- Core-exclusion candidates require explicit scope review before identifier/category enrichment.",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a listing-keyed ASX residual ISIN/category review report.")
    parser.add_argument("--tickers-csv", type=Path, default=DEFAULT_TICKERS_CSV)
    parser.add_argument("--masterfile-reference-csv", type=Path, default=DEFAULT_MASTERFILE_REFERENCE_CSV)
    parser.add_argument("--source-gap-classification-csv", type=Path, default=DEFAULT_SOURCE_GAP_CLASSIFICATION_CSV)
    parser.add_argument("--source-of-truth-decisions-csv", type=Path, default=DEFAULT_SOURCE_OF_TRUTH_DECISIONS_CSV)
    parser.add_argument("--asx-isin-probe-csv", type=Path, default=DEFAULT_ASX_ISIN_PROBE_CSV)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_CSV_OUT)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_JSON_OUT)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_MD_OUT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    rows = build_review_rows(
        tickers=load_csv(args.tickers_csv),
        masterfile_rows=load_csv(args.masterfile_reference_csv),
        source_gap_rows=load_csv(args.source_gap_classification_csv),
        source_of_truth_rows=load_csv(args.source_of_truth_decisions_csv),
        asx_isin_probe_rows=load_csv(args.asx_isin_probe_csv),
    )
    summary = summarize(rows, utc_now_iso())
    write_csv(args.csv_out, rows)
    payload = build_json_payload(
        summary=summary,
        rows=rows,
        source_files={
            "tickers_csv": display_path(args.tickers_csv),
            "masterfile_reference_csv": display_path(args.masterfile_reference_csv),
            "source_gap_classification_csv": display_path(args.source_gap_classification_csv),
            "source_of_truth_decisions_csv": display_path(args.source_of_truth_decisions_csv),
            "asx_isin_probe_csv": display_path(args.asx_isin_probe_csv),
        },
    )
    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.json_out.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    write_markdown(args.md_out, rows, summary)
    print(
        json.dumps(
            {
                "rows": len(rows),
                "field_totals": summary["field_totals"],
                "residual_decision_totals": summary["residual_decision_totals"],
                "csv_out": display_path(args.csv_out),
                "json_out": display_path(args.json_out),
                "md_out": display_path(args.md_out),
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()

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
CANADA_EXCHANGES = {"TSX", "TSXV", "NEO"}

DEFAULT_TICKERS_CSV = DATA_DIR / "tickers.csv"
DEFAULT_IDENTIFIERS_EXTENDED_CSV = DATA_DIR / "identifiers_extended.csv"
DEFAULT_MASTERFILE_REFERENCE_CSV = DATA_DIR / "masterfiles" / "reference.csv"
DEFAULT_SOURCE_GAP_CLASSIFICATION_CSV = REPORTS_DIR / "source_gap_classification.csv"
DEFAULT_SOURCE_OF_TRUTH_DECISIONS_CSV = REPORTS_DIR / "source_of_truth_decisions.csv"
DEFAULT_OPENFIGI_GAP_CSV = DATA_DIR / "review_overrides" / "canada_figi_openfigi_gaps.csv"
DEFAULT_CSV_OUT = REPORTS_DIR / "canada_residual_review.csv"
DEFAULT_JSON_OUT = REPORTS_DIR / "canada_residual_review.json"
DEFAULT_MD_OUT = REPORTS_DIR / "canada_residual_review.md"

CSV_FIELDNAMES = [
    "listing_key",
    "ticker",
    "exchange",
    "asset_type",
    "name",
    "isin",
    "figi",
    "figi_source",
    "openfigi_review_status",
    "openfigi_review_decision",
    "openfigi_review_source",
    "openfigi_reviewed_at",
    "missing_isin",
    "missing_figi",
    "source_gap_fields",
    "source_gap_classes",
    "source_of_truth_outcomes",
    "source_gap_context",
    "official_masterfile_match",
    "official_masterfile_sources",
    "official_masterfile_exposes_isin",
    "official_masterfile_exposes_sector",
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


def openfigi_gap_key(row: dict[str, str]) -> tuple[str, str]:
    return (row.get("listing_key", ""), row.get("isin", ""))


def build_openfigi_gap_lookup(rows: list[dict[str, str]]) -> dict[tuple[str, str], dict[str, str]]:
    reviewed_statuses = {
        "accepted_source_gap_no_openfigi_match",
        "accepted_source_gap_figi_cross_isin_collision",
    }
    lookup: dict[tuple[str, str], dict[str, str]] = {}
    for row in rows:
        key = openfigi_gap_key(row)
        if not all(key) or row.get("review_status") not in reviewed_statuses:
            continue
        lookup[key] = row
    return lookup


def build_masterfile_lookup(rows: list[dict[str, str]]) -> dict[str, list[dict[str, str]]]:
    grouped: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        if row.get("exchange") not in CANADA_EXCHANGES:
            continue
        if row.get("official") != "true" or row.get("listing_status") != "active":
            continue
        key = f"{row.get('exchange', '')}::{row.get('ticker', '')}"
        grouped.setdefault(key, []).append(row)
    return grouped


def isin_decision_for(row: dict[str, str], gap_rows: list[dict[str, str]], truth_rows: list[dict[str, str]], refs: list[dict[str, str]]) -> str:
    if row.get("isin"):
        return "isin_present"
    if any(truth.get("source_of_truth_outcome") == "core_exclusion_candidate" for truth in truth_rows):
        return "missing_isin_core_exclusion_candidate_requires_scope_review"
    if any(gap.get("field") == "missing_isin_primary" for gap in gap_rows):
        if refs and not any(ref.get("isin") for ref in refs):
            return "missing_isin_official_canada_masterfiles_do_not_expose_isin"
        return "missing_isin_reviewed_source_gap"
    return "missing_isin_unclassified"


def figi_decision_for(row: dict[str, str], openfigi_review: dict[str, str] | None = None) -> str:
    if row.get("figi"):
        return "figi_present"
    if openfigi_review:
        if openfigi_review.get("review_status") == "accepted_source_gap_figi_cross_isin_collision":
            return "missing_figi_reviewed_source_gap_figi_cross_isin_collision"
        return "missing_figi_reviewed_source_gap_no_openfigi_match"
    if row.get("isin"):
        return "missing_figi_openfigi_candidate"
    return "missing_figi_requires_isin_first"


def isin_apply_eligibility_for(isin_decision: str) -> str:
    if isin_decision == "isin_present":
        return "no_isin_action_required"
    if isin_decision == "missing_isin_core_exclusion_candidate_requires_scope_review":
        return "blocked_until_scope_decision"
    if isin_decision in {
        "missing_isin_official_canada_masterfiles_do_not_expose_isin",
        "missing_isin_reviewed_source_gap",
    }:
        return "keep_blank_until_official_isin_source"
    return "manual_review_required"


def figi_apply_eligibility_for(figi_decision: str) -> str:
    if figi_decision == "figi_present":
        return "no_figi_action_required"
    if figi_decision == "missing_figi_openfigi_candidate":
        return "eligible_for_openfigi_queue_after_isin_gate"
    if figi_decision == "missing_figi_requires_isin_first":
        return "blocked_until_isin_resolved"
    if figi_decision.startswith("missing_figi_reviewed_source_gap_"):
        return "keep_blank_as_reviewed_openfigi_source_gap"
    return "manual_review_required"


def verification_evidence_for(isin_decision: str, figi_decision: str) -> str:
    if isin_decision == "missing_isin_core_exclusion_candidate_requires_scope_review":
        return "scope_decision_for_core_extended_or_exclude_before_identifier_enrichment"
    if isin_decision == "missing_isin_official_canada_masterfiles_do_not_expose_isin":
        return "official_csd_issuer_or_reviewed_identifier_source_with_exact_listing_match_and_valid_isin"
    if isin_decision == "missing_isin_reviewed_source_gap":
        return "stronger_official_identifier_source_before_isin_fill"
    if figi_decision == "missing_figi_openfigi_candidate":
        return "openfigi_id_isin_query_with_canada_exchange_hint_then_collision_gate"
    if figi_decision == "missing_figi_requires_isin_first":
        return "valid_isin_required_before_figi_lookup"
    if figi_decision == "missing_figi_reviewed_source_gap_figi_cross_isin_collision":
        return "stronger_figi_source_required_openfigi_cross_isin_collision_reviewed"
    if figi_decision == "missing_figi_reviewed_source_gap_no_openfigi_match":
        return "stronger_figi_source_required_openfigi_no_match_reviewed"
    return "none_no_identifier_change_authorized"


def recommended_action_for(isin_decision: str, figi_decision: str, gap_fields: set[str]) -> str:
    if isin_decision == "missing_isin_core_exclusion_candidate_requires_scope_review":
        return "review_scope_before_identifier_enrichment"
    if isin_decision == "missing_isin_official_canada_masterfiles_do_not_expose_isin":
        return "seek_official_csd_issuer_or_reviewed_identifier_source"
    if figi_decision.startswith("missing_figi_reviewed_source_gap_"):
        return "keep_blank_as_documented_openfigi_source_gap_until_stronger_source"
    if figi_decision == "missing_figi_openfigi_candidate":
        return "run_openfigi_for_listing_key_after_isin_gate"
    if gap_fields:
        return "keep_blank_as_documented_source_gap_until_stronger_source"
    return "no_action"


def canada_resolution_queue_for(row: dict[str, str]) -> str:
    outcomes = set(row["source_of_truth_outcomes"].split("|")) if row["source_of_truth_outcomes"] else set()
    fields = set(row["source_gap_fields"].split("|")) if row["source_gap_fields"] else set()
    if "core_exclusion_candidate" in outcomes:
        if row["missing_isin"] == "true":
            return "core_exclusion_candidate_identifier_scope_review"
        if fields & {"missing_sector_stock", "missing_etf_category"}:
            return "core_exclusion_candidate_metadata_scope_review"
        return "core_exclusion_candidate_scope_review"
    if row["isin_decision"] == "missing_isin_official_canada_masterfiles_do_not_expose_isin":
        return "missing_isin_official_canada_masterfiles_do_not_expose_isin"
    if row["isin_decision"] == "missing_isin_reviewed_source_gap":
        return "missing_isin_reviewed_source_gap"
    if row["figi_decision"] == "missing_figi_reviewed_source_gap_no_openfigi_match":
        return "reviewed_openfigi_no_match_source_gap"
    if row["figi_decision"] == "missing_figi_reviewed_source_gap_figi_cross_isin_collision":
        return "reviewed_openfigi_cross_isin_collision_source_gap"
    if row["figi_decision"] == "missing_figi_openfigi_candidate":
        return "openfigi_candidate_after_isin_gate"
    if row["figi_decision"] == "missing_figi_requires_isin_first":
        return "figi_blocked_until_isin_resolved"
    if fields:
        return "metadata_source_gap_keep_blank_until_stronger_source"
    return "residual_no_identifier_action"


def canada_review_strategy_for(queue: str) -> tuple[str, str]:
    if queue == "core_exclusion_candidate_identifier_scope_review":
        return (
            "scope_review_before_canada_identifier_enrichment",
            "official_listing_scope_decision_for_core_extended_or_exclude",
        )
    if queue == "core_exclusion_candidate_metadata_scope_review":
        return (
            "scope_review_before_canada_metadata_enrichment",
            "official_listing_scope_decision_before_sector_or_category_fill",
        )
    if queue == "missing_isin_official_canada_masterfiles_do_not_expose_isin":
        return (
            "seek_official_canada_isin_source",
            "official_csd_issuer_or_reviewed_identifier_source_with_exact_listing_match_and_valid_isin",
        )
    if queue == "missing_isin_reviewed_source_gap":
        return (
            "keep_isin_blank_until_stronger_official_source",
            "stronger_official_identifier_source_before_isin_fill",
        )
    if queue == "reviewed_openfigi_no_match_source_gap":
        return (
            "keep_figi_blank_after_reviewed_openfigi_no_match",
            "stronger_figi_source_required_openfigi_no_match_reviewed",
        )
    if queue == "reviewed_openfigi_cross_isin_collision_source_gap":
        return (
            "keep_figi_blank_after_reviewed_openfigi_cross_isin_collision",
            "stronger_figi_source_required_openfigi_cross_isin_collision_reviewed",
        )
    if queue == "openfigi_candidate_after_isin_gate":
        return (
            "queue_openfigi_by_isin_with_canada_exchange_hint",
            "openfigi_id_isin_query_with_canada_exchange_hint_then_collision_gate",
        )
    if queue == "figi_blocked_until_isin_resolved":
        return (
            "block_figi_until_valid_isin",
            "valid_isin_required_before_figi_lookup",
        )
    if queue == "metadata_source_gap_keep_blank_until_stronger_source":
        return (
            "keep_metadata_blank_until_stronger_official_source",
            "reviewed_issuer_or_product_metadata_source_with_exact_listing_match",
        )
    if queue == "residual_no_identifier_action":
        return (
            "keep_metadata_blank_until_stronger_official_source",
            "none_no_identifier_change_authorized",
        )
    return ("no_identifier_action", "none_no_identifier_change_authorized")


def recommended_next_source_for(queue: str, official_source: str) -> str:
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


def source_gate_for(queue: str) -> str:
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


def source_gap_context_for(row: dict[str, str]) -> str:
    return (
        f"source_gap_fields={row.get('source_gap_fields', '') or 'none'};"
        f"source_gap_classes={row.get('source_gap_classes', '') or 'none'};"
        f"source_of_truth_outcomes={row.get('source_of_truth_outcomes', '') or 'none'}"
    )


def official_source_context_for(row: dict[str, str]) -> str:
    return (
        f"official_masterfile_match={row.get('official_masterfile_match', '') or 'none'};"
        f"official_masterfile_sources={row.get('official_masterfile_sources', '') or 'none'};"
        f"official_masterfile_exposes_isin={row.get('official_masterfile_exposes_isin', '') or 'none'};"
        f"official_masterfile_exposes_sector={row.get('official_masterfile_exposes_sector', '') or 'none'}"
    )


def identifier_review_context_for(row: dict[str, str]) -> str:
    return (
        f"missing_isin={row.get('missing_isin', '') or 'none'};"
        f"missing_figi={row.get('missing_figi', '') or 'none'};"
        f"isin_decision={row.get('isin_decision', '') or 'none'};"
        f"figi_decision={row.get('figi_decision', '') or 'none'};"
        f"isin_apply_eligibility={row.get('isin_apply_eligibility', '') or 'none'};"
        f"figi_apply_eligibility={row.get('figi_apply_eligibility', '') or 'none'};"
        f"openfigi_review_status={row.get('openfigi_review_status', '') or 'none'};"
        f"openfigi_review_decision={row.get('openfigi_review_decision', '') or 'none'}"
    )


def build_review_rows(
    *,
    tickers: list[dict[str, str]],
    identifiers_extended: list[dict[str, str]],
    masterfile_rows: list[dict[str, str]],
    source_gap_rows: list[dict[str, str]],
    source_of_truth_rows: list[dict[str, str]],
    openfigi_gap_rows: list[dict[str, str]] | None = None,
) -> list[dict[str, str]]:
    identifiers_lookup = build_lookup(identifiers_extended)
    source_gap_lookup = build_multi_lookup(source_gap_rows)
    source_of_truth_lookup = build_multi_lookup(source_of_truth_rows)
    masterfile_lookup = build_masterfile_lookup(masterfile_rows)
    openfigi_gap_lookup = build_openfigi_gap_lookup(openfigi_gap_rows or [])
    rows: list[dict[str, str]] = []
    for ticker in tickers:
        if ticker.get("exchange") not in CANADA_EXCHANGES:
            continue
        key = listing_key(ticker)
        identifier = identifiers_lookup.get(key, {})
        gap_rows = source_gap_lookup.get(key, [])
        truth_rows = source_of_truth_lookup.get(key, [])
        refs = masterfile_lookup.get(key, [])
        gap_fields = {row.get("field", "") for row in gap_rows if row.get("field")}
        gap_classes = {row.get("gap_class", "") for row in gap_rows if row.get("gap_class")}
        outcomes = {row.get("source_of_truth_outcome", "") for row in truth_rows if row.get("source_of_truth_outcome")}
        openfigi_review = openfigi_gap_lookup.get((key, ticker.get("isin", "")), {})
        row = {
            "listing_key": key,
            "ticker": ticker.get("ticker", ""),
            "exchange": ticker.get("exchange", ""),
            "asset_type": ticker.get("asset_type", ""),
            "name": ticker.get("name", ""),
            "isin": ticker.get("isin", ""),
            "figi": identifier.get("figi", ""),
            "figi_source": identifier.get("figi_source", ""),
            "openfigi_review_status": openfigi_review.get("review_status", ""),
            "openfigi_review_decision": openfigi_review.get("decision", ""),
            "openfigi_review_source": openfigi_review.get("source", ""),
            "openfigi_reviewed_at": openfigi_review.get("reviewed_at", ""),
            "missing_isin": "true" if not ticker.get("isin") else "false",
            "missing_figi": "true" if not identifier.get("figi") else "false",
            "source_gap_fields": "|".join(sorted(gap_fields)),
            "source_gap_classes": "|".join(sorted(gap_classes)),
            "source_of_truth_outcomes": "|".join(sorted(outcomes)),
            "source_gap_context": "",
            "official_masterfile_match": "true" if refs else "false",
            "official_masterfile_sources": "|".join(sorted({ref.get("source_key", "") for ref in refs if ref.get("source_key")})),
            "official_masterfile_exposes_isin": "true" if any(ref.get("isin") for ref in refs) else "false",
            "official_masterfile_exposes_sector": "true" if any(ref.get("sector") for ref in refs) else "false",
            "official_source_context": "",
            "canada_resolution_queue": "",
            "review_strategy": "",
            "queue_evidence_required": "",
            "recommended_next_source": "",
            "source_gate": "",
            "isin_decision": "",
            "figi_decision": "",
            "isin_apply_eligibility": "",
            "figi_apply_eligibility": "",
            "verification_evidence_required": "",
            "identifier_review_context": "",
            "recommended_next_action": "",
        }
        row["isin_decision"] = isin_decision_for(ticker, gap_rows, truth_rows, refs)
        row["figi_decision"] = figi_decision_for(row, openfigi_review)
        row["isin_apply_eligibility"] = isin_apply_eligibility_for(row["isin_decision"])
        row["figi_apply_eligibility"] = figi_apply_eligibility_for(row["figi_decision"])
        row["verification_evidence_required"] = verification_evidence_for(row["isin_decision"], row["figi_decision"])
        row["recommended_next_action"] = recommended_action_for(row["isin_decision"], row["figi_decision"], gap_fields)
        row["canada_resolution_queue"] = canada_resolution_queue_for(row)
        row["review_strategy"], row["queue_evidence_required"] = canada_review_strategy_for(
            row["canada_resolution_queue"]
        )
        official_source = row["official_masterfile_sources"] or "none"
        if "|" in official_source:
            official_source = "multiple_official_sources"
        row["recommended_next_source"] = recommended_next_source_for(row["canada_resolution_queue"], official_source)
        row["source_gate"] = source_gate_for(row["canada_resolution_queue"])
        row["source_gap_context"] = source_gap_context_for(row)
        row["official_source_context"] = official_source_context_for(row)
        row["identifier_review_context"] = identifier_review_context_for(row)
        if row["missing_isin"] == "true" or row["missing_figi"] == "true" or gap_rows:
            rows.append(row)
    return sorted(rows, key=lambda row: (row["exchange"], row["ticker"], row["listing_key"]))


def summarize(rows: list[dict[str, str]], generated_at: str) -> dict[str, Any]:
    core_exclusion_rows = [
        row for row in rows if "core_exclusion_candidate" in row["source_of_truth_outcomes"].split("|")
    ]
    queue_totals = Counter(row["canada_resolution_queue"] for row in rows)
    isin_apply_totals = Counter(row["isin_apply_eligibility"] for row in rows)
    figi_apply_totals = Counter(row["figi_apply_eligibility"] for row in rows)
    figi_decision_totals = Counter(row["figi_decision"] for row in rows)
    reviewed_openfigi_source_gap_rows = sum(
        count
        for decision, count in figi_decision_totals.items()
        if str(decision).startswith("missing_figi_reviewed_source_gap_")
    )
    openfigi_candidate_rows = figi_decision_totals.get("missing_figi_openfigi_candidate", 0)
    identifier_backlog_rows = sum(
        row["missing_isin"] == "true" or row["missing_figi"] == "true"
        for row in rows
    )
    canada_identifier_backlog = {
        "status": (
            "complete_no_canada_identifier_backlog"
            if identifier_backlog_rows == 0
            else (
                "openfigi_candidates_pending"
                if openfigi_candidate_rows
                else "figi_queue_drained_remaining_isin_scope_or_reviewed_source_gaps"
            )
        ),
        "rows": identifier_backlog_rows,
        "scope_decision_required_rows": isin_apply_totals.get("blocked_until_scope_decision", 0),
        "official_isin_source_required_rows": isin_apply_totals.get("keep_blank_until_official_isin_source", 0),
        "figi_blocked_until_isin_rows": figi_apply_totals.get("blocked_until_isin_resolved", 0),
        "reviewed_openfigi_source_gap_rows": reviewed_openfigi_source_gap_rows,
        "openfigi_candidate_rows": openfigi_candidate_rows,
        "direct_identifier_apply_allowed_rows": 0,
        "metadata_enrichment_authorized": False,
        "source_gate": (
            "Canadian identifier work remains blocked unless a listing-keyed official CSD, issuer, prospectus, "
            "transfer-agent, OpenFIGI-by-ISIN, or reviewed stronger source proves the value; no symbol/name inference."
        ),
    }
    canada_review_strategy_totals: dict[str, Counter[str]] = {}
    canada_evidence_required_totals: dict[str, Counter[str]] = {}
    canada_review_batches: Counter[tuple[str, str, str]] = Counter()
    for row in rows:
        queue = row["canada_resolution_queue"]
        strategy = row.get("review_strategy", "")
        evidence_required = row.get("queue_evidence_required", "")
        canada_review_strategy_totals.setdefault(queue, Counter())[strategy] += 1
        canada_evidence_required_totals.setdefault(queue, Counter())[evidence_required] += 1
        sources = row["official_masterfile_sources"].split("|") if row["official_masterfile_sources"] else ["none"]
        for source in sources:
            canada_review_batches[(queue, row["exchange"], source)] += 1
    top_canada_review_batches = []
    for (queue, exchange, official_source), count in sorted(
        canada_review_batches.items(),
        key=lambda item: (-item[1], item[0][0], item[0][1], item[0][2]),
    )[:25]:
        strategy, evidence_required = canada_review_strategy_for(queue)
        top_canada_review_batches.append(
            {
                "canada_resolution_queue": queue,
                "exchange": exchange,
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
        "exchange_totals": dict(sorted(Counter(row["exchange"] for row in rows).items())),
        "asset_type_totals": dict(sorted(Counter(row["asset_type"] for row in rows).items())),
        "core_exclusion_candidate_rows": len(core_exclusion_rows),
        "core_exclusion_candidate_exchange_totals": dict(
            sorted(Counter(row["exchange"] for row in core_exclusion_rows).items())
        ),
        "core_exclusion_candidate_asset_type_totals": dict(
            sorted(Counter(row["asset_type"] for row in core_exclusion_rows).items())
        ),
        "core_exclusion_candidate_resolution_queue_totals": dict(
            sorted(Counter(row["canada_resolution_queue"] for row in core_exclusion_rows).items())
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
        "core_exclusion_candidate_source_gap_class_totals": dict(
            sorted(
                Counter(
                    gap_class
                    for row in core_exclusion_rows
                    for gap_class in (
                        row["source_gap_classes"].split("|") if row["source_gap_classes"] else ["none"]
                    )
                    if gap_class
                ).items()
            )
        ),
        "core_exclusion_candidate_examples": [
            {
                "listing_key": row["listing_key"],
                "ticker": row["ticker"],
                "exchange": row["exchange"],
                "asset_type": row["asset_type"],
                "name": row["name"],
                "canada_resolution_queue": row["canada_resolution_queue"],
                "official_masterfile_sources": row["official_masterfile_sources"] or "none",
                "source_gap_classes": row["source_gap_classes"] or "none",
                "source_gate": row.get("source_gate") or source_gate_for(row["canada_resolution_queue"]),
                "recommended_next_action": row["recommended_next_action"],
            }
            for row in core_exclusion_rows[:20]
        ],
        "missing_isin_rows": sum(row["missing_isin"] == "true" for row in rows),
        "missing_figi_rows": sum(row["missing_figi"] == "true" for row in rows),
        "canada_identifier_backlog": canada_identifier_backlog,
        "canada_identifier_backlog_queue_totals": dict(
            sorted(
                (queue, count)
                for queue, count in queue_totals.items()
                if queue != "residual_no_identifier_action"
            )
        ),
        "canada_identifier_backlog_evidence_required_totals": dict(
            sorted(
                Counter(
                    row["verification_evidence_required"]
                    for row in rows
                    if row["missing_isin"] == "true" or row["missing_figi"] == "true"
                ).items()
            )
        ),
        "canada_resolution_queue_totals": dict(sorted(queue_totals.items())),
        "canada_resolution_queue_exchange_totals": {
            queue: dict(sorted(Counter(row["exchange"] for row in rows if row["canada_resolution_queue"] == queue).items()))
            for queue in sorted({row["canada_resolution_queue"] for row in rows})
        },
        "canada_resolution_queue_asset_type_totals": {
            queue: dict(
                sorted(Counter(row["asset_type"] for row in rows if row["canada_resolution_queue"] == queue).items())
            )
            for queue in sorted({row["canada_resolution_queue"] for row in rows})
        },
        "canada_resolution_queue_source_gap_class_totals": {
            queue: dict(
                sorted(
                    Counter(
                        gap_class
                        for row in rows
                        if row["canada_resolution_queue"] == queue
                        for gap_class in (row["source_gap_classes"].split("|") if row["source_gap_classes"] else ["none"])
                        if gap_class
                    ).items()
                )
            )
            for queue in sorted({row["canada_resolution_queue"] for row in rows})
        },
        "canada_resolution_queue_official_source_totals": {
            queue: dict(
                sorted(
                    Counter(
                        source
                        for row in rows
                        if row["canada_resolution_queue"] == queue
                        for source in (
                            row["official_masterfile_sources"].split("|")
                            if row["official_masterfile_sources"]
                            else ["none"]
                        )
                        if source
                    ).items()
                )
            )
            for queue in sorted({row["canada_resolution_queue"] for row in rows})
        },
        "canada_resolution_queue_review_strategy_totals": {
            queue: dict(sorted(strategy_totals.items()))
            for queue, strategy_totals in sorted(canada_review_strategy_totals.items())
        },
        "canada_resolution_queue_evidence_required_totals": {
            queue: dict(sorted(evidence_totals.items()))
            for queue, evidence_totals in sorted(canada_evidence_required_totals.items())
        },
        "top_canada_resolution_review_batches": top_canada_review_batches,
        "isin_decision_totals": dict(sorted(Counter(row["isin_decision"] for row in rows).items())),
        "figi_decision_totals": dict(sorted(figi_decision_totals.items())),
        "isin_apply_eligibility_totals": dict(sorted(isin_apply_totals.items())),
        "figi_apply_eligibility_totals": dict(sorted(figi_apply_totals.items())),
        "verification_evidence_required_totals": dict(sorted(Counter(row["verification_evidence_required"] for row in rows).items())),
        "openfigi_review_status_totals": dict(
            sorted(Counter(row["openfigi_review_status"] for row in rows if row["openfigi_review_status"]).items())
        ),
        "openfigi_review_decision_totals": dict(
            sorted(Counter(row["openfigi_review_decision"] for row in rows if row["openfigi_review_decision"]).items())
        ),
        "source_gap_field_totals": dict(
            sorted(Counter(field for row in rows for field in row["source_gap_fields"].split("|") if field).items())
        ),
        "source_gap_class_totals": dict(
            sorted(Counter(gap_class for row in rows for gap_class in row["source_gap_classes"].split("|") if gap_class).items())
        ),
        "source_of_truth_outcome_totals": dict(
            sorted(Counter(outcome for row in rows for outcome in row["source_of_truth_outcomes"].split("|") if outcome).items())
        ),
        "official_masterfile_source_totals": dict(
            sorted(Counter(source for row in rows for source in row["official_masterfile_sources"].split("|") if source).items())
        ),
        "policy": {
            "tmx_first": "TMX/Cboe Canada official masterfiles are used as listing evidence, but current Canada official feeds do not expose ISIN or sector values.",
            "figi_gate": "FIGI enrichment is OpenFIGI-by-ISIN only; rows without ISIN must resolve the ISIN/source gate first.",
            "openfigi_review_gate": "Accepted OpenFIGI no-match and cross-ISIN collision reviews are retained as source gaps, not queued again as candidates.",
            "no_guessing": "No Canadian ISIN, FIGI, sector, or category is inferred from symbol or name shape.",
        },
    }


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDNAMES, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


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


def write_markdown(path: Path, summary: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Canada Residual Review",
        "",
        f"Generated at: `{summary['generated_at']}`",
        "",
        "This report tracks residual TSX/TSXV/NEO ISIN, FIGI, and metadata gaps with TMX/Cboe official-source context. It does not fill values.",
        "",
        "## Summary",
        "",
        f"- Review rows: `{summary['rows']}`",
        f"- Missing ISIN rows: `{summary['missing_isin_rows']}`",
        f"- Missing FIGI rows: `{summary['missing_figi_rows']}`",
        f"- Core-exclusion candidate rows requiring scope review: `{summary['core_exclusion_candidate_rows']}`",
        f"- Canada identifier backlog rows: `{summary['canada_identifier_backlog']['rows']}`",
        f"- OpenFIGI candidate rows: `{summary['canada_identifier_backlog']['openfigi_candidate_rows']}`",
        f"- Direct identifier apply allowed rows: `{summary['canada_identifier_backlog']['direct_identifier_apply_allowed_rows']}`",
        "",
        "## Canada Identifier Backlog",
        "",
        f"- Status: `{summary['canada_identifier_backlog']['status']}`",
        f"- Scope decision required rows: `{summary['canada_identifier_backlog']['scope_decision_required_rows']}`",
        f"- Official ISIN source required rows: `{summary['canada_identifier_backlog']['official_isin_source_required_rows']}`",
        f"- FIGI blocked until ISIN rows: `{summary['canada_identifier_backlog']['figi_blocked_until_isin_rows']}`",
        f"- Reviewed OpenFIGI source-gap rows: `{summary['canada_identifier_backlog']['reviewed_openfigi_source_gap_rows']}`",
        f"- Metadata enrichment authorized: `{str(summary['canada_identifier_backlog']['metadata_enrichment_authorized']).lower()}`",
        "",
        "| Queue | Rows |",
        "|---|---:|",
    ]
    for queue, count in summary["canada_identifier_backlog_queue_totals"].items():
        lines.append(f"| {queue} | {count} |")
    lines.extend(["", "| Evidence required | Rows |", "|---|---:|"])
    for evidence, count in summary["canada_identifier_backlog_evidence_required_totals"].items():
        lines.append(f"| {evidence} | {count} |")
    lines.extend(
        [
        "",
        "## Exchanges",
        "",
        "| Exchange | Rows |",
        "|---|---:|",
        ]
    )
    for exchange, count in summary["exchange_totals"].items():
        lines.append(f"| {exchange} | {count} |")
    lines.extend(["", "## ISIN Decisions", "", "| Decision | Rows |", "|---|---:|"])
    for decision, count in summary["isin_decision_totals"].items():
        lines.append(f"| {decision} | {count} |")
    lines.extend(["", "## FIGI Decisions", "", "| Decision | Rows |", "|---|---:|"])
    for decision, count in summary["figi_decision_totals"].items():
        lines.append(f"| {decision} | {count} |")
    lines.extend(["", "## ISIN Apply Eligibility", "", "| Eligibility | Rows |", "|---|---:|"])
    for eligibility, count in summary["isin_apply_eligibility_totals"].items():
        lines.append(f"| {eligibility} | {count} |")
    lines.extend(["", "## FIGI Apply Eligibility", "", "| Eligibility | Rows |", "|---|---:|"])
    for eligibility, count in summary["figi_apply_eligibility_totals"].items():
        lines.append(f"| {eligibility} | {count} |")
    lines.extend(["", "## Scope Blockers", "", "| Exchange | Rows |", "|---|---:|"])
    for exchange, count in summary["core_exclusion_candidate_exchange_totals"].items():
        lines.append(f"| {exchange} | {count} |")
    lines.extend(["", "| Asset type | Rows |", "|---|---:|"])
    for asset_type, count in summary["core_exclusion_candidate_asset_type_totals"].items():
        lines.append(f"| {asset_type} | {count} |")
    lines.extend(["", "| Resolution queue | Rows |", "|---|---:|"])
    for queue, count in summary["core_exclusion_candidate_resolution_queue_totals"].items():
        lines.append(f"| {queue} | {count} |")
    lines.extend(["", "| Official source | Rows |", "|---|---:|"])
    for source, count in summary["core_exclusion_candidate_official_source_totals"].items():
        lines.append(f"| {source} | {count} |")
    lines.extend(["", "| Source gap class | Rows |", "|---|---:|"])
    for gap_class, count in summary["core_exclusion_candidate_source_gap_class_totals"].items():
        lines.append(f"| {gap_class} | {count} |")
    lines.extend(["", "## Canada Resolution Queues", "", "| Queue | Rows |", "|---|---:|"])
    for queue, count in summary["canada_resolution_queue_totals"].items():
        lines.append(f"| {queue} | {count} |")
    lines.extend(["", "| Queue | Asset type | Rows |", "|---|---|---:|"])
    for queue, asset_type_totals in summary["canada_resolution_queue_asset_type_totals"].items():
        for asset_type, count in asset_type_totals.items():
            lines.append(f"| {queue} | {asset_type} | {count} |")
    lines.extend(["", "| Queue | Source gap class | Rows |", "|---|---|---:|"])
    for queue, source_gap_class_totals in summary["canada_resolution_queue_source_gap_class_totals"].items():
        for source_gap_class, count in source_gap_class_totals.items():
            lines.append(f"| {queue} | {source_gap_class} | {count} |")
    lines.extend(["", "| Queue | Official source | Rows |", "|---|---|---:|"])
    for queue, official_source_totals in summary["canada_resolution_queue_official_source_totals"].items():
        for source, count in official_source_totals.items():
            lines.append(f"| {queue} | {source} | {count} |")
    lines.extend(["", "## Canada Review Strategies", "", "| Queue | Strategy | Rows |", "|---|---|---:|"])
    for queue, strategy_totals in summary["canada_resolution_queue_review_strategy_totals"].items():
        for strategy, count in strategy_totals.items():
            lines.append(f"| {queue} | {strategy} | {count} |")
    lines.extend(["", "## Canada Queue Evidence", "", "| Queue | Evidence required | Rows |", "|---|---|---:|"])
    for queue, evidence_totals in summary["canada_resolution_queue_evidence_required_totals"].items():
        for evidence_required, count in evidence_totals.items():
            lines.append(f"| {queue} | {evidence_required} | {count} |")
    lines.extend(
        [
            "",
            "## Top Canada Review Batches",
            "",
            "| Queue | Exchange | Official source | Rows | Review strategy | Evidence required | Recommended next source | Source gate |",
            "|---|---|---|---:|---|---|---|---|",
        ]
    )
    for batch in summary["top_canada_resolution_review_batches"]:
        lines.append(
            "| "
            f"{batch['canada_resolution_queue']} | "
            f"{batch['exchange']} | "
            f"{batch['official_source']} | "
            f"{batch['rows']} | "
            f"{batch['review_strategy']} | "
            f"{batch['evidence_required']} | "
            f"{batch['recommended_next_source']} | "
            f"{batch['source_gate']} |"
        )
    lines.extend(["", "## Verification Evidence", "", "| Evidence Gate | Rows |", "|---|---:|"])
    for evidence, count in summary["verification_evidence_required_totals"].items():
        lines.append(f"| {evidence} | {count} |")
    lines.extend(["", "## OpenFIGI Reviews", "", "| Review status | Rows |", "|---|---:|"])
    for status, count in summary["openfigi_review_status_totals"].items():
        lines.append(f"| {status} | {count} |")
    lines.extend(["", "## Source Gap Fields", "", "| Field | Rows |", "|---|---:|"])
    for field, count in summary["source_gap_field_totals"].items():
        lines.append(f"| {field} | {count} |")
    lines.extend(
        [
            "",
            "## Policy",
            "",
            "- TMX/Cboe official rows are listing evidence; they are not treated as ISIN/sector evidence when those fields are absent.",
            "- OpenFIGI is only a FIGI source after a row already has a valid ISIN.",
            "- Accepted OpenFIGI no-match/collision reviews stay blank and documented until a stronger identifier source exists.",
            "- Scope candidates must be reviewed before identifier or sector enrichment.",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a listing-keyed Canada residual review report.")
    parser.add_argument("--tickers-csv", type=Path, default=DEFAULT_TICKERS_CSV)
    parser.add_argument("--identifiers-extended-csv", type=Path, default=DEFAULT_IDENTIFIERS_EXTENDED_CSV)
    parser.add_argument("--masterfile-reference-csv", type=Path, default=DEFAULT_MASTERFILE_REFERENCE_CSV)
    parser.add_argument("--source-gap-classification-csv", type=Path, default=DEFAULT_SOURCE_GAP_CLASSIFICATION_CSV)
    parser.add_argument("--source-of-truth-decisions-csv", type=Path, default=DEFAULT_SOURCE_OF_TRUTH_DECISIONS_CSV)
    parser.add_argument("--openfigi-gap-csv", type=Path, default=DEFAULT_OPENFIGI_GAP_CSV)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_CSV_OUT)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_JSON_OUT)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_MD_OUT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    rows = build_review_rows(
        tickers=load_csv(args.tickers_csv),
        identifiers_extended=load_csv(args.identifiers_extended_csv),
        masterfile_rows=load_csv(args.masterfile_reference_csv),
        source_gap_rows=load_csv(args.source_gap_classification_csv),
        source_of_truth_rows=load_csv(args.source_of_truth_decisions_csv),
        openfigi_gap_rows=load_csv(args.openfigi_gap_csv),
    )
    summary = summarize(rows, utc_now_iso())
    write_csv(args.csv_out, rows)
    payload = build_json_payload(
        summary=summary,
        rows=rows,
        source_files={
            "tickers_csv": display_path(args.tickers_csv),
            "identifiers_extended_csv": display_path(args.identifiers_extended_csv),
            "masterfile_reference_csv": display_path(args.masterfile_reference_csv),
            "source_gap_classification_csv": display_path(args.source_gap_classification_csv),
            "source_of_truth_decisions_csv": display_path(args.source_of_truth_decisions_csv),
            "openfigi_gap_csv": display_path(args.openfigi_gap_csv),
        },
    )
    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.json_out.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
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
                "missing_isin_rows": summary["missing_isin_rows"],
                "missing_figi_rows": summary["missing_figi_rows"],
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()

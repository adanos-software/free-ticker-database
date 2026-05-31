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

DEFAULT_TICKERS_CSV = DATA_DIR / "tickers.csv"
DEFAULT_MASTERFILE_REFERENCE_CSV = DATA_DIR / "masterfiles" / "reference.csv"
DEFAULT_SOURCE_GAP_CLASSIFICATION_CSV = REPORTS_DIR / "source_gap_classification.csv"
DEFAULT_SOURCE_OF_TRUTH_DECISIONS_CSV = REPORTS_DIR / "source_of_truth_decisions.csv"
DEFAULT_CSV_OUT = REPORTS_DIR / "weak_sector_residual_review.csv"
DEFAULT_JSON_OUT = REPORTS_DIR / "weak_sector_residual_review.json"
DEFAULT_MD_OUT = REPORTS_DIR / "weak_sector_residual_review.md"

DEFAULT_EXCHANGES = ("BK", "CSE_MA", "SEM", "PSE", "CSE_LK", "NGX", "OSL", "Euronext")
SECTOR_GAP_FIELDS = {"missing_sector_stock", "missing_stock_sector"}

CSV_FIELDNAMES = [
    "listing_key",
    "ticker",
    "exchange",
    "asset_type",
    "name",
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
    "official_masterfile_sector_values",
    "official_masterfile_exposes_sector",
    "weak_sector_resolution_queue",
    "residual_decision",
    "review_bucket",
    "review_priority",
    "apply_eligibility",
    "verification_evidence_required",
    "review_strategy",
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
    return {(row.get("field", ""), listing_key(row)): row for row in rows}


def build_masterfile_lookup(rows: list[dict[str, str]]) -> dict[str, list[dict[str, str]]]:
    grouped: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        if row.get("official") != "true" or row.get("listing_status") != "active":
            continue
        key = f"{row.get('exchange', '')}::{row.get('ticker', '')}"
        if key != "::":
            grouped.setdefault(key, []).append(row)
    return grouped


def residual_decision_for(gap: dict[str, str], truth: dict[str, str], refs: list[dict[str, str]]) -> str:
    if truth.get("source_of_truth_outcome") == "core_exclusion_candidate":
        return "core_exclusion_candidate_requires_scope_review"
    if any(ref.get("sector", "").strip() for ref in refs):
        return "official_sector_available_review_apply"
    if refs:
        return "accepted_source_gap_official_masterfile_without_sector"
    if gap.get("gap_class") == "official_industry_taxonomy_unavailable_gap":
        return "accepted_source_gap_no_official_sector_taxonomy"
    return "accepted_source_gap_requires_venue_specific_sector_source"


def next_action_for(residual_decision: str) -> str:
    if residual_decision == "official_sector_available_review_apply":
        return "apply_only_after_official_sector_normalization_and_listing_key_gate"
    if residual_decision == "core_exclusion_candidate_requires_scope_review":
        return "review_scope_before_sector_enrichment"
    return "keep_sector_blank_until_official_or_reviewed_venue_specific_taxonomy_exists"


def review_bucket_for(residual_decision: str) -> str:
    if residual_decision == "official_sector_available_review_apply":
        return "official_sector_candidate_requires_normalization_gate"
    if residual_decision == "core_exclusion_candidate_requires_scope_review":
        return "scope_review_before_sector_fill"
    if residual_decision == "accepted_source_gap_official_masterfile_without_sector":
        return "official_masterfile_without_sector_source_gap"
    if residual_decision == "accepted_source_gap_no_official_sector_taxonomy":
        return "venue_official_taxonomy_unavailable_source_gap"
    return "venue_specific_sector_source_gap"


def review_priority_for(review_bucket: str) -> str:
    if review_bucket in {
        "official_sector_candidate_requires_normalization_gate",
        "scope_review_before_sector_fill",
    }:
        return "P1"
    if review_bucket == "venue_official_taxonomy_unavailable_source_gap":
        return "P2"
    if review_bucket == "official_masterfile_without_sector_source_gap":
        return "P3"
    return "P4"


def apply_eligibility_for(review_bucket: str) -> str:
    if review_bucket == "official_sector_candidate_requires_normalization_gate":
        return "blocked_until_canonical_sector_normalization_and_listing_key_gate"
    if review_bucket == "scope_review_before_sector_fill":
        return "blocked_until_core_or_extended_scope_decision"
    if review_bucket == "official_masterfile_without_sector_source_gap":
        return "keep_sector_blank_until_official_masterfile_exposes_sector_or_reviewed_taxonomy"
    if review_bucket == "venue_official_taxonomy_unavailable_source_gap":
        return "keep_sector_blank_until_venue_official_taxonomy_source_exists"
    return "keep_sector_blank_until_reviewed_venue_specific_source"


def verification_evidence_for(review_bucket: str) -> str:
    if review_bucket == "official_sector_candidate_requires_normalization_gate":
        return "official_venue_sector_value_with_canonical_mapping_and_exact_listing_key_match"
    if review_bucket == "scope_review_before_sector_fill":
        return "scope_decision_for_core_extended_or_exclude_before_sector_enrichment"
    if review_bucket == "official_masterfile_without_sector_source_gap":
        return "official_masterfile_or_issuer_taxonomy_update_exposing_sector_for_exact_listing"
    if review_bucket == "venue_official_taxonomy_unavailable_source_gap":
        return "new_or_restored_official_venue_industry_taxonomy_source"
    return "reviewed_venue_specific_taxonomy_source_with_exact_listing_match"


def weak_sector_resolution_queue_for(row: dict[str, str]) -> str:
    residual_decision = row["residual_decision"]
    if residual_decision == "official_sector_available_review_apply":
        return "official_sector_candidate_normalization_review"
    if residual_decision == "core_exclusion_candidate_requires_scope_review":
        return "core_exclusion_candidate_scope_review_before_sector_fill"
    if residual_decision == "accepted_source_gap_official_masterfile_without_sector":
        return "official_masterfile_without_sector_source_gap"
    if residual_decision == "accepted_source_gap_no_official_sector_taxonomy":
        return "venue_official_taxonomy_unavailable_source_gap"
    if residual_decision == "accepted_source_gap_requires_venue_specific_sector_source":
        return "venue_specific_sector_source_gap"
    return "out_of_scope_or_unknown_review"


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


def official_sources_for_summary(row: dict[str, str]) -> list[str]:
    sources = [source for source in row["official_masterfile_sources"].split("|") if source]
    return sources or ["none"]


def official_source_context_for(row: dict[str, str]) -> str:
    sources = row.get("official_masterfile_sources") or "none"
    values = row.get("official_masterfile_sector_values") or "none"
    return f"official_masterfile_sources={sources};official_sector_values={values}"


def official_capability_for(row: dict[str, str]) -> str:
    return (
        f"masterfile_match={row.get('official_masterfile_match', '')};"
        f"masterfile_exposes_sector={row.get('official_masterfile_exposes_sector', '')}"
    )


def build_review_rows(
    *,
    tickers: list[dict[str, str]],
    masterfile_rows: list[dict[str, str]],
    source_gap_rows: list[dict[str, str]],
    source_of_truth_rows: list[dict[str, str]],
    exchanges: set[str],
) -> list[dict[str, str]]:
    ticker_lookup = build_lookup(tickers)
    truth_lookup = build_truth_lookup(source_of_truth_rows)
    masterfile_lookup = build_masterfile_lookup(masterfile_rows)
    rows: list[dict[str, str]] = []
    for gap in source_gap_rows:
        if gap.get("exchange") not in exchanges or gap.get("field") not in SECTOR_GAP_FIELDS:
            continue
        key = listing_key(gap)
        ticker = ticker_lookup.get(key, {})
        truth = truth_lookup.get((gap.get("field", ""), key), {})
        refs = masterfile_lookup.get(key, [])
        sectors = sorted({ref.get("sector", "").strip() for ref in refs if ref.get("sector", "").strip()})
        residual_decision = residual_decision_for(gap, truth, refs)
        review_bucket = review_bucket_for(residual_decision)
        row = {
            "listing_key": key,
            "ticker": gap.get("ticker", ""),
            "exchange": gap.get("exchange", ""),
            "asset_type": gap.get("asset_type", ""),
            "name": gap.get("name", "") or ticker.get("name", ""),
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
            "official_masterfile_sector_values": "|".join(sectors),
            "official_masterfile_exposes_sector": "true" if sectors else "false",
            "weak_sector_resolution_queue": "",
            "residual_decision": residual_decision,
            "review_bucket": review_bucket,
            "review_priority": review_priority_for(review_bucket),
            "apply_eligibility": apply_eligibility_for(review_bucket),
            "verification_evidence_required": verification_evidence_for(review_bucket),
            "review_strategy": "",
            "recommended_next_action": next_action_for(residual_decision),
        }
        row["weak_sector_resolution_queue"] = weak_sector_resolution_queue_for(row)
        row["review_strategy"] = weak_sector_review_strategy_for(row["weak_sector_resolution_queue"])[0]
        source_context = "|".join(official_sources_for_summary(row))
        row["recommended_next_source"] = weak_sector_recommended_next_source_for(
            row["weak_sector_resolution_queue"],
            source_context,
        )
        row["source_gate"] = weak_sector_source_gate_for(row["weak_sector_resolution_queue"])
        row["official_source_context"] = official_source_context_for(row)
        row["official_capability"] = official_capability_for(row)
        rows.append(row)
    return sorted(rows, key=lambda row: (row["review_priority"], row["review_bucket"], row["exchange"], row["ticker"], row["listing_key"]))


def summarize(rows: list[dict[str, str]], generated_at: str, exchanges: set[str]) -> dict[str, Any]:
    official_sector_candidate_rows = [
        row for row in rows if row["residual_decision"] == "official_sector_available_review_apply"
    ]
    scope_review_rows = [
        row for row in rows if row["residual_decision"] == "core_exclusion_candidate_requires_scope_review"
    ]
    masterfile_without_sector_rows = [
        row for row in rows if row["residual_decision"] == "accepted_source_gap_official_masterfile_without_sector"
    ]
    apply_eligibility_totals = Counter(row["apply_eligibility"] for row in rows)
    verification_evidence_required_totals = Counter(row["verification_evidence_required"] for row in rows)
    queue_totals = Counter(row["weak_sector_resolution_queue"] for row in rows)
    weak_sector_backlog = {
        "status": "venue_specific_review_queue_open",
        "rows": len(rows),
        "official_sector_candidate_rows": len(official_sector_candidate_rows),
        "scope_decision_required_rows": len(scope_review_rows),
        "masterfile_without_sector_rows": len(masterfile_without_sector_rows),
        "venue_taxonomy_source_required_rows": queue_totals.get("venue_official_taxonomy_unavailable_source_gap", 0),
        "direct_sector_apply_allowed_rows": 0,
        "metadata_enrichment_authorized": False,
        "source_gate": (
            "Weak-sector enrichment remains blocked unless venue-official masterfile, issuer, or reviewed taxonomy "
            "evidence maps the exact listing to a canonical sector; no global symbol/name/peer inference is allowed."
        ),
    }
    weak_sector_review_strategy_totals: dict[str, Counter[str]] = {}
    weak_sector_official_capability_totals: dict[str, Counter[str]] = {}
    weak_sector_review_batches: Counter[tuple[str, str, str]] = Counter()
    venue_backlog_batches: Counter[tuple[str, str, str]] = Counter()
    for row in rows:
        queue = row["weak_sector_resolution_queue"]
        strategy, _evidence_required = weak_sector_review_strategy_for(queue)
        weak_sector_review_strategy_totals.setdefault(queue, Counter())[strategy] += 1
        weak_sector_official_capability_totals.setdefault(queue, Counter())[
            f"masterfile_match={row['official_masterfile_match']}"
        ] += 1
        weak_sector_official_capability_totals.setdefault(queue, Counter())[
            f"masterfile_exposes_sector={row['official_masterfile_exposes_sector']}"
        ] += 1
        for source in official_sources_for_summary(row):
            weak_sector_review_batches[(queue, row["exchange"], source)] += 1
            venue_backlog_batches[(row["exchange"], queue, source)] += 1
    top_weak_sector_review_batches = []
    for (queue, exchange, official_source), count in sorted(
        weak_sector_review_batches.items(),
        key=lambda item: (-item[1], item[0][0], item[0][1], item[0][2]),
    )[:25]:
        strategy, evidence_required = weak_sector_review_strategy_for(queue)
        top_weak_sector_review_batches.append(
            {
                "weak_sector_resolution_queue": queue,
                "exchange": exchange,
                "official_source": official_source,
                "rows": count,
                "review_strategy": strategy,
                "evidence_required": evidence_required,
                "recommended_next_source": weak_sector_recommended_next_source_for(queue, official_source),
                "source_gate": weak_sector_source_gate_for(queue),
            }
        )
    return {
        "generated_at": generated_at,
        "exchanges": sorted(exchanges),
        "rows": len(rows),
        "exchange_totals": dict(sorted(Counter(row["exchange"] for row in rows).items())),
        "official_sector_candidate_rows": len(official_sector_candidate_rows),
        "official_sector_candidate_exchange_totals": dict(
            sorted(Counter(row["exchange"] for row in official_sector_candidate_rows).items())
        ),
        "official_sector_candidate_value_totals": dict(
            sorted(
                Counter(
                    sector
                    for row in official_sector_candidate_rows
                    for sector in row["official_masterfile_sector_values"].split("|")
                    if sector
                ).items()
            )
        ),
        "scope_review_rows": len(scope_review_rows),
        "scope_review_exchange_totals": dict(sorted(Counter(row["exchange"] for row in scope_review_rows).items())),
        "scope_review_gap_class_totals": dict(sorted(Counter(row["gap_class"] for row in scope_review_rows).items())),
        "masterfile_without_sector_rows": len(masterfile_without_sector_rows),
        "masterfile_without_sector_exchange_totals": dict(
            sorted(Counter(row["exchange"] for row in masterfile_without_sector_rows).items())
        ),
        "gap_class_totals": dict(sorted(Counter(row["gap_class"] for row in rows).items())),
        "source_of_truth_outcome_totals": dict(sorted(Counter(row["source_of_truth_outcome"] for row in rows).items())),
        "weak_sector_backlog": weak_sector_backlog,
        "weak_sector_backlog_queue_totals": dict(sorted(queue_totals.items())),
        "weak_sector_backlog_evidence_required_totals": dict(sorted(verification_evidence_required_totals.items())),
        "weak_sector_resolution_queue_totals": dict(sorted(queue_totals.items())),
        "weak_sector_resolution_queue_exchange_totals": {
            queue: dict(sorted(Counter(row["exchange"] for row in rows if row["weak_sector_resolution_queue"] == queue).items()))
            for queue in sorted({row["weak_sector_resolution_queue"] for row in rows})
        },
        "weak_sector_resolution_queue_gap_class_totals": {
            queue: dict(
                sorted(Counter(row["gap_class"] for row in rows if row["weak_sector_resolution_queue"] == queue).items())
            )
            for queue in sorted({row["weak_sector_resolution_queue"] for row in rows})
        },
        "weak_sector_resolution_queue_official_source_totals": {
            queue: dict(
                sorted(
                    Counter(
                        source
                        for row in rows
                        if row["weak_sector_resolution_queue"] == queue
                        for source in official_sources_for_summary(row)
                    ).items()
                )
            )
            for queue in sorted({row["weak_sector_resolution_queue"] for row in rows})
        },
        "weak_sector_resolution_queue_review_strategy_totals": {
            queue: dict(sorted(strategy_totals.items()))
            for queue, strategy_totals in sorted(weak_sector_review_strategy_totals.items())
        },
        "weak_sector_resolution_queue_official_capability_totals": {
            queue: dict(sorted(capability_totals.items()))
            for queue, capability_totals in sorted(weak_sector_official_capability_totals.items())
        },
        "venue_backlog_exchange_queue_totals": {
            exchange: dict(
                sorted(Counter(row["weak_sector_resolution_queue"] for row in rows if row["exchange"] == exchange).items())
            )
            for exchange in sorted({row["exchange"] for row in rows})
        },
        "venue_backlog_exchange_official_capability_totals": {
            exchange: dict(
                sorted(
                    Counter(
                        capability
                        for row in rows
                        if row["exchange"] == exchange
                        for capability in (
                            f"masterfile_match={row['official_masterfile_match']}",
                            f"masterfile_exposes_sector={row['official_masterfile_exposes_sector']}",
                        )
                    ).items()
                )
            )
            for exchange in sorted({row["exchange"] for row in rows})
        },
        "top_venue_backlog_batches": [
            {
                "exchange": exchange,
                "weak_sector_resolution_queue": queue,
                "official_source": official_source,
                "rows": count,
                "review_strategy": weak_sector_review_strategy_for(queue)[0],
                "evidence_required": weak_sector_review_strategy_for(queue)[1],
                "recommended_next_source": weak_sector_recommended_next_source_for(queue, official_source),
                "source_gate": weak_sector_source_gate_for(queue),
            }
            for (exchange, queue, official_source), count in sorted(
                venue_backlog_batches.items(),
                key=lambda item: (-item[1], item[0][0], item[0][1], item[0][2]),
            )[:25]
        ],
        "top_weak_sector_resolution_review_batches": top_weak_sector_review_batches,
        "residual_decision_totals": dict(sorted(Counter(row["residual_decision"] for row in rows).items())),
        "review_bucket_totals": dict(sorted(Counter(row["review_bucket"] for row in rows).items())),
        "review_priority_totals": dict(sorted(Counter(row["review_priority"] for row in rows).items())),
        "apply_eligibility_totals": dict(sorted(apply_eligibility_totals.items())),
        "verification_evidence_required_totals": dict(sorted(verification_evidence_required_totals.items())),
        "official_masterfile_match_totals": dict(
            sorted(Counter(row["official_masterfile_match"] for row in rows).items())
        ),
        "official_masterfile_exposes_sector_totals": dict(
            sorted(Counter(row["official_masterfile_exposes_sector"] for row in rows).items())
        ),
        "official_masterfile_source_totals": dict(
            sorted(Counter(source for row in rows for source in row["official_masterfile_sources"].split("|") if source).items())
        ),
        "official_sector_value_totals": dict(
            sorted(Counter(sector for row in rows for sector in row["official_masterfile_sector_values"].split("|") if sector).items())
        ),
        "policy": {
            "venue_specific": "Rows are reviewed only for the configured weak-sector venues; no global symbol or name mapping is used.",
            "official_first": "Official masterfile sector values are treated as review candidates, not automatically written.",
            "no_guessing": "No stock sector is inferred from ticker, issuer name, ISIN prefix, or peer instruments.",
        },
    }


def build_json_payload(
    *,
    summary: dict[str, Any],
    rows: list[dict[str, str]],
    source_files: dict[str, str],
    exchanges: set[str],
) -> dict[str, Any]:
    return {
        "_meta": {
            "generated_at": summary["generated_at"],
            "source_files": source_files,
            "exchanges": sorted(exchanges),
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
        "# Weak Sector Residual Review",
        "",
        f"Generated at: `{summary['generated_at']}`",
        "",
        "This report tracks stock-sector gaps for weak-coverage venues using venue-specific official masterfile context. It does not fill sectors.",
        "",
        "## Summary",
        "",
        f"- Review rows: `{summary['rows']}`",
        f"- Exchanges: `{', '.join(summary['exchanges'])}`",
        f"- Official sector candidates needing canonical mapping gate: `{summary['official_sector_candidate_rows']}`",
        f"- Scope review blockers: `{summary['scope_review_rows']}`",
        f"- Official masterfile rows without sector: `{summary['masterfile_without_sector_rows']}`",
        f"- Direct sector apply allowed rows: `{summary['weak_sector_backlog']['direct_sector_apply_allowed_rows']}`",
        "",
        "## Weak Sector Backlog",
        "",
        f"- Status: `{summary['weak_sector_backlog']['status']}`",
        f"- Rows: `{summary['weak_sector_backlog']['rows']}`",
        f"- Official sector candidate rows: `{summary['weak_sector_backlog']['official_sector_candidate_rows']}`",
        f"- Scope decision required rows: `{summary['weak_sector_backlog']['scope_decision_required_rows']}`",
        f"- Masterfile without sector rows: `{summary['weak_sector_backlog']['masterfile_without_sector_rows']}`",
        f"- Venue taxonomy source required rows: `{summary['weak_sector_backlog']['venue_taxonomy_source_required_rows']}`",
        f"- Metadata enrichment authorized: `{str(summary['weak_sector_backlog']['metadata_enrichment_authorized']).lower()}`",
        "",
        "| Queue | Rows |",
        "|---|---:|",
    ]
    for queue, count in summary["weak_sector_backlog_queue_totals"].items():
        lines.append(f"| {queue} | {count} |")
    lines.extend(["", "| Evidence required | Rows |", "|---|---:|"])
    for evidence, count in summary["weak_sector_backlog_evidence_required_totals"].items():
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
    lines.extend(["", "## Residual Decisions", "", "| Decision | Rows |", "|---|---:|"])
    for decision, count in summary["residual_decision_totals"].items():
        lines.append(f"| {decision} | {count} |")
    lines.extend(["", "## Weak Sector Resolution Queues", "", "| Queue | Rows |", "|---|---:|"])
    for queue, count in summary["weak_sector_resolution_queue_totals"].items():
        lines.append(f"| {queue} | {count} |")
    lines.extend(["", "| Queue | Gap class | Rows |", "|---|---|---:|"])
    for queue, gap_class_totals in summary["weak_sector_resolution_queue_gap_class_totals"].items():
        for gap_class, count in gap_class_totals.items():
            lines.append(f"| {queue} | {gap_class} | {count} |")
    lines.extend(["", "| Queue | Official source | Rows |", "|---|---|---:|"])
    for queue, source_totals in summary["weak_sector_resolution_queue_official_source_totals"].items():
        for source, count in source_totals.items():
            lines.append(f"| {queue} | {source} | {count} |")
    lines.extend(["", "| Queue | Strategy | Rows |", "|---|---|---:|"])
    for queue, strategy_totals in summary["weak_sector_resolution_queue_review_strategy_totals"].items():
        for strategy, count in strategy_totals.items():
            lines.append(f"| {queue} | {strategy} | {count} |")
    lines.extend(["", "| Queue | Official capability | Rows |", "|---|---|---:|"])
    for queue, capability_totals in summary["weak_sector_resolution_queue_official_capability_totals"].items():
        for capability, count in capability_totals.items():
            lines.append(f"| {queue} | {capability} | {count} |")
    lines.extend(["", "## Venue Backlog", "", "| Exchange | Queue | Rows |", "|---|---|---:|"])
    for exchange, queue_totals in summary["venue_backlog_exchange_queue_totals"].items():
        for queue, count in queue_totals.items():
            lines.append(f"| {exchange} | {queue} | {count} |")
    lines.extend(["", "| Exchange | Official capability | Rows |", "|---|---|---:|"])
    for exchange, capability_totals in summary["venue_backlog_exchange_official_capability_totals"].items():
        for capability, count in capability_totals.items():
            lines.append(f"| {exchange} | {capability} | {count} |")
    lines.extend(
        [
            "",
            "## Top Venue Backlog Batches",
            "",
            "| Exchange | Queue | Official source | Rows | Review strategy | Evidence required | Recommended next source | Source gate |",
            "|---|---|---|---:|---|---|---|---|",
        ]
    )
    for batch in summary["top_venue_backlog_batches"]:
        lines.append(
            f"| {batch['exchange']} | {batch['weak_sector_resolution_queue']} | {batch['official_source']} | "
            f"{batch['rows']} | {batch['review_strategy']} | {batch['evidence_required']} | "
            f"{batch['recommended_next_source']} | {batch['source_gate']} |"
        )
    lines.extend(
        [
            "",
            "## Top Weak Sector Review Batches",
            "",
            "| Queue | Exchange | Official source | Strategy | Evidence required | Recommended next source | Source gate | Rows |",
            "|---|---|---|---|---|---|---|---:|",
        ]
    )
    for batch in summary["top_weak_sector_resolution_review_batches"]:
        lines.append(
            f"| {batch['weak_sector_resolution_queue']} | {batch['exchange']} | {batch['official_source']} | "
            f"{batch['review_strategy']} | {batch['evidence_required']} | {batch['recommended_next_source']} | "
            f"{batch['source_gate']} | {batch['rows']} |"
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
    lines.extend(["", "## Verification Evidence", "", "| Evidence Required | Rows |", "|---|---:|"])
    for evidence, count in summary["verification_evidence_required_totals"].items():
        lines.append(f"| {evidence} | {count} |")
    lines.extend(["", "## Review Queues", "", "| Queue | Rows |", "|---|---:|"])
    lines.append(f"| official_sector_candidate_rows | {summary['official_sector_candidate_rows']} |")
    lines.append(f"| scope_review_rows | {summary['scope_review_rows']} |")
    lines.append(f"| masterfile_without_sector_rows | {summary['masterfile_without_sector_rows']} |")
    lines.extend(["", "## Scope Review Exchanges", "", "| Exchange | Rows |", "|---|---:|"])
    for exchange, count in summary["scope_review_exchange_totals"].items():
        lines.append(f"| {exchange} | {count} |")
    lines.extend(["", "## Masterfile Without Sector Exchanges", "", "| Exchange | Rows |", "|---|---:|"])
    for exchange, count in summary["masterfile_without_sector_exchange_totals"].items():
        lines.append(f"| {exchange} | {count} |")
    lines.extend(["", "## Official Sector Values", "", "| Sector | Rows |", "|---|---:|"])
    for sector, count in summary["official_sector_value_totals"].items():
        lines.append(f"| {sector} | {count} |")
    lines.extend(["", "## Rows", "", "| Listing key | Priority | Bucket | Exchange | Class | Official sector | Decision |", "|---|---|---|---|---|---|---|"])
    for row in rows:
        lines.append(
            f"| {row['listing_key']} | {row['review_priority']} | {row['review_bucket']} | {row['exchange']} | {row['gap_class']} | "
            f"{row['official_masterfile_sector_values']} | {row['residual_decision']} |"
        )
    lines.extend(
        [
            "",
            "## Policy",
            "",
            "- Do not infer sectors from names, ticker families, ISIN prefixes, or cross-venue peers.",
            "- Scope candidates require explicit scope review before sector enrichment.",
            "- Official sector values require canonical normalization and listing-key gates before any future apply step.",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build weak-venue stock-sector residual review report.")
    parser.add_argument("--tickers-csv", type=Path, default=DEFAULT_TICKERS_CSV)
    parser.add_argument("--masterfile-reference-csv", type=Path, default=DEFAULT_MASTERFILE_REFERENCE_CSV)
    parser.add_argument("--source-gap-classification-csv", type=Path, default=DEFAULT_SOURCE_GAP_CLASSIFICATION_CSV)
    parser.add_argument("--source-of-truth-decisions-csv", type=Path, default=DEFAULT_SOURCE_OF_TRUTH_DECISIONS_CSV)
    parser.add_argument("--exchanges", default=",".join(DEFAULT_EXCHANGES))
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_CSV_OUT)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_JSON_OUT)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_MD_OUT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    exchanges = {exchange.strip() for exchange in args.exchanges.split(",") if exchange.strip()}
    rows = build_review_rows(
        tickers=load_csv(args.tickers_csv),
        masterfile_rows=load_csv(args.masterfile_reference_csv),
        source_gap_rows=load_csv(args.source_gap_classification_csv),
        source_of_truth_rows=load_csv(args.source_of_truth_decisions_csv),
        exchanges=exchanges,
    )
    summary = summarize(rows, utc_now_iso(), exchanges)
    write_csv(args.csv_out, rows)
    payload = build_json_payload(
        summary=summary,
        rows=rows,
        source_files={
            "tickers_csv": display_path(args.tickers_csv),
            "masterfile_reference_csv": display_path(args.masterfile_reference_csv),
            "source_gap_classification_csv": display_path(args.source_gap_classification_csv),
            "source_of_truth_decisions_csv": display_path(args.source_of_truth_decisions_csv),
        },
        exchanges=exchanges,
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
                "exchange_totals": summary["exchange_totals"],
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

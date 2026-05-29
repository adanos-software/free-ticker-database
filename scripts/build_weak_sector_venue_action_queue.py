from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = ROOT / "data" / "reports"

DEFAULT_WEAK_SECTOR_REVIEW_CSV = REPORTS_DIR / "weak_sector_residual_review.csv"
DEFAULT_CSV_OUT = REPORTS_DIR / "weak_sector_venue_action_queue.csv"
DEFAULT_JSON_OUT = REPORTS_DIR / "weak_sector_venue_action_queue.json"
DEFAULT_MD_OUT = REPORTS_DIR / "weak_sector_venue_action_queue.md"

CSV_FIELDNAMES = [
    "exchange",
    "weak_sector_resolution_queue",
    "official_masterfile_sources",
    "official_masterfile_sector_values",
    "rows",
    "review_priority",
    "action_queue",
    "review_strategy",
    "evidence_required",
    "source_gate",
    "recommended_next_action",
    "gap_class_totals",
    "source_of_truth_outcome_totals",
    "example_listing_keys",
]


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def load_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def join_counter(counter: Counter[str]) -> str:
    return ";".join(f"{key}:{count}" for key, count in sorted(counter.items()) if key)


def action_for(queue: str) -> tuple[str, str, str, str, str]:
    if queue == "official_sector_candidate_normalization_review":
        return (
            "review_official_sector_value_to_canonical_mapping",
            "normalize_official_sector_candidate_before_apply",
            "official_venue_sector_value_with_canonical_mapping_and_exact_listing_key_match",
            "Do not map broad official labels without an explicit canonical-sector rule and exact listing-key evidence.",
            (
                "Review whether the official raw sector value can be mapped to a canonical sector; leave sector "
                "blank unless that mapping is defensible."
            ),
        )
    if queue == "core_exclusion_candidate_scope_review_before_sector_fill":
        return (
            "decide_scope_before_sector_enrichment",
            "scope_review_before_weak_sector_enrichment",
            "scope_decision_for_core_extended_or_exclude_before_sector_enrichment",
            "No sector fill until scope is decided as core, extended, or excluded.",
            "Resolve listing scope first; only core or explicitly extended listings may continue to sector enrichment.",
        )
    if queue == "official_masterfile_without_sector_source_gap":
        return (
            "seek_official_masterfile_or_issuer_sector_update",
            "keep_blank_until_official_masterfile_or_issuer_sector_source",
            "official_masterfile_or_issuer_taxonomy_update_exposing_sector_for_exact_listing",
            "Keep sector blank until an official masterfile or issuer record exposes sector for the exact listing.",
            "Look for an updated venue masterfile or issuer taxonomy with listing-keyed sector evidence.",
        )
    if queue == "venue_official_taxonomy_unavailable_source_gap":
        return (
            "restore_or_add_venue_official_taxonomy_parser",
            "restore_or_add_venue_official_taxonomy_parser",
            "new_or_restored_official_venue_industry_taxonomy_source",
            "Keep sector blank until a venue-official taxonomy parser or source exists.",
            "Add or restore a parser for an official venue industry or sector taxonomy source before any sector fill.",
        )
    return (
        "manual_venue_specific_sector_source_review",
        "seek_reviewed_venue_specific_taxonomy_source",
        "reviewed_venue_specific_taxonomy_source_with_exact_listing_match",
        "Keep sector blank until reviewed venue-specific evidence matches the exact listing.",
        "Perform manual venue-specific source review; no symbol, name, or peer inference is allowed.",
    )


def build_action_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    grouped: dict[tuple[str, str, str, str], list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        key = (
            row.get("exchange", ""),
            row.get("weak_sector_resolution_queue", ""),
            row.get("official_masterfile_sources", ""),
            row.get("official_masterfile_sector_values", ""),
        )
        grouped[key].append(row)

    action_rows: list[dict[str, str]] = []
    for (exchange, queue, official_sources, sector_values), group in grouped.items():
        action_queue, review_strategy, evidence_required, source_gate, recommended_next_action = action_for(queue)
        priorities = sorted({row.get("review_priority", "") for row in group if row.get("review_priority")})
        gap_classes = Counter(row.get("gap_class", "") for row in group)
        truth_outcomes = Counter(row.get("source_of_truth_outcome", "") or "accepted_source_gap" for row in group)
        examples = sorted({row.get("listing_key", "") for row in group if row.get("listing_key")})[:8]
        action_rows.append(
            {
                "exchange": exchange,
                "weak_sector_resolution_queue": queue,
                "official_masterfile_sources": official_sources or "none",
                "official_masterfile_sector_values": sector_values,
                "rows": str(len(group)),
                "review_priority": "|".join(priorities),
                "action_queue": action_queue,
                "review_strategy": review_strategy,
                "evidence_required": evidence_required,
                "source_gate": source_gate,
                "recommended_next_action": recommended_next_action,
                "gap_class_totals": join_counter(gap_classes),
                "source_of_truth_outcome_totals": join_counter(truth_outcomes),
                "example_listing_keys": "|".join(examples),
            }
        )
    return sorted(
        action_rows,
        key=lambda row: (
            row["review_priority"],
            row["action_queue"],
            row["exchange"],
            row["official_masterfile_sources"],
            row["official_masterfile_sector_values"],
        ),
    )


def summarize(action_rows: list[dict[str, str]], generated_at: str) -> dict[str, Any]:
    row_counts = Counter()
    exchange_counts = Counter()
    action_counts = Counter()
    queue_counts = Counter()
    evidence_counts = Counter()
    for row in action_rows:
        rows = int(row["rows"])
        row_counts["rows"] += rows
        exchange_counts[row["exchange"]] += rows
        action_counts[row["action_queue"]] += rows
        queue_counts[row["weak_sector_resolution_queue"]] += rows
        evidence_counts[row["evidence_required"]] += rows
    return {
        "generated_at": generated_at,
        "batches": len(action_rows),
        "rows": row_counts["rows"],
        "exchange_totals": dict(sorted(exchange_counts.items())),
        "action_queue_totals": dict(sorted(action_counts.items())),
        "weak_sector_resolution_queue_totals": dict(sorted(queue_counts.items())),
        "evidence_required_totals": dict(sorted(evidence_counts.items())),
        "direct_sector_apply_allowed_rows": 0,
        "metadata_enrichment_authorized": False,
        "policy": {
            "official_first": (
                "Every batch remains blocked until official venue, issuer, or reviewed venue-specific taxonomy "
                "evidence exists for the exact listing."
            ),
            "no_inference": "No sector is inferred from ticker, issuer name, ISIN prefix, peers, or broad exchange labels.",
            "reviewable_batches": (
                "Rows are grouped by exchange, residual queue, official source, and raw official sector value so "
                "parser work can be reviewed venue by venue."
            ),
        },
    }


def build_payload(weak_sector_review_csv: Path) -> dict[str, Any]:
    generated_at = utc_now_iso()
    weak_sector_rows = load_csv_rows(weak_sector_review_csv)
    action_rows = build_action_rows(weak_sector_rows)
    summary = summarize(action_rows, generated_at)
    return {
        "_meta": {
            "generated_at": generated_at,
            "weak_sector_review_csv": display_path(weak_sector_review_csv),
        },
        "summary": summary,
        "items": action_rows,
    }


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def markdown_escape(value: str) -> str:
    return value.replace("|", "\\|")


def render_markdown(payload: dict[str, Any]) -> str:
    meta = payload["_meta"]
    summary = payload["summary"]
    lines = [
        "# Weak-Sector Venue Action Queue",
        "",
        f"Generated: `{meta['generated_at']}`",
        f"Source: `{meta['weak_sector_review_csv']}`",
        "",
        "Policy: no sector values are applied from this report. It only groups blocked weak-sector gaps into venue-specific review and parser batches.",
        "",
        "## Summary",
        "",
        "| Metric | Value |",
        "| --- | ---: |",
        f"| Batches | {summary['batches']} |",
        f"| Underlying rows | {summary['rows']} |",
        f"| Direct sector apply allowed rows | {summary['direct_sector_apply_allowed_rows']} |",
        "",
        "## Action Queues",
        "",
        "| Action queue | Rows |",
        "| --- | ---: |",
    ]
    for action_queue, count in summary["action_queue_totals"].items():
        lines.append(f"| {action_queue} | {count} |")
    lines.extend(["", "## Exchange Backlog", "", "| Exchange | Rows |", "| --- | ---: |"])
    for exchange, count in summary["exchange_totals"].items():
        lines.append(f"| {exchange} | {count} |")
    lines.extend(
        [
            "",
            "## Top Batches",
            "",
            "| Exchange | Queue | Source | Raw official sector | Rows | Action | Evidence required |",
            "| --- | --- | --- | --- | ---: | --- | --- |",
        ]
    )
    for row in payload["items"][:25]:
        lines.append(
            "| "
            + " | ".join(
                [
                    markdown_escape(row["exchange"]),
                    markdown_escape(row["weak_sector_resolution_queue"]),
                    markdown_escape(row["official_masterfile_sources"]),
                    markdown_escape(row["official_masterfile_sector_values"] or "none"),
                    row["rows"],
                    markdown_escape(row["action_queue"]),
                    markdown_escape(row["evidence_required"]),
                ]
            )
            + " |"
        )
    lines.extend(
        [
            "",
            "## Gates",
            "",
            "- Direct sector apply allowed rows: `0`.",
            "- Broad official labels remain review candidates, not canonical sector values.",
            "- Scope candidates must be resolved before sector enrichment.",
            "- Rows without official taxonomy evidence stay blank.",
            "",
        ]
    )
    return "\n".join(lines)


def write_outputs(payload: dict[str, Any], csv_out: Path, json_out: Path, md_out: Path) -> None:
    write_csv(csv_out, payload["items"])
    json_out.parent.mkdir(parents=True, exist_ok=True)
    json_out.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_out.parent.mkdir(parents=True, exist_ok=True)
    md_out.write_text(render_markdown(payload), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build venue-specific action batches for weak-sector residual gaps.")
    parser.add_argument("--weak-sector-review-csv", type=Path, default=DEFAULT_WEAK_SECTOR_REVIEW_CSV)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_CSV_OUT)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_JSON_OUT)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_MD_OUT)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    payload = build_payload(args.weak_sector_review_csv)
    write_outputs(payload, args.csv_out, args.json_out, args.md_out)
    print(
        f"Wrote {len(payload['items'])} weak-sector venue action batches "
        f"for {payload['summary']['rows']} residual rows."
    )


if __name__ == "__main__":
    main()

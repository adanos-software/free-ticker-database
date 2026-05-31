from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DEEPSEEK_SUMMARY_JSON = ROOT / "data" / "reports" / "deepseek_review_summary.json"
DEFAULT_WEAK_SECTOR_REVIEW_CSV = ROOT / "data" / "reports" / "weak_sector_residual_review.csv"
DEFAULT_JSON_OUT = ROOT / "data" / "reports" / "deepseek_weak_sector_review_queue.json"
DEFAULT_CSV_OUT = ROOT / "data" / "reports" / "deepseek_weak_sector_review_queue.csv"
DEFAULT_MD_OUT = ROOT / "data" / "reports" / "deepseek_weak_sector_review_queue.md"


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def select_deepseek_weak_sector_reviews(payload: dict[str, Any]) -> list[dict[str, Any]]:
    items = payload.get("items", [])
    if not isinstance(items, list):
        return []
    selected: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        if item.get("review_kind") != "weak_sector":
            continue
        if item.get("decision_candidate") not in {"needs_official_evidence", "keep_source_gap", "out_of_scope_candidate"}:
            continue
        selected.append(item)
    return selected


def build_weak_sector_lookup(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    return {row["listing_key"]: row for row in rows if row.get("listing_key")}


def classify_review_queue(review: dict[str, Any], weak_row: dict[str, str]) -> str:
    if review.get("decision_candidate") == "out_of_scope_candidate":
        return "scope_review_before_sector_fill"
    if weak_row.get("weak_sector_resolution_queue") == "official_sector_candidate_normalization_review":
        return "official_sector_value_mapping_review"
    return "keep_source_gap_until_official_taxonomy"


def review_gate_for(queue: str) -> str:
    if queue == "official_sector_value_mapping_review":
        return (
            "Do not apply the raw official sector value as a canonical sector until a reviewer records a "
            "venue-specific mapping rule and exact listing-key evidence."
        )
    if queue == "scope_review_before_sector_fill":
        return "Do not fill sector until the listing scope is reviewed as core, extended, or excluded."
    return "Keep sector blank until an official venue taxonomy or issuer taxonomy source maps the exact listing."


def build_queue_rows(
    *,
    deepseek_reviews: list[dict[str, Any]],
    weak_sector_rows: list[dict[str, str]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    weak_lookup = build_weak_sector_lookup(weak_sector_rows)
    queue_rows: list[dict[str, Any]] = []
    unmatched: list[dict[str, Any]] = []
    for review in deepseek_reviews:
        listing_key = str(review.get("listing_key", ""))
        weak_row = weak_lookup.get(listing_key)
        if weak_row is None:
            unmatched.append({"listing_key": listing_key, "reason": "missing_weak_sector_residual_review_row"})
            continue
        review_queue = classify_review_queue(review, weak_row)
        queue_rows.append(
            {
                "listing_key": weak_row.get("listing_key", listing_key),
                "ticker": weak_row.get("ticker", review.get("ticker", "")),
                "exchange": weak_row.get("exchange", review.get("exchange", "")),
                "asset_type": weak_row.get("asset_type", ""),
                "name": weak_row.get("name", ""),
                "gap_class": weak_row.get("gap_class", ""),
                "source_of_truth_outcome": weak_row.get("source_of_truth_outcome", ""),
                "official_masterfile_sources": weak_row.get("official_masterfile_sources", ""),
                "official_masterfile_sector_values": weak_row.get("official_masterfile_sector_values", ""),
                "weak_sector_resolution_queue": weak_row.get("weak_sector_resolution_queue", ""),
                "residual_decision": weak_row.get("residual_decision", ""),
                "deepseek_decision_candidate": review.get("decision_candidate", ""),
                "deepseek_confidence": review.get("confidence", 0),
                "deepseek_evidence_needed": review.get("evidence_needed", ""),
                "deepseek_rationale": review.get("rationale", ""),
                "deepseek_do_not_apply_reason": review.get("do_not_apply_reason", ""),
                "review_queue": review_queue,
                "review_gate": review_gate_for(review_queue),
                "recommended_next_action": (
                    "Record a listing-keyed official taxonomy mapping decision, or leave the sector blank as a "
                    "reviewed source gap. DeepSeek output alone must not be applied."
                ),
            }
        )
    return queue_rows, unmatched


def summarize(queue_rows: list[dict[str, Any]], unmatched: list[dict[str, Any]]) -> dict[str, Any]:
    by_exchange = Counter(str(row.get("exchange", "")) for row in queue_rows)
    by_decision = Counter(str(row.get("deepseek_decision_candidate", "")) for row in queue_rows)
    by_queue = Counter(str(row.get("review_queue", "")) for row in queue_rows)
    by_official_value = Counter(str(row.get("official_masterfile_sector_values", "")) for row in queue_rows)
    return {
        "rows": len(queue_rows),
        "unmatched_deepseek_rows": len(unmatched),
        "exchange_totals": dict(sorted(by_exchange.items())),
        "deepseek_decision_totals": dict(sorted(by_decision.items())),
        "review_queue_totals": dict(sorted(by_queue.items())),
        "official_sector_value_totals": dict(sorted(by_official_value.items())),
    }


def build_payload(deepseek_summary_json: Path, weak_sector_review_csv: Path) -> dict[str, Any]:
    deepseek_payload = load_json(deepseek_summary_json)
    weak_sector_rows = load_csv_rows(weak_sector_review_csv)
    deepseek_reviews = select_deepseek_weak_sector_reviews(deepseek_payload)
    queue_rows, unmatched = build_queue_rows(
        deepseek_reviews=deepseek_reviews,
        weak_sector_rows=weak_sector_rows,
    )
    return {
        "_meta": {
            "generated_at": utc_now_iso(),
            "deepseek_summary_json": display_path(deepseek_summary_json),
            "weak_sector_review_csv": display_path(weak_sector_review_csv),
            "policy": (
                "DeepSeek weak-sector reviews are triage only. They do not authorize inferred sector, category, "
                "scope, identifier, name, or symbol changes."
            ),
        },
        "summary": summarize(queue_rows, unmatched),
        "items": queue_rows,
        "unmatched_deepseek_rows": unmatched,
    }


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "listing_key",
        "ticker",
        "exchange",
        "asset_type",
        "name",
        "gap_class",
        "source_of_truth_outcome",
        "official_masterfile_sources",
        "official_masterfile_sector_values",
        "weak_sector_resolution_queue",
        "residual_decision",
        "deepseek_decision_candidate",
        "deepseek_confidence",
        "deepseek_evidence_needed",
        "deepseek_rationale",
        "deepseek_do_not_apply_reason",
        "review_queue",
        "review_gate",
        "recommended_next_action",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def render_markdown(payload: dict[str, Any]) -> str:
    meta = payload["_meta"]
    summary = payload["summary"]
    lines = [
        "# DeepSeek Weak-Sector Review Queue",
        "",
        f"Generated: `{meta['generated_at']}`",
        "",
        "Policy: DeepSeek weak-sector reviews are triage only and do not authorize sector fills.",
        "",
        "## Summary",
        "",
        "| Metric | Value |",
        "| --- | ---: |",
        f"| Queue rows | {summary['rows']} |",
        f"| Unmatched DeepSeek rows | {summary['unmatched_deepseek_rows']} |",
        "",
        "## Review Queues",
        "",
        "| Queue | Rows |",
        "| --- | ---: |",
    ]
    for queue, count in summary["review_queue_totals"].items():
        lines.append(f"| {queue or 'missing'} | {count} |")
    lines.extend(["", "## Official Sector Values", "", "| Official raw value | Rows |", "| --- | ---: |"])
    for value, count in summary["official_sector_value_totals"].items():
        lines.append(f"| {value or 'missing'} | {count} |")
    lines.extend(
        [
            "",
            "## Review Gate",
            "",
            "Do not apply raw venue sector values as canonical sectors. Each fill requires a listing-keyed official "
            "taxonomy mapping decision; otherwise keep the sector blank as a reviewed source gap.",
        ]
    )
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Build a weak-sector mapping review queue from DeepSeek triage.")
    parser.add_argument("--deepseek-summary-json", type=Path, default=DEFAULT_DEEPSEEK_SUMMARY_JSON)
    parser.add_argument("--weak-sector-review-csv", type=Path, default=DEFAULT_WEAK_SECTOR_REVIEW_CSV)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_JSON_OUT)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_CSV_OUT)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_MD_OUT)
    args = parser.parse_args(argv)

    payload = build_payload(args.deepseek_summary_json, args.weak_sector_review_csv)
    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.json_out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    write_csv(args.csv_out, payload["items"])
    args.md_out.write_text(render_markdown(payload), encoding="utf-8")
    print(json.dumps({"summary": payload["summary"], "json_out": display_path(args.json_out)}, indent=2))


if __name__ == "__main__":
    main()

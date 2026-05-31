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
DEFAULT_OTC_SCOPE_REVIEW_CSV = ROOT / "data" / "reports" / "otc_scope_review.csv"
DEFAULT_JSON_OUT = ROOT / "data" / "reports" / "deepseek_otc_review_queue.json"
DEFAULT_CSV_OUT = ROOT / "data" / "reports" / "deepseek_otc_review_queue.csv"
DEFAULT_MD_OUT = ROOT / "data" / "reports" / "deepseek_otc_review_queue.md"


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


def select_deepseek_otc_reviews(payload: dict[str, Any]) -> list[dict[str, Any]]:
    items = payload.get("items", [])
    if not isinstance(items, list):
        return []
    selected: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        if item.get("review_kind") != "otc_scope":
            continue
        if item.get("decision_candidate") != "needs_official_evidence":
            continue
        selected.append(item)
    return selected


def build_otc_lookup(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    return {row["listing_key"]: row for row in rows if row.get("listing_key")}


def classify_review_queue(row: dict[str, str]) -> str:
    issue_types = row.get("issue_types", "")
    if "official_name_mismatch" in issue_types:
        return "official_name_mismatch_evidence_review"
    if row.get("source_gap_class"):
        return "otc_source_gap_evidence_review"
    return "otc_scope_evidence_review"


def review_gate_for(queue: str) -> str:
    if queue == "official_name_mismatch_evidence_review":
        return (
            "Do not change name, aliases, sector, or scope until a listing-keyed OTC Markets, SEC, issuer, "
            "or ISIN-anchored issuer-history source resolves the name mismatch."
        )
    if queue == "otc_source_gap_evidence_review":
        return "Keep missing metadata blank until official OTC, issuer, or registry evidence is attached."
    return "Do not alter OTC scope unless official listing evidence supports the decision."


def build_queue_rows(
    *,
    deepseek_reviews: list[dict[str, Any]],
    otc_rows: list[dict[str, str]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    otc_lookup = build_otc_lookup(otc_rows)
    queue_rows: list[dict[str, Any]] = []
    unmatched: list[dict[str, Any]] = []
    for review in deepseek_reviews:
        listing_key = str(review.get("listing_key", ""))
        otc_row = otc_lookup.get(listing_key)
        if otc_row is None:
            unmatched.append({"listing_key": listing_key, "reason": "missing_otc_scope_review_row"})
            continue
        review_queue = classify_review_queue(otc_row)
        queue_rows.append(
            {
                "listing_key": otc_row.get("listing_key", listing_key),
                "ticker": otc_row.get("ticker", review.get("ticker", "")),
                "exchange": otc_row.get("exchange", review.get("exchange", "")),
                "asset_type": otc_row.get("asset_type", ""),
                "name": otc_row.get("name", ""),
                "instrument_scope": otc_row.get("instrument_scope", ""),
                "scope_reason": otc_row.get("scope_reason", ""),
                "quality_status": otc_row.get("quality_status", ""),
                "issue_types": otc_row.get("issue_types", ""),
                "source_gap_field": otc_row.get("source_gap_field", ""),
                "source_gap_class": otc_row.get("source_gap_class", ""),
                "source_of_truth_outcome": otc_row.get("source_of_truth_outcome", ""),
                "scope_decision": otc_row.get("scope_decision", ""),
                "otc_review_decision_status": otc_row.get("otc_review_decision_status", ""),
                "metadata_enrichment_gate": otc_row.get("metadata_enrichment_gate", ""),
                "deepseek_decision_candidate": review.get("decision_candidate", ""),
                "deepseek_confidence": review.get("confidence", 0),
                "deepseek_evidence_needed": review.get("evidence_needed", ""),
                "deepseek_rationale": review.get("rationale", ""),
                "deepseek_do_not_apply_reason": review.get("do_not_apply_reason", ""),
                "review_queue": review_queue,
                "review_gate": review_gate_for(review_queue),
                "recommended_next_action": (
                    "Resolve with listing-keyed official OTC/SEC/issuer evidence or keep the field blank/source-gapped. "
                    "DeepSeek output alone must not be applied."
                ),
            }
        )
    return queue_rows, unmatched


def summarize(queue_rows: list[dict[str, Any]], unmatched: list[dict[str, Any]]) -> dict[str, Any]:
    by_queue = Counter(str(row.get("review_queue", "")) for row in queue_rows)
    by_issue = Counter(str(row.get("issue_types", "")) for row in queue_rows)
    by_scope = Counter(str(row.get("scope_decision", "")) for row in queue_rows)
    by_gap = Counter(str(row.get("source_gap_class", "")) for row in queue_rows)
    return {
        "rows": len(queue_rows),
        "unmatched_deepseek_rows": len(unmatched),
        "review_queue_totals": dict(sorted(by_queue.items())),
        "issue_type_totals": dict(sorted(by_issue.items())),
        "scope_decision_totals": dict(sorted(by_scope.items())),
        "source_gap_class_totals": dict(sorted(by_gap.items())),
    }


def build_payload(deepseek_summary_json: Path, otc_scope_review_csv: Path) -> dict[str, Any]:
    deepseek_payload = load_json(deepseek_summary_json)
    otc_rows = load_csv_rows(otc_scope_review_csv)
    deepseek_reviews = select_deepseek_otc_reviews(deepseek_payload)
    queue_rows, unmatched = build_queue_rows(deepseek_reviews=deepseek_reviews, otc_rows=otc_rows)
    return {
        "_meta": {
            "generated_at": utc_now_iso(),
            "deepseek_summary_json": display_path(deepseek_summary_json),
            "otc_scope_review_csv": display_path(otc_scope_review_csv),
            "policy": (
                "DeepSeek OTC reviews are triage only. They do not authorize inferred names, aliases, sectors, "
                "scope decisions, identifiers, or symbol changes."
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
        "instrument_scope",
        "scope_reason",
        "quality_status",
        "issue_types",
        "source_gap_field",
        "source_gap_class",
        "source_of_truth_outcome",
        "scope_decision",
        "otc_review_decision_status",
        "metadata_enrichment_gate",
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
        "# DeepSeek OTC Review Queue",
        "",
        f"Generated: `{meta['generated_at']}`",
        "",
        "Policy: DeepSeek OTC reviews are triage only and do not authorize names, sectors, aliases, or scope changes.",
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
    lines.extend(["", "## Issue Types", "", "| Issue type | Rows |", "| --- | ---: |"])
    for issue_type, count in summary["issue_type_totals"].items():
        lines.append(f"| {issue_type or 'missing'} | {count} |")
    lines.extend(
        [
            "",
            "## Review Gate",
            "",
            "Do not change OTC names, aliases, sectors, identifiers, or scope from DeepSeek output. Resolve each row "
            "with listing-keyed OTC Markets, SEC, issuer, or ISIN-anchored issuer-history evidence.",
        ]
    )
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Build an OTC evidence review queue from DeepSeek triage.")
    parser.add_argument("--deepseek-summary-json", type=Path, default=DEFAULT_DEEPSEEK_SUMMARY_JSON)
    parser.add_argument("--otc-scope-review-csv", type=Path, default=DEFAULT_OTC_SCOPE_REVIEW_CSV)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_JSON_OUT)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_CSV_OUT)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_MD_OUT)
    args = parser.parse_args(argv)

    payload = build_payload(args.deepseek_summary_json, args.otc_scope_review_csv)
    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.json_out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    write_csv(args.csv_out, payload["items"])
    args.md_out.write_text(render_markdown(payload), encoding="utf-8")
    print(json.dumps({"summary": payload["summary"], "json_out": display_path(args.json_out)}, indent=2))


if __name__ == "__main__":
    main()

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = ROOT / "data" / "reports"

DEFAULT_COVERAGE_JSON = REPORTS_DIR / "coverage_report.json"
DEFAULT_JSON_OUT = REPORTS_DIR / "source_refresh_queue.json"
DEFAULT_CSV_OUT = REPORTS_DIR / "source_refresh_queue.csv"
DEFAULT_MD_OUT = REPORTS_DIR / "source_refresh_queue.md"

FIELDNAMES = [
    "source_key",
    "provider",
    "reference_scope",
    "mode",
    "rows",
    "generated_at",
    "last_error",
    "age_hours",
    "freshness_status",
    "refresh_priority",
    "refresh_queue",
    "recommended_refresh_action",
    "recommended_next_source",
    "source_gate",
    "review_strategy",
    "evidence_required",
    "freshness_review_context",
    "refresh_gate_context",
]


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def priority_rank(priority: str) -> int:
    if priority.startswith("P") and priority[1:].isdigit():
        return int(priority[1:])
    return 99


def build_rows(coverage: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for source in coverage.get("source_coverage", []):
        refresh_queue = source.get("refresh_queue", "")
        if not refresh_queue or refresh_queue == "fresh_no_refresh_needed":
            continue
        rows.append(
            {
                "source_key": source.get("key", ""),
                "provider": source.get("provider", ""),
                "reference_scope": source.get("reference_scope", ""),
                "mode": source.get("mode", ""),
                "rows": source.get("rows", 0),
                "generated_at": source.get("generated_at", ""),
                "last_error": source.get("last_error", ""),
                "age_hours": source.get("age_hours"),
                "freshness_status": source.get("freshness_status", ""),
                "refresh_priority": source.get("refresh_priority", ""),
                "refresh_queue": refresh_queue,
                "recommended_refresh_action": source.get("recommended_refresh_action", ""),
                "recommended_next_source": source.get("recommended_next_source", ""),
                "source_gate": source.get("source_gate", ""),
                "review_strategy": source.get("review_strategy", ""),
                "evidence_required": source.get("evidence_required", ""),
                "freshness_review_context": source.get("freshness_review_context", ""),
                "refresh_gate_context": source.get("refresh_gate_context", ""),
            }
        )
    return sorted(
        rows,
        key=lambda row: (
            priority_rank(str(row.get("refresh_priority", ""))),
            str(row.get("refresh_queue", "")),
            -(float(row.get("age_hours") or 0)),
            str(row.get("provider", "")),
            str(row.get("source_key", "")),
        ),
    )


def count_by(rows: list[dict[str, Any]], key: str) -> dict[str, int]:
    return dict(sorted(Counter(str(row.get(key, "") or "missing") for row in rows).items()))


def build_top_refresh_batches(coverage: dict[str, Any], rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    queued_refreshes = {str(row.get("refresh_queue", "")) for row in rows}
    if not queued_refreshes:
        return []
    return [
        batch
        for batch in coverage.get("source_freshness_summary", {}).get("top_source_refresh_batches", [])
        if str(batch.get("refresh_queue", "")) in queued_refreshes
    ]


def build_payload(*, coverage: dict[str, Any], coverage_json: Path) -> dict[str, Any]:
    rows = build_rows(coverage)
    return {
        "_meta": {
            "generated_at": utc_now_iso(),
            "source_report": display_path(coverage_json),
            "policy": (
                "Source refresh queue only. Freshness and availability signals do not authorize inferred identifiers, "
                "sectors, categories, names, symbols, scope changes, or direct data application."
            ),
        },
        "summary": {
            "rows": len(rows),
            "priority_totals": count_by(rows, "refresh_priority"),
            "queue_totals": count_by(rows, "refresh_queue"),
            "mode_totals": count_by(rows, "mode"),
            "reference_scope_totals": count_by(rows, "reference_scope"),
            "freshness_status_totals": count_by(rows, "freshness_status"),
            "evidence_required_totals": count_by(rows, "evidence_required"),
            "top_source_refresh_batches": build_top_refresh_batches(coverage, rows),
        },
        "items": rows,
    }


def render_markdown(payload: dict[str, Any]) -> str:
    summary = payload["summary"]
    lines = [
        "# Source Refresh Queue",
        "",
        f"Generated: `{payload['_meta']['generated_at']}`",
        "",
        "Policy: freshness and availability signals are review gates only; they do not authorize direct data application.",
        "",
        "## Summary",
        "",
        f"- Rows: `{summary['rows']}`",
        f"- Priority totals: `{summary['priority_totals']}`",
        f"- Queue totals: `{summary['queue_totals']}`",
        f"- Mode totals: `{summary['mode_totals']}`",
        f"- Reference scope totals: `{summary['reference_scope_totals']}`",
        "",
        "## Top Refresh Batches",
        "",
        "| Queue | Scope | Mode | Priority | Sources | Rows | Max Age Hours | Evidence Required |",
        "| --- | --- | --- | --- | ---: | ---: | ---: | --- |",
    ]
    for batch in summary.get("top_source_refresh_batches", [])[:10]:
        lines.append(
            f"| {batch.get('refresh_queue', '')} | {batch.get('reference_scope', '')} | {batch.get('mode', '')} | "
            f"{batch.get('refresh_priority', '')} | {batch.get('source_count', 0)} | {batch.get('total_rows', 0)} | "
            f"{batch.get('max_age_hours', '')} | {batch.get('evidence_required', '')} |"
        )
    lines.extend(
        [
            "",
            "## Top Sources",
            "",
            "| Priority | Source | Provider | Scope | Mode | Rows | Age Hours | Queue | Last Error | Evidence Required |",
            "| --- | --- | --- | --- | --- | ---: | ---: | --- | --- | --- |",
        ]
    )
    for row in payload["items"][:25]:
        lines.append(
            f"| {row.get('refresh_priority', '')} | {row.get('source_key', '')} | {row.get('provider', '')} | "
            f"{row.get('reference_scope', '')} | {row.get('mode', '')} | {row.get('rows', 0)} | "
            f"{row.get('age_hours', '')} | {row.get('refresh_queue', '')} | {row.get('last_error', '')} | "
            f"{row.get('evidence_required', '')} |"
        )
    return "\n".join(lines) + "\n"


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Build a source refresh queue from coverage freshness signals.")
    parser.add_argument("--coverage-json", type=Path, default=DEFAULT_COVERAGE_JSON)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_JSON_OUT)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_CSV_OUT)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_MD_OUT)
    args = parser.parse_args(argv)

    coverage = json.loads(args.coverage_json.read_text(encoding="utf-8"))
    payload = build_payload(coverage=coverage, coverage_json=args.coverage_json)
    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.json_out.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    write_csv(args.csv_out, payload["items"])
    args.md_out.write_text(render_markdown(payload), encoding="utf-8")
    print(
        json.dumps(
            {
                "rows": payload["summary"]["rows"],
                "json_out": display_path(args.json_out),
                "csv_out": display_path(args.csv_out),
                "md_out": display_path(args.md_out),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()

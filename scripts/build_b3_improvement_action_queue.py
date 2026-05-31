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

DEFAULT_MASTERFILE_GAP_CSV = REPORTS_DIR / "b3_masterfile_gap_review.csv"
DEFAULT_MASTERFILE_GAP_JSON = REPORTS_DIR / "b3_masterfile_gap_review.json"
DEFAULT_RESIDUAL_ISIN_CSV = REPORTS_DIR / "b3_residual_isin_review.csv"
DEFAULT_RESIDUAL_SECTOR_CSV = REPORTS_DIR / "b3_residual_sector_review.csv"
DEFAULT_CSV_OUT = REPORTS_DIR / "b3_improvement_action_queue.csv"
DEFAULT_JSON_OUT = REPORTS_DIR / "b3_improvement_action_queue.json"
DEFAULT_MD_OUT = REPORTS_DIR / "b3_improvement_action_queue.md"

CSV_FIELDNAMES = [
    "campaign",
    "priority",
    "review_queue",
    "asset_type",
    "gap_class",
    "rows",
    "action_queue",
    "review_strategy",
    "evidence_required",
    "recommended_next_source",
    "source_gate",
    "example_listing_keys",
]


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def load_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def campaign_row(
    *,
    campaign: str,
    source_row: dict[str, str],
    review_queue: str,
    gap_class: str,
) -> dict[str, str]:
    return {
        "campaign": campaign,
        "priority": source_row.get("review_priority", ""),
        "review_queue": review_queue,
        "asset_type": source_row.get("asset_type", ""),
        "gap_class": gap_class,
        "listing_key": source_row.get("listing_key", ""),
        "review_strategy": source_row.get("review_strategy", ""),
        "evidence_required": source_row.get("verification_evidence_required", ""),
        "recommended_next_source": source_row.get("recommended_next_source", ""),
        "source_gate": source_row.get("source_gate", ""),
    }


def normalize_masterfile_review_queue(row: dict[str, str]) -> str:
    queue = row.get("b3_resolution_queue", "")
    if row.get("official_subset_closure_eligibility", "").startswith("closure_ready"):
        return f"{queue}_no_data_change_closure"
    return queue


def action_for(campaign: str, review_queue: str) -> str:
    if campaign == "b3_masterfile_gap":
        if review_queue.endswith("_no_data_change_closure"):
            return "document_official_subset_closure_without_data_change"
        if review_queue.startswith("official_subset_"):
            return "review_official_subset_scope_or_parser_gap"
        return "seek_current_b3_or_issuer_listing_evidence"
    if campaign == "b3_residual_isin":
        if review_queue == "scope_review_before_identifier_fill":
            return "decide_scope_before_identifier_enrichment"
        return "seek_official_identifier_source"
    if campaign == "b3_residual_sector":
        return "seek_stronger_official_b3_or_issuer_taxonomy"
    return "manual_b3_review"


def build_action_rows(
    *,
    masterfile_gap_rows: list[dict[str, str]],
    residual_isin_rows: list[dict[str, str]],
    residual_sector_rows: list[dict[str, str]],
) -> list[dict[str, str]]:
    source_rows: list[dict[str, str]] = []
    for row in masterfile_gap_rows:
        source_rows.append(
            campaign_row(
                campaign="b3_masterfile_gap",
                source_row=row,
                review_queue=normalize_masterfile_review_queue(row),
                gap_class=row.get("b3_gap_category", ""),
            )
        )
    for row in residual_isin_rows:
        source_rows.append(
            campaign_row(
                campaign="b3_residual_isin",
                source_row=row,
                review_queue=row.get("review_bucket", ""),
                gap_class=row.get("gap_class", ""),
            )
        )
    for row in residual_sector_rows:
        source_rows.append(
            campaign_row(
                campaign="b3_residual_sector",
                source_row=row,
                review_queue=row.get("review_bucket", ""),
                gap_class=row.get("gap_class", ""),
            )
        )

    grouped: dict[tuple[str, str, str, str, str], list[dict[str, str]]] = defaultdict(list)
    for row in source_rows:
        grouped[
            (
                row["campaign"],
                row["priority"],
                row["review_queue"],
                row["asset_type"],
                row["gap_class"],
            )
        ].append(row)

    action_rows: list[dict[str, str]] = []
    for (campaign, priority, review_queue, asset_type, gap_class), group in grouped.items():
        examples = sorted({row["listing_key"] for row in group if row["listing_key"]})[:10]
        review_strategies = sorted({row["review_strategy"] for row in group if row["review_strategy"]})
        evidence = sorted({row["evidence_required"] for row in group if row["evidence_required"]})
        next_sources = sorted({row["recommended_next_source"] for row in group if row["recommended_next_source"]})
        source_gates = sorted({row["source_gate"] for row in group if row["source_gate"]})
        action_rows.append(
            {
                "campaign": campaign,
                "priority": priority,
                "review_queue": review_queue,
                "asset_type": asset_type,
                "gap_class": gap_class,
                "rows": str(len(group)),
                "action_queue": action_for(campaign, review_queue),
                "review_strategy": " | ".join(review_strategies),
                "evidence_required": " | ".join(evidence),
                "recommended_next_source": " | ".join(next_sources),
                "source_gate": " | ".join(source_gates),
                "example_listing_keys": "|".join(examples),
            }
        )
    return sorted(
        action_rows,
        key=lambda row: (
            row["priority"],
            row["campaign"],
            row["action_queue"],
            row["review_queue"],
            row["asset_type"],
            row["gap_class"],
        ),
    )


def count_underlying_rows(action_rows: list[dict[str, str]], field: str) -> dict[str, int]:
    totals: Counter[str] = Counter()
    for row in action_rows:
        totals[row[field]] += int(row["rows"])
    return dict(sorted(totals.items()))


def summarize(
    *,
    action_rows: list[dict[str, str]],
    masterfile_gap_payload: dict[str, Any],
    generated_at: str,
) -> dict[str, Any]:
    coverage = masterfile_gap_payload.get("summary", {}).get("coverage_snapshot", {})
    coverage_diagnosis = masterfile_gap_payload.get("summary", {}).get("coverage_diagnosis", {})
    return {
        "generated_at": generated_at,
        "batches": len(action_rows),
        "underlying_review_rows": sum(int(row["rows"]) for row in action_rows),
        "campaign_totals": count_underlying_rows(action_rows, "campaign"),
        "priority_totals": count_underlying_rows(action_rows, "priority"),
        "action_queue_totals": count_underlying_rows(action_rows, "action_queue"),
        "review_queue_totals": count_underlying_rows(action_rows, "review_queue"),
        "b3_coverage_snapshot": coverage,
        "b3_coverage_diagnosis": coverage_diagnosis,
        "direct_data_change_authorized": False,
        "policy": {
            "official_first": "B3 changes require exact listing-keyed B3, CVM, issuer, CSD, fund registry, or prospectus evidence.",
            "no_guessing": "No ISIN, sector, category, name, symbol, or scope change is inferred from ticker shape or issuer name.",
            "campaign_delta": (
                "This queue records the current B3 residual backlog. Future B3 data campaigns should compare against "
                "these counts before and after apply-gated changes."
            ),
        },
    }


def build_payload(
    *,
    masterfile_gap_csv: Path,
    masterfile_gap_json: Path,
    residual_isin_csv: Path,
    residual_sector_csv: Path,
) -> dict[str, Any]:
    generated_at = utc_now_iso()
    action_rows = build_action_rows(
        masterfile_gap_rows=load_csv(masterfile_gap_csv),
        residual_isin_rows=load_csv(residual_isin_csv),
        residual_sector_rows=load_csv(residual_sector_csv),
    )
    masterfile_gap_payload = load_json(masterfile_gap_json)
    return {
        "_meta": {
            "generated_at": generated_at,
            "source_files": {
                "masterfile_gap_csv": display_path(masterfile_gap_csv),
                "masterfile_gap_json": display_path(masterfile_gap_json),
                "residual_isin_csv": display_path(residual_isin_csv),
                "residual_sector_csv": display_path(residual_sector_csv),
            },
        },
        "summary": summarize(
            action_rows=action_rows,
            masterfile_gap_payload=masterfile_gap_payload,
            generated_at=generated_at,
        ),
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
    coverage = summary["b3_coverage_snapshot"]
    diagnosis = summary["b3_coverage_diagnosis"]
    lines = [
        "# B3 Improvement Action Queue",
        "",
        f"Generated: `{meta['generated_at']}`",
        "",
        "Policy: this report does not apply data. It consolidates B3 masterfile, ISIN, and sector residuals into reviewable official-evidence batches.",
        "",
        "## Coverage Context",
        "",
        "| Metric | Value |",
        "| --- | ---: |",
        f"| Dataset rows | {coverage.get('dataset_rows', 0)} |",
        f"| Active directory match rate | {coverage.get('active_directory_match_rate', 0)} |",
        f"| Active directory missing rows | {coverage.get('active_directory_missing_dataset_rows', 0)} |",
        f"| Any official B3 source match rate | {coverage.get('official_any_source_match_rate', 0)} |",
        f"| Absent from all B3 source-gap rows | {coverage.get('absent_from_all_b3_source_gap_rows', 0)} |",
        "",
        f"Diagnosis: {diagnosis.get('root_cause', 'missing')}",
        "",
        "## Summary",
        "",
        "| Metric | Value |",
        "| --- | ---: |",
        f"| Action batches | {summary['batches']} |",
        f"| Underlying review rows | {summary['underlying_review_rows']} |",
        f"| Direct data changes authorized | {summary['direct_data_change_authorized']} |",
        "",
        "## Campaigns",
        "",
        "| Campaign | Rows |",
        "| --- | ---: |",
    ]
    for campaign, count in summary["campaign_totals"].items():
        lines.append(f"| {campaign} | {count} |")
    lines.extend(["", "## Action Queues", "", "| Action | Rows |", "| --- | ---: |"])
    for action, count in summary["action_queue_totals"].items():
        lines.append(f"| {action} | {count} |")
    lines.extend(
        [
            "",
            "## Batches",
            "",
            "| Priority | Campaign | Queue | Asset type | Gap class | Rows | Action | Evidence required |",
            "| --- | --- | --- | --- | --- | ---: | --- | --- |",
        ]
    )
    for row in payload["items"]:
        lines.append(
            "| "
            + " | ".join(
                [
                    markdown_escape(row["priority"]),
                    markdown_escape(row["campaign"]),
                    markdown_escape(row["review_queue"]),
                    markdown_escape(row["asset_type"]),
                    markdown_escape(row["gap_class"]),
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
            "- Direct data changes authorized: `False`.",
            "- ISIN, sector, category, name, symbol, and scope changes require exact official evidence.",
            "- Rows without current official evidence remain source gaps.",
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
    parser = argparse.ArgumentParser(description="Build a consolidated B3 improvement action queue.")
    parser.add_argument("--masterfile-gap-csv", type=Path, default=DEFAULT_MASTERFILE_GAP_CSV)
    parser.add_argument("--masterfile-gap-json", type=Path, default=DEFAULT_MASTERFILE_GAP_JSON)
    parser.add_argument("--residual-isin-csv", type=Path, default=DEFAULT_RESIDUAL_ISIN_CSV)
    parser.add_argument("--residual-sector-csv", type=Path, default=DEFAULT_RESIDUAL_SECTOR_CSV)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_CSV_OUT)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_JSON_OUT)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_MD_OUT)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    payload = build_payload(
        masterfile_gap_csv=args.masterfile_gap_csv,
        masterfile_gap_json=args.masterfile_gap_json,
        residual_isin_csv=args.residual_isin_csv,
        residual_sector_csv=args.residual_sector_csv,
    )
    write_outputs(payload, args.csv_out, args.json_out, args.md_out)
    print(
        f"Wrote {len(payload['items'])} B3 action batches "
        f"for {payload['summary']['underlying_review_rows']} review rows."
    )


if __name__ == "__main__":
    main()

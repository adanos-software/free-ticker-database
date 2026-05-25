from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.backfill_yahoo_generic_etf_names import merge_metadata_updates
from scripts.rebuild_dataset import normalize_sector


DATA_DIR = ROOT / "data"
REPORTS_DIR = DATA_DIR / "reports"

DEFAULT_B3_MASTERFILE_GAP_REVIEW_CSV = REPORTS_DIR / "b3_masterfile_gap_review.csv"
DEFAULT_METADATA_UPDATES_CSV = DATA_DIR / "review_overrides" / "metadata_updates.csv"
DEFAULT_CSV_OUT = REPORTS_DIR / "b3_etf_category_apply_report.csv"
DEFAULT_JSON_OUT = REPORTS_DIR / "b3_etf_category_apply_report.json"
DEFAULT_MD_OUT = REPORTS_DIR / "b3_etf_category_apply_report.md"

CSV_FIELDNAMES = [
    "listing_key",
    "ticker",
    "exchange",
    "name",
    "current_etf_category",
    "official_sector",
    "category_update",
    "candidate_sources",
    "candidate_source_urls",
    "decision",
    "reason",
    "verification_evidence_required",
    "official_source_context",
    "category_review_context",
    "apply_gate_context",
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


def single_candidate_category(value: str) -> str:
    candidates = {candidate.strip() for candidate in value.split("|") if candidate.strip()}
    if len(candidates) != 1:
        return ""
    candidate = next(iter(candidates))
    normalized = normalize_sector(candidate, "ETF")
    return normalized if normalized == candidate else ""


def official_source_context_for(row: dict[str, str]) -> str:
    return (
        f"candidate_sources={row.get('candidate_sources', '') or 'none'};"
        f"candidate_source_urls={row.get('candidate_source_urls', '') or 'none'}"
    )


def category_review_context_for(row: dict[str, str]) -> str:
    return (
        f"current_etf_category={row.get('current_etf_category', '') or 'none'};"
        f"official_sector={row.get('official_sector', '') or 'none'};"
        f"category_update={row.get('category_update', '') or 'none'}"
    )


def apply_gate_context_for(row: dict[str, str]) -> str:
    return (
        f"decision={row.get('decision', '') or 'none'};"
        f"reason={row.get('reason', '') or 'none'};"
        f"verification_evidence_required={row.get('verification_evidence_required', '') or 'none'}"
    )


def build_apply_rows(review_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for row in review_rows:
        if row.get("exchange") != "B3":
            continue
        category_update = single_candidate_category(row.get("candidate_sectors", ""))
        eligible = (
            row.get("asset_type") == "ETF"
            and row.get("source_presence") == "present_only_in_non_exchange_directory_source"
            and row.get("candidate_category_review_decision")
            == "official_candidate_category_differs_from_current_requires_review"
            and row.get("candidate_sources") == "b3_listed_etfs"
            and bool(category_update)
        )
        decision = "apply" if eligible else "skip"
        reason = (
            "official_b3_listed_etf_category_differs_from_current"
            if eligible
            else "not_eligible_for_b3_official_etf_category_apply"
        )
        report_row = {
            "listing_key": row.get("listing_key", ""),
            "ticker": row.get("ticker", ""),
            "exchange": row.get("exchange", ""),
            "name": row.get("name", ""),
            "current_etf_category": row.get("current_etf_category", ""),
            "official_sector": row.get("candidate_sectors", ""),
            "category_update": category_update,
            "candidate_sources": row.get("candidate_sources", ""),
            "candidate_source_urls": row.get("candidate_source_urls", ""),
            "decision": decision,
            "reason": reason,
            "verification_evidence_required": (
                "official_b3_listed_etfs_row_with_exact_listing_key_and_single_canonical_etf_category"
            ),
        }
        report_row["official_source_context"] = official_source_context_for(report_row)
        report_row["category_review_context"] = category_review_context_for(report_row)
        report_row["apply_gate_context"] = apply_gate_context_for(report_row)
        rows.append(report_row)
    return sorted(rows, key=lambda row: (row["decision"], row["ticker"], row["listing_key"]))


def build_metadata_updates(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    updates: list[dict[str, str]] = []
    for row in rows:
        if row.get("decision") != "apply" or not row.get("category_update"):
            continue
        updates.append(
            {
                "ticker": row["ticker"],
                "exchange": row["exchange"],
                "field": "etf_category",
                "decision": "update",
                "proposed_value": row["category_update"],
                "confidence": "0.97",
                "reason": (
                    "Official B3 listed ETF source provided a single canonical ETF category for the exact "
                    "B3 listing; applied only after listing_key, ETF asset type, official source, and "
                    "current-vs-official mismatch gates. "
                    f"Source: {row['candidate_source_urls']}"
                ),
            }
        )
    return updates


def summarize(rows: list[dict[str, str]], generated_at: str, applied: bool, written_updates: int) -> dict[str, Any]:
    return {
        "generated_at": generated_at,
        "applied": applied,
        "rows": len(rows),
        "written_updates": written_updates,
        "decision_totals": dict(sorted(Counter(row["decision"] for row in rows).items())),
        "official_sector_totals": dict(sorted(Counter(row["official_sector"] for row in rows if row["official_sector"]).items())),
        "category_update_totals": dict(
            sorted(
                Counter(
                    row["category_update"]
                    for row in rows
                    if row.get("decision") == "apply" and row["category_update"]
                ).items()
            )
        ),
        "policy": {
            "source": "Only b3_masterfile_gap_review rows backed by b3_listed_etfs official source evidence are eligible.",
            "no_guessing": "No ETF category is inferred from name or ticker shape; rows require a single canonical official B3 candidate category.",
            "traceability": "Every apply row is listing-keyed and writes a review_overrides metadata update only for etf_category.",
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
        "# B3 ETF Category Apply Report",
        "",
        f"Generated at: `{summary['generated_at']}`",
        "",
        "This report applies only exact official B3 listed-ETF category mismatches. It does not infer categories from names or ticker shape.",
        "",
        "## Summary",
        "",
        f"- Rows: `{summary['rows']}`",
        f"- Applied: `{summary['applied']}`",
        f"- Written updates: `{summary['written_updates']}`",
        "",
        "## Decisions",
        "",
        "| Decision | Rows |",
        "|---|---:|",
    ]
    for decision, count in summary["decision_totals"].items():
        lines.append(f"| {decision} | {count} |")
    lines.extend(["", "## Category Updates", "", "| ETF category | Rows |", "|---|---:|"])
    for category, count in summary["category_update_totals"].items():
        lines.append(f"| {category} | {count} |")
    lines.extend(
        [
            "",
            "## Policy",
            "",
            "- Only official B3 listed-ETF rows with one canonical candidate category are eligible.",
            "- The report writes only `etf_category` metadata overrides; it does not change ISINs, names, symbols, or stock sectors.",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Apply safe B3 ETF category corrections from official B3 review evidence.")
    parser.add_argument("--b3-masterfile-gap-review-csv", type=Path, default=DEFAULT_B3_MASTERFILE_GAP_REVIEW_CSV)
    parser.add_argument("--metadata-updates-csv", type=Path, default=DEFAULT_METADATA_UPDATES_CSV)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_CSV_OUT)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_JSON_OUT)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_MD_OUT)
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    rows = build_apply_rows(load_csv(args.b3_masterfile_gap_review_csv))
    updates = build_metadata_updates(rows)
    if args.apply and updates:
        merge_metadata_updates(args.metadata_updates_csv, updates)
    summary = summarize(rows, utc_now_iso(), args.apply, len(updates) if args.apply else 0)
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
                "written_updates": summary["written_updates"],
                "decision_totals": summary["decision_totals"],
                "csv_out": display_path(args.csv_out),
                "json_out": display_path(args.json_out),
                "md_out": display_path(args.md_out),
                "applied": args.apply,
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()

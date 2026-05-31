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

DEFAULT_NAME_MISMATCH_CSV = REPORTS_DIR / "otc_name_mismatch_review.csv"
DEFAULT_DEEPSEEK_OTC_CSV = REPORTS_DIR / "deepseek_otc_review_queue.csv"
DEFAULT_CSV_OUT = REPORTS_DIR / "otc_name_mismatch_action_queue.csv"
DEFAULT_JSON_OUT = REPORTS_DIR / "otc_name_mismatch_action_queue.json"
DEFAULT_MD_OUT = REPORTS_DIR / "otc_name_mismatch_action_queue.md"

CSV_FIELDNAMES = [
    "review_class",
    "review_priority",
    "official_sources",
    "isin_presence",
    "deepseek_triage",
    "rows",
    "action_queue",
    "apply_eligibility",
    "verification_evidence_required",
    "review_strategy",
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
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def deepseek_lookup(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    return {row["listing_key"]: row for row in rows if row.get("listing_key")}


def isin_presence(row: dict[str, str]) -> str:
    return "with_isin" if row.get("isin") else "without_isin"


def deepseek_triage_for(row: dict[str, str], deepseek_rows: dict[str, dict[str, str]]) -> str:
    review = deepseek_rows.get(row.get("listing_key", ""))
    if review is None:
        return "not_triaged_by_deepseek"
    return f"deepseek_{review.get('deepseek_decision_candidate') or 'manual_review'}"


def action_for(review_class: str) -> str:
    if review_class == "stale_or_symbol_reuse_without_isin":
        return "resolve_or_quarantine_before_trusting_otc_symbol"
    if review_class == "probable_otc_rename_or_symbol_reuse":
        return "verify_isin_anchored_issuer_history_before_name_change"
    if review_class == "weak_abbreviation_or_truncation_review":
        return "review_official_alias_before_matcher_tuning"
    if review_class == "matcher_false_positive":
        return "tighten_matcher_without_dataset_metadata_change"
    if review_class == "hold_unresolved":
        return "keep_current_until_stronger_issuer_history_source"
    return "manual_otc_name_mismatch_review"


def join_unique(rows: list[dict[str, str]], field: str) -> str:
    return " | ".join(sorted({row.get(field, "") for row in rows if row.get(field, "")}))


def build_action_rows(
    *,
    name_mismatch_rows: list[dict[str, str]],
    deepseek_rows: list[dict[str, str]],
) -> list[dict[str, str]]:
    deepseek_by_key = deepseek_lookup(deepseek_rows)
    grouped: dict[tuple[str, str, str, str, str], list[dict[str, str]]] = defaultdict(list)
    for row in name_mismatch_rows:
        key = (
            row.get("review_class", ""),
            row.get("review_priority", ""),
            row.get("official_sources", "") or "none",
            isin_presence(row),
            deepseek_triage_for(row, deepseek_by_key),
        )
        grouped[key].append(row)

    action_rows: list[dict[str, str]] = []
    for (review_class, priority, official_sources, isin_state, deepseek_triage), group in grouped.items():
        examples = sorted({row.get("listing_key", "") for row in group if row.get("listing_key")})[:10]
        action_rows.append(
            {
                "review_class": review_class,
                "review_priority": priority,
                "official_sources": official_sources,
                "isin_presence": isin_state,
                "deepseek_triage": deepseek_triage,
                "rows": str(len(group)),
                "action_queue": action_for(review_class),
                "apply_eligibility": join_unique(group, "apply_eligibility"),
                "verification_evidence_required": join_unique(group, "verification_evidence_required"),
                "review_strategy": join_unique(group, "review_strategy"),
                "recommended_next_source": join_unique(group, "recommended_next_source"),
                "source_gate": join_unique(group, "source_gate"),
                "example_listing_keys": "|".join(examples),
            }
        )
    return sorted(
        action_rows,
        key=lambda row: (
            priority_rank(row["review_priority"]),
            row["review_class"],
            row["official_sources"],
            row["isin_presence"],
            row["deepseek_triage"],
        ),
    )


def priority_rank(priority: str) -> int:
    return {
        "critical": 0,
        "high": 1,
        "medium": 2,
        "low": 3,
        "held": 4,
    }.get(priority, 9)


def count_rows(action_rows: list[dict[str, str]], field: str) -> dict[str, int]:
    totals: Counter[str] = Counter()
    for row in action_rows:
        totals[row[field]] += int(row["rows"])
    return dict(sorted(totals.items()))


def summarize(action_rows: list[dict[str, str]], generated_at: str) -> dict[str, Any]:
    return {
        "generated_at": generated_at,
        "batches": len(action_rows),
        "rows": sum(int(row["rows"]) for row in action_rows),
        "review_class_totals": count_rows(action_rows, "review_class"),
        "review_priority_totals": count_rows(action_rows, "review_priority"),
        "action_queue_totals": count_rows(action_rows, "action_queue"),
        "deepseek_triage_totals": count_rows(action_rows, "deepseek_triage"),
        "official_source_totals": count_rows(action_rows, "official_sources"),
        "direct_name_changes_authorized": False,
        "metadata_enrichment_authorized": False,
        "policy": {
            "deepseek_triage_only": "DeepSeek output is only a triage signal and never authorizes OTC names, aliases, sectors, identifiers, or scope changes.",
            "official_identity_first": "Name changes require listing-keyed OTC Markets, SEC, issuer, registry, or ISIN-anchored issuer-history evidence.",
            "no_symbol_only_resolution": "No OTC symbol is trusted or renamed from ticker-only, name-shape, or broad issuer-family evidence.",
        },
    }


def build_payload(name_mismatch_csv: Path, deepseek_otc_csv: Path) -> dict[str, Any]:
    generated_at = utc_now_iso()
    action_rows = build_action_rows(
        name_mismatch_rows=load_csv(name_mismatch_csv),
        deepseek_rows=load_csv(deepseek_otc_csv),
    )
    return {
        "_meta": {
            "generated_at": generated_at,
            "source_files": {
                "name_mismatch_csv": display_path(name_mismatch_csv),
                "deepseek_otc_csv": display_path(deepseek_otc_csv),
            },
        },
        "summary": summarize(action_rows, generated_at),
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
        "# OTC Name Mismatch Action Queue",
        "",
        f"Generated: `{meta['generated_at']}`",
        "",
        "Policy: this report does not change names or metadata. It groups OTC name mismatches into official-evidence review batches.",
        "",
        "## Summary",
        "",
        "| Metric | Value |",
        "| --- | ---: |",
        f"| Batches | {summary['batches']} |",
        f"| Underlying rows | {summary['rows']} |",
        f"| Direct name changes authorized | {summary['direct_name_changes_authorized']} |",
        f"| Metadata enrichment authorized | {summary['metadata_enrichment_authorized']} |",
        "",
        "## Review Classes",
        "",
        "| Review class | Rows |",
        "| --- | ---: |",
    ]
    for review_class, count in summary["review_class_totals"].items():
        lines.append(f"| {review_class} | {count} |")
    lines.extend(["", "## DeepSeek Triage", "", "| Triage | Rows |", "| --- | ---: |"])
    for triage, count in summary["deepseek_triage_totals"].items():
        lines.append(f"| {triage} | {count} |")
    lines.extend(
        [
            "",
            "## Batches",
            "",
            "| Priority | Review class | Source | ISIN | DeepSeek | Rows | Action | Evidence required |",
            "| --- | --- | --- | --- | --- | ---: | --- | --- |",
        ]
    )
    for row in payload["items"]:
        lines.append(
            "| "
            + " | ".join(
                [
                    markdown_escape(row["review_priority"]),
                    markdown_escape(row["review_class"]),
                    markdown_escape(row["official_sources"]),
                    markdown_escape(row["isin_presence"]),
                    markdown_escape(row["deepseek_triage"]),
                    row["rows"],
                    markdown_escape(row["action_queue"]),
                    markdown_escape(row["verification_evidence_required"]),
                ]
            )
            + " |"
        )
    lines.extend(
        [
            "",
            "## Gates",
            "",
            "- Direct name changes authorized: `False`.",
            "- DeepSeek triage does not authorize any data change.",
            "- Symbol reuse and stale OTC names require official listing-keyed identity evidence or quarantine review.",
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
    parser = argparse.ArgumentParser(description="Build OTC name-mismatch action batches.")
    parser.add_argument("--name-mismatch-csv", type=Path, default=DEFAULT_NAME_MISMATCH_CSV)
    parser.add_argument("--deepseek-otc-csv", type=Path, default=DEFAULT_DEEPSEEK_OTC_CSV)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_CSV_OUT)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_JSON_OUT)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_MD_OUT)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    payload = build_payload(args.name_mismatch_csv, args.deepseek_otc_csv)
    write_outputs(payload, args.csv_out, args.json_out, args.md_out)
    print(
        f"Wrote {len(payload['items'])} OTC name-mismatch action batches "
        f"for {payload['summary']['rows']} review rows."
    )


if __name__ == "__main__":
    main()

from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter
from dataclasses import dataclass, asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.build_completion_backlog import (
    FIELD_MISSING_ETF_CATEGORY,
    FIELD_MISSING_ISIN,
    FIELD_MISSING_STOCK_SECTOR,
)

DATA_DIR = ROOT / "data"
REPORTS_DIR = DATA_DIR / "reports"
DEFAULT_SOURCE_GAP_CLASSIFICATION_CSV = REPORTS_DIR / "source_gap_classification.csv"
DEFAULT_CSV_OUT = REPORTS_DIR / "source_of_truth_decisions.csv"
DEFAULT_JSON_OUT = REPORTS_DIR / "source_of_truth_decisions.json"
DEFAULT_MD_OUT = REPORTS_DIR / "source_of_truth_decisions.md"

OFFICIAL_FILL_REQUIRED_CLASSES = {
    "official_identifier_source_gap",
    "exchange_industry_source_gap",
    "generic_etf_category_source_gap",
}
CORE_EXCLUSION_CANDIDATE_CLASSES = {
    "adr_cdr_or_depositary_identifier_gap",
    "adr_cdr_or_depositary_sector_gap",
    "capital_pool_or_halted_identifier_gap",
    "debt_or_securitized_identifier_gap",
    "fund_or_trust_identifier_gap",
    "fundlike_stock_sector_gap",
    "inactive_or_legacy_identifier_gap",
    "shell_or_cpc_sector_gap",
}
ACCEPTED_SOURCE_GAP_CLASSES = {
    "commodity_etf_category_gap",
    "digital_asset_etf_category_gap",
    "equity_etf_category_gap",
    "fixed_income_etf_category_gap",
    "official_identifier_not_exposed_source_gap",
    "official_industry_taxonomy_unavailable_gap",
    "otc_sector_source_gap",
}

OUTCOME_POLICIES = {
    "official_fill_required": "Keep the row in scope and fill only from an official or reviewed source that satisfies the source gate.",
    "accepted_source_gap": "Keep the row as an explicit, review-gated source gap until the named source becomes available.",
    "core_exclusion_candidate": "Do not remove automatically; create a reviewed drop/scope decision only after official status or instrument-kind evidence confirms it is outside core stock/ETF scope.",
}

OUTCOME_NEXT_ACTIONS = {
    "official_fill_required": "Prioritize source acquisition or parser work for this class before any manual override.",
    "accepted_source_gap": "Leave metadata blank, preserve the classification, and revisit only when a stronger source is found.",
    "core_exclusion_candidate": "Review official listing status/instrument type; if confirmed non-core, add a reviewed drop entry or future scope override.",
}

CSV_FIELDNAMES = [
    "field",
    "target_field",
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
    "decision_policy",
    "next_action",
    "source_gate",
]


@dataclass(frozen=True)
class SourceOfTruthDecisionRow:
    field: str
    target_field: str
    listing_key: str
    ticker: str
    exchange: str
    asset_type: str
    name: str
    gap_class: str
    source_of_truth_outcome: str
    core_action: str
    fill_action: str
    review_needed: bool
    decision_policy: str
    next_action: str
    source_gate: str


def utc_now() -> str:
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


def outcome_for_gap_class(gap_class: str) -> str:
    if gap_class in OFFICIAL_FILL_REQUIRED_CLASSES:
        return "official_fill_required"
    if gap_class in ACCEPTED_SOURCE_GAP_CLASSES:
        return "accepted_source_gap"
    if gap_class in CORE_EXCLUSION_CANDIDATE_CLASSES:
        return "core_exclusion_candidate"
    return "unresolved"


def core_action_for(outcome: str) -> str:
    if outcome == "core_exclusion_candidate":
        return "review_for_core_exclusion"
    return "keep_in_current_scope"


def fill_action_for(outcome: str) -> str:
    if outcome == "official_fill_required":
        return "fill_from_source"
    if outcome == "accepted_source_gap":
        return "leave_blank_until_source_available"
    if outcome == "core_exclusion_candidate":
        return "do_not_fill_until_scope_review"
    return "manual_review_required"


def build_decisions(classification_rows: list[dict[str, str]]) -> list[SourceOfTruthDecisionRow]:
    decisions: list[SourceOfTruthDecisionRow] = []
    for row in classification_rows:
        outcome = outcome_for_gap_class(row.get("gap_class", ""))
        decisions.append(
            SourceOfTruthDecisionRow(
                field=row.get("field", ""),
                target_field=row.get("target_field", ""),
                listing_key=row.get("listing_key", ""),
                ticker=row.get("ticker", ""),
                exchange=row.get("exchange", ""),
                asset_type=row.get("asset_type", ""),
                name=row.get("name", ""),
                gap_class=row.get("gap_class", ""),
                source_of_truth_outcome=outcome,
                core_action=core_action_for(outcome),
                fill_action=fill_action_for(outcome),
                review_needed=True,
                decision_policy=OUTCOME_POLICIES.get(outcome, "Manual review is required before release."),
                next_action=OUTCOME_NEXT_ACTIONS.get(outcome, "Resolve this row before release."),
                source_gate=row.get("source_gate", ""),
            )
        )
    return sorted(decisions, key=lambda row: (row.source_of_truth_outcome, row.field, row.exchange, row.ticker, row.listing_key))


def rows_to_dicts(rows: list[SourceOfTruthDecisionRow]) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for row in rows:
        item = asdict(row)
        item["review_needed"] = "true" if row.review_needed else "false"
        payload.append(item)
    return payload


def summarize(rows: list[SourceOfTruthDecisionRow], generated_at: str) -> dict[str, Any]:
    outcome_counts = Counter(row.source_of_truth_outcome for row in rows)
    class_counts = Counter(row.gap_class for row in rows)
    field_outcomes = Counter((row.field, row.source_of_truth_outcome) for row in rows)
    return {
        "generated_at": generated_at,
        "rows": len(rows),
        "outcome_totals": dict(sorted(outcome_counts.items())),
        "class_totals": dict(sorted(class_counts.items())),
        "field_outcomes": {
            f"{field}:{outcome}": count for (field, outcome), count in sorted(field_outcomes.items())
        },
        "policy": {
            "source_of_truth_program": "Every residual gap must have exactly one outcome: official_fill_required, accepted_source_gap, or core_exclusion_candidate.",
            "no_automatic_core_exclusion": "Core exclusion candidates are not dropped by this report; they require reviewed evidence and the normal drop/scope path.",
        },
    }


def write_csv(path: Path, rows: list[SourceOfTruthDecisionRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDNAMES, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows_to_dicts(rows))


def write_json(path: Path, rows: list[SourceOfTruthDecisionRow], summary: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"summary": summary, "rows": rows_to_dicts(rows)}, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def format_counts(counts: dict[str, int]) -> str:
    lines = ["| Value | Rows |", "|---|---:|"]
    for key, count in sorted(counts.items(), key=lambda item: (-item[1], item[0])):
        lines.append(f"| {key} | {count} |")
    return "\n".join(lines)


def write_markdown(path: Path, summary: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Source-of-Truth Decisions",
        "",
        f"Generated at: `{summary['generated_at']}`",
        "",
        "This report converts residual source-gap classes into release-trackable outcomes. It does not fill fields and does not drop rows automatically.",
        "",
        "## Outcomes",
        "",
        format_counts(summary["outcome_totals"]),
        "",
        "## Top Classes",
        "",
        format_counts(dict(sorted(summary["class_totals"].items(), key=lambda item: (-item[1], item[0]))[:20])),
        "",
        "## Policy",
        "",
        "- `official_fill_required`: get a source/parser or reviewed override before filling.",
        "- `accepted_source_gap`: keep the blank value as a documented source gap.",
        "- `core_exclusion_candidate`: review official evidence before adding drop/scope overrides.",
        "- Validator gates fail unresolved, stale, duplicate, or non-review-gated decision rows.",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build source-of-truth decisions for residual ticker DB gaps.")
    parser.add_argument("--source-gap-classification-csv", type=Path, default=DEFAULT_SOURCE_GAP_CLASSIFICATION_CSV)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_CSV_OUT)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_JSON_OUT)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_MD_OUT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    rows = build_decisions(load_csv(args.source_gap_classification_csv))
    generated_at = utc_now()
    summary = summarize(rows, generated_at)
    write_csv(args.csv_out, rows)
    write_json(args.json_out, rows, summary)
    write_markdown(args.md_out, summary)
    print(
        json.dumps(
            {
                "rows": len(rows),
                "outcome_totals": summary["outcome_totals"],
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

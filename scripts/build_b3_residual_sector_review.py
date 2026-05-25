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
DEFAULT_SOURCE_GAP_CLASSIFICATION_CSV = REPORTS_DIR / "source_gap_classification.csv"
DEFAULT_SOURCE_OF_TRUTH_DECISIONS_CSV = REPORTS_DIR / "source_of_truth_decisions.csv"
DEFAULT_B3_SECTOR_PROBE_CSV = DATA_DIR / "b3_verification" / "sector_classification_backfill.csv"
DEFAULT_CSV_OUT = REPORTS_DIR / "b3_residual_sector_review.csv"
DEFAULT_JSON_OUT = REPORTS_DIR / "b3_residual_sector_review.json"
DEFAULT_MD_OUT = REPORTS_DIR / "b3_residual_sector_review.md"

CSV_FIELDNAMES = [
    "listing_key",
    "ticker",
    "exchange",
    "asset_type",
    "name",
    "gap_class",
    "source_of_truth_outcome",
    "source_gap_context",
    "review_needed",
    "recommended_next_source",
    "source_gate",
    "b3_probe_decision",
    "b3_code",
    "b3_name",
    "b3_sector_pt",
    "b3_subsector",
    "b3_segment",
    "b3_sector_update",
    "b3_source_url",
    "official_source_context",
    "residual_decision",
    "review_bucket",
    "review_priority",
    "review_strategy",
    "apply_eligibility",
    "verification_evidence_required",
    "residual_review_context",
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


def build_listing_lookup(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    return {listing_key(row): row for row in rows if listing_key(row) != "::"}


def residual_decision_for(probe: dict[str, str], source_of_truth: dict[str, str]) -> str:
    if probe.get("decision") == "accept" and probe.get("sector_update", "").strip():
        return "official_b3_sector_available_rebuild_required"
    if source_of_truth.get("source_of_truth_outcome") == "core_exclusion_candidate":
        return "core_exclusion_candidate_requires_scope_review"
    if probe.get("decision") == "no_b3_code_match":
        return "accepted_source_gap_no_b3_classification_code_match"
    if probe.get("decision") == "ambiguous_b3_code_match":
        return "accepted_source_gap_ambiguous_b3_classification_code"
    if not probe:
        return "accepted_source_gap_missing_b3_sector_probe_row"
    return "accepted_source_gap_requires_stronger_official_taxonomy"


def review_bucket_for(residual_decision: str) -> str:
    if residual_decision == "official_b3_sector_available_rebuild_required":
        return "official_sector_candidate_requires_apply_gate"
    if residual_decision == "core_exclusion_candidate_requires_scope_review":
        return "scope_review_before_sector_fill"
    if residual_decision == "accepted_source_gap_ambiguous_b3_classification_code":
        return "ambiguous_b3_code_requires_manual_review"
    if residual_decision == "accepted_source_gap_missing_b3_sector_probe_row":
        return "missing_probe_row_requires_parser_or_source_review"
    if residual_decision == "accepted_source_gap_no_b3_classification_code_match":
        return "no_b3_classification_code_match_source_gap"
    return "requires_stronger_official_taxonomy_source_gap"


def review_priority_for(review_bucket: str) -> str:
    if review_bucket in {
        "official_sector_candidate_requires_apply_gate",
        "scope_review_before_sector_fill",
        "ambiguous_b3_code_requires_manual_review",
    }:
        return "P1"
    if review_bucket == "missing_probe_row_requires_parser_or_source_review":
        return "P2"
    if review_bucket == "no_b3_classification_code_match_source_gap":
        return "P3"
    return "P4"


def apply_eligibility_for(review_bucket: str) -> str:
    if review_bucket == "official_sector_candidate_requires_apply_gate":
        return "apply_only_after_listing_key_taxonomy_and_canonical_sector_validation"
    if review_bucket == "scope_review_before_sector_fill":
        return "blocked_until_core_or_extended_scope_decision"
    if review_bucket in {
        "ambiguous_b3_code_requires_manual_review",
        "missing_probe_row_requires_parser_or_source_review",
        "no_b3_classification_code_match_source_gap",
        "requires_stronger_official_taxonomy_source_gap",
    }:
        return "source_gap_keep_blank_until_official_taxonomy_evidence"
    return "manual_review_required"


def verification_evidence_for(review_bucket: str) -> str:
    if review_bucket == "official_sector_candidate_requires_apply_gate":
        return "official_b3_taxonomy_with_exact_listing_key_b3_code_and_canonical_sector_mapping"
    if review_bucket == "scope_review_before_sector_fill":
        return "scope_decision_for_core_extended_or_exclude_before_sector_fill"
    if review_bucket == "ambiguous_b3_code_requires_manual_review":
        return "manual_b3_code_disambiguation_against_exact_listing_name_before_sector_mapping"
    if review_bucket == "missing_probe_row_requires_parser_or_source_review":
        return "parser_or_source_refresh_to_produce_b3_classification_probe_row"
    if review_bucket == "no_b3_classification_code_match_source_gap":
        return "stronger_official_b3_or_issuer_taxonomy_source_with_exact_listing_match"
    if review_bucket == "requires_stronger_official_taxonomy_source_gap":
        return "stronger_official_taxonomy_source_before_canonical_sector_mapping"
    return "manual_review_required"


def recommended_next_source_for(review_bucket: str) -> str:
    if review_bucket == "official_sector_candidate_requires_apply_gate":
        return "Official B3 taxonomy/probe row with exact listing, B3 code, and canonical sector mapping."
    if review_bucket == "scope_review_before_sector_fill":
        return "Current B3 source plus reviewed core, extended, or exclude scope decision before sector work."
    if review_bucket == "ambiguous_b3_code_requires_manual_review":
        return "Official B3 taxonomy row plus manual exact-name disambiguation for the listing key."
    if review_bucket == "missing_probe_row_requires_parser_or_source_review":
        return "Refreshed official B3 taxonomy source or parser output that produces a listing-keyed probe row."
    if review_bucket == "no_b3_classification_code_match_source_gap":
        return "Stronger official B3 or issuer taxonomy source exposing sector for the exact listing."
    if review_bucket == "requires_stronger_official_taxonomy_source_gap":
        return "Stronger official taxonomy source before any canonical sector mapping."
    return "Manual B3 sector review source with exact listing-key evidence."


def source_gate_for(review_bucket: str) -> str:
    if review_bucket == "official_sector_candidate_requires_apply_gate":
        return "Apply only after listing-key, taxonomy row, and canonical sector mapping validation."
    if review_bucket == "scope_review_before_sector_fill":
        return "No sector fill until the B3 listing is reviewed as core, extended, or excluded."
    if review_bucket == "ambiguous_b3_code_requires_manual_review":
        return "Keep sector blank until manual disambiguation proves the exact B3 code/listing match."
    if review_bucket == "missing_probe_row_requires_parser_or_source_review":
        return "Keep sector blank until the official parser/source produces a listing-keyed taxonomy probe row."
    if review_bucket == "no_b3_classification_code_match_source_gap":
        return "Keep stock_sector blank until official B3 or issuer taxonomy evidence matches the exact listing."
    if review_bucket == "requires_stronger_official_taxonomy_source_gap":
        return "Keep sector blank until stronger official taxonomy evidence is available."
    return "Manual B3 sector review required before any sector change."


def review_strategy_for(review_bucket: str) -> str:
    if review_bucket == "official_sector_candidate_requires_apply_gate":
        return "validate_official_b3_taxonomy_candidate_before_sector_apply"
    if review_bucket == "scope_review_before_sector_fill":
        return "decide_b3_core_extended_or_exclude_before_sector_work"
    if review_bucket == "ambiguous_b3_code_requires_manual_review":
        return "manual_b3_code_disambiguation_before_taxonomy_mapping"
    if review_bucket == "missing_probe_row_requires_parser_or_source_review":
        return "refresh_b3_taxonomy_probe_or_parser_before_sector_work"
    if review_bucket == "no_b3_classification_code_match_source_gap":
        return "keep_blank_until_stronger_official_b3_or_issuer_taxonomy"
    if review_bucket == "requires_stronger_official_taxonomy_source_gap":
        return "seek_stronger_official_taxonomy_before_sector_mapping"
    return "manual_sector_review"


def source_gap_context_for(row: dict[str, str]) -> str:
    return (
        f"gap_class={row.get('gap_class', '') or 'none'};"
        f"source_of_truth_outcome={row.get('source_of_truth_outcome', '') or 'none'};"
        f"review_needed={row.get('review_needed', '') or 'none'}"
    )


def official_source_context_for(row: dict[str, str]) -> str:
    return (
        f"b3_probe_decision={row.get('b3_probe_decision', '') or 'none'};"
        f"b3_code={row.get('b3_code', '') or 'none'};"
        f"b3_sector_update={row.get('b3_sector_update', '') or 'none'};"
        f"b3_source_url={row.get('b3_source_url', '') or 'none'}"
    )


def residual_review_context_for(row: dict[str, str]) -> str:
    return (
        f"residual_decision={row.get('residual_decision', '') or 'none'};"
        f"review_bucket={row.get('review_bucket', '') or 'none'};"
        f"apply_eligibility={row.get('apply_eligibility', '') or 'none'};"
        f"verification_evidence_required={row.get('verification_evidence_required', '') or 'none'}"
    )


def b3_code_shape(value: str) -> str:
    code = value.strip().upper()
    if not code:
        return "missing_b3_code"
    if any(char.isdigit() for char in code):
        return "alphanumeric_b3_code"
    return "alpha_b3_code"


def build_review_rows(
    *,
    tickers: list[dict[str, str]],
    source_gap_rows: list[dict[str, str]],
    source_of_truth_rows: list[dict[str, str]],
    b3_sector_probe_rows: list[dict[str, str]],
) -> list[dict[str, str]]:
    ticker_lookup = build_listing_lookup(tickers)
    source_of_truth_lookup = {
        (row.get("field", ""), listing_key(row)): row for row in source_of_truth_rows
    }
    probe_lookup = build_listing_lookup(b3_sector_probe_rows)
    rows: list[dict[str, str]] = []
    for gap in source_gap_rows:
        if gap.get("exchange") != "B3" or gap.get("field") != "missing_sector_stock":
            continue
        key = listing_key(gap)
        current = ticker_lookup.get(key, {})
        probe = probe_lookup.get(key, {})
        source_of_truth = source_of_truth_lookup.get((gap.get("field", ""), key), {})
        residual_decision = residual_decision_for(probe, source_of_truth)
        review_bucket = review_bucket_for(residual_decision)
        row = {
            "listing_key": key,
            "ticker": gap.get("ticker", ""),
            "exchange": "B3",
            "asset_type": gap.get("asset_type", ""),
            "name": gap.get("name", "") or current.get("name", ""),
            "gap_class": gap.get("gap_class", ""),
            "source_of_truth_outcome": source_of_truth.get("source_of_truth_outcome", ""),
            "source_gap_context": "",
            "review_needed": gap.get("review_needed", "true"),
            "recommended_next_source": recommended_next_source_for(review_bucket),
            "source_gate": source_gate_for(review_bucket),
            "b3_probe_decision": probe.get("decision", ""),
            "b3_code": probe.get("b3_code", ""),
            "b3_name": probe.get("b3_name", ""),
            "b3_sector_pt": probe.get("b3_sector_pt", ""),
            "b3_subsector": probe.get("b3_subsector", ""),
            "b3_segment": probe.get("b3_segment", ""),
            "b3_sector_update": probe.get("sector_update", ""),
            "b3_source_url": probe.get("source_url", ""),
            "official_source_context": "",
            "residual_decision": residual_decision,
            "review_bucket": review_bucket,
            "review_priority": review_priority_for(review_bucket),
            "review_strategy": review_strategy_for(review_bucket),
            "apply_eligibility": apply_eligibility_for(review_bucket),
            "verification_evidence_required": verification_evidence_for(review_bucket),
            "residual_review_context": "",
        }
        row["source_gap_context"] = source_gap_context_for(row)
        row["official_source_context"] = official_source_context_for(row)
        row["residual_review_context"] = residual_review_context_for(row)
        rows.append(row)
    return sorted(rows, key=lambda row: (row["review_priority"], row["review_bucket"], row["ticker"], row["listing_key"]))


def summarize(rows: list[dict[str, str]], generated_at: str) -> dict[str, Any]:
    alphanumeric_rows = [row for row in rows if b3_code_shape(row.get("b3_code", "")) == "alphanumeric_b3_code"]
    strategy_counter: Counter[str] = Counter()
    top_batch_counter: Counter[tuple[str, str, str, str, str]] = Counter()
    for row in rows:
        strategy = review_strategy_for(row.get("review_bucket", ""))
        strategy_counter[strategy] += 1
        top_batch_counter[
            (
                row.get("review_priority", ""),
                row.get("review_bucket", ""),
                row.get("gap_class", ""),
                b3_code_shape(row.get("b3_code", "")),
                row.get("asset_type", ""),
            )
        ] += 1
    return {
        "generated_at": generated_at,
        "rows": len(rows),
        "gap_class_totals": dict(sorted(Counter(row["gap_class"] for row in rows).items())),
        "residual_decision_totals": dict(sorted(Counter(row["residual_decision"] for row in rows).items())),
        "review_bucket_totals": dict(sorted(Counter(row["review_bucket"] for row in rows).items())),
        "review_priority_totals": dict(sorted(Counter(row["review_priority"] for row in rows).items())),
        "apply_eligibility_totals": dict(sorted(Counter(row["apply_eligibility"] for row in rows).items())),
        "verification_evidence_required_totals": dict(sorted(Counter(row["verification_evidence_required"] for row in rows).items())),
        "b3_probe_decision_totals": dict(
            sorted(Counter(row["b3_probe_decision"] or "missing_probe_row" for row in rows).items())
        ),
        "review_strategy_totals": dict(sorted(strategy_counter.items())),
        "top_b3_sector_review_batches": [
            {
                "review_priority": priority,
                "review_bucket": review_bucket,
                "gap_class": gap_class,
                "b3_code_shape": code_shape,
                "asset_type": asset_type,
                "rows": count,
                "review_strategy": review_strategy_for(review_bucket),
                "verification_evidence_required": verification_evidence_for(review_bucket),
                "recommended_next_source": recommended_next_source_for(review_bucket),
                "source_gate": source_gate_for(review_bucket),
            }
            for (priority, review_bucket, gap_class, code_shape, asset_type), count in sorted(
                top_batch_counter.items(),
                key=lambda item: (-item[1], item[0][0], item[0][1], item[0][2], item[0][3], item[0][4]),
            )[:10]
        ],
        "b3_code_shape_totals": dict(sorted(Counter(b3_code_shape(row.get("b3_code", "")) for row in rows).items())),
        "alphanumeric_b3_code_rows": len(alphanumeric_rows),
        "alphanumeric_b3_code_examples": [
            {
                "listing_key": row.get("listing_key", ""),
                "ticker": row.get("ticker", ""),
                "b3_code": row.get("b3_code", ""),
                "b3_probe_decision": row.get("b3_probe_decision", "") or "missing_probe_row",
            }
            for row in alphanumeric_rows[:10]
        ],
        "policy": {
            "no_guessing": "Rows in this report remain blank unless an official B3 taxonomy or reviewed issuer source maps the exact listing to a canonical stock_sector.",
            "listing_keyed_review": "Every row is keyed by listing_key and tied back to source_gap_classification/source_of_truth_decisions.",
            "b3_code_shape_review": "Alphanumeric B3 issuer-code roots are preserved for probe diagnostics; no sector is inferred from the code shape.",
        },
    }


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


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDNAMES, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def write_markdown(path: Path, rows: list[dict[str, str]], summary: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# B3 Residual Sector Review",
        "",
        f"Generated at: `{summary['generated_at']}`",
        "",
        "This report tracks remaining B3 stock-sector gaps after the official B3 sector-classification probe. It does not fill values.",
        "",
        "## Summary",
        "",
        f"- Residual B3 stock-sector gaps: `{summary['rows']}`",
        "",
        "## Residual Decisions",
        "",
        "| Decision | Rows |",
        "|---|---:|",
    ]
    for decision, count in summary["residual_decision_totals"].items():
        lines.append(f"| {decision} | {count} |")
    lines.extend(["", "## Review Priorities", "", "| Priority | Rows |", "|---|---:|"])
    for priority, count in summary["review_priority_totals"].items():
        lines.append(f"| {priority} | {count} |")
    lines.extend(["", "## Review Buckets", "", "| Bucket | Rows |", "|---|---:|"])
    for bucket, count in summary["review_bucket_totals"].items():
        lines.append(f"| {bucket} | {count} |")
    lines.extend(["", "## Apply Eligibility", "", "| Eligibility | Rows |", "|---|---:|"])
    for eligibility, count in summary["apply_eligibility_totals"].items():
        lines.append(f"| {eligibility} | {count} |")
    lines.extend(["", "## Verification Evidence", "", "| Evidence Gate | Rows |", "|---|---:|"])
    for evidence, count in summary["verification_evidence_required_totals"].items():
        lines.append(f"| {evidence} | {count} |")
    lines.extend(["", "## Review Strategies", "", "| Strategy | Rows |", "|---|---:|"])
    for strategy, count in summary["review_strategy_totals"].items():
        lines.append(f"| {strategy} | {count} |")
    lines.extend(
        [
            "",
            "## Top Review Batches",
            "",
            "| Priority | Bucket | Gap class | B3 code shape | Asset type | Rows | Strategy | Evidence gate | Recommended next source | Source gate |",
            "|---|---|---|---|---|---:|---|---|---|---|",
        ]
    )
    for batch in summary["top_b3_sector_review_batches"]:
        lines.append(
            f"| {batch['review_priority']} | {batch['review_bucket']} | {batch['gap_class']} | "
            f"{batch['b3_code_shape']} | {batch['asset_type']} | {batch['rows']} | {batch['review_strategy']} | "
            f"{batch['verification_evidence_required']} | {batch['recommended_next_source']} | "
            f"{batch['source_gate']} |"
        )
    lines.extend(["", "## B3 Probe Decisions", "", "| Probe decision | Rows |", "|---|---:|"])
    for decision, count in summary["b3_probe_decision_totals"].items():
        lines.append(f"| {decision} | {count} |")
    lines.extend(["", "## B3 Code Shape Diagnostics", "", "| Shape | Rows |", "|---|---:|"])
    for shape, count in summary["b3_code_shape_totals"].items():
        lines.append(f"| {shape} | {count} |")
    lines.extend(["", "### Alphanumeric B3 Code Examples", "", "| Listing key | Ticker | B3 code | Probe decision |", "|---|---|---|---|"])
    for example in summary["alphanumeric_b3_code_examples"]:
        lines.append(
            f"| {example['listing_key']} | {example['ticker']} | {example['b3_code']} | {example['b3_probe_decision']} |"
        )
    lines.extend(["", "## Rows", "", "| Listing key | Priority | Bucket | Name | B3 code | Probe | Decision |", "|---|---|---|---|---|---|---|"])
    for row in rows:
        lines.append(
            f"| {row['listing_key']} | {row['review_priority']} | {row['review_bucket']} | {row['name']} | {row['b3_code']} | "
            f"{row['b3_probe_decision'] or 'missing_probe_row'} | {row['residual_decision']} |"
        )
    lines.extend(
        [
            "",
            "## Policy",
            "",
            "- No sector is inferred from ticker root, issuer-name shape, or peer rows in this report.",
            "- Fill only after a direct official B3 taxonomy row or reviewed issuer source satisfies the row-level source gate.",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a listing-keyed review report for residual B3 stock-sector gaps.")
    parser.add_argument("--tickers-csv", type=Path, default=DEFAULT_TICKERS_CSV)
    parser.add_argument("--source-gap-classification-csv", type=Path, default=DEFAULT_SOURCE_GAP_CLASSIFICATION_CSV)
    parser.add_argument("--source-of-truth-decisions-csv", type=Path, default=DEFAULT_SOURCE_OF_TRUTH_DECISIONS_CSV)
    parser.add_argument("--b3-sector-probe-csv", type=Path, default=DEFAULT_B3_SECTOR_PROBE_CSV)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_CSV_OUT)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_JSON_OUT)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_MD_OUT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    rows = build_review_rows(
        tickers=load_csv(args.tickers_csv),
        source_gap_rows=load_csv(args.source_gap_classification_csv),
        source_of_truth_rows=load_csv(args.source_of_truth_decisions_csv),
        b3_sector_probe_rows=load_csv(args.b3_sector_probe_csv),
    )
    summary = summarize(rows, utc_now_iso())
    write_csv(args.csv_out, rows)
    payload = build_json_payload(
        summary=summary,
        rows=rows,
        source_files={
            "tickers_csv": display_path(args.tickers_csv),
            "source_gap_classification_csv": display_path(args.source_gap_classification_csv),
            "source_of_truth_decisions_csv": display_path(args.source_of_truth_decisions_csv),
            "b3_sector_probe_csv": display_path(args.b3_sector_probe_csv),
        },
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
                "csv_out": display_path(args.csv_out),
                "json_out": display_path(args.json_out),
                "md_out": display_path(args.md_out),
                "residual_decision_totals": summary["residual_decision_totals"],
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()

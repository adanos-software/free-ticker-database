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
DEFAULT_INSTRUMENT_SCOPES_CSV = DATA_DIR / "instrument_scopes.csv"
DEFAULT_SOURCE_GAP_CLASSIFICATION_CSV = REPORTS_DIR / "source_gap_classification.csv"
DEFAULT_SOURCE_OF_TRUTH_DECISIONS_CSV = REPORTS_DIR / "source_of_truth_decisions.csv"
DEFAULT_MASTERFILE_REFERENCE_CSV = DATA_DIR / "masterfiles" / "reference.csv"
DEFAULT_COTAHIST_PROBE_CSV = DATA_DIR / "b3_verification" / "cotahist_isin_probe_current.csv"
DEFAULT_CSV_OUT = REPORTS_DIR / "b3_residual_isin_review.csv"
DEFAULT_JSON_OUT = REPORTS_DIR / "b3_residual_isin_review.json"
DEFAULT_MD_OUT = REPORTS_DIR / "b3_residual_isin_review.md"

CSV_FIELDNAMES = [
    "listing_key",
    "ticker",
    "exchange",
    "asset_type",
    "name",
    "gap_class",
    "source_of_truth_outcome",
    "source_gap_context",
    "current_instrument_scope",
    "current_scope_reason",
    "scope_review_context",
    "review_needed",
    "recommended_next_source",
    "source_gate",
    "b3_instruments_equities_match",
    "b3_instruments_equities_isin",
    "cotahist_probe_decision",
    "cotahist_probe_isin",
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


def build_reference_lookup(masterfile_rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    lookup: dict[str, dict[str, str]] = {}
    for row in masterfile_rows:
        if row.get("source_key") != "b3_instruments_equities":
            continue
        if row.get("exchange") != "B3":
            continue
        if row.get("official") != "true" or row.get("listing_status") != "active":
            continue
        ticker = row.get("ticker", "")
        if ticker:
            lookup[ticker] = row
    return lookup


def build_cotahist_lookup(cotahist_rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    return {row.get("ticker", ""): row for row in cotahist_rows if row.get("ticker")}


def residual_decision_for(
    row: dict[str, str],
    reference: dict[str, str],
    cotahist: dict[str, str],
    source_of_truth: dict[str, str],
) -> str:
    if reference.get("isin", "").strip():
        return "official_b3_isin_available_rebuild_required"
    if cotahist.get("decision") == "accept" and cotahist.get("cotahist_isin", "").strip():
        return "official_cotahist_isin_available_review_apply"
    if source_of_truth.get("source_of_truth_outcome") == "core_exclusion_candidate":
        return "core_exclusion_candidate_requires_scope_review"
    if row.get("gap_class") == "official_current_directory_absent_identifier_gap":
        return "accepted_source_gap_not_in_current_b3_directory"
    if row.get("gap_class") == "inactive_or_legacy_identifier_gap":
        return "accepted_source_gap_requires_active_listing_evidence"
    return "accepted_source_gap_requires_fund_or_registry_source"


def review_bucket_for(residual_decision: str) -> str:
    if residual_decision in {
        "official_b3_isin_available_rebuild_required",
        "official_cotahist_isin_available_review_apply",
    }:
        return "official_isin_candidate_requires_apply_gate"
    if residual_decision == "core_exclusion_candidate_requires_scope_review":
        return "scope_review_before_identifier_fill"
    if residual_decision == "accepted_source_gap_requires_active_listing_evidence":
        return "active_listing_evidence_required_source_gap"
    if residual_decision == "accepted_source_gap_requires_fund_or_registry_source":
        return "fund_or_registry_identifier_source_gap"
    if residual_decision == "accepted_source_gap_not_in_current_b3_directory":
        return "not_in_current_b3_directory_source_gap"
    return "identifier_source_gap"


def review_priority_for(review_bucket: str) -> str:
    if review_bucket in {
        "official_isin_candidate_requires_apply_gate",
        "scope_review_before_identifier_fill",
    }:
        return "P1"
    if review_bucket in {
        "active_listing_evidence_required_source_gap",
        "fund_or_registry_identifier_source_gap",
    }:
        return "P2"
    if review_bucket == "not_in_current_b3_directory_source_gap":
        return "P3"
    return "P4"


def apply_eligibility_for(review_bucket: str) -> str:
    if review_bucket == "official_isin_candidate_requires_apply_gate":
        return "apply_only_after_listing_key_and_checksum_validation"
    if review_bucket == "scope_review_before_identifier_fill":
        return "blocked_until_core_or_extended_scope_decision"
    if review_bucket in {
        "active_listing_evidence_required_source_gap",
        "fund_or_registry_identifier_source_gap",
        "not_in_current_b3_directory_source_gap",
    }:
        return "source_gap_keep_blank_until_official_identifier_evidence"
    return "manual_review_required"


def verification_evidence_for(review_bucket: str) -> str:
    if review_bucket == "official_isin_candidate_requires_apply_gate":
        return "official_b3_or_cotahist_isin_with_exact_listing_key_name_and_isin_checksum"
    if review_bucket == "scope_review_before_identifier_fill":
        return "scope_decision_for_core_extended_or_exclude_before_identifier_fill"
    if review_bucket == "active_listing_evidence_required_source_gap":
        return "current_active_b3_listing_status_plus_direct_identifier_source"
    if review_bucket == "fund_or_registry_identifier_source_gap":
        return "official_fund_trust_registry_or_prospectus_with_exact_symbol_product_name_and_valid_isin"
    if review_bucket == "not_in_current_b3_directory_source_gap":
        return "new_current_b3_directory_or_official_delisting_inactive_evidence"
    return "manual_review_required"


def recommended_next_source_for(review_bucket: str) -> str:
    if review_bucket == "official_isin_candidate_requires_apply_gate":
        return "Official B3 InstrumentsEquities or COTAHIST row exposing a valid ISIN for the exact listing."
    if review_bucket == "scope_review_before_identifier_fill":
        return "Current B3 source plus reviewed core, extended, or exclude scope decision before identifier work."
    if review_bucket == "active_listing_evidence_required_source_gap":
        return "Current B3 active-listing status source plus a direct official identifier source."
    if review_bucket == "fund_or_registry_identifier_source_gap":
        return "Official fund/trust registry, prospectus, CSD, or reviewed identifier feed with exact product match."
    if review_bucket == "not_in_current_b3_directory_source_gap":
        return "Current official B3 directory, CSD/security registry, or official delisting/inactive evidence."
    return "Manual B3 identifier review source with exact listing-key evidence."


def source_gate_for(review_bucket: str) -> str:
    if review_bucket == "official_isin_candidate_requires_apply_gate":
        return "Apply only after listing-key, issuer/product name, and ISIN checksum validation."
    if review_bucket == "scope_review_before_identifier_fill":
        return "No ISIN fill until the B3 listing is reviewed as core, extended, or excluded."
    if review_bucket == "active_listing_evidence_required_source_gap":
        return "Keep ISIN blank until active listing status and a direct identifier source are both present."
    if review_bucket == "fund_or_registry_identifier_source_gap":
        return "Keep ISIN blank until official fund/trust product evidence exposes a valid checksum ISIN."
    if review_bucket == "not_in_current_b3_directory_source_gap":
        return "Keep ISIN blank unless the listing reappears in a current official directory or registry evidence."
    return "Manual B3 identifier review required before any ISIN change."


def review_strategy_for(review_bucket: str) -> str:
    if review_bucket == "official_isin_candidate_requires_apply_gate":
        return "validate_official_b3_or_cotahist_isin_before_apply"
    if review_bucket == "scope_review_before_identifier_fill":
        return "decide_b3_core_extended_or_exclude_before_identifier_work"
    if review_bucket == "active_listing_evidence_required_source_gap":
        return "seek_current_active_b3_listing_status_then_identifier_source"
    if review_bucket == "fund_or_registry_identifier_source_gap":
        return "seek_official_fund_trust_registry_or_prospectus_isin"
    if review_bucket == "not_in_current_b3_directory_source_gap":
        return "keep_blank_until_current_b3_directory_or_registry_evidence"
    return "manual_identifier_review"


def source_gap_context_for(row: dict[str, str]) -> str:
    return (
        f"gap_class={row.get('gap_class', '') or 'none'};"
        f"source_of_truth_outcome={row.get('source_of_truth_outcome', '') or 'none'};"
        f"review_needed={row.get('review_needed', '') or 'none'}"
    )


def official_source_context_for(row: dict[str, str]) -> str:
    return (
        f"b3_instruments_equities_match={row.get('b3_instruments_equities_match', '') or 'none'};"
        f"b3_instruments_equities_isin={row.get('b3_instruments_equities_isin', '') or 'none'};"
        f"cotahist_probe_decision={row.get('cotahist_probe_decision', '') or 'none'};"
        f"cotahist_probe_isin={row.get('cotahist_probe_isin', '') or 'none'}"
    )


def scope_review_context_for(row: dict[str, str]) -> str:
    return (
        f"current_instrument_scope={row.get('current_instrument_scope', '') or 'none'};"
        f"current_scope_reason={row.get('current_scope_reason', '') or 'none'};"
        f"source_of_truth_outcome={row.get('source_of_truth_outcome', '') or 'none'}"
    )


def residual_review_context_for(row: dict[str, str]) -> str:
    return (
        f"residual_decision={row.get('residual_decision', '') or 'none'};"
        f"review_bucket={row.get('review_bucket', '') or 'none'};"
        f"apply_eligibility={row.get('apply_eligibility', '') or 'none'};"
        f"verification_evidence_required={row.get('verification_evidence_required', '') or 'none'}"
    )


def build_review_rows(
    *,
    tickers: list[dict[str, str]],
    instrument_scope_rows: list[dict[str, str]],
    source_gap_rows: list[dict[str, str]],
    source_of_truth_rows: list[dict[str, str]],
    masterfile_rows: list[dict[str, str]],
    cotahist_rows: list[dict[str, str]],
) -> list[dict[str, str]]:
    ticker_lookup = {f"{row.get('exchange', '')}::{row.get('ticker', '')}": row for row in tickers}
    instrument_scope_lookup = {
        listing_key(row): row
        for row in instrument_scope_rows or []
        if (row.get("exchange") == "B3" or str(row.get("listing_key", "")).startswith("B3::"))
    }
    source_of_truth_lookup = {
        (row.get("field", ""), listing_key(row)): row for row in source_of_truth_rows
    }
    reference_lookup = build_reference_lookup(masterfile_rows)
    cotahist_lookup = build_cotahist_lookup(cotahist_rows)
    rows: list[dict[str, str]] = []
    for gap in source_gap_rows:
        if gap.get("exchange") != "B3" or gap.get("field") != "missing_isin_primary":
            continue
        key = listing_key(gap)
        ticker = gap.get("ticker", "")
        current = ticker_lookup.get(key, {})
        current_scope = instrument_scope_lookup.get(key, {})
        reference = reference_lookup.get(ticker, {})
        cotahist = cotahist_lookup.get(ticker, {})
        source_of_truth = source_of_truth_lookup.get((gap.get("field", ""), key), {})
        residual_decision = residual_decision_for(gap, reference, cotahist, source_of_truth)
        review_bucket = review_bucket_for(residual_decision)
        row = {
            "listing_key": key,
            "ticker": ticker,
            "exchange": "B3",
            "asset_type": gap.get("asset_type", ""),
            "name": gap.get("name", "") or current.get("name", ""),
            "gap_class": gap.get("gap_class", ""),
            "source_of_truth_outcome": source_of_truth.get("source_of_truth_outcome", ""),
            "source_gap_context": "",
            "current_instrument_scope": current_scope.get("instrument_scope", ""),
            "current_scope_reason": current_scope.get("scope_reason", ""),
            "scope_review_context": "",
            "review_needed": gap.get("review_needed", "true"),
            "recommended_next_source": recommended_next_source_for(review_bucket),
            "source_gate": source_gate_for(review_bucket),
            "b3_instruments_equities_match": "true" if reference else "false",
            "b3_instruments_equities_isin": reference.get("isin", ""),
            "cotahist_probe_decision": cotahist.get("decision", ""),
            "cotahist_probe_isin": cotahist.get("cotahist_isin", ""),
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
        row["scope_review_context"] = scope_review_context_for(row)
        row["official_source_context"] = official_source_context_for(row)
        row["residual_review_context"] = residual_review_context_for(row)
        rows.append(row)
    return sorted(rows, key=lambda row: (row["review_priority"], row["review_bucket"], row["gap_class"], row["ticker"]))


def b3_official_source_identifier_exposure(masterfile_rows: list[dict[str, str]] | None) -> dict[str, dict[str, int]]:
    exposure: dict[str, dict[str, int]] = {}
    for row in masterfile_rows or []:
        if row.get("exchange") != "B3" or row.get("official") != "true":
            continue
        source_key = row.get("source_key") or "unknown"
        source_exposure = exposure.setdefault(
            source_key,
            {
                "rows": 0,
                "isin_present_rows": 0,
                "isin_missing_rows": 0,
            },
        )
        source_exposure["rows"] += 1
        if row.get("isin", "").strip():
            source_exposure["isin_present_rows"] += 1
        else:
            source_exposure["isin_missing_rows"] += 1
    return dict(sorted(exposure.items()))


def summarize(
    rows: list[dict[str, str]],
    generated_at: str,
    masterfile_rows: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    strategy_counter: Counter[str] = Counter()
    top_batch_counter: Counter[tuple[str, str, str, str]] = Counter()
    for row in rows:
        strategy = review_strategy_for(row.get("review_bucket", ""))
        strategy_counter[strategy] += 1
        top_batch_counter[
            (
                row.get("review_priority", ""),
                row.get("review_bucket", ""),
                row.get("gap_class", ""),
                row.get("asset_type", ""),
            )
        ] += 1
    return {
        "generated_at": generated_at,
        "rows": len(rows),
        "gap_class_totals": dict(sorted(Counter(row["gap_class"] for row in rows).items())),
        "residual_decision_totals": dict(sorted(Counter(row["residual_decision"] for row in rows).items())),
        "current_instrument_scope_totals": dict(
            sorted(Counter(row["current_instrument_scope"] or "missing_scope" for row in rows).items())
        ),
        "current_scope_reason_totals": dict(
            sorted(Counter(row["current_scope_reason"] or "missing_scope_reason" for row in rows).items())
        ),
        "review_bucket_totals": dict(sorted(Counter(row["review_bucket"] for row in rows).items())),
        "review_priority_totals": dict(sorted(Counter(row["review_priority"] for row in rows).items())),
        "apply_eligibility_totals": dict(sorted(Counter(row["apply_eligibility"] for row in rows).items())),
        "verification_evidence_required_totals": dict(sorted(Counter(row["verification_evidence_required"] for row in rows).items())),
        "cotahist_probe_decision_totals": dict(
            sorted(Counter(row["cotahist_probe_decision"] or "missing_probe_row" for row in rows).items())
        ),
        "review_strategy_totals": dict(sorted(strategy_counter.items())),
        "top_b3_isin_review_batches": [
            {
                "review_priority": priority,
                "review_bucket": review_bucket,
                "gap_class": gap_class,
                "asset_type": asset_type,
                "rows": count,
                "review_strategy": review_strategy_for(review_bucket),
                "verification_evidence_required": verification_evidence_for(review_bucket),
                "recommended_next_source": recommended_next_source_for(review_bucket),
                "source_gate": source_gate_for(review_bucket),
            }
            for (priority, review_bucket, gap_class, asset_type), count in sorted(
                top_batch_counter.items(),
                key=lambda item: (-item[1], item[0][0], item[0][1], item[0][2], item[0][3]),
            )[:10]
        ],
        "b3_official_source_identifier_exposure": b3_official_source_identifier_exposure(masterfile_rows),
        "policy": {
            "no_guessing": "Rows in this report remain blank in the dataset unless an official B3/CSD/fund source provides a direct valid ISIN.",
            "listing_keyed_review": "Every row is keyed by listing_key and tied back to source_gap_classification/source_of_truth_decisions.",
            "source_exposure_review": "Official B3 sources without exposed ISIN values are review evidence only; they do not authorize inferred identifiers.",
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
        "# B3 Residual ISIN Review",
        "",
        f"Generated at: `{summary['generated_at']}`",
        "",
        "This report tracks the remaining B3 primary-listing ISIN gaps after the official B3 InstrumentsEquities refresh and dataset rebuild. It does not fill values.",
        "",
        "## Summary",
        "",
        f"- Residual B3 ISIN gaps: `{summary['rows']}`",
        "",
        "## Residual Decisions",
        "",
        "| Decision | Rows |",
        "|---|---:|",
    ]
    for decision, count in summary["residual_decision_totals"].items():
        lines.append(f"| {decision} | {count} |")
    lines.extend(["", "## Current Scope Context", "", "| Instrument scope | Rows |", "|---|---:|"])
    for scope, count in summary["current_instrument_scope_totals"].items():
        lines.append(f"| {scope} | {count} |")
    lines.extend(["", "| Scope reason | Rows |", "|---|---:|"])
    for reason, count in summary["current_scope_reason_totals"].items():
        lines.append(f"| {reason} | {count} |")
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
            "| Priority | Bucket | Gap class | Asset type | Rows | Strategy | Evidence gate | Recommended next source | Source gate |",
            "|---|---|---|---|---:|---|---|---|---|",
        ]
    )
    for batch in summary["top_b3_isin_review_batches"]:
        lines.append(
            f"| {batch['review_priority']} | {batch['review_bucket']} | {batch['gap_class']} | {batch['asset_type']} | "
            f"{batch['rows']} | {batch['review_strategy']} | {batch['verification_evidence_required']} | "
            f"{batch['recommended_next_source']} | {batch['source_gate']} |"
        )
    lines.extend(
        [
            "",
            "## Gap Classes",
            "",
            "| Class | Rows |",
            "|---|---:|",
        ]
    )
    for gap_class, count in summary["gap_class_totals"].items():
        lines.append(f"| {gap_class} | {count} |")
    lines.extend(
        [
            "",
            "## Official B3 Source Identifier Exposure",
            "",
            "| Source | Rows | ISIN present | ISIN missing |",
            "|---|---:|---:|---:|",
        ]
    )
    for source, exposure in summary["b3_official_source_identifier_exposure"].items():
        lines.append(
            f"| {source} | {exposure['rows']} | {exposure['isin_present_rows']} | {exposure['isin_missing_rows']} |"
        )
    lines.extend(
        [
            "",
            "## Rows",
            "",
            "| Listing key | Priority | Bucket | Current scope | Scope reason | Name | Class | COTAHIST | Decision |",
            "|---|---|---|---|---|---|---|---|---|",
        ]
    )
    for row in rows:
        lines.append(
            f"| {row['listing_key']} | {row['review_priority']} | {row['review_bucket']} | "
            f"{row['current_instrument_scope']} | {row['current_scope_reason']} | {row['name']} | {row['gap_class']} | "
            f"{row['cotahist_probe_decision'] or 'missing_probe_row'} | {row['residual_decision']} |"
        )
    lines.extend(
        [
            "",
            "## Policy",
            "",
            "- No value in this report is inferred from ticker or issuer-name shape.",
            "- Fill only after a direct official B3/CSD/fund source satisfies the row-level source gate.",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a listing-keyed review report for residual B3 ISIN gaps.")
    parser.add_argument("--tickers-csv", type=Path, default=DEFAULT_TICKERS_CSV)
    parser.add_argument("--instrument-scopes-csv", type=Path, default=DEFAULT_INSTRUMENT_SCOPES_CSV)
    parser.add_argument("--source-gap-classification-csv", type=Path, default=DEFAULT_SOURCE_GAP_CLASSIFICATION_CSV)
    parser.add_argument("--source-of-truth-decisions-csv", type=Path, default=DEFAULT_SOURCE_OF_TRUTH_DECISIONS_CSV)
    parser.add_argument("--masterfile-reference-csv", type=Path, default=DEFAULT_MASTERFILE_REFERENCE_CSV)
    parser.add_argument("--cotahist-probe-csv", type=Path, default=DEFAULT_COTAHIST_PROBE_CSV)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_CSV_OUT)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_JSON_OUT)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_MD_OUT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    masterfile_rows = load_csv(args.masterfile_reference_csv)
    rows = build_review_rows(
        tickers=load_csv(args.tickers_csv),
        instrument_scope_rows=load_csv(args.instrument_scopes_csv),
        source_gap_rows=load_csv(args.source_gap_classification_csv),
        source_of_truth_rows=load_csv(args.source_of_truth_decisions_csv),
        masterfile_rows=masterfile_rows,
        cotahist_rows=load_csv(args.cotahist_probe_csv),
    )
    summary = summarize(rows, utc_now_iso(), masterfile_rows=masterfile_rows)
    write_csv(args.csv_out, rows)
    payload = build_json_payload(
        summary=summary,
        rows=rows,
        source_files={
            "tickers_csv": display_path(args.tickers_csv),
            "instrument_scopes_csv": display_path(args.instrument_scopes_csv),
            "source_gap_classification_csv": display_path(args.source_gap_classification_csv),
            "source_of_truth_decisions_csv": display_path(args.source_of_truth_decisions_csv),
            "masterfile_reference_csv": display_path(args.masterfile_reference_csv),
            "cotahist_probe_csv": display_path(args.cotahist_probe_csv),
        },
    )
    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.json_out.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
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

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

DATA_DIR = ROOT / "data"
REPORTS_DIR = DATA_DIR / "reports"
DEFAULT_CORE_LISTINGS_CSV = DATA_DIR / "core_listings.csv"
DEFAULT_SOURCE_GAP_CLASSIFICATION_CSV = REPORTS_DIR / "source_gap_classification.csv"
DEFAULT_SOURCE_OF_TRUTH_DECISIONS_CSV = REPORTS_DIR / "source_of_truth_decisions.csv"
DEFAULT_CSV_OUT = REPORTS_DIR / "primary_isin_completeness.csv"
DEFAULT_JSON_OUT = REPORTS_DIR / "primary_isin_completeness.json"
DEFAULT_MD_OUT = REPORTS_DIR / "primary_isin_completeness.md"

PRIORITY_EXCHANGES = ("NASDAQ", "ASX", "TSX", "NYSE", "TSXV", "NYSE ARCA", "NEO", "SSE")
CANADA_EXCHANGES = {"TSX", "TSXV", "NEO"}

CSV_FIELDNAMES = [
    "priority_rank",
    "listing_key",
    "ticker",
    "exchange",
    "asset_type",
    "name",
    "gap_class",
    "source_of_truth_outcome",
    "allowed_source_path",
    "apply_eligibility",
    "source_gate",
    "next_action",
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


def priority_rank(exchange: str) -> int:
    try:
        return PRIORITY_EXCHANGES.index(exchange) + 1
    except ValueError:
        return len(PRIORITY_EXCHANGES) + 1


def source_gap_lookup(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    return {
        row.get("listing_key", ""): row
        for row in rows
        if row.get("field") == "missing_isin_primary" and row.get("listing_key")
    }


def source_of_truth_lookup(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    return {
        row.get("listing_key", ""): row
        for row in rows
        if row.get("field") == "missing_isin_primary" and row.get("listing_key")
    }


def allowed_source_path_for(exchange: str, gap_class: str, asset_type: str) -> str:
    if exchange == "ASX":
        return "ASX ISIN workbook, official ASX/security registry, or issuer/trustee evidence"
    if exchange in CANADA_EXCHANGES:
        return "TMX/CDS-listed security evidence, issuer/prospectus source, or OpenFIGI after valid ISIN"
    if exchange in {"NASDAQ", "NYSE", "NYSE ARCA"}:
        return "OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches"
    if exchange == "SSE":
        return "ESMA/FCA FIRDS, GLEIF ISIN↔LEI reverse match, or official SSE/security-registry evidence"
    if gap_class in {"fund_or_trust_identifier_gap", "debt_or_securitized_identifier_gap"} or asset_type == "ETF":
        return "official fund/trust/debt prospectus, registry, FIRDS, or reviewed identifier source"
    return "GLEIF ISIN↔LEI mapping, ESMA/FCA FIRDS, OpenFIGI, or official exchange/security-registry source"


def apply_eligibility_for(gap_class: str, source_of_truth_outcome: str) -> str:
    if source_of_truth_outcome == "core_exclusion_candidate":
        return "blocked_until_core_or_extended_scope_decision"
    if gap_class in {
        "fund_or_trust_identifier_gap",
        "debt_or_securitized_identifier_gap",
        "adr_cdr_or_depositary_identifier_gap",
        "capital_pool_or_halted_identifier_gap",
        "inactive_or_legacy_identifier_gap",
    }:
        return "blocked_until_instrument_specific_identity_source"
    return "eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates"


def d1_source_gate(exchange: str, gap_class: str, source_gate: str, source_of_truth_outcome: str) -> str:
    base_gate = (
        "require_valid_isin_checksum_exact_listing_identity_match_and_no_existing_listing_or_identifier_collision"
    )
    if source_of_truth_outcome == "core_exclusion_candidate":
        suffix = f";{source_gate}" if source_gate else ""
        return f"scope_review_required_before_isin_work;{base_gate}{suffix}"
    if source_gate:
        return f"{base_gate};{source_gate}"
    if exchange == "ASX":
        return f"{base_gate};ASX workbook/security registry row must match exact ticker/name"
    if exchange in CANADA_EXCHANGES:
        return f"{base_gate};TMX/CDS or issuer/prospectus evidence must match exact Canadian listing"
    return base_gate


def next_action_for(exchange: str, gap_class: str, source_of_truth_outcome: str) -> str:
    if source_of_truth_outcome == "core_exclusion_candidate":
        return "review official instrument scope before identifier enrichment"
    if exchange == "ASX":
        return "refresh ASX ISIN workbook and review exact ticker/name matches"
    if exchange in CANADA_EXCHANGES:
        return "seek TMX/CDS or issuer/prospectus ISIN evidence for exact listing"
    if exchange in {"NASDAQ", "NYSE", "NYSE ARCA"}:
        return "probe OpenFIGI/GLEIF/FIRDS candidates and apply only through collision gates"
    if exchange == "SSE":
        return "seek official SSE/security-registry, GLEIF, or FIRDS identifier evidence"
    if gap_class == "official_identifier_not_exposed_source_gap":
        return "seek separate official identifier registry because venue source does not expose ISIN"
    return "keep blank until stronger official or review-gated identifier evidence exists"


def build_rows(
    core_rows: list[dict[str, str]],
    classification_rows: list[dict[str, str]],
    decision_rows: list[dict[str, str]],
) -> list[dict[str, str]]:
    classifications = source_gap_lookup(classification_rows)
    decisions = source_of_truth_lookup(decision_rows)
    rows: list[dict[str, str]] = []
    for row in core_rows:
        if row.get("scope_reason") != "primary_listing_missing_isin":
            continue
        key = row.get("listing_key", "")
        classification = classifications.get(key, {})
        decision = decisions.get(key, {})
        exchange = row.get("exchange", "")
        gap_class = classification.get("gap_class", "")
        source_of_truth_outcome = decision.get("source_of_truth_outcome", "")
        rows.append(
            {
                "priority_rank": str(priority_rank(exchange)),
                "listing_key": key,
                "ticker": row.get("ticker", ""),
                "exchange": exchange,
                "asset_type": row.get("asset_type", ""),
                "name": row.get("name", ""),
                "gap_class": gap_class,
                "source_of_truth_outcome": source_of_truth_outcome,
                "allowed_source_path": allowed_source_path_for(exchange, gap_class, row.get("asset_type", "")),
                "apply_eligibility": apply_eligibility_for(gap_class, source_of_truth_outcome),
                "source_gate": d1_source_gate(
                    exchange,
                    gap_class,
                    classification.get("source_gate", ""),
                    source_of_truth_outcome,
                ),
                "next_action": next_action_for(exchange, gap_class, source_of_truth_outcome),
            }
        )
    return sorted(rows, key=lambda item: (int(item["priority_rank"]), item["exchange"], item["ticker"]))


def counter_dict(counter: Counter[str]) -> dict[str, int]:
    return dict(sorted(counter.items()))


def summarize(rows: list[dict[str, str]], generated_at: str) -> dict[str, Any]:
    priority_rows = [row for row in rows if row["exchange"] in PRIORITY_EXCHANGES]
    blocked_rows = [
        row
        for row in rows
        if row["apply_eligibility"]
        in {"blocked_until_core_or_extended_scope_decision", "blocked_until_instrument_specific_identity_source"}
    ]
    return {
        "generated_at": generated_at,
        "missing_primary_isin_rows": len(rows),
        "priority_exchange_rows": len(priority_rows),
        "non_priority_exchange_rows": len(rows) - len(priority_rows),
        "blocked_rows": len(blocked_rows),
        "eligible_after_allowed_source_gates": len(rows) - len(blocked_rows),
        "priority_exchanges": list(PRIORITY_EXCHANGES),
        "exchange_totals": counter_dict(Counter(row["exchange"] for row in rows)),
        "priority_exchange_totals": counter_dict(Counter(row["exchange"] for row in priority_rows)),
        "gap_class_totals": counter_dict(Counter(row["gap_class"] for row in rows)),
        "source_of_truth_outcome_totals": counter_dict(Counter(row["source_of_truth_outcome"] for row in rows)),
        "apply_eligibility_totals": counter_dict(Counter(row["apply_eligibility"] for row in rows)),
        "policy": {
            "no_auto_fill": "This report does not fill ISINs.",
            "allowed_sources": "D1 fills require GLEIF ISIN↔LEI, ESMA/FCA FIRDS, OpenFIGI ticker→FIGI→ISIN, ASX ISIN workbook, TMX/CDS lists, or exact official/review-gated issuer/security-registry evidence.",
            "identity_gate": "Every fill must pass valid ISIN checksum, exact listing identity match, and no-collision checks before apply.",
        },
    }


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDNAMES)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in CSV_FIELDNAMES})


def write_json(path: Path, rows: list[dict[str, str]], summary: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "_meta": {
            "generated_at": summary["generated_at"],
            "source_files": {
                "core_listings_csv": "data/core_listings.csv",
                "source_gap_classification_csv": "data/reports/source_gap_classification.csv",
                "source_of_truth_decisions_csv": "data/reports/source_of_truth_decisions.csv",
            },
            "policy": summary["policy"],
        },
        "summary": summary,
        "rows": rows,
    }
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def render_markdown(rows: list[dict[str, str]], summary: dict[str, Any]) -> str:
    lines = [
        "# Primary ISIN Completeness",
        "",
        f"Generated at: `{summary['generated_at']}`",
        "",
        "This report scopes D1 primary-listing ISIN work. It does not fill values; it assigns allowed source paths and gates to every missing primary ISIN row.",
        "",
        "## Summary",
        "",
        "| Metric | Value |",
        "|---|---:|",
    ]
    for key in (
        "missing_primary_isin_rows",
        "priority_exchange_rows",
        "non_priority_exchange_rows",
        "blocked_rows",
        "eligible_after_allowed_source_gates",
    ):
        lines.append(f"| {key} | {summary[key]} |")

    lines.extend(["", "## Priority Exchanges", "", "| Exchange | Rows |", "|---|---:|"])
    for exchange in PRIORITY_EXCHANGES:
        lines.append(f"| {exchange} | {summary['priority_exchange_totals'].get(exchange, 0)} |")

    lines.extend(["", "## Gap Classes", "", "| Gap Class | Rows |", "|---|---:|"])
    for key, count in summary["gap_class_totals"].items():
        lines.append(f"| {key} | {count} |")

    lines.extend(["", "## Apply Eligibility", "", "| Eligibility | Rows |", "|---|---:|"])
    for key, count in summary["apply_eligibility_totals"].items():
        lines.append(f"| {key} | {count} |")

    lines.extend(
        [
            "",
            "## Top Rows",
            "",
            "| Exchange | Ticker | Asset Type | Gap Class | Outcome | Allowed Source Path | Apply Eligibility |",
            "|---|---|---|---|---|---|---|",
        ]
    )
    for row in rows[:120]:
        lines.append(
            f"| {row['exchange']} | {row['ticker']} | {row['asset_type']} | {row['gap_class']} | "
            f"{row['source_of_truth_outcome']} | {row['allowed_source_path']} | {row['apply_eligibility']} |"
        )

    lines.extend(
        [
            "",
            "## Policy",
            "",
            "- No ISIN is inferred from ticker, name, issuer family, sector, or peer rows.",
            "- Allowed D1 sources are GLEIF ISIN↔LEI mapping, ESMA/FCA FIRDS, OpenFIGI ticker→FIGI→ISIN, ASX ISIN workbook, TMX/CDS lists, or exact official/review-gated issuer/security-registry evidence.",
            "- Every apply still requires a valid ISIN checksum, exact listing identity match, and no-collision validation.",
        ]
    )
    return "\n".join(lines) + "\n"


def write_markdown(path: Path, rows: list[dict[str, str]], summary: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_markdown(rows, summary), encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a D1 primary ISIN completeness and source-gate report.")
    parser.add_argument("--core-listings-csv", type=Path, default=DEFAULT_CORE_LISTINGS_CSV)
    parser.add_argument("--source-gap-classification-csv", type=Path, default=DEFAULT_SOURCE_GAP_CLASSIFICATION_CSV)
    parser.add_argument("--source-of-truth-decisions-csv", type=Path, default=DEFAULT_SOURCE_OF_TRUTH_DECISIONS_CSV)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_CSV_OUT)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_JSON_OUT)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_MD_OUT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    generated_at = utc_now_iso()
    rows = build_rows(
        load_csv(args.core_listings_csv),
        load_csv(args.source_gap_classification_csv),
        load_csv(args.source_of_truth_decisions_csv),
    )
    summary = summarize(rows, generated_at)
    write_csv(args.csv_out, rows)
    write_json(args.json_out, rows, summary)
    write_markdown(args.md_out, rows, summary)
    print(
        json.dumps(
            {
                "csv_out": display_path(args.csv_out),
                "json_out": display_path(args.json_out),
                "md_out": display_path(args.md_out),
                **{key: summary[key] for key in ("missing_primary_isin_rows", "priority_exchange_rows", "blocked_rows")},
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()

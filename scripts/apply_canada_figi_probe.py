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

from scripts.enrich_global_identifiers import build_summary


DATA_DIR = ROOT / "data"
REPORTS_DIR = DATA_DIR / "reports"

DEFAULT_PROBE_CSV = REPORTS_DIR / "canada_figi_batch_probe.csv"
DEFAULT_IDENTIFIERS_EXTENDED_CSV = DATA_DIR / "identifiers_extended.csv"
DEFAULT_LISTING_INDEX_CSV = DATA_DIR / "listing_index.csv"
DEFAULT_IDENTIFIER_SUMMARY_JSON = DATA_DIR / "identifier_summary.json"
DEFAULT_OPENFIGI_GAP_CSV = DATA_DIR / "review_overrides" / "canada_figi_openfigi_gaps.csv"
DEFAULT_CSV_OUT = REPORTS_DIR / "canada_figi_apply_report.csv"
DEFAULT_JSON_OUT = REPORTS_DIR / "canada_figi_apply_report.json"
DEFAULT_MD_OUT = REPORTS_DIR / "canada_figi_apply_report.md"

IDENTIFIERS_FIELDNAMES = [
    "listing_key",
    "ticker",
    "exchange",
    "isin",
    "wkn",
    "figi",
    "cik",
    "lei",
    "figi_source",
    "cik_source",
    "lei_source",
]
LISTING_INDEX_FIELDNAMES = [
    "listing_key",
    "ticker",
    "exchange",
    "name",
    "asset_type",
    "country",
    "country_code",
    "isin",
    "wkn",
    "figi",
    "cik",
    "lei",
]
REPORT_FIELDNAMES = [
    "listing_key",
    "ticker",
    "exchange",
    "isin",
    "figi",
    "requested_exchange_hint",
    "openfigi_exch_code",
    "openfigi_candidate_count",
    "identifier_isin",
    "listing_index_isin",
    "existing_identifier_figi",
    "existing_listing_index_figi",
    "decision",
    "reason",
    "verification_evidence_required",
    "openfigi_probe_context",
    "existing_identifier_context",
    "apply_gate_context",
]
OPENFIGI_GAP_FIELDNAMES = [
    "listing_key",
    "ticker",
    "exchange",
    "asset_type",
    "name",
    "isin",
    "requested_exchange_hint",
    "candidate_count",
    "decision",
    "review_status",
    "source",
    "reviewed_at",
]


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows([{field: row.get(field, "") for field in fieldnames} for row in rows])


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def accepted_probe_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[str] = set()
    accepted: list[dict[str, str]] = []
    for row in rows:
        key = row.get("listing_key", "")
        if row.get("decision") != "accept" or not key or key in seen:
            continue
        seen.add(key)
        accepted.append(row)
    return accepted


def no_match_probe_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[tuple[str, str, str]] = set()
    no_matches: list[dict[str, str]] = []
    for row in rows:
        key = (row.get("listing_key", ""), row.get("isin", ""), row.get("requested_exchange_hint", ""))
        if row.get("decision") != "no_openfigi_match" or not all(key) or key in seen:
            continue
        seen.add(key)
        no_matches.append(row)
    return no_matches


def merge_openfigi_gap_rows(
    *,
    existing_rows: list[dict[str, str]],
    probe_rows: list[dict[str, str]],
    report_rows: list[dict[str, str]] | None = None,
    reviewed_at: str,
) -> tuple[list[dict[str, str]], int]:
    merged = [{field: row.get(field, "") for field in OPENFIGI_GAP_FIELDNAMES} for row in existing_rows]
    seen = {
        (row.get("listing_key", ""), row.get("isin", ""), row.get("requested_exchange_hint", ""))
        for row in merged
        if row.get("listing_key") and row.get("isin") and row.get("requested_exchange_hint")
    }
    added = 0
    for probe in no_match_probe_rows(probe_rows):
        key = (probe.get("listing_key", ""), probe.get("isin", ""), probe.get("requested_exchange_hint", ""))
        if key in seen:
            continue
        seen.add(key)
        added += 1
        merged.append(
            {
                "listing_key": probe.get("listing_key", ""),
                "ticker": probe.get("ticker", ""),
                "exchange": probe.get("exchange", ""),
                "asset_type": probe.get("asset_type", ""),
                "name": probe.get("name", ""),
                "isin": probe.get("isin", ""),
                "requested_exchange_hint": probe.get("requested_exchange_hint", ""),
                "candidate_count": probe.get("candidate_count", ""),
                "decision": "no_openfigi_match",
                "review_status": "accepted_source_gap_no_openfigi_match",
                "source": "OpenFIGI ID_ISIN mapping",
                "reviewed_at": reviewed_at,
            }
        )
    probe_by_key = {row.get("listing_key", ""): row for row in probe_rows if row.get("listing_key")}
    for report in report_rows or []:
        if report.get("decision") != "reject" or report.get("reason") != "figi_cross_isin_collision":
            continue
        probe = probe_by_key.get(report.get("listing_key", ""))
        key = (
            report.get("listing_key", ""),
            report.get("isin", ""),
            probe.get("requested_exchange_hint", "") if probe else "",
        )
        if not all(key) or key in seen:
            continue
        seen.add(key)
        added += 1
        merged.append(
            {
                "listing_key": report.get("listing_key", ""),
                "ticker": report.get("ticker", ""),
                "exchange": report.get("exchange", ""),
                "asset_type": probe.get("asset_type", "") if probe else "",
                "name": probe.get("name", "") if probe else "",
                "isin": report.get("isin", ""),
                "requested_exchange_hint": key[2],
                "candidate_count": probe.get("candidate_count", "") if probe else "",
                "decision": "reject",
                "review_status": "accepted_source_gap_figi_cross_isin_collision",
                "source": "OpenFIGI ID_ISIN mapping",
                "reviewed_at": reviewed_at,
            }
        )
    return sorted(merged, key=lambda row: (row["exchange"], row["ticker"], row["listing_key"], row["isin"])), added


def figi_cross_isin_collisions(rows: list[dict[str, str]]) -> set[str]:
    figi_to_isins: dict[str, set[str]] = {}
    for row in rows:
        figi = row.get("figi", "")
        isin = row.get("isin", "")
        if figi and isin:
            figi_to_isins.setdefault(figi, set()).add(isin)
    return {figi for figi, isins in figi_to_isins.items() if len(isins) > 1}


def openfigi_probe_context_for(row: dict[str, str]) -> str:
    return (
        f"requested_exchange_hint={row.get('requested_exchange_hint', '') or 'none'};"
        f"openfigi_exch_code={row.get('openfigi_exch_code', '') or 'none'};"
        f"openfigi_figi_present={'true' if row.get('figi') else 'false'};"
        f"candidate_count={row.get('openfigi_candidate_count', '') or 'none'}"
    )


def existing_identifier_context_for(row: dict[str, str]) -> str:
    return (
        f"identifier_isin={row.get('identifier_isin', '') or 'none'};"
        f"listing_index_isin={row.get('listing_index_isin', '') or 'none'};"
        f"existing_identifier_figi={row.get('existing_identifier_figi', '') or 'none'};"
        f"existing_listing_index_figi={row.get('existing_listing_index_figi', '') or 'none'}"
    )


def apply_gate_context_for(row: dict[str, str]) -> str:
    return (
        f"decision={row.get('decision', '') or 'none'};"
        f"reason={row.get('reason', '') or 'none'};"
        f"verification_evidence_required={row.get('verification_evidence_required', '') or 'none'}"
    )


def build_apply_report(
    *,
    probe_rows: list[dict[str, str]],
    identifiers_rows: list[dict[str, str]],
    listing_index_rows: list[dict[str, str]],
) -> list[dict[str, str]]:
    identifiers_by_key = {row.get("listing_key", ""): row for row in identifiers_rows}
    listing_index_by_key = {row.get("listing_key", ""): row for row in listing_index_rows}
    accepted = accepted_probe_rows(probe_rows)
    proposed_by_key: dict[str, str] = {}
    report: list[dict[str, str]] = []

    for probe in accepted:
        key = probe.get("listing_key", "")
        figi = probe.get("openfigi_figi", "").strip()
        identifier = identifiers_by_key.get(key)
        listing_index = listing_index_by_key.get(key)
        reason = ""
        decision = "apply"
        if not identifier:
            decision = "reject"
            reason = "missing_identifier_row"
        elif not listing_index:
            decision = "reject"
            reason = "missing_listing_index_row"
        elif not figi:
            decision = "reject"
            reason = "missing_openfigi_figi"
        elif probe.get("requested_exchange_hint") != probe.get("openfigi_exch_code"):
            decision = "reject"
            reason = "exchange_hint_mismatch"
        elif identifier.get("isin") != probe.get("isin"):
            decision = "reject"
            reason = "identifier_isin_mismatch"
        elif listing_index.get("isin") != probe.get("isin"):
            decision = "reject"
            reason = "listing_index_isin_mismatch"
        elif identifier.get("figi") == figi:
            decision = "skip"
            reason = "identifier_figi_already_set_to_same_value"
        elif identifier.get("figi") and identifier.get("figi") != figi:
            decision = "reject"
            reason = "identifier_figi_already_set_to_different_value"
        else:
            proposed_by_key[key] = figi
            reason = "accepted_probe_match"
        report.append(
            row := {
                "listing_key": key,
                "ticker": probe.get("ticker", ""),
                "exchange": probe.get("exchange", ""),
                "isin": probe.get("isin", ""),
                "figi": figi,
                "requested_exchange_hint": probe.get("requested_exchange_hint", ""),
                "openfigi_exch_code": probe.get("openfigi_exch_code", ""),
                "openfigi_candidate_count": probe.get("candidate_count", ""),
                "identifier_isin": identifier.get("isin", "") if identifier else "",
                "listing_index_isin": listing_index.get("isin", "") if listing_index else "",
                "existing_identifier_figi": identifier.get("figi", "") if identifier else "",
                "existing_listing_index_figi": listing_index.get("figi", "") if listing_index else "",
                "decision": decision,
                "reason": reason,
                "verification_evidence_required": "listing_key_isin_exchange_hint_openfigi_figi_and_cross_isin_collision_gates",
            }
        )
        row["openfigi_probe_context"] = openfigi_probe_context_for(row)
        row["existing_identifier_context"] = existing_identifier_context_for(row)
        row["apply_gate_context"] = apply_gate_context_for(row)

    simulated = [dict(row) for row in identifiers_rows]
    for row in simulated:
        figi = proposed_by_key.get(row.get("listing_key", ""))
        if figi:
            row["figi"] = figi
            row["figi_source"] = "OpenFIGI"
    collision_figis = figi_cross_isin_collisions(simulated)
    if collision_figis:
        for row in report:
            if row["decision"] == "apply" and row["figi"] in collision_figis:
                row["decision"] = "reject"
                row["reason"] = "figi_cross_isin_collision"
                row["apply_gate_context"] = apply_gate_context_for(row)
    return report


def apply_report_rows(
    *,
    report_rows: list[dict[str, str]],
    identifiers_rows: list[dict[str, str]],
    listing_index_rows: list[dict[str, str]],
) -> int:
    figi_by_key = {
        row["listing_key"]: row["figi"]
        for row in report_rows
        if row.get("decision") == "apply" and row.get("figi")
    }
    applied = 0
    for row in identifiers_rows:
        figi = figi_by_key.get(row.get("listing_key", ""))
        if figi and row.get("figi") != figi:
            row["figi"] = figi
            row["figi_source"] = "OpenFIGI"
            applied += 1
    for row in listing_index_rows:
        figi = figi_by_key.get(row.get("listing_key", ""))
        if figi:
            row["figi"] = figi
    return applied


def summarize(report_rows: list[dict[str, str]], generated_at: str, applied: bool, gap_rows_added: int = 0) -> dict[str, Any]:
    return {
        "generated_at": generated_at,
        "applied": applied,
        "rows": len(report_rows),
        "gap_rows_added": gap_rows_added,
        "decision_totals": dict(sorted(Counter(row["decision"] for row in report_rows).items())),
        "reason_totals": dict(sorted(Counter(row["reason"] for row in report_rows).items())),
        "applied_rows": sum(row["decision"] == "apply" for row in report_rows) if applied else 0,
        "policy": {
            "source": "Only decision=accept rows from canada_figi_batch_probe are eligible.",
            "gates": "listing_key, ISIN, requested exchange hint, non-empty FIGI, and cross-ISIN FIGI collision gates must pass before writing.",
            "openfigi_no_match": "decision=no_openfigi_match probe rows are persisted as reviewed source gaps and excluded from repeat probing.",
        },
    }


def write_markdown(path: Path, summary: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Canada FIGI Apply Report",
        "",
        f"Generated at: `{summary['generated_at']}`",
        "",
        "This report applies accepted Canada OpenFIGI probe rows only after listing-key, ISIN, exchange-hint, and collision gates.",
        "",
        "## Summary",
        "",
        f"- Rows: `{summary['rows']}`",
        f"- Applied: `{summary['applied']}`",
        f"- Applied rows: `{summary['applied_rows']}`",
        f"- OpenFIGI no-match source gaps added: `{summary['gap_rows_added']}`",
        "",
        "## Decisions",
        "",
        "| Decision | Rows |",
        "|---|---:|",
    ]
    for decision, count in summary["decision_totals"].items():
        lines.append(f"| {decision} | {count} |")
    lines.extend(["", "## Reasons", "", "| Reason | Rows |", "|---|---:|"])
    for reason, count in summary["reason_totals"].items():
        lines.append(f"| {reason} | {count} |")
    lines.extend(
        [
            "",
            "## Policy",
            "",
            "- No FIGI is inferred from symbol or issuer name.",
            "- OpenFIGI no-match rows are recorded as source gaps, not filled values.",
            "- Rows rejected by any gate remain unchanged.",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Apply accepted Canada FIGI probe rows after strict gates.")
    parser.add_argument("--probe-csv", type=Path, default=DEFAULT_PROBE_CSV)
    parser.add_argument("--identifiers-extended-csv", type=Path, default=DEFAULT_IDENTIFIERS_EXTENDED_CSV)
    parser.add_argument("--listing-index-csv", type=Path, default=DEFAULT_LISTING_INDEX_CSV)
    parser.add_argument("--identifier-summary-json", type=Path, default=DEFAULT_IDENTIFIER_SUMMARY_JSON)
    parser.add_argument("--openfigi-gap-csv", type=Path, default=DEFAULT_OPENFIGI_GAP_CSV)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_CSV_OUT)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_JSON_OUT)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_MD_OUT)
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    identifiers_rows = load_csv(args.identifiers_extended_csv)
    listing_index_rows = load_csv(args.listing_index_csv)
    probe_rows = load_csv(args.probe_csv)
    report_rows = build_apply_report(
        probe_rows=probe_rows,
        identifiers_rows=identifiers_rows,
        listing_index_rows=listing_index_rows,
    )
    applied_count = 0
    gap_rows_added = 0
    if args.apply:
        applied_count = apply_report_rows(
            report_rows=report_rows,
            identifiers_rows=identifiers_rows,
            listing_index_rows=listing_index_rows,
        )
        gap_rows, gap_rows_added = merge_openfigi_gap_rows(
            existing_rows=load_csv(args.openfigi_gap_csv),
            probe_rows=probe_rows,
            report_rows=report_rows,
            reviewed_at=utc_now_iso(),
        )
        write_csv(args.identifiers_extended_csv, IDENTIFIERS_FIELDNAMES, identifiers_rows)
        write_csv(args.listing_index_csv, LISTING_INDEX_FIELDNAMES, listing_index_rows)
        write_csv(args.openfigi_gap_csv, OPENFIGI_GAP_FIELDNAMES, gap_rows)
        args.identifier_summary_json.write_text(
            json.dumps(build_summary(identifiers_rows), indent=2) + "\n",
            encoding="utf-8",
        )
    summary = summarize(report_rows, utc_now_iso(), args.apply, gap_rows_added)
    summary["written_rows"] = applied_count
    write_csv(args.csv_out, REPORT_FIELDNAMES, report_rows)
    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.json_out.write_text(
        json.dumps({"summary": summary, "rows": report_rows}, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    write_markdown(args.md_out, summary)
    print(
        json.dumps(
            {
                "rows": len(report_rows),
                "applied": args.apply,
                "written_rows": applied_count,
                "gap_rows_added": gap_rows_added,
                "csv_out": display_path(args.csv_out),
                "json_out": display_path(args.json_out),
                "md_out": display_path(args.md_out),
                "decision_totals": summary["decision_totals"],
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()

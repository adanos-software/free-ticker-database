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
HISTORY_DIR = ROOT / "data" / "history"

DEFAULT_B3_RESIDUAL_ISIN_CSV = REPORTS_DIR / "b3_residual_isin_review.csv"
DEFAULT_B3_MASTERFILE_GAP_CSV = REPORTS_DIR / "b3_masterfile_gap_review.csv"
DEFAULT_LISTING_STATUS_HISTORY_CSV = HISTORY_DIR / "listing_status_history.csv"
DEFAULT_OHLCV_PLAUSIBILITY_CSV = REPORTS_DIR / "ohlcv_plausibility.csv"
DEFAULT_CSV_OUT = REPORTS_DIR / "b3_core_scope_review_queue.csv"
DEFAULT_JSON_OUT = REPORTS_DIR / "b3_core_scope_review_queue.json"
DEFAULT_MD_OUT = REPORTS_DIR / "b3_core_scope_review_queue.md"

CSV_FIELDNAMES = [
    "listing_key",
    "ticker",
    "exchange",
    "asset_type",
    "name",
    "gap_class",
    "current_instrument_scope",
    "current_scope_reason",
    "source_of_truth_outcome",
    "residual_decision",
    "masterfile_source_presence",
    "b3_resolution_queue",
    "listing_history_status",
    "listing_history_last_observed_at",
    "ohlcv_plausibility_status",
    "ohlcv_review_bucket",
    "scope_review_queue",
    "scope_decision_gate",
    "recommended_scope_action",
    "verification_evidence_required",
    "recommended_next_source",
    "source_gate",
    "review_context",
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


def by_listing_key(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    return {row["listing_key"]: row for row in rows if row.get("listing_key")}


def select_scope_candidates(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    return [
        row
        for row in rows
        if row.get("exchange") == "B3"
        and row.get("review_bucket") == "scope_review_before_identifier_fill"
        and row.get("residual_decision") == "core_exclusion_candidate_requires_scope_review"
    ]


def scope_review_queue_for(row: dict[str, str]) -> str:
    gap_class = row.get("gap_class", "")
    if gap_class == "fund_or_trust_identifier_gap":
        return "b3_fund_or_trust_core_scope_review"
    if gap_class == "inactive_or_legacy_identifier_gap":
        return "b3_inactive_or_legacy_core_scope_review"
    return "b3_core_scope_review"


def verification_evidence_for(scope_review_queue: str) -> str:
    if scope_review_queue == "b3_fund_or_trust_core_scope_review":
        return "current_b3_fund_product_or_issuer_registry_evidence_plus_core_extended_or_exclude_scope_decision"
    if scope_review_queue == "b3_inactive_or_legacy_core_scope_review":
        return "current_active_b3_directory_or_official_inactive_delisting_evidence_plus_scope_decision"
    return "current_official_b3_listing_or_registry_evidence_plus_scope_decision"


def recommended_next_source_for(scope_review_queue: str) -> str:
    if scope_review_queue == "b3_fund_or_trust_core_scope_review":
        return "Current B3 fund/ETF/FII source, fund administrator page, issuer/sponsor page, CVM fund registry, or prospectus."
    if scope_review_queue == "b3_inactive_or_legacy_core_scope_review":
        return "Current B3 exchange directory, B3 issuer page, CVM filing, issuer investor-relations page, or official delisting/inactive notice."
    return "Current official B3 directory, registry, issuer, or CVM evidence for the exact listing."


def source_gate_for(scope_review_queue: str) -> str:
    if scope_review_queue == "b3_fund_or_trust_core_scope_review":
        return "No ISIN, category, name, or scope change until the exact product is proven current or excluded by official evidence."
    if scope_review_queue == "b3_inactive_or_legacy_core_scope_review":
        return "Do not delete, rename, extend, or exclude until current active or inactive status is proven by official evidence."
    return "No scope or metadata change until exact listing-keyed official evidence is reviewed."


def scope_decision_gate_for(scope_review_queue: str) -> str:
    if scope_review_queue == "b3_fund_or_trust_core_scope_review":
        return "decide_core_extended_or_exclude_before_identifier_or_category_work"
    if scope_review_queue == "b3_inactive_or_legacy_core_scope_review":
        return "decide_core_extended_or_exclude_before_identifier_or_listing_status_work"
    return "decide_core_extended_or_exclude_before_metadata_work"


def recommended_scope_action_for(scope_review_queue: str) -> str:
    if scope_review_queue == "b3_fund_or_trust_core_scope_review":
        return "review_product_currentness_then_choose_core_extended_or_exclude_keep_identifier_blank_until_official_isin"
    if scope_review_queue == "b3_inactive_or_legacy_core_scope_review":
        return "review_active_or_inactive_status_then_choose_core_extended_or_exclude_keep_identifier_blank_until_official_isin"
    return "review_scope_then_keep_metadata_blank_until_official_evidence"


def review_context_for(row: dict[str, str]) -> str:
    return (
        f"gap_class={row.get('gap_class', '') or 'none'};"
        f"masterfile_source_presence={row.get('masterfile_source_presence', '') or 'none'};"
        f"b3_resolution_queue={row.get('b3_resolution_queue', '') or 'none'};"
        f"listing_history_status={row.get('listing_history_status', '') or 'none'};"
        f"ohlcv_plausibility_status={row.get('ohlcv_plausibility_status', '') or 'none'};"
        f"scope_decision_gate={row.get('scope_decision_gate', '') or 'none'}"
    )


def build_queue_rows(
    *,
    residual_rows: list[dict[str, str]],
    masterfile_gap_rows: list[dict[str, str]],
    listing_status_rows: list[dict[str, str]],
    ohlcv_rows: list[dict[str, str]],
) -> list[dict[str, str]]:
    masterfile_lookup = by_listing_key(masterfile_gap_rows)
    status_lookup = by_listing_key(listing_status_rows)
    ohlcv_lookup = by_listing_key(ohlcv_rows)
    queue_rows: list[dict[str, str]] = []
    for residual in select_scope_candidates(residual_rows):
        key = residual["listing_key"]
        masterfile = masterfile_lookup.get(key, {})
        status = status_lookup.get(key, {})
        ohlcv = ohlcv_lookup.get(key, {})
        scope_review_queue = scope_review_queue_for(residual)
        row = {
            "listing_key": key,
            "ticker": residual.get("ticker", ""),
            "exchange": residual.get("exchange", ""),
            "asset_type": residual.get("asset_type", ""),
            "name": residual.get("name", ""),
            "gap_class": residual.get("gap_class", ""),
            "current_instrument_scope": residual.get("current_instrument_scope", ""),
            "current_scope_reason": residual.get("current_scope_reason", ""),
            "source_of_truth_outcome": residual.get("source_of_truth_outcome", ""),
            "residual_decision": residual.get("residual_decision", ""),
            "masterfile_source_presence": masterfile.get("source_presence", ""),
            "b3_resolution_queue": masterfile.get("b3_resolution_queue", ""),
            "listing_history_status": status.get("status", ""),
            "listing_history_last_observed_at": status.get("last_observed_at", ""),
            "ohlcv_plausibility_status": ohlcv.get("plausibility_status", ""),
            "ohlcv_review_bucket": ohlcv.get("review_bucket", ""),
            "scope_review_queue": scope_review_queue,
            "scope_decision_gate": scope_decision_gate_for(scope_review_queue),
            "recommended_scope_action": recommended_scope_action_for(scope_review_queue),
            "verification_evidence_required": verification_evidence_for(scope_review_queue),
            "recommended_next_source": recommended_next_source_for(scope_review_queue),
            "source_gate": source_gate_for(scope_review_queue),
            "review_context": "",
        }
        row["review_context"] = review_context_for(row)
        queue_rows.append(row)
    return sorted(queue_rows, key=lambda row: (row["scope_review_queue"], row["gap_class"], row["ticker"]))


def summarize(rows: list[dict[str, str]], generated_at: str) -> dict[str, Any]:
    return {
        "generated_at": generated_at,
        "rows": len(rows),
        "scope_review_queue_totals": dict(sorted(Counter(row["scope_review_queue"] for row in rows).items())),
        "gap_class_totals": dict(sorted(Counter(row["gap_class"] for row in rows).items())),
        "asset_type_totals": dict(sorted(Counter(row["asset_type"] for row in rows).items())),
        "masterfile_source_presence_totals": dict(
            sorted(Counter(row["masterfile_source_presence"] or "missing_masterfile_gap_row" for row in rows).items())
        ),
        "listing_history_status_totals": dict(
            sorted(Counter(row["listing_history_status"] or "missing_listing_history_row" for row in rows).items())
        ),
        "ohlcv_plausibility_status_totals": dict(
            sorted(Counter(row["ohlcv_plausibility_status"] or "not_sampled" for row in rows).items())
        ),
        "verification_evidence_required_totals": dict(
            sorted(Counter(row["verification_evidence_required"] for row in rows).items())
        ),
        "policy": {
            "scope_first": "Core exclusion candidates must be resolved as core, extended, or exclude before B3 identifier/category work.",
            "no_data_apply": "This queue does not authorize ISIN, category, name, symbol, listing-status, or scope changes.",
            "source_gate": "Only exact listing-keyed official B3, CVM, issuer, fund administrator, registry, or prospectus evidence can close a row.",
        },
    }


def build_payload(
    *,
    residual_rows: list[dict[str, str]],
    masterfile_gap_rows: list[dict[str, str]],
    listing_status_rows: list[dict[str, str]],
    ohlcv_rows: list[dict[str, str]],
    generated_at: str | None = None,
) -> dict[str, Any]:
    rows = build_queue_rows(
        residual_rows=residual_rows,
        masterfile_gap_rows=masterfile_gap_rows,
        listing_status_rows=listing_status_rows,
        ohlcv_rows=ohlcv_rows,
    )
    summary = summarize(rows, generated_at or utc_now_iso())
    return {
        "_meta": {
            "generated_at": summary["generated_at"],
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


def render_markdown(payload: dict[str, Any]) -> str:
    summary = payload["summary"]
    rows = payload["rows"]
    lines = [
        "# B3 Core Scope Review Queue",
        "",
        f"Generated at: `{summary['generated_at']}`",
        "",
        "This queue isolates B3 `core_exclusion_candidate` rows that must be decided as core, extended, or exclude before identifier or category work. It does not apply data changes.",
        "",
        "## Summary",
        "",
        f"- Scope review rows: `{summary['rows']}`",
        "",
        "## Scope Review Queues",
        "",
        "| Queue | Rows |",
        "|---|---:|",
    ]
    for queue, count in summary["scope_review_queue_totals"].items():
        lines.append(f"| {queue} | {count} |")
    lines.extend(["", "## Gap Classes", "", "| Gap class | Rows |", "|---|---:|"])
    for gap_class, count in summary["gap_class_totals"].items():
        lines.append(f"| {gap_class} | {count} |")
    lines.extend(["", "## Evidence Status", "", "| Signal | Value | Rows |", "|---|---|---:|"])
    for value, count in summary["masterfile_source_presence_totals"].items():
        lines.append(f"| masterfile_source_presence | {value} | {count} |")
    for value, count in summary["listing_history_status_totals"].items():
        lines.append(f"| listing_history_status | {value} | {count} |")
    for value, count in summary["ohlcv_plausibility_status_totals"].items():
        lines.append(f"| ohlcv_plausibility_status | {value} | {count} |")
    lines.extend(
        [
            "",
            "## Rows",
            "",
            "| Listing key | Queue | Asset type | Gap class | Name | Evidence required | Source gate |",
            "|---|---|---|---|---|---|---|",
        ]
    )
    for row in rows:
        lines.append(
            f"| {row['listing_key']} | {row['scope_review_queue']} | {row['asset_type']} | {row['gap_class']} | "
            f"{row['name']} | {row['verification_evidence_required']} | {row['source_gate']} |"
        )
    lines.extend(
        [
            "",
            "## Policy",
            "",
            "- Do not fill B3 ISINs, categories, names, symbols, listing status, or scope from this queue alone.",
            "- Close a row only with exact listing-keyed official evidence and an explicit scope decision.",
            "",
        ]
    )
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build B3 scope review queue for core exclusion candidates.")
    parser.add_argument("--b3-residual-isin-csv", type=Path, default=DEFAULT_B3_RESIDUAL_ISIN_CSV)
    parser.add_argument("--b3-masterfile-gap-csv", type=Path, default=DEFAULT_B3_MASTERFILE_GAP_CSV)
    parser.add_argument("--listing-status-history-csv", type=Path, default=DEFAULT_LISTING_STATUS_HISTORY_CSV)
    parser.add_argument("--ohlcv-plausibility-csv", type=Path, default=DEFAULT_OHLCV_PLAUSIBILITY_CSV)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_CSV_OUT)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_JSON_OUT)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_MD_OUT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    payload = build_payload(
        residual_rows=load_csv(args.b3_residual_isin_csv),
        masterfile_gap_rows=load_csv(args.b3_masterfile_gap_csv),
        listing_status_rows=load_csv(args.listing_status_history_csv),
        ohlcv_rows=load_csv(args.ohlcv_plausibility_csv),
    )
    payload["_meta"]["source_files"] = {
        "b3_residual_isin_csv": display_path(args.b3_residual_isin_csv),
        "b3_masterfile_gap_csv": display_path(args.b3_masterfile_gap_csv),
        "listing_status_history_csv": display_path(args.listing_status_history_csv),
        "ohlcv_plausibility_csv": display_path(args.ohlcv_plausibility_csv),
    }
    write_csv(args.csv_out, payload["rows"])
    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.json_out.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    args.md_out.parent.mkdir(parents=True, exist_ok=True)
    args.md_out.write_text(render_markdown(payload), encoding="utf-8")
    print(
        json.dumps(
            {
                "rows": payload["summary"]["rows"],
                "csv_out": display_path(args.csv_out),
                "json_out": display_path(args.json_out),
                "md_out": display_path(args.md_out),
                "scope_review_queue_totals": payload["summary"]["scope_review_queue_totals"],
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()

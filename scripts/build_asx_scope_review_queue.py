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
HISTORY_DIR = DATA_DIR / "history"

DEFAULT_ASX_RESIDUAL_REVIEW_CSV = REPORTS_DIR / "asx_residual_review.csv"
DEFAULT_LISTING_STATUS_HISTORY_CSV = HISTORY_DIR / "listing_status_history.csv"
DEFAULT_INSTRUMENT_SCOPES_CSV = DATA_DIR / "instrument_scopes.csv"
DEFAULT_OHLCV_PLAUSIBILITY_CSV = REPORTS_DIR / "ohlcv_plausibility.csv"
DEFAULT_CSV_OUT = REPORTS_DIR / "asx_scope_review_queue.csv"
DEFAULT_JSON_OUT = REPORTS_DIR / "asx_scope_review_queue.json"
DEFAULT_MD_OUT = REPORTS_DIR / "asx_scope_review_queue.md"

CSV_FIELDNAMES = [
    "listing_key",
    "ticker",
    "exchange",
    "asset_type",
    "name",
    "field",
    "gap_class",
    "official_masterfile_sources",
    "official_masterfile_match",
    "official_masterfile_exposes_isin",
    "official_masterfile_exposes_sector",
    "asx_isin_probe_decision",
    "current_instrument_scope",
    "current_scope_reason",
    "listing_history_status",
    "listing_history_last_observed_at",
    "ohlcv_plausibility_status",
    "ohlcv_review_bucket",
    "asx_resolution_queue",
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


def listing_key(row: dict[str, str]) -> str:
    return row.get("listing_key") or f"{row.get('exchange', '')}::{row.get('ticker', '')}"


def build_lookup(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    return {listing_key(row): row for row in rows if listing_key(row) != "::"}


def select_scope_candidates(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    return [
        row
        for row in rows
        if row.get("asx_resolution_queue") == "core_exclusion_candidate_identifier_scope_review"
        and row.get("source_of_truth_outcome") == "core_exclusion_candidate"
    ]


def scope_review_queue_for(row: dict[str, str]) -> str:
    gap_class = row.get("gap_class", "")
    if gap_class == "debt_or_securitized_identifier_gap":
        return "asx_debt_or_securitized_scope_review"
    if gap_class == "fund_or_trust_identifier_gap":
        return "asx_fund_or_trust_scope_review"
    if gap_class == "inactive_or_legacy_identifier_gap":
        return "asx_inactive_or_legacy_scope_review"
    return "asx_core_scope_review"


def verification_evidence_for(queue: str) -> str:
    if queue == "asx_debt_or_securitized_scope_review":
        return "official_asx_registry_issuer_trustee_or_prospectus_evidence_plus_scope_decision"
    if queue == "asx_fund_or_trust_scope_review":
        return "official_asx_investment_product_issuer_pds_or_registry_evidence_plus_scope_decision"
    if queue == "asx_inactive_or_legacy_scope_review":
        return "current_active_or_inactive_official_asx_listing_evidence_plus_scope_decision"
    return "official_asx_listing_scope_decision_for_core_extended_or_exclude"


def recommended_next_source_for(queue: str, official_sources: str) -> str:
    source_label = official_sources or "current official ASX, registry, or issuer source"
    if "|" in source_label:
        source_label = "multiple official ASX sources"
    if queue == "asx_debt_or_securitized_scope_review":
        return f"{source_label}, trustee page, prospectus, registry, or official product evidence for the exact listing."
    if queue == "asx_fund_or_trust_scope_review":
        return f"{source_label}, issuer/sponsor product page, PDS, or registry evidence for the exact listing."
    if queue == "asx_inactive_or_legacy_scope_review":
        return f"{source_label}, issuer notice, ASX announcement, or official inactive/delisting evidence."
    return f"{source_label} plus reviewed scope decision for core, extended, or exclude."


def source_gate_for(queue: str) -> str:
    if queue == "asx_debt_or_securitized_scope_review":
        return "No ISIN, category, name, symbol, or scope change until exact debt/securitized product evidence is reviewed."
    if queue == "asx_fund_or_trust_scope_review":
        return "No ISIN, category, name, symbol, or scope change until exact fund/trust product evidence is reviewed."
    if queue == "asx_inactive_or_legacy_scope_review":
        return "Do not delete, rename, extend, or exclude until current active or inactive status is proven by official evidence."
    return "No identifier, category, metadata, or scope change until exact listing-keyed official evidence is reviewed."


def recommended_scope_action_for(queue: str) -> str:
    if queue == "asx_debt_or_securitized_scope_review":
        return "review_debt_or_securitized_product_then_choose_core_extended_or_exclude"
    if queue == "asx_fund_or_trust_scope_review":
        return "review_product_currentness_then_choose_core_extended_or_exclude"
    if queue == "asx_inactive_or_legacy_scope_review":
        return "review_active_or_inactive_status_then_choose_core_extended_or_exclude"
    return "review_scope_then_keep_metadata_blank_until_official_evidence"


def review_context_for(row: dict[str, str]) -> str:
    return (
        f"gap_class={row.get('gap_class', '') or 'none'};"
        f"official_masterfile_sources={row.get('official_masterfile_sources', '') or 'none'};"
        f"asx_isin_probe_decision={row.get('asx_isin_probe_decision', '') or 'none'};"
        f"current_scope={row.get('current_instrument_scope', '') or 'none'};"
        f"listing_history_status={row.get('listing_history_status', '') or 'none'};"
        f"ohlcv_plausibility_status={row.get('ohlcv_plausibility_status', '') or 'none'};"
        f"scope_decision_gate={row.get('scope_decision_gate', '') or 'none'}"
    )


def build_queue_rows(
    *,
    asx_residual_rows: list[dict[str, str]],
    listing_status_rows: list[dict[str, str]],
    instrument_scope_rows: list[dict[str, str]],
    ohlcv_rows: list[dict[str, str]],
) -> list[dict[str, str]]:
    status_lookup = build_lookup(listing_status_rows)
    scope_lookup = build_lookup(instrument_scope_rows)
    ohlcv_lookup = build_lookup(ohlcv_rows)
    rows: list[dict[str, str]] = []
    for residual in select_scope_candidates(asx_residual_rows):
        key = residual.get("listing_key", "")
        scope_review_queue = scope_review_queue_for(residual)
        status = status_lookup.get(key, {})
        scope = scope_lookup.get(key, {})
        ohlcv = ohlcv_lookup.get(key, {})
        row = {
            "listing_key": key,
            "ticker": residual.get("ticker", ""),
            "exchange": residual.get("exchange", ""),
            "asset_type": residual.get("asset_type", ""),
            "name": residual.get("name", ""),
            "field": residual.get("field", ""),
            "gap_class": residual.get("gap_class", ""),
            "official_masterfile_sources": residual.get("official_masterfile_sources", ""),
            "official_masterfile_match": residual.get("official_masterfile_match", ""),
            "official_masterfile_exposes_isin": residual.get("official_masterfile_exposes_isin", ""),
            "official_masterfile_exposes_sector": residual.get("official_masterfile_exposes_sector", ""),
            "asx_isin_probe_decision": residual.get("asx_isin_probe_decision", ""),
            "current_instrument_scope": scope.get("instrument_scope", ""),
            "current_scope_reason": scope.get("scope_reason", ""),
            "listing_history_status": status.get("status", ""),
            "listing_history_last_observed_at": status.get("last_observed_at", ""),
            "ohlcv_plausibility_status": ohlcv.get("plausibility_status", ""),
            "ohlcv_review_bucket": ohlcv.get("review_bucket", ""),
            "asx_resolution_queue": residual.get("asx_resolution_queue", ""),
            "scope_review_queue": scope_review_queue,
            "scope_decision_gate": "decide_core_extended_or_exclude_before_asx_identifier_or_category_enrichment",
            "recommended_scope_action": recommended_scope_action_for(scope_review_queue),
            "verification_evidence_required": verification_evidence_for(scope_review_queue),
            "recommended_next_source": recommended_next_source_for(
                scope_review_queue, residual.get("official_masterfile_sources", "")
            ),
            "source_gate": source_gate_for(scope_review_queue),
            "review_context": "",
        }
        row["review_context"] = review_context_for(row)
        rows.append(row)
    return sorted(rows, key=lambda row: (row["scope_review_queue"], row["ticker"], row["listing_key"]))


def summarize(rows: list[dict[str, str]], generated_at: str) -> dict[str, Any]:
    return {
        "generated_at": generated_at,
        "rows": len(rows),
        "scope_review_queue_totals": dict(sorted(Counter(row["scope_review_queue"] for row in rows).items())),
        "asset_type_totals": dict(sorted(Counter(row["asset_type"] for row in rows).items())),
        "gap_class_totals": dict(sorted(Counter(row["gap_class"] for row in rows).items())),
        "official_source_totals": dict(
            sorted(
                Counter(
                    source
                    for row in rows
                    for source in (
                        row["official_masterfile_sources"].split("|")
                        if row["official_masterfile_sources"]
                        else ["none"]
                    )
                ).items()
            )
        ),
        "current_scope_reason_totals": dict(
            sorted(Counter(row["current_scope_reason"] or "missing_scope_reason" for row in rows).items())
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
        "top_scope_review_batches": [
            {
                "scope_review_queue": queue,
                "official_source": official_source,
                "rows": count,
                "verification_evidence_required": verification_evidence_for(queue),
                "recommended_next_source": recommended_next_source_for(queue, official_source if official_source != "none" else ""),
                "source_gate": source_gate_for(queue),
            }
            for (queue, official_source), count in sorted(
                Counter(
                    (
                        row["scope_review_queue"],
                        source,
                    )
                    for row in rows
                    for source in (
                        row["official_masterfile_sources"].split("|")
                        if row["official_masterfile_sources"]
                        else ["none"]
                    )
                ).items(),
                key=lambda item: (-item[1], item[0][0], item[0][1]),
            )[:20]
        ],
        "policy": {
            "scope_first": "ASX core_exclusion_candidate rows must be resolved as core, extended, or exclude before identifier or category enrichment.",
            "no_data_apply": "This queue does not authorize ISIN, ETF-category, sector, name, symbol, listing-status, or scope changes.",
            "source_gate": "Only exact listing-keyed official ASX, registry, issuer, trustee, prospectus, or reviewed stronger evidence can close a row.",
        },
    }


def build_payload(
    *,
    asx_residual_rows: list[dict[str, str]],
    listing_status_rows: list[dict[str, str]],
    instrument_scope_rows: list[dict[str, str]],
    ohlcv_rows: list[dict[str, str]],
    generated_at: str | None = None,
) -> dict[str, Any]:
    rows = build_queue_rows(
        asx_residual_rows=asx_residual_rows,
        listing_status_rows=listing_status_rows,
        instrument_scope_rows=instrument_scope_rows,
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
        "# ASX Scope Review Queue",
        "",
        f"Generated at: `{summary['generated_at']}`",
        "",
        "This queue isolates ASX `core_exclusion_candidate` rows that must be decided as core, extended, or exclude before ASX ISIN or ETF-category work. It does not apply data changes.",
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
    lines.extend(["", "## Evidence Status", "", "| Signal | Value | Rows |", "|---|---|---:|"])
    for value, count in summary["listing_history_status_totals"].items():
        lines.append(f"| listing_history_status | {value} | {count} |")
    for value, count in summary["current_scope_reason_totals"].items():
        lines.append(f"| current_scope_reason | {value} | {count} |")
    for value, count in summary["ohlcv_plausibility_status_totals"].items():
        lines.append(f"| ohlcv_plausibility_status | {value} | {count} |")
    lines.extend(
        [
            "",
            "## Top Scope Review Batches",
            "",
            "| Queue | Official source | Rows | Evidence required | Recommended next source | Source gate |",
            "|---|---|---:|---|---|---|",
        ]
    )
    for batch in summary["top_scope_review_batches"]:
        lines.append(
            f"| {batch['scope_review_queue']} | {batch['official_source']} | {batch['rows']} | "
            f"{batch['verification_evidence_required']} | {batch['recommended_next_source']} | {batch['source_gate']} |"
        )
    lines.extend(
        [
            "",
            "## Rows",
            "",
            "| Listing key | Queue | Asset type | Gap class | Name | Source gate |",
            "|---|---|---|---|---|---|",
        ]
    )
    for row in rows:
        lines.append(
            f"| {row['listing_key']} | {row['scope_review_queue']} | {row['asset_type']} | "
            f"{row['gap_class']} | {row['name']} | {row['source_gate']} |"
        )
    lines.extend(
        [
            "",
            "## Policy",
            "",
            "- Do not fill ASX ISINs, ETF categories, sectors, names, symbols, listing status, or scope from this queue alone.",
            "- Close a row only with exact listing-keyed official evidence and an explicit scope decision.",
            "",
        ]
    )
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build ASX scope review queue from ASX residual review rows.")
    parser.add_argument("--asx-residual-review-csv", type=Path, default=DEFAULT_ASX_RESIDUAL_REVIEW_CSV)
    parser.add_argument("--listing-status-history-csv", type=Path, default=DEFAULT_LISTING_STATUS_HISTORY_CSV)
    parser.add_argument("--instrument-scopes-csv", type=Path, default=DEFAULT_INSTRUMENT_SCOPES_CSV)
    parser.add_argument("--ohlcv-plausibility-csv", type=Path, default=DEFAULT_OHLCV_PLAUSIBILITY_CSV)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_CSV_OUT)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_JSON_OUT)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_MD_OUT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    payload = build_payload(
        asx_residual_rows=load_csv(args.asx_residual_review_csv),
        listing_status_rows=load_csv(args.listing_status_history_csv),
        instrument_scope_rows=load_csv(args.instrument_scopes_csv),
        ohlcv_rows=load_csv(args.ohlcv_plausibility_csv),
    )
    payload["_meta"]["source_files"] = {
        "asx_residual_review_csv": display_path(args.asx_residual_review_csv),
        "listing_status_history_csv": display_path(args.listing_status_history_csv),
        "instrument_scopes_csv": display_path(args.instrument_scopes_csv),
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

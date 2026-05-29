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
DATA_DIR = ROOT / "data"

DEFAULT_CANADA_RESIDUAL_REVIEW_CSV = REPORTS_DIR / "canada_residual_review.csv"
DEFAULT_LISTING_STATUS_HISTORY_CSV = HISTORY_DIR / "listing_status_history.csv"
DEFAULT_INSTRUMENT_SCOPES_CSV = DATA_DIR / "instrument_scopes.csv"
DEFAULT_OHLCV_PLAUSIBILITY_CSV = REPORTS_DIR / "ohlcv_plausibility.csv"
DEFAULT_CSV_OUT = REPORTS_DIR / "canada_scope_review_queue.csv"
DEFAULT_JSON_OUT = REPORTS_DIR / "canada_scope_review_queue.json"
DEFAULT_MD_OUT = REPORTS_DIR / "canada_scope_review_queue.md"

SCOPE_REVIEW_QUEUES = {
    "core_exclusion_candidate_identifier_scope_review",
    "core_exclusion_candidate_metadata_scope_review",
    "core_exclusion_candidate_scope_review",
}

CSV_FIELDNAMES = [
    "listing_key",
    "ticker",
    "exchange",
    "asset_type",
    "name",
    "isin",
    "figi",
    "source_gap_fields",
    "source_gap_classes",
    "official_masterfile_sources",
    "official_masterfile_match",
    "official_masterfile_exposes_isin",
    "official_masterfile_exposes_sector",
    "current_instrument_scope",
    "current_scope_reason",
    "listing_history_status",
    "listing_history_last_observed_at",
    "ohlcv_plausibility_status",
    "ohlcv_review_bucket",
    "canada_resolution_queue",
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


def normalized_listing_keys(row: dict[str, str]) -> list[str]:
    key = row.get("listing_key", "")
    exchange = row.get("exchange", "")
    ticker = row.get("ticker", "")
    keys = [key] if key else []
    if exchange and ticker:
        keys.append(f"{exchange}::{ticker}")
        if "." in ticker:
            keys.append(f"{exchange}::{ticker.replace('.', '-')}")
        if "-" in ticker:
            keys.append(f"{exchange}::{ticker.replace('-', '.')}")
    return [value for index, value in enumerate(keys) if value and value not in keys[:index]]


def build_lookup(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    lookup: dict[str, dict[str, str]] = {}
    for row in rows:
        for key in normalized_listing_keys(row):
            lookup.setdefault(key, row)
    return lookup


def select_scope_candidates(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    return [
        row
        for row in rows
        if row.get("canada_resolution_queue") in SCOPE_REVIEW_QUEUES
        and "core_exclusion_candidate" in (row.get("source_of_truth_outcomes", "").split("|"))
    ]


def scope_review_queue_for(row: dict[str, str]) -> str:
    queue = row.get("canada_resolution_queue", "")
    gap_classes = set(row.get("source_gap_classes", "").split("|")) if row.get("source_gap_classes") else set()
    if queue == "core_exclusion_candidate_metadata_scope_review":
        return "canada_metadata_scope_review_before_sector_or_category_fill"
    if gap_classes & {"adr_cdr_or_depositary_identifier_gap", "adr_cdr_or_depositary_sector_gap"}:
        return "canada_depositary_or_cdr_scope_review"
    if gap_classes & {"capital_pool_or_halted_identifier_gap", "shell_or_cpc_sector_gap"}:
        return "canada_cpc_shell_or_halted_scope_review"
    if gap_classes & {"fund_or_trust_identifier_gap", "fundlike_stock_sector_gap", "official_product_taxonomy_unavailable_gap"}:
        return "canada_fund_or_trust_scope_review"
    if gap_classes & {"debt_or_securitized_identifier_gap"}:
        return "canada_debt_or_securitized_scope_review"
    if gap_classes & {"inactive_or_legacy_identifier_gap"}:
        return "canada_inactive_or_legacy_scope_review"
    return "canada_core_scope_review"


def verification_evidence_for(scope_review_queue: str) -> str:
    if scope_review_queue == "canada_metadata_scope_review_before_sector_or_category_fill":
        return "official_listing_scope_decision_before_sector_or_category_fill"
    if scope_review_queue == "canada_depositary_or_cdr_scope_review":
        return "official_cdr_depositary_or_exchange_listing_evidence_plus_core_extended_or_exclude_scope_decision"
    if scope_review_queue == "canada_cpc_shell_or_halted_scope_review":
        return "official_tmx_or_issuer_status_evidence_plus_core_extended_or_exclude_scope_decision"
    if scope_review_queue == "canada_fund_or_trust_scope_review":
        return "official_tmx_cboe_fund_issuer_or_prospectus_evidence_plus_scope_decision"
    if scope_review_queue == "canada_debt_or_securitized_scope_review":
        return "official_exchange_or_product_registry_evidence_plus_scope_decision"
    if scope_review_queue == "canada_inactive_or_legacy_scope_review":
        return "current_active_or_inactive_official_listing_evidence_plus_scope_decision"
    return "official_listing_scope_decision_for_core_extended_or_exclude"


def recommended_next_source_for(scope_review_queue: str, official_sources: str) -> str:
    source_label = official_sources or "current official exchange or issuer source"
    if "|" in source_label:
        source_label = "multiple official Canada sources"
    if scope_review_queue == "canada_metadata_scope_review_before_sector_or_category_fill":
        return f"{source_label} plus reviewed scope decision before any sector or ETF-category work."
    if scope_review_queue == "canada_depositary_or_cdr_scope_review":
        return f"{source_label}, CDR/depositary sponsor page, issuer page, or prospectus evidence for the exact listing."
    if scope_review_queue == "canada_cpc_shell_or_halted_scope_review":
        return f"{source_label}, TMX status page, issuer filing, or halt/CPC classification evidence for the exact listing."
    if scope_review_queue == "canada_fund_or_trust_scope_review":
        return f"{source_label}, fund issuer page, prospectus, or product registry evidence for the exact listing."
    if scope_review_queue == "canada_debt_or_securitized_scope_review":
        return f"{source_label}, prospectus, CSD/security registry, or product registry evidence for the exact listing."
    if scope_review_queue == "canada_inactive_or_legacy_scope_review":
        return f"{source_label}, issuer notice, exchange bulletin, or official inactive/delisting evidence."
    return f"{source_label} plus reviewed scope decision for core, extended, or exclude."


def source_gate_for(scope_review_queue: str) -> str:
    if scope_review_queue == "canada_metadata_scope_review_before_sector_or_category_fill":
        return "No sector or category fill until scope is decided with official listing evidence."
    if scope_review_queue == "canada_depositary_or_cdr_scope_review":
        return "No ISIN, FIGI, sector, or scope change until the exact CDR/depositary listing is proven by official evidence."
    if scope_review_queue == "canada_cpc_shell_or_halted_scope_review":
        return "No ISIN, FIGI, sector, or scope change until current CPC, shell, halt, active, or inactive status is proven."
    if scope_review_queue == "canada_fund_or_trust_scope_review":
        return "No ISIN, FIGI, category, or scope change until the exact fund/trust product is proven by official evidence."
    if scope_review_queue == "canada_debt_or_securitized_scope_review":
        return "No ISIN, FIGI, category, or scope change until exact official product evidence is reviewed."
    if scope_review_queue == "canada_inactive_or_legacy_scope_review":
        return "Do not delete, rename, extend, or exclude until current active or inactive status is proven by official evidence."
    return "No identifier, metadata, or scope change until exact listing-keyed official evidence is reviewed."


def recommended_scope_action_for(scope_review_queue: str) -> str:
    if scope_review_queue == "canada_metadata_scope_review_before_sector_or_category_fill":
        return "decide_scope_before_sector_or_category_enrichment"
    if scope_review_queue == "canada_depositary_or_cdr_scope_review":
        return "review_cdr_or_depositary_scope_then_choose_core_extended_or_exclude"
    if scope_review_queue == "canada_cpc_shell_or_halted_scope_review":
        return "review_cpc_shell_or_halt_status_then_choose_core_extended_or_exclude"
    if scope_review_queue == "canada_fund_or_trust_scope_review":
        return "review_product_currentness_then_choose_core_extended_or_exclude"
    if scope_review_queue == "canada_debt_or_securitized_scope_review":
        return "review_product_registry_then_choose_core_extended_or_exclude"
    if scope_review_queue == "canada_inactive_or_legacy_scope_review":
        return "review_active_or_inactive_status_then_choose_core_extended_or_exclude"
    return "review_scope_then_keep_metadata_blank_until_official_evidence"


def review_context_for(row: dict[str, str]) -> str:
    return (
        f"source_gap_classes={row.get('source_gap_classes', '') or 'none'};"
        f"official_masterfile_sources={row.get('official_masterfile_sources', '') or 'none'};"
        f"official_masterfile_exposes_isin={row.get('official_masterfile_exposes_isin', '') or 'none'};"
        f"current_scope={row.get('current_instrument_scope', '') or 'none'};"
        f"listing_history_status={row.get('listing_history_status', '') or 'none'};"
        f"ohlcv_plausibility_status={row.get('ohlcv_plausibility_status', '') or 'none'};"
        f"scope_decision_gate={row.get('scope_decision_gate', '') or 'none'}"
    )


def build_queue_rows(
    *,
    canada_residual_rows: list[dict[str, str]],
    listing_status_rows: list[dict[str, str]],
    instrument_scope_rows: list[dict[str, str]],
    ohlcv_rows: list[dict[str, str]],
) -> list[dict[str, str]]:
    status_lookup = build_lookup(listing_status_rows)
    scope_lookup = build_lookup(instrument_scope_rows)
    ohlcv_lookup = build_lookup(ohlcv_rows)
    rows: list[dict[str, str]] = []
    for residual in select_scope_candidates(canada_residual_rows):
        key = residual.get("listing_key", "")
        status = next((status_lookup[candidate] for candidate in normalized_listing_keys(residual) if candidate in status_lookup), {})
        scope = next((scope_lookup[candidate] for candidate in normalized_listing_keys(residual) if candidate in scope_lookup), {})
        ohlcv = next((ohlcv_lookup[candidate] for candidate in normalized_listing_keys(residual) if candidate in ohlcv_lookup), {})
        scope_review_queue = scope_review_queue_for(residual)
        row = {
            "listing_key": key,
            "ticker": residual.get("ticker", ""),
            "exchange": residual.get("exchange", ""),
            "asset_type": residual.get("asset_type", ""),
            "name": residual.get("name", ""),
            "isin": residual.get("isin", ""),
            "figi": residual.get("figi", ""),
            "source_gap_fields": residual.get("source_gap_fields", ""),
            "source_gap_classes": residual.get("source_gap_classes", ""),
            "official_masterfile_sources": residual.get("official_masterfile_sources", ""),
            "official_masterfile_match": residual.get("official_masterfile_match", ""),
            "official_masterfile_exposes_isin": residual.get("official_masterfile_exposes_isin", ""),
            "official_masterfile_exposes_sector": residual.get("official_masterfile_exposes_sector", ""),
            "current_instrument_scope": scope.get("instrument_scope", ""),
            "current_scope_reason": scope.get("scope_reason", ""),
            "listing_history_status": status.get("status", ""),
            "listing_history_last_observed_at": status.get("last_observed_at", ""),
            "ohlcv_plausibility_status": ohlcv.get("plausibility_status", ""),
            "ohlcv_review_bucket": ohlcv.get("review_bucket", ""),
            "canada_resolution_queue": residual.get("canada_resolution_queue", ""),
            "scope_review_queue": scope_review_queue,
            "scope_decision_gate": "decide_core_extended_or_exclude_before_canada_identifier_or_metadata_enrichment",
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
    return sorted(rows, key=lambda row: (row["scope_review_queue"], row["exchange"], row["ticker"], row["listing_key"]))


def summarize(rows: list[dict[str, str]], generated_at: str) -> dict[str, Any]:
    return {
        "generated_at": generated_at,
        "rows": len(rows),
        "scope_review_queue_totals": dict(sorted(Counter(row["scope_review_queue"] for row in rows).items())),
        "exchange_totals": dict(sorted(Counter(row["exchange"] for row in rows).items())),
        "asset_type_totals": dict(sorted(Counter(row["asset_type"] for row in rows).items())),
        "source_gap_class_totals": dict(
            sorted(
                Counter(
                    gap_class
                    for row in rows
                    for gap_class in (row["source_gap_classes"].split("|") if row["source_gap_classes"] else ["none"])
                ).items()
            )
        ),
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
                "exchange": exchange,
                "official_source": official_source,
                "rows": count,
                "verification_evidence_required": verification_evidence_for(queue),
                "recommended_next_source": recommended_next_source_for(queue, official_source if official_source != "none" else ""),
                "source_gate": source_gate_for(queue),
            }
            for (queue, exchange, official_source), count in sorted(
                Counter(
                    (
                        row["scope_review_queue"],
                        row["exchange"],
                        source,
                    )
                    for row in rows
                    for source in (
                        row["official_masterfile_sources"].split("|")
                        if row["official_masterfile_sources"]
                        else ["none"]
                    )
                ).items(),
                key=lambda item: (-item[1], item[0][0], item[0][1], item[0][2]),
            )[:20]
        ],
        "policy": {
            "scope_first": "Canada core_exclusion_candidate rows must be resolved as core, extended, or exclude before identifier or metadata enrichment.",
            "no_data_apply": "This queue does not authorize ISIN, FIGI, sector, ETF-category, name, symbol, listing-status, or scope changes.",
            "source_gate": "Only exact listing-keyed official TMX/Cboe Canada, issuer, CSD, prospectus, registry, or reviewed stronger evidence can close a row.",
        },
    }


def build_payload(
    *,
    canada_residual_rows: list[dict[str, str]],
    listing_status_rows: list[dict[str, str]],
    instrument_scope_rows: list[dict[str, str]],
    ohlcv_rows: list[dict[str, str]],
    generated_at: str | None = None,
) -> dict[str, Any]:
    rows = build_queue_rows(
        canada_residual_rows=canada_residual_rows,
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

    def cell(value: str) -> str:
        return str(value).replace("|", "\\|")

    lines = [
        "# Canada Scope Review Queue",
        "",
        f"Generated at: `{summary['generated_at']}`",
        "",
        "This queue isolates TSX/TSXV/NEO `core_exclusion_candidate` rows that must be decided as core, extended, or exclude before Canada ISIN, FIGI, sector, or ETF-category work. It does not apply data changes.",
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
    lines.extend(["", "## Exchanges", "", "| Exchange | Rows |", "|---|---:|"])
    for exchange, count in summary["exchange_totals"].items():
        lines.append(f"| {exchange} | {count} |")
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
            "| Queue | Exchange | Official source | Rows | Evidence required | Recommended next source | Source gate |",
            "|---|---|---|---:|---|---|---|",
        ]
    )
    for batch in summary["top_scope_review_batches"]:
        lines.append(
            f"| {batch['scope_review_queue']} | {batch['exchange']} | {batch['official_source']} | "
            f"{batch['rows']} | {batch['verification_evidence_required']} | "
            f"{batch['recommended_next_source']} | {batch['source_gate']} |"
        )
    lines.extend(
        [
            "",
            "## Rows",
            "",
            "| Listing key | Queue | Exchange | Asset type | Name | Gap classes | Source gate |",
            "|---|---|---|---|---|---|---|",
        ]
    )
    for row in rows:
        lines.append(
            f"| {cell(row['listing_key'])} | {cell(row['scope_review_queue'])} | {cell(row['exchange'])} | "
            f"{cell(row['asset_type'])} | {cell(row['name'])} | {cell(row['source_gap_classes'] or 'none')} | "
            f"{cell(row['source_gate'])} |"
        )
    lines.extend(
        [
            "",
            "## Policy",
            "",
            "- Do not fill Canada ISINs, FIGIs, sectors, ETF categories, names, symbols, listing status, or scope from this queue alone.",
            "- Close a row only with exact listing-keyed official evidence and an explicit scope decision.",
            "",
        ]
    )
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Canada scope review queue from residual review rows.")
    parser.add_argument("--canada-residual-review-csv", type=Path, default=DEFAULT_CANADA_RESIDUAL_REVIEW_CSV)
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
        canada_residual_rows=load_csv(args.canada_residual_review_csv),
        listing_status_rows=load_csv(args.listing_status_history_csv),
        instrument_scope_rows=load_csv(args.instrument_scopes_csv),
        ohlcv_rows=load_csv(args.ohlcv_plausibility_csv),
    )
    payload["_meta"]["source_files"] = {
        "canada_residual_review_csv": display_path(args.canada_residual_review_csv),
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

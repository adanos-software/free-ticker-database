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

DEFAULT_JSON_OUT = REPORTS_DIR / "improvement_baseline.json"
DEFAULT_MD_OUT = REPORTS_DIR / "improvement_baseline.md"

BASELINE_SOURCE_FILES = {
    "coverage_report": "data/reports/coverage_report.json",
    "source_gap_classification_json": "data/reports/source_gap_classification.json",
    "source_gap_classification_csv": "data/reports/source_gap_classification.csv",
    "entry_quality_json": "data/reports/entry_quality.json",
    "entry_quality_csv": "data/reports/entry_quality.csv",
    "validation_report": "data/reports/validation_report.json",
    "b3_residual_isin_review": "data/reports/b3_residual_isin_review.json",
    "b3_residual_sector_review": "data/reports/b3_residual_sector_review.json",
    "otc_scope_review": "data/reports/otc_scope_review.json",
    "canada_residual_review": "data/reports/canada_residual_review.json",
    "canada_figi_queue": "data/reports/canada_figi_queue.json",
    "asx_residual_review": "data/reports/asx_residual_review.json",
    "weak_sector_residual_review": "data/reports/weak_sector_residual_review.json",
    "masterfile_collision_review": "data/reports/masterfile_collision_review.json",
    "symbol_changes_review": "data/reports/symbol_changes_review.json",
    "ohlcv_plausibility": "data/reports/ohlcv_plausibility.json",
    "ohlcv_warning_review": "data/reports/ohlcv_warning_review.json",
    "financialdata_isin_supplements_review": "data/reports/financialdata_isin_supplements_review.json",
}


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def load_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def summary(payload: dict[str, Any]) -> dict[str, Any]:
    value = payload.get("summary", {})
    return value if isinstance(value, dict) else {}


def row_count(payload: dict[str, Any]) -> int:
    for container in (summary(payload), payload.get("_meta", {}), payload):
        if not isinstance(container, dict):
            continue
        for key in ("review_rows", "rows", "selected_rows", "review_items", "items"):
            value = container.get(key)
            if isinstance(value, list):
                return len(value)
            if value in {"", None}:
                continue
            try:
                return int(value)
            except (TypeError, ValueError):
                continue
    for key in ("rows", "review_items", "items"):
        rows = payload.get(key)
        if isinstance(rows, list):
            return len(rows)
    return 0


def source_freshness_totals(coverage_report: dict[str, Any]) -> dict[str, int]:
    totals: dict[str, int] = {}
    for row in coverage_report.get("source_coverage", []):
        status = row.get("freshness_status", "unknown") or "unknown"
        totals[status] = totals.get(status, 0) + 1
    return dict(sorted(totals.items()))


def markdown_cell(value: Any) -> str:
    return str(value if value is not None else "").replace("\n", " ").replace("|", "\\|")


def append_source_files_markdown(lines: list[str], payload: dict[str, Any]) -> None:
    meta = payload.get("_meta", {})
    source_files = meta.get("source_files", {}) if isinstance(meta, dict) else {}
    if not isinstance(source_files, dict):
        source_files = {}
    lines.extend(["", "## Source Files", "", "| Key | Path |", "|---|---|"])
    for key, path in sorted(source_files.items()):
        lines.append(f"| `{markdown_cell(key)}` | `{markdown_cell(path)}` |")


def global_baseline_context(global_baseline: dict[str, Any]) -> str:
    return (
        f"metric_count={len(global_baseline)};"
        f"tickers={global_baseline.get('tickers', 0)};"
        f"listing_keys={global_baseline.get('listing_keys', 0)};"
        f"source_gap_rows={global_baseline.get('source_gap_rows', 0)};"
        f"warn_rows={global_baseline.get('entry_quality_warn_rows', 0)};"
        f"quarantine_rows={global_baseline.get('entry_quality_quarantine_rows', 0)};"
        f"validation_failed_error_gates={global_baseline.get('validation_failed_error_gates', 0)}"
    )


def campaign_baseline_context(campaign_key: str, values: dict[str, Any]) -> str:
    numeric_row_total = sum(
        int(value)
        for key, value in values.items()
        if isinstance(value, int) and not isinstance(value, bool) and ("rows" in key or key.endswith("_count"))
    )
    nested_metric_count = sum(1 for value in values.values() if isinstance(value, (dict, list)))
    return (
        f"campaign_key={campaign_key};"
        f"metric_count={len(values)};"
        f"nested_metric_count={nested_metric_count};"
        f"numeric_row_total={numeric_row_total}"
    )


def exchange_baseline_context(exchange: str, values: dict[str, Any]) -> str:
    return (
        f"exchange={exchange};"
        f"tickers={values.get('tickers', 0)};"
        f"isin_coverage={values.get('isin_coverage', 0)};"
        f"sector_coverage={values.get('sector_coverage', 0)};"
        f"source_gap_rows={values.get('source_gap_rows', 0)};"
        f"warn_rows={values.get('entry_quality_warn_rows', 0)};"
        f"quality_source_gap_rows={values.get('entry_quality_source_gap_rows', 0)};"
        f"quarantine_rows={values.get('entry_quality_quarantine_rows', 0)}"
    )


def baseline_contexts(
    global_baseline: dict[str, Any],
    campaign_baseline: dict[str, dict[str, Any]],
    exchange_rows: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    return {
        "global": global_baseline_context(global_baseline),
        "campaigns": {
            campaign_key: campaign_baseline_context(campaign_key, values)
            for campaign_key, values in campaign_baseline.items()
        },
        "exchanges": {
            exchange: exchange_baseline_context(exchange, values)
            for exchange, values in exchange_rows.items()
        },
    }


def exchange_baseline(
    coverage_report: dict[str, Any],
    entry_quality_rows: list[dict[str, str]],
    source_gap_rows: list[dict[str, str]],
) -> dict[str, dict[str, int]]:
    entry_status_by_exchange: dict[str, Counter[str]] = {}
    for row in entry_quality_rows:
        exchange = row.get("exchange", "")
        status = row.get("quality_status", "")
        if not exchange or not status:
            continue
        entry_status_by_exchange.setdefault(exchange, Counter())[status] += 1

    source_gap_by_exchange = Counter(row.get("exchange", "") for row in source_gap_rows if row.get("exchange"))

    rows: dict[str, dict[str, int]] = {}
    exchanges = {
        row.get("exchange", "")
        for row in coverage_report.get("exchange_coverage", [])
        if row.get("exchange")
    } | set(entry_status_by_exchange) | set(source_gap_by_exchange)
    coverage_by_exchange = {
        row.get("exchange", ""): row
        for row in coverage_report.get("exchange_coverage", [])
        if row.get("exchange")
    }
    for exchange in sorted(exchanges):
        coverage = coverage_by_exchange.get(exchange, {})
        status_counts = entry_status_by_exchange.get(exchange, Counter())
        rows[exchange] = {
            "tickers": int(coverage.get("tickers", 0) or 0),
            "isin_coverage": int(coverage.get("isin_coverage", 0) or 0),
            "sector_coverage": int(coverage.get("sector_coverage", 0) or 0),
            "masterfile_matches": int(coverage.get("masterfile_matches", 0) or 0),
            "masterfile_collisions": int(coverage.get("masterfile_collisions", 0) or 0),
            "masterfile_missing": int(coverage.get("masterfile_missing", 0) or 0),
            "source_gap_rows": int(source_gap_by_exchange.get(exchange, 0)),
            "entry_quality_warn_rows": int(status_counts.get("warn", 0)),
            "entry_quality_source_gap_rows": int(status_counts.get("source_gap", 0)),
            "entry_quality_quarantine_rows": int(status_counts.get("quarantine", 0)),
        }
    return rows


def build_campaign_baseline(payloads: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    coverage = payloads.get("coverage", {})
    source_gap = payloads.get("source_gap", {})
    b3_isin = payloads.get("b3_isin", {})
    b3_sector = payloads.get("b3_sector", {})
    otc_scope = payloads.get("otc_scope", {})
    canada = payloads.get("canada", {})
    canada_figi_queue = payloads.get("canada_figi_queue", {})
    asx = payloads.get("asx", {})
    weak_sector = payloads.get("weak_sector", {})
    collisions = payloads.get("collisions", {})
    symbol_changes = payloads.get("symbol_changes", {})
    ohlcv = payloads.get("ohlcv", {})
    ohlcv_warning = payloads.get("ohlcv_warning", {})
    financialdata = payloads.get("financialdata", {})

    source_freshness_summary = coverage.get("source_freshness_summary", {})
    freshness = coverage.get("freshness", {})
    financialdata_summary = summary(financialdata)

    campaign_baseline = {
        "b3": {
            "missing_isin_residual_rows": row_count(b3_isin),
            "missing_sector_residual_rows": row_count(b3_sector),
        },
        "otc": {
            "scope_review_rows": row_count(otc_scope),
            "accepted_source_gap_rows": summary(otc_scope).get("source_of_truth_outcome_totals", {}).get("accepted_source_gap", 0),
            "drop_override_rows_still_present": summary(otc_scope).get("drop_override_rows_still_present", 0),
        },
        "canada": {
            "residual_rows": row_count(canada),
            "figi_queue_rows": row_count(canada_figi_queue),
            "missing_isin_rows": summary(canada).get("missing_isin_rows", 0),
            "missing_figi_rows": summary(canada).get("missing_figi_rows", 0),
            "reviewed_openfigi_source_gap_rows": sum(
                summary(canada).get("openfigi_review_status_totals", {}).values()
            ),
        },
        "asx": {
            "residual_rows": row_count(asx),
            "field_totals": summary(asx).get("field_totals", {}),
        },
        "weak_sector": {
            "residual_rows": row_count(weak_sector),
            "exchange_totals": summary(weak_sector).get("exchange_totals", {}),
        },
        "masterfile_collisions": {
            "review_rows": row_count(collisions),
            "decision_totals": summary(collisions).get("decision_totals", {}),
            "review_bucket_totals": summary(collisions).get("review_bucket_totals", {}),
            "review_priority_totals": summary(collisions).get("review_priority_totals", {}),
        },
        "symbol_changes": {
            "review_rows": summary(symbol_changes).get("review_rows", 0),
            "exchange_scope_status_counts": summary(symbol_changes).get("exchange_scope_status_counts", {}),
            "review_bucket_counts": summary(symbol_changes).get("review_bucket_counts", {}),
        },
        "ohlcv": {
            "sample_rows": row_count(ohlcv),
            "status_counts": summary(ohlcv).get("status_counts", {}),
            "warning_review_rows": row_count(ohlcv_warning),
            "warning_review_authorization_counts": summary(ohlcv_warning).get(
                "canonical_data_change_authorization_counts", {}
            ),
        },
        "freshness": {
            "source_count": source_freshness_summary.get("source_count", 0),
            "source_freshness_status_totals": source_freshness_summary.get(
                "freshness_status_totals", source_freshness_totals(coverage)
            ),
            "source_refresh_priority_totals": source_freshness_summary.get("refresh_priority_totals", {}),
            "source_refresh_queue_priority_totals": source_freshness_summary.get(
                "refresh_queue_priority_totals", {}
            ),
            "source_refresh_action_totals": source_freshness_summary.get("recommended_refresh_action_totals", {}),
            "old_official_exchange_directory_count": source_freshness_summary.get(
                "old_official_exchange_directory_count", 0
            ),
            "source_gap_rows": row_count(source_gap),
            "source_gap_class_totals": summary(source_gap).get("class_totals", {}),
            "top_source_gap_review_batches": summary(source_gap).get("top_source_gap_review_batches", []),
            "symbol_changes_review_rows": freshness.get("symbol_changes_review_rows", 0),
            "ohlcv_plausibility_rows": freshness.get("ohlcv_plausibility_rows", 0),
            "financialdata_supplement_rows": financialdata_summary.get("supplement_rows", 0),
            "financialdata_apply_eligibility_counts": financialdata_summary.get("apply_eligibility_counts", {}),
            "financialdata_verification_evidence_required_counts": financialdata_summary.get(
                "verification_evidence_required_counts", {}
            ),
        },
    }
    campaign_baseline["baseline"] = {
        "tracked_campaigns": len(campaign_baseline) + 1,
        "global_metric_count": 16,
        "exchange_baseline_enabled": 1,
        "baseline_snapshot_rows": 1,
    }
    return campaign_baseline


def build_payload() -> dict[str, Any]:
    coverage = load_json(REPORTS_DIR / "coverage_report.json")
    source_gap = load_json(REPORTS_DIR / "source_gap_classification.json")
    entry_quality = load_json(REPORTS_DIR / "entry_quality.json")
    validation = load_json(REPORTS_DIR / "validation_report.json")
    b3_isin = load_json(REPORTS_DIR / "b3_residual_isin_review.json")
    b3_sector = load_json(REPORTS_DIR / "b3_residual_sector_review.json")
    otc_scope = load_json(REPORTS_DIR / "otc_scope_review.json")
    canada = load_json(REPORTS_DIR / "canada_residual_review.json")
    canada_figi_queue = load_json(REPORTS_DIR / "canada_figi_queue.json")
    asx = load_json(REPORTS_DIR / "asx_residual_review.json")
    weak_sector = load_json(REPORTS_DIR / "weak_sector_residual_review.json")
    collisions = load_json(REPORTS_DIR / "masterfile_collision_review.json")
    symbol_changes = load_json(REPORTS_DIR / "symbol_changes_review.json")
    ohlcv = load_json(REPORTS_DIR / "ohlcv_plausibility.json")
    ohlcv_warning = load_json(REPORTS_DIR / "ohlcv_warning_review.json")
    financialdata = load_json(REPORTS_DIR / "financialdata_isin_supplements_review.json")
    entry_quality_rows = load_csv(REPORTS_DIR / "entry_quality.csv")
    source_gap_rows = load_csv(REPORTS_DIR / "source_gap_classification.csv")

    coverage_global = coverage.get("global", {})
    entry_summary = summary(entry_quality)
    source_gap_summary = summary(source_gap)
    validation_summary = validation.get("summary", {})

    global_baseline = {
        "tickers": coverage_global.get("tickers", 0),
        "listing_keys": coverage_global.get("listing_keys", 0),
        "isin_coverage": coverage_global.get("isin_coverage", 0),
        "sector_coverage": coverage_global.get("sector_coverage", 0),
        "stock_sector_coverage": coverage_global.get("stock_sector_coverage", 0),
        "etf_category_coverage": coverage_global.get("etf_category_coverage", 0),
        "figi_coverage": coverage_global.get("figi_coverage", 0),
        "official_masterfile_matches": coverage_global.get("official_masterfile_matches", 0),
        "official_masterfile_collisions": coverage_global.get("official_masterfile_collisions", 0),
        "official_masterfile_missing": coverage_global.get("official_masterfile_missing", 0),
        "source_gap_rows": row_count(source_gap),
        "entry_quality_warn_rows": entry_summary.get("status_counts", {}).get("warn", 0),
        "entry_quality_source_gap_rows": entry_summary.get("status_counts", {}).get("source_gap", 0),
        "entry_quality_quarantine_rows": entry_summary.get("status_counts", {}).get("quarantine", 0),
        "validation_failed_error_gates": validation_summary.get("failed_error_gates", 0),
        "source_freshness_status_totals": source_freshness_totals(coverage),
    }
    exchange_rows = exchange_baseline(coverage, entry_quality_rows, source_gap_rows)
    campaign_baseline = build_campaign_baseline(
        {
            "coverage": coverage,
            "source_gap": source_gap,
            "b3_isin": b3_isin,
            "b3_sector": b3_sector,
            "otc_scope": otc_scope,
            "canada": canada,
            "canada_figi_queue": canada_figi_queue,
            "asx": asx,
            "weak_sector": weak_sector,
            "collisions": collisions,
            "symbol_changes": symbol_changes,
            "ohlcv": ohlcv,
            "ohlcv_warning": ohlcv_warning,
            "financialdata": financialdata,
        }
    )
    campaign_baseline["baseline"]["global_metric_count"] = len(global_baseline)
    campaign_baseline["baseline"]["exchange_count"] = len(exchange_rows)
    contexts = baseline_contexts(global_baseline, campaign_baseline, exchange_rows)

    return {
        "_meta": {
            "generated_at": utc_now_iso(),
            "purpose": "Baseline snapshot for future campaign before/after deltas. It is not a data-fill source.",
            "source_files": BASELINE_SOURCE_FILES,
            "source_reports": BASELINE_SOURCE_FILES,
        },
        "summary": {
            "global_metric_count": len(global_baseline),
            "campaign_count": len(campaign_baseline),
            "exchange_count": len(exchange_rows),
            "source_file_count": len(BASELINE_SOURCE_FILES),
            "baseline_context": contexts["global"],
        },
        "global_baseline": global_baseline,
        "campaign_baseline": campaign_baseline,
        "exchange_baseline": exchange_rows,
        "baseline_contexts": contexts,
    }


def render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Improvement Baseline",
        "",
        f"Generated: `{payload['_meta']['generated_at']}`",
        "",
        "Baseline snapshot for future before/after deltas. It does not authorize inferred metadata changes.",
        "",
        "## Summary",
        "",
        "| Metric | Value |",
        "|---|---:|",
    ]
    for key, value in payload.get("summary", {}).items():
        lines.append(f"| {key} | `{json.dumps(value, ensure_ascii=False, sort_keys=True)}` |")
    contexts = payload.get("baseline_contexts", {})
    global_context = contexts.get("global", "") if isinstance(contexts, dict) else ""
    if global_context:
        lines.extend(["", f"Global context: `{markdown_cell(global_context)}`"])
    lines.extend(
        [
            "",
            "## Global",
            "",
            "| Metric | Value |",
            "|---|---:|",
        ]
    )
    for key, value in payload["global_baseline"].items():
        lines.append(f"| {key} | `{json.dumps(value, ensure_ascii=False, sort_keys=True)}` |")
    lines.extend(["", "## Campaign Baseline", ""])
    for campaign, values in payload["campaign_baseline"].items():
        lines.extend([f"### {campaign}", "", "| Metric | Value |", "|---|---:|"])
        campaign_context = (
            contexts.get("campaigns", {}).get(campaign, "") if isinstance(contexts.get("campaigns"), dict) else ""
        )
        if campaign_context:
            lines.append(f"| baseline_context | `{markdown_cell(campaign_context)}` |")
        for key, value in values.items():
            if key == "top_source_gap_review_batches" and isinstance(value, list):
                lines.append(f"| {key} | `{len(value)}` ranked batches |")
                lines.extend(
                    [
                        "",
                        "| Field | Gap Class | Exchange | Rows | Recommended Next Source | Source Gate |",
                        "|---|---|---|---:|---|---|",
                    ]
                )
                for batch in value:
                    if not isinstance(batch, dict):
                        continue
                    lines.append(
                        "| "
                        f"`{markdown_cell(batch.get('field'))}` | "
                        f"`{markdown_cell(batch.get('gap_class'))}` | "
                        f"`{markdown_cell(batch.get('exchange'))}` | "
                        f"{markdown_cell(batch.get('rows'))} | "
                        f"{markdown_cell(batch.get('recommended_next_source'))} | "
                        f"{markdown_cell(batch.get('source_gate'))} |"
                    )
                lines.extend(["", "| Metric | Value |", "|---|---:|"])
                continue
            lines.append(f"| {key} | `{json.dumps(value, ensure_ascii=False, sort_keys=True)}` |")
        lines.append("")
    lines.extend(
        [
            "",
            "## Exchange Baseline",
            "",
            "| Exchange | Tickers | ISIN | Sector | Source Gaps | Warns | Quality Source Gaps | Quarantine | Review Context |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---|",
        ]
    )
    for exchange, values in payload["exchange_baseline"].items():
        exchange_context = (
            contexts.get("exchanges", {}).get(exchange, "") if isinstance(contexts.get("exchanges"), dict) else ""
        )
        lines.append(
            f"| {exchange} | {values['tickers']} | {values['isin_coverage']} | {values['sector_coverage']} | {values['source_gap_rows']} | {values['entry_quality_warn_rows']} | {values['entry_quality_source_gap_rows']} | {values['entry_quality_quarantine_rows']} | `{markdown_cell(exchange_context)}` |"
        )
    append_source_files_markdown(lines, payload)
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a baseline snapshot for future improvement campaign deltas.")
    parser.add_argument("--json-out", type=Path, default=DEFAULT_JSON_OUT)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_MD_OUT)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    payload = build_payload()
    args.json_out.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    args.md_out.write_text(render_markdown(payload) + "\n", encoding="utf-8")
    print(json.dumps({"generated_at": payload["_meta"]["generated_at"], "campaigns": len(payload["campaign_baseline"])}, indent=2))


if __name__ == "__main__":
    main()

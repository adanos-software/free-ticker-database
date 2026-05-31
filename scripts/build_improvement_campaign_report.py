from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
REPORTS_DIR = ROOT / "data" / "reports"

DEFAULT_JSON_OUT = REPORTS_DIR / "improvement_campaigns.json"
DEFAULT_MD_OUT = REPORTS_DIR / "improvement_campaigns.md"
COMMAND_SCRIPT_PATTERN = re.compile(r"\bpython\s+(scripts/[\w./-]+\.py)")

ARTIFACT_NAMES = [
    "coverage_report",
    "source_gap_classification",
    "b3_masterfile_gap_review",
    "b3_etf_category_apply_report",
    "b3_residual_isin_review",
    "b3_residual_sector_review",
    "otc_scope_review",
    "otc_name_mismatch_review",
    "canada_residual_review",
    "canada_figi_queue",
    "canada_figi_apply_report",
    "asx_residual_review",
    "weak_sector_residual_review",
    "ngx_official_sector_apply_report",
    "masterfile_collision_review",
    "symbol_changes_review",
    "ohlcv_plausibility",
    "ohlcv_warning_review",
    "financialdata_isin_supplements_review",
    "validation_report",
    "improvement_baseline",
    "improvement_deltas",
]

ACCEPTANCE_DELTA_KEYS = (
    "isin_delta",
    "sector_delta",
    "category_delta",
    "source_gap_delta",
    "warn_delta",
    "quarantine_delta",
)

EXCHANGE_DELTA_KEY_MAP = {
    "isin_delta": "isin_delta",
    "sector_delta": "sector_delta",
    "category_delta": "sector_delta",
    "source_gap_delta": "source_gap_delta",
    "warn_delta": "warn_delta",
    "quarantine_delta": "quarantine_delta",
}

REVIEW_POLICY = {
    "no_guessing": "No inferred identifiers, sectors, ETF categories, names, or symbol changes may be applied from symbol or name shape.",
    "source_authority": "Official exchange, registry, CSD, or issuer evidence is required first; secondary sources are discovery signals only unless explicitly reviewed.",
    "uncertain_gap_handling": "Unverified fields remain blank and are tracked as reviewed source gaps until stronger evidence exists.",
    "traceability": "Any future data change must be listing-keyed and tied to source evidence, review status, or source-gap status.",
}


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


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


def b3_residual_workstream_rows(payloads: dict[str, dict[str, Any]]) -> dict[str, int]:
    return {
        "masterfile_active_directory_gap": row_count(payloads.get("b3_masterfile_gap_review", {})),
        "missing_isin_residual": row_count(payloads.get("b3_residual_isin_review", {})),
        "missing_sector_residual": row_count(payloads.get("b3_residual_sector_review", {})),
    }


def b3_residual_workstream_priority_totals(payloads: dict[str, dict[str, Any]]) -> dict[str, dict[str, int]]:
    workstreams = {
        "masterfile_active_directory_gap": payloads.get("b3_masterfile_gap_review", {}),
        "missing_isin_residual": payloads.get("b3_residual_isin_review", {}),
        "missing_sector_residual": payloads.get("b3_residual_sector_review", {}),
    }
    return {
        workstream: dict(summary(payload).get("review_priority_totals", {}))
        for workstream, payload in workstreams.items()
    }


def b3_residual_workstream_readiness_totals(payloads: dict[str, dict[str, Any]]) -> dict[str, dict[str, int]]:
    workstreams = {
        "masterfile_active_directory_gap": payloads.get("b3_masterfile_gap_review", {}),
        "missing_isin_residual": payloads.get("b3_residual_isin_review", {}),
        "missing_sector_residual": payloads.get("b3_residual_sector_review", {}),
    }
    readiness: dict[str, dict[str, int]] = {}
    for workstream, payload in workstreams.items():
        apply_totals = summary(payload).get("apply_eligibility_totals", {})
        if isinstance(apply_totals, dict):
            readiness[workstream] = {
                str(key): int(value)
                for key, value in sorted(apply_totals.items())
                if isinstance(value, int) and not isinstance(value, bool) and value > 0
            }
        else:
            readiness[workstream] = {}
    return readiness


def top_counts(values: dict[str, Any], limit: int = 5) -> dict[str, int]:
    numeric = {}
    for key, value in values.items():
        try:
            number = int(value)
            if number <= 0:
                continue
            numeric[key] = number
        except (TypeError, ValueError):
            continue
    return dict(sorted(numeric.items(), key=lambda item: (-item[1], item[0]))[:limit])


def freshness_age_bucket(age_hours: Any) -> str:
    try:
        age = float(age_hours)
    except (TypeError, ValueError):
        return "unknown_age"
    if age < 0:
        return "unknown_age"
    if age <= 48:
        return "age_0_48h"
    if age <= 168:
        return "age_48_168h"
    if age <= 336:
        return "age_168_336h"
    return "age_over_336h"


def freshness_snapshot_row(
    *,
    key: str,
    source_type: str,
    generated_at: Any,
    age_hours: Any,
    rows: Any,
) -> dict[str, Any]:
    try:
        numeric_age = round(float(age_hours), 2)
    except (TypeError, ValueError):
        numeric_age = None
    try:
        numeric_rows = int(rows)
    except (TypeError, ValueError):
        numeric_rows = 0
    return {
        "key": key,
        "source_type": source_type,
        "generated_at": str(generated_at or ""),
        "age_hours": numeric_age,
        "age_bucket": freshness_age_bucket(numeric_age),
        "rows": max(0, numeric_rows),
        "recommended_next_source": f"Fresh generated_at and row-count evidence for {key}.",
        "source_gate": "Freshness is visibility evidence only; it does not authorize canonical data changes without listing-keyed source review.",
    }


def freshness_snapshot(coverage: dict[str, Any]) -> list[dict[str, Any]]:
    freshness = coverage.get("freshness", {})
    if not isinstance(freshness, dict):
        freshness = {}
    global_counts = coverage.get("global", {})
    if not isinstance(global_counts, dict):
        global_counts = {}
    rows = [
        freshness_snapshot_row(
            key="tickers",
            source_type="dataset",
            generated_at=freshness.get("tickers_built_at"),
            age_hours=freshness.get("tickers_age_hours"),
            rows=global_counts.get("tickers", 0),
        ),
        freshness_snapshot_row(
            key="masterfiles",
            source_type="source_inventory",
            generated_at=freshness.get("masterfiles_generated_at"),
            age_hours=freshness.get("masterfiles_age_hours"),
            rows=global_counts.get("official_masterfile_symbols", 0),
        ),
        freshness_snapshot_row(
            key="identifiers",
            source_type="dataset",
            generated_at=freshness.get("identifiers_generated_at"),
            age_hours=freshness.get("identifiers_age_hours"),
            rows=global_counts.get("isin_coverage", 0),
        ),
        freshness_snapshot_row(
            key="entry_quality",
            source_type="quality_gate",
            generated_at=freshness.get("entry_quality_generated_at"),
            age_hours=freshness.get("entry_quality_age_hours"),
            rows=freshness.get("entry_quality_rows", 0),
        ),
        freshness_snapshot_row(
            key="source_gap_classification",
            source_type="review_report",
            generated_at=freshness.get("source_gap_classification_generated_at"),
            age_hours=freshness.get("source_gap_classification_age_hours"),
            rows=freshness.get("source_gap_classification_rows", 0),
        ),
        freshness_snapshot_row(
            key="symbol_changes",
            source_type="workflow_report",
            generated_at=freshness.get("symbol_changes_generated_at"),
            age_hours=freshness.get("symbol_changes_age_hours"),
            rows=freshness.get("symbol_changes_review_rows", 0),
        ),
        freshness_snapshot_row(
            key="masterfile_collision_review",
            source_type="review_report",
            generated_at=freshness.get("masterfile_collision_review_generated_at"),
            age_hours=freshness.get("masterfile_collision_review_age_hours"),
            rows=freshness.get("masterfile_collision_review_rows", 0),
        ),
        freshness_snapshot_row(
            key="ohlcv_plausibility",
            source_type="sampling_report",
            generated_at=freshness.get("ohlcv_plausibility_generated_at"),
            age_hours=freshness.get("ohlcv_plausibility_age_hours"),
            rows=freshness.get("ohlcv_plausibility_rows", 0),
        ),
    ]
    return sorted(rows, key=lambda row: (row["source_type"], row["key"]))


def freshness_snapshot_age_bucket_totals(snapshot: list[dict[str, Any]]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for row in snapshot:
        bucket = str(row.get("age_bucket") or "unknown_age")
        counts[bucket] += 1
    return dict(sorted(counts.items()))


def top_freshness_snapshot_ages(snapshot: list[dict[str, Any]], limit: int = 8) -> list[dict[str, Any]]:
    rows = [
        row
        for row in snapshot
        if isinstance(row.get("age_hours"), int | float) and not isinstance(row.get("age_hours"), bool)
    ]
    rows.sort(key=lambda row: (-float(row.get("age_hours") or 0), str(row.get("source_type") or ""), str(row.get("key") or "")))
    return [
        {
            "key": str(row.get("key") or ""),
            "source_type": str(row.get("source_type") or ""),
            "generated_at": str(row.get("generated_at") or ""),
            "age_hours": row.get("age_hours"),
            "age_bucket": str(row.get("age_bucket") or "unknown_age"),
            "rows": int(row.get("rows") or 0),
            "recommended_next_source": row.get("recommended_next_source", ""),
            "source_gate": row.get("source_gate", ""),
        }
        for row in rows[:limit]
    ]


def artifact_entry(path: Path, payload: dict[str, Any]) -> dict[str, Any]:
    artifact_summary = summary(payload)
    generated_at = (
        payload.get("_meta", {}).get("generated_at", "")
        or artifact_summary.get("generated_at", "")
        or payload.get("generated_at", "")
    )
    return {
        "path": display_path(path),
        "generated_at": generated_at,
        "rows": row_count(payload),
    }


def delta_evidence(status: str, known: dict[str, Any] | None = None, missing: list[str] | None = None) -> dict[str, Any]:
    return {
        "status": status,
        "known_deltas": known or {},
        "missing_deltas": missing or [],
    }


def numeric_delta_value(row: dict[str, Any] | None) -> int | float:
    if not isinstance(row, dict):
        return 0
    value = row.get("delta", 0)
    return value if isinstance(value, int | float) and not isinstance(value, bool) else 0


def campaign_metric_deltas(deltas: dict[str, Any], campaign_key: str) -> dict[str, Any]:
    row = deltas.get("campaign_deltas", {}).get(campaign_key, {})
    if not isinstance(row, dict):
        return {}
    return row.get("children", row)


def scoped_exchange_delta_totals(deltas: dict[str, Any], exchanges: list[str] | None) -> dict[str, Any]:
    exchange_matrix = deltas.get("exchange_acceptance_delta_matrix", {})
    if not isinstance(exchange_matrix, dict):
        exchange_matrix = {}
    if exchanges is None:
        scoped_rows = exchange_matrix
    else:
        scoped_rows = {exchange: exchange_matrix.get(exchange, {}) for exchange in exchanges}
    totals: dict[str, int | float | None] = {}
    for key in ACCEPTANCE_DELTA_KEYS:
        exchange_key = EXCHANGE_DELTA_KEY_MAP[key]
        totals[key] = sum(numeric_delta_value(row.get(exchange_key)) for row in scoped_rows.values() if isinstance(row, dict))
    changed_exchange_rows = {
        exchange: row
        for exchange, row in scoped_rows.items()
        if isinstance(row, dict) and any(numeric_delta_value(value) != 0 for value in row.values() if isinstance(value, dict))
    }
    return {
        "exchange_count": len(scoped_rows),
        "changed_exchange_rows": len(changed_exchange_rows),
        "delta_totals": totals,
    }


def acceptance_matrix(
    *,
    campaign_key: str,
    exchanges: list[str] | None,
    artifacts: list[dict[str, Any]],
    deltas: dict[str, Any],
) -> dict[str, Any]:
    global_matrix = deltas.get("acceptance_delta_matrix", {})
    return {
        "campaign_key": campaign_key,
        "exchange_scope": "all" if exchanges is None else exchanges,
        "affected_artifact_rows": sum(int(artifact.get("rows", 0)) for artifact in artifacts),
        "campaign_metric_deltas": campaign_metric_deltas(deltas, campaign_key),
        "global_acceptance_deltas": {key: global_matrix.get(key, {}) for key in ACCEPTANCE_DELTA_KEYS},
        "exchange_scope_deltas": scoped_exchange_delta_totals(deltas, exchanges),
        "required_metrics": list(ACCEPTANCE_DELTA_KEYS),
        "note": "Deltas are evidence only. Non-zero deltas require listing-keyed source review before any data interpretation.",
    }


def before_after_delta_row(row: dict[str, Any] | None) -> dict[str, int | float | None]:
    if not isinstance(row, dict):
        return {"before": None, "after": None, "delta": None}
    return {
        "before": row.get("baseline"),
        "after": row.get("current"),
        "delta": row.get("delta"),
    }


def count_nonzero_deltas(rows: dict[str, Any]) -> int:
    total = 0
    for row in rows.values():
        if not isinstance(row, dict):
            continue
        value = row.get("delta")
        if isinstance(value, (int, float)) and not isinstance(value, bool) and value != 0:
            total += 1
    return total


def before_after_context_for(summary: dict[str, Any]) -> str:
    global_before_after = summary.get("global_before_after", {})
    if not isinstance(global_before_after, dict):
        global_before_after = {}
    exchange_scope_delta_totals = summary.get("exchange_scope_delta_totals", {})
    if not isinstance(exchange_scope_delta_totals, dict):
        exchange_scope_delta_totals = {}
    nonzero_scope_deltas = sum(
        1
        for value in exchange_scope_delta_totals.values()
        if isinstance(value, (int, float)) and not isinstance(value, bool) and value != 0
    )
    warn_quarantine = summary.get("warn_quarantine_delta", {})
    if not isinstance(warn_quarantine, dict):
        warn_quarantine = {}
    warn_delta = warn_quarantine.get("warn_delta", {})
    quarantine_delta = warn_quarantine.get("quarantine_delta", {})
    if not isinstance(warn_delta, dict):
        warn_delta = {}
    if not isinstance(quarantine_delta, dict):
        quarantine_delta = {}
    return (
        f"exchange_scope={exchange_scope_text(summary.get('exchange_scope'))};"
        f"affected_artifact_rows={summary.get('affected_artifact_rows', 0)};"
        f"global_delta_count={len(global_before_after)};"
        f"nonzero_global_delta_count={count_nonzero_deltas(global_before_after)};"
        f"nonzero_exchange_scope_delta_count={nonzero_scope_deltas};"
        f"warn_delta={warn_delta.get('delta')};"
        f"quarantine_delta={quarantine_delta.get('delta')}"
    )


def before_after_summary(matrix: dict[str, Any]) -> dict[str, Any]:
    global_deltas = matrix.get("global_acceptance_deltas", {})
    if not isinstance(global_deltas, dict):
        global_deltas = {}
    exchange_scope_deltas = matrix.get("exchange_scope_deltas", {})
    if not isinstance(exchange_scope_deltas, dict):
        exchange_scope_deltas = {}
    delta_totals = exchange_scope_deltas.get("delta_totals", {})
    if not isinstance(delta_totals, dict):
        delta_totals = {}
    row = {
        "exchange_scope": matrix.get("exchange_scope"),
        "affected_artifact_rows": matrix.get("affected_artifact_rows", 0),
        "global_before_after": {
            key: before_after_delta_row(global_deltas.get(key))
            for key in ACCEPTANCE_DELTA_KEYS
        },
        "exchange_scope_delta_totals": {
            key: delta_totals.get(key)
            for key in ACCEPTANCE_DELTA_KEYS
        },
        "warn_quarantine_delta": {
            "warn_delta": before_after_delta_row(global_deltas.get("warn_delta")),
            "quarantine_delta": before_after_delta_row(global_deltas.get("quarantine_delta")),
        },
        "policy": "Before/after values are release evidence only; any non-zero delta still requires listing-keyed source review.",
    }
    row["before_after_context"] = before_after_context_for(row)
    return row


def exchange_scope_text(scope: Any) -> str:
    if isinstance(scope, list):
        return "|".join(str(item) for item in scope) or "none"
    return str(scope or "none")


def campaign_context_for(campaign: dict[str, Any]) -> str:
    matrix = campaign.get("acceptance_matrix", {})
    if not isinstance(matrix, dict):
        matrix = {}
    return (
        f"priority={campaign.get('priority', 'none')};"
        f"campaign_key={campaign.get('campaign_key', '') or 'none'};"
        f"status={campaign.get('status', '') or 'none'};"
        f"exchange_scope={exchange_scope_text(matrix.get('exchange_scope'))}"
    )


def artifact_context_for(campaign: dict[str, Any]) -> str:
    artifacts = campaign.get("artifacts", [])
    if not isinstance(artifacts, list):
        artifacts = []
    artifact_rows = [artifact for artifact in artifacts if isinstance(artifact, dict)]
    primary_artifact = max(artifact_rows, key=lambda artifact: int(artifact.get("rows") or 0), default={})
    return (
        f"artifact_count={len(artifact_rows)};"
        f"affected_artifact_rows={sum(int(artifact.get('rows') or 0) for artifact in artifact_rows)};"
        f"primary_artifact={primary_artifact.get('path', '') or 'none'};"
        f"primary_artifact_rows={int(primary_artifact.get('rows') or 0)}"
    )


def delta_review_context_for(campaign: dict[str, Any]) -> str:
    delta = campaign.get("delta_evidence", {})
    if not isinstance(delta, dict):
        delta = {}
    missing = delta.get("missing_deltas", [])
    if not isinstance(missing, list):
        missing = []
    return (
        f"delta_status={delta.get('status', '') or 'none'};"
        f"missing_delta_count={len(missing)};"
        f"before_after_present={'true' if isinstance(campaign.get('before_after_summary'), dict) else 'false'};"
        f"next_action_present={'true' if bool(campaign.get('next_action')) else 'false'}"
    )


def review_batch_artifact_rows(campaign: dict[str, Any]) -> int:
    artifacts = campaign.get("artifacts", [])
    if not isinstance(artifacts, list):
        return 0
    return sum(
        int(artifact.get("rows") or 0)
        for artifact in artifacts
        if isinstance(artifact, dict) and int(artifact.get("rows") or 0) > 0
    )


def campaign_closure_gate_for(campaign: dict[str, Any]) -> str:
    campaign_key = str(campaign.get("campaign_key") or campaign.get("name") or "")
    delta = campaign.get("delta_evidence", {})
    if not isinstance(delta, dict):
        delta = {}
    missing = delta.get("missing_deltas", [])
    if not isinstance(missing, list):
        missing = []
    if campaign_key == "baseline":
        return "future_campaign_delta_comparison_required_before_release_closure"
    if missing:
        return "missing_delta_evidence_must_be_resolved_or_documented_before_campaign_closure"
    return "review_artifact_rows_must_be_processed_before_campaign_closure"


def closure_status_for_gate(closure_gate: str) -> str:
    if closure_gate == "review_artifact_rows_must_be_processed_before_campaign_closure":
        return "ready_after_review_artifact_processing"
    return "blocked_until_closure_gate_resolved"


def closure_context_for(campaign: dict[str, Any]) -> str:
    delta = campaign.get("delta_evidence", {})
    if not isinstance(delta, dict):
        delta = {}
    missing = delta.get("missing_deltas", [])
    if not isinstance(missing, list):
        missing = []
    closure_gate = campaign_closure_gate_for(campaign)
    return (
        f"closure_gate={closure_gate};"
        f"artifact_rows={review_batch_artifact_rows(campaign)};"
        f"missing_delta_count={len(missing)};"
        f"closure_status={closure_status_for_gate(closure_gate)}"
    )


def campaign_review_policy(campaign_key: str) -> dict[str, str]:
    return {
        **REVIEW_POLICY,
        "campaign_scope": f"{campaign_key} campaign decisions must preserve the no-guessing and source-authority gates.",
    }


def markdown_cell(value: Any) -> str:
    return str(value if value is not None else "").replace("\n", " ").replace("|", "\\|")


def source_gated_batch_identity(batch: dict[str, Any]) -> str:
    for key in (
        "review_bucket",
        "b3_resolution_queue",
        "canada_resolution_queue",
        "asx_resolution_queue",
        "weak_sector_resolution_queue",
        "identity_resolution_queue",
        "symbol_change_workflow_queue",
        "refresh_queue",
        "financialdata_review_queue",
        "review_class",
        "review_bucket",
        "field",
    ):
        value = batch.get(key)
        if value not in {"", None}:
            return f"{key}={value}"
    return "review_batch"


def append_source_gated_batch_table(lines: list[str], key: str, value: list[Any]) -> bool:
    batches = [batch for batch in value if isinstance(batch, dict)]
    if not batches or not any(batch.get("recommended_next_source") or batch.get("source_gate") for batch in batches):
        return False
    lines.extend(
        [
            f"- `{key}`:",
            "",
            "| Batch | Priority | Rows | Strategy | Evidence | Recommended Next Source | Source Gate |",
            "|---|---|---:|---|---|---|---|",
        ]
    )
    for batch in batches:
        evidence = batch.get("verification_evidence_required") or batch.get("evidence_required") or batch.get(
            "queue_evidence_required"
        )
        strategy = batch.get("review_strategy") or batch.get("sampling_strategy")
        lines.append(
            "| "
            f"`{markdown_cell(source_gated_batch_identity(batch))}` | "
            f"`{markdown_cell(batch.get('review_priority') or batch.get('refresh_priority'))}` | "
            f"{markdown_cell(batch.get('rows') or batch.get('source_count') or batch.get('total_rows'))} | "
            f"{markdown_cell(strategy)} | "
            f"{markdown_cell(evidence)} | "
            f"{markdown_cell(batch.get('recommended_next_source'))} | "
            f"{markdown_cell(batch.get('source_gate'))} |"
        )
    lines.append("")
    return True


def append_delta_evidence(lines: list[str], delta_evidence: dict[str, Any]) -> None:
    known_deltas = delta_evidence.get("known_deltas") if isinstance(delta_evidence, dict) else {}
    if not isinstance(known_deltas, dict):
        lines.append(f"- `delta_evidence`: `{json.dumps(delta_evidence, ensure_ascii=False, sort_keys=True)}`")
        return

    compact_delta_evidence = dict(delta_evidence)
    compact_known_deltas = dict(known_deltas)
    source_gated_lists = {
        key: value
        for key, value in compact_known_deltas.items()
        if isinstance(value, list) and any(isinstance(item, dict) for item in value)
    }
    for key in source_gated_lists:
        compact_known_deltas.pop(key, None)
    compact_delta_evidence["known_deltas"] = compact_known_deltas
    lines.append(f"- `delta_evidence`: `{json.dumps(compact_delta_evidence, ensure_ascii=False, sort_keys=True)}`")
    for key, value in source_gated_lists.items():
        append_source_gated_batch_table(lines, f"delta_evidence.known_deltas.{key}", value)


def next_review_recommended_source(campaign_key: str) -> str:
    sources = {
        "b3": "Current official B3 directories, B3 taxonomy, COTAHIST, CSD, fund registry, or issuer evidence keyed to the exact listing.",
        "otc": "OTC Markets profile, SEC/issuer filing, product taxonomy, or reviewed issuer/product source matching the OTC listing key.",
        "canada": "TMX/TSX/TSXV/Cboe Canada official listing or identifier source first; stronger reviewed FIGI evidence only after ISIN scope is resolved.",
        "asx": "Official ASX workbook/listing/product taxonomy, registry, or issuer source matching the exact ASX listing.",
        "weak_sector": "Venue-specific official taxonomy, exchange masterfile, or issuer taxonomy source for the exact listing.",
        "masterfile_collisions": "Official exchange directories, MIC/listing-key evidence, ISIN, instrument type, and issuer-name evidence for both sides of the pair.",
        "symbol_changes": "Official exchange notice, issuer notice, or current exchange directory proving old/new symbol state for the same listing identity.",
        "ohlcv": "Local or bounded-network OHLCV sample paired with existing official source-gap or entry-quality evidence.",
        "freshness": "Fresh official exchange, registry, issuer, or source-of-truth artifact with generated_at and row-count evidence.",
        "baseline": "Future regenerated campaign reports and acceptance deltas compared against the current baseline snapshot.",
    }
    return sources.get(campaign_key, "Official or reviewed source evidence matching the campaign's listing-keyed review scope.")


def next_review_source_gate(campaign_key: str) -> str:
    gates = {
        "b3": "Do not fill B3 ISIN, sector, category, or scope values until exact official evidence and row-level gates pass.",
        "otc": "Do not enrich OTC rows from symbol/name shape; decide scope and require issuer/product evidence before metadata changes.",
        "canada": "Do not add Canada ISIN/FIGI from symbol/name alone; require official identifier evidence or reviewed stronger FIGI source.",
        "asx": "Do not apply ASX identifiers or categories until exact ASX listing/name/product gates pass.",
        "weak_sector": "Keep sector blank until venue-official or issuer taxonomy evidence maps to a canonical sector.",
        "masterfile_collisions": "Do not resolve collisions with symbol-only matches; require listing-key, exchange/MIC, ISIN, name, and instrument-type review.",
        "symbol_changes": "Do not rename canonical symbols until official listing-keyed evidence proves the old/new symbol transition.",
        "ohlcv": "Use OHLCV only as plausibility evidence; never authorize identifier, sector, category, name, or symbol changes from price data.",
        "freshness": "Do not perform stale-source data work until official artifacts are refreshed or a reviewed unavailable/replacement decision exists.",
        "baseline": "Do not close future campaign work until deltas are compared against this baseline and review artifacts remain source-gated.",
    }
    return gates.get(campaign_key, "Do not apply data changes until listing-keyed official or reviewed source evidence satisfies the campaign gate.")


def next_review_batches(campaigns: list[dict[str, Any]]) -> list[dict[str, Any]]:
    batches: list[dict[str, Any]] = []
    for campaign in sorted(campaigns, key=lambda row: row.get("priority", 999)):
        artifacts = campaign.get("artifacts", [])
        if not isinstance(artifacts, list):
            artifacts = []
        artifact_rows = [
            artifact for artifact in artifacts if isinstance(artifact, dict) and int(artifact.get("rows") or 0) > 0
        ]
        primary_artifact = max(artifact_rows, key=lambda artifact: int(artifact.get("rows") or 0), default={})
        delta_evidence_value = campaign.get("delta_evidence", {})
        delta_evidence_row = delta_evidence_value if isinstance(delta_evidence_value, dict) else {}
        missing_delta_evidence = delta_evidence_row.get("missing_deltas", [])
        if not isinstance(missing_delta_evidence, list):
            missing_delta_evidence = []
        total_rows = sum(int(artifact.get("rows") or 0) for artifact in artifact_rows)
        if total_rows <= 0 and not missing_delta_evidence:
            continue
        campaign_key = str(campaign.get("campaign_key") or campaign.get("name") or "")
        closure_gate = campaign_closure_gate_for(campaign)
        batches.append(
            {
                "priority": campaign.get("priority"),
                "campaign_key": campaign_key,
                "status": campaign.get("status", ""),
                "delta_status": delta_evidence_row.get("status", ""),
                "artifact_rows": total_rows,
                "primary_artifact": primary_artifact.get("path", ""),
                "primary_artifact_rows": int(primary_artifact.get("rows") or 0),
                "missing_delta_evidence": missing_delta_evidence,
                "closure_gate": closure_gate,
                "next_action": campaign.get("next_action", ""),
                "recommended_next_source": next_review_recommended_source(campaign_key),
                "source_gate": next_review_source_gate(campaign_key),
                "closure_context": closure_context_for(campaign),
            }
        )
    return batches


def closure_readiness_summary(batches: list[dict[str, Any]]) -> dict[str, Any]:
    closure_gate_counts: dict[str, int] = {}
    blocked_campaigns: list[str] = []
    ready_campaigns: list[str] = []
    for batch in batches:
        gate = str(batch.get("closure_gate") or "missing_closure_gate")
        closure_gate_counts[gate] = closure_gate_counts.get(gate, 0) + 1
        campaign_key = str(batch.get("campaign_key") or "")
        if gate == "review_artifact_rows_must_be_processed_before_campaign_closure":
            ready_campaigns.append(campaign_key)
        else:
            blocked_campaigns.append(campaign_key)
    return {
        "ready_campaigns": len(ready_campaigns),
        "blocked_campaigns": len(blocked_campaigns),
        "closure_gate_counts": dict(sorted(closure_gate_counts.items())),
        "ready_campaign_keys": ready_campaigns,
        "blocked_campaign_keys": blocked_campaigns,
        "policy": "Campaigns are not closure-ready while missing delta evidence, future-baseline comparison, or unprocessed review artifacts remain.",
    }


def next_review_workload_context(workload: dict[str, Any]) -> str:
    top_batch = workload.get("largest_batch", {})
    if not isinstance(top_batch, dict):
        top_batch = {}
    return (
        f"batches={workload.get('total_batches', 0)};"
        f"rows={workload.get('total_rows', 0)};"
        f"blocked_batches={workload.get('blocked_batches', 0)};"
        f"top_campaign={top_batch.get('campaign_key', '') or 'none'};"
        f"top_rows={top_batch.get('artifact_rows', 0)};"
        f"top_gate={top_batch.get('closure_gate', '') or 'none'}"
    )


def next_review_workload_summary(batches: list[dict[str, Any]]) -> dict[str, Any]:
    rows_by_campaign_key: dict[str, int] = {}
    rows_by_closure_gate: dict[str, int] = {}
    blocked_batches = 0
    for batch in batches:
        campaign_key = str(batch.get("campaign_key") or "unknown")
        closure_gate = str(batch.get("closure_gate") or "missing_closure_gate")
        rows = int(batch.get("artifact_rows") or 0)
        rows_by_campaign_key[campaign_key] = rows_by_campaign_key.get(campaign_key, 0) + rows
        rows_by_closure_gate[closure_gate] = rows_by_closure_gate.get(closure_gate, 0) + rows
        if closure_gate != "review_artifact_rows_must_be_processed_before_campaign_closure":
            blocked_batches += 1
    largest_batch = max(batches, key=lambda row: int(row.get("artifact_rows") or 0), default={})
    workload = {
        "total_batches": len(batches),
        "total_rows": sum(int(batch.get("artifact_rows") or 0) for batch in batches),
        "blocked_batches": blocked_batches,
        "rows_by_campaign_key": dict(sorted(rows_by_campaign_key.items())),
        "rows_by_closure_gate": dict(sorted(rows_by_closure_gate.items())),
        "largest_batch": {
            "priority": largest_batch.get("priority"),
            "campaign_key": largest_batch.get("campaign_key", ""),
            "artifact_rows": int(largest_batch.get("artifact_rows") or 0),
            "primary_artifact": largest_batch.get("primary_artifact", ""),
            "closure_gate": largest_batch.get("closure_gate", ""),
        },
        "policy": "Workload rows are review workload only. They do not authorize data changes or closure without processing the listing-keyed artifacts.",
    }
    workload["workload_context"] = next_review_workload_context(workload)
    return workload


def execution_plan_context_for(row: dict[str, Any]) -> str:
    return (
        f"priority={row.get('priority', 'none')};"
        f"campaign_key={row.get('campaign_key', '') or 'none'};"
        f"artifact_rows={int(row.get('artifact_rows') or 0)};"
        f"command_mode={row.get('command_mode', '') or 'none'};"
        f"network_required={str(row.get('network_required', '')).lower()};"
        f"data_change_authorized={str(row.get('data_change_authorized', '')).lower()}"
    )


def execution_ranking_reason(row: dict[str, Any]) -> str:
    if row.get("network_required") is True:
        return "objective_priority_order_with_external_source_refresh_required"
    if int(row.get("artifact_rows") or 0) <= 0:
        return "objective_priority_order_future_delta_baseline_gate"
    return "objective_priority_order_with_local_review_artifact_processing"


def execution_ranking_context_for(row: dict[str, Any]) -> str:
    return (
        f"execution_order={row.get('execution_order', 'none')};"
        f"priority={row.get('priority', 'none')};"
        f"campaign_key={row.get('campaign_key', '') or 'none'};"
        f"artifact_rows={int(row.get('artifact_rows') or 0)};"
        f"network_required={str(row.get('network_required', '')).lower()};"
        f"ranking_reason={row.get('ranking_reason', '') or 'none'}"
    )


def command_script_paths(command: str) -> list[str]:
    return COMMAND_SCRIPT_PATTERN.findall(command)


def command_readiness_context_for(row: dict[str, Any]) -> str:
    scripts = row.get("command_scripts", [])
    if not isinstance(scripts, list):
        scripts = []
    missing = row.get("missing_command_scripts", [])
    if not isinstance(missing, list):
        missing = []
    return (
        f"campaign_key={row.get('campaign_key', '') or 'none'};"
        f"script_count={len(scripts)};"
        f"missing_script_count={len(missing)};"
        f"all_scripts_exist={str(len(missing) == 0).lower()}"
    )


def command_mutation_risk_for(scripts: list[str]) -> str:
    risky_markers = ("/apply_", "/backfill_", "/enrich_")
    if any(any(marker in f"/{script}" for marker in risky_markers) for script in scripts):
        return "review_required"
    return "report_or_fetch_only"


def risky_command_scripts(scripts: list[str]) -> list[str]:
    risky_markers = ("/apply_", "/backfill_", "/enrich_")
    return [
        script for script in scripts
        if any(marker in f"/{script}" for marker in risky_markers)
    ]


def command_safety_context_for(row: dict[str, Any]) -> str:
    return (
        f"campaign_key={row.get('campaign_key', '') or 'none'};"
        f"command_mutation_risk={row.get('command_mutation_risk', '') or 'none'};"
        f"manual_review_required_before_run={str(row.get('manual_review_required_before_run', '')).lower()};"
        f"data_change_authorized={str(row.get('data_change_authorized', '')).lower()}"
    )


def review_required_command_context_for(row: dict[str, Any]) -> str:
    risky_scripts = row.get("risky_command_scripts", [])
    if not isinstance(risky_scripts, list):
        risky_scripts = []
    return (
        f"campaign_key={row.get('campaign_key', '') or 'none'};"
        f"risky_script_count={len(risky_scripts)};"
        f"manual_review_required_before_run={str(row.get('manual_review_required_before_run', '')).lower()};"
        f"data_change_authorized={str(row.get('data_change_authorized', '')).lower()}"
    )


def review_required_preflight_checks(row: dict[str, Any]) -> list[str]:
    if row.get("command_mutation_risk") != "review_required":
        return []
    return [
        "inspect_risky_scripts_before_execution",
        "confirm_listing_keyed_source_review_for_any_write",
        "rerun_quality_validation_and_release_acceptance_after_execution",
    ]


def review_required_preflight_context_for(row: dict[str, Any]) -> str:
    checks = row.get("review_required_preflight_checks", [])
    if not isinstance(checks, list):
        checks = []
    return (
        f"campaign_key={row.get('campaign_key', '') or 'none'};"
        f"preflight_check_count={len(checks)};"
        f"manual_review_required_before_run={str(row.get('manual_review_required_before_run', '')).lower()};"
        f"data_change_authorized={str(row.get('data_change_authorized', '')).lower()}"
    )


def next_review_execution_plan(batches: list[dict[str, Any]]) -> list[dict[str, Any]]:
    plan: list[dict[str, Any]] = []
    for execution_order, batch in enumerate(
        sorted(batches, key=lambda row: row.get("priority") if isinstance(row.get("priority"), int) else 999),
        start=1,
    ):
        campaign_key = str(batch.get("campaign_key") or "")
        missing = batch.get("missing_delta_evidence", [])
        if not isinstance(missing, list):
            missing = []
        evidence_command = evidence_command_for_blocker(campaign_key, missing[0] if missing else "")
        row = {
            "execution_order": execution_order,
            "priority": batch.get("priority"),
            "campaign_key": campaign_key,
            "artifact_rows": int(batch.get("artifact_rows") or 0),
            "primary_artifact": batch.get("primary_artifact", ""),
            "closure_gate": batch.get("closure_gate", ""),
            "evidence_command": evidence_command,
            **command_metadata_for_blocker(campaign_key, evidence_command),
            "recommended_next_source": batch.get("recommended_next_source") or next_review_recommended_source(campaign_key),
            "source_gate": batch.get("source_gate") or next_review_source_gate(campaign_key),
            "next_action": batch.get("next_action", ""),
        }
        command_scripts = command_script_paths(evidence_command)
        row["command_scripts"] = command_scripts
        row["missing_command_scripts"] = [
            script for script in command_scripts
            if not (ROOT / script).exists()
        ]
        row["command_readiness_context"] = command_readiness_context_for(row)
        row["command_mutation_risk"] = command_mutation_risk_for(command_scripts)
        row["risky_command_scripts"] = risky_command_scripts(command_scripts)
        row["manual_review_required_before_run"] = row["command_mutation_risk"] == "review_required"
        row["command_safety_context"] = command_safety_context_for(row)
        row["review_required_command_context"] = review_required_command_context_for(row)
        row["review_required_preflight_checks"] = review_required_preflight_checks(row)
        row["review_required_preflight_context"] = review_required_preflight_context_for(row)
        row["ranking_reason"] = execution_ranking_reason(row)
        row["ranking_context"] = execution_ranking_context_for(row)
        row["execution_context"] = execution_plan_context_for(row)
        plan.append(row)
    return plan


def execution_summary_context_for(summary: dict[str, Any]) -> str:
    return (
        f"actions={summary.get('total_actions', 0)};"
        f"local_actions={summary.get('local_report_rebuild_actions', 0)};"
        f"network_actions={summary.get('network_evidence_refresh_actions', 0)};"
        f"network_rows={summary.get('network_required_rows', 0)};"
        f"data_change_authorized_actions={summary.get('data_change_authorized_actions', 0)}"
    )


def next_review_execution_summary(plan: list[dict[str, Any]]) -> dict[str, Any]:
    rows_by_command_mode: dict[str, int] = {}
    network_campaign_keys: list[str] = []
    local_campaign_keys: list[str] = []
    data_change_authorized_actions = 0
    for row in plan:
        command_mode = str(row.get("command_mode") or "unknown")
        artifact_rows = int(row.get("artifact_rows") or 0)
        rows_by_command_mode[command_mode] = rows_by_command_mode.get(command_mode, 0) + artifact_rows
        campaign_key = str(row.get("campaign_key") or "")
        if row.get("network_required") is True:
            network_campaign_keys.append(campaign_key)
        else:
            local_campaign_keys.append(campaign_key)
        if row.get("data_change_authorized") is True:
            data_change_authorized_actions += 1
    summary = {
        "total_actions": len(plan),
        "local_report_rebuild_actions": sum(1 for row in plan if row.get("command_mode") == "local_report_rebuild"),
        "network_evidence_refresh_actions": sum(1 for row in plan if row.get("command_mode") == "network_evidence_refresh"),
        "network_required_rows": sum(int(row.get("artifact_rows") or 0) for row in plan if row.get("network_required") is True),
        "local_report_rebuild_rows": sum(int(row.get("artifact_rows") or 0) for row in plan if row.get("network_required") is not True),
        "rows_by_command_mode": dict(sorted(rows_by_command_mode.items())),
        "network_campaign_keys": network_campaign_keys,
        "local_campaign_keys": local_campaign_keys,
        "data_change_authorized_actions": data_change_authorized_actions,
        "policy": "Execution summary is planning evidence only. Network refreshes collect source evidence, and no row authorizes data changes.",
    }
    summary["execution_summary_context"] = execution_summary_context_for(summary)
    return summary


def command_safety_summary_context_for(summary: dict[str, Any]) -> str:
    return (
        f"actions={summary.get('total_actions', 0)};"
        f"review_required_actions={summary.get('review_required_actions', 0)};"
        f"report_or_fetch_only_actions={summary.get('report_or_fetch_only_actions', 0)};"
        f"manual_review_required_actions={summary.get('manual_review_required_actions', 0)};"
        f"preflight_complete_actions={summary.get('preflight_complete_actions', 0)};"
        f"data_change_authorized_actions={summary.get('data_change_authorized_actions', 0)};"
        f"execution_ready_without_manual_review={str(summary.get('execution_ready_without_manual_review', '')).lower()};"
        f"execution_blocking_gate={summary.get('execution_blocking_gate', '') or 'none'}"
    )


def next_review_command_safety_summary(plan: list[dict[str, Any]]) -> dict[str, Any]:
    risk_counts: dict[str, int] = {}
    review_required_campaign_keys: list[str] = []
    review_required_command_rows: list[dict[str, Any]] = []
    preflight_complete_actions = 0
    preflight_gap_campaign_keys: list[str] = []
    for row in plan:
        risk = str(row.get("command_mutation_risk") or "unknown")
        risk_counts[risk] = risk_counts.get(risk, 0) + 1
        if risk == "review_required":
            review_required_campaign_keys.append(str(row.get("campaign_key") or ""))
            risky_scripts = row.get("risky_command_scripts", [])
            if not isinstance(risky_scripts, list):
                risky_scripts = []
            checks = row.get("review_required_preflight_checks", [])
            if not isinstance(checks, list):
                checks = []
            if len(checks) >= 3:
                preflight_complete_actions += 1
            else:
                preflight_gap_campaign_keys.append(str(row.get("campaign_key") or ""))
            review_required_command_rows.append(
                {
                    "campaign_key": row.get("campaign_key", ""),
                    "risky_command_scripts": risky_scripts,
                    "manual_review_required_before_run": row.get("manual_review_required_before_run") is True,
                    "data_change_authorized": row.get("data_change_authorized") is True,
                    "review_required_command_context": row.get("review_required_command_context", ""),
                    "review_required_preflight_checks": row.get("review_required_preflight_checks", []),
                    "review_required_preflight_context": row.get("review_required_preflight_context", ""),
                }
            )
    execution_blocking_campaign_keys = sorted(set(review_required_campaign_keys + preflight_gap_campaign_keys))
    execution_ready_without_manual_review = not execution_blocking_campaign_keys
    execution_blocking_gate = (
        "no_manual_command_blockers_detected"
        if execution_ready_without_manual_review
        else "manual_review_required_before_execution"
    )
    summary = {
        "total_actions": len(plan),
        "risk_counts": dict(sorted(risk_counts.items())),
        "review_required_actions": risk_counts.get("review_required", 0),
        "report_or_fetch_only_actions": risk_counts.get("report_or_fetch_only", 0),
        "manual_review_required_actions": sum(1 for row in plan if row.get("manual_review_required_before_run") is True),
        "review_required_campaign_keys": review_required_campaign_keys,
        "review_required_command_rows": review_required_command_rows,
        "preflight_complete_actions": preflight_complete_actions,
        "preflight_gap_campaign_keys": preflight_gap_campaign_keys,
        "data_change_authorized_actions": sum(1 for row in plan if row.get("data_change_authorized") is True),
        "execution_ready_without_manual_review": execution_ready_without_manual_review,
        "execution_blocking_gate": execution_blocking_gate,
        "execution_blocking_campaign_keys": execution_blocking_campaign_keys,
        "policy": "Command safety summary is a planning guard. Review-required commands must be inspected before execution and still do not authorize data changes.",
    }
    summary["command_safety_summary_context"] = command_safety_summary_context_for(summary)
    return summary


def evidence_command_for_blocker(campaign_key: str, first_missing_delta: str) -> str:
    commands = {
        "b3": "python scripts/build_b3_masterfile_gap_review.py && python scripts/apply_b3_etf_category_review.py && python scripts/backfill_b3_sector_classification.py && python scripts/build_b3_residual_isin_review.py && python scripts/build_b3_residual_sector_review.py && python scripts/build_coverage_report.py && python scripts/build_improvement_delta_report.py",
        "otc": "python scripts/build_otc_scope_review.py && python scripts/build_otc_name_mismatch_review.py && python scripts/build_improvement_delta_report.py",
        "canada": "python scripts/build_canada_residual_review.py && python scripts/build_canada_figi_queue.py && python scripts/build_improvement_delta_report.py",
        "asx": "python scripts/build_asx_residual_review.py && python scripts/build_improvement_delta_report.py",
        "weak_sector": "python scripts/build_weak_sector_residual_review.py && python scripts/build_improvement_delta_report.py",
        "masterfile_collisions": "python scripts/build_masterfile_collision_review.py && python scripts/build_improvement_delta_report.py",
        "symbol_changes": "python scripts/fetch_symbol_changes.py && python scripts/build_improvement_delta_report.py",
        "ohlcv": "python scripts/build_ohlcv_plausibility_report.py --fetch-yahoo --max-fetch 250 --focus-status source_gap && python scripts/build_improvement_delta_report.py",
        "freshness": "python scripts/fetch_exchange_masterfiles.py && python scripts/build_coverage_report.py && python scripts/build_improvement_delta_report.py",
        "baseline": "python scripts/build_improvement_delta_report.py",
    }
    command = commands.get(campaign_key)
    if command:
        return command
    return f"python scripts/build_improvement_delta_report.py # resolve {first_missing_delta}"


def command_metadata_for_blocker(campaign_key: str, command: str) -> dict[str, Any]:
    network_required = any(marker in command for marker in ("fetch_", "--fetch-yahoo"))
    return {
        "command_mode": "network_evidence_refresh" if network_required else "local_report_rebuild",
        "network_required": network_required,
        "data_change_authorized": False,
        "safety_policy": (
            "Evidence commands refresh source/review reports only. They do not authorize identifier, sector, "
            "category, name, or symbol changes without listing-keyed source review."
        ),
    }


def blocker_context_for(blocker: dict[str, Any]) -> str:
    return (
        f"campaign_key={blocker.get('campaign_key', '') or 'none'};"
        f"blocker_type={blocker.get('blocker_type', '') or 'none'};"
        f"closure_gate={blocker.get('closure_gate', '') or 'none'};"
        f"command_mode={blocker.get('command_mode', '') or 'none'};"
        f"network_required={str(blocker.get('network_required', '')).lower()};"
        f"data_change_authorized={str(blocker.get('data_change_authorized', '')).lower()}"
    )


def summary_context_for(summary: dict[str, Any]) -> str:
    return (
        f"campaigns={summary.get('campaigns', 0)};"
        f"complete_campaigns={summary.get('complete_campaigns', 0)};"
        f"next_review_batches={summary.get('next_review_batches', 0)};"
        f"next_review_batch_rows={summary.get('next_review_batch_rows', 0)};"
        f"closure_ready_campaigns={summary.get('closure_ready_campaigns', 0)};"
        f"closure_blocked_campaigns={summary.get('closure_blocked_campaigns', 0)};"
        f"closure_blockers={summary.get('closure_blockers', 0)};"
        f"validation_failed_error_gates={summary.get('validation_failed_error_gates', 0)}"
    )


def closure_blockers(batches: list[dict[str, Any]]) -> list[dict[str, Any]]:
    blockers: list[dict[str, Any]] = []
    for batch in batches:
        gate = str(batch.get("closure_gate") or "")
        if gate == "review_artifact_rows_must_be_processed_before_campaign_closure":
            continue
        missing = batch.get("missing_delta_evidence", [])
        if not isinstance(missing, list):
            missing = []
        blocker_type = "future_baseline_comparison" if gate.startswith("future_campaign") else "missing_delta_evidence"
        campaign_key = str(batch.get("campaign_key") or "")
        evidence_command = evidence_command_for_blocker(campaign_key, missing[0] if missing else "")
        blocker = {
                "priority": batch.get("priority"),
                "campaign_key": campaign_key,
                "blocker_type": blocker_type,
                "closure_gate": gate,
                "artifact_rows": int(batch.get("artifact_rows") or 0),
                "primary_artifact": batch.get("primary_artifact", ""),
                "missing_delta_evidence": missing,
                "first_missing_delta": missing[0] if missing else "",
                "evidence_command": evidence_command,
                **command_metadata_for_blocker(campaign_key, evidence_command),
                "next_action": batch.get("next_action", ""),
                "recommended_next_source": batch.get("recommended_next_source") or next_review_recommended_source(campaign_key),
                "source_gate": batch.get("source_gate") or next_review_source_gate(campaign_key),
            }
        blocker["blocker_context"] = blocker_context_for(blocker)
        blockers.append(blocker)
    return sorted(
        blockers,
        key=lambda row: (
            1 if row["blocker_type"] == "future_baseline_comparison" else 0,
            row.get("priority") if isinstance(row.get("priority"), int) else 999,
        ),
    )


def build_campaigns(payloads: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    coverage = payloads.get("coverage_report", {})
    coverage_global = coverage.get("global", {})
    b3_diagnostics = coverage.get("b3_masterfile_diagnostics", {})
    if not isinstance(b3_diagnostics, dict):
        b3_diagnostics = {}
    source_gap = payloads.get("source_gap_classification", {})
    source_gap_summary = summary(source_gap)
    freshness_rows = freshness_snapshot(coverage)
    freshness_age_bucket_totals = freshness_snapshot_age_bucket_totals(freshness_rows)
    stale_freshness_rows = top_freshness_snapshot_ages(freshness_rows)
    deltas = payloads.get("improvement_deltas", {})
    b3_artifacts = [
        artifact_entry(REPORTS_DIR / "b3_masterfile_gap_review.json", payloads.get("b3_masterfile_gap_review", {})),
        artifact_entry(REPORTS_DIR / "b3_etf_category_apply_report.json", payloads.get("b3_etf_category_apply_report", {})),
        artifact_entry(REPORTS_DIR / "b3_residual_isin_review.json", payloads.get("b3_residual_isin_review", {})),
        artifact_entry(REPORTS_DIR / "b3_residual_sector_review.json", payloads.get("b3_residual_sector_review", {})),
    ]
    otc_artifacts = [
        artifact_entry(REPORTS_DIR / "otc_scope_review.json", payloads.get("otc_scope_review", {})),
        artifact_entry(REPORTS_DIR / "otc_name_mismatch_review.json", payloads.get("otc_name_mismatch_review", {})),
    ]
    canada_artifacts = [
        artifact_entry(REPORTS_DIR / "canada_residual_review.json", payloads.get("canada_residual_review", {})),
        artifact_entry(REPORTS_DIR / "canada_figi_queue.json", payloads.get("canada_figi_queue", {})),
        artifact_entry(REPORTS_DIR / "canada_figi_apply_report.json", payloads.get("canada_figi_apply_report", {})),
    ]
    asx_artifacts = [artifact_entry(REPORTS_DIR / "asx_residual_review.json", payloads.get("asx_residual_review", {}))]
    weak_sector_artifacts = [
        artifact_entry(REPORTS_DIR / "weak_sector_residual_review.json", payloads.get("weak_sector_residual_review", {})),
        artifact_entry(REPORTS_DIR / "ngx_official_sector_apply_report.json", payloads.get("ngx_official_sector_apply_report", {})),
    ]
    masterfile_artifacts = [artifact_entry(REPORTS_DIR / "masterfile_collision_review.json", payloads.get("masterfile_collision_review", {}))]
    symbol_change_artifacts = [artifact_entry(REPORTS_DIR / "symbol_changes_review.json", payloads.get("symbol_changes_review", {}))]
    ohlcv_artifacts = [
        artifact_entry(REPORTS_DIR / "ohlcv_plausibility.json", payloads.get("ohlcv_plausibility", {})),
        artifact_entry(REPORTS_DIR / "ohlcv_warning_review.json", payloads.get("ohlcv_warning_review", {})),
    ]
    freshness_artifacts = [
        artifact_entry(REPORTS_DIR / "coverage_report.json", payloads.get("coverage_report", {})),
        artifact_entry(REPORTS_DIR / "source_gap_classification.json", source_gap),
        artifact_entry(
            REPORTS_DIR / "financialdata_isin_supplements_review.json",
            payloads.get("financialdata_isin_supplements_review", {}),
        ),
    ]
    baseline_artifacts = [
        artifact_entry(REPORTS_DIR / "improvement_baseline.json", payloads.get("improvement_baseline", {})),
        artifact_entry(REPORTS_DIR / "improvement_deltas.json", payloads.get("improvement_deltas", {})),
    ]
    b3_acceptance_matrix = acceptance_matrix(
        campaign_key="b3",
        exchanges=["B3"],
        artifacts=b3_artifacts,
        deltas=deltas,
    )
    b3_global_deltas = b3_acceptance_matrix.get("global_acceptance_deltas", {})
    if not isinstance(b3_global_deltas, dict):
        b3_global_deltas = {}
    otc_acceptance_matrix = acceptance_matrix(
        campaign_key="otc",
        exchanges=["OTC"],
        artifacts=otc_artifacts,
        deltas=deltas,
    )
    otc_global_deltas = otc_acceptance_matrix.get("global_acceptance_deltas", {})
    if not isinstance(otc_global_deltas, dict):
        otc_global_deltas = {}
    canada_acceptance_matrix = acceptance_matrix(
        campaign_key="canada",
        exchanges=["TSX", "TSXV", "NEO"],
        artifacts=canada_artifacts,
        deltas=deltas,
    )
    canada_global_deltas = canada_acceptance_matrix.get("global_acceptance_deltas", {})
    if not isinstance(canada_global_deltas, dict):
        canada_global_deltas = {}
    campaigns = [
        {
            "priority": 1,
            "name": "B3 official coverage, ISIN and sector residuals",
            "campaign_key": "b3",
            "status": "partially_improved_with_residual_source_gaps",
            "evidence": {
                "official_masterfile_symbols": coverage_global.get("official_masterfile_symbols", 0),
                "b3_dataset_rows": b3_diagnostics.get("dataset_rows", 0),
                "b3_active_exchange_directory_rows": b3_diagnostics.get("active_exchange_directory_rows", 0),
                "b3_all_masterfile_rows": b3_diagnostics.get("all_b3_masterfile_rows", 0),
                "b3_masterfile_matched_dataset_rows": b3_diagnostics.get("matched_dataset_rows", 0),
                "b3_masterfile_missing_dataset_rows": b3_diagnostics.get("missing_dataset_rows", 0),
                "b3_masterfile_dataset_match_rate": b3_diagnostics.get("dataset_match_rate"),
                "b3_official_any_source_matched_dataset_rows": b3_diagnostics.get("official_any_source_matched_dataset_rows", 0),
                "b3_official_any_source_missing_dataset_rows": b3_diagnostics.get("official_any_source_missing_dataset_rows", 0),
                "b3_official_any_source_match_rate": b3_diagnostics.get("official_any_source_match_rate"),
                "b3_missing_category_totals": b3_diagnostics.get("missing_category_totals", {}),
                "b3_missing_asset_type_totals": b3_diagnostics.get("missing_asset_type_totals", {}),
                "b3_missing_source_presence_totals": b3_diagnostics.get("missing_source_presence_totals", {}),
                "b3_missing_examples": b3_diagnostics.get("missing_examples", {}),
                "b3_masterfile_gap_review_rows": row_count(payloads.get("b3_masterfile_gap_review", {})),
                "b3_masterfile_gap_review_open_rows": summary(payloads.get("b3_masterfile_gap_review", {})).get("open_review_rows", 0),
                "b3_masterfile_gap_review_closed_no_data_change_rows": summary(
                    payloads.get("b3_masterfile_gap_review", {})
                ).get("closed_no_data_change_rows", 0),
                "b3_masterfile_gap_review_source_presence_totals": summary(payloads.get("b3_masterfile_gap_review", {})).get("source_presence_totals", {}),
                "b3_masterfile_gap_review_open_source_presence_totals": summary(
                    payloads.get("b3_masterfile_gap_review", {})
                ).get("open_review_source_presence_totals", {}),
                "b3_masterfile_gap_review_bucket_totals": summary(payloads.get("b3_masterfile_gap_review", {})).get("review_bucket_totals", {}),
                "b3_masterfile_gap_review_resolution_queue_totals": summary(payloads.get("b3_masterfile_gap_review", {})).get("b3_resolution_queue_totals", {}),
                "b3_masterfile_gap_review_open_resolution_queue_totals": summary(
                    payloads.get("b3_masterfile_gap_review", {})
                ).get("open_review_resolution_queue_totals", {}),
                "b3_masterfile_gap_review_open_next_source_totals": summary(
                    payloads.get("b3_masterfile_gap_review", {})
                ).get("open_review_next_source_totals", {}),
                "b3_masterfile_gap_review_open_evidence_path_totals": summary(
                    payloads.get("b3_masterfile_gap_review", {})
                ).get("open_review_evidence_path_totals", {}),
                "b3_masterfile_gap_review_source_gap_resolution_gate_totals": summary(
                    payloads.get("b3_masterfile_gap_review", {})
                ).get("source_gap_resolution_gate_totals", {}),
                "top_open_b3_masterfile_review_rows": summary(payloads.get("b3_masterfile_gap_review", {})).get(
                    "top_open_b3_masterfile_review_rows", []
                ),
                "b3_masterfile_gap_review_strategy_totals": summary(payloads.get("b3_masterfile_gap_review", {})).get("review_strategy_totals", {}),
                "b3_masterfile_gap_review_resolution_queue_asset_type_totals": summary(
                    payloads.get("b3_masterfile_gap_review", {})
                ).get("b3_resolution_queue_asset_type_totals", {}),
                "b3_masterfile_gap_review_resolution_queue_gap_category_totals": summary(
                    payloads.get("b3_masterfile_gap_review", {})
                ).get("b3_resolution_queue_gap_category_totals", {}),
                "b3_masterfile_gap_review_candidate_source_totals": summary(payloads.get("b3_masterfile_gap_review", {})).get("candidate_source_totals", {}),
                "b3_masterfile_gap_review_candidate_sector_present_rows": summary(payloads.get("b3_masterfile_gap_review", {})).get("candidate_sector_present_rows", 0),
                "b3_masterfile_gap_review_candidate_isin_present_rows": summary(payloads.get("b3_masterfile_gap_review", {})).get("candidate_isin_present_rows", 0),
                "b3_masterfile_gap_review_candidate_category_review_decision_totals": summary(payloads.get("b3_masterfile_gap_review", {})).get("candidate_category_review_decision_totals", {}),
                "b3_masterfile_gap_review_candidate_category_mismatch_rows": summary(payloads.get("b3_masterfile_gap_review", {})).get("candidate_category_mismatch_rows", 0),
                "b3_masterfile_gap_review_candidate_category_mismatch_examples": summary(payloads.get("b3_masterfile_gap_review", {})).get("candidate_category_mismatch_examples", []),
                "b3_masterfile_gap_review_coverage_diagnosis": summary(payloads.get("b3_masterfile_gap_review", {})).get("coverage_diagnosis", {}),
                "b3_masterfile_gap_review_official_subset_review_decision_totals": summary(
                    payloads.get("b3_masterfile_gap_review", {})
                ).get("official_subset_review_decision_totals", {}),
                "b3_masterfile_gap_review_official_subset_closure_eligibility_totals": summary(
                    payloads.get("b3_masterfile_gap_review", {})
                ).get("official_subset_closure_eligibility_totals", {}),
                "b3_masterfile_gap_review_official_subset_closure_ready_rows": summary(
                    payloads.get("b3_masterfile_gap_review", {})
                ).get("official_subset_closure_ready_rows", 0),
                "top_b3_masterfile_gap_review_batches": summary(payloads.get("b3_masterfile_gap_review", {})).get("top_b3_masterfile_gap_review_batches", []),
                "top_open_b3_masterfile_review_batches": summary(payloads.get("b3_masterfile_gap_review", {})).get(
                    "top_open_b3_masterfile_review_batches", []
                ),
                "b3_etf_category_apply_rows": summary(payloads.get("b3_etf_category_apply_report", {})).get("rows", 0),
                "b3_etf_category_written_updates": summary(payloads.get("b3_etf_category_apply_report", {})).get("written_updates", 0),
                "b3_etf_category_apply_decision_totals": summary(payloads.get("b3_etf_category_apply_report", {})).get("decision_totals", {}),
                "b3_etf_category_update_totals": summary(payloads.get("b3_etf_category_apply_report", {})).get("category_update_totals", {}),
                "b3_missing_isin_residual_rows": row_count(payloads.get("b3_residual_isin_review", {})),
                "b3_missing_sector_residual_rows": row_count(payloads.get("b3_residual_sector_review", {})),
                "isin_review_bucket_totals": summary(payloads.get("b3_residual_isin_review", {})).get("review_bucket_totals", {}),
                "isin_review_priority_totals": summary(payloads.get("b3_residual_isin_review", {})).get("review_priority_totals", {}),
                "isin_apply_eligibility_totals": summary(payloads.get("b3_residual_isin_review", {})).get("apply_eligibility_totals", {}),
                "isin_verification_evidence_required_totals": summary(payloads.get("b3_residual_isin_review", {})).get("verification_evidence_required_totals", {}),
                "isin_review_strategy_totals": summary(payloads.get("b3_residual_isin_review", {})).get("review_strategy_totals", {}),
                "top_b3_isin_review_batches": summary(payloads.get("b3_residual_isin_review", {})).get("top_b3_isin_review_batches", []),
                "b3_isin_official_source_identifier_exposure": summary(
                    payloads.get("b3_residual_isin_review", {})
                ).get("b3_official_source_identifier_exposure", {}),
                "sector_review_bucket_totals": summary(payloads.get("b3_residual_sector_review", {})).get("review_bucket_totals", {}),
                "sector_review_priority_totals": summary(payloads.get("b3_residual_sector_review", {})).get("review_priority_totals", {}),
                "sector_apply_eligibility_totals": summary(payloads.get("b3_residual_sector_review", {})).get("apply_eligibility_totals", {}),
                "sector_verification_evidence_required_totals": summary(payloads.get("b3_residual_sector_review", {})).get("verification_evidence_required_totals", {}),
                "sector_review_strategy_totals": summary(payloads.get("b3_residual_sector_review", {})).get("review_strategy_totals", {}),
                "top_b3_sector_review_batches": summary(payloads.get("b3_residual_sector_review", {})).get("top_b3_sector_review_batches", []),
                "sector_b3_code_shape_totals": summary(payloads.get("b3_residual_sector_review", {})).get("b3_code_shape_totals", {}),
                "sector_alphanumeric_b3_code_rows": summary(payloads.get("b3_residual_sector_review", {})).get("alphanumeric_b3_code_rows", 0),
                "sector_alphanumeric_b3_code_examples": summary(payloads.get("b3_residual_sector_review", {})).get("alphanumeric_b3_code_examples", []),
                "b3_residual_workstream_rows": b3_residual_workstream_rows(payloads),
                "b3_residual_workstream_priority_totals": b3_residual_workstream_priority_totals(payloads),
                "b3_residual_workstream_readiness_totals": b3_residual_workstream_readiness_totals(payloads),
            },
            "artifacts": b3_artifacts,
            "acceptance_matrix": b3_acceptance_matrix,
            "delta_evidence": delta_evidence(
                "partial",
                {
                    "current_b3_masterfile_missing_dataset_rows": b3_diagnostics.get("missing_dataset_rows", 0),
                    "current_b3_masterfile_dataset_match_rate": b3_diagnostics.get("dataset_match_rate"),
                    "current_b3_official_any_source_missing_dataset_rows": b3_diagnostics.get("official_any_source_missing_dataset_rows", 0),
                    "current_b3_official_any_source_match_rate": b3_diagnostics.get("official_any_source_match_rate"),
                    "current_b3_masterfile_gap_review_rows": row_count(payloads.get("b3_masterfile_gap_review", {})),
                    "current_b3_masterfile_gap_review_open_rows": summary(
                        payloads.get("b3_masterfile_gap_review", {})
                    ).get("open_review_rows", 0),
                    "current_b3_etf_category_written_updates": summary(payloads.get("b3_etf_category_apply_report", {})).get("written_updates", 0),
                    "current_b3_missing_isin_residual_rows": row_count(payloads.get("b3_residual_isin_review", {})),
                    "current_b3_missing_sector_residual_rows": row_count(payloads.get("b3_residual_sector_review", {})),
                    "current_b3_sector_alphanumeric_code_rows": summary(payloads.get("b3_residual_sector_review", {})).get("alphanumeric_b3_code_rows", 0),
                    "campaign_start_coverage_snapshot": b3_acceptance_matrix.get("campaign_metric_deltas", {}),
                    "source_gap_delta": b3_global_deltas.get("source_gap_delta", {}),
                    "warn_quarantine_delta": {
                        "warn_delta": b3_global_deltas.get("warn_delta", {}).get("delta", 0),
                        "quarantine_delta": b3_global_deltas.get("quarantine_delta", {}).get("delta", 0),
                    },
                },
                [],
            ),
            "next_action": "refresh official B3 sources and only apply residual ISIN/sector values with exact official identifier or taxonomy evidence",
        },
        {
            "priority": 2,
            "name": "OTC scope review",
            "campaign_key": "otc",
            "status": "scoped_as_extended_with_source_gaps_documented",
            "evidence": {
                "otc_review_rows": row_count(payloads.get("otc_scope_review", {})),
                "source_gap_rows": summary(payloads.get("otc_scope_review", {})).get("source_of_truth_outcome_totals", {}).get("accepted_source_gap", 0),
                "drop_override_rows_still_present": summary(payloads.get("otc_scope_review", {})).get("drop_override_rows_still_present", 0),
                "otc_review_decision_active_name_mismatch_rows": summary(payloads.get("otc_scope_review", {})).get("otc_review_decision_active_name_mismatch_rows", 0),
                "otc_name_mismatch_unreviewed_active_rows": summary(payloads.get("otc_scope_review", {})).get("otc_name_mismatch_unreviewed_active_rows", 0),
                "otc_review_decision_resolution_totals": summary(payloads.get("otc_scope_review", {})).get("otc_review_decision_resolution_totals", {}),
                "otc_review_decision_current_listing_suppressed_rows": summary(payloads.get("otc_scope_review", {})).get("otc_review_decision_current_listing_suppressed_rows", 0),
                "otc_review_decision_not_current_scope_rows": summary(payloads.get("otc_scope_review", {})).get("otc_review_decision_not_current_scope_rows", 0),
                "otc_review_decision_stale_rows": summary(payloads.get("otc_scope_review", {})).get("otc_review_decision_stale_rows", 0),
                "review_bucket_totals": summary(payloads.get("otc_scope_review", {})).get("review_bucket_totals", {}),
                "review_bucket_asset_type_totals": summary(payloads.get("otc_scope_review", {})).get(
                    "review_bucket_asset_type_totals", {}
                ),
                "review_bucket_metadata_gate_totals": summary(payloads.get("otc_scope_review", {})).get(
                    "review_bucket_metadata_gate_totals", {}
                ),
                "review_priority_totals": summary(payloads.get("otc_scope_review", {})).get("review_priority_totals", {}),
                "scope_review_strategy_totals": summary(payloads.get("otc_scope_review", {})).get("review_strategy_totals", {}),
                "scope_verification_evidence_required_totals": summary(payloads.get("otc_scope_review", {})).get(
                    "verification_evidence_required_totals", {}
                ),
                "top_otc_scope_review_batches": summary(payloads.get("otc_scope_review", {})).get("top_otc_scope_review_batches", []),
                "otc_core_exclusion_candidate_rows": summary(payloads.get("otc_scope_review", {})).get(
                    "otc_core_exclusion_candidate_rows", 0
                ),
                "otc_core_exclusion_candidate_asset_type_totals": summary(payloads.get("otc_scope_review", {})).get(
                    "otc_core_exclusion_candidate_asset_type_totals", {}
                ),
                "otc_core_exclusion_candidate_metadata_gate_totals": summary(payloads.get("otc_scope_review", {})).get(
                    "otc_core_exclusion_candidate_metadata_gate_totals", {}
                ),
                "otc_core_exclusion_candidate_review_examples": summary(payloads.get("otc_scope_review", {})).get(
                    "otc_core_exclusion_candidate_review_examples", []
                ),
                "scope_apply_eligibility_totals": summary(payloads.get("otc_scope_review", {})).get("scope_apply_eligibility_totals", {}),
                "otc_scope_completion": summary(payloads.get("otc_scope_review", {})).get("otc_scope_completion", {}),
                "post_scope_metadata_backlog": summary(payloads.get("otc_scope_review", {})).get("post_scope_metadata_backlog", {}),
                "post_scope_metadata_backlog_bucket_totals": summary(payloads.get("otc_scope_review", {})).get(
                    "post_scope_metadata_backlog_bucket_totals", {}
                ),
                "post_scope_metadata_backlog_gate_totals": summary(payloads.get("otc_scope_review", {})).get(
                    "post_scope_metadata_backlog_gate_totals", {}
                ),
                "post_scope_metadata_backlog_examples": summary(payloads.get("otc_scope_review", {})).get(
                    "post_scope_metadata_backlog_examples", []
                ),
                "metadata_enrichment_gate_totals": summary(payloads.get("otc_scope_review", {})).get("metadata_enrichment_gate_totals", {}),
                "source_gap_field_totals": summary(payloads.get("otc_scope_review", {})).get("source_gap_field_totals", {}),
                "source_gap_class_totals": summary(payloads.get("otc_scope_review", {})).get("source_gap_class_totals", {}),
                "source_of_truth_outcome_totals": summary(payloads.get("otc_scope_review", {})).get("source_of_truth_outcome_totals", {}),
                "name_mismatch_review_rows": row_count(payloads.get("otc_name_mismatch_review", {})),
                "name_mismatch_class_counts": summary(payloads.get("otc_name_mismatch_review", {})).get("review_class_counts", {}),
                "name_mismatch_priority_counts": summary(payloads.get("otc_name_mismatch_review", {})).get("review_priority_counts", {}),
                "name_mismatch_review_strategy_counts": summary(payloads.get("otc_name_mismatch_review", {})).get("review_strategy_counts", {}),
                "top_otc_name_mismatch_review_batches": summary(payloads.get("otc_name_mismatch_review", {})).get(
                    "top_otc_name_mismatch_review_batches", []
                ),
                "name_mismatch_apply_eligibility_counts": summary(payloads.get("otc_name_mismatch_review", {})).get(
                    "apply_eligibility_counts", {}
                ),
                "name_mismatch_verification_evidence_required_counts": summary(
                    payloads.get("otc_name_mismatch_review", {})
                ).get("verification_evidence_required_counts", {}),
            },
            "artifacts": otc_artifacts,
            "acceptance_matrix": otc_acceptance_matrix,
            "delta_evidence": delta_evidence(
                "partial",
                {
                    "drop_override_rows_still_present": summary(payloads.get("otc_scope_review", {})).get("drop_override_rows_still_present", 0),
                    "accepted_source_gap_rows": summary(payloads.get("otc_scope_review", {})).get("source_of_truth_outcome_totals", {}).get("accepted_source_gap", 0),
                    "otc_core_exclusion_candidate_rows": summary(payloads.get("otc_scope_review", {})).get(
                        "otc_core_exclusion_candidate_rows", 0
                    ),
                    "otc_scope_completion": summary(payloads.get("otc_scope_review", {})).get("otc_scope_completion", {}),
                    "active_name_mismatch_review_rows": row_count(payloads.get("otc_name_mismatch_review", {})),
                    "otc_name_mismatch_unreviewed_active_rows": summary(payloads.get("otc_scope_review", {})).get("otc_name_mismatch_unreviewed_active_rows", 0),
                    "otc_review_decision_current_listing_suppressed_rows": summary(payloads.get("otc_scope_review", {})).get("otc_review_decision_current_listing_suppressed_rows", 0),
                    "otc_review_decision_not_current_scope_rows": summary(payloads.get("otc_scope_review", {})).get("otc_review_decision_not_current_scope_rows", 0),
                    "otc_review_decision_stale_rows": summary(payloads.get("otc_scope_review", {})).get("otc_review_decision_stale_rows", 0),
                    "campaign_start_scope_snapshot": otc_acceptance_matrix.get("campaign_metric_deltas", {}),
                    "warn_quarantine_delta": {
                        "warn_delta": otc_global_deltas.get("warn_delta", {}).get("delta", 0),
                        "quarantine_delta": otc_global_deltas.get("quarantine_delta", {}).get("delta", 0),
                    },
                },
                [],
            ),
            "next_action": "keep OTC enrichment gated by issuer/product evidence; do not sector-fill extended OTC rows from symbol or name shape",
        },
        {
            "priority": 3,
            "name": "Canada ISIN/FIGI review",
            "campaign_key": "canada",
            "status": "figi_queue_drained_remaining_isin_first_gaps",
            "evidence": {
                "canada_residual_rows": row_count(payloads.get("canada_residual_review", {})),
                "active_figi_queue_rows": row_count(payloads.get("canada_figi_queue", {})),
                "missing_isin_rows": summary(payloads.get("canada_residual_review", {})).get("missing_isin_rows", 0),
                "missing_figi_rows": summary(payloads.get("canada_residual_review", {})).get("missing_figi_rows", 0),
                "canada_identifier_backlog": summary(payloads.get("canada_residual_review", {})).get(
                    "canada_identifier_backlog", {}
                ),
                "canada_identifier_backlog_queue_totals": summary(payloads.get("canada_residual_review", {})).get(
                    "canada_identifier_backlog_queue_totals", {}
                ),
                "canada_identifier_backlog_evidence_required_totals": summary(
                    payloads.get("canada_residual_review", {})
                ).get("canada_identifier_backlog_evidence_required_totals", {}),
                "canada_core_exclusion_candidate_rows": summary(payloads.get("canada_residual_review", {})).get(
                    "core_exclusion_candidate_rows", 0
                ),
                "canada_core_exclusion_candidate_exchange_totals": summary(payloads.get("canada_residual_review", {})).get(
                    "core_exclusion_candidate_exchange_totals", {}
                ),
                "canada_core_exclusion_candidate_asset_type_totals": summary(payloads.get("canada_residual_review", {})).get(
                    "core_exclusion_candidate_asset_type_totals", {}
                ),
                "canada_core_exclusion_candidate_resolution_queue_totals": summary(
                    payloads.get("canada_residual_review", {})
                ).get("core_exclusion_candidate_resolution_queue_totals", {}),
                "canada_core_exclusion_candidate_official_source_totals": summary(
                    payloads.get("canada_residual_review", {})
                ).get("core_exclusion_candidate_official_source_totals", {}),
                "canada_core_exclusion_candidate_source_gap_class_totals": summary(
                    payloads.get("canada_residual_review", {})
                ).get("core_exclusion_candidate_source_gap_class_totals", {}),
                "exchange_totals": summary(payloads.get("canada_residual_review", {})).get("exchange_totals", {}),
                "official_masterfile_source_totals": summary(payloads.get("canada_residual_review", {})).get(
                    "official_masterfile_source_totals", {}
                ),
                "canada_resolution_queue_totals": summary(payloads.get("canada_residual_review", {})).get(
                    "canada_resolution_queue_totals", {}
                ),
                "canada_resolution_queue_exchange_totals": summary(payloads.get("canada_residual_review", {})).get(
                    "canada_resolution_queue_exchange_totals", {}
                ),
                "canada_resolution_queue_asset_type_totals": summary(payloads.get("canada_residual_review", {})).get(
                    "canada_resolution_queue_asset_type_totals", {}
                ),
                "canada_resolution_queue_source_gap_class_totals": summary(payloads.get("canada_residual_review", {})).get(
                    "canada_resolution_queue_source_gap_class_totals", {}
                ),
                "canada_resolution_queue_official_source_totals": summary(payloads.get("canada_residual_review", {})).get(
                    "canada_resolution_queue_official_source_totals", {}
                ),
                "canada_resolution_queue_review_strategy_totals": summary(payloads.get("canada_residual_review", {})).get(
                    "canada_resolution_queue_review_strategy_totals", {}
                ),
                "canada_resolution_queue_evidence_required_totals": summary(
                    payloads.get("canada_residual_review", {})
                ).get("canada_resolution_queue_evidence_required_totals", {}),
                "top_canada_resolution_review_batches": summary(payloads.get("canada_residual_review", {})).get(
                    "top_canada_resolution_review_batches", []
                ),
                "source_gap_field_totals": summary(payloads.get("canada_residual_review", {})).get(
                    "source_gap_field_totals", {}
                ),
                "source_gap_class_totals": summary(payloads.get("canada_residual_review", {})).get(
                    "source_gap_class_totals", {}
                ),
                "source_of_truth_outcome_totals": summary(payloads.get("canada_residual_review", {})).get(
                    "source_of_truth_outcome_totals", {}
                ),
                "reviewed_openfigi_source_gap_rows": sum(
                    summary(payloads.get("canada_residual_review", {})).get("openfigi_review_status_totals", {}).values()
                ),
                "openfigi_review_status_totals": summary(payloads.get("canada_residual_review", {})).get(
                    "openfigi_review_status_totals", {}
                ),
                "openfigi_review_decision_totals": summary(payloads.get("canada_residual_review", {})).get(
                    "openfigi_review_decision_totals", {}
                ),
                "isin_apply_eligibility_totals": summary(payloads.get("canada_residual_review", {})).get(
                    "isin_apply_eligibility_totals", {}
                ),
                "figi_apply_eligibility_totals": summary(payloads.get("canada_residual_review", {})).get(
                    "figi_apply_eligibility_totals", {}
                ),
                "verification_evidence_required_totals": summary(payloads.get("canada_residual_review", {})).get(
                    "verification_evidence_required_totals", {}
                ),
                "figi_queue_apply_eligibility_totals": summary(payloads.get("canada_figi_queue", {})).get(
                    "apply_eligibility_totals", {}
                ),
                "figi_queue_verification_evidence_required_totals": summary(payloads.get("canada_figi_queue", {})).get(
                    "verification_evidence_required_totals", {}
                ),
                "figi_queue_review_strategy_totals": summary(payloads.get("canada_figi_queue", {})).get(
                    "review_strategy_totals", {}
                ),
                "top_canada_figi_queue_review_batches": summary(payloads.get("canada_figi_queue", {})).get(
                    "top_canada_figi_queue_review_batches", []
                ),
                "applied_figi_rows": summary(payloads.get("canada_figi_apply_report", {})).get("applied_rows", 0),
                "openfigi_gap_rows_added": summary(payloads.get("canada_figi_apply_report", {})).get("gap_rows_added", 0),
            },
            "artifacts": canada_artifacts,
            "acceptance_matrix": canada_acceptance_matrix,
            "delta_evidence": delta_evidence(
                "present_for_figi_apply",
                {
                    "applied_figi_rows": summary(payloads.get("canada_figi_apply_report", {})).get("applied_rows", 0),
                    "openfigi_gap_rows_added": summary(payloads.get("canada_figi_apply_report", {})).get("gap_rows_added", 0),
                    "active_figi_queue_rows": row_count(payloads.get("canada_figi_queue", {})),
                    "canada_core_exclusion_candidate_rows": summary(payloads.get("canada_residual_review", {})).get(
                        "core_exclusion_candidate_rows", 0
                    ),
                    "reviewed_openfigi_source_gap_rows": sum(
                        summary(payloads.get("canada_residual_review", {})).get("openfigi_review_status_totals", {}).values()
                    ),
                    "isin_delta": canada_global_deltas.get("isin_delta", {}),
                    "sector_category_delta": {
                        "sector_delta": canada_global_deltas.get("sector_delta", {}),
                        "category_delta": canada_global_deltas.get("category_delta", {}),
                    },
                    "warn_quarantine_delta": {
                        "warn_delta": canada_global_deltas.get("warn_delta", {}).get("delta", 0),
                        "quarantine_delta": canada_global_deltas.get("quarantine_delta", {}).get("delta", 0),
                    },
                },
                [],
            ),
            "next_action": "resolve remaining Canada identifiers through official ISIN-capable sources before any additional FIGI probing",
        },
        {
            "priority": 4,
            "name": "ASX ETF/ISIN residuals",
            "campaign_key": "asx",
            "status": "official_probe_reviewed_residuals_documented",
            "evidence": {
                "asx_residual_rows": row_count(payloads.get("asx_residual_review", {})),
                "field_totals": summary(payloads.get("asx_residual_review", {})).get("field_totals", {}),
                "asset_type_totals": summary(payloads.get("asx_residual_review", {})).get("asset_type_totals", {}),
                "asx_core_exclusion_candidate_rows": summary(payloads.get("asx_residual_review", {})).get(
                    "core_exclusion_candidate_rows", 0
                ),
                "asx_core_exclusion_candidate_field_totals": summary(payloads.get("asx_residual_review", {})).get(
                    "core_exclusion_candidate_field_totals", {}
                ),
                "asx_core_exclusion_candidate_asset_type_totals": summary(payloads.get("asx_residual_review", {})).get(
                    "core_exclusion_candidate_asset_type_totals", {}
                ),
                "asx_core_exclusion_candidate_gap_class_totals": summary(payloads.get("asx_residual_review", {})).get(
                    "core_exclusion_candidate_gap_class_totals", {}
                ),
                "asx_core_exclusion_candidate_resolution_queue_totals": summary(
                    payloads.get("asx_residual_review", {})
                ).get("core_exclusion_candidate_resolution_queue_totals", {}),
                "asx_core_exclusion_candidate_official_source_totals": summary(
                    payloads.get("asx_residual_review", {})
                ).get("core_exclusion_candidate_official_source_totals", {}),
                "asx_core_exclusion_candidate_official_capability_totals": summary(
                    payloads.get("asx_residual_review", {})
                ).get("core_exclusion_candidate_official_capability_totals", {}),
                "gap_class_totals": summary(payloads.get("asx_residual_review", {})).get("gap_class_totals", {}),
                "source_of_truth_outcome_totals": summary(payloads.get("asx_residual_review", {})).get(
                    "source_of_truth_outcome_totals", {}
                ),
                "asx_residual_backlog": summary(payloads.get("asx_residual_review", {})).get(
                    "asx_residual_backlog", {}
                ),
                "asx_residual_backlog_queue_totals": summary(payloads.get("asx_residual_review", {})).get(
                    "asx_residual_backlog_queue_totals", {}
                ),
                "asx_residual_backlog_evidence_required_totals": summary(payloads.get("asx_residual_review", {})).get(
                    "asx_residual_backlog_evidence_required_totals", {}
                ),
                "asx_resolution_queue_totals": summary(payloads.get("asx_residual_review", {})).get(
                    "asx_resolution_queue_totals", {}
                ),
                "asx_resolution_queue_field_totals": summary(payloads.get("asx_residual_review", {})).get(
                    "asx_resolution_queue_field_totals", {}
                ),
                "asx_resolution_queue_gap_class_totals": summary(payloads.get("asx_residual_review", {})).get(
                    "asx_resolution_queue_gap_class_totals", {}
                ),
                "asx_resolution_queue_official_source_totals": summary(payloads.get("asx_residual_review", {})).get(
                    "asx_resolution_queue_official_source_totals", {}
                ),
                "asx_resolution_queue_review_strategy_totals": summary(payloads.get("asx_residual_review", {})).get(
                    "asx_resolution_queue_review_strategy_totals", {}
                ),
                "asx_resolution_queue_evidence_required_totals": summary(payloads.get("asx_residual_review", {})).get(
                    "asx_resolution_queue_evidence_required_totals", {}
                ),
                "asx_resolution_queue_official_capability_totals": summary(
                    payloads.get("asx_residual_review", {})
                ).get("asx_resolution_queue_official_capability_totals", {}),
                "top_asx_resolution_review_batches": summary(payloads.get("asx_residual_review", {})).get(
                    "top_asx_resolution_review_batches", []
                ),
                "residual_decision_totals": summary(payloads.get("asx_residual_review", {})).get("residual_decision_totals", {}),
                "review_bucket_totals": summary(payloads.get("asx_residual_review", {})).get("review_bucket_totals", {}),
                "review_priority_totals": summary(payloads.get("asx_residual_review", {})).get("review_priority_totals", {}),
                "apply_eligibility_totals": summary(payloads.get("asx_residual_review", {})).get("apply_eligibility_totals", {}),
                "verification_evidence_required_totals": summary(payloads.get("asx_residual_review", {})).get("verification_evidence_required_totals", {}),
                "asx_isin_probe_decision_totals": summary(payloads.get("asx_residual_review", {})).get(
                    "asx_isin_probe_decision_totals", {}
                ),
                "official_masterfile_match_totals": summary(payloads.get("asx_residual_review", {})).get(
                    "official_masterfile_match_totals", {}
                ),
                "official_masterfile_exposes_isin_totals": summary(payloads.get("asx_residual_review", {})).get(
                    "official_masterfile_exposes_isin_totals", {}
                ),
                "official_masterfile_exposes_sector_totals": summary(payloads.get("asx_residual_review", {})).get(
                    "official_masterfile_exposes_sector_totals", {}
                ),
                "official_masterfile_source_totals": summary(payloads.get("asx_residual_review", {})).get(
                    "official_masterfile_source_totals", {}
                ),
            },
            "artifacts": asx_artifacts,
            "acceptance_matrix": acceptance_matrix(campaign_key="asx", exchanges=["ASX"], artifacts=asx_artifacts, deltas=deltas),
            "delta_evidence": delta_evidence(
                "review_only_no_data_apply",
                {
                    "current_asx_residual_rows": row_count(payloads.get("asx_residual_review", {})),
                    "asx_core_exclusion_candidate_rows": summary(payloads.get("asx_residual_review", {})).get(
                        "core_exclusion_candidate_rows", 0
                    ),
                    "campaign_start_residual_snapshot": campaign_metric_deltas(deltas, "asx").get("residual_rows", {}),
                    "source_gap_delta": acceptance_matrix(
                        campaign_key="asx",
                        exchanges=["ASX"],
                        artifacts=asx_artifacts,
                        deltas=deltas,
                    ).get("global_acceptance_deltas", {}).get("source_gap_delta", {}),
                    "warn_quarantine_delta": {
                        "warn_delta": acceptance_matrix(
                            campaign_key="asx",
                            exchanges=["ASX"],
                            artifacts=asx_artifacts,
                            deltas=deltas,
                        ).get("global_acceptance_deltas", {}).get("warn_delta", {}).get("delta", 0),
                        "quarantine_delta": acceptance_matrix(
                            campaign_key="asx",
                            exchanges=["ASX"],
                            artifacts=asx_artifacts,
                            deltas=deltas,
                        ).get("global_acceptance_deltas", {}).get("quarantine_delta", {}).get("delta", 0),
                    },
                },
            ),
            "next_action": "only apply ASX identifiers or categories after exact ASX workbook/listing/name gates pass",
        },
        {
            "priority": 5,
            "name": "Weak sector venue residuals",
            "campaign_key": "weak_sector",
            "status": "venue_specific_review_queue_with_safe_ngx_apply",
            "evidence": {
                "weak_sector_rows": row_count(payloads.get("weak_sector_residual_review", {})),
                "exchanges": summary(payloads.get("weak_sector_residual_review", {})).get("exchanges", []),
                "exchange_totals": summary(payloads.get("weak_sector_residual_review", {})).get("exchange_totals", {}),
                "top_exchange_residuals": top_counts(summary(payloads.get("weak_sector_residual_review", {})).get("exchange_totals", {})),
                "official_sector_candidate_rows": summary(payloads.get("weak_sector_residual_review", {})).get(
                    "official_sector_candidate_rows", 0
                ),
                "official_sector_candidate_exchange_totals": summary(payloads.get("weak_sector_residual_review", {})).get(
                    "official_sector_candidate_exchange_totals", {}
                ),
                "official_sector_candidate_value_totals": summary(payloads.get("weak_sector_residual_review", {})).get(
                    "official_sector_candidate_value_totals", {}
                ),
                "scope_review_rows": summary(payloads.get("weak_sector_residual_review", {})).get("scope_review_rows", 0),
                "scope_review_exchange_totals": summary(payloads.get("weak_sector_residual_review", {})).get(
                    "scope_review_exchange_totals", {}
                ),
                "scope_review_gap_class_totals": summary(payloads.get("weak_sector_residual_review", {})).get(
                    "scope_review_gap_class_totals", {}
                ),
                "masterfile_without_sector_rows": summary(payloads.get("weak_sector_residual_review", {})).get(
                    "masterfile_without_sector_rows", 0
                ),
                "masterfile_without_sector_exchange_totals": summary(payloads.get("weak_sector_residual_review", {})).get(
                    "masterfile_without_sector_exchange_totals", {}
                ),
                "gap_class_totals": summary(payloads.get("weak_sector_residual_review", {})).get("gap_class_totals", {}),
                "source_of_truth_outcome_totals": summary(payloads.get("weak_sector_residual_review", {})).get(
                    "source_of_truth_outcome_totals", {}
                ),
                "weak_sector_backlog": summary(payloads.get("weak_sector_residual_review", {})).get(
                    "weak_sector_backlog", {}
                ),
                "weak_sector_backlog_queue_totals": summary(payloads.get("weak_sector_residual_review", {})).get(
                    "weak_sector_backlog_queue_totals", {}
                ),
                "weak_sector_backlog_evidence_required_totals": summary(
                    payloads.get("weak_sector_residual_review", {})
                ).get("weak_sector_backlog_evidence_required_totals", {}),
                "weak_sector_resolution_queue_totals": summary(payloads.get("weak_sector_residual_review", {})).get(
                    "weak_sector_resolution_queue_totals", {}
                ),
                "weak_sector_resolution_queue_exchange_totals": summary(payloads.get("weak_sector_residual_review", {})).get(
                    "weak_sector_resolution_queue_exchange_totals", {}
                ),
                "weak_sector_resolution_queue_gap_class_totals": summary(payloads.get("weak_sector_residual_review", {})).get(
                    "weak_sector_resolution_queue_gap_class_totals", {}
                ),
                "weak_sector_resolution_queue_official_source_totals": summary(
                    payloads.get("weak_sector_residual_review", {})
                ).get("weak_sector_resolution_queue_official_source_totals", {}),
                "weak_sector_resolution_queue_review_strategy_totals": summary(
                    payloads.get("weak_sector_residual_review", {})
                ).get("weak_sector_resolution_queue_review_strategy_totals", {}),
                "weak_sector_resolution_queue_official_capability_totals": summary(
                    payloads.get("weak_sector_residual_review", {})
                ).get("weak_sector_resolution_queue_official_capability_totals", {}),
                "venue_backlog_exchange_queue_totals": summary(payloads.get("weak_sector_residual_review", {})).get(
                    "venue_backlog_exchange_queue_totals", {}
                ),
                "venue_backlog_exchange_official_capability_totals": summary(
                    payloads.get("weak_sector_residual_review", {})
                ).get("venue_backlog_exchange_official_capability_totals", {}),
                "top_venue_backlog_batches": summary(payloads.get("weak_sector_residual_review", {})).get(
                    "top_venue_backlog_batches", []
                ),
                "top_weak_sector_resolution_review_batches": summary(
                    payloads.get("weak_sector_residual_review", {})
                ).get("top_weak_sector_resolution_review_batches", []),
                "residual_decision_totals": summary(payloads.get("weak_sector_residual_review", {})).get(
                    "residual_decision_totals", {}
                ),
                "review_bucket_totals": summary(payloads.get("weak_sector_residual_review", {})).get("review_bucket_totals", {}),
                "review_priority_totals": summary(payloads.get("weak_sector_residual_review", {})).get("review_priority_totals", {}),
                "apply_eligibility_totals": summary(payloads.get("weak_sector_residual_review", {})).get("apply_eligibility_totals", {}),
                "verification_evidence_required_totals": summary(payloads.get("weak_sector_residual_review", {})).get(
                    "verification_evidence_required_totals", {}
                ),
                "official_masterfile_match_totals": summary(payloads.get("weak_sector_residual_review", {})).get(
                    "official_masterfile_match_totals", {}
                ),
                "official_masterfile_exposes_sector_totals": summary(
                    payloads.get("weak_sector_residual_review", {})
                ).get("official_masterfile_exposes_sector_totals", {}),
                "official_masterfile_source_totals": summary(payloads.get("weak_sector_residual_review", {})).get(
                    "official_masterfile_source_totals", {}
                ),
                "official_sector_value_totals": summary(payloads.get("weak_sector_residual_review", {})).get(
                    "official_sector_value_totals", {}
                ),
                "ngx_applied_rows": row_count(payloads.get("ngx_official_sector_apply_report", {})),
                "ngx_written_updates": summary(payloads.get("ngx_official_sector_apply_report", {})).get("written_updates", 0),
            },
            "artifacts": weak_sector_artifacts,
            "acceptance_matrix": acceptance_matrix(
                campaign_key="weak_sector",
                exchanges=["BK", "CSE_MA", "SEM", "PSE", "CSE_LK", "NGX", "OSL", "Euronext"],
                artifacts=weak_sector_artifacts,
                deltas=deltas,
            ),
            "delta_evidence": delta_evidence(
                "partial",
                {
                    "ngx_review_rows": row_count(payloads.get("ngx_official_sector_apply_report", {})),
                    "ngx_written_updates": summary(payloads.get("ngx_official_sector_apply_report", {})).get("written_updates", 0),
                    "current_weak_sector_rows": row_count(payloads.get("weak_sector_residual_review", {})),
                    "official_sector_candidate_rows": summary(payloads.get("weak_sector_residual_review", {})).get(
                        "official_sector_candidate_rows", 0
                    ),
                    "scope_review_rows": summary(payloads.get("weak_sector_residual_review", {})).get("scope_review_rows", 0),
                    "masterfile_without_sector_rows": summary(payloads.get("weak_sector_residual_review", {})).get(
                        "masterfile_without_sector_rows", 0
                    ),
                    "campaign_start_sector_coverage_snapshot": campaign_metric_deltas(
                        deltas, "weak_sector"
                    ).get("residual_rows", {}),
                    "source_gap_delta": acceptance_matrix(
                        campaign_key="weak_sector",
                        exchanges=["BK", "CSE_MA", "SEM", "PSE", "CSE_LK", "NGX", "OSL", "Euronext"],
                        artifacts=weak_sector_artifacts,
                        deltas=deltas,
                    ).get("global_acceptance_deltas", {}).get("source_gap_delta", {}),
                    "warn_quarantine_delta": {
                        "warn_delta": acceptance_matrix(
                            campaign_key="weak_sector",
                            exchanges=["BK", "CSE_MA", "SEM", "PSE", "CSE_LK", "NGX", "OSL", "Euronext"],
                            artifacts=weak_sector_artifacts,
                            deltas=deltas,
                        ).get("global_acceptance_deltas", {}).get("warn_delta", {}).get("delta", 0),
                        "quarantine_delta": acceptance_matrix(
                            campaign_key="weak_sector",
                            exchanges=["BK", "CSE_MA", "SEM", "PSE", "CSE_LK", "NGX", "OSL", "Euronext"],
                            artifacts=weak_sector_artifacts,
                            deltas=deltas,
                        ).get("global_acceptance_deltas", {}).get("quarantine_delta", {}).get("delta", 0),
                    },
                },
            ),
            "next_action": "continue venue-specific official taxonomy work; skip broad NGX labels unless they map cleanly to canonical sectors",
        },
        {
            "priority": 6,
            "name": "Masterfile collision identity review",
            "campaign_key": "masterfile_collisions",
            "status": "listing_keyed_review_queue_ready_no_symbol_only_additions",
            "evidence": {
                "collision_review_rows": row_count(payloads.get("masterfile_collision_review", {})),
                "decision_totals": summary(payloads.get("masterfile_collision_review", {})).get("decision_totals", {}),
                "review_bucket_totals": summary(payloads.get("masterfile_collision_review", {})).get("review_bucket_totals", {}),
                "review_priority_totals": summary(payloads.get("masterfile_collision_review", {})).get("review_priority_totals", {}),
                "collision_risk_flag_totals": summary(payloads.get("masterfile_collision_review", {})).get("collision_risk_flag_totals", {}),
                "identity_resolution_queue_totals": summary(payloads.get("masterfile_collision_review", {})).get(
                    "identity_resolution_queue_totals", {}
                ),
                "identity_resolution_backlog": summary(payloads.get("masterfile_collision_review", {})).get(
                    "identity_resolution_backlog", {}
                ),
                "identity_resolution_risk_flag_totals": summary(payloads.get("masterfile_collision_review", {})).get(
                    "identity_resolution_risk_flag_totals", {}
                ),
                "identity_resolution_exchange_totals": summary(payloads.get("masterfile_collision_review", {})).get(
                    "identity_resolution_exchange_totals", {}
                ),
                "identity_resolution_asset_type_totals": summary(payloads.get("masterfile_collision_review", {})).get(
                    "identity_resolution_asset_type_totals", {}
                ),
                "identity_resolution_official_source_totals": summary(
                    payloads.get("masterfile_collision_review", {})
                ).get("identity_resolution_official_source_totals", {}),
                "identity_resolution_existing_exchange_pair_totals": summary(
                    payloads.get("masterfile_collision_review", {})
                ).get("identity_resolution_existing_exchange_pair_totals", {}),
                "identity_resolution_pair_review_strategy_totals": summary(
                    payloads.get("masterfile_collision_review", {})
                ).get("identity_resolution_pair_review_strategy_totals", {}),
                "identity_resolution_review_strategy_totals": summary(
                    payloads.get("masterfile_collision_review", {})
                ).get("identity_resolution_review_strategy_totals", {}),
                "identity_resolution_evidence_required_totals": summary(
                    payloads.get("masterfile_collision_review", {})
                ).get("identity_resolution_evidence_required_totals", {}),
                "identity_resolution_identity_evidence_totals": summary(
                    payloads.get("masterfile_collision_review", {})
                ).get("identity_resolution_identity_evidence_totals", {}),
                "identity_resolution_clearance_readiness_totals": summary(
                    payloads.get("masterfile_collision_review", {})
                ).get("identity_resolution_clearance_readiness_totals", {}),
                "identity_resolution_queue_clearance_readiness_totals": summary(
                    payloads.get("masterfile_collision_review", {})
                ).get("identity_resolution_queue_clearance_readiness_totals", {}),
                "top_identity_resolution_clearance_batches": summary(
                    payloads.get("masterfile_collision_review", {})
                ).get("top_identity_resolution_clearance_batches", []),
                "top_identity_resolution_pair_review_batches": summary(
                    payloads.get("masterfile_collision_review", {})
                ).get("top_identity_resolution_pair_review_batches", []),
                "same_isin_exact_name_scope_review_rows": summary(
                    payloads.get("masterfile_collision_review", {})
                ).get("same_isin_exact_name_scope_review_rows", 0),
                "top_same_isin_exact_name_scope_review_batches": summary(
                    payloads.get("masterfile_collision_review", {})
                ).get("top_same_isin_exact_name_scope_review_batches", []),
                "clearance_evidence_totals": summary(payloads.get("masterfile_collision_review", {})).get("clearance_evidence_totals", {}),
                "exchange_totals": summary(payloads.get("masterfile_collision_review", {})).get("exchange_totals", {}),
                "official_asset_type_totals": summary(payloads.get("masterfile_collision_review", {})).get(
                    "official_asset_type_totals", {}
                ),
                "asset_type_mismatch_totals": summary(payloads.get("masterfile_collision_review", {})).get(
                    "asset_type_mismatch_totals", {}
                ),
                "official_source_totals": summary(payloads.get("masterfile_collision_review", {})).get(
                    "official_source_totals", {}
                ),
                "asset_type_mismatches": summary(payloads.get("masterfile_collision_review", {})).get("asset_type_mismatch_totals", {}).get("true", 0),
            },
            "artifacts": masterfile_artifacts,
            "acceptance_matrix": acceptance_matrix(campaign_key="masterfile_collisions", exchanges=None, artifacts=masterfile_artifacts, deltas=deltas),
            "delta_evidence": delta_evidence(
                "review_queue_only_no_data_apply",
                {
                    "current_collision_review_rows": row_count(payloads.get("masterfile_collision_review", {})),
                    "collision_resolution_delta": 0,
                    "listing_addition_delta": 0,
                    "warn_quarantine_delta": {
                        "warn_delta": acceptance_matrix(
                            campaign_key="masterfile_collisions",
                            exchanges=None,
                            artifacts=masterfile_artifacts,
                            deltas=deltas,
                        ).get("global_acceptance_deltas", {}).get("warn_delta", {}).get("delta", 0),
                        "quarantine_delta": acceptance_matrix(
                            campaign_key="masterfile_collisions",
                            exchanges=None,
                            artifacts=masterfile_artifacts,
                            deltas=deltas,
                        ).get("global_acceptance_deltas", {}).get("quarantine_delta", {}).get("delta", 0),
                    },
                },
            ),
            "next_action": "process same-ISIN cross-listing candidates first, with exchange/MIC/name/instrument-type review gates",
        },
        {
            "priority": 7,
            "name": "Symbol-change workflow",
            "campaign_key": "symbol_changes",
            "status": "source_scope_aware_review_queue",
            "evidence": {
                "symbol_change_review_rows": summary(payloads.get("symbol_changes_review", {})).get("review_rows", 0),
                "match_status_counts": summary(payloads.get("symbol_changes_review", {})).get("match_status_counts", {}),
                "symbol_change_workflow_queue_counts": summary(payloads.get("symbol_changes_review", {})).get(
                    "symbol_change_workflow_queue_counts", {}
                ),
                "symbol_change_backlog": summary(payloads.get("symbol_changes_review", {})).get(
                    "symbol_change_backlog", {}
                ),
                "review_bucket_counts": summary(payloads.get("symbol_changes_review", {})).get("review_bucket_counts", {}),
                "review_priority_counts": summary(payloads.get("symbol_changes_review", {})).get("review_priority_counts", {}),
                "review_bucket_priorities": summary(payloads.get("symbol_changes_review", {})).get("review_bucket_priorities", {}),
                "recency_bucket_counts": summary(payloads.get("symbol_changes_review", {})).get("recency_bucket_counts", {}),
                "review_priority_recency_counts": summary(payloads.get("symbol_changes_review", {})).get("review_priority_recency_counts", {}),
                "workflow_queue_recency_counts": summary(payloads.get("symbol_changes_review", {})).get("workflow_queue_recency_counts", {}),
                "workflow_queue_priority_counts": summary(payloads.get("symbol_changes_review", {})).get("workflow_queue_priority_counts", {}),
                "workflow_queue_scope_counts": summary(payloads.get("symbol_changes_review", {})).get("workflow_queue_scope_counts", {}),
                "workflow_queue_match_status_counts": summary(payloads.get("symbol_changes_review", {})).get(
                    "workflow_queue_match_status_counts", {}
                ),
                "workflow_queue_source_hint_counts": summary(payloads.get("symbol_changes_review", {})).get(
                    "workflow_queue_source_hint_counts", {}
                ),
                "workflow_queue_source_confidence_counts": summary(payloads.get("symbol_changes_review", {})).get(
                    "workflow_queue_source_confidence_counts", {}
                ),
                "workflow_queue_issuer_name_review_counts": summary(payloads.get("symbol_changes_review", {})).get(
                    "workflow_queue_issuer_name_review_counts", {}
                ),
                "workflow_queue_listing_key_review_counts": summary(payloads.get("symbol_changes_review", {})).get(
                    "workflow_queue_listing_key_review_counts", {}
                ),
                "workflow_queue_review_strategy_counts": summary(payloads.get("symbol_changes_review", {})).get(
                    "workflow_queue_review_strategy_counts", {}
                ),
                "top_symbol_change_workflow_batches": summary(payloads.get("symbol_changes_review", {})).get(
                    "top_symbol_change_workflow_batches", []
                ),
                "apply_eligibility_counts": summary(payloads.get("symbol_changes_review", {})).get("apply_eligibility_counts", {}),
                "symbol_change_apply_readiness_counts": summary(payloads.get("symbol_changes_review", {})).get(
                    "symbol_change_apply_readiness_counts", {}
                ),
                "verification_evidence_required_counts": summary(payloads.get("symbol_changes_review", {})).get(
                    "verification_evidence_required_counts", {}
                ),
                "recommended_action_counts": summary(payloads.get("symbol_changes_review", {})).get(
                    "recommended_action_counts", {}
                ),
                "time_sensitive_review_rows": summary(payloads.get("symbol_changes_review", {})).get("time_sensitive_review_rows", 0),
                "time_sensitive_workflow_queue_counts": summary(payloads.get("symbol_changes_review", {})).get(
                    "time_sensitive_workflow_queue_counts", {}
                ),
                "time_sensitive_recency_counts": summary(payloads.get("symbol_changes_review", {})).get(
                    "time_sensitive_recency_counts", {}
                ),
                "time_sensitive_apply_readiness_counts": summary(payloads.get("symbol_changes_review", {})).get(
                    "time_sensitive_apply_readiness_counts", {}
                ),
                "top_time_sensitive_symbol_change_batches": summary(payloads.get("symbol_changes_review", {})).get(
                    "top_time_sensitive_symbol_change_batches", []
                ),
                "exchange_scope_status_counts": summary(payloads.get("symbol_changes_review", {})).get("exchange_scope_status_counts", {}),
            },
            "artifacts": symbol_change_artifacts,
            "acceptance_matrix": acceptance_matrix(campaign_key="symbol_changes", exchanges=None, artifacts=symbol_change_artifacts, deltas=deltas),
            "delta_evidence": delta_evidence(
                "review_queue_only_no_symbol_changes_applied",
                {
                    "source_scope_outside_collision_rows": summary(payloads.get("symbol_changes_review", {})).get(
                        "exchange_scope_status_counts", {}
                    ).get("global_symbol_collision_outside_source_scope", 0),
                    "verified_rename_delta": 0,
                    "duplicate_resolution_delta": 0,
                    "warn_quarantine_delta": {
                        "warn_delta": acceptance_matrix(
                            campaign_key="symbol_changes",
                            exchanges=None,
                            artifacts=symbol_change_artifacts,
                            deltas=deltas,
                        ).get("global_acceptance_deltas", {}).get("warn_delta", {}).get("delta", 0),
                        "quarantine_delta": acceptance_matrix(
                            campaign_key="symbol_changes",
                            exchanges=None,
                            artifacts=symbol_change_artifacts,
                            deltas=deltas,
                        ).get("global_acceptance_deltas", {}).get("quarantine_delta", {}).get("delta", 0),
                    },
                },
            ),
            "next_action": "review scoped rename candidates against official exchange notices before changing any canonical symbol",
        },
        {
            "priority": 8,
            "name": "OHLCV plausibility sampling",
            "campaign_key": "ohlcv",
            "status": "sampling_queue_enabled_plausibility_only",
            "evidence": {
                "ohlcv_rows": row_count(payloads.get("ohlcv_plausibility", {})),
                "selected_sample_rows": summary(payloads.get("ohlcv_plausibility", {})).get("sampling_coverage", {}).get(
                    "selected_rows", row_count(payloads.get("ohlcv_plausibility", {}))
                ),
                "checked_sample_rows": summary(payloads.get("ohlcv_plausibility", {})).get("sampling_coverage", {}).get(
                    "checked_rows", 0
                ),
                "not_checked_sample_rows": summary(payloads.get("ohlcv_plausibility", {})).get("sampling_coverage", {}).get(
                    "not_checked_rows", 0
                ),
                "sampling_coverage": summary(payloads.get("ohlcv_plausibility", {})).get("sampling_coverage", {}),
                "status_counts": summary(payloads.get("ohlcv_plausibility", {})).get("status_counts", {}),
                "issue_counts": summary(payloads.get("ohlcv_plausibility", {})).get("issue_counts", {}),
                "selection_bucket_counts": summary(payloads.get("ohlcv_plausibility", {})).get("selection_bucket_counts", {}),
                "selection_bucket_exchange_counts": summary(payloads.get("ohlcv_plausibility", {})).get(
                    "selection_bucket_exchange_counts", {}
                ),
                "selection_bucket_status_counts": summary(payloads.get("ohlcv_plausibility", {})).get(
                    "selection_bucket_status_counts", {}
                ),
                "review_bucket_counts": summary(payloads.get("ohlcv_plausibility", {})).get("review_bucket_counts", {}),
                "review_bucket_selection_bucket_counts": summary(payloads.get("ohlcv_plausibility", {})).get(
                    "review_bucket_selection_bucket_counts", {}
                ),
                "review_bucket_exchange_counts": summary(payloads.get("ohlcv_plausibility", {})).get(
                    "review_bucket_exchange_counts", {}
                ),
                "review_bucket_sampling_strategy_counts": summary(payloads.get("ohlcv_plausibility", {})).get(
                    "review_bucket_sampling_strategy_counts", {}
                ),
                "review_bucket_sampling_readiness_counts": summary(payloads.get("ohlcv_plausibility", {})).get(
                    "review_bucket_sampling_readiness_counts", {}
                ),
                "top_ohlcv_sampling_batches": summary(payloads.get("ohlcv_plausibility", {})).get(
                    "top_ohlcv_sampling_batches", []
                ),
                "ohlcv_sampling_backlog": summary(payloads.get("ohlcv_plausibility", {})).get(
                    "ohlcv_sampling_backlog", {}
                ),
                "review_priority_counts": summary(payloads.get("ohlcv_plausibility", {})).get("review_priority_counts", {}),
                "plausibility_use_counts": summary(payloads.get("ohlcv_plausibility", {})).get("plausibility_use_counts", {}),
                "canonical_data_change_authorization_counts": summary(
                    payloads.get("ohlcv_plausibility", {})
                ).get("canonical_data_change_authorization_counts", {}),
                "verification_evidence_required_counts": summary(payloads.get("ohlcv_plausibility", {})).get("verification_evidence_required_counts", {}),
                "source_gap_class_counts": summary(payloads.get("ohlcv_plausibility", {})).get("source_gap_class_counts", {}),
                "top_flagged_exchanges": summary(payloads.get("ohlcv_plausibility", {})).get("top_flagged_exchanges", []),
                "ohlcv_warning_review_rows": row_count(payloads.get("ohlcv_warning_review", {})),
                "ohlcv_warning_review_bucket_counts": summary(payloads.get("ohlcv_warning_review", {})).get(
                    "ohlcv_review_bucket_counts", {}
                ),
                "ohlcv_warning_review_priority_counts": summary(payloads.get("ohlcv_warning_review", {})).get(
                    "official_review_priority_counts", {}
                ),
                "ohlcv_warning_review_authorization_counts": summary(payloads.get("ohlcv_warning_review", {})).get(
                    "canonical_data_change_authorization_counts", {}
                ),
                "ohlcv_warning_review_status_counts": summary(payloads.get("ohlcv_warning_review", {})).get(
                    "official_listing_review_status_counts", {}
                ),
                "ohlcv_warning_review_source_locator_counts": summary(payloads.get("ohlcv_warning_review", {})).get(
                    "official_source_locator_status_counts", {}
                ),
            },
            "artifacts": ohlcv_artifacts,
            "acceptance_matrix": acceptance_matrix(campaign_key="ohlcv", exchanges=None, artifacts=ohlcv_artifacts, deltas=deltas),
            "delta_evidence": delta_evidence(
                "sampling_queue_only_no_data_apply",
                {
                    "selected_sample_rows": summary(payloads.get("ohlcv_plausibility", {})).get(
                        "sampling_coverage", {}
                    ).get("selected_rows", row_count(payloads.get("ohlcv_plausibility", {}))),
                    "checked_sample_rows": summary(payloads.get("ohlcv_plausibility", {})).get(
                        "sampling_coverage", {}
                    ).get("checked_rows", 0),
                    "warn_or_source_gap_signal_rows": summary(payloads.get("ohlcv_plausibility", {})).get(
                        "sampling_coverage", {}
                    ).get("warn_or_source_gap_signal_rows", 0),
                },
            ),
            "next_action": "run bounded Yahoo/local samples for selected buckets and use results only as review signals",
        },
        {
            "priority": 9,
            "name": "Freshness and reporting",
            "campaign_key": "freshness",
            "status": "global_and_source_freshness_visible",
            "evidence": {
                "source_gap_rows": row_count(source_gap),
                "coverage_freshness_keys": len(coverage.get("freshness", {})),
                "freshness_snapshot": freshness_rows,
                "freshness_snapshot_age_bucket_totals": freshness_age_bucket_totals,
                "top_freshness_snapshot_ages": stale_freshness_rows,
                "source_freshness_status_totals": top_counts(
                    {
                        status: sum(1 for row in coverage.get("source_coverage", []) if row.get("freshness_status") == status)
                        for status in {"fresh", "stale", "old", "unknown"}
                    }
                ),
                "source_age_bucket_totals": coverage.get("source_freshness_summary", {}).get("source_age_bucket_totals", {}),
                "source_refresh_priority_totals": coverage.get("source_freshness_summary", {}).get("refresh_priority_totals", {}),
                "source_refresh_queue_totals": coverage.get("source_freshness_summary", {}).get("refresh_queue_totals", {}),
                "source_refresh_queue_scope_totals": coverage.get("source_freshness_summary", {}).get(
                    "refresh_queue_scope_totals", {}
                ),
                "source_refresh_queue_mode_totals": coverage.get("source_freshness_summary", {}).get(
                    "refresh_queue_mode_totals", {}
                ),
                "source_refresh_queue_priority_totals": coverage.get("source_freshness_summary", {}).get(
                    "refresh_queue_priority_totals", {}
                ),
                "source_refresh_queue_age_bucket_totals": coverage.get("source_freshness_summary", {}).get(
                    "refresh_queue_age_bucket_totals", {}
                ),
                "source_refresh_action_totals": coverage.get("source_freshness_summary", {}).get(
                    "recommended_refresh_action_totals", {}
                ),
                "source_refresh_queue_review_strategy_totals": coverage.get("source_freshness_summary", {}).get(
                    "refresh_queue_review_strategy_totals", {}
                ),
                "source_refresh_queue_evidence_required_totals": coverage.get("source_freshness_summary", {}).get(
                    "refresh_queue_evidence_required_totals", {}
                ),
                "top_source_refresh_batches": coverage.get("source_freshness_summary", {}).get(
                    "top_source_refresh_batches", []
                ),
                "old_official_exchange_directory_count": coverage.get("source_freshness_summary", {}).get("old_official_exchange_directory_count", 0),
                "top_old_official_exchange_directories": coverage.get("source_freshness_summary", {}).get(
                    "top_old_official_exchange_directories", []
                ),
                "source_gap_class_totals": source_gap_summary.get("class_totals", {}),
                "top_source_gap_review_batches": source_gap_summary.get("top_source_gap_review_batches", []),
                "financialdata_supplement_rows": summary(payloads.get("financialdata_isin_supplements_review", {})).get(
                    "supplement_rows", 0
                ),
                "financialdata_apply_eligibility_counts": summary(
                    payloads.get("financialdata_isin_supplements_review", {})
                ).get("apply_eligibility_counts", {}),
                "financialdata_verification_evidence_required_counts": summary(
                    payloads.get("financialdata_isin_supplements_review", {})
                ).get("verification_evidence_required_counts", {}),
                "top_financialdata_supplement_review_batches": summary(
                    payloads.get("financialdata_isin_supplements_review", {})
                ).get("top_financialdata_supplement_review_batches", []),
            },
            "artifacts": freshness_artifacts,
            "acceptance_matrix": acceptance_matrix(campaign_key="freshness", exchanges=None, artifacts=freshness_artifacts, deltas=deltas),
            "delta_evidence": delta_evidence(
                "present_for_report_visibility",
                {
                    "coverage_freshness_keys": len(coverage.get("freshness", {})),
                    "freshness_snapshot": freshness_rows,
                    "freshness_snapshot_age_bucket_totals": freshness_age_bucket_totals,
                    "top_freshness_snapshot_ages": stale_freshness_rows,
                    "source_freshness_status_totals": top_counts(
                        {
                            status: sum(1 for row in coverage.get("source_coverage", []) if row.get("freshness_status") == status)
                            for status in {"fresh", "stale", "old", "unknown"}
                        }
                    ),
                    "source_age_bucket_totals": coverage.get("source_freshness_summary", {}).get("source_age_bucket_totals", {}),
                    "source_refresh_priority_totals": coverage.get("source_freshness_summary", {}).get("refresh_priority_totals", {}),
                    "source_refresh_queue_totals": coverage.get("source_freshness_summary", {}).get("refresh_queue_totals", {}),
                    "source_refresh_queue_scope_totals": coverage.get("source_freshness_summary", {}).get(
                        "refresh_queue_scope_totals", {}
                    ),
                    "source_refresh_queue_mode_totals": coverage.get("source_freshness_summary", {}).get(
                        "refresh_queue_mode_totals", {}
                    ),
                    "source_refresh_queue_priority_totals": coverage.get("source_freshness_summary", {}).get(
                        "refresh_queue_priority_totals", {}
                    ),
                    "source_refresh_queue_age_bucket_totals": coverage.get("source_freshness_summary", {}).get(
                        "refresh_queue_age_bucket_totals", {}
                    ),
                    "source_refresh_queue_review_strategy_totals": coverage.get("source_freshness_summary", {}).get(
                        "refresh_queue_review_strategy_totals", {}
                    ),
                    "source_refresh_queue_evidence_required_totals": coverage.get("source_freshness_summary", {}).get(
                        "refresh_queue_evidence_required_totals", {}
                    ),
                    "source_refresh_action_totals": coverage.get("source_freshness_summary", {}).get(
                        "recommended_refresh_action_totals", {}
                    ),
                    "old_official_exchange_directory_count": coverage.get("source_freshness_summary", {}).get("old_official_exchange_directory_count", 0),
                    "top_source_gap_review_batches": source_gap_summary.get("top_source_gap_review_batches", []),
                    "financialdata_supplement_rows": summary(
                        payloads.get("financialdata_isin_supplements_review", {})
                    ).get("supplement_rows", 0),
                    "top_financialdata_supplement_review_batches": summary(
                        payloads.get("financialdata_isin_supplements_review", {})
                    ).get("top_financialdata_supplement_review_batches", []),
                },
            ),
            "next_action": "refresh old official masterfile sources and rerun verification reports",
        },
        {
            "priority": 10,
            "name": "Before/after delta baseline",
            "campaign_key": "baseline",
            "status": "baseline_snapshot_available_for_future_campaign_deltas",
            "evidence": {
                "baseline_global_metrics": len(payloads.get("improvement_baseline", {}).get("global_baseline", {})),
                "baseline_campaigns": len(payloads.get("improvement_baseline", {}).get("campaign_baseline", {})),
                "changed_numeric_delta_rows": summary(payloads.get("improvement_deltas", {})).get("changed_numeric_delta_rows", 0),
            },
            "artifacts": baseline_artifacts,
            "acceptance_matrix": acceptance_matrix(campaign_key="baseline", exchanges=None, artifacts=baseline_artifacts, deltas=deltas),
            "delta_evidence": delta_evidence(
                "baseline_only",
                {
                    "baseline_generated_at": payloads.get("improvement_baseline", {}).get("_meta", {}).get("generated_at", ""),
                    "future_delta_reference": "data/reports/improvement_baseline.json",
                    "current_delta_report": "data/reports/improvement_deltas.json",
                },
                ["next_campaign_after_snapshot"],
            ),
            "next_action": "compare future campaign reports against this baseline before applying or closing a PR",
        },
    ]
    for campaign in campaigns:
        campaign["review_policy"] = campaign_review_policy(str(campaign["campaign_key"]))
        campaign["before_after_summary"] = before_after_summary(campaign.get("acceptance_matrix", {}))
        campaign["campaign_context"] = campaign_context_for(campaign)
        campaign["artifact_context"] = artifact_context_for(campaign)
        campaign["delta_review_context"] = delta_review_context_for(campaign)
        campaign["closure_context"] = closure_context_for(campaign)
    return campaigns


def build_payload() -> dict[str, Any]:
    payloads = {name: load_json(REPORTS_DIR / f"{name}.json") for name in ARTIFACT_NAMES}
    campaigns = build_campaigns(payloads)
    review_batches = next_review_batches(campaigns)
    workload = next_review_workload_summary(review_batches)
    execution_plan = next_review_execution_plan(review_batches)
    execution_summary = next_review_execution_summary(execution_plan)
    command_safety_summary = next_review_command_safety_summary(execution_plan)
    closure_summary = closure_readiness_summary(review_batches)
    blockers = closure_blockers(review_batches)
    validation_summary = payloads.get("validation_report", {}).get("summary", {})
    summary_row = {
        "campaigns": len(campaigns),
        "complete_campaigns": sum(1 for row in campaigns if row["status"].startswith("complete")),
        "next_review_batches": len(review_batches),
        "next_review_batch_rows": sum(int(row.get("artifact_rows") or 0) for row in review_batches),
        "closure_ready_campaigns": closure_summary["ready_campaigns"],
        "closure_blocked_campaigns": closure_summary["blocked_campaigns"],
        "closure_blockers": len(blockers),
        "validation_failed_error_gates": validation_summary.get("failed_error_gates", 0),
    }
    return {
        "_meta": {
            "generated_at": utc_now_iso(),
            "rows": len(campaigns),
            "source_files": {
                name: display_path(REPORTS_DIR / f"{name}.json")
                for name in ARTIFACT_NAMES
            },
            "policy": "Progress report only. It summarizes review artifacts and does not authorize data fills or symbol changes.",
        },
        "summary": summary_row,
        "summary_context": summary_context_for(summary_row),
        "campaigns": campaigns,
        "next_review_batches": review_batches,
        "next_review_workload": workload,
        "next_review_execution_plan": execution_plan,
        "next_review_execution_summary": execution_summary,
        "next_review_command_safety_summary": command_safety_summary,
        "closure_readiness": closure_summary,
        "closure_blockers": blockers,
    }


def render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Improvement Campaigns",
        "",
        f"Generated: `{payload['_meta']['generated_at']}`",
        "",
        "This report summarizes review evidence only. It does not authorize guessed identifiers, sectors, categories, names, or symbol changes.",
        "",
        f"Summary context: `{payload.get('summary_context', '')}`",
        "",
        "| Priority | Campaign | Status | Delta Evidence | Rows | Next Action |",
        "|---:|---|---|---|---:|---|",
    ]
    for campaign in payload["campaigns"]:
        rows = sum(int(artifact.get("rows", 0)) for artifact in campaign["artifacts"])
        lines.append(
            f"| {campaign['priority']} | {campaign['name']} | {campaign['status']} | {campaign['delta_evidence']['status']} | {rows} | {campaign['next_action']} |"
        )
    meta = payload.get("_meta", {})
    source_files = meta.get("source_files", {}) if isinstance(meta, dict) else {}
    if not isinstance(source_files, dict):
        source_files = {}
    lines.extend(["", "## Source Files", "", "| Key | Path |", "|---|---|"])
    for key, path in sorted(source_files.items()):
        lines.append(f"| `{markdown_cell(key)}` | `{markdown_cell(path)}` |")
    batches = payload.get("next_review_batches", [])
    if batches:
        lines.extend(
            [
                "",
                "## Next Review Batches",
                "",
                "| Priority | Campaign | Rows | Primary Artifact | Delta Gaps | Closure Gate | Closure Context | Next Action | Recommended Next Source | Source Gate |",
                "|---:|---|---:|---|---|---|---|---|---|---|",
            ]
        )
        for batch in batches:
            missing = batch.get("missing_delta_evidence", [])
            missing_text = ", ".join(str(value) for value in missing) if isinstance(missing, list) else str(missing)
            lines.append(
                "| "
                f"{markdown_cell(batch.get('priority'))} | "
                f"`{markdown_cell(batch.get('campaign_key'))}` | "
                f"{markdown_cell(batch.get('artifact_rows'))} | "
                f"`{markdown_cell(batch.get('primary_artifact'))}` | "
                f"{markdown_cell(missing_text)} | "
                f"`{markdown_cell(batch.get('closure_gate'))}` | "
                f"`{markdown_cell(batch.get('closure_context'))}` | "
                f"{markdown_cell(batch.get('next_action'))} | "
                f"{markdown_cell(batch.get('recommended_next_source'))} | "
                f"{markdown_cell(batch.get('source_gate'))} |"
            )
    workload = payload.get("next_review_workload", {})
    if workload:
        rows_by_campaign = workload.get("rows_by_campaign_key", {}) if isinstance(workload, dict) else {}
        rows_by_gate = workload.get("rows_by_closure_gate", {}) if isinstance(workload, dict) else {}
        largest_batch = workload.get("largest_batch", {}) if isinstance(workload, dict) else {}
        lines.extend(
            [
                "",
                "## Next Review Workload",
                "",
                f"Workload context: `{markdown_cell(workload.get('workload_context'))}`",
                "",
                "| Metric | Value |",
                "|---|---:|",
                f"| `total_batches` | `{markdown_cell(workload.get('total_batches'))}` |",
                f"| `total_rows` | `{markdown_cell(workload.get('total_rows'))}` |",
                f"| `blocked_batches` | `{markdown_cell(workload.get('blocked_batches'))}` |",
                f"| `largest_campaign` | `{markdown_cell(largest_batch.get('campaign_key'))}` |",
                f"| `largest_rows` | `{markdown_cell(largest_batch.get('artifact_rows'))}` |",
                "",
                "| Campaign | Rows |",
                "|---|---:|",
            ]
        )
        for key, rows in sorted(rows_by_campaign.items()):
            lines.append(f"| `{markdown_cell(key)}` | {markdown_cell(rows)} |")
        lines.extend(["", "| Closure Gate | Rows |", "|---|---:|"])
        for key, rows in sorted(rows_by_gate.items()):
            lines.append(f"| `{markdown_cell(key)}` | {markdown_cell(rows)} |")
    execution_plan = payload.get("next_review_execution_plan", [])
    if execution_plan:
        lines.extend(
            [
                "",
                "## Next Review Execution Plan",
                "",
                "| Priority | Campaign | Rows | Mode | Network | Data Change Authorized | Context | Evidence Command | Source Gate |",
                "|---:|---|---:|---|---:|---:|---|---|---|",
            ]
        )
        for row in execution_plan:
            lines.append(
                "| "
                f"{markdown_cell(row.get('priority'))} | "
                f"`{markdown_cell(row.get('campaign_key'))}` | "
                f"{markdown_cell(row.get('artifact_rows'))} | "
                f"`{markdown_cell(row.get('command_mode'))}` | "
                f"{markdown_cell(row.get('network_required'))} | "
                f"{markdown_cell(row.get('data_change_authorized'))} | "
                f"`{markdown_cell(row.get('execution_context'))}` | "
                f"`{markdown_cell(row.get('evidence_command'))}` | "
                f"{markdown_cell(row.get('source_gate'))} |"
            )
        lines.extend(
            [
                "",
                "| Order | Campaign | Ranking Reason | Ranking Context |",
                "|---:|---|---|---|",
            ]
        )
        for row in execution_plan:
            lines.append(
                "| "
                f"{markdown_cell(row.get('execution_order'))} | "
                f"`{markdown_cell(row.get('campaign_key'))}` | "
                f"`{markdown_cell(row.get('ranking_reason'))}` | "
                f"`{markdown_cell(row.get('ranking_context'))}` |"
            )
        lines.extend(
            [
                "",
                "| Campaign | Scripts | Missing Scripts | Command Readiness Context |",
                "|---|---:|---:|---|",
            ]
        )
        for row in execution_plan:
            scripts = row.get("command_scripts", [])
            missing = row.get("missing_command_scripts", [])
            script_count = len(scripts) if isinstance(scripts, list) else "invalid"
            missing_count = len(missing) if isinstance(missing, list) else "invalid"
            lines.append(
                "| "
                f"`{markdown_cell(row.get('campaign_key'))}` | "
                f"{markdown_cell(script_count)} | "
                f"{markdown_cell(missing_count)} | "
                f"`{markdown_cell(row.get('command_readiness_context'))}` |"
            )
        lines.extend(
            [
                "",
                "| Campaign | Mutation Risk | Manual Review Before Run | Command Safety Context |",
                "|---|---|---:|---|",
            ]
        )
        for row in execution_plan:
            lines.append(
                "| "
                f"`{markdown_cell(row.get('campaign_key'))}` | "
                f"`{markdown_cell(row.get('command_mutation_risk'))}` | "
                f"{markdown_cell(row.get('manual_review_required_before_run'))} | "
                f"`{markdown_cell(row.get('command_safety_context'))}` |"
            )
    command_safety_summary = payload.get("next_review_command_safety_summary", {})
    if command_safety_summary:
        lines.extend(
            [
                "",
                "## Next Review Command Safety Summary",
                "",
                f"Command safety summary context: `{markdown_cell(command_safety_summary.get('command_safety_summary_context'))}`",
                "",
                "| Metric | Value |",
                "|---|---:|",
                f"| `total_actions` | `{markdown_cell(command_safety_summary.get('total_actions'))}` |",
                f"| `review_required_actions` | `{markdown_cell(command_safety_summary.get('review_required_actions'))}` |",
                f"| `report_or_fetch_only_actions` | `{markdown_cell(command_safety_summary.get('report_or_fetch_only_actions'))}` |",
                f"| `manual_review_required_actions` | `{markdown_cell(command_safety_summary.get('manual_review_required_actions'))}` |",
                f"| `preflight_complete_actions` | `{markdown_cell(command_safety_summary.get('preflight_complete_actions'))}` |",
                f"| `preflight_gap_campaign_keys` | `{markdown_cell(json.dumps(command_safety_summary.get('preflight_gap_campaign_keys', []), ensure_ascii=False))}` |",
                f"| `data_change_authorized_actions` | `{markdown_cell(command_safety_summary.get('data_change_authorized_actions'))}` |",
                f"| `execution_ready_without_manual_review` | `{markdown_cell(command_safety_summary.get('execution_ready_without_manual_review'))}` |",
                f"| `execution_blocking_gate` | `{markdown_cell(command_safety_summary.get('execution_blocking_gate'))}` |",
                f"| `execution_blocking_campaign_keys` | `{markdown_cell(json.dumps(command_safety_summary.get('execution_blocking_campaign_keys', []), ensure_ascii=False))}` |",
                f"| `review_required_campaign_keys` | `{markdown_cell(json.dumps(command_safety_summary.get('review_required_campaign_keys', []), ensure_ascii=False))}` |",
            ]
        )
        rows = command_safety_summary.get("review_required_command_rows", [])
        if rows:
            lines.extend(
                [
                    "",
                    "| Campaign | Risky Scripts | Manual Review | Data Change Authorized | Preflight Checks | Review-Required Context | Preflight Context |",
                    "|---|---|---:|---:|---|---|---|",
                ]
            )
            for row in rows:
                lines.append(
                    "| "
                    f"`{markdown_cell(row.get('campaign_key'))}` | "
                    f"`{markdown_cell(json.dumps(row.get('risky_command_scripts', []), ensure_ascii=False))}` | "
                    f"{markdown_cell(row.get('manual_review_required_before_run'))} | "
                    f"{markdown_cell(row.get('data_change_authorized'))} | "
                    f"`{markdown_cell(json.dumps(row.get('review_required_preflight_checks', []), ensure_ascii=False))}` | "
                    f"`{markdown_cell(row.get('review_required_command_context'))}` | "
                    f"`{markdown_cell(row.get('review_required_preflight_context'))}` |"
                )
    execution_summary = payload.get("next_review_execution_summary", {})
    if execution_summary:
        lines.extend(
            [
                "",
                "## Next Review Execution Summary",
                "",
                f"Execution summary context: `{markdown_cell(execution_summary.get('execution_summary_context'))}`",
                "",
                "| Metric | Value |",
                "|---|---:|",
                f"| `total_actions` | `{markdown_cell(execution_summary.get('total_actions'))}` |",
                f"| `local_report_rebuild_actions` | `{markdown_cell(execution_summary.get('local_report_rebuild_actions'))}` |",
                f"| `network_evidence_refresh_actions` | `{markdown_cell(execution_summary.get('network_evidence_refresh_actions'))}` |",
                f"| `network_required_rows` | `{markdown_cell(execution_summary.get('network_required_rows'))}` |",
                f"| `local_report_rebuild_rows` | `{markdown_cell(execution_summary.get('local_report_rebuild_rows'))}` |",
                f"| `data_change_authorized_actions` | `{markdown_cell(execution_summary.get('data_change_authorized_actions'))}` |",
                f"| `network_campaign_keys` | `{markdown_cell(json.dumps(execution_summary.get('network_campaign_keys', []), ensure_ascii=False))}` |",
            ]
        )
    closure = payload.get("closure_readiness", {})
    if closure:
        lines.extend(["", "## Closure Readiness", "", "| Metric | Value |", "|---|---|"])
        for key in ("ready_campaigns", "blocked_campaigns"):
            lines.append(f"| `{key}` | `{markdown_cell(closure.get(key))}` |")
        lines.append(
            "| `closure_gate_counts` | "
            f"`{json.dumps(closure.get('closure_gate_counts', {}), ensure_ascii=False, sort_keys=True)}` |"
        )
        lines.append(
            "| `blocked_campaign_keys` | "
            f"`{json.dumps(closure.get('blocked_campaign_keys', []), ensure_ascii=False)}` |"
        )
        lines.append(f"| `policy` | {markdown_cell(closure.get('policy'))} |")
    blockers = payload.get("closure_blockers", [])
    if blockers:
        lines.extend(
            [
                "",
                "## Closure Blockers",
                "",
                "| Priority | Campaign | Blocker | Rows | Primary Artifact | First Missing Delta | Next Action |",
                "|---:|---|---|---:|---|---|---|",
            ]
        )
        for blocker in blockers:
            lines.append(
                "| "
                f"{markdown_cell(blocker.get('priority'))} | "
                f"`{markdown_cell(blocker.get('campaign_key'))}` | "
                f"`{markdown_cell(blocker.get('blocker_type'))}` | "
                f"{markdown_cell(blocker.get('artifact_rows'))} | "
                f"`{markdown_cell(blocker.get('primary_artifact'))}` | "
                f"`{markdown_cell(blocker.get('first_missing_delta'))}` | "
                f"{markdown_cell(blocker.get('next_action'))} |"
            )
        lines.extend(
            [
                "",
                "| Campaign | Mode | Network | Data Change Authorized | Blocker Context | Evidence Command |",
                "|---|---|---:|---:|---|---|",
            ]
        )
        for blocker in blockers:
            lines.append(
                "| "
                f"`{markdown_cell(blocker.get('campaign_key'))}` | "
                f"`{markdown_cell(blocker.get('command_mode'))}` | "
                f"{markdown_cell(blocker.get('network_required'))} | "
                f"{markdown_cell(blocker.get('data_change_authorized'))} | "
                f"`{markdown_cell(blocker.get('blocker_context'))}` | "
                f"`{markdown_cell(blocker.get('evidence_command'))}` |"
            )
    lines.extend(["", "## Evidence", ""])
    for campaign in payload["campaigns"]:
        lines.extend([f"### {campaign['priority']}. {campaign['name']}", "", f"- Status: `{campaign['status']}`"])
        for key, value in campaign["evidence"].items():
            if isinstance(value, list) and append_source_gated_batch_table(lines, key, value):
                continue
            lines.append(f"- `{key}`: `{json.dumps(value, ensure_ascii=False, sort_keys=True)}`")
        lines.append(f"- `review_policy`: `{json.dumps(campaign['review_policy'], ensure_ascii=False, sort_keys=True)}`")
        lines.append(f"- `campaign_context`: `{campaign.get('campaign_context', '')}`")
        lines.append(f"- `artifact_context`: `{campaign.get('artifact_context', '')}`")
        lines.append(f"- `delta_review_context`: `{campaign.get('delta_review_context', '')}`")
        lines.append(f"- `closure_context`: `{campaign.get('closure_context', '')}`")
        lines.append(f"- `before_after_context`: `{campaign.get('before_after_summary', {}).get('before_after_context', '')}`")
        lines.append(
            f"- `before_after_summary`: `{json.dumps(campaign.get('before_after_summary', {}), ensure_ascii=False, sort_keys=True)}`"
        )
        lines.append(f"- `acceptance_matrix`: `{json.dumps(campaign['acceptance_matrix'], ensure_ascii=False, sort_keys=True)}`")
        append_delta_evidence(lines, campaign["delta_evidence"])
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a summary of active database-improvement campaign artifacts.")
    parser.add_argument("--json-out", type=Path, default=DEFAULT_JSON_OUT)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_MD_OUT)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    payload = build_payload()
    args.json_out.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    args.md_out.write_text(render_markdown(payload), encoding="utf-8")
    print(json.dumps(payload["summary"], indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()

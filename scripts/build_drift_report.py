"""Anti-drift freshness report.

Companion to the daily symbol-changes feed: summarises, on a schedule, the
signals that indicate the dataset is drifting out of date — dataset staleness,
renames detected by the feed but not yet applied, and the current data-quality
indicator counts from the release gates. Detection only; nothing is auto-applied
(corrections stay on the verified override/verify path).

A naive source-vs-dataset symbol set-diff is intentionally NOT used: exchange
symbol universes differ in scope (BDRs, share classes, units), so it produces
thousands of false new/delisting candidates rather than real drift.

Writes data/reports/drift_report.{json,md}; in GitHub Actions also writes
``drift_detected=true|false`` to ``$GITHUB_OUTPUT``.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.rebuild_dataset import TICKERS_CSV

COVERAGE_REPORT_JSON = ROOT / "data" / "reports" / "coverage_report.json"
SYMBOL_CHANGES_CSV = ROOT / "data" / "corporate_actions" / "symbol_changes.csv"
SYMBOL_CHANGES_REVIEW_CSV = ROOT / "data" / "reports" / "symbol_changes_review.csv"
SYMBOL_CHANGES_APPLY_JSON = ROOT / "data" / "reports" / "symbol_changes_apply.json"
VALIDATION_JSON = ROOT / "data" / "reports" / "validation_report.json"
REPORT_JSON = ROOT / "data" / "reports" / "drift_report.json"
REPORT_MD = ROOT / "data" / "reports" / "drift_report.md"
MANUAL_REVIEW_JSON = ROOT / "data" / "reports" / "pending_renames_manual_review.json"
MANUAL_REVIEW_CSV = ROOT / "data" / "reports" / "pending_renames_manual_review.csv"
MANUAL_REVIEW_MD = ROOT / "data" / "reports" / "pending_renames_manual_review.md"
SAMPLE_CAP = 30
QUALITY_KEYS = (
    "source_gap_rows", "missing_stock_sector", "missing_etf_category",
    "expected_missing_primary_isin", "country_isin_mismatch",
    "official_name_mismatch", "allowed_warn_rows",
)
QUALITY_REGRESSION_KEYS = (
    "source_gap_rows",
    "expected_missing_primary_isin",
    "missing_stock_sector",
    "missing_etf_category",
)
OFFICIAL_RECALL_REGRESSION_KEYS = (
    "official_recall_missing",
    "collision_adjusted_recall_missing",
)
RENAME_REVIEW_QUEUES = {
    "review_verified_rename_or_delisting",
    "blocked_out_of_scope_symbol_collision",
    "blocked_missing_source_scope_mapping",
    "review_duplicate_or_cross_listing",
}
APPLY_READY_STATUSES = {"apply"}
PENDING_TRIAGE_STATUSES = {"apply_ready", "fallback_pending"}


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def dataset_built_at() -> str | None:
    try:
        return json.loads(COVERAGE_REPORT_JSON.read_text(encoding="utf-8"))["_meta"]["generated_at"]
    except Exception:
        return None


def staleness_days(built_at: str | None, *, now: datetime) -> float | None:
    if not built_at:
        return None
    try:
        ts = datetime.fromisoformat(built_at.replace("Z", "+00:00"))
    except ValueError:
        return None
    return round((now - ts).total_seconds() / 86400.0, 1)


def current_symbols() -> set[str]:
    with TICKERS_CSV.open(newline="", encoding="utf-8") as handle:
        return {r["ticker"].strip().upper() for r in csv.DictReader(handle)}


def apply_status_lookup() -> tuple[dict[tuple[str, str, str], str], str]:
    if not SYMBOL_CHANGES_APPLY_JSON.exists():
        return {}, "missing"
    data = json.loads(SYMBOL_CHANGES_APPLY_JSON.read_text(encoding="utf-8"))
    lookup: dict[tuple[str, str, str], str] = {}
    for section in ("accepted", "blocked"):
        for row in data.get(section, []):
            key = (
                (row.get("effective_date") or "").strip(),
                (row.get("old_symbol") or "").strip().upper(),
                (row.get("new_symbol") or "").strip().upper(),
            )
            if key[1] and key[2]:
                lookup[key] = "apply" if section == "accepted" else row.get("status", "")
    return lookup, "available"


def blocker_reason(row: dict[str, str], apply_status: str) -> str:
    queue = row.get("symbol_change_workflow_queue", "")
    scope = row.get("exchange_scope_status", "")
    old_keys = row.get("old_listing_keys", "") or row.get("old_scoped_listing_keys", "")
    new_keys = row.get("new_listing_keys", "") or row.get("new_scoped_listing_keys", "")
    if apply_status == "manual_isin_not_proven_unchanged":
        return (
            "manual: official active new-symbol evidence exists, but unchanged ISIN/identity is not proven "
            "and the old symbol is still present in an official source"
        )
    if apply_status == "manual_non_us_or_unscoped_source":
        if scope == "global_symbol_collision_outside_source_scope":
            return (
                f"blocked: secondary feed scope is {row.get('source_exchange_hint') or 'unscoped'}, "
                f"but old symbol matches dataset listing(s) outside that scope: {old_keys or 'none'}"
            )
        return "manual: source exchange scope is not mapped to a safe listing-keyed apply path"
    if apply_status == "blocked_new_symbol_collision":
        return f"blocked: new symbol already has dataset listing(s), requiring duplicate/cross-listing review: {new_keys or 'present'}"
    if apply_status == "blocked_old_symbol_not_unique_in_us_scope":
        return f"blocked: old symbol is not unique in the scoped US listing universe: {old_keys or 'multiple'}"
    if apply_status == "manual_transition_or_shell_name":
        return "manual: transition, shell, unit, right, warrant, or acquisition-like name requires issuer/listing review"
    if apply_status == "manual_missing_isin":
        return "manual: current dataset row has no ISIN, so unchanged identity cannot be proven"
    if queue == "review_verified_rename_or_delisting":
        return row.get("source_gate", "") or "manual: official old-inactive/new-active same-issuer evidence required"
    if queue == "blocked_out_of_scope_symbol_collision":
        return (
            f"blocked: symbol collision outside source scope; old={old_keys or 'none'} "
            f"new={new_keys or 'none'}"
        )
    if queue == "blocked_missing_source_scope_mapping":
        return "blocked: secondary feed event has no source exchange mapping"
    if queue == "review_duplicate_or_cross_listing":
        return "manual: both old and new symbols are present in source scope; duplicate/cross-listing state must be resolved first"
    return row.get("source_gate", "") or "manual review required before any canonical symbol change"


def rename_triage_rows() -> list[dict[str, str]]:
    """Classify feed-detected rename signals against scoped review/apply evidence.

    The old drift metric intentionally used a broad symbol set as a smoke test,
    but that over-counted symbol reuse across exchanges as real drift. This
    triage keeps those rows visible while only counting rows as pending when a
    listing-keyed apply gate says they are ready to apply.
    """
    symbols = current_symbols()
    review_rows = read_csv(SYMBOL_CHANGES_REVIEW_CSV)
    triage_source = "symbol_changes_review"
    if not review_rows:
        review_rows = read_csv(SYMBOL_CHANGES_CSV)
        triage_source = "symbol_changes_csv_fallback"
    statuses, apply_status_source = apply_status_lookup()
    triage: list[dict[str, str]] = []
    for row in review_rows:
        old = (row.get("old_symbol") or "").strip().upper()
        new = (row.get("new_symbol") or "").strip().upper()
        if not old or old not in symbols or not new or new in symbols:
            continue
        queue = row.get("symbol_change_workflow_queue", "")
        if queue and queue not in RENAME_REVIEW_QUEUES:
            continue
        key = ((row.get("effective_date") or "").strip(), old, new)
        apply_status = statuses.get(key, "")
        if apply_status in APPLY_READY_STATUSES:
            triage_status = "apply_ready"
        elif triage_source != "symbol_changes_review" or apply_status_source == "missing":
            triage_status = "fallback_pending"
        else:
            triage_status = "blocked_or_manual"
        triage.append({
            "effective_date": row.get("effective_date", ""),
            "old_symbol": old,
            "new_symbol": new,
            "new_company_name": row.get("new_company_name", ""),
            "source_exchange_hint": row.get("source_exchange_hint", ""),
            "match_status": row.get("match_status", ""),
            "symbol_change_workflow_queue": queue,
            "exchange_scope_status": row.get("exchange_scope_status", ""),
            "old_listing_keys": row.get("old_listing_keys", ""),
            "new_listing_keys": row.get("new_listing_keys", ""),
            "apply_status": apply_status,
            "apply_status_source": apply_status_source,
            "triage_source": triage_source,
            "triage_status": triage_status,
            "blocker_reason": blocker_reason(row, apply_status),
        })
    return triage


def pending_renames(rows: list[dict[str, str]] | None = None) -> list[dict[str, str]]:
    rows = rename_triage_rows() if rows is None else rows
    return [
        {
            "old_symbol": row["old_symbol"],
            "new_symbol": row["new_symbol"],
            "new_company_name": row["new_company_name"],
            "effective_date": row["effective_date"],
            "blocker_reason": row["blocker_reason"],
        }
        for row in rows
        if row["triage_status"] in PENDING_TRIAGE_STATUSES
    ]


def quality_indicators() -> dict[str, int]:
    """Current info-gate counts from the validation report (data-quality drift)."""
    out: dict[str, int] = {}
    if not VALIDATION_JSON.exists():
        return out
    data = json.loads(VALIDATION_JSON.read_text(encoding="utf-8"))

    def walk(node):
        if isinstance(node, dict):
            name = node.get("name") or node.get("gate")
            if name in QUALITY_KEYS and ("actual" in node or "count" in node):
                val = node.get("actual", node.get("count"))
                if isinstance(val, int):
                    out[name] = val
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for v in node:
                walk(v)

    walk(data)
    return out


def previous_drift_report() -> dict:
    try:
        relative_report = REPORT_JSON.relative_to(ROOT).as_posix()
    except ValueError:
        relative_report = str(REPORT_JSON)
    try:
        result = subprocess.run(
            ["git", "show", f"HEAD:{relative_report}"],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired):
        result = None
    if result is not None and result.returncode == 0 and result.stdout.strip():
        return json.loads(result.stdout)
    if not REPORT_JSON.exists():
        return {}
    return json.loads(REPORT_JSON.read_text(encoding="utf-8"))


def quality_regressions(current: dict[str, int], previous: dict) -> list[dict[str, int | str]]:
    baseline = previous.get("quality_indicators", {}) if isinstance(previous, dict) else {}
    if not isinstance(baseline, dict):
        return []
    regressions: list[dict[str, int | str]] = []
    for key in QUALITY_REGRESSION_KEYS:
        current_value = current.get(key)
        previous_value = baseline.get(key)
        if isinstance(current_value, int) and isinstance(previous_value, int) and current_value > previous_value:
            regressions.append(
                {
                    "metric": key,
                    "previous": previous_value,
                    "current": current_value,
                    "delta": current_value - previous_value,
                }
            )
    return regressions


def official_recall_indicators() -> dict[str, dict[str, int | float | bool | str | None]]:
    if not COVERAGE_REPORT_JSON.exists():
        return {}
    data = json.loads(COVERAGE_REPORT_JSON.read_text(encoding="utf-8"))
    indicators: dict[str, dict[str, int | float | bool | str | None]] = {}
    for row in data.get("by_exchange", []):
        exchange = row.get("exchange")
        if not exchange:
            continue
        indicators[exchange] = {
            "official_recall_target": row.get("official_recall_target"),
            "official_recall_pass": row.get("official_recall_pass"),
            "official_recall_pct": row.get("official_recall_pct"),
            "official_recall_missing": row.get("official_recall_missing"),
            "official_recall_exception": row.get("official_recall_exception", ""),
            "collision_adjusted_recall_missing": row.get("collision_adjusted_recall_missing"),
            "collision_adjusted_recall_pct": row.get("collision_adjusted_recall_pct"),
            "collision_adjusted_recall_pass": row.get("collision_adjusted_recall_pass"),
        }
    return indicators


def official_recall_regressions(
    current: dict[str, dict[str, int | float | bool | str | None]],
    previous: dict,
) -> list[dict[str, int | str]]:
    baseline = previous.get("official_recall_indicators", {}) if isinstance(previous, dict) else {}
    if not isinstance(baseline, dict):
        return []
    regressions: list[dict[str, int | str]] = []
    for exchange, current_row in current.items():
        previous_row = baseline.get(exchange, {})
        if not isinstance(previous_row, dict):
            continue
        for key in OFFICIAL_RECALL_REGRESSION_KEYS:
            current_value = current_row.get(key)
            previous_value = previous_row.get(key)
            if isinstance(current_value, int) and isinstance(previous_value, int) and current_value > previous_value:
                regressions.append(
                    {
                        "exchange": exchange,
                        "metric": key,
                        "previous": previous_value,
                        "current": current_value,
                        "delta": current_value - previous_value,
                    }
                )
    return regressions


def build_markdown(s: dict) -> str:
    L = ["# Drift / freshness report", "",
         f"Generated: {s['generated_at']}",
         f"Dataset built_at: {s['built_at']} ({s['staleness_days']} days ago; "
         f"threshold {s['stale_threshold_days']})",
         f"**drift_detected: {s['drift_detected']}**", "",
         f"## Pending renames (feed-detected, not yet applied): {s['pending_renames_count']}"]
    for r in s["pending_renames_sample"]:
        L.append(f"- {r['old_symbol']} -> {r['new_symbol']} ({r['new_company_name']}, {r['effective_date']})")
    if s["pending_renames_count"] > len(s["pending_renames_sample"]):
        L.append(f"- ... and {s['pending_renames_count'] - len(s['pending_renames_sample'])} more")
    if s.get("rename_triage_fallback"):
        L.append("- Rename triage fallback is active; raw feed or missing apply-artifact rows count as pending drift.")
    if s.get("rename_triage_source_totals"):
        L.append(f"- Triage sources: {s['rename_triage_source_totals']}")
    manual_review_count = s.get("manual_review_count", 0)
    L += ["", f"## Blocked/manual rename review rows: {manual_review_count}"]
    for r in s.get("manual_review_sample", []):
        L.append(
            f"- {r['old_symbol']} -> {r['new_symbol']} ({r['new_company_name']}, "
            f"{r['effective_date']}): {r['blocker_reason']}"
        )
    if manual_review_count > len(s.get("manual_review_sample", [])):
        L.append(f"- ... and {manual_review_count - len(s.get('manual_review_sample', []))} more")
    L += ["", "## Quality indicators (release-gate info counts)"]
    for k, v in sorted(s["quality_indicators"].items()):
        L.append(f"- {k}: {v}")
    L += ["", f"## Quality regressions: {len(s.get('quality_regressions', []))}"]
    for row in s.get("quality_regressions", [])[:SAMPLE_CAP]:
        L.append(f"- {row['metric']}: {row['previous']} -> {row['current']} (+{row['delta']})")
    L += ["", f"## Official recall regressions: {len(s.get('official_recall_regressions', []))}"]
    for row in s.get("official_recall_regressions", [])[:SAMPLE_CAP]:
        L.append(
            f"- {row['exchange']} {row['metric']}: {row['previous']} -> {row['current']} (+{row['delta']})"
        )
    L += ["", "_Detection only. Triage renames via the symbol-change review feed; "
          "apply corrections through the verified override/verify pipeline._"]
    return "\n".join(L) + "\n"


def write_manual_review_artifacts(rows: list[dict[str, str]], *, generated_at: str) -> None:
    fieldnames = [
        "effective_date",
        "old_symbol",
        "new_symbol",
        "new_company_name",
        "source_exchange_hint",
        "match_status",
        "symbol_change_workflow_queue",
        "exchange_scope_status",
        "old_listing_keys",
        "new_listing_keys",
        "apply_status",
        "apply_status_source",
        "triage_source",
        "triage_status",
        "blocker_reason",
    ]
    manual_rows = [row for row in rows if row["triage_status"] == "blocked_or_manual"]
    payload = {
        "generated_at": generated_at,
        "rows": len(manual_rows),
        "policy": (
            "Blocked/manual rename rows are not canonical ticker changes. Apply only after "
            "listing-keyed official old-inactive/new-active same-issuer evidence, with no collision."
        ),
        "manual_review_items": manual_rows,
    }
    MANUAL_REVIEW_JSON.parent.mkdir(parents=True, exist_ok=True)
    MANUAL_REVIEW_JSON.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    with MANUAL_REVIEW_CSV.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows({field: row.get(field, "") for field in fieldnames} for row in manual_rows)
    lines = [
        "# Pending Renames Manual Review",
        "",
        f"Generated: {generated_at}",
        "",
        "Rows here are explicitly blocked or manual-review only; no ticker change is authorized by this report.",
        "",
        "| Old | New | Effective | Queue | Apply status | Blocker |",
        "|---|---|---|---|---|---|",
    ]
    if manual_rows:
        for row in manual_rows:
            lines.append(
                f"| {row['old_symbol']} | {row['new_symbol']} | {row['effective_date']} | "
                f"{row['symbol_change_workflow_queue'] or 'none'} | {row['apply_status'] or 'none'} | "
                f"{row['blocker_reason']} |"
            )
    else:
        lines.append("|  |  |  |  |  | No blocked/manual rows. |")
    MANUAL_REVIEW_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stale-days", type=float, default=45.0,
                        help="Staleness threshold (days) that counts as drift.")
    args = parser.parse_args(argv)

    now = datetime.now(timezone.utc)
    built = dataset_built_at()
    stale = staleness_days(built, now=now)
    triage_rows = rename_triage_rows()
    pend = pending_renames(triage_rows)
    manual_rows = [row for row in triage_rows if row["triage_status"] == "blocked_or_manual"]
    quality = quality_indicators()
    previous = previous_drift_report()
    quality_regression_rows = quality_regressions(quality, previous)
    recall = official_recall_indicators()
    recall_regression_rows = official_recall_regressions(recall, previous)

    drift_detected = (
        bool(pend)
        or (stale is not None and stale > args.stale_days)
        or bool(quality_regression_rows)
        or bool(recall_regression_rows)
    )
    generated_at = now.isoformat(timespec="seconds").replace("+00:00", "Z")

    summary = {
        "generated_at": generated_at,
        "built_at": built,
        "staleness_days": stale,
        "stale_threshold_days": args.stale_days,
        "pending_renames_count": len(pend),
        "pending_renames_sample": pend[:SAMPLE_CAP],
        "rename_triage_count": len(triage_rows),
        "rename_triage_fallback": any(row["triage_status"] == "fallback_pending" for row in triage_rows),
        "rename_triage_source_totals": {
            key: sum(row.get("triage_source") == key for row in triage_rows)
            for key in sorted({row.get("triage_source", "") for row in triage_rows})
        },
        "rename_triage_apply_status_source_totals": {
            key: sum(row.get("apply_status_source") == key for row in triage_rows)
            for key in sorted({row.get("apply_status_source", "") for row in triage_rows})
        },
        "manual_review_count": len(manual_rows),
        "manual_review_sample": manual_rows[:SAMPLE_CAP],
        "manual_review_report": str(MANUAL_REVIEW_MD.relative_to(ROOT)),
        "quality_indicators": quality,
        "quality_regressions": quality_regression_rows,
        "official_recall_indicators": recall,
        "official_recall_regressions": recall_regression_rows,
        "drift_detected": drift_detected,
    }
    REPORT_JSON.parent.mkdir(parents=True, exist_ok=True)
    REPORT_JSON.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    REPORT_MD.write_text(build_markdown(summary), encoding="utf-8")
    write_manual_review_artifacts(triage_rows, generated_at=generated_at)

    gh_out = os.environ.get("GITHUB_OUTPUT")
    if gh_out:
        with open(gh_out, "a", encoding="utf-8") as handle:
            handle.write(f"drift_detected={'true' if drift_detected else 'false'}\n")

    print(json.dumps({k: summary[k] for k in (
        "built_at", "staleness_days", "pending_renames_count",
        "quality_indicators", "drift_detected")}, indent=2))


if __name__ == "__main__":
    main()

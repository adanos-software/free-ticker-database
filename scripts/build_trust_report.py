"""Measure core-universe trust metrics. Advisory only; does not authorize fills."""

from __future__ import annotations

import argparse
import math
import sys
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

try:
    from scripts.lib.dataio import display_path, load_csv, read_json, write_json
except ModuleNotFoundError:  # pragma: no cover - script execution path
    from lib.dataio import display_path, load_csv, read_json, write_json

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

DATA_DIR = ROOT / "data"
REPORTS_DIR = DATA_DIR / "reports"
DEFAULT_LISTINGS_CSV = DATA_DIR / "listings.csv"
DEFAULT_SCOPES_CSV = DATA_DIR / "instrument_scopes.csv"
DEFAULT_ENTRY_QUALITY_CSV = REPORTS_DIR / "entry_quality.csv"
DEFAULT_COLLISION_QUEUE_CSV = REPORTS_DIR / "isin_identity_collision_review_queue.csv"
DEFAULT_QUARANTINE_CSV = REPORTS_DIR / "identifier_quarantine.csv"
DEFAULT_ALIAS_AUDIT_JSON = REPORTS_DIR / "adanos_alias_audit.json"
DEFAULT_COVERAGE_JSON = REPORTS_DIR / "coverage_report.json"
DEFAULT_AUDIT_JSON = REPORTS_DIR / "trust_audit.json"
DEFAULT_WARN_ALLOWLIST_CSV = REPORTS_DIR / "entry_quality_warn_allowlist.csv"
DEFAULT_JSON_OUT = REPORTS_DIR / "trust_report.json"
DEFAULT_MD_OUT = REPORTS_DIR / "trust_report.md"

TRUST_POLICY = (
    "Trust is measured on instrument_scope=core only. accepted_source_gap does not "
    "count as filled. Known identity bugs must be zero; 99% completeness is a fill "
    "rate, not a correctness claim. This report does not authorize inferred "
    "identifiers, sectors, categories, names, or symbol changes. A failing trust "
    "report is not a merge blocker."
)


def utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def filled(value: Any) -> bool:
    return bool(str(value or "").strip())


def split_issue_types(value: str) -> list[str]:
    if not value:
        return []
    return [part.strip() for part in value.replace(";", "|").split("|") if part.strip()]


def split_listing_keys(value: str) -> list[str]:
    if not value:
        return []
    return [part.strip() for part in value.split("|") if part.strip()]


def coverage_stat(n: int, filled_count: int) -> dict[str, Any]:
    pct = 0.0 if n <= 0 else round(100.0 * filled_count / n, 2)
    target = 0 if n <= 0 else math.ceil(0.99 * n)
    missing = max(0, n - filled_count)
    return {
        "n": n,
        "filled": filled_count,
        "missing": missing,
        "pct": pct,
        "fills_needed_for_99": max(0, target - filled_count),
        "pass": filled_count >= target if n else True,
    }


def check(name: str, passed: bool, details: Mapping[str, Any] | None = None) -> dict[str, Any]:
    return {"name": name, "status": "pass" if passed else "fail", "details": dict(details or {})}


def core_listing_keys(scopes: list[dict[str, str]]) -> set[str]:
    return {
        row["listing_key"]
        for row in scopes
        if row.get("instrument_scope") == "core" and row.get("listing_key")
    }


def allowlisted_issue_keys(allowlist: list[dict[str, str]], issue_type: str) -> set[str]:
    return {
        row["listing_key"]
        for row in allowlist
        if row.get("listing_key") and issue_type in split_issue_types(row.get("issue_types", ""))
    }


def build_trust_report(
    *,
    listings: list[dict[str, str]],
    scopes: list[dict[str, str]],
    entry_quality: list[dict[str, str]],
    collision_queue: list[dict[str, str]],
    quarantine: list[dict[str, str]],
    alias_audit: Mapping[str, Any] | None,
    coverage: Mapping[str, Any] | None,
    audit: Mapping[str, Any] | None,
    warn_allowlist: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    core_keys = core_listing_keys(scopes)
    by_key = {row.get("listing_key", ""): row for row in listings if row.get("listing_key")}
    core_rows = [by_key[key] for key in sorted(core_keys) if key in by_key]
    n = len(core_rows)

    isin_filled = sum(1 for row in core_rows if filled(row.get("isin")))
    country_filled = sum(
        1 for row in core_rows if filled(row.get("country")) and filled(row.get("country_code"))
    )
    taxonomy_filled = 0
    missing_sector_by_exchange: Counter[str] = Counter()
    missing_isin_by_exchange: Counter[str] = Counter()
    missing_category_by_exchange: Counter[str] = Counter()
    for row in core_rows:
        exchange = row.get("exchange", "")
        asset_type = row.get("asset_type", "")
        if not filled(row.get("isin")):
            missing_isin_by_exchange[exchange] += 1
        if asset_type == "Stock":
            if filled(row.get("stock_sector")):
                taxonomy_filled += 1
            else:
                missing_sector_by_exchange[exchange] += 1
        elif asset_type == "ETF":
            if filled(row.get("etf_category")):
                taxonomy_filled += 1
            else:
                missing_category_by_exchange[exchange] += 1

    isin_cov = coverage_stat(n, isin_filled)
    country_cov = coverage_stat(n, country_filled)
    taxonomy_cov = coverage_stat(n, taxonomy_filled)

    core_eq = [row for row in entry_quality if row.get("listing_key") in core_keys]
    official_isin_mismatch = [
        row["listing_key"]
        for row in core_eq
        if "official_isin_mismatch" in split_issue_types(row.get("issue_types", ""))
    ]
    reviewed_isin_holds = allowlisted_issue_keys(warn_allowlist or [], "official_isin_mismatch")
    unexpected_isin_mismatch = [
        key for key in official_isin_mismatch if key not in reviewed_isin_holds
    ]
    official_name_mismatch = [
        row["listing_key"]
        for row in core_eq
        if "official_name_mismatch" in split_issue_types(row.get("issue_types", ""))
    ]
    quality_status = Counter(row.get("quality_status", "") for row in core_eq)

    open_collision_groups = []
    for row in collision_queue:
        keys = split_listing_keys(row.get("listing_keys", ""))
        if not any(key in core_keys for key in keys):
            continue
        if str(row.get("closure_status", "")).startswith("open"):
            open_collision_groups.append(row.get("isin", ""))

    core_quarantine = [row for row in quarantine if row.get("listing_key") in core_keys]
    quarantine_actions = Counter(row.get("action", "") for row in core_quarantine)

    alias_findings = int((alias_audit or {}).get("summary", {}).get("blocking_issue_count", 0) or 0)
    global_cov = (coverage or {}).get("global", {}) if isinstance(coverage, Mapping) else {}
    audit_present = isinstance(audit, Mapping) and bool(audit)
    audit_pass = bool(audit_present and audit.get("status") == "pass")

    checks = [
        check(
            "core_identity_known_bugs_zero",
            not unexpected_isin_mismatch and not open_collision_groups,
            {
                "official_isin_mismatch": len(official_isin_mismatch),
                "official_isin_mismatch_unexpected": len(unexpected_isin_mismatch),
                "official_isin_mismatch_reviewed_holds": len(official_isin_mismatch)
                - len(unexpected_isin_mismatch),
                "open_collision_groups": len(open_collision_groups),
                "sample_mismatch": unexpected_isin_mismatch[:10],
                "sample_collision_isins": open_collision_groups[:10],
            },
        ),
        check("core_isin_coverage_99", isin_cov["pass"], isin_cov),
        check("core_taxonomy_coverage_99", taxonomy_cov["pass"], taxonomy_cov),
        check("core_country_coverage_99", country_cov["pass"], country_cov),
        check(
            "core_official_name_hard_mismatches_zero",
            not official_name_mismatch,
            {
                "official_name_mismatch": len(official_name_mismatch),
                "sample": official_name_mismatch[:10],
            },
        ),
        check(
            "core_equity_recall_99",
            False,
            {
                "status": "blocked_wrong_denominator",
                "collision_adjusted_recall_pct": global_cov.get("collision_adjusted_recall_pct"),
                "reason": "Official recall still uses all-tradable directories, not common-stock+ETF.",
            },
        ),
        check("adanos_alias_safe", alias_findings == 0, {"blocking_issue_count": alias_findings}),
        check(
            "stratified_audit_99",
            audit_pass,
            {
                "audit_present": audit_present,
                "reason": "missing_audit" if not audit_present else audit.get("status", "unknown"),
            },
        ),
    ]
    failing = [item["name"] for item in checks if item["status"] != "pass"]
    return {
        "policy": TRUST_POLICY,
        "claim_allowed": not failing,
        "status": "pass" if not failing else "fail",
        "failing_checks": failing,
        "universe": {
            "name": "instrument_scope=core",
            "rows": n,
            "joined_listings": n,
            "scope_rows": len(core_keys),
        },
        "completeness": {
            "isin": isin_cov,
            "country": country_cov,
            "taxonomy": taxonomy_cov,
            "missing_isin_by_exchange": dict(missing_isin_by_exchange.most_common(12)),
            "missing_sector_by_exchange": dict(missing_sector_by_exchange.most_common(12)),
            "missing_category_by_exchange": dict(missing_category_by_exchange.most_common(8)),
        },
        "identity": {
            "official_isin_mismatch": len(official_isin_mismatch),
            "official_name_mismatch": len(official_name_mismatch),
            "open_collision_groups": len(open_collision_groups),
            "quarantine_unresolved": quarantine_actions.get("quarantined_unresolved_identifier", 0),
            "quarantine_proposed_clear": quarantine_actions.get("proposed_clear_conflicting_identifier", 0),
            "quarantine_kept": quarantine_actions.get("kept_listing_keyed_identifier", 0),
        },
        "entry_quality_status": dict(quality_status),
        "checks": checks,
    }


def render_markdown(payload: dict[str, Any], *, generated_at: str) -> str:
    universe = payload["universe"]
    completeness = payload["completeness"]
    identity = payload["identity"]
    lines = [
        "# Core trust report",
        "",
        f"Generated at: `{generated_at}`",
        "",
        payload["policy"],
        "",
        f"Status: **{payload['status'].upper()}**. Public 99% claim allowed: **no**.",
        "",
        f"Universe: `{universe['name']}` — {universe['rows']:,} rows.",
        "",
        "## Completeness",
        "",
        "| Field | Filled | n | Pct | Missing | Fills to 99% | Pass |",
        "|---|---:|---:|---:|---:|---:|---|",
    ]
    for field in ("isin", "country", "taxonomy"):
        stat = completeness[field]
        lines.append(
            f"| {field} | {stat['filled']:,} | {stat['n']:,} | {stat['pct']:.2f}% | "
            f"{stat['missing']:,} | {stat['fills_needed_for_99']:,} | {str(stat['pass']).lower()} |"
        )
    lines.extend(
        [
            "",
            "## Identity",
            "",
            "| Metric | Count |",
            "|---|---:|",
            f"| official_isin_mismatch | {identity['official_isin_mismatch']:,} |",
            f"| official_name_mismatch | {identity['official_name_mismatch']:,} |",
            f"| open_collision_groups | {identity['open_collision_groups']:,} |",
            f"| quarantine_unresolved | {identity['quarantine_unresolved']:,} |",
            f"| quarantine_proposed_clear | {identity['quarantine_proposed_clear']:,} |",
            "",
            "## Checks",
            "",
            "| Check | Status |",
            "|---|---|",
        ]
    )
    for item in payload["checks"]:
        lines.append(f"| `{item['name']}` | **{item['status']}** |")
    lines.extend(
        [
            "",
            "This report never claims 99% correctness without `data/reports/trust_audit.json`.",
            "",
        ]
    )
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build the core trust measurement report.")
    parser.add_argument("--listings-csv", type=Path, default=DEFAULT_LISTINGS_CSV)
    parser.add_argument("--scopes-csv", type=Path, default=DEFAULT_SCOPES_CSV)
    parser.add_argument("--entry-quality-csv", type=Path, default=DEFAULT_ENTRY_QUALITY_CSV)
    parser.add_argument("--collision-queue-csv", type=Path, default=DEFAULT_COLLISION_QUEUE_CSV)
    parser.add_argument("--quarantine-csv", type=Path, default=DEFAULT_QUARANTINE_CSV)
    parser.add_argument("--alias-audit-json", type=Path, default=DEFAULT_ALIAS_AUDIT_JSON)
    parser.add_argument("--coverage-json", type=Path, default=DEFAULT_COVERAGE_JSON)
    parser.add_argument("--audit-json", type=Path, default=DEFAULT_AUDIT_JSON)
    parser.add_argument("--warn-allowlist-csv", type=Path, default=DEFAULT_WARN_ALLOWLIST_CSV)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_JSON_OUT)
    parser.add_argument("--md-out", type=Path, default=DEFAULT_MD_OUT)
    args = parser.parse_args(argv)

    generated_at = utc_now()
    report = build_trust_report(
        listings=load_csv(args.listings_csv),
        scopes=load_csv(args.scopes_csv),
        entry_quality=load_csv(args.entry_quality_csv),
        collision_queue=load_csv(args.collision_queue_csv),
        quarantine=load_csv(args.quarantine_csv),
        alias_audit=read_json(args.alias_audit_json, default={}),
        coverage=read_json(args.coverage_json, default={}),
        audit=read_json(args.audit_json, default=None),
        warn_allowlist=load_csv(args.warn_allowlist_csv),
    )
    payload = {
        "_meta": {
            "generated_at": generated_at,
            "source_files": {
                "listings_csv": display_path(args.listings_csv, ROOT),
                "instrument_scopes_csv": display_path(args.scopes_csv, ROOT),
                "entry_quality_csv": display_path(args.entry_quality_csv, ROOT),
                "isin_identity_collision_review_queue_csv": display_path(args.collision_queue_csv, ROOT),
                "identifier_quarantine_csv": display_path(args.quarantine_csv, ROOT),
                "adanos_alias_audit_json": display_path(args.alias_audit_json, ROOT),
                "coverage_report_json": display_path(args.coverage_json, ROOT),
                "trust_audit_json": display_path(args.audit_json, ROOT),
                "entry_quality_warn_allowlist_csv": display_path(args.warn_allowlist_csv, ROOT),
            },
            "policy": TRUST_POLICY,
        },
        **report,
    }
    write_json(args.json_out, payload)
    args.md_out.parent.mkdir(parents=True, exist_ok=True)
    args.md_out.write_text(render_markdown(payload, generated_at=generated_at), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

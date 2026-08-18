"""Evaluate hierarchical merge, stable-release, and completeness contracts."""

from __future__ import annotations

import argparse
import csv
import hashlib
from collections import Counter
import json
import os
import subprocess
from pathlib import Path
from typing import Any, Mapping

try:
    from scripts.build_coverage_contracts import parse_time, source_license_approved
    from scripts.check_workflow_source_policy import check_repository
    from scripts.validate_canonical_v4_exports import validate as validate_canonical
    from scripts.lib.identity_integrity import find_identity_conflicts, listing_key as identity_listing_key
    from scripts.lib.review_adjudications import valid_isin
except ModuleNotFoundError:  # pragma: no cover
    from build_coverage_contracts import parse_time, source_license_approved
    from check_workflow_source_policy import check_repository
    from validate_canonical_v4_exports import validate as validate_canonical
    from lib.identity_integrity import find_identity_conflicts, listing_key as identity_listing_key
    from lib.review_adjudications import valid_isin

ROOT = Path(__file__).resolve().parents[1]
QUALITY_JSON = ROOT / "data/reports/quality_contract.json"
QUALITY_MD = ROOT / "data/reports/quality_contract.md"
PROFILE_LEVEL = {"merge": 1, "stable": 2, "complete": 3}


def load_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def load_json(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def git_commit(root: Path) -> str:
    value = os.environ.get("GITHUB_SHA", "")
    if value:
        return value
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True, stderr=subprocess.DEVNULL).strip()
    except (OSError, subprocess.CalledProcessError):
        return ""


def check(name: str, passed: bool, details: Mapping[str, Any] | None = None, *, scope: str = "merge") -> dict[str, Any]:
    return {"name": name, "scope": scope, "status": "pass" if passed else "fail", "details": dict(details or {})}


def _listing_key(row: Mapping[str, str]) -> str:
    return row.get("listing_key") or f"{row.get('exchange','')}::{row.get('ticker','')}"


def evaluate(
    *, root: Path = ROOT, profile: str = "merge", expected_git_commit: str | None = None
) -> dict[str, Any]:
    if profile not in PROFILE_LEVEL:
        raise ValueError(f"unknown quality profile: {profile}")
    data = root / "data"
    expected_git_commit = expected_git_commit or git_commit(root)
    reports = data / "reports"
    checks: list[dict[str, Any]] = []

    validation_report = load_json(reports / "validation_report.json", {}) or {}
    checks.append(check("legacy_database_validation", bool(validation_report.get("passed")), {"summary": validation_report.get("summary", {})}))

    policy_violations = check_repository(root)
    checks.append(check("reviewable_source_and_workflow_policy", not policy_violations, {"violations": [item.__dict__ for item in policy_violations[:50]], "count": len(policy_violations)}))

    try:
        canonical_report = validate_canonical(
            data_dir=data / "canonical_v4",
            contract_json=root / "schema/canonical_v4_contract.json",
            schema_sql=root / "schema/canonical_v4.sql",
            compatibility_listings=data / "listings.csv",
            expected_git_commit=expected_git_commit or None,
        )
        canonical_error = ""
    except Exception as exc:  # noqa: BLE001 - rendered as gate evidence
        canonical_report = {"status": "fail"}
        canonical_error = str(exc)
    checks.append(check("canonical_v4_exports_and_schema", canonical_report.get("status") == "pass", {"error": canonical_error, "counts": canonical_report.get("counts", {})}))

    safe_merge = load_json(reports / "safe_merge.json", {}) or {}
    checks.append(check("safe_merge_gate", safe_merge.get("status") == "pass", {"status": safe_merge.get("status", "missing"), "summary": safe_merge.get("summary", {})}))

    listings = load_csv(data / "listings.csv")
    snapshot = load_csv(data / "history/latest_snapshot.csv")
    listing_counts = Counter(_listing_key(row) for row in listings)
    snapshot_counts = Counter(_listing_key(row) for row in snapshot)
    listing_keys = set(listing_counts)
    snapshot_keys = set(snapshot_counts)
    duplicate_listing_keys = sorted(key for key, count in listing_counts.items() if key and count > 1)
    duplicate_snapshot_keys = sorted(key for key, count in snapshot_counts.items() if key and count > 1)
    checks.append(check(
        "current_snapshot_keyset",
        bool(listing_keys)
        and listing_keys == snapshot_keys
        and not duplicate_listing_keys
        and not duplicate_snapshot_keys,
        {
            "listings": len(listing_keys), "snapshot": len(snapshot_keys),
            "missing": len(listing_keys - snapshot_keys), "extra": len(snapshot_keys - listing_keys),
            "duplicate_listing_keys": len(duplicate_listing_keys),
            "duplicate_snapshot_keys": len(duplicate_snapshot_keys),
        },
    ))

    quarantine = load_csv(reports / "identifier_quarantine.csv")
    current_by_key = {_listing_key(row): row for row in listings}
    quarantine_failures = []
    decision_pairs: set[tuple[str, str]] = set()
    for row in quarantine:
        key = row.get("listing_key", "")
        isin = row.get("isin", "").upper()
        decision_pairs.add((isin, key))
        current = current_by_key.get(key)
        if current is None:
            quarantine_failures.append({"listing_key": key, "reason": "decision listing absent"})
            continue
        actual = current.get("isin", "").upper()
        expected = row.get("retained_isin", "").upper()
        if actual != expected:
            quarantine_failures.append({
                "listing_key": key,
                "reason": f"decision {row.get('action','')} expects {expected!r}, got {actual!r}",
            })
    current_conflicts = find_identity_conflicts(listings)
    for conflict in current_conflicts:
        for member in conflict.rows:
            key = identity_listing_key(member)
            if (conflict.isin, key) not in decision_pairs:
                quarantine_failures.append({
                    "listing_key": key,
                    "reason": f"current conflict {conflict.isin} is missing from quarantine ledger",
                })
    checks.append(check(
        "identifier_adjudications_enforced",
        (reports / "identifier_quarantine.csv").exists() and not quarantine_failures,
        {
            "decisions": len(quarantine), "current_conflict_groups": len(current_conflicts),
            "failure_count": len(quarantine_failures), "sample": quarantine_failures[:50],
        },
    ))

    reconciliation = load_csv(reports / "reference_reconciliation.csv")
    unclassified = [row for row in reconciliation if not row.get("classification")]
    checks.append(check("reference_observations_classified", bool(reconciliation) and not unclassified, {"rows": len(reconciliation), "unclassified": len(unclassified)}))

    sources = load_json(data / "masterfiles/sources.json", []) or []
    required_source_fields = {
        "key", "provider", "source_url", "reference_scope", "authority_level", "license_status",
        "derived_facts_redistribution_status", "raw_redistribution_allowed", "attribution_required",
        "commercial_use_status", "terms_version", "terms_sha256", "license_reviewed_at",
        "freshness_sla_days", "enabled",
    }
    source_schema_failures = [
        {"source_key": source.get("key", ""), "missing": sorted(required_source_fields - set(source))}
        for source in sources if required_source_fields - set(source)
    ]
    checks.append(check("source_registry_governance_schema", bool(sources) and not source_schema_failures, {"sources": len(sources), "failure_count": len(source_schema_failures), "sample": source_schema_failures[:25]}))

    # Stable-release gates: every failure is intentionally visible even when the
    # selected profile is only merge.
    checks.append(check(
        "zero_unresolved_identifier_conflicts",
        not current_conflicts,
        {
            "conflict_groups": len(current_conflicts),
            "sample": [conflict.isin for conflict in current_conflicts[:50]],
        },
        scope="stable",
    ))
    name_reconciliations = load_csv(reports / "official_name_reconciliation.csv")
    proposed_names = [row for row in name_reconciliations if row.get("action") == "proposed"]
    checks.append(check(
        "official_name_reconciliation_resolved",
        not proposed_names,
        {
            "proposed_rows": len(proposed_names),
            "sample": [row.get("listing_key", "") for row in proposed_names[:50]],
        },
        scope="stable",
    ))
    contracts = load_csv(reports / "coverage_contracts.csv")
    failing_full = [row for row in contracts if row.get("claim_type") == "official_full" and row.get("contract_status") != "pass"]
    checks.append(check("all_official_full_coverage_contracts_pass", bool(contracts) and not failing_full, {"contracts": len(contracts), "failing_full": len(failing_full), "sample": [row.get("contract_key") for row in failing_full[:50]]}, scope="stable"))

    manifest = load_json(data / "canonical_v4/manifest.json", {}) or {}
    license_as_of = parse_time(str(manifest.get("generated_at", "")))

    contributing = set()
    for row in contracts:
        if row.get("claim_type") == "official_full":
            contributing.update(item for item in row.get("source_keys", "").split("|") if item)
    source_by_key = {str(row.get("key", "")): row for row in sources}
    license_failures = []
    for key in sorted(contributing):
        approved, reason = source_license_approved(
            source_by_key.get(key, {}), as_of=license_as_of
        )
        if not approved:
            license_failures.append({"source_key": key, "reason": reason})
    checks.append(check("contributing_source_licenses_verified", bool(contributing) and not license_failures, {"sources": len(contributing), "failures": len(license_failures), "sample": license_failures[:50]}, scope="stable"))

    provenance_gaps = load_csv(data / "canonical_v4/provenance_gaps.csv")
    checks.append(check("field_level_provenance_complete", (data / "canonical_v4/provenance_gaps.csv").exists() and not provenance_gaps, {"gap_rows": len(provenance_gaps)}, scope="stable"))

    venues = load_csv(data / "canonical_v4/venues.csv")
    missing_mic = [row for row in venues if not row.get("operating_mic") and not row.get("segment_mic")]
    checks.append(check("venue_mic_mapping_complete", bool(venues) and not missing_mic, {"missing_venues": len(missing_mic), "sample": [row.get("exchange_code") for row in missing_mic[:50]]}, scope="stable"))

    # Complete-database gates.
    missing_isin = [row for row in listings if not row.get("isin")]
    missing_country = [row for row in listings if not row.get("country") or not row.get("country_code")]
    missing_stock_sector = [row for row in listings if row.get("asset_type") == "Stock" and not row.get("stock_sector")]
    missing_etf_category = [row for row in listings if row.get("asset_type") == "ETF" and not row.get("etf_category")]
    checks.extend([
        check("complete_listing_isin_coverage", not missing_isin, {"missing_rows": len(missing_isin)}, scope="complete"),
        check("complete_country_metadata", not missing_country, {"missing_rows": len(missing_country)}, scope="complete"),
        check("complete_stock_sector_metadata", not missing_stock_sector, {"missing_rows": len(missing_stock_sector)}, scope="complete"),
        check("complete_etf_category_metadata", not missing_etf_category, {"missing_rows": len(missing_etf_category)}, scope="complete"),
    ])
    missing_reference = [row for row in reconciliation if row.get("classification") == "missing_from_database"]
    checks.append(check("zero_in_scope_official_reference_gaps", not missing_reference, {"missing_rows": len(missing_reference)}, scope="complete"))
    non_full = [row for row in contracts if row.get("claim_type") != "official_full"]
    checks.append(check("all_target_venues_have_full_contracts", bool(contracts) and not non_full, {"partial_contracts": len(non_full)}, scope="complete"))

    failures_by_profile: dict[str, list[dict[str, Any]]] = {}
    for target, level in PROFILE_LEVEL.items():
        failures_by_profile[target] = [
            item for item in checks
            if PROFILE_LEVEL[item["scope"]] <= level and item["status"] != "pass"
        ]
    selected_failures = failures_by_profile[profile]
    result = {
        "profile": profile,
        "status": "pass" if not selected_failures else "fail",
        "merge_status": "pass" if not failures_by_profile["merge"] else "fail",
        "stable_status": "pass" if not failures_by_profile["stable"] else "fail",
        "complete_status": "pass" if not failures_by_profile["complete"] else "fail",
        "dataset_sha256": sha256_file(data / "listings.csv") if (data / "listings.csv").exists() else "",
        "source_manifest_sha256": sha256_file(data / "masterfiles/sources.json") if (data / "masterfiles/sources.json").exists() else "",
        "git_commit": expected_git_commit,
        "summary": {
            "checks": len(checks),
            "merge_failures": len(failures_by_profile["merge"]),
            "stable_failures": len(failures_by_profile["stable"]),
            "complete_failures": len(failures_by_profile["complete"]),
            "selected_failures": len(selected_failures),
        },
        "checks": checks,
    }
    return result


def write(result: Mapping[str, Any], *, json_path: Path = QUALITY_JSON, md_path: Path = QUALITY_MD) -> None:
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    lines = [
        "# Canonical quality contract", "",
        f"- Selected profile: `{result['profile']}`",
        f"- Selected status: **{str(result['status']).upper()}**",
        f"- Merge status: **{str(result['merge_status']).upper()}**",
        f"- Stable-release status: **{str(result['stable_status']).upper()}**",
        f"- Complete-database status: **{str(result['complete_status']).upper()}**",
        "", "| Scope | Check | Status |", "|---|---|---|",
    ]
    for item in result["checks"]:
        lines.append(f"| {item['scope']} | `{item['name']}` | **{item['status'].upper()}** |")
    lines.extend([
        "",
        "`merge` protects the reviewable code/data transition. `stable` additionally requires full official coverage, verified contributing-source rights, complete field provenance, and MIC mappings. `complete` additionally requires zero metadata and official-reference gaps.",
        "",
    ])
    md_path.write_text("\n".join(lines), encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=tuple(PROFILE_LEVEL), default="merge")
    parser.add_argument("--strict", action="store_true")
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--expected-git-commit")
    parser.add_argument("--json-out", type=Path)
    parser.add_argument("--md-out", type=Path)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    root = args.root.resolve()
    result = evaluate(
        root=root, profile=args.profile, expected_git_commit=args.expected_git_commit
    )
    write(
        result,
        json_path=args.json_out or root / "data/reports/quality_contract.json",
        md_path=args.md_out or root / "data/reports/quality_contract.md",
    )
    print(json.dumps(result["summary"], indent=2))
    return 1 if args.strict and result["status"] != "pass" else 0


if __name__ == "__main__":
    raise SystemExit(main())

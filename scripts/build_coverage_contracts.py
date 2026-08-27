"""Build conservative venue/product coverage contracts from official observations."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

try:
    from scripts.build_reference_reconciliation import COVERED_CLASSIFICATIONS, load_csv
except ModuleNotFoundError:  # pragma: no cover
    from build_reference_reconciliation import COVERED_CLASSIFICATIONS, load_csv

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
REFERENCE_CSV = DATA_DIR / "masterfiles" / "reference.csv"
SOURCES_JSON = DATA_DIR / "masterfiles" / "sources.json"
MASTERFILE_SUMMARY_JSON = DATA_DIR / "masterfiles" / "summary.json"
EXCHANGE_AUDIT_CSV = DATA_DIR / "reports" / "exchange_source_audit.csv"
RECONCILIATION_CSV = DATA_DIR / "reports" / "reference_reconciliation.csv"
REPORTS_DIR = DATA_DIR / "reports"
OUT_CSV = REPORTS_DIR / "coverage_contracts.csv"
OUT_JSON = REPORTS_DIR / "coverage_contracts.json"
OUT_MD = REPORTS_DIR / "coverage_contracts.md"
MINIMUM_FULL_RECALL_PCT = 99.5
TERMS_SHA_RE = re.compile(r"^[0-9a-f]{64}$")


def parse_time(value: str) -> datetime | None:
    value = (value or "").strip()
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


def source_license_approved(
    source: Mapping[str, Any], *, as_of: datetime | None = None
) -> tuple[bool, str]:
    as_of = as_of or datetime.now(timezone.utc)
    if source.get("license_status") == "internal" and str(source.get("source_url", "")).startswith("internal://"):
        return True, "internal governed source"
    required = {
        "license_status": "verified_open",
        "derived_facts_redistribution_status": "allowed",
        "commercial_use_status": "allowed",
    }
    for field, expected in required.items():
        if str(source.get(field, "")) != expected:
            return False, f"{field} is not {expected}"
    if not str(source.get("license_name", "")).strip() or not str(source.get("license_url", "")).startswith("https://"):
        return False, "license name/URL missing"
    if not str(source.get("terms_version", "")).strip() or not TERMS_SHA_RE.fullmatch(str(source.get("terms_sha256", "")).lower()):
        return False, "reviewed terms version/hash missing"
    reviewed_at = parse_time(str(source.get("license_reviewed_at", "")))
    if reviewed_at is None:
        return False, "license_reviewed_at missing or not timezone-aware"
    if reviewed_at.astimezone(timezone.utc) > as_of.astimezone(timezone.utc) + timedelta(minutes=5):
        return False, "license_reviewed_at is in the future"
    if str(source.get("attribution_required", "")) not in {"none", "required"}:
        return False, "attribution requirement not adjudicated"
    return True, "verified open derived-facts license"


def freshness_as_of(
    *,
    dataset_as_of: datetime,
    source_detail: Mapping[str, Any] | None = None,
    last_refresh: Mapping[str, Any] | None = None,
) -> datetime:
    """Evaluate a post-build official snapshot on its own observation clock.

    Dataset `as_of` stays pinned to built_at so other venues are not aged by a
    later targeted refresh. A source whose `generated_at` is after built_at but
    not after the latest recorded `last_refresh` is aged against that envelope
    so later fetches of other sources still advance the SLA clock.
    """
    if not last_refresh:
        return dataset_as_of
    refresh_at = parse_time(str(last_refresh.get("generated_at") or ""))
    generated_at = parse_time(str((source_detail or {}).get("generated_at") or ""))
    if refresh_at is None or generated_at is None:
        return dataset_as_of
    refresh_utc = refresh_at.astimezone(timezone.utc)
    generated_utc = generated_at.astimezone(timezone.utc)
    as_of_utc = dataset_as_of.astimezone(timezone.utc)
    if generated_utc <= as_of_utc:
        return dataset_as_of
    if generated_utc > refresh_utc + timedelta(minutes=5):
        return dataset_as_of
    return refresh_utc


def source_fresh(
    source: Mapping[str, Any], source_detail: Mapping[str, Any] | None, *, as_of: datetime
) -> tuple[bool, str]:
    if source_detail is None:
        return False, "source snapshot metadata missing"
    if str(source_detail.get("mode", "")) == "unavailable":
        return False, "latest refresh is unavailable"
    generated_at = parse_time(str(source_detail.get("generated_at", "")))
    if generated_at is None:
        return False, "source snapshot timestamp missing"
    generated_utc = generated_at.astimezone(timezone.utc)
    as_of_utc = as_of.astimezone(timezone.utc)
    if generated_utc > as_of_utc + timedelta(minutes=5):
        return False, "source snapshot timestamp is in the future"
    age_days = max(0.0, (as_of_utc - generated_utc).total_seconds() / 86400)
    sla = int(source.get("freshness_sla_days") or 0)
    if sla <= 0:
        return False, "freshness SLA missing"
    if age_days > sla:
        return False, f"snapshot age {age_days:.2f}d exceeds {sla}d SLA"
    return True, f"snapshot age {age_days:.2f}d within {sla}d SLA"


def _split(value: str) -> set[str]:
    return {item for item in (value or "").split("|") if item}


def _int(row: Mapping[str, Any], field: str) -> int:
    value = row.get(field, 0)
    return int(value or 0)


def build_contract_rows(
    *,
    references: Sequence[Mapping[str, str]],
    reconciliations: Sequence[Mapping[str, str]],
    sources: Mapping[str, Mapping[str, Any]],
    source_details: Mapping[str, Mapping[str, Any]],
    exchange_audit: Mapping[str, Mapping[str, Any]],
    as_of: datetime,
    last_refresh: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    official = [
        row for row in references
        if str(row.get("official", "")).lower() == "true" and str(row.get("listing_status", "")) == "active"
    ]
    source_keys_by_group: dict[tuple[str, str], set[str]] = defaultdict(set)
    observed_keys_by_group: dict[tuple[str, str], set[str]] = defaultdict(set)
    scopes_by_group: dict[tuple[str, str], set[str]] = defaultdict(set)
    for row in official:
        exchange = str(row.get("exchange", "")).strip()
        asset_type = str(row.get("asset_type", "") or "Unknown").strip()
        source_key = str(row.get("source_key", "")).strip()
        if not exchange or not source_key:
            continue
        group = (exchange, asset_type)
        source_keys_by_group[group].add(source_key)
        observed_keys_by_group[group].add(f"{exchange}::{str(row.get('ticker', '')).strip().upper()}")
        scopes_by_group[group].add(str(row.get("reference_scope", "")).strip())

    reconciliation_by_group_key: dict[tuple[str, str, str], list[Mapping[str, str]]] = defaultdict(list)
    for row in reconciliations:
        exchange = str(row.get("exchange", "")).strip()
        asset_types = _split(str(row.get("asset_types", ""))) or {"Unknown"}
        for asset_type in asset_types:
            reconciliation_by_group_key[(exchange, asset_type, str(row.get("reference_key", "")))].append(row)

    rows: list[dict[str, Any]] = []
    for group in sorted(source_keys_by_group):
        exchange, asset_type = group
        audit = exchange_audit.get(exchange, {})
        source_keys = sorted(source_keys_by_group[group])
        venue_status = str(audit.get("venue_status", "missing"))
        claim_type = "official_full" if venue_status == "official_full" else "official_partial"
        denominator_field = "official_active_stock_rows" if asset_type == "Stock" else "official_active_etf_rows"
        if asset_type not in {"Stock", "ETF"}:
            denominator_field = ""
        audit_denominator = _int(audit, denominator_field) if denominator_field else 0
        observed = observed_keys_by_group[group]
        denominator = audit_denominator if claim_type == "official_full" else len(observed)

        classification_by_key: dict[str, set[str]] = defaultdict(set)
        credited_keys: set[str] = set()
        conflict_keys: set[str] = set()
        unclassified_keys: set[str] = set()
        for key in observed:
            recs = reconciliation_by_group_key.get((exchange, asset_type, key), [])
            if not recs:
                unclassified_keys.add(key)
                continue
            for rec in recs:
                classification = str(rec.get("classification", ""))
                if classification:
                    classification_by_key[key].add(classification)
                else:
                    unclassified_keys.add(key)
                if str(rec.get("coverage_credit", "")).lower() == "true" and classification in COVERED_CLASSIFICATIONS:
                    credited_keys.add(key)
                if classification in {"exact_identity_conflict", "ambiguous_same_venue_identifier", "mixed_scope_conflict"}:
                    conflict_keys.add(key)

        covered = len(credited_keys)
        missing = max(0, denominator - covered)
        recall_pct = 100.0 * covered / denominator if denominator else None

        freshness_failures: list[str] = []
        license_failures: list[str] = []
        for source_key in source_keys:
            source = sources.get(source_key, {})
            source_as_of = freshness_as_of(
                dataset_as_of=as_of,
                source_detail=source_details.get(source_key),
                last_refresh=last_refresh,
            )
            fresh, fresh_reason = source_fresh(
                source, source_details.get(source_key), as_of=source_as_of
            )
            if not fresh:
                freshness_failures.append(f"{source_key}: {fresh_reason}")
            licensed, license_reason = source_license_approved(source, as_of=as_of)
            if not licensed:
                license_failures.append(f"{source_key}: {license_reason}")

        if claim_type != "official_full":
            status = "partial_scope_observed"
        elif denominator <= 0:
            status = "fail_no_denominator"
        elif covered > denominator or len(observed) > denominator:
            status = "fail_denominator_inconsistent"
        elif conflict_keys:
            status = "fail_identity_conflict"
        elif unclassified_keys:
            status = "fail_unclassified"
        elif freshness_failures:
            status = "fail_freshness"
        elif license_failures:
            status = "fail_license"
        elif recall_pct is None or recall_pct < MINIMUM_FULL_RECALL_PCT:
            status = "fail_recall"
        else:
            status = "pass"

        contract_key = f"{exchange}::{asset_type}"
        rows.append({
            "contract_id": hashlib.sha256(contract_key.encode("utf-8")).hexdigest()[:24],
            "contract_key": contract_key,
            "exchange": exchange,
            "asset_type": asset_type,
            "claim_type": claim_type,
            "venue_status": venue_status,
            "source_keys": "|".join(source_keys),
            "reference_scopes": "|".join(sorted(scopes_by_group[group])),
            "denominator_method": "exchange_audit_official_active_product_rows" if claim_type == "official_full" else "observed_partial_scope_keys",
            "denominator": denominator,
            "observed_reference_keys": len(observed),
            "covered_reference_keys": covered,
            "missing_reference_keys": missing,
            "identity_conflict_keys": len(conflict_keys),
            "unclassified_keys": len(unclassified_keys),
            "recall_pct": "" if recall_pct is None else round(recall_pct, 6),
            "minimum_recall_pct": MINIMUM_FULL_RECALL_PCT if claim_type == "official_full" else "",
            "freshness_status": "fail" if freshness_failures else "pass",
            "license_status": "fail" if license_failures else "pass",
            "contract_status": status,
            "freshness_failures": " | ".join(freshness_failures),
            "license_failures": " | ".join(license_failures),
            "required_next_action": (
                "maintain source freshness, licensing evidence, and regression monitoring"
                if status == "pass" else
                "do not promote partial scope to a full completeness claim"
                if status == "partial_scope_observed" else
                "resolve the failing contract evidence before stable release"
            ),
        })
    return rows


def build(
    *,
    reference_csv: Path = REFERENCE_CSV,
    sources_json: Path = SOURCES_JSON,
    summary_json: Path = MASTERFILE_SUMMARY_JSON,
    exchange_audit_csv: Path = EXCHANGE_AUDIT_CSV,
    reconciliation_csv: Path = RECONCILIATION_CSV,
    out_csv: Path = OUT_CSV,
    out_json: Path = OUT_JSON,
    out_md: Path = OUT_MD,
    as_of: datetime | None = None,
) -> dict[str, Any]:
    as_of = as_of or datetime.now(timezone.utc)
    sources_payload = json.loads(sources_json.read_text(encoding="utf-8"))
    sources = {str(row.get("key", "")): row for row in sources_payload if isinstance(row, dict)}
    summary_payload = json.loads(summary_json.read_text(encoding="utf-8")) if summary_json.exists() else {}
    details = summary_payload.get("source_details", {}) if isinstance(summary_payload, dict) else {}
    last_refresh = summary_payload.get("last_refresh") if isinstance(summary_payload, dict) else None
    if not isinstance(last_refresh, dict):
        last_refresh = None
    audit = {row.get("exchange", ""): row for row in load_csv(exchange_audit_csv)}
    rows = build_contract_rows(
        references=load_csv(reference_csv),
        reconciliations=load_csv(reconciliation_csv),
        sources=sources,
        source_details=details,
        exchange_audit=audit,
        as_of=as_of,
        last_refresh=last_refresh,
    )
    counts = Counter(str(row["contract_status"]) for row in rows)
    full_rows = [row for row in rows if row["claim_type"] == "official_full"]
    summary = {
        "generated_at": as_of.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "contracts": len(rows),
        "official_full_contracts": len(full_rows),
        "official_full_pass": sum(row["contract_status"] == "pass" for row in full_rows),
        "official_full_fail": sum(row["contract_status"] != "pass" for row in full_rows),
        "status_counts": dict(sorted(counts.items())),
        "minimum_full_recall_pct": MINIMUM_FULL_RECALL_PCT,
    }
    fieldnames = [
        "contract_id", "contract_key", "exchange", "asset_type", "claim_type", "venue_status",
        "source_keys", "reference_scopes", "denominator_method", "denominator",
        "observed_reference_keys", "covered_reference_keys", "missing_reference_keys",
        "identity_conflict_keys", "unclassified_keys", "recall_pct", "minimum_recall_pct",
        "freshness_status", "license_status", "contract_status", "freshness_failures",
        "license_failures", "required_next_action",
    ]
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader(); writer.writerows(rows)
    out_json.write_text(json.dumps({"summary": summary, "contracts": rows}, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    lines = [
        "# Coverage contracts", "",
        f"- Contracts: **{len(rows):,}**",
        f"- Official-full contracts: **{len(full_rows):,}**",
        f"- Official-full passing all recall/freshness/license/identity gates: **{summary['official_full_pass']:,}**",
        f"- Official-full failing: **{summary['official_full_fail']:,}**",
        "", "| Status | Contracts |", "|---|---:|",
    ]
    lines.extend(f"| `{status}` | {count:,} |" for status, count in sorted(counts.items()))
    lines.extend(["", "A full contract passes only with a current official denominator, venue-specific identity-aware recall of at least 99.5%, fresh source snapshots, and verified redistribution/commercial-use evidence.", ""])
    out_md.write_text("\n".join(lines), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return {"summary": summary, "contracts": rows}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference-csv", type=Path, default=REFERENCE_CSV)
    parser.add_argument("--sources-json", type=Path, default=SOURCES_JSON)
    parser.add_argument("--summary-json", type=Path, default=MASTERFILE_SUMMARY_JSON)
    parser.add_argument("--exchange-audit-csv", type=Path, default=EXCHANGE_AUDIT_CSV)
    parser.add_argument("--reconciliation-csv", type=Path, default=RECONCILIATION_CSV)
    parser.add_argument("--out-csv", type=Path, default=OUT_CSV)
    parser.add_argument("--out-json", type=Path, default=OUT_JSON)
    parser.add_argument("--out-md", type=Path, default=OUT_MD)
    parser.add_argument("--as-of", help="Timezone-aware ISO-8601 evaluation timestamp")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    as_of = parse_time(args.as_of) if args.as_of else None
    if args.as_of and as_of is None:
        raise SystemExit("--as-of must be timezone-aware ISO-8601")
    build(
        reference_csv=args.reference_csv, sources_json=args.sources_json,
        summary_json=args.summary_json, exchange_audit_csv=args.exchange_audit_csv,
        reconciliation_csv=args.reconciliation_csv, out_csv=args.out_csv,
        out_json=args.out_json, out_md=args.out_md, as_of=as_of,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

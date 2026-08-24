"""Atomic canonical rebuild entrypoint.

This is the only supported operational rebuild command. It wraps the legacy
compatibility exporter with strict listing-keyed identifier adjudication, then
synchronises extended identifiers, listing history, and the canonical-v4 bridge.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

try:
    from scripts import (
        build_adanos_ticker_reference, build_canonical_v4, build_coverage_contracts,
        build_coverage_report, build_entry_quality_report, build_exchange_source_audit,
        build_listing_history, build_reference_reconciliation,
        build_source_gap_classification, build_source_of_truth_decisions,
        enrich_global_identifiers, normalize_source_registry, update_readme_snapshot,
    )
    from scripts.lib.identity_integrity import (
        ResolutionDecision, decision_rows, find_identity_conflicts, is_full_identity_name,
        listing_key, names_refer_to_same_identity, resolve_identity_conflicts,
    )
    from scripts.lib.review_adjudications import keep_listing_keys_by_isin
except ModuleNotFoundError:  # pragma: no cover - direct script execution
    import build_adanos_ticker_reference
    import build_canonical_v4
    import build_coverage_contracts
    import build_coverage_report
    import build_entry_quality_report
    import build_exchange_source_audit
    import build_listing_history
    import build_reference_reconciliation
    import build_source_gap_classification
    import build_source_of_truth_decisions
    import enrich_global_identifiers
    import normalize_source_registry
    import update_readme_snapshot
    from lib.identity_integrity import (
        ResolutionDecision, decision_rows, find_identity_conflicts, is_full_identity_name,
        listing_key, names_refer_to_same_identity, resolve_identity_conflicts,
    )
    from lib.review_adjudications import keep_listing_keys_by_isin


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
REPORTS_DIR = DATA_DIR / "reports"
QUARANTINE_CSV = REPORTS_DIR / "identifier_quarantine.csv"
QUARANTINE_JSON = REPORTS_DIR / "identifier_quarantine.json"
QUARANTINE_MD = REPORTS_DIR / "identifier_quarantine.md"
OFFICIAL_NAME_RECONCILIATION_CSV = REPORTS_DIR / "official_name_reconciliation.csv"
OFFICIAL_NAME_RECONCILIATION_JSON = REPORTS_DIR / "official_name_reconciliation.json"
OFFICIAL_NAME_RECONCILIATION_MD = REPORTS_DIR / "official_name_reconciliation.md"

_DECISIONS: list[ResolutionDecision] = []
_NAME_RECONCILIATIONS: list[dict[str, str]] = []
_REBUILD_DATASET: Any | None = None
_ORIGINAL_CLEANSE: Any | None = None
_ORIGINAL_CLEANED_ROWS: Any | None = None
_APPLY_IDENTITY_FIXES = False
_APPLY_OFFICIAL_NAME_UPDATES = False


def _dataset_module() -> Any:
    global _REBUILD_DATASET
    if _REBUILD_DATASET is None:
        import importlib
        name = "scripts.rebuild_dataset" if __package__ else "rebuild_dataset"
        _REBUILD_DATASET = importlib.import_module(name)
    return _REBUILD_DATASET


def _built_at() -> str:
    try:
        payload = json.loads((DATA_DIR / "tickers.json").read_text(encoding="utf-8"))
        return str(payload.get("_meta", {}).get("built_at", ""))
    except (OSError, json.JSONDecodeError):
        return ""


def _built_at_datetime() -> datetime:
    value = _built_at()
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("canonical dataset built_at must be timezone-aware ISO-8601") from exc
    if parsed.tzinfo is None:
        raise ValueError("canonical dataset built_at must be timezone-aware ISO-8601")
    return parsed


def _official_isin_by_listing() -> dict[str, set[str]]:
    result: dict[str, set[str]] = defaultdict(set)
    dataset = _dataset_module()
    for (ticker, exchange, _asset_type), isin in dataset.load_active_official_isin_fallbacks().items():
        if isin:
            result[f"{exchange}::{ticker}"].add(isin.strip().upper())
    return dict(result)


_REFERENCE_SCOPE_PRIORITY = {
    "exchange_directory": 0,
    "security_identifier_registry": 1,
    "security_identifier_registry_subset": 2,
    "security_lookup_subset": 3,
    "listed_companies_subset": 4,
}


def _official_name_evidence_by_listing_isin() -> dict[tuple[str, str, str], tuple[dict[str, str], ...]]:
    """Return active exact-listing official names keyed by listing, ISIN and asset type.

    The ISIN is part of the key deliberately: an official name may only repair the
    identity family for the exact identifier asserted on that listing. Ticker-only
    or issuer-only name propagation remains unsupported.
    """

    grouped: dict[tuple[str, str, str], list[dict[str, str]]] = defaultdict(list)
    dataset = _dataset_module()
    path = dataset.MASTERFILE_REFERENCE_CSV
    if not path.exists():
        return {}
    with path.open(newline="", encoding="utf-8") as handle:
        for reference in csv.DictReader(handle):
            if reference.get("official") != "true" or reference.get("listing_status") != "active":
                continue
            if reference.get("reference_scope") in {"", "manual"}:
                continue
            isin = reference.get("isin", "").strip().upper()
            name = reference.get("name", "").strip()
            ticker = reference.get("ticker", "").strip().upper()
            exchange = reference.get("exchange", "").strip()
            asset_type = reference.get("asset_type", "").strip()
            if exchange in dataset.EXCHANGE_ALIASES:
                exchange = dataset.EXCHANGE_ALIASES[exchange]
            if (
                not ticker
                or not exchange
                or asset_type not in {"Stock", "ETF"}
                or not is_full_identity_name(name)
                or not dataset.is_valid_isin(isin)
            ):
                continue
            grouped[(f"{exchange}::{ticker}", isin, asset_type)].append(
                {
                    "name": name,
                    "source_key": reference.get("source_key", ""),
                    "reference_scope": reference.get("reference_scope", ""),
                    "source_url": reference.get("source_url", ""),
                    "observation_id": hashlib.sha256(
                        f"{reference.get('source_key','')}|{exchange}::{ticker}|{isin}|{name}|{reference.get('source_url','')}".encode("utf-8")
                    ).hexdigest()[:24],
                }
            )
    return {
        key: tuple(
            sorted(
                evidence,
                key=lambda item: (
                    _REFERENCE_SCOPE_PRIORITY.get(item.get("reference_scope", ""), 99),
                    len(item.get("name", "")),
                    item.get("name", ""),
                    item.get("source_key", ""),
                ),
            )
        )
        for key, evidence in grouped.items()
    }


def _names_form_one_identity(names: Iterable[str], asset_type: str) -> bool:
    ordered = sorted({name.strip() for name in names if name.strip()})
    return bool(ordered) and all(
        names_refer_to_same_identity(left, right, asset_type)
        for index, left in enumerate(ordered)
        for right in ordered[index + 1 :]
    )


def reconcile_exact_official_names(
    rows: list[dict[str, Any]], *, apply_updates: bool = False
) -> list[dict[str, Any]]:
    """Repair stale canonical names only from coherent exact-listing official evidence.

    A name is changed when active official sources match the exact listing key,
    asset type and current valid ISIN, all returned official names form one
    coherent identity, and none matches the stored name. This resolves verified
    rename drift without weakening the identifier quarantine for genuine source
    conflicts.
    """

    evidence_by_key = _official_name_evidence_by_listing_isin()
    protected_listing_keys = _reviewed_name_listing_keys()
    peer_names: dict[tuple[str, str], list[str]] = defaultdict(list)
    for peer in rows:
        peer_isin = str(peer.get("isin", "")).strip().upper()
        peer_asset = str(peer.get("asset_type", "")).strip()
        peer_name = str(peer.get("name", "")).strip()
        if peer_isin and peer_name and is_full_identity_name(peer_name):
            peer_names[(peer_isin, peer_asset)].append(peer_name)
    reconciled: list[dict[str, Any]] = []
    for source_row in rows:
        row = dict(source_row)
        row_key = listing_key(row)
        isin = str(row.get("isin", "")).strip().upper()
        asset_type = str(row.get("asset_type", "")).strip()
        current_name = str(row.get("name", "")).strip()
        evidence = evidence_by_key.get((row_key, isin, asset_type), ())
        official_names = [
            item.get("name", "").strip()
            for item in evidence
            if item.get("name", "").strip()
        ]
        if (
            row_key in protected_listing_keys
            or not isin
            or not current_name
            or not evidence
            or not _names_form_one_identity(official_names, asset_type)
            or any(names_refer_to_same_identity(current_name, name, asset_type) for name in official_names)
        ):
            reconciled.append(row)
            continue

        preferred = evidence[0]
        new_name = preferred.get("name", "").strip()
        coherent_peers = [name for name in peer_names.get((isin, asset_type), []) if name != current_name]
        if coherent_peers and _names_form_one_identity([current_name, *coherent_peers], asset_type):
            if not all(names_refer_to_same_identity(new_name, peer, asset_type) for peer in coherent_peers):
                reconciled.append(row)
                continue
        if not new_name or new_name == current_name:
            reconciled.append(row)
            continue
        action = "applied" if apply_updates else "proposed"
        if apply_updates:
            row["name"] = new_name
        _NAME_RECONCILIATIONS.append(
            {
                "listing_key": row_key,
                "action": action,
                "ticker": str(row.get("ticker", "")),
                "exchange": str(row.get("exchange", "")),
                "asset_type": asset_type,
                "isin": isin,
                "old_name": current_name,
                "new_name": new_name,
                "source_key": preferred.get("source_key", ""),
                "reference_scope": preferred.get("reference_scope", ""),
                "source_url": preferred.get("source_url", ""),
                "observation_id": preferred.get("observation_id", ""),
                "evidence_status": "active_official_exact_listing_isin_name",
            }
        )
        reconciled.append(row)
    return reconciled


def write_official_name_reconciliation_report() -> dict[str, Any]:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    deduplicated = {
        (row["listing_key"], row["isin"], row["old_name"], row["new_name"]): row
        for row in _NAME_RECONCILIATIONS
    }
    rows = sorted(
        deduplicated.values(),
        key=lambda row: (row["listing_key"], row["isin"], row["new_name"]),
    )
    fieldnames = [
        "listing_key",
        "action",
        "ticker",
        "exchange",
        "asset_type",
        "isin",
        "old_name",
        "new_name",
        "source_key",
        "reference_scope",
        "source_url",
        "observation_id",
        "evidence_status",
    ]
    with OFFICIAL_NAME_RECONCILIATION_CSV.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    summary = {
        "generated_at": _built_at(),
        "policy": (
            "canonical names are changed only by active official evidence matching "
            "listing_key, asset_type and the current valid ISIN"
        ),
        "reconciled_rows": len(rows),
        "applied_rows": sum(row.get("action") == "applied" for row in rows),
        "proposed_rows": sum(row.get("action") == "proposed" for row in rows),
        "csv": str(OFFICIAL_NAME_RECONCILIATION_CSV.relative_to(ROOT)),
        "rows": rows,
    }
    OFFICIAL_NAME_RECONCILIATION_JSON.write_text(
        json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    lines = [
        "# Official name reconciliation",
        "",
        f"Generated: `{summary['generated_at']}`",
        "",
        (
            "Names are changed only when active official evidence matches the exact "
            "listing key, asset type and current valid ISIN, and all official names "
            "form one coherent identity."
        ),
        "",
        f"- Official name decisions: **{len(rows):,}**",
        f"- Applied: **{summary['applied_rows']:,}**",
        f"- Proposed only: **{summary['proposed_rows']:,}**",
        "",
        "The complete evidence ledger is `data/reports/official_name_reconciliation.csv`.",
        "",
    ]
    OFFICIAL_NAME_RECONCILIATION_MD.write_text("\n".join(lines), encoding="utf-8")
    return {key: value for key, value in summary.items() if key != "rows"}


def _reviewed_name_listing_keys() -> set[str]:
    """Listing keys with explicit reviewed name decisions; manual review wins."""

    path = _dataset_module().REVIEW_METADATA_UPDATES_CSV
    keys: set[str] = set()
    if not path.exists():
        return keys
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if row.get("field") != "name" or row.get("decision") not in {"update", "clear"}:
                continue
            ticker = row.get("ticker", "").strip().upper()
            exchange = row.get("exchange", "").strip()
            if ticker and exchange:
                keys.add(f"{exchange}::{ticker}")
    return keys


def _reviewed_keep_listing_keys() -> dict[str, set[str]]:
    path = DATA_DIR / "review_overrides" / "identifier_adjudications.csv"
    return keep_listing_keys_by_isin(path)


def strict_cleanse(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    if _ORIGINAL_CLEANSE is None:
        raise RuntimeError("strict_cleanse used outside canonical rebuild")
    preliminary = reconcile_exact_official_names(
        [dict(row) for row in rows],
        apply_updates=_APPLY_OFFICIAL_NAME_UPDATES,
    )
    cleaned, decisions = resolve_identity_conflicts(
        preliminary,
        official_isin_by_listing=_official_isin_by_listing(),
        reviewed_keep_listing_keys=_reviewed_keep_listing_keys(),
        apply_resolved_clears=_APPLY_IDENTITY_FIXES,
    )
    _DECISIONS.extend(decisions)
    return cleaned


def strict_cleaned_rows() -> tuple[list[dict[str, str]], dict[tuple[str, str], str]]:
    """Re-run identity adjudication after all output overrides and alias cleanup.

    The legacy pipeline applies reviewed output metadata after its mid-pipeline
    contamination hook.  A stale or contradictory ISIN override can therefore
    reintroduce a conflict after ``strict_cleanse``.  This final pass makes the
    postcondition apply to the exact rows that are exported.

    Intermediate identity decisions from ``strict_cleanse`` are discarded here so
    the quarantine ledger and export assertion describe the exported rows only.
    """

    if _ORIGINAL_CLEANED_ROWS is None:
        raise RuntimeError("strict_cleaned_rows used outside canonical rebuild")
    rows, alias_type_lookup = _ORIGINAL_CLEANED_ROWS()
    # The legacy pass invokes ``strict_cleanse`` before applying reviewed
    # transition drops, so it can record decisions for predecessor rows that no
    # longer exist in the exported snapshot. Discard those intermediate
    # decisions only after the legacy pass has completed.
    _DECISIONS.clear()
    rows = reconcile_exact_official_names(
        rows, apply_updates=_APPLY_OFFICIAL_NAME_UPDATES
    )
    cleaned, decisions = resolve_identity_conflicts(
        rows,
        official_isin_by_listing=_official_isin_by_listing(),
        reviewed_keep_listing_keys=_reviewed_keep_listing_keys(),
        apply_resolved_clears=_APPLY_IDENTITY_FIXES,
    )
    _DECISIONS.extend(decisions)
    return cleaned, alias_type_lookup


def write_quarantine_report() -> dict[str, Any]:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    deduplicated: dict[tuple[str, str, str], ResolutionDecision] = {}
    for decision in _DECISIONS:
        deduplicated[(decision.isin, decision.listing_key, decision.action)] = decision
    decisions = sorted(deduplicated.values(), key=lambda item: (item.isin, item.listing_key, item.action))
    rows = decision_rows(decisions)
    fieldnames = [
        "conflict_id",
        "isin",
        "listing_key",
        "ticker",
        "exchange",
        "name",
        "asset_type",
        "action",
        "reason",
        "evidence_status",
        "retained_isin",
    ]
    with QUARANTINE_CSV.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)

    action_counts = Counter(row["action"] for row in rows)
    conflict_count = len({row["conflict_id"] for row in rows})
    summary = {
        "generated_at": _built_at(),
        "policy": (
            "retain supported identity families; propose or quarantine every unresolved assertion; "
            "destructive clears occur only in explicit apply mode"
        ),
        "conflict_count": conflict_count,
        "decision_rows": len(rows),
        "action_counts": dict(sorted(action_counts.items())),
        "cleared_rows": sum(count for action, count in action_counts.items() if action.startswith("cleared")),
        "proposed_clear_rows": action_counts.get("proposed_clear_conflicting_identifier", 0),
        "quarantined_rows": action_counts.get("quarantined_unresolved_identifier", 0),
        "retained_rows": action_counts.get("kept_listing_keyed_identifier", 0),
        "csv": str(QUARANTINE_CSV.relative_to(ROOT)),
        "decisions": rows,
    }
    QUARANTINE_JSON.write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    lines = [
        "# Identifier quarantine",
        "",
        f"Generated: `{summary['generated_at']}`",
        "",
        "The canonical rebuild never guesses or transfers an ISIN by ticker alone. Unsupported assertions are quarantined without mutation. A conflicting assertion is cleared only in explicit apply mode when a different full-name family has decisive listing-keyed official or reviewed evidence.",
        "",
        f"- Conflict groups adjudicated: **{conflict_count:,}**",
        f"- Listing decisions: **{len(rows):,}**",
        f"- Identifier assertions cleared in explicit apply mode: **{summary['cleared_rows']:,}**",
        f"- Proposed clears: **{summary['proposed_clear_rows']:,}**",
        f"- Quarantined unresolved assertions: **{summary['quarantined_rows']:,}**",
        f"- Identifier assertions retained with decisive evidence: **{summary['retained_rows']:,}**",
        "",
        "## Actions",
        "",
        "| Action | Rows |",
        "|---|---:|",
    ]
    lines.extend(f"| `{action}` | {count:,} |" for action, count in sorted(action_counts.items()))
    lines.extend(
        [
            "",
            "The complete row-level ledger is `data/reports/identifier_quarantine.csv`.",
            "",
        ]
    )
    QUARANTINE_MD.write_text("\n".join(lines), encoding="utf-8")
    return summary


def assert_identity_decisions_match_export() -> int:
    with (DATA_DIR / "listings.csv").open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    current = {listing_key(row): row for row in rows}
    errors: list[str] = []
    for decision in _DECISIONS:
        row = current.get(decision.listing_key)
        if row is None:
            errors.append(f"decision listing is absent: {decision.listing_key}")
            continue
        actual = str(row.get("isin", "") or "").strip().upper()
        expected = str(decision.retained_isin or "").strip().upper()
        if actual != expected:
            errors.append(
                f"{decision.listing_key}: decision {decision.action} expects {expected!r}, got {actual!r}"
            )
    if errors:
        raise SystemExit("identity decision/export mismatch: " + "; ".join(errors[:10]))
    return len(find_identity_conflicts(rows))


def rebuild_validation_dependents() -> None:
    """Regenerate every validator input derived from the rebuilt current dataset."""

    build_entry_quality_report.main([])
    build_source_gap_classification.main([])
    build_source_of_truth_decisions.main([])
    if build_adanos_ticker_reference.main([]) != 0:
        raise SystemExit("Adanos ticker reference rebuild failed")
    if update_readme_snapshot.main([]) != 0:
        raise SystemExit("README snapshot rebuild failed")


def _load_listings_csv() -> list[dict[str, str]]:
    path = DATA_DIR / "listings.csv"
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def rebuild(
    *, apply_identity_fixes: bool = False, apply_official_name_updates: bool = False
) -> dict[str, Any]:
    global _ORIGINAL_CLEANSE, _ORIGINAL_CLEANED_ROWS
    global _APPLY_IDENTITY_FIXES, _APPLY_OFFICIAL_NAME_UPDATES
    _APPLY_IDENTITY_FIXES = apply_identity_fixes
    _APPLY_OFFICIAL_NAME_UPDATES = apply_official_name_updates
    _DECISIONS.clear()
    _NAME_RECONCILIATIONS.clear()
    previous_listings = _load_listings_csv()
    normalize_source_registry.build()
    dataset = _dataset_module()
    _ORIGINAL_CLEANSE = dataset.cleanse_conflicting_isin_rows
    _ORIGINAL_CLEANED_ROWS = dataset.cleaned_rows
    dataset.cleanse_conflicting_isin_rows = strict_cleanse
    dataset.cleaned_rows = strict_cleaned_rows
    try:
        dataset.rebuild()
    finally:
        dataset.cleanse_conflicting_isin_rows = _ORIGINAL_CLEANSE
        dataset.cleaned_rows = _ORIGINAL_CLEANED_ROWS

    official_name_summary = write_official_name_reconciliation_report()
    # Synchronise listing-keyed extended identifiers without network enrichment.
    identifier_summary = enrich_global_identifiers.main(
        enable_cik=False,
        enable_figi=False,
        enable_lei=False,
    )
    # The enrichment helper normally timestamps the invocation. Pin the summary
    # to the canonical dataset build time so identical inputs remain byte-stable.
    identifier_summary["generated_at"] = _built_at()
    (DATA_DIR / "identifier_summary.json").write_text(
        json.dumps(identifier_summary, indent=2) + "\n", encoding="utf-8"
    )
    unresolved_identity_conflicts = assert_identity_decisions_match_export()
    quarantine_summary = write_quarantine_report()
    quarantine_summary["remaining_conflict_groups"] = unresolved_identity_conflicts
    history_summary = build_listing_history.build_history(previous_listings=previous_listings)
    # Coverage contracts depend on reports derived from this exact rebuilt
    # listing snapshot. Rebuild those inputs here instead of trusting stale
    # committed reports from an earlier dataset.
    build_coverage_report.build_report()
    if build_exchange_source_audit.main([]) != 0:
        raise SystemExit("exchange source audit failed")
    rebuild_validation_dependents()
    evidence_as_of = _built_at_datetime()
    reference_summary = build_reference_reconciliation.build(as_of=evidence_as_of)
    coverage_summary = build_coverage_contracts.build(as_of=evidence_as_of)
    canonical_v4_manifest = build_canonical_v4.build()
    summary = {
        "official_name_reconciliation": official_name_summary,
        "identifier_quarantine": {
            key: value for key, value in quarantine_summary.items() if key != "decisions"
        },
        "identifier_summary": identifier_summary,
        "listing_history": history_summary,
        "reference_reconciliation": reference_summary.get("summary", {}),
        "coverage_contracts": coverage_summary.get("summary", {}),
        "canonical_v4": canonical_v4_manifest.get("counts", {}),
    }
    print(json.dumps(summary, indent=2))
    return summary


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply-identity-fixes", action="store_true",
        help="Apply only decisively supported cross-family ISIN clears. Unresolved rows remain quarantined.",
    )
    parser.add_argument(
        "--apply-official-name-updates", action="store_true",
        help="Apply exact listing+ISIN official name updates instead of reporting proposals only.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    rebuild(
        apply_identity_fixes=args.apply_identity_fixes,
        apply_official_name_updates=args.apply_official_name_updates,
    )


if __name__ == "__main__":
    main()

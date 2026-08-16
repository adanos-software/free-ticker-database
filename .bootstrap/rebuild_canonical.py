"""Atomic canonical rebuild entrypoint.

This is the only supported operational rebuild command. It wraps the legacy
compatibility exporter with strict listing-keyed identifier adjudication, then
synchronises extended identifiers, listing history, and the canonical-v4 bridge.
"""

from __future__ import annotations

import csv
import hashlib
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

try:
    from scripts import build_canonical_v4, build_listing_history, enrich_global_identifiers, rebuild_dataset
    from scripts.lib.identity_integrity import (
        ResolutionDecision,
        decision_rows,
        find_identity_conflicts,
        listing_key,
        resolve_identity_conflicts,
    )
except ModuleNotFoundError:  # pragma: no cover - direct script execution
    import build_canonical_v4
    import build_listing_history
    import enrich_global_identifiers
    import rebuild_dataset
    from lib.identity_integrity import (
        ResolutionDecision,
        decision_rows,
        find_identity_conflicts,
        listing_key,
        resolve_identity_conflicts,
    )

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
REPORTS_DIR = DATA_DIR / "reports"
QUARANTINE_CSV = REPORTS_DIR / "identifier_quarantine.csv"
QUARANTINE_JSON = REPORTS_DIR / "identifier_quarantine.json"
QUARANTINE_MD = REPORTS_DIR / "identifier_quarantine.md"

_DECISIONS: list[ResolutionDecision] = []
_ORIGINAL_CLEANSE = rebuild_dataset.cleanse_conflicting_isin_rows
_ORIGINAL_CLEANED_ROWS = rebuild_dataset.cleaned_rows


def _built_at() -> str:
    try:
        payload = json.loads((DATA_DIR / "tickers.json").read_text(encoding="utf-8"))
        return str(payload.get("_meta", {}).get("built_at", ""))
    except (OSError, json.JSONDecodeError):
        return ""


def _official_isin_by_listing() -> dict[str, set[str]]:
    result: dict[str, set[str]] = defaultdict(set)
    for (ticker, exchange, _asset_type), isin in rebuild_dataset.load_active_official_isin_fallbacks().items():
        if isin:
            result[f"{exchange}::{ticker}"].add(isin.strip().upper())
    return dict(result)


def _reviewed_keep_listing_keys() -> dict[str, set[str]]:
    path = rebuild_dataset.REVIEW_METADATA_UPDATES_CSV
    result: dict[str, set[str]] = defaultdict(set)
    if not path.exists():
        return {}
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if row.get("field") != "isin" or row.get("decision") != "update":
                continue
            isin = row.get("proposed_value", "").strip().upper()
            reason = row.get("reason", "").strip()
            ticker = row.get("ticker", "").strip().upper()
            exchange = row.get("exchange", "").strip()
            if isin and reason and ticker and exchange:
                result[isin].add(f"{exchange}::{ticker}")
    return dict(result)


def _legacy_decision(before: dict[str, str], after: dict[str, str]) -> ResolutionDecision:
    isin = before.get("isin", "").strip().upper()
    key = listing_key(before)
    conflict_id = hashlib.sha256(f"legacy-cleanse|{isin}|{key}".encode("utf-8")).hexdigest()[:20]
    return ResolutionDecision(
        conflict_id=conflict_id,
        isin=isin,
        listing_key=key,
        ticker=before.get("ticker", ""),
        exchange=before.get("exchange", ""),
        name=before.get("name", ""),
        asset_type=before.get("asset_type", ""),
        action="cleared_legacy_contamination",
        reason="The existing alias/peer contamination guard cleared this listing-keyed identifier before strict family adjudication.",
        evidence_status="legacy_contamination_guard",
        retained_isin=after.get("isin", ""),
    )


def strict_cleanse(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    before_by_key = {listing_key(row): dict(row) for row in rows}
    preliminary = _ORIGINAL_CLEANSE(rows)
    for row in preliminary:
        before = before_by_key.get(listing_key(row), {})
        if before.get("isin") and not row.get("isin"):
            _DECISIONS.append(_legacy_decision(before, row))

    cleaned, decisions = resolve_identity_conflicts(
        preliminary,
        official_isin_by_listing=_official_isin_by_listing(),
        reviewed_keep_listing_keys=_reviewed_keep_listing_keys(),
    )
    _DECISIONS.extend(decisions)
    return cleaned


def strict_cleaned_rows() -> tuple[list[dict[str, str]], dict[tuple[str, str], str]]:
    """Re-run identity adjudication after all output overrides and alias cleanup.

    The legacy pipeline applies reviewed output metadata after its mid-pipeline
    contamination hook.  A stale or contradictory ISIN override can therefore
    reintroduce a conflict after ``strict_cleanse``.  This final pass makes the
    postcondition apply to the exact rows that are exported.
    """

    rows, alias_type_lookup = _ORIGINAL_CLEANED_ROWS()
    cleaned, decisions = resolve_identity_conflicts(
        rows,
        official_isin_by_listing=_official_isin_by_listing(),
        reviewed_keep_listing_keys=_reviewed_keep_listing_keys(),
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
        "policy": "retain only a single coherent identity family with decisive listing-keyed evidence; otherwise clear the identifier assertion",
        "conflict_count": conflict_count,
        "decision_rows": len(rows),
        "action_counts": dict(sorted(action_counts.items())),
        "cleared_rows": sum(count for action, count in action_counts.items() if action.startswith("cleared")),
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
        "The canonical rebuild never guesses or transfers an ISIN by ticker alone. A conflicting assertion is retained only for one coherent identity family with exact listing-keyed official evidence or an explicit reviewed override.",
        "",
        f"- Conflict groups adjudicated: **{conflict_count:,}**",
        f"- Listing decisions: **{len(rows):,}**",
        f"- Identifier assertions cleared: **{summary['cleared_rows']:,}**",
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


def assert_final_identity_integrity() -> None:
    with (DATA_DIR / "listings.csv").open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    conflicts = find_identity_conflicts(rows)
    if conflicts:
        sample = ", ".join(conflict.isin for conflict in conflicts[:10])
        raise SystemExit(f"canonical rebuild left unresolved ISIN identity conflicts: {sample}")


def rebuild() -> dict[str, Any]:
    _DECISIONS.clear()
    rebuild_dataset.cleanse_conflicting_isin_rows = strict_cleanse
    rebuild_dataset.cleaned_rows = strict_cleaned_rows
    try:
        rebuild_dataset.rebuild()
    finally:
        rebuild_dataset.cleanse_conflicting_isin_rows = _ORIGINAL_CLEANSE
        rebuild_dataset.cleaned_rows = _ORIGINAL_CLEANED_ROWS

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
    history_summary = build_listing_history.build_history()
    assert_final_identity_integrity()
    quarantine_summary = write_quarantine_report()
    canonical_v4_manifest = build_canonical_v4.build()
    summary = {
        "identifier_quarantine": {
            key: value for key, value in quarantine_summary.items() if key != "decisions"
        },
        "identifier_summary": identifier_summary,
        "listing_history": history_summary,
        "canonical_v4": canonical_v4_manifest.get("counts", {}),
    }
    print(json.dumps(summary, indent=2))
    return summary


def main() -> None:
    rebuild()


if __name__ == "__main__":
    main()

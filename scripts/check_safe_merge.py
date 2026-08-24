"""Block every destructive canonical change lacking exact current-row evidence."""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from pathlib import Path
from typing import Any

try:
    from scripts.lib.merge_evidence import (
        CRITICAL_FIELDS, FIELD_EVENT_TYPES, REMOVAL_EVENT_TYPES,
        event_has_provenance, event_timestamp_is_valid, listing_key, row_fingerprint,
    )
    from scripts.lib.official_change_evidence import build_official_change_evidence
except ModuleNotFoundError:  # pragma: no cover
    from lib.merge_evidence import (
        CRITICAL_FIELDS, FIELD_EVENT_TYPES, REMOVAL_EVENT_TYPES,
        event_has_provenance, event_timestamp_is_valid, listing_key, row_fingerprint,
    )
    from lib.official_change_evidence import build_official_change_evidence

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
REPORTS_DIR = DATA_DIR / "reports"
DEFAULT_OFFICIAL_REFERENCE = DATA_DIR / "masterfiles/reference.csv"
DEFAULT_TICKERS_JSON = DATA_DIR / "tickers.json"


def load_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))



def _display_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ROOT.resolve()))
    except ValueError:
        return str(path)


def _dataset_built_at(path: Path = DEFAULT_TICKERS_JSON) -> str:
    if not path.exists():
        return ""
    try:
        return str(json.loads(path.read_text(encoding="utf-8")).get("_meta", {}).get("built_at", ""))
    except (OSError, json.JSONDecodeError):
        return ""


def _duplicates(rows: list[dict[str, str]]) -> list[str]:
    counts = Counter(listing_key(row) for row in rows)
    return sorted(key for key, count in counts.items() if key and count > 1)


def _valid_removal(event: dict[str, str], before: dict[str, str]) -> bool:
    return (
        event.get("event_type", "") in REMOVAL_EVENT_TYPES
        and event_timestamp_is_valid(event)
        and event_has_provenance(event)
        and event.get("before_row_sha256", "").strip() == row_fingerprint(before)
    )


def _valid_change(
    event: dict[str, str], *, before: dict[str, str], field: str, old: str, new: str
) -> bool:
    return (
        event.get("event_type", "") in FIELD_EVENT_TYPES.get(field, set())
        and event.get("field_name", "") == field
        and event.get("old_value", "") == old
        and event.get("new_value", "") == new
        and event.get("before_row_sha256", "").strip() == row_fingerprint(before)
        and event_timestamp_is_valid(event)
        and event_has_provenance(event)
    )


def evaluate(
    before_rows: list[dict[str, str]],
    after_rows: list[dict[str, str]],
    event_rows: list[dict[str, str]],
    *,
    total_shrink_limit: float = 0.0025,
    venue_shrink_limit: float = 0.02,
    allow_large_evidenced_removal: bool = False,
    reference_rows: list[dict[str, str]] | None = None,
    observed_at: str = "",
    reference_source_report: str = "",
    previous_reference_rows: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    failures: list[str] = []
    before_duplicates = _duplicates(before_rows)
    after_duplicates = _duplicates(after_rows)
    if before_duplicates:
        failures.append(f"baseline contains {len(before_duplicates)} duplicate listing keys")
    if after_duplicates:
        failures.append(f"candidate contains {len(after_duplicates)} duplicate listing keys")

    before = {listing_key(row): row for row in before_rows if listing_key(row)}
    after = {listing_key(row): row for row in after_rows if listing_key(row)}
    generated_reference_evidence = build_official_change_evidence(
        before_rows, after_rows, reference_rows or [],
        observed_at=observed_at, source_report=reference_source_report,
        previous_reference_rows=previous_reference_rows,
    )
    events: dict[str, list[dict[str, str]]] = {}
    for event in [*event_rows, *generated_reference_evidence]:
        events.setdefault(listing_key(event), []).append(event)

    removed = sorted(set(before) - set(after))
    evidenced: list[str] = []
    unevidenced: list[str] = []
    for key in removed:
        if any(_valid_removal(event, before[key]) for event in events.get(key, [])):
            evidenced.append(key)
        else:
            unevidenced.append(key)
    if unevidenced:
        failures.append(f"{len(unevidenced)} listing removals lack exact, current-row evidence")

    changes: list[dict[str, str]] = []
    unevidenced_changes: list[dict[str, str]] = []
    for key in sorted(set(before) & set(after)):
        for field in CRITICAL_FIELDS:
            old = str(before[key].get(field, "") or "")
            new = str(after[key].get(field, "") or "")
            if old == new:
                continue
            change = {"listing_key": key, "field_name": field, "old_value": old, "new_value": new}
            changes.append(change)
            if not any(
                _valid_change(event, before=before[key], field=field, old=old, new=new)
                for event in events.get(key, [])
            ):
                unevidenced_changes.append(change)
    if unevidenced_changes:
        failures.append(f"{len(unevidenced_changes)} critical field changes lack exact evidence")

    total_shrink = max(0, len(before_rows) - len(after_rows)) / max(1, len(before_rows))
    if removed and total_shrink > total_shrink_limit and not allow_large_evidenced_removal:
        failures.append(
            f"overall row shrink {total_shrink:.3%} exceeds review threshold {total_shrink_limit:.3%}"
        )
    before_by_exchange = Counter(row.get("exchange", "") for row in before_rows)
    after_by_exchange = Counter(row.get("exchange", "") for row in after_rows)
    venue_findings: list[dict[str, Any]] = []
    for exchange, count in sorted(before_by_exchange.items()):
        after_count = after_by_exchange.get(exchange, 0)
        shrink = max(0, count - after_count) / max(1, count)
        removed_here = [key for key in removed if before[key].get("exchange", "") == exchange]
        exceeded = bool(removed_here) and shrink > venue_shrink_limit
        if exceeded and not allow_large_evidenced_removal:
            failures.append(
                f"{exchange} row shrink {shrink:.3%} exceeds review threshold {venue_shrink_limit:.3%}"
            )
        venue_findings.append(
            {
                "exchange": exchange,
                "before_rows": count,
                "after_rows": after_count,
                "shrink_pct": round(shrink * 100, 6),
                "removed_rows": len(removed_here),
                "unevidenced_removed_rows": sum(key in unevidenced for key in removed_here),
                "status": "fail" if exceeded and not allow_large_evidenced_removal else "pass",
            }
        )
    failures = list(dict.fromkeys(failures))
    return {
        "status": "pass" if not failures else "fail",
        "thresholds": {
            "total_shrink_pct": total_shrink_limit * 100,
            "venue_shrink_pct": venue_shrink_limit * 100,
            "large_evidenced_removal_override": allow_large_evidenced_removal,
        },
        "summary": {
            "before_rows": len(before_rows), "after_rows": len(after_rows),
            "overall_shrink_pct": round(total_shrink * 100, 6),
            "removed_rows": len(removed), "evidenced_removed_rows": len(evidenced),
            "unevidenced_removed_rows": len(unevidenced),
            "critical_field_changes": len(changes),
            "unevidenced_critical_field_changes": len(unevidenced_changes),
            "before_duplicate_listing_keys": len(before_duplicates),
            "after_duplicate_listing_keys": len(after_duplicates),
            "generated_official_change_evidence_rows": len(generated_reference_evidence),
        },
        "failures": failures,
        "unevidenced_removed_listing_keys": unevidenced,
        "evidenced_removed_listing_keys": evidenced,
        "unevidenced_critical_field_changes": unevidenced_changes,
        "critical_field_changes": changes,
        "generated_official_change_evidence": generated_reference_evidence,
        "venue_findings": venue_findings,
    }


def write_report(report: dict[str, Any]) -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    (REPORTS_DIR / "safe_merge.json").write_text(
        json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    lines = ["# Safe merge gate", "", f"Status: **{report['status'].upper()}**", ""]
    lines.extend(f"- {failure}" for failure in report["failures"])
    (REPORTS_DIR / "safe_merge.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--before", type=Path, required=True)
    parser.add_argument("--after", type=Path, default=DATA_DIR / "listings.csv")
    parser.add_argument("--events", type=Path, default=DATA_DIR / "history/listing_events.csv")
    parser.add_argument("--allow-large-evidenced-removal", action="store_true")
    parser.add_argument("--official-reference", type=Path, default=DEFAULT_OFFICIAL_REFERENCE)
    parser.add_argument("--previous-official-reference", type=Path)
    parser.add_argument("--observed-at", default="")
    parser.add_argument("--strict", action="store_true")
    args = parser.parse_args()
    report = evaluate(
        load_csv(args.before), load_csv(args.after),
        load_csv(args.events) if args.events.exists() else [],
        allow_large_evidenced_removal=args.allow_large_evidenced_removal,
        reference_rows=load_csv(args.official_reference) if args.official_reference.exists() else [],
        observed_at=args.observed_at or _dataset_built_at(),
        reference_source_report=_display_path(args.official_reference),
        previous_reference_rows=(
            load_csv(args.previous_official_reference)
            if args.previous_official_reference and args.previous_official_reference.exists()
            else []
        ),
    )
    write_report(report)
    print(json.dumps(report["summary"], indent=2))
    if args.strict and report["status"] != "pass":
        raise SystemExit("safe merge gate failed")


if __name__ == "__main__":
    main()

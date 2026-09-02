"""Block every destructive canonical change lacking exact current-row evidence."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from collections import Counter
from pathlib import Path
from typing import Any

try:
    from scripts.lib.merge_evidence import (
        CRITICAL_FIELDS, FIELD_EVENT_TYPES, REMOVAL_EVENT_TYPES,
        event_has_provenance, event_timestamp_is_valid, listing_key, row_fingerprint,
    )
    from scripts.lib.official_change_evidence import build_official_change_evidence
    from scripts.lib.official_change_evidence import is_valid_isin
except ModuleNotFoundError:  # pragma: no cover
    from lib.merge_evidence import (
        CRITICAL_FIELDS, FIELD_EVENT_TYPES, REMOVAL_EVENT_TYPES,
        event_has_provenance, event_timestamp_is_valid, listing_key, row_fingerprint,
    )
    from lib.official_change_evidence import build_official_change_evidence
    from lib.official_change_evidence import is_valid_isin

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
REPORTS_DIR = DATA_DIR / "reports"
DEFAULT_OFFICIAL_REFERENCE = DATA_DIR / "masterfiles/reference.csv"
DEFAULT_TICKERS_JSON = DATA_DIR / "tickers.json"
DEFAULT_LISTING_TRANSITIONS = DATA_DIR / "review_overrides/listing_transitions.csv"
DEFAULT_SEC_EXCHANGE_CACHE = DATA_DIR / "masterfiles/cache/sec_company_tickers_exchange.json"


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


def _split_listing_key(value: str) -> tuple[str, str]:
    exchange, separator, ticker = value.partition("::")
    return (exchange, ticker) if separator and exchange and ticker else ("", "")


def _sec_ciks_by_ticker(payload: dict[str, Any] | None) -> dict[str, set[str]]:
    if not payload:
        return {}
    fields = payload.get("fields", [])
    data = payload.get("data", [])
    if not isinstance(fields, list) or not isinstance(data, list):
        return {}
    indexed: dict[str, set[str]] = {}
    for values in data:
        if not isinstance(values, list) or len(values) != len(fields):
            continue
        row = dict(zip(fields, values))
        ticker = str(row.get("ticker", "") or "").strip().upper()
        cik = str(row.get("cik", "") or "").strip()
        if ticker and cik.isdigit():
            indexed.setdefault(ticker, set()).add(cik.zfill(10))
    return indexed


def build_reviewed_transition_evidence(
    before_rows: list[dict[str, str]],
    after_rows: list[dict[str, str]],
    transition_rows: list[dict[str, str]],
    *,
    sec_exchange_payload: dict[str, Any] | None,
    observed_at: str,
    source_report: str,
) -> list[dict[str, str]]:
    if not observed_at or not source_report:
        return []
    before = {listing_key(row): row for row in before_rows if listing_key(row)}
    after = {listing_key(row): row for row in after_rows if listing_key(row)}
    sec_ciks = _sec_ciks_by_ticker(sec_exchange_payload)
    evidence: list[dict[str, str]] = []
    for transition in transition_rows:
        old_key = transition.get("old_listing_key", "").strip()
        new_key = transition.get("new_listing_key", "").strip()
        old_row = before.get(old_key)
        event_type = transition.get("event_type", "").strip()
        if not old_row or old_key in after:
            continue
        old_exchange, old_ticker = _split_listing_key(old_key)
        new_row = after.get(new_key)
        before_new_row = before.get(new_key)
        new_exchange, new_ticker = _split_listing_key(new_key)
        if event_type == "delisted":
            shape_is_valid = not new_key
            field_name, old_value, new_value = "", old_row.get("name", ""), ""
        elif not new_row:
            continue
        elif event_type == "venue_changed":
            shape_is_valid = old_ticker == new_ticker and old_exchange != new_exchange
            field_name, old_value, new_value = "exchange", old_exchange, new_exchange
        elif event_type == "symbol_changed":
            shape_is_valid = old_exchange == new_exchange and old_ticker != new_ticker
            field_name, old_value, new_value = "ticker", old_ticker, new_ticker
        elif event_type == "listing_changed":
            shape_is_valid = old_exchange != new_exchange and old_ticker != new_ticker
            field_name, old_value, new_value = "listing_key", old_key, new_key
        else:
            continue
        if not shape_is_valid or (
            new_row and old_row.get("asset_type") != new_row.get("asset_type")
        ):
            continue
        identity_type = transition.get("identity_type", "").strip()
        identity_value = transition.get("identity_value", "").strip().upper()
        if event_type == "delisted" and identity_type == "exact_isin":
            identity_is_valid = bool(
                is_valid_isin(identity_value)
                and old_row.get("isin", "").strip().upper() == identity_value
            )
        elif identity_type == "same_isin" and new_row:
            identity_is_valid = bool(
                is_valid_isin(identity_value)
                and old_row.get("isin", "").strip().upper() == identity_value
                and new_row.get("isin", "").strip().upper() == identity_value
            )
        elif identity_type == "same_cik":
            identity_is_valid = bool(
                identity_value.isdigit()
                and identity_value.zfill(10) in sec_ciks.get(old_ticker, set())
                and identity_value.zfill(10) in sec_ciks.get(new_ticker, set())
            )
        else:
            continue
        source_key = transition.get("source_key", "").strip()
        source_url = transition.get("source_url", "").strip()
        try:
            confidence = float(transition.get("confidence", "0"))
        except ValueError:
            confidence = 0.0
        if (
            not identity_is_valid
            or not math.isfinite(confidence)
            or not 0.95 <= confidence <= 1.0
            or not source_key
            or not source_url.startswith("https://")
        ):
            continue
        observation_payload = "|".join((old_key, new_key, identity_type, identity_value, source_url))
        evidence.append(
            {
                "listing_key": old_key,
                "ticker": old_ticker,
                "exchange": old_exchange,
                "event_type": event_type,
                "field_name": field_name,
                "old_value": old_value,
                "new_value": new_value,
                "before_row_sha256": row_fingerprint(old_row),
                "effective_at": "",
                "observed_at": observed_at,
                "source_key": source_key,
                "source_url": source_url,
                "source_report": source_report,
                "observation_id": hashlib.sha256(observation_payload.encode("utf-8")).hexdigest()[:24],
                "evidence_status": "reviewed",
            }
        )
        if not before_new_row or not new_row or identity_type != "same_isin":
            continue
        inherited_event_types = {
            "isin": "identifier_changed",
            "country": "country_changed",
            "country_code": "country_changed",
            "stock_sector": "taxonomy_changed",
            "etf_category": "taxonomy_changed",
        }
        for inherited_field, inherited_event_type in inherited_event_types.items():
            prior_value = str(before_new_row.get(inherited_field, "") or "")
            inherited_value = str(new_row.get(inherited_field, "") or "")
            predecessor_value = str(old_row.get(inherited_field, "") or "")
            if (
                prior_value == inherited_value
                or not inherited_value
                or inherited_value != predecessor_value
            ):
                continue
            field_observation_payload = "|".join(
                (
                    observation_payload,
                    inherited_field,
                    prior_value,
                    inherited_value,
                )
            )
            evidence.append(
                {
                    "listing_key": new_key,
                    "ticker": new_ticker,
                    "exchange": new_exchange,
                    "event_type": inherited_event_type,
                    "field_name": inherited_field,
                    "old_value": prior_value,
                    "new_value": inherited_value,
                    "before_row_sha256": row_fingerprint(before_new_row),
                    "effective_at": "",
                    "observed_at": observed_at,
                    "source_key": source_key,
                    "source_url": source_url,
                    "source_report": source_report,
                    "observation_id": hashlib.sha256(
                        field_observation_payload.encode("utf-8")
                    ).hexdigest()[:24],
                    "evidence_status": "reviewed",
                }
            )
    return evidence


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
    reviewed_transition_rows: list[dict[str, str]] | None = None,
    sec_exchange_payload: dict[str, Any] | None = None,
    reviewed_transition_source_report: str = "",
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
    generated_reviewed_transition_evidence = build_reviewed_transition_evidence(
        before_rows,
        after_rows,
        reviewed_transition_rows or [],
        sec_exchange_payload=sec_exchange_payload,
        observed_at=observed_at,
        source_report=reviewed_transition_source_report,
    )
    events: dict[str, list[dict[str, str]]] = {}
    for event in [*event_rows, *generated_reference_evidence, *generated_reviewed_transition_evidence]:
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
            "generated_reviewed_transition_evidence_rows": len(generated_reviewed_transition_evidence),
        },
        "failures": failures,
        "unevidenced_removed_listing_keys": unevidenced,
        "evidenced_removed_listing_keys": evidenced,
        "unevidenced_critical_field_changes": unevidenced_changes,
        "critical_field_changes": changes,
        "generated_official_change_evidence": generated_reference_evidence,
        "generated_reviewed_transition_evidence": generated_reviewed_transition_evidence,
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
    parser.add_argument("--reviewed-listing-transitions", type=Path, default=DEFAULT_LISTING_TRANSITIONS)
    parser.add_argument("--sec-exchange-cache", type=Path, default=DEFAULT_SEC_EXCHANGE_CACHE)
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
        reviewed_transition_rows=(
            load_csv(args.reviewed_listing_transitions)
            if args.reviewed_listing_transitions.exists()
            else []
        ),
        sec_exchange_payload=(
            json.loads(args.sec_exchange_cache.read_text(encoding="utf-8"))
            if args.sec_exchange_cache.exists()
            else None
        ),
        reviewed_transition_source_report=_display_path(args.reviewed_listing_transitions),
    )
    write_report(report)
    print(json.dumps(report["summary"], indent=2))
    if args.strict and report["status"] != "pass":
        raise SystemExit("safe merge gate failed")


if __name__ == "__main__":
    main()

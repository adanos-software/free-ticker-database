"""Build current snapshot, evidence-bound listing events and point-in-time status."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Iterable

try:
    from scripts.lib.delisting_evidence import valid_official_delisting_evidence
    from scripts.listing_keys import row_listing_key
    from scripts.lib.merge_evidence import row_fingerprint
except ModuleNotFoundError:  # pragma: no cover
    from lib.delisting_evidence import valid_official_delisting_evidence
    from listing_keys import row_listing_key
    from lib.merge_evidence import row_fingerprint

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
HISTORY_DIR = DATA_DIR / "history"
LATEST_SNAPSHOT_CSV = HISTORY_DIR / "latest_snapshot.csv"
LISTING_EVENTS_CSV = HISTORY_DIR / "listing_events.csv"
LISTING_STATUS_HISTORY_CSV = HISTORY_DIR / "listing_status_history.csv"
DAILY_LISTING_SUMMARY_JSON = HISTORY_DIR / "daily_listing_summary.json"
DAILY_LISTING_SUMMARY_CSV = HISTORY_DIR / "daily_listing_summary.csv"
LISTINGS_CSV = DATA_DIR / "listings.csv"
TICKERS_JSON = DATA_DIR / "tickers.json"
DELISTING_APPLY_JSON = DATA_DIR / "reports/delisting_apply.json"
IDENTIFIER_QUARANTINE_CSV = DATA_DIR / "reports/identifier_quarantine.csv"
OFFICIAL_NAME_RECONCILIATION_CSV = DATA_DIR / "reports/official_name_reconciliation.csv"
METADATA_UPDATES_CSV = DATA_DIR / "review_overrides/metadata_updates.csv"
TRUSTED_EVIDENCE = {"official", "reviewed", "verified"}
VALID_LISTING_STATUSES = {"active", "suspended", "delisted"}
STATUS_HISTORY_FIELDS = [
    "listing_key", "ticker", "exchange", "status", "first_observed_at",
    "last_observed_at", "effective_at", "status_source", "source_report",
    "evidence_status",
]
EVENT_FIELDS = [
    "listing_key", "ticker", "exchange", "event_type", "field_name", "old_value",
    "new_value", "before_row_sha256", "effective_at", "observed_at", "source_key",
    "source_url", "source_report", "observation_id", "evidence_status",
]
CRITICAL_FIELD_EVENT_TYPES = {
    "name": "renamed", "asset_type": "reclassified", "country": "country_changed",
    "country_code": "country_changed", "isin": "identifier_changed",
    "stock_sector": "taxonomy_changed", "etf_category": "taxonomy_changed",
}


def write_csv(path: Path, fieldnames: list[str], rows: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n", extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def load_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def listing_identity(row: dict[str, str]) -> str:
    return row.get("listing_key") or row_listing_key(row)


def load_current_rows() -> tuple[list[dict[str, str]], str]:
    built_at = json.loads(TICKERS_JSON.read_text(encoding="utf-8"))["_meta"]["built_at"]
    return load_csv(LISTINGS_CSV), built_at


def sector_model_fields(row: dict[str, str]) -> tuple[str, str]:
    legacy = row.get("sector", "")
    if row.get("asset_type") == "Stock":
        return row.get("stock_sector", "") or legacy, ""
    if row.get("asset_type") == "ETF":
        return "", row.get("etf_category", "") or legacy
    return row.get("stock_sector", ""), row.get("etf_category", "")


def build_snapshot(rows: list[dict[str, str]], observed_at: str) -> list[dict[str, str]]:
    snapshot = []
    for row in rows:
        stock_sector, etf_category = sector_model_fields(row)
        snapshot.append({
            "listing_key": listing_identity(row), "ticker": row["ticker"],
            "exchange": row["exchange"], "name": row["name"],
            "asset_type": row["asset_type"], "country": row["country"],
            "country_code": row["country_code"], "isin": row["isin"],
            "stock_sector": stock_sector, "etf_category": etf_category,
            "status": "active", "observed_at": observed_at,
        })
    return sorted(snapshot, key=lambda row: row["listing_key"])


def compact_legacy_status_history(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    grouped: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        grouped.setdefault(listing_identity(row), []).append(row)
    compacted: list[dict[str, str]] = []
    for members in grouped.values():
        current: dict[str, str] | None = None
        for row in sorted(members, key=lambda item: (item.get("observed_at", ""), item.get("status", ""))):
            status = row.get("status", "")
            evidence = row.get("evidence_status", "observed_unverified")
            if status == "delisted" and evidence not in TRUSTED_EVIDENCE:
                continue
            observed_at = row.get("observed_at", "")
            if not observed_at:
                continue
            if current and current["status"] == status:
                current["last_observed_at"] = observed_at
                continue
            if current:
                compacted.append(current)
            current = {
                "listing_key": listing_identity(row), "ticker": row["ticker"],
                "exchange": row["exchange"], "status": status,
                "first_observed_at": observed_at, "last_observed_at": observed_at,
                "effective_at": row.get("effective_at", ""),
                "status_source": row.get("status_source", "snapshot"),
                "source_report": row.get("source_report", ""),
                "evidence_status": evidence,
            }
        if current:
            compacted.append(current)
    return compacted


def normalize_status_history(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    if rows and "first_observed_at" not in rows[0]:
        rows = compact_legacy_status_history(rows)
    normalized = []
    for row in rows:
        status = row.get("status", "")
        evidence = row.get("evidence_status", "observed_unverified")
        if status not in VALID_LISTING_STATUSES:
            continue
        if status == "delisted" and evidence not in TRUSTED_EVIDENCE:
            continue
        first = row.get("first_observed_at") or row.get("observed_at", "")
        last = row.get("last_observed_at") or row.get("observed_at", "") or first
        if not first:
            continue
        if last < first:
            last = first
        normalized.append({
            "listing_key": listing_identity(row), "ticker": row["ticker"],
            "exchange": row["exchange"], "status": status,
            "first_observed_at": first, "last_observed_at": last,
            "effective_at": row.get("effective_at", ""),
            "status_source": row.get("status_source", "snapshot"),
            "source_report": row.get("source_report", ""),
            "evidence_status": evidence,
        })
    return sorted(normalized, key=lambda row: (row["listing_key"], row["first_observed_at"], row["status"]))


def _event_base(row: dict[str, str], event_type: str, observed_at: str) -> dict[str, str]:
    return {
        "listing_key": listing_identity(row), "ticker": row["ticker"],
        "exchange": row["exchange"], "event_type": event_type, "field_name": "",
        "old_value": "", "new_value": "", "before_row_sha256": "",
        "effective_at": "", "observed_at": observed_at, "source_key": "",
        "source_url": "", "source_report": "", "observation_id": "",
        "evidence_status": "observed_unverified",
    }


def load_change_evidence() -> dict[tuple[str, str, str, str], dict[str, str]]:
    evidence: dict[tuple[str, str, str, str], dict[str, str]] = {}
    for row in load_csv(OFFICIAL_NAME_RECONCILIATION_CSV):
        if row.get("action", "applied") != "applied":
            continue
        evidence[(listing_identity(row), "name", row.get("old_name", ""), row.get("new_name", ""))] = {
            "source_key": row.get("source_key", ""), "source_url": row.get("source_url", ""),
            "source_report": str(OFFICIAL_NAME_RECONCILIATION_CSV.relative_to(ROOT)),
            "observation_id": row.get("observation_id", ""), "evidence_status": "official",
        }
    for row in load_csv(IDENTIFIER_QUARANTINE_CSV):
        old = row.get("isin", "")
        new = row.get("retained_isin", "")
        if not row.get("action", "").startswith("cleared") or not old or old == new:
            continue
        raw = row.get("evidence_status", "")
        if raw.startswith("reviewed_"):
            status = "reviewed"
        elif raw.startswith("official_"):
            status = "official"
        else:
            # A detector result or absence of decisive evidence is not proof
            # authorising a destructive identifier change.
            continue
        evidence[(listing_identity(row), "isin", old, new)] = {
            "source_key": "identifier_quarantine", "source_url": "",
            "source_report": str(IDENTIFIER_QUARANTINE_CSV.relative_to(ROOT)),
            "observation_id": row.get("conflict_id", ""), "evidence_status": status,
        }
    for row in load_csv(METADATA_UPDATES_CSV):
        decision = str(row.get("decision", "") or "").strip()
        if decision not in {"update", "clear"}:
            continue
        field = str(row.get("field", "") or "").strip()
        if field not in CRITICAL_FIELD_EVENT_TYPES:
            continue
        ticker = str(row.get("ticker", "") or "").strip().upper()
        exchange = str(row.get("exchange", "") or "").strip()
        new = str(row.get("proposed_value", "") or "")
        if not ticker or not exchange or (decision == "update" and not new) or (decision == "clear" and new):
            continue
        evidence[(f"{exchange}::{ticker}", field, "*", new)] = {
            "source_key": "review_metadata_updates",
            "source_url": "",
            "source_report": str(METADATA_UPDATES_CSV.relative_to(ROOT)),
            "observation_id": f"{exchange}::{ticker}:{field}",
            "evidence_status": "reviewed",
        }
    return evidence


def _inferred_country_from_isin(isin: str) -> tuple[str, str]:
    try:
        from scripts.rebuild_dataset import COUNTRY_TO_ISO, country_from_isin
    except ModuleNotFoundError:  # pragma: no cover
        from rebuild_dataset import COUNTRY_TO_ISO, country_from_isin
    country = country_from_isin(isin) or ""
    return country, COUNTRY_TO_ISO.get(country, "") if country else ""


def _stamp_country_inference_from_isin(events: list[dict[str, str]]) -> None:
    isin_event = next(
        (
            event
            for event in events
            if event.get("field_name") == "isin"
            and event.get("evidence_status") in TRUSTED_EVIDENCE
            and event.get("new_value")
        ),
        None,
    )
    if isin_event is None:
        return
    inferred_country, inferred_code = _inferred_country_from_isin(isin_event["new_value"])
    provenance = {
        "source_key": isin_event.get("source_key", ""),
        "source_url": isin_event.get("source_url", ""),
        "source_report": isin_event.get("source_report", ""),
        "observation_id": f"{isin_event.get('observation_id', '')}:isin_prefix_country",
        "evidence_status": "verified",
    }
    for event in events:
        if event.get("evidence_status") in TRUSTED_EVIDENCE:
            continue
        if event.get("field_name") == "country" and inferred_country and event.get("new_value") == inferred_country:
            event.update(provenance)
        elif event.get("field_name") == "country_code" and inferred_code and event.get("new_value") == inferred_code:
            event.update(provenance)


def build_event_rows(
    previous_snapshot: list[dict[str, str]], current_snapshot: list[dict[str, str]],
    observed_at: str,
    change_evidence: dict[tuple[str, str, str, str], dict[str, str]] | None = None,
) -> list[dict[str, str]]:
    if not previous_snapshot:
        return []
    evidence = change_evidence or {}
    previous = {listing_identity(row): row for row in previous_snapshot}
    current = {listing_identity(row): row for row in current_snapshot}
    events: list[dict[str, str]] = []
    for key, row in sorted(current.items()):
        before = previous.get(key)
        if before is None:
            event = _event_base(row, "listed", observed_at)
            event["new_value"] = row.get("name", "")
            events.append(event)
            continue
        row_events: list[dict[str, str]] = []
        for field, event_type in CRITICAL_FIELD_EVENT_TYPES.items():
            old = str(before.get(field, "") or "")
            new = str(row.get(field, "") or "")
            if old == new:
                continue
            if field == "isin" and old and not new:
                event_type = "identifier_removed"
            event = _event_base(row, event_type, observed_at)
            event.update({
                "field_name": field, "old_value": old, "new_value": new,
                "before_row_sha256": row_fingerprint(before),
            })
            event.update(evidence.get((key, field, old, new), {}))
            if event.get("evidence_status") == "observed_unverified":
                event.update(evidence.get((key, field, "*", new), {}))
            row_events.append(event)
        _stamp_country_inference_from_isin(row_events)
        events.extend(row_events)
    for key, row in sorted(previous.items()):
        if key in current:
            continue
        event = _event_base(row, "not_observed", observed_at)
        event.update({"old_value": row.get("name", ""), "before_row_sha256": row_fingerprint(row)})
        events.append(event)
    return events


def delisting_apply_status_rows(payload: dict[str, Any]) -> list[dict[str, str]]:
    generated = str(payload.get("summary", {}).get("generated_at", ""))
    source_report = str(payload.get("summary", {}).get("delisting_report_json", ""))
    rows = []
    evidence_rows = [
        *(payload.get("applied", []) or []),
        *(payload.get("already_applied", []) or []),
    ]
    for row in evidence_rows:
        if (
            row.get("classification") != "delisted"
            or not row.get("ticker")
            or not row.get("exchange")
            or not valid_official_delisting_evidence(row)
        ):
            continue
        rows.append({
            "listing_key": listing_identity(row), "ticker": row["ticker"],
            "exchange": row["exchange"], "status": "delisted",
            "first_observed_at": row.get("observed_at", "") or generated,
            "last_observed_at": row.get("observed_at", "") or generated,
            "effective_at": row.get("effective_at", ""), "status_source": "delisting_apply",
            "source_report": source_report, "source_key": row.get("source_key", ""),
            "source_url": row.get("source_url", ""), "observation_id": row.get("observation_id", ""),
            "evidence_status": "official",
        })
    return rows


def normalize_existing_events(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    normalized = []
    for source in rows:
        row = {field: source.get(field, "") for field in EVENT_FIELDS}
        if row["event_type"] == "delisted" and row["evidence_status"] not in TRUSTED_EVIDENCE:
            row["event_type"] = "not_observed"
            row["evidence_status"] = "observed_unverified"
        normalized.append(row)
    return normalized


def build_status_evidence_event_rows(
    evidence_rows: list[dict[str, str]], existing_events: list[dict[str, str]],
    previous_snapshot: list[dict[str, str]] | None = None,
) -> list[dict[str, str]]:
    existing = {
        (listing_identity(row), row.get("event_type", ""), row.get("effective_at") or row.get("observed_at", ""), row.get("field_name", ""), row.get("old_value", ""), row.get("new_value", ""))
        for row in existing_events
    }
    previous = {listing_identity(row): row for row in (previous_snapshot or [])}
    events = []
    for row in evidence_rows:
        if row["status"] not in {"suspended", "delisted"}:
            continue
        before = previous.get(listing_identity(row), {})
        event = _event_base(row, row["status"], row.get("first_observed_at", ""))
        event.update({
            "old_value": before.get("name", ""), "new_value": row["status"],
            "before_row_sha256": row_fingerprint(before) if before else "",
            "effective_at": row.get("effective_at", ""),
            "source_key": row.get("source_key", ""), "source_url": row.get("source_url", ""),
            "source_report": row.get("source_report", ""), "observation_id": row.get("observation_id", ""),
            "evidence_status": row.get("evidence_status", "observed_unverified"),
        })
        key = (event["listing_key"], event["event_type"], event["effective_at"] or event["observed_at"], event["field_name"], event["old_value"], event["new_value"])
        if key not in existing:
            events.append(event)
            existing.add(key)
    return events


def merge_status_history(
    existing_rows: list[dict[str, str]], previous_snapshot: list[dict[str, str]],
    current_snapshot: list[dict[str, str]], observed_at: str,
    status_evidence_rows: list[dict[str, str]] | None = None,
) -> list[dict[str, str]]:
    grouped: dict[str, list[dict[str, str]]] = {}
    for row in normalize_status_history(existing_rows):
        grouped.setdefault(listing_identity(row), []).append(row)

    def upsert(row: dict[str, str], status: str, *, at: str = observed_at, effective: str = "", source: str = "snapshot", report: str = "", evidence: str = "observed_unverified") -> None:
        if status not in VALID_LISTING_STATUSES:
            return
        intervals = grouped.setdefault(listing_identity(row), [])
        if intervals and intervals[-1]["status"] == status:
            intervals[-1]["last_observed_at"] = max(intervals[-1]["last_observed_at"], at)
            return
        intervals.append({
            "listing_key": listing_identity(row), "ticker": row["ticker"], "exchange": row["exchange"],
            "status": status, "first_observed_at": at, "last_observed_at": at,
            "effective_at": effective, "status_source": source, "source_report": report,
            "evidence_status": evidence,
        })

    for row in current_snapshot:
        upsert(row, "active")
    # Snapshot disappearance is not a status transition. Only explicit evidence
    # can create suspended/delisted intervals.
    for row in status_evidence_rows or []:
        upsert(
            row, row["status"], at=row.get("first_observed_at") or observed_at,
            effective=row.get("effective_at", ""), source=row.get("status_source", ""),
            report=row.get("source_report", ""), evidence=row.get("evidence_status", "observed_unverified"),
        )
    return sorted((row for rows in grouped.values() for row in rows), key=lambda row: (row["listing_key"], row["first_observed_at"], row["status"]))


def build_daily_summary(current: list[dict[str, str]], events: list[dict[str, str]], observed_at: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    types: dict[str, int] = {}
    exchanges: dict[str, dict[str, Any]] = {}
    def blank(exchange: str) -> dict[str, Any]:
        return {"observed_at": observed_at, "exchange": exchange, "listed": 0, "renamed": 0, "suspended": 0, "delisted": 0, "not_observed": 0, "active_snapshot_rows": 0}
    for event in events:
        typ = event["event_type"]
        types[typ] = types.get(typ, 0) + 1
        row = exchanges.setdefault(event["exchange"], blank(event["exchange"]))
        row[typ] = row.get(typ, 0) + 1
    for listing in current:
        exchanges.setdefault(listing["exchange"], blank(listing["exchange"]))["active_snapshot_rows"] += 1
    rows = sorted(exchanges.values(), key=lambda row: row["exchange"])
    return {
        "observed_at": observed_at, "active_snapshot_rows": len(current), "new_events": len(events),
        "listed": types.get("listed", 0), "renamed": types.get("renamed", 0),
        "suspended": types.get("suspended", 0), "delisted": types.get("delisted", 0),
        "not_observed": types.get("not_observed", 0), "exchange_rows": len(rows),
    }, rows


def listing_status_on_date(history_rows: list[dict[str, str]], key: str, date_value: str) -> str:
    candidates = []
    for row in normalize_status_history(history_rows):
        if row["listing_key"] != key:
            continue
        start = row["effective_at"] or row["first_observed_at"]
        if start <= date_value:
            candidates.append((start, row))
    return max(candidates, key=lambda item: item[0])[1]["status"] if candidates else ""


def was_listing_active_on_date(history_rows: list[dict[str, str]], key: str, date_value: str) -> bool:
    return listing_status_on_date(history_rows, key, date_value) == "active"


def build_history(previous_listings: list[dict[str, str]] | None = None) -> dict[str, Any]:
    current_rows, observed_at = load_current_rows()
    current = build_snapshot(current_rows, observed_at)
    previous = (
        build_snapshot(previous_listings, observed_at)
        if previous_listings is not None
        else load_csv(LATEST_SNAPSHOT_CSV)
    )
    existing_events = normalize_existing_events(load_csv(LISTING_EVENTS_CSV))
    evidence_rows = delisting_apply_status_rows(load_json(DELISTING_APPLY_JSON))
    new_events = [
        *build_event_rows(previous, current, observed_at, load_change_evidence()),
        *build_status_evidence_event_rows(evidence_rows, existing_events, previous),
    ]
    keys = {
        (listing_identity(row), row["event_type"], row.get("effective_at") or row.get("observed_at", ""), row.get("field_name", ""), row.get("old_value", ""), row.get("new_value", ""))
        for row in existing_events
    }
    merged_events = list(existing_events)
    for row in new_events:
        key = (listing_identity(row), row["event_type"], row.get("effective_at") or row["observed_at"], row["field_name"], row["old_value"], row["new_value"])
        if key not in keys:
            merged_events.append(row)
            keys.add(key)
    status_history = merge_status_history(load_csv(LISTING_STATUS_HISTORY_CSV), previous, current, observed_at, evidence_rows)
    write_csv(LATEST_SNAPSHOT_CSV, [
        "listing_key", "ticker", "exchange", "name", "asset_type", "country", "country_code",
        "isin", "stock_sector", "etf_category", "status", "observed_at",
    ], current)
    write_csv(LISTING_EVENTS_CSV, EVENT_FIELDS, sorted(merged_events, key=lambda row: (row.get("effective_at") or row.get("observed_at", ""), row["listing_key"], row["event_type"])))
    write_csv(LISTING_STATUS_HISTORY_CSV, STATUS_HISTORY_FIELDS, status_history)
    daily, daily_rows = build_daily_summary(current, new_events, observed_at)
    DAILY_LISTING_SUMMARY_JSON.write_text(json.dumps(daily, indent=2) + "\n", encoding="utf-8")
    write_csv(DAILY_LISTING_SUMMARY_CSV, ["observed_at", "exchange", "listed", "renamed", "suspended", "delisted", "not_observed", "active_snapshot_rows"], daily_rows)
    return {"snapshot_rows": len(current), "new_events": len(new_events), "total_events": len(merged_events), "status_rows": len(status_history), "observed_at": observed_at, "daily_summary": daily}


def main() -> None:
    print(json.dumps(build_history(), indent=2))


if __name__ == "__main__":
    main()

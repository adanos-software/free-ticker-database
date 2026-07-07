from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Iterable

try:
    from scripts.listing_keys import row_listing_key
except ModuleNotFoundError:  # pragma: no cover - script execution path
    from listing_keys import row_listing_key


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
DELISTING_APPLY_JSON = DATA_DIR / "reports" / "delisting_apply.json"
VALID_LISTING_STATUSES = {"active", "suspended", "delisted"}
STATUS_HISTORY_FIELDS = [
    "listing_key",
    "ticker",
    "exchange",
    "status",
    "first_observed_at",
    "last_observed_at",
    "effective_at",
    "status_source",
    "source_report",
]


def write_csv(path: Path, fieldnames: list[str], rows: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
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


def load_current_rows() -> tuple[list[dict[str, str]], str]:
    with TICKERS_JSON.open(encoding="utf-8") as handle:
        built_at = json.load(handle)["_meta"]["built_at"]
    return load_csv(LISTINGS_CSV), built_at


def listing_identity(row: dict[str, str]) -> str:
    return row.get("listing_key") or row_listing_key(row)


def compact_legacy_status_history(existing_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    by_listing: dict[str, list[dict[str, str]]] = {}
    for row in existing_rows:
        key = listing_identity(row)
        by_listing.setdefault(key, []).append(row)

    compacted: list[dict[str, str]] = []
    for rows in by_listing.values():
        current_interval: dict[str, str] | None = None
        for row in sorted(rows, key=lambda value: (value["observed_at"], value["status"])):
            listing_key = listing_identity(row)
            observed_at = row["observed_at"]
            status = row["status"]
            if current_interval and current_interval["status"] == status:
                current_interval["last_observed_at"] = observed_at
                continue
            if current_interval:
                compacted.append(current_interval)
            current_interval = {
                "listing_key": listing_key,
                "ticker": row["ticker"],
                "exchange": row["exchange"],
                "status": status,
                "first_observed_at": observed_at,
                "last_observed_at": observed_at,
                "effective_at": row.get("effective_at", ""),
                "status_source": row.get("status_source", "snapshot"),
                "source_report": row.get("source_report", ""),
            }
        if current_interval:
            compacted.append(current_interval)

    return compacted


def normalize_status_history(existing_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    if not existing_rows:
        return []
    if "first_observed_at" not in existing_rows[0]:
        existing_rows = compact_legacy_status_history(existing_rows)

    normalized: list[dict[str, str]] = []
    for row in existing_rows:
        listing_key = listing_identity(row)
        first_observed_at = row.get("first_observed_at") or row.get("observed_at", "")
        last_observed_at = row.get("last_observed_at") or row.get("observed_at", "")
        status = row.get("status", "")
        if status not in VALID_LISTING_STATUSES:
            continue
        if not first_observed_at:
            continue
        if last_observed_at and last_observed_at < first_observed_at:
            last_observed_at = first_observed_at
        normalized.append(
            {
                "listing_key": listing_key,
                "ticker": row["ticker"],
                "exchange": row["exchange"],
                "status": status,
                "first_observed_at": first_observed_at,
                "last_observed_at": last_observed_at or first_observed_at,
                "effective_at": row.get("effective_at", ""),
                "status_source": row.get("status_source", "snapshot"),
                "source_report": row.get("source_report", ""),
            }
        )

    return sorted(
        normalized,
        key=lambda row: (row["first_observed_at"], row["ticker"], row["exchange"], row["status"]),
    )


def sector_model_fields(row: dict[str, str]) -> tuple[str, str]:
    asset_type = row.get("asset_type", "")
    legacy_sector = row.get("sector", "")
    stock_sector = row.get("stock_sector", "")
    etf_category = row.get("etf_category", "")
    if asset_type == "Stock":
        stock_sector = stock_sector or legacy_sector
        etf_category = ""
        legacy_sector = stock_sector
    elif asset_type == "ETF":
        etf_category = etf_category or legacy_sector
        stock_sector = ""
    return stock_sector, etf_category


def build_snapshot(rows: list[dict[str, str]], observed_at: str) -> list[dict[str, str]]:
    snapshot: list[dict[str, str]] = []
    for row in rows:
        stock_sector, etf_category = sector_model_fields(row)
        snapshot.append(
            {
                "listing_key": listing_identity(row),
                "ticker": row["ticker"],
                "exchange": row["exchange"],
                "name": row["name"],
                "asset_type": row["asset_type"],
                "country": row["country"],
                "country_code": row["country_code"],
                "isin": row["isin"],
                "stock_sector": stock_sector,
                "etf_category": etf_category,
                "status": "active",
                "observed_at": observed_at,
            }
        )
    return snapshot


def build_event_rows(
    previous_snapshot: list[dict[str, str]],
    current_snapshot: list[dict[str, str]],
    observed_at: str,
) -> list[dict[str, str]]:
    if not previous_snapshot:
        return []
    previous = {listing_identity(row): row for row in previous_snapshot}
    current = {listing_identity(row): row for row in current_snapshot}
    events: list[dict[str, str]] = []

    for key, row in sorted(current.items(), key=lambda item: (item[1]["ticker"], item[1]["exchange"])):
        previous_row = previous.get(key)
        if previous_row is None:
            events.append(
                {
                    "listing_key": listing_identity(row),
                    "ticker": row["ticker"],
                    "exchange": row["exchange"],
                    "event_type": "listed",
                    "old_value": "",
                    "new_value": row["name"],
                    "observed_at": observed_at,
                }
            )
            continue
        if previous_row["name"] != row["name"]:
            events.append(
                {
                    "listing_key": listing_identity(row),
                    "ticker": row["ticker"],
                    "exchange": row["exchange"],
                    "event_type": "renamed",
                    "old_value": previous_row["name"],
                    "new_value": row["name"],
                    "observed_at": observed_at,
                }
            )

    for key, row in sorted(previous.items(), key=lambda item: (item[1]["ticker"], item[1]["exchange"])):
        if key in current:
            continue
        events.append(
            {
                "listing_key": listing_identity(row),
                "ticker": row["ticker"],
                "exchange": row["exchange"],
                "event_type": "delisted",
                "old_value": row["name"],
                "new_value": "",
                "observed_at": observed_at,
            }
        )

    return events


def delisting_apply_status_rows(payload: dict[str, Any]) -> list[dict[str, str]]:
    generated_at = str(payload.get("summary", {}).get("generated_at", ""))
    source_report = str(payload.get("summary", {}).get("delisting_report_json", ""))
    rows: list[dict[str, str]] = []
    for section, status in (("applied", "delisted"), ("drafted", "delisted"), ("blocked", "suspended")):
        for row in payload.get(section, []) or []:
            classification = row.get("classification", "")
            if section == "blocked" and classification != "suspended":
                continue
            if section in {"applied", "drafted"} and classification != "delisted":
                continue
            ticker = row.get("ticker", "")
            exchange = row.get("exchange", "")
            if not ticker or not exchange:
                continue
            rows.append(
                {
                    "listing_key": listing_identity(row),
                    "ticker": ticker,
                    "exchange": exchange,
                    "status": status,
                    "first_observed_at": generated_at,
                    "last_observed_at": generated_at,
                    "effective_at": row.get("effective_at", "") or generated_at,
                    "status_source": "delisting_apply",
                    "source_report": source_report,
                }
            )
    return rows


def build_status_evidence_event_rows(
    evidence_rows: list[dict[str, str]],
    existing_events: list[dict[str, str]],
) -> list[dict[str, str]]:
    existing_keys = {
        (listing_identity(row), row.get("event_type", ""), row.get("observed_at", ""))
        for row in existing_events
    }
    events: list[dict[str, str]] = []
    for row in evidence_rows:
        event_type = row["status"]
        if event_type not in {"suspended", "delisted"}:
            continue
        observed_at = row.get("effective_at") or row.get("first_observed_at", "")
        key = (listing_identity(row), event_type, observed_at)
        if key in existing_keys:
            continue
        events.append(
            {
                "listing_key": listing_identity(row),
                "ticker": row["ticker"],
                "exchange": row["exchange"],
                "event_type": event_type,
                "old_value": "",
                "new_value": row["status"],
                "observed_at": observed_at,
            }
        )
    return events


def merge_status_history(
    existing_rows: list[dict[str, str]],
    previous_snapshot: list[dict[str, str]],
    current_snapshot: list[dict[str, str]],
    observed_at: str,
    status_evidence_rows: list[dict[str, str]] | None = None,
) -> list[dict[str, str]]:
    normalized_rows = normalize_status_history(existing_rows)
    by_listing: dict[str, list[dict[str, str]]] = {}
    for row in normalized_rows:
        key = listing_identity(row)
        by_listing.setdefault(key, []).append(row)

    def upsert_status_interval(
        row: dict[str, str],
        status: str,
        *,
        interval_observed_at: str = observed_at,
        effective_at: str = "",
        status_source: str = "snapshot",
        source_report: str = "",
    ) -> None:
        if status not in VALID_LISTING_STATUSES:
            return
        key = listing_identity(row)
        listing_key = listing_identity(row)
        intervals = by_listing.setdefault(key, [])
        if intervals and intervals[-1]["status"] == status:
            if interval_observed_at > intervals[-1]["last_observed_at"]:
                intervals[-1]["last_observed_at"] = interval_observed_at
            intervals[-1]["listing_key"] = listing_key
            intervals[-1]["effective_at"] = intervals[-1].get("effective_at") or effective_at
            intervals[-1]["status_source"] = status_source or intervals[-1].get("status_source", "")
            intervals[-1]["source_report"] = source_report or intervals[-1].get("source_report", "")
            return
        intervals.append(
            {
                "listing_key": listing_key,
                "ticker": row["ticker"],
                "exchange": row["exchange"],
                "status": status,
                "first_observed_at": interval_observed_at,
                "last_observed_at": interval_observed_at,
                "effective_at": effective_at,
                "status_source": status_source,
                "source_report": source_report,
            }
        )

    current_keys = {listing_identity(row): row for row in current_snapshot}

    for row in current_snapshot:
        upsert_status_interval(row, "active")

    for row in previous_snapshot:
        if listing_identity(row) in current_keys:
            continue
        upsert_status_interval(row, "delisted")

    for row in status_evidence_rows or []:
        upsert_status_interval(
            row,
            row["status"],
            interval_observed_at=row.get("first_observed_at") or row.get("effective_at") or observed_at,
            effective_at=row.get("effective_at", ""),
            status_source=row.get("status_source", ""),
            source_report=row.get("source_report", ""),
        )

    merged = [row for rows in by_listing.values() for row in rows]
    return sorted(
        merged,
        key=lambda row: (row["first_observed_at"], row["ticker"], row["exchange"], row["status"]),
    )


def build_daily_summary(
    current_snapshot: list[dict[str, str]],
    new_events: list[dict[str, str]],
    observed_at: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    event_type_counts: dict[str, int] = {}
    by_exchange: dict[str, dict[str, Any]] = {}
    for event in new_events:
        event_type = event["event_type"]
        exchange = event["exchange"]
        event_type_counts[event_type] = event_type_counts.get(event_type, 0) + 1
        exchange_row = by_exchange.setdefault(
            exchange,
            {
                "observed_at": observed_at,
                "exchange": exchange,
                "listed": 0,
                "renamed": 0,
                "suspended": 0,
                "delisted": 0,
                "active_snapshot_rows": 0,
            },
        )
        if event_type not in exchange_row:
            exchange_row[event_type] = 0
        exchange_row[event_type] += 1

    for row in current_snapshot:
        exchange_row = by_exchange.setdefault(
            row["exchange"],
            {
                "observed_at": observed_at,
                "exchange": row["exchange"],
                "listed": 0,
                "renamed": 0,
                "suspended": 0,
                "delisted": 0,
                "active_snapshot_rows": 0,
            },
        )
        exchange_row["active_snapshot_rows"] += 1

    rows = sorted(by_exchange.values(), key=lambda row: row["exchange"])
    summary = {
        "observed_at": observed_at,
        "active_snapshot_rows": len(current_snapshot),
        "new_events": len(new_events),
        "listed": event_type_counts.get("listed", 0),
        "renamed": event_type_counts.get("renamed", 0),
        "suspended": event_type_counts.get("suspended", 0),
        "delisted": event_type_counts.get("delisted", 0),
        "exchange_rows": len(rows),
    }
    return summary, rows


def listing_status_on_date(history_rows: list[dict[str, str]], listing_key: str, date_value: str) -> str:
    candidates = [row for row in normalize_status_history(history_rows) if row["listing_key"] == listing_key]
    matching = []
    for row in candidates:
        start = row["effective_at"] or row["first_observed_at"]
        end = row["last_observed_at"]
        if start <= date_value <= end:
            matching.append((start, row))
    if matching:
        return max(matching, key=lambda item: item[0])[1]["status"]
    return ""


def was_listing_active_on_date(history_rows: list[dict[str, str]], listing_key: str, date_value: str) -> bool:
    return listing_status_on_date(history_rows, listing_key, date_value) == "active"


def build_history() -> dict[str, Any]:
    current_rows, observed_at = load_current_rows()
    current_snapshot = build_snapshot(current_rows, observed_at)
    previous_snapshot = load_csv(LATEST_SNAPSHOT_CSV)
    existing_events = load_csv(LISTING_EVENTS_CSV)
    existing_status_history = load_csv(LISTING_STATUS_HISTORY_CSV)
    status_evidence_rows = delisting_apply_status_rows(load_json(DELISTING_APPLY_JSON))

    new_events = [
        *build_event_rows(previous_snapshot, current_snapshot, observed_at),
        *build_status_evidence_event_rows(status_evidence_rows, existing_events),
    ]
    event_keys = {
        (listing_identity(row), row["event_type"], row["observed_at"])
        for row in existing_events
    }
    merged_events = list(existing_events)
    for row in new_events:
        key = (listing_identity(row), row["event_type"], row["observed_at"])
        if key not in event_keys:
            merged_events.append(row)
            event_keys.add(key)

    merged_status_history = merge_status_history(
        existing_status_history,
        previous_snapshot,
        current_snapshot,
        observed_at,
        status_evidence_rows=status_evidence_rows,
    )

    write_csv(
        LATEST_SNAPSHOT_CSV,
        [
            "listing_key",
            "ticker",
            "exchange",
            "name",
            "asset_type",
            "country",
            "country_code",
            "isin",
            "stock_sector",
            "etf_category",
            "status",
            "observed_at",
        ],
        current_snapshot,
    )
    write_csv(
        LISTING_EVENTS_CSV,
        ["listing_key", "ticker", "exchange", "event_type", "old_value", "new_value", "observed_at"],
        sorted(merged_events, key=lambda row: (row["observed_at"], row["ticker"], row["exchange"], row["event_type"])),
    )
    write_csv(
        LISTING_STATUS_HISTORY_CSV,
        STATUS_HISTORY_FIELDS,
        merged_status_history,
    )
    daily_summary, daily_rows = build_daily_summary(current_snapshot, new_events, observed_at)
    DAILY_LISTING_SUMMARY_JSON.write_text(json.dumps(daily_summary, indent=2), encoding="utf-8")
    write_csv(
        DAILY_LISTING_SUMMARY_CSV,
        ["observed_at", "exchange", "listed", "renamed", "suspended", "delisted", "active_snapshot_rows"],
        daily_rows,
    )

    return {
        "snapshot_rows": len(current_snapshot),
        "new_events": len(new_events),
        "total_events": len(merged_events),
        "status_rows": len(merged_status_history),
        "observed_at": observed_at,
        "daily_summary": daily_summary,
    }


def main() -> None:
    summary = build_history()
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()

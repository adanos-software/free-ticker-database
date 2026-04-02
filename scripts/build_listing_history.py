from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Iterable


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
HISTORY_DIR = DATA_DIR / "history"
LATEST_SNAPSHOT_CSV = HISTORY_DIR / "latest_snapshot.csv"
LISTING_EVENTS_CSV = HISTORY_DIR / "listing_events.csv"
LISTING_STATUS_HISTORY_CSV = HISTORY_DIR / "listing_status_history.csv"
TICKERS_CSV = DATA_DIR / "tickers.csv"
TICKERS_JSON = DATA_DIR / "tickers.json"


def write_csv(path: Path, fieldnames: list[str], rows: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def load_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def load_current_rows() -> tuple[list[dict[str, str]], str]:
    with TICKERS_JSON.open(encoding="utf-8") as handle:
        built_at = json.load(handle)["_meta"]["built_at"]
    return load_csv(TICKERS_CSV), built_at


def build_snapshot(rows: list[dict[str, str]], observed_at: str) -> list[dict[str, str]]:
    return [
        {
            "ticker": row["ticker"],
            "exchange": row["exchange"],
            "name": row["name"],
            "asset_type": row["asset_type"],
            "country": row["country"],
            "country_code": row["country_code"],
            "isin": row["isin"],
            "sector": row["sector"],
            "status": "active",
            "observed_at": observed_at,
        }
        for row in rows
    ]


def build_event_rows(
    previous_snapshot: list[dict[str, str]],
    current_snapshot: list[dict[str, str]],
    observed_at: str,
) -> list[dict[str, str]]:
    if not previous_snapshot:
        return []
    previous = {(row["ticker"], row["exchange"]): row for row in previous_snapshot}
    current = {(row["ticker"], row["exchange"]): row for row in current_snapshot}
    events: list[dict[str, str]] = []

    for key, row in sorted(current.items()):
        previous_row = previous.get(key)
        if previous_row is None:
            events.append(
                {
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
                    "ticker": row["ticker"],
                    "exchange": row["exchange"],
                    "event_type": "renamed",
                    "old_value": previous_row["name"],
                    "new_value": row["name"],
                    "observed_at": observed_at,
                }
            )

    for key, row in sorted(previous.items()):
        if key in current:
            continue
        events.append(
            {
                "ticker": row["ticker"],
                "exchange": row["exchange"],
                "event_type": "delisted",
                "old_value": row["name"],
                "new_value": "",
                "observed_at": observed_at,
            }
        )

    return events


def merge_status_history(
    existing_rows: list[dict[str, str]],
    previous_snapshot: list[dict[str, str]],
    current_snapshot: list[dict[str, str]],
    observed_at: str,
) -> list[dict[str, str]]:
    merged = list(existing_rows)
    existing_keys = {
        (row["ticker"], row["exchange"], row["status"], row["observed_at"])
        for row in existing_rows
    }
    current_keys = {(row["ticker"], row["exchange"]): row for row in current_snapshot}

    for row in current_snapshot:
        history_row = {
            "ticker": row["ticker"],
            "exchange": row["exchange"],
            "status": "active",
            "observed_at": observed_at,
        }
        key = (history_row["ticker"], history_row["exchange"], history_row["status"], history_row["observed_at"])
        if key not in existing_keys:
            merged.append(history_row)
            existing_keys.add(key)

    for row in previous_snapshot:
        if (row["ticker"], row["exchange"]) in current_keys:
            continue
        history_row = {
            "ticker": row["ticker"],
            "exchange": row["exchange"],
            "status": "delisted",
            "observed_at": observed_at,
        }
        key = (history_row["ticker"], history_row["exchange"], history_row["status"], history_row["observed_at"])
        if key not in existing_keys:
            merged.append(history_row)
            existing_keys.add(key)

    return sorted(merged, key=lambda row: (row["observed_at"], row["ticker"], row["exchange"], row["status"]))


def build_history() -> dict[str, Any]:
    current_rows, observed_at = load_current_rows()
    current_snapshot = build_snapshot(current_rows, observed_at)
    previous_snapshot = load_csv(LATEST_SNAPSHOT_CSV)
    existing_events = load_csv(LISTING_EVENTS_CSV)
    existing_status_history = load_csv(LISTING_STATUS_HISTORY_CSV)

    new_events = build_event_rows(previous_snapshot, current_snapshot, observed_at)
    event_keys = {
        (row["ticker"], row["exchange"], row["event_type"], row["observed_at"])
        for row in existing_events
    }
    merged_events = list(existing_events)
    for row in new_events:
        key = (row["ticker"], row["exchange"], row["event_type"], row["observed_at"])
        if key not in event_keys:
            merged_events.append(row)
            event_keys.add(key)

    merged_status_history = merge_status_history(
        existing_status_history,
        previous_snapshot,
        current_snapshot,
        observed_at,
    )

    write_csv(
        LATEST_SNAPSHOT_CSV,
        ["ticker", "exchange", "name", "asset_type", "country", "country_code", "isin", "sector", "status", "observed_at"],
        current_snapshot,
    )
    write_csv(
        LISTING_EVENTS_CSV,
        ["ticker", "exchange", "event_type", "old_value", "new_value", "observed_at"],
        sorted(merged_events, key=lambda row: (row["observed_at"], row["ticker"], row["exchange"], row["event_type"])),
    )
    write_csv(
        LISTING_STATUS_HISTORY_CSV,
        ["ticker", "exchange", "status", "observed_at"],
        merged_status_history,
    )

    return {
        "snapshot_rows": len(current_snapshot),
        "new_events": len(new_events),
        "total_events": len(merged_events),
        "status_rows": len(merged_status_history),
        "observed_at": observed_at,
    }


def main() -> None:
    summary = build_history()
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()

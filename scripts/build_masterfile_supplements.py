from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

try:
    from scripts.rebuild_dataset import should_exclude_stock_row
except ModuleNotFoundError:  # pragma: no cover - script execution path
    from rebuild_dataset import should_exclude_stock_row

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
TICKERS_CSV = DATA_DIR / "tickers.csv"
MASTERFILE_REFERENCE_CSV = DATA_DIR / "masterfiles" / "reference.csv"
MASTERFILE_SUPPLEMENT_CSV = DATA_DIR / "masterfiles" / "supplemental_listings.csv"
MASTERFILE_SUPPLEMENT_SUMMARY_JSON = DATA_DIR / "masterfiles" / "supplemental_summary.json"

SUPPLEMENT_EXCHANGES: dict[str, dict[str, str]] = {
    "TSE": {
        "country": "Japan",
        "country_code": "JP",
    }
}


def load_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def build_supplement_rows(
    core_rows: list[dict[str, str]],
    masterfile_rows: list[dict[str, str]],
) -> tuple[list[dict[str, str]], dict[str, Any]]:
    core_exchanges_by_ticker: dict[str, set[str]] = {}
    for row in core_rows:
        core_exchanges_by_ticker.setdefault(row["ticker"], set()).add(row["exchange"])

    supplements: list[dict[str, str]] = []
    summary: dict[str, Any] = {
        "supplement_rows": 0,
        "safe_missing_rows": 0,
        "refreshable_existing_rows": 0,
        "colliding_rows_skipped": 0,
        "by_exchange": {},
    }

    seen: set[tuple[str, str]] = set()
    for row in masterfile_rows:
        exchange = row["exchange"]
        if exchange not in SUPPLEMENT_EXCHANGES:
            continue
        if row.get("listing_status") != "active":
            continue
        if row.get("reference_scope") != "exchange_directory":
            continue

        ticker = row["ticker"]
        exchanges = core_exchanges_by_ticker.get(ticker, set())
        stats = summary["by_exchange"].setdefault(
            exchange,
            {
                "safe_missing_rows": 0,
                "refreshable_existing_rows": 0,
                "colliding_rows_skipped": 0,
            },
        )
        if exchanges and exchanges != {exchange}:
            summary["colliding_rows_skipped"] += 1
            stats["colliding_rows_skipped"] += 1
            continue

        key = (ticker, exchange)
        if key in seen:
            continue
        seen.add(key)

        exchange_meta = SUPPLEMENT_EXCHANGES[exchange]
        candidate = {
            "ticker": ticker,
            "name": row["name"],
            "exchange": exchange,
            "asset_type": row["asset_type"],
            "sector": "",
            "country": exchange_meta["country"],
            "country_code": exchange_meta["country_code"],
            "isin": "",
            "aliases": "",
            "source_key": row.get("source_key", ""),
            "source_url": row.get("source_url", ""),
            "reference_scope": row.get("reference_scope", ""),
        }
        if should_exclude_stock_row(candidate):
            summary["colliding_rows_skipped"] += 1
            stats["colliding_rows_skipped"] += 1
            continue

        if exchanges == {exchange}:
            summary["refreshable_existing_rows"] += 1
            stats["refreshable_existing_rows"] += 1
        else:
            summary["safe_missing_rows"] += 1
            stats["safe_missing_rows"] += 1

        supplements.append(candidate)

    supplements.sort(key=lambda row: (row["exchange"], row["ticker"]))
    summary["supplement_rows"] = len(supplements)
    return supplements, summary


def main() -> dict[str, Any]:
    core_rows = load_csv(TICKERS_CSV)
    masterfile_rows = load_csv(MASTERFILE_REFERENCE_CSV)
    supplement_rows, summary = build_supplement_rows(core_rows, masterfile_rows)
    fieldnames = [
        "ticker",
        "name",
        "exchange",
        "asset_type",
        "sector",
        "country",
        "country_code",
        "isin",
        "aliases",
        "source_key",
        "source_url",
        "reference_scope",
    ]
    write_csv(MASTERFILE_SUPPLEMENT_CSV, fieldnames, supplement_rows)
    MASTERFILE_SUPPLEMENT_SUMMARY_JSON.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return summary


if __name__ == "__main__":
    main()

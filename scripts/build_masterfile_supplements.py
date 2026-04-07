from __future__ import annotations

import csv
import json
import re
from pathlib import Path
from typing import Any

try:
    from scripts.rebuild_dataset import normalize_tokens, normalized_compact, should_exclude_stock_row
except ModuleNotFoundError:  # pragma: no cover - script execution path
    from rebuild_dataset import normalize_tokens, normalized_compact, should_exclude_stock_row

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
LISTINGS_CSV = DATA_DIR / "listings.csv"
MASTERFILE_REFERENCE_CSV = DATA_DIR / "masterfiles" / "reference.csv"
MASTERFILE_SUPPLEMENT_CSV = DATA_DIR / "masterfiles" / "supplemental_listings.csv"
MASTERFILE_SUPPLEMENT_SUMMARY_JSON = DATA_DIR / "masterfiles" / "supplemental_summary.json"

SUPPLEMENT_EXCHANGES: dict[str, dict[str, str]] = {
    "AMS": {
        "country": "Netherlands",
        "country_code": "NL",
    },
    "ASX": {
        "country": "Australia",
        "country_code": "AU",
    },
    "B3": {
        "country": "Brazil",
        "country_code": "BR",
    },
    "OSL": {
        "country": "Norway",
        "country_code": "NO",
    },
    "TSE": {
        "country": "Japan",
        "country_code": "JP",
    },
    "TWSE": {
        "country": "Taiwan",
        "country_code": "TW",
    },
    "XETRA": {
        "country": "Germany",
        "country_code": "DE",
    },
}

SUPPLEMENT_EXCLUDED_STOCK_PATTERNS = [
    re.compile(r"\babs trust\b", re.IGNORECASE),
]

SUPPLEMENT_ALLOWED_REFERENCE_SCOPES_BY_EXCHANGE: dict[str, set[str]] = {
    "XETRA": {"exchange_directory", "listed_companies_subset"},
}
LOCAL_LANGUAGE_REFRESH_EXCHANGES = {"TWSE", "TPEX"}


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
    core_rows_by_key: dict[tuple[str, str], dict[str, str]] = {}
    for row in core_rows:
        core_exchanges_by_ticker.setdefault(row["ticker"], set()).add(row["exchange"])
        core_rows_by_key[(row["ticker"], row["exchange"])] = row

    supplements: list[dict[str, str]] = []
    eligible_missing_exchanges_by_ticker: dict[str, set[str]] = {}
    for row in masterfile_rows:
        exchange = row["exchange"]
        if exchange not in SUPPLEMENT_EXCHANGES:
            continue
        if row.get("listing_status") != "active":
            continue
        allowed_reference_scopes = SUPPLEMENT_ALLOWED_REFERENCE_SCOPES_BY_EXCHANGE.get(
            exchange,
            {"exchange_directory"},
        )
        if row.get("reference_scope") not in allowed_reference_scopes:
            continue
        ticker = row["ticker"]
        if core_exchanges_by_ticker.get(ticker):
            continue
        eligible_missing_exchanges_by_ticker.setdefault(ticker, set()).add(exchange)

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
        allowed_reference_scopes = SUPPLEMENT_ALLOWED_REFERENCE_SCOPES_BY_EXCHANGE.get(
            exchange,
            {"exchange_directory"},
        )
        if row.get("reference_scope") not in allowed_reference_scopes:
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
        if not exchanges and len(eligible_missing_exchanges_by_ticker.get(ticker, set())) > 1:
            summary["colliding_rows_skipped"] += 1
            stats["colliding_rows_skipped"] += 1
            continue

        existing_row = core_rows_by_key.get((ticker, exchange))
        if existing_row and not rows_refer_to_same_entity(existing_row, row):
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
        if candidate["asset_type"] == "Stock" and any(
            pattern.search(candidate["name"]) for pattern in SUPPLEMENT_EXCLUDED_STOCK_PATTERNS
        ):
            summary["colliding_rows_skipped"] += 1
            stats["colliding_rows_skipped"] += 1
            continue
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


def rows_refer_to_same_entity(core_row: dict[str, str], masterfile_row: dict[str, str]) -> bool:
    core_isin = (core_row.get("isin") or "").strip()
    masterfile_isin = (masterfile_row.get("isin") or "").strip()
    if core_isin and masterfile_isin:
        return core_isin == masterfile_isin

    core_name = (core_row.get("name") or "").strip()
    masterfile_name = (masterfile_row.get("name") or "").strip()
    if not core_name or not masterfile_name:
        return True

    if (
        core_row.get("exchange") == masterfile_row.get("exchange")
        and core_row.get("exchange") in LOCAL_LANGUAGE_REFRESH_EXCHANGES
        and any(ord(character) > 127 and character.isalpha() for character in masterfile_name)
    ):
        return True

    core_compact = normalized_compact(core_name)
    masterfile_compact = normalized_compact(masterfile_name)
    if core_compact and masterfile_compact and (
        core_compact in masterfile_compact or masterfile_compact in core_compact
    ):
        return True

    core_tokens = normalize_tokens(core_name)
    masterfile_tokens = normalize_tokens(masterfile_name)
    if not core_tokens or not masterfile_tokens:
        return False

    overlap = len(core_tokens & masterfile_tokens)
    return overlap >= 2 and overlap / min(len(core_tokens), len(masterfile_tokens)) >= 0.5


def main() -> dict[str, Any]:
    core_rows = load_csv(LISTINGS_CSV)
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

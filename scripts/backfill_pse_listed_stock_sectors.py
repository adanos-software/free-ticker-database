from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import Counter
from html import unescape
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.fetch_exchange_masterfiles import (
    PSE_ACTIVE_SECURITY_STATUSES,
    PSE_LISTED_COMPANY_DIRECTORY_PAGE_URL,
    PSE_LISTED_COMPANY_DIRECTORY_URL,
    PSE_SECURITY_TYPE_TO_ASSET_TYPE,
)
from scripts.lib.dataio import merge_metadata_updates
from scripts.rebuild_dataset import LISTINGS_CSV, is_valid_isin, normalize_sector

DEFAULT_OUTPUT_DIR = ROOT / "data" / "pse_verification"
DEFAULT_CAPTURE_JSON = DEFAULT_OUTPUT_DIR / "listed_company_directory.json"
DEFAULT_REPORT_JSON = DEFAULT_OUTPUT_DIR / "listed_stock_sector_backfill.json"
DEFAULT_REPORT_CSV = DEFAULT_OUTPUT_DIR / "listed_stock_sector_backfill.csv"
DEFAULT_METADATA_UPDATES_CSV = ROOT / "data" / "review_overrides" / "metadata_updates.csv"
STORE_JSON_RE = re.compile(r'id="store-json"[^>]*value="([^"]+)"')
PSE_SUBSECTOR_MAP = {
    "BANKS": "Financials",
    "CASINOS & GAMING": "Consumer Discretionary",
    "CHEMICALS": "Materials",
    "CONSTRUCTION, INFR. & ALLIED": "Industrials",
    "EDUCATION": "Consumer Discretionary",
    "ELECTRICAL COMPONENTS & EQUIP.": "Industrials",
    "FOOD, BEVERAGE & TOBACCO": "Consumer Staples",
    "HOTEL & LEISURE": "Consumer Discretionary",
    "INFORMATION TECHNOLOGY": "Information Technology",
    "MEDIA": "Communication Services",
    "MINING": "Materials",
    "OIL": "Energy",
    "OTHER FINANCIAL INSTITUTIONS": "Financials",
    "OTHER INDUSTRIALS": "Industrials",
    "PROPERTY": "Real Estate",
    "TELECOMMUNICATIONS": "Communication Services",
    "TRANSPORTATION SERVICES": "Industrials",
}
REASON = (
    "Copied stock_sector from the official PSE listed-company directory after an exact "
    "ticker and ISIN match; Holding Firms, SME, Other Services, Retail, and "
    "Elec./Energy/Power & Water left unmapped."
)
REPORT_FIELDNAMES = [
    "ticker",
    "exchange",
    "asset_type",
    "name",
    "isin",
    "pse_ticker",
    "pse_name",
    "pse_isin",
    "pse_subsector",
    "sector_update",
    "decision",
]


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def map_pse_subsector(subsector: str) -> str:
    raw = " ".join((subsector or "").split()).upper()
    return normalize_sector(PSE_SUBSECTOR_MAP.get(raw, ""), "Stock")


def parse_directory_items(payload: list[dict[str, Any]]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for item in payload:
        ticker = str(item.get("SecuritySymbol") or "").strip().upper()
        name = str(item.get("SecurityName") or "").strip() or str(item.get("SecurityAlias") or "").strip()
        security_type = str(item.get("SecurityType") or "").strip().upper()
        status = str(item.get("SecurityStatus") or "").strip().upper()
        asset_type = PSE_SECURITY_TYPE_TO_ASSET_TYPE.get(security_type)
        if not ticker or not name or asset_type is None or status not in PSE_ACTIVE_SECURITY_STATUSES:
            continue
        if ticker in seen:
            continue
        isin = str(item.get("SecurityISIN") or "").strip().upper()
        rows.append(
            {
                "ticker": ticker,
                "name": name,
                "isin": isin,
                "asset_type": asset_type,
                "status": status,
                "subsector": " ".join(str(item.get("SubsectorName") or "").split()),
            }
        )
        seen.add(ticker)
    return rows


def parse_directory_html(html: str) -> list[dict[str, str]]:
    match = STORE_JSON_RE.search(html or "")
    if match is None:
        raise ValueError("PSE listed company directory JSON payload missing")
    payload = json.loads(unescape(match.group(1)))
    if not isinstance(payload, list):
        raise ValueError("PSE listed company directory JSON payload is not a list")
    return parse_directory_items(payload)


def download_directory(url: str = PSE_LISTED_COMPANY_DIRECTORY_URL, *, timeout_seconds: float = 60.0) -> list[dict[str, str]]:
    response = requests.get(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 free-ticker-database/3.0",
            "Referer": PSE_LISTED_COMPANY_DIRECTORY_PAGE_URL,
        },
        timeout=timeout_seconds,
    )
    response.raise_for_status()
    return parse_directory_html(response.text)


def index_by_ticker(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    return {row["ticker"]: row for row in rows}


def load_missing_rows(path: Path = LISTINGS_CSV, *, exchanges: set[str] | None = None) -> list[dict[str, str]]:
    selected_exchanges = exchanges or {"PSE"}
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    selected: list[dict[str, str]] = []
    for row in rows:
        if row.get("asset_type") != "Stock":
            continue
        if row.get("exchange") not in selected_exchanges:
            continue
        if (row.get("stock_sector") or row.get("sector") or "").strip():
            continue
        selected.append(row)
    return selected


def evaluate_row(row: dict[str, str], by_ticker: dict[str, dict[str, str]]) -> dict[str, Any]:
    isin = (row.get("isin") or "").strip().upper()
    ticker = (row.get("ticker") or "").strip().upper()
    base = {
        "ticker": row.get("ticker", ""),
        "exchange": row.get("exchange", ""),
        "asset_type": row.get("asset_type", ""),
        "name": row.get("name", ""),
        "isin": isin,
        "pse_ticker": "",
        "pse_name": "",
        "pse_isin": "",
        "pse_subsector": "",
        "sector_update": "",
    }
    if (row.get("stock_sector") or row.get("sector") or "").strip():
        return {**base, "decision": "already_has_sector"}
    if (row.get("exchange") or "").strip() != "PSE":
        return {**base, "decision": "not_pse"}
    if not is_valid_isin(isin):
        return {**base, "decision": "missing_or_invalid_isin"}
    official = by_ticker.get(ticker)
    if official is None:
        return {**base, "decision": "no_pse_match"}
    official_isin = (official.get("isin") or "").strip().upper()
    if official_isin != isin:
        return {
            **base,
            "pse_ticker": official.get("ticker", ""),
            "pse_name": official.get("name", ""),
            "pse_isin": official_isin,
            "pse_subsector": official.get("subsector", ""),
            "decision": "isin_mismatch",
        }
    if official.get("asset_type") != "Stock":
        return {
            **base,
            "pse_ticker": official.get("ticker", ""),
            "pse_name": official.get("name", ""),
            "pse_isin": official_isin,
            "pse_subsector": official.get("subsector", ""),
            "decision": "not_stock",
        }
    mapped = map_pse_subsector(official.get("subsector") or "")
    if mapped:
        return {
            **base,
            "pse_ticker": official.get("ticker", ""),
            "pse_name": official.get("name", ""),
            "pse_isin": official_isin,
            "pse_subsector": official.get("subsector", ""),
            "sector_update": mapped,
            "decision": "accept",
        }
    if (official.get("subsector") or "").strip():
        return {
            **base,
            "pse_ticker": official.get("ticker", ""),
            "pse_name": official.get("name", ""),
            "pse_isin": official_isin,
            "pse_subsector": official.get("subsector", ""),
            "decision": "unsupported_pse_subsector",
        }
    return {
        **base,
        "pse_ticker": official.get("ticker", ""),
        "pse_name": official.get("name", ""),
        "pse_isin": official_isin,
        "decision": "empty_pse_subsector",
    }


def build_metadata_updates(results: list[dict[str, Any]]) -> list[dict[str, str]]:
    return [
        {
            "ticker": result["ticker"],
            "exchange": result["exchange"],
            "field": "stock_sector",
            "decision": "update",
            "proposed_value": result["sector_update"],
            "confidence": "0.90",
            "reason": REASON,
        }
        for result in results
        if result.get("decision") == "accept"
    ]


def write_report_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=REPORT_FIELDNAMES, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in REPORT_FIELDNAMES})


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill missing PSE stock sectors from the official listed-company directory."
    )
    parser.add_argument("--listings-csv", type=Path, default=LISTINGS_CSV)
    parser.add_argument("--capture-json", type=Path, default=DEFAULT_CAPTURE_JSON)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_REPORT_JSON)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_REPORT_CSV)
    parser.add_argument("--metadata-updates-csv", type=Path, default=DEFAULT_METADATA_UPDATES_CSV)
    parser.add_argument("--exchange", action="append")
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    exchanges = set(args.exchange) if args.exchange else {"PSE"}
    if args.refresh or not args.capture_json.exists():
        directory_rows = download_directory()
        args.capture_json.parent.mkdir(parents=True, exist_ok=True)
        args.capture_json.write_text(json.dumps(directory_rows, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    else:
        directory_rows = json.loads(args.capture_json.read_text(encoding="utf-8"))
    by_ticker = index_by_ticker(directory_rows)
    results = [evaluate_row(row, by_ticker) for row in load_missing_rows(args.listings_csv, exchanges=exchanges)]
    updates = build_metadata_updates(results)
    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.json_out.write_text(
        json.dumps([result for result in results if result["decision"] == "accept"], indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    write_report_csv(args.csv_out, results)
    if args.apply and updates:
        merge_metadata_updates(args.metadata_updates_csv, updates)
    print(
        json.dumps(
            {
                "accepted_sector_updates": len(updates),
                "applied": args.apply,
                "candidates": len(results),
                "csv_out": display_path(args.csv_out),
                "decision_counts": dict(Counter(result["decision"] for result in results)),
                "directory_rows": len(directory_rows),
                "json_out": display_path(args.json_out),
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.lib.dataio import merge_metadata_updates
from scripts.rebuild_dataset import LISTINGS_CSV, is_valid_isin, normalize_sector

AQUIS_COMPANIES_URL = "https://aquis.eu/companies"
DEFAULT_OUTPUT_DIR = ROOT / "data" / "aquis_verification"
DEFAULT_CAPTURE_JSON = DEFAULT_OUTPUT_DIR / "companies_capture.json"
DEFAULT_REPORT_JSON = DEFAULT_OUTPUT_DIR / "isin_stock_sector_backfill.json"
DEFAULT_REPORT_CSV = DEFAULT_OUTPUT_DIR / "isin_stock_sector_backfill.csv"
DEFAULT_METADATA_UPDATES_CSV = ROOT / "data" / "review_overrides" / "metadata_updates.csv"
EQUITY_TYPES = frozenset({"Ordinary Shares", "B Ordinary Shares"})
NEXT_DATA_RE = re.compile(
    r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
    re.S,
)
REASON = (
    "Copied stock_sector from the official Aquis companies directory after an exact "
    "ISIN match on an ordinary share; Finance and Financial Conglomerates left unmapped."
)
REPORT_FIELDNAMES = [
    "ticker",
    "exchange",
    "asset_type",
    "name",
    "isin",
    "aquis_ticker",
    "aquis_name",
    "aquis_sector",
    "aquis_instrument_type",
    "aquis_status",
    "sector_update",
    "decision",
]


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def map_aquis_sector(sector: str) -> str:
    raw = " ".join((sector or "").split())
    if raw in {"Finance", "Financial Conglomerates"}:
        return ""
    return normalize_sector(raw, "Stock")


def parse_companies_page(html: str) -> list[dict[str, str]]:
    match = NEXT_DATA_RE.search(html or "")
    if not match:
        return []
    payload = json.loads(match.group(1))
    companies = (
        payload.get("props", {}).get("pageProps", {}).get("companies")
        if isinstance(payload, dict)
        else None
    )
    if not isinstance(companies, list):
        return []
    rows: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for record in companies:
        if not isinstance(record, dict):
            continue
        isin = str(record.get("isin") or "").strip().upper()
        if not is_valid_isin(isin):
            continue
        ticker = str(record.get("symbol") or "").strip()
        key = (ticker, isin)
        if key in seen:
            continue
        seen.add(key)
        rows.append(
            {
                "ticker": ticker,
                "name": str(record.get("name") or "").strip(),
                "isin": isin,
                "sector": str(record.get("sector") or "").strip(),
                "instrument_type": str(record.get("instrument_type") or "").strip(),
                "status": str(record.get("status") or "").strip(),
            }
        )
    return rows


def index_by_isin(rows: list[dict[str, str]]) -> dict[str, list[dict[str, str]]]:
    indexed: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        indexed[row["isin"]].append(row)
    return indexed


def load_missing_rows(path: Path = LISTINGS_CSV, *, exchanges: set[str] | None = None) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    selected: list[dict[str, str]] = []
    for row in rows:
        if row.get("asset_type") != "Stock":
            continue
        if exchanges is not None and row.get("exchange") not in exchanges:
            continue
        if (row.get("stock_sector") or row.get("sector") or "").strip():
            continue
        selected.append(row)
    return selected


def evaluate_row(row: dict[str, str], by_isin: dict[str, list[dict[str, str]]]) -> dict[str, Any]:
    isin = (row.get("isin") or "").strip().upper()
    base = {
        "ticker": row.get("ticker", ""),
        "exchange": row.get("exchange", ""),
        "asset_type": row.get("asset_type", ""),
        "name": row.get("name", ""),
        "isin": isin,
        "aquis_ticker": "",
        "aquis_name": "",
        "aquis_sector": "",
        "aquis_instrument_type": "",
        "aquis_status": "",
        "sector_update": "",
    }
    if (row.get("stock_sector") or row.get("sector") or "").strip():
        return {**base, "decision": "already_has_sector"}
    if not is_valid_isin(isin):
        return {**base, "decision": "missing_isin"}
    matches = [item for item in by_isin.get(isin, []) if item.get("instrument_type") in EQUITY_TYPES]
    if not matches:
        if by_isin.get(isin):
            return {**base, "decision": "unsupported_instrument_type"}
        return {**base, "decision": "no_aquis_match"}
    sectors: set[str] = set()
    chosen: dict[str, str] | None = None
    unsupported = False
    for item in matches:
        sector = map_aquis_sector(item.get("sector") or "")
        if not (item.get("sector") or "").strip():
            continue
        if not sector:
            unsupported = True
            continue
        sectors.add(sector)
        if chosen is None:
            chosen = item
    if len(sectors) == 1 and chosen is not None:
        return {
            **base,
            "aquis_ticker": chosen.get("ticker", ""),
            "aquis_name": chosen.get("name", ""),
            "aquis_sector": chosen.get("sector", ""),
            "aquis_instrument_type": chosen.get("instrument_type", ""),
            "aquis_status": chosen.get("status", ""),
            "sector_update": next(iter(sectors)),
            "decision": "accept",
        }
    if unsupported:
        return {**base, "decision": "unsupported_sector"}
    if not sectors:
        return {**base, "decision": "empty_sector"}
    return {**base, "decision": "ambiguous_sector"}


def build_metadata_updates(results: list[dict[str, Any]]) -> list[dict[str, str]]:
    return [
        {
            "ticker": result["ticker"],
            "exchange": result["exchange"],
            "field": "stock_sector",
            "decision": "update",
            "proposed_value": result["sector_update"],
            "confidence": "0.90",
            "reason": REASON + f" Source: {AQUIS_COMPANIES_URL}",
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


def fetch_companies_page(*, timeout_seconds: float = 30.0) -> str:
    response = requests.get(
        AQUIS_COMPANIES_URL,
        headers={"User-Agent": "Mozilla/5.0 free-ticker-database/3.0"},
        timeout=timeout_seconds,
    )
    response.raise_for_status()
    return response.text


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill missing stock sectors from the official Aquis companies directory after an exact ISIN match."
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
    exchanges = set(args.exchange) if args.exchange else {"FSX"}
    if args.refresh or not args.capture_json.exists():
        companies = parse_companies_page(fetch_companies_page())
    else:
        payload = json.loads(args.capture_json.read_text(encoding="utf-8"))
        companies = payload if isinstance(payload, list) else []
    args.capture_json.parent.mkdir(parents=True, exist_ok=True)
    args.capture_json.write_text(json.dumps(companies, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    results = [evaluate_row(row, index_by_isin(companies)) for row in load_missing_rows(args.listings_csv, exchanges=exchanges)]
    updates = build_metadata_updates(results)
    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.json_out.write_text(
        json.dumps([result for result in results if result["decision"] == "accept"], indent=2, sort_keys=True),
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
                "capture_rows": len(companies),
                "csv_out": display_path(args.csv_out),
                "decision_counts": dict(Counter(result["decision"] for result in results)),
                "json_out": display_path(args.json_out),
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()

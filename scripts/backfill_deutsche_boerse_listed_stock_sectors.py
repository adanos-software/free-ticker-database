from __future__ import annotations

import argparse
import csv
import io
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.fetch_exchange_masterfiles import DEUTSCHE_BOERSE_LISTED_URL, DEUTSCHE_BOERSE_SHEETS
from scripts.lib.dataio import merge_metadata_updates
from scripts.rebuild_dataset import LISTINGS_CSV, is_valid_isin, normalize_sector

DEFAULT_OUTPUT_DIR = ROOT / "data" / "deutsche_boerse_verification"
DEFAULT_CAPTURE_JSON = DEFAULT_OUTPUT_DIR / "listed_companies_capture.json"
DEFAULT_REPORT_JSON = DEFAULT_OUTPUT_DIR / "listed_stock_sector_backfill.json"
DEFAULT_REPORT_CSV = DEFAULT_OUTPUT_DIR / "listed_stock_sector_backfill.csv"
DEFAULT_METADATA_UPDATES_CSV = ROOT / "data" / "review_overrides" / "metadata_updates.csv"
ALLOWED_EXCHANGES = frozenset({"XETRA", "FSX"})
SECTOR_MAP = {
    "industrial": "Industrials",
    "software": "Information Technology",
    "pharma & healthcare": "Health Care",
    "automobile": "Consumer Discretionary",
    "technology": "Information Technology",
    "media": "Communication Services",
    "chemicals": "Materials",
    "food & beverages": "Consumer Staples",
    "transportation & logistics": "Industrials",
    "utilities": "Utilities",
    "telecommunication": "Communication Services",
    "banks": "Financials",
    "insurance": "Financials",
    "construction": "Industrials",
    "basic resources": "Materials",
}
SUBSECTOR_MAP = {
    "real estate": "Real Estate",
    "diversified financial": "Financials",
    "private equity & venture capital": "Financials",
    "credit banks": "Financials",
    "securities brokers": "Financials",
    "clothing & footwear": "Consumer Discretionary",
    "personal products": "Consumer Staples",
    "leisure": "Consumer Discretionary",
    "leisure, goods & services": "Consumer Discretionary",
    "leisure goods & services": "Consumer Discretionary",
    "home construction & furnishings": "Consumer Discretionary",
    "household appliances & housewares": "Consumer Discretionary",
    "consumer electronics": "Consumer Discretionary",
    "retail, internet": "Consumer Discretionary",
    "retail,internet": "Consumer Discretionary",
    "retail, specialty": "Consumer Discretionary",
    "retail, food & drug": "Consumer Staples",
}
REASON = (
    "Copied stock_sector from the official Deutsche Boerse listed-companies workbook after an "
    "exact ticker and ISIN match on XETRA or FSX; Consumer, Retail, and Financial Services mapped "
    "only through unique subsectors; empty and dash sectors left unmapped."
)
REPORT_FIELDNAMES = [
    "ticker",
    "exchange",
    "asset_type",
    "name",
    "isin",
    "official_ticker",
    "official_name",
    "official_isin",
    "official_sector",
    "official_subsector",
    "official_instrument_exchange",
    "sector_update",
    "decision",
]


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def clean_cell(value: Any) -> str:
    text = " ".join(str(value or "").split())
    return "" if text.lower() == "nan" else text


def map_deutsche_boerse_sector(sector: str, subsector: str = "") -> str:
    sector_key = " ".join((sector or "").split()).casefold()
    if sector_key in {"", "-"}:
        return ""
    mapped = SECTOR_MAP.get(sector_key, "")
    if mapped:
        return normalize_sector(mapped, "Stock")
    sub_key = " ".join((subsector or "").split()).casefold()
    mapped = SUBSECTOR_MAP.get(sub_key, "")
    return normalize_sector(mapped, "Stock") if mapped else ""


def venue_matches(exchange: str, instrument_exchange: str) -> bool:
    text = (instrument_exchange or "").upper()
    if exchange == "FSX":
        return "FRANKFURT" in text
    if exchange == "XETRA":
        return "XETRA" in text or "FRANKFURT" in text
    return False


def parse_listed_companies_excel(content: bytes) -> list[dict[str, str]]:
    workbook = pd.ExcelFile(io.BytesIO(content))
    rows: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for sheet_name in DEUTSCHE_BOERSE_SHEETS:
        if sheet_name not in workbook.sheet_names:
            continue
        dataframe = pd.read_excel(io.BytesIO(content), sheet_name=sheet_name, header=7)
        for record in dataframe.to_dict(orient="records"):
            ticker = clean_cell(record.get("Trading Symbol")).upper()
            name = clean_cell(record.get("Company"))
            isin = clean_cell(record.get("ISIN")).upper()
            if not ticker or not name or not is_valid_isin(isin):
                continue
            key = (ticker, isin)
            if key in seen:
                continue
            seen.add(key)
            rows.append(
                {
                    "ticker": ticker,
                    "name": name,
                    "isin": isin,
                    "sector": clean_cell(record.get("Sector")),
                    "subsector": clean_cell(record.get("Subsector")),
                    "instrument_exchange": clean_cell(record.get("Instrument Exchange")),
                    "sheet": sheet_name,
                }
            )
    return rows


def download_listed_companies_excel(url: str = DEUTSCHE_BOERSE_LISTED_URL, *, timeout_seconds: float = 60.0) -> bytes:
    response = requests.get(url, headers={"User-Agent": "Mozilla/5.0 free-ticker-database/3.0"}, timeout=timeout_seconds)
    response.raise_for_status()
    return response.content


def index_by_ticker(rows: list[dict[str, str]]) -> dict[str, list[dict[str, str]]]:
    indexed: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        indexed[row["ticker"]].append(row)
    return indexed


def load_missing_rows(path: Path = LISTINGS_CSV, *, exchanges: set[str] | None = None) -> list[dict[str, str]]:
    selected_exchanges = exchanges or set(ALLOWED_EXCHANGES)
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


def evaluate_row(row: dict[str, str], by_ticker: dict[str, list[dict[str, str]]]) -> dict[str, Any]:
    isin = (row.get("isin") or "").strip().upper()
    ticker = (row.get("ticker") or "").strip().upper()
    exchange = (row.get("exchange") or "").strip()
    officials = by_ticker.get(ticker) or []
    matching = [item for item in officials if item.get("isin") == isin]
    official = matching[0] if matching else (officials[0] if officials else {})
    base = {
        "ticker": row.get("ticker", ""),
        "exchange": exchange,
        "asset_type": row.get("asset_type", ""),
        "name": row.get("name", ""),
        "isin": isin,
        "official_ticker": official.get("ticker", ""),
        "official_name": official.get("name", ""),
        "official_isin": official.get("isin", ""),
        "official_sector": official.get("sector", ""),
        "official_subsector": official.get("subsector", ""),
        "official_instrument_exchange": official.get("instrument_exchange", ""),
        "sector_update": "",
    }
    if (row.get("stock_sector") or row.get("sector") or "").strip():
        return {**base, "decision": "already_has_sector"}
    if exchange not in ALLOWED_EXCHANGES:
        return {**base, "decision": "not_deutsche_boerse_exchange"}
    if not is_valid_isin(isin):
        return {**base, "decision": "missing_or_invalid_isin"}
    if not officials:
        return {**base, "decision": "no_official_match"}
    if not matching:
        return {**base, "decision": "isin_mismatch"}
    venue_ok = [item for item in matching if venue_matches(exchange, item.get("instrument_exchange", ""))]
    if not venue_ok:
        return {**base, "decision": "venue_mismatch"}
    mapped = {map_deutsche_boerse_sector(item.get("sector", ""), item.get("subsector", "")) for item in venue_ok}
    mapped.discard("")
    if len(mapped) == 1:
        chosen = venue_ok[0]
        return {
            **base,
            "official_ticker": chosen.get("ticker", ""),
            "official_name": chosen.get("name", ""),
            "official_isin": chosen.get("isin", ""),
            "official_sector": chosen.get("sector", ""),
            "official_subsector": chosen.get("subsector", ""),
            "official_instrument_exchange": chosen.get("instrument_exchange", ""),
            "sector_update": next(iter(mapped)),
            "decision": "accept",
        }
    if any(item.get("sector", "").strip() for item in venue_ok):
        return {**base, "decision": "unsupported_sector"}
    return {**base, "decision": "empty_sector"}


def build_metadata_updates(results: list[dict[str, Any]]) -> list[dict[str, str]]:
    return [
        {
            "ticker": result["ticker"],
            "exchange": result["exchange"],
            "field": "stock_sector",
            "decision": "update",
            "proposed_value": result["sector_update"],
            "confidence": "0.90",
            "reason": REASON + f" Source: {DEUTSCHE_BOERSE_LISTED_URL}",
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
        description="Backfill missing XETRA/FSX stock sectors from the official Deutsche Boerse listed-companies workbook."
    )
    parser.add_argument("--listings-csv", type=Path, default=LISTINGS_CSV)
    parser.add_argument("--xlsx-path", type=Path)
    parser.add_argument("--source-url", default=DEUTSCHE_BOERSE_LISTED_URL)
    parser.add_argument("--capture-json", type=Path, default=DEFAULT_CAPTURE_JSON)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_REPORT_JSON)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_REPORT_CSV)
    parser.add_argument("--metadata-updates-csv", type=Path, default=DEFAULT_METADATA_UPDATES_CSV)
    parser.add_argument("--exchange", action="append")
    parser.add_argument("--timeout-seconds", type=float, default=60.0)
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    exchanges = set(args.exchange) if args.exchange else set(ALLOWED_EXCHANGES)
    if args.xlsx_path:
        official_rows = parse_listed_companies_excel(args.xlsx_path.read_bytes())
        args.capture_json.parent.mkdir(parents=True, exist_ok=True)
        args.capture_json.write_text(json.dumps(official_rows, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    elif args.refresh or not args.capture_json.exists():
        official_rows = parse_listed_companies_excel(
            download_listed_companies_excel(args.source_url, timeout_seconds=args.timeout_seconds)
        )
        args.capture_json.parent.mkdir(parents=True, exist_ok=True)
        args.capture_json.write_text(json.dumps(official_rows, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    else:
        official_rows = json.loads(args.capture_json.read_text(encoding="utf-8"))
    by_ticker = index_by_ticker(official_rows)
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
                "json_out": display_path(args.json_out),
                "official_rows": len(official_rows),
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()

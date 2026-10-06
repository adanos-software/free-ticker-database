from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import Counter, defaultdict
from io import BytesIO
from pathlib import Path
from typing import Any

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.lib.dataio import merge_metadata_updates
from scripts.rebuild_dataset import LISTINGS_CSV, is_valid_isin, normalize_sector

BALTIC_SHARES_URL = "https://nasdaqbaltic.com/statistics/en/shares?download=1"
BALTIC_INSTRUMENT_URL = "https://nasdaqbaltic.com/statistics/en/instrument/{isin}/trading"
DEFAULT_OUTPUT_DIR = ROOT / "data" / "nasdaq_baltic_verification"
DEFAULT_SHARES_XLSX = DEFAULT_OUTPUT_DIR / "shares.xlsx"
DEFAULT_SHARES_JSON = DEFAULT_OUTPUT_DIR / "shares.json"
DEFAULT_PAGES_JSON = DEFAULT_OUTPUT_DIR / "instrument_pages.json"
DEFAULT_REPORT_JSON = DEFAULT_OUTPUT_DIR / "isin_stock_sector_backfill.json"
DEFAULT_REPORT_CSV = DEFAULT_OUTPUT_DIR / "isin_stock_sector_backfill.csv"
DEFAULT_METADATA_UPDATES_CSV = ROOT / "data" / "review_overrides" / "metadata_updates.csv"
BALTIC_PREFIXES = frozenset({"EE", "LT", "LV"})
BALTIC_INDUSTRY_MAP = {
    "Telecommunications": "Communication Services",
}
PAGE_ISIN_RE = re.compile(r"<strong>ISIN\s+([A-Z]{2}[A-Z0-9]{9}[0-9])</strong>", re.I)
PAGE_INDUSTRY_RE = re.compile(r'<p class="text-grey">([^<]+)</p>', re.I)
PAGE_TICKER_RE = re.compile(
    r"<strong>([A-Z0-9]{2,10})</strong>\s*<span class=\"text-muted text-thin\">&nbsp;\|&nbsp;</span>\s*<strong>ISIN",
    re.I,
)
REASON = (
    "Copied stock_sector from the official Nasdaq Baltic shares workbook or instrument "
    "page after an exact ISIN match; Telecommunications mapped locally to Communication "
    "Services; unique GICS required."
)
REPORT_FIELDNAMES = [
    "ticker",
    "exchange",
    "asset_type",
    "name",
    "isin",
    "baltic_ticker",
    "baltic_name",
    "baltic_industry",
    "sector_update",
    "decision",
]


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def map_baltic_industry(industry: str) -> str:
    raw = (industry or "").strip()
    if not raw:
        return ""
    return normalize_sector(BALTIC_INDUSTRY_MAP.get(raw, raw), "Stock")


def parse_instrument_page(html: str) -> dict[str, str]:
    isin_match = PAGE_ISIN_RE.search(html or "")
    industry_match = PAGE_INDUSTRY_RE.search(html or "")
    ticker_match = PAGE_TICKER_RE.search(html or "")
    industry = " ".join((industry_match.group(1) if industry_match else "").replace("&gt;", ">").split())
    industry = industry.split(">")[0].strip()
    return {
        "ticker": (ticker_match.group(1) if ticker_match else "").strip(),
        "name": "",
        "isin": (isin_match.group(1) if isin_match else "").strip().upper(),
        "industry": industry,
    }


def fetch_instrument_page(isin: str, *, timeout_seconds: float = 30.0) -> str:
    response = requests.get(
        BALTIC_INSTRUMENT_URL.format(isin=isin),
        headers={"User-Agent": "Mozilla/5.0 free-ticker-database/3.0"},
        timeout=timeout_seconds,
    )
    response.raise_for_status()
    return response.text


def parse_share_records(records: list[dict[str, Any]]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for record in records:
        isin = str(record.get("ISIN") or record.get("isin") or "").strip().upper()
        if not is_valid_isin(isin):
            continue
        ticker = str(record.get("Ticker") or record.get("ticker") or "").strip()
        key = (ticker, isin)
        if key in seen:
            continue
        seen.add(key)
        rows.append(
            {
                "ticker": ticker,
                "name": str(record.get("Name") or record.get("name") or "").strip(),
                "isin": isin,
                "industry": str(record.get("Industry") or record.get("industry") or "").strip(),
            }
        )
    return rows


def parse_shares_xlsx(payload: bytes | Path) -> list[dict[str, str]]:
    if isinstance(payload, Path):
        data = payload.read_bytes()
    else:
        data = payload
    dataframe = pd.read_excel(BytesIO(data), sheet_name="Shares", dtype=str).fillna("")
    return parse_share_records(dataframe.to_dict("records"))


def download_shares_xlsx(url: str = BALTIC_SHARES_URL, *, timeout_seconds: float = 60.0) -> bytes:
    response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=timeout_seconds)
    response.raise_for_status()
    return response.content


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
        "baltic_ticker": "",
        "baltic_name": "",
        "baltic_industry": "",
        "sector_update": "",
    }
    if (row.get("stock_sector") or row.get("sector") or "").strip():
        return {**base, "decision": "already_has_sector"}
    if not is_valid_isin(isin):
        return {**base, "decision": "missing_isin"}
    matches = by_isin.get(isin, [])
    if not matches:
        return {**base, "decision": "no_baltic_match"}
    sectors: set[str] = set()
    chosen: dict[str, str] | None = None
    unsupported = False
    for item in matches:
        sector = map_baltic_industry(item.get("industry") or "")
        if not (item.get("industry") or "").strip():
            continue
        if not sector:
            unsupported = True
            continue
        sectors.add(sector)
        if chosen is None:
            chosen = item
    if len(sectors) == 1 and chosen is not None:
        sector = next(iter(sectors))
        return {
            **base,
            "baltic_ticker": chosen.get("ticker", ""),
            "baltic_name": chosen.get("name", ""),
            "baltic_industry": chosen.get("industry", ""),
            "sector_update": sector,
            "decision": "accept",
        }
    if unsupported:
        return {**base, "decision": "unsupported_industry"}
    if not sectors:
        return {**base, "decision": "empty_industry"}
    return {**base, "decision": "ambiguous_industry"}


def verify_rows(rows: list[dict[str, str]], share_rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    indexed = index_by_isin(share_rows)
    return [evaluate_row(row, indexed) for row in rows]


def build_metadata_updates(results: list[dict[str, Any]]) -> list[dict[str, str]]:
    return [
        {
            "ticker": result["ticker"],
            "exchange": result["exchange"],
            "field": "stock_sector",
            "decision": "update",
            "proposed_value": result["sector_update"],
            "confidence": "0.88",
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
        description="Backfill missing stock sectors from the official Nasdaq Baltic shares workbook by exact ISIN."
    )
    parser.add_argument("--listings-csv", type=Path, default=LISTINGS_CSV)
    parser.add_argument("--shares-xlsx", type=Path, default=DEFAULT_SHARES_XLSX)
    parser.add_argument("--shares-json", type=Path, default=DEFAULT_SHARES_JSON)
    parser.add_argument("--pages-json", type=Path, default=DEFAULT_PAGES_JSON)
    parser.add_argument("--shares-url", default=BALTIC_SHARES_URL)
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
    if args.refresh or not args.shares_xlsx.exists():
        payload = download_shares_xlsx(args.shares_url)
        args.shares_xlsx.parent.mkdir(parents=True, exist_ok=True)
        args.shares_xlsx.write_bytes(payload)
    share_rows = parse_shares_xlsx(args.shares_xlsx)
    args.shares_json.parent.mkdir(parents=True, exist_ok=True)
    args.shares_json.write_text(json.dumps(share_rows, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    missing_rows = load_missing_rows(args.listings_csv, exchanges=exchanges)
    indexed = index_by_isin(share_rows)
    page_rows: list[dict[str, str]] = []
    if args.pages_json.exists() and not args.refresh:
        payload = json.loads(args.pages_json.read_text(encoding="utf-8"))
        page_rows = payload if isinstance(payload, list) else []
    else:
        for row in missing_rows:
            isin = (row.get("isin") or "").strip().upper()
            if not is_valid_isin(isin) or isin[:2] not in BALTIC_PREFIXES or isin in indexed:
                continue
            try:
                parsed = parse_instrument_page(fetch_instrument_page(isin))
            except (requests.RequestException, ValueError):
                continue
            if parsed.get("isin") != isin:
                continue
            page_rows.append(parsed)
        args.pages_json.parent.mkdir(parents=True, exist_ok=True)
        args.pages_json.write_text(json.dumps(page_rows, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    for parsed in page_rows:
        if parsed.get("isin") and parsed.get("industry"):
            share_rows.append(
                {
                    "ticker": parsed.get("ticker", ""),
                    "name": parsed.get("name", ""),
                    "isin": parsed["isin"],
                    "industry": parsed["industry"],
                }
            )
    results = verify_rows(missing_rows, share_rows)
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
                "csv_out": display_path(args.csv_out),
                "decision_counts": dict(Counter(result["decision"] for result in results)),
                "json_out": display_path(args.json_out),
                "share_rows": len(share_rows),
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()

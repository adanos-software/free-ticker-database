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

SPOTLIGHT_COMPANIES_URL = "https://spotlightstockmarket.com/Umbraco/api/companyapi/GetCompanies"
SPOTLIGHT_COMPANIES_PAGE_URL = "https://spotlightstockmarket.com/en/market-overview/our-companies/"
DEFAULT_OUTPUT_DIR = ROOT / "data" / "spotlight_verification"
DEFAULT_CAPTURE_JSON = DEFAULT_OUTPUT_DIR / "isin_industry_capture.json"
DEFAULT_REPORT_JSON = DEFAULT_OUTPUT_DIR / "isin_stock_sector_backfill.json"
DEFAULT_REPORT_CSV = DEFAULT_OUTPUT_DIR / "isin_stock_sector_backfill.csv"
DEFAULT_METADATA_UPDATES_CSV = ROOT / "data" / "review_overrides" / "metadata_updates.csv"
INDUSTRY_MAP = {
    "Basic Materials": "Materials",
    "Technology": "Information Technology",
}
ISIN_LABEL_RE = re.compile(
    r"ISIN(?:-Kod)?</span>\s*<span class=\"sl-ii__value\">\s*([A-Z]{2}[A-Z0-9]{9}[0-9])",
    re.I,
)
REASON = (
    "Copied stock_sector from the official Spotlight companies API after an exact labeled "
    "ISIN match on the instrument trade page; Consumer Goods & Services, Finance, and "
    "Financial Conglomerates left unmapped."
)
REPORT_FIELDNAMES = [
    "ticker",
    "exchange",
    "asset_type",
    "name",
    "isin",
    "spotlight_name",
    "spotlight_industry",
    "spotlight_instrument_id",
    "sector_update",
    "decision",
]


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def map_spotlight_industry(industry: str) -> str:
    raw = " ".join((industry or "").split())
    if raw in {"Finance", "Financial Conglomerates", "Consumer Goods & Services"}:
        return ""
    return normalize_sector(INDUSTRY_MAP.get(raw, raw), "Stock")


def parse_trade_isin(html: str) -> str:
    match = ISIN_LABEL_RE.search(html or "")
    isin = (match.group(1) if match else "").strip().upper()
    return isin if is_valid_isin(isin) else ""


def instrument_id_from_url(url: str) -> str:
    match = re.search(r"InstrumentId=([^&]+)", url or "")
    return (match.group(1) if match else "").strip()


def trade_url_from_detail_url(url: str) -> str:
    return (url or "").replace("irabout", "irtrade", 1)


def parse_company_records(records: list[dict[str, Any]]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for record in records:
        if not isinstance(record, dict):
            continue
        detail_url = str(record.get("url") or "").strip()
        if detail_url.startswith("/"):
            detail_url = "https://spotlightstockmarket.com" + detail_url
        instrument_id = instrument_id_from_url(detail_url)
        if not instrument_id:
            continue
        rows.append(
            {
                "name": str(record.get("heading") or record.get("companyName") or "").strip(),
                "industry": str(record.get("industry") or "").strip(),
                "instrument_id": instrument_id,
                "detail_url": detail_url,
                "trade_url": trade_url_from_detail_url(detail_url),
            }
        )
    return rows


def index_by_isin(rows: list[dict[str, str]]) -> dict[str, list[dict[str, str]]]:
    indexed: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        isin = (row.get("isin") or "").strip().upper()
        if is_valid_isin(isin):
            indexed[isin].append(row)
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
        "spotlight_name": "",
        "spotlight_industry": "",
        "spotlight_instrument_id": "",
        "sector_update": "",
    }
    if (row.get("stock_sector") or row.get("sector") or "").strip():
        return {**base, "decision": "already_has_sector"}
    if not is_valid_isin(isin):
        return {**base, "decision": "missing_isin"}
    matches = by_isin.get(isin, [])
    if not matches:
        return {**base, "decision": "no_spotlight_match"}
    sectors: set[str] = set()
    chosen: dict[str, str] | None = None
    unsupported = False
    for item in matches:
        sector = map_spotlight_industry(item.get("industry") or "")
        if not (item.get("industry") or "").strip():
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
            "spotlight_name": chosen.get("name", ""),
            "spotlight_industry": chosen.get("industry", ""),
            "spotlight_instrument_id": chosen.get("instrument_id", ""),
            "sector_update": next(iter(sectors)),
            "decision": "accept",
        }
    if unsupported:
        return {**base, "decision": "unsupported_industry"}
    if not sectors:
        return {**base, "decision": "empty_industry"}
    return {**base, "decision": "ambiguous_industry"}


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


def spotlight_headers() -> dict[str, str]:
    return {
        "User-Agent": "Mozilla/5.0 free-ticker-database/3.0",
        "Referer": SPOTLIGHT_COMPANIES_PAGE_URL,
    }


def fetch_company_records(*, timeout_seconds: float = 30.0) -> list[dict[str, Any]]:
    response = requests.get(SPOTLIGHT_COMPANIES_URL, headers=spotlight_headers(), timeout=timeout_seconds)
    response.raise_for_status()
    payload = response.json()
    records = payload.get("results") if isinstance(payload, dict) else payload
    if not isinstance(records, list):
        raise ValueError("Unexpected Spotlight GetCompanies payload")
    return records


def fetch_trade_page(url: str, *, timeout_seconds: float = 30.0) -> str:
    response = requests.get(url, headers=spotlight_headers(), timeout=timeout_seconds)
    response.raise_for_status()
    return response.text


def capture_companies(records: list[dict[str, Any]], *, fetch_html) -> list[dict[str, str]]:
    captured: list[dict[str, str]] = []
    for row in parse_company_records(records):
        try:
            isin = parse_trade_isin(fetch_html(row["trade_url"]))
        except (requests.RequestException, ValueError):
            isin = ""
        captured.append({**row, "isin": isin})
    return captured


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill missing stock sectors from official Spotlight industry labels after an exact labeled ISIN match."
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
        captured = capture_companies(fetch_company_records(), fetch_html=fetch_trade_page)
    else:
        payload = json.loads(args.capture_json.read_text(encoding="utf-8"))
        captured = payload if isinstance(payload, list) else []
    args.capture_json.parent.mkdir(parents=True, exist_ok=True)
    args.capture_json.write_text(json.dumps(captured, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    results = [evaluate_row(row, index_by_isin(captured)) for row in load_missing_rows(args.listings_csv, exchanges=exchanges)]
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
                "capture_rows": len(captured),
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

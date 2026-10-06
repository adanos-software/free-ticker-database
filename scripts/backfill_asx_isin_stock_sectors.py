from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter, defaultdict
from io import StringIO
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.backfill_asx_isins import AsxIsinRow, asx_cell
from scripts.fetch_exchange_masterfiles import ASX_GICS_INDUSTRY_GROUP_SECTOR_MAP
from scripts.lib.dataio import merge_metadata_updates
from scripts.rebuild_dataset import LISTINGS_CSV, is_valid_isin, normalize_sector

DEFAULT_LISTED_CSV = ROOT / "data" / "asx_listed_verification" / "ASXListedCompanies.csv"
DEFAULT_ISIN_XLS = ROOT / "data" / "asx_listed_verification" / "ISIN.xls"
DEFAULT_LISTED_URL = "https://www.asx.com.au/asx/research/ASXListedCompanies.csv"
DEFAULT_ISIN_URL = "https://www.asx.com.au/content/dam/asx/issuers/ISIN.xls"
DEFAULT_OUTPUT_DIR = ROOT / "data" / "asx_listed_verification"
DEFAULT_REPORT_JSON = DEFAULT_OUTPUT_DIR / "isin_stock_sector_backfill.json"
DEFAULT_REPORT_CSV = DEFAULT_OUTPUT_DIR / "isin_stock_sector_backfill.csv"
DEFAULT_METADATA_UPDATES_CSV = ROOT / "data" / "review_overrides" / "metadata_updates.csv"
NON_EQUITY_TOKENS = ("OPTION", "WARRANT", "RIGHT", "NOTE", "BOND", "PREF", "CONVERT")
REASON = (
    "Copied stock_sector from the official ASX listed-companies GICS industry group after an "
    "exact ISIN match to the official ASX ISIN workbook equity line; unique GICS required."
)
REPORT_FIELDNAMES = [
    "ticker",
    "exchange",
    "asset_type",
    "name",
    "isin",
    "asx_ticker",
    "asx_name",
    "asx_security_type",
    "asx_gics_group",
    "sector_update",
    "decision",
]


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def asx_security_is_equity(security_type: str) -> bool:
    value = (security_type or "").upper()
    if not value or any(token in value for token in NON_EQUITY_TOKENS):
        return False
    return "ORDINARY" in value or "STAPLED" in value


def parse_asx_listed_sector_rows(text: str) -> dict[str, dict[str, str]]:
    lines = text.splitlines()
    if lines and lines[0].startswith("ASX listed companies as at"):
        lines = lines[2:]
    indexed: dict[str, dict[str, str]] = {}
    for record in csv.DictReader(StringIO("\n".join(lines))):
        ticker = (record.get("ASX code") or "").strip().upper()
        name = (record.get("Company name") or "").strip()
        group = (record.get("GICS industry group") or "").strip()
        if not ticker or not name:
            continue
        sector = normalize_sector(ASX_GICS_INDUSTRY_GROUP_SECTOR_MAP.get(group, ""), "Stock")
        indexed[ticker] = {"ticker": ticker, "name": name, "gics_group": group, "sector": sector}
    return indexed


def parse_asx_isin_equity_rows(xls_bytes: bytes) -> list[AsxIsinRow]:
    import pandas as pd
    from io import BytesIO

    dataframe = pd.read_excel(BytesIO(xls_bytes), sheet_name="ISIN", dtype=str).fillna("")
    rows: list[AsxIsinRow] = []
    seen: set[tuple[str, str]] = set()
    for record in dataframe.to_dict("records"):
        ticker = asx_cell(record, "ASX code").upper()
        name = asx_cell(record, "Company name")
        security_type = asx_cell(record, "Security type")
        isin = asx_cell(record, "ISIN code", "ISIN").upper()
        if not ticker or not name or not asx_security_is_equity(security_type) or not is_valid_isin(isin):
            continue
        key = (ticker, isin)
        if key in seen:
            continue
        seen.add(key)
        rows.append(AsxIsinRow(ticker=ticker, name=name, security_type=security_type, isin=isin))
    return rows


def index_by_isin(rows: list[AsxIsinRow]) -> dict[str, list[AsxIsinRow]]:
    indexed: dict[str, list[AsxIsinRow]] = defaultdict(list)
    for row in rows:
        indexed[row.isin].append(row)
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


def evaluate_row(
    row: dict[str, str],
    by_isin: dict[str, list[AsxIsinRow]],
    listed_by_code: dict[str, dict[str, str]],
) -> dict[str, Any]:
    isin = (row.get("isin") or "").strip().upper()
    base = {
        "ticker": row.get("ticker", ""),
        "exchange": row.get("exchange", ""),
        "asset_type": row.get("asset_type", ""),
        "name": row.get("name", ""),
        "isin": isin,
        "asx_ticker": "",
        "asx_name": "",
        "asx_security_type": "",
        "asx_gics_group": "",
        "sector_update": "",
    }
    if (row.get("stock_sector") or row.get("sector") or "").strip():
        return {**base, "decision": "already_has_sector"}
    if not is_valid_isin(isin):
        return {**base, "decision": "missing_isin"}
    matches = by_isin.get(isin, [])
    if not matches:
        return {**base, "decision": "no_asx_isin_match"}
    mapped: list[tuple[AsxIsinRow, dict[str, str]]] = []
    missing_listed = False
    empty_sector = False
    for item in matches:
        listed = listed_by_code.get(item.ticker)
        if listed is None:
            missing_listed = True
            continue
        if not listed.get("sector"):
            empty_sector = True
            continue
        mapped.append((item, listed))
    sectors = {listed["sector"] for _, listed in mapped}
    if len(sectors) == 1:
        item, listed = mapped[0]
        return {
            **base,
            "asx_ticker": item.ticker,
            "asx_name": listed.get("name") or item.name,
            "asx_security_type": item.security_type,
            "asx_gics_group": listed.get("gics_group", ""),
            "sector_update": next(iter(sectors)),
            "decision": "accept",
        }
    if len(sectors) > 1:
        return {**base, "decision": "ambiguous_asx_sector"}
    if empty_sector:
        return {**base, "decision": "empty_asx_gics"}
    if missing_listed:
        return {**base, "decision": "asx_code_not_listed"}
    return {**base, "decision": "no_asx_isin_match"}


def verify_rows(
    rows: list[dict[str, str]],
    isin_rows: list[AsxIsinRow],
    listed_by_code: dict[str, dict[str, str]],
) -> list[dict[str, Any]]:
    indexed = index_by_isin(isin_rows)
    return [evaluate_row(row, indexed, listed_by_code) for row in rows]


def build_metadata_updates(results: list[dict[str, Any]]) -> list[dict[str, str]]:
    return [
        {
            "ticker": result["ticker"],
            "exchange": result["exchange"],
            "field": "stock_sector",
            "decision": "update",
            "proposed_value": result["sector_update"],
            "confidence": "0.90",
            "reason": REASON + f" Source ASX code {result['asx_ticker']}.",
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


def download_bytes(url: str, *, timeout_seconds: float) -> bytes:
    import requests

    response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=timeout_seconds)
    response.raise_for_status()
    return response.content


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill missing stock sectors from official ASX listed-companies GICS after exact ISIN match."
    )
    parser.add_argument("--listings-csv", type=Path, default=LISTINGS_CSV)
    parser.add_argument("--listed-csv", type=Path, default=DEFAULT_LISTED_CSV)
    parser.add_argument("--listed-url", default=DEFAULT_LISTED_URL)
    parser.add_argument("--isin-xls", type=Path, default=DEFAULT_ISIN_XLS)
    parser.add_argument("--isin-url", default=DEFAULT_ISIN_URL)
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
    exchanges = set(args.exchange) if args.exchange else {"FSX"}
    if args.refresh or not args.listed_csv.exists():
        listed_bytes = download_bytes(args.listed_url, timeout_seconds=args.timeout_seconds)
        args.listed_csv.parent.mkdir(parents=True, exist_ok=True)
        args.listed_csv.write_bytes(listed_bytes)
    else:
        listed_bytes = args.listed_csv.read_bytes()
    if args.refresh or not args.isin_xls.exists():
        isin_bytes = download_bytes(args.isin_url, timeout_seconds=args.timeout_seconds)
        args.isin_xls.parent.mkdir(parents=True, exist_ok=True)
        args.isin_xls.write_bytes(isin_bytes)
    else:
        isin_bytes = args.isin_xls.read_bytes()
    listed_by_code = parse_asx_listed_sector_rows(listed_bytes.decode("utf-8", errors="replace"))
    isin_rows = parse_asx_isin_equity_rows(isin_bytes)
    results = verify_rows(load_missing_rows(args.listings_csv, exchanges=exchanges), isin_rows, listed_by_code)
    updates = build_metadata_updates(results)
    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.json_out.write_text(
        json.dumps([result for result in results if result["decision"] == "accept"], indent=2, sort_keys=True)
        + "\n",
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
                "isin_rows": len(isin_rows),
                "json_out": display_path(args.json_out),
                "listed_rows": len(listed_by_code),
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()

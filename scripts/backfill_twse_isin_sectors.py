from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import Counter
from dataclasses import dataclass
from io import BytesIO, StringIO
from pathlib import Path
from typing import Any

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.backfill_yahoo_generic_etf_names import merge_metadata_updates
from scripts.rebuild_dataset import TICKERS_CSV, is_valid_isin, normalize_sector


TWSE_ISIN_URL = "https://isin.twse.com.tw/isin/e_C_public.jsp?strMode=2"
DEFAULT_OUTPUT_DIR = ROOT / "data" / "twse_verification"
DEFAULT_REPORT_JSON = DEFAULT_OUTPUT_DIR / "isin_sector_backfill.json"
DEFAULT_REPORT_CSV = DEFAULT_OUTPUT_DIR / "isin_sector_backfill.csv"
DEFAULT_METADATA_UPDATES_CSV = ROOT / "data" / "review_overrides" / "metadata_updates.csv"

TWSE_INDUSTRY_SECTOR_MAP = {
    "Cement": "Materials",
    "Food": "Consumer Staples",
    "Plastic": "Materials",
    "Textile": "Consumer Discretionary",
    "Electric Machinery": "Industrials",
    "Electrical and Cable": "Industrials",
    "Glass and Ceramics": "Materials",
    "Paper and Pulp": "Materials",
    "Iron & Steel": "Materials",
    "Rubber": "Consumer Discretionary",
    "Automobile": "Consumer Discretionary",
    "Building Material and Construction": "Industrials",
    "Shipping and Transportation": "Industrials",
    "Tourism and Hospitality": "Consumer Discretionary",
    "Financial and Insurance": "Financials",
    "Trading & Consumers Goods": "Consumer Discretionary",
    "Chemical": "Materials",
    "Biotechnology and Medical Care": "Health Care",
    "Oil, Gas and Electricity": "Utilities",
    "Semiconductor Industry": "Information Technology",
    "Computer and Peripheral Equipm": "Information Technology",
    "Optoelectronic": "Information Technology",
    "Communications and Internet": "Communication Services",
    "Electronic Parts/Components": "Information Technology",
    "Electronic Products Distrib": "Information Technology",
    "Information Service": "Information Technology",
    "Other Electronic Industry": "Information Technology",
    "Cultural and Creative": "Communication Services",
    "Agricultural Technology": "Consumer Staples",
    "E-commerce": "Consumer Discretionary",
    "Green Energy and Environmental Services": "Industrials",
    "Digital Cloud": "Information Technology",
    "Sports and Leisure": "Consumer Discretionary",
    "Household": "Consumer Discretionary",
}

REPORT_FIELDNAMES = [
    "ticker",
    "exchange",
    "asset_type",
    "name",
    "twse_security_name",
    "twse_isin",
    "twse_market",
    "twse_industry",
    "isin_update",
    "sector_update",
    "decision",
]


@dataclass(frozen=True)
class TwseIsinRow:
    ticker: str
    security_name: str
    isin: str
    market: str
    industry: str


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def clean_cell(value: Any) -> str:
    text = str(value or "").strip()
    return "" if text.lower() == "nan" else text


def split_security_code_name(value: str) -> tuple[str, str]:
    text = clean_cell(value)
    if "¡@" in text:
        ticker, name = text.split("¡@", 1)
        return ticker.strip().upper(), name.strip()
    match = re.match(r"^([A-Z0-9]+)\s+(.+)$", text)
    if match:
        return match.group(1).strip().upper(), match.group(2).strip()
    return text.strip().upper(), ""


def parse_twse_isin_html(html: bytes | str) -> list[TwseIsinRow]:
    data = BytesIO(html) if isinstance(html, bytes) else StringIO(html)
    dataframe = pd.read_html(data)[0].fillna("")
    rows: list[TwseIsinRow] = []
    seen: set[str] = set()
    for record in dataframe.to_dict("records"):
        ticker, name = split_security_code_name(record.get(0, ""))
        isin = clean_cell(record.get(1, "")).upper()
        market = clean_cell(record.get(3, ""))
        industry = clean_cell(record.get(4, ""))
        if not ticker or ticker in seen or not is_valid_isin(isin):
            continue
        if ticker in {"SECURITY CODE & SECURITY NAME", "STOCKS"}:
            continue
        seen.add(ticker)
        rows.append(TwseIsinRow(ticker=ticker, security_name=name, isin=isin, market=market, industry=industry))
    return rows


def download_twse_isin_html(url: str, *, timeout_seconds: float) -> bytes:
    response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=timeout_seconds)
    response.raise_for_status()
    return response.content


def normalize_twse_industry_sector(industry: str) -> str:
    return normalize_sector(TWSE_INDUSTRY_SECTOR_MAP.get(industry.strip(), ""), "Stock")


def load_target_rows(path: Path = TICKERS_CSV) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return [
            row
            for row in csv.DictReader(handle)
            if row.get("exchange") == "TWSE"
            and (not row.get("isin", "").strip() or (row.get("asset_type") == "Stock" and not row.get("stock_sector", "").strip()))
        ]


def evaluate_row(target: dict[str, str], source: TwseIsinRow | None) -> dict[str, Any]:
    base = {
        "ticker": target.get("ticker", ""),
        "exchange": target.get("exchange", ""),
        "asset_type": target.get("asset_type", ""),
        "name": target.get("name", ""),
        "twse_security_name": "",
        "twse_isin": "",
        "twse_market": "",
        "twse_industry": "",
        "isin_update": "",
        "sector_update": "",
    }
    if source is None:
        return {**base, "decision": "no_twse_match"}
    base.update(
        {
            "twse_security_name": source.security_name,
            "twse_isin": source.isin,
            "twse_market": source.market,
            "twse_industry": source.industry,
        }
    )
    isin_update = source.isin if not target.get("isin", "").strip() else ""
    sector_update = ""
    if target.get("asset_type") == "Stock" and not target.get("stock_sector", "").strip():
        sector_update = normalize_twse_industry_sector(source.industry)
    if not isin_update and not sector_update:
        return {**base, "decision": "no_update"}
    return {**base, "isin_update": isin_update, "sector_update": sector_update, "decision": "accept"}


def verify_rows(targets: list[dict[str, str]], source_rows: list[TwseIsinRow]) -> list[dict[str, Any]]:
    by_ticker = {row.ticker: row for row in source_rows}
    return [evaluate_row(row, by_ticker.get(row.get("ticker", "").strip().upper())) for row in targets]


def build_metadata_updates(results: list[dict[str, Any]]) -> list[dict[str, str]]:
    updates: list[dict[str, str]] = []
    for result in results:
        if result.get("decision") != "accept":
            continue
        if result.get("isin_update"):
            updates.append(
                {
                    "ticker": result["ticker"],
                    "exchange": result["exchange"],
                    "field": "isin",
                    "decision": "update",
                    "proposed_value": result["isin_update"],
                    "confidence": "0.96",
                    "reason": (
                        "Official TWSE ISIN table supplied a valid ISIN for the exact security code; "
                        f"accepted after exact TWSE code and ISIN-checksum gates. Source: {TWSE_ISIN_URL}"
                    ),
                }
            )
        if result.get("sector_update"):
            updates.append(
                {
                    "ticker": result["ticker"],
                    "exchange": result["exchange"],
                    "field": "stock_sector",
                    "decision": "update",
                    "proposed_value": result["sector_update"],
                    "confidence": "0.88",
                    "reason": (
                        "Official TWSE ISIN table supplied an industrial group for the exact security code; "
                        f"mapped to canonical stock_sector. Source: {TWSE_ISIN_URL}"
                    ),
                }
            )
    return updates


def write_report_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=REPORT_FIELDNAMES)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in REPORT_FIELDNAMES})


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill TWSE ISINs and stock sectors from the official TWSE ISIN table.")
    parser.add_argument("--source-url", default=TWSE_ISIN_URL)
    parser.add_argument("--html-path", type=Path)
    parser.add_argument("--tickers-csv", type=Path, default=TICKERS_CSV)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_REPORT_JSON)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_REPORT_CSV)
    parser.add_argument("--metadata-updates-csv", type=Path, default=DEFAULT_METADATA_UPDATES_CSV)
    parser.add_argument("--timeout-seconds", type=float, default=30.0)
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    html = args.html_path.read_bytes() if args.html_path else download_twse_isin_html(args.source_url, timeout_seconds=args.timeout_seconds)
    source_rows = parse_twse_isin_html(html)
    target_rows = load_target_rows(args.tickers_csv)
    results = verify_rows(target_rows, source_rows)
    updates = build_metadata_updates(results)
    if args.apply and updates:
        merge_metadata_updates(args.metadata_updates_csv, updates)

    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.json_out.write_text(
        json.dumps([row for row in results if row["decision"] == "accept"], indent=2, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )
    write_report_csv(args.csv_out, results)

    print(
        json.dumps(
            {
                "accepted_isin_updates": sum(1 for update in updates if update["field"] == "isin"),
                "accepted_sector_updates": sum(1 for update in updates if update["field"] == "stock_sector"),
                "applied": args.apply,
                "candidates": len(target_rows),
                "csv_out": display_path(args.csv_out),
                "decision_counts": dict(Counter(row["decision"] for row in results)),
                "json_out": display_path(args.json_out),
                "source_rows": len(source_rows),
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()

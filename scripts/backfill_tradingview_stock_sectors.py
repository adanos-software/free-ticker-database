from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.backfill_tradingview_missing_isins import (
    EXCHANGE_TO_TRADINGVIEW,
    TradingViewRow,
    choose_source_for_row,
    display_path,
    fetch_tradingview_rows,
)
from scripts.backfill_yahoo_generic_etf_names import merge_metadata_updates
from scripts.backfill_xtb_omi_isins import names_match
from scripts.rebuild_dataset import TICKERS_CSV, normalize_sector


DEFAULT_OUTPUT_DIR = ROOT / "data" / "tradingview_verification"
DEFAULT_REPORT_JSON = DEFAULT_OUTPUT_DIR / "stock_sector_backfill.json"
DEFAULT_REPORT_CSV = DEFAULT_OUTPUT_DIR / "stock_sector_backfill.csv"
DEFAULT_METADATA_UPDATES_CSV = ROOT / "data" / "review_overrides" / "metadata_updates.csv"

TRADINGVIEW_STOCK_SECTOR_MAP = {
    "Communications": "Communication Services",
    "Consumer Durables": "Consumer Discretionary",
    "Consumer Non-Durables": "Consumer Staples",
    "Consumer Services": "Consumer Discretionary",
    "Distribution Services": "Industrials",
    "Electronic Technology": "Information Technology",
    "Energy Minerals": "Energy",
    "Finance": "Financials",
    "Health Services": "Health Care",
    "Health Technology": "Health Care",
    "Industrial Services": "Industrials",
    "Non-Energy Minerals": "Materials",
    "Process Industries": "Materials",
    "Producer Manufacturing": "Industrials",
    "Technology Services": "Information Technology",
    "Transportation": "Industrials",
    "Utilities": "Utilities",
}

RETAIL_STAPLES_INDUSTRIES = {
    "Food Retail",
    "Food Distributors",
    "Drugstore Chains",
}

COMMUNICATION_SERVICES_INDUSTRIES = {
    "Broadcasting",
    "Cable/Satellite TV",
    "Media Conglomerates",
    "Movies/Entertainment",
}

REPORT_FIELDNAMES = [
    "ticker",
    "exchange",
    "name",
    "tv_symbol",
    "tv_name",
    "tv_exchange",
    "tv_type",
    "tv_sector",
    "tv_industry",
    "sector_update",
    "decision",
]


def map_tradingview_sector(tv_sector: str, tv_industry: str) -> str:
    if tv_industry == "Aerospace & Defense":
        mapped = "Industrials"
    elif tv_industry == "Agricultural Commodities/Milling":
        mapped = "Consumer Staples"
    elif "Real Estate" in tv_industry or "REIT" in tv_industry:
        mapped = "Real Estate"
    elif tv_sector == "Finance" and tv_industry == "Financial Conglomerates":
        mapped = ""
    elif tv_sector == "Consumer Services" and tv_industry in COMMUNICATION_SERVICES_INDUSTRIES:
        mapped = "Communication Services"
    elif tv_sector == "Distribution Services" and tv_industry == "Food Distributors":
        mapped = "Consumer Staples"
    elif tv_sector == "Retail Trade":
        mapped = "Consumer Staples" if tv_industry in RETAIL_STAPLES_INDUSTRIES else "Consumer Discretionary"
    else:
        mapped = TRADINGVIEW_STOCK_SECTOR_MAP.get(tv_sector, "")
    return normalize_sector(mapped, "Stock")


def load_missing_stock_sector_rows(
    *,
    exchanges: set[str],
    tickers_csv: Path = TICKERS_CSV,
) -> list[dict[str, str]]:
    with tickers_csv.open(newline="", encoding="utf-8") as handle:
        return [
            row
            for row in csv.DictReader(handle)
            if row.get("exchange") in exchanges
            and row.get("exchange") in EXCHANGE_TO_TRADINGVIEW
            and row.get("asset_type") == "Stock"
            and not row.get("stock_sector", "").strip()
        ]


def evaluate_row(row: dict[str, str], source: TradingViewRow | None) -> dict[str, Any]:
    base = {
        "ticker": row.get("ticker", ""),
        "exchange": row.get("exchange", ""),
        "name": row.get("name", ""),
        "tv_symbol": "",
        "tv_name": "",
        "tv_exchange": "",
        "tv_type": "",
        "tv_sector": "",
        "tv_industry": "",
        "sector_update": "",
    }
    if source is None:
        return {**base, "decision": "no_tradingview_match"}

    base.update(
        {
            "tv_symbol": source.symbol,
            "tv_name": source.name,
            "tv_exchange": source.exchange,
            "tv_type": source.instrument_type,
            "tv_sector": source.sector,
            "tv_industry": source.industry,
        }
    )
    _, expected_tv_exchange = EXCHANGE_TO_TRADINGVIEW[row["exchange"]]
    if source.exchange != expected_tv_exchange:
        return {**base, "decision": "exchange_mismatch"}
    if source.instrument_type != "stock":
        return {**base, "decision": "asset_type_mismatch"}
    if not names_match(source.name, row["name"]):
        return {**base, "decision": "name_mismatch"}

    sector = map_tradingview_sector(source.sector, source.industry)
    if not sector:
        return {**base, "decision": "unsupported_sector"}
    return {**base, "sector_update": sector, "decision": "accept"}


def verify_rows(target_rows: list[dict[str, str]], source_rows: dict[str, TradingViewRow]) -> list[dict[str, Any]]:
    return [evaluate_row(row, choose_source_for_row(row, source_rows)) for row in target_rows]


def build_metadata_updates(results: list[dict[str, Any]]) -> list[dict[str, str]]:
    updates: list[dict[str, str]] = []
    for result in results:
        if result.get("decision") != "accept":
            continue
        updates.append(
            {
                "ticker": result["ticker"],
                "exchange": result["exchange"],
                "field": "stock_sector",
                "decision": "update",
                "proposed_value": result["sector_update"],
                "confidence": "0.72",
                "reason": (
                    "TradingView free scanner supplied a stock sector mapped to canonical GICS; accepted only after exact "
                    "scanner exchange, symbol, stock asset type, issuer-name, and canonical-sector gates matched."
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
    parser = argparse.ArgumentParser(description="Backfill missing stock sectors from the free TradingView scanner.")
    parser.add_argument("--json-out", type=Path, default=DEFAULT_REPORT_JSON)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_REPORT_CSV)
    parser.add_argument("--metadata-updates-csv", type=Path, default=DEFAULT_METADATA_UPDATES_CSV)
    parser.add_argument("--exchange", action="append", choices=sorted(EXCHANGE_TO_TRADINGVIEW))
    parser.add_argument("--batch-size", type=int, default=150)
    parser.add_argument("--delay-seconds", type=float, default=0.0)
    parser.add_argument("--timeout-seconds", type=float, default=20.0)
    parser.add_argument("--progress-every", type=int, default=500)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    exchanges = set(args.exchange or EXCHANGE_TO_TRADINGVIEW)
    target_rows = load_missing_stock_sector_rows(exchanges=exchanges)
    if args.offset:
        target_rows = target_rows[args.offset :]
    if args.limit is not None:
        target_rows = target_rows[: args.limit]
    source_rows = fetch_tradingview_rows(
        target_rows,
        batch_size=args.batch_size,
        delay_seconds=args.delay_seconds,
        timeout_seconds=args.timeout_seconds,
        progress_every=args.progress_every,
    )
    results = verify_rows(target_rows, source_rows)
    updates = build_metadata_updates(results)

    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.json_out.write_text(json.dumps(results, indent=2), encoding="utf-8")
    write_report_csv(args.csv_out, results)
    if args.apply and updates:
        merge_metadata_updates(args.metadata_updates_csv, updates)

    print(
        json.dumps(
            {
                "accepted_sector_updates": len(updates),
                "applied": args.apply,
                "candidates": len(target_rows),
                "csv_out": display_path(args.csv_out),
                "decision_counts": dict(Counter(result["decision"] for result in results)),
                "exchanges": sorted(exchanges),
                "json_out": display_path(args.json_out),
                "source_rows": len(source_rows),
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()

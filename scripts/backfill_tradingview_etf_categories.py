from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.backfill_tradingview_missing_isins import (
    EXCHANGE_TO_TRADINGVIEW,
    TRADINGVIEW_SCAN_URL,
    USER_AGENT,
    display_path,
    tradingview_symbols_for_row,
)
from scripts.backfill_yahoo_generic_etf_names import merge_metadata_updates
from scripts.backfill_xtb_omi_isins import names_match
from scripts.rebuild_dataset import GENERIC_FUND_WRAPPER_NAMES, TICKERS_CSV, is_valid_isin, normalized_compact, normalize_sector


DEFAULT_OUTPUT_DIR = ROOT / "data" / "tradingview_verification"
DEFAULT_REPORT_JSON = DEFAULT_OUTPUT_DIR / "etf_category_backfill.json"
DEFAULT_REPORT_CSV = DEFAULT_OUTPUT_DIR / "etf_category_backfill.csv"
DEFAULT_METADATA_UPDATES_CSV = ROOT / "data" / "review_overrides" / "metadata_updates.csv"

SCAN_COLUMNS = ["name", "description", "exchange", "type", "subtype", "isin", "asset_class"]

ASSET_CLASS_CATEGORY_MAP = {
    # Verified via TradingView ETF analysis pages and cross-checked against
    # canonical rows: SPY/XLE/VNQ -> Equity, TLT/AGGA -> Fixed Income,
    # GLD -> Commodity, ACEI -> Alternatives. The crypto/currency-like hash is
    # intentionally omitted because it covers mixed currency and digital-asset
    # funds that need a finer taxonomy than a single canonical bucket.
    "c05f85d35d1cd0be6ebb2af4be16e06a": "Equity",
    "b6e443a6c4a8a2e7918c5dbf3d45c796": "Fixed Income",
    "8fe80395f389e29e3ea42210337f0350": "Commodity",
    "4071518f1736a5a43dae51b47590322f": "Alternative",
    # Verified on TradingView analysis pages as Asset Class/Category "Asset allocation".
    "b090e99b8d95f5837ec178c2d3d3fc50": "Alternative",
}

REPORT_FIELDNAMES = [
    "ticker",
    "exchange",
    "name",
    "tv_symbol",
    "tv_name",
    "tv_exchange",
    "tv_type",
    "tv_subtype",
    "tv_isin",
    "tv_asset_class",
    "category_update",
    "decision",
]


GENERIC_WRAPPER_KEYS = {normalized_compact(value) for value in GENERIC_FUND_WRAPPER_NAMES}


def is_generic_wrapper_like(name: str) -> bool:
    compact = normalized_compact(name)
    lowered = name.lower()
    return compact in GENERIC_WRAPPER_KEYS or (
        any(term in lowered for term in (" trust", "funds", "fund ", "etf trust", "series trust"))
        and " etf" not in lowered.replace("etf trust", "")
    )


def names_compatible_for_category(target_name: str, source_name: str) -> bool:
    if names_match(source_name, target_name):
        return True
    source_lower = source_name.lower()
    return is_generic_wrapper_like(target_name) and (" etf" in source_lower or " fund" in source_lower)


def same_valid_isin(target_isin: str, source_isin: str) -> bool:
    target = target_isin.strip().upper()
    source = source_isin.strip().upper()
    return bool(target and source and target == source and is_valid_isin(target) and is_valid_isin(source))


def category_from_source(source: dict[str, str]) -> str:
    source_name = source.get("description", "").lower()
    if "reit" in source_name or "real estate" in source_name:
        return "Real Estate"
    return normalize_sector(ASSET_CLASS_CATEGORY_MAP.get(source.get("asset_class", ""), ""), "ETF")


def load_missing_etf_category_rows(
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
            and row.get("asset_type") == "ETF"
            and not row.get("etf_category", "").strip()
        ]


def fetch_tradingview_etf_rows(
    target_rows: list[dict[str, str]],
    *,
    batch_size: int,
    timeout_seconds: float,
    progress_every: int,
) -> dict[str, dict[str, str]]:
    session = requests.Session()
    requests_by_market: dict[str, list[str]] = {}
    for row in target_rows:
        market, _ = EXCHANGE_TO_TRADINGVIEW[row["exchange"]]
        requests_by_market.setdefault(market, []).extend(tradingview_symbols_for_row(row))

    source_rows: dict[str, dict[str, str]] = {}
    checked = 0
    total = sum(len(symbols) for symbols in requests_by_market.values())
    for market, symbols in sorted(requests_by_market.items()):
        unique_symbols = list(dict.fromkeys(symbols))
        for start in range(0, len(unique_symbols), batch_size):
            batch = unique_symbols[start : start + batch_size]
            response = session.post(
                TRADINGVIEW_SCAN_URL.format(market=market),
                headers={"User-Agent": USER_AGENT, "Content-Type": "application/json"},
                json={"symbols": {"tickers": batch}, "columns": SCAN_COLUMNS},
                timeout=timeout_seconds,
            )
            response.raise_for_status()
            for item in response.json().get("data") or []:
                if not isinstance(item, dict):
                    continue
                values = item.get("d") or []
                if len(values) != len(SCAN_COLUMNS):
                    continue
                row = dict(zip(SCAN_COLUMNS, (str(value or "").strip() for value in values), strict=True))
                source_rows[str(item.get("s") or "").upper()] = row
            checked += len(batch)
            if progress_every > 0 and checked % progress_every < batch_size:
                print(f"checked {checked}/{total} TradingView ETF symbols", file=sys.stderr, flush=True)
    return source_rows


def choose_source(row: dict[str, str], source_rows: dict[str, dict[str, str]]) -> dict[str, str] | None:
    for symbol in tradingview_symbols_for_row(row):
        source = source_rows.get(symbol.upper())
        if source is not None:
            return source
    return None


def evaluate_row(row: dict[str, str], source: dict[str, str] | None) -> dict[str, Any]:
    base = {
        "ticker": row.get("ticker", ""),
        "exchange": row.get("exchange", ""),
        "name": row.get("name", ""),
        "tv_symbol": "",
        "tv_name": "",
        "tv_exchange": "",
        "tv_type": "",
        "tv_subtype": "",
        "tv_isin": "",
        "tv_asset_class": "",
        "category_update": "",
    }
    if source is None:
        return {**base, "decision": "no_tradingview_match"}

    base.update(
        {
            "tv_symbol": source.get("name", ""),
            "tv_name": source.get("description", ""),
            "tv_exchange": source.get("exchange", ""),
            "tv_type": source.get("type", ""),
            "tv_subtype": source.get("subtype", ""),
            "tv_isin": source.get("isin", ""),
            "tv_asset_class": source.get("asset_class", ""),
        }
    )
    _, expected_exchange = EXCHANGE_TO_TRADINGVIEW[row["exchange"]]
    if source.get("exchange") != expected_exchange:
        return {**base, "decision": "exchange_mismatch"}
    if source.get("type") != "fund" or source.get("subtype") != "etf":
        return {**base, "decision": "asset_type_mismatch"}
    if not names_compatible_for_category(row["name"], source.get("description", "")) and not same_valid_isin(
        row.get("isin", ""), source.get("isin", "")
    ):
        return {**base, "decision": "name_mismatch"}
    category = category_from_source(source)
    if not category:
        return {**base, "decision": "unsupported_asset_class"}
    return {**base, "category_update": category, "decision": "accept"}


def verify_rows(target_rows: list[dict[str, str]], source_rows: dict[str, dict[str, str]]) -> list[dict[str, Any]]:
    return [evaluate_row(row, choose_source(row, source_rows)) for row in target_rows]


def build_metadata_updates(results: list[dict[str, Any]]) -> list[dict[str, str]]:
    updates: list[dict[str, str]] = []
    for result in results:
        if result.get("decision") != "accept":
            continue
        updates.append(
            {
                "ticker": result["ticker"],
                "exchange": result["exchange"],
                "field": "etf_category",
                "decision": "update",
                "proposed_value": result["category_update"],
                "confidence": "0.74",
                "reason": (
                    "TradingView free scanner supplied an ETF asset-class code mapped to a canonical category; "
                    "accepted only after exact scanner exchange, ETF asset type, product-name, and supported "
                    "asset-class gates matched."
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
    parser = argparse.ArgumentParser(description="Backfill missing ETF categories from TradingView ETF asset classes.")
    parser.add_argument("--json-out", type=Path, default=DEFAULT_REPORT_JSON)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_REPORT_CSV)
    parser.add_argument("--metadata-updates-csv", type=Path, default=DEFAULT_METADATA_UPDATES_CSV)
    parser.add_argument("--exchange", action="append", choices=sorted(EXCHANGE_TO_TRADINGVIEW))
    parser.add_argument("--batch-size", type=int, default=150)
    parser.add_argument("--timeout-seconds", type=float, default=20.0)
    parser.add_argument("--progress-every", type=int, default=500)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    exchanges = set(args.exchange or EXCHANGE_TO_TRADINGVIEW)
    target_rows = load_missing_etf_category_rows(exchanges=exchanges)
    if args.offset:
        target_rows = target_rows[args.offset :]
    if args.limit is not None:
        target_rows = target_rows[: args.limit]
    source_rows = fetch_tradingview_etf_rows(
        target_rows,
        batch_size=args.batch_size,
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
                "accepted_category_updates": len(updates),
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

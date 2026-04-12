from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request
from collections import Counter
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.backfill_xtb_omi_isins import names_match
from scripts.backfill_yahoo_generic_etf_names import merge_metadata_updates
from scripts.rebuild_dataset import TICKERS_CSV, normalize_sector


DEFAULT_OUTPUT_DIR = ROOT / "data" / "alphavantage_verification"
DEFAULT_CACHE_DIR = DEFAULT_OUTPUT_DIR / "overview_cache"
DEFAULT_REPORT_JSON = DEFAULT_OUTPUT_DIR / "sector_backfill.json"
DEFAULT_REPORT_CSV = DEFAULT_OUTPUT_DIR / "sector_backfill.csv"
DEFAULT_METADATA_UPDATES_CSV = ROOT / "data" / "review_overrides" / "metadata_updates.csv"

ALPHAVANTAGE_BASE_URL = "https://www.alphavantage.co/query"
SUPPORTED_EXCHANGES: set[str] = {"NASDAQ", "NYSE", "NYSE MKT"}
ALPHAVANTAGE_EXCHANGE_CODES: dict[str, set[str]] = {
    "NASDAQ": {"NASDAQ"},
    "NYSE": {"NYSE"},
    "NYSE MKT": {"AMEX", "NYSE MKT"},
}
REPORT_FIELDNAMES = [
    "ticker",
    "exchange",
    "asset_type",
    "name",
    "av_symbol",
    "av_exchange",
    "av_name",
    "av_asset_type",
    "av_sector",
    "av_industry",
    "sector_update",
    "number_tokens_match",
    "name_match",
    "decision",
]


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def normalized_ticker_key(value: str) -> str:
    return value.strip().upper().replace("-", ".").replace("/", ".")


def significant_numbers(value: str) -> set[str]:
    return set(re.findall(r"\d+", value))


def normalize_alphavantage_sector(raw_sector: str) -> str:
    if not raw_sector:
        return ""
    return normalize_sector(raw_sector.strip().replace("&", "and").title(), "Stock")


def load_missing_sector_rows(
    *,
    exchanges: set[str],
    tickers_csv: Path = TICKERS_CSV,
) -> list[dict[str, str]]:
    with tickers_csv.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    return [
        row
        for row in rows
        if row["exchange"] in exchanges
        and row["asset_type"] == "Stock"
        and not (row.get("stock_sector", "") or row.get("sector", "")).strip()
    ]


def cache_path_for_symbol(cache_dir: Path, symbol: str) -> Path:
    safe_symbol = re.sub(r"[^A-Za-z0-9_.-]+", "_", symbol.strip().upper())
    return cache_dir / f"{safe_symbol}.json"


def fetch_alphavantage_overview(
    symbol: str,
    api_key: str,
    *,
    cache_dir: Path,
    timeout_seconds: float,
) -> tuple[dict[str, Any], bool]:
    cache_path = cache_path_for_symbol(cache_dir, symbol)
    if cache_path.exists():
        return json.loads(cache_path.read_text(encoding="utf-8")), True

    params = urllib.parse.urlencode({"function": "OVERVIEW", "symbol": symbol, "apikey": api_key})
    url = f"{ALPHAVANTAGE_BASE_URL}?{params}"
    with urllib.request.urlopen(url, timeout=timeout_seconds) as response:
        payload = json.load(response)
    if not isinstance(payload, dict):
        payload = {"Information": f"Unexpected Alpha Vantage OVERVIEW payload type: {type(payload).__name__}"}

    if not (payload.get("Note") or payload.get("Information") or payload.get("Error Message")):
        cache_dir.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return payload, False


def evaluate_alphavantage_row(row: dict[str, str], payload: dict[str, Any]) -> dict[str, Any]:
    av_name = str(payload.get("Name") or "").strip()
    av_symbol = str(payload.get("Symbol") or "").strip().upper()
    av_exchange = str(payload.get("Exchange") or "").strip().upper()
    av_asset_type = str(payload.get("AssetType") or "").strip()
    av_sector = str(payload.get("Sector") or "").strip()

    base = {
        "ticker": row["ticker"],
        "exchange": row["exchange"],
        "asset_type": row["asset_type"],
        "name": row["name"],
        "av_symbol": av_symbol,
        "av_exchange": av_exchange,
        "av_name": av_name,
        "av_asset_type": av_asset_type,
        "av_sector": av_sector,
        "av_industry": str(payload.get("Industry") or "").strip(),
        "sector_update": "",
        "number_tokens_match": False,
        "name_match": False,
    }

    api_message = payload.get("Note") or payload.get("Information") or payload.get("Error Message")
    if api_message:
        return {**base, "decision": "api_message"}
    if not av_symbol and not av_name:
        return {**base, "decision": "no_alphavantage_match"}
    if normalized_ticker_key(av_symbol) != normalized_ticker_key(row["ticker"]):
        return {**base, "decision": "symbol_mismatch"}
    if av_exchange not in ALPHAVANTAGE_EXCHANGE_CODES.get(row["exchange"], set()):
        return {**base, "decision": "exchange_mismatch"}
    if av_asset_type != "Common Stock":
        return {**base, "decision": "asset_type_mismatch"}

    number_tokens_match = significant_numbers(av_name) == significant_numbers(row["name"])
    name_match = names_match(av_name, row["name"])
    base["number_tokens_match"] = number_tokens_match
    base["name_match"] = name_match
    if not number_tokens_match:
        return {**base, "decision": "number_token_mismatch"}
    if not name_match:
        return {**base, "decision": "name_mismatch"}

    sector_update = normalize_alphavantage_sector(av_sector)
    if not av_sector:
        return {**base, "decision": "missing_sector"}
    if not sector_update:
        return {**base, "decision": "invalid_sector"}
    return {**base, "sector_update": sector_update, "decision": "accept"}


def build_metadata_updates(results: list[dict[str, Any]]) -> list[dict[str, str]]:
    updates: list[dict[str, str]] = []
    for result in results:
        if result["decision"] != "accept":
            continue
        updates.append(
            {
                "ticker": result["ticker"],
                "exchange": result["exchange"],
                "field": "stock_sector",
                "decision": "update",
                "proposed_value": result["sector_update"],
                "confidence": "0.76",
                "reason": "Alpha Vantage OVERVIEW supplied a normalized stock sector for a US listing without sector; accepted only after ticker, exchange, Common Stock asset type, issuer-name, numeric-token, and canonical-sector gates matched.",
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
    parser = argparse.ArgumentParser(description="Backfill missing US stock sectors from Alpha Vantage OVERVIEW.")
    parser.add_argument("--json-out", type=Path, default=DEFAULT_REPORT_JSON)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_REPORT_CSV)
    parser.add_argument("--cache-dir", type=Path, default=DEFAULT_CACHE_DIR)
    parser.add_argument("--metadata-updates-csv", type=Path, default=DEFAULT_METADATA_UPDATES_CSV)
    parser.add_argument("--exchange", action="append", help="Restrict to one or more internal exchanges.")
    parser.add_argument("--timeout-seconds", type=float, default=30.0)
    parser.add_argument("--rate-limit-seconds", type=float, default=12.5)
    parser.add_argument("--max-requests", type=int, help="Maximum uncached Alpha Vantage API requests for this run.")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    api_key = os.environ.get("ALPHA_VANTAGE_API_KEY", "").strip()
    if not api_key:
        raise SystemExit("ALPHA_VANTAGE_API_KEY is required. Pass it as an environment variable; do not commit it.")

    exchanges = set(args.exchange or SUPPORTED_EXCHANGES)
    unsupported = sorted(exchanges - SUPPORTED_EXCHANGES)
    if unsupported:
        raise SystemExit(f"Unsupported Alpha Vantage exchange(s): {', '.join(unsupported)}")

    rows = load_missing_sector_rows(exchanges=exchanges)
    if args.offset:
        rows = rows[args.offset :]
    if args.limit is not None:
        rows = rows[: args.limit]

    results: list[dict[str, Any]] = []
    requests_made = 0
    for row in rows:
        if args.max_requests is not None and requests_made >= args.max_requests:
            results.append(
                {
                    "ticker": row["ticker"],
                    "exchange": row["exchange"],
                    "asset_type": row["asset_type"],
                    "name": row["name"],
                    "decision": "max_requests_reached",
                }
            )
            continue

        payload, cache_hit = fetch_alphavantage_overview(
            row["ticker"],
            api_key,
            cache_dir=args.cache_dir,
            timeout_seconds=args.timeout_seconds,
        )
        if not cache_hit:
            requests_made += 1
        results.append(evaluate_alphavantage_row(row, payload))
        if not cache_hit and args.rate_limit_seconds > 0:
            time.sleep(args.rate_limit_seconds)

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
                "candidates": len(rows),
                "decision_counts": dict(Counter(result["decision"] for result in results)),
                "exchanges": sorted(exchanges),
                "requests_made": requests_made,
                "accepted_sector_updates": len(updates),
                "json_out": display_path(args.json_out),
                "csv_out": display_path(args.csv_out),
                "applied": args.apply,
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()

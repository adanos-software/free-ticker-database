from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.backfill_yahoo_generic_etf_names import merge_metadata_updates, socket_timeout
from scripts.rebuild_dataset import TICKERS_CSV, alias_matches_company, is_valid_isin
from scripts.verify_yahoo_listings import (
    choose_yahoo_name,
    expected_exchange_match,
    expected_quote_type_match,
    fetch_yahoo_symbol,
    normalize_isin,
    yahoo_symbol_candidates,
)


DEFAULT_OUTPUT_DIR = ROOT / "data" / "yahoo_verification"
DEFAULT_REPORT_JSON = DEFAULT_OUTPUT_DIR / "otc_missing_isin_backfill.json"
DEFAULT_REPORT_CSV = DEFAULT_OUTPUT_DIR / "otc_missing_isin_backfill.csv"
DEFAULT_METADATA_UPDATES_CSV = ROOT / "data" / "review_overrides" / "metadata_updates.csv"

REPORT_FIELDNAMES = [
    "ticker",
    "exchange",
    "asset_type",
    "name",
    "current_isin",
    "yahoo_symbol",
    "yahoo_name",
    "yahoo_quote_type",
    "yahoo_exchange",
    "yahoo_full_exchange",
    "yahoo_isin",
    "history_rows",
    "exchange_match",
    "quote_type_match",
    "name_match",
    "decision",
    "error",
]


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def load_otc_missing_isin_rows(tickers_csv: Path = TICKERS_CSV) -> list[dict[str, str]]:
    with tickers_csv.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    return [
        row
        for row in rows
        if row["exchange"] == "OTC" and not row.get("isin", "").strip()
    ]


def evaluate_otc_missing_isin_row(
    row: dict[str, str],
    yahoo_result: dict[str, Any],
) -> dict[str, Any]:
    yahoo_name = choose_yahoo_name(yahoo_result)
    yahoo_exchange = str(yahoo_result.get("exchange") or "")
    yahoo_full_exchange = str(yahoo_result.get("fullExchangeName") or "")
    yahoo_quote_type = str(yahoo_result.get("quoteType") or "")
    yahoo_isin = normalize_isin(yahoo_result.get("isin")).upper()
    exchange_match = expected_exchange_match(row["exchange"], yahoo_exchange, yahoo_full_exchange)
    quote_type_match = expected_quote_type_match(row["asset_type"], yahoo_quote_type)
    name_match = alias_matches_company(yahoo_name, row["name"]) if yahoo_name else False

    if not yahoo_result.get("exists"):
        decision = "not_found"
    elif not yahoo_isin:
        decision = "missing_isin"
    elif not is_valid_isin(yahoo_isin):
        decision = "invalid_isin"
    elif exchange_match is not True:
        decision = "exchange_mismatch"
    elif quote_type_match is not True:
        decision = "quote_type_mismatch"
    elif not yahoo_name:
        decision = "missing_name"
    elif not name_match:
        decision = "name_mismatch"
    else:
        decision = "accept"

    return {
        "ticker": row["ticker"],
        "exchange": row["exchange"],
        "asset_type": row["asset_type"],
        "name": row["name"],
        "current_isin": row.get("isin", ""),
        "yahoo_symbol": yahoo_result.get("symbol", ""),
        "yahoo_name": yahoo_name,
        "yahoo_quote_type": yahoo_quote_type,
        "yahoo_exchange": yahoo_exchange,
        "yahoo_full_exchange": yahoo_full_exchange,
        "yahoo_isin": yahoo_isin,
        "history_rows": yahoo_result.get("history_rows", 0),
        "exchange_match": exchange_match,
        "quote_type_match": quote_type_match,
        "name_match": name_match,
        "decision": decision,
        "error": "",
    }


def verify_otc_missing_isins(
    rows: list[dict[str, str]],
    *,
    delay_seconds: float,
    timeout_seconds: float,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        candidates = yahoo_symbol_candidates(row["ticker"], row["exchange"])
        if not candidates:
            results.append(
                {
                    "ticker": row["ticker"],
                    "exchange": row["exchange"],
                    "asset_type": row["asset_type"],
                    "name": row["name"],
                    "current_isin": row.get("isin", ""),
                    "decision": "unsupported_exchange",
                    "yahoo_symbol": "",
                    "error": "",
                }
            )
            continue

        chosen = None
        last_error = None
        for symbol in candidates:
            try:
                with socket_timeout(timeout_seconds):
                    yahoo_result = fetch_yahoo_symbol(symbol)
            except Exception as exc:  # noqa: BLE001
                last_error = str(exc)
                continue
            chosen = evaluate_otc_missing_isin_row(row, yahoo_result)
            if chosen["decision"] == "accept":
                break

        if chosen is not None:
            results.append(chosen)
        else:
            results.append(
                {
                    "ticker": row["ticker"],
                    "exchange": row["exchange"],
                    "asset_type": row["asset_type"],
                    "name": row["name"],
                    "current_isin": row.get("isin", ""),
                    "decision": "error",
                    "yahoo_symbol": candidates[0],
                    "error": last_error or "No Yahoo result.",
                }
            )

        if delay_seconds > 0 and index < len(rows) - 1:
            time.sleep(delay_seconds)
    return results


def build_metadata_updates(results: list[dict[str, Any]]) -> list[dict[str, str]]:
    updates: list[dict[str, str]] = []
    for result in results:
        if result["decision"] != "accept":
            continue
        updates.append(
            {
                "ticker": result["ticker"],
                "exchange": result["exchange"],
                "field": "isin",
                "decision": "update",
                "proposed_value": result["yahoo_isin"],
                "confidence": "0.85",
                "reason": "Yahoo Finance returned a valid ISIN for an OTC row without ISIN, with matching OTC venue, quote type, and issuer name; used as a gated review override rather than a hard source import.",
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
    parser = argparse.ArgumentParser(description="Backfill missing OTC ISINs via gated Yahoo Finance verification.")
    parser.add_argument("--json-out", type=Path, default=DEFAULT_REPORT_JSON)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_REPORT_CSV)
    parser.add_argument("--metadata-updates-csv", type=Path, default=DEFAULT_METADATA_UPDATES_CSV)
    parser.add_argument("--delay-seconds", type=float, default=0.0)
    parser.add_argument("--timeout-seconds", type=float, default=15.0)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--ticker", action="append", help="Restrict to one or more OTC tickers.")
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    rows = load_otc_missing_isin_rows()
    if args.ticker:
        tickers = {ticker.upper() for ticker in args.ticker}
        rows = [row for row in rows if row["ticker"].upper() in tickers]
    if args.offset:
        rows = rows[args.offset :]
    if args.limit is not None:
        rows = rows[: args.limit]

    results = verify_otc_missing_isins(
        rows,
        delay_seconds=args.delay_seconds,
        timeout_seconds=args.timeout_seconds,
    )
    updates = build_metadata_updates(results)

    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.json_out.write_text(json.dumps(results, indent=2), encoding="utf-8")
    write_report_csv(args.csv_out, results)

    if args.apply and updates:
        merge_metadata_updates(args.metadata_updates_csv, updates)

    print(
        json.dumps(
            {
                "candidates": len(rows),
                "offset": args.offset,
                "decision_counts": dict(Counter(result["decision"] for result in results)),
                "accepted_isin_updates": len(updates),
                "json_out": display_path(args.json_out),
                "csv_out": display_path(args.csv_out),
                "applied": args.apply,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()

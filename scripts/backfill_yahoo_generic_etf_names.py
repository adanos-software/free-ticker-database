from __future__ import annotations

import argparse
import csv
import json
import socket
import sys
import time
from collections import Counter
from contextlib import contextmanager
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.rebuild_dataset import GENERIC_FUND_WRAPPER_NAMES, TICKERS_CSV, is_valid_isin, normalized_compact
from scripts.verify_yahoo_listings import (
    choose_yahoo_name,
    expected_exchange_match,
    fetch_yahoo_symbol,
    normalize_isin,
    yahoo_symbol_candidates,
)

DEFAULT_OUTPUT_DIR = ROOT / "data" / "yahoo_verification"
DEFAULT_REPORT_JSON = DEFAULT_OUTPUT_DIR / "generic_etf_name_backfill.json"
DEFAULT_REPORT_CSV = DEFAULT_OUTPUT_DIR / "generic_etf_name_backfill.csv"
DEFAULT_METADATA_UPDATES_CSV = ROOT / "data" / "review_overrides" / "metadata_updates.csv"

GENERIC_ETF_WRAPPER_NAME_KEYS = {normalized_compact(value) for value in GENERIC_FUND_WRAPPER_NAMES}

ETFISH_NAME_MARKERS = (
    " etf",
    " etn",
    " etp",
    " fund",
    " funds",
    " trust",
    " ucits",
)


def is_generic_etf_wrapper_name(name: str) -> bool:
    return normalized_compact(name) in GENERIC_ETF_WRAPPER_NAME_KEYS


def looks_like_specific_fund_name(name: str) -> bool:
    lowered = name.lower().strip()
    if not lowered or is_generic_etf_wrapper_name(lowered):
        return False
    return any(marker in f" {lowered} " for marker in ETFISH_NAME_MARKERS)


def load_generic_etf_rows() -> list[dict[str, str]]:
    with TICKERS_CSV.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    return [
        row
        for row in rows
        if row["asset_type"] == "ETF" and is_generic_etf_wrapper_name(row["name"])
    ]


@contextmanager
def socket_timeout(seconds: float):
    previous = socket.getdefaulttimeout()
    socket.setdefaulttimeout(seconds)
    try:
        yield
    finally:
        socket.setdefaulttimeout(previous)


def evaluate_generic_etf_row(row: dict[str, str], yahoo_result: dict[str, Any]) -> dict[str, Any]:
    yahoo_name = choose_yahoo_name(yahoo_result)
    yahoo_exchange = str(yahoo_result.get("exchange") or "")
    yahoo_full_exchange = str(yahoo_result.get("fullExchangeName") or "")
    yahoo_quote_type = str(yahoo_result.get("quoteType") or "")
    yahoo_isin = normalize_isin(yahoo_result.get("isin"))
    exchange_match = expected_exchange_match(row["exchange"], yahoo_exchange, yahoo_full_exchange)
    specific_name = looks_like_specific_fund_name(yahoo_name)
    quote_type_match = yahoo_quote_type == "ETF" or (yahoo_quote_type == "EQUITY" and specific_name)
    isin_conflict = bool(row["isin"] and yahoo_isin and row["isin"] != yahoo_isin)

    if not yahoo_result.get("exists"):
        decision = "not_found"
    elif exchange_match is False:
        decision = "exchange_mismatch"
    elif not specific_name:
        decision = "non_specific_name"
    elif not quote_type_match:
        decision = "quote_type_mismatch"
    elif isin_conflict:
        decision = "isin_conflict"
    else:
        decision = "accept"

    return {
        "ticker": row["ticker"],
        "exchange": row["exchange"],
        "current_name": row["name"],
        "current_isin": row["isin"],
        "yahoo_symbol": yahoo_result.get("symbol", ""),
        "yahoo_name": yahoo_name,
        "yahoo_quote_type": yahoo_quote_type,
        "yahoo_exchange": yahoo_exchange,
        "yahoo_full_exchange": yahoo_full_exchange,
        "yahoo_isin": yahoo_isin,
        "history_rows": yahoo_result.get("history_rows", 0),
        "exchange_match": exchange_match,
        "specific_name": specific_name,
        "quote_type_match": quote_type_match,
        "isin_conflict": isin_conflict,
        "decision": decision,
    }


def verify_generic_etfs(
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
                    "current_name": row["name"],
                    "current_isin": row["isin"],
                    "decision": "unsupported_exchange",
                    "yahoo_symbol": "",
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
            chosen = evaluate_generic_etf_row(row, yahoo_result)
            if chosen["decision"] == "accept":
                break

        if chosen is not None:
            results.append(chosen)
        else:
            results.append(
                {
                    "ticker": row["ticker"],
                    "exchange": row["exchange"],
                    "current_name": row["name"],
                    "current_isin": row["isin"],
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
                "field": "name",
                "decision": "update",
                "proposed_value": result["yahoo_name"],
                "confidence": "0.95",
                "reason": "Yahoo Finance verified a specific ETF product name for a row that previously only carried a generic trust/fund wrapper name.",
            }
        )

        yahoo_isin = result.get("yahoo_isin", "")
        if yahoo_isin and not result.get("current_isin") and is_valid_isin(yahoo_isin):
            updates.append(
                {
                    "ticker": result["ticker"],
                    "exchange": result["exchange"],
                    "field": "isin",
                    "decision": "update",
                    "proposed_value": yahoo_isin,
                    "confidence": "0.9",
                    "reason": "Yahoo Finance provided an ISIN for an ETF row that previously had no ISIN.",
                }
            )
    return updates


def merge_metadata_updates(path: Path, updates: list[dict[str, str]]) -> None:
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    by_key: dict[tuple[str, str, str], dict[str, str]] = {
        (row["ticker"], row["exchange"], row["field"]): row
        for row in rows
    }
    for update in updates:
        by_key[(update["ticker"], update["exchange"], update["field"])] = update

    merged_rows = sorted(
        by_key.values(),
        key=lambda row: (row["ticker"], row["exchange"], row["field"]),
    )
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["ticker", "exchange", "field", "decision", "proposed_value", "confidence", "reason"],
        )
        writer.writeheader()
        writer.writerows(merged_rows)


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill generic ETF wrapper names via Yahoo Finance.")
    parser.add_argument("--json-out", type=Path, default=DEFAULT_REPORT_JSON)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_REPORT_CSV)
    parser.add_argument("--metadata-updates-csv", type=Path, default=DEFAULT_METADATA_UPDATES_CSV)
    parser.add_argument("--delay-seconds", type=float, default=0.0)
    parser.add_argument("--timeout-seconds", type=float, default=15.0)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    rows = load_generic_etf_rows()
    if args.offset:
        rows = rows[args.offset :]
    if args.limit is not None:
        rows = rows[: args.limit]

    results = verify_generic_etfs(
        rows,
        delay_seconds=args.delay_seconds,
        timeout_seconds=args.timeout_seconds,
    )
    updates = build_metadata_updates(results)

    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.json_out.write_text(json.dumps(results, indent=2), encoding="utf-8")
    write_csv(args.csv_out, results)

    if args.apply and updates:
        merge_metadata_updates(args.metadata_updates_csv, updates)

    print(
        json.dumps(
            {
                "candidates": len(rows),
                "offset": args.offset,
                "decision_counts": dict(Counter(result["decision"] for result in results)),
                "accepted_name_updates": sum(update["field"] == "name" for update in updates),
                "accepted_isin_updates": sum(update["field"] == "isin" for update in updates),
                "json_out": str(args.json_out.relative_to(ROOT)),
                "csv_out": str(args.csv_out.relative_to(ROOT)),
                "applied": args.apply,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()

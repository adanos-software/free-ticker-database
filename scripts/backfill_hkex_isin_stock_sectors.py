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

from scripts.backfill_hkex_hsic_sectors import (
    DEFAULT_CHROME_EXECUTABLE,
    HKEX_QUOTE_URL,
    capture_hkex_hsic,
    hsic_to_canonical,
    quote_symbol,
)
from scripts.lib.dataio import merge_metadata_updates
from scripts.rebuild_dataset import LISTINGS_CSV, is_valid_isin

DEFAULT_SECURITIES_JSON = ROOT / "data" / "masterfiles" / "cache" / "hkex_securities_list.json"
DEFAULT_OUTPUT_DIR = ROOT / "data" / "hkex_verification"
DEFAULT_CAPTURE_JSON = DEFAULT_OUTPUT_DIR / "fsx_isin_hsic_capture.json"
DEFAULT_CAPTURE_CSV = DEFAULT_OUTPUT_DIR / "fsx_isin_hsic_capture.csv"
DEFAULT_REPORT_JSON = DEFAULT_OUTPUT_DIR / "fsx_isin_stock_sector_backfill.json"
DEFAULT_REPORT_CSV = DEFAULT_OUTPUT_DIR / "fsx_isin_stock_sector_backfill.csv"
DEFAULT_METADATA_UPDATES_CSV = ROOT / "data" / "review_overrides" / "metadata_updates.csv"
REASON = (
    "Copied stock_sector from the official HKEX quote-page HSIC industry after an exact ISIN "
    "match to the HKEX ListOfSecurities workbook; Conglomerates left unmapped."
)
REPORT_FIELDNAMES = [
    "ticker",
    "exchange",
    "asset_type",
    "name",
    "isin",
    "hkex_ticker",
    "hkex_name",
    "hkex_quote_url",
    "hkex_industry",
    "sector_update",
    "decision",
]


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def load_hkex_securities_by_isin(path: Path = DEFAULT_SECURITIES_JSON) -> dict[str, dict[str, str]]:
    rows = json.loads(path.read_text(encoding="utf-8"))
    indexed: dict[str, dict[str, str]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        if str(row.get("asset_type") or "") != "Stock":
            continue
        isin = str(row.get("isin") or "").strip().upper()
        ticker = str(row.get("ticker") or "").strip()
        if not is_valid_isin(isin) or not ticker:
            continue
        indexed[isin] = {
            "ticker": ticker,
            "name": str(row.get("name") or "").strip(),
            "isin": isin,
        }
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
    hkex_row: dict[str, str] | None,
    capture: dict[str, Any] | None,
) -> dict[str, Any]:
    isin = (row.get("isin") or "").strip().upper()
    base = {
        "ticker": row.get("ticker", ""),
        "exchange": row.get("exchange", ""),
        "asset_type": row.get("asset_type", ""),
        "name": row.get("name", ""),
        "isin": isin,
        "hkex_ticker": "",
        "hkex_name": "",
        "hkex_quote_url": "",
        "hkex_industry": "",
        "sector_update": "",
    }
    if (row.get("stock_sector") or row.get("sector") or "").strip():
        return {**base, "decision": "already_has_sector"}
    if not is_valid_isin(isin):
        return {**base, "decision": "missing_isin"}
    if not hkex_row:
        return {**base, "decision": "no_hkex_isin"}
    base.update(
        {
            "hkex_ticker": hkex_row.get("ticker", ""),
            "hkex_name": hkex_row.get("name", ""),
            "hkex_quote_url": HKEX_QUOTE_URL.format(sym=quote_symbol(hkex_row.get("ticker", ""))),
        }
    )
    if not capture:
        return {**base, "decision": "missing_capture"}
    industry = str(capture.get("hkex_industry") or "").strip()
    base["hkex_industry"] = industry
    if capture.get("error") and not industry:
        return {**base, "decision": "capture_error"}
    if not industry:
        return {**base, "decision": "missing_hsic_industry"}
    sector = hsic_to_canonical(industry)
    if not sector:
        return {**base, "decision": "unsupported_hsic_sector"}
    return {**base, "sector_update": sector, "decision": "accept"}


def verify_rows(
    rows: list[dict[str, str]],
    securities_by_isin: dict[str, dict[str, str]],
    captures: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    capture_by_ticker = {str(row.get("ticker", "")): row for row in captures}
    results: list[dict[str, Any]] = []
    for row in rows:
        isin = (row.get("isin") or "").strip().upper()
        hkex_row = securities_by_isin.get(isin)
        capture = capture_by_ticker.get(hkex_row["ticker"]) if hkex_row else None
        results.append(evaluate_row(row, hkex_row, capture))
    return results


def build_metadata_updates(results: list[dict[str, Any]]) -> list[dict[str, str]]:
    return [
        {
            "ticker": result["ticker"],
            "exchange": result["exchange"],
            "field": "stock_sector",
            "decision": "update",
            "proposed_value": result["sector_update"],
            "confidence": "0.90",
            "reason": REASON + f" Source: {result['hkex_quote_url']}",
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


def capture_targets(securities_by_isin: dict[str, dict[str, str]], rows: list[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[str] = set()
    targets: list[dict[str, str]] = []
    for row in rows:
        isin = (row.get("isin") or "").strip().upper()
        hkex_row = securities_by_isin.get(isin)
        if not hkex_row or hkex_row["ticker"] in seen:
            continue
        seen.add(hkex_row["ticker"])
        targets.append(
            {
                "ticker": hkex_row["ticker"],
                "exchange": "HKEX",
                "asset_type": "Stock",
                "name": hkex_row["name"],
                "isin": hkex_row["isin"],
            }
        )
    return targets


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill missing stock sectors from official HKEX HSIC metadata after an exact ISIN match."
    )
    parser.add_argument("--listings-csv", type=Path, default=LISTINGS_CSV)
    parser.add_argument("--securities-json", type=Path, default=DEFAULT_SECURITIES_JSON)
    parser.add_argument("--capture-json", type=Path, default=DEFAULT_CAPTURE_JSON)
    parser.add_argument("--capture-csv", type=Path, default=DEFAULT_CAPTURE_CSV)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_REPORT_JSON)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_REPORT_CSV)
    parser.add_argument("--metadata-updates-csv", type=Path, default=DEFAULT_METADATA_UPDATES_CSV)
    parser.add_argument("--chrome-executable", default=DEFAULT_CHROME_EXECUTABLE)
    parser.add_argument("--exchange", action="append")
    parser.add_argument("--capture", action="store_true")
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    exchanges = set(args.exchange) if args.exchange else {"FSX"}
    rows = load_missing_rows(args.listings_csv, exchanges=exchanges)
    securities_by_isin = load_hkex_securities_by_isin(args.securities_json)
    if args.capture:
        captures = capture_hkex_hsic(
            capture_targets(securities_by_isin, rows),
            chrome_executable=args.chrome_executable,
            output_json=args.capture_json,
            output_csv=args.capture_csv,
        )
    else:
        captures = json.loads(args.capture_json.read_text(encoding="utf-8")) if args.capture_json.exists() else []
    results = verify_rows(rows, securities_by_isin, captures)
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
                "capture_rows": len(captures),
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

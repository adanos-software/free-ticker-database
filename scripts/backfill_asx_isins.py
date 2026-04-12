from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import Counter
from dataclasses import asdict, dataclass
from io import BytesIO
from pathlib import Path
from typing import Any

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.backfill_yahoo_generic_etf_names import merge_metadata_updates
from scripts.rebuild_dataset import TICKERS_CSV, alias_matches_company, is_valid_isin


DEFAULT_ASX_ISIN_URL = "https://www.asx.com.au/content/dam/asx/issuers/ISIN.xls"
DEFAULT_OUTPUT_DIR = ROOT / "data" / "asx_verification"
DEFAULT_REPORT_JSON = DEFAULT_OUTPUT_DIR / "missing_isin_backfill.json"
DEFAULT_REPORT_CSV = DEFAULT_OUTPUT_DIR / "missing_isin_backfill.csv"
DEFAULT_METADATA_UPDATES_CSV = ROOT / "data" / "review_overrides" / "metadata_updates.csv"

REPORT_FIELDNAMES = [
    "ticker",
    "exchange",
    "asset_type",
    "target_name",
    "asx_name",
    "asx_security_type",
    "asx_isin",
    "name_match",
    "decision",
]


@dataclass(frozen=True)
class AsxIsinRow:
    ticker: str
    name: str
    security_type: str
    isin: str


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def significant_numbers(value: str) -> set[str]:
    return set(re.findall(r"\d+", value))


def strict_names_match(source_name: str, target_name: str) -> bool:
    if significant_numbers(source_name) != significant_numbers(target_name):
        return False
    return alias_matches_company(source_name, target_name)


def download_asx_isin_xls(url: str, *, timeout_seconds: float) -> bytes:
    response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=timeout_seconds)
    response.raise_for_status()
    return response.content


def parse_asx_isin_xls(xls_bytes: bytes) -> list[AsxIsinRow]:
    dataframe = pd.read_excel(BytesIO(xls_bytes), sheet_name="ISIN", dtype=str).fillna("")
    rows: list[AsxIsinRow] = []
    seen: set[str] = set()
    for record in dataframe.to_dict("records"):
        ticker = str(record.get("ASX code") or "").strip().upper()
        name = str(record.get("Company name") or "").strip()
        security_type = str(record.get("Security type") or "").strip()
        isin = str(record.get("ISIN code") or "").strip().upper()
        if not ticker or not name or ticker in seen or not is_valid_isin(isin):
            continue
        seen.add(ticker)
        rows.append(AsxIsinRow(ticker=ticker, name=name, security_type=security_type, isin=isin))
    return rows


def load_asx_missing_isin_rows(tickers_csv: Path = TICKERS_CSV) -> list[dict[str, str]]:
    with tickers_csv.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    return [
        row
        for row in rows
        if row["exchange"] == "ASX" and not row.get("isin", "").strip()
    ]


def evaluate_asx_row(target: dict[str, str], source_by_ticker: dict[str, AsxIsinRow]) -> dict[str, Any]:
    base = {
        "ticker": target["ticker"],
        "exchange": target["exchange"],
        "asset_type": target["asset_type"],
        "target_name": target["name"],
        "asx_name": "",
        "asx_security_type": "",
        "asx_isin": "",
        "name_match": "",
    }
    source = source_by_ticker.get(target["ticker"])
    if source is None:
        return {**base, "decision": "no_asx_match"}

    base.update(
        {
            "asx_name": source.name,
            "asx_security_type": source.security_type,
            "asx_isin": source.isin,
        }
    )
    if not is_valid_isin(source.isin):
        return {**base, "decision": "invalid_isin"}

    name_match = strict_names_match(source.name, target["name"])
    base["name_match"] = name_match
    if not name_match:
        return {**base, "decision": "name_mismatch"}
    return {**base, "decision": "accept"}


def verify_asx_missing_isins(
    target_rows: list[dict[str, str]],
    source_rows: list[AsxIsinRow],
) -> list[dict[str, Any]]:
    source_by_ticker = {row.ticker: row for row in source_rows}
    return [evaluate_asx_row(row, source_by_ticker) for row in target_rows]


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
                "proposed_value": result["asx_isin"],
                "confidence": "0.96",
                "reason": "Official ASX ISIN.xls lists this ASX code with a valid ISIN; accepted only after exact ASX code, issuer-name, numeric-token, and ISIN-checksum gates matched a current ASX row without ISIN.",
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
    parser = argparse.ArgumentParser(description="Backfill ASX missing ISINs from the official ASX ISIN.xls file.")
    parser.add_argument("--xls-url", default=DEFAULT_ASX_ISIN_URL)
    parser.add_argument("--xls-path", type=Path, help="Read a local ASX ISIN.xls instead of downloading it.")
    parser.add_argument("--json-out", type=Path, default=DEFAULT_REPORT_JSON)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_REPORT_CSV)
    parser.add_argument("--metadata-updates-csv", type=Path, default=DEFAULT_METADATA_UPDATES_CSV)
    parser.add_argument("--timeout-seconds", type=float, default=30.0)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    xls_bytes = args.xls_path.read_bytes() if args.xls_path else download_asx_isin_xls(
        args.xls_url,
        timeout_seconds=args.timeout_seconds,
    )
    source_rows = parse_asx_isin_xls(xls_bytes)
    target_rows = load_asx_missing_isin_rows()
    if args.limit is not None:
        target_rows = target_rows[: args.limit]

    results = verify_asx_missing_isins(target_rows, source_rows)
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
                "source_rows": len(source_rows),
                "candidates": len(target_rows),
                "decision_counts": dict(Counter(result["decision"] for result in results)),
                "accepted_isin_updates": len(updates),
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

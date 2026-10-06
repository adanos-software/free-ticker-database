from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.lib.dataio import merge_metadata_updates
from scripts.rebuild_dataset import LISTINGS_CSV, is_valid_isin, normalize_sector

LJSE_QUOTE_URL = "https://ljse.si/en/papir-311/310?isin={isin}"
DEFAULT_OUTPUT_DIR = ROOT / "data" / "ljse_verification"
DEFAULT_CAPTURE_JSON = DEFAULT_OUTPUT_DIR / "fsx_nace_capture.json"
DEFAULT_REPORT_JSON = DEFAULT_OUTPUT_DIR / "isin_stock_sector_backfill.json"
DEFAULT_REPORT_CSV = DEFAULT_OUTPUT_DIR / "isin_stock_sector_backfill.csv"
DEFAULT_METADATA_UPDATES_CSV = ROOT / "data" / "review_overrides" / "metadata_updates.csv"
NACE_PREFIX_MAP = {
    "05": "Materials",
    "06": "Energy",
    "07": "Materials",
    "08": "Materials",
    "35": "Utilities",
    "61": "Communication Services",
    "64": "Financials",
    "65": "Financials",
    "66": "Financials",
    "68": "Real Estate",
}
SECTOR_TEXT_MAP = {
    "FINANCIAL AND INSURANCE ACTIVITIES": "Financials",
    "REAL ESTATE ACTIVITIES": "Real Estate",
    "ELECTRICITY, GAS, STEAM AND AIR CONDITIONING SUPPLY": "Utilities",
    "MINING AND QUARRYING": "Materials",
}
PAGE_ISIN_RE = re.compile(r"<li>\s*ISIN\s*</li>\s*<li>\s*([A-Z]{2}[A-Z0-9]{9}[0-9])\s*</li>", re.I)
NACE_RE = re.compile(
    r"<li>\s*NACE\s*</li>\s*<li><strong>(\d+)</strong>&nbsp;&middot;&nbsp;([^<]+)</li>",
    re.I,
)
SECTOR_RE = re.compile(
    r"sector_id=[A-Z]\">\s*<strong>[A-Z]</strong>&nbsp;&middot;&nbsp;([^<]+)",
    re.I,
)
REASON = (
    "Copied stock_sector from the official LJSE instrument page after an exact ISIN match; "
    "NACE 64/65/66 and FINANCIAL AND INSURANCE ACTIVITIES mapped to Financials."
)
REPORT_FIELDNAMES = [
    "ticker",
    "exchange",
    "asset_type",
    "name",
    "isin",
    "ljse_isin",
    "ljse_sector",
    "ljse_nace",
    "ljse_nace_name",
    "sector_update",
    "decision",
]


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def map_nace(code: str, sector_text: str) -> str:
    digits = re.sub(r"\D", "", code or "")
    prefix = digits[:2]
    if prefix in NACE_PREFIX_MAP:
        return normalize_sector(NACE_PREFIX_MAP[prefix], "Stock")
    text = " ".join((sector_text or "").split()).upper()
    return normalize_sector(SECTOR_TEXT_MAP.get(text, ""), "Stock")


def parse_ljse_page(html: str) -> dict[str, str]:
    isin_match = PAGE_ISIN_RE.search(html or "")
    nace_match = NACE_RE.search(html or "")
    sector_match = SECTOR_RE.search(html or "")
    return {
        "ljse_isin": (isin_match.group(1) if isin_match else "").strip().upper(),
        "ljse_nace": (nace_match.group(1) if nace_match else "").strip(),
        "ljse_nace_name": " ".join((nace_match.group(2) if nace_match else "").split()),
        "ljse_sector": " ".join((sector_match.group(1) if sector_match else "").split()),
    }


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


def evaluate_row(row: dict[str, str], capture: dict[str, str] | None) -> dict[str, Any]:
    isin = (row.get("isin") or "").strip().upper()
    parsed = capture or {}
    base = {
        "ticker": row.get("ticker", ""),
        "exchange": row.get("exchange", ""),
        "asset_type": row.get("asset_type", ""),
        "name": row.get("name", ""),
        "isin": isin,
        "ljse_isin": parsed.get("ljse_isin", ""),
        "ljse_sector": parsed.get("ljse_sector", ""),
        "ljse_nace": parsed.get("ljse_nace", ""),
        "ljse_nace_name": parsed.get("ljse_nace_name", ""),
        "sector_update": "",
    }
    if (row.get("stock_sector") or row.get("sector") or "").strip():
        return {**base, "decision": "already_has_sector"}
    if not is_valid_isin(isin) or not isin.startswith("SI"):
        return {**base, "decision": "not_si_isin"}
    if not capture:
        return {**base, "decision": "missing_capture"}
    if parsed.get("ljse_isin") != isin:
        return {**base, "decision": "isin_mismatch"}
    sector = map_nace(parsed.get("ljse_nace", ""), parsed.get("ljse_sector", ""))
    if not sector:
        return {**base, "decision": "unsupported_nace_sector"}
    return {**base, "sector_update": sector, "decision": "accept"}


def build_metadata_updates(results: list[dict[str, Any]]) -> list[dict[str, str]]:
    return [
        {
            "ticker": result["ticker"],
            "exchange": result["exchange"],
            "field": "stock_sector",
            "decision": "update",
            "proposed_value": result["sector_update"],
            "confidence": "0.90",
            "reason": REASON + f" Source: {LJSE_QUOTE_URL.format(isin=result['isin'])}",
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


def fetch_ljse_page(isin: str, *, timeout_seconds: float = 30.0) -> str:
    response = requests.get(
        LJSE_QUOTE_URL.format(isin=isin),
        headers={"User-Agent": "Mozilla/5.0 free-ticker-database/3.0"},
        timeout=timeout_seconds,
    )
    response.raise_for_status()
    return response.text


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill missing stock sectors from official LJSE NACE/sector metadata after an exact ISIN match."
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
    rows = load_missing_rows(args.listings_csv, exchanges=exchanges)
    captures: dict[str, dict[str, str]] = {}
    if args.capture_json.exists() and not args.refresh:
        payload = json.loads(args.capture_json.read_text(encoding="utf-8"))
        if isinstance(payload, list):
            captures = {str(item.get("ljse_isin") or item.get("isin") or ""): item for item in payload if isinstance(item, dict)}
        elif isinstance(payload, dict):
            captures = payload
    results: list[dict[str, Any]] = []
    for row in rows:
        isin = (row.get("isin") or "").strip().upper()
        if is_valid_isin(isin) and isin.startswith("SI") and (args.refresh or isin not in captures):
            captures[isin] = parse_ljse_page(fetch_ljse_page(isin))
        results.append(evaluate_row(row, captures.get(isin)))
    updates = build_metadata_updates(results)
    args.capture_json.parent.mkdir(parents=True, exist_ok=True)
    args.capture_json.write_text(json.dumps(list(captures.values()), indent=2, sort_keys=True) + "\n", encoding="utf-8")
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

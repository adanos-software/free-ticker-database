from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.backfill_primary_same_issuer_sectors import issuer_key
from scripts.lib.dataio import merge_metadata_updates
from scripts.rebuild_dataset import LISTINGS_CSV, is_valid_isin, normalize_sector

CSE_LISTED_COMPANIES_URL = "https://thecse.com/api/webapi/listed-companies/"
DEFAULT_OUTPUT_DIR = ROOT / "data" / "cse_verification"
DEFAULT_LISTED_JSON = DEFAULT_OUTPUT_DIR / "listed_companies.json"
DEFAULT_REPORT_JSON = DEFAULT_OUTPUT_DIR / "listed_stock_sector_backfill.json"
DEFAULT_REPORT_CSV = DEFAULT_OUTPUT_DIR / "listed_stock_sector_backfill.csv"
DEFAULT_METADATA_UPDATES_CSV = ROOT / "data" / "review_overrides" / "metadata_updates.csv"
CSE_SECTOR_MAP = {
    "Mining": "Materials",
    "Oil and Gas": "Energy",
    "Technology": "Information Technology",
}
CSE_LISTED_STATUSES = frozenset({"Active", "Suspended"})
REASON = (
    "Copied stock_sector from the official CSE listed-companies directory after an exact "
    "issuer key of at least two tokens; CSE Mining/Oil and Gas/Technology mapped locally; "
    "Diversified Industries, Life Sciences and CleanTech left unmapped; Active and "
    "Suspended listings accepted, Delisted skipped."
)
REPORT_FIELDNAMES = [
    "ticker",
    "exchange",
    "asset_type",
    "name",
    "isin",
    "cse_symbol",
    "cse_name",
    "cse_sector",
    "sector_update",
    "decision",
]


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def map_cse_sector(sector: str) -> str:
    raw = (sector or "").strip()
    if not raw:
        return ""
    return normalize_sector(CSE_SECTOR_MAP.get(raw, ""), "Stock")


def key_usable_multi(key: str) -> bool:
    return len(key.split()) >= 2


def parse_listed_records(records: list[dict[str, Any]]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for record in records:
        if str(record.get("listing_market") or "").strip().upper() != "CSE":
            continue
        if str(record.get("status") or "").strip() not in CSE_LISTED_STATUSES:
            continue
        if str(record.get("security_type") or "").strip() != "Equity":
            continue
        name = str(record.get("security_name") or "").strip()
        key = issuer_key(name)
        if not key_usable_multi(key):
            continue
        rows.append(
            {
                "symbol": str(record.get("symbol") or "").strip(),
                "name": name,
                "sector": str(record.get("sector") or "").strip(),
                "issuer_key": key,
            }
        )
    return rows


def index_by_issuer_key(rows: list[dict[str, str]]) -> dict[str, list[dict[str, str]]]:
    indexed: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        indexed[row["issuer_key"]].append(row)
    return indexed


def download_listed_companies(url: str = CSE_LISTED_COMPANIES_URL, *, timeout_seconds: float = 60.0) -> list[dict[str, Any]]:
    response = requests.get(
        url,
        headers={"User-Agent": "Mozilla/5.0", "Referer": "https://thecse.com/listing/listed-companies/"},
        timeout=timeout_seconds,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list):
        raise ValueError("Unexpected CSE listed-companies payload")
    return payload


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


def evaluate_row(row: dict[str, str], by_key: dict[str, list[dict[str, str]]]) -> dict[str, Any]:
    isin = (row.get("isin") or "").strip().upper()
    base = {
        "ticker": row.get("ticker", ""),
        "exchange": row.get("exchange", ""),
        "asset_type": row.get("asset_type", ""),
        "name": row.get("name", ""),
        "isin": isin,
        "cse_symbol": "",
        "cse_name": "",
        "cse_sector": "",
        "sector_update": "",
    }
    if (row.get("stock_sector") or row.get("sector") or "").strip():
        return {**base, "decision": "already_has_sector"}
    if not is_valid_isin(isin) or not isin.startswith("CA"):
        return {**base, "decision": "not_ca_isin"}
    key = issuer_key(row.get("name") or "")
    if not key_usable_multi(key):
        return {**base, "decision": "short_issuer_key"}
    matches = by_key.get(key, [])
    if not matches:
        return {**base, "decision": "no_cse_match"}
    mapped: set[str] = set()
    unsupported = False
    chosen: dict[str, str] | None = None
    for item in matches:
        sector = map_cse_sector(item.get("sector") or "")
        if not (item.get("sector") or "").strip():
            continue
        if not sector:
            unsupported = True
            continue
        mapped.add(sector)
        if chosen is None:
            chosen = item
    if len(mapped) == 1 and chosen is not None:
        return {
            **base,
            "cse_symbol": chosen.get("symbol", ""),
            "cse_name": chosen.get("name", ""),
            "cse_sector": chosen.get("sector", ""),
            "sector_update": next(iter(mapped)),
            "decision": "accept",
        }
    if unsupported:
        return {**base, "decision": "unsupported_cse_sector"}
    if not mapped:
        return {**base, "decision": "empty_cse_sector"}
    return {**base, "decision": "ambiguous_cse_sector"}


def verify_rows(rows: list[dict[str, str]], listed_rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    indexed = index_by_issuer_key(listed_rows)
    return [evaluate_row(row, indexed) for row in rows]


def build_metadata_updates(results: list[dict[str, Any]]) -> list[dict[str, str]]:
    return [
        {
            "ticker": result["ticker"],
            "exchange": result["exchange"],
            "field": "stock_sector",
            "decision": "update",
            "proposed_value": result["sector_update"],
            "confidence": "0.86",
            "reason": REASON,
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


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill missing stock sectors from the official CSE listed-companies directory."
    )
    parser.add_argument("--listings-csv", type=Path, default=LISTINGS_CSV)
    parser.add_argument("--listed-json", type=Path, default=DEFAULT_LISTED_JSON)
    parser.add_argument("--listed-url", default=CSE_LISTED_COMPANIES_URL)
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
    if args.refresh or not args.listed_json.exists():
        payload = download_listed_companies(args.listed_url)
        args.listed_json.parent.mkdir(parents=True, exist_ok=True)
        args.listed_json.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    else:
        payload = json.loads(args.listed_json.read_text(encoding="utf-8"))
    listed_rows = parse_listed_records(payload)
    results = verify_rows(load_missing_rows(args.listings_csv, exchanges=exchanges), listed_rows)
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
                "csv_out": display_path(args.csv_out),
                "decision_counts": dict(Counter(result["decision"] for result in results)),
                "json_out": display_path(args.json_out),
                "listed_rows": len(listed_rows),
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()

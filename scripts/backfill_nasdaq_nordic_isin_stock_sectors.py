from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.lib.dataio import merge_metadata_updates
from scripts.rebuild_dataset import LISTINGS_CSV, is_valid_isin, normalize_sector

DEFAULT_SOURCE_DIR = ROOT / "data" / "nasdaq_nordic_verification"
DEFAULT_REPORT_JSON = DEFAULT_SOURCE_DIR / "isin_stock_sector_backfill.json"
DEFAULT_REPORT_CSV = DEFAULT_SOURCE_DIR / "isin_stock_sector_backfill.csv"
DEFAULT_METADATA_UPDATES_CSV = ROOT / "data" / "review_overrides" / "metadata_updates.csv"
NORDIC_SECTOR_MAP = {
    "Telecommunications": "Communication Services",
}
REASON = (
    "Copied stock_sector from the official Nasdaq Nordic shares directory after an exact "
    "ISIN match; Telecommunications mapped locally to Communication Services; unique GICS required."
)
REPORT_FIELDNAMES = [
    "ticker",
    "exchange",
    "asset_type",
    "name",
    "isin",
    "nordic_ticker",
    "nordic_name",
    "nordic_exchange",
    "nordic_sector",
    "sector_update",
    "decision",
]


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def map_nordic_sector(sector: str) -> str:
    raw = (sector or "").strip()
    if not raw:
        return ""
    return normalize_sector(NORDIC_SECTOR_MAP.get(raw, raw), "Stock")


def parse_nordic_share_rows(payload: Any) -> list[dict[str, str]]:
    records: list[Any]
    if isinstance(payload, list):
        records = payload
    elif isinstance(payload, dict):
        records = ((payload.get("data") or {}).get("instrumentListing") or {}).get("rows") or []
        if not records and isinstance(payload.get("data"), list):
            records = payload["data"]
    else:
        records = []
    rows: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for record in records:
        if not isinstance(record, dict):
            continue
        isin = str(record.get("isin") or record.get("ISIN") or "").strip().upper()
        if not is_valid_isin(isin):
            continue
        ticker = str(record.get("ticker") or record.get("symbol") or "").strip()
        key = (ticker, isin)
        if key in seen:
            continue
        seen.add(key)
        rows.append(
            {
                "ticker": ticker,
                "name": str(record.get("name") or record.get("fullName") or "").strip(),
                "exchange": str(record.get("exchange") or "").strip(),
                "isin": isin,
                "sector": str(record.get("sector") or "").strip(),
            }
        )
    return rows


def load_nordic_share_rows(paths: list[Path]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for path in paths:
        payload = json.loads(path.read_text(encoding="utf-8"))
        for row in parse_nordic_share_rows(payload):
            key = (row["ticker"], row["isin"])
            if key in seen:
                continue
            seen.add(key)
            rows.append(row)
    return rows


def index_by_isin(rows: list[dict[str, str]]) -> dict[str, list[dict[str, str]]]:
    indexed: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        indexed[row["isin"]].append(row)
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


def evaluate_row(row: dict[str, str], by_isin: dict[str, list[dict[str, str]]]) -> dict[str, Any]:
    isin = (row.get("isin") or "").strip().upper()
    base = {
        "ticker": row.get("ticker", ""),
        "exchange": row.get("exchange", ""),
        "asset_type": row.get("asset_type", ""),
        "name": row.get("name", ""),
        "isin": isin,
        "nordic_ticker": "",
        "nordic_name": "",
        "nordic_exchange": "",
        "nordic_sector": "",
        "sector_update": "",
    }
    if (row.get("stock_sector") or row.get("sector") or "").strip():
        return {**base, "decision": "already_has_sector"}
    if not is_valid_isin(isin):
        return {**base, "decision": "missing_isin"}
    matches = by_isin.get(isin, [])
    if not matches:
        return {**base, "decision": "no_nordic_match"}
    mapped: list[tuple[dict[str, str], str]] = []
    unsupported = False
    for item in matches:
        sector = map_nordic_sector(item.get("sector") or "")
        if not (item.get("sector") or "").strip():
            continue
        if not sector:
            unsupported = True
            continue
        mapped.append((item, sector))
    sectors = {sector for _, sector in mapped}
    if len(sectors) == 1:
        item, sector = mapped[0]
        return {
            **base,
            "nordic_ticker": item.get("ticker", ""),
            "nordic_name": item.get("name", ""),
            "nordic_exchange": item.get("exchange", ""),
            "nordic_sector": item.get("sector", ""),
            "sector_update": sector,
            "decision": "accept",
        }
    if len(sectors) > 1:
        return {**base, "decision": "ambiguous_nordic_sector"}
    if unsupported:
        return {**base, "decision": "unsupported_nordic_sector"}
    return {**base, "decision": "empty_nordic_sector"}


def verify_rows(rows: list[dict[str, str]], nordic_rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    indexed = index_by_isin(nordic_rows)
    return [evaluate_row(row, indexed) for row in rows]


def build_metadata_updates(results: list[dict[str, Any]]) -> list[dict[str, str]]:
    return [
        {
            "ticker": result["ticker"],
            "exchange": result["exchange"],
            "field": "stock_sector",
            "decision": "update",
            "proposed_value": result["sector_update"],
            "confidence": "0.90",
            "reason": REASON + f" Source {result['nordic_exchange']} {result['nordic_ticker']}.".strip(),
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
        description="Backfill missing stock sectors from official Nasdaq Nordic shares after exact ISIN match."
    )
    parser.add_argument("--listings-csv", type=Path, default=LISTINGS_CSV)
    parser.add_argument("--source-json", action="append", type=Path)
    parser.add_argument("--source-dir", type=Path, default=DEFAULT_SOURCE_DIR)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_REPORT_JSON)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_REPORT_CSV)
    parser.add_argument("--metadata-updates-csv", type=Path, default=DEFAULT_METADATA_UPDATES_CSV)
    parser.add_argument("--exchange", action="append")
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    exchanges = set(args.exchange) if args.exchange else {"FSX"}
    source_paths = list(args.source_json or [])
    if not source_paths:
        source_paths = sorted(args.source_dir.glob("*shares*.json"))
    if not source_paths:
        raise SystemExit(f"No Nasdaq Nordic shares JSON found in {display_path(args.source_dir)}")
    nordic_rows = load_nordic_share_rows(source_paths)
    results = verify_rows(load_missing_rows(args.listings_csv, exchanges=exchanges), nordic_rows)
    updates = build_metadata_updates(results)
    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.json_out.write_text(
        json.dumps([result for result in results if result["decision"] == "accept"], indent=2, sort_keys=True)
        + "\n",
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
                "nordic_rows": len(nordic_rows),
                "source_files": [display_path(path) for path in source_paths],
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()

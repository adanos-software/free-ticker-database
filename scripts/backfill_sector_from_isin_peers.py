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

from scripts.backfill_yahoo_generic_etf_names import merge_metadata_updates
from scripts.rebuild_dataset import LISTINGS_CSV, TICKERS_CSV, normalize_sector


DEFAULT_OUTPUT_DIR = ROOT / "data" / "isin_peer_sector_verification"
DEFAULT_REPORT_JSON = DEFAULT_OUTPUT_DIR / "sector_backfill.json"
DEFAULT_REPORT_CSV = DEFAULT_OUTPUT_DIR / "sector_backfill.csv"
DEFAULT_METADATA_UPDATES_CSV = ROOT / "data" / "review_overrides" / "metadata_updates.csv"
REPORT_FIELDNAMES = [
    "ticker",
    "exchange",
    "asset_type",
    "name",
    "isin",
    "sector_update",
    "peer_count",
    "peer_examples",
    "decision",
]


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def load_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def sector_value(row: dict[str, str]) -> str:
    if row.get("asset_type") == "Stock":
        return row.get("stock_sector", "") or row.get("sector", "")
    if row.get("asset_type") == "ETF":
        return row.get("etf_category", "") or row.get("sector", "")
    return row.get("sector", "")


def target_field_for_asset_type(asset_type: str) -> str:
    if asset_type == "ETF":
        return "etf_category"
    if asset_type == "Stock":
        return "stock_sector"
    return "sector"


def index_peer_sectors(listing_rows: list[dict[str, str]]) -> dict[tuple[str, str], list[dict[str, str]]]:
    indexed: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    for row in listing_rows:
        isin = row.get("isin", "").strip().upper()
        sector = sector_value(row).strip()
        if not isin or not sector:
            continue
        indexed[(isin, row["asset_type"])].append({**row, "sector": sector})
    return indexed


def evaluate_sector_peer_row(
    row: dict[str, str],
    peers: list[dict[str, str]],
) -> dict[str, Any]:
    base = {
        "ticker": row["ticker"],
        "exchange": row["exchange"],
        "asset_type": row["asset_type"],
        "name": row["name"],
        "isin": row.get("isin", "").strip().upper(),
        "sector_update": "",
        "peer_count": len(peers),
        "peer_examples": "; ".join(f"{peer['exchange']}::{peer['ticker']}={peer['sector']}" for peer in peers[:5]),
    }
    if sector_value(row).strip():
        return {**base, "decision": "already_has_sector"}
    if not base["isin"]:
        return {**base, "decision": "missing_isin"}
    if not peers:
        return {**base, "decision": "no_sector_peer"}

    normalized_sectors = {
        normalize_sector(peer["sector"], row["asset_type"])
        for peer in peers
        if normalize_sector(peer.get("sector", ""), row["asset_type"])
    }
    if len(normalized_sectors) != 1:
        return {**base, "decision": "conflicting_peer_sectors"}
    return {**base, "sector_update": next(iter(normalized_sectors)), "decision": "accept"}


def verify_sector_peers(
    ticker_rows: list[dict[str, str]],
    listing_rows: list[dict[str, str]],
    *,
    exchanges: set[str],
    asset_types: set[str],
) -> list[dict[str, Any]]:
    peer_sectors = index_peer_sectors(listing_rows)
    candidates = [
        row
        for row in ticker_rows
        if row["exchange"] in exchanges
        and row["asset_type"] in asset_types
        and not sector_value(row).strip()
        and row.get("isin", "").strip()
    ]
    return [
        evaluate_sector_peer_row(
            row,
            peer_sectors.get((row["isin"].strip().upper(), row["asset_type"]), []),
        )
        for row in candidates
    ]


def build_metadata_updates(results: list[dict[str, Any]]) -> list[dict[str, str]]:
    updates: list[dict[str, str]] = []
    for result in results:
        if result["decision"] != "accept":
            continue
        updates.append(
            {
                "ticker": result["ticker"],
                "exchange": result["exchange"],
                "field": target_field_for_asset_type(result["asset_type"]),
                "decision": "update",
                "proposed_value": result["sector_update"],
                "confidence": "0.88",
                "reason": "Sector/category propagated from same-ISIN listing peers after requiring the primary row to be missing sector and all same-asset peer sectors to normalize to one value.",
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
    parser = argparse.ArgumentParser(description="Backfill missing sectors from same-ISIN listing peers.")
    parser.add_argument("--json-out", type=Path, default=DEFAULT_REPORT_JSON)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_REPORT_CSV)
    parser.add_argument("--metadata-updates-csv", type=Path, default=DEFAULT_METADATA_UPDATES_CSV)
    parser.add_argument("--exchange", action="append", help="Restrict to one or more internal exchanges.")
    parser.add_argument("--asset-type", action="append", choices=["ETF", "Stock"], help="Restrict to one or more asset types.")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    ticker_rows = load_csv_rows(TICKERS_CSV)
    listing_rows = load_csv_rows(LISTINGS_CSV)
    exchanges = set(args.exchange or {row["exchange"] for row in ticker_rows})
    asset_types = set(args.asset_type or ["ETF", "Stock"])

    results = verify_sector_peers(
        ticker_rows,
        listing_rows,
        exchanges=exchanges,
        asset_types=asset_types,
    )
    if args.offset:
        results = results[args.offset :]
    if args.limit is not None:
        results = results[: args.limit]
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
                "asset_types": sorted(asset_types),
                "candidates": len(results),
                "decision_counts": dict(Counter(result["decision"] for result in results)),
                "exchanges": sorted(exchanges),
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

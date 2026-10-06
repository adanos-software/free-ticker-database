from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.fetch_exchange_masterfiles import SET_STOCK_SEARCH_SECTOR_MAP
from scripts.lib.dataio import merge_metadata_updates
from scripts.lib.normalize import ascii_fold
from scripts.rebuild_dataset import LISTINGS_CSV, is_valid_isin, normalize_sector

DEFAULT_SET_SEARCH_JSON = ROOT / "data" / "masterfiles" / "cache" / "set_stock_search.json"
DEFAULT_OUTPUT_DIR = ROOT / "data" / "set_verification"
DEFAULT_REPORT_JSON = DEFAULT_OUTPUT_DIR / "dual_stock_sector_backfill.json"
DEFAULT_REPORT_CSV = DEFAULT_OUTPUT_DIR / "dual_stock_sector_backfill.csv"
DEFAULT_METADATA_UPDATES_CSV = ROOT / "data" / "review_overrides" / "metadata_updates.csv"

# Thai local vs foreign/NVDR share-class lives in the last 3 ISIN chars.
THAI_NSIN_PREFIX_LEN = 9
THAI_LEGAL_RE = re.compile(
    r"\b(public company limited|public company ltd\.?|public co\.?,?\s*ltd\.?|pcl|plc\.?)\b",
    re.I,
)
ALIEN_MARKET_RE = re.compile(r"\(alien mkt\)", re.I)
TRAILING_F_RE = re.compile(r"\s+F$", re.I)
REASON_NSIN = (
    "Official SET listing shares this Thai issuer NSIN stem and exact issuer key after "
    "stripping PCL/legal-form noise; copied the SET stock_sector onto the dual listing."
)
REASON_SEARCH = (
    "Official SET stock-search row matched this dual by exact issuer key after stripping "
    "PCL/legal-form noise; accepted only after unique mapped GICS among common shares."
)
REPORT_FIELDNAMES = [
    "ticker",
    "exchange",
    "asset_type",
    "name",
    "isin",
    "set_ticker",
    "set_name",
    "set_isin",
    "sector_update",
    "match_path",
    "decision",
]


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def thai_issuer_key(name: str) -> str:
    value = ascii_fold(name or "")
    value = ALIEN_MARKET_RE.sub(" ", value)
    value = THAI_LEGAL_RE.sub(" ", value)
    value = TRAILING_F_RE.sub(" ", value)
    value = value.replace("&", " and ")
    value = re.sub(r"[^a-z0-9]+", " ", value.lower())
    return " ".join(value.split())


def map_set_search_sector(item: dict[str, Any]) -> str:
    raw = SET_STOCK_SEARCH_SECTOR_MAP.get(str(item.get("sector") or "").strip().upper()) or SET_STOCK_SEARCH_SECTOR_MAP.get(
        str(item.get("industry") or "").strip().upper()
    )
    return normalize_sector(raw or "", "Stock")


def is_set_common_share_symbol(symbol: str) -> bool:
    ticker = symbol.strip().upper()
    if not ticker:
        return False
    if "-" not in ticker:
        return True
    return ticker.endswith("-F")


def load_missing_th_dual_rows(
    path: Path = LISTINGS_CSV,
    *,
    exchanges: set[str] | None = None,
) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    selected: list[dict[str, str]] = []
    for row in rows:
        if row.get("asset_type") != "Stock":
            continue
        if row.get("exchange") == "SET":
            continue
        if exchanges is not None and row.get("exchange") not in exchanges:
            continue
        if (row.get("stock_sector") or row.get("sector") or "").strip():
            continue
        isin = (row.get("isin") or "").strip().upper()
        if not is_valid_isin(isin) or not isin.startswith("TH"):
            continue
        selected.append(row)
    return selected


def load_set_listing_rows(path: Path = LISTINGS_CSV) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return [
            row
            for row in csv.DictReader(handle)
            if row.get("exchange") == "SET" and row.get("asset_type") == "Stock"
        ]


def index_set_listings_by_nsin_prefix(rows: list[dict[str, str]]) -> dict[str, list[dict[str, str]]]:
    indexed: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        isin = (row.get("isin") or "").strip().upper()
        if len(isin) >= THAI_NSIN_PREFIX_LEN:
            indexed[isin[:THAI_NSIN_PREFIX_LEN]].append(row)
    return indexed


def load_set_stock_search_rows(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return [item for item in payload.get("securitySymbols", []) if isinstance(item, dict)]


def evaluate_row(
    row: dict[str, str],
    set_listings_by_prefix: dict[str, list[dict[str, str]]],
    search_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    isin = (row.get("isin") or "").strip().upper()
    base = {
        "ticker": row.get("ticker", ""),
        "exchange": row.get("exchange", ""),
        "asset_type": row.get("asset_type", ""),
        "name": row.get("name", ""),
        "isin": isin,
        "set_ticker": "",
        "set_name": "",
        "set_isin": "",
        "sector_update": "",
        "match_path": "",
    }
    if (row.get("stock_sector") or row.get("sector") or "").strip():
        return {**base, "decision": "already_has_sector"}
    if not is_valid_isin(isin) or not isin.startswith("TH"):
        return {**base, "decision": "not_th_isin"}
    if row.get("exchange") == "SET":
        return {**base, "decision": "skip_set_primary"}

    key = thai_issuer_key(row.get("name", ""))
    if len(key) < 3:
        return {**base, "decision": "empty_issuer_key"}

    nsin_matches = [
        listing
        for listing in set_listings_by_prefix.get(isin[:THAI_NSIN_PREFIX_LEN], [])
        if thai_issuer_key(listing.get("name", "")) == key
        and (listing.get("stock_sector") or listing.get("sector") or "").strip()
    ]
    nsin_sectors = {
        (listing.get("stock_sector") or listing.get("sector") or "").strip()
        for listing in nsin_matches
    }
    nsin_sectors.discard("")
    if len(nsin_sectors) == 1:
        match = nsin_matches[0]
        return {
            **base,
            "set_ticker": match.get("ticker", ""),
            "set_name": match.get("name", ""),
            "set_isin": (match.get("isin") or "").strip().upper(),
            "sector_update": next(iter(nsin_sectors)),
            "match_path": "nsin_stem",
            "decision": "accept",
        }
    if len(nsin_sectors) > 1:
        return {**base, "decision": "ambiguous_nsin_stem"}

    search_matches: list[tuple[dict[str, Any], str]] = []
    for item in search_rows:
        if str(item.get("securityType") or "").strip().upper() != "S":
            continue
        if str(item.get("market") or "").strip().upper() not in {"SET", "MAI"}:
            continue
        symbol = str(item.get("symbol") or "").strip().upper()
        if not is_set_common_share_symbol(symbol):
            continue
        if thai_issuer_key(str(item.get("nameEN") or "")) != key:
            continue
        mapped = map_set_search_sector(item)
        if mapped:
            search_matches.append((item, mapped))
    sectors = {mapped for _, mapped in search_matches}
    if len(sectors) == 1:
        item, mapped = search_matches[0]
        return {
            **base,
            "set_ticker": str(item.get("symbol") or "").strip().upper(),
            "set_name": str(item.get("nameEN") or ""),
            "set_isin": "",
            "sector_update": mapped,
            "match_path": "stock_search",
            "decision": "accept",
        }
    if len(sectors) > 1:
        return {**base, "decision": "ambiguous_stock_search"}
    return {**base, "decision": "no_set_match"}


def verify_rows(
    rows: list[dict[str, str]],
    set_listings: list[dict[str, str]],
    search_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    indexed = index_set_listings_by_nsin_prefix(set_listings)
    return [evaluate_row(row, indexed, search_rows) for row in rows]


def build_metadata_updates(results: list[dict[str, Any]]) -> list[dict[str, str]]:
    updates: list[dict[str, str]] = []
    for result in results:
        if result.get("decision") != "accept":
            continue
        reason = REASON_NSIN if result.get("match_path") == "nsin_stem" else REASON_SEARCH
        updates.append(
            {
                "ticker": result["ticker"],
                "exchange": result["exchange"],
                "field": "stock_sector",
                "decision": "update",
                "proposed_value": result["sector_update"],
                "confidence": "0.86" if result.get("match_path") == "nsin_stem" else "0.84",
                "reason": reason,
            }
        )
    return updates


def write_report_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=REPORT_FIELDNAMES, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in REPORT_FIELDNAMES})


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill missing stock sectors on Thai-ISIN duals from official SET listings."
    )
    parser.add_argument("--listings-csv", type=Path, default=LISTINGS_CSV)
    parser.add_argument("--set-search-json", type=Path, default=DEFAULT_SET_SEARCH_JSON)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_REPORT_JSON)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_REPORT_CSV)
    parser.add_argument("--metadata-updates-csv", type=Path, default=DEFAULT_METADATA_UPDATES_CSV)
    parser.add_argument("--exchange", action="append")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    exchanges = set(args.exchange) if args.exchange else None
    rows = load_missing_th_dual_rows(args.listings_csv, exchanges=exchanges)
    if args.offset:
        rows = rows[args.offset :]
    if args.limit is not None:
        rows = rows[: args.limit]
    if not args.set_search_json.exists():
        raise SystemExit(f"SET stock-search cache missing: {display_path(args.set_search_json)}")
    results = verify_rows(
        rows,
        load_set_listing_rows(args.listings_csv),
        load_set_stock_search_rows(args.set_search_json),
    )
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
                "candidates": len(results),
                "decision_counts": dict(Counter(result["decision"] for result in results)),
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

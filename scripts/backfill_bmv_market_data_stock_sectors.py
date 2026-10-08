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

from scripts.fetch_exchange_masterfiles import (
    BMV_MARKET_DATA_PROFILE_URL_TEMPLATE,
    BMV_MARKET_DATA_SECURITIES_URL,
    BMV_MARKET_DATA_STATS_URL_TEMPLATE,
    BMV_SEARCH_TOKEN_URL,
    OFFICIAL_SOURCES,
    bmv_search_text,
    fetch_bmv_market_data_page,
    normalize_bmv_stock_sector,
    parse_bmv_market_data_instruments_html,
    parse_bmv_market_data_profile_html,
    select_bmv_market_data_instrument,
    select_bmv_market_data_search_hit,
)
from scripts.lib.dataio import merge_metadata_updates
from scripts.rebuild_dataset import LISTINGS_CSV, is_valid_isin

DEFAULT_OUTPUT_DIR = ROOT / "data" / "bmv_verification"
DEFAULT_CAPTURE_JSON = DEFAULT_OUTPUT_DIR / "market_data_capture.json"
DEFAULT_REPORT_JSON = DEFAULT_OUTPUT_DIR / "listed_stock_sector_backfill.json"
DEFAULT_REPORT_CSV = DEFAULT_OUTPUT_DIR / "listed_stock_sector_backfill.csv"
DEFAULT_METADATA_UPDATES_CSV = ROOT / "data" / "review_overrides" / "metadata_updates.csv"
ALLOWED_EXCHANGES = frozenset({"BMV"})
REASON = (
    "Copied stock_sector from the official BMV issuer market-data profile after an exact "
    "listing ticker match to a unique issuer; ISIN required only when the official instrument "
    "row publishes one; unique mapped GICS required."
)
REPORT_FIELDNAMES = [
    "ticker",
    "exchange",
    "asset_type",
    "name",
    "isin",
    "official_ticker",
    "official_name",
    "official_isin",
    "official_sector",
    "official_issuer",
    "sector_update",
    "decision",
]


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def bmv_market_data_source():
    return next(item for item in OFFICIAL_SOURCES if item.key == "bmv_market_data_securities")


def map_bmv_sector(value: str) -> str:
    return normalize_bmv_stock_sector(value)


def load_missing_rows(path: Path = LISTINGS_CSV, *, exchanges: set[str] | None = None) -> list[dict[str, str]]:
    selected_exchanges = exchanges or set(ALLOWED_EXCHANGES)
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    selected: list[dict[str, str]] = []
    for row in rows:
        if row.get("asset_type") != "Stock":
            continue
        if row.get("exchange") not in selected_exchanges:
            continue
        if (row.get("stock_sector") or row.get("sector") or "").strip():
            continue
        selected.append(row)
    return selected


def index_by_ticker(rows: list[dict[str, str]]) -> dict[str, list[dict[str, str]]]:
    indexed: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        indexed[(row.get("ticker") or "").strip().upper()].append(row)
    return indexed


def fetch_official_bmv_rows(
    listings: list[dict[str, str]],
    *,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    source = bmv_market_data_source()
    session = session or requests.Session()
    token_response = session.get(BMV_SEARCH_TOKEN_URL, timeout=30)
    token_response.raise_for_status()
    access_token = str(token_response.json().get("response", {}).get("access_token", "")).strip()
    if not access_token:
        raise ValueError("BMV search token missing access_token")

    page_cache: dict[tuple[str, str], tuple[list[dict[str, str]], dict[str, str]]] = {}
    rows: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for listing_row in listings:
        selected_hit = select_bmv_market_data_search_hit(
            source,
            listing_row,
            access_token=access_token,
            session=session,
        )
        if selected_hit is None:
            continue
        issuer = bmv_search_text(selected_hit.get("cve_emisora")).upper()
        issuer_id = bmv_search_text(selected_hit.get("id_empresa"))
        if not issuer or not issuer_id:
            continue
        cache_key = (issuer, issuer_id)
        if cache_key not in page_cache:
            stats_url = BMV_MARKET_DATA_STATS_URL_TEMPLATE.format(issuer=issuer, issuer_id=issuer_id)
            profile_url = BMV_MARKET_DATA_PROFILE_URL_TEMPLATE.format(issuer=issuer, issuer_id=issuer_id)
            try:
                instruments = parse_bmv_market_data_instruments_html(
                    fetch_bmv_market_data_page(session, stats_url)
                )
                profile = parse_bmv_market_data_profile_html(
                    fetch_bmv_market_data_page(session, profile_url)
                )
            except requests.RequestException:
                page_cache[cache_key] = ([], {})
                continue
            page_cache[cache_key] = (instruments, profile)
        instruments, profile = page_cache[cache_key]
        instrument = select_bmv_market_data_instrument(listing_row, selected_hit, instruments)
        if instrument is None:
            continue
        ticker = (listing_row.get("ticker") or "").strip().upper()
        official_isin = (instrument.get("isin") or "").strip().upper()
        key = (ticker, official_isin)
        if key in seen:
            continue
        seen.add(key)
        rows.append(
            {
                "ticker": ticker,
                "name": (
                    bmv_search_text(selected_hit.get("razon_social"))
                    or bmv_search_text(selected_hit.get("instrumento"))
                    or listing_row.get("name", "")
                ),
                "isin": official_isin,
                "sector": profile.get("SECTOR", ""),
                "issuer": issuer,
            }
        )
    return rows


def evaluate_row(row: dict[str, str], by_ticker: dict[str, list[dict[str, str]]]) -> dict[str, Any]:
    isin = (row.get("isin") or "").strip().upper()
    ticker = (row.get("ticker") or "").strip().upper()
    exchange = (row.get("exchange") or "").strip()
    officials = by_ticker.get(ticker) or []
    official_with_isin = [item for item in officials if is_valid_isin(item.get("isin") or "")]
    matching = [item for item in official_with_isin if item.get("isin") == isin]
    chosen_pool = matching or (officials if not official_with_isin else [])
    official = chosen_pool[0] if chosen_pool else (officials[0] if officials else {})
    base = {
        "ticker": row.get("ticker", ""),
        "exchange": exchange,
        "asset_type": row.get("asset_type", ""),
        "name": row.get("name", ""),
        "isin": isin,
        "official_ticker": official.get("ticker", ""),
        "official_name": official.get("name", ""),
        "official_isin": official.get("isin", ""),
        "official_sector": official.get("sector", ""),
        "official_issuer": official.get("issuer", ""),
        "sector_update": "",
    }
    if (row.get("stock_sector") or row.get("sector") or "").strip():
        return {**base, "decision": "already_has_sector"}
    if exchange not in ALLOWED_EXCHANGES:
        return {**base, "decision": "not_bmv_exchange"}
    if not is_valid_isin(isin):
        return {**base, "decision": "missing_or_invalid_isin"}
    if not officials:
        return {**base, "decision": "no_official_match"}
    if official_with_isin and not matching:
        return {**base, "decision": "isin_mismatch"}
    mapped = {map_bmv_sector(item.get("sector") or "") for item in (matching or officials)}
    mapped.discard("")
    if len(mapped) == 1:
        chosen = (matching or officials)[0]
        return {
            **base,
            "official_ticker": chosen.get("ticker", ""),
            "official_name": chosen.get("name", ""),
            "official_isin": chosen.get("isin", ""),
            "official_sector": chosen.get("sector", ""),
            "official_issuer": chosen.get("issuer", ""),
            "sector_update": next(iter(mapped)),
            "decision": "accept",
        }
    if any((item.get("sector") or "").strip() for item in (matching or officials)):
        return {**base, "decision": "unsupported_sector"}
    return {**base, "decision": "empty_sector"}


def build_metadata_updates(results: list[dict[str, Any]]) -> list[dict[str, str]]:
    return [
        {
            "ticker": result["ticker"],
            "exchange": result["exchange"],
            "field": "stock_sector",
            "decision": "update",
            "proposed_value": result["sector_update"],
            "confidence": "0.90",
            "reason": REASON + f" Source: {BMV_MARKET_DATA_SECURITIES_URL}",
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
        description="Backfill missing BMV stock sectors from official issuer market-data profiles."
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
    exchanges = set(args.exchange) if args.exchange else set(ALLOWED_EXCHANGES)
    missing_rows = load_missing_rows(args.listings_csv, exchanges=exchanges)
    if args.refresh or not args.capture_json.exists():
        official_rows = fetch_official_bmv_rows(missing_rows)
        args.capture_json.parent.mkdir(parents=True, exist_ok=True)
        args.capture_json.write_text(
            json.dumps(official_rows, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    else:
        official_rows = json.loads(args.capture_json.read_text(encoding="utf-8"))
    results = [evaluate_row(row, index_by_ticker(official_rows)) for row in missing_rows]
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
                "official_rows": len(official_rows),
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()

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

EURONEXT_SEARCH_URL = "https://live.euronext.com/en/instrumentSearch/searchJSON"
EURONEXT_ICB_URL = "https://live.euronext.com/en/ajax/getFactsheetInfoBlock/STOCK/{product}/fs_icb_block"
EURONEXT_PRODUCT_URL = "https://live.euronext.com/en/product/equities/{product}/market-information"
DEFAULT_OUTPUT_DIR = ROOT / "data" / "euronext_verification"
DEFAULT_CAPTURE_JSON = DEFAULT_OUTPUT_DIR / "fsx_icb_capture.json"
DEFAULT_REPORT_JSON = DEFAULT_OUTPUT_DIR / "icb_stock_sector_backfill.json"
DEFAULT_REPORT_CSV = DEFAULT_OUTPUT_DIR / "icb_stock_sector_backfill.csv"
DEFAULT_METADATA_UPDATES_CSV = ROOT / "data" / "review_overrides" / "metadata_updates.csv"
EURONEXT_PREFIXES = frozenset({"BE", "CY", "FR", "IE", "IT", "LU", "MT", "NL", "NO", "PT", "SG"})
ICB_INDUSTRY_MAP = {
    "Basic Materials": "Materials",
    "Technology": "Information Technology",
    "Telecommunications": "Communication Services",
}
ICB_INDUSTRY_RE = re.compile(
    r"<td>\s*Industry\s*</td>\s*<td>\s*<strong>\s*\d+\s*,\s*([^<]+?)\s*</strong>",
    re.I,
)
REASON = (
    "Copied stock_sector from the official Euronext ICB industry after an exact ISIN "
    "match; Telecommunications mapped to Communication Services and Basic Materials to Materials."
)
REPORT_FIELDNAMES = [
    "ticker",
    "exchange",
    "asset_type",
    "name",
    "isin",
    "euronext_symbol",
    "euronext_name",
    "euronext_mic",
    "icb_industry",
    "sector_update",
    "decision",
]


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def map_icb_industry(industry: str) -> str:
    raw = (industry or "").strip()
    if not raw:
        return ""
    return normalize_sector(ICB_INDUSTRY_MAP.get(raw, raw), "Stock")


def parse_icb_industry(html: str) -> str:
    match = ICB_INDUSTRY_RE.search(html or "")
    return " ".join((match.group(1) if match else "").split())


def parse_product_instrument(html: str) -> dict[str, str]:
    match = re.search(r'data-drupal-selector="drupal-settings-json">(.*?)</script>', html or "", re.S)
    if not match:
        return {}
    payload = json.loads(match.group(1))
    instrument = payload.get("custom", {}).get("instrument") or {}
    if not isinstance(instrument, dict):
        return {}
    isin = str(instrument.get("isin") or "").strip().upper()
    mic = str(instrument.get("mic") or "").strip().upper()
    if not is_valid_isin(isin) or not mic:
        return {}
    return {
        "isin": isin,
        "mic": mic,
        "name": str(instrument.get("name") or "").strip(),
        "symbol": str(instrument.get("symbol") or "").strip(),
    }


def euronext_headers() -> dict[str, str]:
    return {
        "User-Agent": "Mozilla/5.0 free-ticker-database/3.0",
        "X-Requested-With": "XMLHttpRequest",
    }


def search_euronext(isin: str, *, timeout_seconds: float = 30.0) -> list[dict[str, str]]:
    response = requests.get(
        EURONEXT_SEARCH_URL,
        params={"q": isin},
        headers=euronext_headers(),
        timeout=timeout_seconds,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list):
        return []
    hits: list[dict[str, str]] = []
    for row in payload:
        if not isinstance(row, dict):
            continue
        hit_isin = str(row.get("isin") or "").strip().upper()
        mic = str(row.get("mic") or "").strip().upper()
        if hit_isin != isin or not mic:
            continue
        hits.append(
            {
                "isin": hit_isin,
                "mic": mic,
                "name": str(row.get("name") or "").strip(),
                "symbol": str(row.get("symbol") or "").strip(),
            }
        )
    return hits


def load_oslo_instrument(isin: str, *, timeout_seconds: float = 30.0) -> dict[str, str]:
    product = f"{isin}-XOSL"
    response = requests.get(
        EURONEXT_PRODUCT_URL.format(product=product),
        headers={"User-Agent": "Mozilla/5.0 free-ticker-database/3.0"},
        timeout=timeout_seconds,
    )
    response.raise_for_status()
    instrument = parse_product_instrument(response.text)
    if instrument.get("isin") != isin:
        return {}
    return instrument


def fetch_icb_html(isin: str, mic: str, *, timeout_seconds: float = 30.0) -> str:
    response = requests.get(
        EURONEXT_ICB_URL.format(product=f"{isin}-{mic}"),
        headers=euronext_headers(),
        timeout=timeout_seconds,
    )
    response.raise_for_status()
    return response.text


def resolve_hits(isin: str, search_hits: list[dict[str, str]], oslo_instrument: dict[str, str] | None) -> list[dict[str, str]]:
    if search_hits:
        return search_hits
    if oslo_instrument and oslo_instrument.get("isin") == isin:
        return [oslo_instrument]
    return []


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


def evaluate_row(row: dict[str, str], hits: list[dict[str, str]], industries: list[str]) -> dict[str, Any]:
    isin = (row.get("isin") or "").strip().upper()
    chosen = hits[0] if hits else {}
    base = {
        "ticker": row.get("ticker", ""),
        "exchange": row.get("exchange", ""),
        "asset_type": row.get("asset_type", ""),
        "name": row.get("name", ""),
        "isin": isin,
        "euronext_symbol": chosen.get("symbol", ""),
        "euronext_name": chosen.get("name", ""),
        "euronext_mic": chosen.get("mic", ""),
        "icb_industry": industries[0] if len(set(industries)) == 1 else "",
        "sector_update": "",
    }
    if (row.get("stock_sector") or row.get("sector") or "").strip():
        return {**base, "decision": "already_has_sector"}
    if not is_valid_isin(isin):
        return {**base, "decision": "missing_isin"}
    if isin[:2] not in EURONEXT_PREFIXES:
        return {**base, "decision": "not_euronext_isin"}
    if not hits:
        return {**base, "decision": "no_euronext_match"}
    mapped = {map_icb_industry(item) for item in industries if item.strip()}
    mapped.discard("")
    if len(mapped) == 1:
        return {**base, "icb_industry": next(iter(industries)), "sector_update": next(iter(mapped)), "decision": "accept"}
    if any(item.strip() and not map_icb_industry(item) for item in industries):
        return {**base, "decision": "unsupported_icb_industry"}
    if not any(item.strip() for item in industries):
        return {**base, "decision": "missing_icb_industry"}
    return {**base, "decision": "ambiguous_icb_industry"}


def build_metadata_updates(results: list[dict[str, Any]]) -> list[dict[str, str]]:
    return [
        {
            "ticker": result["ticker"],
            "exchange": result["exchange"],
            "field": "stock_sector",
            "decision": "update",
            "proposed_value": result["sector_update"],
            "confidence": "0.90",
            "reason": REASON + f" Source: {EURONEXT_PRODUCT_URL.format(product=result['isin'] + '-' + result['euronext_mic'])}",
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


def capture_row(isin: str, capture_by_isin: dict[str, Any]) -> tuple[list[dict[str, str]], list[str]]:
    cached = capture_by_isin.get(isin) or {}
    hits = list(cached.get("hits") or [])
    industries = list(cached.get("industries") or [])
    return hits, industries


def refresh_capture(isin: str) -> dict[str, Any]:
    hits = search_euronext(isin)
    if not hits and isin.startswith("NO"):
        oslo = load_oslo_instrument(isin)
        hits = resolve_hits(isin, [], oslo)
    industries: list[str] = []
    for hit in hits:
        industries.append(parse_icb_industry(fetch_icb_html(isin, hit["mic"])))
    return {"isin": isin, "hits": hits, "industries": industries}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill missing stock sectors from official Euronext ICB industry after an exact ISIN match."
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
    capture_by_isin: dict[str, Any] = {}
    if args.capture_json.exists() and not args.refresh:
        payload = json.loads(args.capture_json.read_text(encoding="utf-8"))
        if isinstance(payload, list):
            capture_by_isin = {str(item.get("isin") or ""): item for item in payload if isinstance(item, dict)}
        elif isinstance(payload, dict):
            capture_by_isin = payload
    captures: list[dict[str, Any]] = []
    results: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        isin = (row.get("isin") or "").strip().upper()
        if is_valid_isin(isin) and isin[:2] in EURONEXT_PREFIXES and isin not in seen:
            seen.add(isin)
            if args.refresh or isin not in capture_by_isin:
                capture_by_isin[isin] = refresh_capture(isin)
            captures.append(capture_by_isin[isin])
        hits, industries = capture_row(isin, capture_by_isin)
        results.append(evaluate_row(row, hits, industries))
    updates = build_metadata_updates(results)
    args.capture_json.parent.mkdir(parents=True, exist_ok=True)
    args.capture_json.write_text(json.dumps(captures, indent=2, sort_keys=True) + "\n", encoding="utf-8")
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

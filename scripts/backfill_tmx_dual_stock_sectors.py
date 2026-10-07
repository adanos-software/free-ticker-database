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

from scripts.backfill_tmx_stock_sectors import (
    TmxSectorRow,
    display_path,
    download_tmx_issuers_xlsx,
    load_tmx_sector_rows,
    normalize_tmx_sector,
)
from scripts.lib.dataio import merge_metadata_updates
from scripts.lib.normalize import ascii_fold
from scripts.rebuild_dataset import LISTINGS_CSV, is_valid_isin

DEFAULT_OUTPUT_DIR = ROOT / "data" / "tmx_verification"
DEFAULT_REPORT_JSON = DEFAULT_OUTPUT_DIR / "dual_stock_sector_backfill.json"
DEFAULT_REPORT_CSV = DEFAULT_OUTPUT_DIR / "dual_stock_sector_backfill.csv"
DEFAULT_METADATA_UPDATES_CSV = ROOT / "data" / "review_overrides" / "metadata_updates.csv"
DEFAULT_SOURCE_XLSX = ROOT / "data" / "masterfiles" / "cache" / "tmx_listed_issuers.xlsx"
TMX_PRIMARY_EXCHANGES = frozenset({"TSX", "TSXV"})
LEGAL_RE = re.compile(
    r"\b(incorporated|corporation|company|limited|ltd|llc|inc|corp|plc)\b",
    re.I,
)
REASON = (
    "Official TMX TSX/TSXV issuer workbook matched this CA-ISIN dual by exact issuer key "
    "after stripping legal-form noise, or by unique exact full name; accepted only after "
    "unique mapped GICS."
)
REPORT_FIELDNAMES = [
    "ticker",
    "exchange",
    "asset_type",
    "name",
    "isin",
    "tmx_symbol",
    "tmx_name",
    "tmx_exchange",
    "tmx_sector",
    "sector_update",
    "decision",
]


def tmx_issuer_key(name: str) -> str:
    value = ascii_fold(name or "")
    value = value.replace("&", " and ")
    value = re.sub(r"\([^)]*\)", " ", value)
    value = LEGAL_RE.sub(" ", value)
    value = re.sub(r"[^a-z0-9]+", " ", value.lower())
    return " ".join(value.split())


def exact_name_key(name: str) -> str:
    return " ".join((name or "").casefold().split())


def load_missing_ca_dual_rows(
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
        if row.get("exchange") in TMX_PRIMARY_EXCHANGES:
            continue
        if exchanges is not None and row.get("exchange") not in exchanges:
            continue
        if (row.get("stock_sector") or "").strip():
            continue
        isin = (row.get("isin") or "").strip().upper()
        if not is_valid_isin(isin) or not isin.startswith("CA"):
            continue
        selected.append(row)
    return selected


def index_tmx_by_issuer_key(rows: list[TmxSectorRow]) -> dict[str, list[TmxSectorRow]]:
    indexed: dict[str, list[TmxSectorRow]] = defaultdict(list)
    for row in rows:
        key = tmx_issuer_key(row.name)
        if len(key.split()) >= 2:
            indexed[key].append(row)
    return indexed


def index_tmx_by_exact_name(rows: list[TmxSectorRow]) -> dict[str, list[TmxSectorRow]]:
    indexed: dict[str, list[TmxSectorRow]] = defaultdict(list)
    for row in rows:
        key = exact_name_key(row.name)
        if key:
            indexed[key].append(row)
    return indexed


def _accept_or_ambiguous(base: dict[str, str], matches: list[TmxSectorRow]) -> dict[str, Any]:
    sectors = {normalize_tmx_sector(match.sector) for match in matches}
    sectors.discard("")
    if len(sectors) != 1:
        match = matches[0]
        return {
            **base,
            "tmx_symbol": match.symbol,
            "tmx_name": match.name,
            "tmx_exchange": match.exchange,
            "tmx_sector": "|".join(sorted({item.sector for item in matches})),
            "decision": "unsupported_or_ambiguous_tmx_sector",
        }
    match = matches[0]
    return {
        **base,
        "tmx_symbol": match.symbol,
        "tmx_name": match.name,
        "tmx_exchange": match.exchange,
        "tmx_sector": match.sector,
        "sector_update": next(iter(sectors)),
        "decision": "accept",
    }


def evaluate_row(
    row: dict[str, str],
    matches: list[TmxSectorRow],
    exact_name_matches: list[TmxSectorRow] | None = None,
) -> dict[str, Any]:
    base = {
        "ticker": row.get("ticker", ""),
        "exchange": row.get("exchange", ""),
        "asset_type": row.get("asset_type", ""),
        "name": row.get("name", ""),
        "isin": (row.get("isin") or "").strip().upper(),
        "tmx_symbol": "",
        "tmx_name": "",
        "tmx_exchange": "",
        "tmx_sector": "",
        "sector_update": "",
    }
    key = tmx_issuer_key(row.get("name", ""))
    if len(key.split()) >= 2:
        matches = [match for match in matches if tmx_issuer_key(match.name) == key]
        if not matches:
            return {**base, "decision": "no_tmx_name_match"}
        return _accept_or_ambiguous(base, matches)
    exact_hits = [
        match
        for match in (exact_name_matches or [])
        if exact_name_key(match.name) == exact_name_key(row.get("name", ""))
    ]
    if not exact_hits:
        return {**base, "decision": "short_issuer_key"}
    return _accept_or_ambiguous(base, exact_hits)


def evaluate_rows(target_rows: list[dict[str, str]], source_rows: list[TmxSectorRow]) -> list[dict[str, Any]]:
    indexed = index_tmx_by_issuer_key(source_rows)
    exact_indexed = index_tmx_by_exact_name(source_rows)
    return [
        evaluate_row(
            row,
            indexed.get(tmx_issuer_key(row.get("name", "")), []),
            exact_indexed.get(exact_name_key(row.get("name", "")), []),
        )
        for row in target_rows
    ]


def build_metadata_updates(results: list[dict[str, Any]]) -> list[dict[str, str]]:
    return [
        {
            "ticker": result["ticker"],
            "exchange": result["exchange"],
            "field": "stock_sector",
            "decision": "update",
            "proposed_value": result["sector_update"],
            "confidence": "0.80",
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
        description="Backfill missing stock sectors on CA-ISIN duals from the official TMX issuer workbook."
    )
    parser.add_argument("--listings-csv", type=Path, default=LISTINGS_CSV)
    parser.add_argument("--source-xlsx", type=Path, default=DEFAULT_SOURCE_XLSX)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_REPORT_JSON)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_REPORT_CSV)
    parser.add_argument("--metadata-updates-csv", type=Path, default=DEFAULT_METADATA_UPDATES_CSV)
    parser.add_argument("--exchange", action="append")
    parser.add_argument("--timeout-seconds", type=float, default=30.0)
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    exchanges = set(args.exchange) if args.exchange else None
    workbook_bytes = (
        args.source_xlsx.read_bytes()
        if args.source_xlsx.exists()
        else download_tmx_issuers_xlsx(
            "https://www.tsx.com/en/resource/571",
            args.timeout_seconds,
        )
    )
    results = evaluate_rows(
        load_missing_ca_dual_rows(args.listings_csv, exchanges=exchanges),
        load_tmx_sector_rows(workbook_bytes),
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
                "accepted_sector_updates": len(updates),
                "applied": args.apply,
                "candidates": len(results),
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

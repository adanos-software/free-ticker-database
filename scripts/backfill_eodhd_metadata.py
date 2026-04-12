from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.backfill_xtb_omi_isins import names_match
from scripts.backfill_yahoo_generic_etf_names import merge_metadata_updates
from scripts.rebuild_dataset import TICKERS_CSV, is_valid_isin


DEFAULT_OUTPUT_DIR = ROOT / "data" / "eodhd_verification"
DEFAULT_REPORT_JSON = DEFAULT_OUTPUT_DIR / "metadata_backfill.json"
DEFAULT_REPORT_CSV = DEFAULT_OUTPUT_DIR / "metadata_backfill.csv"
DEFAULT_METADATA_UPDATES_CSV = ROOT / "data" / "review_overrides" / "metadata_updates.csv"

EODHD_BASE_URL = "https://eodhd.com/api"
EODHD_EXCHANGE_CODES: dict[str, str] = {
    "ASX": "AU",
    "B3": "SA",
    "BATS": "US",
    "BMV": "MX",
    "Bursa": "KLSE",
    "EGX": "EGX",
    "HOSE": "VN",
    "IDX": "JK",
    "KOSDAQ": "KQ",
    "KRX": "KO",
    "LSE": "LSE",
    "NASDAQ": "US",
    "NEO": "NEO",
    "NYSE": "US",
    "NYSE ARCA": "US",
    "NYSE MKT": "US",
    "OTC": "US",
    "SET": "BK",
    "SIX": "SW",
    "SSE": "SHG",
    "STO": "ST",
    "SZSE": "SHE",
    "TASE": "TA",
    "TPEX": "TWO",
    "TSX": "TO",
    "TSXV": "V",
    "TWSE": "TW",
    "WSE": "WAR",
    "XETRA": "XETRA",
}
EODHD_MARKET_CODES: dict[str, set[str]] = {
    "ASX": {"AU"},
    "B3": {"SA"},
    "BATS": {"BATS"},
    "BMV": {"MX"},
    "Bursa": {"KLSE"},
    "EGX": {"EGX"},
    "HOSE": {"VN"},
    "IDX": {"JK"},
    "KOSDAQ": {"KQ"},
    "KRX": {"KO"},
    "LSE": {"LSE"},
    "NASDAQ": {"NASDAQ"},
    "NEO": {"NEO"},
    "NYSE": {"NYSE"},
    "NYSE ARCA": {"NYSE ARCA", "NYSEARCA"},
    "NYSE MKT": {"AMEX", "NYSE MKT"},
    "OTC": {"OTC", "OTCBB", "OTCCE", "OTCGREY", "OTCMKTS", "OTCQB", "OTCQX", "PINK"},
    "SET": {"BK"},
    "SIX": {"SW"},
    "SSE": {"SHG"},
    "STO": {"ST"},
    "SZSE": {"SHE"},
    "TASE": {"TA"},
    "TPEX": {"TWO"},
    "TSX": {"TO"},
    "TSXV": {"V"},
    "TWSE": {"TW"},
    "WSE": {"WAR"},
    "XETRA": {"XETRA"},
}
EXPECTED_ISIN_PREFIXES: dict[str, tuple[str, ...]] = {
    "ASX": ("AU", "NZ"),
    "B3": ("BR",),
    "BATS": ("US", "IE", "LU"),
    "BMV": ("MX", "US", "CA", "IE", "LU", "GB", "DE", "FR"),
    "Bursa": ("MY",),
    "EGX": ("EG",),
    "HOSE": ("VN",),
    "IDX": ("ID",),
    "KOSDAQ": ("KR",),
    "KRX": ("KR",),
    "LSE": ("GB", "IE", "LU", "JE", "GG", "IM", "US", "DE", "FR", "NL"),
    "NASDAQ": ("US", "KY", "VG", "NL", "IE", "LU", "GB", "CA", "IL", "BM", "MH"),
    "NEO": ("CA",),
    "NYSE": ("US",),
    "NYSE ARCA": ("US", "IE", "LU", "GB", "CA"),
    "NYSE MKT": ("US", "CA", "IL"),
    "OTC": ("US", "CA", "GB", "AU", "DE", "FR", "NL", "JP", "CH", "SE", "NO", "FI", "BR", "MX", "MY", "CN", "HK"),
    "SET": ("TH",),
    "SIX": ("CH", "IE", "LU", "US", "DE", "FR"),
    "SSE": ("CN",),
    "STO": ("SE",),
    "SZSE": ("CN",),
    "TASE": ("IL",),
    "TPEX": ("TW",),
    "TSX": ("CA",),
    "TSXV": ("CA",),
    "TWSE": ("TW",),
    "WSE": ("PL",),
    "XETRA": ("DE", "IE", "LU", "FR", "US", "GB", "NL"),
}
EODHD_TYPES: dict[str, set[str]] = {
    "ETF": {"ETF"},
    "Stock": {"Common Stock"},
}
REPORT_FIELDNAMES = [
    "ticker",
    "exchange",
    "asset_type",
    "name",
    "eodhd_code",
    "eodhd_exchange",
    "eodhd_name",
    "eodhd_type",
    "eodhd_isin",
    "number_tokens_match",
    "name_match",
    "decision",
]


@dataclass(frozen=True)
class EodhdSymbol:
    code: str
    name: str
    exchange: str
    asset_type: str
    isin: str


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def normalized_ticker_key(value: str) -> str:
    return value.strip().upper().replace("-", ".").replace("/", ".")


def significant_numbers(value: str) -> set[str]:
    return set(re.findall(r"\d+", value))


def strict_names_match(source_name: str, target_name: str) -> bool:
    return significant_numbers(source_name) == significant_numbers(target_name) and names_match(source_name, target_name)


def expected_isin_prefix_match(exchange: str, isin: str) -> bool:
    prefixes = EXPECTED_ISIN_PREFIXES.get(exchange)
    return prefixes is None or isin.startswith(prefixes)


def load_missing_isin_rows(
    *,
    exchanges: set[str],
    asset_types: set[str],
    tickers_csv: Path = TICKERS_CSV,
) -> list[dict[str, str]]:
    with tickers_csv.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    return [
        row
        for row in rows
        if row["exchange"] in exchanges
        and row["asset_type"] in asset_types
        and not row.get("isin", "").strip()
    ]


def fetch_eodhd_symbol_list(exchange_code: str, api_token: str, *, timeout_seconds: float) -> list[dict[str, Any]]:
    params = urllib.parse.urlencode({"api_token": api_token, "fmt": "json"})
    url = f"{EODHD_BASE_URL}/exchange-symbol-list/{exchange_code}?{params}"
    with urllib.request.urlopen(url, timeout=timeout_seconds) as response:
        payload = json.load(response)
    if not isinstance(payload, list):
        raise ValueError(f"EODHD returned non-list payload for {exchange_code}: {payload!r}")
    return payload


def parse_eodhd_symbol(row: dict[str, Any]) -> EodhdSymbol:
    return EodhdSymbol(
        code=str(row.get("Code") or "").strip(),
        name=str(row.get("Name") or "").strip(),
        exchange=str(row.get("Exchange") or "").strip(),
        asset_type=str(row.get("Type") or "").strip(),
        isin=str(row.get("Isin") or "").strip().upper(),
    )


def index_eodhd_symbols(
    rows_by_exchange_code: dict[str, list[dict[str, Any]]],
    exchanges: set[str],
) -> dict[tuple[str, str], list[EodhdSymbol]]:
    indexed: dict[tuple[str, str], list[EodhdSymbol]] = defaultdict(list)
    for exchange in exchanges:
        exchange_code = EODHD_EXCHANGE_CODES[exchange]
        allowed_market_codes = EODHD_MARKET_CODES[exchange]
        for raw_row in rows_by_exchange_code[exchange_code]:
            symbol = parse_eodhd_symbol(raw_row)
            if symbol.exchange not in allowed_market_codes:
                continue
            indexed[(exchange, normalized_ticker_key(symbol.code))].append(symbol)
    return indexed


def evaluate_eodhd_row(
    row: dict[str, str],
    candidates: list[EodhdSymbol],
    *,
    existing_isins: set[str] | None = None,
    allow_existing_isin: bool = False,
) -> dict[str, Any]:
    existing_isins = existing_isins or set()
    base = {
        "ticker": row["ticker"],
        "exchange": row["exchange"],
        "asset_type": row["asset_type"],
        "name": row["name"],
        "eodhd_code": "",
        "eodhd_exchange": "",
        "eodhd_name": "",
        "eodhd_type": "",
        "eodhd_isin": "",
        "number_tokens_match": False,
        "name_match": False,
    }
    if not candidates:
        return {**base, "decision": "no_eodhd_match"}
    if len(candidates) > 1:
        return {**base, "decision": "ambiguous_eodhd_match"}

    candidate = candidates[0]
    base.update(
        {
            "eodhd_code": candidate.code,
            "eodhd_exchange": candidate.exchange,
            "eodhd_name": candidate.name,
            "eodhd_type": candidate.asset_type,
            "eodhd_isin": candidate.isin,
            "number_tokens_match": significant_numbers(candidate.name) == significant_numbers(row["name"]),
            "name_match": strict_names_match(candidate.name, row["name"]),
        }
    )

    if candidate.asset_type not in EODHD_TYPES.get(row["asset_type"], set()):
        return {**base, "decision": "asset_type_mismatch"}
    if not candidate.isin:
        return {**base, "decision": "missing_isin"}
    if not is_valid_isin(candidate.isin):
        return {**base, "decision": "invalid_isin"}
    if not expected_isin_prefix_match(row["exchange"], candidate.isin):
        return {**base, "decision": "isin_country_mismatch"}
    if not allow_existing_isin and candidate.isin in existing_isins:
        return {**base, "decision": "isin_already_present"}
    if not base["number_tokens_match"]:
        return {**base, "decision": "number_token_mismatch"}
    if not base["name_match"]:
        return {**base, "decision": "name_mismatch"}
    return {**base, "decision": "accept"}


def verify_eodhd_isins(
    rows: list[dict[str, str]],
    rows_by_exchange_code: dict[str, list[dict[str, Any]]],
    *,
    exchanges: set[str],
    existing_isins: set[str] | None = None,
    allow_existing_isin: bool = False,
) -> list[dict[str, Any]]:
    indexed = index_eodhd_symbols(rows_by_exchange_code, exchanges)
    return [
        evaluate_eodhd_row(
            row,
            indexed.get((row["exchange"], normalized_ticker_key(row["ticker"])), []),
            existing_isins=existing_isins,
            allow_existing_isin=allow_existing_isin,
        )
        for row in rows
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
                "field": "isin",
                "decision": "update",
                "proposed_value": result["eodhd_isin"],
                "confidence": "0.82",
                "reason": "EODHD exchange-symbol-list returned a valid ISIN for a row without ISIN, accepted only after ticker, EODHD exchange/subvenue, asset type, expected ISIN prefix, strict issuer/product-name, numeric-token, and checksum gates matched.",
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
    parser = argparse.ArgumentParser(description="Backfill missing ISINs from EODHD exchange-symbol-list data.")
    parser.add_argument("--json-out", type=Path, default=DEFAULT_REPORT_JSON)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_REPORT_CSV)
    parser.add_argument("--metadata-updates-csv", type=Path, default=DEFAULT_METADATA_UPDATES_CSV)
    parser.add_argument("--exchange", action="append", help="Restrict to one or more internal exchanges.")
    parser.add_argument("--asset-type", action="append", choices=["ETF", "Stock"], help="Restrict to one or more asset types.")
    parser.add_argument("--timeout-seconds", type=float, default=30.0)
    parser.add_argument(
        "--allow-existing-isin",
        action="store_true",
        help="Allow ISINs already present in the primary export. Keep disabled by default to avoid collapsing primary rows into existing cross-listing groups.",
    )
    parser.add_argument("--limit", type=int)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    api_token = os.environ.get("EODHD_API_TOKEN", "").strip()
    if not api_token:
        raise SystemExit("EODHD_API_TOKEN is required. Pass it as an environment variable; do not commit it.")

    exchanges = set(args.exchange or EODHD_EXCHANGE_CODES)
    unsupported = sorted(exchanges - set(EODHD_EXCHANGE_CODES))
    if unsupported:
        raise SystemExit(f"Unsupported EODHD exchange(s): {', '.join(unsupported)}")

    asset_types = set(args.asset_type or ["ETF", "Stock"])
    with TICKERS_CSV.open(newline="", encoding="utf-8") as handle:
        ticker_rows = list(csv.DictReader(handle))
    existing_isins = {row["isin"].strip().upper() for row in ticker_rows if row.get("isin", "").strip()}
    rows = [
        row
        for row in ticker_rows
        if row["exchange"] in exchanges
        and row["asset_type"] in asset_types
        and not row.get("isin", "").strip()
    ]
    if args.offset:
        rows = rows[args.offset :]
    if args.limit is not None:
        rows = rows[: args.limit]

    active_exchanges = {row["exchange"] for row in rows}
    needed_exchange_codes = sorted({EODHD_EXCHANGE_CODES[exchange] for exchange in active_exchanges})
    rows_by_exchange_code = {
        exchange_code: fetch_eodhd_symbol_list(exchange_code, api_token, timeout_seconds=args.timeout_seconds)
        for exchange_code in needed_exchange_codes
    }
    results = verify_eodhd_isins(
        rows,
        rows_by_exchange_code,
        exchanges=active_exchanges,
        existing_isins=existing_isins,
        allow_existing_isin=args.allow_existing_isin,
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
                "asset_types": sorted(asset_types),
                "candidates": len(rows),
                "decision_counts": dict(Counter(result["decision"] for result in results)),
                "eodhd_exchange_codes": needed_exchange_codes,
                "exchanges": sorted(exchanges),
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

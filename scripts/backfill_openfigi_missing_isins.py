from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.backfill_xtb_omi_isins import names_match
from scripts.backfill_yahoo_generic_etf_names import merge_metadata_updates
from scripts.rebuild_dataset import TICKERS_CSV, is_valid_isin


OPENFIGI_URL = "https://api.openfigi.com/v3/mapping"
DEFAULT_OUTPUT_DIR = ROOT / "data" / "openfigi_verification"
DEFAULT_REPORT_JSON = DEFAULT_OUTPUT_DIR / "missing_isin_backfill.json"
DEFAULT_REPORT_CSV = DEFAULT_OUTPUT_DIR / "missing_isin_backfill.csv"
DEFAULT_METADATA_UPDATES_CSV = ROOT / "data" / "review_overrides" / "metadata_updates.csv"

EXCHANGE_TO_FIGI: dict[str, str] = {
    "ASX": "AT",
    "B3": "BZ",
    "BATS": "US",
    "BME": "SM",
    "BMV": "MM",
    "Bursa": "MK",
    "CPH": "DC",
    "Euronext": "FP",
    "HEL": "FH",
    "HKEX": "HK",
    "IDX": "IJ",
    "JSE": "SJ",
    "KOSDAQ": "KQ",
    "KRX": "KS",
    "LSE": "LN",
    "NASDAQ": "US",
    "NEO": "CN",
    "NYSE": "US",
    "NYSE ARCA": "US",
    "NYSE MKT": "US",
    "OSL": "NO",
    "PSX": "PA",
    "SET": "TB",
    "SIX": "SW",
    "SSE": "CH",
    "STO": "SS",
    "SZSE": "CZ",
    "TASE": "IT",
    "TPEX": "TT",
    "TSE": "JT",
    "TSX": "CN",
    "TSXV": "CN",
    "TWSE": "TT",
    "XETRA": "GY",
}

EXPECTED_ISIN_PREFIXES: dict[str, tuple[str, ...]] = {
    "ASX": ("AU", "NZ"),
    "B3": ("BR",),
    "BATS": ("US", "IE", "LU"),
    "BME": ("ES",),
    "BMV": ("MX", "US", "CA", "IE", "LU", "GB", "DE", "FR"),
    "Bursa": ("MY",),
    "CPH": ("DK",),
    "Euronext": ("FR", "BE", "PT", "NL", "IE", "LU"),
    "HEL": ("FI",),
    "HKEX": ("HK", "KYG", "BMG", "CNE", "US"),
    "IDX": ("ID",),
    "JSE": ("ZA",),
    "KOSDAQ": ("KR",),
    "KRX": ("KR",),
    "LSE": ("GB", "IE", "LU", "JE", "GG", "IM", "US", "DE", "FR", "NL"),
    "NASDAQ": ("US", "KY", "VG", "NL", "IE", "LU", "GB", "CA", "IL", "BM", "MH"),
    "NEO": ("CA",),
    "NYSE": ("US",),
    "NYSE ARCA": ("US", "IE", "LU", "GB", "CA"),
    "NYSE MKT": ("US", "CA", "IL"),
    "OSL": ("NO",),
    "PSX": ("PK",),
    "SET": ("TH",),
    "SIX": ("CH", "IE", "LU", "US", "DE", "FR"),
    "SSE": ("CN",),
    "STO": ("SE",),
    "SZSE": ("CN",),
    "TASE": ("IL",),
    "TPEX": ("TW",),
    "TSE": ("JP",),
    "TSX": ("CA",),
    "TSXV": ("CA",),
    "TWSE": ("TW",),
    "XETRA": ("DE", "IE", "LU", "FR", "US", "GB", "NL"),
}

REPORT_FIELDNAMES = [
    "ticker",
    "exchange",
    "asset_type",
    "name",
    "figi_exch_code",
    "openfigi_ticker",
    "openfigi_name",
    "openfigi_security_type",
    "openfigi_isin",
    "number_tokens_match",
    "name_match",
    "decision",
]


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def normalized_ticker(value: str) -> str:
    return value.strip().upper().replace("-", ".").replace("/", ".")


def significant_numbers(value: str) -> set[str]:
    return set(re.findall(r"\d+", value))


def expected_isin_prefix_match(exchange: str, isin: str) -> bool:
    prefixes = EXPECTED_ISIN_PREFIXES.get(exchange)
    return prefixes is None or isin.startswith(prefixes)


def strict_names_match(source_name: str, target_name: str) -> bool:
    return significant_numbers(source_name) == significant_numbers(target_name) and names_match(source_name, target_name)


def load_missing_rows(path: Path, exchanges: set[str], asset_types: set[str]) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    return [
        row
        for row in rows
        if row.get("exchange") in exchanges
        and row.get("asset_type") in asset_types
        and not row.get("isin", "").strip()
        and row.get("exchange") in EXCHANGE_TO_FIGI
    ]


def fetch_openfigi(jobs: list[dict[str, str]], api_key: str, timeout_seconds: float) -> list[dict[str, Any]]:
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["X-OPENFIGI-APIKEY"] = api_key
    response = requests.post(OPENFIGI_URL, headers=headers, json=jobs, timeout=timeout_seconds)
    if response.status_code == 429:
        time.sleep(10)
        response = requests.post(OPENFIGI_URL, headers=headers, json=jobs, timeout=timeout_seconds)
    response.raise_for_status()
    return response.json()


def evaluate_candidate(row: dict[str, str], figi_exch_code: str, candidates: list[dict[str, Any]]) -> dict[str, Any]:
    base = {
        "ticker": row["ticker"],
        "exchange": row["exchange"],
        "asset_type": row["asset_type"],
        "name": row["name"],
        "figi_exch_code": figi_exch_code,
        "openfigi_ticker": "",
        "openfigi_name": "",
        "openfigi_security_type": "",
        "openfigi_isin": "",
        "number_tokens_match": False,
        "name_match": False,
    }
    if not candidates:
        return {**base, "decision": "no_openfigi_match"}

    ticker_matches = [
        candidate
        for candidate in candidates
        if normalized_ticker(str(candidate.get("ticker") or "")) == normalized_ticker(row["ticker"])
    ]
    if len(ticker_matches) != 1:
        return {**base, "decision": "ambiguous_openfigi_match" if ticker_matches else "ticker_mismatch"}

    candidate = ticker_matches[0]
    candidate_name = str(candidate.get("name") or "").strip()
    candidate_isin = str(candidate.get("idIsin") or "").strip().upper()
    base.update(
        {
            "openfigi_ticker": str(candidate.get("ticker") or "").strip(),
            "openfigi_name": candidate_name,
            "openfigi_security_type": str(candidate.get("securityType") or "").strip(),
            "openfigi_isin": candidate_isin,
            "number_tokens_match": significant_numbers(candidate_name) == significant_numbers(row["name"]),
            "name_match": strict_names_match(candidate_name, row["name"]) if candidate_name else False,
        }
    )
    if not candidate_isin:
        return {**base, "decision": "missing_isin"}
    if not is_valid_isin(candidate_isin):
        return {**base, "decision": "invalid_isin"}
    if not expected_isin_prefix_match(row["exchange"], candidate_isin):
        return {**base, "decision": "isin_country_mismatch"}
    if not candidate_name:
        return {**base, "decision": "missing_name"}
    if not base["number_tokens_match"]:
        return {**base, "decision": "number_token_mismatch"}
    if not base["name_match"]:
        return {**base, "decision": "name_mismatch"}
    return {**base, "decision": "accept"}


def verify_rows(
    rows: list[dict[str, str]],
    *,
    api_key: str,
    batch_size: int,
    delay_seconds: float,
    timeout_seconds: float,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for start in range(0, len(rows), batch_size):
        batch = rows[start : start + batch_size]
        jobs = [
            {"idType": "TICKER", "idValue": row["ticker"], "exchCode": EXCHANGE_TO_FIGI[row["exchange"]]}
            for row in batch
        ]
        payload = fetch_openfigi(jobs, api_key, timeout_seconds)
        for row, result in zip(batch, payload):
            candidates = result.get("data", []) if isinstance(result, dict) else []
            results.append(evaluate_candidate(row, EXCHANGE_TO_FIGI[row["exchange"]], candidates))
        if delay_seconds:
            time.sleep(delay_seconds)
    return results


def build_metadata_updates(results: list[dict[str, Any]]) -> list[dict[str, str]]:
    return [
        {
            "ticker": result["ticker"],
            "exchange": result["exchange"],
            "field": "isin",
            "decision": "update",
            "proposed_value": result["openfigi_isin"],
            "confidence": "0.8",
            "reason": "OpenFIGI TICKER mapping returned a valid ISIN for a missing-ISIN row; accepted only after exact ticker, exchange-code, expected ISIN prefix, numeric-token, issuer-name, and checksum gates matched.",
        }
        for result in results
        if result.get("decision") == "accept"
    ]


def write_report_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=REPORT_FIELDNAMES)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in REPORT_FIELDNAMES})


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reviewed OpenFIGI missing-ISIN backfill.")
    parser.add_argument("--tickers-csv", type=Path, default=TICKERS_CSV)
    parser.add_argument("--json-out", type=Path, default=DEFAULT_REPORT_JSON)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_REPORT_CSV)
    parser.add_argument("--metadata-updates-csv", type=Path, default=DEFAULT_METADATA_UPDATES_CSV)
    parser.add_argument("--exchange", action="append")
    parser.add_argument("--asset-type", action="append", choices=["ETF", "Stock"])
    parser.add_argument("--limit", type=int)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--batch-size", type=int, default=25)
    parser.add_argument("--delay-seconds", type=float, default=0.0)
    parser.add_argument("--timeout-seconds", type=float, default=30.0)
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    exchanges = set(args.exchange or EXCHANGE_TO_FIGI)
    asset_types = set(args.asset_type or ["ETF", "Stock"])
    api_key = os.environ.get("OPENFIGI_API_KEY", "").strip()

    rows = load_missing_rows(args.tickers_csv, exchanges, asset_types)
    if args.offset:
        rows = rows[args.offset :]
    if args.limit is not None:
        rows = rows[: args.limit]

    results = verify_rows(
        rows,
        api_key=api_key,
        batch_size=args.batch_size,
        delay_seconds=args.delay_seconds,
        timeout_seconds=args.timeout_seconds,
    )
    updates = build_metadata_updates(results)
    if args.apply and updates:
        merge_metadata_updates(args.metadata_updates_csv, updates)

    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.json_out.write_text(
        json.dumps([row for row in results if row["decision"] == "accept"], indent=2, sort_keys=True),
        encoding="utf-8",
    )
    write_report_csv(args.csv_out, results)

    print(
        json.dumps(
            {
                "accepted_isin_updates": len(updates),
                "applied": args.apply,
                "asset_types": sorted(asset_types),
                "candidates": len(rows),
                "csv_out": display_path(args.csv_out),
                "decision_counts": dict(Counter(row["decision"] for row in results)),
                "exchanges": sorted(exchanges),
                "json_out": display_path(args.json_out),
                "tickers_csv": display_path(args.tickers_csv),
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()

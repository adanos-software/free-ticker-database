"""Enrich missing ISINs using the OpenFIGI API.

Usage:
    python scripts/enrich_isins.py                  # dry-run: show what would change
    python scripts/enrich_isins.py --apply           # write changes to data/tickers.csv
    python scripts/enrich_isins.py --exchange NASDAQ  # only enrich a specific exchange

The OpenFIGI API (https://www.openfigi.com/api) is free for up to 250 requests/minute
without an API key, or 25,000/minute with a free key.

Set OPENFIGI_API_KEY env var for higher rate limits.
"""
from __future__ import annotations

import csv
import json
import os
import re
import sys
import time
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
TICKERS_CSV = DATA_DIR / "tickers.csv"

OPENFIGI_URL = "https://api.openfigi.com/v3/mapping"
OPENFIGI_KEY = os.environ.get("OPENFIGI_API_KEY", "")
BATCH_SIZE = 50  # OpenFIGI allows up to 100 per request
RATE_LIMIT_DELAY = 0.5  # seconds between batches (no key)
RATE_LIMIT_DELAY_KEYED = 0.05

ISIN_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$")

# Map our exchange names to OpenFIGI exchange codes
EXCHANGE_TO_FIGI: dict[str, str] = {
    "NASDAQ": "US",
    "NYSE": "US",
    "NYSE ARCA": "US",
    "NYSE MKT": "US",
    "LSE": "LN",
    "XETRA": "GY",
    "TSX": "CN",
    "ASX": "AT",
    "HKEX": "HK",
    "KRX": "KS",
    "KOSDAQ": "KQ",
    "B3": "BZ",
    "SSE": "CH",
    "SZSE": "CZ",
    "TWSE": "TT",
    "TPEX": "TT",
    "SIX": "SE",
    "Euronext": "FP",
    "WSE": "PW",
    "JSE": "SJ",
    "NSE_KE": "NR",
    "TASE": "IT",
    "BME": "SM",
    "Bursa": "MK",
    "STO": "SS",
    "HEL": "FH",
    "CPH": "DC",
    "OSL": "NO",
    "IDX": "IJ",
    "SET": "TB",
    "PSX": "PA",
}


def validate_isin_checksum(isin: str) -> bool:
    """Validate ISIN Luhn check digit."""
    if not ISIN_RE.fullmatch(isin):
        return False
    digits = ""
    for char in isin[:-1]:
        digits += char if char.isdigit() else str(ord(char) - 55)
    total = 0
    for i, d in enumerate(reversed(digits)):
        n = int(d)
        if i % 2 == 0:
            n *= 2
            if n > 9:
                n -= 9
        total += n
    return (10 - (total % 10)) % 10 == int(isin[-1])


def lookup_batch(jobs: list[dict]) -> list[dict | None]:
    """Send a batch to OpenFIGI and return results."""
    headers = {"Content-Type": "application/json"}
    if OPENFIGI_KEY:
        headers["X-OPENFIGI-APIKEY"] = OPENFIGI_KEY

    resp = requests.post(OPENFIGI_URL, headers=headers, json=jobs, timeout=30)
    if resp.status_code == 429:
        print("  Rate limited, waiting 10s...")
        time.sleep(10)
        resp = requests.post(OPENFIGI_URL, headers=headers, json=jobs, timeout=30)

    resp.raise_for_status()
    return resp.json()


def extract_isin(result: dict) -> str | None:
    """Extract a valid ISIN from an OpenFIGI result."""
    if "data" not in result:
        return None
    for item in result["data"]:
        isin = item.get("idIsin", "")
        if isin and validate_isin_checksum(isin):
            return isin
    return None


def main():
    apply = "--apply" in sys.argv
    exchange_filter = None
    if "--exchange" in sys.argv:
        idx = sys.argv.index("--exchange")
        exchange_filter = sys.argv[idx + 1] if idx + 1 < len(sys.argv) else None

    with TICKERS_CSV.open(newline="") as f:
        rows = list(csv.DictReader(f))

    # Find rows missing ISIN that we can look up
    candidates = []
    for i, row in enumerate(rows):
        if row["isin"]:
            continue
        if exchange_filter and row["exchange"] != exchange_filter:
            continue
        figi_exchange = EXCHANGE_TO_FIGI.get(row["exchange"])
        if not figi_exchange:
            continue
        candidates.append((i, row, figi_exchange))

    print(f"Found {len(candidates)} tickers missing ISIN (lookupable exchanges)")
    if not candidates:
        return

    enriched = 0
    delay = RATE_LIMIT_DELAY_KEYED if OPENFIGI_KEY else RATE_LIMIT_DELAY

    for batch_start in range(0, len(candidates), BATCH_SIZE):
        batch = candidates[batch_start : batch_start + BATCH_SIZE]
        jobs = [
            {"idType": "TICKER", "idValue": row["ticker"], "exchCode": figi_ex}
            for _, row, figi_ex in batch
        ]

        print(f"  Batch {batch_start // BATCH_SIZE + 1}: looking up {len(jobs)} tickers...")
        try:
            results = lookup_batch(jobs)
        except Exception as e:
            print(f"    Error: {e}")
            time.sleep(5)
            continue

        for (row_idx, row, _), result in zip(batch, results):
            isin = extract_isin(result)
            if isin:
                rows[row_idx]["isin"] = isin
                enriched += 1
                if not apply:
                    print(f"    {row['ticker']} ({row['exchange']}): -> {isin}")

        time.sleep(delay)

    print(f"\nEnriched {enriched} ISINs out of {len(candidates)} candidates")

    if apply and enriched > 0:
        fieldnames = list(rows[0].keys())
        with TICKERS_CSV.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        print(f"Written to {TICKERS_CSV}")
        print("Run 'python scripts/rebuild_dataset.py' to regenerate all formats.")
    elif not apply and enriched > 0:
        print("\nDry run. Use --apply to write changes.")


if __name__ == "__main__":
    main()

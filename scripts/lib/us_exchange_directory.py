"""Live US exchange directory snapshots for symbol-change apply gates."""

from __future__ import annotations

import csv
import io
import json
from pathlib import Path
from typing import Any

import requests

try:
    from scripts.lib.normalize import normalize_bool, normalize_symbol
except ModuleNotFoundError:  # pragma: no cover
    from lib.normalize import normalize_bool, normalize_symbol

ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / "data"
CACHE_DIR = DATA_DIR / "masterfiles" / "cache"

NASDAQ_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt"
NASDAQ_OTHER_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/symdir/otherlisted.txt"
SEC_COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers_exchange.json"

SEC_COMPANY_TICKERS_CACHE = CACHE_DIR / "sec_company_tickers_exchange.json"
NASDAQ_LISTED_CACHE = CACHE_DIR / "nasdaqlisted.txt"
NASDAQ_OTHER_LISTED_CACHE = CACHE_DIR / "otherlisted.txt"

NASDAQ_US_SOURCE_KEYS = {"nasdaq_listed", "nasdaq_other_listed"}
SEC_SOURCE_KEY = "sec_company_tickers_exchange"

OTHER_LISTED_EXCHANGE_MAP = {
    "A": "NYSE MKT",
    "F": "TXSE",
    "M": "NYSE CHICAGO",
    "N": "NYSE",
    "P": "NYSE ARCA",
    "Q": "NASDAQ",
    "V": "IEX",
    "Z": "BATS",
}

SEC_EXCHANGE_MAP = {
    "Nasdaq": "NASDAQ",
    "NYSE": "NYSE",
    "NYSE American": "NYSE MKT",
    "NYSE Arca": "NYSE ARCA",
    "BATS": "BATS",
    "OTC": "OTC",
}


def sec_request_headers() -> dict[str, str]:
    return {"User-Agent": "free-ticker-database/1.0 (symbol-change-apply)"}


def _fetch_text(url: str, *, cache_path: Path) -> str:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        text = response.text
        cache_path.write_text(text, encoding="utf-8")
        return text
    except requests.RequestException:
        if cache_path.exists():
            return cache_path.read_text(encoding="utf-8")
        raise


def _fetch_sec_payload(*, cache_path: Path = SEC_COMPANY_TICKERS_CACHE) -> dict[str, Any]:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        response = requests.get(SEC_COMPANY_TICKERS_URL, headers=sec_request_headers(), timeout=60)
        response.raise_for_status()
        payload = response.json()
        cache_path.write_text(json.dumps(payload), encoding="utf-8")
        return payload
    except (requests.RequestException, json.JSONDecodeError):
        if cache_path.exists():
            return json.loads(cache_path.read_text(encoding="utf-8"))
        raise


def _parse_pipe_table(text: str) -> list[dict[str, str]]:
    lines = [line for line in text.splitlines() if line.strip()]
    if lines and lines[-1].lower().startswith("file creation time"):
        lines = lines[:-1]
    reader = csv.DictReader(io.StringIO("\n".join(lines)), delimiter="|")
    return [{key: (value or "").strip() for key, value in row.items()} for row in reader]


def _infer_asset_type(name: str) -> str:
    lowered = name.casefold()
    if " etf" in lowered or lowered.endswith(" etf"):
        return "ETF"
    return "Stock"


def parse_nasdaq_listed_rows(text: str) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for row in _parse_pipe_table(text):
        symbol = row.get("Symbol", "")
        if not symbol:
            continue
        rows.append(
            {
                "source_key": "nasdaq_listed",
                "provider": "Nasdaq Trader",
                "source_url": NASDAQ_LISTED_URL,
                "ticker": symbol,
                "name": row.get("Security Name", ""),
                "exchange": "NASDAQ",
                "asset_type": "ETF" if row.get("ETF") == "Y" else "Stock",
                "listing_status": "test" if row.get("Test Issue") == "Y" else "active",
                "reference_scope": "exchange_directory",
                "official": "true",
                "isin": "",
                "sector": "",
            }
        )
    return rows


def parse_other_listed_rows(text: str) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for row in _parse_pipe_table(text):
        symbol = row.get("ACT Symbol", "")
        exchange = OTHER_LISTED_EXCHANGE_MAP.get(row.get("Exchange", ""), row.get("Exchange", ""))
        if not symbol or not exchange:
            continue
        rows.append(
            {
                "source_key": "nasdaq_other_listed",
                "provider": "Nasdaq Trader",
                "source_url": NASDAQ_OTHER_LISTED_URL,
                "ticker": symbol,
                "name": row.get("Security Name", ""),
                "exchange": exchange,
                "asset_type": "ETF" if row.get("ETF") == "Y" else "Stock",
                "listing_status": "test" if row.get("Test Issue") == "Y" else "active",
                "reference_scope": "exchange_directory",
                "official": "true",
                "isin": "",
                "sector": "",
            }
        )
    return rows


def parse_sec_company_tickers_rows(payload: dict[str, Any]) -> list[dict[str, str]]:
    fields = payload.get("fields", [])
    data = payload.get("data", [])
    if not isinstance(fields, list) or not isinstance(data, list):
        return []
    rows: list[dict[str, str]] = []
    for values in data:
        if not isinstance(values, list) or len(values) != len(fields):
            continue
        record = dict(zip(fields, values))
        ticker = str(record.get("ticker", "")).strip()
        exchange = SEC_EXCHANGE_MAP.get(str(record.get("exchange", "")).strip())
        if not ticker or not exchange:
            continue
        name = str(record.get("name", "")).strip()
        rows.append(
            {
                "source_key": SEC_SOURCE_KEY,
                "provider": "SEC",
                "source_url": SEC_COMPANY_TICKERS_URL,
                "ticker": ticker,
                "name": name,
                "exchange": exchange,
                "asset_type": _infer_asset_type(name),
                "listing_status": "active",
                "reference_scope": "exchange_directory",
                "official": "true",
                "isin": "",
                "sector": "",
            }
        )
    return rows


def load_live_us_directory_reference_rows(*, allow_network: bool = True) -> list[dict[str, str]]:
    if not allow_network:
        if not (
            NASDAQ_LISTED_CACHE.exists()
            and NASDAQ_OTHER_LISTED_CACHE.exists()
            and SEC_COMPANY_TICKERS_CACHE.exists()
        ):
            return []
        nasdaq_text = NASDAQ_LISTED_CACHE.read_text(encoding="utf-8")
        other_text = NASDAQ_OTHER_LISTED_CACHE.read_text(encoding="utf-8")
        sec_payload = json.loads(SEC_COMPANY_TICKERS_CACHE.read_text(encoding="utf-8"))
    else:
        nasdaq_text = _fetch_text(NASDAQ_LISTED_URL, cache_path=NASDAQ_LISTED_CACHE)
        other_text = _fetch_text(NASDAQ_OTHER_LISTED_URL, cache_path=NASDAQ_OTHER_LISTED_CACHE)
        sec_payload = _fetch_sec_payload()
    return [
        *parse_nasdaq_listed_rows(nasdaq_text),
        *parse_other_listed_rows(other_text),
        *parse_sec_company_tickers_rows(sec_payload),
    ]


def merge_us_directory_reference_rows(
    static_rows: list[dict[str, str]],
    live_rows: list[dict[str, str]],
) -> list[dict[str, str]]:
    """Prefer live Nasdaq Trader / SEC directory rows over stale static copies."""

    if not live_rows:
        return static_rows

    live_keys = {
        (row.get("source_key", ""), row.get("exchange", ""), normalize_symbol(row.get("ticker", "")))
        for row in live_rows
    }
    retained = [
        row
        for row in static_rows
        if row.get("source_key") not in NASDAQ_US_SOURCE_KEYS
        and (
            row.get("source_key", ""),
            row.get("exchange", ""),
            normalize_symbol(row.get("ticker", "")),
        )
        not in live_keys
    ]
    return [*retained, *live_rows]


def active_directory_keys(reference_rows: list[dict[str, str]]) -> set[tuple[str, str]]:
    keys: set[tuple[str, str]] = set()
    for row in reference_rows:
        if row.get("source_key") not in NASDAQ_US_SOURCE_KEYS:
            continue
        if row.get("listing_status") != "active" or not normalize_bool(row.get("official", "")):
            continue
        exchange = row.get("exchange", "")
        ticker = normalize_symbol(row.get("ticker", ""))
        if exchange and ticker:
            keys.add((exchange, ticker))
    return keys


def sec_ciks_by_ticker(payload: dict[str, Any] | None) -> dict[str, set[str]]:
    if not payload:
        return {}
    fields = payload.get("fields", [])
    data = payload.get("data", [])
    if not isinstance(fields, list) or not isinstance(data, list):
        return {}
    indexed: dict[str, set[str]] = {}
    for values in data:
        if not isinstance(values, list) or len(values) != len(fields):
            continue
        row = dict(zip(fields, values))
        ticker = normalize_symbol(str(row.get("ticker", "") or ""))
        cik = str(row.get("cik", "") or "").strip()
        if ticker and cik.isdigit():
            indexed.setdefault(ticker, set()).add(cik.zfill(10))
    return indexed


def load_sec_exchange_payload(*, allow_network: bool = True) -> dict[str, Any] | None:
    if allow_network:
        try:
            return _fetch_sec_payload()
        except (requests.RequestException, json.JSONDecodeError, OSError, ValueError, TypeError):
            pass
    if SEC_COMPANY_TICKERS_CACHE.exists():
        try:
            return json.loads(SEC_COMPANY_TICKERS_CACHE.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return None
    return None


def shared_sec_cik(old_ticker: str, new_ticker: str, sec_payload: dict[str, Any] | None) -> str:
    ciks = sec_ciks_by_ticker(sec_payload)
    old_set = ciks.get(normalize_symbol(old_ticker), set())
    new_set = ciks.get(normalize_symbol(new_ticker), set())
    shared = old_set & new_set
    return next(iter(sorted(shared)), "") if len(shared) == 1 else ""


def sec_cik_mismatch(old_ticker: str, new_ticker: str, sec_payload: dict[str, Any] | None) -> bool:
    ciks = sec_ciks_by_ticker(sec_payload)
    old_set = ciks.get(normalize_symbol(old_ticker), set())
    new_set = ciks.get(normalize_symbol(new_ticker), set())
    return bool(old_set and new_set and not (old_set & new_set))


def official_directory_row(
    reference_rows: list[dict[str, str]], exchange: str, ticker: str
) -> dict[str, str] | None:
    ticker = normalize_symbol(ticker)
    matches = [
        row
        for row in reference_rows
        if row.get("source_key") in NASDAQ_US_SOURCE_KEYS
        and row.get("exchange", "") == exchange
        and normalize_symbol(row.get("ticker", "")) == ticker
        and row.get("listing_status") == "active"
        and normalize_bool(row.get("official", ""))
    ]
    if len(matches) != 1:
        return None
    return matches[0]

from __future__ import annotations

import argparse
import csv
import json
import time
from pathlib import Path
from typing import Any, Callable, TypeVar

import requests


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
TICKERS_CSV = DATA_DIR / "tickers.csv"
IDENTIFIERS_CSV = DATA_DIR / "identifiers.csv"
IDENTIFIERS_EXTENDED_CSV = DATA_DIR / "identifiers_extended.csv"
IDENTIFIER_SUMMARY_JSON = DATA_DIR / "identifier_summary.json"
MASTERFILES_DIR = DATA_DIR / "masterfiles"
SEC_COMPANY_TICKERS_CACHE = MASTERFILES_DIR / "cache" / "sec_company_tickers_exchange.json"
LEGACY_SEC_COMPANY_TICKERS_CACHE = MASTERFILES_DIR / "sec_company_tickers_exchange.json"

SEC_COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers_exchange.json"
OPENFIGI_MAPPING_URL = "https://api.openfigi.com/v3/mapping"
GLEIF_LEI_RECORDS_URL = "https://api.gleif.org/api/v1/lei-records"
USER_AGENT = "free-ticker-database/2.0 (+https://github.com/adanos-software/free-ticker-database)"
REQUEST_TIMEOUT = 30.0
RETRY_ATTEMPTS = 3
RETRY_DELAY_SECONDS = 2.0
RATE_LIMIT_RETRY_SECONDS = 15.0
T = TypeVar("T")

SEC_EXCHANGE_MAP = {
    "Nasdaq": "NASDAQ",
    "NYSE": "NYSE",
    "NYSE American": "NYSE MKT",
    "NYSE Arca": "NYSE ARCA",
    "OTC": "OTC",
    "CboeBZX": "BATS",
}

OPENFIGI_EXCHANGE_HINTS = {
    "ASX": {"AU"},
    "BATS": {"US"},
    "IEX": {"US"},
    "NASDAQ": {"US"},
    "NYSE": {"US"},
    "NYSE ARCA": {"US"},
    "NYSE CHICAGO": {"US"},
    "NYSE MKT": {"US"},
    "TSX": {"CN"},
    "TSXV": {"CN"},
}


def load_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def normalize_company_name(value: str) -> str:
    return "".join(ch for ch in value.lower() if ch.isalnum())


def fetch_json(url: str, session: requests.Session | None = None, headers: dict[str, str] | None = None) -> Any:
    session = session or requests.Session()
    merged_headers = {"User-Agent": USER_AGENT}
    if headers:
        merged_headers.update(headers)
    response = session.get(url, headers=merged_headers, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.json()


def with_retries(
    fn: Callable[[], T],
    attempts: int = RETRY_ATTEMPTS,
    delay_seconds: float = RETRY_DELAY_SECONDS,
) -> T:
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except requests.RequestException as exc:
            last_error = exc
            if attempt == attempts:
                break
            time.sleep(retry_delay_for(exc, attempt, delay_seconds))
    assert last_error is not None
    raise last_error


def retry_delay_for(
    error: requests.RequestException,
    attempt: int,
    base_delay_seconds: float = RETRY_DELAY_SECONDS,
) -> float:
    response = getattr(error, "response", None)
    retry_after = None
    if response is not None:
        retry_after = response.headers.get("Retry-After")
    if retry_after:
        try:
            return float(retry_after)
        except ValueError:
            pass
    if response is not None and response.status_code == 429:
        return max(base_delay_seconds * attempt, RATE_LIMIT_RETRY_SECONDS)
    return base_delay_seconds * attempt


def load_sec_payload(session: requests.Session | None = None) -> tuple[dict[str, Any] | None, str]:
    for path in (SEC_COMPANY_TICKERS_CACHE, LEGACY_SEC_COMPANY_TICKERS_CACHE):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"
    try:
        payload = fetch_json(SEC_COMPANY_TICKERS_URL, session=session)
    except requests.RequestException:
        return None, "unavailable"
    SEC_COMPANY_TICKERS_CACHE.parent.mkdir(parents=True, exist_ok=True)
    SEC_COMPANY_TICKERS_CACHE.write_text(json.dumps(payload), encoding="utf-8")
    return payload, "network"


def post_json(
    url: str,
    payload: Any,
    session: requests.Session | None = None,
    headers: dict[str, str] | None = None,
) -> Any:
    session = session or requests.Session()
    merged_headers = {"User-Agent": USER_AGENT, "Content-Type": "application/json"}
    if headers:
        merged_headers.update(headers)
    response = session.post(url, headers=merged_headers, json=payload, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.json()


def build_base_identifier_rows() -> list[dict[str, str]]:
    tickers = {(row["ticker"], row["exchange"]): row for row in load_csv(TICKERS_CSV)}
    identifiers_by_ticker = {row["ticker"]: row for row in load_csv(IDENTIFIERS_CSV)}
    existing_extended: dict[tuple[str, str], dict[str, str]] = {}
    if IDENTIFIERS_EXTENDED_CSV.exists():
        existing_extended = {
            (row["ticker"], row["exchange"]): row
            for row in load_csv(IDENTIFIERS_EXTENDED_CSV)
        }
    rows: list[dict[str, str]] = []
    for (ticker, exchange), ticker_row in sorted(tickers.items()):
        identifier_row = identifiers_by_ticker.get(ticker, {"isin": "", "wkn": ""})
        existing_row = existing_extended.get((ticker, exchange), {})
        rows.append(
            {
                "ticker": ticker,
                "exchange": exchange,
                "isin": identifier_row.get("isin", ""),
                "wkn": identifier_row.get("wkn", ""),
                "figi": existing_row.get("figi", ""),
                "cik": existing_row.get("cik", ""),
                "lei": existing_row.get("lei", ""),
                "figi_source": existing_row.get("figi_source", ""),
                "cik_source": existing_row.get("cik_source", ""),
                "lei_source": existing_row.get("lei_source", ""),
                "name": ticker_row["name"],
                "country": ticker_row["country"],
                "asset_type": ticker_row["asset_type"],
            }
        )
    return rows


def build_sec_cik_index(payload: dict[str, Any]) -> dict[tuple[str, str], str]:
    index: dict[tuple[str, str], str] = {}
    fields = payload.get("fields", [])
    for values in payload.get("data", []):
        record = dict(zip(fields, values))
        ticker = str(record.get("ticker", "")).strip()
        exchange = SEC_EXCHANGE_MAP.get(str(record.get("exchange", "")).strip())
        cik = record.get("cik")
        if not ticker or not exchange or cik is None:
            continue
        index[(ticker, exchange)] = str(cik).zfill(10)
    return index


def apply_sec_cik(rows: list[dict[str, str]], cik_index: dict[tuple[str, str], str]) -> int:
    updated = 0
    by_ticker = {ticker: cik for (ticker, _exchange), cik in cik_index.items()}
    for row in rows:
        cik = cik_index.get((row["ticker"], row["exchange"])) or by_ticker.get(row["ticker"], "")
        if cik:
            row["cik"] = cik
            row["cik_source"] = "SEC company_tickers_exchange.json"
            updated += 1
    return updated


def fetch_openfigi_by_isin(
    isins: list[str],
    session: requests.Session | None = None,
    api_key: str | None = None,
    delay_seconds: float = 0.0,
    batch_size: int = 10,
    retry_attempts: int = RETRY_ATTEMPTS,
    retry_delay_seconds: float = RETRY_DELAY_SECONDS,
) -> tuple[dict[str, list[dict[str, Any]]], list[str]]:
    session = session or requests.Session()
    headers = {}
    if api_key:
        headers["X-OPENFIGI-APIKEY"] = api_key

    result: dict[str, list[dict[str, Any]]] = {}
    errors: list[str] = []
    for start in range(0, len(isins), batch_size):
        batch = isins[start : start + batch_size]
        jobs = [{"idType": "ID_ISIN", "idValue": isin} for isin in batch]
        try:
            response = with_retries(
                lambda: post_json(OPENFIGI_MAPPING_URL, jobs, session=session, headers=headers),
                attempts=retry_attempts,
                delay_seconds=retry_delay_seconds,
            )
        except requests.RequestException as exc:
            errors.append(f"OpenFIGI batch {batch[0]}..{batch[-1]} failed: {exc}")
            continue
        for isin, payload in zip(batch, response):
            result[isin] = payload.get("data", [])
        if delay_seconds:
            time.sleep(delay_seconds)
    return result, errors


def select_figi_rows(
    rows: list[dict[str, str]],
    exchanges: set[str] | None = None,
    limit: int | None = None,
) -> list[dict[str, str]]:
    candidates = [
        row
        for row in rows
        if row["isin"]
        and not row["figi"]
        and (not exchanges or row["exchange"] in exchanges)
    ]
    ordered = sorted(candidates, key=lambda row: (row["exchange"], row["ticker"]))
    return ordered[:limit] if limit is not None else ordered


def select_openfigi_candidate(
    row: dict[str, str],
    candidates: list[dict[str, Any]],
) -> str:
    if not candidates:
        return ""

    target_ticker = row["ticker"].upper()
    hinted_codes = OPENFIGI_EXCHANGE_HINTS.get(row["exchange"], set())

    hinted = [candidate for candidate in candidates if candidate.get("exchCode") in hinted_codes]
    ticker_and_hint = [candidate for candidate in hinted if str(candidate.get("ticker", "")).upper() == target_ticker]
    if ticker_and_hint:
        return str(ticker_and_hint[0].get("figi", ""))
    if hinted:
        return str(hinted[0].get("figi", ""))

    exact_ticker = [candidate for candidate in candidates if str(candidate.get("ticker", "")).upper() == target_ticker]
    if exact_ticker:
        return str(exact_ticker[0].get("figi", ""))

    return str(candidates[0].get("figi", ""))


def build_figi_matches(
    rows: list[dict[str, str]],
    candidates_by_isin: dict[str, list[dict[str, Any]]],
) -> dict[tuple[str, str], str]:
    matches: dict[tuple[str, str], str] = {}
    for row in rows:
        figi = select_openfigi_candidate(row, candidates_by_isin.get(row["isin"], []))
        if figi:
            matches[(row["ticker"], row["exchange"])] = figi
    return matches


def apply_figi(rows: list[dict[str, str]], figi_by_listing: dict[tuple[str, str], str]) -> int:
    updated = 0
    for row in rows:
        figi = figi_by_listing.get((row["ticker"], row["exchange"]), "")
        if figi and row["figi"] != figi:
            row["figi"] = figi
            row["figi_source"] = "OpenFIGI"
            updated += 1
    return updated


def fetch_gleif_lei(
    name: str,
    session: requests.Session | None = None,
    retry_attempts: int = RETRY_ATTEMPTS,
    retry_delay_seconds: float = RETRY_DELAY_SECONDS,
) -> str:
    session = session or requests.Session()
    payload = with_retries(
        lambda: fetch_json(
            f"{GLEIF_LEI_RECORDS_URL}?filter[entity.legalName]={requests.utils.quote(name)}&page[size]=1",
            session=session,
        ),
        attempts=retry_attempts,
        delay_seconds=retry_delay_seconds,
    )
    data = payload.get("data", [])
    if not data:
        return ""
    legal_name = (((data[0].get("attributes") or {}).get("entity") or {}).get("legalName") or {}).get("name", "")
    if normalize_company_name(legal_name) != normalize_company_name(name):
        return ""
    return data[0].get("id", "")


def select_lei_candidates(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    preferred_exchanges = {"NASDAQ", "NYSE", "NYSE ARCA", "NYSE MKT", "LSE", "XETRA", "TSX", "ASX", "Euronext"}

    def sort_key(row: dict[str, str]) -> tuple[int, int, int, int, str]:
        starts_with_letter = int(bool(row["ticker"]) and row["ticker"][0].isalpha())
        return (
            int(bool(row["isin"])),
            int(row.get("asset_type") == "Stock"),
            int(row.get("exchange") in preferred_exchanges),
            starts_with_letter,
            row["ticker"],
        )

    return sorted(rows, key=sort_key, reverse=True)


def filter_lei_candidates(
    rows: list[dict[str, str]],
    exchanges: set[str] | None = None,
) -> list[dict[str, str]]:
    candidates = [
        row
        for row in rows
        if row["asset_type"] == "Stock"
        and row["isin"]
        and not row["lei"]
        and (not exchanges or row["exchange"] in exchanges)
    ]
    return select_lei_candidates(candidates)


def apply_lei(
    rows: list[dict[str, str]],
    session: requests.Session | None = None,
    delay_seconds: float = 0.0,
    limit: int | None = None,
    exchanges: set[str] | None = None,
) -> tuple[int, list[str]]:
    updated = 0
    errors: list[str] = []
    session = session or requests.Session()
    candidates = filter_lei_candidates(rows, exchanges=exchanges)
    for row in candidates[:limit] if limit else candidates:
        try:
            lei = fetch_gleif_lei(row["name"], session=session)
        except requests.RequestException as exc:
            errors.append(f"GLEIF lookup failed for {row['ticker']}:{row['exchange']}: {exc}")
            continue
        if lei and row["lei"] != lei:
            row["lei"] = lei
            row["lei_source"] = "GLEIF"
            updated += 1
        if delay_seconds:
            time.sleep(delay_seconds)
    return updated, errors


def build_summary(rows: list[dict[str, str]]) -> dict[str, Any]:
    return {
        "rows": len(rows),
        "cik_coverage": sum(bool(row["cik"]) for row in rows),
        "figi_coverage": sum(bool(row["figi"]) for row in rows),
        "lei_coverage": sum(bool(row["lei"]) for row in rows),
    }


def persist_rows(rows: list[dict[str, str]]) -> dict[str, Any]:
    fieldnames = [
        "ticker",
        "exchange",
        "isin",
        "wkn",
        "figi",
        "cik",
        "lei",
        "figi_source",
        "cik_source",
        "lei_source",
    ]
    write_csv(
        IDENTIFIERS_EXTENDED_CSV,
        fieldnames,
        [{field: row.get(field, "") for field in fieldnames} for row in rows],
    )
    summary = build_summary(rows)
    IDENTIFIER_SUMMARY_JSON.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary


def main(
    enable_cik: bool = True,
    enable_figi: bool = False,
    enable_lei: bool = False,
    openfigi_api_key: str | None = None,
    figi_delay_seconds: float = 0.0,
    figi_batch_size: int = 10,
    figi_limit: int | None = None,
    figi_exchanges: set[str] | None = None,
    lei_delay_seconds: float = 0.0,
    lei_limit: int | None = None,
    lei_exchanges: set[str] | None = None,
) -> dict[str, Any]:
    rows = build_base_identifier_rows()
    session = requests.Session()
    errors: list[str] = []
    if enable_cik:
        payload, payload_source = load_sec_payload(session=session)
        if payload is None:
            errors.append("SEC company_tickers_exchange.json unavailable from this environment")
        else:
            cik_index = build_sec_cik_index(payload)
            apply_sec_cik(rows, cik_index)
    if enable_figi:
        figi_rows = select_figi_rows(rows, exchanges=figi_exchanges, limit=figi_limit)
        figi_candidates, figi_errors = fetch_openfigi_by_isin(
            sorted({row["isin"] for row in figi_rows}),
            session=session,
            api_key=openfigi_api_key,
            delay_seconds=figi_delay_seconds,
            batch_size=figi_batch_size,
        )
        figi_matches = build_figi_matches(figi_rows, figi_candidates)
        apply_figi(rows, figi_matches)
        errors.extend(figi_errors)
    if enable_lei:
        _updated, lei_errors = apply_lei(
            rows,
            session=session,
            delay_seconds=lei_delay_seconds,
            limit=lei_limit,
            exchanges=lei_exchanges,
        )
        errors.extend(lei_errors)
    summary = persist_rows(rows)
    if enable_cik:
        summary["cik_source_mode"] = payload_source
    if errors:
        summary["errors"] = errors
    print(json.dumps(summary, indent=2))
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Enrich extended identifiers with CIK, FIGI, and LEI.")
    parser.add_argument("--disable-cik", action="store_true")
    parser.add_argument("--enable-figi", action="store_true")
    parser.add_argument("--enable-lei", action="store_true")
    parser.add_argument("--openfigi-api-key", default="")
    parser.add_argument("--figi-delay-seconds", type=float, default=0.0)
    parser.add_argument("--figi-batch-size", type=int, default=10)
    parser.add_argument("--figi-limit", type=int)
    parser.add_argument("--figi-exchanges", default="")
    parser.add_argument("--lei-delay-seconds", type=float, default=0.0)
    parser.add_argument("--lei-limit", type=int)
    parser.add_argument("--lei-exchanges", default="")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    main(
        enable_cik=not args.disable_cik,
        enable_figi=args.enable_figi,
        enable_lei=args.enable_lei,
        openfigi_api_key=args.openfigi_api_key or None,
        figi_delay_seconds=args.figi_delay_seconds,
        figi_batch_size=args.figi_batch_size,
        figi_limit=args.figi_limit,
        figi_exchanges={value.strip() for value in args.figi_exchanges.split(",") if value.strip()} or None,
        lei_delay_seconds=args.lei_delay_seconds,
        lei_limit=args.lei_limit,
        lei_exchanges={value.strip() for value in args.lei_exchanges.split(",") if value.strip()} or None,
    )

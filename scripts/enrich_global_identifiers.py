from __future__ import annotations

import argparse
import csv
import json
import time
from pathlib import Path
from typing import Any

import requests


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
TICKERS_CSV = DATA_DIR / "tickers.csv"
IDENTIFIERS_CSV = DATA_DIR / "identifiers.csv"
IDENTIFIERS_EXTENDED_CSV = DATA_DIR / "identifiers_extended.csv"
IDENTIFIER_SUMMARY_JSON = DATA_DIR / "identifier_summary.json"
SEC_COMPANY_TICKERS_CACHE = DATA_DIR / "masterfiles" / "sec_company_tickers_exchange.json"

SEC_COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers_exchange.json"
OPENFIGI_MAPPING_URL = "https://api.openfigi.com/v3/mapping"
GLEIF_LEI_RECORDS_URL = "https://api.gleif.org/api/v1/lei-records"
USER_AGENT = "free-ticker-database/2.0 (+https://github.com/adanos-software/free-ticker-database)"
REQUEST_TIMEOUT = 30.0

SEC_EXCHANGE_MAP = {
    "Nasdaq": "NASDAQ",
    "NYSE": "NYSE",
    "NYSE American": "NYSE MKT",
    "NYSE Arca": "NYSE ARCA",
    "OTC": "OTC",
    "CboeBZX": "BATS",
}


def load_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def fetch_json(url: str, session: requests.Session | None = None, headers: dict[str, str] | None = None) -> Any:
    session = session or requests.Session()
    merged_headers = {"User-Agent": USER_AGENT}
    if headers:
        merged_headers.update(headers)
    response = session.get(url, headers=merged_headers, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.json()


def load_sec_payload(session: requests.Session | None = None) -> tuple[dict[str, Any] | None, str]:
    if SEC_COMPANY_TICKERS_CACHE.exists():
        return json.loads(SEC_COMPANY_TICKERS_CACHE.read_text(encoding="utf-8")), "cache"
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
    rows: list[dict[str, str]] = []
    for (ticker, exchange), ticker_row in sorted(tickers.items()):
        identifier_row = identifiers_by_ticker.get(ticker, {"isin": "", "wkn": ""})
        rows.append(
            {
                "ticker": ticker,
                "exchange": exchange,
                "isin": identifier_row.get("isin", ""),
                "wkn": identifier_row.get("wkn", ""),
                "figi": "",
                "cik": "",
                "lei": "",
                "figi_source": "",
                "cik_source": "",
                "lei_source": "",
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
) -> dict[str, str]:
    session = session or requests.Session()
    headers = {}
    if api_key:
        headers["X-OPENFIGI-APIKEY"] = api_key

    result: dict[str, str] = {}
    for start in range(0, len(isins), 10):
        batch = isins[start : start + 10]
        jobs = [{"idType": "ID_ISIN", "idValue": isin} for isin in batch]
        response = post_json(OPENFIGI_MAPPING_URL, jobs, session=session, headers=headers)
        for isin, payload in zip(batch, response):
            candidates = payload.get("data", [])
            if candidates:
                figi = candidates[0].get("figi", "")
                if figi:
                    result[isin] = figi
        if delay_seconds:
            time.sleep(delay_seconds)
    return result


def apply_figi(rows: list[dict[str, str]], figi_by_isin: dict[str, str]) -> int:
    updated = 0
    for row in rows:
        figi = figi_by_isin.get(row["isin"], "")
        if figi:
            row["figi"] = figi
            row["figi_source"] = "OpenFIGI"
            updated += 1
    return updated


def fetch_gleif_lei(name: str, session: requests.Session | None = None) -> str:
    session = session or requests.Session()
    payload = fetch_json(
        f"{GLEIF_LEI_RECORDS_URL}?filter[entity.legalName]={requests.utils.quote(name)}&page[size]=1",
        session=session,
    )
    data = payload.get("data", [])
    if not data:
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


def apply_lei(
    rows: list[dict[str, str]],
    session: requests.Session | None = None,
    delay_seconds: float = 0.0,
    limit: int | None = None,
) -> int:
    updated = 0
    session = session or requests.Session()
    candidates = select_lei_candidates(rows)
    for row in candidates[:limit] if limit else candidates:
        lei = fetch_gleif_lei(row["name"], session=session)
        if lei:
            row["lei"] = lei
            row["lei_source"] = "GLEIF"
            updated += 1
        if delay_seconds:
            time.sleep(delay_seconds)
    return updated


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
    figi_limit: int | None = None,
    lei_delay_seconds: float = 0.0,
    lei_limit: int | None = None,
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
        try:
            isins = sorted({row["isin"] for row in rows if row["isin"]})
            if figi_limit is not None:
                isins = isins[:figi_limit]
            figi_by_isin = fetch_openfigi_by_isin(
                isins,
                session=session,
                api_key=openfigi_api_key,
                delay_seconds=figi_delay_seconds,
            )
            apply_figi(rows, figi_by_isin)
        except requests.RequestException as exc:
            errors.append(f"OpenFIGI unavailable: {exc}")
    if enable_lei:
        try:
            apply_lei(rows, session=session, delay_seconds=lei_delay_seconds, limit=lei_limit)
        except requests.RequestException as exc:
            errors.append(f"GLEIF unavailable: {exc}")
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
    parser.add_argument("--figi-limit", type=int)
    parser.add_argument("--lei-delay-seconds", type=float, default=0.0)
    parser.add_argument("--lei-limit", type=int)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    main(
        enable_cik=not args.disable_cik,
        enable_figi=args.enable_figi,
        enable_lei=args.enable_lei,
        openfigi_api_key=args.openfigi_api_key or None,
        figi_delay_seconds=args.figi_delay_seconds,
        figi_limit=args.figi_limit,
        lei_delay_seconds=args.lei_delay_seconds,
        lei_limit=args.lei_limit,
    )

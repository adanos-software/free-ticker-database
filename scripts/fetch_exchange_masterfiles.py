from __future__ import annotations

import csv
import io
import json
import os
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
import requests


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
MASTERFILES_DIR = DATA_DIR / "masterfiles"
MASTERFILE_REFERENCE_CSV = MASTERFILES_DIR / "reference.csv"
MASTERFILE_SOURCES_JSON = MASTERFILES_DIR / "sources.json"
MASTERFILE_SUMMARY_JSON = MASTERFILES_DIR / "summary.json"
MASTERFILE_CACHE_DIR = MASTERFILES_DIR / "cache"
SEC_COMPANY_TICKERS_CACHE = MASTERFILE_CACHE_DIR / "sec_company_tickers_exchange.json"
LEGACY_SEC_COMPANY_TICKERS_CACHE = MASTERFILES_DIR / "sec_company_tickers_exchange.json"
TPEX_MAINBOARD_QUOTES_CACHE = MASTERFILE_CACHE_DIR / "tpex_mainboard_daily_close_quotes.json"
LEGACY_TPEX_MAINBOARD_QUOTES_CACHE = MASTERFILES_DIR / "tpex_mainboard_daily_close_quotes.json"

SEC_COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers_exchange.json"
NASDAQ_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt"
NASDAQ_OTHER_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/symdir/otherlisted.txt"
ASX_LISTED_URL = "https://www.asx.com.au/asx/research/ASXListedCompanies.csv"
TMX_INTERLISTED_URL = "https://www.tsx.com/files/trading/interlisted-companies.txt"
EURONEXT_EQUITIES_DOWNLOAD_URL = "https://live.euronext.com/pd_es/data/stocks/download?mics=dm_all_stock"
JPX_LISTED_ISSUES_URL = "https://www.jpx.co.jp/english/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_e.xls"
DEUTSCHE_BOERSE_LISTED_URL = "https://www.cashmarket.deutsche-boerse.com/resource/blob/67858/dd766fc6588100c79294324175f95501/data/Listed-companies.xlsx"
B3_INSTRUMENTS_EQUITIES_URL = "https://arquivos.b3.com.br/bdi/table/InstrumentsEquities"
TWSE_LISTED_COMPANIES_URL = "https://openapi.twse.com.tw/v1/opendata/t187ap03_L"
TPEX_MAINBOARD_QUOTES_URL = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes"

USER_AGENT = "free-ticker-database/2.0 (+https://github.com/adanos-software/free-ticker-database)"
SEC_CONTACT_EMAIL = os.environ.get("SEC_CONTACT_EMAIL", "opensource@adanos.software")
REQUEST_TIMEOUT = 30.0

OTHER_LISTED_EXCHANGE_MAP = {
    "A": "NYSE MKT",
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
    "OTC": "OTC",
    "CboeBZX": "BATS",
}

ETF_NAME_MARKERS = (
    " etf",
    " etn",
    " fund",
    " trust",
    " ucits",
    "shares ",
)

EURONEXT_MARKET_MAP = {
    "Euronext Amsterdam": "AMS",
    "Oslo Børs": "OSL",
    "Euronext Oslo Børs": "OSL",
    "Euronext Expand Oslo": "OSL",
    "Euronext Growth Oslo": "OSL",
}

EURONEXT_SECONDARY_MARKETS = {
    "EuroTLX",
    "Euronext Global Equity Market",
    "Trading After Hours",
}

DEUTSCHE_BOERSE_SHEETS = ("Prime Standard", "General Standard", "Scale", "Basic Board")
B3_ALLOWED_CASH_CATEGORIES = {
    "ETF EQUITIES": "ETF",
    "ETF FOREIGN INDEX": "ETF",
    "SHARES": "Stock",
    "UNIT": "Stock",
}
B3_EXCLUDED_ISSUER_MARKERS = (
    "taxa de financiamento",
    "financ/termo",
)
TPEX_CANONICAL_TICKER_RE = re.compile(r"(?:\d{4}|00\d{4}[A-Z]?)$")


@dataclass(frozen=True)
class MasterfileSource:
    key: str
    provider: str
    description: str
    source_url: str
    format: str
    reference_scope: str = "exchange_directory"
    official: bool = True


OFFICIAL_SOURCES = [
    MasterfileSource(
        key="nasdaq_listed",
        provider="Nasdaq Trader",
        description="Official Nasdaq-listed symbol directory",
        source_url=NASDAQ_LISTED_URL,
        format="nasdaq_listed_pipe",
    ),
    MasterfileSource(
        key="nasdaq_other_listed",
        provider="Nasdaq Trader",
        description="Official other-listed/CQS symbol directory",
        source_url=NASDAQ_OTHER_LISTED_URL,
        format="nasdaq_other_listed_pipe",
    ),
    MasterfileSource(
        key="asx_listed_companies",
        provider="ASX",
        description="Official ASX listed companies directory",
        source_url=ASX_LISTED_URL,
        format="asx_listed_companies_csv",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="tmx_interlisted_companies",
        provider="TMX",
        description="Official TSX/TSXV interlisted companies reference",
        source_url=TMX_INTERLISTED_URL,
        format="tmx_interlisted_tab",
        reference_scope="interlisted_subset",
    ),
    MasterfileSource(
        key="euronext_equities",
        provider="Euronext",
        description="Official Euronext live equities directory export",
        source_url=EURONEXT_EQUITIES_DOWNLOAD_URL,
        format="euronext_equities_semicolon_csv",
    ),
    MasterfileSource(
        key="jpx_listed_issues",
        provider="JPX",
        description="Official JPX list of TSE-listed issues",
        source_url=JPX_LISTED_ISSUES_URL,
        format="jpx_listed_issues_excel",
    ),
    MasterfileSource(
        key="deutsche_boerse_listed_companies",
        provider="Deutsche Boerse",
        description="Official Deutsche Boerse listed companies workbook",
        source_url=DEUTSCHE_BOERSE_LISTED_URL,
        format="deutsche_boerse_listed_companies_excel",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="b3_instruments_equities",
        provider="B3",
        description="Official B3 instruments consolidated cash-equities table",
        source_url=B3_INSTRUMENTS_EQUITIES_URL,
        format="b3_instruments_equities_api",
    ),
    MasterfileSource(
        key="twse_listed_companies",
        provider="TWSE",
        description="Official TWSE listed companies open data feed",
        source_url=TWSE_LISTED_COMPANIES_URL,
        format="twse_listed_companies_json",
    ),
    MasterfileSource(
        key="tpex_mainboard_daily_quotes",
        provider="TPEX",
        description="Official TPEX mainboard daily quotes open data feed",
        source_url=TPEX_MAINBOARD_QUOTES_URL,
        format="tpex_mainboard_quotes_json",
    ),
    MasterfileSource(
        key="sec_company_tickers_exchange",
        provider="SEC",
        description="Official SEC company ticker to exchange mapping",
        source_url=SEC_COMPANY_TICKERS_URL,
        format="sec_company_tickers_exchange_json",
    ),
]


def ensure_output_dirs() -> None:
    MASTERFILES_DIR.mkdir(parents=True, exist_ok=True)
    MASTERFILE_CACHE_DIR.mkdir(parents=True, exist_ok=True)


def write_csv(path: Path, fieldnames: list[str], rows: Iterable[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def load_manual_masterfiles(manual_dir: Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    if not manual_dir.exists():
        return rows

    for path in sorted(manual_dir.glob("*.csv")):
        with path.open(newline="", encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                rows.append(
                    {
                        "source_key": f"manual:{path.stem}",
                        "provider": "manual",
                        "source_url": str(path.relative_to(ROOT)),
                        "ticker": row.get("ticker", "").strip(),
                        "name": row.get("name", "").strip(),
                        "exchange": row.get("exchange", "").strip(),
                        "asset_type": row.get("asset_type", "").strip() or infer_asset_type(row.get("name", "")),
                        "listing_status": row.get("listing_status", "").strip() or "active",
                        "reference_scope": row.get("reference_scope", "").strip() or "manual",
                        "official": "false",
                    }
                )
    return rows


def infer_asset_type(name: str) -> str:
    lowered = f" {name.lower()} "
    return "ETF" if any(marker in lowered for marker in ETF_NAME_MARKERS) else "Stock"


def infer_taiwan_asset_type(ticker: str, name: str) -> str:
    normalized_ticker = ticker.strip()
    if normalized_ticker.startswith("00"):
        return "ETF"
    return infer_asset_type(name)


def fetch_text(url: str, session: requests.Session | None = None) -> str:
    session = session or requests.Session()
    response = session.get(url, headers={"User-Agent": USER_AGENT}, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.text


def fetch_bytes(url: str, session: requests.Session | None = None) -> bytes:
    session = session or requests.Session()
    response = session.get(url, headers={"User-Agent": USER_AGENT}, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.content


def fetch_json(
    url: str,
    session: requests.Session | None = None,
    headers: dict[str, str] | None = None,
) -> Any:
    session = session or requests.Session()
    merged_headers = {"User-Agent": USER_AGENT}
    if headers:
        merged_headers.update(headers)
    response = session.get(url, headers=merged_headers, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.json()


def sec_request_headers() -> dict[str, str]:
    return {
        "User-Agent": f"free-ticker-database/2.0 ({SEC_CONTACT_EMAIL})",
        "Accept": "application/json,text/plain,*/*",
        "Accept-Encoding": "gzip, deflate",
        "Referer": "https://www.sec.gov/",
    }


def tpex_request_headers() -> dict[str, str]:
    return {
        "User-Agent": USER_AGENT,
        "Accept": "application/json,text/plain,*/*",
        "Accept-Encoding": "gzip, deflate",
        "Referer": "https://www.tpex.org.tw/",
        "Origin": "https://www.tpex.org.tw",
    }


def load_sec_company_tickers_exchange_payload(
    session: requests.Session | None = None,
) -> tuple[dict[str, Any] | None, str]:
    for path in (SEC_COMPANY_TICKERS_CACHE, LEGACY_SEC_COMPANY_TICKERS_CACHE):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"

    try:
        payload = fetch_json(SEC_COMPANY_TICKERS_URL, session=session, headers=sec_request_headers())
    except requests.RequestException:
        return None, "unavailable"

    ensure_output_dirs()
    SEC_COMPANY_TICKERS_CACHE.write_text(json.dumps(payload), encoding="utf-8")
    return payload, "network"


def load_tpex_mainboard_quotes_payload(
    session: requests.Session | None = None,
) -> tuple[list[dict[str, Any]] | None, str]:
    for path in (TPEX_MAINBOARD_QUOTES_CACHE, LEGACY_TPEX_MAINBOARD_QUOTES_CACHE):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"

    try:
        payload = fetch_json(TPEX_MAINBOARD_QUOTES_URL, session=session, headers=tpex_request_headers())
    except requests.RequestException:
        return None, "unavailable"

    ensure_output_dirs()
    TPEX_MAINBOARD_QUOTES_CACHE.write_text(json.dumps(payload), encoding="utf-8")
    return payload, "network"


def parse_pipe_table(text: str) -> list[dict[str, str]]:
    lines = [line for line in text.splitlines() if line.strip()]
    if lines and lines[-1].lower().startswith("file creation time"):
        lines = lines[:-1]
    reader = csv.DictReader(io.StringIO("\n".join(lines)), delimiter="|")
    return [{key: (value or "").strip() for key, value in row.items()} for row in reader]


def parse_nasdaq_listed(text: str, source: MasterfileSource) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for row in parse_pipe_table(text):
        symbol = row.get("Symbol", "")
        if not symbol:
            continue
        rows.append(
            {
                "source_key": source.key,
                "provider": source.provider,
                "source_url": source.source_url,
                "ticker": symbol,
                "name": row.get("Security Name", ""),
                "exchange": "NASDAQ",
                "asset_type": "ETF" if row.get("ETF") == "Y" else "Stock",
                "listing_status": "test" if row.get("Test Issue") == "Y" else "active",
                "reference_scope": source.reference_scope,
                "official": "true",
            }
        )
    return rows


def parse_other_listed(text: str, source: MasterfileSource) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for row in parse_pipe_table(text):
        symbol = row.get("ACT Symbol", "")
        exchange = OTHER_LISTED_EXCHANGE_MAP.get(row.get("Exchange", ""), row.get("Exchange", ""))
        if not symbol or not exchange:
            continue
        rows.append(
            {
                "source_key": source.key,
                "provider": source.provider,
                "source_url": source.source_url,
                "ticker": symbol,
                "name": row.get("Security Name", ""),
                "exchange": exchange,
                "asset_type": "ETF" if row.get("ETF") == "Y" else "Stock",
                "listing_status": "test" if row.get("Test Issue") == "Y" else "active",
                "reference_scope": source.reference_scope,
                "official": "true",
            }
        )
    return rows


def parse_asx_listed_companies(text: str, source: MasterfileSource) -> list[dict[str, str]]:
    lines = text.splitlines()
    if lines and lines[0].startswith("ASX listed companies as at"):
        lines = lines[2:]
    reader = csv.DictReader(io.StringIO("\n".join(lines)))
    rows: list[dict[str, str]] = []
    for row in reader:
        ticker = (row.get("ASX code") or "").strip()
        if not ticker:
            continue
        name = (row.get("Company name") or "").strip()
        rows.append(
            {
                "source_key": source.key,
                "provider": source.provider,
                "source_url": source.source_url,
                "ticker": ticker,
                "name": name,
                "exchange": "ASX",
                "asset_type": infer_asset_type(name),
                "listing_status": "active",
                "reference_scope": source.reference_scope,
                "official": "true",
            }
        )
    return rows


def infer_jpx_asset_type(section: str, name: str) -> str:
    normalized = section.strip().lower()
    if "etf" in normalized or "etn" in normalized:
        return "ETF"
    return infer_asset_type(name)


def parse_jpx_listed_issues_excel(content: bytes, source: MasterfileSource) -> list[dict[str, str]]:
    dataframe = pd.read_excel(io.BytesIO(content))
    rows: list[dict[str, str]] = []
    for record in dataframe.to_dict(orient="records"):
        ticker = str(record.get("Local Code", "")).strip()
        if not ticker or ticker.lower() == "nan":
            continue
        name = str(record.get("Name (English)", "")).strip()
        section = str(record.get("Section/Products", "")).strip()
        if not name or not section or name.lower() == "nan" or section.lower() == "nan":
            continue
        rows.append(
            {
                "source_key": source.key,
                "provider": source.provider,
                "source_url": source.source_url,
                "ticker": ticker,
                "name": name,
                "exchange": "TSE",
                "asset_type": infer_jpx_asset_type(section, name),
                "listing_status": "active",
                "reference_scope": source.reference_scope,
                "official": "true",
            }
        )
    return rows


def map_deutsche_boerse_exchange(exchange_field: str) -> str:
    normalized = exchange_field.strip().upper()
    if "XETRA" in normalized or "FRANKFURT" in normalized:
        return "XETRA"
    return "XETRA"


def parse_deutsche_boerse_listed_companies_excel(content: bytes, source: MasterfileSource) -> list[dict[str, str]]:
    workbook = pd.ExcelFile(io.BytesIO(content))
    rows: list[dict[str, str]] = []
    for sheet_name in DEUTSCHE_BOERSE_SHEETS:
        if sheet_name not in workbook.sheet_names:
            continue
        dataframe = pd.read_excel(io.BytesIO(content), sheet_name=sheet_name, header=7)
        for record in dataframe.to_dict(orient="records"):
            ticker = str(record.get("Trading Symbol", "")).strip()
            name = str(record.get("Company", "")).strip()
            isin = str(record.get("ISIN", "")).strip()
            exchange_field = str(record.get("Instrument Exchange", "")).strip()
            if not ticker or not name or not isin or ticker.lower() == "nan" or name.lower() == "nan":
                continue
            rows.append(
                {
                    "source_key": source.key,
                    "provider": source.provider,
                    "source_url": source.source_url,
                    "ticker": ticker,
                    "name": name,
                    "exchange": map_deutsche_boerse_exchange(exchange_field),
                    "asset_type": "Stock",
                    "listing_status": "active",
                    "reference_scope": source.reference_scope,
                    "official": "true",
                }
            )
    return rows


def parse_tmx_interlisted(text: str, source: MasterfileSource) -> list[dict[str, str]]:
    lines = text.splitlines()
    if lines and lines[0].startswith("As of "):
        lines = lines[2:]
    reader = csv.DictReader(io.StringIO("\n".join(lines)), delimiter="\t")
    rows: list[dict[str, str]] = []
    for row in reader:
        symbol = (row.get("Symbol") or "").strip()
        if not symbol or ":" not in symbol:
            continue
        ticker, exchange = symbol.split(":", 1)
        rows.append(
            {
                "source_key": source.key,
                "provider": source.provider,
                "source_url": source.source_url,
                "ticker": ticker.strip(),
                "name": (row.get("Name") or "").strip(),
                "exchange": exchange.strip(),
                "asset_type": infer_asset_type((row.get("Name") or "").strip()),
                "listing_status": "active",
                "reference_scope": source.reference_scope,
                "official": "true",
            }
        )
    return rows


def map_euronext_market(market: str) -> str:
    normalized = market.strip()
    if normalized in EURONEXT_MARKET_MAP:
        return EURONEXT_MARKET_MAP[normalized]
    return "Euronext"


def euronext_reference_scope(market: str) -> str:
    normalized = market.strip()
    if normalized in EURONEXT_SECONDARY_MARKETS or "," in normalized:
        return "secondary_listing_subset"
    return "exchange_directory"


def parse_euronext_equities_download(text: str, source: MasterfileSource) -> list[dict[str, str]]:
    lines = [line for line in text.splitlines() if line.strip()]
    if len(lines) < 4:
        return []

    data_lines = [line.lstrip("\ufeff") for line in lines]
    reader = csv.DictReader(io.StringIO("\n".join([data_lines[0], *data_lines[4:]])), delimiter=";")
    rows: list[dict[str, str]] = []
    for row in reader:
        ticker = (row.get("Symbol") or "").strip()
        name = (row.get("Name") or "").strip()
        market = (row.get("Market") or "").strip()
        isin = (row.get("ISIN") or "").strip()
        if not ticker or not name or not market or not isin:
            continue
        rows.append(
            {
                "source_key": source.key,
                "provider": source.provider,
                "source_url": source.source_url,
                "ticker": ticker,
                "name": name,
                "exchange": map_euronext_market(market),
                "asset_type": infer_asset_type(name),
                "listing_status": "active",
                "reference_scope": euronext_reference_scope(market),
                "official": "true",
            }
        )
    return rows


def parse_sec_company_tickers_exchange(payload: dict[str, Any], source: MasterfileSource) -> list[dict[str, str]]:
    fields = payload.get("fields", [])
    rows: list[dict[str, str]] = []
    for values in payload.get("data", []):
        record = dict(zip(fields, values))
        ticker = str(record.get("ticker", "")).strip()
        exchange = SEC_EXCHANGE_MAP.get(str(record.get("exchange", "")).strip())
        if not ticker or not exchange:
            continue
        name = str(record.get("name", "")).strip()
        rows.append(
            {
                "source_key": source.key,
                "provider": source.provider,
                "source_url": source.source_url,
                "ticker": ticker,
                "name": name,
                "exchange": exchange,
                "asset_type": infer_asset_type(name),
                "listing_status": "active",
                "reference_scope": source.reference_scope,
                "official": "true",
            }
        )
    return rows


def parse_twse_listed_companies(payload: list[dict[str, Any]], source: MasterfileSource) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for record in payload:
        ticker = str(record.get("公司代號", "")).strip()
        name = str(record.get("公司名稱", "")).strip()
        if not ticker or not name:
            continue
        rows.append(
            {
                "source_key": source.key,
                "provider": source.provider,
                "source_url": source.source_url,
                "ticker": ticker,
                "name": name,
                "exchange": "TWSE",
                "asset_type": infer_taiwan_asset_type(ticker, name),
                "listing_status": "active",
                "reference_scope": source.reference_scope,
                "official": "true",
            }
        )
    return rows


def parse_tpex_mainboard_quotes(payload: list[dict[str, Any]], source: MasterfileSource) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for record in payload:
        ticker = str(record.get("SecuritiesCompanyCode", "")).strip()
        name = str(record.get("CompanyName", "")).strip()
        if not ticker or not name or not TPEX_CANONICAL_TICKER_RE.fullmatch(ticker):
            continue
        rows.append(
            {
                "source_key": source.key,
                "provider": source.provider,
                "source_url": source.source_url,
                "ticker": ticker,
                "name": name,
                "exchange": "TPEX",
                "asset_type": infer_taiwan_asset_type(ticker, name),
                "listing_status": "active",
                "reference_scope": source.reference_scope,
                "official": "true",
            }
        )
    return rows


def parse_b3_instruments_equities_table(table: dict[str, Any], source: MasterfileSource) -> list[dict[str, str]]:
    columns = [column.get("name", "") for column in table.get("columns") or []]
    rows: list[dict[str, str]] = []
    for values in table.get("values") or []:
        record = dict(zip(columns, values))
        market = str(record.get("MktNm", "")).strip()
        segment = str(record.get("SgmtNm", "")).strip()
        category = str(record.get("SctyCtgyNm", "")).strip()
        ticker = str(record.get("TckrSymb", "")).strip()
        name = str(record.get("CrpnNm") or record.get("AsstDesc") or "").strip()
        if market != "EQUITY-CASH" or segment != "CASH":
            continue
        asset_type = B3_ALLOWED_CASH_CATEGORIES.get(category)
        if not asset_type or not ticker or not name:
            continue
        normalized_name = name.lower()
        normalized_desc = str(record.get("AsstDesc") or "").lower()
        if ticker.startswith("TAXA") or any(marker in normalized_name for marker in B3_EXCLUDED_ISSUER_MARKERS):
            continue
        if any(marker in normalized_desc for marker in B3_EXCLUDED_ISSUER_MARKERS):
            continue
        rows.append(
            {
                "source_key": source.key,
                "provider": source.provider,
                "source_url": source.source_url,
                "ticker": ticker,
                "name": name,
                "exchange": "B3",
                "asset_type": asset_type,
                "listing_status": "active",
                "reference_scope": source.reference_scope,
                "official": "true",
            }
        )
    return rows


def fetch_b3_instruments_equities(source: MasterfileSource, session: requests.Session | None = None) -> list[dict[str, str]]:
    session = session or requests.Session()
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    workday_response = session.get("https://arquivos.b3.com.br/bdi/table/workday", headers=headers, timeout=REQUEST_TIMEOUT)
    workday_response.raise_for_status()
    workday = str(workday_response.json())[:10]

    rows: list[dict[str, str]] = []
    page = 1
    take = 1000
    page_count = 1
    while page <= page_count:
        response = session.post(
            f"{source.source_url}/{workday}/{workday}/{page}/{take}",
            headers=headers,
            json={},
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        payload = response.json()
        table = payload.get("table") or {}
        rows.extend(parse_b3_instruments_equities_table(table, source))
        page_count = int(table.get("pageCount") or 0) or page_count
        page += 1
    return rows


def fetch_source_rows(source: MasterfileSource, session: requests.Session | None = None) -> list[dict[str, str]]:
    if source.format == "nasdaq_listed_pipe":
        text = fetch_text(source.source_url, session=session)
        return parse_nasdaq_listed(text, source)
    if source.format == "nasdaq_other_listed_pipe":
        text = fetch_text(source.source_url, session=session)
        return parse_other_listed(text, source)
    if source.format == "asx_listed_companies_csv":
        text = fetch_text(source.source_url, session=session)
        return parse_asx_listed_companies(text, source)
    if source.format == "tmx_interlisted_tab":
        text = fetch_text(source.source_url, session=session)
        return parse_tmx_interlisted(text, source)
    if source.format == "euronext_equities_semicolon_csv":
        text = fetch_text(source.source_url, session=session)
        return parse_euronext_equities_download(text, source)
    if source.format == "jpx_listed_issues_excel":
        content = fetch_bytes(source.source_url, session=session)
        return parse_jpx_listed_issues_excel(content, source)
    if source.format == "deutsche_boerse_listed_companies_excel":
        content = fetch_bytes(source.source_url, session=session)
        return parse_deutsche_boerse_listed_companies_excel(content, source)
    if source.format == "b3_instruments_equities_api":
        return fetch_b3_instruments_equities(source, session=session)
    if source.format == "twse_listed_companies_json":
        payload = fetch_json(source.source_url, session=session)
        return parse_twse_listed_companies(payload, source)
    if source.format == "tpex_mainboard_quotes_json":
        payload = fetch_json(source.source_url, session=session)
        return parse_tpex_mainboard_quotes(payload, source)
    if source.format == "sec_company_tickers_exchange_json":
        payload = fetch_json(source.source_url, session=session)
        return parse_sec_company_tickers_exchange(payload, source)
    raise ValueError(f"Unsupported source format: {source.format}")


def fetch_source_rows_with_mode(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]], str]:
    if source.format == "sec_company_tickers_exchange_json":
        payload, mode = load_sec_company_tickers_exchange_payload(session=session)
        if payload is None:
            raise requests.RequestException("SEC company_tickers_exchange.json unavailable")
        return parse_sec_company_tickers_exchange(payload, source), mode
    if source.format == "tpex_mainboard_quotes_json":
        payload, mode = load_tpex_mainboard_quotes_payload(session=session)
        if payload is None:
            raise requests.RequestException("TPEX mainboard quotes unavailable")
        return parse_tpex_mainboard_quotes(payload, source), mode
    return fetch_source_rows(source, session=session), "network"


def dedupe_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    deduped: dict[tuple[str, str, str, str], dict[str, str]] = {}
    for row in rows:
        key = (
            row["source_key"],
            row["ticker"],
            row["exchange"],
            row["listing_status"],
            row.get("reference_scope", "exchange_directory"),
        )
        deduped[key] = row
    return sorted(deduped.values(), key=lambda row: (row["exchange"], row["ticker"], row["source_key"]))


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def build_summary(
    rows: list[dict[str, str]],
    source_modes: dict[str, str] | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    exchanges = sorted({row["exchange"] for row in rows if row["exchange"]})
    source_counts: dict[str, int] = {}
    for row in rows:
        source_counts[row["source_key"]] = source_counts.get(row["source_key"], 0) + 1
    source_details = {
        source.key: {
            "provider": source.provider,
            "reference_scope": source.reference_scope,
            "official": source.official,
            "mode": source_modes.get(source.key, "unknown") if source_modes else "unknown",
            "rows": source_counts.get(source.key, 0),
            "generated_at": generated_at or "",
        }
        for source in OFFICIAL_SOURCES
    }
    summary = {
        "generated_at": generated_at or "",
        "rows": len(rows),
        "exchanges": exchanges,
        "source_counts": source_counts,
        "source_details": source_details,
    }
    if source_modes:
        summary["source_modes"] = source_modes
    return summary


def fetch_all_sources(
    session: requests.Session | None = None,
    include_manual: bool = True,
    manual_dir: Path | None = None,
) -> tuple[list[dict[str, str]], dict[str, Any]]:
    ensure_output_dirs()
    session = session or requests.Session()
    rows: list[dict[str, str]] = []
    errors: list[dict[str, str]] = []
    source_modes: dict[str, str] = {}
    generated_at = utc_now_iso()
    for source in OFFICIAL_SOURCES:
        try:
            source_rows, mode = fetch_source_rows_with_mode(source, session=session)
            rows.extend(source_rows)
            source_modes[source.key] = mode
        except requests.RequestException as exc:
            source_modes[source.key] = "unavailable"
            errors.append({"source_key": source.key, "error": str(exc)})
    if include_manual:
        rows.extend(load_manual_masterfiles(manual_dir or MASTERFILES_DIR / "manual"))
    deduped = dedupe_rows(rows)
    summary = build_summary(deduped, source_modes=source_modes, generated_at=generated_at)
    if errors:
        summary["errors"] = errors
    return deduped, summary


def persist_source_metadata() -> None:
    ensure_output_dirs()
    payload = [asdict(source) for source in OFFICIAL_SOURCES]
    MASTERFILE_SOURCES_JSON.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> None:
    rows, summary = fetch_all_sources()
    persist_source_metadata()
    write_csv(
        MASTERFILE_REFERENCE_CSV,
        ["source_key", "provider", "source_url", "ticker", "name", "exchange", "asset_type", "listing_status", "reference_scope", "official"],
        rows,
    )
    MASTERFILE_SUMMARY_JSON.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps({"reference_csv": str(MASTERFILE_REFERENCE_CSV.relative_to(ROOT)), **summary}, indent=2))


if __name__ == "__main__":
    main()

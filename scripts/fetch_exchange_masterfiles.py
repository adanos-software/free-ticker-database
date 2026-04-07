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
LSE_COMPANY_REPORTS_CACHE = MASTERFILE_CACHE_DIR / "lse_company_reports.json"
LEGACY_LSE_COMPANY_REPORTS_CACHE = MASTERFILES_DIR / "lse_company_reports.json"
TMX_LISTED_ISSUERS_CACHE = MASTERFILE_CACHE_DIR / "tmx_listed_issuers.xlsx"
LEGACY_TMX_LISTED_ISSUERS_CACHE = MASTERFILES_DIR / "tmx_listed_issuers.xlsx"
TPEX_MAINBOARD_QUOTES_CACHE = MASTERFILE_CACHE_DIR / "tpex_mainboard_daily_close_quotes.json"
LEGACY_TPEX_MAINBOARD_QUOTES_CACHE = MASTERFILES_DIR / "tpex_mainboard_daily_close_quotes.json"

SEC_COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers_exchange.json"
NASDAQ_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt"
NASDAQ_OTHER_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/symdir/otherlisted.txt"
LSE_COMPANY_REPORTS_URL = (
    "https://www.londonstockexchange.com/exchange/instrument-result.html"
    "?filterBy=CompanyReports&filterClause=1&initial={initial}&page={page}"
)
ASX_LISTED_URL = "https://www.asx.com.au/asx/research/ASXListedCompanies.csv"
TMX_INTERLISTED_URL = "https://www.tsx.com/files/trading/interlisted-companies.txt"
TMX_LISTED_ISSUERS_ARCHIVE_URL = "https://www.tsx.com/en/listings/current-market-statistics/mig-archives"
EURONEXT_EQUITIES_DOWNLOAD_URL = "https://live.euronext.com/pd_es/data/stocks/download?mics=dm_all_stock"
JPX_LISTED_ISSUES_URL = "https://www.jpx.co.jp/english/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_e.xls"
DEUTSCHE_BOERSE_LISTED_URL = "https://www.cashmarket.deutsche-boerse.com/resource/blob/67858/dd766fc6588100c79294324175f95501/data/Listed-companies.xlsx"
DEUTSCHE_BOERSE_ETPS_URL = "https://www.cashmarket.deutsche-boerse.com/resource/blob/1553442/2936716b8f6c2d7a0bb85337485bdcdb/data/Master_DataSheet_Download.xls"
DEUTSCHE_BOERSE_XETRA_ALL_TRADABLE_URL = "https://www.cashmarket.deutsche-boerse.com/resource/blob/1528/b52ea43a2edac92e8283d40645d1c076/data/t7-xetr-allTradableInstruments.csv"
B3_INSTRUMENTS_EQUITIES_URL = "https://arquivos.b3.com.br/bdi/table/InstrumentsEquities"
TWSE_LISTED_COMPANIES_URL = "https://openapi.twse.com.tw/v1/opendata/t187ap03_L"
SZSE_STOCK_LIST_URL = "https://www.szse.cn/market/product/stock/list/index.html"
SZSE_REPORT_DATA_URL = "https://www.szse.cn/api/report/ShowReport/data"
SZSE_A_SHARE_CATALOG_ID = "1110"
SZSE_A_SHARE_TAB_KEY = "tab1"
SSE_STOCK_LIST_URL = "https://www.sse.com.cn/assortment/stock/list/share/"
SSE_ETF_LIST_URL = "https://www.sse.com.cn/assortment/fund/etf/list/"
SSE_COMMON_QUERY_URL = "https://query.sse.com.cn/sseQuery/commonQuery.do"
SSE_JSONP_CALLBACK = "jsonpCallback"
TPEX_MAINBOARD_QUOTES_URL = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes"
KRX_LISTED_COMPANIES_URL = "https://global.krx.co.kr/contents/GLB/03/0308/0308010000/GLB0308010000.jsp"
KRX_DATA_URL = "https://global.krx.co.kr/contents/GLB/99/GLB99000001.jspx"
KRX_GENERATE_OTP_URL = "https://global.krx.co.kr/contents/COM/GenerateOTP.jspx"
KRX_JSON_DATA_URL = "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"

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
LSE_PAGE_INITIALS = tuple("ABCDEFGHIJKLMNOPQRSTUVWXYZ") + ("0",)
TMX_LISTED_ISSUERS_HREF_RE = re.compile(
    r'href="([^"]+tsx-tsxv-listed-issuers-(\d{4})-(\d{2})-en\.xlsx)"',
    re.I,
)
KRX_MARKET_GUBUN_TO_EXCHANGE = {
    "1": "KRX",
    "2": "KOSDAQ",
}
SSE_JSONP_RE = re.compile(r"^[^(]+\((.*)\)\s*$", re.S)
SSE_ETF_SUBCLASSES = ("01", "02", "03", "06", "08", "09", "31", "32", "33", "37")


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
        key="lse_company_reports",
        provider="LSE",
        description="Official London Stock Exchange company reports directory",
        source_url=LSE_COMPANY_REPORTS_URL,
        format="lse_company_reports_html",
        reference_scope="listed_companies_subset",
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
        key="tmx_listed_issuers",
        provider="TMX",
        description="Official TMX TSX/TSXV listed issuers workbook",
        source_url=TMX_LISTED_ISSUERS_ARCHIVE_URL,
        format="tmx_listed_issuers_excel",
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
        key="deutsche_boerse_etfs_etps",
        provider="Deutsche Boerse",
        description="Official Deutsche Boerse Xetra ETFs and ETPs master workbook",
        source_url=DEUTSCHE_BOERSE_ETPS_URL,
        format="deutsche_boerse_etfs_etps_excel",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="deutsche_boerse_xetra_all_tradable_equities",
        provider="Deutsche Boerse",
        description="Official Deutsche Boerse Xetra all tradable instruments directory (equities subset)",
        source_url=DEUTSCHE_BOERSE_XETRA_ALL_TRADABLE_URL,
        format="deutsche_boerse_xetra_all_tradable_csv",
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
        key="sse_a_share_list",
        provider="SSE",
        description="Official SSE A-share list",
        source_url=SSE_STOCK_LIST_URL,
        format="sse_a_share_list_jsonp",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="sse_etf_list",
        provider="SSE",
        description="Official SSE ETF list",
        source_url=SSE_ETF_LIST_URL,
        format="sse_etf_list_json",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="szse_a_share_list",
        provider="SZSE",
        description="Official SZSE A-share list",
        source_url=SZSE_STOCK_LIST_URL,
        format="szse_a_share_list_json",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="tpex_mainboard_daily_quotes",
        provider="TPEX",
        description="Official TPEX mainboard daily quotes open data feed",
        source_url=TPEX_MAINBOARD_QUOTES_URL,
        format="tpex_mainboard_quotes_json",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="krx_listed_companies",
        provider="KRX",
        description="Official KRX listed company directory",
        source_url=KRX_LISTED_COMPANIES_URL,
        format="krx_listed_companies_json",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="krx_etf_finder",
        provider="KRX",
        description="Official KRX ETF issue finder",
        source_url="https://data.krx.co.kr/contents/MDC/MAIN/main/index.cmd",
        format="krx_etf_finder_json",
        reference_scope="listed_companies_subset",
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


def infer_tmx_listed_asset_type(name: str, sector: str) -> str:
    if sector.strip().lower() == "etp":
        return "ETF"
    return infer_asset_type(name)


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


def krx_request_headers() -> dict[str, str]:
    return {
        "User-Agent": USER_AGENT,
        "Accept": "application/json,text/plain,*/*",
        "Accept-Encoding": "gzip, deflate",
        "Referer": KRX_LISTED_COMPANIES_URL,
        "Origin": "https://global.krx.co.kr",
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


def load_lse_company_reports_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    for path in (LSE_COMPANY_REPORTS_CACHE, LEGACY_LSE_COMPANY_REPORTS_CACHE):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"

    rows = fetch_lse_company_reports(source, session=session)
    ensure_output_dirs()
    LSE_COMPANY_REPORTS_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def resolve_tmx_listed_issuers_download_url(session: requests.Session | None = None) -> str:
    session = session or requests.Session()
    html = fetch_text(TMX_LISTED_ISSUERS_ARCHIVE_URL, session=session)
    matches = TMX_LISTED_ISSUERS_HREF_RE.findall(html)
    if not matches:
        raise ValueError("Unable to locate TMX listed issuers workbook link")
    href, _, _ = max(matches, key=lambda item: (int(item[1]), int(item[2])))
    return requests.compat.urljoin(TMX_LISTED_ISSUERS_ARCHIVE_URL, href)


def load_tmx_listed_issuers_content(
    session: requests.Session | None = None,
) -> tuple[bytes | None, str]:
    for path in (TMX_LISTED_ISSUERS_CACHE, LEGACY_TMX_LISTED_ISSUERS_CACHE):
        if path.exists():
            return path.read_bytes(), "cache"

    try:
        download_url = resolve_tmx_listed_issuers_download_url(session=session)
        content = fetch_bytes(download_url, session=session)
    except (requests.RequestException, ValueError):
        return None, "unavailable"

    ensure_output_dirs()
    TMX_LISTED_ISSUERS_CACHE.write_bytes(content)
    return content, "network"


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


def parse_lse_company_reports_html(text: str, source: MasterfileSource) -> list[dict[str, str]]:
    try:
        dataframes = pd.read_html(io.StringIO(text))
    except ValueError:
        return []
    if not dataframes:
        return []
    dataframe = dataframes[0]
    if "Code" not in dataframe.columns or "Name" not in dataframe.columns:
        return []

    rows: list[dict[str, str]] = []
    for record in dataframe.to_dict(orient="records"):
        ticker = str(record.get("Code", "")).strip()
        name = str(record.get("Name", "")).strip()
        if not ticker or not name or ticker.lower() == "nan" or name.lower() == "nan":
            continue
        rows.append(
            {
                "source_key": source.key,
                "provider": source.provider,
                "source_url": source.source_url,
                "ticker": ticker,
                "name": name,
                "exchange": "LSE",
                "asset_type": infer_asset_type(name),
                "listing_status": "active",
                "reference_scope": source.reference_scope,
                "official": "true",
            }
        )
    return rows


def parse_krx_listed_companies(
    payload: list[dict[str, Any]],
    source: MasterfileSource,
    *,
    exchange: str,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for record in payload:
        ticker = str(record.get("isu_cd", "")).strip()
        name = str(record.get("eng_cor_nm", "")).strip()
        if not ticker or not name or ticker.lower() == "nan" or name.lower() == "nan":
            continue
        rows.append(
            {
                "source_key": source.key,
                "provider": source.provider,
                "source_url": source.source_url,
                "ticker": ticker,
                "name": name,
                "exchange": exchange,
                "asset_type": "Stock",
                "listing_status": "active",
                "reference_scope": source.reference_scope,
                "official": "true",
            }
        )
    return rows


def parse_krx_etf_finder(
    payload: dict[str, Any],
    source: MasterfileSource,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for record in payload.get("block1", []):
        ticker = str(record.get("short_code", "")).strip()
        name = str(record.get("codeName", "")).strip()
        if not ticker or not name or ticker.lower() == "nan" or name.lower() == "nan":
            continue
        rows.append(
            {
                "source_key": source.key,
                "provider": source.provider,
                "source_url": source.source_url,
                "ticker": ticker,
                "name": name,
                "exchange": "KRX",
                "asset_type": "ETF",
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


def parse_deutsche_boerse_etfs_etps_excel(content: bytes, source: MasterfileSource) -> list[dict[str, str]]:
    dataframe = pd.read_excel(io.BytesIO(content), sheet_name="ETFs & ETPs", header=8)
    rows: list[dict[str, str]] = []
    for record in dataframe.to_dict(orient="records"):
        ticker = str(record.get("XETRA SYMBOL", "")).strip()
        name = str(record.get("PRODUCT NAME", "")).strip()
        if not ticker or not name or ticker.lower() == "nan" or name.lower() == "nan":
            continue
        rows.append(
            {
                "source_key": source.key,
                "provider": source.provider,
                "source_url": source.source_url,
                "ticker": ticker,
                "name": name,
                "exchange": "XETRA",
                "asset_type": "ETF",
                "listing_status": "active",
                "reference_scope": source.reference_scope,
                "official": "true",
            }
        )
    return rows


def parse_deutsche_boerse_xetra_all_tradable_csv(text: str, source: MasterfileSource) -> list[dict[str, str]]:
    lines = text.splitlines()
    if len(lines) < 3:
        return []

    reader = csv.DictReader(io.StringIO("\n".join(lines[2:])), delimiter=";")
    rows: list[dict[str, str]] = []
    for record in reader:
        if record.get("Product Status") != "Active":
            continue
        if record.get("Instrument Status") != "Active":
            continue
        if record.get("MIC Code") != "XETR":
            continue
        if record.get("Instrument Type") != "CS":
            continue
        ticker = str(record.get("Mnemonic", "")).strip()
        name = str(record.get("Instrument", "")).strip()
        if not ticker or not name or ticker.lower() == "nan" or name.lower() == "nan":
            continue

        rows.append(
            {
                "source_key": source.key,
                "provider": source.provider,
                "source_url": source.source_url,
                "ticker": ticker,
                "name": name,
                "exchange": "XETRA",
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


def parse_tmx_listed_issuers_excel(content: bytes, source: MasterfileSource) -> list[dict[str, str]]:
    workbook = pd.ExcelFile(io.BytesIO(content))
    rows: list[dict[str, str]] = []
    for sheet_name in workbook.sheet_names:
        if sheet_name.startswith("TSX Issuers"):
            default_exchange = "TSX"
        elif sheet_name.startswith("TSXV Issuers"):
            default_exchange = "TSXV"
        else:
            continue

        dataframe = workbook.parse(sheet_name=sheet_name, header=9)
        for record in dataframe.to_dict(orient="records"):
            ticker = str(record.get("Root\nTicker", "")).strip()
            name = str(record.get("Name", "")).strip()
            exchange = str(record.get("Exchange", "")).strip() or default_exchange
            sector = str(record.get("Sector", "")).strip()
            if not ticker or not name or ticker.lower() == "nan" or name.lower() == "nan":
                continue
            rows.append(
                {
                    "source_key": source.key,
                    "provider": source.provider,
                    "source_url": source.source_url,
                    "ticker": ticker,
                    "name": name,
                    "exchange": exchange,
                    "asset_type": infer_tmx_listed_asset_type(name, sector),
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


def parse_sse_jsonp(text: str) -> dict[str, Any]:
    match = SSE_JSONP_RE.match(text.strip())
    if not match:
        raise ValueError("Invalid SSE JSONP payload")
    return json.loads(match.group(1))


def parse_sse_a_share_list(payload: dict[str, Any], source: MasterfileSource) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for record in payload.get("result", []) or []:
        ticker = str(record.get("A_STOCK_CODE", "")).strip()
        name = str(record.get("FULL_NAME", "")).strip() or str(record.get("SEC_NAME_CN", "")).strip()
        if not ticker or not name:
            continue
        rows.append(
            {
                "source_key": source.key,
                "provider": source.provider,
                "source_url": source.source_url,
                "ticker": ticker,
                "name": name,
                "exchange": "SSE",
                "asset_type": "Stock",
                "listing_status": "active",
                "reference_scope": source.reference_scope,
                "official": "true",
            }
        )
    return rows


def parse_sse_etf_list(payload: dict[str, Any], source: MasterfileSource) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    records = payload.get("result") or (payload.get("pageHelp") or {}).get("data") or []
    for record in records:
        ticker = str(record.get("fundCode", "")).strip()
        name = str(record.get("secNameFull", "")).strip() or str(record.get("fundAbbr", "")).strip()
        if not ticker or not name:
            continue
        rows.append(
            {
                "source_key": source.key,
                "provider": source.provider,
                "source_url": source.source_url,
                "ticker": ticker,
                "name": name,
                "exchange": "SSE",
                "asset_type": "ETF",
                "listing_status": "active",
                "reference_scope": source.reference_scope,
                "official": "true",
            }
        )
    return rows


def strip_html_tags(value: str) -> str:
    return re.sub(r"<[^>]+>", "", value).strip()


def parse_szse_a_share_list(payload: dict[str, Any] | list[dict[str, Any]], source: MasterfileSource) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    sections: list[dict[str, Any]] = []
    if isinstance(payload, list):
        sections = [section for section in payload if isinstance(section, dict)]
    elif isinstance(payload, dict):
        sections = [
            report
            for nested in payload.values()
            if isinstance(nested, list)
            for report in nested
            if isinstance(report, dict)
        ]

    for report in sections:
        for record in report.get("data", []) or []:
            ticker = str(record.get("agdm", "")).strip()
            name = strip_html_tags(str(record.get("agjc", "")).strip())
            if not ticker or not name:
                continue
            rows.append(
                {
                    "source_key": source.key,
                    "provider": source.provider,
                    "source_url": source.source_url,
                    "ticker": ticker,
                    "name": name,
                    "exchange": "SZSE",
                    "asset_type": "Stock",
                    "listing_status": "active",
                    "reference_scope": source.reference_scope,
                    "official": "true",
                }
            )
    return rows


def fetch_szse_a_share_list(source: MasterfileSource, session: requests.Session | None = None) -> list[dict[str, str]]:
    session = session or requests.Session()
    headers = {
        "User-Agent": USER_AGENT,
        "Referer": source.source_url,
        "Connection": "close",
    }
    session.get(source.source_url, headers={"User-Agent": USER_AGENT, "Connection": "close"}, timeout=REQUEST_TIMEOUT)
    rows: list[dict[str, str]] = []
    page = 1
    total_pages = 1
    while page <= total_pages:
        params = {
            "CATALOGID": SZSE_A_SHARE_CATALOG_ID,
            "TABKEY": SZSE_A_SHARE_TAB_KEY,
            "PAGENO": page,
            "random": f"{page / 1000:.3f}",
        }
        payload: dict[str, Any] | list[dict[str, Any]] | None = None
        last_error: Exception | None = None
        for _attempt in range(3):
            try:
                response = session.get(
                    SZSE_REPORT_DATA_URL,
                    params=params,
                    headers=headers,
                    timeout=REQUEST_TIMEOUT,
                )
                response.raise_for_status()
                payload = response.json()
                break
            except (requests.RequestException, ValueError) as exc:
                last_error = exc
        if payload is None:
            if isinstance(last_error, requests.RequestException):
                raise last_error
            raise requests.RequestException("SZSE A-share list unavailable")
        page_rows = parse_szse_a_share_list(payload, source)
        if not page_rows:
            break
        rows.extend(page_rows)
        if isinstance(payload, list):
            sections = [section for section in payload if isinstance(section, dict)]
        elif isinstance(payload, dict):
            sections = [
                report
                for nested in payload.values()
                if isinstance(nested, list)
                for report in nested
                if isinstance(report, dict)
            ]
        else:
            sections = []
        metadata = sections[0].get("metadata", {}) if sections else {}
        total_pages = int(metadata.get("pagecount") or total_pages)
        page += 1
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


def fetch_sse_a_share_list(source: MasterfileSource, session: requests.Session | None = None) -> list[dict[str, str]]:
    session = session or requests.Session()
    headers = {
        "User-Agent": USER_AGENT,
        "Referer": source.source_url,
        "Connection": "close",
    }
    rows: list[dict[str, str]] = []
    page = 1
    total_pages = 1
    while page <= total_pages:
        response = session.get(
            SSE_COMMON_QUERY_URL,
            params={
                "jsonCallBack": SSE_JSONP_CALLBACK,
                "sqlId": "COMMON_SSE_CP_GPJCTPZ_GPLB_GP_L",
                "STOCK_TYPE": "1",
                "COMPANY_STATUS": "2,4,5,7,8",
                "type": "inParams",
                "isPagination": "true",
                "pageHelp.cacheSize": "1",
                "pageHelp.beginPage": "1",
                "pageHelp.pageSize": "500",
                "pageHelp.pageNo": str(page),
            },
            headers=headers,
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        payload = parse_sse_jsonp(response.text)
        page_rows = parse_sse_a_share_list(payload, source)
        if not page_rows:
            break
        rows.extend(page_rows)
        total_pages = int((payload.get("pageHelp") or {}).get("pageCount") or total_pages)
        page += 1
    return rows


def fetch_sse_etf_list(source: MasterfileSource, session: requests.Session | None = None) -> list[dict[str, str]]:
    session = session or requests.Session()
    headers = {
        "User-Agent": USER_AGENT,
        "Referer": source.source_url,
        "Accept": "application/json,text/plain,*/*",
        "Connection": "close",
    }
    rows: list[dict[str, str]] = []
    for subclass in SSE_ETF_SUBCLASSES:
        page = 1
        total_pages = 1
        while page <= total_pages:
            response = session.get(
                "https://query.sse.com.cn/commonSoaQuery.do",
                params={
                    "isPagination": "true",
                    "pageHelp.pageSize": "500",
                    "pageHelp.pageNo": str(page),
                    "pageHelp.beginPage": "1",
                    "pageHelp.cacheSize": "1",
                    "pageHelp.endPage": "1",
                    "pagecache": "false",
                    "sqlId": "FUND_LIST",
                    "fundType": "00",
                    "subClass": subclass,
                },
                headers=headers,
                timeout=REQUEST_TIMEOUT,
            )
            response.raise_for_status()
            payload = response.json()
            page_rows = parse_sse_etf_list(payload, source)
            if not page_rows:
                break
            rows.extend(page_rows)
            total_pages = int((payload.get("pageHelp") or {}).get("pageCount") or total_pages)
            page += 1
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


def fetch_lse_company_reports(source: MasterfileSource, session: requests.Session | None = None) -> list[dict[str, str]]:
    session = session or requests.Session()
    rows: list[dict[str, str]] = []
    for initial in LSE_PAGE_INITIALS:
        page = 1
        seen_signatures: set[tuple[tuple[str, str], ...]] = set()
        while True:
            text = fetch_text(source.source_url.format(initial=initial, page=page), session=session)
            page_rows = parse_lse_company_reports_html(text, source)
            if not page_rows:
                break
            signature = tuple((row["ticker"], row["name"]) for row in page_rows[:5])
            if signature in seen_signatures:
                break
            seen_signatures.add(signature)
            rows.extend(page_rows)
            if f"initial={initial}&page={page + 1}" not in text:
                break
            page += 1
    return rows


def fetch_krx_listed_companies(source: MasterfileSource, session: requests.Session | None = None) -> list[dict[str, str]]:
    session = session or requests.Session()
    session.headers.update(krx_request_headers())
    page_html = session.get(source.source_url, timeout=REQUEST_TIMEOUT).text
    page_soup = pd.read_html(io.StringIO(page_html))
    if not page_soup:
        raise requests.RequestException("KRX listed companies page unavailable")

    rows: list[dict[str, str]] = []
    for market_gubun, exchange in KRX_MARKET_GUBUN_TO_EXCHANGE.items():
        otp_response = session.get(
            KRX_GENERATE_OTP_URL,
            params={"name": "tablesubmit", "bld": "GLB/03/0308/0308010000/glb0308010000"},
            timeout=REQUEST_TIMEOUT,
        )
        otp_response.raise_for_status()
        form_data = {
            "market_gubun": market_gubun,
            "isu_cdnm": "All",
            "isu_cd": "",
            "isu_nm": "",
            "isu_srt_cd": "",
            "sort": "",
            "detailSch": "",
            "ck_std_ind_cd": "Y",
            "std_ind_cd": "",
            "ck_par_pr": "Y",
            "par_pr": "",
            "ck_cpta_scl": "Y",
            "cpta_scl": "",
            "ck_sttl_trm": "Y",
            "sttl_trm": "",
            "ck_lst_stk_vl": "Y",
            "lst_stk_vl": "",
            "in_lst_stk_vl": "",
            "in_lst_stk_vl2": "",
            "ck_cpt": "Y",
            "cpt": "",
            "in_cpt": "",
            "in_cpt2": "",
            "ck_nat_tot_amt": "Y",
            "nat_tot_amt": "",
            "in_nat_tot_amt": "",
            "in_nat_tot_amt2": "",
            "pagePath": "/contents/GLB/03/0308/0308010000/GLB0308010000.jsp",
            "code": otp_response.text.strip(),
            "bldcode": "GLB/03/0308/0308010000/glb0308010000",
        }
        response = session.post(KRX_DATA_URL, data=form_data, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        payload = response.json().get("block1", [])
        rows.extend(parse_krx_listed_companies(payload, source, exchange=exchange))
    return rows


def fetch_krx_etf_finder(source: MasterfileSource, session: requests.Session | None = None) -> list[dict[str, str]]:
    session = session or requests.Session()
    response = session.post(
        KRX_JSON_DATA_URL,
        data={
            "bld": "dbms/comm/finder/finder_secuprodisu",
            "mktsel": "ETF",
            "searchText": "",
        },
        headers={
            "User-Agent": USER_AGENT,
            "Referer": "https://data.krx.co.kr/contents/MDC/MAIN/main/index.cmd",
        },
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return parse_krx_etf_finder(response.json(), source)


def fetch_source_rows(source: MasterfileSource, session: requests.Session | None = None) -> list[dict[str, str]]:
    if source.format == "nasdaq_listed_pipe":
        text = fetch_text(source.source_url, session=session)
        return parse_nasdaq_listed(text, source)
    if source.format == "nasdaq_other_listed_pipe":
        text = fetch_text(source.source_url, session=session)
        return parse_other_listed(text, source)
    if source.format == "lse_company_reports_html":
        return fetch_lse_company_reports(source, session=session)
    if source.format == "asx_listed_companies_csv":
        text = fetch_text(source.source_url, session=session)
        return parse_asx_listed_companies(text, source)
    if source.format == "tmx_listed_issuers_excel":
        content, mode = load_tmx_listed_issuers_content(session=session)
        if content is None:
            return []
        return parse_tmx_listed_issuers_excel(content, source)
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
    if source.format == "deutsche_boerse_etfs_etps_excel":
        content = fetch_bytes(source.source_url, session=session)
        return parse_deutsche_boerse_etfs_etps_excel(content, source)
    if source.format == "deutsche_boerse_xetra_all_tradable_csv":
        text = fetch_text(source.source_url, session=session)
        return parse_deutsche_boerse_xetra_all_tradable_csv(text, source)
    if source.format == "b3_instruments_equities_api":
        return fetch_b3_instruments_equities(source, session=session)
    if source.format == "twse_listed_companies_json":
        payload = fetch_json(source.source_url, session=session)
        return parse_twse_listed_companies(payload, source)
    if source.format == "sse_a_share_list_jsonp":
        return fetch_sse_a_share_list(source, session=session)
    if source.format == "sse_etf_list_json":
        return fetch_sse_etf_list(source, session=session)
    if source.format == "szse_a_share_list_json":
        return fetch_szse_a_share_list(source, session=session)
    if source.format == "tpex_mainboard_quotes_json":
        payload = fetch_json(source.source_url, session=session)
        return parse_tpex_mainboard_quotes(payload, source)
    if source.format == "krx_listed_companies_json":
        return fetch_krx_listed_companies(source, session=session)
    if source.format == "krx_etf_finder_json":
        return fetch_krx_etf_finder(source, session=session)
    if source.format == "sec_company_tickers_exchange_json":
        payload = fetch_json(source.source_url, session=session)
        return parse_sec_company_tickers_exchange(payload, source)
    raise ValueError(f"Unsupported source format: {source.format}")


def fetch_source_rows_with_mode(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]], str]:
    if source.format == "lse_company_reports_html":
        rows, mode = load_lse_company_reports_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("LSE company reports unavailable")
        return rows, mode
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
    if source.format == "tmx_listed_issuers_excel":
        content, mode = load_tmx_listed_issuers_content(session=session)
        if content is None:
            raise requests.RequestException("TMX listed issuers workbook unavailable")
        return parse_tmx_listed_issuers_excel(content, source), mode
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

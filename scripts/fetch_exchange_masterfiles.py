from __future__ import annotations

import csv
import io
import json
import os
import re
import zipfile
from base64 import b64encode
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from html import unescape
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
import requests

try:
    from scripts.rebuild_dataset import normalize_tokens
except ModuleNotFoundError:  # pragma: no cover - script execution path
    from rebuild_dataset import normalize_tokens


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
MASTERFILES_DIR = DATA_DIR / "masterfiles"
MASTERFILE_REFERENCE_CSV = MASTERFILES_DIR / "reference.csv"
MASTERFILE_SOURCES_JSON = MASTERFILES_DIR / "sources.json"
MASTERFILE_SUMMARY_JSON = MASTERFILES_DIR / "summary.json"
MASTERFILE_CACHE_DIR = MASTERFILES_DIR / "cache"
LISTINGS_CSV = DATA_DIR / "listings.csv"
STOCK_VERIFICATION_DIR = DATA_DIR / "stock_verification"
ETF_VERIFICATION_DIR = DATA_DIR / "etf_verification"
SEC_COMPANY_TICKERS_CACHE = MASTERFILE_CACHE_DIR / "sec_company_tickers_exchange.json"
LEGACY_SEC_COMPANY_TICKERS_CACHE = MASTERFILES_DIR / "sec_company_tickers_exchange.json"
LSE_COMPANY_REPORTS_CACHE = MASTERFILE_CACHE_DIR / "lse_company_reports.json"
LEGACY_LSE_COMPANY_REPORTS_CACHE = MASTERFILES_DIR / "lse_company_reports.json"
LSE_INSTRUMENT_DIRECTORY_CACHE = MASTERFILE_CACHE_DIR / "lse_instrument_directory.json"
LEGACY_LSE_INSTRUMENT_DIRECTORY_CACHE = MASTERFILES_DIR / "lse_instrument_directory.json"
LSE_INSTRUMENT_SEARCH_CACHE = MASTERFILE_CACHE_DIR / "lse_instrument_search.json"
LEGACY_LSE_INSTRUMENT_SEARCH_CACHE = MASTERFILES_DIR / "lse_instrument_search.json"
TMX_LISTED_ISSUERS_CACHE = MASTERFILE_CACHE_DIR / "tmx_listed_issuers.xlsx"
LEGACY_TMX_LISTED_ISSUERS_CACHE = MASTERFILES_DIR / "tmx_listed_issuers.xlsx"
TMX_ETF_SCREENER_CACHE = MASTERFILE_CACHE_DIR / "tmx_etf_screener.json"
LEGACY_TMX_ETF_SCREENER_CACHE = MASTERFILES_DIR / "tmx_etf_screener.json"
TPEX_MAINBOARD_QUOTES_CACHE = MASTERFILE_CACHE_DIR / "tpex_mainboard_daily_close_quotes.json"
LEGACY_TPEX_MAINBOARD_QUOTES_CACHE = MASTERFILES_DIR / "tpex_mainboard_daily_close_quotes.json"
NASDAQ_NORDIC_STOCKHOLM_SHARES_CACHE = MASTERFILE_CACHE_DIR / "nasdaq_nordic_stockholm_shares.json"
LEGACY_NASDAQ_NORDIC_STOCKHOLM_SHARES_CACHE = MASTERFILES_DIR / "nasdaq_nordic_stockholm_shares.json"
B3_INSTRUMENTS_EQUITIES_CACHE = MASTERFILE_CACHE_DIR / "b3_instruments_equities.json"
LEGACY_B3_INSTRUMENTS_EQUITIES_CACHE = MASTERFILES_DIR / "b3_instruments_equities.json"
JSE_ETF_LIST_CACHE = MASTERFILE_CACHE_DIR / "jse_etf_list.json"
LEGACY_JSE_ETF_LIST_CACHE = MASTERFILES_DIR / "jse_etf_list.json"
JSE_ETN_LIST_CACHE = MASTERFILE_CACHE_DIR / "jse_etn_list.json"
LEGACY_JSE_ETN_LIST_CACHE = MASTERFILES_DIR / "jse_etn_list.json"

SEC_COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers_exchange.json"
NASDAQ_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt"
NASDAQ_OTHER_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/symdir/otherlisted.txt"
LSE_COMPANY_REPORTS_URL = (
    "https://www.londonstockexchange.com/exchange/instrument-result.html"
    "?filterBy=CompanyReports&filterClause=1&initial={initial}&page={page}"
)
LSE_INSTRUMENT_SEARCH_URL = (
    "https://www.londonstockexchange.com/exchange/instrument-result.html"
    "?codeName={ticker}&filterBy=&filterClause="
)
LSE_INSTRUMENT_DIRECTORY_URL = (
    "https://www.londonstockexchange.com/exchange/instrument-result.html"
    "?codeName=&search=search&page={page}"
)
CBOE_CANADA_LISTING_DIRECTORY_URL = "https://www.cboe.com/ca/equities/market-activity/listing-directory/"
ASX_LISTED_URL = "https://www.asx.com.au/asx/research/ASXListedCompanies.csv"
ASX_FUNDS_STATISTICS_URL = "https://www.asx.com.au/issuers/investment-products/asx-funds-statistics"
TMX_INTERLISTED_URL = "https://www.tsx.com/files/trading/interlisted-companies.txt"
TMX_LISTED_ISSUERS_ARCHIVE_URL = "https://www.tsx.com/en/listings/current-market-statistics/mig-archives"
TMX_ETF_SCREENER_URL = "https://dgr53wu9i7rmp.cloudfront.net/etfs/etfs.json"
TMX_MONEY_GRAPHQL_URL = "https://app-money.tmx.com/graphql"
EURONEXT_EQUITIES_DOWNLOAD_URL = "https://live.euronext.com/pd_es/data/stocks/download?mics=dm_all_stock"
EURONEXT_ETFS_DOWNLOAD_URL = (
    "https://live.euronext.com/en/product_directory/data/etf-all-markets/download"
    "?mics=ALXA,ALXB,ALXL,ALXP,ATFX,BGEM,ENXB,ENXL,ETFP,ETLX,EXGM,MERK,MIVX,MLXB,"
    "MOTX,MTAA,MTAH,MTCH,SEDX,TNLA,TNLB,VPXB,WOMF,XACD,XAMC,XAMS,XATL,XBRU,XDUB,"
    "XESM,XLDN,XLIS,XMLI,XMOT,XMSM,XOAM,XOAS,XOBD,XOSL,XPAR,XPMC"
)
JPX_LISTED_ISSUES_URL = "https://www.jpx.co.jp/english/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_e.xls"
DEUTSCHE_BOERSE_LISTED_URL = "https://www.cashmarket.deutsche-boerse.com/resource/blob/67858/dd766fc6588100c79294324175f95501/data/Listed-companies.xlsx"
DEUTSCHE_BOERSE_ETPS_URL = "https://www.cashmarket.deutsche-boerse.com/resource/blob/1553442/2936716b8f6c2d7a0bb85337485bdcdb/data/Master_DataSheet_Download.xls"
DEUTSCHE_BOERSE_XETRA_ALL_TRADABLE_URL = "https://www.cashmarket.deutsche-boerse.com/resource/blob/1528/b52ea43a2edac92e8283d40645d1c076/data/t7-xetr-allTradableInstruments.csv"
SIX_EQUITY_ISSUERS_URL = "https://www.six-group.com/sheldon/equity_issuers/v1/equity_issuers.json"
SIX_ETF_EXPLORER_URL = "https://www.six-group.com/en/market-data/etf/etf-explorer.html"
SIX_ETP_EXPLORER_URL = "https://www.six-group.com/en/market-data/etp/etp-explorer.html"
B3_INSTRUMENTS_EQUITIES_URL = "https://arquivos.b3.com.br/bdi/table/InstrumentsEquities"
B3_FUNDS_LISTED_PROXY_URL = "https://sistemaswebb3-listados.b3.com.br/fundsListedProxy/Search/"
B3_BDR_COMPANIES_PROXY_URL = "https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/"
JSE_EXCHANGE_TRADED_PRODUCTS_URL = "https://www.jse.co.za/trade/equities-market/exchange-traded-products"
NASDAQ_NORDIC_API_ROOT_URL = "https://api.nasdaq.com/api/nordic/"
NASDAQ_NORDIC_SHARES_SCREENER_URL = "https://api.nasdaq.com/api/nordic/screener/shares"
TWSE_LISTED_COMPANIES_URL = "https://openapi.twse.com.tw/v1/opendata/t187ap03_L"
TWSE_ETF_LIST_URL = "https://www.twse.com.tw/rwd/en/ETF/list"
SET_LISTED_COMPANIES_URL = "https://www.set.or.th/dat/eod/listedcompany/static/listedCompanies_en_US.xls"
SZSE_STOCK_LIST_URL = "https://www.szse.cn/market/product/stock/list/index.html"
SZSE_ETF_LIST_URL = "https://www.szse.cn/market/product/list/etfList/index.html"
SZSE_REPORT_DATA_URL = "https://www.szse.cn/api/report/ShowReport/data"
SZSE_A_SHARE_CATALOG_ID = "1110"
SZSE_A_SHARE_TAB_KEY = "tab1"
SZSE_ETF_CATALOG_ID = "1945"
SZSE_ETF_TAB_KEY = "tab1"
SSE_STOCK_LIST_URL = "https://www.sse.com.cn/assortment/stock/list/share/"
SSE_ETF_LIST_URL = "https://www.sse.com.cn/assortment/fund/etf/list/"
SSE_COMMON_QUERY_URL = "https://query.sse.com.cn/sseQuery/commonQuery.do"
SSE_JSONP_CALLBACK = "jsonpCallback"
TPEX_MAINBOARD_QUOTES_URL = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes"
KRX_LISTED_COMPANIES_URL = "https://global.krx.co.kr/contents/GLB/03/0308/0308010000/GLB0308010000.jsp"
KRX_DATA_URL = "https://global.krx.co.kr/contents/GLB/99/GLB99000001.jspx"
KRX_GENERATE_OTP_URL = "https://global.krx.co.kr/contents/COM/GenerateOTP.jspx"
KRX_JSON_DATA_URL = "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
PSX_LISTED_COMPANIES_URL = "https://www.psx.com.pk/psx/resources-and-tools/listings/listed-companies"
PSX_COMPANIES_BY_SECTOR_URL = "https://www.psx.com.pk/psx/custom-templates/companiesSearch-sector"
PSX_DAILY_DOWNLOADS_URL = "https://dps.psx.com.pk/daily-downloads"

USER_AGENT = "free-ticker-database/2.0 (+https://github.com/adanos-software/free-ticker-database)"
SEC_CONTACT_EMAIL = os.environ.get("SEC_CONTACT_EMAIL", "opensource@adanos.software")
REQUEST_TIMEOUT = 30.0
LSE_UPDATE_OPENER_RE = re.compile(r"UpdateOpener\('(?P<name>(?:\\'|[^'])*)',\s*'(?P<meta>[^']*)'\)")
CBOE_CANADA_LISTING_DIRECTORY_RE = re.compile(r"CTX\['listingDirectory'\]\s*=\s*(\[[\s\S]*?\]);")
LSE_INSTRUMENT_SEARCH_MAX_WORKERS = 8

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

SIX_FUND_DOWNLOAD_FIELDS = (
    "FundLongName",
    "ValorSymbol",
    "FundReutersTicker",
    "FundBloombergTicker",
    "ValorNumber",
    "ISIN",
    "IssuerLongNameDesc",
    "IssuerNameFull",
    "TradingBaseCurrency",
    "FundCurrency",
    "ManagementFee",
    "ReplicationMethodDesc",
    "ManagementStyleDesc",
    "MarketMakers",
    "ClosingPrice",
    "ClosingPerformance",
    "ClosingDelta",
    "FundUnderlyingDescription",
    "BidVolume",
    "BidPrice",
    "AskVolume",
    "AskPrice",
    "MidSpread",
    "PreviousClosingPrice",
    "MarketDate",
    "MarketTime",
    "OpeningPrice",
    "DailyLowPrice",
    "OnMarketVolume",
    "OffBookVolume",
    "TotalTurnover",
    "TotalTurnoverCHF",
    "ProductLineDesc",
    "AssetClassDesc",
    "UnderlyingGeographicalDesc",
    "LegalStructureCountryDesc",
    "UnderlyingProviderDesc",
)

_SIX_FUND_DOWNLOAD_SELECT = ",".join(SIX_FUND_DOWNLOAD_FIELDS)
SIX_ETF_PRODUCTS_URL = (
    "https://www.six-group.com/fqs/ref.csv"
    f"?select={_SIX_FUND_DOWNLOAD_SELECT}"
    "&where=ProductLine=ET*PortalSegment=FU"
    "&orderby=FundLongName"
    "&page=1"
    "&pagesize=99999"
)
SIX_ETP_PRODUCTS_URL = (
    "https://www.six-group.com/fqs/ref.csv"
    f"?select={_SIX_FUND_DOWNLOAD_SELECT}"
    "&where=ProductLine=EP*PortalSegment=EP"
    "&orderby=FundLongName"
    "&page=1"
    "&pagesize=99999"
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
B3_ETF_FUND_TYPES = ("ETF", "ETF-RF", "ETF-FII", "ETF-CRIPTO")
B3_FUNDS_PAGE_SIZE = 120
TPEX_CANONICAL_TICKER_RE = re.compile(r"(?:\d{4}|00\d{4}[A-Z]?)$")
LSE_PAGE_INITIALS = tuple("ABCDEFGHIJKLMNOPQRSTUVWXYZ") + ("0",)
TMX_LISTED_ISSUERS_HREF_RE = re.compile(
    r'href="([^"]+tsx-tsxv-listed-issuers-(\d{4})-(\d{2})-en\.xlsx)"',
    re.I,
)
JSE_ETF_LIST_HREF_RE = re.compile(
    r'https://www\.jse\.co\.za/sites/default/files/media/documents/[^"\']*/ETF%20List[^"\']+\.xlsx',
    re.I,
)
JSE_ETN_LIST_HREF_RE = re.compile(
    r'https://www\.jse\.co\.za/sites/default/files/media/documents/[^"\']*/ETN%20List[^"\']+\.xlsx',
    re.I,
)
LSE_PAGE_NUMBER_RE = re.compile(r'title="Page (\d+)" class="page-number(?: active)?"')
PSX_XID_RE = re.compile(r'<input[^>]*\bid=["\']XID["\'][^>]*\bvalue=["\']([^"\']+)["\']', re.I)
PSX_SECTOR_SELECT_RE = re.compile(
    r'<select[^>]*\bname=["\']sector["\'][^>]*>(?P<body>.*?)</select>',
    re.I | re.S,
)
PSX_OPTION_RE = re.compile(
    r'<option[^>]*\bvalue=["\']([^"\']*)["\'][^>]*>(.*?)</option>',
    re.I | re.S,
)
PSX_SYMBOL_NAME_DOWNLOAD_RE = re.compile(r'href=["\']([^"\']*/download/symbol_name/[^"\']+\.zip)["\']', re.I)
ASX_INVESTMENT_PRODUCTS_LINK_RE = re.compile(
    r'(?P<path>/content/dam/asx/issuers/asx-investment-products-reports/(?P<year>\d{4})/excel/asx-investment-products-[^"\']+\.xlsx)',
    re.I,
)
KRX_MARKET_GUBUN_TO_EXCHANGE = {
    "1": "KRX",
    "2": "KOSDAQ",
}
SSE_JSONP_RE = re.compile(r"^[^(]+\((.*)\)\s*$", re.S)
SSE_STOCK_TYPES = ("1", "2", "8")
SSE_ETF_SUBCLASSES = ("01", "02", "03", "06", "08", "09", "31", "32", "33", "37")
PSX_SECTOR_LABEL_SKIP_MARKERS = (
    "select sector",
    "bond",
    "close-end mutual fund",
    "future contract",
)
PSX_ETF_SECTOR_LABEL_MARKERS = (
    "exchange traded fund",
    "exchange-traded fund",
)
ASX_MONTH_MAP = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}

ASX_ETP_TYPES = {"ETF", "SP", "ETMF", "ETN", "ETP", "ACTIVE", "COMPLEX"}


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
        key="lse_instrument_search",
        provider="LSE",
        description="Official London Stock Exchange exact instrument search lookup for ETF/ETP products",
        source_url=LSE_INSTRUMENT_SEARCH_URL,
        format="lse_instrument_search_html",
        reference_scope="security_lookup_subset",
    ),
    MasterfileSource(
        key="lse_instrument_directory",
        provider="LSE",
        description="Official London Stock Exchange paginated instrument directory",
        source_url=LSE_INSTRUMENT_DIRECTORY_URL,
        format="lse_instrument_directory_html",
        reference_scope="security_lookup_subset",
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
        key="cboe_canada_listing_directory",
        provider="Cboe Canada",
        description="Official Cboe Canada listing directory",
        source_url=CBOE_CANADA_LISTING_DIRECTORY_URL,
        format="cboe_canada_listing_directory_html",
    ),
    MasterfileSource(
        key="asx_investment_products",
        provider="ASX",
        description="Official ASX investment products monthly workbook",
        source_url=ASX_FUNDS_STATISTICS_URL,
        format="asx_investment_products_excel",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="set_listed_companies",
        provider="SET",
        description="Official Stock Exchange of Thailand listed companies table",
        source_url=SET_LISTED_COMPANIES_URL,
        format="set_listed_companies_html",
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
        key="tmx_etf_screener",
        provider="TMX",
        description="Official TMX Money ETF screener dataset",
        source_url=TMX_ETF_SCREENER_URL,
        format="tmx_etf_screener_json",
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
        key="euronext_etfs",
        provider="Euronext",
        description="Official Euronext ETF and ETP product directory export",
        source_url=EURONEXT_ETFS_DOWNLOAD_URL,
        format="euronext_etfs_semicolon_csv",
        reference_scope="listed_companies_subset",
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
        key="six_equity_issuers",
        provider="SIX",
        description="Official SIX Swiss Exchange equity issuers directory",
        source_url=SIX_EQUITY_ISSUERS_URL,
        format="six_equity_issuers_json",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="six_etf_products",
        provider="SIX",
        description="Official SIX Swiss Exchange ETF explorer export",
        source_url=SIX_ETF_PRODUCTS_URL,
        format="six_fund_products_csv",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="six_etp_products",
        provider="SIX",
        description="Official SIX Swiss Exchange ETP explorer export",
        source_url=SIX_ETP_PRODUCTS_URL,
        format="six_fund_products_csv",
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
        key="b3_listed_etfs",
        provider="B3",
        description="Official B3 listed ETF directories (equity, fixed-income, FII, crypto)",
        source_url=B3_FUNDS_LISTED_PROXY_URL,
        format="b3_listed_funds_api",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="b3_bdr_etfs",
        provider="B3",
        description="Official B3 BDR ETF and ETP directory",
        source_url=B3_BDR_COMPANIES_PROXY_URL,
        format="b3_bdr_companies_api",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="jse_etf_list",
        provider="JSE",
        description="Official JSE ETF product list",
        source_url=JSE_EXCHANGE_TRADED_PRODUCTS_URL,
        format="jse_etf_list_xlsx",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="jse_etn_list",
        provider="JSE",
        description="Official JSE ETN product list",
        source_url=JSE_EXCHANGE_TRADED_PRODUCTS_URL,
        format="jse_etn_list_xlsx",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="nasdaq_nordic_stockholm_shares",
        provider="Nasdaq Nordic",
        description="Official Nasdaq Nordic Stockholm Main Market shares screener",
        source_url=NASDAQ_NORDIC_SHARES_SCREENER_URL,
        format="nasdaq_nordic_stockholm_shares_json",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="twse_listed_companies",
        provider="TWSE",
        description="Official TWSE listed companies open data feed",
        source_url=TWSE_LISTED_COMPANIES_URL,
        format="twse_listed_companies_json",
    ),
    MasterfileSource(
        key="twse_etf_list",
        provider="TWSE",
        description="Official TWSE ETF product directory",
        source_url=TWSE_ETF_LIST_URL,
        format="twse_etf_list_json",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="sse_a_share_list",
        provider="SSE",
        description="Official SSE stock list (A/B/STAR boards)",
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
        key="szse_etf_list",
        provider="SZSE",
        description="Official SZSE ETF list",
        source_url=SZSE_ETF_LIST_URL,
        format="szse_etf_list_json",
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
        key="psx_listed_companies",
        provider="PSX",
        description="Official Pakistan Stock Exchange listed companies sector directory",
        source_url=PSX_LISTED_COMPANIES_URL,
        format="psx_listed_companies_json",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="psx_symbol_name_daily",
        provider="PSX",
        description="Official PSX daily symbol-name download",
        source_url=PSX_DAILY_DOWNLOADS_URL,
        format="psx_symbol_name_daily_zip",
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


def load_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


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


def infer_set_asset_type(name: str) -> str:
    lowered = f" {name.lower()} "
    if (
        " real estate investment trust" in lowered
        or " infrastructure fund" in lowered
        or " property fund" in lowered
    ):
        return "Stock"
    return infer_asset_type(name)


def latest_verification_run(base_dir: Path) -> Path | None:
    candidates = [path for path in base_dir.iterdir() if path.is_dir()] if base_dir.exists() else []
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def latest_reference_gap_tickers(
    base_dir: Path,
    *,
    exchanges: set[str] | None = None,
) -> set[str]:
    latest_run = latest_verification_run(base_dir)
    if latest_run is None:
        return set()
    tickers: set[str] = set()
    for path in sorted(latest_run.glob("chunk-*-of-*.csv")):
        with path.open(newline="", encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                if row.get("status") != "reference_gap":
                    continue
                exchange = row.get("exchange", "")
                ticker = row.get("ticker", "").strip()
                if exchanges and exchange not in exchanges:
                    continue
                if ticker:
                    tickers.add(ticker)
    return tickers


def lse_reference_gap_tickers(base_dirs: Iterable[Path] | None = None) -> set[str]:
    base_dirs = tuple(base_dirs or (STOCK_VERIFICATION_DIR, ETF_VERIFICATION_DIR))
    tickers: set[str] = set()
    for base_dir in base_dirs:
        latest_run = latest_verification_run(base_dir)
        if latest_run is None:
            continue
        for path in sorted(latest_run.glob("chunk-*-of-*.csv")):
            with path.open(newline="", encoding="utf-8") as handle:
                for row in csv.DictReader(handle):
                    ticker = row.get("ticker", "").strip()
                    if row.get("exchange") == "LSE" and row.get("status") == "reference_gap" and ticker:
                        tickers.add(ticker)
    return tickers


def compact_company_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def tmx_lookup_name_matches(listing_name: str, candidate_name: str) -> bool:
    listing_compact = compact_company_name(listing_name)
    candidate_compact = compact_company_name(candidate_name)
    if listing_compact and candidate_compact and (
        listing_compact == candidate_compact
        or listing_compact in candidate_compact
        or candidate_compact in listing_compact
    ):
        return True
    return normalize_tokens(listing_name) == normalize_tokens(candidate_name)


def tmx_lookup_symbol_variants(ticker: str) -> list[str]:
    normalized = ticker.strip().upper()
    if not normalized:
        return []
    variants = [normalized]
    dotted = normalized.replace("-", ".")
    if dotted not in variants:
        variants.append(dotted)
    return variants


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


def psx_request_headers(*, ajax: bool = False) -> dict[str, str]:
    headers = {
        "User-Agent": USER_AGENT,
        "Referer": PSX_LISTED_COMPANIES_URL,
        "Origin": "https://www.psx.com.pk",
        "Connection": "close",
    }
    if ajax:
        headers["Accept"] = "application/json,text/plain,*/*"
        headers["X-Requested-With"] = "XMLHttpRequest"
    return headers


def nasdaq_nordic_request_headers() -> dict[str, str]:
    return {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json,text/plain,*/*",
        "Accept-Encoding": "gzip, deflate",
        "Referer": "https://www.nasdaqomxnordic.com/shares/listed-companies/stockholm",
        "Origin": "https://www.nasdaqomxnordic.com",
    }


def extract_jse_exchange_traded_product_download_url(page_html: str, product_type: str) -> str | None:
    product_type = product_type.strip().upper()
    if product_type == "ETF":
        matches = JSE_ETF_LIST_HREF_RE.findall(page_html)
    elif product_type == "ETN":
        matches = JSE_ETN_LIST_HREF_RE.findall(page_html)
    else:
        raise ValueError(f"Unsupported JSE product type: {product_type}")
    return matches[-1] if matches else None


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


def load_tmx_etf_screener_payload(
    session: requests.Session | None = None,
) -> tuple[list[dict[str, Any]] | None, str]:
    cached_payload: list[dict[str, Any]] | None = None
    for path in (TMX_ETF_SCREENER_CACHE, LEGACY_TMX_ETF_SCREENER_CACHE):
        if path.exists():
            cached_payload = json.loads(path.read_text(encoding="utf-8"))
            break

    mode = "cache"
    payload = cached_payload
    if payload is None:
        try:
            payload = fetch_json(TMX_ETF_SCREENER_URL, session=session)
        except requests.RequestException:
            return None, "unavailable"
        mode = "network"

    payload = list(payload)
    covered_tickers = {str(record.get("symbol", "")).strip().upper() for record in payload}
    supplemental_rows = fetch_tmx_etf_screener_quote_rows(
        payload,
        listings_path=LISTINGS_CSV,
        session=session,
    )
    if supplemental_rows:
        for row in supplemental_rows:
            if row["symbol"].upper() in covered_tickers:
                continue
            payload.append(row)
            covered_tickers.add(row["symbol"].upper())
        mode = "network"

    ensure_output_dirs()
    TMX_ETF_SCREENER_CACHE.write_text(json.dumps(payload), encoding="utf-8")
    return payload, mode


def fetch_tmx_money_quote(
    symbol: str,
    session: requests.Session | None = None,
) -> dict[str, Any] | None:
    session = session or requests.Session()
    response = session.post(
        TMX_MONEY_GRAPHQL_URL,
        headers={"User-Agent": USER_AGENT, "Content-Type": "application/json"},
        json={
            "query": (
                "query ($symbol: String, $locale: String) { "
                "getQuoteBySymbol(symbol: $symbol, locale: $locale) { "
                "symbol name exchangeCode datatype issueType "
                "} }"
            ),
            "variables": {"symbol": symbol, "locale": "en"},
        },
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    payload = response.json()
    return payload.get("data", {}).get("getQuoteBySymbol")


def tmx_etf_quote_lookup_target_rows(
    *,
    listings_path: Path = LISTINGS_CSV,
    verification_dir: Path = ETF_VERIFICATION_DIR,
) -> list[dict[str, str]]:
    target_tickers = latest_reference_gap_tickers(
        verification_dir,
        exchanges={"TSX", "TSXV"},
    )
    if not target_tickers:
        return []
    return [
        row
        for row in load_csv(listings_path)
        if row.get("exchange") in {"TSX", "TSXV"}
        and row.get("asset_type") == "ETF"
        and row.get("ticker", "").strip() in target_tickers
    ]


def fetch_tmx_etf_screener_quote_rows(
    payload: list[dict[str, Any]],
    *,
    listings_path: Path = LISTINGS_CSV,
    verification_dir: Path = ETF_VERIFICATION_DIR,
    session: requests.Session | None = None,
) -> list[dict[str, Any]]:
    covered_tickers = {str(record.get("symbol", "")).strip().upper() for record in payload}
    target_rows = [
        row
        for row in tmx_etf_quote_lookup_target_rows(
            listings_path=listings_path,
            verification_dir=verification_dir,
        )
        if row.get("ticker", "").strip().upper() not in covered_tickers
    ]
    session = session or requests.Session()
    rows: list[dict[str, Any]] = []
    for listing_row in target_rows:
        listing_ticker = listing_row.get("ticker", "").strip().upper()
        listing_name = listing_row.get("name", "").strip()
        exchange = listing_row.get("exchange", "").strip()
        if not listing_ticker or not listing_name or not exchange:
            continue
        for symbol in tmx_lookup_symbol_variants(listing_ticker):
            try:
                candidate = fetch_tmx_money_quote(symbol, session=session)
            except (requests.RequestException, ValueError, json.JSONDecodeError):
                candidate = None
            if candidate is None:
                continue
            if candidate.get("exchangeCode") != exchange:
                continue
            if str(candidate.get("symbol", "")).strip().upper() != symbol:
                continue
            candidate_name = str(candidate.get("name", "")).strip()
            if not candidate_name or not tmx_lookup_name_matches(listing_name, candidate_name):
                continue
            rows.append(
                {
                    "symbol": listing_ticker,
                    "longname": candidate_name,
                    "exchange": exchange,
                    "source_url": TMX_MONEY_GRAPHQL_URL,
                }
            )
            covered_tickers.add(listing_ticker)
            break
    return rows


def load_jse_exchange_traded_product_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    cache_paths = {
        "jse_etf_list": (JSE_ETF_LIST_CACHE, LEGACY_JSE_ETF_LIST_CACHE),
        "jse_etn_list": (JSE_ETN_LIST_CACHE, LEGACY_JSE_ETN_LIST_CACHE),
    }.get(source.key)
    if cache_paths is None:
        raise ValueError(f"Unsupported JSE source key: {source.key}")

    for path in cache_paths:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"

    try:
        rows = fetch_jse_exchange_traded_product_rows(source, session=session)
    except (requests.RequestException, ValueError):
        return None, "unavailable"

    ensure_output_dirs()
    cache_paths[0].write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def load_nasdaq_nordic_stockholm_shares_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    for path in (
        NASDAQ_NORDIC_STOCKHOLM_SHARES_CACHE,
        LEGACY_NASDAQ_NORDIC_STOCKHOLM_SHARES_CACHE,
    ):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"

    try:
        rows = fetch_nasdaq_nordic_stockholm_shares(source, session=session)
    except requests.RequestException:
        return None, "unavailable"

    ensure_output_dirs()
    NASDAQ_NORDIC_STOCKHOLM_SHARES_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def load_b3_instruments_equities_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    try:
        rows = fetch_b3_instruments_equities(source, session=session)
    except requests.RequestException:
        for path in (B3_INSTRUMENTS_EQUITIES_CACHE, LEGACY_B3_INSTRUMENTS_EQUITIES_CACHE):
            if path.exists():
                return json.loads(path.read_text(encoding="utf-8")), "cache"
        return None, "unavailable"

    ensure_output_dirs()
    B3_INSTRUMENTS_EQUITIES_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


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


def extract_lse_last_page(text: str) -> int:
    pages = [int(page) for page in LSE_PAGE_NUMBER_RE.findall(text)]
    return max(pages) if pages else 1


def fetch_lse_instrument_directory(
    source: MasterfileSource,
    *,
    target_tickers: set[str] | None = None,
    asset_type_by_ticker: dict[str, str] | None = None,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    session = session or requests.Session()
    target_tickers = target_tickers or set()
    asset_type_by_ticker = asset_type_by_ticker or {}
    rows: list[dict[str, str]] = []
    seen_signatures: set[tuple[str, str]] = set()

    first_text = fetch_text(source.source_url.format(page=1), session=session)
    last_page = extract_lse_last_page(first_text)
    page_texts = [(1, first_text)] + [
        (page, fetch_text(source.source_url.format(page=page), session=session))
        for page in range(2, last_page + 1)
    ]

    for page, text in page_texts:
        for row in parse_lse_company_reports_html(text, source):
            ticker = row.get("ticker", "").strip()
            if not ticker or (target_tickers and ticker not in target_tickers):
                continue
            signature = (ticker, row.get("name", ""))
            if signature in seen_signatures:
                continue
            seen_signatures.add(signature)
            row["source_url"] = source.source_url.format(page=page)
            row["asset_type"] = asset_type_by_ticker.get(ticker, row.get("asset_type", ""))
            rows.append(row)
    return rows


def load_lse_instrument_directory_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    for path in (LSE_INSTRUMENT_DIRECTORY_CACHE, LEGACY_LSE_INSTRUMENT_DIRECTORY_CACHE):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"

    listings_rows = load_csv(LISTINGS_CSV)
    target_tickers = {
        row.get("ticker", "").strip()
        for row in listings_rows
        if row.get("exchange") == "LSE"
        and row.get("asset_type") in {"Stock", "ETF"}
        and row.get("ticker", "").strip()
    }
    asset_type_by_ticker = {
        row.get("ticker", "").strip(): row.get("asset_type", "")
        for row in listings_rows
        if row.get("exchange") == "LSE" and row.get("ticker", "").strip()
    }

    try:
        rows = fetch_lse_instrument_directory(
            source,
            target_tickers=target_tickers,
            asset_type_by_ticker=asset_type_by_ticker,
            session=session,
        )
    except requests.RequestException:
        return None, "unavailable"

    ensure_output_dirs()
    LSE_INSTRUMENT_DIRECTORY_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def group_rows_by_ticker(rows: list[dict[str, str]]) -> dict[str, list[dict[str, str]]]:
    grouped: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        ticker = row.get("ticker", "").strip()
        if not ticker:
            continue
        grouped.setdefault(ticker, []).append(row)
    return grouped


def infer_lse_lookup_asset_type(instrument_code: str, name: str, fallback_asset_type: str = "") -> str:
    normalized = instrument_code.strip().upper()
    if normalized.startswith(("ETF", "ETC", "ETN", "ETP", "ETS", "ECE", "EUE")):
        return "ETF"
    if normalized.startswith(("EQS", "SS", "ST")):
        return "Stock"
    if fallback_asset_type:
        return fallback_asset_type
    return infer_asset_type(name)


def normalize_lse_lookup_ticker(ticker: str) -> str:
    return ticker.strip().upper().rstrip(".")


def extract_lse_instrument_search_metadata(text: str) -> dict[str, dict[str, str]]:
    metadata_by_ticker: dict[str, dict[str, str]] = {}
    for match in LSE_UPDATE_OPENER_RE.finditer(text):
        meta = " ".join(match.group("meta").split())
        parts = [part.strip() for part in meta.split("|")]
        if len(parts) < 6:
            continue
        isin, country_code, currency, instrument_code, figi, ticker = parts[:6]
        if not ticker:
            continue
        metadata_by_ticker[ticker] = {
            "isin": isin,
            "country_code": country_code,
            "currency": currency,
            "instrument_code": instrument_code,
            "figi": figi,
        }
    return metadata_by_ticker


def lse_instrument_search_target_tickers(
    company_report_rows: list[dict[str, str]],
    *,
    listings_path: Path | None = None,
    reference_gap_tickers: set[str] | None = None,
) -> list[str]:
    listings_path = listings_path or LISTINGS_CSV
    target_tickers = {
        row.get("ticker", "").strip()
        for row in load_csv(listings_path)
        if row.get("exchange") == "LSE"
        and row.get("asset_type") in {"Stock", "ETF"}
        and row.get("ticker", "").strip()
    }
    reference_gap_tickers = reference_gap_tickers if reference_gap_tickers is not None else lse_reference_gap_tickers()
    if reference_gap_tickers:
        target_tickers &= reference_gap_tickers
    return sorted(
        {
            ticker
            for ticker in target_tickers
            if ticker
        }
    )


def fetch_lse_instrument_search_exact(
    source: MasterfileSource,
    tickers: Iterable[str],
    session: requests.Session | None = None,
    asset_type_by_ticker: dict[str, str] | None = None,
) -> list[dict[str, str]]:
    session = session or requests.Session()
    asset_type_by_ticker = asset_type_by_ticker or {}
    rows: list[dict[str, str]] = []
    for ticker in tickers:
        query_ticker = ticker.strip()
        if not query_ticker:
            continue
        normalized_query_ticker = normalize_lse_lookup_ticker(query_ticker)
        lookup_url = source.source_url.format(ticker=requests.utils.quote(query_ticker, safe=""))
        text = fetch_text(lookup_url, session=session)
        metadata_by_ticker = extract_lse_instrument_search_metadata(text)
        normalized_metadata_by_ticker = {
            normalize_lse_lookup_ticker(candidate_ticker): metadata
            for candidate_ticker, metadata in metadata_by_ticker.items()
            if normalize_lse_lookup_ticker(candidate_ticker)
        }
        seen_signatures: set[tuple[str, str]] = set()
        for row in parse_lse_company_reports_html(text, source):
            row_ticker = row.get("ticker", "").strip()
            if normalize_lse_lookup_ticker(row_ticker) != normalized_query_ticker:
                continue
            signature = (query_ticker, row["name"])
            if signature in seen_signatures:
                continue
            seen_signatures.add(signature)
            row["ticker"] = query_ticker
            row["source_url"] = lookup_url
            metadata = (
                metadata_by_ticker.get(query_ticker)
                or metadata_by_ticker.get(row_ticker)
                or normalized_metadata_by_ticker.get(normalized_query_ticker)
            )
            if metadata:
                row["isin"] = metadata.get("isin", "")
                row["asset_type"] = infer_lse_lookup_asset_type(
                    metadata.get("instrument_code", ""),
                    row.get("name", ""),
                    asset_type_by_ticker.get(query_ticker, ""),
                )
            elif not row.get("asset_type"):
                row["asset_type"] = asset_type_by_ticker.get(query_ticker, "")
            rows.append(row)
    return rows


def load_lse_instrument_search_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    company_reports_source = next(item for item in OFFICIAL_SOURCES if item.key == "lse_company_reports")
    directory_source = next(item for item in OFFICIAL_SOURCES if item.key == "lse_instrument_directory")
    company_report_rows, _ = load_lse_company_reports_rows(company_reports_source, session=session)
    directory_rows, _ = load_lse_instrument_directory_rows(directory_source, session=session)
    listings_rows = load_csv(LISTINGS_CSV)
    asset_type_by_ticker = {
        row.get("ticker", "").strip(): row.get("asset_type", "")
        for row in listings_rows
        if row.get("exchange") == "LSE" and row.get("ticker", "").strip()
    }
    covered_rows = (company_report_rows or []) + (directory_rows or [])
    target_tickers = lse_instrument_search_target_tickers(covered_rows, listings_path=LISTINGS_CSV)

    cached_lookup: dict[str, list[dict[str, str]]] = {}
    for path in (LSE_INSTRUMENT_SEARCH_CACHE, LEGACY_LSE_INSTRUMENT_SEARCH_CACHE):
        if not path.exists():
            continue
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            cached_lookup = {str(key): value for key, value in payload.items()}
        elif isinstance(payload, list):
            cached_lookup = group_rows_by_ticker(payload)
        break

    missing_tickers = [
        ticker
        for ticker in target_tickers
        if ticker not in cached_lookup
        or not any(row.get("isin", "").strip() for row in cached_lookup.get(ticker, []))
    ]
    used_network = False
    if missing_tickers:
        fetched_lookup = {ticker: [] for ticker in missing_tickers}
        successful_fetch = False
        max_workers = min(LSE_INSTRUMENT_SEARCH_MAX_WORKERS, len(missing_tickers))
        if max_workers <= 1:
            for ticker in missing_tickers:
                try:
                    fetched_lookup[ticker] = fetch_lse_instrument_search_exact(
                        source,
                        [ticker],
                        session=session,
                        asset_type_by_ticker=asset_type_by_ticker,
                    )
                except requests.RequestException:
                    continue
                successful_fetch = True
        else:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {
                    executor.submit(
                        fetch_lse_instrument_search_exact,
                        source,
                        [ticker],
                        None,
                        asset_type_by_ticker,
                    ): ticker
                    for ticker in missing_tickers
                }
                for future in as_completed(futures):
                    ticker = futures[future]
                    try:
                        fetched_lookup[ticker] = future.result()
                    except requests.RequestException:
                        continue
                    successful_fetch = True
        if not cached_lookup and not successful_fetch:
            return None, "unavailable"
        cached_lookup.update(fetched_lookup)
        ensure_output_dirs()
        LSE_INSTRUMENT_SEARCH_CACHE.write_text(json.dumps(cached_lookup), encoding="utf-8")
        used_network = successful_fetch

    rows: list[dict[str, str]] = []
    for ticker in sorted(cached_lookup):
        rows.extend(cached_lookup.get(ticker, []))
    return rows, "network" if used_network else "cache"


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


def normalize_excel_header(value: Any) -> str:
    if pd.isna(value):
        return ""
    return " ".join(str(value).replace("\n", " ").split()).strip().lower()


def b64encode_json(value: Any, latin1_safe: bool = False) -> str:
    text = json.dumps(value, ensure_ascii=False)
    payload = text.encode("utf-8" if latin1_safe else "utf-8")
    return b64encode(payload).decode()


def asx_investment_products_sort_key(path: str, fallback_year: int) -> tuple[int, int]:
    suffix = Path(path).stem.lower().removeprefix("asx-investment-products-")
    compact_match = re.search(r"(\d{6})", suffix)
    if compact_match:
        compact_value = compact_match.group(1)
        return int(compact_value[:4]), int(compact_value[4:6])
    for token in re.split(r"[^a-z0-9]+", suffix):
        month = ASX_MONTH_MAP.get(token)
        if month is not None:
            return fallback_year, month
    return fallback_year, 0


def extract_latest_asx_investment_products_url(text: str) -> str:
    matches: list[tuple[tuple[int, int], str]] = []
    for match in ASX_INVESTMENT_PRODUCTS_LINK_RE.finditer(text):
        path = match.group("path")
        year = int(match.group("year"))
        matches.append((asx_investment_products_sort_key(path, year), path))
    if not matches:
        raise ValueError("No ASX investment products workbook links found")
    _, latest_path = max(matches, key=lambda item: item[0])
    return requests.compat.urljoin(ASX_FUNDS_STATISTICS_URL, latest_path)


def parse_asx_investment_products_excel(content: bytes, source: MasterfileSource) -> list[dict[str, str]]:
    dataframe = pd.read_excel(io.BytesIO(content), sheet_name="Spotlight ETP List", header=9)
    column_map = {normalize_excel_header(column): column for column in dataframe.columns}
    ticker_column = column_map.get("asx code")
    type_column = column_map.get("type")
    name_column = column_map.get("fund name")
    if not ticker_column or not type_column or not name_column:
        raise ValueError("ASX investment products workbook missing expected columns")

    rows: list[dict[str, str]] = []
    for record in dataframe.to_dict(orient="records"):
        ticker_value = record.get(ticker_column)
        type_value = record.get(type_column)
        name_value = record.get(name_column)
        if pd.isna(ticker_value) or pd.isna(type_value) or pd.isna(name_value):
            continue
        type_name = str(type_value).strip().upper()
        if type_name not in ASX_ETP_TYPES:
            continue
        ticker = str(ticker_value).strip().upper()
        name = str(name_value).strip()
        if not ticker or not name:
            continue
        rows.append(
            {
                "source_key": source.key,
                "provider": source.provider,
                "source_url": source.source_url,
                "ticker": ticker,
                "name": name,
                "exchange": "ASX",
                "asset_type": "ETF",
                "listing_status": "active",
                "reference_scope": source.reference_scope,
                "official": "true",
            }
        )
    return rows


def parse_lse_company_reports_html(text: str, source: MasterfileSource) -> list[dict[str, str]]:
    parser = _TableParser()
    parser.feed(text)
    target_table: list[list[str]] | None = None
    for table in parser.tables:
        if not table:
            continue
        header = [cell.strip() for cell in table[0]]
        if "Code" in header and "Name" in header:
            target_table = table
            break
    if not target_table:
        return []
    header = target_table[0]
    try:
        code_index = header.index("Code")
        name_index = header.index("Name")
    except ValueError:
        return []
    rows: list[dict[str, str]] = []
    for record in target_table[1:]:
        if len(record) <= max(code_index, name_index):
            continue
        ticker = str(record[code_index]).strip()
        name = str(record[name_index]).strip()
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


def parse_cboe_canada_listing_directory_html(text: str, source: MasterfileSource) -> list[dict[str, str]]:
    match = CBOE_CANADA_LISTING_DIRECTORY_RE.search(text)
    if not match:
        return []
    try:
        payload = json.loads(match.group(1))
    except json.JSONDecodeError:
        return []
    rows: list[dict[str, str]] = []
    for record in payload:
        ticker = str(record.get("symbol") or "").strip().upper()
        name = str(record.get("name") or "").strip()
        security = str(record.get("security") or "").strip().lower()
        if not ticker or not name:
            continue
        if security in {"equity", "dr"}:
            asset_type = "Stock"
        elif security in {"etf", "cef"}:
            asset_type = "ETF"
        else:
            continue
        rows.append(
            {
                "source_key": source.key,
                "provider": source.provider,
                "source_url": source.source_url,
                "ticker": ticker,
                "name": name,
                "exchange": "NEO",
                "asset_type": asset_type,
                "listing_status": "active",
                "reference_scope": source.reference_scope,
                "official": "true",
            }
        )
    return rows


class _TableParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.tables: list[list[list[str]]] = []
        self._current_table: list[list[str]] | None = None
        self._current_row: list[str] | None = None
        self._current_cell: list[str] | None = None
        self._in_cell = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "table":
            self._current_table = []
        elif tag == "tr" and self._current_table is not None:
            self._current_row = []
        elif tag in {"td", "th"} and self._current_row is not None:
            self._current_cell = []
            self._in_cell = True

    def handle_endtag(self, tag: str) -> None:
        if tag in {"td", "th"} and self._current_row is not None and self._current_cell is not None:
            text = " ".join("".join(self._current_cell).split())
            self._current_row.append(unescape(text))
            self._current_cell = None
            self._in_cell = False
        elif tag == "tr" and self._current_table is not None and self._current_row is not None:
            if self._current_row:
                self._current_table.append(self._current_row)
            self._current_row = None
        elif tag == "table" and self._current_table is not None:
            if self._current_table:
                self.tables.append(self._current_table)
            self._current_table = None

    def handle_data(self, data: str) -> None:
        if self._in_cell and self._current_cell is not None:
            self._current_cell.append(data)


def parse_set_listed_companies_html(text: str, source: MasterfileSource) -> list[dict[str, str]]:
    parser = _TableParser()
    parser.feed(text)
    rows: list[dict[str, str]] = []
    for table in parser.tables:
        header_row_index = None
        for index, record in enumerate(table):
            header = [cell.strip() for cell in record]
            if {"Symbol", "Company", "Market"} <= set(header):
                header_row_index = index
                break
        if header_row_index is None:
            continue
        header = table[header_row_index]
        try:
            symbol_index = header.index("Symbol")
            company_index = header.index("Company")
            market_index = header.index("Market")
        except ValueError:
            continue
        for record in table[header_row_index + 1 :]:
            if len(record) <= max(symbol_index, company_index, market_index):
                continue
            ticker = str(record[symbol_index]).strip()
            name = str(record[company_index]).strip()
            market = str(record[market_index]).strip()
            if market != "SET" or not ticker or not name or ticker.lower() == "nan" or name.lower() == "nan":
                continue
            rows.append(
                {
                    "source_key": source.key,
                    "provider": source.provider,
                    "source_url": source.source_url,
                    "ticker": ticker,
                    "name": name,
                    "exchange": "SET",
                    "asset_type": infer_set_asset_type(name),
                    "listing_status": "active",
                    "reference_scope": source.reference_scope,
                    "official": "true",
                }
            )
        break
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


def parse_jse_exchange_traded_product_excel(
    content: bytes,
    source: MasterfileSource,
    *,
    source_url: str,
) -> list[dict[str, str]]:
    dataframe = pd.read_excel(io.BytesIO(content), sheet_name=0)
    rows: list[dict[str, str]] = []
    for record in dataframe.to_dict(orient="records"):
        ticker = str(record.get("Alpha", "")).strip()
        name = re.sub(r"\s+", " ", str(record.get("Long Name", "")).strip())
        if not ticker or not name or ticker.lower() == "nan" or name.lower() == "nan":
            continue
        rows.append(
            {
                "source_key": source.key,
                "provider": source.provider,
                "source_url": source_url,
                "ticker": ticker,
                "name": name,
                "exchange": "JSE",
                "asset_type": "ETF",
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


def parse_tmx_etf_screener(payload: list[dict[str, Any]], source: MasterfileSource) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for record in payload:
        ticker = str(record.get("symbol", "")).strip()
        name = str(record.get("longname") or record.get("shortname") or "").strip()
        if not ticker or not name:
            continue
        rows.append(
            {
                "source_key": source.key,
                "provider": source.provider,
                "source_url": str(record.get("source_url") or source.source_url).strip(),
                "ticker": ticker,
                "name": name,
                "exchange": str(record.get("exchange") or "TSX").strip() or "TSX",
                "asset_type": "ETF",
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


def parse_euronext_etfs_download(text: str, source: MasterfileSource) -> list[dict[str, str]]:
    lines = [line for line in text.splitlines() if line.strip()]
    if len(lines) < 4:
        return []

    data_lines = [line.lstrip("\ufeff") for line in lines]
    reader = csv.DictReader(io.StringIO("\n".join([data_lines[0], *data_lines[4:]])), delimiter=";")
    rows: list[dict[str, str]] = []
    for row in reader:
        ticker = (row.get("Symbol") or "").strip()
        name = (row.get("Instrument Fullname") or row.get("Name") or "").strip()
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
                "asset_type": "ETF",
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


def parse_twse_etf_list(payload: dict[str, Any], source: MasterfileSource) -> list[dict[str, str]]:
    fields = payload.get("fields", [])
    rows: list[dict[str, str]] = []
    for values in payload.get("data", []):
        record = dict(zip(fields, values))
        ticker = str(record.get("Security Code", "")).strip()
        name = str(record.get("Name of ETF", "")).strip()
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
        stock_type = str(record.get("STOCK_TYPE", "")).strip()
        ticker = str(record.get("A_STOCK_CODE", "")).strip()
        b_ticker = str(record.get("B_STOCK_CODE", "")).strip()
        if stock_type == "2" and b_ticker and b_ticker != "-":
            ticker = b_ticker
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


def extract_szse_report_sections(payload: dict[str, Any] | list[dict[str, Any]]) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [section for section in payload if isinstance(section, dict)]
    if isinstance(payload, dict):
        return [
            report
            for nested in payload.values()
            if isinstance(nested, list)
            for report in nested
            if isinstance(report, dict)
        ]
    return []


def parse_szse_a_share_list(payload: dict[str, Any] | list[dict[str, Any]], source: MasterfileSource) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for report in extract_szse_report_sections(payload):
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


def parse_szse_etf_list(payload: dict[str, Any] | list[dict[str, Any]], source: MasterfileSource) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for report in extract_szse_report_sections(payload):
        for record in report.get("data", []) or []:
            ticker = strip_html_tags(str(record.get("sys_key", "")).strip())
            name = strip_html_tags(str(record.get("kzjcurl", "")).strip())
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
                    "asset_type": "ETF",
                    "listing_status": "active",
                    "reference_scope": source.reference_scope,
                    "official": "true",
                }
            )
    return rows


def parse_szse_etf_workbook(content: bytes, source: MasterfileSource) -> list[dict[str, str]]:
    dataframe = pd.read_excel(io.BytesIO(content), sheet_name=0)
    rows: list[dict[str, str]] = []
    for record in dataframe.to_dict(orient="records"):
        ticker_value = record.get("证券代码")
        name_value = record.get("证券简称")
        if pd.isna(ticker_value) or pd.isna(name_value):
            continue
        ticker = str(ticker_value).strip()
        if ticker.endswith(".0"):
            ticker = ticker[:-2]
        ticker = ticker.zfill(6)
        name = str(name_value).strip()
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
                "asset_type": "ETF",
                "listing_status": "active",
                "reference_scope": source.reference_scope,
                "official": "true",
            }
        )
    return rows


def parse_szse_a_share_workbook(content: bytes, source: MasterfileSource) -> list[dict[str, str]]:
    dataframe = pd.read_excel(io.BytesIO(content), sheet_name=0)
    rows: list[dict[str, str]] = []
    for record in dataframe.to_dict(orient="records"):
        ticker_value = record.get("A股代码")
        name_value = record.get("公司全称") or record.get("A股简称")
        if pd.isna(ticker_value) or pd.isna(name_value):
            continue
        ticker = str(ticker_value).strip()
        if ticker.endswith(".0"):
            ticker = ticker[:-2]
        ticker = ticker.zfill(6)
        name = str(name_value).strip()
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

    workbook_params = {
        "SHOWTYPE": "xlsx",
        "CATALOGID": SZSE_A_SHARE_CATALOG_ID,
        "TABKEY": SZSE_A_SHARE_TAB_KEY,
        "PAGENO": 1,
        "random": "0.001",
    }
    try:
        response = session.get(
            "https://www.szse.cn/api/report/ShowReport",
            params=workbook_params,
            headers=headers,
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        workbook_rows = parse_szse_a_share_workbook(response.content, source)
        if workbook_rows:
            return workbook_rows
    except (requests.RequestException, ValueError):
        pass

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
        sections = extract_szse_report_sections(payload)
        metadata = sections[0].get("metadata", {}) if sections else {}
        total_pages = int(metadata.get("pagecount") or total_pages)
        page += 1
    return rows


def fetch_szse_etf_list(source: MasterfileSource, session: requests.Session | None = None) -> list[dict[str, str]]:
    session = session or requests.Session()
    headers = {
        "User-Agent": USER_AGENT,
        "Referer": source.source_url,
        "Connection": "close",
    }
    session.get(source.source_url, headers={"User-Agent": USER_AGENT, "Connection": "close"}, timeout=REQUEST_TIMEOUT)

    workbook_params = {
        "SHOWTYPE": "xlsx",
        "CATALOGID": SZSE_ETF_CATALOG_ID,
        "TABKEY": SZSE_ETF_TAB_KEY,
        "PAGENO": 1,
        "random": "0.001",
    }
    try:
        response = session.get(
            "https://www.szse.cn/api/report/ShowReport",
            params=workbook_params,
            headers=headers,
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        workbook_rows = parse_szse_etf_workbook(response.content, source)
        if workbook_rows:
            return workbook_rows
    except (requests.RequestException, ValueError):
        pass

    rows: list[dict[str, str]] = []
    page = 1
    total_pages = 1
    while page <= total_pages:
        params = {
            "SHOWTYPE": "JSON",
            "CATALOGID": SZSE_ETF_CATALOG_ID,
            "TABKEY": SZSE_ETF_TAB_KEY,
            "PAGENO": page,
        }
        response = session.get(
            SZSE_REPORT_DATA_URL,
            params=params,
            headers=headers,
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        payload = response.json()
        page_rows = parse_szse_etf_list(payload, source)
        if not page_rows:
            break
        rows.extend(page_rows)
        sections = extract_szse_report_sections(payload)
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


def parse_b3_listed_funds_payload(payload: dict[str, Any], source: MasterfileSource) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for record in payload.get("results") or []:
        acronym = str(record.get("acronym") or "").strip().upper()
        name = str(record.get("fundName") or "").strip()
        if not acronym or not name:
            continue
        rows.append(
            {
                "source_key": source.key,
                "provider": source.provider,
                "source_url": source.source_url,
                "ticker": f"{acronym}11",
                "name": name,
                "exchange": "B3",
                "asset_type": "ETF",
                "listing_status": "active",
                "reference_scope": source.reference_scope,
                "official": "true",
            }
        )
    return rows


def parse_b3_bdr_companies_payload(payload: dict[str, Any], source: MasterfileSource) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for record in payload.get("results") or []:
        if str(record.get("typeBDR") or "").strip().upper() != "DRE":
            continue
        issuing_company = str(record.get("issuingCompany") or "").strip().upper()
        name = str(record.get("companyName") or "").strip()
        if not issuing_company or not name:
            continue
        rows.append(
            {
                "source_key": source.key,
                "provider": source.provider,
                "source_url": source.source_url,
                "ticker": f"{issuing_company}39",
                "name": name,
                "exchange": "B3",
                "asset_type": "ETF",
                "listing_status": "active",
                "reference_scope": source.reference_scope,
                "official": "true",
            }
        )
    return rows


def parse_nasdaq_nordic_stockholm_shares(payload: dict[str, Any], source: MasterfileSource) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for record in ((payload.get("data") or {}).get("instrumentListing") or {}).get("rows") or []:
        ticker = str(record.get("symbol", "")).strip()
        name = str(record.get("fullName", "")).strip()
        if not ticker or not name:
            continue
        rows.append(
            {
                "source_key": source.key,
                "provider": source.provider,
                "source_url": source.source_url,
                "ticker": ticker,
                "name": name,
                "exchange": "STO",
                "asset_type": "Stock",
                "listing_status": "active",
                "reference_scope": source.reference_scope,
                "official": "true",
            }
        )
    return rows


def parse_six_equity_issuers(payload: dict[str, Any], source: MasterfileSource) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for record in payload.get("itemList") or []:
        ticker = str(record.get("valorSymbol", "")).strip()
        name = str(record.get("company", "")).strip()
        if not ticker or not name:
            continue
        rows.append(
            {
                "source_key": source.key,
                "provider": source.provider,
                "source_url": source.source_url,
                "ticker": ticker,
                "name": name,
                "exchange": "SIX",
                "asset_type": "Stock",
                "listing_status": "active",
                "reference_scope": source.reference_scope,
                "official": "true",
            }
        )
    return rows


def parse_six_fund_products_csv(text: str, source: MasterfileSource) -> list[dict[str, str]]:
    lines = [line for line in text.splitlines() if line.strip()]
    if not lines:
        return []

    reader = csv.DictReader(io.StringIO("\n".join(lines)), delimiter=";")
    rows: list[dict[str, str]] = []
    for record in reader:
        ticker = str(record.get("ValorSymbol", "")).strip()
        name = str(record.get("FundLongName", "")).strip()
        if not ticker or not name:
            continue
        rows.append(
            {
                "source_key": source.key,
                "provider": source.provider,
                "source_url": source.source_url,
                "ticker": ticker,
                "name": name,
                "exchange": "SIX",
                "asset_type": "ETF",
                "listing_status": "active",
                "reference_scope": source.reference_scope,
                "official": "true",
            }
        )
    return rows


def extract_psx_xid(text: str) -> str:
    match = PSX_XID_RE.search(text)
    if not match:
        raise ValueError("Unable to locate PSX XID token")
    return unescape(match.group(1)).strip()


def extract_psx_sector_options(text: str) -> list[tuple[str, str]]:
    match = PSX_SECTOR_SELECT_RE.search(text)
    if not match:
        raise ValueError("Unable to locate PSX sector selector")
    options: list[tuple[str, str]] = []
    for value, label_html in PSX_OPTION_RE.findall(match.group("body")):
        normalized_value = unescape(value).strip()
        label = re.sub(r"<[^>]+>", "", unescape(label_html)).strip()
        if not normalized_value or not label:
            continue
        options.append((normalized_value, label))
    return options


def should_skip_psx_sector(label: str) -> bool:
    lowered = " ".join(label.lower().split())
    return any(marker in lowered for marker in PSX_SECTOR_LABEL_SKIP_MARKERS)


def infer_psx_asset_type_from_sector(label: str) -> str:
    lowered = " ".join(label.lower().split())
    if any(marker in lowered for marker in PSX_ETF_SECTOR_LABEL_MARKERS):
        return "ETF"
    return "Stock"


def parse_psx_listed_companies(
    payload: list[dict[str, Any]],
    source: MasterfileSource,
    *,
    sector_label: str,
) -> list[dict[str, str]]:
    asset_type = infer_psx_asset_type_from_sector(sector_label)
    rows: list[dict[str, str]] = []
    for record in payload:
        ticker = str(record.get("symbol_code", "")).strip()
        name = str(record.get("company_name", "")).strip()
        if not ticker or not name:
            continue
        rows.append(
            {
                "source_key": source.key,
                "provider": source.provider,
                "source_url": source.source_url,
                "ticker": ticker,
                "name": name,
                "exchange": "PSX",
                "asset_type": asset_type,
                "listing_status": "active",
                "reference_scope": source.reference_scope,
                "official": "true",
            }
        )
    return rows


def extract_psx_symbol_name_download_url(text: str) -> str:
    match = PSX_SYMBOL_NAME_DOWNLOAD_RE.search(text)
    if not match:
        raise ValueError("Unable to locate PSX symbol-name download link")
    return requests.compat.urljoin(PSX_DAILY_DOWNLOADS_URL, match.group(1))


def parse_psx_symbol_name_daily(
    content: bytes,
    source: MasterfileSource,
    *,
    asset_type_by_ticker: dict[str, str] | None = None,
) -> list[dict[str, str]]:
    asset_type_by_ticker = asset_type_by_ticker or {}
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        names = archive.namelist()
        if not names:
            return []
        payload = archive.read(names[0]).decode("utf-16")

    rows: list[dict[str, str]] = []
    for raw_line in payload.splitlines():
        line = raw_line.replace("\ufeff", "").strip()
        if not line:
            continue
        parts = [part.replace("\ufeff", "").strip() for part in line.split("|")]
        if len(parts) < 3:
            continue
        ticker, short_name, full_name = parts[:3]
        if not ticker:
            continue
        asset_type = asset_type_by_ticker.get(ticker)
        if not asset_type:
            continue
        rows.append(
            {
                "source_key": source.key,
                "provider": source.provider,
                "source_url": source.source_url,
                "ticker": ticker,
                "name": full_name or short_name,
                "exchange": "PSX",
                "asset_type": asset_type,
                "listing_status": "active",
                "reference_scope": source.reference_scope,
                "official": "true",
            }
        )
    return rows


def fetch_psx_symbol_name_daily(source: MasterfileSource, session: requests.Session | None = None) -> list[dict[str, str]]:
    session = session or requests.Session()
    asset_type_by_ticker = {
        row.get("ticker", "").strip(): row.get("asset_type", "").strip()
        for row in load_csv(LISTINGS_CSV)
        if row.get("exchange", "").strip() == "PSX"
        and row.get("asset_type", "").strip() in {"Stock", "ETF"}
        and row.get("ticker", "").strip()
    }
    if not asset_type_by_ticker:
        return []

    page_headers = {
        "User-Agent": USER_AGENT,
        "Referer": source.source_url,
        "Origin": "https://dps.psx.com.pk",
        "Connection": "close",
    }
    download_headers = {
        "User-Agent": USER_AGENT,
        "Referer": source.source_url,
        "Origin": "https://dps.psx.com.pk",
        "Connection": "close",
    }

    for days_back in range(8):
        target_date = (datetime.now(timezone.utc) - timedelta(days=days_back)).date().isoformat()
        response = session.post(
            source.source_url,
            data={"date": target_date},
            headers=page_headers,
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        try:
            download_url = extract_psx_symbol_name_download_url(response.text)
        except ValueError:
            continue

        download_response = session.get(download_url, headers=download_headers, timeout=REQUEST_TIMEOUT)
        download_response.raise_for_status()
        rows = parse_psx_symbol_name_daily(
            download_response.content,
            source,
            asset_type_by_ticker=asset_type_by_ticker,
        )
        for row in rows:
            row["source_url"] = download_url
        return rows
    return []


def fetch_sse_a_share_list(source: MasterfileSource, session: requests.Session | None = None) -> list[dict[str, str]]:
    session = session or requests.Session()
    headers = {
        "User-Agent": USER_AGENT,
        "Referer": source.source_url,
        "Connection": "close",
    }
    rows: list[dict[str, str]] = []
    seen_tickers: set[str] = set()
    for stock_type in SSE_STOCK_TYPES:
        response = session.get(
            SSE_COMMON_QUERY_URL,
            params={
                "jsonCallBack": SSE_JSONP_CALLBACK,
                "sqlId": "COMMON_SSE_CP_GPJCTPZ_GPLB_GP_L",
                "STOCK_TYPE": stock_type,
                "COMPANY_STATUS": "2,4,5,7,8",
                "type": "inParams",
                "isPagination": "true",
                "pageHelp.cacheSize": "1",
                "pageHelp.beginPage": "1",
                "pageHelp.pageSize": "5000",
                "pageHelp.pageNo": "1",
            },
            headers=headers,
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        payload = parse_sse_jsonp(response.text)
        for page_row in parse_sse_a_share_list(payload, source):
            ticker = page_row["ticker"]
            if ticker in seen_tickers:
                continue
            seen_tickers.add(ticker)
            rows.append(page_row)
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

    take = 1000

    def fetch_page(page: int) -> dict[str, Any]:
        response = session.post(
            f"{source.source_url}/{workday}/{workday}/{page}/{take}",
            headers=headers,
            json={},
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        payload = response.json()
        return payload.get("table") or {}

    first_table = fetch_page(1)
    page_count = int(first_table.get("pageCount") or 0) or 1

    rows: list[dict[str, str]] = []
    if page_count <= 5:
        rows.extend(parse_b3_instruments_equities_table(first_table, source))
        for page in range(2, page_count + 1):
            rows.extend(parse_b3_instruments_equities_table(fetch_page(page), source))
        return rows

    cash_rows_seen = False
    # The B3 consolidated table places cash equities in the last pages.
    for page in range(page_count, 0, -1):
        table = first_table if page == 1 else fetch_page(page)
        page_rows = parse_b3_instruments_equities_table(table, source)
        if page_rows:
            cash_rows_seen = True
            rows.extend(page_rows)
            continue
        if cash_rows_seen:
            break
    return rows


def fetch_b3_listed_funds(source: MasterfileSource, session: requests.Session | None = None) -> list[dict[str, str]]:
    session = session or requests.Session()
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json",
    }
    rows: list[dict[str, str]] = []
    for fund_type in B3_ETF_FUND_TYPES:
        filters = {
            "language": "en-us",
            "pageNumber": 1,
            "pageSize": B3_FUNDS_PAGE_SIZE,
            "typeFund": fund_type,
        }
        response = session.get(
            source.source_url + "GetListFunds/" + b64encode_json(filters, latin1_safe=True),
            headers=headers,
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        payload = response.json()
        rows.extend(parse_b3_listed_funds_payload(payload, source))
        total_pages = int((payload.get("page") or {}).get("totalPages") or 1)
        for page in range(2, total_pages + 1):
            filters["pageNumber"] = page
            response = session.get(
                source.source_url + "GetListFunds/" + b64encode_json(filters, latin1_safe=True),
                headers=headers,
                timeout=REQUEST_TIMEOUT,
            )
            response.raise_for_status()
            rows.extend(parse_b3_listed_funds_payload(response.json(), source))
    return rows


def fetch_b3_bdr_companies(source: MasterfileSource, session: requests.Session | None = None) -> list[dict[str, str]]:
    session = session or requests.Session()
    response = session.get(
        source.source_url + "GetCompaniesBDR/" + b64encode_json({"language": "en-us"}),
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
        },
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return parse_b3_bdr_companies_payload(response.json(), source)


def fetch_jse_exchange_traded_product_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    session = session or requests.Session()
    page_response = session.get(
        source.source_url,
        headers={"User-Agent": USER_AGENT},
        timeout=REQUEST_TIMEOUT,
    )
    page_response.raise_for_status()
    product_type = "ETF" if source.key == "jse_etf_list" else "ETN"
    download_url = extract_jse_exchange_traded_product_download_url(page_response.text, product_type)
    if not download_url:
        raise ValueError(f"Could not locate JSE {product_type} workbook URL")
    workbook_response = session.get(
        download_url,
        headers={
            "User-Agent": USER_AGENT,
            "Referer": source.source_url,
        },
        timeout=REQUEST_TIMEOUT,
    )
    workbook_response.raise_for_status()
    return parse_jse_exchange_traded_product_excel(
        workbook_response.content,
        source,
        source_url=download_url,
    )


def fetch_nasdaq_nordic_stockholm_shares(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    session = session or requests.Session()
    headers = nasdaq_nordic_request_headers()
    response = session.get(
        source.source_url,
        params={
            "category": "MAIN_MARKET",
            "tableonly": "false",
            "market": "STO",
        },
        headers=headers,
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return parse_nasdaq_nordic_stockholm_shares(response.json(), source)


def fetch_six_equity_issuers(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    session = session or requests.Session()
    response = session.get(
        source.source_url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "application/json,text/plain,*/*",
            "Referer": "https://www.six-group.com/en/market-data/shares/companies.html",
        },
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return parse_six_equity_issuers(response.json(), source)


def fetch_six_fund_products(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    session = session or requests.Session()
    referer = SIX_ETP_EXPLORER_URL if "etp" in source.key else SIX_ETF_EXPLORER_URL
    response = session.get(
        source.source_url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "text/csv,application/octet-stream,*/*",
            "Referer": referer,
        },
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return parse_six_fund_products_csv(response.text, source)


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


def fetch_psx_listed_companies(source: MasterfileSource, session: requests.Session | None = None) -> list[dict[str, str]]:
    session = session or requests.Session()
    page_response = session.get(source.source_url, headers=psx_request_headers(), timeout=REQUEST_TIMEOUT)
    page_response.raise_for_status()
    page_html = page_response.text
    xid = extract_psx_xid(page_html)
    sector_options = extract_psx_sector_options(page_html)

    rows: list[dict[str, str]] = []
    seen_tickers: set[str] = set()
    for sector_value, sector_label in sector_options:
        if should_skip_psx_sector(sector_label):
            continue
        try:
            response = session.get(
                PSX_COMPANIES_BY_SECTOR_URL,
                params={"sector": sector_value, "XID": xid},
                headers=psx_request_headers(ajax=True),
                timeout=REQUEST_TIMEOUT,
            )
            response.raise_for_status()
        except requests.RequestException:
            if not isinstance(session, requests.Session):
                raise
            response = requests.get(
                PSX_COMPANIES_BY_SECTOR_URL,
                params={"sector": sector_value, "XID": xid},
                headers=psx_request_headers(ajax=True),
                timeout=REQUEST_TIMEOUT,
            )
            response.raise_for_status()
        try:
            payload = json.loads(response.text)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Unexpected PSX sector payload for {sector_value}") from exc
        for row in parse_psx_listed_companies(payload, source, sector_label=sector_label):
            ticker = row["ticker"]
            if ticker in seen_tickers:
                continue
            seen_tickers.add(ticker)
            rows.append(row)
    return rows


def fetch_asx_investment_products(source: MasterfileSource, session: requests.Session | None = None) -> list[dict[str, str]]:
    session = session or requests.Session()
    page_response = session.get(source.source_url, headers={"User-Agent": USER_AGENT}, timeout=REQUEST_TIMEOUT)
    page_response.raise_for_status()
    workbook_url = extract_latest_asx_investment_products_url(page_response.text)
    workbook_response = session.get(workbook_url, headers={"User-Agent": USER_AGENT}, timeout=REQUEST_TIMEOUT)
    workbook_response.raise_for_status()
    rows = parse_asx_investment_products_excel(workbook_response.content, source)
    for row in rows:
        row["source_url"] = workbook_url
    return rows


def fetch_source_rows(source: MasterfileSource, session: requests.Session | None = None) -> list[dict[str, str]]:
    if source.format == "nasdaq_listed_pipe":
        text = fetch_text(source.source_url, session=session)
        return parse_nasdaq_listed(text, source)
    if source.format == "nasdaq_other_listed_pipe":
        text = fetch_text(source.source_url, session=session)
        return parse_other_listed(text, source)
    if source.format == "lse_company_reports_html":
        return fetch_lse_company_reports(source, session=session)
    if source.format == "lse_instrument_directory_html":
        return fetch_lse_instrument_directory(source, session=session)
    if source.format == "lse_instrument_search_html":
        return fetch_lse_instrument_search_exact(
            source,
            lse_instrument_search_target_tickers([]),
            session=session,
        )
    if source.format == "asx_listed_companies_csv":
        text = fetch_text(source.source_url, session=session)
        return parse_asx_listed_companies(text, source)
    if source.format == "asx_investment_products_excel":
        return fetch_asx_investment_products(source, session=session)
    if source.format == "cboe_canada_listing_directory_html":
        text = fetch_text(source.source_url, session=session)
        return parse_cboe_canada_listing_directory_html(text, source)
    if source.format == "set_listed_companies_html":
        text = fetch_bytes(source.source_url, session=session).decode("windows-1250", errors="replace")
        return parse_set_listed_companies_html(text, source)
    if source.format == "tmx_listed_issuers_excel":
        content, mode = load_tmx_listed_issuers_content(session=session)
        if content is None:
            return []
        return parse_tmx_listed_issuers_excel(content, source)
    if source.format == "tmx_interlisted_tab":
        text = fetch_text(source.source_url, session=session)
        return parse_tmx_interlisted(text, source)
    if source.format == "tmx_etf_screener_json":
        payload = fetch_json(source.source_url, session=session)
        return parse_tmx_etf_screener(payload, source)
    if source.format in {"jse_etf_list_xlsx", "jse_etn_list_xlsx"}:
        return fetch_jse_exchange_traded_product_rows(source, session=session)
    if source.format == "euronext_equities_semicolon_csv":
        text = fetch_text(source.source_url, session=session)
        return parse_euronext_equities_download(text, source)
    if source.format == "euronext_etfs_semicolon_csv":
        text = fetch_text(source.source_url, session=session)
        return parse_euronext_etfs_download(text, source)
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
    if source.format == "six_equity_issuers_json":
        return fetch_six_equity_issuers(source, session=session)
    if source.format == "six_fund_products_csv":
        return fetch_six_fund_products(source, session=session)
    if source.format == "b3_instruments_equities_api":
        return fetch_b3_instruments_equities(source, session=session)
    if source.format == "b3_listed_funds_api":
        return fetch_b3_listed_funds(source, session=session)
    if source.format == "b3_bdr_companies_api":
        return fetch_b3_bdr_companies(source, session=session)
    if source.format == "nasdaq_nordic_stockholm_shares_json":
        return fetch_nasdaq_nordic_stockholm_shares(source, session=session)
    if source.format == "twse_listed_companies_json":
        payload = fetch_json(source.source_url, session=session)
        return parse_twse_listed_companies(payload, source)
    if source.format == "twse_etf_list_json":
        payload = fetch_json(source.source_url, session=session)
        return parse_twse_etf_list(payload, source)
    if source.format == "sse_a_share_list_jsonp":
        return fetch_sse_a_share_list(source, session=session)
    if source.format == "sse_etf_list_json":
        return fetch_sse_etf_list(source, session=session)
    if source.format == "szse_a_share_list_json":
        return fetch_szse_a_share_list(source, session=session)
    if source.format == "szse_etf_list_json":
        return fetch_szse_etf_list(source, session=session)
    if source.format == "tpex_mainboard_quotes_json":
        payload = fetch_json(source.source_url, session=session)
        return parse_tpex_mainboard_quotes(payload, source)
    if source.format == "krx_listed_companies_json":
        return fetch_krx_listed_companies(source, session=session)
    if source.format == "krx_etf_finder_json":
        return fetch_krx_etf_finder(source, session=session)
    if source.format == "psx_listed_companies_json":
        return fetch_psx_listed_companies(source, session=session)
    if source.format == "psx_symbol_name_daily_zip":
        return fetch_psx_symbol_name_daily(source, session=session)
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
    if source.format == "lse_instrument_directory_html":
        rows, mode = load_lse_instrument_directory_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("LSE instrument directory unavailable")
        return rows, mode
    if source.format == "lse_instrument_search_html":
        rows, mode = load_lse_instrument_search_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("LSE instrument search unavailable")
        return rows, mode
    if source.format == "b3_instruments_equities_api":
        rows, mode = load_b3_instruments_equities_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("B3 instruments equities unavailable")
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
    if source.format in {"jse_etf_list_xlsx", "jse_etn_list_xlsx"}:
        rows, mode = load_jse_exchange_traded_product_rows(source, session=session)
        if rows is None:
            raise requests.RequestException(f"{source.provider} {source.key} workbook unavailable")
        return rows, mode
    if source.format == "nasdaq_nordic_stockholm_shares_json":
        rows, mode = load_nasdaq_nordic_stockholm_shares_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("Nasdaq Nordic Stockholm shares unavailable")
        return rows, mode
    if source.format == "tmx_listed_issuers_excel":
        content, mode = load_tmx_listed_issuers_content(session=session)
        if content is None:
            raise requests.RequestException("TMX listed issuers workbook unavailable")
        return parse_tmx_listed_issuers_excel(content, source), mode
    if source.format == "tmx_etf_screener_json":
        payload, mode = load_tmx_etf_screener_payload(session=session)
        if payload is None:
            raise requests.RequestException("TMX ETF screener dataset unavailable")
        return parse_tmx_etf_screener(payload, source), mode
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
        ["source_key", "provider", "source_url", "ticker", "name", "exchange", "asset_type", "listing_status", "reference_scope", "official", "isin"],
        rows,
    )
    MASTERFILE_SUMMARY_JSON.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps({"reference_csv": str(MASTERFILE_REFERENCE_CSV.relative_to(ROOT)), **summary}, indent=2))


if __name__ == "__main__":
    main()

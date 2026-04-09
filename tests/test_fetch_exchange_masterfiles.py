from __future__ import annotations

import io
import json
import zipfile
from base64 import b64decode
from datetime import datetime, timezone

import pandas as pd
import requests
import scripts.fetch_exchange_masterfiles as fetch_exchange_masterfiles

from scripts.fetch_exchange_masterfiles import (
    B3_INSTRUMENTS_EQUITIES_CACHE,
    JSE_INSTRUMENT_SEARCH_CACHE,
    LEGACY_B3_INSTRUMENTS_EQUITIES_CACHE,
    LEGACY_LSE_COMPANY_REPORTS_CACHE,
    LEGACY_LSE_INSTRUMENT_DIRECTORY_CACHE,
    LEGACY_LSE_INSTRUMENT_SEARCH_CACHE,
    LEGACY_NASDAQ_NORDIC_COPENHAGEN_SHARES_CACHE,
    LEGACY_NASDAQ_NORDIC_HELSINKI_ETFS_CACHE,
    LEGACY_NASDAQ_NORDIC_STOCKHOLM_ETFS_CACHE,
    LEGACY_NASDAQ_NORDIC_STOCKHOLM_TRACKERS_CACHE,
    LEGACY_SET_ETF_SEARCH_CACHE,
    LEGACY_SZSE_ETF_LIST_CACHE,
    LEGACY_TMX_ETF_SCREENER_CACHE,
    LEGACY_TPEX_ETF_FILTER_CACHE,
    LEGACY_TPEX_MAINBOARD_QUOTES_CACHE,
    LSE_PAGE_INITIALS,
    LSE_COMPANY_REPORTS_CACHE,
    LSE_INSTRUMENT_DIRECTORY_CACHE,
    LSE_INSTRUMENT_SEARCH_CACHE,
    JSE_EXCHANGE_TRADED_PRODUCTS_URL,
    JSE_SEARCH_URL,
    MasterfileSource,
    OFFICIAL_SOURCES,
    extract_lse_last_page,
    extract_jse_exchange_traded_product_download_url,
    extract_jse_instrument_metadata,
    extract_jse_instrument_search_links,
    extract_latest_asx_investment_products_url,
    SSE_ETF_SUBCLASSES,
    TMX_ETF_SCREENER_CACHE,
    TPEX_ETF_FILTER_CACHE,
    TPEX_MAINBOARD_QUOTES_CACHE,
    fetch_b3_instruments_equities,
    fetch_b3_bdr_companies,
    fetch_b3_listed_funds,
    fetch_all_sources,
    fetch_krx_etf_finder,
    fetch_krx_listed_companies,
    fetch_lse_company_reports,
    fetch_lse_instrument_directory,
    fetch_lse_instrument_search_exact,
    fetch_jse_exchange_traded_product_rows,
    fetch_jse_instrument_search_exact,
    fetch_nasdaq_nordic_helsinki_shares_search,
    fetch_nasdaq_nordic_stockholm_shares,
    fetch_nasdaq_nordic_stockholm_trackers,
    fetch_psx_listed_companies,
    fetch_psx_symbol_name_daily,
    fetch_six_equity_issuers,
    fetch_six_fund_products,
    fetch_sse_a_share_list,
    fetch_sse_etf_list,
    fetch_szse_a_share_list,
    fetch_szse_etf_list,
    fetch_source_rows_with_mode,
    fetch_tmx_money_etfs,
    fetch_tmx_stock_quote_rows,
    fetch_tmx_etf_screener_quote_rows,
    infer_jpx_asset_type,
    infer_lse_lookup_asset_type,
    load_b3_instruments_equities_rows,
    load_jse_instrument_search_rows,
    NASDAQ_NORDIC_COPENHAGEN_SHARES_CACHE,
    NASDAQ_NORDIC_HELSINKI_ETFS_CACHE,
    load_lse_company_reports_rows,
    load_lse_instrument_directory_rows,
    load_lse_instrument_search_rows,
    load_nasdaq_nordic_share_search_rows,
    load_nasdaq_nordic_stockholm_etf_rows,
    load_nasdaq_nordic_stockholm_tracker_rows,
    load_nasdaq_nordic_stockholm_shares_rows,
    load_set_etf_search_rows,
    load_szse_etf_list_rows,
    merge_reference_rows,
    NASDAQ_NORDIC_STOCKHOLM_ETFS_CACHE,
    NASDAQ_NORDIC_STOCKHOLM_TRACKERS_CACHE,
    load_sec_company_tickers_exchange_payload,
    normalize_source_keys,
    load_tpex_etf_filter_payload,
    load_tmx_etf_screener_payload,
    load_tpex_mainboard_quotes_payload,
    parse_tpex_etf_filter,
    lse_instrument_search_target_tickers,
    parse_asx_listed_companies,
    parse_asx_investment_products_excel,
    parse_b3_bdr_companies_payload,
    parse_b3_instruments_equities_table,
    parse_b3_listed_funds_payload,
    parse_cboe_canada_listing_directory_html,
    parse_deutsche_boerse_etfs_etps_excel,
    parse_deutsche_boerse_listed_companies_excel,
    parse_deutsche_boerse_xetra_all_tradable_csv,
    parse_euronext_equities_download,
    parse_euronext_etfs_download,
    parse_jpx_listed_issues_excel,
    parse_jse_exchange_traded_product_excel,
    parse_krx_etf_finder,
    parse_krx_listed_companies,
    parse_krx_product_finder_records,
    parse_krx_stock_finder_records,
    parse_nasdaq_nordic_shares,
    parse_nasdaq_nordic_stockholm_etfs,
    parse_nasdaq_nordic_stockholm_trackers,
    parse_lse_company_reports_html,
    parse_nasdaq_nordic_stockholm_shares,
    parse_nasdaq_listed,
    parse_other_listed,
    parse_psx_listed_companies,
    parse_psx_symbol_name_daily,
    parse_set_listed_companies_html,
    parse_set_quote_search_payload,
    parse_sec_company_tickers_exchange,
    parse_six_equity_issuers,
    parse_six_fund_products_csv,
    parse_sse_a_share_list,
    parse_sse_etf_list,
    parse_szse_a_share_list,
    parse_szse_a_share_workbook,
    parse_szse_etf_list,
    parse_szse_etf_workbook,
    parse_tpex_mainboard_quotes,
    parse_twse_etf_list,
    parse_twse_listed_companies,
    parse_tmx_etf_screener,
    parse_tmx_interlisted,
    parse_tmx_listed_issuers_excel,
    resolve_tmx_listed_issuers_download_url,
    tmx_stock_quote_symbol_variants,
    sec_request_headers,
    select_official_sources,
    extract_psx_sector_options,
    extract_psx_symbol_name_download_url,
    extract_psx_xid,
    SZSE_ETF_LIST_CACHE,
    SET_ETF_SEARCH_CACHE,
    TMX_MONEY_GRAPHQL_URL,
)


SOURCE = MasterfileSource(
    key="test",
    provider="test",
    description="test",
    source_url="https://example.com",
    format="test",
)

SUBSET_SOURCE = MasterfileSource(
    key="subset",
    provider="test",
    description="subset",
    source_url="https://example.com/subset",
    format="test",
    reference_scope="interlisted_subset",
)


def test_parse_nasdaq_listed_maps_etf_and_status():
    text = "\n".join(
        [
            "Symbol|Security Name|Market Category|Test Issue|Financial Status|Round Lot Size|ETF|NextShares",
            "AAPL|Apple Inc.|Q|N|N|100|N|N",
            "QQQ|Invesco QQQ Trust, Series 1|Q|N|N|100|Y|N",
            "TEST|Test Issue Corp|Q|Y|N|100|N|N",
            "File Creation Time: 04022026",
        ]
    )

    rows = parse_nasdaq_listed(text, SOURCE)

    assert rows[0]["ticker"] == "AAPL"
    assert rows[0]["exchange"] == "NASDAQ"
    assert rows[0]["asset_type"] == "Stock"
    assert rows[1]["asset_type"] == "ETF"
    assert rows[2]["listing_status"] == "test"


def test_parse_other_listed_maps_exchange_codes():
    text = "\n".join(
        [
            "ACT Symbol|Security Name|Exchange|CQS Symbol|ETF|Round Lot Size|Test Issue|NASDAQ Symbol",
            "IBM|International Business Machines|N|IBM|N|100|N|IBM",
            "SPY|SPDR S&P 500 ETF TRUST|P|SPY|Y|100|N|SPY",
            "File Creation Time: 04022026",
        ]
    )

    rows = parse_other_listed(text, SOURCE)

    assert rows == [
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "IBM",
            "name": "International Business Machines",
            "exchange": "NYSE",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "SPY",
            "name": "SPDR S&P 500 ETF TRUST",
            "exchange": "NYSE ARCA",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
    ]


def test_parse_sec_company_tickers_exchange_normalizes_exchange_names():
    payload = {
        "fields": ["cik", "name", "ticker", "exchange"],
        "data": [
            [320193, "Apple Inc.", "AAPL", "Nasdaq"],
            [732717, "AT&T Inc.", "T", "NYSE"],
            [884394, "SPDR S&P 500 ETF TRUST", "SPY", "NYSE"],
            [111, "Ignored", "BAD", None],
        ],
    }

    rows = parse_sec_company_tickers_exchange(payload, SOURCE)

    assert rows[0]["exchange"] == "NASDAQ"
    assert rows[0]["asset_type"] == "Stock"
    assert rows[2]["asset_type"] == "ETF"
    assert [row["ticker"] for row in rows] == ["AAPL", "T", "SPY"]


def test_parse_twse_listed_companies_maps_twse_rows():
    payload = [
        {"公司代號": "1101", "公司名稱": "臺灣水泥股份有限公司"},
        {"公司代號": "0050", "公司名稱": "元大台灣50"},
        {"公司代號": "", "公司名稱": "Ignored"},
    ]

    rows = parse_twse_listed_companies(payload, SOURCE)

    assert rows == [
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "1101",
            "name": "臺灣水泥股份有限公司",
            "exchange": "TWSE",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "0050",
            "name": "元大台灣50",
            "exchange": "TWSE",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
    ]


def test_parse_twse_etf_list_maps_twse_rows():
    payload = {
        "stat": "OK",
        "fields": ["Listing Date", "Security Code", "Name of ETF", "Issuer"],
        "data": [
            ["2026.04.10", "00401A", "JPMorgan (Taiwan) Taiwan Equity High Income Active ETF", "JPMorgan"],
            ["2026.03.23", "009818", "Hua Nan NASDAQ 100 Technology ETF", "Hua Nan"],
            ["2026.01.01", "", "Ignored", "Issuer"],
        ],
    }

    rows = parse_twse_etf_list(payload, SOURCE)

    assert rows == [
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "00401A",
            "name": "JPMorgan (Taiwan) Taiwan Equity High Income Active ETF",
            "exchange": "TWSE",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "009818",
            "name": "Hua Nan NASDAQ 100 Technology ETF",
            "exchange": "TWSE",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
    ]


def test_twse_etf_source_is_modeled_as_partial_official_coverage() -> None:
    source = next(item for item in OFFICIAL_SOURCES if item.key == "twse_etf_list")

    assert source.provider == "TWSE"
    assert source.reference_scope == "listed_companies_subset"


def test_parse_sse_a_share_list_maps_sse_rows() -> None:
    payload = {
        "result": [
            {"A_STOCK_CODE": "600000", "FULL_NAME": "上海浦东发展银行股份有限公司", "SEC_NAME_CN": "浦发银行", "STOCK_TYPE": "1"},
            {
                "A_STOCK_CODE": "600054",
                "B_STOCK_CODE": "900942",
                "FULL_NAME": "黄山旅游发展股份有限公司",
                "SEC_NAME_CN": "黄山Ｂ股",
                "STOCK_TYPE": "2",
            },
            {"A_STOCK_CODE": "688001", "FULL_NAME": "苏州华兴源创科技股份有限公司", "SEC_NAME_CN": "华兴源创", "STOCK_TYPE": "8"},
            {"A_STOCK_CODE": "", "FULL_NAME": "Ignored"},
        ]
    }

    rows = parse_sse_a_share_list(payload, SOURCE)

    assert rows == [
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "600000",
            "name": "上海浦东发展银行股份有限公司",
            "exchange": "SSE",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "900942",
            "name": "黄山旅游发展股份有限公司",
            "exchange": "SSE",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "688001",
            "name": "苏州华兴源创科技股份有限公司",
            "exchange": "SSE",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
    ]


def test_fetch_sse_a_share_list_fetches_all_pages() -> None:
    source = MasterfileSource(
        key="sse",
        provider="SSE",
        description="SSE A-share list",
        source_url="https://www.sse.com.cn/assortment/stock/list/share/",
        format="sse_a_share_list_jsonp",
        reference_scope="listed_companies_subset",
    )

    class FakeResponse:
        def __init__(self, text):
            self.text = text

        def raise_for_status(self):
            return None

    class FakeSession:
        def __init__(self):
            self.calls = []

        def get(self, url, params=None, headers=None, **kwargs):
            self.calls.append((url, params, headers, kwargs))
            stock_type = params["STOCK_TYPE"]
            if stock_type == "1":
                return FakeResponse(
                    'jsonpCallback({"pageHelp":{"pageCount":1},"result":[{"A_STOCK_CODE":"600000","FULL_NAME":"上海浦东发展银行股份有限公司","STOCK_TYPE":"1"}]})'
                )
            if stock_type == "2":
                return FakeResponse(
                    'jsonpCallback({"pageHelp":{"pageCount":1},"result":[{"A_STOCK_CODE":"600054","B_STOCK_CODE":"900942","FULL_NAME":"黄山旅游发展股份有限公司","STOCK_TYPE":"2"}]})'
                )
            return FakeResponse(
                'jsonpCallback({"pageHelp":{"pageCount":1},"result":[{"A_STOCK_CODE":"688001","FULL_NAME":"苏州华兴源创科技股份有限公司","STOCK_TYPE":"8"}]})'
            )

    session = FakeSession()
    rows = fetch_sse_a_share_list(source, session=session)

    assert [row["ticker"] for row in rows] == ["600000", "900942", "688001"]
    assert [call[1]["STOCK_TYPE"] for call in session.calls] == ["1", "2", "8"]
    assert all(call[1]["pageHelp.pageNo"] == "1" for call in session.calls)
    assert all(call[1]["pageHelp.pageSize"] == "5000" for call in session.calls)
    assert all(call[1]["sqlId"] == "COMMON_SSE_CP_GPJCTPZ_GPLB_GP_L" for call in session.calls)


def test_sse_source_is_modeled_as_partial_official_coverage() -> None:
    source = next(item for item in OFFICIAL_SOURCES if item.key == "sse_a_share_list")
    assert source.reference_scope == "listed_companies_subset"


def test_parse_sse_etf_list_maps_sse_rows() -> None:
    payload = {
        "result": [
            {"fundCode": "510300", "secNameFull": "沪深300ETF华泰柏瑞", "fundAbbr": "300ETF"},
            {"fundCode": "513100", "secNameFull": "", "fundAbbr": "纳指ETF"},
            {"fundCode": "", "secNameFull": "Ignored"},
        ]
    }

    rows = parse_sse_etf_list(payload, SOURCE)

    assert rows == [
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "510300",
            "name": "沪深300ETF华泰柏瑞",
            "exchange": "SSE",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "513100",
            "name": "纳指ETF",
            "exchange": "SSE",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
    ]


def test_fetch_sse_etf_list_fetches_all_pages() -> None:
    source = MasterfileSource(
        key="sse_etf_list",
        provider="SSE",
        description="SSE ETF list",
        source_url="https://www.sse.com.cn/assortment/fund/etf/list/",
        format="sse_etf_list_json",
        reference_scope="listed_companies_subset",
    )

    class FakeResponse:
        def __init__(self, payload):
            self._payload = payload

        def raise_for_status(self):
            return None

        def json(self):
            return self._payload

    class FakeSession:
        def __init__(self):
            self.calls = []

        def get(self, url, params=None, headers=None, **kwargs):
            self.calls.append((url, params, headers, kwargs))
            subclass = params["subClass"]
            page = params["pageHelp.pageNo"]
            if subclass == "03" and page == "1":
                return FakeResponse(
                    {
                        "pageHelp": {"pageCount": 2},
                        "result": [{"fundCode": "510300", "secNameFull": "沪深300ETF华泰柏瑞"}],
                    }
                )
            if subclass == "03" and page == "2":
                return FakeResponse(
                    {
                        "pageHelp": {"pageCount": 2},
                        "result": [{"fundCode": "513100", "secNameFull": "纳指ETF"}],
                    }
                )
            return FakeResponse({"pageHelp": {"pageCount": 1}, "result": []})

    session = FakeSession()
    rows = fetch_sse_etf_list(source, session=session)

    assert [row["ticker"] for row in rows] == ["510300", "513100"]
    assert all(call[1]["sqlId"] == "FUND_LIST" for call in session.calls)
    assert all(call[1]["fundType"] == "00" for call in session.calls)
    assert sorted({call[1]["subClass"] for call in session.calls}) == list(SSE_ETF_SUBCLASSES)
    assert [call[1]["pageHelp.pageNo"] for call in session.calls if call[1]["subClass"] == "03"] == ["1", "2"]


def test_sse_etf_source_is_modeled_as_partial_official_coverage() -> None:
    source = next(item for item in OFFICIAL_SOURCES if item.key == "sse_etf_list")
    assert source.reference_scope == "listed_companies_subset"


def test_parse_szse_a_share_list_maps_szse_rows() -> None:
    payload = {
        "result": [
            {
                "metadata": {"pagecount": 1, "recordcount": 2},
                "data": [
                    {"agdm": "000001", "agjc": '<a href="/x">平安银行</a>'},
                    {"agdm": "300750", "agjc": '<a href="/y">宁德时代</a>'},
                    {"agdm": "", "agjc": "Ignored"},
                ],
            }
        ]
    }

    rows = parse_szse_a_share_list(payload, SOURCE)

    assert rows == [
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "000001",
            "name": "平安银行",
            "exchange": "SZSE",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "300750",
            "name": "宁德时代",
            "exchange": "SZSE",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
    ]


def test_parse_szse_a_share_workbook_maps_szse_rows() -> None:
    dataframe = pd.DataFrame(
        [
            {"A股代码": 1, "公司全称": "平安银行股份有限公司"},
            {"A股代码": "300750", "公司全称": "宁德时代新能源科技股份有限公司"},
            {"A股代码": None, "公司全称": "Ignored"},
        ]
    )
    content = io.BytesIO()
    with pd.ExcelWriter(content, engine="openpyxl") as writer:
        dataframe.to_excel(writer, sheet_name="A股列表", index=False)

    rows = parse_szse_a_share_workbook(content.getvalue(), SOURCE)

    assert rows == [
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "000001",
            "name": "平安银行股份有限公司",
            "exchange": "SZSE",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "300750",
            "name": "宁德时代新能源科技股份有限公司",
            "exchange": "SZSE",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
    ]


def test_parse_szse_etf_list_maps_szse_rows() -> None:
    payload = {
        "result": [
            {
                "metadata": {"pagecount": 1, "recordcount": 2},
                "data": [
                    {
                        "sys_key": '<a href="/x"><u>159176</u></a>',
                        "kzjcurl": '<a href="/y"><u>家电ETF华宝</u></a>',
                    },
                    {
                        "sys_key": '<a href="/z"><u>159869</u></a>',
                        "kzjcurl": '<a href="/q"><u>游戏ETF</u></a>',
                    },
                    {"sys_key": "", "kzjcurl": "Ignored"},
                ],
            }
        ]
    }

    rows = parse_szse_etf_list(payload, SOURCE)

    assert rows == [
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "159176",
            "name": "家电ETF华宝",
            "exchange": "SZSE",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "159869",
            "name": "游戏ETF",
            "exchange": "SZSE",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
    ]


def test_parse_szse_etf_workbook_maps_szse_rows() -> None:
    dataframe = pd.DataFrame(
        [
            {"证券代码": 159176, "证券简称": "家电ETF华宝"},
            {"证券代码": "159869", "证券简称": "游戏ETF"},
            {"证券代码": None, "证券简称": "Ignored"},
        ]
    )
    content = io.BytesIO()
    with pd.ExcelWriter(content, engine="openpyxl") as writer:
        dataframe.to_excel(writer, sheet_name="ETF列表", index=False)

    rows = parse_szse_etf_workbook(content.getvalue(), SOURCE)

    assert rows == [
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "159176",
            "name": "家电ETF华宝",
            "exchange": "SZSE",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "159869",
            "name": "游戏ETF",
            "exchange": "SZSE",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
    ]


def test_fetch_szse_a_share_list_fetches_all_pages() -> None:
    source = MasterfileSource(
        key="szse",
        provider="SZSE",
        description="SZSE A-share list",
        source_url="https://www.szse.cn/market/product/stock/list/index.html",
        format="szse_a_share_list_json",
        reference_scope="listed_companies_subset",
    )

    class FakeResponse:
        def __init__(self, payload=None, content=b""):
            self._payload = payload
            self.content = content

        def raise_for_status(self):
            return None

        def json(self):
            return self._payload

    class FakeSession:
        def __init__(self):
            self.calls = []

        def get(self, url, params=None, headers=None, **kwargs):
            self.calls.append((url, params, headers, kwargs))
            if params is None:
                return FakeResponse({})
            if params.get("SHOWTYPE") == "xlsx":
                return FakeResponse({}, b"not-an-excel-file")
            page = params["PAGENO"]
            if page == 1:
                return FakeResponse(
                    {
                        "result": [
                            {
                                "metadata": {"pagecount": 2, "recordcount": 2},
                                "data": [{"agdm": "000001", "agjc": '<a href="/x">平安银行</a>'}],
                            }
                        ]
                    }
                )
            return FakeResponse(
                {
                    "result": [
                        {
                            "metadata": {"pagecount": 2, "recordcount": 2},
                            "data": [{"agdm": "300750", "agjc": '<a href="/y">宁德时代</a>'}],
                        }
                    ]
                }
            )

    session = FakeSession()
    rows = fetch_szse_a_share_list(source, session=session)

    assert [row["ticker"] for row in rows] == ["000001", "300750"]
    api_calls = [call for call in session.calls if call[1] is not None and "SHOWTYPE" not in call[1]]
    assert [call[1]["PAGENO"] for call in api_calls] == [1, 2]
    assert all(call[1]["CATALOGID"] == "1110" for call in api_calls)
    assert all(call[1]["TABKEY"] == "tab1" for call in api_calls)


def test_fetch_szse_etf_list_fetches_all_pages() -> None:
    source = MasterfileSource(
        key="szse_etf",
        provider="SZSE",
        description="SZSE ETF list",
        source_url="https://www.szse.cn/market/product/list/etfList/index.html",
        format="szse_etf_list_json",
        reference_scope="listed_companies_subset",
    )

    class FakeResponse:
        def __init__(self, payload=None, content=b""):
            self._payload = payload
            self.content = content

        def raise_for_status(self):
            return None

        def json(self):
            return self._payload

    class FakeSession:
        def __init__(self):
            self.calls = []

        def get(self, url, params=None, headers=None, **kwargs):
            self.calls.append((url, params, headers, kwargs))
            if params is None:
                return FakeResponse({})
            if params.get("SHOWTYPE") == "xlsx":
                return FakeResponse({}, b"not-an-excel-file")
            page = params["PAGENO"]
            if page == 1:
                return FakeResponse(
                    {
                        "result": [
                            {
                                "metadata": {"pagecount": 2, "recordcount": 2},
                                "data": [
                                    {
                                        "sys_key": '<a href="/x"><u>159176</u></a>',
                                        "kzjcurl": '<a href="/y"><u>家电ETF华宝</u></a>',
                                    }
                                ],
                            }
                        ]
                    }
                )
            return FakeResponse(
                {
                    "result": [
                        {
                            "metadata": {"pagecount": 2, "recordcount": 2},
                            "data": [
                                {
                                    "sys_key": '<a href="/z"><u>159869</u></a>',
                                    "kzjcurl": '<a href="/q"><u>游戏ETF</u></a>',
                                }
                            ],
                        }
                    ]
                }
            )

    session = FakeSession()
    rows = fetch_szse_etf_list(source, session=session)

    assert [row["ticker"] for row in rows] == ["159176", "159869"]
    api_calls = [call for call in session.calls if call[1] is not None and call[1].get("SHOWTYPE") != "xlsx"]
    assert [call[1]["PAGENO"] for call in api_calls] == [1, 2]
    assert all(call[1]["SHOWTYPE"] == "JSON" for call in api_calls)
    assert all(call[1]["CATALOGID"] == "1945" for call in api_calls)
    assert all(call[1]["TABKEY"] == "tab1" for call in api_calls)


def test_szse_source_is_modeled_as_partial_official_coverage() -> None:
    source = next(item for item in OFFICIAL_SOURCES if item.key == "szse_a_share_list")
    assert source.reference_scope == "listed_companies_subset"


def test_szse_etf_source_is_modeled_as_partial_official_coverage() -> None:
    source = next(item for item in OFFICIAL_SOURCES if item.key == "szse_etf_list")
    assert source.reference_scope == "listed_companies_subset"


def test_set_etf_search_source_is_modeled_as_partial_official_coverage() -> None:
    source = next(item for item in OFFICIAL_SOURCES if item.key == "set_etf_search")
    assert source.reference_scope == "listed_companies_subset"


def test_tpex_source_is_modeled_as_partial_official_coverage() -> None:
    source = next(item for item in OFFICIAL_SOURCES if item.key == "tpex_mainboard_daily_quotes")
    assert source.reference_scope == "listed_companies_subset"


def test_tpex_etf_filter_source_is_modeled_as_partial_official_coverage() -> None:
    source = next(item for item in OFFICIAL_SOURCES if item.key == "tpex_etf_filter")
    assert source.reference_scope == "listed_companies_subset"


def test_parse_tpex_mainboard_quotes_maps_tpex_rows():
    payload = [
        {"SecuritiesCompanyCode": "006201", "CompanyName": "元大富櫃50"},
        {"SecuritiesCompanyCode": "6488", "CompanyName": "環球晶圓股份有限公司"},
        {"SecuritiesCompanyCode": "ABC123", "CompanyName": "Skip Me"},
        {"SecuritiesCompanyCode": "", "CompanyName": "Ignored"},
    ]

    rows = parse_tpex_mainboard_quotes(payload, SOURCE)

    assert rows == [
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "006201",
            "name": "元大富櫃50",
            "exchange": "TPEX",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "6488",
            "name": "環球晶圓股份有限公司",
            "exchange": "TPEX",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
    ]


def test_parse_tpex_etf_filter_maps_tpex_etf_rows() -> None:
    payload = {
        "status": "success",
        "data": [
            {
                "stockNo": "006201",
                "stockName": "Yuanta/P-shares Taiwan GreTai 50 ETF",
                "listingDate": "2011/09/20",
                "issuer": "Yuanta Securities Investment Trust",
            },
            {
                "stockNo": "00679B",
                "stockName": "Yuanta U.S. Treasury 20+ Year Bond ETF",
                "listingDate": "2017/03/31",
                "issuer": "Yuanta Securities Investment Trust",
            },
            {
                "stockNo": "ABC123",
                "stockName": "Skip Me",
            },
            {
                "stockNo": "",
                "stockName": "Ignored",
            },
        ],
    }

    rows = parse_tpex_etf_filter(payload, SOURCE)

    assert rows == [
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "006201",
            "name": "Yuanta/P-shares Taiwan GreTai 50 ETF",
            "exchange": "TPEX",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "00679B",
            "name": "Yuanta U.S. Treasury 20+ Year Bond ETF",
            "exchange": "TPEX",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
    ]


def test_load_tpex_mainboard_quotes_payload_prefers_cache(tmp_path, monkeypatch):
    cache_path = tmp_path / "tpex_mainboard_daily_close_quotes.json"
    cache_path.write_text('[{"SecuritiesCompanyCode":"6488","CompanyName":"環球晶圓股份有限公司"}]', encoding="utf-8")
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.TPEX_MAINBOARD_QUOTES_CACHE", cache_path)
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.LEGACY_TPEX_MAINBOARD_QUOTES_CACHE", tmp_path / "legacy.json")

    payload, mode = load_tpex_mainboard_quotes_payload()

    assert mode == "cache"
    assert payload == [{"SecuritiesCompanyCode": "6488", "CompanyName": "環球晶圓股份有限公司"}]


def test_load_tpex_etf_filter_payload_prefers_cache(tmp_path, monkeypatch):
    cache_path = tmp_path / "tpex_etf_filter.json"
    cache_path.write_text(
        '{"status":"success","data":[{"stockNo":"00679B","stockName":"Yuanta U.S. Treasury 20+ Year Bond ETF"}]}',
        encoding="utf-8",
    )
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.TPEX_ETF_FILTER_CACHE", cache_path)
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.LEGACY_TPEX_ETF_FILTER_CACHE", tmp_path / "legacy.json")

    payload, mode = load_tpex_etf_filter_payload()

    assert mode == "cache"
    assert payload == {
        "status": "success",
        "data": [
            {
                "stockNo": "00679B",
                "stockName": "Yuanta U.S. Treasury 20+ Year Bond ETF",
            }
        ],
    }


def test_parse_asx_listed_companies_skips_banner_lines():
    text = "\n".join(
        [
            "ASX listed companies as at Thu Apr 02 19:05:21 AEDT 2026",
            "",
            "Company name,ASX code,GICS industry group",
            "\"1414 DEGREES LIMITED\",\"14D\",\"Capital Goods\"",
            "\"SPDR S&P/ASX 200 FUND\",\"STW\",\"Not Applic\"",
        ]
    )

    rows = parse_asx_listed_companies(text, SOURCE)

    assert rows == [
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "14D",
            "name": "1414 DEGREES LIMITED",
            "exchange": "ASX",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "STW",
            "name": "SPDR S&P/ASX 200 FUND",
            "exchange": "ASX",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
    ]


def test_extract_latest_asx_investment_products_url_prefers_newest_month() -> None:
    html = """
    <a href="/content/dam/asx/issuers/asx-investment-products-reports/2025/excel/asx-investment-products-dec-2025-abs.xlsx">Dec</a>
    <a href="/content/dam/asx/issuers/asx-investment-products-reports/2026/excel/asx-investment-products-jan-2026-abs.xlsx">Jan</a>
    <a href="/content/dam/asx/issuers/asx-investment-products-reports/2026/excel/asx-investment-products-feb-2026-abs.xlsx">Feb</a>
    """

    url = extract_latest_asx_investment_products_url(html)

    assert url == (
        "https://www.asx.com.au/content/dam/asx/issuers/asx-investment-products-reports/"
        "2026/excel/asx-investment-products-feb-2026-abs.xlsx"
    )


def test_parse_asx_investment_products_excel_maps_etf_and_sp_rows() -> None:
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        pd.DataFrame([[None] * 6 for _ in range(9)]).to_excel(
            writer,
            sheet_name="Spotlight ETP List",
            header=False,
            index=False,
        )
        pd.DataFrame(
            [
                {"ASX \nCode": "Equity - Australia", "Type": None, "Issuer": None, "Fund Name": None},
                {
                    "ASX \nCode": "A200",
                    "Type": "ETF",
                    "Issuer": "Betashares",
                    "Fund Name": "Betashares Australia 200 ETF",
                },
                {
                    "ASX \nCode": "GOLD",
                    "Type": "SP",
                    "Issuer": "Global X",
                    "Fund Name": "Global X Physical Gold",
                },
                {
                    "ASX \nCode": "DACE",
                    "Type": "Active",
                    "Issuer": "Dimensional",
                    "Fund Name": "Dimensional Australian Core Equity Trust - Active ETF",
                },
                {
                    "ASX \nCode": "ALFA",
                    "Type": "Complex",
                    "Issuer": "VanEck",
                    "Fund Name": "VanEck Australian Long Short Complex ETF",
                },
                {
                    "ASX \nCode": "XJOAI",
                    "Type": "Index",
                    "Issuer": None,
                    "Fund Name": "S&P/ASX 200 Accumulation",
                },
            ]
        ).to_excel(writer, sheet_name="Spotlight ETP List", startrow=9, index=False)

    rows = parse_asx_investment_products_excel(buffer.getvalue(), SOURCE)

    assert rows == [
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "A200",
            "name": "Betashares Australia 200 ETF",
            "exchange": "ASX",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "GOLD",
            "name": "Global X Physical Gold",
            "exchange": "ASX",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "DACE",
            "name": "Dimensional Australian Core Equity Trust - Active ETF",
            "exchange": "ASX",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "ALFA",
            "name": "VanEck Australian Long Short Complex ETF",
            "exchange": "ASX",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
    ]


def test_parse_set_listed_companies_html_keeps_set_and_mai_markets() -> None:
    text = """
    <table>
      <tr><td colspan="6">List of Listed Companies & Contact Information</td></tr>
      <tr>
        <td><strong>Symbol</strong></td>
        <td><strong>Company</strong></td>
        <td><strong>Market</strong></td>
        <td><strong>Industry</strong></td>
        <td><strong>Sector</strong></td>
        <td><strong>Website</strong></td>
      </tr>
      <tr>
        <td>ADVANC</td>
        <td>ADVANCED INFO SERVICE PUBLIC COMPANY LIMITED</td>
        <td>SET</td>
        <td>Technology</td>
        <td>Information & Communication Technology</td>
        <td>www.ais.th</td>
      </tr>
      <tr>
        <td>AIMIRT</td>
        <td>AIM INDUSTRIAL GROWTH FREEHOLD AND LEASEHOLD REAL ESTATE INVESTMENT TRUST</td>
        <td>SET</td>
        <td>Property & Construction</td>
        <td>Property Fund & REITs</td>
        <td>www.example.com</td>
      </tr>
      <tr>
        <td>ABFTH</td>
        <td>The ABF Thailand Bond Index Fund</td>
        <td>mai</td>
        <td>Funds</td>
        <td>ETF</td>
        <td>www.example.com</td>
      </tr>
    </table>
    """

    rows = parse_set_listed_companies_html(text, SOURCE)

    assert rows == [
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "ADVANC",
            "name": "ADVANCED INFO SERVICE PUBLIC COMPANY LIMITED",
            "exchange": "SET",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "AIMIRT",
            "name": "AIM INDUSTRIAL GROWTH FREEHOLD AND LEASEHOLD REAL ESTATE INVESTMENT TRUST",
            "exchange": "SET",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "ABFTH",
            "name": "The ABF Thailand Bond Index Fund",
            "exchange": "SET",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
    ]


def test_parse_set_quote_search_payload_keeps_only_etfs() -> None:
    text = """
    <script>
    window.__NUXT__=(function(a,b,c,d,e,f,g,h,i,j,k,l,m,n){return {state:{quote:{searchOption:[
      {symbol:a,nameTH:b,nameEN:c,market:d,securityType:e,typeSequence:f,industry:g,sector:h,querySector:h,isIFF:!1,isForeignListing:!1,remark:"",name:a,value:a,securityTypeName:i},
      {symbol:j,nameTH:k,nameEN:l,market:d,securityType:m,typeSequence:n,industry:g,sector:h,querySector:h,isIFF:!1,isForeignListing:!1,remark:"",name:j,value:j,securityTypeName:"Stock"},
      {symbol:"XETF",nameTH:"Skip",nameEN:"Skip ETF",market:"OTC",securityType:"L",typeSequence:7,industry:"",sector:"",querySector:"",isIFF:!1,isForeignListing:!1,remark:"",name:"XETF",value:"XETF",securityTypeName:"ETF"}
    ],dropdownAdditional:[]}}}})("ABFTH","กองทุนเอบีเอฟ","THE ABF THAILAND BOND INDEX FUND","SET","L",7,"","","ETF","ADVANC","แอดวานซ์","ADVANCED INFO SERVICE PUBLIC COMPANY LIMITED","S",1);
    </script>
    """

    rows = parse_set_quote_search_payload(text, SOURCE)

    assert rows == [
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "ABFTH",
            "name": "THE ABF THAILAND BOND INDEX FUND",
            "exchange": "SET",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
    ]


def test_parse_lse_company_reports_html_maps_lse_rows():
    html = """
    <html>
      <body>
        <table>
          <tr><th>Code</th><th>Name</th></tr>
          <tr><td>ABF</td><td>ASSOCIATED BRITISH FOODS PLC ORD 5 15/22P</td></tr>
          <tr><td>VUSA</td><td>VANGUARD S&P 500 UCITS ETF USD</td></tr>
        </table>
      </body>
    </html>
    """

    rows = parse_lse_company_reports_html(html, SOURCE)

    assert rows == [
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "ABF",
            "name": "ASSOCIATED BRITISH FOODS PLC ORD 5 15/22P",
            "exchange": "LSE",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "VUSA",
            "name": "VANGUARD S&P 500 UCITS ETF USD",
            "exchange": "LSE",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
    ]


def test_parse_cboe_canada_listing_directory_html_maps_supported_security_types() -> None:
    html = """
    <script>
    CTX['listingDirectory'] = [
      {"symbol": "ABXX", "name": "ABXX CORP.", "security": "equity"},
      {"symbol": "BYLD.B", "name": "BMO YLD ETF", "security": "etf"},
      {"symbol": "ABCD", "name": "ABC DR", "security": "dr"},
      {"symbol": "FUND", "name": "Closed End Fund", "security": "cef"},
      {"symbol": "WARR", "name": "Ignored Warrant", "security": "warrant"},
      {"symbol": "DEBT", "name": "Ignored Debt", "security": "debt"}
    ];
    </script>
    """

    rows = parse_cboe_canada_listing_directory_html(html, SOURCE)

    assert rows == [
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "ABXX",
            "name": "ABXX CORP.",
            "exchange": "NEO",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "BYLD.B",
            "name": "BMO YLD ETF",
            "exchange": "NEO",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "BYLD-B",
            "name": "BMO YLD ETF",
            "exchange": "NEO",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "ABCD",
            "name": "ABC DR",
            "exchange": "NEO",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "FUND",
            "name": "Closed End Fund",
            "exchange": "NEO",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
    ]


def test_fetch_lse_company_reports_paginates_until_empty(monkeypatch):
    source = MasterfileSource(
        key="lse",
        provider="LSE",
        description="LSE company reports",
        source_url="https://example.com?initial={initial}&page={page}",
        format="lse_company_reports_html",
        reference_scope="listed_companies_subset",
    )
    requested_urls: list[str] = []
    first_page = """
    <table>
      <tr><th>Code</th><th>Name</th></tr>
      <tr><td>ABF</td><td>ASSOCIATED BRITISH FOODS PLC ORD 5 15/22P</td></tr>
    </table>
    <a href="?initial=A&page=2">Next</a>
    """
    second_page = """
    <table>
      <tr><th>Code</th><th>Name</th></tr>
      <tr><td>ABDN</td><td>ABERDEEN GROUP PLC ORD 13 61/63P</td></tr>
    </table>
    <a href="?initial=A&page=3">Next</a>
    """
    empty_page = "<html><body>No table</body></html>"

    def fake_fetch_text(url: str, session=None) -> str:
        requested_urls.append(url)
        if url.endswith("initial=A&page=1"):
            return first_page
        if url.endswith("initial=A&page=2"):
            return second_page
        return empty_page

    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.fetch_text", fake_fetch_text)
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.LSE_PAGE_INITIALS", ("A",))

    rows = fetch_lse_company_reports(source)

    assert requested_urls == [
        "https://example.com?initial=A&page=1",
        "https://example.com?initial=A&page=2",
        "https://example.com?initial=A&page=3",
    ]
    assert [row["ticker"] for row in rows] == ["ABF", "ABDN"]
    assert all(row["exchange"] == "LSE" for row in rows)


def test_fetch_lse_company_reports_uses_zero_initial_for_numeric_bucket():
    assert LSE_PAGE_INITIALS[-1] == "0"


def test_load_lse_company_reports_rows_prefers_cache(tmp_path, monkeypatch):
    cache_path = tmp_path / "lse_company_reports.json"
    cache_path.write_text('[{"ticker":"ABF","name":"ASSOCIATED BRITISH FOODS PLC ORD 5 15/22P"}]', encoding="utf-8")
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.LSE_COMPANY_REPORTS_CACHE", cache_path)
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.LEGACY_LSE_COMPANY_REPORTS_CACHE", tmp_path / "legacy.json")

    source = MasterfileSource(
        key="lse",
        provider="LSE",
        description="LSE company reports",
        source_url="https://example.com?initial={initial}&page={page}",
        format="lse_company_reports_html",
        reference_scope="listed_companies_subset",
    )

    rows, mode = load_lse_company_reports_rows(source)

    assert mode == "cache"


def test_extract_lse_last_page_uses_paginator_links():
    text = """
    <div class="paginator">
      <a title="Page 1241" class="page-number">1241</a>
      <a title="Page 1242" class="page-number">1242</a>
      <a title="Page 1250" class="page-number active">1250</a>
    </div>
    """

    assert extract_lse_last_page(text) == 1250


def test_fetch_lse_instrument_directory_paginates_and_filters_to_target_tickers(monkeypatch):
    source = MasterfileSource(
        key="lse_directory",
        provider="LSE",
        description="LSE instrument directory",
        source_url="https://example.com?page={page}",
        format="lse_instrument_directory_html",
        reference_scope="security_lookup_subset",
    )
    requested_urls: list[str] = []
    first_page = """
    <table>
      <tr><th>Code</th><th>Name</th></tr>
      <tr><td>ABF</td><td>ASSOCIATED BRITISH FOODS PLC ORD 5 15/22P</td></tr>
      <tr><td>VUSA</td><td>VANGUARD S&P 500 UCITS ETF USD</td></tr>
    </table>
    <div class="paginator">
      <a title="Page 1" class="page-number active">1</a>
      <a title="Page 2" class="page-number">2</a>
    </div>
    """
    second_page = """
    <table>
      <tr><th>Code</th><th>Name</th></tr>
      <tr><td>PHGP</td><td>WISDOMTREE METAL SECURITIES WISDOMTREE PHYSICAL GOLD £</td></tr>
      <tr><td>IGN</td><td>IGNORE ME PLC</td></tr>
    </table>
    """

    def fake_fetch_text(url: str, session=None) -> str:
        requested_urls.append(url)
        if url.endswith("page=1"):
            return first_page
        return second_page

    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.fetch_text", fake_fetch_text)

    rows = fetch_lse_instrument_directory(
        source,
        target_tickers={"ABF", "PHGP"},
        asset_type_by_ticker={"ABF": "Stock", "PHGP": "ETF"},
    )

    assert requested_urls == ["https://example.com?page=1", "https://example.com?page=2"]
    assert [(row["ticker"], row["asset_type"]) for row in rows] == [("ABF", "Stock"), ("PHGP", "ETF")]


def test_load_lse_instrument_directory_rows_prefers_cache(tmp_path, monkeypatch):
    cache_path = tmp_path / "lse_instrument_directory.json"
    cache_path.write_text('[{"ticker":"ABF","name":"ASSOCIATED BRITISH FOODS PLC ORD 5 15/22P"}]', encoding="utf-8")
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.LSE_INSTRUMENT_DIRECTORY_CACHE", cache_path)
    monkeypatch.setattr(
        "scripts.fetch_exchange_masterfiles.LEGACY_LSE_INSTRUMENT_DIRECTORY_CACHE",
        tmp_path / "legacy.json",
    )

    source = MasterfileSource(
        key="lse_directory",
        provider="LSE",
        description="LSE instrument directory",
        source_url="https://example.com?page={page}",
        format="lse_instrument_directory_html",
        reference_scope="security_lookup_subset",
    )

    rows, mode = load_lse_instrument_directory_rows(source)

    assert rows == [{"ticker": "ABF", "name": "ASSOCIATED BRITISH FOODS PLC ORD 5 15/22P"}]
    assert mode == "cache"


def test_fetch_lse_instrument_search_exact_filters_to_exact_ticker(monkeypatch):
    source = MasterfileSource(
        key="lse_lookup",
        provider="LSE",
        description="LSE instrument search",
        source_url="https://example.com?codeName={ticker}",
        format="lse_instrument_search_html",
        reference_scope="security_lookup_subset",
    )

    def fake_fetch_text(url: str, session=None) -> str:
        if "PHGP" in url:
            return """
            <table>
              <tr><th>Code</th><th>Name</th></tr>
              <tr><td>PHGP</td><td><a href="javascript: UpdateOpener('WISDOMTREE METAL SECURITIES WISDOMTREE PHYSICAL GOLD £', 'JE00B1VS3770|ZZ|GBX|ETC2|B285Z72|PHGP');">WISDOMTREE METAL SECURITIES WISDOMTREE PHYSICAL GOLD £</a></td></tr>
              <tr><td>UC86</td><td><a href="javascript: UpdateOpener('UBS ETF USD', 'LU1048314949|ZZ|USD|ETF2|BMPHGP6|UC86');">UBS ETF USD</a></td></tr>
            </table>
            """
        return """
        <table>
          <tr><th>Code</th><th>Name</th></tr>
          <tr><td>1MSF</td><td><a href="javascript: UpdateOpener('LEVERAGE SHARES PUBLIC LIMITED CO. 1X MSFT', 'XS1234567890|IE|USD|SSX4|ABC1234|1MSF');">LEVERAGE SHARES PUBLIC LIMITED CO. 1X MSFT</a></td></tr>
        </table>
        """

    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.fetch_text", fake_fetch_text)

    rows = fetch_lse_instrument_search_exact(
        source,
        ["PHGP", "1MSF"],
        asset_type_by_ticker={"PHGP": "ETF", "1MSF": "Stock"},
    )

    assert [row["ticker"] for row in rows] == ["PHGP", "1MSF"]
    assert all(row["exchange"] == "LSE" for row in rows)
    assert all(row["source_url"].startswith("https://example.com?codeName=") for row in rows)
    assert rows[0]["asset_type"] == "ETF"
    assert rows[0]["isin"] == "JE00B1VS3770"
    assert rows[1]["asset_type"] == "Stock"
    assert rows[1]["isin"] == "XS1234567890"


def test_fetch_lse_instrument_search_exact_accepts_trailing_dot_variant(monkeypatch):
    source = MasterfileSource(
        key="lse_lookup",
        provider="LSE",
        description="LSE instrument search",
        source_url="https://example.com?codeName={ticker}",
        format="lse_instrument_search_html",
        reference_scope="security_lookup_subset",
    )

    def fake_fetch_text(url: str, session=None) -> str:
        assert "QQ" in url
        return """
        <table>
          <tr><th>Code</th><th>Name</th></tr>
          <tr><td>QQ.</td><td><a href="javascript: UpdateOpener('QINETIQ GROUP PLC ORD 1P', 'GB00B0WMWD03|GB|GBX|EQS2|B0WMWD0|QQ.');">QINETIQ GROUP PLC ORD 1P</a></td></tr>
          <tr><td>QQQ</td><td><a href="javascript: UpdateOpener('UNRELATED ETF', 'US0000000001|US|USD|ETF2|ABC1234|QQQ');">UNRELATED ETF</a></td></tr>
        </table>
        """

    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.fetch_text", fake_fetch_text)

    rows = fetch_lse_instrument_search_exact(
        source,
        ["QQ"],
        asset_type_by_ticker={"QQ": "Stock"},
    )

    assert rows == [
        {
            "source_key": "lse_lookup",
            "provider": "LSE",
            "source_url": "https://example.com?codeName=QQ",
            "ticker": "QQ",
            "name": "QINETIQ GROUP PLC ORD 1P",
            "exchange": "LSE",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "security_lookup_subset",
            "official": "true",
            "isin": "GB00B0WMWD03",
        }
    ]


def test_infer_lse_lookup_asset_type_uses_stock_fallback_for_stmm_trusts():
    assert (
        infer_lse_lookup_asset_type(
            "STMM",
            "THE GLOBAL SMALLER CO'S TRUST PLC ORD 2.5P",
            "Stock",
        )
        == "Stock"
    )
    assert infer_lse_lookup_asset_type("EUE2", "IMGP IMGP DBI MNGD FUTURES FD R USD UCITS ETF", "Stock") == "ETF"


def test_lse_instrument_search_target_tickers_selects_only_uncovered_lse_reference_gaps(tmp_path):
    listings_path = tmp_path / "listings.csv"
    listings_path.write_text(
        "\n".join(
            [
                "ticker,exchange,asset_type,name,country,country_code,isin,sector",
                "PHGP,LSE,ETF,WisdomTree Physical Gold,United Kingdom,GB,,",
                "VUSA,LSE,ETF,Vanguard S&P 500 UCITS ETF USD,United Kingdom,GB,,",
                "ABF,LSE,Stock,Associated British Foods,United Kingdom,GB,,",
                "SPY,NYSE,ETF,SPDR S&P 500 ETF Trust,United States,US,,",
            ]
        ),
        encoding="utf-8",
    )

    target_tickers = lse_instrument_search_target_tickers(
        [{"ticker": "VUSA", "exchange": "LSE"}],
        listings_path=listings_path,
        reference_gap_tickers={"PHGP", "ABF"},
    )

    assert target_tickers == ["ABF", "PHGP"]


def test_load_lse_instrument_search_rows_prefers_cache_and_only_fetches_missing(tmp_path, monkeypatch):
    listings_path = tmp_path / "listings.csv"
    listings_path.write_text(
        "\n".join(
            [
                "ticker,exchange,asset_type,name,country,country_code,isin,sector",
                "PHGP,LSE,ETF,WisdomTree Physical Gold,United Kingdom,GB,,",
                "VUSA,LSE,ETF,Vanguard S&P 500 UCITS ETF USD,United Kingdom,GB,,",
                "ABF,LSE,Stock,Associated British Foods,United Kingdom,GB,,",
            ]
        ),
        encoding="utf-8",
    )
    cache_path = tmp_path / "lse_instrument_search.json"
    cache_path.write_text(
        '{"PHGP":[{"source_key":"lse_lookup","provider":"LSE","source_url":"https://example.com?codeName=PHGP","ticker":"PHGP","name":"WISDOMTREE METAL SECURITIES WISDOMTREE PHYSICAL GOLD £","exchange":"LSE","asset_type":"ETF","listing_status":"active","reference_scope":"security_lookup_subset","official":"true"}]}',
        encoding="utf-8",
    )
    legacy_cache_path = tmp_path / "legacy.json"
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.LISTINGS_CSV", listings_path)
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.LSE_INSTRUMENT_SEARCH_MAX_WORKERS", 1)
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.LSE_INSTRUMENT_SEARCH_CACHE", cache_path)
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.LEGACY_LSE_INSTRUMENT_SEARCH_CACHE", legacy_cache_path)
    monkeypatch.setattr(
        "scripts.fetch_exchange_masterfiles.lse_reference_gap_tickers",
        lambda base_dirs=None: {"PHGP", "ABF"},
    )
    monkeypatch.setattr(
        "scripts.fetch_exchange_masterfiles.load_lse_company_reports_rows",
        lambda source, session=None: ([{"ticker": "VUSA", "exchange": "LSE"}], "cache"),
    )
    monkeypatch.setattr(
        "scripts.fetch_exchange_masterfiles.load_lse_instrument_directory_rows",
        lambda source, session=None: ([{"ticker": "PHGP", "exchange": "LSE"}], "cache"),
    )
    fetched: list[tuple[list[str], dict[str, str] | None]] = []

    def fake_fetch(source, tickers, session=None, asset_type_by_ticker=None):
        fetched.append((list(tickers), asset_type_by_ticker))
        ticker = list(tickers)[0]
        names = {
            "ABF": "ASSOCIATED BRITISH FOODS PLC ORD 5 15/22P",
            "PHGP": "WISDOMTREE METAL SECURITIES WISDOMTREE PHYSICAL GOLD £",
        }
        return [
            {
                "source_key": source.key,
                "provider": source.provider,
                "source_url": f"https://example.com?codeName={ticker}",
                "ticker": ticker,
                "name": names[ticker],
                "exchange": "LSE",
                "asset_type": asset_type_by_ticker[ticker],
                "listing_status": "active",
                "reference_scope": source.reference_scope,
                "official": "true",
                "isin": f"ISIN-{ticker}",
            }
        ]

    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.fetch_lse_instrument_search_exact", fake_fetch)
    source = MasterfileSource(
        key="lse_lookup",
        provider="LSE",
        description="LSE instrument search",
        source_url="https://example.com?codeName={ticker}",
        format="lse_instrument_search_html",
        reference_scope="security_lookup_subset",
    )

    rows, mode = load_lse_instrument_search_rows(source)

    assert mode == "network"
    assert [item[0] for item in fetched] == [["ABF"], ["PHGP"]]
    assert fetched[0][1] == {"PHGP": "ETF", "VUSA": "ETF", "ABF": "Stock"}
    assert [(row["ticker"], row["asset_type"], row.get("isin", "")) for row in rows] == [("ABF", "Stock", "ISIN-ABF"), ("PHGP", "ETF", "ISIN-PHGP")]

    monkeypatch.setattr(
        "scripts.fetch_exchange_masterfiles.fetch_lse_instrument_search_exact",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("cache should be used")),
    )
    rows, mode = load_lse_instrument_search_rows(source)

    assert mode == "cache"
    assert [(row["ticker"], row["asset_type"], row.get("isin", "")) for row in rows] == [("ABF", "Stock", "ISIN-ABF"), ("PHGP", "ETF", "ISIN-PHGP")]


def test_load_lse_instrument_search_rows_retains_cached_rows_outside_current_target_set(tmp_path, monkeypatch):
    listings_path = tmp_path / "listings.csv"
    listings_path.write_text(
        "\n".join(
            [
                "ticker,exchange,asset_type,name,country,country_code,isin,sector",
                "PHGP,LSE,ETF,WisdomTree Physical Gold,United Kingdom,GB,,",
            ]
        ),
        encoding="utf-8",
    )
    cache_path = tmp_path / "lse_instrument_search.json"
    cache_path.write_text(
        '{"OLD1":[{"source_key":"lse_lookup","provider":"LSE","source_url":"https://example.com?codeName=OLD1","ticker":"OLD1","name":"Legacy cached row","exchange":"LSE","asset_type":"ETF","listing_status":"active","reference_scope":"security_lookup_subset","official":"true"}]}',
        encoding="utf-8",
    )
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.LISTINGS_CSV", listings_path)
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.LSE_INSTRUMENT_SEARCH_MAX_WORKERS", 1)
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.LSE_INSTRUMENT_SEARCH_CACHE", cache_path)
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.LEGACY_LSE_INSTRUMENT_SEARCH_CACHE", tmp_path / "legacy.json")
    monkeypatch.setattr(
        "scripts.fetch_exchange_masterfiles.lse_reference_gap_tickers",
        lambda base_dirs=None: {"PHGP"},
    )
    monkeypatch.setattr(
        "scripts.fetch_exchange_masterfiles.load_lse_company_reports_rows",
        lambda source, session=None: ([], "cache"),
    )
    monkeypatch.setattr(
        "scripts.fetch_exchange_masterfiles.load_lse_instrument_directory_rows",
        lambda source, session=None: ([{"ticker": "PHGP", "exchange": "LSE"}], "cache"),
    )
    monkeypatch.setattr(
        "scripts.fetch_exchange_masterfiles.fetch_lse_instrument_search_exact",
        lambda source, tickers, session=None, asset_type_by_ticker=None: [
            {
                "source_key": source.key,
                "provider": source.provider,
                "source_url": "https://example.com?codeName=PHGP",
                "ticker": "PHGP",
                "name": "WISDOMTREE METAL SECURITIES WISDOMTREE PHYSICAL GOLD £",
                "exchange": "LSE",
                "asset_type": "ETF",
                "listing_status": "active",
                "reference_scope": source.reference_scope,
                "official": "true",
            }
        ],
    )

    source = MasterfileSource(
        key="lse_lookup",
        provider="LSE",
        description="LSE instrument search",
        source_url="https://example.com?codeName={ticker}",
        format="lse_instrument_search_html",
        reference_scope="security_lookup_subset",
    )

    rows, mode = load_lse_instrument_search_rows(source)

    assert mode == "network"
    assert [(row["ticker"], row["asset_type"]) for row in rows] == [("OLD1", "ETF"), ("PHGP", "ETF")]


def test_parse_krx_listed_companies_maps_market_rows():
    payload = [
        {
            "isu_cd": "005930",
            "eng_cor_nm": "SAMSUNG ELECTRONICS",
        },
        {
            "isu_cd": "091990",
            "eng_cor_nm": "CELLTRIONHEALTHCARE",
        },
        {
            "isu_cd": "",
            "eng_cor_nm": "Ignored",
        },
    ]

    rows = parse_krx_listed_companies(payload, SOURCE, exchange="KRX")

    assert rows == [
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "005930",
            "name": "SAMSUNG ELECTRONICS",
            "exchange": "KRX",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "091990",
            "name": "CELLTRIONHEALTHCARE",
            "exchange": "KRX",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
    ]


def test_parse_krx_etf_finder_maps_rows():
    payload = {
        "block1": [
            {
                "short_code": "451060",
                "codeName": "1Q 200액티브",
            },
            {
                "short_code": "",
                "codeName": "Ignored",
            },
        ]
    }

    rows = parse_krx_etf_finder(payload, SOURCE)

    assert rows == [
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "451060",
            "name": "1Q 200액티브",
            "exchange": "KRX",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        }
    ]


def test_parse_krx_stock_finder_records_maps_rows():
    payload = [
        {
            "short_code": "00279K",
            "codeName": "아모레퍼시픽그룹1우",
            "marketEngName": "KOSPI",
            "full_code": "KR700279K016",
        }
    ]

    rows = parse_krx_stock_finder_records(payload, SOURCE)

    assert rows == [
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "00279K",
            "name": "아모레퍼시픽그룹1우",
            "exchange": "KRX",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "KR700279K016",
        }
    ]


def test_parse_krx_product_finder_records_maps_rows():
    payload = [
        {
            "short_code": "448100",
            "codeName": "ACE 테슬라밸류체인액티브",
            "full_code": "KR7448100001",
        }
    ]

    rows = parse_krx_product_finder_records(payload, SOURCE)

    assert rows == [
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "448100",
            "name": "ACE 테슬라밸류체인액티브",
            "exchange": "KRX",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "KR7448100001",
        }
    ]


def test_fetch_krx_listed_companies_fetches_kospi_kosdaq_and_konex(monkeypatch):
    source = MasterfileSource(
        key="krx",
        provider="KRX",
        description="KRX listed companies",
        source_url="https://example.com/krx",
        format="krx_listed_companies_json",
    )

    class FakeResponse:
        def __init__(self, text="", payload=None):
            self.text = text
            self._payload = payload

        def raise_for_status(self):
            return None

        def json(self):
            return self._payload

    class FakeSession:
        def __init__(self):
            self.headers = {}
            self.get_calls = []
            self.post_calls = []

        def get(self, url, **kwargs):
            self.get_calls.append((url, kwargs))
            if "GenerateOTP.jspx" in url:
                otp_calls = len([call for call in self.get_calls if "GenerateOTP.jspx" in call[0]])
                return FakeResponse(text=f"otp-{otp_calls}")
            return FakeResponse(text="<table><tr><td>ok</td></tr></table>")

        def post(self, url, data=None, **kwargs):
            self.post_calls.append((url, data, kwargs))
            market = data["market_gubun"]
            if market == "1":
                payload = {"block1": [{"isu_cd": "005930", "eng_cor_nm": "SAMSUNG ELECTRONICS"}]}
            elif market == "2":
                payload = {"block1": [{"isu_cd": "091990", "eng_cor_nm": "CELLTRIONHEALTHCARE"}]}
            else:
                payload = {"block1": [{"isu_cd": "092590", "eng_cor_nm": "Luxpia"}]}
            return FakeResponse(payload=payload)

    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.pd.read_html", lambda *_args, **_kwargs: [object()])
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.latest_reference_gap_tickers", lambda *_args, **_kwargs: set())
    session = FakeSession()
    rows = fetch_krx_listed_companies(source, session=session)

    assert [row["ticker"] for row in rows] == ["005930", "091990", "092590"]
    assert [row["exchange"] for row in rows] == ["KRX", "KOSDAQ", "KRX"]


def test_fetch_krx_listed_companies_supplements_target_gaps_from_finder(monkeypatch):
    source = MasterfileSource(
        key="krx",
        provider="KRX",
        description="KRX listed companies",
        source_url="https://example.com/krx",
        format="krx_listed_companies_json",
    )

    class FakeResponse:
        def __init__(self, text="", payload=None):
            self.text = text
            self._payload = payload

        def raise_for_status(self):
            return None

        def json(self):
            return self._payload

    class FakeSession:
        def __init__(self):
            self.headers = {}

        def get(self, url, **kwargs):
            if "GenerateOTP.jspx" in url:
                return FakeResponse(text="otp")
            return FakeResponse(text="<table><tr><td>ok</td></tr></table>")

        def post(self, url, data=None, **kwargs):
            return FakeResponse(payload={"block1": []})

    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.pd.read_html", lambda *_args, **_kwargs: [object()])
    monkeypatch.setattr(
        "scripts.fetch_exchange_masterfiles.latest_reference_gap_tickers",
        lambda *_args, **_kwargs: {"00279K"},
    )
    monkeypatch.setattr(
        "scripts.fetch_exchange_masterfiles.fetch_krx_finder_records",
        lambda *args, **kwargs: [
            {
                "short_code": "00279K",
                "codeName": "아모레퍼시픽그룹1우",
                "marketEngName": "KOSPI",
                "full_code": "KR700279K016",
            }
        ],
    )

    rows = fetch_krx_listed_companies(source, session=FakeSession())

    assert rows == [
        {
            "source_key": "krx",
            "provider": "KRX",
            "source_url": "https://example.com/krx",
            "ticker": "00279K",
            "name": "아모레퍼시픽그룹1우",
            "exchange": "KRX",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "KR700279K016",
        }
    ]


def test_fetch_krx_etf_finder_posts_finder_request(monkeypatch):
    source = MasterfileSource(
        key="krx_etf_finder",
        provider="KRX",
        description="KRX ETF finder",
        source_url="https://example.com/krx-etf",
        format="krx_etf_finder_json",
    )

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"block1": [{"short_code": "451060", "codeName": "1Q 200액티브"}]}

    class FakeSession:
        def __init__(self):
            self.calls = []

        def post(self, url, data=None, headers=None, timeout=None):
            self.calls.append((url, data, headers, timeout))
            return FakeResponse()

    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.latest_reference_gap_tickers", lambda *_args, **_kwargs: set())
    session = FakeSession()
    rows = fetch_krx_etf_finder(source, session=session)

    assert rows[0]["ticker"] == "451060"
    assert rows[0]["asset_type"] == "ETF"
    assert session.calls == [
        (
            "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd",
            {
                "bld": "dbms/comm/finder/finder_secuprodisu",
                "mktsel": "ETF",
                "searchText": "",
            },
            {
                "User-Agent": "free-ticker-database/2.0 (+https://github.com/adanos-software/free-ticker-database)",
                "Referer": "https://data.krx.co.kr/contents/MDC/MAIN/main/index.cmd",
            },
            30.0,
        )
    ]


def test_fetch_krx_etf_finder_replaces_target_gap_with_exact_product_finder_row(monkeypatch):
    source = MasterfileSource(
        key="krx_etf_finder",
        provider="KRX",
        description="KRX ETF finder",
        source_url="https://example.com/krx-etf",
        format="krx_etf_finder_json",
    )

    monkeypatch.setattr(
        "scripts.fetch_exchange_masterfiles.latest_reference_gap_tickers",
        lambda *_args, **_kwargs: {"448100"},
    )

    def fake_fetch_krx_finder_records(bld, *, mktsel="ALL", search_text="", session=None):
        assert bld == "dbms/comm/finder/finder_secuprodisu"
        if mktsel == "ETF":
            return [{"short_code": "448100", "codeName": "ACE Tesla Value Chain Active"}]
        return [{"short_code": "448100", "codeName": "ACE 테슬라밸류체인액티브", "full_code": "KR7448100001"}]

    monkeypatch.setattr(
        "scripts.fetch_exchange_masterfiles.fetch_krx_finder_records",
        fake_fetch_krx_finder_records,
    )

    rows = fetch_krx_etf_finder(source, session=object())

    assert rows == [
        {
            "source_key": "krx_etf_finder",
            "provider": "KRX",
            "source_url": "https://example.com/krx-etf",
            "ticker": "448100",
            "name": "ACE 테슬라밸류체인액티브",
            "exchange": "KRX",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "KR7448100001",
        }
    ]


def test_krx_source_is_modeled_as_partial_official_coverage() -> None:
    source = next(item for item in OFFICIAL_SOURCES if item.key == "krx_listed_companies")
    assert source.reference_scope == "listed_companies_subset"


def test_krx_etf_finder_source_is_modeled_as_partial_official_coverage() -> None:
    source = next(item for item in OFFICIAL_SOURCES if item.key == "krx_etf_finder")
    assert source.reference_scope == "listed_companies_subset"


def test_extract_psx_xid_and_sector_options() -> None:
    html = """
    <html>
      <body>
        <input type="hidden" id="XID" value="abc123" />
        <select name="sector">
          <option value="">Select</option>
          <option value="0801">Automobile Assembler</option>
          <option value="0837">Exchange Traded Funds</option>
        </select>
      </body>
    </html>
    """

    assert extract_psx_xid(html) == "abc123"
    assert extract_psx_sector_options(html) == [
        ("0801", "Automobile Assembler"),
        ("0837", "Exchange Traded Funds"),
    ]


def test_parse_psx_listed_companies_maps_stock_and_etf_rows() -> None:
    stock_rows = parse_psx_listed_companies(
        [{"symbol_code": "AGTL", "company_name": "AL-Ghazi Tractors"}],
        SOURCE,
        sector_label="Automobile Assembler",
    )
    etf_rows = parse_psx_listed_companies(
        [{"symbol_code": "MIIETF", "company_name": "MII ETF"}],
        SOURCE,
        sector_label="Exchange Traded Funds",
    )

    assert stock_rows[0]["exchange"] == "PSX"
    assert stock_rows[0]["asset_type"] == "Stock"
    assert etf_rows[0]["asset_type"] == "ETF"


def test_fetch_psx_listed_companies_uses_ajax_sector_lookup() -> None:
    source = MasterfileSource(
        key="psx_listed_companies",
        provider="PSX",
        description="PSX listed companies",
        source_url="https://example.com/psx",
        format="psx_listed_companies_json",
        reference_scope="listed_companies_subset",
    )

    class FakeResponse:
        def __init__(self, text: str):
            self.text = text

        def raise_for_status(self):
            return None

    class FakeSession:
        def __init__(self):
            self.calls = []

        def get(self, url, params=None, headers=None, timeout=None):
            self.calls.append((url, params, headers, timeout))
            if url == "https://example.com/psx":
                return FakeResponse(
                    """
                    <input type="hidden" id="XID" value="token-1" />
                    <select name="sector">
                      <option value="0801">Automobile Assembler</option>
                      <option value="0837">Exchange Traded Funds</option>
                      <option value="36">Bonds</option>
                      <option value="0806">Close-End Mutual Fund</option>
                    </select>
                    """
                )
            if params["sector"] == "0801":
                return FakeResponse('[{"symbol_code":"AGTL","company_name":"AL-Ghazi Tractors"}]')
            if params["sector"] == "0837":
                return FakeResponse('[{"symbol_code":"MIIETF","company_name":"MII ETF"}]')
            raise AssertionError(f"Unexpected request: {url} {params}")

    session = FakeSession()
    rows = fetch_psx_listed_companies(source, session=session)

    assert [(row["ticker"], row["asset_type"]) for row in rows] == [
        ("AGTL", "Stock"),
        ("MIIETF", "ETF"),
    ]
    assert session.calls == [
        (
            "https://example.com/psx",
            None,
            {
                "User-Agent": "free-ticker-database/2.0 (+https://github.com/adanos-software/free-ticker-database)",
                "Referer": "https://www.psx.com.pk/psx/resources-and-tools/listings/listed-companies",
                "Origin": "https://www.psx.com.pk",
                "Connection": "close",
            },
            30.0,
        ),
        (
            "https://www.psx.com.pk/psx/custom-templates/companiesSearch-sector",
            {"sector": "0801", "XID": "token-1"},
            {
                "User-Agent": "free-ticker-database/2.0 (+https://github.com/adanos-software/free-ticker-database)",
                "Referer": "https://www.psx.com.pk/psx/resources-and-tools/listings/listed-companies",
                "Origin": "https://www.psx.com.pk",
                "Connection": "close",
                "Accept": "application/json,text/plain,*/*",
                "X-Requested-With": "XMLHttpRequest",
            },
            30.0,
        ),
        (
            "https://www.psx.com.pk/psx/custom-templates/companiesSearch-sector",
            {"sector": "0837", "XID": "token-1"},
            {
                "User-Agent": "free-ticker-database/2.0 (+https://github.com/adanos-software/free-ticker-database)",
                "Referer": "https://www.psx.com.pk/psx/resources-and-tools/listings/listed-companies",
                "Origin": "https://www.psx.com.pk",
                "Connection": "close",
                "Accept": "application/json,text/plain,*/*",
                "X-Requested-With": "XMLHttpRequest",
            },
            30.0,
        ),
    ]


def test_extract_psx_symbol_name_download_url_finds_zip_link() -> None:
    html = """
    <div class="panel-body">
      <a href="/download/symbol_name/2026-04-08.zip">Download</a>
    </div>
    """

    assert (
        extract_psx_symbol_name_download_url(html)
        == "https://dps.psx.com.pk/download/symbol_name/2026-04-08.zip"
    )


def test_parse_psx_symbol_name_daily_prefers_full_name_and_filters_to_known_tickers() -> None:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(
            "symbolname.lis",
            (
                "AGTL|Al-Ghazi Tr.|Al-Ghazi Tractors Limited|\n"
                "MIIETF|MII ETF|MII ETF|\n"
                "SKIP|Skip Corp|Skip Corp|\n"
            ).encode("utf-16"),
        )

    rows = parse_psx_symbol_name_daily(
        buffer.getvalue(),
        SOURCE,
        asset_type_by_ticker={"AGTL": "Stock", "MIIETF": "ETF"},
    )

    assert [(row["ticker"], row["name"], row["asset_type"]) for row in rows] == [
        ("AGTL", "Al-Ghazi Tractors Limited", "Stock"),
        ("MIIETF", "MII ETF", "ETF"),
    ]


def test_fetch_psx_symbol_name_daily_downloads_latest_available_symbol_file(monkeypatch) -> None:
    source = MasterfileSource(
        key="psx_symbol_name_daily",
        provider="PSX",
        description="PSX symbol names",
        source_url="https://dps.psx.com.pk/daily-downloads",
        format="psx_symbol_name_daily_zip",
        reference_scope="listed_companies_subset",
    )

    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "load_csv",
        lambda path: [
            {"exchange": "PSX", "ticker": "AGTL", "asset_type": "Stock"},
            {"exchange": "PSX", "ticker": "MIIETF", "asset_type": "ETF"},
        ],
    )

    class FixedDateTime:
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 4, 8, tzinfo=tz or timezone.utc)

    monkeypatch.setattr(fetch_exchange_masterfiles, "datetime", FixedDateTime)

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(
            "symbolname.lis",
            (
                "AGTL|Al-Ghazi Tr.|Al-Ghazi Tractors Limited|\n"
                "MIIETF|MII ETF|MII ETF|\n"
            ).encode("utf-16"),
        )
    zip_bytes = buffer.getvalue()

    class FakeResponse:
        def __init__(self, text: str = "", content: bytes = b""):
            self.text = text
            self.content = content

        def raise_for_status(self):
            return None

    class FakeSession:
        def __init__(self):
            self.post_calls = []
            self.get_calls = []

        def post(self, url, data=None, headers=None, timeout=None):
            self.post_calls.append((url, data, headers, timeout))
            if data["date"].endswith("08"):
                return FakeResponse("<div>No file yet</div>")
            return FakeResponse('<a href="/download/symbol_name/2026-04-07.zip">Download</a>')

        def get(self, url, headers=None, timeout=None):
            self.get_calls.append((url, headers, timeout))
            return FakeResponse(content=zip_bytes)

    session = FakeSession()
    rows = fetch_psx_symbol_name_daily(source, session=session)

    assert [(row["ticker"], row["name"], row["asset_type"], row["source_url"]) for row in rows] == [
        (
            "AGTL",
            "Al-Ghazi Tractors Limited",
            "Stock",
            "https://dps.psx.com.pk/download/symbol_name/2026-04-07.zip",
        ),
        (
            "MIIETF",
            "MII ETF",
            "ETF",
            "https://dps.psx.com.pk/download/symbol_name/2026-04-07.zip",
        ),
    ]
    assert session.post_calls[0][0] == "https://dps.psx.com.pk/daily-downloads"
    assert session.post_calls[0][2] == {
        "User-Agent": "free-ticker-database/2.0 (+https://github.com/adanos-software/free-ticker-database)",
        "Referer": "https://dps.psx.com.pk/daily-downloads",
        "Origin": "https://dps.psx.com.pk",
        "Connection": "close",
    }
    assert session.get_calls == [
        (
            "https://dps.psx.com.pk/download/symbol_name/2026-04-07.zip",
            {
                "User-Agent": "free-ticker-database/2.0 (+https://github.com/adanos-software/free-ticker-database)",
                "Referer": "https://dps.psx.com.pk/daily-downloads",
                "Origin": "https://dps.psx.com.pk",
                "Connection": "close",
            },
            30.0,
        )
    ]


def test_psx_source_is_modeled_as_partial_official_coverage() -> None:
    source = next(item for item in OFFICIAL_SOURCES if item.key == "psx_listed_companies")
    assert source.reference_scope == "listed_companies_subset"


def test_psx_symbol_name_source_is_modeled_as_partial_official_coverage() -> None:
    source = next(item for item in OFFICIAL_SOURCES if item.key == "psx_symbol_name_daily")
    assert source.reference_scope == "listed_companies_subset"


def test_deutsche_boerse_etfs_etps_source_is_modeled_as_partial_official_coverage() -> None:
    source = next(item for item in OFFICIAL_SOURCES if item.key == "deutsche_boerse_etfs_etps")
    assert source.reference_scope == "listed_companies_subset"


def test_deutsche_boerse_xetra_all_tradable_source_is_modeled_as_partial_official_coverage() -> None:
    source = next(item for item in OFFICIAL_SOURCES if item.key == "deutsche_boerse_xetra_all_tradable_equities")
    assert source.reference_scope == "listed_companies_subset"


def test_parse_jpx_listed_issues_excel_maps_tse_rows(tmp_path):
    dataframe_path = tmp_path / "jpx.xlsx"

    import pandas as pd

    pd.DataFrame(
        [
            {"Local Code": 1301, "Name (English)": "KYOKUYO CO.,LTD.", "Section/Products": "Prime Market (Domestic)"},
            {"Local Code": 1305, "Name (English)": "iFreeETF TOPIX (Yearly Dividend Type)", "Section/Products": "ETFs/ ETNs"},
        ]
    ).to_excel(dataframe_path, index=False)

    rows = parse_jpx_listed_issues_excel(dataframe_path.read_bytes(), SOURCE)

    assert rows == [
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "1301",
            "name": "KYOKUYO CO.,LTD.",
            "exchange": "TSE",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "1305",
            "name": "iFreeETF TOPIX (Yearly Dividend Type)",
            "exchange": "TSE",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
    ]


def test_extract_jse_exchange_traded_product_download_url_picks_latest_match() -> None:
    html = """
    <a href="https://www.jse.co.za/sites/default/files/media/documents/etf-list-v86/ETF%20List%20v.86.xlsx">old etf</a>
    <a href="https://www.jse.co.za/sites/default/files/media/documents/etf-list-v87/ETF%20List%20v.87.xlsx">new etf</a>
    <a href="https://www.jse.co.za/sites/default/files/media/documents/etn-list/ETN%20List%20v.21_0.xlsx">etn</a>
    """

    assert (
        extract_jse_exchange_traded_product_download_url(html, "ETF")
        == "https://www.jse.co.za/sites/default/files/media/documents/etf-list-v87/ETF%20List%20v.87.xlsx"
    )
    assert (
        extract_jse_exchange_traded_product_download_url(html, "ETN")
        == "https://www.jse.co.za/sites/default/files/media/documents/etn-list/ETN%20List%20v.21_0.xlsx"
    )


def test_parse_jse_exchange_traded_product_excel_skips_section_headers() -> None:
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        pd.DataFrame(
            [
                {"Alpha": "Top 40 Equity", "Long Name": None},
                {"Alpha": "ETFT40", "Long Name": "1nvest Top 40 ETF"},
                {"Alpha": "ADETNC", "Long Name": "FNB ETN on ADOBEC NOV25"},
                {"Alpha": None, "Long Name": None},
            ]
        ).to_excel(writer, index=False)

    rows = parse_jse_exchange_traded_product_excel(
        buffer.getvalue(),
        SOURCE,
        source_url="https://www.jse.co.za/sites/default/files/media/documents/etf-list-v87/ETF%20List%20v.87.xlsx",
    )

    assert rows == [
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://www.jse.co.za/sites/default/files/media/documents/etf-list-v87/ETF%20List%20v.87.xlsx",
            "ticker": "ETFT40",
            "name": "1nvest Top 40 ETF",
            "exchange": "JSE",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://www.jse.co.za/sites/default/files/media/documents/etf-list-v87/ETF%20List%20v.87.xlsx",
            "ticker": "ADETNC",
            "name": "FNB ETN on ADOBEC NOV25",
            "exchange": "JSE",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
    ]


def test_fetch_jse_exchange_traded_product_rows_uses_discovered_workbook_url() -> None:
    workbook = io.BytesIO()
    with pd.ExcelWriter(workbook, engine="openpyxl") as writer:
        pd.DataFrame([{"Alpha": "EASYAI", "Long Name": "EasyETFs AI World Actively Managed ETF"}]).to_excel(writer, index=False)

    html = """
    <a href="https://www.jse.co.za/sites/default/files/media/documents/etf-list-v87/ETF%20List%20v.87.xlsx">ETF</a>
    <a href="https://www.jse.co.za/sites/default/files/media/documents/etn-list/ETN%20List%20v.21_0.xlsx">ETN</a>
    """

    class FakeResponse:
        def __init__(self, *, text: str = "", content: bytes = b""):
            self.text = text
            self.content = content

        def raise_for_status(self) -> None:
            return None

    class FakeSession:
        def get(self, url, headers=None, timeout=None):
            if url == JSE_EXCHANGE_TRADED_PRODUCTS_URL:
                return FakeResponse(text=html)
            if url == "https://www.jse.co.za/sites/default/files/media/documents/etf-list-v87/ETF%20List%20v.87.xlsx":
                return FakeResponse(content=workbook.getvalue())
            raise AssertionError(url)

    source = MasterfileSource(
        key="jse_etf_list",
        provider="JSE",
        description="JSE ETF list",
        source_url=JSE_EXCHANGE_TRADED_PRODUCTS_URL,
        format="jse_etf_list_xlsx",
        reference_scope="listed_companies_subset",
    )

    rows = fetch_jse_exchange_traded_product_rows(source, session=FakeSession())

    assert rows == [
        {
            "source_key": "jse_etf_list",
            "provider": "JSE",
            "source_url": "https://www.jse.co.za/sites/default/files/media/documents/etf-list-v87/ETF%20List%20v.87.xlsx",
            "ticker": "EASYAI",
            "name": "EasyETFs AI World Actively Managed ETF",
            "exchange": "JSE",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
        }
    ]


def test_extract_jse_instrument_search_links_returns_unique_absolute_urls() -> None:
    html = """
    <a href="/jse/instruments/3059">AVI</a>
    <a href="https://www.jse.co.za/jse/instruments/3059">AVI duplicate</a>
    <a href="/jse/instruments/3715">MCZ</a>
    """

    assert extract_jse_instrument_search_links(html) == [
        "https://www.jse.co.za/jse/instruments/3059",
        "https://www.jse.co.za/jse/instruments/3715",
    ]


def test_extract_jse_instrument_metadata_parses_meta_description() -> None:
    html = """
    <meta
      name="description"
      content="Instrument name: MC Mining Limited. Instrument code: MCZ. Instrument type: Equities. Listing date: 1970-01-01"
    />
    """

    assert extract_jse_instrument_metadata(html) == {
        "name": "MC Mining Limited",
        "code": "MCZ",
        "instrument_type": "Equities",
    }


def test_fetch_jse_instrument_search_exact_returns_only_exact_code_matches() -> None:
    search_html = """
    <a href="/jse/instruments/3262">enX Group Limited</a>
    <a href="/jse/instruments/9999">Exact match</a>
    """
    mismatched_html = """
    <meta name="description" content="Instrument name: enX Group Limited. Instrument code: ENX. Instrument type: Equities. Listing date: 1970-01-01" />
    """
    matched_html = """
    <meta name="description" content="Instrument name: Example X Holdings Ltd. Instrument code: EXX. Instrument type: Equities. Listing date: 1970-01-01" />
    """

    class FakeResponse:
        def __init__(self, text: str):
            self.text = text

        def raise_for_status(self) -> None:
            return None

    class FakeSession:
        def get(self, url, params=None, headers=None, timeout=None):
            if url == JSE_SEARCH_URL:
                assert params == {"keys": "EXX"}
                return FakeResponse(search_html)
            if url == "https://www.jse.co.za/jse/instruments/3262":
                return FakeResponse(mismatched_html)
            if url == "https://www.jse.co.za/jse/instruments/9999":
                return FakeResponse(matched_html)
            raise AssertionError((url, params))

    source = MasterfileSource(
        key="jse_instrument_search",
        provider="JSE",
        description="JSE instrument search",
        source_url=JSE_SEARCH_URL,
        format="jse_instrument_search_html",
        reference_scope="listed_companies_subset",
    )

    rows = fetch_jse_instrument_search_exact(
        source,
        ["EXX"],
        session=FakeSession(),
        asset_type_by_ticker={"EXX": "Stock"},
    )

    assert rows == [
        {
            "source_key": "jse_instrument_search",
            "provider": "JSE",
            "source_url": "https://www.jse.co.za/jse/instruments/9999",
            "ticker": "EXX",
            "name": "Example X Holdings Ltd",
            "exchange": "JSE",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
        }
    ]


def test_jse_instrument_search_source_is_modeled_as_partial_official_coverage() -> None:
    source = next(item for item in OFFICIAL_SOURCES if item.key == "jse_instrument_search")
    assert source.reference_scope == "listed_companies_subset"


def test_load_jse_instrument_search_rows_prefers_cache(tmp_path, monkeypatch) -> None:
    cache_path = tmp_path / "jse_instrument_search.json"
    cache_path.write_text(
        json.dumps(
            {
                "AVI": [
                    {
                        "source_key": "jse_instrument_search",
                        "provider": "JSE",
                        "source_url": "https://www.jse.co.za/jse/instruments/3059",
                        "ticker": "AVI",
                        "name": "AVI Ltd",
                        "exchange": "JSE",
                        "asset_type": "Stock",
                        "listing_status": "active",
                        "reference_scope": "listed_companies_subset",
                        "official": "true",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.JSE_INSTRUMENT_SEARCH_CACHE", cache_path)
    monkeypatch.setattr(
        "scripts.fetch_exchange_masterfiles.LEGACY_JSE_INSTRUMENT_SEARCH_CACHE",
        tmp_path / "legacy_jse_instrument_search.json",
    )
    monkeypatch.setattr(
        "scripts.fetch_exchange_masterfiles.jse_instrument_search_target_tickers",
        lambda *args, **kwargs: ["AVI"],
    )

    source = next(item for item in OFFICIAL_SOURCES if item.key == "jse_instrument_search")
    rows, mode = load_jse_instrument_search_rows(source)

    assert mode == "cache"
    assert rows == [
        {
            "source_key": "jse_instrument_search",
            "provider": "JSE",
            "source_url": "https://www.jse.co.za/jse/instruments/3059",
            "ticker": "AVI",
            "name": "AVI Ltd",
            "exchange": "JSE",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
        }
    ]


def test_infer_jpx_asset_type_prefers_section_label():
    assert infer_jpx_asset_type("ETFs/ ETNs", "Ordinary Corp.") == "ETF"
    assert infer_jpx_asset_type("Prime Market (Domestic)", "Ordinary Corp.") == "Stock"


def test_parse_deutsche_boerse_listed_companies_excel_maps_xetra_rows(tmp_path):
    workbook_path = tmp_path / "listed-companies.xlsx"

    import pandas as pd

    with pd.ExcelWriter(workbook_path, engine="openpyxl") as writer:
        pd.DataFrame({"placeholder": [1]}).to_excel(writer, sheet_name="Cover", index=False)
        sheet = pd.DataFrame(
            [
                ["Companies in Prime Standard", None, None, None, None],
                [None, None, None, None, None],
                ["2026-04-01", None, None, None, None],
                [None, None, None, None, None],
                [None, None, None, None, None],
                ["Number of instruments:", 2, None, None, None],
                ["Number of companies:", 2, None, None, None],
                ["ISIN", "Trading Symbol", "Company", "Country", "Instrument Exchange"],
                ["DE0005557508", "DTE", "Deutsche Telekom AG", "Germany", "XETRA + FRANKFURT"],
                ["DE000A0WMPJ6", "AIXA", "AIXTRON SE", "Germany", "FRANKFURT"],
            ]
        )
        sheet.to_excel(writer, sheet_name="Prime Standard", header=False, index=False)

    rows = parse_deutsche_boerse_listed_companies_excel(workbook_path.read_bytes(), SOURCE)

    assert rows == [
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "DTE",
            "name": "Deutsche Telekom AG",
            "exchange": "XETRA",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "AIXA",
            "name": "AIXTRON SE",
            "exchange": "XETRA",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
    ]


def test_parse_deutsche_boerse_etfs_etps_excel_maps_xetra_rows() -> None:
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        pd.DataFrame([[None] * 18 for _ in range(8)]).to_excel(
            writer,
            sheet_name="ETFs & ETPs",
            header=False,
            index=False,
        )
        pd.DataFrame(
            [
                {
                    "PRODUCT TYPE": "Active ETF",
                    "PRODUCT NAME": "abrdn Future Raw Materials UCITS ETF USD Accumulating",
                    "ISIN": "IE000J7QYHD8",
                    "PRODUCT FAMILY": "abrdn",
                    "XETRA SYMBOL": "ARAW",
                },
                {
                    "PRODUCT TYPE": "ETP",
                    "PRODUCT NAME": "ETC Group Physical Bitcoin",
                    "ISIN": "DE000A27Z304",
                    "PRODUCT FAMILY": "ETC Group",
                    "XETRA SYMBOL": "BTCE",
                },
            ]
        ).to_excel(writer, sheet_name="ETFs & ETPs", startrow=8, index=False)

    rows = parse_deutsche_boerse_etfs_etps_excel(buffer.getvalue(), SOURCE)

    assert rows == [
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "ARAW",
            "name": "abrdn Future Raw Materials UCITS ETF USD Accumulating",
            "exchange": "XETRA",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "BTCE",
            "name": "ETC Group Physical Bitcoin",
            "exchange": "XETRA",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
    ]


def test_parse_deutsche_boerse_xetra_all_tradable_csv_maps_stock_rows() -> None:
    text = "\n".join(
        [
            "Market:;XETR",
            "Date Last Update:;07.04.2026",
            "Product Status;Instrument Status;Instrument;ISIN;Product ID;Instrument ID;WKN;Mnemonic;MIC Code;Product Assignment Group;Instrument Type",
            "Active;Active;PAYPAL HDGS INC.DL-,0001;US70450Y1038;1;2;A14R7U;2PP;XETR;AUS0;CS",
            "Active;Active;XTRACKERS MSCI WORLD UCITS ETF;IE00BJ0KDQ92;1;2;A1XB5U;XDWD;XETR;PAG_ETF;ETF",
            "Inactive;Active;OLD EQUITY;US0000000001;1;2;000000;OLD;XETR;PAG_EQU;CS",
        ]
    )

    rows = parse_deutsche_boerse_xetra_all_tradable_csv(text, SOURCE)

    assert rows == [
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "2PP",
            "name": "PAYPAL HDGS INC.DL-,0001",
            "exchange": "XETRA",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        }
    ]


def test_parse_tmx_interlisted_marks_subset_scope():
    text = "\n".join(
        [
            "As of March 2, 2026",
            "",
            "Symbol\tName\tUS Symbol\tSector\tInternational Market",
            "AEM:TSX\tAgnico Eagle Mines Limited\tAEM\tMining\tNYSE",
            "AFE:TSXV\tAfrica Energy Corp.\t\tOil & Gas\tNasdaq Nordic",
        ]
    )

    rows = parse_tmx_interlisted(text, SUBSET_SOURCE)

    assert rows == [
        {
            "source_key": "subset",
            "provider": "test",
            "source_url": "https://example.com/subset",
            "ticker": "AEM",
            "name": "Agnico Eagle Mines Limited",
            "exchange": "TSX",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "interlisted_subset",
            "official": "true",
        },
        {
            "source_key": "subset",
            "provider": "test",
            "source_url": "https://example.com/subset",
            "ticker": "AFE",
            "name": "Africa Energy Corp.",
            "exchange": "TSXV",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "interlisted_subset",
            "official": "true",
        },
    ]


def test_parse_tmx_listed_issuers_excel_maps_tsx_and_tsxv_rows() -> None:
    buffer = io.BytesIO()
    tsx_rows = pd.DataFrame(
        [
            {"Exchange": "TSX", "Name": "3iQ Bitcoin ETF", "Root\nTicker": "BTCQ", "Sector": "ETP"},
            {"Exchange": "TSX", "Name": "5N Plus Inc.", "Root\nTicker": "VNP", "Sector": "Clean Technology & Renewable Energy"},
        ]
    )
    tsxv_rows = pd.DataFrame(
        [
            {"Exchange": "TSXV", "Name": "01 Quantum Inc.", "Root\nTicker": "ONE", "Sector": "Technology"},
        ]
    )
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        tsx_rows.to_excel(writer, sheet_name="TSX Issuers December 2025", startrow=9, index=False)
        tsxv_rows.to_excel(writer, sheet_name="TSXV Issuers December 2025", startrow=9, index=False)

    rows = parse_tmx_listed_issuers_excel(buffer.getvalue(), SOURCE)

    assert rows == [
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "BTCQ",
            "name": "3iQ Bitcoin ETF",
            "exchange": "TSX",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "VNP",
            "name": "5N Plus Inc.",
            "exchange": "TSX",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "ONE",
            "name": "01 Quantum Inc.",
            "exchange": "TSXV",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
    ]


def test_resolve_tmx_listed_issuers_download_url_uses_latest_workbook() -> None:
    class FakeResponse:
        def __init__(self, text: str):
            self.text = text

        def raise_for_status(self):
            return None

    class FakeSession:
        def get(self, url, headers=None, timeout=None):
            return FakeResponse(
                """
                <a href="/en/resource/3315/tsx-tsxv-listed-issuers-2024-12-en.xlsx">old</a>
                <a href="/en/resource/3477/tsx-tsxv-listed-issuers-2025-12-en.xlsx">new</a>
                """
            )

    assert (
        resolve_tmx_listed_issuers_download_url(session=FakeSession())
        == "https://www.tsx.com/en/resource/3477/tsx-tsxv-listed-issuers-2025-12-en.xlsx"
    )


def test_tmx_listed_issuers_source_is_modeled_as_partial_official_coverage() -> None:
    source = next(item for item in OFFICIAL_SOURCES if item.key == "tmx_listed_issuers")
    assert source.reference_scope == "listed_companies_subset"


def test_tmx_etf_screener_source_is_modeled_as_partial_official_coverage() -> None:
    source = next(item for item in OFFICIAL_SOURCES if item.key == "tmx_etf_screener")
    assert source.reference_scope == "listed_companies_subset"


def test_parse_tmx_etf_screener_maps_tsx_etf_rows() -> None:
    payload = [
        {"symbol": "TOKN", "longname": "Global X Tokenization Ecosystem Index ETF"},
        {"symbol": "MNT", "shortname": "Royal Canadian Mint ETR"},
        {"symbol": "", "longname": "Ignored"},
    ]

    rows = parse_tmx_etf_screener(payload, SOURCE)

    assert rows == [
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "TOKN",
            "name": "Global X Tokenization Ecosystem Index ETF",
            "exchange": "TSX",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "MNT",
            "name": "Royal Canadian Mint ETR",
            "exchange": "TSX",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
    ]


def test_parse_tmx_etf_screener_respects_record_source_url_and_exchange() -> None:
    payload = [
        {
            "symbol": "LYUV-U",
            "longname": "Lysander-Canso U.S. Corporate Value Bond Fund",
            "exchange": "TSXV",
            "source_url": "https://app-money.tmx.com/graphql",
        }
    ]

    rows = parse_tmx_etf_screener(payload, SOURCE)

    assert rows == [
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://app-money.tmx.com/graphql",
            "ticker": "LYUV-U",
            "name": "Lysander-Canso U.S. Corporate Value Bond Fund",
            "exchange": "TSXV",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        }
    ]


def test_fetch_tmx_etf_screener_quote_rows_normalizes_series_and_skips_name_mismatch(tmp_path, monkeypatch):
    listings_path = tmp_path / "listings.csv"
    listings_path.write_text(
        "\n".join(
            [
                "ticker,name,exchange,asset_type",
                "LYUV-U,Lysander-Canso U.S. Corporate Value Bond Fund,TSX,ETF",
                "TKN-U,Ninepoint Web3 Innovators Fund,TSX,ETF",
            ]
        ),
        encoding="utf-8",
    )
    verification_dir = tmp_path / "etf_verification"
    run_dir = verification_dir / "run-01"
    run_dir.mkdir(parents=True)
    (run_dir / "chunk-01-of-01.csv").write_text(
        "\n".join(
            [
                "ticker,name,exchange,status",
                "LYUV-U,Lysander-Canso U.S. Corporate Value Bond Fund,TSX,reference_gap",
                "TKN-U,Ninepoint Web3 Innovators Fund,TSX,reference_gap",
            ]
        ),
        encoding="utf-8",
    )

    def fake_fetch_tmx_money_quote(symbol: str, session=None):
        if symbol == "LYUV.U":
            return {
                "symbol": "LYUV.U",
                "name": "Lysander-Canso U.S. Corporate Value Bond Fund",
                "exchangeCode": "TSX",
            }
        if symbol == "TKN.U":
            return {
                "symbol": "TKN.U",
                "name": "Ninepoint Crypto and AI Leaders ETF",
                "exchangeCode": "TSX",
            }
        return None

    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "fetch_tmx_money_quote",
        fake_fetch_tmx_money_quote,
    )

    rows = fetch_tmx_etf_screener_quote_rows(
        [],
        listings_path=listings_path,
        verification_dir=verification_dir,
    )

    assert rows == [
        {
            "symbol": "LYUV-U",
            "longname": "Lysander-Canso U.S. Corporate Value Bond Fund",
            "exchange": "TSX",
            "source_url": TMX_MONEY_GRAPHQL_URL,
        }
    ]


def test_tmx_stock_quote_symbol_variants_prefers_dot_then_root() -> None:
    assert tmx_stock_quote_symbol_variants("ALAB-P") == ["ALAB.P", "ALAB", "ALAB-P"]
    assert tmx_stock_quote_symbol_variants("ACL") == ["ACL"]


def test_fetch_tmx_stock_quote_rows_backfills_tmx_suffix_stocks(tmp_path, monkeypatch):
    listings_path = tmp_path / "listings.csv"
    listings_path.write_text(
        "\n".join(
            [
                "ticker,name,exchange,asset_type,isin",
                "ALAB-P,A-Labs Capital II Corp,TSXV,Stock,CA58504D1006",
                "ODV-WTV,ODV-WTV,TSXV,Stock,",
                "MMX,MMX,TSXV,Stock,US5732841060",
            ]
        ),
        encoding="utf-8",
    )
    verification_dir = tmp_path / "stock_verification"
    run_dir = verification_dir / "run-01"
    run_dir.mkdir(parents=True)
    (run_dir / "chunk-01-of-01.csv").write_text(
        "\n".join(
            [
                "ticker,name,exchange,status",
                "ALAB-P,A-Labs Capital II Corp,TSXV,reference_gap",
                "ODV-WTV,ODV-WTV,TSXV,reference_gap",
                "MMX,MMX,TSXV,reference_gap",
            ]
        ),
        encoding="utf-8",
    )

    def fake_fetch_tmx_money_quote(symbol: str, session=None):
        payloads = {
            "ALAB.P": {"symbol": "ALAB.P", "name": "A-Labs Capital II Corp.", "exchangeCode": "CDX"},
            "ODV": {"symbol": "ODV", "name": "Osisko Development Corp.", "exchangeCode": "CDX"},
            "MMX": {"symbol": "MMX", "name": "Mustang Minerals Limited", "exchangeCode": "CDX"},
        }
        return payloads.get(symbol)

    monkeypatch.setattr(fetch_exchange_masterfiles, "fetch_tmx_money_quote", fake_fetch_tmx_money_quote)

    rows = fetch_tmx_stock_quote_rows(
        [],
        MasterfileSource(
            key="tmx_listed_issuers",
            provider="TMX",
            description="Official TSX/TSXV listed issuers workbook",
            source_url="https://www.tsx.com/en/listings/current-market-statistics/mig-archives",
            format="tmx_listed_issuers_excel",
        ),
        listings_path=listings_path,
        verification_dir=verification_dir,
    )

    assert rows == [
        {
            "source_key": "tmx_listed_issuers",
            "provider": "TMX",
            "source_url": TMX_MONEY_GRAPHQL_URL,
            "ticker": "ALAB-P",
            "name": "A-Labs Capital II Corp.",
            "exchange": "TSXV",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "CA58504D1006",
        }
    ]


def test_fetch_tmx_money_etfs_adds_source_url() -> None:
    class DummyResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return {
                "data": {
                    "getETFs": [
                        {
                            "symbol": "SPXU",
                            "shortname": "BetaPro S&P 5",
                            "longname": "BetaPro S&P 500 2x Daily Bull ETF",
                            "currency": "CAD",
                        }
                    ]
                }
            }

    class DummySession:
        def __init__(self) -> None:
            self.last_json = None

        def post(self, url: str, *, headers=None, json=None, timeout=None):
            self.last_json = json
            assert url == TMX_MONEY_GRAPHQL_URL
            assert headers == {"User-Agent": fetch_exchange_masterfiles.USER_AGENT, "Content-Type": "application/json"}
            assert timeout == fetch_exchange_masterfiles.REQUEST_TIMEOUT
            return DummyResponse()

    session = DummySession()

    rows = fetch_tmx_money_etfs(session=session)

    assert rows == [
        {
            "symbol": "SPXU",
            "shortname": "BetaPro S&P 5",
            "longname": "BetaPro S&P 500 2x Daily Bull ETF",
            "currency": "CAD",
            "source_url": TMX_MONEY_GRAPHQL_URL,
        }
    ]
    assert session.last_json == {"query": fetch_exchange_masterfiles.TMX_MONEY_GET_ETFS_QUERY}


def test_load_tmx_etf_screener_payload_prefers_cache(tmp_path, monkeypatch):
    cache_path = tmp_path / "tmx_etf_screener.json"
    cache_path.write_text('[{"symbol":"TOKN","longname":"Global X Tokenization Ecosystem Index ETF"}]', encoding="utf-8")
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.TMX_ETF_SCREENER_CACHE", cache_path)
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.LEGACY_TMX_ETF_SCREENER_CACHE", tmp_path / "legacy.json")
    monkeypatch.setattr(fetch_exchange_masterfiles, "fetch_tmx_money_etfs", lambda session=None: [])
    monkeypatch.setattr(fetch_exchange_masterfiles, "fetch_tmx_etf_screener_quote_rows", lambda payload, **kwargs: [])

    payload, mode = load_tmx_etf_screener_payload()

    assert mode == "cache"
    assert payload == [{"symbol": "TOKN", "longname": "Global X Tokenization Ecosystem Index ETF"}]


def test_load_tmx_etf_screener_payload_falls_back_to_graphql(tmp_path, monkeypatch):
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.TMX_ETF_SCREENER_CACHE", tmp_path / "tmx_etf_screener.json")
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.LEGACY_TMX_ETF_SCREENER_CACHE", tmp_path / "legacy.json")
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "fetch_json",
        lambda *args, **kwargs: (_ for _ in ()).throw(requests.RequestException("blocked")),
    )
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "fetch_tmx_money_etfs",
        lambda session=None: [
            {
                "symbol": "SPXU",
                "longname": "BetaPro S&P 500 2x Daily Bull ETF",
                "shortname": "BetaPro S&P 5",
                "source_url": TMX_MONEY_GRAPHQL_URL,
            }
        ],
    )
    monkeypatch.setattr(fetch_exchange_masterfiles, "fetch_tmx_etf_screener_quote_rows", lambda payload, **kwargs: [])

    payload, mode = load_tmx_etf_screener_payload()

    assert mode == "network"
    assert payload == [
        {
            "symbol": "SPXU",
            "longname": "BetaPro S&P 500 2x Daily Bull ETF",
            "shortname": "BetaPro S&P 5",
            "source_url": TMX_MONEY_GRAPHQL_URL,
        }
    ]


def test_load_tmx_etf_screener_payload_merges_graphql_rows(tmp_path, monkeypatch):
    cache_path = tmp_path / "tmx_etf_screener.json"
    cache_path.write_text('[{"symbol":"TOKN","longname":"Global X Tokenization Ecosystem Index ETF"}]', encoding="utf-8")
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.TMX_ETF_SCREENER_CACHE", cache_path)
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.LEGACY_TMX_ETF_SCREENER_CACHE", tmp_path / "legacy.json")
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "fetch_tmx_money_etfs",
        lambda session=None: [
            {
                "symbol": "SPXU",
                "longname": "BetaPro S&P 500 2x Daily Bull ETF",
                "shortname": "BetaPro S&P 5",
                "source_url": TMX_MONEY_GRAPHQL_URL,
            }
        ],
    )
    monkeypatch.setattr(fetch_exchange_masterfiles, "fetch_tmx_etf_screener_quote_rows", lambda payload, **kwargs: [])

    payload, mode = load_tmx_etf_screener_payload()

    assert mode == "network"
    assert payload == [
        {"symbol": "TOKN", "longname": "Global X Tokenization Ecosystem Index ETF"},
        {
            "symbol": "SPXU",
            "longname": "BetaPro S&P 500 2x Daily Bull ETF",
            "shortname": "BetaPro S&P 5",
            "source_url": TMX_MONEY_GRAPHQL_URL,
        },
    ]
    assert json.loads(cache_path.read_text(encoding="utf-8")) == payload


def test_load_tmx_etf_screener_payload_merges_safe_quote_backfill(tmp_path, monkeypatch):
    cache_path = tmp_path / "tmx_etf_screener.json"
    cache_path.write_text('[{"symbol":"TOKN","longname":"Global X Tokenization Ecosystem Index ETF"}]', encoding="utf-8")
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.TMX_ETF_SCREENER_CACHE", cache_path)
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.LEGACY_TMX_ETF_SCREENER_CACHE", tmp_path / "legacy.json")
    monkeypatch.setattr(fetch_exchange_masterfiles, "fetch_tmx_money_etfs", lambda session=None: [])
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "fetch_tmx_etf_screener_quote_rows",
        lambda payload, **kwargs: [
            {
                "symbol": "LYUV-U",
                "longname": "Lysander-Canso U.S. Corporate Value Bond Fund",
                "exchange": "TSX",
                "source_url": TMX_MONEY_GRAPHQL_URL,
            }
        ],
    )

    payload, mode = load_tmx_etf_screener_payload()

    assert mode == "network"
    assert payload == [
        {"symbol": "TOKN", "longname": "Global X Tokenization Ecosystem Index ETF"},
        {
            "symbol": "LYUV-U",
            "longname": "Lysander-Canso U.S. Corporate Value Bond Fund",
            "exchange": "TSX",
            "source_url": TMX_MONEY_GRAPHQL_URL,
        },
    ]
    assert json.loads(cache_path.read_text(encoding="utf-8")) == payload


def test_euronext_etf_source_is_modeled_as_partial_official_coverage() -> None:
    source = next(item for item in OFFICIAL_SOURCES if item.key == "euronext_etfs")
    assert source.reference_scope == "listed_companies_subset"


def test_parse_euronext_equities_download_maps_markets():
    text = "\n".join(
        [
            '\ufeffName;ISIN;Symbol;Market;Currency;"Open Price";"High Price";"low Price";"last Price";"last Trade MIC Time";"Time Zone";Volume;Turnover;"Closing Price";"Closing Price DateTime"',
            '"European Equities"',
            '"02 Apr 2026"',
            '"All datapoints provided as of end of last active trading day."',
            'A2A;IT0001233417;A2A;"Euronext Milan";EUR;2.458;2.481;2.454;2.457;" 17:37";CET;8256154;20352541.80;2.457;',
            '"2020 BULKERS";BMG9156K1018;2020;"Oslo Børs";NOK;137.80;140.70;135.50;140.40;" 13:07";CET;166844;23212042.70;-;-',
            '"AEX ETF";NL0000000001;AEX;"Euronext Amsterdam";EUR;1;1;1;1;" 17:35";CET;1;1;1;',
            '"3M";US88579Y1010;4MMM;EuroTLX;EUR;1;1;1;1;" 12:56";CET;1;1;1;',
        ]
    )

    rows = parse_euronext_equities_download(text, SOURCE)

    assert rows == [
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "A2A",
            "name": "A2A",
            "exchange": "Euronext",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "2020",
            "name": "2020 BULKERS",
            "exchange": "OSL",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "AEX",
            "name": "AEX ETF",
            "exchange": "AMS",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "4MMM",
            "name": "3M",
            "exchange": "Euronext",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "secondary_listing_subset",
            "official": "true",
        },
    ]


def test_parse_euronext_etfs_download_keeps_etf_asset_type():
    text = "\n".join(
        [
            '\ufeff"Instrument Fullname";"ESG Classification";Name;ISIN;Symbol;Market;Currency;"Open Price";"High Price";"low Price";"last Price";"last Trade MIC Time";"Time Zone";Volume;Turnover;"Closing Price";"Closing Price DateTime"',
            '"European Trackers"',
            '"08 Apr 2026"',
            '"All datapoints provided as of end of last active trading day."',
            '"Leverage Shares -1x Short Disney ETP Securities";-;"-1X SHORT DIS";XS2337085422;SDIS;"Euronext Amsterdam";EUR;1;1;1;1;" 09:04";CET;-;-;1;',
            '"Amundi ETF BX4";-;"AMUNDI ETF BX4";FR0010411884;BX4;"Euronext Paris";EUR;1;1;1;1;" 17:35";CET;1;1;1;',
        ]
    )

    rows = parse_euronext_etfs_download(text, SOURCE)

    assert rows == [
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "SDIS",
            "name": "Leverage Shares -1x Short Disney ETP Securities",
            "exchange": "AMS",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "BX4",
            "name": "Amundi ETF BX4",
            "exchange": "Euronext",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
    ]


def test_parse_b3_instruments_equities_table_keeps_cash_stocks_and_etfs_only():
    table = {
        "columns": [
            {"name": "TckrSymb"},
            {"name": "AsstDesc"},
            {"name": "SgmtNm"},
            {"name": "MktNm"},
            {"name": "SctyCtgyNm"},
            {"name": "ISIN"},
            {"name": "CrpnNm"},
        ],
        "values": [
            ["PETR4", "PETROBRAS PN", "CASH", "EQUITY-CASH", "SHARES", "BRPETRACNPR6", "PETROLEO BRASILEIRO S.A. PETROBRAS"],
            ["BOVA11", "ISHARES IBOV CI", "CASH", "EQUITY-CASH", "ETF EQUITIES", "BRBOVACTF004", "ISHARES IBOVESPA FUNDO DE INDICE"],
            ["AAPL34", "APPLE DRN", "CASH", "EQUITY-CASH", "BDR", "BRAAPLBDR002", "APPLE INC."],
            ["TAXA150", "FINANC/TERMO", "CASH", "EQUITY-CASH", "SHARES", "BRTAXAINDM77", "TAXA DE FINANCIAMENTO"],
            ["PETR4T", "PETROBRAS PN", "EQUITY FORWARD", "EQUITY-DERIVATE", "COMMON EQUITIES FORWARD", "BRPETRTNO001", "PETROLEO BRASILEIRO S.A. PETROBRAS"],
        ],
    }

    rows = parse_b3_instruments_equities_table(table, SOURCE)

    assert rows == [
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "PETR4",
            "name": "PETROLEO BRASILEIRO S.A. PETROBRAS",
            "exchange": "B3",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "BOVA11",
            "name": "ISHARES IBOVESPA FUNDO DE INDICE",
            "exchange": "B3",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
    ]


def test_parse_b3_listed_funds_payload_maps_acronym_to_11_ticker():
    payload = {
        "page": {"pageNumber": 1, "pageSize": 20, "totalRecords": 2, "totalPages": 1},
        "results": [
            {
                "id": 1,
                "acronym": "BOVA",
                "fundName": "ISHARES IBOVESPA FUNDO DE ÍNDICE",
                "tradingName": "BOVA11",
            },
            {
                "id": 2,
                "acronym": None,
                "fundName": "Ignored Fund",
                "tradingName": "IGNO11",
            },
        ],
    }

    rows = parse_b3_listed_funds_payload(payload, SOURCE)

    assert rows == [
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "BOVA11",
            "name": "ISHARES IBOVESPA FUNDO DE ÍNDICE",
            "exchange": "B3",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        }
    ]


def test_parse_b3_bdr_companies_payload_maps_dre_to_39_ticker():
    payload = {
        "page": {"pageNumber": -1, "pageSize": -1, "totalRecords": 2, "totalPages": -2},
        "results": [
            {
                "issuingCompany": "CBTC",
                "companyName": "21SHARES BITCOIN CORE ETP",
                "typeBDR": "DRE",
            },
            {
                "issuingCompany": "AAPL",
                "companyName": "APPLE INC.",
                "typeBDR": "DRN",
            },
        ],
    }

    rows = parse_b3_bdr_companies_payload(payload, SOURCE)

    assert rows == [
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "CBTC39",
            "name": "21SHARES BITCOIN CORE ETP",
            "exchange": "B3",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        }
    ]


def test_fetch_b3_instruments_equities_uses_workday_and_paginates(monkeypatch):
    class FakeResponse:
        def __init__(self, payload, status_code=200):
            self._payload = payload
            self.status_code = status_code

        def raise_for_status(self):
            return None

        def json(self):
            return self._payload

    class FakeSession:
        def __init__(self):
            self.calls = []

        def get(self, url, headers=None, timeout=None):
            self.calls.append(("GET", url))
            return FakeResponse("2026-04-02T00:00:00")

        def post(self, url, headers=None, json=None, timeout=None):
            self.calls.append(("POST", url))
            page = int(url.rstrip("/").split("/")[-2])
            payload = {
                "table": {
                    "pageCount": 2,
                    "columns": [
                        {"name": "TckrSymb"},
                        {"name": "AsstDesc"},
                        {"name": "SgmtNm"},
                        {"name": "MktNm"},
                        {"name": "SctyCtgyNm"},
                        {"name": "ISIN"},
                        {"name": "CrpnNm"},
                    ],
                    "values": [
                        [f"ROW{page}", f"ROW {page}", "CASH", "EQUITY-CASH", "SHARES", f"BRROW{page}", f"Issuer {page}"],
                    ],
                }
            }
            return FakeResponse(payload)

    rows = fetch_b3_instruments_equities(
        MasterfileSource(
            key="b3",
            provider="B3",
            description="Official B3 instruments consolidated cash-equities table",
            source_url="https://arquivos.b3.com.br/bdi/table/InstrumentsEquities",
            format="b3_instruments_equities_api",
        ),
        session=FakeSession(),
    )

    assert rows == [
        {
            "source_key": "b3",
            "provider": "B3",
            "source_url": "https://arquivos.b3.com.br/bdi/table/InstrumentsEquities",
            "ticker": "ROW1",
            "name": "Issuer 1",
            "exchange": "B3",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
        {
            "source_key": "b3",
            "provider": "B3",
            "source_url": "https://arquivos.b3.com.br/bdi/table/InstrumentsEquities",
            "ticker": "ROW2",
            "name": "Issuer 2",
            "exchange": "B3",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
    ]


def test_fetch_b3_instruments_equities_reads_tail_pages_for_large_tables():
    class FakeResponse:
        def __init__(self, payload, status_code=200):
            self._payload = payload
            self.status_code = status_code

        def raise_for_status(self):
            return None

        def json(self):
            return self._payload

    class FakeSession:
        def __init__(self):
            self.calls = []

        def get(self, url, headers=None, timeout=None):
            self.calls.append(("GET", url))
            return FakeResponse("2026-04-02T00:00:00")

        def post(self, url, headers=None, json=None, timeout=None):
            self.calls.append(("POST", url))
            page = int(url.rstrip("/").split("/")[-2])
            values_by_page = {
                108: [["ROW108", "ROW 108", "CASH", "EQUITY-CASH", "SHARES", "BRROW108", "Issuer 108"]],
                109: [["ROW109", "ROW 109", "CASH", "EQUITY-CASH", "SHARES", "BRROW109", "Issuer 109"]],
                110: [["ROW110", "ROW 110", "CASH", "EQUITY-CASH", "SHARES", "BRROW110", "Issuer 110"]],
                111: [["ROW111", "ROW 111", "CASH", "EQUITY-CASH", "SHARES", "BRROW111", "Issuer 111"]],
            }
            payload = {
                "table": {
                    "pageCount": 111,
                    "columns": [
                        {"name": "TckrSymb"},
                        {"name": "AsstDesc"},
                        {"name": "SgmtNm"},
                        {"name": "MktNm"},
                        {"name": "SctyCtgyNm"},
                        {"name": "ISIN"},
                        {"name": "CrpnNm"},
                    ],
                    "values": values_by_page.get(page, [["ROWX", "ROW X", "EQUITY FORWARD", "EQUITY-DERIVATE", "COMMON EQUITIES FORWARD", "BRROWX", "Issuer X"]]),
                }
            }
            return FakeResponse(payload)

    session = FakeSession()
    rows = fetch_b3_instruments_equities(
        MasterfileSource(
            key="b3",
            provider="B3",
            description="Official B3 instruments consolidated cash-equities table",
            source_url="https://arquivos.b3.com.br/bdi/table/InstrumentsEquities",
            format="b3_instruments_equities_api",
        ),
        session=session,
    )

    assert [row["ticker"] for row in rows] == ["ROW111", "ROW110", "ROW109", "ROW108"]
    post_pages = [int(url.rstrip("/").split("/")[-2]) for method, url in session.calls if method == "POST"]
    assert post_pages == [1, 111, 110, 109, 108, 107]


def test_fetch_b3_listed_funds_reads_all_etf_types_and_pages():
    class FakeResponse:
        def __init__(self, payload, status_code=200):
            self._payload = payload
            self.status_code = status_code

        def raise_for_status(self):
            return None

        def json(self):
            return self._payload

    class FakeSession:
        def __init__(self):
            self.calls = []

        def get(self, url, headers=None, timeout=None):
            self.calls.append(url)
            encoded = url.rstrip("/").split("/")[-1]
            payload = json.loads(b64decode(encoded).decode("utf-8"))
            fund_type = payload["typeFund"]
            page_number = payload["pageNumber"]
            total_pages = 2 if fund_type == "ETF" else 1
            results = []
            if fund_type == "ETF" and page_number == 1:
                results = [{"acronym": "BOVA", "fundName": "ISHARES IBOVESPA FUNDO DE ÍNDICE"}]
            elif fund_type == "ETF" and page_number == 2:
                results = [{"acronym": "SMAL", "fundName": "ISHARES SMALL CAP FUNDO DE ÍNDICE"}]
            elif fund_type == "ETF-RF":
                results = [{"acronym": "FIXA", "fundName": "BB ETF RENDA FIXA"}]
            elif fund_type == "ETF-FII":
                results = [{"acronym": "XFIX", "fundName": "TREND ETF IFIX-L"}]
            elif fund_type == "ETF-CRIPTO":
                results = [{"acronym": "BITC", "fundName": "BTG PACTUAL TEVA BITCOIN FUNDO DE ÍNDICE"}]
            return FakeResponse(
                {
                    "page": {
                        "pageNumber": page_number,
                        "pageSize": payload["pageSize"],
                        "totalRecords": len(results),
                        "totalPages": total_pages,
                    },
                    "results": results,
                }
            )

    rows = fetch_b3_listed_funds(
        MasterfileSource(
            key="b3_listed_etfs",
            provider="B3",
            description="Official B3 listed ETF directories",
            source_url="https://sistemaswebb3-listados.b3.com.br/fundsListedProxy/Search/",
            format="b3_listed_funds_api",
            reference_scope="listed_companies_subset",
        ),
        session=FakeSession(),
    )

    assert [row["ticker"] for row in rows] == ["BOVA11", "SMAL11", "FIXA11", "XFIX11", "BITC11"]


def test_fetch_b3_bdr_companies_keeps_dre_rows_only():
    class FakeResponse:
        def __init__(self, payload, status_code=200):
            self._payload = payload
            self.status_code = status_code

        def raise_for_status(self):
            return None

        def json(self):
            return self._payload

    class FakeSession:
        def get(self, url, headers=None, timeout=None):
            return FakeResponse(
                {
                    "page": {"pageNumber": -1, "pageSize": -1, "totalRecords": 2, "totalPages": -2},
                    "results": [
                        {"issuingCompany": "CBTC", "companyName": "21SHARES BITCOIN CORE ETP", "typeBDR": "DRE"},
                        {"issuingCompany": "AAPL", "companyName": "APPLE INC.", "typeBDR": "DRN"},
                    ],
                }
            )

    rows = fetch_b3_bdr_companies(
        MasterfileSource(
            key="b3_bdr_etfs",
            provider="B3",
            description="Official B3 BDR ETF directory",
            source_url="https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/",
            format="b3_bdr_companies_api",
            reference_scope="listed_companies_subset",
        ),
        session=FakeSession(),
    )

    assert rows == [
        {
            "source_key": "b3_bdr_etfs",
            "provider": "B3",
            "source_url": "https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/",
            "ticker": "CBTC39",
            "name": "21SHARES BITCOIN CORE ETP",
            "exchange": "B3",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
        }
    ]


def test_load_b3_instruments_equities_rows_uses_network_and_refreshes_cache(tmp_path, monkeypatch):
    cache_path = tmp_path / "b3_instruments_equities.json"
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.B3_INSTRUMENTS_EQUITIES_CACHE", cache_path)
    monkeypatch.setattr(
        "scripts.fetch_exchange_masterfiles.LEGACY_B3_INSTRUMENTS_EQUITIES_CACHE",
        tmp_path / "missing.json",
    )
    monkeypatch.setattr(
        "scripts.fetch_exchange_masterfiles.fetch_b3_instruments_equities",
        lambda source, session=None: [
            {
                "ticker": "ABEV3",
                "name": "AMBEV S.A.",
                "exchange": "B3",
                "asset_type": "Stock",
                "listing_status": "active",
            }
        ],
    )

    source = next(item for item in OFFICIAL_SOURCES if item.key == "b3_instruments_equities")
    rows, mode = load_b3_instruments_equities_rows(source)

    assert mode == "network"
    assert rows == [
        {
            "ticker": "ABEV3",
            "name": "AMBEV S.A.",
            "exchange": "B3",
            "asset_type": "Stock",
            "listing_status": "active",
        }
    ]
    assert json.loads(cache_path.read_text(encoding="utf-8")) == rows


def test_fetch_source_rows_with_mode_uses_b3_cache_when_network_unavailable(tmp_path, monkeypatch):
    cache_path = tmp_path / "b3_instruments_equities.json"
    cache_path.write_text(
        '[{"ticker":"ABEV3","name":"AMBEV S.A.","exchange":"B3","asset_type":"Stock","listing_status":"active","source_key":"b3_instruments_equities","reference_scope":"exchange_directory","official":"true","provider":"B3","source_url":"https://example.com"}]',
        encoding="utf-8",
    )
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.B3_INSTRUMENTS_EQUITIES_CACHE", cache_path)
    monkeypatch.setattr(
        "scripts.fetch_exchange_masterfiles.LEGACY_B3_INSTRUMENTS_EQUITIES_CACHE",
        tmp_path / "missing.json",
    )
    monkeypatch.setattr(
        "scripts.fetch_exchange_masterfiles.fetch_b3_instruments_equities",
        lambda source, session=None: (_ for _ in ()).throw(requests.RequestException("network down")),
    )

    source = next(item for item in OFFICIAL_SOURCES if item.key == "b3_instruments_equities")
    rows, mode = fetch_source_rows_with_mode(source)

    assert mode == "cache"
    assert rows[0]["ticker"] == "ABEV3"
    assert rows[0]["exchange"] == "B3"


def test_parse_nasdaq_nordic_stockholm_shares_maps_rows() -> None:
    payload = {
        "data": {
            "instrumentListing": {
                "rows": [
                    {"symbol": "AAK", "fullName": "AAK", "assetClass": "SHARES"},
                    {"symbol": "ABB", "fullName": "ABB Ltd", "assetClass": "SHARES"},
                    {"symbol": "", "fullName": "Ignored"},
                ]
            }
        }
    }

    rows = parse_nasdaq_nordic_stockholm_shares(payload, SOURCE)

    assert rows == [
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "AAK",
            "name": "AAK",
            "exchange": "STO",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "ABB",
            "name": "ABB Ltd",
            "exchange": "STO",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
    ]


def test_parse_nasdaq_nordic_copenhagen_shares_normalizes_symbols_and_keeps_isin() -> None:
    payload = {
        "data": {
            "instrumentListing": {
                "rows": [
                    {
                        "symbol": "MAERSK A",
                        "fullName": "A.P. Møller - Mærsk A",
                        "isin": "DK0010244425",
                        "sector": "Industrials",
                    }
                ]
            }
        }
    }

    rows = parse_nasdaq_nordic_shares(payload, SOURCE, exchange="CPH")

    assert rows == [
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "MAERSK-A",
            "name": "A.P. Møller - Mærsk A",
            "exchange": "CPH",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "DK0010244425",
            "sector": "Industrials",
        }
    ]


def test_fetch_nasdaq_nordic_stockholm_shares_includes_first_north() -> None:
    source = MasterfileSource(
        key="nasdaq_nordic_stockholm_shares",
        provider="Nasdaq Nordic",
        description="Official Stockholm shares screener",
        source_url="https://api.nasdaq.com/api/nordic/screener/shares",
        format="nasdaq_nordic_stockholm_shares_json",
        reference_scope="listed_companies_subset",
    )

    class FakeResponse:
        def __init__(self, payload, status_code=200):
            self._payload = payload
            self.status_code = status_code

        def raise_for_status(self):
            if self.status_code >= 400:
                raise requests.HTTPError(f"status={self.status_code}")

        def json(self):
            return self._payload

    class FakeSession:
        def __init__(self):
            self.calls = []

        def get(self, url, params=None, headers=None, timeout=None):
            self.calls.append((url, params))
            assert params in (
                {"category": "MAIN_MARKET", "tableonly": "false", "market": "STO"},
                {"category": "FIRST_NORTH", "tableonly": "false", "market": "STO"},
            )
            return FakeResponse(
                {
                    "data": {
                        "instrumentListing": {
                            "rows": [
                                {"symbol": "AAK", "fullName": "AAK"},
                                {"symbol": "ABB", "fullName": "ABB Ltd"},
                            ]
                        }
                    }
                }
            )

    session = FakeSession()
    rows = fetch_nasdaq_nordic_stockholm_shares(source, session=session)

    assert [row["ticker"] for row in rows] == ["AAK", "ABB"]
    assert session.calls == [
        (
            "https://api.nasdaq.com/api/nordic/screener/shares",
            {"category": "MAIN_MARKET", "tableonly": "false", "market": "STO"},
        ),
        (
            "https://api.nasdaq.com/api/nordic/screener/shares",
            {"category": "FIRST_NORTH", "tableonly": "false", "market": "STO"},
        ),
    ]


def test_fetch_nasdaq_nordic_helsinki_shares_search_backfills_exact_ticker_gaps(
    tmp_path, monkeypatch
) -> None:
    source = MasterfileSource(
        key="nasdaq_nordic_helsinki_shares_search",
        provider="Nasdaq Nordic",
        description="Official Helsinki share search supplement",
        source_url="https://api.nasdaq.com/api/nordic/search",
        format="nasdaq_nordic_helsinki_shares_search_json",
        reference_scope="listed_companies_subset",
    )
    listings_path = tmp_path / "listings.csv"
    listings_path.write_text(
        "\n".join(
            [
                "ticker,exchange,asset_type,name,isin",
                "ERIBR,HEL,Stock,Telefonaktiebolaget LM Ericsson Class B,SE0000108656",
                "ILKKA1,HEL,Stock,Ilkka Oyj 1,",
                "WITH,HEL,Stock,WITHSECURE,FI4000519228",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "latest_reference_gap_tickers",
        lambda base_dir, exchanges=None: {"ERIBR", "ILKKA1", "WITH"},
    )

    class FakeResponse:
        def __init__(self, payload):
            self._payload = payload

        def raise_for_status(self):
            return None

        def json(self):
            return self._payload

    class FakeSession:
        def __init__(self):
            self.calls = []

        def get(self, url, params=None, headers=None, timeout=None):
            self.calls.append((url, params))
            payloads = {
                "ERIBR": {
                    "data": [
                        {
                            "group": "Shares Main Market",
                            "instruments": [
                                {
                                    "symbol": "ERIBR",
                                    "fullName": "Ericsson B",
                                    "isin": "SE0000108656",
                                    "assetClass": "SHARES",
                                    "currency": "EUR",
                                }
                            ],
                        }
                    ]
                },
                "ILKKA1": {
                    "data": [
                        {
                            "group": "Shares Main Market",
                            "instruments": [
                                {
                                    "symbol": "ILKKA1",
                                    "fullName": "Ilkka Oyj 1",
                                    "isin": "FI0009800197",
                                    "assetClass": "SHARES",
                                    "currency": "EUR",
                                }
                            ],
                        }
                    ]
                },
                "WITH": {
                    "data": [
                        {
                            "group": "Shares Main Market",
                            "instruments": [
                                {
                                    "symbol": "FSECURE",
                                    "fullName": "F-Secure Oyj",
                                    "isin": "FI4000519236",
                                    "assetClass": "SHARES",
                                    "currency": "EUR",
                                }
                            ],
                        }
                    ]
                },
            }
            return FakeResponse(payloads[params["searchText"]])

    session = FakeSession()
    rows = fetch_nasdaq_nordic_helsinki_shares_search(
        source,
        listings_path=listings_path,
        verification_dir=tmp_path,
        session=session,
    )

    assert rows == [
        {
            "source_key": "nasdaq_nordic_helsinki_shares_search",
            "provider": "Nasdaq Nordic",
            "source_url": "https://api.nasdaq.com/api/nordic/search",
            "ticker": "ERIBR",
            "name": "Ericsson B",
            "exchange": "HEL",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "SE0000108656",
        },
        {
            "source_key": "nasdaq_nordic_helsinki_shares_search",
            "provider": "Nasdaq Nordic",
            "source_url": "https://api.nasdaq.com/api/nordic/search",
            "ticker": "ILKKA1",
            "name": "Ilkka Oyj 1",
            "exchange": "HEL",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "FI0009800197",
        },
    ]
    assert session.calls == [
        ("https://api.nasdaq.com/api/nordic/search", {"searchText": "ERIBR"}),
        ("https://api.nasdaq.com/api/nordic/search", {"searchText": "ILKKA1"}),
        ("https://api.nasdaq.com/api/nordic/search", {"searchText": "WITH"}),
    ]


def test_parse_nasdaq_nordic_stockholm_etfs_maps_symbol_aliases() -> None:
    payload = {
        "data": {
            "instrumentListing": {
                "rows": [
                    {
                        "symbol": "XACT Sverige",
                        "fullName": "XACT Sverige (UCITS ETF)",
                        "isin": "SE0001056045",
                    },
                    {
                        "symbol": "XACT BULL 2",
                        "fullName": "XACT BULL 2",
                        "isin": "SE0003051010",
                    },
                    {
                        "symbol": "MONTDIV",
                        "fullName": "Montrose Global Monthly Dividend MSCI World UCITS ETF",
                        "isin": "IE000DMPF2D5",
                    },
                    {
                        "symbol": "",
                        "fullName": "Ignored",
                        "isin": "IE0000000000",
                    },
                ]
            }
        }
    }

    rows = parse_nasdaq_nordic_stockholm_etfs(payload, SOURCE)

    assert rows == [
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "XACT-SVERIGE",
            "name": "XACT Sverige (UCITS ETF)",
            "exchange": "STO",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "SE0001056045",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "XACT-SVERI",
            "name": "XACT Sverige (UCITS ETF)",
            "exchange": "STO",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "SE0001056045",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "XACT-BULL-2",
            "name": "XACT BULL 2",
            "exchange": "STO",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "SE0003051010",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "XACT-BULL-",
            "name": "XACT BULL 2",
            "exchange": "STO",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "SE0003051010",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "MONTDIV",
            "name": "Montrose Global Monthly Dividend MSCI World UCITS ETF",
            "exchange": "STO",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "IE000DMPF2D5",
        },
    ]


def test_parse_nasdaq_nordic_helsinki_etfs_adds_compact_symbol_alias() -> None:
    payload = {
        "data": {
            "instrumentListing": {
                "rows": [
                    {
                        "symbol": "SLG OMXH25",
                        "fullName": "Seligson & Co OMX Helsinki 25 UCITS ETF",
                        "isin": "FI0008805627",
                    }
                ]
            }
        }
    }

    rows = fetch_exchange_masterfiles.parse_nasdaq_nordic_etfs(payload, SOURCE, exchange="HEL")

    assert [row["ticker"] for row in rows] == ["SLG-OMXH25", "SLGOMXH25"]
    assert all(row["exchange"] == "HEL" for row in rows)


def test_fetch_nasdaq_nordic_stockholm_etfs_filters_to_sto() -> None:
    source = MasterfileSource(
        key="nasdaq_nordic_stockholm_etfs",
        provider="Nasdaq Nordic",
        description="Official Stockholm ETF screener",
        source_url="https://api.nasdaq.com/api/nordic/screener/etp",
        format="nasdaq_nordic_stockholm_etfs_json",
        reference_scope="listed_companies_subset",
    )

    class FakeResponse:
        def __init__(self, payload, status_code=200):
            self._payload = payload
            self.status_code = status_code

        def raise_for_status(self):
            if self.status_code >= 400:
                raise requests.HTTPError(f"status={self.status_code}")

        def json(self):
            return self._payload

    class FakeSession:
        def __init__(self):
            self.calls = []

        def get(self, url, params=None, headers=None, timeout=None):
            self.calls.append((url, params))
            assert params == {"category": "ETF", "tableonly": "false", "market": "STO"}
            return FakeResponse(
                {
                    "data": {
                        "instrumentListing": {
                            "rows": [
                                {"symbol": "XACT Sverige", "fullName": "XACT Sverige (UCITS ETF)"},
                                {"symbol": "MONTDIV", "fullName": "Montrose Global Monthly Dividend MSCI World UCITS ETF"},
                            ]
                        }
                    }
                }
            )

    session = FakeSession()
    rows = fetch_exchange_masterfiles.fetch_nasdaq_nordic_stockholm_etfs(source, session=session)

    assert [row["ticker"] for row in rows] == ["XACT-SVERIGE", "XACT-SVERI", "MONTDIV"]
    assert session.calls == [
        (
            "https://api.nasdaq.com/api/nordic/screener/etp",
            {"category": "ETF", "tableonly": "false", "market": "STO"},
        ),
    ]


def test_parse_nasdaq_nordic_stockholm_trackers_maps_symbol_aliases() -> None:
    payload = {
        "data": [
            {
                "group": "Warrants",
                "instruments": [
                    {
                        "symbol": "BITCOIN XBT",
                        "fullName": "Bitcoin Tracker One XBT Provider",
                        "isin": "SE0007126024",
                        "assetClass": "TRACKER_CERTIFICATES",
                    },
                    {
                        "symbol": "ETHEREUM XBT",
                        "fullName": "Ether Tracker One XBT PROVIDER",
                        "isin": "SE0010296574",
                        "assetClass": "TRACKER_CERTIFICATES",
                    },
                    {
                        "symbol": "IGNORED",
                        "fullName": "Ignored non-tracker",
                        "isin": "SE0000000000",
                        "assetClass": "ETF",
                    },
                ],
            }
        ]
    }

    rows = parse_nasdaq_nordic_stockholm_trackers(payload, SOURCE)

    assert rows == [
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "BITCOIN-XBT",
            "name": "Bitcoin Tracker One XBT Provider",
            "exchange": "STO",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "SE0007126024",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "BITCOIN-XB",
            "name": "Bitcoin Tracker One XBT Provider",
            "exchange": "STO",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "SE0007126024",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "ETHEREUM-XBT",
            "name": "Ether Tracker One XBT PROVIDER",
            "exchange": "STO",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "SE0010296574",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "ETHEREUM-X",
            "name": "Ether Tracker One XBT PROVIDER",
            "exchange": "STO",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "SE0010296574",
        },
    ]


def test_fetch_nasdaq_nordic_stockholm_trackers_uses_search_endpoint() -> None:
    source = MasterfileSource(
        key="nasdaq_nordic_stockholm_trackers",
        provider="Nasdaq Nordic",
        description="Official Stockholm tracker certificates",
        source_url="https://api.nasdaq.com/api/nordic/search",
        format="nasdaq_nordic_stockholm_trackers_json",
        reference_scope="listed_companies_subset",
    )

    class FakeResponse:
        def __init__(self, payload, status_code=200):
            self._payload = payload
            self.status_code = status_code

        def raise_for_status(self):
            if self.status_code >= 400:
                raise requests.HTTPError(f"status={self.status_code}")

        def json(self):
            return self._payload

    class FakeSession:
        def __init__(self):
            self.calls = []

        def get(self, url, params=None, headers=None, timeout=None):
            self.calls.append((url, params))
            assert params == {"searchText": "XBT Provider"}
            return FakeResponse(
                {
                    "data": [
                        {
                            "group": "Warrants",
                            "instruments": [
                                {
                                    "symbol": "BITCOIN XBT",
                                    "fullName": "Bitcoin Tracker One XBT Provider",
                                    "isin": "SE0007126024",
                                    "assetClass": "TRACKER_CERTIFICATES",
                                },
                                {
                                    "symbol": "ETHEREUM XBT",
                                    "fullName": "Ether Tracker One XBT PROVIDER",
                                    "isin": "SE0010296574",
                                    "assetClass": "TRACKER_CERTIFICATES",
                                },
                            ],
                        }
                    ]
                }
            )

    session = FakeSession()
    rows = fetch_exchange_masterfiles.fetch_nasdaq_nordic_stockholm_trackers(source, session=session)

    assert [row["ticker"] for row in rows] == ["BITCOIN-XBT", "BITCOIN-XB", "ETHEREUM-XBT", "ETHEREUM-X"]
    assert session.calls == [
        (
            "https://api.nasdaq.com/api/nordic/search",
            {"searchText": "XBT Provider"},
        ),
    ]


def test_sto_source_is_modeled_as_partial_official_coverage() -> None:
    source = next(item for item in OFFICIAL_SOURCES if item.key == "nasdaq_nordic_stockholm_shares")
    assert source.reference_scope == "listed_companies_subset"


def test_load_nasdaq_nordic_stockholm_shares_rows_prefers_cache(tmp_path, monkeypatch) -> None:
    cache_path = tmp_path / "nasdaq_nordic_stockholm_shares.json"
    cache_path.write_text(
        '[{"ticker":"ABB","name":"ABB Ltd","exchange":"STO","asset_type":"Stock","listing_status":"active"}]',
        encoding="utf-8",
    )
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.NASDAQ_NORDIC_STOCKHOLM_SHARES_CACHE", cache_path)

    source = next(item for item in OFFICIAL_SOURCES if item.key == "nasdaq_nordic_stockholm_shares")
    rows, mode = load_nasdaq_nordic_stockholm_shares_rows(source)

    assert mode == "cache"
    assert rows == [{"ticker": "ABB", "name": "ABB Ltd", "exchange": "STO", "asset_type": "Stock", "listing_status": "active"}]


def test_load_nasdaq_nordic_share_search_rows_prefers_cache(tmp_path, monkeypatch) -> None:
    cache_path = tmp_path / "nasdaq_nordic_helsinki_shares_search.json"
    cache_path.write_text(
        '[{"ticker":"ERIBR","name":"Ericsson B","exchange":"HEL","asset_type":"Stock","listing_status":"active"}]',
        encoding="utf-8",
    )
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "NASDAQ_NORDIC_HELSINKI_SHARES_SEARCH_CACHE",
        cache_path,
    )
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "LEGACY_NASDAQ_NORDIC_HELSINKI_SHARES_SEARCH_CACHE",
        tmp_path / "missing.json",
    )

    source = next(item for item in OFFICIAL_SOURCES if item.key == "nasdaq_nordic_helsinki_shares_search")
    rows, mode = load_nasdaq_nordic_share_search_rows(source)

    assert mode == "cache"
    assert rows == [
        {
            "ticker": "ERIBR",
            "name": "Ericsson B",
            "exchange": "HEL",
            "asset_type": "Stock",
            "listing_status": "active",
        }
    ]


def test_fetch_source_rows_with_mode_uses_sto_cache(tmp_path, monkeypatch) -> None:
    cache_path = tmp_path / "nasdaq_nordic_stockholm_shares.json"
    cache_path.write_text(
        '[{"ticker":"ABB","name":"ABB Ltd","exchange":"STO","asset_type":"Stock","listing_status":"active","source_key":"nasdaq_nordic_stockholm_shares","reference_scope":"listed_companies_subset","official":"true","provider":"Nasdaq Nordic","source_url":"https://example.com"}]',
        encoding="utf-8",
    )
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.NASDAQ_NORDIC_STOCKHOLM_SHARES_CACHE", cache_path)

    source = next(item for item in OFFICIAL_SOURCES if item.key == "nasdaq_nordic_stockholm_shares")
    rows, mode = fetch_source_rows_with_mode(source)

    assert mode == "cache"
    assert rows[0]["ticker"] == "ABB"
    assert rows[0]["exchange"] == "STO"


def test_fetch_source_rows_with_mode_uses_copenhagen_shares_cache(tmp_path, monkeypatch) -> None:
    cache_path = tmp_path / "nasdaq_nordic_copenhagen_shares.json"
    cache_path.write_text(
        '[{"ticker":"MAERSK-A","name":"A.P. Møller - Mærsk A","exchange":"CPH","asset_type":"Stock","listing_status":"active","source_key":"nasdaq_nordic_copenhagen_shares","reference_scope":"listed_companies_subset","official":"true","provider":"Nasdaq Nordic","source_url":"https://example.com","isin":"DK0010244425"}]',
        encoding="utf-8",
    )
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.NASDAQ_NORDIC_COPENHAGEN_SHARES_CACHE", cache_path)
    monkeypatch.setattr(
        "scripts.fetch_exchange_masterfiles.LEGACY_NASDAQ_NORDIC_COPENHAGEN_SHARES_CACHE",
        tmp_path / "missing.json",
    )

    source = next(item for item in OFFICIAL_SOURCES if item.key == "nasdaq_nordic_copenhagen_shares")
    rows, mode = fetch_source_rows_with_mode(source)

    assert mode == "cache"
    assert rows[0]["ticker"] == "MAERSK-A"
    assert rows[0]["exchange"] == "CPH"


def test_load_nasdaq_nordic_stockholm_etf_rows_prefers_cache(tmp_path, monkeypatch) -> None:
    cache_path = tmp_path / "nasdaq_nordic_stockholm_etfs.json"
    cache_path.write_text(
        '[{"ticker":"XACT-SVERI","name":"XACT Sverige (UCITS ETF)","exchange":"STO","asset_type":"ETF","listing_status":"active"}]',
        encoding="utf-8",
    )
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.NASDAQ_NORDIC_STOCKHOLM_ETFS_CACHE", cache_path)
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.LEGACY_NASDAQ_NORDIC_STOCKHOLM_ETFS_CACHE", tmp_path / "missing.json")

    source = next(item for item in OFFICIAL_SOURCES if item.key == "nasdaq_nordic_stockholm_etfs")
    rows, mode = load_nasdaq_nordic_stockholm_etf_rows(source)

    assert mode == "cache"
    assert rows == [{"ticker": "XACT-SVERI", "name": "XACT Sverige (UCITS ETF)", "exchange": "STO", "asset_type": "ETF", "listing_status": "active"}]


def test_fetch_source_rows_with_mode_uses_helsinki_etf_cache(tmp_path, monkeypatch) -> None:
    cache_path = tmp_path / "nasdaq_nordic_helsinki_etfs.json"
    cache_path.write_text(
        '[{"ticker":"SLGOMXH25","name":"Seligson & Co OMX Helsinki 25 UCITS ETF","exchange":"HEL","asset_type":"ETF","listing_status":"active","source_key":"nasdaq_nordic_helsinki_etfs","reference_scope":"listed_companies_subset","official":"true","provider":"Nasdaq Nordic","source_url":"https://example.com","isin":"FI0008805627"}]',
        encoding="utf-8",
    )
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.NASDAQ_NORDIC_HELSINKI_ETFS_CACHE", cache_path)
    monkeypatch.setattr(
        "scripts.fetch_exchange_masterfiles.LEGACY_NASDAQ_NORDIC_HELSINKI_ETFS_CACHE",
        tmp_path / "missing.json",
    )

    source = next(item for item in OFFICIAL_SOURCES if item.key == "nasdaq_nordic_helsinki_etfs")
    rows, mode = fetch_source_rows_with_mode(source)

    assert mode == "cache"
    assert rows[0]["ticker"] == "SLGOMXH25"
    assert rows[0]["exchange"] == "HEL"


def test_load_szse_etf_list_rows_falls_back_to_cache(tmp_path, monkeypatch) -> None:
    cache_path = tmp_path / "szse_etf_list.json"
    cache_path.write_text(
        '[{"ticker":"159199","name":"石油ETF平安","exchange":"SZSE","asset_type":"ETF","listing_status":"active"}]',
        encoding="utf-8",
    )
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.SZSE_ETF_LIST_CACHE", cache_path)
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.LEGACY_SZSE_ETF_LIST_CACHE", tmp_path / "missing.json")
    monkeypatch.setattr(
        "scripts.fetch_exchange_masterfiles.fetch_szse_etf_list",
        lambda source, session=None: (_ for _ in ()).throw(requests.RequestException("boom")),
    )

    source = next(item for item in OFFICIAL_SOURCES if item.key == "szse_etf_list")
    rows, mode = load_szse_etf_list_rows(source)

    assert mode == "cache"
    assert rows == [{"ticker": "159199", "name": "石油ETF平安", "exchange": "SZSE", "asset_type": "ETF", "listing_status": "active"}]


def test_fetch_source_rows_with_mode_uses_szse_etf_cache(tmp_path, monkeypatch) -> None:
    cache_path = tmp_path / "szse_etf_list.json"
    cache_path.write_text(
        '[{"ticker":"159199","name":"石油ETF平安","exchange":"SZSE","asset_type":"ETF","listing_status":"active","source_key":"szse_etf_list","reference_scope":"listed_companies_subset","official":"true","provider":"SZSE","source_url":"https://example.com"}]',
        encoding="utf-8",
    )
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.SZSE_ETF_LIST_CACHE", cache_path)
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.LEGACY_SZSE_ETF_LIST_CACHE", tmp_path / "missing.json")
    monkeypatch.setattr(
        "scripts.fetch_exchange_masterfiles.fetch_szse_etf_list",
        lambda source, session=None: (_ for _ in ()).throw(requests.RequestException("boom")),
    )

    source = next(item for item in OFFICIAL_SOURCES if item.key == "szse_etf_list")
    rows, mode = fetch_source_rows_with_mode(source)

    assert mode == "cache"
    assert rows[0]["ticker"] == "159199"
    assert rows[0]["exchange"] == "SZSE"


def test_load_set_etf_search_rows_falls_back_to_cache(tmp_path, monkeypatch) -> None:
    cache_path = tmp_path / "set_etf_search.json"
    cache_path.write_text(
        '[{"ticker":"ABFTH","name":"THE ABF THAILAND BOND INDEX FUND","exchange":"SET","asset_type":"ETF","listing_status":"active"}]',
        encoding="utf-8",
    )
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.SET_ETF_SEARCH_CACHE", cache_path)
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.LEGACY_SET_ETF_SEARCH_CACHE", tmp_path / "missing.json")
    monkeypatch.setattr(
        "scripts.fetch_exchange_masterfiles.fetch_set_etf_search",
        lambda source, session=None: (_ for _ in ()).throw(requests.RequestException("boom")),
    )

    source = next(item for item in OFFICIAL_SOURCES if item.key == "set_etf_search")
    rows, mode = load_set_etf_search_rows(source)

    assert mode == "cache"
    assert rows == [{"ticker": "ABFTH", "name": "THE ABF THAILAND BOND INDEX FUND", "exchange": "SET", "asset_type": "ETF", "listing_status": "active"}]


def test_fetch_source_rows_with_mode_uses_set_etf_cache(tmp_path, monkeypatch) -> None:
    cache_path = tmp_path / "set_etf_search.json"
    cache_path.write_text(
        '[{"ticker":"ABFTH","name":"THE ABF THAILAND BOND INDEX FUND","exchange":"SET","asset_type":"ETF","listing_status":"active","source_key":"set_etf_search","reference_scope":"listed_companies_subset","official":"true","provider":"SET","source_url":"https://example.com"}]',
        encoding="utf-8",
    )
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.SET_ETF_SEARCH_CACHE", cache_path)
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.LEGACY_SET_ETF_SEARCH_CACHE", tmp_path / "missing.json")
    monkeypatch.setattr(
        "scripts.fetch_exchange_masterfiles.fetch_set_etf_search",
        lambda source, session=None: (_ for _ in ()).throw(requests.RequestException("boom")),
    )

    source = next(item for item in OFFICIAL_SOURCES if item.key == "set_etf_search")
    rows, mode = fetch_source_rows_with_mode(source)

    assert mode == "cache"
    assert rows[0]["ticker"] == "ABFTH"
    assert rows[0]["exchange"] == "SET"


def test_fetch_source_rows_with_mode_uses_sto_etf_cache(tmp_path, monkeypatch) -> None:
    cache_path = tmp_path / "nasdaq_nordic_stockholm_etfs.json"
    cache_path.write_text(
        '[{"ticker":"XACT-SVERI","name":"XACT Sverige (UCITS ETF)","exchange":"STO","asset_type":"ETF","listing_status":"active","source_key":"nasdaq_nordic_stockholm_etfs","reference_scope":"listed_companies_subset","official":"true","provider":"Nasdaq Nordic","source_url":"https://example.com","isin":"SE0001056045"}]',
        encoding="utf-8",
    )
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.NASDAQ_NORDIC_STOCKHOLM_ETFS_CACHE", cache_path)
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.LEGACY_NASDAQ_NORDIC_STOCKHOLM_ETFS_CACHE", tmp_path / "missing.json")

    source = next(item for item in OFFICIAL_SOURCES if item.key == "nasdaq_nordic_stockholm_etfs")
    rows, mode = fetch_source_rows_with_mode(source)

    assert mode == "cache"
    assert rows[0]["ticker"] == "XACT-SVERI"
    assert rows[0]["exchange"] == "STO"


def test_load_nasdaq_nordic_stockholm_tracker_rows_prefers_cache(tmp_path, monkeypatch) -> None:
    cache_path = tmp_path / "nasdaq_nordic_stockholm_trackers.json"
    cache_path.write_text(
        '[{"ticker":"BITCOIN-XB","name":"Bitcoin Tracker One XBT Provider","exchange":"STO","asset_type":"ETF","listing_status":"active"}]',
        encoding="utf-8",
    )
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.NASDAQ_NORDIC_STOCKHOLM_TRACKERS_CACHE", cache_path)
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.LEGACY_NASDAQ_NORDIC_STOCKHOLM_TRACKERS_CACHE", tmp_path / "missing.json")

    source = next(item for item in OFFICIAL_SOURCES if item.key == "nasdaq_nordic_stockholm_trackers")
    rows, mode = load_nasdaq_nordic_stockholm_tracker_rows(source)

    assert mode == "cache"
    assert rows == [{"ticker": "BITCOIN-XB", "name": "Bitcoin Tracker One XBT Provider", "exchange": "STO", "asset_type": "ETF", "listing_status": "active"}]


def test_fetch_source_rows_with_mode_uses_sto_tracker_cache(tmp_path, monkeypatch) -> None:
    cache_path = tmp_path / "nasdaq_nordic_stockholm_trackers.json"
    cache_path.write_text(
        '[{"ticker":"BITCOIN-XB","name":"Bitcoin Tracker One XBT Provider","exchange":"STO","asset_type":"ETF","listing_status":"active","source_key":"nasdaq_nordic_stockholm_trackers","reference_scope":"listed_companies_subset","official":"true","provider":"Nasdaq Nordic","source_url":"https://example.com","isin":"SE0007126024"}]',
        encoding="utf-8",
    )
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.NASDAQ_NORDIC_STOCKHOLM_TRACKERS_CACHE", cache_path)
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.LEGACY_NASDAQ_NORDIC_STOCKHOLM_TRACKERS_CACHE", tmp_path / "missing.json")

    source = next(item for item in OFFICIAL_SOURCES if item.key == "nasdaq_nordic_stockholm_trackers")
    rows, mode = fetch_source_rows_with_mode(source)

    assert mode == "cache"
    assert rows[0]["ticker"] == "BITCOIN-XB"
    assert rows[0]["exchange"] == "STO"


def test_parse_six_equity_issuers_maps_rows() -> None:
    payload = {
        "itemList": [
            {"valorSymbol": "ABBN", "company": "ABB Ltd"},
            {"valorSymbol": "ROG", "company": "Roche Holding AG"},
            {"valorSymbol": "", "company": "Ignored"},
        ]
    }

    rows = parse_six_equity_issuers(payload, SOURCE)

    assert rows == [
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "ABBN",
            "name": "ABB Ltd",
            "exchange": "SIX",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "ROG",
            "name": "Roche Holding AG",
            "exchange": "SIX",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
    ]


def test_fetch_six_equity_issuers_uses_official_endpoint() -> None:
    source = MasterfileSource(
        key="six_equity_issuers",
        provider="SIX",
        description="Official SIX Swiss Exchange equity issuers directory",
        source_url="https://www.six-group.com/sheldon/equity_issuers/v1/equity_issuers.json",
        format="six_equity_issuers_json",
        reference_scope="listed_companies_subset",
    )

    class FakeResponse:
        def __init__(self, payload, status_code=200):
            self._payload = payload
            self.status_code = status_code

        def raise_for_status(self):
            if self.status_code >= 400:
                raise requests.HTTPError(f"status={self.status_code}")

        def json(self):
            return self._payload

    class FakeSession:
        def __init__(self):
            self.calls = []

        def get(self, url, headers=None, timeout=None):
            self.calls.append((url, headers))
            return FakeResponse(
                {
                    "itemList": [
                        {"valorSymbol": "ABBN", "company": "ABB Ltd"},
                        {"valorSymbol": "ROG", "company": "Roche Holding AG"},
                    ]
                }
            )

    session = FakeSession()
    rows = fetch_six_equity_issuers(source, session=session)

    assert [row["ticker"] for row in rows] == ["ABBN", "ROG"]
    assert session.calls == [
        (
            "https://www.six-group.com/sheldon/equity_issuers/v1/equity_issuers.json",
            {
                "User-Agent": "free-ticker-database/2.0 (+https://github.com/adanos-software/free-ticker-database)",
                "Accept": "application/json,text/plain,*/*",
                "Referer": "https://www.six-group.com/en/market-data/shares/companies.html",
            },
        )
    ]


def test_six_source_is_modeled_as_partial_official_coverage() -> None:
    source = next(item for item in OFFICIAL_SOURCES if item.key == "six_equity_issuers")
    assert source.reference_scope == "listed_companies_subset"


def test_parse_six_fund_products_csv_maps_rows() -> None:
    text = "\n".join(
        [
            "FundLongName;ValorSymbol;FundReutersTicker;FundBloombergTicker;ISIN;TradingBaseCurrency;FundCurrency;ProductLineDesc",
            "abrdn Future Raw Materials UCITS ETF USD Acc ETF Share Class;ARAW;;;IE000QUAANR0;USD;USD;Exchange Traded Funds",
            "21Shares Bitcoin ETP;ABTC;ABTC.S;ABTC SE;CH0454664001;USD;USD;Exchange Traded Product",
            "WisdomTree Physical Crypto Mega Cap Securities;BLOC;BLOCEUR.S;BLOCEUR SE;GB00BMTP1736;EUR;USD;Exchange Traded Product",
            ";IGNORED;;;IE0000000000;USD;USD;Exchange Traded Funds",
        ]
    )

    rows = parse_six_fund_products_csv(text, SOURCE)

    assert rows == [
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "ARAW",
            "name": "abrdn Future Raw Materials UCITS ETF USD Acc ETF Share Class",
            "exchange": "SIX",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "IE000QUAANR0",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "ARAW-USD",
            "name": "abrdn Future Raw Materials UCITS ETF USD Acc ETF Share Class",
            "exchange": "SIX",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "IE000QUAANR0",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "ARAWUSD",
            "name": "abrdn Future Raw Materials UCITS ETF USD Acc ETF Share Class",
            "exchange": "SIX",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "IE000QUAANR0",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "ABTC",
            "name": "21Shares Bitcoin ETP",
            "exchange": "SIX",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "CH0454664001",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "ABTC-USD",
            "name": "21Shares Bitcoin ETP",
            "exchange": "SIX",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "CH0454664001",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "ABTCUSD",
            "name": "21Shares Bitcoin ETP",
            "exchange": "SIX",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "CH0454664001",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "BLOC",
            "name": "WisdomTree Physical Crypto Mega Cap Securities",
            "exchange": "SIX",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "GB00BMTP1736",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "BLOC-EUR",
            "name": "WisdomTree Physical Crypto Mega Cap Securities",
            "exchange": "SIX",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "GB00BMTP1736",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "BLOCEUR",
            "name": "WisdomTree Physical Crypto Mega Cap Securities",
            "exchange": "SIX",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "GB00BMTP1736",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "BLOCUSD",
            "name": "WisdomTree Physical Crypto Mega Cap Securities",
            "exchange": "SIX",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "GB00BMTP1736",
        },
    ]


def test_fetch_six_fund_products_uses_official_endpoint() -> None:
    source = MasterfileSource(
        key="six_etf_products",
        provider="SIX",
        description="Official SIX Swiss Exchange ETF explorer export",
        source_url="https://www.six-group.com/fqs/ref.csv?select=FundLongName,ValorSymbol&where=ProductLine=ET*PortalSegment=FU&orderby=FundLongName&page=1&pagesize=99999",
        format="six_fund_products_csv",
        reference_scope="listed_companies_subset",
    )

    class FakeResponse:
        def __init__(self, text, status_code=200):
            self.text = text
            self.status_code = status_code

        def raise_for_status(self):
            if self.status_code >= 400:
                raise requests.HTTPError(f"status={self.status_code}")

    class FakeSession:
        def __init__(self):
            self.calls = []

        def get(self, url, headers=None, timeout=None):
            self.calls.append((url, headers))
            return FakeResponse("FundLongName;ValorSymbol\n21Shares Bitcoin ETP;ABTC\n")

    session = FakeSession()
    rows = fetch_six_fund_products(source, session=session)

    assert [row["ticker"] for row in rows] == ["ABTC"]
    assert session.calls == [
        (
            "https://www.six-group.com/fqs/ref.csv?select=FundLongName,ValorSymbol&where=ProductLine=ET*PortalSegment=FU&orderby=FundLongName&page=1&pagesize=99999",
            {
                "User-Agent": "free-ticker-database/2.0 (+https://github.com/adanos-software/free-ticker-database)",
                "Accept": "text/csv,application/octet-stream,*/*",
                "Referer": "https://www.six-group.com/en/market-data/etf/etf-explorer.html",
            },
        )
    ]


def test_six_fund_sources_are_modeled_as_partial_official_coverage() -> None:
    etf_source = next(item for item in OFFICIAL_SOURCES if item.key == "six_etf_products")
    etp_source = next(item for item in OFFICIAL_SOURCES if item.key == "six_etp_products")

    assert etf_source.reference_scope == "listed_companies_subset"
    assert etp_source.reference_scope == "listed_companies_subset"


def test_fetch_all_sources_collects_source_errors(monkeypatch):
    def fake_fetch_source_rows_with_mode(source, session=None):
        if source.key == "nasdaq_listed":
            return [{"source_key": source.key, "provider": source.provider, "source_url": source.source_url, "ticker": "AAPL", "name": "Apple Inc.", "exchange": "NASDAQ", "asset_type": "Stock", "listing_status": "active", "reference_scope": source.reference_scope, "official": "true"}], "network"
        raise requests.RequestException("boom")

    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.fetch_source_rows_with_mode", fake_fetch_source_rows_with_mode)

    rows, summary = fetch_all_sources(include_manual=False)

    assert rows == [
        {
            "source_key": "nasdaq_listed",
            "provider": "Nasdaq Trader",
            "source_url": "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt",
            "ticker": "AAPL",
            "name": "Apple Inc.",
            "exchange": "NASDAQ",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        }
    ]
    assert summary["generated_at"].endswith("Z")
    assert summary["source_modes"]["nasdaq_listed"] == "network"
    assert summary["source_details"]["nasdaq_listed"]["rows"] == 1
    assert summary["source_details"]["nasdaq_listed"]["generated_at"] == summary["generated_at"]
    assert summary["errors"]


def test_normalize_source_keys_supports_repeated_and_comma_delimited_values() -> None:
    assert normalize_source_keys(["nasdaq_listed, nasdaq_other_listed", "nasdaq_listed", " krx_etf_finder "]) == [
        "nasdaq_listed",
        "nasdaq_other_listed",
        "krx_etf_finder",
    ]


def test_select_official_sources_rejects_unknown_keys() -> None:
    try:
        select_official_sources(["not_a_source"])
    except ValueError as exc:
        assert "not_a_source" in str(exc)
    else:  # pragma: no cover - defensive
        raise AssertionError("Expected ValueError for unknown source key")


def test_fetch_all_sources_limits_to_selected_sources(monkeypatch) -> None:
    seen: list[str] = []

    def fake_fetch_source_rows_with_mode(source, session=None):
        seen.append(source.key)
        return [
            {
                "source_key": source.key,
                "provider": source.provider,
                "source_url": source.source_url,
                "ticker": source.key.upper(),
                "name": source.key,
                "exchange": "TEST",
                "asset_type": "Stock",
                "listing_status": "active",
                "reference_scope": source.reference_scope,
                "official": "true",
            }
        ], "network"

    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.fetch_source_rows_with_mode", fake_fetch_source_rows_with_mode)
    selected_sources = select_official_sources(["nasdaq_listed", "krx_etf_finder"])

    rows, summary = fetch_all_sources(include_manual=False, sources=selected_sources)

    assert seen == ["nasdaq_listed", "krx_etf_finder"]
    assert [row["source_key"] for row in rows] == ["krx_etf_finder", "nasdaq_listed"]
    assert summary["source_modes"] == {"nasdaq_listed": "network", "krx_etf_finder": "network"}


def test_merge_reference_rows_replaces_only_selected_sources() -> None:
    merged = merge_reference_rows(
        [
            {
                "source_key": "nasdaq_listed",
                "ticker": "AAPL",
                "exchange": "NASDAQ",
                "listing_status": "active",
                "reference_scope": "exchange_directory",
            },
            {
                "source_key": "krx_etf_finder",
                "ticker": "091220",
                "exchange": "KRX",
                "listing_status": "active",
                "reference_scope": "listed_companies_subset",
            },
        ],
        [
            {
                "source_key": "krx_etf_finder",
                "ticker": "104530",
                "exchange": "KRX",
                "listing_status": "active",
                "reference_scope": "listed_companies_subset",
            }
        ],
        source_keys={"krx_etf_finder"},
    )

    assert merged == [
        {
            "source_key": "krx_etf_finder",
            "ticker": "104530",
            "exchange": "KRX",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
        },
        {
            "source_key": "nasdaq_listed",
            "ticker": "AAPL",
            "exchange": "NASDAQ",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
        },
    ]


def test_load_sec_company_tickers_exchange_payload_prefers_cache(tmp_path, monkeypatch):
    cache = tmp_path / "sec_company_tickers_exchange.json"
    cache.write_text('{"fields":["ticker"],"data":[["AAPL"]]}', encoding="utf-8")
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.SEC_COMPANY_TICKERS_CACHE", cache)
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.LEGACY_SEC_COMPANY_TICKERS_CACHE", tmp_path / "missing.json")

    payload, mode = load_sec_company_tickers_exchange_payload()

    assert mode == "cache"
    assert payload == {"fields": ["ticker"], "data": [["AAPL"]]}


def test_sec_request_headers_include_contactable_user_agent(monkeypatch):
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.SEC_CONTACT_EMAIL", "sec@example.com")

    headers = sec_request_headers()

    assert headers["User-Agent"] == "free-ticker-database/2.0 (sec@example.com)"
    assert headers["Referer"] == "https://www.sec.gov/"


def test_fetch_source_rows_with_mode_uses_sec_cache(tmp_path, monkeypatch):
    cache = tmp_path / "sec_company_tickers_exchange.json"
    cache.write_text(
        '{"fields":["cik","name","ticker","exchange"],"data":[[320193,"Apple Inc.","AAPL","Nasdaq"]]}',
        encoding="utf-8",
    )
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.SEC_COMPANY_TICKERS_CACHE", cache)
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.LEGACY_SEC_COMPANY_TICKERS_CACHE", tmp_path / "missing.json")

    rows, mode = fetch_source_rows_with_mode(
        MasterfileSource(
            key="sec_company_tickers_exchange",
            provider="SEC",
            description="Official SEC company ticker to exchange mapping",
            source_url="https://www.sec.gov/files/company_tickers_exchange.json",
            format="sec_company_tickers_exchange_json",
        )
    )

    assert mode == "cache"
    assert rows == [
        {
            "source_key": "sec_company_tickers_exchange",
            "provider": "SEC",
            "source_url": "https://www.sec.gov/files/company_tickers_exchange.json",
            "ticker": "AAPL",
            "name": "Apple Inc.",
            "exchange": "NASDAQ",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        }
    ]

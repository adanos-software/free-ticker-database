from __future__ import annotations

import io

import pandas as pd
import requests

from scripts.fetch_exchange_masterfiles import (
    LEGACY_LSE_COMPANY_REPORTS_CACHE,
    LEGACY_TPEX_MAINBOARD_QUOTES_CACHE,
    LSE_PAGE_INITIALS,
    LSE_COMPANY_REPORTS_CACHE,
    MasterfileSource,
    OFFICIAL_SOURCES,
    SSE_ETF_SUBCLASSES,
    TPEX_MAINBOARD_QUOTES_CACHE,
    fetch_b3_instruments_equities,
    fetch_all_sources,
    fetch_krx_etf_finder,
    fetch_krx_listed_companies,
    fetch_lse_company_reports,
    fetch_sse_a_share_list,
    fetch_sse_etf_list,
    fetch_szse_a_share_list,
    fetch_source_rows_with_mode,
    infer_jpx_asset_type,
    load_lse_company_reports_rows,
    load_sec_company_tickers_exchange_payload,
    load_tpex_mainboard_quotes_payload,
    parse_asx_listed_companies,
    parse_b3_instruments_equities_table,
    parse_deutsche_boerse_etfs_etps_excel,
    parse_deutsche_boerse_listed_companies_excel,
    parse_deutsche_boerse_xetra_all_tradable_csv,
    parse_euronext_equities_download,
    parse_jpx_listed_issues_excel,
    parse_krx_etf_finder,
    parse_krx_listed_companies,
    parse_lse_company_reports_html,
    parse_nasdaq_listed,
    parse_other_listed,
    parse_sec_company_tickers_exchange,
    parse_sse_a_share_list,
    parse_sse_etf_list,
    parse_szse_a_share_list,
    parse_szse_a_share_workbook,
    parse_tpex_mainboard_quotes,
    parse_twse_listed_companies,
    parse_tmx_interlisted,
    parse_tmx_listed_issuers_excel,
    resolve_tmx_listed_issuers_download_url,
    sec_request_headers,
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


def test_szse_source_is_modeled_as_partial_official_coverage() -> None:
    source = next(item for item in OFFICIAL_SOURCES if item.key == "szse_a_share_list")
    assert source.reference_scope == "listed_companies_subset"


def test_tpex_source_is_modeled_as_partial_official_coverage() -> None:
    source = next(item for item in OFFICIAL_SOURCES if item.key == "tpex_mainboard_daily_quotes")
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


def test_load_tpex_mainboard_quotes_payload_prefers_cache(tmp_path, monkeypatch):
    cache_path = tmp_path / "tpex_mainboard_daily_close_quotes.json"
    cache_path.write_text('[{"SecuritiesCompanyCode":"6488","CompanyName":"環球晶圓股份有限公司"}]', encoding="utf-8")
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.TPEX_MAINBOARD_QUOTES_CACHE", cache_path)
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.LEGACY_TPEX_MAINBOARD_QUOTES_CACHE", tmp_path / "legacy.json")

    payload, mode = load_tpex_mainboard_quotes_payload()

    assert mode == "cache"
    assert payload == [{"SecuritiesCompanyCode": "6488", "CompanyName": "環球晶圓股份有限公司"}]


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


def test_fetch_krx_listed_companies_fetches_kospi_and_kosdaq(monkeypatch):
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
                market = "1" if len([c for c in self.get_calls if "GenerateOTP.jspx" in c[0]]) == 1 else "2"
                return FakeResponse(text=f"otp-{market}")
            return FakeResponse(text="<table><tr><td>ok</td></tr></table>")

        def post(self, url, data=None, **kwargs):
            self.post_calls.append((url, data, kwargs))
            market = data["market_gubun"]
            if market == "1":
                payload = {"block1": [{"isu_cd": "005930", "eng_cor_nm": "SAMSUNG ELECTRONICS"}]}
            else:
                payload = {"block1": [{"isu_cd": "091990", "eng_cor_nm": "CELLTRIONHEALTHCARE"}]}
            return FakeResponse(payload=payload)

    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.pd.read_html", lambda *_args, **_kwargs: [object()])
    session = FakeSession()
    rows = fetch_krx_listed_companies(source, session=session)

    assert [row["ticker"] for row in rows] == ["005930", "091990"]
    assert [row["exchange"] for row in rows] == ["KRX", "KOSDAQ"]


def test_fetch_krx_etf_finder_posts_finder_request():
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


def test_krx_source_is_modeled_as_partial_official_coverage() -> None:
    source = next(item for item in OFFICIAL_SOURCES if item.key == "krx_listed_companies")
    assert source.reference_scope == "listed_companies_subset"


def test_krx_etf_finder_source_is_modeled_as_partial_official_coverage() -> None:
    source = next(item for item in OFFICIAL_SOURCES if item.key == "krx_etf_finder")
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

from __future__ import annotations

import requests

from scripts.fetch_exchange_masterfiles import (
    MasterfileSource,
    fetch_all_sources,
    fetch_source_rows_with_mode,
    infer_jpx_asset_type,
    load_sec_company_tickers_exchange_payload,
    parse_asx_listed_companies,
    parse_euronext_equities_download,
    parse_jpx_listed_issues_excel,
    parse_nasdaq_listed,
    parse_other_listed,
    parse_sec_company_tickers_exchange,
    parse_tmx_interlisted,
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

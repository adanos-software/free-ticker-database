from __future__ import annotations

import html
import io
import json
import zipfile
from base64 import b64decode, b64encode
from datetime import datetime, timezone

import pandas as pd
import requests
import scripts.fetch_exchange_masterfiles as fetch_exchange_masterfiles

from scripts.fetch_exchange_masterfiles import (
    BME_ETF_LIST_CACHE,
    BME_LISTED_COMPANIES_CACHE,
    BMV_CAPITAL_TRUST_SEARCH_CACHE,
    BMV_ETF_SEARCH_CACHE,
    BMV_ISSUER_DIRECTORY_CACHE,
    BMV_MARKET_DATA_SECURITIES_CACHE,
    BMV_STOCK_SEARCH_CACHE,
    QSE_MARKET_WATCH_CACHE,
    SIX_SHARE_DETAILS_FQS_CACHE,
    OTC_MARKETS_SECURITY_PROFILE_CACHE,
    OTC_MARKETS_STOCK_SCREENER_CACHE,
    B3_INSTRUMENTS_EQUITIES_CACHE,
    JSE_INSTRUMENT_SEARCH_CACHE,
    IDX_LISTED_COMPANIES_CACHE,
    IDX_COMPANY_PROFILES_CACHE,
    WSE_LISTED_COMPANIES_CACHE,
    WSE_ETF_LIST_CACHE,
    NEWCONNECT_LISTED_COMPANIES_CACHE,
    TASE_SECURITIES_MARKETDATA_CACHE,
    TASE_ETF_MARKETDATA_CACHE,
    TASE_FOREIGN_ETF_SEARCH_CACHE,
    TASE_PARTICIPATING_UNIT_SEARCH_CACHE,
    HOSE_LISTED_STOCKS_CACHE,
    HOSE_ETF_LIST_CACHE,
    HNX_LISTED_SECURITIES_CACHE,
    UPCOM_REGISTERED_SECURITIES_CACHE,
    LEGACY_BME_ETF_LIST_CACHE,
    LEGACY_BME_LISTED_COMPANIES_CACHE,
    LEGACY_BMV_CAPITAL_TRUST_SEARCH_CACHE,
    LEGACY_BMV_ETF_SEARCH_CACHE,
    LEGACY_BMV_ISSUER_DIRECTORY_CACHE,
    LEGACY_BMV_MARKET_DATA_SECURITIES_CACHE,
    LEGACY_BMV_STOCK_SEARCH_CACHE,
    LEGACY_QSE_MARKET_WATCH_CACHE,
    LEGACY_SIX_SHARE_DETAILS_FQS_CACHE,
    LEGACY_OTC_MARKETS_SECURITY_PROFILE_CACHE,
    LEGACY_OTC_MARKETS_STOCK_SCREENER_CACHE,
    LEGACY_B3_INSTRUMENTS_EQUITIES_CACHE,
    LEGACY_LSE_COMPANY_REPORTS_CACHE,
    LEGACY_LSE_INSTRUMENT_DIRECTORY_CACHE,
    LEGACY_LSE_INSTRUMENT_SEARCH_CACHE,
    LEGACY_LSE_PRICE_EXPLORER_CACHE,
    LEGACY_NASDAQ_NORDIC_COPENHAGEN_SHARES_CACHE,
    LEGACY_NASDAQ_NORDIC_HELSINKI_ETFS_CACHE,
    LEGACY_IDX_LISTED_COMPANIES_CACHE,
    LEGACY_IDX_COMPANY_PROFILES_CACHE,
    LEGACY_WSE_LISTED_COMPANIES_CACHE,
    LEGACY_WSE_ETF_LIST_CACHE,
    LEGACY_NEWCONNECT_LISTED_COMPANIES_CACHE,
    LEGACY_TASE_SECURITIES_MARKETDATA_CACHE,
    LEGACY_TASE_ETF_MARKETDATA_CACHE,
    LEGACY_TASE_FOREIGN_ETF_SEARCH_CACHE,
    LEGACY_TASE_PARTICIPATING_UNIT_SEARCH_CACHE,
    LEGACY_HOSE_LISTED_STOCKS_CACHE,
    LEGACY_HOSE_ETF_LIST_CACHE,
    LEGACY_HNX_LISTED_SECURITIES_CACHE,
    LEGACY_UPCOM_REGISTERED_SECURITIES_CACHE,
    LEGACY_NGM_MARKET_DATA_EQUITIES_CACHE,
    LEGACY_NGM_COMPANIES_PAGE_CACHE,
    LEGACY_SPOTLIGHT_COMPANIES_DIRECTORY_CACHE,
    LEGACY_SPOTLIGHT_COMPANIES_SEARCH_CACHE,
    LEGACY_NASDAQ_NORDIC_STOCKHOLM_ETFS_CACHE,
    LEGACY_NASDAQ_NORDIC_STOCKHOLM_TRACKERS_CACHE,
    LEGACY_SET_DR_SEARCH_CACHE,
    LEGACY_SET_ETF_SEARCH_CACHE,
    LEGACY_SZSE_ETF_LIST_CACHE,
    LEGACY_TMX_ETF_SCREENER_CACHE,
    LEGACY_TPEX_ETF_FILTER_CACHE,
    LEGACY_TPEX_EMERGING_BASIC_INFO_CACHE,
    LEGACY_TPEX_MAINBOARD_QUOTES_CACHE,
    LSE_PAGE_INITIALS,
    LSE_COMPANY_REPORTS_CACHE,
    LSE_INSTRUMENT_DIRECTORY_CACHE,
    LSE_INSTRUMENT_SEARCH_CACHE,
    LSE_PRICE_EXPLORER_CACHE,
    JSE_EXCHANGE_TRADED_PRODUCTS_URL,
    JSE_SEARCH_URL,
    MasterfileSource,
    OFFICIAL_SOURCES,
    extract_lse_last_page,
    extract_lse_price_explorer_search,
    extract_jse_exchange_traded_product_download_url,
    extract_jse_instrument_metadata,
    extract_jse_instrument_search_links,
    extract_latest_asx_investment_products_url,
    SSE_ETF_SUBCLASSES,
    TMX_ETF_SCREENER_CACHE,
    TPEX_ETF_FILTER_CACHE,
    TPEX_EMERGING_BASIC_INFO_CACHE,
    TPEX_MAINBOARD_QUOTES_CACHE,
    fetch_b3_instruments_equities,
    fetch_b3_bdr_companies,
    fetch_b3_listed_funds,
    fetch_bme_reference_rows,
    fetch_bmv_capital_trust_search,
    fetch_bmv_etf_search,
    fetch_bmv_issuer_directory,
    fetch_bmv_market_data_securities,
    fetch_bmv_stock_search,
    fetch_all_sources,
    fetch_krx_etf_finder,
    fetch_krx_listed_companies,
    fetch_lse_company_reports,
    fetch_lse_instrument_directory,
    fetch_lse_instrument_search_exact,
    fetch_lse_price_explorer_rows,
    fetch_jse_exchange_traded_product_rows,
    fetch_jse_instrument_search_exact,
    fetch_jpx_tse_stock_detail_rows,
    fetch_idx_listed_companies,
    fetch_idx_company_profiles,
    fetch_newconnect_listed_companies,
    fetch_wse_etf_list,
    fetch_wse_listed_companies,
    fetch_tase_securities_marketdata,
    fetch_tase_etf_marketdata,
    fetch_tase_foreign_etf_search,
    fetch_tase_participating_unit_search,
    fetch_hose_securities_rows,
    fetch_hnx_issuer_rows,
    fetch_nasdaq_nordic_iceland_shares,
    fetch_nasdaq_nordic_share_search,
    fetch_nasdaq_nordic_stockholm_shares,
    fetch_nasdaq_nordic_stockholm_trackers,
    fetch_ngm_companies_page,
    fetch_ngm_market_data_equities,
    fetch_otc_markets_security_profile,
    fetch_qse_market_watch_rows,
    fetch_spotlight_companies_directory,
    fetch_spotlight_companies_search,
    fetch_psx_listed_companies,
    fetch_psx_symbol_name_daily,
    fetch_six_equity_issuers,
    fetch_six_fund_products,
    fetch_six_share_details_fqs,
    fetch_sse_a_share_list,
    fetch_sse_etf_list,
    fetch_szse_a_share_list,
    fetch_szse_b_share_list,
    fetch_szse_etf_list,
    fetch_source_rows_with_mode,
    fetch_tmx_money_etfs,
    fetch_tmx_stock_quote_rows,
    fetch_tmx_etf_screener_quote_rows,
    infer_jpx_asset_type,
    infer_lse_lookup_asset_type,
    jpx_tse_stock_detail_target_rows,
    jse_instrument_search_target_tickers,
    load_b3_instruments_equities_rows,
    load_bme_reference_rows,
    load_bmv_capital_trust_search_rows,
    load_bmv_etf_search_rows,
    load_bmv_issuer_directory_rows,
    load_bmv_market_data_securities_rows,
    load_bmv_stock_search_rows,
    load_jse_instrument_search_rows,
    load_jpx_tse_stock_detail_rows,
    load_idx_listed_companies_rows,
    load_idx_company_profiles_rows,
    load_wse_reference_rows,
    load_tase_securities_marketdata_rows,
    load_tase_etf_marketdata_rows,
    load_tase_foreign_etf_search_rows,
    load_tase_participating_unit_search_rows,
    load_hose_securities_rows,
    load_hnx_issuer_rows,
    NASDAQ_NORDIC_COPENHAGEN_SHARES_CACHE,
    NASDAQ_NORDIC_HELSINKI_ETFS_CACHE,
    load_lse_company_reports_rows,
    load_lse_instrument_directory_rows,
    load_lse_instrument_search_rows,
    load_lse_price_explorer_rows,
    load_nasdaq_nordic_share_search_rows,
    load_nasdaq_nordic_stockholm_etf_rows,
    load_nasdaq_nordic_stockholm_tracker_rows,
    load_nasdaq_nordic_stockholm_shares_rows,
    load_ngm_companies_rows,
    load_ngm_market_data_equity_rows,
    load_otc_markets_security_profile_rows,
    load_otc_markets_stock_screener_rows,
    load_psx_dps_symbols_rows,
    load_qse_market_watch_rows,
    load_spotlight_directory_rows,
    load_spotlight_search_rows,
    load_set_dr_search_rows,
    load_set_etf_search_rows,
    load_set_stock_search_rows,
    load_szse_b_share_list_rows,
    load_szse_etf_list_rows,
    merge_reference_rows,
    dedupe_rows,
    NASDAQ_NORDIC_STOCKHOLM_ETFS_CACHE,
    NASDAQ_NORDIC_STOCKHOLM_TRACKERS_CACHE,
    NGM_COMPANIES_PAGE_CACHE,
    NGM_MARKET_DATA_EQUITIES_CACHE,
    PSE_LISTED_COMPANY_DIRECTORY_CACHE,
    SPOTLIGHT_COMPANIES_DIRECTORY_CACHE,
    SPOTLIGHT_COMPANIES_SEARCH_CACHE,
    load_sec_company_tickers_exchange_payload,
    load_six_share_details_fqs_rows,
    normalize_source_keys,
    load_tpex_etf_filter_payload,
    load_tpex_mainboard_basic_info_text,
    parse_tase_securities_marketdata_payload,
    parse_tase_etf_marketdata_payload,
    parse_hose_securities_payload,
    parse_hnx_issuer_table_html,
    load_tpex_emerging_basic_info_text,
    load_tmx_etf_screener_payload,
    load_pse_listed_company_directory_rows,
    parse_pse_listed_company_directory_html,
    build_pse_cz_share_rows,
    parse_pse_cz_detail_ticker,
    parse_pse_cz_share_links,
    parse_ngm_companies_page_html,
    parse_ngm_market_data_equities,
    parse_spotlight_company_title,
    extract_spotlight_detail_isin,
    load_tpex_mainboard_quotes_payload,
    parse_spotlight_search_heading,
    spotlight_trade_url_from_detail_url,
    parse_set_dr_search_payload,
    parse_tpex_mainboard_basic_info_csv,
    parse_tpex_emerging_basic_info_csv,
    parse_tpex_etf_filter,
    lse_instrument_search_target_tickers,
    parse_asx_listed_companies,
    parse_asx_investment_products_excel,
    parse_athex_classification_lines,
    parse_bse_bw_listed_companies_html,
    parse_bursa_closing_prices_table_rows,
    parse_bse_hu_marketdata_html,
    parse_bse_india_scrips_payload,
    parse_egx_listed_stocks_viewstate,
    parse_hkex_securities_list_workbook,
    parse_bahrain_bourse_isin_codes_html,
    parse_bmv_market_data_instruments_html,
    parse_bmv_market_data_profile_html,
    parse_bist_kap_company_list,
    parse_bist_kap_mkk_listed_securities_payload,
    parse_adx_market_watch_payloads,
    parse_tadawul_active_symbols,
    parse_tadawul_securities_payload,
    parse_boursa_kuwait_legacy_mix_payload,
    parse_bvc_rv_issuers_payload,
    parse_byma_equity_detail_payload,
    parse_byma_header_search_payload,
    parse_cavali_bvl_emisores_html_pages,
    parse_cse_lk_all_security_code_payload,
    parse_cse_lk_company_info_summary_payload,
    parse_cse_ma_jsonapi_collection,
    parse_dse_tz_listed_companies_html,
    parse_mse_mw_mainboard_html,
    parse_msx_companies_payload,
    parse_qse_market_watch_payload,
    parse_nse_india_equity_csv,
    parse_nse_india_etf_csv,
    parse_nse_ke_listed_companies_html,
    parse_nzx_instruments_next_data_payload,
    parse_nasdaq_mutual_fund_quote_payload,
    parse_bolsa_santiago_instruments_payload,
    parse_gse_listed_companies_markdown,
    parse_luse_listed_companies_markdown,
    parse_rse_listed_companies_html,
    parse_sem_isin_workbook,
    parse_use_ug_market_snapshot_html,
    parse_zse_zw_issuers_payload,
    parse_bvb_fund_units_directory_html,
    parse_bvb_shares_directory_html,
    parse_b3_bdr_companies_payload,
    parse_b3_instruments_equities_table,
    parse_b3_listed_funds_payload,
    parse_cboe_canada_listing_directory_payload,
    parse_cboe_canada_listing_directory_html,
    parse_deutsche_boerse_etfs_etps_excel,
    parse_deutsche_boerse_listed_companies_excel,
    parse_deutsche_boerse_xetra_all_tradable_csv,
    parse_euronext_equities_download,
    parse_euronext_etfs_download,
    parse_wse_company_search_html,
    parse_wse_etf_list_html,
    normalize_wse_stock_sector,
    parse_vienna_listed_companies_html,
    parse_zagreb_securities_html,
    parse_jpx_listed_issues_excel,
    parse_jpx_tse_stock_detail_payload,
    parse_jse_exchange_traded_product_excel,
    parse_ngx_company_profile_detail_html,
    parse_ngx_company_profile_directory_html,
    parse_ngx_equities_price_list_payload,
    parse_idx_listed_companies_payload,
    parse_idx_company_profiles_payload,
    parse_krx_etf_finder,
    parse_krx_listed_companies,
    parse_krx_product_finder_records,
    parse_krx_stock_finder_records,
    parse_nasdaq_nordic_shares,
    parse_nasdaq_nordic_stockholm_etfs,
    parse_nasdaq_nordic_stockholm_trackers,
    parse_lse_company_reports_html,
    parse_lse_price_explorer_rows,
    parse_nasdaq_nordic_stockholm_shares,
    parse_nasdaq_listed,
    parse_other_listed,
    parse_psx_listed_companies,
    parse_psx_dps_symbols_payload,
    parse_psx_symbol_name_daily,
    LEGACY_PSE_LISTED_COMPANY_DIRECTORY_CACHE,
    parse_set_listed_companies_html,
    parse_set_quote_search_payload,
    parse_set_stock_search_payload,
    parse_sec_company_tickers_exchange,
    parse_otc_markets_security_profile,
    parse_otc_markets_stock_screener_csv,
    parse_six_equity_issuers,
    parse_six_fund_products_csv,
    parse_six_share_details_fqs_payload,
    parse_dfm_company_profile_payloads,
    parse_sgx_securities_prices_payload,
    parse_sse_a_share_list,
    parse_sse_etf_list,
    parse_szse_a_share_list,
    parse_szse_a_share_workbook,
    parse_szse_b_share_list,
    parse_szse_b_share_workbook,
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
    derive_isin_from_otc_markets_cusip,
    derive_taiwan_isin,
    sec_request_headers,
    select_official_sources,
    extract_psx_sector_options,
    extract_psx_symbol_name_download_url,
    extract_psx_xid,
    extract_html_form_inputs,
    extract_aspnet_viewstate_strings,
    normalize_egx_sector,
    SZSE_ETF_LIST_CACHE,
    SET_DR_SEARCH_CACHE,
    SET_ETF_SEARCH_CACHE,
    SET_STOCK_SEARCH_CACHE,
    TMX_MONEY_GRAPHQL_URL,
    LEGACY_SET_STOCK_SEARCH_CACHE,
    build_six_share_details_rows,
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
            [999, "Acme OTC Markets Corp.", "ACME", "OTC"],
            [111, "Ignored", "BAD", None],
        ],
    }

    rows = parse_sec_company_tickers_exchange(payload, SOURCE)

    assert rows[0]["exchange"] == "NASDAQ"
    assert rows[0]["asset_type"] == "Stock"
    assert rows[2]["asset_type"] == "ETF"
    assert rows[3]["exchange"] == "OTC"
    assert rows[3]["reference_scope"] == "listed_companies_subset"
    assert [row["ticker"] for row in rows] == ["AAPL", "T", "SPY", "ACME"]


def test_derive_isin_from_otc_markets_cusip_is_conservative():
    assert (
        derive_isin_from_otc_markets_cusip(
            "02028L107",
            issuer_country="CA",
            type_name="Ordinary Shares",
            is_adr=False,
        )
        == "CA02028L1076"
    )
    assert (
        derive_isin_from_otc_markets_cusip(
            "75944B106",
            issuer_country="US",
            type_name="Common Stock",
            is_adr=False,
        )
        == "US75944B1061"
    )
    assert (
        derive_isin_from_otc_markets_cusip(
            "02008B103",
            issuer_country="DK",
            type_name="ADR",
            is_adr=True,
        )
        == "US02008B1035"
    )
    assert (
        derive_isin_from_otc_markets_cusip(
            "J02571107",
            issuer_country="JP",
            type_name="Ordinary Shares",
            is_adr=False,
        )
        == ""
    )


def test_parse_otc_markets_security_profile_maps_verified_cusip_isin():
    source = MasterfileSource(
        key="otc_markets_security_profile",
        provider="OTC Markets",
        description="OTC profile",
        source_url="https://www.otcmarkets.com/stock/{symbol}/security",
        format="otc_markets_security_profile_json",
        reference_scope="security_lookup_subset",
    )
    payload = {
        "name": "Almadex Minerals Ltd.",
        "countryId": "CA",
        "securities": [
            {
                "symbol": "AAMMF",
                "cusip": "02028L107",
                "typeName": "Ordinary Shares",
                "isAdr": False,
                "tierName": "Pink Limited",
            }
        ],
    }

    rows = parse_otc_markets_security_profile(
        payload,
        source,
        {
            "ticker": "AAMMF",
            "exchange": "OTC",
            "name": "Almadex Minerals Ltd",
            "asset_type": "Stock",
            "isin": "",
        },
    )

    assert rows == [
        {
            "source_key": "otc_markets_security_profile",
            "provider": "OTC Markets",
            "source_url": "https://www.otcmarkets.com/stock/AAMMF/security",
            "ticker": "AAMMF",
            "name": "Almadex Minerals Ltd.",
            "exchange": "OTC",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "security_lookup_subset",
            "official": "true",
            "isin": "CA02028L1076",
        }
    ]


def test_fetch_otc_markets_security_profile_targets_include_missing_isin_and_review_queue(tmp_path):
    listings_path = tmp_path / "listings.csv"
    listings_path.write_text(
        "\n".join(
            [
                "listing_key,ticker,exchange,name,asset_type,stock_sector,etf_category,country,country_code,isin,aliases",
                "OTC::AAMMF,AAMMF,OTC,Almadex Minerals Ltd,Stock,Materials,,United States,US,,",
                "OTC::HASISIN,HASISIN,OTC,Has Isin Inc,Stock,,,United States,US,US0000000002,",
                "NASDAQ::AAMMF,AAMMF,NASDAQ,Wrong Venue Inc,Stock,,,United States,US,,",
            ]
        ),
        encoding="utf-8",
    )
    review_path = tmp_path / "otc_name_mismatch_review.csv"
    review_path.write_text(
        "\n".join(
            [
                "listing_key,ticker,exchange,asset_type,current_name,official_name,isin,country,official_sources,token_overlap,current_token_count,official_token_count,review_class,review_priority,recommended_action",
                "OTC::HASISIN,HASISIN,OTC,Stock,Has Isin Inc,Current Has Isin Inc,US0000000002,United States,otc_markets_stock_screener,0.5,3,3,weak_abbreviation_or_truncation_review,medium,review",
            ]
        ),
        encoding="utf-8",
    )

    class FakeResponse:
        def __init__(self, symbol: str):
            self.symbol = symbol

        headers = {"content-type": "application/json;charset=UTF-8"}

        def json(self):
            return {
                "name": "Almadex Minerals Ltd." if self.symbol == "AAMMF" else "Has Isin Inc.",
                "countryId": "CA",
                "securities": [
                    {
                        "symbol": self.symbol,
                        "cusip": "02028L107",
                        "typeName": "Ordinary Shares",
                    }
                ],
            }

        def raise_for_status(self):
            return None

    class FakeSession:
        def __init__(self):
            self.urls: list[str] = []

        def get(self, url, params=None, headers=None, timeout=None):
            self.urls.append(url)
            assert params in ({"symbol": "AAMMF"}, {"symbol": "HASISIN"})
            assert headers["referer"] == "https://www.otcmarkets.com/"
            return FakeResponse(params["symbol"])

    session = FakeSession()
    source = MasterfileSource(
        key="otc_markets_security_profile",
        provider="OTC Markets",
        description="OTC profile",
        source_url="https://www.otcmarkets.com/stock/{symbol}/security",
        format="otc_markets_security_profile_json",
        reference_scope="security_lookup_subset",
    )

    rows = fetch_otc_markets_security_profile(
        source,
        session=session,
        listings_path=listings_path,
        otc_name_mismatch_review_path=review_path,
    )

    assert len(session.urls) == 2
    assert {row["ticker"] for row in rows} == {"AAMMF", "HASISIN"}
    assert next(row for row in rows if row["ticker"] == "AAMMF")["isin"] == "CA02028L1076"


def test_parse_otc_markets_stock_screener_csv_maps_conservative_equity_rows():
    source = MasterfileSource(
        key="otc_markets_stock_screener",
        provider="OTC Markets",
        description="OTC stock screener",
        source_url="https://www.otcmarkets.com/research/stock-screener/api/downloadCSV",
        format="otc_markets_stock_screener_csv",
        reference_scope="exchange_directory",
    )
    text = "\n".join(
        [
            "Symbol,Security Name,Tier,Price,Change %,Vol,Sec Type,Country,State",
            "AAPL,Apple Inc,OTCQX,100,,1,Common Stock,USA,California",
            "DUP,First Name,OTCQB,1,,0,Common Stock,USA,",
            "DUP,Second Name,OTCQB,1,,0,Common Stock,USA,",
            "VUSA,ETF Name,OTCID,1,,0,ETFs,USA,",
            "WARR,Ignored Warrant,OTCID,1,,0,Warrants,USA,",
            "UNIT,Ignored Unit,OTCID,1,,0,Units,USA,",
        ]
    )

    rows = parse_otc_markets_stock_screener_csv(text, source)

    assert rows == [
        {
            "source_key": "otc_markets_stock_screener",
            "provider": "OTC Markets",
            "source_url": "https://www.otcmarkets.com/research/stock-screener/api/downloadCSV",
            "ticker": "AAPL",
            "name": "Apple Inc",
            "exchange": "OTC",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
        {
            "source_key": "otc_markets_stock_screener",
            "provider": "OTC Markets",
            "source_url": "https://www.otcmarkets.com/research/stock-screener/api/downloadCSV",
            "ticker": "DUP",
            "name": "First Name",
            "exchange": "OTC",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
        {
            "source_key": "otc_markets_stock_screener",
            "provider": "OTC Markets",
            "source_url": "https://www.otcmarkets.com/research/stock-screener/api/downloadCSV",
            "ticker": "VUSA",
            "name": "ETF Name",
            "exchange": "OTC",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
    ]


def test_load_otc_markets_stock_screener_rows_prefers_cache(tmp_path, monkeypatch) -> None:
    cache_path = tmp_path / "otc_markets_stock_screener.csv"
    cache_path.write_text(
        "\n".join(
            [
                "Symbol,Security Name,Tier,Price,Change %,Vol,Sec Type,Country,State",
                "AAPL,Apple Inc,OTCQX,100,,1,Common Stock,USA,California",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(fetch_exchange_masterfiles, "OTC_MARKETS_STOCK_SCREENER_CACHE", cache_path)
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "LEGACY_OTC_MARKETS_STOCK_SCREENER_CACHE",
        tmp_path / "missing.csv",
    )
    source = MasterfileSource(
        key="otc_markets_stock_screener",
        provider="OTC Markets",
        description="OTC stock screener",
        source_url="https://www.otcmarkets.com/research/stock-screener/api/downloadCSV",
        format="otc_markets_stock_screener_csv",
        reference_scope="exchange_directory",
    )

    rows, mode = load_otc_markets_stock_screener_rows(source)

    assert mode == "cache"
    assert rows == [
        {
            "source_key": "otc_markets_stock_screener",
            "provider": "OTC Markets",
            "source_url": "https://www.otcmarkets.com/research/stock-screener/api/downloadCSV",
            "ticker": "AAPL",
            "name": "Apple Inc",
            "exchange": "OTC",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        }
    ]


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
            "isin": "TW0001101004",
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
            "isin": "TW0000050004",
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
            "isin": "TW00000401A1",
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
            "isin": "TW0000098185",
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
                    {"agdm": "000001", "agjc": '<a href="/x">平安银行</a>', "sshymc": "J 金融业"},
                    {"agdm": "300750", "agjc": '<a href="/y">宁德时代</a>', "sshymc": "C 制造业"},
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
            "sector": "Financials",
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
            "sector": "Industrials",
        },
    ]


def test_parse_szse_a_share_workbook_maps_szse_rows() -> None:
    dataframe = pd.DataFrame(
        [
            {"A股代码": 1, "公司全称": "平安银行股份有限公司", "所属行业": "J 金融业"},
            {"A股代码": "300750", "公司全称": "宁德时代新能源科技股份有限公司", "所属行业": "C 制造业"},
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
            "sector": "Financials",
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
            "sector": "Industrials",
        },
    ]


def test_parse_szse_b_share_list_maps_szse_rows() -> None:
    payload = {
        "result": [
            {
                "metadata": {"pagecount": 1, "recordcount": 2},
                "data": [
                    {"bgdm": "200011", "bgjc": '<a href="/x">深物业B</a>', "sshymc": "K 房地产"},
                    {"bgdm": "200012", "bgjc": '<a href="/y">南玻B</a>', "sshymc": "C 制造业"},
                    {"bgdm": "", "bgjc": "Ignored"},
                ],
            }
        ]
    }

    rows = parse_szse_b_share_list(payload, SOURCE)

    assert rows == [
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "200011",
            "name": "深物业B",
            "exchange": "SZSE",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "sector": "Real Estate",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "200012",
            "name": "南玻B",
            "exchange": "SZSE",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "sector": "Industrials",
        },
    ]


def test_parse_szse_b_share_workbook_maps_szse_rows() -> None:
    dataframe = pd.DataFrame(
        [
            {"B股代码": 200011, "B股简称": "深物业B", "所属行业": "K 房地产"},
            {"B股代码": "200012", "B股简称": "南玻B", "所属行业": "C 制造业"},
            {"B股代码": None, "B股简称": "Ignored"},
        ]
    )
    content = io.BytesIO()
    with pd.ExcelWriter(content, engine="openpyxl") as writer:
        dataframe.to_excel(writer, sheet_name="B股列表", index=False)

    rows = parse_szse_b_share_workbook(content.getvalue(), SOURCE)

    assert rows == [
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "200011",
            "name": "深物业B",
            "exchange": "SZSE",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "sector": "Real Estate",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "200012",
            "name": "南玻B",
            "exchange": "SZSE",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "sector": "Industrials",
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


def test_fetch_szse_b_share_list_fetches_all_pages() -> None:
    source = MasterfileSource(
        key="szse_b",
        provider="SZSE",
        description="SZSE B-share list",
        source_url="https://www.szse.cn/market/product/stock/list/index.html",
        format="szse_b_share_list_json",
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
                                "data": [{"bgdm": "200011", "bgjc": '<a href="/x">深物业B</a>'}],
                            }
                        ]
                    }
                )
            return FakeResponse(
                {
                    "result": [
                        {
                            "metadata": {"pagecount": 2, "recordcount": 2},
                            "data": [{"bgdm": "200012", "bgjc": '<a href="/y">南玻B</a>'}],
                        }
                    ]
                }
            )

    session = FakeSession()
    rows = fetch_szse_b_share_list(source, session=session)

    assert [row["ticker"] for row in rows] == ["200011", "200012"]
    api_calls = [call for call in session.calls if call[1] is not None and "SHOWTYPE" not in call[1]]
    assert [call[1]["PAGENO"] for call in api_calls] == [1, 2]
    assert all(call[1]["CATALOGID"] == "1110" for call in api_calls)
    assert all(call[1]["TABKEY"] == "tab2" for call in api_calls)


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


def test_szse_b_share_source_is_modeled_as_partial_official_coverage() -> None:
    source = next(item for item in OFFICIAL_SOURCES if item.key == "szse_b_share_list")
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


def test_tpex_emerging_basic_info_source_is_modeled_as_partial_official_coverage() -> None:
    source = next(item for item in OFFICIAL_SOURCES if item.key == "tpex_emerging_basic_info")
    assert source.reference_scope == "listed_companies_subset"


def test_tpex_mainboard_basic_info_source_is_modeled_as_partial_official_coverage() -> None:
    source = next(item for item in OFFICIAL_SOURCES if item.key == "tpex_mainboard_basic_info")
    assert source.reference_scope == "listed_companies_subset"


def test_derive_taiwan_isin_handles_board_specific_domestic_codes() -> None:
    assert derive_taiwan_isin("2330") == "TW0002330008"
    assert derive_taiwan_isin("00679B") == "TW00000679B0"
    assert derive_taiwan_isin("1269", emerging_board=True) == "TW0001269B11"
    assert (
        derive_taiwan_isin(
            "4971",
            foreign_registration_country="KY 開曼群島",
        )
        == ""
    )


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
            "isin": "TW0000062017",
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
            "isin": "TW0000062017",
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
            "isin": "TW00000679B0",
            "sector": "Fixed Income",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
    ]


def test_parse_tpex_emerging_basic_info_csv_maps_tpex_rows() -> None:
    text = "\n".join(
        [
            "出表日期,公司代號,公司名稱,公司簡稱,外國企業註冊地國,產業別,英文簡稱",
            "1150409,1269,乾杯股份有限公司,乾杯,－ ,16,KANPAI",
            "1150409,1271,晨暉生物科技股份有限公司,晨暉生技,－ ,22,SunWay",
            "1150409,ABC123,Skip Me,Skip Me,－ ,99,SKIP",
        ]
    )

    rows = parse_tpex_emerging_basic_info_csv(text, SOURCE)

    assert rows == [
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "1269",
            "name": "乾杯股份有限公司",
            "exchange": "TPEX",
            "asset_type": "Stock",
            "isin": "TW0001269B11",
            "sector": "Consumer Discretionary",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "1271",
            "name": "晨暉生物科技股份有限公司",
            "exchange": "TPEX",
            "asset_type": "Stock",
            "isin": "TW0001271B17",
            "sector": "Health Care",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
    ]


def test_parse_tpex_mainboard_basic_info_csv_maps_tpex_rows() -> None:
    text = "\n".join(
        [
            "出表日期,公司代號,公司名稱,公司簡稱,外國企業註冊地國,產業別,英文簡稱",
            "1150410,4971,英特磊科技股份有限公司,IET-KY,KY 開曼群島,24,IntelliEPI Cayman",
            "1150410,5381,光譜電工股份有限公司,光譜,－ ,28,Uniplus Electronics",
            "1150410,ABC123,Skip Me,Skip Me,－ ,99,SKIP",
        ]
    )

    rows = parse_tpex_mainboard_basic_info_csv(text, SOURCE)

    assert rows == [
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "4971",
            "name": "英特磊科技股份有限公司",
            "exchange": "TPEX",
            "asset_type": "Stock",
            "sector": "Information Technology",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "5381",
            "name": "光譜電工股份有限公司",
            "exchange": "TPEX",
            "asset_type": "Stock",
            "isin": "TW0005381008",
            "sector": "Information Technology",
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


def test_load_tpex_emerging_basic_info_text_prefers_cache(tmp_path, monkeypatch):
    cache_path = tmp_path / "tpex_emerging_basic_info.csv"
    cache_path.write_text("出表日期,公司代號,公司名稱\n1150409,1269,乾杯股份有限公司\n", encoding="utf-8-sig")
    monkeypatch.setattr(fetch_exchange_masterfiles, "TPEX_EMERGING_BASIC_INFO_CACHE", cache_path)
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "LEGACY_TPEX_EMERGING_BASIC_INFO_CACHE",
        tmp_path / "legacy.csv",
    )

    text, mode = load_tpex_emerging_basic_info_text()

    assert mode == "cache"
    assert "乾杯股份有限公司" in text


def test_load_tpex_mainboard_basic_info_text_prefers_cache(tmp_path, monkeypatch):
    cache_path = tmp_path / "tpex_mainboard_basic_info.csv"
    cache_path.write_text("出表日期,公司代號,公司名稱\n1150410,4971,英特磊科技股份有限公司\n", encoding="utf-8-sig")
    monkeypatch.setattr(fetch_exchange_masterfiles, "TPEX_MAINBOARD_BASIC_INFO_CACHE", cache_path)
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "LEGACY_TPEX_MAINBOARD_BASIC_INFO_CACHE",
        tmp_path / "legacy.csv",
    )

    text, mode = load_tpex_mainboard_basic_info_text()

    assert mode == "cache"
    assert "英特磊科技股份有限公司" in text


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
            "sector": "Industrials",
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


def test_parse_set_dr_search_payload_keeps_only_drs() -> None:
    text = """
    <script>
    window.__NUXT__=(function(a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r){return {state:{quote:{searchOption:[
      {symbol:a,nameTH:b,nameEN:c,market:d,securityType:e,typeSequence:f,industry:g,sector:h,querySector:h,isIFF:!1,isForeignListing:!1,remark:"",name:a,value:a,securityTypeName:i},
      {symbol:j,nameTH:k,nameEN:l,market:m,securityType:n,typeSequence:o,industry:g,sector:h,querySector:h,isIFF:!1,isForeignListing:!1,remark:"",name:j,value:j,securityTypeName:p},
      {symbol:q,nameTH:"Skip",nameEN:r,market:"SET",securityType:"L",typeSequence:7,industry:g,sector:h,querySector:h,isIFF:!1,isForeignListing:!1,remark:"",name:q,value:q,securityTypeName:"ETF"}
    ],dropdownAdditional:[]}}}})("AMD80","เอเอ็มดี","Depositary Receipt on AMD Issued by KTB","SET","L",7,"","","DR","BABA80","บาบา","Depositary Receipt on BABA Issued by KTB","MAI","L",8,"DR","ABFTH","THE ABF THAILAND BOND INDEX FUND");
    </script>
    """

    rows = parse_set_dr_search_payload(text, SOURCE)

    assert rows == [
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "AMD80",
            "name": "Depositary Receipt on AMD Issued by KTB",
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
            "ticker": "BABA80",
            "name": "Depositary Receipt on BABA Issued by KTB",
            "exchange": "SET",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
    ]


def test_parse_set_stock_search_payload_keeps_common_stock_and_etfs() -> None:
    payload = {
        "securitySymbols": [
            {
                "symbol": "ADVANC",
                "nameEN": "ADVANCED INFO SERVICE PUBLIC COMPANY LIMITED",
                "market": "SET",
                "securityType": "S",
                "industry": "TECH",
                "sector": "ICT",
            },
            {
                "symbol": "1DIV",
                "nameEN": "ThaiDEX SET High Dividend ETF",
                "market": "SET",
                "securityType": "L",
                "industry": "",
                "sector": "",
            },
            {
                "symbol": "ADVANC-F",
                "nameEN": "ADVANCED INFO SERVICE PUBLIC COMPANY LIMITED",
                "market": "SET",
                "securityType": "F",
                "industry": "TECH",
                "sector": "ICT",
            },
            {
                "symbol": "AMD80",
                "nameEN": "Depositary Receipt on AMD Issued by KTB",
                "market": "SET",
                "securityType": "X",
                "industry": "",
                "sector": "",
            },
            {
                "symbol": "A5-W4",
                "nameEN": "Warrants of ASSET FIVE GROUP PUBLIC COMPANY LIMITED",
                "market": "SET",
                "securityType": "W",
                "industry": "PROPCON",
                "sector": "PROP",
            },
        ]
    }

    rows = parse_set_stock_search_payload(payload, SOURCE)

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
            "sector": "Communication Services",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "1DIV",
            "name": "ThaiDEX SET High Dividend ETF",
            "exchange": "SET",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
    ]


def test_load_set_stock_search_rows_prefers_cache(monkeypatch, tmp_path) -> None:
    payload = {
        "securitySymbols": [
            {
                "symbol": "2S",
                "nameEN": "2S METAL PUBLIC COMPANY LIMITED",
                "market": "SET",
                "securityType": "S",
                "industry": "INDUS",
                "sector": "STEEL",
            }
        ]
    }
    cache = tmp_path / "set_stock_search.json"
    cache.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setattr(fetch_exchange_masterfiles, "SET_STOCK_SEARCH_CACHE", cache)
    monkeypatch.setattr(fetch_exchange_masterfiles, "LEGACY_SET_STOCK_SEARCH_CACHE", tmp_path / "missing.json")
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "fetch_set_stock_search",
        lambda source, session=None: (_ for _ in ()).throw(requests.RequestException("blocked")),
    )

    rows, mode = load_set_stock_search_rows(SOURCE)

    assert mode == "cache"
    assert rows == [
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "2S",
            "name": "2S METAL PUBLIC COMPANY LIMITED",
            "exchange": "SET",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "sector": "Materials",
        }
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


def test_parse_lse_price_explorer_rows_maps_equity_and_etfs_only():
    source = MasterfileSource(
        key="lse_price_explorer",
        provider="LSE",
        description="LSE price explorer",
        source_url="https://example.com/price-explorer",
        format="lse_price_explorer_json",
        reference_scope="exchange_directory",
    )
    rows = parse_lse_price_explorer_rows(
        [
            {
                "tidm": "SPA",
                "category": "EQUITY",
                "issuername": "1SPATIAL PLC",
                "description": "1SPATIAL PLC ORD 10P",
                "name": "ORD 10P",
                "isin": "GB00BFZ45C84",
            },
            {
                "tidm": "CBTC",
                "category": "ETFS",
                "issuername": "21SHARES AG",
                "description": "21SHARES AG 21SHARES BITCOIN CORE ETP",
                "name": "21SHARES BITCOIN CORE ETP",
                "isin": "CH1199067674",
            },
            {
                "tidm": "ZL04",
                "category": "BONDS",
                "description": "6.700% NTS 17/10/28",
                "isin": "XS0000000000",
            },
        ],
        source,
    )

    assert rows == [
        {
            "source_key": "lse_price_explorer",
            "provider": "LSE",
            "source_url": "https://example.com/price-explorer",
            "ticker": "SPA",
            "name": "1SPATIAL PLC",
            "exchange": "LSE",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "GB00BFZ45C84",
        },
        {
            "source_key": "lse_price_explorer",
            "provider": "LSE",
            "source_url": "https://example.com/price-explorer",
            "ticker": "CBTC",
            "name": "21SHARES AG 21SHARES BITCOIN CORE ETP",
            "exchange": "LSE",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "CH1199067674",
        },
    ]


def test_fetch_lse_price_explorer_rows_paginates_equity_and_etfs():
    source = MasterfileSource(
        key="lse_price_explorer",
        provider="LSE",
        description="LSE price explorer",
        source_url="https://example.com/price-explorer",
        format="lse_price_explorer_json",
        reference_scope="exchange_directory",
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
            self.params = []

        def get(self, url, params=None, headers=None, timeout=None):
            self.params.append(params)
            page = int(params["parameters"].rsplit("page=", 1)[1])
            content = [
                {
                    "tidm": "SPA" if page == 0 else "CBTC",
                    "category": "EQUITY" if page == 0 else "ETFS",
                    "issuername": "1SPATIAL PLC" if page == 0 else "21SHARES AG",
                    "description": "1SPATIAL PLC ORD 10P"
                    if page == 0
                    else "21SHARES AG 21SHARES BITCOIN CORE ETP",
                    "name": "ORD 10P" if page == 0 else "21SHARES BITCOIN CORE ETP",
                    "isin": "GB00BFZ45C84" if page == 0 else "CH1199067674",
                }
            ]
            return FakeResponse(
                {
                    "components": [
                        {
                            "content": [
                                {
                                    "name": "priceexplorersearch",
                                    "value": {"content": content, "totalPages": 2},
                                }
                            ]
                        }
                    ]
                }
            )

    session = FakeSession()
    rows = fetch_lse_price_explorer_rows(source, session=session)

    assert [row["ticker"] for row in rows] == ["CBTC", "SPA"]
    assert all("categories=EQUITY,ETFS" in call["parameters"] for call in session.params)
    assert [call["path"] for call in session.params] == [
        "live-markets/market-data-dashboard/price-explorer",
        "live-markets/market-data-dashboard/price-explorer",
    ]


def test_load_lse_price_explorer_rows_prefers_cache(tmp_path, monkeypatch):
    cache_path = tmp_path / "lse_price_explorer.json"
    cache_path.write_text('[{"ticker":"SPA","name":"1SPATIAL PLC"}]', encoding="utf-8")
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.LSE_PRICE_EXPLORER_CACHE", cache_path)
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.LEGACY_LSE_PRICE_EXPLORER_CACHE", tmp_path / "legacy.json")

    source = MasterfileSource(
        key="lse_price_explorer",
        provider="LSE",
        description="LSE price explorer",
        source_url="https://example.com/price-explorer",
        format="lse_price_explorer_json",
        reference_scope="exchange_directory",
    )

    rows, mode = load_lse_price_explorer_rows(source)

    assert rows == [{"ticker": "SPA", "name": "1SPATIAL PLC"}]
    assert mode == "cache"


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


def test_parse_cboe_canada_listing_directory_payload_maps_supported_security_types() -> None:
    payload = {
        "data": [
            {"symbol": "ABXX", "name": "ABXX CORP.", "security": "equity"},
            {"symbol": "BYLD.B", "name": "BMO YLD ETF", "security": "etf"},
            {"symbol": "ABCD", "name": "ABC DR", "security": "dr"},
            {"symbol": "FUND", "name": "Closed End Fund", "security": "cef"},
            {"symbol": "WARR", "name": "Ignored Warrant", "security": "warrant"},
        ]
    }

    rows = parse_cboe_canada_listing_directory_payload(payload, SOURCE)

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


def test_fetch_source_rows_with_mode_uses_cboe_canada_api() -> None:
    source = MasterfileSource(
        key="cboe_canada_listing_directory",
        provider="Cboe Canada",
        description="Official Cboe Canada listing directory API",
        source_url="https://www-api.cboe.com/ca/equities/listing-directory-data/",
        format="cboe_canada_listing_directory_json",
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

        def get(self, url, headers=None, timeout=None):
            self.calls.append(url)
            return FakeResponse(
                {
                    "data": [
                        {"symbol": "ABXX", "name": "ABXX CORP.", "security": "equity"},
                        {"symbol": "BYLD.B", "name": "BMO YLD ETF", "security": "etf"},
                    ]
                }
            )

    session = FakeSession()
    rows, mode = fetch_source_rows_with_mode(source, session=session)

    assert mode == "network"
    assert [row["ticker"] for row in rows] == ["ABXX", "BYLD.B", "BYLD-B"]
    assert session.calls == ["https://www-api.cboe.com/ca/equities/listing-directory-data/"]


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
    assert infer_lse_lookup_asset_type("STMM", "ISHARES IV PLC ISHS EUR GVMT BD 20YR TGT DUR EUR DIST", "ETF") == "ETF"


def test_lse_instrument_search_target_tickers_selects_reference_gaps_and_missing_isins(tmp_path):
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

    assert target_tickers == ["ABF", "PHGP", "VUSA"]


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
        '{"PHGP":[{"source_key":"lse_lookup","provider":"LSE","source_url":"https://example.com?codeName=PHGP","ticker":"PHGP","name":"WISDOMTREE METAL SECURITIES WISDOMTREE PHYSICAL GOLD £","exchange":"LSE","asset_type":"Stock","listing_status":"active","reference_scope":"security_lookup_subset","official":"true","isin":"JE00B1VS3770"}]}',
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
            "VUSA": "VANGUARD FUNDS PLC VANGUARD S&P 500 UCITS ETF USD",
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
    assert [item[0] for item in fetched] == [["ABF"], ["VUSA"]]
    assert fetched[0][1] == {"PHGP": "ETF", "VUSA": "ETF", "ABF": "Stock"}
    assert [(row["ticker"], row["asset_type"], row.get("isin", "")) for row in rows] == [
        ("ABF", "Stock", "ISIN-ABF"),
        ("PHGP", "ETF", "JE00B1VS3770"),
        ("VUSA", "ETF", "ISIN-VUSA"),
    ]

    monkeypatch.setattr(
        "scripts.fetch_exchange_masterfiles.fetch_lse_instrument_search_exact",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("cache should be used")),
    )
    rows, mode = load_lse_instrument_search_rows(source)

    assert mode == "cache"
    assert [(row["ticker"], row["asset_type"], row.get("isin", "")) for row in rows] == [
        ("ABF", "Stock", "ISIN-ABF"),
        ("PHGP", "ETF", "JE00B1VS3770"),
        ("VUSA", "ETF", "ISIN-VUSA"),
    ]


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
            "std_ind_cd": "032601",
            "ind_nm": "Manufacture of Semiconductor",
        },
        {
            "isu_cd": "091990",
            "eng_cor_nm": "CELLTRIONHEALTHCARE",
            "std_ind_cd": "032101",
            "ind_nm": "Manufacture of Medicaments",
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
            "sector": "Information Technology",
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
            "sector": "Health Care",
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
        },
        {
            "short_code": "003380",
            "codeName": "하림지주",
            "marketEngName": "KOSDAQ GLOBAL",
            "full_code": "KR7003380003",
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
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "003380",
            "name": "하림지주",
            "exchange": "KOSDAQ",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "KR7003380003",
        },
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
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.missing_isin_listing_tickers", lambda **_kwargs: set())
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.fetch_krx_finder_records", lambda *args, **kwargs: [])
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
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.missing_isin_listing_tickers", lambda **_kwargs: set())
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


def test_fetch_krx_listed_companies_replaces_missing_isin_from_finder(monkeypatch):
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
            if data["market_gubun"] == "1":
                return FakeResponse(payload={"block1": [{"isu_cd": "005930", "eng_cor_nm": "SAMSUNG ELECTRONICS"}]})
            return FakeResponse(payload={"block1": []})

    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.pd.read_html", lambda *_args, **_kwargs: [object()])
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.latest_reference_gap_tickers", lambda *_args, **_kwargs: set())
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.missing_isin_listing_tickers", lambda **_kwargs: {"005930"})
    monkeypatch.setattr(
        "scripts.fetch_exchange_masterfiles.fetch_krx_finder_records",
        lambda *args, **kwargs: [
            {
                "short_code": "005930",
                "codeName": "삼성전자",
                "marketEngName": "KOSPI",
                "full_code": "KR7005930003",
            }
        ],
    )

    rows = fetch_krx_listed_companies(source, session=FakeSession())

    assert rows[0]["ticker"] == "005930"
    assert rows[0]["isin"] == "KR7005930003"


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
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.missing_isin_listing_tickers", lambda **_kwargs: set())
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
        ),
        (
            "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd",
            {
                "bld": "dbms/comm/finder/finder_secuprodisu",
                "mktsel": "ALL",
                "searchText": "",
            },
            {
                "User-Agent": "free-ticker-database/2.0 (+https://github.com/adanos-software/free-ticker-database)",
                "Referer": "https://data.krx.co.kr/contents/MDC/MAIN/main/index.cmd",
            },
            30.0,
        ),
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
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.missing_isin_listing_tickers", lambda **_kwargs: set())

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
            "name": "ACE Tesla Value Chain Active",
            "exchange": "KRX",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "KR7448100001",
        }
    ]


def test_fetch_krx_etf_finder_replaces_missing_isin_from_product_finder(monkeypatch):
    source = MasterfileSource(
        key="krx_etf_finder",
        provider="KRX",
        description="KRX ETF finder",
        source_url="https://example.com/krx-etf",
        format="krx_etf_finder_json",
    )

    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.latest_reference_gap_tickers", lambda *_args, **_kwargs: set())
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.missing_isin_listing_tickers", lambda **_kwargs: {"448100"})

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

    assert rows[0]["ticker"] == "448100"
    assert rows[0]["isin"] == "KR7448100001"


def test_krx_source_is_modeled_as_exchange_directory() -> None:
    source = next(item for item in OFFICIAL_SOURCES if item.key == "krx_listed_companies")
    assert source.reference_scope == "exchange_directory"


def test_krx_etf_finder_source_is_modelled_as_exchange_directory() -> None:
    source = next(item for item in OFFICIAL_SOURCES if item.key == "krx_etf_finder")
    assert source.reference_scope == "exchange_directory"


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


def test_parse_psx_dps_symbols_payload_filters_debt_rights_and_maps_sectors() -> None:
    source = MasterfileSource(
        key="psx_dps_symbols",
        provider="PSX",
        description="PSX DPS symbols",
        source_url="https://dps.psx.com.pk/symbols",
        format="psx_dps_symbols_json",
        reference_scope="exchange_directory",
    )

    rows = parse_psx_dps_symbols_payload(
        [
            {
                "symbol": "AGTL",
                "name": "Al-Ghazi Tractors Limited",
                "sectorName": "AUTOMOBILE ASSEMBLER",
                "isETF": False,
                "isDebt": False,
            },
            {
                "symbol": "MIIETF",
                "name": "MII ETF",
                "sectorName": "EXCHANGE TRADED FUND",
                "isETF": True,
                "isDebt": False,
            },
            {
                "symbol": "AKBLTFC6",
                "name": "Askari Bank(TFC6)",
                "sectorName": "BILLS AND BONDS",
                "isETF": False,
                "isDebt": True,
            },
            {
                "symbol": "786R",
                "name": "786 Investment (Right)",
                "sectorName": "INV. BANKS / INV. COS. / SECURITIES COS.",
                "isETF": False,
                "isDebt": False,
            },
        ],
        source,
    )

    assert [(row["ticker"], row["asset_type"], row["sector"]) for row in rows] == [
        ("AGTL", "Stock", "Consumer Discretionary"),
        ("MIIETF", "ETF", "Equity"),
    ]
    assert all(row["official"] == "true" for row in rows)


def test_load_psx_dps_symbols_rows_prefers_cache_on_network_error(monkeypatch, tmp_path) -> None:
    source = MasterfileSource(
        key="psx_dps_symbols",
        provider="PSX",
        description="PSX DPS symbols",
        source_url="https://dps.psx.com.pk/symbols",
        format="psx_dps_symbols_json",
        reference_scope="exchange_directory",
    )
    cache_path = tmp_path / "psx_dps_symbols.json"
    legacy_path = tmp_path / "legacy_psx_dps_symbols.json"
    cache_path.write_text(
        json.dumps(
            [
                {
                    "symbol": "AGTL",
                    "name": "Al-Ghazi Tractors Limited",
                    "sectorName": "AUTOMOBILE ASSEMBLER",
                    "isETF": False,
                    "isDebt": False,
                }
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(fetch_exchange_masterfiles, "PSX_DPS_SYMBOLS_CACHE", cache_path)
    monkeypatch.setattr(fetch_exchange_masterfiles, "LEGACY_PSX_DPS_SYMBOLS_CACHE", legacy_path)

    def fail_fetch(*args, **kwargs):
        raise requests.RequestException("network unavailable")

    monkeypatch.setattr(fetch_exchange_masterfiles, "fetch_psx_dps_symbols", fail_fetch)

    rows, mode = load_psx_dps_symbols_rows(source)

    assert mode == "cache"
    assert [(row["ticker"], row["sector"]) for row in rows] == [("AGTL", "Consumer Discretionary")]


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


def test_deutsche_boerse_xetra_all_tradable_source_is_modeled_as_exchange_directory() -> None:
    source = next(item for item in OFFICIAL_SOURCES if item.key == "deutsche_boerse_xetra_all_tradable_equities")
    assert source.reference_scope == "exchange_directory"


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


def test_parse_jpx_tse_stock_detail_payload_maps_isin_and_sector() -> None:
    row = parse_jpx_tse_stock_detail_payload(
        {
            "section1": {
                "data": {
                    "7203/T": {
                        "TTCODE2": "7203",
                        "FLLNE": "TOYOTA MOTOR CORP.",
                        "ISIN": "JP3633400001",
                        "JSECE_CNV": "Transportation Equipment",
                    }
                }
            }
        },
        SOURCE,
        fallback_asset_type="Stock",
    )

    assert row == {
        "source_key": "test",
        "provider": "test",
        "source_url": "https://example.com7203",
        "ticker": "7203",
        "name": "TOYOTA MOTOR CORP.",
        "exchange": "TSE",
        "asset_type": "Stock",
        "listing_status": "active",
        "reference_scope": "exchange_directory",
        "official": "true",
        "isin": "JP3633400001",
        "sector": "Consumer Discretionary",
    }


def test_jpx_tse_stock_detail_target_rows_dedupes_tse_stock_and_etf_rows(tmp_path) -> None:
    listings_path = tmp_path / "listings.csv"
    listings_path.write_text(
        "\n".join(
            [
                "ticker,exchange,asset_type,name,isin",
                "7203,TSE,Stock,Toyota,",
                "7203,TSE,Stock,Toyota duplicate,",
                "1306,TSE,ETF,Topix ETF,",
                "MSFT,NASDAQ,Stock,Microsoft,US5949181045",
            ]
        ),
        encoding="utf-8",
    )

    rows = jpx_tse_stock_detail_target_rows(listings_path)

    assert [(row["ticker"], row["asset_type"]) for row in rows] == [("1306", "ETF"), ("7203", "Stock")]


def test_fetch_jpx_tse_stock_detail_rows_uses_official_detail_api(tmp_path) -> None:
    listings_path = tmp_path / "listings.csv"
    listings_path.write_text("ticker,exchange,asset_type,name,isin\n7203,TSE,Stock,Toyota,\n", encoding="utf-8")
    urls: list[str] = []

    class FakeResponse:
        def __init__(self, payload=None):
            self._payload = payload or {}

        def raise_for_status(self) -> None:
            return None

        def json(self):
            return self._payload

    class FakeSession:
        def get(self, url, headers=None, timeout=None):
            urls.append(url)
            if "stock_detail" in url:
                assert headers["Referer"].endswith("qcode=7203")
                return FakeResponse(
                    {
                        "section1": {
                            "data": {
                                "7203/T": {
                                    "TTCODE2": "7203",
                                    "FLLNE": "TOYOTA MOTOR CORP.",
                                    "ISIN": "JP3633400001",
                                }
                            }
                        }
                    }
                )
            return FakeResponse({})

    rows = fetch_jpx_tse_stock_detail_rows(SOURCE, listings_path=listings_path, session=FakeSession())

    assert rows[0]["isin"] == "JP3633400001"
    assert any("F=e_stock_search" in url for url in urls)
    assert any("F=ctl/stock_detail&qcode=7203" in url for url in urls)


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
                {"Alpha": None, "Long Name": None},
                {"Alpha": "Actively Managed", "Long Name": None},
                {
                    "Alpha": "91DINC",
                    "Long Name": "Ninety One Diversified Income Prescient Feeder Actively Managed ETF",
                    "Underlying": "ASISA SA Multi-Asset Income",
                },
                {"Alpha": "Equity", "Long Name": None},
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
            "sector": "Equity",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://www.jse.co.za/sites/default/files/media/documents/etf-list-v87/ETF%20List%20v.87.xlsx",
            "ticker": "91DINC",
            "name": "Ninety One Diversified Income Prescient Feeder Actively Managed ETF",
            "exchange": "JSE",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "sector": "Multi-Asset",
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
            "sector": "Equity",
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
    <div class="instrument-profile__sector">
      <span class="instrument-profile__sector--sector">Basic Resources</span>
    </div>
    <div class="instrument-profile__industry">
      <span class="instrument-profile__industry--Industry">Materials</span>
    </div>
    """

    assert extract_jse_instrument_metadata(html) == {
        "name": "MC Mining Limited",
        "code": "MCZ",
        "instrument_type": "Equities",
        "sector": "Materials",
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
    <div class="instrument-profile__industry"><span class="instrument-profile__industry--Industry">Industrials</span></div>
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
            "sector": "Industrials",
        }
    ]


def test_jse_instrument_search_source_is_modeled_as_partial_official_coverage() -> None:
    source = next(item for item in OFFICIAL_SOURCES if item.key == "jse_instrument_search")
    assert source.reference_scope == "listed_companies_subset"


def test_jse_instrument_search_target_tickers_include_current_metadata_gaps(tmp_path, monkeypatch) -> None:
    listings_path = tmp_path / "listings.csv"
    listings_path.write_text(
        "\n".join(
            [
                "ticker,exchange,asset_type,isin,stock_sector,sector",
                "CPIP,JSE,Stock,,Financials,Financials",
                "PMR,JSE,Stock,,,",
                "STX40,JSE,ETF,,,",
                "ALE,LSE,Stock,,,",
                "AVI,JSE,Stock,ZAE000049433,,",
                "EXX,JSE,Stock,ZAE000084992,Energy,Energy",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(fetch_exchange_masterfiles, "latest_reference_gap_tickers", lambda *_args, **_kwargs: {"EXX"})

    assert jse_instrument_search_target_tickers(listings_path=listings_path) == ["AVI", "CPIP", "EXX", "PMR"]


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


def test_load_jse_instrument_search_rows_keeps_existing_cached_rows_when_targets_change(tmp_path, monkeypatch) -> None:
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
        lambda *args, **kwargs: ["SBK"],
    )
    monkeypatch.setattr(
        "scripts.fetch_exchange_masterfiles.fetch_jse_instrument_search_exact",
        lambda *args, **kwargs: [],
    )

    source = next(item for item in OFFICIAL_SOURCES if item.key == "jse_instrument_search")
    rows, mode = load_jse_instrument_search_rows(source)

    assert mode == "network"
    assert len(rows) == 1
    assert rows[0]["ticker"] == "AVI"


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
            "isin": "DE0005557508",
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
            "isin": "DE000A0WMPJ6",
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
                    "PRODUCT TYPE": "ETN",
                    "PRODUCT NAME": "ETC Group Physical Bitcoin",
                    "ISIN": "DE000A27Z304",
                    "PRODUCT FAMILY": "ETC Group",
                    "XETRA SYMBOL": "BTCE",
                },
                {
                    "PRODUCT TYPE": "ETC",
                    "PRODUCT NAME": "BNPP Gold ETC",
                    "ISIN": "DE000PB8C0P8",
                    "PRODUCT FAMILY": "BNPP ETC",
                    "XETRA SYMBOL": "BNQG",
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
            "isin": "IE000J7QYHD8",
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
            "isin": "DE000A27Z304",
            "sector": "Alternative",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "BNQG",
            "name": "BNPP Gold ETC",
            "exchange": "XETRA",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "DE000PB8C0P8",
            "sector": "Commodity",
        },
    ]


def test_parse_deutsche_boerse_xetra_all_tradable_csv_maps_stock_and_etp_rows() -> None:
    text = "\n".join(
        [
            "Market:;XETR",
            "Date Last Update:;07.04.2026",
            "Product Status;Instrument Status;Instrument;ISIN;Product ID;Instrument ID;WKN;Mnemonic;MIC Code;Product Assignment Group;Product Assignment Group Description;Instrument Type",
            "Active;Active;PAYPAL HDGS INC.DL-,0001;US70450Y1038;1;2;A14R7U;2PP;XETR;AUS0;AUSTRALIEN NEUSEELAND OZEANIEN;CS",
            "Active;Active;XTRACKERS II EUR CORPORATE BOND UCITS ETF;LU0478205379;1;2;DBX0;XBLC;XETR;FONA;EXCHANGE TRADED FUNDS - RENTEN;ETF",
            "Active;Active;SGIS O.E. ETC ICE EUA;DE000ETC0001;1;2;ETC000;SGS1;XETR;ETC1;EXCHANGE TRADED COMMODITIES;ETC",
            "Active;Active;21SHARES HODL BSK ETP OE;CH0445689208;1;2;A3GCRR;21XH;XETR;ETN0;EXCHANGE TRADED NOTES;ETN",
            "Active;Active;MUTARES KGAA BZR;DE000A41YEC7;1;2;A41YEC;MUXA;XETR;SDX1;SDAX;SR",
            "Inactive;Active;OLD EQUITY;US0000000001;1;2;000000;OLD;XETR;PAG_EQU;AUSTRALIEN NEUSEELAND OZEANIEN;CS",
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
            "isin": "US70450Y1038",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "XBLC",
            "name": "XTRACKERS II EUR CORPORATE BOND UCITS ETF",
            "exchange": "XETRA",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "LU0478205379",
            "sector": "Fixed Income",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "SGS1",
            "name": "SGIS O.E. ETC ICE EUA",
            "exchange": "XETRA",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "DE000ETC0001",
            "sector": "Commodity",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "21XH",
            "name": "21SHARES HODL BSK ETP OE",
            "exchange": "XETRA",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "CH0445689208",
            "sector": "Alternative",
        },
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


def test_parse_tmx_listed_issuers_excel_keeps_etfs_subset_scope_but_promotes_stocks(tmp_path) -> None:
    source = MasterfileSource(
        key="tmx_listed_issuers",
        provider="TMX",
        description="Official TSX/TSXV listed issuers workbook",
        source_url="https://example.com/tmx.xlsx",
        format="tmx_listed_issuers_excel",
        reference_scope="listed_companies_subset",
    )
    buffer = io.BytesIO()
    tsx_rows = pd.DataFrame(
        [
            {"Exchange": "TSX", "Name": "Blossom Gold Inc.", "Root\nTicker": "BGAU", "Sector": "Mining"},
            {"Exchange": "TSX", "Name": "First Trust Dow Jones Internet Index ETF", "Root\nTicker": "FDN", "Sector": "ETF"},
        ]
    )
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        tsx_rows.to_excel(writer, sheet_name="TSX Issuers December 2025", startrow=9, index=False)

    rows = parse_tmx_listed_issuers_excel(buffer.getvalue(), source)

    assert rows == [
        {
            "source_key": "tmx_listed_issuers",
            "provider": "TMX",
            "source_url": "https://example.com/tmx.xlsx",
            "ticker": "BGAU",
            "name": "Blossom Gold Inc.",
            "exchange": "TSX",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
        {
            "source_key": "tmx_listed_issuers",
            "provider": "TMX",
            "source_url": "https://example.com/tmx.xlsx",
            "ticker": "FDN",
            "name": "First Trust Dow Jones Internet Index ETF",
            "exchange": "TSX",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
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
                "FFI-UN,Flaherty & Crumrine Investment Grade Preferred Income Fund,TSX,ETF",
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
                "LYUV-U,Lysander-Canso U.S. Corporate Value Bond Fund,TSX,missing_from_official",
                "TKN-U,Ninepoint Web3 Innovators Fund,TSX,reference_gap",
                "FFI-UN,Flaherty & Crumrine Investment Grade Preferred Income Fund,TSX,missing_from_official",
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
        if symbol == "FFI.UN":
            return {
                "symbol": "FFI.UN",
                "name": "Flaherty & Crumrine Investment Grade Fixed Income Fund",
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
        },
        {
            "symbol": "FFI-UN",
            "longname": "Flaherty & Crumrine Investment Grade Fixed Income Fund",
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
                "MBI-H,Med BioGene Inc,TSXV,Stock,",
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
                "MBI-H,Med BioGene Inc,TSXV,missing_from_official",
                "ODV-WTV,ODV-WTV,TSXV,reference_gap",
                "MMX,MMX,TSXV,reference_gap",
            ]
        ),
        encoding="utf-8",
    )

    def fake_fetch_tmx_money_quote(symbol: str, session=None):
        payloads = {
            "ALAB.P": {"symbol": "ALAB.P", "name": "A-Labs Capital II Corp.", "exchangeCode": "CDX"},
            "MBI.H": {"symbol": "MBI.H", "name": "Many Bright Ideas Technologies Inc.", "exchangeCode": "CDX"},
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
            reference_scope="listed_companies_subset",
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
        },
        {
            "source_key": "tmx_listed_issuers",
            "provider": "TMX",
            "source_url": TMX_MONEY_GRAPHQL_URL,
            "ticker": "MBI-H",
            "name": "Many Bright Ideas Technologies Inc.",
            "exchange": "TSXV",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "",
        },
        {
            "source_key": "tmx_listed_issuers",
            "provider": "TMX",
            "source_url": TMX_MONEY_GRAPHQL_URL,
            "ticker": "MMX",
            "name": "Mustang Minerals Limited",
            "exchange": "TSXV",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "US5732841060",
        },
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
            '"AIB GROUP PLC";IE00BF0L3536;A5G;"Euronext Dublin";EUR;9.46;9.578;9.442;9.564;" 21:07";IST;3693893;35243700.07;9.564;',
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
            "isin": "IT0001233417",
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
            "isin": "BMG9156K1018",
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
            "isin": "NL0000000001",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "A5G",
            "name": "AIB GROUP PLC",
            "exchange": "ISE",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "IE00BF0L3536",
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
            "isin": "US88579Y1010",
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
            "isin": "XS2337085422",
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
            "isin": "FR0010411884",
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
            ["BOVA11", "ISHARES IBOV CI", "CASH", "EQUITY-CASH", "ETF EQUITIES", "BRBOVACTF003", "ISHARES IBOVESPA FUNDO DE INDICE"],
            ["KNOX11", "FIP IE KNOX CI", "CASH", "EQUITY-CASH", "FUNDS", "BRKNOXCTF003", "KNOX DEBT FDO. INVEST. PART. INFRAESTRUTURA"],
            ["BEWQ39", "MSCI FRANCE DRE", "CASH", "EQUITY-CASH", "BDR", "BRBEWQBDR009", "ISHARES MSCI FRANCE ETF"],
            ["ATTB34", "ATT INC DRN ED", "CASH", "EQUITY-CASH", "BDR", "BRATTBBDR007", "AT&T INC."],
            ["ADMF3", "", "CASH", "EQUITY-CASH", "", "BRADMFACNOR3", "CIABRASF CIA BRASILEIRA DE SERVIÇOS FINANCEIROS SA"],
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
            "isin": "BRPETRACNPR6",
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
            "isin": "BRBOVACTF003",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "KNOX11",
            "name": "KNOX DEBT FDO. INVEST. PART. INFRAESTRUTURA",
            "exchange": "B3",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "BRKNOXCTF003",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "BEWQ39",
            "name": "ISHARES MSCI FRANCE ETF",
            "exchange": "B3",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "BRBEWQBDR009",
        },
        {
            "source_key": "test",
            "provider": "test",
            "source_url": "https://example.com",
            "ticker": "ADMF3",
            "name": "CIABRASF CIA BRASILEIRA DE SERVIÇOS FINANCEIROS SA",
            "exchange": "B3",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "BRADMFACNOR3",
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


def test_fetch_nasdaq_nordic_iceland_shares_uses_ice_market() -> None:
    source = MasterfileSource(
        key="nasdaq_nordic_iceland_shares",
        provider="Nasdaq Nordic",
        description="Official Iceland shares screener",
        source_url="https://api.nasdaq.com/api/nordic/screener/shares",
        format="nasdaq_nordic_iceland_shares_json",
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

        def get(self, url, params=None, headers=None, timeout=None):
            self.calls.append((url, params))
            assert params in (
                {"category": "MAIN_MARKET", "tableonly": "false", "market": "ICE"},
                {"category": "FIRST_NORTH", "tableonly": "false", "market": "ICE"},
            )
            return FakeResponse(
                {
                    "data": {
                        "instrumentListing": {
                            "rows": [
                                {
                                    "symbol": "ALVO",
                                    "fullName": "Alvotech",
                                    "isin": "LU2458332611",
                                    "sector": "Health Care",
                                }
                            ]
                        }
                    }
                }
            )

    session = FakeSession()
    rows = fetch_nasdaq_nordic_iceland_shares(source, session=session)

    assert {row["ticker"] for row in rows} == {"ALVO"}
    assert rows[0]["exchange"] == "ICE_IS"
    assert rows[0]["isin"] == "LU2458332611"
    assert session.calls == [
        (
            "https://api.nasdaq.com/api/nordic/screener/shares",
            {"category": "MAIN_MARKET", "tableonly": "false", "market": "ICE"},
        ),
        (
            "https://api.nasdaq.com/api/nordic/screener/shares",
            {"category": "FIRST_NORTH", "tableonly": "false", "market": "ICE"},
        ),
    ]


def test_fetch_nasdaq_nordic_share_search_backfills_exact_ticker_gaps(
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
    rows = fetch_nasdaq_nordic_share_search(
        source,
        listings_path=listings_path,
        verification_dir=tmp_path,
        session=session,
    )

    assert rows == sorted(
        [
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
        ],
        key=lambda row: row["ticker"],
    )
    assert session.calls == [
        ("https://api.nasdaq.com/api/nordic/search", {"searchText": "ERIBR"}),
        ("https://api.nasdaq.com/api/nordic/search", {"searchText": "ILKKA1"}),
        ("https://api.nasdaq.com/api/nordic/search", {"searchText": "WITH"}),
    ]


def test_fetch_nasdaq_nordic_stockholm_shares_search_normalizes_official_symbol_format(
    tmp_path, monkeypatch
) -> None:
    source = MasterfileSource(
        key="nasdaq_nordic_stockholm_shares_search",
        provider="Nasdaq Nordic",
        description="Official Stockholm share search supplement",
        source_url="https://api.nasdaq.com/api/nordic/search",
        format="nasdaq_nordic_stockholm_shares_search_json",
        reference_scope="listed_companies_subset",
    )
    listings_path = tmp_path / "listings.csv"
    listings_path.write_text(
        "\n".join(
            [
                "ticker,exchange,asset_type,name,isin",
                "ERIC-B,STO,Stock,Telefonaktiebolaget LM Ericsson Class B,SE0000108656",
                "NOKIA-SEK,STO,Stock,Nokia Oyj,FI0009000681",
                "HOTEL,STO,Stock,LION E-Mobility AG,CH0132594711",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "latest_reference_gap_tickers",
        lambda base_dir, exchanges=None: {"ERIC-B", "NOKIA-SEK", "HOTEL"},
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
                "ERIC-B": {"data": None},
                "ERIC B": {
                    "data": [
                        {
                            "group": "Shares Main Market",
                            "instruments": [
                                {
                                    "symbol": "ERIC B",
                                    "fullName": "Ericsson B",
                                    "isin": "SE0000108656",
                                    "assetClass": "SHARES",
                                    "currency": "SEK",
                                }
                            ],
                        }
                    ]
                },
                "NOKIA-SEK": {"data": None},
                "NOKIA SEK": {
                    "data": [
                        {
                            "group": "Shares Main Market",
                            "instruments": [
                                {
                                    "symbol": "NOKIA SEK",
                                    "fullName": "Nokia Oyj",
                                    "isin": "FI0009000681",
                                    "assetClass": "SHARES",
                                    "currency": "SEK",
                                }
                            ],
                        }
                    ]
                },
                "HOTEL": {
                    "data": [
                        {
                            "group": "Shares Main Market",
                            "instruments": [
                                {
                                    "symbol": "HOTEL",
                                    "fullName": "Hotel Properties Ltd.",
                                    "isin": "SG1DB3000004",
                                    "assetClass": "SHARES",
                                    "currency": "SEK",
                                }
                            ],
                        }
                    ]
                },
            }
            return FakeResponse(payloads[params["searchText"]])

    session = FakeSession()
    rows = fetch_nasdaq_nordic_share_search(
        source,
        listings_path=listings_path,
        verification_dir=tmp_path,
        session=session,
    )

    assert rows == sorted(
        [
            {
                "source_key": "nasdaq_nordic_stockholm_shares_search",
                "provider": "Nasdaq Nordic",
            "source_url": "https://api.nasdaq.com/api/nordic/search",
            "ticker": "ERIC-B",
            "name": "Ericsson B",
            "exchange": "STO",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "SE0000108656",
        },
        {
            "source_key": "nasdaq_nordic_stockholm_shares_search",
            "provider": "Nasdaq Nordic",
            "source_url": "https://api.nasdaq.com/api/nordic/search",
            "ticker": "NOKIA-SEK",
            "name": "Nokia Oyj",
            "exchange": "STO",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
                "official": "true",
                "isin": "FI0009000681",
            },
        ],
        key=lambda row: row["ticker"],
    )
    assert session.calls == [
        ("https://api.nasdaq.com/api/nordic/search", {"searchText": "ERIC-B"}),
        ("https://api.nasdaq.com/api/nordic/search", {"searchText": "ERIC B"}),
        ("https://api.nasdaq.com/api/nordic/search", {"searchText": "NOKIA-SEK"}),
        ("https://api.nasdaq.com/api/nordic/search", {"searchText": "NOKIA SEK"}),
        ("https://api.nasdaq.com/api/nordic/search", {"searchText": "HOTEL"}),
    ]


def test_fetch_nasdaq_nordic_copenhagen_shares_search_normalizes_official_symbol_format(
    tmp_path, monkeypatch
) -> None:
    source = MasterfileSource(
        key="nasdaq_nordic_copenhagen_shares_search",
        provider="Nasdaq Nordic",
        description="Official Copenhagen share search supplement",
        source_url="https://api.nasdaq.com/api/nordic/search",
        format="nasdaq_nordic_copenhagen_shares_search_json",
        reference_scope="listed_companies_subset",
    )
    listings_path = tmp_path / "listings.csv"
    listings_path.write_text(
        "\n".join(
            [
                "ticker,exchange,asset_type,name,isin",
                "AMBU-B,CPH,Stock,Ambu A/S,DK0060946788",
                "BETCO-DKK,CPH,Stock,Better Collective A/S,DK0060952240",
                "GN,CPH,Stock,GN Store Nord A/S,DK0010272632",
                "HOTEL,CPH,Stock,Scandic Hotels Group AB,SE0007640156",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "latest_reference_gap_tickers",
        lambda base_dir, exchanges=None: {"AMBU-B", "BETCO-DKK", "GN", "HOTEL"},
    )

    class FakeResponse:
        def __init__(self, payload, status_code=200):
            self._payload = payload
            self.status_code = status_code

        def raise_for_status(self):
            if self.status_code >= 400:
                raise requests.HTTPError(f"{self.status_code} error")
            return None

        def json(self):
            return self._payload

    class FakeSession:
        def __init__(self):
            self.calls = []

        def get(self, url, params=None, headers=None, timeout=None):
            self.calls.append((url, params))
            payloads = {
                "AMBU-B": {"data": None},
                "AMBU B": {
                    "data": [
                        {
                            "group": "Shares Main Market",
                            "instruments": [
                                {
                                    "symbol": "AMBU B",
                                    "fullName": "Ambu",
                                    "isin": "DK0060946788",
                                    "assetClass": "SHARES",
                                    "currency": "DKK",
                                }
                            ],
                        }
                    ]
                },
                "BETCO-DKK": {"data": None},
                "BETCO DKK": {
                    "data": [
                        {
                            "group": "Shares Main Market",
                            "instruments": [
                                {
                                    "symbol": "BETCO DKK",
                                    "fullName": "Better Collective",
                                    "isin": "DK0060952240",
                                    "assetClass": "SHARES",
                                    "currency": "DKK",
                                }
                            ],
                        }
                    ]
                },
                "GN": {"data": None},
                "GN Store Nord A/S": {
                    "data": [
                        {
                            "group": "Shares Main Market",
                            "instruments": [
                                {
                                    "symbol": "GN",
                                    "fullName": "GN Store Nord",
                                    "isin": "DK0010272632",
                                    "assetClass": "SHARES",
                                    "currency": "DKK",
                                }
                            ],
                        }
                    ]
                },
                "HOTEL": {
                    "data": [
                        {
                            "group": "Shares Main Market",
                            "instruments": [
                                {
                                    "symbol": "HOTEL",
                                    "fullName": "Hotel Properties Ltd.",
                                    "isin": "SG1DB3000004",
                                    "assetClass": "SHARES",
                                    "currency": "DKK",
                                }
                            ],
                        }
                    ]
                },
            }
            if params["searchText"] == "GN":
                return FakeResponse({"data": None}, status_code=400)
            return FakeResponse(payloads[params["searchText"]])

    session = FakeSession()
    rows = fetch_nasdaq_nordic_share_search(
        source,
        listings_path=listings_path,
        verification_dir=tmp_path,
        session=session,
    )

    assert rows == sorted(
        [
            {
                "source_key": "nasdaq_nordic_copenhagen_shares_search",
                "provider": "Nasdaq Nordic",
                "source_url": "https://api.nasdaq.com/api/nordic/search",
                "ticker": "AMBU-B",
                "name": "Ambu",
                "exchange": "CPH",
                "asset_type": "Stock",
                "listing_status": "active",
                "reference_scope": "listed_companies_subset",
                "official": "true",
                "isin": "DK0060946788",
            },
            {
                "source_key": "nasdaq_nordic_copenhagen_shares_search",
                "provider": "Nasdaq Nordic",
                "source_url": "https://api.nasdaq.com/api/nordic/search",
                "ticker": "BETCO-DKK",
                "name": "Better Collective",
                "exchange": "CPH",
                "asset_type": "Stock",
                "listing_status": "active",
                "reference_scope": "listed_companies_subset",
                "official": "true",
                "isin": "DK0060952240",
            },
            {
                "source_key": "nasdaq_nordic_copenhagen_shares_search",
                "provider": "Nasdaq Nordic",
                "source_url": "https://api.nasdaq.com/api/nordic/search",
                "ticker": "GN",
                "name": "GN Store Nord",
                "exchange": "CPH",
                "asset_type": "Stock",
                "listing_status": "active",
                "reference_scope": "listed_companies_subset",
                "official": "true",
                "isin": "DK0010272632",
            },
        ],
        key=lambda row: row["ticker"],
    )
    assert session.calls == [
        ("https://api.nasdaq.com/api/nordic/search", {"searchText": "AMBU-B"}),
        ("https://api.nasdaq.com/api/nordic/search", {"searchText": "AMBU B"}),
        ("https://api.nasdaq.com/api/nordic/search", {"searchText": "BETCO-DKK"}),
        ("https://api.nasdaq.com/api/nordic/search", {"searchText": "BETCO DKK"}),
        ("https://api.nasdaq.com/api/nordic/search", {"searchText": "GN"}),
        ("https://api.nasdaq.com/api/nordic/search", {"searchText": "GN Store Nord A/S"}),
        ("https://api.nasdaq.com/api/nordic/search", {"searchText": "HOTEL"}),
    ]


def test_parse_spotlight_search_heading_extracts_name_and_ticker() -> None:
    name, ticker = parse_spotlight_search_heading(
        "<b><u>ABAS</u></b> Protect (<b><u>ABAS</u></b>)"
    )

    assert name == "ABAS Protect"
    assert ticker == "ABAS"


def test_parse_spotlight_company_title_extracts_name_and_ticker() -> None:
    name, ticker = parse_spotlight_company_title("HomeMaid (HOME B) | Spotlight")

    assert name == "HomeMaid"
    assert ticker == "HOME B"


def test_spotlight_trade_url_and_isin_extraction() -> None:
    detail_url = "https://spotlightstockmarket.com/sv/bolag/irabout?InstrumentId=XSAT01001575"

    assert (
        spotlight_trade_url_from_detail_url(detail_url)
        == "https://spotlightstockmarket.com/sv/bolag/irtrade?InstrumentId=XSAT01001575"
    )
    assert extract_spotlight_detail_isin("XSAT01001575 ISIN-kod SE0008014476") == "SE0008014476"


def test_fetch_spotlight_companies_directory_builds_rows_from_detail_pages() -> None:
    source = MasterfileSource(
        key="spotlight_companies_directory",
        provider="Spotlight",
        description="Official Spotlight companies directory with detail-page symbols",
        source_url="https://spotlightstockmarket.com/Umbraco/api/companyapi/GetCompanies",
        format="spotlight_companies_directory_json",
        reference_scope="listed_companies_subset",
    )

    class FakeResponse:
        def __init__(self, *, payload=None, text=""):
            self._payload = payload
            self.text = text

        def raise_for_status(self):
            return None

        def json(self):
            return self._payload

    class FakeSession:
        def __init__(self):
            self.calls = []

        def get(self, url, headers=None, timeout=None):
            self.calls.append(url)
            if url.endswith("/GetCompanies"):
                return FakeResponse(
                    payload={
                        "results": [
                            {"heading": "HomeMaid AB", "url": "/sv/bolag/irabout?InstrumentId=XSAT01000436"},
                            {"heading": "B Treasury Capital AB", "url": "/sv/bolag/irabout?InstrumentId=XSAT0000413578"},
                        ]
                    }
                )
            pages = {
                "https://spotlightstockmarket.com/sv/bolag/irabout?InstrumentId=XSAT01000436": "<title>HomeMaid (HOME B) | Spotlight</title>",
                "https://spotlightstockmarket.com/sv/bolag/irtrade?InstrumentId=XSAT01000436": "XSAT01000436 ISIN-kod SE0001426131",
                "https://spotlightstockmarket.com/sv/bolag/irabout?InstrumentId=XSAT0000413578": "<title>B Treasury Capital AB (BTC B) | Spotlight</title>",
                "https://spotlightstockmarket.com/sv/bolag/irtrade?InstrumentId=XSAT0000413578": "ISIN SE0025198542",
            }
            return FakeResponse(text=pages[url])

    session = FakeSession()
    rows = fetch_spotlight_companies_directory(source, session=session)

    assert rows == [
        {
            "source_key": "spotlight_companies_directory",
            "provider": "Spotlight",
            "source_url": "https://spotlightstockmarket.com/sv/bolag/irabout?InstrumentId=XSAT01000436",
            "ticker": "HOME-B",
            "name": "HomeMaid",
            "exchange": "STO",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "SE0001426131",
        },
        {
            "source_key": "spotlight_companies_directory",
            "provider": "Spotlight",
            "source_url": "https://spotlightstockmarket.com/sv/bolag/irabout?InstrumentId=XSAT0000413578",
            "ticker": "BTC-B",
            "name": "B Treasury Capital AB",
            "exchange": "STO",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "SE0025198542",
        },
    ]
    assert session.calls == [
        "https://spotlightstockmarket.com/Umbraco/api/companyapi/GetCompanies",
        "https://spotlightstockmarket.com/sv/bolag/irabout?InstrumentId=XSAT01000436",
        "https://spotlightstockmarket.com/sv/bolag/irtrade?InstrumentId=XSAT01000436",
        "https://spotlightstockmarket.com/sv/bolag/irabout?InstrumentId=XSAT0000413578",
        "https://spotlightstockmarket.com/sv/bolag/irtrade?InstrumentId=XSAT0000413578",
    ]


def test_fetch_spotlight_companies_directory_skips_broken_detail_pages() -> None:
    source = MasterfileSource(
        key="spotlight_companies_directory",
        provider="Spotlight",
        description="Official Spotlight companies directory with detail-page symbols",
        source_url="https://spotlightstockmarket.com/Umbraco/api/companyapi/GetCompanies",
        format="spotlight_companies_directory_json",
        reference_scope="listed_companies_subset",
    )

    class FakeResponse:
        def __init__(self, *, payload=None, text=""):
            self._payload = payload
            self.text = text

        def raise_for_status(self):
            return None

        def json(self):
            return self._payload

    class FakeSession:
        def get(self, url, headers=None, timeout=None):
            if url.endswith("/GetCompanies"):
                return FakeResponse(
                    payload={
                        "results": [
                            {"heading": "Broken Co", "url": "/sv/bolag/irabout?InstrumentId=BROKEN"},
                            {"heading": "HomeMaid AB", "url": "/sv/bolag/irabout?InstrumentId=XSAT01000436"},
                        ]
                    }
                )
            if "BROKEN" in url:
                raise requests.RequestException("boom")
            if "irtrade" in url:
                return FakeResponse(text="ISIN-kod SE0001426131")
            return FakeResponse(text="<title>HomeMaid (HOME B) | Spotlight</title>")

    rows = fetch_spotlight_companies_directory(source, session=FakeSession())

    assert rows == [
        {
            "source_key": "spotlight_companies_directory",
            "provider": "Spotlight",
            "source_url": "https://spotlightstockmarket.com/sv/bolag/irabout?InstrumentId=XSAT01000436",
            "ticker": "HOME-B",
            "name": "HomeMaid",
            "exchange": "STO",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "SE0001426131",
        }
    ]


def test_fetch_spotlight_companies_search_backfills_exact_ticker_gaps(
    tmp_path, monkeypatch
) -> None:
    source = MasterfileSource(
        key="spotlight_companies_search",
        provider="Spotlight",
        description="Official Spotlight company search supplement for exact Swedish stock ticker gaps",
        source_url="https://spotlightstockmarket.com/Umbraco/api/companyapi/CompanySimpleSearch",
        format="spotlight_companies_search_json",
        reference_scope="listed_companies_subset",
    )
    listings_path = tmp_path / "listings.csv"
    listings_path.write_text(
        "\n".join(
            [
                "ticker,exchange,asset_type,name,isin",
                "ABAS,STO,Stock,ABAS Protect AB,SE0018588659",
                "BIOWKS,STO,Stock,Bio-Works Technologies AB,SE0017563232",
                "ASTOR,STO,Stock,Scandinavian Astor Group AB,SE0019175274",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "latest_reference_gap_tickers",
        lambda base_dir, exchanges=None: {"ABAS", "BIOWKS", "ASTOR"},
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
                "ABAS": {
                    "results": [
                        {"heading": "<b><u>ABAS</u></b> Protect (<b><u>ABAS</u></b>)"},
                    ]
                },
                "BIOWKS": {
                    "results": [
                        {"heading": "Bio-Works Technologies (<b><u>BIOWKS</u></b>)"},
                        {"heading": "Bio-Works Technologies TO 2 (<b><u>BIOWKS</u></b> TO 2)"},
                    ]
                },
                "ASTOR": {"results": []},
            }
            return FakeResponse(payloads[params["searchText"]])

    session = FakeSession()
    rows = fetch_spotlight_companies_search(
        source,
        listings_path=listings_path,
        verification_dir=tmp_path,
        session=session,
    )

    assert rows == [
        {
            "source_key": "spotlight_companies_search",
            "provider": "Spotlight",
            "source_url": "https://spotlightstockmarket.com/Umbraco/api/companyapi/CompanySimpleSearch",
            "ticker": "ABAS",
            "name": "ABAS Protect",
            "exchange": "STO",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "SE0018588659",
        },
        {
            "source_key": "spotlight_companies_search",
            "provider": "Spotlight",
            "source_url": "https://spotlightstockmarket.com/Umbraco/api/companyapi/CompanySimpleSearch",
            "ticker": "BIOWKS",
            "name": "Bio-Works Technologies",
            "exchange": "STO",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "SE0017563232",
        },
    ]
    assert session.calls == [
        ("https://spotlightstockmarket.com/Umbraco/api/companyapi/CompanySimpleSearch", {"searchText": "ABAS"}),
        ("https://spotlightstockmarket.com/Umbraco/api/companyapi/CompanySimpleSearch", {"searchText": "BIOWKS"}),
        ("https://spotlightstockmarket.com/Umbraco/api/companyapi/CompanySimpleSearch", {"searchText": "ASTOR"}),
    ]


def test_parse_ngm_companies_page_html_maps_primary_symbols() -> None:
    source = MasterfileSource(
        key="ngm_companies_page",
        provider="NGM",
        description="Official Nordic Growth Market companies page",
        source_url="https://www.ngm.se/en/our-companies/",
        format="ngm_companies_page_html",
        reference_scope="listed_companies_subset",
    )
    payload = html.escape(
        json.dumps(
            [
                {
                    "title": "Arbona AB",
                    "symbols": [
                        {"symbol": "ARBO A", "is_primary": True},
                        {"symbol": "ARBO TO 1", "is_primary": False},
                    ],
                },
                {
                    "title": "Front Ventures AB",
                    "symbols": [{"symbol": "FRNT B", "is_primary": True}],
                },
                {"title": "Empty Co", "symbols": []},
            ]
        ),
        quote=True,
    )
    text = f'<html><body><company-list :items="{payload}" /></body></html>'

    rows = parse_ngm_companies_page_html(text, source)

    assert rows == [
        {
            "source_key": "ngm_companies_page",
            "provider": "NGM",
            "source_url": "https://www.ngm.se/en/our-companies/",
            "ticker": "ARBO-A",
            "name": "Arbona AB",
            "exchange": "STO",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
        },
        {
            "source_key": "ngm_companies_page",
            "provider": "NGM",
            "source_url": "https://www.ngm.se/en/our-companies/",
            "ticker": "FRNT-B",
            "name": "Front Ventures AB",
            "exchange": "STO",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
        },
    ]


def test_fetch_ngm_detailview_isin_posts_official_gwt_payload() -> None:
    class FakeResponse:
        text = '//OK["SE0000598278","AIK Fotboll","instrumentID","2X7"]'

        def raise_for_status(self):
            return None

    class FakeSession:
        def __init__(self):
            self.calls = []

        def post(self, url, data=None, headers=None, timeout=None):
            self.calls.append((url, data.decode("utf-8"), headers, timeout))
            return FakeResponse()

    session = FakeSession()
    isin = fetch_exchange_masterfiles.fetch_ngm_detailview_isin("2X7", session=session)

    assert isin == "SE0000598278"
    assert session.calls == [
        (
            "https://mdweb.ngm.se/MDWebFront/detailview/service",
            (
                "7|0|8|https://mdweb.ngm.se/MDWebFront/detailview/|"
                "9CB8691CD75E82471063725CB524FFF9|"
                "se.ngm.mdweb.front.client.rpc.SearchRPCService|findInstrument|"
                "java.lang.String/2004016611|Z|2X7|en_US|"
                "1|2|3|4|5|5|5|5|5|6|7|0|0|8|0|"
            ),
            {
                "User-Agent": fetch_exchange_masterfiles.USER_AGENT,
                "Content-Type": "text/x-gwt-rpc; charset=UTF-8",
                "Referer": "https://mdweb.ngm.se/",
                "X-GWT-Module-Base": "https://mdweb.ngm.se/MDWebFront/detailview/",
                "X-GWT-Permutation": "E66E1EEBE9B1CEBA056C88011CCBF307",
            },
            fetch_exchange_masterfiles.REQUEST_TIMEOUT,
        )
    ]


def test_fetch_ngm_companies_page_parses_official_page() -> None:
    source = MasterfileSource(
        key="ngm_companies_page",
        provider="NGM",
        description="Official Nordic Growth Market companies page",
        source_url="https://www.ngm.se/en/our-companies/",
        format="ngm_companies_page_html",
        reference_scope="listed_companies_subset",
    )
    payload = html.escape(
        json.dumps(
                [
                    {
                        "title": "AIK Fotboll",
                        "symbols": [{"symbol": "AIK B", "is_primary": True, "order_book_id": "2X7"}],
                    }
                ]
            ),
            quote=True,
        )

    class FakeResponse:
        def __init__(self, text):
            self.text = text

        def raise_for_status(self):
            return None

    class FakeSession:
        def __init__(self):
            self.calls = []

        def get(self, url, headers=None, timeout=None):
            self.calls.append(("GET", url, timeout))
            return FakeResponse(f'<company-list :items="{payload}" />')

        def post(self, url, data=None, headers=None, timeout=None):
            self.calls.append(("POST", url, data.decode("utf-8"), timeout))
            return FakeResponse('//OK["SE0000598278","AIK Fotboll","instrumentID","2X7"]')

    session = FakeSession()
    rows = fetch_ngm_companies_page(source, session=session)

    assert rows == [
        {
            "source_key": "ngm_companies_page",
                "provider": "NGM",
                "source_url": "https://www.ngm.se/en/our-companies/",
                "ticker": "AIK-B",
                "name": "AIK Fotboll",
                "exchange": "STO",
                "asset_type": "Stock",
                "listing_status": "active",
                "reference_scope": "listed_companies_subset",
                "official": "true",
                "isin": "SE0000598278",
            }
        ]
    assert session.calls == [
        ("GET", "https://www.ngm.se/en/our-companies/", fetch_exchange_masterfiles.REQUEST_TIMEOUT),
        (
            "POST",
            "https://mdweb.ngm.se/MDWebFront/detailview/service",
            (
                "7|0|8|https://mdweb.ngm.se/MDWebFront/detailview/|"
                "9CB8691CD75E82471063725CB524FFF9|"
                "se.ngm.mdweb.front.client.rpc.SearchRPCService|findInstrument|"
                "java.lang.String/2004016611|Z|2X7|en_US|"
                "1|2|3|4|5|5|5|5|5|6|7|0|0|8|0|"
            ),
            fetch_exchange_masterfiles.REQUEST_TIMEOUT,
        ),
    ]


def test_parse_ngm_market_data_equities_maps_symbols_and_isins() -> None:
    source = MasterfileSource(
        key="ngm_market_data_equities",
        provider="NGM",
        description="Official Nordic Growth Market market-data supplement for active equities",
        source_url="https://mdapi.ngm.se/api/beta/web/",
        format="ngm_market_data_equities_json",
        reference_scope="listed_companies_subset",
    )

    rows = parse_ngm_market_data_equities(
        [
            {
                "symbol": "ECC B",
                "name": "Ecoclime Group B",
                "isin": "SE0012729937",
                "currency": "SEK",
                "cfi": "ESVUFR",
            },
            {
                "symbol": "BULL TEST",
                "name": "BULL TEST",
                "isin": "DE0000000001",
                "currency": "SEK",
                "cfi": "RFXXXX",
            },
            {
                "symbol": "NOKIA",
                "name": "Nokia Oyj",
                "isin": "FI0009000681",
                "currency": "EUR",
                "cfi": "ESVUFR",
            },
        ],
        source,
        source_url="https://mdapi.ngm.se/api/beta/web/security_statistics/winners?cfi=ESVUFR",
    )

    assert rows == [
        {
            "source_key": "ngm_market_data_equities",
            "provider": "NGM",
            "source_url": "https://mdapi.ngm.se/api/beta/web/security_statistics/winners?cfi=ESVUFR",
            "ticker": "ECC-B",
            "name": "Ecoclime Group B",
            "exchange": "STO",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "SE0012729937",
        }
    ]


def test_fetch_ngm_market_data_equities_combines_official_endpoints() -> None:
    source = MasterfileSource(
        key="ngm_market_data_equities",
        provider="NGM",
        description="Official Nordic Growth Market market-data supplement for active equities",
        source_url="https://mdapi.ngm.se/api/beta/web/",
        format="ngm_market_data_equities_json",
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

        def get(self, url, headers=None, timeout=None):
            self.calls.append(url)
            payloads = {
                "last_trades?cfi=ESVUFR": [
                    {
                        "symbol": "ARGO",
                        "name": "Argo Defence Group",
                        "isin": "SE0026820540",
                        "currency": "SEK",
                        "cfi": "ESVUFR",
                    }
                ],
                "security_statistics/highest_turnover?cfi=ESVUFR": [
                    {
                        "symbol": "ARGO",
                        "name": "Argo Defence Group",
                        "isin": "SE0026820540",
                        "currency": "SEK",
                        "cfi": "ESVUFR",
                    },
                    {
                        "symbol": "ASTOR",
                        "name": "Scandinavian Astor Group",
                        "isin": "SE0019175274",
                        "currency": "SEK",
                        "cfi": "ESVUFR",
                    },
                ],
                "security_statistics/winners?cfi=ESVUFR": [],
                "security_statistics/losers?cfi=ESVUFR": [],
            }
            return FakeResponse(payloads[url.split("/web/", 1)[1]])

    session = FakeSession()
    rows = fetch_ngm_market_data_equities(source, session=session)

    assert [row["ticker"] for row in rows] == ["ARGO", "ASTOR"]
    assert rows[0]["isin"] == "SE0026820540"
    assert rows[1]["source_url"] == (
        "https://mdapi.ngm.se/api/beta/web/security_statistics/highest_turnover?cfi=ESVUFR"
    )
    assert session.calls == [
        "https://mdapi.ngm.se/api/beta/web/last_trades?cfi=ESVUFR",
        "https://mdapi.ngm.se/api/beta/web/security_statistics/highest_turnover?cfi=ESVUFR",
        "https://mdapi.ngm.se/api/beta/web/security_statistics/winners?cfi=ESVUFR",
        "https://mdapi.ngm.se/api/beta/web/security_statistics/losers?cfi=ESVUFR",
    ]


def test_fetch_bmv_stock_search_backfills_exact_ticker_gaps(tmp_path, monkeypatch) -> None:
    source = MasterfileSource(
        key="bmv_stock_search",
        provider="BMV",
        description="Official BMV stock search supplement",
        source_url="https://www.bmv.com.mx/api/searchservice/v1",
        format="bmv_stock_search_json",
        reference_scope="listed_companies_subset",
    )
    listings_path = tmp_path / "listings.csv"
    listings_path.write_text(
        "\n".join(
            [
                "ticker,exchange,asset_type,name,isin",
                "AMXB,BMV,Stock,AMERICA MOVIL SAB DE CV,MX01AM050019",
                "WALMEX,BMV,Stock,Wal-Mart de Mexico S.A.B. de C.V,MX01WA000038",
                "AXAN,BMV,Stock,AXA SA,FR0000120628",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "latest_reference_gap_tickers",
        lambda base_dir, exchanges=None: {"AMXB", "WALMEX", "AXAN"},
    )

    class FakeResponse:
        def __init__(self, *, text: str | None = None, payload: object | None = None):
            self.text = text or ""
            self._payload = payload

        def raise_for_status(self):
            return None

        def json(self):
            return self._payload

    class FakeSession:
        def __init__(self):
            self.get_calls = []
            self.post_calls = []

        def get(self, url, params=None, headers=None, timeout=None):
            self.get_calls.append((url, params))
            if url == fetch_exchange_masterfiles.BMV_MOBILE_QUOTE_KEYS_URL:
                assert params == {"idBusquedaCotizacion": 2}
                return FakeResponse(
                    text=(
                        'for(;;);({"response":{"clavesCotizacion":['
                        '{"clave":"AMX","serie":"B"},'
                        '{"clave":"WALMEX","serie":"*"},'
                        '{"clave":"AXA","serie":"N"}'
                        ']}})'
                    )
                )
            if url == fetch_exchange_masterfiles.BMV_SEARCH_TOKEN_URL:
                return FakeResponse(payload={"response": {"access_token": "token-123"}})
            raise AssertionError(url)

        def post(self, url, headers=None, json=None, timeout=None):
            self.post_calls.append((url, json))
            assert url == "https://www.bmv.com.mx/api/searchservice/v1"
            term = json["payload"]["term"]
            payloads = {
                "AMX": {
                    "response": {
                        "busquedaPanel": {
                            "busquedaGeneral": {
                                "instrumentosEmisoras": {
                                    "instrumentos": {
                                        "coincidenciaParcialInstrumentos": {
                                            "emisoras": {
                                                "hits": [
                                                    {
                                                        "_source": {
                                                            "cve_emisora": "AMX",
                                                            "serie": "B",
                                                            "descripcion": "ACCIONES",
                                                            "mercado": "Capitales",
                                                            "estatus": "Activa",
                                                            "razon_social": "AMERICA MOVIL, S.A.B. DE C.V.",
                                                        }
                                                    },
                                                    {
                                                        "_source": {
                                                            "cve_emisora": "AMX",
                                                            "serie": "00926",
                                                            "descripcion": "CERTIFICADO BURSATIL",
                                                            "mercado": "Deuda",
                                                            "estatus": "Activa",
                                                            "razon_social": "AMERICA MOVIL, S.A.B. DE C.V.",
                                                        }
                                                    },
                                                ]
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                "WALMEX": {
                    "response": {
                        "busquedaPanel": {
                            "busquedaGeneral": {
                                "instrumentosEmisoras": {
                                    "instrumentos": {
                                        "coincidenciaParcialInstrumentos": {
                                            "emisoras": {
                                                "hits": [
                                                    {
                                                        "_source": {
                                                            "cve_emisora": "WALMEX",
                                                            "serie": "*",
                                                            "descripcion": "ACCIONES",
                                                            "mercado": "Capitales",
                                                            "estatus": "Activa",
                                                            "razon_social": "WAL - MART DE MEXICO, S.A.B. DE C.V.",
                                                        }
                                                    }
                                                ]
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                "AXA": {
                    "response": {
                        "busquedaPanel": {
                            "busquedaGeneral": {
                                "instrumentosEmisoras": {
                                    "instrumentos": {
                                        "coincidenciaParcialInstrumentos": {
                                            "emisoras": {
                                                "hits": [
                                                    {
                                                        "_source": {
                                                            "cve_emisora": "AXA",
                                                            "serie": "N",
                                                            "descripcion": "ACCIONES SISTEMA INTERNACIONAL DE COTIZACIONES",
                                                            "mercado": "Global",
                                                            "estatus": "Activa",
                                                            "razon_social": "AXA SA",
                                                        }
                                                    },
                                                    {
                                                        "_source": {
                                                            "cve_emisora": "AXAIM29",
                                                            "serie": "BF",
                                                            "descripcion": "ACCIONES SOCIEDADES DE INVERSION RENTA VARIABLE",
                                                            "mercado": "Capitales",
                                                            "estatus": "Activa",
                                                            "razon_social": "CICLO DE VIDA 2029",
                                                        }
                                                    },
                                                ]
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
            }
            return FakeResponse(payload=payloads[term])

    session = FakeSession()
    rows = fetch_bmv_stock_search(
        source,
        listings_path=listings_path,
        verification_dir=tmp_path,
        session=session,
    )

    assert rows == [
        {
            "source_key": "bmv_stock_search",
            "provider": "BMV",
            "source_url": "https://www.bmv.com.mx/api/searchservice/v1",
            "ticker": "AMXB",
            "name": "AMERICA MOVIL, S.A.B. DE C.V.",
            "exchange": "BMV",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "MX01AM050019",
        },
        {
            "source_key": "bmv_stock_search",
            "provider": "BMV",
            "source_url": "https://www.bmv.com.mx/api/searchservice/v1",
            "ticker": "WALMEX",
            "name": "WAL - MART DE MEXICO, S.A.B. DE C.V.",
            "exchange": "BMV",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "MX01WA000038",
        },
        {
            "source_key": "bmv_stock_search",
            "provider": "BMV",
            "source_url": "https://www.bmv.com.mx/api/searchservice/v1",
            "ticker": "AXAN",
            "name": "AXA SA",
            "exchange": "BMV",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "FR0000120628",
        },
    ]
    assert session.get_calls == [
        (fetch_exchange_masterfiles.BMV_MOBILE_QUOTE_KEYS_URL, {"idBusquedaCotizacion": 2}),
        (fetch_exchange_masterfiles.BMV_SEARCH_TOKEN_URL, None),
    ]
    assert session.post_calls == [
        ("https://www.bmv.com.mx/api/searchservice/v1", {"lang": "es", "payload": {"term": "AMX", "term2": "", "termT": "AMX", "searchType": "busquedaPanel"}}),
        ("https://www.bmv.com.mx/api/searchservice/v1", {"lang": "es", "payload": {"term": "WALMEX", "term2": "", "termT": "WALMEX", "searchType": "busquedaPanel"}}),
        ("https://www.bmv.com.mx/api/searchservice/v1", {"lang": "es", "payload": {"term": "AXA", "term2": "", "termT": "AXA", "searchType": "busquedaPanel"}}),
    ]


def test_fetch_bmv_stock_search_uses_unique_strong_ticker_search_fallback(tmp_path, monkeypatch) -> None:
    source = MasterfileSource(
        key="bmv_stock_search",
        provider="BMV",
        description="Official BMV stock search supplement for exact ticker gaps",
        source_url="https://www.bmv.com.mx/api/searchservice/v1",
        format="bmv_stock_search_json",
        reference_scope="listed_companies_subset",
    )
    listings_path = tmp_path / "listings.csv"
    listings_path.write_text(
        "\n".join(
            [
                "ticker,exchange,asset_type,name,isin",
                "AEROMEX,BMV,Stock,Grupo Aeroméxico S.A.B. de C.V,MX01AE010005",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "latest_reference_gap_tickers",
        lambda base_dir, exchanges=None: {"AEROMEX"},
    )

    class FakeResponse:
        def __init__(self, *, text: str | None = None, payload: object | None = None):
            self.text = text or ""
            self._payload = payload

        def raise_for_status(self):
            return None

        def json(self):
            return self._payload

    class FakeSession:
        def __init__(self):
            self.get_calls = []
            self.post_calls = []

        def get(self, url, params=None, headers=None, timeout=None):
            self.get_calls.append((url, params))
            if url == fetch_exchange_masterfiles.BMV_MOBILE_QUOTE_KEYS_URL:
                return FakeResponse(
                    text='for(;;);({"response":{"clavesCotizacion":[{"clave":"AMX","serie":"B"}]}})'
                )
            if url == fetch_exchange_masterfiles.BMV_SEARCH_TOKEN_URL:
                return FakeResponse(payload={"response": {"access_token": "token-123"}})
            raise AssertionError(url)

        def post(self, url, headers=None, json=None, timeout=None):
            self.post_calls.append((url, json))
            assert url == "https://www.bmv.com.mx/api/searchservice/v1"
            assert json == {
                "lang": "es",
                "payload": {
                    "term": "AEROMEX",
                    "term2": "",
                    "termT": "AEROMEX",
                    "searchType": "busquedaPanel",
                },
            }
            return FakeResponse(
                payload={
                    "response": {
                        "busquedaPanel": {
                            "busquedaGeneral": {
                                "instrumentosEmisoras": {
                                    "instrumentos": {
                                        "coincidenciaParcialInstrumentos": {
                                            "emisoras": {
                                                "hits": [
                                                    {
                                                        "_source": {
                                                            "cve_emisora": "AERO",
                                                            "serie": "*",
                                                            "descripcion": "ACCIONES",
                                                            "mercado": "Capitales",
                                                            "estatus": "Activa",
                                                            "razon_social": "GRUPO AEROMÉXICO, S.A.B. DE C.V.",
                                                        }
                                                    }
                                                ]
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            )

    session = FakeSession()
    rows = fetch_bmv_stock_search(
        source,
        listings_path=listings_path,
        verification_dir=tmp_path,
        session=session,
    )

    assert rows == [
        {
            "source_key": "bmv_stock_search",
            "provider": "BMV",
            "source_url": "https://www.bmv.com.mx/api/searchservice/v1",
            "ticker": "AEROMEX",
            "name": "GRUPO AEROMÉXICO, S.A.B. DE C.V.",
            "exchange": "BMV",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "MX01AE010005",
        }
    ]
    assert session.get_calls == [
        (fetch_exchange_masterfiles.BMV_MOBILE_QUOTE_KEYS_URL, {"idBusquedaCotizacion": 2}),
        (fetch_exchange_masterfiles.BMV_SEARCH_TOKEN_URL, None),
    ]
    assert session.post_calls == [
        (
            "https://www.bmv.com.mx/api/searchservice/v1",
            {
                "lang": "es",
                "payload": {
                    "term": "AEROMEX",
                    "term2": "",
                    "termT": "AEROMEX",
                    "searchType": "busquedaPanel",
                },
            },
        )
    ]


def test_bmv_stock_search_terms_expand_hyphenated_series_suffixes() -> None:
    assert fetch_exchange_masterfiles.bmv_stock_search_terms("LASITEB-1") == [
        "LASITEB-1",
        "LASITEB",
        "LASITE",
    ]


def test_bmv_stock_search_terms_expand_trailing_numeric_suffixes() -> None:
    assert fetch_exchange_masterfiles.bmv_stock_search_terms("FNOVA17") == [
        "FNOVA17",
        "FNOVA",
    ]


def test_fetch_bmv_stock_search_accepts_suspended_and_root_series_matches(tmp_path, monkeypatch) -> None:
    source = MasterfileSource(
        key="bmv_stock_search",
        provider="BMV",
        description="Official BMV stock search supplement for suspended and root-series matches",
        source_url="https://www.bmv.com.mx/api/searchservice/v1",
        format="bmv_stock_search_json",
        reference_scope="listed_companies_subset",
    )
    listings_path = tmp_path / "listings.csv"
    listings_path.write_text(
        "\n".join(
            [
                "ticker,exchange,asset_type,name,isin",
                "GFAMSAA,BMV,Stock,Grupo Famsa S.A.B. de C.V,MX01GF010008",
                "SAREB,BMV,Stock,Sare Holding S.A.B. de C.V,MX01SA030007",
                "UNIFINA,BMV,Stock,Unifin Financiera S. A. B. de C. V,MX00UN000002",
                "LASITEB-1,BMV,Stock,Sitios Latinoamérica S.A.B. de C.V.,",
                "FNOVA17,BMV,Stock,Banco Actinver S.A. Institución de Banca Múltiple Grupo Financiero Actinver,",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "latest_reference_gap_tickers",
        lambda base_dir, exchanges=None: {"GFAMSAA", "SAREB", "UNIFINA", "LASITEB-1", "FNOVA17"},
    )

    class FakeResponse:
        def __init__(self, *, text: str | None = None, payload: object | None = None):
            self.text = text or ""
            self._payload = payload

        def raise_for_status(self):
            return None

        def json(self):
            return self._payload

    class FakeSession:
        def __init__(self):
            self.get_calls = []
            self.post_calls = []

        def get(self, url, params=None, headers=None, timeout=None):
            self.get_calls.append((url, params))
            if url == fetch_exchange_masterfiles.BMV_MOBILE_QUOTE_KEYS_URL:
                return FakeResponse(text='for(;;);({"response":{"clavesCotizacion":[]}})')
            if url == fetch_exchange_masterfiles.BMV_SEARCH_TOKEN_URL:
                return FakeResponse(payload={"response": {"access_token": "token-123"}})
            raise AssertionError(url)

        def post(self, url, headers=None, json=None, timeout=None):
            self.post_calls.append((url, json))
            term = json["payload"]["term"]
            payloads = {
                "GFAMSAA": {"response": {"busquedaPanel": {"busquedaGeneral": {"instrumentosEmisoras": {"instrumentos": {"coincidenciaParcialInstrumentos": {"emisoras": {"hits": []}}}}}}}},
                "GFAMSA": {
                    "response": {
                        "busquedaPanel": {
                            "busquedaGeneral": {
                                "instrumentosEmisoras": {
                                    "instrumentos": {
                                        "coincidenciaParcialInstrumentos": {
                                            "emisoras": {
                                                "hits": [
                                                    {
                                                        "_source": {
                                                            "cve_emisora": "GFAMSA",
                                                            "serie": "A",
                                                            "descripcion": "ACCIONES",
                                                            "mercado": "Capitales",
                                                            "estatus": "Suspendida",
                                                            "razon_social": "GRUPO FAMSA, S.A.B. DE C.V.",
                                                        }
                                                    }
                                                ]
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                "SAREB": {"response": {"busquedaPanel": {"busquedaGeneral": {"instrumentosEmisoras": {"instrumentos": {"coincidenciaParcialInstrumentos": {"emisoras": {"hits": []}}}}}}}},
                "SARE": {
                    "response": {
                        "busquedaPanel": {
                            "busquedaGeneral": {
                                "instrumentosEmisoras": {
                                    "instrumentos": {
                                        "coincidenciaParcialInstrumentos": {
                                            "emisoras": {
                                                "hits": [
                                                    {
                                                        "_source": {
                                                            "cve_emisora": "SARE",
                                                            "serie": "B",
                                                            "descripcion": "ACCIONES",
                                                            "mercado": "Capitales",
                                                            "estatus": "Suspendida",
                                                            "razon_social": "SARE HOLDING, S.A.B. DE C.V.",
                                                        }
                                                    }
                                                ]
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                "UNIFINA": {"response": {"busquedaPanel": {"busquedaGeneral": {"instrumentosEmisoras": {"instrumentos": {"coincidenciaParcialInstrumentos": {"emisoras": {"hits": []}}}}}}}},
                "UNIFIN": {
                    "response": {
                        "busquedaPanel": {
                            "busquedaGeneral": {
                                "instrumentosEmisoras": {
                                    "instrumentos": {
                                        "coincidenciaParcialInstrumentos": {
                                            "emisoras": {
                                                "hits": [
                                                    {
                                                        "_source": {
                                                            "cve_emisora": "UNIFIN",
                                                            "serie": "A",
                                                            "descripcion": "ACCIONES SEGUROS, FIANZAS Y ORGANIZACIONES AUXILIARES DE CREDITO",
                                                            "mercado": "Capitales",
                                                            "estatus": "Suspendida",
                                                            "razon_social": "UNIFIN FINANCIERA, S.A.B. DE C.V.",
                                                        }
                                                    }
                                                ]
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                "LASITEB-1": {"response": {"busquedaPanel": {"busquedaGeneral": {"instrumentosEmisoras": {"instrumentos": {"coincidenciaParcialInstrumentos": {"emisoras": {"hits": []}}}}}}}},
                "LASITEB": {"response": {"busquedaPanel": {"busquedaGeneral": {"instrumentosEmisoras": {"instrumentos": {"coincidenciaParcialInstrumentos": {"emisoras": {"hits": []}}}}}}}},
                "LASITE": {
                    "response": {
                        "busquedaPanel": {
                            "busquedaGeneral": {
                                "instrumentosEmisoras": {
                                    "instrumentos": {
                                        "coincidenciaParcialInstrumentos": {
                                            "emisoras": {
                                                "hits": [
                                                    {
                                                        "_source": {
                                                            "cve_emisora": "LASITE",
                                                            "serie": "*",
                                                            "descripcion": "ACCIONES",
                                                            "mercado": "Capitales",
                                                            "estatus": "Activa",
                                                            "razon_social": "SITIOS LATINOAMÉRICA, S.A.B. DE C.V.",
                                                        }
                                                    }
                                                ]
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                "FNOVA17": {"response": {"busquedaPanel": {"busquedaGeneral": {"instrumentosEmisoras": {"instrumentos": {"coincidenciaParcialInstrumentos": {"emisoras": {"hits": []}}}}}}}},
                "FNOVA": {
                    "response": {
                        "busquedaPanel": {
                            "busquedaGeneral": {
                                "instrumentosEmisoras": {
                                    "instrumentos": {
                                        "coincidenciaParcialInstrumentos": {
                                            "emisoras": {
                                                "hits": [
                                                    {
                                                        "_source": {
                                                            "cve_emisora": "FNOVA",
                                                            "serie": None,
                                                            "descripcion": None,
                                                            "mercado": None,
                                                            "estatus": None,
                                                            "razon_social": "BANCO ACTINVER, S.A. INSTITUCION DE BANCA MULTIPLE, GRUPO FINANCIERO ACTINVER",
                                                        }
                                                    }
                                                ]
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
            }
            return FakeResponse(payload=payloads[term])

    session = FakeSession()
    rows = fetch_bmv_stock_search(
        source,
        listings_path=listings_path,
        verification_dir=tmp_path,
        session=session,
    )

    assert rows == [
        {
            "source_key": "bmv_stock_search",
            "provider": "BMV",
            "source_url": "https://www.bmv.com.mx/api/searchservice/v1",
            "ticker": "GFAMSAA",
            "name": "GRUPO FAMSA, S.A.B. DE C.V.",
            "exchange": "BMV",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "MX01GF010008",
        },
        {
            "source_key": "bmv_stock_search",
            "provider": "BMV",
            "source_url": "https://www.bmv.com.mx/api/searchservice/v1",
            "ticker": "SAREB",
            "name": "SARE HOLDING, S.A.B. DE C.V.",
            "exchange": "BMV",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "MX01SA030007",
        },
        {
            "source_key": "bmv_stock_search",
            "provider": "BMV",
            "source_url": "https://www.bmv.com.mx/api/searchservice/v1",
            "ticker": "UNIFINA",
            "name": "UNIFIN FINANCIERA, S.A.B. DE C.V.",
            "exchange": "BMV",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "MX00UN000002",
        },
        {
            "source_key": "bmv_stock_search",
            "provider": "BMV",
            "source_url": "https://www.bmv.com.mx/api/searchservice/v1",
            "ticker": "LASITEB-1",
            "name": "SITIOS LATINOAMÉRICA, S.A.B. DE C.V.",
            "exchange": "BMV",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
        },
        {
            "source_key": "bmv_stock_search",
            "provider": "BMV",
            "source_url": "https://www.bmv.com.mx/api/searchservice/v1",
            "ticker": "FNOVA17",
            "name": "BANCO ACTINVER, S.A. INSTITUCION DE BANCA MULTIPLE, GRUPO FINANCIERO ACTINVER",
            "exchange": "BMV",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
        },
    ]
    assert session.get_calls == [
        (fetch_exchange_masterfiles.BMV_MOBILE_QUOTE_KEYS_URL, {"idBusquedaCotizacion": 2}),
        (fetch_exchange_masterfiles.BMV_SEARCH_TOKEN_URL, None),
    ]
    assert session.post_calls == [
        ("https://www.bmv.com.mx/api/searchservice/v1", {"lang": "es", "payload": {"term": "GFAMSAA", "term2": "", "termT": "GFAMSAA", "searchType": "busquedaPanel"}}),
        ("https://www.bmv.com.mx/api/searchservice/v1", {"lang": "es", "payload": {"term": "GFAMSA", "term2": "", "termT": "GFAMSA", "searchType": "busquedaPanel"}}),
        ("https://www.bmv.com.mx/api/searchservice/v1", {"lang": "es", "payload": {"term": "SAREB", "term2": "", "termT": "SAREB", "searchType": "busquedaPanel"}}),
        ("https://www.bmv.com.mx/api/searchservice/v1", {"lang": "es", "payload": {"term": "SARE", "term2": "", "termT": "SARE", "searchType": "busquedaPanel"}}),
        ("https://www.bmv.com.mx/api/searchservice/v1", {"lang": "es", "payload": {"term": "UNIFINA", "term2": "", "termT": "UNIFINA", "searchType": "busquedaPanel"}}),
        ("https://www.bmv.com.mx/api/searchservice/v1", {"lang": "es", "payload": {"term": "UNIFIN", "term2": "", "termT": "UNIFIN", "searchType": "busquedaPanel"}}),
        ("https://www.bmv.com.mx/api/searchservice/v1", {"lang": "es", "payload": {"term": "LASITEB-1", "term2": "", "termT": "LASITEB-1", "searchType": "busquedaPanel"}}),
        ("https://www.bmv.com.mx/api/searchservice/v1", {"lang": "es", "payload": {"term": "LASITEB", "term2": "", "termT": "LASITEB", "searchType": "busquedaPanel"}}),
        ("https://www.bmv.com.mx/api/searchservice/v1", {"lang": "es", "payload": {"term": "LASITE", "term2": "", "termT": "LASITE", "searchType": "busquedaPanel"}}),
        ("https://www.bmv.com.mx/api/searchservice/v1", {"lang": "es", "payload": {"term": "FNOVA17", "term2": "", "termT": "FNOVA17", "searchType": "busquedaPanel"}}),
        ("https://www.bmv.com.mx/api/searchservice/v1", {"lang": "es", "payload": {"term": "FNOVA", "term2": "", "termT": "FNOVA", "searchType": "busquedaPanel"}}),
    ]


def test_fetch_bmv_capital_trust_search_backfills_exact_ticker_gaps(tmp_path, monkeypatch) -> None:
    source = MasterfileSource(
        key="bmv_capital_trust_search",
        provider="BMV",
        description="Official BMV capital-market trust supplement",
        source_url="https://www.bmv.com.mx/api/searchservice/v1",
        format="bmv_capital_trust_search_json",
        reference_scope="listed_companies_subset",
    )
    listings_path = tmp_path / "listings.csv"
    listings_path.write_text(
        "\n".join(
            [
                "ticker,exchange,asset_type,name,isin",
                "DANHOS13,BMV,Stock,Fibra Danhos,MXCFDA020005",
                "FCFE18,BMV,Stock,CFECapital,MXFEFC0C0003",
                "FPLUS16,BMV,Stock,Fibra Plus,",
                "COXA,BMV,Stock,Cox Energy America SAB de CV,MX01CX0A0002",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "latest_reference_gap_tickers",
        lambda base_dir, exchanges=None: {"DANHOS13", "FCFE18", "FPLUS16", "COXA"},
    )

    class FakeResponse:
        def __init__(self, *, text: str | None = None, payload: object | None = None):
            self.text = text or ""
            self._payload = payload

        def raise_for_status(self):
            return None

        def json(self):
            return self._payload

    class FakeSession:
        def __init__(self):
            self.get_calls = []
            self.post_calls = []

        def get(self, url, params=None, headers=None, timeout=None):
            self.get_calls.append((url, params))
            if url == fetch_exchange_masterfiles.BMV_MOBILE_QUOTE_KEYS_URL:
                assert params == {"idBusquedaCotizacion": 2}
                return FakeResponse(
                    text=(
                        'for(;;);({"response":{"clavesCotizacion":['
                        '{"clave":"DANHOS","serie":"13"},'
                        '{"clave":"FCFE","serie":"18"},'
                        '{"clave":"FPLUS","serie":"16"},'
                        '{"clave":"COXA","serie":"*"}'
                        ']}})'
                    )
                )
            if url == fetch_exchange_masterfiles.BMV_SEARCH_TOKEN_URL:
                return FakeResponse(payload={"response": {"access_token": "token-123"}})
            raise AssertionError(url)

        def post(self, url, headers=None, json=None, timeout=None):
            self.post_calls.append((url, json))
            assert url == "https://www.bmv.com.mx/api/searchservice/v1"
            term = json["payload"]["term"]
            payloads = {
                "DANHOS": {
                    "response": {
                        "busquedaPanel": {
                            "busquedaGeneral": {
                                "instrumentosEmisoras": {
                                    "instrumentos": {
                                        "coincidenciaParcialInstrumentos": {
                                            "emisoras": {
                                                "hits": [
                                                    {
                                                        "_source": {
                                                            "cve_emisora": "DANHOS",
                                                            "serie": "13",
                                                            "descripcion": "FIBRAS CERTIFICADOS INMOBILIARIOS",
                                                            "mercado": "Capitales",
                                                            "estatus": "Activa",
                                                            "instrumento": "DANHOS 13",
                                                        }
                                                    }
                                                ]
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                "FCFE": {
                    "response": {
                        "busquedaPanel": {
                            "busquedaGeneral": {
                                "instrumentosEmisoras": {
                                    "instrumentos": {
                                        "coincidenciaParcialInstrumentos": {
                                            "emisoras": {
                                                "hits": [
                                                    {
                                                        "_source": {
                                                            "cve_emisora": "FCFE",
                                                            "serie": "18",
                                                            "descripcion": "FIDEICOMISOS DE INVERSION EN ENERGIA",
                                                            "mercado": "Capitales",
                                                            "estatus": "Activa",
                                                            "instrumento": "FCFE 18",
                                                        }
                                                    }
                                                ]
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                "FPLUS": {
                    "response": {
                        "busquedaPanel": {
                            "busquedaGeneral": {
                                "instrumentosEmisoras": {
                                    "instrumentos": {
                                        "coincidenciaParcialInstrumentos": {
                                            "emisoras": {
                                                "hits": [
                                                    {
                                                        "_source": {
                                                            "cve_emisora": "FPLUS",
                                                            "serie": "16",
                                                            "descripcion": "FIBRAS CERTIFICADOS INMOBILIARIOS",
                                                            "mercado": "Capitales",
                                                            "estatus": "Activa",
                                                            "instrumento": "FPLUS 16",
                                                        }
                                                    }
                                                ]
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                "COXA": {
                    "response": {
                        "busquedaPanel": {
                            "busquedaGeneral": {
                                "instrumentosEmisoras": {
                                    "instrumentos": {
                                        "coincidenciaParcialInstrumentos": {
                                            "emisoras": {
                                                "hits": [
                                                    {
                                                        "_source": {
                                                            "cve_emisora": "COXA",
                                                            "serie": "A",
                                                            "descripcion": "ACCIONES",
                                                            "mercado": "Capitales",
                                                            "estatus": "Activa",
                                                            "instrumento": "COXA A",
                                                        }
                                                    }
                                                ]
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
            }
            return FakeResponse(payload=payloads[term])

    session = FakeSession()
    rows = fetch_bmv_capital_trust_search(
        source,
        listings_path=listings_path,
        verification_dir=tmp_path,
        session=session,
    )

    assert rows == [
        {
            "source_key": "bmv_capital_trust_search",
            "provider": "BMV",
            "source_url": "https://www.bmv.com.mx/api/searchservice/v1",
            "ticker": "DANHOS13",
            "name": "DANHOS 13",
            "exchange": "BMV",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "MXCFDA020005",
        },
        {
            "source_key": "bmv_capital_trust_search",
            "provider": "BMV",
            "source_url": "https://www.bmv.com.mx/api/searchservice/v1",
            "ticker": "FCFE18",
            "name": "FCFE 18",
            "exchange": "BMV",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "MXFEFC0C0003",
        },
        {
            "source_key": "bmv_capital_trust_search",
            "provider": "BMV",
            "source_url": "https://www.bmv.com.mx/api/searchservice/v1",
            "ticker": "FPLUS16",
            "name": "FPLUS 16",
            "exchange": "BMV",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
        },
    ]
    assert session.get_calls == [
        (fetch_exchange_masterfiles.BMV_MOBILE_QUOTE_KEYS_URL, {"idBusquedaCotizacion": 2}),
        (fetch_exchange_masterfiles.BMV_SEARCH_TOKEN_URL, None),
    ]
    assert session.post_calls == [
        ("https://www.bmv.com.mx/api/searchservice/v1", {"lang": "es", "payload": {"term": "DANHOS", "term2": "", "termT": "DANHOS", "searchType": "busquedaPanel"}}),
        ("https://www.bmv.com.mx/api/searchservice/v1", {"lang": "es", "payload": {"term": "FCFE", "term2": "", "termT": "FCFE", "searchType": "busquedaPanel"}}),
        ("https://www.bmv.com.mx/api/searchservice/v1", {"lang": "es", "payload": {"term": "FPLUS", "term2": "", "termT": "FPLUS", "searchType": "busquedaPanel"}}),
        ("https://www.bmv.com.mx/api/searchservice/v1", {"lang": "es", "payload": {"term": "COXA", "term2": "", "termT": "COXA", "searchType": "busquedaPanel"}}),
    ]


def test_fetch_bmv_etf_search_backfills_exact_and_root_matches(tmp_path, monkeypatch) -> None:
    source = MasterfileSource(
        key="bmv_etf_search",
        provider="BMV",
        description="Official BMV ETF and TRACS search supplement",
        source_url="https://www.bmv.com.mx/api/searchservice/v1",
        format="bmv_etf_search_json",
        reference_scope="listed_companies_subset",
    )
    listings_path = tmp_path / "listings.csv"
    listings_path.write_text(
        "\n".join(
            [
                "ticker,exchange,asset_type,name,isin",
                "CSPXN,BMV,ETF,iShares Core S&P 500 UCITS ETF,",
                "NAFTRACISH,BMV,ETF,NAFTRAC ISHARES,",
            ]
        ),
        encoding="utf-8",
    )

    class FakeResponse:
        def __init__(self, *, text: str | None = None, payload: object | None = None):
            self.text = text or ""
            self._payload = payload

        def raise_for_status(self):
            return None

        def json(self):
            return self._payload

    class FakeSession:
        def __init__(self):
            self.get_calls = []
            self.post_calls = []

        def get(self, url, params=None, headers=None, timeout=None):
            self.get_calls.append((url, params))
            if url == fetch_exchange_masterfiles.BMV_MOBILE_QUOTE_KEYS_URL:
                return FakeResponse(
                    text='for(;;);({"response":{"clavesCotizacion":[{"clave":"CSPX","serie":"N"}]}})'
                )
            if url == fetch_exchange_masterfiles.BMV_SEARCH_TOKEN_URL:
                return FakeResponse(payload={"response": {"access_token": "token-123"}})
            raise AssertionError(url)

        def post(self, url, headers=None, json=None, timeout=None):
            self.post_calls.append((url, json))
            assert url == "https://www.bmv.com.mx/api/searchservice/v1"
            term = json["payload"]["term"]
            payloads = {
                "CSPX": {
                    "response": {
                        "busquedaPanel": {
                            "busquedaGeneral": {
                                "instrumentosEmisoras": {
                                    "instrumentos": {
                                        "coincidenciaParcialInstrumentos": {
                                            "emisoras": {
                                                "hits": [
                                                    {
                                                        "_source": {
                                                            "cve_emisora": "CSPX",
                                                            "serie": "N",
                                                            "descripcion": "CANASTAS DE ACCIONES (TRAC´S EXTRANJEROS).",
                                                            "mercado": "Global",
                                                            "estatus": "Activa",
                                                            "instrumento": "iShares Core S&P 500 UCITS ETF USD (Acc)",
                                                        }
                                                    }
                                                ]
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                "NAFTRACISH": {"response": {"busquedaPanel": {"busquedaGeneral": {"instrumentosEmisoras": {"instrumentos": {"coincidenciaParcialInstrumentos": {"emisoras": {"hits": []}}}}}}}},
                "NAFTRAC": {
                    "response": {
                        "busquedaPanel": {
                            "busquedaGeneral": {
                                "instrumentosEmisoras": {
                                    "instrumentos": {
                                        "coincidenciaParcialInstrumentos": {
                                            "emisoras": {
                                                "hits": [
                                                    {
                                                        "_source": {
                                                            "cve_emisora": "NAFTRAC",
                                                            "serie": "ISHRS",
                                                            "descripcion": "TRACS",
                                                            "mercado": "Capitales",
                                                            "estatus": "Activa",
                                                            "instrumento": "NAFTRAC ISHRS",
                                                        }
                                                    }
                                                ]
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
            }
            return FakeResponse(payload=payloads[term])

    session = FakeSession()
    rows = fetch_bmv_etf_search(
        source,
        listings_path=listings_path,
        verification_dir=tmp_path,
        session=session,
    )

    assert rows == [
        {
            "source_key": "bmv_etf_search",
            "provider": "BMV",
            "source_url": "https://www.bmv.com.mx/api/searchservice/v1",
            "ticker": "CSPXN",
            "name": "iShares Core S&P 500 UCITS ETF USD (Acc)",
            "exchange": "BMV",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
        },
        {
            "source_key": "bmv_etf_search",
            "provider": "BMV",
            "source_url": "https://www.bmv.com.mx/api/searchservice/v1",
            "ticker": "NAFTRACISH",
            "name": "NAFTRAC ISHRS",
            "exchange": "BMV",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
        },
    ]
    assert session.get_calls == [
        (fetch_exchange_masterfiles.BMV_MOBILE_QUOTE_KEYS_URL, {"idBusquedaCotizacion": 2}),
        (fetch_exchange_masterfiles.BMV_SEARCH_TOKEN_URL, None),
        (fetch_exchange_masterfiles.BMV_SEARCH_TOKEN_URL, None),
    ]
    assert session.post_calls == [
        ("https://www.bmv.com.mx/api/searchservice/v1", {"lang": "es", "payload": {"term": "CSPX", "term2": "", "termT": "CSPX", "searchType": "busquedaPanel"}}),
        ("https://www.bmv.com.mx/api/searchservice/v1", {"lang": "es", "payload": {"term": "NAFTRACISH", "term2": "", "termT": "NAFTRACISH", "searchType": "busquedaPanel"}}),
        ("https://www.bmv.com.mx/api/searchservice/v1", {"lang": "es", "payload": {"term": "NAFTRAC", "term2": "", "termT": "NAFTRAC", "searchType": "busquedaPanel"}}),
    ]


def test_bmv_issuer_directory_source_is_modeled_as_partial_official_coverage() -> None:
    source = next(item for item in OFFICIAL_SOURCES if item.key == "bmv_issuer_directory")
    assert source.reference_scope == "listed_companies_subset"


def test_parse_bmv_market_data_pages_extracts_isin_and_profile_taxonomy() -> None:
    stats_html = """
    <section>
      <table>
        <thead><tr><th>Tipo Valor</th><th>Serie</th><th>Isin</th><th>Estatus</th><th>Descripci&oacute;n</th></tr></thead>
        <tbody>
          <tr><td>1I</td><td>N</td><td>IE00BFMXXD54</td><td>ACTIVA</td><td>CANASTAS DE ACCIONES</td></tr>
          <tr><td>1I</td><td>C</td><td>not-an-isin</td><td>ACTIVA</td><td>CANASTAS DE ACCIONES</td></tr>
        </tbody>
      </table>
    </section>
    """
    profile_html = """
    <table class="info">
      <tr><td>Sector:</td><td>SERVICIOS FINANCIEROS</td></tr>
      <tr><td>Clasificaci&oacute;n ETF:</td><td>ACCIONES</td></tr>
      <tr><td>Objeto de Inversi&oacute;n:</td><td>Replica el S&amp;P 500</td></tr>
    </table>
    """

    assert parse_bmv_market_data_instruments_html(stats_html) == [
        {
            "tipo_valor": "1I",
            "serie": "N",
            "isin": "IE00BFMXXD54",
            "estatus": "ACTIVA",
            "descripcion": "CANASTAS DE ACCIONES",
        },
        {
            "tipo_valor": "1I",
            "serie": "C",
            "isin": "",
            "estatus": "ACTIVA",
            "descripcion": "CANASTAS DE ACCIONES",
        }
    ]
    assert parse_bmv_market_data_profile_html(profile_html) == {
        "SECTOR": "SERVICIOS FINANCIEROS",
        "CLASIFICACION ETF": "ACCIONES",
        "OBJETO DE INVERSION": "Replica el S&P 500",
    }


def test_bmv_market_data_securities_source_is_modeled_as_official_supplement() -> None:
    source = next(item for item in OFFICIAL_SOURCES if item.key == "bmv_market_data_securities")
    assert source.provider == "BMV"
    assert source.reference_scope == "listed_companies_subset"
    assert source.format == "bmv_market_data_security_pages_html"


def test_fetch_bmv_issuer_directory_backfills_local_and_global_matches(tmp_path, monkeypatch) -> None:
    source = MasterfileSource(
        key="bmv_issuer_directory",
        provider="BMV",
        description="Official BMV issuer directory supplement for local and global listings",
        source_url="https://staging.bmv.com.mx/es/Grupo_BMV/Informacion_de_emisora/_rid/541/_mto/3/_mod/doSearch",
        format="bmv_issuer_directory_json",
        reference_scope="listed_companies_subset",
    )
    listings_path = tmp_path / "listings.csv"
    listings_path.write_text(
        "\n".join(
            [
                "ticker,exchange,asset_type,name,isin",
                "CREAL,BMV,Stock,Credito Real S.A.B. de C.V.,MX00CR000000",
                "NION,BMV,Stock,NIO Inc,KYG6525F1028",
                "ROGN,BMV,Stock,Roche Holding AG,CH0012032048",
                "BRKB,BMV,Stock,Berkshire Hathaway Inc,",
                "VDCAN,BMV,ETF,Vanguard Funds Public Limited Company - Vanguard USD Corporate 1-3 Year Bond UCI,IE00BGYWSV06",
                "VFEAN,BMV,ETF,Vanguard Funds Public Limited Company - Vanguard FTSE Emerging Markets UCITS ETF,",
            ]
        ),
        encoding="utf-8",
    )
    stock_verification_dir = tmp_path / "stock_verification" / "run-01"
    stock_verification_dir.mkdir(parents=True)
    (stock_verification_dir / "chunk-01-of-01.csv").write_text(
        "\n".join(
            [
                "ticker,exchange,status",
                "CREAL,BMV,reference_gap",
                "NION,BMV,reference_gap",
                "ROGN,BMV,reference_gap",
            ]
        ),
        encoding="utf-8",
    )
    etf_verification_dir = tmp_path / "etf_verification" / "run-01"
    etf_verification_dir.mkdir(parents=True)
    (etf_verification_dir / "chunk-01-of-01.csv").write_text(
        "\n".join(
            [
                "ticker,exchange,status",
                "VDCAN,BMV,reference_gap",
                "VFEAN,BMV,reference_gap",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "BMV_ISSUER_DIRECTORY_QUERY_COMBINATIONS",
        (
            ("CGEN_CAPIT", "CGEN_ELAC"),
            ("CGEN_GLOB", "CGEN_ELGA"),
            ("CGEN_GLOB", "CGEN_ELGE"),
        ),
    )

    class FakeResponse:
        def __init__(self, text: str):
            self.text = text

        def raise_for_status(self):
            return None

    class FakeSession:
        def __init__(self):
            self.get_calls = []

        def get(self, url, params=None, headers=None, timeout=None):
            self.get_calls.append((url, params))
            payloads = {
                ("CGEN_CAPIT", "CGEN_ELAC"): {
                    "response": {
                        "resultado": [
                            {
                                "claveEmisora": "CREAL",
                                "serie": None,
                                "razonSocial": "CREDITO REAL, S.A.B. DE C.V., SOFOM, E.N.R.",
                                "isin": None,
                            }
                        ]
                    }
                },
                ("CGEN_GLOB", "CGEN_ELGA"): {
                    "response": {
                        "resultado": [
                            {
                                "claveEmisora": None,
                                "claveEmision": "NIO",
                                "serie": "N",
                                "razonSocial": "NIO Inc",
                                "isin": "US62914V1061",
                            },
                            {
                                "claveEmisora": None,
                                "claveEmision": "ROG",
                                "serie": "N",
                                "razonSocial": "ROCHE HOLDING AG",
                                "isin": "CH0012032048",
                            },
                            {
                                "claveEmisora": None,
                                "claveEmision": "BRKB",
                                "serie": "*",
                                "razonSocial": "BERKSHIRE HATHAWAY INC",
                                "isin": "US0846707026",
                            },
                        ]
                    }
                },
                ("CGEN_GLOB", "CGEN_ELGE"): {
                    "response": {
                        "resultado": [
                            {
                                "claveEmisora": None,
                                "claveEmision": "VDCA",
                                "serie": "N",
                                "razonSocial": "Vanguard USD Corporate 1-3 year UCITS ETF",
                                "isin": "IE00BGYWSV06",
                            },
                            {
                                "claveEmisora": None,
                                "claveEmision": "VFEA",
                                "serie": "N",
                                "razonSocial": "Vanguard FTSE Emerging Markets UCITS ETF",
                                "isin": "IE00BK5BR733",
                            },
                        ]
                    }
                },
            }
            payload = payloads[(params["idTipoMercado"], params["idTipoInstrumento"])]
            return FakeResponse(f'for(;;);({json.dumps(payload)})')

    session = FakeSession()
    rows = fetch_bmv_issuer_directory(
        source,
        listings_path=listings_path,
        stock_verification_dir=stock_verification_dir.parent,
        etf_verification_dir=etf_verification_dir.parent,
        session=session,
    )

    assert rows == [
        {
            "source_key": "bmv_issuer_directory",
            "provider": "BMV",
            "source_url": "https://staging.bmv.com.mx/es/Grupo_BMV/Informacion_de_emisora/_rid/541/_mto/3/_mod/doSearch",
            "ticker": "CREAL",
            "name": "CREDITO REAL, S.A.B. DE C.V., SOFOM, E.N.R.",
            "exchange": "BMV",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "MX00CR000000",
        },
        {
            "source_key": "bmv_issuer_directory",
            "provider": "BMV",
            "source_url": "https://staging.bmv.com.mx/es/Grupo_BMV/Informacion_de_emisora/_rid/541/_mto/3/_mod/doSearch",
            "ticker": "NION",
            "name": "NIO Inc",
            "exchange": "BMV",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "US62914V1061",
        },
        {
            "source_key": "bmv_issuer_directory",
            "provider": "BMV",
            "source_url": "https://staging.bmv.com.mx/es/Grupo_BMV/Informacion_de_emisora/_rid/541/_mto/3/_mod/doSearch",
            "ticker": "ROGN",
            "name": "ROCHE HOLDING AG",
            "exchange": "BMV",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "CH0012032048",
        },
        {
            "source_key": "bmv_issuer_directory",
            "provider": "BMV",
            "source_url": "https://staging.bmv.com.mx/es/Grupo_BMV/Informacion_de_emisora/_rid/541/_mto/3/_mod/doSearch",
            "ticker": "BRKB",
            "name": "BERKSHIRE HATHAWAY INC",
            "exchange": "BMV",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "US0846707026",
        },
        {
            "source_key": "bmv_issuer_directory",
            "provider": "BMV",
            "source_url": "https://staging.bmv.com.mx/es/Grupo_BMV/Informacion_de_emisora/_rid/541/_mto/3/_mod/doSearch",
            "ticker": "VDCAN",
            "name": "Vanguard USD Corporate 1-3 year UCITS ETF",
            "exchange": "BMV",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "IE00BGYWSV06",
        },
        {
            "source_key": "bmv_issuer_directory",
            "provider": "BMV",
            "source_url": "https://staging.bmv.com.mx/es/Grupo_BMV/Informacion_de_emisora/_rid/541/_mto/3/_mod/doSearch",
            "ticker": "VFEAN",
            "name": "Vanguard FTSE Emerging Markets UCITS ETF",
            "exchange": "BMV",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "IE00BK5BR733",
        },
    ]
    assert session.get_calls == [
        (
            "https://staging.bmv.com.mx/es/Grupo_BMV/Informacion_de_emisora/_rid/541/_mto/3/_mod/doSearch",
            {
                "idTipoMercado": "CGEN_CAPIT",
                "idTipoInstrumento": "CGEN_ELAC",
                "idTipoEmpresa": "",
                "idSector": "",
                "idSubsector": "",
                "idRamo": "",
                "idSubramo": "",
            },
        ),
        (
            "https://staging.bmv.com.mx/es/Grupo_BMV/Informacion_de_emisora/_rid/541/_mto/3/_mod/doSearch",
            {
                "idTipoMercado": "CGEN_GLOB",
                "idTipoInstrumento": "CGEN_ELGA",
                "idTipoEmpresa": "",
                "idSector": "",
                "idSubsector": "",
                "idRamo": "",
                "idSubramo": "",
            },
        ),
        (
            "https://staging.bmv.com.mx/es/Grupo_BMV/Informacion_de_emisora/_rid/541/_mto/3/_mod/doSearch",
            {
                "idTipoMercado": "CGEN_GLOB",
                "idTipoInstrumento": "CGEN_ELGE",
                "idTipoEmpresa": "",
                "idSector": "",
                "idSubsector": "",
                "idRamo": "",
                "idSubramo": "",
            },
        ),
    ]


def test_bme_sources_are_modeled_as_partial_official_coverage() -> None:
    stock_source = next(item for item in OFFICIAL_SOURCES if item.key == "bme_listed_companies")
    etf_source = next(item for item in OFFICIAL_SOURCES if item.key == "bme_etf_list")
    security_prices_source = next(item for item in OFFICIAL_SOURCES if item.key == "bme_security_prices_directory")

    assert stock_source.reference_scope == "listed_companies_subset"
    assert etf_source.reference_scope == "listed_companies_subset"
    assert security_prices_source.format == "bme_security_prices_json"
    assert security_prices_source.reference_scope == "exchange_directory"


def test_bursa_equity_isin_source_is_modeled_as_partial_official_coverage() -> None:
    source = next(item for item in OFFICIAL_SOURCES if item.key == "bursa_equity_isin")

    assert source.provider == "Bursa Malaysia"
    assert source.format == "bursa_equity_isin_pdf"
    assert source.reference_scope == "listed_companies_subset"


def test_bursa_closing_prices_source_is_modeled_as_official_sector_subset() -> None:
    source = next(item for item in OFFICIAL_SOURCES if item.key == "bursa_closing_prices")

    assert source.provider == "Bursa Malaysia"
    assert source.format == "bursa_closing_prices_pdf"
    assert source.reference_scope == "listed_companies_subset"


def test_derive_bursa_ticker_from_isin_handles_supported_official_shapes() -> None:
    assert fetch_exchange_masterfiles.derive_bursa_ticker_from_isin("MYQ0306OO009", "ORDINARY SHARE") == "0306"
    assert fetch_exchange_masterfiles.derive_bursa_ticker_from_isin("MYL03060O000", "ORDINARY SHARE") == "03060"
    assert fetch_exchange_masterfiles.derive_bursa_ticker_from_isin("MYL5338TO007", "REITS") == "5338"
    assert fetch_exchange_masterfiles.derive_bursa_ticker_from_isin("MYL5235SS008", "REITS") == "5235SS"
    assert fetch_exchange_masterfiles.derive_bursa_ticker_from_isin("MYL5108FO003", "CLOSE END FUND") == "5108"
    assert fetch_exchange_masterfiles.derive_bursa_ticker_from_isin("MYL0828EA003", "ETF") == "0828EA"
    assert fetch_exchange_masterfiles.derive_bursa_ticker_from_isin("MYL1163PA000", "PREFERENCE SHARES") == ""
    assert fetch_exchange_masterfiles.derive_bursa_ticker_from_isin("BMG9828L1072", "ORDINARY SHARE FOREIGN") == ""


def test_parse_bursa_equity_isin_table_rows_maps_derivable_stock_and_etf_rows() -> None:
    source = MasterfileSource(
        key="bursa_equity_isin",
        provider="Bursa Malaysia",
        description="Official Bursa Malaysia equity ISIN PDF",
        source_url="https://www.bursamalaysia.com/trade/trading_resources/equities/isin",
        format="bursa_equity_isin_pdf",
        reference_scope="listed_companies_subset",
    )
    table_rows = [
        [
            "No.",
            "Stock Name (Long)",
            "Stock Name (Short)",
            "ISIN",
            "Issue Description",
            "Date Of Listing",
            "Instrument Category",
        ],
        ["1", "SMART ASIA CHEMICAL BHD", "SMART", "MYQ0306OO009", "ORDINARY SHARE", "17/02/2025", ""],
        ["2", "HYDROPIPES BERHAD", "HPI", "MYL03060O000", "ORDINARY SHARE", "27/09/2024", ""],
        ["3", "TRADEPLUS SHARIAH GOLD TRACKER", "GOLDETF", "MYL0828EA003", "ETF", "06/12/2017", ""],
        ["4", "ICAPITAL.BIZ BHD", "ICAP", "MYL5108FO003", "CLOSE END FUND", "19/10/2005", ""],
        ["5", "KLCC PROP&REITS-STAPLED SEC", "KLCC", "MYL5235SS008", "REITS", "09/05/2013", ""],
        ["6", "ALLIANZ MALAYSIA BERHAD", "ALLIANZ-PA", "MYL1163PA000", "PREFERENCE SHARES", "", ""],
        ["7", "XIDELANG HOLDINGS LTD", "XDL", "BMG9828L1072", "ORDINARY SHARE", "", ""],
    ]

    rows = fetch_exchange_masterfiles.parse_bursa_equity_isin_table_rows(
        table_rows,
        source,
        listings_by_isin={
            "BMG9828L1072": [
                {
                    "ticker": "5156",
                    "exchange": "Bursa",
                    "asset_type": "Stock",
                    "isin": "BMG9828L1072",
                }
            ]
        },
        source_url="https://example.com/isinequity.pdf",
    )

    assert rows == [
        {
            "source_key": "bursa_equity_isin",
            "provider": "Bursa Malaysia",
            "source_url": "https://example.com/isinequity.pdf",
            "ticker": "0306",
            "name": "SMART ASIA CHEMICAL BHD",
            "exchange": "Bursa",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "MYQ0306OO009",
        },
        {
            "source_key": "bursa_equity_isin",
            "provider": "Bursa Malaysia",
            "source_url": "https://example.com/isinequity.pdf",
            "ticker": "03060",
            "name": "HYDROPIPES BERHAD",
            "exchange": "Bursa",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "MYL03060O000",
        },
        {
            "source_key": "bursa_equity_isin",
            "provider": "Bursa Malaysia",
            "source_url": "https://example.com/isinequity.pdf",
            "ticker": "0828EA",
            "name": "TRADEPLUS SHARIAH GOLD TRACKER",
            "exchange": "Bursa",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "MYL0828EA003",
        },
        {
            "source_key": "bursa_equity_isin",
            "provider": "Bursa Malaysia",
            "source_url": "https://example.com/isinequity.pdf",
            "ticker": "5108",
            "name": "ICAPITAL.BIZ BHD",
            "exchange": "Bursa",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "MYL5108FO003",
        },
        {
            "source_key": "bursa_equity_isin",
            "provider": "Bursa Malaysia",
            "source_url": "https://example.com/isinequity.pdf",
            "ticker": "5235SS",
            "name": "KLCC PROP&REITS-STAPLED SEC",
            "exchange": "Bursa",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "MYL5235SS008",
        },
        {
            "source_key": "bursa_equity_isin",
            "provider": "Bursa Malaysia",
            "source_url": "https://example.com/isinequity.pdf",
            "ticker": "5156",
            "name": "XIDELANG HOLDINGS LTD",
            "exchange": "Bursa",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "BMG9828L1072",
        },
    ]


def test_parse_bursa_closing_prices_table_rows_maps_sector_and_etf_category() -> None:
    source = MasterfileSource(
        key="bursa_closing_prices",
        provider="Bursa Malaysia",
        description="Official Bursa Malaysia closing price PDF with board and sector labels",
        source_url="https://www.bursamalaysia.com/misc/missftp/securities/example.pdf",
        format="bursa_closing_prices_pdf",
        reference_scope="listed_companies_subset",
    )
    table_rows = [
        ["Stock Code", "Stock Name", "Stock Long Name", "Board", "Sector", "Closing Price"],
        ["0002", "KOTRA", "KOTRA INDUSTRIES BHD", "MAIN MARKET", "HEALTH CARE", "4.440"],
        ["0820EA", "FBMKLCI-EA", "FTSE BURSA MALAYSIA KLCI ETF", "EXCHANGE\nTRADED", "EXCHANGE TRADED FUND-EQUITY", "1.745"],
        ["0001CC", "SCOMNET-CC", "CW SUPERCOMNET", "STRCWARR", "HEALTH CARE", "0.030"],
    ]

    rows = parse_bursa_closing_prices_table_rows(table_rows, source, source_url="https://example.com/bursa.pdf")

    assert rows == [
        {
            "source_key": "bursa_closing_prices",
            "provider": "Bursa Malaysia",
            "source_url": "https://example.com/bursa.pdf",
            "ticker": "0002",
            "name": "KOTRA INDUSTRIES BHD",
            "exchange": "Bursa",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "sector": "Health Care",
        },
        {
            "source_key": "bursa_closing_prices",
            "provider": "Bursa Malaysia",
            "source_url": "https://example.com/bursa.pdf",
            "ticker": "0820EA",
            "name": "FTSE BURSA MALAYSIA KLCI ETF",
            "exchange": "Bursa",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "sector": "Equity",
        },
    ]


def test_load_bursa_equity_isin_rows_falls_back_to_cache(tmp_path, monkeypatch) -> None:
    cache_path = tmp_path / "bursa_equity_isin.json"
    cache_path.write_text(
        '[{"ticker":"0306","name":"SMART ASIA CHEMICAL BHD","exchange":"Bursa","asset_type":"Stock","listing_status":"active","isin":"MYQ0306OO009"}]',
        encoding="utf-8",
    )
    monkeypatch.setattr(fetch_exchange_masterfiles, "BURSA_EQUITY_ISIN_CACHE", cache_path)
    monkeypatch.setattr(fetch_exchange_masterfiles, "LEGACY_BURSA_EQUITY_ISIN_CACHE", tmp_path / "missing.json")
    monkeypatch.setattr(fetch_exchange_masterfiles, "BURSA_EQUITY_ISIN_PDF_CACHE", tmp_path / "missing.pdf")
    monkeypatch.setattr(fetch_exchange_masterfiles, "LEGACY_BURSA_EQUITY_ISIN_PDF_CACHE", tmp_path / "missing-legacy.pdf")
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "fetch_bursa_equity_isin_rows",
        lambda source, session=None: (_ for _ in ()).throw(requests.RequestException("boom")),
    )

    source = next(item for item in OFFICIAL_SOURCES if item.key == "bursa_equity_isin")
    rows, mode = fetch_exchange_masterfiles.load_bursa_equity_isin_rows(source)

    assert mode == "cache"
    assert rows == [
        {
            "ticker": "0306",
            "name": "SMART ASIA CHEMICAL BHD",
            "exchange": "Bursa",
            "asset_type": "Stock",
            "listing_status": "active",
            "isin": "MYQ0306OO009",
        }
    ]


def test_bme_request_headers_match_browser_xhr_shape() -> None:
    headers = fetch_exchange_masterfiles.bme_request_headers()

    assert headers["Origin"] == "https://www.bolsasymercados.es"
    assert headers["Referer"] == fetch_exchange_masterfiles.BME_COMPANIES_SEARCH_PAGE_URL
    assert headers["X-Requested-With"] == "XMLHttpRequest"
    assert headers["Sec-Fetch-Site"] == "same-site"
    assert headers["Sec-Fetch-Mode"] == "cors"
    assert headers["Sec-Fetch-Dest"] == "empty"


def test_fetch_bme_reference_rows_maps_official_tickers_and_variants(tmp_path) -> None:
    source = MasterfileSource(
        key="bme_listed_companies",
        provider="BME",
        description="Official BME listed companies directory",
        source_url="https://apiweb.bolsasymercados.es/Market/v1/EQ/ListedCompanies",
        format="bme_listed_companies_json",
        reference_scope="listed_companies_subset",
    )
    listings_path = tmp_path / "listings.csv"
    listings_path.write_text("ticker,exchange,asset_type,name,isin\n", encoding="utf-8")

    class FakeResponse:
        def __init__(self, payload):
            self._payload = payload

        def raise_for_status(self):
            return None

        def json(self):
            return self._payload

    class FakeSession:
        def __init__(self):
            self.get_calls = []

        def get(self, url, params=None, headers=None, timeout=None):
            self.get_calls.append((url, params))
            if url == fetch_exchange_masterfiles.BME_LISTED_COMPANIES_API_URL:
                payloads = {
                    0: {
                        "hasMoreResults": True,
                        "data": [
                            {"isin": "ES0113900J37", "shareName": "BANCO SANTANDER", "tradingSystem": "SIBE"},
                        ],
                    },
                    1: {
                        "hasMoreResults": False,
                        "data": [
                            {"isin": "ES0171996095", "shareName": "GRIFOLS CL.A PFD", "tradingSystem": "SIBE"},
                        ],
                    },
                }
                assert params in (
                    {"tradingSystem": "SIBE", "page": 0, "pageSize": 100},
                    {"tradingSystem": "SIBE", "page": 1, "pageSize": 100},
                )
                return FakeResponse(payloads[params["page"]])
            if url == fetch_exchange_masterfiles.BME_SHARE_DETAILS_INFO_API_URL:
                payloads = {
                    "ES0113900J37": {
                        "isin": "ES0113900J37",
                        "ticker": "SAN",
                        "name": "BANCO SANTANDER",
                        "shortName": "B.SANTANDER",
                        "tradingSystem": "SIBE",
                        "sector": "05",
                        "subsector": "01",
                    },
                    "ES0171996095": {
                        "isin": "ES0171996095",
                        "ticker": "GRF.P",
                        "name": "GRIFOLS, S.A. CL.A PFD",
                        "shortName": "GRIFOLS A PFD",
                        "tradingSystem": "SIBE",
                        "sector": "03",
                        "subsector": "05",
                    },
                }
                return FakeResponse(payloads[params["isin"]])
            raise AssertionError(url)

    session = FakeSession()
    rows = fetch_bme_reference_rows(source, listings_path=listings_path, session=session)

    assert rows == [
        {
            "source_key": "bme_listed_companies",
            "provider": "BME",
            "source_url": "https://apiweb.bolsasymercados.es/Market/v1/EQ/ListedCompanies",
            "ticker": "SAN",
            "name": "BANCO SANTANDER",
            "exchange": "BME",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "ES0113900J37",
            "sector": "Financials",
        },
        {
            "source_key": "bme_listed_companies",
            "provider": "BME",
            "source_url": "https://apiweb.bolsasymercados.es/Market/v1/EQ/ListedCompanies",
            "ticker": "GRF.P",
            "name": "GRIFOLS, S.A. CL.A PFD",
            "exchange": "BME",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "ES0171996095",
            "sector": "Health Care",
        },
        {
            "source_key": "bme_listed_companies",
            "provider": "BME",
            "source_url": "https://apiweb.bolsasymercados.es/Market/v1/EQ/ListedCompanies",
            "ticker": "GRF-P",
            "name": "GRIFOLS, S.A. CL.A PFD",
            "exchange": "BME",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "ES0171996095",
            "sector": "Health Care",
        },
    ]
    assert session.get_calls == [
        (
            fetch_exchange_masterfiles.BME_LISTED_COMPANIES_API_URL,
            {"tradingSystem": "SIBE", "page": 0, "pageSize": 100},
        ),
        (
            fetch_exchange_masterfiles.BME_LISTED_COMPANIES_API_URL,
            {"tradingSystem": "SIBE", "page": 1, "pageSize": 100},
        ),
        (
            fetch_exchange_masterfiles.BME_SHARE_DETAILS_INFO_API_URL,
            {"isin": "ES0113900J37", "language": "en"},
        ),
        (
            fetch_exchange_masterfiles.BME_SHARE_DETAILS_INFO_API_URL,
            {"isin": "ES0171996095", "language": "en"},
        ),
    ]


def test_fetch_bme_reference_rows_maps_official_etfs(tmp_path) -> None:
    source = MasterfileSource(
        key="bme_etf_list",
        provider="BME",
        description="Official BME ETF directory",
        source_url="https://apiweb.bolsasymercados.es/Market/v1/EQ/ListedCompanies",
        format="bme_etf_list_json",
        reference_scope="listed_companies_subset",
    )
    listings_path = tmp_path / "listings.csv"
    listings_path.write_text(
        "\n".join(
            [
                "ticker,exchange,asset_type,name,isin",
                "BBVAI,BME,ETF,Legacy ETF,ES0105336038",
                "2INVE,BME,ETF,Legacy inverse ETF,FR0011036268",
            ]
        ),
        encoding="utf-8",
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
            self.get_calls = []

        def get(self, url, params=None, headers=None, timeout=None):
            self.get_calls.append((url, params))
            if url == fetch_exchange_masterfiles.BME_LISTED_COMPANIES_API_URL:
                assert params == {"tradingSystem": "ETF", "page": 0, "pageSize": 100}
                return FakeResponse(
                    {
                        "hasMoreResults": False,
                        "data": [
                            {
                                "isin": "ES0105336038",
                                "shareName": "ACCION IBEX 35 ETF,FI COTIZ,ARM.",
                                "tradingSystem": "ETF",
                            },
                            {
                                "isin": "FR0011036268",
                                "shareName": "AMUNDI IBEX DOUBLE INVERSE ETF",
                                "tradingSystem": "ETF",
                            },
                        ]
                    }
                )
            raise AssertionError(url)

    session = FakeSession()
    rows = fetch_bme_reference_rows(source, listings_path=listings_path, session=session)

    assert rows == [
        {
            "source_key": "bme_etf_list",
            "provider": "BME",
            "source_url": "https://apiweb.bolsasymercados.es/Market/v1/EQ/ListedCompanies",
            "ticker": "BBVAI",
            "name": "ACCION IBEX 35 ETF,FI COTIZ,ARM.",
            "exchange": "BME",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "ES0105336038",
        },
        {
            "source_key": "bme_etf_list",
            "provider": "BME",
            "source_url": "https://apiweb.bolsasymercados.es/Market/v1/EQ/ListedCompanies",
            "ticker": "2INVE",
            "name": "AMUNDI IBEX DOUBLE INVERSE ETF",
            "exchange": "BME",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "FR0011036268",
        }
    ]
    assert session.get_calls == [
        (
            fetch_exchange_masterfiles.BME_LISTED_COMPANIES_API_URL,
            {"tradingSystem": "ETF", "page": 0, "pageSize": 100},
        )
    ]


def test_parse_bme_listed_values_text_lines_maps_equities_etfs_and_latibex() -> None:
    source = MasterfileSource(
        key="bme_listed_values",
        provider="BME",
        description="Official BME listed values PDF",
        source_url="https://example.com/listado-valores-renta-variable-es-en.pdf",
        format="bme_listed_values_pdf",
        reference_scope="listed_companies_subset",
    )

    rows = fetch_exchange_masterfiles.parse_bme_listed_values_text_lines(
        [
            "20250905 EQ ADX ES0136463017 AUDAX RENOV 281,1641 III 100.000",
            "20250905 TF BBVAI ES0105336038 ACCION IBEX 35 ETF F.I. COTIZADO ARMONIZADO 0 VI 3.000.000",
            "20250905 LT XPBRA BRPETRACNPR6 PETROBRAS PR 78936,06 VI 60.000",
            "Date Segment Ticker ISIN Code Security Name ADNT / Liquidity Band LIS Fixing",
        ],
        source,
        source_url="https://example.com/current.pdf",
    )

    assert rows == [
        {
            "source_key": "bme_listed_values",
            "provider": "BME",
            "source_url": "https://example.com/current.pdf",
            "ticker": "ADX",
            "name": "AUDAX RENOV",
            "exchange": "BME",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "ES0136463017",
        },
        {
            "source_key": "bme_listed_values",
            "provider": "BME",
            "source_url": "https://example.com/current.pdf",
            "ticker": "BBVAI",
            "name": "ACCION IBEX 35 ETF F.I. COTIZADO ARMONIZADO",
            "exchange": "BME",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "ES0105336038",
        },
        {
            "source_key": "bme_listed_values",
            "provider": "BME",
            "source_url": "https://example.com/current.pdf",
            "ticker": "XPBRA",
            "name": "PETROBRAS PR",
            "exchange": "BME",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "BRPETRACNPR6",
        },
    ]


def test_enrich_bme_listed_value_rows_with_share_details_adds_stock_sector() -> None:
    rows = [
        {
            "ticker": "ISE",
            "exchange": "BME",
            "asset_type": "Stock",
            "isin": "ES0143421073",
        },
        {
            "ticker": "2INVE",
            "exchange": "BME",
            "asset_type": "ETF",
            "isin": "FR0011036268",
        },
    ]

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"isin": "ES0143421073", "sector": "04", "subsector": "02"}

    class FakeSession:
        def __init__(self):
            self.get_calls = []

        def get(self, url, params=None, headers=None, timeout=None):
            self.get_calls.append((url, params))
            return FakeResponse()

    session = FakeSession()

    assert fetch_exchange_masterfiles.enrich_bme_listed_value_rows_with_share_details(
        rows,
        session=session,
    ) == [
        {
            "ticker": "ISE",
            "exchange": "BME",
            "asset_type": "Stock",
            "isin": "ES0143421073",
            "sector": "Consumer Discretionary",
        },
        {
            "ticker": "2INVE",
            "exchange": "BME",
            "asset_type": "ETF",
            "isin": "FR0011036268",
        },
    ]
    assert session.get_calls == [
        (
            fetch_exchange_masterfiles.BME_SHARE_DETAILS_INFO_API_URL,
            {"isin": "ES0143421073", "language": "en"},
        )
    ]


def test_fetch_bme_reference_rows_uses_listing_isin_when_share_details_fail(tmp_path) -> None:
    source = MasterfileSource(
        key="bme_listed_companies",
        provider="BME",
        description="Official BME listed companies directory",
        source_url="https://apiweb.bolsasymercados.es/Market/v1/EQ/ListedCompanies",
        format="bme_listed_companies_json",
        reference_scope="listed_companies_subset",
    )
    listings_path = tmp_path / "listings.csv"
    listings_path.write_text(
        "\n".join(
            [
                "ticker,exchange,asset_type,name,isin",
                "SAN,BME,Stock,Legacy Santander,ES0113900J37",
            ]
        ),
        encoding="utf-8",
    )

    class FakeResponse:
        def __init__(self, payload):
            self._payload = payload

        def raise_for_status(self):
            return None

        def json(self):
            return self._payload

    class FakeSession:
        def get(self, url, params=None, headers=None, timeout=None):
            if url == fetch_exchange_masterfiles.BME_LISTED_COMPANIES_API_URL:
                return FakeResponse(
                    {
                        "hasMoreResults": False,
                        "data": [
                            {"isin": "ES0113900J37", "shareName": "BANCO SANTANDER", "tradingSystem": "SIBE"},
                        ],
                    }
                )
            if url == fetch_exchange_masterfiles.BME_SHARE_DETAILS_INFO_API_URL:
                raise requests.RequestException("403")
            raise AssertionError(url)

    rows = fetch_bme_reference_rows(source, listings_path=listings_path, session=FakeSession())

    assert rows == [
        {
            "source_key": "bme_listed_companies",
            "provider": "BME",
            "source_url": "https://apiweb.bolsasymercados.es/Market/v1/EQ/ListedCompanies",
            "ticker": "SAN",
            "name": "BANCO SANTANDER",
            "exchange": "BME",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "ES0113900J37",
        }
    ]


def test_fetch_bme_security_prices_rows_maps_mixed_trading_systems() -> None:
    source = MasterfileSource(
        key="bme_security_prices_directory",
        provider="BME",
        description="Official BME security prices directory",
        source_url=fetch_exchange_masterfiles.BME_LISTED_COMPANIES_API_URL,
        format="bme_security_prices_json",
        reference_scope="exchange_directory",
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
            self.get_calls = []

        def get(self, url, params=None, headers=None, timeout=None):
            self.get_calls.append((url, params))
            if url == fetch_exchange_masterfiles.BME_LISTED_COMPANIES_API_URL:
                assert params == {
                    "tradingSystem": fetch_exchange_masterfiles.BME_SECURITY_PRICES_TRADING_SYSTEMS,
                    "page": 0,
                    "pageSize": 100,
                }
                return FakeResponse(
                    {
                        "hasMoreResults": False,
                        "data": [
                            {"isin": "ES0105877007", "shareName": "BETTER CONSULTANTS", "tradingSystem": "MTF"},
                            {
                                "isin": "FR0011036268",
                                "shareName": "AMUNDI IBEX 35 DOBL INV DIAR (-2X) UCITS",
                                "tradingSystem": "ETF",
                            },
                        ],
                    }
                )
            if url == fetch_exchange_masterfiles.BME_SHARE_DETAILS_INFO_API_URL:
                payloads = {
                    "ES0105877007": {
                        "isin": "ES0105877007",
                        "ticker": "SCBTT",
                        "name": "BETTER CONSULTANTS",
                        "shortName": "BETTER",
                        "tradingSystem": "MTF",
                        "sector": "06",
                        "subsector": "02",
                    },
                    "FR0011036268": {
                        "isin": "FR0011036268",
                        "ticker": "2INVE",
                        "name": "AMUNDI IBEX 35 DOBL INV DIAR (-2X) UCITS",
                        "shortName": "AMUIBEX2INVE",
                        "tradingSystem": "ETF",
                        "sector": "05",
                        "subsector": "07",
                    },
                }
                return FakeResponse(payloads[params["isin"]])
            raise AssertionError(url)

    rows = fetch_exchange_masterfiles.fetch_bme_security_prices_rows(source, session=FakeSession())

    assert rows == [
        {
            "source_key": "bme_security_prices_directory",
            "provider": "BME",
            "source_url": fetch_exchange_masterfiles.BME_LISTED_COMPANIES_API_URL,
            "ticker": "SCBTT",
            "name": "BETTER CONSULTANTS",
            "exchange": "BME",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "ES0105877007",
            "sector": "Information Technology",
        },
        {
            "source_key": "bme_security_prices_directory",
            "provider": "BME",
            "source_url": fetch_exchange_masterfiles.BME_LISTED_COMPANIES_API_URL,
            "ticker": "2INVE",
            "name": "AMUNDI IBEX 35 DOBL INV DIAR (-2X) UCITS",
            "exchange": "BME",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "FR0011036268",
        },
    ]


def test_load_bme_reference_rows_falls_back_to_cache(tmp_path, monkeypatch) -> None:
    cache_path = tmp_path / "bme_listed_companies.json"
    cache_path.write_text(
        '[{"ticker":"SAN","name":"BANCO SANTANDER","exchange":"BME","asset_type":"Stock","listing_status":"active"}]',
        encoding="utf-8",
    )
    monkeypatch.setattr(fetch_exchange_masterfiles, "BME_LISTED_COMPANIES_CACHE", cache_path)
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "LEGACY_BME_LISTED_COMPANIES_CACHE",
        tmp_path / "missing.json",
    )
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "fetch_bme_reference_rows",
        lambda source, session=None: (_ for _ in ()).throw(requests.RequestException("boom")),
    )

    source = next(item for item in OFFICIAL_SOURCES if item.key == "bme_listed_companies")
    rows, mode = load_bme_reference_rows(source)

    assert mode == "cache"
    assert rows == [
        {
            "ticker": "SAN",
            "name": "BANCO SANTANDER",
            "exchange": "BME",
            "asset_type": "Stock",
            "listing_status": "active",
        }
    ]


def test_load_bme_security_prices_rows_ignores_partial_cache(tmp_path, monkeypatch) -> None:
    cache_path = tmp_path / "bme_security_prices_directory.json"
    cache_path.write_text(
        '[{"ticker":"SAN","name":"BANCO SANTANDER","exchange":"BME","asset_type":"Stock","listing_status":"active"}]',
        encoding="utf-8",
    )
    monkeypatch.setattr(fetch_exchange_masterfiles, "BME_SECURITY_PRICES_CACHE", cache_path)
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "LEGACY_BME_SECURITY_PRICES_CACHE",
        tmp_path / "missing.json",
    )
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "fetch_bme_security_prices_rows",
        lambda source, session=None: (_ for _ in ()).throw(requests.RequestException("blocked")),
    )

    source = next(item for item in OFFICIAL_SOURCES if item.key == "bme_security_prices_directory")
    rows, mode = fetch_exchange_masterfiles.load_bme_security_prices_rows(source)

    assert mode == "unavailable"
    assert rows is None


def test_extract_bme_growth_detail_links_dedupes_official_ficha_links() -> None:
    html = """
    <a href="/ing/Ficha/CUATROOCHENT_ES0105509006.aspx">CUATROOCHENT.</a>
    <a href="/ing/Ficha/CUATROOCHENT_ES0105509006.aspx">CUATROOCHENT.</a>
    <a href="/ing/Indice/Ficha/IBEX_Growth_Market__All_Share_ES0S00001149.aspx">Index</a>
    <a href="/ing/Ficha/COX_ENERGY_MX01CO0U0028.aspx">COX ENERGY</a>
    """

    assert fetch_exchange_masterfiles.extract_bme_growth_detail_links(html) == [
        "https://www.bmegrowth.es/ing/Ficha/CUATROOCHENT_ES0105509006.aspx",
        "https://www.bmegrowth.es/ing/Ficha/COX_ENERGY_MX01CO0U0028.aspx",
    ]


def test_bme_growth_request_headers_use_full_browser_user_agent() -> None:
    headers = fetch_exchange_masterfiles.bme_growth_request_headers()

    assert "Chrome/" in headers["User-Agent"]
    assert headers["Referer"] == fetch_exchange_masterfiles.BME_GROWTH_PRICES_URL


def test_parse_bme_growth_detail_page_maps_ticker_name_and_isin() -> None:
    source = next(item for item in OFFICIAL_SOURCES if item.key == "bme_growth_prices")
    html = """
    <h1>SOLUCIONES CUATROOCHENTA</h1>
    <h3>Security name</h3><p>CUATROOCHENTA</p>
    <h3>Ticker</h3><p>480S</p>
    <h3>ISIN</h3><p>ES0105509006</p>
    """

    assert fetch_exchange_masterfiles.parse_bme_growth_detail_page(
        html,
        source,
        detail_url="https://www.bmegrowth.es/ing/Ficha/CUATROOCHENT_ES0105509006.aspx",
    ) == {
        "source_key": "bme_growth_prices",
        "provider": "BME Growth",
        "source_url": "https://www.bmegrowth.es/ing/Ficha/CUATROOCHENT_ES0105509006.aspx",
        "ticker": "480S",
        "name": "SOLUCIONES CUATROOCHENTA",
        "exchange": "BME",
        "asset_type": "Stock",
        "listing_status": "active",
        "reference_scope": "listed_companies_subset",
        "official": "true",
        "isin": "ES0105509006",
    }


def test_fetch_bme_growth_price_pages_replays_viewstate_without_search_submit() -> None:
    first_page = """
    <form method="post" action="/ing/Precios.aspx" id="Form1">
    <input type="hidden" name="__VIEWSTATE" value="state0" />
    <input type="hidden" name="__EVENTTARGET" value="" />
    <input type="hidden" name="__EVENTARGUMENT" value="" />
    <input type="submit" name="ctl00$Contenido$Buscar" value=" Search " />
    <input type="hidden" name="ctl00$Contenido$NumPag" id="NumPag" />
    <a href="javascript:selPag('0');">A</a><a href="javascript:selPag('1');">B</a>
    <a href="/ing/Ficha/CUATROOCHENT_ES0105509006.aspx">CUATROOCHENT.</a>
    </form>
    """
    second_page = """
    <form method="post" action="/ing/Precios.aspx" id="Form1">
    <input type="hidden" name="__VIEWSTATE" value="state1" />
    <input type="hidden" name="__EVENTTARGET" value="" />
    <input type="hidden" name="__EVENTARGUMENT" value="" />
    <input type="submit" name="ctl00$Contenido$Buscar" value=" Search " />
    <input type="hidden" name="ctl00$Contenido$NumPag" id="NumPag" />
    <a href="/ing/Ficha/COX_ENERGY_MX01CO0U0028.aspx">COX ENERGY</a>
    </form>
    """

    class FakeResponse:
        def __init__(self, text):
            self.text = text

        def raise_for_status(self):
            return None

    class FakeSession:
        def __init__(self):
            self.post_calls = []

        def get(self, url, headers=None, timeout=None):
            assert url == fetch_exchange_masterfiles.BME_GROWTH_PRICES_URL
            return FakeResponse(first_page)

        def post(self, url, data=None, headers=None, timeout=None):
            self.post_calls.append((url, data, headers))
            assert data["ctl00$Contenido$NumPag"] == "1"
            assert "ctl00$Contenido$Buscar" not in data
            assert data["__VIEWSTATE"] == "state0"
            return FakeResponse(second_page)

    session = FakeSession()

    pages = fetch_exchange_masterfiles.fetch_bme_growth_price_pages(session=session)

    assert pages == [first_page, second_page]
    assert session.post_calls[0][0] == fetch_exchange_masterfiles.BME_GROWTH_PRICES_URL


def test_parse_pse_listed_company_directory_html_maps_active_securities() -> None:
    source = MasterfileSource(
        key="pse_listed_company_directory",
        provider="PSE",
        description="Official PSE listed company directory frame",
        source_url="https://frames.pse.com.ph/listedCompany",
        format="pse_listed_company_directory_html",
    )
    payload = [
        {
            "SecuritySymbol": "BDO",
            "SecurityName": "BDO Unibank, Inc.",
            "SecurityAlias": "BDO Unibank, Inc.",
            "SecurityType": "C",
            "SecurityStatus": "T",
            "SecurityISIN": "PHY077751022",
        },
        {
            "SecuritySymbol": "PREFA",
            "SecurityName": "Preferred A",
            "SecurityType": "P",
            "SecurityStatus": "V",
            "SecurityISIN": "",
        },
        {
            "SecuritySymbol": "FMETF",
            "SecurityName": "First Metro Philippine Equity Exchange Traded Fund Inc.",
            "SecurityType": "E",
            "SecurityStatus": "T",
            "SecurityISIN": "PHY272571498",
        },
        {
            "SecuritySymbol": "NOTE1",
            "SecurityName": "Debt note",
            "SecurityType": "D",
            "SecurityStatus": "T",
            "SecurityISIN": "PH0000000001",
        },
        {
            "SecuritySymbol": "",
            "SecurityName": "Missing symbol",
            "SecurityType": "C",
            "SecurityStatus": "T",
            "SecurityISIN": "PH0000000002",
        },
    ]
    text = f'<input id="store-json" type="hidden" value="{html.escape(json.dumps(payload), quote=True)}" />'

    rows = parse_pse_listed_company_directory_html(text, source)

    assert rows == [
        {
            "source_key": "pse_listed_company_directory",
            "provider": "PSE",
            "source_url": "https://frames.pse.com.ph/listedCompany",
            "ticker": "BDO",
            "name": "BDO Unibank, Inc.",
            "exchange": "PSE",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "PHY077751022",
        },
        {
            "source_key": "pse_listed_company_directory",
            "provider": "PSE",
            "source_url": "https://frames.pse.com.ph/listedCompany",
            "ticker": "PREFA",
            "name": "Preferred A",
            "exchange": "PSE",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
        {
            "source_key": "pse_listed_company_directory",
            "provider": "PSE",
            "source_url": "https://frames.pse.com.ph/listedCompany",
            "ticker": "FMETF",
            "name": "First Metro Philippine Equity Exchange Traded Fund Inc.",
            "exchange": "PSE",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "PHY272571498",
        },
    ]


def test_load_pse_listed_company_directory_rows_falls_back_to_cache(tmp_path, monkeypatch) -> None:
    cache_path = tmp_path / "pse_listed_company_directory.json"
    cache_path.write_text(
        '[{"ticker":"BDO","name":"BDO Unibank, Inc.","exchange":"PSE","asset_type":"Stock","listing_status":"active"}]',
        encoding="utf-8",
    )
    monkeypatch.setattr(fetch_exchange_masterfiles, "PSE_LISTED_COMPANY_DIRECTORY_CACHE", cache_path)
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "LEGACY_PSE_LISTED_COMPANY_DIRECTORY_CACHE",
        tmp_path / "missing.json",
    )
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "fetch_pse_listed_company_directory",
        lambda source, session=None: (_ for _ in ()).throw(requests.RequestException("boom")),
    )

    source = next(item for item in OFFICIAL_SOURCES if item.key == "pse_listed_company_directory")
    rows, mode = load_pse_listed_company_directory_rows(source)

    assert mode == "cache"
    assert rows == [
        {
            "ticker": "BDO",
            "name": "BDO Unibank, Inc.",
            "exchange": "PSE",
            "asset_type": "Stock",
            "listing_status": "active",
        }
    ]


def test_parse_pse_cz_share_links_extracts_isin_detail_links() -> None:
    text = """
    <td class="item-title js-item-title" data-title="COLTCZ">
        <a href="/en/detail/CZ0009008942"> COLTCZ </a>
        <div class="isin"> CZ0009008942 </div>
    </td>
    <td class="item-title js-item-title" data-title="DOOSAN">
        <a href="/en/detail/CZ1008000310"> DOOSAN </a>
        <div class="isin"> CZ1008000310 </div>
    </td>
    """

    rows = parse_pse_cz_share_links(text, "https://www.pse.cz/en/market-data/shares/prime-market")

    assert rows == [
        {
            "isin": "CZ0009008942",
            "label": "COLTCZ",
            "detail_url": "https://www.pse.cz/en/detail/CZ0009008942",
        },
        {
            "isin": "CZ1008000310",
            "label": "DOOSAN",
            "detail_url": "https://www.pse.cz/en/detail/CZ1008000310",
        },
    ]


def test_parse_pse_cz_detail_ticker_prefers_xetra_ticker() -> None:
    text = """
    <table>
        <tr><td>Ticker Bloomberg</td><td>DSPW CP Equity</td></tr>
        <tr><td>Ticker Reuters</td><td>DSPW.PR</td></tr>
        <tr><td>Ticker Xetra®</td><td>DSPW</td></tr>
    </table>
    """

    assert parse_pse_cz_detail_ticker(text) == "DSPW"


def test_build_pse_cz_share_rows_uses_current_listing_name_and_detail_ticker(tmp_path) -> None:
    listings_path = tmp_path / "listings.csv"
    listings_path.write_text(
        "\n".join(
            [
                "listing_key,ticker,exchange,name,asset_type,stock_sector,etf_category,country,country_code,isin,aliases",
                "PSE_CZ::CZG,CZG,PSE_CZ,Colt CZ Group SE,Stock,Industrials,,Czech Republic,CZ,CZ0009008942,",
                "PSE_CZ::DSPW,DSPW,PSE_CZ,Doosan Skoda Power as,Stock,,,,CZ,CZ1008000310,",
            ]
        ),
        encoding="utf-8",
    )
    source = MasterfileSource(
        key="pse_cz_shares_directory",
        provider="Prague Stock Exchange",
        description="Official Prague Stock Exchange share market directory",
        source_url="https://www.pse.cz/en/market-data/shares/prime-market",
        format="pse_cz_shares_directory_html",
        reference_scope="listed_companies_subset",
    )
    links = [
        {
            "isin": "CZ0009008942",
            "label": "COLTCZ",
            "detail_url": "https://www.pse.cz/en/detail/CZ0009008942",
        },
        {
            "isin": "CZ1008000310",
            "label": "DOOSAN",
            "detail_url": "https://www.pse.cz/en/detail/CZ1008000310",
        },
    ]
    detail_html_by_isin = {
        "CZ0009008942": "<tr><td>Ticker Xetra®</td><td>CZG</td></tr>",
        "CZ1008000310": "<tr><td>Ticker Reuters</td><td>DSPW.PR</td></tr>",
    }

    rows = build_pse_cz_share_rows(
        links,
        detail_html_by_isin,
        source,
        listings_path=listings_path,
    )

    assert rows == [
        {
            "source_key": "pse_cz_shares_directory",
            "provider": "Prague Stock Exchange",
            "source_url": "https://www.pse.cz/en/detail/CZ0009008942",
            "ticker": "CZG",
            "name": "Colt CZ Group SE",
            "exchange": "PSE_CZ",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "CZ0009008942",
        },
        {
            "source_key": "pse_cz_shares_directory",
            "provider": "Prague Stock Exchange",
            "source_url": "https://www.pse.cz/en/detail/CZ1008000310",
            "ticker": "DSPW",
            "name": "Doosan Skoda Power as",
            "exchange": "PSE_CZ",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "CZ1008000310",
        },
    ]


def test_parse_bse_bw_listed_companies_html_matches_current_rows_by_name(tmp_path) -> None:
    listings_path = tmp_path / "listings.csv"
    listings_path.write_text(
        "\n".join(
            [
                "listing_key,ticker,exchange,name,asset_type,stock_sector,etf_category,country,country_code,isin,aliases",
                "BSE_BW::ABBL-EQO,ABBL-EQO,BSE_BW,ABSA BANK OF BOTSWANA LIMITED,Stock,,,,BW,BW0000000025,",
                "BSE_BW::BTE-EQO,BTE-EQO,BSE_BW,BOTALA,Stock,,,,AU,AU0000224552,",
            ]
        ),
        encoding="utf-8",
    )
    source = MasterfileSource(
        key="bse_bw_listed_companies",
        provider="BSE Botswana",
        description="Official Botswana Stock Exchange listed companies directory",
        source_url="https://www.bse.co.bw/companies/",
        format="bse_bw_listed_companies_html",
        reference_scope="listed_companies_subset",
    )
    text = """
    <div class="lvca-panel">
        <div class="lvca-panel-title">ABSA Bank of Botswana Limited</div>
        <div class="lvca-panel-content">
            <table>
                <tr><th>COUNTER</th><td>ABSA</td></tr>
                <tr><th>Sector</th><td>Banking</td></tr>
                <tr><th>Board</th><td>Domestic Main Board</td></tr>
            </table>
        </div>
    </div><!-- .lvca-panel -->
    <div class="lvca-panel">
        <div class="lvca-panel-title">Botala Energy Limited</div>
        <div class="lvca-panel-content">
            <table>
                <tr><th>COUNTER</th><td>BOTALA</td></tr>
                <tr><th>Sector</th><td>Mining</td></tr>
            </table>
        </div>
    </div><!-- .lvca-panel -->
    """

    rows = parse_bse_bw_listed_companies_html(text, source, listings_path=listings_path)

    assert rows == [
        {
            "source_key": "bse_bw_listed_companies",
            "provider": "BSE Botswana",
            "source_url": "https://www.bse.co.bw/companies/",
            "ticker": "ABBL-EQO",
            "name": "ABSA BANK OF BOTSWANA LIMITED",
            "exchange": "BSE_BW",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "BW0000000025",
            "sector": "Banking",
        },
        {
            "source_key": "bse_bw_listed_companies",
            "provider": "BSE Botswana",
            "source_url": "https://www.bse.co.bw/companies/",
            "ticker": "BTE-EQO",
            "name": "BOTALA",
            "exchange": "BSE_BW",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "AU0000224552",
            "sector": "Mining",
        },
    ]


def test_parse_cse_ma_jsonapi_collection_extracts_official_instruments() -> None:
    source = MasterfileSource(
        key="cse_ma_listed_companies",
        provider="Casablanca Stock Exchange",
        description="Official Casablanca Stock Exchange active equity instruments JSONAPI directory",
        source_url="https://www.casablanca-bourse.com/en/marche-cash/instruments-actions",
        format="cse_ma_listed_companies_json",
        reference_scope="exchange_directory",
    )
    payload = {
        "data": [
            {
                "type": "instrument",
                "id": "instrument-1",
                "attributes": {
                    "symbol": "AFM",
                    "codeISIN": "MA0000012296",
                    "libelleEN": "AFMA",
                    "instrument_url": "/en/live-market/instruments/AFM",
                },
                "relationships": {
                    "codeSociete": {"data": {"type": "emetteur", "id": "issuer-1"}},
                },
            },
            {
                "type": "instrument",
                "id": "instrument-2",
                "attributes": {
                    "symbol": "ADI",
                    "codeISIN": "MA0000011819",
                    "libelleEN": "ALLIANCES",
                    "instrument_url": "/en/live-market/instruments/ADI",
                },
                "relationships": {
                    "codeSociete": {"data": {"type": "emetteur", "id": "issuer-2"}},
                },
            },
        ],
        "included": [
            {
                "type": "emetteur",
                "id": "issuer-1",
                "attributes": {"raisonSociale": "AFMA SA"},
            },
            {
                "type": "emetteur",
                "id": "issuer-2",
                "attributes": {"name": "ALLIANCES DEVELOPPEMENT IMMOBILIER SA"},
            },
        ],
        "links": {"next": {"href": "https://api.casablanca-bourse.com/en/api/bourse_data/instrument?page%5Boffset%5D=50"}},
    }

    rows, next_url = parse_cse_ma_jsonapi_collection(payload, source)

    assert next_url == "https://api.casablanca-bourse.com/en/api/bourse_data/instrument?page%5Boffset%5D=50"
    assert rows == [
        {
            "source_key": "cse_ma_listed_companies",
            "provider": "Casablanca Stock Exchange",
            "source_url": "https://www.casablanca-bourse.com/en/live-market/instruments/AFM",
            "ticker": "AFM",
            "name": "AFMA SA",
            "exchange": "CSE_MA",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "MA0000012296",
        },
        {
            "source_key": "cse_ma_listed_companies",
            "provider": "Casablanca Stock Exchange",
            "source_url": "https://www.casablanca-bourse.com/en/live-market/instruments/ADI",
            "ticker": "ADI",
            "name": "ALLIANCES DEVELOPPEMENT IMMOBILIER SA",
            "exchange": "CSE_MA",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "MA0000011819",
        },
    ]


def test_parse_nse_ke_listed_companies_html_extracts_symbols_isins_and_sectors() -> None:
    source = MasterfileSource(
        key="nse_ke_listed_companies",
        provider="NSE Kenya",
        description="Official Nairobi Securities Exchange listed companies page",
        source_url="https://www.nse.co.ke/listed-companies/",
        format="nse_ke_listed_companies_html",
        reference_scope="exchange_directory",
    )
    html = """
    [toggle title=&#8221;BANKING&#8221;]
    [nectar_animated_title heading_tag=&#8221;h6&#8221; text=&#8221;Absa Bank Kenya PLC&#8221;][vc_column_text]
    <strong>Trading Symbol:<span>ABSA</span><br /></strong></p>
    <p><strong>ISIN CODE:<span>KE0000000067</span><br /></strong>[/vc_column_text]
    [toggle title=&#8221;EXCHANGE TRADED FUND&#8221;]
    [nectar_animated_title heading_tag=&#8221;h6&#8221; text=&#8221;Satrix MSCI World Feeder ETF&#8221;][vc_column_text]
    <strong>Trading Symbol:SMWF.E0000<br /></strong></p>
    <p><strong>ISIN CODE:ZAE000246104<br /></strong>[/vc_column_text]
    [toggle title=&#8221;REAL ESTATE INVESTMENT TRUST&#8221;]
    [nectar_animated_title heading_tag=&#8221;h6&#8221; text=&#8221;Shri Krishana Overseas (SKL)&#8221;][vc_column_text]
    <strong>Trading Symbol:SKL.O0000<br /></strong></p>
    <p><strong>ISIN: KE9900001216</strong>[/vc_column_text]
    """

    assert parse_nse_ke_listed_companies_html(html, source) == [
        {
            "source_key": "nse_ke_listed_companies",
            "provider": "NSE Kenya",
            "source_url": "https://www.nse.co.ke/listed-companies/",
            "ticker": "ABSA",
            "name": "Absa Bank Kenya PLC",
            "exchange": "NSE_KE",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "KE0000000067",
            "sector": "Financials",
        },
        {
            "source_key": "nse_ke_listed_companies",
            "provider": "NSE Kenya",
            "source_url": "https://www.nse.co.ke/listed-companies/",
            "ticker": "SMWF.E0000",
            "name": "Satrix MSCI World Feeder ETF",
            "exchange": "NSE_KE",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "ZAE000246104",
        },
        {
            "source_key": "nse_ke_listed_companies",
            "provider": "NSE Kenya",
            "source_url": "https://www.nse.co.ke/listed-companies/",
            "ticker": "SKL.O0000",
            "name": "Shri Krishana Overseas (SKL)",
            "exchange": "NSE_KE",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "KE9900001216",
            "sector": "Real Estate",
        },
    ]


def test_parse_sem_isin_workbook_gates_official_rows_by_current_listing_isin(tmp_path) -> None:
    listings_path = tmp_path / "listings.csv"
    listings_path.write_text(
        "\n".join(
            [
                "listing_key,ticker,exchange,name,asset_type,stock_sector,etf_category,country,country_code,isin,aliases",
                "SEM::ADBF,ADBF,SEM,AFRICAN DOMESTIC BOND FUND ETF - (USD),ETF,,Bond,Mauritius,MU,MU0607S00004,",
                "SEM::AEIB,AEIB,SEM,AFREXIMBANK,Stock,Financials,,Mauritius,MU,MU0559N00040,",
            ]
        ),
        encoding="utf-8",
    )
    workbook = io.BytesIO()
    with pd.ExcelWriter(workbook) as writer:
        pd.DataFrame(
            [
                ["COMPANIES LISTED ON THE OFFICIAL MARKET", None, None, None, None],
                [None, None, None, None, None],
                ["ISSUER NAME", "ISSUE DESCRIPTION", "ISIN", "CFI", "FISN"],
                [
                    "AFRICAN DOMESTIC BOND FUND",
                    "REDEEMABLE PARTICIPATING SHARES",
                    "MU0607S00004",
                    "CEOIBS",
                    "ADBF/RED PART SH USD",
                ],
                ["AFRICAN EXPORT-IMPORT BANK", "DEPOSITARY RECEIPT", "MU0559N00040", "EDSNDR", "AFREXIMBANK/DR USD"],
                ["ABC BANKING CORPORATION LIMITED", "10 YEARS NOTE", "MU0507D01634", "DTFUFR", "ABC BANKING/NOTE"],
            ]
        ).to_excel(writer, sheet_name="Official Market", header=False, index=False)
        pd.DataFrame([["ISSUER NAME", "ISSUE DESCRIPTION", "ISIN", "CFI", "FISN"]]).to_excel(
            writer,
            sheet_name="DEM",
            header=False,
            index=False,
        )
    source = MasterfileSource(
        key="sem_isin",
        provider="SEM",
        description="Official Stock Exchange of Mauritius CDS ISIN workbook",
        source_url="https://www.stockexchangeofmauritius.com/media/11318/isin.xlsx",
        format="sem_isin_xlsx",
        reference_scope="exchange_directory",
    )

    assert parse_sem_isin_workbook(workbook.getvalue(), source, listings_path=listings_path) == [
        {
            "source_key": "sem_isin",
            "provider": "SEM",
            "source_url": "https://www.stockexchangeofmauritius.com/cds/isins",
            "ticker": "ADBF",
            "name": "AFRICAN DOMESTIC BOND FUND ETF - (USD)",
            "exchange": "SEM",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "MU0607S00004",
            "sector": "Bond",
        },
        {
            "source_key": "sem_isin",
            "provider": "SEM",
            "source_url": "https://www.stockexchangeofmauritius.com/cds/isins",
            "ticker": "AEIB",
            "name": "AFREXIMBANK",
            "exchange": "SEM",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "MU0559N00040",
            "sector": "Financials",
        },
    ]


def test_parse_bse_hu_marketdata_html_gates_to_exact_current_tickers(tmp_path) -> None:
    listings_path = tmp_path / "listings.csv"
    listings_path.write_text(
        "\n".join(
            [
                "listing_key,ticker,exchange,name,asset_type,stock_sector,etf_category,country,country_code,isin,aliases",
                "BSE_HU::ALTEO,ALTEO,BSE_HU,ALTEO Energiaszolgaltato Nyrt.,Stock,Utilities,,Hungary,HU,,",
                "BSE_HU::BAYER,BAYER,BSE_HU,Bayer AG,Stock,Health Care,,Germany,DE,,",
                "BSE_HU::AUTOW,AUTOW,BSE_HU,AutoWallis Nyrt,Stock,Consumer Discretionary,,Hungary,HU,,",
            ]
        ),
        encoding="utf-8",
    )
    text = """
    <script>
    window.dataSourceResults={
      "TickerDataSource":[
        {"seccode":"ALTEO","isin":"HU0000155726","instrgrpids":["RESZVENY"]},
        {"seccode":"AUTOWALLIS","isin":"HU0000164504","instrgrpids":["RESZVENY"]},
        {"seccode":"BAYER","isin":"DE000BAY0017","instrgrpids":["BETAMARKET"]}
      ],
      "PromptTablesDataSource;instrgrpid=W_BETF;filterEmpty=false":{"rows":[
        {"seccode":"ETFCETOPOTP","isin":"HU0000734454","instrgrpids":["W_ETF"]}
      ]}
    };
    </script>
    """
    source = MasterfileSource(
        key="bse_hu_listed_companies",
        provider="Budapest Stock Exchange",
        description="Official Budapest Stock Exchange embedded market-data feed",
        source_url="https://www.bet.hu/oldalak/beta_piac#reszvenyek",
        format="bse_hu_marketdata_html",
        reference_scope="listed_companies_subset",
    )

    assert parse_bse_hu_marketdata_html(text, source, listings_path=listings_path) == [
        {
            "source_key": "bse_hu_listed_companies",
            "provider": "Budapest Stock Exchange",
            "source_url": "https://www.bet.hu/oldalak/beta_piac#reszvenyek",
            "ticker": "ALTEO",
            "name": "ALTEO Energiaszolgaltato Nyrt.",
            "exchange": "BSE_HU",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "HU0000155726",
        },
        {
            "source_key": "bse_hu_listed_companies",
            "provider": "Budapest Stock Exchange",
            "source_url": "https://www.bet.hu/oldalak/beta_piac#reszvenyek",
            "ticker": "BAYER",
            "name": "Bayer AG",
            "exchange": "BSE_HU",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "DE000BAY0017",
        },
    ]


def test_parse_egx_listed_stocks_viewstate_matches_by_isin(tmp_path) -> None:
    listings_path = tmp_path / "listings.csv"
    listings_path.write_text(
        "\n".join(
            [
                "listing_key,ticker,exchange,name,asset_type,stock_sector,etf_category,country,country_code,isin,aliases",
                "EGX::ACAP,ACAP,EGX,Local A Capital,Stock,,,Egypt,EG,EGS697S1C015,",
                "EGX::ASCM,ASCM,EGX,ASEC Mining,Stock,,,Egypt,EG,EGS10001C013,",
            ]
        ),
        encoding="utf-8",
    )
    strings = [
        "A Capital Holding",
        "show('ctl00_C_L_GridView2_ctl02_divContainer', 'x')",
        "show('ctl00_C_L_GridView2_ctl02_divContainer', 'x')",
        "/en/StocksData.aspx?ISIN=EGS697S1C015",
        "/en/NewsList.aspx?ISIN=EGS697S1C015",
        "EGS697S1C015",
        "Real Estate",
        "ASEC Company For Mining - ASCOM",
        "show('ctl00_C_L_GridView2_ctl03_divContainer', 'x')",
        "/en/StocksData.aspx?ISIN=EGS10001C013",
        "EGS10001C013",
        "Basic Resources",
        "Not In Current Universe",
        "show('ctl00_C_L_GridView2_ctl04_divContainer', 'x')",
        "/en/StocksData.aspx?ISIN=EGS597R1C017",
        "EGS597R1C017",
        "Education Services",
    ]
    raw = b"".join(b"\x05" + bytes([len(value.encode())]) + value.encode() for value in strings)
    viewstate = html.escape(b64encode(raw).decode("ascii"))
    source = MasterfileSource(
        key="egx_listed_stocks",
        provider="EGX",
        description="Official EGX listed stocks",
        source_url="https://www.egx.com.eg/en/ListedStocks.aspx",
        format="egx_listed_stocks_viewstate",
        reference_scope="listed_companies_subset",
    )

    rows = parse_egx_listed_stocks_viewstate(
        f'<form id="aspnetForm"><input type="hidden" name="__VIEWSTATE" value="{viewstate}"></form>',
        source,
        listings_path=listings_path,
    )

    assert rows == [
        {
            "source_key": "egx_listed_stocks",
            "provider": "EGX",
            "source_url": "https://www.egx.com.eg/en/ListedStocks.aspx",
            "ticker": "ACAP",
            "name": "A Capital Holding",
            "exchange": "EGX",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "EGS697S1C015",
            "sector": "Real Estate",
        },
        {
            "source_key": "egx_listed_stocks",
            "provider": "EGX",
            "source_url": "https://www.egx.com.eg/en/ListedStocks.aspx",
            "ticker": "ASCM",
            "name": "ASEC Company For Mining - ASCOM",
            "exchange": "EGX",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "EGS10001C013",
            "sector": "Materials",
        },
    ]


def test_egx_viewstate_string_extractor_and_sector_normalizer() -> None:
    strings = ["A Capital Holding", "Basic Resources", "Textile & Durables"]
    raw = b"".join(b"\x05" + bytes([len(value.encode())]) + value.encode() for value in strings)

    assert extract_aspnet_viewstate_strings(b64encode(raw).decode("ascii")) == strings
    assert normalize_egx_sector("Basic Resources") == "Materials"
    assert normalize_egx_sector("IT , Media &amp; Communication Services") == "Communication Services"
    assert normalize_egx_sector("Textile & Durables") == "Consumer Discretionary"


def test_parse_zse_zw_issuers_payload_maps_exact_ticker_and_isin_fallback(tmp_path) -> None:
    listings_path = tmp_path / "listings.csv"
    listings_path.write_text(
        "\n".join(
            [
                "listing_key,ticker,exchange,name,asset_type,stock_sector,etf_category,country,country_code,isin,aliases",
                "ZSE_ZW::NPKZ,NPKZ,ZSE_ZW,NAMPAK ZIMBABWE LIMITED,Stock,,,Zimbabwe,ZW,ZW0009012213,",
                "ZSE_ZW::AFDS,AFDS,ZSE_ZW,AFRICAN DISTILLERS LIMITED,Stock,,,Zimbabwe,ZW,ZW0009011025,",
            ]
        ),
        encoding="utf-8",
    )
    source = MasterfileSource(
        key="zse_zw_listed_companies",
        provider="ZSE Zimbabwe",
        description="Official Zimbabwe Stock Exchange issuer APIs",
        source_url="https://www.zse.co.zw/listed-securities/equity",
        format="zse_zw_issuers_json",
        reference_scope="listed_companies_subset",
    )

    assert parse_zse_zw_issuers_payload(
        {
            "issuers": [{"short_name": "NPKZ.ZW", "name": "Nampak Zimbabwe Limited"}],
            "price_sheet": [{"isin": "ZW0009011025", "name": "AFDIS DISTILLERS LIMITED"}],
        },
        source,
        listings_path=listings_path,
    ) == [
        {
            "source_key": "zse_zw_listed_companies",
            "provider": "ZSE Zimbabwe",
            "source_url": "https://www.zse.co.zw/listed-securities/equity",
            "ticker": "NPKZ",
            "name": "Nampak Zimbabwe Limited",
            "exchange": "ZSE_ZW",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "ZW0009012213",
        },
        {
            "source_key": "zse_zw_listed_companies",
            "provider": "ZSE Zimbabwe",
            "source_url": "https://www.zse.co.zw/listed-securities/equity",
            "ticker": "AFDS",
            "name": "AFDIS DISTILLERS LIMITED",
            "exchange": "ZSE_ZW",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "ZW0009011025",
        },
    ]


def test_parse_small_african_exchange_html_sources_gate_to_current_rows(tmp_path) -> None:
    listings_path = tmp_path / "listings.csv"
    listings_path.write_text(
        "\n".join(
            [
                "listing_key,ticker,exchange,name,asset_type,stock_sector,etf_category,country,country_code,isin,aliases",
                "DSE_TZ::CRDB,CRDB,DSE_TZ,CRDB BANK LTD,Stock,Financials,,Tanzania,TZ,TZ1996100305,",
                "DSE_TZ::TCCL,TCCL,DSE_TZ,TANGA CEMENT PUBLIC LTD CO.,Stock,Materials,,Tanzania,TZ,TZ1996100057,",
                "MSE_MW::FDHB,FDHB,MSE_MW,FDH BANK PLC,Stock,Financials,,Malawi,MW,,",
                "USE_UG::AIRTELUGAN,AIRTELUGAN,USE_UG,AIRTEL UGANDA LIMITED,Stock,Communication Services,,Uganda,UG,UG0000001582,",
                "USE_UG::BATU,BATU,USE_UG,BRITISH AMERICAN TOBACCO UGANDA LTD,Stock,Consumer Staples,,Uganda,UG,UG0000000022,",
                "USE_UG::BOBU,BOBU,USE_UG,BANK OF BARODA UGANDA LTD,Stock,Financials,,Uganda,UG,UG0000000055,",
                "USE_UG::DFCU,DFCU,USE_UG,DFCU GROUP,Stock,Financials,,Uganda,UG,UG0000000147,",
                "USE_UG::MTNU,MTNU,USE_UG,MTN UGANDA LIMITED,Stock,Communication Services,,Uganda,UG,UG0000001566,",
                "USE_UG::UMEM,UMEM,USE_UG,UMEME LIMITED,Stock,Utilities,,Uganda,UG,UG0000001145,",
                "RSE::BLR,BLR,RSE,BRALIRWA LTD,Stock,Consumer Staples,,Rwanda,RW,RW000A1H63N6,",
                "GSE::GCB,GCB,GSE,GHANA COMMERCIAL BANK LTD.,Stock,Financials,,Ghana,GH,GH0000000094,",
                "GSE::AADS,AADS,GSE,ANGLOGOLD ASHANTI GHANIAN DEPOSITORY SHARES,Stock,,,Ghana,GH,GH0000000615,",
                "LUSE::ZMBF,ZMBF,LUSE,ZAMBEEF PRODUCTS PLC,Stock,Consumer Staples,,Zambia,ZM,ZM0000000201,",
                "LUSE::CECZ,CECZ,LUSE,COPPERBELT ENERGY CORPORATION PLC,Stock,Utilities,,Zambia,ZM,ZM0000000136,",
            ]
        ),
        encoding="utf-8",
    )
    dse_source = MasterfileSource(
        key="dse_tz_listed_companies",
        provider="DSE Tanzania",
        description="Official DSE table",
        source_url="https://www.dse.co.tz/listed/company/list",
        format="dse_tz_listed_companies_html",
        reference_scope="listed_companies_subset",
    )
    mse_source = MasterfileSource(
        key="mse_mw_listed_companies",
        provider="MSE Malawi",
        description="Official MSE mainboard",
        source_url="https://mse.co.mw/market/mainboard",
        format="mse_mw_mainboard_html",
        reference_scope="listed_companies_subset",
    )
    use_source = MasterfileSource(
        key="use_ug_listed_companies",
        provider="USE Uganda",
        description="Official USE snapshot",
        source_url="https://www.use.or.ug/market-statistics/market-snapshot",
        format="use_ug_market_snapshot_html",
        reference_scope="listed_companies_subset",
    )
    rse_source = MasterfileSource(
        key="rse_listed_companies",
        provider="RSE",
        description="Official RSE cards",
        source_url="https://www.rse.rw/listed-compagnies",
        format="rse_listed_companies_html",
        reference_scope="listed_companies_subset",
    )
    gse_source = MasterfileSource(
        key="gse_listed_companies",
        provider="GSE",
        description="Official GSE listed-company markdown",
        source_url="https://gse.com.gh/listed-companies/",
        format="gse_listed_companies_markdown",
        reference_scope="listed_companies_subset",
    )
    luse_source = MasterfileSource(
        key="luse_listed_companies",
        provider="LuSE",
        description="Official LuSE listed-company markdown",
        source_url="https://www.luse.co.zm/listed-companies/",
        format="luse_listed_companies_markdown",
        reference_scope="listed_companies_subset",
    )

    dse_html = """
    <table><tr><th>No</th><th>Company Code</th><th>Company description</th><th>Profile</th></tr>
    <tr><td>1</td><td>CRDB</td><td></td><td>View</td></tr>
    <tr><td>2</td><td>TCPLC</td><td></td><td>View</td></tr>
    <tr><td>2</td><td>UNKNOWN</td><td></td><td>View</td></tr></table>
    """
    mse_html = '<a href="https://mse.co.mw/company/MWFDHB001166">FDHB</a><a href="/company/MWXXXX010000">UNKNOWN</a>'
    use_html = """
    <table><tr><th>Stock Code</th><th>Name</th></tr>
    <tr><td>AIRTEL UGANDA</td><td>AIRTEL UGANDA LIMITED</td></tr>
    <tr><td>BOBU</td><td>Bank of Baroda Uganda</td></tr>
    <tr><td>BATU</td><td>British American Tobacco Uganda</td></tr>
    <tr><td>DFCU</td><td>DEVELOPMENT FINANCE COMPANY OF UGANDA LTD</td></tr>
    <tr><td>MTNU</td><td>MTN UGANDA LIMITED</td></tr>
    <tr><td>UMEME</td><td>UMEME LIMITED</td></tr>
    <tr><td>NOPE</td><td>No Match PLC</td></tr></table>
    """
    rse_html = """
    <span class="listed-compagnies-card-item-name">Bralirwa</span>
    <span class="listed-compagnies-card-item-name">No Match</span>
    """
    gse_markdown = """
    | Symbol | Company | Date Listed |
    | --- | --- | --- |
    | [GCB](https://gse.com.gh/listed-companies/GCB) | Ghana Commercial Bank Limited | 17/05/1996 |
    | [AGA](https://gse.com.gh/listed-companies/AGA) | AngloGold Ashanti Plc | 27/04/2004 |
    """
    luse_markdown = """
    *    ZMBF

    # Zambeef Plc

    *    CEC

    # Copperbelt Energy Corporation Plc
    """

    assert [row["ticker"] for row in parse_dse_tz_listed_companies_html(dse_html, dse_source, listings_path=listings_path)] == [
        "CRDB",
        "TCCL",
    ]
    assert parse_mse_mw_mainboard_html(mse_html, mse_source, listings_path=listings_path)[0]["isin"] == "MWFDHB001166"
    assert [row["ticker"] for row in parse_use_ug_market_snapshot_html(use_html, use_source, listings_path=listings_path)] == [
        "AIRTELUGAN",
        "BOBU",
        "BATU",
        "DFCU",
        "MTNU",
        "UMEM",
    ]
    assert [row["ticker"] for row in parse_rse_listed_companies_html(rse_html, rse_source, listings_path=listings_path)] == [
        "BLR"
    ]
    gse_rows = parse_gse_listed_companies_markdown(gse_markdown, gse_source, listings_path=listings_path)
    assert [(row["ticker"], row["name"]) for row in gse_rows] == [
        ("GCB", "Ghana Commercial Bank Limited")
    ]
    luse_rows = parse_luse_listed_companies_markdown(luse_markdown, luse_source, listings_path=listings_path)
    assert [(row["ticker"], row["name"]) for row in luse_rows] == [("ZMBF", "Zambeef Plc")]


def test_parse_bolsa_santiago_instruments_payload_gates_to_current_rows(tmp_path) -> None:
    listings_path = tmp_path / "listings.csv"
    listings_path.write_text(
        "\n".join(
            [
                "listing_key,ticker,exchange,name,asset_type,stock_sector,etf_category,country,country_code,isin,aliases",
                "SSE_CL::AGUAS-A,AGUAS-A,SSE_CL,Aguas Andinas S.A,Stock,Utilities,,Chile,CL,CL0000000035,",
                "SSE_CL::AZULAZUL,AZULAZUL,SSE_CL,Azul Azul Sa,Stock,Consumer Discretionary,,Chile,CL,CL0000006172,",
                "SSE_CL::CFIBCHREN1,CFIBCHREN1,SSE_CL,Fondo de Inversión Banchile Rentas Inmobiliarias,ETF,,Real Estate,Chile,CL,,",
                "BCBA::AZUL AZUL,AZUL AZUL,BCBA,Wrong Exchange,Stock,,,Argentina,AR,,",
            ]
        ),
        encoding="utf-8",
    )
    source = MasterfileSource(
        key="bolsa_santiago_instruments",
        provider="Bolsa de Santiago",
        description="Official Bolsa de Santiago instruments API",
        source_url="https://www.bolsadesantiago.com/api/RV_ResumenMercado/getInstrumentos",
        format="bolsa_santiago_instruments_json",
        reference_scope="exchange_directory",
    )
    payload = {
        "listaResult": [
            {"NEMO": "AGUAS-A", "RAZON_SOCIAL": "AGUAS ANDINAS S.A., SERIE A", "TIPO": "A"},
            {"NEMO": "AZUL AZUL", "RAZON_SOCIAL": "AZUL AZUL S.A.", "TIPO": "A"},
            {
                "NEMO": "CFIBCHREN1",
                "RAZON_SOCIAL": "FONDO DE INVERSIÓN BANCHILE RENTAS INMOBILIARIAS",
                "TIPO": "A",
            },
            {"NEMO": "AAPL", "RAZON_SOCIAL": "APPLE INC", "TIPO": "A"},
        ]
    }

    rows = parse_bolsa_santiago_instruments_payload(payload, source, listings_path=listings_path)

    assert [(row["ticker"], row["name"], row["asset_type"], row["isin"]) for row in rows] == [
        ("AGUAS-A", "AGUAS ANDINAS S.A., SERIE A", "Stock", "CL0000000035"),
        ("AZULAZUL", "AZUL AZUL S.A.", "Stock", "CL0000006172"),
        (
            "CFIBCHREN1",
            "FONDO DE INVERSIÓN BANCHILE RENTAS INMOBILIARIAS",
            "ETF",
            "",
        ),
    ]


def test_parse_nasdaq_mutual_fund_quote_payload_gates_to_fund_network() -> None:
    source = MasterfileSource(
        key="nasdaq_mutual_fund_quotes",
        provider="Nasdaq",
        description="Official Nasdaq Fund Network quote API",
        source_url="https://api.nasdaq.com/api/screener/mutualfunds",
        format="nasdaq_mutual_fund_quote_json",
        reference_scope="security_lookup_subset",
    )
    listing = {
        "ticker": "VRGWX",
        "exchange": "NMFQS",
        "name": "Vanguard Russell 1000 Growth Index Fund Institutional Shares",
        "asset_type": "ETF",
        "isin": "US92206C6729",
    }
    payload = {
        "data": {
            "symbol": "VRGWX",
            "companyName": "Vanguard Russell 1000 Growth Index Fd Insti Cl",
            "exchange": "Nasdaq Fund Network",
            "stockType": "Managed Fund",
        }
    }

    row = parse_nasdaq_mutual_fund_quote_payload(payload, source, listing)

    assert row == {
        "source_key": "nasdaq_mutual_fund_quotes",
        "provider": "Nasdaq",
        "source_url": "https://api.nasdaq.com/api/quote/VRGWX/info?assetclass=mutualfunds",
        "ticker": "VRGWX",
        "name": "Vanguard Russell 1000 Growth Index Fd Insti Cl",
        "exchange": "NMFQS",
        "asset_type": "ETF",
        "listing_status": "active",
        "reference_scope": "security_lookup_subset",
        "official": "true",
        "isin": "US92206C6729",
    }
    assert parse_nasdaq_mutual_fund_quote_payload(
        {"data": {**payload["data"], "exchange": "Other"}},
        source,
        listing,
    ) is None


def test_parse_bvc_rv_issuers_payload_gates_to_current_bvc_rows(tmp_path) -> None:
    listings_path = tmp_path / "listings.csv"
    listings_path.write_text(
        "\n".join(
            [
                "listing_key,ticker,exchange,name,asset_type,stock_sector,etf_category,country,country_code,isin,aliases",
                "BVC::CELSIA,CELSIA,BVC,Celsia: CELSIA,Stock,,,Colombia,CO,,",
                "CSE_LK::CELSIA,CELSIA,CSE_LK,Celsia: CELSIA,Stock,,,Sri Lanka,LK,,",
                "BVC::ENKA,ENKA,BVC,Enka de Colombia S.A.: ENKA,Stock,,,Colombia,CO,,",
            ]
        ),
        encoding="utf-8",
    )
    source = MasterfileSource(
        key="bvc_colombia_issuers",
        provider="BVC",
        description="Official BVC local-equity issuer API",
        source_url="https://rest.bvc.com.co/market-information/rv/lvl-3/issuer?filters[marketDataRv][type]=Local",
        format="bvc_rv_issuers_json",
        reference_scope="listed_companies_subset",
    )
    payload = {
        "data": [
            {"issuerName": "CELSIA", "symbol": "CELSIA", "industryName": {"en": "Utilities"}},
            {"issuerName": "Enka de Colombia", "symbol": "ENKA", "industryName": {"en": "Basic Materials"}},
            {"issuerName": "No Match", "symbol": "NOPE", "industryName": {"en": "Financials"}},
        ]
    }

    rows = parse_bvc_rv_issuers_payload(payload, source, listings_path=listings_path)

    assert rows == [
        {
            "source_key": "bvc_colombia_issuers",
            "provider": "BVC",
            "source_url": "https://rest.bvc.com.co/market-information/rv/lvl-3/issuer?filters[marketDataRv][type]=Local",
            "ticker": "CELSIA",
            "name": "CELSIA",
            "exchange": "BVC",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "",
            "sector": "Utilities",
        },
        {
            "source_key": "bvc_colombia_issuers",
            "provider": "BVC",
            "source_url": "https://rest.bvc.com.co/market-information/rv/lvl-3/issuer?filters[marketDataRv][type]=Local",
            "ticker": "ENKA",
            "name": "Enka de Colombia",
            "exchange": "BVC",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "",
            "sector": "Materials",
        },
    ]


def test_parse_cse_lk_all_security_code_payload_gates_active_current_symbols(tmp_path) -> None:
    listings_path = tmp_path / "listings.csv"
    listings_path.write_text(
        "\n".join(
            [
                "listing_key,ticker,exchange,name,asset_type,stock_sector,etf_category,country,country_code,isin,aliases",
                "CSE_LK::ABAN,ABAN,CSE_LK,Abans Electricals PLC,Stock,,,Sri Lanka,LK,,",
                "BVC::CELSIA,CELSIA,BVC,Celsia: CELSIA,Stock,,,Colombia,CO,,",
            ]
        ),
        encoding="utf-8",
    )
    source = MasterfileSource(
        key="cse_lk_all_security_code",
        provider="CSE Sri Lanka",
        description="Official Colombo Stock Exchange all-security-code API",
        source_url="https://www.cse.lk/api/allSecurityCode",
        format="cse_lk_all_security_code_json",
        reference_scope="exchange_directory",
    )
    payload = [
        {"name": "ABANS ELECTRICALS PLC", "symbol": "ABAN.N0000", "active": 1},
        {"name": "INACTIVE PLC", "symbol": "OLD.N0000", "active": 0},
        {"name": "CELSIA", "symbol": "CELSIA", "active": 1},
    ]

    rows = parse_cse_lk_all_security_code_payload(payload, source, listings_path=listings_path)

    assert rows == [
        {
            "source_key": "cse_lk_all_security_code",
            "provider": "CSE Sri Lanka",
            "source_url": "https://www.cse.lk/api/allSecurityCode",
            "ticker": "ABAN",
            "name": "ABANS ELECTRICALS PLC",
            "exchange": "CSE_LK",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "",
        }
    ]


def test_parse_cse_lk_company_info_summary_payload_requires_valid_isin() -> None:
    source = MasterfileSource(
        key="cse_lk_company_info_summary",
        provider="CSE Sri Lanka",
        description="Official Colombo Stock Exchange company-info detail API",
        source_url="https://www.cse.lk/api/companyInfoSummery",
        format="cse_lk_company_info_summary_json",
        reference_scope="exchange_directory",
    )

    row = parse_cse_lk_company_info_summary_payload(
        {
            "reqSymbolInfo": {
                "symbol": "ABAN.N0000",
                "name": "ABANS ELECTRICALS PLC",
                "isin": "LK0001N00004",
            }
        },
        source,
    )

    assert row == {
        "source_key": "cse_lk_company_info_summary",
        "provider": "CSE Sri Lanka",
        "source_url": "https://www.cse.lk/api/companyInfoSummery",
        "ticker": "ABAN.N0000",
        "name": "ABANS ELECTRICALS PLC",
        "exchange": "CSE_LK",
        "asset_type": "Stock",
        "listing_status": "active",
        "reference_scope": "exchange_directory",
        "official": "true",
        "isin": "LK0001N00004",
    }
    assert parse_cse_lk_company_info_summary_payload(
        {"reqSymbolInfo": {"symbol": "ABNS.D0000", "name": "ABANS PLC", "isin": None}},
        source,
    ) is None


def test_parse_byma_equity_detail_payload_accepts_exact_local_equity() -> None:
    source = MasterfileSource(
        key="byma_equity_details",
        provider="BYMA",
        description="Official Open BYMADATA equity detail endpoint",
        source_url="https://open.bymadata.com.ar/vanoms-be-core/rest/api/bymadata/free/bnown/fichatecnica/especies/general",
        format="byma_equity_details_json",
        reference_scope="security_lookup_subset",
    )
    listing = {
        "ticker": "ALUA",
        "exchange": "BCBA",
        "name": "Aluar Aluminio Argentino",
        "asset_type": "Stock",
        "isin": "",
    }

    row = parse_byma_equity_detail_payload(
        {
            "data": [
                {
                    "codigoIsin": "ARALUA010258",
                    "tipoEspecie": "Acciones",
                    "insType": "EQUITY",
                    "emisor": "ALUAR ALUMINIO ARGENTINO S.A.",
                }
            ]
        },
        source,
        listing,
        remote_symbol="ALUA",
    )

    assert row == {
        "source_key": "byma_equity_details",
        "provider": "BYMA",
        "source_url": "https://open.bymadata.com.ar/vanoms-be-core/rest/api/bymadata/free/bnown/fichatecnica/especies/general",
        "ticker": "ALUA",
        "name": "ALUAR ALUMINIO ARGENTINO S.A.",
        "exchange": "BCBA",
        "asset_type": "Stock",
        "listing_status": "active",
        "reference_scope": "security_lookup_subset",
        "official": "true",
        "isin": "ARALUA010258",
    }


def test_parse_byma_equity_detail_payload_name_gates_remote_symbol_alias() -> None:
    source = MasterfileSource(
        key="byma_equity_details",
        provider="BYMA",
        description="Official Open BYMADATA equity detail endpoint",
        source_url="https://open.bymadata.com.ar/vanoms-be-core/rest/api/bymadata/free/bnown/fichatecnica/especies/general",
        format="byma_equity_details_json",
        reference_scope="security_lookup_subset",
    )
    listing = {
        "ticker": "BA-C",
        "exchange": "BCBA",
        "name": "BANK OF AMERICA CORPORATION CED",
        "asset_type": "Stock",
        "isin": "",
    }
    payload = {
        "data": [
            {
                "codigoIsin": "ARDEUT112851",
                "tipoEspecie": "Cedears",
                "denominacion": "BANK OF AMERICA CORPORATION",
                "emisor": "BANCO COMAFI S.A.",
            }
        ]
    }

    assert parse_byma_equity_detail_payload(payload, source, listing, remote_symbol="BA.C") == {
        "source_key": "byma_equity_details",
        "provider": "BYMA",
        "source_url": "https://open.bymadata.com.ar/vanoms-be-core/rest/api/bymadata/free/bnown/fichatecnica/especies/general",
        "ticker": "BA-C",
        "name": "BANK OF AMERICA CORPORATION",
        "exchange": "BCBA",
        "asset_type": "Stock",
        "listing_status": "active",
        "reference_scope": "security_lookup_subset",
        "official": "true",
        "isin": "ARDEUT112851",
    }
    assert parse_byma_equity_detail_payload(payload, source, {**listing, "name": "The Boeing Company"}, remote_symbol="BA.C") is None


def test_parse_byma_header_search_payload_confirms_exact_stock_symbol() -> None:
    source = MasterfileSource(
        key="byma_equity_details",
        provider="BYMA",
        description="Official Open BYMADATA equity detail endpoint",
        source_url="https://open.bymadata.com.ar/vanoms-be-core/rest/api/bymadata/free/bnown/fichatecnica/especies/general",
        format="byma_equity_details_json",
        reference_scope="security_lookup_subset",
    )
    listing = {
        "ticker": "BRIO",
        "exchange": "BCBA",
        "name": "Banco Santander Rio S.A",
        "asset_type": "Stock",
        "isin": "",
    }
    payload = [
        {"symbol": "BRIO", "market": "BYMA", "description": 'BCO. SANTANDER RIO - ORD. B 1 VOTO', "type": "Accion"},
        {"symbol": "BRIO1", "market": "BYMA", "description": "Bond", "type": "Bono"},
        {"symbol": "BRIO", "market": "SENEBI", "description": "Other market", "type": "Accion"},
    ]

    assert parse_byma_header_search_payload(payload, source, listing) == {
        "source_key": "byma_equity_details",
        "provider": "BYMA",
        "source_url": "https://open.bymadata.com.ar/vanoms-be-core/rest/api/bymadata/free/bnown/fichatecnica/especies/general",
        "ticker": "BRIO",
        "name": 'BCO. SANTANDER RIO - ORD. B 1 VOTO',
        "exchange": "BCBA",
        "asset_type": "Stock",
        "listing_status": "active",
        "reference_scope": "security_lookup_subset",
        "official": "true",
        "isin": "",
    }


def test_parse_cavali_bvl_emisores_html_pages_gates_to_current_bvl_equities(tmp_path) -> None:
    listings_path = tmp_path / "listings.csv"
    listings_path.write_text(
        "\n".join(
            [
                "listing_key,ticker,exchange,name,asset_type,stock_sector,etf_category,country,country_code,isin,aliases",
                "BVL::AENZAC1,AENZAC1,BVL,Aenza S.A.A.,Stock,Industrials,,Peru,PE,,",
                "BVL::ETFPERUD,ETFPERUD,BVL,Fondo Bursatil Van Eck El Dorado Peru ETF,ETF,,Peru ETF,Peru,PE,,",
                "BVL::LUSURC1,LUSURC1,BVL,Luz del Sur S.A.A.,Stock,Utilities,,Peru,PE,,",
            ]
        ),
        encoding="utf-8",
    )
    html = """
    <table>
        <tr><th>ISIN</th><th>EMPRESA</th><th>NEMONICO</th><th>GRUPO</th><th>TIPO</th><th>MONEDA</th><th>V. NOMINAL</th><th>TASA</th></tr>
        <tr><td>PEP736581005</td><td>AENZA S.A.A.</td><td>AENZAC1</td><td>ACCIONES</td><td>COMUNES</td><td>SOLES</td><td>1</td><td>-</td></tr>
        <tr><td>PEP798008004</td><td>FONDO BURSÁTIL VAN ECK EL DORADO PERÚ ETF</td><td>ETFPERUD</td><td>UNIDAD DE PARTICIPACION</td><td>UNIDAD DE PARTICIPACION ETFS</td><td>DÓLARES</td><td>-</td><td>-</td></tr>
        <tr><td>PEP70252M259</td><td>LUZ DEL SUR S.A.A.</td><td>LUSURC1</td><td>BONOS</td><td>CORPORATIVOS</td><td>SOLES</td><td>5000</td><td>6.6875</td></tr>
        <tr><td>PEP111111111</td><td>UNKNOWN S.A.A.</td><td>UNKNOWNC1</td><td>ACCIONES</td><td>COMUNES</td><td>SOLES</td><td>1</td><td>-</td></tr>
    </table>
    """
    source = MasterfileSource(
        key="bvl_issuers_directory",
        provider="CAVALI",
        description="Official Peruvian CSD issuer securities registry",
        source_url="https://cavali-corporativa.screativa.com/emisores",
        format="cavali_bvl_emisores_html",
        reference_scope="security_lookup_subset",
    )

    assert parse_cavali_bvl_emisores_html_pages([html], source, listings_path=listings_path) == [
        {
            "source_key": "bvl_issuers_directory",
            "provider": "CAVALI",
            "source_url": "https://cavali-corporativa.screativa.com/emisores",
            "ticker": "AENZAC1",
            "name": "AENZA S.A.A.",
            "exchange": "BVL",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "security_lookup_subset",
            "official": "true",
            "isin": "PEP736581005",
        },
        {
            "source_key": "bvl_issuers_directory",
            "provider": "CAVALI",
            "source_url": "https://cavali-corporativa.screativa.com/emisores",
            "ticker": "ETFPERUD",
            "name": "FONDO BURSÁTIL VAN ECK EL DORADO PERÚ ETF",
            "exchange": "BVL",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "security_lookup_subset",
            "official": "true",
            "isin": "PEP798008004",
        },
    ]


def test_parse_bvb_shares_directory_html_extracts_symbol_name_and_isin() -> None:
    source = MasterfileSource(
        key="bvb_shares_directory",
        provider="BVB",
        description="Official Bucharest Stock Exchange shares directory with ISINs",
        source_url="https://m.bvb.ro/FinancialInstruments/Markets/Shares",
        format="bvb_shares_directory_html",
    )
    text = """
    <table id="gv">
        <tr>
            <td>
                <a href="/FinancialInstruments/Details/FinancialInstrumentsDetails.aspx?s=TLV"><b>
                    TLV</b></a><p style="font-size: 11px">ROTLVAACNOR1</p>
            </td>
            <td align="left">BANCA TRANSILVANIA S.A.</td>
            <td align="right">36,7800</td>
            <td class="numericspvar" align="right">0,77</td>
            <td align="right">09.04.2026 17:55</td>
            <td align="center">Premium</td>
        </tr>
        <tr>
            <td>
                <a href="/FinancialInstruments/Details/FinancialInstrumentsDetails.aspx?s=PE"><b>
                    PE</b></a><p style="font-size: 11px">CY0200900914</p>
            </td>
            <td align="left">Premier Energy PLC</td>
            <td align="right">41,3000</td>
            <td class="numericspvar" align="right">-0,24</td>
            <td align="right">09.04.2026 17:54</td>
            <td align="center">Int'l</td>
        </tr>
    </table>
    """

    rows = parse_bvb_shares_directory_html(text, source)

    assert rows == [
        {
            "source_key": "bvb_shares_directory",
            "provider": "BVB",
            "source_url": "https://m.bvb.ro/FinancialInstruments/Details/FinancialInstrumentsDetails.aspx?s=TLV",
            "ticker": "TLV",
            "name": "BANCA TRANSILVANIA S.A.",
            "exchange": "BVB",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "ROTLVAACNOR1",
        },
        {
            "source_key": "bvb_shares_directory",
            "provider": "BVB",
            "source_url": "https://m.bvb.ro/FinancialInstruments/Details/FinancialInstrumentsDetails.aspx?s=PE",
            "ticker": "PE",
            "name": "Premier Energy PLC",
            "exchange": "BVB",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "CY0200900914",
        },
    ]


def test_parse_bvb_fund_units_directory_html_keeps_only_etf_rows() -> None:
    source = MasterfileSource(
        key="bvb_fund_units_directory",
        provider="BVB",
        description="Official Bucharest Stock Exchange fund units directory gated to ETF rows",
        source_url="https://m.bvb.ro/FinancialInstruments/Markets/FundUnits",
        format="bvb_fund_units_directory_html",
        reference_scope="listed_companies_subset",
    )
    text = """
    <table id="gv">
        <tr>
            <td>
                <a href="/FinancialInstruments/Details/FinancialInstrumentsDetails.aspx?s=PTENGETF"><b>
                    PTENGETF</b></a><p style="font-size: 11px">ROR9ULE5VKA1</p>
            </td>
            <td>FONDUL DESCHIS DE INVESTITII ETF ENERGIE PATRIA-TRADEVILLE</td>
            <td align="right">13,1200</td>
            <td class="numericspvar" align="right">0,00</td>
            <td align="right">09.04.2026 17:55</td>
            <td>ETF</td>
        </tr>
        <tr>
            <td>
                <a href="/FinancialInstruments/Details/FinancialInstrumentsDetails.aspx?s=BTF"><b>
                    BTF</b></a><p style="font-size: 11px">ROFIIN0000T6</p>
            </td>
            <td>F.I.A.I.R. BET FI INDEX INVEST</td>
            <td align="right">2360,0000</td>
            <td class="numericspvar" align="right">0,00</td>
            <td align="right">09.04.2026 17:55</td>
            <td>Units</td>
        </tr>
    </table>
    """

    rows = parse_bvb_fund_units_directory_html(text, source)

    assert rows == [
        {
            "source_key": "bvb_fund_units_directory",
            "provider": "BVB",
            "source_url": "https://m.bvb.ro/FinancialInstruments/Details/FinancialInstrumentsDetails.aspx?s=PTENGETF",
            "ticker": "PTENGETF",
            "name": "FONDUL DESCHIS DE INVESTITII ETF ENERGIE PATRIA-TRADEVILLE",
            "exchange": "BVB",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "ROR9ULE5VKA1",
        },
    ]


def test_parse_athex_classification_lines_maps_current_stock_rows() -> None:
    source = MasterfileSource(
        key="athex_sector_classification",
        provider="ATHEX",
        description="Official ATHEX companies sector classification PDF with ISINs",
        source_url="https://www.athexgroup.gr/documents/athex-classification.pdf",
        format="athex_sector_classification_pdf",
        reference_scope="listed_companies_subset",
    )
    listings_by_ticker = {
        "MERKO": {
            "ticker": "MERKO",
            "exchange": "ATHEX",
            "name": "MERMEREN KOMBINAT A.D. PRILEP",
            "asset_type": "Stock",
        },
        "AETF": {
            "ticker": "AETF",
            "exchange": "ATHEX",
            "name": "ALPHA ETF FTSE Athex Large Cap Equity UCITS",
            "asset_type": "ETF",
        },
    }
    lines = [
        (
            "GRK014011008 MERKO MERMEREN KOMBINAT A.D. PRILEP "
            "1700 Basic Resources 1755 Nonferrous Metals 5510 Basic Resources 55102015 Metal Fabricating"
        ),
        (
            "GRF000153004 AETF ALPHA ETF FTSE Athex Large Cap Equity UCITS "
            "8700 Financial Services 8771 Asset Managers 3020 Financial Services 30202010 Asset Managers"
        ),
    ]

    rows = parse_athex_classification_lines(lines, source, listings_by_ticker=listings_by_ticker)

    assert rows == [
        {
            "source_key": "athex_sector_classification",
            "provider": "ATHEX",
            "source_url": "https://www.athexgroup.gr/documents/athex-classification.pdf",
            "ticker": "MERKO",
            "name": "MERMEREN KOMBINAT A.D. PRILEP",
            "exchange": "ATHEX",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "GRK014011008",
            "sector": "Materials",
        },
    ]


def test_parse_ngx_equities_price_list_payload_matches_current_rows_by_ticker(tmp_path) -> None:
    listings_path = tmp_path / "listings.csv"
    listings_path.write_text(
        "\n".join(
            [
                "listing_key,ticker,exchange,name,asset_type,stock_sector,etf_category,country,country_code,isin,aliases",
                "NGX::ACCESSCORP,ACCESSCORP,NGX,ACCESS HOLDINGS PLC,Stock,,,,NG,NGACCESS0005,",
                "NGX::ABCTRANS,ABCTRANS,NGX,ABC TRANSPORT PLC,Stock,,,,NG,NGABCTRANS01,",
            ]
        ),
        encoding="utf-8",
    )
    source = MasterfileSource(
        key="ngx_equities_price_list",
        provider="NGX",
        description="Official Nigerian Exchange equities price list API with sector labels",
        source_url="https://doclib.ngxgroup.com/REST/api/statistics/equities/?market=&sector=&orderby=&pageSize=300&pageNo=0",
        format="ngx_equities_price_list_json",
        reference_scope="listed_companies_subset",
    )
    payload = [
        {"Symbol": "ACCESSCORP", "Sector": "FINANCIAL SERVICES", "Company2": "ACCESSCORP "},
        {"Symbol": "ABCTRANS", "Sector": "SERVICES", "Company2": "ABCTRANS "},
        {"Symbol": "BOND2028", "Sector": "BONDS", "Company2": "BOND2028"},
    ]

    rows = parse_ngx_equities_price_list_payload(payload, source, listings_path=listings_path)

    assert rows == [
        {
            "source_key": "ngx_equities_price_list",
            "provider": "NGX",
            "source_url": "https://doclib.ngxgroup.com/REST/api/statistics/equities/?market=&sector=&orderby=&pageSize=300&pageNo=0",
            "ticker": "ACCESSCORP",
            "name": "ACCESS HOLDINGS PLC",
            "exchange": "NGX",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "NGACCESS0005",
            "sector": "FINANCIAL SERVICES",
        },
        {
            "source_key": "ngx_equities_price_list",
            "provider": "NGX",
            "source_url": "https://doclib.ngxgroup.com/REST/api/statistics/equities/?market=&sector=&orderby=&pageSize=300&pageNo=0",
            "ticker": "ABCTRANS",
            "name": "ABC TRANSPORT PLC",
            "exchange": "NGX",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "NGABCTRANS01",
            "sector": "SERVICES",
        },
    ]


def test_parse_vienna_listed_companies_html_matches_current_vse_by_isin(tmp_path) -> None:
    listings_path = tmp_path / "listings.csv"
    listings_path.write_text(
        "\n".join(
            [
                "listing_key,ticker,exchange,name,asset_type,stock_sector,etf_category,country,country_code,isin,aliases",
                "VSE::AGR,AGR,VSE,AGRANA Beteiligungs-AG,Stock,Consumer Staples,,Austria,AT,AT000AGRANA3,",
                "XETRA::AGR,AGR,XETRA,AGRANA Beteiligungs-AG,Stock,Consumer Staples,,Austria,AT,AT000AGRANA3,",
                "VSE::NOISIN,NOISIN,VSE,No ISIN AG,Stock,,,,AT,,",
            ]
        ),
        encoding="utf-8",
    )
    source = MasterfileSource(
        key="vienna_listed_companies",
        provider="Wiener Boerse",
        description="Official Vienna Stock Exchange listed companies directory",
        source_url="https://www.wienerborse.at/en/listing/shares/companies-list/",
        format="vienna_listed_companies_html",
        reference_scope="listed_companies_subset",
    )
    text = """
    <table>
        <thead>
            <tr><th>ISIN</th><th>Issuer</th><th>Country</th><th>Market</th><th>Market Segment</th><th>Type of Security</th></tr>
        </thead>
        <tbody>
            <tr><td>AT000AGRANA3</td><td>AGRANA Beteiligungs-AG</td><td>Austria</td><td>Vienna</td><td>Prime Market</td><td>Equity Share</td></tr>
            <tr><td>AT0000BOND01</td><td>Bond Issuer</td><td>Austria</td><td>Vienna</td><td>Bond Market</td><td>Bond</td></tr>
            <tr><td>AT000UNMATCH0</td><td>Unmatched AG</td><td>Austria</td><td>Vienna</td><td>Prime Market</td><td>Equity Share</td></tr>
        </tbody>
    </table>
    """

    rows = parse_vienna_listed_companies_html(text, source, listings_path=listings_path)

    assert rows == [
        {
            "source_key": "vienna_listed_companies",
            "provider": "Wiener Boerse",
            "source_url": "https://www.wienerborse.at/en/listing/shares/companies-list/",
            "ticker": "AGR",
            "name": "AGRANA Beteiligungs-AG",
            "exchange": "VSE",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "AT000AGRANA3",
        }
    ]


def test_parse_zagreb_securities_html_maps_active_equity_rows() -> None:
    source = MasterfileSource(
        key="zagreb_securities_directory",
        provider="ZSE Croatia",
        description="Official Zagreb Stock Exchange listed share directory",
        source_url="https://zse.hr/en/shares/68",
        format="zagreb_securities_html",
        reference_scope="listed_companies_subset",
    )
    text = """
    <table>
        <tbody>
            <tr data-status="LISTED_SECURITIES" data-type="EQTY">
                <td>ADRS</td><td>HRADRSRA0007</td><td>ADRIS GRUPA d. d.</td><td>Consumer Staples</td>
                <td>Regular</td><td>1</td><td>2003-01-01</td><td>-</td>
            </tr>
            <tr data-status="DELISTED_SECURITIES" data-type="EQTY">
                <td>OLD</td><td>HROLD0RA0001</td><td>Old Issuer d.d.</td><td>Industrials</td>
                <td>Regular</td><td>1</td><td>2003-01-01</td><td>2020-01-01</td>
            </tr>
            <tr data-status="LISTED_SECURITIES" data-type="BOND">
                <td>BOND</td><td>HRBONDRA0001</td><td>Bond Issuer d.d.</td><td>Financials</td>
                <td>Regular</td><td>1</td><td>2003-01-01</td><td>-</td>
            </tr>
        </tbody>
    </table>
    """

    rows = parse_zagreb_securities_html(text, source)

    assert rows == [
        {
            "source_key": "zagreb_securities_directory",
            "provider": "ZSE Croatia",
            "source_url": "https://zse.hr/en/shares/68",
            "ticker": "ADRS",
            "name": "ADRIS GRUPA d. d.",
            "exchange": "ZSE",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "HRADRSRA0007",
            "sector": "Consumer Staples",
        }
    ]


def test_parse_idx_listed_companies_payload_maps_rows() -> None:
    source = MasterfileSource(
        key="idx_listed_companies",
        provider="IDX",
        description="Official IDX stock list directory",
        source_url="https://www.idx.id/primary/StockData/GetSecuritiesStock",
        format="idx_listed_companies_json",
        reference_scope="listed_companies_subset",
    )
    payload = {
        "recordsTotal": 3,
        "recordsFiltered": 3,
        "data": [
            {"Code": "AALI", "Name": "Astra Agro Lestari Tbk."},
            {"Code": "BBCA", "Name": "Bank Central Asia Tbk."},
            {"Code": "AALI", "Name": "Astra Agro Lestari Tbk."},
            {"Code": "", "Name": "Missing Code"},
        ],
    }

    rows = parse_idx_listed_companies_payload(payload, source)

    assert rows == [
        {
            "source_key": "idx_listed_companies",
            "provider": "IDX",
            "source_url": "https://www.idx.id/primary/StockData/GetSecuritiesStock",
            "ticker": "AALI",
            "name": "Astra Agro Lestari Tbk.",
            "exchange": "IDX",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
        },
        {
            "source_key": "idx_listed_companies",
            "provider": "IDX",
            "source_url": "https://www.idx.id/primary/StockData/GetSecuritiesStock",
            "ticker": "BBCA",
            "name": "Bank Central Asia Tbk.",
            "exchange": "IDX",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
        },
    ]


def test_parse_idx_company_profiles_payload_maps_idx_sectors() -> None:
    source = MasterfileSource(
        key="idx_company_profiles",
        provider="IDX",
        description="Official IDX company profiles metadata API",
        source_url="https://www.idx.id/primary/ListedCompany/GetCompanyProfiles",
        format="idx_company_profiles_json",
        reference_scope="exchange_directory",
    )
    payload = {
        "recordsTotal": 4,
        "recordsFiltered": 4,
        "data": [
            {
                "KodeEmiten": "AALI",
                "NamaEmiten": "Astra Agro Lestari Tbk",
                "Sektor": "Barang Konsumen Primer",
                "SubSektor": "Makanan & Minuman",
            },
            {
                "KodeEmiten": "TLKM",
                "NamaEmiten": "Telkom Indonesia (Persero) Tbk",
                "Sektor": "Infrastruktur",
                "SubSektor": "Telekomunikasi",
            },
            {
                "KodeEmiten": "JSMR",
                "NamaEmiten": "Jasa Marga (Persero) Tbk",
                "Sektor": "Infrastruktur",
                "SubSektor": "Infrastruktur Transportasi",
            },
            {
                "KodeEmiten": "AALI",
                "NamaEmiten": "Astra Agro Lestari Tbk",
                "Sektor": "Barang Konsumen Primer",
                "SubSektor": "Makanan & Minuman",
            },
        ],
    }

    rows = parse_idx_company_profiles_payload(payload, source)

    assert [(row["ticker"], row["sector"]) for row in rows] == [
        ("AALI", "Consumer Staples"),
        ("TLKM", "Communication Services"),
        ("JSMR", "Industrials"),
    ]
    assert all(row["official"] == "true" for row in rows)


def test_load_idx_company_profiles_rows_falls_back_to_cache(tmp_path, monkeypatch) -> None:
    cache_path = tmp_path / "idx_company_profiles.json"
    legacy_path = tmp_path / "legacy_idx_company_profiles.json"
    cache_path.write_text(
        json.dumps(
            [
                {
                    "source_key": "idx_company_profiles",
                    "provider": "IDX",
                    "source_url": "https://www.idx.id/primary/ListedCompany/GetCompanyProfiles",
                    "ticker": "AALI",
                    "name": "Astra Agro Lestari Tbk",
                    "exchange": "IDX",
                    "asset_type": "Stock",
                    "listing_status": "active",
                    "reference_scope": "exchange_directory",
                    "official": "true",
                    "sector": "Consumer Staples",
                }
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(fetch_exchange_masterfiles, "IDX_COMPANY_PROFILES_CACHE", cache_path)
    monkeypatch.setattr(fetch_exchange_masterfiles, "LEGACY_IDX_COMPANY_PROFILES_CACHE", legacy_path)
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "fetch_idx_company_profiles",
        lambda source, session=None: (_ for _ in ()).throw(requests.RequestException("network unavailable")),
    )
    source = next(item for item in OFFICIAL_SOURCES if item.key == "idx_company_profiles")

    rows, mode = load_idx_company_profiles_rows(source)

    assert mode == "cache"
    assert [(row["ticker"], row["sector"]) for row in rows] == [("AALI", "Consumer Staples")]


def test_fetch_idx_listed_companies_paginates_official_api() -> None:
    source = MasterfileSource(
        key="idx_listed_companies",
        provider="IDX",
        description="Official IDX stock list directory",
        source_url="https://www.idx.id/primary/StockData/GetSecuritiesStock",
        format="idx_listed_companies_json",
        reference_scope="listed_companies_subset",
    )

    class DummyResponse:
        def __init__(self, payload: dict[str, object]):
            self._payload = payload

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return self._payload

    class DummySession:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        def get(self, url: str, *, params=None, headers=None, timeout=None):
            self.calls.append({"url": url, "params": params, "headers": headers, "timeout": timeout})
            if params["start"] == 0:
                return DummyResponse(
                    {
                        "recordsTotal": 1001,
                        "recordsFiltered": 1001,
                        "data": [{"Code": "AALI", "Name": "Astra Agro Lestari Tbk."}],
                    }
                )
            return DummyResponse(
                {
                    "recordsTotal": 1001,
                    "recordsFiltered": 1001,
                    "data": [{"Code": "BBCA", "Name": "Bank Central Asia Tbk."}],
                }
            )

    session = DummySession()
    rows = fetch_idx_listed_companies(source, session=session)

    assert [row["ticker"] for row in rows] == ["AALI", "BBCA"]
    assert [call["params"]["start"] for call in session.calls] == [0, 1000]
    assert all(call["params"]["language"] == "en-us" for call in session.calls)


def test_load_idx_listed_companies_rows_prefers_cache(tmp_path, monkeypatch) -> None:
    cache_path = tmp_path / "idx_listed_companies.json"
    cache_path.write_text(
        '[{"ticker":"AALI","name":"Astra Agro Lestari Tbk.","exchange":"IDX","asset_type":"Stock","listing_status":"active"}]',
        encoding="utf-8",
    )
    monkeypatch.setattr(fetch_exchange_masterfiles, "IDX_LISTED_COMPANIES_CACHE", cache_path)
    monkeypatch.setattr(fetch_exchange_masterfiles, "LEGACY_IDX_LISTED_COMPANIES_CACHE", tmp_path / "missing.json")
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "fetch_idx_listed_companies",
        lambda source, session=None: (_ for _ in ()).throw(requests.RequestException("boom")),
    )

    source = next(item for item in OFFICIAL_SOURCES if item.key == "idx_listed_companies")
    rows, mode = load_idx_listed_companies_rows(source)

    assert mode == "cache"
    assert rows == [
        {
            "ticker": "AALI",
            "name": "Astra Agro Lestari Tbk.",
            "exchange": "IDX",
            "asset_type": "Stock",
            "listing_status": "active",
        }
    ]


def test_fetch_source_rows_with_mode_uses_idx_cache(tmp_path, monkeypatch) -> None:
    cache_path = tmp_path / "idx_listed_companies.json"
    cache_path.write_text(
        '[{"ticker":"AALI","name":"Astra Agro Lestari Tbk.","exchange":"IDX","asset_type":"Stock","listing_status":"active","source_key":"idx_listed_companies","reference_scope":"listed_companies_subset","official":"true","provider":"IDX","source_url":"https://example.com"}]',
        encoding="utf-8",
    )
    monkeypatch.setattr(fetch_exchange_masterfiles, "IDX_LISTED_COMPANIES_CACHE", cache_path)
    monkeypatch.setattr(fetch_exchange_masterfiles, "LEGACY_IDX_LISTED_COMPANIES_CACHE", tmp_path / "missing.json")
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "fetch_idx_listed_companies",
        lambda source, session=None: (_ for _ in ()).throw(requests.RequestException("boom")),
    )

    source = next(item for item in OFFICIAL_SOURCES if item.key == "idx_listed_companies")
    rows, mode = fetch_source_rows_with_mode(source)

    assert mode == "cache"
    assert rows[0]["ticker"] == "AALI"
    assert rows[0]["exchange"] == "IDX"


def test_wse_sources_are_modeled_as_partial_official_coverage() -> None:
    stock_source = next(item for item in OFFICIAL_SOURCES if item.key == "wse_listed_companies")
    newconnect_source = next(item for item in OFFICIAL_SOURCES if item.key == "newconnect_listed_companies")
    etf_source = next(item for item in OFFICIAL_SOURCES if item.key == "wse_etf_list")

    assert stock_source.reference_scope == "listed_companies_subset"
    assert newconnect_source.reference_scope == "listed_companies_subset"
    assert etf_source.reference_scope == "listed_companies_subset"


def test_extract_html_form_inputs_keeps_checked_boxes_and_empty_text() -> None:
    text = """
    <form action="/spolki" id='search-form'>
        <input type="hidden" name="action" value="GPWCompanySearch"/>
        <input type="hidden" name="limit" value="10"/>
        <input type="text" name="searchText"/>
        <input type="checkbox" name="index[WIG20]" checked />
        <input type="checkbox" name="index[mWIG40]" />
    </form>
    """

    fields = extract_html_form_inputs(text, "search-form")

    assert fields == {
        "action": "GPWCompanySearch",
        "limit": "10",
        "searchText": "",
        "index[WIG20]": "on",
    }


def test_parse_wse_company_search_html_maps_rows() -> None:
    source = MasterfileSource(
        key="wse_listed_companies",
        provider="GPW",
        description="Official GPW listed companies directory",
        source_url="https://www.gpw.pl/spolki",
        format="wse_listed_companies_html",
        reference_scope="listed_companies_subset",
    )
    text = """
    <tbody id="search-result">
        <tr>
            <td>
                <a href="spolka?isin=PL11BTS00015">
                    <strong class="name">11 BIT STUDIOS SPÓŁKA AKCYJNA <span class="grey">(11B)</span></strong>
                </a>
                <small class="grey">Główny Rynek | WIG-gry, WIG | Gry</small>
            </td>
        </tr>
        <tr>
            <td>
                <a href="spolka?isin=LU2237380790">
                    <strong class="name">Allegro.eu S.A. <span class="grey">(ALE)</span></strong>
                </a>
                <small class="grey">Główny Rynek | Handel Internetowy</small>
            </td>
        </tr>
    </tbody>
    """

    rows = parse_wse_company_search_html(text, source)

    assert rows == [
        {
            "source_key": "wse_listed_companies",
            "provider": "GPW",
            "source_url": "https://www.gpw.pl/spolka?isin=PL11BTS00015",
            "ticker": "11B",
            "name": "11 BIT STUDIOS SPÓŁKA AKCYJNA",
            "exchange": "WSE",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "PL11BTS00015",
            "sector": "Communication Services",
        },
        {
            "source_key": "wse_listed_companies",
            "provider": "GPW",
            "source_url": "https://www.gpw.pl/spolka?isin=LU2237380790",
            "ticker": "ALE",
            "name": "Allegro.eu S.A.",
            "exchange": "WSE",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "LU2237380790",
            "sector": "Consumer Discretionary",
        },
    ]


def test_normalize_wse_stock_sector_maps_polish_exchange_labels() -> None:
    assert normalize_wse_stock_sector("oprogramowanie") == "Information Technology"
    assert normalize_wse_stock_sector("Gry") == "Communication Services"
    assert normalize_wse_stock_sector("banki komercyjne") == "Financials"
    assert normalize_wse_stock_sector("sprzedaż nieruchomości") == "Real Estate"
    assert normalize_wse_stock_sector("Sprzęt i Materiały Medyczne") == "Health Care"
    assert normalize_wse_stock_sector("produkcja rolna i rybołówstwo") == "Consumer Staples"
    assert normalize_wse_stock_sector("Usługi dla przedsiębiorstw") == "Industrials"


def test_fetch_wse_listed_companies_uses_initial_page_and_ajax_pagination() -> None:
    source = MasterfileSource(
        key="wse_listed_companies",
        provider="GPW",
        description="Official GPW listed companies directory",
        source_url="https://www.gpw.pl/spolki",
        format="wse_listed_companies_html",
        reference_scope="listed_companies_subset",
    )
    page_html = """
    <h1>Lista spółek <small><span id="count-all">22</span> Spółek</small></h1>
    <form action="/spolki" id='search-form' data-target="search-result">
        <input type="hidden" name="action" value="GPWCompanySearch"/>
        <input type="hidden" name="start" value="ajaxSearch"/>
        <input type="hidden" name="page" value="spolki"/>
        <input type="hidden" name="format" value="html"/>
        <input type="hidden" name="lang" value="PL"/>
        <input type="hidden" name="letter" value=""/>
        <input type="hidden" name="offset" value="0"/>
        <input type="hidden" name="limit" value="10"/>
        <input type="text" name="searchText"/>
        <input type="checkbox" name="index[WIG20]" checked />
    </form>
    <table id="lista-spolek"><tbody id="search-result">
        <tr><td><a href="spolka?isin=PL11BTS00015"><strong class="name">11 BIT STUDIOS SPÓŁKA AKCYJNA <span class="grey">(11B)</span></strong></a></td></tr>
        <tr><td><a href="spolka?isin=PLGRNKT00019"><strong class="name">3R GAMES SPÓŁKA AKCYJNA <span class="grey">(3RG)</span></strong></a></td></tr>
        <tr><td><a href="spolka?isin=PL4MASS00011"><strong class="name">4MASS SPÓŁKA AKCYJNA <span class="grey">(4MS)</span></strong></a></td></tr>
        <tr><td><a href="spolka?isin=PLABAK000013"><strong class="name">ABAK SPÓŁKA AKCYJNA <span class="grey">(ABK)</span></strong></a></td></tr>
        <tr><td><a href="spolka?isin=AU0000187577"><strong class="name">AB S.A. <span class="grey">(ABE)</span></strong></a></td></tr>
        <tr><td><a href="spolka?isin=PLALIOR00045"><strong class="name">ALIOR BANK SPÓŁKA AKCYJNA <span class="grey">(ALR)</span></strong></a></td></tr>
        <tr><td><a href="spolka?isin=PLAMBRA00013"><strong class="name">AMBRA SPÓŁKA AKCYJNA <span class="grey">(AMB)</span></strong></a></td></tr>
        <tr><td><a href="spolka?isin=PLARHCM00016"><strong class="name">ARCHICOM SPÓŁKA AKCYJNA <span class="grey">(ARH)</span></strong></a></td></tr>
        <tr><td><a href="spolka?isin=PLASSEE00014"><strong class="name">ASSECO SOUTH EASTERN EUROPE SPÓŁKA AKCYJNA <span class="grey">(ASE)</span></strong></a></td></tr>
        <tr><td><a href="spolka?isin=PLASTRK00013"><strong class="name">ASTRO SPÓŁKA AKCYJNA <span class="grey">(AST)</span></strong></a></td></tr>
        <tr><td><a href="spolka?isin=PLATAL000046"><strong class="name">ATAL SPÓŁKA AKCYJNA <span class="grey">(1AT)</span></strong></a></td></tr>
        <tr><td><a href="spolka?isin=PLATC000011"><strong class="name">ATENDE SPÓŁKA AKCYJNA <span class="grey">(ATD)</span></strong></a></td></tr>
        <tr><td><a href="spolka?isin=PLATMSO00013"><strong class="name">ATM SYSTEMY INFORMATYCZNE SPÓŁKA AKCYJNA <span class="grey">(ATO)</span></strong></a></td></tr>
        <tr><td><a href="spolka?isin=PLATPRC00011"><strong class="name">AUTO PARTNER SPÓŁKA AKCYJNA <span class="grey">(APR)</span></strong></a></td></tr>
        <tr><td><a href="spolka?isin=PLATRMG00017"><strong class="name">ATREM SPÓŁKA AKCYJNA <span class="grey">(ATR)</span></strong></a></td></tr>
        <tr><td><a href="spolka?isin=PLAWBUD00017"><strong class="name">AWBUD SPÓŁKA AKCYJNA <span class="grey">(AWB)</span></strong></a></td></tr>
        <tr><td><a href="spolka?isin=PLAPSEN00011"><strong class="name">APS ENERGIA SPÓŁKA AKCYJNA <span class="grey">(APE)</span></strong></a></td></tr>
        <tr><td><a href="spolka?isin=PLANSWR00019"><strong class="name">ANSWEAR.COM SPÓŁKA AKCYJNA <span class="grey">(ANR)</span></strong></a></td></tr>
        <tr><td><a href="spolka?isin=PLAQUA000014"><strong class="name">AQUABB SPÓŁKA AKCYJNA <span class="grey">(AQU)</span></strong></a></td></tr>
        <tr><td><a href="spolka?isin=PLAOLPR00012"><strong class="name">AOL SPÓŁKA AKCYJNA <span class="grey">(AOL)</span></strong></a></td></tr>
    </tbody></table>
    """
    page_two_html = """
    <tr><td><a href="spolka?isin=LU2237380790"><strong class="name">Allegro.eu S.A. <span class="grey">(ALE)</span></strong></a></td></tr>
    """

    class DummyResponse:
        def __init__(self, text: str):
            self.text = text

        def raise_for_status(self) -> None:
            return None

    class DummySession:
        def __init__(self) -> None:
            self.get_calls: list[dict[str, object]] = []
            self.post_calls: list[dict[str, object]] = []

        def get(self, url: str, *, headers=None, timeout=None):
            self.get_calls.append({"url": url, "headers": headers, "timeout": timeout})
            return DummyResponse(page_html)

        def post(self, url: str, *, data=None, headers=None, timeout=None):
            self.post_calls.append({"url": url, "data": data, "headers": headers, "timeout": timeout})
            return DummyResponse(page_two_html)

    session = DummySession()
    rows = fetch_wse_listed_companies(source, session=session)

    assert rows[0]["ticker"] == "11B"
    assert rows[-1]["ticker"] == "ALE"
    assert len(rows) == 21
    assert session.post_calls == [
        {
            "url": "https://www.gpw.pl/ajaxindex.php",
            "data": {
                "action": "GPWCompanySearch",
                "start": "ajaxSearch",
                "page": "spolki",
                "format": "html",
                "lang": "PL",
                "letter": "",
                "offset": "20",
                "limit": "500",
                "searchText": "",
                "index[WIG20]": "on",
            },
            "headers": fetch_exchange_masterfiles.wse_request_headers("https://www.gpw.pl/spolki"),
            "timeout": fetch_exchange_masterfiles.REQUEST_TIMEOUT,
        }
    ]


def test_fetch_newconnect_listed_companies_uses_initial_page_and_ajax_pagination() -> None:
    source = MasterfileSource(
        key="newconnect_listed_companies",
        provider="NewConnect",
        description="Official NewConnect listed companies directory",
        source_url="https://newconnect.pl/spolki",
        format="newconnect_listed_companies_html",
        reference_scope="listed_companies_subset",
    )
    page_html = """
    <form action="/spolki" id='search-form' data-target="search-result">
        <input type="hidden" name="format" value="html"/>
        <input type="hidden" name="lang" value="PL"/>
        <input type="hidden" name="offset" value="0"/>
        <input type="hidden" name="limit" value="10"/>
        <input type="hidden" name="letter" value=""/>
        <input type="hidden" name="action" value="NCCompany"/>
        <input type="hidden" name="start" value="listAjax"/>
        <input type="hidden" name="order" value="ncc_name"/>
        <input type="hidden" name="order_type" value="ASC"/>
        <input type="text" name="searchText"/>
        <input type="checkbox" name="countries[1000]" checked />
        <input type="checkbox" name="countries[1002]" checked />
    </form>
    <table><tbody id="search-result">
        <tr><td><a href="spolka?isin=PLESLTN00010"><strong class="name">4MOBILITY SPÓŁKA AKCYJNA <span class="grey">(4MB)</span></strong></a></td></tr>
        <tr><td><a href="spolka?isin=PLTRCPS00016"><strong class="name">7FIT SPÓŁKA AKCYJNA <span class="grey">(7FT)</span></strong></a></td></tr>
        <tr><td><a href="spolka?isin=PL7LVLS00017"><strong class="name">7LEVELS SPÓŁKA AKCYJNA <span class="grey">(7LV)</span></strong></a></td></tr>
        <tr><td><a href="spolka?isin=PLABAK000013"><strong class="name">ABAK SPÓŁKA AKCYJNA <span class="grey">(ABK)</span></strong></a></td></tr>
        <tr><td><a href="spolka?isin=PLABSIN00012"><strong class="name">ABS INVESTMENT ALTERNATYWNA SPÓŁKA INWESTYCYJNA SPÓŁKA AKCYJNA <span class="grey">(AIN)</span></strong></a></td></tr>
        <tr><td><a href="spolka?isin=PLABMSA00019"><strong class="name">ABSOLUTE GAMES SPÓŁKA AKCYJNA <span class="grey">(AGS)</span></strong></a></td></tr>
        <tr><td><a href="spolka?isin=PLABPLD00015"><strong class="name">ABPL SPÓŁKA AKCYJNA <span class="grey">(ABP)</span></strong></a></td></tr>
        <tr><td><a href="spolka?isin=PLABRVS00012"><strong class="name">ABRYS SPÓŁKA AKCYJNA <span class="grey">(ABR)</span></strong></a></td></tr>
        <tr><td><a href="spolka?isin=PLACAUT00014"><strong class="name">ACAUTOGAZ SPÓŁKA AKCYJNA <span class="grey">(ACG)</span></strong></a></td></tr>
        <tr><td><a href="spolka?isin=PLACPPL00018"><strong class="name">ACP SPÓŁKA AKCYJNA <span class="grey">(ACP)</span></strong></a></td></tr>
    </tbody></table>
    """
    page_two_html = """
    <tr><td><a href="spolka?isin=PLMNDPL00012"><strong class="name">MIND DEVELOPMENT SPÓŁKA AKCYJNA <span class="grey">(MND)</span></strong></a></td></tr>
    """

    class DummyResponse:
        def __init__(self, text: str):
            self.text = text

        def raise_for_status(self) -> None:
            return None

    class DummySession:
        def __init__(self) -> None:
            self.get_calls: list[dict[str, object]] = []
            self.post_calls: list[dict[str, object]] = []

        def get(self, url: str, *, headers=None, timeout=None):
            self.get_calls.append({"url": url, "headers": headers, "timeout": timeout})
            return DummyResponse(page_html)

        def post(self, url: str, *, data=None, headers=None, timeout=None):
            self.post_calls.append({"url": url, "data": data, "headers": headers, "timeout": timeout})
            return DummyResponse(page_two_html)

    session = DummySession()
    rows = fetch_newconnect_listed_companies(source, session=session)

    assert rows[0]["ticker"] == "4MB"
    assert rows[-1]["ticker"] == "MND"
    assert len(rows) == 11
    assert session.post_calls == [
        {
            "url": "https://newconnect.pl/ajaxindex.php",
            "data": {
                "format": "html",
                "lang": "PL",
                "offset": "10",
                "limit": "500",
                "letter": "",
                "action": "NCCompany",
                "start": "listAjax",
                "order": "ncc_name",
                "order_type": "ASC",
                "searchText": "",
                "countries[1000]": "on",
                "countries[1002]": "on",
            },
            "headers": fetch_exchange_masterfiles.wse_request_headers("https://newconnect.pl/spolki"),
            "timeout": fetch_exchange_masterfiles.REQUEST_TIMEOUT,
        }
    ]


def test_parse_wse_etf_list_html_maps_rows_with_listing_name_fallback() -> None:
    source = MasterfileSource(
        key="wse_etf_list",
        provider="GPW",
        description="Official GPW ETF/ETC/ETN directory",
        source_url="https://www.gpw.pl/etfy",
        format="wse_etf_list_html",
        reference_scope="listed_companies_subset",
    )
    text = """
    <tbody>
        <tr>
            <td id="id_Nazwa" class="left nowrap col1"><a href="etf?isin=PLBETWT00010"><b>ETFBCASH </b></a></td>
            <td id="id_ISIN" class="left nowrap col2" style="display:none">PLBETWT00010</td>
        </tr>
        <tr>
            <td id="id_Nazwa" class="left nowrap col1"><a href="etf?isin=PLPZUTR00013"><b>ETFPZUW20M </b></a></td>
            <td id="id_ISIN" class="left nowrap col2" style="display:none">PLPZUTR00013</td>
        </tr>
    </tbody>
    """

    rows = parse_wse_etf_list_html(
        text,
        source,
        listing_name_by_ticker={
            "ETFBCASH": "Beta ETF Obligacji 6M",
            "ETFPZUW20M": "PZU ETF WIG20 TR & MWIG40 - Investment Certificates ETF",
        },
    )

    assert rows == [
        {
            "source_key": "wse_etf_list",
            "provider": "GPW",
            "source_url": "https://www.gpw.pl/etf?isin=PLBETWT00010",
            "ticker": "ETFBCASH",
            "name": "Beta ETF Obligacji 6M",
            "exchange": "WSE",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "PLBETWT00010",
        },
        {
            "source_key": "wse_etf_list",
            "provider": "GPW",
            "source_url": "https://www.gpw.pl/etf?isin=PLPZUTR00013",
            "ticker": "ETFPZUW20M",
            "name": "PZU ETF WIG20 TR & MWIG40 - Investment Certificates ETF",
            "exchange": "WSE",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "PLPZUTR00013",
        },
    ]


def test_fetch_wse_etf_list_uses_official_ajax(tmp_path) -> None:
    source = MasterfileSource(
        key="wse_etf_list",
        provider="GPW",
        description="Official GPW ETF/ETC/ETN directory",
        source_url="https://www.gpw.pl/etfy",
        format="wse_etf_list_html",
        reference_scope="listed_companies_subset",
    )
    listings_path = tmp_path / "listings.csv"
    listings_path.write_text(
        "\n".join(
            [
                "ticker,exchange,asset_type,name,isin",
                "ETFBCASH,WSE,ETF,Beta ETF Obligacji 6M,",
                "ETFPZUW20M,WSE,ETF,PZU ETF WIG20 TR & MWIG40 - Investment Certificates ETF,",
            ]
        ),
        encoding="utf-8",
    )
    payload = """
    <tbody>
        <tr><td id="id_Nazwa" class="left nowrap col1"><a href="etf?isin=PLBETWT00010"><b>ETFBCASH </b></a></td></tr>
        <tr><td id="id_Nazwa" class="left nowrap col1"><a href="etf?isin=PLPZUTR00013"><b>ETFPZUW20M </b></a></td></tr>
    </tbody>
    """

    class DummyResponse:
        def __init__(self, text: str):
            self.text = text

        def raise_for_status(self) -> None:
            return None

    class DummySession:
        def __init__(self) -> None:
            self.post_calls: list[dict[str, object]] = []

        def post(self, url: str, *, data=None, headers=None, timeout=None):
            self.post_calls.append({"url": url, "data": data, "headers": headers, "timeout": timeout})
            return DummyResponse(payload)

    session = DummySession()
    rows = fetch_wse_etf_list(source, listings_path=listings_path, session=session)

    assert [row["name"] for row in rows] == [
        "Beta ETF Obligacji 6M",
        "PZU ETF WIG20 TR & MWIG40 - Investment Certificates ETF",
    ]
    assert session.post_calls == [
        {
            "url": "https://www.gpw.pl/ajaxindex.php",
            "data": {"action": "GPWQuotationsETF", "start": "ajaxList", "page": "etfy"},
            "headers": fetch_exchange_masterfiles.wse_request_headers("https://www.gpw.pl/etfy"),
            "timeout": fetch_exchange_masterfiles.REQUEST_TIMEOUT,
        }
    ]


def test_load_wse_reference_rows_prefers_cache(tmp_path, monkeypatch) -> None:
    cache_path = tmp_path / "wse_listed_companies.json"
    cache_path.write_text(
        '[{"ticker":"11B","name":"11 bit studios S.A.","exchange":"WSE","asset_type":"Stock","listing_status":"active"}]',
        encoding="utf-8",
    )
    monkeypatch.setattr(fetch_exchange_masterfiles, "WSE_LISTED_COMPANIES_CACHE", cache_path)
    monkeypatch.setattr(fetch_exchange_masterfiles, "LEGACY_WSE_LISTED_COMPANIES_CACHE", tmp_path / "missing.json")
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "fetch_wse_listed_companies",
        lambda source, session=None: (_ for _ in ()).throw(requests.RequestException("boom")),
    )

    source = next(item for item in OFFICIAL_SOURCES if item.key == "wse_listed_companies")
    rows, mode = load_wse_reference_rows(source)

    assert mode == "cache"
    assert rows == [
        {
            "ticker": "11B",
            "name": "11 bit studios S.A.",
            "exchange": "WSE",
            "asset_type": "Stock",
            "listing_status": "active",
        }
    ]


def test_load_newconnect_reference_rows_prefers_cache(tmp_path, monkeypatch) -> None:
    cache_path = tmp_path / "newconnect_listed_companies.json"
    cache_path.write_text(
        '[{"ticker":"4MB","name":"4Mobility S.A.","exchange":"WSE","asset_type":"Stock","listing_status":"active"}]',
        encoding="utf-8",
    )
    monkeypatch.setattr(fetch_exchange_masterfiles, "NEWCONNECT_LISTED_COMPANIES_CACHE", cache_path)
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "LEGACY_NEWCONNECT_LISTED_COMPANIES_CACHE",
        tmp_path / "missing.json",
    )
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "fetch_newconnect_listed_companies",
        lambda source, session=None: (_ for _ in ()).throw(requests.RequestException("boom")),
    )

    source = next(item for item in OFFICIAL_SOURCES if item.key == "newconnect_listed_companies")
    rows, mode = load_wse_reference_rows(source)

    assert mode == "cache"
    assert rows == [
        {
            "ticker": "4MB",
            "name": "4Mobility S.A.",
            "exchange": "WSE",
            "asset_type": "Stock",
            "listing_status": "active",
        }
    ]


def test_parse_tase_securities_marketdata_payload_filters_shares() -> None:
    source = MasterfileSource(
        key="tase_securities_marketdata",
        provider="TASE",
        description="Official TASE market securities directory (shares subset)",
        source_url="https://api.tase.co.il/api/security/securitiesmarketdata",
        format="tase_securities_marketdata_json",
        reference_scope="listed_companies_subset",
    )
    payload = {
        "Items": [
            {"Symbol": "ABRA", "Name": "Abra", "Type": "Shares", "ISIN_ID": "IL0012345678"},
            {"Symbol": "ABOU", "Name": "Aboitiz", "Type": "Shares", "ISIN_ID": ""},
            {"Symbol": "ABRA", "Name": "Abra Duplicate", "Type": "Shares", "ISIN_ID": "IL0012345678"},
            {"Symbol": "GOVB", "Name": "Gov Bond", "Type": "Bonds", "ISIN_ID": "IL0099999999"},
            {"Symbol": "", "Name": "Missing Ticker", "Type": "Shares", "ISIN_ID": "IL0011111111"},
        ]
    }

    rows = parse_tase_securities_marketdata_payload(payload, source)

    assert rows == [
        {
            "source_key": "tase_securities_marketdata",
            "provider": "TASE",
            "source_url": "https://api.tase.co.il/api/security/securitiesmarketdata",
            "ticker": "ABRA",
            "name": "Abra",
            "exchange": "TASE",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "IL0012345678",
        },
        {
            "source_key": "tase_securities_marketdata",
            "provider": "TASE",
            "source_url": "https://api.tase.co.il/api/security/securitiesmarketdata",
            "ticker": "ABOU",
            "name": "Aboitiz",
            "exchange": "TASE",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
        },
    ]


def test_fetch_tase_securities_marketdata_paginates_bootstrapped_api(monkeypatch) -> None:
    source = MasterfileSource(
        key="tase_securities_marketdata",
        provider="TASE",
        description="Official TASE market securities directory (shares subset)",
        source_url="https://api.tase.co.il/api/security/securitiesmarketdata",
        format="tase_securities_marketdata_json",
        reference_scope="listed_companies_subset",
    )

    class DummyResponse:
        def __init__(self, payload: dict[str, object]):
            self._payload = payload

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return self._payload

    class DummySession:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        def post(self, url: str, *, headers=None, data=None, timeout=None):
            payload = json.loads(data)
            self.calls.append({"url": url, "headers": headers, "payload": payload, "timeout": timeout})
            if payload["pageNum"] == 1:
                return DummyResponse(
                    {
                        "TotalRec": 31,
                        "Items": [
                            {"Symbol": "ABRA", "Name": "Abra", "Type": "Shares", "ISIN_ID": "IL0012345678"},
                            {"Symbol": "BOND1", "Name": "Ignored Bond", "Type": "Bonds", "ISIN_ID": "IL0099999999"},
                        ],
                    }
                )
            return DummyResponse(
                {
                    "TotalRec": 31,
                    "Items": [
                        {"Symbol": "ABOU", "Name": "Aboitiz", "Type": "Shares", "ISIN_ID": ""},
                    ],
                }
            )

    session = DummySession()
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "bootstrap_tase_market_session",
        lambda session=None: (session or DummySession(), {"Accept": "application/json"}),
    )

    rows = fetch_tase_securities_marketdata(source, session=session)

    assert [row["ticker"] for row in rows] == ["ABRA", "ABOU"]
    assert [call["payload"]["pageNum"] for call in session.calls] == [1, 2]
    assert all(call["payload"]["cl1"] == "0" for call in session.calls)


def test_load_tase_securities_marketdata_rows_prefers_cache(tmp_path, monkeypatch) -> None:
    cache_path = tmp_path / "tase_securities_marketdata.json"
    cache_path.write_text(
        '[{"ticker":"ABRA","name":"Abra","exchange":"TASE","asset_type":"Stock","listing_status":"active"}]',
        encoding="utf-8",
    )
    monkeypatch.setattr(fetch_exchange_masterfiles, "TASE_SECURITIES_MARKETDATA_CACHE", cache_path)
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "LEGACY_TASE_SECURITIES_MARKETDATA_CACHE",
        tmp_path / "missing.json",
    )
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "fetch_tase_securities_marketdata",
        lambda source, session=None: (_ for _ in ()).throw(requests.RequestException("boom")),
    )

    source = next(item for item in OFFICIAL_SOURCES if item.key == "tase_securities_marketdata")
    rows, mode = load_tase_securities_marketdata_rows(source)

    assert mode == "cache"
    assert rows == [
        {
            "ticker": "ABRA",
            "name": "Abra",
            "exchange": "TASE",
            "asset_type": "Stock",
            "listing_status": "active",
        }
    ]


def test_parse_tase_etf_marketdata_payload_normalizes_symbols() -> None:
    source = MasterfileSource(
        key="tase_etf_marketdata",
        provider="TASE",
        description="Official TASE ETF market directory",
        source_url="https://api.tase.co.il/api/marketdata/etfs",
        format="tase_etf_marketdata_json",
        reference_scope="listed_companies_subset",
    )
    payload = {
        "Items": [
            {
                "Symbol": "HRL.F303",
                "LongName": "Harel Sal Tel Bond",
                "ISIN": "IL0011507477",
                "Classification": "Israeli bonds - Corporate and Convertibles",
            },
            {
                "Symbol": "ANLT.F2",
                "SecurityLongName": "ATF S&P 500",
                "ISIN_ID": "IL0012189242",
                "Classification": "Foreign Shares - FX-Hedged Geographical Stocks",
            },
            {
                "Symbol": "KSM.F208",
                "SecurityLongName": "KSM Short TA-RealEstate",
                "ISIN_ID": "IL0011866345",
                "Classification": "Leveraged - Short Leveraged, High-Risk - Stocks in Israel",
            },
            {"Symbol": "HRL.F303", "LongName": "Duplicate", "ISIN": "IL0011507477"},
            {"Symbol": "", "LongName": "Missing", "ISIN": "IL0011111111"},
        ]
    }

    rows = parse_tase_etf_marketdata_payload(payload, source)

    assert rows == [
        {
            "source_key": "tase_etf_marketdata",
            "provider": "TASE",
            "source_url": "https://api.tase.co.il/api/marketdata/etfs",
            "ticker": "HRL-F303",
            "name": "Harel Sal Tel Bond",
            "exchange": "TASE",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "IL0011507477",
            "sector": "Fixed Income",
        },
        {
            "source_key": "tase_etf_marketdata",
            "provider": "TASE",
            "source_url": "https://api.tase.co.il/api/marketdata/etfs",
            "ticker": "ANLT-F2",
            "name": "ATF S&P 500",
            "exchange": "TASE",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "IL0012189242",
            "sector": "Equity",
        },
        {
            "source_key": "tase_etf_marketdata",
            "provider": "TASE",
            "source_url": "https://api.tase.co.il/api/marketdata/etfs",
            "ticker": "KSM-F208",
            "name": "KSM Short TA-RealEstate",
            "exchange": "TASE",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "IL0011866345",
            "sector": "Leveraged/Inverse",
        },
    ]


def test_fetch_tase_etf_marketdata_paginates_bootstrapped_api(monkeypatch) -> None:
    source = MasterfileSource(
        key="tase_etf_marketdata",
        provider="TASE",
        description="Official TASE ETF market directory",
        source_url="https://api.tase.co.il/api/marketdata/etfs",
        format="tase_etf_marketdata_json",
        reference_scope="listed_companies_subset",
    )

    class DummyResponse:
        def __init__(self, payload: dict[str, object]):
            self._payload = payload

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return self._payload

    class DummySession:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        def post(self, url: str, *, headers=None, data=None, timeout=None):
            payload = json.loads(data)
            self.calls.append({"url": url, "headers": headers, "payload": payload, "timeout": timeout})
            if payload["pageNum"] == 1:
                return DummyResponse(
                    {
                        "TotalRec": 31,
                        "Items": [
                            {"Symbol": "HRL.F303", "LongName": "Harel Sal Tel Bond", "ISIN": "IL0011507477"},
                        ],
                    }
                )
            return DummyResponse(
                {
                    "TotalRec": 31,
                    "Items": [
                        {"Symbol": "ANLT.F2", "LongName": "ATF S&P 500", "ISIN": "IL0012189242"},
                    ],
                }
            )

    session = DummySession()
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "bootstrap_tase_market_session",
        lambda session=None: (session or DummySession(), {"Accept": "application/json"}),
    )

    rows = fetch_tase_etf_marketdata(source, session=session)

    assert [row["ticker"] for row in rows] == ["HRL-F303", "ANLT-F2"]
    assert [call["payload"]["pageNum"] for call in session.calls] == [1, 2]
    assert all(call["payload"]["lang"] == "1" for call in session.calls)


def test_load_tase_etf_marketdata_rows_prefers_cache(tmp_path, monkeypatch) -> None:
    cache_path = tmp_path / "tase_etf_marketdata.json"
    cache_path.write_text(
        '[{"ticker":"HRL-F303","name":"Harel Sal Tel Bond","exchange":"TASE","asset_type":"ETF","listing_status":"active"}]',
        encoding="utf-8",
    )
    monkeypatch.setattr(fetch_exchange_masterfiles, "TASE_ETF_MARKETDATA_CACHE", cache_path)
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "LEGACY_TASE_ETF_MARKETDATA_CACHE",
        tmp_path / "missing.json",
    )
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "fetch_tase_etf_marketdata",
        lambda source, session=None: (_ for _ in ()).throw(requests.RequestException("boom")),
    )

    source = next(item for item in OFFICIAL_SOURCES if item.key == "tase_etf_marketdata")
    rows, mode = load_tase_etf_marketdata_rows(source)

    assert mode == "cache"
    assert rows == [
        {
            "ticker": "HRL-F303",
            "name": "Harel Sal Tel Bond",
            "exchange": "TASE",
            "asset_type": "ETF",
            "listing_status": "active",
        }
    ]


def test_fetch_tase_foreign_etf_search_matches_normalized_symbols(monkeypatch, tmp_path) -> None:
    source = next(item for item in OFFICIAL_SOURCES if item.key == "tase_foreign_etf_search")
    listings_path = tmp_path / "listings.csv"
    listings_path.write_text(
        "\n".join(
            [
                "ticker,exchange,asset_type,name,isin",
                "IN-FF1,TASE,ETF,Invesco S&P 500,IE00B3YCGJ38",
                "ISFF505,TASE,ETF,iShares MSCI ACWI,IE00B6R52259",
                "PSG-F106,TASE,ETF,Psagot AC World,",
            ]
        ),
        encoding="utf-8",
    )
    verification_dir = tmp_path / "verification"
    verification_dir.mkdir()
    run_dir = verification_dir / "run-01"
    run_dir.mkdir()
    (run_dir / "chunk-01-of-01.csv").write_text(
        "\n".join(
            [
                "ticker,exchange,status",
                "IN-FF1,TASE,reference_gap",
                "ISFF505,TASE,reference_gap",
                "PSG-F106,TASE,reference_gap",
            ]
        ),
        encoding="utf-8",
    )

    class DummyResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> list[dict[str, str]]:
            return [
                {
                    "Smb": "IN.FF1",
                    "Name": "INV.FRFS&P 500",
                    "ISIN": "IE00B3YCGJ38",
                    "SubTypeDesc": "Foreign ETF - Equity",
                },
                {
                    "Smb": "IS.FF505",
                    "Name": "ISH.FRF MSCIACW",
                    "ISIN": "IE00B6R52259",
                    "SubTypeDesc": "Foreign ETF - Equity",
                },
                {
                    "Smb": "PSG.F106",
                    "Name": "PSAGOT ETF ACW",
                    "ISIN": "IL0000000001",
                    "SubTypeDesc": "Deleted Fund",
                },
            ]

    class DummySession:
        def get(self, url: str, headers=None, timeout=None) -> DummyResponse:
            assert url == fetch_exchange_masterfiles.TASE_SEARCH_ENTITIES_URL
            return DummyResponse()

    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "bootstrap_tase_market_session",
        lambda session=None: (DummySession(), {"User-Agent": "Mozilla/5.0"}),
    )

    rows = fetch_tase_foreign_etf_search(
        source,
        listings_path=listings_path,
        verification_dir=verification_dir,
    )

    assert rows == [
        {
            "source_key": "tase_foreign_etf_search",
            "provider": "TASE",
            "source_url": fetch_exchange_masterfiles.TASE_SEARCH_ENTITIES_URL,
            "ticker": "IN-FF1",
            "name": "INV.FRFS&P 500",
            "exchange": "TASE",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "IE00B3YCGJ38",
        },
        {
            "source_key": "tase_foreign_etf_search",
            "provider": "TASE",
            "source_url": fetch_exchange_masterfiles.TASE_SEARCH_ENTITIES_URL,
            "ticker": "ISFF505",
            "name": "ISH.FRF MSCIACW",
            "exchange": "TASE",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "IE00B6R52259",
        },
    ]


def test_load_tase_foreign_etf_search_rows_prefers_cache(tmp_path, monkeypatch) -> None:
    cache_path = tmp_path / "tase_foreign_etf_search.json"
    cache_path.write_text(
        '[{"ticker":"IN-FF1","name":"INV.FRFS&P 500","exchange":"TASE","asset_type":"ETF","listing_status":"active"}]',
        encoding="utf-8",
    )
    monkeypatch.setattr(fetch_exchange_masterfiles, "TASE_FOREIGN_ETF_SEARCH_CACHE", cache_path)
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "LEGACY_TASE_FOREIGN_ETF_SEARCH_CACHE",
        tmp_path / "legacy_tase_foreign_etf_search.json",
    )
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "fetch_tase_foreign_etf_search",
        lambda source, session=None: (_ for _ in ()).throw(requests.RequestException("boom")),
    )

    source = next(item for item in OFFICIAL_SOURCES if item.key == "tase_foreign_etf_search")
    rows, mode = load_tase_foreign_etf_search_rows(source)

    assert mode == "cache"
    assert rows == [
        {
            "ticker": "IN-FF1",
            "name": "INV.FRFS&P 500",
            "exchange": "TASE",
            "asset_type": "ETF",
            "listing_status": "active",
        }
    ]


def test_fetch_tase_participating_unit_search_matches_exact_symbols(monkeypatch, tmp_path) -> None:
    source = next(item for item in OFFICIAL_SOURCES if item.key == "tase_participating_unit_search")
    listings_path = tmp_path / "listings.csv"
    listings_path.write_text(
        "\n".join(
            [
                "ticker,exchange,asset_type,name,isin",
                "AMDA,TASE,Stock,Almeda Ventures Limited Partnership,",
                "NVPT,TASE,Stock,Navitas Petroleum Limited Partnership,",
                "RATI-L,TASE,Stock,Ratio Energies LP,",
            ]
        ),
        encoding="utf-8",
    )
    verification_dir = tmp_path / "verification"
    verification_dir.mkdir()
    run_dir = verification_dir / "run-01"
    run_dir.mkdir()
    (run_dir / "chunk-01-of-01.csv").write_text(
        "\n".join(
            [
                "ticker,exchange,status",
                "AMDA,TASE,reference_gap",
                "NVPT,TASE,reference_gap",
                "RATI-L,TASE,reference_gap",
            ]
        ),
        encoding="utf-8",
    )

    class DummyResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> list[dict[str, str]]:
            return [
                {
                    "Smb": "AMDA",
                    "Name": "ALMEDA PU",
                    "ISIN": "IL0011689622",
                    "SubTypeDesc": "Participating unit",
                },
                {
                    "Smb": "NVPT",
                    "Name": "NAVITAS PTRO PU",
                    "ISIN": "IL0011419699",
                    "SubTypeDesc": "Participating unit",
                },
                {
                    "Smb": "RATI",
                    "Name": "RATIO PU",
                    "ISIN": "IL0003940157",
                    "SubTypeDesc": "Participating unit",
                },
            ]

    class DummySession:
        def get(self, url: str, headers=None, timeout=None) -> DummyResponse:
            assert url == fetch_exchange_masterfiles.TASE_SEARCH_ENTITIES_URL
            return DummyResponse()

    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "bootstrap_tase_market_session",
        lambda session=None: (DummySession(), {"User-Agent": "Mozilla/5.0"}),
    )

    rows = fetch_tase_participating_unit_search(
        source,
        listings_path=listings_path,
        verification_dir=verification_dir,
    )

    assert rows == [
        {
            "source_key": "tase_participating_unit_search",
            "provider": "TASE",
            "source_url": fetch_exchange_masterfiles.TASE_SEARCH_ENTITIES_URL,
            "ticker": "AMDA",
            "name": "ALMEDA PU",
            "exchange": "TASE",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "IL0011689622",
        },
        {
            "source_key": "tase_participating_unit_search",
            "provider": "TASE",
            "source_url": fetch_exchange_masterfiles.TASE_SEARCH_ENTITIES_URL,
            "ticker": "NVPT",
            "name": "NAVITAS PTRO PU",
            "exchange": "TASE",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "IL0011419699",
        },
    ]


def test_load_tase_participating_unit_search_rows_prefers_cache(tmp_path, monkeypatch) -> None:
    cache_path = tmp_path / "tase_participating_unit_search.json"
    cache_path.write_text(
        '[{"ticker":"AMDA","name":"ALMEDA PU","exchange":"TASE","asset_type":"Stock","listing_status":"active"}]',
        encoding="utf-8",
    )
    monkeypatch.setattr(fetch_exchange_masterfiles, "TASE_PARTICIPATING_UNIT_SEARCH_CACHE", cache_path)
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "LEGACY_TASE_PARTICIPATING_UNIT_SEARCH_CACHE",
        tmp_path / "legacy_tase_participating_unit_search.json",
    )
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "fetch_tase_participating_unit_search",
        lambda source, session=None: (_ for _ in ()).throw(requests.RequestException("boom")),
    )

    source = next(item for item in OFFICIAL_SOURCES if item.key == "tase_participating_unit_search")
    rows, mode = load_tase_participating_unit_search_rows(source)

    assert mode == "cache"
    assert rows == [
        {
            "ticker": "AMDA",
            "name": "ALMEDA PU",
            "exchange": "TASE",
            "asset_type": "Stock",
            "listing_status": "active",
        }
    ]


def test_parse_hose_securities_payload_requires_isin() -> None:
    source = MasterfileSource(
        key="hose_listed_stocks",
        provider="HOSE",
        description="Official Ho Chi Minh Stock Exchange listed stocks directory",
        source_url="https://api.hsx.vn/l/api/v1/2/securities/stock",
        format="hose_listed_stocks_json",
        reference_scope="listed_companies_subset",
    )
    payload = {
        "data": {
            "list": [
                {"code": "AAA", "name": "An Phat Bioplastics Joint Stock Company", "isin": "VN000000AAA4"},
                {"code": "AAA", "name": "Duplicate", "isin": "VN000000AAA4"},
                {"code": "NOISIN", "name": "Missing ISIN", "isin": ""},
                {"code": "", "name": "Missing Ticker", "isin": "VN0000000001"},
            ]
        }
    }

    rows = parse_hose_securities_payload(payload, source, asset_type="Stock")

    assert rows == [
        {
            "source_key": "hose_listed_stocks",
            "provider": "HOSE",
            "source_url": "https://api.hsx.vn/l/api/v1/2/securities/stock",
            "ticker": "AAA",
            "name": "An Phat Bioplastics Joint Stock Company",
            "exchange": "HOSE",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "VN000000AAA4",
        }
    ]


def test_parse_hose_securities_payload_maps_conservative_categories() -> None:
    source = MasterfileSource(
        key="hose_etf_list",
        provider="HOSE",
        description="Official Ho Chi Minh Stock Exchange ETF directory",
        source_url="https://api.hsx.vn/l/api/v1/2/securities/etf",
        format="hose_etf_list_json",
        reference_scope="listed_companies_subset",
    )
    payload = {
        "data": {
            "list": [
                {
                    "code": "FUESSVFL",
                    "name": "SSIAM VNFIN LEAD ETF",
                    "isin": "VN0FUESSVFL3",
                    "refIndex": "VNFIN LEAD",
                }
            ]
        }
    }

    rows = parse_hose_securities_payload(payload, source, asset_type="ETF")

    assert rows[0]["sector"] == "Equity"


def test_parse_hose_securities_payload_maps_reit_fund_certificate() -> None:
    source = MasterfileSource(
        key="hose_fund_certificate_list",
        provider="HOSE",
        description="Official Ho Chi Minh Stock Exchange fund certificate directory",
        source_url="https://api.hsx.vn/l/api/v1/2/securities/fundcertificate",
        format="hose_fund_certificate_list_json",
        reference_scope="listed_companies_subset",
    )
    payload = {
        "data": {
            "list": [
                {
                    "code": "FUCVREIT",
                    "name": "Techcom Vietnam REIT Fund",
                    "isin": "VN0FUCVREIT6",
                    "displayText": "FUCVREIT | Quỹ Đầu tư Bất động sản Techcom Việt Nam",
                }
            ]
        }
    }

    rows = parse_hose_securities_payload(payload, source, asset_type="Stock")

    assert rows[0]["sector"] == "Real Estate"


def test_fetch_hose_securities_rows_paginates_official_api() -> None:
    source = MasterfileSource(
        key="hose_listed_stocks",
        provider="HOSE",
        description="Official Ho Chi Minh Stock Exchange listed stocks directory",
        source_url="https://api.hsx.vn/l/api/v1/2/securities/stock",
        format="hose_listed_stocks_json",
        reference_scope="listed_companies_subset",
    )

    class DummyResponse:
        def __init__(self, payload: dict[str, object]):
            self._payload = payload

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return self._payload

    class DummySession:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        def get(self, url: str, *, params=None, headers=None, timeout=None) -> DummyResponse:
            self.calls.append({"url": url, "params": params, "headers": headers, "timeout": timeout})
            page_index = params["pageIndex"]
            if page_index == 1:
                return DummyResponse(
                    {
                        "data": {
                            "list": [
                                {
                                    "code": "AAA",
                                    "name": "An Phat Bioplastics Joint Stock Company",
                                    "isin": "VN000000AAA4",
                                }
                            ],
                            "paging": {"pageIndex": 1, "pageSize": 1000, "totalCount": 2, "totalPages": 2},
                        }
                    }
                )
            return DummyResponse(
                {
                    "data": {
                        "list": [
                            {
                                "code": "VNM",
                                "name": "Viet Nam Dairy Products Joint Stock Company",
                                "isin": "VN000000VNM8",
                            }
                        ],
                        "paging": {"pageIndex": 2, "pageSize": 1000, "totalCount": 2, "totalPages": 2},
                    }
                }
            )

    session = DummySession()
    rows = fetch_hose_securities_rows(source, session=session)

    assert [row["ticker"] for row in rows] == ["AAA", "VNM"]
    assert rows[1]["isin"] == "VN000000VNM8"
    assert [call["params"]["pageIndex"] for call in session.calls] == [1, 2]
    assert all(call["headers"]["Origin"] == "https://www.hsx.vn" for call in session.calls)


def test_load_hose_securities_rows_prefers_cache(tmp_path, monkeypatch) -> None:
    cache_path = tmp_path / "hose_listed_stocks.json"
    cache_path.write_text(
        '[{"ticker":"AAA","name":"An Phat Bioplastics Joint Stock Company","exchange":"HOSE","asset_type":"Stock","listing_status":"active","isin":"VN000000AAA4"}]',
        encoding="utf-8",
    )
    monkeypatch.setattr(fetch_exchange_masterfiles, "HOSE_LISTED_STOCKS_CACHE", cache_path)
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "LEGACY_HOSE_LISTED_STOCKS_CACHE",
        tmp_path / "missing.json",
    )
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "fetch_hose_securities_rows",
        lambda source, session=None: (_ for _ in ()).throw(requests.RequestException("boom")),
    )

    source = next(item for item in OFFICIAL_SOURCES if item.key == "hose_listed_stocks")
    rows, mode = load_hose_securities_rows(source)

    assert mode == "cache"
    assert rows == [
        {
            "ticker": "AAA",
            "name": "An Phat Bioplastics Joint Stock Company",
            "exchange": "HOSE",
            "asset_type": "Stock",
            "listing_status": "active",
            "isin": "VN000000AAA4",
        }
    ]


def test_load_hose_fund_certificate_rows_prefers_cache(tmp_path, monkeypatch) -> None:
    cache_path = tmp_path / "hose_fund_certificate_list.json"
    cache_path.write_text(
        '[{"ticker":"FUCVREIT","name":"Techcom Vietnam REIT Fund","exchange":"HOSE","asset_type":"Stock","listing_status":"active","isin":"VN0FUCVREIT6"}]',
        encoding="utf-8",
    )
    monkeypatch.setattr(fetch_exchange_masterfiles, "HOSE_FUND_CERTIFICATE_LIST_CACHE", cache_path)
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "LEGACY_HOSE_FUND_CERTIFICATE_LIST_CACHE",
        tmp_path / "missing.json",
    )
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "fetch_hose_securities_rows",
        lambda source, session=None: (_ for _ in ()).throw(requests.RequestException("boom")),
    )

    source = next(item for item in OFFICIAL_SOURCES if item.key == "hose_fund_certificate_list")
    rows, mode = load_hose_securities_rows(source)

    assert mode == "cache"
    assert rows == [
        {
            "ticker": "FUCVREIT",
            "name": "Techcom Vietnam REIT Fund",
            "exchange": "HOSE",
            "asset_type": "Stock",
            "listing_status": "active",
            "isin": "VN0FUCVREIT6",
        }
    ]


def test_parse_hnx_issuer_table_html_uses_vsdc_isin_lookup() -> None:
    source = MasterfileSource(
        key="hnx_listed_securities",
        provider="HNX",
        description="Official Hanoi Stock Exchange listed securities directory",
        source_url="https://www.hnx.vn/cophieu-etfs/chung-khoan-ny.html",
        format="hnx_listed_securities_json",
        reference_scope="exchange_directory",
    )
    content = """
    <table>
      <tr><th>STT</th><th>M&#227; CK</th><th>T&#234;n t&#7893; ch&#7913;c ph&#225;t h&#224;nh</th></tr>
      <tr><td>1</td><td>DVM</td><td>Vietnam Medicinal Materials Joint Stock Company</td></tr>
      <tr><td>2</td><td>DVM</td><td>Duplicate</td></tr>
      <tr><td>3</td><td></td><td>Missing ticker</td></tr>
    </table>
    """

    rows = parse_hnx_issuer_table_html(
        content,
        source,
        exchange="HNX",
        isin_lookup={"DVM": "VN000000DVM9"},
    )

    assert rows == [
        {
            "source_key": "hnx_listed_securities",
            "provider": "HNX",
            "source_url": "https://www.hnx.vn/cophieu-etfs/chung-khoan-ny.html",
            "ticker": "DVM",
            "name": "Vietnam Medicinal Materials Joint Stock Company",
            "exchange": "HNX",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "VN000000DVM9",
        }
    ]


def test_fetch_hnx_issuer_rows_posts_official_search_params(monkeypatch) -> None:
    source = MasterfileSource(
        key="upcom_registered_securities",
        provider="HNX",
        description="Official Hanoi Stock Exchange UPCoM registered securities directory",
        source_url="https://www.hnx.vn/cophieu-etfs/chung-khoan-uc.html",
        format="upcom_registered_securities_json",
        reference_scope="exchange_directory",
    )
    content = """
    <table>
      <tr><th>STT</th><th>M&#227; CK</th><th>T&#234;n t&#7893; ch&#7913;c ph&#225;t h&#224;nh</th></tr>
      <tr><td>1</td><td>VNI</td><td>Viet Nam Land Investment Corporation</td></tr>
    </table>
    """

    class DummyResponse:
        def __init__(self, *, payload: dict[str, object] | None = None, url: str = ""):
            self._payload = payload or {}
            self.url = url

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return self._payload

    class DummySession:
        def __init__(self) -> None:
            self.get_calls: list[dict[str, object]] = []
            self.post_calls: list[dict[str, object]] = []

        def get(self, url: str, **kwargs) -> DummyResponse:
            self.get_calls.append({"url": url, **kwargs})
            return DummyResponse(url="https://www.hnx.vn/vi-vn/cophieu-etfs/chung-khoan-uc.html")

        def post(self, url: str, **kwargs) -> DummyResponse:
            self.post_calls.append({"url": url, **kwargs})
            return DummyResponse(payload={"Content": content})

    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "fetch_vsdc_isin_lookup",
        lambda tickers, session=None: {"VNI": "VN000000VNI6"},
    )

    session = DummySession()
    rows = fetch_hnx_issuer_rows(source, session=session)

    assert rows[0]["exchange"] == "UPCOM"
    assert rows[0]["isin"] == "VN000000VNI6"
    assert session.get_calls[0]["verify"] is False
    assert session.post_calls[0]["verify"] is False
    assert session.post_calls[0]["data"]["p_keysearch"] == ""
    assert session.post_calls[0]["data"]["p_market_code"] == "UC"
    assert session.post_calls[0]["headers"]["Referer"] == "https://www.hnx.vn/vi-vn/cophieu-etfs/chung-khoan-uc.html"


def test_load_hnx_issuer_rows_prefers_cache(tmp_path, monkeypatch) -> None:
    cache_path = tmp_path / "hnx_listed_securities.json"
    cache_path.write_text(
        '[{"ticker":"DVM","name":"Vietnam Medicinal Materials Joint Stock Company","exchange":"HNX","asset_type":"Stock","listing_status":"active","isin":"VN000000DVM9"}]',
        encoding="utf-8",
    )
    monkeypatch.setattr(fetch_exchange_masterfiles, "HNX_LISTED_SECURITIES_CACHE", cache_path)
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "LEGACY_HNX_LISTED_SECURITIES_CACHE",
        tmp_path / "missing.json",
    )
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "fetch_hnx_issuer_rows",
        lambda source, session=None: (_ for _ in ()).throw(requests.RequestException("boom")),
    )

    source = next(item for item in OFFICIAL_SOURCES if item.key == "hnx_listed_securities")
    rows, mode = load_hnx_issuer_rows(source)

    assert mode == "cache"
    assert rows == [
        {
            "ticker": "DVM",
            "name": "Vietnam Medicinal Materials Joint Stock Company",
            "exchange": "HNX",
            "asset_type": "Stock",
            "listing_status": "active",
            "isin": "VN000000DVM9",
        }
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


def test_load_nasdaq_nordic_stockholm_share_search_rows_prefers_cache(tmp_path, monkeypatch) -> None:
    cache_path = tmp_path / "nasdaq_nordic_stockholm_shares_search.json"
    cache_path.write_text(
        '[{"ticker":"ERIC-B","name":"Ericsson B","exchange":"STO","asset_type":"Stock","listing_status":"active"}]',
        encoding="utf-8",
    )
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "NASDAQ_NORDIC_STOCKHOLM_SHARES_SEARCH_CACHE",
        cache_path,
    )
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "LEGACY_NASDAQ_NORDIC_STOCKHOLM_SHARES_SEARCH_CACHE",
        tmp_path / "missing.json",
    )

    source = next(item for item in OFFICIAL_SOURCES if item.key == "nasdaq_nordic_stockholm_shares_search")
    rows, mode = load_nasdaq_nordic_share_search_rows(source)

    assert mode == "cache"
    assert rows == [
        {
            "ticker": "ERIC-B",
            "name": "Ericsson B",
            "exchange": "STO",
            "asset_type": "Stock",
            "listing_status": "active",
        }
    ]


def test_load_nasdaq_nordic_copenhagen_share_search_rows_prefers_cache(tmp_path, monkeypatch) -> None:
    cache_path = tmp_path / "nasdaq_nordic_copenhagen_shares_search.json"
    cache_path.write_text(
        '[{"ticker":"AMBU-B","name":"Ambu","exchange":"CPH","asset_type":"Stock","listing_status":"active"}]',
        encoding="utf-8",
    )
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "NASDAQ_NORDIC_COPENHAGEN_SHARES_SEARCH_CACHE",
        cache_path,
    )
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "LEGACY_NASDAQ_NORDIC_COPENHAGEN_SHARES_SEARCH_CACHE",
        tmp_path / "missing.json",
    )

    source = next(item for item in OFFICIAL_SOURCES if item.key == "nasdaq_nordic_copenhagen_shares_search")
    rows, mode = load_nasdaq_nordic_share_search_rows(source)

    assert mode == "cache"
    assert rows == [
        {
            "ticker": "AMBU-B",
            "name": "Ambu",
            "exchange": "CPH",
            "asset_type": "Stock",
            "listing_status": "active",
        }
    ]


def test_fetch_nasdaq_nordic_copenhagen_etf_search_maps_exact_fund_symbol(monkeypatch, tmp_path) -> None:
    source = MasterfileSource(
        key="nasdaq_nordic_copenhagen_etf_search",
        provider="Nasdaq Nordic",
        description="Official Nasdaq Nordic Copenhagen ETF search supplement",
        source_url="https://api.nasdaq.com/api/nordic/search",
        format="nasdaq_nordic_copenhagen_etf_search_json",
        reference_scope="listed_companies_subset",
    )
    listings_path = tmp_path / "listings.csv"
    listings_path.write_text(
        "\n".join(
            [
                "ticker,exchange,asset_type,name,isin",
                "MAJMEL,CPH,ETF,Maj Invest UCITS ETF Metal&El.,",
            ]
        ),
        encoding="utf-8",
    )
    verification_dir = tmp_path / "verify"
    verification_dir.mkdir()
    run_dir = verification_dir / "run-01"
    run_dir.mkdir()
    (run_dir / "chunk-01-of-01.csv").write_text(
        "\n".join(
            [
                "ticker,exchange,status",
                "MAJMEL,CPH,reference_gap",
            ]
        ),
        encoding="utf-8",
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
            return FakeResponse(
                {
                    "data": [
                        {
                            "group": "Funds",
                            "instruments": [
                                {
                                    "symbol": "MAJMEL",
                                    "fullName": "Maj Invest UCITS ETF Metaller & Elektrif",
                                    "currency": "DKK",
                                    "assetClass": "FUNDS",
                                    "isin": "DK0061681913",
                                }
                            ],
                        }
                    ]
                }
            )

    rows = fetch_exchange_masterfiles.fetch_nasdaq_nordic_etf_search(
        source,
        listings_path=listings_path,
        verification_dir=verification_dir,
        session=FakeSession(),
    )

    assert rows == [
        {
            "source_key": "nasdaq_nordic_copenhagen_etf_search",
            "provider": "Nasdaq Nordic",
            "source_url": "https://api.nasdaq.com/api/nordic/search",
            "ticker": "MAJMEL",
            "name": "Maj Invest UCITS ETF Metaller & Elektrif",
            "exchange": "CPH",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "DK0061681913",
        }
    ]


def test_load_nasdaq_nordic_copenhagen_etf_search_rows_prefers_cache(tmp_path, monkeypatch) -> None:
    cache_path = tmp_path / "nasdaq_nordic_copenhagen_etf_search.json"
    cache_path.write_text(
        '[{"ticker":"MAJMEL","name":"Maj Invest UCITS ETF Metaller & Elektrif","exchange":"CPH","asset_type":"ETF","listing_status":"active"}]',
        encoding="utf-8",
    )
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "NASDAQ_NORDIC_COPENHAGEN_ETF_SEARCH_CACHE",
        cache_path,
    )
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "LEGACY_NASDAQ_NORDIC_COPENHAGEN_ETF_SEARCH_CACHE",
        tmp_path / "missing.json",
    )

    source = next(item for item in OFFICIAL_SOURCES if item.key == "nasdaq_nordic_copenhagen_etf_search")
    rows, mode = fetch_exchange_masterfiles.load_nasdaq_nordic_etf_search_rows(source)

    assert mode == "cache"
    assert rows == [
        {
            "ticker": "MAJMEL",
            "name": "Maj Invest UCITS ETF Metaller & Elektrif",
            "exchange": "CPH",
            "asset_type": "ETF",
            "listing_status": "active",
        }
    ]


def test_load_spotlight_search_rows_prefers_cache(tmp_path, monkeypatch) -> None:
    cache_path = tmp_path / "spotlight_companies_search.json"
    cache_path.write_text(
        '[{"ticker":"ABAS","name":"ABAS Protect","exchange":"STO","asset_type":"Stock","listing_status":"active"}]',
        encoding="utf-8",
    )
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "SPOTLIGHT_COMPANIES_SEARCH_CACHE",
        cache_path,
    )
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "LEGACY_SPOTLIGHT_COMPANIES_SEARCH_CACHE",
        tmp_path / "missing.json",
    )

    source = next(item for item in OFFICIAL_SOURCES if item.key == "spotlight_companies_search")
    rows, mode = load_spotlight_search_rows(source)

    assert mode == "cache"
    assert rows == [
        {
            "ticker": "ABAS",
            "name": "ABAS Protect",
            "exchange": "STO",
            "asset_type": "Stock",
            "listing_status": "active",
        }
    ]


def test_load_spotlight_directory_rows_prefers_cache(tmp_path, monkeypatch) -> None:
    cache_path = tmp_path / "spotlight_companies_directory.json"
    cache_path.write_text(
        '[{"ticker":"HOME-B","name":"HomeMaid","exchange":"STO","asset_type":"Stock","listing_status":"active"}]',
        encoding="utf-8",
    )
    monkeypatch.setattr(fetch_exchange_masterfiles, "SPOTLIGHT_COMPANIES_DIRECTORY_CACHE", cache_path)
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "LEGACY_SPOTLIGHT_COMPANIES_DIRECTORY_CACHE",
        tmp_path / "missing.json",
    )

    source = next(item for item in OFFICIAL_SOURCES if item.key == "spotlight_companies_directory")
    rows, mode = load_spotlight_directory_rows(source)

    assert mode == "cache"
    assert rows == [
        {
            "ticker": "HOME-B",
            "name": "HomeMaid",
            "exchange": "STO",
            "asset_type": "Stock",
            "listing_status": "active",
        }
    ]


def test_load_ngm_companies_rows_prefers_cache(tmp_path, monkeypatch) -> None:
    cache_path = tmp_path / "ngm_companies_page.json"
    cache_path.write_text(
        '[{"ticker":"ARBO-A","name":"Arbona AB","exchange":"STO","asset_type":"Stock","listing_status":"active"}]',
        encoding="utf-8",
    )
    monkeypatch.setattr(fetch_exchange_masterfiles, "NGM_COMPANIES_PAGE_CACHE", cache_path)
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "LEGACY_NGM_COMPANIES_PAGE_CACHE",
        tmp_path / "missing.json",
    )

    source = next(item for item in OFFICIAL_SOURCES if item.key == "ngm_companies_page")
    rows, mode = load_ngm_companies_rows(source)

    assert mode == "cache"
    assert rows == [
        {
            "ticker": "ARBO-A",
            "name": "Arbona AB",
            "exchange": "STO",
            "asset_type": "Stock",
            "listing_status": "active",
        }
    ]


def test_load_bmv_stock_search_rows_falls_back_to_cache(tmp_path, monkeypatch) -> None:
    cache_path = tmp_path / "bmv_stock_search.json"
    cache_path.write_text(
        '[{"ticker":"AMXB","name":"AMERICA MOVIL, S.A.B. DE C.V.","exchange":"BMV","asset_type":"Stock","listing_status":"active"}]',
        encoding="utf-8",
    )
    monkeypatch.setattr(fetch_exchange_masterfiles, "BMV_STOCK_SEARCH_CACHE", cache_path)
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "LEGACY_BMV_STOCK_SEARCH_CACHE",
        tmp_path / "missing.json",
    )
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "fetch_bmv_stock_search",
        lambda source, session=None: (_ for _ in ()).throw(requests.RequestException("boom")),
    )

    source = next(item for item in OFFICIAL_SOURCES if item.key == "bmv_stock_search")
    rows, mode = load_bmv_stock_search_rows(source)

    assert mode == "cache"
    assert rows == [
        {
            "ticker": "AMXB",
            "name": "AMERICA MOVIL, S.A.B. DE C.V.",
            "exchange": "BMV",
            "asset_type": "Stock",
            "listing_status": "active",
        }
    ]


def test_load_bmv_capital_trust_search_rows_falls_back_to_cache(tmp_path, monkeypatch) -> None:
    cache_path = tmp_path / "bmv_capital_trust_search.json"
    cache_path.write_text(
        '[{"ticker":"DANHOS13","name":"DANHOS 13","exchange":"BMV","asset_type":"Stock","listing_status":"active"}]',
        encoding="utf-8",
    )
    monkeypatch.setattr(fetch_exchange_masterfiles, "BMV_CAPITAL_TRUST_SEARCH_CACHE", cache_path)
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "LEGACY_BMV_CAPITAL_TRUST_SEARCH_CACHE",
        tmp_path / "missing.json",
    )
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "fetch_bmv_capital_trust_search",
        lambda source, session=None: (_ for _ in ()).throw(requests.RequestException("boom")),
    )

    source = next(item for item in OFFICIAL_SOURCES if item.key == "bmv_capital_trust_search")
    rows, mode = load_bmv_capital_trust_search_rows(source)

    assert mode == "cache"
    assert rows == [
        {
            "ticker": "DANHOS13",
            "name": "DANHOS 13",
            "exchange": "BMV",
            "asset_type": "Stock",
            "listing_status": "active",
        }
    ]


def test_load_bmv_etf_search_rows_falls_back_to_cache(tmp_path, monkeypatch) -> None:
    cache_path = tmp_path / "bmv_etf_search.json"
    cache_path.write_text(
        '[{"ticker":"CSPXN","name":"iShares Core S&P 500 UCITS ETF USD (Acc)","exchange":"BMV","asset_type":"ETF","listing_status":"active"}]',
        encoding="utf-8",
    )
    monkeypatch.setattr(fetch_exchange_masterfiles, "BMV_ETF_SEARCH_CACHE", cache_path)
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "LEGACY_BMV_ETF_SEARCH_CACHE",
        tmp_path / "missing.json",
    )
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "fetch_bmv_etf_search",
        lambda source, session=None: (_ for _ in ()).throw(requests.RequestException("boom")),
    )

    source = next(item for item in OFFICIAL_SOURCES if item.key == "bmv_etf_search")
    rows, mode = load_bmv_etf_search_rows(source)

    assert mode == "cache"
    assert rows == [
        {
            "ticker": "CSPXN",
            "name": "iShares Core S&P 500 UCITS ETF USD (Acc)",
            "exchange": "BMV",
            "asset_type": "ETF",
            "listing_status": "active",
        }
    ]


def test_load_bmv_issuer_directory_rows_falls_back_to_cache(tmp_path, monkeypatch) -> None:
    cache_path = tmp_path / "bmv_issuer_directory.json"
    cache_path.write_text(
        '[{"ticker":"ROGN","name":"ROCHE HOLDING AG","exchange":"BMV","asset_type":"Stock","listing_status":"active"}]',
        encoding="utf-8",
    )
    monkeypatch.setattr(fetch_exchange_masterfiles, "BMV_ISSUER_DIRECTORY_CACHE", cache_path)
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "LEGACY_BMV_ISSUER_DIRECTORY_CACHE",
        tmp_path / "missing.json",
    )
    monkeypatch.setattr(
        fetch_exchange_masterfiles,
        "fetch_bmv_issuer_directory",
        lambda source, session=None: (_ for _ in ()).throw(requests.RequestException("boom")),
    )

    source = next(item for item in OFFICIAL_SOURCES if item.key == "bmv_issuer_directory")
    rows, mode = load_bmv_issuer_directory_rows(source)

    assert mode == "cache"
    assert rows == [
        {
            "ticker": "ROGN",
            "name": "ROCHE HOLDING AG",
            "exchange": "BMV",
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


def test_load_szse_b_share_list_rows_falls_back_to_cache(tmp_path, monkeypatch) -> None:
    cache_path = tmp_path / "szse_b_share_list.json"
    cache_path.write_text(
        '[{"ticker":"200011","name":"深物业B","exchange":"SZSE","asset_type":"Stock","listing_status":"active"}]',
        encoding="utf-8",
    )
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.SZSE_B_SHARE_LIST_CACHE", cache_path)
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.LEGACY_SZSE_B_SHARE_LIST_CACHE", tmp_path / "missing.json")
    monkeypatch.setattr(
        "scripts.fetch_exchange_masterfiles.fetch_szse_b_share_list",
        lambda source, session=None: (_ for _ in ()).throw(requests.RequestException("boom")),
    )

    source = next(item for item in OFFICIAL_SOURCES if item.key == "szse_b_share_list")
    rows, mode = load_szse_b_share_list_rows(source)

    assert mode == "cache"
    assert rows == [{"ticker": "200011", "name": "深物业B", "exchange": "SZSE", "asset_type": "Stock", "listing_status": "active"}]


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


def test_fetch_source_rows_with_mode_uses_szse_b_share_cache(tmp_path, monkeypatch) -> None:
    cache_path = tmp_path / "szse_b_share_list.json"
    cache_path.write_text(
        '[{"ticker":"200011","name":"深物业B","exchange":"SZSE","asset_type":"Stock","listing_status":"active","source_key":"szse_b_share_list","reference_scope":"listed_companies_subset","official":"true","provider":"SZSE","source_url":"https://example.com"}]',
        encoding="utf-8",
    )
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.SZSE_B_SHARE_LIST_CACHE", cache_path)
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.LEGACY_SZSE_B_SHARE_LIST_CACHE", tmp_path / "missing.json")
    monkeypatch.setattr(
        "scripts.fetch_exchange_masterfiles.fetch_szse_b_share_list",
        lambda source, session=None: (_ for _ in ()).throw(requests.RequestException("boom")),
    )

    source = next(item for item in OFFICIAL_SOURCES if item.key == "szse_b_share_list")
    rows, mode = fetch_source_rows_with_mode(source)

    assert mode == "cache"
    assert rows[0]["ticker"] == "200011"
    assert rows[0]["exchange"] == "SZSE"


def test_fetch_source_rows_with_mode_uses_ngm_companies_cache(tmp_path, monkeypatch) -> None:
    cache_path = tmp_path / "ngm_companies_page.json"
    cache_path.write_text(
        '[{"ticker":"ARBO-A","name":"Arbona AB","exchange":"STO","asset_type":"Stock","listing_status":"active","source_key":"ngm_companies_page","reference_scope":"listed_companies_subset","official":"true","provider":"NGM","source_url":"https://example.com"}]',
        encoding="utf-8",
    )
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.NGM_COMPANIES_PAGE_CACHE", cache_path)
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.LEGACY_NGM_COMPANIES_PAGE_CACHE", tmp_path / "missing.json")
    monkeypatch.setattr(
        "scripts.fetch_exchange_masterfiles.fetch_ngm_companies_page",
        lambda source, session=None: (_ for _ in ()).throw(requests.RequestException("boom")),
    )

    source = next(item for item in OFFICIAL_SOURCES if item.key == "ngm_companies_page")
    rows, mode = fetch_source_rows_with_mode(source)

    assert mode == "cache"
    assert rows[0]["ticker"] == "ARBO-A"
    assert rows[0]["exchange"] == "STO"


def test_fetch_source_rows_with_mode_uses_spotlight_directory_cache(tmp_path, monkeypatch) -> None:
    cache_path = tmp_path / "spotlight_companies_directory.json"
    cache_path.write_text(
        '[{"ticker":"HOME-B","name":"HomeMaid","exchange":"STO","asset_type":"Stock","listing_status":"active","source_key":"spotlight_companies_directory","reference_scope":"listed_companies_subset","official":"true","provider":"Spotlight","source_url":"https://example.com"}]',
        encoding="utf-8",
    )
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.SPOTLIGHT_COMPANIES_DIRECTORY_CACHE", cache_path)
    monkeypatch.setattr(
        "scripts.fetch_exchange_masterfiles.LEGACY_SPOTLIGHT_COMPANIES_DIRECTORY_CACHE",
        tmp_path / "missing.json",
    )
    monkeypatch.setattr(
        "scripts.fetch_exchange_masterfiles.fetch_spotlight_companies_directory",
        lambda source, session=None: (_ for _ in ()).throw(requests.RequestException("boom")),
    )

    source = next(item for item in OFFICIAL_SOURCES if item.key == "spotlight_companies_directory")
    rows, mode = fetch_source_rows_with_mode(source)

    assert mode == "cache"
    assert rows[0]["ticker"] == "HOME-B"
    assert rows[0]["exchange"] == "STO"


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


def test_load_set_dr_search_rows_falls_back_to_cache(tmp_path, monkeypatch) -> None:
    cache_path = tmp_path / "set_dr_search.json"
    cache_path.write_text(
        '[{"ticker":"AMD80","name":"Depositary Receipt on AMD Issued by KTB","exchange":"SET","asset_type":"Stock","listing_status":"active"}]',
        encoding="utf-8",
    )
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.SET_DR_SEARCH_CACHE", cache_path)
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.LEGACY_SET_DR_SEARCH_CACHE", tmp_path / "missing.json")
    monkeypatch.setattr(
        "scripts.fetch_exchange_masterfiles.fetch_set_dr_search",
        lambda source, session=None: (_ for _ in ()).throw(requests.RequestException("boom")),
    )

    source = next(item for item in OFFICIAL_SOURCES if item.key == "set_dr_search")
    rows, mode = load_set_dr_search_rows(source)

    assert mode == "cache"
    assert rows == [{"ticker": "AMD80", "name": "Depositary Receipt on AMD Issued by KTB", "exchange": "SET", "asset_type": "Stock", "listing_status": "active"}]


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


def test_fetch_source_rows_with_mode_uses_set_dr_cache(tmp_path, monkeypatch) -> None:
    cache_path = tmp_path / "set_dr_search.json"
    cache_path.write_text(
        '[{"ticker":"AMD80","name":"Depositary Receipt on AMD Issued by KTB","exchange":"SET","asset_type":"Stock","listing_status":"active","source_key":"set_dr_search","reference_scope":"listed_companies_subset","official":"true","provider":"SET","source_url":"https://example.com"}]',
        encoding="utf-8",
    )
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.SET_DR_SEARCH_CACHE", cache_path)
    monkeypatch.setattr("scripts.fetch_exchange_masterfiles.LEGACY_SET_DR_SEARCH_CACHE", tmp_path / "missing.json")
    monkeypatch.setattr(
        "scripts.fetch_exchange_masterfiles.fetch_set_dr_search",
        lambda source, session=None: (_ for _ in ()).throw(requests.RequestException("boom")),
    )

    source = next(item for item in OFFICIAL_SOURCES if item.key == "set_dr_search")
    rows, mode = fetch_source_rows_with_mode(source)

    assert mode == "cache"
    assert rows[0]["ticker"] == "AMD80"
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
            {"valorSymbol": "ABBN", "company": "ABB Ltd", "isin": "CH0012221716"},
            {"valorSymbol": "ROG", "company": "Roche Holding AG", "isin": "CH0012032048"},
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
            "isin": "CH0012221716",
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
            "isin": "CH0012032048",
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


def test_parse_six_share_details_fqs_payload_maps_rows() -> None:
    payload = {
        "colNames": ["ISIN", "IssuerNameFull", "ValorSymbol", "IndustrySectorDesc", "AssetClassDesc"],
        "rowData": [
            ["CH1169360919", "Accelleron Industries AG", "ACLN", "Electrical engineering & electronics", ""],
            ["IE00B14X4S71", "iShares plc", "IBTS", "Indices", "Fixed Income"],
        ],
    }

    rows = parse_six_share_details_fqs_payload(payload)

    assert rows == [
        {
            "ISIN": "CH1169360919",
            "IssuerNameFull": "Accelleron Industries AG",
            "ValorSymbol": "ACLN",
            "IndustrySectorDesc": "Electrical engineering & electronics",
            "AssetClassDesc": "",
        },
        {
            "ISIN": "IE00B14X4S71",
            "IssuerNameFull": "iShares plc",
            "ValorSymbol": "IBTS",
            "IndustrySectorDesc": "Indices",
            "AssetClassDesc": "Fixed Income",
        },
    ]


def test_build_six_share_details_rows_maps_stock_and_etf_taxonomy() -> None:
    stock_payload = {
        "colNames": ["ISIN", "IssuerNameFull", "ValorSymbol", "IndustrySectorDesc", "AssetClassDesc"],
        "rowData": [["CH1169360919", "Accelleron Industries AG", "ACLN", "Electrical engineering & electronics", ""]],
    }
    etf_payload = {
        "colNames": ["ISIN", "FundLongName", "ValorSymbol", "IndustrySectorDesc", "AssetClassDesc"],
        "rowData": [
            ["IE00B14X4S71", "iShares $ Treasury Bond 1-3yr UCITS ETF USD (Dist)", "IBTS", "Indices", "Fixed Income"]
        ],
    }

    stock_rows = build_six_share_details_rows(
        stock_payload,
        SOURCE,
        {
            "ticker": "ACLN",
            "name": "Accelleron Industries AG",
            "exchange": "SIX",
            "asset_type": "Stock",
            "isin": "CH1169360919",
        },
    )
    etf_rows = build_six_share_details_rows(
        etf_payload,
        SOURCE,
        {
            "ticker": "IE00B14X4S",
            "name": "iShares $ Treasury Bond 1-3yr UCITS ETF USD",
            "exchange": "SIX",
            "asset_type": "ETF",
            "isin": "IE00B14X4S71",
        },
    )

    assert stock_rows[0]["ticker"] == "ACLN"
    assert stock_rows[0]["sector"] == "Industrials"
    assert etf_rows[0]["ticker"] == "IE00B14X4S"
    assert etf_rows[0]["sector"] == "Fixed Income"


def test_build_six_share_details_rows_special_cases_misc_services() -> None:
    payload = {
        "colNames": ["ISIN", "IssuerNameFull", "ValorSymbol", "IndustrySectorDesc"],
        "rowData": [
            ["CH1107979838", "R&S Group Holding AG", "RSGN", "Misc. services"],
            ["CH1386220409", "Sunrise Communications AG", "SUNN", "Misc. services"],
        ],
    }

    rsgn_rows = build_six_share_details_rows(
        payload,
        SOURCE,
        {"ticker": "RSGN", "name": "R&S GROUP HOLDING AG", "exchange": "SIX", "asset_type": "Stock", "isin": "CH1107979838"},
    )
    sunn_rows = build_six_share_details_rows(
        payload,
        SOURCE,
        {"ticker": "SUNN", "name": "SUNRISE N", "exchange": "SIX", "asset_type": "Stock", "isin": "CH1386220409"},
    )

    assert rsgn_rows[0]["sector"] == "Industrials"
    assert sunn_rows[0]["sector"] == "Communication Services"


def test_fetch_six_share_details_fqs_uses_current_missing_taxonomy_rows(tmp_path) -> None:
    listings_path = tmp_path / "listings.csv"
    listings_path.write_text(
        "\n".join(
            [
                "ticker,name,exchange,asset_type,stock_sector,etf_category,sector,country,country_code,isin,aliases",
                "ACLN,Accelleron Industries AG,SIX,Stock,,,,Switzerland,CH,CH1169360919,accelleron",
                "ROG,Roche Holding AG,SIX,Stock,Health Care,,,Switzerland,CH,CH0012032048,roche",
                "IE00B14X4S,iShares $ Treasury Bond 1-3yr UCITS ETF USD,SIX,ETF,,,,Ireland,IE,IE00B14X4S71,ishares",
            ]
        ),
        encoding="utf-8",
    )
    source = MasterfileSource(
        key="six_shares_explorer_full",
        provider="SIX",
        description="Official SIX FQS supplement",
        source_url="https://www.six-group.com/fqs/ref.json",
        format="six_share_details_fqs_json",
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

        def get(self, url, params=None, headers=None, timeout=None):
            self.calls.append((url, params, headers, timeout))
            isin = params["where"].split("=", 1)[1]
            if isin == "CH1169360919":
                return FakeResponse(
                    {
                        "colNames": ["ISIN", "IssuerNameFull", "ValorSymbol", "IndustrySectorDesc", "AssetClassDesc"],
                        "rowData": [
                            ["CH1169360919", "Accelleron Industries AG", "ACLN", "Electrical engineering & electronics", ""]
                        ],
                    }
                )
            return FakeResponse(
                {
                    "colNames": ["ISIN", "FundLongName", "ValorSymbol", "IndustrySectorDesc", "AssetClassDesc"],
                    "rowData": [
                        [
                            "IE00B14X4S71",
                            "iShares $ Treasury Bond 1-3yr UCITS ETF USD (Dist)",
                            "IBTS",
                            "Indices",
                            "Fixed Income",
                        ]
                    ],
                }
            )

    session = FakeSession()
    rows = fetch_six_share_details_fqs(source, session=session, listings_path=listings_path)

    assert [(row["ticker"], row["sector"]) for row in rows] == [
        ("ACLN", "Industrials"),
        ("IE00B14X4S", "Fixed Income"),
    ]
    assert [call[1]["where"] for call in session.calls] == ["ISIN=CH1169360919", "ISIN=IE00B14X4S71"]
    assert all(call[2]["Accept"] == "application/json,text/plain,*/*" for call in session.calls)


def test_six_share_details_source_is_official_taxonomy_supplement() -> None:
    source = next(item for item in OFFICIAL_SOURCES if item.key == "six_shares_explorer_full")
    assert source.format == "six_share_details_fqs_json"
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


def test_parse_six_fund_products_csv_maps_asset_class_to_category() -> None:
    text = "\n".join(
        [
            "FundLongName;ValorSymbol;FundReutersTicker;FundBloombergTicker;ISIN;TradingBaseCurrency;FundCurrency;ProductLineDesc;AssetClassDesc",
            "SIX Equity ETF;EQTY;;;IE0000000001;USD;USD;Exchange Traded Funds;Equity Developed Markets",
            "SIX Crypto ETP;CRYP;;;CH0000000002;USD;USD;Exchange Traded Product;Crypto",
            "SIX Bond ETF;BOND;;;LU0000000003;CHF;CHF;Exchange Traded Funds;Fixed Income",
        ]
    )

    rows = parse_six_fund_products_csv(text, SOURCE)

    assert rows[0]["sector"] == "Equity"
    assert rows[3]["sector"] == "Alternative"
    assert rows[6]["sector"] == "Fixed Income"


def test_parse_six_fund_products_csv_maps_reuters_currency_aliases() -> None:
    text = "\n".join(
        [
            "FundLongName;ValorSymbol;FundReutersTicker;FundBloombergTicker;ISIN;TradingBaseCurrency;FundCurrency;ProductLineDesc;AssetClassDesc",
            "21Shares Ethereum Core ETP;ETHC;CETH S;CETH SE;CH1209763130;USD;USD;Exchange Traded Product;Crypto",
        ]
    )

    rows = parse_six_fund_products_csv(text, SOURCE)

    assert [row["ticker"] for row in rows] == [
        "ETHC",
        "ETHC-USD",
        "ETHCUSD",
        "CETH",
        "CETH-USD",
        "CETHUSD",
    ]
    assert {row["sector"] for row in rows} == {"Alternative"}


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


def test_parse_ngx_company_profile_detail_html_extracts_official_name_and_sector() -> None:
    html = """
    <strong class="CompanyName">FIRST HOLDCO PLC (FIRSTHOLDCO)</strong>
    <strong class="Symbol">FIRSTHOLDCO</strong>
    <strong class="Sector">FINANCIAL SERVICES</strong>
    """

    assert parse_ngx_company_profile_detail_html(html) == {
        "ticker": "FIRSTHOLDCO",
        "name": "FIRST HOLDCO PLC",
        "sector": "FINANCIAL SERVICES",
    }


def test_parse_ngx_company_profile_directory_html_matches_prefix_replaced_current_listing(tmp_path) -> None:
    listings_path = tmp_path / "listings.csv"
    listings_path.write_text(
        "\n".join(
            [
                "listing_key,ticker,exchange,name,asset_type,stock_sector,etf_category,country,country_code,isin,aliases",
                "NGX::FIRSTHOLDC,FIRSTHOLDC,NGX,FIRST HOLDCO PLC,Stock,,,,NG,NGFBNH000009,first hold",
                "NGX::ABBEYBDS,ABBEYBDS,NGX,ABBEY MORTGAGE BANK PLC,Stock,,,,NG,NGABBEYBDS3,abbey",
                "NGX::DUNLOP,DUNLOP,NGX,DN TYRE & RUBBER PLC,Stock,,,,NG,NGDUNLOP0005,dn tyre",
            ]
        ),
        encoding="utf-8",
    )
    html = """
    <a href="https://ngxgroup.com/exchange/data/company-profile/?symbol=FIRSTHOLDCO&directory=companydirectory">FIRSTHOLDCO</a>
    <a href="https://ngxgroup.com/exchange/data/company-profile/?symbol=ABBEYBDS&directory=companydirectory">ABBEYBDS</a>
    """
    source = MasterfileSource(
        key="ngx_company_profile_directory",
        provider="NGX",
        description="Official Nigerian Exchange company profile directory",
        source_url="https://ngxgroup.com/exchange/data/company-profile/",
        format="ngx_company_profile_directory_html",
        reference_scope="exchange_directory",
    )

    rows = parse_ngx_company_profile_directory_html(
        html,
        source,
        profile_details_by_symbol={
            "FIRSTHOLDCO": {
                "ticker": "FIRSTHOLDCO",
                "name": "FIRST HOLDCO PLC",
                "sector": "FINANCIAL SERVICES",
            },
            "ABBEYBDS": {
                "ticker": "ABBEYBDS",
                "name": "ABBEY MORTGAGE BANK PLC",
                "sector": "FINANCIAL SERVICES",
            },
        },
        listings_path=listings_path,
    )

    assert rows == [
        {
            "source_key": "ngx_company_profile_directory",
            "provider": "NGX",
            "source_url": "https://ngxgroup.com/exchange/data/company-profile/?symbol=ABBEYBDS&directory=companydirectory",
            "ticker": "ABBEYBDS",
            "name": "ABBEY MORTGAGE BANK PLC",
            "exchange": "NGX",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "NGABBEYBDS3",
            "sector": "FINANCIAL SERVICES",
        },
        {
            "source_key": "ngx_company_profile_directory",
            "provider": "NGX",
            "source_url": "https://ngxgroup.com/exchange/data/company-profile/?symbol=FIRSTHOLDCO&directory=companydirectory",
            "ticker": "FIRSTHOLDCO",
            "name": "FIRST HOLDCO PLC",
            "exchange": "NGX",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "NGFBNH000009",
            "sector": "FINANCIAL SERVICES",
        },
    ]


def test_dedupe_rows_normalizes_none_values_before_sorting() -> None:
    rows = dedupe_rows(
        [
            {
                "source_key": "source-b",
                "ticker": None,
                "exchange": "NYSE",
                "listing_status": "active",
                "reference_scope": None,
            },
            {
                "source_key": "source-a",
                "ticker": "AAPL",
                "exchange": None,
                "listing_status": "active",
            },
        ]
    )

    assert rows == [
        {
            "source_key": "source-a",
            "ticker": "AAPL",
            "exchange": "",
            "listing_status": "active",
        },
        {
            "source_key": "source-b",
            "ticker": "",
            "exchange": "NYSE",
            "listing_status": "active",
            "reference_scope": "",
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


def test_parse_nse_india_equity_csv_filters_rights_and_keeps_isins() -> None:
    source = MasterfileSource(
        key="nse_india_securities_available",
        provider="NSE India",
        description="Official NSE India securities",
        source_url="https://www.nseindia.com/static/market-data/securities-available-for-trading",
        format="nse_india_securities_available_csv",
    )
    text = "\n".join(
        [
            "SYMBOL,NAME_OF_COMPANY,SERIES,DATE_OF_LISTING,PAID_UP_VALUE,ISIN_NUMBER,FACE_VALUE,",
            "VIVIDEL,Vivid Electromech Limited,ST,07-Apr-26,10,INE24H301028,10,",
            "SUNREST-RE,Sunrest Lifescience Ltd-RE,ST,02-Apr-26,10,INE0PLZ20012,10,",
            "NOTSME,Not SME Limited,BE,01-Jan-26,10,INE111111111,10,",
        ]
    )

    rows = parse_nse_india_equity_csv(text, source, source_url="https://example.com/sme.csv", sme=True)

    assert rows == [
        {
            "source_key": "nse_india_securities_available",
            "provider": "NSE India",
            "source_url": "https://example.com/sme.csv",
            "ticker": "VIVIDEL",
            "name": "Vivid Electromech Limited",
            "exchange": "NSE_IN",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "INE24H301028",
        }
    ]


def test_parse_nse_india_etf_csv_maps_conservative_categories() -> None:
    source = MasterfileSource(
        key="nse_india_securities_available",
        provider="NSE India",
        description="Official NSE India securities",
        source_url="https://www.nseindia.com/static/market-data/securities-available-for-trading",
        format="nse_india_securities_available_csv",
    )
    text = "\n".join(
        [
            "Symbol,Underlying,SecurityName,DateofListing,MarketLot,ISINNumber,FaceValue",
            "GOLDBEES,Gold,NIPINDETFGOLDBEES,19-Mar-07,1,INF204KB17I5,1",
            "LIQUIDBEES,GovernmentSecurities,NIPINDETFLIQUIDBEES,16-Jul-03,1,INF732E01037,1000",
            "NIFTYBEES,Nifty50,NIPINDETFNIFTYBEES,08-Jan-02,1,INF204KB14I2,1",
        ]
    )

    rows = parse_nse_india_etf_csv(text, source, source_url="https://example.com/etf.csv")

    assert [(row["ticker"], row["asset_type"], row["sector"]) for row in rows] == [
        ("GOLDBEES", "ETF", "Commodity"),
        ("LIQUIDBEES", "ETF", "Fixed Income"),
        ("NIFTYBEES", "ETF", "Equity"),
    ]


def test_nse_india_source_is_modeled_as_official_exchange_directory() -> None:
    source = next(item for item in OFFICIAL_SOURCES if item.key == "nse_india_securities_available")

    assert source.provider == "NSE India"
    assert source.reference_scope == "exchange_directory"
    assert source.format == "nse_india_securities_available_csv"


def test_parse_bse_india_scrips_payload_keeps_active_isin_rows_and_etfs() -> None:
    source = MasterfileSource(
        key="bse_india_scrips",
        provider="BSE India",
        description="Official BSE India scrips",
        source_url="https://www.bseindia.com/corporates/List_Scrips.html",
        format="bse_india_scrips_json",
    )
    payload = [
        {
            "scrip_id": "RELIANCE",
            "Scrip_Name": "Reliance Industries Ltd",
            "Status": "Active",
            "Segment": "Equity",
            "ISIN_NUMBER": "INE002A01018",
            "Issuer_Name": "Reliance Industries Limited",
        },
        {
            "scrip_id": "NIFTYIETF",
            "Scrip_Name": "ICICI Prudential Nifty 50 ETF",
            "Status": "Active",
            "Segment": "Equity",
            "ISIN_NUMBER": "INF109K012R6",
            "Issuer_Name": "ICICI Prudential Mutual Fund",
        },
        {
            "scrip_id": "DELISTED",
            "Scrip_Name": "Delisted Co",
            "Status": "Delisted",
            "Segment": "Equity",
            "ISIN_NUMBER": "INE000000000",
        },
        {
            "scrip_id": "NOISIN",
            "Scrip_Name": "No ISIN Co",
            "Status": "Active",
            "Segment": "Equity",
            "ISIN_NUMBER": "",
        },
    ]

    rows = parse_bse_india_scrips_payload(payload, source)

    assert rows == [
        {
            "source_key": "bse_india_scrips",
            "provider": "BSE India",
            "source_url": "https://www.bseindia.com/corporates/List_Scrips.html",
            "ticker": "RELIANCE",
            "name": "Reliance Industries Ltd",
            "exchange": "BSE_IN",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "INE002A01018",
        },
        {
            "source_key": "bse_india_scrips",
            "provider": "BSE India",
            "source_url": "https://www.bseindia.com/corporates/List_Scrips.html",
            "ticker": "NIFTYIETF",
            "name": "ICICI Prudential Nifty 50 ETF",
            "exchange": "BSE_IN",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "INF109K012R6",
            "sector": "Equity",
        },
    ]


def test_bse_india_source_is_modelled_as_official_exchange_directory() -> None:
    source = next(item for item in OFFICIAL_SOURCES if item.key == "bse_india_scrips")

    assert source.provider == "BSE India"
    assert source.reference_scope == "exchange_directory"
    assert source.format == "bse_india_scrips_json"


def test_parse_hkex_securities_list_workbook_keeps_equity_reit_and_etfs() -> None:
    source = MasterfileSource(
        key="hkex_securities_list",
        provider="HKEX",
        description="Official HKEX securities list",
        source_url="https://www.hkex.com.hk/eng/services/trading/securities/securitieslists/ListOfSecurities.xlsx",
        format="hkex_securities_list_xlsx",
    )
    rows = [
        ["List of Securities", None, None, None, None, None],
        ["Updated as at 14/04/2026", None, None, None, None, None],
        ["Stock Code", "Name of Securities", "Category", "Sub-Category", "ISIN", "Trading Currency"],
        ["00005", "HSBC HOLDINGS", "Equity", "Equity Securities (Main Board)", "GB0005405286", "HKD"],
        ["00405", "YUEXIU REIT", "Real Estate Investment Trusts", "", "HK0405033157", "HKD"],
        ["07500", "FI2 CSOP HSCEI", "Exchange Traded Products", "Leveraged and Inverse", "HK0000345782", "HKD"],
        ["02527", "PTT N3508", "Debt Securities", "", "USY71548AX22", "USD"],
        ["09999", "NO ISIN", "Equity", "Equity Securities (Main Board)", "", "HKD"],
    ]
    buffer = io.BytesIO()
    pd.DataFrame(rows).to_excel(buffer, index=False, header=False)

    parsed_rows = parse_hkex_securities_list_workbook(buffer.getvalue(), source)

    assert parsed_rows == [
        {
            "source_key": "hkex_securities_list",
            "provider": "HKEX",
            "source_url": "https://www.hkex.com.hk/eng/services/trading/securities/securitieslists/ListOfSecurities.xlsx",
            "ticker": "00005",
            "name": "HSBC HOLDINGS",
            "exchange": "HKEX",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "GB0005405286",
        },
        {
            "source_key": "hkex_securities_list",
            "provider": "HKEX",
            "source_url": "https://www.hkex.com.hk/eng/services/trading/securities/securitieslists/ListOfSecurities.xlsx",
            "ticker": "00405",
            "name": "YUEXIU REIT",
            "exchange": "HKEX",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "HK0405033157",
            "sector": "Real Estate",
        },
        {
            "source_key": "hkex_securities_list",
            "provider": "HKEX",
            "source_url": "https://www.hkex.com.hk/eng/services/trading/securities/securitieslists/ListOfSecurities.xlsx",
            "ticker": "07500",
            "name": "FI2 CSOP HSCEI",
            "exchange": "HKEX",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "HK0000345782",
            "sector": "Leveraged/Inverse",
        },
    ]


def test_hkex_securities_list_source_is_modeled_as_official_exchange_directory() -> None:
    source = next(item for item in OFFICIAL_SOURCES if item.key == "hkex_securities_list")

    assert source.provider == "HKEX"
    assert source.reference_scope == "exchange_directory"
    assert source.format == "hkex_securities_list_xlsx"


def test_parse_sgx_securities_prices_payload_keeps_stock_reit_adr_and_etfs() -> None:
    source = MasterfileSource(
        key="sgx_securities_prices",
        provider="SGX",
        description="Official SGX securities prices",
        source_url="https://www.sgx.com/securities/securities-prices",
        format="sgx_securities_prices_json",
    )
    payload = {
        "data": {
            "prices": [
                {"nc": "LVR", "n": "17LIVE GROUP", "type": "stocks"},
                {"nc": "A35", "n": "ABF SG BOND ETF", "type": "etfs"},
                {"nc": "C38U", "n": "CapLand IntCom T", "type": "reits"},
                {"nc": "S27", "n": "SIA ADR", "type": "adrs"},
                {"nc": "VT2W", "n": "17LIVE W281207", "type": "companywarrants"},
                {"nc": "", "n": "Missing Code", "type": "stocks"},
            ]
        }
    }

    rows = parse_sgx_securities_prices_payload(payload, source)

    assert rows == [
        {
            "source_key": "sgx_securities_prices",
            "provider": "SGX",
            "source_url": "https://api.sgx.com/securities/v1.1",
            "ticker": "LVR",
            "name": "17LIVE GROUP",
            "exchange": "SGX",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
        {
            "source_key": "sgx_securities_prices",
            "provider": "SGX",
            "source_url": "https://api.sgx.com/securities/v1.1",
            "ticker": "A35",
            "name": "ABF SG BOND ETF",
            "exchange": "SGX",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "sector": "Fixed Income",
        },
        {
            "source_key": "sgx_securities_prices",
            "provider": "SGX",
            "source_url": "https://api.sgx.com/securities/v1.1",
            "ticker": "C38U",
            "name": "CapLand IntCom T",
            "exchange": "SGX",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "sector": "Real Estate",
        },
        {
            "source_key": "sgx_securities_prices",
            "provider": "SGX",
            "source_url": "https://api.sgx.com/securities/v1.1",
            "ticker": "S27",
            "name": "SIA ADR",
            "exchange": "SGX",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
    ]


def test_sgx_securities_prices_source_is_modeled_as_official_exchange_directory() -> None:
    source = next(item for item in OFFICIAL_SOURCES if item.key == "sgx_securities_prices")

    assert source.provider == "SGX"
    assert source.reference_scope == "exchange_directory"
    assert source.format == "sgx_securities_prices_json"


def test_parse_dfm_company_profile_payloads_keeps_dfm_stock_reit_and_etfs() -> None:
    source = MasterfileSource(
        key="dfm_listed_securities",
        provider="DFM",
        description="Official DFM listed securities",
        source_url="https://www.dfm.ae/issuers/listed-securities/company-profile-page",
        format="dfm_listed_securities_json",
    )
    profiles = [
        {
            "Symbol": "DFM",
            "Market": "DFM",
            "ISIN": "AED000901010",
            "Sector": "Financials",
            "InstrumentType": "equities",
            "FullName": "Dubai Financial Market PJSC",
        },
        {
            "Symbol": "AMCREIT",
            "Market": "DFM",
            "ISIN": "AEA003630067",
            "Sector": "Real Estate Investment Trust",
            "InstrumentType": "realEstateInvestmentTrust",
            "FullName": "Al Mal Capital REIT",
        },
        {
            "Symbol": "CHAE",
            "Market": "DFM",
            "ISIN": "IE00BKDMN692",
            "Sector": "Exchange Traded Funds",
            "InstrumentType": "exchangeTradedFunds",
            "FullName": "Chimera S&P UAE UCITS ETF - Share Class A - Accumulating",
        },
        {
            "Symbol": "ABTC",
            "Market": "NASDAQ Dubai",
            "ISIN": "CH0454664001",
            "Sector": "",
            "InstrumentType": "funds",
            "FullName": "21Shares Bitcoin ETP",
        },
        {
            "Symbol": "NOISIN",
            "Market": "DFM",
            "ISIN": "",
            "Sector": "Financials",
            "InstrumentType": "equities",
            "FullName": "No ISIN PJSC",
        },
    ]

    rows = parse_dfm_company_profile_payloads(profiles, source)

    assert rows == [
        {
            "source_key": "dfm_listed_securities",
            "provider": "DFM",
            "source_url": "https://api2.dfm.ae/web/widgets/v1/data",
            "ticker": "DFM",
            "name": "Dubai Financial Market PJSC",
            "exchange": "DFM",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "AED000901010",
            "sector": "Financials",
        },
        {
            "source_key": "dfm_listed_securities",
            "provider": "DFM",
            "source_url": "https://api2.dfm.ae/web/widgets/v1/data",
            "ticker": "AMCREIT",
            "name": "Al Mal Capital REIT",
            "exchange": "DFM",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "AEA003630067",
            "sector": "Real Estate",
        },
        {
            "source_key": "dfm_listed_securities",
            "provider": "DFM",
            "source_url": "https://api2.dfm.ae/web/widgets/v1/data",
            "ticker": "CHAE",
            "name": "Chimera S&P UAE UCITS ETF - Share Class A - Accumulating",
            "exchange": "DFM",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "IE00BKDMN692",
            "sector": "Equity",
        },
    ]


def test_dfm_listed_securities_source_is_modeled_as_official_exchange_directory() -> None:
    source = next(item for item in OFFICIAL_SOURCES if item.key == "dfm_listed_securities")

    assert source.provider == "DFM"
    assert source.reference_scope == "exchange_directory"
    assert source.format == "dfm_listed_securities_json"


def test_parse_boursa_kuwait_legacy_mix_payload_keeps_regular_and_reit_rows() -> None:
    source = MasterfileSource(
        key="boursa_kuwait_stocks",
        provider="Boursa Kuwait",
        description="Official Boursa Kuwait market data",
        source_url="https://www.boursakuwait.com.kw/en/securities/main-market/overview---main-market/",
        format="boursa_kuwait_legacy_mix_json",
    )
    payload = {
        "HED": {
            "TD": (
                "EXCHANGE|SYMBOL|INSTRUMENT_TYPE|SYMBOL_DESCRIPTION|SECTOR|CURRENCY|SHRT_DSC|"
                "DECIMAL_PLACES|MARKET_ID|LOT_SIZE|UNIT|INDEX_TYPE|COMPANY_CODE|CORRECTION_FACTOR|"
                "EQUITY_SYMBOL|CFID|SERIAL|TI|RIC_CODE|DS|ISIN_CODE|LASTTRADABLEDATE|SOURCE_SHORT_ID|"
                "STRIKE_PRICE|EXP_DATE|OPI|AST|TSZ|TCL1|TCL2|TCL3|FSSD|FSED|SSSD|SSED|MDATE|TC|"
                "FSSH|FSEH|SSSH|SSEH|BBGID|GROUP_SECTORS|LOT_SIZE_DEC|CONTRACT_SIZE|LISTING_DATE"
            ),
            "SCTD": "SECTOR|SECT_DSC|SORT_KEY",
        },
        "DAT": {
            "SCTD": [
                "24|Banking|",
                "22|Telecommunications|",
            ],
            "TD": [
                "KSE|NBK`B|0|NATIONAL BANK OF KUWAIT|24|KWD|NBK|-1|B|0|||101|10||||||NBK|KW0EQ0100010||||||||||||||||||||||||",
                "KSE|NBK`R|0|National Bank of Kuwait|24|KWD|NBK|-1|P|1|||101|10||||||NBK|KW0EQ0100010||||||||||||||||||||||||",
                "KSE|OOREDOO`R|0|National Mobile Telecommunications|22|KWD|OOREDOO|-1|M|1|||613|10||||||OOREDOO|KW0EQ0601058||||||||||||||||||||||||",
                "KSE|BAITAKREIT`F|65|KFH Capital REIT||KWD|BAITAKREIT|-1|T|1|||949|10||||||BAITAKREIT|KW0EQ0909493||||||||||||||||||||||||",
                "KSE|BADISIN`R|0|Bad ISIN|24|KWD|BAD|-1|M|1|||999|10||||||BAD|BAD||||||||||||||||||||||||",
            ],
        },
    }

    rows = parse_boursa_kuwait_legacy_mix_payload(payload, source)

    assert rows == [
        {
            "source_key": "boursa_kuwait_stocks",
            "provider": "Boursa Kuwait",
            "source_url": "https://www.boursakuwait.com.kw/data-api/legacy-mix-services",
            "ticker": "NBK",
            "name": "National Bank of Kuwait",
            "exchange": "BK",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "KW0EQ0100010",
            "sector": "Financials",
        },
        {
            "source_key": "boursa_kuwait_stocks",
            "provider": "Boursa Kuwait",
            "source_url": "https://www.boursakuwait.com.kw/data-api/legacy-mix-services",
            "ticker": "OOREDOO",
            "name": "National Mobile Telecommunications",
            "exchange": "BK",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "KW0EQ0601058",
            "sector": "Communication Services",
        },
        {
            "source_key": "boursa_kuwait_stocks",
            "provider": "Boursa Kuwait",
            "source_url": "https://www.boursakuwait.com.kw/data-api/legacy-mix-services",
            "ticker": "BAITAKREIT",
            "name": "KFH Capital REIT",
            "exchange": "BK",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "KW0EQ0909493",
            "sector": "Real Estate",
        },
    ]


def test_boursa_kuwait_stocks_source_is_modeled_as_official_exchange_directory() -> None:
    source = next(item for item in OFFICIAL_SOURCES if item.key == "boursa_kuwait_stocks")

    assert source.provider == "Boursa Kuwait"
    assert source.reference_scope == "exchange_directory"
    assert source.format == "boursa_kuwait_legacy_mix_json"


def test_parse_bist_kap_mkk_listed_securities_payload_joins_kap_stocks_to_mkk_isins() -> None:
    source = MasterfileSource(
        key="bist_kap_mkk_listed_securities",
        provider="KAP/MKK",
        description="Official BIST KAP/MKK listed securities",
        source_url="https://www.kap.org.tr/tr/bist-sirketler",
        format="bist_kap_mkk_listed_securities_json",
    )
    kap_content = (
        '"mkkMemberOid":"cid-ak","kapMemberTitle":"AKBANK T.A.Ş.",'
        '"relatedMemberTitle":"","stockCode":"AKBNK","cityName":"İSTANBUL"'
        '"mkkMemberOid":"cid-as","kapMemberTitle":"ASELSAN ELEKTRONİK SANAYİ VE TİCARET A.Ş.",'
        '"relatedMemberTitle":"","stockCode":"ASELS","cityName":"ANKARA"'
    )
    kap_html = f"<html><script>self.__next_f.push([1,{json.dumps(kap_content)}])</script></html>"
    mkk_payload = [
        {
            "borsaKodu": "AKBNK",
            "aciklama": "AKBANK T.A.Ş.",
            "isinKodu": "TRAAKBNK91N6",
            "kategori": "HS",
            "takasKodu": "ESKİ",
        },
        {
            "borsaKodu": "ASELS",
            "aciklama": "ASELSAN ELEKTRONİK SANAYİ VE TİCARET A.Ş. İMTİYAZLI",
            "isinKodu": "TREASLS00018",
            "kategori": "HS",
            "takasKodu": "İMTİYAZ",
        },
        {
            "borsaKodu": "ASELS",
            "aciklama": "ASELSAN ELEKTRONİK SANAYİ VE TİCARET A.Ş.",
            "isinKodu": "TRAASELS91H2",
            "kategori": "HS",
            "takasKodu": "ESKİ",
        },
        {
            "borsaKodu": "GLDTR",
            "aciklama": "QNB FİNANS PORTFÖY ALTIN KATILIM BORSA YATIRIM FONU",
            "isinKodu": "TRYFNBK00055",
            "kategori": "BYF",
            "takasKodu": "BORSA YATIRIM FONU",
        },
        {
            "borsaKodu": "ANRUV",
            "aciklama": "AKBNKP3004260098.00GRM00000.1NA",
            "isinKodu": "TRWGRNM19133",
            "kategori": "VR",
            "takasKodu": "VARANT",
        },
    ]

    assert parse_bist_kap_company_list(kap_html) == {
        "AKBNK": "AKBANK T.A.Ş.",
        "ASELS": "ASELSAN ELEKTRONİK SANAYİ VE TİCARET A.Ş.",
    }
    rows = parse_bist_kap_mkk_listed_securities_payload(kap_html, mkk_payload, source)

    assert rows == [
        {
            "source_key": "bist_kap_mkk_listed_securities",
            "provider": "KAP/MKK",
            "source_url": "https://www.kap.org.tr/tr/bist-sirketler",
            "ticker": "AKBNK",
            "name": "AKBANK T.A.Ş.",
            "exchange": "BIST",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "TRAAKBNK91N6",
        },
        {
            "source_key": "bist_kap_mkk_listed_securities",
            "provider": "KAP/MKK",
            "source_url": "https://www.kap.org.tr/tr/bist-sirketler",
            "ticker": "ASELS",
            "name": "ASELSAN ELEKTRONİK SANAYİ VE TİCARET A.Ş.",
            "exchange": "BIST",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "TRAASELS91H2",
        },
        {
            "source_key": "bist_kap_mkk_listed_securities",
            "provider": "KAP/MKK",
            "source_url": "https://www.mkk.com.tr/api/getSecurities",
            "ticker": "GLDTR",
            "name": "QNB FİNANS PORTFÖY ALTIN KATILIM BORSA YATIRIM FONU",
            "exchange": "BIST",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "TRYFNBK00055",
            "sector": "Commodity",
        },
    ]


def test_bist_kap_mkk_listed_securities_source_is_modeled_as_official_exchange_directory() -> None:
    source = next(item for item in OFFICIAL_SOURCES if item.key == "bist_kap_mkk_listed_securities")

    assert source.provider == "KAP/MKK"
    assert source.reference_scope == "exchange_directory"
    assert source.format == "bist_kap_mkk_listed_securities_json"


def test_parse_bahrain_bourse_isin_codes_html_keeps_local_equities_only() -> None:
    source = MasterfileSource(
        key="bahrain_bourse_listed_companies",
        provider="Bahrain Bourse",
        description="Official Bahrain Bourse ISIN codes",
        source_url="https://bahrainbourse.com/en/Bahrain%20Clear/ISINCodes",
        format="bahrain_bourse_isin_codes_html",
    )
    html = """
        <table>
            <tr><th>BHB Company Symbol</th><th>ISIN</th><th>Issuer Name</th><th>Issue Description</th><th>CFI</th><th>LEI</th><th>FISN</th></tr>
            <tr><td>Equities</td></tr>
            <tr><td>ABC</td><td>BH0008794115</td><td>Arab Banking Corporation B.S.C.</td><td>Common Share</td><td>ESVUFR</td><td></td><td>ABC/Sh USD 10</td></tr>
            <tr><td>EBRIT</td><td>BH0005158K14</td><td>Eskan Bank Realty Income Trust</td><td>Mutual Fund</td><td>CBXIXX</td><td></td><td>Eskan Bk REIT/Sh BHD 1</td></tr>
            <tr><td>Equities (Non Bahrain Companies)</td></tr>
            <tr><td>KFH</td><td>KW0EQ0100085</td><td>Kuwait Finance House K.S.C.P.</td><td>Common Share</td><td>ESVUFR</td><td></td><td>KFH/Sh KWD 0.1</td></tr>
            <tr><td>Bahrain Investment Market (BIM)</td></tr>
            <tr><td>HOPE</td><td>BH0003538822</td><td>HOPE VENTURES HOLDING B.S.C (c)</td><td>Common Share</td><td>ESVUFR</td><td></td><td>HOPE/Sh BHD 0.1</td></tr>
            <tr><td>Bonds &amp; Sukuk</td></tr>
            <tr><td>GDEV22.BND</td><td>BH0006FH0881</td><td>Government Development Bond - Issue 22</td><td>Government Bond</td><td>DBVUFR</td><td></td><td>Govt Bond</td></tr>
        </table>
    """

    rows = parse_bahrain_bourse_isin_codes_html(html, source)

    assert rows == [
        {
            "source_key": "bahrain_bourse_listed_companies",
            "provider": "Bahrain Bourse",
            "source_url": "https://bahrainbourse.com/en/Bahrain%20Clear/ISINCodes",
            "ticker": "ABC",
            "name": "Arab Banking Corporation B.S.C.",
            "exchange": "BHB",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "BH0008794115",
        },
        {
            "source_key": "bahrain_bourse_listed_companies",
            "provider": "Bahrain Bourse",
            "source_url": "https://bahrainbourse.com/en/Bahrain%20Clear/ISINCodes",
            "ticker": "EBRIT",
            "name": "Eskan Bank Realty Income Trust",
            "exchange": "BHB",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "BH0005158K14",
            "sector": "Real Estate",
        },
        {
            "source_key": "bahrain_bourse_listed_companies",
            "provider": "Bahrain Bourse",
            "source_url": "https://bahrainbourse.com/en/Bahrain%20Clear/ISINCodes",
            "ticker": "HOPE",
            "name": "HOPE VENTURES HOLDING B.S.C (c)",
            "exchange": "BHB",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "BH0003538822",
        },
    ]


def test_bahrain_bourse_listed_companies_source_is_modeled_as_official_exchange_directory() -> None:
    source = next(item for item in OFFICIAL_SOURCES if item.key == "bahrain_bourse_listed_companies")

    assert source.provider == "Bahrain Bourse"
    assert source.reference_scope == "exchange_directory"
    assert source.format == "bahrain_bourse_isin_codes_html"


def test_parse_tadawul_securities_payload_filters_active_stocks_and_funds() -> None:
    source = MasterfileSource(
        key="tadawul_main_market_watch",
        provider="Saudi Exchange",
        description="Official Saudi Exchange securities",
        source_url="https://www.saudiexchange.sa/wps/portal/tadawul/market-participants/issuers/issuers-directory",
        format="tadawul_securities_json",
    )
    issuer_directory_html = """
        <script>
        var issuers = [
          { company: "2010", companyDisplay: "SABIC", price: "1",
            link: "/wps/portal/saudiexchange/hidden/company-profile-main/?companySymbol=2010#chart_tab2" },
          { company: "9505", companyDisplay: "Arab Sea", price: "1",
            link: "/wps/portal/saudiexchange/hidden/company-profile-nomu-parallel/?companySymbol=9505#chart_tab2" },
          { company: "4700", companyDisplay: "Alkhabeer Income", price: "1",
            link: "/wps/portal/saudiexchange/hidden/company-profile-main/?companySymbol=4700#chart_tab2" }
        ];
        </script>
    """
    search_payload = [
        {
            "symbol": "2010",
            "tradingNameEn": "SABIC",
            "companyNameEN": "Saudi Basic Industries Corp.",
            "market_type": "M",
            "isin": "SA0007879121",
        },
        {
            "symbol": "9505",
            "tradingNameEn": "Arab Sea",
            "companyNameEN": "Arab Sea Information Systems Co.",
            "market_type": "S",
            "isin": "SA14TG012N13",
        },
        {
            "symbol": "4330",
            "tradingNameEn": "Riyad REIT",
            "companyNameEN": "Riyad REIT Fund",
            "market_type": "M",
            "isin": "SA14GG523Q50",
        },
        {
            "symbol": "9405",
            "tradingNameEn": "Albilad Gold ETF",
            "companyNameEN": "Albilad Gold ETF",
            "market_type": "E",
            "isin": "SA000A0T0LJ8",
        },
        {
            "symbol": "4700",
            "tradingNameEn": "Alkhabeer Diversified Income",
            "companyNameEN": "Alkhabeer Diversified Income Traded Fund",
            "market_type": "C",
            "isin": "SA1590D1RLL8",
        },
        {
            "symbol": "5302",
            "tradingNameEn": "Sukuk 2030",
            "companyNameEN": "Government Sukuk",
            "market_type": "B",
            "isin": "SA000A0T0LJ8",
        },
        {
            "symbol": "ABCD",
            "tradingNameEn": "Ignored Option",
            "companyNameEN": "Ignored Option",
            "market_type": "D",
            "isin": "",
        },
    ]

    assert parse_tadawul_active_symbols(issuer_directory_html) == {"2010", "9505", "4700"}
    rows = parse_tadawul_securities_payload(issuer_directory_html, search_payload, source)

    assert rows == [
        {
            "source_key": "tadawul_main_market_watch",
            "provider": "Saudi Exchange",
            "source_url": "https://www.saudiexchange.sa/tadawul.eportal.theme.helper/ThemeSearchUtilityServlet",
            "ticker": "2010",
            "name": "SABIC",
            "exchange": "TADAWUL",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "SA0007879121",
        },
        {
            "source_key": "tadawul_main_market_watch",
            "provider": "Saudi Exchange",
            "source_url": "https://www.saudiexchange.sa/tadawul.eportal.theme.helper/ThemeSearchUtilityServlet",
            "ticker": "9505",
            "name": "Arab Sea",
            "exchange": "TADAWUL",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "SA14TG012N13",
        },
        {
            "source_key": "tadawul_main_market_watch",
            "provider": "Saudi Exchange",
            "source_url": "https://www.saudiexchange.sa/tadawul.eportal.theme.helper/ThemeSearchUtilityServlet",
            "ticker": "9405",
            "name": "Albilad Gold ETF",
            "exchange": "TADAWUL",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "SA000A0T0LJ8",
            "sector": "Commodity",
        },
        {
            "source_key": "tadawul_main_market_watch",
            "provider": "Saudi Exchange",
            "source_url": "https://www.saudiexchange.sa/tadawul.eportal.theme.helper/ThemeSearchUtilityServlet",
            "ticker": "4700",
            "name": "Alkhabeer Diversified Income",
            "exchange": "TADAWUL",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "SA1590D1RLL8",
            "sector": "Multi-Asset",
        },
    ]


def test_tadawul_main_market_watch_source_is_modeled_as_official_exchange_directory() -> None:
    source = next(item for item in OFFICIAL_SOURCES if item.key == "tadawul_main_market_watch")

    assert source.provider == "Saudi Exchange"
    assert source.reference_scope == "exchange_directory"
    assert source.format == "tadawul_securities_json"


def test_parse_adx_market_watch_payloads_keeps_primary_equities_growth_and_etfs() -> None:
    source = MasterfileSource(
        key="adx_market_watch",
        provider="ADX",
        description="ADX market watch",
        source_url="https://www.adx.ae/english/pages/marketwatch.aspx",
        format="adx_market_watch_json",
    )
    issuers_payload = {
        "response": {
            "issuers": [
                {
                    "tradingCode": "ADAVIATION",
                    "nameEnglish": "Abu Dhabi Aviation Co",
                    "isin": "AEA001001014",
                    "sectorNameArabic": "Consumer Discretionary",
                    "status": "L",
                },
                {
                    "tradingCode": "ANAN",
                    "nameEnglish": "ANAN INVESTMENT HOLDING P.J.S.C",
                    "isin": "AEW000201015",
                    "sectorNameArabic": "Financials",
                    "status": "L",
                },
                {
                    "tradingCode": "SUKUK",
                    "nameEnglish": "Chimera Umbrella Fund - Chimera JP Morgan Global Sukuk",
                    "isin": "AEC01443C244",
                    "status": "L",
                },
                {
                    "tradingCode": "GFH",
                    "nameEnglish": "GFH Financial Group B.S.C",
                    "isin": "BH000A0CAQK6",
                    "sectorNameArabic": "Financials",
                    "status": "L",
                },
            ]
        }
    }
    main_market_payload = {
        "response": {
            "results": [
                {
                    "companySymbol": "ADAVIATION",
                    "companyISIN": "AEA001001014",
                    "companyID": "Abu Dhabi Aviation Co",
                    "boardId": "EQTY",
                },
                {
                    "companySymbol": "E7",
                    "companyISIN": "AEE01073A225",
                    "companyID": "E7 Group PJSC",
                    "boardId": "EQTY",
                },
                {
                    "companySymbol": "GFH",
                    "companyISIN": "BH000A0CAQK6",
                    "companyID": "GFH Financial Group B.S.C",
                    "boardId": "DUAL",
                },
                {
                    "companySymbol": "E7W",
                    "companyISIN": "AER01074A228",
                    "companyID": "E7 Group PJSC Warrants",
                    "boardId": "SPCW",
                },
            ]
        }
    }
    growth_market_payload = {
        "response": {
            "results": [
                {
                    "companySymbol": "ANAN",
                    "companyISIN": "AEW000201015",
                    "companyID": "ANAN INVESTMENT HOLDING P.J.S.C",
                    "boardId": "PRCN",
                }
            ]
        }
    }
    etf_payload = {
        "response": {
            "results": [
                {
                    "companySymbol": "SUKUK",
                    "companyISIN": "AEC01443C244",
                    "companyID": "Chimera Umbrella Fund - Chimera JP Morgan Global Sukuk",
                    "boardId": "FUND",
                }
            ]
        }
    }
    symbol_sector_payload = {
        "response": {
            "companies": {
                "company": [
                    {"symbol": "ADAVIATION", "sectorNameEn": "Consumer Discretionary"},
                    {"symbol": "ANAN", "sectorNameEn": "Industrial"},
                ]
            }
        }
    }

    rows = parse_adx_market_watch_payloads(
        issuers_payload,
        main_market_payload,
        growth_market_payload,
        etf_payload,
        symbol_sector_payload,
        source,
    )

    assert rows == [
        {
            "source_key": "adx_market_watch",
            "provider": "ADX",
            "source_url": "https://www.adx.ae/english/pages/marketwatch.aspx",
            "ticker": "ADAVIATION",
            "name": "Abu Dhabi Aviation Co",
            "exchange": "ADX",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "AEA001001014",
            "sector": "Consumer Discretionary",
        },
        {
            "source_key": "adx_market_watch",
            "provider": "ADX",
            "source_url": "https://www.adx.ae/english/pages/marketwatch.aspx",
            "ticker": "E7",
            "name": "E7 Group PJSC",
            "exchange": "ADX",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "AEE01073A225",
        },
        {
            "source_key": "adx_market_watch",
            "provider": "ADX",
            "source_url": "https://www.adx.ae/english/pages/marketwatch.aspx",
            "ticker": "ANAN",
            "name": "ANAN INVESTMENT HOLDING P.J.S.C",
            "exchange": "ADX",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "AEW000201015",
            "sector": "Industrials",
        },
        {
            "source_key": "adx_market_watch",
            "provider": "ADX",
            "source_url": "https://www.adx.ae/english/pages/marketwatch.aspx",
            "ticker": "SUKUK",
            "name": "Chimera Umbrella Fund - Chimera JP Morgan Global Sukuk",
            "exchange": "ADX",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "AEC01443C244",
            "sector": "Fixed Income",
        },
    ]


def test_adx_market_watch_source_is_modeled_as_official_exchange_directory() -> None:
    source = next(item for item in OFFICIAL_SOURCES if item.key == "adx_market_watch")

    assert source.provider == "ADX"
    assert source.reference_scope == "exchange_directory"
    assert source.format == "adx_market_watch_json"


def test_parse_qse_market_watch_payload_keeps_stocks_and_etfs_only() -> None:
    source = MasterfileSource(
        key="qse_market_watch",
        provider="QSE",
        description="Official QSE market watch",
        source_url="https://www.qe.com.qa/wp/mw/data/MarketWatch.txt",
        format="qse_market_watch_json",
    )
    payload = {
        "rows": [
            {
                "Symbol": "QNBK",
                "CompanyEN": "QNB",
                "CompType": "COMP",
                "SectorEN": "Banks & Financial Services",
            },
            {
                "Symbol": "QETF",
                "CompanyEN": "QE Index ETF",
                "CompType": "ETF",
                "SectorEN": "ETF",
            },
            {
                "Symbol": "TQES",
                "CompanyEN": "Techno Q",
                "CompType": "V",
                "SectorEN": "QVM",
            },
            {
                "Symbol": "GA85",
                "CompanyEN": "Bond 4.30% 240830",
                "CompType": "BOND",
                "SectorEN": "Government Bonds",
            },
        ]
    }

    rows = parse_qse_market_watch_payload(payload, source)

    assert rows == [
        {
            "source_key": "qse_market_watch",
            "provider": "QSE",
            "source_url": "https://www.qe.com.qa/wp/mw/data/MarketWatch.txt",
            "ticker": "QNBK",
            "name": "QNB",
            "exchange": "QSE",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "sector": "Financials",
        },
        {
            "source_key": "qse_market_watch",
            "provider": "QSE",
            "source_url": "https://www.qe.com.qa/wp/mw/data/MarketWatch.txt",
            "ticker": "QETF",
            "name": "QE Index ETF",
            "exchange": "QSE",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "sector": "Equity",
        },
        {
            "source_key": "qse_market_watch",
            "provider": "QSE",
            "source_url": "https://www.qe.com.qa/wp/mw/data/MarketWatch.txt",
            "ticker": "TQES",
            "name": "Techno Q",
            "exchange": "QSE",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
        },
    ]


def test_fetch_qse_market_watch_rows_uses_curl_user_agent() -> None:
    source = MasterfileSource(
        key="qse_market_watch",
        provider="QSE",
        description="Official QSE market watch",
        source_url="https://www.qe.com.qa/wp/mw/data/MarketWatch.txt",
        format="qse_market_watch_json",
    )

    class FakeResponse:
        def json(self):
            return {"rows": [{"Symbol": "ORDS", "CompanyEN": "Ooredoo", "CompType": "COMP", "SectorEN": "Telecoms"}]}

        def raise_for_status(self):
            return None

    class FakeSession:
        def __init__(self):
            self.headers = None

        def get(self, url, headers=None, timeout=None):
            assert url == "https://www.qe.com.qa/wp/mw/data/MarketWatch.txt"
            self.headers = headers
            return FakeResponse()

    session = FakeSession()

    rows = fetch_qse_market_watch_rows(source, session=session)

    assert session.headers["User-Agent"] == "curl/8.0"
    assert rows[0]["ticker"] == "ORDS"
    assert rows[0]["sector"] == "Communication Services"


def test_qse_market_watch_source_is_modeled_as_official_exchange_directory() -> None:
    source = next(item for item in OFFICIAL_SOURCES if item.key == "qse_market_watch")

    assert source.provider == "QSE"
    assert source.reference_scope == "exchange_directory"
    assert source.format == "qse_market_watch_json"


def test_parse_msx_companies_payload_keeps_active_stock_rows_only() -> None:
    source = MasterfileSource(
        key="muscat_securities_companies",
        provider="MSX",
        description="Official MSX companies",
        source_url="https://www.msx.om/companies.aspx",
        format="msx_companies_json",
    )
    payload = {
        "d": [
            {"Symbol": "CMII", "LongNameEn": "CONSTRUCTION MATERIAL INDUSTRIES", "Listed": "1", "Sector": "3", "SubSector": "11"},
            {"Symbol": "BANK", "LongNameEn": "BANK MUSCAT", "Listed": "2", "Sector": "1", "SubSector": "1"},
            {"Symbol": "RIGHT", "LongNameEn": "AL ANWAR HOLDING -RIGHT ISSUE", "Listed": "1", "Sector": "1", "SubSector": "1"},
            {"Symbol": "BOND", "LongNameEn": "OMAN BOND", "Listed": "1", "Sector": "4", "SubSector": "0"},
            {"Symbol": "OLD", "LongNameEn": "OLD COMPANY", "Listed": "3", "Sector": "3", "SubSector": "11"},
        ]
    }

    rows = parse_msx_companies_payload(payload, source)

    assert rows == [
        {
            "source_key": "muscat_securities_companies",
            "provider": "MSX",
            "source_url": "https://www.msx.om/companies.aspx",
            "ticker": "CMII",
            "name": "CONSTRUCTION MATERIAL INDUSTRIES",
            "exchange": "MSX",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "sector": "Materials",
        },
        {
            "source_key": "muscat_securities_companies",
            "provider": "MSX",
            "source_url": "https://www.msx.om/companies.aspx",
            "ticker": "BANK",
            "name": "BANK MUSCAT",
            "exchange": "MSX",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "sector": "Financial Services",
        },
    ]


def test_msx_companies_source_is_modeled_as_official_exchange_directory() -> None:
    source = next(item for item in OFFICIAL_SOURCES if item.key == "muscat_securities_companies")

    assert source.provider == "MSX"
    assert source.reference_scope == "exchange_directory"
    assert source.format == "msx_companies_json"


def test_parse_nzx_instruments_next_data_payload_keeps_nzsx_stocks_and_etfs() -> None:
    source = MasterfileSource(
        key="nzx_instruments",
        provider="NZX",
        description="Official NZX instruments",
        source_url="https://new.nzx.com/markets/NZSX",
        format="nzx_instruments_next_data",
    )
    payload = {
        "props": {
            "pageProps": {
                "data": {
                    "instruments": {
                        "activeInstruments": [
                            {
                                "isin": "NZAIAE0002S6",
                                "code": "AIA",
                                "name": "Auckland International Airport Limited Ordinary Shares",
                                "marketType": "NZSX",
                                "category": "SHRS",
                                "subCategory": None,
                                "type": "SHRS",
                            },
                            {
                                "isin": "NZAGGE0006S8",
                                "code": "AGG",
                                "name": "Smart Global Aggregate Bond ETF Units",
                                "marketType": "NZSX",
                                "category": "SHRS",
                                "subCategory": "ETF",
                                "type": "UNIT",
                            },
                            {
                                "isin": "NZAIAD0240L9",
                                "code": "AIA240",
                                "name": "Auckland Airport bond",
                                "marketType": "NZDX",
                                "category": "RGBD",
                                "subCategory": "CORP",
                                "type": "FRBD",
                            },
                        ]
                    }
                }
            }
        }
    }

    rows = parse_nzx_instruments_next_data_payload(payload, source)

    assert rows == [
        {
            "source_key": "nzx_instruments",
            "provider": "NZX",
            "source_url": "https://new.nzx.com/markets/NZSX",
            "ticker": "AIA",
            "name": "Auckland International Airport Limited Ordinary Shares",
            "exchange": "NZX",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "NZAIAE0002S6",
        },
        {
            "source_key": "nzx_instruments",
            "provider": "NZX",
            "source_url": "https://new.nzx.com/markets/NZSX",
            "ticker": "AGG",
            "name": "Smart Global Aggregate Bond ETF Units",
            "exchange": "NZX",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "official": "true",
            "isin": "NZAGGE0006S8",
            "sector": "Fixed Income",
        },
    ]


def test_nzx_instruments_source_is_modeled_as_official_exchange_directory() -> None:
    source = next(item for item in OFFICIAL_SOURCES if item.key == "nzx_instruments")

    assert source.provider == "NZX"
    assert source.reference_scope == "exchange_directory"
    assert source.format == "nzx_instruments_next_data"


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

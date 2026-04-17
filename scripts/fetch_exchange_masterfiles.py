from __future__ import annotations

import argparse
import csv
import io
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import zipfile
from base64 import b64decode, b64encode
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from html import unescape
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import quote, urljoin
from urllib.request import Request, urlopen

import pandas as pd
import requests
import urllib3

try:
    from scripts.rebuild_dataset import alias_matches_company, ascii_fold, is_valid_isin, normalize_sector, normalize_tokens
except ModuleNotFoundError:  # pragma: no cover - script execution path
    from rebuild_dataset import alias_matches_company, ascii_fold, is_valid_isin, normalize_sector, normalize_tokens


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
MASTERFILES_DIR = DATA_DIR / "masterfiles"
MASTERFILE_REFERENCE_CSV = MASTERFILES_DIR / "reference.csv"
MASTERFILE_SOURCES_JSON = MASTERFILES_DIR / "sources.json"
MASTERFILE_SUMMARY_JSON = MASTERFILES_DIR / "summary.json"
MASTERFILE_CACHE_DIR = MASTERFILES_DIR / "cache"
MASTERFILE_FIELDNAMES = [
    "source_key",
    "provider",
    "source_url",
    "ticker",
    "name",
    "exchange",
    "asset_type",
    "listing_status",
    "reference_scope",
    "official",
    "isin",
    "sector",
]
LISTINGS_CSV = DATA_DIR / "listings.csv"
STOCK_VERIFICATION_DIR = DATA_DIR / "stock_verification"
ETF_VERIFICATION_DIR = DATA_DIR / "etf_verification"
SEC_COMPANY_TICKERS_CACHE = MASTERFILE_CACHE_DIR / "sec_company_tickers_exchange.json"
LEGACY_SEC_COMPANY_TICKERS_CACHE = MASTERFILES_DIR / "sec_company_tickers_exchange.json"
OTC_MARKETS_SECURITY_PROFILE_CACHE = MASTERFILE_CACHE_DIR / "otc_markets_security_profile.json"
LEGACY_OTC_MARKETS_SECURITY_PROFILE_CACHE = MASTERFILES_DIR / "otc_markets_security_profile.json"
OTC_MARKETS_STOCK_SCREENER_CACHE = MASTERFILE_CACHE_DIR / "otc_markets_stock_screener.csv"
LEGACY_OTC_MARKETS_STOCK_SCREENER_CACHE = MASTERFILES_DIR / "otc_markets_stock_screener.csv"
LSE_COMPANY_REPORTS_CACHE = MASTERFILE_CACHE_DIR / "lse_company_reports.json"
LEGACY_LSE_COMPANY_REPORTS_CACHE = MASTERFILES_DIR / "lse_company_reports.json"
LSE_INSTRUMENT_DIRECTORY_CACHE = MASTERFILE_CACHE_DIR / "lse_instrument_directory.json"
LEGACY_LSE_INSTRUMENT_DIRECTORY_CACHE = MASTERFILES_DIR / "lse_instrument_directory.json"
LSE_INSTRUMENT_SEARCH_CACHE = MASTERFILE_CACHE_DIR / "lse_instrument_search.json"
LEGACY_LSE_INSTRUMENT_SEARCH_CACHE = MASTERFILES_DIR / "lse_instrument_search.json"
LSE_PRICE_EXPLORER_CACHE = MASTERFILE_CACHE_DIR / "lse_price_explorer.json"
LEGACY_LSE_PRICE_EXPLORER_CACHE = MASTERFILES_DIR / "lse_price_explorer.json"
TMX_LISTED_ISSUERS_CACHE = MASTERFILE_CACHE_DIR / "tmx_listed_issuers.xlsx"
LEGACY_TMX_LISTED_ISSUERS_CACHE = MASTERFILES_DIR / "tmx_listed_issuers.xlsx"
TMX_ETF_SCREENER_CACHE = MASTERFILE_CACHE_DIR / "tmx_etf_screener.json"
LEGACY_TMX_ETF_SCREENER_CACHE = MASTERFILES_DIR / "tmx_etf_screener.json"
JPX_TSE_STOCK_DETAIL_CACHE = MASTERFILE_CACHE_DIR / "jpx_tse_stock_detail.json"
JPX_TSE_STOCK_DETAIL_PARTIAL_CACHE = MASTERFILE_CACHE_DIR / "jpx_tse_stock_detail.partial.json"
LEGACY_JPX_TSE_STOCK_DETAIL_CACHE = MASTERFILES_DIR / "jpx_tse_stock_detail.json"
TPEX_MAINBOARD_QUOTES_CACHE = MASTERFILE_CACHE_DIR / "tpex_mainboard_daily_close_quotes.json"
LEGACY_TPEX_MAINBOARD_QUOTES_CACHE = MASTERFILES_DIR / "tpex_mainboard_daily_close_quotes.json"
TPEX_ETF_FILTER_CACHE = MASTERFILE_CACHE_DIR / "tpex_etf_filter.json"
LEGACY_TPEX_ETF_FILTER_CACHE = MASTERFILES_DIR / "tpex_etf_filter.json"
TPEX_MAINBOARD_BASIC_INFO_CACHE = MASTERFILE_CACHE_DIR / "tpex_mainboard_basic_info.csv"
LEGACY_TPEX_MAINBOARD_BASIC_INFO_CACHE = MASTERFILES_DIR / "tpex_mainboard_basic_info.csv"
TPEX_EMERGING_BASIC_INFO_CACHE = MASTERFILE_CACHE_DIR / "tpex_emerging_basic_info.csv"
LEGACY_TPEX_EMERGING_BASIC_INFO_CACHE = MASTERFILES_DIR / "tpex_emerging_basic_info.csv"
SZSE_ETF_LIST_CACHE = MASTERFILE_CACHE_DIR / "szse_etf_list.json"
LEGACY_SZSE_ETF_LIST_CACHE = MASTERFILES_DIR / "szse_etf_list.json"
SZSE_B_SHARE_LIST_CACHE = MASTERFILE_CACHE_DIR / "szse_b_share_list.json"
LEGACY_SZSE_B_SHARE_LIST_CACHE = MASTERFILES_DIR / "szse_b_share_list.json"
NASDAQ_NORDIC_STOCKHOLM_SHARES_CACHE = MASTERFILE_CACHE_DIR / "nasdaq_nordic_stockholm_shares.json"
LEGACY_NASDAQ_NORDIC_STOCKHOLM_SHARES_CACHE = MASTERFILES_DIR / "nasdaq_nordic_stockholm_shares.json"
NASDAQ_NORDIC_STOCKHOLM_SHARES_SEARCH_CACHE = MASTERFILE_CACHE_DIR / "nasdaq_nordic_stockholm_shares_search.json"
LEGACY_NASDAQ_NORDIC_STOCKHOLM_SHARES_SEARCH_CACHE = MASTERFILES_DIR / "nasdaq_nordic_stockholm_shares_search.json"
NASDAQ_NORDIC_STOCKHOLM_ETFS_CACHE = MASTERFILE_CACHE_DIR / "nasdaq_nordic_stockholm_etfs.json"
LEGACY_NASDAQ_NORDIC_STOCKHOLM_ETFS_CACHE = MASTERFILES_DIR / "nasdaq_nordic_stockholm_etfs.json"
NASDAQ_NORDIC_STOCKHOLM_TRACKERS_CACHE = MASTERFILE_CACHE_DIR / "nasdaq_nordic_stockholm_trackers.json"
LEGACY_NASDAQ_NORDIC_STOCKHOLM_TRACKERS_CACHE = MASTERFILES_DIR / "nasdaq_nordic_stockholm_trackers.json"
NASDAQ_NORDIC_HELSINKI_SHARES_CACHE = MASTERFILE_CACHE_DIR / "nasdaq_nordic_helsinki_shares.json"
LEGACY_NASDAQ_NORDIC_HELSINKI_SHARES_CACHE = MASTERFILES_DIR / "nasdaq_nordic_helsinki_shares.json"
NASDAQ_NORDIC_HELSINKI_SHARES_SEARCH_CACHE = MASTERFILE_CACHE_DIR / "nasdaq_nordic_helsinki_shares_search.json"
LEGACY_NASDAQ_NORDIC_HELSINKI_SHARES_SEARCH_CACHE = MASTERFILES_DIR / "nasdaq_nordic_helsinki_shares_search.json"
NASDAQ_NORDIC_ICELAND_SHARES_CACHE = MASTERFILE_CACHE_DIR / "nasdaq_nordic_iceland_shares.json"
LEGACY_NASDAQ_NORDIC_ICELAND_SHARES_CACHE = MASTERFILES_DIR / "nasdaq_nordic_iceland_shares.json"
NASDAQ_NORDIC_COPENHAGEN_SHARES_SEARCH_CACHE = MASTERFILE_CACHE_DIR / "nasdaq_nordic_copenhagen_shares_search.json"
LEGACY_NASDAQ_NORDIC_COPENHAGEN_SHARES_SEARCH_CACHE = MASTERFILES_DIR / "nasdaq_nordic_copenhagen_shares_search.json"
NASDAQ_NORDIC_COPENHAGEN_ETF_SEARCH_CACHE = MASTERFILE_CACHE_DIR / "nasdaq_nordic_copenhagen_etf_search.json"
LEGACY_NASDAQ_NORDIC_COPENHAGEN_ETF_SEARCH_CACHE = MASTERFILES_DIR / "nasdaq_nordic_copenhagen_etf_search.json"
SPOTLIGHT_COMPANIES_SEARCH_CACHE = MASTERFILE_CACHE_DIR / "spotlight_companies_search.json"
LEGACY_SPOTLIGHT_COMPANIES_SEARCH_CACHE = MASTERFILES_DIR / "spotlight_companies_search.json"
SPOTLIGHT_COMPANIES_DIRECTORY_CACHE = MASTERFILE_CACHE_DIR / "spotlight_companies_directory.json"
LEGACY_SPOTLIGHT_COMPANIES_DIRECTORY_CACHE = MASTERFILES_DIR / "spotlight_companies_directory.json"
NGM_COMPANIES_PAGE_CACHE = MASTERFILE_CACHE_DIR / "ngm_companies_page.json"
LEGACY_NGM_COMPANIES_PAGE_CACHE = MASTERFILES_DIR / "ngm_companies_page.json"
NGM_MARKET_DATA_EQUITIES_CACHE = MASTERFILE_CACHE_DIR / "ngm_market_data_equities.json"
LEGACY_NGM_MARKET_DATA_EQUITIES_CACHE = MASTERFILES_DIR / "ngm_market_data_equities.json"
NASDAQ_NORDIC_HELSINKI_ETFS_CACHE = MASTERFILE_CACHE_DIR / "nasdaq_nordic_helsinki_etfs.json"
LEGACY_NASDAQ_NORDIC_HELSINKI_ETFS_CACHE = MASTERFILES_DIR / "nasdaq_nordic_helsinki_etfs.json"
NASDAQ_NORDIC_COPENHAGEN_SHARES_CACHE = MASTERFILE_CACHE_DIR / "nasdaq_nordic_copenhagen_shares.json"
LEGACY_NASDAQ_NORDIC_COPENHAGEN_SHARES_CACHE = MASTERFILES_DIR / "nasdaq_nordic_copenhagen_shares.json"
NASDAQ_NORDIC_COPENHAGEN_ETFS_CACHE = MASTERFILE_CACHE_DIR / "nasdaq_nordic_copenhagen_etfs.json"
LEGACY_NASDAQ_NORDIC_COPENHAGEN_ETFS_CACHE = MASTERFILES_DIR / "nasdaq_nordic_copenhagen_etfs.json"
B3_INSTRUMENTS_EQUITIES_CACHE = MASTERFILE_CACHE_DIR / "b3_instruments_equities.json"
LEGACY_B3_INSTRUMENTS_EQUITIES_CACHE = MASTERFILES_DIR / "b3_instruments_equities.json"
JSE_ETF_LIST_CACHE = MASTERFILE_CACHE_DIR / "jse_etf_list.json"
LEGACY_JSE_ETF_LIST_CACHE = MASTERFILES_DIR / "jse_etf_list.json"
JSE_ETN_LIST_CACHE = MASTERFILE_CACHE_DIR / "jse_etn_list.json"
LEGACY_JSE_ETN_LIST_CACHE = MASTERFILES_DIR / "jse_etn_list.json"
JSE_INSTRUMENT_SEARCH_CACHE = MASTERFILE_CACHE_DIR / "jse_instrument_search.json"
LEGACY_JSE_INSTRUMENT_SEARCH_CACHE = MASTERFILES_DIR / "jse_instrument_search.json"
SET_ETF_SEARCH_CACHE = MASTERFILE_CACHE_DIR / "set_etf_search.json"
LEGACY_SET_ETF_SEARCH_CACHE = MASTERFILES_DIR / "set_etf_search.json"
SET_DR_SEARCH_CACHE = MASTERFILE_CACHE_DIR / "set_dr_search.json"
LEGACY_SET_DR_SEARCH_CACHE = MASTERFILES_DIR / "set_dr_search.json"
SET_STOCK_SEARCH_CACHE = MASTERFILE_CACHE_DIR / "set_stock_search.json"
LEGACY_SET_STOCK_SEARCH_CACHE = MASTERFILES_DIR / "set_stock_search.json"
PSX_DPS_SYMBOLS_CACHE = MASTERFILE_CACHE_DIR / "psx_dps_symbols.json"
LEGACY_PSX_DPS_SYMBOLS_CACHE = MASTERFILES_DIR / "psx_dps_symbols.json"
BMV_STOCK_SEARCH_CACHE = MASTERFILE_CACHE_DIR / "bmv_stock_search.json"
LEGACY_BMV_STOCK_SEARCH_CACHE = MASTERFILES_DIR / "bmv_stock_search.json"
BMV_CAPITAL_TRUST_SEARCH_CACHE = MASTERFILE_CACHE_DIR / "bmv_capital_trust_search.json"
LEGACY_BMV_CAPITAL_TRUST_SEARCH_CACHE = MASTERFILES_DIR / "bmv_capital_trust_search.json"
BMV_ETF_SEARCH_CACHE = MASTERFILE_CACHE_DIR / "bmv_etf_search.json"
LEGACY_BMV_ETF_SEARCH_CACHE = MASTERFILES_DIR / "bmv_etf_search.json"
BMV_MARKET_DATA_SECURITIES_CACHE = MASTERFILE_CACHE_DIR / "bmv_market_data_securities.json"
LEGACY_BMV_MARKET_DATA_SECURITIES_CACHE = MASTERFILES_DIR / "bmv_market_data_securities.json"
BMV_ISSUER_DIRECTORY_CACHE = MASTERFILE_CACHE_DIR / "bmv_issuer_directory.json"
LEGACY_BMV_ISSUER_DIRECTORY_CACHE = MASTERFILES_DIR / "bmv_issuer_directory.json"
SIX_SHARE_DETAILS_FQS_CACHE = MASTERFILE_CACHE_DIR / "six_share_details_fqs.json"
LEGACY_SIX_SHARE_DETAILS_FQS_CACHE = MASTERFILES_DIR / "six_share_details_fqs.json"
BME_LISTED_COMPANIES_CACHE = MASTERFILE_CACHE_DIR / "bme_listed_companies.json"
LEGACY_BME_LISTED_COMPANIES_CACHE = MASTERFILES_DIR / "bme_listed_companies.json"
BME_ETF_LIST_CACHE = MASTERFILE_CACHE_DIR / "bme_etf_list.json"
LEGACY_BME_ETF_LIST_CACHE = MASTERFILES_DIR / "bme_etf_list.json"
BME_LISTED_VALUES_CACHE = MASTERFILE_CACHE_DIR / "bme_listed_values.json"
LEGACY_BME_LISTED_VALUES_CACHE = MASTERFILES_DIR / "bme_listed_values.json"
BME_LISTED_VALUES_PDF_CACHE = MASTERFILE_CACHE_DIR / "bme_listed_values.pdf"
LEGACY_BME_LISTED_VALUES_PDF_CACHE = MASTERFILES_DIR / "bme_listed_values.pdf"
BME_SECURITY_PRICES_CACHE = MASTERFILE_CACHE_DIR / "bme_security_prices_directory.json"
LEGACY_BME_SECURITY_PRICES_CACHE = MASTERFILES_DIR / "bme_security_prices_directory.json"
BME_GROWTH_PRICES_CACHE = MASTERFILE_CACHE_DIR / "bme_growth_prices.json"
LEGACY_BME_GROWTH_PRICES_CACHE = MASTERFILES_DIR / "bme_growth_prices.json"
ATHEX_CLASSIFICATION_CACHE = MASTERFILE_CACHE_DIR / "athex_sector_classification.json"
LEGACY_ATHEX_CLASSIFICATION_CACHE = MASTERFILES_DIR / "athex_sector_classification.json"
ATHEX_CLASSIFICATION_PDF_CACHE = MASTERFILE_CACHE_DIR / "athex_sector_classification.pdf"
LEGACY_ATHEX_CLASSIFICATION_PDF_CACHE = MASTERFILES_DIR / "athex_sector_classification.pdf"
BURSA_EQUITY_ISIN_CACHE = MASTERFILE_CACHE_DIR / "bursa_equity_isin.json"
LEGACY_BURSA_EQUITY_ISIN_CACHE = MASTERFILES_DIR / "bursa_equity_isin.json"
BURSA_EQUITY_ISIN_PDF_CACHE = MASTERFILE_CACHE_DIR / "bursa_equity_isin.pdf"
LEGACY_BURSA_EQUITY_ISIN_PDF_CACHE = MASTERFILES_DIR / "bursa_equity_isin.pdf"
BURSA_CLOSING_PRICES_CACHE = MASTERFILE_CACHE_DIR / "bursa_closing_prices.json"
LEGACY_BURSA_CLOSING_PRICES_CACHE = MASTERFILES_DIR / "bursa_closing_prices.json"
BURSA_CLOSING_PRICES_PDF_CACHE = MASTERFILE_CACHE_DIR / "bursa_closing_prices.pdf"
LEGACY_BURSA_CLOSING_PRICES_PDF_CACHE = MASTERFILES_DIR / "bursa_closing_prices.pdf"
BSE_BW_LISTED_COMPANIES_CACHE = MASTERFILE_CACHE_DIR / "bse_bw_listed_companies.json"
LEGACY_BSE_BW_LISTED_COMPANIES_CACHE = MASTERFILES_DIR / "bse_bw_listed_companies.json"
BSE_HU_MARKETDATA_CACHE = MASTERFILE_CACHE_DIR / "bse_hu_marketdata.json"
LEGACY_BSE_HU_MARKETDATA_CACHE = MASTERFILES_DIR / "bse_hu_marketdata.json"
EGX_LISTED_STOCKS_CACHE = MASTERFILE_CACHE_DIR / "egx_listed_stocks.json"
LEGACY_EGX_LISTED_STOCKS_CACHE = MASTERFILES_DIR / "egx_listed_stocks.json"
BVL_ISSUERS_DIRECTORY_CACHE = MASTERFILE_CACHE_DIR / "bvl_issuers_directory.json"
LEGACY_BVL_ISSUERS_DIRECTORY_CACHE = MASTERFILES_DIR / "bvl_issuers_directory.json"
CSE_MA_LISTED_COMPANIES_CACHE = MASTERFILE_CACHE_DIR / "cse_ma_listed_companies.json"
LEGACY_CSE_MA_LISTED_COMPANIES_CACHE = MASTERFILES_DIR / "cse_ma_listed_companies.json"
CSE_LK_ALL_SECURITY_CODE_CACHE = MASTERFILE_CACHE_DIR / "cse_lk_all_security_code.json"
LEGACY_CSE_LK_ALL_SECURITY_CODE_CACHE = MASTERFILES_DIR / "cse_lk_all_security_code.json"
CSE_LK_COMPANY_INFO_SUMMARY_CACHE = MASTERFILE_CACHE_DIR / "cse_lk_company_info_summary.json"
LEGACY_CSE_LK_COMPANY_INFO_SUMMARY_CACHE = MASTERFILES_DIR / "cse_lk_company_info_summary.json"
DSE_TZ_LISTED_COMPANIES_CACHE = MASTERFILE_CACHE_DIR / "dse_tz_listed_companies.json"
LEGACY_DSE_TZ_LISTED_COMPANIES_CACHE = MASTERFILES_DIR / "dse_tz_listed_companies.json"
BVC_RV_ISSUERS_CACHE = MASTERFILE_CACHE_DIR / "bvc_rv_issuers.json"
LEGACY_BVC_RV_ISSUERS_CACHE = MASTERFILES_DIR / "bvc_rv_issuers.json"
BYMA_EQUITY_DETAILS_CACHE = MASTERFILE_CACHE_DIR / "byma_equity_details.json"
LEGACY_BYMA_EQUITY_DETAILS_CACHE = MASTERFILES_DIR / "byma_equity_details.json"
MSE_MW_MAINBOARD_CACHE = MASTERFILE_CACHE_DIR / "mse_mw_mainboard.json"
LEGACY_MSE_MW_MAINBOARD_CACHE = MASTERFILES_DIR / "mse_mw_mainboard.json"
NSE_KE_LISTED_COMPANIES_CACHE = MASTERFILE_CACHE_DIR / "nse_ke_listed_companies.json"
LEGACY_NSE_KE_LISTED_COMPANIES_CACHE = MASTERFILES_DIR / "nse_ke_listed_companies.json"
NSE_IN_SECURITIES_AVAILABLE_CACHE = MASTERFILE_CACHE_DIR / "nse_in_securities_available.json"
LEGACY_NSE_IN_SECURITIES_AVAILABLE_CACHE = MASTERFILES_DIR / "nse_in_securities_available.json"
BSE_IN_SCRIPS_CACHE = MASTERFILE_CACHE_DIR / "bse_in_scrips.json"
LEGACY_BSE_IN_SCRIPS_CACHE = MASTERFILES_DIR / "bse_in_scrips.json"
HKEX_SECURITIES_LIST_CACHE = MASTERFILE_CACHE_DIR / "hkex_securities_list.json"
LEGACY_HKEX_SECURITIES_LIST_CACHE = MASTERFILES_DIR / "hkex_securities_list.json"
SGX_SECURITIES_PRICES_CACHE = MASTERFILE_CACHE_DIR / "sgx_securities_prices.json"
LEGACY_SGX_SECURITIES_PRICES_CACHE = MASTERFILES_DIR / "sgx_securities_prices.json"
DFM_LISTED_SECURITIES_CACHE = MASTERFILE_CACHE_DIR / "dfm_listed_securities.json"
LEGACY_DFM_LISTED_SECURITIES_CACHE = MASTERFILES_DIR / "dfm_listed_securities.json"
BOURSA_KUWAIT_STOCKS_CACHE = MASTERFILE_CACHE_DIR / "boursa_kuwait_stocks.json"
LEGACY_BOURSA_KUWAIT_STOCKS_CACHE = MASTERFILES_DIR / "boursa_kuwait_stocks.json"
BAHRAIN_BOURSE_LISTED_COMPANIES_CACHE = MASTERFILE_CACHE_DIR / "bahrain_bourse_listed_companies.json"
LEGACY_BAHRAIN_BOURSE_LISTED_COMPANIES_CACHE = MASTERFILES_DIR / "bahrain_bourse_listed_companies.json"
BIST_KAP_MKK_LISTED_SECURITIES_CACHE = MASTERFILE_CACHE_DIR / "bist_kap_mkk_listed_securities.json"
LEGACY_BIST_KAP_MKK_LISTED_SECURITIES_CACHE = MASTERFILES_DIR / "bist_kap_mkk_listed_securities.json"
TADAWUL_SECURITIES_CACHE = MASTERFILE_CACHE_DIR / "tadawul_securities.json"
LEGACY_TADAWUL_SECURITIES_CACHE = MASTERFILES_DIR / "tadawul_securities.json"
ADX_MARKET_WATCH_CACHE = MASTERFILE_CACHE_DIR / "adx_market_watch.json"
LEGACY_ADX_MARKET_WATCH_CACHE = MASTERFILES_DIR / "adx_market_watch.json"
QSE_MARKET_WATCH_CACHE = MASTERFILE_CACHE_DIR / "qse_market_watch.json"
LEGACY_QSE_MARKET_WATCH_CACHE = MASTERFILES_DIR / "qse_market_watch.json"
MSX_COMPANIES_CACHE = MASTERFILE_CACHE_DIR / "msx_companies.json"
LEGACY_MSX_COMPANIES_CACHE = MASTERFILES_DIR / "msx_companies.json"
RSE_LISTED_COMPANIES_CACHE = MASTERFILE_CACHE_DIR / "rse_listed_companies.json"
LEGACY_RSE_LISTED_COMPANIES_CACHE = MASTERFILES_DIR / "rse_listed_companies.json"
GSE_LISTED_COMPANIES_CACHE = MASTERFILE_CACHE_DIR / "gse_listed_companies.json"
LEGACY_GSE_LISTED_COMPANIES_CACHE = MASTERFILES_DIR / "gse_listed_companies.json"
LUSE_LISTED_COMPANIES_CACHE = MASTERFILE_CACHE_DIR / "luse_listed_companies.json"
LEGACY_LUSE_LISTED_COMPANIES_CACHE = MASTERFILES_DIR / "luse_listed_companies.json"
BOLSA_SANTIAGO_INSTRUMENTS_CACHE = MASTERFILE_CACHE_DIR / "bolsa_santiago_instruments.json"
LEGACY_BOLSA_SANTIAGO_INSTRUMENTS_CACHE = MASTERFILES_DIR / "bolsa_santiago_instruments.json"
SEM_ISIN_CACHE = MASTERFILE_CACHE_DIR / "sem_isin.json"
LEGACY_SEM_ISIN_CACHE = MASTERFILES_DIR / "sem_isin.json"
USE_UG_MARKET_SNAPSHOT_CACHE = MASTERFILE_CACHE_DIR / "use_ug_market_snapshot.json"
LEGACY_USE_UG_MARKET_SNAPSHOT_CACHE = MASTERFILES_DIR / "use_ug_market_snapshot.json"
NZX_INSTRUMENTS_CACHE = MASTERFILE_CACHE_DIR / "nzx_instruments.json"
LEGACY_NZX_INSTRUMENTS_CACHE = MASTERFILES_DIR / "nzx_instruments.json"
NASDAQ_MUTUAL_FUND_QUOTES_CACHE = MASTERFILE_CACHE_DIR / "nasdaq_mutual_fund_quotes.json"
LEGACY_NASDAQ_MUTUAL_FUND_QUOTES_CACHE = MASTERFILES_DIR / "nasdaq_mutual_fund_quotes.json"
BVB_SHARES_DIRECTORY_CACHE = MASTERFILE_CACHE_DIR / "bvb_shares_directory.json"
LEGACY_BVB_SHARES_DIRECTORY_CACHE = MASTERFILES_DIR / "bvb_shares_directory.json"
ZSE_ZW_ISSUERS_CACHE = MASTERFILE_CACHE_DIR / "zse_zw_issuers.json"
LEGACY_ZSE_ZW_ISSUERS_CACHE = MASTERFILES_DIR / "zse_zw_issuers.json"
BVB_FUND_UNITS_DIRECTORY_CACHE = MASTERFILE_CACHE_DIR / "bvb_fund_units_directory.json"
LEGACY_BVB_FUND_UNITS_DIRECTORY_CACHE = MASTERFILES_DIR / "bvb_fund_units_directory.json"
NGX_EQUITIES_PRICE_LIST_CACHE = MASTERFILE_CACHE_DIR / "ngx_equities_price_list.json"
LEGACY_NGX_EQUITIES_PRICE_LIST_CACHE = MASTERFILES_DIR / "ngx_equities_price_list.json"
NGX_COMPANY_PROFILE_DIRECTORY_CACHE = MASTERFILE_CACHE_DIR / "ngx_company_profile_directory.json"
LEGACY_NGX_COMPANY_PROFILE_DIRECTORY_CACHE = MASTERFILES_DIR / "ngx_company_profile_directory.json"
PSE_LISTED_COMPANY_DIRECTORY_CACHE = MASTERFILE_CACHE_DIR / "pse_listed_company_directory.json"
LEGACY_PSE_LISTED_COMPANY_DIRECTORY_CACHE = MASTERFILES_DIR / "pse_listed_company_directory.json"
PSE_CZ_SHARES_DIRECTORY_CACHE = MASTERFILE_CACHE_DIR / "pse_cz_shares_directory.json"
LEGACY_PSE_CZ_SHARES_DIRECTORY_CACHE = MASTERFILES_DIR / "pse_cz_shares_directory.json"
IDX_LISTED_COMPANIES_CACHE = MASTERFILE_CACHE_DIR / "idx_listed_companies.json"
LEGACY_IDX_LISTED_COMPANIES_CACHE = MASTERFILES_DIR / "idx_listed_companies.json"
IDX_COMPANY_PROFILES_CACHE = MASTERFILE_CACHE_DIR / "idx_company_profiles.json"
LEGACY_IDX_COMPANY_PROFILES_CACHE = MASTERFILES_DIR / "idx_company_profiles.json"
WSE_LISTED_COMPANIES_CACHE = MASTERFILE_CACHE_DIR / "wse_listed_companies.json"
LEGACY_WSE_LISTED_COMPANIES_CACHE = MASTERFILES_DIR / "wse_listed_companies.json"
WSE_ETF_LIST_CACHE = MASTERFILE_CACHE_DIR / "wse_etf_list.json"
LEGACY_WSE_ETF_LIST_CACHE = MASTERFILES_DIR / "wse_etf_list.json"
NEWCONNECT_LISTED_COMPANIES_CACHE = MASTERFILE_CACHE_DIR / "newconnect_listed_companies.json"
LEGACY_NEWCONNECT_LISTED_COMPANIES_CACHE = MASTERFILES_DIR / "newconnect_listed_companies.json"
TASE_SECURITIES_MARKETDATA_CACHE = MASTERFILE_CACHE_DIR / "tase_securities_marketdata.json"
LEGACY_TASE_SECURITIES_MARKETDATA_CACHE = MASTERFILES_DIR / "tase_securities_marketdata.json"
TASE_ETF_MARKETDATA_CACHE = MASTERFILE_CACHE_DIR / "tase_etf_marketdata.json"
LEGACY_TASE_ETF_MARKETDATA_CACHE = MASTERFILES_DIR / "tase_etf_marketdata.json"
TASE_FOREIGN_ETF_SEARCH_CACHE = MASTERFILE_CACHE_DIR / "tase_foreign_etf_search.json"
LEGACY_TASE_FOREIGN_ETF_SEARCH_CACHE = MASTERFILES_DIR / "tase_foreign_etf_search.json"
TASE_PARTICIPATING_UNIT_SEARCH_CACHE = MASTERFILE_CACHE_DIR / "tase_participating_unit_search.json"
LEGACY_TASE_PARTICIPATING_UNIT_SEARCH_CACHE = MASTERFILES_DIR / "tase_participating_unit_search.json"
HOSE_LISTED_STOCKS_CACHE = MASTERFILE_CACHE_DIR / "hose_listed_stocks.json"
LEGACY_HOSE_LISTED_STOCKS_CACHE = MASTERFILES_DIR / "hose_listed_stocks.json"
HOSE_ETF_LIST_CACHE = MASTERFILE_CACHE_DIR / "hose_etf_list.json"
LEGACY_HOSE_ETF_LIST_CACHE = MASTERFILES_DIR / "hose_etf_list.json"
HOSE_FUND_CERTIFICATE_LIST_CACHE = MASTERFILE_CACHE_DIR / "hose_fund_certificate_list.json"
LEGACY_HOSE_FUND_CERTIFICATE_LIST_CACHE = MASTERFILES_DIR / "hose_fund_certificate_list.json"
HNX_LISTED_SECURITIES_CACHE = MASTERFILE_CACHE_DIR / "hnx_listed_securities.json"
LEGACY_HNX_LISTED_SECURITIES_CACHE = MASTERFILES_DIR / "hnx_listed_securities.json"
UPCOM_REGISTERED_SECURITIES_CACHE = MASTERFILE_CACHE_DIR / "upcom_registered_securities.json"
LEGACY_UPCOM_REGISTERED_SECURITIES_CACHE = MASTERFILES_DIR / "upcom_registered_securities.json"
VIENNA_LISTED_COMPANIES_CACHE = MASTERFILE_CACHE_DIR / "vienna_listed_companies.json"
LEGACY_VIENNA_LISTED_COMPANIES_CACHE = MASTERFILES_DIR / "vienna_listed_companies.json"
ZAGREB_SECURITIES_CACHE = MASTERFILE_CACHE_DIR / "zagreb_securities_directory.json"
LEGACY_ZAGREB_SECURITIES_CACHE = MASTERFILES_DIR / "zagreb_securities_directory.json"

SEC_COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers_exchange.json"
OTC_MARKETS_SECURITY_PROFILE_API_URL = "https://backend.otcmarkets.com/otcapi/company/profile/full/{symbol}"
OTC_MARKETS_SECURITY_PROFILE_PAGE_URL = "https://www.otcmarkets.com/stock/{symbol}/security"
OTC_MARKETS_STOCK_SCREENER_PAGE_URL = "https://www.otcmarkets.com/research/stock-screener"
OTC_MARKETS_STOCK_SCREENER_CSV_URL = (
    "https://www.otcmarkets.com/research/stock-screener/api/downloadCSV?"
    "greyAccess=false&expertAccess=false"
)
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
LSE_PRICE_EXPLORER_URL = "https://www.londonstockexchange.com/live-markets/market-data-dashboard/price-explorer"
LSE_PRICE_EXPLORER_API_URL = "https://api.londonstockexchange.com/api/v1/pages"
LSE_PRICE_EXPLORER_PAGE_PATH = "live-markets/market-data-dashboard/price-explorer"
CBOE_CANADA_LISTING_DIRECTORY_URL = "https://www.cboe.com/ca/equities/market-activity/listing-directory/"
CBOE_CANADA_LISTING_DIRECTORY_API_URL = "https://www-api.cboe.com/ca/equities/listing-directory-data/"
ASX_LISTED_URL = "https://www.asx.com.au/asx/research/ASXListedCompanies.csv"
ASX_FUNDS_STATISTICS_URL = "https://www.asx.com.au/issuers/investment-products/asx-funds-statistics"
ASX_GICS_INDUSTRY_GROUP_SECTOR_MAP = {
    "Automobiles & Components": "Consumer Discretionary",
    "Banks": "Financials",
    "Capital Goods": "Industrials",
    "Commercial & Professional Services": "Industrials",
    "Consumer Discretionary Distribution & Retail": "Consumer Discretionary",
    "Consumer Durables & Apparel": "Consumer Discretionary",
    "Consumer Services": "Consumer Discretionary",
    "Consumer Staples Distribution & Retail": "Consumer Staples",
    "Energy": "Energy",
    "Equity Real Estate Investment Trusts (REITs)": "Real Estate",
    "Financial Services": "Financials",
    "Food, Beverage & Tobacco": "Consumer Staples",
    "Health Care Equipment & Services": "Health Care",
    "Household & Personal Products": "Consumer Staples",
    "Insurance": "Financials",
    "Materials": "Materials",
    "Media & Entertainment": "Communication Services",
    "Pharmaceuticals, Biotechnology & Life Sciences": "Health Care",
    "Real Estate Management & Development": "Real Estate",
    "Semiconductors & Semiconductor Equipment": "Information Technology",
    "Software & Services": "Information Technology",
    "Technology Hardware & Equipment": "Information Technology",
    "Telecommunication Services": "Communication Services",
    "Transportation": "Industrials",
    "Utilities": "Utilities",
}
TMX_INTERLISTED_URL = "https://www.tsx.com/files/trading/interlisted-companies.txt"
TMX_LISTED_ISSUERS_ARCHIVE_URL = "https://www.tsx.com/en/listings/current-market-statistics/mig-archives"
TMX_ETF_SCREENER_URL = "https://dgr53wu9i7rmp.cloudfront.net/etfs/etfs.json"
TMX_MONEY_GRAPHQL_URL = "https://app-money.tmx.com/graphql"
TMX_MONEY_GET_ETFS_QUERY = (
    "query { getETFs { "
    "symbol shortname longname fundfamily currency "
    "} }"
)
TMX_QUOTE_EXCHANGE_CODE_TO_EXCHANGE = {
    "TSX": "TSX",
    "CDX": "TSXV",
}
EURONEXT_EQUITIES_DOWNLOAD_URL = "https://live.euronext.com/pd_es/data/stocks/download?mics=dm_all_stock"
EURONEXT_ETFS_DOWNLOAD_URL = (
    "https://live.euronext.com/en/product_directory/data/etf-all-markets/download"
    "?mics=ALXA,ALXB,ALXL,ALXP,ATFX,BGEM,ENXB,ENXL,ETFP,ETLX,EXGM,MERK,MIVX,MLXB,"
    "MOTX,MTAA,MTAH,MTCH,SEDX,TNLA,TNLB,VPXB,WOMF,XACD,XAMC,XAMS,XATL,XBRU,XDUB,"
    "XESM,XLDN,XLIS,XMLI,XMOT,XMSM,XOAM,XOAS,XOBD,XOSL,XPAR,XPMC"
)
JPX_LISTED_ISSUES_URL = "https://www.jpx.co.jp/english/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_e.xls"
JPX_STOCK_SEARCH_URL = "https://quote.jpx.co.jp/jpxhp/main/index.aspx?F=e_stock_search"
JPX_STOCK_DETAIL_PAGE_URL = "https://quote.jpx.co.jp/jpxhp/main/index.aspx?f=e_stock_detail&qcode="
JPX_STOCK_DETAIL_API_URL = "https://quote.jpx.co.jp/jpxhp/jcgi/wrap/qjsonp.aspx?F=ctl/stock_detail&qcode="
JPX_STOCK_DETAIL_WORKERS = 6
JPX_STOCK_DETAIL_TIMEOUT = 5.0
DEUTSCHE_BOERSE_LISTED_URL = "https://www.cashmarket.deutsche-boerse.com/resource/blob/67858/dd766fc6588100c79294324175f95501/data/Listed-companies.xlsx"
DEUTSCHE_BOERSE_ETPS_URL = "https://www.cashmarket.deutsche-boerse.com/resource/blob/1553442/2936716b8f6c2d7a0bb85337485bdcdb/data/Master_DataSheet_Download.xls"
DEUTSCHE_BOERSE_XETRA_ALL_TRADABLE_URL = "https://www.cashmarket.deutsche-boerse.com/resource/blob/1528/b52ea43a2edac92e8283d40645d1c076/data/t7-xetr-allTradableInstruments.csv"
DEUTSCHE_BOERSE_ETP_PRODUCT_TYPE_CATEGORY_MAP = {
    "ETC": "Commodity",
    "ETN": "Alternative",
}
DEUTSCHE_BOERSE_XETRA_INSTRUMENT_TYPE_ASSET_TYPE = {
    "CS": "Stock",
    "ETF": "ETF",
    "ETN": "ETF",
    "ETC": "ETF",
}
SIX_EQUITY_ISSUERS_URL = "https://www.six-group.com/sheldon/equity_issuers/v1/equity_issuers.json"
SIX_SHARE_DETAILS_FQS_URL = "https://www.six-group.com/fqs/ref.json"
SIX_ETF_EXPLORER_URL = "https://www.six-group.com/en/market-data/etf/etf-explorer.html"
SIX_ETP_EXPLORER_URL = "https://www.six-group.com/en/market-data/etp/etp-explorer.html"
B3_INSTRUMENTS_EQUITIES_URL = "https://arquivos.b3.com.br/bdi/table/InstrumentsEquities"
B3_FUNDS_LISTED_PROXY_URL = "https://sistemaswebb3-listados.b3.com.br/fundsListedProxy/Search/"
B3_BDR_COMPANIES_PROXY_URL = "https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/"
JSE_EXCHANGE_TRADED_PRODUCTS_URL = "https://www.jse.co.za/trade/equities-market/exchange-traded-products"
JSE_SEARCH_URL = "https://www.jse.co.za/search"
JSE_EXCHANGE_TRADED_PRODUCT_CATEGORY_MAP = {
    "Top 40 Equity": "Equity",
    "Other Local Equity": "Equity",
    "International Equity": "Equity",
    "Commodities": "Commodity",
    "Local Bonds": "Fixed Income",
    "International Bonds": "Fixed Income",
    "Money Market": "Money Market",
    "Multi-Asset Class": "Multi-Asset",
    "Local Property": "Real Estate",
    "International Property": "Real Estate",
    "Equity": "Equity",
    "Commodity": "Commodity",
    "Currency": "Currency",
    "Interest Rate": "Fixed Income",
}
NASDAQ_NORDIC_API_ROOT_URL = "https://api.nasdaq.com/api/nordic/"
NASDAQ_NORDIC_SHARES_SCREENER_URL = "https://api.nasdaq.com/api/nordic/screener/shares"
NASDAQ_NORDIC_ETP_SCREENER_URL = "https://api.nasdaq.com/api/nordic/screener/etp"
NASDAQ_NORDIC_SEARCH_URL = "https://api.nasdaq.com/api/nordic/search"
NASDAQ_NORDIC_ETF_PAGE_URL = "https://www.nasdaq.com/european-market-activity/etf"
NASDAQ_NORDIC_STOCK_PAGE_URL = "https://www.nasdaq.com/european-market-activity/stocks"
NASDAQ_NORDIC_STOCKHOLM_TRACKER_SEARCH_TERMS = ("XBT Provider",)
NASDAQ_NORDIC_SHARES_SOURCE_CONFIG = {
    "nasdaq_nordic_stockholm_shares": {
        "exchange": "STO",
        "market": "STO",
        "categories": ("MAIN_MARKET", "FIRST_NORTH"),
    },
    "nasdaq_nordic_helsinki_shares": {
        "exchange": "HEL",
        "market": "HEL",
        "categories": ("MAIN_MARKET", "FIRST_NORTH"),
    },
    "nasdaq_nordic_copenhagen_shares": {
        "exchange": "CPH",
        "market": "CPH",
        "categories": ("MAIN_MARKET", "FIRST_NORTH"),
    },
    "nasdaq_nordic_iceland_shares": {
        "exchange": "ICE_IS",
        "market": "ICE",
        "categories": ("MAIN_MARKET", "FIRST_NORTH"),
    },
}
NASDAQ_NORDIC_ETF_SOURCE_CONFIG = {
    "nasdaq_nordic_stockholm_etfs": {"exchange": "STO", "market": "STO"},
    "nasdaq_nordic_helsinki_etfs": {"exchange": "HEL", "market": "HEL"},
    "nasdaq_nordic_copenhagen_etfs": {"exchange": "CPH", "market": "CPH"},
}
NASDAQ_NORDIC_SHARE_SEARCH_SOURCE_CONFIG = {
    "nasdaq_nordic_stockholm_shares_search": {"exchange": "STO", "currency": "SEK"},
    "nasdaq_nordic_helsinki_shares_search": {"exchange": "HEL", "currency": "EUR"},
    "nasdaq_nordic_copenhagen_shares_search": {"exchange": "CPH", "currency": "DKK"},
}
NASDAQ_NORDIC_ETF_SEARCH_SOURCE_CONFIG = {
    "nasdaq_nordic_copenhagen_etf_search": {"exchange": "CPH", "currency": "DKK"},
}
SPOTLIGHT_SEARCH_SOURCE_CONFIG = {
    "spotlight_companies_search": {"exchange": "STO"},
}
BMV_SEARCH_SOURCE_CONFIG = {
    "bmv_stock_search": {"exchange": "BMV", "quote_search_id": 2, "asset_type": "Stock"},
    "bmv_capital_trust_search": {"exchange": "BMV", "quote_search_id": 2, "asset_type": "Stock"},
    "bmv_etf_search": {"exchange": "BMV", "quote_search_id": 2, "asset_type": "ETF"},
}
BME_LISTED_SOURCE_CONFIG = {
    "bme_listed_companies": {"exchange": "BME", "trading_system": "SIBE", "asset_type": "Stock"},
    "bme_etf_list": {"exchange": "BME", "trading_system": "ETF", "asset_type": "ETF"},
}
HOSE_SECURITIES_SOURCE_CONFIG = {
    "hose_listed_stocks": {"endpoint_type": "stock", "asset_type": "Stock"},
    "hose_etf_list": {"endpoint_type": "etf", "asset_type": "ETF"},
    "hose_fund_certificate_list": {"endpoint_type": "fundcertificate", "asset_type": "Stock"},
}
HNX_SECURITIES_SOURCE_CONFIG = {
    "hnx_listed_securities": {
        "exchange": "HNX",
        "page_url": "https://www.hnx.vn/cophieu-etfs/chung-khoan-ny.html",
        "endpoint_url": "https://www.hnx.vn/ModuleIssuer/List/ListSearch_Datas",
        "market_code": "",
    },
    "upcom_registered_securities": {
        "exchange": "UPCOM",
        "page_url": "https://www.hnx.vn/cophieu-etfs/chung-khoan-uc.html",
        "endpoint_url": "https://www.hnx.vn/ModuleIssuer/UC_Issuer/ListSearch_Datas",
        "market_code": "UC",
    },
}
TWSE_LISTED_COMPANIES_URL = "https://openapi.twse.com.tw/v1/opendata/t187ap03_L"
TWSE_ETF_LIST_URL = "https://www.twse.com.tw/rwd/en/ETF/list"
SET_LISTED_COMPANIES_URL = "https://www.set.or.th/dat/eod/listedcompany/static/listedCompanies_en_US.xls"
SET_STOCK_SEARCH_URL = "https://www.set.or.th/en/market/product/stock/search"
SET_STOCK_LIST_API_URL = "https://www.set.or.th/api/set/stock/list"
SET_ETF_SEARCH_URL = "https://www.set.or.th/en/market/get-quote/etf"
SET_DR_SEARCH_URL = "https://www.set.or.th/en/market/get-quote/dr"
BMV_MOBILE_QUOTE_KEYS_URL = "https://www.bmv.com.mx/es/movil/JSONClaveCotizacion"
BMV_SEARCH_TOKEN_URL = "https://www.bmv.com.mx/rest/tokenservice/token"
BMV_SEARCH_URL = "https://www.bmv.com.mx/api/searchservice/v1"
BMV_MARKET_DATA_SECURITIES_URL = "https://www.bmv.com.mx/es/emisoras/estadisticas"
BMV_MARKET_DATA_PROFILE_URL_TEMPLATE = "https://www.bmv.com.mx/es/emisoras/perfil/{issuer}-{issuer_id}"
BMV_MARKET_DATA_STATS_URL_TEMPLATE = "https://www.bmv.com.mx/es/emisoras/estadisticas/{issuer}-{issuer_id}"
BMV_ISSUER_DIRECTORY_URL = "https://staging.bmv.com.mx/es/emisoras/informacion-de-emisoras"
BMV_ISSUER_DIRECTORY_SEARCH_URL = (
    "https://staging.bmv.com.mx/es/Grupo_BMV/Informacion_de_emisora/_rid/541/_mto/3/_mod/doSearch"
)
BME_MARKET_API_ROOT_URL = "https://apiweb.bolsasymercados.es/Market/"
BME_COMPANIES_SEARCH_PAGE_URL = "https://www.bolsasymercados.es/en/bme-exchange/companies-search.html"
BME_LISTED_COMPANIES_API_URL = f"{BME_MARKET_API_ROOT_URL}v1/EQ/ListedCompanies"
BME_SHARE_DETAILS_INFO_API_URL = f"{BME_MARKET_API_ROOT_URL}v1/EQ/ShareDetailsInfo"
BME_SECURITY_PRICES_PAGE_URL = "https://www.bolsasymercados.es/en/bme-exchange/companies-search.html"
BME_SECURITY_PRICES_TRADING_SYSTEMS = "SIBE,Floor,Latibex,MTF,ETF"
BME_SECURITY_PRICES_MIN_CACHE_ROWS = 500
BME_LISTED_VALUES_PDF_URL = (
    "https://www.bolsasymercados.es/dam/descargas/exchange/renta-variable/"
    "listado-valores-renta-variable-es-en.pdf"
)
BME_GROWTH_PRICES_URL = "https://www.bmegrowth.es/ing/Precios.aspx"
BME_SECTOR_TO_CANONICAL_STOCK_SECTOR = {
    "01": "Energy",
    "02": "Industrials",
    "03": "Consumer Discretionary",
    "04": "Consumer Discretionary",
    "05": "Financials",
    "06": "Information Technology",
    "07": "Real Estate",
}
BME_SUBSECTOR_TO_CANONICAL_STOCK_SECTOR = {
    ("01", "02"): "Utilities",
    ("01", "03"): "Utilities",
    ("01", "04"): "Utilities",
    ("02", "01"): "Materials",
    ("02", "04"): "Materials",
    ("02", "05"): "Materials",
    ("03", "01"): "Consumer Staples",
    ("03", "03"): "Materials",
    ("03", "05"): "Health Care",
    ("04", "03"): "Communication Services",
    ("04", "04"): "Industrials",
    ("04", "05"): "Industrials",
    ("04", "06"): "Industrials",
    ("06", "01"): "Communication Services",
}
BME_TRADING_SYSTEM_TO_ASSET_TYPE = {
    "ETF": "ETF",
    "FLOOR": "Stock",
    "LATIBEX": "Stock",
    "MTF": "Stock",
    "SIBE": "Stock",
}
ATHEX_CLASSIFICATION_PDF_URL = (
    "https://www.athexgroup.gr/documents/10180/5517704/"
    "ATHEX%2BClassification%2BVer0.810%2B%28EN%29.pdf/5c49a27d-1078-4974-990b-5a5e1f278b73"
)
BURSA_EQUITY_ISIN_PAGE_URL = "https://www.bursamalaysia.com/trade/trading_resources/equities/isin"
BURSA_CLOSING_PRICES_PDF_URL = (
    "https://www.bursamalaysia.com/misc/missftp/securities/"
    "securities_equities_year_end_closing_prices_20250702.pdf?t=1751516082"
)
BSE_BW_LISTED_COMPANIES_URL = "https://www.bse.co.bw/companies/"
BSE_HU_BETA_MARKET_URL = "https://www.bet.hu/oldalak/beta_piac#reszvenyek"
EGX_LISTED_STOCKS_URL = "https://www.egx.com.eg/en/ListedStocks.aspx"
CAVALI_BVL_EMISORES_URL = "https://cavali-corporativa.screativa.com/emisores"
CASABLANCA_BOURSE_INSTRUMENTS_URL = "https://www.casablanca-bourse.com/en/marche-cash/instruments-actions"
CSE_LK_ALL_SECURITY_CODE_URL = "https://www.cse.lk/api/allSecurityCode"
CSE_LK_COMPANY_INFO_SUMMARY_URL = "https://www.cse.lk/api/companyInfoSummery"
CSE_LK_COMPANY_INFO_WORKERS = 8
DSE_TZ_LISTED_COMPANIES_URL = "https://www.dse.co.tz/listed/company/list"
BVC_HANDSHAKE_URL = "https://www.bvc.com.co/api/handshake"
BVC_RV_ISSUERS_URL = (
    "https://rest.bvc.com.co/market-information/rv/lvl-3/issuer?"
    "filters[marketDataRv][type]=Local"
)
BVC_RV_ISSUERS_FILTERS = "filters[marketDataRv][type]=Local"
BVC_RV_ISSUERS_PAGE_URL = "https://www.bvc.com.co/list-of-issuers-local-market"
BYMA_API_BASE_URL = "https://open.bymadata.com.ar/vanoms-be-core/rest/api"
BYMA_EQUITY_DETAIL_URL = f"{BYMA_API_BASE_URL}/bymadata/free/bnown/fichatecnica/especies/general"
BYMA_HEADER_SEARCH_URL = f"{BYMA_API_BASE_URL}/bymadata/free/instruments-for-header-search"
BYMA_EQUITY_DETAILS_PAGE_URL = "https://open.bymadata.com.ar/#/issuers-negociable-securities-information"
MSE_MW_MARKET_MAINBOARD_URL = "https://mse.co.mw/market/mainboard"
NSE_KE_LISTED_COMPANIES_URL = "https://www.nse.co.ke/listed-companies/"
NSE_KE_LISTED_COMPANIES_API_URL = "https://www.nse.co.ke/wp-json/wp/v2/pages?slug=listed-companies"
NSE_IN_SECURITIES_AVAILABLE_PAGE_URL = "https://www.nseindia.com/static/market-data/securities-available-for-trading"
NSE_IN_EQUITY_LIST_URL = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"
NSE_IN_SME_EQUITY_LIST_URL = "https://nsearchives.nseindia.com/emerge/corporates/content/SME_EQUITY_L.csv"
NSE_IN_ETF_LIST_URL = "https://nsearchives.nseindia.com/content/equities/eq_etfseclist.csv"
BSE_IN_SCRIPS_PAGE_URL = "https://www.bseindia.com/corporates/List_Scrips.html"
BSE_IN_SCRIPS_API_URL = "https://api.bseindia.com/BseIndiaAPI/api/ListofScripData/w"
HKEX_SECURITIES_LIST_URL = "https://www.hkex.com.hk/eng/services/trading/securities/securitieslists/ListOfSecurities.xlsx"
HKEX_SECURITIES_PRICES_PAGE_URL = "https://www.hkex.com.hk/Market-Data/Securities-Prices/Equities"
SGX_SECURITIES_PRICES_PAGE_URL = "https://www.sgx.com/securities/securities-prices"
SGX_SECURITIES_PRICES_API_URL = "https://api.sgx.com/securities/v1.1"
DFM_LISTED_SECURITIES_PAGE_URL = "https://www.dfm.ae/issuers/listed-securities/company-profile-page"
DFM_STOCKS_API_URL = "https://api2.dfm.ae/mw/v1/stocks"
DFM_COMPANY_PROFILE_API_URL = "https://api2.dfm.ae/web/widgets/v1/data"
BOURSA_KUWAIT_STOCKS_PAGE_URL = "https://www.boursakuwait.com.kw/en/securities/main-market/overview---main-market/"
BOURSA_KUWAIT_LEGACY_MIX_API_URL = "https://www.boursakuwait.com.kw/data-api/legacy-mix-services"
BAHRAIN_BOURSE_HOME_URL = "https://bahrainbourse.com/en"
BAHRAIN_BOURSE_ISIN_CODES_URL = "https://bahrainbourse.com/en/Bahrain%20Clear/ISINCodes"
BIST_LISTED_COMPANIES_PAGE_URL = "https://www.kap.org.tr/tr/bist-sirketler"
BIST_MKK_SECURITIES_PAGE_URL = "https://www.mkk.com.tr/en/depository-services/information-center/list-securities"
BIST_MKK_SECURITIES_API_URL = "https://www.mkk.com.tr/api/getSecurities"
TADAWUL_ISSUERS_DIRECTORY_URL = "https://www.saudiexchange.sa/wps/portal/tadawul/market-participants/issuers/issuers-directory"
TADAWUL_THEME_SEARCH_URL = "https://www.saudiexchange.sa/tadawul.eportal.theme.helper/ThemeSearchUtilityServlet"
ADX_MARKET_WATCH_PAGE_URL = "https://www.adx.ae/english/pages/marketwatch.aspx"
ADX_API_BASE_URL = "https://apigateway.adx.ae"
ADX_GATEWAY_API_KEY = "1863a94c-582b-46f9-b4f0-0d02c0cc5307"
ADX_ISSUERS_API_URL = f"{ADX_API_BASE_URL}/adx/tradings/1.1/issuers"
ADX_MAIN_MARKET_API_URL = f"{ADX_API_BASE_URL}/adx/marketwatch/1.1/securityBoards/mainMarket"
ADX_GROWTH_MARKET_API_URL = f"{ADX_API_BASE_URL}/adx/marketwatch/1.1/securityBoards/growthMarket"
ADX_ETF_API_URL = f"{ADX_API_BASE_URL}/adx/marketwatch/1.1/securityBoards/etf"
ADX_SYMBOL_SECTOR_API_URL = f"{ADX_API_BASE_URL}/adx/lookups/1.0/data/symbol-sector"
QSE_LISTED_COMPANIES_URL = "https://www.qe.com.qa/listed-companies"
QSE_MARKET_WATCH_URL = "https://www.qe.com.qa/wp/mw/data/MarketWatch.txt"
MSX_COMPANIES_URL = "https://www.msx.om/companies.aspx"
MSX_COMPANIES_LIST_URL = "https://www.msx.om/companies.aspx/List"
RSE_LISTED_COMPANIES_URL = "https://www.rse.rw/listed-compagnies"
GSE_LISTED_COMPANIES_URL = "https://gse.com.gh/listed-companies/"
LUSE_LISTED_COMPANIES_URL = "https://www.luse.co.zm/listed-companies/"
BOLSA_SANTIAGO_INSTRUMENTS_PAGE_URL = "https://www.bolsadesantiago.com/informacion-instrumentos"
BOLSA_SANTIAGO_INSTRUMENTS_URL = "https://www.bolsadesantiago.com/api/RV_ResumenMercado/getInstrumentos"
SEM_ISIN_PAGE_URL = "https://www.stockexchangeofmauritius.com/cds/isins"
SEM_ISIN_XLSX_URL = "https://www.stockexchangeofmauritius.com/media/11318/isin.xlsx"
USE_UG_MARKET_SNAPSHOT_URL = "https://www.use.or.ug/market-statistics/market-snapshot"
NZX_INSTRUMENTS_URL = "https://new.nzx.com/markets/NZSX"
NASDAQ_MUTUAL_FUND_SCREENER_URL = "https://api.nasdaq.com/api/screener/mutualfunds"
NASDAQ_MUTUAL_FUND_QUOTE_URL_TEMPLATE = "https://api.nasdaq.com/api/quote/{ticker}/info?assetclass=mutualfunds"
NASDAQ_MUTUAL_FUND_PAGE_URL = "https://www.nasdaq.com/market-activity/mutual-funds"
ZSE_ZW_LISTED_EQUITIES_URL = "https://www.zse.co.zw/listed-securities/equity"
ZSE_ZW_API_ROOT_URL = "https://ds88jcmqc11je.cloudfront.net"
ZSE_ZW_ISSUERS_URL = f"{ZSE_ZW_API_ROOT_URL}/api/issuers"
ZSE_ZW_PRICE_SHEET_URL = f"{ZSE_ZW_API_ROOT_URL}/api/fetch/price-sheet?exchange=ZSE"
BVB_SHARES_DIRECTORY_URL = "https://m.bvb.ro/FinancialInstruments/Markets/Shares"
BVB_FUND_UNITS_DIRECTORY_URL = "https://m.bvb.ro/FinancialInstruments/Markets/FundUnits"
NGX_EQUITIES_PRICE_LIST_PAGE_URL = "https://ngxgroup.com/exchange/data/equities-price-list/"
NGX_EQUITIES_PRICE_LIST_URL = (
    "https://doclib.ngxgroup.com/REST/api/statistics/equities/"
    "?market=&sector=&orderby=&pageSize=300&pageNo=0"
)
NGX_COMPANY_PROFILE_DIRECTORY_URL = "https://ngxgroup.com/exchange/data/company-profile/"
SZSE_STOCK_LIST_URL = "https://www.szse.cn/market/product/stock/list/index.html"
SZSE_ETF_LIST_URL = "https://www.szse.cn/market/product/list/etfList/index.html"
SZSE_REPORT_DATA_URL = "https://www.szse.cn/api/report/ShowReport/data"
SZSE_A_SHARE_CATALOG_ID = "1110"
SZSE_A_SHARE_TAB_KEY = "tab1"
SZSE_B_SHARE_TAB_KEY = "tab2"
SZSE_ETF_CATALOG_ID = "1945"
SZSE_ETF_TAB_KEY = "tab1"
SSE_STOCK_LIST_URL = "https://www.sse.com.cn/assortment/stock/list/share/"
SSE_ETF_LIST_URL = "https://www.sse.com.cn/assortment/fund/etf/list/"
SSE_COMMON_QUERY_URL = "https://query.sse.com.cn/sseQuery/commonQuery.do"
SSE_JSONP_CALLBACK = "jsonpCallback"
TPEX_MAINBOARD_QUOTES_URL = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes"
TASE_MARKET_SECURITIES_PAGE_URL = "https://market.tase.co.il/en/market_data/securities/data/all"
TASE_API_ROOT_URL = "https://api.tase.co.il/api/"
TASE_SECURITIES_MARKETDATA_URL = f"{TASE_API_ROOT_URL}security/securitiesmarketdata"
TASE_ETF_MARKETDATA_URL = f"{TASE_API_ROOT_URL}marketdata/etfs"
TASE_SEARCH_ENTITIES_URL = f"{TASE_API_ROOT_URL}content/searchentities?lang=1"
HOSE_SECURITIES_API_ROOT_URL = "https://api.hsx.vn/l/api/v1/2"
HOSE_LISTED_STOCKS_URL = f"{HOSE_SECURITIES_API_ROOT_URL}/securities/stock"
HOSE_ETF_LIST_URL = f"{HOSE_SECURITIES_API_ROOT_URL}/securities/etf"
HOSE_FUND_CERTIFICATE_LIST_URL = f"{HOSE_SECURITIES_API_ROOT_URL}/securities/fundcertificate"
HNX_LISTED_SECURITIES_PAGE_URL = HNX_SECURITIES_SOURCE_CONFIG["hnx_listed_securities"]["page_url"]
HNX_LISTED_SECURITIES_URL = HNX_SECURITIES_SOURCE_CONFIG["hnx_listed_securities"]["endpoint_url"]
UPCOM_REGISTERED_SECURITIES_PAGE_URL = HNX_SECURITIES_SOURCE_CONFIG["upcom_registered_securities"]["page_url"]
UPCOM_REGISTERED_SECURITIES_URL = HNX_SECURITIES_SOURCE_CONFIG["upcom_registered_securities"]["endpoint_url"]
VSDC_BASE_URL = "https://vsd.vn"
VSDC_TOKEN_SEED_URL = f"{VSDC_BASE_URL}/en/ad/151482"
VSDC_SEARCH_SUGGEST_URL = f"{VSDC_BASE_URL}/search-suggest"
TPEX_ETF_FILTER_PAGE_URL = "https://info.tpex.org.tw/ETF/en/filter.html"
TPEX_ETF_FILTER_API_URL = "https://info.tpex.org.tw/api/etfFilter"
TPEX_MAINBOARD_BASIC_INFO_URL = "https://mopsfin.twse.com.tw/opendata/t187ap03_O.csv"
TPEX_EMERGING_BASIC_INFO_URL = "https://mopsfin.twse.com.tw/opendata/t187ap03_R.csv"
SPOTLIGHT_COMPANIES_PAGE_URL = "https://spotlightstockmarket.com/en/market-overview/our-companies/"
SPOTLIGHT_COMPANY_SEARCH_URL = "https://spotlightstockmarket.com/Umbraco/api/companyapi/CompanySimpleSearch"
SPOTLIGHT_COMPANIES_DIRECTORY_URL = "https://spotlightstockmarket.com/Umbraco/api/companyapi/GetCompanies"
NGM_COMPANIES_PAGE_URL = "https://www.ngm.se/en/our-companies/"
NGM_MARKET_DATA_API_ROOT_URL = "https://mdapi.ngm.se/api/beta/web/"
NGM_DETAILVIEW_SERVICE_URL = "https://mdweb.ngm.se/MDWebFront/detailview/service"
NGM_DETAILVIEW_MODULE_BASE_URL = "https://mdweb.ngm.se/MDWebFront/detailview/"
NGM_DETAILVIEW_PERMUTATION = "E66E1EEBE9B1CEBA056C88011CCBF307"
NGM_DETAILVIEW_STRONG_NAME = "9CB8691CD75E82471063725CB524FFF9"
KRX_LISTED_COMPANIES_URL = "https://global.krx.co.kr/contents/GLB/03/0308/0308010000/GLB0308010000.jsp"
KRX_DATA_URL = "https://global.krx.co.kr/contents/GLB/99/GLB99000001.jspx"
KRX_GENERATE_OTP_URL = "https://global.krx.co.kr/contents/COM/GenerateOTP.jspx"
KRX_JSON_DATA_URL = "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
PSX_LISTED_COMPANIES_URL = "https://www.psx.com.pk/psx/resources-and-tools/listings/listed-companies"
PSX_COMPANIES_BY_SECTOR_URL = "https://www.psx.com.pk/psx/custom-templates/companiesSearch-sector"
PSX_DAILY_DOWNLOADS_URL = "https://dps.psx.com.pk/daily-downloads"
PSX_DPS_SYMBOLS_URL = "https://dps.psx.com.pk/symbols"
PSE_LISTED_COMPANY_DIRECTORY_PAGE_URL = "https://www.pse.com.ph/listed-company-directory/"
PSE_LISTED_COMPANY_DIRECTORY_URL = "https://frames.pse.com.ph/listedCompany"
PSE_CZ_SHARES_URL = "https://www.pse.cz/en/market-data/shares/prime-market"
PSE_CZ_SHARES_MARKET_URLS = (
    "https://www.pse.cz/en/market-data/shares/prime-market",
    "https://www.pse.cz/en/market-data/shares/standard-market",
    "https://www.pse.cz/en/market-data/shares/free-market",
    "https://www.pse.cz/en/market-data/shares/start-market",
)
IDX_STOCK_LIST_PAGE_URL = "https://www.idx.id/en/market-data/stocks-data/stock-list"
IDX_PRIMARY_API_ROOT_URL = "https://www.idx.id/primary"
IDX_LISTED_COMPANIES_URL = f"{IDX_PRIMARY_API_ROOT_URL}/StockData/GetSecuritiesStock"
IDX_COMPANY_PROFILES_URL = f"{IDX_PRIMARY_API_ROOT_URL}/ListedCompany/GetCompanyProfiles"
WSE_COMPANIES_PAGE_URL = "https://www.gpw.pl/spolki"
WSE_ETF_PAGE_URL = "https://www.gpw.pl/etfy"
WSE_AJAX_URL = "https://www.gpw.pl/ajaxindex.php"
NEWCONNECT_COMPANIES_PAGE_URL = "https://newconnect.pl/spolki"
NEWCONNECT_AJAX_URL = "https://newconnect.pl/ajaxindex.php"
VIENNA_LISTED_COMPANIES_URL = "https://www.wienerborse.at/en/listing/shares/companies-list/"
ZAGREB_SECURITIES_URL = "https://zse.hr/en/shares/68"

USER_AGENT = "free-ticker-database/2.0 (+https://github.com/adanos-software/free-ticker-database)"
BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)
SEC_CONTACT_EMAIL = os.environ.get("SEC_CONTACT_EMAIL", "opensource@adanos.software")
REQUEST_TIMEOUT = 30.0
SPOTLIGHT_DETAIL_TIMEOUT = 5.0
PSE_SECURITY_TYPE_TO_ASSET_TYPE = {"C": "Stock", "P": "Stock", "E": "ETF"}
PSE_ACTIVE_SECURITY_STATUSES = {"T", "V"}
BMV_CAPITAL_TRUST_DESCRIPTIONS = {
    "FIBRAS CERTIFICADOS INMOBILIARIOS",
    "FIDEICOMISOS DE INVERSION EN ENERGIA",
    "FIDEICOMISOS DE INVERSION EN INFRAESTRUCTURA",
    "TRACS",
}
BMV_ETF_DESCRIPTION_MARKERS = (
    "TRACS",
    "CANASTAS DE ACCIONES",
)
BMV_STOCK_SECTOR_MAP = {
    "BIENES RAICES": "Real Estate",
    "ENERGIA": "Energy",
    "INDUSTRIAL": "Industrials",
    "MATERIALES": "Materials",
    "PRODUCTOS DE CONSUMO FRECUENTE": "Consumer Staples",
    "PRODUCTOS DE CONSUMO NO BASICO": "Consumer Discretionary",
    "SALUD": "Health Care",
    "SERVICIOS Y BIENES DE CONSUMO NO BASICO": "Consumer Discretionary",
    "SERVICIOS DE TELECOMUNICACIONES": "Communication Services",
    "SERVICIOS FINANCIEROS": "Financials",
    "SERVICIOS PUBLICOS": "Utilities",
    "TECNOLOGIA DE LA INFORMACION": "Information Technology",
}
BURSA_STOCK_ISSUE_DESCRIPTIONS = {
    "ORDINARY SHARE",
    "ORDINARY SHARE FOREIGN",
    "REITS",
    "CLOSE END FUND",
}
BURSA_ETF_ISSUE_DESCRIPTIONS = {"ETF"}
BURSA_EQUITY_ISIN_PDF_HREF_RE = re.compile(
    r"href=[\"'](?P<href>[^\"']*isinequity[^\"']*\.pdf)[\"']",
    re.I,
)
BURSA_EQUITY_ISIN_PDF_URL_RE = re.compile(
    r"https://www\.bursamalaysia\.com/sites/[^\"'<>]+/isinequity[^\"'<>]+\.pdf",
    re.I,
)
BURSA_EQUITY_ISIN_4_DIGIT_RE = re.compile(r"^MY[QL](?P<ticker>\d{4})OO[0-9A-Z]{3}$")
BURSA_EQUITY_ISIN_5_DIGIT_RE = re.compile(r"^MYL(?P<ticker>\d{5})O[0-9A-Z]{3}$")
BURSA_REIT_ISIN_RE = re.compile(r"^MYL(?P<ticker>\d{4})TO[0-9A-Z]{3}$")
BURSA_STAPLED_REIT_ISIN_RE = re.compile(r"^MYL(?P<ticker>\d{4}SS)[0-9A-Z]{3}$")
BURSA_CLOSED_END_FUND_ISIN_RE = re.compile(r"^MYL(?P<ticker>\d{4})FO[0-9A-Z]{3}$")
BURSA_ETF_ISIN_RE = re.compile(r"^MYL(?P<ticker>\d{4}E[A-Z])[0-9A-Z]{3}$")
BURSA_CLOSING_PRICES_STOCK_BOARDS = {"MAIN MARKET", "ACE MARKET", "LEAP MARKET"}
BURSA_CLOSING_PRICES_STOCK_SECTOR_MAP = {
    "CONSTRUCTION": "Industrials",
    "CONSUMER PRODUCTS & SERVICES": "Consumer Discretionary",
    "ENERGY": "Energy",
    "FINANCIAL SERVICES": "Financials",
    "HEALTH CARE": "Health Care",
    "INDUSTRIAL PRODUCTS & SERVICES": "Industrials",
    "PLANTATION": "Consumer Staples",
    "PROPERTY": "Real Estate",
    "REITS": "Real Estate",
    "TECHNOLOGY": "Information Technology",
    "TELECOMMUNICATIONS & MEDIA": "Communication Services",
    "TRANSPORTATION & LOGISTICS": "Industrials",
    "UTILITIES": "Utilities",
}
BURSA_CLOSING_PRICES_ETF_CATEGORY_MAP = {
    "EXCHANGE TRADED FUND-EQUITY": "Equity",
    "EXCHANGE TRADED FUND-FIXED INCOME": "Fixed Income",
}
BMV_ISSUER_DIRECTORY_QUERY_COMBINATIONS = (
    ("CGEN_CAPIT", "CGEN_ELAC"),
    ("CGEN_CAPIT", "CGEN_ELTRA"),
    ("CGEN_CAPIT", "CGEN_ELFI"),
    ("CGEN_CAPIT", "CGEN_ELFH"),
    ("CGEN_CAPIT", "CGEN_ELFII"),
    ("CGEN_CAPIT", "CGEN_FINMB"),
    ("CGEN_GLOB", "CGEN_ELGA"),
    ("CGEN_GLOB", "CGEN_ELGE"),
)
TMX_EXACT_QUOTE_ACCEPTED_NAMES = {
    ("TSX", "FFI-UN"): {
        "Flaherty & Crumrine Investment Grade Fixed Income Fund",
    },
}
LSE_UPDATE_OPENER_RE = re.compile(r"UpdateOpener\('(?P<name>(?:\\'|[^'])*)',\s*'(?P<meta>[^']*)'\)")
CBOE_CANADA_LISTING_DIRECTORY_RE = re.compile(r"CTX\['listingDirectory'\]\s*=\s*(\[[\s\S]*?\]);")
LSE_INSTRUMENT_SEARCH_MAX_WORKERS = 8
LSE_PRICE_EXPLORER_PAGE_SIZE = 2000
LSE_PRICE_EXPLORER_CATEGORY_TO_ASSET_TYPE = {
    "EQUITY": "Stock",
    "ETFS": "ETF",
}
JSE_INSTRUMENT_SEARCH_MAX_WORKERS = 4
JSE_INSTRUMENT_LINK_RE = re.compile(r'href="(?P<href>(?:https://www\.jse\.co\.za)?/jse/instruments/\d+)"')
JSE_INSTRUMENT_META_RE = re.compile(
    r"Instrument name:\s*(?P<name>.*?)\.\s*"
    r"Instrument code:\s*(?P<code>[A-Z0-9]+)\.\s*"
    r"Instrument type:\s*(?P<instrument_type>.*?)\."
)
JSE_INSTRUMENT_PROFILE_VALUE_RE = re.compile(
    r'instrument-profile__(?P<field>sector|industry)--[^"\']*["\'][^>]*>(?P<value>.*?)</span>',
    re.I | re.S,
)
WSE_FORM_RE = re.compile(r"<form\b[^>]*\bid=['\"](?P<form_id>[^'\"]+)['\"][\s\S]*?</form>", re.I)
WSE_INPUT_TAG_RE = re.compile(r"<input\b[^>]*>", re.I)
WSE_ATTR_RE = re.compile(r"\b(?P<name>[A-Za-z_:][-A-Za-z0-9_:.]*)\s*=\s*(['\"])(?P<value>.*?)\2", re.S)
WSE_CHECKED_RE = re.compile(r"\bchecked(?:\s*=\s*(['\"]).*?\1)?", re.I | re.S)
WSE_COUNT_ALL_RE = re.compile(r"<span[^>]*\bid=['\"]count-all['\"][^>]*>(?P<count>\d+)</span>", re.I)
WSE_COMPANY_LINK_RE = re.compile(
    r'<a\s+href=["\'](?P<href>spolka\?isin=[^"\']+)["\'][^>]*>\s*'
    r'<strong[^>]*class=["\']name["\'][^>]*>(?P<label>.*?)</strong>',
    re.I | re.S,
)
WSE_SMALL_GREY_RE = re.compile(
    r'<small[^>]*class=["\'][^"\']*\bgrey\b[^"\']*["\'][^>]*>(?P<value>.*?)</small>',
    re.I | re.S,
)
WSE_ETF_LINK_RE = re.compile(
    r'<a\s+href=["\'](?P<href>etf\?isin=[^"\']+)["\'][^>]*>\s*<b>(?P<ticker>.*?)</b>\s*</a>',
    re.I | re.S,
)
HTML_TD_RE = re.compile(r"<td[^>]*>(?P<value>.*?)</td>", re.I | re.S)
HTML_TH_TD_ROW_RE = re.compile(
    r"<tr[^>]*>\s*<th[^>]*>(?P<label>.*?)</th>\s*<td[^>]*>(?P<value>.*?)</td>\s*</tr>",
    re.I | re.S,
)
BSE_BW_PANEL_RE = re.compile(
    r'<div[^>]*class="[^"]*\blvca-panel\b[^"]*"[^>]*>.*?'
    r'<div[^>]*class="[^"]*\blvca-panel-title\b[^"]*"[^>]*>(?P<title>.*?)</div>\s*'
    r'<div[^>]*class="[^"]*\blvca-panel-content\b[^"]*"[^>]*>(?P<body>.*?)'
    r"</div>\s*</div>\s*<!--\s*\.lvca-panel\s*-->",
    re.I | re.S,
)
CAVALI_PAGE_COUNT_RE = re.compile(r"P[aá]g\s+\d+\s+de\s+(?P<count>\d+)", re.I)
CAVALI_STOCK_GROUPS = {
    "ACCIONES",
    "CERTIFICADOS DE PARTICIPACION",
    "CERTIFICADOS DE PARTICIPACIÓN",
    "CUOTAS DE PARTICIPACION",
    "CUOTAS DE PARTICIPACIÓN",
}
CAVALI_ETF_GROUPS = {
    "UNIDAD DE PARTICIPACION",
    "UNIDAD DE PARTICIPACIÓN",
}
BVB_INSTRUMENT_ROW_RE = re.compile(
    r"<tr\b[^>]*>(?P<body>.*?)</tr>",
    re.I | re.S,
)
BVB_INSTRUMENT_LINK_RE = re.compile(
    r'<a\s+href="(?P<href>/FinancialInstruments/Details/FinancialInstrumentsDetails\.aspx\?s=(?P<href_ticker>[^"]+))"[^>]*>\s*'
    r"<b>\s*(?P<ticker>.*?)\s*</b>\s*</a>",
    re.I | re.S,
)
BVB_INSTRUMENT_ISIN_RE = re.compile(r"<p[^>]*>\s*(?P<isin>[A-Z]{2}[A-Z0-9]{10})\s*</p>", re.I | re.S)
BVB_SHARES_ASYNC_TABS = (
    "ctl00$ctl00$body$rightColumnPlaceHolder$TabsControlPiete$lb1",
    "ctl00$ctl00$body$rightColumnPlaceHolder$TabsControlPiete$lb2",
)
NSE_KE_COMPANY_TITLE_RE = re.compile(r"\[nectar_animated_title\b", re.I)
NSE_KE_TITLE_TEXT_RE = re.compile(r'\btext=["“”″](?P<name>.*?)["“”″]\]', re.I | re.S)
NSE_KE_SECTION_TITLE_RE = re.compile(r'\btitle=["“”″](?P<title>.*?)["“”″]', re.I | re.S)
NSE_KE_SYMBOL_RE = re.compile(
    r"Trading\s+Symbol:\s*(?P<symbol>.*?)(?:ISIN\s*(?:CODE)?\s*:|<br\s*/?>|\[/vc_column_text\])",
    re.I | re.S,
)
NSE_KE_ISIN_RE = re.compile(
    r"ISIN\s*(?:CODE)?\s*:\s*(?P<isin>.*?)(?:<br\s*/?>|\[/vc_column_text\])",
    re.I | re.S,
)
NSE_KE_SECTOR_MAP = {
    "AGRICULTURAL": "Consumer Staples",
    "AUTOMOBILES AND ACCESSORIES": "Consumer Discretionary",
    "BANKING": "Financials",
    "CONSTRUCTION AND ALLIED": "Materials",
    "ENERGY AND PETROLEUM": "Energy",
    "INSURANCE": "Financials",
    "INVESTMENT": "Financials",
    "INVESTMENT SERVICES": "Financials",
    "TELECOMMUNICATION AND TECHNOLOGY": "Communication Services",
    "REAL ESTATE INVESTMENT TRUST": "Real Estate",
}
ATHEX_CLASSIFICATION_LINE_RE = re.compile(
    r"^(?P<isin>[A-Z]{2}[A-Z0-9]{10})\s+(?P<ticker>[A-Z0-9]+)\s+(?P<rest>.+)$"
)
ATHEX_SUPER_SECTOR_MAP = {
    "Banks": "Financials",
    "Basic Resources": "Materials",
    "Construction & Materials": "Industrials",
    "Consumer Products & Services": "Consumer Discretionary",
    "Energy": "Energy",
    "Financial Services": "Financials",
    "Food, Beverage & Tobacco": "Consumer Staples",
    "Health Care": "Health Care",
    "Industrial Goods & Services": "Industrials",
    "Insurance": "Financials",
    "Media": "Communication Services",
    "Personal Care, Drug & Grocery Stores": "Consumer Staples",
    "Real Estate": "Real Estate",
    "Retail": "Consumer Discretionary",
    "Technology": "Information Technology",
    "Telecommunications": "Communication Services",
    "Travel & Leisure": "Consumer Discretionary",
    "Utilities": "Utilities",
}
ZAGREB_SECURITIES_ROW_RE = re.compile(
    r"<tr\b(?P<attrs>[^>]*)>(?P<body>.*?)</tr>",
    re.I | re.S,
)
PSE_CZ_SHARE_LINK_RE = re.compile(
    r'<td[^>]*class="[^"]*\bitem-title\b[^"]*"[^>]*>.*?'
    r'<a\s+href="(?P<href>/en/detail/(?P<isin>[A-Z0-9]+))">\s*(?P<label>.*?)\s*</a>\s*'
    r'<div[^>]*class="[^"]*\bisin\b[^"]*"[^>]*>\s*(?P=isin)\s*</div>',
    re.I | re.S,
)
PSE_CZ_TICKER_XETRA_RE = re.compile(
    r"<td[^>]*>\s*Ticker Xetra.*?</td>\s*<td[^>]*>(?P<ticker>.*?)</td>",
    re.I | re.S,
)
PSE_CZ_TICKER_BLOOMBERG_RE = re.compile(
    r"<td[^>]*>\s*Ticker Bloomberg\s*</td>\s*<td[^>]*>(?P<ticker>.*?)</td>",
    re.I | re.S,
)
PSE_CZ_TICKER_REUTERS_RE = re.compile(
    r"<td[^>]*>\s*Ticker Reuters\s*</td>\s*<td[^>]*>(?P<ticker>.*?)</td>",
    re.I | re.S,
)
BME_GROWTH_DETAIL_LINK_RE = re.compile(
    r'<a\s+href=["\'](?P<href>/ing/Ficha/[^"\']+?_[A-Z]{2}[A-Z0-9]{10}\.aspx)["\'][^>]*>'
    r"(?P<label>.*?)</a>",
    re.I | re.S,
)
BME_GROWTH_PAGE_RE = re.compile(r"selPag\(['\"](?P<page>\d+)['\"]\)", re.I)
BME_GROWTH_DETAIL_FIELD_RE = re.compile(
    r"<h3>\s*(?P<label>Security name|Ticker|ISIN)\s*</h3>\s*<p>\s*(?P<value>.*?)\s*</p>",
    re.I | re.S,
)
BME_GROWTH_H1_RE = re.compile(r"<h1[^>]*>(?P<name>.*?)</h1>", re.I | re.S)
BME_LISTED_VALUES_ROW_RE = re.compile(
    r"^(?P<date>\d{8})\s+"
    r"(?P<segment>[A-Z]{2})\s+"
    r"(?P<ticker>[A-Z0-9.-]+)\s+"
    r"(?P<isin>[A-Z]{2}[A-Z0-9]{10})\s+"
    r"(?P<rest>.+)$"
)
BME_LISTED_VALUES_NAME_RE = re.compile(
    r"^(?P<name>.*?)\s+[-+]?\d[\d.,]*\s+[IVX]+\s+[\d.]+(?:\s+Fixing)?$"
)
BME_LISTED_VALUES_SEGMENT_ASSET_TYPES = {
    "EQ": "Stock",
    "LT": "Stock",
    "TF": "ETF",
}
BME_GROWTH_RETRY_DELAY_SECONDS = 10.0
ISIN_TOKEN_RE = re.compile(r"\b[A-Z]{2}[A-Z0-9]{10}\b")
SET_NUXT_PREFIX = "window.__NUXT__=(function("
SET_NUXT_FUNCTION_BODY_MARKER = "){"
SET_NUXT_RETURN_MARKER = ";return "
SET_NUXT_SEARCH_OPTION_MARKER = "searchOption:["
SET_NUXT_SEARCH_OPTION_END_MARKER = "],dropdownAdditional:"
SET_STOCK_SEARCH_SECURITY_TYPES = {"S": "Stock", "L": "ETF"}
SET_STOCK_SEARCH_SECTOR_MAP = {
    "AGRI": "Consumer Staples",
    "AGRO": "Consumer Staples",
    "AUTO": "Consumer Discretionary",
    "BANK": "Financials",
    "COMM": "Consumer Discretionary",
    "CONMAT": "Materials",
    "CONS": "Industrials",
    "CONSUMP": "Consumer Discretionary",
    "ENERG": "Energy",
    "ETRON": "Information Technology",
    "FASHION": "Consumer Discretionary",
    "FIN": "Financials",
    "FINCIAL": "Financials",
    "FOOD": "Consumer Staples",
    "HELTH": "Health Care",
    "HOME": "Consumer Discretionary",
    "ICT": "Communication Services",
    "IMM": "Industrials",
    "INDUS": "Industrials",
    "INSUR": "Financials",
    "MEDIA": "Communication Services",
    "PAPER": "Materials",
    "PERSON": "Consumer Staples",
    "PETRO": "Materials",
    "PF&REIT": "Real Estate",
    "PKG": "Materials",
    "PROF": "Industrials",
    "PROP": "Real Estate",
    "PROPCON": "Real Estate",
    "RESOURC": "Energy",
    "SERVICE": "Consumer Discretionary",
    "STEEL": "Materials",
    "TECH": "Information Technology",
    "TOURISM": "Consumer Discretionary",
    "TRANS": "Industrials",
}
JS_IDENTIFIER_RE = re.compile(r"^[A-Za-z_$][A-Za-z0-9_$]*$")

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

OTC_MARKETS_PROFILE_HEADERS = {
    "accept": "application/json, text/plain, */*",
    "origin": "https://www.otcmarkets.com",
    "referer": "https://www.otcmarkets.com/",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/26.0 Safari/605.1.15"
    ),
}
OTC_MARKETS_PROFILE_MAX_WORKERS = 24
OTC_MARKETS_PROFILE_TIMEOUT = 4.0
OTC_MARKETS_ADR_TYPE_NAMES = {"adr", "adrs"}
OTC_MARKETS_STOCK_SCREENER_STOCK_TYPES = {
    "adrs",
    "common stock",
    "foreign ordinary shares",
    "new york registry shs",
    "preferred stock",
}
CUSIP_RE = re.compile(r"^[A-Z0-9]{9}$")


def sec_reference_scope(exchange: str) -> str:
    if exchange == "OTC":
        return "listed_companies_subset"
    return "exchange_directory"

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

SIX_SHARE_DETAILS_FQS_FIELDS = (
    "ISIN",
    "IssuerNameFull",
    "IssuerNameShort",
    "FundLongName",
    "ShortName",
    "ProductLine",
    "ProductLineDesc",
    "TitleSegment",
    "TitleSegmentDesc",
    "TradingBaseCurrency",
    "ValorNumber",
    "ValorSymbol",
    "SBISector",
    "SBISectorDesc",
    "ListingSegmentDesc",
    "IndustrySectorDesc",
    "AssetClassDesc",
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
SIX_ASSET_CLASS_CATEGORY_MAP = {
    "commodities": "Commodity",
    "crypto": "Alternative",
    "equity developed markets": "Equity",
    "equity emerging markets": "Equity",
    "equity strategy": "Equity",
    "equity themes": "Equity",
    "fixed income": "Fixed Income",
    "funds": "Other",
    "loans": "Fixed Income",
    "money market": "Money Market",
    "other": "Other",
    "volatility": "Volatility",
}
SIX_SHARE_INDUSTRY_STOCK_SECTOR_MAP = {
    "banks": "Financials",
    "chemicals, pharma": "Health Care",
    "electrical engineering & electronics": "Industrials",
    "food, luxury goods": "Consumer Staples",
    "insurance companies": "Financials",
    "investment companies": "Financials",
    "real-estate companies": "Real Estate",
    "telecommunications": "Communication Services",
    "utilities": "Utilities",
}
SIX_SHARE_MISC_SERVICE_STOCK_SECTOR_OVERRIDES = {
    "RSGN": "Industrials",
    "SUNN": "Communication Services",
}

EURONEXT_MARKET_MAP = {
    "Euronext Amsterdam": "AMS",
    "Euronext Dublin": "ISE",
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
    "FUNDS": "ETF",
    "SHARES": "Stock",
    "UNIT": "Stock",
}
B3_CASH_STOCK_TICKER_RE = re.compile(r"^[A-Z0-9]{4}[3-8]B?$")
B3_BDR_ETF_MARKERS = (" ETF", "ETP")
B3_EXCLUDED_ISSUER_MARKERS = (
    "taxa de financiamento",
    "financ/termo",
)
B3_ETF_FUND_TYPES = ("ETF", "ETF-RF", "ETF-FII", "ETF-CRIPTO")
B3_FUNDS_PAGE_SIZE = 120
TPEX_CANONICAL_TICKER_RE = re.compile(r"(?:\d{4}|00\d{4}[A-Z]?)$")
TPEX_ETF_TICKER_RE = re.compile(r"(?:\d{4}|00\d{3,4}[A-Z]?)$")
TAIWAN_ISIN_TICKER_RE = re.compile(r"(?:\d{4}|00\d{3,4}[A-Z]?)$")
TAIWAN_DOMESTIC_REGISTRATION_MARKERS = {"", "-", "－", "--", "—"}
TAIWAN_INDUSTRY_CODE_SECTOR_MAP = {
    "01": "Materials",  # Cement
    "02": "Consumer Staples",  # Food
    "03": "Materials",  # Plastics
    "04": "Consumer Discretionary",  # Textiles
    "05": "Industrials",  # Electric machinery
    "06": "Industrials",  # Electrical and cable
    "08": "Materials",  # Glass and ceramics
    "09": "Materials",  # Paper and pulp
    "10": "Materials",  # Steel and iron
    "11": "Consumer Discretionary",  # Rubber
    "12": "Consumer Discretionary",  # Automobile
    "14": "Industrials",  # Building material and construction
    "15": "Industrials",  # Shipping and transportation
    "16": "Consumer Discretionary",  # Tourism
    "17": "Financials",  # Finance and insurance
    "18": "Consumer Discretionary",  # Trading and consumers' goods
    "21": "Materials",  # Chemical
    "22": "Health Care",  # Biotechnology and medical care
    "23": "Utilities",  # Oil, gas and electricity
    "24": "Information Technology",  # Semiconductor
    "25": "Information Technology",  # Computer and peripheral equipment
    "26": "Information Technology",  # Optoelectronic
    "27": "Communication Services",  # Communications and internet
    "28": "Information Technology",  # Electronic parts/components
    "29": "Information Technology",  # Electronic products distribution
    "30": "Information Technology",  # Information service
    "31": "Information Technology",  # Other electronic
    "32": "Communication Services",  # Cultural and creative
    "33": "Consumer Staples",  # Agriculture technology
    "34": "Consumer Discretionary",  # E-commerce
    "35": "Industrials",  # Green energy and environmental services
    "36": "Information Technology",  # Digital cloud
    "37": "Consumer Discretionary",  # Sports and leisure
    "38": "Consumer Discretionary",  # Household living
}
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
    "6": "KRX",
}
KRX_MARKET_ENGNAME_TO_EXCHANGE = {
    "KOSPI": "KRX",
    "KOSDAQ": "KOSDAQ",
    "KOSDAQ GLOBAL": "KOSDAQ",
    "KONEX": "KRX",
}
KRX_STD_INDUSTRY_PREFIX_SECTOR_MAP = {
    "01": "Consumer Staples",
    "02": "Materials",
    "03": "Industrials",
    "04": "Utilities",
    "05": "Utilities",
    "06": "Industrials",
    "07": "Consumer Discretionary",
    "08": "Industrials",
    "09": "Consumer Discretionary",
    "10": "Information Technology",
    "11": "Financials",
    "12": "Real Estate",
    "13": "Industrials",
    "14": "Industrials",
    "16": "Consumer Discretionary",
    "17": "Health Care",
    "18": "Consumer Discretionary",
}
KRX_INDUSTRY_KEYWORD_SECTOR_RULES = (
    (
        "Health Care",
        (
            "biotechnology",
            "diagnostic",
            "health",
            "hospital",
            "medical",
            "medicament",
            "medicinal",
            "pharma",
        ),
    ),
    (
        "Financials",
        ("banking", "credit", "financial", "funding", "insurance", "investment", "pension", "trust"),
    ),
    ("Real Estate", ("real estate",)),
    ("Utilities", ("distribution of gaseous fuel", "electricity", "sewerage", "steam", "water supply")),
    ("Energy", ("coal", "gas extraction", "oil", "petroleum")),
    (
        "Information Technology",
        (
            "computer",
            "data processing",
            "electronic component",
            "electronic video and audio equipment",
            "information service",
            "measuring",
            "optical instrument",
            "peripheral",
            "semiconductor",
            "software",
            "system integration",
            "telecommunication and broadcasting apparatus",
            "web portal",
        ),
    ),
    ("Communication Services", ("advertising", "broadcasting", "motion picture", "publishing", "telecommunications")),
    (
        "Consumer Staples",
        (
            "beverage",
            "dairy",
            "edible",
            "feed",
            "fish",
            "food",
            "grain",
            "livestock",
            "meat",
            "tobacco",
            "vegetable and animal oils",
        ),
    ),
    (
        "Consumer Discretionary",
        (
            "accommodation",
            "amusement",
            "apparel",
            "arts",
            "domestic appliances",
            "education",
            "footwear",
            "furniture",
            "household goods",
            "leather",
            "luggage",
            "motor vehicle",
            "recreation",
            "retail",
            "travel",
            "wearing",
        ),
    ),
    (
        "Materials",
        (
            "basic chemical",
            "cement",
            "ceramic",
            "chemical products",
            "fertilizer",
            "glass",
            "iron",
            "man-made fiber",
            "metal",
            "mineral",
            "paper",
            "plastic",
            "pulp",
            "rubber",
            "steel",
            "textile",
            "wood",
        ),
    ),
    (
        "Industrials",
        (
            "aircraft",
            "architectural",
            "business support",
            "construction",
            "engineering",
            "fabricated metal",
            "head offices",
            "machinery",
            "management consultancy",
            "scientific",
            "ship",
            "technical services",
            "transport",
            "weapon",
        ),
    ),
)
EGX_LISTED_STOCKS_DATA_ISIN_RE = re.compile(r"StocksData\.aspx\?ISIN=([A-Z0-9]{12})")
EGX_VIEWSTATE_RE = re.compile(
    r"<input\b(?=[^>]*(?:name|id)=[\"']__VIEWSTATE[\"'])(?P<tag>[^>]*)>",
    re.I | re.S,
)
EGX_VIEWSTATE_VALUE_RE = re.compile(r"\bvalue=(?P<quote>[\"'])(?P<value>.*?)(?P=quote)", re.I | re.S)
EGX_GRID_ROW_MARKER = "show('ctl00_C_L_GridView2_ctl"
EGX_SECTOR_MAP = {
    "banks": "Financials",
    "basic resources": "Materials",
    "health care & pharmaceuticals": "Health Care",
    "industrial goods , services and automobiles": "Industrials",
    "real estate": "Real Estate",
    "travel & leisure": "Consumer Discretionary",
    "utilities": "Utilities",
    "it , media & communication services": "Communication Services",
    "food, beverages and tobacco": "Consumer Staples",
    "energy & support services": "Energy",
    "trade & distributors": "Consumer Discretionary",
    "shipping & transportation services": "Industrials",
    "education services": "Consumer Discretionary",
    "non-bank financial services": "Financials",
    "contracting & construction engineering": "Industrials",
    "textile & durables": "Consumer Discretionary",
    "building materials": "Materials",
    "paper & packaging": "Materials",
}
SSE_JSONP_RE = re.compile(r"^[^(]+\((.*)\)\s*$", re.S)
SSE_STOCK_TYPES = ("1", "2", "8")
SSE_ETF_SUBCLASSES = ("01", "02", "03", "05", "06", "08", "09", "31", "32", "33", "37")
CHINA_CSRC_SECTOR_MAP = {
    "A": "Consumer Staples",
    "B": "Materials",
    "C": "Industrials",
    "D": "Utilities",
    "E": "Industrials",
    "F": "Consumer Discretionary",
    "G": "Industrials",
    "H": "Consumer Discretionary",
    "I": "Information Technology",
    "J": "Financials",
    "K": "Real Estate",
    "L": "Industrials",
    "M": "Industrials",
    "N": "Utilities",
    "O": "Consumer Discretionary",
    "P": "Consumer Discretionary",
    "Q": "Health Care",
    "R": "Communication Services",
    "S": "Industrials",
}
IDX_SECTOR_MAP = {
    "barang baku": "Materials",
    "barang konsumen non-primer": "Consumer Discretionary",
    "barang konsumen primer": "Consumer Staples",
    "energi": "Energy",
    "kesehatan": "Health Care",
    "keuangan": "Financials",
    "perindustrian": "Industrials",
    "properti & real estat": "Real Estate",
    "teknologi": "Information Technology",
    "transportasi & logistik": "Industrials",
}
IDX_INFRASTRUCTURE_SUBSECTOR_MAP = {
    "telekomunikasi": "Communication Services",
    "utilitas": "Utilities",
}
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
PSX_DPS_SECTOR_SKIP_MARKERS = (
    "bills and bonds",
    "close - end mutual fund",
    "close-end mutual fund",
)
PSX_DPS_SECTOR_MAP = {
    "AUTOMOBILE ASSEMBLER": "Consumer Discretionary",
    "AUTOMOBILE PARTS & ACCESSORIES": "Consumer Discretionary",
    "CABLE & ELECTRICAL GOODS": "Industrials",
    "CEMENT": "Materials",
    "CHEMICAL": "Materials",
    "COMMERCIAL BANKS": "Financials",
    "ENGINEERING": "Industrials",
    "FERTILIZER": "Materials",
    "FOOD & PERSONAL CARE PRODUCTS": "Consumer Staples",
    "GLASS & CERAMICS": "Materials",
    "INSURANCE": "Financials",
    "INV. BANKS / INV. COS. / SECURITIES COS.": "Financials",
    "JUTE": "Consumer Discretionary",
    "LEASING COMPANIES": "Financials",
    "LEATHER & TANNERIES": "Consumer Discretionary",
    "MODARABAS": "Financials",
    "OIL & GAS EXPLORATION COMPANIES": "Energy",
    "OIL & GAS MARKETING COMPANIES": "Energy",
    "PAPER, BOARD & PACKAGING": "Materials",
    "PHARMACEUTICALS": "Health Care",
    "POWER GENERATION & DISTRIBUTION": "Utilities",
    "PROPERTY": "Real Estate",
    "REAL ESTATE INVESTMENT TRUST": "Real Estate",
    "REFINERY": "Energy",
    "SUGAR & ALLIED INDUSTRIES": "Consumer Staples",
    "SYNTHETIC & RAYON": "Materials",
    "TECHNOLOGY & COMMUNICATION": "Communication Services",
    "TEXTILE COMPOSITE": "Consumer Discretionary",
    "TEXTILE SPINNING": "Consumer Discretionary",
    "TEXTILE WEAVING": "Consumer Discretionary",
    "TOBACCO": "Consumer Staples",
    "TRANSPORT": "Industrials",
    "VANASPATI & ALLIED INDUSTRIES": "Consumer Staples",
    "WOOLLEN": "Consumer Discretionary",
}
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
        key="lse_price_explorer",
        provider="LSE",
        description="Official London Stock Exchange price explorer equity and ETF directory",
        source_url=LSE_PRICE_EXPLORER_URL,
        format="lse_price_explorer_json",
        reference_scope="exchange_directory",
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
        description="Official Cboe Canada listing directory API",
        source_url=CBOE_CANADA_LISTING_DIRECTORY_API_URL,
        format="cboe_canada_listing_directory_json",
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
        key="set_stock_search",
        provider="SET",
        description="Official Stock Exchange of Thailand stock search API",
        source_url=SET_STOCK_LIST_API_URL,
        format="set_stock_search_json",
    ),
    MasterfileSource(
        key="set_etf_search",
        provider="SET",
        description="Official Stock Exchange of Thailand ETF quote directory",
        source_url=SET_ETF_SEARCH_URL,
        format="set_etf_search_html",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="set_dr_search",
        provider="SET",
        description="Official Stock Exchange of Thailand DR quote directory",
        source_url=SET_DR_SEARCH_URL,
        format="set_dr_search_html",
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
        key="jpx_tse_stock_detail",
        provider="JPX",
        description="Official JPX/TSE Stock Data Search detail records with ISINs",
        source_url=JPX_STOCK_DETAIL_API_URL,
        format="jpx_tse_stock_detail_json",
        reference_scope="security_identifier_registry_subset",
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
        description="Official Deutsche Boerse Xetra all tradable instruments directory for shares and ETPs",
        source_url=DEUTSCHE_BOERSE_XETRA_ALL_TRADABLE_URL,
        format="deutsche_boerse_xetra_all_tradable_csv",
        reference_scope="exchange_directory",
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
        key="six_shares_explorer_full",
        provider="SIX",
        description="Official SIX FQS share and fund detail taxonomy supplement",
        source_url=SIX_SHARE_DETAILS_FQS_URL,
        format="six_share_details_fqs_json",
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
        key="jse_instrument_search",
        provider="JSE",
        description="Official JSE instrument pages discovered via on-site search",
        source_url=JSE_SEARCH_URL,
        format="jse_instrument_search_html",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="bme_listed_companies",
        provider="BME",
        description="Official BME listed companies directory for the continuous market",
        source_url=BME_LISTED_COMPANIES_API_URL,
        format="bme_listed_companies_json",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="bme_etf_list",
        provider="BME",
        description="Official BME ETF directory",
        source_url=BME_LISTED_COMPANIES_API_URL,
        format="bme_etf_list_json",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="bme_listed_values",
        provider="BME",
        description="Official BME listed values PDF for SIBE, ETFs, and LATIBEX",
        source_url=BME_LISTED_VALUES_PDF_URL,
        format="bme_listed_values_pdf",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="bme_security_prices_directory",
        provider="BME",
        description="Official BME security price directory for SIBE, Floor, Latibex, MTF, and ETFs",
        source_url=BME_LISTED_COMPANIES_API_URL,
        format="bme_security_prices_json",
        reference_scope="exchange_directory",
    ),
    MasterfileSource(
        key="bme_growth_prices",
        provider="BME Growth",
        description="Official BME Growth market price directory with instrument detail pages",
        source_url=BME_GROWTH_PRICES_URL,
        format="bme_growth_prices_html",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="athex_sector_classification",
        provider="ATHEX",
        description="Official ATHEX companies sector classification PDF with ISINs",
        source_url=ATHEX_CLASSIFICATION_PDF_URL,
        format="athex_sector_classification_pdf",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="bursa_equity_isin",
        provider="Bursa Malaysia",
        description="Official Bursa Malaysia equity ISIN PDF",
        source_url=BURSA_EQUITY_ISIN_PAGE_URL,
        format="bursa_equity_isin_pdf",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="bursa_closing_prices",
        provider="Bursa Malaysia",
        description="Official Bursa Malaysia closing price PDF with board and sector labels",
        source_url=BURSA_CLOSING_PRICES_PDF_URL,
        format="bursa_closing_prices_pdf",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="bse_bw_listed_companies",
        provider="BSE Botswana",
        description="Official Botswana Stock Exchange listed companies directory",
        source_url=BSE_BW_LISTED_COMPANIES_URL,
        format="bse_bw_listed_companies_html",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="bse_hu_listed_companies",
        provider="Budapest Stock Exchange",
        description="Official Budapest Stock Exchange embedded market-data feed for equities and BETA products",
        source_url=BSE_HU_BETA_MARKET_URL,
        format="bse_hu_marketdata_html",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="egx_listed_stocks",
        provider="EGX",
        description="Official Egyptian Exchange listed-stocks page decoded from ASP.NET ViewState",
        source_url=EGX_LISTED_STOCKS_URL,
        format="egx_listed_stocks_viewstate",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="bvl_issuers_directory",
        provider="CAVALI",
        description="Official Peruvian CSD issuer securities registry with BVL ISIN and ticker fields",
        source_url=CAVALI_BVL_EMISORES_URL,
        format="cavali_bvl_emisores_html",
        reference_scope="security_lookup_subset",
    ),
    MasterfileSource(
        key="cse_ma_listed_companies",
        provider="Casablanca Stock Exchange",
        description="Official Casablanca Stock Exchange active equity instruments JSONAPI directory",
        source_url=CASABLANCA_BOURSE_INSTRUMENTS_URL,
        format="cse_ma_listed_companies_json",
        reference_scope="exchange_directory",
    ),
    MasterfileSource(
        key="cse_lk_all_security_code",
        provider="CSE Sri Lanka",
        description="Official Colombo Stock Exchange all-security-code API",
        source_url=CSE_LK_ALL_SECURITY_CODE_URL,
        format="cse_lk_all_security_code_json",
        reference_scope="exchange_directory",
    ),
    MasterfileSource(
        key="cse_lk_company_info_summary",
        provider="CSE Sri Lanka",
        description="Official Colombo Stock Exchange company-info detail API with ISIN",
        source_url=CSE_LK_COMPANY_INFO_SUMMARY_URL,
        format="cse_lk_company_info_summary_json",
        reference_scope="exchange_directory",
    ),
    MasterfileSource(
        key="dse_tz_listed_companies",
        provider="DSE Tanzania",
        description="Official Dar es Salaam Stock Exchange listed-company table",
        source_url=DSE_TZ_LISTED_COMPANIES_URL,
        format="dse_tz_listed_companies_html",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="bvc_colombia_issuers",
        provider="BVC",
        description="Official Bolsa de Valores de Colombia local-equity issuer API with sector labels",
        source_url=BVC_RV_ISSUERS_URL,
        format="bvc_rv_issuers_json",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="byma_equity_details",
        provider="BYMA",
        description="Official Open BYMADATA equity detail endpoint for BCBA/BYMA instruments",
        source_url=BYMA_EQUITY_DETAIL_URL,
        format="byma_equity_details_json",
        reference_scope="security_lookup_subset",
    ),
    MasterfileSource(
        key="mse_mw_listed_companies",
        provider="MSE Malawi",
        description="Official Malawi Stock Exchange mainboard table with company links",
        source_url=MSE_MW_MARKET_MAINBOARD_URL,
        format="mse_mw_mainboard_html",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="nse_ke_listed_companies",
        provider="NSE Kenya",
        description="Official Nairobi Securities Exchange listed companies page with symbols, ISINs, and local sector sections",
        source_url=NSE_KE_LISTED_COMPANIES_URL,
        format="nse_ke_listed_companies_html",
        reference_scope="exchange_directory",
    ),
    MasterfileSource(
        key="nse_india_securities_available",
        provider="NSE India",
        description="Official NSE India securities-available CSVs for equity and ETF symbols",
        source_url=NSE_IN_SECURITIES_AVAILABLE_PAGE_URL,
        format="nse_india_securities_available_csv",
        reference_scope="exchange_directory",
    ),
    MasterfileSource(
        key="bse_india_scrips",
        provider="BSE India",
        description="Official BSE India active equity scrip API with ISINs",
        source_url=BSE_IN_SCRIPS_PAGE_URL,
        format="bse_india_scrips_json",
        reference_scope="exchange_directory",
    ),
    MasterfileSource(
        key="hkex_securities_list",
        provider="HKEX",
        description="Official HKEX list of securities workbook with stock codes, categories, and ISINs",
        source_url=HKEX_SECURITIES_LIST_URL,
        format="hkex_securities_list_xlsx",
        reference_scope="exchange_directory",
    ),
    MasterfileSource(
        key="sgx_securities_prices",
        provider="SGX",
        description="Official Singapore Exchange securities prices API with stock and ETF trading universe",
        source_url=SGX_SECURITIES_PRICES_PAGE_URL,
        format="sgx_securities_prices_json",
        reference_scope="exchange_directory",
    ),
    MasterfileSource(
        key="dfm_listed_securities",
        provider="DFM",
        description="Official Dubai Financial Market stocks API plus company profile lookup with ISINs and sectors",
        source_url=DFM_LISTED_SECURITIES_PAGE_URL,
        format="dfm_listed_securities_json",
        reference_scope="exchange_directory",
    ),
    MasterfileSource(
        key="boursa_kuwait_stocks",
        provider="Boursa Kuwait",
        description="Official Boursa Kuwait market data API with listed symbols, ISINs, and sector codes",
        source_url=BOURSA_KUWAIT_STOCKS_PAGE_URL,
        format="boursa_kuwait_legacy_mix_json",
        reference_scope="exchange_directory",
    ),
    MasterfileSource(
        key="bahrain_bourse_listed_companies",
        provider="Bahrain Bourse",
        description="Official Bahrain Bourse ISIN code table for local equities and Bahrain Investment Market listings",
        source_url=BAHRAIN_BOURSE_ISIN_CODES_URL,
        format="bahrain_bourse_isin_codes_html",
        reference_scope="exchange_directory",
    ),
    MasterfileSource(
        key="bist_kap_mkk_listed_securities",
        provider="KAP/MKK",
        description="Official BIST listed-company list from KAP joined to official MKK ISIN security registry",
        source_url=BIST_LISTED_COMPANIES_PAGE_URL,
        format="bist_kap_mkk_listed_securities_json",
        reference_scope="exchange_directory",
    ),
    MasterfileSource(
        key="tadawul_main_market_watch",
        provider="Saudi Exchange",
        description="Official Saudi Exchange issuer directory joined to official theme search symbol/ISIN feed",
        source_url=TADAWUL_ISSUERS_DIRECTORY_URL,
        format="tadawul_securities_json",
        reference_scope="exchange_directory",
    ),
    MasterfileSource(
        key="adx_market_watch",
        provider="ADX",
        description="Official Abu Dhabi Securities Exchange issuer directory joined to official market-watch security boards",
        source_url=ADX_MARKET_WATCH_PAGE_URL,
        format="adx_market_watch_json",
        reference_scope="exchange_directory",
    ),
    MasterfileSource(
        key="qse_market_watch",
        provider="QSE",
        description="Official Qatar Stock Exchange MarketWatch stock and ETF directory",
        source_url=QSE_MARKET_WATCH_URL,
        format="qse_market_watch_json",
        reference_scope="exchange_directory",
    ),
    MasterfileSource(
        key="muscat_securities_companies",
        provider="MSX",
        description="Official Muscat Stock Exchange active company directory",
        source_url=MSX_COMPANIES_URL,
        format="msx_companies_json",
        reference_scope="exchange_directory",
    ),
    MasterfileSource(
        key="rse_listed_companies",
        provider="RSE",
        description="Official Rwanda Stock Exchange listed-company cards",
        source_url=RSE_LISTED_COMPANIES_URL,
        format="rse_listed_companies_html",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="gse_listed_companies",
        provider="GSE",
        description="Official Ghana Stock Exchange listed-company page captured as markdown",
        source_url=GSE_LISTED_COMPANIES_URL,
        format="gse_listed_companies_markdown",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="luse_listed_companies",
        provider="LuSE",
        description="Official Lusaka Securities Exchange listed-company page captured as markdown",
        source_url=LUSE_LISTED_COMPANIES_URL,
        format="luse_listed_companies_markdown",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="bolsa_santiago_instruments",
        provider="Bolsa de Santiago",
        description="Official Bolsa de Santiago instruments API captured through a browser session",
        source_url=BOLSA_SANTIAGO_INSTRUMENTS_URL,
        format="bolsa_santiago_instruments_json",
        reference_scope="exchange_directory",
    ),
    MasterfileSource(
        key="sem_isin",
        provider="SEM",
        description="Official Stock Exchange of Mauritius CDS ISIN workbook for Official Market and DEM instruments",
        source_url=SEM_ISIN_XLSX_URL,
        format="sem_isin_xlsx",
        reference_scope="exchange_directory",
    ),
    MasterfileSource(
        key="use_ug_listed_companies",
        provider="USE Uganda",
        description="Official Uganda Securities Exchange market-snapshot listed-company table",
        source_url=USE_UG_MARKET_SNAPSHOT_URL,
        format="use_ug_market_snapshot_html",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="nzx_instruments",
        provider="NZX",
        description="Official New Zealand Exchange NZSX instruments dataset from the market page Next.js payload",
        source_url=NZX_INSTRUMENTS_URL,
        format="nzx_instruments_next_data",
    ),
    MasterfileSource(
        key="nasdaq_mutual_fund_quotes",
        provider="Nasdaq",
        description="Official Nasdaq Fund Network quote API for current NMFQS symbols",
        source_url=NASDAQ_MUTUAL_FUND_SCREENER_URL,
        format="nasdaq_mutual_fund_quote_json",
        reference_scope="security_lookup_subset",
    ),
    MasterfileSource(
        key="zse_zw_listed_companies",
        provider="ZSE Zimbabwe",
        description="Official Zimbabwe Stock Exchange listed issuer and price-sheet APIs",
        source_url=ZSE_ZW_LISTED_EQUITIES_URL,
        format="zse_zw_issuers_json",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="bvb_shares_directory",
        provider="BVB",
        description="Official Bucharest Stock Exchange shares directory with ISINs across Regulated, AeRO, and SMT Intl tabs",
        source_url=BVB_SHARES_DIRECTORY_URL,
        format="bvb_shares_directory_html",
        reference_scope="exchange_directory",
    ),
    MasterfileSource(
        key="bvb_fund_units_directory",
        provider="BVB",
        description="Official Bucharest Stock Exchange fund units directory gated to ETF rows",
        source_url=BVB_FUND_UNITS_DIRECTORY_URL,
        format="bvb_fund_units_directory_html",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="ngx_equities_price_list",
        provider="NGX",
        description="Official Nigerian Exchange equities price list API with sector labels",
        source_url=NGX_EQUITIES_PRICE_LIST_URL,
        format="ngx_equities_price_list_json",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="ngx_company_profile_directory",
        provider="NGX",
        description="Official Nigerian Exchange company profile directory with issuer names and sectors",
        source_url=NGX_COMPANY_PROFILE_DIRECTORY_URL,
        format="ngx_company_profile_directory_html",
        reference_scope="exchange_directory",
    ),
    MasterfileSource(
        key="bmv_stock_search",
        provider="BMV",
        description="Official BMV stock search supplement for exact ticker gaps",
        source_url=BMV_SEARCH_URL,
        format="bmv_stock_search_json",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="bmv_capital_trust_search",
        provider="BMV",
        description="Official BMV capital-market trust supplement for exact ticker gaps",
        source_url=BMV_SEARCH_URL,
        format="bmv_capital_trust_search_json",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="bmv_etf_search",
        provider="BMV",
        description="Official BMV ETF and TRACS search supplement",
        source_url=BMV_SEARCH_URL,
        format="bmv_etf_search_json",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="bmv_market_data_securities",
        provider="BMV",
        description="Official BMV issuer market-data pages with instrument ISINs and issuer taxonomy",
        source_url=BMV_MARKET_DATA_SECURITIES_URL,
        format="bmv_market_data_security_pages_html",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="bmv_issuer_directory",
        provider="BMV",
        description="Official BMV issuer directory supplement for local and global listings",
        source_url=BMV_ISSUER_DIRECTORY_SEARCH_URL,
        format="bmv_issuer_directory_json",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="nasdaq_nordic_stockholm_shares",
        provider="Nasdaq Nordic",
        description="Official Nasdaq Nordic Stockholm shares screener (Main Market + First North)",
        source_url=NASDAQ_NORDIC_SHARES_SCREENER_URL,
        format="nasdaq_nordic_stockholm_shares_json",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="nasdaq_nordic_stockholm_shares_search",
        provider="Nasdaq Nordic",
        description="Official Nasdaq Nordic Stockholm share search supplement for exact stock ticker gaps",
        source_url=NASDAQ_NORDIC_SEARCH_URL,
        format="nasdaq_nordic_stockholm_shares_search_json",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="nasdaq_nordic_helsinki_shares",
        provider="Nasdaq Nordic",
        description="Official Nasdaq Nordic Helsinki shares screener (Main Market + First North)",
        source_url=NASDAQ_NORDIC_SHARES_SCREENER_URL,
        format="nasdaq_nordic_helsinki_shares_json",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="nasdaq_nordic_helsinki_shares_search",
        provider="Nasdaq Nordic",
        description="Official Nasdaq Nordic Helsinki share search supplement for exact stock ticker gaps",
        source_url=NASDAQ_NORDIC_SEARCH_URL,
        format="nasdaq_nordic_helsinki_shares_search_json",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="nasdaq_nordic_iceland_shares",
        provider="Nasdaq Nordic",
        description="Official Nasdaq Nordic Iceland shares screener (Main Market + First North)",
        source_url=NASDAQ_NORDIC_SHARES_SCREENER_URL,
        format="nasdaq_nordic_iceland_shares_json",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="spotlight_companies_directory",
        provider="Spotlight",
        description="Official Spotlight companies directory with detail-page symbols",
        source_url=SPOTLIGHT_COMPANIES_DIRECTORY_URL,
        format="spotlight_companies_directory_json",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="spotlight_companies_search",
        provider="Spotlight",
        description="Official Spotlight company search supplement for exact Swedish stock ticker gaps",
        source_url=SPOTLIGHT_COMPANY_SEARCH_URL,
        format="spotlight_companies_search_json",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="ngm_companies_page",
        provider="NGM",
        description="Official Nordic Growth Market companies page",
        source_url=NGM_COMPANIES_PAGE_URL,
        format="ngm_companies_page_html",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="ngm_market_data_equities",
        provider="NGM",
        description="Official Nordic Growth Market market-data supplement for active equities",
        source_url=NGM_MARKET_DATA_API_ROOT_URL,
        format="ngm_market_data_equities_json",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="nasdaq_nordic_copenhagen_shares",
        provider="Nasdaq Nordic",
        description="Official Nasdaq Nordic Copenhagen shares screener (Main Market + First North)",
        source_url=NASDAQ_NORDIC_SHARES_SCREENER_URL,
        format="nasdaq_nordic_copenhagen_shares_json",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="nasdaq_nordic_copenhagen_shares_search",
        provider="Nasdaq Nordic",
        description="Official Nasdaq Nordic Copenhagen share search supplement for exact stock ticker gaps",
        source_url=NASDAQ_NORDIC_SEARCH_URL,
        format="nasdaq_nordic_copenhagen_shares_search_json",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="nasdaq_nordic_stockholm_etfs",
        provider="Nasdaq Nordic",
        description="Official Nasdaq Nordic Stockholm ETF screener",
        source_url=NASDAQ_NORDIC_ETP_SCREENER_URL,
        format="nasdaq_nordic_stockholm_etfs_json",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="nasdaq_nordic_helsinki_etfs",
        provider="Nasdaq Nordic",
        description="Official Nasdaq Nordic Helsinki ETF screener",
        source_url=NASDAQ_NORDIC_ETP_SCREENER_URL,
        format="nasdaq_nordic_helsinki_etfs_json",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="nasdaq_nordic_copenhagen_etfs",
        provider="Nasdaq Nordic",
        description="Official Nasdaq Nordic Copenhagen ETF screener",
        source_url=NASDAQ_NORDIC_ETP_SCREENER_URL,
        format="nasdaq_nordic_copenhagen_etfs_json",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="nasdaq_nordic_copenhagen_etf_search",
        provider="Nasdaq Nordic",
        description="Official Nasdaq Nordic Copenhagen ETF search supplement",
        source_url=NASDAQ_NORDIC_SEARCH_URL,
        format="nasdaq_nordic_copenhagen_etf_search_json",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="nasdaq_nordic_stockholm_trackers",
        provider="Nasdaq Nordic",
        description="Official Nasdaq Nordic Stockholm tracker certificates discovered via search",
        source_url=NASDAQ_NORDIC_SEARCH_URL,
        format="nasdaq_nordic_stockholm_trackers_json",
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
        key="szse_b_share_list",
        provider="SZSE",
        description="Official SZSE B-share list",
        source_url=SZSE_STOCK_LIST_URL,
        format="szse_b_share_list_json",
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
        key="tpex_etf_filter",
        provider="TPEX",
        description="Official TPEX ETF InfoHub filter directory",
        source_url=TPEX_ETF_FILTER_PAGE_URL,
        format="tpex_etf_filter_json",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="tpex_mainboard_basic_info",
        provider="MOPS",
        description="Official MOPS mainboard company basic information feed for TPEX listings",
        source_url=TPEX_MAINBOARD_BASIC_INFO_URL,
        format="tpex_mainboard_basic_csv",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="tpex_emerging_basic_info",
        provider="MOPS",
        description="Official MOPS emerging company basic information feed for TPEX listings",
        source_url=TPEX_EMERGING_BASIC_INFO_URL,
        format="tpex_emerging_basic_csv",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="krx_listed_companies",
        provider="KRX",
        description="Official KRX listed company directory with industry taxonomy",
        source_url=KRX_LISTED_COMPANIES_URL,
        format="krx_listed_companies_json",
        reference_scope="exchange_directory",
    ),
    MasterfileSource(
        key="krx_etf_finder",
        provider="KRX",
        description="Official KRX ETF issue finder",
        source_url="https://data.krx.co.kr/contents/MDC/MAIN/main/index.cmd",
        format="krx_etf_finder_json",
        reference_scope="exchange_directory",
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
        key="psx_dps_symbols",
        provider="PSX",
        description="Official PSX DPS symbols directory",
        source_url=PSX_DPS_SYMBOLS_URL,
        format="psx_dps_symbols_json",
        reference_scope="exchange_directory",
    ),
    MasterfileSource(
        key="pse_listed_company_directory",
        provider="PSE",
        description="Official PSE listed company directory frame",
        source_url=PSE_LISTED_COMPANY_DIRECTORY_URL,
        format="pse_listed_company_directory_html",
    ),
    MasterfileSource(
        key="pse_cz_shares_directory",
        provider="Prague Stock Exchange",
        description="Official Prague Stock Exchange share market directory",
        source_url=PSE_CZ_SHARES_URL,
        format="pse_cz_shares_directory_html",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="idx_listed_companies",
        provider="IDX",
        description="Official IDX stock list directory",
        source_url=IDX_LISTED_COMPANIES_URL,
        format="idx_listed_companies_json",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="idx_company_profiles",
        provider="IDX",
        description="Official IDX company profiles metadata API",
        source_url=IDX_COMPANY_PROFILES_URL,
        format="idx_company_profiles_json",
        reference_scope="exchange_directory",
    ),
    MasterfileSource(
        key="wse_listed_companies",
        provider="GPW",
        description="Official GPW listed companies directory",
        source_url=WSE_COMPANIES_PAGE_URL,
        format="wse_listed_companies_html",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="newconnect_listed_companies",
        provider="NewConnect",
        description="Official NewConnect listed companies directory",
        source_url=NEWCONNECT_COMPANIES_PAGE_URL,
        format="newconnect_listed_companies_html",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="wse_etf_list",
        provider="GPW",
        description="Official GPW ETF/ETC/ETN directory",
        source_url=WSE_ETF_PAGE_URL,
        format="wse_etf_list_html",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="tase_securities_marketdata",
        provider="TASE",
        description="Official TASE market securities directory (shares subset)",
        source_url=TASE_SECURITIES_MARKETDATA_URL,
        format="tase_securities_marketdata_json",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="tase_etf_marketdata",
        provider="TASE",
        description="Official TASE ETF market directory",
        source_url=TASE_ETF_MARKETDATA_URL,
        format="tase_etf_marketdata_json",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="tase_foreign_etf_search",
        provider="TASE",
        description="Official TASE search entity supplement for exact foreign ETF ticker gaps",
        source_url=TASE_SEARCH_ENTITIES_URL,
        format="tase_foreign_etf_search_json",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="tase_participating_unit_search",
        provider="TASE",
        description="Official TASE search entity supplement for exact participating unit stock ticker gaps",
        source_url=TASE_SEARCH_ENTITIES_URL,
        format="tase_participating_unit_search_json",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="hose_listed_stocks",
        provider="HOSE",
        description="Official Ho Chi Minh Stock Exchange listed stocks directory",
        source_url=HOSE_LISTED_STOCKS_URL,
        format="hose_listed_stocks_json",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="hose_etf_list",
        provider="HOSE",
        description="Official Ho Chi Minh Stock Exchange ETF directory",
        source_url=HOSE_ETF_LIST_URL,
        format="hose_etf_list_json",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="hose_fund_certificate_list",
        provider="HOSE",
        description="Official Ho Chi Minh Stock Exchange fund certificate directory",
        source_url=HOSE_FUND_CERTIFICATE_LIST_URL,
        format="hose_fund_certificate_list_json",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="hnx_listed_securities",
        provider="HNX",
        description="Official Hanoi Stock Exchange listed securities directory",
        source_url=HNX_LISTED_SECURITIES_PAGE_URL,
        format="hnx_listed_securities_json",
        reference_scope="exchange_directory",
    ),
    MasterfileSource(
        key="upcom_registered_securities",
        provider="HNX",
        description="Official Hanoi Stock Exchange UPCoM registered securities directory",
        source_url=UPCOM_REGISTERED_SECURITIES_PAGE_URL,
        format="upcom_registered_securities_json",
        reference_scope="exchange_directory",
    ),
    MasterfileSource(
        key="vienna_listed_companies",
        provider="Wiener Boerse",
        description="Official Vienna Stock Exchange listed companies directory",
        source_url=VIENNA_LISTED_COMPANIES_URL,
        format="vienna_listed_companies_html",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="zagreb_securities_directory",
        provider="ZSE Croatia",
        description="Official Zagreb Stock Exchange listed share directory",
        source_url=ZAGREB_SECURITIES_URL,
        format="zagreb_securities_html",
        reference_scope="listed_companies_subset",
    ),
    MasterfileSource(
        key="sec_company_tickers_exchange",
        provider="SEC",
        description="Official SEC company ticker to exchange mapping",
        source_url=SEC_COMPANY_TICKERS_URL,
        format="sec_company_tickers_exchange_json",
    ),
    MasterfileSource(
        key="otc_markets_security_profile",
        provider="OTC Markets",
        description="Official OTC Markets security profile supplement for OTC ISIN and status gaps",
        source_url=OTC_MARKETS_SECURITY_PROFILE_PAGE_URL,
        format="otc_markets_security_profile_json",
        reference_scope="security_lookup_subset",
    ),
    MasterfileSource(
        key="otc_markets_stock_screener",
        provider="OTC Markets",
        description="Official OTC Markets stock screener CSV export excluding Grey and Expert Market access",
        source_url=OTC_MARKETS_STOCK_SCREENER_CSV_URL,
        format="otc_markets_stock_screener_csv",
        reference_scope="exchange_directory",
    ),
]


def ensure_output_dirs() -> None:
    MASTERFILES_DIR.mkdir(parents=True, exist_ok=True)
    MASTERFILE_CACHE_DIR.mkdir(parents=True, exist_ok=True)


def write_csv(path: Path, fieldnames: list[str], rows: Iterable[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def load_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def normalize_source_keys(values: Iterable[str] | None) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for value in values or ():
        for item in str(value).split(","):
            key = item.strip()
            if not key or key in seen:
                continue
            seen.add(key)
            normalized.append(key)
    return normalized


def select_official_sources(source_keys: Iterable[str] | None = None) -> list[MasterfileSource]:
    requested_keys = normalize_source_keys(source_keys)
    if not requested_keys:
        return list(OFFICIAL_SOURCES)

    available = {source.key: source for source in OFFICIAL_SOURCES}
    unknown = [key for key in requested_keys if key not in available]
    if unknown:
        available_keys = ", ".join(sorted(available))
        unknown_keys = ", ".join(sorted(unknown))
        raise ValueError(f"Unknown source key(s): {unknown_keys}. Available keys: {available_keys}")
    return [available[key] for key in requested_keys]


def merge_reference_rows(
    existing_rows: Iterable[dict[str, str]],
    refreshed_rows: Iterable[dict[str, str]],
    *,
    source_keys: Iterable[str],
) -> list[dict[str, str]]:
    selected_source_keys = set(source_keys)
    preserved_rows = [row for row in existing_rows if row.get("source_key", "") not in selected_source_keys]
    return dedupe_rows([*preserved_rows, *refreshed_rows])


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
    return latest_verification_tickers(base_dir, exchanges=exchanges, statuses={"reference_gap"})


def latest_verification_tickers(
    base_dir: Path,
    *,
    exchanges: set[str] | None = None,
    statuses: set[str] | None = None,
) -> set[str]:
    latest_run = latest_verification_run(base_dir)
    if latest_run is None:
        return set()
    tickers: set[str] = set()
    for path in sorted(latest_run.glob("chunk-*-of-*.csv")):
        with path.open(newline="", encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                if statuses and row.get("status") not in statuses:
                    continue
                exchange = row.get("exchange", "")
                ticker = row.get("ticker", "").strip()
                if exchanges and exchange not in exchanges:
                    continue
                if ticker:
                    tickers.add(ticker)
    return tickers


def missing_isin_listing_tickers(
    *,
    exchanges: set[str],
    asset_types: set[str],
    listings_path: Path = LISTINGS_CSV,
) -> set[str]:
    return {
        row.get("ticker", "").strip()
        for row in load_csv(listings_path)
        if row.get("exchange") in exchanges
        and row.get("asset_type") in asset_types
        and row.get("ticker", "").strip()
        and not row.get("isin", "").strip()
    }


def jse_instrument_search_target_tickers(
    *,
    listings_path: Path = LISTINGS_CSV,
    verification_dir: Path = STOCK_VERIFICATION_DIR,
) -> list[str]:
    reference_gap_tickers = latest_reference_gap_tickers(verification_dir, exchanges={"JSE"})
    current_gap_tickers = {
        row.get("ticker", "").strip()
        for row in load_csv(listings_path)
        if row.get("exchange") == "JSE"
        and row.get("asset_type") == "Stock"
        and row.get("ticker", "").strip()
        and (
            row.get("ticker", "").strip() in reference_gap_tickers
            or not row.get("isin", "").strip()
            or not (row.get("stock_sector", "").strip() or row.get("sector", "").strip())
        )
    }
    return sorted(current_gap_tickers)


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


def extract_bmv_json_wrapper_payload(text: str) -> Any:
    marker_index = text.find("for(;;);")
    if marker_index != -1:
        text = text[marker_index + len("for(;;);") :].strip()
    if text.startswith("("):
        text = text[1:]
    closing_index = text.rfind(")")
    if closing_index != -1:
        text = text[:closing_index]
    return json.loads(text)


def bmv_compose_reference_ticker(clave: str, serie: str) -> str:
    normalized_clave = clave.strip().upper()
    normalized_serie = serie.strip().upper().replace(" ", "").replace("*", "")
    return f"{normalized_clave}{normalized_serie}"


def bmv_search_target_rows(
    source: MasterfileSource,
    *,
    listings_path: Path = LISTINGS_CSV,
    verification_dir: Path = STOCK_VERIFICATION_DIR,
) -> list[dict[str, str]]:
    config = BMV_SEARCH_SOURCE_CONFIG[source.key]
    target_tickers = latest_verification_tickers(
        verification_dir,
        exchanges={config["exchange"]},
        statuses={"reference_gap", "missing_from_official"},
    )
    asset_type = config.get("asset_type", "Stock")
    sector_fields = ("etf_category", "sector") if asset_type == "ETF" else ("stock_sector", "sector")
    return [
        row
        for row in load_csv(listings_path)
        if row.get("exchange") == config["exchange"]
        and row.get("asset_type") == asset_type
        and row.get("ticker", "").strip()
        and (
            row.get("ticker", "").strip().upper() in target_tickers
            or not row.get("isin", "").strip()
            or not any(row.get(field, "").strip() for field in sector_fields)
        )
    ]


def compact_company_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def has_strong_company_name_match(left: str, right: str) -> bool:
    if not alias_matches_company(left, right):
        return False
    left_compact = compact_company_name(left)
    right_compact = compact_company_name(right)
    if left_compact and right_compact and (
        left_compact in right_compact or right_compact in left_compact
    ):
        return True
    return len(normalize_tokens(left) & normalize_tokens(right)) >= 2


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


def tmx_stock_quote_symbol_variants(ticker: str) -> list[str]:
    normalized = ticker.strip().upper()
    if not normalized:
        return []
    variants: list[str] = []
    if "-" in normalized:
        variants.append(normalized.replace("-", "."))
        variants.append(normalized.split("-", 1)[0])
    variants.append(normalized)
    seen: set[str] = set()
    deduped: list[str] = []
    for variant in variants:
        if variant in seen:
            continue
        seen.add(variant)
        deduped.append(variant)
    return deduped


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


def tpex_infohub_request_headers(referer: str = TPEX_ETF_FILTER_PAGE_URL) -> dict[str, str]:
    return {
        "User-Agent": USER_AGENT,
        "Accept": "application/json,text/javascript,*/*; q=0.01",
        "Accept-Encoding": "gzip, deflate",
        "Referer": referer,
        "Origin": "https://info.tpex.org.tw",
        "X-Requested-With": "XMLHttpRequest",
    }


def spotlight_request_headers(referer: str = SPOTLIGHT_COMPANIES_PAGE_URL) -> dict[str, str]:
    return {
        "User-Agent": USER_AGENT,
        "Accept": "application/json,text/plain,*/*",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": referer,
        "X-Requested-With": "XMLHttpRequest",
    }


def krx_request_headers() -> dict[str, str]:
    return {
        "User-Agent": USER_AGENT,
        "Accept": "application/json,text/plain,*/*",
        "Accept-Encoding": "gzip, deflate",
        "Referer": KRX_LISTED_COMPANIES_URL,
        "Origin": "https://global.krx.co.kr",
    }


def fetch_krx_finder_records(
    bld: str,
    *,
    mktsel: str = "ALL",
    search_text: str = "",
    session: requests.Session | None = None,
) -> list[dict[str, Any]]:
    session = session or requests.Session()
    response = session.post(
        KRX_JSON_DATA_URL,
        data={
            "bld": bld,
            "mktsel": mktsel,
            "searchText": search_text,
        },
        headers={
            "User-Agent": USER_AGENT,
            "Referer": "https://data.krx.co.kr/contents/MDC/MAIN/main/index.cmd",
        },
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return response.json().get("block1", [])


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


def nasdaq_nordic_request_headers(referer: str = NASDAQ_NORDIC_STOCK_PAGE_URL) -> dict[str, str]:
    return {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json,text/plain,*/*",
        "Accept-Encoding": "gzip, deflate",
        "Referer": referer,
        "Origin": "https://www.nasdaq.com",
    }


def nasdaq_us_request_headers(referer: str = NASDAQ_MUTUAL_FUND_PAGE_URL) -> dict[str, str]:
    return {
        "User-Agent": BROWSER_USER_AGENT,
        "Accept": "application/json,text/plain,*/*",
        "Referer": referer,
        "Origin": "https://www.nasdaq.com",
    }


def idx_request_headers(referer: str = IDX_STOCK_LIST_PAGE_URL) -> dict[str, str]:
    return {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/plain, */*",
        "Referer": referer,
        "Origin": "https://www.idx.id",
    }


def tase_market_request_headers(base_headers: dict[str, str] | None = None) -> dict[str, str]:
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json;charset=UTF-8",
        "Referer": "https://market.tase.co.il/",
        "Origin": "https://market.tase.co.il",
        "Sec-Fetch-Site": "same-site",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Dest": "empty",
    }
    for key, value in (base_headers or {}).items():
        if value and key.lower() != "cookie":
            headers[key] = value
    return headers


def hose_request_headers() -> dict[str, str]:
    return {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://www.hsx.vn/",
        "Origin": "https://www.hsx.vn",
        "Sec-Fetch-Site": "same-site",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Dest": "empty",
    }


def hnx_request_headers(referer: str = "https://www.hnx.vn/") -> dict[str, str]:
    return {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": referer,
        "Origin": "https://www.hnx.vn",
        "X-Requested-With": "XMLHttpRequest",
    }


def vsdc_request_headers(*, token: str = "", referer: str = VSDC_TOKEN_SEED_URL) -> dict[str, str]:
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/plain, */*",
        "Referer": referer,
        "Origin": VSDC_BASE_URL,
        "X-Requested-With": "XMLHttpRequest",
    }
    if token:
        headers["__VPToken"] = token
    return headers


def bme_request_headers(referer: str = BME_COMPANIES_SEARCH_PAGE_URL) -> dict[str, str]:
    return {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/plain, */*",
        "Referer": referer,
        "Origin": "https://www.bolsasymercados.es",
        "X-Requested-With": "XMLHttpRequest",
        "Sec-Fetch-Site": "same-site",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Dest": "empty",
    }


def bme_growth_request_headers(referer: str = BME_GROWTH_PRICES_URL) -> dict[str, str]:
    return {
        "User-Agent": BROWSER_USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": referer,
    }


def bme_growth_request_with_retries(
    session: requests.Session,
    method: str,
    url: str,
    **kwargs: Any,
) -> requests.Response:
    response = None
    for attempt in range(4):
        response = getattr(session, method)(url, **kwargs)
        if getattr(response, "status_code", 200) < 500:
            break
        if attempt < 3:
            time.sleep(BME_GROWTH_RETRY_DELAY_SECONDS)
    assert response is not None
    return response


def normalize_nasdaq_nordic_search_symbol(value: str) -> str:
    return "".join(ch for ch in str(value or "").upper() if ch.isalnum())


def extract_jse_exchange_traded_product_download_url(page_html: str, product_type: str) -> str | None:
    product_type = product_type.strip().upper()
    if product_type == "ETF":
        matches = JSE_ETF_LIST_HREF_RE.findall(page_html)
    elif product_type == "ETN":
        matches = JSE_ETN_LIST_HREF_RE.findall(page_html)
    else:
        raise ValueError(f"Unsupported JSE product type: {product_type}")
    return matches[-1] if matches else None


def extract_jse_instrument_search_links(page_html: str) -> list[str]:
    links: list[str] = []
    for href in JSE_INSTRUMENT_LINK_RE.findall(page_html):
        absolute_url = requests.compat.urljoin(JSE_SEARCH_URL, href)
        if absolute_url not in links:
            links.append(absolute_url)
    return links


def extract_jse_instrument_profile_values(page_html: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for match in JSE_INSTRUMENT_PROFILE_VALUE_RE.finditer(page_html):
        field = match.group("field").lower()
        value = " ".join(unescape(strip_html_tags(match.group("value"))).split())
        if value:
            values[field] = value
    return values


def extract_jse_instrument_metadata(page_html: str) -> dict[str, str] | None:
    match = JSE_INSTRUMENT_META_RE.search(page_html)
    if not match:
        return None
    profile_values = extract_jse_instrument_profile_values(page_html)
    sector = normalize_sector(profile_values.get("industry", ""), "Stock") or normalize_sector(
        profile_values.get("sector", ""),
        "Stock",
    )
    metadata = {
        "name": " ".join(unescape(match.group("name")).split()),
        "code": match.group("code").strip().upper(),
        "instrument_type": " ".join(unescape(match.group("instrument_type")).split()),
    }
    if sector:
        metadata["sector"] = sector
    return metadata



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


def otc_markets_security_profile_targets(
    listings_path: Path = LISTINGS_CSV,
) -> list[dict[str, str]]:
    rows = [
        row
        for row in load_csv(listings_path)
        if row.get("exchange") == "OTC" and not row.get("isin", "").strip()
    ]
    return sorted(rows, key=lambda row: row.get("ticker", ""))


def fetch_otc_markets_profile_payload(
    symbol: str,
    session: requests.Session | None = None,
) -> dict[str, Any] | None:
    url = OTC_MARKETS_SECURITY_PROFILE_API_URL.format(symbol=symbol)
    request_session = session or requests
    try:
        response = request_session.get(
            url,
            params={"symbol": symbol},
            headers=OTC_MARKETS_PROFILE_HEADERS,
            timeout=OTC_MARKETS_PROFILE_TIMEOUT,
        )
        response.raise_for_status()
    except requests.RequestException:
        return None
    if not response.headers.get("content-type", "").startswith("application/json"):
        return None
    payload = response.json()
    return payload if isinstance(payload, dict) else None


def fetch_otc_markets_security_profile(
    source: MasterfileSource,
    session: requests.Session | None = None,
    listings_path: Path = LISTINGS_CSV,
) -> list[dict[str, str]]:
    targets = otc_markets_security_profile_targets(listings_path)

    def fetch_one(listing_row: dict[str, str]) -> list[dict[str, str]]:
        symbol = listing_row["ticker"].strip().upper()
        payload = fetch_otc_markets_profile_payload(symbol, session=session if session is not None else None)
        if not payload:
            return []
        return parse_otc_markets_security_profile(payload, source, listing_row)

    if session is not None:
        return [row for target in targets for row in fetch_one(target)]

    rows: list[dict[str, str]] = []
    with ThreadPoolExecutor(max_workers=OTC_MARKETS_PROFILE_MAX_WORKERS) as executor:
        futures = [executor.submit(fetch_one, target) for target in targets]
        for future in as_completed(futures):
            rows.extend(future.result())
    return sorted(rows, key=lambda row: row["ticker"])


def load_otc_markets_security_profile_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    for path in (OTC_MARKETS_SECURITY_PROFILE_CACHE, LEGACY_OTC_MARKETS_SECURITY_PROFILE_CACHE):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"

    rows = fetch_otc_markets_security_profile(source, session=session)
    ensure_output_dirs()
    OTC_MARKETS_SECURITY_PROFILE_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def otc_markets_screener_headers() -> dict[str, str]:
    return {
        "Accept": "text/csv,*/*",
        "Referer": OTC_MARKETS_STOCK_SCREENER_PAGE_URL,
        "User-Agent": BROWSER_USER_AGENT,
    }


def infer_otc_markets_stock_screener_asset_type(sec_type: str) -> str:
    normalized = sec_type.strip().lower()
    if normalized == "etfs":
        return "ETF"
    if normalized in OTC_MARKETS_STOCK_SCREENER_STOCK_TYPES:
        return "Stock"
    return ""


def parse_otc_markets_stock_screener_csv(text: str, source: MasterfileSource) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for record in csv.DictReader(io.StringIO(text)):
        ticker = str(record.get("Symbol", "")).strip().upper()
        name = str(record.get("Security Name", "")).strip()
        sec_type = str(record.get("Sec Type", "")).strip()
        asset_type = infer_otc_markets_stock_screener_asset_type(sec_type)
        if not ticker or not name or not asset_type or ticker in seen:
            continue
        seen.add(ticker)
        rows.append(
            {
                "source_key": source.key,
                "provider": source.provider,
                "source_url": source.source_url,
                "ticker": ticker,
                "name": name,
                "exchange": "OTC",
                "asset_type": asset_type,
                "listing_status": "active",
                "reference_scope": source.reference_scope,
                "official": "true",
            }
        )
    return rows


def fetch_otc_markets_stock_screener_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    request_session = session or requests.Session()
    response = request_session.get(
        source.source_url,
        headers=otc_markets_screener_headers(),
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    text = response.text
    return parse_otc_markets_stock_screener_csv(text, source)


def load_otc_markets_stock_screener_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    for path in (OTC_MARKETS_STOCK_SCREENER_CACHE, LEGACY_OTC_MARKETS_STOCK_SCREENER_CACHE):
        if path.exists():
            return parse_otc_markets_stock_screener_csv(path.read_text(encoding="utf-8"), source), "cache"

    try:
        request_session = session or requests.Session()
        response = request_session.get(
            source.source_url,
            headers=otc_markets_screener_headers(),
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        text = response.text
    except requests.RequestException:
        return None, "unavailable"

    ensure_output_dirs()
    OTC_MARKETS_STOCK_SCREENER_CACHE.write_text(text, encoding="utf-8")
    return parse_otc_markets_stock_screener_csv(text, source), "network"


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


def load_tpex_etf_filter_payload(
    session: requests.Session | None = None,
) -> tuple[dict[str, Any] | list[dict[str, Any]] | None, str]:
    for path in (TPEX_ETF_FILTER_CACHE, LEGACY_TPEX_ETF_FILTER_CACHE):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"

    session = session or requests.Session()
    try:
        response = session.post(
            f"{TPEX_ETF_FILTER_API_URL}?lang=en-us",
            headers=tpex_infohub_request_headers(),
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        payload = response.json()
    except requests.RequestException:
        return None, "unavailable"

    ensure_output_dirs()
    TPEX_ETF_FILTER_CACHE.write_text(json.dumps(payload), encoding="utf-8")
    return payload, "network"


def load_tpex_emerging_basic_info_text(
    session: requests.Session | None = None,
) -> tuple[str | None, str]:
    for path in (TPEX_EMERGING_BASIC_INFO_CACHE, LEGACY_TPEX_EMERGING_BASIC_INFO_CACHE):
        if path.exists():
            return path.read_text(encoding="utf-8-sig"), "cache"

    session = session or requests.Session()
    try:
        response = session.get(
            TPEX_EMERGING_BASIC_INFO_URL,
            headers={"User-Agent": "Mozilla/5.0", "Accept": "text/csv,*/*"},
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
    except requests.RequestException:
        return None, "unavailable"

    text = response.content.decode("utf-8-sig")
    ensure_output_dirs()
    TPEX_EMERGING_BASIC_INFO_CACHE.write_text(text, encoding="utf-8-sig")
    return text, "network"


def load_tpex_mainboard_basic_info_text(
    session: requests.Session | None = None,
) -> tuple[str | None, str]:
    for path in (TPEX_MAINBOARD_BASIC_INFO_CACHE, LEGACY_TPEX_MAINBOARD_BASIC_INFO_CACHE):
        if path.exists():
            return path.read_text(encoding="utf-8-sig"), "cache"

    session = session or requests.Session()
    try:
        response = session.get(
            TPEX_MAINBOARD_BASIC_INFO_URL,
            headers={"User-Agent": "Mozilla/5.0", "Accept": "text/csv,*/*"},
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
    except requests.RequestException:
        return None, "unavailable"

    text = response.content.decode("utf-8-sig")
    ensure_output_dirs()
    TPEX_MAINBOARD_BASIC_INFO_CACHE.write_text(text, encoding="utf-8-sig")
    return text, "network"


def load_set_etf_search_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    try:
        rows = fetch_set_etf_search(source, session=session)
    except (requests.RequestException, ValueError, json.JSONDecodeError):
        for path in (SET_ETF_SEARCH_CACHE, LEGACY_SET_ETF_SEARCH_CACHE):
            if path.exists():
                return json.loads(path.read_text(encoding="utf-8")), "cache"
        return None, "unavailable"

    ensure_output_dirs()
    SET_ETF_SEARCH_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def load_set_dr_search_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    try:
        rows = fetch_set_dr_search(source, session=session)
    except (requests.RequestException, ValueError, json.JSONDecodeError):
        for path in (SET_DR_SEARCH_CACHE, LEGACY_SET_DR_SEARCH_CACHE):
            if path.exists():
                return json.loads(path.read_text(encoding="utf-8")), "cache"
        return None, "unavailable"

    ensure_output_dirs()
    SET_DR_SEARCH_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def load_set_stock_search_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    try:
        rows = fetch_set_stock_search(source, session=session)
    except (ImportError, RuntimeError, requests.RequestException, ValueError, json.JSONDecodeError):
        for path in (SET_STOCK_SEARCH_CACHE, LEGACY_SET_STOCK_SEARCH_CACHE):
            if path.exists():
                return parse_set_stock_search_payload(
                    json.loads(path.read_text(encoding="utf-8")),
                    source,
                ), "cache"
        return None, "unavailable"

    ensure_output_dirs()
    SET_STOCK_SEARCH_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return parse_set_stock_search_payload(rows, source), "network"


def load_psx_dps_symbols_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    try:
        payload = fetch_psx_dps_symbols(source, session=session)
    except (requests.RequestException, ValueError, json.JSONDecodeError):
        for path in (PSX_DPS_SYMBOLS_CACHE, LEGACY_PSX_DPS_SYMBOLS_CACHE):
            if path.exists():
                return parse_psx_dps_symbols_payload(
                    json.loads(path.read_text(encoding="utf-8")),
                    source,
                ), "cache"
        return None, "unavailable"

    ensure_output_dirs()
    PSX_DPS_SYMBOLS_CACHE.write_text(json.dumps(payload), encoding="utf-8")
    return parse_psx_dps_symbols_payload(payload, source), "network"


def parse_pse_listed_company_directory_html(text: str, source: MasterfileSource) -> list[dict[str, str]]:
    match = re.search(r'id="store-json"[^>]*value="([^"]+)"', text)
    if match is None:
        raise ValueError("PSE listed company directory JSON payload missing")

    payload = json.loads(unescape(match.group(1)))
    rows: list[dict[str, str]] = []
    seen_tickers: set[str] = set()
    for item in payload:
        ticker = str(item.get("SecuritySymbol", "")).strip().upper()
        name = str(item.get("SecurityName", "")).strip() or str(item.get("SecurityAlias", "")).strip()
        security_type = str(item.get("SecurityType", "")).strip().upper()
        security_status = str(item.get("SecurityStatus", "")).strip().upper()
        asset_type = PSE_SECURITY_TYPE_TO_ASSET_TYPE.get(security_type)
        if not ticker or not name or asset_type is None or security_status not in PSE_ACTIVE_SECURITY_STATUSES:
            continue
        if ticker in seen_tickers:
            continue

        row = {
            "source_key": source.key,
            "provider": source.provider,
            "source_url": source.source_url,
            "ticker": ticker,
            "name": name,
            "exchange": "PSE",
            "asset_type": asset_type,
            "listing_status": "active",
            "reference_scope": source.reference_scope,
            "official": "true",
        }
        isin = str(item.get("SecurityISIN", "")).strip().upper()
        if isin:
            row["isin"] = isin
        rows.append(row)
        seen_tickers.add(ticker)
    return rows


def fetch_pse_listed_company_directory(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    session = session or requests.Session()
    response = session.get(
        source.source_url,
        headers={
            "User-Agent": USER_AGENT,
            "Referer": PSE_LISTED_COMPANY_DIRECTORY_PAGE_URL,
        },
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return parse_pse_listed_company_directory_html(response.text, source)


def load_pse_listed_company_directory_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    try:
        rows = fetch_pse_listed_company_directory(source, session=session)
    except (requests.RequestException, ValueError, json.JSONDecodeError):
        for path in (PSE_LISTED_COMPANY_DIRECTORY_CACHE, LEGACY_PSE_LISTED_COMPANY_DIRECTORY_CACHE):
            if path.exists():
                return json.loads(path.read_text(encoding="utf-8")), "cache"
        return None, "unavailable"

    ensure_output_dirs()
    PSE_LISTED_COMPANY_DIRECTORY_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def parse_pse_cz_share_links(text: str, page_url: str) -> list[dict[str, str]]:
    links: list[dict[str, str]] = []
    seen: set[str] = set()
    for match in PSE_CZ_SHARE_LINK_RE.finditer(text):
        isin = match.group("isin").strip().upper()
        if not is_valid_isin(isin) or isin in seen:
            continue
        links.append(
            {
                "isin": isin,
                "label": clean_html_text(match.group("label")).upper(),
                "detail_url": urljoin(page_url, match.group("href")),
            }
        )
        seen.add(isin)
    return links


def parse_pse_cz_detail_ticker(text: str) -> str:
    for pattern in (PSE_CZ_TICKER_XETRA_RE, PSE_CZ_TICKER_REUTERS_RE, PSE_CZ_TICKER_BLOOMBERG_RE):
        match = pattern.search(text)
        if match is None:
            continue
        ticker = clean_html_text(match.group("ticker")).upper()
        ticker = ticker.split()[0]
        ticker = ticker.split(".", 1)[0]
        if ticker and re.fullmatch(r"[A-Z0-9&.-]+", ticker):
            return ticker
    return ""


def listing_rows_by_exchange_ticker(exchange: str, listings_path: Path = LISTINGS_CSV) -> dict[str, dict[str, str]]:
    rows: dict[str, dict[str, str]] = {}
    for row in load_csv(listings_path):
        if row.get("exchange") != exchange:
            continue
        ticker = row.get("ticker", "").strip().upper()
        if ticker:
            rows.setdefault(ticker, row)
    return rows


def build_pse_cz_share_rows(
    share_links: list[dict[str, str]],
    detail_html_by_isin: dict[str, str],
    source: MasterfileSource,
    *,
    listings_path: Path = LISTINGS_CSV,
) -> list[dict[str, str]]:
    listings_by_isin = listing_rows_by_exchange_isin("PSE_CZ", listings_path=listings_path)
    listings_by_ticker = listing_rows_by_exchange_ticker("PSE_CZ", listings_path=listings_path)
    rows: list[dict[str, str]] = []
    seen: set[str] = set()

    for link in share_links:
        isin = link["isin"].strip().upper()
        detail_html = detail_html_by_isin.get(isin, "")
        ticker = parse_pse_cz_detail_ticker(detail_html) or link.get("label", "").strip().upper()
        if not ticker or ticker in seen or not is_valid_isin(isin):
            continue
        listing = listings_by_ticker.get(ticker) or listings_by_isin.get(isin)
        rows.append(
            {
                "source_key": source.key,
                "provider": source.provider,
                "source_url": link["detail_url"],
                "ticker": ticker,
                "name": (listing or {}).get("name") or link.get("label", ticker),
                "exchange": "PSE_CZ",
                "asset_type": "Stock",
                "listing_status": "active",
                "reference_scope": source.reference_scope,
                "official": "true",
                "isin": isin,
            }
        )
        seen.add(ticker)
    return rows


def fetch_pse_cz_shares_directory(
    source: MasterfileSource,
    *,
    session: requests.Session | None = None,
    listings_path: Path = LISTINGS_CSV,
) -> list[dict[str, str]]:
    session = session or requests.Session()
    share_links: list[dict[str, str]] = []
    seen_isins: set[str] = set()
    for page_url in PSE_CZ_SHARES_MARKET_URLS:
        response = session.get(page_url, headers={"User-Agent": USER_AGENT}, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        for link in parse_pse_cz_share_links(response.text, page_url):
            if link["isin"] in seen_isins:
                continue
            share_links.append(link)
            seen_isins.add(link["isin"])

    detail_html_by_isin: dict[str, str] = {}
    for link in share_links:
        response = session.get(link["detail_url"], headers={"User-Agent": USER_AGENT}, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        detail_html_by_isin[link["isin"]] = response.text

    return build_pse_cz_share_rows(
        share_links,
        detail_html_by_isin,
        source,
        listings_path=listings_path,
    )


def load_pse_cz_shares_directory_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    for path in (PSE_CZ_SHARES_DIRECTORY_CACHE, LEGACY_PSE_CZ_SHARES_DIRECTORY_CACHE):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"
    try:
        rows = fetch_pse_cz_shares_directory(source, session=session)
    except (requests.RequestException, ValueError, json.JSONDecodeError):
        return None, "unavailable"

    ensure_output_dirs()
    PSE_CZ_SHARES_DIRECTORY_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def clean_html_text(value: str) -> str:
    return " ".join(unescape(strip_html_tags(value)).split())


def listing_rows_by_exchange_isin(exchange: str, listings_path: Path = LISTINGS_CSV) -> dict[str, dict[str, str]]:
    rows: dict[str, dict[str, str]] = {}
    for row in load_csv(listings_path):
        if row.get("exchange") != exchange:
            continue
        isin = row.get("isin", "").strip().upper()
        if isin and is_valid_isin(isin):
            rows.setdefault(isin, row)
    return rows


def bse_bw_listing_match_key(value: str) -> str:
    return compact_company_name(value)


def find_bse_bw_listing_for_name(
    name: str,
    current_rows: list[dict[str, str]],
) -> dict[str, str] | None:
    official_key = bse_bw_listing_match_key(name)
    if not official_key:
        return None
    for row in current_rows:
        row_key = bse_bw_listing_match_key(row.get("name", ""))
        if row_key and row_key == official_key:
            return row
    for row in current_rows:
        row_key = bse_bw_listing_match_key(row.get("name", ""))
        if row_key and len(row_key) >= 5 and row_key in official_key:
            return row
        if row_key and len(official_key) >= 8 and official_key in row_key:
            return row
    official_tokens = {token.rstrip("s") for token in normalize_tokens(name) if len(token) >= 4}
    for row in current_rows:
        row_tokens = {
            token.rstrip("s")
            for token in normalize_tokens(row.get("name", ""))
            if len(token) >= 4
        }
        if len(official_tokens & row_tokens) >= 2:
            return row
    return None


def parse_bse_bw_listed_companies_html(
    text: str,
    source: MasterfileSource,
    *,
    listings_path: Path = LISTINGS_CSV,
) -> list[dict[str, str]]:
    current_rows = [
        row
        for row in load_csv(listings_path)
        if row.get("exchange") == "BSE_BW"
    ]
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for match in BSE_BW_PANEL_RE.finditer(text):
        name = clean_html_text(match.group("title"))
        fields = {
            clean_html_text(field.group("label")).lower(): clean_html_text(field.group("value"))
            for field in HTML_TH_TD_ROW_RE.finditer(match.group("body"))
        }
        counter = fields.get("counter", "").strip().upper()
        sector = fields.get("sector", "").strip()
        listing = find_bse_bw_listing_for_name(name, current_rows)
        if not listing:
            continue
        ticker = listing.get("ticker", "").strip().upper() or counter
        if not ticker or ticker in seen or not name:
            continue
        row = {
            "source_key": source.key,
            "provider": source.provider,
            "source_url": source.source_url,
            "ticker": ticker,
            "name": (listing or {}).get("name") or name,
            "exchange": "BSE_BW",
            "asset_type": (listing or {}).get("asset_type") or "Stock",
            "listing_status": "active",
            "reference_scope": source.reference_scope,
            "official": "true",
        }
        if (listing or {}).get("isin"):
            row["isin"] = listing["isin"]
        if sector:
            row["sector"] = sector
        rows.append(row)
        seen.add(ticker)
    return rows


def fetch_bse_bw_listed_companies(
    source: MasterfileSource,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    text = fetch_text(source.source_url, session=session)
    return parse_bse_bw_listed_companies_html(text, source)


def load_bse_bw_listed_companies_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    for path in (BSE_BW_LISTED_COMPANIES_CACHE, LEGACY_BSE_BW_LISTED_COMPANIES_CACHE):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"
    try:
        rows = fetch_bse_bw_listed_companies(source, session=session)
    except (requests.RequestException, ValueError):
        return None, "unavailable"

    ensure_output_dirs()
    BSE_BW_LISTED_COMPANIES_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def extract_bse_hu_datasource_payload(text: str) -> dict[str, Any]:
    marker = "window.dataSourceResults="
    marker_index = text.find(marker)
    if marker_index == -1:
        raise ValueError("BSE HU dataSourceResults payload not found")
    body_start = text.find("{", marker_index + len(marker))
    if body_start == -1:
        raise ValueError("BSE HU dataSourceResults JSON start not found")
    body_end = find_javascript_block_end(text, body_start, open_char="{", close_char="}")
    payload = json.loads(text[body_start : body_end + 1])
    if not isinstance(payload, dict):
        raise ValueError("BSE HU dataSourceResults payload is not an object")
    return payload


def bse_hu_datasource_records(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for value in payload.values():
        candidate_rows: list[Any] = []
        if isinstance(value, list):
            candidate_rows = value
        elif isinstance(value, dict) and isinstance(value.get("rows"), list):
            candidate_rows = value["rows"]
        for record in candidate_rows:
            if not isinstance(record, dict):
                continue
            ticker = str(record.get("seccode") or "").strip().upper()
            isin = str(record.get("isin") or "").strip().upper()
            if not ticker or ticker in seen or not is_valid_isin(isin):
                continue
            rows.append(record)
            seen.add(ticker)
    return rows


def parse_bse_hu_marketdata_html(
    text: str,
    source: MasterfileSource,
    *,
    listings_path: Path = LISTINGS_CSV,
) -> list[dict[str, str]]:
    current_rows_by_ticker = {
        row.get("ticker", "").strip().upper(): row
        for row in load_csv(listings_path)
        if row.get("exchange") == "BSE_HU" and row.get("ticker", "").strip()
    }
    rows: list[dict[str, str]] = []
    for record in bse_hu_datasource_records(extract_bse_hu_datasource_payload(text)):
        ticker = str(record.get("seccode") or "").strip().upper()
        isin = str(record.get("isin") or "").strip().upper()
        listing = current_rows_by_ticker.get(ticker)
        if not listing:
            continue
        name = listing.get("name", "").strip()
        if not name:
            continue
        rows.append(
            {
                "source_key": source.key,
                "provider": source.provider,
                "source_url": source.source_url,
                "ticker": ticker,
                "name": name,
                "exchange": "BSE_HU",
                "asset_type": listing.get("asset_type", "") or infer_asset_type(name),
                "listing_status": "active",
                "reference_scope": source.reference_scope,
                "official": "true",
                "isin": isin,
            }
        )
    return rows


def fetch_bse_hu_marketdata_rows(
    source: MasterfileSource,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    text = fetch_text(source.source_url, session=session)
    return parse_bse_hu_marketdata_html(text, source)


def load_bse_hu_marketdata_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    for path in (BSE_HU_MARKETDATA_CACHE, LEGACY_BSE_HU_MARKETDATA_CACHE):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"
    try:
        rows = fetch_bse_hu_marketdata_rows(source, session=session)
    except (requests.RequestException, ValueError, json.JSONDecodeError):
        return None, "unavailable"

    ensure_output_dirs()
    BSE_HU_MARKETDATA_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def normalize_egx_sector(value: str) -> str:
    sector = clean_html_text(value)
    return EGX_SECTOR_MAP.get(sector.lower(), "")


def extract_egx_viewstate(text: str) -> str:
    match = EGX_VIEWSTATE_RE.search(text)
    if not match:
        raise ValueError("EGX __VIEWSTATE input not found")
    value_match = EGX_VIEWSTATE_VALUE_RE.search(match.group("tag"))
    if not value_match:
        raise ValueError("EGX __VIEWSTATE value not found")
    return unescape(value_match.group("value"))


def extract_aspnet_viewstate_strings(viewstate: str) -> list[str]:
    try:
        raw = b64decode(viewstate)
    except ValueError as exc:
        raise ValueError("EGX __VIEWSTATE is not valid base64") from exc

    strings: list[str] = []
    index = 0
    while index < len(raw) - 2:
        if raw[index] != 0x05:
            index += 1
            continue
        length = raw[index + 1]
        start = index + 2
        end = start + length
        if length <= 0 or end > len(raw):
            index += 1
            continue
        value_bytes = raw[start:end]
        try:
            value = value_bytes.decode("utf-8")
        except UnicodeDecodeError:
            index += 1
            continue
        if all(char >= " " and char != "\x7f" for char in value):
            strings.append(value)
            index = end
            continue
        index += 1
    return strings


def parse_egx_listed_stocks_viewstate(
    text: str,
    source: MasterfileSource,
    *,
    listings_path: Path = LISTINGS_CSV,
) -> list[dict[str, str]]:
    current_by_isin = listing_rows_by_exchange_isin("EGX", listings_path=listings_path)
    if not current_by_isin:
        return []

    strings = extract_aspnet_viewstate_strings(extract_egx_viewstate(text))
    rows_by_isin: dict[str, dict[str, str]] = {}
    for index, name_value in enumerate(strings[:-2]):
        if name_value.startswith("show("):
            continue
        if not strings[index + 1].startswith(EGX_GRID_ROW_MARKER):
            continue

        name = clean_html_text(name_value)
        window = strings[index : index + 18]
        isin = ""
        sector = ""
        for offset, value in enumerate(window):
            match = EGX_LISTED_STOCKS_DATA_ISIN_RE.search(value)
            if not match:
                continue
            isin = match.group(1).upper()
            for sector_index in range(offset + 1, min(len(window) - 1, offset + 10)):
                if window[sector_index].strip().upper() == isin:
                    sector = normalize_egx_sector(window[sector_index + 1])
                    break
            break

        if not name or not is_valid_isin(isin) or isin not in current_by_isin:
            continue
        rows_by_isin.setdefault(
            isin,
            build_current_listing_reference_row(
                source,
                current_by_isin[isin],
                name=name,
                isin=isin,
                sector=sector,
            ),
        )

    return sorted(rows_by_isin.values(), key=lambda row: row["ticker"])


def fetch_egx_listed_stocks_html_with_browser(source: MasterfileSource) -> str:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:  # pragma: no cover - depends on local browser tooling
        raise requests.RequestException("Playwright is required for EGX browser capture") from exc

    profile_dir = Path(tempfile.mkdtemp(prefix="egx-chrome-"))
    context = None
    try:
        with sync_playwright() as playwright:
            launch_kwargs = {
                "headless": True,
                "args": ["--disable-blink-features=AutomationControlled"],
                "user_agent": BROWSER_USER_AGENT,
                "locale": "en-US",
                "viewport": {"width": 1440, "height": 1000},
            }
            try:
                context = playwright.chromium.launch_persistent_context(
                    str(profile_dir),
                    channel="chrome",
                    **launch_kwargs,
                )
            except Exception:
                context = playwright.chromium.launch_persistent_context(str(profile_dir), **launch_kwargs)
            page = context.new_page()
            page.goto(source.source_url, wait_until="domcontentloaded", timeout=60_000)
            for _ in range(24):
                try:
                    html = page.content()
                except Exception:
                    page.wait_for_timeout(2_500)
                    continue
                if "__VIEWSTATE" in html and "GridView2" in html and len(html) > 50_000:
                    return html
                page.wait_for_timeout(2_500)
    except Exception as exc:  # pragma: no cover - network/browser dependent
        raise requests.RequestException("EGX browser capture unavailable") from exc
    finally:
        if context is not None:
            try:
                context.close()
            except Exception:
                pass
        shutil.rmtree(profile_dir, ignore_errors=True)

    raise requests.RequestException("EGX browser capture did not expose listed-stocks ViewState")


def fetch_egx_listed_stocks_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    del session
    text = fetch_egx_listed_stocks_html_with_browser(source)
    rows = parse_egx_listed_stocks_viewstate(text, source)
    if not rows:
        raise ValueError("EGX listed-stocks ViewState did not match current listings")
    return rows


def load_egx_listed_stocks_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    for path in (EGX_LISTED_STOCKS_CACHE, LEGACY_EGX_LISTED_STOCKS_CACHE):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"
    try:
        rows = fetch_egx_listed_stocks_rows(source, session=session)
    except (requests.RequestException, ValueError, json.JSONDecodeError):
        return None, "unavailable"

    ensure_output_dirs()
    EGX_LISTED_STOCKS_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def current_exchange_listings(exchange: str, listings_path: Path = LISTINGS_CSV) -> list[dict[str, str]]:
    return [
        row
        for row in load_csv(listings_path)
        if row.get("exchange") == exchange and row.get("ticker", "").strip()
    ]


def build_current_listing_reference_row(
    source: MasterfileSource,
    listing: dict[str, str],
    *,
    name: str | None = None,
    isin: str | None = None,
    sector: str | None = None,
) -> dict[str, str]:
    row = {
        "source_key": source.key,
        "provider": source.provider,
        "source_url": source.source_url,
        "ticker": listing.get("ticker", "").strip().upper(),
        "name": (name or listing.get("name", "")).strip(),
        "exchange": listing.get("exchange", "").strip(),
        "asset_type": listing.get("asset_type", "").strip() or infer_asset_type(name or listing.get("name", "")),
        "listing_status": "active",
        "reference_scope": source.reference_scope,
        "official": "true",
        "isin": (isin or listing.get("isin", "")).strip().upper(),
    }
    if sector:
        row["sector"] = sector.strip()
    return row


DSE_TZ_VISIBLE_TICKER_OVERRIDES = {
    # DSE uses the current TCPLC code, while the local DB still carries the legacy TCCL ticker.
    "TCPLC": "TCCL",
}


def parse_dse_tz_listed_companies_html(
    text: str,
    source: MasterfileSource,
    *,
    listings_path: Path = LISTINGS_CSV,
) -> list[dict[str, str]]:
    current_by_ticker = {
        row.get("ticker", "").strip().upper(): row for row in current_exchange_listings("DSE_TZ", listings_path)
    }
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for table in pd.read_html(io.StringIO(text)):
        if "Company Code" not in table.columns:
            continue
        for record in table.to_dict("records"):
            official_ticker = str(record.get("Company Code") or "").strip().upper()
            ticker = DSE_TZ_VISIBLE_TICKER_OVERRIDES.get(official_ticker, official_ticker)
            listing = current_by_ticker.get(ticker)
            if not listing or ticker in seen:
                continue
            seen.add(ticker)
            rows.append(build_current_listing_reference_row(source, listing))
    return rows


def fetch_dse_tz_listed_companies(
    source: MasterfileSource,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    session = session or requests.Session()
    response = session.get(source.source_url, headers={"User-Agent": USER_AGENT}, timeout=REQUEST_TIMEOUT, verify=False)
    response.raise_for_status()
    return parse_dse_tz_listed_companies_html(response.text, source)


def load_dse_tz_listed_companies_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    for path in (DSE_TZ_LISTED_COMPANIES_CACHE, LEGACY_DSE_TZ_LISTED_COMPANIES_CACHE):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"
    try:
        rows = fetch_dse_tz_listed_companies(source, session=session)
    except (requests.RequestException, ValueError):
        return None, "unavailable"

    ensure_output_dirs()
    DSE_TZ_LISTED_COMPANIES_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def parse_mse_mw_mainboard_html(
    text: str,
    source: MasterfileSource,
    *,
    listings_path: Path = LISTINGS_CSV,
) -> list[dict[str, str]]:
    current_by_ticker = {
        row.get("ticker", "").strip().upper(): row for row in current_exchange_listings("MSE_MW", listings_path)
    }
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for match in re.finditer(
        r'<a[^>]+href=["\'](?:https://mse\.co\.mw)?/company/([A-Z0-9]{12})["\'][^>]*>(.*?)</a>',
        text,
        re.IGNORECASE | re.DOTALL,
    ):
        isin = match.group(1).strip().upper()
        ticker = re.sub(r"<[^>]+>", "", match.group(2)).strip().upper()
        listing = current_by_ticker.get(ticker)
        if not listing or ticker in seen or not is_valid_isin(isin):
            continue
        seen.add(ticker)
        rows.append(build_current_listing_reference_row(source, listing, isin=isin))
    return rows


def fetch_mse_mw_mainboard_rows(
    source: MasterfileSource,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    text = fetch_text(source.source_url, session=session)
    return parse_mse_mw_mainboard_html(text, source)


def load_mse_mw_mainboard_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    for path in (MSE_MW_MAINBOARD_CACHE, LEGACY_MSE_MW_MAINBOARD_CACHE):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"
    try:
        rows = fetch_mse_mw_mainboard_rows(source, session=session)
    except (requests.RequestException, ValueError):
        return None, "unavailable"

    ensure_output_dirs()
    MSE_MW_MAINBOARD_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def find_current_listing_by_name(
    official_name: str,
    current_rows: list[dict[str, str]],
    seen_tickers: set[str],
) -> dict[str, str] | None:
    for row in current_rows:
        ticker = row.get("ticker", "").strip().upper()
        if ticker in seen_tickers:
            continue
        if alias_matches_company(official_name, row.get("name", "")):
            return row
    return None


USE_UG_VISIBLE_TICKER_OVERRIDES = {
    # USE writes these labels in the Stock Code column; the local DB keeps compact ticker symbols.
    "AIRTEL UGANDA": "AIRTELUGAN",
    "UMEME": "UMEM",
}


def parse_use_ug_market_snapshot_html(
    text: str,
    source: MasterfileSource,
    *,
    listings_path: Path = LISTINGS_CSV,
) -> list[dict[str, str]]:
    current_by_ticker = {
        row.get("ticker", "").strip().upper(): row for row in current_exchange_listings("USE_UG", listings_path)
    }
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for table in pd.read_html(io.StringIO(text)):
        if "Stock Code" not in table.columns or "Name" not in table.columns:
            continue
        for record in table.to_dict("records"):
            official_ticker = str(record.get("Stock Code") or "").strip().upper()
            ticker = USE_UG_VISIBLE_TICKER_OVERRIDES.get(official_ticker, official_ticker.replace(" ", ""))
            official_name = str(record.get("Name") or "").strip()
            listing = current_by_ticker.get(ticker)
            if not listing or ticker in seen:
                continue
            seen.add(ticker)
            rows.append(build_current_listing_reference_row(source, listing, name=official_name))
    return rows


def fetch_use_ug_market_snapshot_rows(
    source: MasterfileSource,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    text = fetch_text(source.source_url, session=session)
    return parse_use_ug_market_snapshot_html(text, source)


def load_use_ug_market_snapshot_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    for path in (USE_UG_MARKET_SNAPSHOT_CACHE, LEGACY_USE_UG_MARKET_SNAPSHOT_CACHE):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"
    try:
        rows = fetch_use_ug_market_snapshot_rows(source, session=session)
    except (requests.RequestException, ValueError):
        return None, "unavailable"

    ensure_output_dirs()
    USE_UG_MARKET_SNAPSHOT_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def parse_nasdaq_mutual_fund_quote_payload(
    payload: dict[str, Any],
    source: MasterfileSource,
    listing: dict[str, str],
) -> dict[str, str] | None:
    data = payload.get("data")
    if not isinstance(data, dict):
        return None
    ticker = listing.get("ticker", "").strip().upper()
    if str(data.get("symbol") or "").strip().upper() != ticker:
        return None
    if str(data.get("exchange") or "").strip() != "Nasdaq Fund Network":
        return None
    name = str(data.get("companyName") or "").strip()
    if not name:
        return None
    row = build_current_listing_reference_row(source, listing, name=name)
    row["source_url"] = NASDAQ_MUTUAL_FUND_QUOTE_URL_TEMPLATE.format(ticker=ticker)
    return row


def fetch_nasdaq_mutual_fund_quote_rows(
    source: MasterfileSource,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    session = session or requests.Session()
    rows: list[dict[str, str]] = []
    for listing in current_exchange_listings("NMFQS"):
        ticker = listing.get("ticker", "").strip().upper()
        if not ticker:
            continue
        payload = fetch_json(
            NASDAQ_MUTUAL_FUND_QUOTE_URL_TEMPLATE.format(ticker=ticker),
            session=session,
            headers=nasdaq_us_request_headers(),
        )
        row = parse_nasdaq_mutual_fund_quote_payload(payload, source, listing)
        if row:
            rows.append(row)
    return rows


def load_nasdaq_mutual_fund_quote_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    for path in (NASDAQ_MUTUAL_FUND_QUOTES_CACHE, LEGACY_NASDAQ_MUTUAL_FUND_QUOTES_CACHE):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"
    try:
        rows = fetch_nasdaq_mutual_fund_quote_rows(source, session=session)
    except (requests.RequestException, ValueError, json.JSONDecodeError):
        return None, "unavailable"

    ensure_output_dirs()
    NASDAQ_MUTUAL_FUND_QUOTES_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def parse_rse_listed_companies_html(
    text: str,
    source: MasterfileSource,
    *,
    listings_path: Path = LISTINGS_CSV,
) -> list[dict[str, str]]:
    current_rows = current_exchange_listings("RSE", listings_path)
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for match in re.finditer(r'class=["\']listed-compagnies-card-item-name["\'][^>]*>(.*?)</span>', text, re.DOTALL):
        official_name = unescape(re.sub(r"<[^>]+>", "", match.group(1))).strip()
        listing = find_current_listing_by_name(official_name, current_rows, seen)
        if not listing:
            continue
        seen.add(listing.get("ticker", "").strip().upper())
        rows.append(build_current_listing_reference_row(source, listing, name=official_name))
    return rows


def fetch_rse_listed_companies_rows(
    source: MasterfileSource,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    session = session or requests.Session()
    response = session.get(source.source_url, headers={"User-Agent": USER_AGENT}, timeout=REQUEST_TIMEOUT, verify=False)
    response.raise_for_status()
    return parse_rse_listed_companies_html(response.text, source)


def load_rse_listed_companies_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    for path in (RSE_LISTED_COMPANIES_CACHE, LEGACY_RSE_LISTED_COMPANIES_CACHE):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"
    try:
        rows = fetch_rse_listed_companies_rows(source, session=session)
    except (requests.RequestException, ValueError):
        return None, "unavailable"

    ensure_output_dirs()
    RSE_LISTED_COMPANIES_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def official_reader_url(source_url: str) -> str:
    return f"https://r.jina.ai/http://r.jina.ai/http://{source_url}"


def fetch_official_reader_markdown(
    source: MasterfileSource,
    *,
    session: requests.Session | None = None,
) -> str:
    session = session or requests.Session()
    response = session.get(
        official_reader_url(source.source_url),
        headers={"User-Agent": USER_AGENT},
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return response.text


GSE_LISTED_MARKDOWN_ROW_RE = re.compile(
    r"^\s*\|\s*\[(?P<ticker>[A-Z0-9][A-Z0-9 .-]*)\]\([^)]*\)\s*\|\s*(?P<name>[^|]+?)\s*\|",
    re.MULTILINE,
)


def parse_gse_listed_companies_markdown(
    text: str,
    source: MasterfileSource,
    *,
    listings_path: Path = LISTINGS_CSV,
) -> list[dict[str, str]]:
    current_by_ticker = {
        row.get("ticker", "").strip().upper(): row for row in current_exchange_listings("GSE", listings_path)
    }
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for match in GSE_LISTED_MARKDOWN_ROW_RE.finditer(text):
        ticker = re.sub(r"\s+", " ", match.group("ticker")).strip().upper()
        name = clean_html_text(match.group("name")).strip()
        listing = current_by_ticker.get(ticker)
        if not listing or ticker in seen or not name:
            continue
        seen.add(ticker)
        rows.append(build_current_listing_reference_row(source, listing, name=name))
    return rows


def fetch_gse_listed_companies_rows(
    source: MasterfileSource,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    text = fetch_official_reader_markdown(source, session=session)
    return parse_gse_listed_companies_markdown(text, source)


def load_gse_listed_companies_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    for path in (GSE_LISTED_COMPANIES_CACHE, LEGACY_GSE_LISTED_COMPANIES_CACHE):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"
    try:
        rows = fetch_gse_listed_companies_rows(source, session=session)
    except (requests.RequestException, ValueError):
        return None, "unavailable"

    ensure_output_dirs()
    GSE_LISTED_COMPANIES_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


LUSE_LISTED_MARKDOWN_ROW_RE = re.compile(
    r"^\s*\*\s+(?P<ticker>[A-Z0-9][A-Z0-9 .-]{0,20}[A-Z0-9])\s*\n+\s*#\s+(?P<name>[^\n]+)",
    re.MULTILINE,
)


def normalize_luse_listed_ticker(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip().upper()


def clean_luse_listed_name(value: str, ticker: str) -> str:
    name = clean_html_text(value).strip()
    for separator in (" – ", " - "):
        prefix = f"{ticker}{separator}"
        if name.upper().startswith(prefix.upper()):
            return name[len(prefix) :].strip()
    return name


def parse_luse_listed_companies_markdown(
    text: str,
    source: MasterfileSource,
    *,
    listings_path: Path = LISTINGS_CSV,
) -> list[dict[str, str]]:
    current_by_ticker = {
        row.get("ticker", "").strip().upper(): row for row in current_exchange_listings("LUSE", listings_path)
    }
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for match in LUSE_LISTED_MARKDOWN_ROW_RE.finditer(text):
        ticker = normalize_luse_listed_ticker(match.group("ticker"))
        listing = current_by_ticker.get(ticker)
        if not listing or ticker in seen:
            continue
        name = clean_luse_listed_name(match.group("name"), ticker)
        if not name:
            continue
        seen.add(ticker)
        rows.append(build_current_listing_reference_row(source, listing, name=name))
    return rows


def fetch_luse_listed_companies_rows(
    source: MasterfileSource,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    text = fetch_official_reader_markdown(source, session=session)
    return parse_luse_listed_companies_markdown(text, source)


def load_luse_listed_companies_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    for path in (LUSE_LISTED_COMPANIES_CACHE, LEGACY_LUSE_LISTED_COMPANIES_CACHE):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"
    try:
        rows = fetch_luse_listed_companies_rows(source, session=session)
    except (requests.RequestException, ValueError):
        return None, "unavailable"

    ensure_output_dirs()
    LUSE_LISTED_COMPANIES_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def normalize_bolsa_santiago_nemo(value: Any) -> str:
    return re.sub(r"\s+", "", str(value or "").strip().upper())


def parse_bolsa_santiago_instruments_payload(
    payload: dict[str, Any],
    source: MasterfileSource,
    *,
    listings_path: Path = LISTINGS_CSV,
) -> list[dict[str, str]]:
    current_by_normalized_ticker = {
        normalize_bolsa_santiago_nemo(row.get("ticker", "")): row
        for row in current_exchange_listings("SSE_CL", listings_path)
    }
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for record in payload.get("listaResult", []):
        if not isinstance(record, dict):
            continue
        normalized_ticker = normalize_bolsa_santiago_nemo(record.get("NEMO"))
        listing = current_by_normalized_ticker.get(normalized_ticker)
        if not listing:
            continue
        ticker = listing.get("ticker", "").strip().upper()
        if not ticker or ticker in seen:
            continue
        name = clean_html_text(str(record.get("RAZON_SOCIAL") or "")).strip()
        if not name:
            continue
        seen.add(ticker)
        rows.append(build_current_listing_reference_row(source, listing, name=name))
    return rows


def fetch_bolsa_santiago_instruments_payload_via_browser() -> dict[str, Any]:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:  # pragma: no cover - optional browser dependency
        raise requests.RequestException("Playwright is required for Bolsa de Santiago browser fetch") from exc

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        try:
            context = browser.new_context(
                locale="es-CL",
                user_agent=BROWSER_USER_AGENT,
                viewport={"width": 1365, "height": 900},
            )
            page = context.new_page()
            page.goto(
                BOLSA_SANTIAGO_INSTRUMENTS_PAGE_URL,
                wait_until="domcontentloaded",
                timeout=int(REQUEST_TIMEOUT * 1000),
            )
            # Radware blocks raw requests; normal browser interaction is enough to set the session cookies.
            page.mouse.move(80, 80)
            page.mouse.down()
            page.mouse.move(220, 180)
            page.mouse.up()
            page.wait_for_timeout(8000)
            result = page.evaluate(
                """async ({ tokenUrl, instrumentsUrl }) => {
                    const tokenResponse = await fetch(tokenUrl, {
                        headers: { "Accept": "application/json, text/plain, */*" }
                    });
                    const tokenPayload = await tokenResponse.json();
                    const response = await fetch(instrumentsUrl, {
                        method: "POST",
                        headers: {
                            "Accept": "application/json, text/plain, */*",
                            "Content-Type": "application/json;charset=UTF-8",
                            "X-CSRF-Token": tokenPayload.csrf || ""
                        },
                        body: "{}"
                    });
                    return { status: response.status, text: await response.text() };
                }""",
                {
                    "tokenUrl": "https://www.bolsadesantiago.com/api/Securities/csrfToken",
                    "instrumentsUrl": BOLSA_SANTIAGO_INSTRUMENTS_URL,
                },
            )
        finally:
            browser.close()

    if int(result.get("status") or 0) != 200:
        raise requests.HTTPError(f"Bolsa de Santiago instruments API returned {result.get('status')}")
    payload = json.loads(str(result.get("text") or ""))
    if not isinstance(payload, dict):
        raise ValueError("Bolsa de Santiago instruments payload must be an object")
    return payload


def fetch_bolsa_santiago_instruments_rows(
    source: MasterfileSource,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    del session
    payload = fetch_bolsa_santiago_instruments_payload_via_browser()
    return parse_bolsa_santiago_instruments_payload(payload, source)


def load_bolsa_santiago_instruments_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    for path in (BOLSA_SANTIAGO_INSTRUMENTS_CACHE, LEGACY_BOLSA_SANTIAGO_INSTRUMENTS_CACHE):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"
    try:
        rows = fetch_bolsa_santiago_instruments_rows(source, session=session)
    except (requests.RequestException, ValueError, json.JSONDecodeError):
        return None, "unavailable"

    ensure_output_dirs()
    BOLSA_SANTIAGO_INSTRUMENTS_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def zse_zw_short_name_ticker(value: Any) -> str:
    return str(value or "").strip().upper().split(".", 1)[0]


def parse_zse_zw_issuers_payload(
    payload: dict[str, Any],
    source: MasterfileSource,
    *,
    listings_path: Path = LISTINGS_CSV,
) -> list[dict[str, str]]:
    current_rows = current_exchange_listings("ZSE_ZW", listings_path)
    current_by_ticker = {row.get("ticker", "").strip().upper(): row for row in current_rows}
    current_by_isin = {
        row.get("isin", "").strip().upper(): row
        for row in current_rows
        if is_valid_isin(row.get("isin", "").strip().upper())
    }
    rows: list[dict[str, str]] = []
    seen: set[str] = set()

    for record in payload.get("issuers", []):
        if not isinstance(record, dict):
            continue
        ticker = zse_zw_short_name_ticker(record.get("short_name"))
        listing = current_by_ticker.get(ticker)
        if not listing or ticker in seen:
            continue
        seen.add(ticker)
        rows.append(build_current_listing_reference_row(source, listing, name=str(record.get("name") or "")))

    for record in payload.get("price_sheet", []):
        if not isinstance(record, dict):
            continue
        isin = str(record.get("isin") or "").strip().upper()
        listing = current_by_isin.get(isin)
        if not listing:
            continue
        ticker = listing.get("ticker", "").strip().upper()
        if ticker in seen:
            continue
        seen.add(ticker)
        name = str(record.get("name") or record.get("symbol") or listing.get("name", "")).strip()
        rows.append(build_current_listing_reference_row(source, listing, name=name, isin=isin))

    return rows


def fetch_zse_zw_issuers_payload(session: requests.Session | None = None) -> dict[str, Any]:
    session = session or requests.Session()
    issuers: list[dict[str, Any]] = []
    for market in ("equity", "etf"):
        payload = fetch_json(f"{ZSE_ZW_ISSUERS_URL}?exchange=ZSE&market={market}", session=session)
        if isinstance(payload, list):
            issuers.extend(payload)
    price_payload = fetch_json(ZSE_ZW_PRICE_SHEET_URL, session=session)
    price_sheet = price_payload.get("data", []) if isinstance(price_payload, dict) else []
    return {"issuers": issuers, "price_sheet": price_sheet}


def fetch_zse_zw_issuers_rows(
    source: MasterfileSource,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    return parse_zse_zw_issuers_payload(fetch_zse_zw_issuers_payload(session=session), source)


def load_zse_zw_issuers_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    for path in (ZSE_ZW_ISSUERS_CACHE, LEGACY_ZSE_ZW_ISSUERS_CACHE):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"
    try:
        rows = fetch_zse_zw_issuers_rows(source, session=session)
    except (requests.RequestException, ValueError, json.JSONDecodeError):
        return None, "unavailable"

    ensure_output_dirs()
    ZSE_ZW_ISSUERS_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def cavali_normalize_group(value: str) -> str:
    return value.strip().upper().replace("Ó", "O")


def cavali_row_matches_listing_asset_type(
    row_group: str,
    row_type: str,
    listing_asset_type: str,
) -> bool:
    group = cavali_normalize_group(row_group)
    row_type_upper = row_type.strip().upper()
    if listing_asset_type == "ETF":
        return group in {cavali_normalize_group(value) for value in CAVALI_ETF_GROUPS} and "ETF" in row_type_upper
    if listing_asset_type == "Stock":
        return group in {cavali_normalize_group(value) for value in CAVALI_STOCK_GROUPS}
    return False


def parse_cavali_bvl_emisores_html_pages(
    html_pages: Iterable[str],
    source: MasterfileSource,
    *,
    listings_path: Path = LISTINGS_CSV,
) -> list[dict[str, str]]:
    current_rows_by_ticker = {
        row.get("ticker", "").strip().upper(): row
        for row in load_csv(listings_path)
        if row.get("exchange") == "BVL" and row.get("ticker", "").strip()
    }
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for text in html_pages:
        parser = _TableParser()
        parser.feed(text)
        for table in parser.tables:
            if not table:
                continue
            header = [cell.strip().upper() for cell in table[0]]
            if {"ISIN", "EMPRESA", "NEMONICO", "GRUPO", "TIPO"} - set(header):
                continue
            isin_index = header.index("ISIN")
            name_index = header.index("EMPRESA")
            ticker_index = header.index("NEMONICO")
            group_index = header.index("GRUPO")
            type_index = header.index("TIPO")
            for record in table[1:]:
                if len(record) <= max(isin_index, name_index, ticker_index, group_index, type_index):
                    continue
                ticker = record[ticker_index].strip().upper()
                isin = record[isin_index].strip().upper()
                name = record[name_index].strip()
                listing = current_rows_by_ticker.get(ticker)
                if (
                    not listing
                    or ticker in seen
                    or not name
                    or not is_valid_isin(isin)
                    or not cavali_row_matches_listing_asset_type(
                        record[group_index],
                        record[type_index],
                        listing.get("asset_type", ""),
                    )
                ):
                    continue
                rows.append(
                    {
                        "source_key": source.key,
                        "provider": source.provider,
                        "source_url": source.source_url,
                        "ticker": ticker,
                        "name": name,
                        "exchange": "BVL",
                        "asset_type": listing.get("asset_type", "") or infer_asset_type(name),
                        "listing_status": "active",
                        "reference_scope": source.reference_scope,
                        "official": "true",
                        "isin": isin,
                    }
                )
                seen.add(ticker)
    return rows


def cavali_page_count(text: str) -> int:
    match = CAVALI_PAGE_COUNT_RE.search(text)
    if match is None:
        return 1
    return max(1, int(match.group("count")))


def fetch_cavali_bvl_emisores_rows(
    source: MasterfileSource,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    session = session or requests.Session()
    response = session.get(source.source_url, headers={"User-Agent": USER_AGENT}, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    html_pages = [response.text]
    for page in range(2, cavali_page_count(response.text) + 1):
        page_response = session.get(
            source.source_url,
            params={"page": page},
            headers={"User-Agent": USER_AGENT},
            timeout=REQUEST_TIMEOUT,
        )
        page_response.raise_for_status()
        html_pages.append(page_response.text)
    return parse_cavali_bvl_emisores_html_pages(html_pages, source)


def load_cavali_bvl_emisores_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    for path in (BVL_ISSUERS_DIRECTORY_CACHE, LEGACY_BVL_ISSUERS_DIRECTORY_CACHE):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"
    try:
        rows = fetch_cavali_bvl_emisores_rows(source, session=session)
    except (requests.RequestException, ValueError):
        return None, "unavailable"

    ensure_output_dirs()
    BVL_ISSUERS_DIRECTORY_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def extract_next_data_json(text: str) -> str:
    match = re.search(
        r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(?P<payload>.*?)</script>',
        text,
        re.I | re.S,
    )
    if not match:
        raise ValueError("Missing Next.js data payload")
    return unescape(match.group("payload"))


def normalize_nzx_etf_category(name: Any) -> str:
    value = str(name or "").lower()
    if not value:
        return ""
    if any(marker in value for marker in ("bond", "cash", "deposit", "aggregate")):
        return "Fixed Income"
    if any(marker in value for marker in ("gold", "commodity", "carbon")):
        return "Commodity"
    if any(marker in value for marker in ("property", "real estate", "reit")):
        return "Real Estate"
    if any(marker in value for marker in ("inverse", "leveraged")):
        return "Leveraged/Inverse"
    if "etf" in value or "index" in value or "equity" in value:
        return "Equity"
    return ""


def parse_nzx_instruments_next_data_payload(
    payload: dict[str, Any],
    source: MasterfileSource,
) -> list[dict[str, str]]:
    instruments = (
        ((payload.get("props") or {}).get("pageProps") or {})
        .get("data", {})
        .get("instruments", {})
        .get("activeInstruments", [])
    )
    if not isinstance(instruments, list):
        raise ValueError("Unexpected NZX instruments payload")

    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for record in instruments:
        if not isinstance(record, dict) or str(record.get("marketType") or "").strip().upper() != "NZSX":
            continue
        ticker = str(record.get("code") or "").strip().upper()
        name = str(record.get("name") or "").strip()
        isin = str(record.get("isin") or "").strip().upper()
        if not ticker or not name or ticker in seen:
            continue
        category = str(record.get("category") or "").strip().upper()
        instrument_type = str(record.get("type") or "").strip().upper()
        subcategory = str(record.get("subCategory") or "").strip().upper()
        if category != "SHRS" or instrument_type not in {"SHRS", "UNIT"}:
            continue

        asset_type = "ETF" if subcategory == "ETF" or " ETF" in f" {name.upper()} " else "Stock"
        row = {
            "source_key": source.key,
            "provider": source.provider,
            "source_url": source.source_url,
            "ticker": ticker,
            "name": name,
            "exchange": "NZX",
            "asset_type": asset_type,
            "listing_status": "active",
            "reference_scope": source.reference_scope,
            "official": "true",
        }
        if is_valid_isin(isin):
            row["isin"] = isin
        if asset_type == "ETF":
            etf_category = normalize_nzx_etf_category(name)
            if etf_category:
                row["sector"] = etf_category
        rows.append(row)
        seen.add(ticker)
    return rows


def parse_nzx_instruments_next_data_html(html: str, source: MasterfileSource) -> list[dict[str, str]]:
    payload = json.loads(extract_next_data_json(html))
    return parse_nzx_instruments_next_data_payload(payload, source)


def fetch_nzx_instruments_rows(
    source: MasterfileSource,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    response = (session or requests).get(source.source_url, headers={"User-Agent": USER_AGENT}, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return parse_nzx_instruments_next_data_html(response.text, source)


def load_nzx_instruments_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    for path in (NZX_INSTRUMENTS_CACHE, LEGACY_NZX_INSTRUMENTS_CACHE):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"
    try:
        rows = fetch_nzx_instruments_rows(source, session=session)
    except (requests.RequestException, ValueError, json.JSONDecodeError):
        return None, "unavailable"

    ensure_output_dirs()
    NZX_INSTRUMENTS_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def cse_ma_relationship_id(item: dict[str, Any], relationship_name: str) -> str:
    relationship = (item.get("relationships") or {}).get(relationship_name) or {}
    data = relationship.get("data") or {}
    return str(data.get("id") or "")


def parse_cse_ma_jsonapi_collection(
    payload: dict[str, Any],
    source: MasterfileSource,
) -> tuple[list[dict[str, str]], str]:
    included_by_key = {
        (str(item.get("type") or ""), str(item.get("id") or "")): item
        for item in payload.get("included", [])
        if isinstance(item, dict)
    }
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for item in payload.get("data", []):
        attributes = item.get("attributes") or {}
        ticker = str(attributes.get("symbol") or "").strip().upper()
        isin = str(attributes.get("codeISIN") or "").strip().upper()
        issuer_id = cse_ma_relationship_id(item, "codeSociete")
        issuer = included_by_key.get(("emetteur", issuer_id), {})
        issuer_attributes = issuer.get("attributes") or {}
        name = (
            str(issuer_attributes.get("raisonSociale") or "").strip()
            or str(issuer_attributes.get("name") or "").strip()
            or str(attributes.get("libelleEN") or "").strip()
            or str(attributes.get("libelleFR") or "").strip()
        )
        if not ticker or ticker in seen or not name:
            continue

        row = {
            "source_key": source.key,
            "provider": source.provider,
            "source_url": urljoin(CASABLANCA_BOURSE_INSTRUMENTS_URL, str(attributes.get("instrument_url") or "")),
            "ticker": ticker,
            "name": name,
            "exchange": "CSE_MA",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": source.reference_scope,
            "official": "true",
        }
        if is_valid_isin(isin):
            row["isin"] = isin
        rows.append(row)
        seen.add(ticker)
    next_url = str(((payload.get("links") or {}).get("next") or {}).get("href") or "")
    return rows, next_url


def cse_ma_initial_collection_from_next_payload(payload: dict[str, Any]) -> dict[str, Any]:
    paragraphs = (((payload.get("pageProps") or {}).get("node") or {}).get("field_vactory_paragraphs") or [])
    for paragraph in paragraphs:
        widget_data = ((paragraph.get("field_vactory_component") or {}).get("widget_data") or "")
        if not widget_data or "collection" not in widget_data or "instrument" not in widget_data:
            continue
        widget = json.loads(widget_data)
        components = widget.get("components") or []
        for component in components:
            collection = ((component.get("collection") or {}).get("data") or {})
            if collection.get("data"):
                return collection
    raise ValueError("Unexpected Casablanca Stock Exchange instruments payload")


def fetch_cse_ma_listed_companies(
    source: MasterfileSource,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    session = session or requests.Session()
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    page_response = session.get(
        source.source_url,
        headers={"User-Agent": BROWSER_USER_AGENT},
        timeout=REQUEST_TIMEOUT,
        verify=False,
    )
    page_response.raise_for_status()
    shell_payload = json.loads(extract_next_data_json(page_response.text))
    build_id = str(shell_payload.get("buildId") or "").strip()
    if not build_id:
        raise ValueError("Missing Casablanca Stock Exchange Next.js build id")
    next_data_url = urljoin(source.source_url, f"/_next/data/{build_id}/en/marche-cash/instruments-actions.json")
    next_data_response = session.get(
        next_data_url,
        headers={"User-Agent": BROWSER_USER_AGENT, "Accept": "application/json"},
        timeout=REQUEST_TIMEOUT,
        verify=False,
    )
    next_data_response.raise_for_status()
    next_payload = next_data_response.json()
    collection = cse_ma_initial_collection_from_next_payload(next_payload)

    rows: list[dict[str, str]] = []
    seen_urls: set[str] = set()
    while collection:
        collection_rows, next_url = parse_cse_ma_jsonapi_collection(collection, source)
        rows.extend(collection_rows)
        if not next_url or next_url in seen_urls:
            break
        seen_urls.add(next_url)
        response = session.get(
            next_url,
            headers={"User-Agent": BROWSER_USER_AGENT, "Accept": "application/vnd.api+json"},
            timeout=REQUEST_TIMEOUT,
            verify=False,
        )
        response.raise_for_status()
        collection = response.json()

    rows = dedupe_rows(rows)
    if not rows:
        raise ValueError("Unexpected Casablanca Stock Exchange instruments payload")
    return rows


def load_cse_ma_listed_companies_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    for path in (CSE_MA_LISTED_COMPANIES_CACHE, LEGACY_CSE_MA_LISTED_COMPANIES_CACHE):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"
    try:
        rows = fetch_cse_ma_listed_companies(source, session=session)
    except (requests.RequestException, ValueError, json.JSONDecodeError):
        return None, "unavailable"

    ensure_output_dirs()
    CSE_MA_LISTED_COMPANIES_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


BVC_SECTOR_MAP = {
    "basic materials": "Materials",
    "consumer goods": "Consumer Staples",
    "consumer services": "Consumer Discretionary",
    "energy": "Energy",
    "financials": "Financials",
    "health care": "Health Care",
    "industrials": "Industrials",
    "oil & gas": "Energy",
    "real estate": "Real Estate",
    "technology": "Information Technology",
    "telecommunications": "Communication Services",
    "utilities": "Utilities",
}


def bvc_request_headers(filters: str, token: str) -> dict[str, str]:
    return {
        "User-Agent": BROWSER_USER_AGENT,
        "Accept": "application/json,text/plain,*/*",
        "Origin": "https://www.bvc.com.co",
        "Referer": BVC_RV_ISSUERS_PAGE_URL,
        "token": token,
        "k": b64encode(filters.encode("utf-8")).decode("ascii"),
    }


def fetch_bvc_handshake_token(session: requests.Session) -> str:
    response = session.get(
        f"{BVC_HANDSHAKE_URL}?ts={int(time.time() * 1000)}&r={time.time_ns()}",
        headers={"User-Agent": BROWSER_USER_AGENT, "Accept": "application/json"},
        timeout=REQUEST_TIMEOUT,
        verify=False,
    )
    response.raise_for_status()
    token = str((response.json() or {}).get("token") or "").strip()
    if not token:
        raise ValueError("Missing BVC handshake token")
    return token


def normalize_bvc_sector(value: Any) -> str:
    sector = str(value or "").strip()
    if not sector:
        return ""
    return BVC_SECTOR_MAP.get(sector.lower(), sector)


def parse_bvc_rv_issuers_payload(
    payload: dict[str, Any],
    source: MasterfileSource,
    *,
    listings_path: Path = LISTINGS_CSV,
) -> list[dict[str, str]]:
    current_by_ticker = {
        row.get("ticker", "").strip().upper(): row for row in current_exchange_listings("BVC", listings_path)
    }
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for record in payload.get("data", []):
        if not isinstance(record, dict):
            continue
        ticker = str(record.get("symbol") or "").strip().upper()
        listing = current_by_ticker.get(ticker)
        if not listing or ticker in seen:
            continue
        name = str(record.get("issuerName") or "").strip() or listing.get("name", "")
        industry = record.get("industryName") or {}
        sector = ""
        if isinstance(industry, dict):
            sector = normalize_bvc_sector(industry.get("en") or industry.get("es_CO"))
        else:
            sector = normalize_bvc_sector(industry)
        rows.append(build_current_listing_reference_row(source, listing, name=name, sector=sector))
        seen.add(ticker)
    return rows


def fetch_bvc_rv_issuers_rows(
    source: MasterfileSource,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    session = session or requests.Session()
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    token = fetch_bvc_handshake_token(session)
    response = session.get(
        source.source_url,
        headers=bvc_request_headers(BVC_RV_ISSUERS_FILTERS, token),
        timeout=REQUEST_TIMEOUT,
        verify=False,
    )
    response.raise_for_status()
    return parse_bvc_rv_issuers_payload(response.json(), source)


def load_bvc_rv_issuers_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    for path in (BVC_RV_ISSUERS_CACHE, LEGACY_BVC_RV_ISSUERS_CACHE):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"
    try:
        rows = fetch_bvc_rv_issuers_rows(source, session=session)
    except (requests.RequestException, ValueError, json.JSONDecodeError):
        return None, "unavailable"

    ensure_output_dirs()
    BVC_RV_ISSUERS_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def byma_request_headers() -> dict[str, str]:
    return {
        "User-Agent": BROWSER_USER_AGENT,
        "Accept": "application/json,text/plain,*/*",
        "Content-Type": "application/json",
        "Origin": "https://open.bymadata.com.ar",
        "Referer": BYMA_EQUITY_DETAILS_PAGE_URL,
    }


def byma_post_json(
    session: requests.Session,
    url: str,
    payload: dict[str, str],
) -> dict[str, Any] | list[dict[str, Any]]:
    response = session.post(
        url,
        json=payload,
        headers=byma_request_headers(),
        timeout=REQUEST_TIMEOUT,
        verify=False,
    )
    response.raise_for_status()
    return response.json()


def byma_current_ticker_candidates(ticker: str) -> list[str]:
    ticker = ticker.strip().upper()
    candidates = [ticker]
    if "-" in ticker:
        candidates.append(ticker.replace("-", "."))
    return list(dict.fromkeys(candidate for candidate in candidates if candidate))


def byma_name_tokens(value: str) -> set[str]:
    tokens = {token for token in normalize_tokens(value) if len(token) >= 3}
    tokens.update(token.rstrip("ns") for token in list(tokens) if len(token) >= 6)
    return {token for token in tokens if len(token) >= 3}


def byma_names_compatible(local_name: str, official_name: str) -> bool:
    local_tokens = byma_name_tokens(local_name)
    official_tokens = byma_name_tokens(official_name)
    if not local_tokens or not official_tokens:
        return False
    common_tokens = local_tokens & official_tokens
    return len(common_tokens) >= 2 or any(len(token) >= 6 for token in common_tokens)


def byma_official_name(record: dict[str, Any], fallback: str) -> str:
    instrument_type = str(record.get("tipoEspecie") or "").strip().lower()
    if "cedear" in instrument_type:
        return str(record.get("denominacion") or record.get("simboloMercado") or fallback).strip()
    return str(record.get("emisor") or record.get("denominacion") or fallback).strip()


def parse_byma_equity_detail_payload(
    payload: dict[str, Any],
    source: MasterfileSource,
    listing: dict[str, str],
    *,
    remote_symbol: str | None = None,
) -> dict[str, str] | None:
    data = payload.get("data")
    if not isinstance(data, list) or not data:
        return None
    record = next((item for item in data if isinstance(item, dict)), None)
    if not record:
        return None
    instrument_type = str(record.get("tipoEspecie") or "").strip().lower()
    instrument_class = str(record.get("insType") or "").strip().lower()
    if "cedear" not in instrument_type and "accion" not in instrument_type and instrument_class != "equity":
        return None
    name = byma_official_name(record, listing.get("name", ""))
    if remote_symbol and remote_symbol.strip().upper() != listing.get("ticker", "").strip().upper():
        if not byma_names_compatible(listing.get("name", ""), name):
            return None
    isin = str(record.get("codigoIsin") or "").strip().upper()
    if isin and not is_valid_isin(isin):
        isin = ""
    return build_current_listing_reference_row(source, listing, name=name, isin=isin)


def parse_byma_header_search_payload(
    payload: list[dict[str, Any]],
    source: MasterfileSource,
    listing: dict[str, str],
) -> dict[str, str] | None:
    ticker = listing.get("ticker", "").strip().upper()
    for record in payload:
        if not isinstance(record, dict):
            continue
        if str(record.get("market") or "").strip().upper() != "BYMA":
            continue
        if str(record.get("type") or "").strip().lower() != "accion":
            continue
        if str(record.get("symbol") or "").strip().upper() != ticker:
            continue
        return build_current_listing_reference_row(
            source,
            listing,
            name=str(record.get("description") or listing.get("name", "")).strip(),
        )
    return None


def fetch_byma_equity_details_rows(
    source: MasterfileSource,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    session = session or requests.Session()
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for listing in current_exchange_listings("BCBA"):
        ticker = listing.get("ticker", "").strip().upper()
        if not ticker or ticker in seen:
            continue
        row = None
        for candidate in byma_current_ticker_candidates(ticker):
            try:
                payload = byma_post_json(session, BYMA_EQUITY_DETAIL_URL, {"symbol": candidate})
            except (requests.RequestException, json.JSONDecodeError):
                continue
            if isinstance(payload, dict):
                row = parse_byma_equity_detail_payload(payload, source, listing, remote_symbol=candidate)
            if row:
                break
        if not row:
            try:
                payload = byma_post_json(session, BYMA_HEADER_SEARCH_URL, {"key": "searchInstruments", "value": ticker})
            except (requests.RequestException, json.JSONDecodeError):
                payload = []
            if isinstance(payload, list):
                row = parse_byma_header_search_payload(payload, source, listing)
        if row:
            rows.append(row)
            seen.add(ticker)
    return dedupe_rows(rows)


def load_byma_equity_details_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    for path in (BYMA_EQUITY_DETAILS_CACHE, LEGACY_BYMA_EQUITY_DETAILS_CACHE):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"
    try:
        rows = fetch_byma_equity_details_rows(source, session=session)
    except (requests.RequestException, ValueError, json.JSONDecodeError):
        return None, "unavailable"

    ensure_output_dirs()
    BYMA_EQUITY_DETAILS_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def cse_lk_request_headers() -> dict[str, str]:
    return {
        "User-Agent": BROWSER_USER_AGENT,
        "Accept": "application/json,text/plain,*/*",
        "x-api-key": b64encode(b"Cse123Api").decode("ascii"),
    }


def parse_cse_lk_all_security_code_payload(
    payload: list[dict[str, Any]],
    source: MasterfileSource,
    *,
    listings_path: Path = LISTINGS_CSV,
) -> list[dict[str, str]]:
    current_by_ticker = {
        row.get("ticker", "").strip().upper(): row for row in current_exchange_listings("CSE_LK", listings_path)
    }
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for record in payload:
        if not isinstance(record, dict) or int(record.get("active") or 0) != 1:
            continue
        symbol = str(record.get("symbol") or "").strip().upper()
        ticker_candidates = [symbol]
        if "." in symbol:
            ticker_candidates.append(symbol.split(".", 1)[0])
        ticker = next((candidate for candidate in ticker_candidates if candidate in current_by_ticker), "")
        listing = current_by_ticker.get(ticker)
        if not ticker or not listing or ticker in seen:
            continue
        name = str(record.get("name") or "").strip() or listing.get("name", "")
        rows.append(build_current_listing_reference_row(source, listing, name=name))
        seen.add(ticker)
    return rows


def fetch_cse_lk_all_security_code_rows(
    source: MasterfileSource,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    payload = fetch_json(source.source_url, session=session, headers=cse_lk_request_headers())
    if not isinstance(payload, list):
        raise ValueError("Unexpected CSE Sri Lanka security-code payload")
    return parse_cse_lk_all_security_code_payload(payload, source)


def load_cse_lk_all_security_code_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    for path in (CSE_LK_ALL_SECURITY_CODE_CACHE, LEGACY_CSE_LK_ALL_SECURITY_CODE_CACHE):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"
    try:
        rows = fetch_cse_lk_all_security_code_rows(source, session=session)
    except (requests.RequestException, ValueError, json.JSONDecodeError):
        return None, "unavailable"

    ensure_output_dirs()
    CSE_LK_ALL_SECURITY_CODE_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def parse_cse_lk_company_info_summary_payload(
    payload: dict[str, Any],
    source: MasterfileSource,
) -> dict[str, str] | None:
    if not isinstance(payload, dict):
        return None
    info = payload.get("reqSymbolInfo")
    if not isinstance(info, dict):
        return None
    symbol = str(info.get("symbol") or "").strip().upper()
    name = str(info.get("name") or "").strip()
    isin = str(info.get("isin") or "").strip().upper()
    if not symbol or not name or not is_valid_isin(isin):
        return None
    return {
        "source_key": source.key,
        "provider": source.provider,
        "source_url": source.source_url,
        "ticker": symbol,
        "name": name,
        "exchange": "CSE_LK",
        "asset_type": "Stock",
        "listing_status": "active",
        "reference_scope": source.reference_scope,
        "official": "true",
        "isin": isin,
    }


def fetch_cse_lk_company_info_summary_payload(
    symbol: str,
    *,
    session: requests.Session | None = None,
) -> dict[str, Any]:
    headers = cse_lk_request_headers()
    if session is not None:
        response = session.post(
            CSE_LK_COMPANY_INFO_SUMMARY_URL,
            data={"symbol": symbol},
            headers=headers,
            timeout=15,
        )
    else:
        response = requests.post(
            CSE_LK_COMPANY_INFO_SUMMARY_URL,
            data={"symbol": symbol},
            headers=headers,
            timeout=15,
        )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("Unexpected CSE Sri Lanka company-info payload")
    return payload


def fetch_cse_lk_company_info_summary_rows(
    source: MasterfileSource,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    security_payload = fetch_json(CSE_LK_ALL_SECURITY_CODE_URL, session=session, headers=cse_lk_request_headers())
    if not isinstance(security_payload, list):
        raise ValueError("Unexpected CSE Sri Lanka security-code payload")
    symbols = [
        str(record.get("symbol") or "").strip().upper()
        for record in security_payload
        if isinstance(record, dict) and int(record.get("active") or 0) == 1
    ]

    rows: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()

    def append_payload(payload: dict[str, Any]) -> None:
        row = parse_cse_lk_company_info_summary_payload(payload, source)
        if row is None:
            return
        key = (row["ticker"], row["isin"])
        if key in seen:
            return
        rows.append(row)
        seen.add(key)

    if session is not None:
        for symbol in symbols:
            append_payload(fetch_cse_lk_company_info_summary_payload(symbol, session=session))
    else:
        with ThreadPoolExecutor(max_workers=CSE_LK_COMPANY_INFO_WORKERS) as executor:
            futures = {
                executor.submit(fetch_cse_lk_company_info_summary_payload, symbol): symbol
                for symbol in symbols
            }
            for future in as_completed(futures):
                append_payload(future.result())

    return sorted(rows, key=lambda row: (row["ticker"], row["isin"]))


def load_cse_lk_company_info_summary_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    for path in (CSE_LK_COMPANY_INFO_SUMMARY_CACHE, LEGACY_CSE_LK_COMPANY_INFO_SUMMARY_CACHE):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"
    try:
        rows = fetch_cse_lk_company_info_summary_rows(source, session=session)
    except (requests.RequestException, ValueError, json.JSONDecodeError):
        return None, "unavailable"

    ensure_output_dirs()
    CSE_LK_COMPANY_INFO_SUMMARY_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def extract_nse_ke_wp_json_payload(text: str) -> list[dict[str, Any]]:
    payload_start = text.find('[{"id"')
    if payload_start < 0:
        payload_start = text.find("[")
    if payload_start < 0:
        raise ValueError("Missing NSE Kenya WordPress JSON payload")
    payload = json.loads(text[payload_start:])
    if not isinstance(payload, list):
        raise ValueError("Unexpected NSE Kenya WordPress JSON payload")
    return payload


def nse_ke_section_title_before(content: str, position: int) -> str:
    toggle_start = content.rfind("[toggle", 0, position)
    if toggle_start < 0:
        return ""
    section_matches = list(NSE_KE_SECTION_TITLE_RE.finditer(content[toggle_start:position]))
    if not section_matches:
        return ""
    return clean_html_text(section_matches[-1].group("title")).upper()


def nse_ke_asset_type(name: str, section: str) -> str:
    upper_name = name.upper()
    if section == "EXCHANGE TRADED FUND" or " ETF" in f" {upper_name} ":
        return "ETF"
    return "Stock"


def parse_nse_ke_listed_companies_html(
    text: str,
    source: MasterfileSource,
) -> list[dict[str, str]]:
    content = unescape(text)
    title_matches = list(NSE_KE_COMPANY_TITLE_RE.finditer(content))
    rows: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for index, title_match in enumerate(title_matches):
        start = title_match.start()
        end = title_matches[index + 1].start() if index + 1 < len(title_matches) else len(content)
        block = content[start:end]
        name_match = NSE_KE_TITLE_TEXT_RE.search(block)
        symbol_match = NSE_KE_SYMBOL_RE.search(block)
        isin_match = NSE_KE_ISIN_RE.search(block)
        if not name_match or not symbol_match or not isin_match:
            continue

        name = clean_html_text(name_match.group("name"))
        ticker = clean_html_text(symbol_match.group("symbol")).upper()
        isin = clean_html_text(isin_match.group("isin")).upper()
        if not ticker or not name or not is_valid_isin(isin):
            continue

        section = nse_ke_section_title_before(content, start)
        asset_type = nse_ke_asset_type(name, section)
        row = {
            "source_key": source.key,
            "provider": source.provider,
            "source_url": source.source_url,
            "ticker": ticker,
            "name": name,
            "exchange": "NSE_KE",
            "asset_type": asset_type,
            "listing_status": "active",
            "reference_scope": source.reference_scope,
            "official": "true",
            "isin": isin,
        }
        sector = NSE_KE_SECTOR_MAP.get(section, "")
        if sector and asset_type == "Stock":
            row["sector"] = sector
        key = (ticker, asset_type)
        if key not in seen:
            rows.append(row)
            seen.add(key)

    if not rows:
        raise ValueError("Unexpected NSE Kenya listed companies payload")
    return rows


def fetch_nse_ke_listed_companies(
    source: MasterfileSource,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    session = session or requests.Session()
    response = session.get(
        NSE_KE_LISTED_COMPANIES_API_URL,
        headers={"User-Agent": BROWSER_USER_AGENT, "Accept": "application/json"},
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    payload = extract_nse_ke_wp_json_payload(response.text)
    if not payload:
        raise ValueError("Unexpected NSE Kenya WordPress JSON payload")
    rendered_content = (((payload[0] or {}).get("content") or {}).get("rendered") or "")
    if not rendered_content:
        raise ValueError("Unexpected NSE Kenya listed companies payload")
    return parse_nse_ke_listed_companies_html(rendered_content, source)


def load_nse_ke_listed_companies_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    for path in (NSE_KE_LISTED_COMPANIES_CACHE, LEGACY_NSE_KE_LISTED_COMPANIES_CACHE):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"
    try:
        rows = fetch_nse_ke_listed_companies(source, session=session)
    except (requests.RequestException, ValueError, json.JSONDecodeError):
        return None, "unavailable"

    ensure_output_dirs()
    NSE_KE_LISTED_COMPANIES_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def nse_india_headers() -> dict[str, str]:
    return {
        "Accept": "text/csv,*/*",
        "Referer": NSE_IN_SECURITIES_AVAILABLE_PAGE_URL,
        "User-Agent": BROWSER_USER_AGENT,
    }


def normalize_nse_india_csv_record(record: dict[str, Any]) -> dict[str, str]:
    return {str(key or "").strip(): str(value or "").strip() for key, value in record.items()}


def normalize_nse_india_etf_category(underlying: str, name: str) -> str:
    value = f"{underlying} {name}".lower()
    if not value.strip():
        return ""
    if any(marker in value for marker in ("gold", "silver", "commodity")):
        return "Commodity"
    if any(marker in value for marker in ("liquid", "gsec", "government", "bond", "debt", "money market")):
        return "Fixed Income"
    if any(marker in value for marker in ("bank", "nifty", "sensex", "index", "equity", "midcap", "smallcap")):
        return "Equity"
    return ""


def build_nse_india_row(
    source: MasterfileSource,
    *,
    ticker: str,
    name: str,
    isin: str,
    asset_type: str,
    source_url: str,
    sector: str = "",
) -> dict[str, str] | None:
    ticker = ticker.strip().upper()
    name = name.strip()
    isin = isin.strip().upper()
    if not ticker or not name or not is_valid_isin(isin):
        return None
    row = {
        "source_key": source.key,
        "provider": source.provider,
        "source_url": source_url,
        "ticker": ticker,
        "name": name,
        "exchange": "NSE_IN",
        "asset_type": asset_type,
        "listing_status": "active",
        "reference_scope": source.reference_scope,
        "official": "true",
        "isin": isin,
    }
    if sector:
        row["sector"] = sector
    return row


def parse_nse_india_equity_csv(
    text: str,
    source: MasterfileSource,
    *,
    source_url: str,
    sme: bool = False,
) -> list[dict[str, str]]:
    reader = csv.DictReader(io.StringIO(text.lstrip("\ufeff")))
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    allowed_series = {"SM", "ST"} if sme else {"EQ"}
    for raw_record in reader:
        record = normalize_nse_india_csv_record(raw_record)
        ticker = (record.get("SYMBOL") or "").strip().upper()
        series = (record.get("SERIES") or "").strip().upper()
        if series not in allowed_series or ticker.endswith("-RE") or ticker in seen:
            continue
        name = record.get("NAME OF COMPANY") or record.get("NAME_OF_COMPANY") or ""
        isin = record.get("ISIN NUMBER") or record.get("ISIN_NUMBER") or ""
        row = build_nse_india_row(
            source,
            ticker=ticker,
            name=name,
            isin=isin,
            asset_type="Stock",
            source_url=source_url,
        )
        if row:
            rows.append(row)
            seen.add(ticker)
    return rows


def parse_nse_india_etf_csv(
    text: str,
    source: MasterfileSource,
    *,
    source_url: str,
) -> list[dict[str, str]]:
    reader = csv.DictReader(io.StringIO(text.lstrip("\ufeff")))
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for raw_record in reader:
        record = normalize_nse_india_csv_record(raw_record)
        ticker = (record.get("Symbol") or record.get("SYMBOL") or "").strip().upper()
        if not ticker or ticker in seen:
            continue
        underlying = record.get("Underlying", "")
        security_name = record.get("SecurityName", "")
        isin = record.get("ISINNumber", "")
        row = build_nse_india_row(
            source,
            ticker=ticker,
            name=security_name or underlying,
            isin=isin,
            asset_type="ETF",
            source_url=source_url,
            sector=normalize_nse_india_etf_category(underlying, security_name),
        )
        if row:
            rows.append(row)
            seen.add(ticker)
    return rows


def fetch_nse_india_securities_available_rows(
    source: MasterfileSource,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    session = session or requests.Session()
    rows: list[dict[str, str]] = []
    for url, parser in (
        (NSE_IN_EQUITY_LIST_URL, lambda text: parse_nse_india_equity_csv(text, source, source_url=url)),
        (
            NSE_IN_SME_EQUITY_LIST_URL,
            lambda text: parse_nse_india_equity_csv(text, source, source_url=url, sme=True),
        ),
        (NSE_IN_ETF_LIST_URL, lambda text: parse_nse_india_etf_csv(text, source, source_url=url)),
    ):
        response = session.get(url, headers=nse_india_headers(), timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        rows.extend(parser(response.text))
    if not rows:
        raise ValueError("Unexpected NSE India securities-available payload")
    return dedupe_rows(rows)


def load_nse_india_securities_available_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    for path in (NSE_IN_SECURITIES_AVAILABLE_CACHE, LEGACY_NSE_IN_SECURITIES_AVAILABLE_CACHE):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"
    try:
        rows = fetch_nse_india_securities_available_rows(source, session=session)
    except (requests.RequestException, ValueError, json.JSONDecodeError):
        return None, "unavailable"

    ensure_output_dirs()
    NSE_IN_SECURITIES_AVAILABLE_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def bse_india_headers() -> dict[str, str]:
    return {
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://www.bseindia.com",
        "Referer": BSE_IN_SCRIPS_PAGE_URL,
        "User-Agent": BROWSER_USER_AGENT,
    }


def infer_bse_india_asset_type(record: dict[str, Any]) -> str:
    ticker = str(record.get("scrip_id") or "").strip().upper()
    name = str(record.get("Scrip_Name") or "").strip().upper()
    issuer = str(record.get("Issuer_Name") or "").strip().upper()
    if (
        " ETF" in f" {name} "
        or ticker.endswith("BEES")
        or "IETF" in ticker
        or ("MUTUAL FUND" in issuer and any(marker in name for marker in ("NIFTY", "SENSEX", "GOLD", "LIQUID")))
    ):
        return "ETF"
    return "Stock"


def parse_bse_india_scrips_payload(payload: list[dict[str, Any]], source: MasterfileSource) -> list[dict[str, str]]:
    if not isinstance(payload, list):
        raise ValueError("Unexpected BSE India scrips payload")
    rows: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for record in payload:
        if not isinstance(record, dict):
            continue
        if str(record.get("Status") or "").strip().lower() != "active":
            continue
        if str(record.get("Segment") or "").strip() != "Equity":
            continue
        ticker = str(record.get("scrip_id") or "").strip().upper()
        name = str(record.get("Scrip_Name") or record.get("Issuer_Name") or "").strip()
        isin = str(record.get("ISIN_NUMBER") or "").strip().upper()
        asset_type = infer_bse_india_asset_type(record)
        key = (ticker, asset_type)
        if not ticker or not name or not is_valid_isin(isin) or key in seen:
            continue
        row = {
            "source_key": source.key,
            "provider": source.provider,
            "source_url": source.source_url,
            "ticker": ticker,
            "name": name,
            "exchange": "BSE_IN",
            "asset_type": asset_type,
            "listing_status": "active",
            "reference_scope": source.reference_scope,
            "official": "true",
            "isin": isin,
        }
        if asset_type == "ETF":
            sector = normalize_nse_india_etf_category("", name)
            if sector:
                row["sector"] = sector
        rows.append(row)
        seen.add(key)
    if not rows:
        raise ValueError("Unexpected BSE India scrips payload")
    return rows


def fetch_bse_india_scrips_rows(
    source: MasterfileSource,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    session = session or requests.Session()
    response = session.get(
        BSE_IN_SCRIPS_API_URL,
        headers=bse_india_headers(),
        params={"Group": "", "Scripcode": "", "industry": "", "segment": "Equity", "status": "Active"},
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return parse_bse_india_scrips_payload(response.json(), source)


def load_bse_india_scrips_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    for path in (BSE_IN_SCRIPS_CACHE, LEGACY_BSE_IN_SCRIPS_CACHE):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"
    try:
        rows = fetch_bse_india_scrips_rows(source, session=session)
    except (requests.RequestException, ValueError, json.JSONDecodeError):
        return None, "unavailable"

    ensure_output_dirs()
    BSE_IN_SCRIPS_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def normalize_hkex_etp_category(sub_category: str, name: str) -> str:
    value = f"{sub_category} {name}".lower()
    if "leveraged" in value or "inverse" in value:
        return "Leveraged/Inverse"
    if any(marker in value for marker in ("bond", "treasury", "money market", "deposit", "income")):
        return "Fixed Income"
    if any(marker in value for marker in ("gold", "silver", "commodity", "oil")):
        return "Commodity"
    if any(marker in value for marker in ("reit", "property", "real estate")):
        return "Real Estate"
    if "currency" in value:
        return "Currency"
    if "multi" in value or "asset" in value:
        return "Multi-Asset"
    return "Equity"


def parse_hkex_securities_list_workbook(content: bytes, source: MasterfileSource) -> list[dict[str, str]]:
    dataframe = pd.read_excel(io.BytesIO(content), sheet_name=0, header=2, dtype=str)
    rows: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for record in dataframe.to_dict(orient="records"):
        ticker = str(record.get("Stock Code") or "").strip().zfill(5)
        name = str(record.get("Name of Securities") or "").strip()
        category = str(record.get("Category") or "").strip()
        sub_category = str(record.get("Sub-Category") or "").strip()
        isin = str(record.get("ISIN") or "").strip().upper()
        if not ticker or not name or not is_valid_isin(isin):
            continue
        if category == "Exchange Traded Products":
            asset_type = "ETF"
            sector = normalize_hkex_etp_category(sub_category, name)
        elif category == "Real Estate Investment Trusts":
            asset_type = "Stock"
            sector = "Real Estate"
        elif category == "Equity":
            asset_type = "Stock"
            sector = ""
        else:
            continue
        key = (ticker, asset_type)
        if key in seen:
            continue
        row = {
            "source_key": source.key,
            "provider": source.provider,
            "source_url": source.source_url,
            "ticker": ticker,
            "name": name,
            "exchange": "HKEX",
            "asset_type": asset_type,
            "listing_status": "active",
            "reference_scope": source.reference_scope,
            "official": "true",
            "isin": isin,
        }
        if sector:
            row["sector"] = sector
        rows.append(row)
        seen.add(key)
    if not rows:
        raise ValueError("Unexpected HKEX securities list workbook")
    return rows


def fetch_hkex_securities_list_rows(
    source: MasterfileSource,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    session = session or requests.Session()
    response = session.get(source.source_url, headers={"User-Agent": BROWSER_USER_AGENT}, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    content = response.content
    return parse_hkex_securities_list_workbook(content, source)


def load_hkex_securities_list_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    for path in (HKEX_SECURITIES_LIST_CACHE, LEGACY_HKEX_SECURITIES_LIST_CACHE):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"
    try:
        rows = fetch_hkex_securities_list_rows(source, session=session)
    except (requests.RequestException, ValueError, json.JSONDecodeError):
        return None, "unavailable"

    ensure_output_dirs()
    HKEX_SECURITIES_LIST_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


SGX_SECURITIES_PRICE_ASSET_TYPES = {
    "stocks": ("Stock", ""),
    "adrs": ("Stock", ""),
    "businesstrusts": ("Stock", ""),
    "reits": ("Stock", "Real Estate"),
    "etfs": ("ETF", ""),
}


def sgx_request_headers() -> dict[str, str]:
    return {
        "Accept": "application/json",
        "Referer": SGX_SECURITIES_PRICES_PAGE_URL,
        "User-Agent": BROWSER_USER_AGENT,
    }


def parse_sgx_securities_prices_payload(payload: dict[str, Any], source: MasterfileSource) -> list[dict[str, str]]:
    prices = (payload.get("data") or {}).get("prices")
    if not isinstance(prices, list):
        raise ValueError("Unexpected SGX securities prices payload")
    rows: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for record in prices:
        if not isinstance(record, dict):
            continue
        mapping = SGX_SECURITIES_PRICE_ASSET_TYPES.get(str(record.get("type") or "").strip().lower())
        if not mapping:
            continue
        asset_type, default_sector = mapping
        ticker = str(record.get("nc") or "").strip().upper()
        name = str(record.get("n") or record.get("issuer-name") or "").strip()
        if not ticker or not name:
            continue
        key = (ticker, asset_type)
        if key in seen:
            continue
        row = {
            "source_key": source.key,
            "provider": source.provider,
            "source_url": SGX_SECURITIES_PRICES_API_URL,
            "ticker": ticker,
            "name": name,
            "exchange": "SGX",
            "asset_type": asset_type,
            "listing_status": "active",
            "reference_scope": source.reference_scope,
            "official": "true",
        }
        sector = default_sector
        if asset_type == "ETF":
            sector = normalize_hkex_etp_category("", name)
        if sector:
            row["sector"] = sector
        rows.append(row)
        seen.add(key)
    if not rows:
        raise ValueError("Unexpected SGX securities prices payload")
    return rows


def fetch_sgx_securities_prices_rows(
    source: MasterfileSource,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    session = session or requests.Session()
    response = session.get(
        SGX_SECURITIES_PRICES_API_URL,
        headers=sgx_request_headers(),
        params={"excludetypes": "bonds"},
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return parse_sgx_securities_prices_payload(response.json(), source)


def load_sgx_securities_prices_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    for path in (SGX_SECURITIES_PRICES_CACHE, LEGACY_SGX_SECURITIES_PRICES_CACHE):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"
    try:
        rows = fetch_sgx_securities_prices_rows(source, session=session)
    except (requests.RequestException, ValueError, json.JSONDecodeError):
        return None, "unavailable"

    ensure_output_dirs()
    SGX_SECURITIES_PRICES_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


DFM_MARKET_CODES = {"510", "520"}
DFM_ASSET_TYPE_BY_INSTRUMENT_TYPE = {
    "equities": ("Stock", ""),
    "realestateinvestmenttrust": ("Stock", "Real Estate"),
    "exchangetradedfunds": ("ETF", ""),
    "funds": ("ETF", ""),
}


def dfm_request_headers(*, form: bool = False) -> dict[str, str]:
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Referer": DFM_LISTED_SECURITIES_PAGE_URL,
        "User-Agent": BROWSER_USER_AGENT,
    }
    if form:
        headers["Content-Type"] = "application/x-www-form-urlencoded"
    return headers


def normalize_dfm_instrument_type(value: str) -> str:
    return re.sub(r"[^a-z]", "", str(value or "").strip().lower())


def normalize_dfm_etf_category(name: str) -> str:
    value = name.lower()
    if any(marker in value for marker in ("bitcoin", "ether", "crypto")):
        return "Alternative"
    return normalize_hkex_etp_category("", name)


def parse_dfm_company_profile_payloads(
    profiles: list[dict[str, Any]],
    source: MasterfileSource,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for profile in profiles:
        if not isinstance(profile, dict):
            continue
        if str(profile.get("Market") or "").strip() != "DFM":
            continue
        instrument_type = normalize_dfm_instrument_type(str(profile.get("InstrumentType") or ""))
        mapping = DFM_ASSET_TYPE_BY_INSTRUMENT_TYPE.get(instrument_type)
        if mapping is None:
            continue
        asset_type, default_sector = mapping
        ticker = str(profile.get("Symbol") or "").strip().upper()
        name = str(profile.get("FullName") or profile.get("ShortName") or "").strip()
        isin = str(profile.get("ISIN") or "").strip().upper()
        key = (ticker, asset_type)
        if not ticker or not name or not is_valid_isin(isin) or key in seen:
            continue
        row = {
            "source_key": source.key,
            "provider": source.provider,
            "source_url": DFM_COMPANY_PROFILE_API_URL,
            "ticker": ticker,
            "name": name,
            "exchange": "DFM",
            "asset_type": asset_type,
            "listing_status": "active",
            "reference_scope": source.reference_scope,
            "official": "true",
            "isin": isin,
        }
        if asset_type == "ETF":
            sector = normalize_sector(str(profile.get("Sector") or ""), "ETF") or normalize_dfm_etf_category(name)
        else:
            sector = default_sector or normalize_sector(str(profile.get("Sector") or ""), "Stock")
            if not sector and "reit" in name.lower():
                sector = "Real Estate"
        if sector:
            row["sector"] = sector
        rows.append(row)
        seen.add(key)
    if not rows:
        raise ValueError("Unexpected DFM listed securities payload")
    return rows


def fetch_dfm_listed_securities_rows(
    source: MasterfileSource,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    session = session or requests.Session()
    response = session.get(DFM_STOCKS_API_URL, headers=dfm_request_headers(), timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    securities = response.json()
    if not isinstance(securities, list):
        raise ValueError("Unexpected DFM stocks payload")

    profiles: list[dict[str, Any]] = []
    for security in securities:
        if not isinstance(security, dict):
            continue
        if str(security.get("market") or "").strip() not in DFM_MARKET_CODES:
            continue
        symbol = str(security.get("id") or "").strip().upper()
        if not symbol:
            continue
        profile_response = session.post(
            DFM_COMPANY_PROFILE_API_URL,
            headers=dfm_request_headers(form=True),
            data={"Command": "companyprofile", "lang": "en", "symbol": symbol},
            timeout=REQUEST_TIMEOUT,
        )
        profile_response.raise_for_status()
        profile = profile_response.json()
        if isinstance(profile, dict):
            profiles.append(profile)
    return parse_dfm_company_profile_payloads(profiles, source)


def load_dfm_listed_securities_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    for path in (DFM_LISTED_SECURITIES_CACHE, LEGACY_DFM_LISTED_SECURITIES_CACHE):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"
    try:
        rows = fetch_dfm_listed_securities_rows(source, session=session)
    except (requests.RequestException, ValueError, json.JSONDecodeError):
        return None, "unavailable"

    ensure_output_dirs()
    DFM_LISTED_SECURITIES_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


BOURSA_KUWAIT_REGULAR_SUFFIXES = {"R"}
BOURSA_KUWAIT_REIT_SUFFIXES = {"F"}
BOURSA_KUWAIT_REGULAR_MARKETS = {"M", "P"}
BOURSA_KUWAIT_REIT_MARKETS = {"T"}
BOURSA_KUWAIT_PARAMS = {
    "UID": "3166765",
    "SID": "3090B191-7C82-49EE-AC52-706F081F265D",
    "L": "EN",
    "UNC": "0",
    "UE": "KSE",
    "H": "1",
    "M": "1",
    "RT": "303",
    "SRC": "KSE",
    "AS": "1",
}


def boursa_kuwait_request_headers() -> dict[str, str]:
    return {
        "Accept": "application/json, text/plain, */*",
        "Referer": BOURSA_KUWAIT_STOCKS_PAGE_URL,
        "User-Agent": BROWSER_USER_AGENT,
    }


def boursa_kuwait_header_index(header: str) -> dict[str, int]:
    return {field: index for index, field in enumerate(str(header or "").split("|"))}


def normalize_boursa_kuwait_sector(label: str) -> str:
    cleaned = str(label or "").strip()
    mapped = {
        "Telecommunications": "Communication Services",
        "Technology": "Information Technology",
        "Banking": "Financials",
        "Banks": "Financials",
        "Insurance": "Financials",
    }.get(cleaned, cleaned)
    return normalize_sector(mapped, "Stock")


def boursa_kuwait_sector_map(payload: dict[str, Any]) -> dict[str, str]:
    header = ((payload.get("HED") or {}).get("SCTD") or "").split("|")
    index = {field: position for position, field in enumerate(header)}
    if "SECTOR" not in index or "SECT_DSC" not in index:
        return {}
    sector_rows = (payload.get("DAT") or {}).get("SCTD") or []
    sectors: dict[str, str] = {}
    for row in sector_rows:
        values = str(row).split("|")
        code = values[index["SECTOR"]] if index["SECTOR"] < len(values) else ""
        label = values[index["SECT_DSC"]] if index["SECT_DSC"] < len(values) else ""
        sector = normalize_boursa_kuwait_sector(label)
        if code and sector:
            sectors[code] = sector
    return sectors


def parse_boursa_kuwait_legacy_mix_payload(
    payload: dict[str, Any],
    source: MasterfileSource,
) -> list[dict[str, str]]:
    rows_data = (payload.get("DAT") or {}).get("TD")
    header = (payload.get("HED") or {}).get("TD")
    if not isinstance(rows_data, list) or not isinstance(header, str):
        raise ValueError("Unexpected Boursa Kuwait market data payload")
    index = boursa_kuwait_header_index(header)
    required = {"SYMBOL", "SYMBOL_DESCRIPTION", "MARKET_ID", "INSTRUMENT_TYPE", "SECTOR", "ISIN_CODE"}
    if not required.issubset(index):
        raise ValueError("Unexpected Boursa Kuwait market data header")

    sector_map = boursa_kuwait_sector_map(payload)
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for raw_row in rows_data:
        values = str(raw_row).split("|")
        symbol_value = values[index["SYMBOL"]] if index["SYMBOL"] < len(values) else ""
        if "`" in symbol_value:
            ticker, suffix = symbol_value.split("`", 1)
        else:
            ticker, suffix = symbol_value, ""
        ticker = ticker.strip().upper()
        suffix = suffix.strip().upper()
        market_id = values[index["MARKET_ID"]].strip().upper() if index["MARKET_ID"] < len(values) else ""
        instrument_type = values[index["INSTRUMENT_TYPE"]].strip() if index["INSTRUMENT_TYPE"] < len(values) else ""
        if suffix in BOURSA_KUWAIT_REGULAR_SUFFIXES and market_id in BOURSA_KUWAIT_REGULAR_MARKETS:
            sector = sector_map.get(values[index["SECTOR"]].strip() if index["SECTOR"] < len(values) else "", "")
        elif suffix in BOURSA_KUWAIT_REIT_SUFFIXES and market_id in BOURSA_KUWAIT_REIT_MARKETS and instrument_type == "65":
            sector = "Real Estate"
        else:
            continue
        name = values[index["SYMBOL_DESCRIPTION"]].strip() if index["SYMBOL_DESCRIPTION"] < len(values) else ""
        isin = values[index["ISIN_CODE"]].strip().upper() if index["ISIN_CODE"] < len(values) else ""
        if not ticker or not name or not is_valid_isin(isin) or ticker in seen:
            continue
        row = {
            "source_key": source.key,
            "provider": source.provider,
            "source_url": BOURSA_KUWAIT_LEGACY_MIX_API_URL,
            "ticker": ticker,
            "name": name,
            "exchange": "BK",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": source.reference_scope,
            "official": "true",
            "isin": isin,
        }
        if sector:
            row["sector"] = sector
        rows.append(row)
        seen.add(ticker)
    if not rows:
        raise ValueError("Unexpected Boursa Kuwait market data payload")
    return rows


def fetch_boursa_kuwait_stocks_rows(
    source: MasterfileSource,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    session = session or requests.Session()
    response = session.get(
        BOURSA_KUWAIT_LEGACY_MIX_API_URL,
        headers=boursa_kuwait_request_headers(),
        params=BOURSA_KUWAIT_PARAMS,
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return parse_boursa_kuwait_legacy_mix_payload(response.json(), source)


def load_boursa_kuwait_stocks_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    for path in (BOURSA_KUWAIT_STOCKS_CACHE, LEGACY_BOURSA_KUWAIT_STOCKS_CACHE):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"
    try:
        rows = fetch_boursa_kuwait_stocks_rows(source, session=session)
    except (requests.RequestException, ValueError, json.JSONDecodeError):
        return None, "unavailable"

    ensure_output_dirs()
    BOURSA_KUWAIT_STOCKS_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


BAHRAIN_BOURSE_LOCAL_EQUITY_SECTIONS = {"Equities", "Bahrain Investment Market (BIM)"}


def bahrain_bourse_request_headers(accept: str = "text/html,*/*") -> dict[str, str]:
    return {
        "Accept": accept,
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": BAHRAIN_BOURSE_HOME_URL,
        "User-Agent": BROWSER_USER_AGENT,
    }


def bahrain_bourse_get(
    session: requests.Session,
    url: str,
    *,
    accept: str = "text/html,*/*",
):
    response = session.get(url, headers=bahrain_bourse_request_headers(accept), timeout=REQUEST_TIMEOUT)
    if response.status_code != 403:
        return response
    try:
        from curl_cffi import requests as curl_requests  # type: ignore[import-not-found]
    except ImportError:
        return response
    return curl_requests.get(
        url,
        headers=bahrain_bourse_request_headers(accept),
        impersonate="chrome120",
        timeout=REQUEST_TIMEOUT,
    )


def classify_bahrain_bourse_security(name: str, issue_description: str, fisn: str) -> tuple[str, str]:
    description = ascii_fold(issue_description).lower()
    combined = ascii_fold(f"{name} {issue_description} {fisn}").lower()
    if "common share" in description:
        return "Stock", ""
    if "mutual fund" in description and any(marker in combined for marker in ("reit", "realty", "real estate")):
        return "Stock", "Real Estate"
    return "", ""


def parse_bahrain_bourse_isin_codes_html(html: str, source: MasterfileSource) -> list[dict[str, str]]:
    parser = _TableParser()
    parser.feed(html)
    source_table = next(
        (
            table
            for table in parser.tables
            if table and {"BHB Company Symbol", "ISIN", "Issuer Name", "Issue Description"} <= set(table[0])
        ),
        None,
    )
    if not source_table:
        raise ValueError("Unexpected Bahrain Bourse ISIN code table")

    section = ""
    rows: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for cells in source_table[1:]:
        if len(cells) == 1:
            section = cells[0].strip()
            continue
        if len(cells) < 4 or section not in BAHRAIN_BOURSE_LOCAL_EQUITY_SECTIONS:
            continue
        ticker = cells[0].strip().upper()
        isin = cells[1].strip().upper()
        name = cells[2].strip()
        issue_description = cells[3].strip()
        fisn = cells[6].strip() if len(cells) > 6 else ""
        asset_type, sector = classify_bahrain_bourse_security(name, issue_description, fisn)
        key = (ticker, asset_type)
        if not ticker or not name or not is_valid_isin(isin) or not asset_type or key in seen:
            continue
        row = {
            "source_key": source.key,
            "provider": source.provider,
            "source_url": source.source_url,
            "ticker": ticker,
            "name": name,
            "exchange": "BHB",
            "asset_type": asset_type,
            "listing_status": "active",
            "reference_scope": source.reference_scope,
            "official": "true",
            "isin": isin,
        }
        if sector:
            row["sector"] = sector
        rows.append(row)
        seen.add(key)
    if not rows:
        raise ValueError("Unexpected Bahrain Bourse ISIN code table")
    return rows


def fetch_bahrain_bourse_listed_companies_rows(
    source: MasterfileSource,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    session = session or requests.Session()
    response = bahrain_bourse_get(session, BAHRAIN_BOURSE_ISIN_CODES_URL)
    response.raise_for_status()
    return parse_bahrain_bourse_isin_codes_html(response.text, source)


def load_bahrain_bourse_listed_companies_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    for path in (BAHRAIN_BOURSE_LISTED_COMPANIES_CACHE, LEGACY_BAHRAIN_BOURSE_LISTED_COMPANIES_CACHE):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"
    try:
        rows = fetch_bahrain_bourse_listed_companies_rows(source, session=session)
    except (requests.RequestException, ValueError):
        return None, "unavailable"

    ensure_output_dirs()
    BAHRAIN_BOURSE_LISTED_COMPANIES_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


BIST_KAP_NEXT_DATA_RE = re.compile(r"self\.__next_f\.push\(\[1,(\".*\")\]\)\s*$", re.S)
BIST_KAP_COMPANY_RE = re.compile(
    r'"mkkMemberOid":"(?P<company_id>[^"]+)",'
    r'"kapMemberTitle":"(?P<name>[^"]+)",'
    r'"relatedMemberTitle":"(?P<auditor>[^"]*)",'
    r'"stockCode":"(?P<stock_codes>[^"]+)",'
    r'"cityName":"(?P<city>[^"]*)"',
)
BIST_STOCK_CATEGORY = "HS"
BIST_ETF_CATEGORY = "BYF"
BIST_PRIMARY_SHARE_TAKAS_CODE = "eski"


def bist_request_headers() -> dict[str, str]:
    return {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "User-Agent": BROWSER_USER_AGENT,
    }


def bist_mkk_request_headers() -> dict[str, str]:
    return {
        "Accept": "application/json, text/plain, */*",
        "Referer": BIST_MKK_SECURITIES_PAGE_URL,
        "User-Agent": BROWSER_USER_AGENT,
    }


def normalize_bist_takas_code(value: str) -> str:
    return ascii_fold(str(value or "")).strip().lower()


def normalize_bist_etf_category(name: str) -> str:
    value = ascii_fold(name).lower()
    if any(marker in value for marker in ("altin", "gumus")):
        return "Commodity"
    if any(marker in value for marker in ("tlref", "tahvil", "borclanma", "para piyasasi")):
        return "Fixed Income"
    return normalize_hkex_etp_category("", name)


def parse_bist_kap_company_list(html: str) -> dict[str, str]:
    scripts = re.findall(r"<script[^>]*>(.*?)</script>", html, re.I | re.S)
    if not scripts:
        raise ValueError("Unexpected KAP BIST company page")
    script = max(scripts, key=len)
    match = BIST_KAP_NEXT_DATA_RE.search(script)
    if not match:
        raise ValueError("Unexpected KAP BIST company page")
    content = json.loads(match.group(1))

    companies: dict[str, str] = {}
    for company_match in BIST_KAP_COMPANY_RE.finditer(content):
        name = company_match.group("name").strip()
        for ticker in company_match.group("stock_codes").split(","):
            ticker = ticker.strip().upper()
            if ticker and name and ticker not in companies:
                companies[ticker] = name
    if not companies:
        raise ValueError("Unexpected KAP BIST company page")
    return companies


def parse_bist_kap_mkk_listed_securities_payload(
    kap_html: str,
    mkk_payload: list[dict[str, Any]],
    source: MasterfileSource,
) -> list[dict[str, str]]:
    kap_companies = parse_bist_kap_company_list(kap_html)

    stock_rows_by_ticker: dict[str, list[dict[str, Any]]] = {}
    etf_rows: list[dict[str, Any]] = []
    for item in mkk_payload:
        if not isinstance(item, dict):
            continue
        ticker = str(item.get("borsaKodu") or "").strip().upper()
        isin = str(item.get("isinKodu") or "").strip().upper()
        category = str(item.get("kategori") or item.get("kiymetSinifi") or "").strip()
        if not ticker or not is_valid_isin(isin):
            continue
        if category == BIST_STOCK_CATEGORY and ticker in kap_companies:
            stock_rows_by_ticker.setdefault(ticker, []).append(item)
        elif category == BIST_ETF_CATEGORY:
            etf_rows.append(item)

    rows: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for ticker, mkk_rows in sorted(stock_rows_by_ticker.items()):
        preferred = sorted(
            mkk_rows,
            key=lambda item: (
                normalize_bist_takas_code(str(item.get("takasKodu") or "")) != BIST_PRIMARY_SHARE_TAKAS_CODE,
                str(item.get("isinKodu") or ""),
            ),
        )[0]
        if normalize_bist_takas_code(str(preferred.get("takasKodu") or "")) != BIST_PRIMARY_SHARE_TAKAS_CODE:
            continue
        key = (ticker, "Stock")
        if key in seen:
            continue
        rows.append(
            {
                "source_key": source.key,
                "provider": source.provider,
                "source_url": BIST_LISTED_COMPANIES_PAGE_URL,
                "ticker": ticker,
                "name": kap_companies[ticker],
                "exchange": "BIST",
                "asset_type": "Stock",
                "listing_status": "active",
                "reference_scope": source.reference_scope,
                "official": "true",
                "isin": str(preferred.get("isinKodu") or "").strip().upper(),
            }
        )
        seen.add(key)

    for item in sorted(etf_rows, key=lambda row: str(row.get("borsaKodu") or "")):
        ticker = str(item.get("borsaKodu") or "").strip().upper()
        name = str(item.get("aciklama") or "").strip()
        isin = str(item.get("isinKodu") or "").strip().upper()
        key = (ticker, "ETF")
        if not ticker or not name or not is_valid_isin(isin) or key in seen:
            continue
        rows.append(
            {
                "source_key": source.key,
                "provider": source.provider,
                "source_url": BIST_MKK_SECURITIES_API_URL,
                "ticker": ticker,
                "name": name,
                "exchange": "BIST",
                "asset_type": "ETF",
                "listing_status": "active",
                "reference_scope": source.reference_scope,
                "official": "true",
                "isin": isin,
                "sector": normalize_bist_etf_category(name),
            }
        )
        seen.add(key)

    if not rows:
        raise ValueError("Unexpected KAP/MKK BIST securities payload")
    return rows


def fetch_bist_kap_mkk_listed_securities_rows(
    source: MasterfileSource,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    session = session or requests.Session()
    kap_response = session.get(BIST_LISTED_COMPANIES_PAGE_URL, headers=bist_request_headers(), timeout=REQUEST_TIMEOUT)
    kap_response.raise_for_status()
    mkk_response = session.get(
        BIST_MKK_SECURITIES_API_URL,
        headers=bist_mkk_request_headers(),
        timeout=REQUEST_TIMEOUT,
    )
    mkk_response.raise_for_status()
    mkk_payload = mkk_response.json()
    if not isinstance(mkk_payload, list):
        raise ValueError("Unexpected MKK securities payload")
    return parse_bist_kap_mkk_listed_securities_payload(kap_response.text, mkk_payload, source)


def load_bist_kap_mkk_listed_securities_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    for path in (BIST_KAP_MKK_LISTED_SECURITIES_CACHE, LEGACY_BIST_KAP_MKK_LISTED_SECURITIES_CACHE):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"
    try:
        rows = fetch_bist_kap_mkk_listed_securities_rows(source, session=session)
    except (requests.RequestException, ValueError, json.JSONDecodeError):
        return None, "unavailable"

    ensure_output_dirs()
    BIST_KAP_MKK_LISTED_SECURITIES_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


TADAWUL_ACTIVE_SECURITY_RE = re.compile(
    r"\{\s*company:\s*\"(?P<symbol>[^\"]+)\",\s*"
    r"companyDisplay:\s*\"(?P<name>[^\"]+)\"(?P<body>.*?)"
    r"link:\s*\"(?P<link>[^\"]+)\"",
    re.S,
)
TADAWUL_STOCK_MARKET_TYPES = {"M", "S"}
TADAWUL_FUND_MARKET_TYPES = {"C", "E"}


def tadawul_request_headers(accept: str = "text/html,*/*") -> dict[str, str]:
    return {
        "Accept": accept,
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": TADAWUL_ISSUERS_DIRECTORY_URL,
        "User-Agent": BROWSER_USER_AGENT,
    }


def tadawul_get(
    session: requests.Session,
    url: str,
    *,
    accept: str = "text/html,*/*",
):
    response = session.get(url, headers=tadawul_request_headers(accept), timeout=REQUEST_TIMEOUT)
    if response.status_code != 403:
        return response
    try:
        from curl_cffi import requests as curl_requests  # type: ignore[import-not-found]
    except ImportError:
        return response
    return curl_requests.get(
        url,
        headers=tadawul_request_headers(accept),
        impersonate="chrome120",
        timeout=REQUEST_TIMEOUT,
    )


def parse_tadawul_active_symbols(html: str) -> set[str]:
    active: set[str] = set()
    for match in TADAWUL_ACTIVE_SECURITY_RE.finditer(html):
        symbol = match.group("symbol").strip().upper()
        link = match.group("link")
        if symbol and "company-profile" in link:
            active.add(symbol)
    if not active:
        raise ValueError("Unexpected Saudi Exchange issuer directory payload")
    return active


def normalize_tadawul_fund_category(name: str, market_type: str) -> str:
    if market_type == "C":
        return "Multi-Asset"
    value = ascii_fold(name).lower()
    if "gold" in value:
        return "Commodity"
    if "sukuk" in value or "government" in value or "sovereign" in value:
        return "Fixed Income"
    if "multi asset" in value:
        return "Multi-Asset"
    return normalize_hkex_etp_category("", name)


def parse_tadawul_securities_payload(
    issuer_directory_html: str,
    search_payload: list[dict[str, Any]],
    source: MasterfileSource,
) -> list[dict[str, str]]:
    active_symbols = parse_tadawul_active_symbols(issuer_directory_html)
    rows: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for item in search_payload:
        if not isinstance(item, dict):
            continue
        symbol = str(item.get("symbol") or "").strip().upper()
        market_type = str(item.get("market_type") or "").strip().upper()
        name = str(item.get("tradingNameEn") or item.get("companyNameEN") or "").strip()
        company_name = str(item.get("companyNameEN") or name).strip()
        isin = str(item.get("isin") or "").strip().upper()
        if not symbol or not name or not is_valid_isin(isin):
            continue
        if market_type in TADAWUL_STOCK_MARKET_TYPES:
            if symbol not in active_symbols:
                continue
            asset_type = "Stock"
            sector = "Real Estate" if "reit" in f"{name} {company_name}".lower() else ""
        elif market_type in TADAWUL_FUND_MARKET_TYPES:
            if market_type != "E" and symbol not in active_symbols:
                continue
            asset_type = "ETF"
            sector = normalize_tadawul_fund_category(f"{name} {company_name}", market_type)
        else:
            continue
        key = (symbol, asset_type)
        if key in seen:
            continue
        row = {
            "source_key": source.key,
            "provider": source.provider,
            "source_url": TADAWUL_THEME_SEARCH_URL,
            "ticker": symbol,
            "name": name,
            "exchange": "TADAWUL",
            "asset_type": asset_type,
            "listing_status": "active",
            "reference_scope": source.reference_scope,
            "official": "true",
            "isin": isin,
        }
        if sector:
            row["sector"] = sector
        rows.append(row)
        seen.add(key)
    if not rows:
        raise ValueError("Unexpected Saudi Exchange symbol payload")
    return rows


def fetch_tadawul_securities_rows(
    source: MasterfileSource,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    session = session or requests.Session()
    directory_response = tadawul_get(session, TADAWUL_ISSUERS_DIRECTORY_URL)
    directory_response.raise_for_status()
    search_response = tadawul_get(session, TADAWUL_THEME_SEARCH_URL, accept="application/json, text/javascript, */*; q=0.01")
    search_response.raise_for_status()
    search_payload = search_response.json()
    if not isinstance(search_payload, list):
        raise ValueError("Unexpected Saudi Exchange search payload")
    return parse_tadawul_securities_payload(directory_response.text, search_payload, source)


def load_tadawul_securities_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    for path in (TADAWUL_SECURITIES_CACHE, LEGACY_TADAWUL_SECURITIES_CACHE):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"
    try:
        rows = fetch_tadawul_securities_rows(source, session=session)
    except (requests.RequestException, ValueError, json.JSONDecodeError):
        return None, "unavailable"

    ensure_output_dirs()
    TADAWUL_SECURITIES_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


ADX_STOCK_BOARDS = {"EQTY", "PRCN"}
ADX_ETF_BOARDS = {"FUND"}
ADX_EXCLUDED_BOARDS = {"DUAL", "SPCW"}
ADX_STOCK_SECTOR_MAP = {
    "Industrial": "Industrials",
    "Technology": "Information Technology",
    "Telecommunication": "Communication Services",
}


def adx_request_headers() -> dict[str, str]:
    return {
        "Accept": "application/json",
        "Channel-ID": "OSS WEB",
        "Content-Type": "application/json",
        "Origin": "https://www.adx.ae",
        "Referer": "https://www.adx.ae/",
        "User-Agent": BROWSER_USER_AGENT,
        "X-Correlation-ID": "uuid",
        "adx-Gateway-APIKey": ADX_GATEWAY_API_KEY,
    }


def adx_get_json(session: requests.Session, url: str) -> dict[str, Any]:
    response = session.get(url, headers=adx_request_headers(), timeout=REQUEST_TIMEOUT)
    if response.status_code == 403:
        try:
            from curl_cffi import requests as curl_requests  # type: ignore[import-not-found]
        except ImportError:
            response.raise_for_status()
        response = curl_requests.get(
            url,
            headers=adx_request_headers(),
            impersonate="chrome120",
            timeout=REQUEST_TIMEOUT,
        )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("Unexpected ADX API payload")
    return payload


def adx_response_list(payload: dict[str, Any], *path: str) -> list[dict[str, Any]]:
    value: Any = payload
    for key in path:
        if not isinstance(value, dict):
            raise ValueError("Unexpected ADX API payload")
        value = value.get(key)
    if not isinstance(value, list):
        raise ValueError("Unexpected ADX API payload")
    return [item for item in value if isinstance(item, dict)]


def normalize_adx_stock_sector(label: str) -> str:
    cleaned = str(label or "").strip()
    mapped = ADX_STOCK_SECTOR_MAP.get(cleaned, cleaned)
    return normalize_sector(mapped, "Stock")


def normalize_adx_etf_category(name: str) -> str:
    value = ascii_fold(name).lower()
    if any(marker in value for marker in ("sukuk", "bond", "treasury", "t-bill", "bill")):
        return "Fixed Income"
    if any(marker in value for marker in ("gold", "silver", "commodity", "oil", "carbon")):
        return "Commodity"
    if any(marker in value for marker in ("reit", "property", "real estate")):
        return "Real Estate"
    if "currency" in value:
        return "Currency"
    if "multi asset" in value:
        return "Multi-Asset"
    return "Equity"


def parse_adx_market_watch_payloads(
    issuers_payload: dict[str, Any],
    main_market_payload: dict[str, Any],
    growth_market_payload: dict[str, Any],
    etf_payload: dict[str, Any],
    symbol_sector_payload: dict[str, Any],
    source: MasterfileSource,
) -> list[dict[str, str]]:
    issuer_rows = adx_response_list(issuers_payload, "response", "issuers")
    sector_rows = adx_response_list(symbol_sector_payload, "response", "companies", "company")

    sector_by_symbol = {
        str(item.get("symbol") or "").strip().upper(): normalize_adx_stock_sector(str(item.get("sectorNameEn") or ""))
        for item in sector_rows
    }
    boards_by_symbol: dict[str, str] = {}
    market_rows: list[tuple[dict[str, Any], str, str]] = []
    for payload, asset_type in (
        (main_market_payload, "Stock"),
        (growth_market_payload, "Stock"),
        (etf_payload, "ETF"),
    ):
        for item in adx_response_list(payload, "response", "results"):
            symbol = str(item.get("companySymbol") or "").strip().upper()
            board = str(item.get("boardId") or "").strip().upper()
            if symbol:
                boards_by_symbol[symbol] = board
                market_rows.append((item, asset_type, board))

    issuer_by_symbol = {
        str(item.get("tradingCode") or item.get("eqCode") or item.get("dSymbol") or "").strip().upper(): item
        for item in issuer_rows
        if str(item.get("status") or "").strip().upper() == "L"
    }

    rows: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for market_item, asset_type, board in market_rows:
        if board in ADX_EXCLUDED_BOARDS:
            continue
        if asset_type == "Stock" and board not in ADX_STOCK_BOARDS:
            continue
        if asset_type == "ETF" and board not in ADX_ETF_BOARDS:
            continue
        ticker = str(market_item.get("companySymbol") or "").strip().upper()
        issuer = issuer_by_symbol.get(ticker, {})
        name = str(issuer.get("nameEnglish") or market_item.get("companyID") or "").strip()
        isin = str(issuer.get("isin") or market_item.get("companyISIN") or "").strip().upper()
        key = (ticker, asset_type)
        if not ticker or not name or not is_valid_isin(isin) or key in seen:
            continue
        row = {
            "source_key": source.key,
            "provider": source.provider,
            "source_url": ADX_MARKET_WATCH_PAGE_URL,
            "ticker": ticker,
            "name": name,
            "exchange": "ADX",
            "asset_type": asset_type,
            "listing_status": "active",
            "reference_scope": source.reference_scope,
            "official": "true",
            "isin": isin,
        }
        if asset_type == "Stock":
            sector = sector_by_symbol.get(ticker) or normalize_adx_stock_sector(str(issuer.get("sectorNameArabic") or ""))
        else:
            sector = normalize_adx_etf_category(name)
        if sector:
            row["sector"] = sector
        rows.append(row)
        seen.add(key)
    if not rows:
        raise ValueError("Unexpected ADX market-watch payload")
    return rows


def fetch_adx_market_watch_rows(
    source: MasterfileSource,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    session = session or requests.Session()
    return parse_adx_market_watch_payloads(
        adx_get_json(session, ADX_ISSUERS_API_URL),
        adx_get_json(session, ADX_MAIN_MARKET_API_URL),
        adx_get_json(session, ADX_GROWTH_MARKET_API_URL),
        adx_get_json(session, ADX_ETF_API_URL),
        adx_get_json(session, ADX_SYMBOL_SECTOR_API_URL),
        source,
    )


def load_adx_market_watch_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    for path in (ADX_MARKET_WATCH_CACHE, LEGACY_ADX_MARKET_WATCH_CACHE):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"
    try:
        rows = fetch_adx_market_watch_rows(source, session=session)
    except (requests.RequestException, ValueError, json.JSONDecodeError):
        return None, "unavailable"

    ensure_output_dirs()
    ADX_MARKET_WATCH_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


QSE_COMP_TYPE_ASSET_TYPE = {"COMP": "Stock", "V": "Stock", "ETF": "ETF"}
QSE_STOCK_SECTOR_MAP = {
    "Banks & Financial Services": "Financials",
    "Insurance": "Financials",
    "Industrials": "Industrials",
    "Real Estate": "Real Estate",
    "Telecoms": "Communication Services",
    "Transportation": "Industrials",
}
QSE_ETF_CATEGORY_MAP = {"ETF": "Equity"}


def qse_request_headers() -> dict[str, str]:
    return {
        "Accept": "text/plain, application/json, */*",
        "Referer": QSE_LISTED_COMPANIES_URL,
        # The official host intermittently resets browser-like requests from this environment.
        "User-Agent": "curl/8.0",
    }


def normalize_qse_sector(record: dict[str, Any], asset_type: str) -> str:
    sector = str(record.get("SectorEN") or "").strip()
    if asset_type == "ETF":
        return QSE_ETF_CATEGORY_MAP.get(sector, "")
    return QSE_STOCK_SECTOR_MAP.get(sector, "")


def parse_qse_market_watch_payload(payload: dict[str, Any], source: MasterfileSource) -> list[dict[str, str]]:
    records = payload.get("rows")
    if not isinstance(records, list):
        raise ValueError("Unexpected QSE MarketWatch payload")
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for record in records:
        if not isinstance(record, dict):
            continue
        comp_type = str(record.get("CompType") or "").strip().upper()
        asset_type = QSE_COMP_TYPE_ASSET_TYPE.get(comp_type, "")
        ticker = str(record.get("Symbol") or "").strip().upper()
        name = str(record.get("CompanyEN") or "").strip()
        if not asset_type or not ticker or not name or ticker in seen:
            continue
        row = {
            "source_key": source.key,
            "provider": source.provider,
            "source_url": source.source_url,
            "ticker": ticker,
            "name": name,
            "exchange": "QSE",
            "asset_type": asset_type,
            "listing_status": "active",
            "reference_scope": source.reference_scope,
            "official": "true",
        }
        sector = normalize_qse_sector(record, asset_type)
        if sector:
            row["sector"] = sector
        rows.append(row)
        seen.add(ticker)
    if not rows:
        raise ValueError("Unexpected QSE MarketWatch payload")
    return rows


def fetch_qse_market_watch_rows(
    source: MasterfileSource,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    session = session or requests.Session()
    response = session.get(QSE_MARKET_WATCH_URL, headers=qse_request_headers(), timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return parse_qse_market_watch_payload(response.json(), source)


def load_qse_market_watch_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    for path in (QSE_MARKET_WATCH_CACHE, LEGACY_QSE_MARKET_WATCH_CACHE):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"
    try:
        rows = fetch_qse_market_watch_rows(source, session=session)
    except (requests.RequestException, ValueError, json.JSONDecodeError):
        return None, "unavailable"

    ensure_output_dirs()
    QSE_MARKET_WATCH_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


MSX_SUBSECTOR_STOCK_SECTOR_MAP = {
    "1": "Financial Services",
    "2": "Financial Services",
    "3": "Financial Services",
    "4": "Financial Services",
    "6": "Consumer Staples",
    "7": "Materials",
    "8": "Industrials",
    "9": "Consumer Discretionary",
    "10": "Materials",
    "11": "Materials",
    "12": "Materials",
    "13": "Materials",
    "14": "Health Care",
    "15": "Industrials",
    "16": "Communication Services",
    "17": "Consumer Discretionary",
    "18": "Industrials",
    "19": "Energy",
    "20": "Consumer Discretionary",
    "21": "Utilities",
    "22": "Industrials",
    "23": "Health Care",
    "24": "Real Estate",
    "25": "Energy",
    "26": "Materials",
}
MSX_SECTOR_STOCK_SECTOR_FALLBACK = {
    "1": "Financial Services",
    "3": "Industrials",
    "5": "Consumer Discretionary",
}


def msx_request_headers(referer: str = MSX_COMPANIES_URL) -> dict[str, str]:
    return {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Content-Type": "application/json; charset=UTF-8",
        "Referer": referer,
        "User-Agent": BROWSER_USER_AGENT,
        "X-Requested-With": "XMLHttpRequest",
    }


def normalize_msx_stock_sector(record: dict[str, Any]) -> str:
    return MSX_SUBSECTOR_STOCK_SECTOR_MAP.get(str(record.get("SubSector") or "").strip()) or MSX_SECTOR_STOCK_SECTOR_FALLBACK.get(
        str(record.get("Sector") or "").strip(),
        "",
    )


def parse_msx_companies_payload(payload: dict[str, Any], source: MasterfileSource) -> list[dict[str, str]]:
    records = payload.get("d")
    if not isinstance(records, list):
        raise ValueError("Unexpected MSX companies payload")
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for record in records:
        if not isinstance(record, dict):
            continue
        listed_status = str(record.get("Listed") or "").strip()
        sector_code = str(record.get("Sector") or "").strip()
        ticker = str(record.get("Symbol") or "").strip().upper()
        name = str(record.get("LongNameEn") or "").strip()
        upper_name = name.upper()
        if (
            listed_status not in {"1", "2"}
            or sector_code not in {"1", "3", "5"}
            or not ticker
            or not name
            or ticker in seen
            or "RIGHT ISSUE" in upper_name
            or upper_name.endswith("-R I")
        ):
            continue
        row = {
            "source_key": source.key,
            "provider": source.provider,
            "source_url": source.source_url,
            "ticker": ticker,
            "name": name,
            "exchange": "MSX",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": source.reference_scope,
            "official": "true",
        }
        sector = normalize_msx_stock_sector(record)
        if sector:
            row["sector"] = sector
        rows.append(row)
        seen.add(ticker)
    if not rows:
        raise ValueError("Unexpected MSX companies payload")
    return rows


def fetch_msx_companies_rows(
    source: MasterfileSource,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    session = session or requests.Session()
    response = session.post(
        MSX_COMPANIES_LIST_URL,
        headers=msx_request_headers(source.source_url),
        data="{}",
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return parse_msx_companies_payload(response.json(), source)


def load_msx_companies_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    for path in (MSX_COMPANIES_CACHE, LEGACY_MSX_COMPANIES_CACHE):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"
    try:
        rows = fetch_msx_companies_rows(source, session=session)
    except (requests.RequestException, ValueError, json.JSONDecodeError):
        return None, "unavailable"

    ensure_output_dirs()
    MSX_COMPANIES_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def sem_sheet_header_index(df: pd.DataFrame) -> int:
    for index, row in df.iterrows():
        values = {str(value).strip().upper() for value in row.tolist() if pd.notna(value)}
        if {"ISSUER NAME", "ISSUE DESCRIPTION", "ISIN"} <= values:
            return int(index)
    raise ValueError("Unexpected SEM ISIN workbook header")


def parse_sem_isin_workbook(
    content: bytes,
    source: MasterfileSource,
    *,
    listings_path: Path = LISTINGS_CSV,
) -> list[dict[str, str]]:
    workbook = pd.ExcelFile(io.BytesIO(content))
    listings_by_isin = listing_rows_by_exchange_isin("SEM", listings_path=listings_path)
    rows: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for sheet_name in ("Official Market", "DEM"):
        if sheet_name not in workbook.sheet_names:
            continue
        raw_df = pd.read_excel(workbook, sheet_name=sheet_name, header=None, dtype=str)
        header_index = sem_sheet_header_index(raw_df)
        header = [str(value).strip().upper() if pd.notna(value) else "" for value in raw_df.iloc[header_index].tolist()]
        isin_col = header.index("ISIN")
        for _, raw_row in raw_df.iloc[header_index + 1 :].iterrows():
            isin = str(raw_row.iloc[isin_col] if len(raw_row) > isin_col else "").strip().upper()
            if not is_valid_isin(isin):
                continue
            listing = listings_by_isin.get(isin)
            if not listing or listing.get("asset_type") not in {"Stock", "ETF"}:
                continue
            key = (listing["ticker"], listing["asset_type"])
            if key in seen:
                continue
            row = {
                "source_key": source.key,
                "provider": source.provider,
                "source_url": SEM_ISIN_PAGE_URL,
                "ticker": listing["ticker"],
                "name": listing.get("name", "").strip(),
                "exchange": "SEM",
                "asset_type": listing["asset_type"],
                "listing_status": "active",
                "reference_scope": source.reference_scope,
                "official": "true",
                "isin": isin,
            }
            sector = listing.get("stock_sector") or listing.get("etf_category") or listing.get("sector") or ""
            if sector:
                row["sector"] = sector
            rows.append(row)
            seen.add(key)

    if not rows:
        raise ValueError("Unexpected SEM ISIN workbook payload")
    return rows


def fetch_sem_isin_rows(
    source: MasterfileSource,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    session = session or requests.Session()
    response = session.get(
        source.source_url,
        headers={"User-Agent": BROWSER_USER_AGENT},
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return parse_sem_isin_workbook(response.content, source)


def load_sem_isin_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    for path in (SEM_ISIN_CACHE, LEGACY_SEM_ISIN_CACHE):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"
    try:
        rows = fetch_sem_isin_rows(source, session=session)
    except (requests.RequestException, ValueError, json.JSONDecodeError):
        return None, "unavailable"

    ensure_output_dirs()
    SEM_ISIN_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def parse_bvb_instrument_directory_html(
    text: str,
    source: MasterfileSource,
    *,
    asset_type: str,
    category_filter: str | None = None,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for row_match in BVB_INSTRUMENT_ROW_RE.finditer(text):
        row_html = row_match.group("body")
        link_match = BVB_INSTRUMENT_LINK_RE.search(row_html)
        isin_match = BVB_INSTRUMENT_ISIN_RE.search(row_html)
        cells = HTML_TD_RE.findall(row_html)
        if link_match is None or isin_match is None or len(cells) < 2:
            continue

        ticker = clean_html_text(link_match.group("ticker")).strip().upper()
        href_ticker = clean_html_text(link_match.group("href_ticker")).strip().upper()
        isin = clean_html_text(isin_match.group("isin")).strip().upper()
        name = clean_html_text(cells[1]).rstrip(";").strip()
        category = clean_html_text(cells[-1]).strip()
        if category_filter and category_filter.upper() not in category.upper():
            continue
        if not ticker or ticker != href_ticker or ticker in seen or not name or not is_valid_isin(isin):
            continue
        rows.append(
            {
                "source_key": source.key,
                "provider": source.provider,
                "source_url": urljoin(source.source_url, link_match.group("href")),
                "ticker": ticker,
                "name": name,
                "exchange": "BVB",
                "asset_type": asset_type,
                "listing_status": "active",
                "reference_scope": source.reference_scope,
                "official": "true",
                "isin": isin,
            }
        )
        seen.add(ticker)
    if not rows:
        raise ValueError("Unexpected BVB instrument directory HTML")
    return rows


def parse_bvb_shares_directory_html(text: str, source: MasterfileSource) -> list[dict[str, str]]:
    return parse_bvb_instrument_directory_html(text, source, asset_type="Stock")


def parse_bvb_fund_units_directory_html(text: str, source: MasterfileSource) -> list[dict[str, str]]:
    return parse_bvb_instrument_directory_html(text, source, asset_type="ETF", category_filter="ETF")


def fetch_bvb_market_page_text(source_url: str, session: requests.Session) -> str:
    response = session.get(source_url, headers={"User-Agent": BROWSER_USER_AGENT}, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.text


def fetch_bvb_async_tab_text(
    source_url: str,
    page_text: str,
    tab_target: str,
    session: requests.Session,
) -> str:
    fields = extract_html_form_inputs(page_text, "aspnetForm")
    fields.update(
        {
            "ctl00$ctl00$MasterScriptManager": f"{tab_target}|{tab_target}",
            "__EVENTTARGET": tab_target,
            "__EVENTARGUMENT": "",
            "__ASYNCPOST": "true",
        }
    )
    response = session.post(
        source_url,
        data=fields,
        headers={
            "User-Agent": BROWSER_USER_AGENT,
            "Referer": source_url,
            "X-MicrosoftAjax": "Delta=true",
            "X-Requested-With": "XMLHttpRequest",
        },
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return response.text


def dedupe_bvb_reference_rows(rows: Iterable[dict[str, str]]) -> list[dict[str, str]]:
    deduped: list[dict[str, str]] = []
    seen: set[str] = set()
    for row in rows:
        ticker = row.get("ticker", "").strip().upper()
        if not ticker or ticker in seen:
            continue
        deduped.append(row)
        seen.add(ticker)
    return deduped


def fetch_bvb_shares_directory(
    source: MasterfileSource,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    session = session or requests.Session()
    text = fetch_bvb_market_page_text(source.source_url, session)
    rows = parse_bvb_shares_directory_html(text, source)
    for tab_target in BVB_SHARES_ASYNC_TABS:
        tab_text = fetch_bvb_async_tab_text(source.source_url, text, tab_target, session)
        rows.extend(parse_bvb_shares_directory_html(tab_text, source))
    return dedupe_bvb_reference_rows(rows)


def fetch_bvb_fund_units_directory(
    source: MasterfileSource,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    session = session or requests.Session()
    text = fetch_bvb_market_page_text(source.source_url, session)
    return parse_bvb_fund_units_directory_html(text, source)


def load_bvb_shares_directory_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    for path in (BVB_SHARES_DIRECTORY_CACHE, LEGACY_BVB_SHARES_DIRECTORY_CACHE):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"
    try:
        rows = fetch_bvb_shares_directory(source, session=session)
    except (requests.RequestException, ValueError):
        return None, "unavailable"

    ensure_output_dirs()
    BVB_SHARES_DIRECTORY_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def load_bvb_fund_units_directory_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    for path in (BVB_FUND_UNITS_DIRECTORY_CACHE, LEGACY_BVB_FUND_UNITS_DIRECTORY_CACHE):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"
    try:
        rows = fetch_bvb_fund_units_directory(source, session=session)
    except (requests.RequestException, ValueError):
        return None, "unavailable"

    ensure_output_dirs()
    BVB_FUND_UNITS_DIRECTORY_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def ngx_request_headers() -> dict[str, str]:
    return {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json;odata=verbose",
        "Referer": NGX_EQUITIES_PRICE_LIST_PAGE_URL,
        "Origin": "https://ngxgroup.com",
    }


NGX_COMPANY_PROFILE_LINK_RE = re.compile(
    r"company-profile/\?symbol=(?P<symbol>[^&\"']+)&(?:amp;)?directory=companydirectory",
    re.IGNORECASE,
)
NGX_COMPANY_PROFILE_FIELD_RE = re.compile(
    r'<strong\s+class="(?P<class>CompanyName|Symbol|Sector)">(?P<value>.*?)</strong>',
    re.IGNORECASE | re.DOTALL,
)


def ngx_company_profile_url(symbol: str) -> str:
    return f"{NGX_COMPANY_PROFILE_DIRECTORY_URL}?symbol={symbol}&directory=companydirectory"


def clean_ngx_profile_text(value: str) -> str:
    return re.sub(r"\s+", " ", unescape(re.sub(r"<[^>]+>", " ", value))).strip()


def normalize_ngx_company_name(value: str, ticker: str) -> str:
    cleaned = clean_ngx_profile_text(value)
    if ticker:
        cleaned = re.sub(rf"\s*\(\s*{re.escape(ticker)}\s*\)\s*$", "", cleaned, flags=re.IGNORECASE)
    return cleaned.strip()


def extract_ngx_company_profile_symbols(html: str) -> list[str]:
    seen: set[str] = set()
    symbols: list[str] = []
    for match in NGX_COMPANY_PROFILE_LINK_RE.finditer(html):
        symbol = clean_ngx_profile_text(match.group("symbol")).upper()
        if not symbol or symbol in seen:
            continue
        symbols.append(symbol)
        seen.add(symbol)
    return symbols


def parse_ngx_company_profile_detail_html(html: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for match in NGX_COMPANY_PROFILE_FIELD_RE.finditer(html):
        field = match.group("class").lower()
        values[field] = clean_ngx_profile_text(match.group("value"))

    ticker = values.get("symbol", "").strip().upper()
    name = normalize_ngx_company_name(values.get("companyname", ""), ticker)
    sector = values.get("sector", "").strip()
    if not ticker or not name:
        return {}
    return {"ticker": ticker, "name": name, "sector": sector}


def find_ngx_prefix_replaced_listing(
    official_ticker: str,
    profile_name: str,
    *,
    missing_current_by_ticker: dict[str, dict[str, str]],
) -> dict[str, str] | None:
    candidates = [
        row
        for ticker, row in missing_current_by_ticker.items()
        if official_ticker.startswith(ticker) and 0 < len(official_ticker) - len(ticker) <= 2
    ]
    matches = [
        row
        for row in candidates
        if alias_matches_company(profile_name, row.get("name", ""))
        or alias_matches_company(row.get("name", ""), profile_name)
    ]
    if len(matches) == 1:
        return matches[0]
    return None


def build_ngx_company_profile_directory_rows(
    symbols: Iterable[str],
    source: MasterfileSource,
    *,
    profile_details_by_symbol: dict[str, dict[str, str]],
    listings_path: Path = LISTINGS_CSV,
) -> list[dict[str, str]]:
    current_by_ticker = {
        row.get("ticker", "").strip().upper(): row
        for row in load_csv(listings_path)
        if row.get("exchange") == "NGX" and row.get("asset_type") == "Stock"
    }
    symbol_set = {symbol.strip().upper() for symbol in symbols if symbol.strip()}
    missing_current_by_ticker = {
        ticker: row for ticker, row in current_by_ticker.items() if ticker not in symbol_set
    }

    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for symbol in sorted(symbol_set):
        detail = profile_details_by_symbol.get(symbol, {})
        profile_ticker = detail.get("ticker", symbol).strip().upper()
        if profile_ticker != symbol:
            continue

        listing = current_by_ticker.get(symbol)
        if listing is None and detail.get("name"):
            listing = find_ngx_prefix_replaced_listing(
                symbol,
                detail["name"],
                missing_current_by_ticker=missing_current_by_ticker,
            )
        if listing is None or symbol in seen:
            continue

        name = detail.get("name") or listing.get("name", "") or symbol
        row = {
            "source_key": source.key,
            "provider": source.provider,
            "source_url": ngx_company_profile_url(symbol),
            "ticker": symbol,
            "name": name,
            "exchange": "NGX",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": source.reference_scope,
            "official": "true",
        }
        if listing.get("isin"):
            row["isin"] = listing["isin"]
        if detail.get("sector"):
            row["sector"] = detail["sector"]
        rows.append(row)
        seen.add(symbol)
    return rows


def parse_ngx_company_profile_directory_html(
    html: str,
    source: MasterfileSource,
    *,
    profile_details_by_symbol: dict[str, dict[str, str]],
    listings_path: Path = LISTINGS_CSV,
) -> list[dict[str, str]]:
    symbols = extract_ngx_company_profile_symbols(html)
    if not symbols:
        raise ValueError("Unexpected NGX company profile directory HTML")
    return build_ngx_company_profile_directory_rows(
        symbols,
        source,
        profile_details_by_symbol=profile_details_by_symbol,
        listings_path=listings_path,
    )


def fetch_ngx_company_profile_detail(
    symbol: str,
    *,
    session: requests.Session | None = None,
) -> dict[str, str]:
    response = (session or requests).get(ngx_company_profile_url(symbol), headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
    response.raise_for_status()
    return parse_ngx_company_profile_detail_html(response.text)


def ngx_company_profile_symbols_to_fetch(symbols: Iterable[str], *, listings_path: Path = LISTINGS_CSV) -> list[str]:
    symbol_set = {symbol.strip().upper() for symbol in symbols if symbol.strip()}
    current_symbols = {
        row.get("ticker", "").strip().upper()
        for row in load_csv(listings_path)
        if row.get("exchange") == "NGX" and row.get("asset_type") == "Stock"
    }
    return sorted(
        symbol
        for symbol in symbol_set - current_symbols
        for current in current_symbols - symbol_set
        if symbol.startswith(current) and 0 < len(symbol) - len(current) <= 2
    )


def fetch_ngx_company_profile_directory(
    source: MasterfileSource,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    response = (session or requests).get(source.source_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
    response.raise_for_status()
    symbols = extract_ngx_company_profile_symbols(response.text)
    if not symbols:
        raise ValueError("Unexpected NGX company profile directory HTML")

    details: dict[str, dict[str, str]] = {}
    target_symbols = ngx_company_profile_symbols_to_fetch(symbols)
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {
            executor.submit(fetch_ngx_company_profile_detail, symbol, session=session): symbol
            for symbol in target_symbols
        }
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                detail = future.result()
            except (requests.RequestException, ValueError):
                continue
            if detail:
                details[symbol] = detail

    return build_ngx_company_profile_directory_rows(
        symbols,
        source,
        profile_details_by_symbol=details,
    )


def load_ngx_company_profile_directory_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    for path in (NGX_COMPANY_PROFILE_DIRECTORY_CACHE, LEGACY_NGX_COMPANY_PROFILE_DIRECTORY_CACHE):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"
    try:
        rows = fetch_ngx_company_profile_directory(source, session=session)
    except (requests.RequestException, ValueError):
        return None, "unavailable"

    ensure_output_dirs()
    NGX_COMPANY_PROFILE_DIRECTORY_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def parse_ngx_equities_price_list_payload(
    payload: Any,
    source: MasterfileSource,
    *,
    listings_path: Path = LISTINGS_CSV,
) -> list[dict[str, str]]:
    if not isinstance(payload, list):
        raise ValueError("Unexpected NGX equities price list payload")
    current_by_ticker = {
        row.get("ticker", "").strip().upper(): row
        for row in load_csv(listings_path)
        if row.get("exchange") == "NGX"
    }
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for record in payload:
        if not isinstance(record, dict):
            continue
        ticker = str(record.get("Symbol", "")).strip().upper()
        listing = current_by_ticker.get(ticker)
        if not ticker or ticker in seen or listing is None:
            continue
        row = {
            "source_key": source.key,
            "provider": source.provider,
            "source_url": source.source_url,
            "ticker": ticker,
            "name": listing.get("name", "") or str(record.get("Company2", "")).strip() or ticker,
            "exchange": "NGX",
            "asset_type": listing.get("asset_type", "") or "Stock",
            "listing_status": "active",
            "reference_scope": source.reference_scope,
            "official": "true",
        }
        if listing.get("isin"):
            row["isin"] = listing["isin"]
        sector = str(record.get("Sector", "")).strip()
        if sector:
            row["sector"] = sector
        rows.append(row)
        seen.add(ticker)
    return rows


def fetch_ngx_equities_price_list(
    source: MasterfileSource,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    payload = fetch_json(source.source_url, session=session, headers=ngx_request_headers())
    return parse_ngx_equities_price_list_payload(payload, source)


def load_ngx_equities_price_list_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    for path in (NGX_EQUITIES_PRICE_LIST_CACHE, LEGACY_NGX_EQUITIES_PRICE_LIST_CACHE):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"
    try:
        rows = fetch_ngx_equities_price_list(source, session=session)
    except (requests.RequestException, ValueError):
        return None, "unavailable"

    ensure_output_dirs()
    NGX_EQUITIES_PRICE_LIST_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def parse_vienna_listed_companies_html(
    text: str,
    source: MasterfileSource,
    *,
    listings_path: Path = LISTINGS_CSV,
) -> list[dict[str, str]]:
    listings_by_isin = listing_rows_by_exchange_isin("VSE", listings_path=listings_path)
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for table in pd.read_html(io.StringIO(text)):
        columns = {str(column) for column in table.columns}
        if not {"ISIN", "Issuer", "Type of Security"} <= columns:
            continue
        for record in table.to_dict("records"):
            isin = str(record.get("ISIN", "")).strip().upper()
            issuer = str(record.get("Issuer", "")).strip()
            security_type = str(record.get("Type of Security", "")).strip().lower()
            listing = listings_by_isin.get(isin)
            if not listing or isin in seen or "equity" not in security_type:
                continue
            rows.append(
                {
                    "source_key": source.key,
                    "provider": source.provider,
                    "source_url": source.source_url,
                    "ticker": listing["ticker"],
                    "name": issuer or listing["name"],
                    "exchange": "VSE",
                    "asset_type": "Stock",
                    "listing_status": "active",
                    "reference_scope": source.reference_scope,
                    "official": "true",
                    "isin": isin,
                }
            )
            seen.add(isin)
    return rows


def fetch_vienna_listed_companies(
    source: MasterfileSource,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    text = fetch_text(source.source_url, session=session)
    return parse_vienna_listed_companies_html(text, source)


def load_vienna_listed_companies_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    for path in (VIENNA_LISTED_COMPANIES_CACHE, LEGACY_VIENNA_LISTED_COMPANIES_CACHE):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"
    try:
        rows = fetch_vienna_listed_companies(source, session=session)
    except (requests.RequestException, ValueError):
        return None, "unavailable"

    ensure_output_dirs()
    VIENNA_LISTED_COMPANIES_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def parse_zagreb_securities_html(text: str, source: MasterfileSource) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for match in ZAGREB_SECURITIES_ROW_RE.finditer(text):
        attrs = match.group("attrs")
        if "LISTED_SECURITIES" not in attrs or "EQTY" not in attrs.upper():
            continue
        cells = [clean_html_text(cell.group("value")) for cell in HTML_TD_RE.finditer(match.group("body"))]
        if len(cells) < 3:
            continue
        ticker, isin, name = cells[0].strip().upper(), cells[1].strip().upper(), cells[2].strip()
        sector = cells[3].strip() if len(cells) > 3 else ""
        delisting_date = cells[7].strip() if len(cells) > 7 else ""
        if not ticker or not name or not is_valid_isin(isin) or (delisting_date and delisting_date != "-"):
            continue
        if ticker in seen:
            continue
        rows.append(
            {
                "source_key": source.key,
                "provider": source.provider,
                "source_url": source.source_url,
                "ticker": ticker,
                "name": name,
                "exchange": "ZSE",
                "asset_type": "Stock",
                "listing_status": "active",
                "reference_scope": source.reference_scope,
                "official": "true",
                "isin": isin,
                "sector": sector,
            }
        )
        seen.add(ticker)
    return rows


def fetch_zagreb_securities(
    source: MasterfileSource,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    text = fetch_text(source.source_url, session=session)
    return parse_zagreb_securities_html(text, source)


def load_zagreb_securities_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    for path in (ZAGREB_SECURITIES_CACHE, LEGACY_ZAGREB_SECURITIES_CACHE):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"
    try:
        rows = fetch_zagreb_securities(source, session=session)
    except (requests.RequestException, ValueError):
        return None, "unavailable"

    ensure_output_dirs()
    ZAGREB_SECURITIES_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def load_idx_listed_companies_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    try:
        rows = fetch_idx_listed_companies(source, session=session)
    except (requests.RequestException, ValueError, json.JSONDecodeError):
        for path in (IDX_LISTED_COMPANIES_CACHE, LEGACY_IDX_LISTED_COMPANIES_CACHE):
            if path.exists():
                return json.loads(path.read_text(encoding="utf-8")), "cache"
        return None, "unavailable"

    ensure_output_dirs()
    IDX_LISTED_COMPANIES_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def load_idx_company_profiles_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    try:
        rows = fetch_idx_company_profiles(source, session=session)
    except (requests.RequestException, ValueError, json.JSONDecodeError):
        for path in (IDX_COMPANY_PROFILES_CACHE, LEGACY_IDX_COMPANY_PROFILES_CACHE):
            if path.exists():
                return json.loads(path.read_text(encoding="utf-8")), "cache"
        return None, "unavailable"

    ensure_output_dirs()
    IDX_COMPANY_PROFILES_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def wse_request_headers(referer: str) -> dict[str, str]:
    origin = ""
    parsed = requests.compat.urlparse(referer)
    if parsed.scheme and parsed.netloc:
        origin = f"{parsed.scheme}://{parsed.netloc}"
    return {
        "User-Agent": USER_AGENT,
        "Referer": referer,
        "Origin": origin,
        "X-Requested-With": "XMLHttpRequest",
    }


def extract_html_attr(tag: str, name: str) -> str:
    for match in WSE_ATTR_RE.finditer(tag):
        if match.group("name").lower() == name.lower():
            return unescape(match.group("value"))
    return ""


def extract_html_form_inputs(text: str, form_id: str) -> dict[str, str]:
    for match in WSE_FORM_RE.finditer(text):
        if match.group("form_id") != form_id:
            continue
        form_html = match.group(0)
        fields: dict[str, str] = {}
        for tag in WSE_INPUT_TAG_RE.findall(form_html):
            name = extract_html_attr(tag, "name").strip()
            if not name:
                continue
            input_type = extract_html_attr(tag, "type").strip().lower() or "text"
            if input_type == "checkbox" and not WSE_CHECKED_RE.search(tag):
                continue
            value = extract_html_attr(tag, "value")
            if input_type == "checkbox" and not value:
                value = "on"
            fields[name] = value
        return fields
    raise ValueError(f"Unable to locate HTML form {form_id}")


def parse_wse_company_label(label_html: str) -> tuple[str, str]:
    text = " ".join(unescape(strip_html_tags(label_html)).split())
    match = re.search(r"\(([^()]+)\)\s*$", text)
    if match is None:
        return text, ""
    return text[: match.start()].strip(), match.group(1).strip().upper()


WSE_STOCK_SECTOR_RULES: tuple[tuple[str, str], ...] = (
    ("bank", "Financials"),
    ("finans", "Financials"),
    ("inwestycy", "Financials"),
    ("zarzadzanie aktywami", "Financials"),
    ("aktywami", "Financials"),
    ("ubezpiec", "Financials"),
    ("nieruchomosci", "Real Estate"),
    ("energetyk", "Utilities"),
    ("energia", "Utilities"),
    ("paliw", "Energy"),
    ("ropa", "Energy"),
    ("gaz", "Energy"),
    ("gornictwo", "Materials"),
    ("biotechnologia", "Health Care"),
    ("medycz", "Health Care"),
    ("farmac", "Health Care"),
    ("gry", "Communication Services"),
    ("wydawnict", "Communication Services"),
    ("media", "Communication Services"),
    ("oprogramowanie", "Information Technology"),
    ("komputery", "Information Technology"),
    ("informaty", "Information Technology"),
    ("elektronika", "Information Technology"),
    ("internet", "Consumer Discretionary"),
    ("podrozy", "Consumer Discretionary"),
    ("sieci handlowe", "Consumer Discretionary"),
    ("sprzedaz", "Consumer Discretionary"),
    ("samochod", "Consumer Discretionary"),
    ("odziez", "Consumer Discretionary"),
    ("kosmetyki", "Consumer Staples"),
    ("chemia gospodarcza", "Consumer Staples"),
    ("napoje", "Consumer Staples"),
    ("spozyw", "Consumer Staples"),
    ("produkcja rolna", "Consumer Staples"),
    ("rybolow", "Consumer Staples"),
    ("budownictwo", "Industrials"),
    ("uslugi dla przedsiebiorstw", "Industrials"),
    ("transport", "Industrials"),
    ("drewno", "Materials"),
    ("papier", "Materials"),
    ("chemia", "Materials"),
)


def extract_wse_company_sector_label(row_html: str) -> str:
    match = WSE_SMALL_GREY_RE.search(row_html)
    if match is None:
        return ""
    text = " ".join(unescape(strip_html_tags(match.group("value"))).split())
    if "|" in text:
        return text.rsplit("|", 1)[-1].strip()
    return text.strip()


def normalize_wse_stock_sector(label: str) -> str:
    if not label:
        return ""
    folded = ascii_fold(label).lower()
    canonical = normalize_sector(label, "Stock")
    if canonical:
        return canonical
    for keyword, sector in WSE_STOCK_SECTOR_RULES:
        if keyword in folded:
            return sector
    return ""


def extract_wse_total_companies(text: str) -> int | None:
    match = WSE_COUNT_ALL_RE.search(text)
    if match is None:
        return None
    return int(match.group("count"))


def parse_wse_company_search_html(text: str, source: MasterfileSource) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for match in WSE_COMPANY_LINK_RE.finditer(text):
        href = unescape(match.group("href")).strip()
        isin_match = re.search(r"isin=([A-Z0-9]+)", href, re.I)
        name, ticker = parse_wse_company_label(match.group("label"))
        if not ticker or not name or ticker in seen:
            continue
        row = {
            "source_key": source.key,
            "provider": source.provider,
            "source_url": requests.compat.urljoin(source.source_url, href),
            "ticker": ticker,
            "name": name,
            "exchange": "WSE",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": source.reference_scope,
            "official": "true",
        }
        if isin_match:
            row["isin"] = isin_match.group(1).upper()
        row_end = text.find("</tr>", match.end())
        if row_end != -1:
            sector = normalize_wse_stock_sector(extract_wse_company_sector_label(text[match.end() : row_end]))
            if sector:
                row["sector"] = sector
        rows.append(row)
        seen.add(ticker)
    return rows


def wse_etf_listing_names(
    listings_path: Path = LISTINGS_CSV,
) -> dict[str, str]:
    names: dict[str, str] = {}
    for row in load_csv(listings_path):
        if row.get("exchange") != "WSE" or row.get("asset_type") != "ETF":
            continue
        ticker = row.get("ticker", "").strip().upper()
        name = row.get("name", "").strip()
        if ticker and name:
            names[ticker] = name
    return names


def parse_wse_etf_list_html(
    text: str,
    source: MasterfileSource,
    *,
    listing_name_by_ticker: dict[str, str] | None = None,
) -> list[dict[str, str]]:
    listing_name_by_ticker = listing_name_by_ticker or {}
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for match in WSE_ETF_LINK_RE.finditer(text):
        href = unescape(match.group("href")).strip()
        isin_match = re.search(r"isin=([A-Z0-9]+)", href, re.I)
        ticker = " ".join(unescape(strip_html_tags(match.group("ticker"))).split()).upper()
        if not ticker or ticker in seen:
            continue
        row = {
            "source_key": source.key,
            "provider": source.provider,
            "source_url": requests.compat.urljoin(source.source_url, href),
            "ticker": ticker,
            "name": listing_name_by_ticker.get(ticker, ticker),
            "exchange": "WSE",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": source.reference_scope,
            "official": "true",
        }
        if isin_match:
            row["isin"] = isin_match.group(1).upper()
        rows.append(row)
        seen.add(ticker)
    return rows


def fetch_wse_listed_companies(
    source: MasterfileSource,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    session = session or requests.Session()
    response = session.get(
        source.source_url,
        headers={"User-Agent": USER_AGENT},
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    rows = parse_wse_company_search_html(response.text, source)
    seen = {row["ticker"] for row in rows}
    form_data = extract_html_form_inputs(response.text, "search-form")
    total = extract_wse_total_companies(response.text)
    initial_page_size = max(len(rows), int(form_data.get("limit") or 10))
    limit = 500
    offset = int(form_data.get("offset") or 0) + initial_page_size

    while total is None or offset < total:
        payload = dict(form_data)
        payload["offset"] = str(offset)
        payload["limit"] = str(limit)
        page_response = session.post(
            WSE_AJAX_URL,
            data=payload,
            headers=wse_request_headers(source.source_url),
            timeout=REQUEST_TIMEOUT,
        )
        page_response.raise_for_status()
        page_rows = parse_wse_company_search_html(page_response.text, source)
        if not page_rows:
            break
        for row in page_rows:
            if row["ticker"] in seen:
                continue
            rows.append(row)
            seen.add(row["ticker"])
        if len(page_rows) < limit:
            break
        offset += limit
    return rows


def fetch_newconnect_listed_companies(
    source: MasterfileSource,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    session = session or requests.Session()
    response = session.get(
        source.source_url,
        headers={"User-Agent": USER_AGENT},
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    rows = parse_wse_company_search_html(response.text, source)
    seen = {row["ticker"] for row in rows}
    form_data = extract_html_form_inputs(response.text, "search-form")
    initial_page_size = max(len(rows), int(form_data.get("limit") or 10))
    limit = 500
    offset = int(form_data.get("offset") or 0) + initial_page_size

    while True:
        payload = dict(form_data)
        payload["offset"] = str(offset)
        payload["limit"] = str(limit)
        page_response = session.post(
            NEWCONNECT_AJAX_URL,
            data=payload,
            headers=wse_request_headers(source.source_url),
            timeout=REQUEST_TIMEOUT,
        )
        page_response.raise_for_status()
        page_rows = parse_wse_company_search_html(page_response.text, source)
        if not page_rows:
            break
        for row in page_rows:
            if row["ticker"] in seen:
                continue
            rows.append(row)
            seen.add(row["ticker"])
        if len(page_rows) < limit:
            break
        offset += limit
    return rows


def fetch_wse_etf_list(
    source: MasterfileSource,
    *,
    listings_path: Path = LISTINGS_CSV,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    session = session or requests.Session()
    response = session.post(
        WSE_AJAX_URL,
        data={"action": "GPWQuotationsETF", "start": "ajaxList", "page": "etfy"},
        headers=wse_request_headers(source.source_url),
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return parse_wse_etf_list_html(
        response.text,
        source,
        listing_name_by_ticker=wse_etf_listing_names(listings_path),
    )


def wse_cache_paths(source_key: str) -> tuple[Path, Path]:
    mapping = {
        "wse_listed_companies": (WSE_LISTED_COMPANIES_CACHE, LEGACY_WSE_LISTED_COMPANIES_CACHE),
        "newconnect_listed_companies": (
            NEWCONNECT_LISTED_COMPANIES_CACHE,
            LEGACY_NEWCONNECT_LISTED_COMPANIES_CACHE,
        ),
        "wse_etf_list": (WSE_ETF_LIST_CACHE, LEGACY_WSE_ETF_LIST_CACHE),
    }
    return mapping[source_key]


def load_wse_reference_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    for path in wse_cache_paths(source.key):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"

    try:
        if source.key == "wse_listed_companies":
            rows = fetch_wse_listed_companies(source, session=session)
        elif source.key == "newconnect_listed_companies":
            rows = fetch_newconnect_listed_companies(source, session=session)
        else:
            rows = fetch_wse_etf_list(source, session=session)
    except (requests.RequestException, ValueError, json.JSONDecodeError):
        return None, "unavailable"

    ensure_output_dirs()
    wse_cache_paths(source.key)[0].write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def bootstrap_tase_market_session(
    session: requests.Session | None = None,
) -> tuple[requests.Session, dict[str, str]]:
    try:
        from playwright.sync_api import sync_playwright
    except ModuleNotFoundError as exc:  # pragma: no cover - exercised in integration only
        raise requests.RequestException("Playwright is required for TASE bootstrap") from exc

    request_session = session or requests.Session()
    captured_headers: dict[str, str] = {}

    with sync_playwright() as playwright:  # pragma: no cover - exercised in integration only
        browser = playwright.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        def handle_response(response: Any) -> None:
            if (
                response.url.startswith(TASE_SECURITIES_MARKETDATA_URL)
                and response.request.method.upper() == "POST"
                and not captured_headers
            ):
                captured_headers.update(response.request.headers)

        page.on("response", handle_response)
        page.goto(TASE_MARKET_SECURITIES_PAGE_URL, wait_until="domcontentloaded", timeout=int(REQUEST_TIMEOUT * 1000))
        page.wait_for_timeout(5000)
        cookies = context.cookies()
        browser.close()

    if not captured_headers:
        raise requests.RequestException("TASE market bootstrap did not capture API headers")

    for cookie in cookies:
        request_session.cookies.set(
            cookie.get("name", ""),
            cookie.get("value", ""),
            domain=cookie.get("domain"),
            path=cookie.get("path"),
        )
    return request_session, tase_market_request_headers(captured_headers)


def parse_tase_securities_marketdata_payload(
    payload: dict[str, Any],
    source: MasterfileSource,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for record in payload.get("Items") or []:
        if str(record.get("Type") or "").strip().lower() != "shares":
            continue
        ticker = str(record.get("Symbol") or "").strip().upper()
        name = str(record.get("Name") or "").strip()
        if not ticker or not name or ticker in seen:
            continue
        seen.add(ticker)
        row = {
            "source_key": source.key,
            "provider": source.provider,
            "source_url": source.source_url,
            "ticker": ticker,
            "name": name,
            "exchange": "TASE",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": source.reference_scope,
            "official": "true",
        }
        isin = str(record.get("ISIN_ID") or "").strip().upper()
        if len(isin) == 12 and isin.isalnum():
            row["isin"] = isin
        rows.append(row)
    return rows


def normalize_tase_market_symbol(symbol: Any) -> str:
    return str(symbol or "").strip().upper().replace(".", "-")


def normalize_tase_search_symbol(symbol: Any) -> str:
    return "".join(ch for ch in str(symbol or "").upper() if ch.isalnum())


TASE_SEARCH_ENTITY_ETF_SUBTYPE_DESCS = {
    "ETF Fund - Equity",
    "ETF - Bonds",
    "Foreign ETF - Equity",
    "Foreign ETF - Bonds",
}
TASE_SEARCH_ENTITY_STOCK_SUBTYPE_DESCS = {
    "Participating unit",
}


def normalize_tase_etf_category(classification: Any) -> str:
    value = str(classification or "").strip().lower()
    if not value:
        return ""
    if any(marker in value for marker in ("leveraged", "short leveraged", "high-risk")):
        return "Leveraged/Inverse"
    if "bond" in value:
        return "Fixed Income"
    if "commodit" in value:
        return "Commodity"
    if any(marker in value for marker in ("share", "stock", "hitech", "hi-tech")):
        return "Equity"
    if "currency" in value or "fx" in value:
        return "Currency"
    return ""


def fetch_tase_securities_marketdata(
    source: MasterfileSource,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    session, headers = bootstrap_tase_market_session(session=session)

    def fetch_page(page_num: int, total_rec: int) -> dict[str, Any]:
        response = session.post(
            source.source_url,
            headers=headers,
            data=json.dumps(
                {
                    "dType": 1,
                    "TotalRec": max(total_rec, 1),
                    "pageNum": page_num,
                    "cl1": "0",
                    "lang": "1",
                }
            ),
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        return response.json()

    first_payload = fetch_page(1, 1)
    total_rec = int(first_payload.get("TotalRec") or 0)
    total_pages = max(1, (total_rec + 29) // 30) if total_rec else 1

    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for page_num in range(1, total_pages + 1):
        payload = first_payload if page_num == 1 else fetch_page(page_num, total_rec)
        page_rows = parse_tase_securities_marketdata_payload(payload, source)
        if not page_rows and page_num == 1:
            break
        for row in page_rows:
            ticker = row["ticker"]
            if ticker in seen:
                continue
            seen.add(ticker)
            rows.append(row)
    return rows


def load_tase_securities_marketdata_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    try:
        rows = fetch_tase_securities_marketdata(source, session=session)
    except (requests.RequestException, ValueError, json.JSONDecodeError):
        for path in (TASE_SECURITIES_MARKETDATA_CACHE, LEGACY_TASE_SECURITIES_MARKETDATA_CACHE):
            if path.exists():
                return json.loads(path.read_text(encoding="utf-8")), "cache"
        return None, "unavailable"

    ensure_output_dirs()
    TASE_SECURITIES_MARKETDATA_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def parse_tase_etf_marketdata_payload(
    payload: dict[str, Any],
    source: MasterfileSource,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for record in payload.get("Items") or []:
        ticker = normalize_tase_market_symbol(record.get("Symbol"))
        name = str(
            record.get("LongName")
            or record.get("SecurityLongName")
            or record.get("Name")
            or ""
        ).strip()
        if not ticker or not name or ticker in seen:
            continue
        seen.add(ticker)
        row = {
            "source_key": source.key,
            "provider": source.provider,
            "source_url": source.source_url,
            "ticker": ticker,
            "name": name,
            "exchange": "TASE",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": source.reference_scope,
            "official": "true",
        }
        isin = str(record.get("ISIN") or record.get("ISIN_ID") or "").strip().upper()
        if len(isin) == 12 and isin.isalnum():
            row["isin"] = isin
        sector = normalize_tase_etf_category(record.get("Classification"))
        if sector:
            row["sector"] = sector
        rows.append(row)
    return rows


def fetch_tase_etf_marketdata(
    source: MasterfileSource,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    session, headers = bootstrap_tase_market_session(session=session)
    request_headers = dict(headers)
    request_headers.setdefault("referer", TASE_MARKET_SECURITIES_PAGE_URL)

    def fetch_page(page_num: int) -> dict[str, Any]:
        response = session.post(
            source.source_url,
            headers=request_headers,
            data=json.dumps({"pageNum": page_num, "lang": "1"}),
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        return response.json()

    first_payload = fetch_page(1)
    total_rec = int(first_payload.get("TotalRec") or 0)
    total_pages = max(1, (total_rec + 29) // 30) if total_rec else 1

    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for page_num in range(1, total_pages + 1):
        payload = first_payload if page_num == 1 else fetch_page(page_num)
        page_rows = parse_tase_etf_marketdata_payload(payload, source)
        if not page_rows and page_num == 1:
            break
        for row in page_rows:
            ticker = row["ticker"]
            if ticker in seen:
                continue
            seen.add(ticker)
            rows.append(row)
    return rows


def load_tase_etf_marketdata_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    try:
        rows = fetch_tase_etf_marketdata(source, session=session)
    except (requests.RequestException, ValueError, json.JSONDecodeError):
        for path in (TASE_ETF_MARKETDATA_CACHE, LEGACY_TASE_ETF_MARKETDATA_CACHE):
            if path.exists():
                return json.loads(path.read_text(encoding="utf-8")), "cache"
        return None, "unavailable"

    ensure_output_dirs()
    TASE_ETF_MARKETDATA_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def tase_foreign_etf_search_target_rows(
    *,
    listings_path: Path = LISTINGS_CSV,
    verification_dir: Path = ETF_VERIFICATION_DIR,
) -> list[dict[str, str]]:
    target_tickers = latest_reference_gap_tickers(
        verification_dir,
        exchanges={"TASE"},
    )
    if not target_tickers:
        return []
    return [
        row
        for row in load_csv(listings_path)
        if row.get("exchange") == "TASE"
        and row.get("asset_type") == "ETF"
        and row.get("ticker", "").strip() in target_tickers
    ]


def fetch_tase_foreign_etf_search(
    source: MasterfileSource,
    *,
    listings_path: Path = LISTINGS_CSV,
    verification_dir: Path = ETF_VERIFICATION_DIR,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    target_rows = tase_foreign_etf_search_target_rows(
        listings_path=listings_path,
        verification_dir=verification_dir,
    )
    if not target_rows:
        return []

    session, headers = bootstrap_tase_market_session(session=session)
    response = session.get(
        source.source_url,
        headers=tase_market_request_headers(headers),
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list):
        raise ValueError("TASE search entities payload must be a list")

    entities_by_symbol: dict[str, list[dict[str, Any]]] = {}
    for record in payload:
        if not isinstance(record, dict):
            continue
        subtype_desc = str(record.get("SubTypeDesc") or "").strip()
        symbol = str(record.get("Smb") or "").strip()
        name = str(record.get("Name") or "").strip()
        if subtype_desc not in TASE_SEARCH_ENTITY_ETF_SUBTYPE_DESCS or not symbol or not name:
            continue
        entities_by_symbol.setdefault(normalize_tase_search_symbol(symbol), []).append(record)

    rows: list[dict[str, str]] = []
    for listing_row in target_rows:
        ticker = listing_row.get("ticker", "").strip()
        if not ticker:
            continue
        listing_isin = listing_row.get("isin", "").strip().upper()
        candidates = entities_by_symbol.get(normalize_tase_search_symbol(ticker), [])
        if not candidates:
            continue
        filtered_candidates = []
        for candidate in candidates:
            candidate_isin = str(candidate.get("ISIN") or "").strip().upper()
            if listing_isin and candidate_isin and candidate_isin != listing_isin:
                continue
            filtered_candidates.append(candidate)
        if len(filtered_candidates) != 1:
            continue
        candidate = filtered_candidates[0]
        row = {
            "source_key": source.key,
            "provider": source.provider,
            "source_url": source.source_url,
            "ticker": ticker,
            "name": str(candidate.get("Name") or "").strip(),
            "exchange": "TASE",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": source.reference_scope,
            "official": "true",
        }
        candidate_isin = str(candidate.get("ISIN") or "").strip().upper()
        if len(candidate_isin) == 12 and candidate_isin.isalnum():
            row["isin"] = candidate_isin
        rows.append(row)
    return rows


def load_tase_foreign_etf_search_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    try:
        rows = fetch_tase_foreign_etf_search(source, session=session)
    except (requests.RequestException, ValueError, json.JSONDecodeError):
        for path in (TASE_FOREIGN_ETF_SEARCH_CACHE, LEGACY_TASE_FOREIGN_ETF_SEARCH_CACHE):
            if path.exists():
                return json.loads(path.read_text(encoding="utf-8")), "cache"
        return None, "unavailable"

    ensure_output_dirs()
    TASE_FOREIGN_ETF_SEARCH_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def tase_participating_unit_search_target_rows(
    *,
    listings_path: Path = LISTINGS_CSV,
    verification_dir: Path = STOCK_VERIFICATION_DIR,
) -> list[dict[str, str]]:
    target_tickers = latest_reference_gap_tickers(
        verification_dir,
        exchanges={"TASE"},
    )
    if not target_tickers:
        return []
    return [
        row
        for row in load_csv(listings_path)
        if row.get("exchange") == "TASE"
        and row.get("asset_type") == "Stock"
        and row.get("ticker", "").strip() in target_tickers
    ]


def fetch_tase_participating_unit_search(
    source: MasterfileSource,
    *,
    listings_path: Path = LISTINGS_CSV,
    verification_dir: Path = STOCK_VERIFICATION_DIR,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    target_rows = tase_participating_unit_search_target_rows(
        listings_path=listings_path,
        verification_dir=verification_dir,
    )
    if not target_rows:
        return []

    session, headers = bootstrap_tase_market_session(session=session)
    response = session.get(
        source.source_url,
        headers=tase_market_request_headers(headers),
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list):
        raise ValueError("TASE search entities payload must be a list")

    entities_by_symbol: dict[str, list[dict[str, Any]]] = {}
    for record in payload:
        if not isinstance(record, dict):
            continue
        subtype_desc = str(record.get("SubTypeDesc") or "").strip()
        symbol = str(record.get("Smb") or "").strip()
        name = str(record.get("Name") or "").strip()
        if subtype_desc not in TASE_SEARCH_ENTITY_STOCK_SUBTYPE_DESCS or not symbol or not name:
            continue
        entities_by_symbol.setdefault(normalize_tase_search_symbol(symbol), []).append(record)

    rows: list[dict[str, str]] = []
    for listing_row in target_rows:
        ticker = listing_row.get("ticker", "").strip()
        if not ticker:
            continue
        listing_isin = listing_row.get("isin", "").strip().upper()
        candidates = entities_by_symbol.get(normalize_tase_search_symbol(ticker), [])
        if not candidates:
            continue
        filtered_candidates = []
        for candidate in candidates:
            candidate_isin = str(candidate.get("ISIN") or "").strip().upper()
            if listing_isin and candidate_isin and candidate_isin != listing_isin:
                continue
            filtered_candidates.append(candidate)
        if len(filtered_candidates) != 1:
            continue
        candidate = filtered_candidates[0]
        row = {
            "source_key": source.key,
            "provider": source.provider,
            "source_url": source.source_url,
            "ticker": ticker,
            "name": str(candidate.get("Name") or "").strip(),
            "exchange": "TASE",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": source.reference_scope,
            "official": "true",
        }
        candidate_isin = str(candidate.get("ISIN") or "").strip().upper()
        if len(candidate_isin) == 12 and candidate_isin.isalnum():
            row["isin"] = candidate_isin
        rows.append(row)
    return rows


def load_tase_participating_unit_search_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    try:
        rows = fetch_tase_participating_unit_search(source, session=session)
    except (requests.RequestException, ValueError, json.JSONDecodeError):
        for path in (TASE_PARTICIPATING_UNIT_SEARCH_CACHE, LEGACY_TASE_PARTICIPATING_UNIT_SEARCH_CACHE):
            if path.exists():
                return json.loads(path.read_text(encoding="utf-8")), "cache"
        return None, "unavailable"

    ensure_output_dirs()
    TASE_PARTICIPATING_UNIT_SEARCH_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def hose_securities_cache_paths(source_key: str) -> tuple[Path, Path]:
    mapping = {
        "hose_listed_stocks": (HOSE_LISTED_STOCKS_CACHE, LEGACY_HOSE_LISTED_STOCKS_CACHE),
        "hose_etf_list": (HOSE_ETF_LIST_CACHE, LEGACY_HOSE_ETF_LIST_CACHE),
        "hose_fund_certificate_list": (
            HOSE_FUND_CERTIFICATE_LIST_CACHE,
            LEGACY_HOSE_FUND_CERTIFICATE_LIST_CACHE,
        ),
    }
    return mapping[source_key]


def normalize_hose_security_sector(record: dict[str, Any], asset_type: str) -> str:
    haystack = " ".join(
        str(record.get(key) or "")
        for key in ("name", "brief", "displayText", "refIndex")
    ).lower()
    if asset_type == "ETF":
        if "etf" in haystack or any(marker in haystack for marker in ("vn30", "vn100", "vnx", "vnfin", "diamond")):
            return "Equity"
    if "reit" in haystack or "bất động sản" in haystack:
        return "Real Estate" if asset_type == "Stock" else "Real Estate"
    return ""


def parse_hose_securities_payload(
    payload: dict[str, Any],
    source: MasterfileSource,
    *,
    asset_type: str,
) -> list[dict[str, str]]:
    data = payload.get("data") or {}
    records = data.get("list") if isinstance(data, dict) else []
    if not isinstance(records, list):
        return []

    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for record in records:
        if not isinstance(record, dict):
            continue
        ticker = str(record.get("code") or "").strip().upper()
        name = str(record.get("name") or record.get("brief") or "").strip()
        isin = str(record.get("isin") or "").strip().upper()
        if not ticker or not name or not isin or ticker in seen:
            continue
        seen.add(ticker)
        row = {
            "source_key": source.key,
            "provider": source.provider,
            "source_url": source.source_url,
            "ticker": ticker,
            "name": name,
            "exchange": "HOSE",
            "asset_type": asset_type,
            "listing_status": "active",
            "reference_scope": source.reference_scope,
            "official": "true",
            "isin": isin,
        }
        sector = normalize_hose_security_sector(record, asset_type)
        if sector:
            row["sector"] = sector
        rows.append(row)
    return rows


def fetch_hose_securities_rows(
    source: MasterfileSource,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    config = HOSE_SECURITIES_SOURCE_CONFIG[source.key]
    session = session or requests.Session()
    page_index = 1
    page_size = 1000
    rows: list[dict[str, str]] = []
    seen: set[str] = set()

    while True:
        response = session.get(
            source.source_url,
            params={
                "pageIndex": page_index,
                "pageSize": page_size,
                "code": "",
                "alphabet": "",
                "sectorId": "",
            },
            headers=hose_request_headers(),
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        payload = response.json()
        page_rows = parse_hose_securities_payload(
            payload,
            source,
            asset_type=config["asset_type"],
        )
        for row in page_rows:
            ticker = row["ticker"]
            if ticker in seen:
                continue
            seen.add(ticker)
            rows.append(row)

        data = payload.get("data") or {}
        paging = data.get("paging") if isinstance(data, dict) else {}
        total_pages = int(paging.get("totalPages") or page_index) if isinstance(paging, dict) else page_index
        if page_index >= total_pages or not page_rows:
            break
        page_index += 1

    return rows


def load_hose_securities_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    for path in hose_securities_cache_paths(source.key):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"

    try:
        rows = fetch_hose_securities_rows(source, session=session)
    except (requests.RequestException, ValueError, json.JSONDecodeError):
        return None, "unavailable"

    ensure_output_dirs()
    hose_securities_cache_paths(source.key)[0].write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def hnx_securities_cache_paths(source_key: str) -> tuple[Path, Path]:
    mapping = {
        "hnx_listed_securities": (HNX_LISTED_SECURITIES_CACHE, LEGACY_HNX_LISTED_SECURITIES_CACHE),
        "upcom_registered_securities": (
            UPCOM_REGISTERED_SECURITIES_CACHE,
            LEGACY_UPCOM_REGISTERED_SECURITIES_CACHE,
        ),
    }
    return mapping[source_key]


VSDC_ISIN_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{10}$")
VSDC_META_TAG_RE = re.compile(r"<meta\s+[^>]*>", re.IGNORECASE)
VSDC_META_CONTENT_RE = re.compile(r"""content=["']([^"']+)["']""", re.IGNORECASE)
HNX_SECURITY_TICKER_RE = re.compile(r"^[A-Z0-9]{1,10}$")


def extract_vsdc_token(html: str) -> str:
    for match in VSDC_META_TAG_RE.finditer(html):
        tag = match.group(0)
        if "__VPToken" not in tag:
            continue
        content_match = VSDC_META_CONTENT_RE.search(tag)
        if content_match:
            return content_match.group(1)
    return ""


def extract_vsdc_label_value(html: str, label: str) -> str:
    pattern = re.compile(
        rf"{re.escape(label)}\s*</[^>]+>\s*<[^>]+>\s*([^<]+)",
        re.IGNORECASE,
    )
    match = pattern.search(html)
    if not match:
        return ""
    return " ".join(unescape(match.group(1)).split()).strip()


def choose_vsdc_security_detail_href(payload: dict[str, Any], ticker: str) -> str:
    records = payload.get("data")
    if not isinstance(records, list):
        return ""
    ticker_prefix = f"{ticker.upper()}:"
    for record in records:
        if not isinstance(record, dict):
            continue
        content = re.sub(r"<[^>]+>", "", str(record.get("content") or ""))
        content = " ".join(unescape(content).split()).upper()
        href = str(record.get("href") or "").strip()
        if content.startswith(ticker_prefix) and href:
            return href
    return ""


def fetch_vsdc_isin_for_ticker(
    ticker: str,
    *,
    token: str,
    cookies: dict[str, str] | None = None,
) -> str:
    ticker = ticker.strip().upper()
    if not ticker:
        return ""
    session = requests.Session()
    if cookies:
        session.cookies.update(cookies)
    search_response = session.post(
        VSDC_SEARCH_SUGGEST_URL,
        json={"text": ticker, "type": 5},
        headers=vsdc_request_headers(token=token),
        timeout=REQUEST_TIMEOUT,
    )
    search_response.raise_for_status()
    href = choose_vsdc_security_detail_href(search_response.json(), ticker)
    if not href:
        return ""

    detail_url = urljoin(VSDC_BASE_URL, href)
    detail_response = session.get(
        detail_url,
        headers=vsdc_request_headers(token=token, referer=VSDC_TOKEN_SEED_URL),
        timeout=REQUEST_TIMEOUT,
    )
    detail_response.raise_for_status()
    code = extract_vsdc_label_value(detail_response.text, "Securities code:").upper()
    isin = re.sub(r"\s+", "", extract_vsdc_label_value(detail_response.text, "ISIN:").upper())
    if code != ticker or not VSDC_ISIN_RE.fullmatch(isin):
        return ""
    return isin


def fetch_vsdc_isin_lookup(
    tickers: Iterable[str],
    *,
    session: requests.Session | None = None,
    max_workers: int = 8,
) -> dict[str, str]:
    unique_tickers = sorted({ticker.strip().upper() for ticker in tickers if ticker.strip()})
    if not unique_tickers:
        return {}

    session = session or requests.Session()
    token_response = session.get(
        VSDC_TOKEN_SEED_URL,
        headers={"User-Agent": "Mozilla/5.0", "Referer": VSDC_BASE_URL},
        timeout=REQUEST_TIMEOUT,
    )
    token_response.raise_for_status()
    token = extract_vsdc_token(token_response.text)
    if not token:
        return {}
    cookies = session.cookies.get_dict()

    lookup: dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(fetch_vsdc_isin_for_ticker, ticker, token=token, cookies=cookies): ticker
            for ticker in unique_tickers
        }
        for future in as_completed(futures):
            ticker = futures[future]
            try:
                isin = future.result()
            except (requests.RequestException, ValueError, json.JSONDecodeError):
                continue
            if isin:
                lookup[ticker] = isin
    return lookup


def parse_hnx_issuer_table_html(
    content: str,
    source: MasterfileSource,
    *,
    exchange: str,
    isin_lookup: dict[str, str] | None = None,
) -> list[dict[str, str]]:
    parser = _TableParser()
    parser.feed(content)
    tables = [table for table in parser.tables if table]
    if not tables:
        return []

    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    isin_lookup = isin_lookup or {}
    for table_row in max(tables, key=len):
        if len(table_row) < 3:
            continue
        ticker = table_row[1].strip().upper()
        name = table_row[2].strip()
        if ticker == "MÃ CK" or not ticker or not name or ticker in seen:
            continue
        if not HNX_SECURITY_TICKER_RE.fullmatch(ticker):
            continue
        seen.add(ticker)
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
                "isin": isin_lookup.get(ticker, ""),
            }
        )
    return rows


def fetch_hnx_issuer_rows(
    source: MasterfileSource,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    config = HNX_SECURITIES_SOURCE_CONFIG[source.key]
    session = session or requests.Session()
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    page_response = session.get(
        config["page_url"],
        headers=hnx_request_headers(),
        timeout=REQUEST_TIMEOUT,
        verify=False,
    )
    page_response.raise_for_status()
    referer = getattr(page_response, "url", config["page_url"]) or config["page_url"]
    response = session.post(
        config["endpoint_url"],
        data={
            "p_issearch": 0,
            "p_keysearch": "",
            "p_market_code": config["market_code"],
            "p_orderby": "STOCK_CODE",
            "p_ordertype": "ASC",
            "p_currentpage": 1,
            "p_record_on_page": 1000,
        },
        headers=hnx_request_headers(referer),
        timeout=REQUEST_TIMEOUT,
        verify=False,
    )
    response.raise_for_status()
    payload = response.json()
    content = str(payload.get("Content") or "")
    rows = parse_hnx_issuer_table_html(content, source, exchange=config["exchange"])
    isin_lookup = fetch_vsdc_isin_lookup((row["ticker"] for row in rows), session=session)
    return parse_hnx_issuer_table_html(
        content,
        source,
        exchange=config["exchange"],
        isin_lookup=isin_lookup,
    )


def load_hnx_issuer_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    for path in hnx_securities_cache_paths(source.key):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"

    try:
        rows = fetch_hnx_issuer_rows(source, session=session)
    except (requests.RequestException, ValueError, json.JSONDecodeError):
        return None, "unavailable"

    ensure_output_dirs()
    hnx_securities_cache_paths(source.key)[0].write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def load_szse_etf_list_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    try:
        rows = fetch_szse_etf_list(source, session=session)
    except (requests.RequestException, ValueError):
        for path in (SZSE_ETF_LIST_CACHE, LEGACY_SZSE_ETF_LIST_CACHE):
            if path.exists():
                return json.loads(path.read_text(encoding="utf-8")), "cache"
        return None, "unavailable"

    ensure_output_dirs()
    SZSE_ETF_LIST_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def load_szse_b_share_list_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    try:
        rows = fetch_szse_b_share_list(source, session=session)
    except (requests.RequestException, ValueError):
        for path in (SZSE_B_SHARE_LIST_CACHE, LEGACY_SZSE_B_SHARE_LIST_CACHE):
            if path.exists():
                return json.loads(path.read_text(encoding="utf-8")), "cache"
        return None, "unavailable"

    ensure_output_dirs()
    SZSE_B_SHARE_LIST_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


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
    graphql_payload: list[dict[str, Any]] | None = None
    if payload is None:
        try:
            payload = fetch_json(TMX_ETF_SCREENER_URL, session=session)
        except requests.RequestException:
            try:
                graphql_payload = fetch_tmx_money_etfs(session=session)
            except (requests.RequestException, ValueError, json.JSONDecodeError):
                return None, "unavailable"
            payload = graphql_payload
        mode = "network"

    payload = list(payload)
    covered_tickers = {str(record.get("symbol", "")).strip().upper() for record in payload}
    if graphql_payload is None:
        try:
            graphql_payload = fetch_tmx_money_etfs(session=session)
        except (requests.RequestException, ValueError, json.JSONDecodeError):
            graphql_payload = []
    if graphql_payload:
        for row in graphql_payload:
            symbol = str(row.get("symbol", "")).strip().upper()
            if not symbol or symbol in covered_tickers:
                continue
            payload.append(row)
            covered_tickers.add(symbol)
            mode = "network"
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


def fetch_tmx_money_etfs(session: requests.Session | None = None) -> list[dict[str, Any]]:
    session = session or requests.Session()
    response = session.post(
        TMX_MONEY_GRAPHQL_URL,
        headers={"User-Agent": USER_AGENT, "Content-Type": "application/json"},
        json={"query": TMX_MONEY_GET_ETFS_QUERY},
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    payload = response.json()
    records = payload.get("data", {}).get("getETFs")
    if not isinstance(records, list):
        raise ValueError("TMX getETFs payload missing data list")
    rows: list[dict[str, Any]] = []
    for record in records:
        if not isinstance(record, dict):
            continue
        row = dict(record)
        row["source_url"] = TMX_MONEY_GRAPHQL_URL
        rows.append(row)
    return rows


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
    target_tickers = latest_verification_tickers(
        verification_dir,
        exchanges={"TSX", "TSXV"},
        statuses={"reference_gap", "missing_from_official"},
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


def tmx_stock_quote_lookup_target_rows(
    *,
    listings_path: Path = LISTINGS_CSV,
    verification_dir: Path = STOCK_VERIFICATION_DIR,
) -> list[dict[str, str]]:
    target_tickers = latest_verification_tickers(
        verification_dir,
        exchanges={"TSX", "TSXV"},
        statuses={"reference_gap", "missing_from_official"},
    )
    if not target_tickers:
        return []
    return [
        row
        for row in load_csv(listings_path)
        if row.get("exchange") in {"TSX", "TSXV"}
        and row.get("asset_type") == "Stock"
        and row.get("ticker", "").strip() in target_tickers
    ]


def nasdaq_nordic_share_search_target_rows(
    source: MasterfileSource,
    *,
    listings_path: Path = LISTINGS_CSV,
    verification_dir: Path = STOCK_VERIFICATION_DIR,
) -> list[dict[str, str]]:
    config = NASDAQ_NORDIC_SHARE_SEARCH_SOURCE_CONFIG[source.key]
    target_tickers = latest_reference_gap_tickers(
        verification_dir,
        exchanges={config["exchange"]},
    )
    if not target_tickers:
        return []
    return [
        row
        for row in load_csv(listings_path)
        if row.get("exchange") == config["exchange"]
        and row.get("asset_type") == "Stock"
        and row.get("ticker", "").strip() in target_tickers
    ]


def spotlight_search_target_rows(
    source: MasterfileSource,
    *,
    listings_path: Path = LISTINGS_CSV,
    verification_dir: Path = STOCK_VERIFICATION_DIR,
) -> list[dict[str, str]]:
    config = SPOTLIGHT_SEARCH_SOURCE_CONFIG[source.key]
    target_tickers = latest_reference_gap_tickers(
        verification_dir,
        exchanges={config["exchange"]},
    )
    if not target_tickers:
        return []
    return [
        row
        for row in load_csv(listings_path)
        if row.get("exchange") == config["exchange"]
        and row.get("asset_type") == "Stock"
        and row.get("ticker", "").strip() in target_tickers
    ]


def nasdaq_nordic_etf_search_target_rows(
    source: MasterfileSource,
    *,
    listings_path: Path = LISTINGS_CSV,
    verification_dir: Path = ETF_VERIFICATION_DIR,
) -> list[dict[str, str]]:
    config = NASDAQ_NORDIC_ETF_SEARCH_SOURCE_CONFIG[source.key]
    target_tickers = latest_reference_gap_tickers(
        verification_dir,
        exchanges={config["exchange"]},
    )
    if not target_tickers:
        return []
    return [
        row
        for row in load_csv(listings_path)
        if row.get("exchange") == config["exchange"]
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
            accepted_override_names = TMX_EXACT_QUOTE_ACCEPTED_NAMES.get((exchange, listing_ticker), set())
            if not candidate_name or (
                candidate_name not in accepted_override_names
                and not tmx_lookup_name_matches(listing_name, candidate_name)
            ):
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


def fetch_tmx_stock_quote_rows(
    existing_rows: list[dict[str, str]],
    source: MasterfileSource,
    *,
    listings_path: Path = LISTINGS_CSV,
    verification_dir: Path = STOCK_VERIFICATION_DIR,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    covered_tickers = {row.get("ticker", "").strip().upper() for row in existing_rows}
    target_rows = [
        row
        for row in tmx_stock_quote_lookup_target_rows(
            listings_path=listings_path,
            verification_dir=verification_dir,
        )
        if row.get("ticker", "").strip().upper() not in covered_tickers
    ]
    session = session or requests.Session()
    rows: list[dict[str, str]] = []
    for listing_row in target_rows:
        listing_ticker = listing_row.get("ticker", "").strip().upper()
        listing_name = listing_row.get("name", "").strip()
        exchange = listing_row.get("exchange", "").strip()
        if not listing_ticker or not listing_name or not exchange:
            continue
        for symbol in tmx_stock_quote_symbol_variants(listing_ticker):
            try:
                candidate = fetch_tmx_money_quote(symbol, session=session)
            except (requests.RequestException, ValueError, json.JSONDecodeError):
                candidate = None
            if candidate is None:
                continue
            candidate_exchange = TMX_QUOTE_EXCHANGE_CODE_TO_EXCHANGE.get(str(candidate.get("exchangeCode", "")).strip().upper(), "")
            if candidate_exchange != exchange:
                continue
            candidate_name = str(candidate.get("name", "")).strip()
            normalized_candidate_symbol = str(candidate.get("symbol", symbol)).strip().upper().replace(".", "-")
            allow_code_like_refresh = (
                compact_company_name(listing_name) == compact_company_name(listing_ticker)
                and normalized_candidate_symbol == listing_ticker
            )
            allow_variant_symbol_refresh = normalized_candidate_symbol == listing_ticker
            if not candidate_name or not (
                tmx_lookup_name_matches(listing_name, candidate_name)
                or allow_code_like_refresh
                or allow_variant_symbol_refresh
            ):
                continue
            rows.append(
                {
                    "source_key": source.key,
                    "provider": source.provider,
                    "source_url": TMX_MONEY_GRAPHQL_URL,
                    "ticker": listing_ticker,
                    "name": candidate_name,
                    "exchange": exchange,
                    "asset_type": "Stock",
                    "listing_status": "active",
                    "reference_scope": "exchange_directory",
                    "official": "true",
                    "isin": listing_row.get("isin", "").strip(),
                }
            )
            covered_tickers.add(listing_ticker)
            break
    return rows


def fetch_nasdaq_nordic_share_search(
    source: MasterfileSource,
    *,
    listings_path: Path = LISTINGS_CSV,
    verification_dir: Path = STOCK_VERIFICATION_DIR,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    config = NASDAQ_NORDIC_SHARE_SEARCH_SOURCE_CONFIG[source.key]
    session = session or requests.Session()
    headers = nasdaq_nordic_request_headers(NASDAQ_NORDIC_STOCK_PAGE_URL)
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for listing_row in nasdaq_nordic_share_search_target_rows(
        source,
        listings_path=listings_path,
        verification_dir=verification_dir,
    ):
        listing_ticker = listing_row.get("ticker", "").strip().upper()
        if not listing_ticker or listing_ticker in seen:
            continue
        search_terms = [listing_ticker]
        normalized_term = listing_ticker.replace("-", " ").strip()
        if normalized_term and normalized_term not in search_terms:
            search_terms.append(normalized_term)
        listing_name = str(listing_row.get("name", "")).strip()
        if len(listing_ticker) <= 3 and listing_name and listing_name not in search_terms:
            search_terms.append(listing_name)
        for search_term in search_terms:
            response = session.get(
                source.source_url,
                params={"searchText": search_term},
                headers=headers,
                timeout=REQUEST_TIMEOUT,
            )
            if getattr(response, "status_code", 200) == 400:
                continue
            response.raise_for_status()
            payload = response.json().get("data") or []
            for group in payload:
                for instrument in group.get("instruments") or []:
                    if instrument.get("assetClass") != "SHARES":
                        continue
                    if str(instrument.get("currency", "")).strip().upper() != config["currency"]:
                        continue
                    symbol = str(instrument.get("symbol", "")).strip().upper()
                    name = str(instrument.get("fullName", "")).strip()
                    isin = str(instrument.get("isin", "")).strip().upper()
                    if not name:
                        continue
                    normalized_match = (
                        normalize_nasdaq_nordic_search_symbol(symbol)
                        == normalize_nasdaq_nordic_search_symbol(listing_ticker)
                    )
                    if not normalized_match:
                        continue
                    exact_symbol_match = symbol == listing_ticker
                    if config["exchange"] in {"STO", "CPH"}:
                        listing_isin = str(listing_row.get("isin", "")).strip().upper()
                        if not has_strong_company_name_match(
                            listing_row.get("name", ""),
                            name,
                        ) and not (listing_isin and isin and listing_isin == isin):
                            continue
                    elif not exact_symbol_match and not has_strong_company_name_match(
                        listing_row.get("name", ""),
                        name,
                    ):
                        continue
                    row = {
                        "source_key": source.key,
                        "provider": source.provider,
                        "source_url": source.source_url,
                        "ticker": listing_ticker,
                        "name": name,
                        "exchange": config["exchange"],
                        "asset_type": "Stock",
                        "listing_status": "active",
                        "reference_scope": source.reference_scope,
                        "official": "true",
                    }
                    if isin:
                        row["isin"] = isin
                    rows.append(row)
                    seen.add(listing_ticker)
                    break
                if listing_ticker in seen:
                    break
            if listing_ticker in seen:
                break
    return rows


def fetch_nasdaq_nordic_etf_search(
    source: MasterfileSource,
    *,
    listings_path: Path = LISTINGS_CSV,
    verification_dir: Path = ETF_VERIFICATION_DIR,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    config = NASDAQ_NORDIC_ETF_SEARCH_SOURCE_CONFIG[source.key]
    session = session or requests.Session()
    headers = nasdaq_nordic_request_headers(NASDAQ_NORDIC_ETF_PAGE_URL)
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for listing_row in nasdaq_nordic_etf_search_target_rows(
        source,
        listings_path=listings_path,
        verification_dir=verification_dir,
    ):
        listing_ticker = listing_row.get("ticker", "").strip().upper()
        listing_name = str(listing_row.get("name", "")).strip()
        listing_isin = str(listing_row.get("isin", "")).strip().upper()
        if not listing_ticker or listing_ticker in seen:
            continue
        search_terms = [listing_ticker]
        normalized_term = listing_ticker.replace("-", " ").strip()
        if normalized_term and normalized_term not in search_terms:
            search_terms.append(normalized_term)
        if listing_name and listing_name not in search_terms:
            search_terms.append(listing_name)
        for search_term in search_terms:
            response = session.get(
                source.source_url,
                params={"searchText": search_term},
                headers=headers,
                timeout=REQUEST_TIMEOUT,
            )
            if getattr(response, "status_code", 200) == 400:
                continue
            response.raise_for_status()
            payload = response.json().get("data") or []
            for group in payload:
                for instrument in group.get("instruments") or []:
                    if str(instrument.get("currency", "")).strip().upper() != config["currency"]:
                        continue
                    if str(instrument.get("assetClass", "")).strip().upper() not in {"FUNDS", "ETF"}:
                        continue
                    symbol = str(instrument.get("symbol", "")).strip().upper()
                    name = str(instrument.get("fullName", "")).strip()
                    isin = str(instrument.get("isin", "")).strip().upper()
                    if not name:
                        continue
                    normalized_match = (
                        normalize_nasdaq_nordic_search_symbol(symbol)
                        == normalize_nasdaq_nordic_search_symbol(listing_ticker)
                    )
                    if not normalized_match:
                        continue
                    if not has_strong_company_name_match(listing_name, name) and not (
                        listing_isin and isin and listing_isin == isin
                    ):
                        continue
                    rows.append(
                        {
                            "source_key": source.key,
                            "provider": source.provider,
                            "source_url": source.source_url,
                            "ticker": listing_ticker,
                            "name": name,
                            "exchange": config["exchange"],
                            "asset_type": "ETF",
                            "listing_status": "active",
                            "reference_scope": source.reference_scope,
                            "official": "true",
                            "isin": isin,
                        }
                    )
                    seen.add(listing_ticker)
                    break
                if listing_ticker in seen:
                    break
            if listing_ticker in seen:
                break
    return rows


def strip_html_tags(value: str) -> str:
    return re.sub(r"<[^>]+>", "", value or "")


def parse_spotlight_search_heading(heading: str) -> tuple[str, str]:
    text = " ".join(unescape(strip_html_tags(heading)).split())
    match = re.search(r"\(([^()]+)\)\s*$", text)
    if match is None:
        return text, ""
    return text[: match.start()].strip(), match.group(1).strip().upper()


def parse_spotlight_company_title(title: str) -> tuple[str, str]:
    text = title.split("|", 1)[0].strip()
    return parse_spotlight_search_heading(text)


def spotlight_trade_url_from_detail_url(detail_url: str) -> str:
    return detail_url.replace("irabout", "irtrade", 1)


def extract_spotlight_detail_isin(text: str) -> str:
    for candidate in ISIN_TOKEN_RE.findall(text):
        if is_valid_isin(candidate):
            return candidate
    return ""


def parse_ngm_companies_page_items(text: str) -> list[dict[str, Any]]:
    match = re.search(r"<company-list\s+.*?:items=\"(\[.*?\])\".*?/>", text, re.S)
    if match is None:
        raise ValueError("NGM company list items payload not found")
    items = json.loads(unescape(match.group(1)))
    if not isinstance(items, list):
        raise ValueError("NGM company list items payload is not a list")
    return items


def build_ngm_companies_page_rows(
    items: list[dict[str, Any]],
    source: MasterfileSource,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for item in items:
        name = str(item.get("title") or item.get("name") or "").strip()
        if not name:
            continue
        for symbol in item.get("symbols") or []:
            if not symbol.get("is_primary"):
                continue
            ticker = normalize_nasdaq_nordic_ticker(str(symbol.get("symbol", "")))
            if not ticker or ticker in seen:
                continue
            row = {
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
            isin = str(symbol.get("isin") or "").strip().upper()
            if is_valid_isin(isin):
                row["isin"] = isin
            rows.append(row)
            seen.add(ticker)
    return rows


def ngm_order_book_ids_by_ticker(items: list[dict[str, Any]]) -> dict[str, str]:
    order_book_ids: dict[str, str] = {}
    for item in items:
        for symbol in item.get("symbols") or []:
            if not symbol.get("is_primary"):
                continue
            ticker = normalize_nasdaq_nordic_ticker(str(symbol.get("symbol", "")))
            order_book_id = str(symbol.get("order_book_id") or symbol.get("orderBookId") or "").strip()
            if ticker and order_book_id:
                order_book_ids[ticker] = order_book_id
    return order_book_ids


def parse_ngm_companies_page_html(text: str, source: MasterfileSource) -> list[dict[str, str]]:
    return build_ngm_companies_page_rows(parse_ngm_companies_page_items(text), source)


def ngm_detailview_headers() -> dict[str, str]:
    return {
        "User-Agent": USER_AGENT,
        "Content-Type": "text/x-gwt-rpc; charset=UTF-8",
        "Referer": "https://mdweb.ngm.se/",
        "X-GWT-Module-Base": NGM_DETAILVIEW_MODULE_BASE_URL,
        "X-GWT-Permutation": NGM_DETAILVIEW_PERMUTATION,
    }


def build_ngm_detailview_find_instrument_payload(order_book_id: str) -> str:
    order_book_id = order_book_id.strip()
    return (
        f"7|0|8|{NGM_DETAILVIEW_MODULE_BASE_URL}|{NGM_DETAILVIEW_STRONG_NAME}|"
        "se.ngm.mdweb.front.client.rpc.SearchRPCService|findInstrument|"
        f"java.lang.String/2004016611|Z|{order_book_id}|en_US|"
        "1|2|3|4|5|5|5|5|5|6|7|0|0|8|0|"
    )


def extract_ngm_detailview_isin(text: str) -> str:
    for candidate in ISIN_TOKEN_RE.findall(text):
        if is_valid_isin(candidate):
            return candidate
    return ""


def fetch_ngm_detailview_isin(
    order_book_id: str,
    *,
    session: requests.Session | None = None,
) -> str:
    order_book_id = order_book_id.strip()
    if not order_book_id:
        return ""
    session = session or requests.Session()
    response = session.post(
        NGM_DETAILVIEW_SERVICE_URL,
        data=build_ngm_detailview_find_instrument_payload(order_book_id).encode("utf-8"),
        headers=ngm_detailview_headers(),
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return extract_ngm_detailview_isin(response.text)


def parse_idx_listed_companies_payload(
    payload: dict[str, Any],
    source: MasterfileSource,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for record in payload.get("data") or []:
        ticker = str(record.get("Code") or "").strip().upper()
        name = str(record.get("Name") or "").strip()
        if not ticker or not name or ticker in seen:
            continue
        seen.add(ticker)
        rows.append(
            {
                "source_key": source.key,
                "provider": source.provider,
                "source_url": source.source_url,
                "ticker": ticker,
                "name": name,
                "exchange": "IDX",
                "asset_type": "Stock",
                "listing_status": "active",
                "reference_scope": source.reference_scope,
                "official": "true",
            }
        )
    return rows


def normalize_idx_sector(sector: Any, subsector: Any = "") -> str:
    sector_text = str(sector or "").strip().lower()
    subsector_text = str(subsector or "").strip().lower()
    if sector_text == "infrastruktur":
        return IDX_INFRASTRUCTURE_SUBSECTOR_MAP.get(subsector_text, "Industrials")
    return IDX_SECTOR_MAP.get(sector_text, "")


def parse_idx_company_profiles_payload(
    payload: dict[str, Any],
    source: MasterfileSource,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for record in payload.get("data") or []:
        ticker = str(record.get("KodeEmiten") or "").strip().upper()
        name = str(record.get("NamaEmiten") or "").strip()
        if not ticker or not name or ticker in seen:
            continue
        seen.add(ticker)
        row = {
            "source_key": source.key,
            "provider": source.provider,
            "source_url": source.source_url,
            "ticker": ticker,
            "name": name,
            "exchange": "IDX",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": source.reference_scope,
            "official": "true",
        }
        sector = normalize_idx_sector(record.get("Sektor"), record.get("SubSektor"))
        if sector:
            row["sector"] = sector
        rows.append(row)
    return rows


def fetch_idx_listed_companies(
    source: MasterfileSource,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    session = session or requests.Session()
    start = 0
    length = 1000
    total = None
    rows: list[dict[str, str]] = []
    seen: set[str] = set()

    while total is None or start < total:
        response = session.get(
            source.source_url,
            params={
                "start": start,
                "length": length,
                "code": "",
                "sector": "",
                "board": "",
                "language": "en-us",
            },
            headers=idx_request_headers(),
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        payload = response.json()
        total = int(payload.get("recordsFiltered") or payload.get("recordsTotal") or 0)
        page_rows = parse_idx_listed_companies_payload(payload, source)
        for row in page_rows:
            ticker = row["ticker"]
            if ticker in seen:
                continue
            seen.add(ticker)
            rows.append(row)
        if not page_rows:
            break
        start += length

    return rows


def fetch_idx_company_profiles(
    source: MasterfileSource,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    session = session or requests.Session()
    start = 0
    length = 1000
    total = None
    rows: list[dict[str, str]] = []
    seen: set[str] = set()

    while total is None or start < total:
        response = session.get(
            source.source_url,
            params={
                "start": start,
                "length": length,
                "code": "",
                "language": "en-us",
            },
            headers=idx_request_headers(),
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        payload = response.json()
        total = int(payload.get("recordsFiltered") or payload.get("recordsTotal") or 0)
        page_rows = parse_idx_company_profiles_payload(payload, source)
        for row in page_rows:
            ticker = row["ticker"]
            if ticker in seen:
                continue
            seen.add(ticker)
            rows.append(row)
        if not page_rows:
            break
        start += length

    return rows


def fetch_spotlight_companies_search(
    source: MasterfileSource,
    *,
    listings_path: Path = LISTINGS_CSV,
    verification_dir: Path = STOCK_VERIFICATION_DIR,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    session = session or requests.Session()
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for listing_row in spotlight_search_target_rows(
        source,
        listings_path=listings_path,
        verification_dir=verification_dir,
    ):
        listing_ticker = listing_row.get("ticker", "").strip().upper()
        listing_name = listing_row.get("name", "").strip()
        if not listing_ticker or listing_ticker in seen:
            continue
        response = session.get(
            source.source_url,
            params={"searchText": listing_ticker},
            headers=spotlight_request_headers(),
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        for item in response.json().get("results") or []:
            candidate_name, candidate_ticker = parse_spotlight_search_heading(str(item.get("heading", "")))
            if candidate_ticker != listing_ticker or not candidate_name:
                continue
            if not has_strong_company_name_match(listing_name, candidate_name):
                continue
            rows.append(
                {
                    "source_key": source.key,
                    "provider": source.provider,
                    "source_url": source.source_url,
                    "ticker": listing_ticker,
                    "name": candidate_name,
                    "exchange": "STO",
                    "asset_type": "Stock",
                    "listing_status": "active",
                    "reference_scope": source.reference_scope,
                    "official": "true",
                    "isin": listing_row.get("isin", "").strip(),
                }
            )
            seen.add(listing_ticker)
            break
    return rows


def fetch_spotlight_companies_directory(
    source: MasterfileSource,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    provided_session = session is not None
    session = session or requests.Session()
    response = session.get(
        source.source_url,
        headers=spotlight_request_headers(),
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    payload = response.json()

    detail_urls: list[str] = []
    results = payload.get("results") or []
    if not results:
        raise requests.RequestException("Spotlight companies directory returned no results")
    for item in results:
        detail_url = str(item.get("url") or "").strip()
        if not detail_url:
            continue
        if detail_url.startswith("/"):
            detail_url = f"https://spotlightstockmarket.com{detail_url}"
        detail_urls.append(detail_url)

    def fetch_detail_row(detail_url: str, detail_session: requests.Session) -> dict[str, str] | None:
        try:
            detail_response = detail_session.get(
                detail_url,
                headers={"User-Agent": USER_AGENT},
                timeout=SPOTLIGHT_DETAIL_TIMEOUT,
            )
            detail_response.raise_for_status()
        except requests.RequestException:
            return None
        match = re.search(r"<title>(.*?)</title>", detail_response.text, re.I | re.S)
        if match is None:
            return None
        name, ticker = parse_spotlight_company_title(unescape(strip_html_tags(match.group(1))))
        ticker = normalize_nasdaq_nordic_ticker(ticker)
        if not ticker or not name:
            return None
        row = {
            "source_key": source.key,
            "provider": source.provider,
            "source_url": detail_url,
            "ticker": ticker,
            "name": name,
            "exchange": "STO",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": source.reference_scope,
            "official": "true",
        }
        try:
            trade_response = detail_session.get(
                spotlight_trade_url_from_detail_url(detail_url),
                headers={"User-Agent": USER_AGENT},
                timeout=SPOTLIGHT_DETAIL_TIMEOUT,
            )
            trade_response.raise_for_status()
        except requests.RequestException:
            return row
        isin = extract_spotlight_detail_isin(trade_response.text)
        if isin:
            row["isin"] = isin
        return row

    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    if provided_session:
        for detail_url in detail_urls:
            row = fetch_detail_row(detail_url, session)
            if row is None or row["ticker"] in seen:
                continue
            rows.append(row)
            seen.add(row["ticker"])
        return rows

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {
            executor.submit(fetch_detail_row, detail_url, requests.Session()): detail_url
            for detail_url in detail_urls
        }
        for future in as_completed(futures):
            row = future.result()
            if row is None or row["ticker"] in seen:
                continue
            rows.append(row)
            seen.add(row["ticker"])
    return rows


def fetch_ngm_companies_page(
    source: MasterfileSource,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    session = session or requests.Session()
    response = session.get(
        source.source_url,
        headers={"User-Agent": USER_AGENT},
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    items = parse_ngm_companies_page_items(response.text)
    rows = build_ngm_companies_page_rows(items, source)
    order_book_ids = ngm_order_book_ids_by_ticker(items)
    for row in rows:
        if row.get("isin"):
            continue
        order_book_id = order_book_ids.get(row["ticker"], "")
        if not order_book_id:
            continue
        try:
            isin = fetch_ngm_detailview_isin(order_book_id, session=session)
        except requests.RequestException:
            continue
        if isin:
            row["isin"] = isin
    return rows


NGM_MARKET_DATA_EQUITY_ENDPOINTS = (
    "last_trades?cfi=ESVUFR",
    "security_statistics/highest_turnover?cfi=ESVUFR",
    "security_statistics/winners?cfi=ESVUFR",
    "security_statistics/losers?cfi=ESVUFR",
)


def parse_ngm_market_data_equities(
    payload: list[dict[str, Any]],
    source: MasterfileSource,
    *,
    source_url: str | None = None,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    row_source_url = source_url or source.source_url
    for record in payload:
        ticker = normalize_nasdaq_nordic_ticker(str(record.get("symbol") or ""))
        name = str(record.get("name") or "").strip()
        isin = str(record.get("isin") or "").strip().upper()
        currency = str(record.get("currency") or "").strip().upper()
        cfi = str(record.get("cfi") or "").strip().upper()
        if not ticker or not name or not isin or ticker in seen:
            continue
        if currency and currency != "SEK":
            continue
        if cfi and not cfi.startswith("ES"):
            continue
        rows.append(
            {
                "source_key": source.key,
                "provider": source.provider,
                "source_url": row_source_url,
                "ticker": ticker,
                "name": name,
                "exchange": "STO",
                "asset_type": "Stock",
                "listing_status": "active",
                "reference_scope": source.reference_scope,
                "official": "true",
                "isin": isin,
            }
        )
        seen.add(ticker)
    return rows


def fetch_ngm_market_data_equities(
    source: MasterfileSource,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    session = session or requests.Session()
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for endpoint in NGM_MARKET_DATA_EQUITY_ENDPOINTS:
        endpoint_url = f"{source.source_url.rstrip('/')}/{endpoint}"
        response = session.get(
            endpoint_url,
            headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        for row in parse_ngm_market_data_equities(
            response.json(),
            source,
            source_url=endpoint_url,
        ):
            if row["ticker"] in seen:
                continue
            rows.append(row)
            seen.add(row["ticker"])
    return rows


def bme_cache_paths(source_key: str) -> tuple[Path, Path]:
    mapping = {
        "bme_listed_companies": (BME_LISTED_COMPANIES_CACHE, LEGACY_BME_LISTED_COMPANIES_CACHE),
        "bme_etf_list": (BME_ETF_LIST_CACHE, LEGACY_BME_ETF_LIST_CACHE),
        "bme_listed_values": (BME_LISTED_VALUES_CACHE, LEGACY_BME_LISTED_VALUES_CACHE),
        "bme_security_prices_directory": (BME_SECURITY_PRICES_CACHE, LEGACY_BME_SECURITY_PRICES_CACHE),
        "bme_growth_prices": (BME_GROWTH_PRICES_CACHE, LEGACY_BME_GROWTH_PRICES_CACHE),
    }
    return mapping[source_key]


def bme_ticker_variants(ticker: str) -> list[str]:
    ticker = ticker.strip().upper()
    variants = [ticker]
    if "." in ticker:
        variants.append(ticker.replace(".", "-"))
    deduped: list[str] = []
    seen: set[str] = set()
    for variant in variants:
        if not variant or variant in seen:
            continue
        seen.add(variant)
        deduped.append(variant)
    return deduped


def fetch_bme_listed_companies_payload(
    trading_system: str,
    *,
    session: requests.Session | None = None,
    page_size: int = 100,
) -> list[dict[str, Any]]:
    session = session or requests.Session()
    page = 0
    rows: list[dict[str, Any]] = []

    while True:
        response = session.get(
            BME_LISTED_COMPANIES_API_URL,
            params={"tradingSystem": trading_system, "page": page, "pageSize": page_size},
            headers=bme_request_headers(),
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        payload = response.json()
        data = payload.get("data") or []
        rows.extend(data)
        if not payload.get("hasMoreResults") or not data:
            break
        page += 1

    return rows


def bme_asset_type_from_trading_system(trading_system: str) -> str:
    normalized = trading_system.strip().upper()
    return BME_TRADING_SYSTEM_TO_ASSET_TYPE.get(normalized, "Stock")


def fetch_bme_share_details_info(
    isin: str,
    *,
    session: requests.Session | None = None,
) -> dict[str, Any]:
    session = session or requests.Session()
    response = session.get(
        BME_SHARE_DETAILS_INFO_API_URL,
        params={"isin": isin, "language": "en"},
        headers=bme_request_headers(),
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return response.json()


def build_bme_reference_rows(
    source: MasterfileSource,
    detail: dict[str, Any],
    *,
    exchange: str,
    asset_type: str,
) -> list[dict[str, str]]:
    ticker = str(detail.get("ticker", "")).strip().upper()
    name = str(detail.get("name", "")).strip() or str(detail.get("shortName", "")).strip()
    if not ticker or not name:
        return []

    rows: list[dict[str, str]] = []
    for ticker_variant in bme_ticker_variants(ticker):
        row = {
            "source_key": source.key,
            "provider": source.provider,
            "source_url": source.source_url,
            "ticker": ticker_variant,
            "name": name,
            "exchange": exchange,
            "asset_type": asset_type,
            "listing_status": "active",
            "reference_scope": source.reference_scope,
            "official": "true",
        }
        isin = str(detail.get("isin", "")).strip().upper()
        if isin:
            row["isin"] = isin
        sector = normalize_bme_stock_sector(detail) if asset_type == "Stock" else ""
        if sector:
            row["sector"] = sector
        rows.append(row)
    return rows


def normalize_bme_stock_sector(detail: dict[str, Any]) -> str:
    sector_code = str(detail.get("sector", "")).strip().zfill(2)
    subsector_code = str(detail.get("subsector", "")).strip().zfill(2)
    return (
        BME_SUBSECTOR_TO_CANONICAL_STOCK_SECTOR.get((sector_code, subsector_code))
        or BME_SECTOR_TO_CANONICAL_STOCK_SECTOR.get(sector_code, "")
    )


def parse_bme_listed_values_text_lines(
    lines: Iterable[str],
    source: MasterfileSource,
    *,
    source_url: str | None = None,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    row_source_url = source_url or source.source_url

    for line in lines:
        match = BME_LISTED_VALUES_ROW_RE.match(re.sub(r"\s+", " ", str(line)).strip())
        if not match:
            continue
        asset_type = BME_LISTED_VALUES_SEGMENT_ASSET_TYPES.get(match.group("segment"))
        if not asset_type:
            continue
        name_match = BME_LISTED_VALUES_NAME_RE.match(match.group("rest").strip())
        name = (name_match.group("name") if name_match else match.group("rest")).strip()
        ticker = match.group("ticker").strip().upper()
        isin = match.group("isin").strip().upper()
        row_key = (ticker, asset_type, isin)
        if not ticker or not name or row_key in seen:
            continue
        seen.add(row_key)
        rows.append(
            {
                "source_key": source.key,
                "provider": source.provider,
                "source_url": row_source_url,
                "ticker": ticker,
                "name": name,
                "exchange": "BME",
                "asset_type": asset_type,
                "listing_status": "active",
                "reference_scope": source.reference_scope,
                "official": "true",
                "isin": isin,
            }
        )
    return rows


def parse_bme_listed_values_pdf(
    content: bytes,
    source: MasterfileSource,
    *,
    source_url: str | None = None,
) -> list[dict[str, str]]:
    try:
        import pdfplumber
    except ImportError as exc:
        raise ImportError("BME listed values PDF parsing requires pdfplumber") from exc

    lines: list[str] = []
    with pdfplumber.open(io.BytesIO(content)) as pdf:
        for page in pdf.pages:
            text = page.extract_text(x_tolerance=1, y_tolerance=3) or ""
            lines.extend(text.splitlines())
    return parse_bme_listed_values_text_lines(lines, source, source_url=source_url)


def enrich_bme_listed_value_rows_with_share_details(
    rows: list[dict[str, str]],
    *,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    session = session or requests.Session()
    enriched_rows: list[dict[str, str]] = []
    sector_by_isin: dict[str, str] = {}
    for row in rows:
        enriched_row = dict(row)
        isin = row.get("isin", "").strip().upper()
        if row.get("asset_type") == "Stock" and isin and not row.get("sector"):
            if isin not in sector_by_isin:
                try:
                    detail = fetch_bme_share_details_info(isin, session=session)
                except requests.RequestException:
                    sector_by_isin[isin] = ""
                else:
                    sector_by_isin[isin] = normalize_bme_stock_sector(detail)
            if sector_by_isin[isin]:
                enriched_row["sector"] = sector_by_isin[isin]
        enriched_rows.append(enriched_row)
    return enriched_rows


def fetch_bme_listed_values_rows(
    source: MasterfileSource,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    session = session or requests.Session()
    response = session.get(
        source.source_url,
        headers={
            "User-Agent": BROWSER_USER_AGENT,
            "Accept": "application/pdf,*/*;q=0.8",
            "Referer": "https://www.bolsasymercados.es/en/bme-exchange.html",
        },
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    if not response.content.startswith(b"%PDF"):
        raise requests.RequestException("BME listed values fetch did not return a PDF")

    ensure_output_dirs()
    BME_LISTED_VALUES_PDF_CACHE.write_bytes(response.content)
    return enrich_bme_listed_value_rows_with_share_details(
        parse_bme_listed_values_pdf(response.content, source, source_url=response.url),
        session=session,
    )


def load_bme_listed_values_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    try:
        rows = fetch_bme_listed_values_rows(source, session=session)
    except (requests.RequestException, ValueError, json.JSONDecodeError, ImportError):
        for path in (BME_LISTED_VALUES_CACHE, LEGACY_BME_LISTED_VALUES_CACHE):
            if path.exists():
                return json.loads(path.read_text(encoding="utf-8")), "cache"
        for path in (BME_LISTED_VALUES_PDF_CACHE, LEGACY_BME_LISTED_VALUES_PDF_CACHE):
            if path.exists():
                try:
                    return parse_bme_listed_values_pdf(path.read_bytes(), source), "cache"
                except ImportError:
                    continue
        return None, "unavailable"

    ensure_output_dirs()
    BME_LISTED_VALUES_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def fetch_bme_security_prices_rows(
    source: MasterfileSource,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    session = session or requests.Session()
    listed_items = fetch_bme_listed_companies_payload(
        BME_SECURITY_PRICES_TRADING_SYSTEMS,
        session=session,
    )
    rows: list[dict[str, str]] = []
    seen_pairs: set[tuple[str, str]] = set()

    def add_detail(detail: dict[str, Any]) -> None:
        trading_system = str(detail.get("tradingSystem", "")).strip()
        asset_type = bme_asset_type_from_trading_system(trading_system)
        for row in build_bme_reference_rows(source, detail, exchange="BME", asset_type=asset_type):
            pair = (row["ticker"], row.get("isin", ""))
            if pair in seen_pairs:
                continue
            seen_pairs.add(pair)
            rows.append(row)

    if not isinstance(session, requests.Session) or len(listed_items) <= 1:
        for item in listed_items:
            isin = str(item.get("isin", "")).strip().upper()
            if not isin:
                continue
            try:
                add_detail(fetch_bme_share_details_info(isin, session=session))
            except requests.RequestException:
                continue
        return rows

    max_workers = min(8, len(listed_items))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(fetch_bme_share_details_info, str(item.get("isin", "")).strip().upper()): item
            for item in listed_items
            if str(item.get("isin", "")).strip()
        }
        for future in as_completed(futures):
            try:
                add_detail(future.result())
            except requests.RequestException:
                continue
    return rows


def load_bme_security_prices_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    try:
        rows = fetch_bme_security_prices_rows(source, session=session)
    except (requests.RequestException, ValueError, json.JSONDecodeError):
        for path in bme_cache_paths(source.key):
            if path.exists():
                cached_rows = json.loads(path.read_text(encoding="utf-8"))
                if len(cached_rows) >= BME_SECURITY_PRICES_MIN_CACHE_ROWS:
                    return cached_rows, "cache"
        return None, "unavailable"

    ensure_output_dirs()
    bme_cache_paths(source.key)[0].write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def parse_athex_classification_lines(
    lines: Iterable[str],
    source: MasterfileSource,
    *,
    listings_by_ticker: dict[str, dict[str, str]] | None = None,
    source_url: str | None = None,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for raw_line in lines:
        line = " ".join(raw_line.split())
        match = ATHEX_CLASSIFICATION_LINE_RE.match(line)
        if match is None:
            continue

        rest = match.group("rest")
        first_code = re.search(r"\s\d{4}\s", rest)
        if first_code is None:
            continue
        company = rest[: first_code.start()].strip()
        tail = rest[first_code.start() + 1 :]
        numeric_tokens = list(re.finditer(r"\b\d{4,8}\b", tail))
        if len(numeric_tokens) < 4:
            continue

        ticker = match.group("ticker").strip().upper()
        isin = match.group("isin").strip().upper()
        if not ticker or ticker in seen or not company or not is_valid_isin(isin):
            continue

        listing = (listings_by_ticker or {}).get(ticker)
        if listings_by_ticker is not None and listing is None:
            continue
        asset_type = (listing or {}).get("asset_type", "Stock")
        if asset_type != "Stock":
            continue

        new_super_sector = tail[numeric_tokens[2].end() : numeric_tokens[3].start()].strip()
        rows.append(
            {
                "source_key": source.key,
                "provider": source.provider,
                "source_url": source_url or source.source_url,
                "ticker": ticker,
                "name": (listing or {}).get("name") or company,
                "exchange": "ATHEX",
                "asset_type": "Stock",
                "listing_status": "active",
                "reference_scope": source.reference_scope,
                "official": "true",
                "isin": isin,
                "sector": ATHEX_SUPER_SECTOR_MAP.get(new_super_sector, ""),
            }
        )
        seen.add(ticker)
    if not rows:
        raise ValueError("Unexpected ATHEX classification PDF text")
    return rows


def parse_athex_classification_pdf(
    content: bytes,
    source: MasterfileSource,
    *,
    listings_by_ticker: dict[str, dict[str, str]] | None = None,
    source_url: str | None = None,
) -> list[dict[str, str]]:
    try:
        import pdfplumber
    except ImportError as exc:
        raise ImportError("ATHEX classification PDF parsing requires pdfplumber") from exc

    lines: list[str] = []
    with pdfplumber.open(io.BytesIO(content)) as pdf:
        for page in pdf.pages:
            text = page.extract_text(x_tolerance=1, y_tolerance=3) or ""
            lines.extend(text.splitlines())
    return parse_athex_classification_lines(
        lines,
        source,
        listings_by_ticker=listings_by_ticker,
        source_url=source_url,
    )


def fetch_athex_classification_rows(
    source: MasterfileSource,
    *,
    session: requests.Session | None = None,
    listings_path: Path = LISTINGS_CSV,
) -> list[dict[str, str]]:
    session = session or requests.Session()
    response = session.get(
        source.source_url,
        headers={
            "User-Agent": BROWSER_USER_AGENT,
            "Accept": "application/pdf,*/*;q=0.8",
            "Referer": "https://www.athexgroup.gr/en",
        },
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    if not response.content.startswith(b"%PDF"):
        raise requests.RequestException("ATHEX classification fetch did not return a PDF")

    ensure_output_dirs()
    ATHEX_CLASSIFICATION_PDF_CACHE.write_bytes(response.content)
    return parse_athex_classification_pdf(
        response.content,
        source,
        listings_by_ticker=listing_rows_by_exchange_ticker("ATHEX", listings_path=listings_path),
        source_url=response.url,
    )


def load_athex_classification_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    for path in (ATHEX_CLASSIFICATION_CACHE, LEGACY_ATHEX_CLASSIFICATION_CACHE):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"
    for path in (ATHEX_CLASSIFICATION_PDF_CACHE, LEGACY_ATHEX_CLASSIFICATION_PDF_CACHE):
        if path.exists():
            try:
                return (
                    parse_athex_classification_pdf(
                        path.read_bytes(),
                        source,
                        listings_by_ticker=listing_rows_by_exchange_ticker("ATHEX"),
                        source_url=source.source_url,
                    ),
                    "cache",
                )
            except ImportError:
                continue
    try:
        rows = fetch_athex_classification_rows(source, session=session)
    except (requests.RequestException, ValueError, json.JSONDecodeError, ImportError):
        return None, "unavailable"

    ensure_output_dirs()
    ATHEX_CLASSIFICATION_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def fetch_bme_reference_rows(
    source: MasterfileSource,
    *,
    listings_path: Path = LISTINGS_CSV,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    config = BME_LISTED_SOURCE_CONFIG[source.key]
    listed_items = fetch_bme_listed_companies_payload(config["trading_system"], session=session)
    rows: list[dict[str, str]] = []
    seen_pairs: set[tuple[str, str]] = set()

    def extend_rows(detail: dict[str, Any]) -> None:
        for row in build_bme_reference_rows(
            source,
            detail,
            exchange=config["exchange"],
            asset_type=config["asset_type"],
        ):
            pair = (row["ticker"], row.get("isin", ""))
            if pair in seen_pairs:
                continue
            seen_pairs.add(pair)
            rows.append(row)

    listings_by_isin: dict[str, list[dict[str, str]]] = {}
    for listing_row in load_csv(listings_path):
        if (
            listing_row.get("exchange") != config["exchange"]
            or listing_row.get("asset_type") != config["asset_type"]
        ):
            continue
        isin = listing_row.get("isin", "").strip().upper()
        if not isin:
            continue
        listings_by_isin.setdefault(isin, []).append(listing_row)

    if config["asset_type"] == "ETF":
        for item in listed_items:
            isin = str(item.get("isin", "")).strip().upper()
            official_name = str(item.get("shareName", "")).strip()
            if not isin or not official_name:
                continue
            for listing_row in listings_by_isin.get(isin, []):
                extend_rows(
                    {
                        "ticker": listing_row.get("ticker", ""),
                        "isin": isin,
                        "name": official_name,
                        "tradingSystem": config["trading_system"],
                    }
                )
        return rows

    for item in listed_items:
        isin = str(item.get("isin", "")).strip().upper()
        official_name = str(item.get("shareName", "")).strip()
        if not isin or not official_name:
            continue
        for listing_row in listings_by_isin.get(isin, []):
            extend_rows(
                {
                    "ticker": listing_row.get("ticker", ""),
                    "isin": isin,
                    "name": official_name,
                    "tradingSystem": config["trading_system"],
                }
            )

    if session is not None or len(listed_items) <= 1:
        for item in listed_items:
            isin = str(item.get("isin", "")).strip().upper()
            if not isin:
                continue
            try:
                detail = fetch_bme_share_details_info(isin, session=session)
            except requests.RequestException:
                continue
            if str(detail.get("tradingSystem", "")).strip().upper() != config["trading_system"]:
                continue
            extend_rows(detail)
        return rows

    max_workers = min(8, len(listed_items))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(fetch_bme_share_details_info, str(item.get("isin", "")).strip().upper()): item
            for item in listed_items
            if str(item.get("isin", "")).strip()
        }
        for future in as_completed(futures):
            try:
                detail = future.result()
            except requests.RequestException:
                continue
            if str(detail.get("tradingSystem", "")).strip().upper() != config["trading_system"]:
                continue
            extend_rows(detail)
    return rows


def load_bme_reference_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    try:
        rows = fetch_bme_reference_rows(source, session=session)
    except (requests.RequestException, ValueError, json.JSONDecodeError):
        for path in bme_cache_paths(source.key):
            if path.exists():
                return json.loads(path.read_text(encoding="utf-8")), "cache"
        return None, "unavailable"

    ensure_output_dirs()
    bme_cache_paths(source.key)[0].write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def extract_bme_growth_detail_links(text: str) -> list[str]:
    links: list[str] = []
    seen: set[str] = set()
    for match in BME_GROWTH_DETAIL_LINK_RE.finditer(text):
        detail_url = requests.compat.urljoin(BME_GROWTH_PRICES_URL, unescape(match.group("href")).strip())
        if detail_url in seen:
            continue
        links.append(detail_url)
        seen.add(detail_url)
    return links


def extract_bme_growth_page_indexes(text: str) -> list[int]:
    indexes = sorted({int(match.group("page")) for match in BME_GROWTH_PAGE_RE.finditer(text)})
    return indexes or [0]


def extract_bme_growth_form_fields(text: str) -> dict[str, str]:
    match = re.search(r"<form\b[^>]*\bid=[\"']Form1[\"'][\s\S]*?</form>", text, re.I)
    form_html = match.group(0) if match else text
    fields: dict[str, str] = {}
    for tag in WSE_INPUT_TAG_RE.findall(form_html):
        name = extract_html_attr(tag, "name").strip()
        if not name:
            continue
        fields[name] = extract_html_attr(tag, "value")
    return fields


def fetch_bme_growth_price_pages(
    *,
    session: requests.Session | None = None,
) -> list[str]:
    session = session or requests.Session()
    response = bme_growth_request_with_retries(
        session,
        "get",
        BME_GROWTH_PRICES_URL,
        headers=bme_growth_request_headers(),
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    pages = [response.text]
    page_indexes = extract_bme_growth_page_indexes(response.text)
    current_text = response.text

    for page_index in [page for page in page_indexes if page > 0]:
        fields = extract_bme_growth_form_fields(current_text)
        fields["ctl00$Contenido$NumPag"] = str(page_index)
        fields["__EVENTTARGET"] = ""
        fields["__EVENTARGUMENT"] = ""
        # This submit input triggers the search handler and prevents page navigation when replayed by requests.
        fields.pop("ctl00$Contenido$Buscar", None)
        fields.pop("texto", None)
        page_response = bme_growth_request_with_retries(
            session,
            "post",
            BME_GROWTH_PRICES_URL,
            data=fields,
            headers={
                **bme_growth_request_headers(),
                "Content-Type": "application/x-www-form-urlencoded",
            },
            timeout=REQUEST_TIMEOUT,
        )
        page_response.raise_for_status()
        pages.append(page_response.text)
        current_text = page_response.text

    return pages


def parse_bme_growth_detail_page(
    text: str,
    source: MasterfileSource,
    *,
    detail_url: str,
) -> dict[str, str] | None:
    fields: dict[str, str] = {}
    for match in BME_GROWTH_DETAIL_FIELD_RE.finditer(text):
        label = " ".join(unescape(strip_html_tags(match.group("label"))).split()).lower()
        value = " ".join(unescape(strip_html_tags(match.group("value"))).split())
        fields[label] = value

    ticker = fields.get("ticker", "").strip().upper()
    isin = fields.get("isin", "").strip().upper()
    if not isin:
        isin_match = re.search(r"_([A-Z]{2}[A-Z0-9]{10})\.aspx$", detail_url, re.I)
        if isin_match:
            isin = isin_match.group(1).upper()
    name = ""
    h1_match = BME_GROWTH_H1_RE.search(text)
    if h1_match:
        name = " ".join(unescape(strip_html_tags(h1_match.group("name"))).split())
    if not name:
        name = fields.get("security name", "").strip()
    if not ticker or not name or not isin:
        return None
    return {
        "source_key": source.key,
        "provider": source.provider,
        "source_url": detail_url,
        "ticker": ticker,
        "name": name,
        "exchange": "BME",
        "asset_type": "Stock",
        "listing_status": "active",
        "reference_scope": source.reference_scope,
        "official": "true",
        "isin": isin,
    }


def fetch_bme_growth_reference_rows(
    source: MasterfileSource,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    session = session or requests.Session()
    detail_urls: list[str] = []
    seen_urls: set[str] = set()
    for page_html in fetch_bme_growth_price_pages(session=session):
        for detail_url in extract_bme_growth_detail_links(page_html):
            if detail_url in seen_urls:
                continue
            detail_urls.append(detail_url)
            seen_urls.add(detail_url)

    rows: list[dict[str, str]] = []
    seen_tickers: set[str] = set()
    for detail_url in detail_urls:
        response = bme_growth_request_with_retries(
            session,
            "get",
            detail_url,
            headers=bme_growth_request_headers(BME_GROWTH_PRICES_URL),
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        row = parse_bme_growth_detail_page(response.text, source, detail_url=detail_url)
        if row is None or row["ticker"] in seen_tickers:
            continue
        rows.append(row)
        seen_tickers.add(row["ticker"])
    return rows


def load_bme_growth_reference_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    try:
        rows = fetch_bme_growth_reference_rows(source, session=session)
    except (requests.RequestException, ValueError, json.JSONDecodeError):
        for path in (BME_GROWTH_PRICES_CACHE, LEGACY_BME_GROWTH_PRICES_CACHE):
            if path.exists():
                return json.loads(path.read_text(encoding="utf-8")), "cache"
        return None, "unavailable"

    ensure_output_dirs()
    BME_GROWTH_PRICES_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def extract_bursa_equity_isin_pdf_url(text: str, base_url: str = BURSA_EQUITY_ISIN_PAGE_URL) -> str:
    href_match = BURSA_EQUITY_ISIN_PDF_HREF_RE.search(text)
    if href_match:
        return urljoin(base_url, unescape(href_match.group("href")))

    url_match = BURSA_EQUITY_ISIN_PDF_URL_RE.search(text)
    if url_match:
        return unescape(url_match.group(0))

    return ""


def bursa_asset_type_from_issue_description(issue_description: str) -> str:
    normalized_issue = issue_description.strip().upper()
    if normalized_issue in BURSA_ETF_ISSUE_DESCRIPTIONS:
        return "ETF"
    if normalized_issue in BURSA_STOCK_ISSUE_DESCRIPTIONS:
        return "Stock"
    return ""


def derive_bursa_ticker_from_isin(isin: str, issue_description: str) -> str:
    normalized_isin = isin.strip().upper()
    normalized_issue = issue_description.strip().upper()
    if len(normalized_isin) != 12:
        return ""

    if normalized_issue in BURSA_ETF_ISSUE_DESCRIPTIONS:
        match = BURSA_ETF_ISIN_RE.fullmatch(normalized_isin)
        return match.group("ticker") if match else ""

    if normalized_issue == "REITS":
        for pattern in (BURSA_REIT_ISIN_RE, BURSA_STAPLED_REIT_ISIN_RE):
            match = pattern.fullmatch(normalized_isin)
            if match:
                return match.group("ticker")
        return ""

    if normalized_issue == "CLOSE END FUND":
        match = BURSA_CLOSED_END_FUND_ISIN_RE.fullmatch(normalized_isin)
        return match.group("ticker") if match else ""

    if normalized_issue not in BURSA_STOCK_ISSUE_DESCRIPTIONS:
        return ""

    for pattern in (BURSA_EQUITY_ISIN_4_DIGIT_RE, BURSA_EQUITY_ISIN_5_DIGIT_RE):
        match = pattern.fullmatch(normalized_isin)
        if match:
            return match.group("ticker")
    return ""


def clean_bursa_pdf_cell(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def parse_bursa_equity_isin_table_rows(
    table_rows: Iterable[Iterable[Any]],
    source: MasterfileSource,
    *,
    listings_by_isin: dict[str, list[dict[str, str]]] | None = None,
    source_url: str | None = None,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    row_source_url = source_url or source.source_url
    listings_by_isin = listings_by_isin or {}

    for raw_cells in table_rows:
        cells = [clean_bursa_pdf_cell(cell) for cell in raw_cells]
        if len(cells) < 5 or not cells[0].isdigit():
            continue

        isin_index = next(
            (
                index
                for index, cell in enumerate(cells)
                if re.fullmatch(r"[A-Z]{2}[A-Z0-9]{10}", cell.upper())
            ),
            -1,
        )
        if isin_index < 0 or isin_index + 1 >= len(cells):
            continue

        isin = cells[isin_index].upper()
        issue_description = cells[isin_index + 1].upper()
        asset_type = bursa_asset_type_from_issue_description(issue_description)
        ticker = derive_bursa_ticker_from_isin(isin, issue_description)
        name = cells[1] if len(cells) > 1 else ""
        short_name = cells[2] if len(cells) > 2 else ""
        if not asset_type or not (name or short_name):
            continue

        tickers = [ticker] if ticker else []
        if not tickers:
            tickers = sorted(
                {
                    row.get("ticker", "").strip().upper()
                    for row in listings_by_isin.get(isin, [])
                    if row.get("exchange") == "Bursa"
                    and row.get("asset_type") == asset_type
                    and row.get("ticker", "").strip()
                }
            )
        for row_ticker in tickers:
            row_key = (row_ticker, asset_type, isin)
            if row_key in seen:
                continue
            seen.add(row_key)
            rows.append(
                {
                    "source_key": source.key,
                    "provider": source.provider,
                    "source_url": row_source_url,
                    "ticker": row_ticker,
                    "name": name or short_name,
                    "exchange": "Bursa",
                    "asset_type": asset_type,
                    "listing_status": "active",
                    "reference_scope": source.reference_scope,
                    "official": "true",
                    "isin": isin,
                }
            )
    return rows


def parse_bursa_equity_isin_pdf(
    content: bytes,
    source: MasterfileSource,
    *,
    listings_by_isin: dict[str, list[dict[str, str]]] | None = None,
    source_url: str | None = None,
) -> list[dict[str, str]]:
    try:
        import pdfplumber
    except ImportError as exc:
        raise ImportError("Bursa equity ISIN PDF parsing requires pdfplumber") from exc

    table_rows: list[list[Any]] = []
    with pdfplumber.open(io.BytesIO(content)) as pdf:
        for page in pdf.pages:
            for table in page.extract_tables() or []:
                table_rows.extend(table)
    return parse_bursa_equity_isin_table_rows(
        table_rows,
        source,
        listings_by_isin=listings_by_isin,
        source_url=source_url,
    )


def fetch_bursa_equity_isin_pdf_direct(
    source: MasterfileSource,
    *,
    session: requests.Session | None = None,
) -> tuple[bytes, str]:
    session = session or requests.Session()
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    page_response = session.get(source.source_url, headers=headers, timeout=REQUEST_TIMEOUT)
    page_response.raise_for_status()
    pdf_url = extract_bursa_equity_isin_pdf_url(page_response.text, source.source_url)
    if not pdf_url:
        raise requests.RequestException("Bursa equity ISIN PDF link not found")

    pdf_response = session.get(
        pdf_url,
        headers={**headers, "Referer": source.source_url, "Accept": "application/pdf,*/*;q=0.8"},
        timeout=REQUEST_TIMEOUT,
    )
    pdf_response.raise_for_status()
    if not pdf_response.content.startswith(b"%PDF"):
        raise requests.RequestException("Bursa equity ISIN direct fetch did not return a PDF")
    return pdf_response.content, pdf_url


def fetch_bursa_equity_isin_pdf_with_browser(source: MasterfileSource) -> tuple[bytes, str]:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise requests.RequestException("Bursa equity ISIN PDF fetch requires Playwright") from exc

    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            try:
                context = browser.new_context(
                    user_agent=(
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
                    )
                )
                page = context.new_page()
                page.goto(source.source_url, wait_until="domcontentloaded", timeout=REQUEST_TIMEOUT * 1000)

                pdf_url = page.evaluate(
                    """() => {
                        const links = Array.from(document.querySelectorAll('a[href]'));
                        const target = links.find((link) => {
                            const href = String(link.href || '').toLowerCase();
                            const text = String(link.textContent || '').toLowerCase();
                            return href.includes('isinequity') ||
                                text.includes('securities listed on bursa malaysia: equity');
                        });
                        return target ? target.href : '';
                    }"""
                )
                if not pdf_url:
                    raise requests.RequestException("Bursa equity ISIN PDF link not found in browser")

                result = page.evaluate(
                    """async (url) => {
                        const response = await fetch(url, { credentials: 'include' });
                        const buffer = await response.arrayBuffer();
                        return {
                            ok: response.ok,
                            status: response.status,
                            url: response.url,
                            contentType: response.headers.get('content-type') || '',
                            bytes: Array.from(new Uint8Array(buffer)),
                        };
                    }""",
                    pdf_url,
                )
            finally:
                browser.close()
    except requests.RequestException:
        raise
    except Exception as exc:
        raise requests.RequestException(f"Bursa equity ISIN browser fetch failed: {exc}") from exc

    status = int(result.get("status") or 0)
    content = bytes(result.get("bytes") or [])
    if not result.get("ok") or status >= 400:
        raise requests.RequestException(f"Bursa equity ISIN PDF fetch returned HTTP {status}")
    if not content.startswith(b"%PDF"):
        content_type = str(result.get("contentType") or "")
        raise requests.RequestException(f"Bursa equity ISIN browser fetch returned {content_type or 'non-PDF content'}")
    return content, str(result.get("url") or pdf_url)


def load_bursa_listing_rows_by_isin(listings_path: Path = LISTINGS_CSV) -> dict[str, list[dict[str, str]]]:
    rows_by_isin: dict[str, list[dict[str, str]]] = {}
    for row in load_csv(listings_path):
        if row.get("exchange") != "Bursa":
            continue
        isin = row.get("isin", "").strip().upper()
        if not isin:
            continue
        rows_by_isin.setdefault(isin, []).append(row)
    return rows_by_isin


def fetch_bursa_equity_isin_rows(
    source: MasterfileSource,
    *,
    listings_path: Path = LISTINGS_CSV,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    try:
        content, pdf_url = fetch_bursa_equity_isin_pdf_direct(source, session=session)
    except requests.RequestException:
        content, pdf_url = fetch_bursa_equity_isin_pdf_with_browser(source)

    ensure_output_dirs()
    BURSA_EQUITY_ISIN_PDF_CACHE.write_bytes(content)
    return parse_bursa_equity_isin_pdf(
        content,
        source,
        listings_by_isin=load_bursa_listing_rows_by_isin(listings_path),
        source_url=pdf_url,
    )


def load_bursa_equity_isin_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    try:
        rows = fetch_bursa_equity_isin_rows(source, session=session)
    except (requests.RequestException, ValueError, json.JSONDecodeError, ImportError):
        for path in (BURSA_EQUITY_ISIN_CACHE, LEGACY_BURSA_EQUITY_ISIN_CACHE):
            if path.exists():
                return json.loads(path.read_text(encoding="utf-8")), "cache"
        for path in (BURSA_EQUITY_ISIN_PDF_CACHE, LEGACY_BURSA_EQUITY_ISIN_PDF_CACHE):
            if path.exists():
                try:
                    return parse_bursa_equity_isin_pdf(path.read_bytes(), source), "cache"
                except ImportError:
                    continue
        return None, "unavailable"

    ensure_output_dirs()
    BURSA_EQUITY_ISIN_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def normalize_bursa_closing_prices_sector(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").replace("\n", " ")).strip().upper()


def parse_bursa_closing_prices_table_rows(
    table_rows: Iterable[Iterable[Any]],
    source: MasterfileSource,
    *,
    source_url: str | None = None,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    row_source_url = source_url or source.source_url

    for raw_cells in table_rows:
        cells = [clean_bursa_pdf_cell(cell) for cell in raw_cells]
        if len(cells) < 5:
            continue

        ticker = cells[0].strip().upper().replace(" ", "")
        short_name = cells[1].strip()
        long_name = cells[2].strip()
        board = normalize_bursa_closing_prices_sector(cells[3])
        sector_label = normalize_bursa_closing_prices_sector(cells[4])
        if not ticker or ticker == "STOCKCODE":
            continue

        asset_type = ""
        sector = ""
        if board in BURSA_CLOSING_PRICES_STOCK_BOARDS:
            asset_type = "Stock"
            sector = BURSA_CLOSING_PRICES_STOCK_SECTOR_MAP.get(sector_label, "")
        elif board == "EXCHANGE TRADED" or sector_label.startswith("EXCHANGE TRADED FUND"):
            asset_type = "ETF"
            sector = BURSA_CLOSING_PRICES_ETF_CATEGORY_MAP.get(sector_label, "")
        if not asset_type:
            continue

        row_key = (ticker, asset_type)
        if row_key in seen:
            continue
        seen.add(row_key)
        row = {
            "source_key": source.key,
            "provider": source.provider,
            "source_url": row_source_url,
            "ticker": ticker,
            "name": long_name or short_name,
            "exchange": "Bursa",
            "asset_type": asset_type,
            "listing_status": "active",
            "reference_scope": source.reference_scope,
            "official": "true",
        }
        if sector:
            row["sector"] = sector
        rows.append(row)
    return rows


def parse_bursa_closing_prices_pdf(
    content: bytes,
    source: MasterfileSource,
    *,
    source_url: str | None = None,
) -> list[dict[str, str]]:
    try:
        import pdfplumber
    except ImportError as exc:
        raise ImportError("Bursa closing-prices PDF parsing requires pdfplumber") from exc

    table_rows: list[list[Any]] = []
    with pdfplumber.open(io.BytesIO(content)) as pdf:
        for page in pdf.pages:
            for table in page.extract_tables() or []:
                table_rows.extend(table)
    return parse_bursa_closing_prices_table_rows(table_rows, source, source_url=source_url)


def fetch_bursa_pdf_url_direct(
    url: str,
    *,
    session: requests.Session | None = None,
) -> bytes:
    session = session or requests.Session()
    response = session.get(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "application/pdf,*/*;q=0.8",
            "Referer": "https://www.bursamalaysia.com/",
        },
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    if not response.content.startswith(b"%PDF"):
        raise requests.RequestException("Bursa direct PDF fetch did not return a PDF")
    return response.content


def fetch_bursa_pdf_url_with_browser(url: str) -> bytes:
    try:
        from playwright.sync_api import Error as PlaywrightError
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise requests.RequestException("Bursa PDF fetch requires Playwright") from exc

    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            try:
                context = browser.new_context(
                    accept_downloads=True,
                    user_agent=(
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
                    ),
                )
                page = context.new_page()
                with page.expect_download(timeout=REQUEST_TIMEOUT * 1000) as download_info:
                    try:
                        page.goto(url, wait_until="commit", timeout=REQUEST_TIMEOUT * 1000)
                    except PlaywrightError as exc:
                        if "Download is starting" not in str(exc):
                            raise
                download = download_info.value
                path = download.path()
                if not path:
                    raise requests.RequestException("Bursa browser PDF download path unavailable")
                content = Path(path).read_bytes()
            finally:
                browser.close()
    except requests.RequestException:
        raise
    except Exception as exc:
        raise requests.RequestException(f"Bursa browser PDF fetch failed: {exc}") from exc

    if not content.startswith(b"%PDF"):
        raise requests.RequestException("Bursa browser PDF fetch returned non-PDF content")
    return content


def fetch_bursa_closing_prices_rows(
    source: MasterfileSource,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    try:
        content = fetch_bursa_pdf_url_direct(source.source_url, session=session)
    except requests.RequestException:
        content = fetch_bursa_pdf_url_with_browser(source.source_url)

    ensure_output_dirs()
    BURSA_CLOSING_PRICES_PDF_CACHE.write_bytes(content)
    return parse_bursa_closing_prices_pdf(content, source, source_url=source.source_url)


def load_bursa_closing_prices_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    try:
        rows = fetch_bursa_closing_prices_rows(source, session=session)
    except (requests.RequestException, ValueError, json.JSONDecodeError, ImportError):
        for path in (BURSA_CLOSING_PRICES_CACHE, LEGACY_BURSA_CLOSING_PRICES_CACHE):
            if path.exists():
                return json.loads(path.read_text(encoding="utf-8")), "cache"
        for path in (BURSA_CLOSING_PRICES_PDF_CACHE, LEGACY_BURSA_CLOSING_PRICES_PDF_CACHE):
            if path.exists():
                try:
                    return parse_bursa_closing_prices_pdf(path.read_bytes(), source), "cache"
                except ImportError:
                    continue
        return None, "unavailable"

    ensure_output_dirs()
    BURSA_CLOSING_PRICES_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def build_bmv_reference_row(
    source: MasterfileSource,
    listing_row: dict[str, str],
    *,
    name: str,
    exchange: str,
    asset_type: str = "Stock",
    listing_status: str = "active",
    isin: str = "",
    sector: str = "",
) -> dict[str, str]:
    row = {
        "source_key": source.key,
        "provider": source.provider,
        "source_url": source.source_url,
        "ticker": listing_row.get("ticker", "").strip().upper(),
        "name": name,
        "exchange": exchange,
        "asset_type": asset_type,
        "listing_status": listing_status,
        "reference_scope": source.reference_scope,
        "official": "true",
    }
    isin = isin.strip() or listing_row.get("isin", "").strip()
    if isin:
        row["isin"] = isin
    sector = normalize_sector(sector.strip(), asset_type)
    if sector:
        row["sector"] = sector
    return row


def bmv_issuer_directory_target_rows(
    *,
    listings_path: Path = LISTINGS_CSV,
    stock_verification_dir: Path = STOCK_VERIFICATION_DIR,
    etf_verification_dir: Path = ETF_VERIFICATION_DIR,
) -> list[dict[str, str]]:
    target_tickers = latest_verification_tickers(
        stock_verification_dir,
        exchanges={"BMV"},
        statuses={"reference_gap", "missing_from_official"},
    ) | latest_verification_tickers(
        etf_verification_dir,
        exchanges={"BMV"},
        statuses={"reference_gap", "missing_from_official"},
    )
    if not target_tickers:
        return []
    return [
        row
        for row in load_csv(listings_path)
        if row.get("exchange") == "BMV"
        and (
            row.get("ticker", "").strip().upper() in target_tickers
            or not row.get("isin", "").strip()
        )
    ]


def bmv_issuer_directory_record_keys(record: dict[str, Any]) -> set[str]:
    keys: set[str] = set()
    serie = str(record.get("serie") or "").strip().upper().replace(" ", "").replace("*", "")
    for value in (record.get("claveEmisora"), record.get("claveEmision")):
        clave = str(value or "").strip().upper()
        if not clave:
            continue
        keys.add(clave)
        keys.add(bmv_compose_reference_ticker(clave, serie))
    return keys


def fetch_bmv_issuer_directory_records(
    source: MasterfileSource,
    *,
    session: requests.Session | None = None,
) -> list[dict[str, Any]]:
    session = session or requests.Session()
    headers = {
        "User-Agent": USER_AGENT,
        "Referer": BMV_ISSUER_DIRECTORY_URL,
        "X-Requested-With": "XMLHttpRequest",
    }
    results: list[dict[str, Any]] = []
    for market, instrument in BMV_ISSUER_DIRECTORY_QUERY_COMBINATIONS:
        response = session.get(
            source.source_url,
            params={
                "idTipoMercado": market,
                "idTipoInstrumento": instrument,
                "idTipoEmpresa": "",
                "idSector": "",
                "idSubsector": "",
                "idRamo": "",
                "idSubramo": "",
            },
            headers=headers,
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        payload = extract_bmv_json_wrapper_payload(response.text)
        for record in payload.get("response", {}).get("resultado") or []:
            if not isinstance(record, dict):
                continue
            enriched = dict(record)
            enriched["_market"] = market
            enriched["_instrument"] = instrument
            results.append(enriched)
    return results


def fetch_bmv_issuer_directory(
    source: MasterfileSource,
    *,
    listings_path: Path = LISTINGS_CSV,
    stock_verification_dir: Path = STOCK_VERIFICATION_DIR,
    etf_verification_dir: Path = ETF_VERIFICATION_DIR,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    target_rows = bmv_issuer_directory_target_rows(
        listings_path=listings_path,
        stock_verification_dir=stock_verification_dir,
        etf_verification_dir=etf_verification_dir,
    )
    if not target_rows:
        return []

    records = fetch_bmv_issuer_directory_records(source, session=session)
    rows: list[dict[str, str]] = []
    for listing_row in target_rows:
        listing_ticker = listing_row.get("ticker", "").strip().upper()
        listing_isin = listing_row.get("isin", "").strip().upper()
        listing_asset_type = listing_row.get("asset_type", "")
        best_score = 0
        best_matches: list[dict[str, Any]] = []
        for record in records:
            instrument = str(record.get("_instrument") or "")
            is_etf_bucket = instrument in {"CGEN_ELTRA", "CGEN_ELGE"}
            if listing_asset_type == "ETF" and not is_etf_bucket:
                continue
            if listing_asset_type == "Stock" and is_etf_bucket:
                continue

            score = 0
            if listing_ticker and listing_ticker in bmv_issuer_directory_record_keys(record):
                score += 2
            record_isin = str(record.get("isin") or "").strip().upper()
            if listing_isin and record_isin and listing_isin == record_isin:
                score += 1
            if score == 0:
                continue
            if score > best_score:
                best_score = score
                best_matches = [record]
            elif score == best_score:
                best_matches.append(record)

        unique_matches = {
            (
                str(match.get("claveEmisora") or ""),
                str(match.get("claveEmision") or ""),
                str(match.get("serie") or ""),
                str(match.get("razonSocial") or ""),
                str(match.get("isin") or ""),
            ): match
            for match in best_matches
        }
        if best_score == 0 or len(unique_matches) != 1:
            continue
        match = next(iter(unique_matches.values()))
        record_isin = str(match.get("isin") or "").strip()
        if not record_isin and not listing_isin:
            continue
        name = (
            str(match.get("razonSocial") or "").strip()
            or str(match.get("descripcion") or "").strip()
        )
        if not name:
            continue
        rows.append(
            build_bmv_reference_row(
                source,
                listing_row,
                name=name,
                exchange="BMV",
                asset_type=listing_asset_type or "Stock",
                isin=record_isin,
            )
        )
    return rows


def extract_bmv_search_hits(payload: dict[str, Any]) -> list[dict[str, Any]]:
    busqueda_panel = payload.get("response", {}).get("busquedaPanel", {})
    if not isinstance(busqueda_panel, dict):
        return []
    stack: list[Any] = [busqueda_panel]
    hits: list[dict[str, Any]] = []
    while stack:
        current = stack.pop()
        if isinstance(current, dict):
            source = current.get("_source")
            if isinstance(source, dict):
                hits.append(source)
            else:
                stack.extend(current.values())
        elif isinstance(current, list):
            stack.extend(current)
    return hits


def fetch_bmv_search_hits(
    source: MasterfileSource,
    term: str,
    *,
    access_token: str,
    session: requests.Session | None = None,
) -> list[dict[str, Any]]:
    session = session or requests.Session()
    response = session.post(
        source.source_url,
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        },
        json={
            "lang": "es",
            "payload": {
                "term": term,
                "term2": "",
                "termT": term,
                "searchType": "busquedaPanel",
            },
        },
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return extract_bmv_search_hits(response.json())


def bmv_stock_search_terms(ticker: str) -> list[str]:
    normalized = ticker.strip().upper()
    if not normalized:
        return []
    queue: list[tuple[str, bool]] = [(normalized, True)]
    if "-" in normalized:
        queue.append((normalized.split("-", 1)[0], True))
    trailing_numeric_match = re.fullmatch(r"(?P<base>[A-Z0-9]*[A-Z])\d+", normalized)
    if trailing_numeric_match:
        queue.append((trailing_numeric_match.group("base"), False))
    hyphen_series_match = re.fullmatch(r"(?P<base>[A-Z0-9]+)(?P<series>[A-Z])-\d+", normalized)
    if hyphen_series_match:
        queue.append((hyphen_series_match.group("base"), False))
    seen: set[str] = set()
    result: list[str] = []
    while queue:
        candidate, can_trim_series_suffix = queue.pop(0)
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        result.append(candidate)
        if can_trim_series_suffix and candidate.endswith(("A", "B", "N")) and len(candidate) > 1:
            queue.append((candidate[:-1], False))
    return result


def bmv_search_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def bmv_stock_search_hit_ticker_score(listing_ticker: str, hit: dict[str, Any]) -> int:
    normalized_listing_ticker = re.sub(r"[^A-Z0-9]+", "", listing_ticker.strip().upper())
    if not normalized_listing_ticker:
        return 0

    clave = bmv_search_text(hit.get("cve_emisora")).upper()
    serie = bmv_search_text(hit.get("serie")).upper()
    official_ticker = re.sub(r"[^A-Z0-9]+", "", bmv_compose_reference_ticker(clave, serie))
    if official_ticker and official_ticker == normalized_listing_ticker:
        return 3

    normalized_clave = re.sub(r"[^A-Z0-9]+", "", clave)
    if not normalized_clave:
        return 0
    if normalized_listing_ticker == normalized_clave:
        return 2
    if not normalized_listing_ticker.startswith(normalized_clave):
        return 0

    suffix = normalized_listing_ticker[len(normalized_clave) :]
    if suffix.isdigit():
        return 1
    if re.fullmatch(r"[A-Z]\d+", suffix):
        return 1
    if re.fullmatch(r"[A-Z]{1,4}", suffix):
        return 1
    return 0


def is_bmv_equity_search_hit(
    hit: dict[str, Any],
    *,
    allow_suspended: bool = False,
    allow_partial_metadata: bool = False,
) -> bool:
    descripcion = bmv_search_text(hit.get("descripcion")).upper()
    mercado = bmv_search_text(hit.get("mercado")).upper()
    estatus = bmv_search_text(hit.get("estatus")).upper()

    if descripcion:
        if not descripcion.startswith("ACCIONES") or "SOCIEDADES DE INVERSION" in descripcion:
            return False
    elif not allow_partial_metadata:
        return False

    if mercado:
        if mercado not in {"CAPITALES", "GLOBAL"}:
            return False
    elif not allow_partial_metadata:
        return False

    if estatus:
        if estatus == "ACTIVA":
            return True
        return allow_suspended and estatus == "SUSPENDIDA"
    return allow_partial_metadata


def select_bmv_unique_stock_search_hit(
    listing_row: dict[str, str],
    hits: list[dict[str, Any]],
) -> dict[str, Any] | None:
    listing_name = listing_row.get("name", "").strip()
    listing_ticker = listing_row.get("ticker", "").strip().upper()
    if not listing_name or not listing_ticker:
        return None

    exact_equity_candidates: list[tuple[int, dict[str, Any]]] = []
    partial_root_candidates: list[tuple[int, dict[str, Any]]] = []
    for hit in hits:
        official_name = bmv_search_text(hit.get("razon_social"))
        clave = bmv_search_text(hit.get("cve_emisora")).upper()
        if not official_name or not clave:
            continue
        if not (
            has_strong_company_name_match(listing_name, official_name)
            or has_strong_company_name_match(official_name, listing_name)
        ):
            continue

        ticker_score = bmv_stock_search_hit_ticker_score(listing_ticker, hit)
        if ticker_score == 0:
            continue

        if is_bmv_equity_search_hit(hit, allow_suspended=True):
            exact_equity_candidates.append((ticker_score, hit))
            continue
        if ticker_score == 1 and is_bmv_equity_search_hit(hit, allow_partial_metadata=True):
            partial_root_candidates.append((ticker_score, hit))

    for candidates in (exact_equity_candidates, partial_root_candidates):
        if not candidates:
            continue
        best_score = max(score for score, _ in candidates)
        unique_candidates: list[dict[str, Any]] = []
        seen_signatures: set[tuple[str, str]] = set()
        for score, hit in candidates:
            if score != best_score:
                continue
            signature = (
                bmv_search_text(hit.get("cve_emisora")).upper(),
                bmv_search_text(hit.get("serie")).upper(),
            )
            if signature in seen_signatures:
                continue
            seen_signatures.add(signature)
            unique_candidates.append(hit)
        if len(unique_candidates) == 1:
            return unique_candidates[0]
    return None


def bmv_etf_search_terms(ticker: str) -> list[str]:
    normalized = ticker.strip().upper()
    if not normalized:
        return []
    candidates = [normalized]
    if "-" in normalized:
        candidates.append(normalized.split("-", 1)[0])
    for suffix in ("ISHRS", "ISHR", "ISH", "N", "C", "M"):
        if normalized.endswith(suffix) and len(normalized) > len(suffix) + 1:
            candidates.append(normalized[: -len(suffix)])
    if len(normalized) > 3 and normalized[-2:].isdigit():
        candidates.append(normalized[:-2])
    seen: set[str] = set()
    result: list[str] = []
    for candidate in candidates:
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        result.append(candidate)
    return result


def is_bmv_etf_search_hit(hit: dict[str, Any]) -> bool:
    descripcion = str(hit.get("descripcion", "")).strip().upper()
    mercado = str(hit.get("mercado", "")).strip().upper()
    estatus = str(hit.get("estatus", "")).strip().upper()
    return (
        mercado in {"CAPITALES", "GLOBAL"}
        and estatus == "ACTIVA"
        and any(marker in descripcion for marker in BMV_ETF_DESCRIPTION_MARKERS)
    )


def bmv_search_hit_matches_listing_ticker(listing_ticker: str, hit: dict[str, Any]) -> bool:
    official_ticker = bmv_compose_reference_ticker(
        str(hit.get("cve_emisora", "")),
        str(hit.get("serie", "")),
    )
    normalized_listing_ticker = listing_ticker.strip().upper()
    if not normalized_listing_ticker or not official_ticker:
        return False
    if official_ticker == normalized_listing_ticker:
        return True
    return (
        normalized_listing_ticker.endswith(("ISH", "ISHR"))
        and official_ticker.startswith(normalized_listing_ticker)
    )


def select_bmv_unique_etf_search_hit(
    listing_row: dict[str, str],
    hits: list[dict[str, Any]],
) -> dict[str, Any] | None:
    listing_ticker = listing_row.get("ticker", "").strip().upper()
    candidates = [
        hit
        for hit in hits
        if is_bmv_etf_search_hit(hit)
        and bmv_search_hit_matches_listing_ticker(listing_ticker, hit)
    ]
    unique_candidates: list[dict[str, Any]] = []
    seen_signatures: set[tuple[str, str]] = set()
    for hit in candidates:
        signature = (
            str(hit.get("cve_emisora", "")).strip().upper(),
            str(hit.get("serie", "")).strip().upper(),
        )
        if signature in seen_signatures:
            continue
        seen_signatures.add(signature)
        unique_candidates.append(hit)
    if len(unique_candidates) != 1:
        return None
    return unique_candidates[0]


def fetch_bmv_exact_search_matches(
    source: MasterfileSource,
    *,
    listings_path: Path = LISTINGS_CSV,
    verification_dir: Path = STOCK_VERIFICATION_DIR,
    session: requests.Session | None = None,
) -> list[tuple[dict[str, str], dict[str, Any]]]:
    config = BMV_SEARCH_SOURCE_CONFIG[source.key]
    session = session or requests.Session()
    quote_response = session.get(
        BMV_MOBILE_QUOTE_KEYS_URL,
        params={"idBusquedaCotizacion": config["quote_search_id"]},
        timeout=REQUEST_TIMEOUT,
    )
    quote_response.raise_for_status()
    quote_items = extract_bmv_json_wrapper_payload(quote_response.text).get("response", {}).get("clavesCotizacion") or []
    quote_lookup = {
        bmv_compose_reference_ticker(
            str(item.get("clave", "")),
            str(item.get("serie", "")),
        ): (
            str(item.get("clave", "")).strip().upper(),
            str(item.get("serie", "")).strip().upper(),
        )
        for item in quote_items
        if str(item.get("clave", "")).strip()
    }

    matches: list[tuple[dict[str, str], dict[str, Any]]] = []
    listings_with_quote_matches: list[tuple[dict[str, str], str, str]] = []

    for listing_row in bmv_search_target_rows(
        source,
        listings_path=listings_path,
        verification_dir=verification_dir,
    ):
        listing_ticker = listing_row.get("ticker", "").strip().upper()
        quote_match = quote_lookup.get(listing_ticker)
        if quote_match is None:
            continue
        clave, serie = quote_match
        listings_with_quote_matches.append((listing_row, clave, serie))

    if not listings_with_quote_matches:
        return matches

    token_response = session.get(BMV_SEARCH_TOKEN_URL, timeout=REQUEST_TIMEOUT)
    token_response.raise_for_status()
    access_token = str(token_response.json().get("response", {}).get("access_token", "")).strip()
    if not access_token:
        raise ValueError("BMV search token missing access_token")

    cached_hits_by_clave: dict[str, list[dict[str, Any]]] = {}
    for listing_row, clave, serie in listings_with_quote_matches:
        if clave not in cached_hits_by_clave:
            cached_hits_by_clave[clave] = fetch_bmv_search_hits(
                source,
                clave,
                access_token=access_token,
                session=session,
            )

        exact_hit = next(
            (
                hit
                for hit in cached_hits_by_clave[clave]
                if str(hit.get("cve_emisora", "")).strip().upper() == clave
                and str(hit.get("serie", "")).strip().upper() == serie
            ),
            None,
        )
        if exact_hit is not None:
            matches.append((listing_row, exact_hit))
    return matches


def fetch_bmv_stock_search(
    source: MasterfileSource,
    *,
    listings_path: Path = LISTINGS_CSV,
    verification_dir: Path = STOCK_VERIFICATION_DIR,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    config = BMV_SEARCH_SOURCE_CONFIG[source.key]
    rows: list[dict[str, str]] = []
    covered_tickers: set[str] = set()
    for listing_row, hit in fetch_bmv_exact_search_matches(
        source,
        listings_path=listings_path,
        verification_dir=verification_dir,
        session=session,
    ):
        official_name = str(hit.get("razon_social", "")).strip()
        if not is_bmv_equity_search_hit(hit, allow_suspended=True):
            continue
        if not official_name:
            continue
        rows.append(
            build_bmv_reference_row(
                source,
                listing_row,
                name=official_name,
                exchange=config["exchange"],
                isin=str(hit.get("isin") or ""),
            )
        )
        covered_tickers.add(listing_row.get("ticker", "").strip().upper())

    fallback_rows = [
        row
        for row in bmv_search_target_rows(
            source,
            listings_path=listings_path,
            verification_dir=verification_dir,
        )
        if row.get("ticker", "").strip().upper() not in covered_tickers
    ]
    if not fallback_rows:
        return rows

    token_response = (session or requests.Session()).get(BMV_SEARCH_TOKEN_URL, timeout=REQUEST_TIMEOUT)
    token_response.raise_for_status()
    access_token = str(token_response.json().get("response", {}).get("access_token", "")).strip()
    if not access_token:
        raise ValueError("BMV search token missing access_token")

    session = session or requests.Session()
    for listing_row in fallback_rows:
        selected_hit = None
        for term in bmv_stock_search_terms(listing_row.get("ticker", "")):
            hits = fetch_bmv_search_hits(
                source,
                term,
                access_token=access_token,
                session=session,
            )
            selected_hit = select_bmv_unique_stock_search_hit(listing_row, hits)
            if selected_hit is not None:
                break
        if selected_hit is None:
            continue
        rows.append(
            build_bmv_reference_row(
                source,
                listing_row,
                name=str(selected_hit.get("razon_social", "")).strip(),
                exchange=config["exchange"],
                isin=str(selected_hit.get("isin") or ""),
            )
        )
    return rows


def load_bmv_stock_search_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    try:
        rows = fetch_bmv_stock_search(source, session=session)
    except (requests.RequestException, ValueError, json.JSONDecodeError):
        for path in (BMV_STOCK_SEARCH_CACHE, LEGACY_BMV_STOCK_SEARCH_CACHE):
            if path.exists():
                return json.loads(path.read_text(encoding="utf-8")), "cache"
        return None, "unavailable"

    ensure_output_dirs()
    BMV_STOCK_SEARCH_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def fetch_bmv_capital_trust_search(
    source: MasterfileSource,
    *,
    listings_path: Path = LISTINGS_CSV,
    verification_dir: Path = STOCK_VERIFICATION_DIR,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    config = BMV_SEARCH_SOURCE_CONFIG[source.key]
    rows: list[dict[str, str]] = []
    for listing_row, hit in fetch_bmv_exact_search_matches(
        source,
        listings_path=listings_path,
        verification_dir=verification_dir,
        session=session,
    ):
        descripcion = str(hit.get("descripcion", "")).strip().upper()
        mercado = str(hit.get("mercado", "")).strip().upper()
        estatus = str(hit.get("estatus", "")).strip().upper()
        instrumento = str(hit.get("instrumento", "")).strip()
        if estatus != "ACTIVA" or mercado != "CAPITALES":
            continue
        if descripcion not in BMV_CAPITAL_TRUST_DESCRIPTIONS or not instrumento:
            continue
        rows.append(
            build_bmv_reference_row(
                source,
                listing_row,
                name=instrumento,
                exchange=config["exchange"],
                isin=str(hit.get("isin") or ""),
            )
        )
    return rows


def load_bmv_capital_trust_search_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    try:
        rows = fetch_bmv_capital_trust_search(source, session=session)
    except (requests.RequestException, ValueError, json.JSONDecodeError):
        for path in (BMV_CAPITAL_TRUST_SEARCH_CACHE, LEGACY_BMV_CAPITAL_TRUST_SEARCH_CACHE):
            if path.exists():
                return json.loads(path.read_text(encoding="utf-8")), "cache"
        return None, "unavailable"

    ensure_output_dirs()
    BMV_CAPITAL_TRUST_SEARCH_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def fetch_bmv_etf_search(
    source: MasterfileSource,
    *,
    listings_path: Path = LISTINGS_CSV,
    verification_dir: Path = ETF_VERIFICATION_DIR,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    config = BMV_SEARCH_SOURCE_CONFIG[source.key]
    rows: list[dict[str, str]] = []
    covered_tickers: set[str] = set()
    for listing_row, hit in fetch_bmv_exact_search_matches(
        source,
        listings_path=listings_path,
        verification_dir=verification_dir,
        session=session,
    ):
        if not is_bmv_etf_search_hit(hit):
            continue
        if not bmv_search_hit_matches_listing_ticker(listing_row.get("ticker", ""), hit):
            continue
        name = str(hit.get("instrumento", "")).strip() or str(hit.get("razon_social", "")).strip()
        if not name:
            continue
        rows.append(
            build_bmv_reference_row(
                source,
                listing_row,
                name=name,
                exchange=config["exchange"],
                asset_type="ETF",
                isin=str(hit.get("isin") or ""),
            )
        )
        covered_tickers.add(listing_row.get("ticker", "").strip().upper())

    fallback_rows = [
        row
        for row in bmv_search_target_rows(
            source,
            listings_path=listings_path,
            verification_dir=verification_dir,
        )
        if row.get("ticker", "").strip().upper() not in covered_tickers
    ]
    if not fallback_rows:
        return rows

    session = session or requests.Session()
    token_response = session.get(BMV_SEARCH_TOKEN_URL, timeout=REQUEST_TIMEOUT)
    token_response.raise_for_status()
    access_token = str(token_response.json().get("response", {}).get("access_token", "")).strip()
    if not access_token:
        raise ValueError("BMV search token missing access_token")

    for listing_row in fallback_rows:
        selected_hit = None
        for term in bmv_etf_search_terms(listing_row.get("ticker", "")):
            hits = fetch_bmv_search_hits(
                source,
                term,
                access_token=access_token,
                session=session,
            )
            selected_hit = select_bmv_unique_etf_search_hit(listing_row, hits)
            if selected_hit is not None:
                break
        if selected_hit is None:
            continue
        name = (
            str(selected_hit.get("instrumento", "")).strip()
            or str(selected_hit.get("razon_social", "")).strip()
        )
        if not name:
            continue
        rows.append(
            build_bmv_reference_row(
                source,
                listing_row,
                name=name,
                exchange=config["exchange"],
                asset_type="ETF",
                isin=str(selected_hit.get("isin") or ""),
            )
        )
    return rows


def load_bmv_etf_search_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    try:
        rows = fetch_bmv_etf_search(source, session=session)
    except (requests.RequestException, ValueError, json.JSONDecodeError):
        for path in (BMV_ETF_SEARCH_CACHE, LEGACY_BMV_ETF_SEARCH_CACHE):
            if path.exists():
                return json.loads(path.read_text(encoding="utf-8")), "cache"
        return None, "unavailable"

    ensure_output_dirs()
    BMV_ETF_SEARCH_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def bmv_market_data_target_rows(
    *,
    listings_path: Path = LISTINGS_CSV,
    stock_verification_dir: Path = STOCK_VERIFICATION_DIR,
    etf_verification_dir: Path = ETF_VERIFICATION_DIR,
) -> list[dict[str, str]]:
    stock_gap_tickers = latest_verification_tickers(
        stock_verification_dir,
        exchanges={"BMV"},
        statuses={"reference_gap", "missing_from_official"},
    )
    etf_gap_tickers = latest_verification_tickers(
        etf_verification_dir,
        exchanges={"BMV"},
        statuses={"reference_gap", "missing_from_official"},
    )
    rows: list[dict[str, str]] = []
    for row in load_csv(listings_path):
        if row.get("exchange") != "BMV" or row.get("asset_type") not in {"Stock", "ETF"}:
            continue
        ticker = row.get("ticker", "").strip().upper()
        if not ticker:
            continue
        if row.get("asset_type") == "Stock":
            if ticker in stock_gap_tickers or not row.get("isin", "").strip() or not row.get("stock_sector", "").strip():
                rows.append(row)
            continue
        if ticker in etf_gap_tickers or not row.get("isin", "").strip() or not row.get("etf_category", "").strip():
            rows.append(row)
    return rows


def normalize_bmv_profile_label(value: str) -> str:
    return re.sub(r"[^A-Z0-9]+", " ", ascii_fold(value).upper()).strip()


def normalize_bmv_stock_sector(value: str) -> str:
    return normalize_sector(BMV_STOCK_SECTOR_MAP.get(normalize_bmv_profile_label(value), ""), "Stock")


def normalize_bmv_etf_category(value: str, investment_objective: str = "") -> str:
    normalized = normalize_bmv_profile_label(value)
    objective = normalize_bmv_profile_label(investment_objective)
    combined = f" {normalized} {objective} "
    if "INVERS" in combined or "APALANC" in combined:
        return "Leveraged/Inverse"
    if "MATERIA" in combined or "COMMOD" in combined or "CRUDO" in combined or "WTI" in combined:
        return "Commodity"
    if "DIVISA" in combined or "MONEDA" in combined or "CURRENCY" in combined:
        return "Currency"
    if "MERCADO DE DINERO" in combined or "CASH" in combined:
        return "Money Market"
    if "DEUDA" in combined or "RENTA FIJA" in combined or "BONO" in combined or "BOND" in combined:
        return "Fixed Income"
    if "MULTI" in combined:
        return "Multi-Asset"
    if "ACCION" in combined or "EQUITY" in combined or "INDICE" in combined:
        return "Equity"
    return ""


def parse_bmv_market_data_instruments_html(text: str) -> list[dict[str, str]]:
    parser = _TableParser()
    parser.feed(text)
    instruments: list[dict[str, str]] = []
    for table in parser.tables:
        if not table:
            continue
        headers = [normalize_bmv_profile_label(cell) for cell in table[0]]
        if "SERIE" not in headers or "ESTATUS" not in headers:
            continue
        for raw_row in table[1:]:
            if len(raw_row) != len(headers):
                continue
            normalized_row = dict(zip(headers, raw_row))
            isin = normalized_row.get("ISIN", "").strip().upper()
            if not is_valid_isin(isin):
                isin = ""
            instruments.append(
                {
                    "tipo_valor": normalized_row.get("TIPO VALOR", "").strip(),
                    "serie": normalized_row.get("SERIE", "").strip(),
                    "isin": isin,
                    "estatus": normalized_row.get("ESTATUS", "").strip(),
                    "descripcion": normalized_row.get("DESCRIPCION", "").strip(),
                }
            )
    return instruments


def parse_bmv_market_data_profile_html(text: str) -> dict[str, str]:
    parser = _TableParser()
    parser.feed(text)
    metadata: dict[str, str] = {}
    for table in parser.tables:
        for row in table:
            if len(row) < 2:
                continue
            key = normalize_bmv_profile_label(row[0])
            value = row[1].strip()
            if key and value:
                metadata[key] = value
    return metadata


def bmv_instrument_matches_listing(
    listing_ticker: str,
    issuer: str,
    instrument: dict[str, str],
) -> bool:
    pseudo_hit = {"cve_emisora": issuer, "serie": instrument.get("serie", "")}
    if bmv_search_hit_matches_listing_ticker(listing_ticker, pseudo_hit):
        return True
    official_ticker = re.sub(r"[^A-Z0-9]+", "", bmv_compose_reference_ticker(issuer, instrument.get("serie", "")))
    normalized_listing_ticker = re.sub(r"[^A-Z0-9]+", "", listing_ticker.strip().upper())
    return bool(official_ticker and official_ticker == normalized_listing_ticker)


def select_bmv_market_data_instrument(
    listing_row: dict[str, str],
    hit: dict[str, Any],
    instruments: list[dict[str, str]],
) -> dict[str, str] | None:
    issuer = bmv_search_text(hit.get("cve_emisora")).upper()
    if not issuer:
        return None
    candidates = [
        instrument
        for instrument in instruments
        if bmv_instrument_matches_listing(listing_row.get("ticker", ""), issuer, instrument)
    ]
    if not candidates:
        hit_series = bmv_search_text(hit.get("serie")).upper()
        candidates = [
            instrument
            for instrument in instruments
            if hit_series and normalize_bmv_profile_label(instrument.get("serie", "")) == normalize_bmv_profile_label(hit_series)
        ]
    if len(candidates) != 1:
        return None
    return candidates[0]


def select_bmv_market_data_search_hit(
    source: MasterfileSource,
    listing_row: dict[str, str],
    *,
    access_token: str,
    session: requests.Session,
) -> dict[str, Any] | None:
    if listing_row.get("asset_type") == "ETF":
        terms = bmv_etf_search_terms(listing_row.get("ticker", ""))
        selector = select_bmv_unique_etf_search_hit
    else:
        terms = bmv_stock_search_terms(listing_row.get("ticker", ""))
        selector = select_bmv_unique_stock_search_hit
    for term in terms:
        search_source = MasterfileSource(
            key=source.key,
            provider=source.provider,
            description=source.description,
            source_url=BMV_SEARCH_URL,
            format=source.format,
            reference_scope=source.reference_scope,
        )
        hits = fetch_bmv_search_hits(search_source, term, access_token=access_token, session=session)
        selected = selector(listing_row, hits)
        if selected is not None:
            return selected
    return None


def fetch_bmv_market_data_page(session: requests.Session, url: str) -> str:
    response = session.get(
        url,
        headers={"User-Agent": USER_AGENT, "Referer": BMV_ISSUER_DIRECTORY_URL},
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return response.text


def fetch_bmv_market_data_securities(
    source: MasterfileSource,
    *,
    listings_path: Path = LISTINGS_CSV,
    stock_verification_dir: Path = STOCK_VERIFICATION_DIR,
    etf_verification_dir: Path = ETF_VERIFICATION_DIR,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    target_rows = bmv_market_data_target_rows(
        listings_path=listings_path,
        stock_verification_dir=stock_verification_dir,
        etf_verification_dir=etf_verification_dir,
    )
    if not target_rows:
        return []

    session = session or requests.Session()
    token_response = session.get(BMV_SEARCH_TOKEN_URL, timeout=REQUEST_TIMEOUT)
    token_response.raise_for_status()
    access_token = str(token_response.json().get("response", {}).get("access_token", "")).strip()
    if not access_token:
        raise ValueError("BMV search token missing access_token")

    rows: list[dict[str, str]] = []
    page_cache: dict[tuple[str, str], tuple[list[dict[str, str]], dict[str, str]]] = {}
    for listing_row in target_rows:
        selected_hit = select_bmv_market_data_search_hit(
            source,
            listing_row,
            access_token=access_token,
            session=session,
        )
        if selected_hit is None:
            continue
        issuer = bmv_search_text(selected_hit.get("cve_emisora")).upper()
        issuer_id = bmv_search_text(selected_hit.get("id_empresa"))
        if not issuer or not issuer_id:
            continue

        cache_key = (issuer, issuer_id)
        if cache_key not in page_cache:
            stats_url = BMV_MARKET_DATA_STATS_URL_TEMPLATE.format(issuer=issuer, issuer_id=issuer_id)
            profile_url = BMV_MARKET_DATA_PROFILE_URL_TEMPLATE.format(issuer=issuer, issuer_id=issuer_id)
            try:
                instruments = parse_bmv_market_data_instruments_html(
                    fetch_bmv_market_data_page(session, stats_url)
                )
                profile = parse_bmv_market_data_profile_html(
                    fetch_bmv_market_data_page(session, profile_url)
                )
            except requests.RequestException:
                page_cache[cache_key] = ([], {})
                continue
            page_cache[cache_key] = (instruments, profile)

        instruments, profile = page_cache[cache_key]
        instrument = select_bmv_market_data_instrument(listing_row, selected_hit, instruments)
        if instrument is None:
            continue

        asset_type = listing_row.get("asset_type", "") or "Stock"
        if asset_type == "ETF":
            etf_context = " ".join(
                value
                for value in (
                    profile.get("OBJETO DE INVERSION", ""),
                    bmv_search_text(selected_hit.get("razon_social")),
                    bmv_search_text(selected_hit.get("instrumento")),
                    bmv_search_text(selected_hit.get("descripcion")),
                    listing_row.get("name", ""),
                )
                if value
            )
            sector = normalize_bmv_etf_category(
                profile.get("CLASIFICACION ETF", ""),
                etf_context,
            )
        else:
            sector = normalize_bmv_stock_sector(profile.get("SECTOR", ""))
        name = (
            bmv_search_text(selected_hit.get("razon_social"))
            or bmv_search_text(selected_hit.get("instrumento"))
            or listing_row.get("name", "")
        )
        row = build_bmv_reference_row(
            source,
            listing_row,
            name=name,
            exchange="BMV",
            asset_type=asset_type,
            listing_status="active"
            if normalize_bmv_profile_label(instrument.get("estatus", "")) == "ACTIVA"
            else "suspended",
            isin=instrument.get("isin", ""),
            sector=sector,
        )
        if row.get("isin") or row.get("sector"):
            rows.append(row)
    return rows


def load_bmv_market_data_securities_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    try:
        rows = fetch_bmv_market_data_securities(source, session=session)
    except (requests.RequestException, ValueError, json.JSONDecodeError):
        for path in (BMV_MARKET_DATA_SECURITIES_CACHE, LEGACY_BMV_MARKET_DATA_SECURITIES_CACHE):
            if path.exists():
                return json.loads(path.read_text(encoding="utf-8")), "cache"
        return None, "unavailable"

    ensure_output_dirs()
    BMV_MARKET_DATA_SECURITIES_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def load_bmv_issuer_directory_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    try:
        rows = fetch_bmv_issuer_directory(source, session=session)
    except (requests.RequestException, ValueError, json.JSONDecodeError):
        for path in (BMV_ISSUER_DIRECTORY_CACHE, LEGACY_BMV_ISSUER_DIRECTORY_CACHE):
            if path.exists():
                return json.loads(path.read_text(encoding="utf-8")), "cache"
        return None, "unavailable"

    ensure_output_dirs()
    BMV_ISSUER_DIRECTORY_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


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


def nasdaq_nordic_shares_cache_paths(source_key: str) -> tuple[Path, Path]:
    mapping = {
        "nasdaq_nordic_stockholm_shares": (
            NASDAQ_NORDIC_STOCKHOLM_SHARES_CACHE,
            LEGACY_NASDAQ_NORDIC_STOCKHOLM_SHARES_CACHE,
        ),
        "nasdaq_nordic_helsinki_shares": (
            NASDAQ_NORDIC_HELSINKI_SHARES_CACHE,
            LEGACY_NASDAQ_NORDIC_HELSINKI_SHARES_CACHE,
        ),
        "nasdaq_nordic_iceland_shares": (
            NASDAQ_NORDIC_ICELAND_SHARES_CACHE,
            LEGACY_NASDAQ_NORDIC_ICELAND_SHARES_CACHE,
        ),
        "nasdaq_nordic_copenhagen_shares": (
            NASDAQ_NORDIC_COPENHAGEN_SHARES_CACHE,
            LEGACY_NASDAQ_NORDIC_COPENHAGEN_SHARES_CACHE,
        ),
    }
    return mapping[source_key]


def nasdaq_nordic_share_search_cache_paths(source_key: str) -> tuple[Path, Path]:
    mapping = {
        "nasdaq_nordic_stockholm_shares_search": (
            NASDAQ_NORDIC_STOCKHOLM_SHARES_SEARCH_CACHE,
            LEGACY_NASDAQ_NORDIC_STOCKHOLM_SHARES_SEARCH_CACHE,
        ),
        "nasdaq_nordic_helsinki_shares_search": (
            NASDAQ_NORDIC_HELSINKI_SHARES_SEARCH_CACHE,
            LEGACY_NASDAQ_NORDIC_HELSINKI_SHARES_SEARCH_CACHE,
        ),
        "nasdaq_nordic_copenhagen_shares_search": (
            NASDAQ_NORDIC_COPENHAGEN_SHARES_SEARCH_CACHE,
            LEGACY_NASDAQ_NORDIC_COPENHAGEN_SHARES_SEARCH_CACHE,
        ),
    }
    return mapping[source_key]


def nasdaq_nordic_etf_search_cache_paths(source_key: str) -> tuple[Path, Path]:
    mapping = {
        "nasdaq_nordic_copenhagen_etf_search": (
            NASDAQ_NORDIC_COPENHAGEN_ETF_SEARCH_CACHE,
            LEGACY_NASDAQ_NORDIC_COPENHAGEN_ETF_SEARCH_CACHE,
        ),
    }
    return mapping[source_key]


def spotlight_search_cache_paths(source_key: str) -> tuple[Path, Path]:
    mapping = {
        "spotlight_companies_directory": (
            SPOTLIGHT_COMPANIES_DIRECTORY_CACHE,
            LEGACY_SPOTLIGHT_COMPANIES_DIRECTORY_CACHE,
        ),
        "spotlight_companies_search": (
            SPOTLIGHT_COMPANIES_SEARCH_CACHE,
            LEGACY_SPOTLIGHT_COMPANIES_SEARCH_CACHE,
        ),
    }
    return mapping[source_key]


def ngm_companies_cache_paths(source_key: str) -> tuple[Path, Path]:
    mapping = {
        "ngm_companies_page": (
            NGM_COMPANIES_PAGE_CACHE,
            LEGACY_NGM_COMPANIES_PAGE_CACHE,
        ),
        "ngm_market_data_equities": (
            NGM_MARKET_DATA_EQUITIES_CACHE,
            LEGACY_NGM_MARKET_DATA_EQUITIES_CACHE,
        ),
    }
    return mapping[source_key]


def nasdaq_nordic_etf_cache_paths(source_key: str) -> tuple[Path, Path]:
    mapping = {
        "nasdaq_nordic_stockholm_etfs": (
            NASDAQ_NORDIC_STOCKHOLM_ETFS_CACHE,
            LEGACY_NASDAQ_NORDIC_STOCKHOLM_ETFS_CACHE,
        ),
        "nasdaq_nordic_helsinki_etfs": (
            NASDAQ_NORDIC_HELSINKI_ETFS_CACHE,
            LEGACY_NASDAQ_NORDIC_HELSINKI_ETFS_CACHE,
        ),
        "nasdaq_nordic_copenhagen_etfs": (
            NASDAQ_NORDIC_COPENHAGEN_ETFS_CACHE,
            LEGACY_NASDAQ_NORDIC_COPENHAGEN_ETFS_CACHE,
        ),
    }
    return mapping[source_key]


def load_nasdaq_nordic_stockholm_shares_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    for path in nasdaq_nordic_shares_cache_paths(source.key):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"

    try:
        rows = fetch_nasdaq_nordic_stockholm_shares(source, session=session)
    except requests.RequestException:
        return None, "unavailable"

    ensure_output_dirs()
    nasdaq_nordic_shares_cache_paths(source.key)[0].write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def load_nasdaq_nordic_share_search_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    for path in nasdaq_nordic_share_search_cache_paths(source.key):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"

    try:
        rows = fetch_nasdaq_nordic_share_search(source, session=session)
    except requests.RequestException:
        return None, "unavailable"

    ensure_output_dirs()
    nasdaq_nordic_share_search_cache_paths(source.key)[0].write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def load_nasdaq_nordic_etf_search_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    for path in nasdaq_nordic_etf_search_cache_paths(source.key):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"

    try:
        rows = fetch_nasdaq_nordic_etf_search(source, session=session)
    except requests.RequestException:
        return None, "unavailable"

    ensure_output_dirs()
    nasdaq_nordic_etf_search_cache_paths(source.key)[0].write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def load_spotlight_search_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    for path in spotlight_search_cache_paths(source.key):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"

    try:
        rows = fetch_spotlight_companies_search(source, session=session)
    except requests.RequestException:
        return None, "unavailable"

    ensure_output_dirs()
    spotlight_search_cache_paths(source.key)[0].write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def load_spotlight_directory_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    for path in spotlight_search_cache_paths(source.key):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"

    try:
        rows = fetch_spotlight_companies_directory(source, session=session)
    except requests.RequestException:
        return None, "unavailable"

    ensure_output_dirs()
    spotlight_search_cache_paths(source.key)[0].write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def load_ngm_companies_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    for path in ngm_companies_cache_paths(source.key):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"

    try:
        rows = fetch_ngm_companies_page(source, session=session)
    except (requests.RequestException, ValueError):
        return None, "unavailable"

    ensure_output_dirs()
    ngm_companies_cache_paths(source.key)[0].write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def load_ngm_market_data_equity_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    for path in ngm_companies_cache_paths(source.key):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"

    try:
        rows = fetch_ngm_market_data_equities(source, session=session)
    except (requests.RequestException, ValueError):
        return None, "unavailable"

    ensure_output_dirs()
    ngm_companies_cache_paths(source.key)[0].write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def load_nasdaq_nordic_stockholm_etf_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    for path in nasdaq_nordic_etf_cache_paths(source.key):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"

    try:
        rows = fetch_nasdaq_nordic_stockholm_etfs(source, session=session)
    except requests.RequestException:
        return None, "unavailable"

    ensure_output_dirs()
    nasdaq_nordic_etf_cache_paths(source.key)[0].write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


def load_nasdaq_nordic_stockholm_tracker_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    for path in (
        NASDAQ_NORDIC_STOCKHOLM_TRACKERS_CACHE,
        LEGACY_NASDAQ_NORDIC_STOCKHOLM_TRACKERS_CACHE,
    ):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"

    try:
        rows = fetch_nasdaq_nordic_stockholm_trackers(source, session=session)
    except requests.RequestException:
        return None, "unavailable"

    ensure_output_dirs()
    NASDAQ_NORDIC_STOCKHOLM_TRACKERS_CACHE.write_text(json.dumps(rows), encoding="utf-8")
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


def extract_lse_price_explorer_search(payload: dict[str, Any]) -> dict[str, Any]:
    for component in payload.get("components", []):
        for item in component.get("content", []):
            if item.get("name") == "priceexplorersearch" and isinstance(item.get("value"), dict):
                return item["value"]
    raise ValueError("LSE price explorer payload does not contain priceexplorersearch")


def parse_lse_price_explorer_rows(records: Iterable[dict[str, Any]], source: MasterfileSource) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    seen_tickers: set[str] = set()
    for record in records:
        category = str(record.get("category") or "").strip().upper()
        asset_type = LSE_PRICE_EXPLORER_CATEGORY_TO_ASSET_TYPE.get(category)
        if not asset_type:
            continue
        ticker = str(record.get("tidm") or "").strip().upper()
        if not ticker or ticker in seen_tickers:
            continue
        seen_tickers.add(ticker)

        description = str(record.get("description") or "").strip()
        issuer_name = str(record.get("issuername") or "").strip()
        instrument_name = str(record.get("name") or "").strip()
        if asset_type == "Stock":
            name = issuer_name or description or instrument_name
        else:
            name = description or " ".join(part for part in (issuer_name, instrument_name) if part)
        if not name:
            continue

        row = {
            "source_key": source.key,
            "provider": source.provider,
            "source_url": source.source_url,
            "ticker": ticker,
            "name": name,
            "exchange": "LSE",
            "asset_type": asset_type,
            "listing_status": "active",
            "reference_scope": source.reference_scope,
            "official": "true",
        }
        isin = str(record.get("isin") or "").strip().upper()
        if isin and is_valid_isin(isin):
            row["isin"] = isin
        rows.append(row)
    return rows


def fetch_lse_price_explorer_rows(source: MasterfileSource, session: requests.Session | None = None) -> list[dict[str, str]]:
    session = session or requests.Session()
    categories = ",".join(LSE_PRICE_EXPLORER_CATEGORY_TO_ASSET_TYPE)
    headers = {
        "User-Agent": BROWSER_USER_AGENT,
        "Origin": "https://www.londonstockexchange.com",
        "Referer": LSE_PRICE_EXPLORER_URL,
    }
    rows: list[dict[str, str]] = []
    page = 0
    total_pages = 1
    while page < total_pages:
        parameters = f"categories={categories}&size={LSE_PRICE_EXPLORER_PAGE_SIZE}&page={page}"
        response = session.get(
            LSE_PRICE_EXPLORER_API_URL,
            params={"path": LSE_PRICE_EXPLORER_PAGE_PATH, "parameters": parameters},
            headers=headers,
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        search_payload = extract_lse_price_explorer_search(response.json())
        rows.extend(parse_lse_price_explorer_rows(search_payload.get("content") or [], source))
        total_pages = int(search_payload.get("totalPages") or 1)
        page += 1

    deduped: dict[str, dict[str, str]] = {}
    for row in rows:
        deduped.setdefault(row["ticker"], row)
    return [deduped[ticker] for ticker in sorted(deduped)]


def load_lse_price_explorer_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    for path in (LSE_PRICE_EXPLORER_CACHE, LEGACY_LSE_PRICE_EXPLORER_CACHE):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), "cache"

    try:
        rows = fetch_lse_price_explorer_rows(source, session=session)
    except requests.RequestException:
        return None, "unavailable"

    ensure_output_dirs()
    LSE_PRICE_EXPLORER_CACHE.write_text(json.dumps(rows), encoding="utf-8")
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
    if fallback_asset_type == "ETF":
        return "ETF"
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
    listings_rows = load_csv(listings_path)
    target_tickers = {
        row.get("ticker", "").strip()
        for row in listings_rows
        if row.get("exchange") == "LSE"
        and row.get("asset_type") in {"Stock", "ETF"}
        and row.get("ticker", "").strip()
    }
    missing_isin_tickers = {
        row.get("ticker", "").strip()
        for row in listings_rows
        if row.get("exchange") == "LSE"
        and row.get("asset_type") in {"Stock", "ETF"}
        and row.get("ticker", "").strip()
        and not row.get("isin", "").strip()
    }
    reference_gap_tickers = reference_gap_tickers if reference_gap_tickers is not None else lse_reference_gap_tickers()
    priority_tickers = reference_gap_tickers | missing_isin_tickers
    if priority_tickers:
        target_tickers &= priority_tickers
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


def fetch_jse_instrument_search_exact(
    source: MasterfileSource,
    tickers: Iterable[str],
    session: requests.Session | None = None,
    asset_type_by_ticker: dict[str, str] | None = None,
) -> list[dict[str, str]]:
    session = session or requests.Session()
    asset_type_by_ticker = asset_type_by_ticker or {}
    rows: list[dict[str, str]] = []
    for ticker in tickers:
        query_ticker = ticker.strip().upper()
        if not query_ticker:
            continue
        response = session.get(
            source.source_url,
            params={"keys": query_ticker},
            headers={"User-Agent": USER_AGENT},
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        for instrument_url in extract_jse_instrument_search_links(response.text)[:6]:
            instrument_response = session.get(
                instrument_url,
                headers={"User-Agent": USER_AGENT},
                timeout=REQUEST_TIMEOUT,
            )
            instrument_response.raise_for_status()
            metadata = extract_jse_instrument_metadata(instrument_response.text)
            if metadata is None or metadata.get("code") != query_ticker:
                continue
            row = {
                "source_key": source.key,
                "provider": source.provider,
                "source_url": instrument_url,
                "ticker": query_ticker,
                "name": metadata["name"],
                "exchange": "JSE",
                "asset_type": asset_type_by_ticker.get(query_ticker, "Stock") or "Stock",
                "listing_status": "active",
                "reference_scope": source.reference_scope,
                "official": "true",
            }
            if metadata.get("sector"):
                row["sector"] = metadata["sector"]
            rows.append(row)
            break
    return rows


def load_jse_instrument_search_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    target_tickers = jse_instrument_search_target_tickers()
    asset_type_by_ticker = {
        row.get("ticker", "").strip(): row.get("asset_type", "")
        for row in load_csv(LISTINGS_CSV)
        if row.get("exchange") == "JSE"
        and row.get("asset_type") == "Stock"
        and row.get("ticker", "").strip()
    }

    cached_lookup: dict[str, list[dict[str, str]]] = {}
    for path in (JSE_INSTRUMENT_SEARCH_CACHE, LEGACY_JSE_INSTRUMENT_SEARCH_CACHE):
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
        if ticker not in cached_lookup or not cached_lookup.get(ticker)
    ]
    used_network = False
    if missing_tickers:
        fetched_lookup = {ticker: [] for ticker in missing_tickers}
        successful_fetch = False
        max_workers = min(JSE_INSTRUMENT_SEARCH_MAX_WORKERS, len(missing_tickers))
        if max_workers <= 1:
            for ticker in missing_tickers:
                try:
                    fetched_lookup[ticker] = fetch_jse_instrument_search_exact(
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
                        fetch_jse_instrument_search_exact,
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
        JSE_INSTRUMENT_SEARCH_CACHE.write_text(json.dumps(cached_lookup), encoding="utf-8")
        used_network = successful_fetch

    rows: list[dict[str, str]] = []
    for ticker in sorted(cached_lookup):
        rows.extend(cached_lookup.get(ticker, []))
    return rows, "network" if used_network else "cache"


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
    for ticker, rows_for_ticker in cached_lookup.items():
        if asset_type_by_ticker.get(ticker) != "ETF":
            continue
        for row in rows_for_ticker:
            if row.get("asset_type") != "ETF":
                row["asset_type"] = "ETF"
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
        asset_type = infer_asset_type(name)
        output_row = {
            "source_key": source.key,
            "provider": source.provider,
            "source_url": source.source_url,
            "ticker": ticker,
            "name": name,
            "exchange": "ASX",
            "asset_type": asset_type,
            "listing_status": "active",
            "reference_scope": source.reference_scope,
            "official": "true",
        }
        if asset_type == "Stock":
            sector = ASX_GICS_INDUSTRY_GROUP_SECTOR_MAP.get((row.get("GICS industry group") or "").strip(), "")
            if sector:
                output_row["sector"] = sector
        rows.append(output_row)
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


def parse_cboe_canada_listing_directory_payload(
    payload: dict[str, Any] | list[dict[str, Any]],
    source: MasterfileSource,
) -> list[dict[str, str]]:
    if isinstance(payload, dict):
        records = payload.get("data") or []
    elif isinstance(payload, list):
        records = payload
    else:
        return []
    rows: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    for record in records:
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

        tickers = [ticker]
        if "." in ticker:
            tickers.append(ticker.replace(".", "-"))

        for candidate in tickers:
            signature = (candidate, name, asset_type)
            if signature in seen:
                continue
            seen.add(signature)
            rows.append(
                {
                    "source_key": source.key,
                    "provider": source.provider,
                    "source_url": source.source_url,
                    "ticker": candidate,
                    "name": name,
                    "exchange": "NEO",
                    "asset_type": asset_type,
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
    return parse_cboe_canada_listing_directory_payload(payload, source)


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


def find_javascript_block_end(text: str, start_index: int, *, open_char: str, close_char: str) -> int:
    depth = 0
    in_string = False
    string_char = ""
    escaped = False
    for index in range(start_index, len(text)):
        character = text[index]
        if in_string:
            if escaped:
                escaped = False
                continue
            if character == "\\":
                escaped = True
                continue
            if character == string_char:
                in_string = False
            continue
        if character in {'"', "'"}:
            in_string = True
            string_char = character
            continue
        if character == open_char:
            depth += 1
            continue
        if character == close_char:
            depth -= 1
            if depth == 0:
                return index
    raise ValueError(f"Unterminated JavaScript block starting at {start_index}")


def split_top_level_javascript_items(text: str) -> list[str]:
    items: list[str] = []
    start = 0
    bracket_depth = 0
    brace_depth = 0
    paren_depth = 0
    in_string = False
    string_char = ""
    escaped = False
    for index, character in enumerate(text):
        if in_string:
            if escaped:
                escaped = False
                continue
            if character == "\\":
                escaped = True
                continue
            if character == string_char:
                in_string = False
            continue
        if character in {'"', "'"}:
            in_string = True
            string_char = character
            continue
        if character == "[":
            bracket_depth += 1
            continue
        if character == "]":
            bracket_depth -= 1
            continue
        if character == "{":
            brace_depth += 1
            continue
        if character == "}":
            brace_depth -= 1
            continue
        if character == "(":
            paren_depth += 1
            continue
        if character == ")":
            paren_depth -= 1
            continue
        if character == "," and bracket_depth == 0 and brace_depth == 0 and paren_depth == 0:
            item = text[start:index].strip()
            if item:
                items.append(item)
            start = index + 1
    tail = text[start:].strip()
    if tail:
        items.append(tail)
    return items


def parse_compact_javascript_literal(
    token: str,
    identifier_values: dict[str, Any] | None = None,
) -> Any:
    token = token.strip()
    if not token:
        return ""
    if identifier_values and token in identifier_values:
        return identifier_values[token]
    if token in {"!0", "true"}:
        return True
    if token in {"!1", "false"}:
        return False
    if token in {"null", "void 0", "undefined"}:
        return None
    if token.startswith('"'):
        return json.loads(token)
    if re.fullmatch(r"-?\d+", token):
        return int(token)
    if re.fullmatch(r"-?\d+\.\d+(?:[eE][+-]?\d+)?", token):
        return float(token)
    if JS_IDENTIFIER_RE.fullmatch(token):
        return token
    return token


def extract_set_search_option_payload(text: str) -> tuple[str, dict[str, Any]]:
    prefix_index = text.find(SET_NUXT_PREFIX)
    if prefix_index == -1:
        raise ValueError("SET Nuxt payload not found")

    params_start = prefix_index + len(SET_NUXT_PREFIX)
    params_end = text.find(SET_NUXT_FUNCTION_BODY_MARKER, params_start)
    if params_end == -1:
        raise ValueError("SET Nuxt function body marker not found")

    parameter_names = [name.strip() for name in text[params_start:params_end].split(",")]
    return_index = text.find(SET_NUXT_RETURN_MARKER, params_end)
    if return_index == -1:
        return_index = text.find("return ", params_end)
    if return_index == -1:
        raise ValueError("SET Nuxt return marker not found")
    body_start = text.find("{", return_index)
    if body_start == -1:
        raise ValueError("SET Nuxt body not found")
    body_end = find_javascript_block_end(text, body_start, open_char="{", close_char="}")
    body = text[body_start : body_end + 1]

    invocation_start = text.find("(", body_end)
    if invocation_start == -1:
        raise ValueError("SET Nuxt invocation args not found")
    invocation_end = find_javascript_block_end(text, invocation_start, open_char="(", close_char=")")
    argument_tokens = split_top_level_javascript_items(text[invocation_start + 1 : invocation_end])
    if len(argument_tokens) < len(parameter_names):
        raise ValueError("SET Nuxt invocation args truncated")

    identifier_values = {
        name: parse_compact_javascript_literal(token)
        for name, token in zip(parameter_names, argument_tokens)
    }
    return body, identifier_values


def parse_set_quote_search_rows(
    text: str,
    source: MasterfileSource,
    *,
    security_type_name: str,
    asset_type: str,
) -> list[dict[str, str]]:
    body, identifier_values = extract_set_search_option_payload(text)
    marker_index = body.find(SET_NUXT_SEARCH_OPTION_MARKER)
    if marker_index == -1:
        return []
    array_start = marker_index + len(SET_NUXT_SEARCH_OPTION_MARKER)
    array_end = body.find(SET_NUXT_SEARCH_OPTION_END_MARKER, array_start)
    if array_end == -1:
        raise ValueError("SET searchOption array terminator missing")

    rows: list[dict[str, str]] = []
    seen_tickers: set[str] = set()
    for token in split_top_level_javascript_items(body[array_start:array_end]):
        token = token.strip()
        if not token.startswith("{") or not token.endswith("}"):
            continue
        values: dict[str, Any] = {}
        for field in split_top_level_javascript_items(token[1:-1]):
            if ":" not in field:
                continue
            key, raw_value = field.split(":", 1)
            values[key.strip()] = parse_compact_javascript_literal(raw_value, identifier_values)

        ticker = str(values.get("symbol") or "").strip().upper()
        market = str(values.get("market") or "").strip().upper()
        record_security_type_name = str(values.get("securityTypeName") or "").strip().upper()
        if market not in {"SET", "MAI"} or record_security_type_name != security_type_name.upper() or not ticker:
            continue

        name_en = str(values.get("nameEN") or "").strip()
        name_th = str(values.get("nameTH") or "").strip()
        name = name_en or name_th
        if not name or ticker in seen_tickers:
            continue
        seen_tickers.add(ticker)
        rows.append(
            {
                "source_key": source.key,
                "provider": source.provider,
                "source_url": source.source_url,
                "ticker": ticker,
                "name": name,
                "exchange": "SET",
                "asset_type": asset_type,
                "listing_status": "active",
                "reference_scope": source.reference_scope,
                "official": "true",
            }
        )
    return rows


def parse_set_quote_search_payload(text: str, source: MasterfileSource) -> list[dict[str, str]]:
    return parse_set_quote_search_rows(text, source, security_type_name="ETF", asset_type="ETF")


def parse_set_dr_search_payload(text: str, source: MasterfileSource) -> list[dict[str, str]]:
    return parse_set_quote_search_rows(text, source, security_type_name="DR", asset_type="Stock")


def parse_set_stock_search_payload(payload: dict[str, Any], source: MasterfileSource) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    seen_tickers: set[str] = set()
    for item in payload.get("securitySymbols", []):
        if not isinstance(item, dict):
            continue
        security_type = str(item.get("securityType") or "").strip().upper()
        asset_type = SET_STOCK_SEARCH_SECURITY_TYPES.get(security_type)
        if asset_type is None:
            continue

        ticker = str(item.get("symbol") or "").strip().upper()
        market = str(item.get("market") or "").strip().upper()
        name = str(item.get("nameEN") or "").strip() or str(item.get("nameTH") or "").strip()
        if market not in {"SET", "MAI"} or not ticker or not name or ticker in seen_tickers:
            continue
        seen_tickers.add(ticker)

        row = {
            "source_key": source.key,
            "provider": source.provider,
            "source_url": source.source_url,
            "ticker": ticker,
            "name": name,
            "exchange": "SET",
            "asset_type": asset_type,
            "listing_status": "active",
            "reference_scope": source.reference_scope,
            "official": "true",
        }
        sector = SET_STOCK_SEARCH_SECTOR_MAP.get(
            str(item.get("sector") or "").strip().upper()
        ) or SET_STOCK_SEARCH_SECTOR_MAP.get(str(item.get("industry") or "").strip().upper())
        if sector:
            row["sector"] = sector
        rows.append(row)
    return rows


def parse_set_listed_companies_html(text: str, source: MasterfileSource) -> list[dict[str, str]]:
    parser = _TableParser()
    parser.feed(text)
    rows: list[dict[str, str]] = []
    allowed_markets = {"set", "mai"}
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
            if (
                market.lower() not in allowed_markets
                or not ticker
                or not name
                or ticker.lower() == "nan"
                or name.lower() == "nan"
            ):
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
        name = clean_html_text(str(record.get("eng_cor_nm", "")))
        if not ticker or not name or ticker.lower() == "nan" or name.lower() == "nan":
            continue
        row = {
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
        sector = normalize_krx_stock_sector(record.get("ind_nm"), record.get("std_ind_cd"))
        if sector:
            row["sector"] = sector
        rows.append(row)
    return rows


def normalize_krx_stock_sector(industry_name: Any, industry_code: Any = "") -> str:
    industry = clean_html_text(str(industry_name or "")).lower()
    for sector, keywords in KRX_INDUSTRY_KEYWORD_SECTOR_RULES:
        if any(keyword in industry for keyword in keywords):
            return sector
    industry_prefix = str(industry_code or "").strip()[:2]
    return KRX_STD_INDUSTRY_PREFIX_SECTOR_MAP.get(industry_prefix, "")


def normalize_krx_finder_isin(value: Any) -> str:
    normalized = str(value or "").strip().upper()
    if len(normalized) != 12 or not normalized.isalnum():
        return ""
    return normalized


def parse_krx_stock_finder_records(
    payload: list[dict[str, Any]],
    source: MasterfileSource,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for record in payload:
        ticker = str(record.get("short_code", "")).strip()
        name = str(record.get("codeName", "")).strip()
        exchange = KRX_MARKET_ENGNAME_TO_EXCHANGE.get(str(record.get("marketEngName", "")).strip().upper(), "")
        if not ticker or not name or not exchange:
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
                "isin": normalize_krx_finder_isin(record.get("full_code", "")),
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


def parse_krx_product_finder_records(
    payload: list[dict[str, Any]],
    source: MasterfileSource,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for record in payload:
        ticker = str(record.get("short_code", "")).strip()
        name = str(record.get("codeName", "")).strip()
        if not ticker or not name:
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
                "isin": normalize_krx_finder_isin(record.get("full_code", "")),
            }
        )
    return rows


def infer_jpx_asset_type(section: str, name: str) -> str:
    normalized = section.strip().lower()
    if "etf" in normalized or "etn" in normalized:
        return "ETF"
    return infer_asset_type(name)


JPX_33_INDUSTRY_TO_SECTOR_EN: dict[str, str] = {
    "Air Transportation": "Industrials",
    "Banks": "Financials",
    "Chemicals": "Materials",
    "Construction": "Industrials",
    "Electric Appliances": "Information Technology",
    "Electric Power and Gas": "Utilities",
    "Fishery, Agriculture and Forestry": "Consumer Staples",
    "Foods": "Consumer Staples",
    "Glass and Ceramics Products": "Materials",
    "Information & Communication": "Communication Services",
    "Insurance": "Financials",
    "Iron and Steel": "Materials",
    "Land Transportation": "Industrials",
    "Machinery": "Industrials",
    "Marine Transportation": "Industrials",
    "Metal Products": "Industrials",
    "Mining": "Materials",
    "Nonferrous Metals": "Materials",
    "Oil and Coal Products": "Energy",
    "Other Financing Business": "Financials",
    "Other Products": "Consumer Discretionary",
    "Pharmaceutical": "Health Care",
    "Precision Instruments": "Health Care",
    "Pulp and Paper": "Materials",
    "Real Estate": "Real Estate",
    "Retail Trade": "Consumer Discretionary",
    "Rubber Products": "Materials",
    "Securities and Commodities Futures": "Financials",
    "Services": "Industrials",
    "Textiles and Apparels": "Consumer Discretionary",
    "Transportation Equipment": "Consumer Discretionary",
    "Warehousing and Harbor Transportation Service": "Industrials",
    "Wholesale Trade": "Industrials",
}


def normalize_jpx_33_industry_sector(value: str, asset_type: str) -> str:
    mapped = JPX_33_INDUSTRY_TO_SECTOR_EN.get(value.strip(), "")
    return mapped or normalize_sector(value, asset_type)


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


def jpx_request_headers(referer: str | None = None) -> dict[str, str]:
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json,text/plain,*/*",
        "Connection": "close",
    }
    if referer:
        headers["Referer"] = referer
    return headers


def jpx_tse_stock_detail_target_rows(listings_path: Path = LISTINGS_CSV) -> list[dict[str, str]]:
    rows = [
        row
        for row in load_csv(listings_path)
        if row.get("exchange") == "TSE"
        and row.get("asset_type") in {"Stock", "ETF"}
        and row.get("ticker", "").strip()
    ]
    deduped: dict[str, dict[str, str]] = {}
    for row in rows:
        deduped.setdefault(row["ticker"].strip(), row)
    return [deduped[ticker] for ticker in sorted(deduped)]


def parse_jpx_tse_stock_detail_payload(
    payload: dict[str, Any],
    source: MasterfileSource,
    *,
    fallback_ticker: str = "",
    fallback_asset_type: str = "",
) -> dict[str, str] | None:
    section = payload.get("section1", {})
    data = section.get("data", {})
    if not isinstance(data, dict) or not data:
        return None
    detail = next(iter(data.values()))
    if not isinstance(detail, dict):
        return None
    ticker = str(detail.get("TTCODE2") or fallback_ticker).strip()
    isin = str(detail.get("ISIN") or "").strip().upper()
    if not ticker or not is_valid_isin(isin):
        return None
    name = str(detail.get("FLLNE") or detail.get("NAMEE") or detail.get("FLLN") or detail.get("NAME") or "").strip()
    asset_type = fallback_asset_type or infer_jpx_asset_type(
        " ".join(
            str(detail.get(key) or "")
            for key in ("LISSE_CNV", "LISS_CNV", "PSTSE", "PSTS")
        ),
        name,
    )
    row = {
        "source_key": source.key,
        "provider": source.provider,
        "source_url": f"{source.source_url}{quote(ticker)}",
        "ticker": ticker,
        "name": name,
        "exchange": "TSE",
        "asset_type": asset_type,
        "listing_status": "active",
        "reference_scope": source.reference_scope,
        "official": "true",
        "isin": isin,
    }
    sector = normalize_jpx_33_industry_sector(str(detail.get("JSECE_CNV") or detail.get("JSEC_CNV") or ""), asset_type)
    if sector:
        row["sector"] = sector
    return row


def fetch_jpx_tse_stock_detail_payload(
    ticker: str,
    session: requests.Session | None = None,
) -> dict[str, Any]:
    url = f"{JPX_STOCK_DETAIL_API_URL}{quote(ticker)}"
    referer = f"{JPX_STOCK_DETAIL_PAGE_URL}{quote(ticker)}"
    headers = jpx_request_headers(referer)
    if session is None:
        curl = shutil.which("curl")
        if curl:
            command = [
                curl,
                "--fail",
                "--silent",
                "--show-error",
                "--location",
                "--http1.1",
                "--no-keepalive",
                "--connect-timeout",
                "2",
                "--max-time",
                str(JPX_STOCK_DETAIL_TIMEOUT),
            ]
            for key, value in headers.items():
                command.extend(["--header", f"{key}: {value}"])
            command.append(url)
            result = subprocess.run(
                command,
                check=True,
                capture_output=True,
                text=True,
                timeout=JPX_STOCK_DETAIL_TIMEOUT + 2,
            )
            return json.loads(result.stdout)

        request = Request(url, headers=headers)
        with urlopen(request, timeout=JPX_STOCK_DETAIL_TIMEOUT) as response:
            return json.loads(response.read().decode("utf-8"))

    response = session.get(url, headers=headers, timeout=JPX_STOCK_DETAIL_TIMEOUT)
    try:
        response.raise_for_status()
        return response.json()
    finally:
        close = getattr(response, "close", None)
        if close is not None:
            close()


def fetch_jpx_tse_stock_detail_rows(
    source: MasterfileSource,
    *,
    listings_path: Path = LISTINGS_CSV,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    if session is not None:
        session.get(JPX_STOCK_SEARCH_URL, headers=jpx_request_headers(), timeout=REQUEST_TIMEOUT).raise_for_status()
    targets = jpx_tse_stock_detail_target_rows(listings_path)
    rows_by_ticker: dict[str, dict[str, str]] = {}

    if session is None:
        for cache_path in (
            JPX_TSE_STOCK_DETAIL_CACHE,
            JPX_TSE_STOCK_DETAIL_PARTIAL_CACHE,
            LEGACY_JPX_TSE_STOCK_DETAIL_CACHE,
        ):
            if not cache_path.exists():
                continue
            try:
                cached_rows = json.loads(cache_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if isinstance(cached_rows, list):
                for cached_row in cached_rows:
                    if isinstance(cached_row, dict) and cached_row.get("ticker") and cached_row.get("isin"):
                        rows_by_ticker[str(cached_row["ticker"])] = dict(cached_row)

    def fetch_target(listing_row: dict[str, str]) -> dict[str, str] | None:
        payload = fetch_jpx_tse_stock_detail_payload(listing_row["ticker"], session=session)
        row = parse_jpx_tse_stock_detail_payload(
            payload,
            source,
            fallback_ticker=listing_row["ticker"],
            fallback_asset_type=listing_row.get("asset_type", ""),
        )
        return row

    def write_partial_cache() -> None:
        if session is not None:
            return
        ensure_output_dirs()
        partial_rows = [rows_by_ticker[ticker] for ticker in sorted(rows_by_ticker)]
        JPX_TSE_STOCK_DETAIL_PARTIAL_CACHE.write_text(json.dumps(partial_rows), encoding="utf-8")

    remaining_targets = [row for row in targets if row["ticker"].strip() not in rows_by_ticker]
    if session is None:
        print(
            f"JPX/TSE stock detail: cached {len(rows_by_ticker)}/{len(targets)} rows; "
            f"fetching {len(remaining_targets)} remaining rows",
            file=sys.stderr,
        )
        completed = 0
        with ThreadPoolExecutor(max_workers=JPX_STOCK_DETAIL_WORKERS) as executor:
            future_rows = {executor.submit(fetch_target, row): row for row in remaining_targets}
            for future in as_completed(future_rows):
                completed += 1
                try:
                    row = future.result()
                except (OSError, ValueError, json.JSONDecodeError, subprocess.SubprocessError):
                    continue
                if row is not None:
                    rows_by_ticker[row["ticker"]] = row
                if completed % 100 == 0:
                    write_partial_cache()
                    print(
                        f"JPX/TSE stock detail: {len(rows_by_ticker)}/{len(targets)} rows cached",
                        file=sys.stderr,
                    )
        write_partial_cache()
    else:
        for listing_row in targets:
            row = fetch_target(listing_row)
            if row is not None:
                rows_by_ticker[row["ticker"]] = row

    rows = [rows_by_ticker[ticker] for ticker in sorted(rows_by_ticker)]
    if targets and not rows:
        raise ValueError("JPX/TSE stock detail API returned no usable ISIN rows")
    return rows


def load_jpx_tse_stock_detail_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    try:
        rows = fetch_jpx_tse_stock_detail_rows(source, session=session)
    except (requests.RequestException, ValueError, json.JSONDecodeError, subprocess.SubprocessError):
        for path in (JPX_TSE_STOCK_DETAIL_CACHE, LEGACY_JPX_TSE_STOCK_DETAIL_CACHE):
            if path.exists():
                return json.loads(path.read_text(encoding="utf-8")), "cache"
        return None, "unavailable"

    ensure_output_dirs()
    JPX_TSE_STOCK_DETAIL_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    if JPX_TSE_STOCK_DETAIL_PARTIAL_CACHE.exists():
        JPX_TSE_STOCK_DETAIL_PARTIAL_CACHE.unlink()
    return rows, "network"


def parse_jse_exchange_traded_product_excel(
    content: bytes,
    source: MasterfileSource,
    *,
    source_url: str,
) -> list[dict[str, str]]:
    dataframe = pd.read_excel(io.BytesIO(content), sheet_name=0)
    rows: list[dict[str, str]] = []
    current_section = ""
    for record in dataframe.to_dict(orient="records"):
        ticker = str(record.get("Alpha", "")).strip()
        name = re.sub(r"\s+", " ", str(record.get("Long Name", "")).strip())
        if ticker and ticker.lower() != "nan" and (not name or name.lower() == "nan"):
            if ticker in JSE_EXCHANGE_TRADED_PRODUCT_CATEGORY_MAP or ticker == "Actively Managed":
                current_section = ticker
            continue
        if not ticker or not name or ticker.lower() == "nan" or name.lower() == "nan":
            continue
        row = {
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
        category = normalize_jse_exchange_traded_product_category(
            current_section,
            str(record.get("Underlying", "")).strip(),
            str(record.get("Description", "")).strip(),
        )
        if category:
            row["sector"] = category
        rows.append(row)
    return rows


def normalize_jse_exchange_traded_product_category(
    section: str,
    underlying: str,
    description: str = "",
) -> str:
    mapped = JSE_EXCHANGE_TRADED_PRODUCT_CATEGORY_MAP.get(section)
    if mapped:
        return mapped
    if section != "Actively Managed":
        return ""
    normalized = ascii_fold(f" {underlying} {description} ").lower()
    if "property" in normalized or "reit" in normalized:
        return "Real Estate"
    if "multi-asset" in normalized or "multi asset" in normalized or "balanced" in normalized:
        return "Multi-Asset"
    if "equity" in normalized:
        return "Equity"
    if (
        "fixed income" in normalized
        or "global income" in normalized
        or "short-term deposit" in normalized
        or "short term deposit" in normalized
        or "investment grade" in normalized
        or "floating rate" in normalized
    ):
        return "Fixed Income"
    return ""


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
                    "isin": isin,
                }
            )
    return rows


def parse_deutsche_boerse_etfs_etps_excel(content: bytes, source: MasterfileSource) -> list[dict[str, str]]:
    dataframe = pd.read_excel(io.BytesIO(content), sheet_name="ETFs & ETPs", header=8)
    rows: list[dict[str, str]] = []
    for record in dataframe.to_dict(orient="records"):
        ticker = str(record.get("XETRA SYMBOL", "")).strip()
        name = str(record.get("PRODUCT NAME", "")).strip()
        isin = str(record.get("ISIN", "")).strip().upper()
        product_type = str(record.get("PRODUCT TYPE", "")).strip().upper()
        if not ticker or not name or ticker.lower() == "nan" or name.lower() == "nan":
            continue
        row = {
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
        if is_valid_isin(isin):
            row["isin"] = isin
        category = DEUTSCHE_BOERSE_ETP_PRODUCT_TYPE_CATEGORY_MAP.get(product_type)
        if category:
            row["sector"] = category
        rows.append(row)
    return rows


def normalize_deutsche_boerse_xetra_etp_category(instrument_type: str, product_group_description: str) -> str:
    normalized_type = instrument_type.strip().upper()
    if normalized_type in {"ETC", "ETN"}:
        return DEUTSCHE_BOERSE_ETP_PRODUCT_TYPE_CATEGORY_MAP[normalized_type]

    normalized_group = re.sub(r"\s+", " ", product_group_description.strip().upper())
    if "RENTEN" in normalized_group or "BOND" in normalized_group:
        return "Fixed Income"
    if "COMMODIT" in normalized_group:
        return "Commodity"
    return ""


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
        instrument_type = str(record.get("Instrument Type", "")).strip().upper()
        asset_type = DEUTSCHE_BOERSE_XETRA_INSTRUMENT_TYPE_ASSET_TYPE.get(instrument_type, "")
        if not asset_type:
            continue
        ticker = str(record.get("Mnemonic", "")).strip()
        name = str(record.get("Instrument", "")).strip()
        isin = str(record.get("ISIN", "")).strip().upper()
        if not ticker or not name or ticker.lower() == "nan" or name.lower() == "nan":
            continue

        row = {
            "source_key": source.key,
            "provider": source.provider,
            "source_url": source.source_url,
            "ticker": ticker,
            "name": name,
            "exchange": "XETRA",
            "asset_type": asset_type,
            "listing_status": "active",
            "reference_scope": source.reference_scope,
            "official": "true",
        }
        if is_valid_isin(isin):
            row["isin"] = isin
        if asset_type == "ETF":
            category = normalize_deutsche_boerse_xetra_etp_category(
                instrument_type,
                str(record.get("Product Assignment Group Description", "")),
            )
            if category:
                row["sector"] = category
        rows.append(row)
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
            asset_type = infer_tmx_listed_asset_type(name, sector)
            rows.append(
                {
                    "source_key": source.key,
                    "provider": source.provider,
                    "source_url": source.source_url,
                    "ticker": ticker,
                    "name": name,
                    "exchange": exchange,
                    "asset_type": asset_type,
                    "listing_status": "active",
                    "reference_scope": "exchange_directory" if asset_type == "Stock" else source.reference_scope,
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
                "isin": isin,
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
                "isin": isin,
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
                "reference_scope": sec_reference_scope(exchange),
                "official": "true",
            }
        )
    return rows


def isin_check_digit(body: str) -> str:
    digits = ""
    for char in body:
        digits += char if char.isdigit() else str(ord(char) - 55)
    total = 0
    for index, digit in enumerate(reversed(digits)):
        value = int(digit)
        if index % 2 == 0:
            value *= 2
            if value > 9:
                value -= 9
        total += value
    return str((10 - (total % 10)) % 10)


def is_taiwan_domestic_registration(foreign_registration_country: str) -> bool:
    normalized = re.sub(r"\s+", "", foreign_registration_country.strip())
    return normalized in TAIWAN_DOMESTIC_REGISTRATION_MARKERS


def derive_taiwan_isin(
    ticker: str,
    *,
    emerging_board: bool = False,
    foreign_registration_country: str = "",
) -> str:
    normalized_ticker = ticker.strip().upper()
    if not TAIWAN_ISIN_TICKER_RE.fullmatch(normalized_ticker):
        return ""
    if foreign_registration_country and not is_taiwan_domestic_registration(foreign_registration_country):
        return ""

    if emerging_board and re.fullmatch(r"\d{4}", normalized_ticker):
        body = f"TW000{normalized_ticker}B1"
    else:
        body = f"TW000{normalized_ticker}"
        if len(body) > 11:
            return ""
        body = body.ljust(11, "0")

    isin = f"{body}{isin_check_digit(body)}"
    return isin if is_valid_isin(isin) else ""


def normalize_taiwan_stock_sector(industry_code: Any) -> str:
    code = str(industry_code or "").strip().zfill(2)
    return normalize_sector(TAIWAN_INDUSTRY_CODE_SECTOR_MAP.get(code, ""), "Stock")


def normalize_tpex_etf_category(record: dict[str, Any]) -> str:
    ticker = str(record.get("stockNo") or "").strip().upper()
    haystack = " ".join(
        str(record.get(key) or "")
        for key in ("stockName", "indexName")
    ).lower()
    if ticker.endswith("B") or any(
        marker in haystack
        for marker in (
            "bond",
            "corporate",
            "treasury",
            "worldbig",
            "fixed income",
            "income active",
        )
    ):
        return "Fixed Income"
    if "balanced" in haystack:
        return "Multi-Asset"
    if any(
        marker in haystack
        for marker in (
            "s&p 500",
            "stoxx",
            "equity",
            "big tech",
            "tech",
            "5g",
            "commercial and wholesale trade",
            "japan",
            "usa",
            "u.s.",
            "china",
        )
    ):
        return "Equity"
    return ""


def derive_isin_from_otc_markets_cusip(
    cusip: str,
    *,
    issuer_country: str,
    type_name: str,
    is_adr: bool,
) -> str:
    cleaned_cusip = re.sub(r"[^A-Z0-9]", "", cusip.upper())
    if not CUSIP_RE.fullmatch(cleaned_cusip):
        return ""

    normalized_country = issuer_country.strip().upper()
    normalized_type_name = type_name.strip().lower()
    if is_adr or normalized_type_name in OTC_MARKETS_ADR_TYPE_NAMES:
        prefix = "US"
    elif normalized_country in {"US", "USA", "UNITED STATES"}:
        prefix = "US"
    elif normalized_country in {"CA", "CAN", "CANADA"}:
        prefix = "CA"
    else:
        return ""

    body = f"{prefix}{cleaned_cusip}"
    isin = f"{body}{isin_check_digit(body)}"
    return isin if is_valid_isin(isin) else ""


def select_otc_markets_security(profile: dict[str, Any], symbol: str) -> dict[str, Any]:
    normalized_symbol = symbol.strip().upper()
    securities = profile.get("securities") or []
    return next(
        (
            security
            for security in securities
            if str(security.get("symbol", "")).strip().upper() == normalized_symbol
        ),
        {},
    )


def parse_otc_markets_security_profile(
    profile: dict[str, Any],
    source: MasterfileSource,
    listing_row: dict[str, str],
) -> list[dict[str, str]]:
    ticker = listing_row.get("ticker", "").strip().upper()
    if not ticker:
        return []
    security = select_otc_markets_security(profile, ticker)
    if not security:
        return []

    issuer_country = str(profile.get("countryId") or profile.get("country") or "")
    type_name = str(security.get("typeName") or "")
    isin = derive_isin_from_otc_markets_cusip(
        str(security.get("cusip") or ""),
        issuer_country=issuer_country,
        type_name=type_name,
        is_adr=bool(security.get("isAdr")),
    )
    name = str(profile.get("name") or security.get("issueName") or listing_row.get("name") or "").strip()
    listing_status = "active"
    status_name = str(security.get("statusName") or "").strip().lower()
    if profile.get("deregistered") or security.get("isTest") or status_name in {"inactive", "deleted"}:
        listing_status = "inactive"

    return [
        {
            "source_key": source.key,
            "provider": source.provider,
            "source_url": OTC_MARKETS_SECURITY_PROFILE_PAGE_URL.format(symbol=ticker),
            "ticker": ticker,
            "name": name,
            "exchange": "OTC",
            "asset_type": listing_row.get("asset_type", "Stock") or "Stock",
            "listing_status": listing_status,
            "reference_scope": source.reference_scope,
            "official": "true",
            "isin": isin,
        }
    ]


def parse_twse_listed_companies(payload: list[dict[str, Any]], source: MasterfileSource) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for record in payload:
        ticker = str(record.get("公司代號", "")).strip()
        name = str(record.get("公司名稱", "")).strip()
        if not ticker or not name:
            continue
        row = {
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
        isin = derive_taiwan_isin(
            ticker,
            foreign_registration_country=str(record.get("外國企業註冊地國", "")),
        )
        if isin:
            row["isin"] = isin
        rows.append(row)
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
        row = {
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
        isin = derive_taiwan_isin(ticker)
        if isin:
            row["isin"] = isin
        category = normalize_tpex_etf_category(record)
        if category:
            row["sector"] = category
        rows.append(row)
    return rows


def parse_sse_jsonp(text: str) -> dict[str, Any]:
    match = SSE_JSONP_RE.match(text.strip())
    if not match:
        raise ValueError("Invalid SSE JSONP payload")
    return json.loads(match.group(1))


def normalize_china_csrc_sector(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    code = text[0].upper()
    return CHINA_CSRC_SECTOR_MAP.get(code, "")


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
        row = {
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
        sector = normalize_china_csrc_sector(record.get("CSRC_CODE") or record.get("CSRC_CODE_DESC"))
        if sector:
            row["sector"] = sector
        rows.append(row)
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
            row = {
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
            sector = normalize_china_csrc_sector(record.get("sshymc"))
            if sector:
                row["sector"] = sector
            rows.append(row)
    return rows


def parse_szse_b_share_list(payload: dict[str, Any] | list[dict[str, Any]], source: MasterfileSource) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for report in extract_szse_report_sections(payload):
        for record in report.get("data", []) or []:
            ticker = str(record.get("bgdm", "")).strip()
            name = strip_html_tags(str(record.get("bgjc", "")).strip())
            if not ticker or not name:
                continue
            row = {
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
            sector = normalize_china_csrc_sector(record.get("sshymc"))
            if sector:
                row["sector"] = sector
            rows.append(row)
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
        row = {
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
        sector = normalize_china_csrc_sector(record.get("所属行业"))
        if sector:
            row["sector"] = sector
        rows.append(row)
    return rows


def parse_szse_b_share_workbook(content: bytes, source: MasterfileSource) -> list[dict[str, str]]:
    dataframe = pd.read_excel(io.BytesIO(content), sheet_name=0)
    rows: list[dict[str, str]] = []
    for record in dataframe.to_dict(orient="records"):
        ticker_value = record.get("B股代码")
        name_value = record.get("公司全称") or record.get("B股简称")
        if pd.isna(ticker_value) or pd.isna(name_value):
            continue
        ticker = str(ticker_value).strip()
        if ticker.endswith(".0"):
            ticker = ticker[:-2]
        ticker = ticker.zfill(6)
        name = str(name_value).strip()
        if not ticker or not name:
            continue
        row = {
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
        sector = normalize_china_csrc_sector(record.get("所属行业"))
        if sector:
            row["sector"] = sector
        rows.append(row)
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


def fetch_szse_b_share_list(source: MasterfileSource, session: requests.Session | None = None) -> list[dict[str, str]]:
    session = session or requests.Session()
    headers = {
        "User-Agent": USER_AGENT,
        "Referer": source.source_url,
        "Connection": "close",
    }
    try:
        session.get(
            source.source_url,
            headers={"User-Agent": USER_AGENT, "Connection": "close"},
            timeout=REQUEST_TIMEOUT,
        )
    except requests.RequestException:
        pass

    workbook_params = {
        "SHOWTYPE": "xlsx",
        "CATALOGID": SZSE_A_SHARE_CATALOG_ID,
        "TABKEY": SZSE_B_SHARE_TAB_KEY,
        "PAGENO": 1,
        "random": "0.001",
    }
    for _attempt in range(3):
        try:
            response = session.get(
                "https://www.szse.cn/api/report/ShowReport",
                params=workbook_params,
                timeout=REQUEST_TIMEOUT,
            )
            response.raise_for_status()
            workbook_rows = parse_szse_b_share_workbook(response.content, source)
            if workbook_rows:
                return workbook_rows
        except (requests.RequestException, ValueError):
            continue
    for _attempt in range(3):
        try:
            response = session.get(
                "https://www.szse.cn/api/report/ShowReport",
                params=workbook_params,
                headers=headers,
                timeout=REQUEST_TIMEOUT,
            )
            response.raise_for_status()
            workbook_rows = parse_szse_b_share_workbook(response.content, source)
            if workbook_rows:
                return workbook_rows
        except (requests.RequestException, ValueError):
            continue

    rows: list[dict[str, str]] = []
    page = 1
    total_pages = 1
    while page <= total_pages:
        params = {
            "CATALOGID": SZSE_A_SHARE_CATALOG_ID,
            "TABKEY": SZSE_B_SHARE_TAB_KEY,
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
            raise requests.RequestException("SZSE B-share list unavailable")
        page_rows = parse_szse_b_share_list(payload, source)
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
        asset_type = infer_taiwan_asset_type(ticker, name)
        row = {
            "source_key": source.key,
            "provider": source.provider,
            "source_url": source.source_url,
            "ticker": ticker,
            "name": name,
            "exchange": "TPEX",
            "asset_type": asset_type,
            "listing_status": "active",
            "reference_scope": source.reference_scope,
            "official": "true",
        }
        if asset_type == "ETF":
            isin = derive_taiwan_isin(ticker)
            if isin:
                row["isin"] = isin
        rows.append(
            row
        )
    return rows


def parse_tpex_etf_filter(
    payload: dict[str, Any] | list[dict[str, Any]],
    source: MasterfileSource,
) -> list[dict[str, str]]:
    records = payload.get("data") if isinstance(payload, dict) else payload
    if not isinstance(records, list):
        return []

    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for record in records:
        ticker = str(record.get("stockNo", "")).strip().upper()
        name = str(record.get("stockName", "")).strip()
        if not ticker or not name or not TPEX_ETF_TICKER_RE.fullmatch(ticker):
            continue
        if ticker in seen:
            continue
        seen.add(ticker)
        row = {
            "source_key": source.key,
            "provider": source.provider,
            "source_url": source.source_url,
            "ticker": ticker,
            "name": name,
            "exchange": "TPEX",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": source.reference_scope,
            "official": "true",
        }
        isin = derive_taiwan_isin(ticker)
        if isin:
            row["isin"] = isin
        category = normalize_tpex_etf_category(record)
        if category:
            row["sector"] = category
        rows.append(row)
    return rows


def parse_tpex_emerging_basic_info_csv(text: str, source: MasterfileSource) -> list[dict[str, str]]:
    return parse_tpex_basic_info_csv(text, source, emerging_board=True)


def parse_tpex_mainboard_basic_info_csv(text: str, source: MasterfileSource) -> list[dict[str, str]]:
    return parse_tpex_basic_info_csv(text, source, emerging_board=False)


def parse_tpex_basic_info_csv(
    text: str,
    source: MasterfileSource,
    *,
    emerging_board: bool,
) -> list[dict[str, str]]:
    reader = csv.DictReader(io.StringIO(text))
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for record in reader:
        ticker = str(record.get("公司代號", "")).strip().upper()
        company_name = str(record.get("公司名稱", "")).strip()
        english_short_name = str(record.get("英文簡稱", "")).strip()
        name = company_name or english_short_name
        if not ticker or not name or not TPEX_CANONICAL_TICKER_RE.fullmatch(ticker):
            continue
        if ticker in seen:
            continue
        seen.add(ticker)
        row = {
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
        isin = derive_taiwan_isin(
            ticker,
            emerging_board=emerging_board,
            foreign_registration_country=str(record.get("外國企業註冊地國", "")),
        )
        if isin:
            row["isin"] = isin
        sector = normalize_taiwan_stock_sector(record.get("產業別"))
        if sector:
            row["sector"] = sector
        rows.append(row)
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
        isin = str(record.get("ISIN") or "").strip().upper()
        if market != "EQUITY-CASH" or segment != "CASH":
            continue
        asset_type = B3_ALLOWED_CASH_CATEGORIES.get(category)
        combined_name = f"{name} {record.get('AsstDesc') or ''}".upper()
        if not asset_type and category == "" and B3_CASH_STOCK_TICKER_RE.fullmatch(ticker):
            asset_type = "Stock"
        if not asset_type and category == "BDR" and ticker.endswith("39"):
            if any(marker in combined_name for marker in B3_BDR_ETF_MARKERS):
                asset_type = "ETF"
        if not asset_type or not ticker or not name:
            continue
        normalized_name = name.lower()
        normalized_desc = str(record.get("AsstDesc") or "").lower()
        if ticker.startswith("TAXA") or any(marker in normalized_name for marker in B3_EXCLUDED_ISSUER_MARKERS):
            continue
        if any(marker in normalized_desc for marker in B3_EXCLUDED_ISSUER_MARKERS):
            continue
        row = {
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
        if is_valid_isin(isin):
            row["isin"] = isin
        rows.append(row)
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


def normalize_nasdaq_nordic_ticker(value: str) -> str:
    return re.sub(r"\s+", "-", value.strip().upper())


def parse_nasdaq_nordic_shares(
    payload: dict[str, Any],
    source: MasterfileSource,
    *,
    exchange: str,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for record in ((payload.get("data") or {}).get("instrumentListing") or {}).get("rows") or []:
        ticker = normalize_nasdaq_nordic_ticker(str(record.get("symbol", "")))
        name = str(record.get("fullName", "")).strip()
        isin = str(record.get("isin", "")).strip().upper()
        sector = str(record.get("sector", "")).strip()
        if not ticker or not name:
            continue
        row = {
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
        if isin:
            row["isin"] = isin
        if sector:
            row["sector"] = sector
        rows.append(row)
    return rows


def parse_nasdaq_nordic_stockholm_shares(payload: dict[str, Any], source: MasterfileSource) -> list[dict[str, str]]:
    return parse_nasdaq_nordic_shares(payload, source, exchange="STO")


def iter_nasdaq_nordic_etf_tickers(record: dict[str, Any]) -> list[str]:
    symbol = str(record.get("symbol", "")).strip().upper()
    if not symbol:
        return []

    tickers: list[str] = []
    seen: set[str] = set()

    def add(value: str) -> None:
        candidate = value.strip().upper()
        if not candidate or candidate in seen:
            return
        seen.add(candidate)
        tickers.append(candidate)

    if " " not in symbol:
        add(symbol)
        return tickers

    normalized = re.sub(r"[^A-Z0-9]+", "-", symbol).strip("-")
    add(normalized)
    if len(normalized) > 10:
        add(normalized[:10])
    return tickers


def parse_nasdaq_nordic_etfs(
    payload: dict[str, Any],
    source: MasterfileSource,
    *,
    exchange: str,
) -> list[dict[str, str]]:
    records = ((payload.get("data") or {}).get("instrumentListing") or {}).get("rows") or []
    return parse_nasdaq_nordic_etf_records(records, source, exchange=exchange)


def parse_nasdaq_nordic_stockholm_etfs(payload: dict[str, Any], source: MasterfileSource) -> list[dict[str, str]]:
    return parse_nasdaq_nordic_etfs(payload, source, exchange="STO")


def parse_nasdaq_nordic_stockholm_trackers(payload: dict[str, Any], source: MasterfileSource) -> list[dict[str, str]]:
    records: list[dict[str, Any]] = []
    for group in payload.get("data") or []:
        for record in group.get("instruments") or []:
            if str(record.get("assetClass", "")).strip().upper() != "TRACKER_CERTIFICATES":
                continue
            records.append(record)
    return parse_nasdaq_nordic_etf_records(records, source)


def parse_nasdaq_nordic_etf_records(
    records: list[dict[str, Any]],
    source: MasterfileSource,
    *,
    exchange: str = "STO",
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for record in records:
        name = str(record.get("fullName", "")).strip()
        isin = str(record.get("isin", "")).strip().upper()
        compact_ticker = re.sub(r"[^A-Z0-9]+", "", str(record.get("symbol", "")).strip().upper())
        if not name:
            continue
        for ticker in iter_nasdaq_nordic_etf_tickers(record):
            signature = (ticker, name)
            if signature in seen:
                continue
            seen.add(signature)
            rows.append(
                {
                    "source_key": source.key,
                    "provider": source.provider,
                    "source_url": source.source_url,
                    "ticker": ticker,
                    "name": name,
                    "exchange": exchange,
                    "asset_type": "ETF",
                    "listing_status": "active",
                    "reference_scope": source.reference_scope,
                    "official": "true",
                    "isin": isin,
                }
            )
        if exchange in {"HEL", "CPH"} and compact_ticker and len(compact_ticker) <= 10:
            signature = (compact_ticker, name)
            if signature not in seen:
                seen.add(signature)
                rows.append(
                    {
                        "source_key": source.key,
                        "provider": source.provider,
                        "source_url": source.source_url,
                        "ticker": compact_ticker,
                        "name": name,
                        "exchange": exchange,
                        "asset_type": "ETF",
                        "listing_status": "active",
                        "reference_scope": source.reference_scope,
                        "official": "true",
                        "isin": isin,
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
        row = {
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
        isin = str(record.get("isin", "")).strip().upper()
        if isin:
            row["isin"] = isin
        rows.append(row)
    return rows


def six_share_details_target_rows(listings_path: Path = LISTINGS_CSV) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for row in load_csv(listings_path):
        if row.get("exchange") != "SIX" or not row.get("isin", "").strip():
            continue
        asset_type = row.get("asset_type", "").strip()
        if asset_type == "Stock" and not (row.get("stock_sector", "").strip() or row.get("sector", "").strip()):
            rows.append(row)
        elif asset_type == "ETF" and not (row.get("etf_category", "").strip() or row.get("sector", "").strip()):
            rows.append(row)
    return rows


def parse_six_share_details_fqs_payload(payload: dict[str, Any]) -> list[dict[str, str]]:
    col_names = [str(name) for name in payload.get("colNames") or []]
    rows: list[dict[str, str]] = []
    for values in payload.get("rowData") or []:
        if not isinstance(values, list):
            continue
        rows.append({name: str(value or "").strip() for name, value in zip(col_names, values)})
    return rows


def normalize_six_share_stock_sector(record: dict[str, str], listing: dict[str, str]) -> str:
    industry = " ".join(record.get("IndustrySectorDesc", "").split()).strip()
    key = industry.lower()
    if key == "misc. services":
        ticker = listing.get("ticker", "").strip().upper()
        name_context = f"{listing.get('name', '')} {record.get('IssuerNameFull', '')} {record.get('ShortName', '')}".lower()
        if ticker in SIX_SHARE_MISC_SERVICE_STOCK_SECTOR_OVERRIDES:
            return SIX_SHARE_MISC_SERVICE_STOCK_SECTOR_OVERRIDES[ticker]
        if "sunrise" in name_context:
            return "Communication Services"
        if "r&s" in name_context or "transform" in name_context:
            return "Industrials"
        return ""
    return normalize_sector(SIX_SHARE_INDUSTRY_STOCK_SECTOR_MAP.get(key, industry), "Stock")


def normalize_six_share_etf_category(record: dict[str, str]) -> str:
    return normalize_six_fund_asset_class_category(record.get("AssetClassDesc", ""))


def build_six_share_details_rows(
    payload: dict[str, Any],
    source: MasterfileSource,
    listing: dict[str, str],
) -> list[dict[str, str]]:
    asset_type = listing.get("asset_type", "").strip()
    listing_isin = listing.get("isin", "").strip().upper()
    if asset_type not in {"Stock", "ETF"} or not is_valid_isin(listing_isin):
        return []

    rows: list[dict[str, str]] = []
    for record in parse_six_share_details_fqs_payload(payload):
        record_isin = record.get("ISIN", "").strip().upper()
        if record_isin != listing_isin:
            continue
        sector = (
            normalize_six_share_etf_category(record)
            if asset_type == "ETF"
            else normalize_six_share_stock_sector(record, listing)
        )
        if not sector:
            continue
        name = (
            record.get("IssuerNameFull", "")
            or record.get("FundLongName", "")
            or record.get("ShortName", "")
            or listing.get("name", "")
        )
        rows.append(build_current_listing_reference_row(source, listing, name=name, isin=record_isin, sector=sector))
    return rows


def parse_six_fund_products_csv(text: str, source: MasterfileSource) -> list[dict[str, str]]:
    lines = [line for line in text.splitlines() if line.strip()]
    if not lines:
        return []

    reader = csv.DictReader(io.StringIO("\n".join(lines)), delimiter=";")
    rows: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for record in reader:
        name = str(record.get("FundLongName", "")).strip()
        isin = str(record.get("ISIN", "")).strip().upper()
        category = normalize_six_fund_asset_class_category(record.get("AssetClassDesc", ""))
        if not name:
            continue
        for ticker in iter_six_fund_product_tickers(record):
            key = (ticker, name)
            if key in seen:
                continue
            seen.add(key)
            row = {
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
                "isin": isin,
            }
            if category:
                row["sector"] = category
            rows.append(row)
    return rows


def normalize_six_fund_asset_class_category(value: Any) -> str:
    asset_class = " ".join(str(value or "").split()).strip().lower()
    return SIX_ASSET_CLASS_CATEGORY_MAP.get(asset_class, "")


def normalize_six_reuters_ticker(value: str) -> str:
    normalized = value.strip().upper()
    for suffix in (".S", " S"):
        if normalized.endswith(suffix):
            normalized = normalized[: -len(suffix)]
            break
    return normalized.replace(" ", "")


def normalize_six_bloomberg_ticker(value: str) -> str:
    normalized = value.strip().upper()
    for suffix in (" SE", ".SE"):
        if normalized.endswith(suffix):
            normalized = normalized[: -len(suffix)]
            break
    return normalized.replace(" ", "")


def iter_six_fund_product_tickers(record: dict[str, str]) -> list[str]:
    tickers: list[str] = []

    def add(value: str) -> None:
        normalized = value.strip().upper()
        if normalized and normalized not in tickers:
            tickers.append(normalized)

    valor_symbol = str(record.get("ValorSymbol", "")).strip()
    trading_currency = str(record.get("TradingBaseCurrency", "")).strip().upper()
    fund_currency = str(record.get("FundCurrency", "")).strip().upper()
    if valor_symbol:
        add(valor_symbol)
        if trading_currency:
            add(f"{valor_symbol}-{trading_currency}")
            add(f"{valor_symbol}{trading_currency}")

    reuters_ticker = normalize_six_reuters_ticker(str(record.get("FundReutersTicker", "")))
    bloomberg_ticker = normalize_six_bloomberg_ticker(str(record.get("FundBloombergTicker", "")))
    currencies = [currency for currency in (trading_currency, fund_currency) if currency]
    for market_ticker in (reuters_ticker, bloomberg_ticker):
        add(market_ticker)
        if market_ticker and not any(market_ticker.endswith(currency) for currency in currencies):
            for currency in currencies:
                add(f"{market_ticker}-{currency}")
                add(f"{market_ticker}{currency}")
    if valor_symbol and fund_currency:
        add(f"{valor_symbol}{fund_currency}")
    return tickers


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


def should_skip_psx_dps_symbol(ticker: str, name: str, sector: str) -> bool:
    lowered_name = " ".join(name.lower().split())
    lowered_sector = " ".join(sector.lower().split())
    if any(marker in lowered_sector for marker in PSX_DPS_SECTOR_SKIP_MARKERS):
        return True
    if "exchange traded fund" in lowered_sector:
        return False
    if "(right" in lowered_name or " right)" in lowered_name or lowered_name.endswith(" right"):
        return True
    if ticker.endswith("R") and "right" in lowered_name:
        return True
    return False


def parse_psx_dps_symbols_payload(payload: list[dict[str, Any]], source: MasterfileSource) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    seen_tickers: set[str] = set()
    for item in payload:
        ticker = str(item.get("symbol") or "").strip().upper()
        name = str(item.get("name") or "").strip()
        sector = str(item.get("sectorName") or "").strip().upper()
        if not ticker or not name or ticker in seen_tickers:
            continue
        if item.get("isDebt") or should_skip_psx_dps_symbol(ticker, name, sector):
            continue
        seen_tickers.add(ticker)

        asset_type = "ETF" if item.get("isETF") or "EXCHANGE TRADED FUND" in sector else "Stock"
        row = {
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
        if asset_type == "ETF":
            row["sector"] = "Equity"
        else:
            canonical_sector = PSX_DPS_SECTOR_MAP.get(sector)
            if canonical_sector:
                row["sector"] = canonical_sector
        rows.append(row)
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


def fetch_psx_dps_symbols(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> list[dict[str, Any]]:
    session = session or requests.Session()
    response = session.get(
        source.source_url,
        headers={"User-Agent": USER_AGENT, "Accept": "application/json,*/*"},
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list):
        raise ValueError("PSX DPS symbols payload must be a JSON array")
    return payload


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


def fetch_jse_instrument_search_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    asset_type_by_ticker = {
        row.get("ticker", "").strip(): row.get("asset_type", "")
        for row in load_csv(LISTINGS_CSV)
        if row.get("exchange") == "JSE"
        and row.get("asset_type") == "Stock"
        and row.get("ticker", "").strip()
    }
    return fetch_jse_instrument_search_exact(
        source,
        jse_instrument_search_target_tickers(),
        session=session,
        asset_type_by_ticker=asset_type_by_ticker,
    )


def fetch_nasdaq_nordic_shares(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    session = session or requests.Session()
    config = NASDAQ_NORDIC_SHARES_SOURCE_CONFIG[source.key]
    headers = nasdaq_nordic_request_headers(NASDAQ_NORDIC_STOCK_PAGE_URL)
    rows: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for category in config["categories"]:
        response = session.get(
            source.source_url,
            params={
                "category": category,
                "tableonly": "false",
                "market": config["market"],
            },
            headers=headers,
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        for row in parse_nasdaq_nordic_shares(response.json(), source, exchange=config["exchange"]):
            signature = (row["ticker"], row["name"])
            if signature in seen:
                continue
            seen.add(signature)
            rows.append(row)
    return rows


def fetch_nasdaq_nordic_stockholm_shares(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    return fetch_nasdaq_nordic_shares(source, session=session)


def fetch_nasdaq_nordic_helsinki_shares(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    return fetch_nasdaq_nordic_shares(source, session=session)


def fetch_nasdaq_nordic_copenhagen_shares(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    return fetch_nasdaq_nordic_shares(source, session=session)


def fetch_nasdaq_nordic_iceland_shares(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    return fetch_nasdaq_nordic_shares(source, session=session)


def fetch_nasdaq_nordic_etfs(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    session = session or requests.Session()
    config = NASDAQ_NORDIC_ETF_SOURCE_CONFIG[source.key]
    headers = nasdaq_nordic_request_headers(NASDAQ_NORDIC_ETF_PAGE_URL)
    response = session.get(
        source.source_url,
        params={
            "category": "ETF",
            "market": config["market"],
            "tableonly": "false",
        },
        headers=headers,
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return parse_nasdaq_nordic_etfs(response.json(), source, exchange=config["exchange"])


def fetch_nasdaq_nordic_stockholm_etfs(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    return fetch_nasdaq_nordic_etfs(source, session=session)


def fetch_nasdaq_nordic_helsinki_etfs(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    return fetch_nasdaq_nordic_etfs(source, session=session)


def fetch_nasdaq_nordic_copenhagen_etfs(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    return fetch_nasdaq_nordic_etfs(source, session=session)


def fetch_nasdaq_nordic_stockholm_trackers(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    session = session or requests.Session()
    headers = nasdaq_nordic_request_headers()
    headers["Referer"] = NASDAQ_NORDIC_ETF_PAGE_URL
    headers["Origin"] = "https://www.nasdaq.com"

    rows: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for search_term in NASDAQ_NORDIC_STOCKHOLM_TRACKER_SEARCH_TERMS:
        response = session.get(
            source.source_url,
            params={"searchText": search_term},
            headers=headers,
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        for row in parse_nasdaq_nordic_stockholm_trackers(response.json(), source):
            signature = (row["ticker"], row["name"])
            if signature in seen:
                continue
            seen.add(signature)
            rows.append(row)
    return rows


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


def fetch_six_share_details_fqs(
    source: MasterfileSource,
    session: requests.Session | None = None,
    *,
    listings_path: Path = LISTINGS_CSV,
) -> list[dict[str, str]]:
    session = session or requests.Session()
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json,text/plain,*/*",
        "Referer": "https://www.six-group.com/en/market-data/shares/share-explorer.html",
    }
    rows: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    select_fields = ",".join(SIX_SHARE_DETAILS_FQS_FIELDS)
    for listing in six_share_details_target_rows(listings_path=listings_path):
        isin = listing.get("isin", "").strip().upper()
        response = session.get(
            source.source_url,
            params={"select": select_fields, "where": f"ISIN={isin}", "pagesize": 50},
            headers=headers,
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        for row in build_six_share_details_rows(response.json(), source, listing):
            signature = (row.get("ticker", ""), row.get("isin", ""), row.get("sector", ""))
            if signature in seen:
                continue
            seen.add(signature)
            rows.append(row)
    return rows


def load_six_share_details_fqs_rows(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> tuple[list[dict[str, str]] | None, str]:
    try:
        rows = fetch_six_share_details_fqs(source, session=session)
    except (requests.RequestException, ValueError, json.JSONDecodeError):
        for path in (SIX_SHARE_DETAILS_FQS_CACHE, LEGACY_SIX_SHARE_DETAILS_FQS_CACHE):
            if path.exists():
                return json.loads(path.read_text(encoding="utf-8")), "cache"
        return None, "unavailable"

    ensure_output_dirs()
    SIX_SHARE_DETAILS_FQS_CACHE.write_text(json.dumps(rows), encoding="utf-8")
    return rows, "network"


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


def fetch_set_etf_search(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    text = fetch_text(source.source_url, session=session)
    return parse_set_quote_search_payload(text, source)


def fetch_set_dr_search(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    text = fetch_text(source.source_url, session=session)
    return parse_set_dr_search_payload(text, source)


def fetch_set_stock_search(
    source: MasterfileSource,
    session: requests.Session | None = None,
) -> dict[str, Any]:
    session = session or requests.Session()
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json,text/plain,*/*",
        "Referer": SET_STOCK_SEARCH_URL,
    }
    response = session.get(source.source_url, headers=headers, timeout=REQUEST_TIMEOUT)
    if response.ok and response.headers.get("content-type", "").startswith("application/json"):
        return response.json()

    try:
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise ImportError("SET stock search API requires Playwright when requests is blocked") from exc

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        try:
            context = browser.new_context(
                locale="en-US",
                user_agent=USER_AGENT,
                extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
            )
            page = context.new_page()
            page.goto(SET_STOCK_SEARCH_URL, wait_until="networkidle", timeout=60_000)
            result = page.evaluate(
                """async (apiUrl) => {
                    const response = await fetch(apiUrl, {
                        credentials: "include",
                        headers: {accept: "application/json,text/plain,*/*"}
                    });
                    const text = await response.text();
                    if (!response.ok) {
                        throw new Error(`${response.status} ${text.slice(0, 120)}`);
                    }
                    return JSON.parse(text);
                }""",
                source.source_url,
            )
        except PlaywrightTimeoutError as exc:
            raise requests.RequestException("SET stock search browser fetch timed out") from exc
        finally:
            browser.close()
    if not isinstance(result, dict):
        raise ValueError("SET stock search API payload must be a JSON object")
    return result


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

    target_tickers = latest_reference_gap_tickers(STOCK_VERIFICATION_DIR, exchanges={"KRX", "KOSDAQ"})
    target_tickers |= missing_isin_listing_tickers(
        exchanges={"KRX", "KOSDAQ"},
        asset_types={"Stock"},
    )
    if not rows and not target_tickers:
        return rows

    finder_payload = fetch_krx_finder_records(
        "dbms/comm/finder/finder_stkisu",
        session=session,
    )
    exact_rows_by_ticker = {
        row["ticker"]: row
        for row in parse_krx_stock_finder_records(finder_payload, source)
        if row.get("isin")
    }
    if not exact_rows_by_ticker:
        return rows

    merged_rows: list[dict[str, str]] = []
    seen_tickers: set[str] = set()
    for row in rows:
        ticker = row["ticker"]
        if ticker in seen_tickers:
            continue
        exact_row = exact_rows_by_ticker.get(ticker)
        if exact_row:
            merged_rows.append({**row, "isin": exact_row["isin"]})
        else:
            merged_rows.append(row)
        seen_tickers.add(ticker)
    for ticker in sorted(target_tickers):
        if ticker in seen_tickers:
            continue
        exact_row = exact_rows_by_ticker.get(ticker)
        if not exact_row:
            continue
        merged_rows.append(exact_row)
        seen_tickers.add(ticker)
    return merged_rows


def fetch_krx_etf_finder(source: MasterfileSource, session: requests.Session | None = None) -> list[dict[str, str]]:
    session = session or requests.Session()
    payload = {
        "block1": fetch_krx_finder_records(
            "dbms/comm/finder/finder_secuprodisu",
            mktsel="ETF",
            session=session,
        )
    }
    rows = parse_krx_etf_finder(payload, source)

    target_tickers = latest_reference_gap_tickers(ETF_VERIFICATION_DIR, exchanges={"KRX"})
    target_tickers |= missing_isin_listing_tickers(
        exchanges={"KRX"},
        asset_types={"ETF"},
    )
    if not rows and not target_tickers:
        return rows

    exact_rows_by_ticker = {
        row["ticker"]: row
        for row in parse_krx_product_finder_records(
            fetch_krx_finder_records(
                "dbms/comm/finder/finder_secuprodisu",
                session=session,
            ),
            source,
        )
        if row.get("isin")
    }
    if not exact_rows_by_ticker:
        return rows

    merged_rows: list[dict[str, str]] = []
    seen_tickers: set[str] = set()
    for row in rows:
        ticker = row["ticker"]
        if ticker in seen_tickers:
            continue
        exact_row = exact_rows_by_ticker.get(ticker)
        if exact_row:
            merged_rows.append({**row, "isin": exact_row["isin"]})
        else:
            merged_rows.append(row)
        seen_tickers.add(ticker)
    for ticker in sorted(target_tickers):
        if ticker in seen_tickers:
            continue
        exact_row = exact_rows_by_ticker.get(ticker)
        if not exact_row:
            continue
        merged_rows.append(exact_row)
        seen_tickers.add(ticker)
    return merged_rows


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
    if source.format == "lse_price_explorer_json":
        return fetch_lse_price_explorer_rows(source, session=session)
    if source.format == "asx_listed_companies_csv":
        text = fetch_text(source.source_url, session=session)
        return parse_asx_listed_companies(text, source)
    if source.format == "asx_investment_products_excel":
        return fetch_asx_investment_products(source, session=session)
    if source.format == "cboe_canada_listing_directory_json":
        payload = fetch_json(source.source_url, session=session)
        return parse_cboe_canada_listing_directory_payload(payload, source), "network"
    if source.format == "cboe_canada_listing_directory_html":
        text = fetch_text(source.source_url, session=session)
        return parse_cboe_canada_listing_directory_html(text, source)
    if source.format == "set_listed_companies_html":
        text = fetch_bytes(source.source_url, session=session).decode("windows-1250", errors="replace")
        return parse_set_listed_companies_html(text, source)
    if source.format == "set_stock_search_json":
        return parse_set_stock_search_payload(fetch_set_stock_search(source, session=session), source)
    if source.format == "set_etf_search_html":
        return fetch_set_etf_search(source, session=session)
    if source.format == "set_dr_search_html":
        return fetch_set_dr_search(source, session=session)
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
    if source.format == "jse_instrument_search_html":
        return fetch_jse_instrument_search_rows(source, session=session)
    if source.format in {"bme_listed_companies_json", "bme_etf_list_json"}:
        return fetch_bme_reference_rows(source, session=session)
    if source.format == "bme_listed_values_pdf":
        return fetch_bme_listed_values_rows(source, session=session)
    if source.format == "bme_security_prices_json":
        return fetch_bme_security_prices_rows(source, session=session)
    if source.format == "bme_growth_prices_html":
        return fetch_bme_growth_reference_rows(source, session=session)
    if source.format == "athex_sector_classification_pdf":
        return fetch_athex_classification_rows(source, session=session)
    if source.format == "bursa_equity_isin_pdf":
        return fetch_bursa_equity_isin_rows(source, session=session)
    if source.format == "bursa_closing_prices_pdf":
        return fetch_bursa_closing_prices_rows(source, session=session)
    if source.format == "bse_bw_listed_companies_html":
        return fetch_bse_bw_listed_companies(source, session=session)
    if source.format == "bse_hu_marketdata_html":
        return fetch_bse_hu_marketdata_rows(source, session=session)
    if source.format == "egx_listed_stocks_viewstate":
        return fetch_egx_listed_stocks_rows(source, session=session)
    if source.format == "cavali_bvl_emisores_html":
        return fetch_cavali_bvl_emisores_rows(source, session=session)
    if source.format == "cse_ma_listed_companies_json":
        return fetch_cse_ma_listed_companies(source, session=session)
    if source.format == "cse_lk_all_security_code_json":
        return fetch_cse_lk_all_security_code_rows(source, session=session)
    if source.format == "cse_lk_company_info_summary_json":
        return fetch_cse_lk_company_info_summary_rows(source)
    if source.format == "bvc_rv_issuers_json":
        return fetch_bvc_rv_issuers_rows(source, session=session)
    if source.format == "byma_equity_details_json":
        return fetch_byma_equity_details_rows(source, session=session)
    if source.format == "dse_tz_listed_companies_html":
        return fetch_dse_tz_listed_companies(source, session=session)
    if source.format == "mse_mw_mainboard_html":
        return fetch_mse_mw_mainboard_rows(source, session=session)
    if source.format == "nse_ke_listed_companies_html":
        return fetch_nse_ke_listed_companies(source, session=session)
    if source.format == "nse_india_securities_available_csv":
        return fetch_nse_india_securities_available_rows(source, session=session)
    if source.format == "bse_india_scrips_json":
        return fetch_bse_india_scrips_rows(source, session=session)
    if source.format == "hkex_securities_list_xlsx":
        return fetch_hkex_securities_list_rows(source, session=session)
    if source.format == "sgx_securities_prices_json":
        return fetch_sgx_securities_prices_rows(source, session=session)
    if source.format == "dfm_listed_securities_json":
        return fetch_dfm_listed_securities_rows(source, session=session)
    if source.format == "boursa_kuwait_legacy_mix_json":
        return fetch_boursa_kuwait_stocks_rows(source, session=session)
    if source.format == "bist_kap_mkk_listed_securities_json":
        return fetch_bist_kap_mkk_listed_securities_rows(source, session=session)
    if source.format == "tadawul_securities_json":
        return fetch_tadawul_securities_rows(source, session=session)
    if source.format == "adx_market_watch_json":
        return fetch_adx_market_watch_rows(source, session=session)
    if source.format == "qse_market_watch_json":
        return fetch_qse_market_watch_rows(source, session=session)
    if source.format == "msx_companies_json":
        return fetch_msx_companies_rows(source, session=session)
    if source.format == "rse_listed_companies_html":
        return fetch_rse_listed_companies_rows(source, session=session)
    if source.format == "gse_listed_companies_markdown":
        return fetch_gse_listed_companies_rows(source, session=session)
    if source.format == "luse_listed_companies_markdown":
        return fetch_luse_listed_companies_rows(source, session=session)
    if source.format == "bolsa_santiago_instruments_json":
        return fetch_bolsa_santiago_instruments_rows(source, session=session)
    if source.format == "sem_isin_xlsx":
        return fetch_sem_isin_rows(source, session=session)
    if source.format == "use_ug_market_snapshot_html":
        return fetch_use_ug_market_snapshot_rows(source, session=session)
    if source.format == "nzx_instruments_next_data":
        return fetch_nzx_instruments_rows(source, session=session)
    if source.format == "nasdaq_mutual_fund_quote_json":
        return fetch_nasdaq_mutual_fund_quote_rows(source, session=session)
    if source.format == "zse_zw_issuers_json":
        return fetch_zse_zw_issuers_rows(source, session=session)
    if source.format == "bvb_shares_directory_html":
        return fetch_bvb_shares_directory(source, session=session)
    if source.format == "bvb_fund_units_directory_html":
        return fetch_bvb_fund_units_directory(source, session=session)
    if source.format == "ngx_company_profile_directory_html":
        return fetch_ngx_company_profile_directory(source, session=session)
    if source.format == "ngx_equities_price_list_json":
        return fetch_ngx_equities_price_list(source, session=session)
    if source.format == "euronext_equities_semicolon_csv":
        text = fetch_text(source.source_url, session=session)
        return parse_euronext_equities_download(text, source)
    if source.format == "euronext_etfs_semicolon_csv":
        text = fetch_text(source.source_url, session=session)
        return parse_euronext_etfs_download(text, source)
    if source.format == "jpx_listed_issues_excel":
        content = fetch_bytes(source.source_url, session=session)
        return parse_jpx_listed_issues_excel(content, source)
    if source.format == "jpx_tse_stock_detail_json":
        return fetch_jpx_tse_stock_detail_rows(source)
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
    if source.format == "six_share_details_fqs_json":
        return fetch_six_share_details_fqs(source, session=session)
    if source.format == "six_fund_products_csv":
        return fetch_six_fund_products(source, session=session)
    if source.format == "b3_instruments_equities_api":
        return fetch_b3_instruments_equities(source, session=session)
    if source.format == "b3_listed_funds_api":
        return fetch_b3_listed_funds(source, session=session)
    if source.format == "b3_bdr_companies_api":
        return fetch_b3_bdr_companies(source, session=session)
    if source.format in {
        "nasdaq_nordic_stockholm_shares_json",
        "nasdaq_nordic_helsinki_shares_json",
        "nasdaq_nordic_iceland_shares_json",
        "nasdaq_nordic_copenhagen_shares_json",
    }:
        return fetch_nasdaq_nordic_stockholm_shares(source, session=session)
    if source.format == "nasdaq_nordic_copenhagen_etf_search_json":
        return fetch_nasdaq_nordic_etf_search(source, session=session)
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
    if source.format == "szse_b_share_list_json":
        rows, mode = load_szse_b_share_list_rows(source, session=session)
        if rows is None:
            return [], mode
        return rows, mode
    if source.format == "szse_etf_list_json":
        rows, mode = load_szse_etf_list_rows(source, session=session)
        if rows is None:
            return [], mode
        return rows, mode
    if source.format == "tpex_mainboard_quotes_json":
        payload = fetch_json(source.source_url, session=session)
        return parse_tpex_mainboard_quotes(payload, source)
    if source.format == "tpex_etf_filter_json":
        session = session or requests.Session()
        response = session.post(
            f"{TPEX_ETF_FILTER_API_URL}?lang=en-us",
            headers=tpex_infohub_request_headers(source.source_url),
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        return parse_tpex_etf_filter(response.json(), source)
    if source.format == "tpex_mainboard_basic_csv":
        text, mode = load_tpex_mainboard_basic_info_text(session=session)
        if text is None:
            raise requests.RequestException("TPEX mainboard basic info unavailable")
        return parse_tpex_mainboard_basic_info_csv(text, source)
    if source.format == "tpex_emerging_basic_csv":
        text, mode = load_tpex_emerging_basic_info_text(session=session)
        if text is None:
            raise requests.RequestException("TPEX emerging basic info unavailable")
        return parse_tpex_emerging_basic_info_csv(text, source)
    if source.format in {
        "nasdaq_nordic_stockholm_shares_search_json",
        "nasdaq_nordic_helsinki_shares_search_json",
        "nasdaq_nordic_copenhagen_shares_search_json",
    }:
        return fetch_nasdaq_nordic_share_search(source, session=session)
    if source.format == "spotlight_companies_search_json":
        return fetch_spotlight_companies_search(source, session=session)
    if source.format == "spotlight_companies_directory_json":
        return fetch_spotlight_companies_directory(source, session=session)
    if source.format == "ngm_companies_page_html":
        return fetch_ngm_companies_page(source, session=session)
    if source.format == "ngm_market_data_equities_json":
        return fetch_ngm_market_data_equities(source, session=session)
    if source.format == "krx_listed_companies_json":
        return fetch_krx_listed_companies(source, session=session)
    if source.format == "krx_etf_finder_json":
        return fetch_krx_etf_finder(source, session=session)
    if source.format == "psx_listed_companies_json":
        return fetch_psx_listed_companies(source, session=session)
    if source.format == "psx_symbol_name_daily_zip":
        return fetch_psx_symbol_name_daily(source, session=session)
    if source.format == "psx_dps_symbols_json":
        return parse_psx_dps_symbols_payload(fetch_psx_dps_symbols(source, session=session), source)
    if source.format == "pse_listed_company_directory_html":
        return fetch_pse_listed_company_directory(source, session=session)
    if source.format == "pse_cz_shares_directory_html":
        return fetch_pse_cz_shares_directory(source, session=session)
    if source.format == "vienna_listed_companies_html":
        return fetch_vienna_listed_companies(source, session=session)
    if source.format == "zagreb_securities_html":
        return fetch_zagreb_securities(source, session=session)
    if source.format == "idx_listed_companies_json":
        return fetch_idx_listed_companies(source, session=session)
    if source.format == "idx_company_profiles_json":
        return fetch_idx_company_profiles(source, session=session)
    if source.format == "wse_listed_companies_html":
        return fetch_wse_listed_companies(source, session=session)
    if source.format == "newconnect_listed_companies_html":
        return fetch_newconnect_listed_companies(source, session=session)
    if source.format == "wse_etf_list_html":
        return fetch_wse_etf_list(source, session=session)
    if source.format == "tase_securities_marketdata_json":
        return fetch_tase_securities_marketdata(source, session=session)
    if source.format == "tase_etf_marketdata_json":
        return fetch_tase_etf_marketdata(source, session=session)
    if source.format == "tase_foreign_etf_search_json":
        return fetch_tase_foreign_etf_search(source, session=session)
    if source.format == "tase_participating_unit_search_json":
        return fetch_tase_participating_unit_search(source, session=session)
    if source.format in {
        "hose_listed_stocks_json",
        "hose_etf_list_json",
        "hose_fund_certificate_list_json",
    }:
        return fetch_hose_securities_rows(source, session=session)
    if source.format in {"hnx_listed_securities_json", "upcom_registered_securities_json"}:
        return fetch_hnx_issuer_rows(source, session=session)
    if source.format == "sec_company_tickers_exchange_json":
        payload = fetch_json(source.source_url, session=session)
        return parse_sec_company_tickers_exchange(payload, source)
    if source.format == "otc_markets_security_profile_json":
        return fetch_otc_markets_security_profile(source, session=session)
    if source.format == "otc_markets_stock_screener_csv":
        return fetch_otc_markets_stock_screener_rows(source, session=session)
    if source.format == "bmv_issuer_directory_json":
        return fetch_bmv_issuer_directory(source, session=session)
    if source.format == "bmv_market_data_security_pages_html":
        return fetch_bmv_market_data_securities(source, session=session)
    if source.format == "bmv_etf_search_json":
        return fetch_bmv_etf_search(source, session=session)
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
    if source.format == "lse_price_explorer_json":
        rows, mode = load_lse_price_explorer_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("LSE price explorer unavailable")
        return rows, mode
    if source.format == "b3_instruments_equities_api":
        rows, mode = load_b3_instruments_equities_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("B3 instruments equities unavailable")
        return rows, mode
    if source.format == "six_share_details_fqs_json":
        rows, mode = load_six_share_details_fqs_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("SIX share details FQS unavailable")
        return rows, mode
    if source.format == "cboe_canada_listing_directory_json":
        return parse_cboe_canada_listing_directory_payload(
            fetch_json(source.source_url, session=session),
            source,
        ), "network"
    if source.format == "sec_company_tickers_exchange_json":
        payload, mode = load_sec_company_tickers_exchange_payload(session=session)
        if payload is None:
            raise requests.RequestException("SEC company_tickers_exchange.json unavailable")
        return parse_sec_company_tickers_exchange(payload, source), mode
    if source.format == "otc_markets_security_profile_json":
        rows, mode = load_otc_markets_security_profile_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("OTC Markets security profiles unavailable")
        return rows, mode
    if source.format == "otc_markets_stock_screener_csv":
        rows, mode = load_otc_markets_stock_screener_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("OTC Markets stock screener unavailable")
        return rows, mode
    if source.format == "tpex_mainboard_quotes_json":
        payload, mode = load_tpex_mainboard_quotes_payload(session=session)
        if payload is None:
            raise requests.RequestException("TPEX mainboard quotes unavailable")
        return parse_tpex_mainboard_quotes(payload, source), mode
    if source.format == "tpex_etf_filter_json":
        payload, mode = load_tpex_etf_filter_payload(session=session)
        if payload is None:
            raise requests.RequestException("TPEX ETF filter unavailable")
        return parse_tpex_etf_filter(payload, source), mode
    if source.format == "tpex_mainboard_basic_csv":
        text, mode = load_tpex_mainboard_basic_info_text(session=session)
        if text is None:
            raise requests.RequestException("TPEX mainboard basic info unavailable")
        return parse_tpex_mainboard_basic_info_csv(text, source), mode
    if source.format == "tpex_emerging_basic_csv":
        text, mode = load_tpex_emerging_basic_info_text(session=session)
        if text is None:
            raise requests.RequestException("TPEX emerging basic info unavailable")
        return parse_tpex_emerging_basic_info_csv(text, source), mode
    if source.format == "set_etf_search_html":
        rows, mode = load_set_etf_search_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("SET ETF search unavailable")
        return rows, mode
    if source.format == "set_dr_search_html":
        rows, mode = load_set_dr_search_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("SET DR search unavailable")
        return rows, mode
    if source.format == "set_stock_search_json":
        rows, mode = load_set_stock_search_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("SET stock search unavailable")
        return rows, mode
    if source.format == "psx_dps_symbols_json":
        rows, mode = load_psx_dps_symbols_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("PSX DPS symbols unavailable")
        return rows, mode
    if source.format == "pse_listed_company_directory_html":
        rows, mode = load_pse_listed_company_directory_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("PSE listed company directory unavailable")
        return rows, mode
    if source.format == "pse_cz_shares_directory_html":
        rows, mode = load_pse_cz_shares_directory_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("Prague Stock Exchange shares directory unavailable")
        return rows, mode
    if source.format == "vienna_listed_companies_html":
        rows, mode = load_vienna_listed_companies_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("Vienna listed companies unavailable")
        return rows, mode
    if source.format == "zagreb_securities_html":
        rows, mode = load_zagreb_securities_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("Zagreb securities directory unavailable")
        return rows, mode
    if source.format == "idx_listed_companies_json":
        rows, mode = load_idx_listed_companies_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("IDX listed companies unavailable")
        return rows, mode
    if source.format == "idx_company_profiles_json":
        rows, mode = load_idx_company_profiles_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("IDX company profiles unavailable")
        return rows, mode
    if source.format in {
        "wse_listed_companies_html",
        "newconnect_listed_companies_html",
        "wse_etf_list_html",
    }:
        rows, mode = load_wse_reference_rows(source, session=session)
        if rows is None:
            raise requests.RequestException(f"WSE reference rows unavailable for {source.key}")
        return rows, mode
    if source.format == "tase_securities_marketdata_json":
        rows, mode = load_tase_securities_marketdata_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("TASE securities marketdata unavailable")
        return rows, mode
    if source.format == "tase_etf_marketdata_json":
        rows, mode = load_tase_etf_marketdata_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("TASE ETF marketdata unavailable")
        return rows, mode
    if source.format == "tase_foreign_etf_search_json":
        rows, mode = load_tase_foreign_etf_search_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("TASE foreign ETF search unavailable")
        return rows, mode
    if source.format == "tase_participating_unit_search_json":
        rows, mode = load_tase_participating_unit_search_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("TASE participating unit search unavailable")
        return rows, mode
    if source.format in {
        "hose_listed_stocks_json",
        "hose_etf_list_json",
        "hose_fund_certificate_list_json",
    }:
        rows, mode = load_hose_securities_rows(source, session=session)
        if rows is None:
            raise requests.RequestException(f"HOSE securities unavailable for {source.key}")
        return rows, mode
    if source.format in {"hnx_listed_securities_json", "upcom_registered_securities_json"}:
        rows, mode = load_hnx_issuer_rows(source, session=session)
        if rows is None:
            raise requests.RequestException(f"HNX issuer rows unavailable for {source.key}")
        return rows, mode
    if source.format == "bmv_stock_search_json":
        rows, mode = load_bmv_stock_search_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("BMV stock search unavailable")
        return rows, mode
    if source.format == "bmv_capital_trust_search_json":
        rows, mode = load_bmv_capital_trust_search_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("BMV capital trust search unavailable")
        return rows, mode
    if source.format == "bmv_etf_search_json":
        rows, mode = load_bmv_etf_search_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("BMV ETF search unavailable")
        return rows, mode
    if source.format == "bmv_issuer_directory_json":
        rows, mode = load_bmv_issuer_directory_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("BMV issuer directory unavailable")
        return rows, mode
    if source.format == "bmv_market_data_security_pages_html":
        rows, mode = load_bmv_market_data_securities_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("BMV market-data security pages unavailable")
        return rows, mode
    if source.format == "szse_b_share_list_json":
        rows, mode = load_szse_b_share_list_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("SZSE B-share list unavailable")
        return rows, mode
    if source.format == "szse_etf_list_json":
        rows, mode = load_szse_etf_list_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("SZSE ETF list unavailable")
        return rows, mode
    if source.format in {"jse_etf_list_xlsx", "jse_etn_list_xlsx"}:
        rows, mode = load_jse_exchange_traded_product_rows(source, session=session)
        if rows is None:
            raise requests.RequestException(f"{source.provider} {source.key} workbook unavailable")
        return rows, mode
    if source.format in {"bme_listed_companies_json", "bme_etf_list_json"}:
        rows, mode = load_bme_reference_rows(source, session=session)
        if rows is None:
            raise requests.RequestException(f"BME reference rows unavailable for {source.key}")
        return rows, mode
    if source.format == "bme_listed_values_pdf":
        rows, mode = load_bme_listed_values_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("BME listed values rows unavailable")
        return rows, mode
    if source.format == "bme_security_prices_json":
        rows, mode = load_bme_security_prices_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("BME security prices rows unavailable")
        return rows, mode
    if source.format == "bme_growth_prices_html":
        rows, mode = load_bme_growth_reference_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("BME Growth reference rows unavailable")
        return rows, mode
    if source.format == "jpx_tse_stock_detail_json":
        rows, mode = load_jpx_tse_stock_detail_rows(source)
        if rows is None:
            raise requests.RequestException("JPX/TSE stock detail ISIN rows unavailable")
        return rows, mode
    if source.format == "athex_sector_classification_pdf":
        rows, mode = load_athex_classification_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("ATHEX classification rows unavailable")
        return rows, mode
    if source.format == "bursa_equity_isin_pdf":
        rows, mode = load_bursa_equity_isin_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("Bursa equity ISIN rows unavailable")
        return rows, mode
    if source.format == "bursa_closing_prices_pdf":
        rows, mode = load_bursa_closing_prices_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("Bursa closing-prices rows unavailable")
        return rows, mode
    if source.format == "bse_bw_listed_companies_html":
        rows, mode = load_bse_bw_listed_companies_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("BSE Botswana listed companies unavailable")
        return rows, mode
    if source.format == "bse_hu_marketdata_html":
        rows, mode = load_bse_hu_marketdata_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("Budapest Stock Exchange marketdata unavailable")
        return rows, mode
    if source.format == "egx_listed_stocks_viewstate":
        rows, mode = load_egx_listed_stocks_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("EGX listed stocks unavailable")
        return rows, mode
    if source.format == "cavali_bvl_emisores_html":
        rows, mode = load_cavali_bvl_emisores_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("CAVALI BVL issuer rows unavailable")
        return rows, mode
    if source.format == "cse_ma_listed_companies_json":
        rows, mode = load_cse_ma_listed_companies_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("Casablanca Stock Exchange listed companies unavailable")
        return rows, mode
    if source.format == "cse_lk_all_security_code_json":
        rows, mode = load_cse_lk_all_security_code_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("CSE Sri Lanka all-security-code API unavailable")
        return rows, mode
    if source.format == "cse_lk_company_info_summary_json":
        rows, mode = load_cse_lk_company_info_summary_rows(source)
        if rows is None:
            raise requests.RequestException("CSE Sri Lanka company-info summary API unavailable")
        return rows, mode
    if source.format == "bvc_rv_issuers_json":
        rows, mode = load_bvc_rv_issuers_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("BVC local-equity issuer API unavailable")
        return rows, mode
    if source.format == "byma_equity_details_json":
        rows, mode = load_byma_equity_details_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("BYMA equity detail API unavailable")
        return rows, mode
    if source.format == "dse_tz_listed_companies_html":
        rows, mode = load_dse_tz_listed_companies_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("DSE Tanzania listed companies unavailable")
        return rows, mode
    if source.format == "mse_mw_mainboard_html":
        rows, mode = load_mse_mw_mainboard_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("MSE Malawi mainboard unavailable")
        return rows, mode
    if source.format == "nse_ke_listed_companies_html":
        rows, mode = load_nse_ke_listed_companies_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("NSE Kenya listed companies unavailable")
        return rows, mode
    if source.format == "nse_india_securities_available_csv":
        rows, mode = load_nse_india_securities_available_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("NSE India securities-available CSVs unavailable")
        return rows, mode
    if source.format == "bse_india_scrips_json":
        rows, mode = load_bse_india_scrips_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("BSE India scrip API unavailable")
        return rows, mode
    if source.format == "hkex_securities_list_xlsx":
        rows, mode = load_hkex_securities_list_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("HKEX securities list workbook unavailable")
        return rows, mode
    if source.format == "sgx_securities_prices_json":
        rows, mode = load_sgx_securities_prices_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("SGX securities prices API unavailable")
        return rows, mode
    if source.format == "dfm_listed_securities_json":
        rows, mode = load_dfm_listed_securities_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("DFM listed securities API unavailable")
        return rows, mode
    if source.format == "boursa_kuwait_legacy_mix_json":
        rows, mode = load_boursa_kuwait_stocks_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("Boursa Kuwait market data API unavailable")
        return rows, mode
    if source.format == "bahrain_bourse_isin_codes_html":
        rows, mode = load_bahrain_bourse_listed_companies_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("Bahrain Bourse ISIN code table unavailable")
        return rows, mode
    if source.format == "bist_kap_mkk_listed_securities_json":
        rows, mode = load_bist_kap_mkk_listed_securities_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("KAP/MKK BIST securities unavailable")
        return rows, mode
    if source.format == "tadawul_securities_json":
        rows, mode = load_tadawul_securities_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("Saudi Exchange securities unavailable")
        return rows, mode
    if source.format == "adx_market_watch_json":
        rows, mode = load_adx_market_watch_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("ADX market-watch securities unavailable")
        return rows, mode
    if source.format == "qse_market_watch_json":
        rows, mode = load_qse_market_watch_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("QSE MarketWatch unavailable")
        return rows, mode
    if source.format == "msx_companies_json":
        rows, mode = load_msx_companies_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("MSX companies API unavailable")
        return rows, mode
    if source.format == "rse_listed_companies_html":
        rows, mode = load_rse_listed_companies_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("RSE listed companies unavailable")
        return rows, mode
    if source.format == "gse_listed_companies_markdown":
        rows, mode = load_gse_listed_companies_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("GSE listed companies unavailable")
        return rows, mode
    if source.format == "luse_listed_companies_markdown":
        rows, mode = load_luse_listed_companies_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("LuSE listed companies unavailable")
        return rows, mode
    if source.format == "bolsa_santiago_instruments_json":
        rows, mode = load_bolsa_santiago_instruments_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("Bolsa de Santiago instruments unavailable")
        return rows, mode
    if source.format == "sem_isin_xlsx":
        rows, mode = load_sem_isin_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("SEM ISIN workbook unavailable")
        return rows, mode
    if source.format == "use_ug_market_snapshot_html":
        rows, mode = load_use_ug_market_snapshot_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("USE Uganda market snapshot unavailable")
        return rows, mode
    if source.format == "nzx_instruments_next_data":
        rows, mode = load_nzx_instruments_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("NZX instruments unavailable")
        return rows, mode
    if source.format == "nasdaq_mutual_fund_quote_json":
        rows, mode = load_nasdaq_mutual_fund_quote_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("Nasdaq mutual fund quote API unavailable")
        return rows, mode
    if source.format == "zse_zw_issuers_json":
        rows, mode = load_zse_zw_issuers_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("ZSE Zimbabwe issuer API unavailable")
        return rows, mode
    if source.format == "bvb_shares_directory_html":
        rows, mode = load_bvb_shares_directory_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("BVB shares directory unavailable")
        return rows, mode
    if source.format == "bvb_fund_units_directory_html":
        rows, mode = load_bvb_fund_units_directory_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("BVB fund units directory unavailable")
        return rows, mode
    if source.format == "ngx_company_profile_directory_html":
        rows, mode = load_ngx_company_profile_directory_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("NGX company profile directory unavailable")
        return rows, mode
    if source.format == "ngx_equities_price_list_json":
        rows, mode = load_ngx_equities_price_list_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("NGX equities price list unavailable")
        return rows, mode
    if source.format == "jse_instrument_search_html":
        rows, mode = load_jse_instrument_search_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("JSE instrument search unavailable")
        return rows, mode
    if source.format in {
        "nasdaq_nordic_stockholm_shares_json",
        "nasdaq_nordic_helsinki_shares_json",
        "nasdaq_nordic_iceland_shares_json",
        "nasdaq_nordic_copenhagen_shares_json",
    }:
        rows, mode = load_nasdaq_nordic_stockholm_shares_rows(source, session=session)
        if rows is None:
            raise requests.RequestException(f"Nasdaq Nordic shares unavailable for {source.key}")
        return rows, mode
    if source.format in {
        "nasdaq_nordic_stockholm_shares_search_json",
        "nasdaq_nordic_helsinki_shares_search_json",
        "nasdaq_nordic_copenhagen_shares_search_json",
    }:
        rows, mode = load_nasdaq_nordic_share_search_rows(source, session=session)
        if rows is None:
            raise requests.RequestException(f"Nasdaq Nordic share search unavailable for {source.key}")
        return rows, mode
    if source.format == "spotlight_companies_search_json":
        rows, mode = load_spotlight_search_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("Spotlight company search unavailable")
        return rows, mode
    if source.format == "spotlight_companies_directory_json":
        rows, mode = load_spotlight_directory_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("Spotlight company directory unavailable")
        return rows, mode
    if source.format == "ngm_companies_page_html":
        rows, mode = load_ngm_companies_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("NGM companies page unavailable")
        return rows, mode
    if source.format == "ngm_market_data_equities_json":
        rows, mode = load_ngm_market_data_equity_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("NGM market-data equities unavailable")
        return rows, mode
    if source.format in {
        "nasdaq_nordic_stockholm_etfs_json",
        "nasdaq_nordic_helsinki_etfs_json",
        "nasdaq_nordic_copenhagen_etfs_json",
    }:
        rows, mode = load_nasdaq_nordic_stockholm_etf_rows(source, session=session)
        if rows is None:
            raise requests.RequestException(f"Nasdaq Nordic ETFs unavailable for {source.key}")
        return rows, mode
    if source.format == "nasdaq_nordic_copenhagen_etf_search_json":
        rows, mode = load_nasdaq_nordic_etf_search_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("Nasdaq Nordic Copenhagen ETF search unavailable")
        return rows, mode
    if source.format == "nasdaq_nordic_stockholm_trackers_json":
        rows, mode = load_nasdaq_nordic_stockholm_tracker_rows(source, session=session)
        if rows is None:
            raise requests.RequestException("Nasdaq Nordic Stockholm trackers unavailable")
        return rows, mode
    if source.format == "tmx_listed_issuers_excel":
        content, mode = load_tmx_listed_issuers_content(session=session)
        if content is None:
            raise requests.RequestException("TMX listed issuers workbook unavailable")
        rows = parse_tmx_listed_issuers_excel(content, source)
        supplemental_rows = fetch_tmx_stock_quote_rows(rows, source, session=session)
        if supplemental_rows:
            rows.extend(supplemental_rows)
            mode = "network"
        return rows, mode
    if source.format == "tmx_etf_screener_json":
        payload, mode = load_tmx_etf_screener_payload(session=session)
        if payload is None:
            raise requests.RequestException("TMX ETF screener dataset unavailable")
        return parse_tmx_etf_screener(payload, source), mode
    return fetch_source_rows(source, session=session), "network"


def normalized_reference_row(row: dict[str, Any]) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for key, value in row.items():
        if value is None:
            normalized[key] = ""
        elif isinstance(value, str):
            normalized[key] = value
        else:
            normalized[key] = str(value)
    return normalized


def dedupe_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    deduped: dict[tuple[str, str, str, str, str], dict[str, str]] = {}
    for raw_row in rows:
        row = normalized_reference_row(raw_row)
        key = (
            row.get("source_key", ""),
            row.get("ticker", ""),
            row.get("exchange", ""),
            row.get("listing_status", ""),
            row.get("reference_scope", "exchange_directory"),
        )
        deduped[key] = row
    return sorted(
        deduped.values(),
        key=lambda row: (
            row.get("exchange", ""),
            row.get("ticker", ""),
            row.get("source_key", ""),
        ),
    )


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def build_summary(
    rows: list[dict[str, str]],
    source_modes: dict[str, str] | None = None,
    generated_at: str | None = None,
    source_metadata_overrides: dict[str, dict[str, Any]] | None = None,
    refreshed_source_keys: Iterable[str] | None = None,
) -> dict[str, Any]:
    exchanges = sorted({row["exchange"] for row in rows if row["exchange"]})
    source_counts: dict[str, int] = {}
    for row in rows:
        source_counts[row["source_key"]] = source_counts.get(row["source_key"], 0) + 1
    source_metadata_overrides = source_metadata_overrides or {}
    source_modes = source_modes or {}
    refreshed = set(refreshed_source_keys or source_modes.keys() or source_counts.keys())
    source_details = {
        source.key: {
            "provider": source.provider,
            "reference_scope": source.reference_scope,
            "official": source.official,
            "mode": source_modes.get(source.key, source_metadata_overrides.get(source.key, {}).get("mode", "unknown")),
            "rows": source_counts.get(source.key, 0),
            "generated_at": (
                generated_at or ""
                if source.key in refreshed
                else str(source_metadata_overrides.get(source.key, {}).get("generated_at", ""))
            ),
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
    sources: Iterable[MasterfileSource] | None = None,
) -> tuple[list[dict[str, str]], dict[str, Any]]:
    ensure_output_dirs()
    session = session or requests.Session()
    rows: list[dict[str, str]] = []
    errors: list[dict[str, str]] = []
    source_modes: dict[str, str] = {}
    generated_at = utc_now_iso()
    selected_sources = list(sources or OFFICIAL_SOURCES)
    for source in selected_sources:
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


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch official exchange masterfiles into data/masterfiles/reference.csv.")
    parser.add_argument(
        "--source",
        action="append",
        dest="sources",
        help="Refresh only selected source key(s). Repeat the flag or pass a comma-separated list.",
    )
    parser.add_argument(
        "--manual-dir",
        type=Path,
        default=MASTERFILES_DIR / "manual",
        help="Directory containing manual supplement CSVs.",
    )
    parser.add_argument(
        "--no-manual",
        action="store_true",
        help="Skip loading manual supplement CSVs.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    try:
        selected_sources = select_official_sources(args.sources)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc

    rows, summary = fetch_all_sources(
        include_manual=not args.no_manual,
        manual_dir=args.manual_dir,
        sources=selected_sources,
    )
    selected_source_keys = {source.key for source in selected_sources}
    if args.sources and MASTERFILE_REFERENCE_CSV.exists():
        existing_rows = load_csv(MASTERFILE_REFERENCE_CSV)
        rows = merge_reference_rows(existing_rows, rows, source_keys=selected_source_keys)
        existing_summary: dict[str, Any] = {}
        if MASTERFILE_SUMMARY_JSON.exists():
            existing_summary = json.loads(MASTERFILE_SUMMARY_JSON.read_text(encoding="utf-8"))
        merged_source_modes = dict(existing_summary.get("source_modes", {}))
        merged_source_modes.update(summary.get("source_modes", {}))
        errors = summary.get("errors")
        summary = build_summary(
            rows,
            source_modes=merged_source_modes,
            generated_at=summary.get("generated_at"),
            source_metadata_overrides=existing_summary.get("source_details", {}),
            refreshed_source_keys=selected_source_keys,
        )
        if errors:
            summary["errors"] = errors
    persist_source_metadata()
    write_csv(
        MASTERFILE_REFERENCE_CSV,
        MASTERFILE_FIELDNAMES,
        rows,
    )
    MASTERFILE_SUMMARY_JSON.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps({"reference_csv": str(MASTERFILE_REFERENCE_CSV.relative_to(ROOT)), **summary}, indent=2))


if __name__ == "__main__":
    main()

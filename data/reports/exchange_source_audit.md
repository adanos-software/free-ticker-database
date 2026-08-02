# Exchange Source Audit

Generated at: `2026-08-02T14:15:22Z`

- Venues: `81`
- Venue status: `{"official_full": 48, "official_partial": 33}`
- Audit outcomes: `{"denominator_missing": 26, "maintain": 13, "refresh_required": 28, "refresh_unavailable": 14}`

| Exchange | Status | Sources | Missing products | Denominator | Recall | Nonfresh | Outcome | Promotion |
|---|---|---|---|---:|---:|---|---|---|
| ADX | official_full | adx_market_watch |  | 123 | 69.11 |  | maintain | not_applicable |
| AMS | official_full | euronext_equities|euronext_etfs |  | 601 | 40.1 | euronext_equities|euronext_etfs | refresh_unavailable | not_applicable |
| ASX | official_partial | asx_investment_products|asx_listed_companies |  | 0 |  | asx_listed_companies | refresh_unavailable | blocked_source_unavailable |
| ATHEX | official_partial | athex_sector_classification | ETF | 0 |  |  | denominator_missing | blocked_product_class_gap |
| B3 | official_full | b3_bdr_etfs|b3_instruments_equities|b3_listed_etfs |  | 1327 | 91.94 |  | maintain | not_applicable |
| BATS | official_full | nasdaq_other_listed|nasdaq_trading_system_adds_deletes |  | 1559 | 78.45 | nasdaq_trading_system_adds_deletes | refresh_required | not_applicable |
| BCBA | official_partial | byma_equity_details |  | 0 |  | byma_equity_details | denominator_missing | blocked_nonfresh_source |
| BHB | official_full | bahrain_bourse_listed_companies |  | 41 | 68.29 |  | maintain | not_applicable |
| BIST | official_full | bist_kap_mkk_listed_securities |  | 647 | 94.44 |  | maintain | not_applicable |
| BK | official_full | boursa_kuwait_stocks |  | 140 | 72.86 | boursa_kuwait_stocks | refresh_required | not_applicable |
| BME | official_full | bme_etf_list|bme_listed_companies|bme_security_prices_directory |  | 50 | 24.0 |  | maintain | not_applicable |
| BMV | official_partial | bmv_capital_trust_search|bmv_etf_search|bmv_market_data_securities|bmv_stock_search |  | 0 |  |  | denominator_missing | blocked_denominator_missing |
| BSE_BW | official_partial | bse_bw_listed_companies | ETF | 0 |  | bse_bw_listed_companies | refresh_unavailable | blocked_source_unavailable |
| BSE_HU | official_partial | bse_hu_listed_companies | ETF | 0 |  | bse_hu_listed_companies | denominator_missing | blocked_nonfresh_source |
| BSE_IN | official_full | bse_india_scrips |  | 5077 | 50.74 | bse_india_scrips | refresh_required | not_applicable |
| BVB | official_full | bvb_fund_units_directory|bvb_shares_directory |  | 350 | 21.43 | bvb_fund_units_directory|bvb_shares_directory | refresh_required | not_applicable |
| BVC | official_partial | bvc_colombia_issuers |  | 0 |  | bvc_colombia_issuers | refresh_unavailable | blocked_source_unavailable |
| BVL | official_partial | bvl_issuers_directory |  | 0 |  | bvl_issuers_directory | denominator_missing | blocked_nonfresh_source |
| Borsa Italiana | official_full | euronext_equities|euronext_etfs |  | 2893 | 8.75 | euronext_equities|euronext_etfs | refresh_unavailable | not_applicable |
| Bursa | official_partial | bursa_closing_prices|bursa_equity_isin |  | 0 |  | bursa_closing_prices|bursa_equity_isin | refresh_unavailable | blocked_source_unavailable |
| CPH | official_partial | nasdaq_nordic_copenhagen_etfs|nasdaq_nordic_copenhagen_shares |  | 0 |  | nasdaq_nordic_copenhagen_etfs|nasdaq_nordic_copenhagen_shares | denominator_missing | blocked_nonfresh_source |
| CSE_LK | official_full | cse_lk_all_security_code|cse_lk_company_info_summary |  | 318 | 96.54 | cse_lk_all_security_code|cse_lk_company_info_summary | refresh_required | not_applicable |
| CSE_MA | official_full | cse_ma_listed_companies |  | 82 | 1.22 | cse_ma_listed_companies | refresh_required | not_applicable |
| DFM | official_full | dfm_listed_securities |  | 71 | 63.38 | dfm_listed_securities | refresh_required | not_applicable |
| DSE_TZ | official_partial | dse_tz_listed_companies |  | 0 |  | dse_tz_listed_companies | denominator_missing | blocked_nonfresh_source |
| EGX | official_partial | egx_listed_stocks |  | 0 |  | egx_listed_stocks | denominator_missing | blocked_nonfresh_source |
| Euronext | official_full | euronext_equities|euronext_etfs |  | 2007 | 48.08 | euronext_equities|euronext_etfs | refresh_unavailable | not_applicable |
| GSE | official_partial | gse_listed_companies |  | 0 |  | gse_listed_companies | denominator_missing | blocked_nonfresh_source |
| HEL | official_partial | nasdaq_nordic_helsinki_etfs|nasdaq_nordic_helsinki_shares |  | 0 |  | nasdaq_nordic_helsinki_etfs|nasdaq_nordic_helsinki_shares | denominator_missing | blocked_nonfresh_source |
| HKEX | official_full | hkex_securities_list |  | 3206 | 94.42 | hkex_securities_list | refresh_required | not_applicable |
| HNX | official_full | hnx_listed_securities |  | 299 | 34.78 | hnx_listed_securities | refresh_required | not_applicable |
| HOSE | official_partial | hose_etf_list|hose_fund_certificate_list|hose_listed_stocks |  | 0 |  | hose_etf_list|hose_fund_certificate_list|hose_listed_stocks | refresh_unavailable | blocked_source_unavailable |
| ICE_IS | official_partial | nasdaq_nordic_iceland_shares |  | 0 |  | nasdaq_nordic_iceland_shares | denominator_missing | blocked_nonfresh_source |
| IDX | official_full | idx_company_profiles|idx_listed_companies |  | 963 | 72.07 | idx_company_profiles|idx_listed_companies | refresh_required | not_applicable |
| ISE | official_full | euronext_equities |  | 15 | 60.0 | euronext_equities | refresh_unavailable | not_applicable |
| JSE | official_partial | jse_etf_list|jse_etn_list | Stock | 0 |  | jse_etf_list|jse_etn_list | denominator_missing | blocked_nonfresh_source |
| KOSDAQ | official_full | krx_listed_companies |  | 1818 | 86.36 | krx_listed_companies | refresh_required | not_applicable |
| KRX | official_full | krx_etf_finder|krx_listed_companies |  | 2097 | 84.31 | krx_etf_finder|krx_listed_companies | refresh_required | not_applicable |
| LSE | official_full | lse_company_reports|lse_instrument_directory|lse_price_explorer |  | 11092 | 57.59 | lse_company_reports|lse_instrument_directory|lse_price_explorer | refresh_unavailable | not_applicable |
| LUSE | official_partial | luse_listed_companies |  | 0 |  | luse_listed_companies | denominator_missing | blocked_nonfresh_source |
| MSE_MW | official_partial | mse_mw_listed_companies |  | 0 |  | mse_mw_listed_companies | denominator_missing | blocked_nonfresh_source |
| MSX | official_full | muscat_securities_companies |  | 108 | 84.26 | muscat_securities_companies | refresh_required | not_applicable |
| NASDAQ | official_full | nasdaq_listed|nasdaq_trading_system_adds_deletes|sec_company_tickers_exchange |  | 5739 | 79.51 | nasdaq_trading_system_adds_deletes|sec_company_tickers_exchange | refresh_required | not_applicable |
| NEO | official_full | cboe_canada_listing_directory |  | 440 | 41.59 | cboe_canada_listing_directory | refresh_required | not_applicable |
| NGX | official_full | ngx_company_profile_directory|ngx_equities_price_list | ETF | 133 | 100.0 | ngx_company_profile_directory|ngx_equities_price_list | refresh_required | not_applicable |
| NMFQS | official_partial | nasdaq_mutual_fund_quotes |  | 0 |  | nasdaq_mutual_fund_quotes | denominator_missing | blocked_nonfresh_source |
| NSE_IN | official_full | nse_india_securities_available |  | 3010 | 78.7 | nse_india_securities_available | refresh_required | not_applicable |
| NSE_KE | official_full | nse_ke_listed_companies |  | 66 | 16.67 | nse_ke_listed_companies | refresh_required | not_applicable |
| NYSE | official_full | nasdaq_other_listed|sec_company_tickers_exchange |  | 3917 | 50.83 | sec_company_tickers_exchange | refresh_required | not_applicable |
| NYSE ARCA | official_full | nasdaq_other_listed|nasdaq_trading_system_adds_deletes |  | 2697 | 95.29 | nasdaq_trading_system_adds_deletes | refresh_required | not_applicable |
| NYSE MKT | official_full | nasdaq_other_listed | ETF | 308 | 72.73 |  | maintain | not_applicable |
| NZX | official_full | nzx_instruments |  | 173 | 26.01 | nzx_instruments | refresh_required | not_applicable |
| OSL | official_full | euronext_equities|euronext_etfs |  | 298 | 82.21 | euronext_equities|euronext_etfs | refresh_unavailable | not_applicable |
| OTC | official_full | otc_markets_security_profile|otc_markets_stock_screener|sec_company_tickers_exchange |  | 11925 | 64.37 | otc_markets_security_profile|otc_markets_stock_screener|sec_company_tickers_exchange | refresh_required | not_applicable |
| PSE | official_full | pse_listed_company_directory |  | 381 | 23.1 | pse_listed_company_directory | refresh_required | not_applicable |
| PSE_CZ | official_partial | pse_cz_shares_directory |  | 0 |  | pse_cz_shares_directory | denominator_missing | blocked_nonfresh_source |
| PSX | official_full | psx_dps_symbols|psx_listed_companies|psx_symbol_name_daily |  | 716 | 51.82 | psx_dps_symbols|psx_listed_companies|psx_symbol_name_daily | refresh_required | not_applicable |
| QSE | official_full | qse_market_watch |  | 57 | 94.74 | qse_market_watch | refresh_required | not_applicable |
| RSE | official_partial | rse_listed_companies |  | 0 |  | rse_listed_companies | denominator_missing | blocked_nonfresh_source |
| SEM | official_full | sem_isin |  | 47 | 97.87 | sem_isin | refresh_required | not_applicable |
| SET | official_full | set_dr_search|set_etf_search|set_listed_companies|set_stock_search |  | 944 | 57.52 |  | maintain | not_applicable |
| SGX | official_full | sgx_securities_prices |  | 746 | 78.95 |  | maintain | not_applicable |
| SIX | official_partial | six_equity_issuers|six_etf_products|six_etp_products |  | 0 |  |  | denominator_missing | blocked_denominator_missing |
| SSE | official_partial | sse_a_share_list|sse_etf_list |  | 0 |  |  | denominator_missing | blocked_denominator_missing |
| SSE_CL | official_full | bolsa_santiago_instruments |  | 111 | 100.0 | bolsa_santiago_instruments | refresh_unavailable | not_applicable |
| STO | official_partial | nasdaq_nordic_stockholm_etfs|nasdaq_nordic_stockholm_shares|nasdaq_nordic_stockholm_trackers|ngm_companies_page|ngm_market_data_equities|spotlight_companies_directory |  | 0 |  | nasdaq_nordic_stockholm_etfs|nasdaq_nordic_stockholm_shares|nasdaq_nordic_stockholm_trackers|ngm_companies_page|ngm_market_data_equities | denominator_missing | blocked_nonfresh_source |
| SZSE | official_partial | szse_a_share_list|szse_b_share_list|szse_etf_list |  | 0 |  | szse_a_share_list | refresh_unavailable | blocked_source_unavailable |
| TADAWUL | official_full | tadawul_main_market_watch |  | 412 | 46.12 |  | maintain | not_applicable |
| TASE | official_partial | tase_etf_marketdata|tase_foreign_etf_search|tase_participating_unit_search|tase_securities_marketdata |  | 0 |  | tase_foreign_etf_search|tase_participating_unit_search | refresh_unavailable | blocked_source_unavailable |
| TPEX | official_partial | tpex_emerging_basic_info|tpex_etf_filter|tpex_mainboard_basic_info|tpex_mainboard_daily_quotes |  | 0 |  |  | denominator_missing | blocked_denominator_missing |
| TSE | official_full | jpx_listed_issues|jpx_tse_stock_detail |  | 4437 | 90.96 | jpx_listed_issues|jpx_tse_stock_detail | refresh_required | not_applicable |
| TSX | official_full | tmx_etf_screener|tmx_interlisted_companies|tmx_listed_issuers |  | 785 | 41.15 |  | maintain | not_applicable |
| TSXV | official_full | tmx_interlisted_companies|tmx_listed_issuers |  | 1518 | 63.31 |  | maintain | not_applicable |
| TWSE | official_full | twse_etf_list|twse_listed_companies |  | 1093 | 88.93 |  | maintain | not_applicable |
| UPCOM | official_full | upcom_registered_securities |  | 822 | 0.24 |  | maintain | not_applicable |
| USE_UG | official_partial | use_ug_listed_companies |  | 0 |  |  | denominator_missing | blocked_denominator_missing |
| VSE | official_partial | vienna_listed_companies | ETF | 0 |  |  | denominator_missing | blocked_product_class_gap |
| WSE | official_partial | newconnect_listed_companies|wse_etf_list|wse_listed_companies |  | 0 |  | newconnect_listed_companies | denominator_missing | blocked_nonfresh_source |
| XETRA | official_full | deutsche_boerse_etfs_etps|deutsche_boerse_listed_companies|deutsche_boerse_xetra_all_tradable_equities |  | 5080 | 71.91 | deutsche_boerse_etfs_etps|deutsche_boerse_listed_companies|deutsche_boerse_xetra_all_tradable_equities | refresh_required | not_applicable |
| ZSE | official_partial | zagreb_securities_directory |  | 0 |  |  | denominator_missing | blocked_denominator_missing |
| ZSE_ZW | official_partial | zse_zw_listed_companies |  | 0 |  |  | denominator_missing | blocked_denominator_missing |

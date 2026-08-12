# Exchange Source Audit

Generated at: `2026-08-12T08:03:38Z`

- Venues: `87`
- Venue status: `{"missing": 6, "official_full": 48, "official_partial": 33}`
- Audit outcomes: `{"denominator_missing": 26, "maintain": 1, "official_source_required": 6, "refresh_required": 43, "refresh_unavailable": 11}`

| Exchange | Status | Sources | Missing products | Denominator | Recall | Nonfresh | Outcome | Promotion |
|---|---|---|---|---:|---:|---|---|---|
| ADX | official_full | adx_market_watch |  | 123 | 69.11 | adx_market_watch | refresh_required | not_applicable |
| AMS | official_full | euronext_equities|euronext_etfs |  | 602 | 62.13 | euronext_equities|euronext_etfs | refresh_required | not_applicable |
| ASX | official_partial | asx_investment_products|asx_listed_companies |  | 0 |  | asx_investment_products|asx_listed_companies | refresh_unavailable | blocked_source_unavailable |
| ATHEX | official_partial | athex_sector_classification | ETF | 0 |  | athex_sector_classification | denominator_missing | blocked_nonfresh_source |
| B3 | official_full | b3_bdr_etfs|b3_instruments_equities|b3_listed_etfs |  | 1327 | 91.94 | b3_bdr_etfs|b3_instruments_equities|b3_listed_etfs | refresh_required | not_applicable |
| BATS | official_full | nasdaq_other_listed|nasdaq_trading_system_adds_deletes |  | 1591 | 78.44 | nasdaq_trading_system_adds_deletes | refresh_required | not_applicable |
| BCBA | official_partial | byma_equity_details |  | 0 |  | byma_equity_details | denominator_missing | blocked_nonfresh_source |
| BHB | official_full | bahrain_bourse_listed_companies |  | 41 | 68.29 | bahrain_bourse_listed_companies | refresh_required | not_applicable |
| BIST | official_full | bist_kap_mkk_listed_securities |  | 647 | 94.44 | bist_kap_mkk_listed_securities | refresh_required | not_applicable |
| BK | official_full | boursa_kuwait_stocks |  | 140 | 72.86 | boursa_kuwait_stocks | refresh_required | not_applicable |
| BME | official_full | bme_etf_list|bme_listed_companies|bme_security_prices_directory |  | 50 | 36.0 | bme_etf_list|bme_listed_companies|bme_security_prices_directory | refresh_required | not_applicable |
| BMV | official_partial | bmv_capital_trust_search|bmv_etf_search|bmv_market_data_securities|bmv_stock_search |  | 0 |  | bmv_capital_trust_search|bmv_etf_search|bmv_market_data_securities|bmv_stock_search | denominator_missing | blocked_nonfresh_source |
| BSE_BW | official_partial | bse_bw_listed_companies | ETF | 0 |  | bse_bw_listed_companies | refresh_unavailable | blocked_source_unavailable |
| BSE_HU | official_partial | bse_hu_listed_companies | ETF | 0 |  | bse_hu_listed_companies | denominator_missing | blocked_nonfresh_source |
| BSE_IN | official_full | bse_india_scrips |  | 5077 | 52.61 | bse_india_scrips | refresh_required | not_applicable |
| BVB | official_full | bvb_fund_units_directory|bvb_shares_directory |  | 350 | 24.86 | bvb_fund_units_directory|bvb_shares_directory | refresh_required | not_applicable |
| BVC | official_partial | bvc_colombia_issuers |  | 0 |  | bvc_colombia_issuers | refresh_unavailable | blocked_source_unavailable |
| BVL | official_partial | bvl_issuers_directory |  | 0 |  | bvl_issuers_directory | denominator_missing | blocked_nonfresh_source |
| Borsa Italiana | official_full | euronext_equities|euronext_etfs |  | 2898 | 8.66 | euronext_equities|euronext_etfs | refresh_required | not_applicable |
| Bursa | official_partial | bursa_closing_prices|bursa_equity_isin |  | 0 |  | bursa_closing_prices|bursa_equity_isin | refresh_unavailable | blocked_source_unavailable |
| CPH | official_partial | nasdaq_nordic_copenhagen_etfs|nasdaq_nordic_copenhagen_shares |  | 0 |  | nasdaq_nordic_copenhagen_etfs|nasdaq_nordic_copenhagen_shares | denominator_missing | blocked_nonfresh_source |
| CSE_LK | official_full | cse_lk_all_security_code|cse_lk_company_info_summary |  | 318 | 96.54 | cse_lk_all_security_code|cse_lk_company_info_summary | refresh_required | not_applicable |
| CSE_MA | official_full | cse_ma_listed_companies |  | 82 | 1.22 | cse_ma_listed_companies | refresh_required | not_applicable |
| DFM | official_full | dfm_listed_securities |  | 71 | 63.38 | dfm_listed_securities | refresh_required | not_applicable |
| DSE_TZ | official_partial | dse_tz_listed_companies |  | 0 |  | dse_tz_listed_companies | denominator_missing | blocked_nonfresh_source |
| EGX | official_partial | egx_listed_stocks |  | 0 |  | egx_listed_stocks | denominator_missing | blocked_nonfresh_source |
| Euronext | official_full | euronext_equities|euronext_etfs |  | 2007 | 66.82 | euronext_equities|euronext_etfs | refresh_required | not_applicable |
| FSX | missing |  | ETF|Stock | 0 |  |  | official_source_required | not_applicable |
| GSE | official_partial | gse_listed_companies |  | 0 |  | gse_listed_companies | denominator_missing | blocked_nonfresh_source |
| HEL | official_partial | nasdaq_nordic_helsinki_etfs|nasdaq_nordic_helsinki_shares |  | 0 |  | nasdaq_nordic_helsinki_etfs|nasdaq_nordic_helsinki_shares | denominator_missing | blocked_nonfresh_source |
| HKEX | official_full | hkex_securities_list |  | 3200 | 94.94 | hkex_securities_list | refresh_required | not_applicable |
| HNX | official_full | hnx_listed_securities |  | 299 | 34.78 | hnx_listed_securities | refresh_required | not_applicable |
| HOSE | official_partial | hose_etf_list|hose_fund_certificate_list|hose_listed_stocks |  | 0 |  | hose_etf_list|hose_fund_certificate_list|hose_listed_stocks | denominator_missing | blocked_nonfresh_source |
| ICE_IS | official_partial | nasdaq_nordic_iceland_shares |  | 0 |  | nasdaq_nordic_iceland_shares | denominator_missing | blocked_nonfresh_source |
| IDX | official_full | idx_company_profiles|idx_listed_companies |  | 962 | 78.59 | idx_company_profiles|idx_listed_companies | refresh_required | not_applicable |
| ISE | official_full | euronext_equities |  | 15 | 60.0 | euronext_equities | refresh_required | not_applicable |
| JSE | official_partial | jse_etf_list|jse_etn_list | Stock | 0 |  | jse_etf_list|jse_etn_list | denominator_missing | blocked_nonfresh_source |
| KOSDAQ | official_full | krx_listed_companies |  | 1817 | 87.84 | krx_listed_companies | refresh_required | not_applicable |
| KRX | official_full | krx_etf_finder|krx_listed_companies |  | 2102 | 93.24 | krx_etf_finder|krx_listed_companies | refresh_required | not_applicable |
| LSE | official_full | lse_company_reports|lse_instrument_directory|lse_price_explorer |  | 11092 | 61.77 | lse_company_reports|lse_instrument_directory|lse_price_explorer | refresh_unavailable | not_applicable |
| LUSE | official_partial | luse_listed_companies |  | 0 |  | luse_listed_companies | denominator_missing | blocked_nonfresh_source |
| MSE_MW | official_partial | mse_mw_listed_companies |  | 0 |  | mse_mw_listed_companies | denominator_missing | blocked_nonfresh_source |
| MSX | official_full | muscat_securities_companies |  | 108 | 84.26 | muscat_securities_companies | refresh_required | not_applicable |
| Munich | missing |  | ETF|Stock | 0 |  |  | official_source_required | not_applicable |
| NASDAQ | official_full | nasdaq_listed|nasdaq_trading_system_adds_deletes|sec_company_tickers_exchange |  | 5606 | 81.64 | nasdaq_trading_system_adds_deletes|sec_company_tickers_exchange | refresh_required | not_applicable |
| NEO | official_full | cboe_canada_listing_directory |  | 440 | 48.41 | cboe_canada_listing_directory | refresh_required | not_applicable |
| NGX | official_full | ngx_company_profile_directory|ngx_equities_price_list | ETF | 130 | 100.0 | ngx_company_profile_directory|ngx_equities_price_list | refresh_required | not_applicable |
| NMFQS | official_partial | nasdaq_mutual_fund_quotes |  | 0 |  | nasdaq_mutual_fund_quotes | denominator_missing | blocked_nonfresh_source |
| NSE_IN | official_full | nse_india_securities_available |  | 2978 | 76.9 | nse_india_securities_available | refresh_required | not_applicable |
| NSE_KE | official_full | nse_ke_listed_companies |  | 68 | 16.18 | nse_ke_listed_companies | refresh_required | not_applicable |
| NYSE | official_full | nasdaq_other_listed|sec_company_tickers_exchange |  | 3899 | 50.94 | sec_company_tickers_exchange | refresh_required | not_applicable |
| NYSE ARCA | official_full | nasdaq_other_listed|nasdaq_trading_system_adds_deletes |  | 2701 | 95.56 | nasdaq_trading_system_adds_deletes | refresh_required | not_applicable |
| NYSE MKT | official_full | nasdaq_other_listed | ETF | 309 | 74.43 |  | maintain | not_applicable |
| NZX | official_full | nzx_instruments |  | 172 | 26.16 | nzx_instruments | refresh_required | not_applicable |
| OSL | official_full | euronext_equities|euronext_etfs |  | 297 | 95.96 | euronext_equities|euronext_etfs | refresh_required | not_applicable |
| OTC | official_full | otc_markets_security_profile|otc_markets_stock_screener|sec_company_tickers_exchange |  | 11925 | 69.33 | otc_markets_security_profile|otc_markets_stock_screener|sec_company_tickers_exchange | refresh_unavailable | not_applicable |
| PSE | official_full | pse_listed_company_directory |  | 385 | 40.26 | pse_listed_company_directory | refresh_required | not_applicable |
| PSE_CZ | official_partial | pse_cz_shares_directory |  | 0 |  | pse_cz_shares_directory | denominator_missing | blocked_nonfresh_source |
| PSX | official_full | psx_dps_symbols|psx_listed_companies|psx_symbol_name_daily |  | 720 | 54.17 | psx_dps_symbols|psx_listed_companies|psx_symbol_name_daily | refresh_required | not_applicable |
| QSE | official_full | qse_market_watch |  | 57 | 96.49 | qse_market_watch | refresh_required | not_applicable |
| RSE | official_partial | rse_listed_companies |  | 0 |  | rse_listed_companies | denominator_missing | blocked_nonfresh_source |
| SEM | official_full | sem_isin |  | 46 | 100.0 | sem_isin | refresh_required | not_applicable |
| SET | official_full | set_dr_search|set_etf_search|set_listed_companies|set_stock_search |  | 944 | 82.1 | set_dr_search|set_etf_search|set_listed_companies|set_stock_search | refresh_required | not_applicable |
| SGX | official_full | sgx_securities_prices |  | 746 | 81.9 | sgx_securities_prices | refresh_required | not_applicable |
| SIX | official_partial | six_equity_issuers|six_etf_products|six_etp_products |  | 0 |  | six_equity_issuers|six_etf_products|six_etp_products | denominator_missing | blocked_nonfresh_source |
| SSE | official_partial | sse_a_share_list|sse_etf_list |  | 0 |  | sse_a_share_list|sse_etf_list | denominator_missing | blocked_nonfresh_source |
| SSE_CL | official_full | bolsa_santiago_instruments |  | 111 | 100.0 | bolsa_santiago_instruments | refresh_unavailable | not_applicable |
| STO | official_partial | nasdaq_nordic_stockholm_etfs|nasdaq_nordic_stockholm_shares|nasdaq_nordic_stockholm_trackers|ngm_companies_page|ngm_market_data_equities|spotlight_companies_directory |  | 0 |  | nasdaq_nordic_stockholm_etfs|nasdaq_nordic_stockholm_shares|nasdaq_nordic_stockholm_trackers|ngm_companies_page|ngm_market_data_equities|spotlight_companies_directory | refresh_unavailable | blocked_source_unavailable |
| SZSE | official_partial | szse_a_share_list|szse_b_share_list|szse_etf_list |  | 0 |  | szse_a_share_list|szse_b_share_list|szse_etf_list | refresh_unavailable | blocked_source_unavailable |
| TADAWUL | official_full | tadawul_main_market_watch |  | 412 | 48.06 | tadawul_main_market_watch | refresh_required | not_applicable |
| TASE | official_partial | tase_etf_marketdata|tase_foreign_etf_search|tase_participating_unit_search|tase_securities_marketdata |  | 0 |  | tase_etf_marketdata|tase_foreign_etf_search|tase_participating_unit_search|tase_securities_marketdata | refresh_unavailable | blocked_source_unavailable |
| TPEX | official_partial | tpex_emerging_basic_info|tpex_etf_filter|tpex_mainboard_basic_info|tpex_mainboard_daily_quotes |  | 0 |  | tpex_emerging_basic_info|tpex_etf_filter|tpex_mainboard_basic_info|tpex_mainboard_daily_quotes | denominator_missing | blocked_nonfresh_source |
| TSE | official_full | jpx_listed_issues|jpx_tse_stock_detail |  | 4437 | 91.35 | jpx_listed_issues|jpx_tse_stock_detail | refresh_unavailable | not_applicable |
| TSX | official_full | tmx_etf_screener|tmx_interlisted_companies|tmx_listed_issuers |  | 785 | 75.16 | tmx_etf_screener|tmx_interlisted_companies|tmx_listed_issuers | refresh_required | not_applicable |
| TSXV | official_full | tmx_interlisted_companies|tmx_listed_issuers |  | 1518 | 87.09 | tmx_interlisted_companies|tmx_listed_issuers | refresh_required | not_applicable |
| TWSE | official_full | twse_etf_list|twse_listed_companies |  | 1093 | 92.86 | twse_etf_list|twse_listed_companies | refresh_required | not_applicable |
| UPCOM | official_full | upcom_registered_securities |  | 822 | 0.24 | upcom_registered_securities | refresh_required | not_applicable |
| USE_UG | official_partial | use_ug_listed_companies |  | 0 |  | use_ug_listed_companies | denominator_missing | blocked_nonfresh_source |
| VSE | official_partial | vienna_listed_companies | ETF | 0 |  | vienna_listed_companies | denominator_missing | blocked_nonfresh_source |
| WSE | official_partial | newconnect_listed_companies|wse_etf_list|wse_listed_companies |  | 0 |  | newconnect_listed_companies|wse_etf_list|wse_listed_companies | denominator_missing | blocked_nonfresh_source |
| XDUS | missing |  | ETF|Stock | 0 |  |  | official_source_required | not_applicable |
| XETRA | official_full | deutsche_boerse_etfs_etps|deutsche_boerse_listed_companies|deutsche_boerse_xetra_all_tradable_equities |  | 5080 | 80.96 | deutsche_boerse_etfs_etps|deutsche_boerse_listed_companies|deutsche_boerse_xetra_all_tradable_equities | refresh_required | not_applicable |
| XHAM | missing |  | ETF|Stock | 0 |  |  | official_source_required | not_applicable |
| XHAN | missing |  | ETF|Stock | 0 |  |  | official_source_required | not_applicable |
| XSTU | missing |  | ETF|Stock | 0 |  |  | official_source_required | not_applicable |
| ZSE | official_partial | zagreb_securities_directory |  | 0 |  | zagreb_securities_directory | denominator_missing | blocked_nonfresh_source |
| ZSE_ZW | official_partial | zse_zw_listed_companies |  | 0 |  | zse_zw_listed_companies | denominator_missing | blocked_nonfresh_source |

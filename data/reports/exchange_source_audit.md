# Exchange Source Audit

Generated at: `2026-08-19T17:08:09Z`

- Venues: `87`
- Venue status: `{"missing": 5, "official_full": 49, "official_partial": 33}`
- Audit outcomes: `{"denominator_missing": 24, "maintain": 45, "official_source_required": 5, "refresh_unavailable": 13}`

| Exchange | Status | Sources | Missing products | Denominator | Recall | Nonfresh | Outcome | Promotion |
|---|---|---|---|---:|---:|---|---|---|
| ADX | official_full | adx_market_watch |  | 123 | 69.11 |  | maintain | not_applicable |
| AMS | official_full | euronext_equities|euronext_etfs |  | 604 | 61.92 |  | maintain | not_applicable |
| ASX | official_partial | asx_investment_products|asx_listed_companies |  | 0 |  | asx_listed_companies | refresh_unavailable | blocked_source_unavailable |
| ATHEX | official_partial | athex_sector_classification | ETF | 0 |  |  | denominator_missing | blocked_product_class_gap |
| B3 | official_full | b3_bdr_etfs|b3_instruments_equities|b3_listed_etfs |  | 1294 | 93.66 |  | maintain | not_applicable |
| BATS | official_full | nasdaq_other_listed|nasdaq_trading_system_adds_deletes |  | 1603 | 78.73 |  | maintain | not_applicable |
| BCBA | official_partial | byma_equity_details |  | 0 |  |  | denominator_missing | blocked_denominator_missing |
| BHB | official_full | bahrain_bourse_listed_companies |  | 41 | 68.29 |  | maintain | not_applicable |
| BIST | official_full | bist_kap_mkk_listed_securities |  | 652 | 93.87 |  | maintain | not_applicable |
| BK | official_full | boursa_kuwait_stocks |  | 140 | 72.14 |  | maintain | not_applicable |
| BME | official_full | bme_etf_list|bme_listed_companies|bme_security_prices_directory |  | 50 | 36.0 | bme_security_prices_directory | refresh_unavailable | not_applicable |
| BMV | official_partial | bmv_capital_trust_search|bmv_etf_search|bmv_issuer_directory|bmv_market_data_securities|bmv_stock_search |  | 0 |  | bmv_capital_trust_search|bmv_market_data_securities|bmv_stock_search | refresh_unavailable | blocked_source_unavailable |
| BSE_BW | official_partial | bse_bw_listed_companies | ETF | 0 |  |  | denominator_missing | blocked_product_class_gap |
| BSE_HU | official_partial | bse_hu_listed_companies | ETF | 0 |  |  | denominator_missing | blocked_product_class_gap |
| BSE_IN | official_full | bse_india_scrips |  | 4970 | 51.01 |  | maintain | not_applicable |
| BVB | official_full | bvb_fund_units_directory|bvb_shares_directory |  | 350 | 24.86 |  | maintain | not_applicable |
| BVC | official_partial | bvc_colombia_issuers |  | 0 |  | bvc_colombia_issuers | refresh_unavailable | blocked_source_unavailable |
| BVL | official_partial | bvl_issuers_directory |  | 0 |  |  | denominator_missing | blocked_denominator_missing |
| Borsa Italiana | official_full | euronext_equities|euronext_etfs |  | 2903 | 8.61 |  | maintain | not_applicable |
| Bursa | official_partial | bursa_closing_prices|bursa_equity_isin |  | 0 |  | bursa_closing_prices|bursa_equity_isin | refresh_unavailable | blocked_source_unavailable |
| CPH | official_partial | nasdaq_nordic_copenhagen_etfs|nasdaq_nordic_copenhagen_shares |  | 0 |  |  | denominator_missing | blocked_denominator_missing |
| CSE_LK | official_full | cse_lk_all_security_code|cse_lk_company_info_summary |  | 319 | 95.92 |  | maintain | not_applicable |
| CSE_MA | official_full | cse_ma_listed_companies |  | 82 | 1.22 | cse_ma_listed_companies | refresh_unavailable | not_applicable |
| DFM | official_full | dfm_listed_securities |  | 71 | 63.38 |  | maintain | not_applicable |
| DSE_TZ | official_partial | dse_tz_listed_companies |  | 0 |  |  | denominator_missing | blocked_denominator_missing |
| EGX | official_partial | egx_listed_stocks |  | 0 |  |  | denominator_missing | blocked_denominator_missing |
| Euronext | official_full | euronext_equities|euronext_etfs |  | 2014 | 66.58 |  | maintain | not_applicable |
| FSX | official_full | deutsche_boerse_frankfurt_all_tradable_equities |  | 18025 | 44.42 |  | maintain | not_applicable |
| GSE | official_partial | gse_listed_companies |  | 0 |  |  | denominator_missing | blocked_denominator_missing |
| HEL | official_partial | nasdaq_nordic_helsinki_etfs|nasdaq_nordic_helsinki_shares |  | 0 |  |  | denominator_missing | blocked_denominator_missing |
| HKEX | official_full | hkex_securities_list |  | 3197 | 95.03 |  | maintain | not_applicable |
| HNX | official_full | hnx_listed_securities |  | 299 | 34.78 |  | maintain | not_applicable |
| HOSE | official_partial | hose_etf_list|hose_fund_certificate_list|hose_listed_stocks |  | 0 |  |  | denominator_missing | blocked_denominator_missing |
| ICE_IS | official_partial | nasdaq_nordic_iceland_shares |  | 0 |  |  | denominator_missing | blocked_denominator_missing |
| IDX | official_full | idx_company_profiles|idx_listed_companies |  | 962 | 78.59 |  | maintain | not_applicable |
| ISE | official_full | euronext_equities |  | 15 | 60.0 |  | maintain | not_applicable |
| JSE | official_partial | jse_etf_list|jse_etn_list | Stock | 0 |  |  | denominator_missing | blocked_product_class_gap |
| KOSDAQ | official_full | krx_listed_companies |  | 1818 | 87.73 |  | maintain | not_applicable |
| KRX | official_full | krx_etf_finder|krx_listed_companies |  | 2103 | 93.11 |  | maintain | not_applicable |
| LSE | official_full | lse_company_reports|lse_instrument_directory|lse_price_explorer |  | 11107 | 61.6 | lse_company_reports|lse_instrument_directory | refresh_unavailable | not_applicable |
| LUSE | official_partial | luse_listed_companies |  | 0 |  | luse_listed_companies | refresh_unavailable | blocked_source_unavailable |
| MSE_MW | official_partial | mse_mw_listed_companies |  | 0 |  | mse_mw_listed_companies | refresh_unavailable | blocked_source_unavailable |
| MSX | official_full | muscat_securities_companies |  | 108 | 84.26 |  | maintain | not_applicable |
| Munich | missing |  | ETF|Stock | 0 |  |  | official_source_required | not_applicable |
| NASDAQ | official_full | nasdaq_listed|nasdaq_trading_system_adds_deletes|sec_company_tickers_exchange |  | 5608 | 81.51 |  | maintain | not_applicable |
| NEO | official_full | cboe_canada_listing_directory |  | 444 | 47.52 |  | maintain | not_applicable |
| NGX | official_full | ngx_company_profile_directory|ngx_equities_price_list | ETF | 130 | 100.0 |  | maintain | not_applicable |
| NMFQS | official_partial | nasdaq_mutual_fund_quotes |  | 0 |  |  | denominator_missing | blocked_denominator_missing |
| NSE_IN | official_full | nse_india_securities_available |  | 3202 | 72.8 |  | maintain | not_applicable |
| NSE_KE | official_full | nse_ke_listed_companies |  | 68 | 16.18 |  | maintain | not_applicable |
| NYSE | official_full | nasdaq_other_listed|nasdaq_trading_system_adds_deletes|sec_company_tickers_exchange |  | 3891 | 50.96 |  | maintain | not_applicable |
| NYSE ARCA | official_full | nasdaq_other_listed|nasdaq_trading_system_adds_deletes |  | 2701 | 95.52 |  | maintain | not_applicable |
| NYSE MKT | official_full | nasdaq_other_listed | ETF | 309 | 74.43 |  | maintain | not_applicable |
| NZX | official_full | nzx_instruments |  | 172 | 26.16 |  | maintain | not_applicable |
| OSL | official_full | euronext_equities|euronext_etfs |  | 297 | 95.96 |  | maintain | not_applicable |
| OTC | official_full | otc_markets_security_profile|otc_markets_stock_screener|sec_company_tickers_exchange |  | 11925 | 69.33 | otc_markets_stock_screener | refresh_unavailable | not_applicable |
| PSE | official_full | pse_listed_company_directory |  | 385 | 40.26 |  | maintain | not_applicable |
| PSE_CZ | official_partial | pse_cz_shares_directory |  | 0 |  |  | denominator_missing | blocked_denominator_missing |
| PSX | official_full | psx_dps_symbols|psx_listed_companies|psx_symbol_name_daily |  | 720 | 54.17 |  | maintain | not_applicable |
| QSE | official_full | qse_market_watch |  | 57 | 96.49 |  | maintain | not_applicable |
| RSE | official_partial | rse_listed_companies |  | 0 |  |  | denominator_missing | blocked_denominator_missing |
| SEM | official_full | sem_isin |  | 46 | 100.0 |  | maintain | not_applicable |
| SET | official_full | set_dr_search|set_etf_search|set_listed_companies|set_stock_search |  | 943 | 82.18 |  | maintain | not_applicable |
| SGX | official_full | sgx_securities_prices |  | 746 | 81.77 |  | maintain | not_applicable |
| SIX | official_partial | six_equity_issuers|six_etf_products|six_etp_products|six_shares_explorer_full |  | 0 |  |  | denominator_missing | blocked_denominator_missing |
| SSE | official_partial | sse_a_share_list|sse_etf_list |  | 0 |  |  | denominator_missing | blocked_denominator_missing |
| SSE_CL | official_full | bolsa_santiago_instruments |  | 122 | 100.0 |  | maintain | not_applicable |
| STO | official_partial | nasdaq_nordic_stockholm_etfs|nasdaq_nordic_stockholm_shares|nasdaq_nordic_stockholm_trackers|ngm_companies_page|ngm_market_data_equities|spotlight_companies_directory |  | 0 |  | ngm_market_data_equities | refresh_unavailable | blocked_source_unavailable |
| SZSE | official_partial | szse_a_share_list|szse_b_share_list|szse_etf_list |  | 0 |  | szse_a_share_list | refresh_unavailable | blocked_source_unavailable |
| TADAWUL | official_full | tadawul_main_market_watch |  | 413 | 47.94 |  | maintain | not_applicable |
| TASE | official_partial | tase_etf_marketdata|tase_foreign_etf_search|tase_participating_unit_search|tase_securities_marketdata |  | 0 |  | tase_foreign_etf_search|tase_participating_unit_search | refresh_unavailable | blocked_source_unavailable |
| TPEX | official_partial | tpex_emerging_basic_info|tpex_etf_filter|tpex_mainboard_basic_info|tpex_mainboard_daily_quotes |  | 0 |  |  | denominator_missing | blocked_denominator_missing |
| TSE | official_full | jpx_listed_issues|jpx_tse_stock_detail |  | 4444 | 90.98 |  | maintain | not_applicable |
| TSX | official_full | tmx_etf_screener|tmx_interlisted_companies|tmx_listed_issuers |  | 788 | 75.25 |  | maintain | not_applicable |
| TSXV | official_full | tmx_interlisted_companies|tmx_listed_issuers |  | 1596 | 87.72 |  | maintain | not_applicable |
| TWSE | official_full | twse_etf_list|twse_listed_companies |  | 1095 | 92.69 |  | maintain | not_applicable |
| UPCOM | official_full | upcom_registered_securities |  | 824 | 0.24 |  | maintain | not_applicable |
| USE_UG | official_partial | use_ug_listed_companies |  | 0 |  |  | denominator_missing | blocked_denominator_missing |
| VSE | official_partial | vienna_listed_companies | ETF | 0 |  |  | denominator_missing | blocked_product_class_gap |
| WSE | official_partial | newconnect_listed_companies|wse_etf_list|wse_listed_companies |  | 0 |  |  | denominator_missing | blocked_denominator_missing |
| XDUS | missing |  | ETF|Stock | 0 |  |  | official_source_required | not_applicable |
| XETRA | official_full | deutsche_boerse_etfs_etps|deutsche_boerse_listed_companies|deutsche_boerse_xetra_all_tradable_equities |  | 5093 | 80.56 |  | maintain | not_applicable |
| XHAM | missing |  | ETF|Stock | 0 |  |  | official_source_required | not_applicable |
| XHAN | missing |  | ETF|Stock | 0 |  |  | official_source_required | not_applicable |
| XSTU | missing |  | ETF|Stock | 0 |  |  | official_source_required | not_applicable |
| ZSE | official_partial | zagreb_securities_directory |  | 0 |  |  | denominator_missing | blocked_denominator_missing |
| ZSE_ZW | official_partial | zse_zw_listed_companies |  | 0 |  |  | denominator_missing | blocked_denominator_missing |

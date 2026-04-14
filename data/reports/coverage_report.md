# Coverage Report

## Global

| Metric | Value |
|---|---|
| tickers | 53446 |
| aliases | 93855 |
| stocks | 38437 |
| etfs | 15009 |
| isin_coverage | 48235 |
| sector_coverage | 44865 |
| stock_sector_coverage | 33909 |
| etf_category_coverage | 10956 |
| cik_coverage | 7699 |
| figi_coverage | 3650 |
| lei_coverage | 920 |
| listing_status_rows | 86675 |
| listing_status_intervals | 86675 |
| listing_events | 29741 |
| listing_keys | 61944 |
| instrument_scope_rows | 61944 |
| instrument_scope_core | 45220 |
| instrument_scope_extended | 16724 |
| instrument_scope_primary_listing | 41302 |
| instrument_scope_primary_listing_missing_isin | 3918 |
| instrument_scope_otc_listing | 11093 |
| instrument_scope_secondary_cross_listing | 5631 |
| official_masterfile_symbols | 78283 |
| official_masterfile_matches | 42430 |
| official_masterfile_collisions | 10379 |
| official_masterfile_missing | 25474 |
| official_full_exchanges | 46 |
| official_partial_exchanges | 34 |
| manual_only_exchanges | 0 |
| missing_exchanges | 0 |
| stock_verification_items | 43852 |
| stock_verification_verified | 39929 |
| stock_verification_reference_gap | 3486 |
| stock_verification_missing_from_official | 43 |
| stock_verification_name_mismatch | 283 |
| stock_verification_cross_exchange_collision | 2 |
| etf_verification_items | 18092 |
| etf_verification_verified | 17770 |
| etf_verification_reference_gap | 282 |
| etf_verification_missing_from_official | 32 |
| etf_verification_name_mismatch | 0 |
| etf_verification_cross_exchange_collision | 0 |

## Freshness

| Metric | Value |
|---|---|
| tickers_built_at | 2026-04-14T04:43:46Z |
| tickers_age_hours | 0.0 |
| masterfiles_generated_at | 2026-04-14T00:35:27Z |
| masterfiles_age_hours | 4.14 |
| identifiers_generated_at | 2026-04-14T04:43:46Z |
| identifiers_age_hours | 0.0 |
| listing_history_observed_at | 2026-04-14T04:43:46Z |
| listing_history_age_hours | 0.0 |
| latest_verification_run | data/stock_verification/run-20260414-003708-cse-lk-source |
| latest_verification_generated_at | 2026-04-14T00:37:10Z |
| latest_verification_age_hours | 4.11 |
| latest_stock_verification_run | data/stock_verification/run-20260414-003708-cse-lk-source |
| latest_stock_verification_generated_at | 2026-04-14T00:37:10Z |
| latest_stock_verification_age_hours | 4.11 |
| latest_etf_verification_run | data/etf_verification/run-20260414-003708-cse-lk-source |
| latest_etf_verification_generated_at | 2026-04-14T00:37:12Z |
| latest_etf_verification_age_hours | 4.11 |

## Source Coverage

| Source | Provider | Scope | Mode | Rows | Generated At |
|---|---|---|---|---|---|
| nasdaq_listed | Nasdaq Trader | exchange_directory | network | 5419 | 2026-04-11T06:56:58Z |
| nasdaq_other_listed | Nasdaq Trader | exchange_directory | network | 7084 | 2026-04-11T06:56:58Z |
| lse_company_reports | LSE | listed_companies_subset | cache | 12397 | 2026-04-11T06:56:58Z |
| lse_instrument_search | LSE | security_lookup_subset | network | 1628 | 2026-04-11T11:54:28Z |
| lse_instrument_directory | LSE | security_lookup_subset | cache | 64 | 2026-04-11T06:56:58Z |
| lse_price_explorer | LSE | exchange_directory | network | 10899 | 2026-04-13T19:14:02Z |
| asx_listed_companies | ASX | listed_companies_subset | network | 1979 | 2026-04-13T18:11:20Z |
| cboe_canada_listing_directory | Cboe Canada | exchange_directory | network | 436 | 2026-04-11T06:56:58Z |
| asx_investment_products | ASX | listed_companies_subset | network | 426 | 2026-04-11T06:56:58Z |
| set_listed_companies | SET | listed_companies_subset | network | 933 | 2026-04-11T06:56:58Z |
| set_stock_search | SET | exchange_directory | network | 946 | 2026-04-13T17:32:06Z |
| set_etf_search | SET | listed_companies_subset | network | 13 | 2026-04-11T06:56:58Z |
| set_dr_search | SET | listed_companies_subset | network | 352 | 2026-04-11T06:56:58Z |
| tmx_listed_issuers | TMX | listed_companies_subset | network | 3704 | 2026-04-12T11:47:56Z |
| tmx_etf_screener | TMX | listed_companies_subset | cache | 1697 | 2026-04-12T11:47:56Z |
| tmx_interlisted_companies | TMX | interlisted_subset | network | 271 | 2026-04-11T06:56:58Z |
| euronext_equities | Euronext | exchange_directory | network | 3882 | 2026-04-13T13:15:07Z |
| euronext_etfs | Euronext | listed_companies_subset | network | 3465 | 2026-04-11T11:23:10Z |
| jpx_listed_issues | JPX | exchange_directory | network | 4444 | 2026-04-11T06:56:58Z |
| jpx_tse_stock_detail | JPX | security_identifier_registry_subset | network | 3212 | 2026-04-14T00:11:31Z |
| deutsche_boerse_listed_companies | Deutsche Boerse | listed_companies_subset | network | 471 | 2026-04-11T11:23:10Z |
| deutsche_boerse_etfs_etps | Deutsche Boerse | listed_companies_subset | network | 3443 | 2026-04-13T18:19:11Z |
| deutsche_boerse_xetra_all_tradable_equities | Deutsche Boerse | exchange_directory | network | 4429 | 2026-04-13T19:25:45Z |
| six_equity_issuers | SIX | listed_companies_subset | network | 241 | 2026-04-13T20:29:46Z |
| six_shares_explorer_full | SIX | listed_companies_subset | network | 12 | 2026-04-13T22:49:36Z |
| six_etf_products | SIX | listed_companies_subset | network | 8568 | 2026-04-13T20:29:46Z |
| six_etp_products | SIX | listed_companies_subset | network | 810 | 2026-04-13T20:29:46Z |
| b3_instruments_equities | B3 | exchange_directory | network | 1218 | 2026-04-11T12:28:19Z |
| b3_listed_etfs | B3 | listed_companies_subset | network | 187 | 2026-04-11T06:56:58Z |
| b3_bdr_etfs | B3 | listed_companies_subset | network | 302 | 2026-04-11T06:56:58Z |
| jse_etf_list | JSE | listed_companies_subset | network | 133 | 2026-04-13T18:26:38Z |
| jse_etn_list | JSE | listed_companies_subset | network | 94 | 2026-04-13T18:26:38Z |
| jse_instrument_search | JSE | listed_companies_subset | network | 12 | 2026-04-13T20:06:19Z |
| bme_listed_companies | BME | listed_companies_subset | network | 57 | 2026-04-11T06:56:58Z |
| bme_etf_list | BME | listed_companies_subset | cache | 5 | 2026-04-11T06:56:58Z |
| bme_listed_values | BME | listed_companies_subset | network | 156 | 2026-04-13T18:32:18Z |
| bme_security_prices_directory | BME | exchange_directory | unavailable | 0 | 2026-04-13T19:48:24Z |
| bme_growth_prices | BME Growth | listed_companies_subset | unavailable | 0 | 2026-04-11T12:16:59Z |
| athex_sector_classification | ATHEX | listed_companies_subset | network | 91 | 2026-04-13T14:52:37Z |
| bursa_equity_isin | Bursa Malaysia | listed_companies_subset | network | 1123 | 2026-04-12T11:47:56Z |
| bursa_closing_prices | Bursa Malaysia | listed_companies_subset | network | 1281 | 2026-04-13T20:43:05Z |
| bse_bw_listed_companies | BSE Botswana | listed_companies_subset | network | 26 | 2026-04-13T13:54:28Z |
| bse_hu_listed_companies | Budapest Stock Exchange | listed_companies_subset | network | 12 | 2026-04-13T15:39:43Z |
| egx_listed_stocks | EGX | listed_companies_subset | network | 190 | 2026-04-13T18:54:08Z |
| bvl_issuers_directory | CAVALI | security_lookup_subset | network | 31 | 2026-04-13T15:32:10Z |
| cse_ma_listed_companies | Casablanca Stock Exchange | exchange_directory | network | 80 | 2026-04-13T15:06:41Z |
| cse_lk_all_security_code | CSE Sri Lanka | exchange_directory | network | 0 | 2026-04-13T16:30:08Z |
| cse_lk_company_info_summary | CSE Sri Lanka | exchange_directory | network | 310 | 2026-04-14T00:35:27Z |
| dse_tz_listed_companies | DSE Tanzania | listed_companies_subset | network | 17 | 2026-04-13T15:56:17Z |
| bvc_colombia_issuers | BVC | listed_companies_subset | network | 3 | 2026-04-13T16:30:08Z |
| byma_equity_details | BYMA | security_lookup_subset | network | 63 | 2026-04-13T16:43:12Z |
| mse_mw_listed_companies | MSE Malawi | listed_companies_subset | network | 8 | 2026-04-13T15:56:17Z |
| nse_ke_listed_companies | NSE Kenya | exchange_directory | network | 66 | 2026-04-13T15:14:39Z |
| nse_india_securities_available | NSE India | exchange_directory | network | 2998 | 2026-04-13T21:00:21Z |
| bse_india_scrips | BSE India | exchange_directory | network | 5015 | 2026-04-13T21:10:05Z |
| hkex_securities_list | HKEX | exchange_directory | network | 3126 | 2026-04-13T21:15:25Z |
| sgx_securities_prices | SGX | exchange_directory | network | 736 | 2026-04-13T21:21:45Z |
| dfm_listed_securities | DFM | exchange_directory | network | 71 | 2026-04-13T21:29:52Z |
| boursa_kuwait_stocks | Boursa Kuwait | exchange_directory | network | 141 | 2026-04-13T21:35:19Z |
| bahrain_bourse_listed_companies | Bahrain Bourse | exchange_directory | network | 41 | 2026-04-13T22:14:05Z |
| bist_kap_mkk_listed_securities | KAP/MKK | exchange_directory | network | 636 | 2026-04-13T21:44:57Z |
| tadawul_main_market_watch | Saudi Exchange | exchange_directory | network | 411 | 2026-04-13T21:55:44Z |
| adx_market_watch | ADX | exchange_directory | network | 120 | 2026-04-13T22:05:57Z |
| qse_market_watch | QSE | exchange_directory | network | 57 | 2026-04-13T23:13:36Z |
| muscat_securities_companies | MSX | exchange_directory | network | 108 | 2026-04-13T21:05:11Z |
| rse_listed_companies | RSE | listed_companies_subset | network | 2 | 2026-04-13T15:56:17Z |
| gse_listed_companies | GSE | listed_companies_subset | network | 18 | 2026-04-13T16:52:56Z |
| luse_listed_companies | LuSE | listed_companies_subset | network | 15 | 2026-04-13T16:52:56Z |
| bolsa_santiago_instruments | Bolsa de Santiago | exchange_directory | network | 115 | 2026-04-13T17:08:10Z |
| sem_isin | SEM | exchange_directory | network | 47 | 2026-04-13T15:19:02Z |
| use_ug_listed_companies | USE Uganda | listed_companies_subset | network | 7 | 2026-04-13T15:56:17Z |
| nzx_instruments | NZX | exchange_directory | network | 173 | 2026-04-13T20:52:21Z |
| nasdaq_mutual_fund_quotes | Nasdaq | security_lookup_subset | network | 7 | 2026-04-13T16:03:40Z |
| zse_zw_listed_companies | ZSE Zimbabwe | listed_companies_subset | network | 27 | 2026-04-13T15:56:17Z |
| bvb_shares_directory | BVB | exchange_directory | network | 347 | 2026-04-13T14:41:14Z |
| bvb_fund_units_directory | BVB | listed_companies_subset | network | 8 | 2026-04-13T14:41:14Z |
| ngx_equities_price_list | NGX | listed_companies_subset | network | 131 | 2026-04-13T14:00:36Z |
| ngx_company_profile_directory | NGX | exchange_directory | network | 133 | 2026-04-13T14:19:30Z |
| bmv_stock_search | BMV | listed_companies_subset | network | 15 | 2026-04-13T20:19:54Z |
| bmv_capital_trust_search | BMV | listed_companies_subset | network | 7 | 2026-04-13T20:19:54Z |
| bmv_etf_search | BMV | listed_companies_subset | network | 7 | 2026-04-13T20:19:54Z |
| bmv_market_data_securities | BMV | listed_companies_subset | network | 17 | 2026-04-13T22:32:56Z |
| bmv_issuer_directory | BMV | listed_companies_subset | network | 0 | 2026-04-13T20:19:54Z |
| nasdaq_nordic_stockholm_shares | Nasdaq Nordic | listed_companies_subset | cache | 747 | 2026-04-11T06:56:58Z |
| nasdaq_nordic_stockholm_shares_search | Nasdaq Nordic | listed_companies_subset | cache | 1 | 2026-04-11T06:56:58Z |
| nasdaq_nordic_helsinki_shares | Nasdaq Nordic | listed_companies_subset | cache | 194 | 2026-04-11T06:56:58Z |
| nasdaq_nordic_helsinki_shares_search | Nasdaq Nordic | listed_companies_subset | cache | 3 | 2026-04-11T06:56:58Z |
| nasdaq_nordic_iceland_shares | Nasdaq Nordic | listed_companies_subset | network | 32 | 2026-04-13T13:15:07Z |
| spotlight_companies_directory | Spotlight | listed_companies_subset | cache | 137 | 2026-04-11T09:48:13Z |
| spotlight_companies_search | Spotlight | listed_companies_subset | cache | 20 | 2026-04-11T06:56:58Z |
| ngm_companies_page | NGM | listed_companies_subset | cache | 53 | 2026-04-11T09:30:20Z |
| ngm_market_data_equities | NGM | listed_companies_subset | cache | 33 | 2026-04-11T06:56:58Z |
| nasdaq_nordic_copenhagen_shares | Nasdaq Nordic | listed_companies_subset | cache | 144 | 2026-04-11T06:56:58Z |
| nasdaq_nordic_copenhagen_shares_search | Nasdaq Nordic | listed_companies_subset | cache | 0 | 2026-04-11T06:56:58Z |
| nasdaq_nordic_stockholm_etfs | Nasdaq Nordic | listed_companies_subset | cache | 33 | 2026-04-11T06:56:58Z |
| nasdaq_nordic_helsinki_etfs | Nasdaq Nordic | listed_companies_subset | cache | 2 | 2026-04-11T06:56:58Z |
| nasdaq_nordic_copenhagen_etfs | Nasdaq Nordic | listed_companies_subset | cache | 1 | 2026-04-11T06:56:58Z |
| nasdaq_nordic_copenhagen_etf_search | Nasdaq Nordic | listed_companies_subset | cache | 1 | 2026-04-11T06:56:58Z |
| nasdaq_nordic_stockholm_trackers | Nasdaq Nordic | listed_companies_subset | cache | 6 | 2026-04-11T06:56:58Z |
| twse_listed_companies | TWSE | exchange_directory | network | 1081 | 2026-04-11T11:06:37Z |
| twse_etf_list | TWSE | listed_companies_subset | network | 216 | 2026-04-11T11:06:37Z |
| sse_a_share_list | SSE | listed_companies_subset | network | 2351 | 2026-04-13T17:44:04Z |
| sse_etf_list | SSE | listed_companies_subset | network | 845 | 2026-04-12T11:47:56Z |
| szse_a_share_list | SZSE | listed_companies_subset | network | 2886 | 2026-04-13T17:44:04Z |
| szse_b_share_list | SZSE | listed_companies_subset | network | 38 | 2026-04-13T17:44:04Z |
| szse_etf_list | SZSE | listed_companies_subset | network | 633 | 2026-04-12T11:47:56Z |
| tpex_mainboard_daily_quotes | TPEX | listed_companies_subset | cache | 884 | 2026-04-11T10:58:35Z |
| tpex_etf_filter | TPEX | listed_companies_subset | cache | 112 | 2026-04-13T20:19:08Z |
| tpex_mainboard_basic_info | MOPS | listed_companies_subset | cache | 881 | 2026-04-13T20:19:08Z |
| tpex_emerging_basic_info | MOPS | listed_companies_subset | cache | 354 | 2026-04-13T20:19:08Z |
| krx_listed_companies | KRX | exchange_directory | network | 2769 | 2026-04-13T19:32:56Z |
| krx_etf_finder | KRX | exchange_directory | network | 1093 | 2026-04-13T19:32:56Z |
| psx_listed_companies | PSX | listed_companies_subset | network | 565 | 2026-04-11T06:56:58Z |
| psx_symbol_name_daily | PSX | listed_companies_subset | network | 370 | 2026-04-11T06:56:58Z |
| psx_dps_symbols | PSX | exchange_directory | network | 712 | 2026-04-13T17:39:29Z |
| pse_listed_company_directory | PSE | exchange_directory | network | 381 | 2026-04-11T06:56:58Z |
| pse_cz_shares_directory | Prague Stock Exchange | listed_companies_subset | network | 63 | 2026-04-13T13:43:15Z |
| idx_listed_companies | IDX | listed_companies_subset | network | 957 | 2026-04-12T11:47:56Z |
| idx_company_profiles | IDX | exchange_directory | network | 958 | 2026-04-13T17:50:09Z |
| wse_listed_companies | GPW | listed_companies_subset | network | 399 | 2026-04-13T19:57:43Z |
| newconnect_listed_companies | NewConnect | listed_companies_subset | network | 364 | 2026-04-13T19:57:43Z |
| wse_etf_list | GPW | listed_companies_subset | network | 25 | 2026-04-13T19:57:43Z |
| tase_securities_marketdata | TASE | listed_companies_subset | network | 524 | 2026-04-11T06:56:58Z |
| tase_etf_marketdata | TASE | listed_companies_subset | network | 460 | 2026-04-13T17:56:58Z |
| tase_foreign_etf_search | TASE | listed_companies_subset | network | 15 | 2026-04-11T07:41:23Z |
| tase_participating_unit_search | TASE | listed_companies_subset | network | 16 | 2026-04-11T07:41:33Z |
| hose_listed_stocks | HOSE | listed_companies_subset | network | 403 | 2026-04-13T20:24:27Z |
| hose_etf_list | HOSE | listed_companies_subset | network | 18 | 2026-04-13T20:24:27Z |
| hose_fund_certificate_list | HOSE | listed_companies_subset | network | 4 | 2026-04-13T20:24:27Z |
| hnx_listed_securities | HNX | exchange_directory | cache | 302 | 2026-04-11T06:56:58Z |
| upcom_registered_securities | HNX | exchange_directory | cache | 837 | 2026-04-11T06:56:58Z |
| vienna_listed_companies | Wiener Boerse | listed_companies_subset | network | 22 | 2026-04-13T13:34:49Z |
| zagreb_securities_directory | ZSE Croatia | listed_companies_subset | network | 76 | 2026-04-13T13:34:49Z |
| sec_company_tickers_exchange | SEC | exchange_directory | cache | 10117 | 2026-04-11T06:56:58Z |
| otc_markets_security_profile | OTC Markets | security_lookup_subset | cache | 1705 | 2026-04-11T10:46:59Z |
| otc_markets_stock_screener | OTC Markets | exchange_directory | network | 11972 | 2026-04-13T17:19:54Z |

## Exchange Coverage

| Exchange | Venue Status | Tickers | ISIN | Sector | CIK | FIGI | LEI | Masterfile Symbols | Matches | Collisions | Missing | Match Rate | Verified on Covered |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| ADX | official_full | 0 | 0 | 0 | 0 | 0 | 0 | 120 | 0 | 30 | 90 | 0.0 |  |
| AMS | official_full | 314 | 308 | 206 | 0 | 144 | 0 | 548 | 240 | 294 | 14 | 43.8 | 100.0 |
| ASX | official_partial | 1298 | 1193 | 1111 | 30 | 1032 | 24 | 0 | 0 | 0 | 0 |  | 100.0 |
| ATHEX | official_partial | 117 | 98 | 117 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| B3 | official_full | 1537 | 1253 | 644 | 0 | 0 | 0 | 1218 | 1206 | 0 | 12 | 99.01 | 100.0 |
| BATS | official_full | 1247 | 1167 | 1001 | 0 | 0 | 0 | 1243 | 1200 | 7 | 36 | 96.54 | 100.0 |
| BCBA | official_partial | 64 | 61 | 50 | 0 | 48 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| BHB | official_full | 0 | 0 | 0 | 0 | 0 | 0 | 41 | 0 | 9 | 32 | 0.0 |  |
| BIST | official_full | 0 | 0 | 0 | 0 | 0 | 0 | 636 | 0 | 20 | 616 | 0.0 |  |
| BK | official_full | 0 | 0 | 0 | 0 | 0 | 0 | 141 | 0 | 28 | 113 | 0.0 |  |
| BME | official_partial | 169 | 169 | 158 | 3 | 2 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| BMV | official_partial | 179 | 160 | 174 | 0 | 1 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| BSE_BW | official_partial | 39 | 39 | 28 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| BSE_HU | official_partial | 31 | 23 | 12 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| BSE_IN | official_full | 0 | 0 | 0 | 0 | 0 | 0 | 5015 | 0 | 278 | 4737 | 0.0 |  |
| BVB | official_full | 80 | 80 | 75 | 0 | 0 | 0 | 347 | 75 | 120 | 152 | 21.61 | 100.0 |
| BVC | official_partial | 3 | 0 | 3 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| BVL | official_partial | 33 | 31 | 3 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| Bursa | official_partial | 926 | 926 | 924 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| CPH | official_partial | 131 | 131 | 112 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| CSE_LK | official_full | 0 | 0 | 0 | 0 | 0 | 0 | 310 | 0 | 0 | 310 | 0.0 |  |
| CSE_MA | official_full | 66 | 66 | 2 | 0 | 0 | 0 | 80 | 1 | 59 | 20 | 1.25 | 92.42 |
| DFM | official_full | 0 | 0 | 0 | 0 | 0 | 0 | 71 | 0 | 16 | 55 | 0.0 |  |
| DSE_TZ | official_partial | 17 | 15 | 2 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| EGX | official_partial | 225 | 225 | 196 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| Euronext | official_full | 975 | 969 | 741 | 7 | 665 | 65 | 4370 | 938 | 2233 | 1199 | 21.46 | 100.0 |
| GSE | official_partial | 19 | 18 | 2 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| HEL | official_partial | 188 | 188 | 140 | 1 | 0 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| HKEX | official_full | 0 | 0 | 0 | 0 | 0 | 0 | 3126 | 0 | 82 | 3044 | 0.0 |  |
| HNX | official_full | 105 | 105 | 13 | 0 | 0 | 0 | 302 | 105 | 158 | 39 | 34.77 | 100.0 |
| HOSE | official_partial | 153 | 153 | 153 | 2 | 0 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| ICE_IS | official_partial | 18 | 18 | 3 | 1 | 0 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| IDX | official_full | 694 | 579 | 694 | 1 | 2 | 0 | 958 | 694 | 243 | 21 | 72.44 | 100.0 |
| ISE | official_full | 14 | 14 | 13 | 0 | 0 | 0 | 15 | 9 | 6 | 0 | 60.0 | 100.0 |
| JSE | official_partial | 213 | 183 | 208 | 2 | 0 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| KOSDAQ | official_full | 1583 | 1578 | 1580 | 0 | 0 | 0 | 1820 | 1577 | 0 | 243 | 86.65 | 99.62 |
| KRX | official_full | 1788 | 1786 | 1261 | 0 | 0 | 0 | 2042 | 1780 | 3 | 259 | 87.17 | 99.76 |
| LSE | official_full | 6408 | 6361 | 5106 | 16 | 22 | 5 | 10899 | 6329 | 1159 | 3411 | 58.07 | 99.32 |
| LUSE | official_partial | 22 | 22 | 2 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| MSE_MW | official_partial | 8 | 8 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| MSX | official_full | 0 | 0 | 0 | 0 | 0 | 0 | 108 | 0 | 14 | 94 | 0.0 |  |
| NASDAQ | official_full | 4634 | 4482 | 4373 | 3441 | 10 | 400 | 5449 | 4568 | 51 | 830 | 83.83 | 100.0 |
| NEO | official_full | 197 | 79 | 129 | 0 | 0 | 0 | 436 | 196 | 85 | 155 | 44.95 | 100.0 |
| NGX | official_full | 145 | 143 | 110 | 0 | 0 | 0 | 133 | 133 | 0 | 0 | 100.0 | 100.0 |
| NMFQS | official_partial | 7 | 7 | 6 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| NSE_IN | official_full | 0 | 0 | 0 | 0 | 0 | 0 | 2998 | 0 | 180 | 2818 | 0.0 |  |
| NSE_KE | official_full | 46 | 46 | 13 | 0 | 0 | 0 | 66 | 10 | 24 | 32 | 15.15 | 100.0 |
| NYSE | official_full | 2078 | 2002 | 2031 | 1993 | 24 | 372 | 3836 | 2060 | 514 | 1262 | 53.7 | 100.0 |
| NYSE ARCA | official_full | 2651 | 2448 | 2081 | 127 | 6 | 3 | 2615 | 2583 | 13 | 19 | 98.78 | 100.0 |
| NYSE MKT | official_full | 238 | 234 | 238 | 221 | 2 | 26 | 313 | 234 | 19 | 60 | 74.76 | 100.0 |
| NZX | official_full | 0 | 0 | 0 | 0 | 0 | 0 | 173 | 0 | 126 | 47 | 0.0 |  |
| OSL | official_full | 241 | 237 | 176 | 2 | 196 | 0 | 298 | 234 | 64 | 0 | 78.52 | 100.0 |
| OTC | official_full | 11093 | 9800 | 8655 | 1795 | 5 | 0 | 11972 | 7835 | 25 | 4112 | 65.44 | 95.21 |
| PSE | official_full | 90 | 90 | 13 | 1 | 0 | 0 | 381 | 90 | 185 | 106 | 23.62 | 100.0 |
| PSE_CZ | official_partial | 24 | 23 | 10 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| PSX | official_full | 373 | 269 | 357 | 3 | 0 | 0 | 712 | 371 | 145 | 196 | 52.11 | 100.0 |
| QSE | official_full | 0 | 0 | 0 | 0 | 0 | 0 | 57 | 0 | 2 | 55 | 0.0 |  |
| RSE | official_partial | 2 | 2 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| SEM | official_full | 53 | 53 | 4 | 1 | 0 | 0 | 47 | 47 | 0 | 0 | 100.0 | 90.2 |
| SET | official_full | 547 | 351 | 543 | 4 | 0 | 0 | 946 | 547 | 350 | 49 | 57.82 | 100.0 |
| SGX | official_full | 0 | 0 | 0 | 0 | 0 | 0 | 736 | 0 | 141 | 595 | 0.0 |  |
| SIX | official_partial | 743 | 743 | 743 | 2 | 0 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| SSE | official_partial | 2789 | 2175 | 2310 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| SSE_CL | official_full | 116 | 86 | 98 | 0 | 0 | 0 | 115 | 115 | 0 | 0 | 100.0 | 98.97 |
| STO | official_partial | 725 | 725 | 486 | 2 | 3 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| SZSE | official_partial | 3083 | 2596 | 2746 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| TADAWUL | official_full | 0 | 0 | 0 | 0 | 0 | 0 | 411 | 0 | 220 | 191 | 0.0 |  |
| TASE | official_partial | 673 | 673 | 587 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| TPEX | official_partial | 1118 | 1118 | 1118 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| TSE | official_full | 3216 | 3212 | 3111 | 0 | 0 | 0 | 4444 | 3216 | 1199 | 29 | 72.37 | 100.0 |
| TSX | official_full | 1895 | 1331 | 1387 | 12 | 932 | 23 | 788 | 326 | 460 | 2 | 41.37 | 100.0 |
| TSXV | official_full | 1066 | 523 | 644 | 17 | 524 | 0 | 1600 | 1043 | 556 | 1 | 65.19 | 100.0 |
| TWSE | official_full | 1242 | 1168 | 1117 | 0 | 0 | 0 | 1081 | 1024 | 29 | 28 | 94.73 | 100.0 |
| UPCOM | official_full | 2 | 2 | 1 | 0 | 0 | 0 | 837 | 2 | 459 | 376 | 0.24 | 100.0 |
| USE_UG | official_partial | 7 | 7 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| VSE | official_partial | 36 | 34 | 32 | 0 | 22 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| WSE | official_partial | 348 | 348 | 313 | 7 | 1 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| XETRA | official_full | 3752 | 3749 | 2805 | 8 | 9 | 2 | 4429 | 3642 | 773 | 14 | 82.23 | 100.0 |
| ZSE | official_partial | 23 | 23 | 1 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| ZSE_ZW | official_partial | 27 | 27 | 3 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |

## Country Coverage

| Country | Tickers | ISIN | Sector | CIK | FIGI | LEI |
|---|---|---|---|---|---|---|
| Argentina | 60 | 57 | 47 | 0 | 48 | 0 |
| Australia | 1747 | 1642 | 1528 | 228 | 1027 | 62 |
| Austria | 57 | 55 | 48 | 8 | 45 | 8 |
| Bahrain | 2 | 2 | 0 | 0 | 0 | 0 |
| Belgium | 121 | 120 | 94 | 7 | 34 | 1 |
| Bermuda | 191 | 191 | 179 | 61 | 8 | 14 |
| Botswana | 24 | 24 | 20 | 0 | 0 | 0 |
| Brazil | 1512 | 1228 | 631 | 0 | 0 | 0 |
| Bulgaria | 14 | 14 | 12 | 3 | 0 | 0 |
| Canada | 4936 | 3715 | 3622 | 664 | 1431 | 66 |
| Cayman Islands | 697 | 692 | 645 | 464 | 1 | 18 |
| Chile | 117 | 87 | 100 | 1 | 0 | 1 |
| China | 5987 | 4886 | 5161 | 13 | 0 | 0 |
| Colombia | 3 | 0 | 3 | 0 | 0 | 0 |
| Croatia | 23 | 23 | 1 | 0 | 0 | 0 |
| Cyprus | 14 | 14 | 10 | 2 | 3 | 0 |
| Czech Republic | 22 | 21 | 8 | 0 | 0 | 0 |
| Denmark | 142 | 142 | 122 | 4 | 1 | 0 |
| Egypt | 243 | 243 | 212 | 6 | 0 | 0 |
| Faroe Islands | 3 | 3 | 2 | 0 | 0 | 0 |
| Finland | 192 | 192 | 144 | 1 | 0 | 0 |
| France | 681 | 677 | 558 | 18 | 483 | 59 |
| Gabon | 1 | 1 | 1 | 0 | 0 | 0 |
| Germany | 754 | 750 | 641 | 14 | 6 | 5 |
| Ghana | 20 | 19 | 4 | 0 | 0 | 0 |
| Greece | 130 | 111 | 128 | 4 | 0 | 0 |
| Guernsey | 68 | 68 | 60 | 7 | 1 | 0 |
| Hong Kong | 68 | 67 | 63 | 2 | 1 | 0 |
| Hungary | 23 | 16 | 8 | 0 | 0 | 0 |
| Iceland | 18 | 18 | 3 | 1 | 0 | 0 |
| India | 9 | 9 | 6 | 0 | 0 | 0 |
| Indonesia | 746 | 631 | 743 | 22 | 2 | 1 |
| Ireland | 2572 | 2572 | 2276 | 67 | 63 | 7 |
| Isle of Man | 15 | 15 | 15 | 3 | 0 | 0 |
| Israel | 768 | 767 | 691 | 94 | 1 | 1 |
| Italy | 123 | 123 | 104 | 2 | 5 | 0 |
| Japan | 3309 | 3305 | 3181 | 15 | 0 | 0 |
| Jersey | 170 | 170 | 157 | 18 | 2 | 4 |
| Kazakhstan | 2 | 2 | 0 | 0 | 0 | 0 |
| Kenya | 45 | 45 | 13 | 0 | 0 | 0 |
| Liechtenstein | 4 | 4 | 3 | 0 | 0 | 0 |
| Luxembourg | 1023 | 1023 | 922 | 29 | 42 | 5 |
| Malawi | 8 | 8 | 0 | 0 | 0 | 0 |
| Malaysia | 924 | 924 | 920 | 0 | 0 | 0 |
| Malta | 6 | 6 | 6 | 0 | 1 | 0 |
| Mauritius | 69 | 69 | 21 | 6 | 1 | 0 |
| Mexico | 134 | 115 | 124 | 4 | 0 | 0 |
| Monaco | 2 | 2 | 2 | 0 | 1 | 0 |
| Morocco | 67 | 67 | 2 | 0 | 0 | 0 |
| Netherlands | 207 | 200 | 169 | 29 | 71 | 1 |
| New Zealand | 36 | 36 | 35 | 5 | 22 | 1 |
| Nigeria | 147 | 145 | 111 | 0 | 0 | 0 |
| Norway | 246 | 241 | 192 | 7 | 165 | 0 |
| Oman | 1 | 1 | 0 | 0 | 0 | 0 |
| Pakistan | 378 | 274 | 358 | 9 | 0 | 0 |
| Panama | 1 | 0 | 1 | 1 | 0 | 0 |
| Peru | 31 | 29 | 2 | 0 | 0 | 0 |
| Philippines | 110 | 110 | 31 | 11 | 0 | 1 |
| Poland | 354 | 354 | 316 | 21 | 1 | 5 |
| Portugal | 34 | 34 | 28 | 0 | 9 | 1 |
| Qatar | 1 | 1 | 1 | 0 | 0 | 0 |
| Romania | 83 | 83 | 77 | 3 | 0 | 0 |
| Rwanda | 2 | 2 | 0 | 0 | 0 | 0 |
| Singapore | 75 | 73 | 66 | 18 | 1 | 2 |
| Slovenia | 2 | 2 | 2 | 0 | 0 | 0 |
| South Africa | 231 | 201 | 221 | 6 | 0 | 0 |
| South Korea | 3364 | 3357 | 2834 | 0 | 0 | 0 |
| Spain | 223 | 223 | 193 | 9 | 7 | 3 |
| Sweden | 757 | 757 | 517 | 11 | 4 | 1 |
| Switzerland | 390 | 390 | 355 | 22 | 4 | 5 |
| Taiwan | 2337 | 2263 | 2210 | 0 | 0 | 0 |
| Tanzania | 15 | 13 | 2 | 0 | 0 | 0 |
| Thailand | 569 | 373 | 564 | 27 | 1 | 3 |
| Turkey | 1 | 1 | 0 | 0 | 0 | 0 |
| Uganda | 7 | 7 | 0 | 0 | 0 | 0 |
| Ukraine | 1 | 1 | 1 | 0 | 0 | 0 |
| United Kingdom | 1328 | 1317 | 1109 | 95 | 15 | 19 |
| United States | 14255 | 12498 | 11975 | 5104 | 27 | 608 |
| Vietnam | 261 | 261 | 167 | 2 | 0 | 0 |
| Zambia | 22 | 22 | 2 | 0 | 0 | 0 |
| Zimbabwe | 28 | 28 | 4 | 0 | 0 | 0 |

## Unresolved Gaps

| Exchange | Venue Status | Findings | Reference Gap | Missing | Name Mismatch | Collision |
|---|---|---|---|---|---|---|
| OTC | official_full | 3027 | 2743 | 0 | 283 | 1 |
| B3 | official_full | 278 | 278 | 0 | 0 | 0 |
| BMV | official_partial | 150 | 150 | 0 | 0 | 0 |
| BME | official_partial | 93 | 93 | 0 | 0 | 0 |
| JSE | official_partial | 76 | 76 | 0 | 0 | 0 |
| NYSE ARCA | official_full | 68 | 68 | 0 | 0 | 0 |
| NASDAQ | official_full | 67 | 67 | 0 | 0 | 0 |
| BATS | official_full | 48 | 48 | 0 | 0 | 0 |
| LSE | official_full | 37 | 7 | 29 | 0 | 1 |
| EGX | official_partial | 34 | 34 | 0 | 0 | 0 |
| Euronext | official_full | 34 | 34 | 0 | 0 | 0 |
| ASX | official_partial | 30 | 30 | 0 | 0 | 0 |
| ATHEX | official_partial | 26 | 26 | 0 | 0 | 0 |
| NYSE | official_full | 19 | 19 | 0 | 0 | 0 |
| BSE_HU | official_partial | 17 | 17 | 0 | 0 | 0 |
| TWSE | official_full | 16 | 16 | 0 | 0 | 0 |
| VSE | official_partial | 14 | 14 | 0 | 0 | 0 |
| BSE_BW | official_partial | 12 | 12 | 0 | 0 | 0 |
| NGX | official_full | 12 | 0 | 12 | 0 | 0 |
| KRX | official_full | 9 | 1 | 8 | 0 | 0 |

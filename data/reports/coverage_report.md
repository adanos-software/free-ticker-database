# Coverage Report

## Global

| Metric | Value |
|---|---|
| tickers | 61439 |
| core_listings | 54002 |
| aliases | 121431 |
| stocks | 45885 |
| etfs | 15554 |
| isin_coverage | 59608 |
| sector_coverage | 58736 |
| stock_sector_coverage | 43287 |
| etf_category_coverage | 15449 |
| cik_coverage | 7721 |
| figi_coverage | 63434 |
| lei_coverage | 919 |
| listing_status_rows | 96056 |
| listing_status_intervals | 96056 |
| listing_events | 39697 |
| listing_keys | 71043 |
| instrument_scope_rows | 71043 |
| instrument_scope_core | 54002 |
| instrument_scope_extended | 17041 |
| instrument_scope_primary_listing | 52943 |
| instrument_scope_primary_listing_missing_isin | 1059 |
| instrument_scope_otc_listing | 11056 |
| instrument_scope_secondary_cross_listing | 5985 |
| legacy_primary_ticker_collision_rows | 1 |
| official_masterfile_symbols | 77262 |
| official_masterfile_matches | 49725 |
| official_masterfile_collisions | 11096 |
| official_masterfile_missing | 16441 |
| official_full_exchanges | 46 |
| official_partial_exchanges | 33 |
| manual_only_exchanges | 0 |
| missing_exchanges | 1 |
| stock_verification_items | 52170 |
| stock_verification_verified | 46506 |
| stock_verification_reference_gap | 4281 |
| stock_verification_missing_from_official | 361 |
| stock_verification_name_mismatch | 974 |
| stock_verification_cross_exchange_collision | 7 |
| etf_verification_items | 18873 |
| etf_verification_verified | 17451 |
| etf_verification_reference_gap | 1359 |
| etf_verification_missing_from_official | 49 |
| etf_verification_name_mismatch | 7 |
| etf_verification_cross_exchange_collision | 0 |

## Freshness

| Metric | Value |
|---|---|
| tickers_built_at | 2026-05-16T17:22:52Z |
| tickers_age_hours | 0.0 |
| masterfiles_generated_at | 2026-05-16T17:15:33Z |
| masterfiles_age_hours | 0.13 |
| identifiers_generated_at | 2026-05-16T17:22:54Z |
| identifiers_age_hours | 0.0 |
| listing_history_observed_at | 2026-05-16T17:22:52Z |
| listing_history_age_hours | 0.0 |
| latest_verification_run | data/stock_verification/run-20260516-source-refresh |
| latest_verification_generated_at | 2026-05-16T17:19:52Z |
| latest_verification_age_hours | 0.05 |
| latest_stock_verification_run | data/stock_verification/run-20260516-source-refresh |
| latest_stock_verification_generated_at | 2026-05-16T17:19:52Z |
| latest_stock_verification_age_hours | 0.05 |
| latest_etf_verification_run | data/etf_verification/run-20260516-source-refresh |
| latest_etf_verification_generated_at | 2026-05-16T17:19:52Z |
| latest_etf_verification_age_hours | 0.05 |

## Source Coverage

| Source | Provider | Scope | Mode | Rows | Generated At |
|---|---|---|---|---|---|
| nasdaq_listed | Nasdaq Trader | exchange_directory | network | 5449 | 2026-05-16T15:58:29Z |
| nasdaq_other_listed | Nasdaq Trader | exchange_directory | network | 7197 | 2026-05-16T15:58:31Z |
| lse_company_reports | LSE | listed_companies_subset | cache | 12707 | 2026-05-16T15:58:33Z |
| lse_instrument_search | LSE | security_lookup_subset | network | 0 | 2026-05-16T15:58:34Z |
| lse_instrument_directory | LSE | security_lookup_subset | cache | 64 | 2026-05-16T15:58:37Z |
| lse_price_explorer | LSE | exchange_directory | cache | 11015 | 2026-05-16T15:58:38Z |
| asx_listed_companies | ASX | listed_companies_subset | network | 1976 | 2026-05-16T15:58:39Z |
| cboe_canada_listing_directory | Cboe Canada | exchange_directory | network | 448 | 2026-05-16T15:58:41Z |
| asx_investment_products | ASX | listed_companies_subset | network | 446 | 2026-05-16T15:58:43Z |
| set_listed_companies | SET | listed_companies_subset | network | 932 | 2026-05-16T15:58:47Z |
| set_stock_search | SET | exchange_directory | network | 945 | 2026-05-16T15:58:51Z |
| set_etf_search | SET | listed_companies_subset | network | 13 | 2026-05-16T15:59:00Z |
| set_dr_search | SET | listed_companies_subset | network | 378 | 2026-05-16T15:59:02Z |
| tmx_listed_issuers | TMX | listed_companies_subset | cache | 3619 | 2026-05-16T15:59:06Z |
| tmx_etf_screener | TMX | listed_companies_subset | cache | 1746 | 2026-05-16T15:59:07Z |
| tmx_interlisted_companies | TMX | interlisted_subset | network | 268 | 2026-05-16T15:59:10Z |
| euronext_equities | Euronext | exchange_directory | network | 3866 | 2026-05-16T15:59:11Z |
| euronext_etfs | Euronext | listed_companies_subset | network | 3535 | 2026-05-16T15:59:14Z |
| jpx_listed_issues | JPX | exchange_directory | network | 4449 | 2026-05-16T15:59:16Z |
| jpx_tse_stock_detail | JPX | security_identifier_registry_subset | network | 3205 | 2026-05-16T15:59:17Z |
| deutsche_boerse_listed_companies | Deutsche Boerse | listed_companies_subset | network | 472 | 2026-05-16T15:59:20Z |
| deutsche_boerse_etfs_etps | Deutsche Boerse | listed_companies_subset | network | 3532 | 2026-05-16T15:59:22Z |
| deutsche_boerse_xetra_all_tradable_equities | Deutsche Boerse | exchange_directory | network | 4519 | 2026-05-16T15:59:24Z |
| six_equity_issuers | SIX | listed_companies_subset | network | 240 | 2026-05-16T15:59:25Z |
| six_shares_explorer_full | SIX | listed_companies_subset | network | 0 | 2026-05-16T15:59:27Z |
| six_etf_products | SIX | listed_companies_subset | network | 8662 | 2026-05-16T15:59:28Z |
| six_etp_products | SIX | listed_companies_subset | network | 821 | 2026-05-16T15:59:30Z |
| b3_instruments_equities | B3 | exchange_directory | network | 9 | 2026-05-16T15:59:31Z |
| b3_listed_etfs | B3 | listed_companies_subset | network | 188 | 2026-05-16T15:59:34Z |
| b3_bdr_etfs | B3 | listed_companies_subset | network | 305 | 2026-05-16T15:59:38Z |
| jse_etf_list | JSE | listed_companies_subset | cache | 134 | 2026-05-16T15:59:41Z |
| jse_etn_list | JSE | listed_companies_subset | cache | 94 | 2026-05-16T15:59:42Z |
| jse_instrument_search | JSE | listed_companies_subset | unavailable | 0 | 2026-05-16T15:59:43Z |
| bme_listed_companies | BME | listed_companies_subset | cache | 78 | 2026-05-16T15:59:46Z |
| bme_etf_list | BME | listed_companies_subset | unavailable | 0 | 2026-05-16T15:59:48Z |
| bme_listed_values | BME | listed_companies_subset | unavailable | 0 | 2026-05-16T15:59:49Z |
| bme_security_prices_directory | BME | exchange_directory | unavailable | 0 | 2026-05-16T15:59:51Z |
| bme_growth_prices | BME Growth | listed_companies_subset | unavailable | 0 | 2026-05-16T15:59:52Z |
| athex_sector_classification | ATHEX | listed_companies_subset | cache | 91 | 2026-05-16T16:00:26Z |
| bursa_equity_isin | Bursa Malaysia | listed_companies_subset | network | 1127 | 2026-05-16T16:00:27Z |
| bursa_closing_prices | Bursa Malaysia | listed_companies_subset | network | 1281 | 2026-05-16T16:00:40Z |
| bse_bw_listed_companies | BSE Botswana | listed_companies_subset | cache | 26 | 2026-05-16T16:01:01Z |
| bse_hu_listed_companies | Budapest Stock Exchange | listed_companies_subset | cache | 2 | 2026-05-16T16:01:02Z |
| egx_listed_stocks | EGX | listed_companies_subset | cache | 190 | 2026-05-16T16:01:03Z |
| bvl_issuers_directory | CAVALI | security_lookup_subset | cache | 31 | 2026-05-16T16:01:04Z |
| cse_ma_listed_companies | Casablanca Stock Exchange | exchange_directory | network | 50 | 2026-05-16T17:14:31Z |
| cse_lk_all_security_code | CSE Sri Lanka | exchange_directory | cache | 307 | 2026-05-16T16:02:07Z |
| cse_lk_company_info_summary | CSE Sri Lanka | exchange_directory | cache | 315 | 2026-05-16T16:02:08Z |
| dse_tz_listed_companies | DSE Tanzania | listed_companies_subset | cache | 17 | 2026-05-16T16:02:09Z |
| bvc_colombia_issuers | BVC | listed_companies_subset | cache | 3 | 2026-05-16T16:02:10Z |
| byma_equity_details | BYMA | security_lookup_subset | cache | 63 | 2026-05-16T16:02:11Z |
| mse_mw_listed_companies | MSE Malawi | listed_companies_subset | unavailable | 0 | 2026-05-16T17:15:33Z |
| nse_ke_listed_companies | NSE Kenya | exchange_directory | cache | 66 | 2026-05-16T16:02:14Z |
| nse_india_securities_available | NSE India | exchange_directory | cache | 2974 | 2026-05-16T16:02:15Z |
| bse_india_scrips | BSE India | exchange_directory | cache | 4847 | 2026-05-16T16:02:17Z |
| hkex_securities_list | HKEX | exchange_directory | cache | 3154 | 2026-05-16T16:02:18Z |
| sgx_securities_prices | SGX | exchange_directory | cache | 738 | 2026-05-16T16:02:19Z |
| dfm_listed_securities | DFM | exchange_directory | cache | 71 | 2026-05-16T16:02:20Z |
| boursa_kuwait_stocks | Boursa Kuwait | exchange_directory | cache | 140 | 2026-05-16T16:02:22Z |
| bahrain_bourse_listed_companies | Bahrain Bourse | exchange_directory | cache | 41 | 2026-05-16T16:02:23Z |
| bist_kap_mkk_listed_securities | KAP/MKK | exchange_directory | cache | 636 | 2026-05-16T16:02:24Z |
| tadawul_main_market_watch | Saudi Exchange | exchange_directory | cache | 411 | 2026-05-16T16:02:26Z |
| adx_market_watch | ADX | exchange_directory | cache | 122 | 2026-05-16T16:02:27Z |
| qse_market_watch | QSE | exchange_directory | cache | 57 | 2026-05-16T16:02:28Z |
| muscat_securities_companies | MSX | exchange_directory | cache | 108 | 2026-05-16T16:02:30Z |
| rse_listed_companies | RSE | listed_companies_subset | cache | 1 | 2026-05-16T16:02:31Z |
| gse_listed_companies | GSE | listed_companies_subset | network | 18 | 2026-05-16T17:14:29Z |
| luse_listed_companies | LuSE | listed_companies_subset | cache | 15 | 2026-05-16T16:02:33Z |
| bolsa_santiago_instruments | Bolsa de Santiago | exchange_directory | cache | 112 | 2026-05-16T16:02:35Z |
| sem_isin | SEM | exchange_directory | cache | 47 | 2026-05-16T16:02:36Z |
| use_ug_listed_companies | USE Uganda | listed_companies_subset | cache | 7 | 2026-05-16T16:02:37Z |
| nzx_instruments | NZX | exchange_directory | cache | 173 | 2026-05-16T16:02:39Z |
| nasdaq_mutual_fund_quotes | Nasdaq | security_lookup_subset | cache | 7 | 2026-05-16T16:02:40Z |
| zse_zw_listed_companies | ZSE Zimbabwe | listed_companies_subset | cache | 27 | 2026-05-16T16:02:41Z |
| bvb_shares_directory | BVB | exchange_directory | cache | 347 | 2026-05-16T16:02:43Z |
| bvb_fund_units_directory | BVB | listed_companies_subset | cache | 9 | 2026-05-16T16:02:44Z |
| ngx_equities_price_list | NGX | listed_companies_subset | cache | 133 | 2026-05-16T16:02:45Z |
| ngx_company_profile_directory | NGX | exchange_directory | network | 133 | 2026-05-16T16:02:47Z |
| bmv_stock_search | BMV | listed_companies_subset | network | 148 | 2026-05-16T16:03:18Z |
| bmv_capital_trust_search | BMV | listed_companies_subset | network | 16 | 2026-05-16T16:06:00Z |
| bmv_etf_search | BMV | listed_companies_subset | network | 4 | 2026-05-16T16:08:02Z |
| bmv_market_data_securities | BMV | listed_companies_subset | network | 125 | 2026-05-16T16:37:25Z |
| bmv_issuer_directory | BMV | listed_companies_subset | network | 76 | 2026-05-16T16:14:15Z |
| nasdaq_nordic_stockholm_shares | Nasdaq Nordic | listed_companies_subset | network | 746 | 2026-05-16T16:15:37Z |
| nasdaq_nordic_stockholm_shares_search | Nasdaq Nordic | listed_companies_subset | network | 0 | 2026-05-16T16:15:41Z |
| nasdaq_nordic_helsinki_shares | Nasdaq Nordic | listed_companies_subset | network | 191 | 2026-05-16T16:15:42Z |
| nasdaq_nordic_helsinki_shares_search | Nasdaq Nordic | listed_companies_subset | network | 0 | 2026-05-16T16:15:45Z |
| nasdaq_nordic_iceland_shares | Nasdaq Nordic | listed_companies_subset | network | 32 | 2026-05-16T16:15:50Z |
| spotlight_companies_directory | Spotlight | listed_companies_subset | network | 134 | 2026-05-16T16:45:27Z |
| spotlight_companies_search | Spotlight | listed_companies_subset | network | 0 | 2026-05-16T16:21:55Z |
| ngm_companies_page | NGM | listed_companies_subset | network | 53 | 2026-05-16T16:21:56Z |
| ngm_market_data_equities | NGM | listed_companies_subset | network | 30 | 2026-05-16T16:22:00Z |
| nasdaq_nordic_copenhagen_shares | Nasdaq Nordic | listed_companies_subset | network | 143 | 2026-05-16T16:22:02Z |
| nasdaq_nordic_copenhagen_shares_search | Nasdaq Nordic | listed_companies_subset | network | 0 | 2026-05-16T16:22:04Z |
| nasdaq_nordic_stockholm_etfs | Nasdaq Nordic | listed_companies_subset | network | 33 | 2026-05-16T16:22:06Z |
| nasdaq_nordic_helsinki_etfs | Nasdaq Nordic | listed_companies_subset | network | 2 | 2026-05-16T16:22:07Z |
| nasdaq_nordic_copenhagen_etfs | Nasdaq Nordic | listed_companies_subset | network | 1 | 2026-05-16T16:22:08Z |
| nasdaq_nordic_copenhagen_etf_search | Nasdaq Nordic | listed_companies_subset | network | 0 | 2026-05-16T16:22:10Z |
| nasdaq_nordic_stockholm_trackers | Nasdaq Nordic | listed_companies_subset | network | 6 | 2026-05-16T16:22:11Z |
| twse_listed_companies | TWSE | exchange_directory | network | 1086 | 2026-05-16T16:22:12Z |
| twse_etf_list | TWSE | listed_companies_subset | network | 220 | 2026-05-16T16:22:16Z |
| sse_a_share_list | SSE | listed_companies_subset | network | 2354 | 2026-05-16T16:22:21Z |
| sse_etf_list | SSE | listed_companies_subset | unavailable | 0 | 2026-05-16T16:22:54Z |
| szse_a_share_list | SZSE | listed_companies_subset | network | 2891 | 2026-05-16T16:24:02Z |
| szse_b_share_list | SZSE | listed_companies_subset | network | 38 | 2026-05-16T16:24:14Z |
| szse_etf_list | SZSE | listed_companies_subset | network | 658 | 2026-05-16T16:24:19Z |
| tpex_mainboard_daily_quotes | TPEX | listed_companies_subset | unavailable | 0 | 2026-05-16T16:24:25Z |
| tpex_etf_filter | TPEX | listed_companies_subset | network | 113 | 2026-05-16T16:24:27Z |
| tpex_mainboard_basic_info | MOPS | listed_companies_subset | network | 887 | 2026-05-16T16:24:31Z |
| tpex_emerging_basic_info | MOPS | listed_companies_subset | network | 349 | 2026-05-16T16:24:37Z |
| krx_listed_companies | KRX | exchange_directory | network | 2766 | 2026-05-16T16:24:40Z |
| krx_etf_finder | KRX | exchange_directory | network | 1107 | 2026-05-16T16:24:52Z |
| psx_listed_companies | PSX | listed_companies_subset | network | 562 | 2026-05-16T16:24:54Z |
| psx_symbol_name_daily | PSX | listed_companies_subset | network | 368 | 2026-05-16T16:25:23Z |
| psx_dps_symbols | PSX | exchange_directory | network | 714 | 2026-05-16T16:25:27Z |
| pse_listed_company_directory | PSE | exchange_directory | network | 381 | 2026-05-16T16:25:30Z |
| pse_cz_shares_directory | Prague Stock Exchange | listed_companies_subset | network | 63 | 2026-05-16T16:25:33Z |
| idx_listed_companies | IDX | listed_companies_subset | network | 957 | 2026-05-16T16:25:40Z |
| idx_company_profiles | IDX | exchange_directory | network | 958 | 2026-05-16T16:25:42Z |
| wse_listed_companies | GPW | listed_companies_subset | network | 400 | 2026-05-16T16:25:44Z |
| newconnect_listed_companies | NewConnect | listed_companies_subset | network | 364 | 2026-05-16T16:25:49Z |
| wse_etf_list | GPW | listed_companies_subset | network | 27 | 2026-05-16T16:25:52Z |
| tase_securities_marketdata | TASE | listed_companies_subset | network | 523 | 2026-05-16T16:25:59Z |
| tase_etf_marketdata | TASE | listed_companies_subset | network | 463 | 2026-05-16T16:26:32Z |
| tase_foreign_etf_search | TASE | listed_companies_subset | network | 0 | 2026-05-16T16:26:44Z |
| tase_participating_unit_search | TASE | listed_companies_subset | network | 0 | 2026-05-16T16:26:45Z |
| hose_listed_stocks | HOSE | listed_companies_subset | network | 402 | 2026-05-16T16:26:46Z |
| hose_etf_list | HOSE | listed_companies_subset | network | 18 | 2026-05-16T16:26:54Z |
| hose_fund_certificate_list | HOSE | listed_companies_subset | network | 4 | 2026-05-16T16:26:57Z |
| hnx_listed_securities | HNX | exchange_directory | network | 301 | 2026-05-16T16:26:59Z |
| upcom_registered_securities | HNX | exchange_directory | network | 832 | 2026-05-16T16:28:08Z |
| vienna_listed_companies | Wiener Boerse | listed_companies_subset | network | 22 | 2026-05-16T16:31:06Z |
| zagreb_securities_directory | ZSE Croatia | listed_companies_subset | network | 74 | 2026-05-16T16:31:09Z |
| sec_company_tickers_exchange | SEC | exchange_directory | cache | 10086 | 2026-05-16T16:31:10Z |
| otc_markets_security_profile | OTC Markets | security_lookup_subset | network | 746 | 2026-05-16T16:52:15Z |
| otc_markets_stock_screener | OTC Markets | exchange_directory | network | 11957 | 2026-05-16T16:37:12Z |

## Exchange Coverage

| Exchange | Venue Status | Tickers | ISIN | Sector | CIK | FIGI | LEI | Masterfile Symbols | Matches | Collisions | Missing | Match Rate | Verified on Covered |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| ADX | official_full | 86 | 86 | 86 | 0 | 86 | 0 | 122 | 85 | 33 | 4 | 69.67 | 100.0 |
| AMS | official_full | 314 | 310 | 226 | 0 | 307 | 0 | 581 | 239 | 301 | 41 | 41.14 | 100.0 |
| ASX | official_partial | 1298 | 1193 | 1253 | 30 | 1161 | 24 | 0 | 0 | 0 | 0 |  | 99.89 |
| ATHEX | official_partial | 117 | 109 | 117 | 0 | 95 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| B3 | official_full | 1584 | 1359 | 1379 | 0 | 1288 | 0 | 9 | 8 | 0 | 1 | 88.89 | 100.0 |
| BATS | official_full | 1241 | 1219 | 1220 | 0 | 1115 | 0 | 1316 | 1180 | 19 | 117 | 89.67 | 100.0 |
| BCBA | official_partial | 64 | 61 | 50 | 0 | 60 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| BHB | official_full | 29 | 29 | 2 | 0 | 27 | 0 | 41 | 29 | 9 | 3 | 70.73 | 100.0 |
| BIST | official_full | 614 | 614 | 608 | 0 | 614 | 0 | 636 | 614 | 20 | 2 | 96.54 | 100.0 |
| BK | official_full | 104 | 104 | 1 | 0 | 104 | 0 | 140 | 104 | 27 | 9 | 74.29 | 100.0 |
| BME | official_partial | 169 | 169 | 160 | 3 | 169 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| BMV | official_partial | 179 | 160 | 174 | 0 | 159 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| BSE_BW | official_partial | 39 | 39 | 28 | 0 | 37 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| BSE_HU | official_partial | 31 | 23 | 13 | 0 | 23 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| BSE_IN | official_full | 2642 | 2642 | 2599 | 0 | 2626 | 0 | 4847 | 2464 | 752 | 1631 | 50.84 | 93.74 |
| BVB | official_full | 80 | 80 | 76 | 0 | 80 | 0 | 347 | 75 | 121 | 151 | 21.61 | 100.0 |
| BVC | official_partial | 3 | 0 | 3 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| BVL | official_partial | 33 | 31 | 3 | 0 | 31 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| Bursa | official_partial | 936 | 936 | 934 | 0 | 935 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| CPH | official_partial | 131 | 131 | 130 | 0 | 131 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| CSE_LK | official_full | 307 | 307 | 164 | 0 | 305 | 0 | 315 | 307 | 0 | 8 | 97.46 | 100.0 |
| CSE_MA | official_full | 66 | 66 | 2 | 0 | 62 | 0 | 50 | 1 | 37 | 12 | 2.0 | 59.09 |
| DFM | official_full | 46 | 46 | 45 | 0 | 46 | 0 | 71 | 46 | 16 | 9 | 64.79 | 100.0 |
| DSE_TZ | official_partial | 17 | 15 | 2 | 0 | 15 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| EGX | official_partial | 225 | 225 | 196 | 0 | 195 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| Euronext | official_full | 975 | 972 | 755 | 7 | 965 | 65 | 4393 | 936 | 2252 | 1205 | 21.31 | 100.0 |
| GSE | official_partial | 19 | 18 | 2 | 0 | 18 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| HEL | official_partial | 188 | 188 | 185 | 1 | 188 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| HKEX | official_full | 3044 | 3044 | 3005 | 0 | 3035 | 0 | 3154 | 3040 | 82 | 32 | 96.39 | 99.89 |
| HNX | official_full | 105 | 105 | 105 | 0 | 105 | 0 | 301 | 105 | 157 | 39 | 34.88 | 100.0 |
| HOSE | official_partial | 153 | 153 | 153 | 2 | 153 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| ICE_IS | official_partial | 18 | 18 | 16 | 1 | 18 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| IDX | official_full | 694 | 689 | 694 | 1 | 578 | 0 | 958 | 694 | 243 | 21 | 72.44 | 99.71 |
| ISE | official_full | 14 | 14 | 13 | 0 | 14 | 0 | 19 | 9 | 6 | 4 | 47.37 | 100.0 |
| JSE | official_partial | 212 | 204 | 210 | 2 | 167 | 0 | 0 | 0 | 0 | 0 |  |  |
| KOSDAQ | official_full | 1583 | 1578 | 1580 | 0 | 1578 | 0 | 1819 | 1575 | 0 | 244 | 86.59 | 99.49 |
| KRX | official_full | 1796 | 1794 | 1794 | 0 | 1793 | 0 | 2054 | 1784 | 3 | 267 | 86.85 | 99.53 |
| LSE | official_full | 6415 | 6404 | 5701 | 16 | 6386 | 5 | 11015 | 6321 | 1184 | 3510 | 57.39 | 99.02 |
| LUSE | official_partial | 22 | 22 | 2 | 0 | 21 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| MSE_MW | missing | 8 | 8 | 0 | 0 | 7 | 0 | 0 | 0 | 0 | 0 |  |  |
| MSX | official_full | 91 | 1 | 91 | 0 | 0 | 0 | 108 | 91 | 14 | 3 | 84.26 | 100.0 |
| NASDAQ | official_full | 4635 | 4595 | 4612 | 3439 | 4022 | 399 | 5510 | 4528 | 70 | 912 | 82.18 | 99.47 |
| NEO | official_full | 197 | 152 | 164 | 0 | 79 | 0 | 448 | 196 | 87 | 165 | 43.75 | 100.0 |
| NGX | official_full | 145 | 143 | 112 | 0 | 134 | 0 | 133 | 133 | 0 | 0 | 100.0 | 100.0 |
| NMFQS | official_partial | 7 | 7 | 6 | 0 | 7 | 0 | 0 | 0 | 0 | 0 |  |  |
| NSE_IN | official_full | 1234 | 1234 | 1228 | 0 | 1232 | 0 | 2974 | 1211 | 290 | 1473 | 40.72 | 98.34 |
| NSE_KE | official_full | 46 | 46 | 13 | 0 | 43 | 0 | 66 | 10 | 24 | 32 | 15.15 | 100.0 |
| NYSE | official_full | 2082 | 2043 | 2069 | 1997 | 1954 | 372 | 3856 | 2052 | 528 | 1276 | 53.22 | 99.95 |
| NYSE ARCA | official_full | 2653 | 2601 | 2600 | 126 | 2369 | 3 | 2645 | 2576 | 24 | 45 | 97.39 | 100.0 |
| NYSE MKT | official_full | 236 | 235 | 236 | 219 | 208 | 26 | 314 | 232 | 23 | 59 | 73.89 | 100.0 |
| NZX | official_full | 45 | 45 | 23 | 0 | 45 | 0 | 173 | 45 | 126 | 2 | 26.01 | 100.0 |
| OSL | official_full | 241 | 237 | 178 | 2 | 233 | 0 | 296 | 230 | 66 | 0 | 77.7 | 100.0 |
| OTC | official_full | 11056 | 10284 | 9911 | 1818 | 9216 | 0 | 11957 | 7679 | 35 | 4243 | 64.22 | 87.31 |
| PSE | official_full | 90 | 90 | 13 | 1 | 90 | 0 | 381 | 90 | 185 | 106 | 23.62 | 100.0 |
| PSE_CZ | official_partial | 24 | 23 | 12 | 0 | 21 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| PSX | official_full | 373 | 338 | 373 | 3 | 266 | 0 | 714 | 371 | 151 | 192 | 51.96 | 99.18 |
| QSE | official_full | 54 | 27 | 47 | 0 | 0 | 0 | 57 | 54 | 2 | 1 | 94.74 | 100.0 |
| RSE | official_partial | 2 | 2 | 0 | 0 | 2 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| SEM | official_full | 53 | 53 | 4 | 1 | 50 | 0 | 47 | 47 | 0 | 0 | 100.0 | 90.2 |
| SET | official_full | 547 | 541 | 547 | 4 | 342 | 0 | 945 | 545 | 350 | 50 | 57.67 | 99.63 |
| SGX | official_full | 594 | 591 | 515 | 0 | 8 | 0 | 738 | 592 | 142 | 4 | 80.22 | 99.63 |
| SIX | official_partial | 743 | 743 | 743 | 2 | 743 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| SSE | official_partial | 2789 | 2747 | 2789 | 0 | 2175 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| SSE_CL | official_full | 116 | 87 | 101 | 0 | 85 | 0 | 112 | 112 | 0 | 0 | 100.0 | 98.97 |
| STO | official_partial | 725 | 725 | 669 | 2 | 723 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| SZSE | official_partial | 3083 | 3069 | 3083 | 0 | 2594 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| TADAWUL | official_full | 191 | 191 | 188 | 0 | 191 | 0 | 411 | 191 | 217 | 3 | 46.47 | 100.0 |
| TASE | official_partial | 673 | 673 | 647 | 0 | 672 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| TPEX | official_partial | 1118 | 1118 | 1118 | 0 | 917 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| TSE | official_full | 3216 | 3212 | 3178 | 0 | 3212 | 0 | 4449 | 3206 | 1187 | 56 | 72.06 | 99.68 |
| TSX | official_full | 1904 | 1802 | 1663 | 12 | 1237 | 23 | 785 | 324 | 460 | 1 | 41.27 | 99.32 |
| TSXV | official_full | 1066 | 984 | 964 | 17 | 498 | 0 | 1518 | 961 | 556 | 1 | 63.31 | 92.78 |
| TWSE | official_full | 1191 | 1191 | 1190 | 0 | 1165 | 0 | 1086 | 973 | 34 | 79 | 89.59 | 100.0 |
| UPCOM | official_full | 2 | 2 | 2 | 0 | 2 | 0 | 832 | 2 | 468 | 362 | 0.24 | 100.0 |
| USE_UG | official_partial | 7 | 7 | 0 | 0 | 7 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| VSE | official_partial | 36 | 34 | 32 | 0 | 34 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| WSE | official_partial | 348 | 348 | 322 | 7 | 347 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| XETRA | official_full | 3779 | 3776 | 3109 | 8 | 3767 | 2 | 4519 | 3658 | 795 | 66 | 80.95 | 99.42 |
| ZSE | official_partial | 23 | 23 | 1 | 0 | 23 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| ZSE_ZW | official_partial | 27 | 27 | 6 | 0 | 24 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |

## Country Coverage

| Country | Tickers | ISIN | Sector | CIK | FIGI | LEI |
|---|---|---|---|---|---|---|
| Argentina | 60 | 57 | 47 | 0 | 56 | 0 |
| Australia | 1774 | 1669 | 1729 | 230 | 1578 | 62 |
| Austria | 58 | 56 | 53 | 8 | 48 | 8 |
| Bahrain | 30 | 30 | 2 | 0 | 28 | 0 |
| Belgium | 121 | 120 | 106 | 7 | 120 | 1 |
| Bermuda | 533 | 533 | 522 | 58 | 507 | 14 |
| Botswana | 24 | 24 | 20 | 0 | 24 | 0 |
| Brazil | 1560 | 1335 | 1367 | 0 | 1270 | 0 |
| Bulgaria | 14 | 14 | 14 | 3 | 14 | 0 |
| Canada | 4826 | 4599 | 4494 | 644 | 3281 | 65 |
| Cayman Islands | 2104 | 2103 | 2084 | 482 | 1938 | 19 |
| Chile | 115 | 86 | 101 | 0 | 84 | 0 |
| China | 6345 | 6289 | 6344 | 1 | 5239 | 0 |
| Colombia | 3 | 0 | 3 | 0 | 0 | 0 |
| Croatia | 23 | 23 | 1 | 0 | 23 | 0 |
| Cyprus | 19 | 19 | 16 | 2 | 14 | 0 |
| Czech Republic | 22 | 21 | 10 | 0 | 21 | 0 |
| Denmark | 142 | 142 | 139 | 4 | 139 | 0 |
| Egypt | 242 | 242 | 212 | 6 | 209 | 0 |
| Faroe Islands | 3 | 3 | 3 | 0 | 3 | 0 |
| Finland | 192 | 192 | 189 | 1 | 192 | 0 |
| France | 683 | 681 | 580 | 18 | 670 | 59 |
| Gabon | 1 | 1 | 1 | 0 | 1 | 0 |
| Germany | 767 | 764 | 729 | 13 | 757 | 5 |
| Ghana | 20 | 19 | 4 | 0 | 18 | 0 |
| Greece | 124 | 116 | 123 | 4 | 106 | 0 |
| Guernsey | 69 | 69 | 66 | 7 | 65 | 0 |
| Hong Kong | 469 | 469 | 467 | 1 | 468 | 0 |
| Hungary | 22 | 15 | 8 | 0 | 15 | 0 |
| Iceland | 18 | 18 | 16 | 1 | 18 | 0 |
| India | 3875 | 3875 | 3827 | 0 | 3857 | 0 |
| Indonesia | 744 | 739 | 744 | 21 | 628 | 1 |
| Ireland | 2589 | 2589 | 2585 | 67 | 2581 | 7 |
| Isle of Man | 15 | 15 | 15 | 3 | 14 | 0 |
| Israel | 773 | 772 | 760 | 94 | 764 | 1 |
| Italy | 126 | 126 | 117 | 2 | 122 | 0 |
| Japan | 3316 | 3312 | 3269 | 15 | 3307 | 0 |
| Jersey | 173 | 173 | 167 | 19 | 171 | 4 |
| Kazakhstan | 1 | 1 | 0 | 0 | 1 | 0 |
| Kenya | 45 | 45 | 13 | 0 | 42 | 0 |
| Kuwait | 102 | 102 | 1 | 0 | 102 | 0 |
| Liechtenstein | 4 | 4 | 3 | 0 | 4 | 0 |
| Lithuania | 2 | 2 | 2 | 0 | 2 | 0 |
| Luxembourg | 1030 | 1030 | 1023 | 28 | 1026 | 5 |
| Malawi | 8 | 8 | 0 | 0 | 7 | 0 |
| Malaysia | 939 | 939 | 937 | 0 | 932 | 0 |
| Malta | 6 | 6 | 6 | 0 | 6 | 0 |
| Marshall Islands | 38 | 38 | 37 | 31 | 30 | 0 |
| Mauritius | 69 | 69 | 21 | 6 | 64 | 0 |
| Mexico | 136 | 117 | 129 | 5 | 114 | 0 |
| Monaco | 2 | 2 | 2 | 0 | 2 | 0 |
| Morocco | 66 | 66 | 2 | 0 | 62 | 0 |
| Netherlands | 193 | 189 | 172 | 28 | 186 | 1 |
| New Zealand | 80 | 80 | 61 | 5 | 77 | 1 |
| Nigeria | 146 | 144 | 114 | 0 | 135 | 0 |
| Norway | 244 | 240 | 193 | 6 | 233 | 0 |
| Oman | 90 | 0 | 90 | 0 | 0 | 0 |
| Pakistan | 378 | 343 | 376 | 9 | 271 | 0 |
| Panama | 1 | 0 | 1 | 1 | 0 | 0 |
| Peru | 31 | 29 | 2 | 0 | 29 | 0 |
| Philippines | 110 | 110 | 33 | 11 | 109 | 1 |
| Poland | 350 | 350 | 323 | 21 | 349 | 5 |
| Portugal | 34 | 34 | 31 | 0 | 34 | 1 |
| Qatar | 54 | 27 | 47 | 0 | 0 | 0 |
| Romania | 83 | 83 | 78 | 3 | 83 | 0 |
| Rwanda | 2 | 2 | 0 | 0 | 2 | 0 |
| Saudi Arabia | 191 | 191 | 188 | 0 | 191 | 0 |
| Singapore | 548 | 543 | 506 | 15 | 51 | 2 |
| Slovenia | 1 | 1 | 1 | 0 | 1 | 0 |
| South Africa | 230 | 222 | 224 | 5 | 183 | 0 |
| South Korea | 3369 | 3362 | 3364 | 0 | 3361 | 0 |
| Spain | 223 | 223 | 202 | 9 | 222 | 3 |
| Sri Lanka | 307 | 307 | 164 | 0 | 305 | 0 |
| Sweden | 757 | 757 | 698 | 11 | 752 | 1 |
| Switzerland | 390 | 390 | 387 | 21 | 387 | 5 |
| Taiwan | 2273 | 2273 | 2272 | 0 | 2055 | 0 |
| Tanzania | 15 | 13 | 2 | 0 | 13 | 0 |
| Thailand | 565 | 559 | 564 | 27 | 358 | 3 |
| Turkey | 614 | 614 | 608 | 0 | 614 | 0 |
| Uganda | 7 | 7 | 0 | 0 | 7 | 0 |
| United Arab Emirates | 123 | 123 | 122 | 0 | 123 | 0 |
| United Kingdom | 1350 | 1339 | 1279 | 96 | 1319 | 19 |
| United States | 13910 | 13002 | 13238 | 5062 | 11714 | 610 |
| Vietnam | 261 | 261 | 260 | 2 | 260 | 0 |
| Zambia | 22 | 22 | 2 | 0 | 21 | 0 |
| Zimbabwe | 28 | 28 | 7 | 0 | 25 | 0 |

## Unresolved Gaps

| Exchange | Venue Status | Findings | Reference Gap | Missing | Name Mismatch | Collision |
|---|---|---|---|---|---|---|
| OTC | official_full | 4010 | 3055 | 0 | 954 | 1 |
| B3 | official_full | 1205 | 1205 | 0 | 0 | 0 |
| SSE | official_partial | 534 | 534 | 0 | 0 | 0 |
| BSE_IN | official_full | 165 | 0 | 165 | 0 | 0 |
| NASDAQ | official_full | 126 | 108 | 0 | 18 | 0 |
| BME | official_partial | 102 | 102 | 0 | 0 | 0 |
| JSE | official_partial | 90 | 87 | 0 | 3 | 0 |
| TSXV | official_full | 84 | 8 | 76 | 0 | 0 |
| NYSE ARCA | official_full | 77 | 77 | 0 | 0 | 0 |
| Euronext | official_full | 64 | 64 | 0 | 0 | 0 |
| BATS | official_full | 62 | 61 | 0 | 1 | 0 |
| LSE | official_full | 59 | 15 | 43 | 0 | 1 |
| TASE | official_partial | 39 | 39 | 0 | 0 | 0 |
| NYSE | official_full | 36 | 35 | 0 | 0 | 1 |
| EGX | official_partial | 34 | 34 | 0 | 0 | 0 |
| ASX | official_partial | 32 | 31 | 0 | 1 | 0 |
| BSE_HU | official_partial | 29 | 29 | 0 | 0 | 0 |
| CSE_MA | official_full | 27 | 0 | 27 | 0 | 0 |
| ATHEX | official_partial | 26 | 26 | 0 | 0 | 0 |
| NSE_IN | official_full | 22 | 2 | 16 | 0 | 4 |

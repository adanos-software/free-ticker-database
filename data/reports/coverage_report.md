# Coverage Report

## Global

| Metric | Value |
|---|---|
| tickers | 53141 |
| aliases | 88692 |
| stocks | 39086 |
| etfs | 14055 |
| isin_coverage | 41846 |
| sector_coverage | 32099 |
| cik_coverage | 7700 |
| figi_coverage | 3709 |
| lei_coverage | 920 |
| listing_status_rows | 84678 |
| listing_status_intervals | 84678 |
| listing_events | 27371 |
| listing_keys | 60241 |
| instrument_scope_rows | 60241 |
| instrument_scope_core | 44422 |
| instrument_scope_extended | 15819 |
| instrument_scope_primary_listing | 35651 |
| instrument_scope_primary_listing_missing_isin | 8771 |
| instrument_scope_otc_listing | 11097 |
| instrument_scope_secondary_cross_listing | 4722 |
| official_masterfile_symbols | 29689 |
| official_masterfile_matches | 18503 |
| official_masterfile_collisions | 6219 |
| official_masterfile_missing | 4967 |
| official_full_exchanges | 17 |
| official_partial_exchanges | 23 |
| manual_only_exchanges | 0 |
| missing_exchanges | 27 |
| stock_verification_items | 43667 |
| stock_verification_verified | 34017 |
| stock_verification_reference_gap | 9570 |
| stock_verification_missing_from_official | 80 |
| stock_verification_name_mismatch | 0 |
| stock_verification_cross_exchange_collision | 0 |
| etf_verification_items | 16574 |
| etf_verification_verified | 16125 |
| etf_verification_reference_gap | 449 |
| etf_verification_missing_from_official | 0 |
| etf_verification_name_mismatch | 0 |
| etf_verification_cross_exchange_collision | 0 |

## Freshness

| Metric | Value |
|---|---|
| tickers_built_at | 2026-04-11T12:35:25Z |
| tickers_age_hours | 0.08 |
| masterfiles_generated_at | 2026-04-11T12:28:19Z |
| masterfiles_age_hours | 0.2 |
| identifiers_generated_at | 2026-04-11T12:35:25Z |
| identifiers_age_hours | 0.08 |
| listing_history_observed_at | 2026-04-11T12:35:25Z |
| listing_history_age_hours | 0.08 |
| latest_verification_run | data/stock_verification/run-20260411-b3-funds |
| latest_verification_generated_at | 2026-04-11T12:29:29Z |
| latest_verification_age_hours | 0.18 |
| latest_stock_verification_run | data/stock_verification/run-20260411-b3-funds |
| latest_stock_verification_generated_at | 2026-04-11T12:29:29Z |
| latest_stock_verification_age_hours | 0.18 |
| latest_etf_verification_run | data/etf_verification/run-20260411-b3-funds |
| latest_etf_verification_generated_at | 2026-04-11T12:29:46Z |
| latest_etf_verification_age_hours | 0.17 |

## Source Coverage

| Source | Provider | Scope | Mode | Rows | Generated At |
|---|---|---|---|---|---|
| nasdaq_listed | Nasdaq Trader | exchange_directory | network | 5419 | 2026-04-11T06:56:58Z |
| nasdaq_other_listed | Nasdaq Trader | exchange_directory | network | 7084 | 2026-04-11T06:56:58Z |
| lse_company_reports | LSE | listed_companies_subset | cache | 12397 | 2026-04-11T06:56:58Z |
| lse_instrument_search | LSE | security_lookup_subset | network | 1628 | 2026-04-11T11:54:28Z |
| lse_instrument_directory | LSE | security_lookup_subset | cache | 64 | 2026-04-11T06:56:58Z |
| asx_listed_companies | ASX | listed_companies_subset | network | 1979 | 2026-04-11T06:56:58Z |
| cboe_canada_listing_directory | Cboe Canada | exchange_directory | network | 436 | 2026-04-11T06:56:58Z |
| asx_investment_products | ASX | listed_companies_subset | network | 426 | 2026-04-11T06:56:58Z |
| set_listed_companies | SET | listed_companies_subset | network | 933 | 2026-04-11T06:56:58Z |
| set_etf_search | SET | listed_companies_subset | network | 13 | 2026-04-11T06:56:58Z |
| set_dr_search | SET | listed_companies_subset | network | 352 | 2026-04-11T06:56:58Z |
| tmx_listed_issuers | TMX | listed_companies_subset | cache | 3619 | 2026-04-11T06:56:58Z |
| tmx_etf_screener | TMX | listed_companies_subset | cache | 1697 | 2026-04-11T06:56:58Z |
| tmx_interlisted_companies | TMX | interlisted_subset | network | 271 | 2026-04-11T06:56:58Z |
| euronext_equities | Euronext | exchange_directory | network | 3882 | 2026-04-11T11:23:10Z |
| euronext_etfs | Euronext | listed_companies_subset | network | 3465 | 2026-04-11T11:23:10Z |
| jpx_listed_issues | JPX | exchange_directory | network | 4444 | 2026-04-11T06:56:58Z |
| deutsche_boerse_listed_companies | Deutsche Boerse | listed_companies_subset | network | 471 | 2026-04-11T11:23:10Z |
| deutsche_boerse_etfs_etps | Deutsche Boerse | listed_companies_subset | network | 3442 | 2026-04-11T11:23:10Z |
| deutsche_boerse_xetra_all_tradable_equities | Deutsche Boerse | listed_companies_subset | network | 986 | 2026-04-11T11:23:10Z |
| six_equity_issuers | SIX | listed_companies_subset | network | 241 | 2026-04-11T06:56:58Z |
| six_etf_products | SIX | listed_companies_subset | network | 7349 | 2026-04-11T06:56:58Z |
| six_etp_products | SIX | listed_companies_subset | network | 752 | 2026-04-11T06:56:58Z |
| b3_instruments_equities | B3 | exchange_directory | network | 1218 | 2026-04-11T12:28:19Z |
| b3_listed_etfs | B3 | listed_companies_subset | network | 187 | 2026-04-11T06:56:58Z |
| b3_bdr_etfs | B3 | listed_companies_subset | network | 302 | 2026-04-11T06:56:58Z |
| jse_etf_list | JSE | listed_companies_subset | cache | 133 | 2026-04-11T06:56:58Z |
| jse_etn_list | JSE | listed_companies_subset | cache | 94 | 2026-04-11T06:56:58Z |
| jse_instrument_search | JSE | listed_companies_subset | network | 53 | 2026-04-11T06:56:58Z |
| bme_listed_companies | BME | listed_companies_subset | network | 57 | 2026-04-11T06:56:58Z |
| bme_etf_list | BME | listed_companies_subset | cache | 5 | 2026-04-11T06:56:58Z |
| bme_listed_values | BME | listed_companies_subset | network | 156 | 2026-04-11T08:55:05Z |
| bme_growth_prices | BME Growth | listed_companies_subset | unavailable | 0 | 2026-04-11T12:16:59Z |
| bursa_equity_isin | Bursa Malaysia | listed_companies_subset | network | 1123 | 2026-04-11T06:56:58Z |
| bmv_stock_search | BMV | listed_companies_subset | network | 151 | 2026-04-11T06:56:58Z |
| bmv_capital_trust_search | BMV | listed_companies_subset | network | 16 | 2026-04-11T06:56:58Z |
| bmv_etf_search | BMV | listed_companies_subset | network | 9 | 2026-04-11T06:56:58Z |
| bmv_issuer_directory | BMV | listed_companies_subset | network | 12 | 2026-04-11T09:57:43Z |
| nasdaq_nordic_stockholm_shares | Nasdaq Nordic | listed_companies_subset | cache | 747 | 2026-04-11T06:56:58Z |
| nasdaq_nordic_stockholm_shares_search | Nasdaq Nordic | listed_companies_subset | cache | 1 | 2026-04-11T06:56:58Z |
| nasdaq_nordic_helsinki_shares | Nasdaq Nordic | listed_companies_subset | cache | 194 | 2026-04-11T06:56:58Z |
| nasdaq_nordic_helsinki_shares_search | Nasdaq Nordic | listed_companies_subset | cache | 3 | 2026-04-11T06:56:58Z |
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
| sse_a_share_list | SSE | listed_companies_subset | network | 2351 | 2026-04-11T06:56:58Z |
| sse_etf_list | SSE | listed_companies_subset | network | 845 | 2026-04-11T06:56:58Z |
| szse_a_share_list | SZSE | listed_companies_subset | network | 2887 | 2026-04-11T06:56:58Z |
| szse_b_share_list | SZSE | listed_companies_subset | network | 38 | 2026-04-11T06:56:58Z |
| szse_etf_list | SZSE | listed_companies_subset | network | 633 | 2026-04-11T06:56:58Z |
| tpex_mainboard_daily_quotes | TPEX | listed_companies_subset | cache | 884 | 2026-04-11T10:58:35Z |
| tpex_etf_filter | TPEX | listed_companies_subset | cache | 112 | 2026-04-11T10:58:35Z |
| tpex_mainboard_basic_info | MOPS | listed_companies_subset | cache | 881 | 2026-04-11T10:58:35Z |
| tpex_emerging_basic_info | MOPS | listed_companies_subset | cache | 354 | 2026-04-11T10:58:35Z |
| krx_listed_companies | KRX | listed_companies_subset | network | 2772 | 2026-04-11T11:45:54Z |
| krx_etf_finder | KRX | listed_companies_subset | network | 1088 | 2026-04-11T11:45:54Z |
| psx_listed_companies | PSX | listed_companies_subset | network | 565 | 2026-04-11T06:56:58Z |
| psx_symbol_name_daily | PSX | listed_companies_subset | network | 370 | 2026-04-11T06:56:58Z |
| pse_listed_company_directory | PSE | exchange_directory | network | 381 | 2026-04-11T06:56:58Z |
| idx_listed_companies | IDX | listed_companies_subset | network | 957 | 2026-04-11T06:56:58Z |
| wse_listed_companies | GPW | listed_companies_subset | cache | 399 | 2026-04-11T06:56:58Z |
| newconnect_listed_companies | NewConnect | listed_companies_subset | cache | 364 | 2026-04-11T06:56:58Z |
| wse_etf_list | GPW | listed_companies_subset | cache | 25 | 2026-04-11T06:56:58Z |
| tase_securities_marketdata | TASE | listed_companies_subset | network | 524 | 2026-04-11T06:56:58Z |
| tase_etf_marketdata | TASE | listed_companies_subset | network | 460 | 2026-04-11T06:56:58Z |
| tase_foreign_etf_search | TASE | listed_companies_subset | network | 15 | 2026-04-11T07:41:23Z |
| tase_participating_unit_search | TASE | listed_companies_subset | network | 16 | 2026-04-11T07:41:33Z |
| hose_listed_stocks | HOSE | listed_companies_subset | cache | 403 | 2026-04-11T06:56:58Z |
| hose_etf_list | HOSE | listed_companies_subset | cache | 18 | 2026-04-11T06:56:58Z |
| hose_fund_certificate_list | HOSE | listed_companies_subset | cache | 4 | 2026-04-11T06:56:58Z |
| hnx_listed_securities | HNX | exchange_directory | cache | 302 | 2026-04-11T06:56:58Z |
| upcom_registered_securities | HNX | exchange_directory | cache | 837 | 2026-04-11T06:56:58Z |
| sec_company_tickers_exchange | SEC | exchange_directory | cache | 10117 | 2026-04-11T06:56:58Z |
| otc_markets_security_profile | OTC Markets | security_lookup_subset | cache | 1705 | 2026-04-11T10:46:59Z |

## Exchange Coverage

| Exchange | Venue Status | Tickers | ISIN | Sector | CIK | FIGI | LEI | Masterfile Symbols | Matches | Collisions | Missing | Match Rate | Verified on Covered |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| AMS | official_full | 222 | 197 | 131 | 0 | 144 | 0 | 548 | 148 | 294 | 106 | 27.01 | 100.0 |
| ASX | official_partial | 1298 | 1035 | 702 | 30 | 1032 | 24 | 0 | 0 | 0 | 0 |  | 100.0 |
| ATHEX | missing | 117 | 91 | 117 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| B3 | official_full | 959 | 673 | 443 | 0 | 0 | 0 | 1218 | 627 | 0 | 591 | 51.48 | 100.0 |
| BATS | official_full | 1247 | 682 | 344 | 0 | 0 | 0 | 1243 | 1200 | 7 | 36 | 96.54 | 100.0 |
| BCBA | missing | 64 | 52 | 50 | 0 | 48 | 0 | 0 | 0 | 0 | 0 |  |  |
| BME | official_partial | 169 | 169 | 157 | 3 | 2 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| BMV | official_partial | 179 | 160 | 157 | 0 | 1 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| BSE_BW | missing | 39 | 39 | 6 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| BSE_HU | missing | 31 | 16 | 9 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| BVB | missing | 85 | 79 | 77 | 1 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| BVL | missing | 33 | 0 | 3 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| Bursa | official_partial | 926 | 926 | 922 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| CPH | official_partial | 131 | 131 | 111 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| CSE_LK | missing | 3 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| CSE_MA | missing | 66 | 66 | 2 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| DSE_TZ | missing | 17 | 15 | 2 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| EGX | missing | 225 | 225 | 125 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| Euronext | official_full | 975 | 967 | 698 | 7 | 665 | 65 | 4385 | 938 | 2132 | 1315 | 21.39 | 100.0 |
| GSE | missing | 19 | 18 | 2 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| HEL | official_partial | 188 | 188 | 137 | 1 | 0 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| HNX | official_full | 105 | 105 | 13 | 0 | 0 | 0 | 302 | 105 | 158 | 39 | 34.77 | 100.0 |
| HOSE | official_partial | 153 | 153 | 138 | 2 | 0 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| ICE_IS | missing | 18 | 17 | 3 | 1 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| IDX | official_partial | 694 | 579 | 508 | 1 | 2 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| ISE | missing | 14 | 14 | 10 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| JSE | official_partial | 213 | 183 | 98 | 2 | 0 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| KOSDAQ | official_partial | 1583 | 1578 | 1202 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| KRX | official_partial | 1788 | 1786 | 807 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| LSE | official_partial | 6408 | 6396 | 4355 | 16 | 22 | 5 | 0 | 0 | 0 | 0 |  | 100.0 |
| LUSE | missing | 22 | 22 | 1 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| MSE_MW | missing | 8 | 5 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| NASDAQ | official_full | 4634 | 3923 | 3615 | 3441 | 10 | 400 | 5449 | 4568 | 48 | 833 | 83.83 | 100.0 |
| NEO | official_full | 197 | 78 | 21 | 0 | 0 | 0 | 436 | 196 | 82 | 158 | 44.95 | 100.0 |
| NGX | missing | 147 | 145 | 4 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| NMFQS | missing | 7 | 7 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| NSE_KE | missing | 46 | 46 | 6 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| NYSE | official_full | 2078 | 1877 | 1966 | 1993 | 24 | 372 | 3836 | 2060 | 513 | 1263 | 53.7 | 100.0 |
| NYSE ARCA | official_full | 2650 | 2273 | 1308 | 127 | 6 | 3 | 2615 | 2582 | 14 | 19 | 98.74 | 100.0 |
| NYSE MKT | official_full | 238 | 226 | 228 | 221 | 2 | 26 | 313 | 234 | 19 | 60 | 74.76 | 100.0 |
| NYSEARCA | missing | 1 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| OSL | official_full | 240 | 202 | 170 | 2 | 196 | 0 | 298 | 233 | 64 | 1 | 78.19 | 100.0 |
| OTC | official_partial | 11097 | 8573 | 8309 | 1795 | 5 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| PSE | official_full | 90 | 90 | 4 | 1 | 0 | 0 | 381 | 90 | 185 | 106 | 23.62 | 100.0 |
| PSE_CZ | missing | 24 | 18 | 10 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| PSX | official_partial | 373 | 269 | 15 | 3 | 0 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| RSE | missing | 2 | 2 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| SEM | missing | 53 | 53 | 3 | 1 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| SET | official_partial | 547 | 352 | 534 | 4 | 0 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| SIX | official_partial | 743 | 739 | 496 | 2 | 0 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| SSE | official_partial | 2789 | 2175 | 1492 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| SSE_CL | missing | 116 | 85 | 94 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| STO | official_partial | 725 | 725 | 474 | 2 | 3 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| SZSE | official_partial | 3083 | 2596 | 1928 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| TASE | official_partial | 673 | 673 | 303 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| TPEX | official_partial | 1118 | 1107 | 1000 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| TSE | official_full | 3214 | 7 | 0 | 0 | 0 | 0 | 4444 | 3214 | 1199 | 31 | 72.32 | 100.0 |
| TSX | official_full | 1601 | 1081 | 767 | 12 | 991 | 23 | 785 | 323 | 460 | 2 | 41.15 | 99.32 |
| TSXV | official_full | 1066 | 503 | 625 | 17 | 524 | 0 | 1518 | 961 | 557 | 0 | 63.31 | 92.71 |
| TWSE | official_full | 1240 | 1166 | 987 | 0 | 0 | 0 | 1081 | 1022 | 29 | 30 | 94.54 | 100.0 |
| UPCOM | official_full | 2 | 2 | 1 | 0 | 0 | 0 | 837 | 2 | 458 | 377 | 0.24 | 100.0 |
| USE_UG | missing | 7 | 7 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| VSE | missing | 36 | 34 | 32 | 0 | 22 | 0 | 0 | 0 | 0 | 0 |  |  |
| WSE | official_partial | 348 | 348 | 258 | 7 | 1 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| XETRA | official_partial | 3007 | 2970 | 1333 | 8 | 9 | 2 | 0 | 0 | 0 | 0 |  | 100.0 |
| ZSE | missing | 23 | 0 | 1 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| ZSE_ZW | missing | 27 | 27 | 3 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |

## Country Coverage

| Country | Tickers | ISIN | Sector | CIK | FIGI | LEI |
|---|---|---|---|---|---|---|
| Argentina | 61 | 49 | 47 | 0 | 48 | 0 |
| Australia | 1944 | 1681 | 1302 | 308 | 1063 | 67 |
| Austria | 55 | 53 | 47 | 11 | 48 | 11 |
| Belgium | 122 | 121 | 79 | 8 | 38 | 2 |
| Bermuda | 166 | 166 | 155 | 59 | 1 | 13 |
| Botswana | 25 | 25 | 1 | 0 | 0 | 0 |
| Brazil | 930 | 643 | 424 | 0 | 0 | 0 |
| Canada | 4738 | 3535 | 2785 | 667 | 1491 | 69 |
| Cayman Islands | 623 | 618 | 477 | 442 | 1 | 16 |
| Chile | 116 | 85 | 93 | 0 | 0 | 0 |
| China | 5949 | 4850 | 3497 | 10 | 0 | 0 |
| Croatia | 23 | 0 | 1 | 0 | 0 | 0 |
| Cyprus | 10 | 10 | 9 | 2 | 0 | 0 |
| Czech Republic | 22 | 16 | 7 | 0 | 0 | 0 |
| Denmark | 145 | 145 | 122 | 4 | 0 | 0 |
| Egypt | 254 | 254 | 153 | 13 | 0 | 1 |
| Finland | 193 | 193 | 141 | 1 | 0 | 0 |
| France | 708 | 702 | 549 | 27 | 484 | 60 |
| Germany | 699 | 662 | 454 | 17 | 10 | 6 |
| Ghana | 18 | 17 | 2 | 0 | 0 | 0 |
| Greece | 130 | 104 | 129 | 4 | 0 | 0 |
| Guernsey | 61 | 61 | 55 | 7 | 1 | 0 |
| Hong Kong | 39 | 39 | 36 | 1 | 1 | 0 |
| Hungary | 29 | 15 | 8 | 0 | 0 | 0 |
| Iceland | 18 | 17 | 3 | 1 | 0 | 0 |
| India | 1 | 1 | 1 | 0 | 0 | 0 |
| Indonesia | 797 | 682 | 606 | 49 | 4 | 4 |
| Ireland | 2405 | 2405 | 1072 | 62 | 63 | 7 |
| Isle of Man | 9 | 9 | 9 | 3 | 0 | 0 |
| Israel | 772 | 772 | 413 | 99 | 0 | 2 |
| Italy | 116 | 116 | 98 | 2 | 5 | 0 |
| Japan | 3309 | 102 | 98 | 36 | 0 | 0 |
| Jersey | 146 | 146 | 54 | 18 | 0 | 4 |
| Kenya | 45 | 45 | 6 | 0 | 0 | 0 |
| Luxembourg | 948 | 948 | 548 | 28 | 44 | 5 |
| Malawi | 8 | 5 | 0 | 0 | 0 | 0 |
| Malaysia | 922 | 922 | 917 | 0 | 0 | 0 |
| Mauritius | 51 | 51 | 3 | 1 | 0 | 0 |
| Mexico | 138 | 119 | 121 | 4 | 0 | 0 |
| Morocco | 66 | 66 | 2 | 0 | 0 | 0 |
| Netherlands | 177 | 152 | 134 | 27 | 79 | 1 |
| New Zealand | 13 | 13 | 13 | 7 | 1 | 1 |
| Nigeria | 145 | 143 | 3 | 0 | 0 | 0 |
| Norway | 281 | 243 | 210 | 9 | 193 | 1 |
| Pakistan | 367 | 263 | 13 | 3 | 0 | 0 |
| Panama | 1 | 0 | 1 | 1 | 0 | 0 |
| Peru | 33 | 0 | 3 | 0 | 0 | 0 |
| Philippines | 114 | 114 | 25 | 15 | 0 | 2 |
| Poland | 370 | 370 | 272 | 31 | 1 | 5 |
| Portugal | 34 | 34 | 25 | 0 | 9 | 1 |
| Romania | 86 | 82 | 76 | 7 | 0 | 0 |
| Rwanda | 2 | 2 | 0 | 0 | 0 | 0 |
| Singapore | 45 | 43 | 40 | 18 | 0 | 2 |
| South Africa | 217 | 187 | 98 | 5 | 0 | 0 |
| South Korea | 3361 | 3353 | 2001 | 1 | 0 | 0 |
| Spain | 220 | 220 | 185 | 10 | 7 | 3 |
| Sri Lanka | 3 | 0 | 0 | 0 | 0 | 0 |
| Sweden | 759 | 759 | 506 | 14 | 3 | 1 |
| Switzerland | 368 | 366 | 241 | 24 | 6 | 6 |
| Taiwan | 2342 | 2257 | 1971 | 0 | 0 | 0 |
| Tanzania | 15 | 13 | 2 | 0 | 0 | 0 |
| Thailand | 584 | 389 | 570 | 42 | 1 | 6 |
| Uganda | 7 | 7 | 0 | 0 | 0 | 0 |
| United Kingdom | 1523 | 1511 | 1074 | 112 | 11 | 24 |
| United States | 14707 | 10532 | 9715 | 4873 | 15 | 553 |
| Vietnam | 262 | 262 | 152 | 2 | 1 | 0 |
| Zambia | 22 | 22 | 1 | 0 | 0 | 0 |
| Zimbabwe | 27 | 27 | 3 | 0 | 0 | 0 |

## Unresolved Gaps

| Exchange | Venue Status | Findings | Reference Gap | Missing | Name Mismatch | Collision |
|---|---|---|---|---|---|---|
| OTC | official_partial | 7974 | 7974 | 0 | 0 | 0 |
| B3 | official_full | 280 | 280 | 0 | 0 | 0 |
| EGX | missing | 225 | 225 | 0 | 0 | 0 |
| NGX | missing | 147 | 147 | 0 | 0 | 0 |
| ATHEX | missing | 117 | 117 | 0 | 0 | 0 |
| SSE_CL | missing | 116 | 116 | 0 | 0 | 0 |
| BME | official_partial | 93 | 93 | 0 | 0 | 0 |
| BVB | missing | 85 | 85 | 0 | 0 | 0 |
| TSXV | official_full | 82 | 5 | 77 | 0 | 0 |
| NYSE ARCA | official_full | 68 | 68 | 0 | 0 | 0 |
| NASDAQ | official_full | 67 | 67 | 0 | 0 | 0 |
| CSE_MA | missing | 66 | 66 | 0 | 0 | 0 |
| BCBA | missing | 64 | 64 | 0 | 0 | 0 |
| SEM | missing | 53 | 53 | 0 | 0 | 0 |
| LSE | official_partial | 52 | 52 | 0 | 0 | 0 |
| BATS | official_full | 48 | 48 | 0 | 0 | 0 |
| NSE_KE | missing | 46 | 46 | 0 | 0 | 0 |
| BSE_BW | missing | 39 | 39 | 0 | 0 | 0 |
| VSE | missing | 36 | 36 | 0 | 0 | 0 |
| Euronext | official_full | 35 | 35 | 0 | 0 | 0 |

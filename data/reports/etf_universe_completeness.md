# ETF Universe Completeness

Generated at: `2026-07-07T10:09:48Z`

This report compares active official ETF masterfile rows against the listing-keyed DB universe. Missing rows are review candidates only.

## Summary

| Metric | Value |
|---|---:|
| official_etf_rows | 48353 |
| matched_etf_listings | 24957 |
| missing_or_review_rows | 23396 |
| missing_from_db | 16553 |
| collision_hidden_by_global_ticker | 6591 |
| local_listing_asset_type_mismatch | 252 |
| etf_recall_pct | 51.61 |
| source_count | 70 |
| exchange_count | 54 |

## By Exchange

| Exchange | Official ETF Rows | Matched | Missing/Review | Missing From DB | Collision-Hidden | Asset-Type Review | Recall % |
|---|---:|---:|---:|---:|---:|---:|---:|
| SIX | 9537 | 562 | 8975 | 7721 | 1254 | 0 | 5.89 |
| LSE | 9772 | 4838 | 4934 | 3615 | 1231 | 88 | 49.51 |
| Borsa Italiana | 2431 | 0 | 2431 | 838 | 1592 | 1 | 0.0 |
| XETRA | 7132 | 5738 | 1394 | 349 | 1045 | 0 | 80.45 |
| Euronext | 1130 | 294 | 836 | 352 | 484 | 0 | 26.02 |
| TSX | 3078 | 2382 | 696 | 330 | 366 | 0 | 77.39 |
| NYSE | 652 | 82 | 570 | 399 | 95 | 76 | 12.58 |
| SSE | 881 | 532 | 349 | 349 | 0 | 0 | 60.39 |
| BATS | 1486 | 1157 | 329 | 314 | 15 | 0 | 77.86 |
| AMS | 477 | 152 | 325 | 65 | 260 | 0 | 31.87 |
| SZSE | 662 | 357 | 305 | 304 | 1 | 0 | 53.93 |
| ASX | 585 | 356 | 229 | 102 | 124 | 3 | 60.85 |
| OTC | 433 | 228 | 205 | 160 | 1 | 44 | 52.66 |
| KRX | 1136 | 936 | 200 | 200 | 0 | 0 | 82.39 |
| NSE_IN | 326 | 146 | 180 | 179 | 1 | 0 | 44.79 |
| BSE_IN | 185 | 7 | 178 | 174 | 4 | 0 | 3.78 |
| NASDAQ | 1329 | 1151 | 178 | 126 | 12 | 40 | 86.61 |
| NEO | 329 | 151 | 178 | 137 | 41 | 0 | 45.9 |
| B3 | 1384 | 1208 | 176 | 176 | 0 | 0 | 87.28 |
| TASE | 478 | 314 | 164 | 164 | 0 | 0 | 65.69 |
| TSE | 832 | 707 | 125 | 125 | 0 | 0 | 84.98 |
| NYSE ARCA | 2655 | 2532 | 123 | 104 | 19 | 0 | 95.37 |
| JSE | 228 | 125 | 103 | 102 | 1 | 0 | 54.82 |
| SGX | 91 | 47 | 44 | 33 | 11 | 0 | 51.65 |
| STO | 39 | 14 | 25 | 12 | 13 | 0 | 35.9 |
| Bursa | 31 | 8 | 23 | 23 | 0 | 0 | 25.81 |
| NZX | 44 | 23 | 21 | 13 | 8 | 0 | 52.27 |
| SET | 32 | 14 | 18 | 11 | 7 | 0 | 43.75 |
| TWSE | 220 | 202 | 18 | 18 | 0 | 0 | 91.82 |
| HKEX | 408 | 398 | 10 | 10 | 0 | 0 | 97.55 |
| WSE | 27 | 17 | 10 | 10 | 0 | 0 | 62.96 |
| TADAWUL | 17 | 9 | 8 | 8 | 0 | 0 | 52.94 |
| TSXV | 13 | 5 | 8 | 6 | 2 | 0 | 38.46 |
| ADX | 23 | 17 | 6 | 3 | 3 | 0 | 73.91 |
| HOSE | 18 | 13 | 5 | 5 | 0 | 0 | 72.22 |
| TPEX | 118 | 113 | 5 | 5 | 0 | 0 | 95.76 |
| BVB | 9 | 5 | 4 | 4 | 0 | 0 | 55.56 |
| ISE | 4 | 0 | 4 | 4 | 0 | 0 | 0.0 |
| NSE_KE | 2 | 0 | 2 | 1 | 1 | 0 | 0.0 |
| HEL | 2 | 1 | 1 | 1 | 0 | 0 | 50.0 |
| NMFQS | 7 | 6 | 1 | 1 | 0 | 0 | 85.71 |
| BIST | 30 | 30 | 0 | 0 | 0 | 0 | 100.0 |
| BME | 5 | 5 | 0 | 0 | 0 | 0 | 100.0 |
| BMV | 19 | 19 | 0 | 0 | 0 | 0 | 100.0 |
| BVL | 1 | 1 | 0 | 0 | 0 | 0 | 100.0 |
| CPH | 1 | 1 | 0 | 0 | 0 | 0 | 100.0 |
| DFM | 2 | 2 | 0 | 0 | 0 | 0 | 100.0 |
| OSL | 2 | 2 | 0 | 0 | 0 | 0 | 100.0 |
| PSE | 1 | 1 | 0 | 0 | 0 | 0 | 100.0 |
| PSX | 27 | 27 | 0 | 0 | 0 | 0 | 100.0 |
| QSE | 2 | 2 | 0 | 0 | 0 | 0 | 100.0 |
| SEM | 1 | 1 | 0 | 0 | 0 | 0 | 100.0 |
| SSE_CL | 16 | 16 | 0 | 0 | 0 | 0 | 100.0 |
| ZSE_ZW | 3 | 3 | 0 | 0 | 0 | 0 | 100.0 |

## By Source

| Source | Provider | Scope | Mode | Official ETF Rows | Matched | Missing/Review | Recall % |
|---|---|---|---|---:|---:|---:|---:|
| six_etf_products | SIX | listed_companies_subset | network | 8707 | 514 | 8193 | 5.9 |
| euronext_etfs | Euronext | exchange_directory | network | 4039 | 445 | 3594 | 11.02 |
| lse_company_reports | LSE | listed_companies_subset | cache | 4615 | 2128 | 2487 | 46.11 |
| lse_price_explorer | LSE | exchange_directory | network | 5157 | 2710 | 2447 | 52.55 |
| six_etp_products | SIX | listed_companies_subset | network | 830 | 48 | 782 | 5.78 |
| deutsche_boerse_etfs_etps | Deutsche Boerse | listed_companies_subset | network | 3565 | 2868 | 697 | 80.45 |
| deutsche_boerse_xetra_all_tradable_equities | Deutsche Boerse | exchange_directory | network | 3567 | 2870 | 697 | 80.46 |
| sec_company_tickers_exchange | SEC | listed_companies_subset | cache | 716 | 30 | 686 | 4.19 |
| nasdaq_other_listed | Nasdaq Trader | exchange_directory | network | 4216 | 3757 | 459 | 89.11 |
| tmx_etf_screener | TMX | listed_companies_subset | network | 1770 | 1412 | 358 | 79.77 |
| sse_etf_list | SSE | listed_companies_subset | network | 881 | 532 | 349 | 60.39 |
| tmx_listed_issuers | TMX | listed_companies_subset | network | 1316 | 975 | 341 | 74.09 |
| szse_etf_list | SZSE | listed_companies_subset | network | 662 | 357 | 305 | 53.93 |
| krx_etf_finder | KRX | exchange_directory | network | 1136 | 936 | 200 | 82.39 |
| asx_investment_products | ASX | listed_companies_subset | network | 446 | 255 | 191 | 57.17 |
| nse_india_securities_available | NSE India | exchange_directory | network | 326 | 146 | 180 | 44.79 |
| bse_india_scrips | BSE India | exchange_directory | network | 185 | 7 | 178 | 3.78 |
| cboe_canada_listing_directory | Cboe Canada | exchange_directory | network | 329 | 151 | 178 | 45.9 |
| tase_etf_marketdata | TASE | listed_companies_subset | network | 463 | 299 | 164 | 64.58 |
| otc_markets_stock_screener | OTC Markets | exchange_directory | cache | 272 | 118 | 154 | 43.38 |
| jpx_listed_issues | JPX | exchange_directory | network | 478 | 353 | 125 | 73.85 |
| nasdaq_listed | Nasdaq Trader | exchange_directory | network | 1243 | 1137 | 106 | 91.47 |
| b3_bdr_etfs | B3 | listed_companies_subset | network | 306 | 220 | 86 | 71.9 |
| b3_instruments_equities | B3 | exchange_directory | cache | 889 | 834 | 55 | 93.81 |
| jse_etn_list | JSE | listed_companies_subset | cache | 94 | 39 | 55 | 41.49 |
| jse_etf_list | JSE | listed_companies_subset | cache | 134 | 86 | 48 | 64.18 |
| sgx_securities_prices | SGX | exchange_directory | cache | 91 | 47 | 44 | 51.65 |
| asx_listed_companies | ASX | listed_companies_subset | network | 139 | 101 | 38 | 72.66 |
| b3_listed_etfs | B3 | listed_companies_subset | network | 189 | 154 | 35 | 81.48 |
| nasdaq_nordic_stockholm_etfs | Nasdaq Nordic | listed_companies_subset | cache | 33 | 12 | 21 | 36.36 |
| nzx_instruments | NZX | exchange_directory | cache | 44 | 23 | 21 | 52.27 |
| twse_etf_list | TWSE | listed_companies_subset | network | 220 | 202 | 18 | 91.82 |
| bursa_closing_prices | Bursa Malaysia | listed_companies_subset | network | 17 | 4 | 13 | 23.53 |
| bursa_equity_isin | Bursa Malaysia | listed_companies_subset | network | 14 | 4 | 10 | 28.57 |
| hkex_securities_list | HKEX | exchange_directory | network | 408 | 398 | 10 | 97.55 |
| wse_etf_list | GPW | listed_companies_subset | cache | 27 | 17 | 10 | 62.96 |
| tadawul_main_market_watch | Saudi Exchange | exchange_directory | cache | 17 | 9 | 8 | 52.94 |
| adx_market_watch | ADX | exchange_directory | cache | 23 | 17 | 6 | 73.91 |
| set_etf_search | SET | listed_companies_subset | network | 13 | 7 | 6 | 53.85 |
| set_listed_companies | SET | listed_companies_subset | network | 6 | 0 | 6 | 0.0 |
| set_stock_search | SET | exchange_directory | network | 13 | 7 | 6 | 53.85 |
| hose_etf_list | HOSE | listed_companies_subset | cache | 18 | 13 | 5 | 72.22 |
| tmx_interlisted_companies | TMX | interlisted_subset | network | 5 | 0 | 5 | 0.0 |
| tpex_etf_filter | TPEX | listed_companies_subset | cache | 113 | 108 | 5 | 95.58 |
| bvb_fund_units_directory | BVB | listed_companies_subset | cache | 9 | 5 | 4 | 55.56 |
| nasdaq_nordic_stockholm_trackers | Nasdaq Nordic | listed_companies_subset | cache | 6 | 2 | 4 | 33.33 |
| euronext_equities | Euronext | secondary_listing_subset | network | 5 | 3 | 2 | 60.0 |
| nse_ke_listed_companies | NSE Kenya | exchange_directory | cache | 2 | 0 | 2 | 0.0 |
| nasdaq_mutual_fund_quotes | Nasdaq | security_lookup_subset | cache | 7 | 6 | 1 | 85.71 |
| nasdaq_nordic_helsinki_etfs | Nasdaq Nordic | listed_companies_subset | cache | 2 | 1 | 1 | 50.0 |
| bist_kap_mkk_listed_securities | KAP/MKK | exchange_directory | cache | 30 | 30 | 0 | 100.0 |
| bme_etf_list | BME | listed_companies_subset | network | 5 | 5 | 0 | 100.0 |
| bmv_etf_search | BMV | listed_companies_subset | network | 7 | 7 | 0 | 100.0 |
| bmv_issuer_directory | BMV | listed_companies_subset | network | 5 | 5 | 0 | 100.0 |
| bmv_market_data_securities | BMV | listed_companies_subset | network | 7 | 7 | 0 | 100.0 |
| bolsa_santiago_instruments | Bolsa de Santiago | exchange_directory | cache | 16 | 16 | 0 | 100.0 |
| bvl_issuers_directory | CAVALI | security_lookup_subset | cache | 1 | 1 | 0 | 100.0 |
| dfm_listed_securities | DFM | exchange_directory | cache | 2 | 2 | 0 | 100.0 |
| jpx_tse_stock_detail | JPX | security_identifier_registry_subset | network | 354 | 354 | 0 | 100.0 |
| nasdaq_nordic_copenhagen_etfs | Nasdaq Nordic | listed_companies_subset | cache | 1 | 1 | 0 | 100.0 |
| otc_markets_security_profile | OTC Markets | security_lookup_subset | network | 108 | 108 | 0 | 100.0 |
| pse_listed_company_directory | PSE | exchange_directory | network | 1 | 1 | 0 | 100.0 |
| psx_dps_symbols | PSX | exchange_directory | network | 9 | 9 | 0 | 100.0 |
| psx_listed_companies | PSX | listed_companies_subset | network | 9 | 9 | 0 | 100.0 |
| psx_symbol_name_daily | PSX | listed_companies_subset | network | 9 | 9 | 0 | 100.0 |
| qse_market_watch | QSE | exchange_directory | cache | 2 | 2 | 0 | 100.0 |
| sem_isin | SEM | exchange_directory | cache | 1 | 1 | 0 | 100.0 |
| tase_foreign_etf_search | TASE | listed_companies_subset | network | 15 | 15 | 0 | 100.0 |
| tpex_mainboard_daily_quotes | TPEX | listed_companies_subset | network | 5 | 5 | 0 | 100.0 |
| zse_zw_listed_companies | ZSE Zimbabwe | listed_companies_subset | cache | 3 | 3 | 0 | 100.0 |

## Top Missing Or Review Rows

| Exchange | Ticker | Source | Status | Candidate Action | Source Gate |
|---|---|---|---|---|---|
| ADX | AGIX | adx_market_watch | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| ADX | INDI | adx_market_watch | missing_from_db | review_official_listing_add | add_only_after_official_identity_isin_checksum_and_no_collision_review |
| ADX | KRBN | adx_market_watch | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| ADX | KWEB | adx_market_watch | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| ADX | KWIN | adx_market_watch | missing_from_db | review_official_listing_add | add_only_after_official_identity_isin_checksum_and_no_collision_review |
| ADX | LUXURY | adx_market_watch | missing_from_db | review_official_listing_add | add_only_after_official_identity_isin_checksum_and_no_collision_review |
| AMS | 2AAP | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | 2AMD | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | 2AMZ | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | 2BAB | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | 2BRK | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | 2FB | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | 2GOO | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | 2MSF | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | 2NFL | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | 2TSL | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | 2VIS | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | 3AAP | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | 3AMD | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | 3AMZ | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | 3BAB | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | 3BP | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | 3CON | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | 3FB | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | 3GDX | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | 3GOO | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | 3KWE | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | 3MSF | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | 3NFL | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | 3NVD | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | 3PLT | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | 3SMH | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | 3TSL | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | 3TSM | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | 5QQQ | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | 5SPY | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | AAPY | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | ADPT | euronext_etfs | missing_from_db | review_official_listing_add | add_only_after_official_identity_isin_checksum_and_no_collision_review |
| AMS | AGED | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | AGGH | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | AMDY | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | AMZN | euronext_etfs | missing_from_db | review_official_listing_add | add_only_after_official_identity_isin_checksum_and_no_collision_review |
| AMS | AMZY | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | AUCO | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | AVGY | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | AWSR | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | BABY | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | BATT | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | BIOT | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | BITC | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | BLC3 | euronext_etfs | missing_from_db | review_official_listing_add | add_only_after_official_identity_isin_checksum_and_no_collision_review |
| AMS | BLCY | euronext_etfs | missing_from_db | review_official_listing_add | add_only_after_official_identity_isin_checksum_and_no_collision_review |
| AMS | BLNT | euronext_etfs | missing_from_db | review_official_listing_add | add_only_after_official_identity_isin_checksum_and_no_collision_review |
| AMS | BRIC | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | BRKY | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | BSEU | euronext_etfs | missing_from_db | review_official_listing_add | add_only_after_official_identity_isin_checksum_and_no_collision_review |
| AMS | BSTE | euronext_etfs | missing_from_db | review_official_listing_add | add_only_after_official_identity_isin_checksum_and_no_collision_review |
| AMS | BSUE | euronext_etfs | missing_from_db | review_official_listing_add | add_only_after_official_identity_isin_checksum_and_no_collision_review |
| AMS | BSWE | euronext_etfs | missing_from_db | review_official_listing_add | add_only_after_official_identity_isin_checksum_and_no_collision_review |
| AMS | BTC | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | CBSE | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | CBU7 | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | CEMG | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | CETH | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | CNDX | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | CNYA | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | COIY | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | COMF | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | CPRY | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | CPXJ | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | CRCY | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | CRWY | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | CSCA | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | CSJP | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | CSPX | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | CSUS | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | CSX5 | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | CYBR | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | DFEU | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | DGTL | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | DIA | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | DJMC | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | DJSC | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | DRM3 | euronext_etfs | missing_from_db | review_official_listing_add | add_only_after_official_identity_isin_checksum_and_no_collision_review |
| AMS | DRMS | euronext_etfs | missing_from_db | review_official_listing_add | add_only_after_official_identity_isin_checksum_and_no_collision_review |
| AMS | ECAR | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | ECLU | euronext_etfs | missing_from_db | review_official_listing_add | add_only_after_official_identity_isin_checksum_and_no_collision_review |
| AMS | ECOM | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | EMIM | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | EMSA | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | ERNE | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | ESXY | euronext_etfs | missing_from_db | review_official_listing_add | add_only_after_official_identity_isin_checksum_and_no_collision_review |
| AMS | EUFG | euronext_etfs | missing_from_db | review_official_listing_add | add_only_after_official_identity_isin_checksum_and_no_collision_review |
| AMS | EUPB | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | EURP | euronext_etfs | missing_from_db | review_official_listing_add | add_only_after_official_identity_isin_checksum_and_no_collision_review |
| AMS | EXS1 | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | FCSG | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | FEUZ | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | FLPE | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |
| AMS | FLRG | euronext_etfs | collision_hidden_by_global_ticker | review_collision_safe_listing_add | add_only_after_listing_key_identity_isin_checksum_and_no_collision_review |

## Policy

- Official ETF directory rows do not authorize automatic additions.
- Additions require official identity evidence, valid ISIN/checksum when present, listing-key review, and no-collision validation.
- Collision-hidden rows belong in `core_listings.csv`/`listings.csv` review paths, not the legacy `tickers.csv` global-unique contract.

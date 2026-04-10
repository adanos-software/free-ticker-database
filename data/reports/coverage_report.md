# Coverage Report

## Global

| Metric | Value |
|---|---|
| tickers | 55495 |
| aliases | 87474 |
| stocks | 41101 |
| etfs | 14394 |
| isin_coverage | 38187 |
| sector_coverage | 33505 |
| cik_coverage | 7703 |
| figi_coverage | 3709 |
| lei_coverage | 920 |
| listing_status_rows | 83050 |
| listing_status_intervals | 83050 |
| listing_events | 25423 |
| listing_keys | 60417 |
| official_masterfile_symbols | 28293 |
| official_masterfile_matches | 18658 |
| official_masterfile_collisions | 5653 |
| official_masterfile_missing | 3982 |
| official_full_exchanges | 15 |
| official_partial_exchanges | 17 |
| manual_only_exchanges | 0 |
| missing_exchanges | 36 |
| stock_verification_items | 43850 |
| stock_verification_verified | 30324 |
| stock_verification_reference_gap | 13522 |
| stock_verification_missing_from_official | 4 |
| stock_verification_name_mismatch | 0 |
| stock_verification_cross_exchange_collision | 0 |
| etf_verification_items | 16567 |
| etf_verification_verified | 15543 |
| etf_verification_reference_gap | 1022 |
| etf_verification_missing_from_official | 0 |
| etf_verification_name_mismatch | 1 |
| etf_verification_cross_exchange_collision | 1 |

## Freshness

| Metric | Value |
|---|---|
| tickers_built_at | 2026-04-10T12:07:15Z |
| tickers_age_hours | 0.01 |
| masterfiles_generated_at | 2026-04-10T11:54:52Z |
| masterfiles_age_hours | 0.21 |
| identifiers_generated_at | 2026-04-10T12:07:18Z |
| identifiers_age_hours | 0.0 |
| listing_history_observed_at | 2026-04-10T12:07:15Z |
| listing_history_age_hours | 0.01 |
| latest_verification_run | data/stock_verification/run-20260410-sto-cleanup-03 |
| latest_verification_generated_at | 2026-04-10T12:07:32Z |
| latest_verification_age_hours | 0.0 |
| latest_stock_verification_run | data/stock_verification/run-20260410-sto-cleanup-03 |
| latest_stock_verification_generated_at | 2026-04-10T12:07:32Z |
| latest_stock_verification_age_hours | 0.0 |
| latest_etf_verification_run | data/etf_verification/run-20260410-tpex-etf-cache-01 |
| latest_etf_verification_generated_at | 2026-04-10T07:07:20Z |
| latest_etf_verification_age_hours | 5.0 |

## Source Coverage

| Source | Provider | Scope | Mode | Rows | Generated At |
|---|---|---|---|---|---|
| nasdaq_listed | Nasdaq Trader | exchange_directory | network | 5417 | 2026-04-09T11:28:35Z |
| nasdaq_other_listed | Nasdaq Trader | exchange_directory | network | 7084 | 2026-04-09T11:28:35Z |
| lse_company_reports | LSE | listed_companies_subset | cache | 12397 | 2026-04-09T11:28:35Z |
| lse_instrument_search | LSE | security_lookup_subset | network | 926 | 2026-04-09T11:28:35Z |
| lse_instrument_directory | LSE | security_lookup_subset | cache | 64 | 2026-04-09T11:28:35Z |
| asx_listed_companies | ASX | listed_companies_subset | network | 1979 | 2026-04-09T11:28:35Z |
| cboe_canada_listing_directory | Cboe Canada | exchange_directory | network | 436 | 2026-04-09T15:38:57Z |
| asx_investment_products | ASX | listed_companies_subset | network | 426 | 2026-04-09T11:28:35Z |
| set_listed_companies | SET | listed_companies_subset | network | 933 | 2026-04-09T11:28:35Z |
| set_etf_search | SET | listed_companies_subset | network | 13 | 2026-04-09T11:28:35Z |
| set_dr_search | SET | listed_companies_subset | network | 352 | 2026-04-09T18:57:21Z |
| tmx_listed_issuers | TMX | listed_companies_subset | network | 3704 | 2026-04-09T20:52:09Z |
| tmx_etf_screener | TMX | listed_companies_subset | network | 1697 | 2026-04-10T06:29:36Z |
| tmx_interlisted_companies | TMX | interlisted_subset | network | 266 | 2026-04-09T11:28:35Z |
| euronext_equities | Euronext | exchange_directory | network | 3883 | 2026-04-09T11:28:35Z |
| euronext_etfs | Euronext | listed_companies_subset | network | 3465 | 2026-04-09T11:28:35Z |
| jpx_listed_issues | JPX | exchange_directory | network | 4444 | 2026-04-09T11:28:35Z |
| deutsche_boerse_listed_companies | Deutsche Boerse | listed_companies_subset | network | 471 | 2026-04-09T11:28:35Z |
| deutsche_boerse_etfs_etps | Deutsche Boerse | listed_companies_subset | network | 3433 | 2026-04-09T11:28:35Z |
| deutsche_boerse_xetra_all_tradable_equities | Deutsche Boerse | listed_companies_subset | network | 986 | 2026-04-09T11:28:35Z |
| six_equity_issuers | SIX | listed_companies_subset | network | 241 | 2026-04-09T11:28:35Z |
| six_etf_products | SIX | listed_companies_subset | network | 7349 | 2026-04-09T11:28:35Z |
| six_etp_products | SIX | listed_companies_subset | network | 752 | 2026-04-09T11:28:35Z |
| b3_instruments_equities | B3 | exchange_directory | network | 878 | 2026-04-09T11:28:35Z |
| b3_listed_etfs | B3 | listed_companies_subset | network | 187 | 2026-04-09T11:28:35Z |
| b3_bdr_etfs | B3 | listed_companies_subset | network | 302 | 2026-04-09T11:28:35Z |
| jse_etf_list | JSE | listed_companies_subset | cache | 133 | 2026-04-09T11:28:35Z |
| jse_etn_list | JSE | listed_companies_subset | cache | 94 | 2026-04-09T11:28:35Z |
| jse_instrument_search | JSE | listed_companies_subset | network | 51 | 2026-04-09T11:28:35Z |
| bme_listed_companies | BME | listed_companies_subset | unknown | 0 |  |
| bme_etf_list | BME | listed_companies_subset | unavailable | 0 | 2026-04-09T16:00:22Z |
| bmv_stock_search | BMV | listed_companies_subset | network | 151 | 2026-04-10T05:26:11Z |
| bmv_capital_trust_search | BMV | listed_companies_subset | network | 16 | 2026-04-09T15:31:07Z |
| bmv_etf_search | BMV | listed_companies_subset | network | 9 | 2026-04-09T20:38:33Z |
| bmv_issuer_directory | BMV | listed_companies_subset | network | 7 | 2026-04-10T04:59:07Z |
| nasdaq_nordic_stockholm_shares | Nasdaq Nordic | listed_companies_subset | cache | 747 | 2026-04-09T11:28:35Z |
| nasdaq_nordic_stockholm_shares_search | Nasdaq Nordic | listed_companies_subset | network | 1 | 2026-04-10T08:06:23Z |
| nasdaq_nordic_helsinki_shares | Nasdaq Nordic | listed_companies_subset | cache | 194 | 2026-04-09T11:28:35Z |
| nasdaq_nordic_helsinki_shares_search | Nasdaq Nordic | listed_companies_subset | network | 3 | 2026-04-09T12:51:50Z |
| spotlight_companies_directory | Spotlight | listed_companies_subset | network | 137 | 2026-04-10T11:54:52Z |
| spotlight_companies_search | Spotlight | listed_companies_subset | network | 20 | 2026-04-10T07:26:52Z |
| ngm_companies_page | NGM | listed_companies_subset | network | 53 | 2026-04-10T11:40:27Z |
| nasdaq_nordic_copenhagen_shares | Nasdaq Nordic | listed_companies_subset | cache | 144 | 2026-04-09T11:28:35Z |
| nasdaq_nordic_stockholm_etfs | Nasdaq Nordic | listed_companies_subset | cache | 33 | 2026-04-09T11:28:35Z |
| nasdaq_nordic_helsinki_etfs | Nasdaq Nordic | listed_companies_subset | cache | 2 | 2026-04-09T11:28:35Z |
| nasdaq_nordic_copenhagen_etfs | Nasdaq Nordic | listed_companies_subset | cache | 1 | 2026-04-09T11:28:35Z |
| nasdaq_nordic_stockholm_trackers | Nasdaq Nordic | listed_companies_subset | cache | 6 | 2026-04-09T11:28:35Z |
| twse_listed_companies | TWSE | exchange_directory | network | 1081 | 2026-04-09T11:28:35Z |
| twse_etf_list | TWSE | listed_companies_subset | network | 215 | 2026-04-09T11:28:35Z |
| sse_a_share_list | SSE | listed_companies_subset | network | 2350 | 2026-04-09T11:28:35Z |
| sse_etf_list | SSE | listed_companies_subset | network | 845 | 2026-04-09T18:43:51Z |
| szse_a_share_list | SZSE | listed_companies_subset | network | 2886 | 2026-04-09T11:28:35Z |
| szse_b_share_list | SZSE | listed_companies_subset | cache | 38 | 2026-04-10T05:43:24Z |
| szse_etf_list | SZSE | listed_companies_subset | network | 632 | 2026-04-09T11:28:35Z |
| tpex_mainboard_daily_quotes | TPEX | listed_companies_subset | cache | 884 | 2026-04-09T11:28:35Z |
| tpex_etf_filter | TPEX | listed_companies_subset | cache | 112 | 2026-04-10T07:04:17Z |
| tpex_mainboard_basic_info | MOPS | listed_companies_subset | cache | 881 | 2026-04-10T11:26:34Z |
| tpex_emerging_basic_info | MOPS | listed_companies_subset | network | 354 | 2026-04-10T08:20:00Z |
| krx_listed_companies | KRX | listed_companies_subset | network | 2771 | 2026-04-09T12:16:07Z |
| krx_etf_finder | KRX | listed_companies_subset | network | 1089 | 2026-04-09T12:16:07Z |
| psx_listed_companies | PSX | listed_companies_subset | network | 565 | 2026-04-09T11:28:35Z |
| psx_symbol_name_daily | PSX | listed_companies_subset | network | 370 | 2026-04-09T11:28:35Z |
| pse_listed_company_directory | PSE | exchange_directory | network | 381 | 2026-04-09T16:17:28Z |
| sec_company_tickers_exchange | SEC | exchange_directory | cache | 10117 | 2026-04-09T17:35:51Z |

## Exchange Coverage

| Exchange | Venue Status | Tickers | ISIN | Sector | CIK | FIGI | LEI | Masterfile Symbols | Matches | Collisions | Missing | Match Rate | Verified on Covered |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| AMS | official_full | 222 | 182 | 131 | 0 | 144 | 0 | 548 | 148 | 294 | 106 | 27.01 | 100.0 |
| ASX | official_partial | 1298 | 1036 | 702 | 30 | 1032 | 24 | 0 | 0 | 0 | 0 |  | 100.0 |
| ATHEX | missing | 117 | 90 | 117 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| B3 | official_full | 959 | 316 | 443 | 0 | 0 | 0 | 878 | 847 | 0 | 31 | 96.47 | 100.0 |
| BATS | official_full | 1240 | 681 | 345 | 0 | 0 | 0 | 1243 | 1191 | 16 | 36 | 95.82 | 100.0 |
| BCBA | missing | 64 | 51 | 50 | 0 | 48 | 0 | 0 | 0 | 0 | 0 |  |  |
| BME | missing | 169 | 154 | 157 | 3 | 2 | 0 | 0 | 0 | 0 | 0 |  |  |
| BMV | official_partial | 179 | 144 | 157 | 0 | 1 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| BSE_BW | missing | 39 | 39 | 6 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| BSE_HU | missing | 31 | 15 | 9 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| BVB | missing | 85 | 79 | 77 | 1 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| BVL | missing | 33 | 0 | 3 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| Bursa | missing | 929 | 854 | 925 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| CPH | official_partial | 216 | 150 | 157 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| CSE_LK | missing | 3 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| CSE_MA | missing | 66 | 66 | 2 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| DSE_TZ | missing | 17 | 15 | 2 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| EGX | missing | 225 | 219 | 125 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| Euronext | official_full | 975 | 863 | 698 | 7 | 665 | 65 | 4385 | 938 | 2134 | 1313 | 21.39 | 100.0 |
| GSE | missing | 19 | 18 | 2 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| HEL | official_partial | 188 | 187 | 137 | 1 | 0 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| HOSE | missing | 260 | 237 | 152 | 3 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| ICE_IS | missing | 18 | 17 | 3 | 1 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| IDX | missing | 697 | 580 | 508 | 1 | 2 | 0 | 0 | 0 | 0 | 0 |  |  |
| ISE | missing | 14 | 14 | 10 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| JSE | official_partial | 213 | 181 | 98 | 2 | 0 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| KOSDAQ | official_partial | 1583 | 1154 | 1202 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| KRX | official_partial | 1788 | 1279 | 807 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| LSE | official_partial | 6408 | 5511 | 4355 | 16 | 22 | 5 | 0 | 0 | 0 | 0 |  | 100.0 |
| LUSE | missing | 22 | 22 | 1 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| MSE_MW | missing | 8 | 5 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| NASDAQ | official_full | 4624 | 3898 | 3615 | 3441 | 10 | 400 | 5446 | 4558 | 57 | 831 | 83.69 | 100.0 |
| NEO | official_full | 201 | 78 | 21 | 0 | 0 | 0 | 436 | 196 | 83 | 157 | 44.95 | 90.91 |
| NGX | missing | 147 | 145 | 4 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| NMFQS | missing | 7 | 7 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| NSE_KE | missing | 46 | 46 | 6 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| NYSE | official_full | 2077 | 1833 | 1966 | 1993 | 24 | 372 | 3836 | 2059 | 514 | 1263 | 53.68 | 100.0 |
| NYSE ARCA | official_full | 2628 | 2256 | 1308 | 127 | 6 | 3 | 2615 | 2559 | 37 | 19 | 97.86 | 100.0 |
| NYSE MKT | official_full | 237 | 226 | 227 | 221 | 2 | 26 | 313 | 233 | 20 | 60 | 74.44 | 100.0 |
| NYSEARCA | missing | 1 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| OSL | official_full | 240 | 154 | 170 | 2 | 196 | 0 | 299 | 234 | 64 | 1 | 78.26 | 100.0 |
| OTC | official_partial | 10542 | 6510 | 7826 | 1775 | 5 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| OTCCE | missing | 505 | 314 | 460 | 6 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| OTCMKTS | missing | 50 | 28 | 23 | 14 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| PSE | official_full | 90 | 90 | 4 | 1 | 0 | 0 | 381 | 90 | 187 | 104 | 23.62 | 100.0 |
| PSE_CZ | missing | 24 | 18 | 10 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| PSX | official_partial | 373 | 269 | 15 | 3 | 0 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| RSE | missing | 2 | 2 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| SEM | missing | 53 | 53 | 3 | 1 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| SET | official_partial | 547 | 355 | 534 | 4 | 0 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| SIX | official_partial | 743 | 712 | 496 | 2 | 0 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| SSE | official_partial | 2789 | 2175 | 1492 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| SSE_CL | missing | 116 | 84 | 94 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| STO | official_partial | 770 | 688 | 490 | 2 | 3 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| SZSE | official_partial | 3083 | 2596 | 1928 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| TASE | missing | 684 | 324 | 305 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| TPEX | official_partial | 1118 | 845 | 1000 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| TSE | official_full | 3214 | 0 | 0 | 0 | 0 | 0 | 4444 | 3214 | 1200 | 30 | 72.32 | 100.0 |
| TSX | official_full | 1601 | 1061 | 767 | 12 | 991 | 23 | 788 | 326 | 461 | 1 | 41.37 | 100.0 |
| TSXV | official_full | 1066 | 507 | 625 | 17 | 524 | 0 | 1600 | 1043 | 557 | 0 | 65.19 | 100.0 |
| TWSE | official_full | 1240 | 955 | 987 | 0 | 0 | 0 | 1081 | 1022 | 29 | 30 | 94.54 | 100.0 |
| US | missing | 54 | 0 | 1 | 2 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| USE_UG | missing | 7 | 7 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| VSE | missing | 36 | 34 | 32 | 0 | 22 | 0 | 0 | 0 | 0 | 0 |  |  |
| WSE | missing | 349 | 299 | 259 | 7 | 1 | 0 | 0 | 0 | 0 | 0 |  |  |
| XETRA | official_partial | 3018 | 2354 | 1339 | 8 | 9 | 2 | 0 | 0 | 0 | 0 |  | 100.0 |
| ZSE | missing | 23 | 0 | 1 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| ZSE_ZW | missing | 27 | 27 | 3 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |

## Country Coverage

| Country | Tickers | ISIN | Sector | CIK | FIGI | LEI |
|---|---|---|---|---|---|---|
| Argentina | 62 | 49 | 48 | 0 | 48 | 0 |
| Australia | 2062 | 1800 | 1418 | 315 | 1101 | 72 |
| Austria | 54 | 52 | 49 | 11 | 51 | 11 |
| Belgium | 121 | 116 | 85 | 8 | 51 | 5 |
| Bermuda | 159 | 159 | 152 | 59 | 2 | 13 |
| Botswana | 25 | 25 | 1 | 0 | 0 | 0 |
| Brazil | 925 | 281 | 408 | 0 | 0 | 0 |
| Canada | 4486 | 3261 | 2695 | 659 | 1508 | 73 |
| Cayman Islands | 618 | 613 | 476 | 443 | 1 | 16 |
| Chile | 118 | 86 | 95 | 0 | 1 | 1 |
| China | 5949 | 4850 | 3498 | 11 | 0 | 0 |
| Croatia | 23 | 0 | 1 | 0 | 0 | 0 |
| Cyprus | 10 | 10 | 9 | 2 | 0 | 0 |
| Czech Republic | 22 | 16 | 7 | 0 | 0 | 0 |
| Denmark | 226 | 163 | 165 | 4 | 0 | 0 |
| Egypt | 260 | 254 | 158 | 15 | 0 | 1 |
| Finland | 192 | 192 | 139 | 0 | 0 | 0 |
| France | 748 | 662 | 560 | 27 | 449 | 51 |
| Germany | 1301 | 654 | 515 | 18 | 10 | 7 |
| Ghana | 18 | 17 | 2 | 0 | 0 | 0 |
| Greece | 130 | 103 | 130 | 4 | 0 | 0 |
| Guernsey | 60 | 60 | 55 | 7 | 1 | 0 |
| Hong Kong | 38 | 38 | 36 | 1 | 1 | 0 |
| Hungary | 30 | 15 | 9 | 0 | 0 | 0 |
| Iceland | 18 | 17 | 3 | 1 | 0 | 0 |
| India | 1 | 1 | 1 | 0 | 0 | 0 |
| Indonesia | 805 | 688 | 611 | 49 | 4 | 4 |
| Ireland | 2027 | 2025 | 1063 | 61 | 62 | 7 |
| Isle of Man | 8 | 8 | 8 | 3 | 0 | 0 |
| Israel | 799 | 439 | 423 | 100 | 0 | 2 |
| Italy | 105 | 105 | 95 | 2 | 5 | 0 |
| Japan | 3318 | 104 | 100 | 36 | 0 | 0 |
| Jersey | 129 | 129 | 52 | 18 | 0 | 4 |
| Kenya | 45 | 45 | 6 | 0 | 0 | 0 |
| Luxembourg | 864 | 863 | 559 | 28 | 58 | 5 |
| Malawi | 8 | 5 | 0 | 0 | 0 | 0 |
| Malaysia | 928 | 853 | 923 | 0 | 0 | 0 |
| Mauritius | 51 | 51 | 3 | 1 | 0 | 0 |
| Mexico | 154 | 119 | 130 | 4 | 0 | 0 |
| Morocco | 66 | 66 | 2 | 0 | 0 | 0 |
| Netherlands | 175 | 135 | 135 | 28 | 79 | 1 |
| New Zealand | 14 | 14 | 14 | 7 | 1 | 1 |
| Nigeria | 145 | 143 | 3 | 0 | 0 | 0 |
| Norway | 299 | 213 | 225 | 9 | 194 | 1 |
| Pakistan | 367 | 263 | 13 | 3 | 0 | 0 |
| Panama | 1 | 0 | 1 | 1 | 0 | 0 |
| Peru | 33 | 0 | 3 | 0 | 0 | 0 |
| Philippines | 122 | 122 | 30 | 15 | 0 | 2 |
| Poland | 280 | 243 | 186 | 30 | 2 | 6 |
| Portugal | 32 | 29 | 24 | 0 | 10 | 2 |
| Romania | 87 | 83 | 77 | 7 | 0 | 0 |
| Rwanda | 2 | 2 | 0 | 0 | 0 | 0 |
| Singapore | 45 | 43 | 40 | 18 | 0 | 2 |
| South Africa | 218 | 186 | 98 | 5 | 0 | 0 |
| South Korea | 3361 | 2422 | 2001 | 1 | 0 | 0 |
| Spain | 206 | 195 | 178 | 8 | 6 | 4 |
| Sri Lanka | 3 | 0 | 0 | 0 | 0 | 0 |
| Sweden | 788 | 711 | 504 | 13 | 1 | 1 |
| Switzerland | 378 | 349 | 241 | 26 | 6 | 6 |
| Taiwan | 2336 | 1778 | 1965 | 0 | 0 | 0 |
| Tanzania | 15 | 13 | 2 | 0 | 0 | 0 |
| Thailand | 587 | 395 | 573 | 42 | 1 | 6 |
| Uganda | 7 | 7 | 0 | 0 | 0 | 0 |
| United Kingdom | 2179 | 1339 | 1308 | 117 | 9 | 24 |
| United States | 16194 | 10224 | 10704 | 5003 | 15 | 553 |
| Vietnam | 252 | 229 | 145 | 3 | 0 | 0 |
| Zambia | 22 | 22 | 1 | 0 | 0 | 0 |
| Zimbabwe | 27 | 27 | 3 | 0 | 0 | 0 |

## Unresolved Gaps

| Exchange | Venue Status | Findings | Reference Gap | Missing | Name Mismatch | Collision |
|---|---|---|---|---|---|---|
| OTC | official_partial | 8846 | 8846 | 0 | 0 | 0 |
| Bursa | missing | 929 | 929 | 0 | 0 | 0 |
| IDX | missing | 697 | 697 | 0 | 0 | 0 |
| TASE | missing | 684 | 684 | 0 | 0 | 0 |
| OTCCE | missing | 505 | 505 | 0 | 0 | 0 |
| WSE | missing | 349 | 349 | 0 | 0 | 0 |
| HOSE | missing | 260 | 260 | 0 | 0 | 0 |
| EGX | missing | 225 | 225 | 0 | 0 | 0 |
| BME | missing | 169 | 169 | 0 | 0 | 0 |
| NGX | missing | 147 | 147 | 0 | 0 | 0 |
| ATHEX | missing | 117 | 117 | 0 | 0 | 0 |
| SSE_CL | missing | 116 | 116 | 0 | 0 | 0 |
| CPH | official_partial | 86 | 86 | 0 | 0 | 0 |
| BVB | missing | 85 | 85 | 0 | 0 | 0 |
| AMS | official_full | 74 | 74 | 0 | 0 | 0 |
| NYSE ARCA | official_full | 69 | 69 | 0 | 0 | 0 |
| XETRA | official_partial | 69 | 69 | 0 | 0 | 0 |
| NASDAQ | official_full | 67 | 67 | 0 | 0 | 0 |
| CSE_MA | missing | 66 | 66 | 0 | 0 | 0 |
| STO | official_partial | 65 | 65 | 0 | 0 | 0 |

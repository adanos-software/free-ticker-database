# Coverage Report

## Global

| Metric | Value |
|---|---|
| tickers | 55690 |
| aliases | 87978 |
| stocks | 41179 |
| etfs | 14511 |
| isin_coverage | 38374 |
| sector_coverage | 33645 |
| cik_coverage | 7711 |
| figi_coverage | 3821 |
| lei_coverage | 921 |
| listing_status_rows | 80842 |
| listing_status_intervals | 80842 |
| listing_events | 20391 |
| listing_keys | 60625 |
| official_masterfile_symbols | 24252 |
| official_masterfile_matches | 18187 |
| official_masterfile_collisions | 2652 |
| official_masterfile_missing | 3413 |
| official_full_exchanges | 12 |
| official_partial_exchanges | 4 |
| manual_only_exchanges | 0 |
| missing_exchanges | 52 |
| stock_verification_items | 43910 |
| stock_verification_verified | 13944 |
| stock_verification_reference_gap | 29957 |
| stock_verification_missing_from_official | 0 |
| stock_verification_name_mismatch | 0 |
| stock_verification_cross_exchange_collision | 9 |
| etf_verification_items | 16715 |
| etf_verification_verified | 5381 |
| etf_verification_reference_gap | 11090 |
| etf_verification_missing_from_official | 0 |
| etf_verification_name_mismatch | 0 |
| etf_verification_cross_exchange_collision | 244 |

## Freshness

| Metric | Value |
|---|---|
| tickers_built_at | 2026-04-07T07:40:13Z |
| tickers_age_hours | 0.01 |
| masterfiles_generated_at | 2026-04-06T17:49:27Z |
| masterfiles_age_hours | 13.85 |
| identifiers_generated_at | 2026-04-07T07:40:16Z |
| identifiers_age_hours | 0.01 |
| listing_history_observed_at | 2026-04-07T07:40:13Z |
| listing_history_age_hours | 0.01 |
| latest_verification_run | data/stock_verification/run-20260407-psx-corporate-actions-stock |
| latest_verification_generated_at | 2026-04-07T07:40:27Z |
| latest_verification_age_hours | 0.0 |
| latest_stock_verification_run | data/stock_verification/run-20260407-psx-corporate-actions-stock |
| latest_stock_verification_generated_at | 2026-04-07T07:40:27Z |
| latest_stock_verification_age_hours | 0.0 |
| latest_etf_verification_run | data/etf_verification/run-20260407-psx-corporate-actions-etf |
| latest_etf_verification_generated_at | 2026-04-07T07:40:37Z |
| latest_etf_verification_age_hours | 0.0 |

## Source Coverage

| Source | Provider | Scope | Mode | Rows | Generated At |
|---|---|---|---|---|---|
| nasdaq_listed | Nasdaq Trader | exchange_directory | network | 5422 | 2026-04-06T17:49:27Z |
| nasdaq_other_listed | Nasdaq Trader | exchange_directory | network | 7073 | 2026-04-06T17:49:27Z |
| asx_listed_companies | ASX | listed_companies_subset | network | 1979 | 2026-04-06T17:49:27Z |
| tmx_interlisted_companies | TMX | interlisted_subset | network | 266 | 2026-04-06T17:49:27Z |
| euronext_equities | Euronext | exchange_directory | network | 3885 | 2026-04-06T17:49:27Z |
| jpx_listed_issues | JPX | exchange_directory | network | 4444 | 2026-04-06T17:49:27Z |
| deutsche_boerse_listed_companies | Deutsche Boerse | listed_companies_subset | network | 471 | 2026-04-06T17:49:27Z |
| b3_instruments_equities | B3 | exchange_directory | network | 876 | 2026-04-06T17:49:27Z |
| twse_listed_companies | TWSE | exchange_directory | network | 1080 | 2026-04-06T17:49:27Z |
| tpex_mainboard_daily_quotes | TPEX | exchange_directory | unavailable | 0 | 2026-04-06T17:49:27Z |
| sec_company_tickers_exchange | SEC | exchange_directory | cache | 10117 | 2026-04-06T17:49:27Z |

## Exchange Coverage

| Exchange | Venue Status | Tickers | ISIN | Sector | CIK | FIGI | LEI | Masterfile Symbols | Matches | Collisions | Missing | Match Rate | Verified on Covered |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| AMS | official_full | 223 | 183 | 132 | 0 | 145 | 0 | 119 | 89 | 26 | 4 | 74.79 | 100.0 |
| ASX | official_partial | 1298 | 1036 | 702 | 30 | 1032 | 24 | 0 | 0 | 0 | 0 |  | 100.0 |
| ATHEX | missing | 117 | 90 | 117 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| B3 | official_full | 959 | 316 | 443 | 0 | 0 | 0 | 876 | 848 | 0 | 28 | 96.8 | 100.0 |
| BATS | official_full | 1100 | 643 | 334 | 0 | 0 | 0 | 1239 | 1040 | 168 | 31 | 83.94 | 100.0 |
| BCBA | missing | 64 | 51 | 50 | 0 | 48 | 0 | 0 | 0 | 0 | 0 |  |  |
| BME | missing | 169 | 154 | 157 | 3 | 2 | 0 | 0 | 0 | 0 | 0 |  |  |
| BMV | missing | 194 | 147 | 163 | 0 | 1 | 0 | 0 | 0 | 0 | 0 |  |  |
| BSE_BW | missing | 39 | 39 | 6 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| BSE_HU | missing | 31 | 15 | 9 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| BVB | missing | 85 | 79 | 77 | 1 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| BVL | missing | 33 | 0 | 3 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| Bursa | missing | 929 | 854 | 925 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| CPH | missing | 216 | 143 | 157 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| CSE_LK | missing | 3 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| CSE_MA | missing | 66 | 66 | 2 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| DSE_TZ | missing | 17 | 15 | 2 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| EGX | missing | 225 | 219 | 125 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| Euronext | official_full | 971 | 859 | 694 | 7 | 666 | 65 | 1404 | 634 | 412 | 358 | 45.16 | 100.0 |
| GSE | missing | 19 | 18 | 2 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| HEL | missing | 188 | 149 | 137 | 1 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| HOSE | missing | 260 | 237 | 152 | 3 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| ICE_IS | missing | 18 | 17 | 3 | 1 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| IDX | missing | 697 | 580 | 508 | 1 | 2 | 0 | 0 | 0 | 0 | 0 |  |  |
| ISE | missing | 14 | 14 | 10 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| JSE | missing | 213 | 181 | 98 | 2 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| KOSDAQ | missing | 1140 | 933 | 1033 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| KRX | missing | 2282 | 1540 | 1025 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| LSE | missing | 6409 | 5488 | 4355 | 16 | 22 | 5 | 0 | 0 | 0 | 0 |  |  |
| LUSE | missing | 22 | 22 | 1 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| MSE_MW | missing | 8 | 5 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| NASDAQ | official_full | 4610 | 3895 | 3616 | 3441 | 10 | 400 | 5442 | 4524 | 87 | 831 | 83.13 | 99.91 |
| NEO | missing | 201 | 78 | 21 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| NGX | missing | 147 | 145 | 4 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| NMFQS | missing | 7 | 7 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| NSE_KE | missing | 46 | 46 | 6 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| NYSE | official_full | 2187 | 1843 | 1972 | 1988 | 25 | 372 | 3829 | 2050 | 512 | 1267 | 53.54 | 99.95 |
| NYSE ARCA | official_full | 2649 | 2288 | 1317 | 129 | 6 | 3 | 2612 | 2523 | 75 | 14 | 96.59 | 100.0 |
| NYSE MKT | official_full | 261 | 232 | 231 | 224 | 2 | 27 | 314 | 234 | 19 | 61 | 74.52 | 98.31 |
| NYSEARCA | missing | 1 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| OSL | official_full | 240 | 154 | 170 | 2 | 196 | 0 | 297 | 233 | 64 | 0 | 78.45 | 100.0 |
| OTC | official_full | 10554 | 6516 | 7829 | 1781 | 5 | 0 | 2596 | 1776 | 50 | 770 | 68.41 | 99.93 |
| OTCCE | missing | 505 | 314 | 460 | 6 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| OTCMKTS | missing | 50 | 28 | 23 | 14 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| PSE | missing | 105 | 76 | 4 | 1 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| PSE_CZ | missing | 24 | 18 | 10 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| PSX | missing | 388 | 269 | 15 | 3 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| RSE | missing | 2 | 2 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| SEM | missing | 53 | 53 | 3 | 1 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| SET | missing | 553 | 358 | 539 | 4 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| SIX | missing | 756 | 719 | 506 | 2 | 1 | 0 | 0 | 0 | 0 | 0 |  |  |
| SSE | missing | 2811 | 2175 | 1493 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| SSE_CL | missing | 116 | 84 | 94 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| STO | missing | 783 | 655 | 493 | 2 | 3 | 0 | 0 | 0 | 0 | 0 |  |  |
| SZSE | missing | 3096 | 2598 | 1931 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| TASE | missing | 684 | 324 | 305 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| TPEX | missing | 1126 | 852 | 1006 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| TSE | official_full | 3214 | 0 | 0 | 0 | 0 | 0 | 4444 | 3214 | 1205 | 25 | 72.32 | 100.0 |
| TSX | official_partial | 1663 | 1221 | 819 | 14 | 1092 | 23 | 0 | 0 | 0 | 0 |  | 100.0 |
| TSXV | official_partial | 1030 | 590 | 631 | 17 | 531 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| TWSE | official_full | 1240 | 955 | 987 | 0 | 0 | 0 | 1080 | 1022 | 34 | 24 | 94.63 | 100.0 |
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
| Australia | 2161 | 1899 | 1507 | 318 | 1203 | 74 |
| Austria | 54 | 52 | 49 | 11 | 52 | 11 |
| Belgium | 121 | 116 | 85 | 8 | 51 | 5 |
| Bermuda | 159 | 159 | 152 | 59 | 2 | 13 |
| Botswana | 25 | 25 | 1 | 0 | 0 | 0 |
| Brazil | 925 | 281 | 408 | 0 | 0 | 0 |
| Canada | 4381 | 3380 | 2641 | 656 | 1493 | 70 |
| Cayman Islands | 621 | 616 | 479 | 444 | 3 | 16 |
| Chile | 118 | 86 | 95 | 0 | 1 | 1 |
| China | 5984 | 4852 | 3502 | 11 | 0 | 0 |
| Croatia | 23 | 0 | 1 | 0 | 0 | 0 |
| Cyprus | 10 | 10 | 9 | 2 | 0 | 0 |
| Czech Republic | 22 | 16 | 7 | 0 | 0 | 0 |
| Denmark | 227 | 157 | 165 | 4 | 0 | 0 |
| Egypt | 261 | 255 | 159 | 16 | 1 | 2 |
| Finland | 196 | 158 | 143 | 0 | 0 | 0 |
| France | 749 | 663 | 561 | 28 | 449 | 51 |
| Germany | 1301 | 654 | 515 | 18 | 10 | 7 |
| Ghana | 18 | 17 | 2 | 0 | 0 | 0 |
| Greece | 130 | 103 | 130 | 4 | 0 | 0 |
| Guernsey | 60 | 60 | 55 | 7 | 1 | 0 |
| Hong Kong | 38 | 38 | 36 | 1 | 1 | 0 |
| Hungary | 30 | 15 | 9 | 0 | 0 | 0 |
| Iceland | 18 | 17 | 3 | 1 | 0 | 0 |
| India | 1 | 1 | 1 | 0 | 0 | 0 |
| Indonesia | 810 | 693 | 616 | 49 | 8 | 4 |
| Ireland | 2028 | 2026 | 1064 | 61 | 63 | 7 |
| Isle of Man | 8 | 8 | 8 | 3 | 0 | 0 |
| Israel | 799 | 439 | 423 | 100 | 0 | 2 |
| Italy | 105 | 105 | 95 | 2 | 5 | 0 |
| Japan | 3318 | 104 | 100 | 36 | 0 | 0 |
| Jersey | 130 | 130 | 53 | 18 | 1 | 4 |
| Kenya | 45 | 45 | 6 | 0 | 0 | 0 |
| Luxembourg | 865 | 864 | 560 | 28 | 58 | 5 |
| Malawi | 8 | 5 | 0 | 0 | 0 | 0 |
| Malaysia | 928 | 853 | 923 | 0 | 0 | 0 |
| Mauritius | 51 | 51 | 3 | 1 | 0 | 0 |
| Mexico | 169 | 123 | 136 | 4 | 1 | 0 |
| Morocco | 66 | 66 | 2 | 0 | 0 | 0 |
| Netherlands | 176 | 136 | 136 | 28 | 80 | 1 |
| New Zealand | 14 | 14 | 14 | 7 | 1 | 1 |
| Nigeria | 145 | 143 | 3 | 0 | 0 | 0 |
| Norway | 299 | 213 | 225 | 9 | 194 | 1 |
| Pakistan | 382 | 263 | 13 | 3 | 0 | 0 |
| Panama | 1 | 0 | 1 | 1 | 0 | 0 |
| Peru | 33 | 0 | 3 | 0 | 0 | 0 |
| Philippines | 139 | 110 | 30 | 15 | 2 | 2 |
| Poland | 280 | 243 | 186 | 30 | 2 | 6 |
| Portugal | 32 | 29 | 24 | 0 | 10 | 2 |
| Romania | 87 | 83 | 77 | 7 | 0 | 0 |
| Rwanda | 2 | 2 | 0 | 0 | 0 | 0 |
| Singapore | 46 | 44 | 41 | 18 | 1 | 2 |
| South Africa | 218 | 186 | 98 | 5 | 0 | 0 |
| South Korea | 3412 | 2462 | 2050 | 1 | 0 | 0 |
| Spain | 207 | 195 | 179 | 8 | 6 | 4 |
| Sri Lanka | 3 | 0 | 0 | 0 | 0 | 0 |
| Sweden | 798 | 675 | 505 | 13 | 1 | 1 |
| Switzerland | 383 | 348 | 242 | 26 | 6 | 6 |
| Taiwan | 2343 | 1784 | 1970 | 0 | 0 | 0 |
| Tanzania | 15 | 13 | 2 | 0 | 0 | 0 |
| Thailand | 596 | 401 | 581 | 42 | 5 | 6 |
| Uganda | 7 | 7 | 0 | 0 | 0 | 0 |
| United Kingdom | 2185 | 1322 | 1310 | 117 | 9 | 24 |
| United States | 16197 | 10227 | 10706 | 5008 | 15 | 554 |
| Vietnam | 252 | 229 | 145 | 3 | 0 | 0 |
| Zambia | 22 | 22 | 1 | 0 | 0 | 0 |
| Zimbabwe | 27 | 27 | 3 | 0 | 0 | 0 |

## Unresolved Gaps

| Exchange | Venue Status | Findings | Reference Gap | Missing | Name Mismatch | Collision |
|---|---|---|---|---|---|---|
| OTC | official_full | 9065 | 9054 | 0 | 0 | 11 |
| LSE | missing | 6409 | 6409 | 0 | 0 | 0 |
| SZSE | missing | 3096 | 3096 | 0 | 0 | 0 |
| SSE | missing | 2811 | 2811 | 0 | 0 | 0 |
| XETRA | official_partial | 2639 | 2639 | 0 | 0 | 0 |
| KRX | missing | 2282 | 2282 | 0 | 0 | 0 |
| TSX | official_partial | 1636 | 1636 | 0 | 0 | 0 |
| KOSDAQ | missing | 1140 | 1140 | 0 | 0 | 0 |
| TPEX | missing | 1126 | 1126 | 0 | 0 | 0 |
| TSXV | official_partial | 1010 | 1010 | 0 | 0 | 0 |
| Bursa | missing | 929 | 929 | 0 | 0 | 0 |
| STO | missing | 783 | 783 | 0 | 0 | 0 |
| SIX | missing | 756 | 756 | 0 | 0 | 0 |
| IDX | missing | 697 | 697 | 0 | 0 | 0 |
| TASE | missing | 684 | 684 | 0 | 0 | 0 |
| SET | missing | 553 | 553 | 0 | 0 | 0 |
| OTCCE | missing | 505 | 505 | 0 | 0 | 0 |
| PSX | missing | 388 | 388 | 0 | 0 | 0 |
| WSE | missing | 349 | 349 | 0 | 0 | 0 |
| Euronext | official_full | 346 | 341 | 0 | 0 | 5 |

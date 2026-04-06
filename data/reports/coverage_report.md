# Coverage Report

## Global

| Metric | Value |
|---|---|
| tickers | 61353 |
| aliases | 98167 |
| stocks | 44638 |
| etfs | 16715 |
| isin_coverage | 43656 |
| sector_coverage | 37839 |
| cik_coverage | 8048 |
| figi_coverage | 4207 |
| lei_coverage | 1001 |
| listing_status_rows | 64905 |
| listing_status_intervals | 64905 |
| listing_events | 8981 |
| listing_keys | 61353 |
| official_masterfile_symbols | 24252 |
| official_masterfile_matches | 18487 |
| official_masterfile_collisions | 2664 |
| official_masterfile_missing | 3101 |
| official_full_exchanges | 12 |
| official_partial_exchanges | 4 |
| manual_only_exchanges | 0 |
| missing_exchanges | 52 |
| stock_verification_items | 44638 |
| stock_verification_verified | 14180 |
| stock_verification_reference_gap | 30437 |
| stock_verification_missing_from_official | 0 |
| stock_verification_name_mismatch | 0 |
| stock_verification_cross_exchange_collision | 21 |

## Freshness

| Metric | Value |
|---|---|
| tickers_built_at | 2026-04-06T20:26:32Z |
| tickers_age_hours | 0.03 |
| masterfiles_generated_at | 2026-04-06T17:49:27Z |
| masterfiles_age_hours | 2.65 |
| identifiers_generated_at | 2026-04-06T20:26:34Z |
| identifiers_age_hours | 0.03 |
| listing_history_observed_at | 2026-04-06T20:26:32Z |
| listing_history_age_hours | 0.03 |
| latest_verification_run | data/stock_verification/run-20260406-stock-100-final3 |
| latest_verification_generated_at | 2026-04-06T20:28:31Z |
| latest_verification_age_hours | 0.0 |

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
| AMS | official_full | 223 | 183 | 132 | 0 | 194 | 0 | 119 | 89 | 26 | 4 | 74.79 | 100.0 |
| ASX | official_partial | 1298 | 1036 | 702 | 30 | 1035 | 24 | 0 | 0 | 0 | 0 |  | 100.0 |
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
| Euronext | official_full | 971 | 859 | 694 | 10 | 856 | 77 | 1404 | 634 | 412 | 358 | 45.16 | 100.0 |
| GSE | missing | 19 | 18 | 2 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| HEL | missing | 188 | 149 | 137 | 1 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| HOSE | missing | 260 | 237 | 152 | 3 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| ICE_IS | missing | 18 | 17 | 3 | 1 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| IDX | missing | 697 | 580 | 508 | 1 | 2 | 0 | 0 | 0 | 0 | 0 |  |  |
| ISE | missing | 14 | 14 | 10 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| JSE | missing | 213 | 181 | 98 | 2 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| KOSDAQ | missing | 1140 | 933 | 1033 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| KRX | missing | 2282 | 1540 | 1025 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| LSE | missing | 6409 | 5488 | 4355 | 19 | 40 | 5 | 0 | 0 | 0 | 0 |  |  |
| LUSE | missing | 22 | 22 | 1 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| MSE_MW | missing | 8 | 5 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| NASDAQ | official_full | 4610 | 3895 | 3616 | 3441 | 10 | 409 | 5442 | 4524 | 87 | 831 | 83.13 | 99.91 |
| NEO | missing | 201 | 78 | 21 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| NGX | missing | 147 | 145 | 4 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| NMFQS | missing | 7 | 7 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| NSE_KE | missing | 46 | 46 | 6 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| NYSE | official_full | 2487 | 2107 | 2220 | 2288 | 25 | 429 | 3829 | 2350 | 524 | 955 | 61.37 | 99.95 |
| NYSE ARCA | official_full | 2649 | 2288 | 1317 | 132 | 6 | 3 | 2612 | 2523 | 75 | 14 | 96.59 | 100.0 |
| NYSE MKT | official_full | 273 | 240 | 241 | 250 | 2 | 29 | 314 | 234 | 19 | 61 | 74.52 | 93.57 |
| NYSEARCA | missing | 1 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| OSL | official_full | 240 | 154 | 170 | 2 | 196 | 0 | 297 | 233 | 64 | 0 | 78.45 | 100.0 |
| OTC | official_full | 10554 | 6516 | 7829 | 1781 | 21 | 0 | 2596 | 1776 | 50 | 770 | 68.41 | 99.93 |
| OTCCE | missing | 505 | 314 | 460 | 6 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| OTCMKTS | missing | 50 | 28 | 23 | 14 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| PSE | missing | 105 | 76 | 4 | 1 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| PSE_CZ | missing | 24 | 18 | 10 | 0 | 1 | 0 | 0 | 0 | 0 | 0 |  |  |
| PSX | missing | 692 | 269 | 16 | 3 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| RSE | missing | 2 | 2 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| SEM | missing | 53 | 53 | 3 | 1 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| SET | missing | 554 | 358 | 539 | 4 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| SIX | missing | 756 | 719 | 506 | 2 | 1 | 0 | 0 | 0 | 0 | 0 |  |  |
| SSE | missing | 2811 | 2175 | 1493 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| SSE_CL | missing | 116 | 84 | 94 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| STO | missing | 787 | 658 | 496 | 2 | 3 | 0 | 0 | 0 | 0 | 0 |  |  |
| SZSE | missing | 3096 | 2598 | 1931 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| TASE | missing | 684 | 324 | 305 | 1 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| TPEX | missing | 1126 | 852 | 1006 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| TSE | official_full | 3214 | 0 | 0 | 0 | 0 | 0 | 4444 | 3214 | 1205 | 25 | 72.32 | 100.0 |
| TSX | official_partial | 1766 | 1292 | 852 | 14 | 1186 | 23 | 0 | 0 | 0 | 0 |  | 100.0 |
| TSXV | official_partial | 1034 | 590 | 632 | 17 | 536 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| TWSE | official_full | 1240 | 955 | 987 | 0 | 0 | 0 | 1080 | 1022 | 34 | 24 | 94.63 | 100.0 |
| US | missing | 55 | 1 | 2 | 3 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| USE_UG | missing | 7 | 7 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| VSE | missing | 36 | 34 | 32 | 0 | 22 | 0 | 0 | 0 | 0 | 0 |  |  |
| WSE | missing | 349 | 299 | 259 | 7 | 1 | 0 | 0 | 0 | 0 | 0 |  |  |
| XETRA | official_partial | 3017 | 2354 | 1339 | 8 | 19 | 2 | 0 | 0 | 0 | 0 |  | 100.0 |
| ZSE | missing | 23 | 0 | 1 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| ZSE_ZW | missing | 27 | 27 | 3 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |

## Country Coverage

| Country | Tickers | ISIN | Sector | CIK | FIGI | LEI |
|---|---|---|---|---|---|---|
| Argentina | 64 | 51 | 50 | 0 | 48 | 0 |
| Australia | 2247 | 1985 | 1587 | 327 | 1211 | 74 |
| Austria | 95 | 93 | 90 | 12 | 92 | 11 |
| Belgium | 179 | 174 | 134 | 11 | 87 | 13 |
| Bermuda | 191 | 191 | 184 | 71 | 5 | 19 |
| Botswana | 26 | 26 | 1 | 0 | 0 | 0 |
| Brazil | 929 | 285 | 412 | 0 | 0 | 0 |
| Canada | 4881 | 3844 | 3020 | 771 | 1585 | 74 |
| Cayman Islands | 656 | 651 | 513 | 460 | 3 | 16 |
| Chile | 118 | 86 | 95 | 0 | 1 | 1 |
| China | 5991 | 4859 | 3509 | 11 | 0 | 0 |
| Croatia | 23 | 0 | 1 | 0 | 0 | 0 |
| Cyprus | 11 | 11 | 10 | 2 | 0 | 0 |
| Czech Republic | 26 | 20 | 9 | 0 | 0 | 0 |
| Denmark | 283 | 213 | 212 | 6 | 0 | 0 |
| Egypt | 264 | 258 | 162 | 18 | 1 | 2 |
| Finland | 283 | 245 | 222 | 3 | 0 | 0 |
| France | 1022 | 936 | 807 | 37 | 451 | 53 |
| Germany | 1641 | 995 | 796 | 25 | 11 | 7 |
| Ghana | 18 | 17 | 2 | 0 | 0 | 0 |
| Greece | 140 | 113 | 139 | 5 | 0 | 0 |
| Guernsey | 68 | 68 | 61 | 7 | 1 | 0 |
| Hong Kong | 40 | 40 | 38 | 1 | 1 | 0 |
| Hungary | 34 | 19 | 11 | 0 | 0 | 0 |
| Iceland | 18 | 17 | 3 | 1 | 0 | 0 |
| India | 1 | 1 | 1 | 0 | 0 | 0 |
| Indonesia | 823 | 706 | 628 | 50 | 8 | 4 |
| Ireland | 3510 | 3508 | 2012 | 71 | 128 | 7 |
| Isle of Man | 9 | 9 | 9 | 3 | 0 | 0 |
| Israel | 822 | 462 | 441 | 106 | 1 | 2 |
| Italy | 134 | 134 | 122 | 5 | 6 | 0 |
| Japan | 3327 | 113 | 109 | 39 | 0 | 0 |
| Jersey | 153 | 153 | 67 | 19 | 3 | 4 |
| Kenya | 45 | 45 | 6 | 0 | 0 | 0 |
| Luxembourg | 1319 | 1318 | 917 | 37 | 151 | 7 |
| Malawi | 8 | 5 | 0 | 0 | 0 | 0 |
| Malaysia | 928 | 853 | 923 | 0 | 0 | 0 |
| Mauritius | 51 | 51 | 3 | 1 | 0 | 0 |
| Mexico | 205 | 159 | 168 | 6 | 1 | 0 |
| Morocco | 66 | 66 | 2 | 0 | 0 | 0 |
| Netherlands | 288 | 248 | 229 | 36 | 87 | 2 |
| New Zealand | 15 | 15 | 15 | 8 | 1 | 1 |
| Nigeria | 145 | 143 | 3 | 0 | 0 | 0 |
| Norway | 396 | 310 | 319 | 14 | 196 | 1 |
| Pakistan | 690 | 267 | 14 | 3 | 0 | 0 |
| Panama | 1 | 0 | 1 | 1 | 0 | 0 |
| Peru | 33 | 0 | 3 | 0 | 0 | 0 |
| Philippines | 139 | 110 | 30 | 15 | 2 | 2 |
| Poland | 293 | 256 | 199 | 32 | 2 | 6 |
| Portugal | 45 | 42 | 37 | 1 | 19 | 4 |
| Romania | 87 | 83 | 77 | 7 | 0 | 0 |
| Rwanda | 2 | 2 | 0 | 0 | 0 | 0 |
| Singapore | 47 | 45 | 42 | 18 | 1 | 2 |
| South Africa | 227 | 195 | 102 | 6 | 0 | 0 |
| South Korea | 3413 | 2463 | 2050 | 1 | 0 | 0 |
| Spain | 264 | 252 | 229 | 12 | 10 | 4 |
| Sri Lanka | 3 | 0 | 0 | 0 | 0 | 0 |
| Sweden | 994 | 870 | 662 | 16 | 1 | 1 |
| Switzerland | 550 | 515 | 372 | 39 | 18 | 8 |
| Taiwan | 2343 | 1784 | 1970 | 0 | 0 | 0 |
| Tanzania | 17 | 15 | 2 | 0 | 0 | 0 |
| Thailand | 600 | 404 | 584 | 42 | 5 | 6 |
| Uganda | 7 | 7 | 0 | 0 | 0 | 0 |
| United Kingdom | 2725 | 1862 | 1776 | 143 | 18 | 24 |
| United States | 16715 | 10705 | 11150 | 5383 | 16 | 607 |
| Vietnam | 252 | 229 | 145 | 3 | 0 | 0 |
| Zambia | 22 | 22 | 1 | 0 | 0 | 0 |
| Zimbabwe | 27 | 27 | 3 | 0 | 0 | 0 |

## Unresolved Gaps

| Exchange | Venue Status | Findings | Reference Gap | Missing | Name Mismatch | Collision |
|---|---|---|---|---|---|---|
| OTC | official_full | 8862 | 8861 | 0 | 0 | 1 |
| LSE | missing | 3662 | 3662 | 0 | 0 | 0 |
| SZSE | missing | 2735 | 2735 | 0 | 0 | 0 |
| SSE | missing | 2277 | 2277 | 0 | 0 | 0 |
| KRX | missing | 1336 | 1336 | 0 | 0 | 0 |
| KOSDAQ | missing | 1140 | 1140 | 0 | 0 | 0 |
| TPEX | missing | 1017 | 1017 | 0 | 0 | 0 |
| TSXV | official_partial | 1014 | 1014 | 0 | 0 | 0 |
| Bursa | missing | 925 | 925 | 0 | 0 | 0 |
| STO | missing | 772 | 772 | 0 | 0 | 0 |
| IDX | missing | 697 | 697 | 0 | 0 | 0 |
| PSX | missing | 683 | 683 | 0 | 0 | 0 |
| SET | missing | 546 | 546 | 0 | 0 | 0 |
| OTCCE | missing | 505 | 505 | 0 | 0 | 0 |
| TSX | official_partial | 505 | 505 | 0 | 0 | 0 |
| TASE | missing | 348 | 348 | 0 | 0 | 0 |
| WSE | missing | 331 | 331 | 0 | 0 | 0 |
| HOSE | missing | 247 | 247 | 0 | 0 | 0 |
| XETRA | official_partial | 247 | 247 | 0 | 0 | 0 |
| EGX | missing | 225 | 225 | 0 | 0 | 0 |

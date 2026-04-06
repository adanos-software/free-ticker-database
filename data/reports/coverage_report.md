# Coverage Report

## Global

| Metric | Value |
|---|---|
| tickers | 61570 |
| aliases | 99767 |
| stocks | 44861 |
| etfs | 16709 |
| isin_coverage | 43883 |
| sector_coverage | 38061 |
| cik_coverage | 8201 |
| figi_coverage | 4208 |
| lei_coverage | 1016 |
| listing_status_rows | 1309517 |
| listing_events | 7430 |
| listing_keys | 61570 |
| official_masterfile_symbols | 25151 |
| official_masterfile_matches | 18451 |
| official_masterfile_collisions | 3770 |
| official_masterfile_missing | 2930 |
| official_full_exchanges | 12 |
| official_partial_exchanges | 3 |
| manual_only_exchanges | 0 |
| missing_exchanges | 53 |
| stock_verification_items | 44861 |
| stock_verification_verified | 13130 |
| stock_verification_reference_gap | 31135 |
| stock_verification_missing_from_official | 122 |
| stock_verification_name_mismatch | 309 |
| stock_verification_cross_exchange_collision | 165 |

## Freshness

| Metric | Value |
|---|---|
| tickers_built_at | 2026-04-06T15:23:44Z |
| tickers_age_hours | 0.0 |
| masterfiles_generated_at | 2026-04-06T15:13:54Z |
| masterfiles_age_hours | 0.17 |
| identifiers_generated_at | 2026-04-06T15:23:50Z |
| identifiers_age_hours | 0.0 |
| listing_history_observed_at | 2026-04-06T15:23:44Z |
| listing_history_age_hours | 0.0 |
| latest_verification_run | data/stock_verification/run-20260406-us-etp-clean |
| latest_verification_generated_at | 2026-04-06T15:23:58Z |
| latest_verification_age_hours | 0.0 |

## Source Coverage

| Source | Provider | Scope | Mode | Rows | Generated At |
|---|---|---|---|---|---|
| nasdaq_listed | Nasdaq Trader | exchange_directory | network | 5422 | 2026-04-06T15:13:54Z |
| nasdaq_other_listed | Nasdaq Trader | exchange_directory | network | 7073 | 2026-04-06T15:13:54Z |
| asx_listed_companies | ASX | exchange_directory | network | 1979 | 2026-04-06T15:13:54Z |
| tmx_interlisted_companies | TMX | interlisted_subset | network | 266 | 2026-04-06T15:13:54Z |
| euronext_equities | Euronext | exchange_directory | network | 3885 | 2026-04-06T15:13:54Z |
| jpx_listed_issues | JPX | exchange_directory | network | 4444 | 2026-04-06T15:13:54Z |
| deutsche_boerse_listed_companies | Deutsche Boerse | listed_companies_subset | network | 471 | 2026-04-06T15:13:54Z |
| b3_instruments_equities | B3 | exchange_directory | network | 876 | 2026-04-06T15:13:54Z |
| sec_company_tickers_exchange | SEC | exchange_directory | cache | 10117 | 2026-04-06T15:13:54Z |

## Exchange Coverage

| Exchange | Venue Status | Tickers | ISIN | Sector | CIK | FIGI | LEI | Masterfile Symbols | Matches | Collisions | Missing | Match Rate | Verified on Covered |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| AMS | official_full | 223 | 183 | 132 | 0 | 194 | 0 | 119 | 89 | 26 | 4 | 74.79 | 97.73 |
| ASX | official_full | 1298 | 1036 | 702 | 30 | 1035 | 24 | 1979 | 1022 | 919 | 38 | 51.64 | 98.07 |
| ATHEX | missing | 117 | 90 | 117 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| B3 | official_full | 959 | 316 | 443 | 0 | 0 | 0 | 876 | 848 | 0 | 28 | 96.8 | 99.71 |
| BATS | official_full | 1102 | 645 | 335 | 2 | 0 | 0 | 1239 | 1040 | 168 | 31 | 83.94 | 25.0 |
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
| Euronext | official_full | 971 | 859 | 694 | 10 | 856 | 77 | 1404 | 634 | 414 | 356 | 45.16 | 91.78 |
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
| NASDAQ | official_full | 4729 | 4009 | 3705 | 3518 | 11 | 422 | 5442 | 4524 | 124 | 794 | 83.13 | 95.37 |
| NEO | missing | 201 | 78 | 21 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| NGX | missing | 147 | 145 | 4 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| NMFQS | missing | 7 | 7 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| NSE_KE | missing | 46 | 46 | 6 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| NYSE | official_full | 2549 | 2145 | 2246 | 2308 | 25 | 431 | 3829 | 2345 | 572 | 912 | 61.24 | 97.27 |
| NYSE ARCA | official_full | 2623 | 2257 | 1314 | 134 | 6 | 3 | 2612 | 2492 | 106 | 14 | 95.41 | 50.0 |
| NYSE MKT | official_full | 286 | 250 | 253 | 260 | 2 | 29 | 314 | 234 | 25 | 55 | 74.52 | 88.26 |
| NYSEARCA | missing | 1 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| OSL | official_full | 240 | 154 | 170 | 2 | 196 | 0 | 297 | 233 | 64 | 0 | 78.45 | 97.49 |
| OTC | official_full | 10596 | 6543 | 7867 | 1823 | 21 | 0 | 2596 | 1776 | 123 | 697 | 68.41 | 85.71 |
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
| TSE | official_full | 3214 | 0 | 0 | 0 | 0 | 0 | 4444 | 3214 | 1229 | 1 | 72.32 | 100.0 |
| TSX | official_partial | 1766 | 1292 | 852 | 14 | 1186 | 23 | 0 | 0 | 0 | 0 |  | 96.43 |
| TSXV | official_partial | 1034 | 590 | 632 | 17 | 536 | 0 | 0 | 0 | 0 | 0 |  | 95.24 |
| TWSE | missing | 1245 | 1022 | 1046 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
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
| Australia | 2267 | 2005 | 1603 | 330 | 1213 | 74 |
| Austria | 95 | 93 | 90 | 12 | 92 | 11 |
| Belgium | 179 | 174 | 134 | 11 | 87 | 13 |
| Bermuda | 194 | 194 | 187 | 71 | 5 | 19 |
| Botswana | 26 | 26 | 1 | 0 | 0 | 0 |
| Brazil | 929 | 285 | 412 | 0 | 0 | 0 |
| Canada | 4905 | 3868 | 3037 | 781 | 1585 | 76 |
| Cayman Islands | 729 | 724 | 579 | 469 | 3 | 16 |
| Chile | 118 | 86 | 95 | 0 | 1 | 1 |
| China | 5991 | 4859 | 3509 | 11 | 0 | 0 |
| Croatia | 23 | 0 | 1 | 0 | 0 | 0 |
| Cyprus | 11 | 11 | 10 | 2 | 0 | 0 |
| Czech Republic | 26 | 20 | 9 | 0 | 0 | 0 |
| Denmark | 283 | 213 | 212 | 6 | 0 | 0 |
| Egypt | 264 | 258 | 162 | 18 | 1 | 2 |
| Finland | 283 | 245 | 222 | 3 | 0 | 0 |
| France | 1023 | 937 | 808 | 38 | 451 | 54 |
| Germany | 1642 | 996 | 796 | 25 | 11 | 7 |
| Ghana | 18 | 17 | 2 | 0 | 0 | 0 |
| Greece | 140 | 113 | 139 | 5 | 0 | 0 |
| Guernsey | 68 | 68 | 61 | 7 | 1 | 0 |
| Hong Kong | 40 | 40 | 38 | 1 | 1 | 0 |
| Hungary | 34 | 19 | 11 | 0 | 0 | 0 |
| Iceland | 18 | 17 | 3 | 1 | 0 | 0 |
| India | 1 | 1 | 1 | 0 | 0 | 0 |
| Indonesia | 831 | 714 | 635 | 53 | 8 | 4 |
| Ireland | 3521 | 3519 | 2017 | 72 | 128 | 7 |
| Isle of Man | 9 | 9 | 9 | 3 | 0 | 0 |
| Israel | 825 | 465 | 444 | 106 | 1 | 2 |
| Italy | 134 | 134 | 122 | 5 | 6 | 0 |
| Japan | 3329 | 115 | 111 | 40 | 0 | 0 |
| Jersey | 154 | 154 | 68 | 19 | 3 | 4 |
| Kenya | 45 | 45 | 6 | 0 | 0 | 0 |
| Luxembourg | 1324 | 1323 | 921 | 38 | 151 | 7 |
| Malawi | 8 | 5 | 0 | 0 | 0 | 0 |
| Malaysia | 929 | 854 | 924 | 0 | 0 | 0 |
| Mauritius | 51 | 51 | 3 | 1 | 0 | 0 |
| Mexico | 205 | 159 | 168 | 6 | 1 | 0 |
| Morocco | 66 | 66 | 2 | 0 | 0 | 0 |
| Netherlands | 288 | 248 | 229 | 36 | 87 | 2 |
| New Zealand | 15 | 15 | 15 | 8 | 1 | 1 |
| Nigeria | 145 | 143 | 3 | 0 | 0 | 0 |
| Norway | 397 | 311 | 320 | 14 | 196 | 1 |
| Pakistan | 690 | 267 | 14 | 3 | 0 | 0 |
| Panama | 1 | 0 | 1 | 1 | 0 | 0 |
| Peru | 33 | 0 | 3 | 0 | 0 | 0 |
| Philippines | 139 | 110 | 30 | 15 | 2 | 2 |
| Poland | 294 | 257 | 200 | 32 | 2 | 6 |
| Portugal | 45 | 42 | 37 | 1 | 19 | 4 |
| Romania | 87 | 83 | 77 | 7 | 0 | 0 |
| Rwanda | 2 | 2 | 0 | 0 | 0 | 0 |
| Singapore | 46 | 45 | 41 | 17 | 1 | 2 |
| South Africa | 227 | 195 | 102 | 6 | 0 | 0 |
| South Korea | 3413 | 2463 | 2050 | 1 | 0 | 0 |
| Spain | 264 | 252 | 229 | 12 | 10 | 4 |
| Sri Lanka | 3 | 0 | 0 | 0 | 0 | 0 |
| Sweden | 996 | 872 | 664 | 16 | 1 | 1 |
| Switzerland | 550 | 515 | 372 | 39 | 18 | 8 |
| Taiwan | 2281 | 1784 | 1962 | 0 | 0 | 0 |
| Tanzania | 17 | 15 | 2 | 0 | 0 | 0 |
| Thailand | 602 | 406 | 586 | 42 | 5 | 6 |
| Uganda | 7 | 7 | 0 | 0 | 0 | 0 |
| United Kingdom | 2729 | 1866 | 1780 | 146 | 18 | 25 |
| United States | 16829 | 10774 | 11242 | 5503 | 15 | 617 |
| Vietnam | 252 | 229 | 145 | 3 | 0 | 0 |
| Zambia | 22 | 22 | 1 | 0 | 0 | 0 |
| Zimbabwe | 27 | 27 | 3 | 0 | 0 | 0 |

## Unresolved Gaps

| Exchange | Venue Status | Findings | Reference Gap | Missing | Name Mismatch | Collision |
|---|---|---|---|---|---|---|
| OTC | official_full | 8904 | 8656 | 0 | 248 | 0 |
| LSE | missing | 3662 | 3662 | 0 | 0 | 0 |
| SZSE | missing | 2735 | 2735 | 0 | 0 | 0 |
| SSE | missing | 2277 | 2277 | 0 | 0 | 0 |
| KRX | missing | 1336 | 1336 | 0 | 0 | 0 |
| KOSDAQ | missing | 1140 | 1140 | 0 | 0 | 0 |
| TWSE | missing | 1037 | 1037 | 0 | 0 | 0 |
| TPEX | missing | 1017 | 1017 | 0 | 0 | 0 |
| TSXV | official_partial | 1014 | 1013 | 0 | 1 | 0 |
| Bursa | missing | 925 | 925 | 0 | 0 | 0 |
| STO | missing | 772 | 772 | 0 | 0 | 0 |
| IDX | missing | 697 | 697 | 0 | 0 | 0 |
| PSX | missing | 683 | 683 | 0 | 0 | 0 |
| SET | missing | 546 | 546 | 0 | 0 | 0 |
| OTCCE | missing | 505 | 505 | 0 | 0 | 0 |
| TSX | official_partial | 505 | 504 | 0 | 1 | 0 |
| TASE | missing | 348 | 348 | 0 | 0 | 0 |
| WSE | missing | 331 | 331 | 0 | 0 | 0 |
| HOSE | missing | 247 | 247 | 0 | 0 | 0 |
| XETRA | official_partial | 247 | 247 | 0 | 0 | 0 |

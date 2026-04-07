# Coverage Report

## Global

| Metric | Value |
|---|---|
| tickers | 55984 |
| aliases | 87860 |
| stocks | 41406 |
| etfs | 14578 |
| isin_coverage | 38255 |
| sector_coverage | 33649 |
| cik_coverage | 7708 |
| figi_coverage | 3818 |
| lei_coverage | 921 |
| listing_status_rows | 81625 |
| listing_status_intervals | 81625 |
| listing_events | 22092 |
| listing_keys | 60910 |
| official_masterfile_symbols | 24255 |
| official_masterfile_matches | 18424 |
| official_masterfile_collisions | 2436 |
| official_masterfile_missing | 3395 |
| official_full_exchanges | 12 |
| official_partial_exchanges | 9 |
| manual_only_exchanges | 0 |
| missing_exchanges | 47 |
| stock_verification_items | 44128 |
| stock_verification_verified | 21306 |
| stock_verification_reference_gap | 22822 |
| stock_verification_missing_from_official | 0 |
| stock_verification_name_mismatch | 0 |
| stock_verification_cross_exchange_collision | 0 |
| etf_verification_items | 16782 |
| etf_verification_verified | 9133 |
| etf_verification_reference_gap | 7649 |
| etf_verification_missing_from_official | 0 |
| etf_verification_name_mismatch | 0 |
| etf_verification_cross_exchange_collision | 0 |

## Freshness

| Metric | Value |
|---|---|
| tickers_built_at | 2026-04-07T13:26:32Z |
| tickers_age_hours | 0.02 |
| masterfiles_generated_at | 2026-04-07T13:26:04Z |
| masterfiles_age_hours | 0.03 |
| identifiers_generated_at | 2026-04-07T13:26:34Z |
| identifiers_age_hours | 0.02 |
| listing_history_observed_at | 2026-04-07T13:26:32Z |
| listing_history_age_hours | 0.02 |
| latest_verification_run | data/stock_verification/run-20260407-b3-10 |
| latest_verification_generated_at | 2026-04-07T13:27:48Z |
| latest_verification_age_hours | 0.0 |
| latest_stock_verification_run | data/stock_verification/run-20260407-b3-10 |
| latest_stock_verification_generated_at | 2026-04-07T13:27:48Z |
| latest_stock_verification_age_hours | 0.0 |
| latest_etf_verification_run | data/etf_verification/run-20260407-b3-10 |
| latest_etf_verification_generated_at | 2026-04-07T13:27:48Z |
| latest_etf_verification_age_hours | 0.0 |

## Source Coverage

| Source | Provider | Scope | Mode | Rows | Generated At |
|---|---|---|---|---|---|
| nasdaq_listed | Nasdaq Trader | exchange_directory | network | 5415 | 2026-04-07T13:26:04Z |
| nasdaq_other_listed | Nasdaq Trader | exchange_directory | network | 7077 | 2026-04-07T13:26:04Z |
| lse_company_reports | LSE | listed_companies_subset | cache | 12397 | 2026-04-07T13:26:04Z |
| asx_listed_companies | ASX | listed_companies_subset | network | 1979 | 2026-04-07T13:26:04Z |
| tmx_listed_issuers | TMX | listed_companies_subset | cache | 3619 | 2026-04-07T13:26:04Z |
| tmx_interlisted_companies | TMX | interlisted_subset | network | 266 | 2026-04-07T13:26:04Z |
| euronext_equities | Euronext | exchange_directory | network | 3885 | 2026-04-07T13:26:04Z |
| jpx_listed_issues | JPX | exchange_directory | network | 4444 | 2026-04-07T13:26:04Z |
| deutsche_boerse_listed_companies | Deutsche Boerse | listed_companies_subset | network | 471 | 2026-04-07T13:26:04Z |
| b3_instruments_equities | B3 | exchange_directory | network | 876 | 2026-04-07T13:26:04Z |
| twse_listed_companies | TWSE | exchange_directory | network | 1080 | 2026-04-07T13:26:04Z |
| sse_a_share_list | SSE | listed_companies_subset | network | 500 | 2026-04-07T13:26:04Z |
| sse_etf_list | SSE | listed_companies_subset | network | 820 | 2026-04-07T13:26:04Z |
| szse_a_share_list | SZSE | listed_companies_subset | unavailable | 0 | 2026-04-07T13:26:04Z |
| tpex_mainboard_daily_quotes | TPEX | listed_companies_subset | cache | 884 | 2026-04-07T13:26:04Z |
| krx_listed_companies | KRX | listed_companies_subset | network | 2660 | 2026-04-07T13:26:04Z |
| sec_company_tickers_exchange | SEC | exchange_directory | cache | 10117 | 2026-04-07T13:26:04Z |

## Exchange Coverage

| Exchange | Venue Status | Tickers | ISIN | Sector | CIK | FIGI | LEI | Masterfile Symbols | Matches | Collisions | Missing | Match Rate | Verified on Covered |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| AMS | official_full | 222 | 182 | 131 | 0 | 144 | 0 | 119 | 89 | 26 | 4 | 74.79 | 100.0 |
| ASX | official_partial | 1298 | 1036 | 702 | 30 | 1032 | 24 | 0 | 0 | 0 | 0 |  | 100.0 |
| ATHEX | missing | 117 | 90 | 117 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| B3 | official_full | 959 | 316 | 443 | 0 | 0 | 0 | 876 | 848 | 0 | 28 | 96.8 | 100.0 |
| BATS | official_full | 1240 | 681 | 345 | 0 | 0 | 0 | 1239 | 1192 | 16 | 31 | 96.21 | 100.0 |
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
| Euronext | official_full | 970 | 858 | 693 | 7 | 665 | 65 | 1404 | 634 | 414 | 356 | 45.16 | 100.0 |
| GSE | missing | 19 | 18 | 2 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| HEL | missing | 188 | 149 | 137 | 1 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| HOSE | missing | 260 | 237 | 152 | 3 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| ICE_IS | missing | 18 | 17 | 3 | 1 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| IDX | missing | 697 | 580 | 508 | 1 | 2 | 0 | 0 | 0 | 0 | 0 |  |  |
| ISE | missing | 14 | 14 | 10 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| JSE | missing | 213 | 181 | 98 | 2 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| KOSDAQ | official_partial | 1140 | 933 | 1033 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| KRX | official_partial | 2282 | 1540 | 1025 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| LSE | official_partial | 6409 | 5488 | 4355 | 16 | 22 | 5 | 0 | 0 | 0 | 0 |  | 100.0 |
| LUSE | missing | 22 | 22 | 1 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| MSE_MW | missing | 8 | 5 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| NASDAQ | official_full | 4627 | 3901 | 3618 | 3442 | 10 | 400 | 5441 | 4562 | 54 | 825 | 83.84 | 100.0 |
| NEO | missing | 201 | 78 | 21 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| NGX | missing | 147 | 145 | 4 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| NMFQS | missing | 7 | 7 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| NSE_KE | missing | 46 | 46 | 6 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| NYSE | official_full | 2076 | 1833 | 1966 | 1992 | 24 | 372 | 3834 | 2058 | 517 | 1259 | 53.68 | 100.0 |
| NYSE ARCA | official_full | 2631 | 2258 | 1310 | 128 | 6 | 3 | 2611 | 2562 | 34 | 15 | 98.12 | 100.0 |
| NYSE MKT | official_full | 238 | 227 | 228 | 222 | 2 | 27 | 314 | 234 | 20 | 60 | 74.52 | 100.0 |
| NYSEARCA | missing | 1 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| OSL | official_full | 240 | 154 | 170 | 2 | 196 | 0 | 297 | 233 | 64 | 0 | 78.45 | 100.0 |
| OTC | official_full | 10544 | 6512 | 7827 | 1776 | 5 | 0 | 2596 | 1776 | 52 | 768 | 68.41 | 100.0 |
| OTCCE | missing | 505 | 314 | 460 | 6 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| OTCMKTS | missing | 50 | 28 | 23 | 14 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| PSE | missing | 105 | 76 | 4 | 1 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| PSE_CZ | missing | 24 | 18 | 10 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| PSX | missing | 388 | 269 | 15 | 3 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| RSE | missing | 2 | 2 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| SEM | missing | 53 | 53 | 3 | 1 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| SET | missing | 553 | 358 | 539 | 4 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| SIX | missing | 756 | 719 | 506 | 2 | 1 | 0 | 0 | 0 | 0 | 0 |  |  |
| SSE | official_partial | 2811 | 2175 | 1493 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| SSE_CL | missing | 116 | 84 | 94 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| STO | missing | 783 | 655 | 493 | 2 | 3 | 0 | 0 | 0 | 0 | 0 |  |  |
| SZSE | missing | 3096 | 2598 | 1931 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| TASE | missing | 684 | 324 | 305 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  |  |
| TPEX | official_partial | 1126 | 852 | 1006 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
| TSE | official_full | 3214 | 0 | 0 | 0 | 0 | 0 | 4444 | 3214 | 1205 | 25 | 72.32 | 100.0 |
| TSX | official_partial | 1756 | 1176 | 821 | 14 | 1092 | 23 | 0 | 0 | 0 | 0 |  | 100.0 |
| TSXV | official_partial | 1229 | 514 | 631 | 17 | 531 | 0 | 0 | 0 | 0 | 0 |  | 100.0 |
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
| Australia | 2063 | 1801 | 1419 | 315 | 1104 | 72 |
| Austria | 54 | 52 | 49 | 11 | 52 | 11 |
| Belgium | 121 | 116 | 85 | 8 | 51 | 5 |
| Bermuda | 159 | 159 | 152 | 59 | 2 | 13 |
| Botswana | 25 | 25 | 1 | 0 | 0 | 0 |
| Brazil | 925 | 281 | 408 | 0 | 0 | 0 |
| Canada | 4798 | 3376 | 2756 | 663 | 1614 | 73 |
| Cayman Islands | 619 | 614 | 477 | 442 | 1 | 16 |
| Chile | 118 | 86 | 95 | 0 | 1 | 1 |
| China | 5984 | 4852 | 3502 | 11 | 0 | 0 |
| Croatia | 23 | 0 | 1 | 0 | 0 | 0 |
| Cyprus | 10 | 10 | 9 | 2 | 0 | 0 |
| Czech Republic | 22 | 16 | 7 | 0 | 0 | 0 |
| Denmark | 227 | 157 | 165 | 4 | 0 | 0 |
| Egypt | 260 | 254 | 158 | 15 | 0 | 1 |
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
| Indonesia | 806 | 689 | 612 | 49 | 4 | 4 |
| Ireland | 2027 | 2025 | 1063 | 61 | 62 | 7 |
| Isle of Man | 8 | 8 | 8 | 3 | 0 | 0 |
| Israel | 799 | 439 | 423 | 100 | 0 | 2 |
| Italy | 105 | 105 | 95 | 2 | 5 | 0 |
| Japan | 3318 | 104 | 100 | 36 | 0 | 0 |
| Jersey | 129 | 129 | 52 | 18 | 0 | 4 |
| Kenya | 45 | 45 | 6 | 0 | 0 | 0 |
| Luxembourg | 865 | 864 | 560 | 28 | 58 | 5 |
| Malawi | 8 | 5 | 0 | 0 | 0 | 0 |
| Malaysia | 928 | 853 | 923 | 0 | 0 | 0 |
| Mauritius | 51 | 51 | 3 | 1 | 0 | 0 |
| Mexico | 169 | 123 | 136 | 4 | 1 | 0 |
| Morocco | 66 | 66 | 2 | 0 | 0 | 0 |
| Netherlands | 175 | 135 | 135 | 28 | 79 | 1 |
| New Zealand | 14 | 14 | 14 | 7 | 1 | 1 |
| Nigeria | 145 | 143 | 3 | 0 | 0 | 0 |
| Norway | 299 | 213 | 225 | 9 | 194 | 1 |
| Pakistan | 382 | 263 | 13 | 3 | 0 | 0 |
| Panama | 1 | 0 | 1 | 1 | 0 | 0 |
| Peru | 33 | 0 | 3 | 0 | 0 | 0 |
| Philippines | 137 | 108 | 30 | 15 | 0 | 2 |
| Poland | 280 | 243 | 186 | 30 | 2 | 6 |
| Portugal | 32 | 29 | 24 | 0 | 10 | 2 |
| Romania | 87 | 83 | 77 | 7 | 0 | 0 |
| Rwanda | 2 | 2 | 0 | 0 | 0 | 0 |
| Singapore | 45 | 43 | 40 | 18 | 0 | 2 |
| South Africa | 218 | 186 | 98 | 5 | 0 | 0 |
| South Korea | 3412 | 2462 | 2050 | 1 | 0 | 0 |
| Spain | 207 | 195 | 179 | 8 | 6 | 4 |
| Sri Lanka | 3 | 0 | 0 | 0 | 0 | 0 |
| Sweden | 798 | 675 | 505 | 13 | 1 | 1 |
| Switzerland | 383 | 348 | 242 | 26 | 6 | 6 |
| Taiwan | 2343 | 1784 | 1970 | 0 | 0 | 0 |
| Tanzania | 15 | 13 | 2 | 0 | 0 | 0 |
| Thailand | 592 | 397 | 577 | 42 | 1 | 6 |
| Uganda | 7 | 7 | 0 | 0 | 0 | 0 |
| United Kingdom | 2185 | 1322 | 1310 | 117 | 9 | 24 |
| United States | 16197 | 10227 | 10706 | 5005 | 15 | 554 |
| Vietnam | 252 | 229 | 145 | 3 | 0 | 0 |
| Zambia | 22 | 22 | 1 | 0 | 0 | 0 |
| Zimbabwe | 27 | 27 | 3 | 0 | 0 | 0 |

## Unresolved Gaps

| Exchange | Venue Status | Findings | Reference Gap | Missing | Name Mismatch | Collision |
|---|---|---|---|---|---|---|
| OTC | official_full | 9055 | 9055 | 0 | 0 | 0 |
| SZSE | missing | 3096 | 3096 | 0 | 0 | 0 |
| XETRA | official_partial | 2639 | 2639 | 0 | 0 | 0 |
| SSE | official_partial | 1803 | 1803 | 0 | 0 | 0 |
| KRX | official_partial | 1644 | 1644 | 0 | 0 | 0 |
| LSE | official_partial | 988 | 988 | 0 | 0 | 0 |
| Bursa | missing | 929 | 929 | 0 | 0 | 0 |
| STO | missing | 783 | 783 | 0 | 0 | 0 |
| SIX | missing | 756 | 756 | 0 | 0 | 0 |
| IDX | missing | 697 | 697 | 0 | 0 | 0 |
| TASE | missing | 684 | 684 | 0 | 0 | 0 |
| SET | missing | 553 | 553 | 0 | 0 | 0 |
| OTCCE | missing | 505 | 505 | 0 | 0 | 0 |
| TSX | official_partial | 475 | 475 | 0 | 0 | 0 |
| PSX | missing | 388 | 388 | 0 | 0 | 0 |
| WSE | missing | 349 | 349 | 0 | 0 | 0 |
| Euronext | official_full | 345 | 345 | 0 | 0 | 0 |
| TPEX | official_partial | 323 | 323 | 0 | 0 | 0 |
| KOSDAQ | official_partial | 305 | 305 | 0 | 0 | 0 |
| TSXV | official_partial | 291 | 291 | 0 | 0 | 0 |

# DeepSeek Collision Review Queue

Generated: `2026-06-02T21:16:08Z`

Policy: DeepSeek collision reviews are triage only and do not authorize automatic data changes.

## Summary

| Metric | Value |
| --- | ---: |
| Queue rows | 2969 |
| Unmatched DeepSeek rows | 6 |

## Target Exchanges

| Exchange | Rows |
| --- | ---: |
| ADX | 4 |
| AMS | 152 |
| BHB | 1 |
| BSE_IN | 406 |
| BVB | 14 |
| CSE_MA | 12 |
| DFM | 1 |
| Euronext | 1156 |
| HNX | 31 |
| ISE | 5 |
| KRX | 1 |
| LSE | 380 |
| NASDAQ | 3 |
| NEO | 9 |
| NSE_IN | 107 |
| NSE_KE | 8 |
| NYSE | 129 |
| NYSE MKT | 5 |
| NZX | 22 |
| OSL | 10 |
| OTC | 6 |
| PSE | 41 |
| SGX | 2 |
| TADAWUL | 25 |
| TSE | 20 |
| TSX | 35 |
| TSXV | 4 |
| TWSE | 8 |
| UPCOM | 72 |
| XETRA | 300 |

## Official Evidence Sources

| Official source key | Rows |
| --- | ---: |
| adx_market_watch | 4 |
| bahrain_bourse_listed_companies | 1 |
| bse_india_scrips | 406 |
| bvb_shares_directory | 14 |
| cboe_canada_listing_directory | 9 |
| cse_ma_listed_companies | 12 |
| deutsche_boerse_xetra_all_tradable_equities | 300 |
| dfm_listed_securities | 1 |
| euronext_equities | 71 |
| euronext_etfs | 1252 |
| hnx_listed_securities | 31 |
| jpx_listed_issues | 20 |
| krx_listed_companies | 1 |
| lse_price_explorer | 380 |
| nasdaq_listed | 3 |
| nasdaq_other_listed | 8 |
| nse_india_securities_available | 107 |
| nse_ke_listed_companies | 8 |
| nzx_instruments | 22 |
| otc_markets_stock_screener | 6 |
| pse_listed_company_directory | 41 |
| sec_company_tickers_exchange | 126 |
| sgx_securities_prices | 2 |
| tadawul_main_market_watch | 25 |
| tmx_listed_issuers | 39 |
| twse_listed_companies | 8 |
| upcom_registered_securities | 72 |

## Unmatched DeepSeek Rows

These advisory rows no longer match the current masterfile collision review and are excluded from the active queue.

| Listing key | Reason |
| --- | --- |
| Euronext::EAGG | missing_masterfile_collision_review_row |
| Euronext::JGRN | missing_masterfile_collision_review_row |
| NSE_IN::AUSOMENT | missing_masterfile_collision_review_row |
| NSE_IN::BNAGROCHEM | missing_masterfile_collision_review_row |
| NSE_IN::MMWL | missing_masterfile_collision_review_row |
| NASDAQ::DUKR | missing_masterfile_collision_review_row |

## Review Gate

Do not merge, alias, or dedupe automatically. Each row needs listing-keyed reviewer evidence covering official listing status, ISIN fungibility, exchange/MIC, instrument type, and local trading attributes.

Next evidence source: use the row's `official_source_key` first, then verify the existing listing keys against their official exchange or issuer pages before recording any gated data change.

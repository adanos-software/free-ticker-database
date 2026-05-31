# DeepSeek Collision Review Queue

Generated: `2026-05-31T21:42:12Z`

Policy: DeepSeek collision reviews are triage only and do not authorize automatic data changes.

## Summary

| Metric | Value |
| --- | ---: |
| Queue rows | 1271 |
| Unmatched DeepSeek rows | 0 |

## Target Exchanges

| Exchange | Rows |
| --- | ---: |
| ADX | 4 |
| AMS | 152 |
| BHB | 1 |
| BSE_IN | 402 |
| BVB | 14 |
| CSE_MA | 12 |
| DFM | 1 |
| Euronext | 421 |
| HNX | 31 |
| ISE | 1 |
| KRX | 1 |
| LSE | 46 |
| NSE_IN | 15 |
| NYSE | 10 |
| NZX | 2 |
| OSL | 3 |
| PSE | 35 |
| SGX | 1 |
| TADAWUL | 25 |
| TWSE | 8 |
| UPCOM | 72 |
| XETRA | 14 |

## Official Evidence Sources

| Official source key | Rows |
| --- | ---: |
| adx_market_watch | 4 |
| bahrain_bourse_listed_companies | 1 |
| bse_india_scrips | 402 |
| bvb_shares_directory | 14 |
| cse_ma_listed_companies | 12 |
| deutsche_boerse_xetra_all_tradable_equities | 14 |
| dfm_listed_securities | 1 |
| euronext_equities | 46 |
| euronext_etfs | 531 |
| hnx_listed_securities | 31 |
| krx_listed_companies | 1 |
| lse_price_explorer | 46 |
| nasdaq_other_listed | 1 |
| nse_india_securities_available | 15 |
| nzx_instruments | 2 |
| pse_listed_company_directory | 35 |
| sec_company_tickers_exchange | 9 |
| sgx_securities_prices | 1 |
| tadawul_main_market_watch | 25 |
| twse_listed_companies | 8 |
| upcom_registered_securities | 72 |

## Review Gate

Do not merge, alias, or dedupe automatically. Each row needs listing-keyed reviewer evidence covering official listing status, ISIN fungibility, exchange/MIC, instrument type, and local trading attributes.

Next evidence source: use the row's `official_source_key` first, then verify the existing listing keys against their official exchange or issuer pages before recording any gated data change.

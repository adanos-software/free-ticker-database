# DeepSeek Collision Review Queue

Generated: `2026-05-31T14:17:17Z`

Policy: DeepSeek collision reviews are triage only and do not authorize automatic data changes.

## Summary

| Metric | Value |
| --- | ---: |
| Queue rows | 507 |
| Unmatched DeepSeek rows | 0 |

## Target Exchanges

| Exchange | Rows |
| --- | ---: |
| ADX | 3 |
| AMS | 41 |
| BHB | 1 |
| BSE_IN | 35 |
| BVB | 4 |
| CSE_MA | 2 |
| Euronext | 237 |
| HNX | 26 |
| ISE | 1 |
| KRX | 1 |
| LSE | 20 |
| NSE_IN | 15 |
| NZX | 1 |
| OSL | 3 |
| PSE | 26 |
| SGX | 1 |
| TADAWUL | 25 |
| TWSE | 8 |
| UPCOM | 57 |

## Official Evidence Sources

| Official source key | Rows |
| --- | ---: |
| adx_market_watch | 3 |
| bahrain_bourse_listed_companies | 1 |
| bse_india_scrips | 35 |
| bvb_shares_directory | 4 |
| cse_ma_listed_companies | 2 |
| euronext_equities | 36 |
| euronext_etfs | 246 |
| hnx_listed_securities | 26 |
| krx_listed_companies | 1 |
| lse_price_explorer | 20 |
| nse_india_securities_available | 15 |
| nzx_instruments | 1 |
| pse_listed_company_directory | 26 |
| sgx_securities_prices | 1 |
| tadawul_main_market_watch | 25 |
| twse_listed_companies | 8 |
| upcom_registered_securities | 57 |

## Review Gate

Do not merge, alias, or dedupe automatically. Each row needs listing-keyed reviewer evidence covering official listing status, ISIN fungibility, exchange/MIC, instrument type, and local trading attributes.

Next evidence source: use the row's `official_source_key` first, then verify the existing listing keys against their official exchange or issuer pages before recording any gated data change.

# DeepSeek Collision Review Queue

Generated: `2026-05-31T07:27:48Z`

Policy: DeepSeek collision reviews are triage only and do not authorize automatic data changes.

## Summary

| Metric | Value |
| --- | ---: |
| Queue rows | 300 |
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
| Euronext | 209 |
| LSE | 5 |

## Official Evidence Sources

| Official source key | Rows |
| --- | ---: |
| adx_market_watch | 3 |
| bahrain_bourse_listed_companies | 1 |
| bse_india_scrips | 35 |
| bvb_shares_directory | 4 |
| cse_ma_listed_companies | 2 |
| euronext_equities | 11 |
| euronext_etfs | 239 |
| lse_price_explorer | 5 |

## Review Gate

Do not merge, alias, or dedupe automatically. Each row needs listing-keyed reviewer evidence covering official listing status, ISIN fungibility, exchange/MIC, instrument type, and local trading attributes.

Next evidence source: use the row's `official_source_key` first, then verify the existing listing keys against their official exchange or issuer pages before recording any gated data change.

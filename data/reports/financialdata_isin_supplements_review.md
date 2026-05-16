# FinancialData ISIN Supplements

Generated at: `2026-05-16T17:03:44Z`

FinancialData rows are used only as discovery signals. Accepted supplement rows require an official active masterfile row, a valid ISIN, issuer-name gate, no existing global ticker, and no existing/selected ISIN.

## Summary

| Metric | Value |
|---|---:|
| Input rows | 665 |
| Accepted supplement rows | 557 |

## Accepted By Exchange

| Exchange | Rows |
|---|---:|
| NSE_IN | 483 |
| HKEX | 39 |
| Bursa | 10 |
| KRX | 8 |
| BSE_IN | 7 |
| LSE | 7 |
| B3 | 3 |

## Decisions

| Decision | Rows |
|---|---:|
| accept | 2 |
| preserve | 38 |
| reject | 429 |
| skip | 196 |

## Reasons

| Reason | Rows |
|---|---:|
| already_in_financialdata_supplement | 38 |
| ambiguous_official_isin_candidates | 163 |
| exchange_not_allowed_for_isin_supplement | 91 |
| isin_already_exists_in_database | 16 |
| no_name_gated_official_isin_match | 175 |
| official_isin_name_gated_unique_primary | 2 |
| official_listing_key_already_exists | 33 |
| ticker_already_exists_globally | 147 |

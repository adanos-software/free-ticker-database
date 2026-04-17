# FinancialData ISIN Supplements

Generated at: `2026-04-17T08:09:33Z`

FinancialData rows are used only as discovery signals. Accepted supplement rows require an official active masterfile row, a valid ISIN, issuer-name gate, no existing global ticker, and no existing/selected ISIN.

## Summary

| Metric | Value |
|---|---:|
| Input rows | 665 |
| Accepted supplement rows | 555 |

## Accepted By Exchange

| Exchange | Rows |
|---|---:|
| NSE_IN | 483 |
| HKEX | 39 |
| Bursa | 10 |
| KRX | 8 |
| LSE | 7 |
| BSE_IN | 5 |
| B3 | 3 |

## Decisions

| Decision | Rows |
|---|---:|
| preserve | 47 |
| reject | 478 |
| skip | 140 |

## Reasons

| Reason | Rows |
|---|---:|
| already_in_financialdata_supplement | 47 |
| ambiguous_official_isin_candidates | 232 |
| exchange_not_allowed_for_isin_supplement | 91 |
| isin_already_exists_in_database | 51 |
| no_name_gated_official_isin_match | 155 |
| official_listing_key_already_exists | 2 |
| ticker_already_exists_globally | 87 |

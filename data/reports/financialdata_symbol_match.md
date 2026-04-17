# FinancialData Symbol Match

Generated at: `2026-04-17T07:41:14Z`

FinancialData.net international stock symbols are treated as secondary review signals. 
They do not include ISIN or sector data and must not be applied as official exchange masterfile rows.

## Summary

| Metric | Value |
|---|---:|
| Requests used | 7 |
| Raw rows | 3078 |
| Deduped rows | 3077 |
| Mapped rows | 3077 |
| Unmapped rows | 0 |
| Current-exchange gaps | 605 |
| Global expansion candidates | 60 |
| Truncated by request limit | False |

## Match Status

| Status | Rows |
|---|---:|
| matched_exchange_name_mismatch | 27 |
| matched_exchange_name_ok | 1763 |
| missing_from_database | 665 |
| ticker_present_other_exchange | 622 |

## Review Scope

| Scope | Rows |
|---|---:|
| current_exchange_gap | 605 |
| current_exchange_mapping_review | 617 |
| global_expansion_candidate | 60 |
| global_expansion_symbol_collision | 5 |
| name_mismatch_review | 27 |
| secondary_reference | 1763 |

## Current-Exchange Gaps

| Exchange | Rows |
|---|---:|
| BSE_IN | 276 |
| HKEX | 102 |
| LSE | 68 |
| NSE_IN | 58 |
| B3 | 17 |
| TSX | 17 |
| Euronext | 16 |
| TSE | 14 |
| XETRA | 14 |
| BMV | 7 |
| IDX | 7 |
| AMS | 5 |
| KRX | 3 |
| Bursa | 1 |

## Global Expansion Candidates

| Exchange | Rows |
|---|---:|
| SGX | 60 |

## Policy

- `matched_exchange_name_ok` rows can be used as secondary coverage evidence only.
- `matched_exchange_name_mismatch` rows need manual/source review before any metadata change.
- `current_exchange_gap` rows need an official source or ISIN-bearing source before core insertion.
- `global_expansion_candidate` rows indicate missing venue coverage and are not current-core gaps.
- Keep the fetch under the daily API cap: page size `500`, max requests `300`.

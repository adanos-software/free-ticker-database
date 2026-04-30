# Symbol Changes Review

Generated at: `2026-04-30T08:36:18Z`

Daily secondary-source symbol-change feed. Rows are review signals, not automatic canonical ticker updates.

## Summary

| Metric | Rows |
|---|---:|
| Fetched rows | 227 |
| Merged history rows | 239 |
| Review rows | 239 |

## Match Status

| Status | Rows |
|---|---:|
| new_symbol_present_old_symbol_missing | 145 |
| no_matching_listing | 14 |
| old_and_new_symbols_present | 57 |
| old_symbol_present_new_symbol_missing | 23 |

## Recommended Actions

| Action | Rows |
|---|---:|
| already_reflected_or_new_symbol_added | 145 |
| ignore_or_map_exchange_scope_before_applying | 14 |
| review_duplicate_or_cross_listing_state | 57 |
| review_possible_rename_or_delisting | 23 |

## Policy

- `source_confidence=secondary_review`: do not auto-merge as official exchange data.
- `review_needed=true`: apply only after exchange/listing-key validation.
- StockAnalysis is used as a broad daily change detector; venue-specific official feeds should override it when available.

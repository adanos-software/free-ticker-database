# Symbol Changes Review

Generated at: `2026-05-03T08:15:32Z`

Daily secondary-source symbol-change feed. Rows are review signals, not automatic canonical ticker updates.

## Summary

| Metric | Rows |
|---|---:|
| Fetched rows | 226 |
| Merged history rows | 241 |
| Review rows | 241 |

## Match Status

| Status | Rows |
|---|---:|
| new_symbol_present_old_symbol_missing | 145 |
| no_matching_listing | 15 |
| old_and_new_symbols_present | 57 |
| old_symbol_present_new_symbol_missing | 24 |

## Recommended Actions

| Action | Rows |
|---|---:|
| already_reflected_or_new_symbol_added | 145 |
| ignore_or_map_exchange_scope_before_applying | 15 |
| review_duplicate_or_cross_listing_state | 57 |
| review_possible_rename_or_delisting | 24 |

## Policy

- `source_confidence=secondary_review`: do not auto-merge as official exchange data.
- `review_needed=true`: apply only after exchange/listing-key validation.
- StockAnalysis is used as a broad daily change detector; venue-specific official feeds should override it when available.

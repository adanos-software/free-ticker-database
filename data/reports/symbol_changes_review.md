# Symbol Changes Review

Generated at: `2026-05-28T10:33:38Z`

Daily secondary-source symbol-change feed. Rows are review signals, not automatic canonical ticker updates.

## Summary

| Metric | Rows |
|---|---:|
| Fetched rows | 234 |
| Merged history rows | 265 |
| Review rows | 265 |

## Match Status

| Status | Rows |
|---|---:|
| new_symbol_present_old_symbol_missing | 144 |
| no_matching_listing | 18 |
| old_and_new_symbols_present | 64 |
| old_symbol_present_new_symbol_missing | 39 |

## Recommended Actions

| Action | Rows |
|---|---:|
| already_reflected_or_new_symbol_added | 144 |
| ignore_or_map_exchange_scope_before_applying | 18 |
| review_duplicate_or_cross_listing_state | 64 |
| review_possible_rename_or_delisting | 39 |

## Policy

- `source_confidence=secondary_review`: do not auto-merge as official exchange data.
- `review_needed=true`: apply only after exchange/listing-key validation.
- StockAnalysis is used as a broad daily change detector; venue-specific official feeds should override it when available.

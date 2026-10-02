# Official reference reconciliation

- Active official source rows: **170,000**
- Source-specific venue/symbol keys: **169,999**
- Coverage-credited keys: **100,007**
- Exact identity conflicts: **12,517**
- In-scope missing listings: **43,997**

| Classification | Keys |
|---|---:|
| `alternate_listing_line` | 3,403 |
| `ambiguous_same_venue_identifier` | 193 |
| `exact_identity_conflict` | 12,517 |
| `exact_match` | 100,007 |
| `missing_from_database` | 43,997 |
| `normalization_candidate` | 1,810 |
| `out_of_scope` | 8,072 |

Coverage credit is venue-specific and identity-aware. Cross-venue ISIN matches and normalized-symbol/name candidates are review queues, not completeness credit.

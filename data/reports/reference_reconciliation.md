# Official reference reconciliation

- Active official source rows: **170,000**
- Source-specific venue/symbol keys: **169,999**
- Coverage-credited keys: **100,025**
- Exact identity conflicts: **12,500**
- In-scope missing listings: **43,992**

| Classification | Keys |
|---|---:|
| `alternate_listing_line` | 3,403 |
| `ambiguous_same_venue_identifier` | 193 |
| `exact_identity_conflict` | 12,500 |
| `exact_match` | 100,025 |
| `missing_from_database` | 43,992 |
| `normalization_candidate` | 1,812 |
| `out_of_scope` | 8,074 |

Coverage credit is venue-specific and identity-aware. Cross-venue ISIN matches and normalized-symbol/name candidates are review queues, not completeness credit.

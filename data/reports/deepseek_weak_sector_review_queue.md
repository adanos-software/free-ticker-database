# DeepSeek Weak-Sector Review Queue

Generated: `2026-05-29T12:03:10Z`

Policy: DeepSeek weak-sector reviews are triage only and do not authorize sector fills.

## Summary

| Metric | Value |
| --- | ---: |
| Queue rows | 50 |
| Unmatched DeepSeek rows | 0 |

## Review Queues

| Queue | Rows |
| --- | ---: |
| keep_source_gap_until_official_taxonomy | 25 |
| official_sector_value_mapping_review | 24 |
| scope_review_before_sector_fill | 1 |

## Official Sector Values

| Official raw value | Rows |
| --- | ---: |
| missing | 26 |
| CONGLOMERATES | 5 |
| INVESTMENT | 1 |
| SERVICES | 18 |

## Review Gate

Do not apply raw venue sector values as canonical sectors. Each fill requires a listing-keyed official taxonomy mapping decision; otherwise keep the sector blank as a reviewed source gap.

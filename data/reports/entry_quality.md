# Entry Quality Report

Generated at: `2026-04-22T06:33:09Z`

## Status Counts

| Status | Rows |
|---|---:|
| pass | 49,265 |
| source_gap | 12,724 |
| warn | 582 |

## Issue Counts

| Issue | Rows |
|---|---:|
| official_reference_gap | 4,454 |
| expected_missing_primary_isin | 3,932 |
| missing_etf_category | 3,829 |
| missing_stock_sector | 3,159 |
| official_name_mismatch | 458 |
| country_isin_mismatch | 124 |

## Top Flagged Exchanges

| Exchange | Pass | Notice | Source Gap | Warn | Quarantine |
|---|---:|---:|---:|---:|---:|
| OTC | 7,764 | 0 | 2,866 | 458 | 0 |
| B3 | 644 | 0 | 940 | 0 | 0 |
| TSX | 1,147 | 0 | 755 | 0 | 0 |
| LSE | 5,756 | 0 | 566 | 93 | 0 |
| SSE | 2,131 | 0 | 658 | 0 | 0 |
| NYSE ARCA | 2,003 | 0 | 651 | 0 | 0 |
| TSXV | 458 | 0 | 604 | 4 | 0 |
| KRX | 1,266 | 0 | 530 | 0 | 0 |
| SZSE | 2,580 | 0 | 502 | 1 | 0 |
| NSE_IN | 0 | 0 | 483 | 0 | 0 |
| NASDAQ | 4,196 | 0 | 435 | 3 | 0 |
| XETRA | 3,421 | 0 | 357 | 1 | 0 |
| BATS | 950 | 0 | 293 | 0 | 0 |
| STO | 489 | 0 | 236 | 0 | 0 |
| ASX | 1,087 | 0 | 211 | 0 | 0 |
| SET | 347 | 0 | 198 | 2 | 0 |
| BMV | 8 | 0 | 159 | 12 | 0 |
| TWSE | 1,072 | 0 | 170 | 0 | 0 |
| Euronext | 817 | 0 | 158 | 0 | 0 |
| TASE | 542 | 0 | 131 | 0 | 0 |

## Notes

- `entry_quality.csv` contains one row per `listing_key` and is the complete per-entry report.
- `notice` marks soft alias-review hints; it is not a structural row warning.
- `source_gap` means the row is structurally valid but lacks stronger source or metadata coverage.
- `quarantine` means deterministic checks found a hard contradiction that should be fixed before treating the row as high quality.

# Entry Quality Report

Generated at: `2026-04-22T09:47:07Z`

## Status Counts

| Status | Rows |
|---|---:|
| pass | 49,455 |
| source_gap | 13,021 |
| warn | 63 |

## Issue Counts

| Issue | Rows |
|---|---:|
| official_reference_gap | 4,758 |
| expected_missing_primary_isin | 3,894 |
| missing_etf_category | 3,830 |
| missing_stock_sector | 3,158 |
| country_isin_mismatch | 63 |

## Top Flagged Exchanges

| Exchange | Pass | Notice | Source Gap | Warn | Quarantine |
|---|---:|---:|---:|---:|---:|
| OTC | 7,887 | 0 | 3,169 | 0 | 0 |
| B3 | 644 | 0 | 940 | 0 | 0 |
| TSX | 1,147 | 0 | 755 | 0 | 0 |
| SSE | 2,131 | 0 | 658 | 0 | 0 |
| NYSE ARCA | 2,004 | 0 | 650 | 0 | 0 |
| LSE | 5,803 | 0 | 552 | 60 | 0 |
| TSXV | 463 | 0 | 603 | 0 | 0 |
| KRX | 1,266 | 0 | 530 | 0 | 0 |
| SZSE | 2,580 | 0 | 502 | 1 | 0 |
| NSE_IN | 0 | 0 | 483 | 0 | 0 |
| NASDAQ | 4,199 | 0 | 434 | 1 | 0 |
| XETRA | 3,422 | 0 | 357 | 0 | 0 |
| BATS | 950 | 0 | 293 | 0 | 0 |
| STO | 489 | 0 | 236 | 0 | 0 |
| ASX | 1,087 | 0 | 211 | 0 | 0 |
| SET | 349 | 0 | 198 | 0 | 0 |
| BMV | 8 | 0 | 171 | 0 | 0 |
| TWSE | 1,072 | 0 | 170 | 0 | 0 |
| Euronext | 817 | 0 | 158 | 0 | 0 |
| TASE | 542 | 0 | 131 | 0 | 0 |

## Notes

- `entry_quality.csv` contains one row per `listing_key` and is the complete per-entry report.
- `notice` marks soft alias-review hints; it is not a structural row warning.
- `source_gap` means the row is structurally valid but lacks stronger source or metadata coverage.
- `quarantine` means deterministic checks found a hard contradiction that should be fixed before treating the row as high quality.

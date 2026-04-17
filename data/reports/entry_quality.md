# Entry Quality Report

Generated at: `2026-04-17T07:45:48Z`

## Status Counts

| Status | Rows |
|---|---:|
| notice | 5,496 |
| pass | 44,642 |
| source_gap | 12,074 |
| warn | 284 |

## Issue Counts

| Issue | Rows |
|---|---:|
| official_reference_gap | 4,449 |
| missing_etf_category | 4,013 |
| shared_name_alias | 4,006 |
| expected_missing_primary_isin | 3,918 |
| missing_stock_sector | 3,173 |
| low_company_name_overlap | 1,983 |
| official_name_mismatch | 282 |
| secondary_listing_points_to_self | 1 |
| country_isin_mismatch | 1 |

## Top Flagged Exchanges

| Exchange | Pass | Notice | Source Gap | Warn | Quarantine |
|---|---:|---:|---:|---:|---:|
| OTC | 6,510 | 1,613 | 2,688 | 282 | 0 |
| B3 | 473 | 138 | 926 | 0 | 0 |
| TSX | 1,054 | 148 | 693 | 0 | 0 |
| SSE | 2,089 | 42 | 658 | 0 | 0 |
| NYSE ARCA | 1,803 | 278 | 570 | 0 | 0 |
| LSE | 5,084 | 776 | 553 | 2 | 0 |
| KRX | 1,214 | 51 | 531 | 0 | 0 |
| SZSE | 2,523 | 58 | 502 | 0 | 0 |
| NSE_IN | 0 | 2 | 481 | 0 | 0 |
| TSXV | 355 | 260 | 451 | 0 | 0 |
| NASDAQ | 3,644 | 578 | 412 | 0 | 0 |
| XETRA | 3,239 | 159 | 354 | 0 | 0 |
| BATS | 890 | 49 | 308 | 0 | 0 |
| STO | 401 | 94 | 230 | 0 | 0 |
| ASX | 974 | 117 | 207 | 0 | 0 |
| SET | 325 | 25 | 197 | 0 | 0 |
| TWSE | 1,072 | 2 | 168 | 0 | 0 |
| Euronext | 742 | 81 | 152 | 0 | 0 |
| BMV | 8 | 36 | 135 | 0 | 0 |
| TASE | 529 | 14 | 130 | 0 | 0 |

## Notes

- `entry_quality.csv` contains one row per `listing_key` and is the complete per-entry report.
- `notice` marks soft alias-review hints; it is not a structural row warning.
- `source_gap` means the row is structurally valid but lacks stronger source or metadata coverage.
- `quarantine` means deterministic checks found a hard contradiction that should be fixed before treating the row as high quality.

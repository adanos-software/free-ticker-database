# Entry Quality Report

Generated at: `2026-08-05T09:22:49Z`

## Status Counts

| Status | Rows |
|---|---:|
| pass | 68,155 |
| source_gap | 7,047 |
| warn | 20 |

## Issue Counts

| Issue | Rows |
|---|---:|
| official_reference_gap | 5,758 |
| expected_missing_primary_isin | 1,135 |
| missing_stock_sector | 314 |
| missing_etf_category | 135 |
| country_isin_mismatch | 12 |
| official_name_mismatch | 6 |
| official_isin_mismatch | 2 |

## Top Flagged Exchanges

| Exchange | Pass | Notice | Source Gap | Warn | Quarantine |
|---|---:|---:|---:|---:|---:|
| OTC | 7,920 | 0 | 3,170 | 4 | 0 |
| B3 | 1,250 | 0 | 331 | 0 | 0 |
| NASDAQ | 4,427 | 0 | 241 | 1 | 0 |
| ASX | 1,491 | 0 | 217 | 0 | 0 |
| NYSE ARCA | 2,482 | 0 | 205 | 0 | 0 |
| BATS | 1,134 | 0 | 191 | 0 | 0 |
| LSE | 6,419 | 0 | 178 | 9 | 0 |
| TSXV | 899 | 0 | 184 | 2 | 0 |
| BMV | 0 | 0 | 179 | 0 | 0 |
| BSE_IN | 2,576 | 0 | 156 | 0 | 0 |
| TSX | 1,696 | 0 | 149 | 0 | 0 |
| SET | 543 | 0 | 148 | 0 | 0 |
| BME | 84 | 0 | 136 | 0 | 0 |
| NSE_IN | 2,369 | 0 | 134 | 0 | 0 |
| XETRA | 3,728 | 0 | 129 | 0 | 0 |
| Euronext | 981 | 0 | 106 | 0 | 0 |
| TASE | 594 | 0 | 101 | 0 | 0 |
| AMS | 241 | 0 | 90 | 0 | 0 |
| JSE | 125 | 0 | 87 | 0 | 0 |
| NYSE | 1,939 | 0 | 66 | 0 | 0 |

## Notes

- `entry_quality.csv` contains one row per `listing_key` and is the complete per-entry report.
- `notice` marks soft alias-review hints; it is not a structural row warning.
- `source_gap` means the row is structurally valid but lacks stronger source or metadata coverage.
- `quarantine` means deterministic checks found a hard contradiction that should be fixed before treating the row as high quality.

# Entry Quality Report

Generated at: `2026-08-06T13:58:58Z`

## Status Counts

| Status | Rows |
|---|---:|
| pass | 67,973 |
| source_gap | 7,211 |
| warn | 47 |

## Issue Counts

| Issue | Rows |
|---|---:|
| official_reference_gap | 5,923 |
| expected_missing_primary_isin | 1,144 |
| missing_stock_sector | 317 |
| missing_etf_category | 141 |
| official_name_mismatch | 34 |
| country_isin_mismatch | 12 |
| official_isin_mismatch | 1 |

## Top Flagged Exchanges

| Exchange | Pass | Notice | Source Gap | Warn | Quarantine |
|---|---:|---:|---:|---:|---:|
| OTC | 7,905 | 0 | 3,172 | 17 | 0 |
| B3 | 1,250 | 0 | 331 | 0 | 0 |
| NASDAQ | 4,363 | 0 | 302 | 8 | 0 |
| ASX | 1,491 | 0 | 217 | 0 | 0 |
| NSE_IN | 2,290 | 0 | 213 | 0 | 0 |
| NYSE ARCA | 2,482 | 0 | 206 | 0 | 0 |
| BATS | 1,134 | 0 | 192 | 3 | 0 |
| LSE | 6,419 | 0 | 178 | 9 | 0 |
| TSXV | 900 | 0 | 184 | 2 | 0 |
| BMV | 0 | 0 | 179 | 0 | 0 |
| BSE_IN | 2,576 | 0 | 156 | 0 | 0 |
| TSX | 1,696 | 0 | 149 | 0 | 0 |
| SET | 543 | 0 | 148 | 0 | 0 |
| BME | 84 | 0 | 136 | 0 | 0 |
| XETRA | 3,728 | 0 | 129 | 0 | 0 |
| Euronext | 981 | 0 | 106 | 0 | 0 |
| TASE | 594 | 0 | 101 | 0 | 0 |
| AMS | 241 | 0 | 90 | 0 | 0 |
| JSE | 125 | 0 | 87 | 0 | 0 |
| NYSE | 1,919 | 0 | 81 | 5 | 0 |

## Notes

- `entry_quality.csv` contains one row per `listing_key` and is the complete per-entry report.
- `notice` marks soft alias-review hints; it is not a structural row warning.
- `source_gap` means the row is structurally valid but lacks stronger source or metadata coverage.
- `quarantine` means deterministic checks found a hard contradiction that should be fixed before treating the row as high quality.

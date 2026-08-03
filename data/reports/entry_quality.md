# Entry Quality Report

Generated at: `2026-08-03T18:18:19Z`

## Status Counts

| Status | Rows |
|---|---:|
| pass | 68,093 |
| source_gap | 6,484 |
| warn | 20 |

## Issue Counts

| Issue | Rows |
|---|---:|
| official_reference_gap | 5,741 |
| expected_missing_primary_isin | 777 |
| missing_etf_category | 122 |
| missing_stock_sector | 87 |
| country_isin_mismatch | 12 |
| official_name_mismatch | 6 |
| official_isin_mismatch | 2 |

## Top Flagged Exchanges

| Exchange | Pass | Notice | Source Gap | Warn | Quarantine |
|---|---:|---:|---:|---:|---:|
| OTC | 7,902 | 0 | 3,170 | 4 | 0 |
| B3 | 1,250 | 0 | 331 | 0 | 0 |
| NASDAQ | 4,427 | 0 | 235 | 1 | 0 |
| NYSE ARCA | 2,488 | 0 | 196 | 0 | 0 |
| BATS | 1,134 | 0 | 180 | 0 | 0 |
| BMV | 0 | 0 | 179 | 0 | 0 |
| TSXV | 899 | 0 | 156 | 2 | 0 |
| LSE | 6,416 | 0 | 146 | 9 | 0 |
| TSX | 1,696 | 0 | 148 | 0 | 0 |
| BME | 84 | 0 | 136 | 0 | 0 |
| ASX | 1,491 | 0 | 134 | 0 | 0 |
| NSE_IN | 2,369 | 0 | 134 | 0 | 0 |
| XETRA | 3,725 | 0 | 123 | 0 | 0 |
| Euronext | 984 | 0 | 100 | 0 | 0 |
| AMS | 241 | 0 | 90 | 0 | 0 |
| JSE | 125 | 0 | 87 | 0 | 0 |
| TASE | 594 | 0 | 80 | 0 | 0 |
| CSE_MA | 1 | 0 | 65 | 0 | 0 |
| BSE_IN | 2,576 | 0 | 64 | 0 | 0 |
| NYSE | 1,939 | 0 | 63 | 0 | 0 |

## Notes

- `entry_quality.csv` contains one row per `listing_key` and is the complete per-entry report.
- `notice` marks soft alias-review hints; it is not a structural row warning.
- `source_gap` means the row is structurally valid but lacks stronger source or metadata coverage.
- `quarantine` means deterministic checks found a hard contradiction that should be fixed before treating the row as high quality.

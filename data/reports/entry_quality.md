# Entry Quality Report

Generated at: `2026-07-15T08:58:47Z`

## Status Counts

| Status | Rows |
|---|---:|
| pass | 68,321 |
| source_gap | 6,291 |
| warn | 75 |

## Issue Counts

| Issue | Rows |
|---|---:|
| official_reference_gap | 5,623 |
| expected_missing_primary_isin | 734 |
| missing_etf_category | 84 |
| country_isin_mismatch | 65 |
| missing_stock_sector | 12 |
| official_name_mismatch | 8 |
| official_isin_mismatch | 2 |

## Top Flagged Exchanges

| Exchange | Pass | Notice | Source Gap | Warn | Quarantine |
|---|---:|---:|---:|---:|---:|
| OTC | 7,899 | 0 | 3,170 | 7 | 0 |
| B3 | 1,262 | 0 | 319 | 0 | 0 |
| TSX | 1,704 | 0 | 199 | 0 | 0 |
| NASDAQ | 4,512 | 0 | 195 | 1 | 0 |
| LSE | 6,366 | 0 | 132 | 60 | 0 |
| NYSE ARCA | 2,496 | 0 | 179 | 0 | 0 |
| BMV | 12 | 0 | 167 | 0 | 0 |
| BATS | 1,137 | 0 | 159 | 0 | 0 |
| BME | 78 | 0 | 143 | 0 | 0 |
| NSE_IN | 2,369 | 0 | 134 | 0 | 0 |
| ASX | 1,501 | 0 | 123 | 1 | 0 |
| XETRA | 3,731 | 0 | 114 | 0 | 0 |
| TSXV | 970 | 0 | 97 | 2 | 0 |
| Euronext | 983 | 0 | 98 | 0 | 0 |
| AMS | 241 | 0 | 90 | 0 | 0 |
| JSE | 125 | 0 | 87 | 0 | 0 |
| TASE | 597 | 0 | 75 | 0 | 0 |
| ATHEX | 89 | 0 | 66 | 0 | 0 |
| CSE_MA | 1 | 0 | 65 | 0 | 0 |
| NYSE | 1,979 | 0 | 60 | 0 | 0 |

## Notes

- `entry_quality.csv` contains one row per `listing_key` and is the complete per-entry report.
- `notice` marks soft alias-review hints; it is not a structural row warning.
- `source_gap` means the row is structurally valid but lacks stronger source or metadata coverage.
- `quarantine` means deterministic checks found a hard contradiction that should be fixed before treating the row as high quality.

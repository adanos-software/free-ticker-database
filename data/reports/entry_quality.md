# Entry Quality Report

Generated at: `2026-07-23T15:59:26Z`

## Status Counts

| Status | Rows |
|---|---:|
| pass | 67,772 |
| source_gap | 6,929 |
| warn | 21 |

## Issue Counts

| Issue | Rows |
|---|---:|
| official_reference_gap | 6,238 |
| expected_missing_primary_isin | 757 |
| missing_etf_category | 95 |
| missing_stock_sector | 26 |
| country_isin_mismatch | 12 |
| official_name_mismatch | 7 |
| official_isin_mismatch | 2 |

## Top Flagged Exchanges

| Exchange | Pass | Notice | Source Gap | Warn | Quarantine |
|---|---:|---:|---:|---:|---:|
| OTC | 7,903 | 0 | 3,170 | 5 | 0 |
| B3 | 672 | 0 | 909 | 0 | 0 |
| NASDAQ | 4,508 | 0 | 209 | 1 | 0 |
| TSX | 1,704 | 0 | 200 | 0 | 0 |
| NYSE ARCA | 2,494 | 0 | 188 | 0 | 0 |
| BMV | 0 | 0 | 179 | 0 | 0 |
| BATS | 1,136 | 0 | 161 | 0 | 0 |
| LSE | 6,418 | 0 | 132 | 9 | 0 |
| ASX | 1,491 | 0 | 134 | 0 | 0 |
| NSE_IN | 2,369 | 0 | 134 | 0 | 0 |
| BME | 90 | 0 | 131 | 0 | 0 |
| XETRA | 3,731 | 0 | 115 | 0 | 0 |
| Euronext | 984 | 0 | 99 | 0 | 0 |
| TSXV | 970 | 0 | 97 | 2 | 0 |
| AMS | 241 | 0 | 90 | 0 | 0 |
| JSE | 125 | 0 | 87 | 0 | 0 |
| TASE | 597 | 0 | 75 | 0 | 0 |
| NYSE | 1,985 | 0 | 65 | 0 | 0 |
| CSE_MA | 1 | 0 | 65 | 0 | 0 |
| BSE_IN | 2,580 | 0 | 57 | 0 | 0 |

## Notes

- `entry_quality.csv` contains one row per `listing_key` and is the complete per-entry report.
- `notice` marks soft alias-review hints; it is not a structural row warning.
- `source_gap` means the row is structurally valid but lacks stronger source or metadata coverage.
- `quarantine` means deterministic checks found a hard contradiction that should be fixed before treating the row as high quality.

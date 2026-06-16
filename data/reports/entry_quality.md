# Entry Quality Report

Generated at: `2026-06-16T19:36:31Z`

## Status Counts

| Status | Rows |
|---|---:|
| pass | 67,463 |
| source_gap | 6,462 |
| warn | 73 |

## Issue Counts

| Issue | Rows |
|---|---:|
| official_reference_gap | 5,750 |
| expected_missing_primary_isin | 791 |
| missing_etf_category | 79 |
| country_isin_mismatch | 65 |
| missing_stock_sector | 29 |
| official_name_mismatch | 8 |
| venue_missing_official_source | 8 |

## Top Flagged Exchanges

| Exchange | Pass | Notice | Source Gap | Warn | Quarantine |
|---|---:|---:|---:|---:|---:|
| OTC | 7,864 | 0 | 3,183 | 6 | 0 |
| B3 | 1,262 | 0 | 322 | 0 | 0 |
| LSE | 6,364 | 0 | 142 | 60 | 0 |
| TSX | 1,704 | 0 | 200 | 0 | 0 |
| NASDAQ | 4,440 | 0 | 194 | 1 | 0 |
| BSE_IN | 2,459 | 0 | 183 | 0 | 0 |
| BMV | 9 | 0 | 170 | 0 | 0 |
| NYSE ARCA | 2,499 | 0 | 154 | 0 | 0 |
| BME | 78 | 0 | 143 | 0 | 0 |
| ASX | 1,160 | 0 | 137 | 0 | 0 |
| NSE_IN | 2,369 | 0 | 134 | 0 | 0 |
| XETRA | 3,744 | 0 | 100 | 0 | 0 |
| TSXV | 970 | 0 | 94 | 2 | 0 |
| AMS | 240 | 0 | 90 | 0 | 0 |
| NYSE | 1,990 | 0 | 89 | 1 | 0 |
| JSE | 124 | 0 | 88 | 0 | 0 |
| BATS | 1,155 | 0 | 86 | 0 | 0 |
| Euronext | 1,003 | 0 | 80 | 0 | 0 |
| TASE | 596 | 0 | 77 | 0 | 0 |
| ATHEX | 90 | 0 | 66 | 0 | 0 |

## Notes

- `entry_quality.csv` contains one row per `listing_key` and is the complete per-entry report.
- `notice` marks soft alias-review hints; it is not a structural row warning.
- `source_gap` means the row is structurally valid but lacks stronger source or metadata coverage.
- `quarantine` means deterministic checks found a hard contradiction that should be fixed before treating the row as high quality.

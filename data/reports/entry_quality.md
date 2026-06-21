# Entry Quality Report

Generated at: `2026-06-21T21:16:00Z`

## Status Counts

| Status | Rows |
|---|---:|
| pass | 67,688 |
| source_gap | 6,791 |
| warn | 75 |

## Issue Counts

| Issue | Rows |
|---|---:|
| official_reference_gap | 5,669 |
| expected_missing_primary_isin | 904 |
| venue_missing_official_source | 285 |
| missing_etf_category | 82 |
| country_isin_mismatch | 65 |
| missing_stock_sector | 50 |
| official_name_mismatch | 8 |
| official_isin_mismatch | 2 |

## Top Flagged Exchanges

| Exchange | Pass | Notice | Source Gap | Warn | Quarantine |
|---|---:|---:|---:|---:|---:|
| OTC | 7,863 | 0 | 3,179 | 6 | 0 |
| B3 | 1,262 | 0 | 321 | 0 | 0 |
| Borsa Italiana | 0 | 0 | 277 | 0 | 0 |
| NASDAQ | 4,399 | 0 | 267 | 1 | 0 |
| TSX | 1,701 | 0 | 202 | 0 | 0 |
| LSE | 6,364 | 0 | 140 | 60 | 0 |
| BSE_IN | 2,459 | 0 | 179 | 0 | 0 |
| BMV | 11 | 0 | 168 | 0 | 0 |
| NYSE ARCA | 2,499 | 0 | 154 | 0 | 0 |
| ASX | 1,478 | 0 | 150 | 1 | 0 |
| BME | 78 | 0 | 143 | 0 | 0 |
| NSE_IN | 2,369 | 0 | 134 | 0 | 0 |
| NYSE | 1,926 | 0 | 103 | 0 | 0 |
| XETRA | 3,742 | 0 | 102 | 0 | 0 |
| TSXV | 970 | 0 | 94 | 2 | 0 |
| AMS | 240 | 0 | 90 | 0 | 0 |
| JSE | 124 | 0 | 88 | 0 | 0 |
| BATS | 1,155 | 0 | 86 | 0 | 0 |
| Euronext | 1,003 | 0 | 80 | 0 | 0 |
| TASE | 595 | 0 | 77 | 0 | 0 |

## Notes

- `entry_quality.csv` contains one row per `listing_key` and is the complete per-entry report.
- `notice` marks soft alias-review hints; it is not a structural row warning.
- `source_gap` means the row is structurally valid but lacks stronger source or metadata coverage.
- `quarantine` means deterministic checks found a hard contradiction that should be fixed before treating the row as high quality.

# Entry Quality Report

Generated at: `2026-07-20T09:45:22Z`

## Status Counts

| Status | Rows |
|---|---:|
| pass | 67,745 |
| source_gap | 6,886 |
| warn | 74 |

## Issue Counts

| Issue | Rows |
|---|---:|
| official_reference_gap | 6,201 |
| expected_missing_primary_isin | 752 |
| missing_etf_category | 93 |
| country_isin_mismatch | 65 |
| missing_stock_sector | 21 |
| official_name_mismatch | 7 |
| official_isin_mismatch | 2 |

## Top Flagged Exchanges

| Exchange | Pass | Notice | Source Gap | Warn | Quarantine |
|---|---:|---:|---:|---:|---:|
| OTC | 7,899 | 0 | 3,170 | 7 | 0 |
| B3 | 672 | 0 | 909 | 0 | 0 |
| NASDAQ | 4,511 | 0 | 202 | 1 | 0 |
| TSX | 1,704 | 0 | 199 | 0 | 0 |
| LSE | 6,366 | 0 | 132 | 60 | 0 |
| NYSE ARCA | 2,495 | 0 | 186 | 0 | 0 |
| BMV | 0 | 0 | 179 | 0 | 0 |
| BATS | 1,137 | 0 | 160 | 0 | 0 |
| ASX | 1,491 | 0 | 134 | 0 | 0 |
| NSE_IN | 2,369 | 0 | 134 | 0 | 0 |
| BME | 90 | 0 | 131 | 0 | 0 |
| XETRA | 3,730 | 0 | 115 | 0 | 0 |
| TSXV | 970 | 0 | 97 | 2 | 0 |
| Euronext | 983 | 0 | 98 | 0 | 0 |
| AMS | 241 | 0 | 90 | 0 | 0 |
| JSE | 125 | 0 | 87 | 0 | 0 |
| TASE | 597 | 0 | 75 | 0 | 0 |
| NYSE | 1,981 | 0 | 65 | 0 | 0 |
| CSE_MA | 1 | 0 | 65 | 0 | 0 |
| BSE_IN | 2,580 | 0 | 57 | 0 | 0 |

## Notes

- `entry_quality.csv` contains one row per `listing_key` and is the complete per-entry report.
- `notice` marks soft alias-review hints; it is not a structural row warning.
- `source_gap` means the row is structurally valid but lacks stronger source or metadata coverage.
- `quarantine` means deterministic checks found a hard contradiction that should be fixed before treating the row as high quality.

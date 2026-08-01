# Entry Quality Report

Generated at: `2026-08-01T08:56:15Z`

## Status Counts

| Status | Rows |
|---|---:|
| pass | 67,346 |
| source_gap | 7,393 |
| warn | 21 |

## Issue Counts

| Issue | Rows |
|---|---:|
| official_reference_gap | 6,680 |
| expected_missing_primary_isin | 799 |
| missing_etf_category | 122 |
| missing_stock_sector | 46 |
| country_isin_mismatch | 12 |
| official_name_mismatch | 7 |
| official_isin_mismatch | 2 |

## Top Flagged Exchanges

| Exchange | Pass | Notice | Source Gap | Warn | Quarantine |
|---|---:|---:|---:|---:|---:|
| OTC | 7,903 | 0 | 3,170 | 5 | 0 |
| B3 | 372 | 0 | 1,209 | 0 | 0 |
| NASDAQ | 4,496 | 0 | 239 | 1 | 0 |
| TSX | 1,696 | 0 | 202 | 0 | 0 |
| NYSE ARCA | 2,491 | 0 | 196 | 0 | 0 |
| BATS | 1,134 | 0 | 180 | 0 | 0 |
| BMV | 0 | 0 | 179 | 0 | 0 |
| TSXV | 899 | 0 | 168 | 2 | 0 |
| LSE | 6,417 | 0 | 133 | 9 | 0 |
| ASX | 1,491 | 0 | 134 | 0 | 0 |
| NSE_IN | 2,369 | 0 | 134 | 0 | 0 |
| BME | 90 | 0 | 131 | 0 | 0 |
| XETRA | 3,725 | 0 | 121 | 0 | 0 |
| Euronext | 984 | 0 | 99 | 0 | 0 |
| AMS | 241 | 0 | 90 | 0 | 0 |
| JSE | 125 | 0 | 87 | 0 | 0 |
| TASE | 594 | 0 | 78 | 0 | 0 |
| NYSE | 1,985 | 0 | 69 | 0 | 0 |
| CSE_MA | 1 | 0 | 65 | 0 | 0 |
| BSE_IN | 2,576 | 0 | 61 | 0 | 0 |

## Notes

- `entry_quality.csv` contains one row per `listing_key` and is the complete per-entry report.
- `notice` marks soft alias-review hints; it is not a structural row warning.
- `source_gap` means the row is structurally valid but lacks stronger source or metadata coverage.
- `quarantine` means deterministic checks found a hard contradiction that should be fixed before treating the row as high quality.

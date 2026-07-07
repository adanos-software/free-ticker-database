# Entry Quality Report

Generated at: `2026-07-07T12:34:36Z`

## Status Counts

| Status | Rows |
|---|---:|
| pass | 67,858 |
| source_gap | 6,625 |
| warn | 74 |

## Issue Counts

| Issue | Rows |
|---|---:|
| official_reference_gap | 5,736 |
| expected_missing_primary_isin | 947 |
| missing_etf_category | 86 |
| missing_stock_sector | 75 |
| country_isin_mismatch | 65 |
| official_name_mismatch | 7 |
| official_isin_mismatch | 2 |

## Top Flagged Exchanges

| Exchange | Pass | Notice | Source Gap | Warn | Quarantine |
|---|---:|---:|---:|---:|---:|
| OTC | 7,858 | 0 | 3,174 | 6 | 0 |
| B3 | 1,262 | 0 | 321 | 0 | 0 |
| NASDAQ | 4,401 | 0 | 276 | 1 | 0 |
| TSX | 1,698 | 0 | 205 | 0 | 0 |
| LSE | 6,362 | 0 | 141 | 60 | 0 |
| BSE_IN | 2,459 | 0 | 179 | 0 | 0 |
| NYSE ARCA | 2,475 | 0 | 179 | 0 | 0 |
| BMV | 11 | 0 | 168 | 0 | 0 |
| ASX | 1,476 | 0 | 152 | 1 | 0 |
| BME | 78 | 0 | 143 | 0 | 0 |
| NSE_IN | 2,369 | 0 | 134 | 0 | 0 |
| NYSE | 1,929 | 0 | 107 | 0 | 0 |
| BATS | 1,136 | 0 | 105 | 0 | 0 |
| Euronext | 980 | 0 | 103 | 0 | 0 |
| XETRA | 3,742 | 0 | 102 | 0 | 0 |
| TSXV | 969 | 0 | 95 | 2 | 0 |
| AMS | 239 | 0 | 91 | 0 | 0 |
| JSE | 124 | 0 | 88 | 0 | 0 |
| TASE | 595 | 0 | 77 | 0 | 0 |
| ATHEX | 89 | 0 | 66 | 0 | 0 |

## Notes

- `entry_quality.csv` contains one row per `listing_key` and is the complete per-entry report.
- `notice` marks soft alias-review hints; it is not a structural row warning.
- `source_gap` means the row is structurally valid but lacks stronger source or metadata coverage.
- `quarantine` means deterministic checks found a hard contradiction that should be fixed before treating the row as high quality.

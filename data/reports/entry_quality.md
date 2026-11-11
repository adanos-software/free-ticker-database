# Entry Quality Report

Generated at: `2026-11-11T15:00:41Z`

## Status Counts

| Status | Rows |
|---|---:|
| notice | 10 |
| pass | 86,569 |
| source_gap | 11,106 |
| warn | 34 |

## Issue Counts

| Issue | Rows |
|---|---:|
| official_reference_gap | 6,804 |
| venue_missing_official_source | 3,287 |
| missing_stock_sector | 624 |
| expected_missing_primary_isin | 612 |
| missing_etf_category | 127 |
| official_name_mismatch | 23 |
| official_isin_mismatch | 11 |
| low_company_name_overlap | 10 |
| country_isin_mismatch | 1 |

## Top Flagged Exchanges

| Exchange | Pass | Notice | Source Gap | Warn | Quarantine |
|---|---:|---:|---:|---:|---:|
| OTC | 8,485 | 2 | 3,251 | 14 | 0 |
| XSTU | 0 | 0 | 2,773 | 0 | 0 |
| FSX | 7,632 | 0 | 511 | 0 | 0 |
| B3 | 1,247 | 0 | 342 | 0 | 0 |
| NASDAQ | 4,508 | 4 | 290 | 4 | 0 |
| BMV | 85 | 0 | 259 | 0 | 0 |
| Munich | 0 | 0 | 223 | 0 | 0 |
| XETRA | 5,335 | 0 | 218 | 0 | 0 |
| XDUS | 0 | 0 | 199 | 0 | 0 |
| AMS | 553 | 0 | 195 | 0 | 0 |
| BVB | 55 | 0 | 192 | 0 | 0 |
| NYSE ARCA | 2,535 | 0 | 188 | 1 | 0 |
| TSX | 2,257 | 0 | 186 | 0 | 0 |
| ASX | 2,101 | 2 | 156 | 0 | 0 |
| TSXV | 1,272 | 0 | 147 | 3 | 0 |
| LSE | 6,879 | 0 | 145 | 2 | 0 |
| Euronext | 1,335 | 0 | 141 | 1 | 0 |
| BATS | 1,279 | 0 | 109 | 0 | 0 |
| HKEX | 3,150 | 0 | 92 | 3 | 0 |
| JSE | 123 | 0 | 89 | 0 | 0 |

## Notes

- `entry_quality.csv` contains one row per `listing_key` and is the complete per-entry report.
- `notice` marks soft alias-review hints; it is not a structural row warning.
- `source_gap` means the row is structurally valid but lacks stronger source or metadata coverage.
- `quarantine` means deterministic checks found a hard contradiction that should be fixed before treating the row as high quality.

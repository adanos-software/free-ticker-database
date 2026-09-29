# Entry Quality Report

Generated at: `2026-09-29T15:23:16Z`

## Status Counts

| Status | Rows |
|---|---:|
| pass | 82,623 |
| source_gap | 14,483 |
| warn | 35 |

## Issue Counts

| Issue | Rows |
|---|---:|
| official_reference_gap | 6,442 |
| missing_stock_sector | 3,933 |
| venue_missing_official_source | 3,287 |
| expected_missing_primary_isin | 1,076 |
| missing_etf_category | 435 |
| official_name_mismatch | 29 |
| official_isin_mismatch | 7 |

## Top Flagged Exchanges

| Exchange | Pass | Notice | Source Gap | Warn | Quarantine |
|---|---:|---:|---:|---:|---:|
| OTC | 8,484 | 0 | 3,254 | 14 | 0 |
| XSTU | 0 | 0 | 2,773 | 0 | 0 |
| BSE_IN | 2,811 | 0 | 2,238 | 1 | 0 |
| FSX | 7,147 | 0 | 996 | 0 | 0 |
| NASDAQ | 4,442 | 0 | 351 | 4 | 0 |
| B3 | 1,243 | 0 | 346 | 0 | 0 |
| XETRA | 4,855 | 0 | 324 | 0 | 0 |
| NYSE ARCA | 2,503 | 0 | 280 | 2 | 0 |
| BMV | 77 | 0 | 267 | 0 | 0 |
| TSX | 2,125 | 0 | 247 | 0 | 0 |
| Munich | 0 | 0 | 223 | 0 | 0 |
| BATS | 1,203 | 0 | 203 | 0 | 0 |
| AMS | 537 | 0 | 200 | 0 | 0 |
| XDUS | 0 | 0 | 199 | 0 | 0 |
| BVB | 49 | 0 | 198 | 0 | 0 |
| LSE | 6,875 | 0 | 149 | 2 | 0 |
| ASX | 2,110 | 0 | 149 | 0 | 0 |
| TSXV | 1,280 | 0 | 139 | 3 | 0 |
| HKEX | 3,059 | 0 | 138 | 1 | 0 |
| Euronext | 1,341 | 0 | 135 | 1 | 0 |

## Notes

- `entry_quality.csv` contains one row per `listing_key` and is the complete per-entry report.
- `notice` marks soft alias-review hints; it is not a structural row warning.
- `source_gap` means the row is structurally valid but lacks stronger source or metadata coverage.
- `quarantine` means deterministic checks found a hard contradiction that should be fixed before treating the row as high quality.

# Entry Quality Report

Generated at: `2026-08-18T07:06:20Z`

## Status Counts

| Status | Rows |
|---|---:|
| pass | 70,760 |
| source_gap | 21,018 |
| warn | 199 |

## Issue Counts

| Issue | Rows |
|---|---:|
| venue_missing_official_source | 11,430 |
| official_reference_gap | 6,456 |
| missing_stock_sector | 2,673 |
| expected_missing_primary_isin | 1,155 |
| missing_etf_category | 936 |
| country_isin_mismatch | 95 |
| official_isin_mismatch | 56 |
| official_name_mismatch | 50 |

## Top Flagged Exchanges

| Exchange | Pass | Notice | Source Gap | Warn | Quarantine |
|---|---:|---:|---:|---:|---:|
| FSX | 0 | 0 | 8,141 | 2 | 0 |
| OTC | 8,390 | 0 | 3,255 | 108 | 0 |
| XSTU | 0 | 0 | 2,772 | 1 | 0 |
| TSX | 1,704 | 0 | 592 | 0 | 0 |
| TSXV | 905 | 0 | 514 | 3 | 0 |
| LSE | 6,523 | 0 | 483 | 24 | 0 |
| NASDAQ | 4,371 | 0 | 385 | 8 | 0 |
| ASX | 1,872 | 0 | 387 | 0 | 0 |
| BMV | 0 | 0 | 344 | 0 | 0 |
| B3 | 1,250 | 0 | 331 | 0 | 0 |
| Euronext | 1,221 | 0 | 235 | 21 | 0 |
| NYSE ARCA | 2,480 | 0 | 241 | 1 | 0 |
| Munich | 0 | 0 | 223 | 0 | 0 |
| NSE_IN | 2,290 | 0 | 213 | 0 | 0 |
| BATS | 1,130 | 0 | 210 | 0 | 0 |
| XETRA | 4,112 | 0 | 200 | 3 | 0 |
| XDUS | 0 | 0 | 199 | 0 | 0 |
| TASE | 604 | 0 | 196 | 1 | 0 |
| AMS | 361 | 0 | 183 | 2 | 0 |
| BSE_IN | 2,576 | 0 | 156 | 0 | 0 |

## Notes

- `entry_quality.csv` contains one row per `listing_key` and is the complete per-entry report.
- `notice` marks soft alias-review hints; it is not a structural row warning.
- `source_gap` means the row is structurally valid but lacks stronger source or metadata coverage.
- `quarantine` means deterministic checks found a hard contradiction that should be fixed before treating the row as high quality.

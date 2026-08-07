# Entry Quality Report

Generated at: `2026-08-07T07:55:27Z`

## Status Counts

| Status | Rows |
|---|---:|
| pass | 70,779 |
| source_gap | 20,951 |
| warn | 202 |

## Issue Counts

| Issue | Rows |
|---|---:|
| venue_missing_official_source | 11,430 |
| official_reference_gap | 6,467 |
| missing_stock_sector | 2,651 |
| expected_missing_primary_isin | 1,110 |
| missing_etf_category | 905 |
| country_isin_mismatch | 95 |
| official_isin_mismatch | 56 |
| official_name_mismatch | 53 |

## Top Flagged Exchanges

| Exchange | Pass | Notice | Source Gap | Warn | Quarantine |
|---|---:|---:|---:|---:|---:|
| FSX | 0 | 0 | 8,141 | 2 | 0 |
| OTC | 8,390 | 0 | 3,256 | 108 | 0 |
| XSTU | 0 | 0 | 2,772 | 1 | 0 |
| TSX | 1,704 | 0 | 592 | 0 | 0 |
| TSXV | 905 | 0 | 514 | 3 | 0 |
| LSE | 6,523 | 0 | 483 | 24 | 0 |
| ASX | 1,868 | 0 | 387 | 4 | 0 |
| NASDAQ | 4,371 | 0 | 367 | 8 | 0 |
| BMV | 0 | 0 | 344 | 0 | 0 |
| B3 | 1,250 | 0 | 331 | 0 | 0 |
| Euronext | 1,221 | 0 | 235 | 21 | 0 |
| NYSE ARCA | 2,482 | 0 | 226 | 1 | 0 |
| Munich | 0 | 0 | 223 | 0 | 0 |
| NSE_IN | 2,290 | 0 | 213 | 0 | 0 |
| XETRA | 4,112 | 0 | 200 | 3 | 0 |
| XDUS | 0 | 0 | 199 | 0 | 0 |
| TASE | 604 | 0 | 196 | 1 | 0 |
| BATS | 1,134 | 0 | 195 | 0 | 0 |
| AMS | 361 | 0 | 183 | 2 | 0 |
| BSE_IN | 2,576 | 0 | 156 | 0 | 0 |

## Notes

- `entry_quality.csv` contains one row per `listing_key` and is the complete per-entry report.
- `notice` marks soft alias-review hints; it is not a structural row warning.
- `source_gap` means the row is structurally valid but lacks stronger source or metadata coverage.
- `quarantine` means deterministic checks found a hard contradiction that should be fixed before treating the row as high quality.

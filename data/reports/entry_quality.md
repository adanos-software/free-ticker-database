# Entry Quality Report

Generated at: `2026-08-19T15:18:32Z`

## Status Counts

| Status | Rows |
|---|---:|
| pass | 80,381 |
| source_gap | 11,437 |
| warn | 188 |

## Issue Counts

| Issue | Rows |
|---|---:|
| official_reference_gap | 6,448 |
| venue_missing_official_source | 3,287 |
| missing_stock_sector | 1,300 |
| expected_missing_primary_isin | 700 |
| country_isin_mismatch | 95 |
| official_isin_mismatch | 57 |
| official_name_mismatch | 38 |
| missing_etf_category | 36 |

## Top Flagged Exchanges

| Exchange | Pass | Notice | Source Gap | Warn | Quarantine |
|---|---:|---:|---:|---:|---:|
| OTC | 8,403 | 0 | 3,255 | 95 | 0 |
| XSTU | 0 | 0 | 2,772 | 1 | 0 |
| FSX | 7,151 | 0 | 990 | 2 | 0 |
| B3 | 1,241 | 0 | 340 | 0 | 0 |
| NASDAQ | 4,493 | 0 | 267 | 7 | 0 |
| BMV | 76 | 0 | 268 | 0 | 0 |
| Munich | 0 | 0 | 223 | 0 | 0 |
| NYSE ARCA | 2,526 | 0 | 201 | 2 | 0 |
| XDUS | 0 | 0 | 199 | 0 | 0 |
| LSE | 6,854 | 0 | 151 | 25 | 0 |
| AMS | 371 | 0 | 173 | 2 | 0 |
| TSX | 2,122 | 0 | 174 | 0 | 0 |
| NSE_IN | 2,331 | 0 | 172 | 0 | 0 |
| ASX | 2,108 | 0 | 151 | 0 | 0 |
| BME | 127 | 0 | 146 | 3 | 0 |
| Euronext | 1,329 | 0 | 127 | 21 | 0 |
| TSXV | 1,283 | 0 | 136 | 3 | 0 |
| XETRA | 4,182 | 0 | 130 | 3 | 0 |
| BATS | 1,226 | 0 | 131 | 0 | 0 |
| NYSE | 1,922 | 0 | 92 | 8 | 0 |

## Notes

- `entry_quality.csv` contains one row per `listing_key` and is the complete per-entry report.
- `notice` marks soft alias-review hints; it is not a structural row warning.
- `source_gap` means the row is structurally valid but lacks stronger source or metadata coverage.
- `quarantine` means deterministic checks found a hard contradiction that should be fixed before treating the row as high quality.

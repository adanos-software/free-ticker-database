# Entry Quality Report

Generated at: `2026-08-21T07:18:03Z`

## Status Counts

| Status | Rows |
|---|---:|
| pass | 80,465 |
| source_gap | 11,460 |
| warn | 81 |

## Issue Counts

| Issue | Rows |
|---|---:|
| official_reference_gap | 6,455 |
| venue_missing_official_source | 3,287 |
| missing_stock_sector | 1,284 |
| expected_missing_primary_isin | 690 |
| official_name_mismatch | 38 |
| official_isin_mismatch | 36 |
| missing_etf_category | 36 |
| country_isin_mismatch | 9 |

## Top Flagged Exchanges

| Exchange | Pass | Notice | Source Gap | Warn | Quarantine |
|---|---:|---:|---:|---:|---:|
| OTC | 8,477 | 0 | 3,257 | 19 | 0 |
| XSTU | 0 | 0 | 2,772 | 1 | 0 |
| FSX | 7,155 | 0 | 987 | 1 | 0 |
| B3 | 1,241 | 0 | 340 | 0 | 0 |
| NASDAQ | 4,493 | 0 | 267 | 7 | 0 |
| BMV | 76 | 0 | 268 | 0 | 0 |
| Munich | 0 | 0 | 223 | 0 | 0 |
| NYSE ARCA | 2,526 | 0 | 201 | 2 | 0 |
| XDUS | 0 | 0 | 199 | 0 | 0 |
| BSE_IN | 2,535 | 0 | 197 | 0 | 0 |
| AMS | 372 | 0 | 173 | 1 | 0 |
| TSX | 2,122 | 0 | 174 | 0 | 0 |
| NSE_IN | 2,331 | 0 | 172 | 0 | 0 |
| LSE | 6,869 | 0 | 151 | 10 | 0 |
| ASX | 2,108 | 0 | 151 | 0 | 0 |
| Euronext | 1,331 | 0 | 128 | 18 | 0 |
| XETRA | 4,170 | 0 | 143 | 2 | 0 |
| TSXV | 1,283 | 0 | 136 | 3 | 0 |
| BATS | 1,226 | 0 | 131 | 0 | 0 |
| NYSE | 1,922 | 0 | 92 | 8 | 0 |

## Notes

- `entry_quality.csv` contains one row per `listing_key` and is the complete per-entry report.
- `notice` marks soft alias-review hints; it is not a structural row warning.
- `source_gap` means the row is structurally valid but lacks stronger source or metadata coverage.
- `quarantine` means deterministic checks found a hard contradiction that should be fixed before treating the row as high quality.

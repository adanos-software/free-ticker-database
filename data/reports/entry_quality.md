# Entry Quality Report

Generated at: `2026-09-08T11:14:31Z`

## Status Counts

| Status | Rows |
|---|---:|
| pass | 80,694 |
| source_gap | 11,311 |
| warn | 31 |

## Issue Counts

| Issue | Rows |
|---|---:|
| official_reference_gap | 6,233 |
| venue_missing_official_source | 3,287 |
| missing_stock_sector | 1,298 |
| expected_missing_primary_isin | 768 |
| missing_etf_category | 56 |
| official_name_mismatch | 26 |
| official_isin_mismatch | 6 |

## Top Flagged Exchanges

| Exchange | Pass | Notice | Source Gap | Warn | Quarantine |
|---|---:|---:|---:|---:|---:|
| OTC | 8,482 | 0 | 3,257 | 13 | 0 |
| XSTU | 0 | 0 | 2,773 | 0 | 0 |
| FSX | 7,139 | 0 | 1,004 | 0 | 0 |
| B3 | 1,241 | 0 | 340 | 0 | 0 |
| NASDAQ | 4,467 | 0 | 274 | 4 | 0 |
| BMV | 77 | 0 | 267 | 0 | 0 |
| NYSE ARCA | 2,518 | 0 | 246 | 2 | 0 |
| Munich | 0 | 0 | 223 | 0 | 0 |
| XDUS | 0 | 0 | 199 | 0 | 0 |
| TSX | 2,122 | 0 | 174 | 0 | 0 |
| AMS | 373 | 0 | 173 | 0 | 0 |
| LSE | 6,876 | 0 | 152 | 2 | 0 |
| ASX | 2,109 | 0 | 150 | 0 | 0 |
| BATS | 1,223 | 0 | 150 | 0 | 0 |
| XETRA | 4,166 | 0 | 149 | 0 | 0 |
| TSXV | 1,283 | 0 | 137 | 2 | 0 |
| Euronext | 1,344 | 0 | 132 | 1 | 0 |
| NYSE | 1,920 | 0 | 101 | 4 | 0 |
| JSE | 123 | 0 | 89 | 0 | 0 |
| TASE | 717 | 0 | 84 | 0 | 0 |

## Notes

- `entry_quality.csv` contains one row per `listing_key` and is the complete per-entry report.
- `notice` marks soft alias-review hints; it is not a structural row warning.
- `source_gap` means the row is structurally valid but lacks stronger source or metadata coverage.
- `quarantine` means deterministic checks found a hard contradiction that should be fixed before treating the row as high quality.

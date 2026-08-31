# Entry Quality Report

Generated at: `2026-08-31T16:37:45Z`

## Status Counts

| Status | Rows |
|---|---:|
| pass | 80,584 |
| source_gap | 11,455 |
| warn | 32 |

## Issue Counts

| Issue | Rows |
|---|---:|
| official_reference_gap | 6,389 |
| venue_missing_official_source | 3,287 |
| missing_stock_sector | 1,298 |
| expected_missing_primary_isin | 752 |
| missing_etf_category | 46 |
| official_name_mismatch | 27 |
| official_isin_mismatch | 6 |

## Top Flagged Exchanges

| Exchange | Pass | Notice | Source Gap | Warn | Quarantine |
|---|---:|---:|---:|---:|---:|
| OTC | 8,482 | 0 | 3,257 | 13 | 0 |
| XSTU | 0 | 0 | 2,773 | 0 | 0 |
| FSX | 7,144 | 0 | 999 | 0 | 0 |
| B3 | 1,241 | 0 | 340 | 0 | 0 |
| NASDAQ | 4,482 | 0 | 300 | 4 | 0 |
| BMV | 77 | 0 | 267 | 0 | 0 |
| NYSE ARCA | 2,520 | 0 | 236 | 2 | 0 |
| Munich | 0 | 0 | 223 | 0 | 0 |
| XDUS | 0 | 0 | 199 | 0 | 0 |
| TSX | 2,122 | 0 | 174 | 0 | 0 |
| AMS | 373 | 0 | 173 | 0 | 0 |
| NSE_IN | 2,331 | 0 | 172 | 0 | 0 |
| LSE | 6,876 | 0 | 152 | 2 | 0 |
| ASX | 2,109 | 0 | 150 | 0 | 0 |
| BATS | 1,223 | 0 | 149 | 0 | 0 |
| XETRA | 4,171 | 0 | 144 | 0 | 0 |
| TSXV | 1,283 | 0 | 137 | 2 | 0 |
| Euronext | 1,348 | 0 | 128 | 1 | 0 |
| NYSE | 1,923 | 0 | 99 | 5 | 0 |
| JSE | 123 | 0 | 89 | 0 | 0 |

## Notes

- `entry_quality.csv` contains one row per `listing_key` and is the complete per-entry report.
- `notice` marks soft alias-review hints; it is not a structural row warning.
- `source_gap` means the row is structurally valid but lacks stronger source or metadata coverage.
- `quarantine` means deterministic checks found a hard contradiction that should be fixed before treating the row as high quality.

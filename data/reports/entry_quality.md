# Entry Quality Report

Generated at: `2026-08-27T06:47:28Z`

## Status Counts

| Status | Rows |
|---|---:|
| pass | 80,622 |
| source_gap | 11,374 |
| warn | 34 |

## Issue Counts

| Issue | Rows |
|---|---:|
| official_reference_gap | 6,345 |
| venue_missing_official_source | 3,287 |
| missing_stock_sector | 1,286 |
| expected_missing_primary_isin | 713 |
| official_name_mismatch | 30 |
| missing_etf_category | 19 |
| official_isin_mismatch | 6 |

## Top Flagged Exchanges

| Exchange | Pass | Notice | Source Gap | Warn | Quarantine |
|---|---:|---:|---:|---:|---:|
| OTC | 8,482 | 0 | 3,257 | 13 | 0 |
| XSTU | 0 | 0 | 2,773 | 0 | 0 |
| FSX | 7,155 | 0 | 988 | 0 | 0 |
| B3 | 1,241 | 0 | 340 | 0 | 0 |
| NASDAQ | 4,494 | 0 | 274 | 5 | 0 |
| BMV | 76 | 0 | 268 | 0 | 0 |
| Munich | 0 | 0 | 223 | 0 | 0 |
| NYSE ARCA | 2,525 | 0 | 211 | 2 | 0 |
| XDUS | 0 | 0 | 199 | 0 | 0 |
| TSX | 2,122 | 0 | 174 | 0 | 0 |
| AMS | 373 | 0 | 173 | 0 | 0 |
| NSE_IN | 2,331 | 0 | 172 | 0 | 0 |
| LSE | 6,876 | 0 | 152 | 2 | 0 |
| ASX | 2,109 | 0 | 150 | 0 | 0 |
| XETRA | 4,171 | 0 | 144 | 0 | 0 |
| BATS | 1,225 | 0 | 141 | 0 | 0 |
| TSXV | 1,283 | 0 | 137 | 2 | 0 |
| Euronext | 1,347 | 0 | 128 | 2 | 0 |
| NYSE | 1,925 | 0 | 93 | 6 | 0 |
| JSE | 123 | 0 | 89 | 0 | 0 |

## Notes

- `entry_quality.csv` contains one row per `listing_key` and is the complete per-entry report.
- `notice` marks soft alias-review hints; it is not a structural row warning.
- `source_gap` means the row is structurally valid but lacks stronger source or metadata coverage.
- `quarantine` means deterministic checks found a hard contradiction that should be fixed before treating the row as high quality.

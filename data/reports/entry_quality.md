# Entry Quality Report

Generated at: `2026-10-08T07:44:21Z`

## Status Counts

| Status | Rows |
|---|---:|
| pass | 86,245 |
| source_gap | 11,001 |
| warn | 27 |

## Issue Counts

| Issue | Rows |
|---|---:|
| official_reference_gap | 6,553 |
| venue_missing_official_source | 3,287 |
| missing_stock_sector | 735 |
| expected_missing_primary_isin | 645 |
| missing_etf_category | 119 |
| official_name_mismatch | 21 |
| official_isin_mismatch | 7 |

## Top Flagged Exchanges

| Exchange | Pass | Notice | Source Gap | Warn | Quarantine |
|---|---:|---:|---:|---:|---:|
| OTC | 8,484 | 0 | 3,254 | 14 | 0 |
| XSTU | 0 | 0 | 2,773 | 0 | 0 |
| FSX | 7,727 | 0 | 416 | 0 | 0 |
| B3 | 1,243 | 0 | 346 | 0 | 0 |
| NASDAQ | 4,511 | 0 | 293 | 2 | 0 |
| BMV | 77 | 0 | 267 | 0 | 0 |
| NYSE ARCA | 2,565 | 0 | 227 | 1 | 0 |
| TSX | 2,196 | 0 | 225 | 0 | 0 |
| Munich | 0 | 0 | 223 | 0 | 0 |
| XDUS | 0 | 0 | 199 | 0 | 0 |
| AMS | 538 | 0 | 199 | 0 | 0 |
| BVB | 55 | 0 | 192 | 0 | 0 |
| TSXV | 1,260 | 0 | 159 | 3 | 0 |
| LSE | 6,875 | 0 | 149 | 2 | 0 |
| XETRA | 5,038 | 0 | 149 | 0 | 0 |
| ASX | 2,113 | 0 | 146 | 0 | 0 |
| BATS | 1,285 | 0 | 130 | 0 | 0 |
| Euronext | 1,357 | 0 | 119 | 1 | 0 |
| JSE | 123 | 0 | 89 | 0 | 0 |
| TASE | 714 | 0 | 87 | 0 | 0 |

## Notes

- `entry_quality.csv` contains one row per `listing_key` and is the complete per-entry report.
- `notice` marks soft alias-review hints; it is not a structural row warning.
- `source_gap` means the row is structurally valid but lacks stronger source or metadata coverage.
- `quarantine` means deterministic checks found a hard contradiction that should be fixed before treating the row as high quality.

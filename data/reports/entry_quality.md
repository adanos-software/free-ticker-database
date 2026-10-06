# Entry Quality Report

Generated at: `2026-10-06T08:47:10Z`

## Status Counts

| Status | Rows |
|---|---:|
| pass | 85,694 |
| source_gap | 11,458 |
| warn | 24 |

## Issue Counts

| Issue | Rows |
|---|---:|
| official_reference_gap | 6,458 |
| venue_missing_official_source | 3,287 |
| missing_stock_sector | 1,325 |
| expected_missing_primary_isin | 593 |
| missing_etf_category | 67 |
| official_name_mismatch | 19 |
| official_isin_mismatch | 6 |

## Top Flagged Exchanges

| Exchange | Pass | Notice | Source Gap | Warn | Quarantine |
|---|---:|---:|---:|---:|---:|
| OTC | 8,484 | 0 | 3,254 | 14 | 0 |
| XSTU | 0 | 0 | 2,773 | 0 | 0 |
| FSX | 7,339 | 0 | 804 | 0 | 0 |
| B3 | 1,243 | 0 | 346 | 0 | 0 |
| NASDAQ | 4,513 | 0 | 292 | 0 | 0 |
| BMV | 77 | 0 | 267 | 0 | 0 |
| NYSE ARCA | 2,567 | 0 | 225 | 1 | 0 |
| Munich | 0 | 0 | 223 | 0 | 0 |
| XDUS | 0 | 0 | 199 | 0 | 0 |
| AMS | 538 | 0 | 199 | 0 | 0 |
| BVB | 55 | 0 | 192 | 0 | 0 |
| TSX | 2,195 | 0 | 177 | 0 | 0 |
| XETRA | 5,029 | 0 | 158 | 0 | 0 |
| LSE | 6,875 | 0 | 149 | 2 | 0 |
| ASX | 2,113 | 0 | 146 | 0 | 0 |
| HKEX | 3,060 | 0 | 137 | 1 | 0 |
| Euronext | 1,342 | 0 | 134 | 1 | 0 |
| BATS | 1,285 | 0 | 128 | 0 | 0 |
| TSXV | 1,304 | 0 | 115 | 3 | 0 |
| JSE | 123 | 0 | 89 | 0 | 0 |

## Notes

- `entry_quality.csv` contains one row per `listing_key` and is the complete per-entry report.
- `notice` marks soft alias-review hints; it is not a structural row warning.
- `source_gap` means the row is structurally valid but lacks stronger source or metadata coverage.
- `quarantine` means deterministic checks found a hard contradiction that should be fixed before treating the row as high quality.

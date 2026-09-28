# Entry Quality Report

Generated at: `2026-09-28T13:54:11Z`

## Status Counts

| Status | Rows |
|---|---:|
| pass | 82,702 |
| source_gap | 14,406 |
| warn | 34 |

## Issue Counts

| Issue | Rows |
|---|---:|
| official_reference_gap | 6,300 |
| missing_stock_sector | 3,889 |
| venue_missing_official_source | 3,287 |
| expected_missing_primary_isin | 1,028 |
| missing_etf_category | 434 |
| official_name_mismatch | 28 |
| official_isin_mismatch | 7 |

## Top Flagged Exchanges

| Exchange | Pass | Notice | Source Gap | Warn | Quarantine |
|---|---:|---:|---:|---:|---:|
| OTC | 8,484 | 0 | 3,254 | 14 | 0 |
| XSTU | 0 | 0 | 2,773 | 0 | 0 |
| BSE_IN | 2,828 | 0 | 2,264 | 1 | 0 |
| FSX | 7,148 | 0 | 995 | 0 | 0 |
| NASDAQ | 4,443 | 0 | 346 | 4 | 0 |
| B3 | 1,243 | 0 | 346 | 0 | 0 |
| XETRA | 4,855 | 0 | 324 | 0 | 0 |
| NYSE ARCA | 2,509 | 0 | 274 | 2 | 0 |
| BMV | 77 | 0 | 267 | 0 | 0 |
| TSX | 2,127 | 0 | 244 | 0 | 0 |
| Munich | 0 | 0 | 223 | 0 | 0 |
| AMS | 537 | 0 | 201 | 0 | 0 |
| XDUS | 0 | 0 | 199 | 0 | 0 |
| BATS | 1,210 | 0 | 196 | 0 | 0 |
| LSE | 6,875 | 0 | 153 | 2 | 0 |
| ASX | 2,113 | 0 | 146 | 0 | 0 |
| BVB | 102 | 0 | 145 | 0 | 0 |
| TSXV | 1,283 | 0 | 137 | 2 | 0 |
| Euronext | 1,344 | 0 | 132 | 1 | 0 |
| HKEX | 3,035 | 0 | 129 | 1 | 0 |

## Notes

- `entry_quality.csv` contains one row per `listing_key` and is the complete per-entry report.
- `notice` marks soft alias-review hints; it is not a structural row warning.
- `source_gap` means the row is structurally valid but lacks stronger source or metadata coverage.
- `quarantine` means deterministic checks found a hard contradiction that should be fixed before treating the row as high quality.

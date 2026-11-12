# Entry Quality Report

Generated at: `2026-11-12T19:13:01Z`

## Status Counts

| Status | Rows |
|---|---:|
| notice | 10 |
| pass | 86,557 |
| source_gap | 11,153 |
| warn | 33 |

## Issue Counts

| Issue | Rows |
|---|---:|
| official_reference_gap | 6,828 |
| venue_missing_official_source | 3,287 |
| expected_missing_primary_isin | 630 |
| missing_stock_sector | 619 |
| missing_etf_category | 144 |
| official_name_mismatch | 23 |
| official_isin_mismatch | 10 |
| low_company_name_overlap | 10 |
| country_isin_mismatch | 1 |

## Top Flagged Exchanges

| Exchange | Pass | Notice | Source Gap | Warn | Quarantine |
|---|---:|---:|---:|---:|---:|
| OTC | 8,485 | 2 | 3,251 | 14 | 0 |
| XSTU | 0 | 0 | 2,773 | 0 | 0 |
| FSX | 7,627 | 0 | 516 | 0 | 0 |
| B3 | 1,247 | 0 | 342 | 0 | 0 |
| NASDAQ | 4,504 | 4 | 294 | 4 | 0 |
| BMV | 87 | 0 | 257 | 0 | 0 |
| XETRA | 5,330 | 0 | 233 | 0 | 0 |
| Munich | 0 | 0 | 223 | 0 | 0 |
| NYSE ARCA | 2,525 | 0 | 205 | 1 | 0 |
| XDUS | 0 | 0 | 199 | 0 | 0 |
| AMS | 553 | 0 | 195 | 0 | 0 |
| BVB | 55 | 0 | 192 | 0 | 0 |
| TSX | 2,257 | 0 | 186 | 0 | 0 |
| ASX | 2,101 | 2 | 156 | 0 | 0 |
| TSXV | 1,274 | 0 | 145 | 3 | 0 |
| LSE | 6,879 | 0 | 145 | 2 | 0 |
| Euronext | 1,333 | 0 | 143 | 1 | 0 |
| BATS | 1,278 | 0 | 113 | 0 | 0 |
| HKEX | 3,148 | 0 | 105 | 2 | 0 |
| JSE | 123 | 0 | 89 | 0 | 0 |

## Notes

- `entry_quality.csv` contains one row per `listing_key` and is the complete per-entry report.
- `notice` marks soft alias-review hints; it is not a structural row warning.
- `source_gap` means the row is structurally valid but lacks stronger source or metadata coverage.
- `quarantine` means deterministic checks found a hard contradiction that should be fixed before treating the row as high quality.

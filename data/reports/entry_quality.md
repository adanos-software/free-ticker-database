# Entry Quality Report

Generated at: `2026-11-16T16:24:20Z`

## Status Counts

| Status | Rows |
|---|---:|
| notice | 10 |
| pass | 86,583 |
| source_gap | 11,010 |
| warn | 34 |

## Issue Counts

| Issue | Rows |
|---|---:|
| official_reference_gap | 6,705 |
| venue_missing_official_source | 3,287 |
| expected_missing_primary_isin | 629 |
| missing_stock_sector | 594 |
| missing_etf_category | 149 |
| official_name_mismatch | 23 |
| official_isin_mismatch | 11 |
| low_company_name_overlap | 10 |
| country_isin_mismatch | 1 |

## Top Flagged Exchanges

| Exchange | Pass | Notice | Source Gap | Warn | Quarantine |
|---|---:|---:|---:|---:|---:|
| OTC | 8,486 | 2 | 3,251 | 13 | 0 |
| XSTU | 0 | 0 | 2,773 | 0 | 0 |
| FSX | 7,629 | 0 | 514 | 0 | 0 |
| B3 | 1,247 | 0 | 342 | 0 | 0 |
| NASDAQ | 4,502 | 4 | 251 | 4 | 0 |
| BMV | 91 | 0 | 253 | 0 | 0 |
| XETRA | 5,332 | 0 | 231 | 0 | 0 |
| Munich | 0 | 0 | 223 | 0 | 0 |
| XDUS | 0 | 0 | 199 | 0 | 0 |
| AMS | 553 | 0 | 195 | 0 | 0 |
| BVB | 55 | 0 | 192 | 0 | 0 |
| TSX | 2,258 | 0 | 185 | 0 | 0 |
| NYSE ARCA | 2,524 | 0 | 170 | 1 | 0 |
| ASX | 2,101 | 2 | 156 | 0 | 0 |
| LSE | 6,879 | 0 | 143 | 2 | 0 |
| TSXV | 1,277 | 0 | 142 | 3 | 0 |
| Euronext | 1,333 | 0 | 142 | 1 | 0 |
| HKEX | 3,146 | 0 | 110 | 3 | 0 |
| NYSE | 1,931 | 0 | 94 | 0 | 0 |
| JSE | 123 | 0 | 89 | 0 | 0 |

## Notes

- `entry_quality.csv` contains one row per `listing_key` and is the complete per-entry report.
- `notice` marks soft alias-review hints; it is not a structural row warning.
- `source_gap` means the row is structurally valid but lacks stronger source or metadata coverage.
- `quarantine` means deterministic checks found a hard contradiction that should be fixed before treating the row as high quality.

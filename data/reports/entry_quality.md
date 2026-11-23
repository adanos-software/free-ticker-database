# Entry Quality Report

Generated at: `2026-11-23T09:37:31Z`

## Status Counts

| Status | Rows |
|---|---:|
| notice | 10 |
| pass | 86,649 |
| source_gap | 11,008 |
| warn | 35 |

## Issue Counts

| Issue | Rows |
|---|---:|
| official_reference_gap | 6,753 |
| venue_missing_official_source | 3,287 |
| expected_missing_primary_isin | 645 |
| missing_stock_sector | 566 |
| missing_etf_category | 74 |
| official_name_mismatch | 24 |
| official_isin_mismatch | 11 |
| low_company_name_overlap | 10 |
| country_isin_mismatch | 1 |

## Top Flagged Exchanges

| Exchange | Pass | Notice | Source Gap | Warn | Quarantine |
|---|---:|---:|---:|---:|---:|
| OTC | 8,485 | 2 | 3,253 | 12 | 0 |
| XSTU | 0 | 0 | 2,773 | 0 | 0 |
| FSX | 7,627 | 0 | 516 | 0 | 0 |
| B3 | 1,247 | 0 | 342 | 0 | 0 |
| NASDAQ | 4,500 | 4 | 267 | 3 | 0 |
| BMV | 91 | 0 | 253 | 0 | 0 |
| Munich | 0 | 0 | 223 | 0 | 0 |
| XETRA | 5,391 | 0 | 202 | 0 | 0 |
| XDUS | 0 | 0 | 199 | 0 | 0 |
| AMS | 554 | 0 | 194 | 0 | 0 |
| BVB | 55 | 0 | 192 | 0 | 0 |
| NYSE ARCA | 2,522 | 0 | 183 | 1 | 0 |
| TSX | 2,269 | 0 | 167 | 1 | 0 |
| ASX | 2,100 | 2 | 156 | 1 | 0 |
| LSE | 6,880 | 0 | 142 | 2 | 0 |
| Euronext | 1,333 | 0 | 142 | 1 | 0 |
| TSXV | 1,279 | 0 | 140 | 3 | 0 |
| HKEX | 3,148 | 0 | 116 | 3 | 0 |
| NYSE | 1,928 | 0 | 97 | 1 | 0 |
| JSE | 123 | 0 | 89 | 0 | 0 |

## Notes

- `entry_quality.csv` contains one row per `listing_key` and is the complete per-entry report.
- `notice` marks soft alias-review hints; it is not a structural row warning.
- `source_gap` means the row is structurally valid but lacks stronger source or metadata coverage.
- `quarantine` means deterministic checks found a hard contradiction that should be fixed before treating the row as high quality.

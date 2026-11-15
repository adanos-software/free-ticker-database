# Entry Quality Report

Generated at: `2026-11-15T18:26:19Z`

## Status Counts

| Status | Rows |
|---|---:|
| notice | 10 |
| pass | 86,561 |
| source_gap | 11,057 |
| warn | 33 |

## Issue Counts

| Issue | Rows |
|---|---:|
| official_reference_gap | 6,745 |
| venue_missing_official_source | 3,287 |
| expected_missing_primary_isin | 626 |
| missing_stock_sector | 602 |
| missing_etf_category | 146 |
| official_name_mismatch | 22 |
| official_isin_mismatch | 11 |
| low_company_name_overlap | 10 |
| country_isin_mismatch | 1 |

## Top Flagged Exchanges

| Exchange | Pass | Notice | Source Gap | Warn | Quarantine |
|---|---:|---:|---:|---:|---:|
| OTC | 8,486 | 2 | 3,251 | 13 | 0 |
| XSTU | 0 | 0 | 2,773 | 0 | 0 |
| FSX | 7,624 | 0 | 519 | 0 | 0 |
| B3 | 1,247 | 0 | 342 | 0 | 0 |
| NASDAQ | 4,502 | 4 | 263 | 4 | 0 |
| BMV | 89 | 0 | 255 | 0 | 0 |
| XETRA | 5,332 | 0 | 231 | 0 | 0 |
| Munich | 0 | 0 | 223 | 0 | 0 |
| XDUS | 0 | 0 | 199 | 0 | 0 |
| AMS | 553 | 0 | 195 | 0 | 0 |
| BVB | 55 | 0 | 192 | 0 | 0 |
| NYSE ARCA | 2,525 | 0 | 185 | 1 | 0 |
| TSX | 2,257 | 0 | 184 | 0 | 0 |
| ASX | 2,101 | 2 | 156 | 0 | 0 |
| TSXV | 1,276 | 0 | 143 | 3 | 0 |
| LSE | 6,879 | 0 | 143 | 2 | 0 |
| Euronext | 1,333 | 0 | 142 | 1 | 0 |
| HKEX | 3,146 | 0 | 110 | 3 | 0 |
| NYSE | 1,933 | 0 | 92 | 0 | 0 |
| JSE | 123 | 0 | 89 | 0 | 0 |

## Notes

- `entry_quality.csv` contains one row per `listing_key` and is the complete per-entry report.
- `notice` marks soft alias-review hints; it is not a structural row warning.
- `source_gap` means the row is structurally valid but lacks stronger source or metadata coverage.
- `quarantine` means deterministic checks found a hard contradiction that should be fixed before treating the row as high quality.

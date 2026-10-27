# Entry Quality Report

Generated at: `2026-10-27T13:25:31Z`

## Status Counts

| Status | Rows |
|---|---:|
| notice | 11 |
| pass | 86,201 |
| source_gap | 11,192 |
| warn | 35 |

## Issue Counts

| Issue | Rows |
|---|---:|
| official_reference_gap | 6,752 |
| venue_missing_official_source | 3,287 |
| missing_stock_sector | 696 |
| expected_missing_primary_isin | 679 |
| missing_etf_category | 161 |
| official_name_mismatch | 25 |
| official_isin_mismatch | 10 |
| low_company_name_overlap | 9 |
| shared_name_alias | 2 |
| country_isin_mismatch | 1 |

## Top Flagged Exchanges

| Exchange | Pass | Notice | Source Gap | Warn | Quarantine |
|---|---:|---:|---:|---:|---:|
| OTC | 8,481 | 2 | 3,255 | 14 | 0 |
| XSTU | 0 | 0 | 2,773 | 0 | 0 |
| FSX | 7,671 | 0 | 472 | 0 | 0 |
| B3 | 1,247 | 0 | 342 | 0 | 0 |
| NASDAQ | 4,527 | 4 | 285 | 6 | 0 |
| NYSE ARCA | 2,537 | 0 | 267 | 1 | 0 |
| BMV | 77 | 0 | 267 | 0 | 0 |
| TSX | 2,196 | 1 | 224 | 0 | 0 |
| Munich | 0 | 0 | 223 | 0 | 0 |
| AMS | 540 | 0 | 204 | 0 | 0 |
| XDUS | 0 | 0 | 199 | 0 | 0 |
| BVB | 55 | 0 | 192 | 0 | 0 |
| XETRA | 5,042 | 0 | 168 | 0 | 0 |
| TSXV | 1,260 | 0 | 159 | 3 | 0 |
| LSE | 6,875 | 0 | 149 | 2 | 0 |
| ASX | 2,111 | 2 | 146 | 0 | 0 |
| BATS | 1,287 | 0 | 138 | 0 | 0 |
| Euronext | 1,354 | 0 | 122 | 1 | 0 |
| JSE | 123 | 0 | 89 | 0 | 0 |
| TASE | 714 | 0 | 87 | 0 | 0 |

## Notes

- `entry_quality.csv` contains one row per `listing_key` and is the complete per-entry report.
- `notice` marks soft alias-review hints; it is not a structural row warning.
- `source_gap` means the row is structurally valid but lacks stronger source or metadata coverage.
- `quarantine` means deterministic checks found a hard contradiction that should be fixed before treating the row as high quality.

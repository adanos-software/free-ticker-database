# Entry Quality Report

Generated at: `2026-10-28T08:26:00Z`

## Status Counts

| Status | Rows |
|---|---:|
| notice | 11 |
| pass | 86,174 |
| source_gap | 11,222 |
| warn | 33 |

## Issue Counts

| Issue | Rows |
|---|---:|
| official_reference_gap | 6,779 |
| venue_missing_official_source | 3,287 |
| missing_stock_sector | 692 |
| expected_missing_primary_isin | 685 |
| missing_etf_category | 162 |
| official_name_mismatch | 23 |
| official_isin_mismatch | 10 |
| low_company_name_overlap | 9 |
| shared_name_alias | 2 |
| country_isin_mismatch | 1 |

## Top Flagged Exchanges

| Exchange | Pass | Notice | Source Gap | Warn | Quarantine |
|---|---:|---:|---:|---:|---:|
| OTC | 8,480 | 2 | 3,256 | 14 | 0 |
| XSTU | 0 | 0 | 2,773 | 0 | 0 |
| FSX | 7,672 | 0 | 471 | 0 | 0 |
| B3 | 1,247 | 0 | 342 | 0 | 0 |
| NASDAQ | 4,511 | 4 | 305 | 4 | 0 |
| NYSE ARCA | 2,533 | 0 | 273 | 1 | 0 |
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
| BATS | 1,287 | 0 | 137 | 0 | 0 |
| Euronext | 1,354 | 0 | 122 | 1 | 0 |
| JSE | 123 | 0 | 89 | 0 | 0 |
| TASE | 714 | 0 | 87 | 0 | 0 |

## Notes

- `entry_quality.csv` contains one row per `listing_key` and is the complete per-entry report.
- `notice` marks soft alias-review hints; it is not a structural row warning.
- `source_gap` means the row is structurally valid but lacks stronger source or metadata coverage.
- `quarantine` means deterministic checks found a hard contradiction that should be fixed before treating the row as high quality.

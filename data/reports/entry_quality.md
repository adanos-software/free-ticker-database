# Entry Quality Report

Generated at: `2026-10-30T13:31:13Z`

## Status Counts

| Status | Rows |
|---|---:|
| notice | 11 |
| pass | 86,431 |
| source_gap | 11,247 |
| warn | 33 |

## Issue Counts

| Issue | Rows |
|---|---:|
| official_reference_gap | 6,767 |
| venue_missing_official_source | 3,287 |
| missing_stock_sector | 708 |
| expected_missing_primary_isin | 682 |
| missing_etf_category | 193 |
| official_name_mismatch | 23 |
| official_isin_mismatch | 10 |
| low_company_name_overlap | 9 |
| shared_name_alias | 2 |
| country_isin_mismatch | 1 |

## Top Flagged Exchanges

| Exchange | Pass | Notice | Source Gap | Warn | Quarantine |
|---|---:|---:|---:|---:|---:|
| OTC | 8,482 | 2 | 3,254 | 14 | 0 |
| XSTU | 0 | 0 | 2,773 | 0 | 0 |
| FSX | 7,647 | 0 | 496 | 0 | 0 |
| B3 | 1,247 | 0 | 342 | 0 | 0 |
| NASDAQ | 4,508 | 4 | 303 | 4 | 0 |
| BMV | 77 | 0 | 267 | 0 | 0 |
| NYSE ARCA | 2,538 | 0 | 235 | 1 | 0 |
| TSX | 2,196 | 1 | 225 | 0 | 0 |
| Munich | 0 | 0 | 223 | 0 | 0 |
| XETRA | 5,323 | 0 | 213 | 0 | 0 |
| AMS | 541 | 0 | 204 | 0 | 0 |
| XDUS | 0 | 0 | 199 | 0 | 0 |
| BVB | 55 | 0 | 192 | 0 | 0 |
| TSXV | 1,260 | 0 | 159 | 3 | 0 |
| LSE | 6,875 | 0 | 149 | 2 | 0 |
| ASX | 2,111 | 2 | 146 | 0 | 0 |
| Euronext | 1,354 | 0 | 122 | 1 | 0 |
| BATS | 1,287 | 0 | 122 | 0 | 0 |
| JSE | 123 | 0 | 89 | 0 | 0 |
| TASE | 714 | 0 | 87 | 0 | 0 |

## Notes

- `entry_quality.csv` contains one row per `listing_key` and is the complete per-entry report.
- `notice` marks soft alias-review hints; it is not a structural row warning.
- `source_gap` means the row is structurally valid but lacks stronger source or metadata coverage.
- `quarantine` means deterministic checks found a hard contradiction that should be fixed before treating the row as high quality.

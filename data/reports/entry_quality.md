# Entry Quality Report

Generated at: `2026-04-13T11:17:12Z`

## Status Counts

| Status | Rows |
|---|---:|
| notice | 5,567 |
| pass | 34,408 |
| source_gap | 21,980 |

## Issue Counts

| Issue | Rows |
|---|---:|
| official_reference_gap | 9,052 |
| expected_missing_primary_isin | 7,165 |
| missing_stock_sector | 5,564 |
| missing_etf_category | 4,468 |
| shared_name_alias | 4,050 |
| low_company_name_overlap | 1,998 |
| venue_missing_official_source | 1,250 |

## Top Flagged Exchanges

| Exchange | Pass | Notice | Source Gap | Warn | Quarantine |
|---|---:|---:|---:|---:|---:|
| OTC | 2,602 | 1,634 | 6,861 | 0 | 0 |
| TSE | 67 | 0 | 3,149 | 0 | 0 |
| SSE | 1,421 | 41 | 1,327 | 0 | 0 |
| SZSE | 1,881 | 58 | 1,144 | 0 | 0 |
| B3 | 473 | 138 | 926 | 0 | 0 |
| TSX | 1,054 | 148 | 693 | 0 | 0 |
| KRX | 1,114 | 51 | 623 | 0 | 0 |
| NYSE ARCA | 1,803 | 278 | 569 | 0 | 0 |
| XETRA | 3,039 | 160 | 553 | 0 | 0 |
| LSE | 5,055 | 805 | 548 | 0 | 0 |
| ASX | 713 | 117 | 468 | 0 | 0 |
| TSXV | 355 | 260 | 451 | 0 | 0 |
| NASDAQ | 3,645 | 577 | 412 | 0 | 0 |
| KOSDAQ | 1,196 | 35 | 352 | 0 | 0 |
| BATS | 890 | 49 | 308 | 0 | 0 |
| STO | 397 | 98 | 230 | 0 | 0 |
| EGX | 0 | 2 | 223 | 0 | 0 |
| TASE | 444 | 14 | 215 | 0 | 0 |
| PSX | 159 | 8 | 206 | 0 | 0 |
| SET | 324 | 25 | 198 | 0 | 0 |

## Notes

- `entry_quality.csv` contains one row per `listing_key` and is the complete per-entry report.
- `notice` marks soft alias-review hints; it is not a structural row warning.
- `source_gap` means the row is structurally valid but lacks stronger source or metadata coverage.
- `quarantine` means deterministic checks found a hard contradiction that should be fixed before treating the row as high quality.

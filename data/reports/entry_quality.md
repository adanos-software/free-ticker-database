# Entry Quality Report

Generated at: `2026-05-03T20:01:22Z`

## Status Counts

| Status | Rows |
|---|---:|
| pass | 54,333 |
| source_gap | 16,696 |
| warn | 63 |

## Issue Counts

| Issue | Rows |
|---|---:|
| missing_stock_sector | 7,874 |
| official_reference_gap | 4,758 |
| expected_missing_primary_isin | 4,578 |
| missing_etf_category | 2,433 |
| country_isin_mismatch | 63 |

## Top Flagged Exchanges

| Exchange | Pass | Notice | Source Gap | Warn | Quarantine |
|---|---:|---:|---:|---:|---:|
| OTC | 7,887 | 0 | 3,169 | 0 | 0 |
| BSE_IN | 7 | 0 | 2,633 | 0 | 0 |
| HKEX | 1,905 | 0 | 1,139 | 0 | 0 |
| TSX | 1,189 | 0 | 714 | 0 | 0 |
| SSE | 2,149 | 0 | 640 | 0 | 0 |
| TSXV | 464 | 0 | 602 | 0 | 0 |
| SGX | 10 | 0 | 584 | 0 | 0 |
| B3 | 1,070 | 0 | 514 | 0 | 0 |
| NYSE ARCA | 2,152 | 0 | 502 | 0 | 0 |
| SZSE | 2,583 | 0 | 499 | 1 | 0 |
| KRX | 1,313 | 0 | 483 | 0 | 0 |
| LSE | 5,951 | 0 | 404 | 60 | 0 |
| NASDAQ | 4,267 | 0 | 366 | 1 | 0 |
| CSE_LK | 0 | 0 | 307 | 0 | 0 |
| NSE_IN | 990 | 0 | 244 | 0 | 0 |
| STO | 492 | 0 | 233 | 0 | 0 |
| BATS | 1,013 | 0 | 230 | 0 | 0 |
| ASX | 1,096 | 0 | 202 | 0 | 0 |
| SET | 349 | 0 | 198 | 0 | 0 |
| XETRA | 3,607 | 0 | 172 | 0 | 0 |

## Notes

- `entry_quality.csv` contains one row per `listing_key` and is the complete per-entry report.
- `notice` marks soft alias-review hints; it is not a structural row warning.
- `source_gap` means the row is structurally valid but lacks stronger source or metadata coverage.
- `quarantine` means deterministic checks found a hard contradiction that should be fixed before treating the row as high quality.

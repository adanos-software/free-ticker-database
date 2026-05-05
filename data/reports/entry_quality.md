# Entry Quality Report

Generated at: `2026-05-05T06:25:07Z`

## Status Counts

| Status | Rows |
|---|---:|
| pass | 59,188 |
| source_gap | 11,841 |
| warn | 63 |

## Issue Counts

| Issue | Rows |
|---|---:|
| official_reference_gap | 5,321 |
| expected_missing_primary_isin | 3,917 |
| missing_stock_sector | 3,762 |
| missing_etf_category | 435 |
| country_isin_mismatch | 63 |

## Top Flagged Exchanges

| Exchange | Pass | Notice | Source Gap | Warn | Quarantine |
|---|---:|---:|---:|---:|---:|
| OTC | 7,887 | 0 | 3,169 | 0 | 0 |
| B3 | 778 | 0 | 806 | 0 | 0 |
| TSX | 1,220 | 0 | 683 | 0 | 0 |
| SSE | 2,173 | 0 | 616 | 0 | 0 |
| TSXV | 488 | 0 | 578 | 0 | 0 |
| SZSE | 2,593 | 0 | 489 | 1 | 0 |
| LSE | 5,975 | 0 | 380 | 60 | 0 |
| NYSE ARCA | 2,283 | 0 | 371 | 0 | 0 |
| SGX | 325 | 0 | 269 | 0 | 0 |
| NASDAQ | 4,369 | 0 | 264 | 1 | 0 |
| NSE_IN | 990 | 0 | 244 | 0 | 0 |
| STO | 492 | 0 | 233 | 0 | 0 |
| SET | 351 | 0 | 196 | 0 | 0 |
| XETRA | 3,608 | 0 | 171 | 0 | 0 |
| BMV | 8 | 0 | 171 | 0 | 0 |
| BATS | 1,079 | 0 | 164 | 0 | 0 |
| TADAWUL | 28 | 0 | 163 | 0 | 0 |
| ASX | 1,146 | 0 | 152 | 0 | 0 |
| Euronext | 828 | 0 | 147 | 0 | 0 |
| CSE_LK | 164 | 0 | 143 | 0 | 0 |

## Notes

- `entry_quality.csv` contains one row per `listing_key` and is the complete per-entry report.
- `notice` marks soft alias-review hints; it is not a structural row warning.
- `source_gap` means the row is structurally valid but lacks stronger source or metadata coverage.
- `quarantine` means deterministic checks found a hard contradiction that should be fixed before treating the row as high quality.

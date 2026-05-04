# Entry Quality Report

Generated at: `2026-05-04T08:39:45Z`

## Status Counts

| Status | Rows |
|---|---:|
| pass | 55,060 |
| source_gap | 15,969 |
| warn | 63 |

## Issue Counts

| Issue | Rows |
|---|---:|
| missing_stock_sector | 7,842 |
| official_reference_gap | 5,321 |
| expected_missing_primary_isin | 3,941 |
| missing_etf_category | 570 |
| country_isin_mismatch | 63 |

## Top Flagged Exchanges

| Exchange | Pass | Notice | Source Gap | Warn | Quarantine |
|---|---:|---:|---:|---:|---:|
| OTC | 7,887 | 0 | 3,169 | 0 | 0 |
| BSE_IN | 7 | 0 | 2,633 | 0 | 0 |
| HKEX | 1,905 | 0 | 1,139 | 0 | 0 |
| B3 | 760 | 0 | 824 | 0 | 0 |
| TSX | 1,211 | 0 | 692 | 0 | 0 |
| SSE | 2,173 | 0 | 616 | 0 | 0 |
| TSXV | 487 | 0 | 579 | 0 | 0 |
| SZSE | 2,593 | 0 | 489 | 1 | 0 |
| SGX | 124 | 0 | 470 | 0 | 0 |
| LSE | 5,974 | 0 | 381 | 60 | 0 |
| NYSE ARCA | 2,265 | 0 | 389 | 0 | 0 |
| CSE_LK | 0 | 0 | 307 | 0 | 0 |
| NASDAQ | 4,331 | 0 | 302 | 1 | 0 |
| NSE_IN | 990 | 0 | 244 | 0 | 0 |
| STO | 492 | 0 | 233 | 0 | 0 |
| SET | 350 | 0 | 197 | 0 | 0 |
| BATS | 1,064 | 0 | 179 | 0 | 0 |
| XETRA | 3,608 | 0 | 171 | 0 | 0 |
| TADAWUL | 20 | 0 | 171 | 0 | 0 |
| BMV | 8 | 0 | 171 | 0 | 0 |

## Notes

- `entry_quality.csv` contains one row per `listing_key` and is the complete per-entry report.
- `notice` marks soft alias-review hints; it is not a structural row warning.
- `source_gap` means the row is structurally valid but lacks stronger source or metadata coverage.
- `quarantine` means deterministic checks found a hard contradiction that should be fixed before treating the row as high quality.

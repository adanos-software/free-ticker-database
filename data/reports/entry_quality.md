# Entry Quality Report

Generated at: `2026-04-17T08:15:57Z`

## Status Counts

| Status | Rows |
|---|---:|
| notice | 5,497 |
| pass | 44,680 |
| source_gap | 12,056 |
| warn | 263 |

## Issue Counts

| Issue | Rows |
|---|---:|
| official_reference_gap | 4,449 |
| shared_name_alias | 4,004 |
| missing_etf_category | 4,002 |
| expected_missing_primary_isin | 3,910 |
| missing_stock_sector | 3,164 |
| low_company_name_overlap | 1,982 |
| official_name_mismatch | 263 |

## Top Flagged Exchanges

| Exchange | Pass | Notice | Source Gap | Warn | Quarantine |
|---|---:|---:|---:|---:|---:|
| OTC | 6,527 | 1,615 | 2,688 | 263 | 0 |
| B3 | 473 | 138 | 926 | 0 | 0 |
| TSX | 1,054 | 148 | 693 | 0 | 0 |
| SSE | 2,089 | 42 | 658 | 0 | 0 |
| NYSE ARCA | 1,803 | 278 | 570 | 0 | 0 |
| LSE | 5,092 | 775 | 548 | 0 | 0 |
| KRX | 1,215 | 51 | 530 | 0 | 0 |
| SZSE | 2,523 | 58 | 502 | 0 | 0 |
| NSE_IN | 0 | 2 | 481 | 0 | 0 |
| TSXV | 355 | 260 | 451 | 0 | 0 |
| NASDAQ | 3,646 | 578 | 410 | 0 | 0 |
| XETRA | 3,242 | 159 | 351 | 0 | 0 |
| BATS | 890 | 49 | 308 | 0 | 0 |
| STO | 401 | 94 | 230 | 0 | 0 |
| ASX | 974 | 117 | 207 | 0 | 0 |
| SET | 325 | 25 | 197 | 0 | 0 |
| TWSE | 1,072 | 2 | 168 | 0 | 0 |
| Euronext | 742 | 81 | 152 | 0 | 0 |
| BMV | 8 | 36 | 135 | 0 | 0 |
| TASE | 529 | 14 | 130 | 0 | 0 |

## Notes

- `entry_quality.csv` contains one row per `listing_key` and is the complete per-entry report.
- `notice` marks soft alias-review hints; it is not a structural row warning.
- `source_gap` means the row is structurally valid but lacks stronger source or metadata coverage.
- `quarantine` means deterministic checks found a hard contradiction that should be fixed before treating the row as high quality.

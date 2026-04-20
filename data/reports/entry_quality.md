# Entry Quality Report

Generated at: `2026-04-20T17:46:51Z`

## Status Counts

| Status | Rows |
|---|---:|
| pass | 48,905 |
| source_gap | 12,729 |
| warn | 862 |

## Issue Counts

| Issue | Rows |
|---|---:|
| official_reference_gap | 4,453 |
| missing_etf_category | 3,960 |
| expected_missing_primary_isin | 3,920 |
| missing_stock_sector | 3,160 |
| official_name_mismatch | 862 |

## Top Flagged Exchanges

| Exchange | Pass | Notice | Source Gap | Warn | Quarantine |
|---|---:|---:|---:|---:|---:|
| OTC | 7,591 | 0 | 2,866 | 636 | 0 |
| B3 | 618 | 0 | 919 | 0 | 0 |
| TSX | 1,125 | 0 | 767 | 3 | 0 |
| NYSE ARCA | 1,954 | 0 | 613 | 87 | 0 |
| SSE | 2,131 | 0 | 658 | 0 | 0 |
| TSXV | 452 | 0 | 595 | 19 | 0 |
| LSE | 5,823 | 0 | 591 | 1 | 0 |
| KRX | 1,266 | 0 | 530 | 0 | 0 |
| SZSE | 2,581 | 0 | 502 | 0 | 0 |
| NSE_IN | 0 | 0 | 483 | 0 | 0 |
| NASDAQ | 4,159 | 0 | 447 | 28 | 0 |
| XETRA | 3,403 | 0 | 349 | 0 | 0 |
| BATS | 919 | 0 | 320 | 4 | 0 |
| STO | 488 | 0 | 237 | 0 | 0 |
| ASX | 1,082 | 0 | 212 | 4 | 0 |
| SET | 341 | 0 | 184 | 22 | 0 |
| BMV | 8 | 0 | 171 | 0 | 0 |
| TWSE | 1,072 | 0 | 170 | 0 | 0 |
| Euronext | 816 | 0 | 159 | 0 | 0 |
| NYSE | 1,935 | 0 | 118 | 28 | 0 |

## Notes

- `entry_quality.csv` contains one row per `listing_key` and is the complete per-entry report.
- `notice` marks soft alias-review hints; it is not a structural row warning.
- `source_gap` means the row is structurally valid but lacks stronger source or metadata coverage.
- `quarantine` means deterministic checks found a hard contradiction that should be fixed before treating the row as high quality.

# Entry Quality Report

Generated at: `2026-05-04T07:20:39Z`

## Status Counts

| Status | Rows |
|---|---:|
| pass | 55,174 |
| source_gap | 15,855 |
| warn | 63 |

## Issue Counts

| Issue | Rows |
|---|---:|
| missing_stock_sector | 7,874 |
| official_reference_gap | 4,760 |
| expected_missing_primary_isin | 4,535 |
| missing_etf_category | 876 |
| country_isin_mismatch | 63 |

## Top Flagged Exchanges

| Exchange | Pass | Notice | Source Gap | Warn | Quarantine |
|---|---:|---:|---:|---:|---:|
| OTC | 7,887 | 0 | 3,169 | 0 | 0 |
| BSE_IN | 7 | 0 | 2,633 | 0 | 0 |
| HKEX | 1,905 | 0 | 1,139 | 0 | 0 |
| TSX | 1,206 | 0 | 697 | 0 | 0 |
| SSE | 2,174 | 0 | 615 | 0 | 0 |
| SGX | 10 | 0 | 584 | 0 | 0 |
| TSXV | 496 | 0 | 570 | 0 | 0 |
| SZSE | 2,594 | 0 | 488 | 1 | 0 |
| LSE | 5,956 | 0 | 399 | 60 | 0 |
| NYSE ARCA | 2,210 | 0 | 444 | 0 | 0 |
| B3 | 1,197 | 0 | 387 | 0 | 0 |
| NASDAQ | 4,302 | 0 | 331 | 1 | 0 |
| CSE_LK | 0 | 0 | 307 | 0 | 0 |
| NSE_IN | 990 | 0 | 244 | 0 | 0 |
| STO | 492 | 0 | 233 | 0 | 0 |
| BATS | 1,033 | 0 | 210 | 0 | 0 |
| SET | 350 | 0 | 197 | 0 | 0 |
| ASX | 1,109 | 0 | 189 | 0 | 0 |
| XETRA | 3,608 | 0 | 171 | 0 | 0 |
| TADAWUL | 20 | 0 | 171 | 0 | 0 |

## Notes

- `entry_quality.csv` contains one row per `listing_key` and is the complete per-entry report.
- `notice` marks soft alias-review hints; it is not a structural row warning.
- `source_gap` means the row is structurally valid but lacks stronger source or metadata coverage.
- `quarantine` means deterministic checks found a hard contradiction that should be fixed before treating the row as high quality.

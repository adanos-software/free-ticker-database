# Entry Quality Report

Generated at: `2026-06-16T11:25:10Z`

## Status Counts

| Status | Rows |
|---|---:|
| pass | 64,982 |
| source_gap | 5,983 |
| warn | 70 |

## Issue Counts

| Issue | Rows |
|---|---:|
| official_reference_gap | 5,309 |
| expected_missing_primary_isin | 763 |
| missing_etf_category | 74 |
| country_isin_mismatch | 65 |
| missing_stock_sector | 23 |
| venue_missing_official_source | 8 |
| official_name_mismatch | 5 |

## Top Flagged Exchanges

| Exchange | Pass | Notice | Source Gap | Warn | Quarantine |
|---|---:|---:|---:|---:|---:|
| OTC | 7,865 | 0 | 3,183 | 5 | 0 |
| B3 | 1,262 | 0 | 322 | 0 | 0 |
| TSX | 1,704 | 0 | 200 | 0 | 0 |
| NASDAQ | 4,451 | 0 | 183 | 1 | 0 |
| BSE_IN | 2,459 | 0 | 183 | 0 | 0 |
| BMV | 9 | 0 | 170 | 0 | 0 |
| LSE | 6,257 | 0 | 98 | 60 | 0 |
| NYSE ARCA | 2,506 | 0 | 147 | 0 | 0 |
| ASX | 1,160 | 0 | 137 | 0 | 0 |
| BME | 72 | 0 | 97 | 0 | 0 |
| TSXV | 970 | 0 | 94 | 2 | 0 |
| JSE | 124 | 0 | 88 | 0 | 0 |
| BATS | 1,156 | 0 | 85 | 0 | 0 |
| NYSE | 2,002 | 0 | 78 | 0 | 0 |
| AMS | 237 | 0 | 77 | 0 | 0 |
| TASE | 596 | 0 | 77 | 0 | 0 |
| XETRA | 3,712 | 0 | 67 | 0 | 0 |
| CSE_MA | 1 | 0 | 65 | 0 | 0 |
| NEO | 146 | 0 | 51 | 0 | 0 |
| SSE | 2,748 | 0 | 41 | 0 | 0 |

## Notes

- `entry_quality.csv` contains one row per `listing_key` and is the complete per-entry report.
- `notice` marks soft alias-review hints; it is not a structural row warning.
- `source_gap` means the row is structurally valid but lacks stronger source or metadata coverage.
- `quarantine` means deterministic checks found a hard contradiction that should be fixed before treating the row as high quality.

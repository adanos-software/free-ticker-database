# Entry Quality Report

Generated at: `2026-05-11T05:47:48Z`

## Status Counts

| Status | Rows |
|---|---:|
| pass | 64,061 |
| source_gap | 6,917 |
| warn | 63 |

## Issue Counts

| Issue | Rows |
|---|---:|
| official_reference_gap | 4,780 |
| missing_stock_sector | 1,867 |
| expected_missing_primary_isin | 1,099 |
| missing_etf_category | 77 |
| country_isin_mismatch | 63 |

## Top Flagged Exchanges

| Exchange | Pass | Notice | Source Gap | Warn | Quarantine |
|---|---:|---:|---:|---:|---:|
| OTC | 7,887 | 0 | 3,169 | 0 | 0 |
| B3 | 1,265 | 0 | 319 | 0 | 0 |
| LSE | 6,181 | 0 | 174 | 60 | 0 |
| TSX | 1,693 | 0 | 210 | 0 | 0 |
| BMV | 8 | 0 | 171 | 0 | 0 |
| TSXV | 901 | 0 | 165 | 0 | 0 |
| Euronext | 828 | 0 | 147 | 0 | 0 |
| CSE_LK | 164 | 0 | 143 | 0 | 0 |
| ASX | 1,157 | 0 | 141 | 0 | 0 |
| NYSE ARCA | 2,525 | 0 | 129 | 0 | 0 |
| NASDAQ | 4,523 | 0 | 110 | 1 | 0 |
| BK | 2 | 0 | 102 | 0 | 0 |
| AMS | 219 | 0 | 95 | 0 | 0 |
| BME | 75 | 0 | 94 | 0 | 0 |
| MSX | 1 | 0 | 90 | 0 | 0 |
| XETRA | 3,691 | 0 | 88 | 0 | 0 |
| JSE | 130 | 0 | 82 | 0 | 0 |
| PSE | 14 | 0 | 76 | 0 | 0 |
| BATS | 1,169 | 0 | 74 | 0 | 0 |
| TASE | 599 | 0 | 74 | 0 | 0 |

## Notes

- `entry_quality.csv` contains one row per `listing_key` and is the complete per-entry report.
- `notice` marks soft alias-review hints; it is not a structural row warning.
- `source_gap` means the row is structurally valid but lacks stronger source or metadata coverage.
- `quarantine` means deterministic checks found a hard contradiction that should be fixed before treating the row as high quality.

# Entry Quality Report

Generated at: `2026-05-16T17:23:09Z`

## Status Counts

| Status | Rows |
|---|---:|
| pass | 62,386 |
| source_gap | 8,440 |
| warn | 217 |

## Issue Counts

| Issue | Rows |
|---|---:|
| official_reference_gap | 6,526 |
| missing_stock_sector | 1,781 |
| expected_missing_primary_isin | 1,059 |
| official_name_mismatch | 154 |
| missing_etf_category | 77 |
| country_isin_mismatch | 63 |
| venue_missing_official_source | 8 |

## Top Flagged Exchanges

| Exchange | Pass | Notice | Source Gap | Warn | Quarantine |
|---|---:|---:|---:|---:|---:|
| OTC | 7,813 | 0 | 3,097 | 146 | 0 |
| B3 | 378 | 0 | 1,206 | 0 | 0 |
| SSE | 2,229 | 0 | 560 | 0 | 0 |
| LSE | 6,149 | 0 | 206 | 60 | 0 |
| TSXV | 836 | 0 | 230 | 0 | 0 |
| TSX | 1,691 | 0 | 213 | 0 | 0 |
| BSE_IN | 2,438 | 0 | 204 | 0 | 0 |
| NASDAQ | 4,485 | 0 | 145 | 5 | 0 |
| Euronext | 826 | 0 | 149 | 0 | 0 |
| CSE_LK | 164 | 0 | 143 | 0 | 0 |
| ASX | 1,156 | 0 | 141 | 1 | 0 |
| NYSE ARCA | 2,517 | 0 | 136 | 0 | 0 |
| TASE | 564 | 0 | 109 | 0 | 0 |
| BME | 67 | 0 | 102 | 0 | 0 |
| BK | 2 | 0 | 102 | 0 | 0 |
| XETRA | 3,682 | 0 | 97 | 0 | 0 |
| AMS | 218 | 0 | 96 | 0 | 0 |
| MSX | 1 | 0 | 90 | 0 | 0 |
| JSE | 124 | 0 | 88 | 0 | 0 |
| BATS | 1,158 | 0 | 83 | 0 | 0 |

## Notes

- `entry_quality.csv` contains one row per `listing_key` and is the complete per-entry report.
- `notice` marks soft alias-review hints; it is not a structural row warning.
- `source_gap` means the row is structurally valid but lacks stronger source or metadata coverage.
- `quarantine` means deterministic checks found a hard contradiction that should be fixed before treating the row as high quality.

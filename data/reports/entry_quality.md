# Entry Quality Report

Generated at: `2026-05-31T04:00:45Z`

## Status Counts

| Status | Rows |
|---|---:|
| pass | 63,385 |
| source_gap | 7,436 |
| warn | 222 |

## Issue Counts

| Issue | Rows |
|---|---:|
| official_reference_gap | 5,331 |
| missing_stock_sector | 1,783 |
| expected_missing_primary_isin | 846 |
| official_name_mismatch | 159 |
| missing_etf_category | 74 |
| country_isin_mismatch | 63 |
| venue_missing_official_source | 8 |

## Top Flagged Exchanges

| Exchange | Pass | Notice | Source Gap | Warn | Quarantine |
|---|---:|---:|---:|---:|---:|
| OTC | 7,757 | 0 | 3,149 | 148 | 0 |
| SSE | 2,229 | 0 | 560 | 0 | 0 |
| LSE | 6,148 | 0 | 207 | 60 | 0 |
| TSXV | 836 | 0 | 230 | 0 | 0 |
| TSX | 1,691 | 0 | 213 | 0 | 0 |
| B3 | 1,372 | 0 | 212 | 0 | 0 |
| NASDAQ | 4,446 | 0 | 184 | 7 | 0 |
| Euronext | 825 | 0 | 150 | 0 | 0 |
| NYSE ARCA | 2,508 | 0 | 145 | 0 | 0 |
| CSE_LK | 164 | 0 | 143 | 0 | 0 |
| ASX | 1,156 | 0 | 141 | 1 | 0 |
| TASE | 564 | 0 | 109 | 0 | 0 |
| XETRA | 3,675 | 0 | 104 | 0 | 0 |
| BME | 67 | 0 | 102 | 0 | 0 |
| BK | 2 | 0 | 102 | 0 | 0 |
| AMS | 218 | 0 | 96 | 0 | 0 |
| MSX | 1 | 0 | 90 | 0 | 0 |
| JSE | 124 | 0 | 88 | 0 | 0 |
| NYSE | 1,996 | 0 | 84 | 2 | 0 |
| BATS | 1,158 | 0 | 83 | 0 | 0 |

## Notes

- `entry_quality.csv` contains one row per `listing_key` and is the complete per-entry report.
- `notice` marks soft alias-review hints; it is not a structural row warning.
- `source_gap` means the row is structurally valid but lacks stronger source or metadata coverage.
- `quarantine` means deterministic checks found a hard contradiction that should be fixed before treating the row as high quality.

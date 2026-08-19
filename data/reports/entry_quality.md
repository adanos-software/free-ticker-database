# Entry Quality Report

Generated at: `2026-08-19T08:50:34Z`

## Status Counts

| Status | Rows |
|---|---:|
| pass | 80,226 |
| source_gap | 11,555 |
| warn | 199 |

## Issue Counts

| Issue | Rows |
|---|---:|
| official_reference_gap | 6,611 |
| venue_missing_official_source | 3,287 |
| missing_stock_sector | 1,299 |
| expected_missing_primary_isin | 674 |
| country_isin_mismatch | 95 |
| official_isin_mismatch | 56 |
| official_name_mismatch | 50 |
| missing_etf_category | 12 |

## Top Flagged Exchanges

| Exchange | Pass | Notice | Source Gap | Warn | Quarantine |
|---|---:|---:|---:|---:|---:|
| OTC | 8,390 | 0 | 3,255 | 108 | 0 |
| XSTU | 0 | 0 | 2,772 | 1 | 0 |
| FSX | 7,151 | 0 | 990 | 2 | 0 |
| BMV | 0 | 0 | 344 | 0 | 0 |
| B3 | 1,250 | 0 | 331 | 0 | 0 |
| NASDAQ | 4,512 | 0 | 247 | 8 | 0 |
| Munich | 0 | 0 | 223 | 0 | 0 |
| NSE_IN | 2,290 | 0 | 213 | 0 | 0 |
| TSXV | 1,216 | 0 | 203 | 3 | 0 |
| XDUS | 0 | 0 | 199 | 0 | 0 |
| NYSE ARCA | 2,540 | 0 | 181 | 1 | 0 |
| TSX | 2,119 | 0 | 177 | 0 | 0 |
| LSE | 6,854 | 0 | 151 | 25 | 0 |
| AMS | 371 | 0 | 173 | 2 | 0 |
| BME | 127 | 0 | 146 | 3 | 0 |
| Euronext | 1,329 | 0 | 127 | 21 | 0 |
| ASX | 2,111 | 0 | 148 | 0 | 0 |
| XETRA | 4,182 | 0 | 130 | 3 | 0 |
| BATS | 1,226 | 0 | 114 | 0 | 0 |
| NYSE | 1,929 | 0 | 85 | 6 | 0 |

## Notes

- `entry_quality.csv` contains one row per `listing_key` and is the complete per-entry report.
- `notice` marks soft alias-review hints; it is not a structural row warning.
- `source_gap` means the row is structurally valid but lacks stronger source or metadata coverage.
- `quarantine` means deterministic checks found a hard contradiction that should be fixed before treating the row as high quality.

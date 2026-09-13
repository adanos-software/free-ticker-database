# Entry Quality Report

Generated at: `2026-09-13T11:50:34Z`

## Status Counts

| Status | Rows |
|---|---:|
| pass | 80,527 |
| source_gap | 11,502 |
| warn | 33 |

## Issue Counts

| Issue | Rows |
|---|---:|
| official_reference_gap | 6,381 |
| venue_missing_official_source | 3,287 |
| missing_stock_sector | 1,296 |
| expected_missing_primary_isin | 819 |
| missing_etf_category | 76 |
| official_name_mismatch | 26 |
| official_isin_mismatch | 8 |

## Top Flagged Exchanges

| Exchange | Pass | Notice | Source Gap | Warn | Quarantine |
|---|---:|---:|---:|---:|---:|
| OTC | 8,481 | 0 | 3,258 | 13 | 0 |
| XSTU | 0 | 0 | 2,773 | 0 | 0 |
| FSX | 7,149 | 0 | 994 | 0 | 0 |
| B3 | 1,243 | 0 | 338 | 0 | 0 |
| NASDAQ | 4,456 | 0 | 297 | 4 | 0 |
| BMV | 77 | 0 | 267 | 0 | 0 |
| NYSE ARCA | 2,516 | 0 | 251 | 2 | 0 |
| Munich | 0 | 0 | 223 | 0 | 0 |
| BSE_IN | 2,525 | 0 | 206 | 1 | 0 |
| XDUS | 0 | 0 | 199 | 0 | 0 |
| AMS | 372 | 0 | 174 | 0 | 0 |
| TSX | 2,122 | 0 | 174 | 0 | 0 |
| BATS | 1,211 | 0 | 171 | 0 | 0 |
| LSE | 6,875 | 0 | 153 | 2 | 0 |
| XETRA | 4,165 | 0 | 150 | 0 | 0 |
| ASX | 2,109 | 0 | 150 | 0 | 0 |
| TSXV | 1,283 | 0 | 137 | 2 | 0 |
| Euronext | 1,343 | 0 | 132 | 2 | 0 |
| NYSE | 1,913 | 0 | 109 | 4 | 0 |
| JSE | 123 | 0 | 89 | 0 | 0 |

## Notes

- `entry_quality.csv` contains one row per `listing_key` and is the complete per-entry report.
- `notice` marks soft alias-review hints; it is not a structural row warning.
- `source_gap` means the row is structurally valid but lacks stronger source or metadata coverage.
- `quarantine` means deterministic checks found a hard contradiction that should be fixed before treating the row as high quality.

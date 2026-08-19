# Entry Quality Report

Generated at: `2026-08-19T16:21:12Z`

## Status Counts

| Status | Rows |
|---|---:|
| pass | 80,252 |
| source_gap | 11,574 |
| warn | 180 |

## Issue Counts

| Issue | Rows |
|---|---:|
| official_reference_gap | 6,577 |
| venue_missing_official_source | 3,287 |
| missing_stock_sector | 1,296 |
| expected_missing_primary_isin | 690 |
| country_isin_mismatch | 95 |
| official_isin_mismatch | 49 |
| official_name_mismatch | 38 |
| missing_etf_category | 36 |

## Top Flagged Exchanges

| Exchange | Pass | Notice | Source Gap | Warn | Quarantine |
|---|---:|---:|---:|---:|---:|
| OTC | 8,403 | 0 | 3,255 | 95 | 0 |
| XSTU | 0 | 0 | 2,772 | 1 | 0 |
| FSX | 7,155 | 0 | 986 | 2 | 0 |
| B3 | 1,241 | 0 | 340 | 0 | 0 |
| NASDAQ | 4,493 | 0 | 267 | 7 | 0 |
| BMV | 76 | 0 | 268 | 0 | 0 |
| Munich | 0 | 0 | 223 | 0 | 0 |
| NYSE ARCA | 2,526 | 0 | 201 | 2 | 0 |
| XDUS | 0 | 0 | 199 | 0 | 0 |
| BSE_IN | 2,535 | 0 | 197 | 0 | 0 |
| AMS | 372 | 0 | 173 | 1 | 0 |
| TSX | 2,122 | 0 | 174 | 0 | 0 |
| LSE | 6,858 | 0 | 151 | 21 | 0 |
| NSE_IN | 2,331 | 0 | 172 | 0 | 0 |
| ASX | 2,108 | 0 | 151 | 0 | 0 |
| BME | 127 | 0 | 146 | 3 | 0 |
| Euronext | 1,330 | 0 | 127 | 20 | 0 |
| XETRA | 4,170 | 0 | 143 | 2 | 0 |
| TSXV | 1,283 | 0 | 136 | 3 | 0 |
| BATS | 1,226 | 0 | 131 | 0 | 0 |

## Notes

- `entry_quality.csv` contains one row per `listing_key` and is the complete per-entry report.
- `notice` marks soft alias-review hints; it is not a structural row warning.
- `source_gap` means the row is structurally valid but lacks stronger source or metadata coverage.
- `quarantine` means deterministic checks found a hard contradiction that should be fixed before treating the row as high quality.

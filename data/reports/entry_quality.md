# Entry Quality Report

Generated at: `2026-08-18T20:23:31Z`

## Status Counts

| Status | Rows |
|---|---:|
| pass | 78,646 |
| source_gap | 13,133 |
| warn | 198 |

## Issue Counts

| Issue | Rows |
|---|---:|
| official_reference_gap | 6,582 |
| venue_missing_official_source | 3,287 |
| missing_stock_sector | 2,232 |
| expected_missing_primary_isin | 874 |
| missing_etf_category | 663 |
| country_isin_mismatch | 95 |
| official_isin_mismatch | 56 |
| official_name_mismatch | 49 |

## Top Flagged Exchanges

| Exchange | Pass | Notice | Source Gap | Warn | Quarantine |
|---|---:|---:|---:|---:|---:|
| OTC | 8,390 | 0 | 3,255 | 108 | 0 |
| XSTU | 0 | 0 | 2,772 | 1 | 0 |
| FSX | 7,137 | 0 | 1,004 | 2 | 0 |
| TSXV | 916 | 0 | 503 | 3 | 0 |
| TSX | 1,830 | 0 | 466 | 0 | 0 |
| ASX | 1,872 | 0 | 387 | 0 | 0 |
| NASDAQ | 4,378 | 0 | 378 | 8 | 0 |
| LSE | 6,667 | 0 | 339 | 24 | 0 |
| BMV | 0 | 0 | 344 | 0 | 0 |
| B3 | 1,250 | 0 | 331 | 0 | 0 |
| NYSE ARCA | 2,482 | 0 | 239 | 1 | 0 |
| Munich | 0 | 0 | 223 | 0 | 0 |
| NSE_IN | 2,290 | 0 | 213 | 0 | 0 |
| XDUS | 0 | 0 | 199 | 0 | 0 |
| Euronext | 1,295 | 0 | 161 | 21 | 0 |
| TASE | 625 | 0 | 175 | 1 | 0 |
| AMS | 371 | 0 | 173 | 2 | 0 |
| XETRA | 4,141 | 0 | 171 | 3 | 0 |
| BSE_IN | 2,580 | 0 | 152 | 0 | 0 |
| BME | 127 | 0 | 146 | 3 | 0 |

## Notes

- `entry_quality.csv` contains one row per `listing_key` and is the complete per-entry report.
- `notice` marks soft alias-review hints; it is not a structural row warning.
- `source_gap` means the row is structurally valid but lacks stronger source or metadata coverage.
- `quarantine` means deterministic checks found a hard contradiction that should be fixed before treating the row as high quality.

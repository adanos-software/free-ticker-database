# Entry Quality Report

Generated at: `2026-06-15T05:23:19Z`

## Status Counts

| Status | Rows |
|---|---:|
| pass | 63,710 |
| source_gap | 7,261 |
| warn | 70 |

## Issue Counts

| Issue | Rows |
|---|---:|
| official_reference_gap | 5,322 |
| missing_stock_sector | 1,763 |
| expected_missing_primary_isin | 855 |
| missing_etf_category | 74 |
| country_isin_mismatch | 65 |
| venue_missing_official_source | 8 |
| official_name_mismatch | 5 |

## Top Flagged Exchanges

| Exchange | Pass | Notice | Source Gap | Warn | Quarantine |
|---|---:|---:|---:|---:|---:|
| OTC | 7,866 | 0 | 3,183 | 5 | 0 |
| B3 | 1,260 | 0 | 324 | 0 | 0 |
| LSE | 6,149 | 0 | 206 | 60 | 0 |
| BSE_IN | 2,435 | 0 | 207 | 0 | 0 |
| TSX | 1,698 | 0 | 206 | 0 | 0 |
| NASDAQ | 4,437 | 0 | 198 | 1 | 0 |
| BMV | 8 | 0 | 171 | 0 | 0 |
| TSXV | 908 | 0 | 156 | 2 | 0 |
| Euronext | 827 | 0 | 148 | 0 | 0 |
| NYSE ARCA | 2,506 | 0 | 147 | 0 | 0 |
| ASX | 1,155 | 0 | 143 | 0 | 0 |
| CSE_LK | 164 | 0 | 143 | 0 | 0 |
| XETRA | 3,673 | 0 | 106 | 0 | 0 |
| BK | 2 | 0 | 102 | 0 | 0 |
| AMS | 217 | 0 | 97 | 0 | 0 |
| BME | 72 | 0 | 97 | 0 | 0 |
| MSX | 1 | 0 | 90 | 0 | 0 |
| JSE | 124 | 0 | 88 | 0 | 0 |
| NYSE | 1,995 | 0 | 88 | 0 | 0 |
| BATS | 1,156 | 0 | 85 | 0 | 0 |

## Notes

- `entry_quality.csv` contains one row per `listing_key` and is the complete per-entry report.
- `notice` marks soft alias-review hints; it is not a structural row warning.
- `source_gap` means the row is structurally valid but lacks stronger source or metadata coverage.
- `quarantine` means deterministic checks found a hard contradiction that should be fixed before treating the row as high quality.

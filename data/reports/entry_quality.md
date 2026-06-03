# Entry Quality Report

Generated at: `2026-06-03T14:38:14Z`

## Status Counts

| Status | Rows |
|---|---:|
| pass | 63,700 |
| source_gap | 7,270 |
| warn | 73 |

## Issue Counts

| Issue | Rows |
|---|---:|
| official_reference_gap | 5,322 |
| missing_stock_sector | 1,783 |
| expected_missing_primary_isin | 846 |
| missing_etf_category | 74 |
| country_isin_mismatch | 63 |
| official_name_mismatch | 10 |
| venue_missing_official_source | 8 |

## Top Flagged Exchanges

| Exchange | Pass | Notice | Source Gap | Warn | Quarantine |
|---|---:|---:|---:|---:|---:|
| OTC | 7,861 | 0 | 3,183 | 10 | 0 |
| B3 | 1,259 | 0 | 325 | 0 | 0 |
| LSE | 6,148 | 0 | 207 | 60 | 0 |
| TSX | 1,694 | 0 | 210 | 0 | 0 |
| BSE_IN | 2,433 | 0 | 209 | 0 | 0 |
| NASDAQ | 4,449 | 0 | 187 | 1 | 0 |
| BMV | 8 | 0 | 171 | 0 | 0 |
| TSXV | 901 | 0 | 165 | 0 | 0 |
| Euronext | 825 | 0 | 150 | 0 | 0 |
| NYSE ARCA | 2,508 | 0 | 145 | 0 | 0 |
| ASX | 1,155 | 0 | 143 | 0 | 0 |
| CSE_LK | 164 | 0 | 143 | 0 | 0 |
| XETRA | 3,673 | 0 | 106 | 0 | 0 |
| BK | 2 | 0 | 102 | 0 | 0 |
| AMS | 217 | 0 | 97 | 0 | 0 |
| BME | 72 | 0 | 97 | 0 | 0 |
| MSX | 1 | 0 | 90 | 0 | 0 |
| JSE | 124 | 0 | 88 | 0 | 0 |
| NYSE | 1,999 | 0 | 84 | 0 | 0 |
| BATS | 1,158 | 0 | 83 | 0 | 0 |

## Notes

- `entry_quality.csv` contains one row per `listing_key` and is the complete per-entry report.
- `notice` marks soft alias-review hints; it is not a structural row warning.
- `source_gap` means the row is structurally valid but lacks stronger source or metadata coverage.
- `quarantine` means deterministic checks found a hard contradiction that should be fixed before treating the row as high quality.

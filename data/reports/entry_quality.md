# Entry Quality Report

Generated at: `2026-06-10T12:52:30Z`

## Status Counts

| Status | Rows |
|---|---:|
| pass | 63,566 |
| source_gap | 7,411 |
| warn | 66 |

## Issue Counts

| Issue | Rows |
|---|---:|
| official_reference_gap | 5,322 |
| missing_stock_sector | 1,787 |
| expected_missing_primary_isin | 929 |
| missing_etf_category | 146 |
| country_isin_mismatch | 63 |
| venue_missing_official_source | 8 |
| official_name_mismatch | 3 |

## Top Flagged Exchanges

| Exchange | Pass | Notice | Source Gap | Warn | Quarantine |
|---|---:|---:|---:|---:|---:|
| OTC | 7,868 | 0 | 3,183 | 3 | 0 |
| B3 | 1,259 | 0 | 325 | 0 | 0 |
| LSE | 6,099 | 0 | 256 | 60 | 0 |
| TSX | 1,694 | 0 | 210 | 0 | 0 |
| BSE_IN | 2,433 | 0 | 209 | 0 | 0 |
| NASDAQ | 4,429 | 0 | 207 | 1 | 0 |
| NYSE ARCA | 2,479 | 0 | 174 | 0 | 0 |
| BMV | 8 | 0 | 171 | 0 | 0 |
| TSXV | 901 | 0 | 165 | 0 | 0 |
| Euronext | 825 | 0 | 150 | 0 | 0 |
| ASX | 1,155 | 0 | 143 | 0 | 0 |
| CSE_LK | 164 | 0 | 143 | 0 | 0 |
| XETRA | 3,661 | 0 | 118 | 0 | 0 |
| NYSE | 1,975 | 0 | 108 | 0 | 0 |
| BK | 2 | 0 | 102 | 0 | 0 |
| AMS | 217 | 0 | 97 | 0 | 0 |
| BME | 72 | 0 | 97 | 0 | 0 |
| MSX | 1 | 0 | 90 | 0 | 0 |
| JSE | 124 | 0 | 88 | 0 | 0 |
| BATS | 1,154 | 0 | 87 | 0 | 0 |

## Notes

- `entry_quality.csv` contains one row per `listing_key` and is the complete per-entry report.
- `notice` marks soft alias-review hints; it is not a structural row warning.
- `source_gap` means the row is structurally valid but lacks stronger source or metadata coverage.
- `quarantine` means deterministic checks found a hard contradiction that should be fixed before treating the row as high quality.

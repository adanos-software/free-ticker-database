# Entry Quality Report

Generated at: `2026-07-02T11:41:13Z`

## Status Counts

| Status | Rows |
|---|---:|
| pass | 67,633 |
| source_gap | 6,847 |
| warn | 75 |

## Issue Counts

| Issue | Rows |
|---|---:|
| official_reference_gap | 5,674 |
| expected_missing_primary_isin | 945 |
| venue_missing_official_source | 285 |
| missing_etf_category | 88 |
| missing_stock_sector | 72 |
| country_isin_mismatch | 65 |
| official_name_mismatch | 8 |
| official_isin_mismatch | 2 |

## Top Flagged Exchanges

| Exchange | Pass | Notice | Source Gap | Warn | Quarantine |
|---|---:|---:|---:|---:|---:|
| OTC | 7,858 | 0 | 3,174 | 6 | 0 |
| B3 | 1,262 | 0 | 321 | 0 | 0 |
| Borsa Italiana | 0 | 0 | 277 | 0 | 0 |
| NASDAQ | 4,401 | 0 | 274 | 1 | 0 |
| TSX | 1,698 | 0 | 205 | 0 | 0 |
| LSE | 6,363 | 0 | 140 | 60 | 0 |
| BSE_IN | 2,459 | 0 | 179 | 0 | 0 |
| NYSE ARCA | 2,475 | 0 | 179 | 0 | 0 |
| BMV | 11 | 0 | 168 | 0 | 0 |
| ASX | 1,476 | 0 | 152 | 1 | 0 |
| BME | 78 | 0 | 143 | 0 | 0 |
| NSE_IN | 2,369 | 0 | 134 | 0 | 0 |
| NYSE | 1,929 | 0 | 107 | 0 | 0 |
| BATS | 1,137 | 0 | 104 | 0 | 0 |
| XETRA | 3,742 | 0 | 102 | 0 | 0 |
| TSXV | 969 | 0 | 95 | 2 | 0 |
| AMS | 240 | 0 | 90 | 0 | 0 |
| JSE | 124 | 0 | 88 | 0 | 0 |
| Euronext | 1,003 | 0 | 80 | 0 | 0 |
| TASE | 595 | 0 | 77 | 0 | 0 |

## Notes

- `entry_quality.csv` contains one row per `listing_key` and is the complete per-entry report.
- `notice` marks soft alias-review hints; it is not a structural row warning.
- `source_gap` means the row is structurally valid but lacks stronger source or metadata coverage.
- `quarantine` means deterministic checks found a hard contradiction that should be fixed before treating the row as high quality.

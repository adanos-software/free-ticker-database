# Entry Quality Report

Generated at: `2026-05-10T06:21:01Z`

## Status Counts

| Status | Rows |
|---|---:|
| pass | 62,483 |
| source_gap | 8,546 |
| warn | 63 |

## Issue Counts

| Issue | Rows |
|---|---:|
| official_reference_gap | 4,780 |
| missing_stock_sector | 2,596 |
| expected_missing_primary_isin | 1,971 |
| missing_etf_category | 221 |
| country_isin_mismatch | 63 |

## Top Flagged Exchanges

| Exchange | Pass | Notice | Source Gap | Warn | Quarantine |
|---|---:|---:|---:|---:|---:|
| OTC | 7,887 | 0 | 3,169 | 0 | 0 |
| SSE | 2,301 | 0 | 488 | 0 | 0 |
| SZSE | 2,739 | 0 | 343 | 1 | 0 |
| B3 | 1,264 | 0 | 320 | 0 | 0 |
| LSE | 6,111 | 0 | 244 | 60 | 0 |
| TSXV | 788 | 0 | 278 | 0 | 0 |
| TSX | 1,652 | 0 | 251 | 0 | 0 |
| SGX | 395 | 0 | 199 | 0 | 0 |
| BMV | 8 | 0 | 171 | 0 | 0 |
| NYSE ARCA | 2,488 | 0 | 166 | 0 | 0 |
| ASX | 1,150 | 0 | 148 | 0 | 0 |
| Euronext | 828 | 0 | 147 | 0 | 0 |
| CSE_LK | 164 | 0 | 143 | 0 | 0 |
| NASDAQ | 4,492 | 0 | 141 | 1 | 0 |
| TADAWUL | 56 | 0 | 135 | 0 | 0 |
| XETRA | 3,657 | 0 | 122 | 0 | 0 |
| STO | 618 | 0 | 107 | 0 | 0 |
| TWSE | 1,136 | 0 | 106 | 0 | 0 |
| BK | 2 | 0 | 102 | 0 | 0 |
| AMS | 219 | 0 | 95 | 0 | 0 |

## Notes

- `entry_quality.csv` contains one row per `listing_key` and is the complete per-entry report.
- `notice` marks soft alias-review hints; it is not a structural row warning.
- `source_gap` means the row is structurally valid but lacks stronger source or metadata coverage.
- `quarantine` means deterministic checks found a hard contradiction that should be fixed before treating the row as high quality.

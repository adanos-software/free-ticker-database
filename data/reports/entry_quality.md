# Entry Quality Report

Generated at: `2026-11-27T15:32:55Z`

## Status Counts

| Status | Rows |
|---|---:|
| notice | 10 |
| pass | 86,965 |
| source_gap | 10,730 |
| warn | 43 |

## Issue Counts

| Issue | Rows |
|---|---:|
| official_reference_gap | 6,495 |
| venue_missing_official_source | 3,287 |
| expected_missing_primary_isin | 646 |
| missing_stock_sector | 554 |
| missing_etf_category | 86 |
| official_name_mismatch | 24 |
| official_isin_mismatch | 19 |
| low_company_name_overlap | 10 |
| country_isin_mismatch | 1 |

## Top Flagged Exchanges

| Exchange | Pass | Notice | Source Gap | Warn | Quarantine |
|---|---:|---:|---:|---:|---:|
| OTC | 8,484 | 2 | 3,254 | 12 | 0 |
| XSTU | 0 | 0 | 2,773 | 0 | 0 |
| FSX | 7,625 | 0 | 518 | 0 | 0 |
| NASDAQ | 4,505 | 4 | 272 | 3 | 0 |
| BMV | 91 | 0 | 253 | 0 | 0 |
| Munich | 0 | 0 | 223 | 0 | 0 |
| XDUS | 0 | 0 | 199 | 0 | 0 |
| AMS | 555 | 0 | 194 | 0 | 0 |
| XETRA | 5,405 | 0 | 193 | 0 | 0 |
| BVB | 55 | 0 | 192 | 0 | 0 |
| NYSE ARCA | 2,530 | 0 | 175 | 1 | 0 |
| TSX | 2,272 | 0 | 163 | 1 | 0 |
| ASX | 2,101 | 2 | 155 | 1 | 0 |
| LSE | 6,880 | 0 | 142 | 2 | 0 |
| Euronext | 1,333 | 0 | 142 | 1 | 0 |
| TSXV | 1,279 | 0 | 140 | 3 | 0 |
| HKEX | 3,148 | 0 | 120 | 3 | 0 |
| NYSE | 1,929 | 0 | 97 | 1 | 0 |
| JSE | 123 | 0 | 89 | 0 | 0 |
| NSE_IN | 3,462 | 0 | 87 | 0 | 0 |

## Notes

- `entry_quality.csv` contains one row per `listing_key` and is the complete per-entry report.
- `notice` marks soft alias-review hints; it is not a structural row warning.
- `source_gap` means the row is structurally valid but lacks stronger source or metadata coverage.
- `quarantine` means deterministic checks found a hard contradiction that should be fixed before treating the row as high quality.

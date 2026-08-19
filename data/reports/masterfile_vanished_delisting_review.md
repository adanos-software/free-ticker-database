# Masterfile Vanished Delisting Review
- Generated at: `2026-08-19T15:42:35Z`
- Rotation date: `2026-08-25` overlay
- Vanished reference rows: `1`
- Still in database: `1`
- Applied drops: `0`
- Policy: `feed_delisting_classifier_not_direct_deletion`

## Classifier counts

| Action | Rows |
|---|---:|
| manual_rename_vs_delisting_required | 1 |

## Rows still in the database

| Exchange | Ticker | Source | Action |
|---|---|---|---|
| KRX | 0036D0 | krx_etf_finder | manual_rename_vs_delisting_required |

## Notes

- `KRX::0036D0` vanished from `krx_etf_finder` only; the listing stays.
- `HKEX::01063` ISIN recode `BMG8571C2494` -> `BMG8571C2643` is listing-keyed from official HKEX ListOfSecurities; name kept.
- `jpx_tse_stock_detail` and `lse_company_reports` remained unavailable.
- No vanished rows are dropped.

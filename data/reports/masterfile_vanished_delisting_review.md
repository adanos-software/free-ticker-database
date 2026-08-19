# Masterfile Vanished Delisting Review
- Generated at: `2026-08-19T16:23:25Z`
- Rotation date: `2026-08-19` JPX TSE stock-detail refresh
- Vanished reference rows: `9`
- Still in database: `9`
- Applied drops: `0`
- Policy: `feed_delisting_classifier_not_direct_deletion`

## Classifier counts

| Action | Rows |
|---|---:|
| manual_rename_vs_delisting_required | 9 |

## Rows still in the database

| Exchange | Ticker | Source | Action |
|---|---|---|---|
| CSE_LK | AMF.N0000 | cse_lk_all_security_code | manual_rename_vs_delisting_required |
| CSE_LK | AMF.N0000 | cse_lk_company_info_summary | manual_rename_vs_delisting_required |
| TSE | 2763 | jpx_tse_stock_detail | manual_rename_vs_delisting_required |
| TSE | 311A | jpx_tse_stock_detail | manual_rename_vs_delisting_required |
| TSE | 4171 | jpx_tse_stock_detail | manual_rename_vs_delisting_required |
| TSE | 4367 | jpx_tse_stock_detail | manual_rename_vs_delisting_required |
| TSE | 6096 | jpx_tse_stock_detail | manual_rename_vs_delisting_required |
| TSE | 6197 | jpx_tse_stock_detail | manual_rename_vs_delisting_required |
| TSE | 8283 | jpx_tse_stock_detail | manual_rename_vs_delisting_required |

## Notes

- JPX TSE stock-detail completed from partial cache: 4040 network rows.
- Seven TSE tickers vanished from the JPX detail API; listings stay.
- `CSE_LK::AMF.N0000` remains vanished from both CSE Sri Lanka sources; listing stays.
- Kuwait, BME security prices, CSE Morocco and TWSE ETF list remained unavailable.
- Listing-keyed official ISIN recode applied for `Euronext::MTH`. Ten empty TSE ISINs filled from JPX stock detail.
- No vanished rows are dropped.

# Masterfile Vanished Delisting Review
- Generated at: `2026-08-19T16:43:03Z`
- Rotation date: `2026-08-19` Kuwait + TWSE ETF refresh
- Vanished reference rows: `15`
- Still in database: `10`
- Applied drops: `0`
- Policy: `feed_delisting_classifier_not_direct_deletion`

## Classifier counts

| Action | Rows |
|---|---:|
| manual_rename_vs_delisting_required | 15 |

## Rows still in the database

| Exchange | Ticker | Source | Action |
|---|---|---|---|
| BK | KPPC | boursa_kuwait_stocks | manual_rename_vs_delisting_required |
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

- Boursa Kuwait stocks refreshed via Chrome impersonation: 140 network rows.
- TWSE ETF list switched from the hanging rwd feed to official open data t187ap47_L: 268 network rows.
- BK::KPPC vanished from the Kuwait feed; listing stays.
- Five old TWSE rwd dual-currency ticker strings vanished; real 006205/00636/00643/00657/00668 listings stay.
- BME security prices and CSE Morocco remain unavailable; empty refresh preserves cache.
- CSE_LK::AMF.N0000 and seven JPX TSE vanished tickers remain classifier-only.
- Listing-keyed official ISIN recodes applied for 12 same-instrument keys after Kuwait/TWSE refresh.
- No vanished rows are dropped.

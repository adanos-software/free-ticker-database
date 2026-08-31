# Masterfile Vanished Delisting Review
- Generated at: `2026-08-31T07:32:26Z`
- Policy: `feed_delisting_classifier_not_direct_deletion`
- Vanished reference rows: `63`
- Rotation vanished rows: `36`
- Backlog rows: `27`
- Still in database: `51`
- Applied drops: `0`

## Classifier counts

| Action | Rows |
|---|---:|
| manual_rename_vs_delisting_required | 51 |
| not_in_database | 12 |

## Rows still in the database

| Exchange | Ticker | Source | Action | Origin |
|---|---|---|---|---|
| BK | KPPC | boursa_kuwait_stocks | manual_rename_vs_delisting_required | backlog |
| BSE_HU | DUNAHOUSE | bse_hu_listed_companies | manual_rename_vs_delisting_required | rotation |
| BSE_IN | ACESEPP | bse_india_scrips | manual_rename_vs_delisting_required | rotation |
| BSE_IN | AUTOPRD | bse_india_scrips | manual_rename_vs_delisting_required | rotation |
| BSE_IN | BANSTEA | bse_india_scrips | manual_rename_vs_delisting_required | rotation |
| BSE_IN | CDG | bse_india_scrips | manual_rename_vs_delisting_required | rotation |
| BSE_IN | FMEC | bse_india_scrips | manual_rename_vs_delisting_required | rotation |
| BSE_IN | SATTVASUKU | bse_india_scrips | manual_rename_vs_delisting_required | rotation |
| BVB | SINA | bvb_shares_directory | manual_rename_vs_delisting_required | rotation |
| Bursa | 1368 | bursa_equity_isin | manual_rename_vs_delisting_required | backlog |
| Bursa | 7130 | bursa_equity_isin | manual_rename_vs_delisting_required | backlog |
| CSE_LK | AMF.N0000 | cse_lk_all_security_code | manual_rename_vs_delisting_required | backlog |
| CSE_LK | AMF.N0000 | cse_lk_company_info_summary | manual_rename_vs_delisting_required | backlog |
| FSX | 20MP | deutsche_boerse_frankfurt_all_tradable_equities | manual_rename_vs_delisting_required | backlog |
| FSX | 2BG | deutsche_boerse_frankfurt_all_tradable_equities | manual_rename_vs_delisting_required | rotation |
| FSX | 2RM | deutsche_boerse_frankfurt_all_tradable_equities | manual_rename_vs_delisting_required | rotation |
| FSX | 37T | deutsche_boerse_frankfurt_all_tradable_equities | manual_rename_vs_delisting_required | rotation |
| FSX | 646 | deutsche_boerse_frankfurt_all_tradable_equities | manual_rename_vs_delisting_required | backlog |
| FSX | 7111 | deutsche_boerse_frankfurt_all_tradable_equities | manual_rename_vs_delisting_required | rotation |
| FSX | 7YS0 | deutsche_boerse_frankfurt_all_tradable_equities | manual_rename_vs_delisting_required | rotation |
| FSX | 87M | deutsche_boerse_frankfurt_all_tradable_equities | manual_rename_vs_delisting_required | rotation |
| FSX | 8L8 | deutsche_boerse_frankfurt_all_tradable_equities | manual_rename_vs_delisting_required | backlog |
| FSX | 8L8C | deutsche_boerse_frankfurt_all_tradable_equities | manual_rename_vs_delisting_required | backlog |
| FSX | 8XG0 | deutsche_boerse_frankfurt_all_tradable_equities | manual_rename_vs_delisting_required | rotation |
| FSX | ANJ | deutsche_boerse_frankfurt_all_tradable_equities | manual_rename_vs_delisting_required | rotation |
| FSX | ANJ0 | deutsche_boerse_frankfurt_all_tradable_equities | manual_rename_vs_delisting_required | rotation |
| FSX | B3H | deutsche_boerse_frankfurt_all_tradable_equities | manual_rename_vs_delisting_required | rotation |
| FSX | E8X | deutsche_boerse_frankfurt_all_tradable_equities | manual_rename_vs_delisting_required | rotation |
| FSX | ECK | deutsche_boerse_frankfurt_all_tradable_equities | manual_rename_vs_delisting_required | backlog |
| FSX | K9A | deutsche_boerse_frankfurt_all_tradable_equities | manual_rename_vs_delisting_required | rotation |
| FSX | MT1 | deutsche_boerse_frankfurt_all_tradable_equities | manual_rename_vs_delisting_required | rotation |
| FSX | W2U1 | deutsche_boerse_frankfurt_all_tradable_equities | manual_rename_vs_delisting_required | rotation |
| FSX | WED | deutsche_boerse_frankfurt_all_tradable_equities | manual_rename_vs_delisting_required | backlog |
| HKEX | 00195 | hkex_securities_list | manual_rename_vs_delisting_required | backlog |
| JSE | RWESG | jse_etf_list | manual_rename_vs_delisting_required | backlog |
| KOSDAQ | 269620 | krx_listed_companies | manual_rename_vs_delisting_required | backlog |
| KRX | 397420 | krx_etf_finder | manual_rename_vs_delisting_required | backlog |
| OSL | PRYME | euronext_equities | manual_rename_vs_delisting_required | backlog |
| TSE | 2763 | jpx_tse_stock_detail | manual_rename_vs_delisting_required | backlog |
| TSE | 311A | jpx_tse_stock_detail | manual_rename_vs_delisting_required | backlog |
| TSE | 4171 | jpx_tse_stock_detail | manual_rename_vs_delisting_required | backlog |
| TSE | 4367 | jpx_tse_stock_detail | manual_rename_vs_delisting_required | backlog |
| TSE | 6096 | jpx_tse_stock_detail | manual_rename_vs_delisting_required | backlog |
| TSE | 6197 | jpx_tse_stock_detail | manual_rename_vs_delisting_required | backlog |
| TSE | 8283 | jpx_tse_stock_detail | manual_rename_vs_delisting_required | backlog |
| XETRA | 5HEE | deutsche_boerse_etfs_etps | manual_rename_vs_delisting_required | rotation |
| XETRA | ECK | deutsche_boerse_xetra_all_tradable_equities | manual_rename_vs_delisting_required | backlog |
| XETRA | INW | deutsche_boerse_xetra_all_tradable_equities | manual_rename_vs_delisting_required | backlog |
| XETRA | OP8E | deutsche_boerse_etfs_etps | manual_rename_vs_delisting_required | rotation |
| XETRA | PGH | deutsche_boerse_xetra_all_tradable_equities | manual_rename_vs_delisting_required | backlog |
| XETRA | UAL1 | deutsche_boerse_xetra_all_tradable_equities | manual_rename_vs_delisting_required | backlog |

## Notes

- Vanished official-reference rows are classified; listings are not dropped from this report.
- Rotation vanished rows: 36; still-in-database backlog carried: 27.
- Still in database: 51; not in database: 12.
- Applied drops from this classifier: 0.

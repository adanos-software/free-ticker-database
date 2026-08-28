# Masterfile Vanished Delisting Review
- Generated at: `2026-08-28T18:41:33Z`
- Policy: `feed_delisting_classifier_not_direct_deletion`
- Vanished reference rows: `46`
- Rotation vanished rows: `18`
- Backlog rows: `28`
- Still in database: `31`
- Applied drops: `0`

## Classifier counts

| Action | Rows |
|---|---:|
| blocked_suspended_kept_by_policy | 1 |
| manual_rename_vs_delisting_required | 30 |
| not_in_database | 15 |

## Rows still in the database

| Exchange | Ticker | Source | Action | Origin |
|---|---|---|---|---|
| BK | KPPC | boursa_kuwait_stocks | manual_rename_vs_delisting_required | backlog |
| BSE_IN | ANANDPROJ | bse_india_scrips | blocked_suspended_kept_by_policy | backlog |
| Bursa | 1368 | bursa_equity_isin | manual_rename_vs_delisting_required | backlog |
| Bursa | 7130 | bursa_equity_isin | manual_rename_vs_delisting_required | backlog |
| CSE_LK | AMF.N0000 | cse_lk_all_security_code | manual_rename_vs_delisting_required | backlog |
| CSE_LK | AMF.N0000 | cse_lk_company_info_summary | manual_rename_vs_delisting_required | backlog |
| FSX | 20MP | deutsche_boerse_frankfurt_all_tradable_equities | manual_rename_vs_delisting_required | backlog |
| FSX | 646 | deutsche_boerse_frankfurt_all_tradable_equities | manual_rename_vs_delisting_required | backlog |
| FSX | 8L8 | deutsche_boerse_frankfurt_all_tradable_equities | manual_rename_vs_delisting_required | backlog |
| FSX | 8L8C | deutsche_boerse_frankfurt_all_tradable_equities | manual_rename_vs_delisting_required | backlog |
| FSX | ECK | deutsche_boerse_frankfurt_all_tradable_equities | manual_rename_vs_delisting_required | backlog |
| FSX | WED | deutsche_boerse_frankfurt_all_tradable_equities | manual_rename_vs_delisting_required | backlog |
| HKEX | 00195 | hkex_securities_list | manual_rename_vs_delisting_required | backlog |
| JSE | RWESG | jse_etf_list | manual_rename_vs_delisting_required | backlog |
| KOSDAQ | 269620 | krx_listed_companies | manual_rename_vs_delisting_required | backlog |
| KRX | 397420 | krx_etf_finder | manual_rename_vs_delisting_required | backlog |
| OSL | PRYME | euronext_equities | manual_rename_vs_delisting_required | backlog |
| SET | COLOR | set_listed_companies | manual_rename_vs_delisting_required | rotation |
| STO | MIR | spotlight_companies_directory | manual_rename_vs_delisting_required | rotation |
| TADAWUL | 9590 | tadawul_main_market_watch | manual_rename_vs_delisting_required | rotation |
| TSE | 2763 | jpx_tse_stock_detail | manual_rename_vs_delisting_required | backlog |
| TSE | 311A | jpx_tse_stock_detail | manual_rename_vs_delisting_required | backlog |
| TSE | 4171 | jpx_tse_stock_detail | manual_rename_vs_delisting_required | backlog |
| TSE | 4367 | jpx_tse_stock_detail | manual_rename_vs_delisting_required | backlog |
| TSE | 6096 | jpx_tse_stock_detail | manual_rename_vs_delisting_required | backlog |
| TSE | 6197 | jpx_tse_stock_detail | manual_rename_vs_delisting_required | backlog |
| TSE | 8283 | jpx_tse_stock_detail | manual_rename_vs_delisting_required | backlog |
| XETRA | ECK | deutsche_boerse_xetra_all_tradable_equities | manual_rename_vs_delisting_required | backlog |
| XETRA | INW | deutsche_boerse_xetra_all_tradable_equities | manual_rename_vs_delisting_required | backlog |
| XETRA | PGH | deutsche_boerse_xetra_all_tradable_equities | manual_rename_vs_delisting_required | backlog |
| XETRA | UAL1 | deutsche_boerse_xetra_all_tradable_equities | manual_rename_vs_delisting_required | backlog |

## Notes

- Vanished official-reference rows are classified; listings are not dropped from this report.
- Rotation vanished rows: 18; still-in-database backlog carried: 28.
- Still in database: 31; not in database: 15.
- Applied drops from this classifier: 0.

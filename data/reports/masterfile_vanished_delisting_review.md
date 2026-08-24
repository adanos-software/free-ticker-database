# Masterfile Vanished Delisting Review
- Generated at: `2026-08-24T10:56:23Z`
- Policy: `feed_delisting_classifier_not_direct_deletion`
- Vanished reference rows: `40`
- Rotation vanished rows: `30`
- Backlog rows: `10`
- Still in database: `23`
- Applied drops: `0`

## Classifier counts

| Action | Rows |
|---|---:|
| blocked_suspended_kept_by_policy | 1 |
| manual_rename_vs_delisting_required | 22 |
| not_in_database | 17 |

## Rows still in the database

| Exchange | Ticker | Source | Action | Origin |
|---|---|---|---|---|
| BK | KPPC | boursa_kuwait_stocks | manual_rename_vs_delisting_required | backlog |
| BSE_IN | ANANDPROJ | bse_india_scrips | blocked_suspended_kept_by_policy | rotation |
| Bursa | 1368 | bursa_equity_isin | manual_rename_vs_delisting_required | rotation |
| Bursa | 7130 | bursa_equity_isin | manual_rename_vs_delisting_required | rotation |
| CSE_LK | AMF.N0000 | cse_lk_all_security_code | manual_rename_vs_delisting_required | backlog |
| CSE_LK | AMF.N0000 | cse_lk_company_info_summary | manual_rename_vs_delisting_required | backlog |
| FSX | 20MP | deutsche_boerse_frankfurt_all_tradable_equities | manual_rename_vs_delisting_required | rotation |
| FSX | 646 | deutsche_boerse_frankfurt_all_tradable_equities | manual_rename_vs_delisting_required | rotation |
| FSX | 8L8 | deutsche_boerse_frankfurt_all_tradable_equities | manual_rename_vs_delisting_required | rotation |
| FSX | 8L8C | deutsche_boerse_frankfurt_all_tradable_equities | manual_rename_vs_delisting_required | rotation |
| FSX | ECK | deutsche_boerse_frankfurt_all_tradable_equities | manual_rename_vs_delisting_required | rotation |
| FSX | WED | deutsche_boerse_frankfurt_all_tradable_equities | manual_rename_vs_delisting_required | rotation |
| TSE | 2763 | jpx_tse_stock_detail | manual_rename_vs_delisting_required | backlog |
| TSE | 311A | jpx_tse_stock_detail | manual_rename_vs_delisting_required | backlog |
| TSE | 4171 | jpx_tse_stock_detail | manual_rename_vs_delisting_required | backlog |
| TSE | 4367 | jpx_tse_stock_detail | manual_rename_vs_delisting_required | backlog |
| TSE | 6096 | jpx_tse_stock_detail | manual_rename_vs_delisting_required | backlog |
| TSE | 6197 | jpx_tse_stock_detail | manual_rename_vs_delisting_required | backlog |
| TSE | 8283 | jpx_tse_stock_detail | manual_rename_vs_delisting_required | backlog |
| XETRA | ECK | deutsche_boerse_xetra_all_tradable_equities | manual_rename_vs_delisting_required | rotation |
| XETRA | INW | deutsche_boerse_xetra_all_tradable_equities | manual_rename_vs_delisting_required | rotation |
| XETRA | PGH | deutsche_boerse_xetra_all_tradable_equities | manual_rename_vs_delisting_required | rotation |
| XETRA | UAL1 | deutsche_boerse_xetra_all_tradable_equities | manual_rename_vs_delisting_required | rotation |

## Notes

- Vanished official-reference rows are classified; listings are not dropped from this report.
- Rotation vanished rows: 30; still-in-database backlog carried: 10.
- Still in database: 23; not in database: 17.
- Applied drops from this classifier: 0.

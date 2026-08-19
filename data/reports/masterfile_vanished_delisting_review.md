# Masterfile Vanished Delisting Review
- Generated at: `2026-08-19T14:56:06Z`
- Rotation date: `2026-08-22` overlay
- Vanished reference rows: `11`
- Still in database: `10`
- Applied drops: `0`
- Policy: `feed_delisting_classifier_not_direct_deletion`

## Classifier counts

| Action | Rows |
|---|---:|
| manual_rename_vs_delisting_required | 11 |

## Rows still in the database

| Exchange | Ticker | Source | Action |
|---|---|---|---|
| TASE | ARTS | tase_securities_marketdata | manual_rename_vs_delisting_required |
| TASE | TKUN | tase_securities_marketdata | manual_rename_vs_delisting_required |
| TSX | GIQG | tmx_etf_screener | manual_rename_vs_delisting_required |
| TSX | GIQG.B | tmx_etf_screener | manual_rename_vs_delisting_required |
| TSX | UDA | tmx_etf_screener | manual_rename_vs_delisting_required |
| TPEX | 2237 | tpex_emerging_basic_info | manual_rename_vs_delisting_required |
| TPEX | 4546 | tpex_emerging_basic_info | manual_rename_vs_delisting_required |
| TPEX | 6241 | tpex_mainboard_daily_quotes | manual_rename_vs_delisting_required |
| WSE | CPA | wse_listed_companies | manual_rename_vs_delisting_required |
| WSE | PUR | wse_listed_companies | manual_rename_vs_delisting_required |

## Notes

- `WSE::ROB` official ISIN recode is not in the database; no listing-keyed apply.
- `TSX::GIQG.B` vanished from `tmx_etf_screener` only; the official row was preserved so `drop_stale_tmx_etf_duplicates` cannot delete the listing.
- No vanished rows are dropped.

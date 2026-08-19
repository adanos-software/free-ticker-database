# Masterfile Vanished Delisting Review
- Generated at: `2026-08-19T14:40:52Z`
- Rotation date: `2026-08-21` overlay on the 19/20 Aug batches
- Vanished reference rows: `13`
- Still in database: `6`
- Applied drops: `0`
- Policy: `feed_delisting_classifier_not_direct_deletion`

SGX::PH0 Hatten Land vanished from sgx_securities_prices; SGX::WYO Metrocon is a different ISIN, so PH0 stays.

SET::BPP Banpu Power vanished from SET listed/search and stays in the database.

## Classifier counts

| Action | Rows |
|---|---:|
| manual_rename_vs_delisting_required | 13 |

## Rows still in the database

| Exchange | Ticker | Source | Action |
|---|---|---|---|
| SET | BPP | set_listed_companies | manual_rename_vs_delisting_required |
| SET | BPP | set_stock_search | manual_rename_vs_delisting_required |
| SGX | PH0 | sgx_securities_prices | manual_rename_vs_delisting_required |
| SIX | COPN | six_equity_issuers | manual_rename_vs_delisting_required |
| STO | LANE-B | spotlight_companies_directory | manual_rename_vs_delisting_required |
| STO | LOVI | spotlight_companies_directory | manual_rename_vs_delisting_required |

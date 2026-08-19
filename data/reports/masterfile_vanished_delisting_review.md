# Masterfile Vanished Delisting Review
- Generated at: `2026-08-19T15:58:38Z`
- Rotation date: `2026-08-19` CSE Sri Lanka freshness retry
- Vanished reference rows: `2`
- Still in database: `2`
- Applied drops: `0`
- Policy: `feed_delisting_classifier_not_direct_deletion`

## Classifier counts

| Action | Rows |
|---|---:|
| manual_rename_vs_delisting_required | 2 |

## Rows still in the database

| Exchange | Ticker | Source | Action |
|---|---|---|---|
| CSE_LK | AMF.N0000 | cse_lk_all_security_code | manual_rename_vs_delisting_required |
| CSE_LK | AMF.N0000 | cse_lk_company_info_summary | manual_rename_vs_delisting_required |

## Notes

- `CSE_LK::AMF.N0000` vanished from both CSE Sri Lanka sources; the listing stays.
- Kuwait, BME security prices, CSE Morocco, TWSE ETF and JPX TSE stock detail remained unavailable.
- Listing-keyed official ISIN recodes applied for `AMS::3PLT`, `STO::AEC`, `LSE::AEG`, `LSE::BHP`, `LSE::NOG`, `LSE::PAY`, `XETRA::SKOR`.
- No vanished rows are dropped.

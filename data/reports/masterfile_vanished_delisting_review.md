# Masterfile Vanished Delisting Review
- Generated at: `2026-08-19T15:19:38Z`
- Rotation date: `2026-08-23` overlay
- Vanished reference rows: `61`
- Still in database: `25`
- Applied drops: `0`
- Policy: `feed_delisting_classifier_not_direct_deletion`

## Classifier counts

| Action | Rows |
|---|---:|
| manual_rename_vs_delisting_required | 61 |

## Rows still in the database

| Exchange | Ticker | Source | Action |
|---|---|---|---|
| ASX | DHOF | asx_investment_products | manual_rename_vs_delisting_required |
| ASX | HHIF | asx_investment_products | manual_rename_vs_delisting_required |
| ASX | HJZP | asx_investment_products | manual_rename_vs_delisting_required |
| ASX | MHG | asx_investment_products | manual_rename_vs_delisting_required |
| B3 | AURB11 | b3_instruments_equities | manual_rename_vs_delisting_required |
| B3 | BAOK39 | b3_instruments_equities | manual_rename_vs_delisting_required |
| B3 | BFLO39 | b3_instruments_equities | manual_rename_vs_delisting_required |
| B3 | BGOZ39 | b3_instruments_equities | manual_rename_vs_delisting_required |
| B3 | BIUS39 | b3_instruments_equities | manual_rename_vs_delisting_required |
| B3 | BRIM11 | b3_instruments_equities | manual_rename_vs_delisting_required |
| B3 | BTLH39 | b3_instruments_equities | manual_rename_vs_delisting_required |
| B3 | BVAR11 | b3_instruments_equities | manual_rename_vs_delisting_required |
| B3 | DVLT11 | b3_instruments_equities | manual_rename_vs_delisting_required |
| B3 | GLCR11 | b3_instruments_equities | manual_rename_vs_delisting_required |
| B3 | HBCR11 | b3_instruments_equities | manual_rename_vs_delisting_required |
| B3 | HCHG11 | b3_instruments_equities | manual_rename_vs_delisting_required |
| B3 | HUSC11 | b3_instruments_equities | manual_rename_vs_delisting_required |
| B3 | KEVE11 | b3_instruments_equities | manual_rename_vs_delisting_required |
| B3 | LLBI3 | b3_instruments_equities | manual_rename_vs_delisting_required |
| B3 | PMFO11 | b3_instruments_equities | manual_rename_vs_delisting_required |
| B3 | VANG11 | b3_instruments_equities | manual_rename_vs_delisting_required |
| B3 | VJFD11 | b3_instruments_equities | manual_rename_vs_delisting_required |
| B3 | TIRB11 | b3_listed_etfs | manual_rename_vs_delisting_required |
| SSE_CL | NORTEGRAN | bolsa_santiago_instruments | manual_rename_vs_delisting_required |
| SSE_CL | OROBLANCO | bolsa_santiago_instruments | manual_rename_vs_delisting_required |

## Notes

- B3 vanished rows are mostly FIIs/BDRs absent from the listed-equity database; remaining listed rows stay.
- ASX investment-product and Santiago vanished rows stay; names are not dumped.
- No vanished rows are dropped.

# Masterfile Vanished Delisting Review

- Generated at: `2026-08-19T08:44:42Z`
- Rotation date: `2026-08-19`
- Vanished reference rows: `48`
- Still in database: `35`
- Applied drops: `0`
- Policy: `feed_delisting_classifier_not_direct_deletion`

## Classifier counts

| Action | Rows |
|---|---:|
| manual_rename_vs_delisting_required | 48 |

## ISSC → IA

Nasdaq listed.txt dropped `ISSC` and added `IA` (Innovative Solutions and Support). Issuer press (2026-08-11) says the ticker change is effective 2026-08-18 with unchanged CUSIP. The row was **not** applied as a new listing and **not** dropped. `apply_symbol_changes` stays blocked until the official master proves the unchanged ISIN.

## Rows still in the database

| Exchange | Ticker | Listing name | Official name | Source | Action |
|---|---|---|---|---|---|
| LSE | 0GB5 | Niloerngruppen AB Series B | NILORNGRUPPEN AB | lse_price_explorer | manual_rename_vs_delisting_required |
| LSE | 0HJO | Avalonbay Communities Inc. | AVALONBAY COMMUNITIES INC | lse_price_explorer | manual_rename_vs_delisting_required |
| LSE | 0IFX | Electronic Arts Inc. | ELECTRONIC ARTS INC | lse_price_explorer | manual_rename_vs_delisting_required |
| LSE | ANCR | Animalcare Group Plc | ANIMALCARE GROUP PLC | lse_price_explorer | manual_rename_vs_delisting_required |
| LSE | BSIF | Bluefield Solar Income Fund | BLUEFIELD SOLAR INCOME FUND LIMITED | lse_price_explorer | manual_rename_vs_delisting_required |
| LSE | CRDL | Cordel Group Plc | CORDEL GROUP PLC | lse_price_explorer | manual_rename_vs_delisting_required |
| LSE | DELT | Deltic Energy PLC | DELTIC ENERGY PLC | lse_price_explorer | manual_rename_vs_delisting_required |
| LSE | FLTR | Flutter Entertainment PLC | FLUTTER ENTERTAINMENT PLC | lse_price_explorer | manual_rename_vs_delisting_required |
| LSE | IIG | Intuitive Investments Group Plc | INTUITIVE INVESTMENTS GROUP PLC | lse_price_explorer | manual_rename_vs_delisting_required |
| LSE | IPF | International Personal Finance PLC | INTERNATIONAL PERSONAL FINANCE PLC | lse_price_explorer | manual_rename_vs_delisting_required |
| NASDAQ | FTRK | FAST TRACK GROUP | FAST TRACK GROUP - Ordinary shares | nasdaq_listed | manual_rename_vs_delisting_required |
| NASDAQ | IPCX | Inflection Point Acquisition Corp. III Class A Ordinary Shares | Inflection Point Acquisition Corp. III - Class A ordinary shares | nasdaq_listed | manual_rename_vs_delisting_required |
| NASDAQ | ISSC | Innovative Solutions and Support | Innovative Solutions and Support, Inc. - Common Stock | nasdaq_listed | manual_rename_vs_delisting_required |
| NASDAQ | TALK | Talkspace Inc | Talkspace, Inc. - Common Stock | nasdaq_listed | manual_rename_vs_delisting_required |
| CPH | ASTK | Asetek A/S | Asetek | nasdaq_nordic_copenhagen_shares | manual_rename_vs_delisting_required |
| HEL | PIIPPO | Piippo OYJ | Piippo Oyj | nasdaq_nordic_helsinki_shares | manual_rename_vs_delisting_required |
| STO | CINT | Cint Group Ab | Cint Group | nasdaq_nordic_stockholm_shares | manual_rename_vs_delisting_required |
| STO | CMOTEC-B | Scandinavian ChemoTech AB Series B | Scandinavian ChemoTech B | nasdaq_nordic_stockholm_shares | manual_rename_vs_delisting_required |
| STO | FING-B | Fingerprint Cards AB (publ) | Fingerprint Cards B | nasdaq_nordic_stockholm_shares | manual_rename_vs_delisting_required |
| STO | FPIP | FormPipe Software AB | Formpipe Software | nasdaq_nordic_stockholm_shares | manual_rename_vs_delisting_required |
| STO | HELIO | Heliospectra publ AB | Heliospectra | nasdaq_nordic_stockholm_shares | manual_rename_vs_delisting_required |
| STO | HOTEL | Hotel Fast SSE | Hotel Fast SSE | nasdaq_nordic_stockholm_shares | manual_rename_vs_delisting_required |
| STO | HULT-B | Hultström Group B | Hultström Group B | nasdaq_nordic_stockholm_shares | manual_rename_vs_delisting_required |
| STO | INSP | Insplorion | Insplorion | nasdaq_nordic_stockholm_shares | manual_rename_vs_delisting_required |
| STO | LOGI-A | Logistea A | Logistea A | nasdaq_nordic_stockholm_shares | manual_rename_vs_delisting_required |
| STO | LPGO | Lipigon Pharmaceuticals AB | Lipigon Pharmaceuticals | nasdaq_nordic_stockholm_shares | manual_rename_vs_delisting_required |
| STO | MAHA-A | Maha Energy AB (publ) | Maha Capital | nasdaq_nordic_stockholm_shares | manual_rename_vs_delisting_required |
| STO | NIL-B | Niloerngruppen AB Series B | Nilörngruppen B | nasdaq_nordic_stockholm_shares | manual_rename_vs_delisting_required |
| STO | REFINE | Refine Group AB | Refine Group | nasdaq_nordic_stockholm_shares | manual_rename_vs_delisting_required |
| STO | SBOK | ScandBook Holding AB | ScandBook Holding | nasdaq_nordic_stockholm_shares | manual_rename_vs_delisting_required |
| STO | SNM | ShaMaran Petroleum Corp | ShaMaran Petroleum Corp | nasdaq_nordic_stockholm_shares | manual_rename_vs_delisting_required |
| STO | STABL | Stayble Therapeutics AB | Stayble Therapeutics | nasdaq_nordic_stockholm_shares | manual_rename_vs_delisting_required |
| STO | TERRNT-B | Terranet AB | Terranet B | nasdaq_nordic_stockholm_shares | manual_rename_vs_delisting_required |
| STO | TRAD | TradeDoubler AB | TradeDoubler | nasdaq_nordic_stockholm_shares | manual_rename_vs_delisting_required |
| STO | VESTUM | Vestum AB (publ) | Vestum | nasdaq_nordic_stockholm_shares | manual_rename_vs_delisting_required |

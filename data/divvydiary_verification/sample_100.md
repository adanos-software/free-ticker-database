# DivvyDiary ISIN sample (same n=100 as Onvista)

Generated: 2026-09-28T10:45:01Z
Method: HEAD `https://divvydiary.com/en/xx-{ISIN}` only. Same ISINs as `data/onvista_verification/sample_100.json`. No ingest.

## Decisions

- `match`: 81
- `name_mismatch`: 13
- `not_on_divvydiary`: 5
- `divvy_non_stock`: 1

## Vs Onvista

- Onvista `not_on_onvista` → DivvyDiary `match`: 33
- Onvista `match` → DivvyDiary `match`: 32
- Onvista `name_mismatch` → DivvyDiary `match`: 16
- Onvista `name_mismatch` → DivvyDiary `name_mismatch`: 6
- Onvista `not_on_onvista` → DivvyDiary `name_mismatch`: 6
- Onvista `not_on_onvista` → DivvyDiary `not_on_divvydiary`: 5
- Onvista `match` → DivvyDiary `name_mismatch`: 1
- Onvista `not_on_onvista` → DivvyDiary `divvy_non_stock`: 1

## Non-match rows

| ISIN | listing | DB name | DivvyDiary | decision | Onvista |
|---|---|---|---|---|---|
| `INE495M01019` | `BSE_IN::ZSHERAPR` | Sheraton Properties & Finance Ltd | sheraton properties and finance | `divvy_non_stock` | `not_on_onvista` |
| `LK0220N00000` | `CSE_LK::COCO.N0000` | RENUKA FOODS PLC | not_found | `not_on_divvydiary` | `not_on_onvista` |
| `KYG2S85A1045` | `HKEX::01691` | JS GLOBAL LIFE | js global lifestyle co | `name_mismatch` | `name_mismatch` |
| `CNE100002R24` | `HKEX::01697` | SDITC | shandong international trust co | `name_mismatch` | `not_on_onvista` |
| `KR7388790008` | `KOSDAQ::388790` | IBKS No. 16 Special Purpose Acquisition Co. Ltd. | licomm co | `name_mismatch` | `not_on_onvista` |
| `JP3268870007` | `TSE::3656` | KLab Inc. | klab | `name_mismatch` | `name_mismatch` |
| `PK0110601017` | `PSX::TPLP` | TPL Properties Ltd | not_found | `not_on_divvydiary` | `not_on_onvista` |
| `US26871Q1031` | `OTC::EMPS` | Emp Solutons Inc | emp solutions | `name_mismatch` | `name_mismatch` |
| `KR7393970009` | `KOSDAQ::393970` | DAEJIN ADVANCED MATERIALS | not_found | `not_on_divvydiary` | `not_on_onvista` |
| `INE538C01017` | `BSE_IN::SAMTELIN` | Samtel India Ltd-$ | not_found | `not_on_divvydiary` | `not_on_onvista` |
| `FI4000081138` | `FSX::L7G` | Lehto Group Oyj | lehto group | `name_mismatch` | `name_mismatch` |
| `BRMTALACNPB8` | `B3::MTAL6` | CIMETAL SIDERURGIA S/A | not_found | `not_on_divvydiary` | `not_on_onvista` |
| `CNE1000023G9` | `HKEX::01799` | XINTE ENERGY | xinte energy co ltd shs h unitary 144a reg s | `name_mismatch` | `match` |
| `DE0007856023` | `XETRA::ZIL2` | ELRINGKLINGER AG NA O.N. | elringklinger | `name_mismatch` | `name_mismatch` |
| `TW0003556007` | `TPEX::3556` | eGalax_eMPIA Technology | egalax emipia technology | `name_mismatch` | `not_on_onvista` |
| `MYQ0383OO008` | `Bursa::0383` | PSP Energy Berhad | psp energy bhd | `name_mismatch` | `not_on_onvista` |
| `JP3346350006` | `TSE::4320` | CE Holdings Co.,Ltd. | ce holdings co | `name_mismatch` | `not_on_onvista` |
| `US4258851009` | `NASDAQ::HNNA` | Hennessy Ad | hennessy advisors | `name_mismatch` | `name_mismatch` |
| `TW0001909000` | `TWSE::1909` | 榮成紙業股份有限公司 | longchen paper and packaging co | `name_mismatch` | `not_on_onvista` |

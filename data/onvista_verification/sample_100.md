# Onvista ISIN sample (n=100)

Generated: 2026-09-28T10:36:41Z
Method: HEAD `https://www.onvista.de/aktien/xxx-{ISIN}` only. Crawl-delay 20s. No ingest.

## Decisions

- `not_on_onvista`: 45
- `match`: 33
- `name_mismatch`: 22

## Prefix mix

- `US`: 18
- `CN`: 11
- `IN`: 10
- `JP`: 8
- `CA`: 7
- `KY`: 5
- `KR`: 5
- `TW`: 4
- `AU`: 4
- `GB`: 3
- `SE`: 2
- `TH`: 2
- `MY`: 2
- `IL`: 1
- `LK`: 1
- `TR`: 1
- `VN`: 1
- `EG`: 1
- `HK`: 1
- `FR`: 1
- `PK`: 1
- `IT`: 1
- `CH`: 1
- `NO`: 1
- `SG`: 1
- `PL`: 1
- `FI`: 1
- `BR`: 1
- `ID`: 1
- `ES`: 1
- `DE`: 1
- `BM`: 1

## Non-match rows

| ISIN | listing | DB name | Onvista | decision |
|---|---|---|---|---|
| `US20344R1032` | `OTC::CYSM` | Community Bancorp of Santa Maria | COMMERICAL BANK OF SANTA MARI | `name_mismatch` |
| `CA7506481075` | `TSXV::RRCC.P` | Raging Rhino Capital Corp. | RAGG RHINNPV | `name_mismatch` |
| `KYG2S85A1045` | `HKEX::01691` | JS GLOBAL LIFE | JS GLOBAL LIFESTYLE | `name_mismatch` |
| `US45579B1017` | `OTC::IGEX` | Indo Global Exchange | INDO GLOBAL EXCHANGES PTE LTD | `name_mismatch` |
| `JP3268870007` | `TSE::3656` | KLab Inc. | KLab | `name_mismatch` |
| `US26871Q1031` | `OTC::EMPS` | Emp Solutons Inc | SECURITIES HOLD | `name_mismatch` |
| `US29382R1077` | `FSX::EV9` | Entravision Communications Corporation | ENTRAVISION COMM | `name_mismatch` |
| `CNE100005485` | `SSE::688665` | Cubic Sensor and Instrument Co Ltd | CUBC SENS | `name_mismatch` |
| `CNE100002VX9` | `SSE::603527` | Anhui Zhongyuan New Materials Co Ltd | ANHI ZHON | `name_mismatch` |
| `US3152932097` | `OTC::FGPR` | Ferrellgas Partners L.P | FERRELLGAS PART UN | `name_mismatch` |
| `US00831X1028` | `OTC::AFTM` | Aftermaster Inc | DIMENSIONAL VISI | `name_mismatch` |
| `US0431132085` | `NASDAQ::ARTNA` | Artesian Resources Corporation | ARTESIAN RES A | `name_mismatch` |
| `KYG0206E1044` | `NASDAQ::AGCC` | Agencia Comercial Spirits Ltd Class A Ordinary Shares | Agencia Commercial Spirits | `name_mismatch` |
| `US2017123041` | `OTC::CIBEY` | Commercial International Bank | COM INTL BK | `name_mismatch` |
| `CNE1000055K3` | `SSE::688236` | Beijing Chunlizhengda Medical Instruments Co Ltd | BEIG CHUN | `name_mismatch` |
| `CA3024371088` | `FSX::TQ4` | FANDIFI TECHNOLOGY CORP. | FANDOM SPORTS MEDIA | `name_mismatch` |
| `FI4000081138` | `FSX::L7G` | Lehto Group Oyj | LEHTO GROUP | `name_mismatch` |
| `KYG7049C1042` | `NYSE MKT::POAS` | Phaos Technology Holdings (Cayman) Limited | PHAOS TECHNO O N | `name_mismatch` |
| `DE0007856023` | `XETRA::ZIL2` | ELRINGKLINGER AG NA O.N. | Elringklinger | `name_mismatch` |
| `GB0002631934` | `LSE::BVT` | Baronsmead Venture Trust Plc | BARONSMEAD VCT 2 | `name_mismatch` |
| `US16947K1079` | `OTC::CBUMY` | China National Building Material Co Ltd ADR | CHINA NATL BUILD MAT CO | `name_mismatch` |
| `US4258851009` | `NASDAQ::HNNA` | Hennessy Ad | HENNESSY ADVISRS | `name_mismatch` |

## Not on Onvista (45)

- `TASE::BLSR` `IL0010985658` Blue Square Real Estate Ltd
- `SZSE::000573` `CNE0000003D0` DongGuan Winnerway Industrial Zone Ltd
- `BSE_IN::ZSHERAPR` `INE495M01019` Sheraton Properties & Finance Ltd
- `NSE_IN::SHANTIGEAR` `INE631A01022` Shanthi Gears Limited
- `CSE_LK::COCO.N0000` `LK0220N00000` RENUKA FOODS PLC
- `NSE_IN::PNGJL` `INE953R01016` P N Gadgil Jewellers Limited
- `HOSE::PNJ` `VN000000PNJ6` Phu Nhuan Jewelry JSC
- `HKEX::01697` `CNE100002R24` SDITC
- `EGX::PHTV` `EGS70331C011` Pyramisa Hotels
- `KOSDAQ::388790` `KR7388790008` IBKS No. 16 Special Purpose Acquisition Co. Ltd.
- `TPEX::6175` `TW0006175003` Liton Technology
- `TSE::226A` `JP3212500007` Katsu Mi Japan Inc.
- `TSE::3969` `JP3160930008` ATLED CORP.
- `PSX::TPLP` `PK0110601017` TPL Properties Ltd
- `BSE_IN::UVS` `INE528C01018` UVS Hospitality And Services Ltd
- `KOSDAQ::060570` `KR7060570009` Dreamus Company
- `KRX::064960` `KR7064960008` SNT Motiv Co Ltd
- `TSE::4761` `JP3317500001` SAKURA KCS Corporation
- `SZSE::003004` `CNE100004926` Beijing Telesound Electronics Co Ltd
- `BSE_IN::AHMDSTE` `INE868C01018` Ahmedabad Steelcraft Ltd
- `BSE_IN::DCMFINSERV` `INE891B01012` DCM Financial Services Ltd
- `KOSDAQ::393970` `KR7393970009` DAEJIN ADVANCED MATERIALS
- `SET::DPAINT` `THA534010006` Delta Paint PCL
- `SGX::L19` `SG1E20001293` Lum Chang
- `WSE::GX1` `PLGENXN00013` Genxone SA
- `BSE_IN::SAMTELIN` `INE538C01017` Samtel India Ltd-$
- `KOSDAQ::065650` `KR7065650004` Hyper Corporation Inc.
- `ASX::FRE` `AU0000197352` Firebrick Pharma Limited
- `B3::MTAL6` `BRMTALACNPB8` CIMETAL SIDERURGIA S/A
- `HKEX::02352` `CNE1000058Z5` DOWELL SERVICE
- `SZSE::002940` `CNE100003GC2` Zhejiang Anglikang Pharmaceutical Co Ltd Class A
- `ASX::PCI` `AU0000041261` Perpetual Credit Income Trust
- `SZSE::300602` `CNE100002Q25` Shenzhen FRD Science & Technology Co Ltd
- `NSE_IN::NETWEB` `INE0NT901020` Netweb Technologies India Limited
- `ASX::WMA` `AU0000110827` Wam Alternative Assets Limited
- `Bursa::5320` `MYL5320OO006` Prolintas Infra Business Trust
- `BSE_IN::CHOKSILA` `INE493D01013` Choksi Laboratories Ltd
- `NSE_IN::MAXPOSURE` `INE0ECC01022` Maxposure Limited
- `TPEX::5488` `TW0005488001` Sunf Pu Technology Co., Ltd.
- `TPEX::3556` `TW0003556007` eGalax_eMPIA Technology
- `Bursa::0383` `MYQ0383OO008` PSP Energy Berhad
- `TSE::4320` `JP3346350006` CE Holdings Co.,Ltd.
- `SET::LPH` `TH6733010008` Ladprao General Hospital Public Company Limited
- `HKEX::08076` `BMG819641017` SING LEE
- `TWSE::1909` `TW0001909000` 榮成紙業股份有限公司

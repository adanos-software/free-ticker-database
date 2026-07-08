# ISIN identity validation (OpenFIGI)

Generated: 2026-07-08T04:36:18Z

**isin_issues_detected: False**

ISINs validated: 61764 | match=59959 mismatch=101 no_data=1704

Detection only. `mismatch` = OpenFIGI resolves the ISIN to a security whose ticker AND name differ from ours (likely wrong/stale ISIN) — verify before correcting via the override pipeline. `no_data` = OpenFIGI has no record (coverage gap, not an error).

## Residual triage

- Mismatch residuals: `101` (review_required_openfigi_resolves_different_security)
- OpenFIGI no-data residuals: `1704` (provider coverage gap)
- Remaining unclassified residuals: `0`

### Mismatch residuals by exchange

| Exchange | Rows |
|---|---:|
| OTC | 28 |
| NSE_IN | 10 |
| NASDAQ | 8 |
| BSE_IN | 7 |
| Bursa | 6 |
| LSE | 5 |
| TSX | 4 |
| CSE_LK | 3 |
| Euronext | 3 |
| NYSE | 3 |
| NYSE ARCA | 3 |
| SGX | 3 |
| TSXV | 3 |
| B3 | 2 |
| SIX | 2 |
| AMS | 1 |
| ASX | 1 |
| ATHEX | 1 |
| BATS | 1 |
| BIST | 1 |
| CSE_MA | 1 |
| JSE | 1 |
| NSE_KE | 1 |
| OSL | 1 |
| SET | 1 |
| WSE | 1 |

### OpenFIGI no-data residuals by exchange

| Exchange | Rows |
|---|---:|
| OTC | 604 |
| B3 | 220 |
| TPEX | 200 |
| NYSE ARCA | 120 |
| NASDAQ | 106 |
| TSX | 98 |
| BATS | 68 |
| TSXV | 67 |
| ASX | 37 |
| EGX | 28 |
| JSE | 15 |
| BSE_IN | 14 |
| SET | 13 |
| NYSE | 12 |
| NGX | 9 |
| SZSE | 6 |
| HKEX | 5 |
| STO | 5 |
| CSE_MA | 4 |
| LSE | 4 |
| SSE_CL | 4 |
| ATHEX | 3 |
| BCBA | 3 |
| KRX | 3 |
| NEO | 3 |
| NSE_KE | 3 |
| NYSE MKT | 3 |
| OSL | 3 |
| PSX | 3 |
| SEM | 3 |

## Mismatch review queue

| ISIN | Our ticker | Our name | OpenFIGI ticker(s) | OpenFIGI name | Triage |
|---|---|---|---|---|---|
| AU000000ODM3 | G11 | G11 RESOURCES LIMITED | 09Z | ODIN METALS LTD | review_required_openfigi_resolves_different_security |
| AU000000PTM6 | PTMGF | L1 Group Limited | PTMAUD | PLATINUM ASSET MANAGEMENT | review_required_openfigi_resolves_different_security |
| BRPMSPCPA000 | PMSP11B | PREFEITURA MUNICIPAL DE SAO PA | PMSP11BL | CEPAC - AGUA ESPRAIADA | review_required_openfigi_resolves_different_security |
| BRPMSPCPA018 | PMSP12B | PREFEITURA MUNICIPAL DE SAO PA | PMSP12BL | CEPAC - FARIA LIMA | review_required_openfigi_resolves_different_security |
| CA04364G1063 | AOTVF | Cambria Gold Mines Inc. | AOT1EUR | ASCOT RESOURCES LTD | review_required_openfigi_resolves_different_security |
| CA09173B1076 | KEEL | Keel Infrastructure Corp | 1B2,1B2D | BITFARMS LTD/CANADA | review_required_openfigi_resolves_different_security |
| CA17165J2020 | CGQD | CI Global Quality Dividend Gro | CGQD/B | CI GL QUTY DIV GRWTH ETF | review_required_openfigi_resolves_different_security |
| CA29408C1005 | NVRO | NVRO MetaNVRO | RGO/XEUR | ENVIROGOLD GLOBAL LTD | review_required_openfigi_resolves_different_security |
| CA30219M1059 | NFLDF | Epic Gold Corp. | NFLDGBP,NFLDUSD | EXPLOITS DISCOVERY CORP | review_required_openfigi_resolves_different_security |
| CA33732U1093 | AUGB | First Trust Vest U.S. Equity B | AUGB/F | FT VT US EQ BF ETF-AUG | review_required_openfigi_resolves_different_security |
| CA40138D2014 | GDPY-B | Guardian Directed Premium Yiel | GGPY | GUARD DRT PREM YLD B | review_required_openfigi_resolves_different_security |
| CA45168X1006 | ID | Secure Blockchain Development  | IDCAD,IDEUR,IDGBX | IDENTILLECT TECHNOLOGIES COR | review_required_openfigi_resolves_different_security |
| CA5545151063 | DVX.P | Drummond Ventures Corp. | MACK,MKGSF | MACKAY GOLD & SILVER CORP | review_required_openfigi_resolves_different_security |
| CA69002L1067 | OZBKF | Valkea Resources Corp. | OZCAD | OUTBACK GOLDFIELDS CORP | review_required_openfigi_resolves_different_security |
| CA78460T1057 | SNCAF | AtkinsRéalis Group Inc | SNCCAD,SNCEUR,SNCGBP | SNC-LAVALIN GROUP INC | review_required_openfigi_resolves_different_security |
| CH0024666528 | HT5 | HT5 N LTD | 0QQI,1Z3,CNTL | CENTIEL AG | review_required_openfigi_resolves_different_security |
| CH0304280636 | HSRN | HelveticStar Holding Ag | HELHCHF,HELHEUR,HSR | HELVETIC STAR AG | review_required_openfigi_resolves_different_security |
| FI0009013429 | CYJBY | Hiab Oyj | CGCBVGBP,CGCBVGBX,CGCBVUSD | CARGOTEC OYJ-B SHARE | review_required_openfigi_resolves_different_security |
| FR0010383877 | ALTTI | Travel Technology Interactive  | ALTTIEUR | TTI | review_required_openfigi_resolves_different_security |
| FR0010781377 | MLAAE | Compagnie Aérienne Inter Régio | MLAAEEUR | CAIRE | review_required_openfigi_resolves_different_security |
| FR00140050Q2 | ALMND | Montagne et Neige Développemen | ALMNDP,MND1EUR | MND | review_required_openfigi_resolves_different_security |
| GB0003775664 | 67GX | ZIGUP PLC 5% CUM PRF 50P | NTGLN 5 PERP | REDDE NORTHGATE PLC | review_required_openfigi_resolves_different_security |
| GB0007655250 | 46IE | S & U PLC 6% CUM PRF #1 | SUSLN 6 PERP | S & U STORES PLC | review_required_openfigi_resolves_different_security |
| GB0007655474 | 47IE | S&U plc | SUSLN 3.9375 PERP | S & U STORES PLC | review_required_openfigi_resolves_different_security |
| GB00BJ9MHH56 | CYK | Cykel AI PLC | MUSTGBX | MUSTANG ENERGY PLC | review_required_openfigi_resolves_different_security |
| GRS534003009 | TRESTATES | TRESTATES | TRESTATE,TRESTEUR,TRESTY | TRADE ESTATES REAL ESTATE IN | review_required_openfigi_resolves_different_security |
| IE0007WMHDE3 | EUGD | HANetf ICAV - European Green D | 8GRT,ASWAD,ETFHGR8 | MAKING EUROPE GREAT AGAIN UC | review_required_openfigi_resolves_different_security |
| INE030P01017 | ALSL | Alacrity Securities Ltd | ALSE | RNIT AI SOLUTIONS LTD | review_required_openfigi_resolves_different_security |
| INE058F01019 | ALUFLUOR | Alufluoride Ltd | ALFD | ALUFLOURIDE LTD | review_required_openfigi_resolves_different_security |
| INE0M3I01029 | AMBEY | Ambey Laboratories Limited | DHANSA | DHANSA LABS LTD | review_required_openfigi_resolves_different_security |
| INE110Q01023 | STCORP | S & T Corporation Ltd | STCL | S&T CORP LTD | review_required_openfigi_resolves_different_security |
| INE130N01010 | UNIJOLL | Unijolly Investments Company L | UJI | UNI JOLLY INVESTMENT CO LTD | review_required_openfigi_resolves_different_security |
| INE188A01015 | FACT | Fertilizers and Chemicals Trav | FCT | FERTILISERS & CHEM TRAVANCR | review_required_openfigi_resolves_different_security |
| INE312B01027 | SJCORP | S J Corporation Ltd | SJCL | SJ CORP LTD | review_required_openfigi_resolves_different_security |
| INE626H01019 | ZBINTXPP | Binayak Tex Processors Ltd | BYT | BINAYAKA TEXTILE PROC LTD | review_required_openfigi_resolves_different_security |
| INE902G01016 | KGPETRO | KG Petrochem Ltd | KGP | KG PETROLEUM CHEM LTD | review_required_openfigi_resolves_different_security |
| INF209KC1134 | ABGSEC | Aditya Birla Sun Life CRISIL B | ACBRGLT | AB CRSL BRD GLT ETF | review_required_openfigi_resolves_different_security |
| INF247L01FC0 | MON50EQUAL | Motilal Oswal Nifty 50 Equal W | MONEWRG | MOT OSW NI 50 EQ WE ETF-RG | review_required_openfigi_resolves_different_security |
| INF247L01FK3 | MOMGF | Motilal Oswal Nifty India Manu | MONIMRG | MOT OSW NIF IND MAN ETF-RG | review_required_openfigi_resolves_different_security |
| INF247L01FP2 | MOTOUR | Motilal Oswal Nifty India Tour | MOSNTRG | MOTI OSW NIF IND TOU ETF-RG | review_required_openfigi_resolves_different_security |
| INF247L01FQ0 | MOMIDMTM | Motilal Oswal Nifty Midcap150  | MOMM5RG | MOTIL OS N MCP150 M50 ETF-RG | review_required_openfigi_resolves_different_security |
| INF247L01FR8 | MOALPHA50 | Motilal Oswal Nifty Alpha 50 E | MONA5RG | MOTI OSW NIF AI 50 ETF- RG | review_required_openfigi_resolves_different_security |
| INF247L01GJ3 | MOSERVICE | Motilal Oswal Nifty Services S | MONSSRG | MTIL OSWL NFTY SRVC SCTR ETF | review_required_openfigi_resolves_different_security |
| INF754K01TF1 | EMULTIMQ | Edelweiss Nifty500 Multicap Mo | EDENM50 | EDEL NF500 MLT MOM QLT50 ETF | review_required_openfigi_resolves_different_security |
| KE0000000547 | KENGEN | KENGEN CO. PLC | KEGC | KENYA ELECTRICITY GENERATING | review_required_openfigi_resolves_different_security |
| KYG2296A1094 | BRR | ProCap Financial, Inc. | CCCMEUR,CCCMUSD | COLUMBUS CIRCLE CAPITAL CO-A | review_required_openfigi_resolves_different_security |
| KYG8232Y1017 | PENG | Penguin Solutions, Inc. | SGH2EUR,SGH2GBP,SGH2USD | SMART GLOBAL HOLDINGS INC | review_required_openfigi_resolves_different_security |
| LK0036N00000 | CPRT.N0000 | KERNER HAUS GLOBAL SOLUTIONS P | CPRT | CEYLON PRINTERS PLC | review_required_openfigi_resolves_different_security |
| LK0113N00007 | LOLC.N0000 | L O L C HOLDINGS PLC | LOLC | LOLC HOLDINGS LTD | review_required_openfigi_resolves_different_security |
| LK0451N00001 | BPPL.N0000 | B P P L HOLDINGS PLC | BPPL | BPPL HOLDINGS LTD | review_required_openfigi_resolves_different_security |
| MA0000012247 | COSUMAR | Cosumar SA | CSR | COMPAGNIE SUCRERIE MAR RAFFI | review_required_openfigi_resolves_different_security |
| MYL2828OO001 | 2828 | CI Holdings Bhd | CIH | C.I. HOLDINGS BERHAD | review_required_openfigi_resolves_different_security |
| MYL4251OO004 | 4251 | I-Berhad | IBHD | I-BHD | review_required_openfigi_resolves_different_security |
| MYL5235SS008 | 5235SS | KLCC Property Holdings Bhd | KLCCSS | KLCCP STAPLED GROUP | review_required_openfigi_resolves_different_security |
| MYQ0245OO009 | 0245 | MN Holdings Berhad | MNHLDG | MN HOLDINGS BHD | review_required_openfigi_resolves_different_security |
| MYQ0296OO002 | 0296 | HE Group Berhad | HEGROUP | HE GROUP SDN BHD | review_required_openfigi_resolves_different_security |
| MYQ0327OO005 | 0327 | OB Holdings Berhad | OBHB | OB HOLDINGS BHD | review_required_openfigi_resolves_different_security |
| NL0015000DX5 | ATAI | AtaiBeckley Inc. | ATAI1EUR,ATAI1GBP,ATAI1USD | ATAI LIFE SCIENCES NV | review_required_openfigi_resolves_different_security |
| NL0015000H56 | BNJW | BANIJAY GROUP WARR | FLEW | FL ENTERTAINMENT N.V.-CW24 | review_required_openfigi_resolves_different_security |
| NO0013711721 | KMCP | KMC PROPERTIES | 0N0L,5FM0,BINT | BEVEST ASA | review_required_openfigi_resolves_different_security |
| PLDRD2400010 | DRF | Dr.Finance SA | D24,D241PLN,D24PLN | DORADCY24 SA | review_required_openfigi_resolves_different_security |
| SG1B56010922 | F13 | Fu Yu Corporation Ltd | FUYU,FUYVF | FU YU CORP LTD | review_required_openfigi_resolves_different_security |
| SG2B91959363 | 5RC | ES Group (Holdings) Limited | ESG | ES GROUP HOLDINGS LTD | review_required_openfigi_resolves_different_security |
| SGXE45420721 | 1Y1 | 9R Limited | 9R | 9R LTD | review_required_openfigi_resolves_different_security |
| SGXZ81555062 | NASO | Naples Soap Company, Inc. | GNS,GNSEUR,GNSGBP | GENIUS GROUP LTD | review_required_openfigi_resolves_different_security |
| TH0052B10Z09 | DV8 | DV8 Public Company Limited | ASTR,DVPCF | ASTRA ENTERPRISE PCL | review_required_openfigi_resolves_different_security |
| TREMEPT00012 | MEPET | MEPET METRO PETROL VE TESİSLER | MEPETTRY,MOLA,MOLAY | BREAK MOLA TURIZM YATIRIMLAR | review_required_openfigi_resolves_different_security |
| US00775Y7287 | RAYD | Rayliant Quantitative Develope | RWLC | RAYLN NXTGN MU US EQ ETF-USD | review_required_openfigi_resolves_different_security |
| US05968L1026 | CIB | Grupo Cibest S.A. | BXK,CIBEUR,CIBGBP | BANCOLOMBIA S.A.-SPONS ADR | review_required_openfigi_resolves_different_security |
| US06690B1153 | PNST | Pinstripes Holdings Inc | PNSWQ | BANYAN ACQUISITION CO -CW23 | review_required_openfigi_resolves_different_security |
| US20731J1025 | PRHI | Presurance Holdings, Inc. | CNFREUR,CNFRUSD | CONIFER HOLDINGS INC | review_required_openfigi_resolves_different_security |
| US2075972040 | CNLTL | The Connecticut Light and Powe | ES 1.9 PERP 1947 | CONN LT & PWR | review_required_openfigi_resolves_different_security |
| US2075973030 | CNLTN | The Connecticut Light and Powe | ES 2 PERP 1947 | CONN LT & PWR | review_required_openfigi_resolves_different_security |
| US2075974020 | CNPWP | The Connecticut Light and Powe | ES 2.04 PERP 1949 | CONN LT & PWR | review_required_openfigi_resolves_different_security |
| US2075975019 | CNLPM | The Connecticut Light and Powe | ES 2.06 PERP 54E | CONN LT & PWR | review_required_openfigi_resolves_different_security |
| US2075976009 | CNLTP | The Connecticut Light and Powe | ES 2.2 PERP 1949 | CONN LT & PWR | review_required_openfigi_resolves_different_security |
| US2075977098 | CNLPL | The Connecticut Light and Powe | ES 3.24 PERP 68G | CONN LT & PWR | review_required_openfigi_resolves_different_security |
| US2075977742 | CNTHP | The Connecticut Light and Powe | ES 6.56 PERP 1968 | CONN LT & PWR | review_required_openfigi_resolves_different_security |
| US2075977908 | CNTHN | The Connecticut Light and Powe | ES 4.96 PERP 1958 | CONN LT & PWR | review_required_openfigi_resolves_different_security |
| US2075978245 | CNLHO | The Connecticut Light and Powe | ES 4.5 PERP 1956 | CONN LT & PWR | review_required_openfigi_resolves_different_security |
| US25460G8078 | JDST | Direxion Daily Junior Gold Min | JDSTGBP,JDSTUSD | DIR DLY JUN GM IB 2X ETF-USD | review_required_openfigi_resolves_different_security |
| US2668881061 | DUERF | Dürr Aktiengesellschaft | DUEB,DURYY,DURYYEUR | DUERR AG -UNSP ADR | review_required_openfigi_resolves_different_security |
| US26922B7091 | IDME | International Drawdown Managed | IDUB | APTUS INTL ENHAN YLD ETF | review_required_openfigi_resolves_different_security |
| US26923H3093 | BWET | ETF Managers Group Commodity T | BWETUSD | BREAKWAVE TANKER SHIP ETF | review_required_openfigi_resolves_different_security |
| US2972842007 | ESLOF | EssilorLuxottica Société anony | ESLC,ESLOY,ESLOYEUR | ESSILORLUXOT-UNSPON ADR | review_required_openfigi_resolves_different_security |
| US3134006408 | FMCKI | Federal Hme 6.55 Pf | FMCC 6.55 PERP Y | FREDDIE MAC | review_required_openfigi_resolves_different_security |
| US3134006739 | FMCKM | Federal Home Ln Mtg | FMCC 5.57 PERP V | FREDDIE MAC | review_required_openfigi_resolves_different_security |
| US3134008222 | FREJP | Federal Home 5.30% | FMCC 5.3 PERP | FREDDIE MAC | review_required_openfigi_resolves_different_security |
| US3135866040 | FNMFM | Federal National Mortgage Asso | FNMA 5.1 PERP +E | FANNIE MAE | review_required_openfigi_resolves_different_security |
| US3135868103 | FNMFO | Federal National Mortgage Asso | FNMA 5.375 PERP | FANNIE MAE | review_required_openfigi_resolves_different_security |
| US33739Q3092 | FDIV | MarketDesk Focused U.S. Divide | HISF | FIRST TR HI INC STRAT FOC | review_required_openfigi_resolves_different_security |
| US44987J1034 | IOOFF | Insignia Financial Ltd | IOOFY | IOOF HOLDINGS LTD-SPON ADR | review_required_openfigi_resolves_different_security |
| US48135NTM82 | VYLD | Inverse VIX Short-Term Futures | JPM 5 08/28/28 MTn | JPMORGAN CHASE FINANCIAL | review_required_openfigi_resolves_different_security |
| US48837P1021 | KMGH | Kemiao Garment Holding Group | WGSK | WORLD GU SHAN KANG HOLDING G | review_required_openfigi_resolves_different_security |
| US6261881063 | MURGF | Münchener Rückversicherungs-Ge | MURGY,MURGYEUR,MURGYUSD | MUENCHENER RUECK-UNSPON ADR | review_required_openfigi_resolves_different_security |
| US6494451031 | FLG | Flagstar Bank, N.A. | NYCBEUR,NYCBGBP,NYCBUSD | NEW YORK COMMUNITY BANCORP | review_required_openfigi_resolves_different_security |
| US67073S3076 | CIMG | CIMG Inc | NUZEEUR,NUZEGBP | NUZEE INC | review_required_openfigi_resolves_different_security |
| US69290X1019 | PDLB | Ponce Financial Group Inc | PDLBUSD | PDL COMMUNITY BANCORP | review_required_openfigi_resolves_different_security |
| US83548F2002 | SONM | DNA X, Inc. | SONMEUR,SONMGBP,SONMUSD | SONIM TECHNOLOGIES INC | review_required_openfigi_resolves_different_security |
| XS0493723968 | PCFBF | Pacific Basin Shipping Limited | PACBAS 1.75 04/12/16 | PB ISSUER NO 2 LTD | review_required_openfigi_resolves_different_security |
| ZAE000261392 | UMMIEA | Absa re Momentum International | UBS 0 08/22/28 @ | UBS AG LONDON | review_required_openfigi_resolves_different_security |

# Delisting-candidate report

Generated: 2026-11-09T17:49:09Z

**delisting_detected: True**

Markets checked: US, ASX, NSE_IN, US_NASDAQ_DELETES
Markets skipped: TSE (fetch failed: XLRDError); BSE_IN (fetch failed: JSONDecodeError: Expecting value: line 1 column 1 (char 0))

Candidates: 247 (delisted=4, suspended=0, master_absent=243)

Detection only — verify each (delisting vs rename vs SME/suspended) and apply via the override/verify pipeline. BSE ListofScripData Delisted rows are drop-ready; Nasdaq Trader Delete rows require manual corporate-action review; `master_absent` need rename-vs-delisting verification; `suspended` are kept by policy (can resume).

| Exchange | Ticker | Classification | Name | ISIN |
|---|---|---|---|---|
| ASX | 5EA | master_absent | 5E ADVANCED MATERIALS INC. | AU0000186207 |
| ASX | ADG | master_absent | ADELONG GOLD LIMITED | AU0000248288 |
| ASX | AQI | master_absent | ALICANTO MINERALS LIMITED | AU000000AQI2 |
| ASX | AS2 | master_absent | ASKARI METALS LIMITED | AU0000153256 |
| ASX | ASK | master_absent | ABACUS STORAGE KING | AU0000286213 |
| ASX | AUK | master_absent | Aumake Ltd | AU0000123432 |
| ASX | BCB | master_absent | BOWEN COKING COAL LIMITED | AU000000BCB5 |
| ASX | BMG | master_absent | BMG RESOURCES LIMITED | AU000000BMG3 |
| ASX | BP8 | master_absent | BPH GLOBAL LTD | AU000000SCU9 |
| ASX | BXN | master_absent | BIOXYNE LIMITED | AU000000BXN6 |
| ASX | C1X | master_absent | COSMOS EXPLORATION LIMITED | AU0000178113 |
| ASX | CL8 | master_absent | CL8 HOLDINGS LIMITED | AU000000CL86 |
| ASX | CR9 | master_absent | CORELLA RESOURCES LTD | AU0000147811 |
| ASX | CY5 | master_absent | CYGNUS METALS LIMITED | AU000000CY57 |
| ASX | DMG | master_absent | DRAGON MOUNTAIN GOLD LIMITED | AU000000DMG9 |
| ASX | EEL | master_absent | Enrg Elements Ltd | AU0000234676 |
| ASX | EMS | master_absent | Eastern Metals Ltd | AU0000173304 |
| ASX | ENN | master_absent | ELANOR INVESTORS GROUP | AU000000ENN1 |
| ASX | ERM | master_absent | EMMERSON RESOURCES LIMITED | AU000000ERM4 |
| ASX | HCD | master_absent | HYDROCARBON DYNAMICS LIMITED | AU0000074742 |
| ASX | HGO | master_absent | HILLGROVE RESOURCES LIMITED | AU000000HGO6 |
| ASX | HHR | master_absent | HARTSHEAD RESOURCES NL | AU0000154148 |
| ASX | HNG | master_absent | Hancock & Gore Ltd | AU000000HNG8 |
| ASX | HRZ | master_absent | HORIZON MINERALS LIMITED | AU0000053373 |
| ASX | IR1 | master_absent | IRIS METALS LIMITED | AU0000166134 |
| ASX | IRX | master_absent | Inhalerx Ltd | AU0000179475 |
| ASX | JAY | master_absent | JAYRIDE GROUP LIMITED | AU000000JAY4 |
| ASX | LLM | master_absent | LOYAL METALS LTD | AU0000399131 |
| ASX | MAUCA | master_absent | Magnetic Resources NL | AU0000MAUCA0 |
| ASX | MCE | master_absent | MATRIX COMPOSITES & ENGINEERING LIMITED | AU000000MCE6 |
| ASX | MFGO | master_absent | Magellan Financial Group Ltd | AU0000215808 |
| ASX | MRI | master_absent | My Rewards International Ltd | AU0000187940 |
| ASX | MRR | master_absent | MINREX RESOURCES LIMITED | AU000000MRR6 |
| ASX | NFNG | master_absent | Nufarm Finance (NZ) Ltd | NZFCND0004S9 |
| ASX | NSB | master_absent | NEUROSCIENTIFIC BIOPHARMACEUTICALS LTD | AU0000012973 |
| ASX | OLH | master_absent | OLDFIELDS HOLDINGS LIMITED | AU000000OLH6 |
| ASX | PGO | master_absent | PACGOLD LIMITED | AU0000158636 |
| ASX | PNM | master_absent | PACIFIC NICKEL MINES LIMITED | AU0000123010 |
| ASX | PVW | master_absent | PVW Resources Ltd | AU0000135188 |
| ASX | QOR | master_absent | QORIA LIMITED | AU0000278491 |
| ASX | RA2 | master_absent | RESIMAC PREMIER SERIES 2020-1 |  |
| ASX | RIL | master_absent | REDIVIUM LIMITED | AU0000310211 |
| ASX | SHP | master_absent | South HARZ Potash Ltd | AU0000151680 |
| ASX | TI1 | master_absent | Tombador Iron Ltd | AU0000107211 |
| ASX | TOE | master_absent | TORO ENERGY LIMITED | AU000000TOE6 |
| ASX | WAK | master_absent | WA KAOLIN LIMITED | AU0000111247 |
| ASX | WFE | master_absent | WOLFE ENERGY LIMITED | AU0000458358 |
| NASDAQ | AACB | master_absent | Artius II Acquisition Inc. Class A Ordin | KYG0509J1159 |
| NASDAQ | ABVE | master_absent | Above Food Ingredients Inc. Common Stock | CA00373V1004 |
| NASDAQ | ADIL | master_absent | Adial Pharmaceuticals Inc | US00688A3041 |
| NASDAQ | ADTX | master_absent | Aditxt Inc. | US0070258696 |
| NASDAQ | AFBI | master_absent | Affinity Bancshares Inc | US00832E1038 |
| NASDAQ | AIXC | master_absent | AIxCrypto Holdings, Inc. | US74754R3012 |
| NASDAQ | ALCY | master_absent | Alchemy Investments Acquisition Corp 1 C | KYG0232F1090 |
| NASDAQ | ALDF | master_absent | Aldel Financial II Inc. Class A Ordinary | KYG015581088 |
| NASDAQ | ALOT | master_absent | AstroNova Inc | US04638F1084 |
| NASDAQ | ANSC | master_absent | Agriculture & Natural Solutions Acquisit | KYG0131Y1008 |
| NASDAQ | AREB | master_absent | American Rebel Holdings Inc | US02919L8853 |
| NASDAQ | ASNS | master_absent | Actelis Networks Inc. | US00503R5081 |
| NASDAQ | ATAI | master_absent | AtaiBeckley Inc. | NL0015000DX5 |
| NASDAQ | ATII | master_absent | Archimedes Tech SPAC Partners II Co. Ord | KYG045371096 |
| NASDAQ | BAYA | master_absent | Bayview Acquisition Corp Class A Ordinar | KY07323B1007 |
| NASDAQ | BCAR | master_absent | D. Boral ARC Acquisition I Corp. Class A | VGG2616F1018 |
| NASDAQ | BCOW | master_absent | 1895 of Wisconsin Inc Bancorp | US28253R1059 |
| NASDAQ | BLFS | master_absent | BioLife Solutions Inc | US09062W2044 |
| NASDAQ | BNBX | master_absent | BNB Plus Corp. | US03815U6073 |
| NASDAQ | BOCN | master_absent | Blue Ocean Acquisition Corp | KYG1330L1059 |
| NASDAQ | BRNS | master_absent | Barinthus Biotherapeutics plc | US91864C1071 |
| NASDAQ | BWMN | delisted | Bowman Consulting Group Ltd | US1030021018 |
| NASDAQ | CCIX | master_absent | Churchill Capital Corp IX | KYG213011094 |
| NASDAQ | CCRN | master_absent | Cross Country Healthcare Inc | US2274831047 |
| NASDAQ | CCXI | master_absent | Churchill Capital Corp XI | KYG2131A1085 |
| NASDAQ | CEPT | master_absent | Cantor Equity Partners II, Inc. | KYG1827K1076 |
| NASDAQ | CIMG | master_absent | CIMG Inc | US67073S3076 |
| NASDAQ | CIZN | master_absent | Citizens Holding Company | US1747151025 |
| NASDAQ | CNTA | master_absent | Centessa Pharmaceuticals plc | US1523091007 |
| NASDAQ | COLA | master_absent | Columbus Acquisition Corp Ordinary Share | KYG2295P1072 |
| NASDAQ | CPRX | master_absent | Catalyst Pharmaceuticals Inc | US14888U1016 |
| NASDAQ | CREG | master_absent | Smart Powerr Corp | US1689133098 |
| NASDAQ | CRNX | master_absent | Crinetics Pharmaceuticals Inc | US22663K1079 |
| NASDAQ | CULL | master_absent | Cullman Bancorp Inc. | US2301531081 |
| NASDAQ | DEVS | master_absent | DevvStream Corp. Common Stock | CA25189R1001 |
| NASDAQ | DRCT | master_absent | Direct Digital Holdings Inc | US25461T3032 |
| NASDAQ | DYNC | master_absent | Dynamix Corporation - Class A Ordinary S | KYG2949D1126 |
| NASDAQ | EA | master_absent | Electronic Arts Inc | US2855121099 |
| NASDAQ | ELSE | master_absent | Electro-Sensors Inc | US2852331022 |
| NASDAQ | EMPG | master_absent | Empro Group Inc. Ordinary shares | KYG3041J1067 |
| NASDAQ | ESPR | master_absent | Esperion Therapeutics Inc | US29664W1053 |
| NASDAQ | EVTV | master_absent | Envirotech Vehicles Inc | US29414V2097 |
| NASDAQ | FATBB | master_absent | FAT Brands Inc | US30258N6004 |
| NASDAQ | FBRX | master_absent | Forte Biosciences Inc | US34962G2084 |
| NASDAQ | FEED | master_absent | ENvue Medical Inc. | US63008J8844 |
| NASDAQ | FGMC | master_absent | FG Merger II Corp. Common stock | US30334J1025 |
| NASDAQ | FGNX | master_absent | FG Nexus Inc. | US30329Y4035 |
| NASDAQ | FLZH | master_absent | Flash Sports & Media Holdings, Inc. | US91704K3014 |
| NASDAQ | FSEA | master_absent | First Seacoast Bancorp | US33631F1049 |
| NASDAQ | FTRK | master_absent | FAST TRACK GROUP | KYG333801093 |
| NASDAQ | GAMB | master_absent | Grandstand Limited | JE00BL970N11 |
| NASDAQ | GBNY | master_absent | Generations Bancorp NY Inc | US37149G1085 |
| NASDAQ | GCTK | master_absent | GlucoTrack Inc | US45824Q8042 |
| NASDAQ | GLBZ | master_absent | Glen Burnie Bancorp | US3774071019 |
| NASDAQ | GNTA | master_absent | Genenta Science S.p.A. | US36870W1009 |
| NASDAQ | GOCO | master_absent | GoHealth Inc. | US38046W2044 |
| NASDAQ | GV | master_absent | Visionary Education Technology Holdings  | CA92838F2008 |
| NASDAQ | HEPA | master_absent | Hepion Pharmaceuticals Inc | US4268971045 |
| NASDAQ | HODO | master_absent | House of Doge Inc. - Common Stock | US1048133086 |
| NASDAQ | HWH | master_absent | HWH International Inc | US44852G1013 |
| NASDAQ | INDV | master_absent | Indivior PLC Ordinary Shares | GB00BN4HT335 |
| NASDAQ | INM | master_absent | InMed Pharmaceuticals Inc | CA4576377002 |
| NASDAQ | ITRM | master_absent | Iterum Therapeutics PLC | IE000TTOOBX0 |
| NASDAQ | KVAC | master_absent | Keen Vision Acquisition Corporation Ordi | VGG524431191 |
| NASDAQ | LBRDA | master_absent | Liberty Broadband Srs A | US5303071071 |
| NASDAQ | LBRDK | master_absent | Liberty Broadband Srs C | US5303073051 |
| NASDAQ | LESL | master_absent | Leslies Inc | US5270642086 |
| NASDAQ | LNAI | master_absent | Lunai Bioworks Inc. | US29350E3027 |
| NASDAQ | LPRO | master_absent | Open Lending Corp | US68373J1043 |
| NASDAQ | LYRA | master_absent | Lyra Therapeutics Inc | US55234L1052 |
| NASDAQ | MAMK | master_absent | MaxsMaking Inc. Class A Ordinary Shares | VGG6053R1064 |
| NASDAQ | MAPS | master_absent | WM Technology Inc | US92971A1097 |
| NASDAQ | MAXN | master_absent | Maxeon Solar Technologies Ltd | SGXZ57724486 |
| NASDAQ | MCTA | delisted | Charming Medical Limited Class A Ordinar | VGG205281089 |
| NASDAQ | MEHA | master_absent | Functional Brands, Inc. Common Stock | US3609481037 |
| NASDAQ | MGPI | master_absent | MGP Ingredients Inc | US55303J1060 |
| NASDAQ | MLAC | master_absent | Mountain Lake Acquisition Corp. Class A  |  |
| NASDAQ | MLEC | master_absent | Moolec Science SA Ordinary Shares | KYG6223S1259 |
| NASDAQ | MRAI | master_absent | Marpai Inc | US5713542083 |
| NASDAQ | NATH | master_absent | Nathans Famous Inc | US6323471002 |
| NASDAQ | NCSM | master_absent | NCS Multistage Holdings Inc | US6288772014 |
| NASDAQ | NFBK | master_absent | Northfield Bancorp Inc | US66611T1088 |
| NASDAQ | NHIC | master_absent | NewHold Investment Corp III Class A Ordi | KYG6486E1026 |
| NASDAQ | NSTS | master_absent | NSTS Bancorp Inc | US6293JP1094 |
| NASDAQ | NUTR | master_absent | Nusatrip Incorporated Common Stock | US67119K1025 |
| NASDAQ | NUVL | master_absent | Nuvalent Inc | US6707031075 |
| NASDAQ | NVNI | master_absent | Nvni Group Limited Ordinary Shares | KYG507161282 |
| NASDAQ | NVVE | master_absent | Nuvve Holding Corp | US67079Y4070 |
| NASDAQ | OLPX | master_absent | Olaplex Holdings Inc | US6793691089 |
| NASDAQ | ORGN | master_absent | Origin Materials Inc | US68622D1063 |
| NASDAQ | ORIS | master_absent | Oriental Rise Holdings Limited Ordinary  | KYG6781A1105 |
| NASDAQ | OSRH | master_absent | OSR Holdings Inc | US68840D1028 |
| NASDAQ | PAIYY | master_absent | Aesthetic Medical International Holdings | US00809M1045 |
| NASDAQ | PIRBF | master_absent | Piraeus Bank S.A. | GRS831003009 |
| NASDAQ | PLTS | master_absent | Platinum Analytics Cayman Limited Class  | KYG712641086 |
| NASDAQ | PSKY | master_absent | Paramount Skydance Corporation Class B C | US69932A2042 |
| NASDAQ | PTNM | master_absent | Pitanium Ltd | VGG7111A1012 |
| NASDAQ | QETA | master_absent | Quetta Acquisition Corporation Common St | US74841A1051 |
| NASDAQ | QMMM | master_absent | QMMM Holdings Limited Ordinary Shares | KYG7309R1149 |
| NASDAQ | QRVO | master_absent | Qorvo Inc | US74736K1016 |
| NASDAQ | QVC | master_absent | QVC Group, Inc. - Common Stock |  |
| NASDAQ | QVCAQ | master_absent | QVC Group Inc | US74915M6057 |
| NASDAQ | REE | master_absent | Ree Automotive Holding Inc | IL0011786154 |
| NASDAQ | RITR | master_absent | Reitar Logtech Holdings Limited Ordinary | KYG7486B1068 |
| NASDAQ | ROKU | delisted | Roku Inc | US77543R1023 |
| NASDAQ | RVPH | master_absent | Reviva Pharmaceuticals Holdings Inc. | US76152G2093 |
| NASDAQ | SDM | master_absent | Smart Digital Group Limited Ordinary Sha | KYG5006S1049 |
| NASDAQ | SDST | master_absent | Stardust Power Inc. | US8549362007 |
| NASDAQ | SGMO | master_absent | Sangamo Therapeutics Inc | US8006771062 |
| NASDAQ | SGRP | master_absent | SPAR Group Inc | US7849331035 |
| NASDAQ | SKYT | master_absent | Skywater Technology Inc | US83089J1088 |
| NASDAQ | SLP | master_absent | Simulations Plus Inc | US8292141053 |
| NASDAQ | SNBR | master_absent | Sleep Number Corp | US83125X1037 |
| NASDAQ | SNYR | master_absent | Synergy CHC Corp. Common Stock | US87165D2080 |
| NASDAQ | SOBR | master_absent | Sobr Safe Inc | US8335924051 |
| NASDAQ | SPKL | master_absent | Spark I Acquisition Corp. Class A Ordina | KYG8316B1005 |
| NASDAQ | STRS | master_absent | Stratus Properties Inc | US8631672016 |
| NASDAQ | TALK | master_absent | Talkspace Inc | US87427V1035 |
| NASDAQ | TBPH | master_absent | Theravance Biopharma Inc | KYG8807B1068 |
| NASDAQ | TBRG | master_absent | TruBridge Inc. | US2053061030 |
| NASDAQ | TCBI | master_absent | Texas Capital Bancshares Inc | US88224Q1076 |
| NASDAQ | TIRX | master_absent | Tian Ruixiang Holdings Ltd | KYG8884K1444 |
| NASDAQ | TWNP | master_absent | Twin Hospitality Group Inc. | US9016431069 |
| NASDAQ | UHGWW | master_absent | United Homes Group Inc. | US91060H1086 |
| NASDAQ | UOKA | master_absent | MDJM Ltd | KYG592901253 |
| NASDAQ | VACH | master_absent | Voyager Acquisition Corp |  |
| NASDAQ | VRME | master_absent | VerifyMe Inc | US92346X2062 |
| NASDAQ | VSEE | master_absent | VSee Health, Inc. | US92919Y1029 |
| NASDAQ | VSTD | master_absent | Vestand Inc. | US98740Y3027 |
| NASDAQ | VXRT | master_absent | Vaxart Inc | US92243A2006 |
| NASDAQ | WALD | master_absent | Waldencast Acquisition Corp | KYG9460C1006 |
| NASDAQ | WBD | master_absent | Warner Bros Discovery Inc | US9344231041 |
| NASDAQ | WKEY | master_absent | Wisekey International Holding AG | US97727L4086 |
| NASDAQ | XOMA | master_absent | XOMA Corp | US98419J2069 |
| NASDAQ | ZENV | master_absent | Zenvia Inc | KYG9889V1014 |
| NASDAQ | ZSPC | master_absent | zSpace, Inc. Common stock | US98980W1071 |
| NASDAQ | ZTEK | master_absent | Zentek Ltd. | CA98942X1024 |
| NSE_IN | AURIGROW | master_absent | Auri Grow India Limited | INE925Y01036 |
| NSE_IN | JBCHEPHARM | master_absent | JB Chemicals & Pharmaceuticals Limited | INE572A01036 |
| NSE_IN | SABEVENTS | master_absent | Sab Events & Governance Now Media Limite | INE860T01019 |
| NYSE | ACA | master_absent | Arcosa Inc | US0396531008 |
| NYSE | ACVA | master_absent | ACV Auctions Inc. | US00091G1040 |
| NYSE | AMZE | master_absent | Amaze Holdings Inc | US35804X2009 |
| NYSE | ARI | delisted | Apollo Commercial Real Estate Finance In | US03762U1051 |
| NYSE | AVB | master_absent | AvalonBay Communities Inc | US0534841012 |
| NYSE | AVNS | master_absent | Avanos Medical Inc | US05350V1061 |
| NYSE | AXIA | master_absent | AXIA Energia | US15234Q2075 |
| NYSE | BBBY | master_absent | Bed Bath & Beyond, Inc. | US6903701018 |
| NYSE | BBU | master_absent | Brookfield Business Partners LP | BMG162341090 |
| NYSE | BLD | master_absent | Topbuild Corp | US89055F1030 |
| NYSE | BZH | master_absent | Beazer Homes USA Inc | US07556Q8814 |
| NYSE | CBZ | master_absent | CBIZ Inc | US1248051021 |
| NYSE | CCO | master_absent | Clear Channel Outdoor Holdings Inc | US18453H1068 |
| … | … | … | (+47 more) | |

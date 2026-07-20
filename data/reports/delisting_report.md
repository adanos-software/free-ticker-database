# Delisting-candidate report

Generated: 2026-07-20T10:30:13Z

**delisting_detected: True**

Markets checked: US, TSE, ASX, NSE_IN
Markets skipped: BSE_IN (fetch failed: JSONDecodeError: Expecting value: line 3 column 1 (char 4))

Candidates: 643 (delisted=0, suspended=0, master_absent=643)

Detection only — verify each (delisting vs rename vs SME/suspended) and apply via the override/verify pipeline. `delisted` (BSE authoritative) are drop-ready; `master_absent` need rename-vs-delisting verification; `suspended` are kept by policy (can resume).

| Exchange | Ticker | Classification | Name | ISIN |
|---|---|---|---|---|
| ASX | 5EA | master_absent | 5E ADVANCED MATERIALS INC. | AU0000186207 |
| ASX | ADG | master_absent | ADELONG GOLD LIMITED | AU0000248288 |
| ASX | AQI | master_absent | ALICANTO MINERALS LIMITED | AU000000AQI2 |
| ASX | ASK | master_absent | ABACUS STORAGE KING | AU0000286213 |
| ASX | AUK | master_absent | Aumake Ltd | AU0000123432 |
| ASX | BCB | master_absent | BOWEN COKING COAL LIMITED | AU000000BCB5 |
| ASX | BMG | master_absent | BMG RESOURCES LIMITED | AU000000BMG3 |
| ASX | BXN | master_absent | BIOXYNE LIMITED | AU000000BXN6 |
| ASX | CR9 | master_absent | CORELLA RESOURCES LTD | AU0000147811 |
| ASX | EEL | master_absent | Enrg Elements Ltd | AU0000234676 |
| ASX | EMS | master_absent | Eastern Metals Ltd | AU0000173304 |
| ASX | ERM | master_absent | EMMERSON RESOURCES LIMITED | AU000000ERM4 |
| ASX | HCD | master_absent | HYDROCARBON DYNAMICS LIMITED | AU0000074742 |
| ASX | HHR | master_absent | HARTSHEAD RESOURCES NL | AU0000154148 |
| ASX | HNG | master_absent | Hancock & Gore Ltd | AU000000HNG8 |
| ASX | HRZ | master_absent | HORIZON MINERALS LIMITED | AU0000053373 |
| ASX | IRX | master_absent | Inhalerx Ltd | AU0000179475 |
| ASX | MAUCA | master_absent | Magnetic Resources NL | AU0000MAUCA0 |
| ASX | MFGO | master_absent | Magellan Financial Group Ltd | AU0000215808 |
| ASX | MRI | master_absent | My Rewards International Ltd | AU0000187940 |
| ASX | NFNG | master_absent | Nufarm Finance (NZ) Ltd | NZFCND0004S9 |
| ASX | PVW | master_absent | PVW Resources Ltd | AU0000135188 |
| ASX | RA2 | master_absent | RESIMAC PREMIER SERIES 2020-1 |  |
| ASX | SHP | master_absent | South HARZ Potash Ltd | AU0000151680 |
| ASX | TI1 | master_absent | Tombador Iron Ltd | AU0000107211 |
| ASX | TOE | master_absent | TORO ENERGY LIMITED | AU000000TOE6 |
| ASX | WFE | master_absent | WOLFE ENERGY LIMITED | AU0000458358 |
| NASDAQ | ABVE | master_absent | Above Food Ingredients Inc. Common Stock | CA00373V1004 |
| NASDAQ | ADTX | master_absent | Aditxt Inc.  | US0070258696 |
| NASDAQ | ALCY | master_absent | Alchemy Investments Acquisition Corp 1 C | KYG0232F1090 |
| NASDAQ | APM | master_absent | Aptorum Group Ltd Class A | KYG6096M1069 |
| NASDAQ | AREB | master_absent | American Rebel Holdings Inc | US02919L8853 |
| NASDAQ | ASNS | master_absent | Actelis Networks Inc. | US00503R5081 |
| NASDAQ | ATLN | master_absent | Atlantic International Corp. Common Stoc | US0485921094 |
| NASDAQ | ATON | master_absent | Alpha Compute Corp | VGG7185A1369 |
| NASDAQ | BAYA | master_absent | Bayview Acquisition Corp Class A Ordinar | KY07323B1007 |
| NASDAQ | BCOW | master_absent | 1895 of Wisconsin Inc Bancorp | US28253R1059 |
| NASDAQ | BNBX | master_absent | BNB Plus Corp. | US03815U6073 |
| NASDAQ | CAEP | master_absent | Cantor Equity Partners III, Inc. Class A | KYG1828A1085 |
| NASDAQ | CEPT | master_absent | Cantor Equity Partners II, Inc. | KYG1827K1076 |
| NASDAQ | CIMG | master_absent | CIMG Inc | US67073S3076 |
| NASDAQ | CIZN | master_absent | Citizens Holding Company | US1747151025 |
| NASDAQ | CNTA | master_absent | Centessa Pharmaceuticals plc | US1523091007 |
| NASDAQ | CPRX | master_absent | Catalyst Pharmaceuticals Inc | US14888U1016 |
| NASDAQ | CPTAF | master_absent | Captivision Inc | KYG189321063 |
| NASDAQ | CULL | master_absent | Cullman Bancorp Inc. | US2301531081 |
| NASDAQ | DEVS | master_absent | DevvStream Corp. Common Stock | CA25189R1001 |
| NASDAQ | ESPR | master_absent | Esperion Therapeutics Inc | US29664W1053 |
| NASDAQ | EVTV | master_absent | Envirotech Vehicles Inc | US29414V2097 |
| NASDAQ | FATBB | master_absent | FAT Brands Inc | US30258N6004 |
| NASDAQ | FGMC | master_absent | FG Merger II Corp. Common stock | US30334J1025 |
| NASDAQ | GBNY | master_absent | Generations Bancorp NY Inc | US37149G1085 |
| NASDAQ | GIG | master_absent | GigCapital7 Corp. Class A Ordinary Share | US37518P1012 |
| NASDAQ | GLBZ | master_absent | Glen Burnie Bancorp | US3774071019 |
| NASDAQ | GOCO | master_absent | GoHealth Inc. | US38046W2044 |
| NASDAQ | ISRL | master_absent | Israel Acquisitions Corp Class A | KYG496671010 |
| NASDAQ | ITRM | master_absent | Iterum Therapeutics PLC | IE000TTOOBX0 |
| NASDAQ | LIXT | master_absent | Lixte Biotechnology Holdings Inc | US5393192027 |
| NASDAQ | LOKV | master_absent | Live Oak Acquisition Corp. V Class A Ord | KYG5509P1028 |
| NASDAQ | LYRA | master_absent | Lyra Therapeutics Inc | US55234L1052 |
| NASDAQ | MAPS | master_absent | WM Technology Inc | US92971A1097 |
| NASDAQ | MAXN | master_absent | Maxeon Solar Technologies Ltd | SGXZ57724486 |
| NASDAQ | MEHA | master_absent | Functional Brands, Inc. Common Stock | US3609481037 |
| NASDAQ | MLAC | master_absent | Mountain Lake Acquisition Corp. Class A  |  |
| NASDAQ | MRAI | master_absent | Marpai Inc | US5713542083 |
| NASDAQ | MSW | master_absent | Ming Shing Group Holdings Limited Ordina | KYG614401068 |
| NASDAQ | NUVL | master_absent | Nuvalent Inc | US6707031075 |
| NASDAQ | OLPX | master_absent | Olaplex Holdings Inc | US6793691089 |
| NASDAQ | ORGN | master_absent | Origin Materials Inc | US68622D1063 |
| NASDAQ | ORIS | master_absent | Oriental Rise Holdings Limited Ordinary  | KYG6781A1105 |
| NASDAQ | PAIYY | master_absent | Aesthetic Medical International Holdings | US00809M1045 |
| NASDAQ | PELI | master_absent | Pelican Acquisition Corporation Ordinary | KYG6993G1038 |
| NASDAQ | PIRBF | master_absent | Piraeus Bank S.A. | GRS831003009 |
| NASDAQ | PTNM | master_absent | Pitanium Ltd | VGG7111A1012 |
| NASDAQ | QVCAQ | master_absent | QVC Group Inc | US74915M6057 |
| NASDAQ | RAAQ | master_absent | Real Asset Acquisition Corp. | KYG739441031 |
| NASDAQ | REE | master_absent | Ree Automotive Holding Inc | IL0011786154 |
| NASDAQ | RVPH | master_absent | Reviva Pharmaceuticals Holdings Inc. | US76152G2093 |
| NASDAQ | SDM | master_absent | Smart Digital Group Limited Ordinary Sha | KYG5006S1049 |
| NASDAQ | SGMO | master_absent | Sangamo Therapeutics Inc | US8006771062 |
| NASDAQ | SNBR | master_absent | Sleep Number Corp | US83125X1037 |
| NASDAQ | SSSS | master_absent | SuRo Capital Corp | US86887Q1094 |
| NASDAQ | SUUN | master_absent | PowerBank Corporation Common Stock | CA73933V1004 |
| NASDAQ | SVAC | master_absent | Spring Valley Acquisition Corp. III | KYG8377R1011 |
| NASDAQ | TBRG | master_absent | TruBridge Inc. | US2053061030 |
| NASDAQ | TIRX | master_absent | Tian Ruixiang Holdings Ltd | KYG8884K1444 |
| NASDAQ | TWNP | master_absent | Twin Hospitality Group Inc. | US9016431069 |
| NASDAQ | UBXG | master_absent | U-BX Technology Ltd. Ordinary Shares | KYG9161K1206 |
| NASDAQ | UHGWW | master_absent | United Homes Group Inc. | US91060H1086 |
| NASDAQ | UOKA | master_absent | MDJM Ltd | KYG592901253 |
| NASDAQ | VACH | master_absent | Voyager Acquisition Corp |  |
| NASDAQ | VVPR | master_absent | VivoPower International PLC | GB00BD3VDH82 |
| NASDAQ | VXRT | master_absent | Vaxart Inc | US92243A2006 |
| NASDAQ | WAI | master_absent | Top KingWin Ltd | KYG8923U1296 |
| NASDAQ | WORX | master_absent | Scworx Corp | US78396V3078 |
| NASDAQ | WTO | master_absent | UTime Limited | KYG9411M1400 |
| NASDAQ | XOMA | master_absent | XOMA Corp | US98419J2069 |
| NASDAQ | ZBAI | master_absent | ATIF Holdings Limited | VGG0602B1186 |
| NASDAQ | ZENV | master_absent | Zenvia Inc | KYG9889V1014 |
| NASDAQ | ZSPC | master_absent | zSpace, Inc. Common stock | US98980W1071 |
| NSE_IN | AAKAAR | master_absent | Aakaar Medical Technologies Limited | INE1GYP01013 |
| NSE_IN | AARADHYA | master_absent | Aaradhya Disposal Industries Limited | INE124401014 |
| NSE_IN | AATMAJ | master_absent | Aatmaj Healthcare Limited | INE0OB201016 |
| NSE_IN | ABHAPOWER | master_absent | Abha Power and Steel Limited | INE0UYG01015 |
| NSE_IN | ABSMARINE | master_absent | ABS Marine Services Limited | INE0QRV01016 |
| NSE_IN | ACCENTMIC | master_absent | Accent Microcell Limited | INE0Q5D01013 |
| NSE_IN | ACCORD | master_absent | Accord Synergy Limited | INE113X01015 |
| NSE_IN | ACCPL | master_absent | Accretion Pharmaceuticals Limited | INE0T8T01010 |
| NSE_IN | ACETEC | master_absent | Acetech E-Commerce Limited | INE1J6M01010 |
| NSE_IN | ACTIVEINFR | master_absent | Active Infrastructures Limited | INE0KLO01025 |
| NSE_IN | ADDICTIVE | master_absent | Addictive Learning Technology Limited | INE0RDH01021 |
| NSE_IN | AERON | master_absent | Aeron Composite Limited | INE0WL801011 |
| NSE_IN | AESTHETIK | master_absent | Aesthetik Engineers Limited | INE0TSF01011 |
| NSE_IN | AGARWALFT | master_absent | Agarwal Float Glass India Limited | INE0MLA01012 |
| NSE_IN | AGARWALTUF | master_absent | Agarwal Toughened Glass India Limited | INE0P8X01016 |
| NSE_IN | AGNI | master_absent | Agni Green Power Limited | INE0LF301013 |
| NSE_IN | AGUL | master_absent | A G Universal Limited | INE0O6N01012 |
| NSE_IN | AHIMSA | master_absent | Ahimsa Industries Limited | INE136T01014 |
| NSE_IN | AILIMITED | master_absent | Abhishek Integrations Limited | INE0CAJ01017 |
| NSE_IN | AIMTRON | master_absent | Aimtron Electronics Limited | INE0RUV01018 |
| NSE_IN | AISL | master_absent | ANI Integrated Services Limited | INE635Y01015 |
| NSE_IN | AKANKSHA | master_absent | Akanksha Power and Infrastructure Limite | INE0PCY01014 |
| NSE_IN | AKIKO | master_absent | Akiko Global Services Limited | INE0PMR01017 |
| NSE_IN | ALCODIS | master_absent | Alcokraft Distilleries Limited | INE448V01019 |
| NSE_IN | ALLETEC | master_absent | All E Technologies Limited | INE0M2X01012 |
| NSE_IN | ALPEXSOLAR | master_absent | Alpex Solar Limited | INE0R4701017 |
| NSE_IN | ALUWIND | master_absent | ALUWIND INFRA-TECH LIMITED | INE0STM01017 |
| NSE_IN | AMBANIORGO | master_absent | AMBANI ORGOCHEM LIMITED | INE00C501018 |
| NSE_IN | AMBEY | master_absent | Ambey Laboratories Limited | INE0M3I01029 |
| NSE_IN | AMCL | master_absent | ANB Metal Cast Limited | INE0VG001016 |
| NSE_IN | AMEYA | master_absent | Ameya Precision Engineers Limited | INE0KT901015 |
| NSE_IN | AMIABLE | master_absent | Amiable Logistics (India) Limited | INE0MTP01013 |
| NSE_IN | ANLON | master_absent | Anlon Technology Solutions Limited | INE0LR101013 |
| NSE_IN | ANNAPURNA | master_absent | Annapurna Swadisht Limited | INE0MGM01017 |
| NSE_IN | ANONDITA | master_absent | Anondita Medicare Limited | INE0VTV01012 |
| NSE_IN | ANYA | master_absent | Anya Polytech & Fertilizers Limited | INE0SI601032 |
| NSE_IN | APEXECO | master_absent | Apex Ecotech Limited | INE0T4V01015 |
| NSE_IN | APRAMEYA | master_absent | Aprameya Engineering Limited | INE0LQG01010 |
| NSE_IN | APSISAERO | master_absent | Apsis Aerocom Limited | INE1OOJ01011 |
| NSE_IN | ARABIAN | master_absent | Arabian Petroleum Limited | INE08NJ01024 |
| NSE_IN | ARCIIL | master_absent | ARC Insulation & Insulators Limited | INE0YDV01010 |
| NSE_IN | ARHAM | master_absent | Arham Technologies Limited | INE0L2Y01011 |
| NSE_IN | ARIHANTACA | master_absent | Arihant Academy Limited | INE0NCC01015 |
| NSE_IN | ARISTO | master_absent | Aristo Bio-Tech And Lifescience Limited | INE082101010 |
| NSE_IN | ARMOUR | master_absent | Armour Security (India) Limited | INE0TZX01019 |
| NSE_IN | ARUNAYA | master_absent | Arunaya Organics Limited | INE0TTG01017 |
| NSE_IN | ARVINDPORT | master_absent | ARVIND PORT AND INFRA LIMITED | INE0P4T01013 |
| NSE_IN | ASCOM | master_absent | Ascom Leasing & Investments Limited | INE08KD01015 |
| NSE_IN | ASHALOG | master_absent | Ashapura Logistics Limited | INE0LAA01017 |
| NSE_IN | ASHWINI | master_absent | Ashwini Container Movers Limited | INE1A6Q01010 |
| NSE_IN | ASLIND | master_absent | ASL Industries Limited | INE617I01024 |
| NSE_IN | ASPIRE | master_absent | Aspire & Innovative Advertising Limited | INE0S7801010 |
| NSE_IN | ATCENERGY | master_absent | ATC Energies System Limited | INE0V0Q01019 |
| NSE_IN | ATMASTCO | master_absent | Atmastco Limited | INE05DH01017 |
| NSE_IN | AURIGROW | master_absent | Auri Grow India Limited | INE925Y01036 |
| NSE_IN | AUROIMPEX | master_absent | Auro Impex  & Chemicals Limited | INE0NUL01018 |
| NSE_IN | AUSL | master_absent | Aditya Ultra Steel Limited | INE01YQ01013 |
| NSE_IN | AVANA | master_absent | Avana Electrosystems Limited | INE1KU201016 |
| NSE_IN | AVIANSH | master_absent | Avi Ansh Textile Limited | INE0TFB01017 |
| NSE_IN | AVPINFRA | master_absent | AVP Infracon Limited | INE0R9401019 |
| NSE_IN | AVSL | master_absent | AVSL Industries Limited | INE522V01011 |
| NSE_IN | BABAFP | master_absent | Baba Food Processing (India) Limited | INE0QW501012 |
| NSE_IN | BAGDIGITAL | master_absent | B.A.G. Convergence Limited | INE17CQ01015 |
| NSE_IN | BAHETI | master_absent | Baheti Recycling Industries Limited | INE029Q01017 |
| NSE_IN | BALAJIPHOS | master_absent | Balaji Phosphates Limited | INE0PQ601019 |
| NSE_IN | BARFLEX | master_absent | Barflex Polyfilms Limited | INE0QX401014 |
| NSE_IN | BASILIC | master_absent | Basilic Fly Studio Limited | INE0OCC01013 |
| NSE_IN | BAWEJA | master_absent | Baweja Studios Limited | INE0JFJ01011 |
| NSE_IN | BEACON | master_absent | Beacon Trusteeship Limited | INE639X01027 |
| NSE_IN | BEWLTD | master_absent | BEW Engineering Limited | INE0HQI01014 |
| NSE_IN | BHADORA | master_absent | Bhadora Industries Limited | INE0ZRC01017 |
| NSE_IN | BIKEWO | master_absent | Bikewo Green Tech Limited | INE0SQH01013 |
| NSE_IN | BIOPOL | master_absent | Biopol Chemicals Limited | INE0XW001014 |
| NSE_IN | BIRDYS | master_absent | Grill Splendour Services Limited | INE0PC901019 |
| NSE_IN | BLUEPEBBLE | master_absent | Blue Pebble Limited | INE0SAK01011 |
| NSE_IN | BLUEWATER | master_absent | Blue Water Logistics Limited | INE0X3M01010 |
| NSE_IN | BMETRICS | master_absent | Bombay Metrics Supply Chain Limited | INE0I3Y01014 |
| NSE_IN | BRACEPORT | master_absent | Brace Port Logistics Limited | INE0R4Z01018 |
| NSE_IN | BRANDMAN | master_absent | Brandman Retail Limited | INE0XUD01014 |
| NSE_IN | BULKCORP | master_absent | Bulkcorp International Limited | INE0SZ301012 |
| NSE_IN | C2C | master_absent | C2C Advanced Systems Limited | INE0U7V01015 |
| NSE_IN | CADSYS | master_absent | Cadsys (India) Limited | INE090Y01013 |
| NSE_IN | CANARYS | master_absent | Canarys Automations Limited | INE0QG301017 |
| NSE_IN | CBAZAAR | master_absent | Net Avenue Technologies Limited | INE518X01015 |
| NSE_IN | CEDAAR | master_absent | Cedaar Textile Limited | INE11J101017 |
| NSE_IN | CELLECOR | master_absent | Cellecor Gadgets Limited | INE0OMO01025 |
| NSE_IN | CELLPOINT | master_absent | Cell Point (India) Limited | INE0O0001013 |
| NSE_IN | CGRAPHICS | master_absent | Creative Graphics Solutions India Limite | INE0R7401011 |
| NSE_IN | CHAMUNDA | master_absent | Chamunda Electrical Limited | INE11HG01018 |
| NSE_IN | CHANDAN | master_absent | Chandan Healthcare Limited | INE0B2N01016 |
| NSE_IN | CHAVDA | master_absent | Chavda Infra Limited | INE0PT101017 |
| NSE_IN | CHETANA | master_absent | Chetana Education Limited | INE0U1T01012 |
| NSE_IN | CKKRETAIL | master_absent | C K K Retail Mart Limited | INE0SMX01019 |
| NSE_IN | CLASSICEIL | master_absent | Classic Electrodes (India) Limited | INE0UQ601012 |
| NSE_IN | CLSL | master_absent | Crop Life Science Limited | INE00NH01017 |
| NSE_IN | CMNL | master_absent | Chaman Metallics Limited | INE06PV01010 |
| NSE_IN | CMRSL | master_absent | Cyber Media Research & Services Limited | INE075Z01011 |
| NSE_IN | COMMITTED | master_absent | Committed Cargo Care Limited | INE597Z01014 |
| NSE_IN | CONNPLEX | master_absent | Connplex Cinemas Limited | INE0EAS01014 |
| NSE_IN | CONTI | master_absent | Continental Seeds and Chemicals Limited | INE340Z01019 |
| … | … | … | (+443 more) | |

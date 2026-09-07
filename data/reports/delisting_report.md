# Delisting-candidate report

Generated: 2026-09-07T13:41:31Z

**delisting_detected: True**

Markets checked: US, ASX, NSE_IN, BSE_IN, US_NASDAQ_DELETES
Markets skipped: TSE (fetch failed: XLRDError)

Candidates: 291 (delisted=2, suspended=31, master_absent=258)

Detection only — verify each (delisting vs rename vs SME/suspended) and apply via the override/verify pipeline. `delisted` (BSE ListofScripData or Nasdaq Trader trading-system Delete) are drop-ready; `master_absent` need rename-vs-delisting verification; `suspended` are kept by policy (can resume).

| Exchange | Ticker | Classification | Name | ISIN |
|---|---|---|---|---|
| ASX | 5EA | master_absent | 5E ADVANCED MATERIALS INC. | AU0000186207 |
| ASX | AEL | master_absent | AMPLITUDE ENERGY LIMITED | AU0000361909 |
| ASX | AEU | master_absent | ATOMIC EAGLE LTD | AU0000433096 |
| ASX | AHE | master_absent | ADHERIS HEALTH LIMITED | AU0000437493 |
| ASX | AM3 | master_absent | Amara Minerals Limited | AU0000469801 |
| ASX | AM5 | master_absent | ANTARES METALS LIMITED | AU0000369829 |
| ASX | AMU | master_absent | AMERICAN URANIUM LTD | AU0000415879 |
| ASX | AT4 | master_absent | AMERICAN TUNGSTEN & ANTIMONY LTD | AU0000445603 |
| ASX | AUV | master_absent | AURAVELLE METALS LIMITED | AU0000418154 |
| ASX | BCB | master_absent | BOWEN COKING COAL LIMITED | AU000000BCB5 |
| ASX | BKB | master_absent | BLACK BEAR MINERALS LIMITED | AU0000433351 |
| ASX | BTL | master_absent | BEETALOO ENERGY AUSTRALIA LIMITED | AU0000401770 |
| ASX | BTM | master_absent | Breakthrough Minerals Limited | AU0000374274 |
| ASX | CC5 | master_absent | CLEVER CULTURE SYSTEMS LIMITED | AU0000367153 |
| ASX | CL8 | master_absent | CL8 HOLDINGS LIMITED | AU000000CL86 |
| ASX | CP8 | master_absent | CANADIAN PHOSPHATE LIMITED | AU0000384935 |
| ASX | CQT | master_absent | CONNEQT HEALTH LIMITED | AU0000420739 |
| ASX | CR3 | master_absent | CORE ENERGY MINERALS LTD | AU0000373722 |
| ASX | DAI | master_absent | DECIDR AI INDUSTRIES LTD | AU0000386310 |
| ASX | EM3 | master_absent | EMC GOLD CORPORATION | AU0000445173 |
| ASX | EMS | master_absent | Eastern Metals Ltd | AU0000173304 |
| ASX | ERE | master_absent | EUROPEAN RESOURCES LIMITED | AU0000448102 |
| ASX | ERM | master_absent | EMMERSON RESOURCES LIMITED | AU000000ERM4 |
| ASX | FEL | master_absent | FORTE ENERGY LIMITED | AU0000456097 |
| ASX | FTI | master_absent | Fortifai Ltd | AU0000408056 |
| ASX | G1C | master_absent | GROUP ONE CAPITAL LIMITED | AU0000423741 |
| ASX | GA8 | master_absent | GOLDARC RESOURCES LIMITED | AU0000419624 |
| ASX | GBL | master_absent | Great Bear Exploration Ltd | AU0000474363 |
| ASX | GBM | master_absent | GBM RESOURCES LIMITED. | AU0000443905 |
| ASX | GG8 | master_absent | GORILLA GOLD MINES LTD | AU0000382079 |
| ASX | GT3 | master_absent | GREEN360 TECHNOLOGIES LIMITED | AU0000387516 |
| ASX | GUM | master_absent | GUMTREE AUSTRALIA MARKETS LIMITED | AU0000381691 |
| ASX | H3E | master_absent | H3 ENERGY LIMITED | AU0000447005 |
| ASX | HHR | master_absent | HARTSHEAD RESOURCES NL | AU0000154148 |
| ASX | IBR | master_absent | IRON BEAR RESOURCES LTD | AU0000453557 |
| ASX | IFG | master_absent | INFOCUS GROUP HOLDINGS LIMITED | AU0000362923 |
| ASX | IOV | master_absent | ION VIDEO LTD | AU0000440992 |
| ASX | IRX | master_absent | Inhalerx Ltd | AU0000179475 |
| ASX | ITS | master_absent | INFOTRUST LTD | AU0000431462 |
| ASX | IVG | master_absent | INVERT GRAPHITE LIMITED | AU0000378903 |
| ASX | JAY | master_absent | JAYRIDE GROUP LIMITED | AU000000JAY4 |
| ASX | JNS | master_absent | JANUS ELECTRIC HOLDINGS LIMITED | AU0000395626 |
| ASX | L1G | master_absent | L1 GROUP LIMITED | AU0000423501 |
| ASX | LLM | master_absent | LOYAL METALS LTD | AU0000399131 |
| ASX | LRM | master_absent | LION ROCK MINERALS LTD | AU000000LRM9 |
| ASX | M79 | master_absent | MAMMOTH MINERALS LIMITED | AU0000411837 |
| ASX | M96 | master_absent | Maverick Minerals Australia Ltd | AU0000475568 |
| ASX | MAUCA | master_absent | Magnetic Resources NL | AU0000MAUCA0 |
| ASX | MCE | master_absent | MATRIX COMPOSITES & ENGINEERING LIMITED | AU000000MCE6 |
| ASX | MFGO | master_absent | Magellan Financial Group Ltd | AU0000215808 |
| ASX | MML | master_absent | MCLAREN MINERALS LIMITED | AU0000221418 |
| ASX | NFNG | master_absent | Nufarm Finance (NZ) Ltd | NZFCND0004S9 |
| ASX | NH3 | master_absent | NH3 CLEAN ENERGY LIMITED | AU0000369753 |
| ASX | NS1 | master_absent | Nodestream Ltd | AU0000481509 |
| ASX | NUZ | master_absent | NEURIZON THERAPEUTICS LIMITED | AU0000357261 |
| ASX | OB1 | master_absent | ORBMINCO LIMITED | AU0000371049 |
| ASX | OLH | master_absent | OLDFIELDS HOLDINGS LIMITED | AU000000OLH6 |
| ASX | OR3 | master_absent | ORE RESOURCES LIMITED | AU0000436933 |
| ASX | P1E | master_absent | PURE ONE CORPORATION LIMITED | AU0000442865 |
| ASX | PKY | master_absent | PATHKEY.AI LTD | AU0000415291 |
| ASX | PL9 | master_absent | PRAIRIE LITHIUM LIMITED | AU0000421893 |
| ASX | PLA | master_absent | PACIFIC LIME AND CEMENT LIMITED | AU0000411175 |
| ASX | PNM | master_absent | PACIFIC NICKEL MINES LIMITED | AU0000123010 |
| ASX | PVW | master_absent | PVW Resources Ltd | AU0000135188 |
| ASX | PXR | master_absent | PACIFIC RESOURCES LIMITED | AU0000435281 |
| ASX | QOR | master_absent | QORIA LIMITED | AU0000278491 |
| ASX | RA2 | master_absent | RESIMAC PREMIER SERIES 2020-1 |  |
| ASX | RCM | master_absent | RAPID CRITICAL METALS LIMITED | AU0000398364 |
| ASX | RG1 | master_absent | REGAL PARTNERS GLOBAL INVESTMENTS LIMITE | AU0000434920 |
| ASX | RIL | master_absent | REDIVIUM LIMITED | AU0000310211 |
| ASX | SBZ | master_absent | SCHOOLBLAZER LIMITED | AU0000458531 |
| ASX | SGH | master_absent | SGH LIMITED | AU0000364754 |
| ASX | SKM | master_absent | SKYLARK MINERALS LIMITED | AU0000378226 |
| ASX | SLA | master_absent | SOLARA MINERALS LTD | AU0000385841 |
| ASX | STV | master_absent | SWIFT TV LTD | AU0000440786 |
| ASX | TOE | master_absent | TORO ENERGY LIMITED | AU000000TOE6 |
| ASX | TR8 | master_absent | TARRINA RESOURCES LIMITED | AU0000427221 |
| ASX | TSR | master_absent | TURNSTONE RESOURCES LTD | AU0000460404 |
| ASX | TXR | master_absent | TALONX RESOURCES LIMITED | ARDEUT116019 |
| ASX | USC | master_absent | US1 CRITICAL MINERALS LIMITED | AU0000436891 |
| ASX | UWC | master_absent | UNDERWOOD CAPITAL LIMITED | AU0000373201 |
| ASX | VHL | master_absent | VITASORA HEALTH LIMITED | AU0000392748 |
| ASX | WAK | master_absent | WA KAOLIN LIMITED | AU0000111247 |
| ASX | WAU | master_absent | Wa Gold Limited | AU0000466021 |
| ASX | WFE | master_absent | WOLFE ENERGY LIMITED | AU0000458358 |
| ASX | XRA | master_absent | XENORA MINERALS LTD | AU0000421117 |
| ASX | YUG | master_absent | YUGO METALS LIMITED | AU0000404998 |
| BSE_IN | ACESEPP | suspended | Ace Software Exports ltd | IN9849B01026 |
| BSE_IN | ALSTONE | suspended | Alstone Textiles (India) Ltd | INE184S01024 |
| BSE_IN | ASHIS | suspended | Ashiana Ispat Ltd | INE587D01012 |
| BSE_IN | ASHUTPM | suspended | Ashutosh Paper Mills Ltd | INE723K01018 |
| BSE_IN | BCCPP | suspended | BCC Fuba India Ltd | IN9788D01014 |
| BSE_IN | CHARMS | suspended | Charms Industries Ltd | INE442C01012 |
| BSE_IN | FCONSUMER | suspended | Future Consumer Ltd | INE220J01025 |
| BSE_IN | GGENG | suspended | G G Engineering Ltd | INE694X01030 |
| BSE_IN | GOLKONDA | suspended | Golkonda Aluminium Extrusions Ltd-$ | INE327C01031 |
| BSE_IN | INRADIA | suspended | India Radiators Ltd | INE461Y01016 |
| BSE_IN | KEDIACN | suspended | Kedia Construction Company Ltd | INE511J01027 |
| BSE_IN | KRISHPP | suspended | KRISHIVAL FOODS Ltd | IN90GGO01013 |
| BSE_IN | MASCH | suspended | Master Chemicals Ltd | INE523D01017 |
| BSE_IN | OMKARCHEM | suspended | Omkar Speciality Chemicals Ltd | INE474L01016 |
| BSE_IN | PRABHAPP | suspended | Prabha Energy Ltd | IN90I0M01014 |
| BSE_IN | PRESSURS | suspended | Pressure Sensitive Systems India Ltd | INE891E01024 |
| BSE_IN | PVVIPP | suspended | PVV Infra Ltd | IN9428B01029 |
| BSE_IN | RAMASIGNS | suspended | Ramasigns Industries Ltd | INE650D01026 |
| BSE_IN | SELLWIN | suspended | Sellwin Traders Ltd | INE195F01027 |
| BSE_IN | SIKOZY | suspended | Sikozy Realtors Ltd | INE528E01022 |
| BSE_IN | SILVERLINE | suspended | Silverline Technologies Ltd | INE368A01021 |
| BSE_IN | SIPTL | suspended | Sharanam Infraproject and Trading Ltd | INE104S01022 |
| BSE_IN | SSFLPP | suspended | Spandana Sphoorty Financial Ltd | IN9572J01019 |
| BSE_IN | SSLEL | suspended | Sir Shadi Lal Enterprises Ltd | INE117H01019 |
| BSE_IN | SUUMAYA | suspended | Suumaya Corporation Ltd | INE0EMB01015 |
| BSE_IN | TIAANC | suspended | Tiaan Consumer Ltd | INE864T01011 |
| BSE_IN | VARDHMAN | suspended | Vardhman Concrete Ltd | INE115C01014 |
| BSE_IN | VASUDHAGAM | suspended | Vasudhagama Enterprises Ltd | INE583K01016 |
| BSE_IN | WIMPLAST | suspended | Wim Plast Ltd-$ | INE015B01018 |
| BSE_IN | WINSOME | suspended | Winsome Yarns Ltd | INE784B01035 |
| BSE_IN | YARNPP | suspended | Yarn Syndicate Ltd | IN9564C01011 |
| NASDAQ | AACB | master_absent | Artius II Acquisition Inc. Class A Ordin | KYG0509J1159 |
| NASDAQ | ABVE | master_absent | Above Food Ingredients Inc. Common Stock | CA00373V1004 |
| NASDAQ | ADTX | master_absent | Aditxt Inc.  | US0070258696 |
| NASDAQ | AFBI | master_absent | Affinity Bancshares Inc | US00832E1038 |
| NASDAQ | AGAE | master_absent | Allied Gaming & Entertainment Inc. | US0191701095 |
| NASDAQ | AIHS | master_absent | Senmiao Technology Ltd | US8172252046 |
| NASDAQ | ALBT | master_absent | Avalon GloboCare Corp. | US05344R3021 |
| NASDAQ | ALCY | master_absent | Alchemy Investments Acquisition Corp 1 C | KYG0232F1090 |
| NASDAQ | ALOT | master_absent | AstroNova Inc | US04638F1084 |
| NASDAQ | ANSC | master_absent | Agriculture & Natural Solutions Acquisit | KYG0131Y1008 |
| NASDAQ | APGE | delisted | Apogee Therapeutics, Inc. Common Stock | US03770N1019 |
| NASDAQ | APM | master_absent | Aptorum Group Ltd Class A | KYG6096M1069 |
| NASDAQ | AREB | master_absent | American Rebel Holdings Inc | US02919L8853 |
| NASDAQ | ASNS | master_absent | Actelis Networks Inc. | US00503R5081 |
| NASDAQ | ATLN | master_absent | Atlantic International Corp. Common Stoc | US0485921094 |
| NASDAQ | BAYA | master_absent | Bayview Acquisition Corp Class A Ordinar | KY07323B1007 |
| NASDAQ | BBCQ | master_absent | Bleichroeder Acquisition Corp. II Class  | KYG1170E1044 |
| NASDAQ | BCAR | master_absent | D. Boral ARC Acquisition I Corp. Class A | VGG2616F1018 |
| NASDAQ | BCOW | master_absent | 1895 of Wisconsin Inc Bancorp | US28253R1059 |
| NASDAQ | BNBX | master_absent | BNB Plus Corp. | US03815U6073 |
| NASDAQ | BNRG | master_absent | Brenmiller Energy Ltd Ordinary Shares | IL0011415309 |
| NASDAQ | BNZI | master_absent | Banzai International Inc | US06682J3086 |
| NASDAQ | BOCN | master_absent | Blue Ocean Acquisition Corp | KYG1330L1059 |
| NASDAQ | CAEP | master_absent | Cantor Equity Partners III, Inc. Class A | KYG1828A1085 |
| NASDAQ | CCIX | master_absent | Churchill Capital Corp IX | KYG213011094 |
| NASDAQ | CCRN | master_absent | Cross Country Healthcare Inc | US2274831047 |
| NASDAQ | CEPT | master_absent | Cantor Equity Partners II, Inc. | KYG1827K1076 |
| NASDAQ | CIMG | master_absent | CIMG Inc | US67073S3076 |
| NASDAQ | CIZN | master_absent | Citizens Holding Company | US1747151025 |
| NASDAQ | CNTA | master_absent | Centessa Pharmaceuticals plc | US1523091007 |
| NASDAQ | CPRX | master_absent | Catalyst Pharmaceuticals Inc | US14888U1016 |
| NASDAQ | CREG | master_absent | Smart Powerr Corp | US1689133098 |
| NASDAQ | CRNX | master_absent | Crinetics Pharmaceuticals Inc | US22663K1079 |
| NASDAQ | CULL | master_absent | Cullman Bancorp Inc. | US2301531081 |
| NASDAQ | DEVS | master_absent | DevvStream Corp. Common Stock | CA25189R1001 |
| NASDAQ | EA | master_absent | Electronic Arts Inc | US2855121099 |
| NASDAQ | ELSE | master_absent | Electro-Sensors Inc | US2852331022 |
| NASDAQ | EMPG | master_absent | Empro Group Inc. Ordinary shares | KYG3041J1067 |
| NASDAQ | ESPR | master_absent | Esperion Therapeutics Inc | US29664W1053 |
| NASDAQ | ETHM | master_absent | Dynamix Corporation | KYG2949D1126 |
| NASDAQ | EVTV | master_absent | Envirotech Vehicles Inc | US29414V2097 |
| NASDAQ | FATBB | master_absent | FAT Brands Inc | US30258N6004 |
| NASDAQ | FBRX | master_absent | Forte Biosciences Inc | US34962G2084 |
| NASDAQ | FGMC | master_absent | FG Merger II Corp. Common stock | US30334J1025 |
| NASDAQ | FLZH | master_absent | Flash Sports & Media Holdings, Inc. | US91704K3014 |
| NASDAQ | FTRK | master_absent | FAST TRACK GROUP | KYG333801093 |
| NASDAQ | GAMB | master_absent | Gambling.com Group Ltd | JE00BL970N11 |
| NASDAQ | GBNY | master_absent | Generations Bancorp NY Inc | US37149G1085 |
| NASDAQ | GIG | master_absent | GigCapital7 Corp. Class A Ordinary Share | US37518P1012 |
| NASDAQ | GLBZ | master_absent | Glen Burnie Bancorp | US3774071019 |
| NASDAQ | GOCO | master_absent | GoHealth Inc. | US38046W2044 |
| NASDAQ | GREE | master_absent | Greenidge Generation Holdings Inc | US39531G3083 |
| NASDAQ | GV | master_absent | Visionary Education Technology Holdings  | CA92838F2008 |
| NASDAQ | HEPA | master_absent | Hepion Pharmaceuticals Inc | US4268971045 |
| NASDAQ | HOTH | master_absent | Hoth Therapeutics Inc | US44148G2049 |
| NASDAQ | IINN | master_absent | Inspira Technologies Oxy BHN Ltd | IL0011715781 |
| NASDAQ | IPCX | master_absent | Inflection Point Acquisition Corp. III C | KYG478751020 |
| NASDAQ | ISSC | master_absent | Innovative Solutions and Support | US45769N1054 |
| NASDAQ | ITRM | master_absent | Iterum Therapeutics PLC | IE000TTOOBX0 |
| NASDAQ | JFB | delisted | JFB Construction Holdings Class A Common | US46658E1073 |
| NASDAQ | KVAC | master_absent | Keen Vision Acquisition Corporation Ordi | VGG524431191 |
| NASDAQ | LBRDA | master_absent | Liberty Broadband Srs A | US5303071071 |
| NASDAQ | LBRDK | master_absent | Liberty Broadband Srs C | US5303073051 |
| NASDAQ | LGCB | master_absent | Linkage Global Inc Ordinary Shares | KYG5500B1023 |
| NASDAQ | LIXT | master_absent | Lixte Biotechnology Holdings Inc | US5393192027 |
| NASDAQ | LMFA | master_absent | LM Funding America Inc | US5020744042 |
| NASDAQ | LOKV | master_absent | Live Oak Acquisition Corp. V Class A Ord | KYG5509P1028 |
| NASDAQ | LPRO | master_absent | Open Lending Corp | US68373J1043 |
| NASDAQ | LSH | master_absent | Lakeside Holding Limited Common Stock | US51216F1093 |
| NASDAQ | LYRA | master_absent | Lyra Therapeutics Inc | US55234L1052 |
| NASDAQ | MAPS | master_absent | WM Technology Inc | US92971A1097 |
| NASDAQ | MAXN | master_absent | Maxeon Solar Technologies Ltd | SGXZ57724486 |
| NASDAQ | MEHA | master_absent | Functional Brands, Inc. Common Stock | US3609481037 |
| NASDAQ | MLAC | master_absent | Mountain Lake Acquisition Corp. Class A  |  |
| NASDAQ | MRAI | master_absent | Marpai Inc | US5713542083 |
| NASDAQ | MSW | master_absent | Ming Shing Group Holdings Limited Ordina | KYG614401068 |
| NASDAQ | NCSM | master_absent | NCS Multistage Holdings Inc | US6288772014 |
| NASDAQ | NFBK | master_absent | Northfield Bancorp Inc | US66611T1088 |
| NASDAQ | NUTR | master_absent | Nusatrip Incorporated Common Stock | US67119K1025 |
| NASDAQ | NUVL | master_absent | Nuvalent Inc | US6707031075 |
| NASDAQ | NVVE | master_absent | Nuvve Holding Corp | US67079Y4070 |
| NASDAQ | OLPX | master_absent | Olaplex Holdings Inc | US6793691089 |
| … | … | … | (+91 more) | |

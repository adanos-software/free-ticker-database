# Delisting-candidate report

Generated: 2026-09-08T09:30:19Z

**delisting_detected: True**

Markets checked: US, ASX, NSE_IN, BSE_IN, US_NASDAQ_DELETES
Markets skipped: TSE (fetch failed: XLRDError)

Candidates: 424 (delisted=2, suspended=163, master_absent=259)

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
| BSE_IN | 4THGEN | suspended | Fourth Generation Information Systems Lt | INE739B01039 |
| BSE_IN | AANANDALAK | suspended | Aananda Lakshmi Spinning Mills Ltd | INE197R01010 |
| BSE_IN | AARSHYAM | suspended | Aar Shyam India Investment Company Ltd | INE512R01010 |
| BSE_IN | ACEEDU | suspended | ACE Edutrend Ltd | INE715F01014 |
| BSE_IN | ACESEPP | suspended | Ace Software Exports ltd | IN9849B01026 |
| BSE_IN | ADJIA | suspended | Adjia Technologies Ltd | INE0G0V01018 |
| BSE_IN | ADVIKLA | suspended | Advik Laboratories Ltd | INE537C01019 |
| BSE_IN | AIRLTD | suspended | Avishkar Infra Realty Ltd | INE433O01024 |
| BSE_IN | ALCHCORP | suspended | Alchemist Corporation Ltd | INE057D01016 |
| BSE_IN | ALSTONE | suspended | Alstone Textiles (India) Ltd | INE184S01024 |
| BSE_IN | AMALGAM | suspended | Amalgamated Electricity Company Ltd | INE492N01022 |
| BSE_IN | AMARSEC | suspended | Amarnath Securities Ltd | INE745P01010 |
| BSE_IN | AMITINT | suspended | Amit International Ltd | INE053D01015 |
| BSE_IN | ANANDPROJ | suspended | Anand Projects Ltd | INE134R01013 |
| BSE_IN | APIL | suspended | Avi Products India Ltd | INE316O01021 |
| BSE_IN | ARISINT | suspended | Aris International Ltd | INE588E01026 |
| BSE_IN | ASHIS | suspended | Ashiana Ispat Ltd | INE587D01012 |
| BSE_IN | ASHOKRE | suspended | Ashoka Refineries Ltd | INE760M01016 |
| BSE_IN | ASHUTPM | suspended | Ashutosh Paper Mills Ltd | INE723K01018 |
| BSE_IN | ASYL | suspended | Advance Syntex Ltd | INE184U01012 |
| BSE_IN | AUTOPRD | suspended | Automobile Products of India Ltd | INE0NY101012 |
| BSE_IN | AVASARA | suspended | Avasara Finance Ltd | INE759D01017 |
| BSE_IN | AXENTRA | suspended | Axentra Corp Ltd | INE919M01026 |
| BSE_IN | BARONINF | suspended | Baron Infotech Ltd | INE228B01017 |
| BSE_IN | BCCPP | suspended | BCC Fuba India Ltd | IN9788D01014 |
| BSE_IN | BCLENTERPR | suspended | BCL Enterprises Ltd | INE368E01023 |
| BSE_IN | BGLOBAL | suspended | Bharatiya Global Infomedia Ltd | INE224M01013 |
| BSE_IN | BHATEXT | suspended | Bharat Textiles & Proofing Industries Lt | INE201N01019 |
| BSE_IN | BJDUP | suspended | BJ Duplex Boards Ltd | INE265C01025 |
| BSE_IN | BLUECHIP | suspended | Blue Chip India Ltd | INE657B01025 |
| BSE_IN | BLUEGOD | suspended | Bluegod Entertainment Ltd | INE924N01024 |
| BSE_IN | BPCAP | suspended | B. P. Capital Ltd | INE947C01010 |
| BSE_IN | CAPFIN | suspended | Capfin India Ltd | INE960C01013 |
| BSE_IN | CARNATIN | suspended | Carnation Industries Ltd | INE081B01028 |
| BSE_IN | CHARMS | suspended | Charms Industries Ltd | INE442C01012 |
| BSE_IN | CITURGIA | suspended | Citurgia Biochemicals Ltd | INE795B01031 |
| BSE_IN | CITYON | suspended | Cityon Systems (India) Ltd | INE324P01014 |
| BSE_IN | CLCIND | suspended | CLC Industries Ltd | INE376C01038 |
| BSE_IN | CLENON | suspended | Clenon Enterprises Ltd | INE769B01028 |
| BSE_IN | CMICABLES | suspended | CMI Ltd | INE981B01011 |
| BSE_IN | COLAB | suspended | Colab Platforms Ltd | INE317W01030 |
| BSE_IN | CRESANTO | suspended | Cresanto Global Ltd | INE741C01017 |
| BSE_IN | DIAMANT | suspended | Diamant Infrastructure Ltd | INE206I01026 |
| BSE_IN | DJSSS | suspended | DJS Stock & Shares Ltd | INE234E01027 |
| BSE_IN | DOLPHMED | suspended | Dolphin Medical Services Ltd | INE796B01013 |
| BSE_IN | EDUCOMP | suspended | Educomp Solutions Ltd | INE216H01027 |
| BSE_IN | ENCASH | suspended | Encash Entertainment Ltd | INE552Q01018 |
| BSE_IN | FCONSUMER | suspended | Future Consumer Ltd | INE220J01025 |
| BSE_IN | GEETANJ | suspended | Geetanjali Credit and Capital Ltd | INE263R01010 |
| BSE_IN | GGENG | suspended | G G Engineering Ltd | INE694X01030 |
| BSE_IN | GOLKONDA | suspended | Golkonda Aluminium Extrusions Ltd-$ | INE327C01031 |
| BSE_IN | GTEIT | suspended | G-Tech Info-Training Ltd | INE634D01038 |
| BSE_IN | HARIGOV | suspended | Popees Baby Care India Limited | INE167F01018 |
| BSE_IN | HEMORGANIC | suspended | Hemo Organic Ltd | INE422G01015 |
| BSE_IN | HIIL | suspended | Hindusthan Insulators & Industries Ltd | INE799B01025 |
| BSE_IN | HRMNYCP | suspended | Harmony Capital Service Ltd | INE264N01017 |
| BSE_IN | INANISEC | suspended | Inani Securities Ltd | INE224C01014 |
| BSE_IN | INDICAP | suspended | Inditrade Capital Ltd | INE347H01012 |
| BSE_IN | INNOCORP | suspended | Innocorp Ltd | INE214B01017 |
| BSE_IN | INOVSYNTH | suspended | Innovassynth Technologies (India) Ltd | INE690J01011 |
| BSE_IN | INRADIA | suspended | India Radiators Ltd | INE461Y01016 |
| BSE_IN | INTEGSW | suspended | Integra Switchgear Ltd | INE0IPL01018 |
| BSE_IN | INTERDIGI | suspended | Interworld Digital Ltd-$ | INE177D01020 |
| BSE_IN | IPOWER | suspended | I-Power Solutions India Ltd | INE468F01010 |
| BSE_IN | IYKOTHITE | suspended | Iykot Hitech Toolroom Ltd | INE079L01013 |
| BSE_IN | JAYTEX | suspended | Jaybharat Textiles and Real Estate Ltd | INE091E01039 |
| BSE_IN | JDL | suspended | Jaisukh Dealers Ltd | INE190P01019 |
| BSE_IN | JMGCORP | suspended | JMG Corporation Ltd | INE745F01011 |
| BSE_IN | JPTSEC | suspended | JPT Securities Ltd | INE630C01012 |
| BSE_IN | KANDAGIRI | suspended | Kandagiri Spinning Mills Ltd-$ | INE292D01019 |
| BSE_IN | KEDIACN | suspended | Kedia Construction Company Ltd | INE511J01027 |
| BSE_IN | KIRANPR | suspended | Kiran Print Pack Ltd | INE516D01011 |
| BSE_IN | KKPLASTICK | suspended | Kkalpana Plastick Ltd | INE465K01016 |
| BSE_IN | KLGCAP | suspended | KLG Capital Services Ltd | INE929C01018 |
| BSE_IN | KLIFESTYL | suspended | K-Lifestyle & Industries Ltd | INE218A01028 |
| BSE_IN | KONARKSY | suspended | Konark Synthetic Ltd | INE517D01019 |
| BSE_IN | KRISHPP | suspended | KRISHIVAL FOODS Ltd | IN90GGO01013 |
| BSE_IN | LADIAMO | suspended | Laser Diamonds Ltd | INE995E01015 |
| BSE_IN | LYNMC | suspended | Lynx Machinery & Commercials Ltd | INE732D01014 |
| BSE_IN | MASCH | suspended | Master Chemicals Ltd | INE523D01017 |
| BSE_IN | MATHEWE | suspended | Mathew Easow Research Securities Ltd | INE963B01019 |
| BSE_IN | MEGFI | suspended | Mega Fin India Ltd | INE524D01015 |
| BSE_IN | MINOLTAF | suspended | Minolta Finance Ltd | INE514C01026 |
| BSE_IN | MORARJEE | suspended | Morarjee Textiles Ltd | INE161G01027 |
| BSE_IN | MSRINDIA | suspended | MSR India Ltd | INE331L01026 |
| BSE_IN | MUDUNURU | suspended | Mudunuru Ltd | INE491C01027 |
| BSE_IN | NAGAFERT | suspended | Nagarjuna Fertilizers and Chemicals Ltd | INE454M01024 |
| BSE_IN | NATURO | suspended | Naturo Indiabull Ltd | INE0JNB01012 |
| BSE_IN | NDMETAL | suspended | ND Metal Industries Ltd | INE643D01013 |
| BSE_IN | NNTL | suspended | N2N Technologies Ltd | INE043F01011 |
| BSE_IN | OLYMTFI | suspended | Olympic Management & Financial Services  | INE091N01014 |
| BSE_IN | OMANSH | suspended | Omansh Enterprises Ltd | INE378P01036 |
| BSE_IN | OMKAR | suspended | Omkar Overseas Ltd | INE680D01015 |
| BSE_IN | OMKARCHEM | suspended | Omkar Speciality Chemicals Ltd | INE474L01016 |
| BSE_IN | ORTEL | suspended | Ortel Communications Ltd | INE849L01019 |
| BSE_IN | OSCARGLO | suspended | Oscar Global Ltd | INE473F01010 |
| BSE_IN | OSWALOR | suspended | Oswal Overseas Ltd | INE906K01027 |
| BSE_IN | OSWAYRN | suspended | Oswal Yarns Ltd | INE670H01017 |
| BSE_IN | OTCO | suspended | OTCO International Ltd | INE910B01028 |
| BSE_IN | PARMAX | suspended | Parmax Pharma Ltd | INE240T01014 |
| BSE_IN | PEOPLIN | suspended | Peoples Investments Ltd | INE644U01015 |
| BSE_IN | PHOTON | suspended | Photon Capital Advisors Ltd | INE107J01016 |
| BSE_IN | PIONAGR | suspended | Pioneer Agro Extracts Ltd | INE062E01014 |
| BSE_IN | PITHP | suspended | Pithampur Poly Products Ltd | INE747D01012 |
| BSE_IN | PMTELELIN | suspended | P.M. Telelinks Ltd | INE092C01015 |
| BSE_IN | POLYTEX | suspended | Polytex India Ltd | INE012F01016 |
| BSE_IN | PRABHAPP | suspended | Prabha Energy Ltd | IN90I0M01014 |
| BSE_IN | PREMCAPM | suspended | Premium Capital Market & Investments Ltd | INE555D01019 |
| BSE_IN | PRESSURS | suspended | Pressure Sensitive Systems India Ltd | INE891E01024 |
| BSE_IN | PROGREXV | suspended | Progrex Ventures Ltd | INE421E01012 |
| BSE_IN | PVVIPP | suspended | PVV Infra Ltd | IN9428B01029 |
| BSE_IN | RAJVIR | suspended | Rajvir Industries Ltd | INE011H01014 |
| BSE_IN | RAMAPETRO | suspended | Rama Petrochemicals Ltd | INE783A01013 |
| … | … | … | (+224 more) | |

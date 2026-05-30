# DeepSeek ISIN Identity Collision Validation

Generated: `2026-05-30T07:30:59Z`

Policy: DeepSeek triage is advisory only and authorizes no ISIN, country, or name change.

## Summary

| Metric | Value |
| --- | ---: |
| Validated groups | 426 |
| Agrees with detector | 355 |
| Disagrees with detector | 71 |
| Errors | 0 |

## Verdicts

| ISIN | Country | Classification | Conf | Likely misassigned | Rationale |
| --- | --- | --- | ---: | --- | --- |
| CA08663L1040 | Canada | distinct_issuers | 0.70 | NYSE ARCA::SPXU | The listings represent two different ETFs from different issuers (ProShares vs Global X). The ISIN is Canadian, and the  |
| US9621661043 | United States | distinct_issuers | 0.95 | OTC::AIABF\|OTC::CIMDF\|OTC::DYNDF\|OTC::GEBHF\|OTC::GMALF\|OTC::KLKBY\|OTC::MLYNF\|OTC::PNAGF\|OTC::RMGNF\|OTC::RNMBF\|OTC::SDPNF\|OTC::SIMEF\|OTC::TNABF | Weyerhaeuser Company is a known US entity with NYSE listing WY and LSE cross-listing. The OTC listings are entirely diff |
| DE000A161408 | Germany | distinct_issuers | 0.95 | OTC::HLFGY\|OTC::HLTFF | HelloFresh SE is a German company matching the ISIN country code. Hilton Food Group plc is a separate UK company, making |
| IE00B6R52259 | Ireland | distinct_issuers | 0.85 | NASDAQ::SSAC | Multiple listings of an iShares ETF align with the Irish ISIN; SPACSphere Acquisition Corp. is a SPAC, completely differ |
| LU1900066207 | Luxembourg | distinct_issuers | 0.95 | OTC::BRANF | Lyxor Brazil ETF is a Luxembourg-domiciled fund matching the ISIN; Baran Group Ltd is an unrelated industrial company. |
| NL0015000IY2 | Netherlands | distinct_issuers | 0.90 | OTC::NZSCF | Four listings refer to Universal Music Group (various denominations) and one refers to New Zealand Coastal Seafoods Limi |
| US57636Q1040 | United States | distinct_issuers | 0.90 | ASX::MA1\|LSE::MAST | Three listings clearly belong to Mastercard, while two refer to unrelated entities (MA Credit Income Trust and MAST Ener |
| CA53056H1047 | Canada | distinct_issuers | 0.90 | OTC::PGWFF | Three listings relate to Liberty Gold Corp., while one (PGG Wrightson) is an unrelated agricultural company from New Zea |
| CA8119161054 | Canada | distinct_issuers | 0.90 | OTC::BCDRF\|OTC::SARDF | Two listings belong to Seabridge Gold, while the other two are for Banco Santander (Spanish bank) and Sanford Limited (l |
| CH0012142631 | Switzerland | distinct_issuers | 0.90 | OTC::CNHHY\|OTC::CRRNF | Two listings represent Clariant AG (Swiss specialty chemicals), while two represent Cairn Homes plc (Irish homebuilder), |
| CH0024638212 | Switzerland | distinct_issuers | 0.95 | SET::SHR | Schindler Holding AG is a Swiss industrial company, while SET listing shows S Hotels and Resorts Public Company Limited, |
| CH0445689208 | Switzerland | distinct_issuers | 0.80 | BATS::HODL | 21Shares Crypto Basket Index ETP is a Swiss-domiciled product by 21Shares AG, while VanEck Bitcoin Trust is a US-listed  |
| FR0000052292 | France | distinct_issuers | 0.95 | OTC::HMIFF | Hermes International SCA is a French luxury goods company. Harvest Minerals Limited is an unrelated mining company. The  |
| FR0000121147 | France | distinct_issuers | 0.95 | OTC::FRSAF | Forvia SE (formerly Faurecia) is a French automotive parts manufacturer. First Au Limited is an Australian gold explorat |
| FR0000125007 | France | distinct_issuers | 0.95 | OTC::CODMF | Compagnie de Saint-Gobain is a major French building materials company. Coda Minerals Limited is a completely unrelated  |
| FR0010331421 | France | distinct_issuers | 0.95 | OTC::IPHLF | Three listings clearly refer to Innate Pharma, while OTC:IPHLF refers to IPH Limited, a separate company. The ISIN likel |
| FR0010375766 | France | same_issuer | 0.70 | - | All listings appear to track MSCI India, likely same ETF with different branding due to Lyxor/Amundi merger. |
| FR0010435297 | France | same_issuer | 0.70 | - | Listings reference MSCI Emerging Markets, likely same ETF with different branding after Lyxor/Amundi merger. |
| GB0004526900 | United Kingdom | distinct_issuers | 0.95 | LSE::IGR\|OTC::IGDFF | Lloyds Banking Group and IG Design Group are distinct companies. The two Lloyds OTC listings match the ISIN's likely sec |
| IE0032895942 | Ireland | distinct_issuers | 0.95 | NASDAQ::LQDA | LQDE, LQDS, IBCD are all iShares $ Corp Bond UCITS ETF listings; LQDA is a biotech company, clearly a different issuer. |
| IE00B02KXK85 | Ireland | distinct_issuers | 0.95 | NYSE ARCA::FXC | Three listings are iShares China Large Cap UCITS ETF variants, while NYSE ARCA::FXC is a completely different Invesco Cu |
| IE00B3B8PX14 | Ireland | distinct_issuers | 0.95 | OTC::EGIL | Three listings are iShares Global Inflation Linked Government Bond UCITS ETF variants. OTC::EGIL is EdgeTech Internation |
| IE00BKWQ0C77 | Ireland | distinct_issuers | 0.95 | OTC::SPYR | Three listings are SPDR MSCI Europe Consumer Discretionary UCITS ETF variants. OTC::SPYR is SPYR Inc, an unrelated compa |
| IE00BKWQ0N82 | Ireland | distinct_issuers | 0.95 | NYSE ARCA::SPYT\|NYSE::STT\|OTC::STTX | Only LSE::TELE matches the ISIN's SPDR MSCI Europe Telecommunications UCITS ETF. The other listings are entirely differe |
| IE00BKWQ0P07 | Ireland | distinct_issuers | 0.95 | NYSE ARCA::SPYU | Three listings are SPDR MSCI Europe Utilities UCITS ETF variants. NYSE ARCA::SPYU is a leveraged ETN, a distinct instrum |
| IE00BMG6Z448 | Ireland | distinct_issuers | 0.95 | OTC::EXCH | Three listings clearly describe the same ETF across different exchanges/currencies. OTC::EXCH (Exchange Bankshares Inc)  |
| LU1900066975 | Luxembourg | distinct_issuers | 0.95 | BATS::LKOR | Three Lyxor MSCI Korea UCITS ETF listings are cross-listings of the same fund; the FlexShares Credit-Scored US Long Corp |
| SE0001662230 | Sweden | distinct_issuers | 0.95 | OTC::HRZMF | Husqvarna AB (publ) appears in three listings, including B shares on different venues. Horizon Minerals Limited is an un |
| US4370761029 | United States | distinct_issuers | 0.95 | OTC::HDNRF | The Home Depot Inc appears under three tickers on different exchanges; HomeCo Daily Needs REIT is a separate entity, cle |
| US45170X2053 | United States | uncertain | 0.30 | - | Identiv Inc is a different issuer from The Alger ETF Trust. The three Alger ETF listings may represent different ETFs or |
| US5732841060 | United States | distinct_issuers | 0.90 | OTC::MMXLF\|TSXV::MMX | Martin Marietta Materials Inc. (US construction materials) and Mustang Minerals Limited (Canadian mining) are different  |
| US59156R1086 | United States | distinct_issuers | 0.90 | OTC::MTLZF | MetLife Inc. (US insurance) and Metall Zug AG (Swiss industrial) are distinct issuers. The ISIN is associated with MetLi |
| US5949724083 | United States | same_issuer | 0.70 | - | All listing names point to MicroStrategy Incorporated (now Strategy Inc.), a single issuer. The ISIN is known for common |
| US7181721090 | United States | distinct_issuers | 0.90 | OTC::PASMF\|OTC::POTMF | Philip Morris International (US tobacco) is distinct from Premier Miton Group and Portmeirion Group (both UK-based). The |
| AT0000758305 | Austria | distinct_issuers | 0.90 | NASDAQ::PAL | Palfinger AG (Austrian manufacturing) and Proficient Auto Logistics (US transportation) are different issuers. The AT-pr |
| AT0000785555 | Austria | distinct_issuers | 0.90 | NYSE::SEM | ISIN AT prefix indicates Austrian issuer; LSE and OTC names match Semperit, an Austrian company. NYSE name 'Select Medic |
| AT0000969985 | Austria | distinct_issuers | 0.90 | NYSE::ATS | ISIN AT indicates Austrian issuer; LSE and OTC names refer to AT & S, an Austrian tech company. NYSE name 'ATS Corporati |
| BE0003739530 | Belgium | distinct_issuers | 0.90 | NYSE::UCB | ISIN BE indicates Belgian issuer; LSE and OTC names match UCB SA, a Belgian pharmaceutical company. NYSE name 'United Co |
| BE0003755692 | Belgium | distinct_issuers | 0.90 | OTC::ALGEF | ISIN BE indicates Belgian issuer; Euronext and LSE names refer to Agfa-Gevaert, a Belgian company. OTC name 'Alligator E |
| BMG2178K1009 | Bermuda | same_issuer | 0.90 | - | All three listings have names referring to CK Infrastructure / CKI Holdings, a Bermuda-incorporated company. ISIN BM pre |
| CA0042721005 | Canada | distinct_issuers | 0.90 | OTC::ANDMF | Acadian Timber Corp and Andromeda Metals Limited are distinct issuers. The ISIN is Canadian and matches the Canadian com |
| CA13321L1085 | Canada | distinct_issuers | 0.90 | OTC::CCPPF | Cameco Corp and Capital & Counties Properties PLC are distinct issuers. The ISIN corresponds to Cameco, a Canadian urani |
| CA15722J1030 | Canada | distinct_issuers | 0.85 | OTC::CTYMF | Catalyst Metals Limited (Australian gold company) and Ceylon Graphite Corp (Canadian graphite company) are distinct. The |
| CA24463V1013 | Canada | distinct_issuers | 0.80 | OTC::DTTLY\|SET::DUSIT | Defence Therapeutics Inc (Canadian), Datatec Limited (South African), and Dusit Thani (Thai) are three distinct issuers. |
| CA68387G2036 | Canada | distinct_issuers | 0.70 | OTC::ESXMF | Optegra Ventures Inc and Essex Minerals Inc appear to be different issuers. The TSXV and one OTC listing point to Optegr |
| CA80100R4089 | Canada | distinct_issuers | 0.90 | OTC::IRMTF\|TSX::ISC | Sangoma Technologies Corp and Information Services Corporation are different companies. CA80100R4089 is known to be the  |
| CA89712R2019 | Canada | distinct_issuers | 0.90 | NYSE::TR\|OTC::TROLB | Tootsie Roll Industries is a US company and unlikely to have a Canadian ISIN. CA89712R2019 likely belongs to the Canadia |
| CH0023405456 | Switzerland | same_issuer | 0.95 | - | Dufry AG and Avolta AG are the same legal entity after a name change. |
| CH1385084384 | Switzerland | distinct_issuers | 0.90 | OTC::CSOL | 21Shares Solana Core Staking ETP is a Swiss product, while China Solar & Clean Energy is a different company. ISIN CH138 |
| CNE100000Q35 | China | same_issuer | 0.95 | - | Guangzhou Automobile Group Co., Ltd. operates under the brand GAC Group; these are cross-listings of the same underlying |
| DE0005103006 | Germany | distinct_issuers | 1.00 | OTC::ARDDF | ADVA Optical Networking SE and Ardiden Limited are different companies; the ISIN is registered in Germany, matching ADVA |
| DE0005785604 | Germany | distinct_issuers | 1.00 | OTC::FTWYF | Fresenius is a German healthcare company, Freightways is a New Zealand logistics firm; ISIN likely belongs to Fresenius. |
| DE0006047004 | Germany | distinct_issuers | 1.00 | NYSE::HEI | Heidelberg Materials (formerly HeidelbergCement) and Heico Corporation are unrelated; ISIN registered in Germany points  |
| DE0007010803 | Germany | distinct_issuers | 1.00 | NASDAQ::RAA | Rational AG is a German manufacturer; the NASDAQ listing is an ETF, clearly a misassigned ISIN. |
| DE0008019001 | Germany | same_issuer | 1.00 | - | All listings represent the same issuer, Deutsche Pfandbriefbank AG, under slight name variations. |
| DE000A0BL849 | Germany | same_issuer | 0.90 | - | VITA 34 AG renamed to FAMICORD AG in 2023; same ISIN covers both listings. |
| DE000A1X3XX4 | Germany | same_issuer | 0.90 | - | DIC Asset AG rebranded to BRANICKS Group AG in 2022; same ISIN. |
| ES0137650018 | Spain | distinct_issuers | 0.80 | OTC::FDMIF | Fluidra S.A. is the legitimate holder; Founders Metals Inc. appears unrelated. |
| FI4000312251 | Finland | distinct_issuers | 0.60 | HEL::LUMO | ISIN FI4000312251 belongs to Kojamo Oyj; HEL listing shows a different company name, likely a data error. |
| FR0000121014 | France | distinct_issuers | 0.95 | NYSE::MC\|OTC::MCCHF | ISIN FR0000121014 belongs to LVMH; Moelis & Co and McChip Resources are unrelated companies. |
| FR0000124570 | France | same_issuer | 0.95 | - | Compagnie Plastic Omnium changed its name to OPmobility SE. The listings under different names and tickers refer to the  |
| FR0004125920 | France | distinct_issuers | 0.95 | NASDAQ::AMUN | Amundi S.A. is a French asset manager, while the NASDAQ listing is for an ETF managed by abrdn. The French ISIN obviousl |
| FR0010451203 | France | distinct_issuers | 0.95 | OTC::RXXRF | Rexel S.A. is a French electrical supplies company, while Rox Resources Limited is an Australian mining company. The Fre |
| FR0012435121 | France | distinct_issuers | 0.95 | NASDAQ::ELIS | Elis S.A. is a French services company. The NASDAQ listing is a Direxion ETF (inverse Eli Lilly) and clearly a different |
| FR0013269123 | France | distinct_issuers | 0.95 | OTC::AIAGF | Rubis SCA is a French energy company, while Aurubis AG is a German copper producer. The OTC ticker AIAGF is typical for  |
| FR0013295789 | France | same_issuer | 0.95 | - | All three listings refer to the same French company; names are synonyms or trading names. Ticker TFF on Euronext corresp |
| FR0013451333 | France | same_issuer | 0.95 | - | 'FDJ United' is the new corporate identity of 'La Française des Jeux', the French national lottery operator. All members |
| GB00B9275X97 | United Kingdom | distinct_issuers | 0.95 | OTC::CROTF | hVIVO plc and Open Orphan plc are the same legal entity (renamed), while Spenda Ltd is an unrelated Australian fintech c |
| GB00BGXQNP29 | United Kingdom | distinct_issuers | 0.80 | LSE::SDLF\|OTC::SLFPF | Phoenix Group Holdings PLC is a distinct legal entity from Standard Life PLC. The ISIN GB00BGXQNP29 is widely known for  |
| GB00BMTV7393 | United Kingdom | distinct_issuers | 0.95 | NYSE::THG | THG Plc (UK e-commerce) and The Hanover Insurance Group (US insurer) are completely unrelated. The ISIN GB00BMTV7393 is  |
| GB00BSB7BS06 | United Kingdom | distinct_issuers | 0.80 | NYSE::PINE\|OTC::APNEV | Pinewood Technologies Group is a UK-incorporated company, matching the GB prefix ISIN. Alpine Income Property Trust is a |
| GRS260333000 | Greece | distinct_issuers | 0.90 | NASDAQ::HTO | OTE is a Greek telecom company, consistent with the Greek ISIN. H2O America is an unrelated US company; its NASDAQ listi |
| IE000716YHJ7 | Ireland | distinct_issuers | 0.90 | NASDAQ::FWRG | The Invesco ETF is an Irish-domiciled UCITS fund, matching the IE prefix. First Watch Restaurant Group is a US restauran |
| IE000EFHIFG3 | Ireland | distinct_issuers | 0.70 | OTC::WELX | The two Amundi listings appear to be the same UCITS ETF listed on different exchanges (naming variations may reflect loc |
| IE000X9TLGN8 | Ireland | distinct_issuers | 0.80 | OTC::WTER | WisdomTree is an Irish UCITS ETF issuer, matching the IE prefix. Alkaline Water Company is a US beverage company, incons |
| IE000XZSV718 | Ireland | distinct_issuers | 1.00 | NYSE ARCA::SPXL | BMV and LSE listings are both SPDR S&P 500 UCITS ETF, consistent with Irish UCITS ISIN. NYSE ARCA listing is Direxion Da |
| IE00B3DKXQ41 | Ireland | distinct_issuers | 1.00 | NASDAQ::IEAG | LSE and XETRA listings are iShares Aggregate Bond UCITS. NASDAQ listing is Infinite Eagle Acquisition Corp., a SPAC, not |
| IE00B43QJJ40 | Ireland | distinct_issuers | 1.00 | BATS::GLBL | LSE and XETRA listings are SPDR Bloomberg Barclays Global Aggregate Bond UCITS. BATS listing is Pacer Funds Trust, a dif |
| IE00B44CND37 | Ireland | distinct_issuers | 1.00 | NASDAQ::SYBT | Euronext and LSE listings are SPDR US Treasury Bond ETF. NASDAQ listing is Stock Yards Bancorp Inc, a bank stock, unrela |
| IE00B4P11460 | Ireland | distinct_issuers | 1.00 | NYSE ARCA::EMLP | LSE and XETRA listings are PIMCO Emerging Markets Bond UCITS ETF. NYSE ARCA listing is First Trust North American Energy |
| IE00B8GF1M35 | Ireland | distinct_issuers | 0.90 | NASDAQ::GLRE | Two listings (GBRE, SPYJ) refer to the same SPDR Dow Jones Global Real Estate UCITS ETF, one in USD and one in EUR. The  |
| IE00BDFBTQ78 | Ireland | distinct_issuers | 0.90 | NYSE ARCA::GIGB | Two listings (GDIG, WMIN) represent the same VanEck Global Mining UCITS A ETF. The third (GIGB) is a Goldman Sachs corpo |
| IE00BDFL4P12 | Ireland | distinct_issuers | 0.90 | LSE::COMM | ICOM and SXRS are both iShares Diversified Commodity Swap UCITS ETF (cross-listings). COMM is WisdomTree Natural Gas, a  |
| IE00BDVPNG13 | Ireland | distinct_issuers | 0.70 | BATS::INTL\|BATS::WTAI | The ISIN is Irish, matching the UCITS ETF (WTI2). WTAI is a US-listed ETF under WisdomTree Trust, likely a distinct lega |
| IE00BF2GFH28 | Ireland | distinct_issuers | 0.90 | NASDAQ::TRSG | TRES and TRDS are the same Invesco US Treasury Bond UCITS ETF (cross-listings). TRSG is Tungray Technologies Inc, a diff |
| IE00BF4RFH31 | Ireland | distinct_issuers | 0.90 | NASDAQ::WLDS | WSML and IUSN are iShares MSCI World Small-Cap ETFs, consistent with an Irish UCITS ISIN. WLDS is an operating company,  |
| IE00BFD2H405 | Ireland | distinct_issuers | 0.85 | NASDAQ::SKYE\|NASDAQ::SKYU | FSKY is an Irish-domiciled UCITS ETF, matching the ISIN country. SKYE is a biotech stock, SKYU is a US ETF, both likely  |
| IE00BFWXDY69 | Ireland | distinct_issuers | 0.95 | OTC::FLES | FRXE and FVSH are the same Franklin Liberty Euro Short Maturity UCITS ETF cross-listed. FLES is an unrelated small compa |
| IE00BGYWFS63 | Ireland | distinct_issuers | 0.95 | OTC::VDTA | VUTA and VAGT are the same Vanguard USD Treasury Bond UCITS ETF. VDTA is Vertical Data, Inc., likely incorrectly sharing |
| IE00BM8QS095 | Ireland | distinct_issuers | 0.95 | OTC::MCHT | MCTS and ICNT are the same Invesco MSCI China Technology UCITS ETF (GBP and EUR share classes). MCHT is a US financial c |
| IE00BMDPBY65 | Ireland | distinct_issuers | 0.95 | OTC::FLXP | LSE listings are clearly the same ETF in different currency share classes (GBP and EUR), while OTC listing 'FlexPower In |
| IE00BMFNW783 | Ireland | distinct_issuers | 0.70 | NASDAQ::TRIP | Irish ISIN likely belongs to a UCITS ETF. The NASDAQ listing 'TripAdvisor Inc' is a distinct issuer and clearly misassig |
| IE00BMWXKN31 | Ireland | distinct_issuers | 0.95 | OTC::HSTC | LSE and XETRA listings are the same HSBC ETF, while OTC 'HST Global Inc' is an unrelated company. Irish ISIN aligns with |
| IE00BMYMHS24 | Ireland | distinct_issuers | 0.95 | NASDAQ::AMAL | LSE and XETRA listings represent the same Saturna ETF, while NASDAQ's 'Amalgamated Bank' is a distinct banking issuer, a |
| IE00BX7RRJ27 | Ireland | same_issuer | 0.95 | - | All three listings are the same UBS Factor MSCI USA Quality UCITS ETF, merely on different exchanges with slightly abbre |
| IE00BYM31M36 | Ireland | distinct_issuers | 0.90 | NASDAQ::WING | iShares Fallen Angels High Yield Corporate Bond UCITS is an ETF managed by BlackRock, while Wingstop Inc is a completely |
| IE00BZ0G8977 | Ireland | distinct_issuers | 0.90 | OTC::TIPS | SPDR Barclays U.S. TIPS UCITS is an ETF from State Street, while Tianrong Internet Products and Services Inc is an unrel |
| IL0001260111 | Israel | distinct_issuers | 0.80 | NASDAQ::GCT | ISIN starts with IL (Israel). Gazit Globe Ltd/G City Ltd are Israeli real estate companies, while GigaCloud Technology I |
| IL0010824113 | Israel | distinct_issuers | 0.90 | SET::CPW | Check Point Software Technologies is an Israeli cybersecurity firm listed on NASDAQ and LSE, while Copperwired Public Co |
| JP3459600007 | Japan | distinct_issuers | 0.90 | NASDAQ::TAX | ISIN starts with JP (Japan). Takara Holdings Inc. is a Japanese company listed on TSE and OTC, while Cambria Tax Aware E |
| JP3787000003 | Japan | distinct_issuers | 0.90 | OTC::HMDCF | Hitachi Construction Machinery is a Japanese company matching the ISIN country, while HUTCHMED (China) is a Chinese biop |
| LU0533032859 | Luxembourg | distinct_issuers | 0.90 | NASDAQ::FINW | The ISIN is registered in Luxembourg, consistent with the UCITS ETF. Euronext and XETRA listings refer to the same ETF.  |
| LU1390062831 | Luxembourg | distinct_issuers | 0.90 | NYSE MKT::INFU | Luxembourg-domiciled ETF matches the ISIN country. LSE and XETRA listings are the same ETF. NYSE MKT::INFU (InfuSystems  |
| LU1834983634 | Luxembourg | distinct_issuers | 0.90 | OTC::CHMMF | ISIN is Luxembourg, fitting the Amundi ETF. Euronext and XETRA listings represent the same ETF. OTC::CHMMF (Chimeric The |
| NL0000334118 | Netherlands | distinct_issuers | 0.90 | OTC::ASMMF | ASM International NV is a Dutch company, matching the Netherlands ISIN. LSE and OTC::ASMXF listings share the same name. |
| NO0010234552 | Norway | distinct_issuers | 0.95 | OTC::FKMCF | ISIN is Norwegian (NO...). Aker ASA is a Norwegian industrial holding, while Fokus Mining Corporation is a Canadian mini |
| SG1T56930848 | Singapore | distinct_issuers | 0.95 | OTC::WMGTF | ISIN is Singapore-registered (SG...). Wilmar International is a Singaporean agribusiness (listed as Wilmar Intl on SGX). |
| SG1U76934819 | Singapore | same_issuer | 0.95 | - | All three listings refer to Yangzijiang Shipbuilding (Holdings) Ltd, with SGX tickers 'YZJ Shipbldg' being abbreviations |
| SG2D00968206 | Singapore | same_issuer | 0.95 | - | All listings correspond to Hutchison Port Holdings Trust (HPH Trust). SGX listings reflect different currencies, and OTC |
| US0152711091 | United States | distinct_issuers | 0.95 | OTC::AGREF | ISIN is US-based. Alexandria Real Estate Equities is a US REIT listed on NYSE and LSE. Orpheus Uranium is a distinct min |
| US22160K1051 | United States | distinct_issuers | 1.00 | OTC::CTOHF | ISIN US22160K1051 belongs to Costco Wholesale Corp. LSE and NASDAQ listings match this issuer, while OTC listing CTOHF r |
| US2441991054 | United States | distinct_issuers | 1.00 | OTC::DEDVF | ISIN US2441991054 belongs to Deere & Company. LSE and NYSE listings correspond to Deere, while OTC listing DEDVF represe |
| US44980X1090 | United States | distinct_issuers | 1.00 | OTC::IPFPF | ISIN US44980X1090 belongs to IPG Photonics Corporation. LSE and NASDAQ listings match this entity, while OTC listing IPF |
| US65339F1012 | United States | distinct_issuers | 1.00 | OTC::NNMTF | ISIN US65339F1012 is assigned to NextEra Energy Inc. LSE and NYSE listings represent NextEra, while OTC listing NNMTF co |
| US75513E1010 | United States | same_issuer | 1.00 | - | RTX Corporation and Raytheon Technologies Corp refer to the same entity (name change). All listings are cross-listings o |
| US7591EP1005 | United States | distinct_issuers | 0.95 | OTC::EUZOF | Regions Financial is a US bank holding company; Eurazeo SE is a French investment company. They are distinct entities. T |
| US8447411088 | United States | same_issuer | 1.00 | - | All listings clearly refer to Southwest Airlines, a US airline, with variations in legal name presentation or exchange t |
| US9884981013 | United States | distinct_issuers | 0.95 | OTC::TGRHF | Yum! Brands is a US fast-food corporation, while Tirupati Graphite is a UK-based mining company. They are unrelated. The |
| XS2875106242 | unknown | distinct_issuers | 0.70 | NYSE ARCA::DSPY | The two London listings appear to be different listings of an IncomeShares ETP. The NYSE ARCA listing is for Tema ETF Tr |
| AT0000741053 | Austria | distinct_issuers | 0.95 | NYSE::EVN | EVN AG is an Austrian energy supplier; Eaton Vance Municipal Income Closed Fund is a US closed-end fund. The AT prefix i |
| AT0000818802 | Austria | distinct_issuers | 0.99 | NYSE::DOC | DO & CO Aktiengesellschaft (Austrian catering company) and Healthpeak Properties Inc (US REIT) are completely different  |
| AT0000A3EPA4 | Austria | distinct_issuers | 0.99 | NYSE MKT::AMS | Ams AG (Austrian semiconductor firm) and American Shared Hospital Service (US healthcare provider) are unrelated. The Au |
| AU000000ACR3 | Australia | distinct_issuers | 0.99 | NYSE::ACR | Acrux Limited (Australian pharmaceutical) and Acres Commercial Realty Corp (US REIT) are distinct. The AU ISIN indicates |
| AU000000AMP6 | Australia | distinct_issuers | 0.99 | NYSE::AMP | AMP Ltd (Australian financial services) and Ameriprise Financial Inc (US financial) are separate issuers. The Australian |
| AU000000ARL4 | Australia | distinct_issuers | 0.85 | OTC::ALFDF | Ardea Resources Limited (Australian mining) and Astral Foods Limited (South African poultry) are distinct. The AU ISIN s |
| AU000000BIT4 | Australia | distinct_issuers | 0.70 | NYSE::BIT | Biotron Limited is an Australian company matching the Australian ISIN prefix and registered country, while Blackrock Mul |
| AU000000BVS9 | Australia | distinct_issuers | 0.70 | NASDAQ::BVS | Bravura Solutions Limited is an Australian company consistent with the ISIN, whereas Bioventus Inc is a US-based medical |
| AU000000CAN2 | Australia | distinct_issuers | 0.70 | NASDAQ::CAN | Cann Group Limited is an Australian medicinal cannabis company fitting the ISIN's Australian origin, while Canaan Inc is |
| AU000000CAR3 | Australia | distinct_issuers | 0.70 | OTC::CCEGF | CAR GROUP LTD is an Australian company matching the ISIN country, while Carclo plc is a UK-based firm, meaning distinct  |
| AU000000CIA2 | Australia | distinct_issuers | 0.70 | NYSE::CIA | Champion Iron Limited is an Australian mining company consistent with the ISIN, while Citizens Inc is a US insurer, so d |
| AU000000CTM4 | Australia | distinct_issuers | 0.80 | NYSE MKT::CTM | The ISIN is Australian, matching Centaurus Metals Limited (Australian mining company) while Castellum Inc. appears to be |
| AU000000EML7 | Australia | distinct_issuers | 0.80 | NASDAQ::EML | Australian ISIN likely belongs to EML Payments Limited (Australian fintech), while Eastern Co is likely a separate US-ba |
| AU000000FLN2 | Australia | distinct_issuers | 0.80 | NASDAQ::FLN | The ISIN is Australian, matching Freelancer Limited (Australian freelancing platform) while First Trust Latin America Al |
| AU000000GPT8 | Australia | distinct_issuers | 0.80 | NASDAQ::GPT | Australian ISIN likely corresponds to GPT Group (Australian real estate), whereas Intelligent Alpha Atlas ETF is a US-ba |
| AU000000IRD4 | Australia | distinct_issuers | 0.80 | NASDAQ::IRD | The Australian ISIN matches Iron Road Limited (Australian mining infrastructure) while Opus Genetics, Inc. is a US biote |
| AU000000IVX4 | Australia | distinct_issuers | 0.90 | TSXV::IVX | Invion Limited is an Australian company matching the ISIN country code, while Inventronics Ltd appears to be Canadian an |
| AU000000KRM1 | Australia | distinct_issuers | 0.90 | OTC::KRMCF | Kingsrose Mining Limited is an Australian company likely holding the Australian ISIN; KRM22 Plc is a UK-incorporated ent |
| AU000000PAR5 | Australia | distinct_issuers | 0.95 | NYSE::PAR | Paradigm Biopharmaceuticals Limited is an Australian biotech consistent with the AU ISIN; PAR Technology Corporation is  |
| AU000000PCL4 | Australia | distinct_issuers | 0.95 | BATS::PCL | Pancontinental Oil & Gas NL is an Australian explorer with a legitimate AU ISIN; the PGIM Corporate Bond ETF is a US ETF |
| AU000000PLS0 | Australia | same_issuer | 0.95 | - | Both listings represent Pilbara Minerals Limited, an Australian lithium miner with primary ASX listing (PLS) and OTC tic |
| AU000000SHO1 | Australia | distinct_issuers | 0.90 | NYSE::SHO | ISIN registered in Australia; SportsHero Limited is an Australian company, whereas Sunstone Hotel Investors Inc is a US- |
| AU000000TLG7 | Australia | distinct_issuers | 0.90 | VSE::TLG | ISIN registered in Australia; Talga Group Ltd is an Australian company, while TLG Immobilien AG is a German real estate  |
| AU000000WOR2 | Australia | distinct_issuers | 0.90 | NYSE::WOR | ISIN registered in Australia; Worley Limited is an Australian engineering company, while Worthington Enterprises, Inc. i |
| AU0000013559 | Australia | distinct_issuers | 0.90 | NYSE::ALX | ISIN registered in Australia; Atlas Arteria Limited is an Australian toll road operator, whereas Alexanders Inc is a US  |
| AU0000016560 | Australia | distinct_issuers | 0.90 | NYSE::IVZ | ISIN registered in Australia; Invictus Energy Limited is an Australian oil & gas explorer, while Invesco Plc is a US-dom |
| AU0000028946 | Australia | distinct_issuers | 0.90 | NYSE::INR | Infinity Natural Resources, Inc. is a US company, while ioneer Ltd is an Australian company. ISIN starting with AU indic |
| AU0000047797 | Australia | distinct_issuers | 0.90 | NYSE::PBH | Prestige Consumer Healthcare Inc. is a US company, while PointsBet Holdings Limited is an Australian company. ISIN start |
| AU0000061897 | Australia | distinct_issuers | 0.90 | NASDAQ::PRN | Invesco DWA Industrials Momentum ETF is a US-listed ETF (likely US ISIN), while Perenti Global Limited is an Australian  |
| AU0000068579 | Australia | distinct_issuers | 0.90 | NASDAQ::REE | Ree Automotive Holding Inc is an Israeli-founded company listed in the US, while RareX Limited is an Australian resource |
| AU0000101636 | Australia | distinct_issuers | 0.90 | NYSE MKT::AIM | AIM ImmunoTech Inc is a US biotech company, while Ai-Media Technologies Limited is an Australian company. ISIN starting  |
| AU0000119307 | Australia | distinct_issuers | 0.95 | NASDAQ::NXL | ISIN AU0000119307 is registered in Australia. Nuix Limited (OTC::NXLLF) is an Australian company, consistent with the IS |
| AU0000154833 | Australia | distinct_issuers | 0.95 | NYSE ARCA::EDV | ISIN AU0000154833 is registered in Australia. Endeavour Group Limited (OTC::EDVGF) is an Australian company, matching th |
| AU0000177172 | Australia | distinct_issuers | 0.95 | BATS::ITM | ISIN AU0000177172 is registered in Australia. iTech Minerals Ltd (OTC::ITMIF) is an Australian mining company, consisten |
| AU0000207912 | Australia | distinct_issuers | 0.95 | TSX::NXTG | ISIN AU0000207912 is registered in Australia. Frontier Energy Limited (OTC::FRHYF) is an Australian company, matching th |
| AU0000218307 | Australia | distinct_issuers | 0.95 | NYSE::ZIP | ISIN AU0000218307 is registered in Australia. Zip Co Limited (OTC::ZIZTF) is an Australian fintech company, consistent w |
| AU0000390544 | Australia | distinct_issuers | 0.90 | NYSE ARCA::DNL | ISIN is Australian (AU prefix); OTC security is an Australian company (Dyno Nobel/Incitec Pivot), while the NYSE ARCA li |
| AU0000XINAJ0 | Australia | distinct_issuers | 0.95 | NYSE::SEE | AU prefix indicates Australian security; Seeing Machines is an Australian company (OTC:SEEMF), while Sealed Air is a US  |
| BE0003656676 | Belgium | distinct_issuers | 0.95 | NASDAQ::RECT | BE prefix denotes Belgian security; Recticel S.A. is a known Belgian company, whereas Rectitude Holdings is a different  |
| BE0003724383 | Belgium | same_issuer | 0.95 | - | Both listings refer to Wereldhave Belgium, a Belgian real estate company. The Euronext and LSE listings are cross-listin |
| BMG6748X1048 | Bermuda | distinct_issuers | 0.95 | NASDAQ::OMH | BM prefix indicates Bermuda incorporation; OM Holdings Ltd is likely Bermuda-domiciled and listed in Bursa, while Ohmyho |
| CA0221431012 | Canada | distinct_issuers | 0.90 | NASDAQ::ALT | CA0221431012 is a Canadian ISIN. Alturas Minerals Corp. (OTC::ALTCF) is a Canadian mineral exploration company, consiste |
| CA03969H1055 | Canada | distinct_issuers | 0.90 | NYSE MKT::AWX | CA03969H1055 is a Canadian ISIN. ArcWest Exploration Inc. (OTC::SJRNF) is a Canadian mineral exploration company, aligni |
| CA04274P1053 | Canada | distinct_issuers | 0.90 | NYSE::AXL | CA04274P1053 is a Canadian ISIN. Arrow Exploration Corp. (OTC::CSTPF) is a Canadian oil and gas company, matching the IS |
| CA0493041085 | Canada | distinct_issuers | 0.90 | NASDAQ::AEP | CA0493041085 is a Canadian ISIN. Atlas Engineered Products Ltd. (OTC::APEUF) is a Canadian engineered wood products comp |
| CA05605B1031 | Canada | distinct_issuers | 0.90 | NYSE::BTU | CA05605B1031 is a Canadian ISIN. BTU Metals Corp. (OTC::BTUMF) is a Canadian mineral exploration company, fitting the IS |
| CA0636711016 | Canada | distinct_issuers | 0.90 | NYSE ARCA::XXXX | The listings represent a leveraged ETN and common stock of a bank, clearly different issuers. ISIN is Canadian, matching |
| CA09352R1055 | Canada | distinct_issuers | 0.90 | OTC::BAGFF | A.G. Barr p.l.c is a UK soft drink company, while Blende Silver Corp. is a Canadian mining company. The Canadian ISIN li |
| CA09370U1066 | Canada | distinct_issuers | 0.60 | NASDAQ::MATE | The names suggest an ETF product and a technology ventures company, likely distinct. The ISIN is Canadian, and Blockmate |
| CA12009C1095 | Canada | distinct_issuers | 0.90 | NYSE ARCA::BILD | Nomura is a Japanese financial group, while BuildDirect is a Canadian tech company. The Canadian ISIN likely belongs to  |
| CA1250091008 | Canada | distinct_issuers | 0.95 | NYSE::CMI | Cummins Inc. is a US-based corporation with a US ISIN; this Canadian ISIN matches C-Com Satellite Systems Inc., a Canadi |
| CA1348011091 | Canada | distinct_issuers | 0.90 | NYSE::CF | CF Industries Holdings Inc (US-based fertilizer company) and Canaccord Genuity Group Inc (Canadian financial services fi |
| CA1363751027 | Canada | distinct_issuers | 0.90 | OTC::CNECF | Canadian National Railway Company and Centuria Capital Group (Australian property fund) are unrelated. The ISIN is Canad |
| CA13648X1087 | Canada | distinct_issuers | 0.90 | NYSE::CPS | Cooper-Standard Holdings Inc (US automotive parts) and Canadian Premium Sand Inc (Canadian industrial sand) are distinct |
| CA1381173048 | Canada | distinct_issuers | 0.70 | NASDAQ::CD | Cantex Mine Development Corp is a Canadian mining company; Chaince Digital Holdings appears unrelated and likely not Can |
| CA1566151066 | Canada | same_issuer | 0.95 | - | Cypress Development Corp changed its name to Century Lithium Corp, continuing as the same entity. Both tickers represent |
| CA1567881018 | Canada | distinct_issuers | 0.70 | NASDAQ::CERT | The names (Certara Inc vs Cerrado Gold Inc) indicate different issuers and industries. The ISIN is Canadian-registered,  |
| CA1715521029 | Canada | distinct_issuers | 0.70 | NYSE::CRI | Carter’s Inc is a US retailer, while Churchill Resources Inc. is a Canadian mineral explorer. The ISIN’s Canadian origin |
| CA17178G1046 | Canada | distinct_issuers | 0.70 | NYSE::CMC | Commercial Metals Company is a US steel manufacturer; Cielo Waste Solutions Corp is Canadian. The ISIN’s Canadian regist |
| CA1903401091 | Canada | distinct_issuers | 0.70 | NASDAQ::COCO | Vita Coco Company Inc is a US beverage company, Coast Copper Corp is a Canadian miner. The Canadian ISIN aligns with OTC |
| CA2107373003 | Canada | distinct_issuers | 0.70 | NYSE::CNS | Cohen & Steers Inc is a US investment firm, while Contagious Gaming Inc is a Canadian gaming company. The ISIN’s Canadia |
| CA26843R1064 | Canada | distinct_issuers | 0.80 | BATS::EFV | BATS::EFV is iShares MSCI EAFE Value ETF, a US-domiciled ETF, while OTC::EFVIF is EF EnergyFunders Ventures Inc., a Cana |
| CA26884V1076 | Canada | distinct_issuers | 0.80 | NASDAQ::EQ | NASDAQ::EQ is Equillium Inc, a US biotech firm, while OTC::CYPXF is EQ Inc, a Canadian company. The Canadian ISIN likely |
| CA26906P1045 | Canada | distinct_issuers | 0.90 | NYSE::ESE | NYSE::ESE is ESCO Technologies Inc, a US company, while OTC::ENTEF is ESE Entertainment Inc, a Canadian company. The Can |
| CA2752551077 | Canada | same_issuer | 0.90 | - | OTC::EAGRF (Leaf Mobile Inc.) and TSX::EAGR (East Side Games Group Inc.) share the same root ticker. East Side Games Gro |
| CA27923D1087 | Canada | distinct_issuers | 0.90 | NYSE::ECO | NYSE::ECO is Okeanis Eco Tankers Corp., a Marshall Islands shipping company, while OTC::ECSNF is EcoSynthetix Inc, a Can |
| CA3001442019 | Canada | distinct_issuers | 0.90 | NASDAQ::EVER | ISIN is registered in Canada. Evergold Corp is a Canadian mineral exploration company, while EverQuote Inc is a US-based |
| CA30041N1078 | Canada | distinct_issuers | 0.90 | NYSE::ET | ISIN is Canadian; Evertz Technologies Limited is Canadian, Energy Transfer LP is American. Thus Energy Transfer likely w |
| CA3025862010 | Canada | distinct_issuers | 0.90 | LSE::FP | ISIN is Canadian; FP Newspapers Inc is a Canadian company, while Fondul Proprietatea S.A. is a Romanian fund. Therefore, |
| CA3025911023 | Canada | distinct_issuers | 0.90 | NYSE ARCA::FPX | ISIN is Canadian; FPX Nickel Corp is a Canadian nickel company, whereas First Trust US Equity Opportunities ETF is a US  |
| CA3073571034 | Canada | distinct_issuers | 0.70 | OTC::CPPKF | Both are Canadian companies, but the names differ (Faraday Copper Corp. vs Copperbank Resources Corp), suggesting distin |
| CA31660A1030 | Canada | distinct_issuers | 0.95 | NASDAQ::FSZ | First Trust Switzerland AlphaDEX Fund is an exchange-traded fund, while Fiera Capital Corporation is a Canadian asset ma |
| CA31810H1073 | Canada | distinct_issuers | 0.95 | NYSE ARCA::FTEC | Fidelity MSCI Information Technology Index ETF is an ETF tracking tech index, while Fintech Select Ltd is a Canadian pay |
| CA34964F1099 | Canada | distinct_issuers | 0.95 | NYSE::FOR | Forestar Group is a US-based residential lot development company, while Fortune Bay Corp is a Canadian gold exploration  |
| CA34967D1015 | Canada | distinct_issuers | 0.95 | NYSE::FT | Franklin Universal Closed Fund is a closed-end fund, while Fortune Minerals Limited is a Canadian mining company, distin |
| CA3611551043 | Canada | distinct_issuers | 0.70 | OTC::SULMF | Sulliden Mining Capital and Future Mineral Resources are both Canadian mining companies, but they are separate entities  |
| CA37183V1022 | Canada | distinct_issuers | 0.40 | NASDAQ::GDC | GD Culture Group Limited and Genesis Land Development Corp are clearly different entities; the ISIN is Canadian, making  |
| CA37232A1093 | Canada | distinct_issuers | 0.30 | OTC::GENX | Genix Pharmaceuticals Corporation and Genex Pharmaceutical Inc have different names and are likely distinct; the ISIN ma |
| CA37957M1068 | Canada | distinct_issuers | 0.70 | OTC::GLCDF | Global Atomic Corp is a Canadian nuclear company, fitting the CA ISIN, while GL Events SA is French and should have an F |
| CA3803551074 | Canada | distinct_issuers | 0.70 | NYSE ARCA::GSY | goeasy Ltd. is a well-known Canadian company and commonly holds this ISIN; the Invesco ETF is a fund product that should |
| CA3807211006 | Canada | distinct_issuers | 0.70 | NYSE::GRC | Gold Springs Resource Corp is a Canadian mining company, appropriate for a CA ISIN; Gorman-Rupp Company is a US industri |
| CA3814951008 | Canada | distinct_issuers | 0.40 | OTC::GSKRF | Different company names (First Nordic Metals vs Goldsky Resources). Ticker GSKR matches Goldsky Resources, suggesting it |
| CA39943R1082 | Canada | distinct_issuers | 0.70 | OTC::GRDAF | Guardforce AI Co. Limited is likely a non-Canadian entity (AI security), while Grounded Lithium Corp. is a Canadian lith |
| CA40281L1094 | Canada | distinct_issuers | 0.70 | NYSE::GUG | Guggenheim Active Allocation Fund is a US closed-end fund and would have a US ISIN. Gungnir Resources Inc is a Canadian  |
| CA4296951094 | Canada | distinct_issuers | 0.70 | NYSE::HLF | High Liner Foods Incorporated is a well-known Canadian seafood company. Herbalife Nutrition Ltd. is incorporated outside |
| CA44049C4011 | Canada | distinct_issuers | 0.80 | NYSE::DLR | Digital Realty Trust Inc is a US REIT with a US ISIN. Global X US Dollar Currency ETF is a Canadian-listed ETF, consiste |
| CA44056K1066 | Canada | distinct_issuers | 0.90 | NYSE::HTB | HomeTrust Bancshares, Inc. is a US bank likely assigned a US ISIN; Global X ETF is a Canadian ETF matching the CA ISIN p |
| CA45250Q1046 | Canada | same_issuer | 0.70 | - | Both listings appear to be the same issuer with a name change; the ISIN is Canadian and both entities are Canadian minin |
| CA4528921022 | Canada | distinct_issuers | 0.90 | NASDAQ::III | Information Services Group is US-based, Imperial Metals is Canadian; the CA ISIN prefix matches the Canadian company onl |
| CA45674Q1028 | Canada | distinct_issuers | 0.30 | NASDAQ::INEO | Similar names but likely distinct issuers; the Canadian ISIN suggests INEO Tech Corp is the legitimate holder. |
| CA45780T2065 | Canada | distinct_issuers | 0.90 | NYSE ARCA::IPO | Renaissance IPO ETF is a US ETF, InPlay Oil Corp is Canadian; the CA ISIN prefix matches the Canadian company only. |
| CA45823T1066 | Canada | distinct_issuers | 0.90 | OTC::IFCNF | ISIN is Canadian. INFICON Holding AG is a Swiss entity, while Intact Financial Corporation is a Canadian insurance compa |
| CA46989B1031 | Canada | distinct_issuers | 0.80 | NYSE ARCA::JADE | ISIN is Canadian. Jade Leader Corp is a Canadian mining company (matching the country code), whereas the JPMorgan ETF is |
| CA4707481046 | Canada | distinct_issuers | 0.90 | NASDAQ::JWEL | ISIN is Canadian. Jamieson Wellness Inc is a Canadian company, while Jowell Global Ltd. is a Chinese company listed on N |
| CA50543R1091 | Canada | distinct_issuers | 0.95 | NASDAQ::LAB | Standard Biotools Inc is a US-domiciled health technology company; its ISIN should begin with US. Labrador Gold Corp is  |
| CA5054401073 | Canada | distinct_issuers | 0.95 | NASDAQ::LIF | Life360, Inc. is a US-incorporated technology firm; its ISIN should be US-based. Labrador Iron Ore Royalty Corporation i |
| CA51925D1069 | Canada | distinct_issuers | 0.95 | NYSE::LB | LandBridge Company LLC is a US-based real estate/oil land management company, while Laurentian Bank of Canada is a Canad |
| CA5649051078 | Canada | distinct_issuers | 0.95 | NASDAQ::MFI | mF International Limited is a Hong Kong-based financial technology company, likely domiciled in Cayman Islands or BVI, w |
| CA5651271077 | Canada | distinct_issuers | 0.95 | OTC::CRNLF | Capricorn Metals Ltd is an Australian gold producer; its ISIN should start with AU. Maple Gold Mines Ltd is a Canadian g |
| CA57384M1077 | Canada | distinct_issuers | 0.80 | NASDAQ::MRVL | The ISIN is Canadian, matching Marvel Biosciences Corp., a Canadian company. Marvell Technology Group is a distinct, non |
| CA5909152038 | Canada | distinct_issuers | 0.90 | NYSE::MTX | Canadian ISIN aligns with Metalex Ventures Ltd; Minerals Technologies Inc is a US-domiciled company, unlikely to have a  |
| CA63000Y1034 | Canada | distinct_issuers | 0.90 | NYSE ARCA::NSCI | Canadian ISIN matches Nanalysis Scientific Corp; the ETF is a US-listed product, typically with a US ISIN. |
| CA6544541072 | Canada | distinct_issuers | 0.90 | NYSE::NBY | Canadian ISIN suggests Niobay Metals Inc as the true issuer; NovaBay Pharmaceuticals is a US company. |
| CA6653783036 | Canada | distinct_issuers | 0.90 | NYSE::NL | Canadian ISIN points to Northern Lion Gold Corp; NL Industries is a US corporation. |
| CA67013H1064 | Canada | distinct_issuers | 0.90 | NYSE::NOW | ServiceNow Inc is a US-based enterprise software company, while NowVertical Group Inc is a Canadian data analytics firm. |
| CA68617J1003 | Canada | distinct_issuers | 0.90 | OTC::MMJ | OrganiGram Holdings Inc is a Canadian cannabis company, while MMJ Group Holdings Limited appears to be an Australian can |
| CA69403X1134 | Canada | distinct_issuers | 0.90 | OTC::PBMFF | Psyence Biomedical Ltd. and Pacific Bay Minerals Ltd are unrelated companies in different sectors (biotech vs mining). T |
| CA69806A1084 | Canada | distinct_issuers | 0.90 | NYSE::PGZ | Principal Real Estate Income Closed Fund is a US closed-end fund, while Pan Global Resources Inc. is a Canadian mineral  |
| CA7056465031 | Canada | distinct_issuers | 0.90 | NYSE::PX | P10 Inc is a US-based alternative asset manager, while Pelangio Exploration Inc is a Canadian gold exploration company.  |
| CA74359T2074 | Canada | distinct_issuers | 0.80 | NYSE ARCA::PGX | Invesco Preferred ETF is a US-domiciled ETF likely having a US ISIN, while Prosper Gold Corp is a Canadian mining compan |
| CA74766V1004 | Canada | distinct_issuers | 0.30 | OTC::ATOXF | Both are Canadian companies, but the ISIN is registered in Canada; the TSXV listing is on a Canadian exchange, making it |
| CA7594021007 | Canada | same_issuer | 0.90 | - | Tickers MYID and MYIDF follow the pattern of a Canadian listing (TSXV) and its US OTC foreign ordinary; the different na |
| CA76711R1001 | Canada | distinct_issuers | 0.90 | NYSE::RGR | Sturm Ruger & Company is a US firearms manufacturer trading on NYSE, likely with a US ISIN. Rio Grande Resources Ltd is  |
| CA77519R1029 | Canada | distinct_issuers | 0.90 | NYSE::RSI | Rush Street Interactive is a US company (NYSE: RSI) with a likely US ISIN. Rogers Sugar Inc is a Canadian company, and t |
| CA77544C1041 | Canada | distinct_issuers | 0.90 | NYSE::ROK | ISIN is Canadian (CA prefix). Rockwell Automation (ROK) is a US industrial company (ISIN US7739031091). ROK Resources In |
| CA7766521099 | Canada | distinct_issuers | 0.90 | NASDAQ::ROOT | ISIN is Canadian. Root Inc (ROOT) is a US insurance company (ISIN US77664L2079). Roots Corporation (RROTF) is a Canadian |
| CA78570Q1081 | Canada | distinct_issuers | 0.90 | NYSE ARCA::SBIO | Canadian ISIN. ALPS Medical Breakthroughs ETF (SBIO) is a US-listed ETF. Sabio Holdings Inc. (SABOF) is a Canadian compa |
| CA8263XP1041 | Canada | distinct_issuers | 0.90 | NYSE::SM | Canadian ISIN. SM Energy (SM) is a US oil and gas company (ISIN US78454L1008). Sierra Madre Gold and Silver (SMDRF) is a |
| CA83179X1087 | Canada | same_issuer | 0.95 | - | Both listings reference Smart REIT / SmartCentres Real Estate Investment Trust. CWYUF is a known OTC symbol for SmartCen |
| CA83438X1050 | Canada | distinct_issuers | 0.80 | NASDAQ::SGC | Superior Group of Companies, Inc. is a US-based company (likely Florida), while Solstice Gold Corp is a Canadian gold ex |
| CA84281U1075 | Canada | distinct_issuers | 0.80 | NYSE::SMP | Standard Motor Products Inc is a US-based auto parts company (New York), Southern Empire Resources is a Canadian explora |
| CA8469051079 | Canada | distinct_issuers | 0.80 | NYSE::SAY | Saratoga Investment Corp is a US business development company; Sparta Capital Ltd appears Canadian. The CA ISIN is more  |
| CA8472431029 | Canada | distinct_issuers | 0.80 | NYSE::SRI | Stoneridge Inc is a US automotive electronics company; Sparton Resources is Canadian. CA ISIN fits Sparton. |
| CA85236T1030 | Canada | distinct_issuers | 0.80 | BATS::SECU | The iShares ETF is likely a US-domiciled product trading on BATS; SSC Security Services is Canadian. The CA ISIN is prob |
| CA85853F1053 | Canada | distinct_issuers | 0.80 | NASDAQ::SJ | Canada-based ISIN likely belongs to Canadian company Stella-Jones Inc; Scienjoy Holding Corp is a Cayman Islands-incorpo |
| CA8682112021 | Canada | distinct_issuers | 0.80 | NYSE::SUI | ISIN 'CA' prefix indicates Canadian issuer; Superior Mining International Corporation appears Canadian, while Sun Commun |
| CA87132P1027 | Canada | distinct_issuers | 0.80 | NASDAQ::SYZ | Canadian ISIN likely assigned to Canadian company Sylogist Ltd; the Lazard ETF is a US-listed fund and should have a US  |
| CA8765111064 | Canada | distinct_issuers | 0.90 | NYSE::TKO | Canadian ISIN and registered country indicate Taseko Mines Ltd, a Canadian mining company, is the correct issuer; TKO Gr |
| CA8787423034 | Canada | distinct_issuers | 0.90 | OTC::TPTJF | Canadian ISIN matches Canadian company Teck Resources Limited; Topps Tiles Plc is a UK company and should have a GB ISIN |
| CA8864531097 | Canada | distinct_issuers | 0.90 | NYSE ARCA::TWM | The ISIN is registered in Canada. TWMIF (Tidewater Midstream and Infrastructure Ltd) is a Canadian energy company, consi |
| CA88651M1086 | Canada | distinct_issuers | 0.90 | BATS::TSLV | The ISIN is Canadian. TSLVF (Tier One Silver Inc) appears to be a Canadian mining company, while TSLV (Azoria TSLA Conve |
| CA88831E1097 | Canada | distinct_issuers | 0.90 | NASDAQ::TLA | The ISIN is Canadian. TPCFF (Titan Logix Corp) is a Canadian technology company, while TLA (GraniteShares Autocallable T |
| CA89072T1021 | Canada | distinct_issuers | 0.90 | NASDAQ::TOI | The ISIN is Canadian. TOITF (Topicus.com Inc) is a Canadian technology company, while TOI (Oncology Institute Inc) is a  |
| CA91360F1099 | Canada | distinct_issuers | 0.90 | NYSE MKT::IBO | The ISIN is Canadian. IBOGF (Universal Ibogaine Inc) appears to be a Canadian biotech company, while IBO (Impact BioMedi |
| CA92537Y1043 | Canada | distinct_issuers | 0.40 | NASDAQ::FORA | The listings represent two different companies: Forian Inc (a US healthcare analytics firm) and VerticalScope Holdings I |
| CA92847V5018 | Canada | distinct_issuers | 0.40 | NYSE::VHI | Valhi Inc is a US-based holding company, while Vitalhub Corp is a Canadian health tech firm. The ISIN's country of regis |
| CA9316741052 | Canada | distinct_issuers | 0.30 | TSXV::WLR | Both entities are Canadian resource companies, but they are distinct issuers. The OTC ticker CMCXF aligns with CMC Metal |
| CA9323971023 | Canada | distinct_issuers | 0.60 | NYSE::WM | Waste Management Inc is a major US corporation and would not normally be associated with a Canadian ISIN. Wallbridge Min |
| CA98979N1006 | Canada | distinct_issuers | 0.40 | NYSE MKT::ZONE | CleanCore Solutions is a US company, while Zonetail Inc is Canadian. The Canadian ISIN more likely belongs to Zonetail;  |
| CH0011339204 | Switzerland | distinct_issuers | 0.95 | OTC::ASCN | Ascom Holding AG is a Swiss technology company; Absecon Bancorp is a US bank. The ISIN is Swiss-registered, consistent w |
| CH0030486770 | Switzerland | same_issuer | 0.99 | - | Both listings represent Daetwyler Holding AG, a Swiss industrial firm. 'Daetwyl I' on SIX is an abbreviated name. The IS |
| CH0305951201 | Switzerland | same_issuer | 0.99 | - | Walliser Kantonalbank (German) and Banque Cantonale du Valais (French) refer to the same Swiss cantonal bank. Both listi |
| CH0475986318 | Switzerland | distinct_issuers | 0.95 | NYSE::KEYS | Keysight Technologies is a US test equipment company; the SIX listing is a 21Shares crypto index ETP, a Swiss product. T |
| CH1130675676 | Switzerland | distinct_issuers | 0.95 | NASDAQ::ALTS | ALT5 Sigma Corporation is a US firm; the SIX listing is a Swiss-domiciled ETP. The Swiss ISIN is likely legitimate for t |
| CNE1000002Q2 | China | same_issuer | 1.00 | - | Both listings refer to Sinopec (China Petroleum & Chemical Corp); HKEX shares and OTC H shares are cross-listings of the |
| CNE100000338 | China | distinct_issuers | 1.00 | OTC::GVLMF | ISIN CNE100000338 belongs to Great Wall Motor Co Ltd (GWLLF). Greenvale Mining (GVLMF) is an unrelated company, likely A |
| CNE1000003W8 | China | distinct_issuers | 1.00 | OTC::PECN | ISIN CNE1000003W8 belongs to PetroChina H shares (PCCYF). Photoelectron Corp (PECN) is an unrelated entity, so its listi |
| CNE100001MK7 | China | same_issuer | 1.00 | - | Both listings refer to the People's Insurance Company (Group) of China (PICC Group); HKSE and OTC represent the same iss |
| COC07PA00027 | unknown | distinct_issuers | 1.00 | NYSE::MSA | ISIN COC07PA00027 is likely Colombian, matching Mineros S.A. (MNSAF). MSA Safety (MSA) is a US company, not associated w |
| DE0005137004 | Germany | distinct_issuers | 0.90 | NASDAQ::QBY | ISIN is German (DE prefix), matching q.beyond AG, a German company. The other listing is a US-listed ETF 'GraniteShares  |
| DE0005439004 | Germany | distinct_issuers | 0.90 | NYSE::CON | ISIN is German (DE prefix), and Continental AG is a well-known German automotive supplier. NYSE listing 'Concentra Group |
| DE0005909006 | Germany | distinct_issuers | 0.90 | NYSE ARCA::GBF | ISIN is German (DE prefix), and Bilfinger SE is a German engineering company. The NYSE Arca listing is an iShares ETF, a |
| DE000A1TNUT7 | Germany | same_issuer | 0.95 | - | Both listings represent Deutsche Beteiligungs AG ('DT.BETEILIG.AG NA O.N.' is an abbreviation). One listed on Xetra (hom |
| DE000A2E4SV8 | Germany | distinct_issuers | 0.90 | OTC::CYRD | ISIN is German (DE prefix), and CYAN AG is a German cybersecurity firm listed on Xetra. The OTC-traded 'CybeRecord Inc'  |
| DK0060670776 | Denmark | same_issuer | 0.70 | - | Names appear related: SJF likely abbreviates Sjaelland-Fyn. Both are Danish; LSE listing 0RD0 is likely a foreign line f |
| EGS3C4L1C015 | Egypt | distinct_issuers | 0.90 | NASDAQ::MEDP | ISIN prefix EG indicates Egypt. Medical Packaging Company is Egyptian; Medpace Holdings Inc is a US-based CRO. Names and |
| EGS67221C019 | Egypt | distinct_issuers | 0.90 | NYSE MKT::AGIG | Egyptian ISIN (EG prefix). Arab Moltaka Investments is an Egyptian company; Abundia Global Impact Group is a US entity. |
| EGS691L1C018 | Egypt | distinct_issuers | 0.90 | NASDAQ::ASPI | EG ISIN points to Egypt. Pioneers Holding is Egyptian, while ASP Isotopes is a US company. |
| EGS738I1C018 | Egypt | distinct_issuers | 0.90 | OTC::CNFN | Egyptian ISIN. Sarwa Capital Holding is an Egyptian firm; Cfn Enterprises appears to be a US entity. |
| ES0105130001 | Spain | distinct_issuers | 0.90 | OTC::DMPZF | Spanish company vs. UK-based pizza chain; country code ES aligns with Global Dominion, Domino's Pizza Group plc likely h |
| FI0009000251 | Finland | same_issuer | 0.85 | - | Both listings refer to the same Finnish retail group; Lindex Group is the rebranded Stockmann Oyj, sharing the same unde |
| FI4000519228 | Finland | same_issuer | 0.90 | - | WithSecure Oyj is the new name of F-Secure Oyj; both listings represent the same legal entity, only the OTC ticker still |
| FR0000053225 | France | distinct_issuers | 0.95 | NYSE::MMT | French TV broadcaster vs. US closed-end fund; ISIN starts with FR, matching Metropole Television. MFS Multimarket Income |
| FR0000074197 | France | distinct_issuers | 0.95 | NYSE::UTI | French IT services firm vs. US vocational school; French ISIN strongly indicates the Euronext listing is correct, NYSE l |
| FR0000120669 | France | distinct_issuers | 0.90 | Euronext::NAE | Esso Societe Anonyme Francaise SA is a well-known French oil company, while North Atlantic Energies appears to be a diff |
| FR0004007813 | France | distinct_issuers | 0.90 | OTC::COCSF | Kaufman Et Broad is likely Kaufman & Broad SA, a French homebuilder, matching the French ISIN. Coca-Cola FEMSA is a Mexi |
| FR0004034072 | France | distinct_issuers | 0.90 | Euronext::ALXIL | Xilam Animation is a known French animation studio and its ISIN is commonly FR0004034072. Passat Société Anonyme is a se |
| FR0007052782 | France | distinct_issuers | 0.95 | NASDAQ::CAC | The ISIN suggests a French issuer; the Amundi ETF is a French UCITS ETF, while Camden National Corporation is a US bank, |
| FR0010120402 | France | distinct_issuers | 0.60 | Euronext::ALTHE | The ISIN is believed to belong to Theraclion SA, which is listed OTC as TCLIF. The Euronext listing has ticker ALTHE, wh |
| FR0011550680 | France | distinct_issuers | 0.95 | NASDAQ::ESEA | BNP Paribas Easy S&P 500 UCITS H (ETF) versus Euroseas Ltd (shipping company) – clearly different issuers. ISIN is regis |
| FR0013185857 | France | distinct_issuers | 0.95 | NASDAQ::ABEO | Abéo SA (French sporting goods) versus Abeona Therapeutics (US biotech) - different issuers. ISIN prefix FR indicates Fr |
| FR001400NLM4 | France | same_issuer | 0.95 | - | Orpea SA changed its corporate name to EMEIS SA. The listings represent the same legal entity. |
| GB0001351955 | United Kingdom | same_issuer | 0.95 | - | D4t4 Solutions Plc was renamed Celebrus Technologies plc; both listings refer to the same UK company. |
| GB0001367019 | United Kingdom | distinct_issuers | 0.95 | NYSE::BLND | Blend Labs (US fintech) versus British Land (UK REIT) - different issuers. ISIN prefix GB points to British Land. |
| GB0001570810 | United Kingdom | distinct_issuers | 0.90 | NYSE ARCA::XAR | XAR is an ETF, XAARF is Xaar plc, a UK industrial inkjet manufacturer. The ISIN is GB-prefixed, matching the UK entity.  |
| GB0002074580 | United Kingdom | distinct_issuers | 0.90 | NYSE MKT::GNS | OTC ticker GENSF corresponds to Genus plc, a UK company; NYSE MKT's GNS is Genius Group Ltd, a different issuer. ISIN co |
| GB0003362992 | United Kingdom | distinct_issuers | 0.90 | NASDAQ::FTC | FTC is a US-listed ETF, FLTCF is Filtronic plc, a UK electronics firm. The GB ISIN aligns with the UK company, not the f |
| GB0004220025 | United Kingdom | uncertain | 0.20 | - | Neither listing appears to be a UK issuer: PCT is a US-based recycling company, AOTUF is a New Zealand property firm. Th |
| GB0004740477 | United Kingdom | distinct_issuers | 0.90 | NASDAQ::BIRD | BIRD is Allbirds, a US footwear company; BBRDF is Blackbird plc, a UK cloud video platform. The ISIN country points to t |
| GB0004915632 | United Kingdom | distinct_issuers | 0.90 | NYSE ARCA::KIE | ISIN GB0004915632 corresponds to Kier Group plc (OTC:KIERF). NYSE ARCA:KIE is an ETF from a different issuer. |
| GB0006449366 | United Kingdom | distinct_issuers | 0.70 | NASDAQ::ECOR | ISIN appears to belong to Ecora Royalties PLC (OTC:ECRAF) based on name and country. NASDAQ:ECOR is a different US compa |
| GB0008782301 | United Kingdom | distinct_issuers | 0.90 | NASDAQ::TW | ISIN GB0008782301 is Taylor Wimpey plc (OTC:TWODF). NASDAQ:TW is a different entity. |
| GB0030927254 | United Kingdom | distinct_issuers | 0.90 | NYSE::ASC | ISIN GB0030927254 is ASOS Plc (OTC:ASOMF). NYSE:ASC is a different shipping company. |
| GB0034330679 | United Kingdom | distinct_issuers | 0.80 | LSE::CLBX | ISIN GB0034330679 is likely ANGLE plc (OTC:ANPCF). LSE:CLBX appears to be a separate entity. |
| GB00B0MDF233 | United Kingdom | distinct_issuers | 0.95 | SET::PRINC | Plexus Holdings plc is a UK oil and gas services company, while Principal Capital Public Company Limited is a Thai inves |
| GB00B44LQR57 | United Kingdom | distinct_issuers | 0.95 | NASDAQ::CDL | Cloudbreak Discovery Plc is a UK-based natural resource exploration company, while VictoryShares US Large Cap High Div V |
| GB00BK6RLF66 | United Kingdom | distinct_issuers | 0.80 | OTC::AERS | Aquila European Renewables Income PLC is a UK renewable energy investment company, while Aerius International Inc appear |
| GB00BKM0ZJ18 | United Kingdom | distinct_issuers | 0.95 | NASDAQ::PRE | Pensana Plc is a UK rare earth exploration company, while Prenetics Global Ltd is a genomics company domiciled in the Ca |
| GB00BLDRH360 | United Kingdom | same_issuer | 0.99 | - | OneSavings Bank PLC changed its name to OSB Group Plc. The LSE ticker OSB and OTC ticker OSBGF represent the same issuer |
| GB00BLY2F708 | United Kingdom | distinct_issuers | 1.00 | NYSE ARCA::CARD | CARD is an ETN, CRFCF is Card Factory plc, a UK retail company. GB ISIN likely belongs to the UK company. |
| GB00BNYDGZ21 | United Kingdom | distinct_issuers | 1.00 | OTC::MLXSF | MEX is Tortilla Mexican Grill (UK), MLXSF is Melexis (Belgium). GB ISIN matches UK company. |
| GI000A0F6407 | unknown | same_issuer | 1.00 | - | EVOKE PLC is the new name for 888 Holdings. Both listings represent the same Gibraltar-based company. |
| HK0291001490 | Hong Kong | distinct_issuers | 1.00 | OTC::CHKMF | CRHKF is China Resources Beer (HK-based), CHKMF is Cohiba Minerals (Australian mining). HK ISIN belongs to HK company. |
| IE0001R850E1 | Ireland | distinct_issuers | 1.00 | NASDAQ::SMID | FTGD is an Irish UCITS ETF, SMID is a US company. IE ISIN matches Irish-domiciled ETF. |
| IE0009Y1MQJ2 | Ireland | distinct_issuers | 0.95 | NASDAQ::FACT | FACT II Acquisition Corp. (SPAC) vs iShares ETF. The ISIN prefix IE indicates Irish issuer; the ETF is a UCITS ETF domic |
| IE000CM02H85 | Ireland | distinct_issuers | 0.95 | OTC::FLXT | Flexpoint Sensor Systems Inc. (operating company) vs Franklin FTSE Taiwan UCITS ETF. The ETF is Irish-domiciled, matchin |
| IE000IM4K4K2 | Ireland | distinct_issuers | 0.90 | NASDAQ::METU | Direxion Daily META Bull 2X Shares (likely US ETF) vs Franklin Metaverse UCITS ETF (Irish). The ISIN's IE prefix points  |
| IE000SBHVL31 | Ireland | distinct_issuers | 0.95 | OTC::ABIT | Athena Bitcoin Global (crypto company) vs AXA IM Biodiversity UCITS ETF. The ETF is an Irish UCITS, consistent with IE I |
| IE000Y9MG996 | Ireland | distinct_issuers | 0.95 | OTC::WEBB | Web Global Holdings Inc. (holding company) vs Amundi US Tech 100 UCITS ETF. The ETF is Irish-domiciled, matching IE pref |
| IE00BDQZ5152 | Ireland | distinct_issuers | 0.90 | OTC::ICBU | iShares USD Intermediate Credit Bond UCITS ETF is an Irish-domiciled ETF, consistent with the 'IE' ISIN prefix. Imd Comp |
| IE00BF2FN646 | Ireland | distinct_issuers | 0.90 | NYSE::TREX | Invesco US Treasury Bond UCITS ETF is an Irish-domiciled fund, matching the 'IE' prefix. Trex Company Inc is a US-based  |
| IE00BF2NR112 | Ireland | distinct_issuers | 0.90 | OTC::GRPI | Greencoat Renewables PLC is an Irish public limited company, aligning with the 'IE' ISIN. Grupo International Inc appear |
| IE00BH3YZ803 | Ireland | distinct_issuers | 0.90 | NYSE ARCA::USML | Invesco S&P SmallCap 600 UCITS ETF is typically Irish-domiciled, matching 'IE'. The ETRACS ETN is a UBS-issued product,  |
| IE00BHZRQZ17 | Ireland | distinct_issuers | 0.90 | OTC::FLXI | Franklin FTSE India UCITS ETF is an Irish-domiciled fund, consistent with 'IE'. FlexiInternational Software Inc is a sof |
| IE00BKVD2N49 | Ireland | distinct_issuers | 0.95 | OTC::STKKF | Seagate Technology Holdings plc is an Irish public limited company, matching the ISIN's registered country (Ireland). St |
| IE00BKWQ0M75 | Ireland | distinct_issuers | 0.95 | NYSE::SMC | The ISIN is Irish-registered. Exchange-traded funds, especially UCITS structures, are often domiciled in Ireland. Summit |
| IE00BL0L0D23 | Ireland | distinct_issuers | 0.95 | NASDAQ::CAPS | Irish ISIN likely belongs to an Irish-domiciled fund. First Trust Capital Strength ETF is likely structured as an Irish  |
| IE00BL6K6H97 | Ireland | distinct_issuers | 0.95 | NASDAQ::TIGR | The ISIN is Irish. L&G India INR Government Bond UCITS ETF is a UCITS ETF, commonly domiciled in Ireland. Up Fintech Hol |
| IE00BYMS5W68 | Ireland | distinct_issuers | 0.95 | NASDAQ::FTEK | The Irish ISIN is consistent with a UCITS ETF (often Irish-domiciled). Fuel Tech Inc is a US-based industrial company, s |
| IE00BYX8XD24 | Ireland | distinct_issuers | 0.30 | NYSE::SPME | The names indicate different issuer types: iShares Edge S&P 500 Minimum Volatility UCITS ETF is an Irish-domiciled UCITS |
| IE00BYXPSP02 | Ireland | distinct_issuers | 0.30 | NYSE::IBTA | Ibotta, Inc. is a US technology company listed on NYSE, while iShares $ Treasury Bond 1-3 UCITS Acc is an Irish-domicile |
| IL0002780109 | Israel | uncertain | 0.10 | - | Both members are Israeli companies listed on TASE with different names and tickers. Without external data, it is impossi |
| IL0003970188 | Israel | uncertain | 0.10 | - | The two companies have distinct names and tickers, indicating they are separate issuers. However, both are linked to the |
| IL0004940131 | Israel | uncertain | 0.10 | - | The listings represent different companies on the same exchange, yet share one ISIN. Such a conflict most likely arises  |
| IL0007590198 | Israel | distinct_issuers | 0.70 | OTC::BYSD | GavYam Lands Corp Ltd is an Israeli real estate company listed on TASE, while Bayside Corp on OTC appears to be a differ |
| IL0010846983 | Israel | distinct_issuers | 0.70 | OTC::HLAN | Hilan Ltd is an established Israeli technology company; Heartland Banccorp seems to be a US-based financial institution. |
| IL0010849045 | Israel | distinct_issuers | 0.70 | NASDAQ::BVC | BATM Advanced Communications Ltd. is a known Israeli tech firm; the OTC listing BVCLF typically represents its foreign s |
| IL0010901416 | Israel | distinct_issuers | 0.70 | TASE::TGI | Tadir-Gan (Precision Products) and TGI Infrastructures are distinct Israeli companies. Publicly available information su |
| IL0010970544 | Israel | distinct_issuers | 0.70 | TASE::SLCL | Israel China Biotechnology (ICB) and Silver Castle Holdings are separate entities. Based on typical ISIN assignments, IC |
| IL0011214744 | Israel | distinct_issuers | 0.90 | NYSE ARCA::ILDR | The Israel Land Development Company Ltd is an Israeli company listed on TASE, matching the ISIN's country. First Trust I |
| IL0011226540 | Israel | distinct_issuers | 0.90 | OTC::STRG | Medivie Therapeutic Ltd is listed on TASE and likely the rightful holder of an Israeli ISIN. Starguide Group Inc appears |
| IL0011702409 | Israel | distinct_issuers | 0.70 | SET::AQUA | The ISIN is Israeli, so its legitimate issuer should be Israeli. VELORYX LTD may be an Israeli company, while Aqua Corpo |
| IM00B3RLCZ58 | Isle of Man | distinct_issuers | 0.90 | NYSE::GEO | The ISIN prefix 'IM' indicates Isle of Man, matching Geodrill Limited (likely incorporated there). Geo Group Inc is a US |
| IT0003895668 | Italy | distinct_issuers | 0.90 | NYSE ARCA::ETH | Eurotech S.p.A. is an Italian company matching the ISIN's country. Grayscale Ethereum Mini Trust is a US crypto trust an |
| IT0005244402 | Italy | same_issuer | 0.90 | - | Banca Farmafactoring S.p.A. is an Italian bank commonly abbreviated as BFF; the OTC listing name 'BFF BK SPA' is highly  |
| JE00B2NFV134 | Jersey | distinct_issuers | 0.95 | NASDAQ::LPLA | LPL Financial Holdings Inc is a US financial services firm, while WisdomTree Platinum 2x Daily Leveraged EUR is a Jersey |
| JE00BDD9QD91 | Jersey | distinct_issuers | 0.95 | NYSE::LBRT | Liberty Energy Inc. is a US energy services company listed on NYSE; WisdomTree Brent Crude Oil 2x Daily Leveraged ETC is |
| JP3148970001 | Japan | distinct_issuers | 0.95 | OTC::IPGDF | IGO Limited is an Australian mining company (ASX: IGO), while IBOKIN Co.,Ltd. is listed on the Tokyo Stock Exchange. A J |
| JP3368000000 | Japan | same_issuer | 0.95 | - | Showa Denko K.K. changed its corporate name to Resonac Holdings Corporation and retains the same Tokyo Stock Exchange li |
| JP3399310006 | Japan | distinct_issuers | 0.90 | SET::SAK | Start Today Co., Ltd. is a Japanese company; Saksiam Leasing is Thai. The ISIN is Japanese, thus the Japanese company is |
| JP3735400008 | Japan | same_issuer | 0.95 | - | Nippon Telegraph & Telephone Corp is NTT, which trades on TSE as 9432. The OTC listing is an ADR of the same company. |
| JP3965410008 | Japan | distinct_issuers | 0.70 | TSE::207A | Aisin Corporation is a major Japanese manufacturer; Rising Corporation Inc. is not known to be related. The ISIN is Japa |
| KYG1827K1076 | Cayman Islands | uncertain | 0.30 | - | Two distinct NASDAQ-listed Cayman Islands companies sharing the same ISIN, likely a data error, but insufficient informa |
| KYG212151016 | Cayman Islands | same_issuer | 0.90 | - | China Renaissance Holdings Limited trades on HKEX under code 1911, and 'CR HOLDINGS' likely stands for China Renaissance |
| KYG217651051 | Cayman Islands | same_issuer | 0.90 | - | Both listings refer to CK Hutchison Holdings; CKH HOLDINGS is an abbreviation of the full name. |
| KYG367381053 | Cayman Islands | distinct_issuers | 0.85 | OTC::FDDMF | Fresh Del Monte Produce Inc and FDM Group are different companies. Based on typical ISIN mapping, KYG367381053 is known  |
| KYG4587A1031 | Cayman Islands | same_issuer | 0.90 | - | HKET HOLDINGS is an abbreviation of Hong Kong Economic Times Holdings; ticker matches. |
| KYG7814S1021 | Cayman Islands | same_issuer | 0.90 | - | SA SA INT'L is an abbreviation of Sa Sa International Holdings; ticker matches. |
| KYG8167W1380 | Cayman Islands | same_issuer | 0.80 | - | Sino Biopharmaceutical Limited trades under ticker 01177 on HKEX; SBP GROUP may refer to the same company (SBP possibly  |
| KYG876361257 | Cayman Islands | distinct_issuers | 0.80 | NYSE::TPL | ISIN prefix KYG indicates Cayman Islands incorporation. Tethys Petroleum Limited is a Cayman-incorporated oil and gas co |
| LR0008862868 | unknown | distinct_issuers | 0.90 | OTC::RCLFF | ISIN prefix LR corresponds to Liberia. Royal Caribbean Group is a cruise line incorporated in Liberia. RCL Foods Limited |
| LU0460391732 | Luxembourg | distinct_issuers | 0.90 | OTC::XSVT | ISIN prefix LU indicates Luxembourg domicile, common for UCITS ETFs. The db x-trackers fund is a UCITS ETF, while Xsovt  |
| LU0832435464 | Luxembourg | distinct_issuers | 0.90 | NASDAQ::LVO | Luxembourg ISIN typically assigned to UCITS ETFs. The Lyxor S&P 500 VIX ETF is a Luxembourg fund. LiveOne Inc is a US me |
| LU1287023185 | Luxembourg | distinct_issuers | 0.90 | NYSE::MTD | ISIN prefix LU indicates Luxembourg domicile. The Amundi ETF is a UCITS fund. Mettler-Toledo International Inc is a glob |
| LU1287023342 | Luxembourg | distinct_issuers | 0.95 | NYSE::MAA | MAA is a US REIT with a US ISIN; LYXA is a Luxembourg-domiciled UCITS ETF whose ISIN is LU1287023342. |
| LU1602144575 | Luxembourg | distinct_issuers | 0.95 | NYSE::CMU | CMU is a US closed-end fund investing in municipal bonds, typically with a US ISIN; AMED is a Luxembourg-domiciled Amund |
| LU1681044720 | Luxembourg | distinct_issuers | 0.95 | NYSE::CSW | CSW Industrials is a US-domiciled company with a US ISIN; 540J is a Luxembourg-domiciled Amundi ETF whose ISIN is LU1681 |
| LU1686830909 | Luxembourg | distinct_issuers | 0.95 | NYSE ARCA::LEMB | LEMB is a US-domiciled iShares ETF (ISIN US4642865174); LYQS is a Luxembourg-domiciled Amundi ETF, matching the LU ISIN. |
| LU1829218749 | Luxembourg | distinct_issuers | 0.40 | Euronext::COMO | Both are European-domiciled ETFs (Amundi on Euronext, Lyxor on Xetra). They are distinct funds, so one ISIN is misassign |
| LU1841731745 | Luxembourg | distinct_issuers | 0.90 | OTC::LCCN | One listing is an ETF (Lyxor MSCI China UCITS ETF) and the other is a corporation (LeapCharger Corporation); they are cl |
| LU1953136287 | Luxembourg | distinct_issuers | 0.90 | NASDAQ::ASRT | The Euronext listing is a BNP Paribas UCITS ETF, while NASDAQ lists Assertio Therapeutics, a pharmaceutical company. The |
| LU2090063673 | Luxembourg | distinct_issuers | 0.90 | OTC::NADA | The SIX listing is a Lyxor MSCI Japan UCITS ETF, while OTC lists a telecom company (North American DataCom). Luxembourg  |
| LU2244387457 | Luxembourg | distinct_issuers | 0.90 | OTC::ASRE | Euronext listing is a BNP Paribas UCITS ETF, while OTC lists an energy company (Astra Energy). The Luxembourg ISIN fits  |
| MHY2065G1219 | Marshall Islands | distinct_issuers | 0.80 | TSX::DHT-UN | DHT Holdings is a Marshall Islands shipping company, matching the ISIN's registration country. DRI Healthcare Trust is a |
| MT0002400118 | Malta | same_issuer | 1.00 | - | UIE PLC is the short name; both listings represent the same issuer. |
| MX01GC2M0006 | Mexico | distinct_issuers | 1.00 | NYSE ARCA::GCC | WisdomTree Continuous Commodity Index Fund is an ETF, while Grupo Cementos de Chihuahua is a Mexican cement company. The |
| MX01OM000018 | Mexico | same_issuer | 1.00 | - | Both listings represent the same airport operator, with OMAB being the NASDAQ listing and GAERF the OTC. |
| MX01Q0000008 | Mexico | distinct_issuers | 1.00 | NYSE::Q | Qnity Electronics, Inc. is an electronics firm, while Quálitas Controladora is a Mexican insurance holding. The ISIN bel |
| MXP001661018 | Mexico | same_issuer | 1.00 | - | Both represent ASUR, a Mexican airport operator, listed on BMV and OTC. |
| NL0009169515 | Netherlands | uncertain | 0.30 | LSE::0MNA | Both listings are Dutch-related, but the names differ significantly. The ISIN is Dutch, suggesting the Amsterdam listing |
| NL0010556726 | Netherlands | distinct_issuers | 0.95 | NYSE::IEX | HAWICK DATA NV is a Dutch company, while IDEX Corporation is a US manufacturer. The ISIN NL0010556726 belongs to a Dutch |
| NO0010310956 | Norway | distinct_issuers | 0.95 | OTC::SALM | SalMar ASA is a Norwegian seafood company, while Salem Media Group is a US media firm. The ISIN is Norwegian, matching S |
| NO0012470089 | Norway | distinct_issuers | 0.95 | OTC::TMCGF | Tomra Systems ASA is a Norwegian recycling company, while TomCo Energy Plc is a UK oil shale explorer. The ISIN is Norwe |
| NZAIRE0001S2 | New Zealand | distinct_issuers | 0.95 | NYSE::AIZ | Air New Zealand Limited is a New Zealand airline, while Assurant Inc is a US insurance company. The ISIN starts with NZ, |
| PK0052901011 | Pakistan | distinct_issuers | 0.40 | PSX::FECM | Elahi Cotton Mills Limited and First Elite Capital Modaraba are different types of entities, likely distinct issuers. As |
| PLCRTCH00017 | Poland | distinct_issuers | 0.80 | OTC::CNVVF\|OTC::SMGKF | Both listed entities are UK companies (ConvaTec Group Plc and Smiths Group plc) while the ISIN is registered in Poland.  |
| SE0000862997 | Sweden | same_issuer | 0.90 | - | BillerudKorsnas AB and Billerud AB likely refer to the same issuer, with Billerud AB being an older name. The ISIN is Sw |
| SG1CI1000004 | Singapore | same_issuer | 0.95 | - | MANULIFE US REIT (OTC) and ManulifeReit USD (SGX) are clearly the same real estate investment trust, with BTOU being the |
| SG1M51904654 | Singapore | same_issuer | 0.95 | - | CapitaLand Integrated Commercial Trust and CapLand IntCom T are the same issuer; C38U is the SGX listing and CPAMF is it |
| SG1S04926220 | Singapore | same_issuer | 1.00 | - | Both listings refer to the same entity: Oversea-Chinese Banking Corporation Limited, commonly known as OCBC Bank. |
| SG1T75931496 | Singapore | same_issuer | 1.00 | - | Both listings represent Singapore Telecommunications Limited; Singtel is its brand name, and 'Singtel 10' likely denotes |
| SG1V52937132 | Singapore | same_issuer | 1.00 | - | Both listings refer to Parkway Life Real Estate Investment Trust; 'ParkwayLife Reit' is an abbreviated form of the same  |
| SG9999003735 | Singapore | distinct_issuers | 0.80 | NYSE ARCA::XPP | ProShares Ultra FTSE China 50 is a US-domiciled ETF managed by ProShares and would typically have a US ISIN. XP Power Li |
| SGXC50067435 | Singapore | same_issuer | 1.00 | - | Both listings clearly denote Digital Core REIT, with 'DigiCore Reit USD' being a variation of the name. |
| TH6041010005 | Thailand | distinct_issuers | 0.40 | SET::SENX | JSP Pharmaceutical and Sena J Property are different companies with distinct business activities; ISIN likely belongs to |
| US00182C1036 | United States | distinct_issuers | 0.70 | OTC::BSFAF | ANI Pharmaceuticals is a US company on NASDAQ, while BSF Enterprise is a UK company trading OTC as a foreign ordinary; t |
| US00809M1045 | United States | distinct_issuers | 0.70 | ASX::AIH | Aesthetic Medical International is a Chinese company with NASDAQ ADR, typifying a US ISIN; Advanced Innerergy is Austral |
| US0255371017 | United States | distinct_issuers | 0.90 | OTC::AEPLF | US0255371017 is well-known as American Electric Power’s ISIN; Anglo-Eastern Plantations is a completely separate UK-base |
| US0755712082 | United States | same_issuer | 0.60 | - | bebe stores, inc. likely renamed to TGE Value Creative Solutions Corp, with residual OTC ticker BDST retaining the old n |
| US0893021032 | United States | same_issuer | 0.70 | - | The LSE listing appears to be a cross-listing or a name variant of the OTC-listed Big Lots, Inc., suggesting both refer  |
| US09076D1019 | United States | distinct_issuers | 0.30 | OTC::AIMV | AI Maverick Intel Inc. and Bionoid Pharma Inc are clearly different companies. Without external data, Bionoid Pharma app |
| US1011211018 | United States | same_issuer | 0.90 | - | Boston Properties Inc. is the former name of BXP, Inc.; thus both listings represent the same issuer. |
| US1251411013 | United States | distinct_issuers | 0.60 | OTC::GPIPF | CECO Environmental Corp. and WesCan Energy Corp. are unrelated; the ISIN likely belongs to the NASDAQ-listed CECO. |
| US1270971039 | United States | same_issuer | 0.90 | - | Cabot Oil & Gas Corp. rebranded to Coterra Energy Inc., so both listings refer to the same entity. |
| US16936J2024 | United States | distinct_issuers | 0.90 | OTC::CALIQ | iShares ETF is a fund managed by BlackRock, while China Auto Logistics is an automotive exporter. They are unrelated ent |
| US16954L2043 | United States | distinct_issuers | 0.80 | OTC::COPJF | 51Talk is a Chinese education company listed on NYSE American, while Amplitude Energy is an Australian oil and gas firm  |
| US1747721033 | United States | distinct_issuers | 0.40 | OTC::CNBL | Bonvenu Bancorp and Citizens National Bancshares are separate community banks. Both trade OTC with different tickers, so |
| US25746U1097 | United States | distinct_issuers | 0.95 | SET::DOD | Dominion Energy is a major U.S. utility, while DOD Biotech is a Thai biotech firm. No plausible connection; ISIN undoubt |
| US2910111044 | United States | distinct_issuers | 0.95 | OTC::EOGSF | Emerson Electric is a large U.S. industrial conglomerate, while Emerald Resources is an Australian gold miner. They are  |
| US2972842007 | United States | distinct_issuers | 0.60 | OTC::ESLOY | EssilorLuxottica (ESLOF) and Essilor International SA (ESLOY) are distinct legal entities. The ISIN likely belongs to Es |
| US3134003017 | United States | same_issuer | 1.00 | - | Both listings represent the Federal Home Loan Mortgage Corporation (Freddie Mac) on different exchanges; they are the sa |
| US3135861090 | United States | same_issuer | 1.00 | - | Both listings represent the Federal National Mortgage Association (Fannie Mae) on different exchanges; they are the same |
| US46187W1071 | United States | distinct_issuers | 0.90 | NASDAQ::IVVD | Invivyd Inc. (IVVD) and Invitation Homes Inc. (INVH) are unrelated, distinct companies. The ISIN is widely associated wi |
| US55279B3015 | United States | distinct_issuers | 0.60 | NASDAQ::LITS | MEI Pharma Inc. and Lite Strategy, Inc. appear to be separate companies. The ISIN likely corresponds to MEI Pharma, and  |
| US6247561029 | United States | distinct_issuers | 0.80 | NASDAQ::MUD | NASDAQ::MUD is a leveraged ETF tracking Micron Technology, while NYSE::MLI is an industrial manufacturing company. The n |
| US6261881063 | United States | same_issuer | 0.90 | - | Both listings refer to Münchener Rückversicherungs-Gesellschaft (Munich Re). MURGF and MURGY are different OTC symbols f |
| US64131A1051 | United States | distinct_issuers | 0.80 | OTC::NNRRF | NASDAQ::STIM (Neuronetics Inc) is a U.S. medical technology company, while OTC::NNRRF (NRC Group ASA) is a Norwegian inf |
| US64828T2015 | United States | same_issuer | 0.90 | - | New Residential Investment Corp. changed its name to Rithm Capital Corp. LSE::0K76 and NYSE::RITM represent the same com |
| US65343D1000 | United States | distinct_issuers | 0.80 | OTC::NXEN | BATS::GTOS is a fixed-income ETF managed by Invesco, while OTC::NXEN is a biotechnology company. The two are fundamental |
| US6757466064 | United States | same_issuer | 0.90 | - | Ocwen Financial Corp. rebranded to Onity Group Inc.; the two listings represent the same entity cross-listed on LSE and  |
| US7163821066 | United States | distinct_issuers | 0.60 | OTC::CELX | PetMed Express Inc. and Celexpress Inc. are clearly different companies. PetMed Express is a known NASDAQ-listed US comp |
| US8168511090 | United States | distinct_issuers | 0.90 | OTC::SRRLF | Sempra Energy (NYSE:SRE) and Sirius Real Estate Limited are distinct entities. ISIN US8168511090 is typically assigned t |
| US82837P4081 | United States | distinct_issuers | 0.50 | NYSE::SI | Silvergate Capital Corp. (formerly NYSE:SI, now OTC:SICPQ) and Shoulder Innovations, Inc. are separate companies. The IS |
| US83192H1068 | United States | distinct_issuers | 0.90 | SET::SDC | SmileDirectClub Inc. (OTC:SDCCQ) and Samart Digital Public Company Limited (SET:SDC) are unrelated. The ISIN is associat |
| US8322482071 | United States | distinct_issuers | 0.90 | OTC::FLLLF | Smithfield Foods is a well-known issuer; Ultra Brands Ltd is unrelated. Likely the ISIN belongs to Smithfield. |
| US90255U1060 | United States | uncertain | 0.30 | - | Both are obscure OTC issuers with no clear link; cannot determine rightful ISIN holder without additional data. |
| US98421K1007 | United States | distinct_issuers | 0.70 | OTC::XNDA | Goldwind is a known international company; Xinda International Corp likely a different entity, so the ISIN probably belo |

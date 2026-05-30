# DeepSeek ISIN Identity Collision Validation

Generated: `2026-05-30T05:19:30Z`

Policy: DeepSeek triage is advisory only and authorizes no ISIN, country, or name change.

## Summary

| Metric | Value |
| --- | ---: |
| Validated groups | 60 |
| Agrees with detector | 50 |
| Disagrees with detector | 10 |
| Errors | 0 |

## Verdicts

| ISIN | Country | Classification | Conf | Likely misassigned | Rationale |
| --- | --- | --- | ---: | --- | --- |
| CA08663L1040 | Canada | distinct_issuers | 0.90 | NYSE ARCA::SPXU | ProShares UltraPro Short S&P500 is a US-listed ETF managed by ProShares; Global X S&P 500 Index Bull ETF is a Canadian E |
| US9621661043 | United States | distinct_issuers | 0.90 | OTC::AIABF\|OTC::CIMDF\|OTC::DYNDF\|OTC::GEBHF\|OTC::GMALF\|OTC::KLKBY\|OTC::MLYNF\|OTC::PNAGF\|OTC::RMGNF\|OTC::RNMBF\|OTC::SDPNF\|OTC::SIMEF\|OTC::TNABF | US9621661043 is historically assigned to Weyerhaeuser Company, a US firm. The OTC listings represent multiple unrelated  |
| DE000A161408 | Germany | distinct_issuers | 0.90 | OTC::HLFGY\|OTC::HLTFF | HelloFresh SE (German meal-kit company) and Hilton Food Group plc (UK food packing) are separate entities. The ISIN pref |
| IE00B6R52259 | Ireland | distinct_issuers | 0.90 | NASDAQ::SSAC | This ISIN is the iShares MSCI ACWI UCITS ETF, traded under multiple tickers/exchanges. NASDAQ::SSAC is SPACSphere Acquis |
| LU1900066207 | Luxembourg | distinct_issuers | 0.90 | OTC::BRANF | Lyxor MSCI Brazil UCITS ETF (domiciled in Luxembourg) is listed across European exchanges under different tickers/curren |
| NL0015000IY2 | Netherlands | distinct_issuers | 0.70 | OTC::NZSCF | The majority of listings are consistent with Universal Music Group N.V.; one listing (OTC::NZSCF) refers to a completely |
| US57636Q1040 | United States | distinct_issuers | 0.70 | ASX::MA1\|LSE::MAST | ISIN US57636Q1040 is widely known as Mastercard Inc.; two members clearly represent different entities (MA Credit Income |
| CA53056H1047 | Canada | distinct_issuers | 0.70 | OTC::PGWFF | Three listings are for Liberty Gold Corp., a Canadian mining company, while PGWFF is PGG Wrightson, an unrelated company |
| CA8119161054 | Canada | distinct_issuers | 0.70 | OTC::BCDRF\|OTC::SARDF | Two listings are for Seabridge Gold Inc.; the other two are clearly different companies (a Spanish bank and a New Zealan |
| CH0012142631 | Switzerland | distinct_issuers | 0.70 | OTC::CNHHY\|OTC::CRRNF | Clariant AG is the issuer associated with this Swiss ISIN; the two OTC listings for Cairn Homes plc are clearly a differ |
| CH0024638212 | Switzerland | distinct_issuers | 0.98 | SET::SHR | Schindler Holding AG is a Swiss elevator manufacturer, while S Hotels and Resorts is a Thai hospitality company. The ISI |
| CH0445689208 | Switzerland | distinct_issuers | 0.98 | BATS::HODL | 21Shares Crypto Basket Index ETP is a Swiss product matching the CH ISIN and registered country. VanEck Bitcoin Trust is |
| FR0000052292 | France | distinct_issuers | 0.98 | OTC::HMIFF | Hermes International SCA is a French luxury goods company, matching the FR ISIN and registered country. Harvest Minerals |
| FR0000121147 | France | distinct_issuers | 0.98 | OTC::FRSAF | Forvia SE (formerly Faurecia) is a French automotive supplier, consistent with the FR ISIN. First Au Limited is a gold e |
| FR0000125007 | France | distinct_issuers | 0.98 | OTC::CODMF | Compagnie de Saint-Gobain is a French industrial group, matching the FR ISIN. Coda Minerals Limited is an Australian min |
| FR0010331421 | France | distinct_issuers | 0.90 | OTC::IPHLF | Three listings refer to Innate Pharma, a French biotech. OTC::IPHLF is IPH Limited, an Australian IP services firm, clea |
| FR0010375766 | France | uncertain | 0.30 | LSE::INRU | ISIN normally belongs to Lyxor MSCI India UCITS ETF. LSE::INRU name suggests a different ETF (Amundi MSCI India II), pos |
| FR0010435297 | France | same_issuer | 0.70 | - | All listings refer to the Lyxor/Amundi MSCI Emerging Markets UCITS ETF family. Slight name variations likely reflect cur |
| GB0004526900 | United Kingdom | distinct_issuers | 0.95 | OTC::LLDTF\|OTC::LLOBF | ISIN GB0004526900 is known for IG Design Group plc. Two OTC listings clearly belong to Lloyds Banking Group, a completel |
| IE0032895942 | Ireland | distinct_issuers | 0.90 | NASDAQ::LQDA | ISIN IE0032895942 corresponds to iShares $ Corp Bond UCITS ETF. Liquidia Technologies is a biopharmaceutical company, no |
| IE00B02KXK85 | Ireland | distinct_issuers | 0.95 | NYSE ARCA::FXC | Three listings are iShares China Large Cap UCITS ETF (name variants); one is Invesco CurrencyShares Canadian Dollar Trus |
| IE00B3B8PX14 | Ireland | distinct_issuers | 0.95 | OTC::EGIL | Three listings are iShares Global Inflation Linked Government Bond UCITS ETF; OTC::EGIL is EdgeTech International Inc, a |
| IE00BKWQ0C77 | Ireland | distinct_issuers | 0.95 | OTC::SPYR | Three listings are SPDR MSCI Europe Consumer Discretionary UCITS ETF (different currency lines); OTC::SPYR is SPYR Inc,  |
| IE00BKWQ0N82 | Ireland | distinct_issuers | 0.95 | NYSE ARCA::SPYT\|NYSE::STT\|OTC::STTX | Only LSE::TELE matches the SPDR ETF; SPYT is a Defiance ETF, STT is State Street Corp stock, STTX is Stratex Oil & Gas—a |
| IE00BKWQ0P07 | Ireland | distinct_issuers | 0.95 | NYSE ARCA::SPYU | Three listings are SPDR MSCI Europe Utilities UCITS ETF variants; NYSE ARCA::SPYU is a leveraged ETN from a different is |
| IE00BMG6Z448 | Ireland | distinct_issuers | 0.95 | OTC::EXCH | Three listings are iShares ETF variants, one is a completely different company (Exchange Bankshares Inc). The ISIN is Ir |
| LU1900066975 | Luxembourg | distinct_issuers | 0.95 | BATS::LKOR | Three Lyxor MSCI Korea ETF listings share the Luxembourg ISIN; LKOR is a different issuer (FlexShares) and likely misass |
| SE0001662230 | Sweden | distinct_issuers | 0.95 | OTC::HRZMF | Three Husqvarna AB listings, one Horizon Minerals. Swedish ISIN belongs to Husqvarna. |
| US4370761029 | United States | distinct_issuers | 0.95 | OTC::HDNRF | Three Home Depot listings, one HomeCo Daily Needs REIT. US ISIN known to be Home Depot. |
| US45170X2053 | United States | distinct_issuers | 0.95 | NYSE ARCA::ALAI\|NYSE ARCA::CNEQ\|NYSE ARCA::INVN | Identiv Inc (INVE) is a distinct company from The Alger ETF Trust. US CUSIP 45170X205 corresponds to Identiv. |
| US5732841060 | United States | distinct_issuers | 0.95 | OTC::MMXLF\|TSXV::MMX | Martin Marietta Materials Inc. and Mustang Minerals Limited are distinct companies; ISIN belongs to Martin Marietta. |
| US59156R1086 | United States | distinct_issuers | 0.95 | OTC::MTLZF | MetLife Inc. and Metall Zug AG are distinct companies; ISIN belongs to MetLife. |
| US5949724083 | United States | same_issuer | 0.95 | - | All listings relate to MicroStrategy Incorporated (recently rebranded to Strategy Inc). |
| US7181721090 | United States | distinct_issuers | 0.95 | OTC::PASMF\|OTC::POTMF | Philip Morris International Inc. is distinct from Premier Miton Group plc and Portmeirion Group PLC; ISIN belongs to Phi |
| AT0000758305 | Austria | distinct_issuers | 0.95 | NASDAQ::PAL | Palfinger AG and Proficient Auto Logistics, Inc. are distinct companies; ISIN belongs to Palfinger. |
| AT0000785555 | Austria | distinct_issuers | 0.95 | NYSE::SEM | LSE and OTC listings represent Semperit AG Holding, an Austrian company, while NYSE:SEM is Select Medical Holdings, a US |
| AT0000969985 | Austria | distinct_issuers | 0.95 | NYSE::ATS | LSE and OTC listings are AT&S Austria, while NYSE:ATS is ATS Corporation (Canada). ISIN AT matches the Austrian company. |
| BE0003739530 | Belgium | distinct_issuers | 0.95 | NYSE::UCB | LSE and OTC listings are UCB SA, a Belgian pharma company. NYSE:UCB is United Community Banks, a US bank. ISIN BE matche |
| BE0003755692 | Belgium | distinct_issuers | 0.95 | OTC::ALGEF | Euronext and LSE listings are Agfa-Gevaert, while OTC:ALGEF is Alligator Energy Limited (Australia). ISIN BE belongs to  |
| BMG2178K1009 | Bermuda | same_issuer | 0.95 | - | All three listings represent CK Infrastructure Holdings (Bermuda registered) with minor name variations. No evidence of  |
| CA0042721005 | Canada | distinct_issuers | 0.90 | OTC::ANDMF | Acadian Timber Corp (ACAZF/ADN) is a Canadian company, consistent with the ISIN prefix. Andromeda Metals Limited is a di |
| CA13321L1085 | Canada | distinct_issuers | 0.90 | OTC::CCPPF | Cameco Corp is a Canadian company; LSE and NYSE listings are consistent. Capital & Counties Properties is a UK REIT, unr |
| CA15722J1030 | Canada | distinct_issuers | 0.90 | OTC::CTYMF | Ceylon Graphite Corp is a Canadian company (TSXV:CYL, OTC:CYLYF). Catalyst Metals Limited is an Australian gold explorer |
| CA24463V1013 | Canada | distinct_issuers | 0.90 | OTC::DTTLY\|SET::DUSIT | Defence Therapeutics Inc is a Canadian biotech company, matching the ISIN's country. Datatec (South Africa) and Dusit Th |
| CA68387G2036 | Canada | distinct_issuers | 0.90 | OTC::ESXMF | Optegra Ventures Inc is listed on TSXV and OTC as ESXMD. Essex Minerals Inc is a separate company; the ISIN likely belon |
| CA80100R4089 | Canada | distinct_issuers | 0.90 | NASDAQ::SANG | Information Services Corporation (ISC) is a Canadian company listed on TSX and OTC, while Sangoma Technologies Corp is a |
| CA89712R2019 | Canada | distinct_issuers | 0.90 | NYSE::TR\|OTC::TROLB | Troubadour Resources Inc is a Canadian mining company, while Tootsie Roll Industries is a U.S.-based confectionery compa |
| CH0023405456 | Switzerland | same_issuer | 0.95 | - | Dufry AG rebranded to Avolta AG in 2023. The listings LSE::0QK3 (Dufry), OTC::DFRYF (Avolta), and SIX::AVOL (Avolta) all |
| CH1385084384 | Switzerland | distinct_issuers | 0.90 | OTC::CSOL | SIX::CSOL-USD and XETRA::2SCS are listings for 21Shares Solana Core Staking ETP, a Swiss-issued exchange-traded product. |
| CNE100000Q35 | China | same_issuer | 0.95 | - | GAC GROUP is the short name for Guangzhou Automobile Group Co., Ltd. The HKEX listing (02238) and OTC tickers (GNZUF, GN |
| DE0005103006 | Germany | distinct_issuers | 0.95 | OTC::ARDDF | Adva Optical Networking SE (ADVA) is a German tech company, while Ardiden Limited is a different entity (likely an Austr |
| DE0005785604 | Germany | distinct_issuers | 0.95 | OTC::FTWYF | Fresenius SE & Co KGaA is a German healthcare group. Freightways Limited is a separate logistics company. The ISIN DE000 |
| DE0006047004 | Germany | distinct_issuers | 0.95 | NYSE::HEI | HeidelbergCement/Heidelberg Materials is a German building materials company. Heico Corporation is a US aerospace compan |
| DE0007010803 | Germany | distinct_issuers | 0.95 | NASDAQ::RAA | Rational AG is a German manufacturer. The NASDAQ listing is an ETF, a distinct security type. The ISIN DE0007010803 is f |
| DE0008019001 | Germany | same_issuer | 0.95 | - | All three listings—Deutsche Pfandbriefbank AG on LSE and OTC, and DT.PFANDBRIEFBK AG on Xetra—represent the same German  |
| DE000A0BL849 | Germany | same_issuer | 0.95 | - | VITA 34 AG changed its name to FAMICORD AG. LSE and OTC show old name, XETRA shows new name. |
| DE000A1X3XX4 | Germany | same_issuer | 0.95 | - | DIC Asset AG rebranded to BRANICKS Group AG. LSE still uses old name. |
| ES0137650018 | Spain | distinct_issuers | 0.95 | OTC::FDMIF | Fluidra S.A. and Founders Metals Inc. are distinct entities from Spain and Canada respectively. The Spanish ISIN likely  |
| FI4000312251 | Finland | same_issuer | 0.95 | - | Kojamo Oyj was formerly Lumo Kodit Oyj. HEL listing shows old name, while LSE and OTC show new name. |
| FR0000121014 | France | distinct_issuers | 0.95 | NYSE::MC\|OTC::MCCHF | LVMH is a French luxury goods company, while Moelis & Co and McChip Resources are unrelated companies from different cou |

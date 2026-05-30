# DeepSeek ISIN Identity Collision Validation

Generated: `2026-05-30T04:34:19Z`

Policy: DeepSeek triage is advisory only and authorizes no ISIN, country, or name change.

## Summary

| Metric | Value |
| --- | ---: |
| Validated groups | 12 |
| Agrees with detector | 12 |
| Disagrees with detector | 0 |
| Errors | 0 |

## Verdicts

| ISIN | Country | Classification | Conf | Likely misassigned | Rationale |
| --- | --- | --- | ---: | --- | --- |
| CA08663L1040 | Canada | distinct_issuers | 0.95 | NYSE ARCA::SPXU | The TSX listing is a Canadian-domiciled Global X ETF matching the ISIN's country code (CA). The NYSE ARCA listing is a U |
| US9621661043 | United States | distinct_issuers | 0.98 | OTC::AIABF\|OTC::CIMDF\|OTC::DYNDF\|OTC::GEBHF\|OTC::GMALF\|OTC::KLKBY\|OTC::MLYNF\|OTC::PNAGF\|OTC::RMGNF\|OTC::RNMBF\|OTC::SDPNF\|OTC::SIMEF\|OTC::TNABF | Weyerhaeuser is a US company consistent with ISIN country code. All OTC listings are unrelated Malaysian (and other) com |
| DE000A161408 | Germany | distinct_issuers | 0.95 | OTC::HLFGY\|OTC::HLTFF | HelloFresh SE is a German company matching the ISIN country. Hilton Food Group is a UK-based firm, distinct from HelloFr |
| IE00B6R52259 | Ireland | distinct_issuers | 0.97 | NASDAQ::SSAC | The iShares ETF is an Irish-domiciled UCITS ETF matching the ISIN country code. The NASDAQ listing is a SPAC, clearly a  |
| LU1900066207 | Luxembourg | distinct_issuers | 0.90 | OTC::BRANF\|SIX::LYRIO\|XETRA::LBRA | The ISIN is linked to a Lyxor Brazil ETF. LSE listings match typical name 'Lyxor UCITS Brazil (Ibovespa) C-EUR'. SIX/XET |
| NL0015000IY2 | Netherlands | distinct_issuers | 0.95 | OTC::NZSCF | All but OTC::NZSCF correspond to Universal Music Group, a Dutch music company. OTC::NZSCF is a seafood company, clearly  |
| US57636Q1040 | United States | distinct_issuers | 0.95 | ASX::MA1\|LSE::MAST | ISIN US57636Q1040 is widely known as Mastercard Inc. ASX::MA1 is a credit income trust, and LSE::MAST is an energy devel |
| CA53056H1047 | Canada | distinct_issuers | 0.95 | OTC::PGWFF | The ISIN is for Liberty Gold Corp, a Canadian mining company. OTC::PGWFF corresponds to PGG Wrightson, a New Zealand agr |
| CA8119161054 | Canada | distinct_issuers | 1.00 | OTC::BCDRF\|OTC::SARDF | Seabridge Gold Inc. is a Canadian gold mining company. Banco Santander is a Spanish bank, and Sanford Limited is a New Z |
| CH0012142631 | Switzerland | distinct_issuers | 1.00 | OTC::CNHHY\|OTC::CRRNF | Clariant AG is a Swiss specialty chemicals company. Cairn Homes plc is an Irish homebuilder. The ISIN is Swiss, so likel |
| CH0024638212 | Switzerland | distinct_issuers | 1.00 | SET::SHR | Schindler Holding AG is a Swiss elevator manufacturer. S Hotels and Resorts is a Thai hospitality company. The ISIN is S |
| CH0445689208 | Switzerland | distinct_issuers | 1.00 | BATS::HODL | 21Shares Crypto Basket Index ETP is a Swiss exchange-traded product. VanEck Bitcoin Trust is a US ETF managed by VanEck. |

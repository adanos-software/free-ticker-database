# Issue 449: nine ISIN identity groups

Reviewed on 2026-10-20 against main ff26aaec. One exact listing ISIN is cleared. Eight groups remain open with the blockers below. No same-issuer closure, replacement identifier, name, country, or version change is proposed. Direct automated identifier apply rows remain zero.

The issuer prospectus PDF page 23 binds the London GDR ticker FP. to a US identifier. The Canadian FP Newspapers ISIN is removed from LSE::FP. The historical US GDR identifier is not adopted because the issuer reports the London GDR delisted in April 2025.

| ISIN | Result | Evidence and blocker |
| --- | --- | --- |
| AU000000TLG7 | open_needs_official_identifier_evidence | Issuer ticker TLG identifies a German instrument, but a current or dated Vienna security-master observation binding VSE::TLG is still missing. Do not substitute a Frankfurt or ASX row. [Source](https://www.tlg.de/investor-relations/aktie/aktieninformationen) |
| CA3025862010 | cleared_misassigned_isin | Replacement and delisting status remain separate review items; no new ISIN, name, or country is adopted. [Source](https://www.fondulproprietatea.ro/files/live/sites/fondul/files/en/listing-materials/london/prospectus_final_with_electronic_disclaimer.pdf) |
| AU000000CRB1 | open_needs_official_identifier_evidence | The legacy CRB ETF symbol is absent from the current exact Euronext reference. A dated official CRB-to-ISIN record is needed before a clear or replacement. [Source](https://live.euronext.com/en/product_directory/data/etf-all-markets/download) |
| AU0000253502 | open_needs_official_identifier_evidence | The official Rush Gold bulletin is for CSE, not the stored NEO::RGN venue. Resolve the exact NEO listing identity or venue history before applying identifiers. [Source](https://thecse.com/bulletin/2025-0624-new-listing-rush-gold-corp-rgn/) |
| CA41752L1076 | open_needs_official_identifier_evidence | TMX confirms TSX::HBIE as the Harvest ETF; the cached master lacks its ISIN. Need an official exact HBIE identifier observation and a separate OTC::HBIE identity review. [Source](https://www.tsx.com/en/news/new-company-listings?id=1751) |
| CA5910881096 | open_needs_official_identifier_evidence | Metal Energy announced a 2025 consolidation with a new ISIN; authenticate the issuer or TSXV post-consolidation record and OTC::MERG identity independently. Do not copy identifiers between the homonyms. [Source](https://www.newsfile-prod.tmx.cloud/release/270743/Metal-Energy-Provides-Corporate-Update) |
| US0896951003 | open_needs_official_identifier_evidence | The historical official BIGG bulletin is for CSE, not TSXV; a subsequent name/consolidation change also exists. Need current exact TSXV and OTC identities before any fill or clear. [Source](https://thecse.com/bulletin/2019-1005-name-change-big-blockchain-intelligence-group-inc-bigg/) |
| NO0010861990 | open_needs_official_identifier_evidence | Prosafe confirms the Norwegian ISIN and Oslo PRS ticker, but TSXV::PRS-H still lacks an exact official Prism identifier observation. No transfer from Oslo or Frankfurt. [Source](https://www.prosafe.com/prosafe-se-final-results-of-the-exercise-period-for-warrants-issued-in-the-warrants-offering/) |
| SE0000722365 | open_needs_official_identifier_evidence | Vivesto documents the Frankfurt OMAX listing; a current or dated exact OTC::VIVE identifier observation is still missing. Do not apply a Nasdaq-era CUSIP as an OTC replacement. [Source](https://www.vivesto.com/en/wp-content/uploads/sites/2/2023/03/vivesto-ar-2022-eng-230425.pdf) |

The reports are manually reviewed evidence. They do not authorize automatic clears for the remaining groups. Existing same-issuer overrides remain unchanged.

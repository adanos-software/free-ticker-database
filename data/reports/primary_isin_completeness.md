# Primary ISIN Completeness

Generated at: `2026-07-08T04:36:03Z`

This report scopes D1 primary-listing ISIN work. It does not fill values; it assigns allowed source paths and gates to every missing primary ISIN row.

## Summary

| Metric | Value |
|---|---:|
| missing_primary_isin_rows | 621 |
| priority_exchange_rows | 519 |
| non_priority_exchange_rows | 102 |
| blocked_rows | 399 |
| eligible_after_allowed_source_gates | 222 |

## Priority Exchanges

| Exchange | Rows |
|---|---:|
| NASDAQ | 80 |
| ASX | 99 |
| TSX | 90 |
| NYSE | 48 |
| TSXV | 77 |
| NYSE ARCA | 47 |
| NEO | 43 |
| SSE | 35 |

## Gap Classes

| Gap Class | Rows |
|---|---:|
| adr_cdr_or_depositary_identifier_gap | 43 |
| capital_pool_or_halted_identifier_gap | 33 |
| debt_or_securitized_identifier_gap | 76 |
| fund_or_trust_identifier_gap | 231 |
| inactive_or_legacy_identifier_gap | 16 |
| official_current_directory_absent_identifier_gap | 9 |
| official_identifier_not_exposed_source_gap | 213 |

## Apply Eligibility

| Eligibility | Rows |
|---|---:|
| blocked_until_core_or_extended_scope_decision | 399 |
| eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates | 222 |

## Top Rows

| Exchange | Ticker | Asset Type | Gap Class | Outcome | Allowed Source Path | Apply Eligibility |
|---|---|---|---|---|---|---|
| NASDAQ | ACAA | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | ACCL | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | ACGC | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | ADAMM | Stock | debt_or_securitized_identifier_gap | core_exclusion_candidate | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | blocked_until_core_or_extended_scope_decision |
| NASDAQ | AESP | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | AIIR | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | AIXI | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | ALP | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | ALPS | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | AMAN | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | APMC | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | APUR | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | ARCI | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | ARCL | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | AUC | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | AYA | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | BMM | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | BREZ | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | BRHY | ETF | fund_or_trust_identifier_gap | core_exclusion_candidate | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | blocked_until_core_or_extended_scope_decision |
| NASDAQ | BRKD | ETF | fund_or_trust_identifier_gap | core_exclusion_candidate | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | blocked_until_core_or_extended_scope_decision |
| NASDAQ | CAII | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | CAQ | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | CMII | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | CTW | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | EU | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | FLD | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | FTHA | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | GAVA | ETF | fund_or_trust_identifier_gap | core_exclusion_candidate | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | blocked_until_core_or_extended_scope_decision |
| NASDAQ | GCGR | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | GDFN | ETF | fund_or_trust_identifier_gap | core_exclusion_candidate | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | blocked_until_core_or_extended_scope_decision |
| NASDAQ | GPAC | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | HELP | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | HERZ | ETF | fund_or_trust_identifier_gap | core_exclusion_candidate | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | blocked_until_core_or_extended_scope_decision |
| NASDAQ | HQ | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | INRO | ETF | fund_or_trust_identifier_gap | core_exclusion_candidate | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | blocked_until_core_or_extended_scope_decision |
| NASDAQ | ITG | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | KEEL | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | KEYY | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | KTWO | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | LAYS | ETF | fund_or_trust_identifier_gap | core_exclusion_candidate | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | blocked_until_core_or_extended_scope_decision |
| NASDAQ | LFS | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | MCAH | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | MLAC | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | MOB | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | MYCO | ETF | debt_or_securitized_identifier_gap | core_exclusion_candidate | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | blocked_until_core_or_extended_scope_decision |
| NASDAQ | MYMK | ETF | debt_or_securitized_identifier_gap | core_exclusion_candidate | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | blocked_until_core_or_extended_scope_decision |
| NASDAQ | MYX | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | NCEL | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | NGEN | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | NHIV | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | NIPG | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | NMAD | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | NWGL | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | OFAL | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | OIO | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | OIOWW | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | PBK | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | PDC | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | RACC | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | RARE | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | REMG | ETF | fund_or_trust_identifier_gap | core_exclusion_candidate | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | blocked_until_core_or_extended_scope_decision |
| NASDAQ | RGLO | ETF | fund_or_trust_identifier_gap | core_exclusion_candidate | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | blocked_until_core_or_extended_scope_decision |
| NASDAQ | RIFR | ETF | fund_or_trust_identifier_gap | core_exclusion_candidate | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | blocked_until_core_or_extended_scope_decision |
| NASDAQ | SKK | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | SLBT | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | SLM | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | SSM | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | TDOG | ETF | fund_or_trust_identifier_gap | core_exclusion_candidate | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | blocked_until_core_or_extended_scope_decision |
| NASDAQ | TJGC | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | TMCR | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | TSUI | ETF | fund_or_trust_identifier_gap | core_exclusion_candidate | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | blocked_until_core_or_extended_scope_decision |
| NASDAQ | VACH | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | VAVX | ETF | fund_or_trust_identifier_gap | core_exclusion_candidate | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | blocked_until_core_or_extended_scope_decision |
| NASDAQ | VIVO | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | VIXI | ETF | fund_or_trust_identifier_gap | core_exclusion_candidate | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | blocked_until_core_or_extended_scope_decision |
| NASDAQ | WFF | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | WLDSW | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | WSE | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | XPM | ETF | fund_or_trust_identifier_gap | core_exclusion_candidate | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | blocked_until_core_or_extended_scope_decision |
| NASDAQ | XRPI | ETF | fund_or_trust_identifier_gap | core_exclusion_candidate | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | blocked_until_core_or_extended_scope_decision |
| ASX | AC2 | ETF | debt_or_securitized_identifier_gap | core_exclusion_candidate | ASX ISIN workbook, official ASX/security registry, or issuer/trustee evidence | blocked_until_core_or_extended_scope_decision |
| ASX | AC3 | ETF | debt_or_securitized_identifier_gap | core_exclusion_candidate | ASX ISIN workbook, official ASX/security registry, or issuer/trustee evidence | blocked_until_core_or_extended_scope_decision |
| ASX | AF2 | ETF | debt_or_securitized_identifier_gap | core_exclusion_candidate | ASX ISIN workbook, official ASX/security registry, or issuer/trustee evidence | blocked_until_core_or_extended_scope_decision |
| ASX | AF4 | ETF | debt_or_securitized_identifier_gap | core_exclusion_candidate | ASX ISIN workbook, official ASX/security registry, or issuer/trustee evidence | blocked_until_core_or_extended_scope_decision |
| ASX | AF5 | ETF | debt_or_securitized_identifier_gap | core_exclusion_candidate | ASX ISIN workbook, official ASX/security registry, or issuer/trustee evidence | blocked_until_core_or_extended_scope_decision |
| ASX | AF6 | ETF | debt_or_securitized_identifier_gap | core_exclusion_candidate | ASX ISIN workbook, official ASX/security registry, or issuer/trustee evidence | blocked_until_core_or_extended_scope_decision |
| ASX | AF7 | ETF | debt_or_securitized_identifier_gap | core_exclusion_candidate | ASX ISIN workbook, official ASX/security registry, or issuer/trustee evidence | blocked_until_core_or_extended_scope_decision |
| ASX | AN3 | Stock | inactive_or_legacy_identifier_gap | core_exclusion_candidate | ASX ISIN workbook, official ASX/security registry, or issuer/trustee evidence | blocked_until_core_or_extended_scope_decision |
| ASX | AO2 | ETF | debt_or_securitized_identifier_gap | core_exclusion_candidate | ASX ISIN workbook, official ASX/security registry, or issuer/trustee evidence | blocked_until_core_or_extended_scope_decision |
| ASX | AO3 | ETF | debt_or_securitized_identifier_gap | core_exclusion_candidate | ASX ISIN workbook, official ASX/security registry, or issuer/trustee evidence | blocked_until_core_or_extended_scope_decision |
| ASX | BA2 | ETF | debt_or_securitized_identifier_gap | core_exclusion_candidate | ASX ISIN workbook, official ASX/security registry, or issuer/trustee evidence | blocked_until_core_or_extended_scope_decision |
| ASX | BP1 | Stock | official_current_directory_absent_identifier_gap | accepted_source_gap | ASX ISIN workbook, official ASX/security registry, or issuer/trustee evidence | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| ASX | BS1 | ETF | fund_or_trust_identifier_gap | core_exclusion_candidate | ASX ISIN workbook, official ASX/security registry, or issuer/trustee evidence | blocked_until_core_or_extended_scope_decision |
| ASX | BW6 | ETF | debt_or_securitized_identifier_gap | core_exclusion_candidate | ASX ISIN workbook, official ASX/security registry, or issuer/trustee evidence | blocked_until_core_or_extended_scope_decision |
| ASX | D10 | ETF | fund_or_trust_identifier_gap | core_exclusion_candidate | ASX ISIN workbook, official ASX/security registry, or issuer/trustee evidence | blocked_until_core_or_extended_scope_decision |
| ASX | DA8 | ETF | fund_or_trust_identifier_gap | core_exclusion_candidate | ASX ISIN workbook, official ASX/security registry, or issuer/trustee evidence | blocked_until_core_or_extended_scope_decision |
| ASX | DA9 | ETF | fund_or_trust_identifier_gap | core_exclusion_candidate | ASX ISIN workbook, official ASX/security registry, or issuer/trustee evidence | blocked_until_core_or_extended_scope_decision |
| ASX | DMN | Stock | inactive_or_legacy_identifier_gap | core_exclusion_candidate | ASX ISIN workbook, official ASX/security registry, or issuer/trustee evidence | blocked_until_core_or_extended_scope_decision |
| ASX | DXA | Stock | official_current_directory_absent_identifier_gap | accepted_source_gap | ASX ISIN workbook, official ASX/security registry, or issuer/trustee evidence | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| ASX | EBTC | ETF | fund_or_trust_identifier_gap | core_exclusion_candidate | ASX ISIN workbook, official ASX/security registry, or issuer/trustee evidence | blocked_until_core_or_extended_scope_decision |
| ASX | FM2 | ETF | debt_or_securitized_identifier_gap | core_exclusion_candidate | ASX ISIN workbook, official ASX/security registry, or issuer/trustee evidence | blocked_until_core_or_extended_scope_decision |
| ASX | FM3 | ETF | debt_or_securitized_identifier_gap | core_exclusion_candidate | ASX ISIN workbook, official ASX/security registry, or issuer/trustee evidence | blocked_until_core_or_extended_scope_decision |
| ASX | FM5 | ETF | debt_or_securitized_identifier_gap | core_exclusion_candidate | ASX ISIN workbook, official ASX/security registry, or issuer/trustee evidence | blocked_until_core_or_extended_scope_decision |
| ASX | HC1 | ETF | fund_or_trust_identifier_gap | core_exclusion_candidate | ASX ISIN workbook, official ASX/security registry, or issuer/trustee evidence | blocked_until_core_or_extended_scope_decision |
| ASX | IF1 | Stock | inactive_or_legacy_identifier_gap | core_exclusion_candidate | ASX ISIN workbook, official ASX/security registry, or issuer/trustee evidence | blocked_until_core_or_extended_scope_decision |
| ASX | KI1 | ETF | debt_or_securitized_identifier_gap | core_exclusion_candidate | ASX ISIN workbook, official ASX/security registry, or issuer/trustee evidence | blocked_until_core_or_extended_scope_decision |
| ASX | KIG | ETF | debt_or_securitized_identifier_gap | core_exclusion_candidate | ASX ISIN workbook, official ASX/security registry, or issuer/trustee evidence | blocked_until_core_or_extended_scope_decision |
| ASX | LI8 | ETF | debt_or_securitized_identifier_gap | core_exclusion_candidate | ASX ISIN workbook, official ASX/security registry, or issuer/trustee evidence | blocked_until_core_or_extended_scope_decision |
| ASX | LN1 | ETF | debt_or_securitized_identifier_gap | core_exclusion_candidate | ASX ISIN workbook, official ASX/security registry, or issuer/trustee evidence | blocked_until_core_or_extended_scope_decision |
| ASX | LN2 | ETF | debt_or_securitized_identifier_gap | core_exclusion_candidate | ASX ISIN workbook, official ASX/security registry, or issuer/trustee evidence | blocked_until_core_or_extended_scope_decision |
| ASX | LN3 | ETF | debt_or_securitized_identifier_gap | core_exclusion_candidate | ASX ISIN workbook, official ASX/security registry, or issuer/trustee evidence | blocked_until_core_or_extended_scope_decision |
| ASX | LO1 | Stock | official_current_directory_absent_identifier_gap | accepted_source_gap | ASX ISIN workbook, official ASX/security registry, or issuer/trustee evidence | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| ASX | LP1 | Stock | debt_or_securitized_identifier_gap | core_exclusion_candidate | ASX ISIN workbook, official ASX/security registry, or issuer/trustee evidence | blocked_until_core_or_extended_scope_decision |
| ASX | LR1 | ETF | debt_or_securitized_identifier_gap | core_exclusion_candidate | ASX ISIN workbook, official ASX/security registry, or issuer/trustee evidence | blocked_until_core_or_extended_scope_decision |
| ASX | LR3 | ETF | debt_or_securitized_identifier_gap | core_exclusion_candidate | ASX ISIN workbook, official ASX/security registry, or issuer/trustee evidence | blocked_until_core_or_extended_scope_decision |
| ASX | LR4 | ETF | debt_or_securitized_identifier_gap | core_exclusion_candidate | ASX ISIN workbook, official ASX/security registry, or issuer/trustee evidence | blocked_until_core_or_extended_scope_decision |
| ASX | LR5 | ETF | debt_or_securitized_identifier_gap | core_exclusion_candidate | ASX ISIN workbook, official ASX/security registry, or issuer/trustee evidence | blocked_until_core_or_extended_scope_decision |
| ASX | LR6 | ETF | debt_or_securitized_identifier_gap | core_exclusion_candidate | ASX ISIN workbook, official ASX/security registry, or issuer/trustee evidence | blocked_until_core_or_extended_scope_decision |
| ASX | LT9 | ETF | debt_or_securitized_identifier_gap | core_exclusion_candidate | ASX ISIN workbook, official ASX/security registry, or issuer/trustee evidence | blocked_until_core_or_extended_scope_decision |
| ASX | MA2 | Stock | inactive_or_legacy_identifier_gap | core_exclusion_candidate | ASX ISIN workbook, official ASX/security registry, or issuer/trustee evidence | blocked_until_core_or_extended_scope_decision |

## Policy

- No ISIN is inferred from ticker, name, issuer family, sector, or peer rows.
- Allowed D1 sources are GLEIF ISIN↔LEI mapping, ESMA/FCA FIRDS, OpenFIGI ticker→FIGI→ISIN, ASX ISIN workbook, TMX/CDS lists, or exact official/review-gated issuer/security-registry evidence.
- Every apply still requires a valid ISIN checksum, exact listing identity match, and no-collision validation.

# Primary ISIN Completeness

Generated at: `2026-07-07T10:09:53Z`

This report scopes D1 primary-listing ISIN work. It does not fill values; it assigns allowed source paths and gates to every missing primary ISIN row.

## Summary

| Metric | Value |
|---|---:|
| missing_primary_isin_rows | 947 |
| priority_exchange_rows | 727 |
| non_priority_exchange_rows | 220 |
| blocked_rows | 478 |
| eligible_after_allowed_source_gates | 469 |

## Priority Exchanges

| Exchange | Rows |
|---|---:|
| NASDAQ | 195 |
| ASX | 106 |
| TSX | 100 |
| NYSE | 95 |
| TSXV | 82 |
| NYSE ARCA | 69 |
| NEO | 43 |
| SSE | 37 |

## Gap Classes

| Gap Class | Rows |
|---|---:|
| adr_cdr_or_depositary_identifier_gap | 43 |
| capital_pool_or_halted_identifier_gap | 35 |
| debt_or_securitized_identifier_gap | 81 |
| fund_or_trust_identifier_gap | 300 |
| inactive_or_legacy_identifier_gap | 19 |
| official_current_directory_absent_identifier_gap | 12 |
| official_identifier_not_exposed_source_gap | 408 |
| official_identifier_reference_unmatched_gap | 49 |

## Apply Eligibility

| Eligibility | Rows |
|---|---:|
| blocked_until_core_or_extended_scope_decision | 478 |
| eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates | 469 |

## Top Rows

| Exchange | Ticker | Asset Type | Gap Class | Outcome | Allowed Source Path | Apply Eligibility |
|---|---|---|---|---|---|---|
| NASDAQ | AACI | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | AACO | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | AACP | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | ABIG | ETF | fund_or_trust_identifier_gap | core_exclusion_candidate | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | blocked_until_core_or_extended_scope_decision |
| NASDAQ | ACAA | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | ACCL | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | ACGC | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | ADAC | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | ADAMM | Stock | debt_or_securitized_identifier_gap | core_exclusion_candidate | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | blocked_until_core_or_extended_scope_decision |
| NASDAQ | ADUR | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | AESP | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | AIBZ | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | AIFA | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | AIIR | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | AIXI | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | ALF | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | ALP | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | ALPS | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | AMAN | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | APAC | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | APAD | Stock | official_identifier_reference_unmatched_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | APMC | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | APUR | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | APXT | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | ARCI | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | ARCL | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | ARTC | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | ATCX | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | AUC | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | AUR | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | AYA | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | BBB | ETF | fund_or_trust_identifier_gap | core_exclusion_candidate | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | blocked_until_core_or_extended_scope_decision |
| NASDAQ | BDCI | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | BHST | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | BMM | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | BOOM | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | BPAC | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | BREZ | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | BRHY | ETF | fund_or_trust_identifier_gap | core_exclusion_candidate | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | blocked_until_core_or_extended_scope_decision |
| NASDAQ | BRKD | ETF | fund_or_trust_identifier_gap | core_exclusion_candidate | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | blocked_until_core_or_extended_scope_decision |
| NASDAQ | BRUN | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | CAES | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | CAII | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | CALI | ETF | fund_or_trust_identifier_gap | core_exclusion_candidate | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | blocked_until_core_or_extended_scope_decision |
| NASDAQ | CAQ | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | CCXI | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | CGTL | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | CMII | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | CMTV | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | CRAN | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | CRE | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | CSHR | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | CTAA | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | CTW | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | CXII | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | DETX | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | DSAC | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | EDBLW | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | EMIS | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | EU | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | EXUS | ETF | fund_or_trust_identifier_gap | core_exclusion_candidate | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | blocked_until_core_or_extended_scope_decision |
| NASDAQ | FISN | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | FLD | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | FMAC | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | FRSX | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | FRTT | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | FTHA | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | FXHO | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | GAVA | ETF | fund_or_trust_identifier_gap | core_exclusion_candidate | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | blocked_until_core_or_extended_scope_decision |
| NASDAQ | GCGR | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | GCT | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | GDFN | ETF | fund_or_trust_identifier_gap | core_exclusion_candidate | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | blocked_until_core_or_extended_scope_decision |
| NASDAQ | GIW | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | GIX | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | GPAC | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | GPT | ETF | fund_or_trust_identifier_gap | core_exclusion_candidate | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | blocked_until_core_or_extended_scope_decision |
| NASDAQ | GSRV | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | HACQ | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | HAVA | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | HCAC | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | HCIC | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | HCMA | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | HELP | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | HERZ | ETF | fund_or_trust_identifier_gap | core_exclusion_candidate | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | blocked_until_core_or_extended_scope_decision |
| NASDAQ | HODO | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | HONA | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | HONAV | Stock | official_identifier_reference_unmatched_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | HQ | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | IACO | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | IACQ | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | IBGM | ETF | fund_or_trust_identifier_gap | core_exclusion_candidate | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | blocked_until_core_or_extended_scope_decision |
| NASDAQ | IGAC | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | ILLU | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | INIO | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | INRO | ETF | fund_or_trust_identifier_gap | core_exclusion_candidate | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | blocked_until_core_or_extended_scope_decision |
| NASDAQ | INSG | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | IPFX | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | ITG | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | JATT | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | KARD | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | KEEL | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | KEYY | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | KTWO | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | LAYS | ETF | fund_or_trust_identifier_gap | core_exclusion_candidate | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | blocked_until_core_or_extended_scope_decision |
| NASDAQ | LFAC | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | LFS | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | LRE | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | LWAC | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | MAKO | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | MCAH | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | MDCX | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | MDXH | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | MLAC | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | MOB | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | MUD | ETF | fund_or_trust_identifier_gap | core_exclusion_candidate | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | blocked_until_core_or_extended_scope_decision |
| NASDAQ | MUZE | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | MYCO | ETF | debt_or_securitized_identifier_gap | core_exclusion_candidate | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | blocked_until_core_or_extended_scope_decision |
| NASDAQ | MYMK | ETF | debt_or_securitized_identifier_gap | core_exclusion_candidate | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | blocked_until_core_or_extended_scope_decision |
| NASDAQ | MYX | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |
| NASDAQ | NAAS | Stock | official_identifier_not_exposed_source_gap | accepted_source_gap | OpenFIGI ticker→FIGI→ISIN, GLEIF ISIN↔LEI reverse match, or ESMA/FCA FIRDS when venue evidence matches | eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates |

## Policy

- No ISIN is inferred from ticker, name, issuer family, sector, or peer rows.
- Allowed D1 sources are GLEIF ISIN↔LEI mapping, ESMA/FCA FIRDS, OpenFIGI ticker→FIGI→ISIN, ASX ISIN workbook, TMX/CDS lists, or exact official/review-gated issuer/security-registry evidence.
- Every apply still requires a valid ISIN checksum, exact listing identity match, and no-collision validation.

# ASX Residual Review

Generated at: `2026-05-25T12:54:48Z`

This report tracks ASX residual ISIN and ETF-category gaps after current official ASX masterfile and ASX ISIN workbook checks. It does not fill values.

## Summary

- Review rows: `114`
- Core-exclusion candidate rows requiring scope review: `94`
- ASX residual backlog rows: `114`
- Direct data apply allowed rows: `0`

## ASX Residual Backlog

- Status: `review_only_scope_identifier_or_product_taxonomy_source_gaps`
- Scope decision required rows: `94`
- Identity review required rows: `1`
- Official identifier source required rows: `10`
- Official product taxonomy required rows: `9`
- Official ISIN apply candidate rows: `0`
- Metadata enrichment authorized: `false`

| Queue | Rows |
|---|---:|
| asx_isin_workbook_name_mismatch_manual_review | 1 |
| core_exclusion_candidate_identifier_scope_review | 94 |
| missing_etf_category_requires_official_product_taxonomy | 9 |
| missing_isin_not_in_current_asx_isin_workbook | 10 |

| Evidence required | Rows |
|---|---:|
| direct_registry_issuer_or_official_asx_identifier_source_with_exact_listing_match | 10 |
| manual_exact_name_or_alias_resolution_before_isin_apply | 1 |
| official_or_reviewed_product_taxonomy_with_exact_listing_match | 9 |
| scope_decision_for_core_extended_or_exclude_before_identifier_or_category_fill | 94 |

## Fields

| Field | Rows |
|---|---:|
| missing_etf_category | 9 |
| missing_isin_primary | 105 |

## Residual Decisions

| Decision | Rows |
|---|---:|
| accepted_source_gap_not_in_current_asx_isin_workbook | 10 |
| accepted_source_gap_requires_exact_name_review | 1 |
| accepted_source_gap_requires_official_product_taxonomy | 9 |
| core_exclusion_candidate_requires_scope_review | 94 |

## ASX Resolution Queues

| Queue | Rows |
|---|---:|
| asx_isin_workbook_name_mismatch_manual_review | 1 |
| core_exclusion_candidate_identifier_scope_review | 94 |
| missing_etf_category_requires_official_product_taxonomy | 9 |
| missing_isin_not_in_current_asx_isin_workbook | 10 |

## ASX Resolution Queue By Gap Class

| Queue | Gap class | Rows |
|---|---|---:|
| asx_isin_workbook_name_mismatch_manual_review | official_identifier_reference_unmatched_gap | 1 |
| core_exclusion_candidate_identifier_scope_review | debt_or_securitized_identifier_gap | 59 |
| core_exclusion_candidate_identifier_scope_review | fund_or_trust_identifier_gap | 26 |
| core_exclusion_candidate_identifier_scope_review | inactive_or_legacy_identifier_gap | 9 |
| missing_etf_category_requires_official_product_taxonomy | official_product_taxonomy_unavailable_gap | 9 |
| missing_isin_not_in_current_asx_isin_workbook | official_current_directory_absent_identifier_gap | 10 |

## ASX Resolution Queue By Official Source

| Queue | Official source | Rows |
|---|---|---:|
| asx_isin_workbook_name_mismatch_manual_review | none | 1 |
| core_exclusion_candidate_identifier_scope_review | asx_listed_companies | 92 |
| core_exclusion_candidate_identifier_scope_review | none | 2 |
| missing_etf_category_requires_official_product_taxonomy | asx_listed_companies | 9 |
| missing_isin_not_in_current_asx_isin_workbook | asx_listed_companies | 10 |

## ASX Official Source Capability

| Queue | Capability | Rows |
|---|---|---:|
| asx_isin_workbook_name_mismatch_manual_review | masterfile_exposes_isin=false | 1 |
| asx_isin_workbook_name_mismatch_manual_review | masterfile_exposes_sector=false | 1 |
| asx_isin_workbook_name_mismatch_manual_review | masterfile_match=false | 1 |
| core_exclusion_candidate_identifier_scope_review | masterfile_exposes_isin=false | 94 |
| core_exclusion_candidate_identifier_scope_review | masterfile_exposes_sector=false | 94 |
| core_exclusion_candidate_identifier_scope_review | masterfile_match=false | 2 |
| core_exclusion_candidate_identifier_scope_review | masterfile_match=true | 92 |
| missing_etf_category_requires_official_product_taxonomy | masterfile_exposes_isin=false | 9 |
| missing_etf_category_requires_official_product_taxonomy | masterfile_exposes_sector=false | 9 |
| missing_etf_category_requires_official_product_taxonomy | masterfile_match=true | 9 |
| missing_isin_not_in_current_asx_isin_workbook | masterfile_exposes_isin=false | 10 |
| missing_isin_not_in_current_asx_isin_workbook | masterfile_exposes_sector=false | 10 |
| missing_isin_not_in_current_asx_isin_workbook | masterfile_match=true | 10 |

## ASX Review Strategies

| Queue | Strategy | Rows |
|---|---|---:|
| asx_isin_workbook_name_mismatch_manual_review | manual_identity_review_before_asx_isin_apply | 1 |
| core_exclusion_candidate_identifier_scope_review | scope_review_before_asx_identifier_enrichment | 94 |
| missing_etf_category_requires_official_product_taxonomy | seek_official_or_reviewed_asx_product_taxonomy | 9 |
| missing_isin_not_in_current_asx_isin_workbook | keep_isin_blank_until_current_asx_or_registry_source | 10 |

## ASX Review Evidence

| Queue | Evidence required | Rows |
|---|---|---:|
| asx_isin_workbook_name_mismatch_manual_review | manual_exact_name_or_alias_resolution_before_isin_apply | 1 |
| core_exclusion_candidate_identifier_scope_review | scope_decision_for_core_extended_or_exclude_before_identifier_or_category_fill | 94 |
| missing_etf_category_requires_official_product_taxonomy | official_or_reviewed_product_taxonomy_with_exact_listing_match | 9 |
| missing_isin_not_in_current_asx_isin_workbook | direct_registry_issuer_or_official_asx_identifier_source_with_exact_listing_match | 10 |

## Top ASX Review Batches

| Queue | Field | Official source | Strategy | Evidence required | Recommended next source | Source gate | Rows |
|---|---|---|---|---|---|---|---:|
| core_exclusion_candidate_identifier_scope_review | missing_isin_primary | asx_listed_companies | scope_review_before_asx_identifier_enrichment | scope_decision_for_core_extended_or_exclude_before_identifier_or_category_fill | asx_listed_companies plus reviewed scope decision for core, extended, or exclude before identifier work. | No ISIN or category enrichment until the listing is reviewed as core, extended, or excluded. | 92 |
| missing_isin_not_in_current_asx_isin_workbook | missing_isin_primary | asx_listed_companies | keep_isin_blank_until_current_asx_or_registry_source | direct_registry_issuer_or_official_asx_identifier_source_with_exact_listing_match | Current ASX ISIN workbook, registry, issuer, trustee, or prospectus source with exact listing match. | Keep ISIN blank until current ASX, registry, issuer, or trustee evidence proves the identifier. | 10 |
| missing_etf_category_requires_official_product_taxonomy | missing_etf_category | asx_listed_companies | seek_official_or_reviewed_asx_product_taxonomy | official_or_reviewed_product_taxonomy_with_exact_listing_match | Official ASX product taxonomy, issuer/sponsor product page, PDS, or reviewed product taxonomy source. | Keep ETF category blank until exact product taxonomy evidence exists. | 9 |
| core_exclusion_candidate_identifier_scope_review | missing_isin_primary | none | scope_review_before_asx_identifier_enrichment | scope_decision_for_core_extended_or_exclude_before_identifier_or_category_fill | current official ASX, registry, or issuer source plus reviewed scope decision for core, extended, or exclude before identifier work. | No ISIN or category enrichment until the listing is reviewed as core, extended, or excluded. | 2 |
| asx_isin_workbook_name_mismatch_manual_review | missing_isin_primary | none | manual_identity_review_before_asx_isin_apply | manual_exact_name_or_alias_resolution_before_isin_apply | Manual exact-name or reviewed-alias resolution against ASX workbook, issuer, or registry evidence. | Do not apply workbook ISIN until the name mismatch is manually resolved with exact listing evidence. | 1 |

## Review Priorities

| Priority | Rows |
|---|---:|
| P1 | 95 |
| P2 | 9 |
| P3 | 10 |

## Review Buckets

| Bucket | Rows |
|---|---:|
| identifier_source_gap | 10 |
| identity_mismatch_requires_manual_review | 1 |
| product_taxonomy_source_gap | 9 |
| scope_review_before_any_data_fill | 94 |

## Apply Eligibility

| Eligibility | Rows |
|---|---:|
| blocked_until_core_or_extended_scope_decision | 94 |
| blocked_until_identity_mismatch_resolved | 1 |
| keep_category_blank_until_official_product_taxonomy_source | 9 |
| keep_identifier_blank_until_direct_registry_issuer_or_official_asx_evidence | 10 |

## Scope Blockers

| Field | Rows |
|---|---:|
| missing_isin_primary | 94 |

| Asset type | Rows |
|---|---:|
| ETF | 82 |
| Stock | 12 |

| Gap class | Rows |
|---|---:|
| debt_or_securitized_identifier_gap | 59 |
| fund_or_trust_identifier_gap | 26 |
| inactive_or_legacy_identifier_gap | 9 |

| Resolution queue | Rows |
|---|---:|
| core_exclusion_candidate_identifier_scope_review | 94 |

| Official source | Rows |
|---|---:|
| asx_listed_companies | 92 |
| none | 2 |

| Official capability | Rows |
|---|---:|
| masterfile_exposes_isin=false | 94 |
| masterfile_exposes_sector=false | 94 |
| masterfile_match=false | 2 |
| masterfile_match=true | 92 |

## Verification Evidence

| Evidence Gate | Rows |
|---|---:|
| direct_registry_issuer_or_official_asx_identifier_source_with_exact_listing_match | 10 |
| manual_exact_name_or_alias_resolution_before_isin_apply | 1 |
| official_or_reviewed_product_taxonomy_with_exact_listing_match | 9 |
| scope_decision_for_core_extended_or_exclude_before_identifier_or_category_fill | 94 |

## Rows

| Listing key | Priority | Bucket | Field | Class | Outcome | Decision |
|---|---|---|---|---|---|---|
| ASX::RAMHA | P1 | identity_mismatch_requires_manual_review | missing_isin_primary | official_identifier_reference_unmatched_gap | accepted_source_gap | accepted_source_gap_requires_exact_name_review |
| ASX::AC2 | P1 | scope_review_before_any_data_fill | missing_isin_primary | debt_or_securitized_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::AC3 | P1 | scope_review_before_any_data_fill | missing_isin_primary | debt_or_securitized_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::AF2 | P1 | scope_review_before_any_data_fill | missing_isin_primary | debt_or_securitized_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::AF4 | P1 | scope_review_before_any_data_fill | missing_isin_primary | debt_or_securitized_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::AF5 | P1 | scope_review_before_any_data_fill | missing_isin_primary | debt_or_securitized_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::AF6 | P1 | scope_review_before_any_data_fill | missing_isin_primary | debt_or_securitized_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::AF7 | P1 | scope_review_before_any_data_fill | missing_isin_primary | debt_or_securitized_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::AN3 | P1 | scope_review_before_any_data_fill | missing_isin_primary | inactive_or_legacy_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::AO2 | P1 | scope_review_before_any_data_fill | missing_isin_primary | debt_or_securitized_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::AO3 | P1 | scope_review_before_any_data_fill | missing_isin_primary | debt_or_securitized_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::BA2 | P1 | scope_review_before_any_data_fill | missing_isin_primary | debt_or_securitized_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::BS1 | P1 | scope_review_before_any_data_fill | missing_isin_primary | fund_or_trust_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::BW6 | P1 | scope_review_before_any_data_fill | missing_isin_primary | debt_or_securitized_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::D10 | P1 | scope_review_before_any_data_fill | missing_isin_primary | fund_or_trust_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::DA8 | P1 | scope_review_before_any_data_fill | missing_isin_primary | fund_or_trust_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::DA9 | P1 | scope_review_before_any_data_fill | missing_isin_primary | fund_or_trust_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::DMN | P1 | scope_review_before_any_data_fill | missing_isin_primary | inactive_or_legacy_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::EBTC | P1 | scope_review_before_any_data_fill | missing_isin_primary | fund_or_trust_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::FM1 | P1 | scope_review_before_any_data_fill | missing_isin_primary | debt_or_securitized_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::FM2 | P1 | scope_review_before_any_data_fill | missing_isin_primary | debt_or_securitized_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::FM3 | P1 | scope_review_before_any_data_fill | missing_isin_primary | debt_or_securitized_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::FM5 | P1 | scope_review_before_any_data_fill | missing_isin_primary | debt_or_securitized_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::HC1 | P1 | scope_review_before_any_data_fill | missing_isin_primary | fund_or_trust_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::IF1 | P1 | scope_review_before_any_data_fill | missing_isin_primary | inactive_or_legacy_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::KI1 | P1 | scope_review_before_any_data_fill | missing_isin_primary | debt_or_securitized_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::KIG | P1 | scope_review_before_any_data_fill | missing_isin_primary | debt_or_securitized_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::LI8 | P1 | scope_review_before_any_data_fill | missing_isin_primary | debt_or_securitized_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::LN1 | P1 | scope_review_before_any_data_fill | missing_isin_primary | debt_or_securitized_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::LN2 | P1 | scope_review_before_any_data_fill | missing_isin_primary | debt_or_securitized_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::LN3 | P1 | scope_review_before_any_data_fill | missing_isin_primary | debt_or_securitized_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::LP1 | P1 | scope_review_before_any_data_fill | missing_isin_primary | debt_or_securitized_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::LR1 | P1 | scope_review_before_any_data_fill | missing_isin_primary | debt_or_securitized_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::LR3 | P1 | scope_review_before_any_data_fill | missing_isin_primary | debt_or_securitized_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::LR4 | P1 | scope_review_before_any_data_fill | missing_isin_primary | debt_or_securitized_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::LR5 | P1 | scope_review_before_any_data_fill | missing_isin_primary | debt_or_securitized_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::LR6 | P1 | scope_review_before_any_data_fill | missing_isin_primary | debt_or_securitized_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::LT9 | P1 | scope_review_before_any_data_fill | missing_isin_primary | debt_or_securitized_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::MA2 | P1 | scope_review_before_any_data_fill | missing_isin_primary | inactive_or_legacy_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::MBL | P1 | scope_review_before_any_data_fill | missing_isin_primary | inactive_or_legacy_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::MF3 | P1 | scope_review_before_any_data_fill | missing_isin_primary | fund_or_trust_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::MF4 | P1 | scope_review_before_any_data_fill | missing_isin_primary | fund_or_trust_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::MF5 | P1 | scope_review_before_any_data_fill | missing_isin_primary | fund_or_trust_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::MF6 | P1 | scope_review_before_any_data_fill | missing_isin_primary | fund_or_trust_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::ML2 | P1 | scope_review_before_any_data_fill | missing_isin_primary | debt_or_securitized_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::MM2 | P1 | scope_review_before_any_data_fill | missing_isin_primary | debt_or_securitized_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::MM4 | P1 | scope_review_before_any_data_fill | missing_isin_primary | fund_or_trust_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::MZ1 | P1 | scope_review_before_any_data_fill | missing_isin_primary | debt_or_securitized_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::MZ2 | P1 | scope_review_before_any_data_fill | missing_isin_primary | debt_or_securitized_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::MZF | P1 | scope_review_before_any_data_fill | missing_isin_primary | debt_or_securitized_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::MZT | P1 | scope_review_before_any_data_fill | missing_isin_primary | debt_or_securitized_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::NW1 | P1 | scope_review_before_any_data_fill | missing_isin_primary | debt_or_securitized_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::OR2 | P1 | scope_review_before_any_data_fill | missing_isin_primary | debt_or_securitized_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::OY1 | P1 | scope_review_before_any_data_fill | missing_isin_primary | fund_or_trust_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::OYS | P1 | scope_review_before_any_data_fill | missing_isin_primary | fund_or_trust_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::PA1 | P1 | scope_review_before_any_data_fill | missing_isin_primary | debt_or_securitized_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::PA2 | P1 | scope_review_before_any_data_fill | missing_isin_primary | debt_or_securitized_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::PA3 | P1 | scope_review_before_any_data_fill | missing_isin_primary | debt_or_securitized_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::PA4 | P1 | scope_review_before_any_data_fill | missing_isin_primary | debt_or_securitized_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::PA5 | P1 | scope_review_before_any_data_fill | missing_isin_primary | debt_or_securitized_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::PA6 | P1 | scope_review_before_any_data_fill | missing_isin_primary | debt_or_securitized_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::POB | P1 | scope_review_before_any_data_fill | missing_isin_primary | fund_or_trust_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::POC | P1 | scope_review_before_any_data_fill | missing_isin_primary | fund_or_trust_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::POE | P1 | scope_review_before_any_data_fill | missing_isin_primary | fund_or_trust_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::POF | P1 | scope_review_before_any_data_fill | missing_isin_primary | fund_or_trust_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::POG | P1 | scope_review_before_any_data_fill | missing_isin_primary | fund_or_trust_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::POH | P1 | scope_review_before_any_data_fill | missing_isin_primary | fund_or_trust_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::POI | P1 | scope_review_before_any_data_fill | missing_isin_primary | fund_or_trust_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::PR4 | P1 | scope_review_before_any_data_fill | missing_isin_primary | fund_or_trust_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::PS2 | P1 | scope_review_before_any_data_fill | missing_isin_primary | debt_or_securitized_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::PU6 | P1 | scope_review_before_any_data_fill | missing_isin_primary | fund_or_trust_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::PU8 | P1 | scope_review_before_any_data_fill | missing_isin_primary | fund_or_trust_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::PU9 | P1 | scope_review_before_any_data_fill | missing_isin_primary | fund_or_trust_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::PUT | P1 | scope_review_before_any_data_fill | missing_isin_primary | debt_or_securitized_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::PUU | P1 | scope_review_before_any_data_fill | missing_isin_primary | debt_or_securitized_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::PUV | P1 | scope_review_before_any_data_fill | missing_isin_primary | debt_or_securitized_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::QNB | P1 | scope_review_before_any_data_fill | missing_isin_primary | inactive_or_legacy_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::RA2 | P1 | scope_review_before_any_data_fill | missing_isin_primary | debt_or_securitized_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::RA3 | P1 | scope_review_before_any_data_fill | missing_isin_primary | debt_or_securitized_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::RM1 | P1 | scope_review_before_any_data_fill | missing_isin_primary | debt_or_securitized_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::SC1 | P1 | scope_review_before_any_data_fill | missing_isin_primary | inactive_or_legacy_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::SCA | P1 | scope_review_before_any_data_fill | missing_isin_primary | fund_or_trust_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::SFV | P1 | scope_review_before_any_data_fill | missing_isin_primary | inactive_or_legacy_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::SP4 | P1 | scope_review_before_any_data_fill | missing_isin_primary | debt_or_securitized_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::SP6 | P1 | scope_review_before_any_data_fill | missing_isin_primary | debt_or_securitized_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::TA1 | P1 | scope_review_before_any_data_fill | missing_isin_primary | inactive_or_legacy_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::TT1 | P1 | scope_review_before_any_data_fill | missing_isin_primary | debt_or_securitized_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::TT2 | P1 | scope_review_before_any_data_fill | missing_isin_primary | debt_or_securitized_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::TT6 | P1 | scope_review_before_any_data_fill | missing_isin_primary | debt_or_securitized_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::VCD | P1 | scope_review_before_any_data_fill | missing_isin_primary | fund_or_trust_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::WB1 | P1 | scope_review_before_any_data_fill | missing_isin_primary | debt_or_securitized_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::WE1 | P1 | scope_review_before_any_data_fill | missing_isin_primary | debt_or_securitized_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::WS1 | P1 | scope_review_before_any_data_fill | missing_isin_primary | debt_or_securitized_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::WS2 | P1 | scope_review_before_any_data_fill | missing_isin_primary | debt_or_securitized_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::WSE | P1 | scope_review_before_any_data_fill | missing_isin_primary | debt_or_securitized_identifier_gap | core_exclusion_candidate | core_exclusion_candidate_requires_scope_review |
| ASX::CDO | P2 | product_taxonomy_source_gap | missing_etf_category | official_product_taxonomy_unavailable_gap | accepted_source_gap | accepted_source_gap_requires_official_product_taxonomy |
| ASX::DN1 | P2 | product_taxonomy_source_gap | missing_etf_category | official_product_taxonomy_unavailable_gap | accepted_source_gap | accepted_source_gap_requires_official_product_taxonomy |
| ASX::OPH | P2 | product_taxonomy_source_gap | missing_etf_category | official_product_taxonomy_unavailable_gap | accepted_source_gap | accepted_source_gap_requires_official_product_taxonomy |
| ASX::PR4 | P2 | product_taxonomy_source_gap | missing_etf_category | official_product_taxonomy_unavailable_gap | accepted_source_gap | accepted_source_gap_requires_official_product_taxonomy |
| ASX::PU6 | P2 | product_taxonomy_source_gap | missing_etf_category | official_product_taxonomy_unavailable_gap | accepted_source_gap | accepted_source_gap_requires_official_product_taxonomy |
| ASX::PU8 | P2 | product_taxonomy_source_gap | missing_etf_category | official_product_taxonomy_unavailable_gap | accepted_source_gap | accepted_source_gap_requires_official_product_taxonomy |
| ASX::PU9 | P2 | product_taxonomy_source_gap | missing_etf_category | official_product_taxonomy_unavailable_gap | accepted_source_gap | accepted_source_gap_requires_official_product_taxonomy |
| ASX::SCA | P2 | product_taxonomy_source_gap | missing_etf_category | official_product_taxonomy_unavailable_gap | accepted_source_gap | accepted_source_gap_requires_official_product_taxonomy |
| ASX::VCD | P2 | product_taxonomy_source_gap | missing_etf_category | official_product_taxonomy_unavailable_gap | accepted_source_gap | accepted_source_gap_requires_official_product_taxonomy |
| ASX::BP1 | P3 | identifier_source_gap | missing_isin_primary | official_current_directory_absent_identifier_gap | accepted_source_gap | accepted_source_gap_not_in_current_asx_isin_workbook |
| ASX::DXA | P3 | identifier_source_gap | missing_isin_primary | official_current_directory_absent_identifier_gap | accepted_source_gap | accepted_source_gap_not_in_current_asx_isin_workbook |
| ASX::KFW | P3 | identifier_source_gap | missing_isin_primary | official_current_directory_absent_identifier_gap | accepted_source_gap | accepted_source_gap_not_in_current_asx_isin_workbook |
| ASX::LO1 | P3 | identifier_source_gap | missing_isin_primary | official_current_directory_absent_identifier_gap | accepted_source_gap | accepted_source_gap_not_in_current_asx_isin_workbook |
| ASX::SHZ | P3 | identifier_source_gap | missing_isin_primary | official_current_directory_absent_identifier_gap | accepted_source_gap | accepted_source_gap_not_in_current_asx_isin_workbook |
| ASX::XCL | P3 | identifier_source_gap | missing_isin_primary | official_current_directory_absent_identifier_gap | accepted_source_gap | accepted_source_gap_not_in_current_asx_isin_workbook |
| ASX::XNC | P3 | identifier_source_gap | missing_isin_primary | official_current_directory_absent_identifier_gap | accepted_source_gap | accepted_source_gap_not_in_current_asx_isin_workbook |
| ASX::XQL | P3 | identifier_source_gap | missing_isin_primary | official_current_directory_absent_identifier_gap | accepted_source_gap | accepted_source_gap_not_in_current_asx_isin_workbook |
| ASX::XVG | P3 | identifier_source_gap | missing_isin_primary | official_current_directory_absent_identifier_gap | accepted_source_gap | accepted_source_gap_not_in_current_asx_isin_workbook |
| ASX::ZOR | P3 | identifier_source_gap | missing_isin_primary | official_current_directory_absent_identifier_gap | accepted_source_gap | accepted_source_gap_not_in_current_asx_isin_workbook |

## Policy

- Do not infer ISINs from issuer names, symbol shape, or same-issuer instruments.
- ETF categories remain blank until official or reviewed product taxonomy evidence exists.
- Core-exclusion candidates require explicit scope review before identifier/category enrichment.

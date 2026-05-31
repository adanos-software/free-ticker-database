# OTC Scope Review

Generated at: `2026-05-25T12:54:46Z`

This report verifies OTC listing scope before any OTC metadata enrichment. It does not change scope or fill fields.

## Summary

- OTC listing rows: `11056`
- OTC drop overrides already removed from current rows: `75`
- OTC reviewed name decisions: `26`
- Active OTC name mismatches already covered by reviewed decisions: `0`
- Active OTC name mismatches still unreviewed: `146`
- Reviewed OTC decisions suppressing current listing warnings: `26`
- Reviewed OTC decisions outside current OTC scope: `0`
- OTC core-exclusion candidates requiring scope decision: `0`
- Post-scope OTC metadata backlog rows: `3732`

## OTC Review Decision Resolution

| Queue | Rows |
|---|---:|
| pending_active_name_mismatch_review | 146 |
| reviewed_decision_suppresses_current_listing_warning | 26 |

## Scope Decisions

| Decision | Rows |
|---|---:|
| already_extended_otc_listing | 11056 |

## Scope Completion

| Metric | Value |
|---|---|
| status | complete_extended_scope_no_core_candidates |
| rows | 11056 |
| extended_otc_rows | 11056 |
| otc_listing_scope_reason_rows | 11056 |
| already_extended_scope_decision_rows | 11056 |
| core_exclusion_candidate_rows | 0 |
| unexpected_core_scope_rows | 0 |
| blocked_scope_decision_rows | 0 |
| scope_apply_allowed_rows | 0 |
| metadata_enrichment_authorized | False |
| source_gate | OTC scope is complete only when every current OTC row is extended/otc_listing and no core or core-exclusion scope decision remains open; metadata still requires listing-keyed evidence. |

## Post-Scope Metadata Backlog

| Metric | Value |
|---|---|
| status | metadata_review_backlog_open |
| rows | 3732 |
| scope_blocked_rows | 0 |
| metadata_enrichment_authorized | False |
| source_gate | Post-scope OTC metadata work remains blocked unless each row has listing-keyed OTC Markets, issuer, SEC, registry, or reviewed fallback evidence; no ticker-only enrichment is allowed. |

| Review bucket | Rows |
|---|---:|
| documented_otc_category_source_gap | 29 |
| documented_otc_sector_source_gap | 792 |
| official_name_mismatch_review_first | 146 |
| otc_quality_source_gap_review | 2765 |

| Metadata gate | Rows |
|---|---:|
| otc_name_mismatch_review_required_before_name_or_metadata_changes | 146 |
| reviewed_issuer_sector_source_required_keep_blank | 792 |
| reviewed_product_category_source_required_keep_blank | 29 |
| source_gap_review_required_before_enrichment | 2765 |

| Listing key | Ticker | Asset type | Bucket | Quality | Metadata gate | Evidence required | Recommended source | Source gate |
|---|---|---|---|---|---|---|---|---|
| OTC::ABNAF | ABNAF | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::ACGBY | ACGBY | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::AECX | AECX | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::AKBTY | AKBTY | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::AKOM | AKOM | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::ANGCF | ANGCF | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::APCOF | APCOF | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::APELY | APELY | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::APETF | APETF | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::ARAFF | ARAFF | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::ARGGY | ARGGY | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::ARGYF | ARGYF | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::ARLSF | ARLSF | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::ARXRF | ARXRF | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::ASBFY | ASBFY | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::ASEKY | ASEKY | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::ATONY | ATONY | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::ATOXF | ATOXF | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::ATUUF | ATUUF | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::AUHIF | AUHIF | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::AWSL | AWSL | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::AXXA | AXXA | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::AZLCZ | AZLCZ | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::AZURF | AZURF | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::BADEF | BADEF | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |

## Core-Exclusion Scope Gate

Core-exclusion candidates are blocked from identifier, name, sector, and category enrichment until a reviewed scope decision selects `core`, `extended`, or `exclude`.

| Metric | Rows |
|---|---:|
| otc_core_exclusion_candidate_rows | 0 |

| Asset type | Rows |
|---|---:|

| Metadata gate | Rows |
|---|---:|

| Listing key | Ticker | Asset type | Quality | Metadata gate | Action |
|---|---|---|---|---|---|

## Quality Status

| Status | Rows |
|---|---:|
| pass | 7813 |
| source_gap | 3097 |
| warn | 146 |

## Review Priorities

| Priority | Rows |
|---|---:|
| P2 | 146 |
| P3 | 3586 |
| P4 | 7324 |

## Review Buckets

| Bucket | Rows |
|---|---:|
| clean_extended_otc_listing | 7324 |
| documented_otc_category_source_gap | 29 |
| documented_otc_sector_source_gap | 792 |
| official_name_mismatch_review_first | 146 |
| otc_quality_source_gap_review | 2765 |

## Review Bucket By Asset Type

| Bucket | Asset Type | Rows |
|---|---|---:|
| clean_extended_otc_listing | ETF | 142 |
| clean_extended_otc_listing | Stock | 7182 |
| documented_otc_category_source_gap | ETF | 29 |
| documented_otc_sector_source_gap | Stock | 792 |
| official_name_mismatch_review_first | ETF | 2 |
| official_name_mismatch_review_first | Stock | 144 |
| otc_quality_source_gap_review | ETF | 49 |
| otc_quality_source_gap_review | Stock | 2716 |

## Review Bucket By Metadata Gate

| Bucket | Metadata gate | Rows |
|---|---|---:|
| clean_extended_otc_listing | no_metadata_enrichment_needed | 7324 |
| documented_otc_category_source_gap | reviewed_product_category_source_required_keep_blank | 29 |
| documented_otc_sector_source_gap | reviewed_issuer_sector_source_required_keep_blank | 792 |
| official_name_mismatch_review_first | otc_name_mismatch_review_required_before_name_or_metadata_changes | 146 |
| otc_quality_source_gap_review | source_gap_review_required_before_enrichment | 2765 |

## Scope Apply Eligibility

| Eligibility | Rows |
|---|---:|
| already_extended_no_scope_change_required | 11056 |

## Metadata Enrichment Gates

| Gate | Rows |
|---|---:|
| no_metadata_enrichment_needed | 7324 |
| otc_name_mismatch_review_required_before_name_or_metadata_changes | 146 |
| reviewed_issuer_sector_source_required_keep_blank | 792 |
| reviewed_product_category_source_required_keep_blank | 29 |
| source_gap_review_required_before_enrichment | 2765 |

## Review Strategies

| Strategy | Rows |
|---|---:|
| keep_category_blank_until_reviewed_product_taxonomy_source | 29 |
| keep_sector_blank_until_reviewed_issuer_sector_source | 792 |
| no_scope_or_metadata_action_required | 7324 |
| resolve_listing_keyed_name_mismatch_before_metadata_work | 146 |
| review_quality_source_gap_before_metadata_work | 2765 |

## Verification Evidence

| Evidence required | Rows |
|---|---:|
| current_pass_status_and_extended_scope_policy_no_metadata_action | 7324 |
| reviewed_issuer_sector_source_with_exact_listing_or_keep_blank_decision | 792 |
| reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | 146 |
| reviewed_product_taxonomy_source_with_exact_listing_or_keep_blank_decision | 29 |
| source_gap_review_or_reviewed_source_before_metadata_change | 2765 |

## Top Review Batches

| Priority | Bucket | Asset type | Quality status | Metadata gate | Rows | Strategy | Evidence required | Recommended next source | Source gate |
|---|---|---|---|---|---:|---|---|---|---|
| P4 | clean_extended_otc_listing | Stock | pass | no_metadata_enrichment_needed | 7182 | no_scope_or_metadata_action_required | current_pass_status_and_extended_scope_policy_no_metadata_action | No additional source required unless a future metadata change is proposed. | Current extended-scope pass row; no metadata action is authorized by this report. |
| P3 | otc_quality_source_gap_review | Stock | source_gap | source_gap_review_required_before_enrichment | 2716 | review_quality_source_gap_before_metadata_work | source_gap_review_or_reviewed_source_before_metadata_change | Entry-quality source-gap artifact and stronger OTC Markets, issuer, SEC, or registry evidence. | Resolve or document the source gap before any metadata enrichment. |
| P3 | documented_otc_sector_source_gap | Stock | pass | reviewed_issuer_sector_source_required_keep_blank | 473 | keep_sector_blank_until_reviewed_issuer_sector_source | reviewed_issuer_sector_source_with_exact_listing_or_keep_blank_decision | SEC SIC, issuer filings, OTC Markets profile, or reviewed secondary company profile. | Canonical stock sector only after exchange/name gate; no ticker/name-only inference. |
| P3 | documented_otc_sector_source_gap | Stock | source_gap | reviewed_issuer_sector_source_required_keep_blank | 319 | keep_sector_blank_until_reviewed_issuer_sector_source | reviewed_issuer_sector_source_with_exact_listing_or_keep_blank_decision | SEC SIC, issuer filings, OTC Markets profile, or reviewed secondary company profile. | Canonical stock sector only after exchange/name gate; no ticker/name-only inference. |
| P2 | official_name_mismatch_review_first | Stock | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | 144 | resolve_listing_keyed_name_mismatch_before_metadata_work | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| P4 | clean_extended_otc_listing | ETF | pass | no_metadata_enrichment_needed | 142 | no_scope_or_metadata_action_required | current_pass_status_and_extended_scope_policy_no_metadata_action | No additional source required unless a future metadata change is proposed. | Current extended-scope pass row; no metadata action is authorized by this report. |
| P3 | otc_quality_source_gap_review | ETF | source_gap | source_gap_review_required_before_enrichment | 49 | review_quality_source_gap_before_metadata_work | source_gap_review_or_reviewed_source_before_metadata_change | Entry-quality source-gap artifact and stronger OTC Markets, issuer, SEC, or registry evidence. | Resolve or document the source gap before any metadata enrichment. |
| P3 | documented_otc_category_source_gap | ETF | pass | reviewed_product_category_source_required_keep_blank | 16 | keep_category_blank_until_reviewed_product_taxonomy_source | reviewed_product_taxonomy_source_with_exact_listing_or_keep_blank_decision | Issuer fund documents, ETF sponsor page, prospectus, OTC Markets profile, or reviewed product taxonomy source. | ETF category only from exact product evidence; no category inference from ticker or issuer family. |
| P3 | documented_otc_category_source_gap | ETF | source_gap | reviewed_product_category_source_required_keep_blank | 13 | keep_category_blank_until_reviewed_product_taxonomy_source | reviewed_product_taxonomy_source_with_exact_listing_or_keep_blank_decision | Issuer fund documents, ETF sponsor page, prospectus, OTC Markets profile, or reviewed product taxonomy source. | ETF category only from exact product evidence; no category inference from ticker or issuer family. |
| P2 | official_name_mismatch_review_first | ETF | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | 2 | resolve_listing_keyed_name_mismatch_before_metadata_work | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |

## Source Gap Fields

| Field | Rows |
|---|---:|
| missing_etf_category | 29 |
| missing_sector_stock | 817 |

## Policy

- OTC rows stay `extended/otc_listing`; unexpected OTC core rows require scope review before release.
- OTC sector/category blanks are source gaps, not an invitation for symbol-only or name-shape enrichment.
- Name warnings route through the OTC name mismatch review before any canonical name change.

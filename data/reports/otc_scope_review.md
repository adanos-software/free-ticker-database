# OTC Scope Review

Generated at: `2026-06-02T20:38:58Z`

This report verifies OTC listing scope before any OTC metadata enrichment. It does not change scope or fill fields.

## Summary

- OTC listing rows: `11054`
- OTC drop overrides already removed from current rows: `75`
- OTC reviewed name decisions: `26`
- Active OTC name mismatches already covered by reviewed decisions: `0`
- Active OTC name mismatches still unreviewed: `24`
- Reviewed OTC decisions suppressing current listing warnings: `26`
- Reviewed OTC decisions outside current OTC scope: `0`
- OTC core-exclusion candidates requiring scope decision: `0`
- Post-scope OTC metadata backlog rows: `3709`

## OTC Review Decision Resolution

| Queue | Rows |
|---|---:|
| pending_active_name_mismatch_review | 24 |
| reviewed_decision_suppresses_current_listing_warning | 26 |

## Scope Decisions

| Decision | Rows |
|---|---:|
| already_extended_otc_listing | 11054 |

## Scope Completion

| Metric | Value |
|---|---|
| status | complete_extended_scope_no_core_candidates |
| rows | 11054 |
| extended_otc_rows | 11054 |
| otc_listing_scope_reason_rows | 11054 |
| already_extended_scope_decision_rows | 11054 |
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
| rows | 3709 |
| scope_blocked_rows | 0 |
| metadata_enrichment_authorized | False |
| source_gate | Post-scope OTC metadata work remains blocked unless each row has listing-keyed OTC Markets, issuer, SEC, registry, or reviewed fallback evidence; no ticker-only enrichment is allowed. |

| Review bucket | Rows |
|---|---:|
| documented_otc_category_source_gap | 29 |
| documented_otc_sector_source_gap | 813 |
| official_name_mismatch_review_first | 24 |
| otc_quality_source_gap_review | 2843 |

| Metadata gate | Rows |
|---|---:|
| otc_name_mismatch_review_required_before_name_or_metadata_changes | 24 |
| reviewed_issuer_sector_source_required_keep_blank | 813 |
| reviewed_product_category_source_required_keep_blank | 29 |
| source_gap_review_required_before_enrichment | 2843 |

| Listing key | Ticker | Asset type | Bucket | Quality | Metadata gate | Evidence required | Recommended source | Source gate |
|---|---|---|---|---|---|---|---|---|
| OTC::AMFN | AMFN | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::ARAFF | ARAFF | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::BCRD | BCRD | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::DCNSF | DCNSF | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::DLICY | DLICY | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::FSLUF | FSLUF | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::GBBGF | GBBGF | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::GLDFF | GLDFF | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::IONGF | IONGF | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::ISRMF | ISRMF | ETF | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::ITGMF | ITGMF | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::KAIFF | KAIFF | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::OLOXF | OLOXF | ETF | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::PNTZF | PNTZF | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::PPZRF | PPZRF | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::QNICF | QNICF | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::RDEXF | RDEXF | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::SCYRF | SCYRF | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::SDSDF | SDSDF | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::SSPLF | SSPLF | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::UREKF | UREKF | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::VGLS | VGLS | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::VTEPF | VTEPF | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::WCHS | WCHS | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::EWQQF | EWQQF | ETF | documented_otc_category_source_gap | pass | reviewed_product_category_source_required_keep_blank | reviewed_product_taxonomy_source_with_exact_listing_or_keep_blank_decision | Issuer fund documents, ETF sponsor page, prospectus, OTC Markets profile, or reviewed product taxonomy source. | ETF category only from exact product evidence; no category inference from ticker or issuer family. |

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
| pass | 7847 |
| source_gap | 3183 |
| warn | 24 |

## Review Priorities

| Priority | Rows |
|---|---:|
| P2 | 24 |
| P3 | 3685 |
| P4 | 7345 |

## Review Buckets

| Bucket | Rows |
|---|---:|
| clean_extended_otc_listing | 7345 |
| documented_otc_category_source_gap | 29 |
| documented_otc_sector_source_gap | 813 |
| official_name_mismatch_review_first | 24 |
| otc_quality_source_gap_review | 2843 |

## Review Bucket By Asset Type

| Bucket | Asset Type | Rows |
|---|---|---:|
| clean_extended_otc_listing | ETF | 141 |
| clean_extended_otc_listing | Stock | 7204 |
| documented_otc_category_source_gap | ETF | 29 |
| documented_otc_sector_source_gap | Stock | 813 |
| official_name_mismatch_review_first | ETF | 2 |
| official_name_mismatch_review_first | Stock | 22 |
| otc_quality_source_gap_review | ETF | 50 |
| otc_quality_source_gap_review | Stock | 2793 |

## Review Bucket By Metadata Gate

| Bucket | Metadata gate | Rows |
|---|---|---:|
| clean_extended_otc_listing | no_metadata_enrichment_needed | 7345 |
| documented_otc_category_source_gap | reviewed_product_category_source_required_keep_blank | 29 |
| documented_otc_sector_source_gap | reviewed_issuer_sector_source_required_keep_blank | 813 |
| official_name_mismatch_review_first | otc_name_mismatch_review_required_before_name_or_metadata_changes | 24 |
| otc_quality_source_gap_review | source_gap_review_required_before_enrichment | 2843 |

## Scope Apply Eligibility

| Eligibility | Rows |
|---|---:|
| already_extended_no_scope_change_required | 11054 |

## Metadata Enrichment Gates

| Gate | Rows |
|---|---:|
| no_metadata_enrichment_needed | 7345 |
| otc_name_mismatch_review_required_before_name_or_metadata_changes | 24 |
| reviewed_issuer_sector_source_required_keep_blank | 813 |
| reviewed_product_category_source_required_keep_blank | 29 |
| source_gap_review_required_before_enrichment | 2843 |

## Review Strategies

| Strategy | Rows |
|---|---:|
| keep_category_blank_until_reviewed_product_taxonomy_source | 29 |
| keep_sector_blank_until_reviewed_issuer_sector_source | 813 |
| no_scope_or_metadata_action_required | 7345 |
| resolve_listing_keyed_name_mismatch_before_metadata_work | 24 |
| review_quality_source_gap_before_metadata_work | 2843 |

## Verification Evidence

| Evidence required | Rows |
|---|---:|
| current_pass_status_and_extended_scope_policy_no_metadata_action | 7345 |
| reviewed_issuer_sector_source_with_exact_listing_or_keep_blank_decision | 813 |
| reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | 24 |
| reviewed_product_taxonomy_source_with_exact_listing_or_keep_blank_decision | 29 |
| source_gap_review_or_reviewed_source_before_metadata_change | 2843 |

## Top Review Batches

| Priority | Bucket | Asset type | Quality status | Metadata gate | Rows | Strategy | Evidence required | Recommended next source | Source gate |
|---|---|---|---|---|---:|---|---|---|---|
| P4 | clean_extended_otc_listing | Stock | pass | no_metadata_enrichment_needed | 7204 | no_scope_or_metadata_action_required | current_pass_status_and_extended_scope_policy_no_metadata_action | No additional source required unless a future metadata change is proposed. | Current extended-scope pass row; no metadata action is authorized by this report. |
| P3 | otc_quality_source_gap_review | Stock | source_gap | source_gap_review_required_before_enrichment | 2793 | review_quality_source_gap_before_metadata_work | source_gap_review_or_reviewed_source_before_metadata_change | Entry-quality source-gap artifact and stronger OTC Markets, issuer, SEC, or registry evidence. | Resolve or document the source gap before any metadata enrichment. |
| P3 | documented_otc_sector_source_gap | Stock | pass | reviewed_issuer_sector_source_required_keep_blank | 475 | keep_sector_blank_until_reviewed_issuer_sector_source | reviewed_issuer_sector_source_with_exact_listing_or_keep_blank_decision | SEC SIC, issuer filings, OTC Markets profile, or reviewed secondary company profile. | Canonical stock sector only after exchange/name gate; no ticker/name-only inference. |
| P3 | documented_otc_sector_source_gap | Stock | source_gap | reviewed_issuer_sector_source_required_keep_blank | 338 | keep_sector_blank_until_reviewed_issuer_sector_source | reviewed_issuer_sector_source_with_exact_listing_or_keep_blank_decision | SEC SIC, issuer filings, OTC Markets profile, or reviewed secondary company profile. | Canonical stock sector only after exchange/name gate; no ticker/name-only inference. |
| P4 | clean_extended_otc_listing | ETF | pass | no_metadata_enrichment_needed | 141 | no_scope_or_metadata_action_required | current_pass_status_and_extended_scope_policy_no_metadata_action | No additional source required unless a future metadata change is proposed. | Current extended-scope pass row; no metadata action is authorized by this report. |
| P3 | otc_quality_source_gap_review | ETF | source_gap | source_gap_review_required_before_enrichment | 50 | review_quality_source_gap_before_metadata_work | source_gap_review_or_reviewed_source_before_metadata_change | Entry-quality source-gap artifact and stronger OTC Markets, issuer, SEC, or registry evidence. | Resolve or document the source gap before any metadata enrichment. |
| P3 | documented_otc_category_source_gap | ETF | pass | reviewed_product_category_source_required_keep_blank | 27 | keep_category_blank_until_reviewed_product_taxonomy_source | reviewed_product_taxonomy_source_with_exact_listing_or_keep_blank_decision | Issuer fund documents, ETF sponsor page, prospectus, OTC Markets profile, or reviewed product taxonomy source. | ETF category only from exact product evidence; no category inference from ticker or issuer family. |
| P2 | official_name_mismatch_review_first | Stock | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | 22 | resolve_listing_keyed_name_mismatch_before_metadata_work | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| P2 | official_name_mismatch_review_first | ETF | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | 2 | resolve_listing_keyed_name_mismatch_before_metadata_work | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| P3 | documented_otc_category_source_gap | ETF | source_gap | reviewed_product_category_source_required_keep_blank | 2 | keep_category_blank_until_reviewed_product_taxonomy_source | reviewed_product_taxonomy_source_with_exact_listing_or_keep_blank_decision | Issuer fund documents, ETF sponsor page, prospectus, OTC Markets profile, or reviewed product taxonomy source. | ETF category only from exact product evidence; no category inference from ticker or issuer family. |

## Source Gap Fields

| Field | Rows |
|---|---:|
| missing_etf_category | 29 |
| missing_sector_stock | 817 |

## Policy

- OTC rows stay `extended/otc_listing`; unexpected OTC core rows require scope review before release.
- OTC sector/category blanks are source gaps, not an invitation for symbol-only or name-shape enrichment.
- Name warnings route through the OTC name mismatch review before any canonical name change.

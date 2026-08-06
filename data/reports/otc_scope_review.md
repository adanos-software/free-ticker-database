# OTC Scope Review

Generated at: `2026-08-06T14:09:08Z`

This report verifies OTC listing scope before any OTC metadata enrichment. It does not change scope or fill fields.

## Summary

- OTC listing rows: `11094`
- OTC drop overrides already removed from current rows: `99`
- OTC reviewed name decisions: `43`
- Active OTC name mismatches already covered by reviewed decisions: `17`
- Active OTC name mismatches still unreviewed: `0`
- Reviewed OTC decisions suppressing current listing warnings: `26`
- Reviewed OTC decisions outside current OTC scope: `0`
- OTC core-exclusion candidates requiring scope decision: `0`
- Post-scope OTC metadata backlog rows: `3206`

## OTC Review Decision Resolution

| Queue | Rows |
|---|---:|
| reviewed_decision_covers_active_name_mismatch | 17 |
| reviewed_decision_suppresses_current_listing_warning | 26 |

## Scope Decisions

| Decision | Rows |
|---|---:|
| already_extended_otc_listing | 11094 |

## Scope Completion

| Metric | Value |
|---|---|
| status | complete_extended_scope_no_core_candidates |
| rows | 11094 |
| extended_otc_rows | 11094 |
| otc_listing_scope_reason_rows | 11094 |
| already_extended_scope_decision_rows | 11094 |
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
| rows | 3206 |
| scope_blocked_rows | 0 |
| metadata_enrichment_authorized | False |
| source_gate | Post-scope OTC metadata work remains blocked unless each row has listing-keyed OTC Markets, issuer, SEC, registry, or reviewed fallback evidence; no ticker-only enrichment is allowed. |

| Review bucket | Rows |
|---|---:|
| documented_otc_sector_source_gap | 17 |
| documented_otc_source_gap | 3172 |
| official_name_mismatch_review_first | 17 |

| Metadata gate | Rows |
|---|---:|
| otc_name_mismatch_review_required_before_name_or_metadata_changes | 17 |
| reviewed_issuer_sector_source_required_keep_blank | 22 |
| reviewed_source_required_keep_blank | 3167 |

| Listing key | Ticker | Asset type | Bucket | Quality | Metadata gate | Evidence required | Recommended source | Source gate |
|---|---|---|---|---|---|---|---|---|
| OTC::ACQC | ACQC | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::ADLRF | ADLRF | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::AGNPF | AGNPF | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::CLUS | CLUS | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::DCNSF | DCNSF | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::EQLCF | EQLCF | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::FLXI | FLXI | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::GCWOF | GCWOF | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::GPUSF | GPUSF | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::MNZO | MNZO | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::NMKCP | NMKCP | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::OLOXF | OLOXF | ETF | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::PHDWY | PHDWY | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::REXC | REXC | ETF | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::TLGYF | TLGYF | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::XESP | XESP | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::ZCRMF | ZCRMF | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::ABZPF | ABZPF | Stock | documented_otc_sector_source_gap | pass | reviewed_issuer_sector_source_required_keep_blank | reviewed_issuer_sector_source_with_exact_listing_or_keep_blank_decision | SEC SIC, issuer filings, OTC Markets profile, or reviewed secondary company profile. | Canonical stock sector only after exchange/name gate; no ticker/name-only inference. |
| OTC::AETLF | AETLF | Stock | documented_otc_sector_source_gap | pass | reviewed_issuer_sector_source_required_keep_blank | reviewed_issuer_sector_source_with_exact_listing_or_keep_blank_decision | SEC SIC, issuer filings, OTC Markets profile, or reviewed secondary company profile. | Canonical stock sector only after exchange/name gate; no ticker/name-only inference. |
| OTC::AGFAF | AGFAF | Stock | documented_otc_sector_source_gap | pass | reviewed_issuer_sector_source_required_keep_blank | reviewed_issuer_sector_source_with_exact_listing_or_keep_blank_decision | SEC SIC, issuer filings, OTC Markets profile, or reviewed secondary company profile. | Canonical stock sector only after exchange/name gate; no ticker/name-only inference. |
| OTC::APNHF | APNHF | Stock | documented_otc_sector_source_gap | pass | reviewed_issuer_sector_source_required_keep_blank | reviewed_issuer_sector_source_with_exact_listing_or_keep_blank_decision | SEC SIC, issuer filings, OTC Markets profile, or reviewed secondary company profile. | Canonical stock sector only after exchange/name gate; no ticker/name-only inference. |
| OTC::BUHPF | BUHPF | Stock | documented_otc_sector_source_gap | pass | reviewed_issuer_sector_source_required_keep_blank | reviewed_issuer_sector_source_with_exact_listing_or_keep_blank_decision | SEC SIC, issuer filings, OTC Markets profile, or reviewed secondary company profile. | Canonical stock sector only after exchange/name gate; no ticker/name-only inference. |
| OTC::ESSI | ESSI | Stock | documented_otc_sector_source_gap | pass | reviewed_issuer_sector_source_required_keep_blank | reviewed_issuer_sector_source_with_exact_listing_or_keep_blank_decision | SEC SIC, issuer filings, OTC Markets profile, or reviewed secondary company profile. | Canonical stock sector only after exchange/name gate; no ticker/name-only inference. |
| OTC::FANDF | FANDF | Stock | documented_otc_sector_source_gap | pass | reviewed_issuer_sector_source_required_keep_blank | reviewed_issuer_sector_source_with_exact_listing_or_keep_blank_decision | SEC SIC, issuer filings, OTC Markets profile, or reviewed secondary company profile. | Canonical stock sector only after exchange/name gate; no ticker/name-only inference. |
| OTC::FKSHF | FKSHF | Stock | documented_otc_sector_source_gap | pass | reviewed_issuer_sector_source_required_keep_blank | reviewed_issuer_sector_source_with_exact_listing_or_keep_blank_decision | SEC SIC, issuer filings, OTC Markets profile, or reviewed secondary company profile. | Canonical stock sector only after exchange/name gate; no ticker/name-only inference. |

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
| pass | 7905 |
| source_gap | 3172 |
| warn | 17 |

## Review Priorities

| Priority | Rows |
|---|---:|
| P2 | 17 |
| P3 | 3189 |
| P4 | 7888 |

## Review Buckets

| Bucket | Rows |
|---|---:|
| clean_extended_otc_listing | 7888 |
| documented_otc_sector_source_gap | 17 |
| documented_otc_source_gap | 3172 |
| official_name_mismatch_review_first | 17 |

## Review Bucket By Asset Type

| Bucket | Asset Type | Rows |
|---|---|---:|
| clean_extended_otc_listing | ETF | 173 |
| clean_extended_otc_listing | Stock | 7715 |
| documented_otc_sector_source_gap | Stock | 17 |
| documented_otc_source_gap | ETF | 53 |
| documented_otc_source_gap | Stock | 3119 |
| official_name_mismatch_review_first | ETF | 2 |
| official_name_mismatch_review_first | Stock | 15 |

## Review Bucket By Metadata Gate

| Bucket | Metadata gate | Rows |
|---|---|---:|
| clean_extended_otc_listing | no_metadata_enrichment_needed | 7888 |
| documented_otc_sector_source_gap | reviewed_issuer_sector_source_required_keep_blank | 17 |
| documented_otc_source_gap | reviewed_issuer_sector_source_required_keep_blank | 5 |
| documented_otc_source_gap | reviewed_source_required_keep_blank | 3167 |
| official_name_mismatch_review_first | otc_name_mismatch_review_required_before_name_or_metadata_changes | 17 |

## Scope Apply Eligibility

| Eligibility | Rows |
|---|---:|
| already_extended_no_scope_change_required | 11094 |

## Metadata Enrichment Gates

| Gate | Rows |
|---|---:|
| no_metadata_enrichment_needed | 7888 |
| otc_name_mismatch_review_required_before_name_or_metadata_changes | 17 |
| reviewed_issuer_sector_source_required_keep_blank | 22 |
| reviewed_source_required_keep_blank | 3167 |

## Review Strategies

| Strategy | Rows |
|---|---:|
| keep_metadata_blank_until_reviewed_otc_source | 3172 |
| keep_sector_blank_until_reviewed_issuer_sector_source | 17 |
| no_scope_or_metadata_action_required | 7888 |
| resolve_listing_keyed_name_mismatch_before_metadata_work | 17 |

## Verification Evidence

| Evidence required | Rows |
|---|---:|
| current_pass_status_and_extended_scope_policy_no_metadata_action | 7888 |
| reviewed_issuer_sector_source_with_exact_listing_or_keep_blank_decision | 17 |
| reviewed_otc_metadata_source_with_exact_listing_or_keep_blank_decision | 3172 |
| reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | 17 |

## Top Review Batches

| Priority | Bucket | Asset type | Quality status | Metadata gate | Rows | Strategy | Evidence required | Recommended next source | Source gate |
|---|---|---|---|---|---:|---|---|---|---|
| P4 | clean_extended_otc_listing | Stock | pass | no_metadata_enrichment_needed | 7715 | no_scope_or_metadata_action_required | current_pass_status_and_extended_scope_policy_no_metadata_action | No additional source required unless a future metadata change is proposed. | Current extended-scope pass row; no metadata action is authorized by this report. |
| P3 | documented_otc_source_gap | Stock | source_gap | reviewed_source_required_keep_blank | 3114 | keep_metadata_blank_until_reviewed_otc_source | reviewed_otc_metadata_source_with_exact_listing_or_keep_blank_decision | Exact listing-keyed OTC Markets, issuer, SEC, or reviewed registry evidence. | Keep metadata blank until exact listing-keyed source evidence or reviewed keep-blank decision exists. |
| P4 | clean_extended_otc_listing | ETF | pass | no_metadata_enrichment_needed | 173 | no_scope_or_metadata_action_required | current_pass_status_and_extended_scope_policy_no_metadata_action | No additional source required unless a future metadata change is proposed. | Current extended-scope pass row; no metadata action is authorized by this report. |
| P3 | documented_otc_source_gap | ETF | source_gap | reviewed_source_required_keep_blank | 53 | keep_metadata_blank_until_reviewed_otc_source | reviewed_otc_metadata_source_with_exact_listing_or_keep_blank_decision | Exact listing-keyed OTC Markets, issuer, SEC, or reviewed registry evidence. | Keep metadata blank until exact listing-keyed source evidence or reviewed keep-blank decision exists. |
| P3 | documented_otc_sector_source_gap | Stock | pass | reviewed_issuer_sector_source_required_keep_blank | 17 | keep_sector_blank_until_reviewed_issuer_sector_source | reviewed_issuer_sector_source_with_exact_listing_or_keep_blank_decision | SEC SIC, issuer filings, OTC Markets profile, or reviewed secondary company profile. | Canonical stock sector only after exchange/name gate; no ticker/name-only inference. |
| P2 | official_name_mismatch_review_first | Stock | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | 15 | resolve_listing_keyed_name_mismatch_before_metadata_work | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| P3 | documented_otc_source_gap | Stock | source_gap | reviewed_issuer_sector_source_required_keep_blank | 5 | keep_metadata_blank_until_reviewed_otc_source | reviewed_otc_metadata_source_with_exact_listing_or_keep_blank_decision | Exact listing-keyed OTC Markets, issuer, SEC, or reviewed registry evidence. | Keep metadata blank until exact listing-keyed source evidence or reviewed keep-blank decision exists. |
| P2 | official_name_mismatch_review_first | ETF | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | 2 | resolve_listing_keyed_name_mismatch_before_metadata_work | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |

## Source Gap Fields

| Field | Rows |
|---|---:|
| missing_sector_stock | 22 |
| official_reference_gap | 3172 |

## Policy

- OTC rows stay `extended/otc_listing`; unexpected OTC core rows require scope review before release.
- OTC sector/category blanks are source gaps, not an invitation for symbol-only or name-shape enrichment.
- Name warnings route through the OTC name mismatch review before any canonical name change.

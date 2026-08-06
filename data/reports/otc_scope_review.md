# OTC Scope Review

Generated at: `2026-08-06T14:18:41Z`

This report verifies OTC listing scope before any OTC metadata enrichment. It does not change scope or fill fields.

## Summary

- OTC listing rows: `11754`
- OTC drop overrides already removed from current rows: `99`
- OTC reviewed name decisions: `43`
- Active OTC name mismatches already covered by reviewed decisions: `17`
- Active OTC name mismatches still unreviewed: `12`
- Reviewed OTC decisions suppressing current listing warnings: `26`
- Reviewed OTC decisions outside current OTC scope: `0`
- OTC core-exclusion candidates requiring scope decision: `0`
- Post-scope OTC metadata backlog rows: `3840`

## OTC Review Decision Resolution

| Queue | Rows |
|---|---:|
| pending_active_name_mismatch_review | 12 |
| reviewed_decision_covers_active_name_mismatch | 17 |
| reviewed_decision_suppresses_current_listing_warning | 26 |

## Scope Decisions

| Decision | Rows |
|---|---:|
| already_extended_otc_listing | 11754 |

## Scope Completion

| Metric | Value |
|---|---|
| status | complete_extended_scope_no_core_candidates |
| rows | 11754 |
| extended_otc_rows | 11754 |
| otc_listing_scope_reason_rows | 11754 |
| already_extended_scope_decision_rows | 11754 |
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
| rows | 3840 |
| scope_blocked_rows | 0 |
| metadata_enrichment_authorized | False |
| source_gate | Post-scope OTC metadata work remains blocked unless each row has listing-keyed OTC Markets, issuer, SEC, registry, or reviewed fallback evidence; no ticker-only enrichment is allowed. |

| Review bucket | Rows |
|---|---:|
| documented_otc_sector_source_gap | 476 |
| documented_otc_source_gap | 3258 |
| official_name_mismatch_review_first | 29 |
| otc_quality_warn_review | 77 |

| Metadata gate | Rows |
|---|---:|
| entry_quality_warn_review_required_before_enrichment | 77 |
| otc_name_mismatch_review_required_before_name_or_metadata_changes | 29 |
| reviewed_issuer_sector_source_required_keep_blank | 545 |
| reviewed_source_required_keep_blank | 3189 |

| Listing key | Ticker | Asset type | Bucket | Quality | Metadata gate | Evidence required | Recommended source | Source gate |
|---|---|---|---|---|---|---|---|---|
| OTC::ACQC | ACQC | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::ADLRF | ADLRF | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::AGNPF | AGNPF | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::CLUS | CLUS | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::CSXXY | CSXXY | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::DCNSF | DCNSF | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::EQLCF | EQLCF | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::FLXI | FLXI | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::GCWOF | GCWOF | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::GPUSF | GPUSF | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::HISEF | HISEF | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::HKSHY | HKSHY | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::KPCPF | KPCPF | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::LIPO | LIPO | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::MNZO | MNZO | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::MTGRY | MTGRY | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::NISUY | NISUY | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::NMKCP | NMKCP | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::NWSGY | NWSGY | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::OLOXF | OLOXF | ETF | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::PHDWY | PHDWY | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::RDEIY | RDEIY | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::REXC | REXC | ETF | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::SAXJY | SAXJY | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| OTC::TLGYF | TLGYF | Stock | official_name_mismatch_review_first | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |

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
| pass | 8390 |
| source_gap | 3256 |
| warn | 108 |

## Review Priorities

| Priority | Rows |
|---|---:|
| P2 | 106 |
| P3 | 3734 |
| P4 | 7914 |

## Review Buckets

| Bucket | Rows |
|---|---:|
| clean_extended_otc_listing | 7914 |
| documented_otc_sector_source_gap | 476 |
| documented_otc_source_gap | 3258 |
| official_name_mismatch_review_first | 29 |
| otc_quality_warn_review | 77 |

## Review Bucket By Asset Type

| Bucket | Asset Type | Rows |
|---|---|---:|
| clean_extended_otc_listing | ETF | 173 |
| clean_extended_otc_listing | Stock | 7741 |
| documented_otc_sector_source_gap | Stock | 476 |
| documented_otc_source_gap | ETF | 56 |
| documented_otc_source_gap | Stock | 3202 |
| official_name_mismatch_review_first | ETF | 2 |
| official_name_mismatch_review_first | Stock | 27 |
| otc_quality_warn_review | Stock | 77 |

## Review Bucket By Metadata Gate

| Bucket | Metadata gate | Rows |
|---|---|---:|
| clean_extended_otc_listing | no_metadata_enrichment_needed | 7914 |
| documented_otc_sector_source_gap | reviewed_issuer_sector_source_required_keep_blank | 476 |
| documented_otc_source_gap | reviewed_issuer_sector_source_required_keep_blank | 69 |
| documented_otc_source_gap | reviewed_source_required_keep_blank | 3189 |
| official_name_mismatch_review_first | otc_name_mismatch_review_required_before_name_or_metadata_changes | 29 |
| otc_quality_warn_review | entry_quality_warn_review_required_before_enrichment | 77 |

## Scope Apply Eligibility

| Eligibility | Rows |
|---|---:|
| already_extended_no_scope_change_required | 11754 |

## Metadata Enrichment Gates

| Gate | Rows |
|---|---:|
| entry_quality_warn_review_required_before_enrichment | 77 |
| no_metadata_enrichment_needed | 7914 |
| otc_name_mismatch_review_required_before_name_or_metadata_changes | 29 |
| reviewed_issuer_sector_source_required_keep_blank | 545 |
| reviewed_source_required_keep_blank | 3189 |

## Review Strategies

| Strategy | Rows |
|---|---:|
| keep_metadata_blank_until_reviewed_otc_source | 3258 |
| keep_sector_blank_until_reviewed_issuer_sector_source | 476 |
| no_scope_or_metadata_action_required | 7914 |
| resolve_listing_keyed_name_mismatch_before_metadata_work | 29 |
| review_entry_quality_warning_before_metadata_work | 77 |

## Verification Evidence

| Evidence required | Rows |
|---|---:|
| current_pass_status_and_extended_scope_policy_no_metadata_action | 7914 |
| entry_quality_warning_review_before_metadata_change | 77 |
| reviewed_issuer_sector_source_with_exact_listing_or_keep_blank_decision | 476 |
| reviewed_otc_metadata_source_with_exact_listing_or_keep_blank_decision | 3258 |
| reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | 29 |

## Top Review Batches

| Priority | Bucket | Asset type | Quality status | Metadata gate | Rows | Strategy | Evidence required | Recommended next source | Source gate |
|---|---|---|---|---|---:|---|---|---|---|
| P4 | clean_extended_otc_listing | Stock | pass | no_metadata_enrichment_needed | 7741 | no_scope_or_metadata_action_required | current_pass_status_and_extended_scope_policy_no_metadata_action | No additional source required unless a future metadata change is proposed. | Current extended-scope pass row; no metadata action is authorized by this report. |
| P3 | documented_otc_source_gap | Stock | source_gap | reviewed_source_required_keep_blank | 3131 | keep_metadata_blank_until_reviewed_otc_source | reviewed_otc_metadata_source_with_exact_listing_or_keep_blank_decision | Exact listing-keyed OTC Markets, issuer, SEC, or reviewed registry evidence. | Keep metadata blank until exact listing-keyed source evidence or reviewed keep-blank decision exists. |
| P3 | documented_otc_sector_source_gap | Stock | pass | reviewed_issuer_sector_source_required_keep_blank | 476 | keep_sector_blank_until_reviewed_issuer_sector_source | reviewed_issuer_sector_source_with_exact_listing_or_keep_blank_decision | SEC SIC, issuer filings, OTC Markets profile, or reviewed secondary company profile. | Canonical stock sector only after exchange/name gate; no ticker/name-only inference. |
| P4 | clean_extended_otc_listing | ETF | pass | no_metadata_enrichment_needed | 173 | no_scope_or_metadata_action_required | current_pass_status_and_extended_scope_policy_no_metadata_action | No additional source required unless a future metadata change is proposed. | Current extended-scope pass row; no metadata action is authorized by this report. |
| P2 | otc_quality_warn_review | Stock | warn | entry_quality_warn_review_required_before_enrichment | 77 | review_entry_quality_warning_before_metadata_work | entry_quality_warning_review_before_metadata_change | Entry-quality source evidence plus OTC Markets, issuer, SEC, or registry confirmation. | Resolve the quality warning before using the row for metadata enrichment. |
| P3 | documented_otc_source_gap | Stock | source_gap | reviewed_issuer_sector_source_required_keep_blank | 69 | keep_metadata_blank_until_reviewed_otc_source | reviewed_otc_metadata_source_with_exact_listing_or_keep_blank_decision | Exact listing-keyed OTC Markets, issuer, SEC, or reviewed registry evidence. | Keep metadata blank until exact listing-keyed source evidence or reviewed keep-blank decision exists. |
| P3 | documented_otc_source_gap | ETF | source_gap | reviewed_source_required_keep_blank | 56 | keep_metadata_blank_until_reviewed_otc_source | reviewed_otc_metadata_source_with_exact_listing_or_keep_blank_decision | Exact listing-keyed OTC Markets, issuer, SEC, or reviewed registry evidence. | Keep metadata blank until exact listing-keyed source evidence or reviewed keep-blank decision exists. |
| P2 | official_name_mismatch_review_first | Stock | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | 27 | resolve_listing_keyed_name_mismatch_before_metadata_work | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| P2 | official_name_mismatch_review_first | ETF | warn | otc_name_mismatch_review_required_before_name_or_metadata_changes | 2 | resolve_listing_keyed_name_mismatch_before_metadata_work | reviewed_otc_name_mismatch_decision_before_name_or_metadata_change | OTC name mismatch review, OTC Markets profile, SEC/issuer filing, or ISIN-anchored issuer history. | No canonical name or metadata change until the name mismatch has a reviewed listing-keyed decision. |
| P3 | documented_otc_source_gap | Stock | warn | reviewed_source_required_keep_blank | 2 | keep_metadata_blank_until_reviewed_otc_source | reviewed_otc_metadata_source_with_exact_listing_or_keep_blank_decision | Exact listing-keyed OTC Markets, issuer, SEC, or reviewed registry evidence. | Keep metadata blank until exact listing-keyed source evidence or reviewed keep-blank decision exists. |

## Source Gap Fields

| Field | Rows |
|---|---:|
| missing_sector_stock | 556 |
| official_reference_gap | 3258 |

## Policy

- OTC rows stay `extended/otc_listing`; unexpected OTC core rows require scope review before release.
- OTC sector/category blanks are source gaps, not an invitation for symbol-only or name-shape enrichment.
- Name warnings route through the OTC name mismatch review before any canonical name change.

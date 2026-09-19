# Symbol Changes Review

Generated at: `2026-09-19T10:55:19Z`

Daily secondary-source symbol-change feed. Rows are review signals, not automatic canonical ticker updates.

## Summary

| Metric | Rows |
|---|---:|
| Fetched rows | 235 |
| Merged history rows | 351 |
| Review rows | 351 |
| Direct symbol-change apply allowed rows | 0 |

## Symbol-Change Backlog

- Status: `listing_keyed_symbol_change_review_queue_open`
- Rows: `351`
- Rename/delisting review rows: `5`
- Duplicate/cross-listing review rows: `13`
- Already reflected audit rows: `282`
- Out-of-scope collision blocked rows: `27`
- Missing source-scope mapping rows: `10`
- No-dataset-match documentation rows: `14`
- Time-sensitive review rows: `9`
- Secondary feed apply authorized: `false`
- Source gate: Symbol-change feed rows are review signals only; ticker, name, listing, or alias changes require listing-keyed official venue or issuer evidence for old/new symbols and issuer identity.

## Match Status

| Status | Rows |
|---|---:|
| new_symbol_present_old_symbol_missing | 285 |
| no_matching_listing | 15 |
| old_and_new_symbols_present | 17 |
| old_symbol_present_new_symbol_missing | 8 |
| symbol_present_only_outside_source_scope | 26 |

## Workflow Queues

| Queue | Rows |
|---|---:|
| audit_already_reflected | 282 |
| blocked_missing_source_scope_mapping | 10 |
| blocked_out_of_scope_symbol_collision | 27 |
| document_no_dataset_match | 14 |
| review_duplicate_or_cross_listing | 13 |
| review_verified_rename_or_delisting | 5 |

## Workflow Queue By Recency

| Queue / Recency | Rows |
|---|---:|
| audit_already_reflected:older_than_90d | 186 |
| audit_already_reflected:recent_30d | 30 |
| audit_already_reflected:recent_7d | 35 |
| audit_already_reflected:recent_90d | 31 |
| blocked_missing_source_scope_mapping:older_than_90d | 8 |
| blocked_missing_source_scope_mapping:recent_30d | 1 |
| blocked_missing_source_scope_mapping:recent_7d | 1 |
| blocked_out_of_scope_symbol_collision:older_than_90d | 13 |
| blocked_out_of_scope_symbol_collision:recent_30d | 4 |
| blocked_out_of_scope_symbol_collision:recent_7d | 6 |
| blocked_out_of_scope_symbol_collision:recent_90d | 4 |
| document_no_dataset_match:older_than_90d | 9 |
| document_no_dataset_match:recent_7d | 3 |
| document_no_dataset_match:recent_90d | 2 |
| review_duplicate_or_cross_listing:older_than_90d | 6 |
| review_duplicate_or_cross_listing:recent_30d | 3 |
| review_duplicate_or_cross_listing:recent_7d | 3 |
| review_duplicate_or_cross_listing:recent_90d | 1 |
| review_verified_rename_or_delisting:recent_7d | 3 |
| review_verified_rename_or_delisting:recent_90d | 2 |

## Workflow Queue By Priority

| Queue / Priority | Rows |
|---|---:|
| audit_already_reflected:P4 | 282 |
| blocked_missing_source_scope_mapping:P2 | 10 |
| blocked_out_of_scope_symbol_collision:P2 | 27 |
| document_no_dataset_match:P3 | 14 |
| review_duplicate_or_cross_listing:P1 | 13 |
| review_verified_rename_or_delisting:P1 | 5 |

## Workflow Queue By Exchange Scope

| Queue / Scope Status | Rows |
|---|---:|
| audit_already_reflected:global_symbol_collision_outside_source_scope | 64 |
| audit_already_reflected:matches_within_source_scope | 218 |
| blocked_missing_source_scope_mapping:unscoped_source_hint | 10 |
| blocked_out_of_scope_symbol_collision:global_symbol_collision_outside_source_scope | 27 |
| document_no_dataset_match:matches_within_source_scope | 14 |
| review_duplicate_or_cross_listing:matches_within_source_scope | 13 |
| review_verified_rename_or_delisting:matches_within_source_scope | 5 |

## Workflow Queue By Match Status

| Queue / Match Status | Rows |
|---|---:|
| audit_already_reflected:new_symbol_present_old_symbol_missing | 282 |
| blocked_missing_source_scope_mapping:new_symbol_present_old_symbol_missing | 3 |
| blocked_missing_source_scope_mapping:no_matching_listing | 1 |
| blocked_missing_source_scope_mapping:old_and_new_symbols_present | 4 |
| blocked_missing_source_scope_mapping:old_symbol_present_new_symbol_missing | 2 |
| blocked_out_of_scope_symbol_collision:old_symbol_present_new_symbol_missing | 1 |
| blocked_out_of_scope_symbol_collision:symbol_present_only_outside_source_scope | 26 |
| document_no_dataset_match:no_matching_listing | 14 |
| review_duplicate_or_cross_listing:old_and_new_symbols_present | 13 |
| review_verified_rename_or_delisting:old_symbol_present_new_symbol_missing | 5 |

## Workflow Queue By Listing-Key Review

| Queue | Listing-Key Status | Rows |
|---|---|---:|
| audit_already_reflected | new_scoped_listing_key_only | 282 |
| blocked_missing_source_scope_mapping | new_scoped_listing_key_only | 3 |
| blocked_missing_source_scope_mapping | no_scoped_listing_key_match | 1 |
| blocked_missing_source_scope_mapping | old_and_new_scoped_listing_keys_present | 4 |
| blocked_missing_source_scope_mapping | old_scoped_listing_key_only | 2 |
| blocked_out_of_scope_symbol_collision | no_scoped_listing_key_match | 26 |
| blocked_out_of_scope_symbol_collision | old_scoped_listing_key_only | 1 |
| document_no_dataset_match | no_scoped_listing_key_match | 14 |
| review_duplicate_or_cross_listing | old_and_new_scoped_listing_keys_present | 13 |
| review_verified_rename_or_delisting | old_scoped_listing_key_only | 5 |

## Workflow Queue By Source Hint

| Queue | Source Hint | Rows |
|---|---|---:|
| audit_already_reflected | OTC | 13 |
| audit_already_reflected | US_LISTED | 269 |
| blocked_missing_source_scope_mapping | missing | 10 |
| blocked_out_of_scope_symbol_collision | OTC | 18 |
| blocked_out_of_scope_symbol_collision | US_LISTED | 9 |
| document_no_dataset_match | OTC | 6 |
| document_no_dataset_match | US_LISTED | 8 |
| review_duplicate_or_cross_listing | US_LISTED | 13 |
| review_verified_rename_or_delisting | US_LISTED | 5 |

## Workflow Queue By Source Confidence

| Queue | Source Confidence | Rows |
|---|---|---:|
| audit_already_reflected | secondary_review | 282 |
| blocked_missing_source_scope_mapping | secondary_review | 10 |
| blocked_out_of_scope_symbol_collision | secondary_review | 27 |
| document_no_dataset_match | secondary_review | 14 |
| review_duplicate_or_cross_listing | secondary_review | 13 |
| review_verified_rename_or_delisting | secondary_review | 5 |

## Workflow Queue By Review Strategy

| Queue | Strategy | Rows |
|---|---|---:|
| audit_already_reflected | audit_already_reflected_no_canonical_change | 282 |
| blocked_missing_source_scope_mapping | map_source_exchange_scope_before_symbol_review | 10 |
| blocked_out_of_scope_symbol_collision | block_until_source_scope_and_non_symbol_identity_resolved | 27 |
| document_no_dataset_match | document_no_dataset_match_without_canonical_action | 14 |
| review_duplicate_or_cross_listing | resolve_duplicate_cross_listing_or_transition_before_any_symbol_change | 13 |
| review_verified_rename_or_delisting | verify_rename_or_delisting_with_official_venue_or_issuer_evidence | 5 |

## Top Workflow Batches

| Queue | Priority | Recency | Scope status | Strategy | Evidence required | Recommended next source | Source gate | Rows |
|---|---|---|---|---|---|---|---|---:|
| review_duplicate_or_cross_listing | P1 | recent_7d | matches_within_source_scope | resolve_duplicate_cross_listing_or_transition_before_any_symbol_change | official_exchange_directory_plus_listing_key_review_to_distinguish_duplicate_cross_listing_or_transition | Official exchange directory records plus listing-key review for both symbols. | Do not change symbols until duplicate, cross-listing, or transition state is resolved listing-key by listing-key. | 3 |
| review_verified_rename_or_delisting | P1 | recent_7d | matches_within_source_scope | verify_rename_or_delisting_with_official_venue_or_issuer_evidence | official_exchange_notice_or_current_directory_showing_old_symbol_inactive_new_symbol_active_same_issuer | Official exchange notice, issuer notice, or current exchange directory proving old/new symbols for the same issuer. | Do not rename until official listing-keyed evidence proves old inactive and new active for the same issuer. | 3 |
| review_duplicate_or_cross_listing | P1 | recent_30d | matches_within_source_scope | resolve_duplicate_cross_listing_or_transition_before_any_symbol_change | official_exchange_directory_plus_listing_key_review_to_distinguish_duplicate_cross_listing_or_transition | Official exchange directory records plus listing-key review for both symbols. | Do not change symbols until duplicate, cross-listing, or transition state is resolved listing-key by listing-key. | 3 |
| review_verified_rename_or_delisting | P1 | recent_90d | matches_within_source_scope | verify_rename_or_delisting_with_official_venue_or_issuer_evidence | official_exchange_notice_or_current_directory_showing_old_symbol_inactive_new_symbol_active_same_issuer | Official exchange notice, issuer notice, or current exchange directory proving old/new symbols for the same issuer. | Do not rename until official listing-keyed evidence proves old inactive and new active for the same issuer. | 2 |
| review_duplicate_or_cross_listing | P1 | recent_90d | matches_within_source_scope | resolve_duplicate_cross_listing_or_transition_before_any_symbol_change | official_exchange_directory_plus_listing_key_review_to_distinguish_duplicate_cross_listing_or_transition | Official exchange directory records plus listing-key review for both symbols. | Do not change symbols until duplicate, cross-listing, or transition state is resolved listing-key by listing-key. | 1 |
| review_duplicate_or_cross_listing | P1 | older_than_90d | matches_within_source_scope | resolve_duplicate_cross_listing_or_transition_before_any_symbol_change | official_exchange_directory_plus_listing_key_review_to_distinguish_duplicate_cross_listing_or_transition | Official exchange directory records plus listing-key review for both symbols. | Do not change symbols until duplicate, cross-listing, or transition state is resolved listing-key by listing-key. | 6 |
| blocked_out_of_scope_symbol_collision | P2 | recent_7d | global_symbol_collision_outside_source_scope | block_until_source_scope_and_non_symbol_identity_resolved | official_exchange_scope_and_non_symbol_identity_evidence_before_apply | Official source exchange scope mapping plus non-symbol identity evidence before any symbol action. | Block apply; global symbol collision outside source scope is not symbol-change evidence. | 6 |
| blocked_missing_source_scope_mapping | P2 | recent_7d | unscoped_source_hint | map_source_exchange_scope_before_symbol_review | source_exchange_mapping_before_any_symbol_change_review | Documented source-to-exchange scope mapping before symbol-change review. | Block review until the secondary feed event is mapped to an exchange scope. | 1 |
| blocked_out_of_scope_symbol_collision | P2 | recent_30d | global_symbol_collision_outside_source_scope | block_until_source_scope_and_non_symbol_identity_resolved | official_exchange_scope_and_non_symbol_identity_evidence_before_apply | Official source exchange scope mapping plus non-symbol identity evidence before any symbol action. | Block apply; global symbol collision outside source scope is not symbol-change evidence. | 4 |
| blocked_missing_source_scope_mapping | P2 | recent_30d | unscoped_source_hint | map_source_exchange_scope_before_symbol_review | source_exchange_mapping_before_any_symbol_change_review | Documented source-to-exchange scope mapping before symbol-change review. | Block review until the secondary feed event is mapped to an exchange scope. | 1 |
| blocked_out_of_scope_symbol_collision | P2 | recent_90d | global_symbol_collision_outside_source_scope | block_until_source_scope_and_non_symbol_identity_resolved | official_exchange_scope_and_non_symbol_identity_evidence_before_apply | Official source exchange scope mapping plus non-symbol identity evidence before any symbol action. | Block apply; global symbol collision outside source scope is not symbol-change evidence. | 4 |
| blocked_out_of_scope_symbol_collision | P2 | older_than_90d | global_symbol_collision_outside_source_scope | block_until_source_scope_and_non_symbol_identity_resolved | official_exchange_scope_and_non_symbol_identity_evidence_before_apply | Official source exchange scope mapping plus non-symbol identity evidence before any symbol action. | Block apply; global symbol collision outside source scope is not symbol-change evidence. | 13 |
| blocked_missing_source_scope_mapping | P2 | older_than_90d | unscoped_source_hint | map_source_exchange_scope_before_symbol_review | source_exchange_mapping_before_any_symbol_change_review | Documented source-to-exchange scope mapping before symbol-change review. | Block review until the secondary feed event is mapped to an exchange scope. | 8 |
| document_no_dataset_match | P3 | recent_7d | matches_within_source_scope | document_no_dataset_match_without_canonical_action | official_exchange_scope_mapping_or_ignore_as_external_non_dataset_event | Official exchange scope mapping, or document the event as outside the dataset. | No dataset action without scoped official mapping to an existing or intended listing. | 3 |
| document_no_dataset_match | P3 | recent_90d | matches_within_source_scope | document_no_dataset_match_without_canonical_action | official_exchange_scope_mapping_or_ignore_as_external_non_dataset_event | Official exchange scope mapping, or document the event as outside the dataset. | No dataset action without scoped official mapping to an existing or intended listing. | 2 |
| document_no_dataset_match | P3 | older_than_90d | matches_within_source_scope | document_no_dataset_match_without_canonical_action | official_exchange_scope_mapping_or_ignore_as_external_non_dataset_event | Official exchange scope mapping, or document the event as outside the dataset. | No dataset action without scoped official mapping to an existing or intended listing. | 9 |
| audit_already_reflected | P4 | recent_7d | matches_within_source_scope | audit_already_reflected_no_canonical_change | audit_only_confirm_no_canonical_change_needed | Audit-only confirmation from scoped listing records; no canonical change. | Audit only; no ticker, listing, or name change is authorized. | 24 |
| audit_already_reflected | P4 | recent_7d | global_symbol_collision_outside_source_scope | audit_already_reflected_no_canonical_change | audit_only_confirm_no_canonical_change_needed | Audit-only comparison against official scoped exchange evidence; no canonical change. | Audit only; no ticker, listing, or name change is authorized. | 11 |
| audit_already_reflected | P4 | recent_30d | matches_within_source_scope | audit_already_reflected_no_canonical_change | audit_only_confirm_no_canonical_change_needed | Audit-only confirmation from scoped listing records; no canonical change. | Audit only; no ticker, listing, or name change is authorized. | 27 |
| audit_already_reflected | P4 | recent_30d | global_symbol_collision_outside_source_scope | audit_already_reflected_no_canonical_change | audit_only_confirm_no_canonical_change_needed | Audit-only comparison against official scoped exchange evidence; no canonical change. | Audit only; no ticker, listing, or name change is authorized. | 3 |
| audit_already_reflected | P4 | recent_90d | matches_within_source_scope | audit_already_reflected_no_canonical_change | audit_only_confirm_no_canonical_change_needed | Audit-only confirmation from scoped listing records; no canonical change. | Audit only; no ticker, listing, or name change is authorized. | 25 |
| audit_already_reflected | P4 | recent_90d | global_symbol_collision_outside_source_scope | audit_already_reflected_no_canonical_change | audit_only_confirm_no_canonical_change_needed | Audit-only comparison against official scoped exchange evidence; no canonical change. | Audit only; no ticker, listing, or name change is authorized. | 6 |
| audit_already_reflected | P4 | older_than_90d | matches_within_source_scope | audit_already_reflected_no_canonical_change | audit_only_confirm_no_canonical_change_needed | Audit-only confirmation from scoped listing records; no canonical change. | Audit only; no ticker, listing, or name change is authorized. | 142 |
| audit_already_reflected | P4 | older_than_90d | global_symbol_collision_outside_source_scope | audit_already_reflected_no_canonical_change | audit_only_confirm_no_canonical_change_needed | Audit-only comparison against official scoped exchange evidence; no canonical change. | Audit only; no ticker, listing, or name change is authorized. | 44 |

## Review Buckets

| Priority | Bucket | Rows |
|---|---|---:|
| P1 | action_required_duplicate_or_cross_listing | 13 |
| P1 | action_required_possible_rename_or_delisting | 5 |
| P4 | already_reflected_in_scope_with_global_symbol_collision | 64 |
| P4 | already_reflected_in_source_scope | 218 |
| P2 | hold_out_of_scope_symbol_collision | 26 |
| P2 | manual_review_due_to_out_of_scope_collision | 1 |
| P2 | manual_scope_mapping_required | 10 |
| P3 | no_dataset_match_for_source_scope | 14 |

## Review Priorities

| Priority | Rows |
|---|---:|
| P1 | 18 |
| P2 | 37 |
| P3 | 14 |
| P4 | 282 |

## Recency

| Recency bucket | Rows |
|---|---:|
| older_than_90d | 222 |
| recent_30d | 38 |
| recent_7d | 51 |
| recent_90d | 40 |

## Time-Sensitive P1 Review

| Workflow queue | Rows |
|---|---:|
| review_duplicate_or_cross_listing | 6 |
| review_verified_rename_or_delisting | 3 |

| Recency bucket | Rows |
|---|---:|
| recent_30d | 3 |
| recent_7d | 6 |

### Top Time-Sensitive Symbol-Change Batches

| Queue | Recency | Scope status | Match status | Listing-key status | Strategy | Evidence required | Source gate | Rows |
|---|---|---|---|---|---|---|---|---:|
| review_duplicate_or_cross_listing | recent_7d | matches_within_source_scope | old_and_new_symbols_present | old_and_new_scoped_listing_keys_present | resolve_duplicate_cross_listing_or_transition_before_any_symbol_change | official_exchange_directory_plus_listing_key_review_to_distinguish_duplicate_cross_listing_or_transition | Do not change symbols until duplicate, cross-listing, or transition state is resolved listing-key by listing-key. | 3 |
| review_verified_rename_or_delisting | recent_7d | matches_within_source_scope | old_symbol_present_new_symbol_missing | old_scoped_listing_key_only | verify_rename_or_delisting_with_official_venue_or_issuer_evidence | official_exchange_notice_or_current_directory_showing_old_symbol_inactive_new_symbol_active_same_issuer | Do not rename until official listing-keyed evidence proves old inactive and new active for the same issuer. | 3 |
| review_duplicate_or_cross_listing | recent_30d | matches_within_source_scope | old_and_new_symbols_present | old_and_new_scoped_listing_keys_present | resolve_duplicate_cross_listing_or_transition_before_any_symbol_change | official_exchange_directory_plus_listing_key_review_to_distinguish_duplicate_cross_listing_or_transition | Do not change symbols until duplicate, cross-listing, or transition state is resolved listing-key by listing-key. | 3 |

## Priority By Recency

| Priority / Recency | Rows |
|---|---:|
| P1:older_than_90d | 6 |
| P1:recent_30d | 3 |
| P1:recent_7d | 6 |
| P1:recent_90d | 3 |
| P2:older_than_90d | 21 |
| P2:recent_30d | 5 |
| P2:recent_7d | 7 |
| P2:recent_90d | 4 |
| P3:older_than_90d | 9 |
| P3:recent_7d | 3 |
| P3:recent_90d | 2 |
| P4:older_than_90d | 186 |
| P4:recent_30d | 30 |
| P4:recent_7d | 35 |
| P4:recent_90d | 31 |

## Apply Eligibility

| Eligibility | Rows |
|---|---:|
| audit_only_no_apply | 282 |
| blocked_until_exchange_scope_resolved | 37 |
| no_dataset_action_without_scope_mapping | 14 |
| requires_official_venue_confirmation | 18 |

## Apply Readiness

| Readiness | Rows |
|---|---:|
| audit_only_no_canonical_change | 282 |
| blocked_until_listing_keyed_official_symbol_change_evidence | 18 |
| blocked_until_source_exchange_scope_and_non_symbol_identity_evidence | 37 |
| document_or_ignore_until_scoped_official_dataset_match | 14 |

## Time-Sensitive Apply Readiness

| Readiness | Rows |
|---|---:|
| blocked_until_listing_keyed_official_symbol_change_evidence | 9 |

## Verification Evidence

| Evidence Gate | Rows |
|---|---:|
| audit_only_confirm_no_canonical_change_needed | 282 |
| official_exchange_directory_plus_listing_key_review_to_distinguish_duplicate_cross_listing_or_transition | 13 |
| official_exchange_notice_or_current_directory_showing_old_symbol_inactive_new_symbol_active_same_issuer | 5 |
| official_exchange_scope_and_non_symbol_identity_evidence_before_apply | 27 |
| official_exchange_scope_mapping_or_ignore_as_external_non_dataset_event | 14 |
| source_exchange_mapping_before_any_symbol_change_review | 10 |

## Recommended Actions

| Action | Rows |
|---|---:|
| already_reflected_or_new_symbol_added_in_source_scope | 285 |
| do_not_apply_from_symbol_match_review_exchange_scope_first | 26 |
| ignore_or_map_exchange_scope_before_applying | 15 |
| review_duplicate_or_cross_listing_state_in_source_scope | 17 |
| review_possible_rename_or_delisting_in_source_scope | 8 |

## Exchange Scope

| Scope Status | Rows |
|---|---:|
| global_symbol_collision_outside_source_scope | 91 |
| matches_within_source_scope | 250 |
| unscoped_source_hint | 10 |

## Policy

- `source_confidence=secondary_review`: do not auto-merge as official exchange data.
- `review_needed=true`: apply only after exchange/listing-key validation.
- `review_priority=P1`: start here; these are in-scope rename/delisting or duplicate/cross-listing candidates, still not automatic updates.
- `review_priority=P4`: generally already reflected; keep only as audit evidence unless an official venue source contradicts it.
- StockAnalysis is used as a broad daily change detector; venue-specific official feeds should override it when available.

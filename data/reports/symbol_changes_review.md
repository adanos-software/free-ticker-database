# Symbol Changes Review

Generated at: `2026-06-18T10:46:17Z`

Daily secondary-source symbol-change feed. Rows are review signals, not automatic canonical ticker updates.

## Summary

| Metric | Rows |
|---|---:|
| Fetched rows | 239 |
| Merged history rows | 282 |
| Review rows | 282 |
| Direct symbol-change apply allowed rows | 0 |

## Symbol-Change Backlog

- Status: `listing_keyed_symbol_change_review_queue_open`
- Rows: `282`
- Rename/delisting review rows: `14`
- Duplicate/cross-listing review rows: `14`
- Already reflected audit rows: `201`
- Out-of-scope collision blocked rows: `31`
- Missing source-scope mapping rows: `3`
- No-dataset-match documentation rows: `19`
- Time-sensitive review rows: `8`
- Secondary feed apply authorized: `false`
- Source gate: Symbol-change feed rows are review signals only; ticker, name, listing, or alias changes require listing-keyed official venue or issuer evidence for old/new symbols and issuer identity.

## Match Status

| Status | Rows |
|---|---:|
| new_symbol_present_old_symbol_missing | 202 |
| no_matching_listing | 20 |
| old_and_new_symbols_present | 14 |
| old_symbol_present_new_symbol_missing | 24 |
| symbol_present_only_outside_source_scope | 22 |

## Workflow Queues

| Queue | Rows |
|---|---:|
| audit_already_reflected | 201 |
| blocked_missing_source_scope_mapping | 3 |
| blocked_out_of_scope_symbol_collision | 31 |
| document_no_dataset_match | 19 |
| review_duplicate_or_cross_listing | 14 |
| review_verified_rename_or_delisting | 14 |

## Workflow Queue By Recency

| Queue / Recency | Rows |
|---|---:|
| audit_already_reflected:older_than_90d | 173 |
| audit_already_reflected:recent_30d | 12 |
| audit_already_reflected:recent_7d | 2 |
| audit_already_reflected:recent_90d | 14 |
| blocked_missing_source_scope_mapping:older_than_90d | 3 |
| blocked_out_of_scope_symbol_collision:older_than_90d | 15 |
| blocked_out_of_scope_symbol_collision:recent_30d | 4 |
| blocked_out_of_scope_symbol_collision:recent_7d | 3 |
| blocked_out_of_scope_symbol_collision:recent_90d | 9 |
| document_no_dataset_match:older_than_90d | 16 |
| document_no_dataset_match:recent_90d | 3 |
| review_duplicate_or_cross_listing:older_than_90d | 9 |
| review_duplicate_or_cross_listing:recent_90d | 5 |
| review_verified_rename_or_delisting:recent_30d | 6 |
| review_verified_rename_or_delisting:recent_7d | 2 |
| review_verified_rename_or_delisting:recent_90d | 6 |

## Workflow Queue By Priority

| Queue / Priority | Rows |
|---|---:|
| audit_already_reflected:P4 | 201 |
| blocked_missing_source_scope_mapping:P2 | 3 |
| blocked_out_of_scope_symbol_collision:P2 | 31 |
| document_no_dataset_match:P3 | 19 |
| review_duplicate_or_cross_listing:P1 | 14 |
| review_verified_rename_or_delisting:P1 | 14 |

## Workflow Queue By Exchange Scope

| Queue / Scope Status | Rows |
|---|---:|
| audit_already_reflected:global_symbol_collision_outside_source_scope | 34 |
| audit_already_reflected:matches_within_source_scope | 167 |
| blocked_missing_source_scope_mapping:unscoped_source_hint | 3 |
| blocked_out_of_scope_symbol_collision:global_symbol_collision_outside_source_scope | 31 |
| document_no_dataset_match:matches_within_source_scope | 19 |
| review_duplicate_or_cross_listing:matches_within_source_scope | 14 |
| review_verified_rename_or_delisting:matches_within_source_scope | 14 |

## Workflow Queue By Match Status

| Queue / Match Status | Rows |
|---|---:|
| audit_already_reflected:new_symbol_present_old_symbol_missing | 201 |
| blocked_missing_source_scope_mapping:new_symbol_present_old_symbol_missing | 1 |
| blocked_missing_source_scope_mapping:no_matching_listing | 1 |
| blocked_missing_source_scope_mapping:old_symbol_present_new_symbol_missing | 1 |
| blocked_out_of_scope_symbol_collision:old_symbol_present_new_symbol_missing | 9 |
| blocked_out_of_scope_symbol_collision:symbol_present_only_outside_source_scope | 22 |
| document_no_dataset_match:no_matching_listing | 19 |
| review_duplicate_or_cross_listing:old_and_new_symbols_present | 14 |
| review_verified_rename_or_delisting:old_symbol_present_new_symbol_missing | 14 |

## Workflow Queue By Listing-Key Review

| Queue | Listing-Key Status | Rows |
|---|---|---:|
| audit_already_reflected | new_scoped_listing_key_only | 201 |
| blocked_missing_source_scope_mapping | new_scoped_listing_key_only | 1 |
| blocked_missing_source_scope_mapping | no_scoped_listing_key_match | 1 |
| blocked_missing_source_scope_mapping | old_scoped_listing_key_only | 1 |
| blocked_out_of_scope_symbol_collision | no_scoped_listing_key_match | 22 |
| blocked_out_of_scope_symbol_collision | old_scoped_listing_key_only | 9 |
| document_no_dataset_match | no_scoped_listing_key_match | 19 |
| review_duplicate_or_cross_listing | old_and_new_scoped_listing_keys_present | 14 |
| review_verified_rename_or_delisting | old_scoped_listing_key_only | 14 |

## Workflow Queue By Source Hint

| Queue | Source Hint | Rows |
|---|---|---:|
| audit_already_reflected | OTC | 13 |
| audit_already_reflected | US_LISTED | 188 |
| blocked_missing_source_scope_mapping | missing | 3 |
| blocked_out_of_scope_symbol_collision | OTC | 12 |
| blocked_out_of_scope_symbol_collision | US_LISTED | 19 |
| document_no_dataset_match | OTC | 5 |
| document_no_dataset_match | US_LISTED | 14 |
| review_duplicate_or_cross_listing | US_LISTED | 14 |
| review_verified_rename_or_delisting | US_LISTED | 14 |

## Workflow Queue By Source Confidence

| Queue | Source Confidence | Rows |
|---|---|---:|
| audit_already_reflected | secondary_review | 201 |
| blocked_missing_source_scope_mapping | secondary_review | 3 |
| blocked_out_of_scope_symbol_collision | secondary_review | 31 |
| document_no_dataset_match | secondary_review | 19 |
| review_duplicate_or_cross_listing | secondary_review | 14 |
| review_verified_rename_or_delisting | secondary_review | 14 |

## Workflow Queue By Review Strategy

| Queue | Strategy | Rows |
|---|---|---:|
| audit_already_reflected | audit_already_reflected_no_canonical_change | 201 |
| blocked_missing_source_scope_mapping | map_source_exchange_scope_before_symbol_review | 3 |
| blocked_out_of_scope_symbol_collision | block_until_source_scope_and_non_symbol_identity_resolved | 31 |
| document_no_dataset_match | document_no_dataset_match_without_canonical_action | 19 |
| review_duplicate_or_cross_listing | resolve_duplicate_cross_listing_or_transition_before_any_symbol_change | 14 |
| review_verified_rename_or_delisting | verify_rename_or_delisting_with_official_venue_or_issuer_evidence | 14 |

## Top Workflow Batches

| Queue | Priority | Recency | Scope status | Strategy | Evidence required | Recommended next source | Source gate | Rows |
|---|---|---|---|---|---|---|---|---:|
| review_verified_rename_or_delisting | P1 | recent_7d | matches_within_source_scope | verify_rename_or_delisting_with_official_venue_or_issuer_evidence | official_exchange_notice_or_current_directory_showing_old_symbol_inactive_new_symbol_active_same_issuer | Official exchange notice, issuer notice, or current exchange directory proving old/new symbols for the same issuer. | Do not rename until official listing-keyed evidence proves old inactive and new active for the same issuer. | 2 |
| review_verified_rename_or_delisting | P1 | recent_30d | matches_within_source_scope | verify_rename_or_delisting_with_official_venue_or_issuer_evidence | official_exchange_notice_or_current_directory_showing_old_symbol_inactive_new_symbol_active_same_issuer | Official exchange notice, issuer notice, or current exchange directory proving old/new symbols for the same issuer. | Do not rename until official listing-keyed evidence proves old inactive and new active for the same issuer. | 6 |
| review_verified_rename_or_delisting | P1 | recent_90d | matches_within_source_scope | verify_rename_or_delisting_with_official_venue_or_issuer_evidence | official_exchange_notice_or_current_directory_showing_old_symbol_inactive_new_symbol_active_same_issuer | Official exchange notice, issuer notice, or current exchange directory proving old/new symbols for the same issuer. | Do not rename until official listing-keyed evidence proves old inactive and new active for the same issuer. | 6 |
| review_duplicate_or_cross_listing | P1 | recent_90d | matches_within_source_scope | resolve_duplicate_cross_listing_or_transition_before_any_symbol_change | official_exchange_directory_plus_listing_key_review_to_distinguish_duplicate_cross_listing_or_transition | Official exchange directory records plus listing-key review for both symbols. | Do not change symbols until duplicate, cross-listing, or transition state is resolved listing-key by listing-key. | 5 |
| review_duplicate_or_cross_listing | P1 | older_than_90d | matches_within_source_scope | resolve_duplicate_cross_listing_or_transition_before_any_symbol_change | official_exchange_directory_plus_listing_key_review_to_distinguish_duplicate_cross_listing_or_transition | Official exchange directory records plus listing-key review for both symbols. | Do not change symbols until duplicate, cross-listing, or transition state is resolved listing-key by listing-key. | 9 |
| blocked_out_of_scope_symbol_collision | P2 | recent_7d | global_symbol_collision_outside_source_scope | block_until_source_scope_and_non_symbol_identity_resolved | official_exchange_scope_and_non_symbol_identity_evidence_before_apply | Official source exchange scope mapping plus non-symbol identity evidence before any symbol action. | Block apply; global symbol collision outside source scope is not symbol-change evidence. | 3 |
| blocked_out_of_scope_symbol_collision | P2 | recent_30d | global_symbol_collision_outside_source_scope | block_until_source_scope_and_non_symbol_identity_resolved | official_exchange_scope_and_non_symbol_identity_evidence_before_apply | Official source exchange scope mapping plus non-symbol identity evidence before any symbol action. | Block apply; global symbol collision outside source scope is not symbol-change evidence. | 4 |
| blocked_out_of_scope_symbol_collision | P2 | recent_90d | global_symbol_collision_outside_source_scope | block_until_source_scope_and_non_symbol_identity_resolved | official_exchange_scope_and_non_symbol_identity_evidence_before_apply | Official source exchange scope mapping plus non-symbol identity evidence before any symbol action. | Block apply; global symbol collision outside source scope is not symbol-change evidence. | 9 |
| blocked_out_of_scope_symbol_collision | P2 | older_than_90d | global_symbol_collision_outside_source_scope | block_until_source_scope_and_non_symbol_identity_resolved | official_exchange_scope_and_non_symbol_identity_evidence_before_apply | Official source exchange scope mapping plus non-symbol identity evidence before any symbol action. | Block apply; global symbol collision outside source scope is not symbol-change evidence. | 15 |
| blocked_missing_source_scope_mapping | P2 | older_than_90d | unscoped_source_hint | map_source_exchange_scope_before_symbol_review | source_exchange_mapping_before_any_symbol_change_review | Documented source-to-exchange scope mapping before symbol-change review. | Block review until the secondary feed event is mapped to an exchange scope. | 3 |
| document_no_dataset_match | P3 | recent_90d | matches_within_source_scope | document_no_dataset_match_without_canonical_action | official_exchange_scope_mapping_or_ignore_as_external_non_dataset_event | Official exchange scope mapping, or document the event as outside the dataset. | No dataset action without scoped official mapping to an existing or intended listing. | 3 |
| document_no_dataset_match | P3 | older_than_90d | matches_within_source_scope | document_no_dataset_match_without_canonical_action | official_exchange_scope_mapping_or_ignore_as_external_non_dataset_event | Official exchange scope mapping, or document the event as outside the dataset. | No dataset action without scoped official mapping to an existing or intended listing. | 16 |
| audit_already_reflected | P4 | recent_7d | matches_within_source_scope | audit_already_reflected_no_canonical_change | audit_only_confirm_no_canonical_change_needed | Audit-only confirmation from scoped listing records; no canonical change. | Audit only; no ticker, listing, or name change is authorized. | 2 |
| audit_already_reflected | P4 | recent_30d | matches_within_source_scope | audit_already_reflected_no_canonical_change | audit_only_confirm_no_canonical_change_needed | Audit-only confirmation from scoped listing records; no canonical change. | Audit only; no ticker, listing, or name change is authorized. | 12 |
| audit_already_reflected | P4 | recent_90d | matches_within_source_scope | audit_already_reflected_no_canonical_change | audit_only_confirm_no_canonical_change_needed | Audit-only confirmation from scoped listing records; no canonical change. | Audit only; no ticker, listing, or name change is authorized. | 14 |
| audit_already_reflected | P4 | older_than_90d | matches_within_source_scope | audit_already_reflected_no_canonical_change | audit_only_confirm_no_canonical_change_needed | Audit-only confirmation from scoped listing records; no canonical change. | Audit only; no ticker, listing, or name change is authorized. | 139 |
| audit_already_reflected | P4 | older_than_90d | global_symbol_collision_outside_source_scope | audit_already_reflected_no_canonical_change | audit_only_confirm_no_canonical_change_needed | Audit-only comparison against official scoped exchange evidence; no canonical change. | Audit only; no ticker, listing, or name change is authorized. | 34 |

## Review Buckets

| Priority | Bucket | Rows |
|---|---|---:|
| P1 | action_required_duplicate_or_cross_listing | 14 |
| P1 | action_required_possible_rename_or_delisting | 14 |
| P4 | already_reflected_in_scope_with_global_symbol_collision | 34 |
| P4 | already_reflected_in_source_scope | 167 |
| P2 | hold_out_of_scope_symbol_collision | 22 |
| P2 | manual_review_due_to_out_of_scope_collision | 9 |
| P2 | manual_scope_mapping_required | 3 |
| P3 | no_dataset_match_for_source_scope | 19 |

## Review Priorities

| Priority | Rows |
|---|---:|
| P1 | 28 |
| P2 | 34 |
| P3 | 19 |
| P4 | 201 |

## Recency

| Recency bucket | Rows |
|---|---:|
| older_than_90d | 216 |
| recent_30d | 22 |
| recent_7d | 7 |
| recent_90d | 37 |

## Time-Sensitive P1 Review

| Workflow queue | Rows |
|---|---:|
| review_verified_rename_or_delisting | 8 |

| Recency bucket | Rows |
|---|---:|
| recent_30d | 6 |
| recent_7d | 2 |

### Top Time-Sensitive Symbol-Change Batches

| Queue | Recency | Scope status | Match status | Listing-key status | Strategy | Evidence required | Source gate | Rows |
|---|---|---|---|---|---|---|---|---:|
| review_verified_rename_or_delisting | recent_7d | matches_within_source_scope | old_symbol_present_new_symbol_missing | old_scoped_listing_key_only | verify_rename_or_delisting_with_official_venue_or_issuer_evidence | official_exchange_notice_or_current_directory_showing_old_symbol_inactive_new_symbol_active_same_issuer | Do not rename until official listing-keyed evidence proves old inactive and new active for the same issuer. | 2 |
| review_verified_rename_or_delisting | recent_30d | matches_within_source_scope | old_symbol_present_new_symbol_missing | old_scoped_listing_key_only | verify_rename_or_delisting_with_official_venue_or_issuer_evidence | official_exchange_notice_or_current_directory_showing_old_symbol_inactive_new_symbol_active_same_issuer | Do not rename until official listing-keyed evidence proves old inactive and new active for the same issuer. | 6 |

## Priority By Recency

| Priority / Recency | Rows |
|---|---:|
| P1:older_than_90d | 9 |
| P1:recent_30d | 6 |
| P1:recent_7d | 2 |
| P1:recent_90d | 11 |
| P2:older_than_90d | 18 |
| P2:recent_30d | 4 |
| P2:recent_7d | 3 |
| P2:recent_90d | 9 |
| P3:older_than_90d | 16 |
| P3:recent_90d | 3 |
| P4:older_than_90d | 173 |
| P4:recent_30d | 12 |
| P4:recent_7d | 2 |
| P4:recent_90d | 14 |

## Apply Eligibility

| Eligibility | Rows |
|---|---:|
| audit_only_no_apply | 201 |
| blocked_until_exchange_scope_resolved | 34 |
| no_dataset_action_without_scope_mapping | 19 |
| requires_official_venue_confirmation | 28 |

## Apply Readiness

| Readiness | Rows |
|---|---:|
| audit_only_no_canonical_change | 201 |
| blocked_until_listing_keyed_official_symbol_change_evidence | 28 |
| blocked_until_source_exchange_scope_and_non_symbol_identity_evidence | 34 |
| document_or_ignore_until_scoped_official_dataset_match | 19 |

## Time-Sensitive Apply Readiness

| Readiness | Rows |
|---|---:|
| blocked_until_listing_keyed_official_symbol_change_evidence | 8 |

## Verification Evidence

| Evidence Gate | Rows |
|---|---:|
| audit_only_confirm_no_canonical_change_needed | 201 |
| official_exchange_directory_plus_listing_key_review_to_distinguish_duplicate_cross_listing_or_transition | 14 |
| official_exchange_notice_or_current_directory_showing_old_symbol_inactive_new_symbol_active_same_issuer | 14 |
| official_exchange_scope_and_non_symbol_identity_evidence_before_apply | 31 |
| official_exchange_scope_mapping_or_ignore_as_external_non_dataset_event | 19 |
| source_exchange_mapping_before_any_symbol_change_review | 3 |

## Recommended Actions

| Action | Rows |
|---|---:|
| already_reflected_or_new_symbol_added_in_source_scope | 202 |
| do_not_apply_from_symbol_match_review_exchange_scope_first | 22 |
| ignore_or_map_exchange_scope_before_applying | 20 |
| review_duplicate_or_cross_listing_state_in_source_scope | 14 |
| review_possible_rename_or_delisting_in_source_scope | 24 |

## Exchange Scope

| Scope Status | Rows |
|---|---:|
| global_symbol_collision_outside_source_scope | 65 |
| matches_within_source_scope | 214 |
| unscoped_source_hint | 3 |

## Policy

- `source_confidence=secondary_review`: do not auto-merge as official exchange data.
- `review_needed=true`: apply only after exchange/listing-key validation.
- `review_priority=P1`: start here; these are in-scope rename/delisting or duplicate/cross-listing candidates, still not automatic updates.
- `review_priority=P4`: generally already reflected; keep only as audit evidence unless an official venue source contradicts it.
- StockAnalysis is used as a broad daily change detector; venue-specific official feeds should override it when available.

# FinancialData ISIN Supplements

Generated at: `2026-05-25T06:00:12Z`

FinancialData rows are used only as discovery signals. Accepted supplement rows require an official active masterfile row, a valid ISIN, issuer-name gate, no existing global ticker, and no existing/selected ISIN.

## Summary

| Metric | Value |
|---|---:|
| Input rows | 665 |
| Accepted supplement rows | 557 |

## Accepted By Exchange

| Exchange | Rows |
|---|---:|
| NSE_IN | 483 |
| HKEX | 39 |
| Bursa | 10 |
| KRX | 8 |
| BSE_IN | 7 |
| LSE | 7 |
| B3 | 3 |

## Decisions

| Decision | Rows |
|---|---:|
| preserve | 43 |
| reject | 426 |
| skip | 196 |

## Reasons

| Reason | Rows |
|---|---:|
| already_in_financialdata_supplement | 43 |
| ambiguous_official_isin_candidates | 163 |
| exchange_not_allowed_for_isin_supplement | 91 |
| isin_already_exists_in_database | 16 |
| no_name_gated_official_isin_match | 172 |
| official_listing_key_already_exists | 33 |
| ticker_already_exists_globally | 147 |

## Apply Eligibility

| Eligibility | Rows |
|---|---:|
| blocked_until_exchange_scope_explicitly_allowed | 91 |
| blocked_until_unique_official_isin_candidate_resolved | 163 |
| keep_absent_until_name_gated_official_isin_match | 172 |
| no_supplement_apply_existing_identifier_or_collision_guard | 196 |
| preserve_existing_reviewed_supplement_no_new_apply | 43 |

## Verification Evidence

| Evidence Required | Rows |
|---|---:|
| existing_database_isin_confirms_no_supplement_needed_or_cross_listing_review | 16 |
| existing_listing_key_confirms_no_supplement_needed | 33 |
| existing_reviewed_supplement_retained_with_original_official_source | 43 |
| explicit_exchange_scope_decision_before_financialdata_discovery_use | 91 |
| identity_resolution_before_any_global_ticker_reuse | 147 |
| official_active_masterfile_or_registry_row_matching_financialdata_name_and_listing | 172 |
| single_official_active_listing_with_valid_isin_and_name_gate | 163 |

## Top Review Batches

| Priority | Queue | Decision | Reason | Exchange | Scope | Official Source | Rows | Strategy | Eligibility | Evidence Required | Recommended Next Source | Source Gate |
|---|---|---|---|---|---|---|---:|---|---|---|---|---|
| P2 | resolve_ambiguous_official_isin_candidates | reject | ambiguous_official_isin_candidates | BSE_IN | current_exchange_gap | bse_india_scrips | 163 | resolve_to_single_official_active_isin_candidate_before_apply | blocked_until_unique_official_isin_candidate_resolved | single_official_active_listing_with_valid_isin_and_name_gate | Single official active listing or registry row resolving the ambiguous candidates with exact name and ISIN. | Do not write supplement until exactly one official valid-ISIN candidate remains after name/listing review. |
| P3 | collision_guard_no_supplement_apply | skip | ticker_already_exists_globally | BSE_IN | current_exchange_gap | bse_india_scrips | 95 | hold_existing_identifier_collision_until_identity_review_resolves | no_supplement_apply_existing_identifier_or_collision_guard | identity_resolution_before_any_global_ticker_reuse | Listing-keyed identity-resolution evidence before considering any global ticker reuse. | Do not reuse a global ticker without listing-keyed identity resolution. |
| P2 | review_exchange_scope_before_financialdata_use | reject | exchange_not_allowed_for_isin_supplement | SGX | global_expansion_candidate | missing_official_source | 60 | decide_exchange_scope_before_any_financialdata_discovery_apply | blocked_until_exchange_scope_explicitly_allowed | explicit_exchange_scope_decision_before_financialdata_discovery_use | Reviewed exchange-scope decision plus official exchange or registry source before using FinancialData discovery. | Do not use FinancialData discovery for this exchange until scope is explicitly reviewed and allowed. |
| P2 | keep_absent_until_official_name_gated_match | reject | no_name_gated_official_isin_match | NSE_IN | current_exchange_gap | missing_official_source | 49 | keep_absent_until_official_name_gated_identifier_evidence_exists | keep_absent_until_name_gated_official_isin_match | official_active_masterfile_or_registry_row_matching_financialdata_name_and_listing | Official active masterfile, registry, or issuer source matching FinancialData name and listing identity. | Keep absent until an official active source satisfies exact name/listing identity and ISIN gates. |
| P2 | keep_absent_until_official_name_gated_match | reject | no_name_gated_official_isin_match | HKEX | current_exchange_gap | missing_official_source | 40 | keep_absent_until_official_name_gated_identifier_evidence_exists | keep_absent_until_name_gated_official_isin_match | official_active_masterfile_or_registry_row_matching_financialdata_name_and_listing | Official active masterfile, registry, or issuer source matching FinancialData name and listing identity. | Keep absent until an official active source satisfies exact name/listing identity and ISIN gates. |
| P4 | preserve_existing_reviewed_supplement | preserve | already_in_financialdata_supplement | HKEX | current_exchange_gap | hkex_securities_list | 33 | preserve_existing_reviewed_supplement_no_new_apply | preserve_existing_reviewed_supplement_no_new_apply | existing_reviewed_supplement_retained_with_original_official_source | Existing reviewed FinancialData supplement source; re-check only if the original official source changes. | Preserve existing reviewed supplement; do not create a new row from FinancialData alone. |
| P2 | keep_absent_until_official_name_gated_match | reject | no_name_gated_official_isin_match | LSE | current_exchange_gap | missing_official_source | 29 | keep_absent_until_official_name_gated_identifier_evidence_exists | keep_absent_until_name_gated_official_isin_match | official_active_masterfile_or_registry_row_matching_financialdata_name_and_listing | Official active masterfile, registry, or issuer source matching FinancialData name and listing identity. | Keep absent until an official active source satisfies exact name/listing identity and ISIN gates. |
| P3 | collision_guard_no_supplement_apply | skip | official_listing_key_already_exists | HKEX | current_exchange_gap | hkex_securities_list | 29 | hold_existing_identifier_collision_until_identity_review_resolves | no_supplement_apply_existing_identifier_or_collision_guard | existing_listing_key_confirms_no_supplement_needed | Existing database listing-key evidence; no supplemental source is needed unless identity review reopens the row. | Do not create duplicate listing-key supplement rows. |
| P3 | collision_guard_no_supplement_apply | skip | ticker_already_exists_globally | LSE | current_exchange_gap | lse_price_explorer | 27 | hold_existing_identifier_collision_until_identity_review_resolves | no_supplement_apply_existing_identifier_or_collision_guard | identity_resolution_before_any_global_ticker_reuse | Listing-keyed identity-resolution evidence before considering any global ticker reuse. | Do not reuse a global ticker without listing-keyed identity resolution. |
| P2 | review_exchange_scope_before_financialdata_use | reject | exchange_not_allowed_for_isin_supplement | TSX | current_exchange_gap | missing_official_source | 17 | decide_exchange_scope_before_any_financialdata_discovery_apply | blocked_until_exchange_scope_explicitly_allowed | explicit_exchange_scope_decision_before_financialdata_discovery_use | Reviewed exchange-scope decision plus official exchange or registry source before using FinancialData discovery. | Do not use FinancialData discovery for this exchange until scope is explicitly reviewed and allowed. |

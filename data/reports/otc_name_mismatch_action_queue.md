# OTC Name Mismatch Action Queue

Generated: `2026-06-02T06:57:05Z`

Policy: this report does not change names or metadata. It groups OTC name mismatches into official-evidence review batches.

## Summary

| Metric | Value |
| --- | ---: |
| Batches | 13 |
| Underlying rows | 146 |
| Direct name changes authorized | False |
| Metadata enrichment authorized | False |

## Review Classes

| Review class | Rows |
| --- | ---: |
| probable_otc_rename_or_symbol_reuse | 65 |
| stale_or_symbol_reuse_without_isin | 7 |
| weak_abbreviation_or_truncation_review | 74 |

## DeepSeek Triage

| Triage | Rows |
| --- | ---: |
| deepseek_candidate_apply_blocked | 22 |
| deepseek_needs_official_evidence | 119 |
| deepseek_uncertain | 5 |

## Batches

| Priority | Review class | Source | ISIN | DeepSeek | Rows | Action | Evidence required |
| --- | --- | --- | --- | --- | ---: | --- | --- |
| critical | stale_or_symbol_reuse_without_isin | otc_markets_security_profile | without_isin | deepseek_candidate_apply_blocked | 4 | resolve_or_quarantine_before_trusting_otc_symbol | official_otc_profile_registry_or_issuer_history_source_matching_listing_key_before_name_or_quarantine_action |
| critical | stale_or_symbol_reuse_without_isin | otc_markets_security_profile | without_isin | deepseek_needs_official_evidence | 1 | resolve_or_quarantine_before_trusting_otc_symbol | official_otc_profile_registry_or_issuer_history_source_matching_listing_key_before_name_or_quarantine_action |
| critical | stale_or_symbol_reuse_without_isin | otc_markets_security_profile\|otc_markets_stock_screener | without_isin | deepseek_candidate_apply_blocked | 1 | resolve_or_quarantine_before_trusting_otc_symbol | official_otc_profile_registry_or_issuer_history_source_matching_listing_key_before_name_or_quarantine_action |
| critical | stale_or_symbol_reuse_without_isin | otc_markets_security_profile\|otc_markets_stock_screener | without_isin | deepseek_needs_official_evidence | 1 | resolve_or_quarantine_before_trusting_otc_symbol | official_otc_profile_registry_or_issuer_history_source_matching_listing_key_before_name_or_quarantine_action |
| high | probable_otc_rename_or_symbol_reuse | otc_markets_stock_screener | with_isin | deepseek_candidate_apply_blocked | 17 | verify_isin_anchored_issuer_history_before_name_change | official_or_reviewed_isin_bearing_source_matching_current_issuer_listing_key_and_name |
| high | probable_otc_rename_or_symbol_reuse | otc_markets_stock_screener | with_isin | deepseek_needs_official_evidence | 45 | verify_isin_anchored_issuer_history_before_name_change | official_or_reviewed_isin_bearing_source_matching_current_issuer_listing_key_and_name |
| high | probable_otc_rename_or_symbol_reuse | otc_markets_stock_screener\|sec_company_tickers_exchange | with_isin | deepseek_needs_official_evidence | 2 | verify_isin_anchored_issuer_history_before_name_change | official_or_reviewed_isin_bearing_source_matching_current_issuer_listing_key_and_name |
| high | probable_otc_rename_or_symbol_reuse | sec_company_tickers_exchange | with_isin | deepseek_needs_official_evidence | 1 | verify_isin_anchored_issuer_history_before_name_change | official_or_reviewed_isin_bearing_source_matching_current_issuer_listing_key_and_name |
| medium | weak_abbreviation_or_truncation_review | otc_markets_security_profile | without_isin | deepseek_needs_official_evidence | 1 | review_official_alias_before_matcher_tuning | official_alias_or_abbreviation_evidence_with_exact_listing_identity_match |
| medium | weak_abbreviation_or_truncation_review | otc_markets_stock_screener | with_isin | deepseek_needs_official_evidence | 64 | review_official_alias_before_matcher_tuning | official_alias_or_abbreviation_evidence_with_exact_listing_identity_match |
| medium | weak_abbreviation_or_truncation_review | otc_markets_stock_screener | with_isin | deepseek_uncertain | 5 | review_official_alias_before_matcher_tuning | official_alias_or_abbreviation_evidence_with_exact_listing_identity_match |
| medium | weak_abbreviation_or_truncation_review | otc_markets_stock_screener\|sec_company_tickers_exchange | with_isin | deepseek_needs_official_evidence | 2 | review_official_alias_before_matcher_tuning | official_alias_or_abbreviation_evidence_with_exact_listing_identity_match |
| medium | weak_abbreviation_or_truncation_review | sec_company_tickers_exchange | with_isin | deepseek_needs_official_evidence | 2 | review_official_alias_before_matcher_tuning | official_alias_or_abbreviation_evidence_with_exact_listing_identity_match |

## Gates

- Direct name changes authorized: `False`.
- DeepSeek triage does not authorize any data change.
- Symbol reuse and stale OTC names require official listing-keyed identity evidence or quarantine review.

# OTC Name Mismatch Action Queue

Generated: `2026-08-06T14:18:42Z`

Policy: this report does not change names or metadata. It groups OTC name mismatches into official-evidence review batches.

## Summary

| Metric | Value |
| --- | ---: |
| Batches | 14 |
| Underlying rows | 29 |
| Direct name changes authorized | False |
| Metadata enrichment authorized | False |

## Review Classes

| Review class | Rows |
| --- | ---: |
| hold_unresolved | 17 |
| probable_otc_rename_or_symbol_reuse | 7 |
| weak_abbreviation_or_truncation_review | 5 |

## DeepSeek Triage

| Triage | Rows |
| --- | ---: |
| deepseek_candidate_apply_blocked | 1 |
| deepseek_needs_official_evidence | 9 |
| not_triaged_by_deepseek | 19 |

## Batches

| Priority | Review class | Source | ISIN | DeepSeek | Rows | Action | Evidence required |
| --- | --- | --- | --- | --- | ---: | --- | --- |
| high | probable_otc_rename_or_symbol_reuse | otc_markets_stock_screener | with_isin | not_triaged_by_deepseek | 6 | verify_isin_anchored_issuer_history_before_name_change | official_or_reviewed_isin_bearing_source_matching_current_issuer_listing_key_and_name |
| high | probable_otc_rename_or_symbol_reuse | otc_markets_stock_screener\|sec_company_tickers_exchange | with_isin | not_triaged_by_deepseek | 1 | verify_isin_anchored_issuer_history_before_name_change | official_or_reviewed_isin_bearing_source_matching_current_issuer_listing_key_and_name |
| medium | weak_abbreviation_or_truncation_review | otc_markets_stock_screener | with_isin | not_triaged_by_deepseek | 4 | review_official_alias_before_matcher_tuning | official_alias_or_abbreviation_evidence_with_exact_listing_identity_match |
| medium | weak_abbreviation_or_truncation_review | otc_markets_stock_screener\|sec_company_tickers_exchange | with_isin | not_triaged_by_deepseek | 1 | review_official_alias_before_matcher_tuning | official_alias_or_abbreviation_evidence_with_exact_listing_identity_match |
| held | hold_unresolved | otc_markets_security_profile | with_isin | deepseek_candidate_apply_blocked | 1 | keep_current_until_stronger_issuer_history_source | stronger_official_or_reviewed_issuer_history_source_before_any_name_change |
| held | hold_unresolved | otc_markets_security_profile | without_isin | deepseek_needs_official_evidence | 1 | keep_current_until_stronger_issuer_history_source | stronger_official_or_reviewed_issuer_history_source_before_any_name_change |
| held | hold_unresolved | otc_markets_security_profile | without_isin | not_triaged_by_deepseek | 4 | keep_current_until_stronger_issuer_history_source | stronger_official_or_reviewed_issuer_history_source_before_any_name_change |
| held | hold_unresolved | otc_markets_security_profile\|otc_markets_stock_screener | with_isin | deepseek_needs_official_evidence | 1 | keep_current_until_stronger_issuer_history_source | stronger_official_or_reviewed_issuer_history_source_before_any_name_change |
| held | hold_unresolved | otc_markets_stock_screener | with_isin | deepseek_needs_official_evidence | 3 | keep_current_until_stronger_issuer_history_source | stronger_official_or_reviewed_issuer_history_source_before_any_name_change |
| held | hold_unresolved | otc_markets_stock_screener | with_isin | not_triaged_by_deepseek | 1 | keep_current_until_stronger_issuer_history_source | stronger_official_or_reviewed_issuer_history_source_before_any_name_change |
| held | hold_unresolved | otc_markets_stock_screener\|sec_company_tickers_exchange | with_isin | deepseek_needs_official_evidence | 2 | keep_current_until_stronger_issuer_history_source | stronger_official_or_reviewed_issuer_history_source_before_any_name_change |
| held | hold_unresolved | otc_markets_stock_screener\|sec_company_tickers_exchange | with_isin | not_triaged_by_deepseek | 1 | keep_current_until_stronger_issuer_history_source | stronger_official_or_reviewed_issuer_history_source_before_any_name_change |
| held | hold_unresolved | sec_company_tickers_exchange | with_isin | deepseek_needs_official_evidence | 2 | keep_current_until_stronger_issuer_history_source | stronger_official_or_reviewed_issuer_history_source_before_any_name_change |
| held | hold_unresolved | sec_company_tickers_exchange | with_isin | not_triaged_by_deepseek | 1 | keep_current_until_stronger_issuer_history_source | stronger_official_or_reviewed_issuer_history_source_before_any_name_change |

## Gates

- Direct name changes authorized: `False`.
- DeepSeek triage does not authorize any data change.
- Symbol reuse and stale OTC names require official listing-keyed identity evidence or quarantine review.

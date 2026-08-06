# OTC Name Mismatch Review

Generated at: `2026-08-06T14:18:41Z`

This report is a deterministic review queue for OTC `official_name_mismatch` warnings.
Reviewed `keep_current_reviewed` decisions are excluded from the active queue.

## Summary

- Rows: 29
- With ISIN: 24
- Without ISIN: 5

## Review Classes

| Class | Rows |
|---|---:|
| hold_unresolved | 17 |
| probable_otc_rename_or_symbol_reuse | 7 |
| weak_abbreviation_or_truncation_review | 5 |

## Priority

| Priority | Rows |
|---|---:|
| held | 17 |
| high | 7 |
| medium | 5 |

## Apply Eligibility

| Eligibility | Rows |
|---|---:|
| keep_current_until_stronger_issuer_history_source | 17 |
| blocked_until_isin_anchored_issuer_history_review | 7 |
| matcher_tuning_only_no_metadata_apply_until_exact_identity_review | 5 |

## Verification Evidence

| Evidence Required | Rows |
|---|---:|
| stronger_official_or_reviewed_issuer_history_source_before_any_name_change | 17 |
| official_or_reviewed_isin_bearing_source_matching_current_issuer_listing_key_and_name | 7 |
| official_alias_or_abbreviation_evidence_with_exact_listing_identity_match | 5 |

## Review Strategies

| Strategy | Rows |
|---|---:|
| keep_current_until_stronger_issuer_history_source | 17 |
| verify_isin_anchored_issuer_history_before_name_change | 7 |
| review_official_alias_or_abbreviation_before_matcher_tuning | 5 |

## Top Review Batches

| Priority | Class | ISIN | Official sources | Rows | Strategy | Evidence required | Recommended next source | Source gate |
|---|---|---|---|---:|---|---|---|---|
| high | probable_otc_rename_or_symbol_reuse | with_isin | otc_markets_stock_screener | 6 | verify_isin_anchored_issuer_history_before_name_change | official_or_reviewed_isin_bearing_source_matching_current_issuer_listing_key_and_name | Official or reviewed ISIN-bearing issuer-history source matching current issuer, listing key, and name. | Do not change the name until ISIN-anchored evidence proves the same current issuer. |
| held | hold_unresolved | without_isin | otc_markets_security_profile | 5 | keep_current_until_stronger_issuer_history_source | stronger_official_or_reviewed_issuer_history_source_before_any_name_change | Stronger official or reviewed issuer-history source matching the OTC listing key. | Keep current name until stronger issuer-history evidence resolves the ambiguity. |
| held | hold_unresolved | with_isin | otc_markets_stock_screener | 4 | keep_current_until_stronger_issuer_history_source | stronger_official_or_reviewed_issuer_history_source_before_any_name_change | Stronger official or reviewed issuer-history source matching the OTC listing key. | Keep current name until stronger issuer-history evidence resolves the ambiguity. |
| medium | weak_abbreviation_or_truncation_review | with_isin | otc_markets_stock_screener | 4 | review_official_alias_or_abbreviation_before_matcher_tuning | official_alias_or_abbreviation_evidence_with_exact_listing_identity_match | Official alias, abbreviation, issuer, OTC profile, or registry evidence matching the exact listing identity. | Tune matcher only after official alias evidence; do not change metadata from abbreviation alone. |
| held | hold_unresolved | with_isin | otc_markets_stock_screener|sec_company_tickers_exchange | 3 | keep_current_until_stronger_issuer_history_source | stronger_official_or_reviewed_issuer_history_source_before_any_name_change | Stronger official or reviewed issuer-history source matching the OTC listing key. | Keep current name until stronger issuer-history evidence resolves the ambiguity. |
| held | hold_unresolved | with_isin | sec_company_tickers_exchange | 3 | keep_current_until_stronger_issuer_history_source | stronger_official_or_reviewed_issuer_history_source_before_any_name_change | Stronger official or reviewed issuer-history source matching the OTC listing key. | Keep current name until stronger issuer-history evidence resolves the ambiguity. |
| held | hold_unresolved | with_isin | otc_markets_security_profile | 1 | keep_current_until_stronger_issuer_history_source | stronger_official_or_reviewed_issuer_history_source_before_any_name_change | Stronger official or reviewed issuer-history source matching the OTC listing key. | Keep current name until stronger issuer-history evidence resolves the ambiguity. |
| held | hold_unresolved | with_isin | otc_markets_security_profile|otc_markets_stock_screener | 1 | keep_current_until_stronger_issuer_history_source | stronger_official_or_reviewed_issuer_history_source_before_any_name_change | Stronger official or reviewed issuer-history source matching the OTC listing key. | Keep current name until stronger issuer-history evidence resolves the ambiguity. |
| high | probable_otc_rename_or_symbol_reuse | with_isin | otc_markets_stock_screener|sec_company_tickers_exchange | 1 | verify_isin_anchored_issuer_history_before_name_change | official_or_reviewed_isin_bearing_source_matching_current_issuer_listing_key_and_name | Official or reviewed ISIN-bearing issuer-history source matching current issuer, listing key, and name. | Do not change the name until ISIN-anchored evidence proves the same current issuer. |
| medium | weak_abbreviation_or_truncation_review | with_isin | otc_markets_stock_screener|sec_company_tickers_exchange | 1 | review_official_alias_or_abbreviation_before_matcher_tuning | official_alias_or_abbreviation_evidence_with_exact_listing_identity_match | Official alias, abbreviation, issuer, OTC profile, or registry evidence matching the exact listing identity. | Tune matcher only after official alias evidence; do not change metadata from abbreviation alone. |

## Policy

- `keep_current_reviewed` suppresses already-reviewed stale OTC naming noise where the current canonical dataset name is intentionally retained.
- `hold_unresolved` marks source-limited ambiguities that remain intentionally open until a stronger issuer-history source is available.
- `probable_otc_rename_or_symbol_reuse` needs an ISIN-bearing issuer/source check before applying a name update.
- `stale_or_symbol_reuse_without_isin` is the highest-risk bucket because ticker reuse cannot be disambiguated locally.
- `weak_abbreviation_or_truncation_review` should improve the matcher only when the official OTC abbreviation is clearly the same issuer.
- `matcher_false_positive` means the deterministic matcher should be tightened if the row still appears in `entry_quality.csv`.

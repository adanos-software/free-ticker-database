# OTC Name Mismatch Review

Generated at: `2026-05-25T05:42:32Z`

This report is a deterministic review queue for OTC `official_name_mismatch` warnings.
Reviewed `keep_current_reviewed` decisions are excluded from the active queue.

## Summary

- Rows: 146
- With ISIN: 138
- Without ISIN: 8

## Review Classes

| Class | Rows |
|---|---:|
| weak_abbreviation_or_truncation_review | 74 |
| probable_otc_rename_or_symbol_reuse | 65 |
| stale_or_symbol_reuse_without_isin | 7 |

## Priority

| Priority | Rows |
|---|---:|
| medium | 74 |
| high | 65 |
| critical | 7 |

## Apply Eligibility

| Eligibility | Rows |
|---|---:|
| matcher_tuning_only_no_metadata_apply_until_exact_identity_review | 74 |
| blocked_until_isin_anchored_issuer_history_review | 65 |
| blocked_until_official_issuer_identity_source_or_quarantine_decision | 7 |

## Verification Evidence

| Evidence Required | Rows |
|---|---:|
| official_alias_or_abbreviation_evidence_with_exact_listing_identity_match | 74 |
| official_or_reviewed_isin_bearing_source_matching_current_issuer_listing_key_and_name | 65 |
| official_otc_profile_registry_or_issuer_history_source_matching_listing_key_before_name_or_quarantine_action | 7 |

## Review Strategies

| Strategy | Rows |
|---|---:|
| review_official_alias_or_abbreviation_before_matcher_tuning | 74 |
| verify_isin_anchored_issuer_history_before_name_change | 65 |
| resolve_or_quarantine_with_official_otc_profile_or_issuer_history | 7 |

## Top Review Batches

| Priority | Class | ISIN | Official sources | Rows | Strategy | Evidence required | Recommended next source | Source gate |
|---|---|---|---|---:|---|---|---|---|
| medium | weak_abbreviation_or_truncation_review | with_isin | otc_markets_stock_screener | 69 | review_official_alias_or_abbreviation_before_matcher_tuning | official_alias_or_abbreviation_evidence_with_exact_listing_identity_match | Official alias, abbreviation, issuer, OTC profile, or registry evidence matching the exact listing identity. | Tune matcher only after official alias evidence; do not change metadata from abbreviation alone. |
| high | probable_otc_rename_or_symbol_reuse | with_isin | otc_markets_stock_screener | 62 | verify_isin_anchored_issuer_history_before_name_change | official_or_reviewed_isin_bearing_source_matching_current_issuer_listing_key_and_name | Official or reviewed ISIN-bearing issuer-history source matching current issuer, listing key, and name. | Do not change the name until ISIN-anchored evidence proves the same current issuer. |
| critical | stale_or_symbol_reuse_without_isin | without_isin | otc_markets_security_profile | 5 | resolve_or_quarantine_with_official_otc_profile_or_issuer_history | official_otc_profile_registry_or_issuer_history_source_matching_listing_key_before_name_or_quarantine_action | Official OTC profile, registry, exchange notice, or issuer-history source matching the listing key. | Do not rename or trust reused OTC symbol without listing-keyed official identity or quarantine evidence. |
| critical | stale_or_symbol_reuse_without_isin | without_isin | otc_markets_security_profile|otc_markets_stock_screener | 2 | resolve_or_quarantine_with_official_otc_profile_or_issuer_history | official_otc_profile_registry_or_issuer_history_source_matching_listing_key_before_name_or_quarantine_action | Official OTC profile, registry, exchange notice, or issuer-history source matching the listing key. | Do not rename or trust reused OTC symbol without listing-keyed official identity or quarantine evidence. |
| high | probable_otc_rename_or_symbol_reuse | with_isin | otc_markets_stock_screener|sec_company_tickers_exchange | 2 | verify_isin_anchored_issuer_history_before_name_change | official_or_reviewed_isin_bearing_source_matching_current_issuer_listing_key_and_name | Official or reviewed ISIN-bearing issuer-history source matching current issuer, listing key, and name. | Do not change the name until ISIN-anchored evidence proves the same current issuer. |
| medium | weak_abbreviation_or_truncation_review | with_isin | otc_markets_stock_screener|sec_company_tickers_exchange | 2 | review_official_alias_or_abbreviation_before_matcher_tuning | official_alias_or_abbreviation_evidence_with_exact_listing_identity_match | Official alias, abbreviation, issuer, OTC profile, or registry evidence matching the exact listing identity. | Tune matcher only after official alias evidence; do not change metadata from abbreviation alone. |
| medium | weak_abbreviation_or_truncation_review | with_isin | sec_company_tickers_exchange | 2 | review_official_alias_or_abbreviation_before_matcher_tuning | official_alias_or_abbreviation_evidence_with_exact_listing_identity_match | Official alias, abbreviation, issuer, OTC profile, or registry evidence matching the exact listing identity. | Tune matcher only after official alias evidence; do not change metadata from abbreviation alone. |
| high | probable_otc_rename_or_symbol_reuse | with_isin | sec_company_tickers_exchange | 1 | verify_isin_anchored_issuer_history_before_name_change | official_or_reviewed_isin_bearing_source_matching_current_issuer_listing_key_and_name | Official or reviewed ISIN-bearing issuer-history source matching current issuer, listing key, and name. | Do not change the name until ISIN-anchored evidence proves the same current issuer. |
| medium | weak_abbreviation_or_truncation_review | without_isin | otc_markets_security_profile | 1 | review_official_alias_or_abbreviation_before_matcher_tuning | official_alias_or_abbreviation_evidence_with_exact_listing_identity_match | Official alias, abbreviation, issuer, OTC profile, or registry evidence matching the exact listing identity. | Tune matcher only after official alias evidence; do not change metadata from abbreviation alone. |

## Policy

- `keep_current_reviewed` suppresses already-reviewed stale OTC naming noise where the current canonical dataset name is intentionally retained.
- `hold_unresolved` marks source-limited ambiguities that remain intentionally open until a stronger issuer-history source is available.
- `probable_otc_rename_or_symbol_reuse` needs an ISIN-bearing issuer/source check before applying a name update.
- `stale_or_symbol_reuse_without_isin` is the highest-risk bucket because ticker reuse cannot be disambiguated locally.
- `weak_abbreviation_or_truncation_review` should improve the matcher only when the official OTC abbreviation is clearly the same issuer.
- `matcher_false_positive` means the deterministic matcher should be tightened if the row still appears in `entry_quality.csv`.

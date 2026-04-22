# OTC Name Mismatch Review

Generated at: `2026-04-22T09:47:04Z`

This report is a deterministic review queue for OTC `official_name_mismatch` warnings.
Reviewed `keep_current_reviewed` decisions are excluded from the active queue.

## Summary

- Rows: 0
- With ISIN: 0
- Without ISIN: 0

## Review Classes

| Class | Rows |
|---|---:|

## Priority

| Priority | Rows |
|---|---:|

## Policy

- `keep_current_reviewed` suppresses already-reviewed stale OTC naming noise where the current canonical dataset name is intentionally retained.
- `hold_unresolved` marks source-limited ambiguities that remain intentionally open until a stronger issuer-history source is available.
- `probable_otc_rename_or_symbol_reuse` needs an ISIN-bearing issuer/source check before applying a name update.
- `stale_or_symbol_reuse_without_isin` is the highest-risk bucket because ticker reuse cannot be disambiguated locally.
- `weak_abbreviation_or_truncation_review` should improve the matcher only when the official OTC abbreviation is clearly the same issuer.
- `matcher_false_positive` means the deterministic matcher should be tightened if the row still appears in `entry_quality.csv`.

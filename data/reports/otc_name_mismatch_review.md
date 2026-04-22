# OTC Name Mismatch Review

Generated at: `2026-04-22T06:33:11Z`

This report is a deterministic review queue for OTC `official_name_mismatch` warnings.
It does not apply metadata updates automatically.

## Summary

- Rows: 458
- With ISIN: 359
- Without ISIN: 99

## Review Classes

| Class | Rows |
|---|---:|
| weak_abbreviation_or_truncation_review | 235 |
| probable_otc_rename_or_symbol_reuse | 156 |
| stale_or_symbol_reuse_without_isin | 67 |

## Priority

| Priority | Rows |
|---|---:|
| medium | 235 |
| high | 156 |
| critical | 67 |

## Policy

- `probable_otc_rename_or_symbol_reuse` needs an ISIN-bearing issuer/source check before applying a name update.
- `stale_or_symbol_reuse_without_isin` is the highest-risk bucket because ticker reuse cannot be disambiguated locally.
- `weak_abbreviation_or_truncation_review` should improve the matcher only when the official OTC abbreviation is clearly the same issuer.
- `matcher_false_positive` means the deterministic matcher should be tightened if the row still appears in `entry_quality.csv`.

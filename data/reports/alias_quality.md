# Alias Quality Report

Generated at: `2026-05-05T06:25:03Z`

This report classifies `data/aliases.csv` for Natural-Language detection safety.
Identifier aliases remain useful for lookup, but are rejected for mention detection.

## Status Counts

| Status | Rows |
|---|---:|
| reject | 61,623 |
| accept | 56,555 |
| review | 479 |

## Detection Policies

| Policy | Rows |
|---|---:|
| identifier_only | 61,623 |
| safe_natural_language | 56,555 |
| symbol_alias_only | 479 |

## Top Reasons

| Reason | Rows |
|---|---:|
| identifier_alias | 61,623 |
| accepted_name_alias | 56,555 |
| same_as_ticker | 479 |

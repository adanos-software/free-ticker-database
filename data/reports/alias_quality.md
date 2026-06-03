# Alias Quality Report

Generated at: `2026-06-03T18:04:31Z`

This report classifies `data/aliases.csv` for Natural-Language detection safety.
Identifier aliases remain useful for lookup, but are rejected for mention detection.

## Status Counts

| Status | Rows |
|---|---:|
| reject | 64,765 |
| accept | 56,436 |
| review | 427 |

## Detection Policies

| Policy | Rows |
|---|---:|
| identifier_only | 64,765 |
| safe_natural_language | 56,436 |
| symbol_alias_only | 427 |

## Top Reasons

| Reason | Rows |
|---|---:|
| identifier_alias | 64,765 |
| accepted_name_alias | 56,436 |
| same_as_ticker | 427 |

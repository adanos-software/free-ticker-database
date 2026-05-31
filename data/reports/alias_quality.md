# Alias Quality Report

Generated at: `2026-05-31T03:59:53Z`

This report classifies `data/aliases.csv` for Natural-Language detection safety.
Identifier aliases remain useful for lookup, but are rejected for mention detection.

## Status Counts

| Status | Rows |
|---|---:|
| reject | 64,762 |
| accept | 56,463 |
| review | 427 |

## Detection Policies

| Policy | Rows |
|---|---:|
| identifier_only | 64,762 |
| safe_natural_language | 56,463 |
| symbol_alias_only | 427 |

## Top Reasons

| Reason | Rows |
|---|---:|
| identifier_alias | 64,762 |
| accepted_name_alias | 56,463 |
| same_as_ticker | 427 |

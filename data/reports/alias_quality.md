# Alias Quality Report

Generated at: `2026-06-04T16:35:16Z`

This report classifies `data/aliases.csv` for Natural-Language detection safety.
Identifier aliases remain useful for lookup, but are rejected for mention detection.

## Status Counts

| Status | Rows |
|---|---:|
| reject | 64,833 |
| accept | 56,747 |
| review | 427 |

## Detection Policies

| Policy | Rows |
|---|---:|
| identifier_only | 64,833 |
| safe_natural_language | 56,747 |
| symbol_alias_only | 427 |

## Top Reasons

| Reason | Rows |
|---|---:|
| identifier_alias | 64,833 |
| accepted_name_alias | 56,747 |
| same_as_ticker | 427 |

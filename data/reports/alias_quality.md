# Alias Quality Report

Generated at: `2026-06-12T05:21:53Z`

This report classifies `data/aliases.csv` for Natural-Language detection safety.
Identifier aliases remain useful for lookup, but are rejected for mention detection.

## Status Counts

| Status | Rows |
|---|---:|
| reject | 64,830 |
| accept | 56,804 |
| review | 424 |

## Detection Policies

| Policy | Rows |
|---|---:|
| identifier_only | 64,830 |
| safe_natural_language | 56,804 |
| symbol_alias_only | 424 |

## Top Reasons

| Reason | Rows |
|---|---:|
| identifier_alias | 64,830 |
| accepted_name_alias | 56,804 |
| same_as_ticker | 424 |

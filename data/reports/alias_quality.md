# Alias Quality Report

Generated at: `2026-06-03T17:07:01Z`

This report classifies `data/aliases.csv` for Natural-Language detection safety.
Identifier aliases remain useful for lookup, but are rejected for mention detection.

## Status Counts

| Status | Rows |
|---|---:|
| reject | 64,767 |
| accept | 56,434 |
| review | 427 |

## Detection Policies

| Policy | Rows |
|---|---:|
| identifier_only | 64,767 |
| safe_natural_language | 56,434 |
| symbol_alias_only | 427 |

## Top Reasons

| Reason | Rows |
|---|---:|
| identifier_alias | 64,767 |
| accepted_name_alias | 56,434 |
| same_as_ticker | 427 |

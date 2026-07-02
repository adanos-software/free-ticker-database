# Alias Quality Report

Generated at: `2026-07-02T12:49:29Z`

This report classifies `data/aliases.csv` for Natural-Language detection safety.
Identifier aliases remain useful for lookup, but are rejected for mention detection.

## Status Counts

| Status | Rows |
|---|---:|
| reject | 65,819 |
| accept | 58,401 |
| review | 482 |

## Detection Policies

| Policy | Rows |
|---|---:|
| identifier_only | 65,819 |
| safe_natural_language | 58,401 |
| symbol_alias_only | 482 |

## Top Reasons

| Reason | Rows |
|---|---:|
| identifier_alias | 65,819 |
| accepted_name_alias | 58,401 |
| same_as_ticker | 482 |

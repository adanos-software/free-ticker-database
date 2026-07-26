# Alias Quality Report

Generated at: `2026-07-26T08:48:00Z`

This report classifies `data/aliases.csv` for Natural-Language detection safety.
Identifier aliases remain useful for lookup, but are rejected for mention detection.

## Status Counts

| Status | Rows |
|---|---:|
| reject | 66,011 |
| accept | 58,567 |
| review | 464 |

## Detection Policies

| Policy | Rows |
|---|---:|
| identifier_only | 66,011 |
| safe_natural_language | 58,567 |
| symbol_alias_only | 464 |

## Top Reasons

| Reason | Rows |
|---|---:|
| identifier_alias | 66,011 |
| accepted_name_alias | 58,567 |
| same_as_ticker | 464 |

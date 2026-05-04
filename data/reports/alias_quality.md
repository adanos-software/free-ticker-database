# Alias Quality Report

Generated at: `2026-05-04T08:18:33Z`

This report classifies `data/aliases.csv` for Natural-Language detection safety.
Identifier aliases remain useful for lookup, but are rejected for mention detection.

## Status Counts

| Status | Rows |
|---|---:|
| reject | 61,595 |
| accept | 56,557 |
| review | 479 |

## Detection Policies

| Policy | Rows |
|---|---:|
| identifier_only | 61,595 |
| safe_natural_language | 56,557 |
| symbol_alias_only | 479 |

## Top Reasons

| Reason | Rows |
|---|---:|
| identifier_alias | 61,595 |
| accepted_name_alias | 56,557 |
| same_as_ticker | 479 |

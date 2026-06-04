# Alias Quality Report

Generated at: `2026-06-04T04:00:38Z`

This report classifies `data/aliases.csv` for Natural-Language detection safety.
Identifier aliases remain useful for lookup, but are rejected for mention detection.

## Status Counts

| Status | Rows |
|---|---:|
| reject | 64,715 |
| accept | 56,668 |
| review | 427 |

## Detection Policies

| Policy | Rows |
|---|---:|
| identifier_only | 64,715 |
| safe_natural_language | 56,668 |
| symbol_alias_only | 427 |

## Top Reasons

| Reason | Rows |
|---|---:|
| identifier_alias | 64,715 |
| accepted_name_alias | 56,668 |
| same_as_ticker | 427 |

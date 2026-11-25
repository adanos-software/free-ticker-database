# Alias Quality Report

Generated at: `2026-11-25T17:34:22Z`

This report classifies `data/aliases.csv` for Natural-Language detection safety.
Identifier aliases remain useful for lookup, but are rejected for mention detection.

## Status Counts

| Status | Rows |
|---|---:|
| reject | 68,491 |
| accept | 60,430 |
| review | 480 |

## Detection Policies

| Policy | Rows |
|---|---:|
| identifier_only | 68,491 |
| safe_natural_language | 60,430 |
| symbol_alias_only | 480 |

## Top Reasons

| Reason | Rows |
|---|---:|
| identifier_alias | 68,491 |
| accepted_name_alias | 60,430 |
| same_as_ticker | 477 |
| exchange_ticker_alias | 3 |

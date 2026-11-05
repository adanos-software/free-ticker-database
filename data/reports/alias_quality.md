# Alias Quality Report

Generated at: `2026-11-05T17:18:42Z`

This report classifies `data/aliases.csv` for Natural-Language detection safety.
Identifier aliases remain useful for lookup, but are rejected for mention detection.

## Status Counts

| Status | Rows |
|---|---:|
| reject | 68,596 |
| accept | 60,064 |
| review | 481 |

## Detection Policies

| Policy | Rows |
|---|---:|
| identifier_only | 68,596 |
| safe_natural_language | 60,064 |
| symbol_alias_only | 481 |

## Top Reasons

| Reason | Rows |
|---|---:|
| identifier_alias | 68,596 |
| accepted_name_alias | 60,064 |
| same_as_ticker | 478 |
| exchange_ticker_alias | 3 |

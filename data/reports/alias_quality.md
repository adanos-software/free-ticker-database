# Alias Quality Report

Generated at: `2026-11-10T16:35:52Z`

This report classifies `data/aliases.csv` for Natural-Language detection safety.
Identifier aliases remain useful for lookup, but are rejected for mention detection.

## Status Counts

| Status | Rows |
|---|---:|
| reject | 68,524 |
| accept | 59,997 |
| review | 481 |

## Detection Policies

| Policy | Rows |
|---|---:|
| identifier_only | 68,524 |
| safe_natural_language | 59,997 |
| symbol_alias_only | 481 |

## Top Reasons

| Reason | Rows |
|---|---:|
| identifier_alias | 68,524 |
| accepted_name_alias | 59,997 |
| same_as_ticker | 478 |
| exchange_ticker_alias | 3 |

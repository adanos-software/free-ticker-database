# Alias Quality Report

Generated at: `2026-11-10T15:15:40Z`

This report classifies `data/aliases.csv` for Natural-Language detection safety.
Identifier aliases remain useful for lookup, but are rejected for mention detection.

## Status Counts

| Status | Rows |
|---|---:|
| reject | 68,523 |
| accept | 59,997 |
| review | 481 |

## Detection Policies

| Policy | Rows |
|---|---:|
| identifier_only | 68,523 |
| safe_natural_language | 59,997 |
| symbol_alias_only | 481 |

## Top Reasons

| Reason | Rows |
|---|---:|
| identifier_alias | 68,523 |
| accepted_name_alias | 59,997 |
| same_as_ticker | 478 |
| exchange_ticker_alias | 3 |

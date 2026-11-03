# Alias Quality Report

Generated at: `2026-11-03T14:15:49Z`

This report classifies `data/aliases.csv` for Natural-Language detection safety.
Identifier aliases remain useful for lookup, but are rejected for mention detection.

## Status Counts

| Status | Rows |
|---|---:|
| reject | 68,580 |
| accept | 60,038 |
| review | 481 |

## Detection Policies

| Policy | Rows |
|---|---:|
| identifier_only | 68,580 |
| safe_natural_language | 60,038 |
| symbol_alias_only | 481 |

## Top Reasons

| Reason | Rows |
|---|---:|
| identifier_alias | 68,580 |
| accepted_name_alias | 60,038 |
| same_as_ticker | 478 |
| exchange_ticker_alias | 3 |

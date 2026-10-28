# Alias Quality Report

Generated at: `2026-10-28T13:33:41Z`

This report classifies `data/aliases.csv` for Natural-Language detection safety.
Identifier aliases remain useful for lookup, but are rejected for mention detection.

## Status Counts

| Status | Rows |
|---|---:|
| reject | 68,411 |
| accept | 59,962 |
| review | 481 |

## Detection Policies

| Policy | Rows |
|---|---:|
| identifier_only | 68,411 |
| safe_natural_language | 59,962 |
| symbol_alias_only | 481 |

## Top Reasons

| Reason | Rows |
|---|---:|
| identifier_alias | 68,411 |
| accepted_name_alias | 59,962 |
| same_as_ticker | 478 |
| exchange_ticker_alias | 3 |

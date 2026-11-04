# Alias Quality Report

Generated at: `2026-11-04T16:55:54Z`

This report classifies `data/aliases.csv` for Natural-Language detection safety.
Identifier aliases remain useful for lookup, but are rejected for mention detection.

## Status Counts

| Status | Rows |
|---|---:|
| reject | 68,598 |
| accept | 60,046 |
| review | 481 |

## Detection Policies

| Policy | Rows |
|---|---:|
| identifier_only | 68,598 |
| safe_natural_language | 60,046 |
| symbol_alias_only | 481 |

## Top Reasons

| Reason | Rows |
|---|---:|
| identifier_alias | 68,598 |
| accepted_name_alias | 60,046 |
| same_as_ticker | 478 |
| exchange_ticker_alias | 3 |

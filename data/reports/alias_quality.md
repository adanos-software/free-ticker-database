# Alias Quality Report

Generated at: `2026-11-07T13:00:02Z`

This report classifies `data/aliases.csv` for Natural-Language detection safety.
Identifier aliases remain useful for lookup, but are rejected for mention detection.

## Status Counts

| Status | Rows |
|---|---:|
| reject | 68,584 |
| accept | 60,052 |
| review | 481 |

## Detection Policies

| Policy | Rows |
|---|---:|
| identifier_only | 68,584 |
| safe_natural_language | 60,052 |
| symbol_alias_only | 481 |

## Top Reasons

| Reason | Rows |
|---|---:|
| identifier_alias | 68,584 |
| accepted_name_alias | 60,052 |
| same_as_ticker | 478 |
| exchange_ticker_alias | 3 |

# Alias Quality Report

Generated at: `2026-09-28T13:57:12Z`

This report classifies `data/aliases.csv` for Natural-Language detection safety.
Identifier aliases remain useful for lookup, but are rejected for mention detection.

## Status Counts

| Status | Rows |
|---|---:|
| reject | 67,931 |
| accept | 59,624 |
| review | 478 |

## Detection Policies

| Policy | Rows |
|---|---:|
| identifier_only | 67,931 |
| safe_natural_language | 59,624 |
| symbol_alias_only | 478 |

## Top Reasons

| Reason | Rows |
|---|---:|
| identifier_alias | 67,931 |
| accepted_name_alias | 59,624 |
| same_as_ticker | 476 |
| exchange_ticker_alias | 2 |

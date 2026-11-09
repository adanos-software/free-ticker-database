# Alias Quality Report

Generated at: `2026-11-09T19:07:50Z`

This report classifies `data/aliases.csv` for Natural-Language detection safety.
Identifier aliases remain useful for lookup, but are rejected for mention detection.

## Status Counts

| Status | Rows |
|---|---:|
| reject | 68,550 |
| accept | 60,009 |
| review | 481 |

## Detection Policies

| Policy | Rows |
|---|---:|
| identifier_only | 68,550 |
| safe_natural_language | 60,009 |
| symbol_alias_only | 481 |

## Top Reasons

| Reason | Rows |
|---|---:|
| identifier_alias | 68,550 |
| accepted_name_alias | 60,009 |
| same_as_ticker | 478 |
| exchange_ticker_alias | 3 |

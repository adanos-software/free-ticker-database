# Alias Quality Report

Generated at: `2026-10-09T13:19:24Z`

This report classifies `data/aliases.csv` for Natural-Language detection safety.
Identifier aliases remain useful for lookup, but are rejected for mention detection.

## Status Counts

| Status | Rows |
|---|---:|
| reject | 68,383 |
| accept | 59,797 |
| review | 480 |

## Detection Policies

| Policy | Rows |
|---|---:|
| identifier_only | 68,383 |
| safe_natural_language | 59,797 |
| symbol_alias_only | 480 |

## Top Reasons

| Reason | Rows |
|---|---:|
| identifier_alias | 68,383 |
| accepted_name_alias | 59,797 |
| same_as_ticker | 478 |
| exchange_ticker_alias | 2 |

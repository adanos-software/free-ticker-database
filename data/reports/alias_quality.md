# Alias Quality Report

Generated at: `2026-09-20T18:40:24Z`

This report classifies `data/aliases.csv` for Natural-Language detection safety.
Identifier aliases remain useful for lookup, but are rejected for mention detection.

## Status Counts

| Status | Rows |
|---|---:|
| reject | 66,663 |
| accept | 58,448 |
| review | 466 |

## Detection Policies

| Policy | Rows |
|---|---:|
| identifier_only | 66,663 |
| safe_natural_language | 58,448 |
| symbol_alias_only | 466 |

## Top Reasons

| Reason | Rows |
|---|---:|
| identifier_alias | 66,663 |
| accepted_name_alias | 58,448 |
| same_as_ticker | 464 |
| exchange_ticker_alias | 2 |

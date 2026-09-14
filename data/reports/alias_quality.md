# Alias Quality Report

Generated at: `2026-09-14T14:28:49Z`

This report classifies `data/aliases.csv` for Natural-Language detection safety.
Identifier aliases remain useful for lookup, but are rejected for mention detection.

## Status Counts

| Status | Rows |
|---|---:|
| reject | 66,664 |
| accept | 58,422 |
| review | 466 |

## Detection Policies

| Policy | Rows |
|---|---:|
| identifier_only | 66,664 |
| safe_natural_language | 58,422 |
| symbol_alias_only | 466 |

## Top Reasons

| Reason | Rows |
|---|---:|
| identifier_alias | 66,664 |
| accepted_name_alias | 58,422 |
| same_as_ticker | 464 |
| exchange_ticker_alias | 2 |

# Alias Quality Report

Generated at: `2026-11-06T14:38:30Z`

This report classifies `data/aliases.csv` for Natural-Language detection safety.
Identifier aliases remain useful for lookup, but are rejected for mention detection.

## Status Counts

| Status | Rows |
|---|---:|
| reject | 68,601 |
| accept | 60,075 |
| review | 481 |

## Detection Policies

| Policy | Rows |
|---|---:|
| identifier_only | 68,601 |
| safe_natural_language | 60,075 |
| symbol_alias_only | 481 |

## Top Reasons

| Reason | Rows |
|---|---:|
| identifier_alias | 68,601 |
| accepted_name_alias | 60,075 |
| same_as_ticker | 478 |
| exchange_ticker_alias | 3 |

# Alias Quality Report

Generated at: `2026-10-08T14:01:07Z`

This report classifies `data/aliases.csv` for Natural-Language detection safety.
Identifier aliases remain useful for lookup, but are rejected for mention detection.

## Status Counts

| Status | Rows |
|---|---:|
| reject | 68,317 |
| accept | 59,745 |
| review | 477 |

## Detection Policies

| Policy | Rows |
|---|---:|
| identifier_only | 68,317 |
| safe_natural_language | 59,745 |
| symbol_alias_only | 477 |

## Top Reasons

| Reason | Rows |
|---|---:|
| identifier_alias | 68,317 |
| accepted_name_alias | 59,745 |
| same_as_ticker | 475 |
| exchange_ticker_alias | 2 |

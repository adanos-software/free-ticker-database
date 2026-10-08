# Alias Quality Report

Generated at: `2026-10-08T15:12:53Z`

This report classifies `data/aliases.csv` for Natural-Language detection safety.
Identifier aliases remain useful for lookup, but are rejected for mention detection.

## Status Counts

| Status | Rows |
|---|---:|
| reject | 68,316 |
| accept | 59,745 |
| review | 477 |

## Detection Policies

| Policy | Rows |
|---|---:|
| identifier_only | 68,316 |
| safe_natural_language | 59,745 |
| symbol_alias_only | 477 |

## Top Reasons

| Reason | Rows |
|---|---:|
| identifier_alias | 68,316 |
| accepted_name_alias | 59,745 |
| same_as_ticker | 475 |
| exchange_ticker_alias | 2 |

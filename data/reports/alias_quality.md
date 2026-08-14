# Alias Quality Report

Generated at: `2026-08-14T08:01:38Z`

This report classifies `data/aliases.csv` for Natural-Language detection safety.
Identifier aliases remain useful for lookup, but are rejected for mention detection.

## Status Counts

| Status | Rows |
|---|---:|
| reject | 66,466 |
| accept | 58,311 |
| review | 466 |

## Detection Policies

| Policy | Rows |
|---|---:|
| identifier_only | 66,466 |
| safe_natural_language | 58,311 |
| symbol_alias_only | 466 |

## Top Reasons

| Reason | Rows |
|---|---:|
| identifier_alias | 66,466 |
| accepted_name_alias | 58,311 |
| same_as_ticker | 464 |
| exchange_ticker_alias | 2 |

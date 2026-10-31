# Alias Quality Report

Generated at: `2026-10-31T12:22:03Z`

This report classifies `data/aliases.csv` for Natural-Language detection safety.
Identifier aliases remain useful for lookup, but are rejected for mention detection.

## Status Counts

| Status | Rows |
|---|---:|
| reject | 68,490 |
| accept | 60,026 |
| review | 481 |

## Detection Policies

| Policy | Rows |
|---|---:|
| identifier_only | 68,490 |
| safe_natural_language | 60,026 |
| symbol_alias_only | 481 |

## Top Reasons

| Reason | Rows |
|---|---:|
| identifier_alias | 68,490 |
| accepted_name_alias | 60,026 |
| same_as_ticker | 478 |
| exchange_ticker_alias | 3 |

# Alias Quality Report

Generated at: `2026-11-16T15:45:27Z`

This report classifies `data/aliases.csv` for Natural-Language detection safety.
Identifier aliases remain useful for lookup, but are rejected for mention detection.

## Status Counts

| Status | Rows |
|---|---:|
| reject | 68,446 |
| accept | 60,056 |
| review | 480 |

## Detection Policies

| Policy | Rows |
|---|---:|
| identifier_only | 68,446 |
| safe_natural_language | 60,056 |
| symbol_alias_only | 480 |

## Top Reasons

| Reason | Rows |
|---|---:|
| identifier_alias | 68,446 |
| accepted_name_alias | 60,056 |
| same_as_ticker | 477 |
| exchange_ticker_alias | 3 |

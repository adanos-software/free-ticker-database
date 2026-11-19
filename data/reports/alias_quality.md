# Alias Quality Report

Generated at: `2026-11-19T16:03:47Z`

This report classifies `data/aliases.csv` for Natural-Language detection safety.
Identifier aliases remain useful for lookup, but are rejected for mention detection.

## Status Counts

| Status | Rows |
|---|---:|
| reject | 68,458 |
| accept | 60,203 |
| review | 480 |

## Detection Policies

| Policy | Rows |
|---|---:|
| identifier_only | 68,458 |
| safe_natural_language | 60,203 |
| symbol_alias_only | 480 |

## Top Reasons

| Reason | Rows |
|---|---:|
| identifier_alias | 68,458 |
| accepted_name_alias | 60,203 |
| same_as_ticker | 477 |
| exchange_ticker_alias | 3 |

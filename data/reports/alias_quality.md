# Alias Quality Report

Generated at: `2026-11-25T17:39:05Z`

This report classifies `data/aliases.csv` for Natural-Language detection safety.
Identifier aliases remain useful for lookup, but are rejected for mention detection.

## Status Counts

| Status | Rows |
|---|---:|
| reject | 68,482 |
| accept | 60,425 |
| review | 480 |

## Detection Policies

| Policy | Rows |
|---|---:|
| identifier_only | 68,482 |
| safe_natural_language | 60,425 |
| symbol_alias_only | 480 |

## Top Reasons

| Reason | Rows |
|---|---:|
| identifier_alias | 68,482 |
| accepted_name_alias | 60,425 |
| same_as_ticker | 477 |
| exchange_ticker_alias | 3 |

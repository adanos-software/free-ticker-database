# Alias Quality Report

Generated at: `2026-11-13T15:16:26Z`

This report classifies `data/aliases.csv` for Natural-Language detection safety.
Identifier aliases remain useful for lookup, but are rejected for mention detection.

## Status Counts

| Status | Rows |
|---|---:|
| reject | 68,545 |
| accept | 60,100 |
| review | 480 |

## Detection Policies

| Policy | Rows |
|---|---:|
| identifier_only | 68,545 |
| safe_natural_language | 60,100 |
| symbol_alias_only | 480 |

## Top Reasons

| Reason | Rows |
|---|---:|
| identifier_alias | 68,545 |
| accepted_name_alias | 60,100 |
| same_as_ticker | 477 |
| exchange_ticker_alias | 3 |

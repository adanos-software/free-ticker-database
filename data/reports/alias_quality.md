# Alias Quality Report

Generated at: `2026-11-18T16:45:31Z`

This report classifies `data/aliases.csv` for Natural-Language detection safety.
Identifier aliases remain useful for lookup, but are rejected for mention detection.

## Status Counts

| Status | Rows |
|---|---:|
| reject | 68,460 |
| accept | 60,124 |
| review | 480 |

## Detection Policies

| Policy | Rows |
|---|---:|
| identifier_only | 68,460 |
| safe_natural_language | 60,124 |
| symbol_alias_only | 480 |

## Top Reasons

| Reason | Rows |
|---|---:|
| identifier_alias | 68,460 |
| accepted_name_alias | 60,124 |
| same_as_ticker | 477 |
| exchange_ticker_alias | 3 |

# Alias Quality Report

Generated at: `2026-11-12T15:37:14Z`

This report classifies `data/aliases.csv` for Natural-Language detection safety.
Identifier aliases remain useful for lookup, but are rejected for mention detection.

## Status Counts

| Status | Rows |
|---|---:|
| reject | 68,533 |
| accept | 60,089 |
| review | 480 |

## Detection Policies

| Policy | Rows |
|---|---:|
| identifier_only | 68,533 |
| safe_natural_language | 60,089 |
| symbol_alias_only | 480 |

## Top Reasons

| Reason | Rows |
|---|---:|
| identifier_alias | 68,533 |
| accepted_name_alias | 60,089 |
| same_as_ticker | 477 |
| exchange_ticker_alias | 3 |

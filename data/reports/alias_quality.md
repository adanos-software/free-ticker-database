# Alias Quality Report

Generated at: `2026-10-08T15:22:41Z`

This report classifies `data/aliases.csv` for Natural-Language detection safety.
Identifier aliases remain useful for lookup, but are rejected for mention detection.

## Status Counts

| Status | Rows |
|---|---:|
| reject | 68,359 |
| accept | 59,775 |
| review | 480 |

## Detection Policies

| Policy | Rows |
|---|---:|
| identifier_only | 68,359 |
| safe_natural_language | 59,775 |
| symbol_alias_only | 480 |

## Top Reasons

| Reason | Rows |
|---|---:|
| identifier_alias | 68,359 |
| accepted_name_alias | 59,775 |
| same_as_ticker | 478 |
| exchange_ticker_alias | 2 |

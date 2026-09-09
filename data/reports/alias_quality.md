# Alias Quality Report

Generated at: `2026-09-09T11:26:31Z`

This report classifies `data/aliases.csv` for Natural-Language detection safety.
Identifier aliases remain useful for lookup, but are rejected for mention detection.

## Status Counts

| Status | Rows |
|---|---:|
| reject | 66,667 |
| accept | 58,405 |
| review | 466 |

## Detection Policies

| Policy | Rows |
|---|---:|
| identifier_only | 66,667 |
| safe_natural_language | 58,405 |
| symbol_alias_only | 466 |

## Top Reasons

| Reason | Rows |
|---|---:|
| identifier_alias | 66,667 |
| accepted_name_alias | 58,405 |
| same_as_ticker | 464 |
| exchange_ticker_alias | 2 |

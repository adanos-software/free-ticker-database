# Alias Quality Report

Generated at: `2026-09-11T11:49:35Z`

This report classifies `data/aliases.csv` for Natural-Language detection safety.
Identifier aliases remain useful for lookup, but are rejected for mention detection.

## Status Counts

| Status | Rows |
|---|---:|
| reject | 66,666 |
| accept | 58,424 |
| review | 466 |

## Detection Policies

| Policy | Rows |
|---|---:|
| identifier_only | 66,666 |
| safe_natural_language | 58,424 |
| symbol_alias_only | 466 |

## Top Reasons

| Reason | Rows |
|---|---:|
| identifier_alias | 66,666 |
| accepted_name_alias | 58,424 |
| same_as_ticker | 464 |
| exchange_ticker_alias | 2 |

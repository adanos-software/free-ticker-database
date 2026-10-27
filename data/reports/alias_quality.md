# Alias Quality Report

Generated at: `2026-10-27T12:43:12Z`

This report classifies `data/aliases.csv` for Natural-Language detection safety.
Identifier aliases remain useful for lookup, but are rejected for mention detection.

## Status Counts

| Status | Rows |
|---|---:|
| reject | 68,385 |
| accept | 59,902 |
| review | 480 |

## Detection Policies

| Policy | Rows |
|---|---:|
| identifier_only | 68,385 |
| safe_natural_language | 59,902 |
| symbol_alias_only | 480 |

## Top Reasons

| Reason | Rows |
|---|---:|
| identifier_alias | 68,385 |
| accepted_name_alias | 59,902 |
| same_as_ticker | 478 |
| exchange_ticker_alias | 2 |

# Alias Quality Report

Generated at: `2026-08-01T16:50:26Z`

This report classifies `data/aliases.csv` for Natural-Language detection safety.
Identifier aliases remain useful for lookup, but are rejected for mention detection.

## Status Counts

| Status | Rows |
|---|---:|
| reject | 65,937 |
| accept | 58,567 |
| review | 466 |

## Detection Policies

| Policy | Rows |
|---|---:|
| identifier_only | 65,937 |
| safe_natural_language | 58,567 |
| symbol_alias_only | 466 |

## Top Reasons

| Reason | Rows |
|---|---:|
| identifier_alias | 65,937 |
| accepted_name_alias | 58,567 |
| same_as_ticker | 464 |
| exchange_ticker_alias | 2 |

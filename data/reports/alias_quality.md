# Alias Quality Report

Generated at: `2026-09-28T13:54:33Z`

This report classifies `data/aliases.csv` for Natural-Language detection safety.
Identifier aliases remain useful for lookup, but are rejected for mention detection.

## Status Counts

| Status | Rows |
|---|---:|
| reject | 67,899 |
| accept | 59,596 |
| review | 478 |

## Detection Policies

| Policy | Rows |
|---|---:|
| identifier_only | 67,899 |
| safe_natural_language | 59,596 |
| symbol_alias_only | 478 |

## Top Reasons

| Reason | Rows |
|---|---:|
| identifier_alias | 67,899 |
| accepted_name_alias | 59,596 |
| same_as_ticker | 476 |
| exchange_ticker_alias | 2 |

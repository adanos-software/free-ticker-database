# Alias Quality Report

Generated at: `2026-10-02T12:34:27Z`

This report classifies `data/aliases.csv` for Natural-Language detection safety.
Identifier aliases remain useful for lookup, but are rejected for mention detection.

## Status Counts

| Status | Rows |
|---|---:|
| reject | 67,908 |
| accept | 59,660 |
| review | 477 |

## Detection Policies

| Policy | Rows |
|---|---:|
| identifier_only | 67,908 |
| safe_natural_language | 59,660 |
| symbol_alias_only | 477 |

## Top Reasons

| Reason | Rows |
|---|---:|
| identifier_alias | 67,908 |
| accepted_name_alias | 59,660 |
| same_as_ticker | 475 |
| exchange_ticker_alias | 2 |

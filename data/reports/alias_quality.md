# Alias Quality Report

Generated at: `2026-10-02T13:10:44Z`

This report classifies `data/aliases.csv` for Natural-Language detection safety.
Identifier aliases remain useful for lookup, but are rejected for mention detection.

## Status Counts

| Status | Rows |
|---|---:|
| reject | 67,908 |
| accept | 59,661 |
| review | 477 |

## Detection Policies

| Policy | Rows |
|---|---:|
| identifier_only | 67,908 |
| safe_natural_language | 59,661 |
| symbol_alias_only | 477 |

## Top Reasons

| Reason | Rows |
|---|---:|
| identifier_alias | 67,908 |
| accepted_name_alias | 59,661 |
| same_as_ticker | 475 |
| exchange_ticker_alias | 2 |

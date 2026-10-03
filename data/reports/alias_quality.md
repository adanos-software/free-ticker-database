# Alias Quality Report

Generated at: `2026-10-03T11:41:57Z`

This report classifies `data/aliases.csv` for Natural-Language detection safety.
Identifier aliases remain useful for lookup, but are rejected for mention detection.

## Status Counts

| Status | Rows |
|---|---:|
| reject | 67,908 |
| accept | 59,685 |
| review | 477 |

## Detection Policies

| Policy | Rows |
|---|---:|
| identifier_only | 67,908 |
| safe_natural_language | 59,685 |
| symbol_alias_only | 477 |

## Top Reasons

| Reason | Rows |
|---|---:|
| identifier_alias | 67,908 |
| accepted_name_alias | 59,685 |
| same_as_ticker | 475 |
| exchange_ticker_alias | 2 |

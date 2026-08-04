# Alias Quality Report

Generated at: `2026-08-04T09:55:19Z`

This report classifies `data/aliases.csv` for Natural-Language detection safety.
Identifier aliases remain useful for lookup, but are rejected for mention detection.

## Status Counts

| Status | Rows |
|---|---:|
| reject | 66,050 |
| accept | 58,733 |
| review | 468 |

## Detection Policies

| Policy | Rows |
|---|---:|
| identifier_only | 66,050 |
| safe_natural_language | 58,733 |
| symbol_alias_only | 468 |

## Top Reasons

| Reason | Rows |
|---|---:|
| identifier_alias | 66,050 |
| accepted_name_alias | 58,733 |
| same_as_ticker | 466 |
| exchange_ticker_alias | 2 |

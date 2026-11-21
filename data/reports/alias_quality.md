# Alias Quality Report

Generated at: `2026-11-21T14:40:29Z`

This report classifies `data/aliases.csv` for Natural-Language detection safety.
Identifier aliases remain useful for lookup, but are rejected for mention detection.

## Status Counts

| Status | Rows |
|---|---:|
| reject | 68,467 |
| accept | 60,217 |
| review | 480 |

## Detection Policies

| Policy | Rows |
|---|---:|
| identifier_only | 68,467 |
| safe_natural_language | 60,217 |
| symbol_alias_only | 480 |

## Top Reasons

| Reason | Rows |
|---|---:|
| identifier_alias | 68,467 |
| accepted_name_alias | 60,217 |
| same_as_ticker | 477 |
| exchange_ticker_alias | 3 |

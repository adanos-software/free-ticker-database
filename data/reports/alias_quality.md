# Alias Quality Report

Generated at: `2026-09-08T09:34:40Z`

This report classifies `data/aliases.csv` for Natural-Language detection safety.
Identifier aliases remain useful for lookup, but are rejected for mention detection.

## Status Counts

| Status | Rows |
|---|---:|
| reject | 66,681 |
| accept | 58,381 |
| review | 466 |

## Detection Policies

| Policy | Rows |
|---|---:|
| identifier_only | 66,681 |
| safe_natural_language | 58,381 |
| symbol_alias_only | 466 |

## Top Reasons

| Reason | Rows |
|---|---:|
| identifier_alias | 66,681 |
| accepted_name_alias | 58,381 |
| same_as_ticker | 464 |
| exchange_ticker_alias | 2 |

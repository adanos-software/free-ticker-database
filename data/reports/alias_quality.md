# Alias Quality Report

Generated at: `2026-11-23T20:47:41Z`

This report classifies `data/aliases.csv` for Natural-Language detection safety.
Identifier aliases remain useful for lookup, but are rejected for mention detection.

## Status Counts

| Status | Rows |
|---|---:|
| reject | 68,475 |
| accept | 60,243 |
| review | 480 |

## Detection Policies

| Policy | Rows |
|---|---:|
| identifier_only | 68,475 |
| safe_natural_language | 60,243 |
| symbol_alias_only | 480 |

## Top Reasons

| Reason | Rows |
|---|---:|
| identifier_alias | 68,475 |
| accepted_name_alias | 60,243 |
| same_as_ticker | 477 |
| exchange_ticker_alias | 3 |

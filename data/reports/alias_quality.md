# Alias Quality Report

Generated at: `2026-04-20T17:46:54Z`

This report classifies `data/aliases.csv` for Natural-Language detection safety.
Identifier aliases remain useful for lookup, but are rejected for mention detection.

## Status Counts

| Status | Rows |
|---|---:|
| reject | 53,737 |
| accept | 49,397 |
| review | 365 |

## Detection Policies

| Policy | Rows |
|---|---:|
| identifier_only | 53,737 |
| safe_natural_language | 49,397 |
| symbol_alias_only | 365 |

## Top Reasons

| Reason | Rows |
|---|---:|
| identifier_alias | 53,737 |
| accepted_name_alias | 49,397 |
| same_as_ticker | 365 |

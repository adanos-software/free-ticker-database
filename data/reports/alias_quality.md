# Alias Quality Report

Generated at: `2026-07-21T08:50:56Z`

This report classifies `data/aliases.csv` for Natural-Language detection safety.
Identifier aliases remain useful for lookup, but are rejected for mention detection.

## Status Counts

| Status | Rows |
|---|---:|
| reject | 66,098 |
| accept | 58,528 |
| review | 464 |

## Detection Policies

| Policy | Rows |
|---|---:|
| identifier_only | 66,098 |
| safe_natural_language | 58,528 |
| symbol_alias_only | 464 |

## Top Reasons

| Reason | Rows |
|---|---:|
| identifier_alias | 66,098 |
| accepted_name_alias | 58,528 |
| same_as_ticker | 464 |

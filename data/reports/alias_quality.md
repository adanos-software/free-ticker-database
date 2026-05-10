# Alias Quality Report

Generated at: `2026-05-10T20:22:54Z`

This report classifies `data/aliases.csv` for Natural-Language detection safety.
Identifier aliases remain useful for lookup, but are rejected for mention detection.

## Status Counts

| Status | Rows |
|---|---:|
| reject | 64,361 |
| accept | 56,473 |
| review | 480 |

## Detection Policies

| Policy | Rows |
|---|---:|
| identifier_only | 64,361 |
| safe_natural_language | 56,473 |
| symbol_alias_only | 480 |

## Top Reasons

| Reason | Rows |
|---|---:|
| identifier_alias | 64,361 |
| accepted_name_alias | 56,473 |
| same_as_ticker | 480 |

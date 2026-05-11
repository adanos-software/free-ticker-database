# Alias Quality Report

Generated at: `2026-05-11T06:16:22Z`

This report classifies `data/aliases.csv` for Natural-Language detection safety.
Identifier aliases remain useful for lookup, but are rejected for mention detection.

## Status Counts

| Status | Rows |
|---|---:|
| reject | 64,400 |
| accept | 56,471 |
| review | 480 |

## Detection Policies

| Policy | Rows |
|---|---:|
| identifier_only | 64,400 |
| safe_natural_language | 56,471 |
| symbol_alias_only | 480 |

## Top Reasons

| Reason | Rows |
|---|---:|
| identifier_alias | 64,400 |
| accepted_name_alias | 56,471 |
| same_as_ticker | 480 |

# Alias Quality Report

Generated at: `2026-05-10T06:20:48Z`

This report classifies `data/aliases.csv` for Natural-Language detection safety.
Identifier aliases remain useful for lookup, but are rejected for mention detection.

## Status Counts

| Status | Rows |
|---|---:|
| reject | 63,393 |
| accept | 56,582 |
| review | 479 |

## Detection Policies

| Policy | Rows |
|---|---:|
| identifier_only | 63,393 |
| safe_natural_language | 56,582 |
| symbol_alias_only | 479 |

## Top Reasons

| Reason | Rows |
|---|---:|
| identifier_alias | 63,393 |
| accepted_name_alias | 56,582 |
| same_as_ticker | 479 |

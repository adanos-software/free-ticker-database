# Alias Quality Report

Generated at: `2026-05-10T18:43:13Z`

This report classifies `data/aliases.csv` for Natural-Language detection safety.
Identifier aliases remain useful for lookup, but are rejected for mention detection.

## Status Counts

| Status | Rows |
|---|---:|
| reject | 64,134 |
| accept | 56,582 |
| review | 479 |

## Detection Policies

| Policy | Rows |
|---|---:|
| identifier_only | 64,134 |
| safe_natural_language | 56,582 |
| symbol_alias_only | 479 |

## Top Reasons

| Reason | Rows |
|---|---:|
| identifier_alias | 64,134 |
| accepted_name_alias | 56,582 |
| same_as_ticker | 479 |

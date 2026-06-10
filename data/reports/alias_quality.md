# Alias Quality Report

Generated at: `2026-06-10T14:56:22Z`

This report classifies `data/aliases.csv` for Natural-Language detection safety.
Identifier aliases remain useful for lookup, but are rejected for mention detection.

## Status Counts

| Status | Rows |
|---|---:|
| reject | 64,776 |
| accept | 56,808 |
| review | 424 |

## Detection Policies

| Policy | Rows |
|---|---:|
| identifier_only | 64,776 |
| safe_natural_language | 56,808 |
| symbol_alias_only | 424 |

## Top Reasons

| Reason | Rows |
|---|---:|
| identifier_alias | 64,776 |
| accepted_name_alias | 56,808 |
| same_as_ticker | 424 |

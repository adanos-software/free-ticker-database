# Alias Quality Report

Generated at: `2026-04-17T17:06:26Z`

This report classifies `data/aliases.csv` for Natural-Language detection safety.
Identifier aliases remain useful for lookup, but are rejected for mention detection.

## Status Counts

| Status | Rows |
|---|---:|
| reject | 53,723 |
| accept | 49,402 |
| review | 365 |

## Detection Policies

| Policy | Rows |
|---|---:|
| identifier_only | 53,723 |
| safe_natural_language | 49,402 |
| symbol_alias_only | 365 |

## Top Reasons

| Reason | Rows |
|---|---:|
| identifier_alias | 53,723 |
| accepted_name_alias | 49,402 |
| same_as_ticker | 365 |

# B3 ETF Category Apply Report

Generated at: `2026-05-25T05:50:19Z`

This report applies only exact official B3 listed-ETF category mismatches. It does not infer categories from names or ticker shape.

## Summary

- Rows: `40`
- Applied: `True`
- Written updates: `0`

## Decisions

| Decision | Rows |
|---|---:|
| skip | 40 |

## Category Updates

| ETF category | Rows |
|---|---:|

## Policy

- Only official B3 listed-ETF rows with one canonical candidate category are eligible.
- The report writes only `etf_category` metadata overrides; it does not change ISINs, names, symbols, or stock sectors.

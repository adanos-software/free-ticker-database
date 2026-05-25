# Canada FIGI Apply Report

Generated at: `2026-05-25T05:46:48Z`

This report applies accepted Canada OpenFIGI probe rows only after listing-key, ISIN, exchange-hint, and collision gates.

## Summary

- Rows: `68`
- Applied: `False`
- Applied rows: `0`
- OpenFIGI no-match source gaps added: `0`

## Decisions

| Decision | Rows |
|---|---:|
| skip | 68 |

## Reasons

| Reason | Rows |
|---|---:|
| identifier_figi_already_set_to_same_value | 68 |

## Policy

- No FIGI is inferred from symbol or issuer name.
- OpenFIGI no-match rows are recorded as source gaps, not filled values.
- Rows rejected by any gate remain unchanged.

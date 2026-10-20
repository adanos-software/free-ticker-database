# SEC listed-current comparison

Report only. No listing or identifier changes.

| Metric | Value |
| --- | ---: |
| adanos_only_us_listed | 5786 |
| ambiguous_venue_review | 5 |
| matched_listed_current | 5511 |
| sec_only_missing_candidate | 1632 |
| sec_only_scope_review | 497 |
| sec_out_of_scope_venue | 2798 |

## Sources

- https://www.sec.gov/files/company_tickers_exchange.json; SHA-256 `6c29e2e3db973d4479875164603c2434f81ad22e14c9f1694999aca9cca246cb`; last-modified `Fri, 16 Oct 2026 11:53:51 GMT`.

## Limits

- SEC NYSE may mean NYSE American; ambiguous ticker-plus-venue matches stay open.
- Scope candidates require review. No bare-ticker join, ISIN claims, identity writes, or delisting inference.
- SEC citation and access policy: https://www.sec.gov/os/accessing-edgar-data

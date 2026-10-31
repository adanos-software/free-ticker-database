# ISIN Identity Collision Review Queue

Generated: `2026-10-31T08:28:19Z`

Policy: an ISIN identifies exactly one issuer. This report flags ISINs shared by distinct issuer names (a provable anomaly) and applies no ISIN, country, or name change without official listing-keyed evidence.

## Summary

| Metric | Value |
| --- | ---: |
| Collision groups | 129 |
| Listings involved | 377 |
| Ticker-collision groups | 35 |
| Open groups | 4 |
| Closed same-issuer reviewed | 125 |
| Direct identifier apply allowed rows | 0 |

## Decision Candidates

| Decision candidate | Groups |
| --- | ---: |
| isin_shared_by_distinct_issuers | 94 |
| ticker_collision_isin_misassignment_suspected | 35 |

## Next Review Batches

| Decision candidate | Registered country | Groups |
| --- | --- | ---: |
| isin_shared_by_distinct_issuers | United States | 26 |
| ticker_collision_isin_misassignment_suspected | United States | 14 |
| ticker_collision_isin_misassignment_suspected | Germany | 9 |
| isin_shared_by_distinct_issuers | Singapore | 8 |
| isin_shared_by_distinct_issuers | Sweden | 6 |
| isin_shared_by_distinct_issuers | Cayman Islands | 5 |
| isin_shared_by_distinct_issuers | France | 5 |
| ticker_collision_isin_misassignment_suspected | India | 5 |
| isin_shared_by_distinct_issuers | Canada | 4 |
| isin_shared_by_distinct_issuers | Germany | 4 |
| isin_shared_by_distinct_issuers | Switzerland | 4 |
| isin_shared_by_distinct_issuers | Australia | 3 |
| isin_shared_by_distinct_issuers | China | 3 |
| isin_shared_by_distinct_issuers | Finland | 3 |
| isin_shared_by_distinct_issuers | Italy | 3 |
| isin_shared_by_distinct_issuers | Mexico | 3 |
| isin_shared_by_distinct_issuers | Belgium | 2 |
| isin_shared_by_distinct_issuers | Bermuda | 2 |
| isin_shared_by_distinct_issuers | Japan | 2 |
| isin_shared_by_distinct_issuers | Netherlands | 2 |
| ticker_collision_isin_misassignment_suspected | Canada | 2 |
| ticker_collision_isin_misassignment_suspected | United Kingdom | 2 |
| isin_shared_by_distinct_issuers | Austria | 1 |
| isin_shared_by_distinct_issuers | Denmark | 1 |
| isin_shared_by_distinct_issuers | Hungary | 1 |

## Highest-Risk Groups

| ISIN | Registered country | Listings | Shared tickers | Names |
| --- | --- | ---: | --- | --- |
| AU000000CRB1 | Australia | 2 | CRB | Carbine Resources Ltd \| Lyxor Commodities Refinitiv/CoreCommodity CRB TR UCITS ETF - Acc-EUR |
| CA41752L1076 | Canada | 2 | HBIE | Hai Jia International Limited Company \| Harvest Balanced Income & Growth Enhanced ETF Class A |
| CA5910881096 | Canada | 2 | MERG | Merger Mines Corporation \| Metal Energy Corp |
| INE534A01028 | India | 2 | - | Grand Foundry Ltd \| Tikona Communication Limited |

## Gate

- Do not change, blank, or reassign any ISIN, country, or name until official listing-keyed identifier evidence (national numbering agency / issuer / exchange security master) confirms which listing holds the ISIN.
- Recommended next source: Official national numbering agency (ISIN registry) or issuer/exchange security master keyed to the exact listing_key.
- Direct identifier apply allowed rows: `0`.

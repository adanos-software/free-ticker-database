# ISIN Identity Collision Review Queue

Generated: `2026-10-08T07:44:21Z`

Policy: an ISIN identifies exactly one issuer. This report flags ISINs shared by distinct issuer names (a provable anomaly) and applies no ISIN, country, or name change without official listing-keyed evidence.

## Summary

| Metric | Value |
| --- | ---: |
| Collision groups | 127 |
| Listings involved | 373 |
| Ticker-collision groups | 36 |
| Open groups | 9 |
| Closed same-issuer reviewed | 118 |
| Direct identifier apply allowed rows | 0 |

## Decision Candidates

| Decision candidate | Groups |
| --- | ---: |
| isin_shared_by_distinct_issuers | 91 |
| ticker_collision_isin_misassignment_suspected | 36 |

## Next Review Batches

| Decision candidate | Registered country | Groups |
| --- | --- | ---: |
| isin_shared_by_distinct_issuers | United States | 23 |
| ticker_collision_isin_misassignment_suspected | United States | 13 |
| ticker_collision_isin_misassignment_suspected | Germany | 9 |
| isin_shared_by_distinct_issuers | Singapore | 8 |
| isin_shared_by_distinct_issuers | Sweden | 7 |
| isin_shared_by_distinct_issuers | France | 6 |
| isin_shared_by_distinct_issuers | Cayman Islands | 5 |
| ticker_collision_isin_misassignment_suspected | India | 5 |
| isin_shared_by_distinct_issuers | Switzerland | 4 |
| isin_shared_by_distinct_issuers | Australia | 3 |
| isin_shared_by_distinct_issuers | Canada | 3 |
| isin_shared_by_distinct_issuers | China | 3 |
| isin_shared_by_distinct_issuers | Finland | 3 |
| isin_shared_by_distinct_issuers | Germany | 3 |
| isin_shared_by_distinct_issuers | Italy | 3 |
| isin_shared_by_distinct_issuers | Mexico | 3 |
| ticker_collision_isin_misassignment_suspected | Australia | 3 |
| ticker_collision_isin_misassignment_suspected | Canada | 3 |
| isin_shared_by_distinct_issuers | Belgium | 2 |
| isin_shared_by_distinct_issuers | Bermuda | 2 |
| isin_shared_by_distinct_issuers | Japan | 2 |
| isin_shared_by_distinct_issuers | Norway | 2 |
| isin_shared_by_distinct_issuers | Austria | 1 |
| isin_shared_by_distinct_issuers | Denmark | 1 |
| isin_shared_by_distinct_issuers | Hungary | 1 |

## Highest-Risk Groups

| ISIN | Registered country | Listings | Shared tickers | Names |
| --- | --- | ---: | --- | --- |
| AU000000TLG7 | Australia | 5 | TLG | TLG Immobilien AG \| Talga Group Ltd |
| CA3025862010 | Canada | 3 | FP | FP Newspapers Inc \| Fondul Proprietatea S.A. GDR |
| AU000000CRB1 | Australia | 2 | CRB | Carbine Resources Ltd \| Lyxor Commodities Refinitiv/CoreCommodity CRB TR UCITS ETF - Acc-EUR |
| AU0000253502 | Australia | 2 | RGN | Region Group \| Rush Gold Corp |
| CA41752L1076 | Canada | 2 | HBIE | Hai Jia International Limited Company \| Harvest Balanced Income & Growth Enhanced ETF Class A |
| CA5910881096 | Canada | 2 | MERG | Merger Mines Corporation \| Metal Energy Corp |
| US0896951003 | United States | 2 | BIGG | BIGG Digital Assets Inc. \| Big Tree Group Inc |
| NO0010861990 | Norway | 2 | - | Prism Resources Inc. \| Prosafe SE |
| SE0000722365 | Sweden | 2 | - | Vivesto AB \| Viveve Medical Inc |

## Gate

- Do not change, blank, or reassign any ISIN, country, or name until official listing-keyed identifier evidence (national numbering agency / issuer / exchange security master) confirms which listing holds the ISIN.
- Recommended next source: Official national numbering agency (ISIN registry) or issuer/exchange security master keyed to the exact listing_key.
- Direct identifier apply allowed rows: `0`.

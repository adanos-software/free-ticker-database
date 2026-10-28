# ISIN Identity Collision Review Queue

Generated: `2026-10-28T14:19:42Z`

Policy: an ISIN identifies exactly one issuer. This report flags ISINs shared by distinct issuer names (a provable anomaly) and applies no ISIN, country, or name change without official listing-keyed evidence.

## Summary

| Metric | Value |
| --- | ---: |
| Collision groups | 133 |
| Listings involved | 389 |
| Ticker-collision groups | 38 |
| Open groups | 16 |
| Closed same-issuer reviewed | 117 |
| Direct identifier apply allowed rows | 0 |

## Decision Candidates

| Decision candidate | Groups |
| --- | ---: |
| isin_shared_by_distinct_issuers | 95 |
| ticker_collision_isin_misassignment_suspected | 38 |

## Next Review Batches

| Decision candidate | Registered country | Groups |
| --- | --- | ---: |
| isin_shared_by_distinct_issuers | United States | 25 |
| ticker_collision_isin_misassignment_suspected | United States | 15 |
| ticker_collision_isin_misassignment_suspected | Germany | 9 |
| isin_shared_by_distinct_issuers | Singapore | 8 |
| isin_shared_by_distinct_issuers | Sweden | 7 |
| isin_shared_by_distinct_issuers | Cayman Islands | 5 |
| isin_shared_by_distinct_issuers | France | 5 |
| ticker_collision_isin_misassignment_suspected | India | 5 |
| isin_shared_by_distinct_issuers | Canada | 4 |
| isin_shared_by_distinct_issuers | Switzerland | 4 |
| isin_shared_by_distinct_issuers | Australia | 3 |
| isin_shared_by_distinct_issuers | China | 3 |
| isin_shared_by_distinct_issuers | Finland | 3 |
| isin_shared_by_distinct_issuers | Germany | 3 |
| isin_shared_by_distinct_issuers | Italy | 3 |
| isin_shared_by_distinct_issuers | Mexico | 3 |
| ticker_collision_isin_misassignment_suspected | Australia | 3 |
| isin_shared_by_distinct_issuers | Belgium | 2 |
| isin_shared_by_distinct_issuers | Bermuda | 2 |
| isin_shared_by_distinct_issuers | Japan | 2 |
| isin_shared_by_distinct_issuers | Netherlands | 2 |
| isin_shared_by_distinct_issuers | Norway | 2 |
| ticker_collision_isin_misassignment_suspected | Canada | 2 |
| ticker_collision_isin_misassignment_suspected | United Kingdom | 2 |
| isin_shared_by_distinct_issuers | Austria | 1 |

## Highest-Risk Groups

| ISIN | Registered country | Listings | Shared tickers | Names |
| --- | --- | ---: | --- | --- |
| AU000000TLG7 | Australia | 5 | TLG | TLG Immobilien AG \| Talga Group Ltd |
| US0268747849 | United States | 4 | AINN | AMER.INTL GRP NEW DL 2,50 \| American International Group Inc. |
| US6153691059 | United States | 4 | DUT | MOODYS CORP       DL-,01 \| Moody's Corporation |
| GB00BNGDN821 | United Kingdom | 3 | 27M | MELR IN PLC LS-,001 \| Melrose Industries PLC |
| AU000000CRB1 | Australia | 2 | CRB | Carbine Resources Ltd \| Lyxor Commodities Refinitiv/CoreCommodity CRB TR UCITS ETF - Acc-EUR |
| AU0000253502 | Australia | 2 | RGN | Region Group \| Rush Gold Corp |
| CA41752L1076 | Canada | 2 | HBIE | Hai Jia International Limited Company \| Harvest Balanced Income & Growth Enhanced ETF Class A |
| CA5910881096 | Canada | 2 | MERG | Merger Mines Corporation \| Metal Energy Corp |
| US0896951003 | United States | 2 | BIGG | BIGG Digital Assets Inc. \| Big Tree Group Inc |
| CA9609085076 | Canada | 3 | - | Aetherion Technologies Inc. \| Westport Fuel Systems Inc. |
| INE534A01028 | India | 3 | - | Grand Foundry Ltd \| Tikona Communication Limited |
| NL0000313286 | Netherlands | 2 | - | ACOMO NV           EO-,45 \| Amsterdam Commodities NV |
| NO0010861990 | Norway | 2 | - | Prism Resources Inc. \| Prosafe SE |
| SE0000722365 | Sweden | 2 | - | Vivesto AB \| Viveve Medical Inc |
| US34631F1021 | United States | 2 | - | FOR.PWR.SOL.INC. O.N. \| Forgent Power Solutions, Inc. |
| US7789203061 | United States | 2 | - | SHA.HO.INC. O.N. \| SharonAI Holdings, Inc. Class A Common Stock |

## Gate

- Do not change, blank, or reassign any ISIN, country, or name until official listing-keyed identifier evidence (national numbering agency / issuer / exchange security master) confirms which listing holds the ISIN.
- Recommended next source: Official national numbering agency (ISIN registry) or issuer/exchange security master keyed to the exact listing_key.
- Direct identifier apply allowed rows: `0`.

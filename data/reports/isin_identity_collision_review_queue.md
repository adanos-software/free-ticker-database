# ISIN Identity Collision Review Queue

Generated: `2026-11-24T10:01:56Z`

Policy: an ISIN identifies exactly one issuer. This report flags ISINs shared by distinct issuer names (a provable anomaly) and applies no ISIN, country, or name change without official listing-keyed evidence.

## Summary

| Metric | Value |
| --- | ---: |
| Collision groups | 122 |
| Listings involved | 352 |
| Ticker-collision groups | 31 |
| Open groups | 11 |
| Closed same-issuer reviewed | 111 |
| Direct identifier apply allowed rows | 0 |

## Decision Candidates

| Decision candidate | Groups |
| --- | ---: |
| isin_shared_by_distinct_issuers | 91 |
| ticker_collision_isin_misassignment_suspected | 31 |

## Next Review Batches

| Decision candidate | Registered country | Groups |
| --- | --- | ---: |
| isin_shared_by_distinct_issuers | United States | 25 |
| ticker_collision_isin_misassignment_suspected | United States | 13 |
| isin_shared_by_distinct_issuers | Singapore | 8 |
| ticker_collision_isin_misassignment_suspected | Germany | 7 |
| isin_shared_by_distinct_issuers | Germany | 6 |
| isin_shared_by_distinct_issuers | Sweden | 6 |
| isin_shared_by_distinct_issuers | Cayman Islands | 5 |
| ticker_collision_isin_misassignment_suspected | India | 5 |
| isin_shared_by_distinct_issuers | France | 4 |
| isin_shared_by_distinct_issuers | Switzerland | 4 |
| isin_shared_by_distinct_issuers | Australia | 3 |
| isin_shared_by_distinct_issuers | Canada | 3 |
| isin_shared_by_distinct_issuers | China | 3 |
| isin_shared_by_distinct_issuers | Italy | 3 |
| isin_shared_by_distinct_issuers | Mexico | 3 |
| isin_shared_by_distinct_issuers | Bermuda | 2 |
| isin_shared_by_distinct_issuers | India | 2 |
| isin_shared_by_distinct_issuers | Israel | 2 |
| isin_shared_by_distinct_issuers | Japan | 2 |
| isin_shared_by_distinct_issuers | Netherlands | 2 |
| ticker_collision_isin_misassignment_suspected | Canada | 2 |
| ticker_collision_isin_misassignment_suspected | United Kingdom | 2 |
| isin_shared_by_distinct_issuers | Belgium | 1 |
| isin_shared_by_distinct_issuers | Denmark | 1 |
| isin_shared_by_distinct_issuers | Finland | 1 |

## Highest-Risk Groups

| ISIN | Registered country | Listings | Shared tickers | Names |
| --- | --- | ---: | --- | --- |
| AU000000CRB1 | Australia | 2 | CRB | Carbine Resources Ltd \| Lyxor Commodities Refinitiv/CoreCommodity CRB TR UCITS ETF - Acc-EUR |
| CA41752L1076 | Canada | 2 | HBIE | Hai Jia International Limited Company \| Harvest Balanced Income & Growth Enhanced ETF Class A |
| CA5910881096 | Canada | 2 | MERG | Merger Mines Corporation \| Metal Energy Corp |
| DE000A40ZVU2 | Germany | 5 | - | UMT UTD MOB.TECHN.AG \| WAYDEAR AG |
| US5049221055 | United States | 5 | - | Labcorp Holdings Inc. \| Laboratory Corp. of America Holdings |
| GRS298343005 | Greece | 3 | - | HELLENiQ ENERGY Holdings S.A. \| Hellenic Petroleum S.A |
| HK1097008929 | Hong Kong | 3 | - | CTF Media & Entertainment Limited \| i-CABLE Communications Limited |
| INE234A01025 | India | 3 | - | Evonile Pharma Limited \| Novartis India Ltd |
| DE000A12UPJ7 | Germany | 2 | - | SCP Standard Capital Partners AG \| VANEA AG INH O.N. |
| IL0011313900 | Israel | 2 | - | Eocene Ltd. - Ordinary Shares \| Galmed Pharmaceuticals Ltd |
| INE534A01028 | India | 2 | - | Grand Foundry Ltd \| Tikona Communication Limited |

## Gate

- Do not change, blank, or reassign any ISIN, country, or name until official listing-keyed identifier evidence (national numbering agency / issuer / exchange security master) confirms which listing holds the ISIN.
- Recommended next source: Official national numbering agency (ISIN registry) or issuer/exchange security master keyed to the exact listing_key.
- Direct identifier apply allowed rows: `0`.

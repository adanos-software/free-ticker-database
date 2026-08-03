# FinanceDatabase ISIN coverage review

This report records the second-source review for the exact-ISIN candidates from the FinanceDatabase reconciliation.

## Decision

- 46 listing rows were reviewed across 44 unique ISINs.
- 42 exact target-venue rows are classified as Common Stock by OpenFIGI.
- 1 row (LSE::DLN) is classified as an equity-like REIT and is retained as Stock with an explicit review decision.
- 3 rows are classified as Closed-End Funds and remain blocked: LSE::ATR, LSE::CRS, and LSE::PIN.
- The 43 approved rows were added to data/coverage_expansion_listings.csv.
- 33 of the approved candidates reuse a ticker globally; the coverage-expansion path keeps those rows in listings.csv without letting them silently replace the primary tickers.csv owner.

## Metadata policy

- Country and ISO code come from the issuer country encoded by the ISIN prefix, not from the venue.
- stock_sector is blank in all 43 coverage source rows. The normal rebuild retains two existing official sector fallbacks (HEL::LEHTO and TPEX::5348); no sector is inferred from a name, venue, or OpenFIGI security type.
- Aliases remain empty in the source rows; the normal rebuild pipeline derives safe aliases.

## Venue coverage

| Venue | Approved rows | Blocked rows |
| --- | ---: | ---: |
| BSE_IN | 3 | 0 |
| CPH | 1 | 0 |
| Euronext | 1 | 0 |
| HEL | 1 | 0 |
| HKEX | 14 | 0 |
| LSE | 14 | 3 |
| OSL | 3 | 0 |
| SGX | 1 | 0 |
| TASE | 2 | 0 |
| TPEX | 1 | 0 |
| XETRA | 2 | 0 |

Evidence source metadata and row-level source URLs, FIGIs, collision flags, and decisions are in the companion CSV/JSON files. The OpenFIGI input hash is adec08e56327e2dfa3bbbd1078c1fe34e0439fe3e8a461cba871dbe1c2ccdf14; official exchange references were frozen at 2026-07-29T09:29:29Z.

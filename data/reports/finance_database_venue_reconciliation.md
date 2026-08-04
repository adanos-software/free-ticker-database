# FinanceDatabase venue reconciliation

This is a read-only, review-gated reconciliation. It does not modify canonical exports, supplemental listings, or reference data.

- FinanceDatabase commit: `8c3ac1eea116436ec565da54b3d116edcf9a02ac`
- FinanceDatabase equities rows: `112572`
- Candidate rows: `1051` (same_isin=3, same_name=119, same_ticker_name=929)
- Selected venue-review rows: `932`
- Official CFI present: `0`
- Apply performed: `False`

## Venue summary

| Venue | Ticker/name | ISIN-exact | Total | Security-type review | Asset-type conflict | Name signal | Existing other type | Same ISIN elsewhere |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| ASX | 89 | 0 | 89 | 89 | 0 | 0 | 0 | 0 |
| BSE_IN | 95 | 0 | 95 | 95 | 0 | 0 | 0 | 6 |
| Bursa | 46 | 0 | 46 | 46 | 0 | 0 | 0 | 0 |
| CPH | 1 | 0 | 1 | 1 | 0 | 0 | 0 | 0 |
| Euronext | 5 | 0 | 5 | 5 | 0 | 0 | 0 | 0 |
| HEL | 1 | 0 | 1 | 1 | 0 | 0 | 0 | 0 |
| IDX | 64 | 0 | 64 | 64 | 0 | 0 | 0 | 0 |
| KOSDAQ | 29 | 0 | 29 | 29 | 0 | 0 | 0 | 0 |
| KRX | 16 | 0 | 16 | 16 | 0 | 0 | 0 | 0 |
| LSE | 51 | 3 | 54 | 48 | 6 | 0 | 0 | 0 |
| NASDAQ | 30 | 0 | 30 | 26 | 0 | 4 | 0 | 0 |
| NYSE | 148 | 0 | 148 | 19 | 121 | 3 | 5 | 0 |
| OSL | 1 | 0 | 1 | 1 | 0 | 0 | 0 | 0 |
| OTC | 32 | 0 | 32 | 32 | 0 | 0 | 0 | 0 |
| PSE_CZ | 1 | 0 | 1 | 1 | 0 | 0 | 0 | 0 |
| QSE | 1 | 0 | 1 | 1 | 0 | 0 | 0 | 0 |
| SET | 183 | 0 | 183 | 183 | 0 | 0 | 0 | 0 |
| SGX | 21 | 0 | 21 | 21 | 0 | 0 | 0 | 0 |
| SIX | 1 | 0 | 1 | 1 | 0 | 0 | 0 | 0 |
| STO | 12 | 0 | 12 | 12 | 0 | 0 | 0 | 1 |
| TADAWUL | 12 | 0 | 12 | 12 | 0 | 0 | 0 | 0 |
| TASE | 21 | 0 | 21 | 21 | 0 | 0 | 0 | 0 |
| TSE | 22 | 0 | 22 | 22 | 0 | 0 | 0 | 0 |
| TSX | 5 | 0 | 5 | 5 | 0 | 0 | 0 | 0 |
| TSXV | 30 | 0 | 30 | 30 | 0 | 0 | 0 | 0 |
| XETRA | 12 | 0 | 12 | 12 | 0 | 0 | 0 | 6 |

No row is authorized for import from ticker/name/ISIN alone. Each security-type review requires exact venue, issuer, CFI, OpenFIGI, or equivalent official evidence.

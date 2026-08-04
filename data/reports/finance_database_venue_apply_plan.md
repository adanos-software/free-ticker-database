# FinanceDatabase venue expansion apply plan

This plan is review-gated. It is generated from the venue reconciliation and a frozen OpenFIGI probe.

- Plan rows: `932`
- Rows authorized by the evidence gate: `604`
- Rows written to coverage expansion in this run: `604`
- Apply performed: `True`
- OpenFIGI probe SHA-256: `64a3abb0ed5110e31ee82dfe9a2ae846547526b2fdd392d8b9cecb4cb070a1c3`
- OpenFIGI probe errors: `0`

| Venue | Add | Already covered/local | OpenFIGI name mismatch | No Common Stock | Other blocked |
| --- | ---: | ---: | ---: | ---: | ---: |
| ASX | 83 | 0 | 5 | 1 | 0 |
| BSE_IN | 92 | 0 | 3 | 0 | 0 |
| Bursa | 0 | 0 | 40 | 6 | 0 |
| CPH | 1 | 0 | 0 | 0 | 0 |
| Euronext | 3 | 0 | 0 | 2 | 0 |
| HEL | 1 | 0 | 0 | 0 | 0 |
| IDX | 62 | 0 | 2 | 0 | 0 |
| KOSDAQ | 27 | 0 | 0 | 2 | 0 |
| KRX | 16 | 0 | 0 | 0 | 0 |
| LSE | 35 | 0 | 6 | 7 | 6 |
| NASDAQ | 0 | 0 | 0 | 4 | 26 |
| NYSE | 2 | 0 | 0 | 1 | 145 |
| OSL | 1 | 0 | 0 | 0 | 0 |
| OTC | 18 | 0 | 1 | 9 | 4 |
| PSE_CZ | 1 | 0 | 0 | 0 | 0 |
| QSE | 1 | 0 | 0 | 0 | 0 |
| SET | 144 | 0 | 29 | 10 | 0 |
| SGX | 21 | 0 | 0 | 0 | 0 |
| SIX | 1 | 0 | 0 | 0 | 0 |
| STO | 11 | 0 | 0 | 0 | 1 |
| TADAWUL | 8 | 0 | 0 | 4 | 0 |
| TASE | 21 | 0 | 0 | 0 | 0 |
| TSE | 17 | 0 | 4 | 1 | 0 |
| TSX | 1 | 0 | 4 | 0 | 0 |
| TSXV | 28 | 0 | 2 | 0 | 0 |
| XETRA | 9 | 0 | 3 | 0 | 0 |

Only the `add_coverage_expansion` rows are written. The output remains collision-safe and does not replace primary ticker owners.

# Twelve Data Stock Compare

- Twelve Data rows: 190,193
- Twelve Data stock-like rows: 166,185
- Local listing rows: 71,043
- Matched Twelve Data rows: 45,963
- Matched local listing keys: 45,961
- Unmatched Twelve Data rows: 144,230
- Unmatched Twelve Data stock-like rows: 120,347
- Unmatched local listing rows: 25,082
- Low name-similarity matched rows: 7,454
- FIGI disagreements where both sides have a FIGI: 30,102

Twelve Data ISIN and CUSIP values in this snapshot are add-on placeholders, so this audit does not use them as identity evidence.
FIGI disagreements are review-only because providers may expose different FIGI levels for the same listed security.

## Top Unmatched Twelve Data Stock-Like Exchanges

| Exchange | Rows |
| --- | ---: |
| XSTU | 18,326 |
| FSX | 14,384 |
| XDUS | 12,696 |
| Munich | 12,048 |
| MTA | 10,799 |
| VSE | 8,667 |
| CBOE | 3,069 |
| NSE | 2,965 |
| OTC | 2,911 |
| BSE | 2,573 |
| XHAN | 2,323 |
| NEO | 2,188 |
| CXA | 1,637 |
| SET | 1,631 |
| BMV | 1,511 |
| Bovespa | 1,492 |
| JPX | 1,202 |
| BCBA | 1,142 |
| ASX | 1,133 |
| LSE | 1,049 |

## Top Unmatched Local Exchanges

| Exchange | Rows |
| --- | ---: |
| LSE | 3,062 |
| XETRA | 2,992 |
| NYSE ARCA | 2,650 |
| TSX | 1,629 |
| NASDAQ | 1,288 |
| BATS | 1,230 |
| OTC | 1,123 |
| HKEX | 1,090 |
| KRX | 970 |
| B3 | 889 |
| NSE_IN | 636 |
| SIX | 575 |
| SSE | 534 |
| ASX | 434 |
| TSE | 393 |
| TASE | 391 |
| SZSE | 359 |
| Euronext | 328 |
| STO | 311 |
| CSE_LK | 307 |

## Output Files

- `data/reports/twelvedata_stock_compare_summary.json`
- `data/reports/twelvedata_missing_stock_like.csv`
- `data/reports/twelvedata_missing_all.csv`
- `data/reports/twelvedata_local_unmatched.csv`
- `data/reports/twelvedata_name_mismatches.csv`
- `data/reports/twelvedata_figi_mismatches.csv`

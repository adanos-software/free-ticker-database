# OpenFIGI ISIN/MIC remap audit

Report only. No listing or identifier changes.

| Metric | Value |
| --- | ---: |
| ambiguous_mapping | 4 |
| mapped | 128 |
| not_probed | 95829 |
| ticker_not_resolved | 27 |
| unmapped | 43 |
| eligible_listings | 96031 |
| unique_isin_mic_requests | 93878 |
| cached_requests | 200 |
| stolen_isin_review_candidates | 42 |

## Sources

- https://api.openfigi.com/v3/mapping; SHA-256 `46d760751a73fde088452684a12017afd3a42803953f5e2b0cee0eda4e0298f8`; last-modified `not supplied`.

## Limits

- Probe order is the SHA-256 of the exact request. --limit bounds new requests; zero fetches the complete eligible universe.
- Only valid ISIN plus mapped MIC is sent. Unlisted equities are excluded. Cached jobs can be resumed.
- FIGI prefixes do not establish a level. Composite and share-class IDs are classified separately from venue FIGIs.
- Unknown local levels remain unresolved. Name-family differences are review candidates, never proven stolen ISINs.
- Unprobed listings are explicit and excluded from accuracy or API-failure claims. OpenFIGI descriptions are not adopted.

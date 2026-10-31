# OpenFIGI ISIN/MIC remap audit

Report only. No listing or identifier changes.

| Metric | Value |
| --- | ---: |
| ambiguous_mapping | 2482 |
| mapped | 60891 |
| ticker_not_resolved | 12732 |
| unmapped | 20261 |
| eligible_listings | 96366 |
| unique_isin_mic_requests | 94275 |
| cached_requests | 94275 |
| response_with_data | 74382 |
| response_with_warning | 19893 |
| stolen_isin_review_candidates | 20625 |
| stolen_isin_review_candidates_before_names_match_tightening | 21961 |
| cache_tarball_sha256 | a374409bdd317d455560fe59c792dfe089a4c944d96524156f04c58a27b0e012 |
| repo_main_sha_at_fetch | 55c95a410485b557c72a55e40983ff6ff8f18d75 |
| openfigi_fetch_window_utc | 2026-10-31T06:19:58Z/2026-10-31T06:32:00Z |

## Sources

- https://api.openfigi.com/v3/mapping; SHA-256 `e4a99843aff8c87fee3dd76f394a3c17ecabb0d00a66f9de4d802d1f9ae26b4c`; last-modified `not supplied`.

## Limits

- Probe order is the SHA-256 of the exact request. --limit bounds new requests; zero fetches the complete eligible universe.
- Only valid ISIN plus mapped MIC is sent. Unlisted equities are excluded. Cached jobs can be resumed.
- FIGI prefixes do not establish a level. Composite and share-class IDs are classified separately from venue FIGIs.
- Unknown local levels remain unresolved. Name-family differences are review candidates, never proven stolen ISINs.
- Unprobed listings are explicit and excluded from accuracy or API-failure claims. OpenFIGI descriptions are not adopted.

## FIGI level agreement (mapped rows only)

| Level | Count |
| --- | ---: |
| local_composite_level | 26450 |
| local_figi_missing | 21731 |
| local_level_unknown_or_disagreement | 1091 |
| venue_match | 11619 |

## Cache manifest

- mode: `external_cache`
- request_count: `94275`
- cached_requests: `94275`
- cache_dir: `data/openfigi_verification/isin_mic_remap_cache`
- cache_tarball_sha256: `a374409bdd317d455560fe59c792dfe089a4c944d96524156f04c58a27b0e012`
- committed_probe_path: `data/reports/openfigi_isin_mic_remap_probe.json`
- committed_probe_sha256: `46d760751a73fde088452684a12017afd3a42803953f5e2b0cee0eda4e0298f8`
- note: `Full probe not committed; replay uses extracted cache or the pinned 200-request probe.`

## Status by exchange (top 15 by eligible rows)

- `OTC`: {'unmapped': 11067}
- `FSX`: {'ambiguous_mapping': 4, 'mapped': 7772, 'ticker_not_resolved': 40, 'unmapped': 326}
- `LSE`: {'ambiguous_mapping': 763, 'mapped': 6039, 'ticker_not_resolved': 104, 'unmapped': 116}
- `XETRA`: {'ambiguous_mapping': 46, 'mapped': 4903, 'ticker_not_resolved': 236, 'unmapped': 350}
- `BSE_IN`: {'ambiguous_mapping': 31, 'mapped': 1531, 'ticker_not_resolved': 3373, 'unmapped': 115}
- `NASDAQ`: {'unmapped': 4747}
- `TSE`: {'ambiguous_mapping': 79, 'mapped': 3997, 'unmapped': 61}
- `NSE_IN`: {'ambiguous_mapping': 17, 'mapped': 1290, 'ticker_not_resolved': 2078, 'unmapped': 119}
- `HKEX`: {'mapped': 108, 'ticker_not_resolved': 3091, 'unmapped': 40}
- `SZSE`: {'mapped': 3126, 'unmapped': 12}
- `XSTU`: {'ambiguous_mapping': 142, 'mapped': 2558, 'ticker_not_resolved': 15, 'unmapped': 58}
- `SSE`: {'mapped': 2745, 'unmapped': 16}
- `NYSE ARCA`: {'mapped': 2417, 'ticker_not_resolved': 31, 'unmapped': 257}
- `TSX`: {'ambiguous_mapping': 6, 'mapped': 1816, 'ticker_not_resolved': 238, 'unmapped': 235}
- `ASX`: {'ambiguous_mapping': 38, 'mapped': 1998, 'ticker_not_resolved': 7, 'unmapped': 118}

## Status by operating MIC (top 15)

- `OTCM`: {'unmapped': 11067}
- `XFRA`: {'ambiguous_mapping': 4, 'mapped': 7772, 'ticker_not_resolved': 40, 'unmapped': 326}
- `XLON`: {'ambiguous_mapping': 763, 'mapped': 6039, 'ticker_not_resolved': 104, 'unmapped': 116}
- `XETR`: {'ambiguous_mapping': 46, 'mapped': 4903, 'ticker_not_resolved': 236, 'unmapped': 350}
- `XBOM`: {'ambiguous_mapping': 31, 'mapped': 1531, 'ticker_not_resolved': 3373, 'unmapped': 115}
- `XNAS`: {'unmapped': 4753}
- `XJPX`: {'ambiguous_mapping': 79, 'mapped': 3997, 'unmapped': 61}
- `XKRX`: {'ambiguous_mapping': 45, 'mapped': 3424, 'unmapped': 126}
- `XNSE`: {'ambiguous_mapping': 17, 'mapped': 1290, 'ticker_not_resolved': 2078, 'unmapped': 119}
- `XHKG`: {'mapped': 108, 'ticker_not_resolved': 3091, 'unmapped': 40}
- `XSHE`: {'mapped': 3126, 'unmapped': 12}
- `XSTU`: {'ambiguous_mapping': 142, 'mapped': 2558, 'ticker_not_resolved': 15, 'unmapped': 58}
- `XSHG`: {'mapped': 2745, 'unmapped': 16}
- `ARCX`: {'mapped': 2417, 'ticker_not_resolved': 31, 'unmapped': 257}
- `XTSE`: {'ambiguous_mapping': 6, 'mapped': 1816, 'ticker_not_resolved': 238, 'unmapped': 235}

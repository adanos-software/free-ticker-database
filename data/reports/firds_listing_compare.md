# Official FIRDS listing comparison

Report only. No listing or identifier changes.

| Metric | Value |
| --- | ---: |
| official_unique_isins | 6343 |
| matched_isin_mic | 4945 |
| local_unique_isins | 10553 |
| local_not_observed_review | 5608 |
| local_listing_rows | 11466 |
| local_listing_rows_observed | 5709 |
| multi_currency_unresolved | 0 |
| firds_only_review | 1398 |
| snapshot_pair_recall_pct | 77.96 |
| local_pair_observed_pct | 46.86 |
| mics_not_in_snapshot | ['XETR', 'XFRA', 'XLON', 'XSWX'] |


## Pooled metrics (scorable MICs only)

Target MICs with zero rows in a provider snapshot are marked `mic_not_in_snapshot` and excluded from pooled ratios.

### By provider

| Provider | Recall % | Local observed % | Not in snapshot |
| --- | ---: | ---: | --- |
| ESMA | 82.49 | 55.79 | XETR, XFRA, XLON, XSWX |
| FCA | 76.56 | 44.49 | XETR, XFRA, XSWX |

### By MIC (exact MIC, per provider)

| Provider | MIC | Recall % | Local observed % | Status |
| --- | --- | ---: | ---: | --- |
| ESMA | XAMS | 92.58 | 60.76 | scorable |
| ESMA | XETR | None | None | mic_not_in_snapshot |
| ESMA | XFRA | None | None | mic_not_in_snapshot |
| ESMA | XLON | None | None | mic_not_in_snapshot |
| ESMA | XPAR | 77.65 | 53.29 | scorable |
| ESMA | XSWX | None | None | mic_not_in_snapshot |
| FCA | XAMS | 92.58 | 60.76 | scorable |
| FCA | XETR | None | None | mic_not_in_snapshot |
| FCA | XFRA | None | None | mic_not_in_snapshot |
| FCA | XLON | 73.9 | 40.43 | scorable |
| FCA | XPAR | 77.7 | 53.22 | scorable |
| FCA | XSWX | None | None | mic_not_in_snapshot |

## Sources

- data/masterfiles/venue_mic_mapping.csv; SHA-256 `0be159507e12005e59d2a08b3e2372fe28bbfb994a299d1251b1a5ac479e5a85`; last-modified `not supplied`.
- https://www.iso20022.org/sites/default/files/ISO10383_MIC/ISO10383_MIC.csv; SHA-256 `09a0d07b7b8348147453a2d8b18bc6638e3c5b16f749462582c9b1edf0fd0e38`; last-modified `Mon, 12 Oct 2026 08:20:53 GMT`.
- https://firds.esma.europa.eu/firds/FULINS_C_20261017_01of01.zip; SHA-256 `71c159f263e936372256494b99515b9ec35b979caeb36b773ba6c3840dabfcc8`; last-modified `Sat, 17 Oct 2026 06:58:11 GMT`.
- https://firds.esma.europa.eu/firds/FULINS_E_20261017_01of02.zip; SHA-256 `3bdd8d5e3bed51fabc42de89d40b3890e641dc328219e9b75392836a9ca9d5d5`; last-modified `Sat, 17 Oct 2026 06:58:12 GMT`.
- https://firds.esma.europa.eu/firds/FULINS_E_20261017_02of02.zip; SHA-256 `5a725b2d35e6b04648d63436f89b2dc21492baf16b53e731246754c626eaed93`; last-modified `Sat, 17 Oct 2026 06:58:12 GMT`.
- https://data.fca.org.uk/artefacts/FIRDS/FULINS_C_20261017_01of01.zip; SHA-256 `a215fd0879269db3c8d4f1a9138d00a30a3dc9769a218239cf28fd3453058978`; last-modified `Sat, 17 Oct 2026 09:55:58 GMT`.
- https://data.fca.org.uk/artefacts/FIRDS/FULINS_E_20261017_01of01.zip; SHA-256 `426e5c46d016804c0a52b7c11b26c696bf957b801a193fcc18a60fe07001f8e5`; last-modified `Sat, 17 Oct 2026 09:56:01 GMT`.

## Limits

- Metrics only. Official XML and ISIN/MIC rows are not redistributed into the MIT dataset or packages.
- ES/ED common equity and depositary receipts plus CE ETFs only. Other CFI categories stay outside scope.
- Operating and segment MICs remain separate. SI/MTF observations do not become missing-listing claims.
- FIRDS has no reliable trading ticker. Same-listing-key ISIN mismatches cannot be scored from these files.
- Local exports lack currency. Multiple FIRDS currencies remain unresolved; no currency-specific completeness claim.
- Weekly full snapshots are not current-day truth. Missing pairs are review candidates, not proven false positives.
- Snapshot ratios pool scorable provider/MIC/instrument pairs only; ESMA and FCA overlap. They are not global recall or identity accuracy.
- German operating MICs XETR and XFRA and Swiss XSWX often have zero rows in ESMA FULINS; those MICs are mic_not_in_snapshot for ESMA and excluded from ESMA pooled ratios. Frankfurt segment MICs such as FRAB, STUB, HAND, and MUND may appear under SI/MTF and are never merged into XETR.
- UK XLON is absent from ESMA FULINS but present in FCA FIRDS; score LSE via FCA, not ESMA.
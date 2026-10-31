# Independent reference audits

These tools address issues #445 through #448. They read canonical listings and write reports, not names, countries, identifiers, or ingestion overrides. Reports record source and input SHA-256 hashes. They do not change `VERSION` or expand the official-reference ingestion queue.

Run the commands from the repository root with `requirements.txt` installed. The scripts are standalone review tools, not network steps in the canonical rebuild.

## SEC listed-current comparison (#448)

```sh
python scripts/build_sec_listed_current_compare.py --fetch
```

The tool caches the official [SEC company ticker file](https://www.sec.gov/files/company_tickers_exchange.json) with its Last-Modified header. Omit `--fetch` to replay the cache. `sec_listed_current_compare.csv`, `.json`, and `.md` distinguish matches, ambiguous venues, missing candidates, scope reviews, and local-only US listings.

The join reuses `SEC_EXCHANGE_CODES` and ticker normalization from `backfill_sec_sic_sectors.py`. SEC NYSE labels can match NYSE American. Multiple local matches remain ambiguous. SEC names do not establish security type, so a missing candidate needs review. Local-only rows are not delistings; SEC is not a complete ETP master. See the [SEC access policy](https://www.sec.gov/os/accessing-edgar-data).

## GLEIF issuer comparison (#447)

The committed JSON report pins the exact bulk relationship URL and hash. Download that snapshot into the ignored cache:

```python
import json
from pathlib import Path
from scripts.lib.reference_audit import fetch_snapshot

source = json.loads(Path("data/reports/gleif_isin_lei_compare.json").read_text())["sources"][0]
metadata = fetch_snapshot(source["url"], Path("data/masterfiles/cache/gleif/pinned.zip"))
assert metadata["sha256"] == source["sha256"]
```

```sh
python scripts/build_gleif_isin_lei_compare.py --source-zip data/masterfiles/cache/gleif/pinned.zip
```

The ZIP is streamed. Only relationships for local valid ISINs are held in memory. The report contains aggregate coverage flags; `gleif_lei_conflict_review_queue.csv` contains only mismatches or multiple-LEI cases, with direct apply rows fixed to zero. LEI is issuer-grain evidence, not a listing join key.

The ANNA-containing mapping file has a separate reuse boundary from GLEIF LEI reference-data CC0. The bulk file stays in the ignored cache and is not added to Kaggle or Hugging Face packages. No GLEIF names or identifiers are adopted. See the [relationship-file page](https://www.gleif.org/en/lei-data/lei-mapping/download-isin-to-lei-relationship-files) and [LEI data terms](https://www.gleif.org/en/meta/lei-data-terms-of-use).

## Official ESMA and FCA FIRDS comparison (#445)

`firds_listing_compare_sources.json` pins every C and E full-snapshot part from both regulators. The parser rejects missing parts, mixed dates, unofficial download hosts, and hash mismatches. Download the pinned files:

```python
import json
from pathlib import Path
from scripts.lib.reference_audit import fetch_snapshot

files = json.loads(Path("data/reports/firds_listing_compare_sources.json").read_text())
for source in files:
    metadata = fetch_snapshot(source["url"], Path(source["path"]))
    assert metadata["sha256"] == source["sha256"]

report = json.loads(Path("data/reports/firds_listing_compare.json").read_text())
iso = next(source for source in report["sources"] if "ISO10383_MIC.csv" in source["url"])
metadata = fetch_snapshot(iso["url"], Path("data/masterfiles/cache/firds/ISO10383_MIC.csv"))
assert metadata["sha256"] == iso["sha256"]
```

```sh
python scripts/build_firds_listing_compare.py \
  --manifest data/reports/firds_listing_compare_sources.json \
  --iso-csv data/masterfiles/cache/firds/ISO10383_MIC.csv
```

The local join uses `venue_mic_mapping.csv`. The official ISO registry identifies related segment MICs and market categories; it does not replace the local mapping. Exact MIC matches, segment differences, and SI/MTF observations remain separate. The conservative CFI scope is ES/ED common equity and depositary receipts, plus CE ETFs. `firds_only_review` counts are split by CFI class (ES, ED, CE, and so on) with SI/MTF kept on separate resolution rows.

Target MICs with **zero RefData rows** in a pinned provider snapshot get status `mic_not_in_snapshot` and are **excluded from pooled recall ratios** for that provider. Pooled metrics are reported globally, per provider (ESMA vs FCA), and per MIC in the JSON, Markdown, and CSV (`resolution=pooled` rows).

**Unscorable venues from these files:** German operating MICs **XETR** and **XFRA** (and other FRA* operating segments) and Swiss **XSWX** typically have no rows in **ESMA** FULINS. Do not treat ESMA `local_not_observed_review` on those MICs as a completeness score. Some Frankfurt **segment** MICs do appear under ESMA, often as SI/MTF (for example **FRAB**, **STUB**, **HAND**, **MUND**); they are reported separately and are **never merged into XETR**. **XLON** is absent from ESMA but present in **FCA** FIRDS; score LSE listing grain from FCA, not ESMA.

Only aggregate metrics are exported. Raw FIRDS XML, names, and ISIN/MIC rows stay in the ignored cache. Current public package builders use explicit export lists; these raw inputs are not added to those lists. File-level MIT reuse has not been approved. See the [ESMA legal notice](https://www.esma.europa.eu/legal-notice).

The weekly snapshot is not current-day truth. Missing pairs need review. FIRDS has no reliable trading ticker, so the report does not claim same-listing-key or same-ticker ISIN mismatches. Local exports also lack trading currency. Ratios describe observed **scorable** provider/MIC/instrument pairs and are not global recall, identity accuracy, or a license to ingest missing rows.

## OpenFIGI level audit (#446)

Replay the committed 200-request probe without network access:

```sh
python scripts/build_openfigi_isin_mic_remap.py \
  --probe-json data/reports/openfigi_isin_mic_remap_probe.json
```

Fetch a bounded, deterministic probe:

```sh
python scripts/build_openfigi_isin_mic_remap.py --fetch --limit 200
```

`--limit 0` fetches the full eligible universe into `data/openfigi_verification/isin_mic_remap_cache/` (gitignored). Probe order is the SHA-256 of the exact request, not a selection of favorable matches. Each request contains a valid `ID_ISIN`, a mapped MIC, and `includeUnlistedEquities=false`. The committed probe preserves 200 requests, responses, and response hashes for CI replay. Full-universe runs write `openfigi_isin_mic_remap_probe.manifest.json` (cache tarball SHA-256 and request counts) instead of replacing the large probe JSON. Unprobed listings remain explicit in the CSV.

Offline rebuild from an extracted cache tarball (no network, no API key):

```sh
python scripts/build_openfigi_isin_mic_remap.py \
  --cache-tarball-sha256 <sha256-of-isin_mic_remap_cache.tar.gz>
```

`OPENFIGI_API_KEY` is optional and is read only from the environment. It is never written to reports. Requests respect the documented batch and rate limits. HTTP errors stop the fetch; already cached batches remain available for resume.

Venue, composite, and share-class FIGIs are compared by explicit returned fields. Prefixes do not establish a FIGI level. An unknown local level does not become an ISIN error. Name-family differences use normalized token overlap (case, punctuation, corporate suffixes, share-class noise, and common OpenFIGI abbreviations/truncations). Residual disagreements land in `openfigi_stolen_isin_review_queue.*` with `apply_allowed=0` on every row; they are review candidates, not proof of a stolen identifier. OpenFIGI names are not adopted as official listing names. See the [API documentation](https://www.openfigi.com/api/documentation) and [terms](https://www.openfigi.com/docs/terms-of-service).

## Manual identity review (#449)

`isin_identity_issue449_review.*` records all nine original groups. `LSE::FP` has one reviewed empty ISIN override, supported by the issuer prospectus, PDF page 23. No historical GDR identifier is filled because the issuer reports that the London listing ended in April 2025. Country and name remain outside this patch.

The other eight groups remain open with listing-specific blockers. CSE evidence does not establish a NEO or TSXV listing. A different venue's identifier, a current homonym, or a bare CUSIP does not authorize a replacement. `FSX::CU2` remains empty. Regenerate the current queue with:

```sh
python scripts/build_isin_identity_collision_review_queue.py
```

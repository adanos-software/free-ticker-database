# M2 Operations

## Canonical Data Contract

CSV files under `data/` remain the diffable source of truth. Generated JSON, SQLite, and Parquet outputs are built by CI and releases, then published as release assets:

- `data/tickers.db`
- `data/tickers.json`
- `data/core_listings.json`
- `data/tickers.parquet`
- `data/core_listings.parquet`

Do not commit routine rebuild churn for those files. A future history-cleanup cutover can use a clean clone plus `git filter-repo`; this PR intentionally does not rewrite history.

## Operational Reports

Keep workflow reports that are consumed by CI, README snapshots, validation, or scheduled automation. Historical campaign evidence should be bundled with:

```bash
python scripts/build_evidence_archive.py
```

Publish `output/evidence_archive/m2-campaign-evidence.zip` as a one-time evidence archive before removing the matching campaign artifacts from the working tree.

## Living Pipeline

`scripts/pipeline_manifest.json` defines the living pipeline, archive candidates, and reports that remain operational. Active workflows should import `scripts/lib/*` helpers directly instead of importing helpers from dormant backfill campaigns.

## Release Publishing

GitHub releases are tag-triggered by `.github/workflows/release.yml`. Kaggle and Hugging Face publishing steps are guarded by repository variables and secrets:

- `PUBLISH_KAGGLE=true` plus `KAGGLE_USERNAME` / `KAGGLE_KEY`
- `PUBLISH_HUGGINGFACE=true` plus `HF_TOKEN`


# Public Dataset Publishing

This repository can be packaged for Kaggle and Hugging Face to create durable discovery and citation pages for the Adanos ticker reference data.

Primary backlink target: https://adanos.org

## Positioning

Use this title:

`Free Global Stock Ticker Database`

Use this short description:

`Global stocks and ETFs with listings, identifiers, aliases, and symbol changes.`

Do not describe this repository as historical sentiment data. It is a ticker, listing, identifier, alias, and symbol-change reference dataset that supports ticker detection and market-data workflows.

## Package Build

Build both upload folders:

```bash
python3 scripts/build_public_dataset_package.py \
  --kaggle-id adanosorg/free-global-stock-ticker-database \
  --hf-repo-id adanosorg/free-global-stock-ticker-database
```

Generated folders:

- `output/public_dataset/kaggle`
- `output/public_dataset/huggingface`

The output directory is intentionally ignored by git.

The build validates each exported CSV header against the publishing schema before writing upload folders. If a source export changes columns or order, update `scripts/build_public_dataset_package.py` and this document intentionally instead of uploading stale metadata.

The Kaggle package also includes `dataset-cover-image.png`, a generated cover image referenced from the Kaggle metadata for the dataset header and thumbnail.

Kaggle tags are restricted to platform-defined tags. The package uses validated finance-adjacent tags: `finance`, `investing`, `business`, `economics`, `data visualization`, and `time series analysis`.

## Included Files

- `tickers.csv`: one canonical row per security
- `listings.csv`: exchange-level listing rows keyed by `listing_key`
- `aliases.csv`: alias, name, ISIN, WKN, and symbol lookup values
- `identifiers.csv`: compact ISIN/WKN lookup
- `cross_listings.csv`: same-ISIN listing groups
- `instrument_scopes.csv`: core versus extended scope metadata
- `listing_events.csv`: listing history events
- `symbol_changes.csv`: reviewed symbol changes with source links
- `README.md`: platform-facing dataset card/readme
- `LICENSE`: MIT license from the source repository
- `CITATION.cff`: citation metadata
- `dataset-cover-image.png`: Kaggle cover image

The Hugging Face folder also contains Parquet versions of the CSV files because Hugging Face recommends Parquet for analytics-friendly tabular datasets.

## Kaggle Upload

Kaggle requires `dataset-metadata.json` next to the uploaded files. The metadata includes:

- dataset title and subtitle
- source and project links to `https://adanos.org`
- GitHub repository link
- file-level descriptions and schemas
- column descriptors for each exported CSV
- explicit provenance/source text
- cover image reference
- expected update frequency

Kaggle does not currently list MIT as a native dataset license option in its documented metadata options. The generated metadata therefore uses `other` and states the MIT license in the dataset description and bundled `LICENSE` file.

Create or update via Kaggle CLI after configuring credentials:

```bash
kaggle datasets create -p output/public_dataset/kaggle
```

For later versions:

```bash
kaggle datasets version -p output/public_dataset/kaggle -m "Refresh ticker database"
```

## Hugging Face Upload

Create a public dataset repository, then upload the generated Hugging Face folder. The generated `README.md` includes YAML metadata for Hub discovery and a visible source link to `https://adanos.org`. The dataset is tagged as finance/tabular reference data, not as a supervised ML task.

CLI flow:

```bash
huggingface-cli repo create free-global-stock-ticker-database --type dataset --organization adanosorg
cd output/public_dataset/huggingface
git init
git remote add origin https://huggingface.co/datasets/adanosorg/free-global-stock-ticker-database
git add .
git commit -m "Initial dataset release"
git push origin main
```

If the repository already exists, clone it and copy the generated files into the clone before committing.

## Backlink Text

Use one of these visible attribution lines in platform descriptions:

`Source and project page: https://adanos.org`

`Maintained by Adanos Software GmbH. Dataset source: https://adanos.org`

`Free Global Stock Ticker Database by Adanos Software GmbH: https://adanos.org`

## Update Cadence

Recommended cadence: daily or weekly, matching the source repository release cadence. For SEO and dataset trust, prefer regular versioned updates over one-off uploads.

## Quality Gate Before Upload

Run the usual validation before building a public package:

```bash
python3 scripts/validate_database.py
python3 scripts/build_public_dataset_package.py
```

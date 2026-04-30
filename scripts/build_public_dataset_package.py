#!/usr/bin/env python3
"""Build Kaggle and Hugging Face upload folders for the public ticker dataset."""

from __future__ import annotations

import argparse
import csv
import json
import shutil
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from PIL import Image, ImageDraw, ImageFont


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = ROOT / "output" / "public_dataset"
KAGGLE_ID = "adanosorg/free-global-stock-ticker-database"
HF_REPO_ID = "adanosorg/free-global-stock-ticker-database"
KAGGLE_METADATA_FILE = "dataset-metadata.json"
COVER_IMAGE = "dataset-cover-image.png"
PROVENANCE = (
    "The dataset is rebuilt by Adanos Software GmbH from official exchange/reference inputs "
    "and reviewed enrichment sources. Official inputs include exchange masterfiles and reference "
    "directories; reviewed enrichment inputs are reconciled into the public exports only after "
    "validation gates for identifiers, listing keys, aliases, and coverage reports. Project and "
    "source documentation: https://adanos.org and "
    "https://github.com/adanos-software/free-ticker-database."
)


@dataclass(frozen=True)
class DatasetFile:
    source: Path
    name: str
    description: str
    columns: dict[str, str]
    parquet: bool = True


FILES = [
    DatasetFile(
        ROOT / "data" / "tickers.csv",
        "tickers.csv",
        "Canonical primary security export, one row per stock or ETF.",
        {
            "ticker": "Primary ticker symbol in the dataset.",
            "name": "Current security or fund name.",
            "exchange": "Primary listing exchange code.",
            "asset_type": "Instrument class, currently Stock or ETF.",
            "stock_sector": "Sector for stock rows.",
            "etf_category": "Category for ETF rows.",
            "country": "Issuer or primary listing country name.",
            "country_code": "ISO-like country code when available.",
            "isin": "International Securities Identification Number when available.",
            "aliases": "Conservative natural-language aliases for mention detection.",
        },
    ),
    DatasetFile(
        ROOT / "data" / "listings.csv",
        "listings.csv",
        "Venue-level listing export keyed by listing_key, including cross-listings.",
        {
            "listing_key": "Stable venue-level key in EXCHANGE::TICKER format.",
            "ticker": "Listing ticker symbol.",
            "exchange": "Listing exchange code.",
            "name": "Security or fund name.",
            "asset_type": "Instrument class, currently Stock or ETF.",
            "stock_sector": "Sector for stock rows.",
            "etf_category": "Category for ETF rows.",
            "country": "Issuer or listing country name.",
            "country_code": "ISO-like country code when available.",
            "isin": "International Securities Identification Number when available.",
            "aliases": "Conservative natural-language aliases for mention detection.",
        },
    ),
    DatasetFile(
        ROOT / "data" / "aliases.csv",
        "aliases.csv",
        "Alias, name, and identifier lookup rows for ticker resolution.",
        {
            "ticker": "Primary ticker symbol in the dataset.",
            "alias": "Lookup value.",
            "alias_type": "Alias category such as name, isin, wkn, or symbol.",
        },
    ),
    DatasetFile(
        ROOT / "data" / "identifiers.csv",
        "identifiers.csv",
        "Compact ISIN and WKN lookup for primary ticker rows.",
        {
            "ticker": "Primary ticker symbol in the dataset.",
            "isin": "International Securities Identification Number when available.",
            "wkn": "German Wertpapierkennnummer when available.",
        },
    ),
    DatasetFile(
        ROOT / "data" / "cross_listings.csv",
        "cross_listings.csv",
        "Same-ISIN listing groups across exchanges.",
        {
            "isin": "International Securities Identification Number.",
            "listing_key": "Stable venue-level key in EXCHANGE::TICKER format.",
            "ticker": "Listing ticker symbol.",
            "exchange": "Listing exchange code.",
            "is_primary": "Whether the row is the selected primary listing.",
        },
    ),
    DatasetFile(
        ROOT / "data" / "instrument_scopes.csv",
        "instrument_scopes.csv",
        "Core versus extended listing scope and primary-listing mapping.",
        {
            "listing_key": "Stable venue-level key in EXCHANGE::TICKER format.",
            "ticker": "Listing ticker symbol.",
            "exchange": "Listing exchange code.",
            "asset_type": "Instrument class, currently Stock or ETF.",
            "isin": "International Securities Identification Number when available.",
            "instrument_group_key": "Security grouping key, usually derived from ISIN.",
            "instrument_scope": "Public scope classification such as core or extended.",
            "scope_reason": "Reason for the scope assignment.",
            "primary_listing_key": "Primary listing key for the security group.",
        },
    ),
    DatasetFile(
        ROOT / "data" / "history" / "listing_events.csv",
        "listing_events.csv",
        "Observed listing lifecycle events from the listing history workflow.",
        {
            "listing_key": "Stable venue-level key in EXCHANGE::TICKER format.",
            "ticker": "Listing ticker symbol.",
            "exchange": "Listing exchange code.",
            "event_type": "Event category detected by the history workflow.",
            "old_value": "Previous value when applicable.",
            "new_value": "New value when applicable.",
            "observed_at": "UTC timestamp or date when the event was observed.",
        },
    ),
    DatasetFile(
        ROOT / "data" / "corporate_actions" / "symbol_changes.csv",
        "symbol_changes.csv",
        "Reviewed symbol-change feed with source links and review flags.",
        {
            "change_id": "Stable change identifier.",
            "effective_date": "Effective date when known.",
            "old_symbol": "Previous ticker symbol.",
            "new_symbol": "New ticker symbol.",
            "new_company_name": "New company name when available.",
            "source": "Source label.",
            "source_url": "Source URL for the symbol change.",
            "new_symbol_url": "Reference URL for the new symbol when available.",
            "source_exchange_hint": "Exchange hint from the source.",
            "source_confidence": "Confidence classification for the source row.",
            "review_needed": "Whether manual review is still needed.",
            "observed_at": "UTC timestamp when the change was observed.",
        },
    ),
]


def count_rows(path: Path) -> int:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        next(reader)
        return sum(1 for _ in reader)


def read_header(path: Path) -> list[str]:
    with path.open(newline="", encoding="utf-8") as handle:
        return next(csv.reader(handle))


def validate_dataset_files() -> None:
    for dataset_file in FILES:
        header = read_header(dataset_file.source)
        expected = list(dataset_file.columns)
        if header != expected:
            raise ValueError(
                f"{dataset_file.source} header does not match publishing schema: "
                f"expected {expected}, got {header}"
            )


def read_version() -> str:
    return (ROOT / "VERSION").read_text(encoding="utf-8").strip()


def ensure_clean_dir(path: Path) -> None:
    if path.exists():
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()
    path.mkdir(parents=True, exist_ok=True)


def copy_license(target: Path) -> None:
    shutil.copy2(ROOT / "LICENSE", target / "LICENSE")


def copy_csvs(target: Path) -> None:
    for dataset_file in FILES:
        shutil.copy2(dataset_file.source, target / dataset_file.name)


def write_parquet_files(target: Path) -> None:
    for dataset_file in FILES:
        if not dataset_file.parquet:
            continue
        dataframe = pd.read_csv(dataset_file.source, dtype=str, keep_default_na=False)
        dataframe.to_parquet(target / dataset_file.name.replace(".csv", ".parquet"), index=False)


def build_resource(dataset_file: DatasetFile) -> dict:
    return {
        "path": dataset_file.name,
        "description": dataset_file.description,
        "schema": {
            "fields": [
                {
                    "name": name,
                    "title": description,
                    "description": description,
                    "type": "string",
                }
                for name, description in dataset_file.columns.items()
            ]
        },
    }


def write_kaggle_metadata(target: Path, kaggle_id: str) -> None:
    metadata = {
        "title": "Free Global Stock Ticker Database",
        "subtitle": "Global stocks and ETFs with listings, identifiers, aliases, and symbol changes",
        "description": (
            "Free stock and ETF ticker reference data maintained by Adanos Software GmbH. "
            "The dataset includes primary ticker rows, venue-level listings, ISIN/WKN identifiers, "
            "alias lookup rows, cross-listings, scope metadata, listing events, and reviewed symbol changes. "
            "Source and project page: https://adanos.org. License: MIT."
        ),
        "id": kaggle_id,
        "licenses": [{"name": "other"}],
        "resources": [build_resource(dataset_file) for dataset_file in FILES],
        "keywords": [
            "finance",
            "investing",
            "business",
            "economics",
            "data visualization",
            "time series analysis",
        ],
        "expectedUpdateFrequency": "daily",
        "userSpecifiedSources": PROVENANCE,
        "image": COVER_IMAGE,
    }
    payload = json.dumps(metadata, indent=2, ensure_ascii=True) + "\n"
    (target / KAGGLE_METADATA_FILE).write_text(payload, encoding="utf-8")


def dataset_summary() -> dict[str, int]:
    return {
        dataset_file.name: count_rows(dataset_file.source)
        for dataset_file in FILES
    }


def summarize_count(count: int) -> str:
    if count >= 1_000_000:
        return f"{count // 1_000_000}M+"
    if count >= 1_000:
        return f"{count // 1_000}K+"
    return str(count)


def isin_coverage() -> str:
    tickers = next(dataset_file for dataset_file in FILES if dataset_file.name == "tickers.csv")
    dataframe = pd.read_csv(tickers.source, dtype=str, keep_default_na=False, usecols=["isin"])
    if dataframe.empty:
        return "0%"
    coverage = dataframe["isin"].str.strip().ne("").mean()
    return f"{int(coverage * 100)}%+"


def cover_metrics(rows: dict[str, int]) -> list[tuple[str, str]]:
    return [
        (summarize_count(rows["tickers.csv"]), "primary tickers"),
        (summarize_count(rows["listings.csv"]), "listing rows"),
        (summarize_count(rows["aliases.csv"]), "aliases"),
        (isin_coverage(), "ISIN coverage"),
    ]


def recommended_use(platform: str) -> str:
    if platform == "huggingface":
        ticker_line = "- Use `tickers.csv` or `tickers.parquet` for one canonical row per security."
        listings_line = "- Use `listings.csv` or `listings.parquet` when exchange-level identity matters."
    else:
        ticker_line = "- Use `tickers.csv` for one canonical row per security."
        listings_line = "- Use `listings.csv` when exchange-level identity matters."
    return f"""{ticker_line}
{listings_line}
- Use `listing_key` rather than `ticker` for venue-level joins.
- Use `aliases.csv` for ticker, name, ISIN, WKN, and symbol lookup workflows.
- Use `symbol_changes.csv` for reviewed ticker-change monitoring."""


def hf_config_name(file_name: str) -> str:
    return Path(file_name).stem


def write_readme(target: Path, *, platform: str, repo_id: str) -> None:
    version = read_version()
    rows = dataset_summary()
    frontmatter = ""
    if platform == "huggingface":
        config_lines = []
        for dataset_file in FILES:
            if not dataset_file.parquet:
                continue
            config_name = hf_config_name(dataset_file.name)
            parquet_name = Path(dataset_file.name).with_suffix(".parquet").name
            config_lines.append(f"- config_name: {config_name}")
            config_lines.append("  data_files:")
            config_lines.append(f'  - split: train')
            config_lines.append(f"    path: {parquet_name}")
        configs_block = "\n".join(config_lines)
        frontmatter = f"""---
license: mit
pretty_name: Free Global Stock Ticker Database
language:
- en
tags:
- finance
- stocks
- etf
- ticker
- isin
- wkn
- securities
- equities
- tabular
- reference-data
size_categories:
- 100K<n<1M
configs:
{configs_block}
---

"""

    body = f"""# Free Global Stock Ticker Database

Global stocks and ETFs with listings, identifiers, aliases, and reviewed symbol changes. Maintained by Adanos Software GmbH for ticker detection, identifier resolution, and market-data workflows.

Source and project page: https://adanos.org

GitHub repository: https://github.com/adanos-software/free-ticker-database

Dataset package: `{repo_id}`

Version: `{version}`

## Contents

| File | Rows | Description |
|---|---:|---|
"""
    for dataset_file in FILES:
        body += f"| `{dataset_file.name}` | {rows[dataset_file.name]:,} | {dataset_file.description} |\n"

    if platform == "huggingface":
        body += f"""
## How to Load

```python
from datasets import load_dataset

tickers = load_dataset("{repo_id}", "tickers", split="train")
listings = load_dataset("{repo_id}", "listings", split="train")
aliases = load_dataset("{repo_id}", "aliases", split="train")
```

Each file is exposed as its own config (`tickers`, `listings`, `aliases`, `identifiers`, `cross_listings`, `instrument_scopes`, `listing_events`, `symbol_changes`). Parquet variants are used by the loader; CSV variants remain available for direct download.
"""

    body += f"""
## Recommended Use

{recommended_use(platform)}

## Citation

If this dataset is useful in research, products, or notebooks, cite the dataset and link back to:

Adanos Software GmbH. Free Global Stock Ticker Database. https://adanos.org

## Provenance

{PROVENANCE}

## License

The source repository is licensed under the MIT License. See `LICENSE`.

## Limitations

This is reference data, not investment advice. Coverage and metadata quality vary by market. Rows are rebuilt from official exchange/reference inputs plus reviewed enrichment sources, and users should validate suitability for their own downstream use.
"""
    (target / "README.md").write_text(frontmatter + body, encoding="utf-8")


def load_font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    candidates = [
        (
            "/System/Library/Fonts/Supplemental/Arial Bold.ttf"
            if bold
            else "/System/Library/Fonts/Supplemental/Arial.ttf"
        ),
        "/System/Library/Fonts/Helvetica.ttc",
        "/Library/Fonts/Arial.ttf",
        (
            "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf"
            if bold
            else "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf"
        ),
        (
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
            if bold
            else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"
        ),
    ]
    for candidate in candidates:
        try:
            return ImageFont.truetype(candidate, size=size)
        except OSError:
            continue
    return ImageFont.load_default()


def write_cover_image(target: Path, rows: dict[str, int]) -> None:
    width, height = 1120, 560
    image = Image.new("RGB", (width, height), "#f6f8fb")
    draw = ImageDraw.Draw(image)

    for y in range(height):
        blend = y / max(height - 1, 1)
        red = int(246 * (1 - blend) + 226 * blend)
        green = int(248 * (1 - blend) + 236 * blend)
        blue = int(251 * (1 - blend) + 242 * blend)
        draw.line([(0, y), (width, y)], fill=(red, green, blue))

    accent = "#147a64"
    dark = "#162033"
    muted = "#5d687a"
    grid = "#d7dee8"

    draw.rectangle((0, 0, width, 18), fill=accent)
    draw.rectangle((72, 210, 1048, 386), outline="#c8d2df", width=2)
    draw.line((72, 352, 1048, 352), fill=grid, width=2)
    draw.line((72, 292, 1048, 292), fill=grid, width=1)
    draw.line((72, 234, 1048, 234), fill=grid, width=1)

    points = [(120, 340), (230, 318), (340, 330), (450, 286), (560, 304), (670, 260), (780, 276), (900, 232), (1010, 244)]
    draw.line(points, fill=accent, width=8, joint="curve")
    for x, y in points:
        draw.ellipse((x - 9, y - 9, x + 9, y + 9), fill="#ffffff", outline=accent, width=4)

    title_font = load_font(54, bold=True)
    subtitle_font = load_font(28)
    metric_font = load_font(24, bold=True)
    label_font = load_font(18)

    draw.text((88, 78), "Free Global Stock Ticker Database", font=title_font, fill=dark)
    draw.text((90, 154), "Global stocks and ETFs with listings, identifiers, aliases, and symbol changes", font=subtitle_font, fill=muted)

    metrics = cover_metrics(rows)
    x = 106
    for value, label in metrics:
        draw.rounded_rectangle((x, 398, x + 190, 482), radius=12, fill="#ffffff", outline="#d2dae5", width=1)
        draw.text((x + 18, 412), value, font=metric_font, fill=dark)
        draw.text((x + 18, 448), label, font=label_font, fill=muted)
        x += 228

    draw.text((88, 512), "Source: adanos.org", font=label_font, fill=accent)
    image.save(target / COVER_IMAGE)


def write_citation(target: Path) -> None:
    citation = f"""cff-version: 1.2.0
title: Free Global Stock Ticker Database
message: If you use this dataset, please cite it using this metadata.
type: dataset
authors:
  - name: Adanos Software GmbH
url: https://adanos.org
repository-code: https://github.com/adanos-software/free-ticker-database
license: MIT
version: {read_version()}
"""
    (target / "CITATION.cff").write_text(citation, encoding="utf-8")


def build_package(output: Path, kaggle_id: str, hf_repo_id: str) -> None:
    validate_dataset_files()
    rows = dataset_summary()

    kaggle = output / "kaggle"
    huggingface = output / "huggingface"
    ensure_clean_dir(kaggle)
    ensure_clean_dir(huggingface)

    copy_csvs(kaggle)
    copy_license(kaggle)
    write_cover_image(kaggle, rows)
    write_kaggle_metadata(kaggle, kaggle_id)
    write_readme(kaggle, platform="kaggle", repo_id=kaggle_id)
    write_citation(kaggle)

    copy_csvs(huggingface)
    write_parquet_files(huggingface)
    copy_license(huggingface)
    write_readme(huggingface, platform="huggingface", repo_id=hf_repo_id)
    write_citation(huggingface)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--kaggle-id",
        default=KAGGLE_ID,
        help="Kaggle owner/dataset-slug for dataset-metadata.json.",
    )
    parser.add_argument(
        "--hf-repo-id",
        default=HF_REPO_ID,
        help="Hugging Face owner/dataset-slug shown in the generated dataset card.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    build_package(args.output, args.kaggle_id, args.hf_repo_id)
    print(f"Wrote Kaggle package to {args.output / 'kaggle'}")
    print(f"Wrote Hugging Face package to {args.output / 'huggingface'}")


if __name__ == "__main__":
    main()

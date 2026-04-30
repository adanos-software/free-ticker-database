from __future__ import annotations

import json

import pytest

from scripts import build_public_dataset_package as package


def write_csv(path, header, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [",".join(header)]
    lines.extend(",".join(row) for row in rows)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def configure_sample_dataset(monkeypatch, tmp_path):
    source_root = tmp_path / "source"
    source_root.mkdir()
    (source_root / "LICENSE").write_text("MIT License\n", encoding="utf-8")
    (source_root / "VERSION").write_text("9.9.9\n", encoding="utf-8")

    tickers = source_root / "data" / "tickers.csv"
    listings = source_root / "data" / "listings.csv"
    aliases = source_root / "data" / "aliases.csv"
    write_csv(
        tickers,
        ["ticker", "name", "exchange", "isin"],
        [
            ["AAPL", "Apple Inc", "NASDAQ", "US0378331005"],
            ["MSFT", "Microsoft Corp", "NASDAQ", ""],
        ],
    )
    write_csv(
        listings,
        ["listing_key", "ticker", "exchange"],
        [
            ["NASDAQ::AAPL", "AAPL", "NASDAQ"],
            ["NASDAQ::MSFT", "MSFT", "NASDAQ"],
            ["XETRA::APC", "APC", "XETRA"],
        ],
    )
    write_csv(
        aliases,
        ["ticker", "alias"],
        [["AAPL", "apple"], ["MSFT", "microsoft"]],
    )

    files = [
        package.DatasetFile(
            tickers,
            "tickers.csv",
            "Primary securities.",
            {
                "ticker": "Ticker symbol.",
                "name": "Security name.",
                "exchange": "Exchange code.",
                "isin": "International Securities Identification Number.",
            },
        ),
        package.DatasetFile(
            listings,
            "listings.csv",
            "Venue listings.",
            {
                "listing_key": "Venue-level listing key.",
                "ticker": "Ticker symbol.",
                "exchange": "Exchange code.",
            },
        ),
        package.DatasetFile(
            aliases,
            "aliases.csv",
            "Alias lookup rows.",
            {
                "ticker": "Ticker symbol.",
                "alias": "Lookup alias.",
            },
        ),
    ]

    monkeypatch.setattr(package, "ROOT", source_root)
    monkeypatch.setattr(package, "FILES", files)
    return files


def test_build_package_writes_platform_specific_metadata(monkeypatch, tmp_path):
    configure_sample_dataset(monkeypatch, tmp_path)

    output = tmp_path / "out"
    package.build_package(
        output,
        "adanos/free-global-stock-ticker-database",
        "adanos/free-global-stock-ticker-database",
    )

    kaggle = output / "kaggle"
    huggingface = output / "huggingface"

    assert (kaggle / "tickers.csv").exists()
    assert not (kaggle / "tickers.parquet").exists()
    assert (kaggle / "dataset-cover-image.png").exists()
    assert (huggingface / "tickers.csv").exists()
    assert (huggingface / "tickers.parquet").exists()
    assert (huggingface / "aliases.parquet").exists()

    assert (kaggle / "dataset-metadata.json").exists()
    assert not (kaggle / "datasets-metadata.json").exists()
    metadata = json.loads((kaggle / "dataset-metadata.json").read_text(encoding="utf-8"))
    assert metadata["id"] == "adanos/free-global-stock-ticker-database"
    assert metadata["licenses"] == [{"name": "other"}]
    assert "https://adanos.org" in metadata["description"]
    assert "License: MIT" in metadata["description"]
    assert metadata["image"] == "dataset-cover-image.png"
    assert "official exchange/reference inputs" in metadata["userSpecifiedSources"]
    assert metadata["keywords"] == [
        "finance",
        "investing",
        "business",
        "economics",
        "data visualization",
        "time series analysis",
    ]
    assert [resource["path"] for resource in metadata["resources"]] == [
        "tickers.csv",
        "listings.csv",
        "aliases.csv",
    ]
    assert [field["name"] for field in metadata["resources"][0]["schema"]["fields"]] == [
        "ticker",
        "name",
        "exchange",
        "isin",
    ]
    assert metadata["resources"][0]["schema"]["fields"][0]["description"] == "Ticker symbol."

    kaggle_readme = (kaggle / "README.md").read_text(encoding="utf-8")
    hf_readme = (huggingface / "README.md").read_text(encoding="utf-8")
    assert "Source and project page: https://adanos.org" in kaggle_readme
    assert "## Provenance" in kaggle_readme
    assert "Version: `9.9.9`" in hf_readme
    assert "`tickers.parquet`" not in kaggle_readme
    assert "`tickers.parquet`" in hf_readme
    assert hf_readme.startswith("---\nlicense: mit")


def test_validate_dataset_files_rejects_schema_drift(monkeypatch, tmp_path):
    files = configure_sample_dataset(monkeypatch, tmp_path)
    write_csv(files[0].source, ["ticker", "exchange", "name"], [["AAPL", "NASDAQ", "Apple Inc"]])

    with pytest.raises(ValueError, match="header does not match publishing schema"):
        package.validate_dataset_files()


def test_ensure_clean_dir_replaces_file(tmp_path):
    target = tmp_path / "target"
    target.write_text("stale file\n", encoding="utf-8")

    package.ensure_clean_dir(target)

    assert target.is_dir()
    assert list(target.iterdir()) == []


def test_cover_metrics_use_current_dataset_counts(monkeypatch, tmp_path):
    configure_sample_dataset(monkeypatch, tmp_path)

    metrics = package.cover_metrics(package.dataset_summary())

    assert metrics == [
        ("2", "primary tickers"),
        ("3", "listing rows"),
        ("2", "aliases"),
        ("50%+", "ISIN coverage"),
    ]


def test_load_font_checks_linux_font_paths(monkeypatch):
    candidates = []

    def fake_truetype(candidate, size):
        candidates.append(candidate)
        if candidate.endswith("DejaVuSans-Bold.ttf"):
            return "font"
        raise OSError

    monkeypatch.setattr(package.ImageFont, "truetype", fake_truetype)

    assert package.load_font(18, bold=True) == "font"
    assert "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf" in candidates
    assert "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" in candidates

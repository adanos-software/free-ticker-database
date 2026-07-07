from __future__ import annotations

from pathlib import Path

from scripts.build_release_artifacts import build_release_artifacts


def test_build_release_artifacts_copies_derived_assets_and_zips_packages(tmp_path: Path) -> None:
    assets = []
    for name in ["tickers.db", "tickers.json"]:
        path = tmp_path / "data" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(name, encoding="utf-8")
        assets.append(path)
    public = tmp_path / "public_dataset"
    (public / "kaggle").mkdir(parents=True)
    (public / "kaggle" / "tickers.csv").write_text("ticker\nA\n", encoding="utf-8")
    (public / "huggingface").mkdir(parents=True)
    (public / "huggingface" / "tickers.parquet").write_text("placeholder", encoding="utf-8")

    manifest = build_release_artifacts(
        output=tmp_path / "release",
        public_dataset_output=public,
        derived_assets=assets,
    )

    paths = {Path(item["path"]).name for item in manifest["assets"]}
    assert {"tickers.db", "tickers.json", "kaggle-dataset-package.zip", "huggingface-dataset-package.zip"} <= paths
    assert all(item["sha256"] for item in manifest["assets"])

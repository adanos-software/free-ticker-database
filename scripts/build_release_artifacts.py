from __future__ import annotations

import argparse
import hashlib
import shutil
from pathlib import Path
from typing import Any

try:
    from scripts.lib.dataio import write_json
except ModuleNotFoundError:  # pragma: no cover - script execution path
    from lib.dataio import write_json


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = ROOT / "output" / "release"
PUBLIC_DATASET_OUTPUT = ROOT / "output" / "public_dataset"
DERIVED_ASSETS = [
    ROOT / "data" / "tickers.db",
    ROOT / "data" / "tickers.json",
    ROOT / "data" / "core_listings.json",
    ROOT / "data" / "tickers.parquet",
    ROOT / "data" / "core_listings.parquet",
]


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def clean_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def copy_assets(target: Path, assets: list[Path]) -> list[dict[str, Any]]:
    copied: list[dict[str, Any]] = []
    for asset in assets:
        if not asset.exists():
            raise FileNotFoundError(f"Required release asset is missing: {asset}")
        destination = target / asset.name
        shutil.copy2(asset, destination)
        copied.append({"path": display_path(destination), "bytes": destination.stat().st_size, "sha256": sha256(destination)})
    return copied


def zip_directory(source: Path, target_zip_without_suffix: Path) -> Path | None:
    if not source.exists():
        return None
    archive = shutil.make_archive(str(target_zip_without_suffix), "zip", source)
    return Path(archive)


def build_release_artifacts(
    *,
    output: Path = DEFAULT_OUTPUT,
    public_dataset_output: Path = PUBLIC_DATASET_OUTPUT,
    derived_assets: list[Path] | None = None,
) -> dict[str, Any]:
    clean_dir(output)
    assets_dir = output / "assets"
    assets_dir.mkdir(parents=True, exist_ok=True)

    copied = copy_assets(assets_dir, derived_assets or DERIVED_ASSETS)
    package_archives: list[dict[str, Any]] = []
    for package_name in ["kaggle", "huggingface"]:
        archive = zip_directory(public_dataset_output / package_name, assets_dir / f"{package_name}-dataset-package")
        if archive is None:
            continue
        package_archives.append(
            {"path": display_path(archive), "bytes": archive.stat().st_size, "sha256": sha256(archive)}
        )
    manifest = {"assets": [*copied, *package_archives]}
    write_json(output / "manifest.json", manifest)
    return manifest


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build GitHub Release assets from rebuilt dataset artifacts.")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--public-dataset-output", type=Path, default=PUBLIC_DATASET_OUTPUT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    manifest = build_release_artifacts(output=args.output, public_dataset_output=args.public_dataset_output)
    print(f"Wrote {len(manifest['assets'])} release assets to {args.output / 'assets'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

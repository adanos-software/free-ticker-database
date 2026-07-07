from __future__ import annotations

import argparse
import fnmatch
import hashlib
import zipfile
from pathlib import Path
from typing import Any

try:
    from scripts.lib.dataio import write_json
except ModuleNotFoundError:  # pragma: no cover - script execution path
    from lib.dataio import write_json


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = ROOT / "output" / "evidence_archive"
DEFAULT_ARCHIVE_NAME = "m2-campaign-evidence.zip"
ARCHIVE_PATTERNS = [
    "data/deepseek_review_jobs/*",
    "data/reports/twelvedata_*",
    "data/reports/deepseek_*",
]
KEEP_OPERATIONAL_PATTERNS = [
    "data/reports/deepseek_review_summary.*",
    "data/reports/deepseek_batch_plan.*",
]


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def is_kept_operational(path: str) -> bool:
    return any(fnmatch.fnmatch(path, pattern) for pattern in KEEP_OPERATIONAL_PATTERNS)


def archive_candidates(patterns: list[str] | None = None) -> list[Path]:
    candidates: dict[str, Path] = {}
    for pattern in patterns or ARCHIVE_PATTERNS:
        for path in ROOT.glob(pattern):
            if path.is_file():
                relative = path.relative_to(ROOT).as_posix()
                if not is_kept_operational(relative):
                    candidates[relative] = path
    return [candidates[key] for key in sorted(candidates)]


def build_evidence_archive(
    *,
    output: Path = DEFAULT_OUTPUT,
    archive_name: str = DEFAULT_ARCHIVE_NAME,
    patterns: list[str] | None = None,
) -> dict[str, Any]:
    output.mkdir(parents=True, exist_ok=True)
    archive_path = output / archive_name
    files = archive_candidates(patterns)
    with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in files:
            archive.write(path, path.relative_to(ROOT).as_posix())
    manifest = {
        "archive": str(archive_path.relative_to(ROOT)),
        "bytes": archive_path.stat().st_size,
        "sha256": sha256(archive_path),
        "file_count": len(files),
        "files": [path.relative_to(ROOT).as_posix() for path in files],
        "policy": "historical_campaign_evidence_release_asset_not_operational_workflow_input",
    }
    write_json(output / "manifest.json", manifest)
    return manifest


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a one-time evidence archive for historical campaign artifacts.")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--archive-name", default=DEFAULT_ARCHIVE_NAME)
    parser.add_argument("--pattern", action="append", dest="patterns")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    manifest = build_evidence_archive(output=args.output, archive_name=args.archive_name, patterns=args.patterns)
    print(f"Wrote {manifest['file_count']} files to {manifest['archive']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

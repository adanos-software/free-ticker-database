"""Build a deterministic canonical-v4 release archive with evidence ledgers."""

from __future__ import annotations

import argparse
import hashlib
import json
import zipfile
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parents[1]
CANONICAL_DIR = ROOT / "data" / "canonical_v4"
OUTPUT_ZIP = ROOT / "output" / "release" / "assets" / "canonical-v4.zip"
DEFAULT_EVIDENCE = [
    ROOT / "data/reports/reference_reconciliation.csv",
    ROOT / "data/reports/reference_reconciliation.json",
    ROOT / "data/reports/coverage_contracts.csv",
    ROOT / "data/reports/coverage_contracts.json",
    ROOT / "data/reports/source_licensing.json",
    ROOT / "data/reports/identifier_quarantine.csv",
    ROOT / "data/reports/identifier_quarantine.json",
    ROOT / "data/reports/safe_merge.json",
    ROOT / "schema/canonical_v4.sql",
    ROOT / "schema/canonical_v4_contract.json",
]
FIXED_ZIP_TIME = (1980, 1, 1, 0, 0, 0)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _arcname(path: Path, root: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return f"evidence/{path.name}"


def build_archive(
    *,
    canonical_dir: Path = CANONICAL_DIR,
    output_zip: Path = OUTPUT_ZIP,
    evidence_paths: Iterable[Path] = DEFAULT_EVIDENCE,
    root: Path = ROOT,
) -> dict[str, object]:
    manifest_path = canonical_dir / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError("canonical-v4 manifest.json is required")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    canonical_paths = [manifest_path]
    for item in manifest.get("files", []):
        path = canonical_dir / str(item.get("path", ""))
        if not path.exists():
            raise FileNotFoundError(f"manifest file is missing: {path}")
        canonical_paths.append(path)
    evidence = list(evidence_paths)
    missing = [path for path in evidence if not path.exists()]
    if missing:
        raise FileNotFoundError("required canonical release evidence is missing: " + ", ".join(str(path) for path in missing))
    paths = sorted({path.resolve() for path in [*canonical_paths, *evidence]}, key=lambda path: _arcname(path, root))
    entries: list[tuple[str, bytes]] = []
    for path in paths:
        entries.append((_arcname(path, root), path.read_bytes()))
    checksums = "".join(f"{sha256_bytes(data)}  {name}\n" for name, data in entries).encode("utf-8")
    entries.append(("SHA256SUMS", checksums))
    output_zip.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(output_zip, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for name, data in sorted(entries):
            info = zipfile.ZipInfo(name, date_time=FIXED_ZIP_TIME)
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o100644 << 16
            archive.writestr(info, data, compress_type=zipfile.ZIP_DEFLATED, compresslevel=9)
    report = {
        "archive": str(output_zip), "sha256": sha256_bytes(output_zip.read_bytes()),
        "files": len(entries), "git_commit": manifest.get("git_commit", ""),
        "aggregate_sha256": manifest.get("aggregate_sha256", ""),
    }
    print(json.dumps(report, indent=2))
    return report


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--canonical-dir", type=Path, default=CANONICAL_DIR)
    parser.add_argument("--output-zip", type=Path, default=OUTPUT_ZIP)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    build_archive(canonical_dir=args.canonical_dir, output_zip=args.output_zip)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

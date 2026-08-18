from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.build_canonical_v4_archive import build_archive


def make_canonical(root: Path) -> tuple[Path, list[Path]]:
    canonical = root / "data/canonical_v4"
    canonical.mkdir(parents=True)
    table = canonical / "sources.csv"
    table.write_text("source_id\n00000000-0000-0000-0000-000000000001\n", encoding="utf-8")
    manifest = {"git_commit":"1"*40,"aggregate_sha256":"a"*64,"files":[{"path":"sources.csv","sha256":"ignored","rows":1}]}
    (canonical / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    evidence = [root / "data/reports/evidence.json", root / "schema/schema.sql"]
    for path in evidence:
        path.parent.mkdir(parents=True, exist_ok=True); path.write_text("evidence\n", encoding="utf-8")
    return canonical, evidence


def test_archive_is_deterministic(tmp_path: Path) -> None:
    canonical, evidence = make_canonical(tmp_path)
    first = tmp_path / "one.zip"; second = tmp_path / "two.zip"
    a = build_archive(canonical_dir=canonical, output_zip=first, evidence_paths=evidence, root=tmp_path)
    b = build_archive(canonical_dir=canonical, output_zip=second, evidence_paths=evidence, root=tmp_path)
    assert a["sha256"] == b["sha256"]
    assert first.read_bytes() == second.read_bytes()


def test_archive_requires_all_evidence(tmp_path: Path) -> None:
    canonical, evidence = make_canonical(tmp_path)
    evidence[0].unlink()
    with pytest.raises(FileNotFoundError, match="evidence"):
        build_archive(canonical_dir=canonical, output_zip=tmp_path / "out.zip", evidence_paths=evidence, root=tmp_path)

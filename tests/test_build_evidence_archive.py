from __future__ import annotations

import zipfile
from pathlib import Path

import scripts.build_evidence_archive as archive


def test_build_evidence_archive_excludes_operational_keep_patterns(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path
    keep = root / "data" / "reports" / "deepseek_review_summary.json"
    archived = root / "data" / "reports" / "twelvedata_batch_a_manual_apply.md"
    keep.parent.mkdir(parents=True, exist_ok=True)
    keep.write_text("{}", encoding="utf-8")
    archived.write_text("evidence", encoding="utf-8")
    monkeypatch.setattr(archive, "ROOT", root)

    manifest = archive.build_evidence_archive(
        output=root / "output" / "evidence_archive",
        patterns=["data/reports/*"],
    )

    assert manifest["files"] == ["data/reports/twelvedata_batch_a_manual_apply.md"]
    with zipfile.ZipFile(root / manifest["archive"]) as zipped:
        assert zipped.namelist() == ["data/reports/twelvedata_batch_a_manual_apply.md"]

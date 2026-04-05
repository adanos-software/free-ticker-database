from __future__ import annotations

import json
from pathlib import Path

from scripts.summarize_stock_masterfile_verification import aggregate_chunk_summaries, collect_findings


def test_aggregate_chunk_summaries_tracks_missing_chunks(tmp_path: Path) -> None:
    (tmp_path / "chunk-01-of-03.summary.json").write_text(
        json.dumps({"chunk_index": 1, "chunk_count": 3, "items": 2, "status_counts": {"verified": 2}}),
        encoding="utf-8",
    )
    (tmp_path / "chunk-03-of-03.summary.json").write_text(
        json.dumps({"chunk_index": 3, "chunk_count": 3, "items": 1, "status_counts": {"missing_from_official": 1}}),
        encoding="utf-8",
    )
    payload = aggregate_chunk_summaries(tmp_path)
    assert payload["chunk_count"] == 3
    assert payload["completed_chunks"] == 2
    assert payload["missing_chunks"] == [2]
    assert payload["status_counts"] == {"missing_from_official": 1, "verified": 2}


def test_collect_findings_only_includes_bad_statuses(tmp_path: Path) -> None:
    (tmp_path / "chunk-01-of-01.json").write_text(
        json.dumps(
            [
                {"ticker": "A", "status": "verified"},
                {"ticker": "B", "status": "name_mismatch"},
                {"ticker": "C", "status": "cross_exchange_collision"},
                {"ticker": "D", "status": "missing_from_official"},
            ]
        ),
        encoding="utf-8",
    )
    findings = collect_findings(tmp_path)
    assert [row["ticker"] for row in findings] == ["B", "D"]

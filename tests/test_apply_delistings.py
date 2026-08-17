from __future__ import annotations

import csv
import json
from pathlib import Path

from scripts.apply_delistings import apply_delistings
from scripts.lib.delisting_evidence import BSE_STATUS_URL_TEMPLATE, evidence_observation_id


def official_delisting_candidate(**overrides: str) -> dict[str, str]:
    candidate = {
        "exchange": "BSE_IN",
        "ticker": "DEAD",
        "classification": "delisted",
        "name": "Dead Ltd",
        "isin": "INE1",
        "source_key": "bse_india_scrips",
        "source_url": BSE_STATUS_URL_TEMPLATE.format(status="Delisted"),
        "observed_at": "2026-04-05T00:00:00Z",
    }
    candidate.update(overrides)
    candidate["observation_id"] = evidence_observation_id(
        candidate, candidate["observed_at"]
    )
    return candidate


def read_drop_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_drop_rows(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["ticker", "exchange", "confidence", "reason"])
        writer.writeheader()
        writer.writerows(rows)


def test_apply_delistings_writes_only_authoritative_bse_delisted(tmp_path: Path) -> None:
    source = tmp_path / "delisting_report.json"
    drops = tmp_path / "drop_entries.csv"
    write_drop_rows(drops, [])
    source.write_text(
        json.dumps(
            {
                "candidates": [
                    official_delisting_candidate(),
                    {"exchange": "BSE_IN", "ticker": "WAIT", "classification": "suspended", "name": "Wait Ltd", "isin": "INE2"},
                    {"exchange": "NASDAQ", "ticker": "OLD", "classification": "master_absent", "name": "Old Inc", "isin": "US1"},
                ]
            }
        ),
        encoding="utf-8",
    )

    report = apply_delistings(
        delisting_report_json=source,
        drop_entries_csv=drops,
        report_json=tmp_path / "apply.json",
        report_md=tmp_path / "apply.md",
        apply=True,
    )

    assert report["summary"]["applied_rows"] == 1
    assert report["summary"]["blocked_rows"] == 1
    assert report["summary"]["manual_rows"] == 1
    assert read_drop_rows(drops)[0]["ticker"] == "DEAD"
    assert report["manual"][0]["status"] == "manual_rename_vs_delisting_required"


def test_apply_delistings_draft_mode_does_not_mutate_drop_entries(tmp_path: Path) -> None:
    source = tmp_path / "delisting_report.json"
    drops = tmp_path / "drop_entries.csv"
    write_drop_rows(drops, [])
    source.write_text(
        json.dumps({"candidates": [official_delisting_candidate(name="", isin="")]}),
        encoding="utf-8",
    )

    report = apply_delistings(
        delisting_report_json=source,
        drop_entries_csv=drops,
        report_json=tmp_path / "apply.json",
        report_md=tmp_path / "apply.md",
        apply=False,
    )

    assert report["summary"]["drafted_rows"] == 1
    assert read_drop_rows(drops) == []


def test_missing_official_evidence_blocks_auto_drop(tmp_path: Path) -> None:
    source = tmp_path / "delisting_report.json"
    drops = tmp_path / "drop_entries.csv"
    write_drop_rows(drops, [])
    source.write_text(json.dumps({"candidates": [{"exchange": "BSE_IN", "ticker": "DEAD", "classification": "delisted"}]}), encoding="utf-8")
    report = apply_delistings(delisting_report_json=source, drop_entries_csv=drops, report_json=tmp_path / "apply.json", report_md=tmp_path / "apply.md", apply=True)
    assert report["summary"]["applied_rows"] == 0
    assert report["blocked"][0]["status"] == "blocked_missing_official_delisting_evidence"


def test_wrong_source_or_non_bse_url_blocks_auto_drop(tmp_path: Path) -> None:
    source = tmp_path / "delisting_report.json"
    drops = tmp_path / "drop_entries.csv"
    write_drop_rows(drops, [])
    source.write_text(json.dumps({"candidates": [{
        "exchange": "BSE_IN", "ticker": "DEAD", "classification": "delisted",
        "source_key": "not_bse", "source_url": "https://example.test/?status=Delisted",
        "observed_at": "2026-04-05T00:00:00Z",
        "observation_id": "obs_123456789012345678901234",
    }]}), encoding="utf-8")
    report = apply_delistings(
        delisting_report_json=source, drop_entries_csv=drops,
        report_json=tmp_path / "apply.json", report_md=tmp_path / "apply.md", apply=True,
    )
    assert report["summary"]["applied_rows"] == 0
    assert report["blocked"][0]["status"] == "blocked_missing_official_delisting_evidence"


def test_tampered_observation_id_blocks_auto_drop(tmp_path: Path) -> None:
    source = tmp_path / "delisting_report.json"
    drops = tmp_path / "drop_entries.csv"
    write_drop_rows(drops, [])
    candidate = official_delisting_candidate()
    candidate["isin"] = "DIFFERENT"
    source.write_text(json.dumps({"candidates": [candidate]}), encoding="utf-8")

    report = apply_delistings(
        delisting_report_json=source, drop_entries_csv=drops,
        report_json=tmp_path / "apply.json", report_md=tmp_path / "apply.md", apply=True,
    )

    assert report["summary"]["applied_rows"] == 0
    assert report["blocked"][0]["status"] == "blocked_missing_official_delisting_evidence"

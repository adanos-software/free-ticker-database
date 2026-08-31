from __future__ import annotations

import csv
import json
from pathlib import Path

from scripts.apply_delistings import apply_delistings
from scripts.lib.delisting_evidence import (
    BSE_STATUS_URL_TEMPLATE,
    NASDAQ_ADDS_DELETES_URL,
    evidence_observation_id,
)


def official_delisting_candidate(**overrides: str) -> dict[str, str]:
    candidate = {
        "exchange": "BSE_IN",
        "ticker": "DEAD",
        "classification": "delisted",
        "name": "Dead Ltd",
        "isin": "INE002A01018",
        "source_key": "bse_india_scrips",
        "source_url": BSE_STATUS_URL_TEMPLATE.format(status="Delisted"),
        "observed_at": "2026-04-05T00:00:00Z",
    }
    candidate.update(overrides)
    candidate["observation_id"] = evidence_observation_id(
        candidate, candidate["observed_at"]
    )
    return candidate


def official_nasdaq_delete_candidate(**overrides: str) -> dict[str, str]:
    candidate = {
        "exchange": "NASDAQ",
        "ticker": "DEAD",
        "classification": "delisted",
        "name": "Dead Inc",
        "isin": "US09077B1044",
        "source_key": "nasdaq_trading_system_adds_deletes",
        "source_url": NASDAQ_ADDS_DELETES_URL,
        "nasdaq_action": "Delete",
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
    transitions = read_drop_rows(tmp_path / "delisting_transitions.csv")
    assert transitions == [
        {
            "old_listing_key": "BSE_IN::DEAD",
            "new_listing_key": "",
            "event_type": "delisted",
            "identity_type": "exact_isin",
            "identity_value": "INE002A01018",
            "confidence": "0.99",
            "source_key": "bse_india_scrips",
            "source_url": BSE_STATUS_URL_TEMPLATE.format(status="Delisted"),
        }
    ]
    assert report["manual"][0]["status"] == "manual_rename_vs_delisting_required"


def test_apply_delistings_draft_mode_does_not_mutate_drop_entries(tmp_path: Path) -> None:
    source = tmp_path / "delisting_report.json"
    drops = tmp_path / "drop_entries.csv"
    write_drop_rows(drops, [])
    source.write_text(
        json.dumps({"candidates": [official_delisting_candidate(name="")]}),
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


def test_official_delete_without_exact_isin_blocks_auto_drop(tmp_path: Path) -> None:
    source = tmp_path / "delisting_report.json"
    drops = tmp_path / "drop_entries.csv"
    write_drop_rows(drops, [])
    source.write_text(
        json.dumps({"candidates": [official_nasdaq_delete_candidate(isin="")]}),
        encoding="utf-8",
    )

    report = apply_delistings(
        delisting_report_json=source,
        drop_entries_csv=drops,
        report_json=tmp_path / "apply.json",
        report_md=tmp_path / "apply.md",
        apply=True,
    )

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


def test_apply_delistings_writes_official_nasdaq_delete(tmp_path: Path) -> None:
    source = tmp_path / "delisting_report.json"
    drops = tmp_path / "drop_entries.csv"
    write_drop_rows(drops, [])
    source.write_text(
        json.dumps({"candidates": [official_nasdaq_delete_candidate()]}),
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
    assert read_drop_rows(drops)[0]["ticker"] == "DEAD"
    assert read_drop_rows(drops)[0]["exchange"] == "NASDAQ"
    assert read_drop_rows(tmp_path / "delisting_transitions.csv")[0]["old_listing_key"] == "NASDAQ::DEAD"


def test_nasdaq_delete_without_action_or_wrong_url_is_blocked(tmp_path: Path) -> None:
    source = tmp_path / "delisting_report.json"
    drops = tmp_path / "drop_entries.csv"
    write_drop_rows(drops, [])
    missing_action = official_nasdaq_delete_candidate()
    missing_action.pop("nasdaq_action")
    missing_action["observation_id"] = evidence_observation_id(
        missing_action, missing_action["observed_at"]
    )
    wrong_url = official_nasdaq_delete_candidate(
        ticker="OTHER",
        source_url="https://example.test/TradingSystemAddsDeletes.txt",
    )
    source.write_text(
        json.dumps({"candidates": [missing_action, wrong_url]}),
        encoding="utf-8",
    )

    report = apply_delistings(
        delisting_report_json=source,
        drop_entries_csv=drops,
        report_json=tmp_path / "apply.json",
        report_md=tmp_path / "apply.md",
        apply=True,
    )

    assert report["summary"]["applied_rows"] == 0
    assert report["summary"]["blocked_rows"] == 2
    assert {row["status"] for row in report["blocked"]} == {
        "blocked_missing_official_delisting_evidence"
    }

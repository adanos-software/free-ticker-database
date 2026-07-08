from datetime import datetime, timezone
import json
import subprocess

import pytest

import scripts.build_drift_report as drift
from scripts.build_drift_report import (
    build_markdown,
    dataset_built_at,
    official_recall_regressions,
    pending_renames,
    previous_drift_report,
    quality_indicators,
    quality_regressions,
    rename_triage_rows,
    staleness_days,
)


def test_staleness_days_parses_iso_and_handles_missing():
    now = datetime(2026, 6, 16, 12, 0, 0, tzinfo=timezone.utc)
    assert staleness_days("2026-06-14T12:00:00Z", now=now) == 2.0
    assert staleness_days(None, now=now) is None
    assert staleness_days("not-a-date", now=now) is None


def test_dataset_built_at_is_iso_or_none():
    built = dataset_built_at()
    assert built is None or built.endswith("Z") or "+" in built


def test_dataset_built_at_uses_committed_coverage_report(tmp_path, monkeypatch):
    coverage = tmp_path / "coverage_report.json"
    coverage.write_text(json.dumps({"_meta": {"generated_at": "2026-07-06T11:16:55Z"}}), encoding="utf-8")
    monkeypatch.setattr(drift, "COVERAGE_REPORT_JSON", coverage)

    assert dataset_built_at() == "2026-07-06T11:16:55Z"


def test_pending_renames_shape():
    rows = pending_renames()
    assert isinstance(rows, list)
    for r in rows[:50]:
        assert set(r) >= {"old_symbol", "new_symbol", "new_company_name", "effective_date"}
        # by construction the new symbol must not already be a current ticker
        assert r["new_symbol"] and r["old_symbol"]


def test_rename_triage_splits_apply_ready_and_manual_rows(tmp_path, monkeypatch):
    tickers = tmp_path / "tickers.csv"
    tickers.write_text("ticker\nAPPLYOLD\nBLOCKOLD\n", encoding="utf-8")
    review = tmp_path / "symbol_changes_review.csv"
    review.write_text(
        "\n".join(
            [
                "effective_date,old_symbol,new_symbol,new_company_name,symbol_change_workflow_queue",
                "2026-01-01,APPLYOLD,APPLYNEW,Apply Inc,review_verified_rename_or_delisting",
                "2026-01-02,BLOCKOLD,BLOCKNEW,Block Inc,review_verified_rename_or_delisting",
                "",
            ]
        ),
        encoding="utf-8",
    )
    apply_json = tmp_path / "symbol_changes_apply.json"
    apply_json.write_text(
        json.dumps(
            {
                "accepted": [{"effective_date": "2026-01-01", "old_symbol": "APPLYOLD", "new_symbol": "APPLYNEW"}],
                "blocked": [
                    {
                        "effective_date": "2026-01-02",
                        "old_symbol": "BLOCKOLD",
                        "new_symbol": "BLOCKNEW",
                        "status": "manual_missing_isin",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(drift, "TICKERS_CSV", tickers)
    monkeypatch.setattr(drift, "SYMBOL_CHANGES_REVIEW_CSV", review)
    monkeypatch.setattr(drift, "SYMBOL_CHANGES_APPLY_JSON", apply_json)

    rows = rename_triage_rows()
    by_old = {row["old_symbol"]: row for row in rows}

    assert by_old["APPLYOLD"]["triage_status"] == "apply_ready"
    assert by_old["BLOCKOLD"]["triage_status"] == "blocked_or_manual"
    assert [row["old_symbol"] for row in pending_renames(rows)] == ["APPLYOLD"]


def test_rename_triage_raw_feed_fallback_counts_as_pending(tmp_path, monkeypatch):
    tickers = tmp_path / "tickers.csv"
    tickers.write_text("ticker\nOLD\n", encoding="utf-8")
    symbol_changes = tmp_path / "symbol_changes.csv"
    symbol_changes.write_text(
        "effective_date,old_symbol,new_symbol,new_company_name\n2026-01-01,OLD,NEW,Fallback Inc\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(drift, "TICKERS_CSV", tickers)
    monkeypatch.setattr(drift, "SYMBOL_CHANGES_REVIEW_CSV", tmp_path / "missing_review.csv")
    monkeypatch.setattr(drift, "SYMBOL_CHANGES_CSV", symbol_changes)
    monkeypatch.setattr(drift, "SYMBOL_CHANGES_APPLY_JSON", tmp_path / "missing_apply.json")

    rows = rename_triage_rows()

    assert rows[0]["triage_source"] == "symbol_changes_csv_fallback"
    assert rows[0]["triage_status"] == "fallback_pending"
    assert [row["old_symbol"] for row in pending_renames(rows)] == ["OLD"]


def test_rename_triage_invalid_apply_json_fails_loudly(tmp_path, monkeypatch):
    tickers = tmp_path / "tickers.csv"
    tickers.write_text("ticker\nOLD\n", encoding="utf-8")
    review = tmp_path / "symbol_changes_review.csv"
    review.write_text(
        "effective_date,old_symbol,new_symbol,new_company_name,symbol_change_workflow_queue\n"
        "2026-01-01,OLD,NEW,Broken Inc,review_verified_rename_or_delisting\n",
        encoding="utf-8",
    )
    apply_json = tmp_path / "symbol_changes_apply.json"
    apply_json.write_text("{broken", encoding="utf-8")
    monkeypatch.setattr(drift, "TICKERS_CSV", tickers)
    monkeypatch.setattr(drift, "SYMBOL_CHANGES_REVIEW_CSV", review)
    monkeypatch.setattr(drift, "SYMBOL_CHANGES_APPLY_JSON", apply_json)

    with pytest.raises(json.JSONDecodeError):
        rename_triage_rows()


def test_quality_indicators_are_ints():
    q = quality_indicators()
    assert isinstance(q, dict)
    for k, v in q.items():
        assert isinstance(v, int) and v >= 0


def test_quality_regressions_compare_against_previous_report():
    rows = quality_regressions(
        {
            "source_gap_rows": 11,
            "expected_missing_primary_isin": 2,
            "missing_stock_sector": 0,
            "missing_etf_category": 1,
        },
        {
            "quality_indicators": {
                "source_gap_rows": 10,
                "expected_missing_primary_isin": 2,
                "missing_stock_sector": 1,
                "missing_etf_category": 1,
            }
        },
    )

    assert rows == [{"metric": "source_gap_rows", "previous": 10, "current": 11, "delta": 1}]


def test_previous_drift_report_prefers_git_head_baseline(tmp_path, monkeypatch):
    report = tmp_path / "drift_report.json"
    report.write_text(json.dumps({"quality_indicators": {"source_gap_rows": 99}}), encoding="utf-8")
    monkeypatch.setattr(drift, "REPORT_JSON", report)

    def fake_run(*args, **kwargs):
        assert args[0][:2] == ["git", "show"]
        return subprocess.CompletedProcess(
            args=args[0],
            returncode=0,
            stdout=json.dumps({"quality_indicators": {"source_gap_rows": 10}}),
            stderr="",
        )

    monkeypatch.setattr(drift.subprocess, "run", fake_run)

    assert previous_drift_report()["quality_indicators"]["source_gap_rows"] == 10


def test_official_recall_regressions_compare_missing_counts_by_exchange():
    rows = official_recall_regressions(
        {
            "NYSE": {
                "official_recall_missing": 3,
                "collision_adjusted_recall_missing": 1,
            },
            "NASDAQ": {
                "official_recall_missing": 1,
                "collision_adjusted_recall_missing": 0,
            },
        },
        {
            "official_recall_indicators": {
                "NYSE": {
                    "official_recall_missing": 2,
                    "collision_adjusted_recall_missing": 1,
                },
                "NASDAQ": {
                    "official_recall_missing": 2,
                    "collision_adjusted_recall_missing": 0,
                },
            }
        },
    )

    assert rows == [
        {
            "exchange": "NYSE",
            "metric": "official_recall_missing",
            "previous": 2,
            "current": 3,
            "delta": 1,
        }
    ]


def test_build_markdown_renders():
    md = build_markdown({
        "generated_at": "2026-06-16T00:00:00+00:00",
        "built_at": "2026-06-16T00:00:00Z",
        "staleness_days": 1.0,
        "stale_threshold_days": 45.0,
        "pending_renames_count": 1,
        "pending_renames_sample": [{"old_symbol": "AAA", "new_symbol": "BBB",
                                    "new_company_name": "Example Inc", "effective_date": "2026-06-01"}],
        "manual_review_count": 1,
        "quality_indicators": {"missing_stock_sector": 23},
        "drift_detected": True,
    })
    assert "Drift / freshness report" in md
    assert "AAA -> BBB" in md
    assert "... and 1 more" in md
    assert "drift_detected: True" in md

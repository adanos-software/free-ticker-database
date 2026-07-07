from datetime import datetime, timezone
import json

import pytest

import scripts.build_drift_report as drift
from scripts.build_drift_report import (
    build_markdown,
    dataset_built_at,
    pending_renames,
    quality_indicators,
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

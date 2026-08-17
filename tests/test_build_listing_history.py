from __future__ import annotations

from scripts.build_listing_history import (
    build_event_rows, build_snapshot, build_status_evidence_event_rows,
    delisting_apply_status_rows, listing_status_on_date, load_change_evidence,
    merge_status_history,
)
from scripts.lib.merge_evidence import row_fingerprint
from scripts.lib.delisting_evidence import BSE_STATUS_URL_TEMPLATE, evidence_observation_id


def official_status_row(ticker: str) -> dict[str, str]:
    row = {
        "listing_key": f"BSE_IN::{ticker}", "ticker": ticker, "exchange": "BSE_IN",
        "classification": "delisted", "source_key": "bse_india_scrips",
        "source_url": BSE_STATUS_URL_TEMPLATE.format(status="Delisted"),
        "observed_at": "2026-08-17T00:00:00Z", "isin": "",
    }
    row["observation_id"] = evidence_observation_id(row, row["observed_at"])
    return row


def test_snapshot_absence_is_not_delisting() -> None:
    previous = [{"listing_key": "NYSE::AAA", "ticker": "AAA", "exchange": "NYSE", "name": "Alpha"}]
    events = build_event_rows(previous, [], "2026-08-17T00:00:00Z")
    assert events[0]["event_type"] == "not_observed"
    assert events[0]["evidence_status"] == "observed_unverified"


def test_critical_changes_record_exact_before_fingerprint() -> None:
    previous = [{"listing_key": "NYSE::AAA", "ticker": "AAA", "exchange": "NYSE", "name": "Old", "isin": "US1"}]
    current = [{"listing_key": "NYSE::AAA", "ticker": "AAA", "exchange": "NYSE", "name": "New", "isin": "US1"}]
    event = build_event_rows(previous, current, "2026-08-17T00:00:00Z")[0]
    assert event["field_name"] == "name"
    assert event["before_row_sha256"] == row_fingerprint(previous[0])


def test_snapshot_absence_does_not_create_status_interval() -> None:
    previous = [{"listing_key": "NYSE::AAA", "ticker": "AAA", "exchange": "NYSE", "name": "Alpha"}]
    assert merge_status_history([], previous, [], "2026-08-17T00:00:00Z") == []


def test_only_applied_delisting_with_official_evidence_enters_history() -> None:
    payload = {
        "summary": {"generated_at": "2026-08-17T00:00:00Z", "delisting_report_json": "data/reports/delisting_report.json"},
        "applied": [official_status_row("DEAD")],
        "drafted": [{"ticker": "DRAFT", "exchange": "BSE_IN", "classification": "delisted"}],
    }
    rows = delisting_apply_status_rows(payload)
    assert [row["listing_key"] for row in rows] == ["BSE_IN::DEAD"]
    event = build_status_evidence_event_rows(rows, [], [{"listing_key": "BSE_IN::DEAD", "ticker": "DEAD", "exchange": "BSE_IN", "name": "Dead Ltd"}])[0]
    assert event["event_type"] == "delisted"
    assert event["observation_id"] == official_status_row("DEAD")["observation_id"]
    assert event["before_row_sha256"]


def test_point_in_time_status_prefers_latest_effective_evidence() -> None:
    history = [
        {"listing_key": "X::A", "ticker": "A", "exchange": "X", "status": "active", "first_observed_at": "2026-01-01T00:00:00Z", "last_observed_at": "2026-12-31T00:00:00Z", "effective_at": "", "evidence_status": "observed_unverified"},
        {"listing_key": "X::A", "ticker": "A", "exchange": "X", "status": "delisted", "first_observed_at": "2026-06-01T00:00:00Z", "last_observed_at": "2026-06-01T00:00:00Z", "effective_at": "2026-05-30T00:00:00Z", "evidence_status": "official"},
    ]
    assert listing_status_on_date(history, "X::A", "2026-06-02T00:00:00Z") == "delisted"


def test_snapshot_builder_preserves_listing_key_and_sector_model() -> None:
    snapshot = build_snapshot([{"listing_key": "X::A", "ticker": "A", "exchange": "X", "name": "Alpha", "asset_type": "Stock", "country": "US", "country_code": "US", "isin": "", "sector": "Industrials"}], "2026-08-17T00:00:00Z")
    assert snapshot[0]["stock_sector"] == "Industrials"


def test_already_applied_official_delisting_enters_history() -> None:
    payload = {
        "summary": {"generated_at": "2026-08-17T00:00:00Z"},
        "already_applied": [official_status_row("OLD")],
    }
    assert [row["listing_key"] for row in delisting_apply_status_rows(payload)] == ["BSE_IN::OLD"]

from __future__ import annotations

from scripts.build_listing_history import (
    build_event_rows, build_snapshot, build_status_evidence_event_rows,
    delisting_apply_status_rows, listing_status_on_date, load_change_evidence,
    merge_status_history,
)
from scripts.lib.merge_evidence import row_fingerprint
from scripts.lib.delisting_evidence import (
    BSE_STATUS_URL_TEMPLATE,
    NASDAQ_ADDS_DELETES_URL,
    evidence_observation_id,
)


def official_status_row(ticker: str) -> dict[str, str]:
    row = {
        "listing_key": f"BSE_IN::{ticker}", "ticker": ticker, "exchange": "BSE_IN",
        "classification": "delisted", "source_key": "bse_india_scrips",
        "source_url": BSE_STATUS_URL_TEMPLATE.format(status="Delisted"),
        "observed_at": "2026-08-17T00:00:00Z", "isin": "",
    }
    row["observation_id"] = evidence_observation_id(row, row["observed_at"])
    return row


def official_nasdaq_status_row(ticker: str) -> dict[str, str]:
    row = {
        "listing_key": f"NASDAQ::{ticker}", "ticker": ticker, "exchange": "NASDAQ",
        "classification": "delisted", "source_key": "nasdaq_trading_system_adds_deletes",
        "source_url": NASDAQ_ADDS_DELETES_URL, "nasdaq_action": "Delete",
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


def test_applied_nasdaq_delete_enters_history() -> None:
    payload = {
        "summary": {"generated_at": "2026-08-17T00:00:00Z"},
        "applied": [official_nasdaq_status_row("DEAD")],
    }
    rows = delisting_apply_status_rows(payload)
    assert [row["listing_key"] for row in rows] == ["NASDAQ::DEAD"]
    assert rows[0]["evidence_status"] == "official"


def test_reviewed_metadata_updates_stamp_taxonomy_changes(monkeypatch) -> None:
    monkeypatch.setattr(
        "scripts.build_listing_history.load_csv",
        lambda path: (
            [{
                "ticker": "WDNA",
                "exchange": "BATS",
                "field": "etf_category",
                "decision": "update",
                "proposed_value": "Equity",
            }]
            if str(path).endswith("metadata_updates.csv")
            else []
        ),
    )
    previous = [{
        "listing_key": "BATS::WDNA", "ticker": "WDNA", "exchange": "BATS",
        "name": "WisdomTree BioRevolution Fund", "asset_type": "ETF",
        "etf_category": "", "isin": "US97717Y6187",
    }]
    current = [{**previous[0], "etf_category": "Equity"}]
    from scripts.build_listing_history import load_change_evidence
    event = build_event_rows(previous, current, "2026-08-18T06:00:00Z", load_change_evidence())[0]
    assert event["event_type"] == "taxonomy_changed"
    assert event["evidence_status"] == "reviewed"
    assert event["source_report"] == "data/review_overrides/metadata_updates.csv"
    assert event["observation_id"] == "BATS::WDNA:etf_category"


def test_evidenced_isin_fill_stamps_inferred_country_change() -> None:
    previous = [{
        "listing_key": "SET::AAA", "ticker": "AAA", "exchange": "SET",
        "name": "Alpha", "asset_type": "Stock", "country": "Thailand",
        "country_code": "TH", "isin": "",
    }]
    current = [{
        **previous[0],
        "isin": "US0378331005",
        "country": "United States",
        "country_code": "US",
    }]
    evidence = {
        ("SET::AAA", "isin", "*", "US0378331005"): {
            "source_key": "review_metadata_updates",
            "source_url": "",
            "source_report": "data/review_overrides/metadata_updates.csv",
            "observation_id": "SET::AAA:isin",
            "evidence_status": "reviewed",
        }
    }
    events = {event["field_name"]: event for event in build_event_rows(
        previous, current, "2026-08-18T07:00:00Z", evidence
    )}
    assert events["isin"]["evidence_status"] == "reviewed"
    assert events["country"]["evidence_status"] == "verified"
    assert events["country_code"]["evidence_status"] == "verified"
    assert events["country"]["new_value"] == "United States"
    assert events["country_code"]["observation_id"].endswith(":isin_prefix_country")


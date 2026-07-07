from __future__ import annotations

from scripts.build_listing_history import (
    build_daily_summary,
    build_event_rows,
    build_snapshot,
    build_status_evidence_event_rows,
    compact_legacy_status_history,
    delisting_apply_status_rows,
    listing_status_on_date,
    merge_status_history,
    was_listing_active_on_date,
)


def test_build_event_rows_detects_listed_renamed_and_delisted():
    previous = [
        {"ticker": "AAA", "exchange": "NYSE", "name": "Old Name"},
        {"ticker": "DEL", "exchange": "NASDAQ", "name": "Delisted Co"},
    ]
    current = [
        {"ticker": "AAA", "exchange": "NYSE", "name": "New Name"},
        {"ticker": "NEW", "exchange": "NASDAQ", "name": "New Listing"},
    ]

    events = build_event_rows(previous, current, "2026-04-02T00:00:00Z")

    assert events == [
        {
            "listing_key": "NYSE::AAA",
            "ticker": "AAA",
            "exchange": "NYSE",
            "event_type": "renamed",
            "old_value": "Old Name",
            "new_value": "New Name",
            "observed_at": "2026-04-02T00:00:00Z",
        },
        {
            "listing_key": "NASDAQ::NEW",
            "ticker": "NEW",
            "exchange": "NASDAQ",
            "event_type": "listed",
            "old_value": "",
            "new_value": "New Listing",
            "observed_at": "2026-04-02T00:00:00Z",
        },
        {
            "listing_key": "NASDAQ::DEL",
            "ticker": "DEL",
            "exchange": "NASDAQ",
            "event_type": "delisted",
            "old_value": "Delisted Co",
            "new_value": "",
            "observed_at": "2026-04-02T00:00:00Z",
        },
    ]


def test_build_event_rows_ignores_initial_baseline_snapshot():
    current = [{"ticker": "AAA", "exchange": "NYSE", "name": "Alpha"}]

    events = build_event_rows([], current, "2026-04-02T00:00:00Z")

    assert events == []


def test_compact_legacy_status_history_merges_repeated_snapshots_into_intervals():
    legacy_rows = [
        {"listing_key": "", "ticker": "AAA", "exchange": "NYSE", "status": "active", "observed_at": "2026-04-01T00:00:00Z"},
        {"listing_key": "", "ticker": "AAA", "exchange": "NYSE", "status": "active", "observed_at": "2026-04-02T00:00:00Z"},
        {"listing_key": "", "ticker": "AAA", "exchange": "NYSE", "status": "delisted", "observed_at": "2026-04-03T00:00:00Z"},
        {"listing_key": "", "ticker": "AAA", "exchange": "NYSE", "status": "delisted", "observed_at": "2026-04-04T00:00:00Z"},
    ]

    history = compact_legacy_status_history(legacy_rows)

    assert history == [
        {
            "listing_key": "NYSE::AAA",
            "ticker": "AAA",
            "exchange": "NYSE",
            "status": "active",
            "first_observed_at": "2026-04-01T00:00:00Z",
            "last_observed_at": "2026-04-02T00:00:00Z",
            "effective_at": "",
            "status_source": "snapshot",
            "source_report": "",
        },
        {
            "listing_key": "NYSE::AAA",
            "ticker": "AAA",
            "exchange": "NYSE",
            "status": "delisted",
            "first_observed_at": "2026-04-03T00:00:00Z",
            "last_observed_at": "2026-04-04T00:00:00Z",
            "effective_at": "",
            "status_source": "snapshot",
            "source_report": "",
        },
    ]


def test_merge_status_history_adds_active_and_delisted_intervals():
    previous_snapshot = [
        {"ticker": "AAA", "exchange": "NYSE", "name": "Alpha"},
        {"ticker": "DEL", "exchange": "NASDAQ", "name": "Delisted Co"},
    ]
    current_snapshot = [
        {"ticker": "AAA", "exchange": "NYSE", "name": "Alpha"},
        {"ticker": "NEW", "exchange": "NASDAQ", "name": "New Co"},
    ]

    history = merge_status_history([], previous_snapshot, current_snapshot, "2026-04-02T00:00:00Z")

    assert history == [
        {
            "listing_key": "NYSE::AAA",
            "ticker": "AAA",
            "exchange": "NYSE",
            "status": "active",
            "first_observed_at": "2026-04-02T00:00:00Z",
            "last_observed_at": "2026-04-02T00:00:00Z",
            "effective_at": "",
            "status_source": "snapshot",
            "source_report": "",
        },
        {
            "listing_key": "NASDAQ::DEL",
            "ticker": "DEL",
            "exchange": "NASDAQ",
            "status": "delisted",
            "first_observed_at": "2026-04-02T00:00:00Z",
            "last_observed_at": "2026-04-02T00:00:00Z",
            "effective_at": "",
            "status_source": "snapshot",
            "source_report": "",
        },
        {
            "listing_key": "NASDAQ::NEW",
            "ticker": "NEW",
            "exchange": "NASDAQ",
            "status": "active",
            "first_observed_at": "2026-04-02T00:00:00Z",
            "last_observed_at": "2026-04-02T00:00:00Z",
            "effective_at": "",
            "status_source": "snapshot",
            "source_report": "",
        },
    ]


def test_merge_status_history_extends_existing_active_interval_without_duplicate_rows():
    existing_rows = [
        {
            "listing_key": "NYSE::AAA",
            "ticker": "AAA",
            "exchange": "NYSE",
            "status": "active",
            "first_observed_at": "2026-04-01T00:00:00Z",
            "last_observed_at": "2026-04-02T00:00:00Z",
            "effective_at": "",
            "status_source": "snapshot",
            "source_report": "",
        }
    ]
    previous_snapshot = [{"ticker": "AAA", "exchange": "NYSE", "name": "Alpha"}]
    current_snapshot = [{"ticker": "AAA", "exchange": "NYSE", "name": "Alpha"}]

    history = merge_status_history(existing_rows, previous_snapshot, current_snapshot, "2026-04-03T00:00:00Z")

    assert history == [
        {
            "listing_key": "NYSE::AAA",
            "ticker": "AAA",
            "exchange": "NYSE",
            "status": "active",
            "first_observed_at": "2026-04-01T00:00:00Z",
            "last_observed_at": "2026-04-03T00:00:00Z",
            "effective_at": "",
            "status_source": "snapshot",
            "source_report": "",
        }
    ]


def test_build_snapshot_sets_active_status_and_listing_key():
    rows = [
        {
            "ticker": "AAA",
            "exchange": "NYSE",
            "name": "Alpha",
            "asset_type": "Stock",
            "country": "United States",
            "country_code": "US",
            "isin": "US0000000001",
            "sector": "Industrials",
        }
    ]

    snapshot = build_snapshot(rows, "2026-04-02T00:00:00Z")

    assert snapshot == [
        {
            "listing_key": "NYSE::AAA",
            "ticker": "AAA",
            "exchange": "NYSE",
            "name": "Alpha",
            "asset_type": "Stock",
            "country": "United States",
            "country_code": "US",
            "isin": "US0000000001",
            "stock_sector": "Industrials",
            "etf_category": "",
            "status": "active",
            "observed_at": "2026-04-02T00:00:00Z",
        }
    ]


def test_build_daily_summary_counts_events_and_active_rows():
    current_snapshot = [
        {"ticker": "AAA", "exchange": "NYSE"},
        {"ticker": "NEW", "exchange": "NASDAQ"},
        {"ticker": "BBB", "exchange": "NYSE"},
    ]
    new_events = [
        {"exchange": "NYSE", "event_type": "renamed"},
        {"exchange": "NASDAQ", "event_type": "listed"},
        {"exchange": "NASDAQ", "event_type": "delisted"},
    ]

    summary, rows = build_daily_summary(current_snapshot, new_events, "2026-04-02T00:00:00Z")

    assert summary == {
        "observed_at": "2026-04-02T00:00:00Z",
        "active_snapshot_rows": 3,
        "new_events": 3,
        "listed": 1,
        "renamed": 1,
        "suspended": 0,
        "delisted": 1,
        "exchange_rows": 2,
    }
    assert rows == [
        {
            "observed_at": "2026-04-02T00:00:00Z",
            "exchange": "NASDAQ",
            "listed": 1,
            "renamed": 0,
            "suspended": 0,
            "delisted": 1,
            "active_snapshot_rows": 1,
        },
        {
            "observed_at": "2026-04-02T00:00:00Z",
            "exchange": "NYSE",
            "listed": 0,
            "renamed": 1,
            "suspended": 0,
            "delisted": 0,
            "active_snapshot_rows": 2,
        },
    ]


def test_delisting_apply_status_rows_promotes_suspended_and_delisted_evidence():
    payload = {
        "summary": {
            "generated_at": "2026-04-05T00:00:00Z",
            "delisting_report_json": "data/reports/delisting_report.json",
        },
        "applied": [
            {"exchange": "BSE_IN", "ticker": "DEAD", "classification": "delisted"},
        ],
        "blocked": [
            {"exchange": "BSE_IN", "ticker": "WAIT", "classification": "suspended"},
            {"exchange": "BSE_IN", "ticker": "MAN", "classification": "master_absent"},
        ],
    }

    rows = delisting_apply_status_rows(payload)

    assert rows == [
        {
            "listing_key": "BSE_IN::DEAD",
            "ticker": "DEAD",
            "exchange": "BSE_IN",
            "status": "delisted",
            "first_observed_at": "2026-04-05T00:00:00Z",
            "last_observed_at": "2026-04-05T00:00:00Z",
            "effective_at": "2026-04-05T00:00:00Z",
            "status_source": "delisting_apply",
            "source_report": "data/reports/delisting_report.json",
        },
        {
            "listing_key": "BSE_IN::WAIT",
            "ticker": "WAIT",
            "exchange": "BSE_IN",
            "status": "suspended",
            "first_observed_at": "2026-04-05T00:00:00Z",
            "last_observed_at": "2026-04-05T00:00:00Z",
            "effective_at": "2026-04-05T00:00:00Z",
            "status_source": "delisting_apply",
            "source_report": "data/reports/delisting_report.json",
        },
    ]


def test_merge_status_history_uses_status_evidence_and_answers_active_on_date():
    evidence = [
        {
            "listing_key": "BSE_IN::WAIT",
            "ticker": "WAIT",
            "exchange": "BSE_IN",
            "status": "suspended",
            "first_observed_at": "2026-04-05T00:00:00Z",
            "last_observed_at": "2026-04-05T00:00:00Z",
            "effective_at": "2026-04-04T00:00:00Z",
            "status_source": "delisting_apply",
            "source_report": "data/reports/delisting_report.json",
        }
    ]

    history = merge_status_history(
        [],
        [],
        [{"ticker": "WAIT", "exchange": "BSE_IN", "name": "Wait Ltd"}],
        "2026-04-03T00:00:00Z",
        status_evidence_rows=evidence,
    )

    assert [row["status"] for row in history] == ["active", "suspended"]
    assert was_listing_active_on_date(history, "BSE_IN::WAIT", "2026-04-03T00:00:00Z") is True
    assert listing_status_on_date(history, "BSE_IN::WAIT", "2026-04-04T00:00:00Z") == "suspended"


def test_listing_status_on_date_prefers_latest_effective_interval_when_overlapping():
    history = [
        {
            "listing_key": "NYSE::AAA",
            "ticker": "AAA",
            "exchange": "NYSE",
            "status": "active",
            "first_observed_at": "2026-04-01T00:00:00Z",
            "last_observed_at": "2026-04-10T00:00:00Z",
            "effective_at": "",
            "status_source": "snapshot",
            "source_report": "",
        },
        {
            "listing_key": "NYSE::AAA",
            "ticker": "AAA",
            "exchange": "NYSE",
            "status": "suspended",
            "first_observed_at": "2026-04-05T00:00:00Z",
            "last_observed_at": "2026-04-08T00:00:00Z",
            "effective_at": "2026-04-05T00:00:00Z",
            "status_source": "delisting_apply",
            "source_report": "data/reports/delisting_report.json",
        },
    ]

    assert listing_status_on_date(history, "NYSE::AAA", "2026-04-06T00:00:00Z") == "suspended"


def test_build_status_evidence_event_rows_dedupes_existing_events():
    evidence = [
        {
            "listing_key": "BSE_IN::WAIT",
            "ticker": "WAIT",
            "exchange": "BSE_IN",
            "status": "suspended",
            "effective_at": "2026-04-04T00:00:00Z",
        }
    ]

    assert build_status_evidence_event_rows(evidence, []) == [
        {
            "listing_key": "BSE_IN::WAIT",
            "ticker": "WAIT",
            "exchange": "BSE_IN",
            "event_type": "suspended",
            "old_value": "",
            "new_value": "suspended",
            "observed_at": "2026-04-04T00:00:00Z",
        }
    ]
    assert build_status_evidence_event_rows(evidence, [{"listing_key": "BSE_IN::WAIT", "event_type": "suspended", "observed_at": "2026-04-04T00:00:00Z"}]) == []

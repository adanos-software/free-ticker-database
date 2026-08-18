from __future__ import annotations

import pytest

from scripts.lib.listing_lifecycles import build_listing_lifecycles


def test_untrusted_delisting_does_not_split_lifecycle() -> None:
    rows = build_listing_lifecycles(
        [{"listing_key": "X::A", "ticker": "A", "exchange": "X"}],
        [{"listing_key": "X::A", "ticker": "A", "exchange": "X", "status": "delisted", "effective_at": "2026-01-01T00:00:00Z", "evidence_status": "observed_unverified"}],
        observed_at="2026-08-17T00:00:00Z",
    )
    assert len(rows) == 1
    assert rows[0].status == "active"


def test_official_delisting_and_reappearance_create_two_lifecycles() -> None:
    rows = build_listing_lifecycles(
        [{"listing_key": "X::A", "ticker": "A", "exchange": "X"}],
        [
            {"listing_key": "X::A", "ticker": "A", "exchange": "X", "status": "active", "first_observed_at": "2025-01-01T00:00:00Z", "evidence_status": "observed_unverified"},
            {"listing_key": "X::A", "ticker": "A", "exchange": "X", "status": "delisted", "effective_at": "2026-01-01T00:00:00Z", "evidence_status": "official", "status_source": "delisting_apply", "source_report": "data/reports/delisting_apply.json"},
            {"listing_key": "X::A", "ticker": "A", "exchange": "X", "status": "active", "first_observed_at": "2026-08-01T00:00:00Z", "evidence_status": "current_snapshot"},
        ],
        observed_at="2026-08-17T00:00:00Z",
    )
    assert [row.status for row in rows] == ["delisted", "active"]
    assert rows[0].valid_to == "2026-01-01T00:00:00Z"
    assert rows[1].valid_from == "2026-08-01T00:00:00Z"
    assert rows[0].listing_id != rows[1].listing_id


def test_historical_only_requires_trusted_delisting() -> None:
    rows = build_listing_lifecycles(
        [],
        [{"listing_key": "X::OLD", "ticker": "OLD", "exchange": "X", "status": "delisted", "effective_at": "2026-01-01T00:00:00Z", "evidence_status": "official", "status_source": "delisting_apply", "source_report": "data/reports/delisting_apply.json"}],
        observed_at="2026-08-17T00:00:00Z",
    )
    assert len(rows) == 1 and rows[0].status == "delisted"


def test_duplicate_current_listing_key_fails() -> None:
    try:
        build_listing_lifecycles(
            [{"listing_key": "X::A"}, {"listing_key": "X::A"}], [], observed_at="2026-08-17T00:00:00Z"
        )
    except ValueError as exc:
        assert "duplicate current listing key" in str(exc)
    else:
        raise AssertionError("expected duplicate key failure")


def test_repeated_delisting_evidence_does_not_create_fake_reuse() -> None:
    rows = build_listing_lifecycles(
        [],
        [
            {"listing_key": "X::OLD", "ticker": "OLD", "exchange": "X", "status": "delisted", "effective_at": "2026-01-01T00:00:00Z", "evidence_status": "official", "status_source": "delisting_apply", "source_report": "data/reports/delisting_apply.json"},
            {"listing_key": "X::OLD", "ticker": "OLD", "exchange": "X", "status": "delisted", "effective_at": "2026-02-01T00:00:00Z", "evidence_status": "official", "status_source": "delisting_apply", "source_report": "data/reports/delisting_apply.json"},
        ],
        observed_at="2026-08-17T00:00:00Z",
    )
    assert len(rows) == 1
    assert rows[0].valid_to == "2026-01-01T00:00:00Z"


def test_two_delistings_require_intervening_active_lifecycle() -> None:
    rows = build_listing_lifecycles(
        [],
        [
            {"listing_key": "X::A", "ticker": "A", "exchange": "X", "status": "active", "first_observed_at": "2025-01-01T00:00:00Z"},
            {"listing_key": "X::A", "ticker": "A", "exchange": "X", "status": "delisted", "effective_at": "2025-06-01T00:00:00Z", "evidence_status": "official", "status_source": "delisting_apply", "source_report": "data/reports/delisting_apply.json"},
            {"listing_key": "X::A", "ticker": "A", "exchange": "X", "status": "active", "first_observed_at": "2026-01-01T00:00:00Z"},
            {"listing_key": "X::A", "ticker": "A", "exchange": "X", "status": "delisted", "effective_at": "2026-06-01T00:00:00Z", "evidence_status": "official", "status_source": "delisting_apply", "source_report": "data/reports/delisting_apply.json"},
        ],
        observed_at="2026-08-17T00:00:00Z",
    )
    assert [row.valid_to for row in rows] == [
        "2025-06-01T00:00:00Z", "2026-06-01T00:00:00Z"
    ]


def test_bare_official_label_without_provenance_does_not_split_lifecycle() -> None:
    rows = build_listing_lifecycles(
        [{"listing_key": "X::A", "ticker": "A", "exchange": "X"}],
        [{
            "listing_key": "X::A", "status": "delisted",
            "effective_at": "2026-01-01T00:00:00Z", "evidence_status": "official",
        }],
        observed_at="2026-08-17T00:00:00Z",
    )
    assert len(rows) == 1
    assert rows[0].status == "active"


def test_future_status_evidence_is_rejected() -> None:
    with pytest.raises(ValueError, match="after the current snapshot"):
        build_listing_lifecycles(
            [{"listing_key": "X::A", "ticker": "A", "exchange": "X"}],
            [{
                "listing_key": "X::A", "status": "delisted",
                "effective_at": "2026-08-18T00:00:00Z", "evidence_status": "official",
                "status_source": "delisting_apply",
                "source_report": "data/reports/delisting_apply.json",
            }],
            observed_at="2026-08-17T00:00:00Z",
        )


def test_naive_snapshot_timestamp_is_rejected() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        build_listing_lifecycles([], [], observed_at="2026-08-17T00:00:00")

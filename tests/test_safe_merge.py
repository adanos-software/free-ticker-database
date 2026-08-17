from __future__ import annotations

from scripts.check_safe_merge import evaluate
from scripts.lib.merge_evidence import row_fingerprint


def row(exchange: str, ticker: str, **overrides: str) -> dict[str, str]:
    value = {
        "listing_key": f"{exchange}::{ticker}", "exchange": exchange, "ticker": ticker,
        "name": f"Company {ticker}", "asset_type": "Stock", "country": "United States",
        "country_code": "US", "isin": "", "stock_sector": "", "etf_category": "",
    }
    value.update(overrides)
    return value


def removal_event(value: dict[str, str]) -> dict[str, str]:
    return {
        "listing_key": value["listing_key"], "exchange": value["exchange"], "ticker": value["ticker"],
        "event_type": "delisted", "before_row_sha256": row_fingerprint(value),
        "observed_at": "2026-08-17T00:00:00Z", "source_key": "official_delisting_notice",
        "source_url": "https://exchange.example/notices/A",
        "observation_id": f"obs-{value['exchange']}-{value['ticker']}",
        "evidence_status": "official",
    }


def test_one_unevidenced_removal_is_blocked() -> None:
    before = [row("NASDAQ", str(index)) for index in range(1000)]
    report = evaluate(before, before[:-1], [])
    assert report["status"] == "fail"
    assert report["summary"]["unevidenced_removed_rows"] == 1


def test_exact_current_row_event_allows_removal() -> None:
    before = [row("NASDAQ", str(index)) for index in range(1000)]
    report = evaluate(before, before[:-1], [removal_event(before[-1])])
    assert report["status"] == "pass"


def test_stale_event_cannot_authorize_removal() -> None:
    before = [row("NASDAQ", "A", name="Current")]
    report = evaluate(before, [], [removal_event(row("NASDAQ", "A", name="Old"))])
    assert report["status"] == "fail"


def test_critical_change_requires_exact_old_new_and_fingerprint() -> None:
    before = [row("NASDAQ", "A", isin="US0378331005")]
    after = [row("NASDAQ", "A", isin="")]
    event = {
        "listing_key": "NASDAQ::A", "event_type": "identifier_removed", "field_name": "isin",
        "old_value": "US0378331005", "new_value": "", "before_row_sha256": row_fingerprint(before[0]),
        "observed_at": "2026-08-17T00:00:00Z", "source_key": "identifier_quarantine",
        "source_report": "data/reports/identifier_quarantine.csv", "observation_id": "conflict-1",
        "evidence_status": "reviewed",
    }
    assert evaluate(before, after, [event])["status"] == "pass"
    event["before_row_sha256"] = "0" * 64
    assert evaluate(before, after, [event])["status"] == "fail"


def test_snapshot_absence_is_not_removal_evidence() -> None:
    before = [row("NASDAQ", "A")]
    event = removal_event(before[0])
    event["event_type"] = "not_observed"
    assert evaluate(before, [], [event])["status"] == "fail"


def test_invalid_timestamp_or_provenance_is_rejected() -> None:
    before = [row("NASDAQ", "A")]
    event = removal_event(before[0])
    event["observed_at"] = "2026-08-17T00:00:00"
    assert evaluate(before, [], [event])["status"] == "fail"
    event = removal_event(before[0])
    event["source_url"] = ""
    assert evaluate(before, [], [event])["status"] == "fail"


def test_duplicate_listing_keys_fail() -> None:
    duplicate = [row("X", "A"), row("X", "A")]
    assert evaluate(duplicate, duplicate, [])["status"] == "fail"


def test_large_evidenced_shrink_requires_explicit_override() -> None:
    before = [row("X", str(index)) for index in range(100)]
    after = before[:90]
    events = [removal_event(value) for value in before[90:]]
    assert evaluate(before, after, events)["status"] == "fail"
    assert evaluate(before, after, events, allow_large_evidenced_removal=True)["status"] == "pass"


def test_future_event_cannot_authorize_destructive_change() -> None:
    before = [row("NASDAQ", "A")]
    event = removal_event(before[0])
    event["observed_at"] = "2999-01-01T00:00:00Z"
    assert evaluate(before, [], [event])["status"] == "fail"

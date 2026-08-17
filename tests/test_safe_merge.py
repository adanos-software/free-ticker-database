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
        "source_url": "https://exchange.example/notices/A", "evidence_status": "official",
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


def official_reference(
    exchange: str,
    ticker: str,
    *,
    name: str,
    isin: str,
    asset_type: str = "Stock",
    official: str = "true",
    listing_status: str = "active",
) -> dict[str, str]:
    return {
        "source_key": "official_exchange_directory",
        "provider": "Example Exchange",
        "source_url": "https://exchange.example/securities.xlsx",
        "ticker": ticker,
        "name": name,
        "exchange": exchange,
        "asset_type": asset_type,
        "listing_status": listing_status,
        "reference_scope": "exchange_directory",
        "official": official,
        "isin": isin,
    }


def test_exact_official_reference_allows_blank_isin_and_derived_country_change() -> None:
    before = [
        row("HKEX", "01124", name="COASTAL GL", country="Hong Kong", country_code="HK"),
        row("NYSE", "BMBASE", name="Bermuda Baseline", country="Bermuda", country_code="BM"),
    ]
    after = [
        row(
            "HKEX", "01124", name="COASTAL GL", country="Bermuda",
            country_code="BM", isin="BMG2239B1643",
        ),
        before[1],
    ]
    report = evaluate(
        before,
        after,
        [],
        reference_rows=[
            official_reference(
                "HKEX", "01124", name="COASTAL GL", isin="BMG2239B1643"
            )
        ],
        observed_at="2026-08-17T00:00:00Z",
        reference_source_report="data/masterfiles/reference.csv",
    )
    assert report["status"] == "pass"
    assert report["summary"]["generated_official_change_evidence_rows"] == 3
    assert {event["field_name"] for event in report["generated_official_change_evidence"]} == {
        "isin", "country", "country_code",
    }


def test_official_reference_cannot_replace_existing_isin() -> None:
    before = [row("X", "A", name="Alpha", isin="US0378331005")]
    after = [row("X", "A", name="Alpha", isin="US5949181045")]
    report = evaluate(
        before,
        after,
        [],
        reference_rows=[official_reference("X", "A", name="Alpha", isin="US5949181045")],
        observed_at="2026-08-17T00:00:00Z",
        reference_source_report="data/masterfiles/reference.csv",
    )
    assert report["status"] == "fail"
    assert report["summary"]["generated_official_change_evidence_rows"] == 0


def test_non_exact_or_untrusted_reference_cannot_authorize_change() -> None:
    before = [row("X", "A", name="Alpha")]
    after = [row("X", "A", name="Alpha", isin="US0378331005")]
    cases = [
        official_reference("Y", "A", name="Alpha", isin="US0378331005"),
        official_reference("X", "A", name="Different", isin="US0378331005"),
        official_reference("X", "A", name="Alpha", isin="US0378331005", official="false"),
        official_reference(
            "X", "A", name="Alpha", isin="US0378331005", listing_status="delisted"
        ),
    ]
    for reference in cases:
        report = evaluate(
            before,
            after,
            [],
            reference_rows=[reference],
            observed_at="2026-08-17T00:00:00Z",
            reference_source_report="data/masterfiles/reference.csv",
        )
        assert report["status"] == "fail"
        assert report["summary"]["generated_official_change_evidence_rows"] == 0


def test_conflicting_exact_official_isins_fail_closed() -> None:
    before = [row("X", "A", name="Alpha")]
    after = [row("X", "A", name="Alpha", isin="US0378331005")]
    report = evaluate(
        before,
        after,
        [],
        reference_rows=[
            official_reference("X", "A", name="Alpha", isin="US0378331005"),
            official_reference("X", "A", name="Alpha", isin="US5949181045"),
        ],
        observed_at="2026-08-17T00:00:00Z",
        reference_source_report="data/masterfiles/reference.csv",
    )
    assert report["status"] == "fail"
    assert report["summary"]["generated_official_change_evidence_rows"] == 0

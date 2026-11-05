from __future__ import annotations

import csv
from pathlib import Path

from scripts.check_safe_merge import evaluate
from scripts.validate_database import invalid_out_of_scope_transitions

ROOT = Path(__file__).resolve().parents[1]
TRANSITIONS_CSV = ROOT / "data" / "review_overrides" / "listing_transitions.csv"
DROP_ENTRIES_CSV = ROOT / "data" / "review_overrides" / "drop_entries.csv"
LISTINGS_CSV = ROOT / "data" / "listings.csv"
SOURCE_REPORT = "data/review_overrides/listing_transitions.csv"


def listing(exchange: str, ticker: str, **overrides: str) -> dict[str, str]:
    value = {
        "listing_key": f"{exchange}::{ticker}", "exchange": exchange, "ticker": ticker,
        "name": f"Fund {ticker}", "asset_type": "ETF", "country": "United States",
        "country_code": "US", "isin": "", "stock_sector": "", "etf_category": "Equity",
    }
    value.update(overrides)
    return value


def out_of_scope(old_key: str, identity_type: str, identity_value: str, **overrides: str) -> dict[str, str]:
    value = {
        "old_listing_key": old_key, "new_listing_key": "", "event_type": "out_of_scope",
        "identity_type": identity_type, "identity_value": identity_value, "confidence": "0.98",
        "source_key": "sponsor_fund_page", "source_url": "https://sponsor.example/cef",
        "reason": "Closed-end fund, not an exchange-traded product.",
    }
    value.update(overrides)
    return value


def run(before: list[dict[str, str]], transition: dict[str, str]) -> dict:
    return evaluate(
        before, [], [],
        reviewed_transition_rows=[transition],
        reviewed_transition_source_report=SOURCE_REPORT,
        observed_at="2026-11-05T08:00:00Z",
        total_shrink_limit=1.0, venue_shrink_limit=1.0,
    )


def test_out_of_scope_removes_etf_row_by_exact_isin_or_exact_listing() -> None:
    with_isin = listing("NYSE", "CEF", isin="US92535C1045")
    report = run([with_isin], out_of_scope("NYSE::CEF", "exact_isin", "US92535C1045"))
    assert report["status"] == "pass"
    assert report["generated_reviewed_transition_evidence"][0]["event_type"] == "out_of_scope"

    without_isin = listing("NYSE", "CFND")
    assert run([without_isin], out_of_scope("NYSE::CFND", "exact_listing", "NYSE::CFND"))["status"] == "pass"


def test_out_of_scope_fails_closed_on_malformed_or_non_etf_rows() -> None:
    etf = listing("NYSE", "CEF", isin="US92535C1045")
    good = out_of_scope("NYSE::CEF", "exact_isin", "US92535C1045")
    rejected = [
        ([listing("NYSE", "CEF", isin="US92535C1045", asset_type="Stock")], good),
        ([etf], good | {"reason": ""}),
        ([etf], good | {"source_url": "http://sponsor.example/cef"}),
        ([etf], good | {"new_listing_key": "NYSE::NEW"}),
        ([etf], good | {"confidence": "0.9"}),
        ([etf], good | {"identity_value": "US19249X1081"}),
        ([etf], out_of_scope("NYSE::CEF", "exact_listing", "NYSE::OTHER")),
        ([etf], out_of_scope("NYSE::CEF", "same_cik", "0000000001")),
    ]
    for before, transition in rejected:
        report = run(before, transition)
        assert report["status"] == "fail"
        assert report["summary"]["generated_reviewed_transition_evidence_rows"] == 0


def test_validation_flags_malformed_or_undropped_out_of_scope_rows() -> None:
    good = out_of_scope("NYSE::CEF", "exact_listing", "NYSE::CEF")
    assert invalid_out_of_scope_transitions([good], []) == []
    assert invalid_out_of_scope_transitions([good | {"reason": ""}], []) == ["row 2: NYSE::CEF reason"]
    assert invalid_out_of_scope_transitions([good], [listing("NYSE", "CEF")]) == ["row 2: NYSE::CEF still_listed"]
    delisted = good | {"event_type": "delisted", "reason": ""}
    assert invalid_out_of_scope_transitions([delisted], []) == []


def test_committed_out_of_scope_rows_are_dropped() -> None:
    with TRANSITIONS_CSV.open(newline="", encoding="utf-8") as handle:
        rows = [row for row in csv.DictReader(handle) if row["event_type"] == "out_of_scope"]
    with DROP_ENTRIES_CSV.open(newline="", encoding="utf-8") as handle:
        drops = {f"{row['exchange']}::{row['ticker']}" for row in csv.DictReader(handle)}
    with LISTINGS_CSV.open(newline="", encoding="utf-8") as handle:
        listed = {row["listing_key"] for row in csv.DictReader(handle)}
    assert invalid_out_of_scope_transitions(rows, []) == []
    for row in rows:
        assert row["old_listing_key"] in drops
        assert row["old_listing_key"] not in listed

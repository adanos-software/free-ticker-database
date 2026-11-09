from __future__ import annotations

from pathlib import Path

from scripts.apply_nasdaq_us_new_listings import reviewed_transition_identity_peers
from scripts.check_safe_merge import evaluate
from scripts.lib.dataio import load_csv


ROOT = Path(__file__).resolve().parents[1]
HLT_ISIN = "US43300A2033"


def _transition(old_listing_key: str, new_listing_key: str) -> dict[str, str]:
    rows = [
        row
        for row in load_csv(ROOT / "data/review_overrides/listing_transitions.csv")
        if row.get("old_listing_key") == old_listing_key and row.get("new_listing_key") == new_listing_key
    ]
    assert len(rows) == 1
    return rows[0]


def test_reviewed_hlt_nyse_to_nasdaq_transition_is_same_isin() -> None:
    row = _transition("NYSE::HLT", "NASDAQ::HLT")
    assert row["event_type"] == "venue_changed"
    assert row["identity_type"] == "same_isin"
    assert row["identity_value"] == HLT_ISIN
    assert row["source_url"] == (
        "https://ir.hilton.com/sec-filings/all-sec-filings/content/0001585689-26-000045/hlt-20261020.htm"
    )
    assert "DTN2026-24" in row["reason"]
    assert "43300A203" in row["reason"]


def test_hlt_predecessor_drop_is_recorded() -> None:
    drops = load_csv(ROOT / "data/review_overrides/drop_entries.csv")
    hlt = [row for row in drops if row.get("ticker") == "HLT" and row.get("exchange") == "NYSE"]
    assert len(hlt) == 1
    assert "NASDAQ::HLT" in hlt[0]["reason"]
    assert "listing_transitions.csv" in hlt[0]["reason"]


def test_hlt_nasdaq_isin_metadata_keeps_transfer_identity() -> None:
    updates = [
        row
        for row in load_csv(ROOT / "data/review_overrides/metadata_updates.csv")
        if row.get("ticker") == "HLT" and row.get("exchange") == "NASDAQ" and row.get("field") == "isin"
    ]
    assert len(updates) == 1
    assert updates[0]["decision"] == "update"
    assert updates[0]["proposed_value"] == HLT_ISIN
    assert "DTN2026-24" in updates[0]["reason"]
    assert "hlt-20261020.htm" in updates[0]["reason"]


def test_reviewed_transition_matches_hlt_predecessor() -> None:
    predecessor = {
        "listing_key": "NYSE::HLT",
        "ticker": "HLT",
        "exchange": "NYSE",
        "name": "Hilton Worldwide Holdings Inc",
        "asset_type": "Stock",
        "isin": HLT_ISIN,
    }
    new_row = {
        "ticker": "HLT",
        "exchange": "NASDAQ",
        "asset_type": "Stock",
        "name": "Hilton Worldwide Holdings Inc. - Common Stock",
    }
    transitions = load_csv(ROOT / "data/review_overrides/listing_transitions.csv")
    peers = reviewed_transition_identity_peers(new_row, [predecessor], transitions)
    assert len(peers) == 1
    assert peers[0]["listing_key"] == "NYSE::HLT"


def test_hlt_venue_transfer_removal_is_evidenced_and_aneb_is_not_a_removal() -> None:
    before = [
        {
            "listing_key": "NYSE::HLT",
            "ticker": "HLT",
            "exchange": "NYSE",
            "name": "Hilton Worldwide Holdings Inc",
            "asset_type": "Stock",
            "country": "United States",
            "country_code": "US",
            "isin": HLT_ISIN,
            "stock_sector": "Consumer Discretionary",
            "etf_category": "",
        },
        {
            "listing_key": "NASDAQ::HLT",
            "ticker": "HLT",
            "exchange": "NASDAQ",
            "name": "Hilton Worldwide Holdings Inc. - Common Stock",
            "asset_type": "Stock",
            "country": "United States",
            "country_code": "US",
            "isin": "",
            "stock_sector": "",
            "etf_category": "",
        },
        {
            "listing_key": "OTC::ANEB",
            "ticker": "ANEB",
            "exchange": "OTC",
            "name": "Anebulo Pharmaceuticals Inc",
            "asset_type": "Stock",
            "country": "United States",
            "country_code": "US",
            "isin": "US0345691036",
            "stock_sector": "Health Care",
            "etf_category": "",
        },
    ]
    after = [
        {
            **before[1],
            "isin": HLT_ISIN,
        },
        before[2],
    ]
    transitions = load_csv(ROOT / "data/review_overrides/listing_transitions.csv")
    report = evaluate(
        before,
        after,
        [],
        reviewed_transition_rows=transitions,
        reviewed_transition_source_report="data/review_overrides/listing_transitions.csv",
        observed_at="2026-11-09T16:10:00Z",
        allow_large_evidenced_removal=True,
        venue_shrink_limit=1.0,
    )

    assert report["status"] == "pass"
    assert report["unevidenced_removed_listing_keys"] == []
    assert report["evidenced_removed_listing_keys"] == ["NYSE::HLT"]
    assert "OTC::ANEB" not in report["unevidenced_removed_listing_keys"]

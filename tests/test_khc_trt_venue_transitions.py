from __future__ import annotations

from pathlib import Path

from scripts.apply_nasdaq_us_new_listings import reviewed_transition_identity_peers
from scripts.lib.dataio import load_csv


ROOT = Path(__file__).resolve().parents[1]
KHC_ISIN = "US5007541064"
TRT_ISIN = "US8967122057"


def _transition(old_listing_key: str, new_listing_key: str) -> dict[str, str]:
    rows = [
        row
        for row in load_csv(ROOT / "data/review_overrides/listing_transitions.csv")
        if row.get("old_listing_key") == old_listing_key and row.get("new_listing_key") == new_listing_key
    ]
    assert len(rows) == 1
    return rows[0]


def test_reviewed_khc_nasdaq_to_nyse_transition_is_same_isin() -> None:
    row = _transition("NASDAQ::KHC", "NYSE::KHC")
    assert row["event_type"] == "venue_changed"
    assert row["identity_type"] == "same_isin"
    assert row["identity_value"] == KHC_ISIN
    assert row["source_url"].startswith("https://www.sec.gov/")


def test_reviewed_trt_nyse_mkt_to_nasdaq_transition_is_same_isin() -> None:
    row = _transition("NYSE MKT::TRT", "NASDAQ::TRT")
    assert row["event_type"] == "venue_changed"
    assert row["identity_type"] == "same_isin"
    assert row["identity_value"] == TRT_ISIN
    assert row["source_url"].startswith("https://www.sec.gov/")


def test_khc_and_trt_predecessor_drops_are_recorded() -> None:
    drops = load_csv(ROOT / "data/review_overrides/drop_entries.csv")
    khc = [row for row in drops if row.get("ticker") == "KHC" and row.get("exchange") == "NASDAQ"]
    trt = [row for row in drops if row.get("ticker") == "TRT" and row.get("exchange") == "NYSE MKT"]
    assert len(khc) == 1
    assert "NYSE::KHC" in khc[0]["reason"]
    assert "listing_transitions.csv" in khc[0]["reason"]
    assert len(trt) == 1
    assert "NASDAQ::TRT" in trt[0]["reason"]
    assert "listing_transitions.csv" in trt[0]["reason"]


def test_reviewed_transition_matches_khc_predecessor() -> None:
    predecessor = {
        "listing_key": "NASDAQ::KHC",
        "ticker": "KHC",
        "exchange": "NASDAQ",
        "name": "Kraft Heinz Co",
        "asset_type": "Stock",
        "isin": KHC_ISIN,
    }
    new_row = {"ticker": "KHC", "exchange": "NYSE", "asset_type": "Stock", "name": "The Kraft Heinz Company Common Stock"}
    transitions = load_csv(ROOT / "data/review_overrides/listing_transitions.csv")
    peers = reviewed_transition_identity_peers(new_row, [predecessor], transitions)
    assert len(peers) == 1
    assert peers[0]["listing_key"] == "NASDAQ::KHC"


def test_reviewed_transition_matches_trt_predecessor() -> None:
    predecessor = {
        "listing_key": "NYSE MKT::TRT",
        "ticker": "TRT",
        "exchange": "NYSE MKT",
        "name": "Trio-Tech International",
        "asset_type": "Stock",
        "isin": TRT_ISIN,
    }
    new_row = {"ticker": "TRT", "exchange": "NASDAQ", "asset_type": "Stock", "name": "Trio-Tech International - Common Stock"}
    transitions = load_csv(ROOT / "data/review_overrides/listing_transitions.csv")
    peers = reviewed_transition_identity_peers(new_row, [predecessor], transitions)
    assert len(peers) == 1
    assert peers[0]["listing_key"] == "NYSE MKT::TRT"

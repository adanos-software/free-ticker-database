from __future__ import annotations

from pathlib import Path

from scripts.apply_nasdaq_us_new_listings import reviewed_transition_identity_peers
from scripts.lib.dataio import load_csv


ROOT = Path(__file__).resolve().parents[1]
LUMN_ISIN = "US5502411037"
NXDR_ISIN = "US65345M1080"


def _transition(old_listing_key: str, new_listing_key: str) -> dict[str, str]:
    rows = [
        row
        for row in load_csv(ROOT / "data/review_overrides/listing_transitions.csv")
        if row.get("old_listing_key") == old_listing_key and row.get("new_listing_key") == new_listing_key
    ]
    assert len(rows) == 1
    return rows[0]


def test_reviewed_lumn_nyse_to_nasdaq_transition_is_same_isin() -> None:
    row = _transition("NYSE::LUMN", "NASDAQ::LUMN")
    assert row["event_type"] == "venue_changed"
    assert row["identity_type"] == "same_isin"
    assert row["identity_value"] == LUMN_ISIN
    assert row["source_url"].startswith("https://www.sec.gov/")


def test_reviewed_nxdr_nyse_to_nasdaq_transition_is_same_isin() -> None:
    row = _transition("NYSE::NXDR", "NASDAQ::NXDR")
    assert row["event_type"] == "venue_changed"
    assert row["identity_type"] == "same_isin"
    assert row["identity_value"] == NXDR_ISIN
    assert row["source_url"].startswith("https://www.sec.gov/")


def test_lumn_and_nxdr_predecessor_drops_are_recorded() -> None:
    drops = load_csv(ROOT / "data/review_overrides/drop_entries.csv")
    lumn = [row for row in drops if row.get("ticker") == "LUMN" and row.get("exchange") == "NYSE"]
    nxdr = [row for row in drops if row.get("ticker") == "NXDR" and row.get("exchange") == "NYSE"]
    assert len(lumn) == 1
    assert "NASDAQ::LUMN" in lumn[0]["reason"]
    assert "listing_transitions.csv" in lumn[0]["reason"]
    assert len(nxdr) == 1
    assert "NASDAQ::NXDR" in nxdr[0]["reason"]
    assert "listing_transitions.csv" in nxdr[0]["reason"]


def test_reviewed_transition_matches_lumn_predecessor() -> None:
    predecessor = {
        "listing_key": "NYSE::LUMN",
        "ticker": "LUMN",
        "exchange": "NYSE",
        "name": "Lumen Technologies Inc",
        "asset_type": "Stock",
        "isin": LUMN_ISIN,
    }
    new_row = {
        "ticker": "LUMN",
        "exchange": "NASDAQ",
        "asset_type": "Stock",
        "name": "Lumen Technologies, Inc.  - Common Stock",
    }
    transitions = load_csv(ROOT / "data/review_overrides/listing_transitions.csv")
    peers = reviewed_transition_identity_peers(new_row, [predecessor], transitions)
    assert len(peers) == 1
    assert peers[0]["listing_key"] == "NYSE::LUMN"


def test_reviewed_transition_matches_nxdr_predecessor() -> None:
    predecessor = {
        "listing_key": "NYSE::NXDR",
        "ticker": "NXDR",
        "exchange": "NYSE",
        "name": "Nextdoor Holdings, Inc. Class A",
        "asset_type": "Stock",
        "isin": NXDR_ISIN,
    }
    new_row = {
        "ticker": "NXDR",
        "exchange": "NASDAQ",
        "asset_type": "Stock",
        "name": "Nextdoor Holdings, Inc. - Common Stock",
    }
    transitions = load_csv(ROOT / "data/review_overrides/listing_transitions.csv")
    peers = reviewed_transition_identity_peers(new_row, [predecessor], transitions)
    assert len(peers) == 1
    assert peers[0]["listing_key"] == "NYSE::NXDR"

from __future__ import annotations

from pathlib import Path

from scripts.apply_nasdaq_us_new_listings import reviewed_transition_identity_peers
from scripts.lib.dataio import load_csv, is_well_formed_metadata_update


ROOT = Path(__file__).resolve().parents[1]
ANY_ISIN = "CA84841L4073"


def test_reviewed_any_to_drk_transition_is_same_isin() -> None:
    rows = [
        row
        for row in load_csv(ROOT / "data/review_overrides/listing_transitions.csv")
        if row.get("old_listing_key") == "NASDAQ::ANY" and row.get("new_listing_key") == "NASDAQ::DRK"
    ]
    assert len(rows) == 1
    row = rows[0]
    assert row["event_type"] == "symbol_changed"
    assert row["identity_type"] == "same_isin"
    assert row["identity_value"] == ANY_ISIN
    assert row["source_url"].startswith("https://")


def test_any_predecessor_drop_is_recorded() -> None:
    rows = [
        row
        for row in load_csv(ROOT / "data/review_overrides/drop_entries.csv")
        if row.get("ticker") == "ANY" and row.get("exchange") == "NASDAQ"
    ]
    assert len(rows) == 1
    assert "NASDAQ::DRK" in rows[0]["reason"]
    assert "listing_transitions.csv" in rows[0]["reason"]


def test_drk_keeps_reviewed_canadian_isin() -> None:
    reviewed = {
        (row["ticker"], row["exchange"], row["isin"])
        for row in load_csv(ROOT / "data/review_overrides/foreign_isin_reviewed.csv")
    }
    assert ("DRK", "NASDAQ", ANY_ISIN) in reviewed
    assert ("ANY", "NASDAQ", ANY_ISIN) in reviewed


def test_drk_metadata_updates_are_well_formed() -> None:
    updates = [
        row
        for row in load_csv(ROOT / "data/review_overrides/metadata_updates.csv")
        if row.get("ticker") == "DRK" and row.get("exchange") == "NASDAQ"
    ]
    by_field = {row["field"]: row for row in updates}
    assert set(by_field) >= {"name", "aliases"}
    for row in updates:
        assert is_well_formed_metadata_update(row)
        assert row["confidence"] == "0.99"
    assert "DarkHorse" in by_field["name"]["proposed_value"]
    assert "sphere 3d" in by_field["aliases"]["proposed_value"]


def test_reviewed_transition_matches_any_predecessor() -> None:
    predecessor = {
        "listing_key": "NASDAQ::ANY",
        "ticker": "ANY",
        "exchange": "NASDAQ",
        "name": "Sphere 3D Corp",
        "asset_type": "Stock",
        "isin": ANY_ISIN,
    }
    new_row = {"ticker": "DRK", "exchange": "NASDAQ", "asset_type": "Stock", "name": "DarkHorse Technologies Inc. - Common Shares"}
    transitions = load_csv(ROOT / "data/review_overrides/listing_transitions.csv")
    peers = reviewed_transition_identity_peers(new_row, [predecessor], transitions)
    assert len(peers) == 1
    assert peers[0]["listing_key"] == "NASDAQ::ANY"

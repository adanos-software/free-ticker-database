from __future__ import annotations

from pathlib import Path

from scripts.apply_nasdaq_us_new_listings import reviewed_transition_identity_peers
from scripts.lib.dataio import load_csv, is_well_formed_metadata_update


ROOT = Path(__file__).resolve().parents[1]
KWM_ISIN = "KYG531511098"
ZONE_ISIN = "US1844921064"
PHGE_ISIN = "US09090D5095"
HLSQ_ISIN = "US09090D6085"
MBAI_ISIN = "IL0011336851"


def _transition(old_listing_key: str, new_listing_key: str) -> dict[str, str]:
    rows = [
        row
        for row in load_csv(ROOT / "data/review_overrides/listing_transitions.csv")
        if row.get("old_listing_key") == old_listing_key and row.get("new_listing_key") == new_listing_key
    ]
    assert len(rows) == 1
    return rows[0]


def test_kwm_to_nxat_is_same_isin_symbol_change() -> None:
    row = _transition("NASDAQ::KWM", "NASDAQ::NXAT")
    assert row["event_type"] == "symbol_changed"
    assert row["identity_type"] == "same_isin"
    assert row["identity_value"] == KWM_ISIN
    assert row["source_url"].startswith("https://www.sec.gov/")


def test_phge_predecessor_is_exact_isin_delist() -> None:
    row = _transition("NYSE::PHGE", "")
    assert row["event_type"] == "delisted"
    assert row["identity_type"] == "exact_isin"
    assert row["identity_value"] == PHGE_ISIN
    assert "HLSQ" in row["reason"]


def test_zone_nyse_to_nyse_mkt_is_same_isin_venue_change() -> None:
    row = _transition("NYSE::ZONE", "NYSE MKT::ZONE")
    assert row["event_type"] == "venue_changed"
    assert row["identity_type"] == "same_isin"
    assert row["identity_value"] == ZONE_ISIN


def test_mbai_name_update_is_well_formed() -> None:
    updates = [
        row
        for row in load_csv(ROOT / "data/review_overrides/metadata_updates.csv")
        if row.get("ticker") == "MBAI" and row.get("exchange") == "NASDAQ" and row.get("field") == "name"
    ]
    assert len(updates) == 1
    assert is_well_formed_metadata_update(updates[0])
    assert "MBody AI" in updates[0]["proposed_value"]


def test_hlsq_isin_is_post_split_cusip() -> None:
    updates = [
        row
        for row in load_csv(ROOT / "data/review_overrides/metadata_updates.csv")
        if row.get("ticker") == "HLSQ" and row.get("exchange") == "NYSE MKT" and row.get("field") == "isin"
    ]
    assert len(updates) == 1
    assert updates[0]["proposed_value"] == HLSQ_ISIN


def test_reviewed_transition_matches_kwm_predecessor() -> None:
    predecessor = {
        "listing_key": "NASDAQ::KWM",
        "ticker": "KWM",
        "exchange": "NASDAQ",
        "name": "K Wave Media Ltd.",
        "asset_type": "Stock",
        "isin": KWM_ISIN,
    }
    new_row = {
        "ticker": "NXAT",
        "exchange": "NASDAQ",
        "asset_type": "Stock",
        "name": "Nexus Advanced Technologies Inc. - Ordinary Shares",
    }
    transitions = load_csv(ROOT / "data/review_overrides/listing_transitions.csv")
    peers = reviewed_transition_identity_peers(new_row, [predecessor], transitions)
    assert len(peers) == 1
    assert peers[0]["listing_key"] == "NASDAQ::KWM"

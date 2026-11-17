from __future__ import annotations

from pathlib import Path

from scripts.apply_nasdaq_us_new_listings import reviewed_transition_identity_peers
from scripts.lib.dataio import load_csv


ROOT = Path(__file__).resolve().parents[1]
GM_ISIN = "US37045V1008"
SEC_8A12B = (
    "https://www.sec.gov/Archives/edgar/data/1467858/000119312526463457/d159065d8a12b.htm"
)
DTN_URL = "https://www.nasdaqtrader.com/TraderNews.aspx?id=DTN2026-27"
IR_URL = (
    "https://investor.gm.com/news-releases/news-release-details/"
    "gm-announces-transfer-stock-exchange-listing-nasdaq/"
)


def _transition(old_listing_key: str, new_listing_key: str) -> dict[str, str]:
    rows = [
        row
        for row in load_csv(ROOT / "data/review_overrides/listing_transitions.csv")
        if row.get("old_listing_key") == old_listing_key and row.get("new_listing_key") == new_listing_key
    ]
    assert len(rows) == 1
    return rows[0]


def test_reviewed_gm_nyse_to_nasdaq_transition_is_same_isin() -> None:
    row = _transition("NYSE::GM", "NASDAQ::GM")
    assert row["event_type"] == "venue_changed"
    assert row["identity_type"] == "same_isin"
    assert row["identity_value"] == GM_ISIN
    assert row["source_key"] == "sec_8a12b_listing_transfer"
    assert row["source_url"] == SEC_8A12B
    assert "DTN2026-27" in row["reason"]
    assert DTN_URL in row["reason"]
    assert IR_URL in row["reason"]
    assert "37045V100" in row["reason"]
    assert "2026-11-16" in row["reason"]
    assert "2026-11-17" in row["reason"]
    assert "GM35B" in row["reason"]
    assert "37045XFK7" in row["reason"]


def test_gm_predecessor_drop_is_recorded() -> None:
    drops = load_csv(ROOT / "data/review_overrides/drop_entries.csv")
    gm = [row for row in drops if row.get("ticker") == "GM" and row.get("exchange") == "NYSE"]
    assert len(gm) == 1
    assert "NASDAQ::GM" in gm[0]["reason"]
    assert "listing_transitions.csv" in gm[0]["reason"]


def test_gm_nasdaq_metadata_keeps_transfer_identity() -> None:
    updates = [
        row
        for row in load_csv(ROOT / "data/review_overrides/metadata_updates.csv")
        if row.get("ticker") == "GM" and row.get("exchange") == "NASDAQ"
    ]
    by_field = {row["field"]: row for row in updates}
    assert set(by_field) == {"isin", "stock_sector"}
    assert by_field["isin"]["decision"] == "update"
    assert by_field["isin"]["proposed_value"] == GM_ISIN
    assert "DTN2026-27" in by_field["isin"]["reason"]
    assert SEC_8A12B in by_field["isin"]["reason"]
    assert IR_URL in by_field["isin"]["reason"]
    assert by_field["stock_sector"]["decision"] == "update"
    assert by_field["stock_sector"]["proposed_value"] == "Consumer Discretionary"


def test_reviewed_transition_matches_gm_predecessor() -> None:
    predecessor = {
        "listing_key": "NYSE::GM",
        "ticker": "GM",
        "exchange": "NYSE",
        "name": "General Motors Company",
        "asset_type": "Stock",
        "isin": GM_ISIN,
    }
    new_row = {
        "ticker": "GM",
        "exchange": "NASDAQ",
        "asset_type": "Stock",
        "name": "General Motors Company - Common Stock",
    }
    transitions = load_csv(ROOT / "data/review_overrides/listing_transitions.csv")
    peers = reviewed_transition_identity_peers(new_row, [predecessor], transitions)
    assert len(peers) == 1
    assert peers[0]["listing_key"] == "NYSE::GM"


def test_gm_financial_note_transfer_is_not_an_equity_listing() -> None:
    listings = load_csv(ROOT / "data/listings.csv")
    tickers = {row.get("ticker") for row in listings}
    assert "GM35B" not in tickers
    assert "GM/35B" not in tickers
    names = " ".join(row.get("name", "") for row in listings if "General Motors Financial" in row.get("name", ""))
    assert names == ""


def test_gm_primary_listing_is_the_nasdaq_venue_move() -> None:
    listings = load_csv(ROOT / "data/listings.csv")
    gm_rows = [row for row in listings if row.get("ticker") == "GM"]
    by_key = {row["listing_key"]: row for row in gm_rows}
    assert "NYSE::GM" not in by_key
    nasdaq = by_key["NASDAQ::GM"]
    assert nasdaq["isin"] == GM_ISIN
    assert nasdaq["stock_sector"] == "Consumer Discretionary"
    assert "general motors" in nasdaq["aliases"].split("|")
    peers = {
        row["listing_key"]: row
        for row in listings
        if row["listing_key"] in {"LSE::0R0E", "FSX::8GM"}
    }
    assert peers["LSE::0R0E"]["isin"] == GM_ISIN
    assert "general motors" in peers["LSE::0R0E"]["aliases"].split("|")
    assert peers["FSX::8GM"]["isin"] == GM_ISIN
    assert "general motors" in peers["FSX::8GM"]["aliases"].split("|")
    coverage = load_csv(ROOT / "data/coverage_expansion_listings.csv")
    assert not any(row.get("listing_key") == "NASDAQ::GM" for row in coverage)
    tickers = load_csv(ROOT / "data/tickers.csv")
    primary = next(row for row in tickers if row.get("ticker") == "GM")
    assert primary["exchange"] == "NASDAQ"
    assert primary["isin"] == GM_ISIN
    assert primary["stock_sector"] == "Consumer Discretionary"
    assert "general motors" in primary["aliases"].split("|")

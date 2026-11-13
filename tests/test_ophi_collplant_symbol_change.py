"""CollPlant Biotechnologies Ltd. changed its Nasdaq symbol from CLGN to OPHI.

Effective 2026-11-13 the ordinary shares kept ISIN IL0004960188 and CUSIP
M2R51X124. The proposed OptiPhi Computing Ltd. name is subject to a later
shareholder vote, so the listing stays CollPlant.
"""

from __future__ import annotations

from pathlib import Path

from scripts.lib.dataio import load_csv


ROOT = Path(__file__).resolve().parents[1]
ISIN = "IL0004960188"


def test_clgn_to_ophi_is_a_same_isin_symbol_change() -> None:
    rows = [
        row
        for row in load_csv(ROOT / "data/review_overrides/listing_transitions.csv")
        if row.get("old_listing_key") == "NASDAQ::CLGN"
    ]
    assert len(rows) == 1
    row = rows[0]
    assert row["new_listing_key"] == "NASDAQ::OPHI"
    assert row["event_type"] == "symbol_changed"
    assert row["identity_type"] == "same_isin"
    assert row["identity_value"] == ISIN
    assert row["source_url"] == "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt"


def test_clgn_predecessor_is_dropped_and_ophi_keeps_the_reviewed_isin() -> None:
    drops = [
        row
        for row in load_csv(ROOT / "data/review_overrides/drop_entries.csv")
        if row.get("ticker") == "CLGN" and row.get("exchange") == "NASDAQ"
    ]
    assert len(drops) == 1
    assert "NASDAQ::OPHI" in drops[0]["reason"]

    reviewed = {
        (row["ticker"], row["exchange"], row["isin"])
        for row in load_csv(ROOT / "data/review_overrides/foreign_isin_reviewed.csv")
    }
    assert ("OPHI", "NASDAQ", ISIN) in reviewed
    assert ("CLGN", "NASDAQ", ISIN) not in reviewed

    tickers = [
        row
        for row in load_csv(ROOT / "data/tickers.csv")
        if row.get("exchange") == "NASDAQ" and row.get("ticker") in {"CLGN", "OPHI"}
    ]
    assert [row["ticker"] for row in tickers] == ["OPHI"]
    assert tickers[0]["isin"] == ISIN
    assert tickers[0]["country_code"] == "IL"
    assert "CollPlant" in tickers[0]["name"]
    assert "OptiPhi" not in tickers[0]["name"]

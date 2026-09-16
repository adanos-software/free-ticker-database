from __future__ import annotations

from pathlib import Path

from scripts.build_listing_history import load_change_evidence
from scripts.lib.dataio import load_csv, is_well_formed_metadata_update
from scripts.lib.review_adjudications import keep_listing_keys_by_isin, validate_rows
from scripts.rebuild_dataset import apply_input_metadata_overrides, load_review_overrides


ROOT = Path(__file__).resolve().parents[1]
SPCX_ISIN = "US84615Q1031"
ETF_ISIN = "US19423L6728"


def _spacex_metadata_updates() -> list[dict[str, str]]:
    return [
        row
        for row in load_csv(ROOT / "data/review_overrides/metadata_updates.csv")
        if row.get("ticker") == "SPCX" and row.get("exchange") == "NASDAQ"
    ]


def test_official_nasdaq_directory_lists_spacex_on_spcx() -> None:
    rows = [
        row
        for row in load_csv(ROOT / "data/masterfiles/reference.csv")
        if row.get("source_key") == "nasdaq_listed" and row.get("ticker") == "SPCX"
    ]
    assert len(rows) == 1
    assert rows[0]["asset_type"] == "Stock"
    assert "SPACE EXPLORATION" in rows[0]["name"].upper()
    assert rows[0]["official"] == "true"


def test_predecessor_etf_remains_on_spck() -> None:
    rows = [
        row
        for row in load_csv(ROOT / "data/listings.csv")
        if row.get("listing_key") == "NASDAQ::SPCK"
    ]
    assert len(rows) == 1
    assert rows[0]["asset_type"] == "ETF"
    assert rows[0]["isin"] == ETF_ISIN


def test_reviewed_spacex_metadata_updates_are_well_formed() -> None:
    updates = _spacex_metadata_updates()
    by_field = {row["field"]: row for row in updates}
    assert set(by_field) >= {"name", "asset_type", "isin", "etf_category", "aliases"}
    for row in updates:
        assert is_well_formed_metadata_update(row)
        assert row["confidence"] == "0.99"
        assert "US84615Q1031" in row["reason"] or row["field"] != "isin"
    assert by_field["asset_type"]["proposed_value"] == "Stock"
    assert by_field["isin"]["proposed_value"] == SPCX_ISIN
    assert by_field["etf_category"]["decision"] == "clear"
    assert "Class A" in by_field["name"]["proposed_value"]


def test_reviewed_metadata_recode_turns_stale_etf_row_into_spacex_stock() -> None:
    _, metadata_updates, _ = load_review_overrides()
    stale = {
        "ticker": "SPCX",
        "exchange": "NASDAQ",
        "name": "SPAC and New Issue ETF",
        "asset_type": "ETF",
        "isin": ETF_ISIN,
        "etf_category": "Equity",
        "aliases": "spac and new issue",
    }
    recoded = apply_input_metadata_overrides(stale, metadata_updates[("SPCX", "NASDAQ")])
    assert recoded["asset_type"] == "Stock"
    assert recoded["isin"] == SPCX_ISIN
    assert recoded["etf_category"] == ""
    assert "Space Exploration Technologies" in recoded["name"]


def test_listing_history_treats_spacex_metadata_as_reviewed_evidence() -> None:
    evidence = load_change_evidence()
    assert ("NASDAQ::SPCX", "isin", "*", SPCX_ISIN) in evidence
    assert evidence[("NASDAQ::SPCX", "isin", "*", SPCX_ISIN)]["evidence_status"] == "reviewed"
    assert ("NASDAQ::SPCX", "asset_type", "*", "Stock") in evidence
    assert ("NASDAQ::SPCX", "etf_category", "*", "") in evidence


def test_spacex_isin_adjudication_keeps_nasdaq_listing() -> None:
    path = ROOT / "data/review_overrides/identifier_adjudications.csv"
    rows = validate_rows(load_csv(path))
    assert keep_listing_keys_by_isin(path)[SPCX_ISIN] == {"NASDAQ::SPCX"}
    assert rows[0]["evidence_url"].startswith("https://")


def test_rebuilt_nasdaq_spcx_is_spacex_stock() -> None:
    rows = [
        row
        for row in load_csv(ROOT / "data/listings.csv")
        if row.get("listing_key") == "NASDAQ::SPCX"
    ]
    assert len(rows) == 1
    assert rows[0]["asset_type"] == "Stock"
    assert rows[0]["isin"] == SPCX_ISIN
    assert "SPACE EXPLORATION" in rows[0]["name"].upper()
    assert rows[0].get("etf_category", "") == ""


def test_xetra_spacex_listing_is_present() -> None:
    rows = [
        row
        for row in load_csv(ROOT / "data/listings.csv")
        if row.get("listing_key") == "XETRA::SPX"
    ]
    assert len(rows) == 1
    assert rows[0]["isin"] == SPCX_ISIN
    assert rows[0]["asset_type"] == "Stock"


def test_xetra_spacex_supplement_is_official_class_a_line() -> None:
    rows = [
        row
        for row in load_csv(ROOT / "data/masterfiles/supplemental_listings.csv")
        if row.get("ticker") == "SPX" and row.get("exchange") == "XETRA"
    ]
    assert len(rows) == 1
    assert rows[0]["isin"] == SPCX_ISIN
    assert rows[0]["asset_type"] == "Stock"
    assert rows[0]["source_key"] == "deutsche_boerse_xetra_all_tradable_equities"
    assert rows[0]["reference_scope"] == "exchange_directory"

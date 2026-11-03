from __future__ import annotations

import csv
from pathlib import Path

from scripts.build_listing_history import build_event_rows, load_change_evidence
from scripts.lib.dataio import (
    is_well_formed_metadata_update,
    load_csv,
    merge_metadata_updates,
    should_apply_metadata_field_override,
    write_csv,
)
from scripts.rebuild_dataset import apply_input_metadata_overrides, apply_output_metadata_overrides


ROOT = Path(__file__).resolve().parents[1]
METADATA_UPDATES_CSV = ROOT / "data" / "review_overrides" / "metadata_updates.csv"
LISTINGS_CSV = ROOT / "data" / "listings.csv"
OVERWRITE = {"etf_category": {"decision": "overwrite", "proposed_value": "Commodity"}}


def test_overwrite_is_limited_to_etf_category_of_etf_rows() -> None:
    assert should_apply_metadata_field_override("etf_category", "Equity", "overwrite", asset_type="ETF")
    assert not should_apply_metadata_field_override("etf_category", "Equity", "overwrite", asset_type="Stock")
    assert not should_apply_metadata_field_override("etf_category", "Equity", "overwrite")
    assert not should_apply_metadata_field_override("stock_sector", "Energy", "overwrite", asset_type="ETF")
    assert not should_apply_metadata_field_override("name", "Old", "overwrite", asset_type="ETF")
    assert not should_apply_metadata_field_override("etf_category", "Equity", "update", asset_type="ETF")


def test_overwrite_replaces_category_on_etf_rows_only() -> None:
    etf = {"ticker": "GLD", "exchange": "NYSE ARCA", "asset_type": "ETF", "etf_category": "Equity"}
    stock = {"ticker": "NEM", "exchange": "NYSE", "asset_type": "Stock", "etf_category": "Equity"}
    for apply in (apply_input_metadata_overrides, apply_output_metadata_overrides):
        assert apply(etf, OVERWRITE)["etf_category"] == "Commodity"
        assert apply(stock, OVERWRITE)["etf_category"] == "Equity"
        update = {"etf_category": {"decision": "update", "proposed_value": "Commodity"}}
        assert apply(etf, update)["etf_category"] == "Equity"


def test_overwrite_rows_outside_etf_category_are_malformed() -> None:
    base = {"ticker": "GLD", "exchange": "NYSE ARCA", "decision": "overwrite", "proposed_value": "X"}
    assert is_well_formed_metadata_update({**base, "field": "etf_category"})
    assert not is_well_formed_metadata_update({**base, "field": "isin"})
    assert not is_well_formed_metadata_update({**base, "field": "etf_category", "proposed_value": ""})


def test_automated_update_does_not_replace_reviewed_overwrite(tmp_path: Path) -> None:
    path = tmp_path / "metadata_updates.csv"
    fields = ["ticker", "exchange", "field", "decision", "proposed_value", "confidence", "reason"]
    reviewed = {
        "ticker": "GLD", "exchange": "NYSE ARCA", "field": "etf_category", "decision": "overwrite",
        "proposed_value": "Commodity", "confidence": "0.95", "reason": "Reviewed",
    }
    write_csv(path, fields, [reviewed])
    merge_metadata_updates(path, [{**reviewed, "decision": "update", "proposed_value": "Equity"}])
    assert load_csv(path)[0]["decision"] == "overwrite"
    assert load_csv(path)[0]["proposed_value"] == "Commodity"


def test_overwrite_stamps_reviewed_taxonomy_change(monkeypatch) -> None:
    monkeypatch.setattr(
        "scripts.build_listing_history.load_csv",
        lambda path: (
            [{"ticker": "GLD", "exchange": "NYSE ARCA", "field": "etf_category",
              "decision": "overwrite", "proposed_value": "Commodity"}]
            if str(path).endswith("metadata_updates.csv")
            else []
        ),
    )
    previous = [{
        "listing_key": "NYSE ARCA::GLD", "ticker": "GLD", "exchange": "NYSE ARCA",
        "name": "SPDR Gold Shares", "asset_type": "ETF", "etf_category": "Equity", "isin": "US78463V1070",
    }]
    current = [{**previous[0], "etf_category": "Commodity"}]
    event = build_event_rows(previous, current, "2026-11-03T08:00:00Z", load_change_evidence())[0]
    assert event["event_type"] == "taxonomy_changed"
    assert event["evidence_status"] == "reviewed"
    assert (event["old_value"], event["new_value"]) == ("Equity", "Commodity")


def test_committed_overwrites_target_etf_rows_and_land_in_listings() -> None:
    with METADATA_UPDATES_CSV.open(newline="", encoding="utf-8") as handle:
        overwrites = [row for row in csv.DictReader(handle) if row["decision"] == "overwrite"]
    with LISTINGS_CSV.open(newline="", encoding="utf-8") as handle:
        listings = {(row["ticker"], row["exchange"]): row for row in csv.DictReader(handle)}
    for row in overwrites:
        listing = listings[(row["ticker"], row["exchange"])]
        assert row["field"] == "etf_category"
        assert listing["asset_type"] == "ETF"
        assert listing["etf_category"] == row["proposed_value"]

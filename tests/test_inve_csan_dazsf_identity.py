from __future__ import annotations

from pathlib import Path

from scripts.lib.dataio import load_csv, is_well_formed_metadata_update


ROOT = Path(__file__).resolve().parents[1]
INVE_ISIN = "US45170X2053"
CSAN_ISIN = "US22113B1035"
DAZSF_ISIN = "KYG989AM1073"


def test_inve_name_update_is_well_formed() -> None:
    updates = [
        row
        for row in load_csv(ROOT / "data/review_overrides/metadata_updates.csv")
        if row.get("ticker") == "INVE" and row.get("exchange") == "NASDAQ" and row.get("field") == "name"
    ]
    assert len(updates) == 1
    assert is_well_formed_metadata_update(updates[0])
    assert "INVE Technologies" in updates[0]["proposed_value"]
    assert updates[0]["confidence"] == "0.99"


def test_csan_nyse_delist_is_exact_isin() -> None:
    rows = [
        row
        for row in load_csv(ROOT / "data/review_overrides/listing_transitions.csv")
        if row.get("old_listing_key") == "NYSE::CSAN"
    ]
    assert len(rows) == 1
    assert rows[0]["event_type"] == "delisted"
    assert rows[0]["identity_type"] == "exact_isin"
    assert rows[0]["identity_value"] == CSAN_ISIN
    assert rows[0]["new_listing_key"] == ""
    drops = [
        row
        for row in load_csv(ROOT / "data/review_overrides/drop_entries.csv")
        if row.get("ticker") == "CSAN" and row.get("exchange") == "NYSE"
    ]
    assert len(drops) == 1


def test_dazsf_name_update_keeps_isin() -> None:
    updates = [
        row
        for row in load_csv(ROOT / "data/review_overrides/metadata_updates.csv")
        if row.get("ticker") == "DAZSF" and row.get("exchange") == "OTC" and row.get("field") == "name"
    ]
    assert len(updates) == 1
    assert is_well_formed_metadata_update(updates[0])
    assert "Da Ai Zhi Shui" in updates[0]["proposed_value"]
    assert "KYG989AM1073" in updates[0]["reason"] or DAZSF_ISIN in updates[0]["reason"]

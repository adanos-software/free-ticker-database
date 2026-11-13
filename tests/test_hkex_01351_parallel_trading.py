from __future__ import annotations

from pathlib import Path

from scripts.lib.dataio import load_csv


ROOT = Path(__file__).resolve().parents[1]
STORED_ISIN = "KYG136931097"
OFFICIAL_PARALLEL_NEW_ISIN = "KYG136931170"


def test_bright_future_parallel_trading_keeps_stored_isin() -> None:
    listings = [
        row
        for row in load_csv(ROOT / "data/listings.csv")
        if row.get("listing_key") == "HKEX::01351"
    ]
    assert len(listings) == 1
    assert listings[0]["isin"] == STORED_ISIN
    assert listings[0]["name"] == "BRIGHT FUTURE"

    isin_updates = [
        row
        for row in load_csv(ROOT / "data/review_overrides/metadata_updates.csv")
        if row.get("ticker") == "01351"
        and row.get("exchange") == "HKEX"
        and row.get("field") == "isin"
        and row.get("decision") == "update"
    ]
    assert isin_updates == []

    allowlist = [
        row
        for row in load_csv(ROOT / "data/reports/entry_quality_warn_allowlist.csv")
        if row.get("listing_key") == "HKEX::01351"
    ]
    assert len(allowlist) == 1
    assert allowlist[0]["issue_types"] == "official_isin_mismatch"
    assert STORED_ISIN in allowlist[0]["reason"]
    assert OFFICIAL_PARALLEL_NEW_ISIN in allowlist[0]["reason"]

    reference = [
        row
        for row in load_csv(ROOT / "data/masterfiles/reference.csv")
        if row.get("source_key") == "hkex_securities_list"
        and row.get("exchange") == "HKEX"
        and row.get("ticker") == "01351"
    ]
    assert len(reference) == 1
    assert reference[0]["isin"] == OFFICIAL_PARALLEL_NEW_ISIN
    assert reference[0]["name"] == "BRIGHT FUT-NEW"

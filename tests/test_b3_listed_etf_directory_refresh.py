from __future__ import annotations

import csv
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def test_obra11_stays_in_b3_listed_etfs_and_is_not_dumped_into_listings() -> None:
    reference = [
        row
        for row in _rows(ROOT / "data" / "masterfiles" / "reference.csv")
        if row.get("source_key") == "b3_listed_etfs" and row.get("ticker") == "OBRA11"
    ]
    assert len(reference) == 1
    assert "INFRAESTRUTURA ESG" in reference[0]["name"].upper()

    listings = [
        row for row in _rows(ROOT / "data" / "listings.csv") if row.get("listing_key") == "B3::OBRA11"
    ]
    assert listings == []


def test_xifr11_is_not_copied_from_the_previous_b3_listed_etf_directory() -> None:
    listed = [
        row
        for row in _rows(ROOT / "data" / "masterfiles" / "reference.csv")
        if row.get("source_key") == "b3_listed_etfs" and row.get("ticker") == "XIFR11"
    ]
    assert listed == []
    listings = [
        row for row in _rows(ROOT / "data" / "listings.csv") if row.get("listing_key") == "B3::XIFR11"
    ]
    assert listings == []


def test_b5mb11_and_imbb11_keep_listing_isins_without_listed_etf_name_copy() -> None:
    by_key = {
        row["listing_key"]: row
        for row in _rows(ROOT / "data" / "listings.csv")
        if row.get("listing_key") in {"B3::B5MB11", "B3::IMBB11"}
    }
    assert by_key["B3::B5MB11"]["isin"] == "BRB5MBCTF005"
    assert by_key["B3::IMBB11"]["isin"] == "BRIMBBCTF002"
    assert "Ima-B5" in by_key["B3::B5MB11"]["name"] or "IMA-B5" in by_key["B3::B5MB11"]["name"].upper()

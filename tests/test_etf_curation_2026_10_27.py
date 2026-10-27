from __future__ import annotations

from pathlib import Path

from scripts.lib.dataio import load_csv


ROOT = Path(__file__).resolve().parents[1]


def _listings() -> dict[str, dict[str, str]]:
    return {row["listing_key"]: row for row in load_csv(ROOT / "data/listings.csv")}


def _aliases(row: dict[str, str]) -> set[str]:
    return {alias for alias in row.get("aliases", "").split("|") if alias}


def test_reviewed_etf_isin_fills() -> None:
    listings = _listings()
    assert listings["BATS::ARKK"]["isin"] == "US00214Q1040"
    assert listings["BATS::ARKG"]["isin"] == "US00214Q3020"
    assert listings["BATS::FBTC"]["isin"] == "US3159481098"
    assert listings["NASDAQ::ETHA"]["isin"] == "US46438R2040"
    assert listings["FSX::P500"]["isin"] == "IE00B3YCGJ38"


def test_reviewed_etf_name_fixes() -> None:
    listings = _listings()
    assert listings["NASDAQ::NVDL"]["name"] == "GraniteShares 2x Long NVDA Daily ETF"
    assert _aliases(listings["NASDAQ::NVDL"]) == {"graniteshares 2x long nvda"}
    assert listings["BATS::FBTC"]["name"] == "Fidelity Wise Origin Bitcoin Fund"
    assert "fidelity physical bitcoin etp" not in _aliases(listings["BATS::FBTC"])
    etf_rows = [row for row in listings.values() if row["asset_type"] == "ETF"]
    assert not [row["listing_key"] for row in etf_rows if "ccumalation" in (row["name"] + row["aliases"]).lower()]


def test_reviewed_etf_venue_duplicates() -> None:
    listings = _listings()
    assert "NYSE ARCA::SGOV" not in listings
    assert listings["NYSE ARCA::ETHE"]["isin"] == "US3896381072"
    assert listings["NYSE::SGOV"]["isin"] == "US46436E7186"


def test_reviewed_etf_coverage_additions() -> None:
    listings = _listings()
    assert listings["XETRA::SPYY"]["isin"] == "IE00B44Z5B48"
    assert listings["XETRA::DFEN"]["isin"] == "IE000YYE6WK5"
    assert listings["TSE::1321"]["isin"] == "JP3027650005"
    for key in ("XETRA::SPYY", "XETRA::DFEN", "TSE::1321"):
        assert listings[key]["asset_type"] == "ETF"


def test_popular_etf_aliases_owned_by_primary_listing() -> None:
    listings = _listings()
    expected = {
        "NYSE ARCA::VOO": "vanguard s&p 500",
        "NYSE ARCA::IVV": "core s&p 500",
        "NASDAQ::QQQM": "invesco nasdaq 100",
        "BATS::ARKK": "ark innovation",
        "BATS::COWZ": "pacer us cash cows 100",
        "LSE::IWDA": "core msci world",
        "BATS::ITA": "us aerospace & defense",
    }
    for key, alias in expected.items():
        assert alias in _aliases(listings[key]), key
    assert "pacer us cash cows 100" not in _aliases(listings["LSE::COWZ"])

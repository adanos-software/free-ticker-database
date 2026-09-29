from __future__ import annotations

from pathlib import Path

from scripts.lib.dataio import is_well_formed_metadata_update, load_csv


ROOT = Path(__file__).resolve().parents[1]
METADATA = ROOT / "data/review_overrides/metadata_updates.csv"
TRANSITIONS = ROOT / "data/review_overrides/listing_transitions.csv"
DROPS = ROOT / "data/review_overrides/drop_entries.csv"
ALLOWLIST = ROOT / "data/reports/entry_quality_warn_allowlist.csv"
LISTINGS = ROOT / "data/listings.csv"
COVERAGE = ROOT / "data/coverage_expansion_listings.csv"

STOLEN_ISIN_CLEARS = {
    ("CU2", "FSX"): "LU1681042864",
    ("MAST", "LSE"): "US57636Q1040",
    ("MA1", "ASX"): "US57636Q1040",
    ("ARDDF", "OTC"): "DE0005103006",
    ("CRRNF", "OTC"): "CH0012142631",
    ("HLTFF", "OTC"): "DE000A161408",
    ("LLDTF", "OTC"): "GB0004526900",
    ("LLOBF", "OTC"): "GB0004526900",
    ("VDTA", "OTC"): "IE00BGYWFS63",
    ("EXCH", "OTC"): "IE00BMG6Z448",
    ("EMDV", "BATS"): "IE00B6YX5B26",
    ("COII", "BATS"): "XS2901886445",
    ("ETHE", "NYSE"): "GB00BLD4ZM24",
    ("ARKG", "BATS"): "IE000O5M6XO1",
    ("ARKK", "BATS"): "IE000GA3D489",
    ("CNYA", "BATS"): "IE00BQT3WG13",
    ("ETHA", "NASDAQ"): "BRETHABDR006",
    ("BSOL", "NYSE ARCA"): "DE000A4A59D2",
    ("BOD-EQO", "BSE_BW"): "GB00B5TFC825",
}

COUNTRY_CLEARS = {
    ("CU2", "FSX"),
    ("MAST", "LSE"),
    ("MA1", "ASX"),
    ("ARDDF", "OTC"),
    ("CRRNF", "OTC"),
    ("HLTFF", "OTC"),
    ("VDTA", "OTC"),
    ("EXCH", "OTC"),
    ("EMDV", "BATS"),
    ("ETHE", "NYSE"),
    ("ARKG", "BATS"),
    ("ARKK", "BATS"),
    ("CNYA", "BATS"),
    ("ETHA", "NASDAQ"),
    ("BSOL", "NYSE ARCA"),
    ("BOD-EQO", "BSE_BW"),
}


def _metadata(ticker: str, exchange: str, field: str) -> dict[str, str]:
    rows = [
        row
        for row in load_csv(METADATA)
        if row.get("ticker") == ticker and row.get("exchange") == exchange and row.get("field") == field
    ]
    assert len(rows) == 1, (ticker, exchange, field, len(rows))
    assert is_well_formed_metadata_update(rows[0])
    return rows[0]


def _transition(old_listing_key: str) -> dict[str, str]:
    rows = [row for row in load_csv(TRANSITIONS) if row.get("old_listing_key") == old_listing_key]
    assert len(rows) == 1, (old_listing_key, len(rows))
    return rows[0]


def test_stolen_isin_clears_are_explicit_and_empty() -> None:
    for (ticker, exchange), stolen in STOLEN_ISIN_CLEARS.items():
        row = _metadata(ticker, exchange, "isin")
        assert row["decision"] == "clear"
        assert row["proposed_value"] == ""
        assert stolen in row["reason"]
        assert row["confidence"] == "0.99"


def test_stolen_country_clears_do_not_invent_a_replacement() -> None:
    for ticker, exchange in COUNTRY_CLEARS:
        country = _metadata(ticker, exchange, "country")
        code = _metadata(ticker, exchange, "country_code")
        assert country["decision"] == "clear"
        assert country["proposed_value"] == ""
        assert code["decision"] == "clear"
        assert code["proposed_value"] == ""


def test_lloyds_otc_keeps_uk_country() -> None:
    lloyds = [
        row
        for row in load_csv(METADATA)
        if row.get("ticker") in {"LLDTF", "LLOBF"}
        and row.get("exchange") == "OTC"
        and row.get("field") in {"country", "country_code"}
    ]
    assert lloyds == []


def test_issc_and_ethm_nasdaq_symbol_changes() -> None:
    issc = _transition("NASDAQ::ISSC")
    assert issc["new_listing_key"] == "NASDAQ::IA"
    assert issc["event_type"] == "symbol_changed"
    assert issc["identity_type"] == "same_isin"
    assert issc["identity_value"] == "US45769N1054"
    assert issc["source_url"].startswith("https://")
    ethm = _transition("NASDAQ::ETHM")
    assert ethm["new_listing_key"] == "NASDAQ::DYNC"
    assert ethm["identity_value"] == "KYG2949D1126"
    drops = {(row["ticker"], row["exchange"]) for row in load_csv(DROPS)}
    assert ("ISSC", "NASDAQ") in drops
    assert ("ETHM", "NASDAQ") in drops
    dync_drops = [
        row
        for row in load_csv(DROPS)
        if row.get("ticker") == "DYNC" and row.get("exchange") == "NASDAQ"
    ]
    assert dync_drops == []


def test_official_directory_leftover_symbol_changes() -> None:
    cases = {
        "NSE_IN::AMBEY": ("NSE_IN::DHANSA", "INE0M3I01029"),
        "NSE_IN::SILLYMONKS": ("NSE_IN::CRESTO", "INE203Y01012"),
        "LSE::JLEN": ("LSE::FGEN", "GG00BJL5FH87"),
        "LSE::GSEO": ("LSE::ENRG", "GB00BNKVP754"),
        "ADX::DRIVE": ("ADX::EMOBILITY", "AEE000601014"),
        "DFM::GULFNAV": ("DFM::ETIHADENERGY", "AEG000601019"),
        "AMS::IEX": ("AMS::HWK", "NL0010556726"),
        "HEL::KOJAMO": ("HEL::LUMO", "FI4000312251"),
        "STO::TFBANK": ("STO::AVARDA", "SE0025666969"),
    }
    for old, (new, isin) in cases.items():
        row = _transition(old)
        assert row["new_listing_key"] == new
        assert row["event_type"] == "symbol_changed"
        assert row["identity_type"] == "same_isin"
        assert row["identity_value"] == isin
        assert row["source_url"].startswith("https://")


def test_dazsf_stale_allowlist_row_removed() -> None:
    rows = [row for row in load_csv(ALLOWLIST) if row.get("listing_key") == "OTC::DAZSF"]
    assert rows == []


def test_issc_successor_seeded_with_same_isin() -> None:
    listings = {row["listing_key"]: row for row in load_csv(LISTINGS)}
    ia = listings["NASDAQ::IA"]
    assert ia["isin"] == "US45769N1054"
    assert ia["asset_type"] == "Stock"
    dync = listings["NASDAQ::DYNC"]
    assert dync["isin"] == "KYG2949D1126"
    assert dync["asset_type"] == "Stock"


def test_collision_hidden_nasdaq_successors_are_coverage_expansion() -> None:
    rows = {row["listing_key"]: row for row in load_csv(COVERAGE)}
    for key, name_fragment in {
        "NASDAQ::VIP": "Vulcan Infrastructure",
        "NASDAQ::MEDS": "DataMeds",
        "NASDAQ::MF": "MindForge",
        "NASDAQ::CIRC": "Circle8",
        "NASDAQ::TMS": "Teamshares",
    }.items():
        assert key in rows
        assert name_fragment.lower() in rows[key]["name"].lower()
        assert rows[key]["exchange"] == "NASDAQ"
        assert rows[key]["asset_type"] == "Stock"

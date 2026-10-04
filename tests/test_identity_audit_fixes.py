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
    ("PAVE", "BATS"): "IE00BLCHJ534",
    ("WTAI", "BATS"): "IE00BDVPNG13",
    ("FBTC", "BATS"): "CA31580V1040",
    ("VSOL", "NASDAQ"): "DE000A3GSUD3",
    ("EWG", "NYSE ARCA"): "BRBEWGBDR000",
    ("CORN", "NYSE ARCA"): "JE00BN7KB441",
    ("WEAT", "NYSE ARCA"): "JE00BN7KB664",
    ("INVN", "NYSE ARCA"): "US45170X2053",
    ("CNEQ", "NYSE ARCA"): "US45170X2053",
    ("METR", "LSE"): "ARP6558L1178",
    ("ESPX", "AMS"): "CA30052U2065",
    ("AIH", "ASX"): "US00809M1045",
    ("TXR", "ASX"): "ARDEUT116019",
    ("BAFS", "SET"): "PK0027901013",
    ("PORT", "SET"): "ID1000138209",
    ("UNIQ", "SET"): "ID1000159502",
    ("EMDE", "IDX"): "AREDER010016",
    ("MOLI", "IDX"): "ARP689251337",
    ("POLL", "IDX"): "ARP7905G1652",
    ("NESTLE", "PSX"): "NGNESTLE0006",
    ("NRL", "PSX"): "MU0049N00000",
    ("CIEB", "BMV"): "EGS60041C018",
    ("CLU", "NEO"): "AU0000113490",
    ("FGX", "NEO"): "AU000000FGX1",
    ("DPM", "TSX"): "AU0000413890",
    ("LONG", "TSX"): "ARP6356B1059",
    ("IVX", "TSXV"): "AU000000IVX4",
    ("RDS", "TSXV"): "AU000000RDS3",
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
    ("PAVE", "BATS"),
    ("WTAI", "BATS"),
    ("FBTC", "BATS"),
    ("VSOL", "NASDAQ"),
    ("EWG", "NYSE ARCA"),
    ("CORN", "NYSE ARCA"),
    ("WEAT", "NYSE ARCA"),
    ("ESPX", "AMS"),
    ("AIH", "ASX"),
    ("BAFS", "SET"),
    ("PORT", "SET"),
    ("UNIQ", "SET"),
    ("NESTLE", "PSX"),
    ("NRL", "PSX"),
    ("CIEB", "BMV"),
    ("CLU", "NEO"),
    ("FGX", "NEO"),
    ("DPM", "TSX"),
    ("IVX", "TSXV"),
    ("RDS", "TSXV"),
}

ISIN_ONLY_COUNTRY_KEEPS = {
    ("INVN", "NYSE ARCA"): ("United States", "US"),
    ("CNEQ", "NYSE ARCA"): ("United States", "US"),
    ("METR", "LSE"): ("United Kingdom", "GB"),
    ("TXR", "ASX"): ("Australia", "AU"),
    ("EMDE", "IDX"): ("Indonesia", "ID"),
    ("MOLI", "IDX"): ("Indonesia", "ID"),
    ("POLL", "IDX"): ("Indonesia", "ID"),
    ("LONG", "TSX"): ("Canada", "CA"),
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


def test_identiv_and_isem_are_not_recode_targets() -> None:
    inve_isin = [
        row
        for row in load_csv(METADATA)
        if row.get("ticker") == "INVE"
        and row.get("exchange") == "NASDAQ"
        and row.get("field") == "isin"
    ]
    assert inve_isin == []
    isem = _metadata("ISEM", "LSE", "isin")
    assert isem["decision"] == "update"
    assert isem["proposed_value"] == "IE00B27YCP72"


def test_lse_rea_official_directory_isin_fill() -> None:
    rea = _metadata("RE", "LSE", "isin")
    assert rea["decision"] == "update"
    assert rea["proposed_value"] == "GB0002349065"
    assert "CA75527Q1081" in rea["reason"]
    assert "lse_price_explorer" in rea["reason"]
    preferred = _metadata("RE-B", "LSE", "isin")
    assert preferred["decision"] == "update"
    assert preferred["proposed_value"] == "GB0007185639"
    assert "CA75527Q1081" in preferred["reason"]
    country = _metadata("RE", "LSE", "country")
    assert country["decision"] == "update"
    assert country["proposed_value"] == "United Kingdom"
    code = _metadata("RE", "LSE", "country_code")
    assert code["decision"] == "update"
    assert code["proposed_value"] == "GB"
    listings = {row["listing_key"]: row for row in load_csv(LISTINGS)}
    assert listings["LSE::RE"]["isin"] == "GB0002349065"
    assert listings["LSE::RE"]["country"] == "United Kingdom"
    assert listings["LSE::RE"]["country_code"] == "GB"
    assert listings["LSE::RE"]["name"] == "R.E.A. Holdings plc"
    assert listings["LSE::RE-B"]["isin"] == "GB0007185639"
    assert listings["LSE::RE-B"]["country"] == "United Kingdom"
    assert listings["LSE::RE-B"]["country_code"] == "GB"
    assert listings["LSE::RE-B"]["name"] == "R.E.A. Holdings plc"
    assert listings["FSX::BY0"]["isin"] == "GB0002349065"
    assert listings["TSXV::RE"]["isin"] == "CA75527Q1081"
    assert listings["OTC::RROYF"]["isin"] == "CA75527Q1081"
    assert listings["Euronext::RE"]["isin"] == "FR0000121634"
    assert listings["Euronext::RE"]["name"] == "Colas Sa"


def test_cleared_stolen_isins_leave_identiv_and_venue_countries() -> None:
    listings = {row["listing_key"]: row for row in load_csv(LISTINGS)}
    for (ticker, exchange), stolen in STOLEN_ISIN_CLEARS.items():
        row = listings[f"{exchange}::{ticker}"]
        assert row["isin"] == "", (exchange, ticker, stolen, row["isin"])
    for ticker, exchange in COUNTRY_CLEARS:
        row = listings[f"{exchange}::{ticker}"]
        assert row["country"] == "", (exchange, ticker, row["country"])
        assert row["country_code"] == "", (exchange, ticker, row["country_code"])
    for (ticker, exchange), (country, code) in ISIN_ONLY_COUNTRY_KEEPS.items():
        row = listings[f"{exchange}::{ticker}"]
        assert row["isin"] == ""
        assert row["country"] == country
        assert row["country_code"] == code
    inve = listings["NASDAQ::INVE"]
    assert inve["isin"] == "US45170X2053"
    assert inve["country"] == "United States"
    assert listings["FSX::INVN"]["isin"] == "US45170X2053"
    assert listings["XSTU::INVN"]["isin"] == "US45170X2053"
    assert listings["LSE::ISEM"]["isin"] == "IE00B27YCP72"
    assert listings["TSX::FBTC"]["isin"] == "CA31580V1040"
    assert listings["TSXV::RE"]["isin"] == "CA75527Q1081"


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

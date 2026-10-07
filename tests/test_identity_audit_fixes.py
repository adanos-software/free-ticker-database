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
    ("RE", "LSE"): "CA75527Q1081",
    ("RE-B", "LSE"): "CA75527Q1081",
    ("METR", "LSE"): "ARP6558L1178",
    ("ESPX", "AMS"): "CA30052U2065",

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
    ("BEAR", "TSXV"): "AU00000BEAR2",
    ("CYG", "TSXV"): "AU000000CYG6",
    ("ACL", "TSXV"): "AU0000148496",
    ("RBX", "TSXV"): "AU0000152803",
    ("MMX", "TSXV"): "US5732841060",
    ("MMXLF", "OTC"): "US5732841060",
    ("SPYR", "OTC"): "IE00BKWQ0C77",
    ("WELX", "OTC"): "IE000EFHIFG3",
    ("WTER", "OTC"): "IE000X9TLGN8",
    ("VISM", "OTC"): "AU0000026171",
    ("ROYL", "OTC"): "AU0000233397",
    ("DOD", "SET"): "US25746U1097",
    ("CPW", "SET"): "IL0010824113",
    ("MCHT", "OTC"): "IE00BM8QS095",
    ("STTX", "OTC"): "IE00BKWQ0N82",
    ("ICBU", "OTC"): "IE00BDQZ5152",
    ("WEBC", "OTC"): "IE000MJIXFE0",
    ("PTAM", "OTC"): "IE000X5OD4M3",
    ("FLXP", "OTC"): "IE00BMDPBY65",
    ("CSOL", "OTC"): "CH1385084384",
    ("FLES", "OTC"): "IE00BFWXDY69",
    ("XSVT", "OTC"): "LU0460391732",
    ("ARIN", "SET"): "IL0003660136",
    ("RLCO", "IDX"): "IL0003930174",
    ("TKN", "TSX"): "TH6927010004",
    ("ASMMF", "OTC"): "NL0000334118",
    ("SHR", "SET"): "CH0024638212",
    ("DUSIT", "SET"): "CA24463V1013",
    ("PRINC", "SET"): "GB00B0MDF233",
    ("TIPS", "OTC"): "IE00BZ0G8977",
    ("CHMMF", "OTC"): "LU1834983634",
    ("CNFN", "OTC"): "EGS738I1C018",
    ("BYSD", "OTC"): "IL0007590198",
    ("EGIL", "OTC"): "IE00B3B8PX14",
    ("ASEKF", "OTC"): "JP3965410008",
    ("BCDRF", "OTC"): "CA8119161054",
    ("SARDF", "OTC"): "CA8119161054",
    ("ABIT", "OTC"): "IE000SBHVL31",
    ("GORO", "NYSE"): "US38068T1051",
}

COUNTRY_CLEARS = {
    ("CU2", "FSX"),
    ("MAST", "LSE"),

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
    ("RE", "LSE"),
    ("RE-B", "LSE"),
    ("ESPX", "AMS"),

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
    ("BEAR", "TSXV"),
    ("CYG", "TSXV"),
    ("ACL", "TSXV"),
    ("RBX", "TSXV"),
    ("MMX", "TSXV"),
    ("MMXLF", "OTC"),
    ("SPYR", "OTC"),
    ("WELX", "OTC"),
    ("WTER", "OTC"),
    ("VISM", "OTC"),
    ("ROYL", "OTC"),
    ("DOD", "SET"),
    ("CPW", "SET"),
    ("MCHT", "OTC"),
    ("STTX", "OTC"),
    ("ICBU", "OTC"),
    ("WEBC", "OTC"),
    ("PTAM", "OTC"),
    ("FLXP", "OTC"),
    ("CSOL", "OTC"),
    ("FLES", "OTC"),
    ("XSVT", "OTC"),
    ("ARIN", "SET"),
    ("RLCO", "IDX"),
    ("TKN", "TSX"),
    ("ASMMF", "OTC"),
    ("SHR", "SET"),
    ("DUSIT", "SET"),
    ("PRINC", "SET"),
    ("TIPS", "OTC"),
    ("CHMMF", "OTC"),
    ("CNFN", "OTC"),
    ("BYSD", "OTC"),
    ("EGIL", "OTC"),
    ("BCDRF", "OTC"),
    ("SARDF", "OTC"),
    ("ABIT", "OTC"),
    ("GORO", "NYSE"),
}

ISIN_ONLY_COUNTRY_KEEPS = {
    ("INVN", "NYSE ARCA"): ("United States", "US"),
    ("CNEQ", "NYSE ARCA"): ("United States", "US"),
    ("METR", "LSE"): ("United Kingdom", "GB"),
    ("EMDE", "IDX"): ("Indonesia", "ID"),
    ("MOLI", "IDX"): ("Indonesia", "ID"),
    ("POLL", "IDX"): ("Indonesia", "ID"),
    ("LONG", "TSX"): ("Canada", "CA"),
    ("ASEKF", "OTC"): ("Japan", "JP"),
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
    rea = _metadata("RE", "LSE", "isin")
    assert rea["proposed_value"] == ""
    assert "GB0002349065" not in rea["reason"]


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
    assert listings["ASX::BEAR"]["isin"] == "AU00000BEAR2"
    assert listings["ASX::CYG"]["isin"] == "AU000000CYG6"
    assert listings["ASX::ACL"]["isin"] == "AU0000148496"
    assert listings["ASX::RBX"]["isin"] == "AU0000152803"
    assert listings["NYSE::MLM"]["isin"] == "US5732841060"
    assert listings["XETRA::SPYR"]["isin"] == "IE00BKWQ0C77"
    assert listings["XETRA::WELX"]["isin"] == "IE000EFHIFG3"
    assert listings["XETRA::WTER"]["isin"] == "IE000X9TLGN8"
    assert listings["ASX::VISM"]["isin"] == "AU0000026171"
    assert listings["ASX::ROYL"]["isin"] == "AU0000233397"
    assert listings["NYSE::D"]["isin"] == "US25746U1097"
    assert listings["NASDAQ::CHKP"]["isin"] == "IL0010824113"
    assert listings["LSE::MCHT"]["isin"] == "IE00BM8QS095"
    assert listings["LSE::TELE"]["isin"] == "IE00BKWQ0N82"
    assert listings["LSE::ICBU"]["isin"] == "IE00BDQZ5152"
    assert listings["XETRA::WEBC"]["isin"] == "IE000MJIXFE0"
    assert listings["XETRA::PTAM"]["isin"] == "IE000X5OD4M3"
    assert listings["LSE::EUPA"]["isin"] == "IE00BMDPBY65"
    assert listings["SIX::CSOL"]["isin"] == "CH1385084384"
    assert listings["LSE::FLES"]["isin"] == "IE00BFWXDY69"
    assert listings["LSE::XBCU"]["isin"] == "LU0460391732"
    assert listings["TASE::ARIN"]["isin"] == "IL0003660136"
    assert listings["TASE::RLCO"]["isin"] == "IL0003930174"
    assert listings["SET::TKN"]["isin"] == "TH6927010004"
    assert listings["AMS::ASM"]["isin"] == "NL0000334118"
    assert listings["OTC::ASMXF"]["isin"] == "NL0000334118"
    assert listings["SIX::SCHN"]["isin"] == "CH0024638212"
    assert listings["FSX::DTC"]["isin"] == "CA24463V1013"
    assert listings["LSE::POS"]["isin"] == "GB00B0MDF233"
    assert listings["LSE::UTIP"]["isin"] == "IE00BZ0G8977"
    assert listings["Euronext::CHM"]["isin"] == "LU1834983634"
    assert listings["EGX::SRWA"]["isin"] == "EGS738I1C018"
    assert listings["TASE::GVYM"]["isin"] == "IL0007590198"
    assert listings["LSE::IGIL"]["isin"] == "IE00B3B8PX14"
    assert listings["TSE::207A"]["isin"] == "JP3965410008"
    assert listings["TSE::7259"]["isin"] == "JP3102000001"
    assert listings["OTC::ASEKY"]["isin"] == "US00956Q1067"
    assert listings["NYSE::SA"]["isin"] == "CA8119161054"
    assert listings["FSX::SRM"]["isin"] == "CA8119161054"
    assert listings["XSTU::SRM"]["isin"] == "CA8119161054"
    assert listings["XETRA::ABIU"]["isin"] == "IE000SBHVL31"
    assert listings["TSE::542A"]["isin"] == "JP3800210001"
    assert listings["TSE::581A"]["isin"] == "JP3306510003"


def test_asx_official_isins_replace_cleared_stolen_values() -> None:
    cases = {
        ("AIH", "ASX"): ("AU0000423667", "US00809M1045"),
        ("MA1", "ASX"): ("AU0000380917", "US57636Q1040"),
        ("TXR", "ASX"): ("AU0000450611", "ARDEUT116019"),
    }
    listings = {row["listing_key"]: row for row in load_csv(LISTINGS)}
    for (ticker, exchange), (official, stolen) in cases.items():
        row = _metadata(ticker, exchange, "isin")
        assert row["decision"] == "update"
        assert row["proposed_value"] == official
        assert stolen in row["reason"]
        assert official != stolen
        listing = listings[f"{exchange}::{ticker}"]
        assert listing["isin"] == official
        assert stolen not in listing["isin"]


def test_tgb_isin_follows_issuer_rebrand() -> None:
    row = _metadata("TGB", "NYSE", "isin")
    assert row["decision"] == "update"
    assert row["proposed_value"] == "CA89472Y1079"
    assert "CA8765111064" in row["reason"]
    udm = _metadata("UDM", "FSX", "name")
    assert udm["decision"] == "update"
    assert udm["proposed_value"] == "Trekor Metals Ltd"
    listings = {item["listing_key"]: item for item in load_csv(LISTINGS)}
    assert listings["NYSE::TGB"]["isin"] == "CA89472Y1079"
    assert listings["NYSE::TGB"]["name"] == "Trekor Metals Ltd"
    assert listings["FSX::UDM"]["name"] == "Trekor Metals Ltd"
    assert listings["LSE::TKO"]["isin"] == "CA89472Y1079"


def test_official_name_updates_match_listing_keyed_directories() -> None:
    cases = {
        ("AGNT", "NASDAQ"): "AGNT, Inc.",
        ("AIB", "NYSE"): "AIB Data Centers Inc.",
        ("GORO", "NYSE"): "Goldgroup Mining Inc.",
        ("PN", "NASDAQ"): "PN Smart Energy Limited",
        ("RAVI", "NYSE ARCA"): "Northern Trust Ultra-Short Fixed Income ETF",
        ("RTB", "NASDAQ"): "RTB Digital, Inc.",
        ("RUM", "NASDAQ"): "RUM Group Inc.",
        ("SRXH", "NYSE"): "SRX Global Inc.",
        ("TGB", "NYSE"): "Trekor Metals Ltd",
    }
    for (ticker, exchange), name in cases.items():
        row = _metadata(ticker, exchange, "name")
        assert row["decision"] == "update"
        assert row["proposed_value"] == name
        assert row["confidence"] == "0.96"


def test_hvii_symbol_change_does_not_copy_cayman_isin() -> None:
    row = _transition("NASDAQ::HVII")
    assert row["new_listing_key"] == "NASDAQ::ONEN"
    assert row["event_type"] == "symbol_changed"
    assert row["identity_type"] == "same_issuer"
    assert "KYG4405D1079" not in row["identity_value"]
    assert row["source_url"].startswith("https://")
    drops = {(item["ticker"], item["exchange"]) for item in load_csv(DROPS)}
    assert ("HVII", "NASDAQ") in drops
    onen_isin = [
        item
        for item in load_csv(METADATA)
        if item.get("ticker") == "ONEN"
        and item.get("exchange") == "NASDAQ"
        and item.get("field") == "isin"
    ]
    assert all("KYG4405D1079" not in item.get("proposed_value", "") for item in onen_isin)
    assert all("KYG4405D1079" not in item.get("reason", "") for item in onen_isin)


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


def test_cell_impact_reverse_split_overrides() -> None:
    row = _metadata("CI", "STO", "isin")
    assert row["decision"] == "update"
    assert row["proposed_value"] == "SE0025940513"
    assert "SE0017885379" in row["reason"]
    transition = _transition("STO::CELLI")
    assert transition["new_listing_key"] == "STO::CI"
    assert transition["event_type"] == "symbol_changed"
    assert transition["identity_type"] == "same_isin"
    assert transition["identity_value"] == "SE0025940513"
    assert transition["source_url"].startswith("https://")
    drops = {(item["ticker"], item["exchange"]) for item in load_csv(DROPS)}
    assert ("CELLI", "STO") in drops
    assert ("CI", "STO") not in drops


def test_cell_impact_listings_follow_official_isin() -> None:
    listings = {row["listing_key"]: row for row in load_csv(LISTINGS)}
    assert listings["STO::CI"]["isin"] == "SE0025940513"
    assert "STO::CELLI" not in listings
    allow = [row for row in load_csv(ALLOWLIST) if row.get("listing_key") == "STO::CI"]
    assert allow == []


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

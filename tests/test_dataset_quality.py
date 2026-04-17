from __future__ import annotations

import csv
import json
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime
from functools import lru_cache
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"


@lru_cache(maxsize=None)
def load_csv(name: str):
    with (DATA_DIR / name).open(newline="") as handle:
        return list(csv.DictReader(handle))


def ticker_row(ticker: str):
    for row in load_csv("tickers.csv"):
        if row["ticker"] == ticker:
            return row
    return None


def ticker_exchange_row(ticker: str, exchange: str):
    for row in load_csv("tickers.csv"):
        if row["ticker"] == ticker and row["exchange"] == exchange:
            return row
    return None


def listing_ticker_exchange_row(ticker: str, exchange: str):
    for row in load_csv("listings.csv"):
        if row["ticker"] == ticker and row["exchange"] == exchange:
            return row
    return None


def test_common_word_aliases_removed():
    aliases = {(row["ticker"], row["alias"]) for row in load_csv("aliases.csv")}
    blocked = {
        ("ANPCF", "angle"),
        ("MSFT", "azure"),
        ("TSLA", "elon"),
        ("TSLA", "musk"),
        ("KO", "coke"),
        ("LOADS", "loads"),
        ("LOADSR1", "loads"),
        ("CA1", "circus"),
        ("LPP", "reserved"),
        ("0R0X", "cybertruck"),
        ("0R0X", "model 3"),
        ("0R0X", "model y"),
        ("0R2V", "iphone"),
        ("0R2V", "tim cook"),
        ("AMZ", "aws"),
        ("AMZ", "bezos"),
        ("MSF", "windows"),
        ("MSF", "satya"),
        ("TL0", "cybertruck"),
        ("TL0", "model 3"),
        ("TL0", "model y"),
    }
    assert aliases.isdisjoint(blocked)


def test_yahoo_high_risk_aliases_removed():
    assert "square inc" not in ticker_row("SQ")["aliases"]
    assert "alta" not in ticker_row("ALT")["aliases"]
    assert "altitude" not in ticker_row("ALT")["aliases"]
    assert "argo investments" not in ticker_row("ARG")["aliases"]
    assert "argo investments" not in ticker_row("ARREF")["aliases"]
    assert "Berkshire" not in ticker_row("BRK")["aliases"]
    assert "BRK-A" not in ticker_row("BRK")["aliases"]
    assert "TBC" not in ticker_row("T")["aliases"]
    assert "TBC" not in listing_ticker_exchange_row("SOBA", "XETRA")["aliases"]
    assert "pt itsec asia" not in listing_ticker_exchange_row("CYBR", "LSE")["aliases"]


def test_residual_alias_collisions_and_metadata_contamination_are_removed():
    gen = ticker_exchange_row("GEN", "NASDAQ")
    gen_lse = listing_ticker_exchange_row("0AD5", "LSE")
    fg = ticker_exchange_row("FG", "NYSE")
    amg = ticker_exchange_row("AMG", "NYSE")
    cntx = ticker_exchange_row("CNTX", "NASDAQ")
    cybr = listing_ticker_exchange_row("CYBR", "LSE")
    sea = ticker_exchange_row("SEA", "NYSE ARCA")
    soba = listing_ticker_exchange_row("SOBA", "XETRA")
    att = ticker_exchange_row("T", "NYSE")
    dte = ticker_exchange_row("DTE", "NYSE")
    gap = ticker_exchange_row("GAP", "NYSE")
    key = ticker_exchange_row("KEY", "NYSE")
    assert gen["country"] == "United States"
    assert gen["isin"] == "US6687711084"
    assert gen_lse["country"] == "United States"
    assert gen_lse["isin"] == "US6687711084"

    assert fg["country"] == "United States"
    assert fg["isin"] == "US30190A1043"
    assert "fasadgruppen group" not in fg["aliases"]

    assert amg["country"] == "United States"
    assert amg["isin"] == "US0082521081"
    assert "amg advanced metallurgical group" not in amg["aliases"]
    assert "atlas metals" not in amg["aliases"]
    assert "MGR" not in amg["aliases"]

    assert cntx["country"] == "United States"
    assert cntx["isin"] == "US21077P1084"
    assert "pt century textile industry" not in cntx["aliases"]

    assert cybr["country"] == "Ireland"
    assert cybr["isin"] == "IE00BJXRZJ40"
    assert "cyberark" not in cybr["aliases"]

    assert sea["country"] == "United States"
    assert sea["isin"] == "US26922B8651"
    assert sea["etf_category"] == ""
    assert "sea forest" not in sea["aliases"]
    assert "seascape energy asia" not in sea["aliases"]
    assert "srm entertainment" not in sea["aliases"]

    assert soba["country"] == "United States"
    assert "TBB" not in soba["aliases"]

    assert att["isin"] == "US00206R1023"
    assert "TBB" not in att["aliases"]

    assert dte["country"] == "United States"
    assert dte["isin"] == "US2333311072"
    assert "DTB" not in dte["aliases"]
    assert "DTG" not in dte["aliases"]
    assert "DTW" not in dte["aliases"]

    assert gap["country"] == "United States"
    assert gap["isin"] == ""
    assert "gale pacific" not in gap["aliases"]

    assert key["isin"] == "US4932671088"
    assert ticker_exchange_row("KEY-PL", "NYSE") is None

    assert ticker_exchange_row("TEFN", "BMV") is None


def test_codex_worker_collision_findings_are_cleaned():
    bmr = ticker_exchange_row("BMR", "NASDAQ")
    rail = ticker_exchange_row("RAIL", "NASDAQ")
    rdn = ticker_exchange_row("RDN", "NYSE")
    klr = ticker_exchange_row("KLR", "LSE")

    assert bmr["isin"] == ""
    assert bmr["country"] == "Israel"
    assert "ballymore resources" not in bmr["aliases"]
    assert "A3DV8W" not in bmr["aliases"]

    assert rail["isin"] == "US3570231007"
    assert rail["country"] == "United States"
    assert "railcare group ab" not in rail["aliases"]
    assert "A0D890" not in rail["aliases"]

    assert rdn["isin"] == "US7502361014"
    assert rdn["country"] == "United States"
    assert rdn["stock_sector"] == "Financials"
    assert "raiden resources" not in rdn["aliases"]
    assert "750236" not in rdn["aliases"]

    assert klr["isin"] == "GB0004866223"
    assert klr["country"] == "United Kingdom"
    assert klr["stock_sector"] == "Industrials"
    assert "kaili resources" not in klr["aliases"]


def test_generic_fund_wrapper_aliases_removed():
    aliases = {(row["ticker"], row["alias"]) for row in load_csv("aliases.csv")}
    blocked = {
        ("IBCA", "ishares trust"),
        ("BOTZ39", "global x funds"),
        ("BITU", "proshares trust"),
        ("AFIX", "allspring exchange-traded funds trust"),
    }
    assert aliases.isdisjoint(blocked)


def test_non_common_instruments_removed():
    tickers = {row["ticker"] for row in load_csv("tickers.csv")}
    assert "AACBR" not in tickers
    assert "AIRLINK-CA" not in tickers
    assert "ATRL-CAPRN" not in tickers
    assert "BAC-P-B" not in tickers
    assert "BN-PFA" not in tickers
    assert "BOHO-PREF" not in tickers
    assert "BRN-PR-A" not in tickers
    assert "CORE-PREF" not in tickers
    assert "MTB-PH" not in tickers
    assert "MTB-PJ" not in tickers
    assert "MTB-PK" not in tickers
    assert "PVF-PR-U" not in tickers
    assert "PWF-PFA" not in tickers
    assert "SDIP-PREF" not in tickers
    assert "SNI-PR-A" not in tickers
    assert "TFIN-P" not in tickers
    assert "MTL-CAPR" not in tickers
    assert "MTL-CAPRN1" not in tickers
    assert "MTL-CMAR" not in tickers
    assert "MTL-CMARN1" not in tickers
    assert "MTL-CMAY" not in tickers
    assert "MTL-CMAYN1" not in tickers
    assert "VOLO-PREF" not in tickers
    assert "BTSGU" not in tickers
    assert "001515" not in tickers
    assert "005385" not in tickers
    assert "00499K" not in tickers
    assert "005945" not in tickers
    assert "003925" not in tickers
    assert "00781K" not in tickers
    assert "P01GIS0802" not in tickers
    assert "P03FRR2110" not in tickers
    assert "P05FRR2201" not in tickers
    assert "MFFLR1" not in tickers
    assert "SHDTR1" not in tickers


def test_country_examples_corrected():
    assert ticker_row("AAIGF")["country"] == "Hong Kong"
    assert listing_ticker_exchange_row("AANNF", "OTC")["country"] == "Luxembourg"
    assert listing_ticker_exchange_row("AAVMY", "OTC")["country"] == "Netherlands"
    assert listing_ticker_exchange_row("0A00", "LSE")["country"] == "Netherlands"
    assert listing_ticker_exchange_row("04Q", "XETRA")["country"] == "Finland"
    assert ticker_row("A1CR34") is None


def test_country_from_isin_handles_vietnam_prefix():
    from scripts.rebuild_dataset import country_from_isin

    assert country_from_isin("VN000000IPA5") == "Vietnam"


def test_country_from_isin_handles_additional_exchange_prefixes():
    from scripts.rebuild_dataset import country_from_isin

    assert country_from_isin("MT0000780107") == "Malta"
    assert country_from_isin("LI0315487269") == "Liechtenstein"
    assert country_from_isin("PK0043901013") == "Pakistan"


def test_vietnam_hnx_official_isin_fallback_corrects_country():
    ipa = listing_ticker_exchange_row("IPA", "HNX")

    assert ipa["isin"] == "VN000000IPA5"
    assert ipa["country"] == "Vietnam"
    assert ipa["country_code"] == "VN"


def test_thin_otc_metadata_is_backfilled_for_verified_listings():
    dmnif = ticker_exchange_row("DMNIF", "OTC")
    dtref = ticker_exchange_row("DTREF", "OTC")

    assert dmnif["name"] == "Damon Inc."
    assert dmnif["country"] == "Canada"
    assert dmnif["country_code"] == "CA"
    assert dmnif["isin"] == "CA2357502053"

    assert dtref["name"] == "DATELINE RESOURCES LTD."
    assert dtref["country"] == "Australia"
    assert dtref["country_code"] == "AU"
    assert dtref["isin"] == "AU000000DTR1"
    assert dtref["stock_sector"] == "Materials"


def test_generic_us_legacy_exchange_is_removed_from_core():
    assert all(row["exchange"] != "US" for row in load_csv("listings.csv"))
    assert ticker_exchange_row("EFGD", "US") is None
    assert ticker_exchange_row("RWAX", "US") is None


def test_psx_name_corrections_and_alias_cleanup_applied():
    elcm = ticker_exchange_row("ELCM", "PSX")
    fecm = listing_ticker_exchange_row("FECM", "PSX")

    assert elcm is not None
    assert elcm["name"] == "Elahi Cotton Mills Limited"
    assert "elite capital modaraba 1st" not in elcm["aliases"]

    assert fecm is not None
    assert fecm["name"] == "First Elite Capital Modaraba"


def test_yahoo_corrected_etf_outliers_are_cleaned():
    cam = ticker_exchange_row("CAM", "NYSE ARCA")
    netz = ticker_exchange_row("NETZ", "NYSE")
    tek = ticker_exchange_row("TEK", "NYSE ARCA")

    assert cam["name"] == "AB California Intermediate Municipal ETF"
    assert cam["country"] == "United States"
    assert cam["country_code"] == "US"
    assert cam["isin"] == "US00039J7726"
    assert cam["etf_category"] == ""

    assert netz["name"] == "TCW Transform Systems ETF"
    assert netz["country"] == "United States"
    assert netz["country_code"] == "US"
    assert netz["isin"] == "US29287L2051"

    assert tek["name"] == "iShares Technology Opportunities Active ETF"
    assert tek["country"] == "United States"
    assert tek["country_code"] == "US"
    assert tek["isin"] == "US09290C7728"


def test_non_otc_country_isin_mismatches_are_cleared_or_verified():
    assert ticker_exchange_row("T", "NYSE")["isin"] == "US00206R1023"
    assert ticker_exchange_row("PKO", "WSE")["isin"] == "PLPKO0000016"
    assert ticker_exchange_row("PRX", "AMS")["isin"] == "NL0013654783"


def test_safe_tse_supplements_are_present_without_cross_exchange_collisions():
    veritas = ticker_exchange_row("130A", "TSE")
    topix = ticker_exchange_row("1306", "TSE")
    note = ticker_exchange_row("5243", "TSE")

    assert veritas is not None
    assert veritas["name"] == "Veritas In Silico Inc."
    assert veritas["country"] == "Japan"
    assert veritas["country_code"] == "JP"

    assert topix is not None
    assert topix["asset_type"] == "ETF"
    assert topix["country"] == "Japan"
    assert topix["country_code"] == "JP"

    assert note is not None
    assert note["name"] == "note inc."
    assert note["country"] == "Japan"
    assert note["country_code"] == "JP"

    assert ticker_exchange_row("1301", "TSE") is None
    assert ticker_exchange_row("25935", "TSE") is None
    assert ticker_exchange_row("1301", "TWSE") is not None


def test_twse_non_common_lines_are_reclassified_or_removed():
    assert ticker_exchange_row("0052", "TWSE")["asset_type"] == "ETF"
    assert ticker_exchange_row("00939", "TWSE")["asset_type"] == "ETF"
    assert ticker_exchange_row("00941", "TWSE")["asset_type"] == "ETF"
    assert ticker_exchange_row("01002T", "TWSE")["asset_type"] == "ETF"
    assert ticker_exchange_row("01009T", "TWSE")["asset_type"] == "ETF"
    assert ticker_exchange_row("FGX", "NEO")["asset_type"] == "ETF"
    assert ticker_exchange_row("2883B", "TWSE") is None
    assert ticker_exchange_row("6781", "TWSE")["name"] == "AES Holding Co., Ltd."


def test_supplement_only_rows_do_not_inherit_cross_exchange_aliases():
    aeu = ticker_exchange_row("AEU", "ASX")
    azt = ticker_exchange_row("AZT", "OSL")
    prs = ticker_exchange_row("PRS", "OSL")
    eam = ticker_exchange_row("EAM", "OSL")

    assert aeu is not None
    assert aeu["aliases"] == "atomic eagle"

    assert azt is not None
    assert azt["aliases"] == "arcticzymes technologies a"

    assert prs is not None
    assert prs["aliases"] == "prosafe"

    assert eam is not None
    assert eam["aliases"] == "eam solar a"


def test_contaminated_us_primaries_cleaned():
    aapl = ticker_row("AAPL")
    msft = ticker_row("MSFT")
    tsla = ticker_row("TSLA")

    assert aapl["isin"] == "US0378331005"
    assert "apple cdr" not in aapl["aliases"]

    assert msft["isin"] == "US5949181045"
    assert "azure" not in msft["aliases"]
    assert "microsoft cdr" not in msft["aliases"]

    assert tsla["isin"] == "US88160R1014"
    assert "elon" not in tsla["aliases"]
    assert "musk" not in tsla["aliases"]
    assert "tesla cdr" not in tsla["aliases"]


def test_depositary_and_cross_issuer_aliases_removed():
    ubs = listing_ticker_exchange_row("UBS", "NYSE")
    asml_ams = ticker_exchange_row("ASML", "AMS")

    assert ticker_row("ARM") is None
    assert ubs is not None
    assert asml_ams is not None
    assert asml_ams["country"] == "Netherlands"
    assert asml_ams["aliases"] == ""
    assert "ubm development" not in ubs["aliases"]
    assert "united bus service" not in ubs["aliases"]
    assert "urbas grupo financiero" not in ubs["aliases"]


def test_numeric_namespace_aliases_and_collisions_cleaned():
    supercomnet = ticker_row("0001")
    assert supercomnet is not None
    assert "ck hutchison" not in supercomnet["aliases"]
    assert "hutchison" not in supercomnet["aliases"]

    assert ticker_row("002620") is None
    assert ticker_row("0050") is None


def test_aliases_csv_has_no_exact_duplicates():
    rows = load_csv("aliases.csv")
    keys = [(row["ticker"], row["alias"], row["alias_type"]) for row in rows]
    counts = Counter(keys)
    assert not [key for key, count in counts.items() if count > 1]


def test_build_alias_rows_prioritizes_isin_type():
    from scripts.rebuild_dataset import build_alias_rows

    rows = [
        {
            "ticker": "TEST",
            "isin": "US0000000001",
            "aliases": ["US0000000001", "TEST.NYSE"],
            "wkn": "",
        }
    ]
    alias_rows = build_alias_rows(rows, {})
    assert alias_rows == [
        {"ticker": "TEST", "alias": "US0000000001", "alias_type": "isin"},
        {"ticker": "TEST", "alias": "TEST.NYSE", "alias_type": "exchange_ticker"},
    ]


def test_build_alias_rows_marks_numeric_namespace_aliases_as_exchange_ticker():
    from scripts.rebuild_dataset import build_alias_rows

    rows = [
        {
            "ticker": "0050",
            "exchange": "TWSE",
            "isin": "TW0000050004",
            "aliases": ["0050", "yuanta taiwan top 50 etf"],
            "wkn": "",
        }
    ]
    alias_rows = build_alias_rows(rows, {("0050", "0050"): "name"})
    assert alias_rows == [
        {"ticker": "0050", "alias": "TW0000050004", "alias_type": "isin"},
        {"ticker": "0050", "alias": "0050", "alias_type": "exchange_ticker"},
        {"ticker": "0050", "alias": "yuanta taiwan top 50 etf", "alias_type": "name"},
    ]


def test_build_primary_ticker_rows_keeps_one_legacy_row_per_ticker():
    from scripts.rebuild_dataset import build_primary_ticker_rows

    rows = [
        {
            "ticker": "GLDU",
            "exchange": "LSE",
            "name": "UBS ETC ON BBG CMCI GOLD IDX USD",
            "isin": "CH0346134395",
        },
        {
            "ticker": "GLDU",
            "exchange": "TSX",
            "name": "Global X Gold Producer Equity Daily Bull ETF",
            "isin": "CA08660T1003",
        },
        {
            "ticker": "PLUR",
            "exchange": "TASE",
            "name": "PLURI",
            "isin": "US72942G2030",
        },
        {
            "ticker": "PLUR",
            "exchange": "NASDAQ",
            "name": "Pluri Inc.",
            "isin": "US72942G2030",
        },
    ]

    primary = build_primary_ticker_rows(rows)

    assert [(row["ticker"], row["exchange"]) for row in primary] == [
        ("GLDU", "TSX"),
        ("PLUR", "NASDAQ"),
    ]


def test_namespace_collision_respects_manual_isin_corrections(monkeypatch):
    from scripts import rebuild_dataset

    row = {
        "ticker": "0050",
        "name": "Yuanta/P-shares Taiwan Top 50",
        "exchange": "TWSE",
        "asset_type": "ETF",
        "isin": "MYQ0050OO003",
    }
    monkeypatch.setitem(
        rebuild_dataset.MANUAL_ISIN_CORRECTIONS,
        "0050",
        "TW0000050004",
    )

    assert rebuild_dataset.is_namespace_collision_row(
        row,
        ["yuanta/p-shares taiwan top 50"],
        set(),
    ) is False


def test_cleanse_conflicting_isin_rows_clears_peer_company_contamination():
    from scripts.rebuild_dataset import cleanse_conflicting_isin_rows

    rows = [
        {
            "ticker": "RR",
            "name": "Richtech Robotics Inc. Class B Common Stock",
            "exchange": "NASDAQ",
            "asset_type": "Stock",
            "country": "United Kingdom",
            "country_code": "GB",
            "isin": "GB00B63H8491",
            "aliases": ["rolls royce holdings", "rolls royce adr"],
        },
        {
            "ticker": "RRU",
            "name": "Rolls-Royce Holdings PLC",
            "exchange": "XETRA",
            "asset_type": "Stock",
            "country": "United Kingdom",
            "country_code": "GB",
            "isin": "GB00B63H8491",
            "aliases": ["rolls royce holdings", "rolls royce adr"],
        },
    ]

    cleaned = cleanse_conflicting_isin_rows(rows)
    richtech = next(row for row in cleaned if row["ticker"] == "RR")
    rolls = next(row for row in cleaned if row["ticker"] == "RRU")

    assert richtech["isin"] == ""
    assert richtech["country"] == ""
    assert richtech["country_code"] == ""
    assert richtech["aliases"] == []
    assert rolls["isin"] == "GB00B63H8491"
    assert rolls["country"] == "United Kingdom"


def test_drop_stale_tmx_etf_duplicates_prefers_current_official_symbol(monkeypatch):
    from scripts import rebuild_dataset

    rows = [
        {
            "ticker": "HIU",
            "name": "BetaPro S&P 500® Daily Inverse ETF",
            "exchange": "TSX",
            "asset_type": "ETF",
            "country": "Canada",
            "country_code": "CA",
            "isin": "CA08660P1080",
            "aliases": [],
        },
        {
            "ticker": "SPXI",
            "name": "BetaPro S&P 500 Daily Inverse ETF",
            "exchange": "TSX",
            "asset_type": "ETF",
            "country": "Canada",
            "country_code": "CA",
            "isin": "",
            "aliases": [],
        },
        {
            "ticker": "QQD",
            "name": "BetaPro NASDAQ-100 -2x Daily Bear ETF",
            "exchange": "TSX",
            "asset_type": "ETF",
            "country": "Canada",
            "country_code": "CA",
            "isin": "",
            "aliases": [],
        },
        {
            "ticker": "QQD.U",
            "name": "BetaPro NASDAQ-100 -2x Daily Bear ETF",
            "exchange": "TSX",
            "asset_type": "ETF",
            "country": "Canada",
            "country_code": "CA",
            "isin": "",
            "aliases": [],
        },
        {
            "ticker": "MUSA",
            "name": "Middlefield U.S. Equity Dividend ETF",
            "exchange": "TSX",
            "asset_type": "ETF",
            "country": "Canada",
            "country_code": "CA",
            "isin": "",
            "aliases": [],
        },
    ]
    monkeypatch.setattr(
        rebuild_dataset,
        "load_active_official_reference_rows",
        lambda: {
            ("SPXI", "ETF"): ({"ticker": "SPXI", "exchange": "TSX", "asset_type": "ETF", "name": "BetaPro S&P 500 Daily Inverse ETF"},),
            ("QQD", "ETF"): ({"ticker": "QQD", "exchange": "TSX", "asset_type": "ETF", "name": "BetaPro NASDAQ-100 -2x Daily Bear ETF"},),
            ("QQD.U", "ETF"): ({"ticker": "QQD.U", "exchange": "TSX", "asset_type": "ETF", "name": "BetaPro NASDAQ-100 -2x Daily Bear ETF"},),
        },
    )

    cleaned = rebuild_dataset.drop_stale_tmx_etf_duplicates(rows)

    assert [row["ticker"] for row in cleaned] == ["SPXI", "QQD", "QQD.U", "MUSA"]


def test_should_drop_contextual_alias_drops_untrusted_shared_alias():
    from scripts.rebuild_dataset import should_drop_contextual_alias

    row = {
        "ticker": "WSML",
        "name": "iShares MSCI World Small-Cap ETF",
        "exchange": "NASDAQ",
    }
    alias_context = {
        "wealthsimple": {
            "entities": {"name:ishares|world", "name:wearable|devices"},
            "matching_entities": set(),
            "ticker_owner_entities": set(),
        }
    }

    assert should_drop_contextual_alias(row, "wealthsimple", alias_context) is True


def test_should_exclude_stock_row_removes_dated_generic_us_futures_only():
    from scripts.rebuild_dataset import should_exclude_stock_row

    assert should_exclude_stock_row(
        {
            "ticker": "FGBLM26",
            "name": "Euro Bund Future June 26",
            "exchange": "US",
            "asset_type": "Stock",
        }
    )
    assert not should_exclude_stock_row(
        {
            "ticker": "FUTR",
            "name": "Future plc",
            "exchange": "LSE",
            "asset_type": "Stock",
        }
    )


def test_normalize_input_row_reclassifies_exchange_traded_products():
    from scripts.rebuild_dataset import normalize_input_row

    wisdomtree = normalize_input_row(
        {
            "ticker": "AIGAP",
            "name": "WISDOMTREE AGRICULTURE",
            "exchange": "Euronext",
            "asset_type": "Stock",
        }
    )
    etn = normalize_input_row(
        {
            "ticker": "VSUI",
            "name": "VanEck Sui ETN A",
            "exchange": "Euronext",
            "asset_type": "Stock",
        }
    )
    exchange_traded_fund = normalize_input_row(
        {
            "ticker": "DMCS",
            "name": "Datvest Modified Consumer Staples Exchange Traded Fund",
            "exchange": "ZSE_ZW",
            "asset_type": "Stock",
        }
    )
    twse_etf = normalize_input_row(
        {
            "ticker": "0052",
            "name": "Fubon Taiwan Technology",
            "exchange": "TWSE",
            "asset_type": "Stock",
        }
    )
    twse_reit = normalize_input_row(
        {
            "ticker": "01002T",
            "name": "Cathay No.1 REIT",
            "exchange": "TWSE",
            "asset_type": "Stock",
        }
    )

    assert wisdomtree["asset_type"] == "ETF"
    assert etn["asset_type"] == "ETF"
    assert exchange_traded_fund["asset_type"] == "ETF"
    assert twse_etf["asset_type"] == "ETF"
    assert twse_reit["asset_type"] == "ETF"


def test_normalize_input_row_canonicalizes_clear_exchange_aliases():
    from scripts.rebuild_dataset import normalize_input_row

    assert normalize_input_row({"ticker": "ABC", "exchange": "OTCCE", "asset_type": "Stock"})["exchange"] == "OTC"
    assert normalize_input_row({"ticker": "ABC", "exchange": "OTCMKTS", "asset_type": "Stock"})["exchange"] == "OTC"
    assert (
        normalize_input_row({"ticker": "BTYB", "exchange": "NYSEARCA", "asset_type": "ETF"})["exchange"]
        == "NYSE ARCA"
    )
    assert normalize_input_row({"ticker": "ABC", "exchange": "US", "asset_type": "Stock"})["exchange"] == "US"
    assert normalize_input_row({"ticker": "ABC", "exchange": "NMFQS", "asset_type": "ETF"})["exchange"] == "NMFQS"


def test_normalize_input_row_reclassifies_generic_us_fund_wrappers_to_etf():
    from scripts.rebuild_dataset import normalize_input_row

    row = normalize_input_row(
        {
            "ticker": "AINT",
            "name": "Tidal Trust I",
            "exchange": "US",
            "asset_type": "Stock",
        }
    )

    assert row["asset_type"] == "ETF"


def test_normalize_input_row_reclassifies_neo_cdrs_and_xdrs_to_stock():
    from scripts.rebuild_dataset import normalize_input_row

    neo_cdr = normalize_input_row(
        {
            "ticker": "NTDO",
            "name": "NINTENDO CO LTD CDR (CAD Hedged)",
            "exchange": "NEO",
            "asset_type": "ETF",
        }
    )
    neo_xdr = normalize_input_row(
        {
            "ticker": "ZSMC",
            "name": "SUPER MICRO COMPUTER (SMCI) BMO XDR (CAD HEDGED)",
            "exchange": "NEO",
            "asset_type": "ETF",
        }
    )
    regular_neo_etf = normalize_input_row(
        {
            "ticker": "BCCC-U",
            "name": "Global X Bitcoin Covered Call ETF",
            "exchange": "NEO",
            "asset_type": "ETF",
        }
    )

    assert neo_cdr["asset_type"] == "Stock"
    assert neo_xdr["asset_type"] == "Stock"
    assert regular_neo_etf["asset_type"] == "ETF"


def test_tsxv_official_point_suffixes_replace_hyphen_variants():
    acap = ticker_exchange_row("ACAP.P", "TSXV")
    azc = ticker_exchange_row("AZC.P", "TSXV")

    assert ticker_exchange_row("ACAP-P", "TSXV") is None
    assert ticker_exchange_row("AZC-P", "TSXV") is None
    assert acap is not None
    assert azc is not None
    assert acap["name"] == "Atlas One Capital Corporation"
    assert azc["name"] == "A2ZCryptocap Inc."
    assert "atlas one capital" in acap["aliases"]


def test_should_exclude_stock_row_drops_ams_certificates():
    from scripts.rebuild_dataset import should_exclude_stock_row

    row = {
        "ticker": "RABO",
        "name": "Cooperatieve Rabobank U.A. PARTICIPATED CERT(RABOBANK ORD)EUR25",
        "exchange": "AMS",
        "asset_type": "Stock",
    }

    assert should_exclude_stock_row(row) is True


def test_should_exclude_stock_row_drops_twse_non_common_b_lines():
    from scripts.rebuild_dataset import should_exclude_stock_row

    row = {
        "ticker": "2883B",
        "name": "CHINA DEVELOPMENT FINANCIAL HOLDIN",
        "exchange": "TWSE",
        "asset_type": "Stock",
    }

    assert should_exclude_stock_row(row) is True


def test_should_exclude_stock_row_drops_krx_secondary_lines_with_matching_base():
    from scripts.rebuild_dataset import should_exclude_stock_row

    row = {
        "ticker": "005945",
        "name": "Nh Investment And Securities 1p",
        "exchange": "KRX",
        "asset_type": "Stock",
    }
    stock_name_lookup = {
        "005940": {"NH Investment & Securities Co Ltd"},
    }

    assert should_exclude_stock_row(row, stock_name_lookup=stock_name_lookup) is True


def test_should_exclude_stock_row_keeps_krx_secondary_lines_without_matching_base():
    from scripts.rebuild_dataset import should_exclude_stock_row

    row = {
        "ticker": "005945",
        "name": "Nh Investment And Securities 1p",
        "exchange": "KRX",
        "asset_type": "Stock",
    }
    stock_name_lookup = {
        "005940": {"Completely Different Issuer"},
    }

    assert should_exclude_stock_row(row, stock_name_lookup=stock_name_lookup) is False


def test_should_exclude_stock_row_drops_psx_government_securities_and_rights():
    from scripts.rebuild_dataset import should_exclude_stock_row

    gis_row = {
        "ticker": "P01GIS0802",
        "exchange": "PSX",
        "asset_type": "Stock",
        "name": "1 Year GIS",
    }
    rights_row = {
        "ticker": "SHDTR1",
        "exchange": "PSX",
        "asset_type": "Stock",
        "name": "Shadab Textile(R)",
    }

    assert should_exclude_stock_row(gis_row) is True
    assert should_exclude_stock_row(rights_row) is True


def test_should_exclude_stock_row_drops_krx_placeholder_and_structured_product_stocks():
    from scripts.rebuild_dataset import should_exclude_stock_row

    placeholder_row = {
        "ticker": "238170",
        "exchange": "KRX",
        "asset_type": "Stock",
        "name": "238170",
        "isin": "",
        "sector": "",
        "aliases": "238170",
    }
    structured_row = {
        "ticker": "570019",
        "exchange": "KRX",
        "asset_type": "Stock",
        "name": "TRUE KOSPI Short Strangle 5% OT",
        "isin": "",
        "sector": "",
        "aliases": "",
    }

    assert should_exclude_stock_row(placeholder_row) is True
    assert should_exclude_stock_row(structured_row) is True


def test_should_exclude_stock_row_drops_tmx_warrant_suffix_lines():
    from scripts.rebuild_dataset import should_exclude_stock_row

    row = {
        "ticker": "ODV-WTV",
        "exchange": "TSXV",
        "asset_type": "Stock",
        "name": "ODV-WTV",
    }

    assert should_exclude_stock_row(row) is True


def test_should_exclude_row_drops_set_index_rows():
    from scripts.rebuild_dataset import should_exclude_row

    row = {
        "ticker": "^SET",
        "exchange": "SET",
        "asset_type": "Stock",
        "name": "SET Index",
    }

    assert should_exclude_row(row) is True


def test_should_exclude_row_drops_official_set_dr_tickers(monkeypatch):
    from scripts import rebuild_dataset

    row = {
        "ticker": "AMD80",
        "exchange": "SET",
        "asset_type": "Stock",
        "name": "Advanced Micro Devices, Inc.",
    }

    monkeypatch.setattr(rebuild_dataset, "load_active_official_set_dr_tickers", lambda: frozenset({"AMD80"}))

    assert rebuild_dataset.should_exclude_row(row) is True


def test_should_exclude_row_drops_set_r_lines_with_matching_official_base(monkeypatch):
    from scripts import rebuild_dataset

    row = {
        "ticker": "CPN-R",
        "exchange": "SET",
        "asset_type": "Stock",
        "name": "Central Pattana Public Company Limited",
    }

    monkeypatch.setattr(
        rebuild_dataset,
        "load_active_official_set_stock_reference_names",
        lambda: {"CPN": "CENTRAL PATTANA PUBLIC COMPANY LIMITED"},
    )

    assert rebuild_dataset.should_exclude_row(row) is True


def test_should_exclude_row_drops_sse_reit_stock_namespace():
    from scripts.rebuild_dataset import should_exclude_row

    row = {
        "ticker": "508018",
        "exchange": "SSE",
        "asset_type": "Stock",
        "name": "Huaxia Fund Management Co Ltd- China Communications Construction REIT",
    }

    assert should_exclude_row(row) is True


def test_should_exclude_row_drops_szse_reit_stock_namespace():
    from scripts.rebuild_dataset import should_exclude_row

    row = {
        "ticker": "180202",
        "exchange": "SZSE",
        "asset_type": "Stock",
        "name": "China Asset Management Co. Ltd. - Yuexiu Highway REIT Fund",
    }

    assert should_exclude_row(row) is True


def test_should_exclude_row_drops_china_lof_etf_lines():
    from scripts.rebuild_dataset import should_exclude_row

    row = {
        "ticker": "160416",
        "exchange": "SZSE",
        "asset_type": "ETF",
        "name": "HuaAn S&P Global Oil Index LOF QDII",
    }

    assert should_exclude_row(row) is True


def test_should_exclude_row_drops_preferred_suffix_even_when_misclassified_as_etf():
    from scripts.rebuild_dataset import should_exclude_row

    row = {
        "ticker": "BN-PFD",
        "exchange": "TSX",
        "asset_type": "ETF",
        "name": "PUTNAM SUSTAINABLE LEADERS ETF",
    }

    assert should_exclude_row(row) is True


def test_alias_matches_company_accepts_trusted_non_lexical_renames():
    from scripts.rebuild_dataset import alias_matches_company

    assert alias_matches_company("Sena J Property PCL", "SEN X PUBLIC COMPANY LIMITED") is True
    assert alias_matches_company("Daetwyl I", "Dätwyler Holding AG") is True
    assert alias_matches_company("CONTRALADORA AXEL SAB", "CONTROLADORA AXTEL, S.A.B. DE C.V.") is True


def test_alias_matches_company_handles_xetra_diacritics_and_compact_labels():
    from scripts.rebuild_dataset import alias_matches_company

    assert alias_matches_company("L'Oréal S.A.", "L OREAL INH. EO 0,2") is True
    assert alias_matches_company("Telefónica S.A", "TELEFONICA INH. EO 1") is True
    assert alias_matches_company("Industria de Diseño Textil, S.A.", "INDITEX INH. EO 0,03") is True
    assert alias_matches_company("Sartorius Stedim Biotech S.A.", "SARTOR.STED.B. EO-,20") is True
    assert alias_matches_company("T. Rowe Price Group Inc", "T.ROW.PR.GRP DL-,20") is True
    assert alias_matches_company("Raytheon Technologies Corp", "RTX CORP. -,01") is True
    assert alias_matches_company("Deutsche Grundstücksauktionen AG", "DT.GRUNDST.AUKT.AG") is True


def test_apply_official_exchange_corrections_moves_krx_stock_to_kosdaq(monkeypatch):
    from scripts import rebuild_dataset

    row = {
        "ticker": "042000",
        "name": "Cafe24 Corp.",
        "exchange": "KRX",
        "asset_type": "Stock",
        "country": "South Korea",
        "country_code": "KR",
        "sector": "",
        "isin": "KR7042000000",
        "aliases": [],
    }

    monkeypatch.setattr(
        rebuild_dataset,
        "load_active_official_reference_rows",
        lambda: {
            ("042000", "Stock"): (
                {
                    "ticker": "042000",
                    "exchange": "KOSDAQ",
                    "asset_type": "Stock",
                    "name": "Cafe24 Corp.",
                    "source_key": "krx_listed_companies",
                    "reference_scope": "listed_companies_subset",
                },
            )
        },
    )

    corrected = rebuild_dataset.apply_official_exchange_corrections([row])

    assert corrected[0]["exchange"] == "KOSDAQ"
    assert corrected[0]["name"] == "Cafe24 Corp."


def test_apply_official_exchange_corrections_backfills_code_like_name_from_same_exchange(monkeypatch):
    from scripts import rebuild_dataset

    row = {
        "ticker": "AGIG",
        "name": "AGIG",
        "exchange": "NYSE MKT",
        "asset_type": "Stock",
        "country": "United States",
        "country_code": "US",
        "sector": "",
        "isin": "",
        "aliases": [],
    }

    monkeypatch.setattr(
        rebuild_dataset,
        "load_active_official_reference_rows",
        lambda: {
            ("AGIG", "Stock"): (
                {
                    "ticker": "AGIG",
                    "exchange": "NYSE MKT",
                    "asset_type": "Stock",
                    "name": "Abundia Global Impact Group Inc. Common stock",
                    "source_key": "nasdaq_other_listed",
                    "reference_scope": "listed_companies_subset",
                },
            )
        },
    )

    corrected = rebuild_dataset.apply_official_exchange_corrections([row])

    assert corrected[0]["exchange"] == "NYSE MKT"
    assert corrected[0]["name"] == "Abundia Global Impact Group Inc. Common stock"


def test_apply_official_exchange_corrections_keeps_non_placeholder_same_exchange_name(monkeypatch):
    from scripts import rebuild_dataset

    row = {
        "ticker": "ARGH",
        "name": "Steer Technologies Inc.",
        "exchange": "TSXV",
        "asset_type": "Stock",
        "country": "Canada",
        "country_code": "CA",
        "sector": "",
        "isin": "",
        "aliases": [],
    }

    monkeypatch.setattr(
        rebuild_dataset,
        "load_active_official_reference_rows",
        lambda: {
            ("ARGH", "Stock"): (
                {
                    "ticker": "ARGH",
                    "exchange": "TSXV",
                    "asset_type": "Stock",
                    "name": "Argo Corporation",
                    "source_key": "tmx_listed_issuers",
                    "reference_scope": "listed_companies_subset",
                },
            )
        },
    )

    corrected = rebuild_dataset.apply_official_exchange_corrections([row])

    assert corrected[0]["name"] == "Steer Technologies Inc."


def test_apply_official_exchange_corrections_moves_six_etf_to_euronext_on_exact_official_match(monkeypatch):
    from scripts import rebuild_dataset

    row = {
        "ticker": "CSMIB",
        "name": "iShares VII PLC - iShares FTSE MIB ETF EUR Acc",
        "exchange": "SIX",
        "asset_type": "ETF",
        "country": "Ireland",
        "country_code": "IE",
        "sector": "",
        "isin": "IE00B53L4X51",
        "aliases": [],
    }

    monkeypatch.setattr(
        rebuild_dataset,
        "load_active_official_reference_rows",
        lambda: {
            ("CSMIB", "ETF"): (
                {
                    "ticker": "CSMIB",
                    "exchange": "Euronext",
                    "asset_type": "ETF",
                    "name": "ISHARES FTSE MIB UCITS ETF EUR ACC",
                    "source_key": "euronext_etfs",
                    "reference_scope": "exchange_directory",
                },
            )
        },
    )

    corrected = rebuild_dataset.apply_official_exchange_corrections([row])

    assert corrected[0]["exchange"] == "Euronext"


def test_apply_official_exchange_corrections_moves_tsx_stock_to_tsxv_on_official_tmx_match(monkeypatch):
    from scripts import rebuild_dataset

    row = {
        "ticker": "AGMR",
        "name": "Silver Mountain Resources Inc.",
        "exchange": "TSX",
        "asset_type": "Stock",
        "country": "Canada",
        "country_code": "CA",
        "sector": "",
        "isin": "",
        "aliases": [],
    }

    monkeypatch.setattr(
        rebuild_dataset,
        "load_active_official_reference_rows",
        lambda: {
            ("AGMR", "Stock"): (
                {
                    "ticker": "AGMR",
                    "exchange": "TSXV",
                    "asset_type": "Stock",
                    "name": "Silver Mountain Resources Inc.",
                    "source_key": "tmx_listed_issuers",
                    "reference_scope": "listed_companies_subset",
                },
            )
        },
    )

    corrected = rebuild_dataset.apply_official_exchange_corrections([row])

    assert corrected[0]["exchange"] == "TSXV"


def test_apply_official_exchange_corrections_moves_hose_stock_without_isin_to_hnx(monkeypatch):
    from scripts import rebuild_dataset

    row = {
        "ticker": "DVM",
        "name": "Vietnam Medicinal Materials JSC",
        "exchange": "HOSE",
        "asset_type": "Stock",
        "country": "Vietnam",
        "country_code": "VN",
        "sector": "",
        "isin": "",
        "aliases": [],
    }

    monkeypatch.setattr(
        rebuild_dataset,
        "load_active_official_reference_rows",
        lambda: {
            ("DVM", "Stock"): (
                {
                    "ticker": "DVM",
                    "exchange": "HNX",
                    "asset_type": "Stock",
                    "name": "Cong ty co phan Duoc lieu Viet Nam",
                    "source_key": "hnx_listed_securities",
                    "reference_scope": "exchange_directory",
                },
            )
        },
    )

    corrected = rebuild_dataset.apply_official_exchange_corrections([row])

    assert corrected[0]["exchange"] == "HNX"


def test_apply_official_exchange_corrections_moves_hose_stock_to_hnx_on_exact_isin(monkeypatch):
    from scripts import rebuild_dataset

    row = {
        "ticker": "BKC",
        "name": "BacKan Mineral Joint Stock Corp",
        "exchange": "HOSE",
        "asset_type": "Stock",
        "country": "Vietnam",
        "country_code": "VN",
        "sector": "",
        "isin": "VN000000BKC7",
        "aliases": [],
    }

    monkeypatch.setattr(
        rebuild_dataset,
        "load_active_official_reference_rows",
        lambda: {
            ("BKC", "Stock"): (
                {
                    "ticker": "BKC",
                    "exchange": "HNX",
                    "asset_type": "Stock",
                    "name": "Công ty cổ phần Khoáng sản Bắc Kạn",
                    "source_key": "hnx_listed_securities",
                    "reference_scope": "exchange_directory",
                    "isin": "VN000000BKC7",
                },
            )
        },
    )

    corrected = rebuild_dataset.apply_official_exchange_corrections([row])

    assert corrected[0]["exchange"] == "HNX"
    assert corrected[0]["isin"] == "VN000000BKC7"


def test_apply_official_exchange_corrections_prefers_hnx_exact_isin_over_foreign_candidate(monkeypatch):
    from scripts import rebuild_dataset

    row = {
        "ticker": "KDM",
        "name": "New Residential Urban Development Holdings Corp",
        "exchange": "HOSE",
        "asset_type": "Stock",
        "country": "Vietnam",
        "country_code": "VN",
        "sector": "",
        "isin": "VN000000KDM2",
        "aliases": [],
    }

    monkeypatch.setattr(
        rebuild_dataset,
        "load_active_official_reference_rows",
        lambda: {
            ("KDM", "Stock"): (
                {
                    "ticker": "KDM",
                    "exchange": "WSE",
                    "asset_type": "Stock",
                    "name": "KDM SHIPPING PUBLIC LIMITED",
                    "source_key": "wse_listed_companies",
                    "reference_scope": "exchange_directory",
                    "isin": "CY0102492119",
                },
                {
                    "ticker": "KDM",
                    "exchange": "HNX",
                    "asset_type": "Stock",
                    "name": "Công ty Cổ Phần Tập đoàn GCL",
                    "source_key": "hnx_listed_securities",
                    "reference_scope": "exchange_directory",
                    "isin": "VN000000KDM2",
                },
            )
        },
    )

    corrected = rebuild_dataset.apply_official_exchange_corrections([row])

    assert corrected[0]["exchange"] == "HNX"
    assert corrected[0]["isin"] == "VN000000KDM2"


def test_apply_official_exchange_corrections_moves_generic_us_row_to_preferred_official_us_venue(monkeypatch):
    from scripts import rebuild_dataset

    row = {
        "ticker": "TII",
        "name": "Titan Mining Corporation",
        "exchange": "US",
        "asset_type": "Stock",
        "country": "United States",
        "country_code": "US",
        "sector": "Materials",
        "isin": "",
        "aliases": [],
    }

    monkeypatch.setattr(
        rebuild_dataset,
        "load_active_official_reference_rows",
        lambda: {
            ("TII", "Stock"): (
                {
                    "ticker": "TII",
                    "exchange": "NYSE",
                    "asset_type": "Stock",
                    "name": "Titan Mining Corp",
                    "source_key": "sec_company_tickers_exchange",
                    "reference_scope": "exchange_directory",
                    "isin": "",
                },
                {
                    "ticker": "TII",
                    "exchange": "NYSE MKT",
                    "asset_type": "Stock",
                    "name": "Titan Mining Corporation Common Shares",
                    "source_key": "nasdaq_other_listed",
                    "reference_scope": "exchange_directory",
                    "isin": "",
                },
            )
        },
    )

    corrected = rebuild_dataset.apply_official_exchange_corrections([row])

    assert corrected[0]["exchange"] == "NYSE MKT"


def test_apply_official_exchange_corrections_replaces_contaminated_hose_isin_from_hnx(monkeypatch):
    from scripts import rebuild_dataset

    row = {
        "ticker": "GLT",
        "name": "Global Electrical Technology Corp",
        "exchange": "HOSE",
        "asset_type": "Stock",
        "country": "Vietnam",
        "country_code": "VN",
        "sector": "",
        "isin": "US3773201062",
        "aliases": [],
    }

    monkeypatch.setattr(
        rebuild_dataset,
        "load_active_official_reference_rows",
        lambda: {
            ("GLT", "Stock"): (
                {
                    "ticker": "GLT",
                    "exchange": "HNX",
                    "asset_type": "Stock",
                    "name": "Công ty cổ phần Kỹ thuật Điện Toàn Cầu",
                    "source_key": "hnx_listed_securities",
                    "reference_scope": "exchange_directory",
                    "isin": "VN000000GLT8",
                },
            )
        },
    )

    corrected = rebuild_dataset.apply_official_exchange_corrections([row])

    assert corrected[0]["exchange"] == "HNX"
    assert corrected[0]["isin"] == "VN000000GLT8"
    assert corrected[0]["country"] == "Vietnam"
    assert corrected[0]["country_code"] == "VN"


def test_apply_official_exchange_corrections_prefers_unique_name_matched_exchange(monkeypatch):
    from scripts import rebuild_dataset

    row = {
        "ticker": "BNKR",
        "name": "Bunker Hill Mining Corp.",
        "exchange": "TSX",
        "asset_type": "Stock",
        "country": "Canada",
        "country_code": "CA",
        "sector": "",
        "isin": "",
        "aliases": [],
    }

    monkeypatch.setattr(
        rebuild_dataset,
        "load_active_official_reference_rows",
        lambda: {
            ("BNKR", "Stock"): (
                {
                    "ticker": "BNKR",
                    "exchange": "LSE",
                    "asset_type": "Stock",
                    "name": "BANKERS INV TST PLC ORD 2.5P",
                    "source_key": "lse_company_reports",
                    "reference_scope": "listed_companies_subset",
                },
                {
                    "ticker": "BNKR",
                    "exchange": "TSXV",
                    "asset_type": "Stock",
                    "name": "Bunker Hill Mining Corp.",
                    "source_key": "tmx_listed_issuers",
                    "reference_scope": "listed_companies_subset",
                },
            )
        },
    )

    corrected = rebuild_dataset.apply_official_exchange_corrections([row])

    assert corrected[0]["exchange"] == "TSXV"


def test_apply_official_exchange_corrections_updates_ngx_prefix_replaced_ticker(monkeypatch):
    from scripts import rebuild_dataset

    row = {
        "ticker": "FIRSTHOLDC",
        "name": "FIRST HOLDCO PLC",
        "exchange": "NGX",
        "asset_type": "Stock",
        "country": "Nigeria",
        "country_code": "NG",
        "sector": "",
        "isin": "NGFBNH000009",
        "aliases": ["first hold"],
    }

    monkeypatch.setattr(
        rebuild_dataset,
        "load_active_official_reference_rows",
        lambda: {
            ("FIRSTHOLDCO", "Stock"): (
                {
                    "ticker": "FIRSTHOLDCO",
                    "exchange": "NGX",
                    "asset_type": "Stock",
                    "name": "FIRST HOLDCO PLC",
                    "source_key": "ngx_company_profile_directory",
                    "reference_scope": "exchange_directory",
                },
            )
        },
    )

    corrected = rebuild_dataset.apply_official_exchange_corrections([row])

    assert corrected[0]["ticker"] == "FIRSTHOLDCO"
    assert corrected[0]["exchange"] == "NGX"
    assert corrected[0]["aliases"] == "first hold|FIRSTHOLDC"


def test_should_not_correct_krx_stock_without_krx_official_source():
    from scripts.rebuild_dataset import should_correct_to_official_exchange

    row = {
        "ticker": "042000",
        "name": "Cafe24 Corp.",
        "exchange": "KRX",
        "asset_type": "Stock",
    }
    official_row = {
        "ticker": "042000",
        "exchange": "KOSDAQ",
        "asset_type": "Stock",
        "name": "Cafe24 Corp.",
        "source_key": "manual_seed",
    }

    assert should_correct_to_official_exchange(row, official_row) is False


def test_load_active_official_isin_fallbacks_only_keeps_unique_active_official_values(tmp_path, monkeypatch):
    from scripts import rebuild_dataset

    reference_csv = tmp_path / "reference.csv"
    with reference_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "ticker",
                "exchange",
                "asset_type",
                "official",
                "listing_status",
                "reference_scope",
                "isin",
            ],
        )
        writer.writeheader()
        writer.writerows(
            [
                {
                    "ticker": "AAA",
                    "exchange": "LSE",
                    "asset_type": "Stock",
                    "official": "true",
                    "listing_status": "active",
                    "reference_scope": "listed_companies_subset",
                    "isin": "GB0002634946",
                },
                {
                    "ticker": "AAA",
                    "exchange": "LSE",
                    "asset_type": "Stock",
                    "official": "true",
                    "listing_status": "active",
                    "reference_scope": "security_lookup_subset",
                    "isin": "GB0002634946",
                },
                {
                    "ticker": "BBB",
                    "exchange": "LSE",
                    "asset_type": "Stock",
                    "official": "true",
                    "listing_status": "active",
                    "reference_scope": "listed_companies_subset",
                    "isin": "GB0002875804",
                },
                {
                    "ticker": "BBB",
                    "exchange": "LSE",
                    "asset_type": "Stock",
                    "official": "true",
                    "listing_status": "active",
                    "reference_scope": "security_lookup_subset",
                    "isin": "GB0005405286",
                },
                {
                    "ticker": "CCC",
                    "exchange": "LSE",
                    "asset_type": "Stock",
                    "official": "false",
                    "listing_status": "active",
                    "reference_scope": "listed_companies_subset",
                    "isin": "GB00BH4HKS39",
                },
                {
                    "ticker": "DDD",
                    "exchange": "LSE",
                    "asset_type": "Stock",
                    "official": "true",
                    "listing_status": "inactive",
                    "reference_scope": "listed_companies_subset",
                    "isin": "GB00B10RZP78",
                },
            ]
        )

    monkeypatch.setattr(rebuild_dataset, "MASTERFILE_REFERENCE_CSV", reference_csv)
    rebuild_dataset.load_active_official_isin_fallbacks.cache_clear()
    try:
        fallbacks = rebuild_dataset.load_active_official_isin_fallbacks()
    finally:
        rebuild_dataset.load_active_official_isin_fallbacks.cache_clear()

    assert fallbacks == {("AAA", "LSE", "Stock"): "GB0002634946"}


def test_load_active_official_sector_fallbacks_only_keeps_unique_active_official_values(tmp_path, monkeypatch):
    from scripts import rebuild_dataset

    reference_csv = tmp_path / "reference.csv"
    with reference_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "ticker",
                "exchange",
                "asset_type",
                "official",
                "listing_status",
                "reference_scope",
                "sector",
            ],
        )
        writer.writeheader()
        writer.writerows(
            [
                {
                    "ticker": "ABBL-EQO",
                    "exchange": "BSE_BW",
                    "asset_type": "Stock",
                    "official": "true",
                    "listing_status": "active",
                    "reference_scope": "listed_companies_subset",
                    "sector": "Banking",
                },
                {
                    "ticker": "ABBL-EQO",
                    "exchange": "BSE_BW",
                    "asset_type": "Stock",
                    "official": "true",
                    "listing_status": "active",
                    "reference_scope": "security_lookup_subset",
                    "sector": "Financial Services",
                },
                {
                    "ticker": "BBB",
                    "exchange": "BSE_BW",
                    "asset_type": "Stock",
                    "official": "true",
                    "listing_status": "active",
                    "reference_scope": "listed_companies_subset",
                    "sector": "Banking",
                },
                {
                    "ticker": "BBB",
                    "exchange": "BSE_BW",
                    "asset_type": "Stock",
                    "official": "true",
                    "listing_status": "active",
                    "reference_scope": "security_lookup_subset",
                    "sector": "Mining",
                },
                {
                    "ticker": "CCC",
                    "exchange": "BSE_BW",
                    "asset_type": "Stock",
                    "official": "false",
                    "listing_status": "active",
                    "reference_scope": "listed_companies_subset",
                    "sector": "Banking",
                },
                {
                    "ticker": "DDD",
                    "exchange": "BSE_BW",
                    "asset_type": "Stock",
                    "official": "true",
                    "listing_status": "inactive",
                    "reference_scope": "listed_companies_subset",
                    "sector": "Banking",
                },
            ]
        )

    monkeypatch.setattr(rebuild_dataset, "MASTERFILE_REFERENCE_CSV", reference_csv)
    rebuild_dataset.load_active_official_sector_fallbacks.cache_clear()
    try:
        fallbacks = rebuild_dataset.load_active_official_sector_fallbacks()
    finally:
        rebuild_dataset.load_active_official_sector_fallbacks.cache_clear()

    assert fallbacks == {("ABBL-EQO", "BSE_BW", "Stock"): "Financials"}


def test_cleaned_rows_backfills_unique_official_isin(monkeypatch):
    from scripts import rebuild_dataset

    row = {
        "ticker": "46IE",
        "name": "46IE",
        "exchange": "LSE",
        "asset_type": "Stock",
        "sector": "",
        "country": "United Kingdom",
        "country_code": "GB",
        "isin": "",
        "aliases": "",
    }

    monkeypatch.setattr(
        rebuild_dataset,
        "load_data",
        lambda: ([row], {}, defaultdict(list), {}, set()),
    )
    monkeypatch.setattr(rebuild_dataset, "load_review_overrides", lambda: (defaultdict(set), defaultdict(dict), set()))
    monkeypatch.setattr(rebuild_dataset, "apply_official_exchange_corrections", lambda rows: rows)
    monkeypatch.setattr(
        rebuild_dataset,
        "load_active_official_isin_fallbacks",
        lambda: {("46IE", "LSE", "Stock"): "GB0007655250"},
    )

    cleaned, _ = rebuild_dataset.cleaned_rows()

    assert cleaned[0]["isin"] == "GB0007655250"


def test_cleaned_rows_backfills_unique_official_sector(monkeypatch):
    from scripts import rebuild_dataset

    row = {
        "ticker": "ABBL-EQO",
        "name": "ABSA BANK OF BOTSWANA LIMITED",
        "exchange": "BSE_BW",
        "asset_type": "Stock",
        "sector": "",
        "country": "Botswana",
        "country_code": "BW",
        "isin": "BW0000000025",
        "aliases": "",
    }

    monkeypatch.setattr(
        rebuild_dataset,
        "load_data",
        lambda: ([row], {}, defaultdict(list), {}, set()),
    )
    monkeypatch.setattr(rebuild_dataset, "load_review_overrides", lambda: (defaultdict(set), defaultdict(dict), set()))
    monkeypatch.setattr(rebuild_dataset, "apply_official_exchange_corrections", lambda rows: rows)
    monkeypatch.setattr(rebuild_dataset, "load_active_official_isin_fallbacks", lambda: {})
    monkeypatch.setattr(
        rebuild_dataset,
        "load_active_official_sector_fallbacks",
        lambda: {("ABBL-EQO", "BSE_BW", "Stock"): "Financials"},
    )

    cleaned, _ = rebuild_dataset.cleaned_rows()

    assert cleaned[0]["stock_sector"] == "Financials"


def test_cleaned_rows_replaces_contaminated_isin_with_exact_official_isin(monkeypatch):
    from scripts import rebuild_dataset

    row = {
        "ticker": "ACS",
        "name": "ACS Actividades de Construccion y Servicios SA",
        "exchange": "BME",
        "asset_type": "Stock",
        "sector": "",
        "country": "Australia",
        "country_code": "AU",
        "isin": "AU000000ACS1",
        "aliases": "",
    }

    monkeypatch.setattr(
        rebuild_dataset,
        "load_data",
        lambda: ([row], {}, defaultdict(list), {}, set()),
    )
    monkeypatch.setattr(rebuild_dataset, "load_review_overrides", lambda: (defaultdict(set), defaultdict(dict), set()))
    monkeypatch.setattr(rebuild_dataset, "apply_official_exchange_corrections", lambda rows: rows)
    monkeypatch.setattr(
        rebuild_dataset,
        "load_active_official_isin_fallbacks",
        lambda: {("ACS", "BME", "Stock"): "ES0167050915"},
    )

    cleaned, _ = rebuild_dataset.cleaned_rows()

    assert cleaned[0]["isin"] == "ES0167050915"
    assert cleaned[0]["country"] == "Spain"
    assert cleaned[0]["country_code"] == "ES"


def test_cleaned_rows_syncs_country_for_review_isin_override(monkeypatch):
    from scripts import rebuild_dataset

    row = {
        "ticker": "ALQ",
        "name": "Alquiber Quality SA",
        "exchange": "BME",
        "asset_type": "Stock",
        "sector": "",
        "country": "Australia",
        "country_code": "AU",
        "isin": "AU000000ALQ6",
        "aliases": "",
    }
    metadata_updates = defaultdict(dict)
    metadata_updates[("ALQ", "BME")]["isin"] = {
        "decision": "update",
        "proposed_value": "ES0105366001",
    }

    monkeypatch.setattr(
        rebuild_dataset,
        "load_data",
        lambda: ([row], {}, defaultdict(list), {}, set()),
    )
    monkeypatch.setattr(rebuild_dataset, "load_review_overrides", lambda: (defaultdict(set), metadata_updates, set()))
    monkeypatch.setattr(rebuild_dataset, "apply_official_exchange_corrections", lambda rows: rows)
    monkeypatch.setattr(rebuild_dataset, "load_active_official_isin_fallbacks", lambda: {})

    cleaned, _ = rebuild_dataset.cleaned_rows()

    assert cleaned[0]["isin"] == "ES0105366001"
    assert cleaned[0]["country"] == "Spain"
    assert cleaned[0]["country_code"] == "ES"


def test_cleaned_rows_uses_isin_country_for_foreign_listing(monkeypatch):
    from scripts import rebuild_dataset

    row = {
        "ticker": "1AST",
        "name": "Austrian Stock Example AG",
        "exchange": "XETRA",
        "asset_type": "Stock",
        "sector": "",
        "country": "Germany",
        "country_code": "DE",
        "isin": "AT100ASTA001",
        "aliases": "",
    }

    monkeypatch.setattr(
        rebuild_dataset,
        "load_data",
        lambda: ([row], {}, defaultdict(list), {}, set()),
    )
    monkeypatch.setattr(rebuild_dataset, "load_review_overrides", lambda: (defaultdict(set), defaultdict(dict), set()))
    monkeypatch.setattr(rebuild_dataset, "apply_official_exchange_corrections", lambda rows: rows)
    monkeypatch.setattr(rebuild_dataset, "load_active_official_isin_fallbacks", lambda: {})

    cleaned, _ = rebuild_dataset.cleaned_rows()

    assert cleaned[0]["isin"] == "AT100ASTA001"
    assert cleaned[0]["country"] == "Austria"
    assert cleaned[0]["country_code"] == "AT"


def test_build_unique_name_isin_fallbacks_only_keeps_unique_non_otc_names():
    from scripts.rebuild_dataset import build_unique_name_isin_fallbacks

    rows = [
        {
            "ticker": "AEV",
            "name": "Aboitiz Equity Ventures Inc",
            "exchange": "PSE",
            "asset_type": "Stock",
            "isin": "PHY0001Z1040",
        },
        {
            "ticker": "ABOIF",
            "name": "Aboitiz Equity Ventures Inc",
            "exchange": "OTC",
            "asset_type": "Stock",
            "isin": "",
        },
        {
            "ticker": "DUP1",
            "name": "Duplicate Name Fund",
            "exchange": "SIX",
            "asset_type": "ETF",
            "isin": "IE0000000001",
        },
        {
            "ticker": "DUP2",
            "name": "Duplicate Name Fund",
            "exchange": "XETRA",
            "asset_type": "ETF",
            "isin": "IE0000000002",
        },
    ]

    fallbacks = build_unique_name_isin_fallbacks(rows)

    assert fallbacks[("aboitizequityventuresinc", "Stock")] == "PHY0001Z1040"
    assert ("duplicatenamefund", "ETF") not in fallbacks


def test_cleaned_rows_backfills_unique_name_isin_for_otc_and_updates_country(monkeypatch):
    from scripts import rebuild_dataset

    primary = {
        "ticker": "AEV",
        "name": "Aboitiz Equity Ventures Inc",
        "exchange": "PSE",
        "asset_type": "Stock",
        "sector": "",
        "country": "Philippines",
        "country_code": "PH",
        "isin": "PHY0001Z1040",
        "aliases": "",
    }
    otc = {
        "ticker": "ABOIF",
        "name": "Aboitiz Equity Ventures Inc",
        "exchange": "OTC",
        "asset_type": "Stock",
        "sector": "",
        "country": "United States",
        "country_code": "US",
        "isin": "",
        "aliases": "",
    }

    monkeypatch.setattr(
        rebuild_dataset,
        "load_data",
        lambda: ([primary, otc], {}, defaultdict(list), {}, set()),
    )
    monkeypatch.setattr(rebuild_dataset, "load_review_overrides", lambda: (defaultdict(set), defaultdict(dict), set()))
    monkeypatch.setattr(rebuild_dataset, "apply_official_exchange_corrections", lambda rows: rows)
    monkeypatch.setattr(rebuild_dataset, "load_active_official_isin_fallbacks", lambda: {})

    cleaned, _ = rebuild_dataset.cleaned_rows()
    otc_row = next(row for row in cleaned if row["ticker"] == "ABOIF")

    assert otc_row["isin"] == "PHY0001Z1040"
    assert otc_row["country"] == "Philippines"
    assert otc_row["country_code"] == "PH"


def test_cleaned_rows_backfills_unique_name_isin_for_otc_with_blank_country(monkeypatch):
    from scripts import rebuild_dataset

    primary = {
        "ticker": "VROY",
        "name": "Vizsla Royalties Corp.",
        "exchange": "TSXV",
        "asset_type": "Stock",
        "sector": "",
        "country": "Canada",
        "country_code": "CA",
        "isin": "CA92859L2012",
        "aliases": "",
    }
    otc = {
        "ticker": "VROYF",
        "name": "Vizsla Royalties Corp.",
        "exchange": "OTC",
        "asset_type": "Stock",
        "sector": "",
        "country": "",
        "country_code": "",
        "isin": "",
        "aliases": "",
    }

    monkeypatch.setattr(
        rebuild_dataset,
        "load_data",
        lambda: ([primary, otc], {}, defaultdict(list), {}, set()),
    )
    monkeypatch.setattr(rebuild_dataset, "load_review_overrides", lambda: (defaultdict(set), defaultdict(dict), set()))
    monkeypatch.setattr(rebuild_dataset, "apply_official_exchange_corrections", lambda rows: rows)
    monkeypatch.setattr(rebuild_dataset, "load_active_official_isin_fallbacks", lambda: {})

    cleaned, _ = rebuild_dataset.cleaned_rows()
    otc_row = next(row for row in cleaned if row["ticker"] == "VROYF")

    assert otc_row["isin"] == "CA92859L2012"
    assert otc_row["country"] == "Canada"
    assert otc_row["country_code"] == "CA"


def test_cleaned_rows_skips_ambiguous_unique_name_isin_backfill(monkeypatch):
    from scripts import rebuild_dataset

    row = {
        "ticker": "DUPX",
        "name": "Duplicate Name Fund",
        "exchange": "OTC",
        "asset_type": "ETF",
        "sector": "",
        "country": "United States",
        "country_code": "US",
        "isin": "",
        "aliases": "",
    }
    source_a = {
        "ticker": "DUP1",
        "name": "Duplicate Name Fund",
        "exchange": "SIX",
        "asset_type": "ETF",
        "sector": "",
        "country": "Ireland",
        "country_code": "IE",
        "isin": "IE0000000001",
        "aliases": "",
    }
    source_b = {
        "ticker": "DUP2",
        "name": "Duplicate Name Fund",
        "exchange": "XETRA",
        "asset_type": "ETF",
        "sector": "",
        "country": "Ireland",
        "country_code": "IE",
        "isin": "IE0000000002",
        "aliases": "",
    }

    monkeypatch.setattr(
        rebuild_dataset,
        "load_data",
        lambda: ([row, source_a, source_b], {}, defaultdict(list), {}, set()),
    )
    monkeypatch.setattr(rebuild_dataset, "load_review_overrides", lambda: (defaultdict(set), defaultdict(dict), set()))
    monkeypatch.setattr(rebuild_dataset, "apply_official_exchange_corrections", lambda rows: rows)
    monkeypatch.setattr(rebuild_dataset, "load_active_official_isin_fallbacks", lambda: {})

    cleaned, _ = rebuild_dataset.cleaned_rows()
    target = next(candidate for candidate in cleaned if candidate["ticker"] == "DUPX")

    assert target["isin"] == ""


def test_cleaned_rows_respects_manual_isin_clear_over_official_fallback(monkeypatch):
    from scripts import rebuild_dataset

    row = {
        "ticker": "46IE",
        "name": "46IE",
        "exchange": "LSE",
        "asset_type": "Stock",
        "sector": "",
        "country": "United Kingdom",
        "country_code": "GB",
        "isin": "",
        "aliases": "",
    }
    review_updates = defaultdict(dict)
    review_updates[("46IE", "LSE")] = {"isin": {"decision": "clear", "proposed_value": ""}}

    monkeypatch.setattr(
        rebuild_dataset,
        "load_data",
        lambda: ([row], {}, defaultdict(list), {}, set()),
    )
    monkeypatch.setattr(rebuild_dataset, "load_review_overrides", lambda: (defaultdict(set), review_updates, set()))
    monkeypatch.setattr(rebuild_dataset, "apply_official_exchange_corrections", lambda rows: rows)
    monkeypatch.setattr(
        rebuild_dataset,
        "load_active_official_isin_fallbacks",
        lambda: {("46IE", "LSE", "Stock"): "GB0007655250"},
    )

    cleaned, _ = rebuild_dataset.cleaned_rows()

    assert cleaned[0]["isin"] == ""


def test_artifact_counts_match():
    tickers_csv = load_csv("tickers.csv")
    aliases_csv = load_csv("aliases.csv")
    listings_csv = load_csv("listings.csv")
    listing_index_csv = load_csv("listing_index.csv")
    identifiers_extended_csv = load_csv("identifiers_extended.csv")

    compact_json = json.loads((DATA_DIR / "tickers.json").read_text())

    # Support both envelope {"_meta": ..., "tickers": [...]} and flat [...] format
    compact_tickers = compact_json["tickers"] if isinstance(compact_json, dict) else compact_json

    conn = sqlite3.connect(DATA_DIR / "tickers.db")
    try:
        db_tickers = conn.execute("SELECT COUNT(*) FROM tickers").fetchone()[0]
        db_aliases = conn.execute("SELECT COUNT(*) FROM aliases").fetchone()[0]
    finally:
        conn.close()

    assert len(tickers_csv) == len(compact_tickers) == db_tickers
    assert len(aliases_csv) == db_aliases
    assert len(listings_csv) == db_rows_for_table("listings")
    assert len(listing_index_csv) == len(listings_csv)
    assert len(identifiers_extended_csv) == len(listings_csv)


def test_sto_review_overrides_keep_current_hotel_and_remove_nosium_b():
    tickers_csv = load_csv("tickers.csv")
    by_key = {(row["ticker"], row["exchange"]): row for row in tickers_csv}

    assert ("NOSIUM-B", "STO") not in by_key

    hotel = by_key[("HOTEL", "STO")]
    assert hotel["name"] == "Hotel Fast SSE"
    assert hotel["country"] == "Sweden"
    assert hotel["country_code"] == "SE"
    assert hotel["isin"] == "SE0011415710"
    assert hotel["aliases"] == "hotel fast sse"


def test_sto_review_overrides_drop_m8g_and_int_and_enrich_ver():
    tickers_csv = load_csv("tickers.csv")
    by_key = {(row["ticker"], row["exchange"]): row for row in tickers_csv}

    assert ("M8G", "STO") not in by_key
    assert ("INT", "STO") not in by_key

    ver = by_key[("VER", "STO")]
    assert ver["name"] == "Verve Group"
    assert ver["country"] == "Sweden"
    assert ver["country_code"] == "SE"
    assert ver["isin"] == "SE0018538068"


def test_cph_review_overrides_drop_delisted_rows_and_move_cessa_to_sto():
    tickers_csv = load_csv("tickers.csv")
    by_key = {(row["ticker"], row["exchange"]): row for row in tickers_csv}

    assert ("BRAINP", "CPH") not in by_key
    assert ("LEDIBOND", "CPH") not in by_key
    assert ("CESSA", "CPH") not in by_key

    cessa = by_key[("CESSA", "STO")]
    assert cessa["name"] == "Cessatech"
    assert cessa["country"] == "Denmark"
    assert cessa["country_code"] == "DK"


def test_wse_review_overrides_fix_legacy_names_and_drop_sgr():
    tickers_csv = load_csv("tickers.csv")
    by_key = {(row["ticker"], row["exchange"]): row for row in tickers_csv}

    assert ("SGR", "WSE") not in by_key

    expected = {
        ("ABE", "WSE"): ("AB SPÓŁKA AKCYJNA", "PLAB00000019"),
        ("ALI", "WSE"): ("ALTUS SPÓŁKA AKCYJNA", "PLATTFI00018"),
        ("APL", "WSE"): ("LC SPÓŁKA AKCYJNA", "PLAMPLI00019"),
        ("FMG", "WSE"): ("WISE ENERGY SPÓŁKA AKCYJNA", "PLTHP0000011"),
        ("HUB", "WSE"): ("HUB.TECH SPÓŁKA AKCYJNA", "PLBRTZM00010"),
        ("IFA", "WSE"): ("GENERATIONIS.AI SPÓŁKA AKCYJNA", "PLINFRA00015"),
        ("IMP", "WSE"): ("IMPERIO ALTERNATYWNA SPÓŁKA INWESTYCYJNA SPÓŁKA AKCYJNA", "PLNFI0700018"),
        ("OBL", "WSE"): ("ORZEŁ BIAŁY SPÓŁKA AKCYJNA", "PLORZBL00013"),
        ("ORL", "WSE"): ("ORZEŁ SPÓŁKA AKCYJNA", "PLORZL000019"),
    }

    for key, (name, isin) in expected.items():
        row = by_key[key]
        assert row["name"] == name
        assert row["country"] == "Poland"
        assert row["country_code"] == "PL"
        assert row["isin"] == isin


def test_jse_and_bats_review_overrides_fix_etfbnd_and_drop_fgro():
    tickers_csv = load_csv("tickers.csv")
    by_key = {(row["ticker"], row["exchange"]): row for row in tickers_csv}

    assert ("FGRO", "BATS") not in by_key

    etfbnd = by_key[("ETFBND", "JSE")]
    assert etfbnd["name"] == "1nvest SA Bond ETF"

    solbe1 = by_key[("SOLBE1", "JSE")]
    assert solbe1["asset_type"] == "Stock"
    assert solbe1["name"] == "BEE - Sasol Limited"

    yylbee = by_key[("YYLBEE", "JSE")]
    assert yylbee["asset_type"] == "Stock"
    assert yylbee["name"] == "YeboYethu (RF) Ltd"

    msla = by_key[("MSLA", "TASE")]
    assert msla["asset_type"] == "Stock"

    ibitec = by_key[("IBITEC-F", "TASE")]
    assert ibitec["asset_type"] == "ETF"

    more_s8 = by_key[("MORE-S8", "TASE")]
    assert more_s8["asset_type"] == "ETF"

    listings_csv = load_csv("listings.csv")
    listings_by_key = {(row["ticker"], row["exchange"]): row for row in listings_csv}

    in_ff17 = listings_by_key[("IN-FF17", "TASE")]
    assert in_ff17["isin"] == "IE000716YHJ7"

    is_ff102 = listings_by_key[("IS-FF102", "TASE")]
    assert is_ff102["isin"] == "IE00BYXPSP02"


def test_tase_review_overrides_rename_legacy_psagot_lines_and_drop_orbi():
    listings_csv = load_csv("listings.csv")
    by_key = {(row["ticker"], row["exchange"]): row for row in listings_csv}
    listings_by_key = by_key

    assert ("ORBI", "TASE") not in by_key
    assert ("MDIN-L", "TASE") not in by_key
    assert ("PSG-F106", "TASE") not in by_key
    assert ("PSG-F65", "TASE") not in by_key
    assert ("PSTI", "TASE") not in by_key
    assert ("HRS&P-25", "TASE") not in by_key
    assert ("PSG-F13", "TASE") not in by_key
    assert ("PSG-FK1", "TASE") not in by_key
    assert ("PSG-FK4", "TASE") not in by_key

    for dropped_ticker in [
        "BMDX",
        "GAMT",
        "INSL",
        "ITYF",
        "NZHT",
        "HRS&P-48",
        "HRS&P-58",
        "KSS&P-53",
        "KSS&P-90",
    ]:
        assert (dropped_ticker, "TASE") not in by_key

    ibi_f106 = by_key[("IBI.F106", "TASE")]
    assert ibi_f106["name"] == "I.B.I. SAL (4D) MSCI AC World"
    assert ibi_f106["isin"] == "IL0011497729"

    ibi_f65 = by_key[("IBI.F65", "TASE")]
    assert ibi_f65["name"] == "I.B.I. SAL (4D) S&P 500"
    assert ibi_f65["isin"] == "IL0011481624"

    plur = by_key[("PLUR", "TASE")]
    assert plur["name"] == "PLURI"
    assert plur["isin"] == "US72942G2030"

    tickers_csv = load_csv("tickers.csv")
    primary_by_key = {(row["ticker"], row["exchange"]): row for row in tickers_csv}
    primary_plur = primary_by_key[("PLUR", "NASDAQ")]
    assert primary_plur["isin"] == "US72942G2030"
    assert primary_plur["country"] == "Israel"
    assert primary_plur["country_code"] == "IL"
    assert "A408KW" not in primary_plur["aliases"]
    assert "CA72942L1031" not in primary_plur["aliases"]

    hrl_f25 = by_key[("HRL.F25", "TASE")]
    assert hrl_f25["name"] == "Harel Sal (4D) S&P 500"
    assert hrl_f25["isin"] == "IL0011490203"

    ibi_f13 = by_key[("IBI.F13", "TASE")]
    assert ibi_f13["name"] == "I.B.I. SAL (00) Tel-Bond 40 CPI - LINKED IL"
    assert ibi_f13["isin"] == "IL0011479743"

    ibi_fk1 = by_key[("IBI.FK1", "TASE")]
    assert ibi_fk1["name"] == "I.B.I. SAL (00) Kosher Tel-Bond 60 CPI - LINKED IL"
    assert ibi_fk1["isin"] == "IL0011550766"

    ibi_fk4 = by_key[("IBI.FK4", "TASE")]
    assert ibi_fk4["name"] == "I.B.I. SAL (4A) Kosher TA-125 IL"
    assert ibi_fk4["isin"] == "IL0011553240"

    is_ff501 = listings_by_key[("IS-FF501", "TASE")]
    assert is_ff501["isin"] == "IE00B3WJKG14"

    is_ff505 = listings_by_key[("IS-FF505", "TASE")]
    assert is_ff505["isin"] == "IE00B6R52259"

    amda = listings_by_key[("AMDA", "TASE")]
    assert amda["isin"] == "IL0011689622"

    nvpt = listings_by_key[("NVPT", "TASE")]
    assert nvpt["isin"] == "IL0011419699"

    rati = listings_by_key[("RATI", "TASE")]
    assert rati["isin"] == "IL0003940157"


def test_egx_review_overrides_fill_secondary_source_isins_and_replace_placeholder_ticker():
    listings_csv = load_csv("listings.csv")
    by_key = {(row["ticker"], row["exchange"]): row for row in listings_csv}

    assert ("NULL", "EGX") not in by_key

    expected_isins = {
        "AMIA": "EGS67221C019",
        "CIRA": "EGS65541C012",
        "GBCO": "EGS673T1C012",
        "MEPA": "EGS3C4L1C015",
        "TWSA": "EGS7D231C010",
        "QNBE": "EGS60081C014",
    }
    for ticker, isin in expected_isins.items():
        row = by_key[(ticker, "EGX")]
        assert row["isin"] == isin
        assert row["country"] == "Egypt"
        assert row["country_code"] == "EG"


def test_drop_overrides_remove_stale_neo_and_placeholder_us_rows() -> None:
    tickers_csv = load_csv("tickers.csv")
    by_key = {(row["ticker"], row["exchange"]): row for row in tickers_csv}

    for key in [
        ("ATMY", "NEO"),
        ("GLAS-A", "NEO"),
        ("KUYA", "NEO"),
        ("QIMC", "NEO"),
        ("PTEST-X", "NYSE ARCA"),
        ("ZTST", "BATS"),
    ]:
        assert key not in by_key


def test_xetra_review_overrides_replace_stale_duplicate_and_legacy_tickers() -> None:
    listings_csv = load_csv("listings.csv")
    by_key = {(row["ticker"], row["exchange"]): row for row in listings_csv}

    for key in [
        ("42FB", "XETRA"),
        ("BWQ", "XETRA"),
        ("DIC", "XETRA"),
        ("EXJ", "XETRA"),
        ("HVXA", "XETRA"),
        ("JY0", "XETRA"),
        ("LSPP", "XETRA"),
        ("M5S", "XETRA"),
        ("PO1", "XETRA"),
        ("TLIK", "XETRA"),
        ("TR9", "XETRA"),
    ]:
        assert key not in by_key

    assert by_key[("B7J1", "XETRA")]["name"] == "SRV Yhtiöt Oyj"
    assert by_key[("BRNK", "XETRA")]["isin"] == "DE000A1X3XX4"
    assert by_key[("DGR", "XETRA")]["country"] == "Germany"
    assert by_key[("DGR", "XETRA")]["country_code"] == "DE"
    assert by_key[("DGR", "XETRA")]["isin"] == "DE0005533400"
    assert by_key[("XD4", "XETRA")]["name"] == "STRABAG"
    assert by_key[("VME", "XETRA")]["name"] == "Viromed Medical AG"


def db_rows_for_table(table: str) -> int:
    conn = sqlite3.connect(DATA_DIR / "tickers.db")
    try:
        return conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
    finally:
        conn.close()


def test_readme_stats_and_claims_are_current():
    readme = (ROOT / "README.md").read_text()
    tickers_csv = load_csv("tickers.csv")
    listings_csv = load_csv("listings.csv")
    aliases_csv = load_csv("aliases.csv")
    instrument_scopes_csv = load_csv("instrument_scopes.csv")
    exchange_counts = Counter(row["exchange"] for row in tickers_csv)
    instrument_scope_counts = Counter(row["instrument_scope"] for row in instrument_scopes_csv)
    instrument_scope_reason_counts = Counter(row["scope_reason"] for row in instrument_scopes_csv)

    total = len(tickers_csv)
    stocks = sum(row["asset_type"] == "Stock" for row in tickers_csv)
    etfs = sum(row["asset_type"] == "ETF" for row in tickers_csv)
    countries = len({row["country"] for row in tickers_csv if row["country"]})
    isin_count = sum(bool(row["isin"]) for row in tickers_csv)
    sector_count = sum(bool(row["stock_sector"] or row["etf_category"]) for row in tickers_csv)

    assert "| Metric | Value | Meaning |" in readme
    assert "sector" not in tickers_csv[0]
    assert "sector" not in listings_csv[0]
    assert f"| Primary tickers | {total:,} | Rows in `data/tickers.csv`; one primary row per security. |" in readme
    assert f"| Full listing rows | {len(listings_csv):,} | Rows in `data/listings.csv`;" in readme
    assert f"| Stocks | {stocks:,} | Primary ticker rows where `asset_type=Stock`. |" in readme
    assert f"| ETFs | {etfs:,} | Primary ticker rows where `asset_type=ETF`. |" in readme
    assert f"| Countries | {countries:,} | Distinct non-empty `country` values" in readme
    assert f"| Aliases | {len(aliases_csv):,} | Rows in `data/aliases.csv`;" in readme
    assert f"| ISIN coverage | {isin_count:,} ({isin_count / total * 100:.1f}%) | Primary ticker rows" in readme
    assert (
        f"| Sector/category coverage | {sector_count:,} ({sector_count / total * 100:.1f}%) | Primary ticker rows"
        in readme
    )
    assert f"| Core listing-scope rows | {instrument_scope_counts['core']:,} | Rows in `data/instrument_scopes.csv`" in readme
    assert f"| Core primary rows with ISIN | {instrument_scope_reason_counts['primary_listing']:,} | Core primary" in readme
    assert (
        f"| Core primary rows missing ISIN | {instrument_scope_reason_counts['primary_listing_missing_isin']:,} | Core primary"
        in readme
    )
    assert (
        f"| Extended listing-scope rows | {instrument_scope_counts['extended']:,} | Rows in `data/instrument_scopes.csv`"
        in readme
    )
    assert f"| NASDAQ | {exchange_counts['NASDAQ']:,} |" in readme
    assert f"| XETRA | {exchange_counts['XETRA']:,} |" in readme
    assert f"| NYSE | {exchange_counts['NYSE']:,} |" in readme
    assert f"| ASX | {exchange_counts['ASX']:,} |" in readme
    assert "SQLite tables: `tickers`, `listings`, `aliases`, `cross_listings`, and `instrument_scopes`." in readme


def test_listing_index_and_identifiers_extended_track_current_listings():
    listings_csv = load_csv("listings.csv")
    listing_index_csv = load_csv("listing_index.csv")
    identifiers_extended_csv = load_csv("identifiers_extended.csv")

    listing_keys = {row["listing_key"] for row in listings_csv}
    listing_index_keys = {row["listing_key"] for row in listing_index_csv}
    identifier_keys = {row["listing_key"] for row in identifiers_extended_csv}

    assert listing_index_keys == listing_keys
    assert identifier_keys == listing_keys


def test_release_docs_and_supporting_docs_are_current():
    contributing = (ROOT / "CONTRIBUTING.md").read_text()
    claude_prompt = (ROOT / "docs" / "claude_review_prompt.md").read_text()
    assert "Keep the dataset build and review scripts dependency-light and easy to trace" in contributing
    assert "one or more `review_queue.json` items" in claude_prompt


def test_open_source_project_files_exist_and_are_linked():
    readme = (ROOT / "README.md").read_text()

    assert (ROOT / ".github" / "workflows" / "ci.yml").exists()
    assert (ROOT / ".github" / "ISSUE_TEMPLATE" / "bug_report.md").exists()
    assert (ROOT / ".github" / "ISSUE_TEMPLATE" / "feature_request.md").exists()
    assert (ROOT / ".github" / "pull_request_template.md").exists()
    assert (ROOT / "CHANGELOG.md").exists()
    assert (DATA_DIR / "masterfiles" / "reference.csv").exists()
    assert (DATA_DIR / "history" / "latest_snapshot.csv").exists()
    assert (DATA_DIR / "identifiers_extended.csv").exists()
    assert (DATA_DIR / "listings.csv").exists()
    assert (DATA_DIR / "instrument_scopes.csv").exists()
    assert (DATA_DIR / "listing_index.csv").exists()
    assert (DATA_DIR / "reports" / "coverage_report.json").exists()
    assert (DATA_DIR / "reports" / "entry_quality.md").exists()
    assert (DATA_DIR / "reports" / "ohlcv_plausibility.md").exists()
    assert (DATA_DIR / "reports" / "masterfile_collision_report.json").exists()
    assert "identifiers_extended.csv" in readme
    assert "listings.csv" in readme
    assert "instrument_scopes.csv" in readme
    assert "listing_index.csv" in readme
    assert "coverage_report.json" in readme
    assert "entry_quality.md" in readme
    assert "ohlcv_plausibility.md" in readme
    assert "masterfile_collision_report.json" in readme

    assert "[![CI]" in readme
    assert "## Project" in readme
    assert "Releases: [GitHub Releases]" in readme
    assert "Changelog: [CHANGELOG.md]" in readme


def test_all_isins_have_valid_checksum():
    from scripts.rebuild_dataset import is_valid_isin

    rows = load_csv("tickers.csv")
    invalid = [(r["ticker"], r["isin"]) for r in rows if r["isin"] and not is_valid_isin(r["isin"])]
    assert not invalid, f"Invalid ISIN checksums: {invalid[:10]}"


def test_panama_country_code_mapping_exists():
    from scripts.rebuild_dataset import COUNTRY_TO_ISO

    assert COUNTRY_TO_ISO["Panama"] == "PA"


def test_sectors_are_normalized():
    from scripts.rebuild_dataset import SECTOR_STOCK_MAP

    rows = load_csv("tickers.csv")
    deprecated = set(SECTOR_STOCK_MAP)
    found = {
        value
        for row in rows
        for value in (row["stock_sector"], row["etf_category"])
        if value
    }
    overlap = found & deprecated
    assert not overlap, f"Deprecated sector names still present: {overlap}"
    long = [
        r
        for r in rows
        if any(value and len(value) > 50 for value in (r["stock_sector"], r["etf_category"]))
    ]
    assert not long, f"Garbage sector values found: {[r['ticker'] for r in long]}"


def test_no_short_or_ambiguous_name_aliases():
    rows = load_csv("aliases.csv")
    short_names = [r for r in rows if len(r["alias"]) <= 2 and r["alias_type"] == "name"]
    assert not short_names, f"Short name aliases found: {[(r['ticker'], r['alias']) for r in short_names[:5]]}"


def test_no_depositary_issues_in_stock_universe():
    rows = load_csv("tickers.csv")
    depositary = [
        r["ticker"]
        for r in rows
        if r["asset_type"] == "Stock"
        and ("depositary" in r["name"].lower() or " adr" in r["name"].lower())
    ]
    assert not depositary, f"Depositary issues found: {depositary[:10]}"


def test_numeric_namespace_aliases_are_removed_from_listing_aliases():
    from scripts.rebuild_dataset import clean_aliases

    row = {
        "ticker": "0050",
        "name": "Yuanta Taiwan Top 50 ETF",
        "exchange": "TWSE",
        "asset_type": "ETF",
        "country": "Taiwan",
        "isin": "TW0000050004",
    }

    _, aliases = clean_aliases(row, ["0050"], set())

    assert aliases == []


def test_b3_non_canonical_stock_lines_are_removed():
    tickers = {row["ticker"] for row in load_csv("tickers.csv") if row["exchange"] == "B3"}

    assert "AAPL34" not in tickers
    assert "XPBR31" not in tickers
    assert "ASAI3F" not in tickers
    assert "ALUP11" not in tickers


def test_asx_non_canonical_lines_are_removed_or_retyped():
    assert ticker_exchange_row("AN3PK", "ASX") is None
    assert ticker_exchange_row("AMCDD", "ASX") is None
    assert ticker_exchange_row("SGLLV", "ASX") is None
    assert ticker_exchange_row("HMND", "ASX")["asset_type"] == "ETF"
    assert ticker_exchange_row("B1SM", "ASX")["asset_type"] == "ETF"


def test_json_contains_version_metadata():
    data = json.loads((DATA_DIR / "tickers.json").read_text())
    assert isinstance(data, dict), "JSON should be an envelope object"
    assert "_meta" in data, "JSON should contain _meta key"
    meta = data["_meta"]
    assert "version" in meta
    assert "built_at" in meta
    assert "total_tickers" in meta
    assert meta["total_tickers"] == len(data["tickers"])


def test_country_codes_are_populated_for_all_named_countries():
    rows = load_csv("tickers.csv")
    missing = [r["ticker"] for r in rows if r["country"] and not r["country_code"]]
    assert not missing, f"Tickers missing country_code: {missing[:10]}"


def test_cross_listings_each_isin_has_exactly_one_primary():
    rows = load_csv("cross_listings.csv")
    from collections import Counter

    primary_counts = Counter(
        r["isin"] for r in rows if r["is_primary"] == "1"
    )
    isins = {r["isin"] for r in rows}
    missing = isins - set(primary_counts)
    assert not missing, f"ISINs without primary: {list(missing)[:5]}"
    multi = {k: v for k, v in primary_counts.items() if v > 1}
    assert not multi, f"ISINs with multiple primaries: {list(multi)[:5]}"


def test_tickers_csv_keeps_only_primary_cross_listing_rows():
    rows = load_csv("tickers.csv")
    by_isin = {}
    duplicates = []
    for row in rows:
        isin = row["isin"]
        if not isin:
            continue
        if isin in by_isin:
            duplicates.append((isin, by_isin[isin], (row["ticker"], row["exchange"])))
            continue
        by_isin[isin] = (row["ticker"], row["exchange"])

    assert not duplicates, f"Duplicate ISIN rows remain in tickers.csv: {duplicates[:5]}"

    msft = ticker_exchange_row("MSFT", "NASDAQ")
    msf = ticker_exchange_row("MSF", "XETRA")
    assert msft is not None
    assert msf is None

    cross_rows = load_csv("cross_listings.csv")
    microsoft_rows = [row for row in cross_rows if row["isin"] == "US5949181045"]
    assert {row["listing_key"] for row in microsoft_rows} == {"NASDAQ::MSFT", "XETRA::MSF"}
    assert sum(row["is_primary"] == "1" for row in microsoft_rows) == 1


def test_instrument_scopes_split_core_and_extended_listings():
    rows = load_csv("instrument_scopes.csv")
    by_key = {row["listing_key"]: row for row in rows}

    assert by_key["NASDAQ::MSFT"]["instrument_scope"] == "core"
    assert by_key["NASDAQ::MSFT"]["scope_reason"] == "primary_listing"
    assert by_key["XETRA::MSF"]["instrument_scope"] == "extended"
    assert by_key["XETRA::MSF"]["scope_reason"] == "secondary_cross_listing"
    assert by_key["XETRA::MSF"]["primary_listing_key"] == "NASDAQ::MSFT"

    otc_rows = [row for row in rows if row["exchange"] == "OTC"]
    assert otc_rows
    assert {row["instrument_scope"] for row in otc_rows} == {"extended"}
    assert {row["scope_reason"] for row in otc_rows} == {"otc_listing"}


def test_instrument_scope_rows_flag_core_primary_without_isin():
    from scripts.rebuild_dataset import build_instrument_scope_rows

    rows = [
        {"ticker": "AAA", "exchange": "NYSE", "asset_type": "Stock", "isin": ""},
        {"ticker": "BBB", "exchange": "NASDAQ", "asset_type": "Stock", "isin": "US0000000001"},
    ]

    by_key = {row["listing_key"]: row for row in build_instrument_scope_rows(rows, rows)}

    assert by_key["NYSE::AAA"]["instrument_scope"] == "core"
    assert by_key["NYSE::AAA"]["scope_reason"] == "primary_listing_missing_isin"
    assert by_key["NASDAQ::BBB"]["instrument_scope"] == "core"
    assert by_key["NASDAQ::BBB"]["scope_reason"] == "primary_listing"


def test_instrument_scope_rows_do_not_self_link_symbol_collisions():
    from scripts.rebuild_dataset import build_instrument_scope_rows

    rows = [
        {"ticker": "GLDU", "exchange": "TSX", "asset_type": "ETF", "isin": "CA08660T1003"},
        {"ticker": "GLDU", "exchange": "LSE", "asset_type": "ETF", "isin": "CH0346134395"},
    ]
    primary_rows = [rows[0]]

    by_key = {row["listing_key"]: row for row in build_instrument_scope_rows(rows, primary_rows)}

    assert by_key["TSX::GLDU"]["instrument_scope"] == "core"
    assert by_key["TSX::GLDU"]["scope_reason"] == "primary_listing"
    assert by_key["LSE::GLDU"]["instrument_scope"] == "extended"
    assert by_key["LSE::GLDU"]["scope_reason"] == "global_ticker_collision"
    assert by_key["LSE::GLDU"]["primary_listing_key"] == "LSE::GLDU"


def test_core_export_corrects_safe_official_exchange_collisions():
    nby = ticker_row("NBY")
    csci = ticker_row("CSCI")
    qipt = ticker_row("QIPT")
    phxep = ticker_row("PHXE-P")
    aprb = ticker_row("APRB")
    fesm = ticker_row("FESM")
    qval = ticker_row("QVAL")
    augt = ticker_row("AUGT")

    assert nby is not None and nby["exchange"] == "NYSE"
    assert csci is not None and csci["exchange"] == "TSX"
    assert qipt is not None and qipt["exchange"] == "TSX"
    assert phxep is not None and phxep["exchange"] == "NYSE"
    assert phxep["name"] == "Phoenix Energy One, LLC"

    assert aprb is not None and aprb["exchange"] == "BATS"
    assert fesm is not None and fesm["exchange"] == "NYSE ARCA"
    assert qval is not None and qval["exchange"] == "NASDAQ"
    assert augt is not None and augt["exchange"] == "BATS"
    assert augt["name"] == "AllianzIM U.S. Large Cap Buffer10 Aug ETF"


def test_core_export_refreshes_generic_etf_wrapper_rows_to_official_us_venues():
    wbiy = ticker_row("WBIY")
    mdaa = ticker_row("MDAA")
    akre = ticker_row("AKRE")
    ppi = ticker_row("PPI")
    dive = ticker_row("DIVE")

    assert wbiy is not None and wbiy["exchange"] == "NYSE"
    assert wbiy["name"] == "WBI Power Factor High Dividend ETF"

    assert mdaa is not None and mdaa["exchange"] == "NYSE ARCA"
    assert mdaa["name"] == "Myriad Dynamic Asset Allocation ETF"

    assert akre is not None and akre["exchange"] == "NYSE ARCA"
    assert akre["name"] == "Akre Focus ETF"

    assert ppi is not None and ppi["exchange"] == "NASDAQ"
    assert ppi["name"] == "Astoria Real Assets ETF"

    assert dive is not None and dive["exchange"] == "NYSE ARCA"
    assert dive["name"] == "Dana Concentrated Dividend ETF"


def test_cross_listings_sqlite_table_matches_csv_rows():
    csv_rows = load_csv("cross_listings.csv")

    conn = sqlite3.connect(DATA_DIR / "tickers.db")
    try:
        db_rows = conn.execute("SELECT COUNT(*) FROM cross_listings").fetchone()[0]
    finally:
        conn.close()

    assert db_rows == len(csv_rows)


def test_listings_sqlite_table_matches_csv_rows():
    csv_rows = load_csv("listings.csv")

    conn = sqlite3.connect(DATA_DIR / "tickers.db")
    try:
        db_rows = conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
    finally:
        conn.close()

    assert db_rows == len(csv_rows)


def test_instrument_scopes_sqlite_table_matches_csv_rows():
    csv_rows = load_csv("instrument_scopes.csv")

    conn = sqlite3.connect(DATA_DIR / "tickers.db")
    try:
        db_rows = conn.execute("SELECT COUNT(*) FROM instrument_scopes").fetchone()[0]
    finally:
        conn.close()

    assert db_rows == len(csv_rows)


def test_history_artifacts_include_listing_keys_and_daily_summary():
    snapshot_rows = load_csv("history/latest_snapshot.csv")
    event_rows = load_csv("history/listing_events.csv")
    status_rows = load_csv("history/listing_status_history.csv")
    daily_summary = json.loads((DATA_DIR / "history" / "daily_listing_summary.json").read_text())

    assert snapshot_rows
    assert event_rows is not None
    assert status_rows
    assert "listing_key" in snapshot_rows[0]
    if event_rows:
        assert "listing_key" in event_rows[0]
    assert "listing_key" in status_rows[0]
    assert "first_observed_at" in status_rows[0]
    assert "last_observed_at" in status_rows[0]
    assert "observed_at" in daily_summary
    assert "active_snapshot_rows" in daily_summary
    assert len(status_rows) <= len(snapshot_rows) + (len(event_rows) * 2)


def test_coverage_report_includes_freshness_sources_and_verification():
    report = json.loads((DATA_DIR / "reports" / "coverage_report.json").read_text())

    assert "freshness" in report
    assert "source_coverage" in report
    assert "verification" in report
    assert "gap_report" in report
    assert "b3_gap_breakdown" in report
    assert report["freshness"]["tickers_built_at"]
    assert isinstance(report["source_coverage"], list)


def test_freshness_timestamps_are_coherent():
    report = json.loads((DATA_DIR / "reports" / "coverage_report.json").read_text())
    freshness = report["freshness"]

    def parse(ts: str) -> datetime:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))

    masterfiles = parse(freshness["masterfiles_generated_at"])
    tickers = parse(freshness["tickers_built_at"])
    identifiers = parse(freshness["identifiers_generated_at"])
    history = parse(freshness["listing_history_observed_at"])
    verification = parse(freshness["latest_verification_generated_at"])
    stock_verification = parse(freshness["latest_stock_verification_generated_at"])
    etf_verification = parse(freshness["latest_etf_verification_generated_at"])

    assert tickers == history
    assert tickers <= identifiers
    assert verification == stock_verification
    assert masterfiles <= max(tickers, stock_verification, etf_verification)
    assert masterfiles <= max(stock_verification, etf_verification)
    assert freshness["latest_verification_run"].startswith("data/stock_verification/run-")

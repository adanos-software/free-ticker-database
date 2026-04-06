from __future__ import annotations

import csv
import json
import sqlite3
from collections import Counter
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


def test_common_word_aliases_removed():
    aliases = {(row["ticker"], row["alias"]) for row in load_csv("aliases.csv")}
    blocked = {
        ("ANPCF", "angle"),
        ("MSFT", "azure"),
        ("TSLA", "elon"),
        ("TSLA", "musk"),
        ("KO", "coke"),
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
    assert "TBC" not in ticker_row("SOBA")["aliases"]
    assert "pt itsec asia" not in ticker_row("CYBR")["aliases"]


def test_residual_alias_collisions_and_metadata_contamination_are_removed():
    gen = ticker_exchange_row("GEN", "NASDAQ")
    gen_lse = ticker_exchange_row("0AD5", "LSE")
    fg = ticker_exchange_row("FG", "NYSE")
    amg = ticker_exchange_row("AMG", "NYSE")
    cntx = ticker_exchange_row("CNTX", "NASDAQ")
    cybr = ticker_exchange_row("CYBR", "LSE")
    sea = ticker_exchange_row("SEA", "NYSE ARCA")
    soba = ticker_exchange_row("SOBA", "XETRA")
    att = ticker_exchange_row("T", "NYSE")
    dte = ticker_exchange_row("DTE", "NYSE")
    gap = ticker_exchange_row("GAP", "NYSE")
    key = ticker_exchange_row("KEY", "NYSE")
    tefn = ticker_exchange_row("TEFN", "BMV")

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
    assert cntx["isin"] == ""
    assert "pt century textile industry" not in cntx["aliases"]

    assert cybr["country"] == "Ireland"
    assert cybr["isin"] == ""
    assert "cyberark" not in cybr["aliases"]

    assert sea["country"] == "United States"
    assert sea["isin"] == "US26922B8651"
    assert sea["sector"] == ""
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

    assert tefn["country"] == "Spain"
    assert tefn["country_code"] == "ES"
    assert tefn["isin"] == ""
    assert tefn["sector"] == "Communication Services"
    assert "intelicanna" not in tefn["aliases"]


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
    assert "BAC-P-B" not in tickers
    assert "BTSGU" not in tickers
    assert "001515" not in tickers
    assert "005385" not in tickers


def test_country_examples_corrected():
    assert ticker_row("AAIGF")["country"] == "Hong Kong"
    assert ticker_row("AANNF")["country"] == "Luxembourg"
    assert ticker_row("AAVMY")["country"] == "Netherlands"
    assert ticker_row("0A00")["country"] == "Netherlands"
    assert ticker_row("04Q")["country"] == "Finland"
    assert ticker_row("A1CR34") is None


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
    assert dtref["sector"] == "Materials"


def test_yahoo_corrected_etf_outliers_are_cleaned():
    cam = ticker_exchange_row("CAM", "NYSE ARCA")
    netz = ticker_exchange_row("NETZ", "NYSE")
    tek = ticker_exchange_row("TEK", "NYSE ARCA")

    assert cam["name"] == "AB California Intermediate Municipal ETF"
    assert cam["country"] == "United States"
    assert cam["country_code"] == "US"
    assert cam["isin"] == "US00039J7726"
    assert cam["sector"] == ""

    assert netz["name"] == "TCW Transform Systems ETF"
    assert netz["country"] == "United States"
    assert netz["country_code"] == "US"
    assert netz["isin"] == ""

    assert tek["name"] == "iShares Technology Opportunities Active ETF"
    assert tek["country"] == "United States"
    assert tek["country_code"] == "US"
    assert tek["isin"] == "US09290C7728"


def test_non_otc_country_isin_mismatches_are_cleared_or_verified():
    assert ticker_exchange_row("T", "NYSE")["isin"] == "US00206R1023"
    assert ticker_exchange_row("PKO", "WSE")["isin"] == ""
    assert ticker_exchange_row("PRX", "AMS")["isin"] == ""


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
    ubs = ticker_row("UBS")
    asml_ams = ticker_exchange_row("ASML", "AMS")

    assert ticker_row("ARM") is None
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

    assert wisdomtree["asset_type"] == "ETF"
    assert etn["asset_type"] == "ETF"


def test_should_exclude_stock_row_drops_ams_certificates():
    from scripts.rebuild_dataset import should_exclude_stock_row

    row = {
        "ticker": "RABO",
        "name": "Cooperatieve Rabobank U.A. PARTICIPATED CERT(RABOBANK ORD)EUR25",
        "exchange": "AMS",
        "asset_type": "Stock",
    }

    assert should_exclude_stock_row(row) is True


def test_artifact_counts_match():
    tickers_csv = load_csv("tickers.csv")
    aliases_csv = load_csv("aliases.csv")

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


def test_readme_stats_and_claims_are_current():
    readme = (ROOT / "README.md").read_text()
    tickers_csv = load_csv("tickers.csv")
    aliases_csv = load_csv("aliases.csv")

    total = len(tickers_csv)
    stocks = sum(row["asset_type"] == "Stock" for row in tickers_csv)
    etfs = sum(row["asset_type"] == "ETF" for row in tickers_csv)
    countries = len({row["country"] for row in tickers_csv if row["country"]})
    isin_count = sum(bool(row["isin"]) for row in tickers_csv)
    sector_count = sum(bool(row["sector"]) for row in tickers_csv)

    assert f"| **Total tickers** | {total:,} |" in readme
    assert f"| Stocks | {stocks:,} |" in readme
    assert f"| ETFs | {etfs:,} |" in readme
    assert f"| Countries | {countries:,} |" in readme
    assert f"| Total aliases | {len(aliases_csv):,} |" in readme
    assert f"| ISIN coverage | {isin_count:,} ({isin_count / total * 100:.1f}%) |" in readme
    assert f"| Sector coverage | {sector_count:,} ({sector_count / total * 100:.1f}%) |" in readme
    assert "| NASDAQ | 4,795 | NASDAQ |" in readme
    assert "| XETRA | 3,017 | Deutsche Boerse |" in readme
    assert "| NYSE | 2,599 | New York Stock Exchange |" in readme
    assert "| ASX | 1,298 | Australian Securities Exchange |" in readme


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
    assert (ROOT / "CODE_OF_CONDUCT.md").exists()
    assert (ROOT / "SECURITY.md").exists()
    assert (DATA_DIR / "masterfiles" / "reference.csv").exists()
    assert (DATA_DIR / "history" / "latest_snapshot.csv").exists()
    assert (DATA_DIR / "identifiers_extended.csv").exists()
    assert (DATA_DIR / "listing_index.csv").exists()
    assert (DATA_DIR / "reports" / "coverage_report.json").exists()
    assert (DATA_DIR / "reports" / "masterfile_collision_report.json").exists()
    assert "identifiers_extended.csv" in readme
    assert "listing_index.csv" in readme
    assert "coverage_report.json" in readme
    assert "masterfile_collision_report.json" in readme

    assert "[![CI]" in readme
    assert "## Project Health" in readme
    assert "Code of Conduct: [CODE_OF_CONDUCT.md]" in readme
    assert "Security policy: [SECURITY.md]" in readme
    assert "Release notes: [GitHub Releases]" in readme


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
    found = {r["sector"] for r in rows if r["sector"]}
    overlap = found & deprecated
    assert not overlap, f"Deprecated sector names still present: {overlap}"
    long = [r for r in rows if r["sector"] and len(r["sector"]) > 50]
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


def test_numeric_namespace_aliases_bypass_strict_company_matching():
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

    assert aliases == ["0050"]


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


def test_cross_listings_sqlite_table_matches_csv_rows():
    csv_rows = load_csv("cross_listings.csv")

    conn = sqlite3.connect(DATA_DIR / "tickers.db")
    try:
        db_rows = conn.execute("SELECT COUNT(*) FROM cross_listings").fetchone()[0]
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

    assert masterfiles <= tickers
    assert tickers == history
    assert tickers <= identifiers <= verification
    assert freshness["latest_verification_run"].startswith("data/stock_verification/run-")

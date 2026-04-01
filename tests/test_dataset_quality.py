from __future__ import annotations

import csv
import json
import sqlite3
from collections import Counter
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
    assert ticker_row("A1CR34")["country"] == "Jersey"


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

    assert ticker_row("ARM") is None
    assert ticker_row("ASML") is None
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


def test_artifact_counts_match():
    tickers_csv = load_csv("tickers.csv")
    aliases_csv = load_csv("aliases.csv")

    compact_json = json.loads((DATA_DIR / "tickers.json").read_text())
    pretty_json = json.loads((DATA_DIR / "tickers.pretty.json").read_text())

    # Support both envelope {"_meta": ..., "tickers": [...]} and flat [...] format
    compact_tickers = compact_json["tickers"] if isinstance(compact_json, dict) else compact_json
    pretty_tickers = pretty_json["tickers"] if isinstance(pretty_json, dict) else pretty_json

    conn = sqlite3.connect(DATA_DIR / "tickers.db")
    try:
        db_tickers = conn.execute("SELECT COUNT(*) FROM tickers").fetchone()[0]
        db_aliases = conn.execute("SELECT COUNT(*) FROM aliases").fetchone()[0]
    finally:
        conn.close()

    assert len(tickers_csv) == len(compact_tickers) == len(pretty_tickers) == db_tickers
    assert len(aliases_csv) == db_aliases


def test_readme_stats_and_claims_are_current():
    readme = (ROOT / "README.md").read_text()
    assert "| **Total tickers** | 59,178 |" in readme
    assert "| Stocks | 43,086 |" in readme
    assert "| ETFs | 16,092 |" in readme
    assert "| Countries | 68 |" in readme
    assert "| Total aliases | 104,387 |" in readme
    assert "| ISIN coverage | 44,839 (75.8%) |" in readme
    assert "| Sector coverage | 38,900 (65.7%) |" in readme
    assert "| NASDAQ | 4,819 | NASDAQ |" in readme
    assert "| XETRA | 2,947 | Deutsche Boerse |" in readme
    assert "| NYSE | 2,618 | New York Stock Exchange |" in readme
    assert "| ASX | 1,236 | Australian Securities Exchange |" in readme


def test_changelog_and_supporting_docs_are_current():
    changelog = (ROOT / "CHANGELOG.md").read_text()
    contributing = (ROOT / "CONTRIBUTING.md").read_text()
    claude_prompt = (ROOT / "docs" / "claude_review_prompt.md").read_text()

    assert "## Unreleased" in changelog
    assert "- No unreleased changes yet." in changelog
    assert "## 2.0.0" in changelog
    assert "59,178 tickers (43,086 stocks, 16,092 ETFs) across 67 exchanges and 68 countries" in changelog
    assert "104,387 aliases" in changelog
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

    assert "[![CI]" in readme
    assert "## Project Health" in readme
    assert "Code of Conduct: [CODE_OF_CONDUCT.md]" in readme
    assert "Security policy: [SECURITY.md]" in readme


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

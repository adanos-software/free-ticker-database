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
    arm = ticker_row("ARM")
    asml = ticker_row("ASML")
    ubs = ticker_row("UBS")

    assert "arima real estate socimi" not in arm["aliases"]
    assert "lithography" not in asml["aliases"]
    assert "euv" not in asml["aliases"]
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

    conn = sqlite3.connect(DATA_DIR / "tickers.db")
    try:
        db_tickers = conn.execute("SELECT COUNT(*) FROM tickers").fetchone()[0]
        db_aliases = conn.execute("SELECT COUNT(*) FROM aliases").fetchone()[0]
    finally:
        conn.close()

    assert len(tickers_csv) == len(compact_json) == len(pretty_json) == db_tickers
    assert len(aliases_csv) == db_aliases


def test_readme_stats_and_claims_are_current():
    readme = (ROOT / "README.md").read_text()
    assert "| **Total tickers** | 60,109 |" in readme
    assert "| Stocks | 44,015 |" in readme
    assert "| ETFs | 16,094 |" in readme
    assert "| Countries | 67 |" in readme
    assert "| Total aliases | 107,074 |" in readme
    assert "| ISIN coverage | 45,773 (76.2%) |" in readme
    assert "| Sector coverage | 39,677 (66.0%) |" in readme
    assert "Zero common-word aliases" not in readme
    assert "Warrants, notes, bonds, and preferred stock debt instruments excluded" not in readme


def test_all_isins_have_valid_checksum():
    from scripts.rebuild_dataset import is_valid_isin

    rows = load_csv("tickers.csv")
    invalid = [(r["ticker"], r["isin"]) for r in rows if r["isin"] and not is_valid_isin(r["isin"])]
    assert not invalid, f"Invalid ISIN checksums: {invalid[:10]}"


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

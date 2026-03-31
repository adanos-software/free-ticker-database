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


def test_country_examples_corrected():
    assert ticker_row("AAIGF")["country"] == "Hong Kong"
    assert ticker_row("AANNF")["country"] == "Luxembourg"
    assert ticker_row("AAVMY")["country"] == "Netherlands"


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
    assert "| **Total tickers** | 60,688 |" in readme
    assert "| Total aliases | 109,039 |" in readme
    assert "| ISIN coverage | 46,310 (76.3%) |" in readme
    assert "Zero common-word aliases" not in readme
    assert "Warrants, notes, bonds, and preferred stock debt instruments excluded" not in readme

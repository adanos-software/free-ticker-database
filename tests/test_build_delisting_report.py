"""Unit tests for the weekly delisting-candidate report (pure logic, no network)."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.build_delisting_report import (
    holdings_by_exchange,
    master_absent,
    classify_bse,
    classify_nasdaq_deletes,
    compute_detected,
    build_markdown,
    nasdaq_effective_at,
    _norm,
)
from scripts.lib.delisting_evidence import parse_nasdaq_trading_system_deletes


def test_norm_strips_dots_and_hyphens():
    assert _norm("BIO.B") == "BIOB"
    assert _norm("cig-c") == "CIGC"


def test_holdings_by_exchange_keeps_only_stocks():
    rows = [
        {"exchange": "TSE", "asset_type": "Stock", "ticker": "7203"},
        {"exchange": "TSE", "asset_type": "ETF", "ticker": "1306"},
        {"exchange": "ASX", "asset_type": "Stock", "ticker": "BHP"},
    ]
    by_ex = holdings_by_exchange(rows)
    assert [r["ticker"] for r in by_ex["TSE"]] == ["7203"]  # ETF excluded
    assert by_ex["ASX"][0]["ticker"] == "BHP"


def test_master_absent_matches_by_symbol_or_isin():
    holdings = [
        {"ticker": "7203", "isin": "JP3633400001"},   # present by symbol
        {"ticker": "9999", "isin": "JP9999999999"},   # present by isin only
        {"ticker": "0000", "isin": "JP0000000000"},   # absent both -> candidate
    ]
    master_syms = {"7203"}
    master_isins = {"JP9999999999"}
    cand = master_absent(holdings, master_syms, master_isins)
    assert [c["ticker"] for c in cand] == ["0000"]


def test_master_absent_blank_isin_only_symbol_decides():
    holdings = [{"ticker": "ABC", "isin": ""}]
    assert master_absent(holdings, {"ABC"}, set()) == []        # symbol present -> kept
    assert [c["ticker"] for c in master_absent(holdings, set(), set())] == ["ABC"]  # absent -> candidate


def test_classify_bse_uses_authoritative_status():
    cand = [
        {"ticker": "KASHYAP", "isin": "INEA"},
        {"ticker": "ABAN", "isin": "INEB"},
        {"ticker": "MYSTERY", "isin": "INEC"},
    ]
    out = classify_bse(cand, delisted_isins={"INEA"}, delisted_ids=set(),
                       suspended_isins={"INEB"}, suspended_ids=set())
    by = {c["ticker"]: c["classification"] for c in out}
    assert by == {"KASHYAP": "delisted", "ABAN": "suspended", "MYSTERY": "master_absent"}


def test_compute_detected():
    keys = {("TSE", "7203"), ("ASX", "BHP")}
    assert compute_detected(keys, None) is True            # first run, has candidates
    assert compute_detected(set(), None) is False          # first run, none
    assert compute_detected(keys, keys) is False           # unchanged
    assert compute_detected(keys, {("TSE", "7203")}) is True  # new candidate appeared


def test_build_markdown_renders_summary():
    summary = {
        "generated_at": "2026-06-19T00:00:00Z",
        "markets_checked": ["US", "TSE"],
        "markets_skipped": [{"market": "BSE_IN", "reason": "fetch failed: ConnectionError"}],
        "candidates": [
            {"exchange": "BSE_IN", "ticker": "KASHYAP", "classification": "delisted",
             "name": "Kashyap Tele-Medicines Ltd", "isin": "INEA"},
        ],
        "delisting_detected": True,
    }
    md = build_markdown(summary)
    assert "delisting_detected: True" in md
    assert "BSE_IN (fetch failed: ConnectionError)" in md
    assert "KASHYAP" in md and "delisted" in md


def test_classify_nasdaq_deletes_matches_holdings_and_ignores_unlisted():
    holdings = [
        {"exchange": "NASDAQ", "ticker": "OLD", "name": "Old Inc", "isin": "US1"},
        {"exchange": "NYSE", "ticker": "KEEP", "name": "Keep Co", "isin": "US2"},
    ]
    deletes = [
        {"ticker": "OLD", "exchange": "NASDAQ", "name": "Old Inc", "nasdaq_action": "Delete", "effective_date": "20260801"},
        {"ticker": "GONE", "exchange": "NASDAQ", "name": "Gone Inc", "nasdaq_action": "Delete", "effective_date": "20260801"},
        {"ticker": "KEEP", "exchange": "NASDAQ", "name": "Wrong Venue", "nasdaq_action": "Delete", "effective_date": "20260801"},
    ]
    out = classify_nasdaq_deletes(holdings, deletes)
    assert [(row["exchange"], row["ticker"], row["classification"]) for row in out] == [
        ("NASDAQ", "OLD", "delisted")
    ]
    assert out[0]["nasdaq_action"] == "Delete"
    assert nasdaq_effective_at(out[0]["effective_date"]) == "2026-08-01T00:00:00Z"


def test_parse_nasdaq_trading_system_deletes_keeps_only_mapped_deletes():
    text = "\n".join(
        [
            "Effective Date|Symbol|Company Name|NASDAQ Action|Primary Listing Market",
            "20260801|OLD|Old Inc|Delete|Q",
            "20260801|NEW|New Inc|Add|Q",
            "20260801|ZZ|Unknown Market Inc|Delete|XX",
            "File Creation Time: 0818202617:00",
        ]
    )
    rows = parse_nasdaq_trading_system_deletes(text)
    assert [(row["ticker"], row["exchange"], row["nasdaq_action"]) for row in rows] == [
        ("OLD", "NASDAQ", "Delete")
    ]


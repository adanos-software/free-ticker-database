"""NASDAQ::KNSA stays primary after the UK redomicile shares a GB ISIN with LSE.

``cross_listing_sort_key`` prefers the home exchange for an ISIN prefix before
exchange rank, so GB00BRXB0C07 would otherwise select LSE::KNSA over
NASDAQ::KNSA. Kiniksa's primary listing is Nasdaq; the reviewed override is
what keeps that selection.
"""

from __future__ import annotations

import csv
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OVERRIDES = ROOT / "data" / "review_overrides" / "primary_listing_overrides.csv"
ISIN = "GB00BRXB0C07"
NASDAQ_LISTING_URL = "https://www.nasdaq.com/market-activity/stocks/knsa"
KINIKSA_IR_URL = "https://investors.kiniksa.com/investor-resources/investor-faqs"
HTTPS_URL = re.compile(r"https://[^\s\"']+")


def _rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _one(path: Path, **expected: str) -> dict[str, str]:
    matches = [row for row in _rows(path) if all(row.get(key) == value for key, value in expected.items())]
    assert len(matches) == 1, (path, expected, len(matches))
    return matches[0]


def _knsa_listings() -> list[dict[str, str]]:
    return [
        {
            "ticker": "KNSA",
            "exchange": "LSE",
            "name": "Kiniksa Pharmaceuticals International Plc",
            "asset_type": "Stock",
            "isin": ISIN,
        },
        {
            "ticker": "KNSA",
            "exchange": "NASDAQ",
            "name": "Kiniksa Pharmaceuticals International, plc",
            "asset_type": "Stock",
            "isin": ISIN,
        },
    ]


def test_kiniksa_primary_override_cites_two_independent_sources() -> None:
    row = _one(OVERRIDES, isin=ISIN)

    assert row["listing_key"] == "NASDAQ::KNSA"
    urls = HTTPS_URL.findall(row["reason"])
    assert NASDAQ_LISTING_URL in urls
    assert KINIKSA_IR_URL in urls
    hosts = {url.split("/")[2] for url in urls}
    assert hosts == {"www.nasdaq.com", "investors.kiniksa.com"}


def test_shared_gb_isin_selects_lse_without_the_primary_override(monkeypatch) -> None:
    from scripts import rebuild_dataset

    monkeypatch.setattr(rebuild_dataset, "load_primary_listing_overrides", lambda: {})

    assert rebuild_dataset.build_primary_listing_key_by_isin(_knsa_listings()) == {ISIN: "LSE::KNSA"}


def test_kiniksa_override_selects_nasdaq_over_the_lse_home_isin() -> None:
    from scripts.rebuild_dataset import (
        build_core_security_rows,
        build_cross_listings,
        build_instrument_scope_rows,
        build_primary_listing_key_by_isin,
        build_primary_ticker_rows,
    )

    rows = _knsa_listings()
    core = build_core_security_rows(rows)

    assert build_primary_listing_key_by_isin(rows) == {ISIN: "NASDAQ::KNSA"}
    assert [(row["ticker"], row["exchange"]) for row in build_primary_ticker_rows(rows)] == [("KNSA", "NASDAQ")]
    assert [f"{row['exchange']}::{row['ticker']}" for row in core] == ["NASDAQ::KNSA"]

    scopes = {row["listing_key"]: row for row in build_instrument_scope_rows(rows, core)}
    assert scopes["NASDAQ::KNSA"]["instrument_scope"] == "core"
    assert scopes["NASDAQ::KNSA"]["scope_reason"] == "primary_listing"
    assert scopes["NASDAQ::KNSA"]["primary_listing_key"] == "NASDAQ::KNSA"
    assert scopes["LSE::KNSA"]["instrument_scope"] == "extended"
    assert scopes["LSE::KNSA"]["scope_reason"] == "secondary_cross_listing"
    assert scopes["LSE::KNSA"]["primary_listing_key"] == "NASDAQ::KNSA"
    assert {(row["listing_key"], row["is_primary"]) for row in build_cross_listings(rows)} == {
        ("NASDAQ::KNSA", "1"),
        ("LSE::KNSA", "0"),
    }


def test_kiniksa_foreign_isin_is_reviewed_for_the_nasdaq_primary() -> None:
    row = _one(ROOT / "data" / "review_overrides" / "foreign_isin_reviewed.csv", ticker="KNSA", exchange="NASDAQ")
    assert row["isin"] == ISIN
    assert NASDAQ_LISTING_URL in row["note"]
    assert KINIKSA_IR_URL in row["note"]


def test_rebuilt_kiniksa_primary_listing_is_nasdaq() -> None:
    primary = _one(ROOT / "data" / "tickers.csv", ticker="KNSA")
    assert primary["exchange"] == "NASDAQ"
    assert primary["isin"] == ISIN
    assert primary["name"] == "Kiniksa Pharmaceuticals International, plc"
    assert primary["country_code"] == "GB"

    nasdaq = _one(ROOT / "data" / "instrument_scopes.csv", listing_key="NASDAQ::KNSA")
    lse = _one(ROOT / "data" / "instrument_scopes.csv", listing_key="LSE::KNSA")
    assert nasdaq["instrument_scope"] == "core"
    assert nasdaq["scope_reason"] == "primary_listing"
    assert nasdaq["primary_listing_key"] == "NASDAQ::KNSA"
    assert lse["instrument_scope"] == "extended"
    assert lse["scope_reason"] == "secondary_cross_listing"
    assert lse["primary_listing_key"] == "NASDAQ::KNSA"

    assert _one(ROOT / "data" / "cross_listings.csv", isin=ISIN, listing_key="NASDAQ::KNSA")["is_primary"] == "1"
    assert _one(ROOT / "data" / "cross_listings.csv", isin=ISIN, listing_key="LSE::KNSA")["is_primary"] == "0"
    assert _one(ROOT / "data" / "core_listings.csv", listing_key="NASDAQ::KNSA")["isin"] == ISIN
    assert all(row["listing_key"] != "LSE::KNSA" for row in _rows(ROOT / "data" / "core_listings.csv"))

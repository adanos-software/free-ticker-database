from __future__ import annotations

from scripts.rebuild_dataset import apply_listing_wkn_from_extended_identifier


def test_changed_listing_isin_does_not_keep_the_previous_security_wkn():
    """Power REIT's WKN A1JNA1 belongs to US73933H1014, not the current ISIN.

    A single compatibility rebuild must drop it. Leaving it for one pass and
    deleting it only after identifiers_extended is rewritten makes the next
    rebuild disagree with the committed artifacts.
    """

    row = {
        "ticker": "PW",
        "exchange": "NYSE MKT",
        "isin": "US73933H3093",
        "wkn": "A1JNA1",
        "aliases": ["power reit", "A1JNA1"],
    }

    apply_listing_wkn_from_extended_identifier(
        row,
        {"isin": "US73933H1014", "wkn": "A1JNA1"},
    )

    assert row["wkn"] == ""
    assert row["aliases"] == ["power reit"]


def test_matching_listing_isin_keeps_extended_wkn():
    row = {
        "isin": "US73933H1014",
        "wkn": "",
        "aliases": ["power reit", "A1JNA1"],
    }

    apply_listing_wkn_from_extended_identifier(
        row,
        {"isin": "US73933H1014", "wkn": "A1JNA1"},
    )

    assert row["wkn"] == "A1JNA1"
    assert row["aliases"] == ["power reit", "A1JNA1"]


def test_missing_listing_isin_does_not_drop_extended_wkn():
    row = {"isin": "", "wkn": "", "aliases": ["A1JNA1"]}

    apply_listing_wkn_from_extended_identifier(
        row,
        {"isin": "US73933H1014", "wkn": "A1JNA1"},
    )

    assert row["wkn"] == "A1JNA1"
    assert row["aliases"] == ["A1JNA1"]

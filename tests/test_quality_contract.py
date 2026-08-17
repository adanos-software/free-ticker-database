from __future__ import annotations

from scripts.build_quality_contract import valid_isin
from scripts.build_reference_reconciliation import is_out_of_scope, normalize_symbol


def test_isin_checksum_validation() -> None:
    assert valid_isin("US0378331005")
    assert valid_isin("IE00B53QG562")
    assert not valid_isin("US0378331006")
    assert not valid_isin("NOT-AN-ISIN")


def test_reference_scope_classifier() -> None:
    assert is_out_of_scope({"asset_type": "Bond", "name": "Example"})[0]
    assert is_out_of_scope({"asset_type": "Stock", "name": "Example Warrants"})[0]
    assert not is_out_of_scope({"asset_type": "Stock", "name": "Example Holdings Inc"})[0]


def test_symbol_normalization_handles_punctuation_and_leading_zeroes() -> None:
    assert normalize_symbol("0005.HK") == "5HK"
    assert normalize_symbol("BRK-B") == "BRKB"

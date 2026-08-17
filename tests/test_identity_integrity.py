from __future__ import annotations

import pytest

from scripts.lib.identity_integrity import (
    find_identity_conflicts,
    names_refer_to_same_identity,
    resolve_identity_conflicts,
)


def row(exchange: str, ticker: str, name: str, asset_type: str, isin: str) -> dict:
    return {
        "listing_key": f"{exchange}::{ticker}",
        "exchange": exchange,
        "ticker": ticker,
        "name": name,
        "asset_type": asset_type,
        "isin": isin,
        "aliases": [],
    }


def test_legitimate_cross_listing_is_not_a_conflict() -> None:
    rows = [
        row("NASDAQ", "ACME", "Acme Holdings Inc", "Stock", "US0000000002"),
        row("LSE", "0ACM", "Acme Holdings, Inc.", "Stock", "US0000000002"),
    ]
    assert find_identity_conflicts(rows) == []


def test_generic_single_token_does_not_merge_unrelated_companies() -> None:
    assert not names_refer_to_same_identity(
        "Global Power Inc", "Global Foods Inc", "Stock"
    )


def test_complete_linkage_prevents_transitive_family_chaining() -> None:
    isin = "US0000000002"
    rows = [
        row("X", "A", "Alpha Beta Holdings Inc", "Stock", isin),
        row("X", "B", "Alpha Beta Gamma Inc", "Stock", isin),
        row("X", "C", "Gamma Delta Systems Inc", "Stock", isin),
    ]
    assert find_identity_conflicts(rows)


def test_short_unknown_names_fail_closed() -> None:
    isin = "US0000000002"
    rows = [
        row("X", "AAA", "AAA", "Stock", isin),
        row("Y", "BBB", "BBB", "Stock", isin),
    ]
    cleaned, decisions = resolve_identity_conflicts(rows)
    assert {item["isin"] for item in cleaned} == {isin}
    assert {item.action for item in decisions} == {"quarantined_unresolved_identifier"}


def test_only_supported_family_retains_identifier() -> None:
    isin = "IE00B53QG562"
    rows = [
        row("XETRA", "SXR7", "iShares Core MSCI EMU UCITS ETF EUR Acc", "ETF", isin),
        row("LSE", "IEMU", "iShares Core MSCI EMU UCITS ETF EUR Acc", "ETF", isin),
        row("OTC", "IHHFF", "iShares US Aggregate Bond UCITS ETF", "ETF", isin),
    ]
    proposed, decisions = resolve_identity_conflicts(
        rows, official_isin_by_listing={"XETRA::SXR7": {isin}}
    )
    proposed_by_key = {item["listing_key"]: item for item in proposed}
    assert proposed_by_key["OTC::IHHFF"]["isin"] == isin
    assert any(item.action == "proposed_clear_conflicting_identifier" for item in decisions)

    cleaned, _ = resolve_identity_conflicts(
        rows,
        official_isin_by_listing={"XETRA::SXR7": {isin}},
        apply_resolved_clears=True,
    )
    by_key = {item["listing_key"]: item for item in cleaned}
    assert by_key["XETRA::SXR7"]["isin"] == isin
    assert by_key["LSE::IEMU"]["isin"] == isin
    assert by_key["OTC::IHHFF"]["isin"] == ""


def test_unknown_or_familyless_review_override_is_rejected() -> None:
    isin = "US0000000002"
    rows = [
        row("X", "ALPHA", "Alpha Holdings Inc", "Stock", isin),
        row("Y", "BETA", "Beta Systems Inc", "Stock", isin),
    ]
    with pytest.raises(ValueError, match="unknown listing"):
        resolve_identity_conflicts(
            rows, reviewed_keep_listing_keys={isin: {"NOPE::ZZZ"}}
        )
    rows.append(row("Z", "ZZZ", "ZZZ", "Stock", isin))
    with pytest.raises(ValueError, match="unresolved"):
        resolve_identity_conflicts(
            rows, reviewed_keep_listing_keys={isin: {"Z::ZZZ"}}
        )


def test_duplicate_listing_keys_are_rejected() -> None:
    rows = [
        row("X", "A", "Alpha Inc", "Stock", "US0000000002"),
        row("X", "A", "Beta Inc", "Stock", "US0000000002"),
    ]
    with pytest.raises(ValueError, match="duplicate listing"):
        resolve_identity_conflicts(rows)


def test_non_identity_payload_types_are_preserved() -> None:
    isin = "ARP6356B1059"
    rows = [
        row("TSX", "LIFE", "CI Global Longevity Economy Fund", "ETF", isin),
        row("BCBA", "LONG", "Longvie SA", "Stock", isin),
    ]
    rows[0]["aliases"] = ["LIFE", "CI Longevity"]
    cleaned, _ = resolve_identity_conflicts(rows)
    assert cleaned[0]["aliases"] == ["LIFE", "CI Longevity"]


def test_explicit_apply_does_not_clear_familyless_short_name() -> None:
    isin = "US0378331005"
    rows = [
        row("NASDAQ", "AAPL", "Apple Inc", "Stock", isin),
        row("OTC", "XXXX", "XXXX", "Stock", isin),
    ]
    cleaned, decisions = resolve_identity_conflicts(
        rows,
        official_isin_by_listing={"NASDAQ::AAPL": {isin}},
        apply_resolved_clears=True,
    )
    assert {item["isin"] for item in cleaned} == {isin}
    assert any(
        item.listing_key == "OTC::XXXX" and item.action == "quarantined_unresolved_identifier"
        for item in decisions
    )


def test_short_name_bridge_cannot_collapse_incompatible_rows() -> None:
    isin = "HK0144000764"
    rows = [
        row("HKEX", "00144", "CHINA MER PORT", "Stock", isin),
        row("OTC", "CMHHF", "China Merchants Port Holdings Company Limited", "Stock", isin),
        row("XSTU", "CPM", "CHINA MERCHANTS (CPM.SG)", "Stock", isin),
    ]
    conflicts = find_identity_conflicts(rows)
    assert len(conflicts) == 1
    assert conflicts[0].family_count >= 2
    cleaned, decisions = resolve_identity_conflicts(rows)
    assert {item["isin"] for item in cleaned} == {isin}
    assert {decision.action for decision in decisions} == {"quarantined_unresolved_identifier"}


def test_abbreviations_cannot_bridge_an_incoherent_same_isin_group() -> None:
    isin = "HK0144000764"
    rows = [
        row("HKEX", "00144", "CHINA MER PORT", "Stock", isin),
        row("OTC", "CMHHF", "China Merchants Port Holdings Company Limited", "Stock", isin),
        row("FSX", "CPM", "China Merchants Port Holdings Company Limited", "Stock", isin),
        row("XSTU", "CPM", "CHINA MERCHANTS (CPM.SG)", "Stock", isin),
    ]
    conflicts = find_identity_conflicts(rows)
    assert len(conflicts) == 1
    cleaned, decisions = resolve_identity_conflicts(
        rows, official_isin_by_listing={"OTC::CMHHF": {isin}}
    )
    assert {item["isin"] for item in cleaned} == {isin}
    by_key = {item.listing_key: item for item in decisions}
    assert by_key["XSTU::CPM"].action == "quarantined_unresolved_identifier"


def test_apply_mode_never_clears_short_abbreviation_family() -> None:
    isin = "HK0144000764"
    rows = [
        row("OTC", "CMHHF", "China Merchants Port Holdings Company Limited", "Stock", isin),
        row("XSTU", "CPM", "CHINA MERCHANTS (CPM.SG)", "Stock", isin),
        row("HKEX", "00144", "CHINA MER PORT", "Stock", isin),
    ]
    cleaned, decisions = resolve_identity_conflicts(
        rows,
        official_isin_by_listing={"OTC::CMHHF": {isin}},
        apply_resolved_clears=True,
    )
    assert {item["isin"] for item in cleaned} == {isin}
    assert any(
        item.listing_key in {"XSTU::CPM", "HKEX::00144"}
        and item.action == "quarantined_unresolved_identifier"
        for item in decisions
    )

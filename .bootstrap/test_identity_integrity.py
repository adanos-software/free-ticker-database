from __future__ import annotations

from scripts.lib.identity_integrity import find_identity_conflicts, resolve_identity_conflicts


def row(exchange: str, ticker: str, name: str, asset_type: str, isin: str) -> dict[str, str]:
    return {
        "listing_key": f"{exchange}::{ticker}",
        "exchange": exchange,
        "ticker": ticker,
        "name": name,
        "asset_type": asset_type,
        "isin": isin,
        "country": "",
        "country_code": "",
        "aliases": "",
    }


def test_legitimate_cross_listings_are_not_conflicts() -> None:
    rows = [
        row("NASDAQ", "ACME", "Acme Holdings Inc", "Stock", "US0000000002"),
        row("LSE", "0ACM", "Acme Holdings, Inc.", "Stock", "US0000000002"),
    ]
    assert find_identity_conflicts(rows) == []


def test_cross_asset_identifier_is_quarantined_without_decisive_evidence() -> None:
    rows = [
        row("TSX", "LIFE", "CI Global Longevity Economy Fund", "ETF", "ARP6356B1059"),
        row("BCBA", "LONG", "Longvie SA", "Stock", "ARP6356B1059"),
    ]
    cleaned, decisions = resolve_identity_conflicts(rows)
    assert {item["isin"] for item in cleaned} == {""}
    assert {decision.action for decision in decisions} == {"cleared_untrusted_identifier"}


def test_only_officially_supported_identity_family_keeps_identifier() -> None:
    isin = "IE00B53QG562"
    rows = [
        row("XETRA", "SXR7", "iShares Core MSCI EMU UCITS ETF EUR Acc", "ETF", isin),
        row("LSE", "IEMU", "iShares Core MSCI EMU UCITS ETF EUR Acc", "ETF", isin),
        row("OTC", "IHHFF", "iShares US Aggregate Bond UCITS ETF", "ETF", isin),
    ]
    official = {"XETRA::SXR7": {isin}}
    cleaned, decisions = resolve_identity_conflicts(rows, official_isin_by_listing=official)
    by_key = {item["listing_key"]: item for item in cleaned}
    assert by_key["XETRA::SXR7"]["isin"] == isin
    assert by_key["LSE::IEMU"]["isin"] == isin
    assert by_key["OTC::IHHFF"]["isin"] == ""
    assert any(decision.action == "kept_listing_keyed_identifier" for decision in decisions)
    assert not find_identity_conflicts(cleaned)


def test_provider_name_does_not_merge_unrelated_funds() -> None:
    isin = "IE00B53QG562"
    rows = [
        row("OTC", "A", "iShares Core S&P 500 UCITS ETF", "ETF", isin),
        row("OTC", "B", "iShares MSCI China A UCITS ETF", "ETF", isin),
        row("OTC", "C", "iShares US Aggregate Bond UCITS ETF", "ETF", isin),
    ]
    conflicts = find_identity_conflicts(rows)
    assert len(conflicts) == 1
    assert "disjoint_identity_families" in conflicts[0].signals


def test_final_export_pass_removes_conflict_reintroduced_after_mid_pipeline_cleanup(monkeypatch) -> None:
    from scripts import rebuild_canonical

    isin = "IE00BF4G7076"
    exported_rows = [
        row(
            "LSE",
            "JURE",
            "JPMorgan ETFs (Ireland) ICAV - US Research Enhanced Index Equity UCITS ETF - USD (acc)",
            "ETF",
            isin,
        ),
        row("OTC", "JUREF", "JPMORGAN ETFS IRELAND ICAV", "Stock", isin),
    ]
    monkeypatch.setattr(rebuild_canonical, "_ORIGINAL_CLEANED_ROWS", lambda: (exported_rows, {}))
    monkeypatch.setattr(rebuild_canonical, "_official_isin_by_listing", lambda: {})
    monkeypatch.setattr(rebuild_canonical, "_reviewed_keep_listing_keys", lambda: {})

    cleaned, alias_types = rebuild_canonical.strict_cleaned_rows()

    assert alias_types == {}
    assert not find_identity_conflicts(cleaned)
    assert {item["isin"] for item in cleaned} == {""}


def test_resolution_preserves_non_identity_payload_types() -> None:
    isin = "ARP6356B1059"
    rows = [
        {**row("TSX", "LIFE", "CI Global Longevity Economy Fund", "ETF", isin), "aliases": ["LIFE", "CI Longevity"]},
        {**row("BCBA", "LONG", "Longvie SA", "Stock", isin), "aliases": ["LONG"]},
    ]

    cleaned, _ = resolve_identity_conflicts(rows)

    assert all(isinstance(item["aliases"], list) for item in cleaned)
    assert cleaned[0]["aliases"] == ["LIFE", "CI Longevity"]

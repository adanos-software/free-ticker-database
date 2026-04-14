from __future__ import annotations

from scripts.backfill_stockanalysis_metadata import (
    build_metadata_updates,
    evaluate_target,
    parse_stockanalysis_company_profile,
    stockanalysis_sector_to_canonical,
)


def test_parse_stockanalysis_company_profile_extracts_core_fields() -> None:
    html = """
    <meta property="og:title" content="J&amp;B International (TPEX:4442) Company Profile &amp; Description"/>
    <table>
      <tr><td>Sector</td><td><a href="/stocks/sector/consumer-discretionary/">Consumer Discretionary</a></td></tr>
      <tr><td>Industry</td><td>Textile Manufacturing</td></tr>
      <tr><td>ISIN Number</td><td>KYG5001G1055</td></tr>
    </table>
    """

    profile = parse_stockanalysis_company_profile(html, "https://stockanalysis.com/quote/tpex/4442/company/")

    assert profile.name == "J&B International"
    assert profile.isin == "KYG5001G1055"
    assert profile.sector == "Consumer Discretionary"
    assert profile.industry == "Textile Manufacturing"


def test_stockanalysis_sector_to_canonical_uses_industry_fallback() -> None:
    profile = parse_stockanalysis_company_profile(
        """
        <meta property="og:title" content="Lizen JSC (HOSE:LCG) Company Profile &amp; Description"/>
        <tr><td>Industry</td><td>Heavy Construction Other Than Building Construction Contractors</td></tr>
        """,
        "https://stockanalysis.com/quote/hose/LCG/company/",
    )

    assert stockanalysis_sector_to_canonical(profile) == "Industrials"


def test_evaluate_target_accepts_valid_isin_and_sector_with_name_gate() -> None:
    profile = parse_stockanalysis_company_profile(
        """
        <meta property="og:title" content="J&amp;B International (TPEX:4442) Company Profile &amp; Description"/>
        <tr><td>Sector</td><td>Consumer Discretionary</td></tr>
        <tr><td>ISIN Number</td><td>KYG5001G1055</td></tr>
        """,
        "https://stockanalysis.com/quote/tpex/4442/company/",
    )

    result = evaluate_target(
        {
            "ticker": "4442",
            "exchange": "TPEX",
            "name": "J&B International Inc.",
            "isin": "",
            "stock_sector": "",
        },
        profile,
    )

    assert result["decision"] == "accept"
    assert result["accepted_isin"] == "KYG5001G1055"
    assert result["accepted_stock_sector"] == "Consumer Discretionary"


def test_build_metadata_updates_labels_stockanalysis_as_secondary() -> None:
    updates = build_metadata_updates(
        [
            {
                "ticker": "4442",
                "exchange": "TPEX",
                "decision": "accept",
                "accepted_isin": "KYG5001G1055",
                "accepted_stock_sector": "Consumer Discretionary",
                "stockanalysis_url": "https://stockanalysis.com/quote/tpex/4442/company/",
            }
        ]
    )

    assert [update["field"] for update in updates] == ["isin", "stock_sector"]
    assert all("reviewed secondary" in update["reason"] for update in updates)

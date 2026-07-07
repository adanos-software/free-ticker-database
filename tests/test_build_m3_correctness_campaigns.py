from scripts.build_m3_correctness_campaigns import (
    build_identity_residual_campaign,
    build_name_freshness_campaign,
    build_non_equity_leakage_campaign,
    build_rollup,
    build_sector_category_campaign,
)


def listing(
    ticker: str,
    exchange: str,
    name: str,
    *,
    asset_type: str = "Stock",
    stock_sector: str = "Information Technology",
    etf_category: str = "",
    isin: str = "US0000000001",
) -> dict[str, str]:
    return {
        "listing_key": f"{exchange}::{ticker}",
        "ticker": ticker,
        "exchange": exchange,
        "asset_type": asset_type,
        "name": name,
        "stock_sector": stock_sector,
        "etf_category": etf_category,
        "country": "United States",
        "country_code": "US",
        "isin": isin,
        "aliases": "",
    }


def official_ref(
    ticker: str,
    exchange: str,
    name: str,
    *,
    asset_type: str = "Stock",
    sector: str = "Financial Services",
    source_key: str = "official_test",
) -> dict[str, str]:
    return {
        "source_key": source_key,
        "provider": "Official Test Exchange",
        "source_url": "https://example.test",
        "ticker": ticker,
        "name": name,
        "exchange": exchange,
        "asset_type": asset_type,
        "listing_status": "active",
        "reference_scope": "exchange_directory",
        "official": "true",
        "isin": "",
        "sector": sector,
    }


def test_sector_campaign_reports_official_canonical_mismatch_and_applied_override() -> None:
    rows, market_rows, summary = build_sector_category_campaign(
        [
            listing("AAA", "NYSE", "AAA Inc", stock_sector="Information Technology"),
            listing("BBB", "NYSE", "BBB Inc", stock_sector="Information Technology"),
        ],
        [
            official_ref("AAA", "NYSE", "AAA Incorporated", sector="Financial Services"),
            official_ref("BBB", "NYSE", "BBB Incorporated", sector="Financial Services"),
        ],
        [
            {
                "ticker": "BBB",
                "exchange": "NYSE",
                "field": "stock_sector",
                "decision": "update",
                "proposed_value": "Financials",
            }
        ],
    )

    by_ticker = {row["ticker"]: row for row in rows}
    assert by_ticker["AAA"]["decision"] == "manual_review_official_value_conflicts_with_current"
    assert by_ticker["AAA"]["canonical_value"] == "Financials"
    assert by_ticker["BBB"]["decision"] == "metadata_override_present_pending_rebuild"
    assert summary["applied_rows"] == 0
    assert summary["metadata_override_pending_rows"] == 1
    assert market_rows[0]["candidate_rows"] == 1


def test_name_freshness_campaign_combines_entry_quality_and_symbol_change_rows() -> None:
    rows, summary = build_name_freshness_campaign(
        [
            {
                "listing_key": "NYSE::OLD",
                "ticker": "OLD",
                "exchange": "NYSE",
                "asset_type": "Stock",
                "name": "Old Name Inc",
                "issue_types": "official_name_mismatch",
            }
        ],
        [official_ref("OLD", "NYSE", "New Name Inc", sector="Financials")],
        [
            {
                "review_needed": "true",
                "old_scoped_listing_keys": "NYSE::AAA",
                "old_symbol": "AAA",
                "new_company_name": "Renamed Inc",
                "source": "official_notice",
                "review_priority": "P1",
                "verification_evidence_required": "official_exchange_notice",
                "recommended_next_source": "Official exchange notice.",
                "source_gate": "Do not rename without official evidence.",
            }
        ],
    )

    assert len(rows) == 2
    assert summary["manual_review_rows"] == 2
    assert {row["source_kind"] for row in rows} == {
        "entry_quality_official_name_mismatch",
        "symbol_change_review",
    }


def test_identity_campaign_tracks_name_equals_ticker_and_umbrella_isins() -> None:
    rows, summary = build_identity_residual_campaign(
        [
            listing("AAA", "NYSE", "AAA", isin="US0000000001"),
            listing("BBB", "NYSE", "First Issuer Inc", isin="US0000000002"),
            listing("BBBA", "LSE", "Second Issuer Plc", isin="US0000000002"),
            listing("BBBB", "TSX", "Third Issuer Ltd", isin="US0000000002"),
        ],
        [
            {
                "listing_key": "NYSE::AAA",
                "ticker": "AAA",
                "exchange": "NYSE",
                "asset_type": "Stock",
                "name": "AAA",
                "isin": "US0000000001",
                "issue_types": "country_isin_mismatch",
            }
        ],
        [],
    )

    residuals = {row["residual_type"] for row in rows}
    assert "name_equals_ticker" in residuals
    assert "country_isin_mismatch" in residuals
    assert "umbrella_isin_three_or_more_distinct_names" in residuals
    assert summary["manual_review_rows"] == len(rows)


def test_non_equity_campaign_uses_shared_guard() -> None:
    rows, summary = build_non_equity_leakage_campaign(
        [
            listing("GOOD", "NASDAQ", "Good Company Common Stock"),
            listing("BADW", "NASDAQ", "Bad Company Warrant"),
        ],
        [],
    )

    assert [row["ticker"] for row in rows] == ["BADW"]
    assert rows[0]["guard_decision"] == "blocked_non_common_stock"
    assert summary["blocked_rows"] == 1


def test_rollup_requires_all_m3_campaigns_and_preserves_no_99_claim() -> None:
    payload = {
        "_meta": {"generated_at": "2026-07-07T00:00:00Z"},
        "summary": {"rows": 1, "applied_rows": 0, "blocked_rows": 1, "manual_review_rows": 1},
    }
    rollup = build_rollup(
        generated_at="2026-07-07T00:00:00Z",
        source_files={"listings_csv": "data/listings.csv"},
        payloads={
            "C1_sector_etf_category_truth": payload,
            "C2_name_freshness": payload,
            "C4_identity_residual_burn_down": payload,
            "C5_non_equity_leakage_guard": payload,
            "C6_reaudit_after_each_block": payload,
        },
    )

    assert rollup["summary"]["required_campaigns_present"] is True
    assert rollup["summary"]["correctness_claim"] == "not_claimed_99_percent_without_external_stratified_audit"

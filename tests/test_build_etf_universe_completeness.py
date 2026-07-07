from scripts.build_etf_universe_completeness import (
    build_report,
    official_active_etf_master_rows,
    render_markdown,
)


def test_official_active_etf_master_rows_filters_and_dedupes() -> None:
    rows = official_active_etf_master_rows(
        [
            {
                "source_key": "six_etf_products",
                "provider": "SIX",
                "ticker": "AAA",
                "exchange": "SIX",
                "asset_type": "ETF",
                "listing_status": "active",
                "reference_scope": "listed_companies_subset",
                "official": "true",
            },
            {
                "source_key": "six_etf_products",
                "provider": "SIX",
                "ticker": "AAA",
                "exchange": "SIX",
                "asset_type": "ETF",
                "listing_status": "active",
                "reference_scope": "listed_companies_subset",
                "official": "true",
            },
            {
                "source_key": "six_equity_issuers",
                "provider": "SIX",
                "ticker": "STOCK",
                "exchange": "SIX",
                "asset_type": "Stock",
                "listing_status": "active",
                "reference_scope": "listed_companies_subset",
                "official": "true",
            },
            {
                "source_key": "manual",
                "provider": "Manual",
                "ticker": "BBB",
                "exchange": "SIX",
                "asset_type": "ETF",
                "listing_status": "active",
                "reference_scope": "listed_companies_subset",
                "official": "false",
            },
            {
                "source_key": "six_etf_products",
                "provider": "SIX",
                "ticker": "OLD",
                "exchange": "SIX",
                "asset_type": "ETF",
                "listing_status": "delisted",
                "reference_scope": "listed_companies_subset",
                "official": "true",
            },
            {
                "source_key": "nasdaq_trading_system_adds_deletes",
                "provider": "Nasdaq Trader",
                "ticker": "DAILY",
                "exchange": "NASDAQ",
                "asset_type": "ETF",
                "listing_status": "active",
                "reference_scope": "corporate_action_daily_list",
                "official": "true",
            },
        ]
    )

    assert [(row["source_key"], row["exchange"], row["ticker"]) for row in rows] == [
        ("six_etf_products", "SIX", "AAA")
    ]


def test_build_report_classifies_matched_missing_collision_and_asset_type_review() -> None:
    listings = [
        {"listing_key": "SIX::AAA", "ticker": "AAA", "exchange": "SIX", "asset_type": "ETF"},
        {"listing_key": "XETRA::CCC", "ticker": "CCC", "exchange": "XETRA", "asset_type": "ETF"},
        {"listing_key": "SIX::DDD", "ticker": "DDD", "exchange": "SIX", "asset_type": "Stock"},
    ]
    masterfiles = [
        {
            "source_key": "six_etf_products",
            "provider": "SIX",
            "ticker": "AAA",
            "name": "AAA ETF",
            "exchange": "SIX",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "CH0000000001",
            "sector": "Equity",
        },
        {
            "source_key": "six_etf_products",
            "provider": "SIX",
            "ticker": "BBB",
            "name": "BBB ETF",
            "exchange": "SIX",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "CH0000000002",
            "sector": "Fixed Income",
        },
        {
            "source_key": "six_etf_products",
            "provider": "SIX",
            "ticker": "CCC",
            "name": "CCC ETF",
            "exchange": "SIX",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "CH0000000003",
            "sector": "Equity",
        },
        {
            "source_key": "six_etf_products",
            "provider": "SIX",
            "ticker": "DDD",
            "name": "DDD ETF",
            "exchange": "SIX",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "official": "true",
            "isin": "CH0000000004",
            "sector": "Equity",
        },
    ]

    report = build_report(
        listings,
        masterfiles,
        masterfile_summary={
            "source_details": {
                "six_etf_products": {
                    "mode": "network",
                    "generated_at": "2026-07-07T00:00:00Z",
                }
            }
        },
        generated_at="2026-07-07T01:00:00Z",
    )

    assert report["summary"] == {
        "official_etf_rows": 4,
        "matched_etf_listings": 1,
        "missing_or_review_rows": 3,
        "missing_from_db": 1,
        "collision_hidden_by_global_ticker": 1,
        "local_listing_asset_type_mismatch": 1,
        "etf_recall_pct": 25.0,
        "source_count": 1,
        "exchange_count": 1,
    }
    assert {row["ticker"]: row["match_status"] for row in report["missing_rows"]} == {
        "BBB": "missing_from_db",
        "CCC": "collision_hidden_by_global_ticker",
        "DDD": "local_listing_asset_type_mismatch",
    }
    assert {row["ticker"]: row["candidate_action"] for row in report["missing_rows"]} == {
        "BBB": "review_official_listing_add",
        "CCC": "review_collision_safe_listing_add",
        "DDD": "review_asset_type",
    }
    assert report["by_source"][0]["source_mode"] == "network"
    assert report["by_source"][0]["generated_at"] == "2026-07-07T00:00:00Z"


def test_render_markdown_documents_non_applying_policy() -> None:
    report = build_report(
        [],
        [
            {
                "source_key": "euronext_etfs",
                "provider": "Euronext",
                "ticker": "ETF1",
                "name": "ETF One",
                "exchange": "Euronext",
                "asset_type": "ETF",
                "listing_status": "active",
                "reference_scope": "listed_companies_subset",
                "official": "true",
                "isin": "FR0000000001",
                "sector": "Equity",
            }
        ],
        generated_at="2026-07-07T01:00:00Z",
    )

    markdown = render_markdown(report)

    assert "# ETF Universe Completeness" in markdown
    assert "Missing rows are review candidates only" in markdown
    assert "add_only_after_official_identity_isin_checksum_and_no_collision_review" in markdown

from __future__ import annotations

from scripts.build_coverage_report import (
    build_b3_gap_breakdown,
    build_country_report,
    build_exchange_reference_catalog,
    build_exchange_report,
    build_freshness_report,
    build_gap_report,
    build_global_summary,
    build_masterfile_collision_report,
    load_verification_report,
    render_markdown,
)


def test_build_exchange_reference_catalog_classifies_venue_statuses():
    masterfiles = [
        {"exchange": "NASDAQ", "source_key": "nasdaq_listed", "official": "true", "reference_scope": "exchange_directory"},
        {"exchange": "TSX", "source_key": "tmx_interlisted", "official": "true", "reference_scope": "interlisted_subset"},
        {"exchange": "OTC", "source_key": "manual:otc", "official": "false", "reference_scope": "manual"},
    ]

    catalog = build_exchange_reference_catalog(masterfiles)

    assert catalog == {
        "NASDAQ": {
            "exchange": "NASDAQ",
            "venue_status": "official_full",
            "official_source_count": 1,
            "manual_source_count": 0,
            "reference_scopes": ["exchange_directory"],
        },
        "OTC": {
            "exchange": "OTC",
            "venue_status": "manual_only",
            "official_source_count": 0,
            "manual_source_count": 1,
            "reference_scopes": [],
        },
        "TSX": {
            "exchange": "TSX",
            "venue_status": "official_partial",
            "official_source_count": 1,
            "manual_source_count": 0,
            "reference_scopes": ["interlisted_subset"],
        },
    }


def test_build_exchange_report_includes_masterfile_and_verification_rates():
    tickers = [
        {"ticker": "AAPL", "exchange": "NASDAQ", "isin": "US0378331005", "sector": "Information Technology"},
        {"ticker": "IBM", "exchange": "NYSE", "isin": "", "sector": "Information Technology"},
    ]
    identifiers = [
        {"ticker": "AAPL", "exchange": "NASDAQ", "cik": "0000320193", "figi": "", "lei": ""},
        {"ticker": "IBM", "exchange": "NYSE", "cik": "0000051143", "figi": "BBG000BLNNH6", "lei": ""},
    ]
    masterfiles = [
        {"ticker": "AAPL", "exchange": "NASDAQ", "listing_status": "active", "reference_scope": "exchange_directory", "official": "true", "source_key": "nasdaq_listed"},
        {"ticker": "QQQ", "exchange": "NASDAQ", "listing_status": "active", "reference_scope": "exchange_directory", "official": "true", "source_key": "nasdaq_listed"},
        {"ticker": "IBM", "exchange": "NYSE", "listing_status": "active", "reference_scope": "exchange_directory", "official": "true", "source_key": "other"},
        {"ticker": "1301", "exchange": "TSE", "listing_status": "active", "reference_scope": "exchange_directory", "official": "true", "source_key": "jpx"},
    ]
    verification_rows = [
        {
            "exchange": "NASDAQ",
            "items": 10,
            "verified": 8,
            "reference_gap": 2,
            "missing_from_official": 0,
            "name_mismatch": 0,
            "cross_exchange_collision": 0,
            "officially_covered_items": 8,
        }
    ]
    etf_verification_rows = [
        {
            "exchange": "NASDAQ",
            "items": 3,
            "verified": 2,
            "reference_gap": 1,
            "missing_from_official": 0,
            "name_mismatch": 0,
            "cross_exchange_collision": 0,
            "officially_covered_items": 2,
        }
    ]

    rows = build_exchange_report(
        tickers,
        identifiers,
        masterfiles,
        stock_verification_exchange_rows=verification_rows,
        etf_verification_exchange_rows=etf_verification_rows,
    )

    assert rows == [
        {
            "exchange": "NASDAQ",
            "venue_status": "official_full",
            "official_source_count": 1,
            "manual_source_count": 0,
            "reference_scopes": ["exchange_directory"],
            "tickers": 1,
            "isin_coverage": 1,
            "sector_coverage": 1,
            "cik_coverage": 1,
            "figi_coverage": 0,
            "lei_coverage": 0,
            "masterfile_symbols": 2,
            "masterfile_matches": 1,
            "masterfile_collisions": 0,
            "masterfile_missing": 1,
            "masterfile_match_rate": 50.0,
            "masterfile_collision_rate": 0.0,
            "verification_items": 10,
            "verification_verified": 8,
            "verification_reference_gap": 2,
            "verification_missing_from_official": 0,
            "verification_name_mismatch": 0,
            "verification_cross_exchange_collision": 0,
            "verification_verified_rate_on_covered": 100.0,
            "stock_verification_items": 10,
            "stock_verification_verified": 8,
            "stock_verification_reference_gap": 2,
            "stock_verification_missing_from_official": 0,
            "stock_verification_name_mismatch": 0,
            "stock_verification_cross_exchange_collision": 0,
            "stock_verification_verified_rate_on_covered": 100.0,
            "etf_verification_items": 3,
            "etf_verification_verified": 2,
            "etf_verification_reference_gap": 1,
            "etf_verification_missing_from_official": 0,
            "etf_verification_name_mismatch": 0,
            "etf_verification_cross_exchange_collision": 0,
            "etf_verification_verified_rate_on_covered": 100.0,
            "unresolved_count": 3,
        },
        {
            "exchange": "NYSE",
            "venue_status": "official_full",
            "official_source_count": 1,
            "manual_source_count": 0,
            "reference_scopes": ["exchange_directory"],
            "tickers": 1,
            "isin_coverage": 0,
            "sector_coverage": 1,
            "cik_coverage": 1,
            "figi_coverage": 1,
            "lei_coverage": 0,
            "masterfile_symbols": 1,
            "masterfile_matches": 1,
            "masterfile_collisions": 0,
            "masterfile_missing": 0,
            "masterfile_match_rate": 100.0,
            "masterfile_collision_rate": 0.0,
            "verification_items": 0,
            "verification_verified": 0,
            "verification_reference_gap": 0,
            "verification_missing_from_official": 0,
            "verification_name_mismatch": 0,
            "verification_cross_exchange_collision": 0,
            "verification_verified_rate_on_covered": None,
            "stock_verification_items": 0,
            "stock_verification_verified": 0,
            "stock_verification_reference_gap": 0,
            "stock_verification_missing_from_official": 0,
            "stock_verification_name_mismatch": 0,
            "stock_verification_cross_exchange_collision": 0,
            "stock_verification_verified_rate_on_covered": None,
            "etf_verification_items": 0,
            "etf_verification_verified": 0,
            "etf_verification_reference_gap": 0,
            "etf_verification_missing_from_official": 0,
            "etf_verification_name_mismatch": 0,
            "etf_verification_cross_exchange_collision": 0,
            "etf_verification_verified_rate_on_covered": None,
            "unresolved_count": 0,
        },
        {
            "exchange": "TSE",
            "venue_status": "official_full",
            "official_source_count": 1,
            "manual_source_count": 0,
            "reference_scopes": ["exchange_directory"],
            "tickers": 0,
            "isin_coverage": 0,
            "sector_coverage": 0,
            "cik_coverage": 0,
            "figi_coverage": 0,
            "lei_coverage": 0,
            "masterfile_symbols": 1,
            "masterfile_matches": 0,
            "masterfile_collisions": 0,
            "masterfile_missing": 1,
            "masterfile_match_rate": 0.0,
            "masterfile_collision_rate": 0.0,
            "verification_items": 0,
            "verification_verified": 0,
            "verification_reference_gap": 0,
            "verification_missing_from_official": 0,
            "verification_name_mismatch": 0,
            "verification_cross_exchange_collision": 0,
            "verification_verified_rate_on_covered": None,
            "stock_verification_items": 0,
            "stock_verification_verified": 0,
            "stock_verification_reference_gap": 0,
            "stock_verification_missing_from_official": 0,
            "stock_verification_name_mismatch": 0,
            "stock_verification_cross_exchange_collision": 0,
            "stock_verification_verified_rate_on_covered": None,
            "etf_verification_items": 0,
            "etf_verification_verified": 0,
            "etf_verification_reference_gap": 0,
            "etf_verification_missing_from_official": 0,
            "etf_verification_name_mismatch": 0,
            "etf_verification_cross_exchange_collision": 0,
            "etf_verification_verified_rate_on_covered": None,
            "unresolved_count": 0,
        },
    ]


def test_build_country_report_summarizes_identifier_coverage():
    tickers = [
        {"ticker": "AAPL", "exchange": "NASDAQ", "country": "United States", "isin": "US0378331005", "sector": "Information Technology"},
        {"ticker": "SHOP", "exchange": "TSX", "country": "Canada", "isin": "", "sector": ""},
    ]
    identifiers = [
        {"ticker": "AAPL", "exchange": "NASDAQ", "cik": "0000320193", "figi": "BBG000B9XRY4", "lei": ""},
        {"ticker": "SHOP", "exchange": "TSX", "cik": "", "figi": "", "lei": ""},
    ]

    rows = build_country_report(tickers, identifiers)

    assert rows == [
        {
            "country": "Canada",
            "tickers": 1,
            "isin_coverage": 0,
            "sector_coverage": 0,
            "cik_coverage": 0,
            "figi_coverage": 0,
            "lei_coverage": 0,
        },
        {
            "country": "United States",
            "tickers": 1,
            "isin_coverage": 1,
            "sector_coverage": 1,
            "cik_coverage": 1,
            "figi_coverage": 1,
            "lei_coverage": 0,
        },
    ]


def test_global_summary_markdown_and_gaps_include_new_sections():
    exchange_coverage = [
        {
            "exchange": "NYSE",
            "venue_status": "official_full",
            "masterfile_symbols": 5,
            "masterfile_matches": 4,
            "masterfile_collisions": 1,
            "masterfile_missing": 0,
        }
    ]
    report = {
        "global": build_global_summary(
            tickers=[{"ticker": "AAA", "exchange": "NYSE", "asset_type": "Stock", "isin": "X", "sector": "Y"}],
            listings=[{"ticker": "AAA", "exchange": "NYSE", "asset_type": "Stock", "isin": "X", "sector": "Y"}],
            aliases=[{"ticker": "AAA", "alias": "alpha"}],
            identifiers_extended=[{"ticker": "AAA", "exchange": "NYSE", "cik": "1", "figi": "", "lei": ""}],
            listing_status_history=[{"ticker": "AAA"}],
            listing_events=[{"ticker": "AAA"}],
            exchange_coverage=exchange_coverage,
            stock_verification_summary={"items": 1, "status_counts": {"verified": 1}},
            etf_verification_summary={"items": 2, "status_counts": {"verified": 1, "reference_gap": 1}},
        ),
        "freshness": {"tickers_built_at": "2026-04-06T00:00:00Z"},
        "source_coverage": [
            {
                "key": "nasdaq_listed",
                "provider": "Nasdaq Trader",
                "reference_scope": "exchange_directory",
                "mode": "network",
                "rows": 10,
                "generated_at": "2026-04-06T00:00:00Z",
            }
        ],
        "exchange_coverage": [
            {
                "exchange": "NYSE",
                "venue_status": "official_full",
                "tickers": 1,
                "isin_coverage": 1,
                "sector_coverage": 1,
                "cik_coverage": 1,
                "figi_coverage": 0,
                "lei_coverage": 0,
                "masterfile_symbols": 5,
                "masterfile_matches": 4,
                "masterfile_collisions": 1,
                "masterfile_missing": 0,
                "masterfile_match_rate": 80.0,
                "masterfile_collision_rate": 20.0,
                "verification_verified_rate_on_covered": 100.0,
            }
        ],
        "country_coverage": [],
        "gap_report": [
            {
                "exchange": "NYSE",
                "venue_status": "official_full",
                "unresolved_findings": 2,
                "reference_gap": 0,
                "missing_from_official": 1,
                "name_mismatch": 1,
                "cross_exchange_collision": 0,
            }
        ],
    }

    markdown = render_markdown(report)

    assert report["global"]["tickers"] == 1
    assert report["global"]["listing_keys"] == 1
    assert report["global"]["official_full_exchanges"] == 1
    assert report["global"]["etf_verification_items"] == 2
    assert "# Coverage Report" in markdown
    assert "## Freshness" in markdown
    assert "## Source Coverage" in markdown
    assert "## Unresolved Gaps" in markdown


def test_build_masterfile_collision_report_exposes_cross_exchange_conflicts():
    tickers = [
        {"ticker": "1301", "exchange": "TWSE"},
        {"ticker": "AAPL", "exchange": "NASDAQ"},
    ]
    masterfiles = [
        {"ticker": "1301", "name": "Kyokuyo", "exchange": "TSE", "listing_status": "active", "reference_scope": "exchange_directory"},
        {"ticker": "1306", "name": "ETF", "exchange": "TSE", "listing_status": "active", "reference_scope": "exchange_directory"},
        {"ticker": "AAPL", "name": "Apple Inc.", "exchange": "NASDAQ", "listing_status": "active", "reference_scope": "exchange_directory"},
    ]

    report = build_masterfile_collision_report(tickers, masterfiles)

    assert report["global"] == {
        "official_symbols": 3,
        "matched": 1,
        "collisions": 1,
        "missing": 1,
    }
    assert report["exchanges"][1]["collision_examples"][0]["ticker"] == "1301"
    assert report["exchanges"][1]["missing_examples"][0]["ticker"] == "1306"


def test_build_freshness_report_calculates_ages():
    freshness = build_freshness_report(
        {"generated_at": "2026-04-06T10:00:00Z"},
        {"generated_at": "2026-04-06T11:00:00Z"},
        {"observed_at": "2026-04-06T12:00:00Z"},
        {"run_dir": "data/stock_verification/run-a", "generated_at": "2026-04-06T13:00:00Z"},
        {"run_dir": "data/etf_verification/run-b", "generated_at": "2026-04-06T14:00:00Z"},
    )

    assert freshness["masterfiles_generated_at"] == "2026-04-06T10:00:00Z"
    assert freshness["identifiers_generated_at"] == "2026-04-06T11:00:00Z"
    assert freshness["listing_history_observed_at"] == "2026-04-06T12:00:00Z"
    assert freshness["latest_verification_run"] == "data/stock_verification/run-a"
    assert freshness["latest_etf_verification_run"] == "data/etf_verification/run-b"


def test_build_gap_report_and_b3_breakdown():
    exchange_coverage = [
        {
            "exchange": "B3",
            "venue_status": "official_full",
            "masterfile_missing": 10,
            "masterfile_collisions": 0,
        }
    ]
    stock_verification_exchange_rows = [
        {
            "exchange": "B3",
            "reference_gap": 0,
            "missing_from_official": 3,
            "name_mismatch": 1,
            "cross_exchange_collision": 0,
        }
    ]
    etf_verification_exchange_rows = [
        {
            "exchange": "B3",
            "reference_gap": 2,
            "missing_from_official": 0,
            "name_mismatch": 0,
            "cross_exchange_collision": 1,
        }
    ]
    verification_rows = [
        {"exchange": "B3", "ticker": "ABBV34", "name": "AbbVie", "status": "missing_from_official"},
        {"exchange": "B3", "ticker": "ASAI3F", "name": "Sendas", "status": "missing_from_official"},
        {"exchange": "B3", "ticker": "PETR4", "name": "Petrobras", "status": "missing_from_official"},
    ]

    gap_report = build_gap_report(exchange_coverage, stock_verification_exchange_rows, etf_verification_exchange_rows)
    b3_breakdown = build_b3_gap_breakdown(verification_rows)

    assert gap_report == [
        {
            "exchange": "B3",
            "venue_status": "official_full",
            "unresolved_findings": 7,
            "reference_gap": 2,
            "missing_from_official": 3,
            "name_mismatch": 1,
            "cross_exchange_collision": 1,
            "stock_reference_gap": 0,
            "stock_missing_from_official": 3,
            "stock_name_mismatch": 1,
            "stock_cross_exchange_collision": 0,
            "etf_reference_gap": 2,
            "etf_missing_from_official": 0,
            "etf_name_mismatch": 0,
            "etf_cross_exchange_collision": 1,
            "masterfile_missing": 10,
            "masterfile_collisions": 0,
        }
    ]
    assert b3_breakdown["categories"] == {
        "bdr_or_foreign_receipt": 1,
        "fractional_line": 1,
        "local_share_line": 1,
    }


def test_load_verification_report_reads_latest_chunk_rows(tmp_path):
    run_dir = tmp_path / "run-1"
    run_dir.mkdir()
    (run_dir / "summary.json").write_text(
        '{"items": 2, "status_counts": {"verified": 1, "reference_gap": 1}}',
        encoding="utf-8",
    )
    (run_dir / "chunk-01-of-01.json").write_text(
        '[{"exchange":"NASDAQ","status":"verified"},{"exchange":"OTC","status":"reference_gap"}]',
        encoding="utf-8",
    )

    report = load_verification_report(run_dir)

    assert report["summary"]["items"] == 2
    assert report["exchange_rows"] == [
        {
            "exchange": "NASDAQ",
            "items": 1,
            "verified": 1,
            "reference_gap": 0,
            "missing_from_official": 0,
            "name_mismatch": 0,
            "cross_exchange_collision": 0,
            "asset_type_mismatch": 0,
            "non_active_official": 0,
            "officially_covered_items": 1,
            "verified_rate_on_covered": 100.0,
        },
        {
            "exchange": "OTC",
            "items": 1,
            "verified": 0,
            "reference_gap": 1,
            "missing_from_official": 0,
            "name_mismatch": 0,
            "cross_exchange_collision": 0,
            "asset_type_mismatch": 0,
            "non_active_official": 0,
            "officially_covered_items": 0,
            "verified_rate_on_covered": None,
        },
    ]

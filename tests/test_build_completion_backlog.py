from __future__ import annotations

from scripts.build_completion_backlog import (
    FIELD_MISSING_ETF_CATEGORY,
    FIELD_MISSING_ISIN,
    FIELD_MISSING_STOCK_SECTOR,
    build_completion_backlog,
    render_markdown,
    summarize,
)


def row_for(rows, *, exchange: str, field: str):
    for row in rows:
        if row.exchange == exchange and row.field == field:
            return row
    raise AssertionError(f"missing row for {exchange} {field}")


def test_build_completion_backlog_splits_fields_and_targets_model_columns():
    tickers = [
        {"ticker": "1301", "exchange": "TSE", "asset_type": "Stock", "sector": "Consumer Staples"},
        {"ticker": "1306", "exchange": "TSE", "asset_type": "ETF", "sector": ""},
        {"ticker": "OTC1", "exchange": "OTC", "asset_type": "Stock", "sector": ""},
        {"ticker": "OTCE", "exchange": "OTC", "asset_type": "ETF", "sector": ""},
        {"ticker": "B3SA3", "exchange": "B3", "asset_type": "Stock", "sector": ""},
    ]
    scopes = [
        {"exchange": "TSE", "asset_type": "Stock", "scope_reason": "primary_listing_missing_isin"},
        {"exchange": "TSE", "asset_type": "ETF", "scope_reason": "primary_listing_missing_isin"},
        {"exchange": "TSX", "asset_type": "Stock", "scope_reason": "primary_listing_missing_isin"},
        {"exchange": "OTC", "asset_type": "Stock", "scope_reason": "otc_listing"},
    ]
    coverage_report = {
        "global": {"official_masterfile_collisions": 6345},
        "by_exchange": [
            {"exchange": "TSE", "venue_status": "official_full", "official_source_count": 1, "reference_scopes": ["exchange_directory"]},
            {"exchange": "OTC", "venue_status": "official_partial", "official_source_count": 1, "reference_scopes": ["otc_subset"]},
            {"exchange": "B3", "venue_status": "official_full", "official_source_count": 1, "reference_scopes": ["exchange_directory"]},
            {"exchange": "TSX", "venue_status": "official_full", "official_source_count": 2, "reference_scopes": ["exchange_directory"]},
        ],
    }

    rows = build_completion_backlog(tickers, scopes, coverage_report)

    assert all(row.recommended_source for row in rows)
    assert all(row.confidence_policy for row in rows)
    assert all(row.target_field in {"isin", "stock_sector", "etf_category"} for row in rows)

    tse_isin = row_for(rows, exchange="TSE", field=FIELD_MISSING_ISIN)
    assert tse_isin.asset_type == "All"
    assert tse_isin.target_field == "isin"
    assert tse_isin.missing_count == 2
    assert tse_isin.stock_missing_count == 1
    assert tse_isin.etf_missing_count == 1
    assert tse_isin.priority_rank == 1
    assert "JPX/TSE" in tse_isin.recommended_source

    otc_sector = row_for(rows, exchange="OTC", field=FIELD_MISSING_STOCK_SECTOR)
    assert otc_sector.asset_type == "Stock"
    assert otc_sector.target_field == "stock_sector"
    assert otc_sector.missing_count == 1
    assert otc_sector.review_needed is True
    assert "canonical stock GICS sector" in otc_sector.confidence_policy

    otc_category = row_for(rows, exchange="OTC", field=FIELD_MISSING_ETF_CATEGORY)
    assert otc_category.asset_type == "ETF"
    assert otc_category.target_field == "etf_category"
    assert otc_category.missing_count == 1
    assert "ETF categories" in otc_category.confidence_policy

    b3_sector = row_for(rows, exchange="B3", field=FIELD_MISSING_STOCK_SECTOR)
    assert "FinanceDatabase" in b3_sector.recommended_source


def test_render_markdown_includes_model_and_source_block_notes():
    coverage_report = {"global": {"official_masterfile_collisions": 5}, "by_exchange": []}
    rows = build_completion_backlog(
        [{"ticker": "A", "exchange": "OTC", "asset_type": "Stock", "sector": ""}],
        [{"exchange": "TSE", "asset_type": "Stock", "scope_reason": "primary_listing_missing_isin"}],
        coverage_report,
    )
    summary = summarize(rows, coverage_report, "2026-04-12T00:00:00Z")

    markdown = render_markdown(rows, summary)

    assert "Missing primary ISIN rows" in markdown
    assert "`stock_sector`" in markdown
    assert "`etf_category`" in markdown
    assert "listing_key" in markdown
    assert "TSE ISIN" in markdown

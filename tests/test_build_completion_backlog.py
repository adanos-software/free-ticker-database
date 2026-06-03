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

    assert summary["next_actions"]
    assert summary["next_actions"][0]["safe_action"] == "candidate_for_official_followup"
    assert "direct" not in summary["next_actions"][0]["safe_action"]
    assert "Missing primary ISIN rows" in markdown
    assert "Next Safe Batches" in markdown
    assert "orchestration candidates only" in markdown
    assert "`stock_sector`" in markdown
    assert "`etf_category`" in markdown
    assert "listing_key" in markdown
    assert "High-count primary ISIN residuals" in markdown


def test_asx_next_action_uses_residual_review_gate_when_no_apply_candidates():
    rows = build_completion_backlog(
        [],
        [
            {"exchange": "ASX", "asset_type": "Stock", "scope_reason": "primary_listing_missing_isin"},
            {"exchange": "ASX", "asset_type": "ETF", "scope_reason": "primary_listing_missing_isin"},
        ],
        {"by_exchange": [{"exchange": "ASX", "venue_status": "official_partial", "official_source_count": 2}]},
    )
    summary = summarize(
        rows,
        {"global": {}, "by_exchange": []},
        "2026-04-12T00:00:00Z",
        asx_residual_review={
            "summary": {
                "asx_residual_backlog": {
                    "rows": 2,
                    "official_isin_apply_candidate_rows": 0,
                    "direct_data_apply_allowed_rows": 0,
                    "source_gate": "ASX residual work remains blocked without exact official evidence.",
                },
                "top_asx_resolution_review_batches": [
                    {
                        "recommended_next_source": "Reviewed ASX scope decision before identifier work.",
                    }
                ],
            }
        },
    )

    action = summary["next_actions"][0]
    assert action["exchange"] == "ASX"
    assert action["review_needed"] is True
    assert action["residual_gate"] == "asx_residual_review_blocks_direct_apply"
    assert action["direct_data_apply_allowed_rows"] == 0
    assert action["official_isin_apply_candidate_rows"] == 0
    assert "Reviewed ASX scope decision" in action["recommended_source"]


def test_tse_sector_next_action_uses_jpx_report_gate_when_no_apply_candidates():
    rows = build_completion_backlog(
        [{"ticker": "2989", "exchange": "TSE", "asset_type": "Stock", "sector": ""}],
        [],
        {"by_exchange": [{"exchange": "TSE", "venue_status": "official_full", "official_source_count": 2}]},
    )
    summary = summarize(
        rows,
        {"global": {}, "by_exchange": []},
        "2026-04-12T00:00:00Z",
        jpx_tse_sector_backfill={
            "summary": {
                "candidates": 1,
                "accepted_sector_updates": 0,
                "decision_counts": {"missing_jpx_industry": 1},
            }
        },
    )

    action = summary["next_actions"][0]
    assert action["exchange"] == "TSE"
    assert action["field"] == FIELD_MISSING_STOCK_SECTOR
    assert action["review_needed"] is True
    assert action["residual_gate"] == "jpx_tse_sector_backfill_blocks_direct_apply"
    assert action["accepted_sector_updates"] == 0
    assert action["jpx_missing_industry_rows"] == 1
    assert "no JPX 33-industry values" in action["recommended_source"]


def test_otc_sector_next_action_uses_sec_sic_residual_gate_when_no_apply_candidates():
    rows = build_completion_backlog(
        [{"ticker": "A", "exchange": "OTC", "asset_type": "Stock", "sector": ""}],
        [],
        {"by_exchange": [{"exchange": "OTC", "venue_status": "official_full", "official_source_count": 3}]},
    )
    summary = summarize(
        rows,
        {"global": {}, "by_exchange": []},
        "2026-04-12T00:00:00Z",
        sec_sic_sector_backfill={
            "summary": {
                "candidates": 1,
                "accepted_sector_updates": 0,
                "exchanges": ["OTC"],
                "requests_made": 0,
                "decision_counts": {"no_sec_match": 1},
            }
        },
    )

    action = summary["next_actions"][0]
    assert action["exchange"] == "OTC"
    assert action["field"] == FIELD_MISSING_STOCK_SECTOR
    assert action["review_needed"] is True
    assert action["residual_gate"] == "sec_sic_otc_no_apply_candidates"
    assert action["sec_sic_candidates"] == 1
    assert action["accepted_sector_updates"] == 0
    assert action["sec_no_match_rows"] == 1
    assert "no accepted OTC sector candidates" in action["recommended_source"]


def test_canada_isin_next_action_uses_residual_gate_when_no_direct_identifier_apply():
    rows = build_completion_backlog(
        [],
        [
            {"exchange": "TSX", "asset_type": "Stock", "scope_reason": "primary_listing_missing_isin"},
            {"exchange": "TSX", "asset_type": "ETF", "scope_reason": "primary_listing_missing_isin"},
        ],
        {"by_exchange": [{"exchange": "TSX", "venue_status": "official_full", "official_source_count": 3}]},
    )
    summary = summarize(
        rows,
        {"global": {}, "by_exchange": []},
        "2026-04-12T00:00:00Z",
        canada_residual_review={
            "summary": {
                "canada_identifier_backlog": {
                    "rows": 4,
                    "direct_identifier_apply_allowed_rows": 0,
                    "official_isin_source_required_rows": 3,
                    "scope_decision_required_rows": 1,
                    "reviewed_openfigi_source_gap_rows": 2,
                    "source_gate": "Canadian identifier work remains blocked without listing-keyed official evidence.",
                },
                "canada_resolution_queue_exchange_totals": {
                    "missing_isin_official_canada_masterfiles_do_not_expose_isin": {"TSX": 2},
                    "missing_isin_reviewed_source_gap": {"TSX": 1},
                },
                "top_canada_resolution_review_batches": [
                    {
                        "canada_resolution_queue": "missing_isin_official_canada_masterfiles_do_not_expose_isin",
                        "exchange": "TSX",
                        "recommended_next_source": "Official Canada identifier source exposing a valid ISIN.",
                    }
                ],
            }
        },
    )

    action = summary["next_actions"][0]
    assert action["exchange"] == "TSX"
    assert action["field"] == FIELD_MISSING_ISIN
    assert action["review_needed"] is True
    assert action["residual_gate"] == "canada_residual_review_blocks_direct_identifier_apply"
    assert action["direct_identifier_apply_allowed_rows"] == 0
    assert action["official_isin_source_required_rows"] == 3
    assert action["scope_decision_required_rows"] == 1
    assert action["exchange_missing_isin_official_source_rows"] == 2
    assert "Official Canada identifier source" in action["recommended_source"]


def test_b3_sector_next_action_uses_residual_gate_when_no_taxonomy_match():
    rows = build_completion_backlog(
        [{"ticker": "B3SA3", "exchange": "B3", "asset_type": "Stock", "sector": ""}],
        [],
        {"by_exchange": [{"exchange": "B3", "venue_status": "official_full", "official_source_count": 3}]},
    )
    summary = summarize(
        rows,
        {"global": {}, "by_exchange": []},
        "2026-04-12T00:00:00Z",
        b3_residual_sector_review={
            "summary": {
                "rows": 1,
                "apply_eligibility_totals": {"source_gap_keep_blank_until_official_taxonomy_evidence": 1},
                "b3_probe_decision_totals": {"no_b3_code_match": 1},
                "b3_code_shape_totals": {"alpha_b3_code": 1},
                "top_b3_sector_review_batches": [
                    {
                        "recommended_next_source": "Stronger official B3 taxonomy source.",
                        "source_gate": "Keep stock_sector blank until official B3 evidence matches.",
                    }
                ],
            }
        },
    )

    action = summary["next_actions"][0]
    assert action["exchange"] == "B3"
    assert action["field"] == FIELD_MISSING_STOCK_SECTOR
    assert action["review_needed"] is True
    assert action["residual_gate"] == "b3_residual_sector_review_blocks_direct_apply"
    assert action["b3_residual_sector_rows"] == 1
    assert action["b3_source_gap_keep_blank_rows"] == 1
    assert action["b3_no_code_match_rows"] == 1
    assert "Stronger official B3 taxonomy source" in action["recommended_source"]


def test_weak_sector_next_action_uses_residual_gate_when_no_direct_sector_apply():
    rows = build_completion_backlog(
        [{"ticker": "A", "exchange": "CSE_LK", "asset_type": "Stock", "sector": ""}],
        [],
        {"by_exchange": [{"exchange": "CSE_LK", "venue_status": "official_partial", "official_source_count": 2}]},
    )
    summary = summarize(
        rows,
        {"global": {}, "by_exchange": []},
        "2026-04-12T00:00:00Z",
        weak_sector_residual_review={
            "summary": {
                "weak_sector_backlog": {
                    "rows": 1,
                    "direct_sector_apply_allowed_rows": 0,
                    "official_sector_candidate_rows": 0,
                    "scope_decision_required_rows": 0,
                    "masterfile_without_sector_rows": 1,
                    "venue_taxonomy_source_required_rows": 0,
                    "source_gate": "Weak-sector enrichment remains blocked without listing-keyed official evidence.",
                },
                "venue_backlog_exchange_queue_totals": {
                    "CSE_LK": {"official_masterfile_without_sector_source_gap": 1}
                },
                "top_weak_sector_resolution_review_batches": [
                    {
                        "exchange": "CSE_LK",
                        "recommended_next_source": "Updated official masterfile or issuer taxonomy exposing sector.",
                        "source_gate": "Keep sector blank until an official masterfile exposes sector.",
                    }
                ],
            }
        },
    )

    action = summary["next_actions"][0]
    assert action["exchange"] == "CSE_LK"
    assert action["field"] == FIELD_MISSING_STOCK_SECTOR
    assert action["review_needed"] is True
    assert action["residual_gate"] == "weak_sector_residual_review_blocks_direct_apply"
    assert action["direct_sector_apply_allowed_rows"] == 0
    assert action["exchange_official_masterfile_without_sector_rows"] == 1
    assert "Updated official masterfile" in action["recommended_source"]


def test_source_gap_classification_context_gates_uncovered_next_action():
    rows = build_completion_backlog(
        [{"ticker": "A", "exchange": "LSE", "asset_type": "Stock", "sector": ""}],
        [],
        {"by_exchange": [{"exchange": "LSE", "venue_status": "official_full", "official_source_count": 3}]},
    )
    summary = summarize(
        rows,
        {"global": {}, "by_exchange": []},
        "2026-04-12T00:00:00Z",
        source_gap_classification={
            "summary": {
                "top_source_gap_review_batches": [
                    {
                        "field": "missing_sector_stock",
                        "gap_class": "official_industry_taxonomy_unavailable_gap",
                        "exchange": "LSE",
                        "rows": 1,
                        "recommended_next_source": "Implemented official venue source layer; residual needs a stronger taxonomy source.",
                        "source_gate": "Keep stock_sector blank until an official taxonomy source exposes a mappable industry value.",
                    }
                ]
            }
        },
    )

    action = summary["next_actions"][0]
    assert action["exchange"] == "LSE"
    assert action["field"] == FIELD_MISSING_STOCK_SECTOR
    assert action["review_needed"] is True
    assert action["residual_gate"] == "source_gap_classification_blocks_direct_apply"
    assert action["source_gap_class"] == "official_industry_taxonomy_unavailable_gap"
    assert action["source_gap_rows"] == 1
    assert "stronger taxonomy source" in action["recommended_source"]


def test_completion_backlog_ranks_by_missing_count_before_static_source_order():
    rows = build_completion_backlog(
        [],
        [
            {"exchange": "TSE", "asset_type": "Stock", "scope_reason": "primary_listing_missing_isin"},
            {"exchange": "TSX", "asset_type": "Stock", "scope_reason": "primary_listing_missing_isin"},
            {"exchange": "TSX", "asset_type": "Stock", "scope_reason": "primary_listing_missing_isin"},
            {"exchange": "TSX", "asset_type": "Stock", "scope_reason": "primary_listing_missing_isin"},
        ],
        {"by_exchange": []},
    )

    assert row_for(rows, exchange="TSX", field=FIELD_MISSING_ISIN).priority_rank == 1
    assert row_for(rows, exchange="TSE", field=FIELD_MISSING_ISIN).priority_rank == 2

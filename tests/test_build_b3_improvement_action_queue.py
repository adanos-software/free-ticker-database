from scripts.build_b3_improvement_action_queue import build_action_rows, summarize


def test_build_action_rows_consolidates_b3_campaigns() -> None:
    rows = build_action_rows(
        masterfile_gap_rows=[
            {
                "listing_key": "B3::ADMF3",
                "asset_type": "Stock",
                "b3_gap_category": "local_share_line",
                "b3_resolution_queue": "absent_from_all_b3_sources_local_share_source_gap",
                "official_subset_closure_eligibility": "blocked_until_current_official_active_source_evidence",
                "review_priority": "P3",
                "review_strategy": "keep_local_share_gap_until_current_official_b3_or_issuer_evidence",
                "verification_evidence_required": "new_current_b3_directory_or_official_issuer_exchange_evidence_for_exact_listing_key",
                "recommended_next_source": "Current B3 exchange directory, B3 issuer page, CVM filing, or issuer investor-relations listing evidence.",
                "source_gate": "Keep row as source gap until current official B3 or issuer evidence proves the active local-share listing.",
            }
        ],
        residual_isin_rows=[
            {
                "listing_key": "B3::AFOF11",
                "asset_type": "ETF",
                "gap_class": "fund_or_trust_identifier_gap",
                "review_bucket": "scope_review_before_identifier_fill",
                "review_priority": "P1",
                "review_strategy": "decide_b3_core_extended_or_exclude_before_identifier_work",
                "verification_evidence_required": "scope_decision_for_core_extended_or_exclude_before_identifier_fill",
                "recommended_next_source": "Current B3 source plus reviewed core, extended, or exclude scope decision before identifier work.",
                "source_gate": "No ISIN fill until the B3 listing is reviewed as core, extended, or excluded.",
            }
        ],
        residual_sector_rows=[
            {
                "listing_key": "B3::2WAV3",
                "asset_type": "Stock",
                "gap_class": "official_industry_taxonomy_unavailable_gap",
                "review_bucket": "no_b3_classification_code_match_source_gap",
                "review_priority": "P3",
                "review_strategy": "keep_blank_until_stronger_official_b3_or_issuer_taxonomy",
                "verification_evidence_required": "stronger_official_b3_or_issuer_taxonomy_source_with_exact_listing_match",
                "recommended_next_source": "Stronger official B3 or issuer taxonomy source exposing sector for the exact listing.",
                "source_gate": "Keep stock_sector blank until official B3 or issuer taxonomy evidence matches the exact listing.",
            }
        ],
    )

    assert rows[0]["campaign"] == "b3_residual_isin"
    assert rows[0]["action_queue"] == "decide_scope_before_identifier_enrichment"
    assert rows[0]["rows"] == "1"
    assert rows[0]["example_listing_keys"] == "B3::AFOF11"
    assert rows[1]["campaign"] == "b3_masterfile_gap"
    assert rows[1]["action_queue"] == "seek_current_b3_or_issuer_listing_evidence"
    assert rows[2]["campaign"] == "b3_residual_sector"
    assert rows[2]["action_queue"] == "seek_stronger_official_b3_or_issuer_taxonomy"


def test_build_action_rows_marks_closure_ready_subset_as_no_data_change() -> None:
    rows = build_action_rows(
        masterfile_gap_rows=[
            {
                "listing_key": "B3::BIAU39",
                "asset_type": "ETF",
                "b3_gap_category": "bdr_or_foreign_receipt",
                "b3_resolution_queue": "official_bdr_subset_without_category_source_gap_closed",
                "official_subset_closure_eligibility": "closure_ready_official_subset_bdr_without_category_source_gap",
                "review_priority": "P2",
                "review_strategy": "close_bdr_subset_gap_without_data_change_keep_category_source_gap",
                "verification_evidence_required": "official_b3_source_row_plus_scope_decision_or_parser_fix_before_reclassifying_gap",
                "recommended_next_source": "Official B3 BDR/ETF subset confirms the listing; keep category/ISIN unchanged until stronger B3 or issuer evidence exposes them.",
                "source_gate": "No B3 category, ISIN, name, symbol, or scope change is authorized.",
            }
        ],
        residual_isin_rows=[],
        residual_sector_rows=[],
    )

    assert rows == [
        {
            "campaign": "b3_masterfile_gap",
            "priority": "P2",
            "review_queue": "official_bdr_subset_without_category_source_gap_closed_no_data_change_closure",
            "asset_type": "ETF",
            "gap_class": "bdr_or_foreign_receipt",
            "rows": "1",
            "action_queue": "document_official_subset_closure_without_data_change",
            "review_strategy": "close_bdr_subset_gap_without_data_change_keep_category_source_gap",
            "evidence_required": "official_b3_source_row_plus_scope_decision_or_parser_fix_before_reclassifying_gap",
            "recommended_next_source": (
                "Official B3 BDR/ETF subset confirms the listing; keep category/ISIN unchanged until stronger "
                "B3 or issuer evidence exposes them."
            ),
            "source_gate": "No B3 category, ISIN, name, symbol, or scope change is authorized.",
            "example_listing_keys": "B3::BIAU39",
        }
    ]


def test_summarize_keeps_b3_coverage_context_and_blocks_data_changes() -> None:
    action_rows = [
        {
            "campaign": "b3_residual_isin",
            "priority": "P1",
            "review_queue": "scope_review_before_identifier_fill",
            "action_queue": "decide_scope_before_identifier_enrichment",
            "rows": "7",
        },
        {
            "campaign": "b3_residual_sector",
            "priority": "P3",
            "review_queue": "no_b3_classification_code_match_source_gap",
            "action_queue": "seek_stronger_official_b3_or_issuer_taxonomy",
            "rows": "194",
        },
    ]
    payload = {
        "summary": {
            "coverage_snapshot": {"active_directory_match_rate": 97.47},
            "coverage_diagnosis": {"status": "active_directory_coverage_high_with_reviewable_residuals"},
        }
    }

    summary = summarize(action_rows=action_rows, masterfile_gap_payload=payload, generated_at="2026-05-29T00:00:00Z")

    assert summary["generated_at"] == "2026-05-29T00:00:00Z"
    assert summary["batches"] == 2
    assert summary["underlying_review_rows"] == 201
    assert summary["campaign_totals"] == {"b3_residual_isin": 7, "b3_residual_sector": 194}
    assert summary["direct_data_change_authorized"] is False
    assert summary["b3_coverage_snapshot"] == {"active_directory_match_rate": 97.47}
    assert summary["b3_coverage_diagnosis"] == {"status": "active_directory_coverage_high_with_reviewable_residuals"}

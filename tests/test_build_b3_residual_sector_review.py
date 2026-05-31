from scripts.build_b3_residual_sector_review import (
    b3_code_shape,
    build_json_payload,
    build_review_rows,
    official_source_context_for,
    recommended_next_source_for,
    residual_review_context_for,
    review_strategy_for,
    source_gate_for,
    source_gap_context_for,
    summarize,
)


def test_b3_code_shape_classifies_alphanumeric_roots_without_guessing_sector() -> None:
    assert b3_code_shape("") == "missing_b3_code"
    assert b3_code_shape("MISS") == "alpha_b3_code"
    assert b3_code_shape("A6OP") == "alphanumeric_b3_code"


def test_build_review_rows_keeps_b3_sector_residuals_listing_keyed() -> None:
    rows = build_review_rows(
        tickers=[
            {
                "ticker": "MISS3",
                "exchange": "B3",
                "asset_type": "Stock",
                "name": "Missing Sector SA",
                "stock_sector": "",
            }
        ],
        source_gap_rows=[
            {
                "field": "missing_sector_stock",
                "listing_key": "B3::MISS3",
                "ticker": "MISS3",
                "exchange": "B3",
                "asset_type": "Stock",
                "name": "Missing Sector SA",
                "gap_class": "official_industry_taxonomy_unavailable_gap",
                "review_needed": "true",
                "recommended_next_source": "Official issuer taxonomy.",
                "source_gate": "Exact B3 listing and canonical sector.",
            },
            {
                "field": "missing_isin_primary",
                "listing_key": "B3::MISS3",
                "ticker": "MISS3",
                "exchange": "B3",
                "asset_type": "Stock",
            },
        ],
        source_of_truth_rows=[
            {
                "field": "missing_sector_stock",
                "listing_key": "B3::MISS3",
                "source_of_truth_outcome": "accepted_source_gap",
            }
        ],
        b3_sector_probe_rows=[
            {
                "listing_key": "B3::MISS3",
                "ticker": "MISS3",
                "decision": "no_b3_code_match",
                "b3_code": "MISS",
            }
        ],
    )

    assert rows == [
        {
            "listing_key": "B3::MISS3",
            "ticker": "MISS3",
            "exchange": "B3",
            "asset_type": "Stock",
            "name": "Missing Sector SA",
            "gap_class": "official_industry_taxonomy_unavailable_gap",
            "source_of_truth_outcome": "accepted_source_gap",
            "source_gap_context": (
                "gap_class=official_industry_taxonomy_unavailable_gap;"
                "source_of_truth_outcome=accepted_source_gap;review_needed=true"
            ),
            "review_needed": "true",
            "recommended_next_source": (
                "Stronger official B3 or issuer taxonomy source exposing sector for the exact listing."
            ),
            "source_gate": (
                "Keep stock_sector blank until official B3 or issuer taxonomy evidence matches the exact listing."
            ),
            "b3_probe_decision": "no_b3_code_match",
            "b3_code": "MISS",
            "b3_name": "",
            "b3_sector_pt": "",
            "b3_subsector": "",
            "b3_segment": "",
            "b3_sector_update": "",
            "b3_source_url": "",
            "official_source_context": (
                "b3_probe_decision=no_b3_code_match;b3_code=MISS;"
                "b3_sector_update=none;b3_source_url=none"
            ),
            "residual_decision": "accepted_source_gap_no_b3_classification_code_match",
            "review_bucket": "no_b3_classification_code_match_source_gap",
            "review_priority": "P3",
            "review_strategy": "keep_blank_until_stronger_official_b3_or_issuer_taxonomy",
            "apply_eligibility": "source_gap_keep_blank_until_official_taxonomy_evidence",
            "verification_evidence_required": "stronger_official_b3_or_issuer_taxonomy_source_with_exact_listing_match",
            "residual_review_context": (
                "residual_decision=accepted_source_gap_no_b3_classification_code_match;"
                "review_bucket=no_b3_classification_code_match_source_gap;"
                "apply_eligibility=source_gap_keep_blank_until_official_taxonomy_evidence;"
                "verification_evidence_required=stronger_official_b3_or_issuer_taxonomy_source_with_exact_listing_match"
            ),
        }
    ]


def test_build_review_rows_flags_available_official_sector() -> None:
    rows = build_review_rows(
        tickers=[],
        source_gap_rows=[
            {
                "field": "missing_sector_stock",
                "listing_key": "B3::GOOD3",
                "ticker": "GOOD3",
                "exchange": "B3",
                "asset_type": "Stock",
                "name": "Good SA",
                "gap_class": "official_industry_taxonomy_unavailable_gap",
            }
        ],
        source_of_truth_rows=[],
        b3_sector_probe_rows=[
            {
                "listing_key": "B3::GOOD3",
                "ticker": "GOOD3",
                "decision": "accept",
                "sector_update": "Utilities",
            }
        ],
    )

    assert rows[0]["residual_decision"] == "official_b3_sector_available_rebuild_required"
    assert rows[0]["review_bucket"] == "official_sector_candidate_requires_apply_gate"
    assert rows[0]["review_priority"] == "P1"
    assert rows[0]["review_strategy"] == "validate_official_b3_taxonomy_candidate_before_sector_apply"
    assert rows[0]["apply_eligibility"] == "apply_only_after_listing_key_taxonomy_and_canonical_sector_validation"
    assert rows[0]["verification_evidence_required"] == "official_b3_taxonomy_with_exact_listing_key_b3_code_and_canonical_sector_mapping"
    assert rows[0]["source_gap_context"] == source_gap_context_for(rows[0])
    assert rows[0]["official_source_context"] == official_source_context_for(rows[0])
    assert rows[0]["residual_review_context"] == residual_review_context_for(rows[0])


def test_summarize_counts_probe_decisions() -> None:
    summary = summarize(
        [
            {
                "gap_class": "official_industry_taxonomy_unavailable_gap",
                "residual_decision": "accepted_source_gap_no_b3_classification_code_match",
                "review_bucket": "no_b3_classification_code_match_source_gap",
                "review_priority": "P3",
                "apply_eligibility": "source_gap_keep_blank_until_official_taxonomy_evidence",
                "verification_evidence_required": "stronger_official_b3_or_issuer_taxonomy_source_with_exact_listing_match",
                "b3_probe_decision": "no_b3_code_match",
                "b3_code": "A6OP",
            },
            {
                "gap_class": "official_industry_taxonomy_unavailable_gap",
                "residual_decision": "accepted_source_gap_missing_b3_sector_probe_row",
                "review_bucket": "missing_probe_row_requires_parser_or_source_review",
                "review_priority": "P2",
                "apply_eligibility": "source_gap_keep_blank_until_official_taxonomy_evidence",
                "verification_evidence_required": "parser_or_source_refresh_to_produce_b3_classification_probe_row",
                "b3_probe_decision": "",
                "b3_code": "",
            },
        ],
        "2026-05-24T00:00:00Z",
    )

    assert summary["rows"] == 2
    assert summary["gap_class_totals"] == {"official_industry_taxonomy_unavailable_gap": 2}
    assert summary["b3_probe_decision_totals"] == {
        "missing_probe_row": 1,
        "no_b3_code_match": 1,
    }
    assert summary["b3_code_shape_totals"] == {
        "alphanumeric_b3_code": 1,
        "missing_b3_code": 1,
    }
    assert summary["alphanumeric_b3_code_rows"] == 1
    assert summary["alphanumeric_b3_code_examples"] == [
        {
            "listing_key": "",
            "ticker": "",
            "b3_code": "A6OP",
            "b3_probe_decision": "no_b3_code_match",
        }
    ]
    assert summary["review_bucket_totals"] == {
        "missing_probe_row_requires_parser_or_source_review": 1,
        "no_b3_classification_code_match_source_gap": 1,
    }
    assert summary["review_priority_totals"] == {"P2": 1, "P3": 1}
    assert summary["apply_eligibility_totals"] == {"source_gap_keep_blank_until_official_taxonomy_evidence": 2}
    assert summary["verification_evidence_required_totals"] == {
        "parser_or_source_refresh_to_produce_b3_classification_probe_row": 1,
        "stronger_official_b3_or_issuer_taxonomy_source_with_exact_listing_match": 1,
    }
    assert summary["review_strategy_totals"] == {
        "keep_blank_until_stronger_official_b3_or_issuer_taxonomy": 1,
        "refresh_b3_taxonomy_probe_or_parser_before_sector_work": 1,
    }
    assert summary["top_b3_sector_review_batches"] == [
        {
            "review_priority": "P2",
            "review_bucket": "missing_probe_row_requires_parser_or_source_review",
            "gap_class": "official_industry_taxonomy_unavailable_gap",
            "b3_code_shape": "missing_b3_code",
            "asset_type": "",
            "rows": 1,
            "review_strategy": "refresh_b3_taxonomy_probe_or_parser_before_sector_work",
            "verification_evidence_required": "parser_or_source_refresh_to_produce_b3_classification_probe_row",
            "recommended_next_source": (
                "Refreshed official B3 taxonomy source or parser output that produces a listing-keyed probe row."
            ),
            "source_gate": (
                "Keep sector blank until the official parser/source produces a listing-keyed taxonomy probe row."
            ),
        },
        {
            "review_priority": "P3",
            "review_bucket": "no_b3_classification_code_match_source_gap",
            "gap_class": "official_industry_taxonomy_unavailable_gap",
            "b3_code_shape": "alphanumeric_b3_code",
            "asset_type": "",
            "rows": 1,
            "review_strategy": "keep_blank_until_stronger_official_b3_or_issuer_taxonomy",
            "verification_evidence_required": "stronger_official_b3_or_issuer_taxonomy_source_with_exact_listing_match",
            "recommended_next_source": "Stronger official B3 or issuer taxonomy source exposing sector for the exact listing.",
            "source_gate": (
                "Keep stock_sector blank until official B3 or issuer taxonomy evidence matches the exact listing."
            ),
        },
    ]


def test_review_strategy_for_keeps_b3_sector_no_guessing_gate_explicit() -> None:
    assert (
        review_strategy_for("no_b3_classification_code_match_source_gap")
        == "keep_blank_until_stronger_official_b3_or_issuer_taxonomy"
    )
    assert (
        review_strategy_for("ambiguous_b3_code_requires_manual_review")
        == "manual_b3_code_disambiguation_before_taxonomy_mapping"
    )
    assert (
        recommended_next_source_for("no_b3_classification_code_match_source_gap")
        == "Stronger official B3 or issuer taxonomy source exposing sector for the exact listing."
    )
    assert (
        source_gate_for("ambiguous_b3_code_requires_manual_review")
        == "Keep sector blank until manual disambiguation proves the exact B3 code/listing match."
    )


def test_build_json_payload_includes_source_traceability_metadata() -> None:
    summary = summarize([], "2026-05-24T00:00:00Z")
    payload = build_json_payload(
        summary=summary,
        rows=[],
        source_files={
            "tickers_csv": "data/tickers.csv",
            "source_gap_classification_csv": "data/reports/source_gap_classification.csv",
            "source_of_truth_decisions_csv": "data/reports/source_of_truth_decisions.csv",
            "b3_sector_probe_csv": "data/b3_verification/sector_classification_backfill.csv",
        },
    )

    assert payload["_meta"]["generated_at"] == "2026-05-24T00:00:00Z"
    assert payload["_meta"]["source_files"]["b3_sector_probe_csv"] == "data/b3_verification/sector_classification_backfill.csv"
    assert payload["_meta"]["policy"] == summary["policy"]
    assert payload["summary"] == summary
    assert payload["rows"] == []

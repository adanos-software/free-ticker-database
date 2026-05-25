from scripts.build_asx_residual_review import build_json_payload, build_review_rows, summarize


def test_build_review_rows_flags_official_asx_isin_available() -> None:
    rows = build_review_rows(
        tickers=[],
        masterfile_rows=[],
        source_gap_rows=[
            {
                "field": "missing_isin_primary",
                "target_field": "isin",
                "listing_key": "ASX::ABC",
                "ticker": "ABC",
                "exchange": "ASX",
                "asset_type": "Stock",
                "name": "ABC Limited",
                "gap_class": "official_identifier_reference_unmatched_gap",
            }
        ],
        source_of_truth_rows=[],
        asx_isin_probe_rows=[
            {
                "ticker": "ABC",
                "decision": "accept",
                "asx_isin": "AU000000ABC1",
                "asx_security_type": "Fully Paid Ordinary",
            }
        ],
    )

    assert rows[0]["asx_isin_probe_decision"] == "accept"
    assert rows[0]["asx_isin_probe_isin"] == "AU000000ABC1"
    assert rows[0]["asx_resolution_queue"] == "official_asx_isin_candidate_apply_gate"
    assert rows[0]["residual_decision"] == "official_asx_isin_available_review_apply"
    assert rows[0]["review_bucket"] == "official_isin_candidate_requires_apply_gate"
    assert rows[0]["review_priority"] == "P1"
    assert rows[0]["review_strategy"] == "apply_candidate_only_after_asx_isin_workbook_gates"
    assert rows[0]["apply_eligibility"] == "apply_only_after_asx_code_name_token_and_isin_checksum_validation"
    assert rows[0]["verification_evidence_required"] == "official_asx_isin_workbook_exact_code_name_numeric_token_and_valid_isin_checksum"


def test_build_review_rows_keeps_core_exclusion_candidates_blank() -> None:
    rows = build_review_rows(
        tickers=[],
        masterfile_rows=[],
        source_gap_rows=[
            {
                "field": "missing_isin_primary",
                "listing_key": "ASX::RM1",
                "ticker": "RM1",
                "exchange": "ASX",
                "asset_type": "ETF",
                "name": "NATIONAL RMBS TRUST 2024-1",
                "gap_class": "debt_or_securitized_identifier_gap",
            }
        ],
        source_of_truth_rows=[
            {
                "field": "missing_isin_primary",
                "listing_key": "ASX::RM1",
                "exchange": "ASX",
                "source_of_truth_outcome": "core_exclusion_candidate",
                "core_action": "review_for_core_exclusion",
                "fill_action": "do_not_fill_until_scope_review",
            }
        ],
        asx_isin_probe_rows=[],
    )

    assert rows[0]["source_of_truth_outcome"] == "core_exclusion_candidate"
    assert rows[0]["asx_resolution_queue"] == "core_exclusion_candidate_identifier_scope_review"
    assert rows[0]["residual_decision"] == "core_exclusion_candidate_requires_scope_review"
    assert rows[0]["review_bucket"] == "scope_review_before_any_data_fill"
    assert rows[0]["review_priority"] == "P1"
    assert rows[0]["review_strategy"] == "scope_review_before_asx_identifier_enrichment"
    assert rows[0]["apply_eligibility"] == "blocked_until_core_or_extended_scope_decision"
    assert rows[0]["verification_evidence_required"] == "scope_decision_for_core_extended_or_exclude_before_identifier_or_category_fill"
    assert rows[0]["recommended_next_action"] == "review_scope_before_identifier_or_category_enrichment"


def test_build_review_rows_tracks_etf_category_taxonomy_gap() -> None:
    rows = build_review_rows(
        tickers=[],
        masterfile_rows=[
            {
                "source_key": "asx_investment_products",
                "ticker": "VCD",
                "exchange": "ASX",
                "asset_type": "ETF",
                "listing_status": "active",
                "official": "true",
            }
        ],
        source_gap_rows=[
            {
                "field": "missing_etf_category",
                "target_field": "etf_category",
                "listing_key": "ASX::VCD",
                "ticker": "VCD",
                "exchange": "ASX",
                "asset_type": "ETF",
                "name": "VICINITY CENTRES TRUST",
                "gap_class": "official_product_taxonomy_unavailable_gap",
            }
        ],
        source_of_truth_rows=[
            {
                "field": "missing_etf_category",
                "listing_key": "ASX::VCD",
                "exchange": "ASX",
                "source_of_truth_outcome": "accepted_source_gap",
            }
        ],
        asx_isin_probe_rows=[],
    )

    assert rows[0]["official_masterfile_match"] == "true"
    assert rows[0]["official_masterfile_sources"] == "asx_investment_products"
    assert rows[0]["asx_resolution_queue"] == "missing_etf_category_official_taxonomy_unavailable"
    assert rows[0]["residual_decision"] == "accepted_source_gap_official_product_taxonomy_unavailable"
    assert rows[0]["review_bucket"] == "product_taxonomy_source_gap"
    assert rows[0]["review_priority"] == "P2"
    assert rows[0]["review_strategy"] == "keep_category_blank_until_asx_product_taxonomy_source"
    assert rows[0]["apply_eligibility"] == "keep_category_blank_until_official_product_taxonomy_source"
    assert rows[0]["verification_evidence_required"] == "official_or_reviewed_product_taxonomy_with_exact_listing_match"


def test_build_review_rows_flags_name_mismatch_as_non_apply_gap() -> None:
    rows = build_review_rows(
        tickers=[],
        masterfile_rows=[],
        source_gap_rows=[
            {
                "field": "missing_isin_primary",
                "listing_key": "ASX::XYZ",
                "ticker": "XYZ",
                "exchange": "ASX",
                "asset_type": "Stock",
                "name": "XYZ Limited",
                "gap_class": "official_identifier_reference_unmatched_gap",
            }
        ],
        source_of_truth_rows=[],
        asx_isin_probe_rows=[
            {
                "ticker": "XYZ",
                "decision": "name_mismatch",
                "asx_isin": "AU000000XYZ1",
                "asx_security_type": "Fully Paid Ordinary",
            }
        ],
    )

    assert rows[0]["asx_resolution_queue"] == "asx_isin_workbook_name_mismatch_manual_review"
    assert rows[0]["residual_decision"] == "accepted_source_gap_requires_exact_name_review"
    assert rows[0]["review_bucket"] == "identity_mismatch_requires_manual_review"
    assert rows[0]["review_priority"] == "P1"
    assert rows[0]["review_strategy"] == "manual_identity_review_before_asx_isin_apply"
    assert rows[0]["apply_eligibility"] == "blocked_until_identity_mismatch_resolved"
    assert rows[0]["verification_evidence_required"] == "manual_exact_name_or_alias_resolution_before_isin_apply"
    assert rows[0]["recommended_next_action"] == "do_not_apply_until_name_mismatch_is_manually_resolved"


def test_summarize_counts_asx_residuals() -> None:
    summary = summarize(
        [
            {
                "field": "missing_isin_primary",
                "asset_type": "Stock",
                "gap_class": "official_identifier_reference_unmatched_gap",
                "source_of_truth_outcome": "accepted_source_gap",
                "asx_resolution_queue": "missing_isin_not_in_current_asx_isin_workbook",
                "residual_decision": "accepted_source_gap_not_in_current_asx_isin_workbook",
                "review_bucket": "identifier_source_gap",
                "review_priority": "P3",
                "review_strategy": "keep_isin_blank_until_current_asx_or_registry_source",
                "apply_eligibility": "keep_identifier_blank_until_direct_registry_issuer_or_official_asx_evidence",
                "verification_evidence_required": "direct_registry_issuer_or_official_asx_identifier_source_with_exact_listing_match",
                "asx_isin_probe_decision": "no_asx_match",
                "official_masterfile_match": "true",
                "official_masterfile_exposes_isin": "false",
                "official_masterfile_exposes_sector": "false",
                "official_masterfile_sources": "asx_listed_companies",
            },
            {
                "field": "missing_etf_category",
                "asset_type": "ETF",
                "gap_class": "official_product_taxonomy_unavailable_gap",
                "source_of_truth_outcome": "accepted_source_gap",
                "asx_resolution_queue": "missing_etf_category_official_taxonomy_unavailable",
                "residual_decision": "accepted_source_gap_official_product_taxonomy_unavailable",
                "review_bucket": "product_taxonomy_source_gap",
                "review_priority": "P2",
                "review_strategy": "keep_category_blank_until_asx_product_taxonomy_source",
                "apply_eligibility": "keep_category_blank_until_official_product_taxonomy_source",
                "verification_evidence_required": "official_or_reviewed_product_taxonomy_with_exact_listing_match",
                "asx_isin_probe_decision": "",
                "official_masterfile_match": "true",
                "official_masterfile_exposes_isin": "false",
                "official_masterfile_exposes_sector": "false",
                "official_masterfile_sources": "asx_investment_products",
            },
            {
                "listing_key": "ASX::RM1",
                "ticker": "RM1",
                "field": "missing_isin_primary",
                "asset_type": "ETF",
                "gap_class": "debt_or_securitized_identifier_gap",
                "source_of_truth_outcome": "core_exclusion_candidate",
                "asx_resolution_queue": "core_exclusion_candidate_identifier_scope_review",
                "residual_decision": "core_exclusion_candidate_requires_scope_review",
                "review_bucket": "scope_review_before_any_data_fill",
                "review_priority": "P1",
                "review_strategy": "scope_review_before_asx_identifier_enrichment",
                "apply_eligibility": "blocked_until_core_or_extended_scope_decision",
                "verification_evidence_required": "scope_decision_for_core_extended_or_exclude_before_identifier_or_category_fill",
                "asx_isin_probe_decision": "no_asx_match",
                "official_masterfile_match": "true",
                "official_masterfile_exposes_isin": "false",
                "official_masterfile_exposes_sector": "false",
                "official_masterfile_sources": "asx_listed_companies",
                "recommended_next_action": "review_scope_before_identifier_or_category_enrichment",
            },
        ],
        "2026-05-24T00:00:00Z",
    )

    assert summary["rows"] == 3
    assert summary["field_totals"] == {"missing_etf_category": 1, "missing_isin_primary": 2}
    assert summary["asx_residual_backlog"] == {
        "status": "review_only_scope_identifier_or_product_taxonomy_source_gaps",
        "rows": 3,
        "scope_decision_required_rows": 1,
        "identity_review_required_rows": 0,
        "official_identifier_source_required_rows": 1,
        "official_product_taxonomy_required_rows": 1,
        "official_isin_apply_candidate_rows": 0,
        "direct_data_apply_allowed_rows": 0,
        "metadata_enrichment_authorized": False,
        "source_gate": (
            "ASX residual work remains blocked unless exact ASX workbook, registry, issuer, trustee, prospectus, "
            "or reviewed product-taxonomy evidence proves the value; no ticker/name or peer-instrument inference."
        ),
    }
    assert summary["asx_residual_backlog_queue_totals"] == {
        "core_exclusion_candidate_identifier_scope_review": 1,
        "missing_etf_category_official_taxonomy_unavailable": 1,
        "missing_isin_not_in_current_asx_isin_workbook": 1,
    }
    assert summary["asx_residual_backlog_evidence_required_totals"] == {
        "direct_registry_issuer_or_official_asx_identifier_source_with_exact_listing_match": 1,
        "official_or_reviewed_product_taxonomy_with_exact_listing_match": 1,
        "scope_decision_for_core_extended_or_exclude_before_identifier_or_category_fill": 1,
    }
    assert summary["core_exclusion_candidate_rows"] == 1
    assert summary["core_exclusion_candidate_field_totals"] == {"missing_isin_primary": 1}
    assert summary["core_exclusion_candidate_asset_type_totals"] == {"ETF": 1}
    assert summary["core_exclusion_candidate_gap_class_totals"] == {"debt_or_securitized_identifier_gap": 1}
    assert summary["core_exclusion_candidate_resolution_queue_totals"] == {
        "core_exclusion_candidate_identifier_scope_review": 1
    }
    assert summary["core_exclusion_candidate_official_source_totals"] == {"asx_listed_companies": 1}
    assert summary["core_exclusion_candidate_official_capability_totals"] == {
        "masterfile_exposes_isin=false": 1,
        "masterfile_exposes_sector=false": 1,
        "masterfile_match=true": 1,
    }
    assert summary["core_exclusion_candidate_examples"] == [
        {
            "listing_key": "ASX::RM1",
            "ticker": "RM1",
            "asset_type": "ETF",
            "field": "missing_isin_primary",
            "gap_class": "debt_or_securitized_identifier_gap",
            "asx_resolution_queue": "core_exclusion_candidate_identifier_scope_review",
            "official_masterfile_sources": "asx_listed_companies",
            "official_capability": (
                "masterfile_match=true;masterfile_exposes_isin=false;masterfile_exposes_sector=false"
            ),
            "source_gate": "No ISIN or category enrichment until the listing is reviewed as core, extended, or excluded.",
            "recommended_next_action": "review_scope_before_identifier_or_category_enrichment",
        }
    ]
    assert summary["review_bucket_totals"] == {
        "identifier_source_gap": 1,
        "product_taxonomy_source_gap": 1,
        "scope_review_before_any_data_fill": 1,
    }
    assert summary["asx_resolution_queue_totals"] == {
        "core_exclusion_candidate_identifier_scope_review": 1,
        "missing_etf_category_official_taxonomy_unavailable": 1,
        "missing_isin_not_in_current_asx_isin_workbook": 1,
    }
    assert summary["asx_resolution_queue_field_totals"] == {
        "core_exclusion_candidate_identifier_scope_review": {"missing_isin_primary": 1},
        "missing_etf_category_official_taxonomy_unavailable": {"missing_etf_category": 1},
        "missing_isin_not_in_current_asx_isin_workbook": {"missing_isin_primary": 1},
    }
    assert summary["asx_resolution_queue_gap_class_totals"] == {
        "core_exclusion_candidate_identifier_scope_review": {"debt_or_securitized_identifier_gap": 1},
        "missing_etf_category_official_taxonomy_unavailable": {"official_product_taxonomy_unavailable_gap": 1},
        "missing_isin_not_in_current_asx_isin_workbook": {"official_identifier_reference_unmatched_gap": 1},
    }
    assert summary["asx_resolution_queue_official_source_totals"] == {
        "core_exclusion_candidate_identifier_scope_review": {"asx_listed_companies": 1},
        "missing_etf_category_official_taxonomy_unavailable": {"asx_investment_products": 1},
        "missing_isin_not_in_current_asx_isin_workbook": {"asx_listed_companies": 1},
    }
    assert summary["asx_resolution_queue_review_strategy_totals"] == {
        "core_exclusion_candidate_identifier_scope_review": {
            "scope_review_before_asx_identifier_enrichment": 1,
        },
        "missing_etf_category_official_taxonomy_unavailable": {
            "keep_category_blank_until_asx_product_taxonomy_source": 1,
        },
        "missing_isin_not_in_current_asx_isin_workbook": {
            "keep_isin_blank_until_current_asx_or_registry_source": 1,
        },
    }
    assert summary["asx_resolution_queue_evidence_required_totals"] == {
        "core_exclusion_candidate_identifier_scope_review": {
            "scope_decision_for_core_extended_or_exclude_before_identifier_or_category_fill": 1,
        },
        "missing_etf_category_official_taxonomy_unavailable": {
            "official_or_reviewed_product_taxonomy_with_exact_listing_match": 1,
        },
        "missing_isin_not_in_current_asx_isin_workbook": {
            "direct_registry_issuer_or_official_asx_identifier_source_with_exact_listing_match": 1,
        },
    }
    assert summary["asx_resolution_queue_official_capability_totals"] == {
        "core_exclusion_candidate_identifier_scope_review": {
            "masterfile_exposes_isin=false": 1,
            "masterfile_exposes_sector=false": 1,
            "masterfile_match=true": 1,
        },
        "missing_etf_category_official_taxonomy_unavailable": {
            "masterfile_exposes_isin=false": 1,
            "masterfile_exposes_sector=false": 1,
            "masterfile_match=true": 1,
        },
        "missing_isin_not_in_current_asx_isin_workbook": {
            "masterfile_exposes_isin=false": 1,
            "masterfile_exposes_sector=false": 1,
            "masterfile_match=true": 1,
        },
    }
    assert summary["top_asx_resolution_review_batches"] == [
        {
            "asx_resolution_queue": "core_exclusion_candidate_identifier_scope_review",
            "field": "missing_isin_primary",
            "official_source": "asx_listed_companies",
            "rows": 1,
            "review_strategy": "scope_review_before_asx_identifier_enrichment",
            "evidence_required": "scope_decision_for_core_extended_or_exclude_before_identifier_or_category_fill",
            "recommended_next_source": (
                "asx_listed_companies plus reviewed scope decision for core, extended, or exclude before identifier work."
            ),
            "source_gate": "No ISIN or category enrichment until the listing is reviewed as core, extended, or excluded.",
        },
        {
            "asx_resolution_queue": "missing_etf_category_official_taxonomy_unavailable",
            "field": "missing_etf_category",
            "official_source": "asx_investment_products",
            "rows": 1,
            "review_strategy": "keep_category_blank_until_asx_product_taxonomy_source",
            "evidence_required": "official_or_reviewed_product_taxonomy_with_exact_listing_match",
            "recommended_next_source": (
                "Official ASX product taxonomy, issuer/sponsor product page, PDS, or reviewed product taxonomy source."
            ),
            "source_gate": "Keep ETF category blank until exact product taxonomy evidence exists.",
        },
        {
            "asx_resolution_queue": "missing_isin_not_in_current_asx_isin_workbook",
            "field": "missing_isin_primary",
            "official_source": "asx_listed_companies",
            "rows": 1,
            "review_strategy": "keep_isin_blank_until_current_asx_or_registry_source",
            "evidence_required": "direct_registry_issuer_or_official_asx_identifier_source_with_exact_listing_match",
            "recommended_next_source": (
                "Current ASX ISIN workbook, registry, issuer, trustee, or prospectus source with exact listing match."
            ),
            "source_gate": (
                "Keep ISIN blank until current ASX, registry, issuer, or trustee evidence proves the identifier."
            ),
        },
    ]
    assert summary["review_priority_totals"] == {"P1": 1, "P2": 1, "P3": 1}
    assert summary["apply_eligibility_totals"] == {
        "blocked_until_core_or_extended_scope_decision": 1,
        "keep_category_blank_until_official_product_taxonomy_source": 1,
        "keep_identifier_blank_until_direct_registry_issuer_or_official_asx_evidence": 1,
    }
    assert summary["verification_evidence_required_totals"] == {
        "direct_registry_issuer_or_official_asx_identifier_source_with_exact_listing_match": 1,
        "official_or_reviewed_product_taxonomy_with_exact_listing_match": 1,
        "scope_decision_for_core_extended_or_exclude_before_identifier_or_category_fill": 1,
    }
    assert summary["asx_isin_probe_decision_totals"] == {"no_asx_match": 2}
    assert summary["official_masterfile_match_totals"] == {"true": 3}
    assert summary["official_masterfile_exposes_isin_totals"] == {"false": 3}
    assert summary["official_masterfile_exposes_sector_totals"] == {"false": 3}
    assert summary["official_masterfile_source_totals"] == {
        "asx_investment_products": 1,
        "asx_listed_companies": 2,
    }


def test_build_json_payload_includes_source_traceability_metadata() -> None:
    summary = summarize([], "2026-05-24T00:00:00Z")
    payload = build_json_payload(
        summary=summary,
        rows=[],
        source_files={
            "tickers_csv": "data/tickers.csv",
            "masterfile_reference_csv": "data/masterfiles/reference.csv",
            "source_gap_classification_csv": "data/reports/source_gap_classification.csv",
            "source_of_truth_decisions_csv": "data/reports/source_of_truth_decisions.csv",
            "asx_isin_probe_csv": "data/asx_verification/missing_isin_backfill.csv",
        },
    )

    assert payload["_meta"]["generated_at"] == "2026-05-24T00:00:00Z"
    assert payload["_meta"]["source_files"]["asx_isin_probe_csv"] == "data/asx_verification/missing_isin_backfill.csv"
    assert payload["_meta"]["policy"] == summary["policy"]
    assert payload["summary"] == summary
    assert payload["rows"] == []

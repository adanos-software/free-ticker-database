from scripts.build_weak_sector_residual_review import (
    apply_eligibility_for,
    build_json_payload,
    build_review_rows,
    summarize,
    verification_evidence_for,
    weak_sector_review_strategy_for,
)


def test_build_review_rows_flags_official_sector_available() -> None:
    rows = build_review_rows(
        tickers=[],
        masterfile_rows=[
            {
                "source_key": "ngx_company_profile_directory",
                "ticker": "ABC",
                "exchange": "NGX",
                "listing_status": "active",
                "official": "true",
                "sector": "Financials",
            }
        ],
        source_gap_rows=[
            {
                "field": "missing_sector_stock",
                "listing_key": "NGX::ABC",
                "ticker": "ABC",
                "exchange": "NGX",
                "asset_type": "Stock",
                "name": "ABC PLC",
                "gap_class": "official_industry_taxonomy_unavailable_gap",
            }
        ],
        source_of_truth_rows=[],
        exchanges={"NGX"},
    )

    assert rows[0]["official_masterfile_exposes_sector"] == "true"
    assert rows[0]["official_masterfile_sector_values"] == "Financials"
    assert rows[0]["weak_sector_resolution_queue"] == "official_sector_candidate_normalization_review"
    assert rows[0]["residual_decision"] == "official_sector_available_review_apply"
    assert rows[0]["review_bucket"] == "official_sector_candidate_requires_normalization_gate"
    assert rows[0]["review_priority"] == "P1"
    assert rows[0]["apply_eligibility"] == "blocked_until_canonical_sector_normalization_and_listing_key_gate"
    assert (
        rows[0]["verification_evidence_required"]
        == "official_venue_sector_value_with_canonical_mapping_and_exact_listing_key_match"
    )
    assert rows[0]["review_strategy"] == "normalize_official_sector_candidate_before_apply"


def test_build_review_rows_tracks_official_masterfile_without_sector() -> None:
    rows = build_review_rows(
        tickers=[],
        masterfile_rows=[
            {
                "source_key": "boursa_kuwait_stocks",
                "ticker": "KFIN",
                "exchange": "BK",
                "listing_status": "active",
                "official": "true",
                "sector": "",
            }
        ],
        source_gap_rows=[
            {
                "field": "missing_sector_stock",
                "listing_key": "BK::KFIN",
                "ticker": "KFIN",
                "exchange": "BK",
                "asset_type": "Stock",
                "name": "Kuwait Finance House",
                "gap_class": "official_industry_taxonomy_unavailable_gap",
            }
        ],
        source_of_truth_rows=[],
        exchanges={"BK"},
    )

    assert rows[0]["official_masterfile_match"] == "true"
    assert rows[0]["weak_sector_resolution_queue"] == "official_masterfile_without_sector_source_gap"
    assert rows[0]["residual_decision"] == "accepted_source_gap_official_masterfile_without_sector"
    assert rows[0]["review_bucket"] == "official_masterfile_without_sector_source_gap"
    assert rows[0]["review_priority"] == "P3"
    assert (
        rows[0]["apply_eligibility"]
        == "keep_sector_blank_until_official_masterfile_exposes_sector_or_reviewed_taxonomy"
    )
    assert rows[0]["review_strategy"] == "keep_blank_until_official_masterfile_or_issuer_sector_source"


def test_build_review_rows_keeps_scope_candidate_before_sector_enrichment() -> None:
    rows = build_review_rows(
        tickers=[],
        masterfile_rows=[],
        source_gap_rows=[
            {
                "field": "missing_sector_stock",
                "listing_key": "PSE::SHELL",
                "ticker": "SHELL",
                "exchange": "PSE",
                "asset_type": "Stock",
                "name": "Shell Company",
                "gap_class": "shell_or_cpc_sector_gap",
            }
        ],
        source_of_truth_rows=[
            {
                "field": "missing_sector_stock",
                "listing_key": "PSE::SHELL",
                "source_of_truth_outcome": "core_exclusion_candidate",
            }
        ],
        exchanges={"PSE"},
    )

    assert rows[0]["weak_sector_resolution_queue"] == "core_exclusion_candidate_scope_review_before_sector_fill"
    assert rows[0]["residual_decision"] == "core_exclusion_candidate_requires_scope_review"
    assert rows[0]["review_bucket"] == "scope_review_before_sector_fill"
    assert rows[0]["review_priority"] == "P1"
    assert rows[0]["apply_eligibility"] == "blocked_until_core_or_extended_scope_decision"
    assert rows[0]["recommended_next_action"] == "review_scope_before_sector_enrichment"
    assert rows[0]["review_strategy"] == "scope_review_before_weak_sector_enrichment"


def test_apply_and_evidence_gates_keep_weak_sector_gaps_blank_until_sourced() -> None:
    assert (
        apply_eligibility_for("venue_official_taxonomy_unavailable_source_gap")
        == "keep_sector_blank_until_venue_official_taxonomy_source_exists"
    )
    assert (
        verification_evidence_for("official_masterfile_without_sector_source_gap")
        == "official_masterfile_or_issuer_taxonomy_update_exposing_sector_for_exact_listing"
    )
    assert weak_sector_review_strategy_for("venue_official_taxonomy_unavailable_source_gap") == (
        "restore_or_add_venue_official_taxonomy_parser",
        "new_or_restored_official_venue_industry_taxonomy_source",
    )


def test_build_review_rows_filters_to_configured_exchanges() -> None:
    rows = build_review_rows(
        tickers=[],
        masterfile_rows=[],
        source_gap_rows=[
            {
                "field": "missing_sector_stock",
                "listing_key": "BK::A",
                "ticker": "A",
                "exchange": "BK",
                "asset_type": "Stock",
            },
            {
                "field": "missing_sector_stock",
                "listing_key": "OTHER::B",
                "ticker": "B",
                "exchange": "OTHER",
                "asset_type": "Stock",
            },
        ],
        source_of_truth_rows=[],
        exchanges={"BK"},
    )

    assert [row["listing_key"] for row in rows] == ["BK::A"]


def test_summarize_counts_sector_residuals() -> None:
    summary = summarize(
        [
            {
                "exchange": "NGX",
                "gap_class": "official_industry_taxonomy_unavailable_gap",
                "source_of_truth_outcome": "accepted_source_gap",
                "weak_sector_resolution_queue": "official_sector_candidate_normalization_review",
                "residual_decision": "official_sector_available_review_apply",
                "review_bucket": "official_sector_candidate_requires_normalization_gate",
                "review_priority": "P1",
                "apply_eligibility": "blocked_until_canonical_sector_normalization_and_listing_key_gate",
                "verification_evidence_required": "official_venue_sector_value_with_canonical_mapping_and_exact_listing_key_match",
                "official_masterfile_match": "true",
                "official_masterfile_exposes_sector": "true",
                "official_masterfile_sources": "ngx_company_profile_directory",
                "official_masterfile_sector_values": "Financials",
            },
            {
                "exchange": "BK",
                "gap_class": "official_industry_taxonomy_unavailable_gap",
                "source_of_truth_outcome": "accepted_source_gap",
                "weak_sector_resolution_queue": "official_masterfile_without_sector_source_gap",
                "residual_decision": "accepted_source_gap_official_masterfile_without_sector",
                "review_bucket": "official_masterfile_without_sector_source_gap",
                "review_priority": "P3",
                "apply_eligibility": "keep_sector_blank_until_official_masterfile_exposes_sector_or_reviewed_taxonomy",
                "verification_evidence_required": "official_masterfile_or_issuer_taxonomy_update_exposing_sector_for_exact_listing",
                "official_masterfile_match": "true",
                "official_masterfile_exposes_sector": "false",
                "official_masterfile_sources": "boursa_kuwait_stocks",
                "official_masterfile_sector_values": "",
            },
            {
                "exchange": "PSE",
                "gap_class": "shell_or_cpc_sector_gap",
                "source_of_truth_outcome": "core_exclusion_candidate",
                "weak_sector_resolution_queue": "core_exclusion_candidate_scope_review_before_sector_fill",
                "residual_decision": "core_exclusion_candidate_requires_scope_review",
                "review_bucket": "scope_review_before_sector_fill",
                "review_priority": "P1",
                "apply_eligibility": "blocked_until_core_or_extended_scope_decision",
                "verification_evidence_required": "scope_decision_for_core_extended_or_exclude_before_sector_enrichment",
                "official_masterfile_match": "true",
                "official_masterfile_exposes_sector": "false",
                "official_masterfile_sources": "pse_listed_company_directory",
                "official_masterfile_sector_values": "",
            },
        ],
        "2026-05-24T00:00:00Z",
        {"BK", "NGX", "PSE"},
    )

    assert summary["rows"] == 3
    assert summary["exchange_totals"] == {"BK": 1, "NGX": 1, "PSE": 1}
    assert summary["official_sector_candidate_rows"] == 1
    assert summary["official_sector_candidate_exchange_totals"] == {"NGX": 1}
    assert summary["official_sector_candidate_value_totals"] == {"Financials": 1}
    assert summary["scope_review_rows"] == 1
    assert summary["scope_review_exchange_totals"] == {"PSE": 1}
    assert summary["scope_review_gap_class_totals"] == {"shell_or_cpc_sector_gap": 1}
    assert summary["masterfile_without_sector_rows"] == 1
    assert summary["masterfile_without_sector_exchange_totals"] == {"BK": 1}
    assert summary["weak_sector_backlog"] == {
        "status": "venue_specific_review_queue_open",
        "rows": 3,
        "official_sector_candidate_rows": 1,
        "scope_decision_required_rows": 1,
        "masterfile_without_sector_rows": 1,
        "venue_taxonomy_source_required_rows": 0,
        "direct_sector_apply_allowed_rows": 0,
        "metadata_enrichment_authorized": False,
        "source_gate": (
            "Weak-sector enrichment remains blocked unless venue-official masterfile, issuer, or reviewed taxonomy "
            "evidence maps the exact listing to a canonical sector; no global symbol/name/peer inference is allowed."
        ),
    }
    assert summary["weak_sector_backlog_queue_totals"] == {
        "core_exclusion_candidate_scope_review_before_sector_fill": 1,
        "official_masterfile_without_sector_source_gap": 1,
        "official_sector_candidate_normalization_review": 1,
    }
    assert summary["weak_sector_backlog_evidence_required_totals"] == {
        "official_masterfile_or_issuer_taxonomy_update_exposing_sector_for_exact_listing": 1,
        "official_venue_sector_value_with_canonical_mapping_and_exact_listing_key_match": 1,
        "scope_decision_for_core_extended_or_exclude_before_sector_enrichment": 1,
    }
    assert summary["review_bucket_totals"] == {
        "official_masterfile_without_sector_source_gap": 1,
        "official_sector_candidate_requires_normalization_gate": 1,
        "scope_review_before_sector_fill": 1,
    }
    assert summary["weak_sector_resolution_queue_totals"] == {
        "core_exclusion_candidate_scope_review_before_sector_fill": 1,
        "official_masterfile_without_sector_source_gap": 1,
        "official_sector_candidate_normalization_review": 1,
    }
    assert summary["weak_sector_resolution_queue_exchange_totals"] == {
        "core_exclusion_candidate_scope_review_before_sector_fill": {"PSE": 1},
        "official_masterfile_without_sector_source_gap": {"BK": 1},
        "official_sector_candidate_normalization_review": {"NGX": 1},
    }
    assert summary["weak_sector_resolution_queue_gap_class_totals"] == {
        "core_exclusion_candidate_scope_review_before_sector_fill": {"shell_or_cpc_sector_gap": 1},
        "official_masterfile_without_sector_source_gap": {"official_industry_taxonomy_unavailable_gap": 1},
        "official_sector_candidate_normalization_review": {"official_industry_taxonomy_unavailable_gap": 1},
    }
    assert summary["weak_sector_resolution_queue_official_source_totals"] == {
        "core_exclusion_candidate_scope_review_before_sector_fill": {"pse_listed_company_directory": 1},
        "official_masterfile_without_sector_source_gap": {"boursa_kuwait_stocks": 1},
        "official_sector_candidate_normalization_review": {"ngx_company_profile_directory": 1},
    }
    assert summary["weak_sector_resolution_queue_review_strategy_totals"] == {
        "core_exclusion_candidate_scope_review_before_sector_fill": {
            "scope_review_before_weak_sector_enrichment": 1
        },
        "official_masterfile_without_sector_source_gap": {
            "keep_blank_until_official_masterfile_or_issuer_sector_source": 1
        },
        "official_sector_candidate_normalization_review": {
            "normalize_official_sector_candidate_before_apply": 1
        },
    }
    assert summary["weak_sector_resolution_queue_official_capability_totals"] == {
        "core_exclusion_candidate_scope_review_before_sector_fill": {
            "masterfile_exposes_sector=false": 1,
            "masterfile_match=true": 1,
        },
        "official_masterfile_without_sector_source_gap": {
            "masterfile_exposes_sector=false": 1,
            "masterfile_match=true": 1,
        },
        "official_sector_candidate_normalization_review": {
            "masterfile_exposes_sector=true": 1,
            "masterfile_match=true": 1,
        },
    }
    assert summary["venue_backlog_exchange_queue_totals"] == {
        "BK": {"official_masterfile_without_sector_source_gap": 1},
        "NGX": {"official_sector_candidate_normalization_review": 1},
        "PSE": {"core_exclusion_candidate_scope_review_before_sector_fill": 1},
    }
    assert summary["venue_backlog_exchange_official_capability_totals"] == {
        "BK": {"masterfile_exposes_sector=false": 1, "masterfile_match=true": 1},
        "NGX": {"masterfile_exposes_sector=true": 1, "masterfile_match=true": 1},
        "PSE": {"masterfile_exposes_sector=false": 1, "masterfile_match=true": 1},
    }
    assert summary["top_venue_backlog_batches"] == [
        {
            "exchange": "BK",
            "weak_sector_resolution_queue": "official_masterfile_without_sector_source_gap",
            "official_source": "boursa_kuwait_stocks",
            "rows": 1,
            "review_strategy": "keep_blank_until_official_masterfile_or_issuer_sector_source",
            "evidence_required": "official_masterfile_or_issuer_taxonomy_update_exposing_sector_for_exact_listing",
            "recommended_next_source": (
                "Updated official masterfile or issuer taxonomy exposing sector for the exact listing; "
                "current source: boursa_kuwait_stocks."
            ),
            "source_gate": (
                "Keep sector blank until an official masterfile or issuer record exposes sector for the exact listing."
            ),
        },
        {
            "exchange": "NGX",
            "weak_sector_resolution_queue": "official_sector_candidate_normalization_review",
            "official_source": "ngx_company_profile_directory",
            "rows": 1,
            "review_strategy": "normalize_official_sector_candidate_before_apply",
            "evidence_required": "official_venue_sector_value_with_canonical_mapping_and_exact_listing_key_match",
            "recommended_next_source": (
                "Official sector value from ngx_company_profile_directory plus canonical sector mapping."
            ),
            "source_gate": "Apply only after exact listing-key match and canonical sector normalization.",
        },
        {
            "exchange": "PSE",
            "weak_sector_resolution_queue": "core_exclusion_candidate_scope_review_before_sector_fill",
            "official_source": "pse_listed_company_directory",
            "rows": 1,
            "review_strategy": "scope_review_before_weak_sector_enrichment",
            "evidence_required": "scope_decision_for_core_extended_or_exclude_before_sector_enrichment",
            "recommended_next_source": "Official listing scope evidence before any sector enrichment.",
            "source_gate": "No sector fill until the listing is confirmed as core, extended, or excluded.",
        },
    ]
    assert summary["top_weak_sector_resolution_review_batches"] == [
        {
            "weak_sector_resolution_queue": "core_exclusion_candidate_scope_review_before_sector_fill",
            "exchange": "PSE",
            "official_source": "pse_listed_company_directory",
            "rows": 1,
            "review_strategy": "scope_review_before_weak_sector_enrichment",
            "evidence_required": "scope_decision_for_core_extended_or_exclude_before_sector_enrichment",
            "recommended_next_source": "Official listing scope evidence before any sector enrichment.",
            "source_gate": "No sector fill until the listing is confirmed as core, extended, or excluded.",
        },
        {
            "weak_sector_resolution_queue": "official_masterfile_without_sector_source_gap",
            "exchange": "BK",
            "official_source": "boursa_kuwait_stocks",
            "rows": 1,
            "review_strategy": "keep_blank_until_official_masterfile_or_issuer_sector_source",
            "evidence_required": "official_masterfile_or_issuer_taxonomy_update_exposing_sector_for_exact_listing",
            "recommended_next_source": (
                "Updated official masterfile or issuer taxonomy exposing sector for the exact listing; "
                "current source: boursa_kuwait_stocks."
            ),
            "source_gate": (
                "Keep sector blank until an official masterfile or issuer record exposes sector for the exact listing."
            ),
        },
        {
            "weak_sector_resolution_queue": "official_sector_candidate_normalization_review",
            "exchange": "NGX",
            "official_source": "ngx_company_profile_directory",
            "rows": 1,
            "review_strategy": "normalize_official_sector_candidate_before_apply",
            "evidence_required": "official_venue_sector_value_with_canonical_mapping_and_exact_listing_key_match",
            "recommended_next_source": (
                "Official sector value from ngx_company_profile_directory plus canonical sector mapping."
            ),
            "source_gate": "Apply only after exact listing-key match and canonical sector normalization.",
        },
    ]
    assert summary["review_priority_totals"] == {"P1": 2, "P3": 1}
    assert summary["apply_eligibility_totals"] == {
        "blocked_until_canonical_sector_normalization_and_listing_key_gate": 1,
        "blocked_until_core_or_extended_scope_decision": 1,
        "keep_sector_blank_until_official_masterfile_exposes_sector_or_reviewed_taxonomy": 1,
    }
    assert summary["verification_evidence_required_totals"] == {
        "official_masterfile_or_issuer_taxonomy_update_exposing_sector_for_exact_listing": 1,
        "official_venue_sector_value_with_canonical_mapping_and_exact_listing_key_match": 1,
        "scope_decision_for_core_extended_or_exclude_before_sector_enrichment": 1,
    }
    assert summary["official_masterfile_match_totals"] == {"true": 3}
    assert summary["official_masterfile_exposes_sector_totals"] == {"false": 2, "true": 1}
    assert summary["official_sector_value_totals"] == {"Financials": 1}


def test_build_json_payload_includes_source_traceability_metadata() -> None:
    summary = summarize([], "2026-05-24T00:00:00Z", {"BK", "NGX"})
    payload = build_json_payload(
        summary=summary,
        rows=[],
        source_files={
            "tickers_csv": "data/tickers.csv",
            "masterfile_reference_csv": "data/masterfiles/reference.csv",
            "source_gap_classification_csv": "data/reports/source_gap_classification.csv",
            "source_of_truth_decisions_csv": "data/reports/source_of_truth_decisions.csv",
        },
        exchanges={"BK", "NGX"},
    )

    assert payload["_meta"]["generated_at"] == "2026-05-24T00:00:00Z"
    assert payload["_meta"]["source_files"]["masterfile_reference_csv"] == "data/masterfiles/reference.csv"
    assert payload["_meta"]["exchanges"] == ["BK", "NGX"]
    assert payload["_meta"]["policy"] == summary["policy"]
    assert payload["summary"] == summary
    assert payload["rows"] == []

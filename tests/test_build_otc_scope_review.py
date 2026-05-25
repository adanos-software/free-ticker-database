from scripts.build_otc_scope_review import (
    build_review_rows,
    otc_review_decision_context_for,
    recommended_next_source_for,
    review_strategy_for,
    source_gate_for,
    scope_review_context_for,
    source_gap_context_for,
    summarize,
    verification_evidence_required_for,
)


def test_build_review_rows_marks_otc_extended_scope() -> None:
    rows = build_review_rows(
        tickers=[
            {
                "listing_key": "OTC::AAAA",
                "ticker": "AAAA",
                "exchange": "OTC",
                "asset_type": "Stock",
                "name": "AAAA Corp",
            }
        ],
        instrument_scopes=[
            {
                "listing_key": "OTC::AAAA",
                "ticker": "AAAA",
                "exchange": "OTC",
                "asset_type": "Stock",
                "name": "AAAA Corp",
                "instrument_scope": "extended",
                "scope_reason": "otc_listing",
            }
        ],
        entry_quality_rows=[
            {
                "listing_key": "OTC::AAAA",
                "ticker": "AAAA",
                "exchange": "OTC",
                "quality_status": "source_gap",
                "issue_types": "official_reference_gap",
            }
        ],
        source_gap_rows=[
            {
                "listing_key": "OTC::AAAA",
                "ticker": "AAAA",
                "exchange": "OTC",
                "field": "missing_sector_stock",
                "gap_class": "otc_sector_source_gap",
            }
        ],
        source_of_truth_rows=[
            {
                "listing_key": "OTC::AAAA",
                "ticker": "AAAA",
                "exchange": "OTC",
                "source_of_truth_outcome": "accepted_source_gap",
            }
        ],
        otc_review_decision_rows=[],
    )

    assert rows == [
        {
            "listing_key": "OTC::AAAA",
            "ticker": "AAAA",
            "exchange": "OTC",
            "asset_type": "Stock",
            "name": "AAAA Corp",
            "instrument_scope": "extended",
            "scope_reason": "otc_listing",
            "quality_status": "source_gap",
            "issue_types": "official_reference_gap",
            "source_gap_field": "missing_sector_stock",
            "source_gap_class": "otc_sector_source_gap",
            "source_of_truth_outcome": "accepted_source_gap",
            "source_gap_context": (
                "quality_status=source_gap;issue_types=official_reference_gap;"
                "source_gap_field=missing_sector_stock;source_gap_class=otc_sector_source_gap;"
                "source_of_truth_outcome=accepted_source_gap"
            ),
            "scope_decision": "already_extended_otc_listing",
            "otc_review_decision_status": "not_applicable",
            "otc_review_decision_context": "listing_key=OTC::AAAA;otc_review_decision_status=not_applicable",
            "review_bucket": "documented_otc_sector_source_gap",
            "review_priority": "P3",
            "scope_apply_eligibility": "already_extended_no_scope_change_required",
            "metadata_enrichment_gate": "reviewed_issuer_sector_source_required_keep_blank",
            "scope_review_context": (
                "instrument_scope=extended;scope_reason=otc_listing;"
                "scope_decision=already_extended_otc_listing;"
                "scope_apply_eligibility=already_extended_no_scope_change_required;"
                "metadata_enrichment_gate=reviewed_issuer_sector_source_required_keep_blank"
            ),
            "review_strategy": "keep_sector_blank_until_reviewed_issuer_sector_source",
            "verification_evidence_required": (
                "reviewed_issuer_sector_source_with_exact_listing_or_keep_blank_decision"
            ),
            "recommended_next_source": (
                "SEC SIC, issuer filings, OTC Markets profile, or reviewed secondary company profile."
            ),
            "source_gate": "Canonical stock sector only after exchange/name gate; no ticker/name-only inference.",
            "recommended_action": "leave_blank_as_documented_source_gap_until_reviewed_source",
        }
    ]


def test_build_review_rows_flags_unexpected_otc_core_scope() -> None:
    rows = build_review_rows(
        tickers=[],
        instrument_scopes=[
            {
                "listing_key": "OTC::CORE",
                "ticker": "CORE",
                "exchange": "OTC",
                "instrument_scope": "core",
                "scope_reason": "primary_listing",
            }
        ],
        entry_quality_rows=[],
        source_gap_rows=[],
        source_of_truth_rows=[],
        otc_review_decision_rows=[],
    )

    assert rows[0]["scope_decision"] == "unexpected_otc_core_scope_review_required"
    assert rows[0]["review_bucket"] == "unexpected_core_scope_requires_review"
    assert rows[0]["review_priority"] == "P1"
    assert rows[0]["scope_apply_eligibility"] == "blocked_until_otc_core_scope_override_reviewed"
    assert rows[0]["metadata_enrichment_gate"] == "scope_decision_required_before_any_metadata_enrichment"
    assert rows[0]["recommended_next_source"] == (
        "Instrument scope override, OTC Markets security profile, SEC/issuer filing, or reviewed scope decision."
    )
    assert rows[0]["source_gate"] == (
        "No scope or metadata change until the core override is reviewed and listing-keyed."
    )
    assert rows[0]["source_gap_context"] == source_gap_context_for(rows[0])
    assert rows[0]["otc_review_decision_context"] == otc_review_decision_context_for(rows[0])
    assert rows[0]["scope_review_context"] == scope_review_context_for(rows[0])
    assert rows[0]["recommended_action"] == "review_scope_override_before_metadata_enrichment"


def test_build_review_rows_blocks_core_exclusion_candidate_metadata() -> None:
    rows = build_review_rows(
        tickers=[],
        instrument_scopes=[
            {
                "listing_key": "OTC::DROP",
                "ticker": "DROP",
                "exchange": "OTC",
                "asset_type": "Stock",
                "instrument_scope": "extended",
                "scope_reason": "otc_listing",
            }
        ],
        entry_quality_rows=[
            {
                "listing_key": "OTC::DROP",
                "ticker": "DROP",
                "exchange": "OTC",
                "quality_status": "source_gap",
                "issue_types": "official_reference_gap",
            }
        ],
        source_gap_rows=[],
        source_of_truth_rows=[
            {
                "listing_key": "OTC::DROP",
                "ticker": "DROP",
                "exchange": "OTC",
                "source_of_truth_outcome": "core_exclusion_candidate",
            }
        ],
        otc_review_decision_rows=[],
    )

    assert rows[0]["scope_decision"] == "core_exclusion_candidate_requires_review"
    assert rows[0]["review_bucket"] == "core_exclusion_candidate_scope_review"
    assert rows[0]["review_priority"] == "P1"
    assert rows[0]["scope_apply_eligibility"] == "blocked_until_core_exclusion_candidate_scope_decision"
    assert rows[0]["metadata_enrichment_gate"] == "scope_decision_required_before_any_metadata_enrichment"
    assert rows[0]["review_strategy"] == "decide_core_extended_or_exclude_before_otc_metadata_work"
    assert rows[0]["verification_evidence_required"] == (
        "official_otc_listing_status_or_scope_policy_decision_before_metadata_work"
    )
    assert rows[0]["recommended_action"] == "review_official_instrument_type_before_fill_or_drop"
    assert rows[0]["source_gate"] == (
        "Decide core, extended, or exclude before any identifier, name, sector, or category enrichment."
    )


def test_summarize_counts_otc_scope_and_overrides() -> None:
    summary = summarize(
        [
            {
                "listing_key": "OTC::AAAA",
                "instrument_scope": "extended",
                "scope_reason": "otc_listing",
                "quality_status": "source_gap",
                "source_gap_field": "missing_sector_stock",
                "source_gap_class": "otc_sector_source_gap",
                "source_of_truth_outcome": "accepted_source_gap",
                "scope_decision": "already_extended_otc_listing",
                "otc_review_decision_status": "not_applicable",
                "review_bucket": "documented_otc_sector_source_gap",
                "review_priority": "P3",
                "scope_apply_eligibility": "already_extended_no_scope_change_required",
                "metadata_enrichment_gate": "reviewed_issuer_sector_source_required_keep_blank",
                "review_strategy": "keep_sector_blank_until_reviewed_issuer_sector_source",
                "verification_evidence_required": (
                    "reviewed_issuer_sector_source_with_exact_listing_or_keep_blank_decision"
                ),
                "recommended_next_source": (
                    "SEC SIC, issuer filings, OTC Markets profile, or reviewed secondary company profile."
                ),
                "source_gate": "Canonical stock sector only after exchange/name gate; no ticker/name-only inference.",
                "asset_type": "Stock",
            }
        ],
        generated_at="2026-05-24T00:00:00Z",
        drop_rows=[
            {"ticker": "DROP", "exchange": "OTC"},
            {"ticker": "AAAA", "exchange": "OTC"},
        ],
        otc_review_decision_rows=[
            {"ticker": "AAAA", "exchange": "OTC"},
            {"ticker": "KEEP", "exchange": "OTC"},
        ],
    )

    assert summary["rows"] == 1
    assert summary["scope_decision_totals"] == {"already_extended_otc_listing": 1}
    assert summary["review_bucket_totals"] == {"documented_otc_sector_source_gap": 1}
    assert summary["review_bucket_asset_type_totals"] == {"documented_otc_sector_source_gap": {"Stock": 1}}
    assert summary["review_bucket_metadata_gate_totals"] == {
        "documented_otc_sector_source_gap": {"reviewed_issuer_sector_source_required_keep_blank": 1}
    }
    assert summary["review_priority_totals"] == {"P3": 1}
    assert summary["scope_apply_eligibility_totals"] == {"already_extended_no_scope_change_required": 1}
    assert summary["otc_scope_completion"] == {
        "status": "complete_extended_scope_no_core_candidates",
        "rows": 1,
        "extended_otc_rows": 1,
        "otc_listing_scope_reason_rows": 1,
        "already_extended_scope_decision_rows": 1,
        "core_exclusion_candidate_rows": 0,
        "unexpected_core_scope_rows": 0,
        "blocked_scope_decision_rows": 0,
        "scope_apply_allowed_rows": 0,
        "metadata_enrichment_authorized": False,
        "source_gate": (
            "OTC scope is complete only when every current OTC row is extended/otc_listing and no core or "
            "core-exclusion scope decision remains open; metadata still requires listing-keyed evidence."
        ),
    }
    assert summary["post_scope_metadata_backlog"] == {
        "status": "metadata_review_backlog_open",
        "rows": 1,
        "metadata_enrichment_authorized": False,
        "scope_blocked_rows": 0,
        "source_gate": (
            "Post-scope OTC metadata work remains blocked unless each row has listing-keyed OTC Markets, "
            "issuer, SEC, registry, or reviewed fallback evidence; no ticker-only enrichment is allowed."
        ),
    }
    assert summary["post_scope_metadata_backlog_bucket_totals"] == {
        "documented_otc_sector_source_gap": 1
    }
    assert summary["post_scope_metadata_backlog_gate_totals"] == {
        "reviewed_issuer_sector_source_required_keep_blank": 1
    }
    assert summary["post_scope_metadata_backlog_examples"] == [
        {
            "listing_key": "OTC::AAAA",
            "ticker": "",
            "asset_type": "Stock",
            "name": "",
            "review_bucket": "documented_otc_sector_source_gap",
            "quality_status": "source_gap",
            "metadata_enrichment_gate": "reviewed_issuer_sector_source_required_keep_blank",
            "review_strategy": "keep_sector_blank_until_reviewed_issuer_sector_source",
            "verification_evidence_required": (
                "reviewed_issuer_sector_source_with_exact_listing_or_keep_blank_decision"
            ),
            "recommended_next_source": (
                "SEC SIC, issuer filings, OTC Markets profile, or reviewed secondary company profile."
            ),
            "source_gate": "Canonical stock sector only after exchange/name gate; no ticker/name-only inference.",
        }
    ]
    assert summary["metadata_enrichment_gate_totals"] == {"reviewed_issuer_sector_source_required_keep_blank": 1}
    assert summary["review_strategy_totals"] == {"keep_sector_blank_until_reviewed_issuer_sector_source": 1}
    assert summary["verification_evidence_required_totals"] == {
        "reviewed_issuer_sector_source_with_exact_listing_or_keep_blank_decision": 1
    }
    assert summary["top_otc_scope_review_batches"] == [
        {
            "review_priority": "P3",
            "review_bucket": "documented_otc_sector_source_gap",
            "asset_type": "Stock",
            "quality_status": "source_gap",
            "metadata_enrichment_gate": "reviewed_issuer_sector_source_required_keep_blank",
            "rows": 1,
            "review_strategy": "keep_sector_blank_until_reviewed_issuer_sector_source",
            "verification_evidence_required": (
                "reviewed_issuer_sector_source_with_exact_listing_or_keep_blank_decision"
            ),
            "recommended_next_source": (
                "SEC SIC, issuer filings, OTC Markets profile, or reviewed secondary company profile."
            ),
            "source_gate": "Canonical stock sector only after exchange/name gate; no ticker/name-only inference.",
        }
    ]
    assert summary["source_gap_field_totals"] == {"missing_sector_stock": 1}
    assert summary["otc_core_exclusion_candidate_rows"] == 0
    assert summary["otc_core_exclusion_candidate_asset_type_totals"] == {}
    assert summary["otc_core_exclusion_candidate_quality_status_totals"] == {}
    assert summary["otc_core_exclusion_candidate_metadata_gate_totals"] == {}
    assert summary["otc_core_exclusion_candidate_review_examples"] == []
    assert summary["drop_override_rows"] == 2
    assert summary["drop_override_rows_still_present"] == 1
    assert summary["otc_review_decision_rows"] == 2
    assert summary["otc_review_decision_active_name_mismatch_rows"] == 0
    assert summary["otc_name_mismatch_unreviewed_active_rows"] == 0
    assert summary["otc_review_decision_resolution_totals"] == {
        "reviewed_decision_not_in_current_otc_scope": 1,
        "reviewed_decision_suppresses_current_listing_warning": 1,
    }
    assert summary["otc_review_decision_current_listing_suppressed_rows"] == 1
    assert summary["otc_review_decision_current_listing_suppressed_examples"] == ["OTC::AAAA"]
    assert summary["otc_review_decision_not_current_scope_rows"] == 1
    assert summary["otc_review_decision_not_current_scope_examples"] == ["OTC::KEEP"]
    assert summary["otc_review_decision_stale_rows"] == 2
    assert summary["otc_review_decision_stale_examples"] == ["OTC::AAAA", "OTC::KEEP"]


def test_summarize_surfaces_core_exclusion_candidate_scope_queue() -> None:
    summary = summarize(
        [
            {
                "listing_key": "OTC::DROP",
                "ticker": "DROP",
                "instrument_scope": "extended",
                "scope_reason": "otc_listing",
                "quality_status": "source_gap",
                "source_gap_field": "",
                "source_gap_class": "",
                "source_of_truth_outcome": "core_exclusion_candidate",
                "scope_decision": "core_exclusion_candidate_requires_review",
                "otc_review_decision_status": "not_applicable",
                "review_bucket": "core_exclusion_candidate_scope_review",
                "review_priority": "P1",
                "scope_apply_eligibility": "blocked_until_core_exclusion_candidate_scope_decision",
                "metadata_enrichment_gate": "scope_decision_required_before_any_metadata_enrichment",
                "review_strategy": "decide_core_extended_or_exclude_before_otc_metadata_work",
                "verification_evidence_required": (
                    "official_otc_listing_status_or_scope_policy_decision_before_metadata_work"
                ),
                "recommended_next_source": (
                    "OTC Markets security profile, exchange tier/status evidence, SEC/issuer filing, "
                    "or reviewed scope policy decision."
                ),
                "source_gate": (
                    "Decide core, extended, or exclude before any identifier, name, sector, or category enrichment."
                ),
                "recommended_action": "review_official_instrument_type_before_fill_or_drop",
                "asset_type": "Stock",
            }
        ],
        generated_at="2026-05-24T00:00:00Z",
        drop_rows=[],
        otc_review_decision_rows=[],
    )

    assert summary["otc_core_exclusion_candidate_rows"] == 1
    assert summary["otc_scope_completion"]["status"] == "scope_review_open"
    assert summary["otc_scope_completion"]["blocked_scope_decision_rows"] == 1
    assert summary["otc_scope_completion"]["core_exclusion_candidate_rows"] == 1
    assert summary["otc_core_exclusion_candidate_asset_type_totals"] == {"Stock": 1}
    assert summary["otc_core_exclusion_candidate_quality_status_totals"] == {"source_gap": 1}
    assert summary["otc_core_exclusion_candidate_metadata_gate_totals"] == {
        "scope_decision_required_before_any_metadata_enrichment": 1
    }
    assert summary["otc_core_exclusion_candidate_review_examples"] == [
        {
            "listing_key": "OTC::DROP",
            "ticker": "DROP",
            "asset_type": "Stock",
            "quality_status": "source_gap",
            "scope_decision": "core_exclusion_candidate_requires_review",
            "review_bucket": "core_exclusion_candidate_scope_review",
            "metadata_enrichment_gate": "scope_decision_required_before_any_metadata_enrichment",
            "recommended_action": "review_official_instrument_type_before_fill_or_drop",
            "source_gate": (
                "Decide core, extended, or exclude before any identifier, name, sector, or category enrichment."
            ),
        }
    ]


def test_build_review_rows_marks_active_name_mismatch_review_status() -> None:
    rows = build_review_rows(
        tickers=[],
        instrument_scopes=[
            {
                "listing_key": "OTC::DONE",
                "ticker": "DONE",
                "exchange": "OTC",
                "asset_type": "Stock",
                "instrument_scope": "extended",
                "scope_reason": "otc_listing",
            },
            {
                "listing_key": "OTC::TODO",
                "ticker": "TODO",
                "exchange": "OTC",
                "asset_type": "Stock",
                "instrument_scope": "extended",
                "scope_reason": "otc_listing",
            },
        ],
        entry_quality_rows=[
            {
                "listing_key": "OTC::DONE",
                "ticker": "DONE",
                "exchange": "OTC",
                "quality_status": "warn",
                "issue_types": "official_name_mismatch",
            },
            {
                "listing_key": "OTC::TODO",
                "ticker": "TODO",
                "exchange": "OTC",
                "quality_status": "warn",
                "issue_types": "official_name_mismatch",
            },
        ],
        source_gap_rows=[],
        source_of_truth_rows=[],
        otc_review_decision_rows=[{"ticker": "DONE", "exchange": "OTC"}],
    )

    by_ticker = {row["ticker"]: row for row in rows}
    assert by_ticker["DONE"]["otc_review_decision_status"] == "reviewed_name_mismatch_decision_present"
    assert by_ticker["DONE"]["otc_review_decision_context"] == (
        "listing_key=OTC::DONE;otc_review_decision_status=reviewed_name_mismatch_decision_present"
    )
    assert by_ticker["TODO"]["otc_review_decision_status"] == "pending_otc_name_mismatch_review"


def test_review_strategy_for_keeps_otc_scope_and_metadata_gates_explicit() -> None:
    assert (
        review_strategy_for("official_name_mismatch_review_first")
        == "resolve_listing_keyed_name_mismatch_before_metadata_work"
    )
    assert (
        review_strategy_for("documented_otc_category_source_gap")
        == "keep_category_blank_until_reviewed_product_taxonomy_source"
    )
    assert (
        verification_evidence_required_for("documented_otc_category_source_gap")
        == "reviewed_product_taxonomy_source_with_exact_listing_or_keep_blank_decision"
    )
    assert (
        verification_evidence_required_for("official_name_mismatch_review_first")
        == "reviewed_otc_name_mismatch_decision_before_name_or_metadata_change"
    )
    assert (
        recommended_next_source_for("core_exclusion_candidate_scope_review")
        == "OTC Markets security profile, exchange tier/status evidence, SEC/issuer filing, or reviewed scope policy decision."
    )
    assert (
        source_gate_for("documented_otc_category_source_gap")
        == "ETF category only from exact product evidence; no category inference from ticker or issuer family."
    )

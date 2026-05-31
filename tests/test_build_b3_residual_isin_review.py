from scripts.build_b3_residual_isin_review import (
    b3_official_source_identifier_exposure,
    build_json_payload,
    build_review_rows,
    official_source_context_for,
    recommended_next_source_for,
    residual_review_context_for,
    review_strategy_for,
    scope_review_context_for,
    source_gate_for,
    source_gap_context_for,
    summarize,
)


def test_build_review_rows_keeps_residual_gaps_listing_keyed() -> None:
    rows = build_review_rows(
        tickers=[
            {
                "ticker": "AFOF11",
                "exchange": "B3",
                "asset_type": "ETF",
                "name": "Alianza Fofii Fundo De Investimento Imobiliario",
                "isin": "",
            }
        ],
        instrument_scope_rows=[
            {
                "listing_key": "B3::AFOF11",
                "ticker": "AFOF11",
                "exchange": "B3",
                "instrument_scope": "core",
                "scope_reason": "primary_listing_missing_isin",
            }
        ],
        source_gap_rows=[
            {
                "field": "missing_isin_primary",
                "listing_key": "B3::AFOF11",
                "ticker": "AFOF11",
                "exchange": "B3",
                "asset_type": "ETF",
                "name": "Alianza Fofii Fundo De Investimento Imobiliario",
                "gap_class": "fund_or_trust_identifier_gap",
                "review_needed": "true",
                "recommended_next_source": "Official fund/trust masterfile.",
                "source_gate": "Exact fund/trust symbol and product name with checksum.",
            },
            {
                "field": "missing_stock_sector",
                "listing_key": "B3::AFOF11",
                "ticker": "AFOF11",
                "exchange": "B3",
                "asset_type": "ETF",
            },
        ],
        source_of_truth_rows=[
            {
                "field": "missing_isin_primary",
                "listing_key": "B3::AFOF11",
                "source_of_truth_outcome": "accepted_source_gap",
            }
        ],
        masterfile_rows=[],
        cotahist_rows=[
            {
                "ticker": "AFOF11",
                "decision": "no_cotahist_match",
                "cotahist_isin": "",
            }
        ],
    )

    assert rows == [
        {
            "listing_key": "B3::AFOF11",
            "ticker": "AFOF11",
            "exchange": "B3",
            "asset_type": "ETF",
            "name": "Alianza Fofii Fundo De Investimento Imobiliario",
            "gap_class": "fund_or_trust_identifier_gap",
            "source_of_truth_outcome": "accepted_source_gap",
            "source_gap_context": (
                "gap_class=fund_or_trust_identifier_gap;"
                "source_of_truth_outcome=accepted_source_gap;review_needed=true"
            ),
            "current_instrument_scope": "core",
            "current_scope_reason": "primary_listing_missing_isin",
            "scope_review_context": (
                "current_instrument_scope=core;current_scope_reason=primary_listing_missing_isin;"
                "source_of_truth_outcome=accepted_source_gap"
            ),
            "review_needed": "true",
            "recommended_next_source": (
                "Official fund/trust registry, prospectus, CSD, or reviewed identifier feed with exact product match."
            ),
            "source_gate": "Keep ISIN blank until official fund/trust product evidence exposes a valid checksum ISIN.",
            "b3_instruments_equities_match": "false",
            "b3_instruments_equities_isin": "",
            "cotahist_probe_decision": "no_cotahist_match",
            "cotahist_probe_isin": "",
            "official_source_context": (
                "b3_instruments_equities_match=false;b3_instruments_equities_isin=none;"
                "cotahist_probe_decision=no_cotahist_match;cotahist_probe_isin=none"
            ),
            "residual_decision": "accepted_source_gap_requires_fund_or_registry_source",
            "review_bucket": "fund_or_registry_identifier_source_gap",
            "review_priority": "P2",
            "review_strategy": "seek_official_fund_trust_registry_or_prospectus_isin",
            "apply_eligibility": "source_gap_keep_blank_until_official_identifier_evidence",
            "verification_evidence_required": "official_fund_trust_registry_or_prospectus_with_exact_symbol_product_name_and_valid_isin",
            "residual_review_context": (
                "residual_decision=accepted_source_gap_requires_fund_or_registry_source;"
                "review_bucket=fund_or_registry_identifier_source_gap;"
                "apply_eligibility=source_gap_keep_blank_until_official_identifier_evidence;"
                "verification_evidence_required=official_fund_trust_registry_or_prospectus_with_exact_symbol_product_name_and_valid_isin"
            ),
        }
    ]


def test_build_review_rows_flags_official_isin_available() -> None:
    rows = build_review_rows(
        tickers=[],
        instrument_scope_rows=[],
        source_gap_rows=[
            {
                "field": "missing_isin_primary",
                "listing_key": "B3::EBRK11",
                "ticker": "EBRK11",
                "exchange": "B3",
                "asset_type": "ETF",
                "name": "Investo ETF",
                "gap_class": "fund_or_trust_identifier_gap",
            }
        ],
        source_of_truth_rows=[],
        masterfile_rows=[
            {
                "source_key": "b3_instruments_equities",
                "ticker": "EBRK11",
                "exchange": "B3",
                "asset_type": "ETF",
                "listing_status": "active",
                "official": "true",
                "isin": "BREBRKCTF001",
            }
        ],
        cotahist_rows=[],
    )

    assert rows[0]["b3_instruments_equities_match"] == "true"
    assert rows[0]["b3_instruments_equities_isin"] == "BREBRKCTF001"
    assert rows[0]["residual_decision"] == "official_b3_isin_available_rebuild_required"
    assert rows[0]["review_bucket"] == "official_isin_candidate_requires_apply_gate"
    assert rows[0]["review_priority"] == "P1"
    assert rows[0]["review_strategy"] == "validate_official_b3_or_cotahist_isin_before_apply"
    assert rows[0]["apply_eligibility"] == "apply_only_after_listing_key_and_checksum_validation"
    assert rows[0]["verification_evidence_required"] == "official_b3_or_cotahist_isin_with_exact_listing_key_name_and_isin_checksum"
    assert rows[0]["source_gap_context"] == source_gap_context_for(rows[0])
    assert rows[0]["official_source_context"] == official_source_context_for(rows[0])
    assert rows[0]["residual_review_context"] == residual_review_context_for(rows[0])


def test_build_review_rows_flags_core_exclusion_candidates() -> None:
    rows = build_review_rows(
        tickers=[],
        instrument_scope_rows=[
            {
                "listing_key": "B3::C3RP3",
                "ticker": "C3RP3",
                "exchange": "B3",
                "instrument_scope": "core",
                "scope_reason": "primary_listing_missing_isin",
            }
        ],
        source_gap_rows=[
            {
                "field": "missing_isin_primary",
                "listing_key": "B3::C3RP3",
                "ticker": "C3RP3",
                "exchange": "B3",
                "asset_type": "Stock",
                "name": "COTRASA PARTICIPACOES S.A.",
                "gap_class": "inactive_or_legacy_identifier_gap",
            }
        ],
        source_of_truth_rows=[
            {
                "field": "missing_isin_primary",
                "listing_key": "B3::C3RP3",
                "source_of_truth_outcome": "core_exclusion_candidate",
            }
        ],
        masterfile_rows=[],
        cotahist_rows=[],
    )

    assert rows[0]["residual_decision"] == "core_exclusion_candidate_requires_scope_review"
    assert rows[0]["review_bucket"] == "scope_review_before_identifier_fill"
    assert rows[0]["review_priority"] == "P1"
    assert rows[0]["review_strategy"] == "decide_b3_core_extended_or_exclude_before_identifier_work"
    assert rows[0]["apply_eligibility"] == "blocked_until_core_or_extended_scope_decision"
    assert rows[0]["verification_evidence_required"] == "scope_decision_for_core_extended_or_exclude_before_identifier_fill"
    assert rows[0]["source_gap_context"] == source_gap_context_for(rows[0])
    assert rows[0]["scope_review_context"] == scope_review_context_for(rows[0])
    assert rows[0]["current_instrument_scope"] == "core"
    assert rows[0]["current_scope_reason"] == "primary_listing_missing_isin"
    assert rows[0]["official_source_context"] == official_source_context_for(rows[0])
    assert rows[0]["residual_review_context"] == residual_review_context_for(rows[0])


def test_summarize_counts_residual_decisions() -> None:
    summary = summarize(
        [
            {
                "gap_class": "fund_or_trust_identifier_gap",
                "current_instrument_scope": "core",
                "current_scope_reason": "primary_listing_missing_isin",
                "residual_decision": "accepted_source_gap_requires_fund_or_registry_source",
                "review_bucket": "fund_or_registry_identifier_source_gap",
                "review_priority": "P2",
                "apply_eligibility": "source_gap_keep_blank_until_official_identifier_evidence",
                "verification_evidence_required": "official_fund_trust_registry_or_prospectus_with_exact_symbol_product_name_and_valid_isin",
                "cotahist_probe_decision": "no_cotahist_match",
            },
            {
                "gap_class": "inactive_or_legacy_identifier_gap",
                "current_instrument_scope": "extended",
                "current_scope_reason": "reviewed_inactive_listing",
                "residual_decision": "accepted_source_gap_requires_active_listing_evidence",
                "review_bucket": "active_listing_evidence_required_source_gap",
                "review_priority": "P2",
                "apply_eligibility": "source_gap_keep_blank_until_official_identifier_evidence",
                "verification_evidence_required": "current_active_b3_listing_status_plus_direct_identifier_source",
                "cotahist_probe_decision": "",
            },
        ],
        "2026-05-24T00:00:00Z",
    )

    assert summary["rows"] == 2
    assert summary["gap_class_totals"] == {
        "fund_or_trust_identifier_gap": 1,
        "inactive_or_legacy_identifier_gap": 1,
    }
    assert summary["cotahist_probe_decision_totals"] == {
        "missing_probe_row": 1,
        "no_cotahist_match": 1,
    }
    assert summary["current_instrument_scope_totals"] == {"core": 1, "extended": 1}
    assert summary["current_scope_reason_totals"] == {
        "primary_listing_missing_isin": 1,
        "reviewed_inactive_listing": 1,
    }
    assert summary["review_bucket_totals"] == {
        "active_listing_evidence_required_source_gap": 1,
        "fund_or_registry_identifier_source_gap": 1,
    }
    assert summary["review_priority_totals"] == {"P2": 2}
    assert summary["apply_eligibility_totals"] == {"source_gap_keep_blank_until_official_identifier_evidence": 2}
    assert summary["verification_evidence_required_totals"] == {
        "current_active_b3_listing_status_plus_direct_identifier_source": 1,
        "official_fund_trust_registry_or_prospectus_with_exact_symbol_product_name_and_valid_isin": 1,
    }
    assert summary["review_strategy_totals"] == {
        "seek_current_active_b3_listing_status_then_identifier_source": 1,
        "seek_official_fund_trust_registry_or_prospectus_isin": 1,
    }
    assert summary["top_b3_isin_review_batches"] == [
        {
            "review_priority": "P2",
            "review_bucket": "active_listing_evidence_required_source_gap",
            "gap_class": "inactive_or_legacy_identifier_gap",
            "asset_type": "",
            "rows": 1,
            "review_strategy": "seek_current_active_b3_listing_status_then_identifier_source",
            "verification_evidence_required": "current_active_b3_listing_status_plus_direct_identifier_source",
            "recommended_next_source": "Current B3 active-listing status source plus a direct official identifier source.",
            "source_gate": "Keep ISIN blank until active listing status and a direct identifier source are both present.",
        },
        {
            "review_priority": "P2",
            "review_bucket": "fund_or_registry_identifier_source_gap",
            "gap_class": "fund_or_trust_identifier_gap",
            "asset_type": "",
            "rows": 1,
            "review_strategy": "seek_official_fund_trust_registry_or_prospectus_isin",
            "verification_evidence_required": "official_fund_trust_registry_or_prospectus_with_exact_symbol_product_name_and_valid_isin",
            "recommended_next_source": (
                "Official fund/trust registry, prospectus, CSD, or reviewed identifier feed with exact product match."
            ),
            "source_gate": "Keep ISIN blank until official fund/trust product evidence exposes a valid checksum ISIN.",
        },
    ]


def test_review_strategy_for_keeps_b3_isin_scope_and_source_gates_explicit() -> None:
    assert (
        review_strategy_for("scope_review_before_identifier_fill")
        == "decide_b3_core_extended_or_exclude_before_identifier_work"
    )
    assert (
        review_strategy_for("not_in_current_b3_directory_source_gap")
        == "keep_blank_until_current_b3_directory_or_registry_evidence"
    )
    assert (
        recommended_next_source_for("scope_review_before_identifier_fill")
        == "Current B3 source plus reviewed core, extended, or exclude scope decision before identifier work."
    )
    assert (
        source_gate_for("not_in_current_b3_directory_source_gap")
        == "Keep ISIN blank unless the listing reappears in a current official directory or registry evidence."
    )


def test_b3_official_source_identifier_exposure_counts_source_isin_presence() -> None:
    exposure = b3_official_source_identifier_exposure(
        [
            {
                "exchange": "B3",
                "official": "true",
                "source_key": "b3_instruments_equities",
                "isin": "BRPETRACNPR6",
            },
            {
                "exchange": "B3",
                "official": "true",
                "source_key": "b3_listed_etfs",
                "isin": "",
            },
            {
                "exchange": "NYSE",
                "official": "true",
                "source_key": "nyse_listed",
                "isin": "US0000000000",
            },
        ]
    )

    assert exposure == {
        "b3_instruments_equities": {
            "rows": 1,
            "isin_present_rows": 1,
            "isin_missing_rows": 0,
        },
        "b3_listed_etfs": {
            "rows": 1,
            "isin_present_rows": 0,
            "isin_missing_rows": 1,
        },
    }


def test_build_json_payload_includes_source_traceability_metadata() -> None:
    summary = summarize([], "2026-05-24T00:00:00Z")
    payload = build_json_payload(
        summary=summary,
        rows=[],
        source_files={
            "tickers_csv": "data/tickers.csv",
            "instrument_scopes_csv": "data/instrument_scopes.csv",
            "source_gap_classification_csv": "data/reports/source_gap_classification.csv",
            "source_of_truth_decisions_csv": "data/reports/source_of_truth_decisions.csv",
            "masterfile_reference_csv": "data/masterfiles/reference.csv",
            "cotahist_probe_csv": "data/b3_verification/cotahist_isin_probe_current.csv",
        },
    )

    assert payload["_meta"]["generated_at"] == "2026-05-24T00:00:00Z"
    assert payload["_meta"]["source_files"]["cotahist_probe_csv"] == "data/b3_verification/cotahist_isin_probe_current.csv"
    assert payload["_meta"]["policy"] == summary["policy"]
    assert payload["summary"] == summary
    assert payload["rows"] == []

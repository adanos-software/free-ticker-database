from scripts.build_canada_residual_review import (
    build_json_payload,
    build_review_rows,
    identifier_review_context_for,
    official_source_context_for,
    source_gap_context_for,
    summarize,
)


def test_build_review_rows_tracks_canada_isin_and_figi_gaps() -> None:
    rows = build_review_rows(
        tickers=[
            {
                "listing_key": "TSX::MISS",
                "ticker": "MISS",
                "exchange": "TSX",
                "asset_type": "Stock",
                "name": "Missing Inc",
                "isin": "",
            },
            {
                "listing_key": "NASDAQ::MISS",
                "ticker": "MISS",
                "exchange": "NASDAQ",
                "asset_type": "Stock",
                "name": "Not Canada",
                "isin": "",
            },
        ],
        identifiers_extended=[
            {
                "listing_key": "TSX::MISS",
                "ticker": "MISS",
                "exchange": "TSX",
                "figi": "",
                "figi_source": "",
            }
        ],
        masterfile_rows=[
            {
                "ticker": "MISS",
                "exchange": "TSX",
                "source_key": "tmx_listed_issuers",
                "official": "true",
                "listing_status": "active",
                "isin": "",
                "sector": "",
            }
        ],
        source_gap_rows=[
            {
                "listing_key": "TSX::MISS",
                "ticker": "MISS",
                "exchange": "TSX",
                "field": "missing_isin_primary",
                "gap_class": "official_identifier_not_exposed_source_gap",
            }
        ],
        source_of_truth_rows=[
            {
                "listing_key": "TSX::MISS",
                "ticker": "MISS",
                "exchange": "TSX",
                "source_of_truth_outcome": "accepted_source_gap",
            }
        ],
    )

    assert rows == [
        {
            "listing_key": "TSX::MISS",
            "ticker": "MISS",
            "exchange": "TSX",
            "asset_type": "Stock",
            "name": "Missing Inc",
            "isin": "",
            "figi": "",
            "figi_source": "",
            "openfigi_review_status": "",
            "openfigi_review_decision": "",
            "openfigi_review_source": "",
            "openfigi_reviewed_at": "",
            "missing_isin": "true",
            "missing_figi": "true",
            "source_gap_fields": "missing_isin_primary",
            "source_gap_classes": "official_identifier_not_exposed_source_gap",
            "source_of_truth_outcomes": "accepted_source_gap",
            "source_gap_context": (
                "source_gap_fields=missing_isin_primary;"
                "source_gap_classes=official_identifier_not_exposed_source_gap;"
                "source_of_truth_outcomes=accepted_source_gap"
            ),
            "official_masterfile_match": "true",
            "official_masterfile_sources": "tmx_listed_issuers",
            "official_masterfile_exposes_isin": "false",
            "official_masterfile_exposes_sector": "false",
            "official_source_context": (
                "official_masterfile_match=true;official_masterfile_sources=tmx_listed_issuers;"
                "official_masterfile_exposes_isin=false;official_masterfile_exposes_sector=false"
            ),
            "canada_resolution_queue": "missing_isin_official_canada_masterfiles_do_not_expose_isin",
            "review_strategy": "seek_official_canada_isin_source",
            "queue_evidence_required": "official_csd_issuer_or_reviewed_identifier_source_with_exact_listing_match_and_valid_isin",
            "recommended_next_source": (
                "Official CSD, issuer, prospectus, transfer-agent, or reviewed identifier source exposing a valid ISIN."
            ),
            "source_gate": (
                "Keep ISIN blank until a direct official or reviewed identifier source exposes a valid checksum ISIN."
            ),
            "isin_decision": "missing_isin_official_canada_masterfiles_do_not_expose_isin",
            "figi_decision": "missing_figi_requires_isin_first",
            "isin_apply_eligibility": "keep_blank_until_official_isin_source",
            "figi_apply_eligibility": "blocked_until_isin_resolved",
            "verification_evidence_required": "official_csd_issuer_or_reviewed_identifier_source_with_exact_listing_match_and_valid_isin",
            "identifier_review_context": (
                "missing_isin=true;missing_figi=true;"
                "isin_decision=missing_isin_official_canada_masterfiles_do_not_expose_isin;"
                "figi_decision=missing_figi_requires_isin_first;"
                "isin_apply_eligibility=keep_blank_until_official_isin_source;"
                "figi_apply_eligibility=blocked_until_isin_resolved;"
                "openfigi_review_status=none;openfigi_review_decision=none"
            ),
            "recommended_next_action": "seek_official_csd_issuer_or_reviewed_identifier_source",
        }
    ]


def test_build_review_rows_tracks_openfigi_candidates() -> None:
    rows = build_review_rows(
        tickers=[
            {
                "listing_key": "TSX::SHOP",
                "ticker": "SHOP",
                "exchange": "TSX",
                "asset_type": "Stock",
                "name": "Shopify Inc",
                "isin": "CA82509L1076",
            }
        ],
        identifiers_extended=[
            {
                "listing_key": "TSX::SHOP",
                "ticker": "SHOP",
                "exchange": "TSX",
                "figi": "",
                "figi_source": "",
            }
        ],
        masterfile_rows=[],
        source_gap_rows=[],
        source_of_truth_rows=[],
    )

    assert rows[0]["isin_decision"] == "isin_present"
    assert rows[0]["figi_decision"] == "missing_figi_openfigi_candidate"
    assert rows[0]["canada_resolution_queue"] == "openfigi_candidate_after_isin_gate"
    assert rows[0]["review_strategy"] == "queue_openfigi_by_isin_with_canada_exchange_hint"
    assert rows[0]["queue_evidence_required"] == "openfigi_id_isin_query_with_canada_exchange_hint_then_collision_gate"
    assert rows[0]["recommended_next_source"] == (
        "OpenFIGI ID_ISIN query with Canada exchange hint, followed by collision and cross-ISIN review."
    )
    assert rows[0]["source_gate"] == (
        "OpenFIGI result is a candidate only; apply only after listing-keyed collision and cross-ISIN gates pass."
    )
    assert rows[0]["isin_apply_eligibility"] == "no_isin_action_required"
    assert rows[0]["figi_apply_eligibility"] == "eligible_for_openfigi_queue_after_isin_gate"
    assert rows[0]["verification_evidence_required"] == "openfigi_id_isin_query_with_canada_exchange_hint_then_collision_gate"
    assert rows[0]["source_gap_context"] == source_gap_context_for(rows[0])
    assert rows[0]["official_source_context"] == official_source_context_for(rows[0])
    assert rows[0]["identifier_review_context"] == identifier_review_context_for(rows[0])
    assert rows[0]["recommended_next_action"] == "run_openfigi_for_listing_key_after_isin_gate"


def test_build_review_rows_marks_reviewed_openfigi_no_match_as_source_gap() -> None:
    rows = build_review_rows(
        tickers=[
            {
                "listing_key": "TSX::BKCC",
                "ticker": "BKCC",
                "exchange": "TSX",
                "asset_type": "ETF",
                "name": "Global X Equal Weight Canadian Bank Covered Call ETF",
                "isin": "CA4404541068",
            }
        ],
        identifiers_extended=[
            {
                "listing_key": "TSX::BKCC",
                "ticker": "BKCC",
                "exchange": "TSX",
                "figi": "",
                "figi_source": "",
            }
        ],
        masterfile_rows=[],
        source_gap_rows=[],
        source_of_truth_rows=[],
        openfigi_gap_rows=[
            {
                "listing_key": "TSX::BKCC",
                "isin": "CA4404541068",
                "decision": "no_openfigi_match",
                "review_status": "accepted_source_gap_no_openfigi_match",
                "source": "OpenFIGI ID_ISIN mapping",
                "reviewed_at": "2026-05-24T14:30:57Z",
            }
        ],
    )

    assert rows[0]["openfigi_review_status"] == "accepted_source_gap_no_openfigi_match"
    assert rows[0]["openfigi_review_decision"] == "no_openfigi_match"
    assert rows[0]["figi_decision"] == "missing_figi_reviewed_source_gap_no_openfigi_match"
    assert rows[0]["canada_resolution_queue"] == "reviewed_openfigi_no_match_source_gap"
    assert rows[0]["review_strategy"] == "keep_figi_blank_after_reviewed_openfigi_no_match"
    assert rows[0]["queue_evidence_required"] == "stronger_figi_source_required_openfigi_no_match_reviewed"
    assert rows[0]["recommended_next_source"] == (
        "Stronger FIGI source or reviewed OpenFIGI re-check evidence; existing no-match remains a source gap."
    )
    assert rows[0]["source_gate"] == (
        "Do not re-probe or fill FIGI from symbol/name; keep blank until stronger reviewed FIGI evidence exists."
    )
    assert rows[0]["figi_apply_eligibility"] == "keep_blank_as_reviewed_openfigi_source_gap"
    assert rows[0]["verification_evidence_required"] == "stronger_figi_source_required_openfigi_no_match_reviewed"
    assert rows[0]["source_gap_context"] == source_gap_context_for(rows[0])
    assert rows[0]["official_source_context"] == official_source_context_for(rows[0])
    assert rows[0]["identifier_review_context"] == identifier_review_context_for(rows[0])
    assert rows[0]["recommended_next_action"] == "keep_blank_as_documented_openfigi_source_gap_until_stronger_source"


def test_build_review_rows_marks_reviewed_openfigi_collision_as_source_gap() -> None:
    rows = build_review_rows(
        tickers=[
            {
                "listing_key": "TSXV::MRZL",
                "ticker": "MRZL",
                "exchange": "TSXV",
                "asset_type": "Stock",
                "name": "Mont Royal Resources Limited",
                "isin": "AU0000041758",
            }
        ],
        identifiers_extended=[
            {
                "listing_key": "TSXV::MRZL",
                "ticker": "MRZL",
                "exchange": "TSXV",
                "figi": "",
                "figi_source": "",
            }
        ],
        masterfile_rows=[],
        source_gap_rows=[],
        source_of_truth_rows=[],
        openfigi_gap_rows=[
            {
                "listing_key": "TSXV::MRZL",
                "isin": "AU0000041758",
                "decision": "reject",
                "review_status": "accepted_source_gap_figi_cross_isin_collision",
                "source": "OpenFIGI ID_ISIN mapping",
                "reviewed_at": "2026-05-24T14:31:00Z",
            }
        ],
    )

    assert rows[0]["figi_decision"] == "missing_figi_reviewed_source_gap_figi_cross_isin_collision"
    assert rows[0]["canada_resolution_queue"] == "reviewed_openfigi_cross_isin_collision_source_gap"
    assert rows[0]["review_strategy"] == "keep_figi_blank_after_reviewed_openfigi_cross_isin_collision"
    assert rows[0]["queue_evidence_required"] == "stronger_figi_source_required_openfigi_cross_isin_collision_reviewed"
    assert rows[0]["recommended_next_source"] == (
        "Stronger FIGI source resolving the cross-ISIN collision with exact listing-key evidence."
    )
    assert rows[0]["source_gate"] == (
        "Do not apply cross-ISIN FIGI candidates; require stronger listing-keyed collision resolution."
    )
    assert rows[0]["figi_apply_eligibility"] == "keep_blank_as_reviewed_openfigi_source_gap"
    assert rows[0]["verification_evidence_required"] == "stronger_figi_source_required_openfigi_cross_isin_collision_reviewed"
    assert rows[0]["source_gap_context"] == source_gap_context_for(rows[0])
    assert rows[0]["official_source_context"] == official_source_context_for(rows[0])
    assert rows[0]["identifier_review_context"] == identifier_review_context_for(rows[0])


def test_summarize_counts_canada_residuals() -> None:
    summary = summarize(
        [
            {
                "exchange": "TSX",
                "asset_type": "Stock",
                "missing_isin": "true",
                "missing_figi": "true",
                "isin_decision": "missing_isin_official_canada_masterfiles_do_not_expose_isin",
                "figi_decision": "missing_figi_requires_isin_first",
                "isin_apply_eligibility": "keep_blank_until_official_isin_source",
                "figi_apply_eligibility": "blocked_until_isin_resolved",
                "verification_evidence_required": "official_csd_issuer_or_reviewed_identifier_source_with_exact_listing_match_and_valid_isin",
                "openfigi_review_status": "accepted_source_gap_no_openfigi_match",
                "openfigi_review_decision": "no_openfigi_match",
                "source_gap_fields": "missing_isin_primary",
                "source_gap_classes": "official_identifier_not_exposed_source_gap",
                "source_of_truth_outcomes": "accepted_source_gap",
                "official_masterfile_sources": "tmx_listed_issuers",
                "canada_resolution_queue": "missing_isin_official_canada_masterfiles_do_not_expose_isin",
                "review_strategy": "seek_official_canada_isin_source",
                "queue_evidence_required": "official_csd_issuer_or_reviewed_identifier_source_with_exact_listing_match_and_valid_isin",
                "listing_key": "TSX::MISS",
                "ticker": "MISS",
                "name": "Missing Inc",
                "recommended_next_action": "seek_official_csd_issuer_or_reviewed_identifier_source",
            },
            {
                "exchange": "TSXV",
                "asset_type": "Stock",
                "missing_isin": "true",
                "missing_figi": "true",
                "isin_decision": "missing_isin_core_exclusion_candidate_requires_scope_review",
                "figi_decision": "missing_figi_requires_isin_first",
                "isin_apply_eligibility": "blocked_until_scope_decision",
                "figi_apply_eligibility": "blocked_until_isin_resolved",
                "verification_evidence_required": "scope_decision_for_core_extended_or_exclude_before_identifier_enrichment",
                "openfigi_review_status": "",
                "openfigi_review_decision": "",
                "source_gap_fields": "missing_isin_primary",
                "source_gap_classes": "capital_pool_or_halted_identifier_gap",
                "source_of_truth_outcomes": "core_exclusion_candidate",
                "official_masterfile_sources": "tmx_listed_issuers",
                "canada_resolution_queue": "core_exclusion_candidate_identifier_scope_review",
                "review_strategy": "scope_review_before_canada_identifier_enrichment",
                "queue_evidence_required": "official_listing_scope_decision_for_core_extended_or_exclude",
                "listing_key": "TSXV::CPC",
                "ticker": "CPC",
                "name": "Capital Pool Corp",
                "recommended_next_action": "review_scope_before_identifier_enrichment",
            }
        ],
        "2026-05-24T00:00:00Z",
    )

    assert summary["rows"] == 2
    assert summary["missing_isin_rows"] == 2
    assert summary["missing_figi_rows"] == 2
    assert summary["canada_identifier_backlog"] == {
        "status": "figi_queue_drained_remaining_isin_scope_or_reviewed_source_gaps",
        "rows": 2,
        "scope_decision_required_rows": 1,
        "official_isin_source_required_rows": 1,
        "figi_blocked_until_isin_rows": 2,
        "reviewed_openfigi_source_gap_rows": 0,
        "openfigi_candidate_rows": 0,
        "direct_identifier_apply_allowed_rows": 0,
        "metadata_enrichment_authorized": False,
        "source_gate": (
            "Canadian identifier work remains blocked unless a listing-keyed official CSD, issuer, prospectus, "
            "transfer-agent, OpenFIGI-by-ISIN, or reviewed stronger source proves the value; no symbol/name inference."
        ),
    }
    assert summary["canada_identifier_backlog_queue_totals"] == {
        "core_exclusion_candidate_identifier_scope_review": 1,
        "missing_isin_official_canada_masterfiles_do_not_expose_isin": 1,
    }
    assert summary["canada_identifier_backlog_evidence_required_totals"] == {
        "official_csd_issuer_or_reviewed_identifier_source_with_exact_listing_match_and_valid_isin": 1,
        "scope_decision_for_core_extended_or_exclude_before_identifier_enrichment": 1,
    }
    assert summary["core_exclusion_candidate_rows"] == 1
    assert summary["core_exclusion_candidate_exchange_totals"] == {"TSXV": 1}
    assert summary["core_exclusion_candidate_asset_type_totals"] == {"Stock": 1}
    assert summary["core_exclusion_candidate_resolution_queue_totals"] == {
        "core_exclusion_candidate_identifier_scope_review": 1
    }
    assert summary["core_exclusion_candidate_official_source_totals"] == {"tmx_listed_issuers": 1}
    assert summary["core_exclusion_candidate_source_gap_class_totals"] == {
        "capital_pool_or_halted_identifier_gap": 1
    }
    assert summary["canada_resolution_queue_totals"] == {
        "core_exclusion_candidate_identifier_scope_review": 1,
        "missing_isin_official_canada_masterfiles_do_not_expose_isin": 1,
    }
    assert summary["canada_resolution_queue_exchange_totals"] == {
        "core_exclusion_candidate_identifier_scope_review": {"TSXV": 1},
        "missing_isin_official_canada_masterfiles_do_not_expose_isin": {"TSX": 1},
    }
    assert summary["canada_resolution_queue_asset_type_totals"] == {
        "core_exclusion_candidate_identifier_scope_review": {"Stock": 1},
        "missing_isin_official_canada_masterfiles_do_not_expose_isin": {"Stock": 1},
    }
    assert summary["canada_resolution_queue_source_gap_class_totals"] == {
        "core_exclusion_candidate_identifier_scope_review": {"capital_pool_or_halted_identifier_gap": 1},
        "missing_isin_official_canada_masterfiles_do_not_expose_isin": {
            "official_identifier_not_exposed_source_gap": 1
        },
    }
    assert summary["canada_resolution_queue_official_source_totals"] == {
        "core_exclusion_candidate_identifier_scope_review": {"tmx_listed_issuers": 1},
        "missing_isin_official_canada_masterfiles_do_not_expose_isin": {"tmx_listed_issuers": 1},
    }
    assert summary["canada_resolution_queue_review_strategy_totals"] == {
        "core_exclusion_candidate_identifier_scope_review": {
            "scope_review_before_canada_identifier_enrichment": 1
        },
        "missing_isin_official_canada_masterfiles_do_not_expose_isin": {
            "seek_official_canada_isin_source": 1
        },
    }
    assert summary["canada_resolution_queue_evidence_required_totals"] == {
        "core_exclusion_candidate_identifier_scope_review": {
            "official_listing_scope_decision_for_core_extended_or_exclude": 1
        },
        "missing_isin_official_canada_masterfiles_do_not_expose_isin": {
            "official_csd_issuer_or_reviewed_identifier_source_with_exact_listing_match_and_valid_isin": 1
        },
    }
    assert summary["top_canada_resolution_review_batches"] == [
        {
            "canada_resolution_queue": "core_exclusion_candidate_identifier_scope_review",
            "exchange": "TSXV",
            "official_source": "tmx_listed_issuers",
            "rows": 1,
            "review_strategy": "scope_review_before_canada_identifier_enrichment",
            "evidence_required": "official_listing_scope_decision_for_core_extended_or_exclude",
            "recommended_next_source": (
                "tmx_listed_issuers plus reviewed scope decision for core, extended, or exclude before identifier work."
            ),
            "source_gate": "No ISIN or FIGI enrichment until the listing is reviewed as core, extended, or excluded.",
        },
        {
            "canada_resolution_queue": "missing_isin_official_canada_masterfiles_do_not_expose_isin",
            "exchange": "TSX",
            "official_source": "tmx_listed_issuers",
            "rows": 1,
            "review_strategy": "seek_official_canada_isin_source",
            "evidence_required": "official_csd_issuer_or_reviewed_identifier_source_with_exact_listing_match_and_valid_isin",
            "recommended_next_source": (
                "Official CSD, issuer, prospectus, transfer-agent, or reviewed identifier source exposing a valid ISIN."
            ),
            "source_gate": (
                "Keep ISIN blank until a direct official or reviewed identifier source exposes a valid checksum ISIN."
            ),
        },
    ]
    assert summary["core_exclusion_candidate_examples"] == [
        {
            "listing_key": "TSXV::CPC",
            "ticker": "CPC",
            "exchange": "TSXV",
            "asset_type": "Stock",
            "name": "Capital Pool Corp",
            "canada_resolution_queue": "core_exclusion_candidate_identifier_scope_review",
            "official_masterfile_sources": "tmx_listed_issuers",
            "source_gap_classes": "capital_pool_or_halted_identifier_gap",
            "source_gate": "No ISIN or FIGI enrichment until the listing is reviewed as core, extended, or excluded.",
            "recommended_next_action": "review_scope_before_identifier_enrichment",
        }
    ]
    assert summary["source_gap_field_totals"] == {"missing_isin_primary": 2}
    assert summary["openfigi_review_status_totals"] == {"accepted_source_gap_no_openfigi_match": 1}
    assert summary["openfigi_review_decision_totals"] == {"no_openfigi_match": 1}
    assert summary["official_masterfile_source_totals"] == {"tmx_listed_issuers": 2}
    assert summary["isin_apply_eligibility_totals"] == {
        "blocked_until_scope_decision": 1,
        "keep_blank_until_official_isin_source": 1,
    }
    assert summary["figi_apply_eligibility_totals"] == {"blocked_until_isin_resolved": 2}
    assert summary["verification_evidence_required_totals"] == {
        "official_csd_issuer_or_reviewed_identifier_source_with_exact_listing_match_and_valid_isin": 1,
        "scope_decision_for_core_extended_or_exclude_before_identifier_enrichment": 1,
    }


def test_build_json_payload_includes_source_traceability_metadata() -> None:
    summary = summarize([], "2026-05-24T00:00:00Z")
    payload = build_json_payload(
        summary=summary,
        rows=[],
        source_files={
            "tickers_csv": "data/tickers.csv",
            "identifiers_extended_csv": "data/identifiers_extended.csv",
            "masterfile_reference_csv": "data/masterfiles/reference.csv",
            "source_gap_classification_csv": "data/reports/source_gap_classification.csv",
            "source_of_truth_decisions_csv": "data/reports/source_of_truth_decisions.csv",
            "openfigi_gap_csv": "data/review_overrides/canada_figi_openfigi_gaps.csv",
        },
    )

    assert payload["_meta"]["generated_at"] == "2026-05-24T00:00:00Z"
    assert payload["_meta"]["source_files"]["openfigi_gap_csv"] == "data/review_overrides/canada_figi_openfigi_gaps.csv"
    assert payload["_meta"]["policy"] == summary["policy"]
    assert payload["summary"] == summary
    assert payload["rows"] == []

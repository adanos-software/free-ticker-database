from scripts.build_canada_scope_review_queue import build_payload, select_scope_candidates


def test_select_scope_candidates_keeps_core_exclusion_review_rows_only() -> None:
    rows = [
        {
            "listing_key": "TSX::CDR",
            "canada_resolution_queue": "core_exclusion_candidate_identifier_scope_review",
            "source_of_truth_outcomes": "core_exclusion_candidate",
        },
        {
            "listing_key": "TSX::FIGI",
            "canada_resolution_queue": "reviewed_openfigi_no_match_source_gap",
            "source_of_truth_outcomes": "",
        },
        {
            "listing_key": "TSXV::MISS",
            "canada_resolution_queue": "missing_isin_reviewed_source_gap",
            "source_of_truth_outcomes": "accepted_source_gap",
        },
    ]

    assert select_scope_candidates(rows) == [rows[0]]


def test_build_payload_joins_scope_review_context_for_depositary_rows() -> None:
    payload = build_payload(
        canada_residual_rows=[
            {
                "listing_key": "NEO::HNDA",
                "ticker": "HNDA",
                "exchange": "NEO",
                "asset_type": "Stock",
                "name": "HONDA MOTOR CO LTD CDR (CAD Hedged)",
                "isin": "",
                "figi": "",
                "source_gap_fields": "missing_isin_primary|missing_sector_stock",
                "source_gap_classes": "adr_cdr_or_depositary_identifier_gap|adr_cdr_or_depositary_sector_gap",
                "source_of_truth_outcomes": "core_exclusion_candidate",
                "official_masterfile_sources": "cboe_canada_listing_directory",
                "official_masterfile_match": "true",
                "official_masterfile_exposes_isin": "false",
                "official_masterfile_exposes_sector": "false",
                "canada_resolution_queue": "core_exclusion_candidate_identifier_scope_review",
            }
        ],
        listing_status_rows=[
            {
                "listing_key": "NEO::HNDA",
                "ticker": "HNDA",
                "exchange": "NEO",
                "status": "active",
                "last_observed_at": "2026-05-16T17:22:52Z",
            }
        ],
        instrument_scope_rows=[
            {
                "listing_key": "NEO::HNDA",
                "ticker": "HNDA",
                "exchange": "NEO",
                "instrument_scope": "core",
                "scope_reason": "primary_listing_missing_isin",
            }
        ],
        ohlcv_rows=[
            {
                "listing_key": "NEO::HNDA",
                "ticker": "HNDA",
                "exchange": "NEO",
                "plausibility_status": "not_checked",
                "review_bucket": "not_checked_source_gap_cluster_sample",
            }
        ],
        generated_at="2026-05-29T00:00:00Z",
    )

    assert payload["summary"]["rows"] == 1
    assert payload["summary"]["scope_review_queue_totals"] == {
        "canada_depositary_or_cdr_scope_review": 1
    }
    assert payload["summary"]["listing_history_status_totals"] == {"active": 1}
    row = payload["rows"][0]
    assert row["scope_review_queue"] == "canada_depositary_or_cdr_scope_review"
    assert row["current_scope_reason"] == "primary_listing_missing_isin"
    assert row["verification_evidence_required"] == (
        "official_cdr_depositary_or_exchange_listing_evidence_plus_core_extended_or_exclude_scope_decision"
    )
    assert row["source_gate"].startswith("No ISIN, FIGI, sector, or scope change")
    assert row["review_context"] == (
        "source_gap_classes=adr_cdr_or_depositary_identifier_gap|adr_cdr_or_depositary_sector_gap;"
        "official_masterfile_sources=cboe_canada_listing_directory;"
        "official_masterfile_exposes_isin=false;"
        "current_scope=core;"
        "listing_history_status=active;"
        "ohlcv_plausibility_status=not_checked;"
        "scope_decision_gate=decide_core_extended_or_exclude_before_canada_identifier_or_metadata_enrichment"
    )


def test_build_payload_normalizes_dot_dash_listing_history_keys() -> None:
    payload = build_payload(
        canada_residual_rows=[
            {
                "listing_key": "TSXV::AAA.P",
                "ticker": "AAA.P",
                "exchange": "TSXV",
                "asset_type": "Stock",
                "name": "First Tidal Acquisition Corp.",
                "source_gap_fields": "missing_isin_primary",
                "source_gap_classes": "capital_pool_or_halted_identifier_gap",
                "source_of_truth_outcomes": "core_exclusion_candidate",
                "official_masterfile_sources": "tmx_listed_issuers",
                "official_masterfile_match": "true",
                "official_masterfile_exposes_isin": "false",
                "official_masterfile_exposes_sector": "false",
                "canada_resolution_queue": "core_exclusion_candidate_identifier_scope_review",
            }
        ],
        listing_status_rows=[
            {
                "listing_key": "TSXV::AAA-P",
                "ticker": "AAA-P",
                "exchange": "TSXV",
                "status": "active",
                "last_observed_at": "2026-04-08T17:39:16Z",
            }
        ],
        instrument_scope_rows=[],
        ohlcv_rows=[],
        generated_at="2026-05-29T00:00:00Z",
    )

    assert payload["rows"][0]["listing_history_status"] == "active"
    assert payload["rows"][0]["scope_review_queue"] == "canada_cpc_shell_or_halted_scope_review"
    assert payload["summary"]["listing_history_status_totals"] == {"active": 1}


def test_build_payload_classifies_metadata_scope_review_before_fill() -> None:
    payload = build_payload(
        canada_residual_rows=[
            {
                "listing_key": "TSXV::SHELL",
                "ticker": "SHELL",
                "exchange": "TSXV",
                "asset_type": "Stock",
                "name": "Shell Corp",
                "source_gap_fields": "missing_sector_stock",
                "source_gap_classes": "shell_or_cpc_sector_gap",
                "source_of_truth_outcomes": "core_exclusion_candidate",
                "official_masterfile_sources": "tmx_listed_issuers",
                "official_masterfile_match": "true",
                "official_masterfile_exposes_isin": "true",
                "official_masterfile_exposes_sector": "false",
                "canada_resolution_queue": "core_exclusion_candidate_metadata_scope_review",
            }
        ],
        listing_status_rows=[],
        instrument_scope_rows=[],
        ohlcv_rows=[],
        generated_at="2026-05-29T00:00:00Z",
    )

    row = payload["rows"][0]
    assert row["scope_review_queue"] == "canada_metadata_scope_review_before_sector_or_category_fill"
    assert row["recommended_scope_action"] == "decide_scope_before_sector_or_category_enrichment"
    assert row["source_gate"] == "No sector or category fill until scope is decided with official listing evidence."

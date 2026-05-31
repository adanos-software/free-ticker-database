from scripts.build_asx_scope_review_queue import build_payload, select_scope_candidates


def test_select_scope_candidates_keeps_core_exclusion_rows_only() -> None:
    rows = [
        {
            "listing_key": "ASX::RM1",
            "asx_resolution_queue": "core_exclusion_candidate_identifier_scope_review",
            "source_of_truth_outcome": "core_exclusion_candidate",
        },
        {
            "listing_key": "ASX::RAMHA",
            "asx_resolution_queue": "asx_isin_workbook_name_mismatch_manual_review",
            "source_of_truth_outcome": "accepted_source_gap",
        },
    ]

    assert select_scope_candidates(rows) == [rows[0]]


def test_build_payload_joins_scope_review_context_for_debt_rows() -> None:
    payload = build_payload(
        asx_residual_rows=[
            {
                "listing_key": "ASX::AC2",
                "ticker": "AC2",
                "exchange": "ASX",
                "asset_type": "ETF",
                "name": "ALLIED CREDIT ABS TRUST 2025-1P",
                "field": "missing_isin_primary",
                "gap_class": "debt_or_securitized_identifier_gap",
                "source_of_truth_outcome": "core_exclusion_candidate",
                "official_masterfile_sources": "asx_listed_companies",
                "official_masterfile_match": "true",
                "official_masterfile_exposes_isin": "false",
                "official_masterfile_exposes_sector": "false",
                "asx_isin_probe_decision": "no_asx_match",
                "asx_resolution_queue": "core_exclusion_candidate_identifier_scope_review",
            }
        ],
        listing_status_rows=[
            {
                "listing_key": "ASX::AC2",
                "ticker": "AC2",
                "exchange": "ASX",
                "status": "active",
                "last_observed_at": "2026-05-16T17:22:52Z",
            }
        ],
        instrument_scope_rows=[
            {
                "listing_key": "ASX::AC2",
                "ticker": "AC2",
                "exchange": "ASX",
                "instrument_scope": "core",
                "scope_reason": "primary_listing_missing_isin",
            }
        ],
        ohlcv_rows=[
            {
                "listing_key": "ASX::AC2",
                "ticker": "AC2",
                "exchange": "ASX",
                "plausibility_status": "not_checked",
                "review_bucket": "not_checked_source_gap_cluster_sample",
            }
        ],
        generated_at="2026-05-29T00:00:00Z",
    )

    assert payload["summary"]["rows"] == 1
    assert payload["summary"]["scope_review_queue_totals"] == {"asx_debt_or_securitized_scope_review": 1}
    row = payload["rows"][0]
    assert row["scope_review_queue"] == "asx_debt_or_securitized_scope_review"
    assert row["current_scope_reason"] == "primary_listing_missing_isin"
    assert row["verification_evidence_required"] == (
        "official_asx_registry_issuer_trustee_or_prospectus_evidence_plus_scope_decision"
    )
    assert row["source_gate"].startswith("No ISIN, category, name, symbol, or scope change")
    assert row["review_context"] == (
        "gap_class=debt_or_securitized_identifier_gap;"
        "official_masterfile_sources=asx_listed_companies;"
        "asx_isin_probe_decision=no_asx_match;"
        "current_scope=core;"
        "listing_history_status=active;"
        "ohlcv_plausibility_status=not_checked;"
        "scope_decision_gate=decide_core_extended_or_exclude_before_asx_identifier_or_category_enrichment"
    )


def test_build_payload_classifies_fund_and_inactive_scope_rows() -> None:
    payload = build_payload(
        asx_residual_rows=[
            {
                "listing_key": "ASX::EBTC",
                "ticker": "EBTC",
                "exchange": "ASX",
                "asset_type": "ETF",
                "name": "Global X 21Shares Bitcoin ETF",
                "field": "missing_isin_primary",
                "gap_class": "fund_or_trust_identifier_gap",
                "source_of_truth_outcome": "core_exclusion_candidate",
                "official_masterfile_sources": "asx_listed_companies",
                "official_masterfile_match": "true",
                "official_masterfile_exposes_isin": "false",
                "official_masterfile_exposes_sector": "false",
                "asx_resolution_queue": "core_exclusion_candidate_identifier_scope_review",
            },
            {
                "listing_key": "ASX::AN3",
                "ticker": "AN3",
                "exchange": "ASX",
                "asset_type": "Stock",
                "name": "Anson Resources Ltd",
                "field": "missing_isin_primary",
                "gap_class": "inactive_or_legacy_identifier_gap",
                "source_of_truth_outcome": "core_exclusion_candidate",
                "official_masterfile_sources": "",
                "official_masterfile_match": "false",
                "official_masterfile_exposes_isin": "false",
                "official_masterfile_exposes_sector": "false",
                "asx_resolution_queue": "core_exclusion_candidate_identifier_scope_review",
            },
        ],
        listing_status_rows=[],
        instrument_scope_rows=[],
        ohlcv_rows=[],
        generated_at="2026-05-29T00:00:00Z",
    )

    assert payload["summary"]["scope_review_queue_totals"] == {
        "asx_fund_or_trust_scope_review": 1,
        "asx_inactive_or_legacy_scope_review": 1,
    }
    by_key = {row["listing_key"]: row for row in payload["rows"]}
    assert by_key["ASX::EBTC"]["recommended_scope_action"] == (
        "review_product_currentness_then_choose_core_extended_or_exclude"
    )
    assert by_key["ASX::AN3"]["recommended_scope_action"] == (
        "review_active_or_inactive_status_then_choose_core_extended_or_exclude"
    )
    assert by_key["ASX::AN3"]["source_gate"].startswith("Do not delete, rename, extend, or exclude")

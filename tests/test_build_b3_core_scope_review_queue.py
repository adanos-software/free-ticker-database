from scripts.build_b3_core_scope_review_queue import build_payload, select_scope_candidates


def test_select_scope_candidates_keeps_b3_core_exclusion_rows_only() -> None:
    rows = [
        {
            "listing_key": "B3::AFOF11",
            "exchange": "B3",
            "review_bucket": "scope_review_before_identifier_fill",
            "residual_decision": "core_exclusion_candidate_requires_scope_review",
        },
        {
            "listing_key": "B3::P5RD3",
            "exchange": "B3",
            "review_bucket": "not_in_current_b3_directory_source_gap",
            "residual_decision": "accepted_source_gap_not_in_current_b3_directory",
        },
        {
            "listing_key": "TSXV::AAA",
            "exchange": "TSXV",
            "review_bucket": "scope_review_before_identifier_fill",
            "residual_decision": "core_exclusion_candidate_requires_scope_review",
        },
    ]

    assert select_scope_candidates(rows) == [rows[0]]


def test_build_payload_joins_scope_candidate_context() -> None:
    payload = build_payload(
        residual_rows=[
            {
                "listing_key": "B3::AFOF11",
                "ticker": "AFOF11",
                "exchange": "B3",
                "asset_type": "ETF",
                "name": "Alianza Fofii Fundo De Investimento Imobiliario",
                "gap_class": "fund_or_trust_identifier_gap",
                "current_instrument_scope": "core",
                "current_scope_reason": "primary_listing_missing_isin",
                "source_of_truth_outcome": "core_exclusion_candidate",
                "residual_decision": "core_exclusion_candidate_requires_scope_review",
                "review_bucket": "scope_review_before_identifier_fill",
            }
        ],
        masterfile_gap_rows=[
            {
                "listing_key": "B3::AFOF11",
                "source_presence": "absent_from_all_b3_masterfile_sources",
                "b3_resolution_queue": "absent_from_all_b3_sources_fund_or_receipt_source_gap",
            }
        ],
        listing_status_rows=[
            {
                "listing_key": "B3::AFOF11",
                "status": "active",
                "last_observed_at": "2026-05-16T17:22:52Z",
            }
        ],
        ohlcv_rows=[
            {
                "listing_key": "B3::AFOF11",
                "plausibility_status": "pass",
                "review_bucket": "checked_plausible_sample",
            }
        ],
        generated_at="2026-05-29T00:00:00Z",
    )

    assert payload["summary"]["rows"] == 1
    assert payload["summary"]["scope_review_queue_totals"] == {"b3_fund_or_trust_core_scope_review": 1}
    assert payload["summary"]["masterfile_source_presence_totals"] == {
        "absent_from_all_b3_masterfile_sources": 1
    }
    row = payload["rows"][0]
    assert row["scope_review_queue"] == "b3_fund_or_trust_core_scope_review"
    assert row["verification_evidence_required"] == (
        "current_b3_fund_product_or_issuer_registry_evidence_plus_core_extended_or_exclude_scope_decision"
    )
    assert row["source_gate"].startswith("No ISIN, category, name, or scope change")
    assert row["review_context"] == (
        "gap_class=fund_or_trust_identifier_gap;"
        "masterfile_source_presence=absent_from_all_b3_masterfile_sources;"
        "b3_resolution_queue=absent_from_all_b3_sources_fund_or_receipt_source_gap;"
        "listing_history_status=active;"
        "ohlcv_plausibility_status=pass;"
        "scope_decision_gate=decide_core_extended_or_exclude_before_identifier_or_category_work"
    )


def test_build_payload_classifies_inactive_or_legacy_rows_without_guessing_scope() -> None:
    payload = build_payload(
        residual_rows=[
            {
                "listing_key": "B3::C3RP3",
                "ticker": "C3RP3",
                "exchange": "B3",
                "asset_type": "Stock",
                "name": "COTRASA PARTICIPACOES S.A.",
                "gap_class": "inactive_or_legacy_identifier_gap",
                "current_instrument_scope": "core",
                "current_scope_reason": "primary_listing_missing_isin",
                "source_of_truth_outcome": "core_exclusion_candidate",
                "residual_decision": "core_exclusion_candidate_requires_scope_review",
                "review_bucket": "scope_review_before_identifier_fill",
            }
        ],
        masterfile_gap_rows=[],
        listing_status_rows=[],
        ohlcv_rows=[],
        generated_at="2026-05-29T00:00:00Z",
    )

    assert payload["summary"]["scope_review_queue_totals"] == {"b3_inactive_or_legacy_core_scope_review": 1}
    assert payload["summary"]["listing_history_status_totals"] == {"missing_listing_history_row": 1}
    row = payload["rows"][0]
    assert row["scope_decision_gate"] == "decide_core_extended_or_exclude_before_identifier_or_listing_status_work"
    assert row["recommended_scope_action"] == (
        "review_active_or_inactive_status_then_choose_core_extended_or_exclude_keep_identifier_blank_until_official_isin"
    )
    assert row["source_gate"].startswith("Do not delete, rename, extend, or exclude")

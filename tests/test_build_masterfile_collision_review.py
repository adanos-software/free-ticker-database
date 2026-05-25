from pathlib import Path

from scripts.build_masterfile_collision_review import (
    build_json_payload,
    build_review_rows,
    existing_dataset_context_for,
    identity_resolution_context_for,
    official_source_context_for,
    pair_recommended_next_source_for,
    pair_source_gate_for,
    summarize,
)


def test_build_review_rows_flags_symbol_collision_without_non_symbol_identity() -> None:
    rows = build_review_rows(
        tickers=[
            {
                "ticker": "ADIB",
                "exchange": "EGX",
                "name": "Abu Dhabi Islamic Bank Egypt",
                "asset_type": "Stock",
                "isin": "EGS60111C019",
            }
        ],
        masterfile_rows=[
            {
                "source_key": "adx_market_watch",
                "source_url": "https://www.adx.ae/english/pages/marketwatch.aspx",
                "ticker": "ADIB",
                "name": "Abu Dhabi Islamic Bank PJSC",
                "exchange": "ADX",
                "asset_type": "Stock",
                "listing_status": "active",
                "reference_scope": "exchange_directory",
                "official": "true",
                "isin": "",
                "sector": "Financials",
            }
        ],
    )

    assert rows == [
        {
            "target_listing_key": "ADX::ADIB",
            "ticker": "ADIB",
            "target_exchange": "ADX",
            "official_name": "Abu Dhabi Islamic Bank PJSC",
            "official_asset_type": "Stock",
            "official_isin": "",
            "official_sector": "Financials",
            "official_source_key": "adx_market_watch",
            "official_source_url": "https://www.adx.ae/english/pages/marketwatch.aspx",
            "official_source_context": (
                "official_source_key=adx_market_watch;"
                "official_source_url=https://www.adx.ae/english/pages/marketwatch.aspx"
            ),
            "existing_listing_keys": "EGX::ADIB",
            "existing_exchanges": "EGX",
            "existing_names": "Abu Dhabi Islamic Bank Egypt",
            "existing_asset_types": "Stock",
            "existing_isins": "EGS60111C019",
            "same_isin_listing_keys": "",
            "name_exact_match_listing_keys": "",
            "asset_type_mismatch": "false",
            "existing_dataset_context": (
                "existing_listing_keys=EGX::ADIB;existing_exchanges=EGX;"
                "existing_isins=EGS60111C019;same_isin_listing_keys=none;"
                "name_exact_match_listing_keys=none;asset_type_mismatch=false"
            ),
            "identity_evidence": "asset_type_consistent",
            "identity_resolution_context": (
                "exchange_context=ADX::EGX;"
                "identity_resolution_queue=blocked_symbol_only_missing_non_symbol_identity;"
                "review_bucket=hold_symbol_only_collision_needs_non_symbol_identity;"
                "identity_evidence=asset_type_consistent"
            ),
            "collision_risk_flags": "ticker_reused_on_other_exchange|missing_official_isin|no_exact_name_match",
            "identity_resolution_queue": "blocked_symbol_only_missing_non_symbol_identity",
            "review_bucket": "hold_symbol_only_collision_needs_non_symbol_identity",
            "review_priority": "P3",
            "review_strategy": "batch_hold_symbol_reuse_until_non_symbol_identity_source",
            "verification_evidence_required": "official_non_symbol_identifier_or_keep_absent",
            "recommended_next_source": (
                "Official non-symbol identifier evidence for ADX::EGX, or keep the target listing absent."
            ),
            "source_gate": "Keep absent; ticker equality alone is not identity evidence.",
            "review_decision": "symbol_collision_requires_non_symbol_identity_source",
            "clearance_evidence_required": "official_non_symbol_identifier_or_keep_absent",
            "recommended_next_action": "keep_absent_until_official_isin_or_other_non_symbol_identity_source_distinguishes_listing",
        }
    ]


def test_build_review_rows_flags_same_isin_cross_listing_candidate() -> None:
    rows = build_review_rows(
        tickers=[
            {
                "ticker": "2AAP",
                "exchange": "LSE",
                "name": "Leverage Shares 2x Apple ETP Securities",
                "asset_type": "ETF",
                "isin": "XS2337099563",
            }
        ],
        masterfile_rows=[
            {
                "source_key": "euronext_live",
                "source_url": "https://live.euronext.com/",
                "ticker": "2AAP",
                "name": "Leverage Shares 2x Apple ETP Securities",
                "exchange": "AMS",
                "asset_type": "ETF",
                "listing_status": "active",
                "reference_scope": "exchange_directory",
                "official": "true",
                "isin": "XS2337099563",
                "sector": "",
            }
        ],
    )

    assert rows[0]["target_listing_key"] == "AMS::2AAP"
    assert rows[0]["same_isin_listing_keys"] == "LSE::2AAP"
    assert rows[0]["name_exact_match_listing_keys"] == "LSE::2AAP"
    assert rows[0]["identity_evidence"] == "official_isin|same_isin_existing_listing|exact_name_match|asset_type_consistent"
    assert rows[0]["collision_risk_flags"] == "ticker_reused_on_other_exchange|same_isin_existing_listing|exact_name_match"
    assert rows[0]["identity_resolution_queue"] == "review_cross_listing_same_isin_exact_name"
    assert rows[0]["review_bucket"] == "same_isin_exact_name_cross_listing_candidate"
    assert rows[0]["review_priority"] == "P1"
    assert rows[0]["review_strategy"] == "batch_review_same_isin_exact_name_cross_listing_scope"
    assert (
        rows[0]["verification_evidence_required"]
        == "official_target_and_existing_exchange_directories_confirm_active_same_instrument"
    )
    assert rows[0]["recommended_next_source"] == "Official active-listing directories for both exchanges in AMS::LSE."
    assert (
        rows[0]["source_gate"]
        == "Do not add or merge until both official exchange directories confirm the same active instrument."
    )
    assert rows[0]["official_source_context"] == official_source_context_for(rows[0])
    assert rows[0]["existing_dataset_context"] == existing_dataset_context_for(rows[0])
    assert rows[0]["identity_resolution_context"] == identity_resolution_context_for(rows[0])
    assert rows[0]["review_decision"] == "same_isin_cross_listing_candidate_requires_exchange_scope_review"
    assert rows[0]["clearance_evidence_required"] == "official_target_exchange_listing_status_mic_name_instrument_type"
    assert rows[0]["recommended_next_action"] == "verify_target_exchange_listing_status_mic_name_and_instrument_type_before_cross_listing_add"


def test_build_review_rows_flags_distinct_official_isin_as_new_listing_candidate() -> None:
    rows = build_review_rows(
        tickers=[
            {
                "ticker": "FOO",
                "exchange": "NASDAQ",
                "name": "Foo Holdings Inc.",
                "asset_type": "Stock",
                "isin": "US0000000001",
            }
        ],
        masterfile_rows=[
            {
                "source_key": "cboe_bats",
                "source_url": "https://www.cboe.com/",
                "ticker": "FOO",
                "name": "Foo Europe PLC",
                "exchange": "BATS",
                "asset_type": "Stock",
                "listing_status": "active",
                "reference_scope": "exchange_directory",
                "official": "true",
                "isin": "IE0000000002",
                "sector": "",
            }
        ],
    )

    assert rows[0]["existing_listing_keys"] == "NASDAQ::FOO"
    assert rows[0]["existing_isins"] == "US0000000001"
    assert rows[0]["same_isin_listing_keys"] == ""
    assert rows[0]["identity_evidence"] == "official_isin|asset_type_consistent"
    assert rows[0]["collision_risk_flags"] == "ticker_reused_on_other_exchange|existing_isin_conflict|no_exact_name_match"
    assert rows[0]["identity_resolution_queue"] == "review_distinct_official_isin_new_listing"
    assert rows[0]["review_bucket"] == "distinct_official_isin_new_listing_candidate"
    assert rows[0]["review_priority"] == "P2"
    assert rows[0]["review_strategy"] == "batch_review_distinct_isin_new_listing_candidates"
    assert (
        rows[0]["verification_evidence_required"]
        == "official_target_exchange_listing_key_isin_name_instrument_type_listing_status"
    )
    assert (
        rows[0]["recommended_next_source"]
        == "Official target-exchange listing record for BATS::NASDAQ with listing key, ISIN, name, type, and status."
    )
    assert (
        rows[0]["source_gate"]
        == "Do not add the listing until official target-exchange evidence proves key, ISIN, name, type, and active status."
    )
    assert rows[0]["official_source_context"] == official_source_context_for(rows[0])
    assert rows[0]["existing_dataset_context"] == existing_dataset_context_for(rows[0])
    assert rows[0]["identity_resolution_context"] == identity_resolution_context_for(rows[0])
    assert rows[0]["review_decision"] == "new_listing_candidate_requires_official_listing_add_review"
    assert rows[0]["clearance_evidence_required"] == "official_target_exchange_listing_key_isin_name_instrument_type_listing_status"
    assert rows[0]["recommended_next_action"] == "add_only_after_official_listing_key_isin_name_exchange_and_scope_review"


def test_build_review_rows_marks_asset_type_mismatch_before_addition() -> None:
    rows = build_review_rows(
        tickers=[
            {
                "ticker": "ABC",
                "exchange": "NYSE",
                "name": "ABC Fund",
                "asset_type": "ETF",
                "isin": "US0000000003",
            }
        ],
        masterfile_rows=[
            {
                "source_key": "official_source",
                "ticker": "ABC",
                "name": "ABC Bank",
                "exchange": "ADX",
                "asset_type": "Stock",
                "listing_status": "active",
                "reference_scope": "exchange_directory",
                "official": "true",
                "isin": "AE0000000004",
            }
        ],
    )

    assert rows[0]["asset_type_mismatch"] == "true"
    assert rows[0]["identity_evidence"] == "official_isin|asset_type_conflict"
    assert rows[0]["collision_risk_flags"] == "ticker_reused_on_other_exchange|existing_isin_conflict|no_exact_name_match|asset_type_mismatch"
    assert rows[0]["identity_resolution_queue"] == "blocked_asset_type_conflict"
    assert rows[0]["review_bucket"] == "resolve_asset_type_conflict_before_identity_review"
    assert rows[0]["review_priority"] == "P2"
    assert rows[0]["review_strategy"] == "batch_block_instrument_type_conflict_until_official_resolution"
    assert (
        rows[0]["verification_evidence_required"]
        == "official_instrument_type_resolution_before_listing_identity_review"
    )
    assert (
        rows[0]["recommended_next_source"]
        == "Official instrument-type evidence resolving the asset-type conflict for ADX::NYSE."
    )
    assert (
        rows[0]["source_gate"]
        == "Block identity resolution until official instrument-type evidence resolves the conflict."
    )
    assert rows[0]["official_source_context"] == official_source_context_for(rows[0])
    assert rows[0]["existing_dataset_context"] == existing_dataset_context_for(rows[0])
    assert rows[0]["identity_resolution_context"] == identity_resolution_context_for(rows[0])
    assert rows[0]["review_decision"] == "new_listing_candidate_requires_official_listing_add_review"
    assert rows[0]["clearance_evidence_required"] == "official_instrument_type_resolution_before_listing_identity_review"
    assert rows[0]["recommended_next_action"] == "resolve_instrument_type_before_any_listing_addition"


def test_build_review_rows_excludes_existing_target_listing_and_plain_missing() -> None:
    rows = build_review_rows(
        tickers=[
            {"ticker": "AAPL", "exchange": "NASDAQ", "asset_type": "Stock"},
        ],
        masterfile_rows=[
            {
                "ticker": "AAPL",
                "exchange": "NASDAQ",
                "asset_type": "Stock",
                "listing_status": "active",
                "reference_scope": "exchange_directory",
            },
            {
                "ticker": "1306",
                "exchange": "TSE",
                "asset_type": "ETF",
                "listing_status": "active",
                "reference_scope": "exchange_directory",
            },
        ],
    )

    assert rows == []


def test_summarize_counts_collision_review_dimensions() -> None:
    summary = summarize(
        [
            {
                "review_decision": "symbol_collision_requires_non_symbol_identity_source",
                "target_exchange": "ADX",
                "official_asset_type": "Stock",
                "asset_type_mismatch": "false",
                "review_bucket": "hold_symbol_only_collision_needs_non_symbol_identity",
                "review_priority": "P3",
                "official_source_key": "adx_market_watch",
                "collision_risk_flags": "ticker_reused_on_other_exchange|missing_official_isin|no_exact_name_match",
                "identity_evidence": "asset_type_consistent",
                "identity_resolution_queue": "blocked_symbol_only_missing_non_symbol_identity",
                "review_strategy": "batch_hold_symbol_reuse_until_non_symbol_identity_source",
                "verification_evidence_required": "official_non_symbol_identifier_or_keep_absent",
                "clearance_evidence_required": "official_non_symbol_identifier_or_keep_absent",
            },
            {
                "review_decision": "new_listing_candidate_requires_official_listing_add_review",
                "target_exchange": "BATS",
                "official_asset_type": "ETF",
                "asset_type_mismatch": "true",
                "review_bucket": "resolve_asset_type_conflict_before_identity_review",
                "review_priority": "P2",
                "official_source_key": "cboe_bats",
                "collision_risk_flags": "ticker_reused_on_other_exchange|existing_isin_conflict|asset_type_mismatch",
                "identity_evidence": "official_isin|asset_type_conflict",
                "identity_resolution_queue": "blocked_asset_type_conflict",
                "review_strategy": "batch_block_instrument_type_conflict_until_official_resolution",
                "verification_evidence_required": "official_instrument_type_resolution_before_listing_identity_review",
                "clearance_evidence_required": "official_instrument_type_resolution_before_listing_identity_review",
            },
        ],
        "2026-05-24T00:00:00Z",
    )

    assert summary["rows"] == 2
    assert summary["decision_totals"] == {
        "new_listing_candidate_requires_official_listing_add_review": 1,
        "symbol_collision_requires_non_symbol_identity_source": 1,
    }
    assert summary["exchange_totals"] == {"ADX": 1, "BATS": 1}
    assert summary["asset_type_mismatch_totals"] == {"false": 1, "true": 1}
    assert summary["review_bucket_totals"] == {
        "hold_symbol_only_collision_needs_non_symbol_identity": 1,
        "resolve_asset_type_conflict_before_identity_review": 1,
    }
    assert summary["review_priority_totals"] == {"P2": 1, "P3": 1}
    assert summary["collision_risk_flag_totals"] == {
        "asset_type_mismatch": 1,
        "existing_isin_conflict": 1,
        "missing_official_isin": 1,
        "no_exact_name_match": 1,
        "ticker_reused_on_other_exchange": 2,
    }
    assert summary["identity_resolution_queue_totals"] == {
        "blocked_asset_type_conflict": 1,
        "blocked_symbol_only_missing_non_symbol_identity": 1,
    }
    assert summary["identity_resolution_backlog"] == {
        "status": "identity_resolution_review_queue_open",
        "rows": 2,
        "same_isin_exact_name_scope_review_rows": 0,
        "same_isin_name_or_scope_reconciliation_rows": 0,
        "distinct_official_isin_listing_add_review_rows": 0,
        "asset_type_conflict_blocked_rows": 1,
        "symbol_only_non_symbol_identity_required_rows": 1,
        "direct_listing_add_allowed_rows": 0,
        "symbol_only_resolution_authorized": False,
        "source_gate": (
            "Masterfile collision rows remain review queues only; listing additions, merges, renames, "
            "or enrichments require official non-symbol identity evidence for the target listing."
        ),
    }
    assert summary["identity_resolution_risk_flag_totals"] == {
        "blocked_asset_type_conflict": {
            "asset_type_mismatch": 1,
            "existing_isin_conflict": 1,
            "ticker_reused_on_other_exchange": 1,
        },
        "blocked_symbol_only_missing_non_symbol_identity": {
            "missing_official_isin": 1,
            "no_exact_name_match": 1,
            "ticker_reused_on_other_exchange": 1,
        },
    }
    assert summary["identity_resolution_exchange_totals"] == {
        "blocked_asset_type_conflict": {"BATS": 1},
        "blocked_symbol_only_missing_non_symbol_identity": {"ADX": 1},
    }
    assert summary["identity_resolution_asset_type_totals"] == {
        "blocked_asset_type_conflict": {"ETF": 1},
        "blocked_symbol_only_missing_non_symbol_identity": {"Stock": 1},
    }
    assert summary["identity_resolution_official_source_totals"] == {
        "blocked_asset_type_conflict": {"cboe_bats": 1},
        "blocked_symbol_only_missing_non_symbol_identity": {"adx_market_watch": 1},
    }
    assert summary["identity_resolution_existing_exchange_pair_totals"] == {
        "blocked_asset_type_conflict": {},
        "blocked_symbol_only_missing_non_symbol_identity": {},
    }
    assert summary["identity_resolution_pair_review_strategy_totals"] == {}
    assert summary["identity_resolution_review_strategy_totals"] == {
        "blocked_asset_type_conflict": {"batch_block_instrument_type_conflict_until_official_resolution": 1},
        "blocked_symbol_only_missing_non_symbol_identity": {
            "batch_hold_symbol_reuse_until_non_symbol_identity_source": 1
        },
    }
    assert summary["identity_resolution_evidence_required_totals"] == {
        "blocked_asset_type_conflict": {
            "official_instrument_type_resolution_before_listing_identity_review": 1
        },
        "blocked_symbol_only_missing_non_symbol_identity": {"official_non_symbol_identifier_or_keep_absent": 1},
    }
    assert summary["identity_resolution_identity_evidence_totals"] == {
        "blocked_asset_type_conflict": {"asset_type_conflict": 1, "official_isin": 1},
        "blocked_symbol_only_missing_non_symbol_identity": {"asset_type_consistent": 1},
    }
    assert summary["top_identity_resolution_pair_review_batches"] == []
    assert summary["same_isin_exact_name_scope_review_rows"] == 0
    assert summary["top_same_isin_exact_name_scope_review_batches"] == []
    assert summary["clearance_evidence_totals"] == {
        "official_instrument_type_resolution_before_listing_identity_review": 1,
        "official_non_symbol_identifier_or_keep_absent": 1,
    }
    assert summary["official_source_totals"] == {"adx_market_watch": 1, "cboe_bats": 1}


def test_summarize_top_pair_batches_include_source_and_gate_traceability() -> None:
    summary = summarize(
        [
            {
                "review_decision": "same_isin_cross_listing_candidate_requires_exchange_scope_review",
                "target_exchange": "AMS",
                "existing_exchanges": "LSE",
                "official_asset_type": "Stock",
                "asset_type_mismatch": "false",
                "review_bucket": "same_isin_exact_name_cross_listing_candidate",
                "review_priority": "P1",
                "official_source_key": "euronext_equities",
                "collision_risk_flags": "ticker_reused_on_other_exchange|same_isin_existing_listing|exact_name_match",
                "identity_evidence": "official_isin|same_isin_existing_listing|exact_name_match|asset_type_consistent",
                "identity_resolution_queue": "review_cross_listing_same_isin_exact_name",
                "review_strategy": "batch_review_same_isin_exact_name_cross_listing_scope",
                "verification_evidence_required": (
                    "official_target_and_existing_exchange_directories_confirm_active_same_instrument"
                ),
                "clearance_evidence_required": "official_target_exchange_listing_status_mic_name_instrument_type",
            },
        ],
        "2026-05-24T00:00:00Z",
    )

    assert summary["top_identity_resolution_pair_review_batches"] == [
        {
            "identity_resolution_queue": "review_cross_listing_same_isin_exact_name",
            "exchange_pair": "AMS::LSE",
            "rows": 1,
            "review_strategy": "batch_review_same_isin_exact_name_cross_listing_scope",
            "evidence_required": "official_target_and_existing_exchange_directories_confirm_active_same_instrument",
            "recommended_next_source": "Official active-listing directories for both exchanges in AMS::LSE.",
            "source_gate": (
                "Do not add or merge until both official exchange directories confirm the same active instrument."
            ),
        }
    ]
    assert summary["same_isin_exact_name_scope_review_rows"] == 1
    assert summary["top_same_isin_exact_name_scope_review_batches"] == [
        {
            "exchange_pair": "AMS::LSE",
            "official_source_key": "euronext_equities",
            "official_asset_type": "Stock",
            "clearance_evidence_required": "official_target_exchange_listing_status_mic_name_instrument_type",
            "rows": 1,
            "review_strategy": "batch_review_same_isin_exact_name_cross_listing_scope",
            "evidence_required": "official_target_and_existing_exchange_directories_confirm_active_same_instrument",
            "recommended_next_source": "Official active-listing directories for both exchanges in AMS::LSE.",
            "source_gate": (
                "Do not add or merge until both official exchange directories confirm the same active instrument."
            ),
        }
    ]


def test_pair_source_instructions_block_symbol_only_identity_resolution() -> None:
    assert (
        pair_recommended_next_source_for("blocked_symbol_only_missing_non_symbol_identity", "ADX::EGX")
        == "Official non-symbol identifier evidence for ADX::EGX, or keep the target listing absent."
    )
    assert (
        pair_source_gate_for("blocked_symbol_only_missing_non_symbol_identity")
        == "Keep absent; ticker equality alone is not identity evidence."
    )


def test_build_json_payload_carries_source_files_and_policy() -> None:
    summary = summarize([], "2026-05-24T00:00:00Z")
    payload = build_json_payload(
        summary,
        [],
        source_files={
            "listings_csv": Path("data/listings.csv"),
            "masterfile_reference_csv": Path("data/masterfiles/reference.csv"),
        },
    )

    assert payload["_meta"] == {
        "generated_at": "2026-05-24T00:00:00Z",
        "source_files": {
            "listings_csv": "data/listings.csv",
            "masterfile_reference_csv": "data/masterfiles/reference.csv",
        },
        "policy": summary["policy"],
    }
    assert payload["summary"] == summary
    assert payload["rows"] == []

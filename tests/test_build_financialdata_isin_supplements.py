from __future__ import annotations

from scripts.build_financialdata_isin_supplements import (
    apply_eligibility_for,
    build_financialdata_isin_supplements,
    financialdata_discovery_context_for,
    financialdata_review_queue_for,
    official_identity_context_for,
    review_priority_for_queue,
    review_strategy_for_queue,
    supplement_review_context_for,
    verification_evidence_for,
)


def test_build_financialdata_isin_supplements_accepts_unique_name_gated_official_isin() -> None:
    supplements, reviews, summary = build_financialdata_isin_supplements(
        financialdata_rows=[
            {
                "financialdata_symbol": "RELIANCE.NS",
                "base_ticker": "RELIANCE",
                "mapped_exchange": "NSE_IN",
                "registrant_name": "Reliance Industries Limited",
                "review_scope": "global_expansion_candidate",
            }
        ],
        masterfile_rows=[
            {
                "ticker": "RELIANCE",
                "name": "Reliance Industries Limited",
                "exchange": "NSE_IN",
                "asset_type": "Stock",
                "listing_status": "active",
                "reference_scope": "exchange_directory",
                "source_key": "nse_india_securities_available",
                "source_url": "https://example.com/nse",
                "isin": "INE002A01018",
                "sector": "",
            }
        ],
        listing_rows=[],
    )

    assert supplements == [
        {
            "ticker": "RELIANCE",
            "name": "Reliance Industries Limited",
            "exchange": "NSE_IN",
            "asset_type": "Stock",
            "sector": "",
            "country": "India",
            "country_code": "IN",
            "isin": "INE002A01018",
            "aliases": "Reliance Industries Limited",
            "source_key": "nse_india_securities_available",
            "source_url": "https://example.com/nse",
            "reference_scope": "exchange_directory",
        }
    ]
    assert reviews[0].decision == "accept"
    assert reviews[0].financialdata_review_queue == "official_name_gated_supplement_candidate"
    assert reviews[0].review_priority == "P1"
    assert reviews[0].review_strategy == "apply_only_after_official_active_isin_name_gate_and_collision_checks"
    assert (
        reviews[0].apply_eligibility
        == "eligible_for_supplement_only_after_official_active_isin_name_gate_and_duplicate_checks"
    )
    assert (
        reviews[0].verification_evidence_required
        == "official_active_masterfile_valid_isin_name_gate_no_existing_ticker_or_isin_collision"
    )
    assert (
        reviews[0].source_gate
        == "Write supplement only after official active listing, valid ISIN, name gate, and duplicate checks pass."
    )
    assert (
        reviews[0].financialdata_discovery_context
        == "financialdata_exchange=NSE_IN;financialdata_ticker=RELIANCE;"
        "financialdata_review_scope=global_expansion_candidate;financialdata_name_present=true"
    )
    assert (
        reviews[0].official_identity_context
        == "official_exchange=NSE_IN;official_ticker=RELIANCE;"
        "official_source_key=nse_india_securities_available;official_reference_scope=exchange_directory;"
        "official_isin_present=true;official_name_present=true"
    )
    assert (
        reviews[0].supplement_review_context
        == "decision=accept;reason=official_isin_name_gated_unique_primary;"
        "financialdata_review_queue=official_name_gated_supplement_candidate;"
        "apply_eligibility=eligible_for_supplement_only_after_official_active_isin_name_gate_and_duplicate_checks;"
        "verification_evidence_required=official_active_masterfile_valid_isin_name_gate_no_existing_ticker_or_isin_collision"
    )
    assert summary["supplement_rows"] == 1
    assert summary["apply_eligibility_counts"] == {
        "eligible_for_supplement_only_after_official_active_isin_name_gate_and_duplicate_checks": 1
    }
    assert summary["top_financialdata_supplement_review_batches"] == [
        {
            "review_priority": "P1",
            "financialdata_review_queue": "official_name_gated_supplement_candidate",
            "decision": "accept",
            "reason": "official_isin_name_gated_unique_primary",
            "financialdata_exchange": "NSE_IN",
            "financialdata_review_scope": "global_expansion_candidate",
            "official_source_key": "nse_india_securities_available",
            "review_strategy": "apply_only_after_official_active_isin_name_gate_and_collision_checks",
            "apply_eligibility": "eligible_for_supplement_only_after_official_active_isin_name_gate_and_duplicate_checks",
            "verification_evidence_required": (
                "official_active_masterfile_valid_isin_name_gate_no_existing_ticker_or_isin_collision"
            ),
            "rows": 1,
            "recommended_next_source": (
                "Official active masterfile or registry row already matched by ticker/name with valid ISIN."
            ),
            "source_gate": (
                "Write supplement only after official active listing, valid ISIN, name gate, and duplicate checks pass."
            ),
        }
    ]


def test_build_financialdata_isin_supplements_matches_hkex_padded_ticker() -> None:
    supplements, reviews, _summary = build_financialdata_isin_supplements(
        financialdata_rows=[
            {
                "financialdata_symbol": "5.HK",
                "base_ticker": "5",
                "mapped_exchange": "HKEX",
                "registrant_name": "HSBC Holdings",
                "review_scope": "global_expansion_candidate",
            }
        ],
        masterfile_rows=[
            {
                "ticker": "00005",
                "name": "HSBC HOLDINGS",
                "exchange": "HKEX",
                "asset_type": "Stock",
                "listing_status": "active",
                "reference_scope": "exchange_directory",
                "source_key": "hkex_securities_list",
                "source_url": "https://example.com/hkex",
                "isin": "GB0005405286",
                "sector": "",
            }
        ],
        listing_rows=[],
    )

    assert supplements[0]["ticker"] == "00005"
    assert supplements[0]["isin"] == "GB0005405286"
    assert reviews[0].decision == "accept"


def test_build_financialdata_isin_supplements_matches_bse_by_unique_name() -> None:
    supplements, reviews, _summary = build_financialdata_isin_supplements(
        financialdata_rows=[
            {
                "financialdata_symbol": "500209.BO",
                "base_ticker": "500209",
                "mapped_exchange": "BSE_IN",
                "registrant_name": "Infosys Limited",
                "review_scope": "global_expansion_candidate",
            }
        ],
        masterfile_rows=[
            {
                "ticker": "INFY",
                "name": "Infosys Ltd",
                "exchange": "BSE_IN",
                "asset_type": "Stock",
                "listing_status": "active",
                "reference_scope": "exchange_directory",
                "source_key": "bse_india_scrips",
                "source_url": "https://example.com/bse",
                "isin": "INE009A01021",
                "sector": "",
            }
        ],
        listing_rows=[],
    )

    assert supplements[0]["ticker"] == "INFY"
    assert supplements[0]["isin"] == "INE009A01021"
    assert reviews[0].decision == "accept"


def test_build_financialdata_isin_supplements_blocks_existing_isin_and_ticker() -> None:
    supplements, reviews, summary = build_financialdata_isin_supplements(
        financialdata_rows=[
            {
                "financialdata_symbol": "RELIANCE.NS",
                "base_ticker": "RELIANCE",
                "mapped_exchange": "NSE_IN",
                "registrant_name": "Reliance Industries Limited",
                "review_scope": "global_expansion_candidate",
            },
            {
                "financialdata_symbol": "SHEL.L",
                "base_ticker": "SHEL",
                "mapped_exchange": "LSE",
                "registrant_name": "Shell plc",
                "review_scope": "current_exchange_gap",
            },
        ],
        masterfile_rows=[
            {
                "ticker": "RELIANCE",
                "name": "Reliance Industries Limited",
                "exchange": "NSE_IN",
                "asset_type": "Stock",
                "listing_status": "active",
                "reference_scope": "exchange_directory",
                "source_key": "nse",
                "source_url": "https://example.com/nse",
                "isin": "INE002A01018",
                "sector": "",
            },
            {
                "ticker": "SHEL",
                "name": "Shell plc",
                "exchange": "LSE",
                "asset_type": "Stock",
                "listing_status": "active",
                "reference_scope": "exchange_directory",
                "source_key": "lse",
                "source_url": "https://example.com/lse",
                "isin": "GB00BP6MXD84",
                "sector": "",
            },
        ],
            listing_rows=[
                {"ticker": "RELIANCE", "exchange": "LSE", "isin": "", "asset_type": "Stock"},
                {"ticker": "OTHER", "exchange": "NASDAQ", "isin": "GB00BP6MXD84", "asset_type": "Stock"},
            ],
        )

    assert supplements == []
    assert [review.reason for review in reviews] == [
        "isin_already_exists_in_database",
        "ticker_already_exists_globally",
    ]
    assert [review.apply_eligibility for review in reviews] == [
        "no_supplement_apply_existing_identifier_or_collision_guard",
        "no_supplement_apply_existing_identifier_or_collision_guard",
    ]
    assert summary["supplement_rows"] == 0


def test_build_financialdata_isin_supplements_preserves_existing_supplement_after_rebuild() -> None:
    existing_supplement = {
        "ticker": "RELIANCE",
        "name": "Reliance Industries Limited",
        "exchange": "NSE_IN",
        "asset_type": "Stock",
        "sector": "",
        "country": "India",
        "country_code": "IN",
        "isin": "INE002A01018",
        "aliases": "Reliance Industries Limited",
        "source_key": "nse_india_securities_available",
        "source_url": "https://example.com/nse",
        "reference_scope": "exchange_directory",
    }

    supplements, reviews, summary = build_financialdata_isin_supplements(
        financialdata_rows=[
            {
                "financialdata_symbol": "RELIANCE.NS",
                "base_ticker": "RELIANCE",
                "mapped_exchange": "NSE_IN",
                "registrant_name": "Reliance Industries Limited",
                "review_scope": "current_exchange_gap",
            }
        ],
        masterfile_rows=[
            {
                "ticker": "RELIANCE",
                "name": "Reliance Industries Limited",
                "exchange": "NSE_IN",
                "asset_type": "Stock",
                "listing_status": "active",
                "reference_scope": "exchange_directory",
                "source_key": "nse_india_securities_available",
                "source_url": "https://example.com/nse",
                "isin": "INE002A01018",
                "sector": "",
            }
        ],
        listing_rows=[
            {
                "ticker": "RELIANCE",
                "name": "Reliance Industries Limited",
                "exchange": "NSE_IN",
                "asset_type": "Stock",
                "isin": "INE002A01018",
            }
        ],
        existing_supplement_rows=[existing_supplement],
    )

    assert supplements == [existing_supplement]
    assert reviews[0].decision == "preserve"
    assert reviews[0].reason == "already_in_financialdata_supplement"
    assert reviews[0].apply_eligibility == "preserve_existing_reviewed_supplement_no_new_apply"
    assert summary["preserved_supplement_rows"] == 1
    assert summary["supplement_rows"] == 1


def test_financialdata_review_gates_block_ambiguous_or_scope_limited_rows() -> None:
    assert (
        apply_eligibility_for("reject", "ambiguous_official_isin_candidates")
        == "blocked_until_unique_official_isin_candidate_resolved"
    )
    assert (
        verification_evidence_for("reject", "exchange_not_allowed_for_isin_supplement")
        == "explicit_exchange_scope_decision_before_financialdata_discovery_use"
    )
    queue = financialdata_review_queue_for("reject", "exchange_not_allowed_for_isin_supplement")
    assert queue == "review_exchange_scope_before_financialdata_use"
    assert review_priority_for_queue(queue) == "P2"
    assert review_strategy_for_queue(queue) == "decide_exchange_scope_before_any_financialdata_discovery_apply"


def test_financialdata_review_contexts_are_derived_from_row_fields() -> None:
    row = {
        "financialdata_exchange": "KRX",
        "financialdata_ticker": "005930",
        "financialdata_review_scope": "current_exchange_gap",
        "financialdata_name": "Samsung Electronics Co Ltd",
        "official_exchange": "KRX",
        "official_ticker": "005930",
        "official_source_key": "krx_listed_companies",
        "official_reference_scope": "exchange_directory",
        "official_isin": "KR7005930003",
        "official_name": "Samsung Electronics",
        "decision": "accept",
        "reason": "official_isin_name_gated_unique_primary",
        "financialdata_review_queue": "official_name_gated_supplement_candidate",
        "apply_eligibility": "eligible_for_supplement_only_after_official_active_isin_name_gate_and_duplicate_checks",
        "verification_evidence_required": "official_active_masterfile_valid_isin_name_gate_no_existing_ticker_or_isin_collision",
    }

    assert (
        financialdata_discovery_context_for(row)
        == "financialdata_exchange=KRX;financialdata_ticker=005930;"
        "financialdata_review_scope=current_exchange_gap;financialdata_name_present=true"
    )
    assert (
        official_identity_context_for(row)
        == "official_exchange=KRX;official_ticker=005930;official_source_key=krx_listed_companies;"
        "official_reference_scope=exchange_directory;official_isin_present=true;official_name_present=true"
    )
    assert (
        supplement_review_context_for(row)
        == "decision=accept;reason=official_isin_name_gated_unique_primary;"
        "financialdata_review_queue=official_name_gated_supplement_candidate;"
        "apply_eligibility=eligible_for_supplement_only_after_official_active_isin_name_gate_and_duplicate_checks;"
        "verification_evidence_required=official_active_masterfile_valid_isin_name_gate_no_existing_ticker_or_isin_collision"
    )

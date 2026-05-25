import pytest

from scripts.build_canada_figi_queue import build_queue_rows, reviewed_openfigi_gap_keys, select_candidates, summarize


def test_select_candidates_requires_canada_openfigi_gate() -> None:
    rows = [
        {
            "listing_key": "TSX::SHOP",
            "ticker": "SHOP",
            "exchange": "TSX",
            "asset_type": "Stock",
            "isin": "CA82509L1076",
            "missing_figi": "true",
            "figi_decision": "missing_figi_openfigi_candidate",
        },
        {
            "listing_key": "TSX::NOISIN",
            "ticker": "NOISIN",
            "exchange": "TSX",
            "asset_type": "Stock",
            "isin": "",
            "missing_figi": "true",
            "figi_decision": "missing_figi_requires_isin_first",
        },
        {
            "listing_key": "NASDAQ::AAPL",
            "ticker": "AAPL",
            "exchange": "NASDAQ",
            "asset_type": "Stock",
            "isin": "US0378331005",
            "missing_figi": "true",
            "figi_decision": "missing_figi_openfigi_candidate",
        },
    ]

    assert [row["listing_key"] for row in select_candidates(rows)] == ["TSX::SHOP"]


def test_build_queue_rows_assigns_batches_and_canada_hint() -> None:
    rows = build_queue_rows(
        [
            {
                "listing_key": "NEO::AEMX",
                "ticker": "AEMX",
                "exchange": "NEO",
                "asset_type": "ETF",
                "name": "AGF Emerging Markets ex China Fund Series ETF",
                "isin": "CA00858F1099",
                "missing_figi": "true",
                "figi_decision": "missing_figi_openfigi_candidate",
            },
            {
                "listing_key": "TSX::SHOP",
                "ticker": "SHOP",
                "exchange": "TSX",
                "asset_type": "Stock",
                "name": "Shopify Inc",
                "isin": "CA82509L1076",
                "missing_figi": "true",
                "figi_decision": "missing_figi_openfigi_candidate",
            },
        ],
        batch_size=1,
    )

    assert rows[0]["listing_key"] == "NEO::AEMX"
    assert rows[0]["batch_id"] == "canada-figi-0001"
    assert rows[0]["openfigi_exchange_hint"] == "CN"
    assert rows[0]["apply_eligibility"] == "eligible_for_openfigi_probe_only_no_figi_apply_until_collision_gate"
    assert (
        rows[0]["verification_evidence_required"]
        == "openfigi_id_isin_query_with_canada_exchange_hint_then_collision_and_cross_isin_review"
    )
    assert rows[1]["batch_id"] == "canada-figi-0002"


def test_build_queue_rows_excludes_reviewed_openfigi_source_gaps() -> None:
    rows = build_queue_rows(
        [
            {
                "listing_key": "TSX::BKCC",
                "ticker": "BKCC",
                "exchange": "TSX",
                "asset_type": "ETF",
                "name": "Global X Equal Weight Canadian Bank Covered Call ETF",
                "isin": "CA4404541068",
                "missing_figi": "true",
                "figi_decision": "missing_figi_openfigi_candidate",
            },
            {
                "listing_key": "TSX::SHOP",
                "ticker": "SHOP",
                "exchange": "TSX",
                "asset_type": "Stock",
                "name": "Shopify Inc",
                "isin": "CA82509L1076",
                "missing_figi": "true",
                "figi_decision": "missing_figi_openfigi_candidate",
            },
        ],
        batch_size=100,
        reviewed_gap_rows=[
            {
                "listing_key": "TSX::BKCC",
                "isin": "CA4404541068",
                "requested_exchange_hint": "CN",
                "decision": "no_openfigi_match",
                "review_status": "accepted_source_gap_no_openfigi_match",
            }
        ],
    )

    assert [row["listing_key"] for row in rows] == ["TSX::SHOP"]


def test_build_queue_rows_excludes_reviewed_figi_collision_source_gaps() -> None:
    rows = build_queue_rows(
        [
            {
                "listing_key": "TSXV::MRZL",
                "ticker": "MRZL",
                "exchange": "TSXV",
                "asset_type": "Stock",
                "name": "Mont Royal Resources Limited",
                "isin": "AU0000041758",
                "missing_figi": "true",
                "figi_decision": "missing_figi_openfigi_candidate",
            },
            {
                "listing_key": "TSXV::SAFE",
                "ticker": "SAFE",
                "exchange": "TSXV",
                "asset_type": "Stock",
                "name": "Safe Resource Corp.",
                "isin": "CA0000000001",
                "missing_figi": "true",
                "figi_decision": "missing_figi_openfigi_candidate",
            },
        ],
        batch_size=100,
        reviewed_gap_rows=[
            {
                "listing_key": "TSXV::MRZL",
                "isin": "AU0000041758",
                "requested_exchange_hint": "CN",
                "decision": "reject",
                "review_status": "accepted_source_gap_figi_cross_isin_collision",
            }
        ],
    )

    assert [row["listing_key"] for row in rows] == ["TSXV::SAFE"]


def test_reviewed_openfigi_gap_keys_requires_accepted_source_gap_status() -> None:
    keys = reviewed_openfigi_gap_keys(
        [
            {
                "listing_key": "TSX::BKCC",
                "isin": "CA4404541068",
                "requested_exchange_hint": "CN",
                "decision": "no_openfigi_match",
                "review_status": "accepted_source_gap_no_openfigi_match",
            },
            {
                "listing_key": "TSX::SHOP",
                "isin": "CA82509L1076",
                "requested_exchange_hint": "CN",
                "decision": "no_openfigi_match",
                "review_status": "needs_review",
            },
        ]
    )

    assert keys == {("TSX::BKCC", "CA4404541068", "CN")}


def test_reviewed_openfigi_gap_keys_includes_figi_collision_status() -> None:
    keys = reviewed_openfigi_gap_keys(
        [
            {
                "listing_key": "TSXV::MRZL",
                "isin": "AU0000041758",
                "requested_exchange_hint": "CN",
                "decision": "reject",
                "review_status": "accepted_source_gap_figi_cross_isin_collision",
            }
        ]
    )

    assert keys == {("TSXV::MRZL", "AU0000041758", "CN")}


def test_build_queue_rows_rejects_invalid_batch_size() -> None:
    with pytest.raises(ValueError, match="batch_size"):
        build_queue_rows([], batch_size=0)


def test_summarize_counts_batches() -> None:
    summary = summarize(
        [
            {
                "batch_id": "canada-figi-0001",
                "exchange": "TSX",
                "asset_type": "Stock",
                "openfigi_exchange_hint": "CN",
                "apply_eligibility": "eligible_for_openfigi_probe_only_no_figi_apply_until_collision_gate",
                "verification_evidence_required": "openfigi_id_isin_query_with_canada_exchange_hint_then_collision_and_cross_isin_review",
            },
            {
                "batch_id": "canada-figi-0002",
                "exchange": "TSXV",
                "asset_type": "ETF",
                "openfigi_exchange_hint": "CN",
                "apply_eligibility": "eligible_for_openfigi_probe_only_no_figi_apply_until_collision_gate",
                "verification_evidence_required": "openfigi_id_isin_query_with_canada_exchange_hint_then_collision_and_cross_isin_review",
            },
        ],
        "2026-05-24T00:00:00Z",
        batch_size=1,
        excluded_openfigi_gap_rows=3,
    )

    assert summary["rows"] == 2
    assert summary["batches"] == 2
    assert summary["excluded_openfigi_gap_rows"] == 3
    assert summary["exchange_totals"] == {"TSX": 1, "TSXV": 1}
    assert summary["openfigi_exchange_hint_totals"] == {"CN": 2}
    assert summary["apply_eligibility_totals"] == {
        "eligible_for_openfigi_probe_only_no_figi_apply_until_collision_gate": 2,
        "keep_figi_blank_as_reviewed_openfigi_source_gap_until_stronger_source": 3,
    }
    assert summary["verification_evidence_required_totals"] == {
        "openfigi_id_isin_query_with_canada_exchange_hint_then_collision_and_cross_isin_review": 2,
        "stronger_figi_source_required_for_reviewed_openfigi_no_match_or_cross_isin_collision": 3,
    }
    assert summary["review_strategy_totals"] == {
        "probe_openfigi_by_valid_isin_with_canada_exchange_hint_then_collision_review": 2,
        "keep_reviewed_openfigi_source_gaps_excluded_until_stronger_source": 3,
    }
    assert summary["top_canada_figi_queue_review_batches"] == [
        {
            "exchange": "reviewed_openfigi_source_gap",
            "asset_type": "all",
            "openfigi_exchange_hint": "CN",
            "rows": 3,
            "review_strategy": "keep_reviewed_openfigi_source_gaps_excluded_until_stronger_source",
            "apply_eligibility": "keep_figi_blank_as_reviewed_openfigi_source_gap_until_stronger_source",
            "verification_evidence_required": "stronger_figi_source_required_for_reviewed_openfigi_no_match_or_cross_isin_collision",
            "recommended_next_source": (
                "Stronger FIGI source or reviewed OpenFIGI re-check evidence for the existing reviewed source gap."
            ),
            "source_gate": (
                "Do not re-probe or fill FIGI from symbol/name; keep blank until stronger reviewed FIGI evidence exists."
            ),
        },
        {
            "exchange": "TSX",
            "asset_type": "Stock",
            "openfigi_exchange_hint": "CN",
            "rows": 1,
            "review_strategy": "probe_openfigi_by_valid_isin_with_canada_exchange_hint_then_collision_review",
            "apply_eligibility": "eligible_for_openfigi_probe_only_no_figi_apply_until_collision_gate",
            "verification_evidence_required": "openfigi_id_isin_query_with_canada_exchange_hint_then_collision_and_cross_isin_review",
            "recommended_next_source": (
                "OpenFIGI ID_ISIN request with Canada exchange hint CN, followed by collision and cross-ISIN review."
            ),
            "source_gate": (
                "OpenFIGI result is a candidate only; apply only after listing-keyed collision and cross-ISIN gates pass."
            ),
        },
        {
            "exchange": "TSXV",
            "asset_type": "ETF",
            "openfigi_exchange_hint": "CN",
            "rows": 1,
            "review_strategy": "probe_openfigi_by_valid_isin_with_canada_exchange_hint_then_collision_review",
            "apply_eligibility": "eligible_for_openfigi_probe_only_no_figi_apply_until_collision_gate",
            "verification_evidence_required": "openfigi_id_isin_query_with_canada_exchange_hint_then_collision_and_cross_isin_review",
            "recommended_next_source": (
                "OpenFIGI ID_ISIN request with Canada exchange hint CN, followed by collision and cross-ISIN review."
            ),
            "source_gate": (
                "OpenFIGI result is a candidate only; apply only after listing-keyed collision and cross-ISIN gates pass."
            ),
        },
    ]


def test_summarize_documents_empty_drained_queue() -> None:
    summary = summarize([], "2026-05-24T00:00:00Z", batch_size=100, excluded_openfigi_gap_rows=1)

    assert summary["rows"] == 0
    assert summary["apply_eligibility_totals"] == {
        "keep_figi_blank_as_reviewed_openfigi_source_gap_until_stronger_source": 1,
        "no_active_openfigi_probe_rows": 1,
    }
    assert summary["verification_evidence_required_totals"] == {
        "none_queue_drained_or_candidates_excluded_as_reviewed_source_gaps": 1,
        "stronger_figi_source_required_for_reviewed_openfigi_no_match_or_cross_isin_collision": 1,
    }
    assert summary["review_strategy_totals"] == {
        "keep_reviewed_openfigi_source_gaps_excluded_until_stronger_source": 1,
        "no_active_openfigi_probe_rows_after_gates": 1,
    }

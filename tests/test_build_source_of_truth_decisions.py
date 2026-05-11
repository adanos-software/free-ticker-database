from __future__ import annotations

from scripts.build_source_of_truth_decisions import build_decisions, summarize


def classification(gap_class: str, *, listing_key: str = "OTC::ABC") -> dict[str, str]:
    exchange, ticker = listing_key.split("::", maxsplit=1)
    return {
        "field": "missing_sector_stock",
        "target_field": "stock_sector",
        "listing_key": listing_key,
        "ticker": ticker,
        "exchange": exchange,
        "asset_type": "Stock",
        "name": "ABC Corp.",
        "gap_class": gap_class,
        "review_needed": "true",
        "confidence_policy": "Require reviewed evidence.",
        "recommended_next_source": "Official source.",
        "source_gate": "Exact symbol/name.",
    }


def test_build_decisions_maps_gap_classes_to_outcomes() -> None:
    rows = build_decisions(
        [
            classification("official_identifier_source_gap", listing_key="TSX::AAA"),
            classification("adr_cdr_or_depositary_sector_gap", listing_key="TSX::CDR"),
            classification("otc_sector_source_gap", listing_key="OTC::BBB"),
            classification("debt_or_securitized_identifier_gap", listing_key="ASX::CCC"),
        ]
    )

    by_key = {row.listing_key: row for row in rows}
    assert by_key["TSX::AAA"].source_of_truth_outcome == "official_fill_required"
    assert by_key["TSX::AAA"].fill_action == "fill_from_source"
    assert by_key["TSX::CDR"].source_of_truth_outcome == "core_exclusion_candidate"
    assert by_key["OTC::BBB"].source_of_truth_outcome == "accepted_source_gap"
    assert by_key["OTC::BBB"].fill_action == "leave_blank_until_source_available"
    assert by_key["ASX::CCC"].source_of_truth_outcome == "core_exclusion_candidate"
    assert by_key["ASX::CCC"].core_action == "review_for_core_exclusion"


def test_summarize_counts_outcomes_and_classes() -> None:
    rows = build_decisions(
        [
            classification("generic_etf_category_source_gap", listing_key="LSE::ETF1"),
            classification("fund_or_trust_identifier_gap", listing_key="B3::FUND11"),
        ]
    )

    summary = summarize(rows, "2026-05-11T00:00:00Z")

    assert summary["rows"] == 2
    assert summary["outcome_totals"] == {
        "core_exclusion_candidate": 1,
        "official_fill_required": 1,
    }
    assert summary["policy"]["source_of_truth_program"]

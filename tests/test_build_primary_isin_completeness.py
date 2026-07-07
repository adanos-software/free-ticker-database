from scripts.build_primary_isin_completeness import (
    allowed_source_path_for,
    build_rows,
    render_markdown,
    summarize,
)


def test_allowed_source_path_prioritizes_d1_sources_by_exchange() -> None:
    assert allowed_source_path_for("ASX", "official_identifier_not_exposed_source_gap", "Stock").startswith(
        "ASX ISIN workbook"
    )
    assert allowed_source_path_for("TSX", "official_identifier_not_exposed_source_gap", "Stock").startswith(
        "TMX/CDS-listed"
    )
    assert allowed_source_path_for("NASDAQ", "official_identifier_not_exposed_source_gap", "Stock").startswith(
        "OpenFIGI"
    )
    assert allowed_source_path_for("SSE", "official_identifier_not_exposed_source_gap", "Stock").startswith(
        "ESMA/FCA"
    )
    assert "prospectus" in allowed_source_path_for("B3", "fund_or_trust_identifier_gap", "ETF")


def test_build_rows_joins_gap_and_source_of_truth_decisions() -> None:
    rows = build_rows(
        [
            {
                "listing_key": "NASDAQ::ABC",
                "ticker": "ABC",
                "exchange": "NASDAQ",
                "asset_type": "Stock",
                "name": "ABC Inc",
                "scope_reason": "primary_listing_missing_isin",
            },
            {
                "listing_key": "ASX::NOTE",
                "ticker": "NOTE",
                "exchange": "ASX",
                "asset_type": "ETF",
                "name": "Note Trust",
                "scope_reason": "primary_listing_missing_isin",
            },
            {
                "listing_key": "NYSE::HASISIN",
                "ticker": "HASISIN",
                "exchange": "NYSE",
                "asset_type": "Stock",
                "name": "Has ISIN Inc",
                "scope_reason": "primary_listing",
            },
        ],
        [
            {
                "field": "missing_isin_primary",
                "listing_key": "NASDAQ::ABC",
                "gap_class": "official_identifier_not_exposed_source_gap",
                "source_gate": "exact_symbol_name_match_required",
            },
            {
                "field": "missing_isin_primary",
                "listing_key": "ASX::NOTE",
                "gap_class": "fund_or_trust_identifier_gap",
                "source_gate": "exact_fund_name_required",
            },
        ],
        [
            {
                "field": "missing_isin_primary",
                "listing_key": "NASDAQ::ABC",
                "source_of_truth_outcome": "accepted_source_gap",
            },
            {
                "field": "missing_isin_primary",
                "listing_key": "ASX::NOTE",
                "source_of_truth_outcome": "core_exclusion_candidate",
            },
        ],
    )

    assert [row["listing_key"] for row in rows] == ["NASDAQ::ABC", "ASX::NOTE"]
    assert rows[0]["priority_rank"] == "1"
    assert rows[0]["apply_eligibility"] == "eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates"
    assert rows[0]["source_gate"].startswith(
        "require_valid_isin_checksum_exact_listing_identity_match_and_no_existing_listing_or_identifier_collision"
    )
    assert rows[1]["priority_rank"] == "2"
    assert rows[1]["apply_eligibility"] == "blocked_until_core_or_extended_scope_decision"
    assert rows[1]["source_gate"].startswith("scope_review_required_before_isin_work")


def test_summarize_and_markdown_expose_priority_counts_and_policy() -> None:
    rows = [
        {
            "priority_rank": "1",
            "listing_key": "NASDAQ::ABC",
            "ticker": "ABC",
            "exchange": "NASDAQ",
            "asset_type": "Stock",
            "name": "ABC Inc",
            "gap_class": "official_identifier_not_exposed_source_gap",
            "source_of_truth_outcome": "accepted_source_gap",
            "allowed_source_path": "OpenFIGI ticker→FIGI→ISIN",
            "apply_eligibility": "eligible_only_after_allowed_source_identity_checksum_and_no_collision_gates",
            "source_gate": "require_valid_isin_checksum_exact_listing_identity_match_and_no_existing_listing_or_identifier_collision",
            "next_action": "probe OpenFIGI/GLEIF/FIRDS candidates",
        },
        {
            "priority_rank": "9",
            "listing_key": "B3::FUND",
            "ticker": "FUND",
            "exchange": "B3",
            "asset_type": "ETF",
            "name": "Fund",
            "gap_class": "fund_or_trust_identifier_gap",
            "source_of_truth_outcome": "core_exclusion_candidate",
            "allowed_source_path": "official fund/trust prospectus",
            "apply_eligibility": "blocked_until_core_or_extended_scope_decision",
            "source_gate": "scope_review_required_before_isin_work",
            "next_action": "review official instrument scope before identifier enrichment",
        },
    ]

    summary = summarize(rows, "2026-07-07T00:00:00Z")
    markdown = render_markdown(rows, summary)

    assert summary["missing_primary_isin_rows"] == 2
    assert summary["priority_exchange_rows"] == 1
    assert summary["blocked_rows"] == 1
    assert summary["eligible_after_allowed_source_gates"] == 1
    assert summary["priority_exchange_totals"] == {"NASDAQ": 1}
    assert "Allowed D1 sources are GLEIF" in markdown
    assert "Every apply still requires a valid ISIN checksum" in markdown

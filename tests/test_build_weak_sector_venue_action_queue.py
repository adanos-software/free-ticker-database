from scripts.build_weak_sector_venue_action_queue import build_action_rows, summarize


def test_build_action_rows_groups_official_sector_candidates_by_raw_value() -> None:
    rows = build_action_rows(
        [
            {
                "listing_key": "NGX::ABCTRANS",
                "exchange": "NGX",
                "weak_sector_resolution_queue": "official_sector_candidate_normalization_review",
                "official_masterfile_sources": "ngx_company_profile_directory|ngx_equities_price_list",
                "official_masterfile_sector_values": "SERVICES",
                "review_priority": "P1",
                "gap_class": "official_industry_taxonomy_unavailable_gap",
                "source_of_truth_outcome": "accepted_source_gap",
            },
            {
                "listing_key": "NGX::AIRTELAFRI",
                "exchange": "NGX",
                "weak_sector_resolution_queue": "official_sector_candidate_normalization_review",
                "official_masterfile_sources": "ngx_company_profile_directory|ngx_equities_price_list",
                "official_masterfile_sector_values": "SERVICES",
                "review_priority": "P1",
                "gap_class": "official_industry_taxonomy_unavailable_gap",
                "source_of_truth_outcome": "accepted_source_gap",
            },
        ]
    )

    assert rows == [
        {
            "exchange": "NGX",
            "weak_sector_resolution_queue": "official_sector_candidate_normalization_review",
            "official_masterfile_sources": "ngx_company_profile_directory|ngx_equities_price_list",
            "official_masterfile_sector_values": "SERVICES",
            "rows": "2",
            "review_priority": "P1",
            "action_queue": "review_official_sector_value_to_canonical_mapping",
            "review_strategy": "normalize_official_sector_candidate_before_apply",
            "evidence_required": "official_venue_sector_value_with_canonical_mapping_and_exact_listing_key_match",
            "source_gate": (
                "Do not map broad official labels without an explicit canonical-sector rule and exact "
                "listing-key evidence."
            ),
            "recommended_next_action": (
                "Review whether the official raw sector value can be mapped to a canonical sector; leave sector "
                "blank unless that mapping is defensible."
            ),
            "gap_class_totals": "official_industry_taxonomy_unavailable_gap:2",
            "source_of_truth_outcome_totals": "accepted_source_gap:2",
            "example_listing_keys": "NGX::ABCTRANS|NGX::AIRTELAFRI",
        }
    ]


def test_build_action_rows_keeps_taxonomy_unavailable_rows_blocked() -> None:
    rows = build_action_rows(
        [
            {
                "listing_key": "CSE_MA::ABC",
                "exchange": "CSE_MA",
                "weak_sector_resolution_queue": "venue_official_taxonomy_unavailable_source_gap",
                "official_masterfile_sources": "",
                "official_masterfile_sector_values": "",
                "review_priority": "P2",
                "gap_class": "official_industry_taxonomy_unavailable_gap",
                "source_of_truth_outcome": "accepted_source_gap",
            }
        ]
    )

    assert rows[0]["official_masterfile_sources"] == "none"
    assert rows[0]["action_queue"] == "restore_or_add_venue_official_taxonomy_parser"
    assert rows[0]["evidence_required"] == "new_or_restored_official_venue_industry_taxonomy_source"
    assert rows[0]["source_gate"] == "Keep sector blank until a venue-official taxonomy parser or source exists."


def test_build_action_rows_keeps_scope_candidates_before_sector_fill() -> None:
    rows = build_action_rows(
        [
            {
                "listing_key": "PSE::SHELL",
                "exchange": "PSE",
                "weak_sector_resolution_queue": "core_exclusion_candidate_scope_review_before_sector_fill",
                "official_masterfile_sources": "pse_listed_company_directory",
                "official_masterfile_sector_values": "",
                "review_priority": "P1",
                "gap_class": "shell_or_cpc_sector_gap",
                "source_of_truth_outcome": "core_exclusion_candidate",
            }
        ]
    )

    assert rows[0]["action_queue"] == "decide_scope_before_sector_enrichment"
    assert rows[0]["review_strategy"] == "scope_review_before_weak_sector_enrichment"
    assert rows[0]["evidence_required"] == "scope_decision_for_core_extended_or_exclude_before_sector_enrichment"
    assert rows[0]["source_gate"] == "No sector fill until scope is decided as core, extended, or excluded."


def test_summarize_counts_batches_and_underlying_rows() -> None:
    action_rows = [
        {
            "exchange": "NGX",
            "weak_sector_resolution_queue": "official_sector_candidate_normalization_review",
            "rows": "2",
            "action_queue": "review_official_sector_value_to_canonical_mapping",
            "evidence_required": "official_venue_sector_value_with_canonical_mapping_and_exact_listing_key_match",
        },
        {
            "exchange": "CSE_MA",
            "weak_sector_resolution_queue": "venue_official_taxonomy_unavailable_source_gap",
            "rows": "3",
            "action_queue": "restore_or_add_venue_official_taxonomy_parser",
            "evidence_required": "new_or_restored_official_venue_industry_taxonomy_source",
        },
    ]

    summary = summarize(action_rows, "2026-05-29T00:00:00Z")

    assert summary["generated_at"] == "2026-05-29T00:00:00Z"
    assert summary["batches"] == 2
    assert summary["rows"] == 5
    assert summary["exchange_totals"] == {"CSE_MA": 3, "NGX": 2}
    assert summary["direct_sector_apply_allowed_rows"] == 0
    assert summary["metadata_enrichment_authorized"] is False

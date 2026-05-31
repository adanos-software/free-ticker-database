from scripts.build_canada_improvement_action_queue import build_action_rows, summarize


def residual_row(
    listing_key: str,
    exchange: str,
    queue: str,
    official_sources: str,
    *,
    evidence: str = "official_listing_scope_decision_for_core_extended_or_exclude",
) -> dict[str, str]:
    return {
        "listing_key": listing_key,
        "exchange": exchange,
        "canada_resolution_queue": queue,
        "official_masterfile_sources": official_sources,
        "queue_evidence_required": evidence,
        "review_strategy": "scope_review_before_canada_identifier_enrichment",
        "recommended_next_source": "tmx_listed_issuers plus reviewed scope decision for core, extended, or exclude before identifier work.",
        "source_gate": "No ISIN or FIGI enrichment until the listing is reviewed as core, extended, or excluded.",
    }


def test_build_action_rows_groups_canada_residuals_by_campaign_exchange_queue_and_source() -> None:
    rows = build_action_rows(
        [
            residual_row("TSX::AAA", "TSX", "core_exclusion_candidate_identifier_scope_review", "tmx_listed_issuers"),
            residual_row("TSX::BBB", "TSX", "core_exclusion_candidate_identifier_scope_review", "tmx_listed_issuers"),
            residual_row(
                "TSXV::CCC",
                "TSXV",
                "missing_isin_official_canada_masterfiles_do_not_expose_isin",
                "tmx_listed_issuers",
                evidence="official_csd_issuer_or_reviewed_identifier_source_with_exact_listing_match_and_valid_isin",
            ),
            residual_row(
                "TSX::DDD",
                "TSX",
                "reviewed_openfigi_no_match_source_gap",
                "tmx_etf_screener",
                evidence="stronger_figi_source_required_openfigi_no_match_reviewed",
            ),
        ]
    )

    assert rows[0]["campaign"] == "canada_scope_blocker"
    assert rows[0]["exchange"] == "TSX"
    assert rows[0]["rows"] == "2"
    assert rows[0]["action_queue"] == "decide_scope_before_identifier_or_metadata_enrichment"
    assert rows[0]["example_listing_keys"] == "TSX::AAA|TSX::BBB"
    assert rows[1]["campaign"] == "canada_isin_source_gap"
    assert rows[1]["action_queue"] == "seek_official_csd_issuer_or_transfer_agent_isin_source"
    assert rows[2]["campaign"] == "canada_figi_reviewed_source_gap"
    assert rows[2]["action_queue"] == "keep_figi_blank_until_stronger_reviewed_figi_source"


def test_build_action_rows_keeps_missing_sources_explicit() -> None:
    rows = build_action_rows(
        [
            residual_row(
                "TSX::NOPE",
                "TSX",
                "missing_isin_reviewed_source_gap",
                "",
                evidence="stronger_official_identifier_source_before_isin_fill",
            )
        ]
    )

    assert rows[0]["official_sources"] == "none"
    assert rows[0]["campaign"] == "canada_isin_source_gap"
    assert rows[0]["evidence_required"] == "stronger_official_identifier_source_before_isin_fill"


def test_summarize_preserves_figi_and_scope_gate_context() -> None:
    action_rows = [
        {
            "campaign": "canada_scope_blocker",
            "exchange": "TSXV",
            "action_queue": "decide_scope_before_identifier_or_metadata_enrichment",
            "rows": "10",
        },
        {
            "campaign": "canada_figi_reviewed_source_gap",
            "exchange": "TSX",
            "action_queue": "keep_figi_blank_until_stronger_reviewed_figi_source",
            "rows": "5",
        },
    ]
    figi_payload = {"summary": {"rows": 0, "excluded_openfigi_gap_rows": 151}}

    summary = summarize(
        action_rows=action_rows,
        scope_rows=[{"listing_key": "TSXV::A"}],
        figi_queue_payload=figi_payload,
        generated_at="2026-05-29T00:00:00Z",
    )

    assert summary["generated_at"] == "2026-05-29T00:00:00Z"
    assert summary["batches"] == 2
    assert summary["underlying_review_rows"] == 15
    assert summary["campaign_totals"] == {"canada_figi_reviewed_source_gap": 5, "canada_scope_blocker": 10}
    assert summary["scope_queue_rows"] == 1
    assert summary["figi_queue_rows"] == 0
    assert summary["figi_excluded_reviewed_source_gaps"] == 151
    assert summary["direct_identifier_apply_allowed_rows"] == 0
    assert summary["metadata_enrichment_authorized"] is False

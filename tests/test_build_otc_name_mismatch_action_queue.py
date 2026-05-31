from scripts.build_otc_name_mismatch_action_queue import build_action_rows, summarize


def name_row(
    listing_key: str,
    review_class: str,
    *,
    isin: str = "",
    official_sources: str = "otc_markets_stock_screener",
    priority: str = "high",
) -> dict[str, str]:
    return {
        "listing_key": listing_key,
        "review_class": review_class,
        "review_priority": priority,
        "official_sources": official_sources,
        "isin": isin,
        "apply_eligibility": "blocked_until_isin_anchored_issuer_history_review",
        "verification_evidence_required": "official_or_reviewed_isin_bearing_source_matching_current_issuer_listing_key_and_name",
        "review_strategy": "verify_isin_anchored_issuer_history_before_name_change",
        "recommended_next_source": "Official or reviewed ISIN-bearing issuer-history source matching current issuer, listing key, and name.",
        "source_gate": "Do not change the name until ISIN-anchored evidence proves the same current issuer.",
    }


def test_build_action_rows_groups_by_review_class_source_isin_and_deepseek_triage() -> None:
    rows = build_action_rows(
        name_mismatch_rows=[
            name_row("OTC::AECX", "probable_otc_rename_or_symbol_reuse", isin="US92855W2017"),
            name_row("OTC::AKOM", "probable_otc_rename_or_symbol_reuse", isin="US00774B2088"),
            name_row(
                "OTC::ISRMF",
                "stale_or_symbol_reuse_without_isin",
                official_sources="otc_markets_security_profile",
                priority="critical",
            ),
        ],
        deepseek_rows=[
            {
                "listing_key": "OTC::AECX",
                "deepseek_decision_candidate": "needs_official_evidence",
            }
        ],
    )

    assert rows[0]["review_class"] == "stale_or_symbol_reuse_without_isin"
    assert rows[0]["action_queue"] == "resolve_or_quarantine_before_trusting_otc_symbol"
    assert rows[0]["isin_presence"] == "without_isin"
    assert rows[0]["deepseek_triage"] == "not_triaged_by_deepseek"
    assert rows[1]["review_class"] == "probable_otc_rename_or_symbol_reuse"
    assert rows[1]["rows"] == "1"
    assert rows[1]["deepseek_triage"] == "deepseek_needs_official_evidence"
    assert rows[1]["example_listing_keys"] == "OTC::AECX"
    assert rows[2]["review_class"] == "probable_otc_rename_or_symbol_reuse"
    assert rows[2]["rows"] == "1"
    assert rows[2]["deepseek_triage"] == "not_triaged_by_deepseek"
    assert rows[2]["example_listing_keys"] == "OTC::AKOM"


def test_build_action_rows_keeps_abbreviation_reviews_matcher_only() -> None:
    rows = build_action_rows(
        name_mismatch_rows=[
            {
                **name_row(
                    "OTC::AIRIQ",
                    "weak_abbreviation_or_truncation_review",
                    isin="CA0091204036",
                    priority="medium",
                ),
                "apply_eligibility": "matcher_tuning_only_no_metadata_apply_until_exact_identity_review",
                "verification_evidence_required": "official_alias_or_abbreviation_evidence_with_exact_listing_identity_match",
                "review_strategy": "review_official_alias_or_abbreviation_before_matcher_tuning",
                "recommended_next_source": "Official alias, abbreviation, issuer, OTC profile, or registry evidence matching the exact listing identity.",
                "source_gate": "Tune matcher only after official alias evidence; do not change metadata from abbreviation alone.",
            }
        ],
        deepseek_rows=[],
    )

    assert rows == [
        {
            "review_class": "weak_abbreviation_or_truncation_review",
            "review_priority": "medium",
            "official_sources": "otc_markets_stock_screener",
            "isin_presence": "with_isin",
            "deepseek_triage": "not_triaged_by_deepseek",
            "rows": "1",
            "action_queue": "review_official_alias_before_matcher_tuning",
            "apply_eligibility": "matcher_tuning_only_no_metadata_apply_until_exact_identity_review",
            "verification_evidence_required": "official_alias_or_abbreviation_evidence_with_exact_listing_identity_match",
            "review_strategy": "review_official_alias_or_abbreviation_before_matcher_tuning",
            "recommended_next_source": (
                "Official alias, abbreviation, issuer, OTC profile, or registry evidence matching the exact "
                "listing identity."
            ),
            "source_gate": "Tune matcher only after official alias evidence; do not change metadata from abbreviation alone.",
            "example_listing_keys": "OTC::AIRIQ",
        }
    ]


def test_summarize_blocks_name_and_metadata_changes() -> None:
    action_rows = [
        {
            "review_class": "stale_or_symbol_reuse_without_isin",
            "review_priority": "critical",
            "action_queue": "resolve_or_quarantine_before_trusting_otc_symbol",
            "deepseek_triage": "deepseek_needs_official_evidence",
            "official_sources": "otc_markets_security_profile",
            "rows": "7",
        },
        {
            "review_class": "weak_abbreviation_or_truncation_review",
            "review_priority": "medium",
            "action_queue": "review_official_alias_before_matcher_tuning",
            "deepseek_triage": "not_triaged_by_deepseek",
            "official_sources": "otc_markets_stock_screener",
            "rows": "74",
        },
    ]

    summary = summarize(action_rows, "2026-05-29T00:00:00Z")

    assert summary["generated_at"] == "2026-05-29T00:00:00Z"
    assert summary["batches"] == 2
    assert summary["rows"] == 81
    assert summary["review_class_totals"] == {
        "stale_or_symbol_reuse_without_isin": 7,
        "weak_abbreviation_or_truncation_review": 74,
    }
    assert summary["direct_name_changes_authorized"] is False
    assert summary["metadata_enrichment_authorized"] is False

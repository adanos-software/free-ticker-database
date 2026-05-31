from __future__ import annotations

import csv

from scripts.build_otc_name_mismatch_review import (
    apply_eligibility_for,
    build_review_rows,
    classify_name_mismatch,
    decision_review_context_for,
    identity_review_context_for,
    official_source_context_for,
    recommended_next_source_for,
    review_strategy_for,
    source_gate_for,
    summarize,
    verification_evidence_for,
    write_csv,
    write_markdown,
)


def entry_row(
    ticker: str,
    name: str,
    isin: str,
    *,
    asset_type: str = "Stock",
) -> dict[str, str]:
    return {
        "listing_key": f"OTC::{ticker}",
        "ticker": ticker,
        "exchange": "OTC",
        "asset_type": asset_type,
        "name": name,
        "country": "United States",
        "country_code": "US",
        "isin": isin,
        "instrument_scope": "extended",
        "scope_reason": "otc_listing",
        "primary_listing_key": f"OTC::{ticker}",
        "venue_status": "official_full",
        "evidence_level": "official_reference",
        "evidence_sources": "official:OTC Markets:otc_markets_stock_screener",
        "quality_status": "warn",
        "quality_score": "65",
        "issue_count": "1",
        "issue_types": "official_name_mismatch",
        "issues": "[]",
        "recommended_action": "review_or_backfill",
    }


def official_ref(ticker: str, name: str) -> dict[str, str]:
    return {
        "source_key": "otc_markets_stock_screener",
        "provider": "OTC Markets",
        "source_url": "https://example.test",
        "ticker": ticker,
        "name": name,
        "exchange": "OTC",
        "asset_type": "Stock",
        "listing_status": "active",
        "reference_scope": "exchange_directory",
        "official": "true",
        "isin": "",
        "sector": "",
    }


def test_classify_name_mismatch_separates_rename_and_stale_without_isin():
    assert classify_name_mismatch("CurrentC Power Corporation", "ACADIA ENERGY CORP", "US92855W2017") == (
        "probable_otc_rename_or_symbol_reuse",
        "high",
        "verify_current_issuer_with_isin_source_before_name_update",
    )
    assert classify_name_mismatch("Old Issuer Inc", "NEW ISSUER CORP", "") == (
        "stale_or_symbol_reuse_without_isin",
        "critical",
        "verify_or_quarantine_before_trusting_listing",
    )


def test_classify_name_mismatch_flags_abbreviation_review():
    assert classify_name_mismatch("AirIQ Inc", "AIRLQ INC NEW", "CA0091204036") == (
        "weak_abbreviation_or_truncation_review",
        "medium",
        "review_before_metadata_update",
    )


def test_apply_and_evidence_gates_block_unsafe_otc_name_changes():
    assert (
        apply_eligibility_for("stale_or_symbol_reuse_without_isin")
        == "blocked_until_official_issuer_identity_source_or_quarantine_decision"
    )
    assert (
        verification_evidence_for("probable_otc_rename_or_symbol_reuse")
        == "official_or_reviewed_isin_bearing_source_matching_current_issuer_listing_key_and_name"
    )
    assert apply_eligibility_for("matcher_false_positive") == "no_metadata_change_authorized_tighten_matcher_only"


def test_build_review_rows_filters_to_otc_official_name_warns():
    rows = build_review_rows(
        [
            entry_row("AECX", "CurrentC Power Corporation", "US92855W2017"),
            {**entry_row("MSFT", "Microsoft Corporation", "US5949181045"), "exchange": "NASDAQ", "listing_key": "NASDAQ::MSFT"},
            {**entry_row("PASS", "Pass Corp", ""), "quality_status": "pass"},
        ],
        [
            official_ref("AECX", "ACADIA ENERGY CORP"),
            official_ref("PASS", "PASS CORP"),
        ],
    )

    assert len(rows) == 1
    assert rows[0].listing_key == "OTC::AECX"
    assert rows[0].official_name == "ACADIA ENERGY CORP"
    assert rows[0].review_class == "probable_otc_rename_or_symbol_reuse"
    assert rows[0].apply_eligibility == "blocked_until_isin_anchored_issuer_history_review"
    assert (
        rows[0].verification_evidence_required
        == "official_or_reviewed_isin_bearing_source_matching_current_issuer_listing_key_and_name"
    )
    assert rows[0].review_strategy == "verify_isin_anchored_issuer_history_before_name_change"
    assert (
        rows[0].recommended_next_source
        == "Official or reviewed ISIN-bearing issuer-history source matching current issuer, listing key, and name."
    )
    assert rows[0].source_gate == "Do not change the name until ISIN-anchored evidence proves the same current issuer."
    assert rows[0].official_source_context == official_source_context_for(
        official_sources=rows[0].official_sources,
        official_name=rows[0].official_name,
    )
    assert rows[0].identity_review_context == identity_review_context_for(
        token_overlap=rows[0].token_overlap,
        current_token_count=rows[0].current_token_count,
        official_token_count=rows[0].official_token_count,
        isin=rows[0].isin,
        country=rows[0].country,
    )
    assert rows[0].decision_review_context == decision_review_context_for(
        review_class=rows[0].review_class,
        apply_eligibility=rows[0].apply_eligibility,
        verification_evidence_required=rows[0].verification_evidence_required,
        review_decision=rows[0].review_decision,
    )


def test_build_review_rows_skips_reviewed_keep_current_entries():
    rows = build_review_rows(
        [entry_row("HKRHF", "3DG Holdings (International) Limited", "BMG4587L1090")],
        [official_ref("HKRHF", "HONG KONG RESOURCES HOLDINGS CO LTD")],
        otc_review_decision_rows=[
            {
                "ticker": "HKRHF",
                "exchange": "OTC",
                "decision": "keep_current_reviewed",
                "confidence": "high",
                "reason": "Reviewed stale OTC official name.",
            }
        ],
    )

    assert rows == []


def test_build_review_rows_reclassifies_held_unresolved_entries():
    rows = build_review_rows(
        [entry_row("POELF", "The Navigator Company S.A", "PLNFI0500012")],
        [official_ref("POELF", "PORTUCEL EMPRS ORD")],
        otc_review_decision_rows=[
            {
                "ticker": "POELF",
                "exchange": "OTC",
                "decision": "hold_unresolved",
                "confidence": "medium",
                "reason": "Needs stronger issuer-history source.",
            }
        ],
    )

    assert len(rows) == 1
    assert rows[0].review_class == "hold_unresolved"
    assert rows[0].review_priority == "held"
    assert rows[0].review_decision == "hold_unresolved"
    assert rows[0].decision_reason == "Needs stronger issuer-history source."
    assert rows[0].recommended_action == "source_needed_for_resolution"
    assert rows[0].apply_eligibility == "keep_current_until_stronger_issuer_history_source"
    assert (
        rows[0].verification_evidence_required
        == "stronger_official_or_reviewed_issuer_history_source_before_any_name_change"
    )
    assert rows[0].review_strategy == "keep_current_until_stronger_issuer_history_source"
    assert (
        rows[0].recommended_next_source
        == "Stronger official or reviewed issuer-history source matching the OTC listing key."
    )
    assert rows[0].source_gate == "Keep current name until stronger issuer-history evidence resolves the ambiguity."
    assert rows[0].decision_review_context == decision_review_context_for(
        review_class="hold_unresolved",
        apply_eligibility="keep_current_until_stronger_issuer_history_source",
        verification_evidence_required="stronger_official_or_reviewed_issuer_history_source_before_any_name_change",
        review_decision="hold_unresolved",
    )


def test_otc_review_writes_csv_and_markdown(tmp_path):
    rows = build_review_rows(
        [entry_row("AECX", "CurrentC Power Corporation", "US92855W2017")],
        [official_ref("AECX", "ACADIA ENERGY CORP")],
    )
    csv_path = tmp_path / "otc.csv"
    md_path = tmp_path / "otc.md"

    write_csv(csv_path, rows)
    payload = summarize(rows, "2026-04-17T00:00:00Z", csv_path)
    write_markdown(md_path, payload)

    csv_rows = list(csv.DictReader(csv_path.open()))
    assert csv_rows[0]["review_class"] == "probable_otc_rename_or_symbol_reuse"
    assert csv_rows[0]["apply_eligibility"] == "blocked_until_isin_anchored_issuer_history_review"
    assert (
        csv_rows[0]["verification_evidence_required"]
        == "official_or_reviewed_isin_bearing_source_matching_current_issuer_listing_key_and_name"
    )
    assert csv_rows[0]["review_strategy"] == "verify_isin_anchored_issuer_history_before_name_change"
    assert (
        csv_rows[0]["recommended_next_source"]
        == "Official or reviewed ISIN-bearing issuer-history source matching current issuer, listing key, and name."
    )
    assert csv_rows[0]["source_gate"] == "Do not change the name until ISIN-anchored evidence proves the same current issuer."
    assert csv_rows[0]["official_source_context"] == "official_sources=otc_markets_stock_screener;official_name_present=true"
    assert csv_rows[0]["decision_review_context"] == (
        "review_class=probable_otc_rename_or_symbol_reuse;"
        "apply_eligibility=blocked_until_isin_anchored_issuer_history_review;"
        "verification_evidence_required=official_or_reviewed_isin_bearing_source_matching_current_issuer_listing_key_and_name;"
        "review_decision=none"
    )
    assert payload["summary"]["review_strategy_counts"] == {
        "verify_isin_anchored_issuer_history_before_name_change": 1
    }
    assert payload["summary"]["top_otc_name_mismatch_review_batches"] == [
        {
            "review_priority": "high",
            "review_class": "probable_otc_rename_or_symbol_reuse",
            "isin_presence": "with_isin",
            "official_sources": "otc_markets_stock_screener",
            "rows": 1,
            "review_strategy": "verify_isin_anchored_issuer_history_before_name_change",
            "verification_evidence_required": "official_or_reviewed_isin_bearing_source_matching_current_issuer_listing_key_and_name",
            "recommended_next_source": (
                "Official or reviewed ISIN-bearing issuer-history source matching current issuer, listing key, and name."
            ),
            "source_gate": "Do not change the name until ISIN-anchored evidence proves the same current issuer.",
        }
    ]
    assert "OTC Name Mismatch Review" in md_path.read_text()
    assert "Apply Eligibility" in md_path.read_text()
    assert "Recommended next source" in md_path.read_text()


def test_review_strategy_for_keeps_otc_name_no_apply_gates_explicit():
    assert (
        review_strategy_for("stale_or_symbol_reuse_without_isin")
        == "resolve_or_quarantine_with_official_otc_profile_or_issuer_history"
    )
    assert (
        review_strategy_for("weak_abbreviation_or_truncation_review")
        == "review_official_alias_or_abbreviation_before_matcher_tuning"
    )
    assert (
        recommended_next_source_for("weak_abbreviation_or_truncation_review")
        == "Official alias, abbreviation, issuer, OTC profile, or registry evidence matching the exact listing identity."
    )
    assert (
        source_gate_for("stale_or_symbol_reuse_without_isin")
        == "Do not rename or trust reused OTC symbol without listing-keyed official identity or quarantine evidence."
    )

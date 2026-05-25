from __future__ import annotations

import csv
from argparse import Namespace

from scripts.fetch_symbol_changes import (
    build_reviews,
    effective_age_days_for,
    parse_stockanalysis_changes,
    recency_bucket_for,
    review_bucket_for,
    review_priority_for,
    scope_match_context_for,
    apply_eligibility_for,
    apply_readiness_for,
    source_review_context_for,
    symbol_change_workflow_queue_for,
    verification_evidence_for,
    workflow_review_context_for,
    run,
)


FIXTURE_HTML = """
<html>
  <body>
    <table>
      <thead><tr><th>Date</th><th>Old</th><th>New</th><th>New Company Name</th></tr></thead>
      <tbody>
        <tr>
          <td>Apr 8, 2026</td>
          <td>CAPT</td>
          <td><a href="/quote/otc/CPTAF/">CPTAF</a></td>
          <td>Captivision Inc</td>
        </tr>
        <tr>
          <td>Apr 7, 2026</td>
          <td>DCOMG</td>
          <td><a href="/stocks/dcbg/">DCBG</a></td>
          <td>Dime Community Bancshares Inc</td>
        </tr>
      </tbody>
    </table>
  </body>
</html>
"""


def test_parse_stockanalysis_changes_extracts_table_rows():
    changes = parse_stockanalysis_changes(
        FIXTURE_HTML,
        source_url="https://stockanalysis.com/actions/changes/",
        observed_at="2026-04-13T00:00:00Z",
    )

    assert [change.effective_date for change in changes] == ["2026-04-08", "2026-04-07"]
    assert changes[0].old_symbol == "CAPT"
    assert changes[0].new_symbol == "CPTAF"
    assert changes[0].source_exchange_hint == "OTC"
    assert changes[1].source_exchange_hint == "US_LISTED"
    assert changes[0].source_confidence == "secondary_review"
    assert changes[0].review_needed == "true"


def test_build_reviews_classifies_current_listing_state():
    changes = parse_stockanalysis_changes(
        FIXTURE_HTML,
        source_url="https://stockanalysis.com/actions/changes/",
        observed_at="2026-04-13T00:00:00Z",
    )
    listings = [
        {"listing_key": "OTC::CAPT", "ticker": "CAPT", "exchange": "OTC", "name": "Captivision Inc"},
        {"listing_key": "NASDAQ::DCBG", "ticker": "DCBG", "exchange": "NASDAQ", "name": "Dime Community Bancshares Inc"},
    ]

    reviews = build_reviews(changes, listings)

    assert reviews[0].match_status == "old_symbol_present_new_symbol_missing"
    assert reviews[0].symbol_change_workflow_queue == "review_verified_rename_or_delisting"
    assert reviews[0].review_bucket == "action_required_possible_rename_or_delisting"
    assert reviews[0].review_priority == "P1"
    assert reviews[0].review_strategy == "verify_rename_or_delisting_with_official_venue_or_issuer_evidence"
    assert reviews[0].effective_age_days == 5
    assert reviews[0].recency_bucket == "recent_7d"
    assert reviews[0].apply_eligibility == "requires_official_venue_confirmation"
    assert apply_readiness_for(reviews[0]) == "blocked_until_listing_keyed_official_symbol_change_evidence"
    assert reviews[0].verification_evidence_required == "official_exchange_notice_or_current_directory_showing_old_symbol_inactive_new_symbol_active_same_issuer"
    assert reviews[0].recommended_next_source == (
        "Official exchange notice, issuer notice, or current exchange directory proving old/new symbols for the same issuer."
    )
    assert reviews[0].source_gate == (
        "Do not rename until official listing-keyed evidence proves old inactive and new active for the same issuer."
    )
    assert reviews[0].recommended_action == "review_possible_rename_or_delisting_in_source_scope"
    assert reviews[0].old_listing_keys == "OTC::CAPT"
    assert reviews[0].old_scoped_listing_keys == "OTC::CAPT"
    assert reviews[0].listing_key_review_status == "old_scoped_listing_key_only"
    assert reviews[0].scoped_listing_names == "Captivision Inc"
    assert reviews[0].issuer_name_review_status == "feed_name_exactly_matches_scoped_listing_name"
    assert reviews[0].source_review_context == source_review_context_for(reviews[0].change)
    assert reviews[0].scope_match_context == scope_match_context_for(
        exchange_scope_status=reviews[0].exchange_scope_status,
        old_matches=[listings[0]],
        new_matches=[],
        old_scoped_matches=[listings[0]],
        new_scoped_matches=[],
    )
    assert reviews[0].workflow_review_context == workflow_review_context_for(
        workflow_queue=reviews[0].symbol_change_workflow_queue,
        review_bucket=reviews[0].review_bucket,
        apply_eligibility=reviews[0].apply_eligibility,
        verification_evidence_required=reviews[0].verification_evidence_required,
    )
    assert reviews[1].match_status == "new_symbol_present_old_symbol_missing"
    assert reviews[1].symbol_change_workflow_queue == "audit_already_reflected"
    assert reviews[1].review_bucket == "already_reflected_in_source_scope"
    assert reviews[1].review_priority == "P4"
    assert reviews[1].review_strategy == "audit_already_reflected_no_canonical_change"
    assert reviews[1].apply_eligibility == "audit_only_no_apply"
    assert apply_readiness_for(reviews[1]) == "audit_only_no_canonical_change"
    assert reviews[1].verification_evidence_required == "audit_only_confirm_no_canonical_change_needed"
    assert reviews[1].recommended_next_source == "Audit-only confirmation from scoped listing records; no canonical change."
    assert reviews[1].source_gate == "Audit only; no ticker, listing, or name change is authorized."
    assert reviews[1].recommended_action == "already_reflected_or_new_symbol_added_in_source_scope"
    assert reviews[1].new_listing_keys == "NASDAQ::DCBG"
    assert reviews[1].new_scoped_listing_keys == "NASDAQ::DCBG"
    assert reviews[1].listing_key_review_status == "new_scoped_listing_key_only"
    assert reviews[1].scoped_listing_names == "Dime Community Bancshares Inc"
    assert reviews[1].issuer_name_review_status == "feed_name_exactly_matches_scoped_listing_name"
    assert reviews[1].source_review_context == source_review_context_for(reviews[1].change)
    assert reviews[1].workflow_review_context == workflow_review_context_for(
        workflow_queue=reviews[1].symbol_change_workflow_queue,
        review_bucket=reviews[1].review_bucket,
        apply_eligibility=reviews[1].apply_eligibility,
        verification_evidence_required=reviews[1].verification_evidence_required,
    )


def test_build_reviews_does_not_treat_out_of_scope_symbol_as_already_reflected():
    changes = parse_stockanalysis_changes(
        FIXTURE_HTML,
        source_url="https://stockanalysis.com/actions/changes/",
        observed_at="2026-04-13T00:00:00Z",
    )
    listings = [
        {"listing_key": "NASDAQ::CAPT", "ticker": "CAPT", "exchange": "NASDAQ", "name": "Captivision Inc"},
        {"listing_key": "ASX::DCBG", "ticker": "DCBG", "exchange": "ASX", "name": "Dime Community Bancshares Inc"},
    ]

    reviews = build_reviews(changes, listings)

    assert reviews[1].change.source_exchange_hint == "US_LISTED"
    assert reviews[1].match_status == "symbol_present_only_outside_source_scope"
    assert reviews[1].symbol_change_workflow_queue == "blocked_out_of_scope_symbol_collision"
    assert reviews[1].review_bucket == "hold_out_of_scope_symbol_collision"
    assert reviews[1].review_priority == "P2"
    assert reviews[1].review_strategy == "block_until_source_scope_and_non_symbol_identity_resolved"
    assert reviews[1].apply_eligibility == "blocked_until_exchange_scope_resolved"
    assert reviews[1].verification_evidence_required == "official_exchange_scope_and_non_symbol_identity_evidence_before_apply"
    assert reviews[1].recommended_next_source == (
        "Official source exchange scope mapping plus non-symbol identity evidence before any symbol action."
    )
    assert reviews[1].source_gate == "Block apply; global symbol collision outside source scope is not symbol-change evidence."
    assert reviews[1].exchange_scope_status == "global_symbol_collision_outside_source_scope"
    assert reviews[1].recommended_action == "do_not_apply_from_symbol_match_review_exchange_scope_first"
    assert reviews[1].new_listing_keys == "ASX::DCBG"
    assert reviews[1].new_scoped_listing_keys == ""
    assert reviews[1].listing_key_review_status == "no_scoped_listing_key_match"
    assert reviews[1].new_match_count == 1
    assert reviews[1].new_scoped_match_count == 0
    assert reviews[1].issuer_name_review_status == "no_scoped_listing_name_available"
    assert reviews[1].source_review_context == source_review_context_for(reviews[1].change)
    assert reviews[1].scope_match_context == scope_match_context_for(
        exchange_scope_status=reviews[1].exchange_scope_status,
        old_matches=[],
        new_matches=[listings[1]],
        old_scoped_matches=[],
        new_scoped_matches=[],
    )


def test_review_bucket_and_priority_map_review_order() -> None:
    assert review_bucket_for(
        "old_and_new_symbols_present",
        "matches_within_source_scope",
    ) == "action_required_duplicate_or_cross_listing"
    assert review_bucket_for(
        "new_symbol_present_old_symbol_missing",
        "global_symbol_collision_outside_source_scope",
    ) == "already_reflected_in_scope_with_global_symbol_collision"
    assert review_bucket_for(
        "no_matching_listing",
        "matches_within_source_scope",
    ) == "no_dataset_match_for_source_scope"
    assert review_bucket_for(
        "new_symbol_present_old_symbol_missing",
        "unscoped_source_hint",
    ) == "manual_scope_mapping_required"
    assert review_priority_for("action_required_duplicate_or_cross_listing") == "P1"
    assert review_priority_for("no_dataset_match_for_source_scope") == "P3"
    assert review_priority_for("already_reflected_in_source_scope") == "P4"
    assert apply_eligibility_for("action_required_duplicate_or_cross_listing") == "requires_official_venue_confirmation"
    assert apply_eligibility_for("manual_scope_mapping_required") == "blocked_until_exchange_scope_resolved"
    assert apply_eligibility_for("no_dataset_match_for_source_scope") == "no_dataset_action_without_scope_mapping"
    assert apply_eligibility_for("already_reflected_in_source_scope") == "audit_only_no_apply"
    assert verification_evidence_for("manual_scope_mapping_required") == "source_exchange_mapping_before_any_symbol_change_review"
    assert symbol_change_workflow_queue_for("action_required_possible_rename_or_delisting") == "review_verified_rename_or_delisting"
    assert symbol_change_workflow_queue_for("action_required_duplicate_or_cross_listing") == "review_duplicate_or_cross_listing"
    assert symbol_change_workflow_queue_for("already_reflected_in_source_scope") == "audit_already_reflected"
    assert symbol_change_workflow_queue_for("no_dataset_match_for_source_scope") == "document_no_dataset_match"
    assert symbol_change_workflow_queue_for("manual_scope_mapping_required") == "blocked_missing_source_scope_mapping"


def test_recency_bucket_maps_effective_age_for_review_order() -> None:
    assert effective_age_days_for("2026-04-08", "2026-04-13T00:00:00Z") == 5
    assert recency_bucket_for(5) == "recent_7d"
    assert recency_bucket_for(30) == "recent_30d"
    assert recency_bucket_for(90) == "recent_90d"
    assert recency_bucket_for(91) == "older_than_90d"
    assert recency_bucket_for(-1) == "unknown_or_future_effective_date"


def test_run_writes_merged_changes_and_review_outputs(tmp_path):
    html_path = tmp_path / "changes.html"
    listings_csv = tmp_path / "listings.csv"
    html_path.write_text(FIXTURE_HTML, encoding="utf-8")
    with listings_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["listing_key", "ticker", "exchange", "name"])
        writer.writeheader()
        writer.writerow({"listing_key": "OTC::CAPT", "ticker": "CAPT", "exchange": "OTC", "name": "Captivision Inc"})

    args = Namespace(
        url="https://stockanalysis.com/actions/changes/",
        html_in=html_path,
        listings_csv=listings_csv,
        changes_csv=tmp_path / "symbol_changes.csv",
        changes_json=tmp_path / "symbol_changes.json",
        review_csv=tmp_path / "symbol_changes_review.csv",
        review_json=tmp_path / "symbol_changes_review.json",
        review_md=tmp_path / "symbol_changes_review.md",
        timeout_seconds=30.0,
        observed_at="2026-04-13T00:00:00Z",
    )

    payload = run(args)

    assert payload["_meta"]["source_files"] == {
        "listings_csv": str(listings_csv),
        "changes_csv": str(args.changes_csv),
    }
    assert payload["summary"]["fetched_rows"] == 2
    assert payload["summary"]["merged_history_rows"] == 2
    assert payload["summary"]["exchange_scope_status_counts"] == {"matches_within_source_scope": 2}
    assert payload["summary"]["symbol_change_workflow_queue_counts"] == {
        "document_no_dataset_match": 1,
        "review_verified_rename_or_delisting": 1,
    }
    assert payload["summary"]["symbol_change_backlog"] == {
        "status": "listing_keyed_symbol_change_review_queue_open",
        "rows": 2,
        "verified_rename_or_delisting_review_rows": 1,
        "duplicate_or_cross_listing_review_rows": 0,
        "already_reflected_audit_rows": 0,
        "out_of_scope_collision_blocked_rows": 0,
        "missing_source_scope_mapping_rows": 0,
        "no_dataset_match_documentation_rows": 1,
        "time_sensitive_review_rows": 1,
        "direct_symbol_change_apply_allowed_rows": 0,
        "secondary_feed_apply_authorized": False,
        "source_gate": (
            "Symbol-change feed rows are review signals only; ticker, name, listing, or alias changes require "
            "listing-keyed official venue or issuer evidence for old/new symbols and issuer identity."
        ),
    }
    assert payload["summary"]["review_priority_counts"] == {"P1": 1, "P3": 1}
    assert payload["summary"]["recency_bucket_counts"] == {"recent_7d": 2}
    assert payload["summary"]["review_priority_recency_counts"] == {"P1:recent_7d": 1, "P3:recent_7d": 1}
    assert payload["summary"]["workflow_queue_recency_counts"] == {
        "document_no_dataset_match:recent_7d": 1,
        "review_verified_rename_or_delisting:recent_7d": 1,
    }
    assert payload["summary"]["workflow_queue_priority_counts"] == {
        "document_no_dataset_match:P3": 1,
        "review_verified_rename_or_delisting:P1": 1,
    }
    assert payload["summary"]["workflow_queue_scope_counts"] == {
        "document_no_dataset_match:matches_within_source_scope": 1,
        "review_verified_rename_or_delisting:matches_within_source_scope": 1,
    }
    assert payload["summary"]["workflow_queue_match_status_counts"] == {
        "document_no_dataset_match:no_matching_listing": 1,
        "review_verified_rename_or_delisting:old_symbol_present_new_symbol_missing": 1,
    }
    assert payload["summary"]["workflow_queue_source_hint_counts"] == {
        "document_no_dataset_match": {"US_LISTED": 1},
        "review_verified_rename_or_delisting": {"OTC": 1},
    }
    assert payload["summary"]["workflow_queue_source_confidence_counts"] == {
        "document_no_dataset_match": {"secondary_review": 1},
        "review_verified_rename_or_delisting": {"secondary_review": 1},
    }
    assert payload["summary"]["workflow_queue_issuer_name_review_counts"] == {
        "document_no_dataset_match": {"no_scoped_listing_name_available": 1},
        "review_verified_rename_or_delisting": {"feed_name_exactly_matches_scoped_listing_name": 1},
    }
    assert payload["summary"]["workflow_queue_listing_key_review_counts"] == {
        "document_no_dataset_match": {"no_scoped_listing_key_match": 1},
        "review_verified_rename_or_delisting": {"old_scoped_listing_key_only": 1},
    }
    assert payload["summary"]["workflow_queue_review_strategy_counts"] == {
        "document_no_dataset_match": {
            "document_no_dataset_match_without_canonical_action": 1,
        },
        "review_verified_rename_or_delisting": {
            "verify_rename_or_delisting_with_official_venue_or_issuer_evidence": 1,
        },
    }
    assert payload["summary"]["top_symbol_change_workflow_batches"] == [
        {
            "symbol_change_workflow_queue": "review_verified_rename_or_delisting",
            "review_priority": "P1",
            "recency_bucket": "recent_7d",
            "exchange_scope_status": "matches_within_source_scope",
            "rows": 1,
            "review_strategy": "verify_rename_or_delisting_with_official_venue_or_issuer_evidence",
            "evidence_required": "official_exchange_notice_or_current_directory_showing_old_symbol_inactive_new_symbol_active_same_issuer",
            "recommended_next_source": (
                "Official exchange notice, issuer notice, or current exchange directory proving old/new symbols "
                "for the same issuer."
            ),
            "source_gate": (
                "Do not rename until official listing-keyed evidence proves old inactive and new active for the same issuer."
            ),
        },
        {
            "symbol_change_workflow_queue": "document_no_dataset_match",
            "review_priority": "P3",
            "recency_bucket": "recent_7d",
            "exchange_scope_status": "matches_within_source_scope",
            "rows": 1,
            "review_strategy": "document_no_dataset_match_without_canonical_action",
            "evidence_required": "official_exchange_scope_mapping_or_ignore_as_external_non_dataset_event",
            "recommended_next_source": "Official exchange scope mapping, or document the event as outside the dataset.",
            "source_gate": "No dataset action without scoped official mapping to an existing or intended listing.",
        },
    ]
    assert payload["summary"]["apply_eligibility_counts"] == {
        "no_dataset_action_without_scope_mapping": 1,
        "requires_official_venue_confirmation": 1,
    }
    assert payload["summary"]["symbol_change_apply_readiness_counts"] == {
        "blocked_until_listing_keyed_official_symbol_change_evidence": 1,
        "document_or_ignore_until_scoped_official_dataset_match": 1,
    }
    assert payload["summary"]["verification_evidence_required_counts"] == {
        "official_exchange_notice_or_current_directory_showing_old_symbol_inactive_new_symbol_active_same_issuer": 1,
        "official_exchange_scope_mapping_or_ignore_as_external_non_dataset_event": 1,
    }
    assert payload["summary"]["time_sensitive_review_rows"] == 1
    assert payload["summary"]["time_sensitive_workflow_queue_counts"] == {
        "review_verified_rename_or_delisting": 1,
    }
    assert payload["summary"]["time_sensitive_recency_counts"] == {"recent_7d": 1}
    assert payload["summary"]["time_sensitive_apply_readiness_counts"] == {
        "blocked_until_listing_keyed_official_symbol_change_evidence": 1,
    }
    assert payload["summary"]["top_time_sensitive_symbol_change_batches"] == [
        {
            "symbol_change_workflow_queue": "review_verified_rename_or_delisting",
            "recency_bucket": "recent_7d",
            "exchange_scope_status": "matches_within_source_scope",
            "match_status": "old_symbol_present_new_symbol_missing",
            "listing_key_review_status": "old_scoped_listing_key_only",
            "rows": 1,
            "review_strategy": "verify_rename_or_delisting_with_official_venue_or_issuer_evidence",
            "evidence_required": "official_exchange_notice_or_current_directory_showing_old_symbol_inactive_new_symbol_active_same_issuer",
            "recommended_next_source": (
                "Official exchange notice, issuer notice, or current exchange directory proving old/new symbols "
                "for the same issuer."
            ),
            "source_gate": (
                "Do not rename until official listing-keyed evidence proves old inactive and new active for the same issuer."
            ),
        }
    ]
    assert payload["summary"]["review_bucket_counts"] == {
        "action_required_possible_rename_or_delisting": 1,
        "no_dataset_match_for_source_scope": 1,
    }
    review_rows = list(csv.DictReader(args.review_csv.open()))
    assert len(review_rows) == 2
    assert review_rows[0]["source_confidence"] == "secondary_review"
    assert review_rows[0]["symbol_change_workflow_queue"] == "review_verified_rename_or_delisting"
    assert review_rows[0]["review_priority"] == "P1"
    assert review_rows[0]["review_strategy"] == "verify_rename_or_delisting_with_official_venue_or_issuer_evidence"
    assert review_rows[0]["effective_age_days"] == "5"
    assert review_rows[0]["recency_bucket"] == "recent_7d"
    assert review_rows[0]["apply_eligibility"] == "requires_official_venue_confirmation"
    assert review_rows[0]["verification_evidence_required"] == "official_exchange_notice_or_current_directory_showing_old_symbol_inactive_new_symbol_active_same_issuer"
    assert review_rows[0]["recommended_next_source"] == (
        "Official exchange notice, issuer notice, or current exchange directory proving old/new symbols for the same issuer."
    )
    assert review_rows[0]["source_gate"] == (
        "Do not rename until official listing-keyed evidence proves old inactive and new active for the same issuer."
    )
    assert review_rows[0]["old_scoped_listing_keys"] == "OTC::CAPT"
    assert review_rows[0]["listing_key_review_status"] == "old_scoped_listing_key_only"
    assert review_rows[0]["scoped_listing_names"] == "Captivision Inc"
    assert review_rows[0]["issuer_name_review_status"] == "feed_name_exactly_matches_scoped_listing_name"
    assert "secondary-source symbol-change feed" in args.review_md.read_text()

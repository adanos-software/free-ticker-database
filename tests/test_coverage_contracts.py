from __future__ import annotations

from datetime import datetime, timezone

from scripts.build_coverage_contracts import (
    build_contract_rows,
    freshness_as_of,
    source_fresh,
    source_license_approved,
)


def source(*, licensed: bool = True, sla: int = 7) -> dict[str, object]:
    if not licensed:
        return {"source_url": "https://example.test", "license_status": "review_required", "freshness_sla_days": sla}
    return {
        "source_url": "https://example.test", "license_status": "verified_open",
        "derived_facts_redistribution_status": "allowed", "commercial_use_status": "allowed",
        "license_name": "CC0", "license_url": "https://example.test/license",
        "terms_version": "2026-01-01", "terms_sha256": "a" * 64,
        "license_reviewed_at": "2026-08-01T00:00:00Z", "attribution_required": "none",
        "freshness_sla_days": sla,
    }


def reference(ticker: str = "A") -> dict[str, str]:
    return {"source_key": "s", "exchange": "X", "ticker": ticker, "asset_type": "Stock", "official": "true", "listing_status": "active", "reference_scope": "exchange_directory"}


def rec(ticker: str = "A", classification: str = "exact_match", credit: str = "true") -> dict[str, str]:
    return {"source_key": "s", "reference_key": f"X::{ticker}", "exchange": "X", "asset_types": "Stock", "classification": classification, "coverage_credit": credit}


def audit(denominator: int = 1) -> dict[str, str]:
    return {"venue_status": "official_full", "official_active_stock_rows": str(denominator), "official_active_etf_rows": "0"}


def build(*, references=None, recs=None, licensed=True, detail=None, denominator=1):
    return build_contract_rows(
        references=references or [reference()], reconciliations=recs or [rec()],
        sources={"s": source(licensed=licensed)},
        source_details={"s": detail or {"mode": "network", "generated_at": "2026-08-16T00:00:00Z"}},
        exchange_audit={"X": audit(denominator)}, as_of=datetime(2026, 8, 17, tzinfo=timezone.utc),
    )[0]


def test_full_contract_passes_only_when_all_gates_pass() -> None:
    assert build()["contract_status"] == "pass"


def test_identity_conflict_blocks_full_contract() -> None:
    assert build(recs=[rec(classification="exact_identity_conflict", credit="false")])["contract_status"] == "fail_identity_conflict"


def test_cross_venue_or_normalization_candidate_gets_no_credit() -> None:
    row = build(recs=[rec(classification="normalization_candidate", credit="false")])
    assert row["covered_reference_keys"] == 0
    assert row["contract_status"] == "fail_recall"


def test_license_review_required_blocks_stable_contract() -> None:
    assert build(licensed=False)["contract_status"] == "fail_license"


def test_stale_or_unavailable_snapshot_blocks_contract() -> None:
    row = build(detail={"mode": "unavailable", "generated_at": "2026-08-16T00:00:00Z"})
    assert row["contract_status"] == "fail_freshness"


def test_denominator_mismatch_is_not_silently_clamped() -> None:
    row = build(references=[reference("A"), reference("B"), reference("C")], recs=[rec("A"), rec("B"), rec("C")], denominator=1)
    assert row["contract_status"] == "fail_denominator_inconsistent"


def test_license_requires_terms_hash_and_review_time() -> None:
    bad = source(); bad["terms_sha256"] = ""
    assert source_license_approved(bad)[0] is False


def test_verified_restricted_does_not_approve_derived_facts_redistribution() -> None:
    restricted = source()
    restricted["license_status"] = "verified_restricted"
    restricted["derived_facts_redistribution_status"] = "restricted"
    restricted["commercial_use_status"] = "restricted"
    restricted["attribution_required"] = "required"
    ok, reason = source_license_approved(restricted)
    assert ok is False
    assert "license_status is not verified_open" in reason


def test_future_snapshot_timestamp_is_rejected() -> None:
    ok, reason = source_fresh(
        source(),
        {"mode": "network", "generated_at": "2026-08-18T00:00:00Z"},
        as_of=datetime(2026, 8, 17, tzinfo=timezone.utc),
    )
    assert not ok
    assert "future" in reason


def test_post_build_refresh_ages_against_latest_refresh_envelope() -> None:
    dataset_as_of = datetime(2026, 8, 26, 13, 34, 26, tzinfo=timezone.utc)
    last_refresh = {
        "generated_at": "2026-08-28T00:00:00Z",
        "selected_source_keys": ["other_source"],
    }
    detail = {"mode": "network", "generated_at": "2026-08-27T05:35:27Z"}
    source_as_of = freshness_as_of(
        dataset_as_of=dataset_as_of,
        source_detail=detail,
        last_refresh=last_refresh,
    )
    ok, reason = source_fresh(source(licensed=False, sla=7), detail, as_of=source_as_of)
    assert ok
    assert "within" in reason
    assert source_as_of == datetime(2026, 8, 28, tzinfo=timezone.utc)
    older_as_of = freshness_as_of(
        dataset_as_of=dataset_as_of,
        source_detail={"mode": "network", "generated_at": "2026-08-19T08:39:18Z"},
        last_refresh=last_refresh,
    )
    assert older_as_of == dataset_as_of


def test_future_snapshot_without_matching_last_refresh_is_still_rejected() -> None:
    row = build(
        licensed=False,
        detail={"mode": "network", "generated_at": "2026-08-18T00:00:00Z"},
    )
    assert row["contract_status"] == "fail_freshness"
    assert "future" in row["freshness_failures"]


def test_matching_last_refresh_lets_live_snapshot_pass_freshness() -> None:
    row = build_contract_rows(
        references=[reference()],
        reconciliations=[rec()],
        sources={"s": source(licensed=False)},
        source_details={"s": {"mode": "network", "generated_at": "2026-08-18T00:00:00Z"}},
        exchange_audit={"X": audit()},
        as_of=datetime(2026, 8, 17, tzinfo=timezone.utc),
        last_refresh={
            "generated_at": "2026-08-18T00:00:00Z",
            "selected_source_keys": ["other"],
        },
    )[0]
    assert row["freshness_status"] == "pass"
    assert row["contract_status"] == "fail_license"


def test_post_build_snapshot_ages_against_later_unrelated_refresh() -> None:
    row = build_contract_rows(
        references=[reference()],
        reconciliations=[rec()],
        sources={"s": source(licensed=False, sla=7)},
        source_details={"s": {"mode": "network", "generated_at": "2026-08-18T00:00:00Z"}},
        exchange_audit={"X": audit()},
        as_of=datetime(2026, 8, 17, tzinfo=timezone.utc),
        last_refresh={
            "generated_at": "2026-08-26T00:00:00Z",
            "selected_source_keys": ["other"],
        },
    )[0]
    assert row["contract_status"] == "fail_freshness"
    assert "exceeds" in row["freshness_failures"]


def test_snapshot_after_last_refresh_envelope_is_rejected() -> None:
    row = build_contract_rows(
        references=[reference()],
        reconciliations=[rec()],
        sources={"s": source(licensed=False)},
        source_details={"s": {"mode": "network", "generated_at": "2026-08-18T00:00:00Z"}},
        exchange_audit={"X": audit()},
        as_of=datetime(2026, 8, 17, tzinfo=timezone.utc),
        last_refresh={
            "generated_at": "2026-08-17T12:00:00Z",
            "selected_source_keys": ["s"],
        },
    )[0]
    assert row["contract_status"] == "fail_freshness"
    assert "future" in row["freshness_failures"]


def test_alternate_line_without_reviewed_mapping_gets_no_credit() -> None:
    row = build(recs=[rec(classification="alternate_listing_line", credit="false")])
    assert row["covered_reference_keys"] == 0
    assert row["contract_status"] == "fail_recall"


def test_future_license_review_timestamp_is_rejected() -> None:
    candidate = source()
    candidate["license_reviewed_at"] = "2026-08-18T00:00:00Z"
    ok, reason = source_license_approved(
        candidate, as_of=datetime(2026, 8, 17, tzinfo=timezone.utc)
    )
    assert not ok
    assert "future" in reason

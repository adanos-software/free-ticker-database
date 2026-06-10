from scripts.build_twelvedata_manual_apply_queue import build_queues, provider_support, rejection_reason


def validation_row(**overrides: str) -> dict[str, str]:
    row = {
        "listing_key": "OTC::AAA",
        "ticker": "AAA",
        "exchange": "OTC",
        "local_name": "Old AAA Inc",
        "twelvedata_name": "New AAA Inc",
        "twelvedata_type": "Common Stock",
        "name_score": "0.2",
        "deepseek_decision_candidate": "needs_official_evidence",
        "deepseek_safe_action": "needs_official_evidence",
        "openfigi_name": "New AAA Inc",
        "openfigi_figi": "BBGTEST",
        "openfigi_match": "supports_twelvedata",
        "alphavantage_name": "",
        "alphavantage_match": "no_name",
        "fmp_name": "New AAA Incorporated",
        "fmp_match": "supports_twelvedata",
        "validation_status": "second_source_supports_twelvedata_name",
        "recommended_next_action": "build_manual_apply_candidate_for_name_update_after_official_or_identifier_gate",
    }
    row.update(overrides)
    return row


def test_provider_support_collects_only_twelvedata_supporting_providers() -> None:
    providers, names = provider_support(validation_row(alphavantage_name="Old AAA Inc", alphavantage_match="supports_local"))

    assert providers == ["OpenFIGI", "FMP"]
    assert names == ["OpenFIGI:New AAA Inc", "FMP:New AAA Incorporated"]


def test_build_queues_keeps_only_twelvedata_supported_manual_candidates() -> None:
    apply_rows, rejected_rows = build_queues(
        [
            validation_row(),
            validation_row(
                listing_key="NASDAQ::BBB",
                validation_status="second_source_supports_local_name",
                openfigi_match="supports_local",
                fmp_match="no_name",
            ),
        ]
    )

    assert len(apply_rows) == 1
    assert apply_rows[0]["listing_key"] == "OTC::AAA"
    assert apply_rows[0]["proposed_name"] == "New AAA Inc"
    assert apply_rows[0]["supporting_providers"] == "OpenFIGI|FMP"
    assert apply_rows[0]["apply_status"] == "manual_review_required"
    assert "Do not apply automatically" in apply_rows[0]["apply_gate"]
    assert len(rejected_rows) == 1
    assert rejected_rows[0]["rejection_reason"] == "second_source_supports_current_local_name"


def test_rejection_reason_describes_conflicting_evidence() -> None:
    assert (
        rejection_reason(validation_row(validation_status="conflicting_second_source_evidence"))
        == "provider_evidence_conflicts_between_local_and_twelvedata"
    )

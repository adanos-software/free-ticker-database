from scripts.build_twelvedata_source_adjudication import (
    build_adjudication,
    classify_row,
    figi_relation,
    load_reviewed_name_overrides,
)


def rename_row(**overrides: str) -> dict[str, str]:
    row = {
        "listing_key": "OTC::AAA",
        "ticker": "AAA",
        "exchange": "OTC",
        "local_name": "Old AAA Inc",
        "twelvedata_name": "New AAA Inc",
        "twelvedata_type": "Common Stock",
        "name_score": "0.2",
        "review_batch": "batch_a_us_core",
    }
    row.update(overrides)
    return row


def validation_row(**overrides: str) -> dict[str, str]:
    row = {
        "listing_key": "OTC::AAA",
        "validation_status": "second_source_supports_twelvedata_name",
        "openfigi_status": "ok",
        "openfigi_match": "supports_twelvedata",
        "openfigi_figi": "BBGAAA",
        "alphavantage_match": "no_name",
        "fmp_match": "no_name",
        "evidence_summary": "OpenFIGI supports Twelve Data name.",
    }
    row.update(overrides)
    return row


def classify(**kwargs):
    row = kwargs.pop("row", rename_row())
    return classify_row(
        row,
        deepseek=kwargs.pop("deepseek", {}),
        validation=kwargs.pop("validation", {"OTC::AAA": validation_row()}),
        queues=kwargs.pop("queues", {}),
        identifiers=kwargs.pop("identifiers", {"OTC::AAA": {"listing_key": "OTC::AAA", "figi": "BBGAAA"}}),
        coverage=kwargs.pop("coverage", {"OTC": {"venue_status": "official_partial", "official_source_count": 1}}),
        name_overrides=kwargs.pop("name_overrides", {}),
        source_truth=kwargs.pop("source_truth", {}),
    )


def test_figi_relation_classifies_identifier_states() -> None:
    assert figi_relation("BBG1", "BBG1") == "local_figi_match"
    assert figi_relation("BBG1", "BBG2") == "figi_mismatch"
    assert figi_relation("", "BBG2") == "openfigi_only"
    assert figi_relation("BBG1", "") == "local_figi_only"


def test_identifier_supported_openfigi_match_is_apply_ready() -> None:
    row = classify()

    assert row["adjudication_decision"] == "apply_twelvedata_name_identifier_supported"
    assert row["apply_eligibility"] == "apply_ready"
    assert row["evidence_tier"] == "identifier"
    assert row["provider_supports_twelvedata"] == "OpenFIGI"


def test_figi_mismatch_blocks_apply_even_when_provider_supports_twelvedata() -> None:
    row = classify(identifiers={"OTC::AAA": {"listing_key": "OTC::AAA", "figi": "BBGLOCAL"}})

    assert row["adjudication_decision"] == "conflict_blocked_figi_mismatch"
    assert row["apply_eligibility"] == "blocked"
    assert row["figi_relation"] == "figi_mismatch"


def test_provider_local_support_keeps_local_name() -> None:
    row = classify(
        validation={
            "OTC::AAA": validation_row(
                validation_status="second_source_supports_local_name",
                openfigi_match="supports_local",
                openfigi_figi="BBGAAA",
            )
        }
    )

    assert row["adjudication_decision"] == "keep_local_name_provider_supported"
    assert row["recommended_operation"] == "keep_local_name"
    assert row["provider_supports_local"] == "OpenFIGI"


def test_pending_provider_queue_stays_blocked() -> None:
    row = classify(
        validation={},
        queues={
            "OTC::AAA": {
                "listing_key": "OTC::AAA",
                "validation_status": "pending_provider_env",
                "evidence_required": "provider validation required",
            }
        },
        identifiers={},
    )

    assert row["adjudication_decision"] == "provider_validation_pending"
    assert row["apply_eligibility"] == "blocked"
    assert row["source_gate"] == "provider validation required"


def test_out_of_scope_type_is_excluded_before_provider_evidence() -> None:
    row = classify(row=rename_row(twelvedata_type="ETF"))

    assert row["adjudication_decision"] == "type_out_of_scope"
    assert row["type_scope"] == "type_out_of_scope"
    assert row["apply_eligibility"] == "blocked"


def test_reviewed_local_name_override_wins_over_twelvedata() -> None:
    overrides = load_reviewed_name_overrides(
        [
            {
                "ticker": "AAA",
                "exchange": "OTC",
                "field": "name",
                "decision": "update",
                "proposed_value": "Old AAA Inc",
                "confidence": "0.96",
                "reason": "Reviewed official name.",
            }
        ]
    )

    row = classify(name_overrides=overrides)

    assert row["adjudication_decision"] == "keep_local_name_reviewed_override"
    assert row["reviewed_name_override"] == "supports_local"


def test_core_exclusion_source_of_truth_blocks_apply() -> None:
    row = classify(
        source_truth={
            "OTC::AAA": [
                {
                    "listing_key": "OTC::AAA",
                    "source_of_truth_outcome": "core_exclusion_candidate",
                    "source_gate": "scope review required",
                }
            ]
        }
    )

    assert row["adjudication_decision"] == "scope_review_blocked"
    assert row["apply_eligibility"] == "blocked"
    assert row["source_gate"] == "scope review required"


def test_build_adjudication_sorts_apply_ready_first() -> None:
    rows = build_adjudication(
        [
            rename_row(listing_key="OTC::BBB", ticker="BBB"),
            rename_row(),
        ],
        deepseek={},
        validation={"OTC::AAA": validation_row()},
        queues={
            "OTC::BBB": {
                "listing_key": "OTC::BBB",
                "validation_status": "pending_provider_env",
            }
        },
        identifiers={"OTC::AAA": {"listing_key": "OTC::AAA", "figi": "BBGAAA"}},
        coverage={},
        name_overrides={},
        source_truth={},
    )

    assert [row["listing_key"] for row in rows] == ["OTC::AAA", "OTC::BBB"]

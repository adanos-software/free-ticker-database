from scripts.validate_isin_collisions_with_deepseek import (
    build_prompt,
    normalize_verdict,
    run_validation,
    select_top_groups,
    summarize,
)


def group(isin, decision, members):
    return {
        "isin": isin,
        "registered_country": "Canada",
        "decision_candidate": decision,
        "_members": [
            {"listing_key": lk, "ticker": tk, "exchange": ex, "name": nm}
            for lk, tk, ex, nm in members
        ],
    }


SAMPLE = group(
    "CA08663L1040",
    "ticker_collision_isin_misassignment_suspected",
    [
        ("NYSE ARCA::SPXU", "SPXU", "NYSE ARCA", "ProShares UltraPro Short S&P500"),
        ("TSX::SPXU", "SPXU", "TSX", "Global X S&P 500 Index Bull ETF"),
    ],
)


def test_select_top_groups_keeps_order_and_limit():
    payload = {"items": [SAMPLE, group("X", "isin_shared_by_distinct_issuers", [("A::1", "A", "A", "Foo Inc")])]}
    selected = select_top_groups(payload, limit=1)
    assert len(selected) == 1
    assert selected[0]["isin"] == "CA08663L1040"


def test_build_prompt_includes_members_and_requests_exact_count():
    prompt = build_prompt([SAMPLE])
    assert "Return exactly 1 verdict objects" in prompt
    assert "NYSE ARCA::SPXU" in prompt
    assert "TSX::SPXU" in prompt


def test_normalize_verdict_filters_unknown_keys_and_marks_agreement():
    raw = {
        "classification": "distinct_issuers",
        "likely_correct_listing_keys": ["TSX::SPXU"],
        "likely_misassigned_listing_keys": ["NYSE ARCA::SPXU", "BOGUS::KEY"],
        "confidence": 0.95,
        "evidence_needed": "official registry",
        "rationale": "ISIN is Canadian",
    }
    verdict = normalize_verdict(raw, SAMPLE)
    assert verdict["deepseek_classification"] == "distinct_issuers"
    assert verdict["likely_correct_listing_keys"] == "TSX::SPXU"
    # Hallucinated listing keys not present in the group are dropped.
    assert verdict["likely_misassigned_listing_keys"] == "NYSE ARCA::SPXU"
    assert verdict["agrees_with_detector"] == "true"
    assert verdict["deepseek_confidence"] == 0.95


def test_normalize_verdict_clamps_bad_confidence_and_defaults_classification():
    verdict = normalize_verdict({"classification": "nonsense", "confidence": "high"}, SAMPLE)
    assert verdict["deepseek_classification"] == "uncertain"
    assert verdict["deepseek_confidence"] == 0.0
    # Detector says distinct; DeepSeek "uncertain" is not agreement.
    assert verdict["agrees_with_detector"] == "false"


def test_run_validation_with_injected_caller_collects_verdicts():
    payload = {"items": [SAMPLE]}

    def fake_call(_prompt):
        return {
            "verdicts": [
                {
                    "isin": "CA08663L1040",
                    "classification": "distinct_issuers",
                    "likely_correct_listing_keys": ["TSX::SPXU"],
                    "likely_misassigned_listing_keys": ["NYSE ARCA::SPXU"],
                    "confidence": 0.9,
                    "evidence_needed": "official registry",
                    "rationale": "ISIN country is CA",
                }
            ]
        }

    verdicts, errors = run_validation(
        queue_payload=payload, limit=12, batch_size=4, call_fn=fake_call
    )
    assert errors == []
    assert len(verdicts) == 1
    assert verdicts[0]["agrees_with_detector"] == "true"

    summary = summarize(verdicts, errors, generated_at="2026-05-30T00:00:00Z")
    assert summary["validated_groups"] == 1
    assert summary["agrees_with_detector"] == 1
    assert summary["classification_totals"] == {"distinct_issuers": 1}


def test_run_validation_records_batch_errors_without_raising():
    payload = {"items": [SAMPLE]}

    def broken_call(_prompt):
        return {"verdicts": []}  # wrong count -> ValueError captured per batch

    verdicts, errors = run_validation(
        queue_payload=payload, limit=12, batch_size=4, call_fn=broken_call
    )
    assert verdicts == []
    assert len(errors) == 1
    assert errors[0]["batch_index"] == 1


def test_dry_run_path_produces_uncertain_verdicts():
    payload = {"items": [SAMPLE]}
    verdicts, errors = run_validation(queue_payload=payload, limit=12, batch_size=4, call_fn=None)
    assert errors == []
    assert verdicts[0]["deepseek_classification"] == "uncertain"

from scripts.build_twelvedata_second_source_queue import build_queue, provider_queue_for


def test_provider_queue_for_us_batch_a_uses_all_configured_sources() -> None:
    assert provider_queue_for("NASDAQ") == "openfigi|alphavantage|fmp"
    assert provider_queue_for("OTC") == "openfigi|alphavantage|fmp"


def test_build_queue_joins_deepseek_reviews_to_source_candidates() -> None:
    rows = build_queue(
        [
            {
                "listing_key": "NASDAQ::AAA",
                "ticker": "AAA",
                "exchange": "NASDAQ",
                "mic_code": "XNMS",
                "local_name": "Old AAA",
                "twelvedata_name": "New AAA",
                "twelvedata_type": "Common Stock",
                "name_score": "0.2",
            }
        ],
        [
            {
                "listing_key": "NASDAQ::AAA",
                "decision_candidate": "needs_official_evidence",
                "safe_action": "needs_official_evidence",
                "confidence": "0.1",
            },
            {
                "listing_key": "NASDAQ::MISSING",
                "decision_candidate": "uncertain",
                "safe_action": "needs_official_evidence",
                "confidence": "0.1",
            },
        ],
    )

    assert len(rows) == 1
    assert rows[0]["listing_key"] == "NASDAQ::AAA"
    assert rows[0]["provider_queue"] == "openfigi|alphavantage|fmp"
    assert rows[0]["validation_status"] == "pending_provider_env"
    assert "DeepSeek triage alone is not apply evidence" in rows[0]["evidence_required"]

import requests

from scripts.validate_twelvedata_second_sources import (
    ProviderEvidence,
    choose_validation_status,
    classify_name,
    fetch_fmp,
    validate_row,
)


def candidate_row() -> dict[str, str]:
    return {
        "listing_key": "NASDAQ::AAA",
        "ticker": "AAA",
        "exchange": "NASDAQ",
        "local_name": "Old Holdings Inc",
        "twelvedata_name": "New Holdings Inc",
        "twelvedata_type": "Common Stock",
        "name_score": "0.2",
        "deepseek_decision_candidate": "needs_official_evidence",
        "deepseek_safe_action": "needs_official_evidence",
    }


def test_classify_name_identifies_provider_support() -> None:
    assert classify_name("New Holdings Corporation", "Old Holdings Inc", "New Holdings Inc") == "supports_twelvedata"
    assert classify_name("Old Holdings Corporation", "Old Holdings Inc", "New Holdings Inc") == "supports_local"
    assert classify_name("", "Old Holdings Inc", "New Holdings Inc") == "no_name"


def test_choose_validation_status_prefers_conservative_status() -> None:
    assert choose_validation_status(
        ["supports_twelvedata", "no_name", "no_name"],
        ["ok", "no_match", "no_match"],
    )[0] == "second_source_supports_twelvedata_name"
    assert choose_validation_status(
        ["supports_twelvedata", "supports_local", "no_name"],
        ["ok", "ok", "no_match"],
    )[0] == "conflicting_second_source_evidence"
    assert choose_validation_status(
        ["no_name", "no_name", "no_name"],
        ["skipped_missing_env", "skipped_missing_env", "skipped_missing_env"],
    )[0] == "provider_validation_not_available"


def test_validate_row_combines_provider_evidence(monkeypatch) -> None:
    monkeypatch.setattr(
        "scripts.validate_twelvedata_second_sources.fetch_openfigi",
        lambda row, session, api_key: ProviderEvidence(status="ok", name="New Holdings Inc", figi="BBGTEST"),
    )
    monkeypatch.setattr(
        "scripts.validate_twelvedata_second_sources.fetch_alphavantage",
        lambda row, session, api_key: ProviderEvidence(status="no_match"),
    )
    monkeypatch.setattr(
        "scripts.validate_twelvedata_second_sources.fetch_fmp",
        lambda row, session, api_key: ProviderEvidence(status="no_match"),
    )

    row = validate_row(
        candidate_row(),
        session=requests.Session(),
        keys={"OPENFIGI_API_KEY": "x", "ALPHAVANTAGE_API_KEY": "x", "FMP_API_KEY": "x"},
    )

    assert row["openfigi_match"] == "supports_twelvedata"
    assert row["validation_status"] == "second_source_supports_twelvedata_name"
    assert row["openfigi_figi"] == "BBGTEST"


def test_fetch_fmp_classifies_rate_limit() -> None:
    class Response:
        status_code = 429

        def raise_for_status(self):
            raise AssertionError("raise_for_status should not be called for 429")

    class Session:
        def get(self, *args, **kwargs):
            return Response()

    evidence = fetch_fmp(candidate_row(), Session(), "secret")

    assert evidence.status == "rate_limited_or_unavailable"

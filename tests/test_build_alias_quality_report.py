from __future__ import annotations

from scripts.build_alias_quality_report import build_alias_quality_rows, summarize


def test_build_alias_quality_rows_classifies_detection_safety():
    tickers = [
        {"ticker": "MSFT", "isin": "US5949181045", "wkn": ""},
        {"ticker": "AAA", "isin": "", "wkn": ""},
        {"ticker": "BBB", "isin": "", "wkn": ""},
    ]
    aliases = [
        {"ticker": "MSFT", "alias": "US5949181045", "alias_type": "isin"},
        {"ticker": "MSFT", "alias": "microsoft", "alias_type": "name"},
        {"ticker": "AAA", "alias": "shared issuer", "alias_type": "name"},
        {"ticker": "BBB", "alias": "shared issuer", "alias_type": "name"},
    ]

    rows = build_alias_quality_rows(tickers, aliases)
    by_alias_type = {(row["ticker"], row["alias"], row["alias_type"]): row for row in rows}

    assert by_alias_type[("MSFT", "US5949181045", "isin")]["detection_policy"] == "identifier_only"
    assert by_alias_type[("MSFT", "microsoft", "name")]["detection_policy"] == "safe_natural_language"
    assert by_alias_type[("AAA", "shared issuer", "name")]["detection_policy"] == "ambiguous_duplicate"
    assert by_alias_type[("AAA", "shared issuer", "name")]["duplicate_ticker_count"] == "2"


def test_alias_quality_summary_counts_status_policy_and_type():
    rows = [
        {
            "ticker": "MSFT",
            "alias": "microsoft",
            "alias_type": "name",
            "status": "accept",
            "detection_policy": "safe_natural_language",
            "confidence": "0.90",
            "reason": "accepted_name_alias",
            "duplicate_ticker_count": "1",
        },
        {
            "ticker": "MSFT",
            "alias": "US5949181045",
            "alias_type": "isin",
            "status": "reject",
            "detection_policy": "identifier_only",
            "confidence": "0.99",
            "reason": "identifier_alias",
            "duplicate_ticker_count": "1",
        },
    ]

    payload = summarize(rows, "2026-04-17T00:00:00Z")

    assert payload["summary"]["status_counts"] == {"accept": 1, "reject": 1}
    assert payload["summary"]["detection_policy_counts"] == {
        "safe_natural_language": 1,
        "identifier_only": 1,
    }
    assert payload["summary"]["alias_type_status_counts"]["name"] == {"accept": 1}
    assert payload["summary"]["alias_type_status_counts"]["isin"] == {"reject": 1}

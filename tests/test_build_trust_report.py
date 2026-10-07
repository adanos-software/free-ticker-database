from __future__ import annotations

from scripts.build_trust_report import build_trust_report, coverage_stat, render_markdown


def listing(
    listing_key: str,
    *,
    isin: str = "US0378331005",
    stock_sector: str = "Information Technology",
    etf_category: str = "",
    asset_type: str = "Stock",
    country: str = "United States",
    country_code: str = "US",
    exchange: str = "NASDAQ",
    ticker: str = "AAPL",
) -> dict[str, str]:
    return {
        "listing_key": listing_key,
        "ticker": ticker,
        "exchange": exchange,
        "name": "Apple Inc",
        "asset_type": asset_type,
        "stock_sector": stock_sector,
        "etf_category": etf_category,
        "country": country,
        "country_code": country_code,
        "isin": isin,
    }


def scope(listing_key: str, instrument_scope: str = "core") -> dict[str, str]:
    return {"listing_key": listing_key, "instrument_scope": instrument_scope}


def test_coverage_stat_fills_needed_uses_ceil_99_percent() -> None:
    stat = coverage_stat(63312, 62208)
    assert stat["missing"] == 1104
    assert stat["fills_needed_for_99"] == 471
    assert stat["pass"] is False
    assert coverage_stat(100, 99)["pass"] is True
    assert coverage_stat(100, 98)["fills_needed_for_99"] == 1


def test_trust_report_ignores_extended_rows_and_does_not_count_gaps_as_filled() -> None:
    report = build_trust_report(
        listings=[
            listing("NASDAQ::AAPL"),
            listing("OTC::JUNK", isin="", stock_sector=""),
        ],
        scopes=[scope("NASDAQ::AAPL"), scope("OTC::JUNK", "extended")],
        entry_quality=[],
        collision_queue=[],
        quarantine=[],
        alias_audit={"summary": {"blocking_issue_count": 0}},
        coverage={},
        audit=None,
    )
    assert report["universe"]["rows"] == 1
    assert report["completeness"]["isin"]["filled"] == 1
    assert report["claim_allowed"] is False
    assert "stratified_audit_99" in report["failing_checks"]
    assert "core_equity_recall_99" in report["failing_checks"]


def test_allowlisted_official_isin_mismatch_is_not_a_known_identity_bug() -> None:
    report = build_trust_report(
        listings=[listing("NYSE ARCA::SPXU")],
        scopes=[scope("NYSE ARCA::SPXU")],
        entry_quality=[
            {
                "listing_key": "NYSE ARCA::SPXU",
                "issue_types": "official_isin_mismatch",
                "quality_status": "warn",
            }
        ],
        collision_queue=[],
        quarantine=[],
        alias_audit={"summary": {"blocking_issue_count": 0}},
        coverage={},
        audit={"status": "pass"},
        warn_allowlist=[
            {
                "listing_key": "NYSE ARCA::SPXU",
                "issue_types": "official_isin_mismatch",
            }
        ],
    )
    identity = next(item for item in report["checks"] if item["name"] == "core_identity_known_bugs_zero")
    assert identity["status"] == "pass"
    assert identity["details"]["official_isin_mismatch"] == 1
    assert identity["details"]["official_isin_mismatch_unexpected"] == 0
    assert identity["details"]["official_isin_mismatch_reviewed_holds"] == 1
    assert identity["details"]["sample_mismatch"] == []


def test_known_identity_bugs_fail_even_when_completeness_is_full() -> None:
    report = build_trust_report(
        listings=[listing("NYSE ARCA::SPXU"), listing("TSX::SPXU", exchange="TSX", ticker="SPXU")],
        scopes=[scope("NYSE ARCA::SPXU"), scope("TSX::SPXU")],
        entry_quality=[
            {
                "listing_key": "NYSE ARCA::SPXU",
                "issue_types": "official_isin_mismatch",
                "quality_status": "warn",
            }
        ],
        collision_queue=[
            {
                "isin": "CA08663L1040",
                "listing_keys": "NYSE ARCA::SPXU|TSX::SPXU",
                "closure_status": "open_needs_official_identifier_evidence",
            }
        ],
        quarantine=[],
        alias_audit={"summary": {"blocking_issue_count": 0}},
        coverage={},
        audit={"status": "pass"},
    )
    identity = next(item for item in report["checks"] if item["name"] == "core_identity_known_bugs_zero")
    assert identity["status"] == "fail"
    assert identity["details"]["official_isin_mismatch"] == 1
    assert identity["details"]["open_collision_groups"] == 1
    assert report["status"] == "fail"
    assert report["claim_allowed"] is False


def test_markdown_refuses_a_public_claim() -> None:
    report = build_trust_report(
        listings=[listing("NASDAQ::AAPL")],
        scopes=[scope("NASDAQ::AAPL")],
        entry_quality=[],
        collision_queue=[],
        quarantine=[],
        alias_audit={"summary": {"blocking_issue_count": 0}},
        coverage={},
        audit=None,
    )
    markdown = render_markdown({**report, "policy": "policy"}, generated_at="2026-10-05T00:00:00Z")
    assert "Public 99% claim allowed: **no**" in markdown
    assert "never claims 99% correctness" in markdown

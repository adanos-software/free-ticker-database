from __future__ import annotations

import json

import pytest

from scripts.normalize_source_registry import SOURCES_JSON, normalize_registry, normalize_source


def test_unknown_external_license_stays_review_required() -> None:
    row = normalize_source({"key": "official", "official": True, "reference_scope": "exchange_directory", "source_url": "https://example.test"})
    assert row["license_status"] == "review_required"
    assert row["commercial_use_status"] == "review_required"
    assert row["freshness_sla_days"] == 7


def test_internal_source_can_use_explicit_internal_governance() -> None:
    row = normalize_source({"key": "internal", "source_url": "internal://review", "official": False})
    assert row["license_status"] == "internal"
    assert row["commercial_use_status"] == "allowed"


def test_verified_restricted_review_is_preserved_without_inferring_open() -> None:
    row = normalize_source(
        {
            "key": "nasdaq_listed",
            "official": True,
            "source_url": "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt",
            "license_status": "verified_restricted",
            "license_name": "Nasdaq Trader Copyright, Trademarks and Disclaimers",
            "license_url": "https://www.nasdaqtrader.com/Trader.aspx?id=CopyDisclaimMain",
            "derived_facts_redistribution_status": "restricted",
            "raw_redistribution_allowed": False,
            "attribution_required": "required",
            "commercial_use_status": "restricted",
            "terms_version": "nasdaqtrader-copydisclaim-main-copyright-2021",
            "terms_sha256": "A" * 64,
            "license_reviewed_at": "2026-08-20T17:50:00Z",
        }
    )
    assert row["license_status"] == "verified_restricted"
    assert row["derived_facts_redistribution_status"] == "restricted"
    assert row["commercial_use_status"] == "restricted"
    assert row["raw_redistribution_allowed"] is False
    assert row["terms_sha256"] == "a" * 64


def test_registry_order_is_preserved() -> None:
    rows = normalize_registry([{"key": "b"}, {"key": "a"}])
    assert [row["key"] for row in rows] == ["b", "a"]


def test_duplicate_keys_fail() -> None:
    with pytest.raises(ValueError, match="duplicate source keys"):
        normalize_registry([{"key": "a"}, {"key": "a"}])


def test_live_nasdaq_and_euronext_reviews_stay_restricted_not_open() -> None:
    rows = json.loads(SOURCES_JSON.read_text(encoding="utf-8"))
    by_key = {row["key"]: row for row in rows}
    restricted_keys = (
        "nasdaq_listed",
        "nasdaq_other_listed",
        "nasdaq_trading_system_adds_deletes",
        "euronext_equities",
        "euronext_etfs",
        "qse_market_watch",
        "pse_listed_company_directory",
        "dfm_listed_securities",
        "bist_kap_mkk_listed_securities",
    )
    for key in restricted_keys:
        row = by_key[key]
        assert row["license_status"] == "verified_restricted"
        assert row["derived_facts_redistribution_status"] == "restricted"
        assert row["commercial_use_status"] == "restricted"
        assert row["raw_redistribution_allowed"] is False
        assert row["license_url"].startswith("https://")
        assert len(row["terms_sha256"]) == 64
        assert row["license_reviewed_at"]
    assert by_key["sec_company_tickers_exchange"]["license_status"] == "verified_open"
    santiago = by_key["bolsa_santiago_instruments"]
    assert santiago["license_status"] == "review_required"
    assert santiago["terms_sha256"] == ""

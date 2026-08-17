from __future__ import annotations

import pytest

from scripts.normalize_source_registry import normalize_registry, normalize_source


def test_unknown_external_license_stays_review_required() -> None:
    row = normalize_source({"key": "official", "official": True, "reference_scope": "exchange_directory", "source_url": "https://example.test"})
    assert row["license_status"] == "review_required"
    assert row["commercial_use_status"] == "review_required"
    assert row["freshness_sla_days"] == 7


def test_internal_source_can_use_explicit_internal_governance() -> None:
    row = normalize_source({"key": "internal", "source_url": "internal://review", "official": False})
    assert row["license_status"] == "internal"
    assert row["commercial_use_status"] == "allowed"


def test_registry_order_is_preserved() -> None:
    rows = normalize_registry([{"key": "b"}, {"key": "a"}])
    assert [row["key"] for row in rows] == ["b", "a"]


def test_duplicate_keys_fail() -> None:
    with pytest.raises(ValueError, match="duplicate source keys"):
        normalize_registry([{"key": "a"}, {"key": "a"}])

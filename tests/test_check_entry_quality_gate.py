from __future__ import annotations

from scripts.check_entry_quality_gate import check_entry_quality_gate


def row(listing_key: str, quality_status: str) -> dict[str, str]:
    return {"listing_key": listing_key, "quality_status": quality_status}


def test_entry_quality_gate_passes_allowed_warns():
    result = check_entry_quality_gate(
        [
            row("NASDAQ::MSFT", "pass"),
            row("OTC::AECX", "warn"),
        ],
        {"OTC::AECX"},
    )

    assert result["passed"] is True
    assert result["unexpected_warn_count"] == 0


def test_entry_quality_gate_fails_new_warns_and_quarantine():
    result = check_entry_quality_gate(
        [
            row("OTC::NEW", "warn"),
            row("NYSE::BAD", "quarantine"),
        ],
        {"OTC::OLD"},
    )

    assert result["passed"] is False
    assert result["unexpected_warns"] == ["OTC::NEW"]
    assert result["quarantined"] == ["NYSE::BAD"]
    assert result["stale_allowlist"] == ["OTC::OLD"]

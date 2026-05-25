from scripts.apply_ngx_official_sector_review import (
    apply_gate_context_for,
    build_apply_rows,
    build_metadata_updates,
    mapping_review_context_for,
    official_source_context_for,
    summarize,
)


def test_build_apply_rows_applies_only_explicit_canonical_mapping() -> None:
    rows = build_apply_rows(
        [
            {
                "listing_key": "NGX::GEREGU",
                "ticker": "GEREGU",
                "exchange": "NGX",
                "name": "GEREGU POWER PLC",
                "official_masterfile_sector_values": "UTILITIES",
                "residual_decision": "official_sector_available_review_apply",
            },
            {
                "listing_key": "NGX::ABCTRANS",
                "ticker": "ABCTRANS",
                "exchange": "NGX",
                "name": "ABC TRANSPORT PLC",
                "official_masterfile_sector_values": "SERVICES",
                "residual_decision": "official_sector_available_review_apply",
            },
        ]
    )

    by_ticker = {row["ticker"]: row for row in rows}
    assert by_ticker["GEREGU"]["decision"] == "apply"
    assert by_ticker["GEREGU"]["sector_update"] == "Utilities"
    assert by_ticker["GEREGU"]["official_source_context"] == official_source_context_for(by_ticker["GEREGU"])
    assert by_ticker["GEREGU"]["mapping_review_context"] == mapping_review_context_for(by_ticker["GEREGU"])
    assert by_ticker["GEREGU"]["apply_gate_context"] == apply_gate_context_for(by_ticker["GEREGU"])
    assert by_ticker["ABCTRANS"]["decision"] == "skip"
    assert by_ticker["ABCTRANS"]["reason"] == "unsupported_broad_ngx_sector_label"
    assert by_ticker["ABCTRANS"]["mapping_review_context"] == (
        "official_sector=SERVICES;sector_update=none;mapping_supported=false"
    )


def test_build_apply_rows_requires_ngx_review_apply_decision() -> None:
    rows = build_apply_rows(
        [
            {
                "listing_key": "NGX::GEREGU",
                "ticker": "GEREGU",
                "exchange": "NGX",
                "official_masterfile_sector_values": "UTILITIES",
                "residual_decision": "accepted_source_gap_official_masterfile_without_sector",
            },
            {
                "listing_key": "BK::UTIL",
                "ticker": "UTIL",
                "exchange": "BK",
                "official_masterfile_sector_values": "UTILITIES",
                "residual_decision": "official_sector_available_review_apply",
            },
        ]
    )

    assert rows == []


def test_build_metadata_updates_only_uses_apply_rows() -> None:
    updates = build_metadata_updates(
        [
            {
                "ticker": "GEREGU",
                "exchange": "NGX",
                "sector_update": "Utilities",
                "decision": "apply",
            },
            {
                "ticker": "ABCTRANS",
                "exchange": "NGX",
                "sector_update": "",
                "decision": "skip",
            },
        ]
    )

    assert updates == [
        {
            "ticker": "GEREGU",
            "exchange": "NGX",
            "field": "stock_sector",
            "decision": "update",
            "proposed_value": "Utilities",
            "confidence": "0.97",
            "reason": (
                "Official NGX company profile sector mapped to canonical stock_sector; accepted only for "
                "explicit one-to-one venue-specific sector mappings after listing_key and official-source gates."
            ),
        }
    ]


def test_summarize_counts_ngx_apply_decisions() -> None:
    summary = summarize(
        [
            {"decision": "apply", "official_sector": "UTILITIES", "sector_update": "Utilities"},
            {"decision": "skip", "official_sector": "SERVICES", "sector_update": ""},
        ],
        "2026-05-24T00:00:00Z",
        applied=True,
        written_updates=1,
    )

    assert summary["decision_totals"] == {"apply": 1, "skip": 1}
    assert summary["official_sector_totals"] == {"SERVICES": 1, "UTILITIES": 1}
    assert summary["sector_update_totals"] == {"Utilities": 1}
    assert summary["written_updates"] == 1

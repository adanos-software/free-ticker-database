from scripts.apply_b3_etf_category_review import (
    apply_gate_context_for,
    build_apply_rows,
    build_metadata_updates,
    category_review_context_for,
    official_source_context_for,
    summarize,
)


def test_build_apply_rows_applies_only_exact_official_b3_etf_category_mismatches() -> None:
    rows = build_apply_rows(
        [
            {
                "listing_key": "B3::B5MB11",
                "ticker": "B5MB11",
                "exchange": "B3",
                "asset_type": "ETF",
                "name": "ETF Bradesco Ima-B5 Plus Fundo De Indice",
                "current_etf_category": "Equity",
                "source_presence": "present_only_in_non_exchange_directory_source",
                "candidate_sources": "b3_listed_etfs",
                "candidate_source_urls": "https://sistemaswebb3-listados.b3.com.br/fundsListedProxy/Search/",
                "candidate_sectors": "Fixed Income",
                "candidate_category_review_decision": "official_candidate_category_differs_from_current_requires_review",
            },
            {
                "listing_key": "B3::BDAP11",
                "ticker": "BDAP11",
                "exchange": "B3",
                "asset_type": "ETF",
                "current_etf_category": "Fixed Income",
                "source_presence": "present_only_in_non_exchange_directory_source",
                "candidate_sources": "b3_listed_etfs",
                "candidate_sectors": "Fixed Income",
                "candidate_category_review_decision": "official_candidate_category_already_reflected",
            },
            {
                "listing_key": "B3::STOCK3",
                "ticker": "STOCK3",
                "exchange": "B3",
                "asset_type": "Stock",
                "current_etf_category": "",
                "source_presence": "present_only_in_non_exchange_directory_source",
                "candidate_sources": "b3_listed_etfs",
                "candidate_sectors": "Fixed Income",
                "candidate_category_review_decision": "official_candidate_category_differs_from_current_requires_review",
            },
        ]
    )

    by_ticker = {row["ticker"]: row for row in rows}
    assert by_ticker["B5MB11"]["decision"] == "apply"
    assert by_ticker["B5MB11"]["category_update"] == "Fixed Income"
    assert by_ticker["B5MB11"]["official_source_context"] == official_source_context_for(by_ticker["B5MB11"])
    assert by_ticker["B5MB11"]["category_review_context"] == category_review_context_for(by_ticker["B5MB11"])
    assert by_ticker["B5MB11"]["apply_gate_context"] == apply_gate_context_for(by_ticker["B5MB11"])
    assert by_ticker["BDAP11"]["decision"] == "skip"
    assert by_ticker["STOCK3"]["decision"] == "skip"


def test_build_metadata_updates_only_uses_apply_rows() -> None:
    updates = build_metadata_updates(
        [
            {
                "ticker": "B5MB11",
                "exchange": "B3",
                "category_update": "Fixed Income",
                "candidate_source_urls": "https://sistemaswebb3-listados.b3.com.br/fundsListedProxy/Search/",
                "decision": "apply",
            },
            {
                "ticker": "BDAP11",
                "exchange": "B3",
                "category_update": "Fixed Income",
                "candidate_source_urls": "https://sistemaswebb3-listados.b3.com.br/fundsListedProxy/Search/",
                "decision": "skip",
            },
        ]
    )

    assert updates == [
        {
            "ticker": "B5MB11",
            "exchange": "B3",
            "field": "etf_category",
            "decision": "update",
            "proposed_value": "Fixed Income",
            "confidence": "0.97",
            "reason": (
                "Official B3 listed ETF source provided a single canonical ETF category for the exact "
                "B3 listing; applied only after listing_key, ETF asset type, official source, and "
                "current-vs-official mismatch gates. "
                "Source: https://sistemaswebb3-listados.b3.com.br/fundsListedProxy/Search/"
            ),
        }
    ]


def test_summarize_counts_apply_rows() -> None:
    summary = summarize(
        [
            {"decision": "apply", "official_sector": "Fixed Income", "category_update": "Fixed Income"},
            {"decision": "skip", "official_sector": "Fixed Income", "category_update": ""},
        ],
        "2026-05-24T00:00:00Z",
        applied=True,
        written_updates=1,
    )

    assert summary["decision_totals"] == {"apply": 1, "skip": 1}
    assert summary["official_sector_totals"] == {"Fixed Income": 2}
    assert summary["category_update_totals"] == {"Fixed Income": 1}
    assert summary["written_updates"] == 1

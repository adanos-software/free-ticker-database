from __future__ import annotations

from scripts.verify_yahoo_high_risk_aliases import (
    build_remove_overrides,
    evaluate_alias_case,
    extract_alias_cases,
    quote_matches_row,
)


def test_extract_alias_cases_filters_to_alias_findings():
    items = [
        {
            "ticker": "XYZ",
            "exchange": "NYSE",
            "name": "Block, Inc",
            "asset_type": "Stock",
            "country": "United States",
            "country_code": "US",
            "isin": "",
            "findings": [
                {"finding_type": "cross_company_alias_collision", "field": "alias", "value": "square inc", "reason": "x"},
                {"finding_type": "invalid_isin", "field": "isin", "value": "BAD", "reason": "y"},
            ],
        }
    ]

    cases = extract_alias_cases(items, finding_types={"cross_company_alias_collision"})
    assert cases == [
        {
            "ticker": "XYZ",
            "exchange": "NYSE",
            "name": "Block, Inc",
            "asset_type": "Stock",
            "country": "United States",
            "country_code": "US",
            "isin": "",
            "alias": "square inc",
            "finding_type": "cross_company_alias_collision",
            "finding_reason": "x",
        }
    ]


def test_quote_matches_row_accepts_symbol_or_name_match():
    row = {"ticker": "XYZ", "name": "Block, Inc"}
    assert quote_matches_row({"symbol": "XYZ", "longname": "", "shortname": ""}, row) is True
    assert quote_matches_row({"symbol": "0L95.IL", "longname": "Block, Inc.", "shortname": ""}, row) is True
    assert quote_matches_row({"symbol": "SQ", "longname": "Something Else", "shortname": ""}, row) is False


def test_evaluate_alias_case_prefers_local_and_yahoo_support():
    local_case = {
        "ticker": "IAG",
        "exchange": "LSE",
        "name": "International Consolidated Airlines Group S.A.",
        "alias": "international consolidated airlines group",
        "finding_type": "low_company_name_overlap",
    }
    assert evaluate_alias_case(local_case, [])["status"] == "supported_local"

    yahoo_case = {
        "ticker": "XYZ",
        "exchange": "NYSE",
        "name": "Block, Inc",
        "alias": "square inc",
        "finding_type": "cross_company_alias_collision",
    }
    supported = evaluate_alias_case(
        yahoo_case,
        [{"symbol": "0L95.IL", "longname": "Block, Inc.", "shortname": "SQUARE INC"}],
    )
    assert supported["status"] == "supported_yahoo"


def test_evaluate_alias_case_marks_noise_and_other_entities():
    blocked = evaluate_alias_case(
        {
            "ticker": "XYZ",
            "exchange": "NYSE",
            "name": "Block, Inc",
            "alias": "cash app",
            "finding_type": "low_company_name_overlap",
        },
        [],
    )
    assert blocked["status"] == "remove_blocked"

    wrong_entity = evaluate_alias_case(
        {
            "ticker": "SQ",
            "exchange": "SET",
            "name": "Sahakol Equipment Public Company Limited",
            "alias": "square inc",
            "finding_type": "cross_company_alias_collision",
        },
        [{"symbol": "0L95.IL", "longname": "Block, Inc.", "shortname": "SQUARE INC"}],
    )
    assert wrong_entity["status"] == "likely_other_entity"

    company_style_ambiguous = evaluate_alias_case(
        {
            "ticker": "FG",
            "exchange": "NYSE",
            "name": "F&G Annuities & Life Inc.",
            "alias": "square inc",
            "finding_type": "low_company_name_overlap",
        },
        [{"symbol": "0L95.IL", "longname": "Some Other Co", "shortname": "SQUARE INC"}],
    )
    assert company_style_ambiguous["status"] == "needs_human"


def test_build_remove_overrides_only_emits_removals():
    overrides = build_remove_overrides(
        [
            {"ticker": "XYZ", "exchange": "NYSE", "alias": "cash app", "status": "remove_blocked"},
            {"ticker": "SQ", "exchange": "SET", "alias": "square inc", "status": "likely_other_entity"},
            {"ticker": "IAG", "exchange": "LSE", "alias": "international consolidated airlines group", "status": "supported_local"},
        ]
    )
    assert overrides == [
        {
            "ticker": "XYZ",
            "exchange": "NYSE",
            "alias": "cash app",
            "reason": "Yahoo alias verification marked `cash app` as remove_blocked.",
        },
        {
            "ticker": "SQ",
            "exchange": "SET",
            "alias": "square inc",
            "reason": "Yahoo alias verification marked `square inc` as likely_other_entity.",
        },
    ]

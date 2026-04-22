from __future__ import annotations

from scripts.alias_policy import (
    classify_alias_for_natural_language,
    is_generic_multiword_alias,
    is_generic_organization_alias,
    is_generic_wrapper_alias,
    should_drop_from_ticker_alias_column,
)
from scripts.rebuild_dataset import drop_duplicate_ticker_aliases
from scripts.rebuild_dataset import clean_aliases


def test_identifier_aliases_are_not_natural_language_aliases():
    decision = classify_alias_for_natural_language(
        alias="US5949181045",
        alias_type="isin",
        ticker="MSFT",
        isin="US5949181045",
    )

    assert decision.status == "reject"
    assert decision.detection_policy == "identifier_only"
    assert should_drop_from_ticker_alias_column(alias="US5949181045", isin="US5949181045")


def test_duplicate_name_aliases_require_review():
    decision = classify_alias_for_natural_language(
        alias="novo nordisk",
        alias_type="name",
        ticker="NVO",
        duplicate_ticker_count=3,
    )

    assert decision.status == "review"
    assert decision.detection_policy == "ambiguous_duplicate"


def test_generic_fund_wrapper_aliases_are_blocked():
    assert is_generic_wrapper_alias("Invesco Exchange-Traded Fund Trust II")

    decision = classify_alias_for_natural_language(
        alias="Invesco Exchange-Traded Fund Trust II",
        alias_type="name",
        ticker="RSPE",
    )

    assert decision.status == "reject"
    assert decision.reason == "generic_fund_or_trust_wrapper"


def test_generic_organization_aliases_are_blocked():
    assert is_generic_organization_alias("Central Bank")

    decision = classify_alias_for_natural_language(
        alias="Central Bank",
        alias_type="name",
        ticker="CBSU",
    )

    assert decision.status == "reject"
    assert decision.reason == "generic_organization_alias"
    assert should_drop_from_ticker_alias_column(alias="Central Bank")


def test_generic_multiword_aliases_are_blocked():
    assert is_generic_multiword_alias("Government Bond")

    decision = classify_alias_for_natural_language(
        alias="Government Bond",
        alias_type="name",
        ticker="ZGB",
    )

    assert decision.status == "reject"
    assert decision.reason == "generic_multiword_alias"
    assert should_drop_from_ticker_alias_column(alias="Government Bond")


def test_short_common_aliases_are_blocked_unless_trusted_brand():
    unsafe = classify_alias_for_natural_language(alias="leo", alias_type="name", ticker="002131")
    trusted = classify_alias_for_natural_language(alias="ford", alias_type="name", ticker="F")

    assert unsafe.status == "review"
    assert unsafe.detection_policy == "context_required"
    assert should_drop_from_ticker_alias_column(alias="leo")
    assert trusted.status == "accept"
    assert not should_drop_from_ticker_alias_column(alias="ford")


def test_new_common_word_aliases_are_blocked():
    decision = classify_alias_for_natural_language(alias="Healthcare", alias_type="name", ticker="603313")

    assert decision.status == "reject"
    assert decision.reason == "common_word_alias"
    assert should_drop_from_ticker_alias_column(alias="Healthcare")


def test_duplicate_ticker_aliases_are_removed_from_public_alias_column():
    rows = [
        {"ticker": "AAA", "name": "Alpha Inc", "isin": "US0000000001", "aliases": ["shared issuer", "alpha"]},
        {"ticker": "BBB", "name": "Beta Inc", "isin": "US0000000002", "aliases": ["Shared Issuer", "beta"]},
        {"ticker": "CCC", "name": "Gamma Inc", "isin": "US0000000003", "aliases": ["gamma"]},
    ]

    cleaned = drop_duplicate_ticker_aliases(rows)

    assert cleaned[0]["aliases"] == ["alpha"]
    assert cleaned[1]["aliases"] == ["beta"]
    assert cleaned[2]["aliases"] == ["gamma"]


def test_duplicate_aliases_are_kept_for_same_isin_cross_listings():
    rows = [
        {
            "ticker": "AZT",
            "name": "Arcticzymes Techno",
            "isin": "NO0010014632",
            "aliases": ["arcticzymes technologies a"],
        },
        {
            "ticker": "0DRV",
            "name": "Arcticzymes Technologies ASA",
            "isin": "NO0010014632",
            "aliases": ["arcticzymes technologies a"],
        },
    ]

    assert drop_duplicate_ticker_aliases(rows) == rows


def test_duplicate_aliases_without_isin_are_treated_as_ambiguous():
    rows = [
        {"ticker": "BENJ", "name": "Horizon Funds", "isin": "", "aliases": ["horizon funds"]},
        {"ticker": "FRGN", "name": "Horizon Funds", "isin": "", "aliases": ["horizon funds"]},
    ]

    assert drop_duplicate_ticker_aliases(rows) == [
        {"ticker": "BENJ", "name": "Horizon Funds", "isin": "", "aliases": []},
        {"ticker": "FRGN", "name": "Horizon Funds", "isin": "", "aliases": []},
    ]


def test_duplicate_aliases_keep_best_non_otc_legal_name_owner():
    rows = [
        {
            "ticker": "0M6S",
            "name": "Allianz SE VNA O.N.",
            "exchange": "LSE",
            "asset_type": "Stock",
            "isin": "DE0008404005",
            "aliases": ["allianz"],
        },
        {
            "ticker": "ALIZF",
            "name": "Allianz SE",
            "exchange": "OTC",
            "asset_type": "Stock",
            "isin": "US0188201000",
            "aliases": ["allianz"],
        },
    ]

    assert drop_duplicate_ticker_aliases(rows) == [
        {
            "ticker": "0M6S",
            "name": "Allianz SE VNA O.N.",
            "exchange": "LSE",
            "asset_type": "Stock",
            "isin": "DE0008404005",
            "aliases": ["allianz"],
        },
        {
            "ticker": "ALIZF",
            "name": "Allianz SE",
            "exchange": "OTC",
            "asset_type": "Stock",
            "isin": "US0188201000",
            "aliases": [],
        },
    ]


def test_stock_aliases_strip_security_suffixes_and_add_safe_brand_alias():
    row = {
        "ticker": "ARTV",
        "name": "Artiva Biotherapeutics, Inc. Common Stock",
        "exchange": "NASDAQ",
        "asset_type": "Stock",
        "country": "United States",
        "isin": "US04317A1079",
    }

    _, aliases = clean_aliases(row, ["artiva biotherapeutics, inc. common stock"], set())

    assert aliases == ["artiva biotherapeutics", "artiva"]


def test_stock_aliases_strip_listing_suffixes_and_industry_tails():
    allianz = {
        "ticker": "0M6S",
        "name": "Allianz SE VNA O.N.",
        "exchange": "LSE",
        "asset_type": "Stock",
        "country": "Germany",
        "isin": "DE0008404005",
    }
    guidong = {
        "ticker": "600310",
        "name": "Guangxi Guidong Eletric Power Co Ltd",
        "exchange": "SSE",
        "asset_type": "Stock",
        "country": "China",
        "isin": "CNE000001741",
    }

    assert clean_aliases(allianz, ["allianz se vna o.n"], set())[1] == ["allianz"]
    assert clean_aliases(guidong, ["guangxi guidong eletric power"], set())[1] == ["guangxi guidong"]


def test_etf_aliases_are_shortened_and_ascii_normalized():
    row = {
        "ticker": "VALG",
        "name": "Leverage Shares 2X Long VALE Daily ETF",
        "exchange": "NASDAQ",
        "asset_type": "ETF",
        "country": "United States",
        "isin": "US88340F6960",
    }
    dax = {
        "ticker": "EXIA",
        "name": "iShares DAX® ESG UCITS ETF (DE)",
        "exchange": "XETRA",
        "asset_type": "ETF",
        "country": "Germany",
        "isin": "DE000A0Q4R69",
    }

    assert clean_aliases(row, ["leverage 2x long vale daily etf"], set())[1] == ["2x long vale"]
    assert clean_aliases(dax, ["ishares dax® esg ucits etf"], set())[1] == ["dax esg"]
    assert clean_aliases(dax, ["ishares dax® esg ucits etf de"], set())[1] == ["dax esg"]


def test_cross_exchange_contaminated_aliases_are_removed():
    row = {
        "ticker": "DAL",
        "name": "Delta Air Lines Inc",
        "exchange": "NYSE",
        "asset_type": "Stock",
        "country": "United States",
        "isin": "US2473617023",
    }

    _, aliases = clean_aliases(row, ["dalaroo metals", "delta air lines", "delta airlines"], set())

    assert aliases == ["delta air lines", "delta airlines"]

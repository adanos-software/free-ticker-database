from scripts.backfill_hkex_hsic_sectors import (
    evaluate_rows,
    hsic_to_canonical,
    parse_hsic_industry,
    quote_symbol,
)


def test_quote_symbol_strips_leading_zeroes_for_hkex_quote_url():
    assert quote_symbol("00042") == "42"
    assert quote_symbol("ABCD") == "ABCD"


def test_parse_hsic_industry_extracts_profile_value():
    text = "Issued Shares 257,950,000 Industry Industrials - Industrial Engineering - Components (HSIC) Listing Date"

    assert parse_hsic_industry(text) == "Industrials - Industrial Engineering - Components"


def test_parse_hsic_industry_uses_last_industry_label_before_hsic():
    text = (
        "Example Industry Holdings Ltd is an investment holding company. "
        "Issued Shares 100 Industry Consumer Staples - Consumer Staple Retailers - Supermarkets (HSIC) Listing Date"
    )

    assert parse_hsic_industry(text) == "Consumer Staples - Consumer Staple Retailers - Supermarkets"


def test_hsic_to_canonical_maps_hkex_top_level_taxonomy():
    assert hsic_to_canonical("Industrials - Industrial Engineering - Components") == "Industrials"
    assert hsic_to_canonical("Healthcare - Pharmaceuticals - Biotechnology") == "Health Care"
    assert hsic_to_canonical("Telecommunications - Telecommunications - Wireless") == "Communication Services"
    assert hsic_to_canonical("Properties & Construction - Properties - Property Development") == "Real Estate"
    assert hsic_to_canonical("Properties & Construction - Construction - Building Construction") == "Industrials"
    assert hsic_to_canonical("Conglomerates - Conglomerates - Conglomerates") == ""


def test_evaluate_rows_accepts_only_capture_with_official_isin_match():
    target = {
        "ticker": "00042",
        "exchange": "HKEX",
        "asset_type": "Stock",
        "name": "NE ELECTRIC",
        "isin": "CNE1000003V0",
    }
    captures = [
        {
            "ticker": "00042",
            "hkex_quote_url": "https://www.hkex.com.hk/Market-Data/Securities-Prices/Equities/Equities-Quote?sym=42&sc_lang=en",
            "hkex_heading": "NORTHEAST ELECTRIC DEVELOPMENT CO. LTD. - H SHARES (42)",
            "hkex_industry": "Industrials - Industrial Engineering - Industrial Components & Equipment",
            "error": "",
        }
    ]

    result = evaluate_rows([target], captures, {"00042": "CNE1000003V0"})[0]

    assert result["decision"] == "accept"
    assert result["sector_update"] == "Industrials"


def test_evaluate_rows_rejects_unsupported_hsic_and_isin_mismatch():
    base_target = {
        "ticker": "00025",
        "exchange": "HKEX",
        "asset_type": "Stock",
        "name": "CHEVALIER INT'L",
        "isin": "BMG2097Z1471",
    }
    unsupported = evaluate_rows(
        [base_target],
        [{"ticker": "00025", "hkex_industry": "Conglomerates - Conglomerates - Conglomerates", "error": ""}],
        {"00025": "BMG2097Z1471"},
    )[0]
    mismatch = evaluate_rows(
        [base_target],
        [{"ticker": "00025", "hkex_industry": "Financials - Banks - Banks", "error": ""}],
        {"00025": "BMG000000000"},
    )[0]

    assert unsupported["decision"] == "unsupported_hsic_sector"
    assert mismatch["decision"] == "official_isin_mismatch"

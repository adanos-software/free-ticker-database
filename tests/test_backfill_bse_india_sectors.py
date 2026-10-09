from scripts.backfill_bse_india_sectors import (
    BseScrip,
    bse_sector_to_canonical,
    evaluate_row,
)


def make_target(**overrides):
    row = {
        "ticker": "3BBLACKBIO",
        "exchange": "BSE_IN",
        "asset_type": "Stock",
        "name": "3B BlackBio Dx Ltd",
        "isin": "INE994E01018",
    }
    row.update(overrides)
    return row


def make_scrip(**overrides):
    scrip = {
        "symbol": "3BBLACKBIO",
        "scrip_code": "532067",
        "name": "3B Blackbio Dx Ltd",
        "issuer_name": "3B Blackbio Dx Ltd",
        "isin": "INE994E01018",
        "url": "https://www.bseindia.com/stock-share-price/3b-blackbio-dx-ltd/3bblackbio/532067/",
    }
    scrip.update(overrides)
    return BseScrip(**scrip)


def make_header(**overrides):
    header = {
        "SecurityId": "3BBLACKBIO",
        "ISIN": "INE994E01018",
        "Sector": "Healthcare",
        "IndustryNew": "Healthcare",
        "IGroup": "Healthcare Services",
        "ISubGroup": "Healthcare Service Provider",
    }
    header.update(overrides)
    return header


def test_bse_sector_to_canonical_maps_direct_sector():
    assert bse_sector_to_canonical("Energy", "") == "Energy"
    assert bse_sector_to_canonical("Financial Services", "") == "Financials"
    assert bse_sector_to_canonical("Fast Moving Consumer Goods", "") == "Consumer Staples"
    assert bse_sector_to_canonical("Healthcare", "") == "Health Care"


def test_bse_sector_to_canonical_maps_reviewed_services_groups():
    assert bse_sector_to_canonical("Services", "Transport Services") == "Industrials"
    assert bse_sector_to_canonical("Services", "Leisure Services") == "Consumer Discretionary"
    assert bse_sector_to_canonical("Services", "Unreviewed Services") == ""


def test_evaluate_row_accepts_matching_bse_header():
    result = evaluate_row(make_target(), make_scrip(), make_header())

    assert result["decision"] == "accept"
    assert result["sector_update"] == "Health Care"
    assert result["isin_match"] is True


def test_evaluate_row_rejects_isin_mismatch_even_when_name_matches():
    result = evaluate_row(make_target(), make_scrip(), make_header(ISIN="INE000A01000"))

    assert result["decision"] == "isin_mismatch"


def test_evaluate_row_rejects_unmapped_services_group():
    result = evaluate_row(
        make_target(),
        make_scrip(),
        make_header(Sector="Services", IGroup="Unreviewed Services"),
    )

    assert result["decision"] == "unsupported_bse_sector"


def test_evaluate_row_accepts_leftover_bse_comheader_equities():
    leftovers = [
        (
            "ASHIKAG",
            "Ashika Global Securities Ltd",
            "INE094B01013",
            "543766",
            "Financial Services",
            "Financials",
        ),
        (
            "FINANCE",
            "Seksaria Finance Ltd",
            "INE0IBX01013",
            "544907",
            "Financial Services",
            "Financials",
        ),
        (
            "IMPERA",
            "Impera Worldwide Ltd",
            "INE351D01013",
            "514177",
            "Consumer Discretionary",
            "Consumer Discretionary",
        ),
        (
            "LEXGLOBAL",
            "Lexora Global Ltd",
            "INE745A01012",
            "512345",
            "Financial Services",
            "Financials",
        ),
        (
            "TYPHOON",
            "Typhoon Holdings Ltd",
            "INE2L3M01012",
            "512307",
            "Consumer Discretionary",
            "Consumer Discretionary",
        ),
    ]
    for ticker, name, isin, scrip_code, official_sector, expected in leftovers:
        result = evaluate_row(
            make_target(ticker=ticker, name=name, isin=isin),
            make_scrip(symbol=ticker, scrip_code=scrip_code, name=name, issuer_name=name, isin=isin),
            make_header(SecurityId=ticker, ISIN=isin, Sector=official_sector),
        )
        assert result["decision"] == "accept", ticker
        assert result["sector_update"] == expected, ticker


def test_evaluate_row_rejects_leftover_gsailpp_empty_sector_and_heg_scrip_rename():
    empty_sector = evaluate_row(
        make_target(ticker="GSAILPP", name="GS Auto International Ltd", isin="IN9736H01014"),
        make_scrip(
            symbol="GSAILPP",
            scrip_code="890238",
            name="GS Auto International Ltd",
            issuer_name="G.S. Auto International Ltd.",
            isin="IN9736H01014",
        ),
        make_header(SecurityId="GSAILPP", ISIN="IN9736H01014", Sector="", IndustryNew="", IGroup=""),
    )
    assert empty_sector["decision"] == "unsupported_bse_sector"

    renamed = evaluate_row(
        make_target(ticker="HEG", name="HEG Advanced Materials Ltd", isin="INE545A01024"),
        make_scrip(
            symbol="HEGAM",
            scrip_code="509631",
            name="HEG Advanced Materials Ltd",
            issuer_name="HEG Advanced Materials Limited",
            isin="INE545A01024",
        ),
        make_header(SecurityId="HEGAM", ISIN="INE545A01024", Sector="Commodities"),
    )
    assert renamed["decision"] == "security_id_mismatch"

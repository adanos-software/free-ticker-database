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

from __future__ import annotations

from scripts.build_financialdata_isin_supplements import build_financialdata_isin_supplements


def test_build_financialdata_isin_supplements_accepts_unique_name_gated_official_isin() -> None:
    supplements, reviews, summary = build_financialdata_isin_supplements(
        financialdata_rows=[
            {
                "financialdata_symbol": "RELIANCE.NS",
                "base_ticker": "RELIANCE",
                "mapped_exchange": "NSE_IN",
                "registrant_name": "Reliance Industries Limited",
                "review_scope": "global_expansion_candidate",
            }
        ],
        masterfile_rows=[
            {
                "ticker": "RELIANCE",
                "name": "Reliance Industries Limited",
                "exchange": "NSE_IN",
                "asset_type": "Stock",
                "listing_status": "active",
                "reference_scope": "exchange_directory",
                "source_key": "nse_india_securities_available",
                "source_url": "https://example.com/nse",
                "isin": "INE002A01018",
                "sector": "",
            }
        ],
        listing_rows=[],
    )

    assert supplements == [
        {
            "ticker": "RELIANCE",
            "name": "Reliance Industries Limited",
            "exchange": "NSE_IN",
            "asset_type": "Stock",
            "sector": "",
            "country": "India",
            "country_code": "IN",
            "isin": "INE002A01018",
            "aliases": "Reliance Industries Limited",
            "source_key": "nse_india_securities_available",
            "source_url": "https://example.com/nse",
            "reference_scope": "exchange_directory",
        }
    ]
    assert reviews[0].decision == "accept"
    assert summary["supplement_rows"] == 1


def test_build_financialdata_isin_supplements_matches_hkex_padded_ticker() -> None:
    supplements, reviews, _summary = build_financialdata_isin_supplements(
        financialdata_rows=[
            {
                "financialdata_symbol": "5.HK",
                "base_ticker": "5",
                "mapped_exchange": "HKEX",
                "registrant_name": "HSBC Holdings",
                "review_scope": "global_expansion_candidate",
            }
        ],
        masterfile_rows=[
            {
                "ticker": "00005",
                "name": "HSBC HOLDINGS",
                "exchange": "HKEX",
                "asset_type": "Stock",
                "listing_status": "active",
                "reference_scope": "exchange_directory",
                "source_key": "hkex_securities_list",
                "source_url": "https://example.com/hkex",
                "isin": "GB0005405286",
                "sector": "",
            }
        ],
        listing_rows=[],
    )

    assert supplements[0]["ticker"] == "00005"
    assert supplements[0]["isin"] == "GB0005405286"
    assert reviews[0].decision == "accept"


def test_build_financialdata_isin_supplements_matches_bse_by_unique_name() -> None:
    supplements, reviews, _summary = build_financialdata_isin_supplements(
        financialdata_rows=[
            {
                "financialdata_symbol": "500209.BO",
                "base_ticker": "500209",
                "mapped_exchange": "BSE_IN",
                "registrant_name": "Infosys Limited",
                "review_scope": "global_expansion_candidate",
            }
        ],
        masterfile_rows=[
            {
                "ticker": "INFY",
                "name": "Infosys Ltd",
                "exchange": "BSE_IN",
                "asset_type": "Stock",
                "listing_status": "active",
                "reference_scope": "exchange_directory",
                "source_key": "bse_india_scrips",
                "source_url": "https://example.com/bse",
                "isin": "INE009A01021",
                "sector": "",
            }
        ],
        listing_rows=[],
    )

    assert supplements[0]["ticker"] == "INFY"
    assert supplements[0]["isin"] == "INE009A01021"
    assert reviews[0].decision == "accept"


def test_build_financialdata_isin_supplements_blocks_existing_isin_and_ticker() -> None:
    supplements, reviews, summary = build_financialdata_isin_supplements(
        financialdata_rows=[
            {
                "financialdata_symbol": "RELIANCE.NS",
                "base_ticker": "RELIANCE",
                "mapped_exchange": "NSE_IN",
                "registrant_name": "Reliance Industries Limited",
                "review_scope": "global_expansion_candidate",
            },
            {
                "financialdata_symbol": "SHEL.L",
                "base_ticker": "SHEL",
                "mapped_exchange": "LSE",
                "registrant_name": "Shell plc",
                "review_scope": "current_exchange_gap",
            },
        ],
        masterfile_rows=[
            {
                "ticker": "RELIANCE",
                "name": "Reliance Industries Limited",
                "exchange": "NSE_IN",
                "asset_type": "Stock",
                "listing_status": "active",
                "reference_scope": "exchange_directory",
                "source_key": "nse",
                "source_url": "https://example.com/nse",
                "isin": "INE002A01018",
                "sector": "",
            },
            {
                "ticker": "SHEL",
                "name": "Shell plc",
                "exchange": "LSE",
                "asset_type": "Stock",
                "listing_status": "active",
                "reference_scope": "exchange_directory",
                "source_key": "lse",
                "source_url": "https://example.com/lse",
                "isin": "GB00BP6MXD84",
                "sector": "",
            },
        ],
            listing_rows=[
                {"ticker": "RELIANCE", "exchange": "LSE", "isin": "", "asset_type": "Stock"},
                {"ticker": "OTHER", "exchange": "NASDAQ", "isin": "GB00BP6MXD84", "asset_type": "Stock"},
            ],
        )

    assert supplements == []
    assert [review.reason for review in reviews] == [
        "isin_already_exists_in_database",
        "ticker_already_exists_globally",
    ]
    assert summary["supplement_rows"] == 0


def test_build_financialdata_isin_supplements_preserves_existing_supplement_after_rebuild() -> None:
    existing_supplement = {
        "ticker": "RELIANCE",
        "name": "Reliance Industries Limited",
        "exchange": "NSE_IN",
        "asset_type": "Stock",
        "sector": "",
        "country": "India",
        "country_code": "IN",
        "isin": "INE002A01018",
        "aliases": "Reliance Industries Limited",
        "source_key": "nse_india_securities_available",
        "source_url": "https://example.com/nse",
        "reference_scope": "exchange_directory",
    }

    supplements, reviews, summary = build_financialdata_isin_supplements(
        financialdata_rows=[
            {
                "financialdata_symbol": "RELIANCE.NS",
                "base_ticker": "RELIANCE",
                "mapped_exchange": "NSE_IN",
                "registrant_name": "Reliance Industries Limited",
                "review_scope": "current_exchange_gap",
            }
        ],
        masterfile_rows=[
            {
                "ticker": "RELIANCE",
                "name": "Reliance Industries Limited",
                "exchange": "NSE_IN",
                "asset_type": "Stock",
                "listing_status": "active",
                "reference_scope": "exchange_directory",
                "source_key": "nse_india_securities_available",
                "source_url": "https://example.com/nse",
                "isin": "INE002A01018",
                "sector": "",
            }
        ],
        listing_rows=[
            {
                "ticker": "RELIANCE",
                "name": "Reliance Industries Limited",
                "exchange": "NSE_IN",
                "asset_type": "Stock",
                "isin": "INE002A01018",
            }
        ],
        existing_supplement_rows=[existing_supplement],
    )

    assert supplements == [existing_supplement]
    assert reviews[0].decision == "preserve"
    assert reviews[0].reason == "already_in_financialdata_supplement"
    assert summary["preserved_supplement_rows"] == 1
    assert summary["supplement_rows"] == 1

from __future__ import annotations

import pytest

from scripts.backfill_stockanalysis_list_sectors import (
    StockAnalysisListRow,
    build_metadata_updates,
    evaluate_row,
    normalize_source_symbol,
    stockanalysis_screener_url,
    write_report_csv,
)


def test_normalize_source_symbol_strips_exchange_prefix() -> None:
    assert normalize_source_symbol("tsxv/ARTG", "tsxv/") == "ARTG"
    assert normalize_source_symbol("SSNLF", "otc/") == "SSNLF"


def test_stockanalysis_screener_url_requests_sector_columns() -> None:
    url = stockanalysis_screener_url("OTC", page=2, count=1000)

    assert "exchangeCode-is-OTC%2Csubtype-is-stock" in url
    assert "c=no%2Cs%2Cn%2Cisin%2Csector%2Cindustry%2CmarketCap" in url
    assert "p=2" in url


def test_evaluate_row_accepts_exact_symbol_name_gated_sector() -> None:
    result = evaluate_row(
        {
            "ticker": "ARTG",
            "exchange": "TSXV",
            "asset_type": "Stock",
            "name": "Artemis Gold Inc.",
            "stock_sector": "",
        },
        StockAnalysisListRow(
            exchange="TSXV",
            asset_type="Stock",
            symbol="ARTG",
            name="Artemis Gold Inc.",
            isin="CA04302L1004",
            sector="Basic Materials",
            industry="Gold",
            category="",
            source_url="https://stockanalysis.com/quote/tsxv/ARTG/",
        ),
    )

    assert result["decision"] == "accept"
    assert result["sector_update"] == "Materials"


def test_evaluate_row_maps_stockanalysis_industry_when_sector_missing() -> None:
    result = evaluate_row(
        {
            "ticker": "GXPLF",
            "exchange": "OTC",
            "asset_type": "Stock",
            "name": "Greenridge Exploration Inc",
            "stock_sector": "",
        },
        StockAnalysisListRow(
            exchange="OTC",
            asset_type="Stock",
            symbol="GXPLF",
            name="Greenridge Exploration Inc.",
            isin="",
            sector="",
            industry="Metal Mining",
            category="",
            source_url="https://stockanalysis.com/quote/otc/GXPLF/",
        ),
    )

    assert result["decision"] == "accept"
    assert result["sector_update"] == "Materials"


def test_evaluate_row_skips_isin_when_reviewed_clear_blocks_backfill() -> None:
    result = evaluate_row(
        {
            "ticker": "HLTFF",
            "exchange": "OTC",
            "asset_type": "Stock",
            "name": "Hilton Food Group plc",
            "isin": "",
            "stock_sector": "Consumer Staples",
        },
        StockAnalysisListRow(
            exchange="OTC",
            asset_type="Stock",
            symbol="HLTFF",
            name="Hilton Food Group plc",
            isin="DE000A161408",
            sector="Consumer Staples",
            industry="Food Products",
            category="",
            source_url="https://stockanalysis.com/quote/otc/HLTFF/",
        ),
        blocked_fields=frozenset({"isin"}),
    )

    assert result["isin_update"] == ""
    assert result["decision"] == "no_update"


def test_evaluate_row_rejects_name_mismatch() -> None:
    result = evaluate_row(
        {
            "ticker": "ARTG",
            "exchange": "TSXV",
            "asset_type": "Stock",
            "name": "Different Issuer Inc.",
            "stock_sector": "",
        },
        StockAnalysisListRow(
            exchange="TSXV",
            asset_type="Stock",
            symbol="ARTG",
            name="Artemis Gold Inc.",
            isin="CA04302L1004",
            sector="Basic Materials",
            industry="Gold",
            category="",
            source_url="https://stockanalysis.com/quote/tsxv/ARTG/",
        ),
    )

    assert result["decision"] == "name_mismatch"
    assert result["sector_update"] == ""


def test_evaluate_row_rejects_generic_legal_suffix_overlap() -> None:
    result = evaluate_row(
        {
            "ticker": "CHKMF",
            "exchange": "OTC",
            "asset_type": "Stock",
            "name": "Cohiba Minerals Limited",
            "isin": "",
            "stock_sector": "",
        },
        StockAnalysisListRow(
            exchange="OTC",
            asset_type="Stock",
            symbol="CHKMF",
            name="Altair Minerals Limited",
            isin="AU0000333676",
            sector="Basic Materials",
            industry="Other Industrial Metals & Mining",
            category="",
            source_url="https://stockanalysis.com/quote/otc/CHKMF/",
        ),
    )

    assert result["decision"] == "name_mismatch"
    assert result["isin_update"] == ""
    assert result["sector_update"] == ""


def test_evaluate_row_rejects_adr_isin_when_listing_country_disagrees() -> None:
    result = evaluate_row(
        {
            "ticker": "VWAGY",
            "exchange": "OTC",
            "asset_type": "Stock",
            "name": "Volkswagen AG ADR",
            "country": "Germany",
            "isin": "",
            "stock_sector": "Consumer Discretionary",
        },
        StockAnalysisListRow(
            exchange="OTC",
            asset_type="Stock",
            symbol="VWAGY",
            name="Volkswagen AG ADR",
            isin="US9286626000",
            sector="Consumer Discretionary",
            industry="Auto Manufacturers",
            category="",
            source_url="https://stockanalysis.com/quote/otc/VWAGY/",
        ),
    )

    assert result["decision"] == "isin_country_mismatch"
    assert result["isin_update"] == ""


def test_evaluate_row_accepts_isin_when_prefix_matches_listing_country() -> None:
    result = evaluate_row(
        {
            "ticker": "VWAGY",
            "exchange": "OTC",
            "asset_type": "Stock",
            "name": "Volkswagen AG ADR",
            "country": "Germany",
            "isin": "",
            "stock_sector": "",
        },
        StockAnalysisListRow(
            exchange="OTC",
            asset_type="Stock",
            symbol="VWAGY",
            name="Volkswagen AG ADR",
            isin="DE0007664039",
            sector="Consumer Discretionary",
            industry="Auto Manufacturers",
            category="",
            source_url="https://stockanalysis.com/quote/otc/VWAGY/",
        ),
    )

    assert result["decision"] == "accept"
    assert result["isin_update"] == "DE0007664039"


@pytest.mark.parametrize(
    ("ticker", "name", "country", "isin"),
    [
        ("ASMVY", "ASMPT Limited", "Hong Kong", "US00212G1085"),
        ("BJGPY", "Beijing Geekplus Technology Co., Ltd./ADR", "China", "US07725A1060"),
        ("CKHUY", "CK Hutchison Holdings Limited ADR", "Hong Kong", "US12562Y1001"),
        ("FXCNY", "FIH Mobile Ltd", "China", "US30255D2099"),
        ("HNHPF", "Hon Hai Precision Industry Co., Ltd.", "Taiwan", "US40415F1012"),
        ("MRUWY", "Maruwa Co., Ltd./ADR", "Japan", "US57381Q1013"),
        ("RAASY", "Cloopen Group Holding Ltd", "Cayman Islands", "US18900M2035"),
        ("VWAGY", "Volkswagen AG ADR", "Germany", "US9286626000"),
    ],
)
def test_evaluate_row_rejects_weekly_backfill_adr_us_isin_mismatches(
    ticker: str,
    name: str,
    country: str,
    isin: str,
) -> None:
    result = evaluate_row(
        {
            "ticker": ticker,
            "exchange": "OTC",
            "asset_type": "Stock",
            "name": name,
            "country": country,
            "isin": "",
            "stock_sector": "Information Technology",
        },
        StockAnalysisListRow(
            exchange="OTC",
            asset_type="Stock",
            symbol=ticker,
            name=name,
            isin=isin,
            sector="Information Technology",
            industry="",
            category="",
            source_url=f"https://stockanalysis.com/quote/otc/{ticker}/",
        ),
    )

    assert result["decision"] == "isin_country_mismatch"
    assert result["isin_update"] == ""
    assert build_metadata_updates([result]) == []


def test_build_metadata_updates_labels_stockanalysis_list_as_secondary() -> None:
    updates = build_metadata_updates(
        [
            {
                "ticker": "ARTG",
                "exchange": "TSXV",
                "decision": "accept",
                "isin_update": "",
                "sector_update": "Materials",
                "category_update": "",
                "source_url": "https://stockanalysis.com/quote/tsxv/ARTG/",
            }
        ]
    )

    assert updates == [
        {
            "ticker": "ARTG",
            "exchange": "TSXV",
            "field": "stock_sector",
            "decision": "update",
            "proposed_value": "Materials",
            "confidence": "0.74",
            "reason": (
                "StockAnalysis exchange list screener supplied sector/industry metadata for a row missing stock_sector; "
                "accepted as reviewed secondary metadata after exact exchange/symbol match and issuer-name gate. "
                "Source: https://stockanalysis.com/quote/tsxv/ARTG/"
            ),
        }
    ]


def test_write_report_csv_uses_lf_line_endings(tmp_path) -> None:
    path = tmp_path / "stockanalysis_list_sector_backfill.csv"

    write_report_csv(
        path,
        [
            {
                "ticker": "ARTG",
                "exchange": "TSXV",
                "asset_type": "Stock",
                "name": "Artemis Gold Inc.",
                "stockanalysis_symbol": "ARTG",
                "stockanalysis_name": "Artemis Gold Inc.",
                "stockanalysis_isin": "CA04302L1004",
                "stockanalysis_sector": "Basic Materials",
                "stockanalysis_industry": "Gold",
                "stockanalysis_category": "",
                "isin_update": "",
                "sector_update": "Materials",
                "category_update": "",
                "decision": "accept",
                "source_url": "https://stockanalysis.com/quote/tsxv/ARTG/",
            }
        ],
    )

    content = path.read_bytes()
    assert b"\r\n" not in content
    assert content.endswith(b"\n")

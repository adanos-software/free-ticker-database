import csv

from scripts.backfill_tradingview_etf_categories import (
    build_metadata_updates,
    category_from_source,
    evaluate_row,
    load_entry_quality_missing_etf_category_rows,
    names_compatible_for_category,
    write_report_csv,
)


def target_row(**overrides):
    values = {
        "ticker": "ACEI",
        "exchange": "NYSE ARCA",
        "asset_type": "ETF",
        "name": "Innovator Equity Autocallable Income Strategy ETF",
        "etf_category": "",
    }
    values.update(overrides)
    return values


def source_row(**overrides):
    values = {
        "name": "ACEI",
        "description": "Innovator Equity Autocallable Income Strategy ETF",
        "exchange": "AMEX",
        "type": "fund",
        "subtype": "etf",
        "isin": "US45784N5932",
        "asset_class": "4071518f1736a5a43dae51b47590322f",
    }
    values.update(overrides)
    return values


def test_evaluate_row_accepts_supported_asset_class():
    result = evaluate_row(target_row(), source_row())
    assert result["decision"] == "accept"
    assert result["category_update"] == "Alternative"


def test_load_entry_quality_missing_etf_category_rows_filters_supported_etf_issues(tmp_path):
    path = tmp_path / "entry_quality.csv"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["ticker", "exchange", "asset_type", "name", "issue_types"],
        )
        writer.writeheader()
        writer.writerows(
            [
                {
                    "ticker": "ACEI",
                    "exchange": "NYSE ARCA",
                    "asset_type": "ETF",
                    "name": "Innovator Equity Autocallable Income Strategy ETF",
                    "issue_types": "expected_missing_primary_isin|missing_etf_category",
                },
                {
                    "ticker": "AAA",
                    "exchange": "NYSE ARCA",
                    "asset_type": "Stock",
                    "name": "Example Inc.",
                    "issue_types": "missing_stock_sector",
                },
                {
                    "ticker": "UNSUP",
                    "exchange": "AMS",
                    "asset_type": "ETF",
                    "name": "Unsupported Exchange ETF",
                    "issue_types": "missing_etf_category",
                },
                {
                    "ticker": "ICCROETF",
                    "exchange": "BVB",
                    "asset_type": "ETF",
                    "name": "Intercapital ETF D.o.o.",
                    "issue_types": "missing_etf_category",
                },
                {
                    "ticker": "DONE",
                    "exchange": "BATS",
                    "asset_type": "ETF",
                    "name": "Already Complete ETF",
                    "issue_types": "",
                },
            ]
        )

    rows = load_entry_quality_missing_etf_category_rows(
        exchanges={"BATS", "BVB", "NYSE ARCA"},
        entry_quality_csv=path,
    )

    assert [(row["ticker"], row["exchange"]) for row in rows] == [("ACEI", "NYSE ARCA"), ("ICCROETF", "BVB")]


def test_evaluate_row_rejects_unsupported_asset_class():
    result = evaluate_row(target_row(), source_row(asset_class="1af0389838508d7016a9841eb6273962"))
    assert result["decision"] == "unsupported_asset_class"


def test_evaluate_row_accepts_asset_allocation_class():
    result = evaluate_row(
        target_row(ticker="GMOD", name="GMO Dynamic Allocation ETF"),
        source_row(
            name="GMOD",
            description="GMO Dynamic Allocation ETF",
            asset_class="b090e99b8d95f5837ec178c2d3d3fc50",
        ),
    )
    assert result["decision"] == "accept"
    assert result["category_update"] == "Alternative"


def test_evaluate_row_rejects_non_etf_source():
    result = evaluate_row(target_row(), source_row(type="stock", subtype="common"))
    assert result["decision"] == "asset_type_mismatch"


def test_evaluate_row_rejects_name_mismatch():
    result = evaluate_row(target_row(), source_row(description="Unrelated ETF"))
    assert result["decision"] == "name_mismatch"


def test_evaluate_row_accepts_generic_wrapper_target_name():
    result = evaluate_row(
        target_row(ticker="DABS", exchange="NYSE ARCA", name="DoubleLine ETF Trust"),
        source_row(
            name="DABS",
            description="Doubleline ABS ETF",
            asset_class="b6e443a6c4a8a2e7918c5dbf3d45c796",
        ),
    )
    assert names_compatible_for_category("DoubleLine ETF Trust", "Doubleline ABS ETF")
    assert result["decision"] == "accept"
    assert result["category_update"] == "Fixed Income"


def test_evaluate_row_accepts_same_isin_name_variant():
    result = evaluate_row(
        target_row(name="ISHARES V PLC ISH IBONDS DEC 2026 TRM $ CRP GBP H", isin="IE000KM34187"),
        source_row(
            name="D26G",
            description="iShares iBonds Dec 2026 Term USD Corp UCITS ETF Hedged GBP",
            isin="IE000KM34187",
            asset_class="b6e443a6c4a8a2e7918c5dbf3d45c796",
        ),
    )
    assert result["decision"] == "accept"
    assert result["category_update"] == "Fixed Income"


def test_evaluate_row_accepts_bvb_same_isin_generic_target_name():
    result = evaluate_row(
        target_row(
            ticker="ICCROETF",
            exchange="BVB",
            name="Intercapital ETF D.o.o.",
            isin="HRICAMFC10C4",
        ),
        source_row(
            name="ICCROETF",
            description="InterCapital CROBEX10tr UCITS ETF",
            exchange="BVB",
            isin="HRICAMFC10C4",
            asset_class="c05f85d35d1cd0be6ebb2af4be16e06a",
        ),
    )

    assert result["decision"] == "accept"
    assert result["category_update"] == "Equity"


def test_names_compatible_for_category_accepts_etf_suffix_and_spacing_noise_only():
    assert names_compatible_for_category(
        "Hamilton EnhancedTechnology DayMAX™ ETF",
        "Hamilton Enhanced Technology DayMAX ETF Trust Units",
    )
    assert not names_compatible_for_category(
        "GMO ETF Trust (Name to be changed from The 2023 ETF Series Trust II)",
        "GMO Ultra-Short Income ETF",
    )
    assert not names_compatible_for_category(
        "Manager Directed Portfolios",
        "Twin Oak Short Horizon Absolute Return ETF",
    )


def test_category_from_source_prefers_real_estate_product_name():
    assert (
        category_from_source(
            source_row(
                description="VanEck Office and Commercial REIT ETF",
                asset_class="c05f85d35d1cd0be6ebb2af4be16e06a",
            )
        )
        == "Real Estate"
    )


def test_build_metadata_updates_emits_category_update():
    updates = build_metadata_updates([evaluate_row(target_row(), source_row())])
    assert updates[0]["field"] == "etf_category"
    assert updates[0]["proposed_value"] == "Alternative"
    assert updates[0]["confidence"] == "0.74"


def test_write_report_csv_uses_lf_line_endings(tmp_path):
    path = tmp_path / "report.csv"
    write_report_csv(path, [evaluate_row(target_row(), source_row())])

    content = path.read_bytes()
    assert b"\r\n" not in content
    assert b"\n" in content

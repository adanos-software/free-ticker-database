from __future__ import annotations

import csv

from scripts.backfill_etf_categories_from_names import (
    build_metadata_updates,
    classify_etf_category,
    evaluate_etf_row,
    load_entry_quality_missing_category_rows,
    load_existing_classifier_update_keys,
    load_source_gap_missing_category_rows,
    load_ticker_rows,
    main,
    prune_stale_classifier_updates,
    verify_etf_categories,
    write_report_csv,
)


def test_load_ticker_rows_reads_csv(tmp_path):
    path = tmp_path / "tickers.csv"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["ticker", "exchange", "asset_type", "name", "sector"])
        writer.writeheader()
        writer.writerow({"ticker": "BND", "exchange": "NYSE ARCA", "asset_type": "ETF", "name": "Example Bond ETF", "sector": ""})

    assert load_ticker_rows(path)[0]["ticker"] == "BND"


def test_load_entry_quality_missing_category_rows_reads_only_etf_category_issues(tmp_path):
    path = tmp_path / "entry_quality.csv"
    fieldnames = ["ticker", "exchange", "asset_type", "name", "issue_types"]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(
            [
                {
                    "ticker": "BND",
                    "exchange": "NYSE ARCA",
                    "asset_type": "ETF",
                    "name": "Example Bond ETF",
                    "issue_types": "expected_missing_primary_isin|missing_etf_category",
                },
                {
                    "ticker": "AAA",
                    "exchange": "NYSE",
                    "asset_type": "Stock",
                    "name": "Example Inc.",
                    "issue_types": "missing_stock_sector",
                },
                {
                    "ticker": "SPY",
                    "exchange": "NYSE ARCA",
                    "asset_type": "ETF",
                    "name": "SPDR S&P 500 ETF Trust",
                    "issue_types": "",
                },
            ]
        )

    rows = load_entry_quality_missing_category_rows(path)

    assert [(row["ticker"], row["exchange"]) for row in rows] == [("BND", "NYSE ARCA")]


def test_load_source_gap_missing_category_rows_reads_only_etf_category_source_gaps(tmp_path):
    path = tmp_path / "source_gap_classification.csv"
    fieldnames = ["field", "ticker", "exchange", "asset_type", "name"]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(
            [
                {
                    "field": "missing_etf_category",
                    "ticker": "ISCHF",
                    "exchange": "OTC",
                    "asset_type": "ETF",
                    "name": "ISHARES ETF CH ISHARES SWISS DOMESTIC GOVT BD 3-7 ETF CH",
                },
                {
                    "field": "official_reference_gap",
                    "ticker": "BFRE",
                    "exchange": "OTC",
                    "asset_type": "ETF",
                    "name": "Ultimus Managers Trust",
                },
                {
                    "field": "missing_sector_stock",
                    "ticker": "AAA",
                    "exchange": "OTC",
                    "asset_type": "Stock",
                    "name": "Example Inc.",
                },
            ]
        )

    rows = load_source_gap_missing_category_rows(path)

    assert rows == [
        {
            "ticker": "ISCHF",
            "exchange": "OTC",
            "asset_type": "ETF",
            "name": "ISHARES ETF CH ISHARES SWISS DOMESTIC GOVT BD 3-7 ETF CH",
            "sector": "",
            "etf_category": "",
        }
    ]


def test_classify_etf_category_uses_specific_order_before_equity_fallback():
    assert classify_etf_category("Example US Corporate Bond ETF") == ("Fixed Income", "corporate_bonds")
    assert classify_etf_category("Example Ethereum & Treasuries Rotation Strategy ETF") == ("Alternative", "digital_assets")
    assert classify_etf_category("Example Bitcoin ETF") == ("Alternative", "digital_assets")
    assert classify_etf_category("21Shares Render ETP") == ("Alternative", "digital_assets")
    assert classify_etf_category("ETFBTCPL") == ("Alternative", "digital_assets")
    assert classify_etf_category("Example S&P 500 Index Fund") == ("Equity", "large_cap")
    assert classify_etf_category("Example Dow Jones Industrial Average ETF") == ("Equity", "large_cap")
    assert classify_etf_category("Example NY Dow Industrial Average ETF") == ("Equity", "large_cap")
    assert classify_etf_category("FI ETF SINGULAR IPSA") == ("Equity", "large_cap")
    assert classify_etf_category("DNB OBX") == ("Equity", "large_cap")
    assert classify_etf_category("Satrix Swix Top 40 ETF") == ("Equity", "large_cap")
    assert classify_etf_category("Seligson & Co OMX Helsinki 25") == ("Equity", "large_cap")
    assert classify_etf_category("STANBIC IBTC ETF 30") == ("Equity", "large_cap")
    assert classify_etf_category("VETIVA GRIFFIN 30 ETF") == ("Equity", "large_cap")
    assert classify_etf_category("Fondul Deschis De Investitii BET Tradeville") == ("Equity", "large_cap")
    assert classify_etf_category("Vanguard Scottsdale Funds - Vanguard Russell 1000 ETF") == (
        "Equity",
        "large_cap",
    )
    assert classify_etf_category("Example Gold Futures ETF") == ("Commodity", "commodities")
    assert classify_etf_category("db Physical Rhodium ETC (EUR)") == ("Commodity", "commodities")
    assert classify_etf_category("Example Equity Index Fund") == ("Equity", "equities")
    assert classify_etf_category("iShares NAFTRAC") == ("Equity", "equities")
    assert classify_etf_category("X SDG 12 CIRCULAR ECONOMY") == ("Equity", "equities")
    assert classify_etf_category("XACT NORDEN") == ("Equity", "equities")
    assert classify_etf_category("KOSEF USA ETF Industry STOXX") == ("Equity", "equities")
    assert classify_etf_category("Example Nikkei 225 Currency-hedged ETF") == ("Equity", "large_cap")
    assert classify_etf_category("Example Currency Basket ETF") == ("Currency", "currencies")
    assert classify_etf_category("ProShares Ultra COIN ETF") == ("Leveraged/Inverse", "leveraged_inverse")
    assert classify_etf_category("Beta ETF WIG20lev Potrfelowy Fundusz Inwestycyjny Zamkniety") == (
        "Leveraged/Inverse",
        "leveraged_inverse",
    )
    assert classify_etf_category("iShares LifePath Target Date 2050 ETF") == ("Multi-Asset", "multi_asset")
    assert classify_etf_category("Example Preferred and Income ETF") == ("Fixed Income", "fixed_income")
    assert classify_etf_category("ISHARES ETF CH ISHARES SWISS DOMESTIC GOVT BD 3-7 ETF CH") == (
        "Fixed Income",
        "fixed_income",
    )
    assert classify_etf_category("iSh Brzil LTN BRL Gvt Bnd") == ("Fixed Income", "fixed_income")
    assert classify_etf_category("Moneda Deuda Latinoamericana fondo de inversion") == (
        "Fixed Income",
        "fixed_income",
    )
    assert classify_etf_category("Peru Soberano Van Eck Eldorado ID ETF") == ("Fixed Income", "fixed_income")
    assert classify_etf_category("DOMINION INCOME TRUST 1") == ("Fixed Income", "fixed_income")
    assert classify_etf_category("SPDR BB SB USCorEH") == ("Fixed Income", "fixed_income")
    assert classify_etf_category("United States Gasoline Fund LP") == ("Commodity", "commodities")
    assert classify_etf_category("ASML Holding NV ADRhedged") == ("Equity", "equities")
    assert classify_etf_category("PBR Improvement over 1x ETF") == ("Equity", "equities")
    assert classify_etf_category("Strategic Shareholding Disposal Promotion ETF") == ("Equity", "equities")
    assert classify_etf_category("Investor-Management Unite as One ETF") == ("Equity", "equities")
    assert classify_etf_category("CADENCE OPPORTUNITIES FUND LIMITED.") == ("Equity", "equities")
    assert classify_etf_category("OPHIR HIGH CONVICTION FUND") == ("Equity", "equities")
    assert classify_etf_category("GREENWICH ASSET ETF") == ("Equity", "equities")
    assert classify_etf_category("SIAML PENSION ETF 40") == ("Equity", "equities")
    assert classify_etf_category("Unusual Whales Subversive Republican Trading ETF") == ("Equity", "equities")
    assert classify_etf_category("Fondo de Inversión Banchile Rentas Inmobiliarias") == (
        "Real Estate",
        "brazil_real_estate_funds",
    )
    assert classify_etf_category("SCENTRE GROUP TRUST 1 AND SCENTRE GROUP TRUST 2") == (
        "Real Estate",
        "brazil_real_estate_funds",
    )
    assert classify_etf_category("VICINITY CENTRES TRUST") == ("Real Estate", "brazil_real_estate_funds")
    assert classify_etf_category("Starlight U.S. Residential Fund (Multi-Family) Investment LP") == (
        "Real Estate",
        "brazil_real_estate_funds",
    )
    assert classify_etf_category("VETIVA BANKING ETF") == ("Equity", "financials")
    assert classify_etf_category("FDI ETF ENERGIE PATRIA TRADEVIL") == ("Equity", "energy")
    assert classify_etf_category("VETIVA CONSUMER GOODS ETF") == ("Equity", "consumer_staples")


def test_classify_etf_category_handles_common_non_english_markers():
    assert classify_etf_category("KODEX 27-12 \ud68c\uc0ac\ucc44(AA-\uc774\uc0c1)\uc561\ud2f0\ube0c") == (
        "Fixed Income",
        "corporate_bonds",
    )
    assert classify_etf_category("TIGER \ubbf8\uad6d\ucd08\ub2e8\uae30(3\uac1c\uc6d4\uc774\ud558)\uad6d\ucc44") == (
        "Fixed Income",
        "treasury_bonds",
    )
    assert classify_etf_category("KODEX \ubbf8\uad6dS&P500\ub370\uc77c\ub9ac\ucee4\ubc84\ub4dc\ucf5cOTM") == (
        "Alternative",
        "alternative",
    )
    assert classify_etf_category("TIGER \uc5d4\ube44\ub514\uc544\ubbf8\uad6d\ucc44\ucee4\ubc84\ub4dc\ucf5c\ubc38\ub7f0\uc2a4") == (
        "Alternative",
        "alternative",
    )
    assert classify_etf_category("\ub85c\ubd07ETF") == ("Equity", "industrials")
    assert classify_etf_category("\uc2e0\ucc3dETF") == ("Equity", "information_technology")
    assert classify_etf_category("\u6807\u666e500ETF") == ("Equity", "large_cap")
    assert classify_etf_category("ETF BRADESCO IBOVESPA FDO DE INDICE") == ("Equity", "equities")
    assert classify_etf_category("Kiwoom KOSEF USD Futures ETF") == ("Currency", "currencies")
    assert classify_etf_category("MiraeAsset TIGER KTB 3-10 ETF") == ("Fixed Income", "treasury_bonds")
    assert classify_etf_category("Fondo De Inversion ETF Singular Chile Corporativo") == (
        "Fixed Income",
        "corporate_bonds_spanish",
    )
    assert classify_etf_category("PEPPER RESIDENTIAL SECURITIES TRUST NO. 40") == (
        "Fixed Income",
        "fixed_income",
    )
    assert classify_etf_category("PEPPER SPARKZ TRUST NO.6") == ("Fixed Income", "fixed_income")
    assert classify_etf_category("Fondo Bursatil Van Eck El Dorado Peru ETF") == ("Equity", "equities")


def test_evaluate_etf_row_accepts_only_missing_etf_category():
    assert evaluate_etf_row(
        {"ticker": "AAA", "exchange": "XETRA", "asset_type": "ETF", "name": "Example Corporate Bond ETF", "sector": ""}
    )["decision"] == "accept"
    assert evaluate_etf_row(
        {"ticker": "AAA", "exchange": "XETRA", "asset_type": "ETF", "name": "Example Corporate Bond ETF", "sector": "Bonds"}
    )["decision"] == "already_has_category"
    assert evaluate_etf_row(
        {"ticker": "AAA", "exchange": "XETRA", "asset_type": "Stock", "name": "Example Corporate Bond ETF", "sector": ""}
    )["decision"] == "not_etf"
    assert evaluate_etf_row(
        {"ticker": "AAA", "exchange": "XETRA", "asset_type": "ETF", "name": "Example Wrapper", "sector": ""}
    )["decision"] == "no_rule_match"


def test_verify_etf_categories_filters_exchange_and_existing_category():
    results = verify_etf_categories(
        [
            {"ticker": "A", "exchange": "XETRA", "asset_type": "ETF", "name": "Example Corporate Bond ETF", "sector": ""},
            {"ticker": "B", "exchange": "LSE", "asset_type": "ETF", "name": "Example Corporate Bond ETF", "sector": ""},
            {"ticker": "C", "exchange": "XETRA", "asset_type": "ETF", "name": "Example Corporate Bond ETF", "sector": "Bonds"},
        ],
        exchanges={"XETRA"},
    )

    assert [result["ticker"] for result in results] == ["A"]
    assert results[0]["category_update"] == "Fixed Income"


def test_verify_etf_categories_refreshes_existing_classifier_updates():
    results = verify_etf_categories(
        [
            {"ticker": "A", "exchange": "XETRA", "asset_type": "ETF", "name": "Example Corporate Bond ETF", "sector": "Corporate Bonds"},
            {"ticker": "B", "exchange": "XETRA", "asset_type": "ETF", "name": "Example Corporate Bond ETF", "sector": "Corporate Bonds"},
        ],
        exchanges={"XETRA"},
        existing_classifier_update_keys={("A", "XETRA")},
    )

    assert [result["ticker"] for result in results] == ["A"]
    assert results[0]["decision"] == "accept"


def test_verify_etf_categories_refreshes_existing_etf_category_updates():
    results = verify_etf_categories(
        [
            {
                "ticker": "A",
                "exchange": "XETRA",
                "asset_type": "ETF",
                "name": "Example Corporate Bond ETF",
                "sector": "",
                "etf_category": "Fixed Income",
            },
            {
                "ticker": "B",
                "exchange": "XETRA",
                "asset_type": "ETF",
                "name": "Example Corporate Bond ETF",
                "sector": "",
                "etf_category": "Fixed Income",
            },
        ],
        exchanges={"XETRA"},
        existing_classifier_update_keys={("A", "XETRA")},
    )

    assert [result["ticker"] for result in results] == ["A"]
    assert results[0]["decision"] == "accept"


def test_build_metadata_updates_emits_reviewed_etf_category_update():
    updates = build_metadata_updates(
        [
            {
                "decision": "accept",
                "ticker": "BND",
                "exchange": "NYSE ARCA",
                "category_update": "Fixed Income",
                "matched_rule": "corporate_bonds",
            },
            {"decision": "no_rule_match", "ticker": "BAD", "exchange": "NYSE ARCA"},
        ]
    )

    assert updates == [
        {
            "ticker": "BND",
            "exchange": "NYSE ARCA",
            "field": "etf_category",
            "decision": "update",
            "proposed_value": "Fixed Income",
            "confidence": "0.68",
            "reason": "Deterministic ETF-name classifier mapped the product name to 'Fixed Income' via rule 'corporate_bonds'. This is an etf_category fill, not a stock-sector assertion.",
        }
    ]


def test_write_report_csv_uses_lf_line_endings(tmp_path):
    path = tmp_path / "name_category_backfill.csv"

    write_report_csv(
        path,
        [
            {
                "ticker": "BND",
                "exchange": "NYSE ARCA",
                "asset_type": "ETF",
                "name": "Example Corporate Bond ETF",
                "category_update": "Fixed Income",
                "matched_rule": "corporate_bonds",
                "decision": "accept",
            }
        ],
    )

    content = path.read_bytes()
    assert b"\r\n" not in content
    assert content.endswith(b"\n")


def test_load_existing_classifier_update_keys_reads_only_classifier_metadata_rows(tmp_path):
    path = tmp_path / "metadata_updates.csv"
    fieldnames = ["ticker", "exchange", "field", "decision", "proposed_value", "confidence", "reason"]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(
            [
                {
                    "ticker": "KEEP",
                    "exchange": "XETRA",
                    "field": "sector",
                    "decision": "update",
                    "proposed_value": "Equity",
                    "confidence": "0.68",
                    "reason": "Deterministic ETF-name classifier mapped the product name to 'Equity' via rule 'large_cap'. This is an etf_category fill, not a stock-sector assertion.",
                },
                {
                    "ticker": "OTHER",
                    "exchange": "XETRA",
                    "field": "sector",
                    "decision": "update",
                    "proposed_value": "Bonds",
                    "confidence": "0.88",
                    "reason": "Sector/category propagated from same-ISIN listing peers.",
                },
            ]
        )

    assert load_existing_classifier_update_keys(path) == {("KEEP", "XETRA")}


def test_load_existing_classifier_update_keys_ignores_blank_reason_rows(tmp_path):
    path = tmp_path / "metadata_updates.csv"
    fieldnames = ["ticker", "exchange", "field", "decision", "proposed_value", "confidence", "reason"]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(
            {
                "ticker": "BLANK",
                "exchange": "XETRA",
                "field": "etf_category",
                "decision": "update",
                "proposed_value": "Equity",
                "confidence": "0.68",
                "reason": None,
            }
        )

    assert load_existing_classifier_update_keys(path) == set()


def test_prune_stale_classifier_updates_removes_legacy_classifier_rows_and_keeps_other_sources(tmp_path):
    path = tmp_path / "metadata_updates.csv"
    fieldnames = ["ticker", "exchange", "field", "decision", "proposed_value", "confidence", "reason"]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(
            [
                {
                    "ticker": "KEEP",
                    "exchange": "XETRA",
                    "field": "sector",
                    "decision": "update",
                    "proposed_value": "Equity",
                    "confidence": "0.68",
                    "reason": "Deterministic ETF-name classifier mapped the product name to 'Equity' via rule 'large_cap'. This is an etf_category fill, not a stock-sector assertion.",
                },
                {
                    "ticker": "DROP",
                    "exchange": "XETRA",
                    "field": "sector",
                    "decision": "update",
                    "proposed_value": "Fixed Income",
                    "confidence": "0.68",
                    "reason": "Deterministic ETF-name classifier mapped the product name to 'Fixed Income' via rule 'fixed_income'. This is an etf_category fill, not a stock-sector assertion.",
                },
                {
                    "ticker": "OTHER",
                    "exchange": "XETRA",
                    "field": "sector",
                    "decision": "update",
                    "proposed_value": "Bonds",
                    "confidence": "0.88",
                    "reason": "Sector/category propagated from same-ISIN listing peers.",
                },
            ]
        )

    prune_stale_classifier_updates(
        path,
        [
            {
                "ticker": "KEEP",
                "exchange": "XETRA",
                "field": "etf_category",
                "decision": "update",
                "proposed_value": "Equity",
                "confidence": "0.68",
                "reason": "new reason",
            }
        ],
    )

    rows = list(csv.DictReader(path.open(newline="", encoding="utf-8")))
    assert [(row["ticker"], row["field"], row["proposed_value"]) for row in rows] == [
        ("OTHER", "sector", "Bonds"),
    ]


def test_prune_stale_classifier_updates_respects_exchange_scope(tmp_path):
    path = tmp_path / "metadata_updates.csv"
    fieldnames = ["ticker", "exchange", "field", "decision", "proposed_value", "confidence", "reason"]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(
            [
                {
                    "ticker": "DROP",
                    "exchange": "XETRA",
                    "field": "etf_category",
                    "decision": "update",
                    "proposed_value": "Equity",
                    "confidence": "0.68",
                    "reason": "Deterministic ETF-name classifier mapped the product name to 'Equity' via rule 'large_cap'. This is an etf_category fill, not a stock-sector assertion.",
                },
                {
                    "ticker": "KEEP",
                    "exchange": "KRX",
                    "field": "etf_category",
                    "decision": "update",
                    "proposed_value": "Equity",
                    "confidence": "0.68",
                    "reason": "Deterministic ETF-name classifier mapped the product name to 'Equity' via rule 'large_cap'. This is an etf_category fill, not a stock-sector assertion.",
                },
            ]
        )

    prune_stale_classifier_updates(path, [], exchanges={"XETRA"})

    rows = list(csv.DictReader(path.open(newline="", encoding="utf-8")))
    assert [(row["ticker"], row["exchange"]) for row in rows] == [("KEEP", "KRX")]


def test_main_source_gap_mode_does_not_prune_other_classifier_updates(tmp_path):
    source_gap_csv = tmp_path / "source_gap_classification.csv"
    with source_gap_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["field", "ticker", "exchange", "asset_type", "name"])
        writer.writeheader()
        writer.writerow(
            {
                "field": "missing_etf_category",
                "ticker": "ISCHF",
                "exchange": "OTC",
                "asset_type": "ETF",
                "name": "ISHARES ETF CH ISHARES SWISS DOMESTIC GOVT BD 3-7 ETF CH",
            }
        )
    metadata_updates_csv = tmp_path / "metadata_updates.csv"
    fieldnames = ["ticker", "exchange", "field", "decision", "proposed_value", "confidence", "reason"]
    with metadata_updates_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(
            {
                "ticker": "KEEP",
                "exchange": "OTC",
                "field": "etf_category",
                "decision": "update",
                "proposed_value": "Equity",
                "confidence": "0.68",
                "reason": "Deterministic ETF-name classifier mapped the product name to 'Equity' via rule 'equities'. This is an etf_category fill, not a stock-sector assertion.",
            }
        )

    main(
        [
            "--source-gap-classification-csv",
            str(source_gap_csv),
            "--metadata-updates-csv",
            str(metadata_updates_csv),
            "--json-out",
            str(tmp_path / "report.json"),
            "--csv-out",
            str(tmp_path / "report.csv"),
            "--apply",
        ]
    )

    rows = list(csv.DictReader(metadata_updates_csv.open(newline="", encoding="utf-8")))
    assert {(row["ticker"], row["exchange"], row["proposed_value"]) for row in rows} == {
        ("ISCHF", "OTC", "Fixed Income"),
        ("KEEP", "OTC", "Equity"),
    }

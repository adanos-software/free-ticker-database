from __future__ import annotations

from scripts.build_masterfile_supplements import build_supplement_rows
from scripts.rebuild_dataset import merge_supplemental_ticker_rows


def test_build_supplement_rows_allows_collision_free_cse_ma_official_directory():
    core_rows = [{"ticker": "IAM", "exchange": "CSE_MA", "name": "Ittissalat Al-Maghrib"}]
    masterfile_rows = [
        {
            "ticker": "NEWCO",
            "name": "New Moroccan Issuer",
            "exchange": "CSE_MA",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "source_key": "cse_ma_listed_companies",
            "source_url": "https://example.com/cse",
        },
        {
            "ticker": "IAM",
            "name": "Ittissalat Al-Maghrib",
            "exchange": "NASDAQ",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "source_key": "nasdaq_listed",
            "source_url": "https://example.com/nasdaq",
        },
    ]

    rows, summary = build_supplement_rows(core_rows, masterfile_rows)

    assert [row["ticker"] for row in rows] == ["NEWCO"]
    assert rows[0]["country_code"] == "MA"
    assert summary["colliding_rows_skipped"] == 0


def test_build_supplement_rows_keeps_only_safe_tse_rows():
    core_rows = [
        {"ticker": "1301", "exchange": "TWSE", "name": "Formosa Plastics Corporation"},
        {"ticker": "130A", "exchange": "TSE", "name": "Veritas In Silico Inc."},
    ]
    masterfile_rows = [
        {
            "ticker": "1301",
            "name": "KYOKUYO CO.,LTD.",
            "exchange": "TSE",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "source_key": "jpx",
            "source_url": "https://example.com/jpx",
        },
        {
            "ticker": "130A",
            "name": "Veritas In Silico Inc.",
            "exchange": "TSE",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "source_key": "jpx",
            "source_url": "https://example.com/jpx",
        },
        {
            "ticker": "1306",
            "name": "NEXT FUNDS TOPIX Exchange Traded Fund",
            "exchange": "TSE",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "source_key": "jpx",
            "source_url": "https://example.com/jpx",
        },
        {
            "ticker": "25935",
            "name": "Class-A Preferred Stock of ITO EN,LTD.",
            "exchange": "TSE",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "source_key": "jpx",
            "source_url": "https://example.com/jpx",
        },
        {
            "ticker": "5243",
            "name": "note inc.",
            "exchange": "TSE",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "source_key": "jpx",
            "source_url": "https://example.com/jpx",
        },
    ]

    rows, summary = build_supplement_rows(core_rows, masterfile_rows)

    assert rows == [
        {
            "ticker": "1306",
            "name": "NEXT FUNDS TOPIX Exchange Traded Fund",
            "exchange": "TSE",
            "asset_type": "ETF",
            "sector": "",
            "country": "Japan",
            "country_code": "JP",
            "isin": "",
            "aliases": "",
            "source_key": "jpx",
            "source_url": "https://example.com/jpx",
            "reference_scope": "exchange_directory",
        },
        {
            "ticker": "130A",
            "name": "Veritas In Silico Inc.",
            "exchange": "TSE",
            "asset_type": "Stock",
            "sector": "",
            "country": "Japan",
            "country_code": "JP",
            "isin": "",
            "aliases": "",
            "source_key": "jpx",
            "source_url": "https://example.com/jpx",
            "reference_scope": "exchange_directory",
        },
        {
            "ticker": "5243",
            "name": "note inc.",
            "exchange": "TSE",
            "asset_type": "Stock",
            "sector": "",
            "country": "Japan",
            "country_code": "JP",
            "isin": "",
            "aliases": "",
            "source_key": "jpx",
            "source_url": "https://example.com/jpx",
            "reference_scope": "exchange_directory",
        },
    ]
    assert summary["supplement_rows"] == 3
    assert summary["safe_missing_rows"] == 2
    assert summary["refreshable_existing_rows"] == 1
    assert summary["colliding_rows_skipped"] == 2


def test_build_supplement_rows_supports_safe_asx_ams_and_osl_rows():
    core_rows = [
        {"ticker": "ADYEN", "exchange": "AMS", "name": "Adyen N.V."},
        {"ticker": "EQNR", "exchange": "NYSE", "name": "Equinor ASA"},
    ]
    masterfile_rows = [
        {
            "ticker": "49M",
            "name": "49 METALS LIMITED",
            "exchange": "ASX",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "source_key": "asx",
            "source_url": "https://example.com/asx",
        },
        {
            "ticker": "AC2",
            "name": "ALLIED CREDIT ABS TRUST 2025-1P",
            "exchange": "ASX",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "source_key": "asx",
            "source_url": "https://example.com/asx",
        },
        {
            "ticker": "ASML",
            "name": "ASML HOLDING",
            "exchange": "AMS",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "source_key": "euronext",
            "source_url": "https://example.com/ams",
        },
        {
            "ticker": "AZRNW",
            "name": "AZERION WARRANTS",
            "exchange": "AMS",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "source_key": "euronext",
            "source_url": "https://example.com/ams",
        },
        {
            "ticker": "EQNR",
            "name": "EQUINOR",
            "exchange": "OSL",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "source_key": "euronext",
            "source_url": "https://example.com/osl",
        },
    ]

    rows, summary = build_supplement_rows(core_rows, masterfile_rows)

    assert rows == [
        {
            "ticker": "ASML",
            "name": "ASML HOLDING",
            "exchange": "AMS",
            "asset_type": "Stock",
            "sector": "",
            "country": "Netherlands",
            "country_code": "NL",
            "isin": "",
            "aliases": "",
            "source_key": "euronext",
            "source_url": "https://example.com/ams",
            "reference_scope": "exchange_directory",
        },
        {
            "ticker": "49M",
            "name": "49 METALS LIMITED",
            "exchange": "ASX",
            "asset_type": "Stock",
            "sector": "",
            "country": "Australia",
            "country_code": "AU",
            "isin": "",
            "aliases": "",
            "source_key": "asx",
            "source_url": "https://example.com/asx",
            "reference_scope": "exchange_directory",
        },
    ]
    assert summary["supplement_rows"] == 2
    assert summary["safe_missing_rows"] == 2
    assert summary["refreshable_existing_rows"] == 0
    assert summary["colliding_rows_skipped"] == 3
    assert summary["by_exchange"] == {
        "AMS": {
            "safe_missing_rows": 1,
            "refreshable_existing_rows": 0,
            "colliding_rows_skipped": 1,
        },
        "ASX": {
            "safe_missing_rows": 1,
            "refreshable_existing_rows": 0,
            "colliding_rows_skipped": 1,
        },
        "OSL": {
            "safe_missing_rows": 0,
            "refreshable_existing_rows": 0,
            "colliding_rows_skipped": 1,
        },
    }


def test_build_supplement_rows_supports_safe_b3_and_xetra_rows():
    core_rows = [
        {"ticker": "P911", "exchange": "LSE", "name": "Dr. Ing. h.c. F. Porsche AG"},
        {"ticker": "DTE", "exchange": "NYSE", "name": "Deutsche Telekom AG"},
    ]
    masterfile_rows = [
        {
            "ticker": "PETR4",
            "name": "PETROLEO BRASILEIRO S.A. PETROBRAS",
            "exchange": "B3",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "source_key": "b3",
            "source_url": "https://example.com/b3",
        },
        {
            "ticker": "P911",
            "name": "DR ING HC F PORSCHE AG",
            "exchange": "XETRA",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "source_key": "fwb",
            "source_url": "https://example.com/fwb",
        },
        {
            "ticker": "DTE",
            "name": "Deutsche Telekom AG",
            "exchange": "XETRA",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "source_key": "fwb",
            "source_url": "https://example.com/fwb",
        },
    ]

    rows, summary = build_supplement_rows(core_rows, masterfile_rows)

    assert rows == [
        {
            "ticker": "PETR4",
            "name": "PETROLEO BRASILEIRO S.A. PETROBRAS",
            "exchange": "B3",
            "asset_type": "Stock",
            "sector": "",
            "country": "Brazil",
            "country_code": "BR",
            "isin": "",
            "aliases": "",
            "source_key": "b3",
            "source_url": "https://example.com/b3",
            "reference_scope": "exchange_directory",
        }
    ]
    assert summary["by_exchange"] == {
        "B3": {
            "safe_missing_rows": 1,
            "refreshable_existing_rows": 0,
            "colliding_rows_skipped": 0,
        },
        "XETRA": {
            "safe_missing_rows": 0,
            "refreshable_existing_rows": 0,
            "colliding_rows_skipped": 2,
        },
    }


def test_build_supplement_rows_skips_incompatible_same_exchange_refresh():
    core_rows = [
        {"ticker": "SVE", "exchange": "XETRA", "name": "SHAREHOLD.VAL.BET.NA O.N."},
    ]
    masterfile_rows = [
        {
            "ticker": "SVE",
            "name": "Silver One Resources Inc.",
            "exchange": "XETRA",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "source_key": "fwb",
            "source_url": "https://example.com/fwb",
        },
    ]

    rows, summary = build_supplement_rows(core_rows, masterfile_rows)

    assert rows == []
    assert summary["supplement_rows"] == 0
    assert summary["safe_missing_rows"] == 0
    assert summary["refreshable_existing_rows"] == 0
    assert summary["colliding_rows_skipped"] == 1
    assert summary["by_exchange"] == {
        "XETRA": {
            "safe_missing_rows": 0,
            "refreshable_existing_rows": 0,
            "colliding_rows_skipped": 1,
        }
    }


def test_build_supplement_rows_supports_safe_twse_rows():
    core_rows = [
        {"ticker": "1101", "exchange": "SSE", "name": "Collision Co"},
        {"ticker": "2330", "exchange": "TWSE", "name": "Taiwan Semiconductor Manufacturing Company Limited"},
    ]
    masterfile_rows = [
        {
            "ticker": "1101",
            "name": "台泥",
            "exchange": "TWSE",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "source_key": "twse",
            "source_url": "https://example.com/twse",
        },
        {
            "ticker": "00631L",
            "name": "元大台灣50正2",
            "exchange": "TWSE",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "source_key": "twse",
            "source_url": "https://example.com/twse",
        },
        {
            "ticker": "2330",
            "name": "台積電",
            "exchange": "TWSE",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "source_key": "twse",
            "source_url": "https://example.com/twse",
        },
    ]

    rows, summary = build_supplement_rows(core_rows, masterfile_rows)

    assert rows == [
        {
            "ticker": "00631L",
            "name": "元大台灣50正2",
            "exchange": "TWSE",
            "asset_type": "ETF",
            "sector": "",
            "country": "Taiwan",
            "country_code": "TW",
            "isin": "",
            "aliases": "",
            "source_key": "twse",
            "source_url": "https://example.com/twse",
            "reference_scope": "exchange_directory",
        },
        {
            "ticker": "2330",
            "name": "台積電",
            "exchange": "TWSE",
            "asset_type": "Stock",
            "sector": "",
            "country": "Taiwan",
            "country_code": "TW",
            "isin": "",
            "aliases": "",
            "source_key": "twse",
            "source_url": "https://example.com/twse",
            "reference_scope": "exchange_directory",
        },
    ]
    assert summary["supplement_rows"] == 2
    assert summary["safe_missing_rows"] == 1
    assert summary["refreshable_existing_rows"] == 1
    assert summary["colliding_rows_skipped"] == 1
    assert summary["by_exchange"] == {
        "TWSE": {
            "safe_missing_rows": 1,
            "refreshable_existing_rows": 1,
            "colliding_rows_skipped": 1,
        }
    }


def test_build_supplement_rows_skips_ambiguous_missing_numeric_tickers():
    core_rows: list[dict[str, str]] = []
    masterfile_rows = [
        {
            "ticker": "1626",
            "name": "NISSIN SUGAR CO., LTD.",
            "exchange": "TSE",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "source_key": "jpx",
            "source_url": "https://example.com/jpx",
        },
        {
            "ticker": "1626",
            "name": "艾美特-KY",
            "exchange": "TWSE",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "source_key": "twse",
            "source_url": "https://example.com/twse",
        },
    ]

    rows, summary = build_supplement_rows(core_rows, masterfile_rows)

    assert rows == []
    assert summary["supplement_rows"] == 0
    assert summary["safe_missing_rows"] == 0
    assert summary["refreshable_existing_rows"] == 0
    assert summary["colliding_rows_skipped"] == 2
    assert summary["by_exchange"] == {
        "TSE": {
            "safe_missing_rows": 0,
            "refreshable_existing_rows": 0,
            "colliding_rows_skipped": 1,
        },
        "TWSE": {
            "safe_missing_rows": 0,
            "refreshable_existing_rows": 0,
            "colliding_rows_skipped": 1,
        },
    }


def test_merge_supplemental_ticker_rows_refreshes_safe_fields(monkeypatch, tmp_path):
    supplemental = tmp_path / "supplemental.csv"
    financialdata_supplemental = tmp_path / "financialdata_supplemental.csv"
    supplemental.write_text(
        "\n".join(
            [
                "ticker,name,exchange,asset_type,sector,country,country_code,isin,aliases,source_key,source_url,reference_scope",
                "130A,Veritas In Silico Inc.,TSE,Stock,,Japan,JP,,,jpx,https://example.com,exchange_directory",
                "1306,NEXT FUNDS TOPIX Exchange Traded Fund,TSE,ETF,,Japan,JP,,,jpx,https://example.com,exchange_directory",
            ]
        ),
        encoding="utf-8",
    )
    financialdata_supplemental.write_text(
        "\n".join(
            [
                "ticker,name,exchange,asset_type,sector,country,country_code,isin,aliases,source_key,source_url,reference_scope",
                "RELIANCE,Reliance Industries Limited,NSE_IN,Stock,,India,IN,INE002A01018,Reliance Industries Limited,nse,https://example.com/nse,exchange_directory",
            ]
        ),
        encoding="utf-8",
    )

    from scripts import rebuild_dataset

    monkeypatch.setattr(rebuild_dataset, "MASTERFILE_SUPPLEMENT_CSV", supplemental)
    monkeypatch.setattr(rebuild_dataset, "FINANCIALDATA_ISIN_SUPPLEMENT_CSV", financialdata_supplemental)
    base_rows = [
        {
            "ticker": "130A",
            "name": "Old Name",
            "exchange": "TSE",
            "asset_type": "Stock",
            "sector": "",
            "country": "",
            "country_code": "",
            "isin": "JP0000000001",
            "aliases": "legacy",
        }
    ]

    merged = sorted(
        merge_supplemental_ticker_rows(base_rows),
        key=lambda row: (row["exchange"], row["ticker"]),
    )

    assert merged == [
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
        },
        {
            "ticker": "1306",
            "name": "NEXT FUNDS TOPIX Exchange Traded Fund",
            "exchange": "TSE",
            "asset_type": "ETF",
            "sector": "",
            "country": "Japan",
            "country_code": "JP",
            "isin": "",
            "aliases": "",
        },
        {
            "ticker": "130A",
            "name": "Old Name",
            "exchange": "TSE",
            "asset_type": "Stock",
            "sector": "",
            "country": "Japan",
            "country_code": "JP",
            "isin": "JP0000000001",
            "aliases": "legacy",
        },
    ]


def test_merge_supplemental_ticker_rows_prefers_official_tmx_series_ticker(monkeypatch, tmp_path):
    supplemental = tmp_path / "supplemental.csv"
    supplemental.write_text(
        "\n".join(
            [
                "ticker,name,exchange,asset_type,sector,country,country_code,isin,aliases,source_key,source_url,reference_scope",
                "ACAP.P,Atlas One Capital Corporation,TSXV,Stock,,Canada,CA,,,tmx,https://example.com,listed_companies_subset",
            ]
        ),
        encoding="utf-8",
    )

    from scripts import rebuild_dataset

    monkeypatch.setattr(rebuild_dataset, "MASTERFILE_SUPPLEMENT_CSV", supplemental)
    monkeypatch.setattr(rebuild_dataset, "FINANCIALDATA_ISIN_SUPPLEMENT_CSV", tmp_path / "missing_financialdata.csv")
    base_rows = [
        {
            "ticker": "ACAP-P",
            "name": "Atlas One Capital Corporation",
            "exchange": "TSXV",
            "asset_type": "Stock",
            "sector": "",
            "country": "",
            "country_code": "",
            "isin": "CA0000000001",
            "aliases": "atlas one capital",
        },
        {
            "ticker": "ACAP.P",
            "name": "Atlas One Capital Corporation",
            "exchange": "TSXV",
            "asset_type": "Stock",
            "sector": "",
            "country": "Canada",
            "country_code": "CA",
            "isin": "",
            "aliases": "",
        },
    ]

    assert merge_supplemental_ticker_rows(base_rows) == [
        {
            "ticker": "ACAP.P",
            "name": "Atlas One Capital Corporation",
            "exchange": "TSXV",
            "asset_type": "Stock",
            "sector": "",
            "country": "Canada",
            "country_code": "CA",
            "isin": "CA0000000001",
            "aliases": "atlas one capital",
        }
    ]


def test_build_supplement_rows_allows_tsx_replacement_for_dropped_wrong_venue_row():
    core_rows = [
        {"ticker": "ESGA", "exchange": "NYSE ARCA", "name": "American Century Sustainable Equity ETF"},
    ]
    masterfile_rows = [
        {
            "ticker": "ESGA",
            "name": "BMO MSCI Canada Selection Equity Index ETF",
            "exchange": "TSX",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "listed_companies_subset",
            "source_key": "tmx_listed_issuers",
            "source_url": "https://example.com/tmx",
        }
    ]

    rows, summary = build_supplement_rows(
        core_rows,
        masterfile_rows,
        dropped_keys={("ESGA", "NYSE ARCA")},
    )

    assert rows == [
        {
            "ticker": "ESGA",
            "name": "BMO MSCI Canada Selection Equity Index ETF",
            "exchange": "TSX",
            "asset_type": "ETF",
            "sector": "",
            "country": "Canada",
            "country_code": "CA",
            "isin": "",
            "aliases": "",
            "source_key": "tmx_listed_issuers",
            "source_url": "https://example.com/tmx",
            "reference_scope": "listed_companies_subset",
        }
    ]
    assert summary["supplement_rows"] == 1
    assert summary["safe_missing_rows"] == 1
    assert summary["refreshable_existing_rows"] == 0
    assert summary["colliding_rows_skipped"] == 0


def test_build_supplement_rows_refreshes_existing_fsx_without_universe_expansion():
    core_rows = [
        {"ticker": "APC", "exchange": "FSX", "name": "APPLE INC.", "isin": "US0378331005"},
    ]
    masterfile_rows = [
        {
            "ticker": "APC",
            "name": "APPLE INC.",
            "exchange": "FSX",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "source_key": "deutsche_boerse_frankfurt_all_tradable_equities",
            "source_url": "https://example.com/xfra.csv",
            "isin": "US0378331005",
        },
        {
            "ticker": "000",
            "name": "BITZERO HOLDINGS  O.N.",
            "exchange": "FSX",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "source_key": "deutsche_boerse_frankfurt_all_tradable_equities",
            "source_url": "https://example.com/xfra.csv",
            "isin": "CA09175N1096",
        },
    ]

    rows, summary = build_supplement_rows(core_rows, masterfile_rows)

    assert [row["ticker"] for row in rows] == ["APC"]
    assert summary["refreshable_existing_rows"] == 1
    assert summary["safe_missing_rows"] == 0
    assert summary["refresh_only_missing_rows_skipped"] == 1
    assert summary["by_exchange"]["FSX"] == {
        "safe_missing_rows": 0,
        "refreshable_existing_rows": 1,
        "colliding_rows_skipped": 0,
        "refresh_only_missing_rows_skipped": 1,
    }


def test_build_supplement_rows_adds_same_isin_cross_listings():
    core_rows: list[dict[str, str]] = []
    masterfile_rows = [
        {
            "ticker": "PFIZER",
            "name": "Pfizer Limited",
            "exchange": "NSE_IN",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "source_key": "nse",
            "source_url": "https://example.com/nse",
            "isin": "INE182A01018",
        },
        {
            "ticker": "PFIZER",
            "name": "Pfizer Ltd",
            "exchange": "BSE_IN",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "source_key": "bse",
            "source_url": "https://example.com/bse",
            "isin": "INE182A01018",
        },
    ]

    rows, summary = build_supplement_rows(core_rows, masterfile_rows)

    assert {(row["exchange"], row["ticker"], row["isin"]) for row in rows} == {
        ("NSE_IN", "PFIZER", "INE182A01018"),
        ("BSE_IN", "PFIZER", "INE182A01018"),
    }
    assert summary["safe_missing_rows"] == 2
    assert summary["colliding_rows_skipped"] == 0
    assert summary["coverage_expansion_missing_rows"] == 0


def test_build_supplement_rows_expands_conflicting_isin_ticker_homonyms():
    core_rows: list[dict[str, str]] = []
    masterfile_rows = [
        {
            "ticker": "4190",
            "name": "JARIR",
            "exchange": "TADAWUL",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "source_key": "tadawul",
            "source_url": "https://example.com/tadawul",
            "isin": "SA000A0BLA62",
        },
        {
            "ticker": "4190",
            "name": "Other Co",
            "exchange": "TWSE",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "source_key": "twse",
            "source_url": "https://example.com/twse",
            "isin": "TW0004190001",
        },
    ]

    rows, summary = build_supplement_rows(core_rows, masterfile_rows)

    assert rows == []
    assert summary["safe_missing_rows"] == 0
    assert summary["coverage_expansion_missing_rows"] == 2
    assert {row["listing_key"] for row in summary["coverage_expansion_rows"]} == {
        "TADAWUL::4190",
        "TWSE::4190",
    }


def test_build_supplement_rows_skips_ticker_homonyms_with_mixed_isins():
    core_rows = [
        {
            "ticker": "MSFT",
            "exchange": "NASDAQ",
            "name": "Microsoft Corporation",
            "isin": "US5949181045",
        },
        {
            "ticker": "MSFT",
            "exchange": "LSE",
            "name": "LS 1x Microsoft Tracker ETP Securities",
            "isin": "XS2337100320",
        },
    ]
    masterfile_rows = [
        {
            "ticker": "MSFT",
            "name": "LS 1x Microsoft Tracker ETP",
            "exchange": "AMS",
            "asset_type": "ETF",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "source_key": "euronext",
            "source_url": "https://example.com/ams",
            "isin": "XS2337100320",
        }
    ]

    rows, summary = build_supplement_rows(core_rows, masterfile_rows)

    assert rows == []
    assert summary["safe_missing_rows"] == 0
    assert summary["colliding_rows_skipped"] == 1


def test_build_supplement_rows_cross_lists_when_core_shares_isin():
    core_rows = [
        {
            "ticker": "EQNR",
            "exchange": "NYSE",
            "name": "Equinor ASA",
            "isin": "NO0010096985",
        }
    ]
    masterfile_rows = [
        {
            "ticker": "EQNR",
            "name": "EQUINOR",
            "exchange": "OSL",
            "asset_type": "Stock",
            "listing_status": "active",
            "reference_scope": "exchange_directory",
            "source_key": "osl",
            "source_url": "https://example.com/osl",
            "isin": "NO0010096985",
        }
    ]

    rows, summary = build_supplement_rows(core_rows, masterfile_rows)

    assert len(rows) == 1
    assert rows[0]["exchange"] == "OSL"
    assert rows[0]["isin"] == "NO0010096985"
    assert summary["safe_missing_rows"] == 1
    assert summary["colliding_rows_skipped"] == 0

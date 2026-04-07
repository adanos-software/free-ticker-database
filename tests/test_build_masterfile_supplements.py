from __future__ import annotations

from scripts.build_masterfile_supplements import build_supplement_rows
from scripts.rebuild_dataset import merge_supplemental_ticker_rows


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

    from scripts import rebuild_dataset

    monkeypatch.setattr(rebuild_dataset, "MASTERFILE_SUPPLEMENT_CSV", supplemental)
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
            "name": "Veritas In Silico Inc.",
            "exchange": "TSE",
            "asset_type": "Stock",
            "sector": "",
            "country": "Japan",
            "country_code": "JP",
            "isin": "JP0000000001",
            "aliases": "legacy",
        },
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

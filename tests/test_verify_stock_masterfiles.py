from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from scripts.verify_stock_masterfiles import (
    chunk_stem,
    classify_row,
    is_code_like_reference_name,
    load_asset_rows,
    load_stock_rows,
    select_chunk,
    summarize_results,
)


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def test_load_stock_rows_filters_and_sorts(tmp_path: Path) -> None:
    path = tmp_path / "tickers.csv"
    write_csv(
        path,
        ["ticker", "exchange", "asset_type", "name", "country", "country_code", "isin", "sector"],
        [
            {"ticker": "B", "exchange": "NYSE", "asset_type": "Stock", "name": "B", "country": "United States", "country_code": "US", "isin": "", "sector": ""},
            {"ticker": "A", "exchange": "NASDAQ", "asset_type": "ETF", "name": "A", "country": "United States", "country_code": "US", "isin": "", "sector": ""},
            {"ticker": "A", "exchange": "NYSE", "asset_type": "Stock", "name": "A", "country": "United States", "country_code": "US", "isin": "", "sector": ""},
        ],
    )
    rows = load_stock_rows(path)
    assert [(row["exchange"], row["ticker"]) for row in rows] == [("NYSE", "A"), ("NYSE", "B")]


def test_load_asset_rows_filters_requested_asset_type(tmp_path: Path) -> None:
    path = tmp_path / "tickers.csv"
    write_csv(
        path,
        ["ticker", "exchange", "asset_type", "name", "country", "country_code", "isin", "sector"],
        [
            {"ticker": "SPY", "exchange": "NYSE ARCA", "asset_type": "ETF", "name": "SPY", "country": "United States", "country_code": "US", "isin": "", "sector": ""},
            {"ticker": "QQQ", "exchange": "NASDAQ", "asset_type": "ETF", "name": "QQQ", "country": "United States", "country_code": "US", "isin": "", "sector": ""},
            {"ticker": "AAPL", "exchange": "NASDAQ", "asset_type": "Stock", "name": "Apple", "country": "United States", "country_code": "US", "isin": "", "sector": ""},
        ],
    )
    rows = load_asset_rows(path, asset_type="ETF")
    assert [(row["exchange"], row["ticker"]) for row in rows] == [("NASDAQ", "QQQ"), ("NYSE ARCA", "SPY")]


def test_select_chunk_is_complete_and_disjoint() -> None:
    rows = [{"ticker": str(index), "exchange": "X"} for index in range(10)]
    first = select_chunk(rows, chunk_index=1, chunk_count=3)
    second = select_chunk(rows, chunk_index=2, chunk_count=3)
    third = select_chunk(rows, chunk_index=3, chunk_count=3)
    assert {row["ticker"] for row in first}.isdisjoint({row["ticker"] for row in second})
    assert {row["ticker"] for row in first}.isdisjoint({row["ticker"] for row in third})
    assert sorted(int(row["ticker"]) for row in [*first, *second, *third]) == list(range(10))


def test_select_chunk_validates_bounds() -> None:
    with pytest.raises(ValueError):
        select_chunk([], chunk_index=0, chunk_count=1)
    with pytest.raises(ValueError):
        select_chunk([], chunk_index=2, chunk_count=1)


def test_chunk_stem_is_stable() -> None:
    assert chunk_stem(3, 10) == "chunk-03-of-10"


def test_classify_row_verified() -> None:
    row = {
        "ticker": "AALB",
        "exchange": "AMS",
        "asset_type": "Stock",
        "name": "Aalberts NV",
        "country": "Netherlands",
        "country_code": "NL",
        "isin": "",
        "sector": "",
    }
    result = classify_row(
        row,
        active_by_key={("AMS", "AALB"): [{"ticker": "AALB", "exchange": "AMS", "name": "AALBERTS NV", "asset_type": "Stock", "source_key": "euronext_equities"}]},
        any_by_key={},
        active_by_ticker={"AALB": [{"ticker": "AALB", "exchange": "AMS", "name": "AALBERTS NV", "asset_type": "Stock"}]},
        covered_exchanges={"AMS"},
        partial_covered_exchanges=set(),
        identifier_map={},
    )
    assert result["status"] == "verified"


def test_classify_row_name_mismatch() -> None:
    row = {
        "ticker": "AALB",
        "exchange": "AMS",
        "asset_type": "Stock",
        "name": "Wrong Co",
        "country": "Netherlands",
        "country_code": "NL",
        "isin": "",
        "sector": "",
    }
    result = classify_row(
        row,
        active_by_key={("AMS", "AALB"): [{"ticker": "AALB", "exchange": "AMS", "name": "AALBERTS NV", "asset_type": "Stock", "source_key": "euronext_equities"}]},
        any_by_key={},
        active_by_ticker={"AALB": [{"ticker": "AALB", "exchange": "AMS", "name": "AALBERTS NV", "asset_type": "Stock"}]},
        covered_exchanges={"AMS"},
        partial_covered_exchanges=set(),
        identifier_map={},
    )
    assert result["status"] == "name_mismatch"


def test_classify_row_treats_twse_local_language_names_as_verified() -> None:
    row = {
        "ticker": "2330",
        "exchange": "TWSE",
        "asset_type": "Stock",
        "name": "Taiwan Semiconductor Manufacturing Company Limited",
        "country": "Taiwan",
        "country_code": "TW",
        "isin": "TW0002330008",
        "sector": "Information Technology",
    }
    result = classify_row(
        row,
        active_by_key={
            ("TWSE", "2330"): [
                {
                    "ticker": "2330",
                    "exchange": "TWSE",
                    "name": "台灣積體電路製造股份有限公司",
                    "asset_type": "Stock",
                    "source_key": "twse_listed_companies",
                    "listing_status": "active",
                }
            ]
        },
        any_by_key={},
        active_by_ticker={"2330": [{"ticker": "2330", "exchange": "TWSE", "name": "台灣積體電路製造股份有限公司", "asset_type": "Stock"}]},
        covered_exchanges={"TWSE"},
        partial_covered_exchanges=set(),
        identifier_map={},
    )
    assert result["status"] == "verified"
    assert result["reason"] == "Matched active official listing with a local-language issuer name."


def test_classify_row_treats_szse_local_language_names_as_verified() -> None:
    row = {
        "ticker": "000001",
        "exchange": "SZSE",
        "asset_type": "Stock",
        "name": "Ping An Bank Co Ltd",
        "country": "China",
        "country_code": "CN",
        "isin": "",
        "sector": "Financials",
    }
    result = classify_row(
        row,
        active_by_key={
            ("SZSE", "000001"): [
                {
                    "ticker": "000001",
                    "exchange": "SZSE",
                    "name": "平安银行",
                    "asset_type": "Stock",
                    "source_key": "szse_a_share_list",
                    "listing_status": "active",
                }
            ]
        },
        any_by_key={},
        active_by_ticker={"000001": [{"ticker": "000001", "exchange": "SZSE", "name": "平安银行", "asset_type": "Stock"}]},
        covered_exchanges=set(),
        partial_covered_exchanges={"SZSE"},
        identifier_map={},
    )
    assert result["status"] == "verified"
    assert result["reason"] == "Matched active official listing with a local-language issuer name."


def test_classify_row_treats_sse_local_language_names_as_verified() -> None:
    row = {
        "ticker": "600000",
        "exchange": "SSE",
        "asset_type": "Stock",
        "name": "Shanghai Pudong Development Bank Co Ltd",
        "country": "China",
        "country_code": "CN",
        "isin": "",
        "sector": "Financials",
    }
    result = classify_row(
        row,
        active_by_key={
            ("SSE", "600000"): [
                {
                    "ticker": "600000",
                    "exchange": "SSE",
                    "name": "上海浦东发展银行股份有限公司",
                    "asset_type": "Stock",
                    "source_key": "sse_a_share_list",
                    "listing_status": "active",
                }
            ]
        },
        any_by_key={},
        active_by_ticker={"600000": [{"ticker": "600000", "exchange": "SSE", "name": "上海浦东发展银行股份有限公司", "asset_type": "Stock"}]},
        covered_exchanges=set(),
        partial_covered_exchanges={"SSE"},
        identifier_map={},
    )
    assert result["status"] == "verified"
    assert result["reason"] == "Matched active official listing with a local-language issuer name."


def test_classify_row_downgrades_tpex_trading_label_to_reference_gap() -> None:
    row = {
        "ticker": "4971",
        "exchange": "TPEX",
        "asset_type": "Stock",
        "name": "IntelliEPI Cayman",
        "country": "Cayman Islands",
        "country_code": "KY",
        "isin": "KYG480071011",
        "sector": "Information Technology",
    }
    result = classify_row(
        row,
        active_by_key={
            ("TPEX", "4971"): [
                {
                    "ticker": "4971",
                    "exchange": "TPEX",
                    "name": "IET-KY",
                    "asset_type": "Stock",
                    "source_key": "tpex_mainboard_daily_quotes",
                    "listing_status": "active",
                }
            ]
        },
        any_by_key={},
        active_by_ticker={"4971": [{"ticker": "4971", "exchange": "TPEX", "name": "IET-KY", "asset_type": "Stock"}]},
        covered_exchanges=set(),
        partial_covered_exchanges={"TPEX"},
        identifier_map={},
    )
    assert result["status"] == "reference_gap"
    assert result["reason"] == "Only low-confidence issuer reference evidence exists for this listing."


def test_classify_row_non_active_official() -> None:
    row = {
        "ticker": "ATEST",
        "exchange": "NYSE MKT",
        "asset_type": "Stock",
        "name": "Tick Pilot Test Control Common Stock",
        "country": "United States",
        "country_code": "US",
        "isin": "",
        "sector": "",
    }
    result = classify_row(
        row,
        active_by_key={},
        any_by_key={("NYSE MKT", "ATEST"): [{"ticker": "ATEST", "exchange": "NYSE MKT", "name": "Tick Pilot Test Control Common Stock", "asset_type": "Stock", "listing_status": "test", "source_key": "nasdaq_other_listed"}]},
        active_by_ticker={},
        covered_exchanges={"NYSE MKT"},
        partial_covered_exchanges=set(),
        identifier_map={},
    )
    assert result["status"] == "non_active_official"


def test_classify_row_collision() -> None:
    row = {
        "ticker": "SENS",
        "exchange": "NYSE MKT",
        "asset_type": "Stock",
        "name": "Senseonics Holdings, Inc.",
        "country": "United States",
        "country_code": "US",
        "isin": "",
        "sector": "",
    }
    result = classify_row(
        row,
        active_by_key={},
        any_by_key={},
        active_by_ticker={"SENS": [{"ticker": "SENS", "exchange": "NASDAQ", "name": "Senseonics Holdings, Inc.", "asset_type": "Stock"}]},
        covered_exchanges={"NYSE MKT"},
        partial_covered_exchanges=set(),
        identifier_map={},
    )
    assert result["status"] == "cross_exchange_collision"


def test_classify_row_downgrades_weak_collision_peers_to_reference_gap() -> None:
    row = {
        "ticker": "AEAE",
        "exchange": "NASDAQ",
        "asset_type": "Stock",
        "name": "Altenergy Acquisition Corp",
        "country": "United States",
        "country_code": "US",
        "isin": "",
        "sector": "",
    }
    result = classify_row(
        row,
        active_by_key={},
        any_by_key={},
        active_by_ticker={"AEAE": [{"ticker": "AEAE", "exchange": "OTC", "name": "AltEnergy Acquisition Corp", "asset_type": "Stock"}]},
        covered_exchanges={"NASDAQ"},
        partial_covered_exchanges=set(),
        identifier_map={},
    )
    assert result["status"] == "reference_gap"
    assert result["reason"] == "Only weak cross-exchange collision evidence exists for this listing."


def test_classify_row_downgrades_low_confidence_collision_sources_to_reference_gap() -> None:
    row = {
        "ticker": "NGD",
        "exchange": "NYSE MKT",
        "asset_type": "Stock",
        "name": "New Gold Inc",
        "country": "Canada",
        "country_code": "CA",
        "isin": "CA6445351068",
        "sector": "Materials",
    }
    result = classify_row(
        row,
        active_by_key={},
        any_by_key={},
        active_by_ticker={
            "NGD": [
                {
                    "ticker": "NGD",
                    "exchange": "NYSE",
                    "name": "New Gold Inc.",
                    "asset_type": "Stock",
                    "source_key": "sec_company_tickers_exchange",
                },
                {
                    "ticker": "NGD",
                    "exchange": "TSX",
                    "name": "New Gold Inc.",
                    "asset_type": "Stock",
                    "source_key": "tmx_interlisted_companies",
                },
            ]
        },
        covered_exchanges={"NYSE MKT"},
        partial_covered_exchanges=set(),
        identifier_map={},
    )
    assert result["status"] == "reference_gap"
    assert result["reason"] == "Only weak cross-exchange collision evidence exists for this listing."


def test_classify_row_downgrades_mismatched_collision_peers_to_reference_gap() -> None:
    row = {
        "ticker": "IMG",
        "exchange": "NASDAQ",
        "asset_type": "Stock",
        "name": "CIMG Inc.",
        "country": "Canada",
        "country_code": "CA",
        "isin": "CA4509131088",
        "sector": "",
    }
    result = classify_row(
        row,
        active_by_key={},
        any_by_key={},
        active_by_ticker={"IMG": [{"ticker": "IMG", "exchange": "TSX", "name": "IAMGold Corporation", "asset_type": "Stock"}]},
        covered_exchanges={"NASDAQ"},
        partial_covered_exchanges=set(),
        identifier_map={},
    )
    assert result["status"] == "reference_gap"
    assert result["reason"] == "Only weak cross-exchange collision evidence exists for this listing."


def test_classify_row_downgrades_lse_peer_collision_to_reference_gap_for_etf() -> None:
    row = {
        "ticker": "JAGG",
        "exchange": "NYSE ARCA",
        "asset_type": "ETF",
        "name": "JPMorgan BetaBuilders U.S. Aggregate Bond ETF",
        "country": "United States",
        "country_code": "US",
        "isin": "US46641Q2416",
        "sector": "Corporate Bonds",
    }
    result = classify_row(
        row,
        active_by_key={},
        any_by_key={},
        active_by_ticker={
            "JAGG": [
                {
                    "ticker": "JAGG",
                    "exchange": "LSE",
                    "name": "JPMORGAN ETFS (IRELAND) ICAV JPM GLOBAL AGG BOND ACTIVE UCITS ETF DIS",
                    "asset_type": "ETF",
                    "source_key": "lse_company_reports",
                }
            ]
        },
        covered_exchanges={"NYSE ARCA"},
        partial_covered_exchanges=set(),
        identifier_map={},
    )
    assert result["status"] == "reference_gap"
    assert result["reason"] == "Only weak cross-exchange collision evidence exists for this listing."


def test_classify_row_ignores_asset_type_mismatched_collision_peers() -> None:
    row = {
        "ticker": "TWN",
        "exchange": "Euronext",
        "asset_type": "ETF",
        "name": "Multi Units Luxembourg - Lyxor Msci Taiwan Ucits Etf",
        "country": "France",
        "country_code": "FR",
        "isin": "",
        "sector": "",
    }
    result = classify_row(
        row,
        active_by_key={},
        any_by_key={},
        active_by_ticker={"TWN": [{"ticker": "TWN", "exchange": "NYSE", "name": "Taiwan Fund, Inc. (The) Common Stock", "asset_type": "Stock"}]},
        covered_exchanges={"Euronext"},
        partial_covered_exchanges=set(),
        identifier_map={},
    )
    assert result["status"] == "reference_gap"
    assert result["reason"] == "Only weak cross-exchange collision evidence exists for this listing."


def test_classify_row_downgrades_single_token_etf_collision_overlap_to_reference_gap() -> None:
    row = {
        "ticker": "STK",
        "exchange": "Euronext",
        "asset_type": "ETF",
        "name": "SPDR MSCI Europe Technology UCITS ETF",
        "country": "Australia",
        "country_code": "AU",
        "isin": "AU0000100901",
        "sector": "",
    }
    result = classify_row(
        row,
        active_by_key={},
        any_by_key={},
        active_by_ticker={"STK": [{"ticker": "STK", "exchange": "NYSE", "name": "Columbia Seligman Premium Technology Growth Fund, Inc.", "asset_type": "ETF"}]},
        covered_exchanges={"Euronext"},
        partial_covered_exchanges=set(),
        identifier_map={},
    )
    assert result["status"] == "reference_gap"
    assert result["reason"] == "Only weak cross-exchange collision evidence exists for this listing."


def test_classify_row_prefers_non_sec_stock_evidence() -> None:
    row = {
        "ticker": "FCBC",
        "exchange": "NASDAQ",
        "asset_type": "Stock",
        "name": "First Community Bancshares Inc",
        "country": "United States",
        "country_code": "US",
        "isin": "",
        "sector": "",
    }
    result = classify_row(
        row,
        active_by_key={
            ("NASDAQ", "FCBC"): [
                {"ticker": "FCBC", "exchange": "NASDAQ", "name": "FIRST COMMUNITY BANKSHARES INC /VA/", "asset_type": "ETF", "source_key": "sec_company_tickers_exchange"},
                {"ticker": "FCBC", "exchange": "NASDAQ", "name": "First Community Bankshares, Inc. - Common Stock", "asset_type": "Stock", "source_key": "nasdaq_listed"},
            ]
        },
        any_by_key={},
        active_by_ticker={"FCBC": [{"ticker": "FCBC", "exchange": "NASDAQ", "name": "First Community Bankshares, Inc. - Common Stock", "asset_type": "Stock"}]},
        covered_exchanges={"NASDAQ"},
        partial_covered_exchanges=set(),
        identifier_map={},
    )
    assert result["status"] == "verified"


def test_classify_row_treats_otc_missing_as_reference_gap() -> None:
    row = {
        "ticker": "AABB",
        "exchange": "OTC",
        "asset_type": "Stock",
        "name": "Asia Broadband Inc",
        "country": "United States",
        "country_code": "US",
        "isin": "",
        "sector": "",
    }
    result = classify_row(
        row,
        active_by_key={},
        any_by_key={},
        active_by_ticker={},
        covered_exchanges={"OTC"},
        partial_covered_exchanges=set(),
        identifier_map={},
    )
    assert result["status"] == "reference_gap"


def test_classify_row_treats_us_missing_as_reference_gap_when_negative_feed_is_weak() -> None:
    row = {
        "ticker": "CFLT",
        "exchange": "NASDAQ",
        "asset_type": "Stock",
        "name": "Confluent Inc",
        "country": "United States",
        "country_code": "US",
        "isin": "US20717M1036",
        "sector": "Information Technology",
    }
    result = classify_row(
        row,
        active_by_key={},
        any_by_key={},
        active_by_ticker={},
        covered_exchanges={"NASDAQ"},
        partial_covered_exchanges=set(),
        identifier_map={},
    )
    assert result["status"] == "reference_gap"
    assert result["reason"] == "This exchange is only weakly covered by the current official reference layer."


def test_classify_row_treats_euronext_missing_as_reference_gap_when_negative_feed_is_weak() -> None:
    row = {
        "ticker": "ALCAR",
        "exchange": "Euronext",
        "asset_type": "Stock",
        "name": "Carmat",
        "country": "France",
        "country_code": "FR",
        "isin": "FR0010907956",
        "sector": "Health Care",
    }
    result = classify_row(
        row,
        active_by_key={},
        any_by_key={},
        active_by_ticker={},
        covered_exchanges={"Euronext"},
        partial_covered_exchanges=set(),
        identifier_map={},
    )
    assert result["status"] == "reference_gap"
    assert result["reason"] == "This exchange is only weakly covered by the current official reference layer."


def test_classify_row_treats_b3_missing_as_reference_gap_when_negative_feed_is_weak() -> None:
    row = {
        "ticker": "GUAR3",
        "exchange": "B3",
        "asset_type": "Stock",
        "name": "Guararapes Confecções S.A",
        "country": "Brazil",
        "country_code": "BR",
        "isin": "BRGUARACNOR4",
        "sector": "Consumer Discretionary",
    }
    result = classify_row(
        row,
        active_by_key={},
        any_by_key={},
        active_by_ticker={},
        covered_exchanges={"B3"},
        partial_covered_exchanges=set(),
        identifier_map={},
    )
    assert result["status"] == "reference_gap"
    assert result["reason"] == "This exchange is only weakly covered by the current official reference layer."


def test_classify_row_treats_partial_official_exchange_missing_as_reference_gap() -> None:
    row = {
        "ticker": "ABEA",
        "exchange": "XETRA",
        "asset_type": "Stock",
        "name": "Alphabet Inc Class A",
        "country": "Germany",
        "country_code": "DE",
        "isin": "US02079K3059",
        "sector": "",
    }
    result = classify_row(
        row,
        active_by_key={},
        any_by_key={},
        active_by_ticker={},
        covered_exchanges=set(),
        partial_covered_exchanges={"XETRA"},
        identifier_map={},
    )
    assert result["status"] == "reference_gap"


def test_classify_row_treats_twse_etf_missing_as_reference_gap() -> None:
    row = {
        "ticker": "00943",
        "exchange": "TWSE",
        "asset_type": "ETF",
        "name": "Mega Taiwan IT Growth and High Dividend Equal Weight ETF",
        "country": "Taiwan",
        "country_code": "TW",
        "isin": "",
        "sector": "",
    }
    result = classify_row(
        row,
        active_by_key={},
        any_by_key={},
        active_by_ticker={},
        covered_exchanges={"TWSE"},
        partial_covered_exchanges=set(),
        identifier_map={},
    )
    assert result["status"] == "reference_gap"
    assert result["reason"] == "This asset type is not fully covered by the current official reference layer for this exchange."


def test_classify_row_treats_etf_like_stock_reference_as_verified() -> None:
    row = {
        "ticker": "BAR",
        "exchange": "NYSE ARCA",
        "asset_type": "ETF",
        "name": "GraniteShares Gold Trust",
        "country": "United States",
        "country_code": "US",
        "isin": "",
        "sector": "",
    }
    result = classify_row(
        row,
        active_by_key={
            ("NYSE ARCA", "BAR"): [
                {
                    "ticker": "BAR",
                    "exchange": "NYSE ARCA",
                    "name": "GraniteShares Gold Trust Shares of Beneficial Interest",
                    "asset_type": "Stock",
                    "source_key": "nasdaq_other_listed",
                    "listing_status": "active",
                }
            ]
        },
        any_by_key={},
        active_by_ticker={},
        covered_exchanges={"NYSE ARCA"},
        partial_covered_exchanges=set(),
        identifier_map={},
    )
    assert result["status"] == "verified"
    assert result["reason"] == "Official directory labels this ETP-like listing as stock, but the issuer name clearly identifies an ETF/ETN trust line."


def test_classify_row_treats_generic_etf_placeholder_name_as_reference_gap() -> None:
    row = {
        "ticker": "CTWO",
        "exchange": "NYSE ARCA",
        "asset_type": "ETF",
        "name": "COtwo Advisors Physical European Carbon Allowance Trust",
        "country": "United States",
        "country_code": "US",
        "isin": "",
        "sector": "",
    }
    result = classify_row(
        row,
        active_by_key={
            ("NYSE ARCA", "CTWO"): [
                {
                    "ticker": "CTWO",
                    "exchange": "NYSE ARCA",
                    "name": "Common units",
                    "asset_type": "ETF",
                    "source_key": "nasdaq_other_listed",
                    "listing_status": "active",
                }
            ]
        },
        any_by_key={},
        active_by_ticker={},
        covered_exchanges={"NYSE ARCA"},
        partial_covered_exchanges=set(),
        identifier_map={},
    )
    assert result["status"] == "reference_gap"
    assert result["reason"] == "Official reference only exposes a generic ETP placeholder name."


def test_classify_row_treats_grouped_euronext_etf_collision_as_reference_gap() -> None:
    row = {
        "ticker": "CEM",
        "exchange": "Euronext",
        "asset_type": "ETF",
        "name": "Amundi MSCI Europe Small Cap ESG Climate Net Zero Ambition CTB ETF Acc",
        "country": "Luxembourg",
        "country_code": "LU",
        "isin": "LU1681041544",
        "sector": "Financials",
    }
    result = classify_row(
        row,
        active_by_key={
            ("Euronext", "CEM"): [
                {
                    "ticker": "CEM",
                    "exchange": "Euronext",
                    "name": "CEMENTIR HOLDING",
                    "asset_type": "Stock",
                    "source_key": "euronext_equities",
                    "listing_status": "active",
                }
            ]
        },
        any_by_key={},
        active_by_ticker={},
        covered_exchanges={"Euronext"},
        partial_covered_exchanges=set(),
        identifier_map={},
    )
    assert result["status"] == "reference_gap"
    assert result["reason"] == "Grouped Euronext feed resolves this ticker to a stock line on another venue."


def test_classify_row_downgrades_otc_sec_name_mismatch_to_reference_gap() -> None:
    row = {
        "ticker": "CNTMF",
        "exchange": "OTC",
        "asset_type": "Stock",
        "name": "Cansortium Inc",
        "country": "Canada",
        "country_code": "CA",
        "isin": "CA13809L1094",
        "sector": "Health Care",
    }
    result = classify_row(
        row,
        active_by_key={
            ("OTC", "CNTMF"): [
                {
                    "ticker": "CNTMF",
                    "exchange": "OTC",
                    "name": "Fluent Corp.",
                    "asset_type": "Stock",
                    "source_key": "sec_company_tickers_exchange",
                    "listing_status": "active",
                }
            ]
        },
        any_by_key={},
        active_by_ticker={},
        covered_exchanges={"OTC"},
        partial_covered_exchanges=set(),
        identifier_map={},
    )
    assert result["status"] == "reference_gap"
    assert result["reason"] == "Only low-confidence issuer reference evidence exists for this listing."


def test_classify_row_downgrades_euronext_trading_label_name_mismatch_to_reference_gap() -> None:
    row = {
        "ticker": "ALENT",
        "exchange": "Euronext",
        "asset_type": "Stock",
        "name": "Entreparticuli",
        "country": "France",
        "country_code": "FR",
        "isin": "FR0010424697",
        "sector": "Real Estate",
    }
    result = classify_row(
        row,
        active_by_key={
            ("Euronext", "ALENT"): [
                {
                    "ticker": "ALENT",
                    "exchange": "Euronext",
                    "name": "ETHERO",
                    "asset_type": "Stock",
                    "source_key": "euronext_equities",
                    "listing_status": "active",
                }
            ]
        },
        any_by_key={},
        active_by_ticker={},
        covered_exchanges={"Euronext"},
        partial_covered_exchanges=set(),
        identifier_map={},
    )
    assert result["status"] == "reference_gap"
    assert result["reason"] == "Official Euronext feed only exposes a trading label rather than a reliable full issuer name."


def test_classify_row_downgrades_nyse_sec_name_mismatch_to_reference_gap() -> None:
    row = {
        "ticker": "SCE-PM",
        "exchange": "NYSE",
        "asset_type": "Stock",
        "name": "SCE Trust VII",
        "country": "United States",
        "country_code": "US",
        "isin": "US7838922018",
        "sector": "",
    }
    result = classify_row(
        row,
        active_by_key={
            ("NYSE", "SCE-PM"): [
                {
                    "ticker": "SCE-PM",
                    "exchange": "NYSE",
                    "name": "SOUTHERN CALIFORNIA EDISON Co",
                    "asset_type": "Stock",
                    "source_key": "sec_company_tickers_exchange",
                    "listing_status": "active",
                }
            ]
        },
        any_by_key={},
        active_by_ticker={},
        covered_exchanges={"NYSE"},
        partial_covered_exchanges=set(),
        identifier_map={},
    )
    assert result["status"] == "reference_gap"
    assert result["reason"] == "Only low-confidence issuer reference evidence exists for this listing."


def test_classify_row_downgrades_tsx_interlisted_name_mismatch_to_reference_gap() -> None:
    row = {
        "ticker": "PMET",
        "exchange": "TSX",
        "asset_type": "Stock",
        "name": "Patriot Battery Metals Inc.",
        "country": "Canada",
        "country_code": "CA",
        "isin": "CA70337R1073",
        "sector": "Materials",
    }
    result = classify_row(
        row,
        active_by_key={
            ("TSX", "PMET"): [
                {
                    "ticker": "PMET",
                    "exchange": "TSX",
                    "name": "PMET Resources Inc.",
                    "asset_type": "Stock",
                    "source_key": "tmx_interlisted_companies",
                    "listing_status": "active",
                }
            ]
        },
        any_by_key={},
        active_by_ticker={},
        covered_exchanges={"TSX"},
        partial_covered_exchanges=set(),
        identifier_map={},
    )
    assert result["status"] == "reference_gap"
    assert result["reason"] == "Only low-confidence issuer reference evidence exists for this listing."


def test_classify_row_downgrades_tmx_listed_issuers_asset_type_mismatch_to_reference_gap() -> None:
    row = {
        "ticker": "FAP",
        "exchange": "TSX",
        "asset_type": "Stock",
        "name": "abrdn Asia Pacific Income Fund VCC",
        "country": "Singapore",
        "country_code": "SG",
        "isin": "SGXZ44536704",
        "sector": "Financials",
    }
    result = classify_row(
        row,
        active_by_key={
            ("TSX", "FAP"): [
                {
                    "ticker": "FAP",
                    "exchange": "TSX",
                    "name": "abrdn Asia-Pacific Income Fund VCC",
                    "asset_type": "ETF",
                    "source_key": "tmx_listed_issuers",
                    "listing_status": "active",
                }
            ]
        },
        any_by_key={},
        active_by_ticker={},
        covered_exchanges=set(),
        partial_covered_exchanges={"TSX"},
        identifier_map={},
    )
    assert result["status"] == "reference_gap"
    assert result["reason"] == "Only low-confidence asset_type evidence exists for this listing."


def test_classify_row_downgrades_lse_name_mismatch_to_reference_gap() -> None:
    row = {
        "ticker": "0NQ5",
        "exchange": "LSE",
        "asset_type": "Stock",
        "name": "Quadient SA",
        "country": "France",
        "country_code": "FR",
        "isin": "FR0000120560",
        "sector": "Industrials",
    }
    result = classify_row(
        row,
        active_by_key={
            ("LSE", "0NQ5"): [
                {
                    "ticker": "0NQ5",
                    "exchange": "LSE",
                    "name": "NEOPOST SA NEOPOST ORD SHS",
                    "asset_type": "Stock",
                    "source_key": "lse_company_reports",
                    "listing_status": "active",
                }
            ]
        },
        any_by_key={},
        active_by_ticker={},
        covered_exchanges={"LSE"},
        partial_covered_exchanges=set(),
        identifier_map={},
    )
    assert result["status"] == "reference_gap"
    assert result["reason"] == "Only low-confidence issuer reference evidence exists for this listing."


def test_classify_row_downgrades_lse_asset_type_mismatch_to_reference_gap() -> None:
    row = {
        "ticker": "PHAG",
        "exchange": "LSE",
        "asset_type": "ETF",
        "name": "WisdomTree Physical Silver",
        "country": "Jersey",
        "country_code": "JE",
        "isin": "JE00B1VS3333",
        "sector": "",
    }
    result = classify_row(
        row,
        active_by_key={
            ("LSE", "PHAG"): [
                {
                    "ticker": "PHAG",
                    "exchange": "LSE",
                    "name": "WISDOMTREE METAL SECURITIES WISDOMTREE PHYSICAL SILVER",
                    "asset_type": "Stock",
                    "source_key": "lse_company_reports",
                    "listing_status": "active",
                }
            ]
        },
        any_by_key={},
        active_by_ticker={},
        covered_exchanges={"LSE"},
        partial_covered_exchanges=set(),
        identifier_map={},
    )
    assert result["status"] == "reference_gap"
    assert result["reason"] == "Only low-confidence asset_type evidence exists for this listing."


def test_classify_row_downgrades_krx_name_mismatch_to_reference_gap() -> None:
    row = {
        "ticker": "016600",
        "exchange": "KOSDAQ",
        "asset_type": "Stock",
        "name": "Q Capital Partners Co. Ltd",
        "country": "South Korea",
        "country_code": "KR",
        "isin": "KR7016600009",
        "sector": "",
    }
    result = classify_row(
        row,
        active_by_key={
            ("KOSDAQ", "016600"): [
                {
                    "ticker": "016600",
                    "exchange": "KOSDAQ",
                    "name": "QCP",
                    "asset_type": "Stock",
                    "source_key": "krx_listed_companies",
                    "listing_status": "active",
                }
            ]
        },
        any_by_key={},
        active_by_ticker={},
        covered_exchanges=set(),
        partial_covered_exchanges={"KOSDAQ"},
        identifier_map={},
    )
    assert result["status"] == "reference_gap"
    assert result["reason"] == "Only low-confidence issuer reference evidence exists for this listing."


def test_classify_row_treats_krx_etfs_as_partial_coverage() -> None:
    row = {
        "ticker": "091220",
        "exchange": "KRX",
        "asset_type": "ETF",
        "name": "MiraeAsset TIGER BANKS ETF",
        "country": "South Korea",
        "country_code": "KR",
        "isin": "KR7091220004",
        "sector": "",
    }
    result = classify_row(
        row,
        active_by_key={},
        any_by_key={},
        active_by_ticker={},
        covered_exchanges=set(),
        partial_covered_exchanges={"KRX"},
        identifier_map={},
    )
    assert result["status"] == "reference_gap"
    assert result["reason"] == "This exchange is only partially covered by the current official reference layer."


def test_classify_row_treats_krx_local_language_etf_names_as_verified() -> None:
    row = {
        "ticker": "451060",
        "exchange": "KRX",
        "asset_type": "ETF",
        "name": "451060",
        "country": "South Korea",
        "country_code": "KR",
        "isin": "KR7451060008",
        "sector": "",
    }
    result = classify_row(
        row,
        active_by_key={
            ("KRX", "451060"): [
                {
                    "ticker": "451060",
                    "exchange": "KRX",
                    "name": "1Q 200액티브",
                    "asset_type": "ETF",
                    "source_key": "krx_etf_finder",
                    "listing_status": "active",
                }
            ]
        },
        any_by_key={},
        active_by_ticker={},
        covered_exchanges=set(),
        partial_covered_exchanges={"KRX"},
        identifier_map={},
    )
    assert result["status"] == "verified"
    assert result["reason"] == "Matched active official listing with a local-language issuer name."


def test_classify_row_downgrades_krx_etf_finder_name_mismatch_to_reference_gap() -> None:
    row = {
        "ticker": "491220",
        "exchange": "KRX",
        "asset_type": "ETF",
        "name": "491220",
        "country": "South Korea",
        "country_code": "KR",
        "isin": "",
        "sector": "",
    }
    result = classify_row(
        row,
        active_by_key={
            ("KRX", "491220"): [
                {
                    "ticker": "491220",
                    "exchange": "KRX",
                    "name": "PLUS 200TR",
                    "asset_type": "ETF",
                    "source_key": "krx_etf_finder",
                    "listing_status": "active",
                }
            ]
        },
        any_by_key={},
        active_by_ticker={},
        covered_exchanges=set(),
        partial_covered_exchanges={"KRX"},
        identifier_map={},
    )
    assert result["status"] == "reference_gap"
    assert result["reason"] == "Only low-confidence issuer reference evidence exists for this listing."


def test_is_code_like_reference_name_handles_compact_trading_labels() -> None:
    assert is_code_like_reference_name("MBWS", "MBWS")
    assert not is_code_like_reference_name("First Community Bankshares, Inc. - Common Stock", "FCBC")


def test_summarize_results_counts_bad_statuses() -> None:
    summary = summarize_results(
        [
            {"status": "verified"},
            {"status": "name_mismatch", "ticker": "A", "exchange": "X"},
            {"status": "missing_from_official", "ticker": "B", "exchange": "Y"},
        ]
    )
    assert summary["items"] == 3
    assert summary["status_counts"]["verified"] == 1
    assert len(summary["finding_examples"]) == 2

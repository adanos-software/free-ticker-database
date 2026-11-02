from __future__ import annotations

from pathlib import Path

from scripts.lib.etf_category_overrides import (
    apply_reviewed_etf_category_override,
    is_well_formed_etf_category_override,
    load_etf_category_overrides,
    stale_etf_category_override_keys,
)
from scripts.lib.dataio import write_csv
from scripts.rebuild_dataset import REVIEW_ETF_CATEGORY_OVERRIDES_CSV


def test_apply_reviewed_etf_category_override_rejects_stale_old_value() -> None:
    row = {
        "ticker": "GLD",
        "exchange": "NYSE ARCA",
        "asset_type": "ETF",
        "etf_category": "Commodity",
        "sector": "Commodity",
    }
    override = {
        "ticker": "GLD",
        "exchange": "NYSE ARCA",
        "old_value": "Equity",
        "new_value": "Commodity",
        "reason": "test",
        "source_1": "https://example.com/a",
        "source_2": "https://example.com/b",
    }
    updated, skip = apply_reviewed_etf_category_override(row, override)
    assert skip == "stale_old_value"
    assert updated["etf_category"] == "Commodity"


def test_apply_reviewed_etf_category_override_rejects_non_etf() -> None:
    row = {
        "ticker": "AAPL",
        "exchange": "NASDAQ",
        "asset_type": "Stock",
        "stock_sector": "Information Technology",
        "etf_category": "",
        "sector": "Information Technology",
    }
    override = {
        "ticker": "AAPL",
        "exchange": "NASDAQ",
        "old_value": "Equity",
        "new_value": "Commodity",
        "reason": "test",
        "source_1": "https://example.com/a",
        "source_2": "https://example.com/b",
    }
    updated, skip = apply_reviewed_etf_category_override(row, override)
    assert skip == "non_etf"
    assert updated["stock_sector"] == "Information Technology"
    assert not updated.get("etf_category")


def test_apply_reviewed_etf_category_override_only_changes_etf_category() -> None:
    row = {
        "ticker": "GLD",
        "exchange": "NYSE ARCA",
        "asset_type": "ETF",
        "name": "SPDR Gold Shares",
        "etf_category": "Equity",
        "sector": "Equity",
        "country": "United States",
        "isin": "US78463V1070",
    }
    override = {
        "ticker": "GLD",
        "exchange": "NYSE ARCA",
        "old_value": "Equity",
        "new_value": "Commodity",
        "reason": "Official issuer and exchange product taxonomy name the fund as a gold bullion ETF.",
        "source_1": "https://www.ssga.com/us/en/institutional/etfs/funds/spdr-gold-shares-gld",
        "source_2": "https://www.nyse.com/quote/XNYS:GLD",
    }
    updated, skip = apply_reviewed_etf_category_override(row, override)
    assert skip is None
    assert updated["etf_category"] == "Commodity"
    assert updated["sector"] == "Commodity"
    assert updated["name"] == "SPDR Gold Shares"
    assert updated["isin"] == "US78463V1070"
    assert updated["country"] == "United States"


def test_stale_etf_category_override_keys_detects_current_mismatch() -> None:
    listings = [
        {
            "ticker": "GLD",
            "exchange": "NYSE ARCA",
            "asset_type": "ETF",
            "etf_category": "Equity",
        }
    ]
    overrides = [
        {
            "ticker": "GLD",
            "exchange": "NYSE ARCA",
            "old_value": "Equity",
            "new_value": "Commodity",
            "reason": "stale guard",
            "source_1": "https://example.com/a",
            "source_2": "https://example.com/b",
        }
    ]
    stale = stale_etf_category_override_keys(
        overrides,
        [
            {
                "ticker": "GLD",
                "exchange": "NYSE ARCA",
                "asset_type": "ETF",
                "etf_category": "Fixed Income",
            }
        ],
    )
    assert any("stale_old_value|NYSE ARCA::GLD" in item for item in stale)

    assert stale_etf_category_override_keys(
        overrides,
        [
            {
                "ticker": "GLD",
                "exchange": "NYSE ARCA",
                "asset_type": "ETF",
                "etf_category": "Commodity",
            }
        ],
    ) == []


def test_is_well_formed_etf_category_override_requires_canonical_values() -> None:
    assert is_well_formed_etf_category_override(
        {
            "ticker": "GLD",
            "exchange": "NYSE ARCA",
            "old_value": "Equity",
            "new_value": "Commodity",
            "reason": "ok",
            "source_1": "https://a.example",
            "source_2": "https://b.example",
        }
    )
    assert not is_well_formed_etf_category_override(
        {
            "ticker": "GLD",
            "exchange": "NYSE ARCA",
            "old_value": "Equity",
            "new_value": "Treasury Bonds",
            "reason": "non-canonical bucket",
            "source_1": "https://a.example",
            "source_2": "https://b.example",
        }
    )


def test_load_etf_category_overrides_from_csv(tmp_path: Path) -> None:
    path = tmp_path / "etf_category_overrides.csv"
    write_csv(
        path,
        ["ticker", "exchange", "old_value", "new_value", "reason", "source_1", "source_2"],
        [
            {
                "ticker": "GLD",
                "exchange": "NYSE ARCA",
                "old_value": "Equity",
                "new_value": "Commodity",
                "reason": "reviewed",
                "source_1": "https://a.example",
                "source_2": "https://b.example",
            }
        ],
    )
    loaded = load_etf_category_overrides(path)
    assert loaded[("GLD", "NYSE ARCA")]["new_value"] == "Commodity"


def test_repo_override_path_is_under_review_overrides() -> None:
    assert REVIEW_ETF_CATEGORY_OVERRIDES_CSV.name == "etf_category_overrides.csv"
    assert REVIEW_ETF_CATEGORY_OVERRIDES_CSV.parent.name == "review_overrides"

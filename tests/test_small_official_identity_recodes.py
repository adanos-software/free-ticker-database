from __future__ import annotations

import csv
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
METADATA_UPDATES_CSV = ROOT / "data" / "review_overrides" / "metadata_updates.csv"


def _load_updates() -> list[dict[str, str]]:
    with METADATA_UPDATES_CSV.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _row(ticker: str, exchange: str, field: str) -> dict[str, str]:
    matches = [
        row
        for row in _load_updates()
        if (row.get("ticker"), row.get("exchange"), row.get("field")) == (ticker, exchange, field)
    ]
    assert len(matches) == 1, (ticker, exchange, field, len(matches))
    return matches[0]


def test_krx_mkif_is_official_stock_not_etf() -> None:
    asset_type = _row("088980", "KRX", "asset_type")
    category = _row("088980", "KRX", "etf_category")

    assert asset_type["decision"] == "update"
    assert asset_type["proposed_value"] == "Stock"
    assert "krx_listed_companies" in asset_type["reason"]
    assert "KR7088980008" in asset_type["reason"]
    assert category["decision"] == "clear"
    assert category["proposed_value"] == ""


def test_sse_cl_nuam_uses_official_bolsa_santiago_name() -> None:
    name = _row("NUAM", "SSE_CL", "name")

    assert name["decision"] == "update"
    assert name["proposed_value"] == "NUAM S.A."
    assert "bolsa_santiago_instruments" in name["reason"]


def test_euronext_cbdg_uses_official_euronext_isin() -> None:
    isin = _row("CBDG", "Euronext", "isin")

    assert isin["decision"] == "update"
    assert isin["proposed_value"] == "FR001400SUB7"
    assert "euronext_equities" in isin["reason"]
    assert "FR0000079659" in isin["reason"]


def _listing(listing_key: str) -> dict[str, str]:
    with (ROOT / "data" / "listings.csv").open(newline="", encoding="utf-8") as handle:
        matches = [row for row in csv.DictReader(handle) if row.get("listing_key") == listing_key]
    assert len(matches) == 1, listing_key
    return matches[0]


def _primary(ticker: str) -> dict[str, str]:
    with (ROOT / "data" / "tickers.csv").open(newline="", encoding="utf-8") as handle:
        matches = [row for row in csv.DictReader(handle) if row.get("ticker") == ticker]
    assert len(matches) == 1, ticker
    return matches[0]


def test_compagnie_du_cambodge_keeps_lse_primary_because_cbdg_ticker_is_taken() -> None:
    path = ROOT / "data" / "review_overrides" / "primary_listing_overrides.csv"
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    matches = [row for row in rows if row.get("isin") == "FR001400SUB7"]
    assert len(matches) == 1
    assert matches[0]["listing_key"] == "LSE::0I76"
    assert "US87243T1007" in matches[0]["reason"]

    cambodge = _primary("0I76")
    assert cambodge["exchange"] == "LSE"
    assert cambodge["isin"] == "FR001400SUB7"
    assert cambodge["name"] == "Compagnie du Cambodge"

    otc = _primary("CBDG")
    assert otc["exchange"] == "OTC"
    assert otc["isin"] == "US87243T1007"


def test_exported_listings_match_official_identity_recodes() -> None:
    krx = _listing("KRX::088980")
    assert krx["asset_type"] == "Stock"
    assert krx["isin"] == "KR7088980008"
    assert krx["etf_category"] == ""

    nuam = _listing("SSE_CL::NUAM")
    assert nuam["name"] == "NUAM S.A."

    euronext = _listing("Euronext::CBDG")
    assert euronext["isin"] == "FR001400SUB7"
    assert euronext["name"] == "Compagnie du Cambodge"

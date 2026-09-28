from __future__ import annotations

from pathlib import Path

from scripts.lib.dataio import is_well_formed_metadata_update, load_csv


ROOT = Path(__file__).resolve().parents[1]
HEG_ISIN = "INE545A01024"
SANGINITA_ISIN = "INE753W01010"
MKA_ISIN = "CA60701M1077"
JINTAI_ISIN = "KYG5141G1148"


def _metadata(ticker: str, exchange: str, field: str) -> dict[str, str]:
    rows = [
        row
        for row in load_csv(ROOT / "data/review_overrides/metadata_updates.csv")
        if row.get("ticker") == ticker and row.get("exchange") == exchange and row.get("field") == field
    ]
    assert len(rows) == 1, (ticker, exchange, field, len(rows))
    assert is_well_formed_metadata_update(rows[0])
    return rows[0]


def _transition(old_listing_key: str) -> dict[str, str]:
    rows = [
        row
        for row in load_csv(ROOT / "data/review_overrides/listing_transitions.csv")
        if row.get("old_listing_key") == old_listing_key
    ]
    assert len(rows) == 1, (old_listing_key, len(rows))
    return rows[0]


def test_heg_nse_symbol_change_keeps_isin() -> None:
    row = _transition("NSE_IN::HEG")
    assert row["new_listing_key"] == "NSE_IN::HEGAM"
    assert row["event_type"] == "symbol_changed"
    assert row["identity_type"] == "same_isin"
    assert row["identity_value"] == HEG_ISIN
    drops = [
        row
        for row in load_csv(ROOT / "data/review_overrides/drop_entries.csv")
        if row.get("ticker") == "HEG" and row.get("exchange") == "NSE_IN"
    ]
    assert len(drops) == 1


def test_sanginita_nse_symbol_change_keeps_isin() -> None:
    row = _transition("NSE_IN::SANGINITA")
    assert row["new_listing_key"] == "NSE_IN::AGASTYAEN"
    assert row["event_type"] == "symbol_changed"
    assert row["identity_type"] == "same_isin"
    assert row["identity_value"] == SANGINITA_ISIN


def test_mka_lse_name_and_isin_follow_issuer_rns() -> None:
    name = _metadata("MKA", "LSE", "name")
    isin = _metadata("MKA", "LSE", "isin")
    assert name["proposed_value"] == "Mkango Magnetic Materials Limited"
    assert isin["proposed_value"] == MKA_ISIN
    assert name["confidence"] == "0.99"
    assert isin["confidence"] == "0.99"
    tsxv_identity = [
        row
        for row in load_csv(ROOT / "data/review_overrides/metadata_updates.csv")
        if row.get("ticker") == "MKA"
        and row.get("exchange") == "TSXV"
        and row.get("field") in {"name", "isin"}
    ]
    assert tsxv_identity == []


def test_jintai_02728_official_isin_after_consolidation() -> None:
    isin = _metadata("02728", "HKEX", "isin")
    name = _metadata("02728", "HKEX", "name")
    assert isin["proposed_value"] == JINTAI_ISIN
    assert name["proposed_value"] == "JINTAI ENGY-NEW"


def test_tsxv_mka_stale_tmx_name_is_allowlisted() -> None:
    rows = [
        row
        for row in load_csv(ROOT / "data/reports/entry_quality_warn_allowlist.csv")
        if row.get("listing_key") == "TSXV::MKA"
    ]
    assert len(rows) == 1
    assert "official_name_mismatch" in rows[0]["issue_types"]


def test_ray_and_glmd_stay_dropped() -> None:
    transitions = load_csv(ROOT / "data/review_overrides/listing_transitions.csv")
    assert any(row.get("old_listing_key") == "NASDAQ::RAY" for row in transitions)
    assert any(row.get("old_listing_key") == "NASDAQ::GLMD" for row in transitions)
    drops = load_csv(ROOT / "data/review_overrides/drop_entries.csv")
    assert any(row.get("ticker") == "RAY" and row.get("exchange") == "NASDAQ" for row in drops)
    assert any(row.get("ticker") == "GLMD" and row.get("exchange") == "NASDAQ" for row in drops)

from __future__ import annotations

import csv
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
METADATA_UPDATES_CSV = ROOT / "data" / "review_overrides" / "metadata_updates.csv"

ACCEPTED_ISSUER_EQUITY = {
    ("AHBM", "NYSE ARCA"): "https://www.amplifyetfs.com/ahbm/",
    ("ROBX", "NYSE ARCA"): "https://www.amplifyetfs.com/robx/",
    ("XQBT", "NYSE ARCA"): "https://www.amplifyetfs.com/xqbt/",
    ("XWNG", "NYSE ARCA"): "https://www.amplifyetfs.com/xwng/",
    ("KAIT", "NYSE ARCA"): "https://kraneshares.com/etf/kait/",
    ("ZDIS", "NYSE ARCA"): "https://www.virtus.com/assets/files/ai7/virtus_zevenbergen_discovery_growth_etf_fact_sheet_1548.pdf",
    ("ZINN", "NYSE ARCA"): "https://www.virtus.com/products/virtus-zevenbergen-innovative-growth-etf",
    ("CROB", "NASDAQ"): "https://www.defianceetfs.com/wp-content/uploads/funddocs/crob/CROB-SummaryProspectus.pdf",
    ("KSMH", "NASDAQ"): "https://www.sec.gov/Archives/edgar/data/1683471/000089418926022087/ksmhsummary.htm",
}

FAIL_CLOSED_BLANK = {
    ("JULV", "NYSE ARCA"),
    ("BAVA", "NYSE ARCA"),
    ("BHYP", "NYSE ARCA"),
    ("GCLO", "NYSE ARCA"),
    ("OVNI", "Euronext"),
}


def _load_updates() -> list[dict[str, str]]:
    with METADATA_UPDATES_CSV.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def test_accepted_issuer_equity_fills_are_listing_keyed_and_sourced():
    updates = {
        (row["ticker"], row["exchange"]): row
        for row in _load_updates()
        if row.get("field") == "etf_category"
        and (row.get("ticker"), row.get("exchange")) in ACCEPTED_ISSUER_EQUITY
    }

    assert set(updates) == set(ACCEPTED_ISSUER_EQUITY)
    for key, row in updates.items():
        assert row["decision"] == "update"
        assert row["proposed_value"] == "Equity"
        assert ACCEPTED_ISSUER_EQUITY[key] in row["reason"]


def test_kait_isin_comes_from_official_kraneshares_product_page():
    matches = [
        row
        for row in _load_updates()
        if (row.get("ticker"), row.get("exchange"), row.get("field")) == ("KAIT", "NYSE ARCA", "isin")
    ]

    assert len(matches) == 1
    assert matches[0]["decision"] == "update"
    assert matches[0]["proposed_value"] == "US5007671739"
    assert "https://kraneshares.com/etf/kait/" in matches[0]["reason"]


def test_fail_closed_residuals_are_not_filled_without_issuer_taxonomy():
    filled = {
        (row["ticker"], row["exchange"])
        for row in _load_updates()
        if row.get("field") == "etf_category"
        and row.get("decision") == "update"
        and row.get("proposed_value")
    }

    assert not (FAIL_CLOSED_BLANK & filled)

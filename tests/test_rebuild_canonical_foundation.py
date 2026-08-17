from __future__ import annotations

import scripts.rebuild_canonical as canonical


def row(exchange: str, ticker: str, name: str, isin: str = "US0378331005") -> dict[str, str]:
    return {"listing_key": f"{exchange}::{ticker}", "exchange": exchange, "ticker": ticker, "name": name, "asset_type": "Stock", "isin": isin}


def test_module_import_does_not_import_heavy_legacy_exporter() -> None:
    assert canonical._REBUILD_DATASET is None


def test_official_name_family_uses_complete_linkage() -> None:
    assert not canonical._names_form_one_identity(
        ["Alpha Beta Holdings Inc", "Alpha Beta Gamma Inc", "Gamma Delta Systems Inc"],
        "Stock",
    )


def test_official_rename_cannot_contradict_coherent_same_isin_peers(monkeypatch) -> None:
    rows = [
        row("X", "A", "Alpha Power Inc"),
        row("Y", "A1", "Alpha Power PLC"),
    ]
    monkeypatch.setattr(canonical, "_reviewed_name_listing_keys", lambda: set())
    monkeypatch.setattr(canonical, "_official_name_evidence_by_listing_isin", lambda: {
        ("X::A", "US0378331005", "Stock"): ({
            "name": "Beta Foods Inc", "source_key": "x", "reference_scope": "exchange_directory",
            "source_url": "https://example.test", "observation_id": "obs",
        },)
    })
    result = canonical.reconcile_exact_official_names(rows, apply_updates=True)
    assert result[0]["name"] == "Alpha Power Inc"


def test_compatible_official_rename_is_recorded(monkeypatch) -> None:
    canonical._NAME_RECONCILIATIONS.clear()
    rows = [row("X", "A", "Legacy Placeholder Holdings Inc")]
    monkeypatch.setattr(canonical, "_reviewed_name_listing_keys", lambda: set())
    monkeypatch.setattr(canonical, "_official_name_evidence_by_listing_isin", lambda: {
        ("X::A", "US0378331005", "Stock"): ({
            "name": "Alpha Power Inc", "source_key": "x", "reference_scope": "exchange_directory",
            "source_url": "https://example.test", "observation_id": "obs",
        },)
    })
    result = canonical.reconcile_exact_official_names(rows)
    assert result[0]["name"] == "Legacy Placeholder Holdings Inc"
    assert canonical._NAME_RECONCILIATIONS[0]["action"] == "proposed"
    applied = canonical.reconcile_exact_official_names(rows, apply_updates=True)
    assert applied[0]["name"] == "Alpha Power Inc"
    assert canonical._NAME_RECONCILIATIONS[-1]["observation_id"] == "obs"

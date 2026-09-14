from __future__ import annotations

from scripts.apply_official_identity_updates import build_official_isin_updates
from scripts.lib.dataio import load_csv


ALNRG_DIFF = {
    "changed": [
        {
            "source_key": "euronext_equities",
            "source_url": "https://live.euronext.com/example",
            "exchange": "Euronext",
            "ticker": "ALNRG",
            "changes": {"isin": {"before": "FR0013399359", "after": "FR001401A702"}},
        }
    ]
}

EASYKNIT_DIFF = {
    "changed": [
        {
            "source_key": "hkex_securities_list",
            "exchange": "HKEX",
            "ticker": "01218",
            "changes": {
                "name": {"before": "EASYKNIT INT'L", "after": "EASYKNIT-NEW"},
                "isin": {"before": "BMG2915Q3296", "after": "BMG2915Q3452"},
            },
        }
    ]
}


def test_applies_same_identity_official_isin_replacement() -> None:
    updates = build_official_isin_updates(
        ALNRG_DIFF,
        [
            {
                "listing_key": "Euronext::ALNRG",
                "ticker": "ALNRG",
                "exchange": "Euronext",
                "name": "Energisme",
                "isin": "FR0013399359",
            }
        ],
        [
            {
                "exchange": "Euronext",
                "ticker": "ALNRG",
                "name": "ENERGISME",
                "listing_status": "active",
                "official": "true",
                "reference_scope": "exchange_directory",
                "source_key": "euronext_equities",
                "isin": "FR001401A702",
            }
        ],
    )

    assert len(updates) == 1
    assert updates[0]["ticker"] == "ALNRG"
    assert updates[0]["exchange"] == "Euronext"
    assert updates[0]["proposed_value"] == "FR001401A702"
    assert "FR0013399359" in updates[0]["reason"]


def test_skips_directory_only_official_isin_change() -> None:
    updates = build_official_isin_updates(
        {
            "changed": [
                {
                    "source_key": "euronext_equities",
                    "exchange": "Euronext",
                    "ticker": "1OKE",
                    "changes": {"isin": {"before": "US6826801036", "after": "US30609A1097"}},
                }
            ]
        },
        [],
        [{"exchange": "Euronext", "ticker": "1OKE", "name": "ONEOK", "listing_status": "active"}],
    )

    assert updates == []


def test_skips_official_isin_change_when_name_no_longer_matches() -> None:
    updates = build_official_isin_updates(
        EASYKNIT_DIFF,
        [
            {
                "listing_key": "HKEX::01218",
                "ticker": "01218",
                "exchange": "HKEX",
                "name": "EASYKNIT INT'L",
                "isin": "BMG2915Q3296",
            }
        ],
        [
            {
                "exchange": "HKEX",
                "ticker": "01218",
                "name": "EASYKNIT-NEW",
                "listing_status": "active",
                "isin": "BMG2915Q3452",
            }
        ],
    )

    assert updates == []


def test_skips_when_reference_row_is_not_official_directory() -> None:
    updates = build_official_isin_updates(
        ALNRG_DIFF,
        [
            {
                "listing_key": "Euronext::ALNRG",
                "ticker": "ALNRG",
                "exchange": "Euronext",
                "name": "Energisme",
                "isin": "FR0013399359",
            }
        ],
        [
            {
                "exchange": "Euronext",
                "ticker": "ALNRG",
                "name": "ENERGISME",
                "listing_status": "active",
                "official": "false",
                "reference_scope": "exchange_directory",
                "source_key": "euronext_equities",
                "isin": "FR001401A702",
            }
        ],
    )

    assert updates == []


def test_skips_when_listing_isin_does_not_match_official_before() -> None:
    updates = build_official_isin_updates(
        ALNRG_DIFF,
        [
            {
                "listing_key": "Euronext::ALNRG",
                "ticker": "ALNRG",
                "exchange": "Euronext",
                "name": "Energisme",
                "isin": "FR0000000000",
            }
        ],
        [
            {
                "exchange": "Euronext",
                "ticker": "ALNRG",
                "name": "ENERGISME",
                "listing_status": "active",
            }
        ],
    )

    assert updates == []


def test_execute_merges_metadata_updates(tmp_path) -> None:
    from scripts.apply_official_identity_updates import apply_official_identity_updates

    path = tmp_path / "metadata_updates.csv"
    path.write_text(
        "ticker,exchange,field,decision,proposed_value,confidence,reason\n",
        encoding="utf-8",
    )
    result = apply_official_identity_updates(
        rotation_diff=ALNRG_DIFF,
        listings=[
            {
                "listing_key": "Euronext::ALNRG",
                "ticker": "ALNRG",
                "exchange": "Euronext",
                "name": "Energisme",
                "isin": "FR0013399359",
            }
        ],
        reference_rows=[
            {
                "exchange": "Euronext",
                "ticker": "ALNRG",
                "name": "ENERGISME",
                "listing_status": "active",
                "official": "true",
                "reference_scope": "exchange_directory",
                "source_key": "euronext_equities",
                "isin": "FR001401A702",
            }
        ],
        metadata_updates_path=path,
        execute=True,
    )

    assert result["executed"] is True
    rows = load_csv(path)
    assert rows[0]["proposed_value"] == "FR001401A702"

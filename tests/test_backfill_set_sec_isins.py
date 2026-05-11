from __future__ import annotations

from scripts.backfill_set_sec_isins import build_metadata_updates, evaluate_row, parse_sec_isin_rows


def test_parse_sec_isin_rows_extracts_local_isin() -> None:
    html = """
    <table>
      <tr><th>ประเภทหลักทรัพย์</th><th>ชื่อหลักทรัพย์</th><th>ชื่อย่อหลักทรัพย์</th><th>ISIN Local</th><th>ISIN Foreign</th><th>ISIN NVDR</th></tr>
      <tr><td>หุ้นสามัญ</td><td>บริษัท ท่าอากาศยานไทย จำกัด (มหาชน)</td><td>AOT</td><td>TH0765010Z08</td><td></td><td></td></tr>
    </table>
    """

    assert parse_sec_isin_rows(html) == [
        {
            "sec_security_type": "หุ้นสามัญ",
            "sec_name": "บริษัท ท่าอากาศยานไทย จำกัด (มหาชน)",
            "sec_symbol": "AOT",
            "sec_isin_local": "TH0765010Z08",
        }
    ]


def test_evaluate_row_accepts_single_valid_exact_symbol_match() -> None:
    target = {"ticker": "AOT", "exchange": "SET", "asset_type": "Stock", "name": "Airports of Thailand Public Company Limited"}

    result = evaluate_row(
        target,
        [
            {
                "sec_security_type": "หุ้นสามัญ",
                "sec_name": "บริษัท ท่าอากาศยานไทย จำกัด (มหาชน)",
                "sec_symbol": "AOT",
                "sec_isin_local": "TH0765010Z08",
            }
        ],
    )

    assert result["decision"] == "accept"
    assert result["sec_isin_local"] == "TH0765010Z08"
    assert build_metadata_updates([result])[0]["proposed_value"] == "TH0765010Z08"


def test_evaluate_row_rejects_missing_ambiguous_or_invalid_matches() -> None:
    target = {"ticker": "AOT", "exchange": "SET", "asset_type": "Stock", "name": "Airports of Thailand Public Company Limited"}

    assert evaluate_row(target, [])["decision"] == "no_sec_isin_match"
    assert (
        evaluate_row(
            target,
            [
                {"sec_symbol": "AOT", "sec_isin_local": "TH0765010Z08"},
                {"sec_symbol": "AOT", "sec_isin_local": "TH0000000000"},
            ],
        )["decision"]
        == "ambiguous_sec_isin"
    )
    assert evaluate_row(target, [{"sec_symbol": "AOT", "sec_isin_local": "US0378331005"}])["decision"] == "invalid_or_non_th_isin"

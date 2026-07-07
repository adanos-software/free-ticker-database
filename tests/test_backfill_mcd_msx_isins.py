from __future__ import annotations

from scripts.backfill_mcd_msx_isins import (
    build_metadata_updates,
    evaluate_rows,
    name_token_subset_match,
    parse_mcd_issuer_registration_rows,
    parse_mcd_listed_instrument_rows,
)


def mcd_html() -> str:
    return """
    <script>
    new DevExpress.data.ArrayStore({"data":[
      {"ISINCode":"OM0000006755","SecuritySymbol":"BWRQ","SecurityName":"BAWAREQ NIZWA","SecurityType":"BOND","MarketType":"Third Market","Sector":"Services"},
      {"ISINCode":"OM0000000000","SecuritySymbol":"BAD","SecurityName":"BAD ISIN COMPANY","SecurityType":"Public Company (SAOG)","MarketType":"Parallel Market","Sector":"Services"},
      {"ISINCode":"QA0006929895","SecuritySymbol":"FOREIGN","SecurityName":"FOREIGN ISIN COMPANY","SecurityType":"Public Company (SAOG)","MarketType":"Parallel Market","Sector":"Services"}
    ]})
    </script>
    """


def issuer_registration_html() -> str:
    return """
    <select id="ISIN" name="ISIN">
      <option value="">---Select---</option>
      <option value="OM0000006755">BAWAREQ NIZWA INTERNATIONAL FOR INVESTMENT SAOC</option>
    </select>
    """


def test_parse_mcd_listed_instrument_rows_extracts_instrument_grid() -> None:
    rows = parse_mcd_listed_instrument_rows(mcd_html())

    assert rows[0] == {
        "mcd_security_symbol": "BWRQ",
        "mcd_security_name": "BAWAREQ NIZWA",
        "mcd_security_type": "BOND",
        "mcd_market_type": "Third Market",
        "mcd_sector": "Services",
        "mcd_isin": "OM0000006755",
        "source_url": "https://www.mcd.om/en/Default/Statistic/ListedInstrumentsInfo",
    }


def test_parse_mcd_issuer_registration_rows_extracts_isin_options() -> None:
    assert parse_mcd_issuer_registration_rows(issuer_registration_html()) == [
        {
            "mcd_isin": "OM0000006755",
            "mcd_issuer_name": "BAWAREQ NIZWA INTERNATIONAL FOR INVESTMENT SAOC",
        }
    ]


def test_name_token_subset_match_allows_short_dataset_name_inside_legal_name() -> None:
    assert name_token_subset_match("BAWAREQ NIZWA INTERNATIONAL FOR INVESTMENT", "BAWAREQ NIZWA")
    assert not name_token_subset_match("BAWAREQ NIZWA INTERNATIONAL FOR INVESTMENT", "Other Company")


def test_evaluate_rows_accepts_exact_symbol_name_subset_valid_om_isin() -> None:
    mcd_rows = parse_mcd_listed_instrument_rows(mcd_html())
    issuer_rows = parse_mcd_issuer_registration_rows(issuer_registration_html())
    results = evaluate_rows(
        [
            {
                "ticker": "BWRQ",
                "exchange": "MSX",
                "asset_type": "Stock",
                "name": "BAWAREQ NIZWA",
                "isin": "",
            },
            {
                "ticker": "BAD",
                "exchange": "MSX",
                "asset_type": "Stock",
                "name": "BAD ISIN COMPANY",
                "isin": "",
            },
            {
                "ticker": "FOREIGN",
                "exchange": "MSX",
                "asset_type": "Stock",
                "name": "FOREIGN ISIN COMPANY",
                "isin": "",
            },
        ],
        mcd_rows,
        issuer_rows,
    )

    assert results[0]["decision"] == "accept"
    assert results[0]["mcd_isin"] == "OM0000006755"
    assert "symbol_exact_match=true" in results[0]["identity_gate_context"]
    assert "valid_isin_checksum=true" in results[0]["identity_gate_context"]
    assert "issuer_registration_match=true" in results[0]["identity_gate_context"]
    assert "name_token_subset_match=true" in results[0]["identity_gate_context"]
    assert results[1]["decision"] == "invalid_or_non_om_isin"
    assert results[2]["decision"] == "invalid_or_non_om_isin"


def test_evaluate_rows_rejects_bond_type_conflict_without_issuer_confirmation() -> None:
    mcd_rows = parse_mcd_listed_instrument_rows(mcd_html())
    [result] = evaluate_rows(
        [
            {
                "ticker": "BWRQ",
                "exchange": "MSX",
                "asset_type": "Stock",
                "name": "BAWAREQ NIZWA",
                "isin": "",
            },
        ],
        mcd_rows,
    )

    assert result["decision"] == "security_type_mismatch"


def test_build_metadata_updates_uses_official_mcd_reason() -> None:
    updates = build_metadata_updates(
        [
            {
                "ticker": "BWRQ",
                "exchange": "MSX",
                "decision": "accept",
                "mcd_isin": "OM0000006755",
                "source_url": "https://example.test/mcd",
            },
            {
                "ticker": "BAD",
                "exchange": "MSX",
                "decision": "invalid_or_non_om_isin",
                "mcd_isin": "OM0000000000",
                "source_url": "https://example.test/mcd",
            },
        ]
    )

    assert updates == [
        {
            "ticker": "BWRQ",
            "exchange": "MSX",
            "field": "isin",
            "decision": "update",
            "proposed_value": "OM0000006755",
            "confidence": "0.97",
            "reason": (
                "Official Muscat Clearing & Depository ListedInstrumentsInfo supplied a valid OM ISIN "
                "for the exact MSX security symbol; accepted only after exact symbol, instrument/issuer-name token subset, "
                "OM country prefix, and ISIN checksum gates matched. Source: https://example.test/mcd"
            ),
        }
    ]

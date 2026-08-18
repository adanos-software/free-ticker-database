from __future__ import annotations

from scripts.fetch_exchange_masterfiles import (
    MasterfileSource,
    parse_deutsche_boerse_frankfurt_all_tradable_csv,
    parse_deutsche_boerse_xetra_all_tradable_csv,
)


XETRA_SOURCE = MasterfileSource(
    key="deutsche_boerse_xetra_all_tradable_equities",
    provider="Deutsche Boerse",
    description="xetra",
    source_url="https://example.test/xetra.csv",
    format="deutsche_boerse_xetra_all_tradable_csv",
)
FRANKFURT_SOURCE = MasterfileSource(
    key="deutsche_boerse_frankfurt_all_tradable_equities",
    provider="Deutsche Boerse",
    description="frankfurt",
    source_url="https://example.test/xfra.csv",
    format="deutsche_boerse_frankfurt_all_tradable_csv",
)

SAMPLE = """Market:;XFRA
Date Last Update:;18.08.2026
Product Status;Instrument Status;Instrument;ISIN;Mnemonic;MIC Code;Instrument Type;Product Assignment Group Description
Active;Active;APPLE INC.;US0378331005;APC;XFRA;CS;
Active;Active;ISHARES CORE DAX UCITS ETF;DE0005933931;EXS1;XFRA;ETF;Equity Germany
Active;Active;ISHARES BOND ETF;IE00B3F81R35;IBGX;XFRA;ETF;ETF Renten
Active;Active;BOND SHOULD DROP;DE0001102309;;XFRA;BOND;
Active;Inactive;INACTIVE SHARE;DE000A0D9PT0;DEAD;XFRA;CS;
Active;Active;XETRA ONLY SHARE;DE0007164600;SAP;XETR;CS;
"""


def test_frankfurt_parser_keeps_active_equity_and_etf_mnemonics() -> None:
    rows = parse_deutsche_boerse_frankfurt_all_tradable_csv(SAMPLE, FRANKFURT_SOURCE)
    assert {(row["ticker"], row["exchange"], row["asset_type"]) for row in rows} == {
        ("APC", "FSX", "Stock"),
        ("EXS1", "FSX", "ETF"),
        ("IBGX", "FSX", "ETF"),
    }
    by_ticker = {row["ticker"]: row for row in rows}
    assert by_ticker["APC"]["isin"] == "US0378331005"
    assert "sector" not in by_ticker["EXS1"]
    assert by_ticker["IBGX"]["sector"] == "Fixed Income"


def test_xetra_parser_ignores_frankfurt_mic_rows() -> None:
    rows = parse_deutsche_boerse_xetra_all_tradable_csv(SAMPLE, XETRA_SOURCE)
    assert [(row["ticker"], row["exchange"]) for row in rows] == [("SAP", "XETRA")]

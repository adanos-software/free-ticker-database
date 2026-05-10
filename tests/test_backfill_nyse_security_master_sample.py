from __future__ import annotations

import csv

from scripts.backfill_nyse_security_master_sample import (
    NyseSecurityMasterRow,
    build_metadata_updates,
    evaluate_row,
    load_target_rows,
    parse_security_master_line,
    parse_security_master_text,
)


def make_line(
    *,
    symbol: str = "AAA",
    isin: str = "US46144X6105",
    issuer: str = "Investment Managers Series Trust II",
    issue: str = "Alternative Access First Priority CLO Bond ETF",
    instrument_type: str = "ETF",
    asset_class: str = "FIXED_INCOME",
    strategy: str = "ACTIVE",
) -> str:
    fields = [""] * 130
    fields[1] = symbol
    fields[4] = isin
    fields[5] = issuer
    fields[6] = issue
    fields[8] = instrument_type
    fields[95] = asset_class
    fields[96] = strategy
    return "|".join(fields)


def test_parse_security_master_line_reads_required_fields():
    assert parse_security_master_line(make_line()) == NyseSecurityMasterRow(
        symbol="AAA",
        issuer_name="Investment Managers Series Trust II",
        issue_name="Alternative Access First Priority CLO Bond ETF",
        instrument_type="ETF",
        isin="US46144X6105",
        asset_class="FIXED_INCOME",
        strategy="ACTIVE",
    )


def test_parse_security_master_line_rejects_short_and_invalid_isin():
    assert parse_security_master_line("AAA|too|short") is None
    assert parse_security_master_line(make_line(isin="US46144X6100")) is None


def test_parse_security_master_text_filters_invalid_rows():
    assert parse_security_master_text(make_line() + "\n" + make_line(isin="US46144X6100")) == [
        NyseSecurityMasterRow(
            symbol="AAA",
            issuer_name="Investment Managers Series Trust II",
            issue_name="Alternative Access First Priority CLO Bond ETF",
            instrument_type="ETF",
            isin="US46144X6105",
            asset_class="FIXED_INCOME",
            strategy="ACTIVE",
        )
    ]


def test_load_target_rows_filters_us_missing_isin_or_etf_category(tmp_path):
    path = tmp_path / "tickers.csv"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["ticker", "exchange", "asset_type", "name", "isin", "etf_category"])
        writer.writeheader()
        writer.writerows(
            [
                {"ticker": "AAA", "exchange": "NYSE ARCA", "asset_type": "ETF", "name": "AAA ETF", "isin": "", "etf_category": ""},
                {"ticker": "A", "exchange": "NYSE", "asset_type": "Stock", "name": "Agilent", "isin": "", "etf_category": ""},
                {"ticker": "BHP", "exchange": "ASX", "asset_type": "Stock", "name": "BHP", "isin": "", "etf_category": ""},
                {"ticker": "SPY", "exchange": "NYSE ARCA", "asset_type": "ETF", "name": "SPDR", "isin": "US78462F1030", "etf_category": "Equity"},
            ]
        )

    assert [row["ticker"] for row in load_target_rows(path)] == ["AAA", "A"]


def test_evaluate_row_accepts_isin_and_category_after_name_gate():
    result = evaluate_row(
        {
            "ticker": "AAA",
            "exchange": "NYSE ARCA",
            "asset_type": "ETF",
            "name": "Alternative Access First Priority CLO Bond ETF",
            "isin": "",
            "etf_category": "",
        },
        [
            NyseSecurityMasterRow(
                "AAA",
                "Investment Managers Series Trust II",
                "Alternative Access First Priority CLO Bond ETF",
                "ETF",
                "US46144X6105",
                "FIXED_INCOME",
                "ACTIVE",
            )
        ],
    )

    assert result["decision"] == "accept_isin_etf_category"
    assert result["isin_update"] == "US46144X6105"
    assert result["category_update"] == "Fixed Income"


def test_evaluate_row_rejects_missing_name_match_and_unknown_category():
    target = {
        "ticker": "AAA",
        "exchange": "NYSE ARCA",
        "asset_type": "ETF",
        "name": "Different ETF",
        "isin": "",
        "etf_category": "",
    }
    source = NyseSecurityMasterRow("AAA", "Issuer", "Alternative Access First Priority CLO Bond ETF", "ETF", "US46144X6105", "", "")
    assert evaluate_row(target, [source])["decision"] == "name_mismatch"


def test_build_metadata_updates_emits_reviewed_fields():
    updates = build_metadata_updates(
        [
            {
                "decision": "accept_isin_etf_category",
                "ticker": "AAA",
                "exchange": "NYSE ARCA",
                "isin_update": "US46144X6105",
                "category_update": "Fixed Income",
            }
        ],
        "https://example.com/sample.txt",
    )

    assert [update["field"] for update in updates] == ["isin", "etf_category"]
    assert updates[0]["proposed_value"] == "US46144X6105"
    assert updates[1]["proposed_value"] == "Fixed Income"

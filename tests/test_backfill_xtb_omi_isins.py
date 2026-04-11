from __future__ import annotations

import csv

from scripts.backfill_xtb_omi_isins import (
    XtbInstrument,
    build_metadata_updates,
    evaluate_xtb_instrument,
    load_missing_isin_rows,
    names_match,
    parse_args,
    parse_toc_page_ranges,
    parse_xtb_omi_page_texts,
)


def test_parse_toc_page_ranges_reads_current_xtb_table_of_contents():
    ranges = parse_toc_page_ranges(
        "\n".join(
            [
                "Specification Table Organised Market Instruments (OMI) page 2",
                "Stocks page 2",
                "ETF, ETN, ETC page 93",
                "Explanatory notes to Specification Table Fractional Rights page 1",
                "Specification Table Fractional Rights page 116",
            ]
        )
    )

    assert ranges.stocks_start == 2
    assert ranges.etf_start == 93
    assert ranges.fractional_start == 116


def test_parse_xtb_omi_page_texts_parses_stocks_and_etfs_but_skips_fractionals():
    page_texts = [
        "\n".join(
            [
                "Stocks page 2",
                "ETF, ETN, ETC page 3",
                "Specification Table Fractional Rights page 4",
            ]
        ),
        "Symbol Company Name ISIN Currency transaction Trading Hours Trading Days\n"
        "AGL.US agilon health inc (Cboe BZX Real-Time Quote) US00857U1079 USD 1 USD 15:30 - 22:00 Monday - Friday",
        "IVV.US iShares Core S&P 500 ETF US4642872000 USD 1 USD 15:30 - 22:00 Monday - Friday",
        "AAPL.US Apple Inc US0378331005 USD 1 USD 15:30 - 22:00 Monday - Friday",
    ]

    instruments = parse_xtb_omi_page_texts(page_texts)

    assert instruments == [
        XtbInstrument(
            symbol="AGL.US",
            base_ticker="AGL",
            suffix="US",
            name="agilon health inc (Cboe BZX Real-Time Quote)",
            isin="US00857U1079",
            currency="USD",
            asset_type="Stock",
            page=2,
        ),
        XtbInstrument(
            symbol="IVV.US",
            base_ticker="IVV",
            suffix="US",
            name="iShares Core S&P 500 ETF",
            isin="US4642872000",
            currency="USD",
            asset_type="ETF",
            page=3,
        ),
    ]


def test_load_missing_isin_rows_filters_only_rows_without_isin(tmp_path):
    path = tmp_path / "tickers.csv"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["ticker", "exchange", "asset_type", "name", "isin"])
        writer.writeheader()
        writer.writerows(
            [
                {"ticker": "AGL", "exchange": "NYSE", "asset_type": "Stock", "name": "agilon health Inc", "isin": ""},
                {"ticker": "AAPL", "exchange": "NASDAQ", "asset_type": "Stock", "name": "Apple Inc.", "isin": "US0378331005"},
            ]
        )

    assert [row["ticker"] for row in load_missing_isin_rows(path)] == ["AGL"]


def test_names_match_allows_broker_suffix_noise_and_rejects_ticker_collisions():
    assert names_match(
        "agilon health inc (Cboe BZX Real-Time Quote)",
        "agilon health Inc",
    )
    assert names_match("Antero Midstream Corp", "Antero Midstream Partners LP")
    assert not names_match("Credit Agricole SA", "Arcosa Inc")
    assert not names_match("AB Science SA", "AllianceBernstein Holding L.P.")


def test_evaluate_xtb_instrument_accepts_suffix_exchange_asset_and_name_match():
    instrument = XtbInstrument(
        symbol="AGL.US",
        base_ticker="AGL",
        suffix="US",
        name="agilon health inc (Cboe BZX Real-Time Quote)",
        isin="US00857U1079",
        currency="USD",
        asset_type="Stock",
        page=2,
    )
    rows = {
        ("AGL", "NYSE"): [
            {"ticker": "AGL", "exchange": "NYSE", "asset_type": "Stock", "name": "agilon health Inc", "isin": ""}
        ]
    }

    result = evaluate_xtb_instrument(instrument, rows)

    assert result["decision"] == "accept"
    assert result["target_exchange"] == "NYSE"
    assert result["xtb_isin"] == "US00857U1079"


def test_evaluate_xtb_instrument_rejects_wrong_suffix_asset_or_name():
    rows = {
        ("AB", "NYSE"): [
            {"ticker": "AB", "exchange": "NYSE", "asset_type": "Stock", "name": "AllianceBernstein Holding L.P.", "isin": ""}
        ],
        ("IVV", "NASDAQ"): [
            {"ticker": "IVV", "exchange": "NASDAQ", "asset_type": "Stock", "name": "iShares Core S&P 500 ETF", "isin": ""}
        ],
        ("ACA", "NYSE"): [
            {"ticker": "ACA", "exchange": "NYSE", "asset_type": "Stock", "name": "Arcosa Inc", "isin": ""}
        ],
    }

    assert (
        evaluate_xtb_instrument(
            XtbInstrument("AB.FR", "AB", "FR", "AB Science SA", "FR0010557264", "EUR", "Stock", 2),
            rows,
        )["decision"]
        == "no_missing_isin_match"
    )
    assert (
        evaluate_xtb_instrument(
            XtbInstrument("IVV.US", "IVV", "US", "iShares Core S&P 500 ETF", "US4642872000", "USD", "ETF", 3),
            rows,
        )["decision"]
        == "asset_type_mismatch"
    )
    assert (
        evaluate_xtb_instrument(
            XtbInstrument("ACA.US", "ACA", "US", "Credit Agricole SA", "FR0000045072", "USD", "Stock", 3),
            rows,
        )["decision"]
        == "name_mismatch"
    )


def test_build_metadata_updates_emits_review_override_rows():
    updates = build_metadata_updates(
        [
            {
                "decision": "accept",
                "target_ticker": "AGL",
                "target_exchange": "NYSE",
                "xtb_isin": "US00857U1079",
                "xtb_symbol": "AGL.US",
            },
            {
                "decision": "name_mismatch",
                "target_ticker": "ACA",
                "target_exchange": "NYSE",
                "xtb_isin": "FR0000045072",
                "xtb_symbol": "ACA.US",
            },
        ]
    )

    assert updates == [
        {
            "ticker": "AGL",
            "exchange": "NYSE",
            "field": "isin",
            "decision": "update",
            "proposed_value": "US00857U1079",
            "confidence": "0.82",
            "reason": "XTB OMI specification table lists AGL.US with this valid ISIN; accepted only after ticker suffix/exchange, asset type, and issuer-name gates matched an existing row without ISIN.",
        }
    ]


def test_parse_args_supports_local_pdf_and_apply():
    args = parse_args(["--pdf-path", "omi.pdf", "--suffix", "US", "--apply", "--limit", "10"])

    assert str(args.pdf_path) == "omi.pdf"
    assert args.suffix == ["US"]
    assert args.apply is True
    assert args.limit == 10

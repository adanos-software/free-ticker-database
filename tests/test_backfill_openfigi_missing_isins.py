import csv

from scripts.backfill_openfigi_missing_isins import evaluate_candidate, load_missing_rows, verify_rows, write_report_csv


def test_evaluate_candidate_accepts_strict_ticker_name_and_isin_match():
    row = {
        "ticker": "BTCW",
        "exchange": "BATS",
        "asset_type": "ETF",
        "name": "WisdomTree Bitcoin Fund",
    }
    result = evaluate_candidate(
        row,
        "US",
        [
            {
                "ticker": "BTCW",
                "name": "WisdomTree Bitcoin Fund",
                "securityType": "ETP",
                "idIsin": "US97720F1012",
            }
        ],
    )

    assert result["decision"] == "accept"
    assert result["openfigi_isin"] == "US97720F1012"


def test_evaluate_candidate_rejects_name_mismatch():
    row = {
        "ticker": "BTCW",
        "exchange": "BATS",
        "asset_type": "ETF",
        "name": "WisdomTree Bitcoin Fund",
    }
    result = evaluate_candidate(
        row,
        "US",
        [
            {
                "ticker": "BTCW",
                "name": "Different Bitcoin Product",
                "securityType": "ETP",
                "idIsin": "US97720F1012",
            }
        ],
    )

    assert result["decision"] == "name_mismatch"


def test_evaluate_candidate_rejects_wrong_country_prefix():
    row = {
        "ticker": "ABC",
        "exchange": "TSX",
        "asset_type": "Stock",
        "name": "ABC Corp",
    }
    result = evaluate_candidate(
        row,
        "CN",
        [
            {
                "ticker": "ABC",
                "name": "ABC Corp",
                "securityType": "Common Stock",
                "idIsin": "US0028241000",
            }
        ],
    )

    assert result["decision"] == "isin_country_mismatch"


def test_write_report_csv_uses_lf_line_endings(tmp_path):
    path = tmp_path / "missing_isin_backfill.csv"

    write_report_csv(
        path,
        [
            {
                "ticker": "BTCW",
                "exchange": "BATS",
                "asset_type": "ETF",
                "name": "WisdomTree Bitcoin Fund",
                "figi_exch_code": "US",
                "openfigi_ticker": "BTCW",
                "openfigi_name": "WisdomTree Bitcoin Fund",
                "openfigi_security_type": "ETP",
                "openfigi_isin": "US97720F1012",
                "number_tokens_match": True,
                "name_match": True,
                "decision": "accept",
            }
        ],
    )

    content = path.read_bytes()
    assert b"\r\n" not in content
    assert content.endswith(b"\n")


def test_load_missing_rows_keeps_unsupported_exchanges_for_reporting(tmp_path):
    path = tmp_path / "tickers.csv"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["ticker", "exchange", "asset_type", "name", "isin"])
        writer.writeheader()
        writer.writerow({"ticker": "QNBK", "exchange": "QSE", "asset_type": "Stock", "name": "QNB", "isin": ""})

    rows = load_missing_rows(path, {"QSE"}, {"Stock"})

    assert [row["ticker"] for row in rows] == ["QNBK"]


def test_verify_rows_classifies_unsupported_openfigi_exchange_without_api_call():
    result = verify_rows(
        [{"ticker": "QNBK", "exchange": "QSE", "asset_type": "Stock", "name": "QNB"}],
        api_key="",
        batch_size=10,
        delay_seconds=0,
        timeout_seconds=1,
    )

    assert result == [
        {
            "ticker": "QNBK",
            "exchange": "QSE",
            "asset_type": "Stock",
            "name": "QNB",
            "figi_exch_code": "",
            "openfigi_ticker": "",
            "openfigi_name": "",
            "openfigi_security_type": "",
            "openfigi_isin": "",
            "number_tokens_match": False,
            "name_match": False,
            "decision": "unsupported_openfigi_exchange",
        }
    ]

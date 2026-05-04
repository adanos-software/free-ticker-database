from scripts.backfill_openfigi_missing_isins import evaluate_candidate


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

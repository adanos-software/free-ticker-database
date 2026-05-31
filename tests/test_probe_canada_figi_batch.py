from scripts.probe_canada_figi_batch import build_probe_rows, select_candidate, select_queue_rows, summarize


def test_select_queue_rows_filters_batch_and_limit() -> None:
    rows = [
        {"batch_id": "canada-figi-0001", "batch_position": "2", "listing_key": "TSX::B"},
        {"batch_id": "canada-figi-0001", "batch_position": "1", "listing_key": "TSX::A"},
        {"batch_id": "canada-figi-0002", "batch_position": "1", "listing_key": "TSX::C"},
    ]

    assert [row["listing_key"] for row in select_queue_rows(rows, "canada-figi-0001", 1)] == ["TSX::A"]


def test_select_candidate_prefers_hint_and_ticker() -> None:
    row = {"ticker": "AEMX", "openfigi_exchange_hint": "CN"}
    candidates = [
        {"ticker": "AEMX", "exchCode": "US", "figi": "USFIGI"},
        {"ticker": "AEMX", "exchCode": "CN", "figi": "CNFIGI"},
    ]

    assert select_candidate(row, candidates) == {"ticker": "AEMX", "exchCode": "CN", "figi": "CNFIGI"}


def test_build_probe_rows_accepts_only_requested_exchange_hint() -> None:
    rows = build_probe_rows(
        [
            {
                "batch_id": "canada-figi-0001",
                "listing_key": "NEO::AEMX",
                "ticker": "AEMX",
                "exchange": "NEO",
                "asset_type": "ETF",
                "name": "AGF Emerging Markets ex China Fund Series ETF",
                "isin": "CA00858F1099",
                "openfigi_exchange_hint": "CN",
            },
            {
                "batch_id": "canada-figi-0001",
                "listing_key": "NEO::MISS",
                "ticker": "MISS",
                "exchange": "NEO",
                "asset_type": "ETF",
                "name": "Missing ETF",
                "isin": "CA0000000001",
                "openfigi_exchange_hint": "CN",
            },
        ],
        {
            "CA00858F1099": [
                {
                    "ticker": "AEMX",
                    "exchCode": "CN",
                    "figi": "BBGFIGI",
                    "name": "AGF Emerging Markets ex China Fund Series ETF",
                    "securityType": "ETP",
                }
            ],
            "CA0000000001": [],
        },
    )

    assert rows[0]["decision"] == "accept"
    assert rows[0]["openfigi_figi"] == "BBGFIGI"
    assert rows[1]["decision"] == "no_openfigi_match"


def test_summarize_counts_probe_decisions() -> None:
    summary = summarize(
        [
            {"decision": "accept", "exchange": "NEO", "asset_type": "ETF"},
            {"decision": "no_openfigi_match", "exchange": "TSX", "asset_type": "Stock"},
        ],
        "2026-05-24T00:00:00Z",
        errors=["rate limited"],
    )

    assert summary["rows"] == 2
    assert summary["accepted_rows"] == 1
    assert summary["decision_totals"] == {"accept": 1, "no_openfigi_match": 1}
    assert summary["errors"] == ["rate limited"]

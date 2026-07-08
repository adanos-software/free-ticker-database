"""Unit tests for the OpenFIGI ISIN-validation report (pure logic, no network)."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.build_isin_validation_report import (
    norm_ticker,
    name_tokens,
    classify,
    build_residual_triage,
    compute_detected,
    enrich_mismatch,
    row_cache_fingerprint,
    select_valid_cache,
)


def test_norm_ticker_strips_separators():
    assert norm_ticker("BIO.B") == "BIOB"
    assert norm_ticker(" brk-b ") == "BRKB"
    assert norm_ticker(None) == ""


def test_norm_ticker_strips_leading_zeros_for_asian_codes():
    # HKEX zero-pads (00001, 00700); OpenFIGI returns them unpadded.
    assert norm_ticker("00001") == norm_ticker("1") == "1"
    assert norm_ticker("00700") == "700"


def test_norm_ticker_strips_lse_slash():
    assert norm_ticker("RM") == norm_ticker("RM/") == norm_ticker("RM/EUR".replace("EUR", "")) == "RM"


def test_name_tokens_fold_diacritics():
    # Frøy / Energía / Sanistål must match their ASCII OpenFIGI spellings.
    assert "froy" in name_tokens("Frøy Asa")
    assert "energia" in name_tokens("Energía Innovación")
    assert "sanistal" in name_tokens("Sanistål A/S")


def test_classify_match_across_diacritics_and_slash():
    assert classify("FROY", "Frøy Asa", [{"ticker": "FROY", "name": "FROY ASA"}]) == "match"
    assert classify("RM", "Regional", [{"ticker": "RM/", "name": "RM PLC"}]) == "match"


def test_classify_match_on_zero_padded_hkex_code():
    recs = [{"ticker": "1", "name": "CK HUTCHISON HOLDINGS LTD"}]
    assert classify("00001", "CKH HOLDINGS", recs) == "match"


def test_name_tokens_drops_stopwords_and_short_words():
    toks = name_tokens("Apple Inc. Common Stock")
    assert "apple" in toks
    assert "inc" not in toks and "common" not in toks and "stock" not in toks


def test_classify_no_data_when_figi_empty():
    assert classify("AAPL", "Apple Inc", None) == "no_data"
    assert classify("AAPL", "Apple Inc", []) == "no_data"


def test_classify_match_on_ticker():
    recs = [{"ticker": "AAPL", "name": "APPLE INC"}]
    assert classify("AAPL", "Whatever Name", recs) == "match"


def test_classify_match_on_name_token_when_ticker_format_differs():
    # Non-US: OpenFIGI ticker convention differs but the name confirms identity.
    recs = [{"ticker": "7203 JP", "name": "TOYOTA MOTOR CORP"}]
    assert classify("7203", "Toyota Motor Corporation", recs) == "match"


def test_classify_match_via_normname_containment_when_tokens_empty():
    # "LY Corp" tokenizes to nothing (LY<3 chars, Corp is a stopword); the
    # normalized-name containment fallback still recognizes the same security.
    recs = [{"ticker": "LCL", "name": "LY CORP LTD"}]
    assert classify("1H8", "LY Corp", recs) == "match"


def test_classify_mismatch_when_data_but_no_overlap():
    # ISIN resolves to a clearly different security -> wrong/stale ISIN candidate.
    recs = [{"ticker": "ZZZZ", "name": "SOME OTHER COMPANY"}]
    assert classify("AAPL", "Apple Inc", recs) == "mismatch"
    # Real example: a Pakistani stock carrying an Israeli ISIN that maps elsewhere.
    recs2 = [{"ticker": "PEAX", "name": "PEAX SOLUTIONS LTD"}]
    assert classify("EMCO", "Emco Industries Ltd", recs2) == "mismatch"


def test_compute_detected():
    keys = {"US0378331005", "JP3633400001"}
    assert compute_detected(keys, None) is True            # first run, has mismatches
    assert compute_detected(set(), None) is False          # first run, none
    assert compute_detected(keys, keys) is False           # unchanged
    assert compute_detected(keys, {"US0378331005"}) is True  # new mismatch appeared


def test_enrich_mismatch_adds_review_required_triage():
    enriched = enrich_mismatch(
        {
            "isin": "IL0011410359",
            "ticker": "EMCO",
            "exchange": "PSX",
            "name": "Emco Industries Ltd",
            "figi_tickers": ["PEAX"],
            "figi_name": "PEAX SOLUTIONS LTD",
        }
    )

    assert enriched["triage_decision"] == "review_required_openfigi_resolves_different_security"
    assert enriched["triage_bucket"] == "possible_wrong_or_stale_isin"
    assert "PEAX" in enriched["triage_rationale"]
    assert "metadata override" in enriched["next_action"]


def test_build_residual_triage_counts_mismatch_and_no_data_by_exchange():
    verdicts = {
        "IL0011410359": "mismatch",
        "US0378331005": "match",
        "PK0000001012": "no_data",
    }
    mismatches = [
        enrich_mismatch(
            {
                "isin": "IL0011410359",
                "ticker": "EMCO",
                "exchange": "PSX",
                "name": "Emco Industries Ltd",
                "figi_tickers": ["PEAX"],
                "figi_name": "PEAX SOLUTIONS LTD",
            }
        )
    ]
    isin_rows = {
        "IL0011410359": {"exchange": "PSX"},
        "US0378331005": {"exchange": "NASDAQ"},
        "PK0000001012": {"exchange": "PSX"},
    }

    triage = build_residual_triage(verdicts, mismatches, isin_rows)

    assert triage["mismatch_rows"] == 1
    assert triage["no_data_rows"] == 1
    assert triage["mismatch_by_exchange"] == {"PSX": 1}
    assert triage["no_data_by_exchange"] == {"PSX": 1}
    assert triage["remaining_unclassified_residuals"] == 0


def test_select_valid_cache_invalidates_when_cached_row_fingerprint_changes():
    rows = {
        "PK0087206139": {
            "ticker": "UBLPETF",
            "exchange": "PSX",
            "name": "UBL Pakistan Enterprise ETF",
        }
    }
    old_row = {"ticker": "UBLPETF", "exchange": "PSX", "name": "UBLPakistanETF XD"}

    verdicts, detail = select_valid_cache(
        rows,
        {"PK0087206139": "mismatch"},
        {},
        {"PK0087206139": row_cache_fingerprint(old_row)},
    )

    assert verdicts == {}
    assert detail == {}


def test_select_valid_cache_invalidates_legacy_mismatch_when_detail_name_changes():
    rows = {
        "PK0087206139": {
            "ticker": "UBLPETF",
            "exchange": "PSX",
            "name": "UBL Pakistan Enterprise ETF",
        }
    }
    mismatch_detail = {
        "PK0087206139": {
            "isin": "PK0087206139",
            "ticker": "UBLPETF",
            "exchange": "PSX",
            "name": "UBLPakistanETF XD",
            "figi_tickers": ["UBLFETF"],
            "figi_name": "UBL PAKISTAN ENTERPRISE-ETF",
        }
    }

    verdicts, detail = select_valid_cache(rows, {"PK0087206139": "mismatch"}, mismatch_detail, {})

    assert verdicts == {}
    assert detail == {}


def test_select_valid_cache_carries_matching_fingerprinted_row():
    rows = {
        "US0378331005": {
            "ticker": "AAPL",
            "exchange": "NASDAQ",
            "name": "Apple Inc",
        }
    }

    verdicts, detail = select_valid_cache(
        rows,
        {"US0378331005": "match"},
        {},
        {"US0378331005": row_cache_fingerprint(rows["US0378331005"])},
    )

    assert verdicts == {"US0378331005": "match"}
    assert detail == {}

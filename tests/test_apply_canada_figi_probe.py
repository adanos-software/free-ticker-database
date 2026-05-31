from scripts.apply_canada_figi_probe import (
    apply_gate_context_for,
    apply_report_rows,
    build_apply_report,
    existing_identifier_context_for,
    merge_openfigi_gap_rows,
    no_match_probe_rows,
    openfigi_probe_context_for,
    summarize,
)


def test_build_apply_report_accepts_strict_probe_match() -> None:
    rows = build_apply_report(
        probe_rows=[
            {
                "listing_key": "NEO::AENU",
                "ticker": "AENU",
                "exchange": "NEO",
                "isin": "CA00792A1093",
                "openfigi_figi": "BBG01J08HXD4",
                "requested_exchange_hint": "CN",
                "openfigi_exch_code": "CN",
                "decision": "accept",
            }
        ],
        identifiers_rows=[
            {
                "listing_key": "NEO::AENU",
                "ticker": "AENU",
                "exchange": "NEO",
                "isin": "CA00792A1093",
                "figi": "",
                "figi_source": "",
            }
        ],
        listing_index_rows=[
            {
                "listing_key": "NEO::AENU",
                "ticker": "AENU",
                "exchange": "NEO",
                "isin": "CA00792A1093",
                "figi": "",
            }
        ],
    )

    assert rows == [
        {
            "listing_key": "NEO::AENU",
            "ticker": "AENU",
            "exchange": "NEO",
            "isin": "CA00792A1093",
            "figi": "BBG01J08HXD4",
            "requested_exchange_hint": "CN",
            "openfigi_exch_code": "CN",
            "openfigi_candidate_count": "",
            "identifier_isin": "CA00792A1093",
            "listing_index_isin": "CA00792A1093",
            "existing_identifier_figi": "",
            "existing_listing_index_figi": "",
            "decision": "apply",
            "reason": "accepted_probe_match",
            "verification_evidence_required": "listing_key_isin_exchange_hint_openfigi_figi_and_cross_isin_collision_gates",
            "openfigi_probe_context": (
                "requested_exchange_hint=CN;openfigi_exch_code=CN;openfigi_figi_present=true;candidate_count=none"
            ),
            "existing_identifier_context": (
                "identifier_isin=CA00792A1093;listing_index_isin=CA00792A1093;"
                "existing_identifier_figi=none;existing_listing_index_figi=none"
            ),
            "apply_gate_context": (
                "decision=apply;reason=accepted_probe_match;"
                "verification_evidence_required=listing_key_isin_exchange_hint_openfigi_figi_and_cross_isin_collision_gates"
            ),
        }
    ]
    assert rows[0]["openfigi_probe_context"] == openfigi_probe_context_for(rows[0])
    assert rows[0]["existing_identifier_context"] == existing_identifier_context_for(rows[0])
    assert rows[0]["apply_gate_context"] == apply_gate_context_for(rows[0])


def test_build_apply_report_rejects_isin_mismatch() -> None:
    rows = build_apply_report(
        probe_rows=[
            {
                "listing_key": "NEO::AENU",
                "ticker": "AENU",
                "exchange": "NEO",
                "isin": "CA00792A1093",
                "openfigi_figi": "BBG01J08HXD4",
                "requested_exchange_hint": "CN",
                "openfigi_exch_code": "CN",
                "decision": "accept",
            }
        ],
        identifiers_rows=[
            {
                "listing_key": "NEO::AENU",
                "isin": "CA0000000000",
                "figi": "",
            }
        ],
        listing_index_rows=[
            {
                "listing_key": "NEO::AENU",
                "isin": "CA00792A1093",
                "figi": "",
            }
        ],
    )

    assert rows[0]["decision"] == "reject"
    assert rows[0]["reason"] == "identifier_isin_mismatch"
    assert rows[0]["existing_identifier_context"] == existing_identifier_context_for(rows[0])


def test_build_apply_report_skips_idempotent_existing_figi() -> None:
    rows = build_apply_report(
        probe_rows=[
            {
                "listing_key": "NEO::AENU",
                "ticker": "AENU",
                "exchange": "NEO",
                "isin": "CA00792A1093",
                "openfigi_figi": "BBG01J08HXD4",
                "requested_exchange_hint": "CN",
                "openfigi_exch_code": "CN",
                "decision": "accept",
            }
        ],
        identifiers_rows=[
            {
                "listing_key": "NEO::AENU",
                "isin": "CA00792A1093",
                "figi": "BBG01J08HXD4",
            }
        ],
        listing_index_rows=[
            {
                "listing_key": "NEO::AENU",
                "isin": "CA00792A1093",
                "figi": "BBG01J08HXD4",
            }
        ],
    )

    assert rows[0]["decision"] == "skip"
    assert rows[0]["reason"] == "identifier_figi_already_set_to_same_value"
    assert rows[0]["apply_gate_context"] == apply_gate_context_for(rows[0])


def test_build_apply_report_rejects_cross_isin_figi_collision() -> None:
    rows = build_apply_report(
        probe_rows=[
            {
                "listing_key": "NEO::AENU",
                "ticker": "AENU",
                "exchange": "NEO",
                "isin": "CA00792A1093",
                "openfigi_figi": "BBGCOLLIDE",
                "requested_exchange_hint": "CN",
                "openfigi_exch_code": "CN",
                "decision": "accept",
            }
        ],
        identifiers_rows=[
            {
                "listing_key": "NEO::AENU",
                "isin": "CA00792A1093",
                "figi": "",
            },
            {
                "listing_key": "TSX::OTHER",
                "isin": "CA9999999999",
                "figi": "BBGCOLLIDE",
            },
        ],
        listing_index_rows=[
            {
                "listing_key": "NEO::AENU",
                "isin": "CA00792A1093",
                "figi": "",
            }
        ],
    )

    assert rows[0]["decision"] == "reject"
    assert rows[0]["reason"] == "figi_cross_isin_collision"
    assert rows[0]["apply_gate_context"] == apply_gate_context_for(rows[0])


def test_apply_report_rows_updates_identifier_and_listing_index() -> None:
    identifiers = [{"listing_key": "NEO::AENU", "figi": "", "figi_source": ""}]
    listing_index = [{"listing_key": "NEO::AENU", "figi": ""}]
    updated = apply_report_rows(
        report_rows=[
            {
                "listing_key": "NEO::AENU",
                "figi": "BBG01J08HXD4",
                "decision": "apply",
            }
        ],
        identifiers_rows=identifiers,
        listing_index_rows=listing_index,
    )

    assert updated == 1
    assert identifiers[0]["figi"] == "BBG01J08HXD4"
    assert identifiers[0]["figi_source"] == "OpenFIGI"
    assert listing_index[0]["figi"] == "BBG01J08HXD4"


def test_no_match_probe_rows_deduplicates_valid_openfigi_gaps() -> None:
    rows = no_match_probe_rows(
        [
            {
                "listing_key": "TSX::BKCC",
                "isin": "CA4404541068",
                "requested_exchange_hint": "CN",
                "decision": "no_openfigi_match",
            },
            {
                "listing_key": "TSX::BKCC",
                "isin": "CA4404541068",
                "requested_exchange_hint": "CN",
                "decision": "no_openfigi_match",
            },
            {
                "listing_key": "TSX::SHOP",
                "isin": "CA82509L1076",
                "requested_exchange_hint": "CN",
                "decision": "accept",
            },
        ]
    )

    assert len(rows) == 1
    assert rows[0]["listing_key"] == "TSX::BKCC"


def test_merge_openfigi_gap_rows_records_reviewed_source_gaps() -> None:
    rows, added = merge_openfigi_gap_rows(
        existing_rows=[],
        probe_rows=[
            {
                "listing_key": "TSX::BKCC",
                "ticker": "BKCC",
                "exchange": "TSX",
                "asset_type": "ETF",
                "name": "Global X Equal Weight Canadian Bank Covered Call ETF",
                "isin": "CA4404541068",
                "requested_exchange_hint": "CN",
                "candidate_count": "0",
                "decision": "no_openfigi_match",
            }
        ],
        reviewed_at="2026-05-24T00:00:00Z",
    )

    assert added == 1
    assert rows[0]["review_status"] == "accepted_source_gap_no_openfigi_match"
    assert rows[0]["source"] == "OpenFIGI ID_ISIN mapping"


def test_merge_openfigi_gap_rows_records_figi_collision_source_gaps() -> None:
    rows, added = merge_openfigi_gap_rows(
        existing_rows=[],
        probe_rows=[
            {
                "listing_key": "TSXV::MRZL",
                "ticker": "MRZL",
                "exchange": "TSXV",
                "asset_type": "Stock",
                "name": "Mont Royal Resources Limited",
                "isin": "AU0000041758",
                "requested_exchange_hint": "CN",
                "candidate_count": "1",
                "decision": "accept",
            }
        ],
        report_rows=[
            {
                "listing_key": "TSXV::MRZL",
                "ticker": "MRZL",
                "exchange": "TSXV",
                "isin": "AU0000041758",
                "decision": "reject",
                "reason": "figi_cross_isin_collision",
            }
        ],
        reviewed_at="2026-05-24T00:00:00Z",
    )

    assert added == 1
    assert rows[0]["decision"] == "reject"
    assert rows[0]["review_status"] == "accepted_source_gap_figi_cross_isin_collision"


def test_merge_openfigi_gap_rows_does_not_duplicate_existing_gap() -> None:
    rows, added = merge_openfigi_gap_rows(
        existing_rows=[
            {
                "listing_key": "TSX::BKCC",
                "isin": "CA4404541068",
                "requested_exchange_hint": "CN",
                "decision": "no_openfigi_match",
                "review_status": "accepted_source_gap_no_openfigi_match",
            }
        ],
        probe_rows=[
            {
                "listing_key": "TSX::BKCC",
                "isin": "CA4404541068",
                "requested_exchange_hint": "CN",
                "decision": "no_openfigi_match",
            }
        ],
        reviewed_at="2026-05-24T00:00:00Z",
    )

    assert added == 0
    assert len(rows) == 1


def test_summarize_counts_written_rows_only_when_applied() -> None:
    summary = summarize(
        [{"decision": "apply", "reason": "accepted_probe_match"}],
        "2026-05-24T00:00:00Z",
        applied=True,
        gap_rows_added=2,
    )

    assert summary["decision_totals"] == {"apply": 1}
    assert summary["applied_rows"] == 1
    assert summary["gap_rows_added"] == 2

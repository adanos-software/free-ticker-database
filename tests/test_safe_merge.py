from __future__ import annotations

from scripts.check_safe_merge import evaluate
from scripts.lib.merge_evidence import row_fingerprint


def row(exchange: str, ticker: str, **overrides: str) -> dict[str, str]:
    value = {
        "listing_key": f"{exchange}::{ticker}", "exchange": exchange, "ticker": ticker,
        "name": f"Company {ticker}", "asset_type": "Stock", "country": "United States",
        "country_code": "US", "isin": "", "stock_sector": "", "etf_category": "",
    }
    value.update(overrides)
    return value


def removal_event(value: dict[str, str]) -> dict[str, str]:
    return {
        "listing_key": value["listing_key"], "exchange": value["exchange"], "ticker": value["ticker"],
        "event_type": "delisted", "before_row_sha256": row_fingerprint(value),
        "observed_at": "2026-08-17T00:00:00Z", "source_key": "official_delisting_notice",
        "source_url": "https://exchange.example/notices/A",
        "observation_id": f"delisting-{value['listing_key']}", "evidence_status": "official",
    }


def test_one_unevidenced_removal_is_blocked() -> None:
    before = [row("NASDAQ", str(index)) for index in range(1000)]
    report = evaluate(before, before[:-1], [])
    assert report["status"] == "fail"
    assert report["summary"]["unevidenced_removed_rows"] == 1


def test_exact_current_row_event_allows_removal() -> None:
    before = [row("NASDAQ", str(index)) for index in range(1000)]
    report = evaluate(before, before[:-1], [removal_event(before[-1])])
    assert report["status"] == "pass"


def test_stale_event_cannot_authorize_removal() -> None:
    before = [row("NASDAQ", "A", name="Current")]
    report = evaluate(before, [], [removal_event(row("NASDAQ", "A", name="Old"))])
    assert report["status"] == "fail"


def test_critical_change_requires_exact_old_new_and_fingerprint() -> None:
    before = [row("NASDAQ", "A", isin="US0378331005")]
    after = [row("NASDAQ", "A", isin="")]
    event = {
        "listing_key": "NASDAQ::A", "event_type": "identifier_removed", "field_name": "isin",
        "old_value": "US0378331005", "new_value": "", "before_row_sha256": row_fingerprint(before[0]),
        "observed_at": "2026-08-17T00:00:00Z", "source_key": "identifier_quarantine",
        "source_report": "data/reports/identifier_quarantine.csv", "observation_id": "conflict-1",
        "evidence_status": "reviewed",
    }
    assert evaluate(before, after, [event])["status"] == "pass"
    event["before_row_sha256"] = "0" * 64
    assert evaluate(before, after, [event])["status"] == "fail"


def test_snapshot_absence_is_not_removal_evidence() -> None:
    before = [row("NASDAQ", "A")]
    event = removal_event(before[0])
    event["event_type"] = "not_observed"
    assert evaluate(before, [], [event])["status"] == "fail"


def test_invalid_timestamp_or_provenance_is_rejected() -> None:
    before = [row("NASDAQ", "A")]
    event = removal_event(before[0])
    event["observed_at"] = "2026-08-17T00:00:00"
    assert evaluate(before, [], [event])["status"] == "fail"
    event = removal_event(before[0])
    event["source_url"] = ""
    assert evaluate(before, [], [event])["status"] == "fail"


def test_duplicate_listing_keys_fail() -> None:
    duplicate = [row("X", "A"), row("X", "A")]
    assert evaluate(duplicate, duplicate, [])["status"] == "fail"


def test_large_evidenced_shrink_requires_explicit_override() -> None:
    before = [row("X", str(index)) for index in range(100)]
    after = before[:90]
    events = [removal_event(value) for value in before[90:]]
    assert evaluate(before, after, events)["status"] == "fail"
    assert evaluate(before, after, events, allow_large_evidenced_removal=True)["status"] == "pass"


def official_reference(
    exchange: str,
    ticker: str,
    *,
    name: str,
    isin: str,
    asset_type: str = "Stock",
    official: str = "true",
    listing_status: str = "active",
    sector: str = "",
) -> dict[str, str]:
    return {
        "source_key": "official_exchange_directory",
        "provider": "Example Exchange",
        "source_url": "https://exchange.example/securities.xlsx",
        "ticker": ticker,
        "name": name,
        "exchange": exchange,
        "asset_type": asset_type,
        "listing_status": listing_status,
        "reference_scope": "exchange_directory",
        "official": official,
        "isin": isin,
        "sector": sector,
    }


def test_exact_official_reference_allows_blank_taxonomy_fill() -> None:
    before = [
        row("BMV", "AC", name="Arca Continental", country="Mexico", country_code="MX", isin="MX01AC100006"),
        row(
            "BMV", "QQQ", name="Invesco QQQ Trust", asset_type="ETF",
            country="United States", country_code="US", isin="US46090E1038",
        ),
    ]
    after = [
        {**before[0], "stock_sector": "Consumer Staples"},
        {**before[1], "etf_category": "Equity"},
    ]
    report = evaluate(
        before,
        after,
        [],
        reference_rows=[
            official_reference(
                "BMV", "AC", name="ARCA CONTINENTAL, S.A.B. DE C.V.",
                isin="MX01AC100006", sector="Consumer Staples",
            ),
            official_reference(
                "BMV", "QQQ", name="QQQ *", isin="US46090E1038",
                asset_type="ETF", sector="Equity",
            ),
        ],
        observed_at="2026-08-31T00:00:00Z",
        reference_source_report="data/masterfiles/reference.csv",
    )
    assert report["status"] == "pass"
    assert {
        (event["listing_key"], event["field_name"], event["new_value"])
        for event in report["generated_official_change_evidence"]
    } == {
        ("BMV::AC", "stock_sector", "Consumer Staples"),
        ("BMV::QQQ", "etf_category", "Equity"),
    }


def test_official_taxonomy_fill_fails_closed_on_identity_conflict_or_replacement() -> None:
    baseline = row(
        "BMV", "AC", name="Arca Continental", country="Mexico",
        country_code="MX", isin="MX01AC100006",
    )
    candidate = {**baseline, "stock_sector": "Consumer Staples"}
    valid = official_reference(
        "BMV", "AC", name="Arca Continental", isin="MX01AC100006",
        sector="Consumer Staples",
    )
    cases = [
        [valid, {**valid, "sector": "Industrials"}],
        [{**valid, "isin": "US0378331005"}],
    ]
    for references in cases:
        report = evaluate(
            [baseline], [candidate], [], reference_rows=references,
            observed_at="2026-08-31T00:00:00Z",
            reference_source_report="data/masterfiles/reference.csv",
        )
        assert report["status"] == "fail"
        assert report["summary"]["generated_official_change_evidence_rows"] == 0

    replacement = {**baseline, "stock_sector": "Industrials"}
    report = evaluate(
        [replacement], [candidate], [], reference_rows=[valid],
        observed_at="2026-08-31T00:00:00Z",
        reference_source_report="data/masterfiles/reference.csv",
    )
    assert report["status"] == "fail"
    assert report["summary"]["generated_official_change_evidence_rows"] == 0


def test_exact_official_reference_allows_blank_isin_and_derived_country_change() -> None:
    before = [
        row("HKEX", "01124", name="COASTAL GL", country="Hong Kong", country_code="HK"),
        row("NYSE", "BMBASE", name="Bermuda Baseline", country="Bermuda", country_code="BM"),
    ]
    after = [
        row(
            "HKEX", "01124", name="COASTAL GL", country="Bermuda",
            country_code="BM", isin="BMG2239B1643",
        ),
        before[1],
    ]
    report = evaluate(
        before,
        after,
        [],
        reference_rows=[
            official_reference(
                "HKEX", "01124", name="COASTAL GL", isin="BMG2239B1643"
            )
        ],
        observed_at="2026-08-17T00:00:00Z",
        reference_source_report="data/masterfiles/reference.csv",
    )
    assert report["status"] == "pass"
    assert report["summary"]["generated_official_change_evidence_rows"] == 3
    assert {event["field_name"] for event in report["generated_official_change_evidence"]} == {
        "isin", "country", "country_code",
    }


def test_official_reference_cannot_replace_existing_isin() -> None:
    before = [row("X", "A", name="Alpha", isin="US0378331005")]
    after = [row("X", "A", name="Alpha", isin="US5949181045")]
    report = evaluate(
        before,
        after,
        [],
        reference_rows=[official_reference("X", "A", name="Alpha", isin="US5949181045")],
        observed_at="2026-08-17T00:00:00Z",
        reference_source_report="data/masterfiles/reference.csv",
    )
    assert report["status"] == "fail"
    assert report["summary"]["generated_official_change_evidence_rows"] == 0


def test_non_exact_or_untrusted_reference_cannot_authorize_change() -> None:
    before = [row("X", "A", name="Alpha")]
    after = [row("X", "A", name="Alpha", isin="US0378331005")]
    cases = [
        official_reference("Y", "A", name="Alpha", isin="US0378331005"),
        official_reference("X", "A", name="Different", isin="US0378331005"),
        official_reference("X", "A", name="Alpha", isin="US0378331005", official="false"),
        official_reference(
            "X", "A", name="Alpha", isin="US0378331005", listing_status="delisted"
        ),
    ]
    for reference in cases:
        report = evaluate(
            before,
            after,
            [],
            reference_rows=[reference],
            observed_at="2026-08-17T00:00:00Z",
            reference_source_report="data/masterfiles/reference.csv",
        )
        assert report["status"] == "fail"
        assert report["summary"]["generated_official_change_evidence_rows"] == 0


def test_conflicting_exact_official_isins_fail_closed() -> None:
    before = [row("X", "A", name="Alpha")]
    after = [row("X", "A", name="Alpha", isin="US0378331005")]
    report = evaluate(
        before,
        after,
        [],
        reference_rows=[
            official_reference("X", "A", name="Alpha", isin="US0378331005"),
            official_reference("X", "A", name="Alpha", isin="US5949181045"),
        ],
        observed_at="2026-08-17T00:00:00Z",
        reference_source_report="data/masterfiles/reference.csv",
    )
    assert report["status"] == "fail"
    assert report["summary"]["generated_official_change_evidence_rows"] == 0


def venue_reference(exchange: str, ticker: str, *, name: str) -> dict[str, str]:
    return {
        "source_key": "nasdaq_other_listed",
        "source_url": "https://www.nasdaqtrader.com/dynamic/symdir/otherlisted.txt",
        "ticker": ticker,
        "name": name,
        "exchange": exchange,
        "asset_type": "Stock",
        "listing_status": "active",
        "reference_scope": "exchange_directory",
        "official": "true",
        "isin": "",
    }


def sec_venue_reference(exchange: str, ticker: str, *, name: str) -> dict[str, str]:
    value = venue_reference(exchange, ticker, name=name)
    value.update(
        {
            "source_key": "sec_company_tickers_exchange",
            "source_url": "https://www.sec.gov/files/company_tickers_exchange.json",
        }
    )
    return value


def test_exact_official_venue_migrations_allow_ncl_and_sbev_removals() -> None:
    before = [
        row("NYSE MKT", "NCL", name="Northann Corp.", isin="US66373M4087"),
        row("NYSE MKT", "SBEV", name="Splash Beverage Group Inc", isin="US84862C3025"),
    ]
    after = [
        row("NYSE", "NCL", name="Northann Corp.", isin="US66373M4087"),
        row("NYSE", "SBEV", name="Splash Beverage Group Inc", isin="US84862C3025"),
    ]
    previous_references = [
        venue_reference("NYSE MKT", "NCL", name="Northann Corp. Common Stock"),
        venue_reference(
            "NYSE MKT", "SBEV", name="Splash Beverage Group, Inc. (NV) Common Stock"
        ),
        sec_venue_reference("NYSE", "NCL", name="Northann Corp."),
        sec_venue_reference("NYSE", "SBEV", name="SPLASH BEVERAGE GROUP, INC."),
    ]
    current_references = [
        sec_venue_reference("NYSE", "NCL", name="Northann Corp."),
        sec_venue_reference("NYSE", "SBEV", name="SPLASH BEVERAGE GROUP, INC."),
    ]

    report = evaluate(
        before,
        after,
        [],
        reference_rows=current_references,
        previous_reference_rows=previous_references,
        observed_at="2026-08-24T00:00:00Z",
        reference_source_report="data/masterfiles/reference.csv",
        venue_shrink_limit=1.0,
    )

    assert report["status"] == "pass"
    assert report["summary"]["evidenced_removed_rows"] == 2
    assert report["summary"]["generated_official_change_evidence_rows"] == 2
    assert {
        event["event_type"] for event in report["generated_official_change_evidence"]
    } == {"venue_reconciled"}
    assert {
        (event["listing_key"], event["old_value"], event["new_value"])
        for event in report["generated_official_change_evidence"]
    } == {
        ("NYSE MKT::NCL", "NYSE MKT", "NYSE"),
        ("NYSE MKT::SBEV", "NYSE MKT", "NYSE"),
    }


def test_ambiguous_official_venue_migration_fails_closed() -> None:
    before = [row("NYSE MKT", "NCL", name="Northann Corp.", isin="US66373M4087")]
    after = [
        row("NYSE", "NCL", name="Northann Corp.", isin="US66373M4087"),
        row("NASDAQ", "NCL", name="Northann Corp.", isin="US66373M4087"),
    ]
    previous_references = [
        venue_reference("NYSE MKT", "NCL", name="Northann Corp. Common Stock")
    ]
    current_references = [
        venue_reference("NYSE", "NCL", name="Northann Corp. Common Stock"),
        venue_reference("NASDAQ", "NCL", name="Northann Corp. Common Stock"),
    ]

    report = evaluate(
        before,
        after,
        [],
        reference_rows=current_references,
        previous_reference_rows=previous_references,
        observed_at="2026-08-24T00:00:00Z",
        reference_source_report="data/masterfiles/reference.csv",
    )

    assert report["status"] == "fail"
    assert report["summary"]["unevidenced_removed_rows"] == 1
    assert report["summary"]["generated_official_change_evidence_rows"] == 0


def test_ticker_only_official_venue_migration_fails_closed() -> None:
    before = [row("NYSE MKT", "AAA", name="Alpha Holdings", isin="US0378331005")]
    after = [row("NYSE", "AAA", name="Alpha Holdings", isin="US0378331005")]

    report = evaluate(
        before,
        after,
        [],
        reference_rows=[venue_reference("NYSE", "AAA", name="Different Issuer Common Stock")],
        previous_reference_rows=[
            venue_reference("NYSE MKT", "AAA", name="Different Issuer Common Stock")
        ],
        observed_at="2026-08-24T00:00:00Z",
        reference_source_report="data/masterfiles/reference.csv",
        venue_shrink_limit=1.0,
    )

    assert report["status"] == "fail"
    assert report["summary"]["generated_official_change_evidence_rows"] == 0


def test_conflicting_current_official_venue_claim_fails_closed() -> None:
    before = [row("NYSE MKT", "NCL", name="Northann Corp.", isin="US66373M4087")]
    after = [row("NYSE", "NCL", name="Northann Corp.", isin="US66373M4087")]
    previous_references = [
        venue_reference("NYSE MKT", "NCL", name="Northann Corp. Common Stock"),
        sec_venue_reference("NYSE", "NCL", name="Northann Corp."),
    ]
    current_references = [
        sec_venue_reference("NYSE", "NCL", name="Northann Corp."),
        venue_reference("NASDAQ", "NCL", name="Northann Corp. Common Stock"),
    ]

    report = evaluate(
        before,
        after,
        [],
        reference_rows=current_references,
        previous_reference_rows=previous_references,
        observed_at="2026-08-24T00:00:00Z",
        reference_source_report="data/masterfiles/reference.csv",
        venue_shrink_limit=1.0,
    )

    assert report["status"] == "fail"
    assert report["summary"]["generated_official_change_evidence_rows"] == 0


def test_conflicting_previous_isin_venue_reconciliation_fails_closed() -> None:
    before = [row("NYSE MKT", "NCL", name="Northann Corp.", isin="US66373M4087")]
    after = [row("NYSE", "NCL", name="Northann Corp.", isin="US66373M4087")]
    old_reference = venue_reference("NYSE MKT", "NCL", name="Northann Corp. Common Stock")
    old_reference["isin"] = "US0378331005"
    sec_reference = sec_venue_reference("NYSE", "NCL", name="Northann Corp.")

    report = evaluate(
        before,
        after,
        [],
        reference_rows=[sec_reference],
        previous_reference_rows=[old_reference, sec_reference],
        observed_at="2026-08-24T00:00:00Z",
        reference_source_report="data/masterfiles/reference.csv",
        venue_shrink_limit=1.0,
    )

    assert report["status"] == "fail"
    assert report["summary"]["generated_official_change_evidence_rows"] == 0


def test_same_isin_conflicting_current_venue_claim_fails_closed() -> None:
    before = [row("NYSE MKT", "NCL", name="Northann Corp.", isin="US66373M4087")]
    after = [row("NYSE", "NCL", name="Northann Corp.", isin="US66373M4087")]
    old_reference = venue_reference("NYSE MKT", "NCL", name="Northann Corp. Common Stock")
    sec_reference = sec_venue_reference("NYSE", "NCL", name="Northann Corp.")
    conflicting = venue_reference("NASDAQ", "NCL", name="Completely Different Name")
    conflicting["isin"] = "US66373M4087"

    report = evaluate(
        before,
        after,
        [],
        reference_rows=[sec_reference, conflicting],
        previous_reference_rows=[old_reference, sec_reference],
        observed_at="2026-08-24T00:00:00Z",
        reference_source_report="data/masterfiles/reference.csv",
        venue_shrink_limit=1.0,
    )

    assert report["status"] == "fail"
    assert report["summary"]["generated_official_change_evidence_rows"] == 0


def test_sec_target_must_be_stable_at_same_venue() -> None:
    before = [row("NYSE MKT", "NCL", name="Northann Corp.", isin="US66373M4087")]
    after = [row("NYSE", "NCL", name="Northann Corp.", isin="US66373M4087")]
    old_reference = venue_reference("NYSE MKT", "NCL", name="Northann Corp. Common Stock")
    previous_sec = sec_venue_reference("NASDAQ", "NCL", name="Northann Corp.")
    current_sec = sec_venue_reference("NYSE", "NCL", name="Northann Corp.")

    report = evaluate(
        before,
        after,
        [],
        reference_rows=[current_sec],
        previous_reference_rows=[old_reference, previous_sec],
        observed_at="2026-08-24T00:00:00Z",
        reference_source_report="data/masterfiles/reference.csv",
        venue_shrink_limit=1.0,
    )

    assert report["status"] == "fail"
    assert report["summary"]["generated_official_change_evidence_rows"] == 0


def reviewed_transition(
    old_key: str,
    new_key: str,
    *,
    event_type: str,
    identity_type: str,
    identity_value: str,
) -> dict[str, str]:
    return {
        "old_listing_key": old_key,
        "new_listing_key": new_key,
        "event_type": event_type,
        "identity_type": identity_type,
        "identity_value": identity_value,
        "confidence": "0.99",
        "source_key": "issuer_announcement",
        "source_url": "https://issuer.example/transition",
        "reason": "Reviewed issuer transition.",
    }


def test_reviewed_same_isin_symbol_and_venue_transitions_allow_removals() -> None:
    isin_symbol = "US53656F1690"
    isin_venue = "US74726N1072"
    before = [
        row("NASDAQ", "TUGN", name="Fund", asset_type="ETF", isin=isin_symbol),
        row("OTC", "QNBC", name="QNB Corp", isin=isin_venue),
    ]
    after = [
        row("NASDAQ", "SEPQ", name="Fund", asset_type="ETF", isin=isin_symbol),
        row("NASDAQ", "QNBC", name="QNB Corp", isin=isin_venue),
    ]
    transitions = [
        reviewed_transition(
            "NASDAQ::TUGN", "NASDAQ::SEPQ",
            event_type="symbol_changed", identity_type="same_isin", identity_value=isin_symbol,
        ),
        reviewed_transition(
            "OTC::QNBC", "NASDAQ::QNBC",
            event_type="venue_changed", identity_type="same_isin", identity_value=isin_venue,
        ),
    ]

    report = evaluate(
        before,
        after,
        [],
        reviewed_transition_rows=transitions,
        reviewed_transition_source_report="data/review_overrides/listing_transitions.csv",
        observed_at="2026-08-24T00:00:00Z",
        venue_shrink_limit=1.0,
    )

    assert report["status"] == "pass"
    assert report["summary"]["generated_reviewed_transition_evidence_rows"] == 2


def test_reviewed_same_cik_transition_requires_both_sec_cache_symbols() -> None:
    before = [row("NASDAQ", "GGRP", name="Glimpse")]
    after = [row("NASDAQ", "BTLN", name="Brightline")]
    transition = reviewed_transition(
        "NASDAQ::GGRP", "NASDAQ::BTLN",
        event_type="symbol_changed", identity_type="same_cik", identity_value="0001854445",
    )
    payload = {
        "fields": ["cik", "name", "ticker", "exchange"],
        "data": [
            [1854445, "Glimpse Group, Inc.", "GGRP", "Nasdaq"],
            [1854445, "Glimpse Group, Inc.", "BTLN", "Nasdaq"],
        ],
    }

    passed = evaluate(
        before,
        after,
        [],
        reviewed_transition_rows=[transition],
        sec_exchange_payload=payload,
        reviewed_transition_source_report="data/review_overrides/listing_transitions.csv",
        observed_at="2026-08-24T00:00:00Z",
    )
    failed = evaluate(
        before,
        after,
        [],
        reviewed_transition_rows=[transition],
        sec_exchange_payload={"fields": payload["fields"], "data": payload["data"][:1]},
        reviewed_transition_source_report="data/review_overrides/listing_transitions.csv",
        observed_at="2026-08-24T00:00:00Z",
    )

    assert passed["status"] == "pass"
    assert failed["status"] == "fail"
    assert failed["summary"]["generated_reviewed_transition_evidence_rows"] == 0


def test_reviewed_transition_fails_closed_on_identity_or_shape_mismatch() -> None:
    before = [row("NASDAQ", "OLD", isin="US53656F1690")]
    after = [row("NYSE", "NEW", isin="US53656F1690")]
    transition = reviewed_transition(
        "NASDAQ::OLD", "NYSE::NEW",
        event_type="symbol_changed", identity_type="same_isin", identity_value="US53656F1690",
    )

    report = evaluate(
        before,
        after,
        [],
        reviewed_transition_rows=[transition],
        reviewed_transition_source_report="data/review_overrides/listing_transitions.csv",
        observed_at="2026-08-24T00:00:00Z",
        venue_shrink_limit=1.0,
    )

    assert report["status"] == "fail"
    assert report["summary"]["generated_reviewed_transition_evidence_rows"] == 0


def test_reviewed_transition_rejects_non_finite_confidence() -> None:
    before = [row("NASDAQ", "OLD", isin="US53656F1690")]
    after = [row("NASDAQ", "NEW", isin="US53656F1690")]
    transition = reviewed_transition(
        "NASDAQ::OLD", "NASDAQ::NEW",
        event_type="symbol_changed", identity_type="same_isin", identity_value="US53656F1690",
    )
    transition["confidence"] = "nan"

    report = evaluate(
        before,
        after,
        [],
        reviewed_transition_rows=[transition],
        reviewed_transition_source_report="data/review_overrides/listing_transitions.csv",
        observed_at="2026-08-24T00:00:00Z",
    )

    assert report["status"] == "fail"
    assert report["summary"]["generated_reviewed_transition_evidence_rows"] == 0

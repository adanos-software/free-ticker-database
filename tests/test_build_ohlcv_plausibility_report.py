from __future__ import annotations

import csv
from argparse import Namespace
from datetime import UTC, datetime, timedelta

from scripts.build_ohlcv_plausibility_report import (
    OhlcvBar,
    assess_bars,
    build_report,
    canonical_data_change_authorization_for,
    ohlcv_sample_context_for,
    plausibility_review_context_for,
    quality_cluster_sample_rows,
    read_completed_listing_keys,
    selection_context_for,
    stream_report,
    write_csv,
    write_markdown,
    yahoo_symbol_candidates,
)


def listing_row(
    listing_key: str = "NASDAQ::MSFT",
    ticker: str = "MSFT",
    exchange: str = "NASDAQ",
    name: str = "Microsoft Corporation",
    asset_type: str = "Stock",
    isin: str = "US5949181045",
) -> dict[str, str]:
    return {
        "listing_key": listing_key,
        "ticker": ticker,
        "exchange": exchange,
        "name": name,
        "asset_type": asset_type,
        "isin": isin,
    }


def test_yahoo_symbol_candidates_cover_key_venues():
    assert yahoo_symbol_candidates("MSFT", "NASDAQ") == ["MSFT"]
    assert yahoo_symbol_candidates("7203", "TSE") == ["7203.T"]
    assert yahoo_symbol_candidates("PETR4", "B3") == ["PETR4.SA"]
    assert yahoo_symbol_candidates("000001", "SZSE") == ["000001.SZ"]
    assert yahoo_symbol_candidates("VOD", "LSE") == ["VOD.L"]
    assert yahoo_symbol_candidates("ABC", "UNSUPPORTED") == []


def test_assess_bars_marks_clean_sample_as_pass():
    bars = [
        OhlcvBar("2026-04-09", 100, 105, 99, 104, 1_000),
        OhlcvBar("2026-04-10", 104, 106, 103, 105, 1_200),
        OhlcvBar("2026-04-13", 105, 107, 104, 106, 900),
    ]

    result = assess_bars(
        listing_row(),
        entry_quality_status="pass",
        ohlcv_source="fixture",
        ohlcv_symbol="MSFT",
        bars=bars,
        checked=True,
        min_bars=3,
        stale_days=21,
        price_jump_threshold=0.30,
        zero_volume_streak_threshold=5,
        stagnant_close_streak_threshold=10,
        now=datetime(2026, 4, 13, tzinfo=UTC),
    )

    assert result.plausibility_status == "pass"
    assert result.plausibility_score == 100
    assert result.review_bucket == "checked_plausible_sample"
    assert result.review_priority == "P4"
    assert result.sampling_strategy == "retain_as_plausibility_baseline_no_data_change"
    assert result.plausibility_use == "market_data_plausibility_evidence_only"
    assert canonical_data_change_authorization_for(result.review_bucket) == "no_canonical_data_change_authorized"
    assert result.verification_evidence_required == "none_no_database_change_authorized"
    assert result.recommended_next_source == "Retain sampled OHLCV evidence for NASDAQ as a plausibility baseline only."
    assert result.source_gate == "Plausible OHLCV sample is baseline evidence only; no database change is authorized."
    assert result.selection_context == selection_context_for(
        selection_bucket="",
        entry_quality_status="pass",
        source_gap_field="",
        source_gap_class="",
    )
    assert result.ohlcv_sample_context == ohlcv_sample_context_for(
        ohlcv_source="fixture",
        ohlcv_symbol="MSFT",
        bar_count=3,
        first_bar_date="2026-04-09",
        last_bar_date="2026-04-13",
        issue_count=0,
    )
    assert result.plausibility_review_context == plausibility_review_context_for(
        plausibility_status="pass",
        review_bucket="checked_plausible_sample",
        sampling_strategy="retain_as_plausibility_baseline_no_data_change",
        verification_evidence_required="none_no_database_change_authorized",
    )
    assert result.bar_count == 3
    assert result.last_bar_date == "2026-04-13"


def test_assess_bars_flags_market_data_anomalies():
    bars = [
        OhlcvBar("2026-03-01", 100, 105, 99, 100, 0),
        OhlcvBar("2026-03-02", 160, 161, 159, 100, 0),
        OhlcvBar("2026-03-03", 100, 99, 101, 100, 0),
    ]

    result = assess_bars(
        listing_row(),
        entry_quality_status="source_gap",
        ohlcv_source="fixture",
        ohlcv_symbol="MSFT",
        bars=bars,
        checked=True,
        min_bars=128,
        stale_days=21,
        price_jump_threshold=0.30,
        zero_volume_streak_threshold=3,
        stagnant_close_streak_threshold=2,
        now=datetime(2026, 4, 13, tzinfo=UTC),
    )

    assert result.plausibility_status == "warn"
    assert result.review_bucket == "checked_ohlcv_anomaly_requires_listing_review"
    assert result.review_priority == "P1"
    assert result.sampling_strategy == "review_checked_ohlcv_anomaly_against_listing_status_and_corporate_actions"
    assert result.plausibility_use == "review_signal_only_possible_listing_or_corporate_action_issue"
    assert (
        canonical_data_change_authorization_for(result.review_bucket)
        == "official_listing_review_required_before_any_canonical_change"
    )
    assert result.verification_evidence_required == "official_listing_status_corporate_action_and_market_data_source_review_before_any_listing_change"
    assert result.recommended_next_source == (
        "Official listing status, corporate-action evidence, and independent market-data sample for NASDAQ."
    )
    assert result.source_gate == (
        "Do not change listing data until official listing status and corporate-action evidence explain the anomaly."
    )
    issue_types = {issue.issue_type for issue in result.issues}
    assert "short_history" in issue_types
    assert "stale_last_bar" in issue_types
    assert "invalid_ohlcv_bar" in issue_types
    assert "large_price_jump" in issue_types
    assert "long_zero_volume_streak" in issue_types
    assert "long_stagnant_close_streak" in issue_types


def test_build_report_uses_local_ohlcv_samples(tmp_path):
    listings_csv = tmp_path / "listings.csv"
    entry_quality_csv = tmp_path / "entry_quality.csv"
    source_gap_csv = tmp_path / "source_gap_classification.csv"
    ohlcv_dir = tmp_path / "ohlcv"
    ohlcv_dir.mkdir()

    with listings_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["listing_key", "ticker", "exchange", "name", "asset_type", "isin"])
        writer.writeheader()
        writer.writerow(listing_row())

    with entry_quality_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["listing_key", "quality_status"])
        writer.writeheader()
        writer.writerow({"listing_key": "NASDAQ::MSFT", "quality_status": "pass"})
    with source_gap_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["listing_key", "field", "gap_class"])
        writer.writeheader()

    today = datetime.now(UTC).date()
    dates = [today - timedelta(days=4), today - timedelta(days=3), today - timedelta(days=1)]
    (ohlcv_dir / "NASDAQ__MSFT.csv").write_text(
        "date,open,high,low,close,volume\n"
        f"{dates[0]},100,105,99,104,1000\n"
        f"{dates[1]},104,106,103,105,1200\n"
        f"{dates[2]},105,107,104,106,900\n",
        encoding="utf-8",
    )

    args = Namespace(
        listings_csv=listings_csv,
        entry_quality_csv=entry_quality_csv,
        source_gap_csv=source_gap_csv,
        ohlcv_dir=ohlcv_dir,
        csv_out=tmp_path / "ohlcv_plausibility.csv",
        json_out=tmp_path / "ohlcv_plausibility.json",
        md_out=tmp_path / "ohlcv_plausibility.md",
        exchange=[],
        asset_type=[],
        focus_status=[],
        max_rows=0,
        sample_profile="filtered",
        samples_per_gap_class=5,
        large_exchange_count=10,
        samples_per_large_exchange=5,
        fetch_yahoo=False,
        include_not_checked=False,
        max_fetch=250,
        delay_seconds=0.0,
        timeout_seconds=10.0,
        chart_range="1y",
        interval="1d",
        min_bars=3,
        stale_days=21,
        price_jump_threshold=0.30,
        zero_volume_streak_threshold=5,
        stagnant_close_streak_threshold=10,
    )

    rows, payload = build_report(args)
    assert len(rows) == 1
    assert rows[0].plausibility_status == "pass"
    assert rows[0].review_bucket == "checked_plausible_sample"
    assert payload["_meta"]["source_files"] == {
        "listings_csv": str(listings_csv),
        "entry_quality_csv": str(entry_quality_csv),
        "source_gap_classification_csv": str(source_gap_csv),
    }
    assert payload["summary"]["status_counts"] == {"pass": 1}
    assert payload["summary"]["selection_bucket_exchange_counts"] == {"filtered": {"NASDAQ": 1}}
    assert payload["summary"]["selection_bucket_status_counts"] == {"filtered": {"pass": 1}}
    assert payload["summary"]["review_priority_counts"] == {"P4": 1}
    assert payload["summary"]["review_bucket_selection_bucket_counts"] == {"checked_plausible_sample": {"filtered": 1}}
    assert payload["summary"]["review_bucket_exchange_counts"] == {"checked_plausible_sample": {"NASDAQ": 1}}
    assert payload["summary"]["review_bucket_sampling_strategy_counts"] == {
        "checked_plausible_sample": {"retain_as_plausibility_baseline_no_data_change": 1}
    }
    assert payload["summary"]["review_bucket_sampling_readiness_counts"] == {
        "checked_plausible_sample": {"checked_local_sample": 1}
    }
    assert payload["summary"]["top_ohlcv_sampling_batches"] == [
        {
            "review_bucket": "checked_plausible_sample",
            "selection_bucket": "filtered",
            "exchange": "NASDAQ",
            "plausibility_status": "pass",
            "rows": 1,
            "review_priority": "P4",
            "sampling_strategy": "retain_as_plausibility_baseline_no_data_change",
            "evidence_required": "none_no_database_change_authorized",
            "recommended_next_source": "Retain sampled OHLCV evidence for NASDAQ as a plausibility baseline only.",
            "source_gate": "Plausible OHLCV sample is baseline evidence only; no database change is authorized.",
        }
    ]
    assert payload["summary"]["plausibility_use_counts"] == {"market_data_plausibility_evidence_only": 1}
    assert payload["summary"]["canonical_data_change_authorization_counts"] == {
        "no_canonical_data_change_authorized": 1,
    }
    assert payload["summary"]["verification_evidence_required_counts"] == {"none_no_database_change_authorized": 1}
    assert payload["summary"]["sampling_coverage"] == {
        "selected_rows": 1,
        "report_rows": 1,
        "checked_rows": 1,
        "not_checked_rows": 0,
        "skipped_not_checked_rows": 0,
        "local_sample_rows": 1,
        "yahoo_sample_rows": 0,
        "warn_or_source_gap_signal_rows": 0,
    }
    assert payload["summary"]["ohlcv_sampling_backlog"] == {
        "status": "sampling_queue_enabled_plausibility_only",
        "selected_rows": 1,
        "report_rows": 1,
        "checked_rows": 1,
        "not_checked_rows": 0,
        "source_gap_cluster_sample_rows": 0,
        "entry_quality_warn_sample_rows": 0,
        "large_exchange_baseline_sample_rows": 0,
        "warn_or_source_gap_signal_rows": 0,
        "direct_canonical_data_change_allowed_rows": 0,
        "plausibility_signal_only": True,
        "source_gate": (
            "OHLCV sampling is plausibility evidence only; identifiers, sectors, categories, names, listings, "
            "and symbols remain blocked until official listing-keyed review evidence is available."
        ),
    }
    assert payload["review_items"] == [
        {
            "listing_key": "NASDAQ::MSFT",
            "ticker": "MSFT",
            "exchange": "NASDAQ",
            "asset_type": "Stock",
            "name": "Microsoft Corporation",
            "isin": "US5949181045",
            "entry_quality_status": "pass",
            "source_gap_field": "",
            "source_gap_class": "",
            "selection_bucket": "filtered",
            "plausibility_status": "pass",
            "plausibility_score": 100,
            "review_bucket": "checked_plausible_sample",
            "review_priority": "P4",
            "sampling_strategy": "retain_as_plausibility_baseline_no_data_change",
            "plausibility_use": "market_data_plausibility_evidence_only",
            "verification_evidence_required": "none_no_database_change_authorized",
            "recommended_next_source": "Retain sampled OHLCV evidence for NASDAQ as a plausibility baseline only.",
            "source_gate": "Plausible OHLCV sample is baseline evidence only; no database change is authorized.",
            "selection_context": (
                "selection_bucket=filtered;entry_quality_status=pass;source_gap_field=none;source_gap_class=none"
            ),
            "ohlcv_sample_context": (
                f"ohlcv_source={ohlcv_dir / 'NASDAQ__MSFT.csv'};ohlcv_symbol=none;"
                f"bar_count=3;first_bar_date={dates[0]};last_bar_date={dates[2]};issue_count=0"
            ),
            "plausibility_review_context": (
                "plausibility_status=pass;review_bucket=checked_plausible_sample;"
                "sampling_strategy=retain_as_plausibility_baseline_no_data_change;"
                "verification_evidence_required=none_no_database_change_authorized"
            ),
            "ohlcv_source": str(ohlcv_dir / "NASDAQ__MSFT.csv"),
            "ohlcv_symbol": "",
            "bar_count": 3,
            "first_bar_date": str(dates[0]),
            "last_bar_date": str(dates[2]),
            "max_price_jump": 0.0,
            "zero_volume_streak": 0,
            "stagnant_close_streak": 0,
            "invalid_bar_count": 0,
            "recommended_action": "none",
            "issues": [],
        }
    ]


def test_ohlcv_report_writes_csv_and_markdown(tmp_path):
    rows = [
        assess_bars(
            listing_row(),
            entry_quality_status="",
            ohlcv_source="",
            ohlcv_symbol="MSFT",
            bars=[],
            checked=False,
            min_bars=128,
            stale_days=21,
            price_jump_threshold=0.30,
            zero_volume_streak_threshold=5,
            stagnant_close_streak_threshold=10,
        )
    ]
    csv_path = tmp_path / "report.csv"
    md_path = tmp_path / "report.md"

    write_csv(csv_path, rows)
    write_markdown(
        md_path,
        {
            "_meta": {"generated_at": "2026-04-13T00:00:00Z"},
            "summary": {
                "status_counts": {"not_checked": 1},
                "issue_counts": {"no_ohlcv_sample": 1},
                "canonical_data_change_authorization_counts": {"no_canonical_data_change_authorized": 1},
                "sampling_coverage": {"selected_rows": 1, "report_rows": 1, "checked_rows": 0, "not_checked_rows": 1},
                "top_flagged_exchanges": [{"exchange": "NASDAQ", "not_checked": 1}],
            },
        },
    )

    csv_rows = list(csv.DictReader(csv_path.open()))
    assert csv_rows[0]["listing_key"] == "NASDAQ::MSFT"
    assert csv_rows[0]["plausibility_status"] == "not_checked"
    assert csv_rows[0]["review_bucket"] == "not_checked_unclassified_sample"
    assert csv_rows[0]["review_priority"] == "P4"
    assert csv_rows[0]["sampling_strategy"] == "manual_ohlcv_sampling_review_required"
    assert csv_rows[0]["plausibility_use"] == "sampling_queue_only"
    assert csv_rows[0]["verification_evidence_required"] == "manual_review_required"
    assert csv_rows[0]["recommended_next_source"] == "Manual OHLCV sampling review for NASDAQ."
    assert csv_rows[0]["source_gate"] == (
        "Manual review required; OHLCV plausibility alone is never sufficient for canonical data changes."
    )
    assert csv_rows[0]["selection_bucket"] == ""
    markdown = md_path.read_text()
    assert "OHLCV Plausibility Report" in markdown
    assert "## Sampling Strategies" in markdown
    assert "## Sampling Readiness" in markdown
    assert "## Top Sampling Batches" in markdown
    assert "## Plausibility Use" in markdown
    assert "## Canonical Data Change Authorization" in markdown
    assert "## Verification Evidence" in markdown


def test_quality_cluster_sample_rows_covers_warn_source_gap_classes_and_large_markets():
    rows = [
        listing_row("NASDAQ::WARN", "WARN", "NASDAQ"),
        listing_row("NASDAQ::GAP1", "GAP1", "NASDAQ"),
        listing_row("NYSE::GAP2", "GAP2", "NYSE"),
        listing_row("NASDAQ::PASS1", "PASS1", "NASDAQ"),
        listing_row("NASDAQ::PASS2", "PASS2", "NASDAQ"),
        listing_row("NYSE::PASS3", "PASS3", "NYSE"),
    ]
    entry_quality_lookup = {
        "NASDAQ::WARN": "warn",
        "NASDAQ::GAP1": "source_gap",
        "NYSE::GAP2": "source_gap",
        "NASDAQ::PASS1": "pass",
        "NASDAQ::PASS2": "pass",
        "NYSE::PASS3": "pass",
    }
    source_gap_lookup = {
        "NASDAQ::GAP1": {"field": "missing_isin_primary", "gap_class": "official_identifier_not_exposed_source_gap"},
        "NYSE::GAP2": {"field": "missing_sector_stock", "gap_class": "official_industry_taxonomy_unavailable_gap"},
    }

    sampled = quality_cluster_sample_rows(
        rows,
        entry_quality_lookup=entry_quality_lookup,
        source_gap_lookup=source_gap_lookup,
        samples_per_gap_class=1,
        large_exchange_count=1,
        samples_per_large_exchange=1,
        max_rows=0,
    )

    by_key = {row["listing_key"]: row for row in sampled}
    assert by_key["NASDAQ::WARN"]["_selection_bucket"] == "entry_quality_warn"
    assert by_key["NASDAQ::GAP1"]["_source_gap_class"] == "official_identifier_not_exposed_source_gap"
    assert by_key["NYSE::GAP2"]["_selection_bucket"] == "source_gap:official_industry_taxonomy_unavailable_gap"
    assert by_key["NASDAQ::PASS1"]["_selection_bucket"] == "large_exchange:NASDAQ"


def test_stream_report_resume_skips_completed_listing_keys(tmp_path):
    listings_csv = tmp_path / "listings.csv"
    entry_quality_csv = tmp_path / "entry_quality.csv"
    source_gap_csv = tmp_path / "source_gap_classification.csv"
    csv_path = tmp_path / "report.csv"

    with listings_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["listing_key", "ticker", "exchange", "name", "asset_type", "isin"])
        writer.writeheader()
        writer.writerow(listing_row())
        writer.writerow(listing_row(listing_key="NASDAQ::AAPL", ticker="AAPL", name="Apple Inc", isin="US0378331005"))

    with entry_quality_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["listing_key", "quality_status"])
        writer.writeheader()
    with source_gap_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["listing_key", "field", "gap_class"])
        writer.writeheader()

    existing = assess_bars(
        listing_row(),
        entry_quality_status="",
        ohlcv_source="",
        ohlcv_symbol="MSFT",
        bars=[],
        checked=False,
        min_bars=128,
        stale_days=21,
        price_jump_threshold=0.30,
        zero_volume_streak_threshold=5,
        stagnant_close_streak_threshold=10,
    )
    write_csv(csv_path, [existing])

    args = Namespace(
        listings_csv=listings_csv,
        entry_quality_csv=entry_quality_csv,
        source_gap_csv=source_gap_csv,
        ohlcv_dir=None,
        csv_out=csv_path,
        json_out=tmp_path / "report.json",
        md_out=tmp_path / "report.md",
        exchange=[],
        asset_type=[],
        focus_status=[],
        max_rows=0,
        sample_profile="filtered",
        samples_per_gap_class=5,
        large_exchange_count=10,
        samples_per_large_exchange=5,
        fetch_yahoo=False,
        include_not_checked=True,
        max_fetch=250,
        stream=True,
        resume=True,
        progress_every=0,
        delay_seconds=0.0,
        timeout_seconds=10.0,
        chart_range="1y",
        interval="1d",
        min_bars=128,
        stale_days=21,
        price_jump_threshold=0.30,
        zero_volume_streak_threshold=5,
        stagnant_close_streak_threshold=10,
    )

    rows, payload = stream_report(args)

    assert read_completed_listing_keys(csv_path) == {"NASDAQ::MSFT", "NASDAQ::AAPL"}
    assert [row.listing_key for row in rows] == ["NASDAQ::MSFT", "NASDAQ::AAPL"]
    assert payload["_meta"]["resumed_rows"] == 1
    assert payload["_meta"]["processed_rows"] == 1

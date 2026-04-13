from __future__ import annotations

import csv
from argparse import Namespace
from datetime import UTC, datetime

from scripts.build_ohlcv_plausibility_report import (
    OhlcvBar,
    assess_bars,
    build_report,
    read_completed_listing_keys,
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

    (ohlcv_dir / "NASDAQ__MSFT.csv").write_text(
        "date,open,high,low,close,volume\n"
        "2026-04-09,100,105,99,104,1000\n"
        "2026-04-10,104,106,103,105,1200\n"
        "2026-04-13,105,107,104,106,900\n",
        encoding="utf-8",
    )

    args = Namespace(
        listings_csv=listings_csv,
        entry_quality_csv=entry_quality_csv,
        ohlcv_dir=ohlcv_dir,
        csv_out=tmp_path / "ohlcv_plausibility.csv",
        json_out=tmp_path / "ohlcv_plausibility.json",
        md_out=tmp_path / "ohlcv_plausibility.md",
        exchange=[],
        asset_type=[],
        focus_status=[],
        max_rows=0,
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
    assert payload["summary"]["status_counts"] == {"pass": 1}


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
                "top_flagged_exchanges": [{"exchange": "NASDAQ", "not_checked": 1}],
            },
        },
    )

    csv_rows = list(csv.DictReader(csv_path.open()))
    assert csv_rows[0]["listing_key"] == "NASDAQ::MSFT"
    assert csv_rows[0]["plausibility_status"] == "not_checked"
    assert "OHLCV Plausibility Report" in md_path.read_text()


def test_stream_report_resume_skips_completed_listing_keys(tmp_path):
    listings_csv = tmp_path / "listings.csv"
    entry_quality_csv = tmp_path / "entry_quality.csv"
    csv_path = tmp_path / "report.csv"

    with listings_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["listing_key", "ticker", "exchange", "name", "asset_type", "isin"])
        writer.writeheader()
        writer.writerow(listing_row())
        writer.writerow(listing_row(listing_key="NASDAQ::AAPL", ticker="AAPL", name="Apple Inc", isin="US0378331005"))

    with entry_quality_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["listing_key", "quality_status"])
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
        ohlcv_dir=None,
        csv_out=csv_path,
        json_out=tmp_path / "report.json",
        md_out=tmp_path / "report.md",
        exchange=[],
        asset_type=[],
        focus_status=[],
        max_rows=0,
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

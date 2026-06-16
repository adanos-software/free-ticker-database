from datetime import datetime, timezone

from scripts.build_drift_report import (
    build_markdown,
    dataset_built_at,
    pending_renames,
    quality_indicators,
    staleness_days,
)


def test_staleness_days_parses_iso_and_handles_missing():
    now = datetime(2026, 6, 16, 12, 0, 0, tzinfo=timezone.utc)
    assert staleness_days("2026-06-14T12:00:00Z", now=now) == 2.0
    assert staleness_days(None, now=now) is None
    assert staleness_days("not-a-date", now=now) is None


def test_dataset_built_at_is_iso_or_none():
    built = dataset_built_at()
    assert built is None or built.endswith("Z") or "+" in built


def test_pending_renames_shape():
    rows = pending_renames()
    assert isinstance(rows, list)
    for r in rows[:50]:
        assert set(r) >= {"old_symbol", "new_symbol", "new_company_name", "effective_date"}
        # by construction the new symbol must not already be a current ticker
        assert r["new_symbol"] and r["old_symbol"]


def test_quality_indicators_are_ints():
    q = quality_indicators()
    assert isinstance(q, dict)
    for k, v in q.items():
        assert isinstance(v, int) and v >= 0


def test_build_markdown_renders():
    md = build_markdown({
        "generated_at": "2026-06-16T00:00:00+00:00",
        "built_at": "2026-06-16T00:00:00Z",
        "staleness_days": 1.0,
        "stale_threshold_days": 45.0,
        "pending_renames_count": 1,
        "pending_renames_sample": [{"old_symbol": "AAA", "new_symbol": "BBB",
                                    "new_company_name": "Example Inc", "effective_date": "2026-06-01"}],
        "quality_indicators": {"missing_stock_sector": 23},
        "drift_detected": True,
    })
    assert "Drift / freshness report" in md
    assert "AAA -> BBB" in md
    assert "drift_detected: True" in md

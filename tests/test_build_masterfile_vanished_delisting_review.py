from __future__ import annotations

import csv
import json
from pathlib import Path

from scripts.build_masterfile_vanished_delisting_review import build_review
from scripts.lib.delisting_evidence import BSE_STATUS_URL_TEMPLATE, evidence_observation_id


LISTING_FIELDS = ["ticker", "exchange", "name", "isin"]


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def vanished_ref(ticker: str, exchange: str, *, source_key: str = "official_feed", **extra: str) -> dict[str, str]:
    row = {
        "source_key": source_key,
        "source_url": "https://example.test/feed",
        "ticker": ticker,
        "exchange": exchange,
        "name": extra.get("name", f"{ticker} Ltd"),
        "isin": extra.get("isin", "XX0000000001"),
    }
    row.update(extra)
    return row


def official_bse_delisted(ticker: str = "DEAD") -> dict[str, str]:
    candidate = {
        "exchange": "BSE_IN",
        "ticker": ticker,
        "classification": "delisted",
        "name": "Dead Ltd",
        "isin": "INE1",
        "source_key": "bse_india_scrips",
        "source_url": BSE_STATUS_URL_TEMPLATE.format(status="Delisted"),
        "observed_at": "2026-04-05T00:00:00Z",
    }
    candidate["observation_id"] = evidence_observation_id(candidate, candidate["observed_at"])
    return candidate


def run_review(
    tmp_path: Path,
    *,
    vanished: list[dict[str, str]],
    listings: list[dict[str, str]],
    delisting_candidates: list[dict[str, str]] | None = None,
    previous_rows: list[dict[str, object]] | None = None,
) -> dict:
    rotation = tmp_path / "rotation.json"
    listings_csv = tmp_path / "listings.csv"
    delisting = tmp_path / "delisting.json"
    previous = tmp_path / "previous.json"
    report_json = tmp_path / "review.json"
    report_md = tmp_path / "review.md"
    write_json(rotation, {"vanished": vanished})
    write_csv(listings_csv, LISTING_FIELDS, listings)
    write_json(delisting, {"candidates": delisting_candidates or []})
    write_json(previous, {"rows": previous_rows or []})
    return build_review(
        rotation_diff_json=rotation,
        listings_csv=listings_csv,
        delisting_report_json=delisting,
        previous_review_json=previous,
        report_json=report_json,
        report_md=report_md,
    )


def test_vanished_row_not_in_listings_is_not_dropped(tmp_path: Path) -> None:
    report = run_review(
        tmp_path,
        vanished=[vanished_ref("EXTRA", "FSX")],
        listings=[],
    )
    row = report["rows"][0]
    assert row["classifier_action"] == "not_in_database"
    assert row["still_in_database"] is False
    assert row["would_apply_drop"] is False
    assert report["applied_drops"] == 0


def test_vanished_listing_without_delisting_evidence_stays_manual(tmp_path: Path) -> None:
    report = run_review(
        tmp_path,
        vanished=[vanished_ref("KEEP", "TSE", name="Keep Co", isin="JP1")],
        listings=[{"ticker": "KEEP", "exchange": "TSE", "name": "Keep Co.", "isin": "JP1"}],
    )
    row = report["rows"][0]
    assert row["classifier_action"] == "manual_rename_vs_delisting_required"
    assert row["still_in_database"] is True
    assert row["listing_name"] == "Keep Co."
    assert row["would_apply_drop"] is False
    assert report["applied_drops"] == 0


def test_suspended_official_status_blocks_drop(tmp_path: Path) -> None:
    report = run_review(
        tmp_path,
        vanished=[vanished_ref("ANANDPROJ", "BSE_IN", source_key="bse_india_scrips")],
        listings=[{"ticker": "ANANDPROJ", "exchange": "BSE_IN", "name": "Anand Projects Ltd", "isin": "INE1"}],
        delisting_candidates=[
            {
                "exchange": "BSE_IN",
                "ticker": "ANANDPROJ",
                "classification": "suspended",
                "name": "Anand Projects Ltd",
                "isin": "INE1",
                "source_key": "bse_india_scrips",
            }
        ],
    )
    row = report["rows"][0]
    assert row["classifier_action"] == "blocked_suspended_kept_by_policy"
    assert row["classification"] == "suspended"
    assert row["would_apply_drop"] is False


def test_official_delisting_evidence_is_reported_not_applied(tmp_path: Path) -> None:
    drops = tmp_path / "drop_entries.csv"
    drops.write_text("ticker,exchange,confidence,reason\n", encoding="utf-8")
    report = run_review(
        tmp_path,
        vanished=[vanished_ref("DEAD", "BSE_IN", source_key="bse_india_scrips", isin="INE1")],
        listings=[{"ticker": "DEAD", "exchange": "BSE_IN", "name": "Dead Ltd", "isin": "INE1"}],
        delisting_candidates=[official_bse_delisted()],
    )
    row = report["rows"][0]
    assert row["classifier_action"] == "apply_drop_override"
    assert row["would_apply_drop"] is True
    assert report["applied_drops"] == 0
    assert drops.read_text(encoding="utf-8") == "ticker,exchange,confidence,reason\n"


def test_still_in_database_backlog_is_carried_and_stale_absences_are_not(tmp_path: Path) -> None:
    report = run_review(
        tmp_path,
        vanished=[vanished_ref("NEW", "XETRA")],
        listings=[
            {"ticker": "KEEP", "exchange": "TSE", "name": "Keep Co", "isin": "JP1"},
            {"ticker": "NEW", "exchange": "XETRA", "name": "New AG", "isin": "DE1"},
        ],
        previous_rows=[
            {
                "ticker": "KEEP",
                "exchange": "TSE",
                "source_key": "jpx_tse_stock_detail",
                "name": "Keep Co",
                "isin": "JP1",
                "still_in_database": True,
            },
            {
                "ticker": "GONE",
                "exchange": "TWSE",
                "source_key": "twse_etf_list",
                "name": "Dual currency leftover",
                "still_in_database": False,
            },
        ],
    )
    identities = {(row["exchange"], row["ticker"], row["origin"]) for row in report["rows"]}
    assert ("XETRA", "NEW", "rotation") in identities
    assert ("TSE", "KEEP", "backlog") in identities
    assert all(row["ticker"] != "GONE" for row in report["rows"])
    assert report["backlog_rows"] == 1
    assert report["rotation_vanished_rows"] == 1
    assert report["policy"] == "feed_delisting_classifier_not_direct_deletion"

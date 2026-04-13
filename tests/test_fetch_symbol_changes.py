from __future__ import annotations

import csv
from argparse import Namespace

from scripts.fetch_symbol_changes import (
    build_reviews,
    parse_stockanalysis_changes,
    run,
)


FIXTURE_HTML = """
<html>
  <body>
    <table>
      <thead><tr><th>Date</th><th>Old</th><th>New</th><th>New Company Name</th></tr></thead>
      <tbody>
        <tr>
          <td>Apr 8, 2026</td>
          <td>CAPT</td>
          <td><a href="/quote/otc/CPTAF/">CPTAF</a></td>
          <td>Captivision Inc</td>
        </tr>
        <tr>
          <td>Apr 7, 2026</td>
          <td>DCOMG</td>
          <td><a href="/stocks/dcbg/">DCBG</a></td>
          <td>Dime Community Bancshares Inc</td>
        </tr>
      </tbody>
    </table>
  </body>
</html>
"""


def test_parse_stockanalysis_changes_extracts_table_rows():
    changes = parse_stockanalysis_changes(
        FIXTURE_HTML,
        source_url="https://stockanalysis.com/actions/changes/",
        observed_at="2026-04-13T00:00:00Z",
    )

    assert [change.effective_date for change in changes] == ["2026-04-08", "2026-04-07"]
    assert changes[0].old_symbol == "CAPT"
    assert changes[0].new_symbol == "CPTAF"
    assert changes[0].source_exchange_hint == "OTC"
    assert changes[1].source_exchange_hint == "US_LISTED"
    assert changes[0].source_confidence == "secondary_review"
    assert changes[0].review_needed == "true"


def test_build_reviews_classifies_current_listing_state():
    changes = parse_stockanalysis_changes(
        FIXTURE_HTML,
        source_url="https://stockanalysis.com/actions/changes/",
        observed_at="2026-04-13T00:00:00Z",
    )
    listings = [
        {"listing_key": "NASDAQ::CAPT", "ticker": "CAPT", "exchange": "NASDAQ"},
        {"listing_key": "NASDAQ::DCBG", "ticker": "DCBG", "exchange": "NASDAQ"},
    ]

    reviews = build_reviews(changes, listings)

    assert reviews[0].match_status == "old_symbol_present_new_symbol_missing"
    assert reviews[0].recommended_action == "review_possible_rename_or_delisting"
    assert reviews[0].old_listing_keys == "NASDAQ::CAPT"
    assert reviews[1].match_status == "new_symbol_present_old_symbol_missing"
    assert reviews[1].recommended_action == "already_reflected_or_new_symbol_added"
    assert reviews[1].new_listing_keys == "NASDAQ::DCBG"


def test_run_writes_merged_changes_and_review_outputs(tmp_path):
    html_path = tmp_path / "changes.html"
    listings_csv = tmp_path / "listings.csv"
    html_path.write_text(FIXTURE_HTML, encoding="utf-8")
    with listings_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["listing_key", "ticker", "exchange"])
        writer.writeheader()
        writer.writerow({"listing_key": "NASDAQ::CAPT", "ticker": "CAPT", "exchange": "NASDAQ"})

    args = Namespace(
        url="https://stockanalysis.com/actions/changes/",
        html_in=html_path,
        listings_csv=listings_csv,
        changes_csv=tmp_path / "symbol_changes.csv",
        changes_json=tmp_path / "symbol_changes.json",
        review_csv=tmp_path / "symbol_changes_review.csv",
        review_json=tmp_path / "symbol_changes_review.json",
        review_md=tmp_path / "symbol_changes_review.md",
        timeout_seconds=30.0,
    )

    payload = run(args)

    assert payload["summary"]["fetched_rows"] == 2
    assert payload["summary"]["merged_history_rows"] == 2
    review_rows = list(csv.DictReader(args.review_csv.open()))
    assert len(review_rows) == 2
    assert review_rows[0]["source_confidence"] == "secondary_review"
    assert "secondary-source symbol-change feed" in args.review_md.read_text()

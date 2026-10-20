import csv
import zipfile

from scripts.build_gleif_isin_lei_compare import compare, run


def test_gleif_match_mismatch_missing_and_no_input_writes(tmp_path):
    listings = tmp_path / "listings.csv"
    listings.write_text("listing_key,isin,exchange\nX::A,US0378331005,X\nX::B,US5949181045,X\nX::C,US4592001014,X\n")
    identifiers = tmp_path / "identifiers.csv"
    identifiers.write_text("listing_key,lei\nX::A,LEI-A\nX::B,WRONG\nX::C,\n")
    before = [p.read_bytes() for p in [listings, identifiers]]
    source = tmp_path / "gleif.zip"
    with zipfile.ZipFile(source, "w") as archive:
        archive.writestr("relationships.csv", "LEI,ISIN\nLEI-A,US0378331005\nLEI-B,US5949181045\nLEI-C,US4592001014\n")
    result = run(listings, identifiers, source, tmp_path / "reports")
    assert result["summary"]["lei_match"] == 1
    assert result["summary"]["lei_mismatch"] == 1
    assert result["summary"]["lei_missing_in_adanos"] == 1
    assert [p.read_bytes() for p in [listings, identifiers]] == before
    assert len(result["sources"][0]["sha256"]) == 64
    queue = list(csv.DictReader((tmp_path / "reports/gleif_lei_conflict_review_queue.csv").open()))
    assert len(queue) == 1 and queue[0]["listing_key"] == "X::B"
    assert queue[0]["direct_identifier_apply_allowed_rows"] == "0"


def test_gleif_missing_source_and_multiple_lei_are_review_only():
    listings = [{"listing_key": "X::A", "exchange": "X", "isin": "US0378331005"}]
    metrics, queue = compare(listings, [], {})
    assert metrics == [{"exchange": "X", "flag": "lei_missing_in_gleif", "listing_rows": 1}]
    assert queue == []
    _, queue = compare(listings, [], {"US0378331005": {"LEI-A", "LEI-B"}})
    assert "same_isin_multiple_lei" in queue[0]["flags"]
    assert queue[0]["direct_identifier_apply_allowed_rows"] == 0

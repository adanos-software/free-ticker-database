import json

import pytest

from scripts.build_sec_listed_current_compare import compare, run


def test_sec_join_scope_and_no_mutation(tmp_path):
    listings = tmp_path / "listings.csv"
    listings.write_text("listing_key,ticker,exchange\nNASDAQ::AAA,AAA,NASDAQ\nNYSE MKT::BBB,BBB,NYSE MKT\n")
    before = listings.read_bytes()
    source = tmp_path / "sec.json"
    source.write_text(json.dumps({"fields": ["cik", "name", "ticker", "exchange"], "data": [
        [1, "A Inc", "AAA", "Nasdaq"], [2, "B Inc", "BBB", "NYSE"],
        [3, "C warrants", "CCC-WT", "Nasdaq"]]}))
    report = run(listings, source, tmp_path / "reports")
    assert report["summary"] == {"matched_listed_current": 2, "sec_only_scope_review": 1}
    assert listings.read_bytes() == before
    assert (tmp_path / "reports/sec_listed_current_compare.csv").exists()
    assert report["direct_identifier_apply_allowed_rows"] == 0


def test_sec_ambiguous_exchange_does_not_pick_a_venue():
    rows = [{"listing_key": venue + "::AAA", "ticker": "AAA", "exchange": venue} for venue in ["NYSE", "NYSE MKT"]]
    result = compare(rows, {"fields": ["cik", "name", "ticker", "exchange"], "data": [[1, "A", "AAA", "NYSE"]]})
    assert result[0]["status"] == "ambiguous_venue_review"


def test_sec_rejects_wrong_payload():
    with pytest.raises(ValueError):
        compare([], {"fields": ["ticker"], "data": []})

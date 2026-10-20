import json

import pytest

from scripts.build_openfigi_isin_mic_remap import classify, request_for, request_key, run


ROW = {"listing_key": "NASDAQ::IBM", "ticker": "IBM", "exchange": "NASDAQ", "name": "International Business Machines Corp", "isin": "US4592001014"}
RESULT = {"data": [{"ticker": "IBM", "name": "International Business Machines", "figi": "VENUE", "compositeFIGI": "COMPOSITE", "shareClassFIGI": "SHARE", "securityType": "Common Stock"}]}


def test_openfigi_compares_explicit_levels_only():
    assert classify(ROW, "VENUE", RESULT)["figi_level_agreement"] == "venue_match"
    assert classify(ROW, "COMPOSITE", RESULT)["figi_level_agreement"] == "local_composite_level"
    assert classify(ROW, "SHARE", RESULT)["figi_level_agreement"] == "local_share_class_level"
    unknown = classify(ROW, "BBG9UNKNOWN", RESULT)
    assert unknown["figi_level_agreement"] == "local_level_unknown_or_disagreement"
    assert not unknown["stolen_isin_candidate"]
    assert classify(ROW, "", {"warning": "No identifier found."})["status"] == "unmapped"
    assert classify(ROW, "", {"error": "failure"})["status"] == "api_error"


def test_exact_ticker_name_disagreement_is_review_only():
    result = classify({**ROW, "name": "Completely Different Mining Limited"}, "OTHER", RESULT)
    assert result["stolen_isin_candidate"]
    assert "review only" in result["reason"]
    assert classify({**ROW, "ticker": "OTHER"}, "OTHER", RESULT)["status"] == "ticker_not_resolved"
    assert classify(ROW, "", {"data": [RESULT["data"][0], {**RESULT["data"][0], "figi": "OTHER"}]})["status"] == "ambiguous_mapping"


def test_cached_report_no_mutation_and_exact_request_pin(tmp_path):
    listings = tmp_path / "listings.csv"
    listings.write_text("listing_key,ticker,exchange,name,isin\nNASDAQ::IBM,IBM,NASDAQ,International Business Machines,US4592001014\nNASDAQ::BAD,BAD,NASDAQ,Bad,INVALID\n")
    identifiers = tmp_path / "identifiers.csv"
    identifiers.write_text("listing_key,figi\nNASDAQ::IBM,COMPOSITE\n")
    mapping = tmp_path / "mapping.csv"
    mapping.write_text("exchange_code,operating_mic,segment_mic\nNASDAQ,XNAS,XNGS\n")
    cache = tmp_path / "cache"
    cache.mkdir()
    job = request_for(ROW, {"operating_mic": "XNAS", "segment_mic": "XNGS"})
    assert job == {"idType": "ID_ISIN", "idValue": "US4592001014", "micCode": "XNGS", "includeUnlistedEquities": False}
    path = cache / (request_key(job) + ".json")
    path.write_text(json.dumps({"request": job, "response": RESULT}))
    before = listings.read_bytes(), identifiers.read_bytes()
    result = run(listings, identifiers, mapping, cache, tmp_path / "reports")
    assert result["summary"]["eligible_listings"] == 1
    assert result["summary"]["mapped"] == 1
    assert (listings.read_bytes(), identifiers.read_bytes()) == before
    assert result["direct_identifier_apply_allowed_rows"] == 0
    replay = run(listings, identifiers, mapping, tmp_path / "empty-cache", tmp_path / "replay",
                 probe_json=tmp_path / "reports/openfigi_isin_mic_remap_probe.json")
    assert replay["summary"] == result["summary"]
    path.write_text(json.dumps({"request": {**job, "micCode": "WRONG"}, "response": RESULT}))
    with pytest.raises(ValueError, match="request mismatch"):
        run(listings, identifiers, mapping, cache, tmp_path / "reports")


def test_malformed_openfigi_result_is_not_counted_as_unmapped():
    with pytest.raises(ValueError, match="Malformed"):
        classify(ROW, "", {"unexpected": "payload"})

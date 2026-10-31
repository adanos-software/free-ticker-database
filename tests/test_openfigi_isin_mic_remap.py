import json

import pytest

from scripts.build_openfigi_isin_mic_remap import classify, request_for, request_key, run
from scripts.lib.dataio import load_csv
from scripts.lib.normalize import names_match


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


@pytest.mark.parametrize(
    ("local_name", "openfigi_name"),
    [
        ("Compass Minerals International Inc", "COMPASS MINERALS INTERNATION"),
        ("Spin Master Corp.", "SPIN MASTER CORP-SUB VTG SHR"),
        ("Vanguard Mid-Cap Growth Index Fund ETF Shares", "VNGRD MRNGSTR MC GRW ETF-USD"),
        ("iShares U.S. Home Construction ETF", "ISHARES U.S. HOME CONSTRUCTI"),
    ],
)
def test_names_match_rejects_openfigi_stolen_isin_false_positives(local_name: str, openfigi_name: str) -> None:
    assert names_match(local_name, openfigi_name)


def test_names_match_still_flags_unrelated_issuers():
    assert not names_match("Credit Agricole SA", "Arcosa Inc")


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


def test_stolen_isin_review_queue_is_never_apply_list(tmp_path):
    listings = tmp_path / "listings.csv"
    listings.write_text(
        "listing_key,ticker,exchange,name,isin\n"
        "NYSE::ZZZ,ZZZ,NYSE,Alpha Mining Corp,US4592001014\n"
    )
    identifiers = tmp_path / "identifiers.csv"
    identifiers.write_text("listing_key,figi\nNYSE::ZZZ,LOCALFIGI\n")
    mapping = tmp_path / "mapping.csv"
    mapping.write_text("exchange_code,operating_mic,segment_mic\nNYSE,XNYS,\n")
    cache = tmp_path / "cache"
    cache.mkdir()
    row = {"listing_key": "NYSE::ZZZ", "ticker": "ZZZ", "exchange": "NYSE", "name": "Alpha Mining Corp", "isin": "US4592001014"}
    job = request_for(row, {"operating_mic": "XNYS", "segment_mic": ""})
    response = {"data": [{"ticker": "ZZZ", "name": "Totally Different Holdings Ltd", "figi": "VENUE", "compositeFIGI": "COMP", "shareClassFIGI": "SHR", "securityType": "Common Stock"}]}
    (cache / (request_key(job) + ".json")).write_text(json.dumps({"request": job, "response": response}))
    run(listings, identifiers, mapping, cache, tmp_path / "reports")
    queue = load_csv(tmp_path / "reports/openfigi_stolen_isin_review_queue.csv")
    assert len(queue) == 1
    assert all(row["apply_allowed"] == "0" for row in queue)
    payload = json.loads((tmp_path / "reports/openfigi_stolen_isin_review_queue.json").read_text())
    assert payload["summary"]["apply_allowed_rows"] == 0
    assert payload["summary"]["direct_identifier_apply_allowed_rows"] == 0


def test_malformed_openfigi_result_is_not_counted_as_unmapped():
    with pytest.raises(ValueError, match="Malformed"):
        classify(ROW, "", {"unexpected": "payload"})

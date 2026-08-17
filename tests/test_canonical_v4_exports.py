from __future__ import annotations

import csv
import json
import uuid
from pathlib import Path

import pytest

from scripts.build_canonical_v4 import build, valid_isin
from scripts.validate_canonical_v4_exports import _validate_type, validate


def write_csv(path: Path, fields: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader(); writer.writerows(rows)


def fixture(tmp_path: Path, *, with_event: bool = True) -> dict[str, Path]:
    data = tmp_path / "data"
    listings = data / "listings.csv"
    write_csv(listings, ["listing_key","ticker","exchange","name","asset_type","stock_sector","etf_category","country","country_code","isin","aliases"], [{
        "listing_key":"X::AAA","ticker":"AAA","exchange":"X","name":"Alpha Power Inc","asset_type":"Stock",
        "stock_sector":"Utilities","etf_category":"","country":"United States","country_code":"US","isin":"US0378331005","aliases":"alpha power",
    }])
    scopes = data / "instrument_scopes.csv"
    write_csv(scopes, ["listing_key","ticker","exchange","asset_type","isin","instrument_group_key","instrument_scope","scope_reason","primary_listing_key"], [{
        "listing_key":"X::AAA","ticker":"AAA","exchange":"X","asset_type":"Stock","isin":"US0378331005","instrument_group_key":"US0378331005","instrument_scope":"core","scope_reason":"primary_listing","primary_listing_key":"X::AAA",
    }])
    identifiers = data / "identifiers_extended.csv"
    write_csv(identifiers, ["listing_key","ticker","exchange","isin","wkn","figi","cik","lei","figi_source","cik_source","lei_source"], [{
        "listing_key":"X::AAA","ticker":"AAA","exchange":"X","isin":"US0378331005","wkn":"","figi":"BBG000TEST01","cik":"0000001","lei":"","figi_source":"OpenFIGI","cik_source":"SEC company_tickers_exchange.json","lei_source":"",
    }])
    status = data / "history/listing_status_history.csv"
    write_csv(status, ["listing_key","ticker","exchange","status","first_observed_at","last_observed_at","effective_at","status_source","source_report","evidence_status"], [{
        "listing_key":"X::AAA","ticker":"AAA","exchange":"X","status":"active","first_observed_at":"2026-01-01T00:00:00Z","last_observed_at":"2026-08-17T00:00:00Z","effective_at":"","status_source":"snapshot","source_report":"","evidence_status":"current_snapshot",
    }])
    events = data / "history/listing_events.csv"
    event_rows = [{
        "listing_key":"X::AAA","ticker":"AAA","exchange":"X","event_type":"renamed","field_name":"name","old_value":"Alpha Energy Inc","new_value":"Alpha Power Inc","before_row_sha256":"a"*64,"effective_at":"2026-02-01T00:00:00Z","observed_at":"2026-02-02T00:00:00Z","source_key":"official_x","source_url":"https://example.test/x","source_report":"","observation_id":"raw-event-7","evidence_status":"official",
    }] if with_event else []
    write_csv(events, ["listing_key","ticker","exchange","event_type","field_name","old_value","new_value","before_row_sha256","effective_at","observed_at","source_key","source_url","source_report","observation_id","evidence_status"], event_rows)
    reference = data / "masterfiles/reference.csv"
    write_csv(reference, ["source_key","provider","source_url","ticker","name","exchange","asset_type","listing_status","reference_scope","official","isin","cfi","sector"], [{
        "source_key":"official_x","provider":"Exchange X","source_url":"https://example.test/x","ticker":"AAA","name":"Alpha Power Inc","exchange":"X","asset_type":"Stock","listing_status":"active","reference_scope":"exchange_directory","official":"true","isin":"US0378331005","cfi":"","sector":"Utilities",
    }])
    sources = data / "masterfiles/sources.json"
    sources.write_text(json.dumps([{"key":"official_x","provider":"Exchange X","description":"Official directory","source_url":"https://example.test/x","format":"csv","reference_scope":"exchange_directory","official":True}]), encoding="utf-8")
    summary = data / "masterfiles/summary.json"
    summary.write_text(json.dumps({"source_details":{"official_x":{"generated_at":"2026-08-16T00:00:00Z","mode":"network"}}}), encoding="utf-8")
    coverage = data / "reports/coverage_contracts.csv"
    write_csv(coverage, ["contract_id","contract_key","exchange","asset_type","claim_type","venue_status","source_keys","reference_scopes","denominator_method","denominator","observed_reference_keys","covered_reference_keys","missing_reference_keys","identity_conflict_keys","unclassified_keys","recall_pct","minimum_recall_pct","freshness_status","license_status","contract_status","freshness_failures","license_failures","required_next_action"], [{
        "contract_id":"x","contract_key":"X::Stock","exchange":"X","asset_type":"Stock","claim_type":"official_full","venue_status":"official_full","source_keys":"official_x","reference_scopes":"exchange_directory","denominator_method":"test","denominator":"1","observed_reference_keys":"1","covered_reference_keys":"1","missing_reference_keys":"0","identity_conflict_keys":"0","unclassified_keys":"0","recall_pct":"100.0","minimum_recall_pct":"99.5","freshness_status":"pass","license_status":"fail","contract_status":"fail_license","freshness_failures":"","license_failures":"review required","required_next_action":"review",
    }])
    tickers = data / "tickers.json"
    tickers.write_text(json.dumps({"_meta":{"built_at":"2026-08-17T00:00:00Z"}}), encoding="utf-8")
    mic = data / "masterfiles/venue_mic_mapping.csv"
    write_csv(mic, ["exchange_code","operating_mic","segment_mic","canonical_name","country_code","evidence_url","reviewed_at","reviewer"], [])
    return {"listings":listings,"scopes":scopes,"identifiers":identifiers,"status":status,"events":events,"reference":reference,"sources":sources,"summary":summary,"coverage":coverage,"tickers":tickers,"mic":mic,"out":data/"canonical_v4"}


def run_build(tmp_path: Path, *, with_event: bool = True) -> tuple[dict[str, Path], dict]:
    paths = fixture(tmp_path, with_event=with_event)
    manifest = build(
        out_dir=paths["out"], listings_csv=paths["listings"], scopes_csv=paths["scopes"], identifiers_csv=paths["identifiers"],
        status_history_csv=paths["status"], listing_events_csv=paths["events"], reference_csv=paths["reference"],
        sources_json=paths["sources"], masterfile_summary_json=paths["summary"], coverage_contracts_csv=paths["coverage"],
        mic_mapping_csv=paths["mic"], tickers_json=paths["tickers"], built_at="2026-08-17T00:00:00Z", git_commit="1"*40,
    )
    return paths, manifest


def test_build_and_validate_canonical_v4(tmp_path: Path) -> None:
    paths, manifest = run_build(tmp_path)
    result = validate(data_dir=paths["out"], compatibility_listings=paths["listings"])
    assert result["status"] == "pass"
    assert manifest["git_commit"] == "1" * 40
    assert manifest["counts"]["listings"] == 1


def test_listing_event_observation_ids_are_canonical_uuids_and_have_fk_targets(tmp_path: Path) -> None:
    paths, _ = run_build(tmp_path)
    with (paths["out"] / "listing_events.csv").open(newline="", encoding="utf-8") as handle:
        event = next(csv.DictReader(handle))
    uuid.UUID(event["observation_id"])
    with (paths["out"] / "source_observations.csv").open(newline="", encoding="utf-8") as handle:
        observations = {row["observation_id"] for row in csv.DictReader(handle)}
    assert event["observation_id"] in observations


def test_build_is_deterministic_for_fixed_inputs(tmp_path: Path) -> None:
    paths, first = run_build(tmp_path)
    hashes = {item["path"]: item["sha256"] for item in first["files"]}
    second = build(
        out_dir=paths["out"], listings_csv=paths["listings"], scopes_csv=paths["scopes"], identifiers_csv=paths["identifiers"],
        status_history_csv=paths["status"], listing_events_csv=paths["events"], reference_csv=paths["reference"],
        sources_json=paths["sources"], masterfile_summary_json=paths["summary"], coverage_contracts_csv=paths["coverage"],
        mic_mapping_csv=paths["mic"], tickers_json=paths["tickers"], built_at="2026-08-17T00:00:00Z", git_commit="1"*40,
    )
    assert hashes == {item["path"]: item["sha256"] for item in second["files"]}
    assert first["aggregate_sha256"] == second["aggregate_sha256"]


def test_validator_rejects_listing_key_mismatch(tmp_path: Path) -> None:
    paths, _ = run_build(tmp_path)
    listing_path = paths["out"] / "listings.csv"
    rows = list(csv.DictReader(listing_path.open(newline="", encoding="utf-8")))
    rows[0]["listing_key"] = "X::WRONG"
    write_csv(listing_path, list(rows[0]), rows)
    with pytest.raises(ValueError, match="listing_key"):
        validate(data_dir=paths["out"], compatibility_listings=paths["listings"])


def test_manifest_requires_full_commit_sha(tmp_path: Path) -> None:
    paths = fixture(tmp_path)
    with pytest.raises(ValueError, match="40-character"):
        build(out_dir=paths["out"], listings_csv=paths["listings"], scopes_csv=paths["scopes"], identifiers_csv=paths["identifiers"], status_history_csv=paths["status"], listing_events_csv=paths["events"], reference_csv=paths["reference"], sources_json=paths["sources"], masterfile_summary_json=paths["summary"], coverage_contracts_csv=paths["coverage"], mic_mapping_csv=paths["mic"], tickers_json=paths["tickers"], built_at="2026-08-17T00:00:00Z", git_commit="short")


def test_isin_validation_requires_checksum_not_only_format() -> None:
    assert valid_isin("US0378331005")
    assert not valid_isin("US0378331006")
    assert not _validate_type("US0378331006", "isin")


def test_conflicting_isin_rows_are_split_and_quarantined(tmp_path: Path) -> None:
    paths = fixture(tmp_path, with_event=False)
    fields = [
        "listing_key", "ticker", "exchange", "name", "asset_type", "stock_sector",
        "etf_category", "country", "country_code", "isin", "aliases",
    ]
    conflict_isin = "US0378331005"
    write_csv(paths["listings"], fields, [
        {
            "listing_key": "X::AAA", "ticker": "AAA", "exchange": "X",
            "name": "Alpha Power Inc", "asset_type": "Stock", "stock_sector": "Utilities",
            "etf_category": "", "country": "United States", "country_code": "US",
            "isin": conflict_isin, "aliases": "alpha power",
        },
        {
            "listing_key": "Y::BBB", "ticker": "BBB", "exchange": "Y",
            "name": "Beta Foods Inc", "asset_type": "Stock", "stock_sector": "Consumer Staples",
            "etf_category": "", "country": "United States", "country_code": "US",
            "isin": conflict_isin, "aliases": "beta foods",
        },
    ])
    write_csv(paths["scopes"], [
        "listing_key", "ticker", "exchange", "asset_type", "isin", "instrument_group_key",
        "instrument_scope", "scope_reason", "primary_listing_key",
    ], [
        {
            "listing_key": "X::AAA", "ticker": "AAA", "exchange": "X", "asset_type": "Stock",
            "isin": conflict_isin, "instrument_group_key": conflict_isin, "instrument_scope": "core",
            "scope_reason": "primary_listing", "primary_listing_key": "X::AAA",
        },
        {
            "listing_key": "Y::BBB", "ticker": "BBB", "exchange": "Y", "asset_type": "Stock",
            "isin": conflict_isin, "instrument_group_key": conflict_isin, "instrument_scope": "core",
            "scope_reason": "primary_listing", "primary_listing_key": "Y::BBB",
        },
    ])
    write_csv(paths["identifiers"], [
        "listing_key", "ticker", "exchange", "isin", "wkn", "figi", "cik", "lei",
        "figi_source", "cik_source", "lei_source",
    ], [])
    write_csv(paths["status"], [
        "listing_key", "ticker", "exchange", "status", "first_observed_at", "last_observed_at",
        "effective_at", "status_source", "source_report", "evidence_status",
    ], [
        {
            "listing_key": key, "ticker": ticker, "exchange": exchange, "status": "active",
            "first_observed_at": "2026-01-01T00:00:00Z", "last_observed_at": "2026-08-17T00:00:00Z",
            "effective_at": "", "status_source": "snapshot", "source_report": "",
            "evidence_status": "current_snapshot",
        }
        for key, ticker, exchange in (("X::AAA", "AAA", "X"), ("Y::BBB", "BBB", "Y"))
    ])
    write_csv(paths["reference"], [
        "source_key", "provider", "source_url", "ticker", "name", "exchange", "asset_type",
        "listing_status", "reference_scope", "official", "isin", "cfi", "sector",
    ], [])
    write_csv(paths["mic"], [
        "exchange_code", "operating_mic", "segment_mic", "canonical_name", "country_code",
        "evidence_url", "reviewed_at", "reviewer",
    ], [])

    manifest = build(
        out_dir=paths["out"], listings_csv=paths["listings"], scopes_csv=paths["scopes"],
        identifiers_csv=paths["identifiers"], status_history_csv=paths["status"],
        listing_events_csv=paths["events"], reference_csv=paths["reference"],
        sources_json=paths["sources"], masterfile_summary_json=paths["summary"],
        coverage_contracts_csv=paths["coverage"], mic_mapping_csv=paths["mic"],
        tickers_json=paths["tickers"], built_at="2026-08-17T00:00:00Z", git_commit="1" * 40,
    )

    instruments = list(csv.DictReader((paths["out"] / "instruments.csv").open(newline="", encoding="utf-8")))
    current_listings = list(csv.DictReader((paths["out"] / "listings.csv").open(newline="", encoding="utf-8")))
    assertions = list(csv.DictReader((paths["out"] / "identifier_assertions.csv").open(newline="", encoding="utf-8")))
    gaps = list(csv.DictReader((paths["out"] / "provenance_gaps.csv").open(newline="", encoding="utf-8")))

    assert len({row["instrument_id"] for row in current_listings if row["current"] == "true"}) == 2
    assert all(row["isin"] == "" for row in instruments if row["status"] == "active")
    isin_assertions = [row for row in assertions if row["scheme"] == "ISIN"]
    assert len(isin_assertions) == 2
    assert {row["adjudication_status"] for row in isin_assertions} == {"quarantined_identity_conflict"}
    assert {row["value"] for row in isin_assertions} == {conflict_isin}
    assert sum(row["gap_class"] == "conflicting_identifier_assertion" for row in gaps) == 2
    assert {row["listing_key"] for row in gaps if row["gap_class"] == "conflicting_identifier_assertion"} == {"X::AAA", "Y::BBB"}
    assert manifest["identity_quarantine"] == {"conflict_groups": 1, "listing_rows": 2}
    assert validate(data_dir=paths["out"], compatibility_listings=paths["listings"])["status"] == "pass"


def test_validator_binds_manifest_to_expected_commit(tmp_path: Path) -> None:
    paths, _ = run_build(tmp_path)
    with pytest.raises(ValueError, match="manifest git_commit"):
        validate(
            data_dir=paths["out"],
            compatibility_listings=paths["listings"],
            expected_git_commit="2" * 40,
        )


def test_validator_rejects_manifest_row_count_tampering(tmp_path: Path) -> None:
    paths, _ = run_build(tmp_path)
    manifest_path = paths["out"] / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["files"][0]["rows"] += 1
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="row count mismatch"):
        validate(data_dir=paths["out"], compatibility_listings=paths["listings"])


def test_validator_rejects_manifest_source_hash_tampering(tmp_path: Path) -> None:
    paths, _ = run_build(tmp_path)
    manifest_path = paths["out"] / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["source_dataset_sha256"] = "0" * 64
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="source_dataset_sha256"):
        validate(data_dir=paths["out"], compatibility_listings=paths["listings"])


def test_schema_contract_columns_match_postgres_schema() -> None:
    from scripts.validate_canonical_v4_exports import CONTRACT_JSON, SCHEMA_SQL, schema_table_columns

    contract = json.loads(CONTRACT_JSON.read_text(encoding="utf-8"))
    assert schema_table_columns(SCHEMA_SQL) == {
        table: list(spec["columns"]) for table, spec in contract["tables"].items()
    }


def test_validator_rejects_schema_contract_drift(tmp_path: Path) -> None:
    paths, _ = run_build(tmp_path)
    from scripts.validate_canonical_v4_exports import SCHEMA_SQL

    broken = tmp_path / "broken.sql"
    broken.write_text(
        SCHEMA_SQL.read_text(encoding="utf-8").replace("  listing_key text not null,\n  field_name", "  field_name", 1),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="schema columns differ for provenance_gaps"):
        validate(
            data_dir=paths["out"], compatibility_listings=paths["listings"], schema_sql=broken
        )


def test_unknown_identifier_source_is_provisional_not_accepted(tmp_path: Path) -> None:
    paths = fixture(tmp_path, with_event=False)
    write_csv(paths["identifiers"], [
        "listing_key", "ticker", "exchange", "isin", "wkn", "figi", "cik", "lei",
        "figi_source", "cik_source", "lei_source",
    ], [{
        "listing_key": "X::AAA", "ticker": "AAA", "exchange": "X",
        "isin": "US0378331005", "wkn": "865985", "figi": "", "cik": "", "lei": "",
        "figi_source": "", "cik_source": "", "lei_source": "",
    }])
    manifest = build(
        out_dir=paths["out"], listings_csv=paths["listings"], scopes_csv=paths["scopes"],
        identifiers_csv=paths["identifiers"], status_history_csv=paths["status"],
        listing_events_csv=paths["events"], reference_csv=paths["reference"],
        sources_json=paths["sources"], masterfile_summary_json=paths["summary"],
        coverage_contracts_csv=paths["coverage"], mic_mapping_csv=paths["mic"],
        tickers_json=paths["tickers"], built_at="2026-08-17T00:00:00Z", git_commit="1" * 40,
    )
    assert manifest["counts"]["identifier_assertions"] >= 2
    assertions = list(csv.DictReader((paths["out"] / "identifier_assertions.csv").open(newline="", encoding="utf-8")))
    wkn = next(row for row in assertions if row["scheme"] == "WKN")
    assert wkn["adjudication_status"] == "provisional"
    assert wkn["confidence"] == "0.6000"


def test_cik_and_lei_remain_listing_scoped_until_issuer_is_adjudicated(tmp_path: Path) -> None:
    paths, _ = run_build(tmp_path)
    assertions = list(csv.DictReader((paths["out"] / "identifier_assertions.csv").open(newline="", encoding="utf-8")))
    assert all(row["entity_type"] == "listing" for row in assertions if row["scheme"] in {"CIK", "LEI"})
    issuers = list(csv.DictReader((paths["out"] / "issuers.csv").open(newline="", encoding="utf-8")))
    assert all(row["lei"] == "" for row in issuers)

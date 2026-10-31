import io
import json
import zipfile

import pytest

from scripts.build_firds_listing_compare import compare, parse_xml, run, _pool_summary


def xml(isin="US0378331005", mic="XLON", cfi="ESVUFR"):
    return f'<Root xmlns="urn:test"><RefData><FinInstrmGnlAttrbts><Id>{isin}</Id><ClssfctnTp>{cfi}</ClssfctnTp><NtnlCcy>USD</NtnlCcy></FinInstrmGnlAttrbts><TradgVnRltdAttrbts><Id>{mic}</Id></TradgVnRltdAttrbts></RefData></Root>'


def test_firds_parser_exact_join_segment_and_scope():
    observations = list(parse_xml(io.BytesIO(xml().encode())))
    assert observations[0]["isin"] == "US0378331005"
    observations += [{**observations[0], "mic": "SEG1"}, {**observations[0], "cfi": "EPUXXX"}]
    listings = [{"listing_key": "LSE::A", "exchange": "LSE", "asset_type": "Stock", "isin": "US0378331005"}]
    mapping = [{"exchange_code": "LSE", "operating_mic": "XLON", "segment_mic": ""}]
    registry = [{"MIC": "SEG1", "OPERATING MIC": "XLON", "MARKET CATEGORY CODE": "RMKT"}]
    rows = compare(listings, mapping, observations, registry, "2026-10-17")
    assert any(r["metric"] == "matched_isin_mic" and r["value"] == 1 for r in rows)
    assert any(r["resolution"] == "operating_vs_segment_mic" for r in rows)
    assert any(r["metric"] == "excluded_cfi_or_invalid_isin_rows" and r["value"] == 1 for r in rows)


def test_mic_not_in_snapshot_excluded_from_pooled_pct():
    listings = [{"listing_key": "XETRA::A", "exchange": "XETRA", "asset_type": "Stock", "isin": "DE0007164600"}]
    mapping = [
        {"exchange_code": "XETRA", "operating_mic": "XETR", "segment_mic": ""},
        {"exchange_code": "LSE", "operating_mic": "XLON", "segment_mic": ""},
    ]
    registry = [{"MIC": "XLON", "OPERATING MIC": "XLON", "MARKET CATEGORY CODE": "RMKT"},
                {"MIC": "XETR", "OPERATING MIC": "XETR", "MARKET CATEGORY CODE": "RMKT"}]
    observations = list(parse_xml(io.BytesIO(xml(mic="XLON").encode())))
    rows = [{"provider": "esma", **row} for row in compare(listings, mapping, observations, registry, "2026-10-17")]
    assert any(r["mic"] == "XETR" and r["metric"] == "mic_not_in_snapshot" and r["value"] == 1 for r in rows)
    pooled = _pool_summary(rows, provider="esma")
    assert "XETR" in pooled["mics_not_in_snapshot"]
    assert pooled["local_unique_isins"] == 0
    assert pooled["local_not_observed_review"] == 0


def test_firds_only_review_split_by_cfi_class():
    observations = [
        {"isin": "US0378331005", "cfi": "ESVUFR", "currency": "USD", "mic": "XLON", "first_traded": "", "terminated": ""},
        {"isin": "US5949181045", "cfi": "EDSXFR", "currency": "USD", "mic": "XLON", "first_traded": "", "terminated": ""},
        {"isin": "US78462F1030", "cfi": "CEOGEU", "currency": "USD", "mic": "XLON", "first_traded": "", "terminated": ""},
    ]
    listings = []
    mapping = [{"exchange_code": "LSE", "operating_mic": "XLON", "segment_mic": ""}]
    registry = [{"MIC": "XLON", "OPERATING MIC": "XLON", "MARKET CATEGORY CODE": "RMKT"}]
    rows = compare(listings, mapping, observations, registry, "2026-10-17")
    firds_only = [r for r in rows if r["metric"] == "firds_only_review"]
    assert {r["cfi_class"]: r["value"] for r in firds_only if r["resolution"] == "exact_mic"} == {"ES": 1, "ED": 1, "CE": 1}


def test_firds_report_has_metrics_only_and_preserves_listings(tmp_path):
    listings = tmp_path / "listings.csv"
    listings.write_text("listing_key,exchange,asset_type,isin\nLSE::A,LSE,Stock,US0378331005\n")
    before = listings.read_bytes()
    mapping = tmp_path / "mapping.csv"
    mapping.write_text("exchange_code,operating_mic,segment_mic\nLSE,XLON,\n")
    iso = tmp_path / "iso.csv"
    iso.write_text("MIC,OPERATING MIC,MARKET CATEGORY CODE\nXLON,XLON,RMKT\n")
    manifest = []
    for provider in ["esma", "fca"]:
        folder = tmp_path / provider
        folder.mkdir()
        for category in ["C", "E"]:
            path = folder / f"FULINS_{category}_20261017_01of01.zip"
            with zipfile.ZipFile(path, "w") as archive:
                archive.writestr("data.xml", xml(cfi="CEOGEU" if category == "C" else "ESVUFR"))
            domain = "https://firds.esma.europa.eu/firds/" if provider == "esma" else "https://data.fca.org.uk/artefacts/FIRDS/"
            manifest.append({"provider": provider, "path": str(path), "publication_date": "2026-10-17", "url": domain + path.name})
    source = tmp_path / "manifest.json"
    source.write_text(json.dumps(manifest))
    result = run(listings, mapping, source, iso, tmp_path / "reports")
    assert result["summary"]["matched_isin_mic"] == 2
    assert result["direct_identifier_apply_allowed_rows"] == 0
    assert listings.read_bytes() == before
    report_csv = (tmp_path / "reports/firds_listing_compare.csv").read_text()
    assert "US0378331005" not in report_csv
    assert "provider,mic,resolution,asset_type,cfi_class,metric,value" in report_csv
    source.write_text(json.dumps(manifest[:-1]))
    with pytest.raises(ValueError, match="Incomplete"):
        run(listings, mapping, source, iso, tmp_path / "reports")


def test_firds_rejects_empty_xml():
    with pytest.raises(ValueError, match="no RefData"):
        list(parse_xml(io.BytesIO(b"<Root/>")))

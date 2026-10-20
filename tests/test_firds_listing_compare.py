import io
import json
import zipfile

import pytest

from scripts.build_firds_listing_compare import compare, parse_xml, run


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
    assert "provider,mic,resolution,asset_type,metric,value" in report_csv
    source.write_text(json.dumps(manifest[:-1]))
    with pytest.raises(ValueError, match="Incomplete"):
        run(listings, mapping, source, iso, tmp_path / "reports")


def test_firds_rejects_empty_xml():
    with pytest.raises(ValueError, match="no RefData"):
        list(parse_xml(io.BytesIO(b"<Root/>")))

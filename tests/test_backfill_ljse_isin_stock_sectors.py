from scripts.backfill_ljse_isin_stock_sectors import (
    build_metadata_updates,
    evaluate_row,
    map_nace,
    parse_ljse_page,
)


def target(**overrides):
    values = {
        "ticker": "I69",
        "exchange": "FSX",
        "asset_type": "Stock",
        "name": "Pozavarovalnica Sava, d.d.",
        "isin": "SI0021110513",
        "stock_sector": "",
    }
    values.update(overrides)
    return values


SAVA_HTML = """
<li>ISIN</li>
<li>SI0021110513</li>
<a href="/en/sektor-303/302?sector_id=L">
<strong>L</strong>&nbsp;&middot;&nbsp;FINANCIAL AND INSURANCE ACTIVITIES
</a>
<li>NACE</li>
<li><strong>65200</strong>&nbsp;&middot;&nbsp;Reinsurance</li>
"""


def test_map_nace_maps_unambiguous_codes_only():
    assert map_nace("65200", "") == "Financials"
    assert map_nace("64190", "") == "Financials"
    assert map_nace("68200", "") == "Real Estate"
    assert map_nace("06100", "") == "Energy"
    assert map_nace("35110", "") == "Utilities"
    assert map_nace("61000", "") == "Communication Services"
    assert map_nace("", "FINANCIAL AND INSURANCE ACTIVITIES") == "Financials"
    assert map_nace("21200", "MANUFACTURING") == ""
    assert map_nace("", "C") == ""


def test_parse_ljse_page_reads_isin_nace_and_sector():
    parsed = parse_ljse_page(SAVA_HTML)
    assert parsed["ljse_isin"] == "SI0021110513"
    assert parsed["ljse_nace"] == "65200"
    assert parsed["ljse_nace_name"] == "Reinsurance"
    assert parsed["ljse_sector"] == "FINANCIAL AND INSURANCE ACTIVITIES"


def test_evaluate_row_accepts_exact_isin_reinsurance():
    result = evaluate_row(target(), parse_ljse_page(SAVA_HTML))

    assert result["decision"] == "accept"
    assert result["sector_update"] == "Financials"
    assert build_metadata_updates([result])[0]["proposed_value"] == "Financials"


def test_evaluate_row_rejects_isin_mismatch():
    parsed = parse_ljse_page(SAVA_HTML)
    result = evaluate_row(target(isin="SI0031102120", name="Krka"), parsed)

    assert result["decision"] == "isin_mismatch"
    assert result["sector_update"] == ""
    assert build_metadata_updates([result]) == []


def test_evaluate_row_rejects_unmapped_manufacturing():
    html = SAVA_HTML.replace("SI0021110513", "SI0031102120").replace("65200", "21200").replace(
        "Reinsurance", "Manufacture of pharmaceuticals"
    ).replace("FINANCIAL AND INSURANCE ACTIVITIES", "MANUFACTURING")
    result = evaluate_row(target(isin="SI0031102120", name="Krka"), parse_ljse_page(html))

    assert result["decision"] == "unsupported_nace_sector"
    assert result["sector_update"] == ""

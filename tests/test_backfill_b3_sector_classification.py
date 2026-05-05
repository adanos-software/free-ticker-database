from io import BytesIO
from zipfile import ZipFile

from openpyxl import Workbook

from scripts.backfill_b3_sector_classification import (
    evaluate_rows,
    extract_workbook,
    load_b3_industry_download_rows,
    load_b3_sector_rows,
)


def make_b3_zip() -> bytes:
    workbook = Workbook()
    worksheet = workbook.active
    worksheet.append(["SETOR ECONÔMICO", "SUBSETOR", "SEGMENTO", "LISTAGEM"])
    worksheet.append(["Utilidade Pública", "Energia Elétrica", "Energia Elétrica", None])
    worksheet.append([None, None, "COPEL", "CPLE"])
    worksheet.append(["Outros", "Outros", "Outros", None])
    worksheet.append([None, None, "IGNORED", "IGNR"])
    workbook_bytes = BytesIO()
    workbook.save(workbook_bytes)

    zip_bytes = BytesIO()
    with ZipFile(zip_bytes, "w") as archive:
        archive.writestr("ClassifSetorial.xlsx", workbook_bytes.getvalue())
    return zip_bytes.getvalue()


def make_b3_download_workbook() -> bytes:
    workbook = Workbook()
    worksheet = workbook.active
    worksheet.append([None, "SECTOR", "SUBSECTOR", "SEGMENT", "ISSUER", None, None])
    worksheet.append([None, None, None, None, "TRADING NAME", "CODE", "TRADING SEGMENT"])
    worksheet.append([None, "Utilities", "Electric Power", "Electric Power", "AXIA ENERGIA", "AXIA", "Novo Mercado"])
    workbook_bytes = BytesIO()
    workbook.save(workbook_bytes)
    return workbook_bytes.getvalue()


def test_load_b3_sector_rows_maps_official_sector_and_skips_outros():
    rows = load_b3_sector_rows(extract_workbook(make_b3_zip()))

    assert len(rows) == 1
    assert rows[0].code == "CPLE"
    assert rows[0].sector == "Utilities"


def test_load_b3_industry_download_rows_maps_current_official_file():
    rows = load_b3_industry_download_rows(make_b3_download_workbook())

    assert len(rows) == 1
    assert rows[0].code == "AXIA"
    assert rows[0].sector == "Utilities"


def test_evaluate_rows_accepts_unique_b3_code_root():
    result = evaluate_rows(
        [
            {
                "ticker": "CPLE99",
                "exchange": "B3",
                "asset_type": "Stock",
                "name": "CIA PARANAENSE DE ENERGIA - COPEL",
            }
        ],
        load_b3_sector_rows(extract_workbook(make_b3_zip())),
    )[0]

    assert result["decision"] == "accept"
    assert result["sector_update"] == "Utilities"


def test_evaluate_rows_rejects_missing_code_root():
    result = evaluate_rows(
        [
            {
                "ticker": "ZZZZ3",
                "exchange": "B3",
                "asset_type": "Stock",
                "name": "Unknown",
            }
        ],
        load_b3_sector_rows(extract_workbook(make_b3_zip())),
    )[0]

    assert result["decision"] == "no_b3_code_match"


def test_evaluate_rows_accepts_duplicate_sources_with_same_sector():
    rows = [
        *load_b3_sector_rows(extract_workbook(make_b3_zip())),
        *load_b3_industry_download_rows(make_b3_download_workbook()),
    ]
    result = evaluate_rows(
        [
            {
                "ticker": "CPLE99",
                "exchange": "B3",
                "asset_type": "Stock",
                "name": "CIA PARANAENSE DE ENERGIA - COPEL",
            }
        ],
        [*rows, rows[0]],
    )[0]

    assert result["decision"] == "accept"
    assert result["sector_update"] == "Utilities"

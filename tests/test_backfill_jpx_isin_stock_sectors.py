from scripts.backfill_jpx_isin_stock_sectors import (
    build_metadata_updates,
    candidate_codes,
    evaluate_row,
    extract_stock_detail,
    index_by_issuer_key,
)


def target(**overrides):
    values = {
        "ticker": "STAA",
        "exchange": "FSX",
        "asset_type": "Stock",
        "name": "Stanley Electric Co. Ltd",
        "isin": "JP3399400005",
        "stock_sector": "",
    }
    values.update(overrides)
    return values


def issue(**overrides):
    values = {
        "local_code": "6923",
        "name": "Stanley Electric Co.,Ltd.",
        "industry": "Electric Appliances",
        "section": "Prime Market (Domestic)",
        "issuer_key": "stanley electric",
    }
    values.update(overrides)
    return values


def detail(**overrides):
    values = {
        "isin": "JP3399400005",
        "industry": "Electric Appliances",
        "local_code": "6923",
        "name": "STANLEY ELECTRIC CO., LTD.",
    }
    values.update(overrides)
    return values


def indexed(*rows):
    return index_by_issuer_key(list(rows))


def test_extract_stock_detail_requires_labeled_isin():
    payload = {
        "section1": {
            "data": {
                "row": {
                    "TTCODE2": "6923",
                    "ISIN": "JP3399400005",
                    "JSECE_CNV": "Electric Appliances",
                    "JSEC_CNV": "電気機器",
                    "FLLNE": "STANLEY ELECTRIC CO., LTD.",
                }
            }
        }
    }
    parsed = extract_stock_detail(payload)
    assert parsed["isin"] == "JP3399400005"
    assert parsed["industry"] == "Electric Appliances"
    assert parsed["local_code"] == "6923"
    assert extract_stock_detail({"section1": {"data": None}}) == {}
    assert extract_stock_detail({"section1": {"data": {"row": {"TTCODE2": "6923"}}}}) == {}


def test_evaluate_row_accepts_unique_name_after_exact_isin():
    result = evaluate_row(target(), indexed(issue()), {"6923": detail()})

    assert result["decision"] == "accept"
    assert result["sector_update"] == "Information Technology"
    assert result["jpx_code"] == "6923"
    assert build_metadata_updates([result])[0]["proposed_value"] == "Information Technology"


def test_evaluate_row_accepts_lion_only_after_isin_confirms_not_office_products():
    lion_issue = issue(
        local_code="4912",
        name="Lion Corporation",
        industry="Chemicals",
        issuer_key="lion",
    )
    accepted = evaluate_row(
        target(ticker="LOC", name="Lion Corporation", isin="JP3965400009"),
        indexed(lion_issue),
        {"4912": detail(isin="JP3965400009", industry="Chemicals", local_code="4912", name="LION CORP.")},
    )
    stolen = evaluate_row(
        target(ticker="LOC", name="Lion Corporation", isin="JP3965400009"),
        indexed(lion_issue),
        {"4912": detail(isin="JP3965440005", industry="Wholesale Trade", local_code="4912")},
    )

    assert accepted["decision"] == "accept"
    assert accepted["sector_update"] == "Materials"
    assert stolen["decision"] == "isin_mismatch"
    assert stolen["sector_update"] == ""
    assert build_metadata_updates([stolen]) == []


def test_evaluate_row_maps_ampersand_securities_industry():
    result = evaluate_row(
        target(ticker="ZOF", name="SBI Holdings Inc", isin="JP3436120004"),
        indexed(
            issue(
                local_code="8473",
                name="SBI Holdings,Inc.",
                industry="Securities and Commodities Futures",
                issuer_key="sbi holdings",
            )
        ),
        {
            "8473": detail(
                isin="JP3436120004",
                industry="Securities & Commodities Futures",
                local_code="8473",
                name="SBI HOLDINGS",
            )
        },
    )

    assert result["decision"] == "accept"
    assert result["sector_update"] == "Financials"


def test_evaluate_row_rejects_locked_name_expansions_and_rebrands():
    by_key = indexed(
        issue(
            local_code="7014",
            name="Namura Shipbuilding Co.,Ltd.",
            industry="Transportation Equipment",
            issuer_key="namura shipbuilding",
        ),
        issue(
            local_code="8359",
            name="Hachijuni Nagano Bank,Ltd.",
            industry="Banks",
            issuer_key="hachijuni nagano bank",
        ),
        issue(
            local_code="7186",
            name="Yokohama Financial Group,Inc.",
            industry="Banks",
            issuer_key="yokohama financial group",
        ),
    )
    details = {
        "7014": detail(isin="JP3651400008", industry="Transportation Equipment", local_code="7014"),
        "8359": detail(isin="JP3769000005", industry="Banks", local_code="8359"),
        "7186": detail(isin="JP3305990008", industry="Banks", local_code="7186"),
    }

    namura = evaluate_row(target(ticker="8AF", name="NAMURA SHIPBLDG LTD", isin="JP3651400008"), by_key, details)
    hachijuni = evaluate_row(target(ticker="5FI", name="HACHIJUNI BK", isin="JP3769000005"), by_key, details)
    concordia = evaluate_row(target(ticker="YC3", name="CONCORDIA FINL GROUP", isin="JP3305990008"), by_key, details)

    assert namura["decision"] == "no_jpx_name_match"
    assert hachijuni["decision"] == "no_jpx_name_match"
    assert concordia["decision"] == "no_jpx_name_match"
    assert candidate_codes(
        [
            target(ticker="8AF", name="NAMURA SHIPBLDG LTD", isin="JP3651400008"),
            target(ticker="YC3", name="CONCORDIA FINL GROUP", isin="JP3305990008"),
        ],
        by_key,
    ) == []

from scripts.backfill_tmx_dual_stock_sectors import evaluate_row, tmx_issuer_key
from scripts.backfill_tmx_stock_sectors import TmxSectorRow


def target(**overrides):
    values = {
        "ticker": "2CC",
        "exchange": "FSX",
        "asset_type": "Stock",
        "name": "Searchlight Resources Inc.",
        "isin": "CA81222L2049",
        "stock_sector": "",
    }
    values.update(overrides)
    return values


def tmx(**overrides):
    values = {
        "exchange": "TSXV",
        "symbol": "SCLT",
        "name": "Searchlight Resources Inc.",
        "sector": "Mining",
        "subsector": "",
    }
    values.update(overrides)
    return TmxSectorRow(**values)


def test_tmx_issuer_key_requires_exact_issuer_not_shared_resources_token():
    assert tmx_issuer_key("Searchlight Resources Inc.") == "searchlight resources"
    assert tmx_issuer_key("Searchlight Resources Inc.") != tmx_issuer_key("Aclara Resources Inc.")


def test_evaluate_row_accepts_exact_issuer_key_with_unique_mapped_sector():
    result = evaluate_row(target(), [tmx()])

    assert result["decision"] == "accept"
    assert result["sector_update"] == "Materials"
    assert result["tmx_symbol"] == "SCLT"


def test_evaluate_row_rejects_jaccard_false_friends():
    result = evaluate_row(
        target(),
        [
            tmx(symbol="ARA", name="Aclara Resources Inc."),
            tmx(symbol="ARG", name="Amerigo Resources Ltd."),
        ],
    )

    assert result["decision"] == "no_tmx_name_match"
    assert result["sector_update"] == ""


def test_evaluate_row_rejects_unmapped_clean_technology():
    result = evaluate_row(
        target(ticker="C360", name="Cielo Waste Solutions Corp.", isin="CA17178G3026"),
        [
            tmx(
                symbol="CMC",
                name="Cielo Waste Solutions Corp.",
                sector="Clean Technology & Renewable Energy",
            )
        ],
    )

    assert result["decision"] == "unsupported_or_ambiguous_tmx_sector"
    assert result["sector_update"] == ""


def test_evaluate_row_accepts_unique_exact_full_name_when_issuer_key_is_one_token():
    result = evaluate_row(
        target(ticker="K2I0", name="ReeXploration Inc.", isin="CA75865L1094"),
        [],
        [tmx(symbol="REE", name="ReeXploration Inc.", sector="Mining")],
    )

    assert result["decision"] == "accept"
    assert result["sector_update"] == "Materials"
    assert result["tmx_symbol"] == "REE"


def test_evaluate_row_keeps_one_token_key_closed_without_exact_full_name():
    result = evaluate_row(
        target(ticker="K2I0", name="ReeXploration Inc.", isin="CA75865L1094"),
        [tmx(symbol="REE", name="ReeXploration Inc.", sector="Mining")],
    )

    assert result["decision"] == "short_issuer_key"
    assert result["sector_update"] == ""

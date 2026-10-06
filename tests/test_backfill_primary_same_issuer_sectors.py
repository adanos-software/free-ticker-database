from scripts.backfill_primary_same_issuer_sectors import evaluate_row, issuer_key, key_usable


def target(**overrides):
    values = {
        "ticker": "1L3",
        "exchange": "FSX",
        "asset_type": "Stock",
        "name": "Lifco AB (publ)",
        "isin": "US53174A1060",
        "stock_sector": "",
    }
    values.update(overrides)
    return values


def listing(**overrides):
    values = {
        "ticker": "LIFCO-B",
        "exchange": "STO",
        "asset_type": "Stock",
        "name": "Lifco AB (publ)",
        "isin": "SE0015949201",
        "stock_sector": "Industrials",
    }
    values.update(overrides)
    return values


def test_issuer_key_keeps_parenthetical_identity_and_strips_share_class_noise():
    assert issuer_key("Banco Santander (Brasil) S.A.") != issuer_key("Banco Santander, S.A.")
    assert issuer_key("SOFTBANK CORP. ADR") == "softbank"
    assert issuer_key("SoftBank Group Corp.") == "softbank group"
    assert issuer_key("WISETECH GLOBAL USP.ADRS") == issuer_key("WISETECH GLOBAL LIMITED")
    assert issuer_key("META PLATFORMS INC. CDR") == "meta platforms"
    assert issuer_key("Bavarian Nordic A/S") == issuer_key("Bavarian Nordic")
    assert issuer_key("HOME DEPOT INC.CDR(REG.S)") == issuer_key("The Home Depot Inc")
    assert issuer_key("PT Astra International Tbk") == issuer_key("Astra International Tbk")
    assert issuer_key("Atlas Copco AB Series A") == issuer_key("Atlas Copco A ADR")
    assert issuer_key("Constellation Brands Inc Class A") == issuer_key("Constellation Brands, Inc.")
    assert key_usable("softbank")
    assert not key_usable("rwe")


def test_evaluate_row_accepts_exact_key_from_primary_listing():
    result = evaluate_row(target(), [listing()])

    assert result["decision"] == "accept"
    assert result["match_path"] == "exact_issuer_key"
    assert result["sector_update"] == "Industrials"
    assert result["source_exchange"] == "STO"


def test_evaluate_row_rejects_softbank_group_for_softbank_corp_adr():
    result = evaluate_row(
        target(ticker="3AG0", name="SOFTBANK CORP. ADR", isin="US83405K1025"),
        [
            listing(
                ticker="9984",
                exchange="TSE",
                name="SoftBank Group Corp.",
                isin="JP3436100006",
                stock_sector="Communication Services",
            )
        ],
    )

    assert result["decision"] == "no_primary_match"
    assert result["sector_update"] == ""


def test_evaluate_row_accepts_softbank_corp_from_tse_primary():
    result = evaluate_row(
        target(ticker="3AG0", name="SOFTBANK CORP. ADR", isin="US83405K1025"),
        [
            listing(
                ticker="9434",
                exchange="TSE",
                name="SoftBank Corp.",
                isin="JP3732000009",
                stock_sector="Communication Services",
            )
        ],
    )

    assert result["decision"] == "accept"
    assert result["match_path"] == "exact_issuer_key"
    assert result["source_ticker"] == "9434"


def test_evaluate_row_rejects_bank_with_non_financials_source():
    result = evaluate_row(
        target(ticker="D1N", name="DNB Bank ASA", isin="US23341C1036"),
        [
            listing(
                ticker="DNB",
                exchange="OSL",
                name="DNB BANK",
                isin="NO0010161896",
                stock_sector="Industrials",
            )
        ],
    )

    assert result["decision"] == "no_primary_match"
    assert result["sector_update"] == ""


def test_evaluate_row_rejects_one_token_cross_country_false_friend():
    result = evaluate_row(
        target(ticker="8T40", name="Zenergy AB (publ)", isin="SE0023313069"),
        [
            listing(
                ticker="03677",
                exchange="HKEX",
                name="ZENERGY",
                isin="KYG9887P1028",
                stock_sector="Consumer Discretionary",
            )
        ],
    )

    assert result["decision"] == "no_primary_match"
    assert result["sector_update"] == ""


def test_evaluate_row_accepts_xetra_same_ticker_after_isin_change():
    result = evaluate_row(
        target(ticker="D6H", name="DATAGROUP SE", isin="DE000A0JC8S7"),
        [
            listing(
                ticker="D6H",
                exchange="XETRA",
                name="DATAGROUP SE  INH. O.N.",
                isin="DE000A41YEV7",
                stock_sector="Information Technology",
            )
        ],
    )

    assert result["decision"] == "accept"
    assert result["match_path"] == "xetra_ticker"
    assert result["sector_update"] == "Information Technology"


def test_evaluate_row_rejects_santander_brasil_vs_spain():
    result = evaluate_row(
        target(ticker="DBSA", name="Banco Santander (Brasil) S.A.", isin="US05967A1079"),
        [
            listing(
                ticker="SAN",
                exchange="NYSE",
                name="Banco Santander, S.A.",
                isin="US80281L1098",
                stock_sector="Financials",
            )
        ],
    )

    assert result["decision"] == "no_primary_match"
    assert result["sector_update"] == ""


def test_evaluate_row_rejects_otc_and_notes():
    otc = evaluate_row(
        target(),
        [listing(exchange="OTC", ticker="LIFCOY", isin="US53174A1060")],
    )
    notes = evaluate_row(
        target(ticker="HH10", name="HANCOCK WHITNEY SU.NTS 25", isin="US410120AB13"),
        [
            listing(
                ticker="HWC",
                exchange="NASDAQ",
                name="Hancock Whitney Corporation",
                isin="US4101201097",
                stock_sector="Financials",
            )
        ],
    )

    assert otc["decision"] == "no_primary_match"
    assert notes["decision"] == "skip_notes"


def test_evaluate_row_rejects_energy_name_with_industrials_source():
    result = evaluate_row(
        target(ticker="9PAA", name="Pampa Energía S.A", isin="US6976602077"),
        [
            listing(
                ticker="PAMP",
                exchange="BCBA",
                name="Pampa Energia SA",
                isin="ARP432631215",
                stock_sector="Industrials",
            )
        ],
    )

    assert result["decision"] == "no_primary_match"
    assert result["sector_update"] == ""


def test_evaluate_row_rejects_stock_exchange_with_materials_source():
    result = evaluate_row(
        target(
            ticker="LS4",
            name="London Stock Exchange Group plc Sponsored ADR",
            isin="US54211Y1073",
        ),
        [
            listing(
                ticker="LS4C",
                exchange="FSX",
                name="London Stock Exchange Group plc",
                isin="GB00B0SWJX34",
                stock_sector="Materials",
            )
        ],
    )

    assert result["decision"] == "no_primary_match"
    assert result["sector_update"] == ""


def test_evaluate_row_prefers_primary_unique_sector_over_fsx_conflict():
    result = evaluate_row(
        target(ticker="BOSA", name="Hugo Boss AG", isin="US4445601069"),
        [
            listing(
                ticker="BOSS",
                exchange="XETRA",
                name="HUGO BOSS AG NA O.N.",
                isin="DE000A1PHFF7",
                stock_sector="Consumer Discretionary",
            ),
            listing(
                ticker="BOSS",
                exchange="FSX",
                name="Hugo Boss AG",
                isin="DE000A1PHFF7",
                stock_sector="Consumer Staples",
            ),
        ],
    )

    assert result["decision"] == "accept"
    assert result["sector_update"] == "Consumer Discretionary"
    assert result["source_exchange"] == "XETRA"


def test_evaluate_row_accepts_short_key_from_primary_with_depositary_isin():
    result = evaluate_row(
        target(ticker="RWEA", name="RWE Aktiengesellschaft", isin="US74975E3036"),
        [
            listing(
                ticker="RWE",
                exchange="XETRA",
                name="RWE AG   INH O.N.",
                isin="DE0007037129",
                stock_sector="Utilities",
            )
        ],
    )

    assert not key_usable("rwe")
    assert result["decision"] == "accept"
    assert result["match_path"] == "exact_issuer_key"
    assert result["sector_update"] == "Utilities"
    assert result["source_ticker"] == "RWE"


def test_evaluate_row_accepts_german_same_ticker_from_munich_after_isin_change():
    result = evaluate_row(
        target(ticker="UMD", name="UMT United Mobility Technology AG", isin="DE0005286108"),
        [
            listing(
                ticker="UMD",
                exchange="Munich",
                name="UMT United Mobility Technology AG",
                isin="DE000A40ZVU2",
                stock_sector="Information Technology",
            )
        ],
    )

    assert result["decision"] == "accept"
    assert result["match_path"] == "german_ticker"
    assert result["sector_update"] == "Information Technology"
    assert result["source_exchange"] == "Munich"


def test_evaluate_row_does_not_copy_munich_as_general_issuer_key_source():
    result = evaluate_row(
        target(ticker="ZZZ", name="UMT United Mobility Technology AG", isin="DE0005286108"),
        [
            listing(
                ticker="UMD",
                exchange="Munich",
                name="UMT United Mobility Technology AG",
                isin="DE000A40ZVU2",
                stock_sector="Information Technology",
            )
        ],
    )

    assert result["decision"] == "no_primary_match"
    assert result["sector_update"] == ""


def test_evaluate_row_accepts_series_share_class_from_primary():
    result = evaluate_row(
        target(ticker="ACO", name="Atlas Copco A ADR", isin="US0492557063"),
        [
            listing(
                ticker="ATCO-A",
                exchange="STO",
                name="Atlas Copco AB Series A",
                isin="SE0017486889",
                stock_sector="Industrials",
            )
        ],
    )

    assert result["decision"] == "accept"
    assert result["match_path"] == "exact_issuer_key"
    assert result["sector_update"] == "Industrials"
    assert result["source_ticker"] == "ATCO-A"


def test_evaluate_row_accepts_cusip6_share_class_with_name_match():
    result = evaluate_row(
        target(ticker="84C", name="CLENE INC.", isin="US1856343009"),
        [
            listing(
                ticker="CLNN",
                exchange="NASDAQ",
                name="Clene Inc.",
                isin="US1856342019",
                stock_sector="Health Care",
            )
        ],
    )

    assert result["decision"] == "accept"
    assert result["match_path"] == "cusip6"
    assert result["sector_update"] == "Health Care"

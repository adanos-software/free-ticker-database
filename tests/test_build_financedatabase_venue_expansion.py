from __future__ import annotations

from scripts.build_financedatabase_venue_expansion import (
    COVERAGE_FIELDS,
    build_plan,
    apply_coverage_rows,
)


def review_row(**overrides: str) -> dict[str, str]:
    row = {
        "dry_run_decision": "review_security_type_required",
        "mapped_exchange": "ASX",
        "fd_symbol": "AAA.AX",
        "fd_name": "Alpha Holdings Limited",
        "fd_isin": "",
        "official_ticker": "AAA",
        "official_name": "ALPHA HOLDINGS LIMITED",
        "official_isin": "AU000000BHP4",
        "official_source_key": "asx_listed_companies",
        "official_sector": "Industrials",
        "reference_asset_type_conflict": "false",
        "local_same_isin_any_venue": "false",
        "local_same_isin_venues": "",
    }
    row.update(overrides)
    return row


def reference_row(**overrides: str) -> dict[str, str]:
    row = {
        "source_key": "asx_listed_companies",
        "ticker": "AAA",
        "name": "ALPHA HOLDINGS LIMITED",
        "exchange": "ASX",
        "asset_type": "Stock",
        "listing_status": "active",
        "official": "true",
        "isin": "AU000000BHP4",
        "sector": "Industrials",
    }
    row.update(overrides)
    return row


def probe(*, common_name: str = "ALPHA HOLDINGS LIMITED", common: bool = True):
    data = []
    if common:
        data.append(
            {
                "figi": "BBG000AAA001",
                "name": common_name,
                "exchCode": "AU",
                "securityType": "Common Stock",
                "securityType2": "Common Stock",
            }
        )
    return {("ID_ISIN", "AU000000BHP4"): {"data": data}}


def test_build_plan_requires_openfigi_name_match_and_official_stock_reference():
    reviews = [
        review_row(),
        review_row(
            mapped_exchange="NYSE",
            official_ticker="BBB",
            official_name="BETA HOLDINGS INC",
            official_isin="US000000BBB1",
            official_source_key="nasdaq_other_listed",
            fd_symbol="BBB",
            fd_name="Beta Holdings Inc",
        ),
        review_row(
            mapped_exchange="LSE",
            official_ticker="CCC",
            official_name="GAMMA CLOSED END FUND",
            official_isin="GB000000CCC1",
            official_source_key="lse_price_explorer",
            fd_symbol="CCC.L",
            fd_name="Gamma Closed End Fund",
            dry_run_decision="manual_reference_asset_type_conflict",
        ),
    ]
    references = [
        reference_row(),
        reference_row(
            exchange="NYSE",
            ticker="BBB",
            name="BETA HOLDINGS INC",
            source_key="nasdaq_other_listed",
            isin="US000000BBB1",
        ),
        reference_row(
            exchange="LSE",
            ticker="CCC",
            name="GAMMA CLOSED END FUND",
            source_key="lse_price_explorer",
            isin="GB000000CCC1",
            asset_type="Stock",
        ),
    ]
    plan = build_plan(reviews, references, [], [], probe())

    by_key = {row["listing_key"]: row for row in plan}
    assert by_key["ASX::AAA"]["apply_action"] == "add_coverage_expansion"
    assert by_key["ASX::AAA"]["stock_sector_action"] == "use_official_reference"
    assert by_key["ASX::AAA"]["isin_action"] == "use_official_reference"
    assert by_key["NYSE::BBB"]["apply_action"] == "blocked_openfigi_no_common_stock"
    assert by_key["LSE::CCC"]["apply_action"] == "blocked_reconciliation_decision"

    dropped = build_plan(reviews[:1], references[:1], [], [], probe(), drop_entries={("AAA", "ASX")})
    assert dropped[0]["apply_action"] == "blocked_review_drop_entry"


def test_apply_coverage_rows_is_idempotent_and_uses_only_official_metadata(tmp_path):
    path = tmp_path / "coverage_expansion_listings.csv"
    plan = build_plan(
        [review_row()],
        [reference_row()],
        [],
        [],
        probe(),
    )

    assert apply_coverage_rows(path, plan) == 1
    assert apply_coverage_rows(path, plan) == 0
    rows = list(__import__("csv").DictReader(path.open(newline="", encoding="utf-8")))
    assert list(rows[0]) == COVERAGE_FIELDS
    assert rows[0]["listing_key"] == "ASX::AAA"
    assert rows[0]["stock_sector"] == "Industrials"
    assert rows[0]["isin"] == "AU000000BHP4"

from __future__ import annotations

from scripts.build_source_inventory import build_source_inventory, render_markdown, summarize


def row_for(rows, exchange: str):
    for row in rows:
        if row.exchange == exchange:
            return row
    raise AssertionError(f"missing row for {exchange}")


def test_build_source_inventory_joins_candidates_to_current_coverage():
    sources = [{"key": "nasdaq_listed"}]
    candidates = [
        {
            "key": "egx_listed_securities",
            "exchange": "EGX",
            "venue_name": "Egyptian Exchange",
            "country": "Egypt",
            "provider": "EGX",
            "source_url": "https://www.egx.com.eg/en/ListedStocks.aspx",
            "candidate_scope": "exchange_directory_candidate",
            "asset_types": ["Stock", "ETF"],
            "expected_format": "html_or_api",
            "implementation_status": "todo",
            "priority": "high",
            "blocker": "needs endpoint discovery",
            "notes": "Largest gap.",
        },
        {
            "key": "nse_india_securities_available",
            "exchange": "NSE_IN",
            "venue_name": "National Stock Exchange of India",
            "country": "India",
            "provider": "NSE India",
            "source_url": "https://www.nseindia.com/market-data/securities-available-for-trading",
            "candidate_scope": "global_expansion_candidate",
            "asset_types": ["Stock", "ETF"],
            "expected_format": "csv_or_api",
            "implementation_status": "todo",
            "priority": "high",
            "blocker": "not yet in current exchange bucket list",
            "notes": "Expansion.",
        },
    ]
    coverage_report = {
        "by_exchange": [
            {
                "exchange": "EGX",
                "venue_status": "missing",
                "tickers": 225,
                "isin_coverage": 200,
                "sector_coverage": 100,
                "unresolved_count": 225,
                "official_source_count": 0,
                "reference_scopes": [],
            },
            {
                "exchange": "LSE",
                "venue_status": "official_partial",
                "tickers": 6408,
                "isin_coverage": 6361,
                "sector_coverage": 5106,
                "unresolved_count": 52,
                "official_source_count": 3,
                "reference_scopes": ["listed_companies_subset", "security_lookup_subset"],
            },
        ],
    }

    rows = build_source_inventory(sources, candidates, coverage_report)

    egx = row_for(rows, "EGX")
    assert egx.current_status == "missing"
    assert egx.missing_isin == 25
    assert egx.missing_sector_or_category == 125
    assert egx.unresolved_findings == 225
    assert egx.review_needed is True
    assert egx.asset_types == "Stock|ETF"

    lse = row_for(rows, "LSE")
    assert lse.current_status == "official_partial"
    assert lse.candidate_scope == "needs_source_research"
    assert lse.reference_scopes == "listed_companies_subset|security_lookup_subset"

    nse = row_for(rows, "NSE_IN")
    assert nse.current_status == "not_in_current_universe"
    assert nse.tickers == 0
    assert nse.candidate_scope == "global_expansion_candidate"


def test_render_markdown_splits_missing_partial_and_global_candidates():
    rows = build_source_inventory(
        sources=[],
        candidates=[
            {
                "key": "egx_listed_securities",
                "exchange": "EGX",
                "venue_name": "Egyptian Exchange",
                "country": "Egypt",
                "provider": "EGX",
                "source_url": "https://www.egx.com.eg/en/ListedStocks.aspx",
                "candidate_scope": "exchange_directory_candidate",
                "asset_types": ["Stock"],
                "expected_format": "html",
                "implementation_status": "todo",
                "priority": "high",
                "blocker": "needs endpoint discovery",
                "notes": "",
            }
        ],
        coverage_report={"by_exchange": []},
    )
    summary = summarize(rows, "2026-04-13T00:00:00Z")

    markdown = render_markdown(rows, summary)

    assert "Source Inventory Gap" in markdown
    assert "Global Expansion Candidates" in markdown
    assert "source_candidates.json" in markdown
    assert "EGX" in markdown

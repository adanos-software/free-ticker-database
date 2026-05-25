from scripts.build_ohlcv_warning_review import (
    build_review_rows,
    official_source_locator_status_for,
    official_source_url_for,
    recommended_next_source_for,
    summarize,
)


def ohlcv_row(
    listing_key: str = "LSE::0A8Q",
    exchange: str = "LSE",
    review_bucket: str = "checked_ohlcv_anomaly_requires_listing_review",
    issues: list[dict[str, str]] | None = None,
    invalid_bar_count: int = 200,
    max_price_jump: float = 0.351563,
) -> dict[str, object]:
    return {
        "listing_key": listing_key,
        "ticker": listing_key.split("::")[1],
        "exchange": exchange,
        "asset_type": "Stock",
        "name": "Example PLC",
        "isin": "US0000000000",
        "entry_quality_status": "warn",
        "ohlcv_source": "yahoo_chart",
        "ohlcv_symbol": f"{listing_key.split('::')[1]}.L",
        "plausibility_status": "warn",
        "plausibility_score": 30,
        "review_bucket": review_bucket,
        "bar_count": 244,
        "first_bar_date": "2025-06-06",
        "last_bar_date": "2026-05-22",
        "max_price_jump": max_price_jump,
        "zero_volume_streak": 1,
        "stagnant_close_streak": 2,
        "invalid_bar_count": invalid_bar_count,
        "issues": issues
        if issues is not None
        else [
            {"issue_type": "invalid_ohlcv_bar"},
            {"issue_type": "large_price_jump"},
        ],
    }


def test_build_review_rows_keeps_only_checked_ohlcv_anomaly_rows() -> None:
    rows = build_review_rows(
        {
            "flagged_items": [
                ohlcv_row(),
                ohlcv_row(
                    listing_key="LSE::NOTICE",
                    review_bucket="checked_low_severity_market_data_notice",
                    issues=[{"issue_type": "short_history"}],
                    invalid_bar_count=0,
                    max_price_jump=0,
                ),
            ]
        }
    )

    assert len(rows) == 1
    assert rows[0]["listing_key"] == "LSE::0A8Q"
    assert rows[0]["ohlcv_review_bucket"] == "official_corporate_action_and_listing_status_review"
    assert rows[0]["official_review_priority"] == "P1"
    assert rows[0]["canonical_data_change_authorization"] == "blocked_until_official_listing_keyed_review"
    assert rows[0]["verification_evidence_required"] == (
        "official_listing_status_corporate_action_and_independent_market_data_review"
    )
    assert rows[0]["official_source_url"] == (
        "https://www.londonstockexchange.com/market-stock/0A8Q/zw-data-action-technologies-inc/overview"
    )
    assert rows[0]["official_source_locator_status"] == "verified_official_exchange_page_seeded"
    assert "OHLCV anomaly is a review signal only" in rows[0]["source_gate"]
    assert rows[0]["recommended_action"] == "perform_official_listing_keyed_review_before_any_canonical_change"
    assert rows[0]["review_context"] == (
        "listing_key=LSE::0A8Q;ohlcv_symbol=0A8Q.L;"
        "review_bucket=official_corporate_action_and_listing_status_review;"
        "priority=P1;plausibility_status=warn;issue_types=invalid_ohlcv_bar|large_price_jump;"
        "official_source_locator_status=verified_official_exchange_page_seeded"
    )


def test_build_review_rows_prioritizes_invalid_bar_reviews() -> None:
    rows = build_review_rows(
        {
            "flagged_items": [
                ohlcv_row(
                    listing_key="SZSE::001289",
                    exchange="SZSE",
                    issues=[{"issue_type": "invalid_ohlcv_bar"}],
                    invalid_bar_count=1,
                    max_price_jump=0.024406,
                )
            ]
        }
    )

    assert rows[0]["ohlcv_review_bucket"] == "official_listing_status_and_market_data_cross_check"
    assert rows[0]["official_review_priority"] == "P2"
    assert rows[0]["recommended_next_source"] == recommended_next_source_for("SZSE")


def test_official_source_url_seeds_verified_lse_pages_only() -> None:
    seeded = ohlcv_row(listing_key="LSE::0A6W", issues=[{"issue_type": "invalid_ohlcv_bar"}])
    grouped = ohlcv_row(listing_key="LSE::0A2M", issues=[{"issue_type": "invalid_ohlcv_bar"}])
    unseeded = ohlcv_row(listing_key="LSE::UNKNOWN")

    assert official_source_url_for(seeded) == (
        "https://www.londonstockexchange.com/market-stock/0A6W/abb-ltd/overview"
    )
    assert official_source_locator_status_for(seeded) == "verified_official_exchange_page_seeded"
    assert official_source_url_for(grouped) == (
        "https://www.londonstockexchange.com/market-stock/0LNG/koninklijke-philips-nv/overview"
    )
    assert official_source_locator_status_for(grouped) == (
        "verified_official_exchange_instrument_group_page_seeded"
    )
    assert official_source_url_for(ohlcv_row(listing_key="LSE::0A8Q")) == (
        "https://www.londonstockexchange.com/market-stock/0A8Q/zw-data-action-technologies-inc/overview"
    )
    assert official_source_url_for(unseeded) == ""
    assert official_source_locator_status_for(unseeded) == "pending_official_exchange_page_or_notice_lookup"


def test_summarize_counts_review_gates() -> None:
    rows = build_review_rows(
        {
            "flagged_items": [
                ohlcv_row(),
                ohlcv_row(
                    listing_key="SZSE::001289",
                    exchange="SZSE",
                    issues=[{"issue_type": "invalid_ohlcv_bar"}],
                    invalid_bar_count=1,
                    max_price_jump=0.024406,
                ),
            ]
        }
    )

    summary = summarize(rows)

    assert summary["review_rows"] == 2
    assert summary["exchange_counts"] == {"LSE": 1, "SZSE": 1}
    assert summary["canonical_data_change_authorization_counts"] == {
        "blocked_until_official_listing_keyed_review": 2
    }
    assert summary["official_listing_review_status_counts"] == {
        "pending_official_listing_status_review": 2
    }
    assert summary["official_source_locator_status_counts"] == {
        "pending_official_exchange_page_or_notice_lookup": 1,
        "verified_official_exchange_page_seeded": 1,
    }
    assert summary["issue_type_counts"] == {"invalid_ohlcv_bar": 2, "large_price_jump": 1}

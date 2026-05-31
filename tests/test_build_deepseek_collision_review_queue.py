import json

from scripts.build_deepseek_collision_review_queue import build_payload, select_deepseek_collision_reviews


def test_select_deepseek_collision_reviews_keeps_only_possible_cross_listings() -> None:
    payload = {
        "items": [
            {
                "listing_key": "ADX::AGIX",
                "review_kind": "masterfile_collision",
                "decision_candidate": "possible_duplicate_or_cross_listing",
            },
            {
                "listing_key": "OTC::ABCD",
                "review_kind": "otc_scope",
                "decision_candidate": "needs_official_evidence",
            },
            {
                "listing_key": "AMS::MISS",
                "review_kind": "masterfile_collision",
                "decision_candidate": "uncertain",
            },
        ]
    }

    assert select_deepseek_collision_reviews(payload) == [
        {
            "listing_key": "ADX::AGIX",
            "review_kind": "masterfile_collision",
            "decision_candidate": "possible_duplicate_or_cross_listing",
        }
    ]


def test_build_payload_joins_deepseek_collision_reviews_to_masterfile_queue(tmp_path) -> None:
    deepseek_json = tmp_path / "deepseek.json"
    deepseek_json.write_text(
        json.dumps(
            {
                "items": [
                    {
                        "listing_key": "ADX::AGIX",
                        "ticker": "AGIX",
                        "exchange": "ADX",
                        "review_kind": "masterfile_collision",
                        "decision_candidate": "possible_duplicate_or_cross_listing",
                        "confidence": 0.8,
                        "evidence_needed": "Confirm listing status",
                        "rationale": "Same ISIN and name",
                        "do_not_apply_reason": "Manual review required",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    collision_csv = tmp_path / "collision.csv"
    collision_csv.write_text(
        "target_listing_key,ticker,target_exchange,official_name,official_asset_type,official_isin,"
        "official_source_key,existing_listing_keys,existing_exchanges,existing_names,existing_asset_types,"
        "existing_isins,same_isin_listing_keys,identity_evidence,collision_risk_flags\n"
        "ADX::AGIX,AGIX,ADX,KraneShares AI ETF,ETF,US5007673636,adx_market_watch,"
        "NASDAQ::AGIX,NASDAQ,KraneShares AI ETF,ETF,US5007673636,NASDAQ::AGIX,"
        "same_isin;name_exact_match,\n",
        encoding="utf-8",
    )

    payload = build_payload(deepseek_json, collision_csv)

    assert payload["summary"]["rows"] == 1
    assert payload["summary"]["target_exchange_totals"] == {"ADX": 1}
    assert payload["summary"]["official_source_key_totals"] == {"adx_market_watch": 1}
    assert payload["items"][0]["review_queue"] == "manual_cross_listing_identity_review"
    assert "Do not merge" in payload["items"][0]["review_gate"]
    assert payload["unmatched_deepseek_rows"] == []


def test_build_payload_reports_unmatched_deepseek_rows(tmp_path) -> None:
    deepseek_json = tmp_path / "deepseek.json"
    deepseek_json.write_text(
        json.dumps(
            {
                "items": [
                    {
                        "listing_key": "MISSING::ABC",
                        "review_kind": "masterfile_collision",
                        "decision_candidate": "possible_duplicate_or_cross_listing",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    collision_csv = tmp_path / "collision.csv"
    collision_csv.write_text("target_listing_key,ticker,target_exchange\n", encoding="utf-8")

    payload = build_payload(deepseek_json, collision_csv)

    assert payload["summary"]["rows"] == 0
    assert payload["summary"]["unmatched_deepseek_rows"] == 1
    assert payload["unmatched_deepseek_rows"] == [
        {"listing_key": "MISSING::ABC", "reason": "missing_masterfile_collision_review_row"}
    ]

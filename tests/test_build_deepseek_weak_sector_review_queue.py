import json

from scripts.build_deepseek_weak_sector_review_queue import build_payload, select_deepseek_weak_sector_reviews


def test_select_deepseek_weak_sector_reviews_keeps_supported_decisions() -> None:
    payload = {
        "items": [
            {"listing_key": "NGX::A", "review_kind": "weak_sector", "decision_candidate": "needs_official_evidence"},
            {"listing_key": "NGX::B", "review_kind": "weak_sector", "decision_candidate": "keep_source_gap"},
            {"listing_key": "NGX::C", "review_kind": "weak_sector", "decision_candidate": "out_of_scope_candidate"},
            {"listing_key": "OTC::D", "review_kind": "otc_scope", "decision_candidate": "needs_official_evidence"},
            {"listing_key": "NGX::E", "review_kind": "weak_sector", "decision_candidate": "apply_sector"},
        ]
    }

    assert [row["listing_key"] for row in select_deepseek_weak_sector_reviews(payload)] == [
        "NGX::A",
        "NGX::B",
        "NGX::C",
    ]


def test_build_payload_joins_weak_sector_reviews_to_residual_queue(tmp_path) -> None:
    deepseek_json = tmp_path / "deepseek.json"
    deepseek_json.write_text(
        json.dumps(
            {
                "items": [
                    {
                        "listing_key": "NGX::ABCTRANS",
                        "ticker": "ABCTRANS",
                        "exchange": "NGX",
                        "review_kind": "weak_sector",
                        "decision_candidate": "needs_official_evidence",
                        "confidence": 0.2,
                        "evidence_needed": "Canonical sector mapping",
                        "rationale": "Raw official sector needs mapping",
                        "do_not_apply_reason": "Mapping not reviewed",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    weak_csv = tmp_path / "weak.csv"
    weak_csv.write_text(
        "listing_key,ticker,exchange,asset_type,name,gap_class,source_of_truth_outcome,"
        "official_masterfile_sources,official_masterfile_sector_values,weak_sector_resolution_queue,residual_decision\n"
        "NGX::ABCTRANS,ABCTRANS,NGX,Stock,ABC TRANSPORT PLC,official_gap,accepted_source_gap,"
        "ngx_company_profile_directory|ngx_equities_price_list,SERVICES,"
        "official_sector_candidate_normalization_review,official_sector_available_review_apply\n",
        encoding="utf-8",
    )

    payload = build_payload(deepseek_json, weak_csv)

    assert payload["summary"]["rows"] == 1
    assert payload["summary"]["official_sector_value_totals"] == {"SERVICES": 1}
    assert payload["summary"]["review_queue_totals"] == {"official_sector_value_mapping_review": 1}
    assert payload["items"][0]["review_queue"] == "official_sector_value_mapping_review"
    assert "Do not apply" in payload["items"][0]["review_gate"]


def test_build_payload_reports_unmatched_weak_sector_rows(tmp_path) -> None:
    deepseek_json = tmp_path / "deepseek.json"
    deepseek_json.write_text(
        json.dumps(
            {
                "items": [
                    {
                        "listing_key": "NGX::MISSING",
                        "review_kind": "weak_sector",
                        "decision_candidate": "keep_source_gap",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    weak_csv = tmp_path / "weak.csv"
    weak_csv.write_text("listing_key,ticker,exchange\n", encoding="utf-8")

    payload = build_payload(deepseek_json, weak_csv)

    assert payload["summary"]["rows"] == 0
    assert payload["summary"]["unmatched_deepseek_rows"] == 1
    assert payload["unmatched_deepseek_rows"] == [
        {"listing_key": "NGX::MISSING", "reason": "missing_weak_sector_residual_review_row"}
    ]

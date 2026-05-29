import json

from scripts.build_deepseek_otc_review_queue import build_payload, select_deepseek_otc_reviews


def test_select_deepseek_otc_reviews_keeps_needs_official_evidence_only() -> None:
    payload = {
        "items": [
            {"listing_key": "OTC::A", "review_kind": "otc_scope", "decision_candidate": "needs_official_evidence"},
            {"listing_key": "OTC::B", "review_kind": "otc_scope", "decision_candidate": "uncertain"},
            {"listing_key": "NGX::C", "review_kind": "weak_sector", "decision_candidate": "needs_official_evidence"},
        ]
    }

    assert select_deepseek_otc_reviews(payload) == [
        {"listing_key": "OTC::A", "review_kind": "otc_scope", "decision_candidate": "needs_official_evidence"}
    ]


def test_build_payload_joins_otc_reviews_to_scope_queue(tmp_path) -> None:
    deepseek_json = tmp_path / "deepseek.json"
    deepseek_json.write_text(
        json.dumps(
            {
                "items": [
                    {
                        "listing_key": "OTC::ABNAF",
                        "ticker": "ABNAF",
                        "exchange": "OTC",
                        "review_kind": "otc_scope",
                        "decision_candidate": "needs_official_evidence",
                        "confidence": 0.3,
                        "evidence_needed": "Official company name",
                        "rationale": "Name mismatch",
                        "do_not_apply_reason": "Missing evidence",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    otc_csv = tmp_path / "otc.csv"
    otc_csv.write_text(
        "listing_key,ticker,exchange,asset_type,name,instrument_scope,scope_reason,quality_status,issue_types,"
        "source_gap_field,source_gap_class,source_of_truth_outcome,scope_decision,otc_review_decision_status,"
        "metadata_enrichment_gate\n"
        "OTC::ABNAF,ABNAF,OTC,Stock,Aben Resources Ltd,extended,otc_listing,warn,"
        "official_name_mismatch,,,,already_extended_otc_listing,pending_otc_name_mismatch_review,"
        "otc_name_mismatch_review_required_before_name_or_metadata_changes\n",
        encoding="utf-8",
    )

    payload = build_payload(deepseek_json, otc_csv)

    assert payload["summary"]["rows"] == 1
    assert payload["summary"]["review_queue_totals"] == {"official_name_mismatch_evidence_review": 1}
    assert payload["summary"]["issue_type_totals"] == {"official_name_mismatch": 1}
    assert payload["items"][0]["review_queue"] == "official_name_mismatch_evidence_review"
    assert "Do not change name" in payload["items"][0]["review_gate"]


def test_build_payload_reports_unmatched_otc_reviews(tmp_path) -> None:
    deepseek_json = tmp_path / "deepseek.json"
    deepseek_json.write_text(
        json.dumps(
            {
                "items": [
                    {
                        "listing_key": "OTC::MISSING",
                        "review_kind": "otc_scope",
                        "decision_candidate": "needs_official_evidence",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    otc_csv = tmp_path / "otc.csv"
    otc_csv.write_text("listing_key,ticker,exchange\n", encoding="utf-8")

    payload = build_payload(deepseek_json, otc_csv)

    assert payload["summary"]["rows"] == 0
    assert payload["summary"]["unmatched_deepseek_rows"] == 1
    assert payload["unmatched_deepseek_rows"] == [
        {"listing_key": "OTC::MISSING", "reason": "missing_otc_scope_review_row"}
    ]

from scripts.build_source_refresh_queue import build_payload, render_markdown


def test_build_payload_prioritizes_restore_queue(tmp_path) -> None:
    coverage = {
        "source_coverage": [
            {
                "key": "fresh_source",
                "provider": "X",
                "reference_scope": "exchange_directory",
                "mode": "network",
                "rows": 10,
                "generated_at": "2026-06-02T00:00:00Z",
                "age_hours": 1,
                "freshness_status": "fresh",
                "refresh_priority": "P3",
                "refresh_queue": "fresh_no_refresh_needed",
            },
            {
                "key": "old_subset",
                "provider": "Y",
                "reference_scope": "listed_companies_subset",
                "mode": "network",
                "rows": 20,
                "generated_at": "2026-05-16T00:00:00Z",
                "age_hours": 350,
                "freshness_status": "old",
                "refresh_priority": "P2",
                "refresh_queue": "refresh_official_subset_before_gap_enrichment",
                "evidence_required": "official_subset_refresh_artifact_with_generated_at_scope_and_row_count",
            },
            {
                "key": "unavailable_directory",
                "provider": "Z",
                "reference_scope": "exchange_directory",
                "mode": "unavailable",
                "rows": 0,
                "generated_at": "2026-05-16T00:00:00Z",
                "age_hours": 340,
                "freshness_status": "old",
                "refresh_priority": "P1",
                "refresh_queue": "restore_or_replace_unavailable_source_before_data_fill",
                "evidence_required": "source_restored_or_replaced_with_official_or_documented_unavailable_decision",
            },
        ],
        "source_freshness_summary": {
            "top_source_refresh_batches": [
                {
                    "refresh_queue": "restore_or_replace_unavailable_source_before_data_fill",
                    "reference_scope": "exchange_directory",
                    "mode": "unavailable",
                    "refresh_priority": "P1",
                    "source_count": 1,
                    "total_rows": 0,
                    "max_age_hours": 340,
                    "evidence_required": "source_restored_or_replaced_with_official_or_documented_unavailable_decision",
                },
                {
                    "refresh_queue": "fresh_no_refresh_needed",
                    "reference_scope": "listed_companies_subset",
                    "mode": "cache",
                    "refresh_priority": "P4",
                    "source_count": 10,
                    "total_rows": 1000,
                    "max_age_hours": 1,
                    "evidence_required": "fresh_source_generated_at_with_age_under_48h",
                },
            ]
        },
    }

    payload = build_payload(coverage=coverage, coverage_json=tmp_path / "coverage_report.json")

    assert payload["summary"]["rows"] == 2
    assert payload["summary"]["priority_totals"] == {"P1": 1, "P2": 1}
    assert payload["items"][0]["source_key"] == "unavailable_directory"
    assert "do not authorize inferred identifiers" in payload["_meta"]["policy"]
    assert [batch["refresh_queue"] for batch in payload["summary"]["top_source_refresh_batches"]] == [
        "restore_or_replace_unavailable_source_before_data_fill"
    ]

    markdown = render_markdown(payload)
    assert "Source Refresh Queue" in markdown
    assert "restore_or_replace_unavailable_source_before_data_fill" in markdown
    assert "fresh_no_refresh_needed" not in markdown

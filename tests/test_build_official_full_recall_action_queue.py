from __future__ import annotations

import json

from scripts.build_official_full_recall_action_queue import build_queue, supplement_lane, write_reports


def test_queue_ranks_still_actionable_official_full_venues_by_true_missing() -> None:
    queue = build_queue(
        {
            "by_exchange": [
                {
                    "exchange": "CSE_MA",
                    "venue_status": "official_full",
                    "official_recall_decision": "still_actionable",
                    "collision_adjusted_recall_pct": 5.56,
                    "official_recall_pct": 1.22,
                    "official_recall_true_missing_excluding_collisions": 40,
                    "official_recall_collision_hidden_missing": 2,
                    "official_recall_next_action": "add reviewed official evidence",
                },
                {
                    "exchange": "NASDAQ",
                    "venue_status": "official_full",
                    "official_recall_decision": "still_actionable",
                    "collision_adjusted_recall_pct": 82.33,
                    "official_recall_pct": 81.4,
                    "official_recall_true_missing_excluding_collisions": 10,
                    "official_recall_collision_hidden_missing": 100,
                },
                {
                    "exchange": "XSTU",
                    "venue_status": "missing",
                    "official_recall_decision": "source_unavailable",
                },
            ]
        }
    )

    assert [row["exchange"] for row in queue] == ["CSE_MA", "NASDAQ"]
    assert queue[0]["true_missing_excluding_collisions"] == 40
    assert queue[0]["rank"] == 1


def test_supplement_lane_marks_refresh_only_fsx() -> None:
    assert supplement_lane("FSX") == "refresh_only"
    assert supplement_lane("TSE") == "collision_free_supplement"


def test_write_reports_persists_generated_at(tmp_path) -> None:
    json_out = tmp_path / "queue.json"
    csv_out = tmp_path / "queue.csv"
    md_out = tmp_path / "queue.md"
    write_reports(
        [
            {
                "rank": 1,
                "exchange": "FSX",
                "collision_adjusted_recall_pct": 57.34,
                "true_missing_excluding_collisions": 1,
                "collision_hidden_missing": 0,
                "official_recall_pct": 43.88,
                "supplement_lane": "refresh_only",
                "next_action": "repair parser",
            }
        ],
        json_out=json_out,
        csv_out=csv_out,
        md_out=md_out,
        generated_at="2026-09-29T18:00:00Z",
    )
    payload = json.loads(json_out.read_text(encoding="utf-8"))
    assert payload["generated_at"] == "2026-09-29T18:00:00Z"
    assert payload["count"] == 1
    assert payload["rows"][0]["exchange"] == "FSX"

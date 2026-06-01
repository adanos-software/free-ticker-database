import csv

from scripts.build_deepseek_batch_plan import (
    QueueConfig,
    build_plan,
    render_markdown,
    write_batch_csv,
)


def write_csv(path, fieldnames, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def test_build_plan_selects_highest_priority_unreviewed_queue(tmp_path) -> None:
    summary = tmp_path / "deepseek_review_summary.csv"
    write_csv(
        summary,
        ["listing_key", "review_kind"],
        [{"listing_key": "A::1", "review_kind": "masterfile_collision"}],
    )
    masterfile = tmp_path / "masterfile.csv"
    otc = tmp_path / "otc.csv"
    write_csv(
        masterfile,
        ["target_listing_key", "ticker", "target_exchange"],
        [
            {"target_listing_key": "A::1", "ticker": "A", "target_exchange": "X"},
            {"target_listing_key": "B::2", "ticker": "B", "target_exchange": "Y"},
        ],
    )
    write_csv(otc, ["listing_key", "ticker", "exchange"], [{"listing_key": "OTC::A", "ticker": "A", "exchange": "OTC"}])
    configs = [
        QueueConfig("masterfile_collision", "masterfile_collision", masterfile, "target_listing_key", 1, "Masterfile first."),
        QueueConfig("otc_scope", "otc_scope", otc, "listing_key", 2, "OTC second."),
    ]

    payload, rows, fieldnames = build_plan(
        configs=configs,
        review_summary_csv=summary,
        requested_queue="auto",
        limit=10,
        batch_size=5,
        batch_csv=tmp_path / "batch.csv",
    )

    assert payload["selected_batch"]["queue"] == "masterfile_collision"
    assert payload["queues"][0]["already_deepseek_reviewed"] == 1
    assert payload["queues"][0]["unreviewed_rows"] == 1
    assert [row["target_listing_key"] for row in rows] == ["B::2"]
    assert fieldnames == ["target_listing_key", "ticker", "target_exchange"]
    assert "DEEPSEEK_API_KEY" in payload["selected_batch"]["required_env"]


def test_write_batch_and_markdown_include_policy(tmp_path) -> None:
    batch = tmp_path / "batch.csv"
    write_batch_csv(batch, [{"listing_key": "OTC::A", "ticker": "A"}], ["listing_key", "ticker"])

    with batch.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert rows == [{"listing_key": "OTC::A", "ticker": "A"}]

    markdown = render_markdown(
        {
            "_meta": {"generated_at": "2026-05-31T00:00:00Z"},
            "queues": [
                {
                    "queue": "otc_scope",
                    "rows": 1,
                    "already_deepseek_reviewed": 0,
                    "unreviewed_rows": 1,
                    "priority": 1,
                }
            ],
            "selected_batch": {
                "queue": "otc_scope",
                "review_kind": "otc_scope",
                "rows": 1,
                "batch_csv": "data/deepseek_review_jobs/next_otc_scope_batch.csv",
                "reason": "Review first.",
                "run_command": ["python", "scripts/run_deepseek_review_queue.py"],
                "dry_run_command": ["python", "scripts/run_deepseek_review_queue.py", "--dry-run"],
            },
        }
    )

    assert "advisory triage only" in markdown
    assert "DEEPSEEK_API_KEY" in markdown
    assert "--dry-run" in markdown

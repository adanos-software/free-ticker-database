# DeepSeek Batch Plan

Generated: `2026-06-02T21:05:27Z`

Policy: DeepSeek output is advisory triage only and cannot authorize direct data application.

## Queue Backlog

| Queue | Rows | Duplicate Keys | Already Reviewed | Unreviewed | Priority |
| --- | ---: | ---: | ---: | ---: | ---: |
| masterfile_collision | 11176 | 0 | 11143 | 33 | 1 |
| otc_scope | 11054 | 0 | 11054 | 0 | 2 |
| otc_name_mismatch | 24 | 0 | 18 | 6 | 3 |
| source_gap | 3548 | 184 | 3364 | 0 | 4 |
| weak_sector | 646 | 0 | 646 | 0 | 5 |

## Selected Batch

- Queue: `masterfile_collision`
- Review kind: `masterfile_collision`
- Rows: `33`
- Reason: Largest official-masterfile identity queue; DeepSeek can triage likely cross-listing vs. still-needs-evidence cases without applying data.
- Batch CSV: `data/deepseek_review_jobs/next_masterfile_collision_batch.csv`

Run when `DEEPSEEK_API_KEY` is set:

```bash
python scripts/run_deepseek_review_queue.py --input-csv data/deepseek_review_jobs/next_masterfile_collision_batch.csv --review-kind masterfile_collision --limit 33 --batch-size 5 --raw-responses-jsonl data/deepseek_review_jobs/raw_responses.jsonl --normalized-json data/deepseek_review_jobs/masterfile_collision_next_normalized_reviews.json --normalized-csv data/deepseek_review_jobs/masterfile_collision_next_normalized_reviews.csv --errors-json data/deepseek_review_jobs/masterfile_collision_next_errors.json
```

Schema-only dry run:

```bash
python scripts/run_deepseek_review_queue.py --input-csv data/deepseek_review_jobs/next_masterfile_collision_batch.csv --review-kind masterfile_collision --limit 33 --batch-size 5 --raw-responses-jsonl data/deepseek_review_jobs/dry_run_masterfile_collision_next_raw_responses.jsonl --normalized-json data/deepseek_review_jobs/dry_run_masterfile_collision_next_normalized_reviews.json --normalized-csv data/deepseek_review_jobs/dry_run_masterfile_collision_next_normalized_reviews.csv --errors-json data/deepseek_review_jobs/dry_run_masterfile_collision_next_errors.json --dry-run
```

Secret policy: read the API key only from `DEEPSEEK_API_KEY`; never write it to files, reports, logs, commits, or prompts.

# DeepSeek Batch Plan

Generated: `2026-05-31T18:18:01Z`

Policy: DeepSeek output is advisory triage only and cannot authorize direct data application.

## Queue Backlog

| Queue | Rows | Already Reviewed | Unreviewed | Priority |
| --- | ---: | ---: | ---: | ---: |
| masterfile_collision | 11107 | 3365 | 7742 | 1 |
| otc_scope | 11056 | 25 | 11031 | 2 |
| weak_sector | 646 | 50 | 596 | 3 |

## Selected Batch

- Queue: `masterfile_collision`
- Review kind: `masterfile_collision`
- Rows: `100`
- Batch CSV: `data/deepseek_review_jobs/next_masterfile_collision_batch.csv`
- Reason: Largest official-masterfile identity queue; DeepSeek can triage likely cross-listing vs. still-needs-evidence cases without applying data.

Run when `DEEPSEEK_API_KEY` is set:

```bash
python scripts/run_deepseek_review_queue.py --input-csv data/deepseek_review_jobs/next_masterfile_collision_batch.csv --review-kind masterfile_collision --limit 100 --batch-size 10 --raw-responses-jsonl data/deepseek_review_jobs/raw_responses.jsonl --normalized-json data/deepseek_review_jobs/masterfile_collision_next_normalized_reviews.json --normalized-csv data/deepseek_review_jobs/masterfile_collision_next_normalized_reviews.csv --errors-json data/deepseek_review_jobs/masterfile_collision_next_errors.json
```

Schema-only dry run:

```bash
python scripts/run_deepseek_review_queue.py --input-csv data/deepseek_review_jobs/next_masterfile_collision_batch.csv --review-kind masterfile_collision --limit 100 --batch-size 10 --raw-responses-jsonl data/deepseek_review_jobs/raw_responses.jsonl --normalized-json data/deepseek_review_jobs/masterfile_collision_next_normalized_reviews.json --normalized-csv data/deepseek_review_jobs/masterfile_collision_next_normalized_reviews.csv --errors-json data/deepseek_review_jobs/masterfile_collision_next_errors.json --dry-run
```

Secret policy: read the API key only from `DEEPSEEK_API_KEY`; never write it to files, reports, logs, commits, or prompts.

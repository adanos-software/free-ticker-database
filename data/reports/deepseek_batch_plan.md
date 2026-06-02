# DeepSeek Batch Plan

Generated: `2026-06-02T01:34:46Z`

Policy: DeepSeek output is advisory triage only and cannot authorize direct data application.

## Queue Backlog

| Queue | Rows | Already Reviewed | Unreviewed | Priority |
| --- | ---: | ---: | ---: | ---: |
| masterfile_collision | 11107 | 11107 | 0 | 1 |
| otc_scope | 11056 | 11056 | 0 | 2 |
| weak_sector | 646 | 50 | 596 | 3 |

## Selected Batch

- Queue: `weak_sector`
- Review kind: `weak_sector`
- Rows: `500`
- Batch CSV: `data/deepseek_review_jobs/next_weak_sector_batch.csv`
- Reason: Official-sector candidate queue; DeepSeek can prioritize normalization review, not sector application.

Run when `DEEPSEEK_API_KEY` is set:

```bash
python scripts/run_deepseek_review_queue.py --input-csv data/deepseek_review_jobs/next_weak_sector_batch.csv --review-kind weak_sector --limit 500 --batch-size 10 --raw-responses-jsonl data/deepseek_review_jobs/raw_responses.jsonl --normalized-json data/deepseek_review_jobs/weak_sector_next_normalized_reviews.json --normalized-csv data/deepseek_review_jobs/weak_sector_next_normalized_reviews.csv --errors-json data/deepseek_review_jobs/weak_sector_next_errors.json
```

Schema-only dry run:

```bash
python scripts/run_deepseek_review_queue.py --input-csv data/deepseek_review_jobs/next_weak_sector_batch.csv --review-kind weak_sector --limit 500 --batch-size 10 --raw-responses-jsonl data/deepseek_review_jobs/raw_responses.jsonl --normalized-json data/deepseek_review_jobs/weak_sector_next_normalized_reviews.json --normalized-csv data/deepseek_review_jobs/weak_sector_next_normalized_reviews.csv --errors-json data/deepseek_review_jobs/weak_sector_next_errors.json --dry-run
```

Secret policy: read the API key only from `DEEPSEEK_API_KEY`; never write it to files, reports, logs, commits, or prompts.

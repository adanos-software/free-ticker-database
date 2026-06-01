# DeepSeek Batch Plan

Generated: `2026-06-01T15:12:47Z`

Policy: DeepSeek output is advisory triage only and cannot authorize direct data application.

## Queue Backlog

| Queue | Rows | Already Reviewed | Unreviewed | Priority |
| --- | ---: | ---: | ---: | ---: |
| masterfile_collision | 11107 | 11107 | 0 | 1 |
| otc_scope | 11056 | 4025 | 7031 | 2 |
| weak_sector | 646 | 50 | 596 | 3 |

## Selected Batch

- Queue: `otc_scope`
- Review kind: `otc_scope`
- Rows: `500`
- Batch CSV: `data/deepseek_review_jobs/next_otc_scope_batch.csv`
- Reason: Large OTC warning/source-gap queue; DeepSeek can summarize evidence gaps while OTC names and metadata remain blocked.

Run when `DEEPSEEK_API_KEY` is set:

```bash
python scripts/run_deepseek_review_queue.py --input-csv data/deepseek_review_jobs/next_otc_scope_batch.csv --review-kind otc_scope --limit 500 --batch-size 10 --raw-responses-jsonl data/deepseek_review_jobs/raw_responses.jsonl --normalized-json data/deepseek_review_jobs/otc_scope_next_normalized_reviews.json --normalized-csv data/deepseek_review_jobs/otc_scope_next_normalized_reviews.csv --errors-json data/deepseek_review_jobs/otc_scope_next_errors.json
```

Schema-only dry run:

```bash
python scripts/run_deepseek_review_queue.py --input-csv data/deepseek_review_jobs/next_otc_scope_batch.csv --review-kind otc_scope --limit 500 --batch-size 10 --raw-responses-jsonl data/deepseek_review_jobs/raw_responses.jsonl --normalized-json data/deepseek_review_jobs/otc_scope_next_normalized_reviews.json --normalized-csv data/deepseek_review_jobs/otc_scope_next_normalized_reviews.csv --errors-json data/deepseek_review_jobs/otc_scope_next_errors.json --dry-run
```

Secret policy: read the API key only from `DEEPSEEK_API_KEY`; never write it to files, reports, logs, commits, or prompts.

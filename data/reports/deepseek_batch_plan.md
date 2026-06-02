# DeepSeek Batch Plan

Generated: `2026-06-02T16:33:15Z`

Policy: DeepSeek output is advisory triage only and cannot authorize direct data application.

## Queue Backlog

| Queue | Rows | Duplicate Keys | Already Reviewed | Unreviewed | Priority |
| --- | ---: | ---: | ---: | ---: | ---: |
| masterfile_collision | 11107 | 0 | 11107 | 0 | 1 |
| otc_scope | 11056 | 0 | 11056 | 0 | 2 |
| otc_name_mismatch | 146 | 0 | 146 | 0 | 3 |
| source_gap | 3548 | 184 | 2750 | 614 | 4 |
| weak_sector | 646 | 0 | 646 | 0 | 5 |

## Selected Batch

- Queue: `source_gap`
- Review kind: `source_gap`
- Rows: `50`
- Reason: Residual source-gap queue; DeepSeek can prioritize evidence follow-up for missing ISIN, stock sector, and ETF category gaps without suggesting direct fills.
- Batch CSV: `data/deepseek_review_jobs/next_source_gap_batch.csv`

Run when `DEEPSEEK_API_KEY` is set:

```bash
python scripts/run_deepseek_review_queue.py --input-csv data/deepseek_review_jobs/next_source_gap_batch.csv --review-kind source_gap --limit 50 --batch-size 5 --raw-responses-jsonl data/deepseek_review_jobs/raw_responses.jsonl --normalized-json data/deepseek_review_jobs/source_gap_next_normalized_reviews.json --normalized-csv data/deepseek_review_jobs/source_gap_next_normalized_reviews.csv --errors-json data/deepseek_review_jobs/source_gap_next_errors.json
```

Schema-only dry run:

```bash
python scripts/run_deepseek_review_queue.py --input-csv data/deepseek_review_jobs/next_source_gap_batch.csv --review-kind source_gap --limit 50 --batch-size 5 --raw-responses-jsonl data/deepseek_review_jobs/dry_run_source_gap_next_raw_responses.jsonl --normalized-json data/deepseek_review_jobs/dry_run_source_gap_next_normalized_reviews.json --normalized-csv data/deepseek_review_jobs/dry_run_source_gap_next_normalized_reviews.csv --errors-json data/deepseek_review_jobs/dry_run_source_gap_next_errors.json --dry-run
```

Secret policy: read the API key only from `DEEPSEEK_API_KEY`; never write it to files, reports, logs, commits, or prompts.

# DeepSeek Batch Plan

Generated: `2026-06-02T23:05:27Z`

Policy: DeepSeek output is advisory triage only and cannot authorize direct data application.

## Queue Backlog

| Queue | Rows | Unique Keys | Duplicate Keys | Reviewed Keys | Unreviewed Keys | Status | Priority |
| --- | ---: | ---: | ---: | ---: | ---: | --- | ---: |
| masterfile_collision | 11176 | 11176 | 0 | 11176 | 0 | complete | 1 |
| otc_scope | 11054 | 11054 | 0 | 11054 | 0 | complete | 2 |
| otc_name_mismatch | 24 | 24 | 0 | 24 | 0 | complete | 3 |
| source_gap | 3548 | 3364 | 184 | 3364 | 0 | complete | 4 |
| weak_sector | 646 | 646 | 0 | 646 | 0 | complete | 5 |

## Selected Batch

- Queue: `None`
- Review kind: `None`
- Rows: `0`
- Reason: No unreviewed DeepSeek-supported queue rows remain.
Secret policy: read the API key only from `DEEPSEEK_API_KEY`; never write it to files, reports, logs, commits, or prompts.

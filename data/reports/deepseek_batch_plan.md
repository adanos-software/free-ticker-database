# DeepSeek Batch Plan

Generated: `2026-06-02T18:38:15Z`

Policy: DeepSeek output is advisory triage only and cannot authorize direct data application.

## Queue Backlog

| Queue | Rows | Duplicate Keys | Already Reviewed | Unreviewed | Priority |
| --- | ---: | ---: | ---: | ---: | ---: |
| masterfile_collision | 11107 | 0 | 11107 | 0 | 1 |
| otc_scope | 11056 | 0 | 11056 | 0 | 2 |
| otc_name_mismatch | 146 | 0 | 146 | 0 | 3 |
| source_gap | 3548 | 184 | 3364 | 0 | 4 |
| weak_sector | 646 | 0 | 646 | 0 | 5 |

## Selected Batch

- Queue: `None`
- Review kind: `None`
- Rows: `0`
- Reason: No unreviewed DeepSeek-supported queue rows remain.
Secret policy: read the API key only from `DEEPSEEK_API_KEY`; never write it to files, reports, logs, commits, or prompts.

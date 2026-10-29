# Symbol Changes Apply

- Generated at: `2026-10-29T12:56:47Z`
- Dry run: `false`
- Accepted rows: `1`
- Blocked/manual rows: `379`

## Accepted

| Old key | New key | ISIN | Evidence |
|---|---|---|---|
| NYSE::MSGS | NYSE::MSGK | US55825T1034 | live_official_directory_old_inactive_new_active_same_issuer |

## Blocked / Manual

| Status | Rows |
|---|---:|
| blocked_new_symbol_collision | 3 |
| blocked_new_symbol_not_active_in_official_master | 3 |
| blocked_old_symbol_not_unique_in_us_scope | 304 |
| blocked_sec_cik_mismatch | 2 |
| manual_cross_venue_ticker_reuse | 7 |
| manual_missing_identity_evidence | 2 |
| manual_non_us_or_unscoped_source | 48 |
| manual_successor_isin_mismatch | 2 |
| manual_ticker_reuse_name_mismatch | 1 |
| manual_transition_or_shell_name | 7 |

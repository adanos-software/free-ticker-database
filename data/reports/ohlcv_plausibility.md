# OHLCV Plausibility Report

Generated at: `2026-04-13T11:51:43Z`

This report uses Kronos-inspired deterministic OHLCV hygiene checks. It does not fill ISINs, sectors, or ETF categories.

## Run Scope

| Metric | Rows |
|---|---:|
| Selected listing rows | 61,955 |
| Checked rows written | 0 |
| Unchecked rows skipped | 61,955 |

## Status Counts

| Status | Rows |
|---|---:|

## Issue Counts

| Issue | Rows |
|---|---:|

## Top Flagged Exchanges

| Exchange | Not Checked | Pass | Notice | Source Gap | Warn |
|---|---:|---:|---:|---:|---:|

## Notes

- `not_checked` means no local OHLCV sample was provided and `--fetch-yahoo` was not requested.
- Default runs omit `not_checked` rows to avoid a large queue-only CSV; use `--include-not-checked` to write them.
- `source_gap` means a market-data lookup was attempted but no usable bars were found.
- `warn` is a market-data anomaly signal, not authoritative proof that the listing row is wrong.
- For network sampling, run `python3 scripts/build_ohlcv_plausibility_report.py --fetch-yahoo --max-fetch 250 --focus-status source_gap`.

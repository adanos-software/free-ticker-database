# PR Review Summary

Generated: `2026-05-25T13:47:50Z`

This PR improves the ticker database through source-gated review workflows, refreshed official masterfile evidence, and release acceptance checks. It does not authorize guessed ISINs, sectors, ETF categories, names, listings, or symbol changes.

## Scope

- Added and updated review workflows for B3 residuals, OTC scope, Canada FIGI/ISIN review, ASX residuals, weak-sector venues, masterfile collisions, symbol-change review, OHLCV plausibility, source freshness, and release acceptance.
- Regenerated listing-keyed review artifacts under `data/reports/` so unresolved gaps remain explicit source gaps instead of inferred data fills.
- Refreshed selected official exchange-directory sources through controlled network refreshes with generated-at and row-count evidence.
- Added refresh safety for partial masterfile updates so an unavailable selected source does not silently delete existing reference rows.

## Data Safety

- No uncertain identifier, sector, category, name, listing, or symbol value is filled from symbol shape, issuer-name shape, peer instruments, or stale secondary evidence.
- Review artifacts are gates, not automatic apply instructions. Rows remain blank unless exact listing-keyed official evidence passes the relevant source gate.
- OHLCV evidence is plausibility-only and never authorizes canonical data changes.
- Freshness evidence only proves source age and row count; it does not authorize data changes by itself.

## Current Evidence

| Metric | Value |
|---|---:|
| Tickers | 61,465 |
| Listing keys | 71,043 |
| Official masterfile symbols | 79,377 |
| Official masterfile matches | 51,282 |
| Official masterfile collisions queued | 11,146 |
| Official masterfile missing queued | 16,949 |
| Source gaps | 3,548 |
| Entry-quality warnings | 217 |
| Quarantine rows | 0 |

## Acceptance

| Gate | Result |
|---|---|
| `python -m pytest tests/ -q` | run before release; not captured by generated report JSON |
| `python scripts/check_entry_quality_gate.py` | passed; `unexpected_warn_count=0`, `quarantine_count=0` |
| `python scripts/validate_database.py` | passed; `failed_error_gates=0` |
| `python scripts/build_release_acceptance_report.py` | passed; `41/41` |
| CRLF-aware `git diff --check` | run before release; not captured by generated report JSON |

## Freshness

| Metric | Value |
|---|---:|
| Fresh sources | 43 |
| Old sources | 93 |
| Remaining old P1 exchange-directory sources | 1 |

Remaining P1 exchange-directory refresh work:

- `bme_security_prices_directory`

## Review Backlog

| Campaign | Rows | Status |
|---|---:|---|
| B3 official coverage, ISIN and sector residuals | 286 | partially_improved_with_residual_source_gaps |
| OTC scope review | 11,202 | scoped_as_extended_with_source_gaps_documented |
| Canada ISIN/FIGI review | 525 | figi_queue_drained_remaining_isin_first_gaps |
| ASX ETF/ISIN residuals | 114 | official_probe_reviewed_residuals_documented |
| Weak sector venue residuals | 670 | venue_specific_review_queue_with_safe_ngx_apply |
| Masterfile collision identity review | 11,107 | listing_keyed_review_queue_ready_no_symbol_only_additions |
| Symbol-change workflow | 263 | source_scope_aware_review_queue |
| OHLCV plausibility sampling | 361 | sampling_queue_enabled_plausibility_only |
| Freshness and reporting | 4,213 | global_and_source_freshness_visible |
| Before/after delta baseline | 0 | baseline_snapshot_available_for_future_campaign_deltas |

## Primary Review Files

- `data/reports/release_acceptance.md`
- `data/reports/improvement_campaigns.md`
- `data/reports/improvement_deltas.md`
- `data/reports/coverage_report.md`
- `data/reports/source_gap_classification.md`
- `data/reports/source_of_truth_decisions.md`
- `data/reports/masterfile_collision_review.md`

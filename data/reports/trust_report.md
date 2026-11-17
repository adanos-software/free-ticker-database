# Core trust report

Generated at: `2026-11-17T16:27:35Z`

Trust is measured on instrument_scope=core only. accepted_source_gap does not count as filled. Known identity bugs must be zero; 99% completeness is a fill rate, not a correctness claim. This report does not authorize inferred identifiers, sectors, categories, names, or symbol changes. A failing trust report is not a merge blocker.

Status: **FAIL**. Public 99% claim allowed: **no**.

Universe: `instrument_scope=core` — 63,517 rows.

## Completeness

| Field | Filled | n | Pct | Missing | Fills to 99% | Pass |
|---|---:|---:|---:|---:|---:|---|
| isin | 62,882 | 63,517 | 99.00% | 635 | 0 | true |
| country | 63,473 | 63,517 | 99.93% | 44 | 0 | true |
| taxonomy | 62,772 | 63,517 | 98.83% | 745 | 110 | false |

## Identity

| Metric | Count |
|---|---:|
| official_isin_mismatch | 9 |
| official_name_mismatch | 8 |
| open_collision_groups | 9 |
| quarantine_unresolved | 1,228 |
| quarantine_proposed_clear | 285 |

## Checks

| Check | Status |
|---|---|
| `core_identity_known_bugs_zero` | **fail** |
| `core_isin_coverage_99` | **pass** |
| `core_taxonomy_coverage_99` | **fail** |
| `core_country_coverage_99` | **pass** |
| `core_official_name_hard_mismatches_zero` | **fail** |
| `core_equity_recall_99` | **fail** |
| `adanos_alias_safe` | **pass** |
| `stratified_audit_99` | **fail** |

This report never claims 99% correctness without `data/reports/trust_audit.json`.

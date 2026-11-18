# Core trust report

Generated at: `2026-11-18T16:13:32Z`

Trust is measured on instrument_scope=core only. accepted_source_gap does not count as filled. Known identity bugs must be zero; 99% completeness is a fill rate, not a correctness claim. This report does not authorize inferred identifiers, sectors, categories, names, or symbol changes. A failing trust report is not a merge blocker.

Status: **FAIL**. Public 99% claim allowed: **no**.

Universe: `instrument_scope=core` — 63,533 rows.

## Completeness

| Field | Filled | n | Pct | Missing | Fills to 99% | Pass |
|---|---:|---:|---:|---:|---:|---|
| isin | 62,897 | 63,533 | 99.00% | 636 | 1 | false |
| country | 63,490 | 63,533 | 99.93% | 43 | 0 | true |
| taxonomy | 62,784 | 63,533 | 98.82% | 749 | 114 | false |

## Identity

| Metric | Count |
|---|---:|
| official_isin_mismatch | 9 |
| official_name_mismatch | 8 |
| open_collision_groups | 10 |
| quarantine_unresolved | 1,229 |
| quarantine_proposed_clear | 285 |

## Checks

| Check | Status |
|---|---|
| `core_identity_known_bugs_zero` | **fail** |
| `core_isin_coverage_99` | **fail** |
| `core_taxonomy_coverage_99` | **fail** |
| `core_country_coverage_99` | **pass** |
| `core_official_name_hard_mismatches_zero` | **fail** |
| `core_equity_recall_99` | **fail** |
| `adanos_alias_safe` | **pass** |
| `stratified_audit_99` | **fail** |

This report never claims 99% correctness without `data/reports/trust_audit.json`.

# Core trust report

Generated at: `2026-10-08T06:23:08Z`

Trust is measured on instrument_scope=core only. accepted_source_gap does not count as filled. Known identity bugs must be zero; 99% completeness is a fill rate, not a correctness claim. This report does not authorize inferred identifiers, sectors, categories, names, or symbol changes. A failing trust report is not a merge blocker.

Status: **FAIL**. Public 99% claim allowed: **no**.

Universe: `instrument_scope=core` — 63,395 rows.

## Completeness

| Field | Filled | n | Pct | Missing | Fills to 99% | Pass |
|---|---:|---:|---:|---:|---:|---|
| isin | 62,750 | 63,395 | 98.98% | 645 | 12 | false |
| country | 63,328 | 63,395 | 99.89% | 67 | 0 | true |
| taxonomy | 62,509 | 63,395 | 98.60% | 886 | 253 | false |

## Identity

| Metric | Count |
|---|---:|
| official_isin_mismatch | 5 |
| official_name_mismatch | 5 |
| open_collision_groups | 7 |
| quarantine_unresolved | 1,156 |
| quarantine_proposed_clear | 249 |

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

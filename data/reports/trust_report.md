# Core trust report

Generated at: `2026-10-29T14:19:52Z`

Trust is measured on instrument_scope=core only. accepted_source_gap does not count as filled. Known identity bugs must be zero; 99% completeness is a fill rate, not a correctness claim. This report does not authorize inferred identifiers, sectors, categories, names, or symbol changes. A failing trust report is not a merge blocker.

Status: **FAIL**. Public 99% claim allowed: **no**.

Universe: `instrument_scope=core` — 63,595 rows.

## Completeness

| Field | Filled | n | Pct | Missing | Fills to 99% | Pass |
|---|---:|---:|---:|---:|---:|---|
| isin | 62,917 | 63,595 | 98.93% | 678 | 43 | false |
| country | 63,529 | 63,595 | 99.90% | 66 | 0 | true |
| taxonomy | 62,698 | 63,595 | 98.59% | 897 | 262 | false |

## Identity

| Metric | Count |
|---|---:|
| official_isin_mismatch | 8 |
| official_name_mismatch | 6 |
| open_collision_groups | 15 |
| quarantine_unresolved | 1,260 |
| quarantine_proposed_clear | 288 |

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

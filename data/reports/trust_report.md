# Core trust report

Generated at: `2026-11-02T20:40:50Z`

Trust is measured on instrument_scope=core only. accepted_source_gap does not count as filled. Known identity bugs must be zero; 99% completeness is a fill rate, not a correctness claim. This report does not authorize inferred identifiers, sectors, categories, names, or symbol changes. A failing trust report is not a merge blocker.

Status: **FAIL**. Public 99% claim allowed: **no**.

Universe: `instrument_scope=core` — 63,642 rows.

## Completeness

| Field | Filled | n | Pct | Missing | Fills to 99% | Pass |
|---|---:|---:|---:|---:|---:|---|
| isin | 63,008 | 63,642 | 99.00% | 634 | 0 | true |
| country | 63,590 | 63,642 | 99.92% | 52 | 0 | true |
| taxonomy | 62,871 | 63,642 | 98.79% | 771 | 135 | false |

## Identity

| Metric | Count |
|---|---:|
| official_isin_mismatch | 8 |
| official_name_mismatch | 4 |
| open_collision_groups | 4 |
| quarantine_unresolved | 1,254 |
| quarantine_proposed_clear | 289 |

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

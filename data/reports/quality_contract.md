# Canonical quality contract

- Selected profile: `merge`
- Selected status: **PASS**
- Merge status: **PASS**
- Stable-release status: **FAIL**
- Complete-database status: **FAIL**

| Scope | Check | Status |
|---|---|---|
| merge | `legacy_database_validation` | **PASS** |
| merge | `reviewable_source_and_workflow_policy` | **PASS** |
| merge | `canonical_v4_exports_and_schema` | **PASS** |
| merge | `safe_merge_gate` | **PASS** |
| merge | `current_snapshot_keyset` | **PASS** |
| merge | `identifier_adjudications_enforced` | **PASS** |
| merge | `reference_observations_classified` | **PASS** |
| merge | `source_registry_governance_schema` | **PASS** |
| stable | `zero_unresolved_identifier_conflicts` | **FAIL** |
| stable | `official_name_reconciliation_resolved` | **FAIL** |
| stable | `all_official_full_coverage_contracts_pass` | **FAIL** |
| stable | `contributing_source_licenses_verified` | **FAIL** |
| stable | `field_level_provenance_complete` | **FAIL** |
| stable | `venue_mic_mapping_complete` | **PASS** |
| complete | `complete_listing_isin_coverage` | **FAIL** |
| complete | `complete_country_metadata` | **FAIL** |
| complete | `complete_stock_sector_metadata` | **FAIL** |
| complete | `complete_etf_category_metadata` | **FAIL** |
| complete | `zero_in_scope_official_reference_gaps` | **FAIL** |
| complete | `all_target_venues_have_full_contracts` | **FAIL** |

`merge` protects the reviewable code/data transition. `stable` additionally requires full official coverage, verified contributing-source rights, complete field provenance, and MIC mappings. `complete` additionally requires zero metadata and official-reference gaps.

# GLEIF ISIN-to-LEI comparison

Report only. No listing or identifier changes.

| Metric | Value |
| --- | ---: |
| lei_match | 17243 |
| lei_mismatch | 50 |
| lei_missing_in_adanos | 11179 |
| lei_missing_in_gleif | 67559 |
| source_relationship_rows | 9322799 |
| local_unique_isins | 69076 |
| local_isins_in_gleif | 14775 |
| conflict_review_rows | 50 |

## Sources

- https://mapping.gleif.org/api/v2/isin-lei/7fa67830-43d1-480e-bb4a-14bad664932d/download; SHA-256 `0133d62a8819a25529dc9cf134a17867c01778043680b9a43ac2c7e5382023eb`; last-modified `not supplied`.

## Limits

- Issuer-grain evidence only. LEI is not a listing join key.
- The bulk relationship file stays in the ignored cache. Only metrics and conflict review rows are exported.
- Flags can overlap. Missing coverage does not prove a wrong local identifier.
- Mapping-file reuse rights are separate from LEI reference-data CC0 terms. No names or identifiers are applied.

# Archived Script Boundary

Dormant campaign scripts should move here only after their shared helpers have been extracted to `scripts/lib/` and active imports/tests prove the living pipeline still works.

Current M2 extraction moved shared helper ownership to:

- `scripts/lib/dataio.py`: `merge_metadata_updates`
- `scripts/lib/http.py`: `socket_timeout`
- `scripts/lib/normalize.py`: `names_match`

The compatibility imports in older campaign scripts can remain until those scripts are actually archived.


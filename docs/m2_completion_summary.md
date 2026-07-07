# M2 Completion Summary

Completed from issue #118 M2:

- A2: Added `scripts/apply_symbol_changes.py` for gated US rename apply. It rekeys `data/listings.csv`, `data/listing_index.csv`, and `data/identifiers_extended.csv` only when official masterfile evidence proves old inactive, new active, same exchange, unchanged ISIN, and no ticker/listing-key collision. The symbol-change workflow now runs the apply step through real CI.
- A3: Added `scripts/apply_delistings.py` for gated delisting apply. BSE `delisted` rows become drop-entry overrides; `master_absent` stays manual until rename-vs-delisting classification.
- A4: Added deterministic source-batch rotation to `scripts/fetch_exchange_masterfiles.py`, `scripts/build_masterfile_diff_report.py`, and `.github/workflows/masterfile-rotation.yml`. The daily batch reports new, vanished, name, ISIN, and field changes. Vanished listings feed review/classification, not direct deletion. US Stock and ETF new-listing apply is enabled.
- A6: Added `.github/workflows/release.yml` and `scripts/build_release_artifacts.py` for tag-triggered release assets built from source CSVs after validation gates. Kaggle/Hugging Face publishing hooks are guarded by repository variables and secrets.
- B3: Removed generated JSON/SQLite/parquet outputs from normal git tracking and updated CI/docs so CSVs remain the diffable source while generated artifacts are built and validated.
- B2: Added `scripts/build_evidence_archive.py`, `scripts/pipeline_manifest.json`, and `docs/m2_operations.md` to package historical campaign evidence as a release asset before working-tree removal.
- B4/B5: Added `scripts/lib/` shared helpers (`dataio`, `http`, `keys`, `normalize`) and migrated active helper imports away from dormant campaign scripts. `scripts/archive/README.md` documents the archive boundary.

Explicit follow-ups left for M3/M4 or a post-M2 cutover:

- Do not broaden rename/delisting auto-apply beyond the verified gates without venue-specific correctness work.
- Publish the M2 evidence archive once, then remove archived campaign artifacts from the working tree in a separate cleanup PR.
- Optional history-size cleanup for previously committed generated binaries should use a clean clone and `git filter-repo`; this M2 work intentionally does not rewrite history.
- Expand delisting/rename classifiers to every `official_full` venue after M2 automation has run safely.

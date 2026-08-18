# Hierarchical quality contract

`data/reports/quality_contract.json` evaluates three cumulative profiles. A lower profile never implies a stronger claim.

## `merge`

The blocking PR contract requires:

- passing legacy database validation;
- visible, reviewable source files with no compressed source payloads, self-pushing workflows, or workflow-time code patching;
- canonical CSV/schema validation and exact current-listing key synchronization;
- a passing non-destructive merge gate, including zero unevidenced removals or critical-field changes;
- an exact current history snapshot;
- enforced identifier adjudications;
- classification of every official reference observation;
- the complete source-governance schema.

## `stable`

The stable-release profile includes all merge checks and additionally requires:

- every official-full coverage contract to pass;
- verified redistribution and commercial-use rights for every contributing source;
- zero field-level provenance gaps;
- reviewed MIC mapping for every venue.

A tag release currently enforces `merge --strict` and records `stable`/`complete` as advisory reports. `stable --strict` remains the target once official-full coverage, contributing-source licenses, provenance, identity, and MIC evidence are actually earned. A published `v3.x` tag is not a `stable`-profile claim.

## `complete`

The completeness profile includes all stable checks and additionally requires:

- no missing ISIN or country metadata;
- no missing stock sector or ETF category;
- no in-scope official reference gap;
- full contracts for every target venue/product scope.

## Commands

```bash
python scripts/rebuild_canonical.py
python scripts/build_quality_contract.py --profile merge --strict
python scripts/build_quality_contract.py --profile stable
python scripts/build_quality_contract.py --profile complete
```

The canonical manifest records the full commit SHA, compatibility-dataset hash, schema hashes, per-file hashes, and an aggregate hash. CI also validates the same exports by loading them into PostgreSQL.

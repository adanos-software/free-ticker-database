# Quality Improvement Plan

Generated from the April 22, 2026 full-dataset audit.

## Goal

Improve the dataset from "structurally valid" to "release-grade for downstream lookup and Adanos-style NLP use" without mixing hard data bugs with expected source-coverage gaps.

## Current State

- Structural integrity is strong: no duplicate primary tickers, no duplicate listing keys, no invalid ISIN checksums, no duplicate primary ISINs.
- Content completeness is still the main weakness: large residual counts for missing ISINs, stock sectors, ETF categories, and official-reference coverage.
- Known review backlog is already substantial and should be treated as first-party quality debt:
  - `data/review_overrides/metadata_updates.csv`
  - `data/review_overrides/remove_aliases.csv`
- The existing validation gate is too permissive for some obvious data bugs such as blank country metadata on ISIN-bearing rows and mojibake in security names.

## Principles

1. Keep release-blocking checks focused on objective, low-ambiguity defects.
2. Track source/completeness gaps separately from hard data correctness bugs.
3. Burn down known reviewed overrides before adding new enrichment sources.
4. Prioritize venue-specific campaigns instead of broad global cleanup passes.
5. Protect the Adanos export separately because alias safety has different risk than general lookup completeness.

## Workstreams

### 1. Release Gates

Objective: fail builds only for clear correctness bugs.

Scope:

- Duplicate identifiers, invalid ISINs, invalid scope relationships
- Blank `country` or `country_code` when `isin` is already present
- Mojibake or replacement-character corruption in security names
- Unexpected entry-quality warnings
- Adanos alias audit failures

Immediate status:

- In progress: harden `scripts/validate_database.py` for blank-country-with-ISIN and mojibake-name defects.

### 2. Reviewed Backlog Application

Objective: convert already-reviewed fixes into canonical exports.

Scope:

- Apply high-confidence `metadata_updates`
- Apply high-confidence `remove_aliases`
- Rebuild dataset artifacts after each batch

Execution order:

1. `country`, `country_code`, `name`, `isin`
2. `stock_sector`
3. `etf_category`
4. alias removals

### 3. Venue Completion Campaigns

Objective: reduce the largest recurring completeness gaps.

Priority order:

1. Missing primary ISINs: `SSE`, `TSX`, `TSXV`, `SZSE`, `B3`, `NYSE ARCA`
2. Missing stock sectors: `OTC`, `NSE_IN`, `TSXV`, `LSE`, `B3`
3. Missing ETF categories: `B3`, `KRX`, `NYSE ARCA`, `SSE`, `SZSE`, `NASDAQ`, `BATS`, `XETRA`

Per-venue policy:

- Prefer official exchange or registry sources first
- Use reviewed secondary sources only for residual tails
- Avoid mixing source discovery with taxonomy cleanup in the same batch

### 4. Name and Encoding Cleanup

Objective: remove low-volume but high-visibility presentation bugs.

Scope:

- Mojibake in `name`
- Blank country metadata on otherwise well-identified rows
- Residual official-name mismatches in warn queues

### 5. Adanos Alias Safety

Objective: make the Adanos export safe for mention detection.

Scope:

- Treat reviewed alias removals as release debt, not optional cleanup
- Add regression checks for production false-positive aliases
- Keep `data/adanos/ticker_reference.csv` aligned with canonical alias ordering and content

## Success Metrics

### Release Metrics

- `rows_missing_country_metadata_despite_isin = 0`
- `rows_with_mojibake_names = 0`
- unexpected warn count = `0`
- Adanos alias findings = `0`

### Completeness Metrics

- `source_gap_rows < 10,000`
- `allowed_warn_rows < 300`
- blank `country` rows = `0`
- open alias-removal pairs materially reduced from current baseline

### Operational Metrics

- Each venue campaign produces a before/after delta for ISIN, sector, and ETF-category coverage
- Each reviewed batch is reproducible from committed inputs and scripts

## Execution Order

1. Harden release gates for objective defects
2. Burn down reviewed metadata/name/country fixes
3. Burn down reviewed alias removals
4. Run venue completion campaigns in priority order
5. Reassess thresholds and decide which current info gates should become hard gates

## Out of Scope For Phase 1

- Promoting broad completeness gaps like `missing_stock_sector` or `missing_etf_category` directly to hard release failures
- Rebuilding the entire source-ingestion layer
- Changing the primary data model

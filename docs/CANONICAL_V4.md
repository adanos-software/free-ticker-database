# Canonical v4 foundation

The public v3 exports remain the compatibility interface. Canonical v4 adds reviewable entity separation, immutable IDs, evidence assertions, temporal listing lifecycles, and a PostgreSQL load contract without hiding source changes behind generated patch payloads.

## Entity identity

- `venue_id` identifies a venue independently of its display label.
- `instrument_id` identifies one security. A validated ISIN is preferred; unidentified instruments stay isolated by listing lifecycle.
- `listing_id` identifies one venue/symbol lifecycle. A trusted delisting followed by symbol reuse creates a new ID.
- `listing_key` remains the collision-safe current `exchange::ticker` lookup key, not a permanent historical identity.
- `issuer_id` is deliberately conservative and currently scoped one-to-one to the resolved instrument. CIK and LEI values remain listing-scoped assertions until separate issuer-identity adjudication proves that they can be promoted without merging unrelated funds or issuers.

## Generated tables

`data/canonical_v4/` contains:

- `sources.csv`
- `source_observations.csv`
- `venues.csv`
- `issuers.csv`
- `instruments.csv`
- `listings.csv`
- `identifier_assertions.csv`
- `field_assertions.csv`
- `provenance_gaps.csv`
- `listing_events.csv`
- `coverage_contracts.csv`
- `manifest.json`

`schema/canonical_v4_contract.json` defines the exact CSV contract. `schema/canonical_v4.sql` defines the matching PostgreSQL schema.

## Evidence policy

Accepted field or identifier assertions require listing-keyed source observations. Unknown or contradictory identity evidence is quarantined; it is never repaired by ticker-only propagation. A conflicting ISIN cannot group listings into one instrument: the affected listings remain separate instruments and the raw identifier is retained only as a quarantined assertion. A missing snapshot observation is not a delisting. A removal requires a trusted, dated source event tied to the exact prior row fingerprint.

Coverage credit is conservative:

- exact venue/symbol matches must also be identity-compatible;
- another venue carrying the same ISIN does not cover the requested venue;
- normalized symbols are candidates only until an explicit reviewed mapping exists;
- official-full claims require fresh, licensed source evidence and the configured recall threshold.

## Validation and PostgreSQL load

```bash
python scripts/rebuild_canonical.py
python scripts/validate_canonical_v4_exports.py
python scripts/load_canonical_v4_postgres.py \
  --dsn postgresql://postgres:postgres@localhost:5432/ticker
```

CI builds the visible source tree, validates every CSV relationship, loads the result into PostgreSQL, verifies row counts, and performs two byte-identical canonical builds from the same inputs.

# Identifier adjudication

ISINs identify instruments, not ticker strings. Canonical rebuilds reject ticker-only propagation and fail closed when identity evidence is incomplete.

For every ISIN attached to potentially incompatible rows:

1. rows are separated by asset type;
2. complete names are grouped with conservative complete-linkage matching—every member must be compatible with every other member;
3. short or ambiguous names do not bridge otherwise distinct families;
4. an ISIN may be retained only for a single coherent family supported by exact listing-keyed evidence;
5. unsupported assertions are cleared and written to `data/reports/identifier_quarantine.csv`;
6. the rebuild fails if incompatible assertions survive.

The resolver never invents a replacement identifier.

## Reviewed identifier ledger

Manual retention decisions live in `data/review_overrides/identifier_adjudications.csv` with the exact schema:

```text
isin,listing_key,decision,evidence_source_key,evidence_url,reviewed_at,reviewer,reason
```

Every referenced listing key must exist, carry the reviewed ISIN, belong to one coherent identity family, and include source URL, reviewer, timestamp, and rationale. Unknown keys or incomplete evidence stop the build.

## Official name reconciliation

A canonical name may change only when active official evidence matches the exact listing key, asset type, and current valid ISIN. An exact venue label is rejected when it contradicts a coherent same-ISIN peer family. Every applied change is recorded in `data/reports/official_name_reconciliation.csv`.

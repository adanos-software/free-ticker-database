# Canada FIGI Batch Probe

Generated at: `2026-05-24T14:47:59Z`

This report probes one Canada FIGI queue slice against OpenFIGI. It does not fill values.

## Summary

- Probe rows: `84`
- Accepted rows: `68`
- Errors: `0`

## Decisions

| Decision | Rows |
|---|---:|
| accept | 68 |
| no_openfigi_match | 16 |

## Policy

- This probe is read-only for dataset identifiers.
- Accepted rows still need the normal FIGI collision gate before apply.

# Canada FIGI Queue

Generated at: `2026-05-25T12:54:47Z`

This report batches TSX/TSXV/NEO rows that can be sent to OpenFIGI because they already have valid ISINs and no FIGI. It does not call OpenFIGI and does not fill values.

## Summary

- Queue rows: `0`
- Batch size: `100`
- Batches: `0`
- Excluded reviewed OpenFIGI source gaps/conflicts: `151`

## Exchanges

| Exchange | Rows |
|---|---:|

## Asset Types

| Asset type | Rows |
|---|---:|

## Apply Eligibility

| Eligibility | Rows |
|---|---:|
| keep_figi_blank_as_reviewed_openfigi_source_gap_until_stronger_source | 151 |
| no_active_openfigi_probe_rows | 1 |

## Verification Evidence

| Evidence Required | Rows |
|---|---:|
| none_queue_drained_or_candidates_excluded_as_reviewed_source_gaps | 1 |
| stronger_figi_source_required_for_reviewed_openfigi_no_match_or_cross_isin_collision | 151 |

## Review Strategies

| Strategy | Rows |
|---|---:|
| keep_reviewed_openfigi_source_gaps_excluded_until_stronger_source | 151 |
| no_active_openfigi_probe_rows_after_gates | 1 |

## Top Review Batches

| Exchange | Asset type | OpenFIGI hint | Rows | Strategy | Eligibility | Evidence required | Recommended next source | Source gate |
|---|---|---|---:|---|---|---|---|---|
| reviewed_openfigi_source_gap | all | CN | 151 | keep_reviewed_openfigi_source_gaps_excluded_until_stronger_source | keep_figi_blank_as_reviewed_openfigi_source_gap_until_stronger_source | stronger_figi_source_required_for_reviewed_openfigi_no_match_or_cross_isin_collision | Stronger FIGI source or reviewed OpenFIGI re-check evidence for the existing reviewed source gap. | Do not re-probe or fill FIGI from symbol/name; keep blank until stronger reviewed FIGI evidence exists. |

## Policy

- Rows without ISIN are excluded from the FIGI queue.
- Rows already reviewed as OpenFIGI no-match or FIGI-collision source gaps are excluded from repeat probing.
- NEO, TSX, and TSXV use the OpenFIGI Canada exchange hint `CN`.
- Applying FIGIs still requires the existing collision gate in `scripts/enrich_global_identifiers.py`.

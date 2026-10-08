# Delisting Apply

- Generated at: `2026-10-08T15:21:52Z`
- Apply mode: `true`
- Applied rows: `0`
- Already-applied rows: `0`
- Drafted rows: `0`
- Blocked rows: `1`
- Manual rows: `275`

Only BSE ListofScripData Delisted rows and Nasdaq Trader trading-system Delete rows with matching observation evidence are eligible for automatic drop overrides. `master_absent` rows stay manual until rename-vs-delisting is classified.

Reviewed 2026-10-08: `NASDAQ::TCBI` (Texas Capital Bancshares) had official Nasdaq `Delete` evidence, but this is a **venue transfer** to TXSE (Form 25; TXSE trading started 2026-10-08, ticker remains TCBI until TXCP on 2026-11-09). TXSE is not a listing venue in this dataset. Applying the drop would delete the only US listing and promote `FSX::TCA` as the global primary. Keep `NASDAQ::TCBI`.

## Status Counts

| Status | Rows |
|---|---:|
| blocked_venue_transfer_keep_listing | 1 |
| manual_rename_vs_delisting_required | 275 |

# Source licensing and redistribution policy

The MIT license covers repository code and original project material. It does not automatically grant rights to redistribute data obtained from an exchange, registry, commercial provider, or website.

Every source in `data/masterfiles/sources.json` therefore records:

- source-specific license status and evidence URL;
- raw-data redistribution permission;
- derived-fact redistribution status;
- attribution requirements;
- commercial-use status;
- reviewed terms version, SHA-256, and review timestamp;
- freshness SLA and operational status.

Unknown terms remain conservative:

```json
{
  "license_status": "review_required",
  "raw_redistribution_allowed": false,
  "derived_facts_redistribution_status": "review_required",
  "commercial_use_status": "review_required"
}
```

This is not a legal conclusion that use is prohibited. It means the repository may not claim a stable redistributable release from that source until source-specific terms have been reviewed and recorded. The merge profile checks the governance schema; the stable profile blocks unresolved rights for every source that contributes to an official-full claim.

## Status meanings

- `review_required`: terms have not been reviewed; permission is not inferred.
- `verified_open`: terms were reviewed and they allow derived-facts redistribution plus commercial use, with hashed evidence. Only this status can pass `source_license_approved()` and unlock official-full coverage contracts.
- `verified_restricted`: terms were reviewed and they do **not** grant public derived-facts redistribution. Evidence is recorded so the conclusion is reviewable; stable and complete stay fail-closed. Do not recode restricted sources to `verified_open`.
- `internal`: project-owned internal:// sources.

## Reviewed sources

`sec_company_tickers_exchange` remains the only `verified_open` source (SEC Website Dissemination Policy).

| Source keys | Status | Terms | Why not open |
| --- | --- | --- | --- |
| `nasdaq_listed`, `nasdaq_other_listed`, `nasdaq_trading_system_adds_deletes` | `verified_restricted` | [Nasdaq Trader Copyright, Trademarks and Disclaimers](https://www.nasdaqtrader.com/Trader.aspx?id=CopyDisclaimMain) | Copy, reproduce, or distribute Content needs prior written consent, except fair use and one personal non-commercial copy. Copies must keep the copyright notice. |
| `euronext_equities`, `euronext_etfs` | `verified_restricted` | [Euronext Website Terms of Use](https://www.euronext.com/en/terms-use) | A single personal non-commercial copy is allowed. Other use, including commercial redistribution, needs express written permission. |

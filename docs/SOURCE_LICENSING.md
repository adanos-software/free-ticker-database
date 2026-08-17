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

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
| `qse_market_watch` | `verified_restricted` | [QSE Terms and Conditions of Use](https://www.qe.com.qa/disclaimer) | Content may not be copied, displayed, distributed, reused, transferred, amended, or sold. Personal, research, and educational use is allowed without commercial gain, with QSE identified as the source. QSE [data products](https://www.qe.com.qa/web/guest/data-policy) also require a Market Data Agreement. |
| `pse_listed_company_directory` | `verified_restricted` | [PSE Website Disclaimer](https://www.pse.com.ph/disclaimer/) | Individual pages may be viewed for personal, non-commercial use. Copying, storing, publishing, creating a derivative work, distributing, or transferring Contents to any third person needs prior written consent of PSE. |
| `dfm_listed_securities` | `verified_restricted` | [DFM Website Disclaimer](https://www.dfm.ae/other/disclaimer) | Website content is copyright DFM and provided as-is. “No charge for provision of data” is access-fee language, not a redistribution licence. The disclaimer does not grant commercial derived-facts use. |
| `bist_kap_mkk_listed_securities` | `verified_restricted` | [KAP Copyright and Disclaimer Notice](https://www.kap.org.tr/en/icerik/Diger/copyright-and-disclaimer-notice) | Viewing is for informational purposes only. Copying, distributing, reproducing, or storing the information needs MKK’s express prior written consent. Bulk/API redistribution of Borsa İstanbul data also requires a [Data Distribution Agreement](https://www.borsaistanbul.com/veriler/veri-yayini/borsa-istanbul-veri-dagitim-sozlesmesi). |

## Attempted reviews that stay `review_required`

`bolsa_santiago_instruments` was reviewed and is **not** marked open. Chile INE open-data terms do not cover the exchange. The main Bolsa de Santiago site and shop legal pages returned bot-challenge HTML, so their original bytes could not be hashed. Converted shop text (different hostname, `tiendaonline.bolsadesantiago.com`) prohibits reproducing web contents without written permission and grants no implicit licence, but that page governs the shop, not the instruments API. Status stays `review_required` until a fetchable hash of the governing terms exists.

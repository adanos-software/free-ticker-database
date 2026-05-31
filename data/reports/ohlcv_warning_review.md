# OHLCV Warning Review

Generated at: `2026-05-25T11:11:55Z`

This queue turns checked OHLCV anomalies into official listing-keyed review work. It does not authorize canonical data changes.

## Summary

| Metric | Rows |
|---|---:|
| Review rows | 16 |

## Review Buckets

| Bucket | Rows |
|---|---:|
| official_corporate_action_and_listing_status_review | 1 |
| official_listing_status_and_market_data_cross_check | 15 |

## Official Review Batches

| Exchange | Bucket | Priority | Rows | Next Source |
|---|---|---|---:|---|
| LSE | official_listing_status_and_market_data_cross_check | P1 | 4 | Official LSE instrument page, LSE notices, and issuer corporate-action announcements. |
| LSE | official_corporate_action_and_listing_status_review | P1 | 1 | Official LSE instrument page, LSE notices, and issuer corporate-action announcements. |
| LSE | official_listing_status_and_market_data_cross_check | P2 | 10 | Official LSE instrument page, LSE notices, and issuer corporate-action announcements. |
| SZSE | official_listing_status_and_market_data_cross_check | P2 | 1 | Official SZSE security page, SZSE announcements, and issuer corporate-action announcements. |

## Authorization

| Authorization | Rows |
|---|---:|
| blocked_until_official_listing_keyed_review | 16 |

## Source Locator Status

| Status | Rows |
|---|---:|
| pending_official_exchange_page_or_notice_lookup | 2 |
| verified_official_exchange_instrument_group_page_seeded | 1 |
| verified_official_exchange_page_seeded | 13 |

# OHLCV Warning Review

Generated at: `2026-06-02T20:39:44Z`

This queue turns checked OHLCV anomalies into official listing-keyed review work. It does not authorize canonical data changes.

## Summary

| Metric | Rows |
|---|---:|
| Review rows | 120 |

## Review Buckets

| Bucket | Rows |
|---|---:|
| official_corporate_action_and_listing_status_review | 21 |
| official_listing_status_and_market_data_cross_check | 99 |

## Official Review Batches

| Exchange | Bucket | Priority | Rows | Next Source |
|---|---|---|---:|---|
| OTC | official_corporate_action_and_listing_status_review | P1 | 18 | Official OTC listing-status page, exchange notices, and issuer corporate-action announcements. |
| LSE | official_listing_status_and_market_data_cross_check | P1 | 17 | Official LSE instrument page, LSE notices, and issuer corporate-action announcements. |
| KRX | official_listing_status_and_market_data_cross_check | P1 | 4 | Official KRX listing-status page, exchange notices, and issuer corporate-action announcements. |
| BATS | official_listing_status_and_market_data_cross_check | P1 | 1 | Official BATS listing-status page, exchange notices, and issuer corporate-action announcements. |
| LSE | official_corporate_action_and_listing_status_review | P1 | 1 | Official LSE instrument page, LSE notices, and issuer corporate-action announcements. |
| NASDAQ | official_corporate_action_and_listing_status_review | P1 | 1 | Official NASDAQ listing-status page, exchange notices, and issuer corporate-action announcements. |
| OTC | official_listing_status_and_market_data_cross_check | P1 | 1 | Official OTC listing-status page, exchange notices, and issuer corporate-action announcements. |
| TSE | official_corporate_action_and_listing_status_review | P1 | 1 | Official TSE listing-status page, exchange notices, and issuer corporate-action announcements. |
| TSE | official_listing_status_and_market_data_cross_check | P1 | 1 | Official TSE listing-status page, exchange notices, and issuer corporate-action announcements. |
| LSE | official_listing_status_and_market_data_cross_check | P2 | 38 | Official LSE instrument page, LSE notices, and issuer corporate-action announcements. |
| OTC | official_listing_status_and_market_data_cross_check | P2 | 10 | Official OTC listing-status page, exchange notices, and issuer corporate-action announcements. |
| TSXV | official_listing_status_and_market_data_cross_check | P2 | 6 | Official TSXV listing-status page, exchange notices, and issuer corporate-action announcements. |
| SSE | official_listing_status_and_market_data_cross_check | P2 | 5 | Official SSE listing-status page, exchange notices, and issuer corporate-action announcements. |
| TSE | official_listing_status_and_market_data_cross_check | P2 | 5 | Official TSE listing-status page, exchange notices, and issuer corporate-action announcements. |
| IDX | official_listing_status_and_market_data_cross_check | P2 | 2 | Official IDX listing-status page, exchange notices, and issuer corporate-action announcements. |
| NASDAQ | official_listing_status_and_market_data_cross_check | P2 | 2 | Official NASDAQ listing-status page, exchange notices, and issuer corporate-action announcements. |
| TWSE | official_listing_status_and_market_data_cross_check | P2 | 2 | Official TWSE listing-status page, exchange notices, and issuer corporate-action announcements. |
| AMS | official_listing_status_and_market_data_cross_check | P2 | 1 | Official AMS listing-status page, exchange notices, and issuer corporate-action announcements. |
| NYSE | official_listing_status_and_market_data_cross_check | P2 | 1 | Official NYSE listing-status page, exchange notices, and issuer corporate-action announcements. |
| NYSE ARCA | official_listing_status_and_market_data_cross_check | P2 | 1 | Official NYSE ARCA listing-status page, exchange notices, and issuer corporate-action announcements. |
| TSX | official_listing_status_and_market_data_cross_check | P2 | 1 | Official TSX listing-status page, exchange notices, and issuer corporate-action announcements. |
| XETRA | official_listing_status_and_market_data_cross_check | P2 | 1 | Official XETRA listing-status page, exchange notices, and issuer corporate-action announcements. |

## Authorization

| Authorization | Rows |
|---|---:|
| blocked_until_official_listing_keyed_review | 120 |

## Source Locator Status

| Status | Rows |
|---|---:|
| pending_official_exchange_page_or_notice_lookup | 106 |
| verified_official_exchange_instrument_group_page_seeded | 1 |
| verified_official_exchange_page_seeded | 13 |

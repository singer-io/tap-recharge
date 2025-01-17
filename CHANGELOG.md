# Changelog

## 2.0.4
  * Fix Dependabot issue
  * [#44](https://github.com/singer-io/tap-recharge/pull/44)

## 2.0.3
  * Eliminate repetition of API calls
  * [#43](https://github.com/singer-io/tap-recharge/pull/43)

## 2.0.2
  * Adds ChunkedEncodingError to backoff handling
  * Adds unittest
  * [#42](https://github.com/singer-io/tap-recharge/pull/42)

## 2.0.1
  * Removes deprecated `shopify_customer_id` field
  * Re-orders customers.json
  * Adds ipdb to dev install
  * [#39](https://github.com/singer-io/tap-recharge/pull/39)

## 2.0.0
  * Changed Shop API [#27](https://github.com/singer-io/tap-recharge/pull/27)
  * Updated the tap to use Old API Version (2021-01) for the Products stream [#29](https://github.com/singer-io/tap-recharge/pull/29)
  * Added custom error handling as per recharge error codes [#30](https://github.com/singer-io/tap-recharge/pull/30)
  * Added missing fields and new fields due to API Upgrade. [#32](https://github.com/singer-io/tap-recharge/pull/32)
  * Updated datatype for certain fields [#33](https://github.com/singer-io/tap-recharge/pull/33)
  * Updated pagination to cursor based for certain streams [#34](https://github.com/singer-io/tap-recharge/pull/34)
  * Added missing integration tests [#31](https://github.com/singer-io/tap-recharge/pull/31)

## 1.1.5
  * Fix Transform Issue [#25](https://github.com/singer-io/tap-recharge/pull/25)

## 1.1.4
  * Minor change due to intermittent 429 errors: 1) Reduce client rate limit from 120 per 60s to 100 per 60s; 2) Add a 5s time delay/retry when 429 error occurs. [Recharge Leaky Bucket Rate Limits](https://docs.rechargepayments.com/docs/api-rate-limits)

## 1.1.3
  * Minor change to improve unterminated string error handling [issue #4](https://github.com/singer-io/tap-recharge/issues/4)

## 1.1.2
  * Request timeout functionality added [#19](https://github.com/singer-io/tap-recharge/pull/19)

## 1.1.1
  * Fix bookmark access [#17](https://github.com/stitchdata/sources-utils/pull/17)

## 1.1.0
  * Refactored the tap with a class based approach [#13](https://github.com/stitchdata/sources-utils/pull/13) / [#14](https://github.com/singer-io/tap-recharge/pull/14)
  * Add integration tests [#14](https://github.com/stitchdata/sources-utils/pull/14)

## 1.0.4
  * Update rate limits to match ReCharge's [#8](https://github.com/singer-io/tap-recharge/pull/8)

## 1.0.3
  * Fix response message being truncated. Change `client.py` to request `stream=True` and error message to include `response.content`. Decrease `sync.py` batch size to `pg_size = 100`.

## 1.0.2
  * Upgrade `singer-python` and `requests` libraries. Reduce batch sizes. Better error logging in `client.py` to log `unterminated string` error results.

## 1.0.1
  * Fix bookmarking. Data is not sorted; need to write bookmark after ALL pages.

## 1.0.0
  * Preparing for v1.0.0 release

## 0.0.1
  * Initial commit

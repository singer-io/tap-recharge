# Changelog

## 1 1.3
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

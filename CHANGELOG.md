# Changelog

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

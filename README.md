# tap-recharge

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from the [ReCharge Payments API](https://developer.rechargepayments.com/)
- Extracts the following resources:
  - [Addresses](https://developer.rechargepayments.com/2021-11/addresses/list_addresses)
  - [Charges](https://developer.rechargepayments.com/2021-11/charges/charge_list)
  - [Collections, DEPRECATED](https://developer.rechargepayments.com/#list-collections-alpha)
  - [Customers](https://developer.rechargepayments.com/2021-11/customers/customers_list)
  - [Discounts](https://developer.rechargepayments.com/2021-11/discounts/discounts_list)
  - [Metafields for Store, Customers, Subscriptions](https://developer.rechargepayments.com/2021-11/metafields/metafields_list)
  - [One-time Products](https://developer.rechargepayments.com/2021-11/onetimes/onetimes_list)
  - [Orders](https://developer.rechargepayments.com/2021-11/orders/orders_list)
  - [Payment Methods](https://developer.rechargepayments.com/2021-11/payment_methods/payment_methods_list)
  - [Products](https://developer.rechargepayments.com/2021-11/products/products_list)
  - [Store](https://developer.rechargepayments.com/2021-11/store/store_retrieve)
  - [Subscriptions](https://developer.rechargepayments.com/2021-11/subscriptions/subscriptions_list)
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

## Streams
[**addresses**](https://developer.rechargepayments.com/2021-11/addresses/list_addresses)
- Endpoint: https://api.rechargeapps.com/addresses
- Primary keys: id
- Foreign keys: customer_id (customers), discount_id (discounts)
- Replication strategy: Incremental (query filtered)
  - Bookmark query parameter: updated_at_min
  - Bookmark: updated_at (date-time)
- Transformations: None

[**charges**](https://developer.rechargepayments.com/2021-11/charges/charge_list)
- Endpoint: https://api.rechargeapps.com/charges
- Primary keys: id
- Foreign keys: address_id (addresses), customer_id (customers), subscription_id (subscriptions), external_product_id, external_variant_id, transaction_id
- Replication strategy: Incremental (query filtered)
  - Bookmark query parameter: updated_at_min
  - Bookmark: updated_at (date-time)
- Transformations: None

[**collections**](https://developer.rechargepayments.com/#list-collections-alpha)
- Endpoint: https://api.rechargeapps.com/collections
- This endpoint is DEPRECATED (not included) in 2021-11 version of the API
- Primary keys: id
- Foreign keys: None
- Replication strategy: Incremental (query all, filter results)
  - Bookmark: updated_at (date-time)
- Transformations: None

[**customers**](https://developer.rechargepayments.com/2021-11/customers/customers_list)
- Endpoint: https://api.rechargeapps.com/customers
- Primary keys: id
- Foreign keys: external_customer_id
- Replication strategy: Incremental (query filtered)
  - Bookmark query parameter: updated_at_min
  - Bookmark: updated_at (date-time)
- Transformations: None

[**discounts**](https://developer.rechargepayments.com/2021-11/discounts/discounts_list)
- Endpoint: https://api.rechargeapps.com/discounts
- Primary keys: id
- Foreign keys: applies_to_id
- Replication strategy: Incremental (query filtered)
  - Bookmark query parameter: updated_at_min
  - Bookmark: updated_at (date-time)
- Transformations: None

[**metafields_customer**](hhttps://developer.rechargepayments.com/2021-11/metafields/metafields_list)
- Endpoint: https://api.rechargeapps.com/metafields
- Primary keys: id
- Foreign keys: owner_id
- Replication strategy: Incremental (query all, filter results)
  - Filter: owner_resource = customer
  - Bookmark: updated_at (date-time)
- Transformations: None

[**metafields_store**](https://developer.rechargepayments.com/2021-11/metafields/metafields_list)
- Endpoint: https://api.rechargeapps.com/metafields
- Primary keys: id
- Foreign keys: owner_id
- Replication strategy: Incremental (query all, filter results)
  - Filter: owner_resource = store
  - Bookmark: updated_at (date-time)
- Transformations: None

[**metafields_subscription**](https://developer.rechargepayments.com/2021-11/metafields/metafields_list)
- Endpoint: https://api.rechargeapps.com/metafields
- Primary keys: id
- Foreign keys: owner_id
- Replication strategy: Incremental (query all, filter results)
  - Filter: owner_resource = subscription
  - Bookmark: updated_at (date-time)
- Transformations: None

[**onetimes**](https://developer.rechargepayments.com/2021-11/onetimes/onetimes_list)
- Endpoint: https://api.rechargeapps.com/onetimes
- Primary keys: id
- Foreign keys: address_id (addresses), customer_id (customers), recharge_product_id (products), external_product_id, external_variant_id
- Replication strategy: Incremental (query filtered)
  - Bookmark query parameter: updated_at_min
  - Bookmark: updated_at (date-time)
- Transformations: None

[**orders**](https://developer.rechargepayments.com/2021-11/orders/orders_list)
- Endpoint: https://api.rechargeapps.com/orders
- Primary keys: id
- Foreign keys: address_id (addresses), charge_id (charges), customer_id (customers), subscription_id (subscriptions), external_product_id, external_variant_id, external_order_id, external_id, external_customer_id, transaction_id
- Replication strategy: Incremental (query filtered)
  - Bookmark query parameter: updated_at_min
  - Bookmark: updated_at (date-time)
- Transformations: None

[**payment_methods**](https://developer.rechargepayments.com/2021-11/payment_methods/payment_methods_list)
- Endpoint: https://api.rechargeapps.com/payment_methods
- Endpoint not available for ALL orgs, needs to be enabled by Recharge Support
- Primary keys: id
- Foreign keys: customer_id
- Replication strategy: Incremental (query all, filter results)
  - Bookmark: updated_at (date-time)
- Transformations: None

[**products**](https://developer.rechargepayments.com/2021-11/products/products_list)
- Endpoint: https://api.rechargeapps.com/products
- Endpoint not available for ALL orgs
- Primary keys: id
- Foreign keys: collection_id (collections), external_product_id
- Replication strategy: Incremental (query all, filter results)
  - Bookmark: updated_at (date-time)
- Transformations: None

[**store**](https://developer.rechargepayments.com/2021-11/store/store_retrieve)
- Endpoint: https://api.rechargeapps.com/store
- Primary keys: id
- Foreign keys: None
- Replication strategy: Full table
- Transformations: None

[**subscriptions**](https://developer.rechargepayments.com/2021-11/subscriptions/subscriptions_list)
- Endpoint: https://api.rechargeapps.com/subscriptions
- Primary keys: id
- Foreign keys: address_id (addresses), customer_id (customers), recharge_product_id (products), external_product_id, external_variant_id
- Replication strategy: Incremental (query filtered)
  - Bookmark query parameter: updated_at_min
  - Bookmark: updated_at (date-time)
- Transformations: None

## Quick Start

1. Install

    Clone this repository, and then install using setup.py. We recommend using a virtualenv:

    ```bash
    > virtualenv -p python3 venv
    > source venv/bin/activate
    > python setup.py install
    OR
    > cd .../tap-recharge
    > pip install .
    ```
2. Dependent libraries
    The following dependent libraries were installed.
    ```bash
    > pip install singer-python
    > pip install singer-tools
    > pip install target-stitch
    > pip install target-json
    
    ```
    - [singer-tools](https://github.com/singer-io/singer-tools)
    - [target-stitch](https://github.com/singer-io/target-stitch)
3. Create your tap's `config.json` file which should look like the following:

    ```json
    {
        "access_token": "YOUR_ACCESS_TOKEN",
        "start_date": "2019-01-01T00:00:00Z",
        "user_agent": "tap-recharge <api_user_email@your_company.com>",
        "request_timeout": 300
    }
    ```
    
    Optionally, also create a `state.json` file. `currently_syncing` is an optional attribute used for identifying the last object to be synced in case the job is interrupted mid-stream. The next run would begin where the last job left off. The `request_timeout` is an optional parameter to set a timeout for requests. Default: 300 seconds

    ```json
    {
        "currently_syncing": "addresses",
        "bookmarks": {
            "addresses": "2021-10-11T13:37:55Z",
            "charges": "2021-10-19T19:48:42Z",
            "collections": "2019-01-20T00:52:46",
            "customers": "2021-10-20T00:52:46Z",
            "discounts": "2021-10-19T19:48:44Z",
            "metafields_store": "2019-10-11T13:37:55Z",
            "metafields_customers": "2019-10-19T19:48:42Z",
            "metafields_subscriptions": "2019-10-18T18:23:58Z",
            "onetimes": "2019-01-20T00:52:46",
            "orders": "2021-10-19T19:48:44Z",
            "payment_methods": "2019-10-11T13:37:55Z",
            "products": "2019-10-11T13:37:55Z",
            "store": "2019-10-19T19:48:42Z",
            "subscriptions": "2021-10-18T18:23:58Z"
        }
    }
    ```

4. Run the Tap in Discovery Mode
    This creates a catalog.json for selecting objects/fields to integrate:
    ```bash
    tap-recharge --config config.json --discover > catalog.json
    ```
   See the Singer docs on discovery mode
   [here](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#discovery-mode).

5. Run the Tap in Sync Mode (with catalog) and [write out to state file](https://github.com/singer-io/getting-started/blob/master/docs/RUNNING_AND_DEVELOPING.md#running-a-singer-tap-with-a-singer-target)

    For Sync mode:
    ```bash
    > tap-recharge --config tap_config.json --catalog catalog.json > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    To load to json files to verify outputs:
    ```bash
    > tap-recharge --config tap_config.json --catalog catalog.json | target-json > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    To pseudo-load to [Stitch Import API](https://github.com/singer-io/target-stitch) with dry run:
    ```bash
    > tap-recharge --config tap_config.json --catalog catalog.json | target-stitch --config target_config.json --dry-run > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```

6. Test the Tap
    
    While developing the ReCharge tap, the following utilities were run in accordance with Singer.io best practices:
    Pylint to improve [code quality](https://github.com/singer-io/getting-started/blob/master/docs/BEST_PRACTICES.md#code-quality):
    ```bash
    > pylint tap_recharge -d missing-docstring -d logging-format-interpolation -d too-many-locals -d too-many-arguments
    ```
    Pylint test resulted in the following score:
    ```bash
    Your code has been rated at 9.54/10
    ```

    To [check the tap](https://github.com/singer-io/singer-tools#singer-check-tap) and verify working:
    ```bash
    > tap-recharge --config tap_config.json --catalog catalog.json | singer-check-tap > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    Check tap resulted in the following:
    ```bash
    The output is valid.
    It contained 145815 messages for 12 streams.

        12 schema messages
    145766 record messages
        37 state messages

    Details by stream:
    +-------------------------+---------+---------+
    | stream                  | records | schemas |
    +-------------------------+---------+---------+
    | customers               | 7375    | 1       |
    | metafields_store        | 0       | 1       |
    | metafields_subscription | 0       | 1       |
    | charges                 | 17356   | 1       |
    | store                   | 1       | 1       |
    | plans                   | 198     | 1       |
    | discounts               | 25370   | 1       |
    | orders                  | 11666   | 1       |
    | metafields_customer     | 0       | 1       |
    | onetimes                | 0       | 1       |
    | subscriptions           | 75426   | 1       |
    | addresses               | 8374    | 1       |
    +-------------------------+---------+---------+

    ```
---

Copyright &copy; 2020 Stitch

from tap_tester import runner, connections, menagerie
from base import RechargeBaseTest


class RechargeAllFieldsTest(RechargeBaseTest):


    fields_to_remove = {
        'collections':{
            'name' # Not able to generate data
        },
        'charges':{
            'browser_ip', # Not able to generate data
            'customer_id', # Not present in the API docs
            'customer_hash', # Not present in the API docs
            'discount_codes', # Not present in the API docs
            'last_name', # Not present in the API docs
            'processor_name', # Not present in the API docs
            'shopify_order_id', # Not present in the API docs
            'has_uncommited_changes', # Not present in the API docs
            'email', # Not present in the API docs
            'total_weight', # Not present in the API docs
            'transaction_id', # Not present in the API docs
            'first_name', # Not present in the API docs
            'sub_total', # Not present in the API docs
            'note_attributes', # Not present in the API docs
            'shipments_count', # Not present in the API docs
        },
        'customers':{
            'braintree_customer_token', # Not able to generate data
            'paypal_customer_token', # Not able to generate data
            'shopify_customer_id', # Not present in the API docs
            'billing_province', # Not present in the API docs
            'status', # Not present in the API docs
            'billing_city', # Not present in the API docs
            'billing_company', # Not present in the API docs
            'number_active_subscriptions', # Not present in the API docs
            'processor_type', # Not present in the API docs
            'number_subscriptions', # Not present in the API docs
            'reason_payment_method_not_valid', # Not present in the API docs
            'has_card_error_in_dunning', # Not present in the API docs
            'billing_address1', # Not present in the API docs
            'billing_zip', # Not present in the API docs
            'billing_phone', # Not present in the API docs
            'billing_address2', # Not present in the API docs
            'billing_country' # Not present in the API docs
        },
        'subscriptions': {
            'recharge_product_id', # Not present in the API docs
            'shopify_variant_id', # Not present in the API docs
            'shopify_product_id' # Not present in the API docs
        },
        'addresses': {
            'original_shipping_lines', # Not present in the API docs
            'discount_id', # Not present in the API docs
            'cart_note', # Not present in the API docs
            'country', # Not present in the API docs
            'cart_attributes', # Not present in the API docs. 'order_attributes' is the replacement
            'note_attributes' # Not present in the API docs
        },
        'discounts': {
            'applies_to_id', # Not present in the API docs
            'usage_limit', # Not present in the API docs
            'duration_usage_limit', # Not present in the API docs
            'duration', # Not present in the API docs
            'applies_to_resource', # Not present in the API docs
            'first_time_customer_restriction', # Not able to generate data
            'applies_to_product_type', # Not present in the API docs
            'times_used', # Not present in the API docs
            'once_per_customer' # Not present in the API docs
        },
        'store': {
            'shop_email', # Not present in the API docs
            'iana_timezone', # Not present in the API docs
            'shop_phone', # Not present in the API docs
            'my_shopify_domain', # Not present in the API docs
            'domain' # Not present in the API docs
        },
        'onetimes': {
            'recharge_product_id', # Not present in the API docs
            'shopify_variant_id', # Not present in the API docs
            'status', # Not present in the API docs
            'shopify_product_id' # Not present in the API docs
        },
        'orders': {
            'hash', # Not present in the API docs
            'shopify_order_number', # Not present in the API docs
            'shopify_id', # Not present in the API docs
            'note_attributes', # Not present in the API docs
            'last_name', # Not present in the API docs
            'email', # Not present in the API docs
            'address_is_active', # Not present in the API docs
            'discount_codes', # Not present in the API docs
            'transaction_id', # Not present in the API docs
            'customer_id', # Not present in the API docs
            'shopify_customer_id', # Not present in the API docs
            'charge_id', # Not present in the API docs
            'shipping_date', # Not present in the API docs
            'shopify_order_id', # Not present in the API docs
            'shopify_cart_token', # Not present in the API docs
            'shipped_date', # Not present in the API docs
            'charge_status', # Not present in the API docs
            'total_weight', # Not present in the API docs
            'first_name', # Not present in the API docs
            'payment_processor' # Not present in the API docs
        }
    }

    def name(self):
        return "tap_tester_recharge_all_fields_test"   

    def test_run(self):
        """
        • Verify no unexpected streams were replicated
        • Verify that more than just the automatic fields are replicated for each stream. 
        • verify all fields for each stream are replicated
        """

        # Streams to verify all fields tests
        expected_streams = self.expected_streams()

        expected_automatic_fields = self.expected_automatic_fields()

        # Instantiate connection
        conn_id = connections.ensure_connection(self)

        # Instantiate connection
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Table and field selection
        test_catalogs_all_fields = [catalog for catalog in found_catalogs
                                    if catalog.get('tap_stream_id') in expected_streams]

        self.perform_and_verify_table_and_field_selection(
            conn_id, test_catalogs_all_fields)

        # Grab metadata after performing table-and-field selection to set expectations
        # used for asserting all fields are replicated
        stream_to_all_catalog_fields = dict()
        for catalog in test_catalogs_all_fields:
            stream_id, stream_name = catalog['stream_id'], catalog['stream_name']
            catalog_entry = menagerie.get_annotated_schema(conn_id, stream_id)
            fields_from_field_level_md = [md_entry['breadcrumb'][1]
                                          for md_entry in catalog_entry['metadata']
                                          if md_entry['breadcrumb'] != []]
            stream_to_all_catalog_fields[stream_name] = set(
                fields_from_field_level_md)

        # Run initial sync
        record_count_by_stream = self.run_and_verify_sync(conn_id)
        synced_records = runner.get_records_from_target_output()

        # Verify no unexpected streams were replicated
        synced_stream_names = set(synced_records.keys())
        self.assertSetEqual(expected_streams, synced_stream_names)


        for stream in expected_streams:
            with self.subTest(stream=stream):

                # Expected values
                expected_all_keys = stream_to_all_catalog_fields[stream]
                expected_automatic_keys = expected_automatic_fields.get(
                    stream, set())

                # Verify that you get some records for each stream
                self.assertGreater(record_count_by_stream.get(stream, -1), 0)

                # Verify all fields for a stream were replicated
                self.assertGreater(len(expected_all_keys), len(expected_automatic_keys))
                self.assertTrue(expected_automatic_keys.issubset(
                    expected_all_keys), msg='{} is not in "expected_all_keys"'.format(expected_automatic_keys-expected_all_keys))

                messages = synced_records.get(stream)
                # Collect actual values
                actual_all_keys = set()
                for message in messages['messages']:
                    if message['action'] == 'upsert':
                        actual_all_keys.update(message['data'].keys())

                expected_all_keys = expected_all_keys - self.fields_to_remove.get(stream,set())
                self.assertSetEqual(expected_all_keys, actual_all_keys)

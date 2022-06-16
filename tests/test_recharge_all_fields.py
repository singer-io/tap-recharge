from tap_tester import runner, connections, menagerie
from base import RechargeBaseTest


class RechargeAllFieldsTest(RechargeBaseTest):


    fields_to_remove = {
        'collections':{
            'name' # Field is not present in API doc
        },
        'charges':{
            'browser_ip' # Field is not present in API doc at first level
        },
        'customers':{
            'braintree_customer_token', # Not able to generate data
            'paypal_customer_token' # Not able to generate data
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
        
        # instantiate connection
        conn_id = connections.ensure_connection(self)
        
        # instantiate connection
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        test_catalogs_all_fields = [catalog for catalog in found_catalogs
                                    if catalog.get('tap_stream_id') in expected_streams]

        self.perform_and_verify_table_and_field_selection(
            conn_id, test_catalogs_all_fields)

        # grab metadata after performing table-and-field selection to set expectations
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
        
        # run initial sync
        record_count_by_stream = self.run_and_verify_sync(conn_id)
        synced_records = runner.get_records_from_target_output()

        # Verify no unexpected streams were replicated
        synced_stream_names = set(synced_records.keys())
        self.assertSetEqual(expected_streams, synced_stream_names)
        
        
        for stream in expected_streams:
            with self.subTest(stream=stream):

                # expected values
                expected_all_keys = stream_to_all_catalog_fields[stream]
                expected_automatic_keys = expected_automatic_fields.get(
                    stream, set())

                # Verify that you get some records for each stream
                self.assertGreater(record_count_by_stream.get(stream, -1), 0)

                # verify all fields for a stream were replicated
                self.assertGreater(len(expected_all_keys), len(expected_automatic_keys))
                self.assertTrue(expected_automatic_keys.issubset(
                    expected_all_keys), msg='{} is not in "expected_all_keys"'.format(expected_automatic_keys-expected_all_keys))

                messages = synced_records.get(stream)
                # collect actual values
                actual_all_keys = set()
                for message in messages['messages']:
                    if message['action'] == 'upsert':
                        actual_all_keys.update(message['data'].keys())

                expected_all_keys = expected_all_keys - self.fields_to_remove.get(stream,set())
                self.assertSetEqual(expected_all_keys, actual_all_keys)

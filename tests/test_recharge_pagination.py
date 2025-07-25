"""
Test tap pagination of streams
"""

from tap_tester import runner, connections

from base import RechargeBaseTest

class RechargePaginationTest(RechargeBaseTest):

    @staticmethod
    def name():
        return "tap_tester_recharge_pagination_test"

    def test_run(self):
        page_size = 25
        conn_id = connections.ensure_connection(self)

        # Checking pagination for streams with enough data
        expected_streams = [
            "addresses",
            "customers",
            "discounts",
            "metafields_subscription",
            # BUG https://jira.talendforge.org/browse/TDL-20783
            # "onetimes",
            ]

        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        test_catalogs = [catalog for catalog in found_catalogs
                         if catalog.get('stream_name') in expected_streams]

        self.perform_and_verify_table_and_field_selection(conn_id, test_catalogs)

        record_count_by_stream = self.run_and_verify_sync(conn_id)

        synced_records = runner.get_records_from_target_output()

        for stream in expected_streams:
            with self.subTest(stream=stream):
                # expected values
                expected_primary_keys = self.expected_primary_keys()

                # collect information for assertions from syncs 1 & 2 base on expected values
                record_count_sync = record_count_by_stream.get(stream, 0)
                primary_keys_list = [tuple(message.get('data').get(expected_pk)
                                           for expected_pk in expected_primary_keys[stream])
                                     for message in synced_records.get(stream).get('messages')
                                     if message.get('action') == 'upsert']

                # verify records are more than page size so multiple page is working
                self.assertGreater(record_count_sync, page_size)

                primary_keys_list_1 = primary_keys_list[:page_size]
                primary_keys_list_2 = primary_keys_list[page_size:2*page_size]

                primary_keys_page_1 = set(primary_keys_list_1)
                primary_keys_page_2 = set(primary_keys_list_2)

                # Verify by private keys that data is unique for page
                self.assertEqual(len(primary_keys_page_1), page_size)  # verify there are no dupes on a page
                self.assertTrue(primary_keys_page_1.isdisjoint(primary_keys_page_2))  # verify there are no dupes between pages

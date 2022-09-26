from tap_tester import connections, runner

from base import RechargeBaseTest


class RechargeStartDateTest(RechargeBaseTest):

    start_date_1 = ""
    start_date_2 = ""

    @staticmethod
    def name():
        return "tap_tester_recharge_start_date_test"

    def test_run(self):
        streams = self.expected_streams()
        # streams for 1st run
        streams_1 = {"customers", "subscriptions", "addresses", "onetimes"}
        self.run_test(streams_1, "2021-09-01T00:00:00Z", "2022-04-01T00:00:00Z")

        # streams of 2nd run
        streams_2 = {"collections"}
        self.run_test(streams_2, "2021-09-01T00:00:00Z", "2022-05-01T00:00:00Z")

        # rest other streams for 3rd run
        self.run_test(streams - streams_1 - streams_2, "2021-09-01T00:00:00Z", "2021-10-01T00:00:00Z")

    def run_test(self, streams, start_date_1, start_date_2):
        """Instantiate start date according to the desired data set and run the test"""

        self.start_date_1 = start_date_1
        self.start_date_2 = start_date_2

        self.start_date = self.start_date_1

        expected_streams = streams
        # BUG https://jira.talendforge.org/browse/TDL-20783
        expected_streams = streams - {"onetimes"}
        expected_replication_methods = self.expected_replication_method()

        ##########################################################################
        ### First Sync
        ##########################################################################

        # instantiate connection
        conn_id_1 = connections.ensure_connection(self)

        # run check mode
        found_catalogs_1 = self.run_and_verify_check_mode(conn_id_1)

        # table and field selection
        test_catalogs_1_all_fields = [catalog for catalog in found_catalogs_1
                                      if catalog.get('tap_stream_id') in expected_streams]
        self.perform_and_verify_table_and_field_selection(conn_id_1, test_catalogs_1_all_fields, select_all_fields=True)

        # run initial sync
        record_count_by_stream_1 = self.run_and_verify_sync(conn_id_1)
        synced_records_1 = runner.get_records_from_target_output()

        ##########################################################################
        ### Update START DATE Between Syncs
        ##########################################################################

        print("REPLICATION START DATE CHANGE: {} ===>>> {} ".format(self.start_date, self.start_date_2))
        self.start_date = self.start_date_2

        ##########################################################################
        ### Second Sync
        ##########################################################################

        # create a new connection with the new start_date
        conn_id_2 = connections.ensure_connection(self, original_properties=False)

        # run check mode
        found_catalogs_2 = self.run_and_verify_check_mode(conn_id_2)

        # table and field selection
        test_catalogs_2_all_fields = [catalog for catalog in found_catalogs_2
                                      if catalog.get('tap_stream_id') in expected_streams]
        self.perform_and_verify_table_and_field_selection(conn_id_2, test_catalogs_2_all_fields, select_all_fields=True)

        # run sync
        record_count_by_stream_2 = self.run_and_verify_sync(conn_id_2)
        synced_records_2 = runner.get_records_from_target_output()

        for stream in expected_streams:
            expected_replication_method = expected_replication_methods[stream]

            with self.subTest(stream=stream):
                # expected values
                expected_primary_keys = self.expected_primary_keys()[stream]

                # collect information for assertions from syncs 1 & 2 base on expected values
                record_count_sync_1 = record_count_by_stream_1.get(stream, 0)
                record_count_sync_2 = record_count_by_stream_2.get(stream, 0)
                primary_keys_list_1 = [tuple(message.get('data').get(expected_pk) for expected_pk in expected_primary_keys)
                                       for message in synced_records_1.get(stream, {}).get('messages', {})
                                       if message.get('action') == 'upsert']
                primary_keys_list_2 = [tuple(message.get('data').get(expected_pk) for expected_pk in expected_primary_keys)
                                       for message in synced_records_2.get(stream, {}).get('messages', {})
                                       if message.get('action') == 'upsert']
                primary_keys_sync_1 = set(primary_keys_list_1)
                primary_keys_sync_2 = set(primary_keys_list_2)

                if expected_replication_method == self.INCREMENTAL:

                    expected_replication_key = next(
                        iter(self.expected_replication_keys().get(stream, [])))
                    bookmark_keys_list_1 = [row.get('data').get(expected_replication_key)
                                            for row in synced_records_1.get(stream, {'messages': []}).get('messages', [])
                                            if row.get('data')]
                    bookmark_keys_list_2 = [row.get('data').get(expected_replication_key)
                                            for row in synced_records_2.get(stream, {'messages': []}).get('messages', [])
                                            if row.get('data')]

                   # Verify bookmark key values are greater than or equal to start date of sync 1
                    for bookmark_key_value in bookmark_keys_list_1:
                        self.assertGreaterEqual(
                                self.parse_date(bookmark_key_value),
                                self.parse_date(self.start_date_1),
                                msg="Report pertains to a date prior to our start date.\n" +
                                "Sync start_date: {}\n".format(self.start_date_1) +
                                "Record date: {} ".format(bookmark_key_value)
                            )

                    # Verify bookmark key values are greater than or equal to start date of sync 2
                    for bookmark_key_value in bookmark_keys_list_2:
                        self.assertGreaterEqual(
                                self.parse_date(bookmark_key_value),
                                self.parse_date(self.start_date_2),
                                msg="Report pertains to a date prior to our start date.\n" +
                                "Sync start_date: {}\n".format(self.start_date_2) +
                                "Record date: {} ".format(bookmark_key_value)
                          )

                    # Verify that the 1st sync with an earlier start date replicates
                    # a greater number of records as the 2nd sync.
                    self.assertGreater(record_count_sync_1, record_count_sync_2,
                                       msg="The 1st sync does not contain a greater number of records than the 2nd sync")

                    # Verify by primary key the records replicated in the 2nd sync are part of the 1st sync
                    self.assertTrue(primary_keys_sync_2.issubset(primary_keys_sync_1),
                                    msg="Records in the 2nd sync are not a subset of the 1st sync")
                elif expected_replication_method == self.FULL_TABLE:
                    # Verify that the 1st sync with an earlier start date replicates
                    # an equal number of records as the 2nd sync.
                    self.assertEqual(record_count_sync_1, record_count_sync_2,
                                     msg="The 1st sync does not contain an equal number of records as in the 2nd sync")

                    # Verify by primary key the same records are replicated in the 1st and 2nd syncs
                    self.assertSetEqual(primary_keys_sync_1, primary_keys_sync_2)
                else:
                    raise NotImplementedError(
                        "INVALID EXPECTATIONS\t\tSTREAM: {} REPLICATION_METHOD: {}".format(stream, expected_replication_method)
                    )

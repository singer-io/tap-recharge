from datetime import datetime as dt
from tap_tester import runner, connections, menagerie
from base import RechargeBaseTest

class RechargeInterruptedSyncTest(RechargeBaseTest):

    def assertIsDateFormat(self, value, str_format):
        """
            Assertion Method that verifies a string value is a formatted datetime with
            the specified format.
        """
        try:
            dt.strptime(value, str_format)
        except ValueError as err:
            raise AssertionError(f"Value: {value} does not conform to expected format: {str_format}") from err

    def name(self):
        """Returns the name of the test case"""

        return "recharge_interrupted_sync_test"

    def test_run(self):
        """
        Scenario: A sync job is interrupted. The state is saved with `currently_syncing`.
                  The next sync job kicks off and the tap picks back up on that `currently_syncing` stream.
        Expected State Structure:
            {
                "currently_syncing": "stream-name",
                "bookmarks": {
                    "stream-name-1": "bookmark-date"
                    "stream-name-2": "bookmark-date"
                }
            }
        Test Cases:
        - Verify an interrupted sync can resume based on the `currently_syncing` and stream level bookmark value.
        - Verify only records with replication-key values greater than or equal to the stream level bookmark are
            replicated on the resuming sync for the interrupted stream.
        - Verify the yet-to-be-synced streams are replicated following the interrupted stream in the resuming sync.
        """

        self.start_date = "2019-01-01T00:00:00Z"
        start_date_datetime = dt.strptime(self.start_date, "%Y-%m-%dT%H:%M:%SZ")

        conn_id = connections.ensure_connection(self, original_properties=False)

        expected_streams = {"charges", "orders", "subscriptions"}

        # Run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Catalog selection
        for catalog in found_catalogs:
            if catalog["stream_name"] not in expected_streams:
                continue

            annoted_schema = menagerie.get_annotated_schema(conn_id, catalog["stream_id"])

            # De-select all fields
            non_selected_properties = annoted_schema.get("annotated-schema", {}).get("properties", {})
            non_selected_properties = non_selected_properties.keys()
            additional_md = []
            connections.select_catalog_and_fields_via_metadata(conn_id,catalog,annoted_schema,additional_md=additional_md,non_selected_fields=non_selected_properties)

        # Run sync
        record_count_by_stream_full_sync = self.run_and_verify_sync(conn_id)
        synced_records_full_sync = runner.get_records_from_target_output()
        full_sync_state = menagerie.get_state(conn_id)

        # State to run 2nd sync
        #   orders: currently syncing
        #   charges: synced records successfully
        #   subscriptions: remaining to sync
        state = {
            "currently_syncing": "orders",
            "bookmarks": { "charges": "2022-07-11T00:08:19.000000Z" }
        }

        # Set state for 2nd sync
        menagerie.set_state(conn_id, state)

        # Run sync after interruption
        record_count_by_stream_interrupted_sync = self.run_and_verify_sync(conn_id)
        synced_records_interrupted_sync = runner.get_records_from_target_output()
        final_state = menagerie.get_state(conn_id)
        currently_syncing = final_state.get('currently_syncing')

        # Checking resuming the sync resulted in a successfully saved state
        with self.subTest():

            # Verify sync is not interrupted by checking currently_syncing in the state for sync
            self.assertIsNone(currently_syncing)

            # Verify bookmarks are saved
            self.assertIsNotNone(final_state.get('bookmarks'))

            # Verify final_state is equal to uninterrupted sync's state
            # (This is what the value would have been without an interruption and proves resuming succeeds)
            self.assertDictEqual(final_state, full_sync_state)

        # Stream level assertions
        for stream in expected_streams:
            with self.subTest(stream=stream):

                # Gather expectations
                expected_replication_method = self.expected_replication_method()[stream]
                expected_replication_key = next(iter(self.expected_replication_keys()[stream]))
                
                # Gather actual results
                full_records = [message['data'] for message in synced_records_full_sync.get(stream, {}).get('messages', [])]
                full_record_count = record_count_by_stream_full_sync.get(stream, 0)
                interrupted_records = [message['data'] for message in synced_records_interrupted_sync.get(stream, {}).get('messages', [])]
                interrupted_record_count = record_count_by_stream_interrupted_sync.get(stream, 0)

                # Final bookmark after interrupted sync
                final_stream_bookmark = final_state['bookmarks'][stream]

                if expected_replication_method == self.INCREMENTAL:
                    
                    # Verify final bookmark saved match formatting standards for resuming sync
                    self.assertIsNotNone(final_stream_bookmark)
                    self.assertIsInstance(final_stream_bookmark, str)
                    self.assertIsDateFormat(final_stream_bookmark, "%Y-%m-%dT%H:%M:%S.%fZ")

                    if stream == state['currently_syncing']:

                        # Check if the interrupted stream has a bookamrk written for it
                        if state["bookmarks"].get(stream,None):        
                            interrupted_stream_bookmark = state['bookmarks'][stream]
                            interrupted_bookmark_datetime = dt.strptime(interrupted_stream_bookmark, "%Y-%m-%dT%H:%M:%S.%fZ")
                        else:
                            # Assign the start date to the interrupted stream
                            interrupted_bookmark_datetime = start_date_datetime

                        # - Verify resuming sync only replicates records with replication key values greater or
                        #       equal to the state for streams that were replicated during the interrupted sync.
                        # - Verify the interrupted sync replicates the expected record set all interrupted records are in full records
                        for record in interrupted_records:
                            rec_time = dt.strptime(record.get(expected_replication_key), "%Y-%m-%dT%H:%M:%S.%fZ")
                            self.assertGreaterEqual(rec_time, interrupted_bookmark_datetime)

                            self.assertIn(record, full_records, msg='Incremental table record in interrupted sync not found in full sync')

                        # Record count for all streams of interrupted sync match expectations
                        full_records_after_interrupted_bookmark = 0
                        for record in full_records:
                            rec_time = dt.strptime(record.get(expected_replication_key), "%Y-%m-%dT%H:%M:%S.%fZ")
                            if rec_time >= interrupted_bookmark_datetime:
                                full_records_after_interrupted_bookmark += 1

                        self.assertEqual(full_records_after_interrupted_bookmark, interrupted_record_count, \
                                         msg='Expected {} records in each sync'.format(full_records_after_interrupted_bookmark))

                    else:
                        # Get the date to start 2nd sync for non-interrupted streams
                        synced_stream_bookmark = state['bookmarks'].get(stream)
                        if synced_stream_bookmark:
                            synced_stream_datetime = dt.strptime(synced_stream_bookmark, "%Y-%m-%dT%H:%M:%S.%fZ")
                        else:
                            synced_stream_datetime = start_date_datetime

                        # Verify we replicated some records for the non-interrupted streams
                        self.assertGreater(interrupted_record_count, 0)

                        # - Verify resuming sync only replicates records with replication key values greater or equal to
                        #       the state for streams that were replicated during the interrupted sync.
                        # - Verify resuming sync replicates all records that were found in the full sync (non-interupted)
                        for record in interrupted_records:
                            rec_time = dt.strptime(record.get(expected_replication_key), "%Y-%m-%dT%H:%M:%S.%fZ")
                            self.assertGreaterEqual(rec_time, synced_stream_datetime)

                            self.assertIn(record, full_records, msg='Unexpected record replicated in resuming sync.')

                        # Verify we replicated all the records from 1st sync for the streams
                        #       that are left to sync (ie. streams without bookmark in the state)
                        if stream not in state["bookmarks"].keys():
                            for record in full_records:
                                self.assertIn(record, interrupted_records, msg='Record missing from resuming sync.' )

                elif expected_replication_method == self.FULL_TABLE:

                    # Verify full table streams do not save bookmarked values at the conclusion of a successful sync
                    self.assertNotIn(stream, full_sync_state['bookmarks'].keys())
                    self.assertNotIn(stream, final_state['bookmarks'].keys())

                    # Verify first and second sync have the same records
                    self.assertEqual(full_record_count, interrupted_record_count)
                    for rec in interrupted_records:
                        self.assertIn(rec, full_records, msg='full table record in interrupted sync not found in full sync')

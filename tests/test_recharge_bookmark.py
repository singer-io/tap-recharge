from tap_tester import menagerie, connections, runner
from base import RechargeBaseTest


class RechargeBookmarkTest(RechargeBaseTest):
    """Recharge bookmarks test"""

    def name(self):
        return "tap_tester_recharge_bookmark_test"   
    
    def test_run(self):

        """
        Verify that:
            For each stream you can do a sync which records bookmarks.
            The bookmark is the maximum value sent to the target for the replication key.
            A second sync respects the bookmark
            All data of the second sync is >= the bookmark from the first sync
            The number of records in the 2nd sync is less then the first (This assumes that
                    new data added to the stream is done at a rate slow enough that you haven't
                    doubled the amount of data from the start date to the first sync between
                    the first sync and second sync run in this test)
            For full table stream, all data replicated in sync 1 is replicated again in sync 2.
        
        PREREQUISITE
        For EACH stream that is incrementally replicated there are multiple rows of data with
            different values for the replication key
        """

        expected_streams = self.expected_streams()
        # BUG https://jira.talendforge.org/browse/TDL-20783
        expected_streams = expected_streams - {"onetimes"}
        expected_replication_keys = self.expected_replication_keys()
        expected_replication_methods = self.expected_replication_method()

        ################### First Sync #####################

        conn_id = connections.ensure_connection(self)

        # Run in check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Table and field selection
        catalog_entries = [
            catalog
            for catalog in found_catalogs
            if catalog.get("tap_stream_id") in expected_streams
        ]

        self.perform_and_verify_table_and_field_selection(conn_id, catalog_entries)

        # Run a first sync job using orchestrator
        first_sync_record_count = self.run_and_verify_sync(conn_id)
        first_sync_records = runner.get_records_from_target_output()
        first_sync_bookmarks = menagerie.get_state(conn_id)

        ######################## Update State Between Syncs ################################
        new_states = {"bookmarks": dict()}
        simulated_states = self.calculated_states_by_stream(first_sync_bookmarks)
        for stream, new_state in simulated_states.items():
            new_states["bookmarks"][stream] = new_state

        menagerie.set_state(conn_id, new_states)

        ##################### Second Sync #######################################
        second_sync_record_count = self.run_and_verify_sync(conn_id)
        second_sync_records = runner.get_records_from_target_output()
        second_sync_bookmarks = menagerie.get_state(conn_id)

        ##################### Test By Stream #########################

        for stream in expected_streams:
            with self.subTest(stream=stream):

                # Expected values
                expected_replication_method = expected_replication_methods[stream]

                # Collect information for assertions from syncs 1 & 2 base on expected values
                first_sync_count = first_sync_record_count.get(stream, 0)
                second_sync_count = second_sync_record_count.get(stream, 0)
                first_sync_messages = [
                    record.get("data")
                    for record in first_sync_records.get(stream, {}).get("messages", [])
                    if record.get("action") == "upsert"
                ]
                second_sync_messages = [
                    record.get("data")
                    for record in second_sync_records.get(stream, {}).get("messages", [])
                    if record.get("action") == "upsert"
                ]
                first_bookmark_value = first_sync_bookmarks.get("bookmarks", {stream: None}).get(stream)
                second_bookmark_value = second_sync_bookmarks.get("bookmarks", {stream: None}).get(stream)

                if expected_replication_method == self.INCREMENTAL:

                    replication_key = list(expected_replication_keys[stream])[0]

                    first_bookmark_value_ts = self.dt_to_ts(first_bookmark_value, self.DATETIME_FMT)

                    second_bookmark_value_ts = self.dt_to_ts(second_bookmark_value, self.DATETIME_FMT)

                    simulated_bookmark_value = self.dt_to_ts(new_states["bookmarks"][stream], self.DATETIME_FMT)

                    # Verify the first sync sets a bookmark of the expected form
                    self.assertIsNotNone(first_bookmark_value)
                    
                    # Verify the second sync sets a bookmark of the expected form
                    self.assertIsNotNone(second_bookmark_value)

                    # Verify the second sync bookmark is Equal to the first sync bookmark
                    self.assertEqual(second_bookmark_value, first_bookmark_value)

                    for record in first_sync_messages:

                        # Verify the first sync bookmark value is the max replication key value for a given stream
                        replication_key_value = self.dt_to_ts(record.get(replication_key), self.DATETIME_FMT)

                        self.assertLessEqual(
                            replication_key_value,
                            first_bookmark_value_ts,
                            msg="First sync bookmark was set incorrectly, a record with a greater replication-key value was synced.",
                        )

                    for record in second_sync_messages:
                        # Verify the second sync replication key value is Greater or Equal to the first sync bookmark
                        replication_key_value = self.dt_to_ts(record.get(replication_key), self.DATETIME_FMT)
                        self.assertGreaterEqual(
                            replication_key_value,
                            simulated_bookmark_value,
                            msg="Second sync records do not respect the previous bookmark.",
                        )

                        # Verify the second sync bookmark value is the max replication key value for a given stream
                        self.assertLessEqual(
                            replication_key_value,
                            second_bookmark_value_ts,
                            msg="Second sync bookmark was set incorrectly, a record with a greater replication-key value was synced.",
                        )

                    # Verify that you get less data the 2nd time around
                    self.assertLess(
                        second_sync_count,
                        first_sync_count,
                        msg="second sync didn't have less records, bookmark usage not verified",
                    )

                elif expected_replication_method == self.FULL_TABLE:

                    # Verify the syncs do not set a bookmark for full table streams
                    self.assertIsNone(first_bookmark_value)
                    self.assertIsNone(second_bookmark_value)

                    # Verify the number of records in the second sync is the same as the first
                    self.assertEqual(second_sync_count, first_sync_count)

                else:

                    raise NotImplementedError(
                        "INVALID EXPECTATIONS\t\tSTREAM: {} REPLICATION_METHOD: {}".format(stream, expected_replication_method))

                # Verify at least 1 record was replicated in the second sync
                self.assertGreater(
                    second_sync_count,
                    0,
                    msg="We are not fully testing bookmarking for {}".format(stream),
                )

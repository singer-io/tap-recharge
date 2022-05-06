import unittest
from unittest import mock
import singer
from tap_recharge.client import RechargeClient
from tap_recharge.streams import IncrementalStream

def mock_transform(*args, **kwargs):
    """Mocked transformer function which returns the first argument received"""
    # return records ie, 1st argument
    return args[0]

@mock.patch("tap_recharge.streams.IncrementalStream.get_records")
@mock.patch("singer.Transformer.transform", side_effect = mock_transform)
@mock.patch("singer.write_record")
class TestReplicationValues(unittest.TestCase):
    """Class to test sync for different replication values"""

    def test_sync_for_valid_replication_key(self, mocked_write_record, mocked_transformer, mocked_get_records):
        """Test sync method when record has a valid replication key"""

        # Mocked config parameters
        mocked_config = {
            "access_token": "dummy_token",
            "start_date": "2021-01-01T00:00:00Z"
            }
        # Client object
        client = RechargeClient(mocked_config["access_token"])

        # Stream object
        stream_obj = IncrementalStream(client)

        # Transformer object
        transfomer = singer.Transformer

        # Dummy parameters
        stream_obj.tap_stream_id = "orders"
        stream_obj.replication_key = "updated_at"
        mocked_state = {}
        mocked_schema = {}
        mocked_stream_metadata = {}

        # Mocked return value for the get_records call
        mocked_get_records.return_value = [
            {"id": "1", "updated_at": "2021-09-16T00:06:34.000000Z"},
            {"id": "2", "updated_at": "2020-09-16T00:00:34.000000Z"},
            {"id": "3", "updated_at": "2021-10-11T00:01:32.000000Z"},
            {"id": "4", "updated_at": "2021-08-21T00:51:10.000000Z"}]

        # Sync function call
        state = stream_obj.sync(mocked_state, mocked_schema, mocked_stream_metadata, mocked_config, transfomer)

        # Expected state for the bookmark
        expected_state = {"bookmarks": {"orders": "2021-10-11T00:01:32.000000Z"}}

        # Check for every mocked record is written 
        self.assertEqual(mocked_write_record.call_count, 3)

        # Check state is written as expected
        self.assertEqual(state, expected_state)

    def test_sync_for_none_replication_key(self, mocked_write_record, mocked_transformer, mocked_get_records):
        """Test sync method when record has a valid replication key"""

        # Mocked config parameters
        mocked_config = {
            "access_token": "dummy_token",
            "start_date": "2021-01-01T00:00:00Z"
            }
        # Client object
        client = RechargeClient(mocked_config["access_token"])

        # Stream object
        stream_obj = IncrementalStream(client)

        # Transformer object
        transfomer = singer.Transformer

        # Dummy parameters
        stream_obj.tap_stream_id = "orders"
        stream_obj.replication_key = "updated_at"
        mocked_state = {}
        mocked_schema = {}
        mocked_stream_metadata = {}

        # Mocked return value for the get_records call
        mocked_get_records.return_value = [
            {"id": "1", "updated_at": "2021-09-16T00:06:34.000000Z"},
            {"id": "2", "updated_at": "2020-09-16T00:00:34.000000Z"},
            {"id": "3"},
            {"id": "4", "updated_at": "2021-08-21T00:51:10.000000Z"}]

        # Sync function call
        state = stream_obj.sync(mocked_state, mocked_schema, mocked_stream_metadata, mocked_config, transfomer)
        
        expected_state = {"bookmarks": {"orders": "2021-09-16T00:06:34.000000Z"}}

        # Check for every mocked record is written 
        self.assertEqual(mocked_write_record.call_count, 3)

        # Check state is written as expected
        self.assertEqual(state, expected_state)

import unittest

from tap_recharge.streams import write_recharge_bookmark

class TestWriteRechargeBookmark(unittest.TestCase):

    def test_no_bookmark_value(self):
        # Bookmark is not present then None should be returned.
        bookmark_value = None
        expected_state_value  = {'bookmarks': {'subscriptions': None}}

        self.assertEqual(expected_state_value, write_recharge_bookmark({}, 'subscriptions', bookmark_value))

    def test_yes_bookmark_value(self):
        # Bookmark is present then stream is set to the value
        bookmark_value = '2021-10-11T09:54:55.000000Z'
        expected_state_value  = {'bookmarks': {'subscriptions': '2021-10-11T09:54:55.000000Z'}}

        self.assertEqual(expected_state_value, write_recharge_bookmark({}, 'subscriptions', bookmark_value))

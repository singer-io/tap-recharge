import unittest

from tap_recharge.streams import get_recharge_bookmark

class TestGetRechargeBookmark(unittest.TestCase):

    def test_no_bookmark_no_start_date(self):
        # Start date is None and bookmark is not present then None should be returned.
        state = {'bookmarks': {}}
        start_date = None
        expected_bookmark_value  = None

        self.assertEqual(expected_bookmark_value, get_recharge_bookmark(state, 'subscriptions', start_date))

    def test_no_bookmark_yes_start_date(self):
        # Start date is present and bookmark is not present then start date should be returned.
        state = {'bookmarks': {}}
        start_date = '2021-09-01T00:00:00Z'
        expected_bookmark_value  = '2021-09-01T00:00:00Z'

        self.assertEqual(expected_bookmark_value, get_recharge_bookmark(state, 'subscriptions', start_date))

    def test_yes_bookmark_yes_start_date(self):
        # Start date and bookmark both are present then bookmark should be returned.
        state = {'bookmarks': {'subscriptions': '2021-10-11T09:54:55.000000Z'}}
        start_date = '2021-09-01T00:00:00Z'
        expected_bookmark_value  = '2021-10-11T09:54:55.000000Z'

        self.assertEqual(expected_bookmark_value, get_recharge_bookmark(state, 'subscriptions', start_date))

    def test_yes_bookmark_no_start_date(self):
        # Start date is not present and bookmark is present then bookmark should be returned.
        state = {'bookmarks': {'subscriptions': '2021-10-11T09:54:55.000000Z'}}
        start_date = None
        expected_bookmark_value  = '2021-10-11T09:54:55.000000Z'

        self.assertEqual(expected_bookmark_value, get_recharge_bookmark(state, 'subscriptions', start_date))

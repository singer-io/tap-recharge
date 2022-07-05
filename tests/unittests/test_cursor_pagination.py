import unittest
from unittest import mock
from tap_recharge.client import RechargeClient
from tap_recharge.streams import Addresses

def get(page, *args, **kwargs):
    """
        Function to return API response with 'next_cursor' for 2 responses and last response without 'next_cursor'
    """
    if page == 3:
        return {'next_cursor': None, 'addresses': [{'key': 'value'}]}
    return {'next_cursor': f'next_cursor_{page}', 'addresses': [{'key': 'value'}]}

@mock.patch('tap_recharge.RechargeClient.request', side_effect = [get(1), get(2), get(3)])
class TestCursorPagination(unittest.TestCase):
    """
        Test case to verify we are doing API calls when we receive 'next_cursor' in API response
    """

    def test_cursor_pagination(self, mocked_get):
        # Create RechargeClient
        client = RechargeClient('test_access_token')
        # Create address object
        addresses = Addresses(client)
        # Function call
        list(addresses.get_records())

        # Get actual records with which 'RechargeClient.request' is called
        actual_calls = mocked_get.mock_calls
        # Expected calls for assertion
        expected_calls = [
            mock.call('GET', path='addresses', url=None, params={'sort_by': 'updated_at-asc', 'limit': 50}),
            mock.call('GET', path='addresses', url=None, params={'cursor': 'next_cursor_1', 'limit': 50}),
            mock.call('GET', path='addresses', url=None, params={'cursor': 'next_cursor_2', 'limit': 50})
        ]
        # verify the actual and expected calls
        self.assertEqual(actual_calls, expected_calls)

import unittest
from unittest import mock
from parameterized import parameterized
from requests.exceptions import Timeout, ConnectionError, ChunkedEncodingError
from tap_recharge.client import RechargeClient, RechargeRateLimitError, Server5xxError

class MockResponse:
    def __init__(self,  status_code, json):
        self.status_code = status_code
        self.text = json
        self.links = {}

    def json(self):
        return self.text

def get_response(status_code, json={}):
    return MockResponse(status_code, json)

class TestBackoffRequest(unittest.TestCase):
    """Test cases to verify we backoff 5 times for Timeout, ConnectionError and 5XX errors when call 'request'"""

    client_obj = RechargeClient('test_access_token')
    method = 'GET'
    path = 'path'
    url = 'url'

    @parameterized.expand([
        ['timeout_error_backoff', Timeout, None],
        ['connection_error_backoff', ConnectionError, None],
        ['server_5XX_error_backoff', Server5xxError, None],
        ['429_error_backoff', RechargeRateLimitError, None],
    ])
    @mock.patch('tap_recharge.client.RechargeClient.check_access_token')
    @mock.patch('time.sleep')
    @mock.patch('requests.Session.request')
    def test_backoff(self, name, test_exception, data, mocked_request, mocked_sleep, mocked_check_access_token):
        mocked_request.side_effect = test_exception('exception')
        with self.assertRaises(test_exception) as e:
            response_json, _ = self.client_obj.request(self.method, self.path, self.url)

        self.assertEqual(mocked_request.call_count, 5)

    @mock.patch('tap_recharge.client.RechargeClient.check_access_token')
    @mock.patch('time.sleep')
    @mock.patch('requests.Session.request', side_effect=ChunkedEncodingError)
    def test_ChunkedEncodingError(self, mocked_request, mocked_sleep, mocked_check_access_token):
        with self.assertRaises(ChunkedEncodingError) as e:
            response_json, _ = self.client_obj.request(self.method, self.path, self.url)

        self.assertEqual(mocked_request.call_count, 5)

class TestBackoffCheckAccessToken(unittest.TestCase):
    """Test cases to verify we backoff 5 times for Timeout, ConnectionError and 5XX errors when call 'check_access_token' from 'request'"""

    client_obj = RechargeClient('test_access_token')
    method = 'GET'
    path = 'path'
    url = 'url'

    @parameterized.expand([
        ['timeout_error_backoff', Timeout, None],
        ['connection_error_backoff', ConnectionError, None],
        ['server_5XX_error_backoff', Server5xxError, None],
        ['429_error_backoff', RechargeRateLimitError, None],
    ])
    @mock.patch('time.sleep')
    @mock.patch('requests.Session.request')
    def test_backoff_check_access_token(self, name, test_exception, data, mocked_request, mocked_sleep):
        mocked_request.side_effect = test_exception('exception')
        with self.assertRaises(test_exception) as e:
            response_json, _ = self.client_obj.request(self.method, self.path, self.url)

        self.assertEqual(mocked_request.call_count, 5)

class TestBackoffRechargeClientInitialization(unittest.TestCase):
    """Test cases to verify we backoff 5 times for Timeout, ConnectionError and 5XX errors when initialization 'RechargeClient' object"""

    client_obj = RechargeClient('test_access_token')
    method = 'GET'
    path = 'path'
    url = 'url'

    @parameterized.expand([
        ['timeout_error_backoff', Timeout, None],
        ['connection_error_backoff', ConnectionError, None],
        ['server_5XX_error_backoff', Server5xxError, None]
    ])
    @mock.patch('time.sleep')
    @mock.patch('requests.Session.request')
    def test_backoff_client_initialization(self, name, test_exception, data, mocked_request, mocked_sleep):
        mocked_request.side_effect = test_exception('exception')
        with self.assertRaises(test_exception) as e:
            response_json, _ = self.client_obj.request(self.method, self.path, self.url)

        self.assertEqual(mocked_request.call_count, 5)

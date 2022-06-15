import unittest
from unittest import mock
from requests.exceptions import Timeout, ConnectionError
from tap_recharge.client import RechargeClient, Server5xxError, Server429Error

class MockResponse:
    def __init__(self,  status_code, json):
        self.status_code = status_code
        self.text = json
        self.links = {}

    def json(self):
        return self.text

def get_response(status_code, json={}):
    return MockResponse(status_code, json)

@mock.patch('tap_recharge.client.RechargeClient.check_access_token')
@mock.patch('time.sleep')
@mock.patch('requests.Session.request')
class TestBackoffRequest(unittest.TestCase):
    """Test cases to verify we backoff 5 times for Timeout, ConnectionError and 5XX errors when call 'request'"""

    client_obj = RechargeClient('test_access_token')
    method = 'GET'
    path = 'path'
    url = 'url'

    def test_Timeout_error_backoff__request(self, mocked_request, mocked_sleep, mocked_check_access_token):
        """Test case to verify we backoff for 5 times for 'Timeout' error"""
        mocked_request.side_effect = Timeout('timeout error')
        with self.assertRaises(Timeout) as e:
            response_json, _ = self.client_obj.request(self.method, self.path, self.url)

        self.assertEqual(mocked_request.call_count, 5)

    def test_ConnectionError_error_backoff__request(self, mocked_request, mocked_sleep, mocked_check_access_token):
        """Test case to verify we backoff for 5 times for 'ConnectionError' error"""
        mocked_request.side_effect = ConnectionError('connection error')
        with self.assertRaises(ConnectionError) as e:
            response_json, _ = self.client_obj.request(self.method, self.path, self.url)

        self.assertEqual(mocked_request.call_count, 5)

    def test_Server5xxError_error_backoff__request(self, mocked_request, mocked_sleep, mocked_check_access_token):
        """Test case to verify we backoff for 5 times for '5XX' error"""
        mocked_request.side_effect = Server5xxError('5XX error occurred')
        with self.assertRaises(Server5xxError) as e:
            response_json, _ = self.client_obj.request(self.method, self.path, self.url)

        self.assertEqual(mocked_request.call_count, 5)

    def test_Server429Error_error_backoff__request(self, mocked_request, mocked_sleep, mocked_check_access_token):
        """Test case to verify we backoff for 5 times for '429' error"""
        mocked_request.side_effect = Server429Error('ratelimit error')
        with self.assertRaises(Server429Error) as e:
            response_json, _ = self.client_obj.request(self.method, self.path, self.url)

        self.assertEqual(mocked_request.call_count, 5)

@mock.patch('time.sleep')
@mock.patch('requests.Session.request')
class TestBackoffCheckAccessToken(unittest.TestCase):
    """Test cases to verify we backoff 5 times for Timeout, ConnectionError and 5XX errors when call 'check_access_token' from 'request'"""

    client_obj = RechargeClient('test_access_token')
    method = 'GET'
    path = 'path'
    url = 'url'

    def test_Timeout_error_backoff__check_access_token(self, mocked_request, mocked_sleep):
        """Test case to verify we backoff for 5 times for 'Timeout' error"""
        mocked_request.side_effect = Timeout('timeout error')
        with self.assertRaises(Timeout) as e:
            response_json, _ = self.client_obj.request(self.method, self.path, self.url)

        self.assertEqual(mocked_request.call_count, 5)

    def test_ConnectionError_error_backoff__check_access_token(self, mocked_request, mocked_sleep):
        """Test case to verify we backoff for 5 times for 'ConnectionError' error"""
        mocked_request.side_effect = ConnectionError('connection error')
        with self.assertRaises(ConnectionError) as e:
            response_json, _ = self.client_obj.request(self.method, self.path, self.url)

        self.assertEqual(mocked_request.call_count, 5)

    def test_Server5xxError_error_backoff__check_access_token(self, mocked_request, mocked_sleep):
        """Test case to verify we backoff for 5 times for '5XX' error"""
        mocked_request.side_effect = Server5xxError('5XX error occurred')
        with self.assertRaises(Server5xxError) as e:
            response_json, _ = self.client_obj.request(self.method, self.path, self.url)

        self.assertEqual(mocked_request.call_count, 5)

    def test_Server429Error_error_backoff__check_access_token(self, mocked_request, mocked_sleep):
        """Test case to verify we backoff for 5 times for '429' error"""
        mocked_request.side_effect = Server429Error('ratelimit error')
        with self.assertRaises(Server429Error) as e:
            response_json, _ = self.client_obj.request(self.method, self.path, self.url)

        self.assertEqual(mocked_request.call_count, 5)

@mock.patch('time.sleep')
@mock.patch('requests.Session.request')
class TestBackoffRechargeClientInitialization(unittest.TestCase):
    """Test cases to verify we backoff 5 times for Timeout, ConnectionError and 5XX errors when initialization 'RechargeClient' object"""

    def test_Timeout_error_backoff__check_access_token(self, mocked_request, mocked_sleep):
        """Test case to verify we backoff for 5 times for 'Timeout' error"""
        mocked_request.side_effect = Timeout('timeout error')
        with self.assertRaises(Timeout) as e:
            client_obj = RechargeClient('test_access_token').__enter__()

        self.assertEqual(mocked_request.call_count, 5)

    def test_ConnectionError_error_backoff__check_access_token(self, mocked_request, mocked_sleep):
        """Test case to verify we backoff for 5 times for 'ConnectionError' error"""
        mocked_request.side_effect = ConnectionError('connection error')
        with self.assertRaises(ConnectionError) as e:
            client_obj = RechargeClient('test_access_token').__enter__()

        self.assertEqual(mocked_request.call_count, 5)

    def test_Server5xxError_error_backoff__check_access_token(self, mocked_request, mocked_sleep):
        """Test case to verify we backoff for 5 times for '5XX' error"""
        mocked_request.side_effect = Server5xxError('5XX error occurred')
        with self.assertRaises(Server5xxError) as e:
            client_obj = RechargeClient('test_access_token').__enter__()

        self.assertEqual(mocked_request.call_count, 5)

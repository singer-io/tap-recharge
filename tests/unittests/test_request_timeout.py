from tap_recharge.client import RechargeClient
import unittest
from unittest import mock
from requests.exceptions import Timeout, ConnectionError
from parameterized import parameterized

class TestBackoffError(unittest.TestCase):
    '''
    Test that backoff logic works properly.
    '''
    @mock.patch('tap_recharge.client.requests.Session.request')
    @mock.patch('tap_recharge.client.RechargeClient.check_access_token')
    def test_request_timeout_and_backoff(self, mock_get_token, mock_request):
        """
        Check whether the request backoffs properly for request() for 5 times in case of Timeout error.
        """
        mock_request.side_effect = Timeout
        client = RechargeClient("dummy_access_token", "dummy_user_agent", 300)
        with self.assertRaises(Timeout):
            client.request("GET")
        self.assertEquals(mock_request.call_count, 5)

    @mock.patch('tap_recharge.client.requests.Session.request')
    def test_check_access_token_timeout_and_backoff(self, mocked_request):
        """
        Check whether the request backoffs properly for __enter__() for 5 times in case of Timeout error.
        """
        mocked_request.side_effect = Timeout

        config = {
            "access_token": "dummy_at",
            "user_agent": "dummy_ua"
        }
        # initialize 'RechargeClient'
        try:
            with RechargeClient(
                config['access_token'],
                config['user_agent'],
                config.get('request_timeout')) as client:
                pass
        except Timeout:
            pass
        # verify that we backoff for 5 times
        self.assertEquals(mocked_request.call_count, 5)

    @mock.patch('tap_recharge.client.requests.Session.request')
    def test_check_access_token_connection_error_and_backoff(self, mocked_request):
        """
        Check whether the request backoffs properly for __enter__() for 5 times in case of Timeout error.
        """
        mocked_request.side_effect = ConnectionError

        config = {
            "access_token": "dummy_at",
            "user_agent": "dummy_ua"
        }
        # initialize 'RechargeClient'
        try:
            with RechargeClient(
                config['access_token'],
                config['user_agent'],
                config.get('request_timeout')) as client:
                pass
        except ConnectionError:
            pass
        # verify that we backoff for 5 times
        self.assertEquals(mocked_request.call_count, 5)

class MockResponse():
    '''
    Mock response  object for the requests call 
    '''
    def __init__(self, resp, status_code, content=[""], headers=None, raise_error=False, text={}, links=""):
        self.json_data = resp
        self.status_code = status_code
        self.content = content
        self.headers = headers
        self.raise_error = raise_error
        self.text = text
        self.reason = "error"
        self.links = links

    def prepare(self):
        return (self.json_data, self.status_code, self.content, self.headers, self.raise_error)

    def json(self):
        return self.text

class TestRequestTimeoutValue(unittest.TestCase):
    '''
    Test that the request timeout parameter works properly in various cases
    '''
    expected_URL = 'https://api.rechargeapps.com/dummy_path'
    expected_headers = {'X-Recharge-Access-Token': 'dummy_at', 'Accept': 'application/json', 'X-Recharge-Version': '2021-11', 'User-Agent': 'dummy_ua'}

    @parameterized.expand([
        ['default_timeout', None, 600],
        ['integer_timeout', 100, 100],
        ['empty_string_timeout', '', 600],
        ['string_timeout', '100', 100],
        ['float_timeout', 100.8, 100.8]
    ])
    @mock.patch('time.sleep')
    @mock.patch('tap_recharge.client.requests.Session.request', return_value = MockResponse("", status_code=200))
    @mock.patch('tap_recharge.client.RechargeClient.check_access_token')
    def test_timeout_value(self, test_case_name, test_value, expected_value, mock_get, mock_request, mocked_sleep):
        """ 
            Unit tests to ensure that request timeout is set based on config value
        """
        config = {"access_token": "dummy_at", "user_agent": "dummy_ua"}

        # set timeout in the config
        config["request_timeout"] = test_value

        # Remove "request_timeout" form config, if the value to be passed is "None"
        if not test_value:
            config.pop("request_timeout")

        client = RechargeClient(**config)
        client.request("GET", "dummy_path")

        mock_request.assert_called_with('GET', self.expected_URL, stream=True, timeout=expected_value, headers=self.expected_headers)

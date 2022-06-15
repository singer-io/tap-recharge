import unittest
from unittest import mock
from tap_recharge.streams import Products, Collections
from tap_recharge.client import RechargeClient

class Mockresponse:
    def __init__(self, json):
        self.status_code = 200
        self.raise_error = False
        self.text = json
        self.links = {}

    def json(self):
        return self.text

class TestProductsVerison(unittest.TestCase):
    """
        Test cases to verify that we are calling 'Products' with API Version: '2021-01'
    """

    recharge_client = RechargeClient('test_access_token')

    @mock.patch('requests.Session.request', return_value=Mockresponse({'products': [{'key': 'value'}]}))
    def test_products_stream_version(self, mocked_request):
        """
            Test case to verify we are calling stream 'Products' with API Version: '2021-01'
        """
        products = Products(self.recharge_client)
        list(products.get_records())

        args, kwargs = mocked_request.call_args
        request_headers = kwargs.get('headers')

        # verify we using API Version: '2021-01'
        self.assertEqual(request_headers.get('X-Recharge-Version'), '2021-01')

    @mock.patch('requests.Session.request', return_value=Mockresponse({'collections': [{'key': 'value'}]}))
    def test_non_products_stream_version(self, mocked_request):
        """
            Test case to verify we are calling stream other than 'Products' with API Version: '2021-11'
        """
        collections = Collections(self.recharge_client)
        list(collections.get_records())

        args, kwargs = mocked_request.call_args
        request_headers = kwargs.get('headers')

        # verify we using API Version: '2021-11'
        self.assertEqual(request_headers.get('X-Recharge-Version'), '2021-11')

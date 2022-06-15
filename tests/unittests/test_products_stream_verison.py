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

@mock.patch('requests.Session.request')
class TestProductsVerison(unittest.TestCase):
    """
        Test cases to verify that we are calling 'Products' with API Version: '2021-01'
    """

    recharge_client = RechargeClient('test_access_token')

    def test_products_stream_version(self, mocked_request):
        """
            Test case to verify we are calling stream 'Products' with API Version: '2021-01'
        """
        # mock request and return value with data key
        mocked_request.return_value = Mockresponse({'products': [{'key': 'value'}]})

        # Products object
        products = Products(self.recharge_client)
        # function call
        list(products.get_records())

        # get arguments passed during calling "requests.Session.request"
        args, kwargs = mocked_request.call_args
        # get 'headers' for API call
        request_headers = kwargs.get('headers')

        # verify we using API Version: '2021-01'
        self.assertEqual(request_headers.get('X-Recharge-Version'), '2021-01')

    def test_non_products_stream_version(self, mocked_request):
        """
            Test case to verify we are calling stream other than 'Products' with API Version: '2021-11'
        """
        # mock request and return value with data key
        mocked_request.return_value = Mockresponse({'collections': [{'key': 'value'}]})

        # Collections object
        collections = Collections(self.recharge_client)
        # function call
        list(collections.get_records())

        # get arguments passed during calling "requests.Session.request"
        args, kwargs = mocked_request.call_args
        # get 'headers' for API call
        request_headers = kwargs.get('headers')

        # verify we using API Version: '2021-11'
        self.assertEqual(request_headers.get('X-Recharge-Version'), '2021-11')

import unittest
from parameterized import parameterized
from unittest import mock
from tap_recharge.client import RechargeClient, RechargeBadRequestError, RechargeUnauthorizedError, RechargeRequestFailedError, \
    RechargeForbiddenError, RechargeNotFoundError, RechargeMethodNotAllowedError, RechargeUnacceptableRequestError, \
    RechargeConflictError, RechargeJSONObjectError, RechargeUnprocessableEntityError, RechargeInvalidAPI, RechargeRateLimitError, \
    RechargeInternalServiceError, RechargeUnimplementedResourceError, RechargeThirdPartyServiceTimeoutError, Server5xxError    

class MockResponse:
    def __init__(self,  status_code, json):
        self.status_code = status_code
        self.text = json
        self.links = {}

    def json(self):
        return self.text

def get_response(status_code, json={}):
    return MockResponse(status_code, json)

class TestRechargeAPIResponseException(unittest.TestCase):
    """Test cases to verify the error from the API are displayed as expected"""

    client_obj = RechargeClient('test_access_token')
    method = 'GET'
    path = 'path'
    url = 'url'

    @mock.patch('tap_recharge.client.RechargeClient.check_access_token')
    @mock.patch('time.sleep')
    @mock.patch('requests.Session.request')
    def test_200_success_API_error(self, mocked_request, mocked_sleep, mocked_check_token):
        """Test case to verify we get proper resoponse for 200 error code"""

        mocked_request.return_value = get_response(200, {'key': 'value'})
        response_json = self.client_obj.request(self.method, self.path, self.url)

        self.assertEqual(response_json, {'key': 'value'})

    @mock.patch('tap_recharge.client.RechargeClient.check_access_token')
    @mock.patch('time.sleep')
    @mock.patch('requests.Session.request')
    @mock.patch('tap_recharge.client.LOGGER.error')
    def test_401_error_with_logger_API_error(self, mocked_logger_error, mocked_request, mocked_sleep, mocked_check_token):
        """Test case to verify we get error message as displayed in the \
            API response for 401 error and error logger is called expected message"""

        mocked_request.return_value = get_response(401, {'error': 'bad authentication'})
        with self.assertRaises(RechargeUnauthorizedError) as e:
            response_json = self.client_obj.request(self.method, self.path, self.url)

        self.assertEqual(str(e.exception), 'HTTP-error-code: 401, Error: bad authentication')
        mocked_logger_error.assert_called_with("Your API Token has been deleted or the token is invalid.\n Please re-authenticate your connection to generate a new token and resume extraction.")

    @parameterized.expand([
        ['400_error', [400, {'error': 'bad request error'}, RechargeBadRequestError], 'HTTP-error-code: 400, Error: bad request error'],
        ['401_error', [401, {'error': 'not authenticated'}, RechargeUnauthorizedError], 'HTTP-error-code: 401, Error: not authenticated'],
        ['402_error', [402, {'error': 'request failed'}, RechargeRequestFailedError], 'HTTP-error-code: 402, Error: request failed'],
        ['403_error', [403, {'error': 'forbidden for URL'}, RechargeForbiddenError], 'HTTP-error-code: 403, Error: forbidden for URL'],
        ['404_error', [404, {'error': 'not found for URL'}, RechargeNotFoundError], 'HTTP-error-code: 404, Error: not found for URL'],
        ['405_error', [405, {'error': 'method not allowed for URL'}, RechargeMethodNotAllowedError], 'HTTP-error-code: 405, Error: method not allowed for URL'],
        ['406_error', [406, {'error': 'request is not accepted'}, RechargeUnacceptableRequestError], 'HTTP-error-code: 406, Error: request is not accepted'],
        ['409_error', [409, {'error': 'there is confict at Recharge side'}, RechargeConflictError], 'HTTP-error-code: 409, Error: there is confict at Recharge side'],
        ['415_error', [415, {'error': 'JSON object error occurred'}, RechargeJSONObjectError], 'HTTP-error-code: 415, Error: JSON object error occurred'],
        ['422_error', [422, {'errors': {'platform': ['This API is not compatible with your platform']}}, RechargeUnprocessableEntityError], 'HTTP-error-code: 422, Error: {\'platform\': [\'This API is not compatible with your platform\']}'],
        ['426_error', [426, {'error': 'the API is invalid'}, RechargeInvalidAPI], 'HTTP-error-code: 426, Error: the API is invalid'],
        ['429_error', [429, {'error': 'timeout error'}, RechargeRateLimitError], 'HTTP-error-code: 429, Error: timeout error'],
        ['500_error', [500, {'error': 'internal server error'}, RechargeInternalServiceError], 'HTTP-error-code: 500, Error: internal server error'],
        ['501_error', [501, {'error': 'not implemented'}, RechargeUnimplementedResourceError], 'HTTP-error-code: 501, Error: not implemented'],
        ['502_error', [502, {'error': 'bad gateway error'}, Server5xxError], 'HTTP-error-code: 502, Error: bad gateway error'],
        ['503_error', [503, {'error': 'third party service timed out'}, RechargeThirdPartyServiceTimeoutError], 'HTTP-error-code: 503, Error: third party service timed out']
    ])
    @mock.patch('tap_recharge.client.RechargeClient.check_access_token')
    @mock.patch('time.sleep')
    @mock.patch('requests.Session.request')
    def test_API_exception_handling(self, name, test_data, expected_data, mocked_request, mocked_sleep, mocked_check_token):
        mocked_request.return_value = get_response(test_data[0], test_data[1])
        with self.assertRaises(test_data[2]) as e:
            response_json = self.client_obj.request(self.method, self.path, self.url)

        self.assertEqual(str(e.exception), expected_data)

class TestRechargeCustomException(unittest.TestCase):
    """Test cases to verify we get custom error messages when we do not recieve error from the API"""

    client_obj = RechargeClient('test_access_token')
    method = 'GET'
    path = 'path'
    url = 'url'

    @parameterized.expand([
        ['400_error', [400, RechargeBadRequestError], 'HTTP-error-code: 400, Error: The request was not understood by Recharge.'],
        ['401_error', [401, RechargeUnauthorizedError], 'HTTP-error-code: 401, Error: The request was not able to be authenticated.'],
        ['402_error', [402, RechargeRequestFailedError], 'HTTP-error-code: 402, Error: The request to the resource failed because of Payment issue.'],
        ['403_error', [403, RechargeForbiddenError], 'HTTP-error-code: 403, Error: The request was authenticated but not authorized for the requested resource (Permission scope error).'],
        ['404_error', [404, RechargeNotFoundError], 'HTTP-error-code: 404, Error: The requested resource was not found.'],
        ['405_error', [405, RechargeMethodNotAllowedError], 'HTTP-error-code: 405, Error: The provided HTTP method is not supported by the URL.'],
        ['406_error', [406, RechargeUnacceptableRequestError], 'HTTP-error-code: 406, Error: The request was unacceptable, or requesting a data source which is not allowed although permissions permit the request.'],
        ['409_error', [409, RechargeConflictError], 'HTTP-error-code: 409, Error: The request is in conflict, or would create a conflict with an existing resource.'],
        ['415_error', [415, RechargeJSONObjectError], 'HTTP-error-code: 415, Error: The request body was not a JSON object.'],
        ['422_error', [422, RechargeUnprocessableEntityError], 'HTTP-error-code: 422, Error: The request was understood but cannot be processed due to invalid or missing supplemental information.'],
        ['426_error', [426, RechargeInvalidAPI], 'HTTP-error-code: 426, Error: The request was made using an invalid API version.'],
        ['429_error', [429, RechargeRateLimitError], 'HTTP-error-code: 429, Error: The request has been rate limited.'],
        ['500_error', [500, RechargeInternalServiceError], 'HTTP-error-code: 500, Error: The request could not be processed due to internal server error.'],
        ['501_error', [501, RechargeUnimplementedResourceError], 'HTTP-error-code: 501, Error: The resource requested has not been implemented in the current version.'],
        ['502_error', [502, Server5xxError], 'HTTP-error-code: 502, Error: Unknown Error'],
        ['503_error', [503, RechargeThirdPartyServiceTimeoutError], 'HTTP-error-code: 503, Error: A third party service on which the request depends has timed out.']
    ])
    @mock.patch('tap_recharge.client.RechargeClient.check_access_token')
    @mock.patch('time.sleep')
    @mock.patch('requests.Session.request')
    def test_custom_exception_handling(self, name, test_data, expected_data, mocked_request, mocked_sleep, mocked_check_token):
        mocked_request.return_value = get_response(test_data[0])
        with self.assertRaises(test_data[1]) as e:
            response_json = self.client_obj.request(self.method, self.path, self.url)

        self.assertEqual(str(e.exception), expected_data)

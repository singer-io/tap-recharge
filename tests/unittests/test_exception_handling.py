import unittest
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

@mock.patch('tap_recharge.client.RechargeClient.check_access_token')
@mock.patch('time.sleep')
@mock.patch('requests.Session.request')
class TestRechargeAPIResponseException(unittest.TestCase):
    """Test cases to verify the error from the API are displayed as expected"""

    client_obj = RechargeClient('test_access_token')
    method = 'GET'
    path = 'path'
    url = 'url'

    def test_200_success_API_error(self, mocked_request, mocked_sleep, mocked_check_token):
        """Test case to verify we get proper resoponse for 200 error code"""

        mocked_request.return_value = get_response(200, {'key': 'value'})
        response_json, _ = self.client_obj.request(self.method, self.path, self.url)

        self.assertEqual(response_json, {'key': 'value'})

    def test_400_error_API_error(self, mocked_request, mocked_sleep, mocked_check_token):
        """Test case to verify we get error message as displayed in the API response for 400 error"""

        mocked_request.return_value = get_response(400, {'error': 'bad request error'})
        with self.assertRaises(RechargeBadRequestError) as e:
            response_json, _ = self.client_obj.request(self.method, self.path, self.url)

        self.assertEqual(str(e.exception), 'HTTP-error-code: 400, Error: bad request error')

    def test_401_error_API_error(self, mocked_request, mocked_sleep, mocked_check_token):
        """Test case to verify we get error message as displayed in the API response for 401 error"""

        mocked_request.return_value = get_response(401, {'error': 'not authenticated'})
        with self.assertRaises(RechargeUnauthorizedError) as e:
            response_json, _ = self.client_obj.request(self.method, self.path, self.url)

        self.assertEqual(str(e.exception), 'HTTP-error-code: 401, Error: not authenticated')

    @mock.patch('tap_recharge.client.LOGGER.error')
    def test_401_error_with_logger_API_error(self, mocked_logger_error, mocked_request, mocked_sleep, mocked_check_token):
        """Test case to verify we get error message as displayed in the \
            API response for 401 error and error logger is called expected message"""

        mocked_request.return_value = get_response(401, {'error': 'bad authentication'})
        with self.assertRaises(RechargeUnauthorizedError) as e:
            response_json, _ = self.client_obj.request(self.method, self.path, self.url)

        self.assertEqual(str(e.exception), 'HTTP-error-code: 401, Error: bad authentication')
        mocked_logger_error.assert_called_with("Your API Token has been deleted or the token is invalid.\n Please re-authenticate your connection to generate a new token and resume extraction.")

    def test_402_error_API_error(self, mocked_request, mocked_sleep, mocked_check_token):
        """Test case to verify we get error message as displayed in the API response for 402 error"""

        mocked_request.return_value = get_response(402, {'error': 'request failed'})
        with self.assertRaises(RechargeRequestFailedError) as e:
            response_json, _ = self.client_obj.request(self.method, self.path, self.url)

        self.assertEqual(str(e.exception), 'HTTP-error-code: 402, Error: request failed')

    def test_403_error_API_error(self, mocked_request, mocked_sleep, mocked_check_token):
        """Test case to verify we get error message as displayed in the API response for 403 error"""

        mocked_request.return_value = get_response(403, {'error': 'forbidden for URL'})
        with self.assertRaises(RechargeForbiddenError) as e:
            response_json, _ = self.client_obj.request(self.method, self.path, self.url)

        self.assertEqual(str(e.exception), 'HTTP-error-code: 403, Error: forbidden for URL')

    def test_404_error_API_error(self, mocked_request, mocked_sleep, mocked_check_token):
        """Test case to verify we get error message as displayed in the API response for 404 error"""

        mocked_request.return_value = get_response(404, {'error': 'not found for URL'})
        with self.assertRaises(RechargeNotFoundError) as e:
            response_json, _ = self.client_obj.request(self.method, self.path, self.url)

        self.assertEqual(str(e.exception), 'HTTP-error-code: 404, Error: not found for URL')

    def test_405_error_API_error(self, mocked_request, mocked_sleep, mocked_check_token):
        """Test case to verify we get error message as displayed in the API response for 405 error"""

        mocked_request.return_value = get_response(405, {'error': 'method not allowed for URL'})
        with self.assertRaises(RechargeMethodNotAllowedError) as e:
            response_json, _ = self.client_obj.request(self.method, self.path, self.url)

        self.assertEqual(str(e.exception), 'HTTP-error-code: 405, Error: method not allowed for URL')

    def test_406_error_API_error(self, mocked_request, mocked_sleep, mocked_check_token):
        """Test case to verify we get error message as displayed in the API response for 406 error"""

        mocked_request.return_value = get_response(406, {'error': 'request is not accepted'})
        with self.assertRaises(RechargeUnacceptableRequestError) as e:
            response_json, _ = self.client_obj.request(self.method, self.path, self.url)

        self.assertEqual(str(e.exception), 'HTTP-error-code: 406, Error: request is not accepted')

    def test_409_error_API_error(self, mocked_request, mocked_sleep, mocked_check_token):
        """Test case to verify we get error message as displayed in the API response for 409 error"""

        mocked_request.return_value = get_response(409, {'error': 'there is confict at Recharge side'})
        with self.assertRaises(RechargeConflictError) as e:
            response_json, _ = self.client_obj.request(self.method, self.path, self.url)

        self.assertEqual(str(e.exception), 'HTTP-error-code: 409, Error: there is confict at Recharge side')

    def test_415_error_API_error(self, mocked_request, mocked_sleep, mocked_check_token):
        """Test case to verify we get error message as displayed in the API response for 415 error"""

        mocked_request.return_value = get_response(415, {'error': 'JSON object error occurred'})
        with self.assertRaises(RechargeJSONObjectError) as e:
            response_json, _ = self.client_obj.request(self.method, self.path, self.url)

        self.assertEqual(str(e.exception), 'HTTP-error-code: 415, Error: JSON object error occurred')

    def test_422_error_API_error(self, mocked_request, mocked_sleep, mocked_check_token):
        """Test case to verify we get error message as displayed in the API response for 422 error"""

        mocked_request.return_value = get_response(422, {'errors': {'platform': ['This API is not compatible with your platform']}})
        with self.assertRaises(RechargeUnprocessableEntityError) as e:
            response_json, _ = self.client_obj.request(self.method, self.path, self.url)

        self.assertEqual(str(e.exception), 'HTTP-error-code: 422, Error: {\'platform\': [\'This API is not compatible with your platform\']}')

    def test_426_error_API_error(self, mocked_request, mocked_sleep, mocked_check_token):
        """Test case to verify we get error message as displayed in the API response for 426 error"""

        mocked_request.return_value = get_response(426, {'error': 'the API is invalid'})
        with self.assertRaises(RechargeInvalidAPI) as e:
            response_json, _ = self.client_obj.request(self.method, self.path, self.url)

        self.assertEqual(str(e.exception), 'HTTP-error-code: 426, Error: the API is invalid')

    def test_429_error_API_error(self, mocked_request, mocked_sleep, mocked_check_token):
        """Test case to verify we get error message as displayed in the API response for 429 error"""

        mocked_request.return_value = get_response(429, {'error': 'timeout error'})
        with self.assertRaises(RechargeRateLimitError) as e:
            response_json, _ = self.client_obj.request(self.method, self.path, self.url)

        self.assertEqual(str(e.exception), 'HTTP-error-code: 429, Error: timeout error')
        self.assertEqual(mocked_request.call_count, 5)

    def test_500_error_API_error(self, mocked_request, mocked_sleep, mocked_check_token):
        """Test case to verify we get error message as displayed in the API response for 500 error"""

        mocked_request.return_value = get_response(500, {'error': 'internal server error'})
        with self.assertRaises(RechargeInternalServiceError) as e:
            response_json, _ = self.client_obj.request(self.method, self.path, self.url)

        self.assertEqual(str(e.exception), 'HTTP-error-code: 500, Error: internal server error')
        self.assertEqual(mocked_request.call_count, 5)

    def test_501_error_API_error(self, mocked_request, mocked_sleep, mocked_check_token):
        """Test case to verify we get error message as displayed in the API response for 501 error"""

        mocked_request.return_value = get_response(501, {'error': 'not implemented'})
        with self.assertRaises(RechargeUnimplementedResourceError) as e:
            response_json, _ = self.client_obj.request(self.method, self.path, self.url)

        self.assertEqual(str(e.exception), 'HTTP-error-code: 501, Error: not implemented')
        self.assertEqual(mocked_request.call_count, 5)

    def test_502_error_API_error(self, mocked_request, mocked_sleep, mocked_check_token):
        """Test case to verify we get error message as displayed in the API response for 502 error"""

        mocked_request.return_value = get_response(502, {'error': 'bad gateway error'})
        with self.assertRaises(Server5xxError) as e:
            response_json, _ = self.client_obj.request(self.method, self.path, self.url)

        self.assertEqual(str(e.exception), 'HTTP-error-code: 502, Error: bad gateway error')
        self.assertEqual(mocked_request.call_count, 5)

    def test_503_error_API_error(self, mocked_request, mocked_sleep, mocked_check_token):
        """Test case to verify we get error message as displayed in the API response for 503 error"""

        mocked_request.return_value = get_response(503, {'error': 'third party service timed out'})
        with self.assertRaises(RechargeThirdPartyServiceTimeoutError) as e:
            response_json, _ = self.client_obj.request(self.method, self.path, self.url)

        self.assertEqual(str(e.exception), 'HTTP-error-code: 503, Error: third party service timed out')
        self.assertEqual(mocked_request.call_count, 5)

@mock.patch('tap_recharge.client.RechargeClient.check_access_token')
@mock.patch('time.sleep')
@mock.patch('requests.Session.request')
class TestRechargeCustomException(unittest.TestCase):
    """Test cases to verify we get custom error messages when we do not recieve error from the API"""

    client_obj = RechargeClient('test_access_token')
    method = 'GET'
    path = 'path'
    url = 'url'

    def test_400_error_custom_error(self, mocked_request, mocked_sleep, mocked_check_token):
        """Test case to verify we get custom error message for 400 error"""

        mocked_request.return_value = get_response(400)
        with self.assertRaises(RechargeBadRequestError) as e:
            response_json, _ = self.client_obj.request(self.method, self.path, self.url)

        self.assertEqual(str(e.exception), 'HTTP-error-code: 400, Error: The request was not understood by Recharge.')

    def test_401_error_custom_error(self, mocked_request, mocked_sleep, mocked_check_token):
        """Test case to verify we get custom error message for 401 error"""

        mocked_request.return_value = get_response(401)
        with self.assertRaises(RechargeUnauthorizedError) as e:
            response_json, _ = self.client_obj.request(self.method, self.path, self.url)

        self.assertEqual(str(e.exception), 'HTTP-error-code: 401, Error: The request was not able to be authenticated.')

    def test_402_error_custom_error(self, mocked_request, mocked_sleep, mocked_check_token):
        """Test case to verify we get custom error message for 402 error"""

        mocked_request.return_value = get_response(402)
        with self.assertRaises(RechargeRequestFailedError) as e:
            response_json, _ = self.client_obj.request(self.method, self.path, self.url)

        self.assertEqual(str(e.exception), 'HTTP-error-code: 402, Error: The request to the resource failed because of Payment issue.')

    def test_403_error_custom_error(self, mocked_request, mocked_sleep, mocked_check_token):
        """Test case to verify we get custom error message for 403 error"""

        mocked_request.return_value = get_response(403)
        with self.assertRaises(RechargeForbiddenError) as e:
            response_json, _ = self.client_obj.request(self.method, self.path, self.url)

        self.assertEqual(str(e.exception), 'HTTP-error-code: 403, Error: The request was authenticated but not authorized for the requested resource (Permission scope error).')

    def test_404_error_custom_error(self, mocked_request, mocked_sleep, mocked_check_token):
        """Test case to verify we get custom error message for 404 error"""

        mocked_request.return_value = get_response(404)
        with self.assertRaises(RechargeNotFoundError) as e:
            response_json, _ = self.client_obj.request(self.method, self.path, self.url)

        self.assertEqual(str(e.exception), 'HTTP-error-code: 404, Error: The requested resource was not found.')

    def test_405_error_custom_error(self, mocked_request, mocked_sleep, mocked_check_token):
        """Test case to verify we get custom error message for 405 error"""

        mocked_request.return_value = get_response(405)
        with self.assertRaises(RechargeMethodNotAllowedError) as e:
            response_json, _ = self.client_obj.request(self.method, self.path, self.url)

        self.assertEqual(str(e.exception), 'HTTP-error-code: 405, Error: The provided HTTP method is not supported by the URL.')

    def test_406_error_custom_error(self, mocked_request, mocked_sleep, mocked_check_token):
        """Test case to verify we get custom error message for 406 error"""

        mocked_request.return_value = get_response(406)
        with self.assertRaises(RechargeUnacceptableRequestError) as e:
            response_json, _ = self.client_obj.request(self.method, self.path, self.url)

        self.assertEqual(str(e.exception), 'HTTP-error-code: 406, Error: The request was unacceptable, or requesting a data source which is not allowed although permissions permit the request.')

    def test_409_error_custom_error(self, mocked_request, mocked_sleep, mocked_check_token):
        """Test case to verify we get custom error message for 409 error"""

        mocked_request.return_value = get_response(409)
        with self.assertRaises(RechargeConflictError) as e:
            response_json, _ = self.client_obj.request(self.method, self.path, self.url)

        self.assertEqual(str(e.exception), 'HTTP-error-code: 409, Error: The request is in conflict, or would create a conflict with an existing resource.')

    def test_415_error_custom_error(self, mocked_request, mocked_sleep, mocked_check_token):
        """Test case to verify we get custom error message for 415 error"""

        mocked_request.return_value = get_response(415)
        with self.assertRaises(RechargeJSONObjectError) as e:
            response_json, _ = self.client_obj.request(self.method, self.path, self.url)

        self.assertEqual(str(e.exception), 'HTTP-error-code: 415, Error: The request body was not a JSON object.')

    def test_422_error_custom_error(self, mocked_request, mocked_sleep, mocked_check_token):
        """Test case to verify we get custom error message for 422 error"""

        mocked_request.return_value = get_response(422)
        with self.assertRaises(RechargeUnprocessableEntityError) as e:
            response_json, _ = self.client_obj.request(self.method, self.path, self.url)

        self.assertEqual(str(e.exception), 'HTTP-error-code: 422, Error: The request was understood but cannot be processed due to invalid or missing supplemental information.')

    def test_426_error_custom_error(self, mocked_request, mocked_sleep, mocked_check_token):
        """Test case to verify we get custom error message for 426 error"""

        mocked_request.return_value = get_response(426)
        with self.assertRaises(RechargeInvalidAPI) as e:
            response_json, _ = self.client_obj.request(self.method, self.path, self.url)

        self.assertEqual(str(e.exception), 'HTTP-error-code: 426, Error: The request was made using an invalid API version.')

    def test_429_error_custom_error(self, mocked_request, mocked_sleep, mocked_check_token):
        """Test case to verify we get custom error message for 429 error"""

        mocked_request.return_value = get_response(429)
        with self.assertRaises(RechargeRateLimitError) as e:
            response_json, _ = self.client_obj.request(self.method, self.path, self.url)

        self.assertEqual(str(e.exception), 'HTTP-error-code: 429, Error: The request has been rate limited.')
        self.assertEqual(mocked_request.call_count, 5)

    def test_500_error_custom_error(self, mocked_request, mocked_sleep, mocked_check_token):
        """Test case to verify we get custom error message for 500 error"""

        mocked_request.return_value = get_response(500)
        with self.assertRaises(RechargeInternalServiceError) as e:
            response_json, _ = self.client_obj.request(self.method, self.path, self.url)

        self.assertEqual(str(e.exception), 'HTTP-error-code: 500, Error: The request could not be processed due to internal server error.')
        self.assertEqual(mocked_request.call_count, 5)

    def test_501_error_custom_error(self, mocked_request, mocked_sleep, mocked_check_token):
        """Test case to verify we get custom error message for 501 error"""

        mocked_request.return_value = get_response(501)
        with self.assertRaises(RechargeUnimplementedResourceError) as e:
            response_json, _ = self.client_obj.request(self.method, self.path, self.url)

        self.assertEqual(str(e.exception), 'HTTP-error-code: 501, Error: The resource requested has not been implemented in the current version.')
        self.assertEqual(mocked_request.call_count, 5)

    def test_502_error_custom_error(self, mocked_request, mocked_sleep, mocked_check_token):
        """Test case to verify we get custom error message for 502 error"""

        mocked_request.return_value = get_response(502)
        with self.assertRaises(Server5xxError) as e:
            response_json, _ = self.client_obj.request(self.method, self.path, self.url)

        self.assertEqual(str(e.exception), 'HTTP-error-code: 502, Error: Unknown Error')
        self.assertEqual(mocked_request.call_count, 5)

    def test_503_error_custom_error(self, mocked_request, mocked_sleep, mocked_check_token):
        """Test case to verify we get custom error message for 503 error"""

        mocked_request.return_value = get_response(503)
        with self.assertRaises(RechargeThirdPartyServiceTimeoutError) as e:
            response_json, _ = self.client_obj.request(self.method, self.path, self.url)

        self.assertEqual(str(e.exception), 'HTTP-error-code: 503, Error: A third party service on which the request depends has timed out.')
        self.assertEqual(mocked_request.call_count, 5)

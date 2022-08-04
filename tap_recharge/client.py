import time
import backoff
import requests

import singer
from singer import metrics, utils
from requests.exceptions import Timeout

LOGGER = singer.get_logger()
REQUEST_TIMEOUT = 600

class Server5xxError(Exception):
    pass


class RechargeError(Exception):
    pass


class RechargeBadRequestError(RechargeError):
    pass


class RechargeUnauthorizedError(RechargeError):
    pass


class RechargeRequestFailedError(RechargeError):
    pass


class RechargeNotFoundError(RechargeError):
    pass

class RechargeUnacceptableRequestError(RechargeError):
    pass

class RechargeMethodNotAllowedError(RechargeError):
    pass


class RechargeConflictError(RechargeError):
    pass

class RechargeJSONObjectError(RechargeError):
    pass

class RechargeForbiddenError(RechargeError):
    pass

class RechargeRateLimitError(Exception):
    pass

class RechargeUnprocessableEntityError(RechargeError):
    pass

class RechargeInvalidAPI(RechargeError):
    pass

class RechargeInternalServiceError(Server5xxError):
    pass

class RechargeUnimplementedResourceError(Server5xxError):
    pass

class RechargeThirdPartyServiceTimeoutError(Server5xxError):
    pass

# Doumentation: https://docs.rechargepayments.com/docs/api-standards#errors
ERROR_CODE_EXCEPTION_MAPPING = {
    400: {
        "exception": RechargeBadRequestError,
        "message": "The request was not understood by Recharge.",
    },
    401: {
        "exception": RechargeUnauthorizedError,
        "message": "The request was not able to be authenticated.",
    },
    402: {
        "exception": RechargeRequestFailedError,
        "message": "The request to the resource failed because of Payment issue.",
    },
    403: {
        "exception": RechargeForbiddenError,
        "message": "The request was authenticated but not authorized for the requested resource (Permission scope error).",
    },
    404: {
        "exception": RechargeNotFoundError,
        "message": "The requested resource was not found.",
    },
    405: {
        "exception": RechargeMethodNotAllowedError,
        "message": "The provided HTTP method is not supported by the URL.",
    },
    406: {
        "exception": RechargeUnacceptableRequestError,
        "message": "The request was unacceptable, or requesting a data source which is not allowed although permissions permit the request.",
    },
    409: {
        "exception": RechargeConflictError,
        "message": "The request is in conflict, or would create a conflict with an existing resource.",
    },
    415: {
        "exception": RechargeJSONObjectError,
        "message": "The request body was not a JSON object.",
    },
    422: {
        "exception": RechargeUnprocessableEntityError,
        "message": "The request was understood but cannot be processed due to invalid or missing supplemental information.",
    },
    426: {
        "exception": RechargeInvalidAPI,
        "message": "The request was made using an invalid API version.",
    },
    429: {
        "exception": RechargeRateLimitError,
        "message": "The request has been rate limited.",
    },
    500: {
        "exception": RechargeInternalServiceError,
        "message": "The request could not be processed due to internal server error.",
    },
    501: {
        "exception": RechargeUnimplementedResourceError,
        "message": "The resource requested has not been implemented in the current version.",
    },
    503: {
        "exception": RechargeThirdPartyServiceTimeoutError,
        "message": "A third party service on which the request depends has timed out.",
    }
}

def get_exception_for_error_code(error_code):
    """Function to retrieve exceptions based on error code"""
    if error_code == 429:
        # Delay for 5 seconds for leaky bucket rate limit algorithm
        time.sleep(5)

    exception = ERROR_CODE_EXCEPTION_MAPPING.get(error_code, {}).get('exception')
    # If the error code is not from the listed error codes then return Server5XXError or RechargeError respectively
    if not exception:
        if error_code >= 500:
            return Server5xxError
        return RechargeError
    return exception

def raise_for_error(response):
    """Function to raise custom error along with message based on response."""

    error_code = response.status_code

    try:
        response_json = response.json()
    except ValueError:
        response_json = {}

    error_message = response_json.get('error', response_json.get('errors'))

    if not error_message:
        error_message = ERROR_CODE_EXCEPTION_MAPPING.get(error_code, {}).get('message', 'Unknown Error')

    message = f'HTTP-error-code: {error_code}, Error: {error_message}'
    ex = get_exception_for_error_code(error_code)

    if error_code == 401 and 'bad authentication' in error_message:
        LOGGER.error("Your API Token has been deleted or the token is invalid.\n Please re-authenticate your connection to generate a new token and resume extraction.")

    raise ex(message) from None


class RechargeClient:
    def __init__(
            self,
            access_token,
            user_agent=None,
            request_timeout=REQUEST_TIMEOUT):
        self.__access_token = access_token
        self.__user_agent = user_agent
        self.__session = requests.Session()
        self.__base_url = None
        self.__verified = False
        # if request_timeout is other than 0,"0" or "" then use request_timeout
        if request_timeout and float(request_timeout):
            request_timeout = float(request_timeout)
        else: # If value is 0,"0" or "" then set default to 300 seconds.
            request_timeout = REQUEST_TIMEOUT
        self.request_timeout = request_timeout

    # Backoff the request for 5 times when Timeout or Connection error occurs
    @backoff.on_exception(
        backoff.expo,
        (Timeout, requests.ConnectionError, Server5xxError),
        max_tries=5,
        factor=2)
    def __enter__(self):
        self.__verified = self.check_access_token()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.__session.close()

    def check_access_token(self):
        if self.__access_token is None:
            raise Exception('Error: Missing access_token.')
        headers = {}
        if self.__user_agent:
            headers['User-Agent'] = self.__user_agent
        headers['X-Recharge-Access-Token'] = self.__access_token
        headers['Accept'] = 'application/json'
        response = self.__session.get(
            # Simple endpoint that returns 1 record w/ default organization URN
            url='https://api.rechargeapps.com',
            headers=headers,
            timeout=self.request_timeout)
        if response.status_code != 200:
            LOGGER.error('Error status_code = %s', response.status_code)
            raise_for_error(response)
        else:
            return True

    # Added backoff for 5 times when Timeout error occurs
    @backoff.on_exception(
        backoff.expo,
        (Timeout, Server5xxError, requests.ConnectionError, RechargeRateLimitError),
        max_tries=5,
        factor=2)
    # Call/rate limit: https://docs.rechargepayments.com/docs/api-rate-limits
    # Reduced rate limit from (120, 60) to (100, 60) due to intermittent 429 errors
    @utils.ratelimit(100, 60)
    def request(self, method, path=None, url=None, **kwargs): # pylint: disable=too-many-branches,too-many-statements
        if not self.__verified:
            self.__verified = self.check_access_token()

        if not url and self.__base_url is None:
            self.__base_url = 'https://api.rechargeapps.com/'

        if not url and path:
            url = self.__base_url + path

        if 'endpoint' in kwargs:
            endpoint = kwargs['endpoint']
            del kwargs['endpoint']
        else:
            endpoint = None

        if 'headers' not in kwargs:
            kwargs['headers'] = {}
        kwargs['headers']['X-Recharge-Access-Token'] = self.__access_token
        kwargs['headers']['Accept'] = 'application/json'
        kwargs['headers']['X-Recharge-Version'] = '2021-11'

        # In API Version: 2021-11, the Products endpoint is only available for merchants doing a custom integration with a PIM
        # (Product Information Management) system that isn’t Shopify or BigCommerce hence keeping API version 2021-01 for Products.
        # Documentation: https://docs.rechargepayments.com/changelog/2021-11-api-release-notes/#2-products-and-plans
        if path == 'products':
            kwargs['headers']['X-Recharge-Version'] = '2021-01'

        if self.__user_agent:
            kwargs['headers']['User-Agent'] = self.__user_agent

        if method == 'POST':
            kwargs['headers']['Content-Type'] = 'application/json'

        with metrics.http_request_timer(endpoint) as timer:
            response = self.__session.request(method, url, stream=True, timeout=self.request_timeout, **kwargs)
            timer.tags[metrics.Tag.http_status_code] = response.status_code

        if response.status_code != 200:
            raise_for_error(response)

        # Intermittent JSONDecodeErrors when parsing JSON; Adding 2 attempts
        # FIRST ATTEMPT
        with metrics.http_request_timer(endpoint) as timer:
            response = self.__session.request(method, url, stream=True, timeout=self.request_timeout, **kwargs)
            timer.tags[metrics.Tag.http_status_code] = response.status_code

        if response.status_code != 200:
            raise_for_error(response)

        # Catch invalid JSON (e.g. unterminated string errors)
        try:
            response_json = response.json()
            return response_json
        except ValueError as err:  # includes simplejson.decoder.JSONDecodeError
            LOGGER.warning(err)

        # SECOND ATTEMPT, if there is a ValueError (unterminated string error)
        with metrics.http_request_timer(endpoint) as timer:
            response = self.__session.request(
                method,
                url,
                stream=True,
                timeout=self.request_timeout,
                **kwargs)
            timer.tags[metrics.Tag.http_status_code] = response.status_code

        if response.status_code != 200:
            raise_for_error(response)

        # Log invalid JSON (e.g. unterminated string errors)
        try:
            response_json = response.json()
            return response_json
        except ValueError as err:  # includes simplejson.decoder.JSONDecodeError
            LOGGER.error(err)
            raise Exception(err)

    def get(self, path, **kwargs):
        return self.request('GET', path=path, **kwargs)

    def post(self, path, **kwargs):
        return self.request('POST', path=path, **kwargs)

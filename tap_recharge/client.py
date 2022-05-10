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


class Server429Error(Exception):
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


class RechargeMethodNotAllowedError(RechargeError):
    pass


class RechargeConflictError(RechargeError):
    pass


class RechargeForbiddenError(RechargeError):
    pass


class RechargeUnprocessableEntityError(RechargeError):
    pass


class RechargeInternalServiceError(RechargeError):
    pass


ERROR_CODE_EXCEPTION_MAPPING = {
    400: RechargeBadRequestError,
    401: RechargeUnauthorizedError,
    402: RechargeRequestFailedError,
    403: RechargeForbiddenError,
    404: RechargeNotFoundError,
    405: RechargeMethodNotAllowedError,
    409: RechargeConflictError,
    422: RechargeUnprocessableEntityError,
    500: RechargeInternalServiceError}


def get_exception_for_error_code(error_code):
    return ERROR_CODE_EXCEPTION_MAPPING.get(error_code, RechargeError)

def raise_for_error(response):
    try:
        response.raise_for_status()
    except (requests.HTTPError, requests.ConnectionError) as error:
        try:
            content_length = len(response.content)
            if content_length == 0:
                # There is nothing we can do here since Recharge has neither sent
                # us a 2xx response nor a response content.
                return
            response = response.json()
            if ('error' in response) or ('errorCode' in response):
                message = f"{response.get('error', str(error))}: \
                    {response.get('message', 'Unknown Error')}"
                error_code = response.get('status')
                ex = get_exception_for_error_code(error_code)
                if response.status_code == 401 and 'Expired access token' in message:
                    LOGGER.error("Your access_token has expired as per Recharge’s security \
                        policy. \n Please re-authenticate your connection to generate a new token \
                        and resume extraction.")
                raise ex(message)
            else:
                raise RechargeError(error)
        except (ValueError, TypeError):
            raise RechargeError(error)


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
        (Timeout, requests.ConnectionError),
        max_tries=5,
        factor=2)
    def __enter__(self):
        self.__verified = self.check_access_token()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.__session.close()

    # Backoff the request for 5 times when Timeout error occurs
    @backoff.on_exception(
        backoff.expo,
        Server5xxError,
        max_tries=5,
        factor=2)
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
        (Timeout, Server5xxError, requests.ConnectionError, Server429Error),
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
        # If we did not specify any API Version during API Call, the Recharge will use the default API Version of our store
        # the 'collections' was added as part of API Version: '2021-11', for older API Version,
        # we will get empty records so adding 'X-Recharge-Version' for 'collections' API call
        if path == 'collections':
            kwargs['headers']['X-Recharge-Version'] = '2021-11'

        if self.__user_agent:
            kwargs['headers']['User-Agent'] = self.__user_agent

        if method == 'POST':
            kwargs['headers']['Content-Type'] = 'application/json'

        with metrics.http_request_timer(endpoint) as timer:
            response = self.__session.request(method, url, stream=True, timeout=self.request_timeout, **kwargs)
            timer.tags[metrics.Tag.http_status_code] = response.status_code

        if response.status_code >= 500:
            raise Server5xxError()

        if response.status_code == 429:
            # Delay for 5 seconds for leaky bucket rate limit algorithm
            time.sleep(5)
            raise Server429Error()

        if response.status_code != 200:
            raise_for_error(response)

        # Intermittent JSONDecodeErrors when parsing JSON; Adding 2 attempts
        # FIRST ATTEMPT
        with metrics.http_request_timer(endpoint) as timer:
            response = self.__session.request(method, url, stream=True, timeout=self.request_timeout, **kwargs)
            timer.tags[metrics.Tag.http_status_code] = response.status_code

        if response.status_code >= 500:
            raise Server5xxError()

        if response.status_code == 429:
            # Delay for 5 seconds for leaky bucket rate limit algorithm
            time.sleep(5)
            raise Server429Error()

        if response.status_code != 200:
            raise_for_error(response)

        # Catch invalid JSON (e.g. unterminated string errors)
        try:
            response_json = response.json()
            return response_json, response.links
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

        if response.status_code >= 500:
            raise Server5xxError()

        if response.status_code == 429:
            raise Server429Error()

        if response.status_code != 200:
            raise_for_error(response)

        # Log invalid JSON (e.g. unterminated string errors)
        try:
            response_json = response.json()
            return response_json, response.links
        except ValueError as err:  # includes simplejson.decoder.JSONDecodeError
            LOGGER.error(err)
            raise Exception(err)

    def get(self, path, **kwargs):
        return self.request('GET', path=path, **kwargs)

    def post(self, path, **kwargs):
        return self.request('POST', path=path, **kwargs)

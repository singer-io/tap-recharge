import backoff
import requests
from requests.exceptions import ConnectionError
from singer import metrics, utils
import singer

LOGGER = singer.get_logger()


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
                message = '%s: %s' % (response.get('error', str(error)),
                                      response.get('message', 'Unknown Error'))
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


class RechargeClient(object):
    def __init__(self,
                 access_token,
                 user_agent=None):
        self.__access_token = access_token
        self.__user_agent = user_agent
        self.__session = requests.Session()
        self.__base_url = None
        self.__verified = False

    def __enter__(self):
        self.__verified = self.check_access_token()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.__session.close()

    @backoff.on_exception(backoff.expo,
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
            headers=headers)
        if response.status_code != 200:
            LOGGER.error('Error status_code = {}'.format(response.status_code))
            raise_for_error(response)
        else:
            return True


    @backoff.on_exception(backoff.expo,
                          (Server5xxError, ConnectionError, Server429Error),
                          max_tries=5,
                          factor=2)
    # Call/rate limit: https://developer.rechargepayments.com/#call-limit
    @utils.ratelimit(160, 60)
    def request(self, method, path=None, url=None, **kwargs):
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

        if self.__user_agent:
            kwargs['headers']['User-Agent'] = self.__user_agent

        if method == 'POST':
            kwargs['headers']['Content-Type'] = 'application/json'

        with metrics.http_request_timer(endpoint) as timer:
            response = self.__session.request(method, url, **kwargs)
            timer.tags[metrics.Tag.http_status_code] = response.status_code

        if response.status_code >= 500:
            raise Server5xxError()

        if response.status_code != 200:
            raise_for_error(response)

        return response.json()

    def get(self, path, **kwargs):
        return self.request('GET', path=path, **kwargs)

    def post(self, path, **kwargs):
        return self.request('POST', path=path, **kwargs)

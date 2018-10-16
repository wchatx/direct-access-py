import sys
import base64
import logging

import requests
from retrying import retry


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)-8s %(message)s',
    datefmt='%a, %d %b %Y %H:%M:%S'
)


class DAAuthException(Exception):
    pass


class DAQueryException(Exception):
    pass


class BaseAPI(object):
    url = 'https://di-api.drillinginfo.com'

    def __init__(self, api_key):
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers = {
            'X-API-KEY': self.api_key,
            'User-Agent': 'direct-access-py'
        }

    @retry(wait_exponential_multiplier=5000, wait_exponential_max=100000, stop_max_attempt_number=20)
    def query(self, dataset, **options):
        """
        Query generator

        :param dataset: a supported Direct Access dataset name (see Direct Access documentation)
        :param options: query parameters for the provided dataset as kwargs (see Direct Access documention for valid
        values)
        :return:
        """
        url = '{base}/{dataset}'.format(base=self.url, dataset=dataset)

        page = 1
        while True:
            params = dict(options)
            params['page'] = page
            request = self.session.get(url, params=params)
            try:
                r = request.json()
            except ValueError:
                logging.error('Query Error: {}'.format(request.content.decode()))
                sys.exit(1)
            if not len(r) > 0:
                break
            page = page + 1
            for record in r:
                yield record


class DirectAccessV1(BaseAPI):
    def __init__(self, api_key):
        super(DirectAccessV1, self).__init__(api_key)
        self.url = self.url + '/v1/direct-access'


class DirectAccessV2(BaseAPI):
    def __init__(self, client_id, client_secret, api_key):
        super(DirectAccessV2, self).__init__(api_key)
        self.client_id = client_id
        self.client_secret = client_secret
        self.url = self.url + '/v2/direct-access'

        # get access/refresh tokens
        initial_token_request = self._manage_token()
        self.access_token = initial_token_request['access_token']
        logging.info('Access token expiration: {}'.format(initial_token_request['expires_in']))

    def _encode_secrets(self):
        return base64.b64encode(':'.join([self.client_id, self.client_secret]).encode()).decode()

    @retry(wait_exponential_multiplier=5000, wait_exponential_max=100000, stop_max_attempt_number=20)
    def _manage_token(self):
        """
        Get initial access_token or refresh existing token. The access_token is added to the session object

        :param refresh_token: the refresh token provided at the time an access token was granted
        :return: the refresh token response
        """
        url = self.url + '/tokens'
        self.session.headers['Authorization'] = 'Basic {}'.format(self._encode_secrets())
        self.session.headers['Content-Type'] = 'application/x-www-form-urlencoded'

        payload = {
            'grant_type': 'client_credentials'
        }

        r = self.session.post(url, params=payload)

        if r.status_code != 200:
            raise DAAuthException(
                'Error getting token. Code: {} Message: {}'.format(r.status_code, r.content)
            )

        self.session.headers['Authorization'] = 'bearer {}'.format(r.json()['access_token'])

        return r.json()

    @retry(wait_exponential_multiplier=5000, wait_exponential_max=100000, stop_max_attempt_number=20)
    def query(self, dataset, **options):
        """
        Direct Access v2 query method

        :param dataset: the API dataset to query. See Direct Access documentation for valid values
        :param options: the API parameters to request. See Direct Access documentation for valid values
        :return: either a list of dictionary-based API responses or the responses themselves
        """
        url = '{base}/{dataset}'.format(base=self.url, dataset=dataset)
        params = dict(options)
        while True:
            if isinstance(params, dict):
                response = self.session.get(
                    url='{}?{}'.format(url, '&'.join(['{}={}'.format(k, v) for k, v in params.items()]))
                )
            else:
                response = self.session.get(url=self.url + params)

            if response.status_code != 200:
                # Token/auth error
                if response.status_code == 401:
                    logging.warning('Access token expired. Attempting refresh...')
                    # Attempt to acquire new token
                    self._manage_token()

                    r = self.session.get(url, params=params)
                # All other errors. Will retry
                else:
                    logging.warning('Non-200 response: {} {}'.format(response.status_code, response.content.decode()))
                    raise DAQueryException('Non-200 response: {} {}'.format(response.status_code, response.content))

            if 'next' in response.links:
                params = response.links['next']['url']

            if len(response.json()) > 0:
                for record in response.json():
                    yield record
            else:
                break


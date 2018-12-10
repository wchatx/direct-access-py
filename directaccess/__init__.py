import base64
import logging

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)-8s %(message)s',
    datefmt='%a, %d %b %Y %H:%M:%S'
)


class DAAuthException(Exception):
    pass


class DAQueryException(Exception):
    pass


class DADatasetException(Exception):
    pass


class BaseAPI(object):
    url = 'https://di-api.drillinginfo.com'

    def __init__(self, api_key):
        self.api_key = api_key
        if not self.api_key:
            raise DAAuthException('API KEY is required')

        self.session = requests.Session()
        self.session.headers.update({
            'X-API-KEY': self.api_key,
            'User-Agent': 'direct-access-py'
        })
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        self.session.mount('https://', HTTPAdapter(max_retries=retries))

        self.logger = logging.getLogger('direct-access-py')

    def query(self, dataset, **options):
        url = self.url + '/' + dataset

        page = 1
        while True:
            params = dict(options)
            params['page'] = page
            request = self.session.get(url, params=params)
            try:
                r = request.json()
            except ValueError:
                msg = 'Query Error: {}'.format(request.content.decode())
                self.logger.error(msg)
                raise DAQueryException(msg)
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
        if not self.client_id and not self.client_secret:
            raise DAAuthException('CLIENT ID and CLIENT SECRET are required')

        self.url = self.url + '/v2/direct-access'

        self.access_token = self._get_access_token()['access_token']
        self.logger.debug('Access token acquired: {}'.format(self.access_token))

    def _encode_secrets(self):
        return base64.b64encode(':'.join([self.client_id, self.client_secret]).encode()).decode()

    def _get_access_token(self):
        url = self.url + '/tokens'
        self.session.headers['Authorization'] = 'Basic {}'.format(self._encode_secrets())
        self.session.headers['Content-Type'] = 'application/x-www-form-urlencoded'

        payload = {
            'grant_type': 'client_credentials'
        }

        r = self.session.post(url, params=payload)

        if r.status_code != 200:
            msg = 'Error getting token. Code: {} Message: {}'.format(r.status_code, r.content)
            self.logger.error(msg)
            raise DAAuthException(msg)

        self.session.headers['Authorization'] = 'bearer {}'.format(r.json()['access_token'])

        return r.json()

    def query(self, dataset, **options):
        url = self.url + '/' + dataset

        while True:
            if isinstance(options, dict):
                response = self.session.get(url, params=options)
            else:
                response = self.session.get(url=self.url + options)

            if response.status_code != 200:
                if response.status_code == 401:
                    self.logger.warning('Access token expired. Acquiring a new one...')
                    self._get_access_token()
                    continue
                elif response.status_code == 404:
                    msg = 'Invalid dataset provided: ' + dataset
                    self.logger.error(msg)
                    raise DADatasetException(msg)

                else:
                    msg = 'Non-200 response: {} {}'.format(response.status_code, response.content.decode())
                    self.logger.error(msg)

            if 'next' in response.links:
                options = response.links['next']['url']

            if len(response.json()) > 0:
                for record in response.json():
                    yield record
            else:
                break
